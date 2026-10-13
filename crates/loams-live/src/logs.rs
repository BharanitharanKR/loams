//! Function logs (design §45 §3.3, D695; LV1 plan Task 10): the
//! `console.*` lines of an app's calls on this node, in a ring of the newest
//! [`RING_LINES`] lines, which `LiveAdminService.TailLogs` streams.
//!
//! The ring is fed from each call's [`CallOutput`] (a mutation's committing
//! attempt only, so a rerun is invisible), with the time it was recorded,
//! the call's request id, its function and its deployment. A call that
//! ends with a function failure (it threw, ran past its CPU or memory
//! limit, or its worker crashed) adds one `error` line with that error;
//! its console lines are lost with the call (LV1 row T3-8). Any other
//! failure adds nothing: those are the caller's or the server's, never the
//! function's own output, and their text may name internal state.
//!
//! What reaches the ring is only what the function itself wrote, cut to
//! [`MAX_LINE_BYTES`]: never the arguments, the result, a document, a
//! credential or the idempotency key, which the server does not add. The
//! ring is the app's own, so one app's lines never reach another's tail.
//! Lines are never written to the server's own log. Not durable: a restart
//! empties the ring, and each node holds its own calls (merging nodes is
//! LV1 Task 24's).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::Stream;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::{CallOutput, LiveError, LogLevel, pb};

/// Lines kept per app per node (design §45 §3.3, Q636).
pub const RING_LINES: usize = 10_000;

/// The longest line the ring keeps, in bytes: the runtime's per-line limit
/// (`JsConfig::console_line_bytes`, 4 KiB). An error's text is cut to it
/// too.
pub const MAX_LINE_BYTES: usize = 4 * 1024;

/// The most entries one [`Ring::tail`] item carries.
pub const TAIL_BATCH: usize = 500;

/// How often a following [`Ring::tail`] yields an empty item when no line
/// arrives, so proxies keep the stream open (as `Watch`'s heartbeats).
pub const TAIL_HEARTBEAT: Duration = Duration::from_secs(15);

/// The response header that names a call's request id.
pub const REQUEST_ID_HEADER: &str = "loams-request-id";

/// One line of a call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The line's position in the ring's stream, from 0.
    pub seq: u64,
    /// Unix time at which the ring recorded it, in ms.
    pub ts_ms: u64,
    pub request_id: String,
    /// `module:export`.
    pub function: String,
    /// The deployment that ran the call; `None` for a built-in.
    pub deployment: Option<Ulid>,
    pub level: LogLevel,
    pub line: String,
    /// The line was longer than the per-line limit and was cut.
    pub truncated: bool,
}

/// Which call a [`Ring::record`] is for.
#[derive(Debug, Clone, Copy)]
pub struct CallInfo<'a> {
    /// `None`: the ring makes one up if the call wrote anything (a
    /// subscription's evaluation has no request of its own).
    pub request_id: Option<&'a str>,
    pub function: &'a str,
    /// The nil ULID for a built-in function.
    pub deployment: Ulid,
}

/// Which lines a tail reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TailFilter {
    /// Only lines recorded at or after this Unix time, in ms.
    pub since_ms: Option<u64>,
    /// Only lines of this function.
    pub function: Option<String>,
}

impl TailFilter {
    fn admits(&self, e: &LogEntry) -> bool {
        self.since_ms.is_none_or(|since| e.ts_ms >= since)
            && self.function.as_deref().is_none_or(|f| e.function == f)
    }
}

/// What one [`Ring::read`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Read {
    /// The matching lines, oldest first.
    pub entries: Vec<LogEntry>,
    /// Lines from the asked position that the ring overwrote before this
    /// read (counted whether they matched or not).
    pub skipped: u64,
    /// Where the next read continues.
    pub next: u64,
    /// The read reached the newest line.
    pub caught_up: bool,
}

/// The newest lines of an app's calls on this node. Cheap to share.
pub struct Ring {
    capacity: usize,
    state: Mutex<State>,
    /// The next sequence number, published on every push.
    written: watch::Sender<u64>,
}

#[derive(Default)]
struct State {
    lines: VecDeque<LogEntry>,
    next_seq: u64,
    /// Lines overwritten since the ring started.
    dropped: u64,
}

impl std::fmt::Debug for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .field("dropped", &self.dropped())
            .finish_non_exhaustive()
    }
}

impl Default for Ring {
    fn default() -> Self {
        Ring::new(RING_LINES)
    }
}

impl Ring {
    /// A ring of `capacity` lines (at least one).
    pub fn new(capacity: usize) -> Self {
        Ring {
            capacity: capacity.max(1),
            state: Mutex::new(State::default()),
            written: watch::Sender::new(0),
        }
    }

    /// The most lines it keeps.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The lines it holds now.
    pub fn len(&self) -> usize {
        self.lock().lines.len()
    }

    /// Whether it holds no line.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lines overwritten by newer ones since the ring started.
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// A new request id: a ULID.
    pub fn request_id() -> String {
        Ulid::generate().to_string()
    }

    /// Records what call `call` wrote: its console lines and, when more
    /// lines than the per-call limit were written, a `warn` line counting
    /// the rest; or, for a call that ended with a function failure, one
    /// `error` line with that error. Other failures record nothing.
    pub fn record(&self, call: &CallInfo<'_>, outcome: Result<&CallOutput, &LiveError>) {
        let mut lines: Vec<(LogLevel, String, bool)> = Vec::new();
        match outcome {
            Ok(output) => {
                for l in &output.logs {
                    let (line, cut) = clip(&l.line);
                    lines.push((l.level, line, l.truncated || cut));
                }
                if output.dropped > 0 {
                    lines.push((
                        LogLevel::Warn,
                        format!(
                            "{} more console lines of this call were dropped (over the per-call \
                             limit)",
                            output.dropped
                        ),
                        false,
                    ));
                }
            }
            Err(e) if function_failure(e) => {
                let (line, cut) = clip(&e.to_string());
                lines.push((LogLevel::Error, line, cut));
            }
            Err(_) => {}
        }
        if lines.is_empty() {
            return;
        }
        let made;
        let request_id = match call.request_id {
            Some(id) => id,
            None => {
                made = Ring::request_id();
                &made
            }
        };
        let deployment = (!call.deployment.is_nil()).then_some(call.deployment);
        let ts_ms = now_ms();
        let next = {
            let mut state = self.lock();
            for (level, line, truncated) in lines {
                if state.lines.len() == self.capacity {
                    state.lines.pop_front();
                    state.dropped += 1;
                }
                let seq = state.next_seq;
                state.next_seq += 1;
                state.lines.push_back(LogEntry {
                    seq,
                    ts_ms,
                    request_id: request_id.to_string(),
                    function: call.function.to_string(),
                    deployment,
                    level,
                    line,
                    truncated,
                });
            }
            state.next_seq
        };
        self.written.send_replace(next);
    }

    /// Up to `max` lines matching `filter` from sequence number `from` on,
    /// oldest first. Lines before the oldest one held are counted in
    /// [`Read::skipped`].
    pub fn read(&self, from: u64, filter: &TailFilter, max: usize) -> Read {
        let state = self.lock();
        let oldest = state.next_seq - state.lines.len() as u64;
        let start = from.max(oldest);
        let skipped = oldest.saturating_sub(from);
        let mut entries = Vec::new();
        let mut next = start;
        // `start - oldest` is within `lines`: `start` is at most `next_seq`.
        let offset = usize::try_from(start - oldest).unwrap_or(usize::MAX);
        for e in state.lines.iter().skip(offset) {
            if entries.len() == max {
                break;
            }
            next = e.seq + 1;
            if filter.admits(e) {
                entries.push(e.clone());
            }
        }
        Read {
            entries,
            skipped,
            next,
            caught_up: next == state.next_seq,
        }
    }

    /// The lines matching `filter`, from the oldest one held, as a stream of
    /// batches of at most [`TAIL_BATCH`]; then, with `follow`, new lines as
    /// they are recorded, with an empty batch every [`TAIL_HEARTBEAT`]
    /// without any, until `stop` is cancelled. Without `follow` the stream
    /// ends once it has caught up.
    pub fn tail(
        self: &Arc<Self>,
        filter: TailFilter,
        follow: bool,
        stop: CancellationToken,
    ) -> impl Stream<Item = Read> + Send + 'static {
        struct Tail {
            ring: Arc<Ring>,
            filter: TailFilter,
            follow: bool,
            stop: CancellationToken,
            written: watch::Receiver<u64>,
            at: u64,
            done: bool,
        }
        let at = {
            let state = self.lock();
            state.next_seq - state.lines.len() as u64
        };
        let tail = Tail {
            ring: self.clone(),
            filter,
            follow,
            stop,
            written: self.written.subscribe(),
            at,
            done: false,
        };
        futures::stream::unfold(tail, |mut t| async move {
            if t.done {
                return None;
            }
            loop {
                // Marked seen before the read, so a line recorded after
                // the read wakes the wait below.
                t.written.borrow_and_update();
                let read = t.ring.read(t.at, &t.filter, TAIL_BATCH);
                t.at = read.next;
                if !read.entries.is_empty() || read.skipped > 0 {
                    return Some((read, t));
                }
                if !read.caught_up {
                    continue;
                }
                if !t.follow {
                    return None;
                }
                tokio::select! {
                    () = t.stop.cancelled() => return None,
                    changed = t.written.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                    }
                    () = tokio::time::sleep(TAIL_HEARTBEAT) => {
                        let heartbeat = Read { next: t.at, caught_up: true, ..Read::default() };
                        return Some((heartbeat, t));
                    }
                }
            }
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Whether `e` is the function's own failure, which the ring records.
fn function_failure(e: &LiveError) -> bool {
    matches!(
        e.code(),
        pb::ErrorCode::ERROR_CODE_FUNCTION_ERROR
            | pb::ErrorCode::ERROR_CODE_FUNCTION_TIMEOUT
            | pb::ErrorCode::ERROR_CODE_FUNCTION_OUT_OF_MEMORY
    )
}

/// `line` cut to [`MAX_LINE_BYTES`] at a character boundary, and whether
/// it was cut.
fn clip(line: &str) -> (String, bool) {
    if line.len() <= MAX_LINE_BYTES {
        return (line.to_string(), false);
    }
    let mut end = MAX_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    (line[..end].to_string(), true)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl LogEntry {
    /// The admin API's `LogEntry`.
    pub fn to_proto(&self) -> pb::LogEntry {
        pb::LogEntry {
            ts_ms: self.ts_ms,
            request_id: self.request_id.clone(),
            function: self.function.clone(),
            deployment: self.deployment.map(|d| d.to_string()).unwrap_or_default(),
            level: level_to_proto(self.level).into(),
            line: self.line.clone(),
            truncated: self.truncated,
            ..Default::default()
        }
    }
}

fn level_to_proto(level: LogLevel) -> pb::LogLevel {
    match level {
        LogLevel::Debug => pb::LogLevel::LOG_LEVEL_DEBUG,
        LogLevel::Log => pb::LogLevel::LOG_LEVEL_LOG,
        LogLevel::Info => pb::LogLevel::LOG_LEVEL_INFO,
        LogLevel::Warn => pb::LogLevel::LOG_LEVEL_WARN,
        LogLevel::Error => pb::LogLevel::LOG_LEVEL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogLine;

    fn output(lines: &[&str]) -> CallOutput {
        CallOutput {
            logs: lines
                .iter()
                .map(|l| LogLine {
                    level: LogLevel::Log,
                    line: (*l).to_string(),
                    truncated: false,
                })
                .collect(),
            dropped: 0,
        }
    }

    fn call<'a>(function: &'a str, request_id: &'a str) -> CallInfo<'a> {
        CallInfo {
            request_id: Some(request_id),
            function,
            deployment: Ulid::nil(),
        }
    }

    #[test]
    fn reads_filter_by_function_and_time() {
        let ring = Ring::new(10);
        ring.record(&call("a:f", "r1"), Ok(&output(&["one", "two"])));
        ring.record(&call("b:g", "r2"), Ok(&output(&["three"])));
        let all = ring.read(0, &TailFilter::default(), 100);
        assert_eq!(
            all.entries
                .iter()
                .map(|e| e.line.as_str())
                .collect::<Vec<_>>(),
            ["one", "two", "three"]
        );
        assert!(all.caught_up);
        assert_eq!(all.next, 3);
        let only_b = TailFilter {
            function: Some("b:g".into()),
            ..TailFilter::default()
        };
        let b = ring.read(0, &only_b, 100);
        assert_eq!(b.entries.len(), 1);
        assert_eq!(b.entries[0].request_id, "r2");
        let later = TailFilter {
            since_ms: Some(now_ms() + 60_000),
            ..TailFilter::default()
        };
        assert!(ring.read(0, &later, 100).entries.is_empty());
        let page = ring.read(0, &TailFilter::default(), 2);
        assert_eq!(
            (page.entries.len(), page.next, page.caught_up),
            (2, 2, false)
        );
    }

    #[test]
    fn only_function_failures_are_recorded() {
        let ring = Ring::new(10);
        ring.record(
            &call("a:f", "r1"),
            Err(&LiveError::FunctionError("Uncaught Error: no".into())),
        );
        ring.record(
            &call("a:f", "r2"),
            Err(&LiveError::Internal("a raw key 0x01".into())),
        );
        ring.record(
            &call("a:f", "r3"),
            Err(&LiveError::invalid("bad arguments")),
        );
        let read = ring.read(0, &TailFilter::default(), 100);
        assert_eq!(read.entries.len(), 1, "{read:?}");
        assert_eq!(read.entries[0].level, LogLevel::Error);
        assert_eq!(read.entries[0].request_id, "r1");
        assert!(read.entries[0].line.contains("Uncaught Error: no"));
    }

    #[test]
    fn long_lines_are_cut_and_dropped_lines_counted() {
        let ring = Ring::new(10);
        let long = "é".repeat(MAX_LINE_BYTES);
        let mut out = output(&[&long]);
        out.dropped = 3;
        ring.record(
            &CallInfo {
                request_id: None,
                function: "a:f",
                deployment: Ulid::from_parts(1, 2),
            },
            Ok(&out),
        );
        let read = ring.read(0, &TailFilter::default(), 100);
        assert_eq!(read.entries.len(), 2);
        assert!(read.entries[0].truncated);
        assert!(read.entries[0].line.len() <= MAX_LINE_BYTES);
        assert_eq!(read.entries[1].level, LogLevel::Warn);
        assert!(read.entries[1].line.starts_with("3 more console lines"));
        assert_eq!(read.entries[0].request_id, read.entries[1].request_id);
        assert!(!read.entries[0].request_id.is_empty());
        assert_eq!(read.entries[0].deployment, Some(Ulid::from_parts(1, 2)));
        // A call that wrote nothing records nothing.
        ring.record(&call("a:f", "r"), Ok(&CallOutput::default()));
        assert_eq!(ring.len(), 2);
    }
}
