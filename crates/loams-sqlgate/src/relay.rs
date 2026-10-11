//! The gate's relay between a logged-in client and its TiDB connection
//! (plan SQ1 Task 4, fix round 1: I2, M1, M2, M3, M7).
//!
//! - Both directions run in one task under `select!`: when either ends
//!   (EOF, an error, a protocol violation, the idle timeout or the gate's
//!   shutdown), both connections close and the database slot is freed.
//! - Client → TiDB is read frame by frame. A command must start at
//!   sequence id 0; continuation frames of a message over `MAX_FRAME` are
//!   tracked and must follow in order. A command outside
//!   [`crate::codec::command::RELAYED`], a refused one, and a `KILL`
//!   statement are answered by the gate and never forwarded; the bytes of
//!   a discarded command are zeroed.
//! - TiDB → client is copied packet by packet, so a gate ERR always lands
//!   on a packet boundary.
//! - A suspend (plan SQ1 Task 5) asks the session to close through its
//!   lease: once idle (quiet, no command awaiting TiDB) it gets ERR 1053
//!   and is closed; a kill closes it at once (with 1053 if it is between
//!   commands). Closing the TiDB connection rolls back an open transaction.
//! - A migration (plan SQ1 Task 6) asks it to move to another member
//!   ([`Close::Migrate`]). TiDB → client follows the response framing
//!   ([`ResponseTracker`]); at the first point where no response is
//!   pending and no transaction or cursor is open, it takes the upstream
//!   writer (so no client command is forwarded meanwhile) and runs
//!   [`crate::migrate::migrate`]. A failed move closes the session with
//!   1053.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;

use crate::codec::command::{Command, ErrPacket, KILL_SCAN, classify, starts_with_kill};
use crate::codec::handshake::Capabilities;
use crate::codec::packet::{HEADER_LEN, MAX_FRAME, encode};
use crate::codec::response::{FRAME_HEAD, ResponseTracker};
use crate::limits::{ActivitySink, Slot};
use crate::migrate::{MigrateError, Outcome, Reconnect, member_addr, migrate};
use crate::upstream::{Close, SessionLease, Upstream, UpstreamMember};
use crate::wire::{ClientStream, zero};

/// After shutdown, a session this quiet is closed.
const QUIET: Duration = Duration::from_millis(250);

/// How often a session asked to close checks whether it is idle.
const IDLE_POLL: Duration = Duration::from_millis(50);

/// What a session closed by its lifecycle gets (MySQL's server shutdown).
fn err_shutdown() -> ErrPacket {
    ErrPacket::new(1053, *b"08S01", "Server shutdown in progress")
}

/// `COM_QUERY` and `COM_STMT_PREPARE`: checked for `KILL`.
const QUERY: u8 = 0x03;
const PREPARE: u8 = 0x16;
/// `COM_SET_OPTION`: its option is tracked for the migration.
const SET_OPTION: u8 = 0x1b;

/// The upstream writer, shared by the client → TiDB direction (one lock per
/// client message) and the migration (which takes it out while it moves the
/// session; `None` only after a failed move).
type UpstreamWriter = Mutex<Option<WriteHalf<Upstream>>>;

/// Why the TiDB → client direction ended.
enum Ended {
    /// An I/O error or EOF on either side.
    Io,
    /// A migration failed: the client gets 1053.
    Migration(MigrateError),
}

impl From<io::Error> for Ended {
    fn from(_: io::Error) -> Self {
        Ended::Io
    }
}

/// What one relay needs besides its two connections.
pub(crate) struct Relay {
    pub branch: String,
    pub activity: Arc<dyn ActivitySink>,
    pub slot: Slot,
    pub idle_timeout: Duration,
    pub shutdown: watch::Receiver<bool>,
    /// From `EnsureRunning`: a suspend closes the session through it.
    pub lease: Box<dyn SessionLease>,
    /// The capabilities of the upstream connection (they decide the
    /// response framing).
    pub upstream_caps: Capabilities,
    /// How to log in to another member; `None` never migrates.
    pub reconnect: Option<Reconnect>,
}

/// The last time a byte moved, in milliseconds since `base`, and whether
/// the client spoke last (a command awaits TiDB's answer).
struct Clock {
    base: Instant,
    last: AtomicU64,
    awaiting: AtomicBool,
}

impl Clock {
    fn touch(&self) {
        let ms = u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.last.store(ms, Ordering::Relaxed);
    }

    fn idle_for(&self) -> Duration {
        self.base
            .elapsed()
            .saturating_sub(Duration::from_millis(self.last.load(Ordering::Relaxed)))
    }
}

/// Relays until either side ends, then closes both.
pub(crate) async fn relay(client: ClientStream, upstream: Upstream, r: Relay) {
    let Relay {
        branch,
        activity,
        slot,
        idle_timeout,
        mut shutdown,
        mut lease,
        upstream_caps,
        reconnect,
    } = r;
    let (cr, cw) = tokio::io::split(client);
    let (ur, uw) = tokio::io::split(upstream);
    let cw = Mutex::new(cw);
    let uw: UpstreamWriter = Mutex::new(Some(uw));
    let tracker = std::sync::Mutex::new(ResponseTracker::new(
        upstream_caps.contains(Capabilities::DEPRECATE_EOF),
        upstream_caps.contains(Capabilities::MULTI_STATEMENTS),
    ));
    let (moves, moves_rx) = watch::channel::<Option<Vec<UpstreamMember>>>(None);
    let clock = Clock {
        base: Instant::now(),
        last: AtomicU64::new(0),
        awaiting: AtomicBool::new(false),
    };
    let idle = async {
        loop {
            let left = idle_timeout.saturating_sub(clock.idle_for());
            if left.is_zero() {
                return;
            }
            tokio::time::sleep(left).await;
        }
    };
    // On shutdown a session closes once quiet (no bytes either way for
    // QUIET) and not busy: a command the client sent last is still waiting
    // for TiDB's answer (a long query). The gate's drain deadline ends the
    // others (fix round 2, N4).
    let stop = async {
        if shutdown.wait_for(|stopped| *stopped).await.is_err() {
            // The gate is gone: nothing will ask us to stop.
            std::future::pending::<()>().await;
        }
        loop {
            let left = QUIET.saturating_sub(clock.idle_for());
            if left.is_zero() && !clock.awaiting.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(left.max(Duration::from_millis(50))).await;
        }
    };
    // A suspend: `WhenIdle` closes the session once quiet and not busy
    // (the rule above), `Now` at once; `Open` cancels (an aborted suspend).
    // A migration request goes to the TiDB → client direction, which moves
    // the session at its next idle point.
    let lifecycle = async {
        let mut want = Close::Open;
        loop {
            let polling = want == Close::WhenIdle;
            tokio::select! {
                c = lease.closing() => match c {
                    Close::Migrate(to) => {
                        moves.send_replace(Some(to));
                    }
                    c => want = c,
                },
                () = tokio::time::sleep(IDLE_POLL), if polling => {}
            }
            let idle = clock.idle_for() >= QUIET && !clock.awaiting.load(Ordering::SeqCst);
            match want {
                Close::Now => return Close::Now,
                Close::WhenIdle if idle => return Close::WhenIdle,
                Close::WhenIdle | Close::Open | Close::Migrate(_) => {}
            }
        }
    };
    let mut farewell = None;
    let to_client = Session {
        uw: &uw,
        cw: &cw,
        clock: &clock,
        tracker: &tracker,
        reconnect: reconnect.as_ref(),
        branch: &branch,
    };
    tokio::select! {
        _ = client_to_upstream(cr, &uw, &cw, &branch, &*activity, &clock, &tracker) => {}
        ended = to_client.upstream_to_client(ur, moves_rx) => {
            if let Err(Ended::Migration(e)) = ended {
                tracing::warn!(branch = %branch, error = %e, "session migration failed");
                farewell = Some(err_shutdown());
            }
        }
        () = idle => tracing::debug!("idle timeout"),
        () = stop => {}
        close = lifecycle => {
            tracing::debug!(branch = %branch, ?close, "closed by the lifecycle");
            // Mid-command there is no packet boundary for an ERR.
            if close == Close::WhenIdle || !clock.awaiting.load(Ordering::SeqCst) {
                farewell = Some(err_shutdown());
            }
        }
    }
    if let Some(err) = farewell {
        let mut out = Vec::new();
        let mut seq = 0;
        encode(&err.encode(), &mut seq, &mut out);
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let mut w = cw.lock().await;
            let _ = w.write_all(&out).await;
            let _ = w.flush().await;
        })
        .await;
    }
    // Close the client cleanly (TLS close_notify) without waiting long.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        cw.lock().await.shutdown().await
    })
    .await;
    drop(slot);
    drop(lease);
}

fn frame_len(header: &[u8; HEADER_LEN]) -> usize {
    usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16
}

/// What the TiDB → client direction shares with the rest of the relay.
struct Session<'a> {
    uw: &'a UpstreamWriter,
    cw: &'a Mutex<WriteHalf<ClientStream>>,
    clock: &'a Clock,
    tracker: &'a std::sync::Mutex<ResponseTracker>,
    reconnect: Option<&'a Reconnect>,
    branch: &'a str,
}

/// What one migration attempt did.
enum Attempt {
    /// Moved, or already on a target member: the request is done.
    Done,
    /// The session cannot move now (tracker or TiDB): try again after its
    /// next command.
    Later,
    /// A command arrived first: its answer comes before the next try.
    Busy,
}

fn tracker_lock(
    t: &std::sync::Mutex<ResponseTracker>,
) -> std::sync::MutexGuard<'_, ResponseTracker> {
    t.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Session<'_> {
    /// TiDB → client, one whole packet per lock of the client's writer;
    /// between responses, moves the session when asked.
    async fn upstream_to_client(
        &self,
        mut ur: ReadHalf<Upstream>,
        mut moves: watch::Receiver<Option<Vec<UpstreamMember>>>,
    ) -> Result<(), Ended> {
        let mut header = [0u8; HEADER_LEN];
        let mut buf = vec![0u8; 64 * 1024];
        let mut target: Option<Vec<UpstreamMember>> = None;
        // The command count when the session last could not move: the
        // next try waits for a newer command.
        let mut later_at: Option<u64> = None;
        let mut moves_open = true;
        loop {
            let mut filled = 0;
            while filled < HEADER_LEN {
                if filled == 0
                    && let (Some(to), Some(r)) = (&target, self.reconnect)
                {
                    let due = {
                        let t = tracker_lock(self.tracker);
                        t.idle() && later_at != Some(t.commands())
                    };
                    if due {
                        let (back, attempt) = self.try_move(ur, to, r).await?;
                        ur = back;
                        match attempt {
                            Attempt::Done => target = None,
                            Attempt::Later => {
                                later_at = Some(tracker_lock(self.tracker).commands());
                            }
                            Attempt::Busy => {}
                        }
                    }
                }
                // Both arms are cancel-safe: `read` loses no bytes, and the
                // header's progress lives outside the loop.
                tokio::select! {
                    n = ur.read(&mut header[filled..]) => {
                        let n = n?;
                        if n == 0 {
                            return Err(Ended::Io);
                        }
                        filled += n;
                    }
                    changed = moves.changed(), if filled == 0 && moves_open => {
                        if changed.is_err() {
                            moves_open = false;
                        } else {
                            target = moves.borrow_and_update().clone();
                            later_at = None;
                        }
                    }
                }
            }
            self.clock.touch();
            let len = frame_len(&header);
            let head = len.min(FRAME_HEAD);
            ur.read_exact(&mut buf[..head]).await?;
            tracker_lock(self.tracker).frame(len, &buf[..head]);
            let mut w = self.cw.lock().await;
            w.write_all(&header).await?;
            w.write_all(&buf[..head]).await?;
            let mut remaining = len - head;
            while remaining > 0 {
                let n = remaining.min(buf.len());
                ur.read_exact(&mut buf[..n]).await?;
                w.write_all(&buf[..n]).await?;
                remaining -= n;
            }
            w.flush().await?;
            drop(w);
            self.clock.awaiting.store(false, Ordering::SeqCst);
            self.clock.touch();
        }
    }

    /// One migration attempt, holding the upstream writer so no client
    /// command reaches TiDB meanwhile. Returns the (possibly new) reader.
    async fn try_move(
        &self,
        ur: ReadHalf<Upstream>,
        to: &[UpstreamMember],
        r: &Reconnect,
    ) -> Result<(ReadHalf<Upstream>, Attempt), Ended> {
        let mut writer = self.uw.lock().await;
        let multi_statements = {
            let t = tracker_lock(self.tracker);
            if !t.idle() {
                return Ok((ur, Attempt::Busy));
            }
            if let Err(why) = t.movable() {
                tracing::debug!(branch = %self.branch, ?why, "session cannot move yet");
                return Ok((ur, Attempt::Later));
            }
            t.multi_statements()
        };
        let Some(uw) = writer.take() else {
            return Err(Ended::Io);
        };
        let old = ur.unsplit(uw);
        let from = member_addr(&old);
        if from.is_some_and(|a| to.iter().any(|m| m.addr == a)) {
            let (ur, uw) = tokio::io::split(old);
            *writer = Some(uw);
            return Ok((ur, Attempt::Done));
        }
        let (stream, attempt) = match migrate(old, to, r, multi_statements).await {
            Ok(Outcome::Moved(new)) => {
                tracing::info!(branch = %self.branch, ?from, to = ?member_addr(&new), "session moved");
                (new, Attempt::Done)
            }
            Ok(Outcome::Stayed(old, reason)) => {
                // Usually an open transaction or cursor; a missing signing
                // key pair (`session-token-signing-*`) also lands here.
                tracing::info!(branch = %self.branch, %reason, "TiDB will not move the session yet");
                (old, Attempt::Later)
            }
            Err(e) => return Err(Ended::Migration(e)),
        };
        let (ur, uw) = tokio::io::split(stream);
        *writer = Some(uw);
        Ok((ur, attempt))
    }
}

/// Whether TiDB answers the command (`COM_STMT_SEND_LONG_DATA`,
/// `COM_STMT_CLOSE` and `COM_QUIT` get no answer).
fn expects_answer(command: Command) -> bool {
    !matches!(command, Command::Quit | Command::Other(0x18 | 0x19))
}

/// A protocol violation by the client: the connection is closed.
fn violation(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// Reads and drops `n` bytes, zeroing them (they may hold credentials,
/// e.g. a refused `COM_CHANGE_USER`).
#[doc(hidden)]
pub async fn discard<R: AsyncRead + Unpin>(
    cr: &mut R,
    mut n: usize,
    buf: &mut [u8],
) -> io::Result<()> {
    while n > 0 {
        let k = n.min(buf.len());
        let read = cr.read_exact(&mut buf[..k]).await;
        zero(&mut buf[..k]);
        read?;
        n -= k;
    }
    Ok(())
}

/// The state of the client's current message.
enum Message {
    /// Between commands: the next frame starts one (sequence id 0).
    Start,
    /// A message over `MAX_FRAME` continues with frame `next`; `refused`
    /// holds the gate's answer when the message is being dropped.
    Continues {
        next: u8,
        refused: Option<ErrPacket>,
    },
}

async fn client_to_upstream(
    mut cr: ReadHalf<ClientStream>,
    uw: &UpstreamWriter,
    cw: &Mutex<WriteHalf<ClientStream>>,
    branch: &str,
    activity: &dyn ActivitySink,
    clock: &Clock,
    tracker: &std::sync::Mutex<ResponseTracker>,
) -> io::Result<()> {
    let mut header = [0u8; HEADER_LEN];
    let mut buf = vec![0u8; 64 * 1024];
    let mut state = Message::Start;
    // The command of the message being read (it may span frames).
    let mut current = None;
    // The upstream writer, held from a message's first frame to its last,
    // so a migration never starts inside a client message.
    let mut held = None;
    loop {
        cr.read_exact(&mut header).await?;
        clock.touch();
        let len = frame_len(&header);
        let seq = header[3];
        let more = len == MAX_FRAME;
        let (refused, first, peeked) = match state {
            Message::Continues { next, refused } => {
                if seq != next {
                    return Err(violation("out-of-order continuation frame"));
                }
                (refused, None, 0)
            }
            Message::Start => {
                if seq != 0 {
                    return Err(violation("a command must start at sequence id 0"));
                }
                if len == 0 {
                    return Err(violation("empty command"));
                }
                // The first byte, and for statements the first KILL_SCAN
                // bytes, decide whether the command is relayed.
                cr.read_exact(&mut buf[..1]).await?;
                let more_bytes = match buf[0] {
                    QUERY | PREPARE => KILL_SCAN,
                    SET_OPTION => 2,
                    _ => 0,
                };
                let n = (len - 1).min(more_bytes);
                cr.read_exact(&mut buf[1..=n]).await?;
                let peek = 1 + n;
                let command = classify(&buf[..1]);
                let mut refused = command.and_then(Command::refusal);
                if refused.is_none()
                    && matches!(buf[0], QUERY | PREPARE)
                    && starts_with_kill(&buf[1..peek], !more && peek == len)
                {
                    refused = Some(ErrPacket::new(
                        1235,
                        *b"42000",
                        "Loams SQL does not support KILL",
                    ));
                }
                if refused.is_none() && command.is_some_and(Command::is_activity) {
                    activity.command(branch);
                }
                (refused, command, peek)
            }
        };
        let remaining = len - peeked;
        match refused {
            Some(err) => {
                zero(&mut buf[..peeked]);
                discard(&mut cr, remaining, &mut buf).await?;
                if more {
                    state = Message::Continues {
                        next: seq.wrapping_add(1),
                        refused: Some(err),
                    };
                    continue;
                }
                let mut out = Vec::new();
                let mut s = seq.wrapping_add(1);
                encode(&err.encode(), &mut s, &mut out);
                let mut w = cw.lock().await;
                w.write_all(&out).await?;
                w.flush().await?;
                state = Message::Start;
            }
            None => {
                let mut guard = match held.take() {
                    Some(g) => g,
                    None => uw.lock().await,
                };
                let Some(w) = guard.as_mut() else {
                    return Err(io::ErrorKind::BrokenPipe.into());
                };
                if first.is_some() {
                    tracker_lock(tracker).command(&buf[..peeked]);
                }
                w.write_all(&header).await?;
                w.write_all(&buf[..peeked]).await?;
                let mut left = remaining;
                while left > 0 {
                    let n = left.min(buf.len());
                    cr.read_exact(&mut buf[..n]).await?;
                    w.write_all(&buf[..n]).await?;
                    left -= n;
                }
                w.flush().await?;
                if more {
                    held = Some(guard);
                }
                if first.is_some() {
                    current = first;
                }
                if !more && current.is_some_and(expects_answer) {
                    clock.awaiting.store(true, Ordering::SeqCst);
                }
                state = if more {
                    Message::Continues {
                        next: seq.wrapping_add(1),
                        refused: None,
                    }
                } else {
                    Message::Start
                };
                if first == Some(Command::Quit) && !more {
                    return Ok(());
                }
            }
        }
    }
}
