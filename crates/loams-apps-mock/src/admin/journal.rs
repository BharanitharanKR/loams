//! The audit log and `loams.admin.v1.AuditService` (API2 Task 7).
//!
//! The log starts with the console seed's events and grows by one event per
//! mutation made over Connect. `WatchAuditEvents` keeps AP0 Ruling 3's shape,
//! as `WatchApprovals` does: a snapshot first, then each new event as an
//! `append`, a heartbeat every period, a cursor on every response, a replay
//! of the missed events for a cursor the log still holds, and a fresh
//! snapshot with `snapshot_reset` for one it does not.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use super::{ConsoleApi, invalid, page};
use crate::proto::loams::admin::v1::__buffa::oneof::watch_audit_events_response::Event;
use crate::proto::loams::admin::v1::{
    AuditEvent, AuditOutcome, AuditService, AuditSnapshot, ListAuditEventsRequest,
    ListAuditEventsResponse, ResourceRef, SubjectKind, SubjectRef, WatchAuditEventsRequest,
    WatchAuditEventsResponse,
};
use crate::seed::ts;
use crate::services::watch::heartbeat_timer;

/// How many Connect-made events a cursor can resume across; an older cursor
/// gets a fresh snapshot with `snapshot_reset`.
const LOG_CAPACITY: usize = 1_024;

/// The snapshot's size when the request leaves `snapshot_size` at 0, and the
/// most it may ask for.
const DEFAULT_SNAPSHOT: usize = 50;
const MAX_SNAPSHOT: usize = 1_000;

/// One event the log recorded after the seed, numbered by `seq` from 1.
#[derive(Debug, Clone)]
pub(crate) struct Recorded {
    seq: u64,
    event: AuditEvent,
}

#[derive(Debug)]
struct Log {
    /// The seed's events, oldest first; they predate every cursor.
    seeded: Vec<AuditEvent>,
    /// Events recorded since, oldest first, at most `LOG_CAPACITY`.
    recorded: VecDeque<Recorded>,
    /// The last `seq` handed out; 0 before the first Connect mutation.
    seq: u64,
}

impl Log {
    /// Every event, newest first.
    fn newest_first(&self) -> impl Iterator<Item = &AuditEvent> {
        self.recorded
            .iter()
            .rev()
            .map(|r| &r.event)
            .chain(self.seeded.iter().rev())
    }

    /// The events after `cursor`, oldest first, if the log still holds them.
    fn since(&self, cursor: u64) -> Option<Vec<Recorded>> {
        if cursor > self.seq {
            return None;
        }
        let oldest_held = self.recorded.front().map_or(self.seq, |r| r.seq - 1);
        if cursor < oldest_held {
            return None;
        }
        Some(
            self.recorded
                .iter()
                .filter(|r| r.seq > cursor)
                .cloned()
                .collect(),
        )
    }
}

/// The audit log: the seed's events and every Connect mutation's.
#[derive(Debug)]
pub(crate) struct Journal {
    log: Mutex<Log>,
    changes: broadcast::Sender<Recorded>,
    heartbeat: Duration,
}

impl Journal {
    /// `seed` is newest first, as the console seed and `GET /api/v1/audit`
    /// list it.
    pub(crate) fn new(mut seed: Vec<AuditEvent>, heartbeat: Duration) -> Self {
        seed.reverse();
        Self {
            log: Mutex::new(Log {
                seeded: seed,
                recorded: VecDeque::new(),
                seq: 0,
            }),
            changes: broadcast::channel(LOG_CAPACITY).0,
            heartbeat,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Records `event`, numbering it, and tells every open watch.
    pub(crate) fn record(&self, mut event: AuditEvent) -> AuditEvent {
        let recorded = {
            let mut log = self.lock();
            log.seq += 1;
            event.id = format!("evt_c{:04}", log.seq);
            let recorded = Recorded {
                seq: log.seq,
                event,
            };
            log.recorded.push_back(recorded.clone());
            if log.recorded.len() > LOG_CAPACITY {
                log.recorded.pop_front();
            }
            recorded
        };
        // Sent after the lock, as the approval paths do; no watcher is fine.
        let _ = self.changes.send(recorded.clone());
        recorded.event
    }
}

/// An audit event for a mutation the signed-in person made over Connect.
///
/// The actor is the seed session's user, since every mutation the mock takes
/// is that person's (the REST mock has no other actor either).
pub(crate) fn event(
    api: &ConsoleApi,
    ctx: &RequestContext,
    action: &str,
    target: (&str, &str, &str),
    project: &str,
    environment: &str,
) -> AuditEvent {
    let user = api.seed.get("session").and_then(|s| s.get("user"));
    let field = |key: &str| {
        user.and_then(|u| u.get(key))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let (kind, id, name) = target;
    AuditEvent {
        at: ts(SystemTime::now()),
        actor: SubjectRef {
            kind: SubjectKind::SUBJECT_KIND_USER.into(),
            id: field("id"),
            name: field("name"),
            ..Default::default()
        }
        .into(),
        action: action.to_owned(),
        target: ResourceRef {
            kind: kind.to_owned(),
            id: id.to_owned(),
            name: name.to_owned(),
            ..Default::default()
        }
        .into(),
        project: project.to_owned(),
        environment: environment.to_owned(),
        ip: ctx
            .peer_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_default(),
        outcome: AuditOutcome::AUDIT_OUTCOME_SUCCESS.into(),
        ..Default::default()
    }
}

/// Which events a list or watch selects.
#[derive(Debug, Clone)]
struct Filter {
    project: String,
    actor: String,
}

impl Filter {
    fn matches(&self, event: &AuditEvent) -> bool {
        (self.project.is_empty() || event.project == self.project)
            && (self.actor.is_empty()
                || event
                    .actor
                    .as_option()
                    .is_some_and(|actor| actor.id == self.actor))
    }
}

fn cursor(seq: u64) -> String {
    format!("a{seq}")
}

fn parse_cursor(cursor: &str) -> Option<u64> {
    cursor.strip_prefix('a')?.parse().ok()
}

fn snapshot(log: &Log, filter: &Filter, size: usize, reset: bool) -> WatchAuditEventsResponse {
    WatchAuditEventsResponse {
        event: Some(Event::Snapshot(Box::new(AuditSnapshot {
            events: log
                .newest_first()
                .filter(|e| filter.matches(e))
                .take(size)
                .cloned()
                .collect(),
            ..Default::default()
        }))),
        cursor: cursor(log.seq),
        snapshot_reset: reset,
        ..Default::default()
    }
}

fn append(recorded: Recorded) -> WatchAuditEventsResponse {
    WatchAuditEventsResponse {
        event: Some(Event::Append(Box::new(recorded.event))),
        cursor: cursor(recorded.seq),
        ..Default::default()
    }
}

/// `loams.admin.v1.AuditService`.
pub(crate) struct Audit(pub(crate) Arc<ConsoleApi>);

impl AuditService for Audit {
    async fn list_audit_events(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, ListAuditEventsRequest>,
    ) -> ServiceResult<ListAuditEventsResponse> {
        let request = request.to_owned_message();
        let filter = Filter {
            project: request.project,
            actor: request.actor,
        };
        let events: Vec<AuditEvent> = self
            .0
            .journal
            .lock()
            .newest_first()
            .filter(|e| filter.matches(e))
            .cloned()
            .collect();
        let (events, next_page_token) = page(events, request.page_size, &request.page_token)?;
        Response::ok(ListAuditEventsResponse {
            events,
            next_page_token,
            ..Default::default()
        })
    }

    async fn watch_audit_events(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, WatchAuditEventsRequest>,
    ) -> ServiceResult<ServiceStream<WatchAuditEventsResponse>> {
        let request = request.to_owned_message();
        let size = match usize::try_from(request.snapshot_size) {
            Ok(0) => DEFAULT_SNAPSHOT,
            Ok(n) => n.min(MAX_SNAPSHOT),
            Err(_) => return Err(invalid("snapshot_size", "must not be negative")),
        };
        let filter = Filter {
            project: request.project,
            actor: request.actor,
        };
        let journal = &self.0.journal;
        // Subscribe under the lock, before reading, so no event falls between
        // the snapshot (or the replay) and the live stream.
        let (rx, queue, last_seq) = {
            let log = journal.lock();
            let rx = journal.changes.subscribe();
            let mut queue = VecDeque::new();
            let asked = !request.resume_cursor.is_empty();
            match parse_cursor(&request.resume_cursor).and_then(|c| log.since(c)) {
                Some(missed) if asked => {
                    queue.extend(
                        missed
                            .into_iter()
                            .filter(|r| filter.matches(&r.event))
                            .map(append),
                    );
                }
                _ => queue.push_back(snapshot(&log, &filter, size, asked)),
            }
            (rx, queue, log.seq)
        };
        let timer = heartbeat_timer(journal.heartbeat);
        let api = self.0.clone();
        let stream = futures::stream::unfold(
            (queue, rx, timer, last_seq, filter, api),
            move |(mut queue, mut rx, mut timer, mut last_seq, filter, api)| async move {
                loop {
                    if let Some(item) = queue.pop_front() {
                        return Some((Ok(item), (queue, rx, timer, last_seq, filter, api)));
                    }
                    tokio::select! {
                        change = rx.recv() => match change {
                            Ok(recorded) if recorded.seq > last_seq => {
                                last_seq = recorded.seq;
                                if filter.matches(&recorded.event) {
                                    queue.push_back(append(recorded));
                                }
                            }
                            Ok(_) => {}
                            Err(RecvError::Lagged(_)) => {
                                // Too far behind: a fresh snapshot.
                                let log = api.journal.lock();
                                last_seq = log.seq;
                                queue.push_back(snapshot(&log, &filter, size, true));
                            }
                            Err(RecvError::Closed) => return None,
                        },
                        _ = timer.tick() => {
                            queue.push_back(WatchAuditEventsResponse {
                                event: Some(Event::Heartbeat(Box::default())),
                                cursor: cursor(last_seq),
                                ..Default::default()
                            });
                        }
                    }
                }
            },
        );
        Response::stream_ok(stream)
    }
}
