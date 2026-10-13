//! `WatchProject` (design §46 §4.1; PG2 Task 11, which R9.1 left it to): a
//! snapshot of the project, its branches, endpoints and computes, then
//! their changes, then a heartbeat every 15 s while nothing changes;
//! resumable from a cursor.
//!
//! - **How changes are found.** The watch reads the project's records every
//!   [`WatchOptions::poll`] and compares each record's version with what it
//!   last sent (versions never repeat, R3.5). Changes between two reads
//!   collapse to the latest record, as the store's own watch does (R3.7).
//!   A compute is the current compute of one of the project's endpoints.
//! - **Cursors.** Every answer carries a cursor naming exactly what the
//!   client holds after it: a digest of every `(kind, id, version)` it has
//!   been sent. A watch resumed with a cursor that names the current state
//!   sends no snapshot and goes on from there; any other cursor (older, or
//!   from another state) gets a snapshot with `snapshot_reset`, which
//!   replaces everything the client held. Nothing is kept on the server.
//! - **The end.** A deleted project ends the watch with
//!   `project_not_found`. A read that fails is logged and tried at the next
//!   poll.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use futures::Stream;
use sha2::{Digest, Sha256};

use super::branches::{BranchView, view};
use super::endpoints::EndpointView;
use super::{NeonRead, PgService, ServiceError};
use crate::model::{
    BranchPrefix, BranchRec, ComputeKey, ComputeRec, EndpointPrefix, EndpointRec, ProjectRec,
};
use crate::store::{PgControlStore, Versioned};

/// How often a quiet watch sends a heartbeat (§46 §4.1, AP0).
pub const HEARTBEAT: Duration = Duration::from_secs(15);

/// How a watch runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchOptions {
    /// How often the project's records are read (default 1 s).
    pub poll: Duration,
    /// [`HEARTBEAT`] by default.
    pub heartbeat: Duration,
}

impl Default for WatchOptions {
    fn default() -> Self {
        WatchOptions {
            poll: Duration::from_secs(1),
            heartbeat: HEARTBEAT,
        }
    }
}

/// What a watched record is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceKind {
    Project,
    Branch,
    Endpoint,
    Compute,
}

/// Everything a watch starts with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSnapshot {
    pub project: Versioned<ProjectRec>,
    pub branches: Vec<BranchView>,
    pub endpoints: Vec<EndpointView>,
    pub computes: Vec<Versioned<ComputeRec>>,
}

/// One answer's event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    Snapshot(ProjectSnapshot),
    Project(Versioned<ProjectRec>),
    Branch(BranchView),
    Endpoint(EndpointView),
    Compute(Versioned<ComputeRec>),
    /// A branch, endpoint or compute that is gone.
    Removed {
        kind: ResourceKind,
        id: String,
    },
    Heartbeat,
}

/// One answer: its event, the cursor of what the client holds after it,
/// and whether a snapshot replaces everything held (a stale cursor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchItem {
    pub event: WatchEvent,
    pub cursor: String,
    pub snapshot_reset: bool,
}

/// What the client holds: each record's version.
type Held = BTreeMap<(ResourceKind, String), u64>;

/// The cursor of `held`: `w1.` and 32 hex characters.
fn cursor(held: &Held) -> String {
    let mut h = Sha256::new();
    for ((kind, id), version) in held {
        h.update([*kind as u8]);
        h.update(id.as_bytes());
        h.update([0]);
        h.update(version.to_be_bytes());
    }
    format!("w1.{}", hex::encode(&h.finalize()[..16]))
}

/// The project's records, as one read sees them.
struct Current {
    project: Versioned<ProjectRec>,
    branches: Vec<Versioned<BranchRec>>,
    endpoints: Vec<Versioned<EndpointRec>>,
    computes: Vec<Versioned<ComputeRec>>,
}

impl Current {
    fn held(&self) -> Held {
        let mut held = Held::new();
        held.insert(
            (ResourceKind::Project, self.project.record.id.clone()),
            self.project.version,
        );
        for b in &self.branches {
            held.insert((ResourceKind::Branch, b.record.id.clone()), b.version);
        }
        for e in &self.endpoints {
            held.insert((ResourceKind::Endpoint, e.record.id.clone()), e.version);
        }
        for c in &self.computes {
            held.insert((ResourceKind::Compute, c.record.id.clone()), c.version);
        }
        held
    }

    fn compute(&self, id: Option<&str>) -> Option<ComputeRec> {
        let id = id?;
        self.computes
            .iter()
            .find(|c| c.record.id == id)
            .map(|c| c.record.clone())
    }

    fn endpoint_view(&self, e: &Versioned<EndpointRec>) -> EndpointView {
        EndpointView {
            endpoint: e.clone(),
            compute: self.compute(e.record.compute_id.as_deref()),
        }
    }

    fn snapshot(&self) -> ProjectSnapshot {
        ProjectSnapshot {
            project: self.project.clone(),
            branches: self
                .branches
                .iter()
                .map(|b| view(b.clone(), &self.project.record))
                .collect(),
            endpoints: self
                .endpoints
                .iter()
                .map(|e| self.endpoint_view(e))
                .collect(),
            computes: self.computes.clone(),
        }
    }

    /// The events that take a client holding `held` to this state, in
    /// order (changes, then removals), each with the cursor after it.
    fn changes(&self, held: &mut Held) -> Vec<WatchItem> {
        let mut out = Vec::new();
        let mut send = |held: &mut Held, key: (ResourceKind, String), version, event| {
            if held.get(&key) != Some(&version) {
                held.insert(key, version);
                out.push(WatchItem {
                    event,
                    cursor: cursor(held),
                    snapshot_reset: false,
                });
            }
        };
        send(
            held,
            (ResourceKind::Project, self.project.record.id.clone()),
            self.project.version,
            WatchEvent::Project(self.project.clone()),
        );
        for b in &self.branches {
            send(
                held,
                (ResourceKind::Branch, b.record.id.clone()),
                b.version,
                WatchEvent::Branch(view(b.clone(), &self.project.record)),
            );
        }
        for e in &self.endpoints {
            send(
                held,
                (ResourceKind::Endpoint, e.record.id.clone()),
                e.version,
                WatchEvent::Endpoint(self.endpoint_view(e)),
            );
        }
        for c in &self.computes {
            send(
                held,
                (ResourceKind::Compute, c.record.id.clone()),
                c.version,
                WatchEvent::Compute(c.clone()),
            );
        }
        let now = self.held();
        let gone: Vec<(ResourceKind, String)> = held
            .keys()
            .filter(|k| !now.contains_key(*k))
            .cloned()
            .collect();
        // Computes first, then endpoints, then branches: children before
        // what they hang from.
        for kind in [
            ResourceKind::Compute,
            ResourceKind::Endpoint,
            ResourceKind::Branch,
        ] {
            for key in gone.iter().filter(|(k, _)| *k == kind) {
                held.remove(key);
                out.push(WatchItem {
                    event: WatchEvent::Removed {
                        kind,
                        id: key.1.clone(),
                    },
                    cursor: cursor(held),
                    snapshot_reset: false,
                });
            }
        }
        out
    }
}

/// A watch's state between answers.
struct Watch<N> {
    service: PgService<N>,
    namespace: String,
    project_id: String,
    options: WatchOptions,
    /// The cursor to resume from, until the first read.
    resume: Option<String>,
    /// A read has been tried: the next waits a poll first.
    polled: bool,
    /// The first read succeeded, and the snapshot or resume was handled.
    started: bool,
    held: Held,
    pending: VecDeque<WatchItem>,
    quiet_since: tokio::time::Instant,
    done: bool,
}

impl<N: NeonRead> PgService<N> {
    /// The project's records now; `Ok(None)` when the project is gone.
    async fn read_current(
        &self,
        namespace: &str,
        project_id: &str,
    ) -> Result<Option<Current>, ServiceError> {
        let project = match self.project(namespace, project_id).await {
            Ok(p) => p,
            Err(e) if e.reason == super::Reason::ProjectNotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let branches = self
            .all::<BranchRec>(&BranchPrefix {
                project_id: project_id.into(),
            })
            .await?;
        let endpoints = self
            .all::<EndpointRec>(&EndpointPrefix {
                project_id: project_id.into(),
            })
            .await?;
        let mut computes = Vec::new();
        for e in &endpoints {
            if let Some(id) = &e.record.compute_id
                && let Some(c) = self
                    .store
                    .get::<ComputeRec>(&ComputeKey { id: id.clone() })
                    .await?
            {
                computes.push(c);
            }
        }
        computes.sort_by(|a, b| a.record.id.cmp(&b.record.id));
        Ok(Some(Current {
            project,
            branches,
            endpoints,
            computes,
        }))
    }

    /// `WatchProject`: the project must exist when the watch starts; the
    /// stream then runs until the client goes or the project is deleted
    /// (see the module docs).
    ///
    /// # Errors
    ///
    /// `project_not_found`; `invalid_argument` for a bad id or namespace.
    pub async fn watch_project(
        &self,
        namespace: &str,
        project_id: &str,
        resume_cursor: &str,
        options: WatchOptions,
    ) -> Result<impl Stream<Item = Result<WatchItem, ServiceError>> + Send + 'static, ServiceError>
    where
        N: Clone + Send + Sync + 'static,
    {
        self.project(namespace, project_id).await?;
        let watch = Watch {
            service: self.clone(),
            namespace: namespace.to_owned(),
            project_id: project_id.to_owned(),
            options,
            resume: (!resume_cursor.is_empty()).then(|| resume_cursor.to_owned()),
            polled: false,
            started: false,
            held: Held::new(),
            pending: VecDeque::new(),
            quiet_since: tokio::time::Instant::now(),
            done: false,
        };
        Ok(futures::stream::unfold(watch, |mut w| async move {
            let item = w.next().await?;
            Some((item, w))
        }))
    }
}

impl<N: NeonRead> Watch<N> {
    /// The next answer; `None` when the watch is over.
    async fn next(&mut self) -> Option<Result<WatchItem, ServiceError>> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                self.quiet_since = tokio::time::Instant::now();
                return Some(Ok(item));
            }
            if self.done {
                return None;
            }
            if self.polled {
                tokio::time::sleep(self.options.poll).await;
            }
            self.polled = true;
            let current = match self
                .service
                .read_current(&self.namespace, &self.project_id)
                .await
            {
                Ok(Some(c)) => c,
                Ok(None) => {
                    self.done = true;
                    return Some(Err(ServiceError::project_not_found(&self.project_id)));
                }
                Err(e) => {
                    // `started` stays as it was: a failed first read still
                    // leads to the snapshot or resume (PR #453 review).
                    tracing::warn!(project = %self.project_id, error = %e, "watch: a read failed; retrying");
                    continue;
                }
            };
            if !self.started {
                self.started = true;
                let held = current.held();
                let here = cursor(&held);
                let resumed = self.resume.take();
                if resumed.as_deref() != Some(here.as_str()) {
                    self.pending.push_back(WatchItem {
                        event: WatchEvent::Snapshot(current.snapshot()),
                        cursor: here,
                        snapshot_reset: resumed.is_some(),
                    });
                }
                self.held = held;
                continue;
            }
            self.pending.extend(current.changes(&mut self.held));
            if self.pending.is_empty() && self.quiet_since.elapsed() >= self.options.heartbeat {
                self.pending.push_back(WatchItem {
                    event: WatchEvent::Heartbeat,
                    cursor: cursor(&self.held),
                    snapshot_reset: false,
                });
            }
        }
    }
}
