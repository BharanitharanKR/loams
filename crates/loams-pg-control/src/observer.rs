//! The compute lifecycle seam (design §46 §5, D703; PG2 Task 12): every
//! compute's start, resize and end go to a [`ComputeLifecycleObserver`] as a
//! plain struct, as D549's `InvocationObserver`. There is no wire format, no
//! buffer and no persistence: an event a process dies before delivering is
//! lost. What turns events into records outside this repository links its
//! own observer in; the default here ([`DefaultObserver`]) does nothing but
//! count them.
//!
//! **When.** The endpoint reconciler sends each event after the batch that
//! makes it true has committed, so an event is sent at most once:
//! - `Started { cu }` when a compute's `compute_ctl` reports Postgres
//!   running and the endpoint is `running`;
//! - `Resized { cu }` when the autoscaler resized it (Task 15);
//! - `Suspended` when a compute was stopped on purpose: a suspend (asked,
//!   or idle), a restart or a token-age replacement (the old compute), the
//!   endpoint's removal;
//! - `Failed { reason }` when a compute crashed or did not start.
//!
//! Each compute ends with exactly one `Suspended` or `Failed`. That end may
//! come for a compute that never sent `Started` (stopped or failed while it
//! started). A compute ran from its `Started` to its end.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use crate::ids::{ComputeId, EndpointId, ProjectId};
use crate::model::{ComputeBinding, Cu};

/// What happened to a compute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleKind {
    Started {
        cu: Cu,
    },
    Resized {
        cu: Cu,
    },
    Suspended,
    /// The text names what failed; it holds no host, token or message from
    /// the compute.
    Failed {
        reason: String,
    },
}

/// One event of one compute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeLifecycleEvent {
    /// `pg-control` keeps no org yet (no record names one until MT's tenants
    /// repository): [`Lifecycle::org`].
    pub org: String,
    pub namespace: String,
    pub project: ProjectId,
    pub endpoint: EndpointId,
    pub compute: ComputeId,
    pub event: LifecycleKind,
    pub at: SystemTime,
}

/// Receives every compute's lifecycle events. Called on the reconciler's
/// task: it must not block.
pub trait ComputeLifecycleObserver: Send + Sync + 'static {
    fn on_event(&self, e: &ComputeLifecycleEvent);
}

/// How many events of each kind a [`DefaultObserver`] saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifecycleCounts {
    pub started: u64,
    pub resized: u64,
    pub suspended: u64,
    pub failed: u64,
}

/// The default observer: no action, only counts per kind, for the metrics
/// Task 46 exports (`loams_pg_compute_events_total{event}`).
#[derive(Debug, Default)]
pub struct DefaultObserver {
    started: AtomicU64,
    resized: AtomicU64,
    suspended: AtomicU64,
    failed: AtomicU64,
}

impl DefaultObserver {
    pub fn counts(&self) -> LifecycleCounts {
        LifecycleCounts {
            started: self.started.load(Ordering::Relaxed),
            resized: self.resized.load(Ordering::Relaxed),
            suspended: self.suspended.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

impl ComputeLifecycleObserver for DefaultObserver {
    fn on_event(&self, e: &ComputeLifecycleEvent) {
        let counter = match e.event {
            LifecycleKind::Started { .. } => &self.started,
            LifecycleKind::Resized { .. } => &self.resized,
            LifecycleKind::Suspended => &self.suspended,
            LifecycleKind::Failed { .. } => &self.failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Where the endpoint reconciler sends lifecycle events: the observer, and
/// the org every event names (R12.6).
#[derive(Clone)]
pub struct Lifecycle {
    pub observer: Arc<dyn ComputeLifecycleObserver>,
    /// `pg-control` keeps no org per namespace yet: one per deployment,
    /// empty by default.
    pub org: String,
}

impl Lifecycle {
    /// Events to `observer`, with an empty org.
    pub fn new(observer: Arc<dyn ComputeLifecycleObserver>) -> Self {
        Lifecycle {
            observer,
            org: String::new(),
        }
    }

    /// Sends `event` of `compute` (bound to `binding`) at `at_ms`. A record
    /// whose ids do not parse is logged and sends nothing.
    pub(crate) fn emit(
        &self,
        binding: &ComputeBinding,
        compute: &str,
        event: LifecycleKind,
        at_ms: u64,
    ) {
        let ids = (
            binding.project_id.parse::<ProjectId>(),
            binding.endpoint_id.parse::<EndpointId>(),
            compute.parse::<ComputeId>(),
        );
        let (Ok(project), Ok(endpoint), Ok(compute_id)) = ids else {
            tracing::warn!(
                compute,
                "a compute record with malformed ids: no lifecycle event"
            );
            return;
        };
        self.observer.on_event(&ComputeLifecycleEvent {
            org: self.org.clone(),
            namespace: binding.namespace.clone(),
            project,
            endpoint,
            compute: compute_id,
            event,
            at: SystemTime::UNIX_EPOCH + Duration::from_millis(at_ms),
        });
    }
}

impl Default for Lifecycle {
    /// A [`DefaultObserver`], with an empty org.
    fn default() -> Self {
        Lifecycle::new(Arc::new(DefaultObserver::default()))
    }
}

impl std::fmt::Debug for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lifecycle")
            .field("org", &self.org)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: LifecycleKind) -> ComputeLifecycleEvent {
        ComputeLifecycleEvent {
            org: String::new(),
            namespace: "acme".into(),
            project: ProjectId::new(),
            endpoint: EndpointId::new(),
            compute: ComputeId::new(),
            event: kind,
            at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn the_default_observer_counts_each_kind() {
        let o = DefaultObserver::default();
        o.on_event(&event(LifecycleKind::Started { cu: Cu(4) }));
        o.on_event(&event(LifecycleKind::Suspended));
        o.on_event(&event(LifecycleKind::Started { cu: Cu(4) }));
        o.on_event(&event(LifecycleKind::Failed {
            reason: "crashed".into(),
        }));
        assert_eq!(
            o.counts(),
            LifecycleCounts {
                started: 2,
                resized: 0,
                suspended: 1,
                failed: 1
            }
        );
    }
}
