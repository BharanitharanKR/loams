//! Eviction and rehydration (GR1 Task 14; design §48 §6.5, §13.3, §17.2).
//!
//! An open graph holds its whole engine in memory. A node drops the graphs it does not need:
//!
//! * **Idle:** a graph with no statement for [`EvictPolicy::idle_after`] (1 hour by default).
//! * **Memory:** while the open graphs' memory is above [`EvictPolicy::node_memory`] (none by
//!   default until Task 26 sets it from the container), the least recently used ones go first,
//!   idle or not.
//! * **Deleted:** a graph whose catalog record is gone (deleted, perhaps on another node) is
//!   closed and its local cache removed at once, without waiting for the purge (Task 4 review
//!   M3).
//!
//! An idle or memory eviction checkpoints the graph first when its newest snapshot does not
//! cover its durable state (§48 §6.5), so a node that rehydrates it elsewhere replays little.
//! Then the engine closes cleanly and its local files stay, stamped ([`crate::recovery`]), and
//! its owner lease is given up. A graph a caller holds (a statement, a stream, a detached
//! statement) is never evicted. The next statement opens the graph again, from its local cache on
//! this node, and that first statement's latency is recorded as a [`Rehydration`] (§17.2's cold
//! gate; Task 27 exports it).
//!
//! [`GraphAdmin::evict_due`](crate::service::admin::GraphAdmin::evict_due) runs a pass; the
//! server runs it on its checkpoint tick.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::GraphId;
use crate::recovery::RecoverySource;

/// The idle time after which a graph is evicted by default (§48 §6.5).
pub const DEFAULT_IDLE_EVICT_AFTER: Duration = Duration::from_secs(3600);

/// How many rehydrations a node remembers (the newest).
pub const MAX_REHYDRATIONS: usize = 256;

/// When open graphs are evicted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictPolicy {
    /// A graph with no statement for this long is evicted (default 1 hour).
    pub idle_after: Duration,
    /// The node's budget for open graphs' memory, in bytes, as their engines estimate it: above
    /// it the least recently used graphs are evicted. `None` (the default until Task 26): no
    /// budget.
    pub node_memory: Option<u64>,
}

impl Default for EvictPolicy {
    fn default() -> Self {
        Self {
            idle_after: DEFAULT_IDLE_EVICT_AFTER,
            node_memory: None,
        }
    }
}

/// Why a graph was evicted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictReason {
    /// No statement for the policy's idle time.
    Idle,
    /// The node's memory budget was exceeded; it was the least recently used.
    Memory,
    /// Its catalog record is gone: closed, and its local cache removed.
    Deleted,
    /// Asked for ([`GraphAdmin::evict`](crate::service::admin::GraphAdmin::evict)).
    OnDemand,
}

impl EvictReason {
    /// A short name, for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Memory => "memory",
            Self::Deleted => "deleted",
            Self::OnDemand => "on_demand",
        }
    }
}

/// One graph an eviction pass closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evicted {
    /// Its namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
    /// Its id.
    pub graph: GraphId,
    /// Why.
    pub reason: EvictReason,
    /// A snapshot was taken first.
    pub checkpointed: bool,
    /// Its local files stay as a stamped cache for its next open.
    pub cache_kept: bool,
}

/// The first statement on an evicted graph: how long its open and the whole statement took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rehydration {
    /// The graph.
    pub graph: GraphId,
    /// What its engine started from.
    pub source: Option<RecoverySource>,
    /// From the statement's arrival until the graph was open.
    pub open: Duration,
    /// From the statement's arrival until its answer was ready.
    pub first_read: Duration,
}

/// One open graph as an eviction pass sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resident {
    /// Its namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
    /// Its id.
    pub graph: GraphId,
    /// How long since it was last used.
    pub idle_for: Duration,
    /// Its engine's memory estimate (0 when the policy has no budget).
    pub bytes: u64,
}

/// Orders `residents` for eviction: least recently used first.
#[must_use]
pub fn by_lru(mut residents: Vec<Resident>) -> Vec<Resident> {
    residents.sort_by(|a, b| b.idle_for.cmp(&a.idle_for).then(a.graph.cmp(&b.graph)));
    residents
}

/// Why `resident` is evicted under `policy` while the open graphs hold `total` bytes, if it is.
#[must_use]
pub fn reason(policy: &EvictPolicy, resident: &Resident, total: u64) -> Option<EvictReason> {
    if resident.idle_for >= policy.idle_after {
        Some(EvictReason::Idle)
    } else if policy.node_memory.is_some_and(|budget| total > budget) {
        Some(EvictReason::Memory)
    } else {
        None
    }
}

/// A node's eviction state: the graphs it evicted (until they open again, or turn out deleted)
/// and its recent rehydrations. Shared by every clone of the admin.
#[derive(Debug, Default)]
pub struct Evictor {
    /// Evicted graphs, with their namespace and name.
    evicted: Mutex<HashMap<GraphId, (String, String)>>,
    rehydrations: Mutex<VecDeque<Rehydration>>,
}

impl Evictor {
    /// Records that `graph` was evicted.
    pub fn mark_evicted(&self, graph: GraphId, namespace: &str, name: &str) {
        self.evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(graph, (namespace.to_string(), name.to_string()));
    }

    /// Whether `graph` was evicted and has not served a statement since.
    #[must_use]
    pub fn is_evicted(&self, graph: GraphId) -> bool {
        self.evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&graph)
    }

    /// The evicted graphs, as `(id, namespace, name)`.
    #[must_use]
    pub fn evicted(&self) -> Vec<(GraphId, String, String)> {
        self.evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(id, (namespace, name))| (*id, namespace.clone(), name.clone()))
            .collect()
    }

    /// Forgets `graph`'s eviction (it was deleted); answers whether it was evicted.
    pub fn forget(&self, graph: GraphId) -> bool {
        self.evicted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&graph)
            .is_some()
    }

    /// Records `rehydration`, the first statement on an evicted graph, unless another statement
    /// recorded one since the eviction. Answers whether it was recorded.
    pub fn rehydrated(&self, rehydration: Rehydration) -> bool {
        if !self.forget(rehydration.graph) {
            return false;
        }
        tracing::info!(graph = %rehydration.graph, source = rehydration.source.map(RecoverySource::as_str), open_ms = rehydration.open.as_millis() as u64, first_read_ms = rehydration.first_read.as_millis() as u64, "rehydrated an evicted graph");
        let mut recent = self
            .rehydrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if recent.len() == MAX_REHYDRATIONS {
            recent.pop_front();
        }
        recent.push_back(rehydration);
        true
    }

    /// The recent rehydrations, oldest first (at most [`MAX_REHYDRATIONS`]).
    #[must_use]
    pub fn rehydrations(&self) -> Vec<Rehydration> {
        self.rehydrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resident(idle_ms: u64, bytes: u64) -> Resident {
        Resident {
            namespace: "acme".to_string(),
            name: format!("g{idle_ms}"),
            graph: GraphId::new(),
            idle_for: Duration::from_millis(idle_ms),
            bytes,
        }
    }

    /// What a pass evicts when every eviction succeeds.
    fn pass(policy: &EvictPolicy, residents: Vec<Resident>) -> Vec<(String, EvictReason)> {
        let mut total: u64 = residents.iter().map(|r| r.bytes).sum();
        let mut out = Vec::new();
        for r in by_lru(residents) {
            if let Some(why) = reason(policy, &r, total) {
                total -= r.bytes;
                out.push((r.name, why));
            }
        }
        out
    }

    #[test]
    fn idle_graphs_go_and_busy_ones_stay_without_a_budget() {
        let policy = EvictPolicy {
            idle_after: Duration::from_millis(100),
            node_memory: None,
        };
        let out = pass(
            &policy,
            vec![resident(50, 9), resident(150, 9), resident(300, 9)],
        );
        assert_eq!(
            out,
            [
                ("g300".to_string(), EvictReason::Idle),
                ("g150".to_string(), EvictReason::Idle)
            ]
        );
    }

    #[test]
    fn over_budget_the_least_recently_used_go_until_under_it() {
        let policy = EvictPolicy {
            idle_after: Duration::from_secs(3600),
            node_memory: Some(100),
        };
        // 160 bytes open: evicting the least recently used (60) leaves 100, within the budget.
        let out = pass(
            &policy,
            vec![resident(10, 50), resident(30, 60), resident(20, 50)],
        );
        assert_eq!(out, [("g30".to_string(), EvictReason::Memory)]);
        // Idle ones go first, and count toward the budget.
        let policy = EvictPolicy {
            idle_after: Duration::from_millis(25),
            node_memory: Some(40),
        };
        let out = pass(
            &policy,
            vec![resident(10, 50), resident(30, 60), resident(20, 50)],
        );
        assert_eq!(
            out,
            [
                ("g30".to_string(), EvictReason::Idle),
                ("g20".to_string(), EvictReason::Memory),
                ("g10".to_string(), EvictReason::Memory),
            ]
        );
    }

    #[test]
    fn one_rehydration_is_recorded_per_eviction() {
        let evictor = Evictor::default();
        let graph = GraphId::new();
        let cold = Rehydration {
            graph,
            source: Some(RecoverySource::LocalCache),
            open: Duration::from_millis(3),
            first_read: Duration::from_millis(4),
        };
        assert!(!evictor.rehydrated(cold), "not evicted");
        evictor.mark_evicted(graph, "acme", "kg");
        assert!(evictor.is_evicted(graph));
        assert!(evictor.rehydrated(cold));
        assert!(!evictor.rehydrated(cold), "recorded once");
        assert_eq!(evictor.rehydrations(), [cold]);
    }
}
