//! Idle detection (design §46 §7.1; PG2 Task 12): when a running compute
//! has had no activity for its endpoint's `suspend_timeout_s` (default 300,
//! at least 60, 0 = never), the endpoint reconciler suspends it.
//!
//! **What counts as activity.** Two sources, sampled on every pass over a
//! running endpoint (at least every resync, 30 s), by the instance holding
//! the project's lease:
//! - `compute_ctl`'s `/status` `last_active`
//!   ([`ComputeStatusResponse::last_active`](loams_postgres::compute_ctl::ComputeStatusResponse::last_active)).
//!   The fork's monitor moves it to now every 500 ms while any client
//!   backend is not `idle` (a query running, however long, without a new
//!   command; a transaction left open), while a walsender other than
//!   walproposer streams (a logical subscriber of this compute), while a
//!   logical subscription's worker runs, and while autovacuum works; and to
//!   an `idle` backend's `state_change` otherwise. So a long query is
//!   activity for as long as it runs, not only when it starts.
//! - PgDog's `sv_active` and `cl_waiting` on the endpoint's pools (R18.7: the
//!   pinned PgDog reports no `cl_active`), on every
//!   replica of the namespace ([`PgDogAdmin`]; no replicas until PG2c).
//!   Any client there is activity at the sample, whether or not it sent a
//!   command since the last one.
//!
//! **How it is judged.** [`IdleMonitor`] keeps, per compute, the
//! `last_active` it last read and when (by `pg-control`'s clock) it last
//! saw activity. A sample is activity when PgDog has a client or
//! `last_active` moved since the last sample; the activity is dated by the
//! compute's `last_active`, but never before the previous sample nor after
//! this one, so a skewed compute clock moves it by at most one sampling
//! interval. The first sample of a compute (after it started, or after
//! this instance took the lease or restarted) counts as activity: nothing
//! is suspended on history this instance did not see. Nothing is kept in
//! the store; a compute that is gone is forgotten.
//!
//! **What blocks a suspend.** A sample that could not be taken (no answer
//! from `compute_ctl` or from a PgDog replica, an unreadable `last_active`)
//! never suspends. Nor does an endpoint with open operations, or one being
//! replaced.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::model::{BranchRec, EndpointRec, ProjectRec};
use crate::pgdog::{PgDogAdmin, PgDogError, pool_serves};

/// A compute this instance has not sampled for this long is forgotten (it
/// is gone, or another instance passes over it now).
const FORGET_AFTER_MS: u64 = 60 * 60 * 1000;

/// How often the forgotten entries are dropped.
const PRUNE_EVERY_MS: u64 = 10 * 60 * 1000;

/// What `pg-control` last saw of one compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    /// `compute_ctl`'s `last_active` at the last sample.
    reported: Option<u64>,
    /// When the compute was last active, by `pg-control`'s clock.
    active_at: u64,
    /// When it was last sampled.
    sampled_at: u64,
}

#[derive(Debug, Default)]
struct State {
    seen: HashMap<String, Seen>,
    pruned_at: u64,
}

/// Idle detection for the computes this instance passes over (see the
/// module docs).
pub struct IdleMonitor {
    pgdog: Arc<dyn PgDogAdmin>,
    state: Mutex<State>,
}

impl std::fmt::Debug for IdleMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleMonitor")
            .field("computes", &self.lock().seen.len())
            .finish_non_exhaustive()
    }
}

impl IdleMonitor {
    /// Reads PgDog's pools through `pgdog`.
    pub fn new(pgdog: Arc<dyn PgDogAdmin>) -> Self {
        IdleMonitor {
            pgdog,
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// PgDog's active and waiting clients on the pools that route to `ep`,
    /// summed over the namespace's replicas.
    ///
    /// # Errors
    ///
    /// When a replica did not answer: the clients are unknown.
    pub async fn clients(
        &self,
        project: &ProjectRec,
        branch: &BranchRec,
        ep: &EndpointRec,
    ) -> Result<u64, PgDogError> {
        let mut clients = 0u64;
        for instance in self.pgdog.instances(&project.namespace).await? {
            for pool in self.pgdog.show_pools(&instance).await? {
                if pool_serves(&pool.database, project, branch, ep) {
                    clients = clients
                        .saturating_add(pool.sv_active)
                        .saturating_add(pool.cl_waiting);
                }
            }
        }
        Ok(clients)
    }

    /// Records a sample of `compute` taken at `now_ms` (its `compute_ctl`'s
    /// `last_active`, and PgDog's clients of its endpoint) and answers how
    /// long, in ms, it has been idle.
    pub fn observe(&self, compute: &str, now_ms: u64, reported: Option<u64>, clients: u64) -> u64 {
        let mut state = self.lock();
        if now_ms.saturating_sub(state.pruned_at) >= PRUNE_EVERY_MS {
            state
                .seen
                .retain(|_, s| now_ms.saturating_sub(s.sampled_at) < FORGET_AFTER_MS);
            state.pruned_at = now_ms;
        }
        let next = match state.seen.get(compute) {
            None => Seen {
                reported,
                active_at: now_ms,
                sampled_at: now_ms,
            },
            Some(prev) => {
                let mut active_at = prev.active_at;
                if reported != prev.reported {
                    // Active since the last sample: when the compute says,
                    // within the interval between the two samples.
                    let at = reported
                        .unwrap_or(now_ms)
                        .clamp(prev.sampled_at.min(now_ms), now_ms);
                    active_at = active_at.max(at);
                }
                if clients > 0 {
                    active_at = now_ms;
                }
                Seen {
                    reported,
                    active_at,
                    sampled_at: now_ms.max(prev.sampled_at),
                }
            }
        };
        state.seen.insert(compute.to_owned(), next);
        now_ms.saturating_sub(next.active_at)
    }

    /// Forgets `compute` (it stopped).
    pub fn forget(&self, compute: &str) {
        self.lock().seen.remove(compute);
    }

    /// How many computes are remembered.
    pub fn len(&self) -> usize {
        self.lock().seen.len()
    }

    /// Whether no compute is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgdog::NoPgDog;

    const S: u64 = 1000;

    fn monitor() -> IdleMonitor {
        IdleMonitor::new(Arc::new(NoPgDog))
    }

    #[test]
    fn the_first_sample_is_activity() {
        let m = monitor();
        // compute_ctl last saw activity long ago, but this instance did not
        // see it: the idle time starts now.
        assert_eq!(m.observe("c", 1000 * S, Some(10 * S), 0), 0);
        assert_eq!(m.observe("c", 1030 * S, Some(10 * S), 0), 30 * S);
    }

    #[test]
    fn a_moved_last_active_is_dated_within_the_interval() {
        const T: u64 = 10_000 * S;
        let m = monitor();
        m.observe("c", T, None, 0);
        // The session went idle 10 s after the first sample.
        assert_eq!(m.observe("c", T + 30 * S, Some(T + 10 * S), 0), 20 * S);
        // A compute clock behind by an hour dates it at the last sample.
        m.observe("d", T, None, 0);
        assert_eq!(
            m.observe("d", T + 30 * S, Some(T + 10 * S - 3600 * S), 0),
            30 * S
        );
        // A compute clock ahead dates it now.
        m.observe("e", T, None, 0);
        assert_eq!(m.observe("e", T + 30 * S, Some(T + 500 * S), 0), 0);
    }

    #[test]
    fn a_long_query_is_activity_on_every_sample() {
        let m = monitor();
        m.observe("c", 0, None, 0);
        // compute_ctl reports now while the query runs; PgDog shows the same
        // one active client on every sample (no new command).
        for t in 1..=20 {
            assert_eq!(m.observe("c", t * 30 * S, Some(t * 30 * S - 400), 1), 0);
        }
        // The query ended at 600 s; nothing since.
        assert_eq!(m.observe("c", 630 * S, Some(600 * S), 0), 30 * S);
        assert_eq!(m.observe("c", 900 * S, Some(600 * S), 0), 300 * S);
    }

    #[test]
    fn clients_alone_are_activity() {
        let m = monitor();
        m.observe("c", 0, Some(1), 0);
        assert_eq!(m.observe("c", 400 * S, Some(1), 1), 0);
        assert_eq!(m.observe("c", 500 * S, Some(1), 0), 100 * S);
    }

    #[test]
    fn computes_are_forgotten() {
        let m = monitor();
        m.observe("a", 0, None, 0);
        m.observe("b", 0, None, 0);
        m.forget("a");
        assert_eq!(m.len(), 1);
        // "b" is not sampled for an hour: dropped at the next prune.
        m.observe("c", FORGET_AFTER_MS + PRUNE_EVERY_MS, None, 0);
        assert_eq!(m.len(), 1);
        // A forgotten compute starts again from its next sample.
        assert_eq!(m.observe("a", 2 * FORGET_AFTER_MS, Some(0), 0), 0);
    }
}
