//! Backpressure (CN2 Task 4). The pipeline hands each read a [`Budget`] cut
//! from the free space of `max_in_flight_batches` and `memory_budget_bytes`;
//! when the Fabric or a sink is slow, leases are not returned, the free space
//! falls to zero, and sources stop being asked to read. (The unit is called a
//! budget, not a credit: Global Constraints, "No billing".)

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// The most a single `Source::next` or `FabricReader::poll` may return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Budget {
    pub const ZERO: Budget = Budget {
        max_rows: 0,
        max_bytes: 0,
    };

    pub fn is_zero(&self) -> bool {
        self.max_rows == 0 || self.max_bytes == 0
    }

    /// Whether a batch of `rows` and `bytes` fits.
    pub fn admits(&self, rows: usize, bytes: usize) -> bool {
        rows <= self.max_rows && bytes <= self.max_bytes
    }
}

/// The peak use of a [`BudgetPool`], for tests and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetStats {
    pub in_flight_batches: usize,
    pub in_flight_bytes: usize,
    pub peak_in_flight_batches: usize,
    pub peak_in_flight_bytes: usize,
}

#[derive(Debug)]
struct PoolState {
    batches: usize,
    bytes: usize,
    stats: BudgetStats,
}

/// One instance's memory and in-flight budget, shared by its splits.
#[derive(Debug)]
pub struct BudgetPool {
    max_batch_rows: usize,
    max_batch_bytes: usize,
    max_in_flight_batches: usize,
    memory_budget_bytes: usize,
    state: Mutex<PoolState>,
    freed: Notify,
}

impl BudgetPool {
    pub fn new(
        max_batch_rows: usize,
        max_batch_bytes: usize,
        max_in_flight_batches: usize,
        memory_budget_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            max_batch_rows: max_batch_rows.max(1),
            max_batch_bytes: max_batch_bytes.max(1),
            max_in_flight_batches: max_in_flight_batches.max(1),
            memory_budget_bytes: memory_budget_bytes.max(1),
            state: Mutex::new(PoolState {
                batches: 0,
                bytes: 0,
                stats: BudgetStats::default(),
            }),
            freed: Notify::new(),
        })
    }

    /// The bytes one lease reserves: a full batch, or the whole memory budget
    /// when that is smaller.
    fn lease_bytes(&self) -> usize {
        self.max_batch_bytes.min(self.memory_budget_bytes)
    }

    /// Reserve room for one batch, if there is room now.
    pub fn try_acquire(self: &Arc<Self>) -> Option<BudgetLease> {
        let want = self.lease_bytes();
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.batches >= self.max_in_flight_batches || s.bytes + want > self.memory_budget_bytes {
            return None;
        }
        s.batches += 1;
        s.bytes += want;
        s.stats.in_flight_batches = s.batches;
        s.stats.in_flight_bytes = s.bytes;
        s.stats.peak_in_flight_batches = s.stats.peak_in_flight_batches.max(s.batches);
        s.stats.peak_in_flight_bytes = s.stats.peak_in_flight_bytes.max(s.bytes);
        Some(BudgetLease {
            pool: Arc::clone(self),
            bytes: want,
            budget: Budget {
                max_rows: self.max_batch_rows,
                max_bytes: want,
            },
        })
    }

    /// Wait until there is room for one batch and reserve it. The reservation
    /// is the whole batch's worth (rows and bytes), so two splits can never
    /// both be promised the same free space.
    pub async fn acquire(self: &Arc<Self>) -> BudgetLease {
        loop {
            let notified = self.freed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(lease) = self.try_acquire() {
                return lease;
            }
            notified.await;
        }
    }

    /// The budget a read could get right now (zero when the pool is full).
    pub fn available(&self) -> Budget {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let want = self.lease_bytes();
        if s.batches >= self.max_in_flight_batches || s.bytes + want > self.memory_budget_bytes {
            Budget::ZERO
        } else {
            Budget {
                max_rows: self.max_batch_rows,
                max_bytes: want,
            }
        }
    }

    pub fn stats(&self) -> BudgetStats {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).stats
    }

    fn release(&self, bytes: usize) {
        {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            s.batches = s.batches.saturating_sub(1);
            s.bytes = s.bytes.saturating_sub(bytes);
            s.stats.in_flight_batches = s.batches;
            s.stats.in_flight_bytes = s.bytes;
        }
        self.freed.notify_waiters();
    }

    fn shrink(&self, from: usize, to: usize) {
        {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            s.bytes = s.bytes.saturating_sub(from - to);
            s.stats.in_flight_bytes = s.bytes;
        }
        self.freed.notify_waiters();
    }
}

/// Room for one batch, held from the read until the batch is acknowledged
/// (by the Fabric for a source, by the sink's flush or commit for a sink).
#[derive(Debug)]
pub struct BudgetLease {
    pool: Arc<BudgetPool>,
    bytes: usize,
    budget: Budget,
}

impl BudgetLease {
    /// The budget this lease grants.
    pub fn budget(&self) -> Budget {
        self.budget
    }

    /// Return the unused part once the batch's real size is known.
    pub fn shrink_to(&mut self, bytes: usize) {
        if bytes < self.bytes {
            self.pool.shrink(self.bytes, bytes);
            self.bytes = bytes;
        }
    }
}

impl Drop for BudgetLease {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_never_exceed_the_pool() {
        let pool = BudgetPool::new(10, 100, 3, 250);
        let a = pool.try_acquire().expect("first");
        let mut b = pool.try_acquire().expect("second");
        // 200 of 250 bytes reserved: a third full lease does not fit.
        assert!(pool.try_acquire().is_none());
        assert!(pool.available().is_zero());
        b.shrink_to(10);
        let c = pool.try_acquire().expect("after shrink");
        assert!(pool.try_acquire().is_none(), "in-flight cap of 3");
        drop((a, b, c));
        assert_eq!(pool.stats().in_flight_bytes, 0);
        assert!(pool.stats().peak_in_flight_bytes <= 250);
    }
}
