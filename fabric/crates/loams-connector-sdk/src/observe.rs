//! The metrics hook (CN2 Task 4; Task 24 builds metrics and the Q359
//! activity counters on it). Plain method calls with plain values: no wire
//! format, no buffer, no persistence (D548).
//!
//! The pipeline calls every method but [`ConnectorObserver::lag`] and
//! [`ConnectorObserver::position_risk`]: those two are what only the
//! connector knows (a Kafka high watermark, a replication slot's retained
//! WAL), so a connector calls them through `ConnectorContext::observe`.
//! `loams_flow::metrics::InstanceMetrics` is the implementation the runtime
//! passes to both.

use std::time::Duration;

use crate::error::ErrorClass;
use crate::position::{InstanceKey, SplitId};

/// Called by the pipeline as work happens. Every method has an empty default.
pub trait ConnectorObserver: Send + Sync {
    /// A source batch the Fabric acknowledged.
    fn source_batch(&self, instance: &InstanceKey, split: &SplitId, rows: u64, bytes: u64) {
        let _ = (instance, split, rows, bytes);
    }
    /// A sink batch the target acknowledged.
    fn sink_batch(&self, instance: &InstanceKey, rows: u64, bytes: u64) {
        let _ = (instance, rows, bytes);
    }
    /// One call to the external system (a `next`, a `write`, a `commit`).
    fn api_call(&self, instance: &InstanceKey) {
        let _ = instance;
    }
    /// An operation will be retried after `delay`.
    fn retry(&self, instance: &InstanceKey, class: &ErrorClass, attempt: u32, delay: Duration) {
        let _ = (instance, class, attempt, delay);
    }
    /// An error the pipeline acted on; `message` is already scrubbed.
    fn error(&self, instance: &InstanceKey, class: &ErrorClass, message: &str) {
        let _ = (instance, class, message);
    }
    fn dead_letter(&self, instance: &InstanceKey) {
        let _ = instance;
    }
    fn checkpoint(&self, instance: &InstanceKey, epoch: u64) {
        let _ = (instance, epoch);
    }
    /// A source task waited `waited` for budget before it could read: the
    /// Fabric or the sink was slower than the source (backpressure).
    fn budget_wait(&self, instance: &InstanceKey, waited: Duration) {
        let _ = (instance, waited);
    }
    /// How far behind the external system the instance is: for a source, the
    /// age of the oldest unread change upstream (a consumer group's lag, a
    /// replication slot's confirmed LSN against the server's); for a sink,
    /// the age of the oldest unwritten Fabric event. `split` is `None` for an
    /// instance-wide value.
    fn lag(&self, instance: &InstanceKey, split: Option<&SplitId>, lag: Duration) {
        let _ = (instance, split, lag);
    }
    /// How much of the upstream log's retention the instance's position has
    /// used, 0–1 (slot WAL against `max_slot_wal_keep_size`, binlog or oplog
    /// window, CDC retention, stream trim horizon). Past 1 the position is
    /// lost.
    fn position_risk(&self, instance: &InstanceKey, split: Option<&SplitId>, ratio: f64) {
        let _ = (instance, split, ratio);
    }
}

/// Observes nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl ConnectorObserver for NoopObserver {}
