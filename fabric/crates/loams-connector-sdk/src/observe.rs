//! The metrics hook (CN2 Task 4; Task 24 builds metrics and the Q359
//! activity counters on it). Plain method calls with plain values: no wire
//! format, no buffer, no persistence (D548).

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
}

/// Observes nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl ConnectorObserver for NoopObserver {}
