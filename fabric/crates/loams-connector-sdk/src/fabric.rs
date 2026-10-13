//! The Fabric as a connector sees it. `loams-flow` implements these over
//! `ingest` and an Iggy consumer group (`fabric_io.rs`, once CN1 Task 3 and
//! FL1 land); [`crate::testing::memory`] implements them in memory.

use async_trait::async_trait;

use crate::budget::Budget;
use crate::error::{ErrorClass, FabricError};
use crate::position::Position;
use crate::source::{EventBatch, SourceBatch};

/// What `ingest` accepted. `duplicates` counts events it dropped by its
/// `(source, id)` dedup (D334): a re-read after a crash shows up here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FabricAck {
    pub accepted: u64,
    pub duplicates: u64,
}

/// Appends a source's batches to the instance's topic.
#[async_trait]
pub trait FabricWriter: Send + Sync {
    async fn append(&self, b: &SourceBatch) -> Result<FabricAck, FabricError>;
}

/// Reads the instance's topic for a sink, as one consumer group.
#[async_trait]
pub trait FabricReader: Send {
    /// The next batch within `budget`, and the position just after it.
    async fn poll(&mut self, budget: Budget)
    -> Result<Option<(EventBatch, Position)>, FabricError>;
    /// Commit the consumer group's offset up to `upto`.
    async fn commit(&mut self, upto: &Position) -> Result<(), FabricError>;
    /// Move the read cursor to `to` (a position this reader returned). An
    /// exactly-once sink's pipeline seeks to the checkpoint the target
    /// recovered (Shared contracts, "Sink, exactly-once"); this method is the
    /// SDK's addition to the contract's two (plan, "Rulings made during
    /// execution"). A reader that cannot seek cannot serve an exactly-once
    /// sink, and the default says so.
    async fn seek(&mut self, to: &Position) -> Result<(), FabricError> {
        let _ = to;
        Err(FabricError::new(
            ErrorClass::Fatal,
            "this Fabric reader cannot seek, so it cannot serve an exactly-once sink",
        ))
    }
}
