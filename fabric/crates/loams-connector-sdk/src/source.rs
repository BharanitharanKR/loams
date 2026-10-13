//! The source side of a connector (CN2 Shared contracts). CN1 Task 3's
//! method names are kept (OR-5): `open`, `next`, `committed`, plus `close`.

use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::budget::Budget;
use crate::envelope::CloudEvent;
use crate::error::ConnectorError;
use crate::position::Position;
use crate::split::Split;

/// A source reads one split. One `Source` value serves one split at a time.
#[async_trait]
pub trait Source: Send {
    /// Open `split`, resuming after `pos` when a checkpoint exists.
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError>;
    /// The next batch, never larger than `budget` (rows and bytes). `None`
    /// means nothing is available right now (a bounded source that has
    /// finished keeps returning `None`). The pipeline never calls `next` with
    /// an empty budget: it stops calling instead, and that is the source's
    /// backpressure (it must not buffer ahead on its own).
    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError>;
    /// The Fabric has acknowledged everything up to `upto`: commit upstream
    /// (Kafka offsets, PG flush LSN, XACK, SQS delete…).
    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError>;
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

/// One batch from a source and the position just after it.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    pub events: EventBatch,
    pub position: Position,
}

/// Events, or an Arrow batch with one envelope (D356 bulk path).
// The variants are the Shared contract's, unboxed: a batch moves through a
// channel once per read, so its size on the stack does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum EventBatch {
    Events(Vec<CloudEvent>),
    Arrow {
        batch: RecordBatch,
        envelope: CloudEvent,
    },
}

impl EventBatch {
    /// Rows in the batch: events, or Arrow rows.
    pub fn rows(&self) -> usize {
        match self {
            EventBatch::Events(v) => v.len(),
            EventBatch::Arrow { batch, .. } => batch.num_rows(),
        }
    }

    /// The batch's size for budgets.
    pub fn size_bytes(&self) -> usize {
        match self {
            EventBatch::Events(v) => v.iter().map(CloudEvent::size_bytes).sum(),
            EventBatch::Arrow { batch, envelope } => {
                batch.get_array_memory_size() + envelope.size_bytes()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows() == 0
    }
}
