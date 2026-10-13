//! Splits: the unit of parallelism and of checkpointing (a Kafka partition, a
//! Kinesis shard, a key range, a replication slot, an object prefix, a
//! DynamoDB scan segment). One split is read by one task at a time.

use async_trait::async_trait;

use crate::error::ConnectorError;
use crate::position::{Position, SplitId};
use crate::sink::SinkHandle;
use crate::source::Source;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Split {
    pub id: SplitId,
    pub spec: serde_json::Value,
}

impl Split {
    pub fn new(id: impl Into<SplitId>, spec: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            spec,
        }
    }
}

impl From<String> for SplitId {
    fn from(s: String) -> Self {
        SplitId(s)
    }
}

/// A split the pipeline already knows, with its last checkpointed position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitState {
    pub split: Split,
    pub position: Option<Position>,
}

/// Finds the splits of a source. Runs on the instance's lease holder, on a
/// timer and on demand; the pipeline adds and removes split tasks to match
/// the answer without restarting the instance.
#[async_trait]
pub trait SplitEnumerator: Send {
    async fn discover(&mut self, known: &[SplitState]) -> Result<Vec<Split>, ConnectorError>;
}

/// Makes one [`Source`] per split.
pub trait SourceFactory: Send + Sync {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError>;
}

/// Makes the instance's sink (plain or transactional).
pub trait SinkFactory: Send + Sync {
    fn create(&self) -> Result<SinkHandle, ConnectorError>;
}
