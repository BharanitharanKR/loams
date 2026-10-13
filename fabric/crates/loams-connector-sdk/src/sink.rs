//! The sink side of a connector (CN2 Shared contracts).

use async_trait::async_trait;

use crate::error::ConnectorError;
use crate::source::EventBatch;
use crate::txn::TransactionalSink;

/// A sink writes batches from the Fabric to the target. At-least-once is the
/// floor (OR-7): the pipeline commits the Fabric offset only after `flush`.
#[async_trait]
pub trait Sink: Send {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError>;
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError>;
}

/// What a sink accepted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkAck {
    pub rows: u64,
    pub bytes: u64,
}

/// A sink as a factory makes it: plain (at-least-once or effectively-once)
/// or transactional (exactly-once, OR-7).
pub enum SinkHandle {
    Plain(Box<dyn Sink>),
    Transactional(Box<dyn TransactionalSink>),
}

impl std::fmt::Debug for SinkHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkHandle::Plain(_) => f.write_str("SinkHandle::Plain"),
            SinkHandle::Transactional(_) => f.write_str("SinkHandle::Transactional"),
        }
    }
}
