//! Exactly-once sinks (OR-7): the target stores Loams' checkpoint atomically
//! with the data, so after a crash the target itself says how far it got.

use async_trait::async_trait;
use bytes::Bytes;

use crate::checkpoint::Checkpoint;
use crate::error::ConnectorError;
use crate::sink::Sink;

/// A two-phase sink. Per batch the pipeline calls `write` (data staged in the
/// open transaction), `prepare(checkpoint)` (the checkpoint staged with it),
/// `commit(token)`, and only then commits the Fabric offset, which is
/// therefore advisory. On open it calls `recover` and seeks the Fabric reader
/// to the recovered checkpoint, so batches the target already committed are
/// skipped.
#[async_trait]
pub trait TransactionalSink: Sink {
    /// The last checkpoint the target committed. A transaction that was
    /// prepared but not committed before a crash is resolved here, by the
    /// token's recorded state (committed or aborted).
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError>;
    /// Stage the written data and `cp` together.
    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError>;
    /// Commit; idempotent, retried after a crash.
    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError>;
    async fn abort(&mut self, t: TxnToken) -> Result<(), ConnectorError>;
}

/// A prepared transaction: a target-defined id (a Postgres prepared
/// transaction's gid, a Kafka transactional id and epoch, a Delta `txn`
/// version) and any opaque state the target needs to finish it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnToken {
    pub id: String,
    pub opaque: Bytes,
}
