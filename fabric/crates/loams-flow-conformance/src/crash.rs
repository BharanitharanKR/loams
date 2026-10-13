//! Crash injection at each [`FaultPoint`] of the delivery path.
//!
//! A harness wraps its Fabric writer, Fabric reader and sink factory in the
//! types below, all sharing one [`FaultSwitch`]. When the delivery path
//! reaches the armed point, the wrapper records the trip and then **never
//! returns**: the task stops dead exactly there, as a crashed process does,
//! and the harness kills it ([`crate::KillTarget::Task`]) and starts a new
//! one under a higher epoch. Nothing after the fault point runs, so the
//! restart sees exactly the state a real crash leaves.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_connector_sdk::{
    Budget, Checkpoint, ConnectorError, EventBatch, FabricAck, FabricError, FabricReader,
    FabricWriter, Position, Sink, SinkAck, SinkFactory, SinkHandle, SourceBatch, TransactionalSink,
    TxnToken,
};

use crate::FaultPoint;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// The fault armed for the next pass through the delivery path, and the
/// one that tripped.
#[derive(Debug, Default)]
pub struct FaultSwitch {
    armed: Mutex<Option<FaultPoint>>,
    tripped: Mutex<Option<FaultPoint>>,
}

impl FaultSwitch {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Arm `at`; the previous trip is forgotten.
    pub fn arm(&self, at: FaultPoint) {
        *lock(&self.armed) = Some(at);
        *lock(&self.tripped) = None;
    }

    pub fn disarm(&self) {
        *lock(&self.armed) = None;
    }

    pub fn armed(&self) -> Option<FaultPoint> {
        *lock(&self.armed)
    }

    pub fn tripped(&self) -> Option<FaultPoint> {
        *lock(&self.tripped)
    }

    /// Wait until the armed fault trips; `false` after `timeout`.
    pub async fn wait_tripped(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.tripped().is_some() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Whether the path at `at` must crash now. A fault trips once.
    pub fn hit(&self, at: FaultPoint) -> bool {
        let mut armed = lock(&self.armed);
        if *armed == Some(at) {
            *armed = None;
            *lock(&self.tripped) = Some(at);
            true
        } else {
            false
        }
    }
}

/// Stop dead: the future never completes, so nothing after it runs. The
/// harness aborts the task.
async fn crash<T>() -> T {
    std::future::pending::<T>().await
}

/// A Fabric writer that crashes at [`FaultPoint::BeforeFabricAck`] (before
/// the append) and [`FaultPoint::AfterFabricAckBeforeUpstreamCommit`] (the
/// Fabric holds the batch, but its ack never reaches the pipeline, so
/// neither the checkpoint nor the upstream commit happens).
#[derive(Debug)]
pub struct CrashingWriter<W> {
    inner: W,
    switch: Arc<FaultSwitch>,
}

impl<W> CrashingWriter<W> {
    pub fn new(inner: W, switch: Arc<FaultSwitch>) -> Self {
        Self { inner, switch }
    }
}

#[async_trait]
impl<W: FabricWriter> FabricWriter for CrashingWriter<W> {
    async fn append(&self, b: &SourceBatch) -> Result<FabricAck, FabricError> {
        if self.switch.hit(FaultPoint::BeforeFabricAck) {
            return crash().await;
        }
        let ack = self.inner.append(b).await?;
        if self
            .switch
            .hit(FaultPoint::AfterFabricAckBeforeUpstreamCommit)
        {
            return crash().await;
        }
        Ok(ack)
    }
}

/// A Fabric reader that crashes at
/// [`FaultPoint::AfterCommitBeforeFabricCommit`]: the sink flushed or
/// committed, and the consumer group's offset never moves.
#[derive(Debug)]
pub struct CrashingReader<R> {
    inner: R,
    switch: Arc<FaultSwitch>,
}

impl<R> CrashingReader<R> {
    pub fn new(inner: R, switch: Arc<FaultSwitch>) -> Self {
        Self { inner, switch }
    }
}

#[async_trait]
impl<R: FabricReader> FabricReader for CrashingReader<R> {
    async fn poll(
        &mut self,
        budget: Budget,
    ) -> Result<Option<(EventBatch, Position)>, FabricError> {
        self.inner.poll(budget).await
    }

    async fn commit(&mut self, upto: &Position) -> Result<(), FabricError> {
        if self.switch.hit(FaultPoint::AfterCommitBeforeFabricCommit) {
            return crash().await;
        }
        self.inner.commit(upto).await
    }

    async fn seek(&mut self, to: &Position) -> Result<(), FabricError> {
        self.inner.seek(to).await
    }
}

/// Wraps a sink factory so its sinks crash at [`FaultPoint::MidBatch`]
/// (part of a batch written) and [`FaultPoint::AfterPrepare`] (data and
/// checkpoint staged, not committed).
#[derive(Clone)]
pub struct CrashingSinkFactory {
    inner: Arc<dyn SinkFactory>,
    switch: Arc<FaultSwitch>,
}

impl std::fmt::Debug for CrashingSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrashingSinkFactory")
            .field("switch", &self.switch)
            .finish_non_exhaustive()
    }
}

impl CrashingSinkFactory {
    pub fn new(inner: Arc<dyn SinkFactory>, switch: Arc<FaultSwitch>) -> Self {
        Self { inner, switch }
    }
}

impl SinkFactory for CrashingSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let switch = self.switch.clone();
        Ok(match self.inner.create()? {
            SinkHandle::Plain(s) => SinkHandle::Plain(Box::new(CrashingSink {
                inner: Inner::Plain(s),
                switch,
            })),
            SinkHandle::Transactional(t) => SinkHandle::Transactional(Box::new(CrashingSink {
                inner: Inner::Txn(t),
                switch,
            })),
        })
    }
}

enum Inner {
    Plain(Box<dyn Sink>),
    Txn(Box<dyn TransactionalSink>),
}

struct CrashingSink {
    inner: Inner,
    switch: Arc<FaultSwitch>,
}

impl CrashingSink {
    fn sink(&mut self) -> &mut dyn Sink {
        match &mut self.inner {
            Inner::Plain(s) => s.as_mut(),
            Inner::Txn(t) => t.as_mut(),
        }
    }

    fn txn(&mut self) -> Result<&mut dyn TransactionalSink, ConnectorError> {
        match &mut self.inner {
            Inner::Txn(t) => Ok(t.as_mut()),
            Inner::Plain(_) => Err(ConnectorError::fatal("not a transactional sink")),
        }
    }
}

#[async_trait]
impl Sink for CrashingSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        if self.switch.hit(FaultPoint::MidBatch) {
            // Write the first half (at least one event), then die before the
            // rest, the flush, the prepare or the commit.
            if let EventBatch::Events(mut events) = batch {
                let half = events.len().div_ceil(2);
                events.truncate(half);
                self.sink().write(EventBatch::Events(events)).await?;
            }
            return crash().await;
        }
        self.sink().write(batch).await
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.sink().flush().await
    }
}

#[async_trait]
impl TransactionalSink for CrashingSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        self.txn()?.recover().await
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        let token = self.txn()?.prepare(cp).await?;
        if self.switch.hit(FaultPoint::AfterPrepare) {
            return crash().await;
        }
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        self.txn()?.commit(t).await
    }

    async fn abort(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        self.txn()?.abort(t).await
    }
}
