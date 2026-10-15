//! Queue-shaped sources: SQS (and EventBridge through SQS), a Pub/Sub
//! subscription, an Event Grid namespace subscription. Each is a [`Queue`]
//! with leases: a received message is invisible to other readers until its
//! lease (visibility timeout, ack deadline, lock) runs out, and comes back
//! if it is not acknowledged.
//!
//! [`QueueSource`] is the delivery contract for all of them (CN2 Shared
//! contracts, "Source"): a message is acknowledged upstream only after the
//! Fabric has acknowledged its events (`Source::committed`), its lease is
//! extended while it waits for that, and a source that stops hands back
//! what it had not finished. A crash in between redelivers; the event ids
//! are the message ids, so `ingest`'s `(source, id)` dedup makes the source
//! effectively once.
//!
//! [`Queue::receive`] and [`Queue::ack`] have the shape of the object-storage
//! family's `NotificationFeed` (CN2 R13.4), so the objects crate can read S3,
//! GCS and Blob notifications through these clients (R19.4).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState,
};

/// A message as a queue delivers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    /// What `ack`, `extend` and `release` take: an SQS receipt handle, a
    /// Pub/Sub ack id, an Event Grid lock token.
    pub handle: String,
    /// The message's own id, stable across redeliveries.
    pub id: String,
    pub body: Bytes,
    /// Message attributes, as strings.
    pub attributes: BTreeMap<String, String>,
    /// When the service accepted the message (RFC 3339).
    pub time: Option<String>,
    /// A grouping the service gives the message (an ordering key, a FIFO
    /// group id).
    pub subject: Option<String>,
}

/// A queue with leases (see the module documentation).
#[async_trait]
pub trait Queue: Send + Sync + fmt::Debug {
    /// The resource the events come from (a queue or subscription name).
    fn name(&self) -> &str;
    /// The lease a receive takes; [`QueueSource`] renews it at half this.
    fn lease(&self) -> Duration;
    /// The most messages one receive returns.
    fn max_receive(&self) -> usize;
    /// Up to `max` messages; empty when none is waiting.
    async fn receive(&self, max: usize) -> Result<Vec<QueueMessage>, ConnectorError>;
    /// Delete or acknowledge messages whose events the Fabric has. A handle
    /// whose lease already ran out is logged, not an error: the message
    /// comes back and its event is deduplicated.
    async fn ack(&self, handles: &[String]) -> Result<(), ConnectorError>;
    /// Renew the lease of messages still in flight.
    async fn extend(&self, handles: &[String]) -> Result<(), ConnectorError>;
    /// Hand messages back for redelivery now (a stop, or what did not fit a
    /// budget).
    async fn release(&self, handles: &[String]) -> Result<(), ConnectorError>;
}

/// Turns one message into its events: a pass-through CloudEvent, a wrapped
/// `message`, or (for notification parsers) several.
pub type Decode = Arc<
    dyn Fn(&EnvelopeBuilder, &str, &QueueMessage) -> Result<Vec<CloudEvent>, ConnectorError>
        + Send
        + Sync,
>;

/// `n` identical splits over one queue (`receivers` in config): each runs
/// its own receive loop, so throughput scales with them.
#[derive(Debug, Clone)]
pub struct QueueSplits {
    pub receivers: usize,
}

#[async_trait]
impl SplitEnumerator for QueueSplits {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok((0..self.receivers.max(1))
            .map(|i| Split::new(format!("receiver-{i}"), serde_json::Value::Null))
            .collect())
    }
}

/// Makes a [`QueueSource`] per split.
pub struct QueueSourceFactory {
    queue: Arc<dyn Queue>,
    decode: Decode,
    envelope: EnvelopeBuilder,
}

impl fmt::Debug for QueueSourceFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSourceFactory")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl QueueSourceFactory {
    pub fn new(queue: Arc<dyn Queue>, decode: Decode, envelope: EnvelopeBuilder) -> Self {
        Self {
            queue,
            decode,
            envelope,
        }
    }
}

impl SourceFactory for QueueSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(QueueSource::new(
            self.queue.clone(),
            self.decode.clone(),
            self.envelope.clone(),
        )))
    }
}

type InFlight = Arc<Mutex<BTreeMap<u64, Vec<String>>>>;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// The source over a [`Queue`]. Its position is a batch counter: on a
/// restart the queue redelivers whatever was not acknowledged.
pub struct QueueSource {
    queue: Arc<dyn Queue>,
    decode: Decode,
    envelope: EnvelopeBuilder,
    seq: u64,
    inflight: InFlight,
    keeper: Option<tokio::task::JoinHandle<()>>,
}

impl fmt::Debug for QueueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSource")
            .field("queue", &self.queue)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

impl Drop for QueueSource {
    fn drop(&mut self) {
        if let Some(k) = self.keeper.take() {
            k.abort();
        }
    }
}

impl QueueSource {
    pub fn new(queue: Arc<dyn Queue>, decode: Decode, envelope: EnvelopeBuilder) -> Self {
        Self {
            queue,
            decode,
            envelope,
            seq: 0,
            inflight: Arc::default(),
            keeper: None,
        }
    }

    fn position(&self) -> Position {
        Position::new(
            Bytes::copy_from_slice(&self.seq.to_be_bytes()),
            format!("{} batch {}", self.queue.name(), self.seq),
        )
    }

    /// Renew every in-flight lease at half the lease, until stopped.
    fn spawn_keeper(&mut self) {
        let queue = self.queue.clone();
        let inflight = self.inflight.clone();
        let every = (queue.lease() / 2).max(Duration::from_millis(500));
        self.keeper = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                let handles: Vec<String> = lock(&inflight).values().flatten().cloned().collect();
                if handles.is_empty() {
                    continue;
                }
                if let Err(e) = queue.extend(&handles).await {
                    tracing::warn!(queue = queue.name(), "extending leases: {}", e.message);
                }
            }
        }));
    }
}

#[async_trait]
impl Source for QueueSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.seq = pos.and_then(|p| p.as_u64()).unwrap_or(0);
        if self.keeper.is_none() {
            self.spawn_keeper();
        }
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let max = budget.max_rows.min(self.queue.max_receive()).max(1);
        let messages = self.queue.receive(max).await?;
        if messages.is_empty() {
            return Ok(None);
        }
        let resource = self.queue.name().to_owned();
        let mut events = Vec::new();
        let mut taken = Vec::new();
        let mut bytes = 0usize;
        let mut give_back = Vec::new();
        let mut oversize = None;
        for (i, m) in messages.iter().enumerate() {
            if !give_back.is_empty() {
                give_back.push(m.handle.clone());
                continue;
            }
            let evs = (self.decode)(&self.envelope, &resource, m)?;
            let size: usize = evs.iter().map(CloudEvent::size_bytes).sum();
            if events.len() + evs.len() > budget.max_rows || bytes + size > budget.max_bytes {
                if i == 0 {
                    // Larger than a whole batch (R4.7): left unacknowledged
                    // and unextended, so the queue's own redrive policy sees
                    // it again after its lease (CN2 R19.5).
                    oversize = Some(m.id.clone());
                    continue;
                }
                give_back.push(m.handle.clone());
                continue;
            }
            bytes += size;
            events.extend(evs);
            taken.push(m.handle.clone());
        }
        if !give_back.is_empty()
            && let Err(e) = self.queue.release(&give_back).await
        {
            tracing::warn!(queue = %resource, "handing back what did not fit: {}", e.message);
        }
        if let Some(id) = oversize
            && taken.is_empty()
        {
            return Err(ConnectorError::poison(format!(
                "message {id} on {resource} is larger than the batch budget"
            )));
        }
        self.seq += 1;
        lock(&self.inflight).insert(self.seq, taken);
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.position(),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some(n) = upto.as_u64() else {
            return Ok(());
        };
        let done: Vec<String> = {
            let mut inflight = lock(&self.inflight);
            let later = inflight.split_off(&(n + 1));
            let done = std::mem::replace(&mut *inflight, later);
            done.into_values().flatten().collect()
        };
        if done.is_empty() {
            return Ok(());
        }
        if let Err(e) = self.queue.ack(&done).await {
            // Put them back so a retried `committed` acknowledges them.
            lock(&self.inflight).insert(n, done);
            return Err(e);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(k) = self.keeper.take() {
            k.abort();
        }
        let rest: Vec<String> = std::mem::take(&mut *lock(&self.inflight))
            .into_values()
            .flatten()
            .collect();
        if !rest.is_empty() {
            self.queue.release(&rest).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    #[derive(Debug, Default)]
    struct Fake {
        ready: Mutex<Vec<QueueMessage>>,
        acked: Mutex<Vec<String>>,
        released: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Queue for Fake {
        fn name(&self) -> &str {
            "q"
        }
        fn lease(&self) -> Duration {
            Duration::from_secs(30)
        }
        fn max_receive(&self) -> usize {
            10
        }
        async fn receive(&self, max: usize) -> Result<Vec<QueueMessage>, ConnectorError> {
            let mut r = lock(&self.ready);
            let n = max.min(r.len());
            Ok(r.drain(..n).collect())
        }
        async fn ack(&self, h: &[String]) -> Result<(), ConnectorError> {
            lock(&self.acked).extend(h.iter().cloned());
            Ok(())
        }
        async fn extend(&self, _h: &[String]) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn release(&self, h: &[String]) -> Result<(), ConnectorError> {
            lock(&self.released).extend(h.iter().cloned());
            Ok(())
        }
    }

    fn msg(i: usize, size: usize) -> QueueMessage {
        QueueMessage {
            handle: format!("h{i}"),
            id: format!("m{i}"),
            body: Bytes::from(vec![b'x'; size]),
            attributes: BTreeMap::new(),
            time: None,
            subject: None,
        }
    }

    fn decode() -> Decode {
        Arc::new(|env: &EnvelopeBuilder, res: &str, m: &QueueMessage| {
            Ok(vec![
                env.event("message", res, m.id.clone())
                    .with_data("text/plain", m.body.clone()),
            ])
        })
    }

    #[tokio::test]
    async fn budget_hands_back_and_acks_in_order() {
        let fake = Arc::new(Fake::default());
        lock(&fake.ready).extend((0..4).map(|i| msg(i, 100)));
        let env = EnvelopeBuilder::new("t", &InstanceKey::new(1, "i"));
        let mut s = QueueSource::new(fake.clone(), decode(), env);
        s.open(&Split::new("receiver-0", serde_json::Value::Null), None)
            .await
            .expect("open");
        let one = msg(0, 100);
        let per = decode()(
            &EnvelopeBuilder::new("t", &InstanceKey::new(1, "i")),
            "q",
            &one,
        )
        .expect("decode")[0]
            .size_bytes();
        let b = s
            .next(Budget {
                max_rows: 10,
                max_bytes: per * 2 + 1,
            })
            .await
            .expect("next")
            .expect("a batch");
        assert_eq!(b.events.rows(), 2);
        assert_eq!(*lock(&fake.released), vec!["h2", "h3"]);
        let b2 = s
            .next(Budget {
                max_rows: 10,
                max_bytes: 1 << 20,
            })
            .await
            .expect("next");
        assert!(b2.is_none(), "the fake does not redeliver");
        s.committed(&b.position).await.expect("ack");
        assert_eq!(*lock(&fake.acked), vec!["h0", "h1"]);
        // An oversize first message is poison and stays unacknowledged.
        lock(&fake.ready).push(msg(9, 10_000));
        let err = s
            .next(Budget {
                max_rows: 10,
                max_bytes: 100,
            })
            .await
            .expect_err("poison");
        assert_eq!(err.class, loams_connector_sdk::ErrorClass::Poison);
        s.close().await.expect("close");
    }
}
