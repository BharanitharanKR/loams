//! The family's sinks share one shape: a batch API that answers per entry
//! (`SendMessageBatch`, `PublishBatch`, `PutEvents`, `PutRecords`, Pub/Sub
//! `publish`, an Event Grid batch). [`BatchSink`] cuts a Fabric batch to the
//! API's limits, retries only the entries that failed transiently (Task 19's
//! "retries partial failures"), and reports an entry the service refuses as
//! `Poison`.
//!
//! The pipeline answers `Poison` by writing the batch again one event at a
//! time (CN2 R4.10), so the sink remembers what it delivered since the last
//! `flush` and does not send it twice: within a batch an event reaches the
//! target once, and across a crash the at-least-once floor holds (OR-7).

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use loams_connector_sdk::{CloudEvent, ConnectorError, ErrorClass, EventBatch, Sink, SinkAck};

/// What the service said about one entry of a batch call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryOutcome {
    Delivered,
    /// A transient failure (throttling, an internal error): retried alone.
    Retry(String),
    /// The service refuses this entry (malformed, too large): `Poison`.
    Refused(String),
}

/// One batch API.
#[async_trait]
pub trait BatchPut: Send {
    /// The most entries one call takes.
    fn max_entries(&self) -> usize;
    /// The most request bytes one call takes (as [`BatchPut::entry_size`]
    /// counts them).
    fn max_bytes(&self) -> usize;
    /// An entry's size toward [`BatchPut::max_bytes`].
    fn entry_size(&self, ev: &CloudEvent) -> usize;
    /// Send `events` in one call; one outcome per event, in order.
    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError>;
}

/// How often and how long the failed entries of a call are retried before
/// the batch is handed back to the pipeline's own retry.
#[derive(Debug, Clone, Copy)]
pub struct PartialRetry {
    pub attempts: u32,
    pub first: Duration,
    pub max: Duration,
}

impl Default for PartialRetry {
    fn default() -> Self {
        Self {
            attempts: 8,
            first: Duration::from_millis(100),
            max: Duration::from_secs(5),
        }
    }
}

/// A [`Sink`] over a [`BatchPut`].
#[derive(Debug)]
pub struct BatchSink<P> {
    put: P,
    retry: PartialRetry,
    delivered: HashSet<(String, String)>,
}

impl<P: BatchPut> BatchSink<P> {
    pub fn new(put: P) -> Self {
        Self {
            put,
            retry: PartialRetry::default(),
            delivered: HashSet::new(),
        }
    }

    pub fn with_retry(mut self, retry: PartialRetry) -> Self {
        self.retry = retry;
        self
    }

    pub fn inner(&self) -> &P {
        &self.put
    }

    fn key(ev: &CloudEvent) -> (String, String) {
        (ev.source.clone(), ev.id.clone())
    }

    /// Cut `events` into calls within the API's limits. An event larger than
    /// a whole call on its own is refused.
    fn chunks(&self, events: Vec<CloudEvent>) -> Result<Vec<Vec<CloudEvent>>, ConnectorError> {
        let (max_n, max_b) = (self.put.max_entries().max(1), self.put.max_bytes());
        let mut out: Vec<Vec<CloudEvent>> = Vec::new();
        let mut cur = Vec::new();
        let mut cur_b = 0usize;
        for ev in events {
            let size = self.put.entry_size(&ev);
            if size > max_b {
                return Err(ConnectorError::poison(format!(
                    "event {} is {size} bytes, over the target's {max_b}-byte limit",
                    ev.id
                )));
            }
            if !cur.is_empty() && (cur.len() == max_n || cur_b + size > max_b) {
                out.push(std::mem::take(&mut cur));
                cur_b = 0;
            }
            cur_b += size;
            cur.push(ev);
        }
        if !cur.is_empty() {
            out.push(cur);
        }
        Ok(out)
    }

    async fn put_chunk(&mut self, mut pending: Vec<CloudEvent>) -> Result<(), ConnectorError> {
        let mut delay = self.retry.first;
        let mut refused: Option<String> = None;
        for attempt in 0..self.retry.attempts.max(1) {
            let outcomes = self.put.put(&pending).await?;
            if outcomes.len() != pending.len() {
                return Err(ConnectorError::retryable(format!(
                    "the target answered {} outcomes for {} entries",
                    outcomes.len(),
                    pending.len()
                )));
            }
            let mut again = Vec::new();
            let mut last_code = String::new();
            for (ev, outcome) in pending.into_iter().zip(outcomes) {
                match outcome {
                    EntryOutcome::Delivered => {
                        self.delivered.insert(Self::key(&ev));
                    }
                    EntryOutcome::Retry(code) => {
                        last_code = code;
                        again.push(ev);
                    }
                    EntryOutcome::Refused(why) => {
                        refused.get_or_insert(format!("event {}: {why}", ev.id));
                    }
                }
            }
            if again.is_empty() {
                break;
            }
            if attempt + 1 == self.retry.attempts.max(1) {
                let throttled = last_code.contains("Throttl")
                    || last_code.contains("Exceeded")
                    || last_code.contains("TooMany");
                let class = if throttled {
                    ErrorClass::Throttled { after: delay }
                } else {
                    ErrorClass::Retryable { after: Some(delay) }
                };
                return Err(ConnectorError::new(
                    class,
                    format!(
                        "{} entries still failing after {} attempts ({last_code})",
                        again.len(),
                        self.retry.attempts
                    ),
                ));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(self.retry.max);
            pending = again;
        }
        match refused {
            Some(why) => Err(ConnectorError::poison(why)),
            None => Ok(()),
        }
    }
}

/// Makes a fresh [`BatchSink`] over a clone of `P` for each pipeline start.
#[derive(Debug, Clone)]
pub struct BatchSinkFactory<P>(pub P);

impl<P> loams_connector_sdk::SinkFactory for BatchSinkFactory<P>
where
    P: BatchPut + Clone + Sync + 'static,
{
    fn create(&self) -> Result<loams_connector_sdk::SinkHandle, ConnectorError> {
        Ok(loams_connector_sdk::SinkHandle::Plain(Box::new(
            BatchSink::new(self.0.clone()),
        )))
    }
}

#[async_trait]
impl<P: BatchPut> Sink for BatchSink<P> {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = match batch {
            EventBatch::Events(v) => v,
            EventBatch::Arrow { envelope, .. } => {
                return Err(ConnectorError::poison(format!(
                    "event {}: a message sink takes events, not Arrow batches (no bulk path)",
                    envelope.id
                )));
            }
        };
        let todo: Vec<CloudEvent> = events
            .into_iter()
            .filter(|e| !self.delivered.contains(&Self::key(e)))
            .collect();
        let rows = todo.len() as u64;
        let bytes: u64 = todo.iter().map(|e| e.size_bytes() as u64).sum();
        let mut refused = None;
        for chunk in self.chunks(todo)? {
            match self.put_chunk(chunk).await {
                Ok(()) => {}
                Err(e) if e.class == ErrorClass::Poison => {
                    refused.get_or_insert(e);
                }
                Err(e) => return Err(e),
            }
        }
        match refused {
            Some(e) => Err(e),
            None => Ok(SinkAck { rows, bytes }),
        }
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.delivered.clear();
        Ok(SinkAck::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct Put {
        calls: Vec<Vec<String>>,
        fail_once: HashSet<String>,
        refuse: HashSet<String>,
    }

    #[async_trait]
    impl BatchPut for Put {
        fn max_entries(&self) -> usize {
            2
        }
        fn max_bytes(&self) -> usize {
            1000
        }
        fn entry_size(&self, ev: &CloudEvent) -> usize {
            ev.data.len()
        }
        async fn put(
            &mut self,
            events: &[CloudEvent],
        ) -> Result<Vec<EntryOutcome>, ConnectorError> {
            self.calls
                .push(events.iter().map(|e| e.id.clone()).collect());
            Ok(events
                .iter()
                .map(|e| {
                    if self.refuse.contains(&e.id) {
                        EntryOutcome::Refused("bad".into())
                    } else if self.fail_once.remove(&e.id) {
                        EntryOutcome::Retry("ThrottlingException".into())
                    } else {
                        EntryOutcome::Delivered
                    }
                })
                .collect())
        }
    }

    fn ev(id: &str, size: usize) -> CloudEvent {
        CloudEvent::new(id, "/s", "t").with_data("text/plain", vec![b'x'; size])
    }

    #[tokio::test(start_paused = true)]
    async fn retries_only_failed_and_skips_delivered() {
        let mut put = Put::default();
        put.fail_once.insert("b".into());
        put.refuse.insert("c".into());
        let mut sink = BatchSink::new(put);
        let batch = vec![ev("a", 1), ev("b", 1), ev("c", 1)];
        let err = sink
            .write(EventBatch::Events(batch.clone()))
            .await
            .expect_err("c is refused");
        assert_eq!(err.class, ErrorClass::Poison);
        assert_eq!(
            sink.inner().calls,
            vec![vec!["a", "b"], vec!["b"], vec!["c"]]
        );
        // The pipeline's event-by-event pass sends only what is left.
        for e in batch {
            let _ = sink.write(EventBatch::Events(vec![e])).await;
        }
        assert_eq!(sink.inner().calls.len(), 4, "only c again");
        sink.flush().await.expect("flush");
        assert!(sink.delivered.is_empty());
        let too_big = sink.write(EventBatch::Events(vec![ev("z", 5000)])).await;
        assert_eq!(too_big.expect_err("too big").class, ErrorClass::Poison);
    }
}
