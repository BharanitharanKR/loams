//! Push sources: SNS's HTTPS subscription and Event Grid's webhook deliver
//! by calling Loams. A [`WebhookHandler`] verifies and answers each request
//! (signatures, the services' handshakes) and hands its events to a
//! [`PushSource`] through a [`PushInbox`]; the request is answered `200`
//! only after the Fabric has acknowledged the events, and `503` with
//! `Retry-After` when the source is busy or stopping, so the service
//! redelivers (its retry policy is the backpressure, CN2 Task 4 "Push
//! sources apply backpressure by their protocol").
//!
//! The handlers are reached through the webhooks route, which CN2 Task 8
//! moves into the HTTP family and wires per instance (`webhook(instance)` on
//! the SNS and Event Grid factories). Until Task 28 opens remote ingress
//! (Q30), a cloud service cannot reach that route, so these sources run on
//! requests shaped like the services' in tests and on fixtures.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorError, EventBatch, Position, Source, SourceBatch, SourceFactory,
    Split, SplitEnumerator, SplitState,
};
use tokio::sync::{Mutex, mpsc, oneshot};

/// A webhook endpoint of a push source.
#[async_trait]
pub trait WebhookHandler: Send + Sync + fmt::Debug {
    async fn handle(&self, req: http::Request<Bytes>) -> http::Response<Bytes>;
}

/// Why a delivery was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushError {
    /// The source has as many deliveries waiting as it holds.
    Busy,
    /// The source is not running.
    Closed,
    /// The Fabric did not acknowledge in time.
    TimedOut,
    /// The delivery is larger than a whole batch.
    TooLarge,
    /// The source stopped before the Fabric acknowledged.
    Abandoned,
}

impl PushError {
    /// The HTTP answer: `413` for a delivery that can never fit, `503` with
    /// `Retry-After` otherwise.
    pub fn response(&self) -> http::Response<Bytes> {
        match self {
            PushError::TooLarge => response(413, "delivery larger than a batch"),
            other => {
                let mut r = response(503, &format!("not accepted now: {other:?}"));
                r.headers_mut().insert(
                    http::header::RETRY_AFTER,
                    http::HeaderValue::from_static("1"),
                );
                r
            }
        }
    }
}

/// A plain-text response.
pub fn response(status: u16, body: &str) -> http::Response<Bytes> {
    let mut r = http::Response::new(Bytes::from(body.to_owned()));
    *r.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::OK);
    r
}

/// A JSON response.
pub fn json_response(status: u16, v: &serde_json::Value) -> http::Response<Bytes> {
    let mut r = response(status, &v.to_string());
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    r
}

#[derive(Debug)]
struct Delivery {
    events: Vec<CloudEvent>,
    done: oneshot::Sender<Result<(), PushError>>,
}

/// The handler's side: deliver events and wait for the Fabric.
#[derive(Debug, Clone)]
pub struct PushInbox {
    tx: mpsc::Sender<Delivery>,
    ack_timeout: Duration,
}

impl PushInbox {
    /// Hand `events` to the source and wait until the Fabric has them.
    pub async fn deliver(&self, events: Vec<CloudEvent>) -> Result<(), PushError> {
        if events.is_empty() {
            return Ok(());
        }
        let (done, wait) = oneshot::channel();
        self.tx
            .try_send(Delivery { events, done })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => PushError::Busy,
                mpsc::error::TrySendError::Closed(_) => PushError::Closed,
            })?;
        match tokio::time::timeout(self.ack_timeout, wait).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(PushError::Abandoned),
            Err(_) => Err(PushError::TimedOut),
        }
    }
}

/// A push source's channel: `capacity` deliveries may wait, and a handler
/// waits `ack_timeout` for the Fabric (the services time a delivery out
/// after about 15 s for SNS and 30 s for Event Grid, then retry).
pub fn channel(capacity: usize, ack_timeout: Duration) -> (PushInbox, PushSourceFactory) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (
        PushInbox { tx, ack_timeout },
        PushSourceFactory {
            rx: Arc::new(Mutex::new(rx)),
            name: Arc::from("webhook"),
        },
    )
}

/// One split, `webhook`: a push source has no parallelism of its own.
#[derive(Debug, Clone, Copy, Default)]
pub struct OneSplit;

#[async_trait]
impl SplitEnumerator for OneSplit {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new("webhook", serde_json::Value::Null)])
    }
}

/// Makes the push source; a restarted split reads the same channel.
#[derive(Debug, Clone)]
pub struct PushSourceFactory {
    rx: Arc<Mutex<mpsc::Receiver<Delivery>>>,
    name: Arc<str>,
}

impl SourceFactory for PushSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(PushSource {
            rx: self.rx.clone(),
            name: self.name.clone(),
            held: None,
            seq: 0,
            inflight: BTreeMap::new(),
        }))
    }
}

/// The source side: batches of deliveries, each answered once the Fabric
/// acknowledges its batch.
#[derive(Debug)]
pub struct PushSource {
    rx: Arc<Mutex<mpsc::Receiver<Delivery>>>,
    name: Arc<str>,
    /// A delivery received that did not fit the last budget.
    held: Option<Delivery>,
    seq: u64,
    inflight: BTreeMap<u64, Vec<oneshot::Sender<Result<(), PushError>>>>,
}

fn size(d: &Delivery) -> usize {
    d.events.iter().map(CloudEvent::size_bytes).sum()
}

#[async_trait]
impl Source for PushSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.seq = pos.and_then(|p| p.as_u64()).unwrap_or(0);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let mut rx = self.rx.lock().await;
        let first = match self.held.take() {
            Some(d) => d,
            None => match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
                Ok(Some(d)) => d,
                Ok(None) | Err(_) => return Ok(None),
            },
        };
        let mut events = Vec::new();
        let mut dones = Vec::new();
        let mut bytes = 0usize;
        let mut next = Some(first);
        while let Some(d) = next.take() {
            let (n, b) = (d.events.len(), size(&d));
            if n > budget.max_rows || b > budget.max_bytes {
                let _ = d.done.send(Err(PushError::TooLarge));
            } else if events.len() + n > budget.max_rows || bytes + b > budget.max_bytes {
                self.held = Some(d);
                break;
            } else {
                bytes += b;
                events.extend(d.events);
                dones.push(d.done);
            }
            next = rx.try_recv().ok();
        }
        if events.is_empty() {
            return Ok(None);
        }
        self.seq += 1;
        self.inflight.insert(self.seq, dones);
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(
                Bytes::copy_from_slice(&self.seq.to_be_bytes()),
                format!("{} batch {}", self.name, self.seq),
            ),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some(n) = upto.as_u64() else {
            return Ok(());
        };
        let later = self.inflight.split_off(&(n + 1));
        for done in std::mem::replace(&mut self.inflight, later)
            .into_values()
            .flatten()
        {
            let _ = done.send(Ok(()));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        for done in std::mem::take(&mut self.inflight).into_values().flatten() {
            let _ = done.send(Err(PushError::Abandoned));
        }
        if let Some(d) = self.held.take() {
            let _ = d.done.send(Err(PushError::Abandoned));
        }
        Ok(())
    }
}

/// The handlers a factory's push sources registered, by instance, for the
/// webhooks route.
#[derive(Debug, Default)]
pub struct WebhookRegistry {
    by_instance:
        std::sync::Mutex<BTreeMap<loams_connector_sdk::InstanceKey, Arc<dyn WebhookHandler>>>,
}

impl WebhookRegistry {
    pub fn insert(&self, i: loams_connector_sdk::InstanceKey, h: Arc<dyn WebhookHandler>) {
        self.by_instance
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(i, h);
    }

    pub fn get(&self, i: &loams_connector_sdk::InstanceKey) -> Option<Arc<dyn WebhookHandler>> {
        self.by_instance
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(i)
            .cloned()
    }

    pub fn remove(&self, i: &loams_connector_sdk::InstanceKey) {
        self.by_instance
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(i);
    }
}

/// The query parameter `name` of `uri`, percent-decoded.
pub fn query_param(uri: &http::Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == name).then(|| pct_decode(v))
    })
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(h) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(h);
            i += 3;
            continue;
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Constant-time equality, for webhook keys.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(id: &str) -> CloudEvent {
        CloudEvent::new(id, "/s", "t")
    }

    #[tokio::test]
    async fn answered_after_commit_and_busy_when_full() {
        let (inbox, factory) = channel(1, Duration::from_secs(5));
        let mut src = factory
            .create(&Split::new("webhook", serde_json::Value::Null))
            .expect("source");
        src.open(&Split::new("webhook", serde_json::Value::Null), None)
            .await
            .expect("open");
        let i2 = inbox.clone();
        let waiting = tokio::spawn(async move { i2.deliver(vec![ev("a"), ev("b")]).await });
        tokio::task::yield_now().await;
        let big = Budget {
            max_rows: 10,
            max_bytes: 1 << 20,
        };
        let b = loop {
            if let Some(b) = src.next(big).await.expect("next") {
                break b;
            }
        };
        assert_eq!(b.events.rows(), 2);
        // The channel holds one waiting delivery; a second is refused.
        let i3 = inbox.clone();
        let queued = tokio::spawn(async move { i3.deliver(vec![ev("c")]).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(inbox.deliver(vec![ev("d")]).await, Err(PushError::Busy));
        assert!(!waiting.is_finished(), "not answered before the commit");
        src.committed(&b.position).await.expect("commit");
        assert_eq!(waiting.await.expect("join"), Ok(()));
        // A delivery larger than any batch is refused; a stop abandons.
        let tiny = Budget {
            max_rows: 1,
            max_bytes: 1 << 20,
        };
        let _ = src.next(tiny).await.expect("next");
        src.close().await.expect("close");
        assert_eq!(queued.await.expect("join"), Err(PushError::Abandoned));
        assert_eq!(PushError::Busy.response().status(), 503);
        assert_eq!(PushError::TooLarge.response().status(), 413);
    }

    #[test]
    fn helpers() {
        let uri: http::Uri = "/x?a=1&code=k%2By&b".parse().expect("uri");
        assert_eq!(query_param(&uri, "code").as_deref(), Some("k+y"));
        assert_eq!(query_param(&uri, "b").as_deref(), Some(""));
        assert!(query_param(&uri, "z").is_none());
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
