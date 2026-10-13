//! The `_bulk` sink (Elasticsearch and OpenSearch). Each event becomes an
//! `index` action with `_id` = the document key (or the event's `ce_id`), or
//! a `delete` by `_id` when `loamsop = d`, so a replay overwrites rather than
//! duplicates (`effectively_once`, OR-7).
//!
//! A partial failure retries **only the failed items**: an item answered 429
//! or 5xx is resent, alone, after a backoff; an item answered with another
//! 4xx (a mapping conflict, a parse error) is refused and fails the write as
//! `Poison`, so the pipeline isolates it event by event into the DLQ (R4.10)
//! while the rest stay written. A 429, for an item or for the whole request,
//! halves the documents per request; clean requests grow it back.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use http::{Method, StatusCode};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ErrorClass, EventBatch, Sink, SinkAck,
    SinkFactory, SinkHandle,
};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::client::{AuthSpec, Client, classify, parse_body, shorten};
use crate::config::{EsConfig, pointer};

/// One `_bulk` item.
#[derive(Debug, Clone)]
pub struct BulkOp {
    pub id: String,
    /// The document; `None` is a delete.
    pub doc: Option<Bytes>,
    /// The event's size, for the ack.
    pub bytes: usize,
}

impl BulkOp {
    fn encoded_len(&self) -> usize {
        self.id.len() + 64 + self.doc.as_ref().map_or(0, |d| d.len() + 1)
    }
}

/// Whether an event is a delete (`loamsop = d`).
pub fn is_delete(ev: &CloudEvent) -> bool {
    ev.extensions.get("loamsop").is_some_and(|op| op == "d")
}

/// An event's JSON data, if it has any.
pub fn data_json(ev: &CloudEvent) -> Result<Option<Value>, ConnectorError> {
    if ev.data.is_empty() {
        return Ok(None);
    }
    if let Some(ct) = &ev.datacontenttype
        && !ct.contains("json")
    {
        return Err(ConnectorError::poison(format!(
            "event {}: data is {ct}, not JSON",
            ev.id
        )));
    }
    serde_json::from_slice(&ev.data)
        .map(Some)
        .map_err(|e| ConnectorError::poison(format!("event {}: data is not JSON: {e}", ev.id)))
}

/// The key a pointer selects, as a string (`_id` and Qdrant keys).
pub fn key_at(data: Option<&Value>, ptr: &str, ev: &CloudEvent) -> Result<String, ConnectorError> {
    match data.and_then(|d| d.pointer(ptr)) {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        Some(Value::Bool(b)) => Ok(b.to_string()),
        _ => Err(ConnectorError::poison(format!(
            "event {}: no key at {ptr}",
            ev.id
        ))),
    }
}

/// The `_bulk` op of one event.
pub fn op_for(ev: &CloudEvent, cfg: &EsConfig) -> Result<BulkOp, ConnectorError> {
    let data = data_json(ev)?;
    let delete = cfg.apply_deletes && is_delete(ev);
    let id = match &cfg.key {
        Some(k) => match key_at(data.as_ref(), &pointer(k), ev) {
            Ok(id) => id,
            // A delete may carry only its key as the subject.
            Err(_) if delete && ev.subject.is_some() => ev.subject.clone().unwrap_or_default(),
            Err(e) => return Err(e),
        },
        None if delete => ev.subject.clone().ok_or_else(|| {
            ConnectorError::poison(format!(
                "event {}: a delete needs `key` or a subject",
                ev.id
            ))
        })?,
        None => ev.id.clone(),
    };
    if id.len() > 512 {
        return Err(ConnectorError::poison(format!(
            "event {}: _id longer than 512 bytes",
            ev.id
        )));
    }
    let doc = if delete {
        None
    } else {
        match data {
            Some(v @ Value::Object(_)) => Some(Bytes::from(
                serde_json::to_vec(&v).map_err(|e| ConnectorError::fatal(e.to_string()))?,
            )),
            _ => {
                return Err(ConnectorError::poison(format!(
                    "event {}: a document must be a JSON object",
                    ev.id
                )));
            }
        }
    };
    Ok(BulkOp {
        id,
        doc,
        bytes: ev.size_bytes(),
    })
}

/// The NDJSON body of a `_bulk` request.
pub fn encode(index: &str, ops: &[BulkOp]) -> Bytes {
    let mut out = BytesMut::with_capacity(ops.iter().map(BulkOp::encoded_len).sum());
    for op in ops {
        let action = if op.doc.is_some() { "index" } else { "delete" };
        let line = json!({ action: { "_index": index, "_id": op.id } });
        out.put_slice(line.to_string().as_bytes());
        out.put_u8(b'\n');
        if let Some(doc) = &op.doc {
            out.put_slice(doc);
            out.put_u8(b'\n');
        }
    }
    out.freeze()
}

/// What `_bulk` said about one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemOutcome {
    Done,
    /// 429 or 5xx: resend.
    Retry {
        status: u16,
    },
    /// Any other 4xx: never accepted.
    Refused {
        status: u16,
        reason: String,
    },
}

/// Parse a `_bulk` answer for `n` items.
pub fn parse_items(body: &Value, n: usize) -> Result<Vec<ItemOutcome>, ConnectorError> {
    let items = body["items"]
        .as_array()
        .ok_or_else(|| ConnectorError::retryable("a _bulk answer without items"))?;
    if items.len() != n {
        return Err(ConnectorError::retryable(format!(
            "a _bulk answer with {} items for {n} actions",
            items.len()
        )));
    }
    Ok(items
        .iter()
        .map(|item| {
            let (action, r) = item
                .as_object()
                .and_then(|o| o.iter().next())
                .map_or(("", &Value::Null), |(k, v)| (k.as_str(), v));
            let status = r["status"].as_u64().unwrap_or(500) as u16;
            match status {
                200..=299 => ItemOutcome::Done,
                // Deleting what is not there is what a delete wants.
                404 if action == "delete" => ItemOutcome::Done,
                429 | 500..=599 => ItemOutcome::Retry { status },
                _ => {
                    let e = &r["error"];
                    let reason = match (e["type"].as_str(), e["reason"].as_str()) {
                        (Some(t), Some(why)) => format!("{t}: {why}"),
                        (Some(t), None) => t.to_owned(),
                        _ => format!("status {status}"),
                    };
                    ItemOutcome::Refused {
                        status,
                        reason: shorten(&reason, 300),
                    }
                }
            }
        })
        .collect())
}

/// The `_bulk` sink.
pub struct BulkSink {
    cfg: Arc<EsConfig>,
    ctx: ConnectorContext,
    client: OnceCell<Client>,
    /// Documents per request now.
    chunk: usize,
}

impl std::fmt::Debug for BulkSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkSink")
            .field("index", &self.cfg.index)
            .field("chunk", &self.chunk)
            .finish_non_exhaustive()
    }
}

enum Sent {
    Items(Vec<ItemOutcome>),
    /// The whole request was refused with 429.
    Rejected(Option<Duration>),
}

pub fn es_auth(cfg: &EsConfig) -> AuthSpec {
    AuthSpec {
        basic: cfg.basic.clone(),
        es_api_key: cfg.api_key.clone(),
        qdrant_api_key: None,
        aws: cfg.aws.clone(),
    }
}

pub async fn connect(cfg: &EsConfig, ctx: &ConnectorContext) -> Result<Client, ConnectorError> {
    Client::connect(
        &cfg.url,
        &es_auth(cfg),
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_millis(cfg.request_timeout_ms),
    )
    .await
}

impl BulkSink {
    pub fn new(cfg: Arc<EsConfig>, ctx: ConnectorContext) -> Self {
        let chunk = cfg.bulk.max_docs;
        Self {
            cfg,
            ctx,
            client: OnceCell::new(),
            chunk,
        }
    }

    async fn client(&self) -> Result<&Client, ConnectorError> {
        self.client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await
    }

    fn shrink(&mut self) {
        self.chunk = (self.chunk / 2).max(1);
    }

    fn grow(&mut self) {
        self.chunk = (self.chunk * 2).min(self.cfg.bulk.max_docs);
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let ms = self
            .cfg
            .bulk
            .retry_backoff_ms
            .saturating_mul(1 << attempt.min(16));
        Duration::from_millis(ms.min(30_000))
    }

    async fn send(&self, ops: &[BulkOp]) -> Result<Sent, ConnectorError> {
        let body = encode(&self.cfg.index, ops);
        let client = self.client().await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        let resp = client
            .send(Method::POST, "/_bulk", body, Some("application/x-ndjson"))
            .await?;
        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            let after = resp
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs);
            return Ok(Sent::Rejected(after));
        }
        if let Some(err) = classify(&resp) {
            // A refused request as a whole (400 on the NDJSON itself) means
            // some document broke it: isolate event by event.
            return Err(err);
        }
        Ok(Sent::Items(parse_items(&parse_body(&resp)?, ops.len())?))
    }

    /// Write `ops`, retrying only failed items; see the module docs.
    async fn write_ops(&mut self, ops: Vec<BulkOp>) -> Result<(), ConnectorError> {
        let mut queue: VecDeque<BulkOp> = ops.into();
        let mut refused: Vec<String> = Vec::new();
        let mut attempt = 0u32;
        let max = self.cfg.bulk.max_item_retries;
        while !queue.is_empty() {
            let mut n = self.chunk.min(queue.len());
            // Cut by bytes too (at least one item per request).
            let mut size = 0;
            for (i, op) in queue.iter().take(n).enumerate() {
                size += op.encoded_len();
                if i > 0 && size > self.cfg.bulk.max_bytes {
                    n = i;
                    break;
                }
            }
            let window: Vec<BulkOp> = queue.drain(..n).collect();
            match self.send(&window).await? {
                Sent::Rejected(after) => {
                    self.shrink();
                    attempt += 1;
                    if attempt > max {
                        return Err(ConnectorError::throttled(
                            after.unwrap_or_else(|| self.backoff(attempt)),
                            format!("_bulk rejected with 429 {attempt} times"),
                        ));
                    }
                    tokio::time::sleep(after.unwrap_or_else(|| self.backoff(attempt))).await;
                    for op in window.into_iter().rev() {
                        queue.push_front(op);
                    }
                }
                Sent::Items(outcomes) => {
                    let mut retry = Vec::new();
                    let mut throttled = false;
                    let mut last_status = 0;
                    let mut progressed = false;
                    for (op, outcome) in window.into_iter().zip(outcomes) {
                        match outcome {
                            ItemOutcome::Done => progressed = true,
                            ItemOutcome::Retry { status } => {
                                throttled |= status == 429;
                                last_status = status;
                                retry.push(op);
                            }
                            ItemOutcome::Refused { status, reason } => {
                                refused.push(format!("_id {}: {status} {reason}", op.id));
                            }
                        }
                    }
                    if throttled {
                        self.shrink();
                    }
                    if progressed {
                        // Retries count failures in a row, not per write.
                        attempt = 0;
                    }
                    if retry.is_empty() {
                        if !throttled {
                            self.grow();
                        }
                        continue;
                    }
                    attempt += 1;
                    let wait = self.backoff(attempt);
                    if attempt > max {
                        let msg = format!(
                            "{} _bulk items still failing ({last_status}) after {max} retries",
                            retry.len()
                        );
                        return Err(if throttled {
                            ConnectorError::throttled(wait, msg)
                        } else {
                            ConnectorError::new(ErrorClass::Retryable { after: Some(wait) }, msg)
                        });
                    }
                    tracing::debug!(
                        index = %self.cfg.index,
                        items = retry.len(),
                        attempt,
                        "retrying the failed _bulk items"
                    );
                    tokio::time::sleep(wait).await;
                    for op in retry.into_iter().rev() {
                        queue.push_front(op);
                    }
                }
            }
        }
        if refused.is_empty() {
            Ok(())
        } else {
            Err(ConnectorError::poison(format!(
                "{} document(s) refused, first {}",
                refused.len(),
                refused[0]
            )))
        }
    }
}

#[async_trait]
impl Sink for BulkSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison(
                "the _bulk sink takes JSON documents, not Arrow batches",
            ));
        };
        let ops = events
            .iter()
            .map(|ev| op_for(ev, &self.cfg))
            .collect::<Result<Vec<_>, _>>()?;
        let ack = SinkAck {
            rows: ops.len() as u64,
            bytes: ops.iter().map(|o| o.bytes as u64).sum(),
        };
        self.write_ops(ops).await?;
        Ok(ack)
    }

    /// Every write is acknowledged by the cluster before it returns; there is
    /// nothing buffered.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Builds a [`BulkSink`] per sink task.
pub struct BulkSinkFactory {
    pub cfg: Arc<EsConfig>,
    pub ctx: ConnectorContext,
}

impl std::fmt::Debug for BulkSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkSinkFactory")
            .field("index", &self.cfg.index)
            .finish_non_exhaustive()
    }
}

impl SinkFactory for BulkSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(BulkSink::new(
            self.cfg.clone(),
            self.ctx.clone(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(v: Value) -> EsConfig {
        EsConfig::parse(&v).expect("config")
    }

    fn ev(id: &str, data: Value) -> CloudEvent {
        CloudEvent::new(id, "/s", "t").with_data("application/json", data.to_string())
    }

    #[test]
    fn ops_and_encoding() {
        let c = cfg(json!({"url": "http://x", "index": "i", "key": "user/id"}));
        let op = op_for(&ev("e", json!({"user": {"id": 7}})), &c).expect("op");
        assert_eq!(op.id, "7");
        let del = op_for(
            &ev("e2", json!({"user": {"id": "k"}})).with_extension("loamsop", "d"),
            &c,
        )
        .expect("delete");
        assert!(del.doc.is_none());
        let body = encode("i", &[op, del]);
        let text = String::from_utf8(body.to_vec()).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let line = |i: usize| serde_json::from_str::<Value>(lines[i]).expect("json");
        assert_eq!(line(0), json!({"index": {"_index": "i", "_id": "7"}}));
        assert_eq!(line(1), json!({"user": {"id": 7}}));
        assert_eq!(line(2), json!({"delete": {"_index": "i", "_id": "k"}}));
        assert!(text.ends_with('\n'), "_bulk needs a final newline");
    }

    #[test]
    fn bad_events_are_poison() {
        let c = cfg(json!({"url": "http://x", "index": "i", "key": "id"}));
        for e in [
            ev("a", json!({"no": "key"})),
            ev("b", json!([1, 2])),
            CloudEvent::new("c", "/s", "t").with_data("text/plain", "hi"),
        ] {
            assert_eq!(
                op_for(&e, &c).expect_err("poison").class,
                ErrorClass::Poison
            );
        }
        let no_deletes = cfg(json!({"url": "http://x", "index": "i", "apply_deletes": false}));
        let op = op_for(
            &ev("d", json!({"id": 1})).with_extension("loamsop", "d"),
            &no_deletes,
        )
        .expect("indexed, not deleted");
        assert!(op.doc.is_some());
    }

    #[test]
    fn item_outcomes() {
        let body = json!({"items": [
            {"index": {"status": 201}},
            {"index": {"status": 429}},
            {"index": {"status": 503}},
            {"delete": {"status": 404}},
            {"index": {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad"}}},
        ]});
        let out = parse_items(&body, 5).expect("parse");
        assert_eq!(out[0], ItemOutcome::Done);
        assert_eq!(out[1], ItemOutcome::Retry { status: 429 });
        assert_eq!(out[2], ItemOutcome::Retry { status: 503 });
        assert_eq!(out[3], ItemOutcome::Done);
        assert!(
            matches!(&out[4], ItemOutcome::Refused { status: 400, reason } if reason == "mapper_parsing_exception: bad")
        );
        assert!(
            parse_items(&body, 4).is_err(),
            "a count mismatch is not trusted"
        );
    }
}
