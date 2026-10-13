//! `upsert` and `create` resources as sinks: each event's JSON `data` is an
//! item; items go `batch` at a time, and a retried request carries the same
//! `Idempotency-Key` as the first attempt.
//!
//! Task 20 adds how a body is built and acknowledged: `select` (the part of
//! the data written) and `wrap` (where it goes in each item), `envelope` (a
//! fixed body the batch goes into), `encoding` (`json`, `ndjson`, `form`),
//! `{item:<pointer>}` paths (one request per item), `conflict_ok` and `ack`
//! (Splunk HEC's indexer acknowledgement).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use loams_connector_sdk::{
    AckSpec, BodyEncoding, ConnectorError, EventBatch, HttpMethod, Sink, SinkAck, SinkFactory,
    SinkHandle, WriteOp,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::{Engine, SendOpts, put_at, sigv4, template};

const IDEMPOTENCY_KEY: &str = "idempotency-key";

/// The default wait between acknowledgement polls, and for all of them.
const DEFAULT_ACK_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub(crate) struct HttpApiSinkFactory {
    engine: Arc<Engine>,
    op: WriteOp,
    resource: String,
}

impl HttpApiSinkFactory {
    pub(crate) fn new(engine: Arc<Engine>, op: WriteOp, resource: String) -> Self {
        Self {
            engine,
            op,
            resource,
        }
    }
}

impl SinkFactory for HttpApiSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        if self.op.encoding == BodyEncoding::Form && self.op.batch.is_some_and(|b| b > 1) {
            return Err(ConnectorError::fatal(format!(
                "the {} resource is form-encoded and cannot batch items",
                self.resource
            )));
        }
        Ok(SinkHandle::Plain(Box::new(HttpApiSink {
            engine: self.engine.clone(),
            op: self.op.clone(),
            resource: self.resource.clone(),
        })))
    }
}

#[derive(Debug)]
struct HttpApiSink {
    engine: Arc<Engine>,
    op: WriteOp,
    resource: String,
}

/// The key a request is retried under: the event's id for one event, else a
/// digest of the batch's ids in order (the pipeline retries the same batch,
/// cut the same way, so a retry repeats it).
fn idempotency_key(ids: &[&str]) -> String {
    if let [one] = ids {
        return (*one).to_string();
    }
    let mut d = Sha256::new();
    for id in ids {
        d.update(id.as_bytes());
        d.update(b"\n");
    }
    let hex: String = d
        .finalize()
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("loams-{hex}")
}

/// `application/x-www-form-urlencoded` of a JSON object, nested values as
/// `a[b]=c` and `a[0]=c` (Stripe's and Twilio's convention); `null` is left
/// out.
fn form_encode(item: &Value) -> Result<String, ConnectorError> {
    fn walk(prefix: &str, v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Null => {}
            Value::Object(m) => {
                // Sorted, so a body is the same however the map is ordered.
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                for k in keys {
                    let x = &m[k];
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}[{k}]")
                    };
                    walk(&key, x, out);
                }
            }
            Value::Array(a) => {
                for (i, x) in a.iter().enumerate() {
                    walk(&format!("{prefix}[{i}]"), x, out);
                }
            }
            Value::String(s) => out.push(format!(
                "{}={}",
                sigv4::uri_encode(prefix, true),
                sigv4::uri_encode(s, true)
            )),
            other => out.push(format!(
                "{}={}",
                sigv4::uri_encode(prefix, true),
                sigv4::uri_encode(&other.to_string(), true)
            )),
        }
    }
    if !item.is_object() {
        return Err(ConnectorError::poison(
            "a form-encoded write needs a JSON object",
        ));
    }
    let mut out = Vec::new();
    walk("", item, &mut out);
    Ok(out.join("&"))
}

impl HttpApiSink {
    /// The fixed body with its `{auth.<field>}` secrets resolved, or `null`.
    async fn envelope(&self) -> Result<Value, ConnectorError> {
        let Some(env) = &self.op.envelope else {
            return Ok(Value::Null);
        };
        let mut out = env.clone();
        resolve(&self.engine, &mut out).await?;
        Ok(out)
    }

    /// Post the ack ids until the API says each is done; a timeout is
    /// retryable (the pipeline resends the batch: at least once).
    async fn wait_for_acks(&self, ack: &AckSpec, ids: Vec<Value>) -> Result<(), ConnectorError> {
        if ids.is_empty() {
            return Ok(());
        }
        let interval = ack
            .interval_ms
            .map_or(DEFAULT_ACK_INTERVAL, Duration::from_millis);
        let deadline = Instant::now()
            + ack
                .timeout_ms
                .map_or(DEFAULT_ACK_TIMEOUT, Duration::from_millis);
        let channel = self.channel_header(ack)?;
        let url = self.engine.url(&ack.path, None, &[]);
        let mut pending = ids;
        loop {
            let mut body = Value::Null;
            put_at(&mut body, &ack.request, Value::Array(pending.clone()));
            let opts = SendOpts {
                label: Some(&ack.path),
                ..SendOpts::default()
            };
            let resp = self
                .engine
                .send(
                    HttpMethod::Post,
                    &url,
                    Some(Bytes::from(body.to_string())),
                    std::slice::from_ref(&channel),
                    opts,
                )
                .await?;
            let doc: Value = serde_json::from_slice(resp.body())
                .map_err(|_| ConnectorError::retryable("the ack response is not JSON"))?;
            let status = doc.pointer(&ack.status);
            pending.retain(|id| {
                let key = match id {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                status.and_then(|s| s.get(&key)) != Some(&Value::Bool(true))
            });
            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ConnectorError::retryable(format!(
                    "{} of the {} batch's requests were not acknowledged in time; resending",
                    pending.len(),
                    self.resource
                )));
            }
            tokio::time::sleep(interval).await;
        }
    }

    fn channel_header(&self, ack: &AckSpec) -> Result<(HeaderName, HeaderValue), ConnectorError> {
        Ok((
            HeaderName::from_bytes(ack.channel_header.as_bytes()).map_err(|_| {
                ConnectorError::fatal(format!("{:?} is not a header", ack.channel_header))
            })?,
            HeaderValue::from_str(self.engine.channel())
                .map_err(|e| ConnectorError::fatal(e.to_string()))?,
        ))
    }
}

/// Resolve `{auth.<field>}` in every string leaf of a profile-written value.
async fn resolve(engine: &Engine, v: &mut Value) -> Result<(), ConnectorError> {
    let mut stack = vec![v];
    while let Some(v) = stack.pop() {
        match v {
            Value::String(s) if s.contains("{auth.") => {
                *s = engine.with_secrets(s, false).await?;
            }
            Value::Array(a) => stack.extend(a.iter_mut()),
            Value::Object(m) => stack.extend(m.values_mut()),
            _ => {}
        }
    }
    Ok(())
}

#[async_trait]
impl Sink for HttpApiSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::fatal(format!(
                "the {} resource takes events, not Arrow batches",
                self.resource
            )));
        };
        let mut items = Vec::with_capacity(events.len());
        for ev in &events {
            let data: Value = serde_json::from_slice(&ev.data).map_err(|_| {
                ConnectorError::poison(format!("event {} has no JSON data to write", ev.id))
            })?;
            if let Some(key) = &self.op.key
                && data.pointer(key).is_none()
            {
                return Err(ConnectorError::poison(format!(
                    "event {} has no key at {key} for the {} upsert",
                    ev.id, self.resource
                )));
            }
            let mut item = match &self.op.select {
                Some(p) => data.pointer(p).cloned().ok_or_else(|| {
                    ConnectorError::poison(format!(
                        "event {} has nothing at {p} to write to {}",
                        ev.id, self.resource
                    ))
                })?,
                None => data.clone(),
            };
            if let Some(w) = &self.op.wrap {
                let mut wrapped = Value::Null;
                put_at(&mut wrapped, w, item);
                item = wrapped;
            }
            items.push((ev.id.as_str(), data, item));
        }
        let per_item_path = template::has_item(&self.op.path);
        let per_request = if per_item_path || self.op.encoding == BodyEncoding::Form {
            1
        } else {
            self.op.batch.unwrap_or(1).max(1)
        };
        let path = self.engine.with_secrets(&self.op.path, true).await?;
        let envelope = self.envelope().await?;
        let content_type = match self.op.encoding {
            BodyEncoding::Json => "application/json",
            BodyEncoding::Ndjson => "application/x-ndjson",
            BodyEncoding::Form => "application/x-www-form-urlencoded",
        };
        let channel = self
            .op
            .ack
            .as_ref()
            .map(|a| self.channel_header(a))
            .transpose()?;
        let mut ack = SinkAck::default();
        let mut ack_ids = Vec::new();
        for chunk in items.chunks(per_request) {
            let url = if per_item_path {
                self.engine
                    .url(&template::item(&path, &chunk[0].1)?, None, &[])
            } else {
                self.engine.url(&path, None, &[])
            };
            let body = match self.op.encoding {
                BodyEncoding::Form => form_encode(&chunk[0].2)?,
                BodyEncoding::Ndjson => chunk
                    .iter()
                    .map(|(_, _, v)| v.to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
                BodyEncoding::Json => match (self.op.batch, &self.op.body) {
                    (None, None) if envelope.is_null() => chunk[0].2.to_string(),
                    (None, None) => {
                        let mut root = envelope.clone();
                        merge_into(&mut root, &chunk[0].2);
                        root.to_string()
                    }
                    (_, pointer) => {
                        let array = Value::Array(chunk.iter().map(|(_, _, v)| v.clone()).collect());
                        let mut root = envelope.clone();
                        put_at(&mut root, pointer.as_deref().unwrap_or(""), array);
                        root.to_string()
                    }
                },
            };
            let body = Bytes::from(body);
            let mut extra = Vec::new();
            if self.op.idempotency_key {
                let ids: Vec<&str> = chunk.iter().map(|(id, _, _)| *id).collect();
                extra.push((
                    HeaderName::from_static(IDEMPOTENCY_KEY),
                    HeaderValue::from_str(&idempotency_key(&ids)).map_err(|_| {
                        ConnectorError::poison("an event id is not a valid header value")
                    })?,
                ));
            }
            if let Some(c) = &channel {
                extra.push(c.clone());
            }
            let len = body.len() as u64;
            let opts = SendOpts {
                content_type: Some(content_type),
                conflict_ok: self.op.conflict_ok,
                write: true,
                label: Some(&self.op.path),
            };
            let resp = self
                .engine
                .send(self.op.method, &url, Some(body), &extra, opts)
                .await?;
            if let Some(spec) = &self.op.ack {
                let doc: Value = serde_json::from_slice(resp.body()).unwrap_or(Value::Null);
                let id = doc.pointer(&spec.id).cloned().ok_or_else(|| {
                    ConnectorError::fatal(format!(
                        "the {} write was not given an ack id at {} (is acknowledgement enabled on the token?)",
                        self.resource, spec.id
                    ))
                })?;
                ack_ids.push(id);
            }
            ack.rows += chunk.len() as u64;
            ack.bytes += len;
        }
        if let Some(spec) = &self.op.ack {
            self.wait_for_acks(spec, ack_ids).await?;
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Every write is sent (and acknowledged) before `write` returns.
        Ok(SinkAck::default())
    }
}

/// `item`'s fields over `root`'s (an envelope around a single item).
fn merge_into(root: &mut Value, item: &Value) {
    match (root, item) {
        (Value::Object(r), Value::Object(i)) => {
            for (k, v) in i {
                r.insert(k.clone(), v.clone());
            }
        }
        (r, i) => *r = i.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys() {
        assert_eq!(idempotency_key(&["ev-1"]), "ev-1");
        let k = idempotency_key(&["a", "b"]);
        assert!(k.starts_with("loams-") && k.len() == 38);
        assert_eq!(k, idempotency_key(&["a", "b"]));
        assert_ne!(k, idempotency_key(&["b", "a"]));
    }

    #[test]
    fn form_bodies_flatten_with_brackets() {
        let body = form_encode(&json!({
            "email": "a+b@example.test", "metadata": {"tier": "gold"},
            "tags": ["x", "y"], "n": 3, "skip": null
        }))
        .expect("form");
        assert_eq!(
            body,
            "email=a%2Bb%40example.test&metadata%5Btier%5D=gold&n=3&tags%5B0%5D=x&tags%5B1%5D=y"
        );
        assert!(form_encode(&json!([1])).is_err());
    }
}
