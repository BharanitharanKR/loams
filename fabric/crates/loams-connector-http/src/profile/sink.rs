//! `upsert` and `create` resources as sinks: each event's JSON `data` is an
//! item; items go `batch` at a time, and a retried request carries the same
//! `Idempotency-Key` as the first attempt.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use loams_connector_sdk::{
    ConnectorError, EventBatch, Sink, SinkAck, SinkFactory, SinkHandle, WriteOp,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Engine, put_at};

const IDEMPOTENCY_KEY: &str = "idempotency-key";

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
            let item: Value = serde_json::from_slice(&ev.data).map_err(|_| {
                ConnectorError::poison(format!("event {} has no JSON data to write", ev.id))
            })?;
            if let Some(key) = &self.op.key
                && item.pointer(key).is_none()
            {
                return Err(ConnectorError::poison(format!(
                    "event {} has no key at {key} for the {} upsert",
                    ev.id, self.resource
                )));
            }
            items.push((ev.id.as_str(), item));
        }
        let per_request = self.op.batch.unwrap_or(1).max(1);
        let url = self.engine.url(&self.op.path, None, &[]);
        let mut ack = SinkAck::default();
        for chunk in items.chunks(per_request) {
            let body = match (self.op.batch, &self.op.body) {
                (None, None) => chunk[0].1.clone(),
                (_, pointer) => {
                    let array = Value::Array(chunk.iter().map(|(_, v)| v.clone()).collect());
                    let mut root = Value::Null;
                    put_at(&mut root, pointer.as_deref().unwrap_or(""), array);
                    root
                }
            };
            let body = Bytes::from(body.to_string());
            let mut extra = Vec::new();
            if self.op.idempotency_key {
                let ids: Vec<&str> = chunk.iter().map(|(id, _)| *id).collect();
                extra.push((
                    HeaderName::from_static(IDEMPOTENCY_KEY),
                    HeaderValue::from_str(&idempotency_key(&ids)).map_err(|_| {
                        ConnectorError::poison("an event id is not a valid header value")
                    })?,
                ));
            }
            let len = body.len() as u64;
            self.engine
                .send(self.op.method, &url, Some(body), &extra)
                .await?;
            ack.rows += chunk.len() as u64;
            ack.bytes += len;
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Every write is sent before `write` returns.
        Ok(SinkAck::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        assert_eq!(idempotency_key(&["ev-1"]), "ev-1");
        let k = idempotency_key(&["a", "b"]);
        assert!(k.starts_with("loams-") && k.len() == 38);
        assert_eq!(k, idempotency_key(&["a", "b"]));
        assert_ne!(k, idempotency_key(&["b", "a"]));
    }
}
