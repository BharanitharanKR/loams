//! Qdrant (`runtime.ref: loams_connector_search::qdrant`), over Qdrant's REST
//! API through the SDK's egress-checked client (ruling R15.2).
//!
//! * **Sink:** each event becomes a point whose id is **UUID v5 of the
//!   document key** under [`POINT_NAMESPACE`], so writing it again
//!   overwrites the same point (`effectively_once`). The vector and payload
//!   come from the instance's JSON pointers; `loamsop = d` deletes the
//!   point. Consecutive upserts share a request (`PUT …/points?wait=true`),
//!   and a delete starts a new one, so per-key order holds.
//! * **Source:** one split per collection, read with `scroll` in point-id
//!   order; the position is the offset (the id of the next point to read),
//!   so a page cut short by the budget resumes at the first point not taken.
//!   Event ids are `<collection>/<point id>@<content digest>`: an unchanged
//!   point re-read has the same id (deduplicated by `ingest`), a changed one
//!   a new id.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder,
    ErrorClass, EventBatch, Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use tokio::time::Instant;
use uuid::Uuid;

use crate::bulk::{data_json, is_delete, key_at};
use crate::client::{AuthSpec, Client, hex, read_error};
use crate::config::{QdrantConfig, pointer};

/// The namespace of point ids: UUID v5 of the URL
/// `https://loams.dev/flow/connectors/qdrant/point` under RFC 9562's URL
/// namespace (checked by a unit test).
pub const POINT_NAMESPACE: Uuid = Uuid::from_u128(0x6d6efca0_be1a_57d0_aab8_fa00e89cbe92);

/// The point id of a document key.
pub fn point_id(key: &str) -> Uuid {
    Uuid::new_v5(&POINT_NAMESPACE, key.as_bytes())
}

/// See the module documentation.
#[derive(Debug, Clone, Copy, Default)]
pub struct Qdrant;

impl Qdrant {
    pub const fn new() -> Self {
        Qdrant
    }
}

async fn connect(cfg: &QdrantConfig, ctx: &ConnectorContext) -> Result<Client, ConnectorError> {
    Client::connect(
        &cfg.url,
        &AuthSpec {
            qdrant_api_key: cfg.api_key.clone(),
            ..AuthSpec::default()
        },
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_millis(cfg.request_timeout_ms),
    )
    .await
}

fn collection_path(c: &str) -> String {
    format!("/collections/{c}")
}

impl ConnectorFactory for Qdrant {
    fn id(&self) -> &'static str {
        "qdrant"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Arc::new(QdrantConfig::parse(&ctx.config)?);
        Ok(Some((
            Box::new(CollectionEnumerator {
                collection: cfg.collection.clone(),
            }),
            Box::new(ScrollSourceFactory {
                cfg,
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(QdrantSinkFactory {
            cfg: Arc::new(QdrantConfig::parse(&ctx.config)?),
            ctx: ctx.clone(),
        })))
    }
}

// ------------------------------------------------------------------ sink --

/// One write.
#[derive(Debug, Clone, PartialEq)]
pub enum PointOp {
    Upsert(Value),
    Delete(String),
}

/// The point op of one event.
pub fn point_op(ev: &CloudEvent, cfg: &QdrantConfig) -> Result<PointOp, ConnectorError> {
    let data = data_json(ev)?;
    let delete = cfg.apply_deletes && is_delete(ev);
    let key = match &cfg.key {
        Some(k) => match key_at(data.as_ref(), &pointer(k), ev) {
            Ok(key) => key,
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
    let id = point_id(&key).to_string();
    if delete {
        return Ok(PointOp::Delete(id));
    }
    let Some(Value::Object(obj)) = data else {
        return Err(ConnectorError::poison(format!(
            "event {}: a point needs a JSON object",
            ev.id
        )));
    };
    let doc = Value::Object(obj);
    let vptr = pointer(&cfg.vector);
    let vector = match doc.pointer(&vptr) {
        Some(v @ Value::Array(a)) if !a.is_empty() && a.iter().all(Value::is_number) => v.clone(),
        Some(v @ Value::Object(_)) => v.clone(),
        _ => {
            return Err(ConnectorError::poison(format!(
                "event {}: no vector at {vptr}",
                ev.id
            )));
        }
    };
    let payload = match &cfg.payload {
        Some(p) => match doc.pointer(&pointer(p)) {
            Some(v @ Value::Object(_)) => v.clone(),
            None | Some(Value::Null) => Value::Object(Map::new()),
            Some(_) => {
                return Err(ConnectorError::poison(format!(
                    "event {}: the payload is not an object",
                    ev.id
                )));
            }
        },
        None => {
            let mut m = doc.as_object().cloned().unwrap_or_default();
            // A top-level vector is not payload.
            if let Some(field) = vptr.strip_prefix('/').filter(|f| !f.contains('/')) {
                m.remove(field);
            }
            Value::Object(m)
        }
    };
    Ok(PointOp::Upsert(
        json!({ "id": id, "vector": vector, "payload": payload }),
    ))
}

/// The Qdrant sink.
pub struct QdrantSink {
    cfg: Arc<QdrantConfig>,
    ctx: ConnectorContext,
    client: OnceCell<Client>,
}

impl std::fmt::Debug for QdrantSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantSink")
            .field("collection", &self.cfg.collection)
            .finish_non_exhaustive()
    }
}

impl QdrantSink {
    async fn client(&self) -> Result<&Client, ConnectorError> {
        self.client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await
    }

    async fn flush_run(&self, run: &mut Vec<PointOp>) -> Result<(), ConnectorError> {
        if run.is_empty() {
            return Ok(());
        }
        let base = collection_path(&self.cfg.collection);
        let (method, target, body) = match &run[0] {
            PointOp::Upsert(_) => (
                Method::PUT,
                format!("{base}/points?wait=true"),
                json!({ "points": run.iter().filter_map(|o| match o {
                    PointOp::Upsert(p) => Some(p.clone()),
                    PointOp::Delete(_) => None,
                }).collect::<Vec<_>>() }),
            ),
            PointOp::Delete(_) => (
                Method::POST,
                format!("{base}/points/delete?wait=true"),
                json!({ "points": run.iter().filter_map(|o| match o {
                    PointOp::Delete(id) => Some(id.clone()),
                    PointOp::Upsert(_) => None,
                }).collect::<Vec<_>>() }),
            ),
        };
        let client = self.client().await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        client.json(method, &target, Some(&body)).await?;
        run.clear();
        Ok(())
    }
}

#[async_trait]
impl Sink for QdrantSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison(
                "the Qdrant sink takes JSON events, not Arrow batches",
            ));
        };
        let ops = events
            .iter()
            .map(|ev| point_op(ev, &self.cfg))
            .collect::<Result<Vec<_>, _>>()?;
        let ack = SinkAck {
            rows: events.len() as u64,
            bytes: events.iter().map(|e| e.size_bytes() as u64).sum(),
        };
        let mut run: Vec<PointOp> = Vec::new();
        for op in ops {
            let same = run.first().is_none_or(|f| {
                matches!(
                    (f, &op),
                    (PointOp::Upsert(_), PointOp::Upsert(_))
                        | (PointOp::Delete(_), PointOp::Delete(_))
                )
            });
            if !same || run.len() >= self.cfg.batch {
                self.flush_run(&mut run).await?;
            }
            run.push(op);
        }
        self.flush_run(&mut run).await?;
        Ok(ack)
    }

    /// `wait=true`: every write is applied before it returns.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Builds a [`QdrantSink`] per sink task.
pub struct QdrantSinkFactory {
    pub cfg: Arc<QdrantConfig>,
    pub ctx: ConnectorContext,
}

impl std::fmt::Debug for QdrantSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantSinkFactory")
            .field("collection", &self.cfg.collection)
            .finish_non_exhaustive()
    }
}

impl SinkFactory for QdrantSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(QdrantSink {
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
        })))
    }
}

// ---------------------------------------------------------------- source --

/// The scroll source's position (JSON in `Position::opaque`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScrollPosition {
    /// The id of the next point to read; `None` is the first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<Value>,
    #[serde(default)]
    pub pass_done: bool,
}

impl ScrollPosition {
    pub fn to_position(&self) -> Position {
        let display = match (&self.offset, self.pass_done) {
            (_, true) => "pass done".to_owned(),
            (Some(o), false) => format!("offset {o}"),
            (None, false) => "start".to_owned(),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque).map_err(|_| {
            ConnectorError::position_lost(format!(
                "the saved position ({}) is not a scroll offset",
                p.display
            ))
        })
    }
}

/// One split per collection.
#[derive(Debug)]
pub struct CollectionEnumerator {
    pub collection: String,
}

#[async_trait]
impl SplitEnumerator for CollectionEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.collection.clone(),
            json!({ "collection": self.collection }),
        )])
    }
}

/// Builds a [`ScrollSource`] per split.
pub struct ScrollSourceFactory {
    pub cfg: Arc<QdrantConfig>,
    pub ctx: ConnectorContext,
}

impl std::fmt::Debug for ScrollSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrollSourceFactory")
            .field("collection", &self.cfg.collection)
            .finish_non_exhaustive()
    }
}

impl SourceFactory for ScrollSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(ScrollSource {
            cfg: self.cfg.clone(),
            builder: EnvelopeBuilder::new("qdrant", &self.ctx.instance),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
            split: split.clone(),
            pos: ScrollPosition::default(),
            wait_until: None,
            finished: false,
        }))
    }
}

/// The event of one point.
pub fn point_event(builder: &EnvelopeBuilder, collection: &str, point: &Value) -> CloudEvent {
    let id = match &point["id"] {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let data = json!({ "id": point["id"], "payload": point["payload"], "vector": point["vector"] });
    let bytes = serde_json::to_vec(&data).unwrap_or_default();
    let digest = hex(&Sha256::digest(&bytes)[..8]);
    builder
        .event(
            "document",
            collection,
            format!("{collection}/{id}@{digest}"),
        )
        .with_subject(id)
        .with_data("application/json", bytes)
}

/// See the module documentation.
pub struct ScrollSource {
    cfg: Arc<QdrantConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    client: OnceCell<Client>,
    split: Split,
    pos: ScrollPosition,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for ScrollSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrollSource")
            .field("split", &self.split.id)
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

impl ScrollSource {
    async fn client(&self) -> Result<&Client, ConnectorError> {
        self.client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await
    }

    fn end_pass(&mut self) {
        self.pos.offset = None;
        self.pos.pass_done = true;
        self.ctx
            .observe
            .lag(&self.ctx.instance, Some(&self.split.id), Duration::ZERO);
        match self.cfg.source.poll_interval_ms {
            Some(ms) => self.wait_until = Some(Instant::now() + Duration::from_millis(ms)),
            None => self.finished = true,
        }
    }
}

#[async_trait]
impl Source for ScrollSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.split = split.clone();
        self.pos = match &pos {
            Some(p) => ScrollPosition::from_position(p)?,
            None => ScrollPosition::default(),
        };
        self.finished = false;
        self.wait_until = None;
        if self.pos.pass_done {
            match self.cfg.source.poll_interval_ms {
                Some(ms) => self.wait_until = Some(Instant::now() + Duration::from_millis(ms)),
                None => self.finished = true,
            }
        }
        self.client().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.finished {
                return Ok(None);
            }
            if let Some(t) = self.wait_until.take() {
                tokio::time::sleep_until(t).await;
            }
            if self.pos.pass_done {
                self.pos = ScrollPosition::default();
            }
            let limit = self.cfg.source.page_size.min(budget.max_rows.max(1));
            let mut body = json!({
                "limit": limit,
                "with_payload": true,
                "with_vector": self.cfg.source.with_vector,
            });
            if let Some(o) = &self.pos.offset {
                body["offset"] = o.clone();
            }
            if let Some(f) = &self.cfg.source.filter {
                body["filter"] = f.clone();
            }
            let target = format!("{}/points/scroll", collection_path(&self.cfg.collection));
            let client = self.client().await?;
            self.ctx.observe.api_call(&self.ctx.instance);
            let answer = client
                .json(Method::POST, &target, Some(&body))
                .await
                .map_err(read_error)?;
            let points = answer["result"]["points"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let next = answer["result"]["next_page_offset"].clone();
            if points.is_empty() {
                self.end_pass();
                continue;
            }
            let mut events = Vec::with_capacity(points.len());
            let mut bytes = 0;
            let mut cut_at = None;
            for (i, p) in points.iter().enumerate() {
                let ev = point_event(&self.builder, &self.cfg.collection, p);
                let sz = ev.size_bytes();
                if events.len() >= budget.max_rows || bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        // R4.7: never readable within the budget; skip it.
                        self.pos.offset = match points.get(i + 1) {
                            Some(n) => Some(n["id"].clone()),
                            None => (!next.is_null()).then(|| next.clone()),
                        };
                        if self.pos.offset.is_none() {
                            self.end_pass();
                        }
                        return Err(ConnectorError::new(
                            ErrorClass::Poison,
                            format!("point {} ({sz} bytes) exceeds the batch budget", p["id"]),
                        ));
                    }
                    cut_at = Some(i);
                    break;
                }
                bytes += sz;
                events.push(ev);
            }
            match cut_at {
                Some(i) => self.pos.offset = Some(points[i]["id"].clone()),
                None if next.is_null() => self.end_pass(),
                None => self.pos.offset = Some(next),
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(events),
                position: self.pos.to_position(),
            }));
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    #[test]
    fn namespace_is_the_documented_v5() {
        assert_eq!(
            POINT_NAMESPACE,
            Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                b"https://loams.dev/flow/connectors/qdrant/point"
            )
        );
        // Python: uuid.uuid5(<namespace>, "doc-1").
        assert_eq!(
            point_id("doc-1").to_string(),
            "b0fb0ea2-602d-5778-a8da-57557ff6ae13"
        );
    }

    fn cfg(v: Value) -> QdrantConfig {
        QdrantConfig::parse(&v).expect("cfg")
    }

    fn ev(data: Value) -> CloudEvent {
        CloudEvent::new("ce-1", "/s", "t").with_data("application/json", data.to_string())
    }

    #[test]
    fn payload_defaults_to_data_without_the_vector() {
        let c = cfg(json!({"url": "http://q", "collection": "c", "key": "k"}));
        let PointOp::Upsert(p) =
            point_op(&ev(json!({"k": "a", "vector": [1.0], "x": 2})), &c).expect("op")
        else {
            panic!("upsert");
        };
        assert_eq!(p["payload"], json!({"k": "a", "x": 2}));
        assert_eq!(p["id"], point_id("a").to_string());
        let named = point_op(&ev(json!({"k": "b", "vector": {"img": [0.5]}})), &c).expect("named");
        assert!(matches!(named, PointOp::Upsert(v) if v["vector"] == json!({"img": [0.5]})));
        let none = cfg(json!({"url": "http://q", "collection": "c"}));
        let PointOp::Upsert(p) = point_op(&ev(json!({"vector": [1]})), &none).expect("op") else {
            panic!("upsert");
        };
        assert_eq!(
            p["id"],
            point_id("ce-1").to_string(),
            "key defaults to ce_id"
        );
    }

    #[test]
    fn scroll_positions() {
        let p = ScrollPosition {
            offset: Some(json!("abc")),
            pass_done: false,
        };
        assert_eq!(
            ScrollPosition::from_position(&p.to_position()).expect("decode"),
            p
        );
        let b = EnvelopeBuilder::new("qdrant", &InstanceKey::new(1, "i"));
        let point = json!({"id": 5, "payload": {"a": 1}, "vector": [1.0]});
        let e = point_event(&b, "c", &point);
        assert!(e.id.starts_with("c/5@"));
        assert_eq!(e.subject.as_deref(), Some("5"));
        assert_eq!(point_event(&b, "c", &point).id, e.id);
        let changed = json!({"id": 5, "payload": {"a": 2}, "vector": [1.0]});
        assert_ne!(point_event(&b, "c", &changed).id, e.id);
    }
}
