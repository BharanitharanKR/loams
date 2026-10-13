//! Grafeo (§48 §4.4, D741): a client of `loams.graph.v1` over Connect, with
//! the messages `loams-flow-proto` generates from the repository's
//! `proto/loams/graph/v1/graph.proto` (OR-20, ruling R21.4). Requests go
//! through the SDK's egress-checked client in the Connect protocol's binary
//! form (`application/proto` unary, `application/connect+proto` streams),
//! so no second HTTP stack dials outside the egress check.
//!
//! * **Auth `key`.** Every request carries `Authorization: Bearer <key>`, a
//!   Loams API key (`loams_<key_id>_<secret>`) resolved by secret name. The
//!   config requires one (`grafeo_requires_api_key`): the graph is no longer
//!   in-process (D634 (b) is dropped), so it is reached only with a key.
//! * **Sink:** `ExecuteBatch` with one parameterised statement per event.
//!   The event's top-level JSON fields are bound as `$name` parameters (plus
//!   `$ce_id`), never interpolated. `atomic: true` (the default) runs the
//!   batch as one transaction, and the `idempotency_key` is a digest of the
//!   batch's event ids, so a retried batch is the same request to the server.
//! * **Source:** `ExecuteStream` of a statement ordered by a cursor property
//!   and the node id, with `$cursor`, `$after` and `$limit`. The position is
//!   the last row's cursor `Value` (protobuf bytes, so it compares as itself)
//!   and node id. Event ids are `<graph>/<label>/<id>@<cursor>`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use buffa::{EnumValue, Message, MessageField};
use bytes::{BufMut, Bytes, BytesMut};
use http::Method;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder,
    ErrorClass, EventBatch, Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState,
};
use loams_flow_proto::loams::graph::v1 as pb;
use pb::__buffa::oneof::value::Kind;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::client::{AuthSpec, RestClient, hex, shorten, with_context};
use crate::template::data_json;

pub const SERVICE: &str = "/loams.graph.v1.GraphService";

/// The query language statements are written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    #[default]
    Gql,
    Cypher,
}

impl Language {
    fn proto(self) -> EnumValue<pb::QueryLanguage> {
        match self {
            Language::Gql => pb::QueryLanguage::QUERY_LANGUAGE_GQL.into(),
            Language::Cypher => pb::QueryLanguage::QUERY_LANGUAGE_CYPHER.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GraphSinkConfig {
    pub statement: String,
    #[serde(default = "yes")]
    pub atomic: bool,
    #[serde(default = "default_batch")]
    pub batch: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GraphSourceConfig {
    pub label: String,
    pub cursor: String,
    /// A custom statement with `$cursor`, `$after` and `$limit`, returning
    /// columns `cursor` and `eid` in that order of sorting.
    #[serde(default)]
    pub statement: Option<String>,
    #[serde(default = "default_batch")]
    pub page_size: usize,
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
}

fn yes() -> bool {
    true
}

fn default_batch() -> usize {
    100
}

fn default_timeout() -> u64 {
    30_000
}

/// The instance config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphConfig {
    /// The Loams endpoint serving `loams.graph.v1`.
    pub url: String,
    pub namespace: String,
    pub graph: String,
    /// The **name** of the secret holding the Loams API key. Required.
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub language: Language,
    #[serde(default)]
    pub sink: Option<GraphSinkConfig>,
    #[serde(default)]
    pub source: Option<GraphSourceConfig>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Json>,
}

impl GraphConfig {
    pub fn parse(v: &Json) -> Result<Self, ConnectorError> {
        let cfg: GraphConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("grafeo config: {e}")))?;
        if cfg.api_key.as_deref().is_none_or(|k| k.trim().is_empty()) {
            return Err(ConnectorError::auth(
                "grafeo needs `api_key`, the secret name of a Loams API key (auth: key)",
            ));
        }
        if let Some(s) = &cfg.source {
            if s.label.contains('`') || s.cursor.contains('`') || s.label.is_empty() {
                return Err(ConnectorError::fatal(
                    "source.label and source.cursor must be plain names",
                ));
            }
            if let Some(st) = &s.statement
                && !(st.contains("$cursor") && st.contains("$after") && st.contains("$limit"))
            {
                return Err(ConnectorError::fatal(
                    "source.statement must use $cursor, $after and $limit",
                ));
            }
        }
        Ok(cfg)
    }
}

/// The default source statement (GQL and Cypher read alike here).
pub fn source_statement(src: &GraphSourceConfig) -> String {
    if let Some(s) = &src.statement {
        return s.clone();
    }
    let (l, c) = (&src.label, &src.cursor);
    format!(
        "MATCH (n:`{l}`) WHERE n.`{c}` IS NOT NULL AND ($cursor IS NULL OR n.`{c}` > $cursor \
         OR (n.`{c}` = $cursor AND id(n) > $after)) \
         RETURN n, n.`{c}` AS cursor, id(n) AS eid ORDER BY cursor, eid LIMIT $limit"
    )
}

// ------------------------------------------------------------- values --

fn v(kind: Kind) -> pb::Value {
    pb::Value {
        kind: Some(kind),
        ..Default::default()
    }
}

/// A JSON value as a `loams.graph.v1.Value`.
pub fn to_value(j: &Json) -> pb::Value {
    match j {
        Json::Null => v(Kind::Null(EnumValue::from(0))),
        Json::Bool(b) => v(Kind::Boolean(*b)),
        Json::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => v(Kind::Int64(i)),
            (None, Some(u), _) => v(Kind::Uint64(u)),
            (_, _, f) => v(Kind::Float64(f.unwrap_or(f64::NAN))),
        },
        Json::String(s) => v(Kind::String(s.clone())),
        Json::Array(a) => v(Kind::List(Box::new(pb::ListValue {
            values: a.iter().map(to_value).collect(),
            ..Default::default()
        }))),
        Json::Object(m) => {
            let mut entries = buffa::__private::HashMap::default();
            for (k, val) in m {
                entries.insert(k.clone(), to_value(val));
            }
            v(Kind::Map(Box::new(pb::MapValue {
                entries,
                ..Default::default()
            })))
        }
    }
}

fn two(n: u32) -> String {
    format!("{n:02}")
}

fn local_time(t: &pb::LocalTime) -> String {
    let base = format!("{}:{}:{}", two(t.hour), two(t.minute), two(t.second));
    if t.nanosecond == 0 {
        base
    } else {
        format!("{base}.{:09}", t.nanosecond)
    }
}

fn date(d: &pb::Date) -> String {
    format!("{:04}-{}-{}", d.year, two(d.month), two(d.day))
}

fn offset(secs: i32) -> String {
    if secs == 0 {
        return "Z".into();
    }
    let sign = if secs < 0 { '-' } else { '+' };
    let a = secs.unsigned_abs();
    format!("{sign}{}:{}", two(a / 3600), two(a % 3600 / 60))
}

fn map_json<'a>(m: impl Iterator<Item = (&'a String, &'a pb::Value)>) -> Json {
    let mut out: Vec<_> = m.map(|(k, v)| (k.clone(), to_json(v))).collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Json::Object(out.into_iter().collect())
}

fn node_json(n: &pb::Node) -> Json {
    json!({"id": n.id, "labels": n.labels, "properties": map_json(n.properties.iter())})
}

fn rel_json(r: &pb::Relationship) -> Json {
    json!({"id": r.id, "type": r.r#type, "src": r.src, "dst": r.dst,
           "properties": map_json(r.properties.iter())})
}

/// A `loams.graph.v1.Value` as JSON: temporal values as ISO 8601 text,
/// bytes as base64, decimals as their text, nodes and relationships as
/// objects.
pub fn to_json(val: &pb::Value) -> Json {
    let Some(k) = &val.kind else {
        return Json::Null;
    };
    match k {
        Kind::Null(_) => Json::Null,
        Kind::Boolean(b) => json!(b),
        Kind::Int64(i) => json!(i),
        Kind::Uint64(u) => json!(u),
        Kind::Float64(f) => serde_json::Number::from_f64(*f).map_or(Json::Null, Json::Number),
        Kind::String(s) => json!(s),
        Kind::Bytes(b) => json!(base64::engine::general_purpose::STANDARD.encode(b)),
        Kind::Decimal(d) => json!(d.value),
        Kind::Date(d) => json!(date(d)),
        Kind::LocalTime(t) => json!(local_time(t)),
        Kind::ZonedTime(z) => json!(format!(
            "{}{}",
            z.time.as_option().map(local_time).unwrap_or_default(),
            offset(z.offset_seconds)
        )),
        Kind::LocalDatetime(l) => json!(format!(
            "{}T{}",
            l.date.as_option().map(date).unwrap_or_default(),
            l.time.as_option().map(local_time).unwrap_or_default()
        )),
        Kind::ZonedDatetime(z) => {
            let l = z.local.as_option();
            json!(format!(
                "{}T{}{}",
                l.and_then(|l| l.date.as_option())
                    .map(date)
                    .unwrap_or_default(),
                l.and_then(|l| l.time.as_option())
                    .map(local_time)
                    .unwrap_or_default(),
                offset(z.offset_seconds)
            ))
        }
        Kind::Duration(d) => json!({"months": d.months, "days": d.days, "nanos": d.nanos}),
        Kind::List(l) => Json::Array(l.values.iter().map(to_json).collect()),
        Kind::Map(m) => map_json(m.entries.iter()),
        Kind::Node(n) => node_json(n),
        Kind::Relationship(r) => rel_json(r),
        Kind::Path(p) => json!({
            "nodes": p.nodes.iter().map(node_json).collect::<Vec<_>>(),
            "relationships": p.relationships.iter().map(rel_json).collect::<Vec<_>>(),
        }),
        Kind::Vector(vec) => json!(vec.values),
        Kind::Counter(c) => {
            let pos: u64 = c.positive.values().sum();
            let neg: u64 = c.negative.values().sum();
            json!(i128::from(pos) - i128::from(neg))
        }
    }
}

// ------------------------------------------------------------ Connect --

/// A Connect error body (`{"code": …, "message": …}`) as a connector error.
pub fn connect_error(code: &str, message: &str, write: bool) -> ConnectorError {
    let msg = format!("grafeo {code}: {}", shorten(message, 300));
    match code {
        "unauthenticated" | "permission_denied" => ConnectorError::auth(msg),
        "resource_exhausted" => ConnectorError::throttled(Duration::from_secs(1), msg),
        "unavailable" | "deadline_exceeded" | "aborted" | "internal" | "unknown" => {
            ConnectorError::retryable(msg)
        }
        "not_found" | "unimplemented" => ConnectorError::fatal(msg),
        _ if write => ConnectorError::poison(msg),
        _ => ConnectorError::fatal(msg),
    }
}

/// One Connect envelope.
pub fn envelope(flags: u8, msg: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(5 + msg.len());
    b.put_u8(flags);
    b.put_u32(msg.len() as u32);
    b.put_slice(msg);
    b.freeze()
}

/// Split a Connect stream body into its messages, checking the end-stream
/// envelope's error.
pub fn unframe(mut body: &[u8], write: bool) -> Result<Vec<Bytes>, ConnectorError> {
    let mut out = Vec::new();
    while !body.is_empty() {
        if body.len() < 5 {
            return Err(ConnectorError::retryable("a truncated Connect envelope"));
        }
        let flags = body[0];
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        let Some(msg) = body.get(5..5 + len) else {
            return Err(ConnectorError::retryable("a truncated Connect envelope"));
        };
        if flags & 0x01 != 0 {
            return Err(ConnectorError::fatal(
                "a compressed Connect message was not asked for",
            ));
        }
        if flags & 0x02 != 0 {
            let end: Json = serde_json::from_slice(msg).unwrap_or(Json::Null);
            if let Some(e) = end.get("error") {
                return Err(connect_error(
                    e["code"].as_str().unwrap_or("unknown"),
                    e["message"].as_str().unwrap_or(""),
                    write,
                ));
            }
            return Ok(out);
        }
        out.push(Bytes::copy_from_slice(msg));
        body = &body[5 + len..];
    }
    Err(ConnectorError::retryable(
        "the Connect stream ended without its end-stream message",
    ))
}

async fn connect(cfg: &GraphConfig, ctx: &ConnectorContext) -> Result<RestClient, ConnectorError> {
    RestClient::connect(
        &cfg.url,
        &AuthSpec::Bearer(cfg.api_key.clone().unwrap_or_default()),
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_millis(cfg.request_timeout_ms),
        "",
    )
    .await?
    .with_headers(&[("connect-protocol-version", "1")])
}

/// A unary call.
pub async fn unary<Req: Message, Resp: Message>(
    client: &RestClient,
    method: &str,
    req: &Req,
    write: bool,
) -> Result<Resp, ConnectorError> {
    let resp = client
        .send(
            Method::POST,
            &format!("{SERVICE}/{method}"),
            Bytes::from(req.encode_to_vec()),
            Some("application/proto"),
            &[],
        )
        .await?;
    if !resp.status().is_success() {
        let body: Json = serde_json::from_slice(resp.body()).unwrap_or(Json::Null);
        let code = body["code"].as_str().map(str::to_owned).unwrap_or_else(|| {
            match resp.status().as_u16() {
                401 => "unauthenticated",
                403 => "permission_denied",
                404 => "unimplemented",
                429 => "resource_exhausted",
                503 => "unavailable",
                _ => "unknown",
            }
            .to_owned()
        });
        return Err(connect_error(
            &code,
            body["message"].as_str().unwrap_or(""),
            write,
        ));
    }
    Resp::decode_from_slice(resp.body())
        .map_err(|e| ConnectorError::retryable(format!("decoding {method}'s answer: {e}")))
}

// ------------------------------------------------------------------ sink --

/// The `ExecuteBatch` request for a batch of events.
pub fn batch_request(
    cfg: &GraphConfig,
    sink: &GraphSinkConfig,
    events: &[CloudEvent],
) -> Result<pb::ExecuteBatchRequest, ConnectorError> {
    let mut statements = Vec::with_capacity(events.len());
    let mut digest = Sha256::new();
    for ev in events {
        let Json::Object(fields) = data_json(ev)? else {
            return Err(ConnectorError::poison(format!(
                "event {}: parameters come from a JSON object",
                ev.id
            )));
        };
        let mut parameters = buffa::__private::HashMap::default();
        for (k, val) in &fields {
            parameters.insert(k.clone(), to_value(val));
        }
        parameters.insert("ce_id".to_owned(), to_value(&json!(ev.id)));
        statements.push(pb::Statement {
            statement: sink.statement.clone(),
            parameters,
            ..Default::default()
        });
        digest.update(ev.source.as_bytes());
        digest.update([0]);
        digest.update(ev.id.as_bytes());
        digest.update([0]);
    }
    Ok(pb::ExecuteBatchRequest {
        namespace: cfg.namespace.clone(),
        graph: cfg.graph.clone(),
        statements,
        atomic: sink.atomic,
        language: cfg.language.proto(),
        idempotency_key: format!("loams-flow-{}", &hex(&digest.finalize())[..32]),
        ..Default::default()
    })
}

/// The Grafeo sink.
pub struct GraphSink {
    cfg: Arc<GraphConfig>,
    ctx: ConnectorContext,
    client: OnceCell<RestClient>,
}

impl std::fmt::Debug for GraphSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphSink")
            .field("graph", &self.cfg.graph)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for GraphSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison("the Grafeo sink takes JSON events"));
        };
        let sink = self
            .cfg
            .sink
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no sink config"))?;
        let client = self
            .client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await?;
        for chunk in events.chunks(sink.batch.max(1)) {
            let req = batch_request(&self.cfg, &sink, chunk)?;
            self.ctx.observe.api_call(&self.ctx.instance);
            let resp: pb::ExecuteBatchResponse = unary(client, "ExecuteBatch", &req, true)
                .await
                .map_err(|e| with_context(e, "grafeo ExecuteBatch"))?;
            if let Some(err) = resp.error.as_option() {
                let at = chunk.get(err.index as usize).map_or("?", |e| e.id.as_str());
                return Err(with_context(
                    connect_error(&err.code, &err.message, true),
                    &format!("statement for event {at}"),
                ));
            }
            if sink.atomic && !resp.committed {
                return Err(ConnectorError::retryable("grafeo did not commit the batch"));
            }
        }
        Ok(SinkAck {
            rows: events.len() as u64,
            bytes: events.iter().map(|e| e.size_bytes() as u64).sum(),
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct GraphSinkFactory {
    cfg: Arc<GraphConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for GraphSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphSinkFactory")
    }
}

impl SinkFactory for GraphSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(GraphSink {
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
        })))
    }
}

// ---------------------------------------------------------------- source --

/// The source's position.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphPosition {
    /// The last row's cursor as `loams.graph.v1.Value` bytes, base64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub after: i64,
}

impl GraphPosition {
    fn cursor_value(&self) -> Result<pb::Value, ConnectorError> {
        match &self.cursor {
            None => Ok(to_value(&Json::Null)),
            Some(b64) => {
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|_| ConnectorError::position_lost("a bad Grafeo cursor"))?;
                pb::Value::decode_from_slice(&raw)
                    .map_err(|_| ConnectorError::position_lost("a bad Grafeo cursor"))
            }
        }
    }

    pub fn to_position(&self) -> Position {
        let shown = self
            .cursor_value()
            .map(|v| to_json(&v).to_string())
            .unwrap_or_default();
        let display = if self.cursor.is_some() {
            format!("cursor {shown} after {}", self.after)
        } else {
            "start".into()
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque).map_err(|_| {
            ConnectorError::position_lost(format!(
                "the saved position ({}) is not a Grafeo cursor",
                p.display
            ))
        })
    }
}

/// The `ExecuteStream` request for one page.
pub fn stream_request(
    cfg: &GraphConfig,
    src: &GraphSourceConfig,
    pos: &GraphPosition,
    limit: usize,
) -> Result<pb::ExecuteStreamRequest, ConnectorError> {
    let mut parameters = buffa::__private::HashMap::default();
    parameters.insert("cursor".to_owned(), pos.cursor_value()?);
    parameters.insert("after".to_owned(), v(Kind::Int64(pos.after)));
    parameters.insert("limit".to_owned(), v(Kind::Int64(limit as i64)));
    Ok(pb::ExecuteStreamRequest {
        request: MessageField::some(pb::ExecuteRequest {
            namespace: cfg.namespace.clone(),
            graph: cfg.graph.clone(),
            statement: source_statement(src),
            language: cfg.language.proto(),
            parameters,
            read_only: true,
            ..Default::default()
        }),
        chunk_rows: limit.min(u32::MAX as usize) as u32,
        ..Default::default()
    })
}

/// One split per label.
#[derive(Debug)]
pub struct GraphEnumerator {
    pub label: String,
}

#[async_trait]
impl SplitEnumerator for GraphEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.label.clone(),
            json!({ "label": self.label }),
        )])
    }
}

/// See the module documentation.
pub struct GraphSource {
    cfg: Arc<GraphConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    client: OnceCell<RestClient>,
    split: Option<Split>,
    pos: GraphPosition,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for GraphSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphSource")
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

impl GraphSource {
    fn end_pass(&mut self) {
        let split = self.split.as_ref().map(|s| s.id.clone());
        self.ctx
            .observe
            .lag(&self.ctx.instance, split.as_ref(), Duration::ZERO);
        match self.cfg.source.as_ref().and_then(|s| s.poll_interval_ms) {
            Some(ms) => self.wait_until = Some(Instant::now() + Duration::from_millis(ms)),
            None => self.finished = true,
        }
    }

    async fn page(&self, limit: usize) -> Result<(Vec<String>, Vec<pb::Row>), ConnectorError> {
        let src = self
            .cfg
            .source
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("no source config"))?;
        let client = self
            .client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await?;
        let req = stream_request(&self.cfg, src, &self.pos, limit)?;
        self.ctx.observe.api_call(&self.ctx.instance);
        let resp = client
            .send(
                Method::POST,
                &format!("{SERVICE}/ExecuteStream"),
                envelope(0, &req.encode_to_vec()),
                Some("application/connect+proto"),
                &[],
            )
            .await?;
        if !resp.status().is_success() {
            let body: Json = serde_json::from_slice(resp.body()).unwrap_or(Json::Null);
            return Err(connect_error(
                body["code"]
                    .as_str()
                    .unwrap_or(if resp.status().as_u16() == 401 {
                        "unauthenticated"
                    } else {
                        "unknown"
                    }),
                body["message"].as_str().unwrap_or(""),
                false,
            ));
        }
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        for msg in unframe(resp.body(), false)? {
            let chunk = pb::ResultChunk::decode_from_slice(&msg)
                .map_err(|e| ConnectorError::retryable(format!("decoding a ResultChunk: {e}")))?;
            if columns.is_empty() {
                columns = chunk.columns.clone();
            }
            rows.extend(chunk.rows);
        }
        Ok((columns, rows))
    }
}

#[async_trait]
impl Source for GraphSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.split = Some(split.clone());
        self.pos = match &pos {
            Some(p) => GraphPosition::from_position(p)?,
            None => GraphPosition::default(),
        };
        self.finished = false;
        self.wait_until = None;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let src = self
            .cfg
            .source
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no source config"))?;
        loop {
            if self.finished {
                return Ok(None);
            }
            if let Some(t) = self.wait_until.take() {
                tokio::time::sleep_until(t).await;
            }
            let limit = src.page_size.min(budget.max_rows).max(1);
            let (columns, rows) = self.page(limit).await?;
            let (Some(ci), Some(ei)) = (
                columns.iter().position(|c| c == "cursor"),
                columns.iter().position(|c| c == "eid"),
            ) else {
                if rows.is_empty() {
                    self.end_pass();
                    continue;
                }
                return Err(ConnectorError::fatal(
                    "the source statement must return `cursor` and `eid`",
                ));
            };
            let full_page = rows.len() >= limit;
            let mut events = Vec::with_capacity(rows.len());
            let mut bytes = 0;
            for row in &rows {
                let cursor = row.values.get(ci).cloned().unwrap_or_default();
                let eid = match row.values.get(ei).and_then(|v| v.kind.clone()) {
                    Some(Kind::Int64(i)) => i,
                    Some(Kind::Uint64(u)) => i64::try_from(u).unwrap_or(i64::MAX),
                    _ => return Err(ConnectorError::fatal("`eid` must be an integer node id")),
                };
                let mut data = Map::new();
                for (i, c) in columns.iter().enumerate() {
                    if i != ci && i != ei {
                        data.insert(
                            c.clone(),
                            row.values.get(i).map(to_json).unwrap_or(Json::Null),
                        );
                    }
                }
                let cursor_json = to_json(&cursor);
                data.insert("cursor".into(), cursor_json.clone());
                data.insert("id".into(), json!(eid));
                let ev = self
                    .builder
                    .event(
                        "rows",
                        &src.label,
                        format!("{}/{}/{eid}@{cursor_json}", self.cfg.graph, src.label),
                    )
                    .with_subject(eid.to_string())
                    .with_data(
                        "application/json",
                        serde_json::to_vec(&Json::Object(data)).unwrap_or_default(),
                    );
                let sz = ev.size_bytes();
                let next_pos = GraphPosition {
                    cursor: Some(
                        base64::engine::general_purpose::STANDARD.encode(cursor.encode_to_vec()),
                    ),
                    after: eid,
                };
                if bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        self.pos = next_pos;
                        return Err(ConnectorError::new(
                            ErrorClass::Poison,
                            format!("row {} ({sz} bytes) exceeds the batch budget", ev.id),
                        ));
                    }
                    break;
                }
                bytes += sz;
                self.pos = next_pos;
                events.push(ev);
            }
            if events.len() == rows.len() && !full_page {
                self.end_pass();
            }
            if events.is_empty() {
                continue;
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

struct GraphSourceFactory {
    cfg: Arc<GraphConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for GraphSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphSourceFactory")
    }
}

impl SourceFactory for GraphSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(GraphSource {
            cfg: self.cfg.clone(),
            builder: EnvelopeBuilder::new("grafeo", &self.ctx.instance),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
            split: None,
            pos: GraphPosition::default(),
            wait_until: None,
            finished: false,
        }))
    }
}

/// `runtime.ref: loams_connector_protocols::grafeo`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Grafeo;

impl ConnectorFactory for Grafeo {
    fn id(&self) -> &'static str {
        "grafeo"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Arc::new(GraphConfig::parse(&ctx.config)?);
        let Some(src) = &cfg.source else {
            return Ok(None);
        };
        Ok(Some((
            Box::new(GraphEnumerator {
                label: src.label.clone(),
            }),
            Box::new(GraphSourceFactory {
                cfg: cfg.clone(),
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Arc::new(GraphConfig::parse(&ctx.config)?);
        if cfg.sink.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(GraphSinkFactory {
            cfg,
            ctx: ctx.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_values_round_trip() {
        let j = json!({"a": 1, "b": [true, "x", 2.5], "c": null});
        assert_eq!(to_json(&to_value(&j)), j);
        let ts = v(Kind::ZonedDatetime(Box::new(pb::ZonedDateTime {
            local: MessageField::some(pb::LocalDateTime {
                date: MessageField::some(pb::Date {
                    year: 2026,
                    month: 10,
                    day: 13,
                    ..Default::default()
                }),
                time: MessageField::some(pb::LocalTime {
                    hour: 8,
                    minute: 5,
                    second: 0,
                    nanosecond: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            offset_seconds: 19_800,
            ..Default::default()
        })));
        assert_eq!(to_json(&ts), json!("2026-10-13T08:05:00+05:30"));
    }

    #[test]
    fn envelopes_and_end_stream() {
        let mut body = envelope(0, b"abc").to_vec();
        body.extend_from_slice(&envelope(2, b"{}"));
        assert_eq!(
            unframe(&body, false).expect("ok"),
            vec![Bytes::from_static(b"abc")]
        );
        let mut bad = envelope(0, b"abc").to_vec();
        bad.extend_from_slice(&envelope(
            2,
            br#"{"error":{"code":"unauthenticated","message":"no key"}}"#,
        ));
        assert_eq!(
            unframe(&bad, false).expect_err("err").class,
            ErrorClass::Auth
        );
        assert!(unframe(&envelope(0, b"x"), false).is_err(), "no end-stream");
    }
}
