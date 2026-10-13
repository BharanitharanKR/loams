//! Neo4j over its **HTTP Query API** (OR-14; no Bolt, D634 (c)):
//! `POST /db/{database}/query/v2`, one implicit transaction per request,
//! through the SDK's egress-checked client (ruling R21.5).
//!
//! * **Version.** The Query API arrived in Neo4j 5.19 (enabled by default on
//!   self-managed servers from 5.25). On first use the connector reads the
//!   discovery document (`GET /`) and refuses a server older than 5.19 as
//!   `Fatal`; a 404 from the query endpoint says the API is disabled.
//! * **Sink:** a parameterised statement run once per batch with the events'
//!   data as `$rows` (`UNWIND $rows AS row MERGE …`). `MERGE` on a key makes
//!   the write idempotent, so a retried batch converges
//!   (`neo4j_sink_merge_idempotent`). With `delete_statement`, events with
//!   `loamsop = d` run it instead, and a change between upserts and deletes
//!   starts a new request, so per-key order holds.
//! * **Source:** one split per label (or per custom statement), read in
//!   `(cursor property, elementId)` order with `$cursor`, `$after` and
//!   `$limit`. The position is the last row's cursor value and element id,
//!   sent back in Neo4j's **typed JSON** so a temporal cursor compares as
//!   itself. Event ids are `<label>/<elementId>@<cursor>`: a node re-read
//!   unchanged has the same id, a node whose cursor moved a new one. With
//!   `poll_interval_ms` the source keeps polling from its cursor; without,
//!   it ends after one pass.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, ErrorClass,
    EventBatch, Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::client::{
    AuthSpec, BasicAuth, RestClient, classify, json_bytes, parse_body, read_error, with_context,
};
use crate::template::data_json;

/// The typed-JSON media type of the Query API.
pub const TYPED_JSON: &str = "application/vnd.neo4j.query";

/// Auth by secret name.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct Neo4jAuth {
    #[serde(default)]
    pub basic: Option<BasicAuth>,
    /// A bearer token's secret name (SSO).
    #[serde(default)]
    pub bearer: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// Must use `$rows`.
    pub statement: String,
    #[serde(default)]
    pub delete_statement: Option<String>,
    #[serde(default = "default_batch")]
    pub batch: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// The label read; also the split id and the events' resource.
    pub label: String,
    /// The property the source orders and resumes by.
    pub cursor: String,
    /// A custom statement: it takes `$cursor`, `$after` and `$limit` and
    /// returns columns `cursor` and `eid`, ordered by them.
    #[serde(default)]
    pub statement: Option<String>,
    #[serde(default = "default_batch")]
    pub page_size: usize,
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
}

fn default_batch() -> usize {
    500
}

fn default_db() -> String {
    "neo4j".into()
}

fn default_timeout() -> u64 {
    30_000
}

/// The instance config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Neo4jConfig {
    /// The HTTP endpoint, e.g. `http://neo4j:7474`.
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default)]
    pub auth: Neo4jAuth,
    #[serde(default)]
    pub sink: Option<SinkConfig>,
    #[serde(default)]
    pub source: Option<SourceConfig>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn is_ident(s: &str) -> bool {
    !s.is_empty() && !s.contains('`')
}

impl Neo4jConfig {
    pub fn parse(v: &Value) -> Result<Self, ConnectorError> {
        let cfg: Neo4jConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("neo4j config: {e}")))?;
        if let Some(s) = &cfg.sink {
            if !s.statement.contains("$rows") {
                return Err(ConnectorError::fatal("sink.statement must use $rows"));
            }
            if s.delete_statement
                .as_ref()
                .is_some_and(|d| !d.contains("$rows"))
            {
                return Err(ConnectorError::fatal(
                    "sink.delete_statement must use $rows",
                ));
            }
        }
        if let Some(s) = &cfg.source {
            if !is_ident(&s.label) || !is_ident(&s.cursor) {
                return Err(ConnectorError::fatal(
                    "source.label and source.cursor must be non-empty and contain no backtick",
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

    fn auth(&self) -> AuthSpec {
        match (&self.auth.basic, &self.auth.bearer) {
            (Some(b), _) => AuthSpec::Basic(b.clone()),
            (None, Some(t)) => AuthSpec::Bearer(t.clone()),
            (None, None) => AuthSpec::None,
        }
    }

    pub fn query_path(&self) -> String {
        format!("/db/{}/query/v2", self.database)
    }
}

/// The source statement for `cfg`.
pub fn source_statement(src: &SourceConfig) -> String {
    if let Some(s) = &src.statement {
        return s.clone();
    }
    let (l, c) = (&src.label, &src.cursor);
    format!(
        "MATCH (n:`{l}`) WHERE n.`{c}` IS NOT NULL AND ($cursor IS NULL OR n.`{c}` > $cursor \
         OR (n.`{c}` = $cursor AND elementId(n) > $after)) \
         RETURN n, n.`{c}` AS cursor, elementId(n) AS eid ORDER BY cursor, eid LIMIT $limit"
    )
}

/// `major.minor` of a version string.
fn version(v: &str) -> Option<(u32, u32)> {
    let mut it = v.split('.');
    Some((
        it.next()?.parse().ok()?,
        it.next()?
            .trim_end_matches(|c: char| !c.is_ascii_digit())
            .parse()
            .ok()?,
    ))
}

/// A connected client, version-checked once.
async fn connect(cfg: &Neo4jConfig, ctx: &ConnectorContext) -> Result<RestClient, ConnectorError> {
    let c = RestClient::connect(
        &cfg.url,
        &cfg.auth(),
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_millis(cfg.request_timeout_ms),
        "",
    )
    .await?;
    let disco = c
        .send(
            Method::GET,
            "/",
            Bytes::new(),
            None,
            &[("accept", "application/json".into())],
        )
        .await?;
    if let Some(e) = classify(&disco) {
        return Err(with_context(
            read_error(e),
            "reading Neo4j's discovery document",
        ));
    }
    let doc = parse_body(&disco)?;
    if let Some(v) = doc["neo4j_version"].as_str() {
        match version(v) {
            Some(ver) if ver >= (5, 19) => {}
            _ => {
                return Err(ConnectorError::fatal(format!(
                    "Neo4j {v} has no HTTP Query API; it needs 5.19 or later"
                )));
            }
        }
    }
    Ok(c)
}

/// A Neo4j error: `Neo.TransientError.*` is `Retryable`; a client error on
/// a write is `Poison` (the batch is bad), on a read `Fatal`; 404 says the
/// database or the Query API is missing (`Fatal`).
fn neo4j_error(resp: &http::Response<Bytes>, what: &str, write: bool) -> Option<ConnectorError> {
    let body: Value = serde_json::from_slice(resp.body()).unwrap_or(Value::Null);
    let code = body["errors"][0]["code"].as_str().unwrap_or("");
    // The Query API may answer 202 with `errors` (its docs: every request
    // but an authentication failure is 202), or 4xx with the same body.
    let mut err = match classify(resp) {
        Some(e) => e,
        None if !code.is_empty() => {
            let detail = crate::client::error_detail(resp.body()).unwrap_or_default();
            ConnectorError::poison(format!("HTTP {}: {detail}", resp.status()))
        }
        None => return None,
    };
    if code.starts_with("Neo.ClientError.Security") {
        err.class = ErrorClass::Auth;
    } else if code.starts_with("Neo.TransientError") {
        err.class = ErrorClass::Retryable { after: None };
    } else if resp.status().as_u16() == 404 {
        err.class = ErrorClass::Fatal;
        err.message = format!(
            "{}: no such database, or the HTTP Query API is disabled on this server",
            err.message
        );
    } else if !write {
        err = read_error(err);
    }
    Some(with_context(err, what))
}

// ------------------------------------------------------------------ sink --

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Upsert,
    Delete,
}

/// The Neo4j sink.
pub struct Neo4jSink {
    cfg: Arc<Neo4jConfig>,
    ctx: ConnectorContext,
    client: OnceCell<RestClient>,
}

impl std::fmt::Debug for Neo4jSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jSink")
            .field("database", &self.cfg.database)
            .finish_non_exhaustive()
    }
}

/// The body of one write request.
pub fn write_body(statement: &str, rows: &[Value]) -> Value {
    json!({ "statement": statement, "parameters": { "rows": rows } })
}

impl Neo4jSink {
    async fn run(&self, op: Op, rows: &mut Vec<Value>) -> Result<(), ConnectorError> {
        if rows.is_empty() {
            return Ok(());
        }
        let sink = self
            .cfg
            .sink
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("no sink config"))?;
        let statement = match op {
            Op::Upsert => sink.statement.as_str(),
            Op::Delete => sink.delete_statement.as_deref().unwrap_or_default(),
        };
        let client = self
            .client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        let resp = client
            .send(
                Method::POST,
                &self.cfg.query_path(),
                json_bytes(&write_body(statement, rows))?,
                Some("application/json"),
                &[("accept", "application/json".into())],
            )
            .await?;
        if let Some(e) = neo4j_error(&resp, "neo4j write", true) {
            return Err(e);
        }
        rows.clear();
        Ok(())
    }
}

#[async_trait]
impl Sink for Neo4jSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison("the Neo4j sink takes JSON events"));
        };
        let sink = self
            .cfg
            .sink
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no sink config"))?;
        let mut ack = SinkAck::default();
        let mut run: Vec<Value> = Vec::new();
        let mut run_op = Op::Upsert;
        for ev in &events {
            let op = if sink.delete_statement.is_some()
                && ev.extensions.get("loamsop").map(String::as_str) == Some("d")
            {
                Op::Delete
            } else {
                Op::Upsert
            };
            let row = match data_json(ev)? {
                v @ Value::Object(_) => v,
                _ => {
                    return Err(ConnectorError::poison(format!(
                        "event {}: a row must be a JSON object",
                        ev.id
                    )));
                }
            };
            if (op != run_op && !run.is_empty()) || run.len() >= sink.batch.max(1) {
                self.run(run_op, &mut run).await?;
            }
            run_op = op;
            run.push(row);
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        self.run(run_op, &mut run).await?;
        Ok(ack)
    }

    /// Every request commits before `write` returns.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct Neo4jSinkFactory {
    cfg: Arc<Neo4jConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for Neo4jSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Neo4jSinkFactory")
    }
}

impl SinkFactory for Neo4jSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(Neo4jSink {
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
        })))
    }
}

// ---------------------------------------------------------------- source --

/// Neo4j's typed JSON (`{"$type": …, "_value": …}`) as plain JSON: integers
/// as numbers, temporal values, durations, points and bytes as their text,
/// nodes as `{elementId, labels, properties}`, relationships as
/// `{elementId, type, startNodeElementId, endNodeElementId, properties}`.
pub fn plain(v: &Value) -> Value {
    let Some(ty) = v.get("$type").and_then(Value::as_str) else {
        return v.clone();
    };
    let val = &v["_value"];
    match ty {
        "Null" => Value::Null,
        "Integer" => val
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map_or_else(|| val.clone(), Value::from),
        "Float" => val
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
            .unwrap_or_else(|| val.clone()),
        "List" => Value::Array(
            val.as_array()
                .map(|a| a.iter().map(plain).collect())
                .unwrap_or_default(),
        ),
        "Map" => Value::Object(
            val.as_object()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), plain(v))).collect())
                .unwrap_or_default(),
        ),
        "Node" => json!({
            "elementId": val["_element_id"],
            "labels": val["_labels"],
            "properties": plain_map(&val["_properties"]),
        }),
        "Relationship" => json!({
            "elementId": val["_element_id"],
            "type": val["_type"],
            "startNodeElementId": val["_start_node_element_id"],
            "endNodeElementId": val["_end_node_element_id"],
            "properties": plain_map(&val["_properties"]),
        }),
        "Path" => Value::Array(
            val.as_array()
                .map(|a| a.iter().map(plain).collect())
                .unwrap_or_default(),
        ),
        _ => val.clone(),
    }
}

fn plain_map(v: &Value) -> Value {
    Value::Object(
        v.as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), plain(v))).collect())
            .unwrap_or_default(),
    )
}

/// The source's position (JSON in `Position::opaque`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CursorPosition {
    /// The last row's cursor, in typed JSON; `None` before the first row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Value>,
    /// The last row's element id.
    #[serde(default)]
    pub after: String,
}

impl CursorPosition {
    pub fn to_position(&self) -> Position {
        let display = match &self.cursor {
            Some(c) => format!("cursor {} after {}", plain(c), self.after),
            None => "start".into(),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque).map_err(|_| {
            ConnectorError::position_lost(format!(
                "the saved position ({}) is not a Neo4j cursor",
                p.display
            ))
        })
    }
}

/// The body of one read request (typed JSON parameters).
pub fn read_body(src: &SourceConfig, pos: &CursorPosition, limit: usize) -> Value {
    json!({
        "statement": source_statement(src),
        "parameters": {
            "cursor": pos.cursor.clone().unwrap_or(json!({"$type": "Null", "_value": null})),
            "after": {"$type": "String", "_value": pos.after},
            "limit": {"$type": "Integer", "_value": limit.to_string()},
        }
    })
}

/// One split per label.
#[derive(Debug)]
pub struct LabelEnumerator {
    pub label: String,
}

#[async_trait]
impl SplitEnumerator for LabelEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.label.clone(),
            json!({ "label": self.label }),
        )])
    }
}

/// See the module documentation.
pub struct Neo4jSource {
    cfg: Arc<Neo4jConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    client: OnceCell<RestClient>,
    split: Option<Split>,
    pos: CursorPosition,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for Neo4jSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jSource")
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

impl Neo4jSource {
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
}

#[async_trait]
impl Source for Neo4jSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.split = Some(split.clone());
        self.pos = match &pos {
            Some(p) => CursorPosition::from_position(p)?,
            None => CursorPosition::default(),
        };
        self.finished = false;
        self.wait_until = None;
        self.client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await?;
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
            let client = self
                .client
                .get_or_try_init(|| connect(&self.cfg, &self.ctx))
                .await?;
            self.ctx.observe.api_call(&self.ctx.instance);
            let resp = client
                .send(
                    Method::POST,
                    &self.cfg.query_path(),
                    json_bytes(&read_body(&src, &self.pos, limit))?,
                    Some(TYPED_JSON),
                    &[("accept", TYPED_JSON.into())],
                )
                .await?;
            if let Some(e) = neo4j_error(&resp, "neo4j read", false) {
                return Err(e);
            }
            let answer = parse_body(&resp)?;
            let fields: Vec<String> = answer["data"]["fields"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|f| f.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let (Some(ci), Some(ei)) = (
                fields.iter().position(|f| f == "cursor"),
                fields.iter().position(|f| f == "eid"),
            ) else {
                return Err(ConnectorError::fatal(
                    "the source statement must return columns `cursor` and `eid`",
                ));
            };
            let values = answer["data"]["values"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let full_page = values.len() >= limit;
            let mut events = Vec::with_capacity(values.len());
            let mut bytes = 0;
            for row in &values {
                let cursor = row[ci].clone();
                let eid = plain(&row[ei]).as_str().unwrap_or_default().to_owned();
                let mut data = Map::new();
                for (i, f) in fields.iter().enumerate() {
                    if i != ci && i != ei {
                        data.insert(f.clone(), plain(&row[i]));
                    }
                }
                data.insert("cursor".into(), plain(&cursor));
                data.insert("elementId".into(), Value::String(eid.clone()));
                let body = serde_json::to_vec(&Value::Object(data)).unwrap_or_default();
                let ev = self
                    .builder
                    .event(
                        "rows",
                        &src.label,
                        format!("{}/{eid}@{}", src.label, plain(&cursor)),
                    )
                    .with_subject(eid.clone())
                    .with_data("application/json", body);
                let sz = ev.size_bytes();
                if bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        // R4.7: a row larger than the batch budget is skipped.
                        self.pos = CursorPosition {
                            cursor: Some(cursor),
                            after: eid,
                        };
                        return Err(ConnectorError::poison(format!(
                            "row {} ({sz} bytes) exceeds the batch budget",
                            ev.id
                        )));
                    }
                    break;
                }
                bytes += sz;
                self.pos = CursorPosition {
                    cursor: Some(cursor),
                    after: eid,
                };
                events.push(ev);
            }
            if events.len() == values.len() && !full_page {
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

struct Neo4jSourceFactory {
    cfg: Arc<Neo4jConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for Neo4jSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Neo4jSourceFactory")
    }
}

impl SourceFactory for Neo4jSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(Neo4jSource {
            cfg: self.cfg.clone(),
            builder: EnvelopeBuilder::new("neo4j", &self.ctx.instance),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
            split: None,
            pos: CursorPosition::default(),
            wait_until: None,
            finished: false,
        }))
    }
}

/// `runtime.ref: loams_connector_protocols::neo4j`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Neo4j;

impl ConnectorFactory for Neo4j {
    fn id(&self) -> &'static str {
        "neo4j"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Arc::new(Neo4jConfig::parse(&ctx.config)?);
        let Some(src) = &cfg.source else {
            return Ok(None);
        };
        Ok(Some((
            Box::new(LabelEnumerator {
                label: src.label.clone(),
            }),
            Box::new(Neo4jSourceFactory {
                cfg: cfg.clone(),
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Arc::new(Neo4jConfig::parse(&ctx.config)?);
        if cfg.sink.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(Neo4jSinkFactory {
            cfg,
            ctx: ctx.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_json_to_plain() {
        let node = json!({"$type": "Node", "_value": {"_element_id": "4:a:1", "_labels": ["Person"],
            "_properties": {"age": {"$type": "Integer", "_value": "42"},
                            "seen": {"$type": "OffsetDateTime", "_value": "2026-10-01T00:00:00Z"}}}});
        assert_eq!(
            plain(&node),
            json!({"elementId": "4:a:1", "labels": ["Person"],
                   "properties": {"age": 42, "seen": "2026-10-01T00:00:00Z"}})
        );
        assert_eq!(
            plain(&json!({"$type": "List", "_value": [{"$type": "Float", "_value": "1.5"}]})),
            json!([1.5])
        );
        assert_eq!(plain(&json!(3)), json!(3));
    }

    #[test]
    fn versions_and_config() {
        assert_eq!(version("5.26.0"), Some((5, 26)));
        assert_eq!(version("2025.08.0"), Some((2025, 8)));
        assert!(version("5.18.1").expect("v") < (5, 19));
        let bad = json!({"url": "http://n:7474", "sink": {"statement": "MERGE (n {id: 1})"}});
        assert!(Neo4jConfig::parse(&bad).is_err());
        let tick = json!({"url": "http://n:7474", "source": {"label": "P`x", "cursor": "u"}});
        assert!(Neo4jConfig::parse(&tick).is_err());
        let ok =
            json!({"url": "http://n:7474", "source": {"label": "Person", "cursor": "updatedAt"}});
        let cfg = Neo4jConfig::parse(&ok).expect("ok");
        let st = source_statement(cfg.source.as_ref().expect("src"));
        assert!(st.starts_with("MATCH (n:`Person`) WHERE n.`updatedAt` IS NOT NULL"));
    }

    #[test]
    fn positions_round_trip() {
        let p = CursorPosition {
            cursor: Some(json!({"$type": "Integer", "_value": "7"})),
            after: "4:a:9".into(),
        };
        let pos = p.to_position();
        assert_eq!(pos.display, "cursor 7 after 4:a:9");
        assert_eq!(CursorPosition::from_position(&pos).expect("decode"), p);
    }
}
