//! GraphQL over HTTP (`POST {query, variables}` through the SDK's
//! egress-checked client) and `graphql-transport-ws` subscriptions.
//!
//! * **Query source** (batch, resumable): one split, a query over a Relay
//!   connection (`connection` points at it in the answer, e.g.
//!   `/data/repository/issues`), paged with the `first`/`after` variables
//!   and `pageInfo { hasNextPage endCursor }`. The position is the cursor
//!   of the last item taken (the edge's `cursor` when the query selects
//!   edges, else the page's start), so a restart resumes where it stopped
//!   (`graphql_cursor_pagination_resumes`). Event ids are a digest of the
//!   item, so an unchanged item re-read is deduplicated by `ingest`. With
//!   `poll_interval_ms` the listing restarts from the first page that long
//!   after it ended; without, it is read once.
//! * **Subscription source** (streaming, `resumable: false`):
//!   `graphql-transport-ws` (`connection_init` → `connection_ack` →
//!   `subscribe` → `next`…) over [`crate::ws`]. Each `next` payload's data
//!   is one event. A restart subscribes afresh.
//! * **Mutation sink:** one mutation per event, its variables the event's
//!   data (or the object at `variables_pointer`). A GraphQL `errors` answer
//!   is `Poison` for that event, unless it says it was rate limited
//!   (`Throttled`).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

use crate::client::{AuthSpec, RestClient, hex, read_error, resolve, shorten, with_context};
use crate::template::{data_json, pointer};
use crate::ws::{WsClient, WsMessage};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct GraphqlAuth {
    /// A bearer token's secret name.
    #[serde(default)]
    pub bearer: Option<String>,
    /// A header carrying a key: `{name, secret, prefix?}`.
    #[serde(default)]
    pub header: Option<HeaderAuth>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderAuth {
    pub name: String,
    pub secret: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    pub query: String,
    #[serde(default)]
    pub variables: Map<String, Value>,
    /// The connection in the answer, a JSON pointer (`/data/viewer/repositories`).
    pub connection: String,
    #[serde(default = "default_page")]
    pub page_size: usize,
    #[serde(default = "first")]
    pub first_variable: String,
    #[serde(default = "after")]
    pub after_variable: String,
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MutationConfig {
    pub mutation: String,
    #[serde(default)]
    pub variables_pointer: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionConfig {
    pub query: String,
    #[serde(default)]
    pub variables: Map<String, Value>,
    /// `ws://` or `wss://`; default the HTTP URL with its scheme swapped.
    #[serde(default)]
    pub url: Option<String>,
}

fn default_page() -> usize {
    50
}

fn first() -> String {
    "first".into()
}

fn after() -> String {
    "after".into()
}

fn default_timeout() -> u64 {
    30_000
}

/// The instance config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphqlConfig {
    /// The GraphQL endpoint.
    pub url: String,
    #[serde(default)]
    pub auth: GraphqlAuth,
    #[serde(default)]
    pub query: Option<QueryConfig>,
    #[serde(default)]
    pub subscription: Option<SubscriptionConfig>,
    #[serde(default)]
    pub mutation: Option<MutationConfig>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

impl GraphqlConfig {
    pub fn parse(v: &Value) -> Result<Self, ConnectorError> {
        let cfg: GraphqlConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("graphql config: {e}")))?;
        if cfg.query.is_some() && cfg.subscription.is_some() {
            return Err(ConnectorError::fatal(
                "a source is a `query` or a `subscription`, not both",
            ));
        }
        Ok(cfg)
    }

    fn auth(&self) -> AuthSpec {
        match (&self.auth.bearer, &self.auth.header) {
            (Some(b), _) => AuthSpec::Bearer(b.clone()),
            (None, Some(h)) => AuthSpec::Header {
                header: h.name.clone(),
                prefix: h.prefix.clone(),
                secret: h.secret.clone(),
            },
            _ => AuthSpec::None,
        }
    }

    fn ws_url(&self) -> String {
        if let Some(u) = self.subscription.as_ref().and_then(|s| s.url.clone()) {
            return u;
        }
        if let Some(rest) = self.url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else {
            format!("ws://{}", self.url.trim_start_matches("http://"))
        }
    }
}

async fn connect(
    cfg: &GraphqlConfig,
    ctx: &ConnectorContext,
) -> Result<RestClient, ConnectorError> {
    RestClient::connect(
        &cfg.url,
        &cfg.auth(),
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_millis(cfg.request_timeout_ms),
        "",
    )
    .await
}

/// The error a GraphQL `errors` array stands for (`None` without one).
pub fn graphql_errors(answer: &Value, write: bool) -> Option<ConnectorError> {
    let errors = answer.get("errors")?.as_array().filter(|a| !a.is_empty())?;
    let text: Vec<String> = errors
        .iter()
        .map(|e| e["message"].as_str().unwrap_or("error").to_owned())
        .collect();
    let msg = format!("graphql: {}", shorten(&text.join("; "), 300));
    let limited = errors.iter().any(|e| {
        let ty = e["type"]
            .as_str()
            .or(e["extensions"]["code"].as_str())
            .unwrap_or("");
        ty.to_ascii_uppercase().contains("RATE_LIMIT") || ty.eq_ignore_ascii_case("THROTTLED")
    });
    Some(if limited {
        ConnectorError::throttled(Duration::from_secs(60), msg)
    } else if write {
        ConnectorError::poison(msg)
    } else {
        ConnectorError::fatal(msg)
    })
}

async fn post(client: &RestClient, body: &Value, write: bool) -> Result<Value, ConnectorError> {
    let v = client
        .json(Method::POST, "", Some(body))
        .await
        .map_err(|e| if write { e } else { read_error(e) })?;
    if let Some(e) = graphql_errors(&v, write) {
        // A partial answer (data with errors) on a read is still an error:
        // skipping part of a page would lose items silently.
        return Err(e);
    }
    Ok(v)
}

// ---------------------------------------------------------- query source --

/// The query source's position.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PagePosition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default)]
    pub pass_done: bool,
}

impl PagePosition {
    pub fn to_position(&self) -> Position {
        let display = match (&self.after, self.pass_done) {
            (_, true) => "pass done".to_owned(),
            (Some(a), false) => format!("after {a}"),
            (None, false) => "start".to_owned(),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }
}

/// The request body of one page.
pub fn page_request(q: &QueryConfig, after: Option<&str>, first: usize) -> Value {
    let mut vars = q.variables.clone();
    vars.insert(q.first_variable.clone(), json!(first));
    vars.insert(
        q.after_variable.clone(),
        after.map_or(Value::Null, |a| json!(a)),
    );
    json!({"query": q.query, "variables": vars})
}

/// The item event.
pub fn item_event(builder: &EnvelopeBuilder, resource: &str, item: &Value) -> CloudEvent {
    let bytes = serde_json::to_vec(item).unwrap_or_default();
    let mut d = Sha256::new();
    d.update(resource.as_bytes());
    d.update([0]);
    d.update(&bytes);
    let mut ev = builder
        .event("item", resource, hex(&d.finalize()[..16]))
        .with_data("application/json", bytes);
    if let Some(id) = item.get("id").and_then(Value::as_str) {
        ev = ev.with_subject(id.to_owned());
    }
    ev
}

/// One split: the query.
#[derive(Debug)]
pub struct QueryEnumerator;

#[async_trait]
impl SplitEnumerator for QueryEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new("query", json!({}))])
    }
}

/// See the module documentation.
pub struct QuerySource {
    cfg: Arc<GraphqlConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    client: OnceCell<RestClient>,
    pos: PagePosition,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for QuerySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuerySource")
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

impl QuerySource {
    fn end_pass(&mut self, q: &QueryConfig) {
        self.pos = PagePosition {
            after: None,
            pass_done: true,
        };
        self.ctx
            .observe
            .lag(&self.ctx.instance, None, Duration::ZERO);
        match q.poll_interval_ms {
            Some(ms) => self.wait_until = Some(Instant::now() + Duration::from_millis(ms)),
            None => self.finished = true,
        }
    }
}

#[async_trait]
impl Source for QuerySource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.pos = match &pos {
            Some(p) => serde_json::from_slice(&p.opaque).map_err(|_| {
                ConnectorError::position_lost(format!(
                    "the saved position ({}) is not a page cursor",
                    p.display
                ))
            })?,
            None => PagePosition::default(),
        };
        self.finished = false;
        self.wait_until = None;
        if self.pos.pass_done {
            let q = self
                .cfg
                .query
                .clone()
                .ok_or_else(|| ConnectorError::fatal("no query"))?;
            match q.poll_interval_ms {
                Some(ms) => self.wait_until = Some(Instant::now() + Duration::from_millis(ms)),
                None => self.finished = true,
            }
        }
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let q = self
            .cfg
            .query
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no query"))?;
        let resource = q.connection.trim_start_matches('/').replace('/', ".");
        loop {
            if self.finished {
                return Ok(None);
            }
            if let Some(t) = self.wait_until.take() {
                tokio::time::sleep_until(t).await;
            }
            if self.pos.pass_done {
                self.pos = PagePosition::default();
            }
            let first = q.page_size.min(budget.max_rows).max(1);
            let client = self
                .client
                .get_or_try_init(|| connect(&self.cfg, &self.ctx))
                .await?;
            self.ctx.observe.api_call(&self.ctx.instance);
            let answer = post(
                client,
                &page_request(&q, self.pos.after.as_deref(), first),
                false,
            )
            .await
            .map_err(|e| with_context(e, "graphql query"))?;
            let conn = answer.pointer(&pointer(&q.connection)).ok_or_else(|| {
                ConnectorError::fatal(format!("the answer has no connection at {}", q.connection))
            })?;
            // (item, the edge's cursor when selected)
            let items: Vec<(Value, Option<String>)> = if let Some(edges) = conn["edges"].as_array()
            {
                edges
                    .iter()
                    .map(|e| (e["node"].clone(), e["cursor"].as_str().map(str::to_owned)))
                    .collect()
            } else {
                conn["nodes"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|n| (n, None))
                    .collect()
            };
            let has_next = conn["pageInfo"]["hasNextPage"].as_bool().unwrap_or(false);
            let end_cursor = conn["pageInfo"]["endCursor"].as_str().map(str::to_owned);
            let mut events = Vec::new();
            let mut bytes = 0;
            let mut cut = false;
            for (item, cursor) in &items {
                let ev = item_event(&self.builder, &resource, item);
                let sz = ev.size_bytes();
                if events.len() >= budget.max_rows || bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        return Err(ConnectorError::new(
                            ErrorClass::Poison,
                            format!("item {} ({sz} bytes) exceeds the batch budget", ev.id),
                        ));
                    }
                    cut = true;
                    break;
                }
                bytes += sz;
                events.push(ev);
                if let Some(c) = cursor {
                    self.pos.after = Some(c.clone());
                }
            }
            if !cut {
                if has_next && end_cursor.is_some() {
                    self.pos.after = end_cursor;
                } else {
                    self.end_pass(&q);
                }
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

// --------------------------------------------------- subscription source --

/// See the module documentation.
pub struct SubscriptionSource {
    cfg: Arc<GraphqlConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    ws: Option<WsClient>,
    held: Option<CloudEvent>,
    started: u128,
    seq: u64,
    finished: bool,
}

impl std::fmt::Debug for SubscriptionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionSource").finish_non_exhaustive()
    }
}

impl SubscriptionSource {
    async fn subscribe(&mut self) -> Result<(), ConnectorError> {
        let sub = self
            .cfg
            .subscription
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no subscription"))?;
        let mut headers = Vec::new();
        let mut init = Map::new();
        if let Some(b) = &self.cfg.auth.bearer {
            let t = resolve(&*self.ctx.secrets, b).await?;
            let v = format!("Bearer {}", t.expose_str().unwrap_or_default());
            headers.push(("Authorization".to_owned(), v.clone()));
            init.insert("Authorization".into(), json!(v));
        } else if let Some(h) = &self.cfg.auth.header {
            let t = resolve(&*self.ctx.secrets, &h.secret).await?;
            let v = format!("{}{}", h.prefix, t.expose_str().unwrap_or_default());
            headers.push((h.name.clone(), v.clone()));
            init.insert(h.name.clone(), json!(v));
        }
        let mut ws = WsClient::connect(
            &self.cfg.ws_url(),
            "graphql-transport-ws",
            &headers,
            self.ctx.egress.clone(),
        )
        .await?;
        ws.send_text(&json!({"type": "connection_init", "payload": init}).to_string())
            .await?;
        loop {
            match ws.recv().await? {
                Some(WsMessage::Text(t)) => {
                    let m: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
                    match m["type"].as_str() {
                        Some("connection_ack") => break,
                        Some("ping") => ws.send_text(r#"{"type":"pong"}"#).await?,
                        _ => {}
                    }
                }
                Some(WsMessage::Binary(_)) => {}
                None => {
                    return Err(ConnectorError::auth(
                        "the server closed before connection_ack",
                    ));
                }
            }
        }
        ws.send_text(
            &json!({"id": "1", "type": "subscribe",
                    "payload": {"query": sub.query, "variables": sub.variables}})
            .to_string(),
        )
        .await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        self.ws = Some(ws);
        self.started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        self.seq = 0;
        Ok(())
    }
}

#[async_trait]
impl Source for SubscriptionSource {
    async fn open(&mut self, _split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        self.ws = None;
        self.finished = false;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.finished {
            return Ok(None);
        }
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut deadline: Option<Instant> = None;
        if let Some(ev) = self.held.take() {
            bytes += ev.size_bytes();
            events.push(ev);
            deadline = Some(Instant::now());
        }
        if self.ws.is_none() && events.is_empty() {
            self.subscribe().await?;
        }
        while events.len() < budget.max_rows.max(1) {
            let Some(ws) = self.ws.as_mut() else { break };
            let msg = match deadline {
                Some(d) => match tokio::time::timeout_at(d, ws.recv()).await {
                    Ok(m) => m,
                    Err(_) => break,
                },
                None => ws.recv().await,
            };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    self.ws = None;
                    return Err(e);
                }
            };
            let Some(WsMessage::Text(t)) = msg else {
                if msg.is_none() {
                    self.ws = None;
                    if events.is_empty() {
                        return Err(ConnectorError::retryable(
                            "the subscription's socket closed",
                        ));
                    }
                    break;
                }
                continue;
            };
            let m: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
            match m["type"].as_str() {
                Some("next") => {
                    self.seq += 1;
                    let data = &m["payload"]["data"];
                    let body = serde_json::to_vec(data).unwrap_or_default();
                    let id = hex(&Sha256::digest(
                        format!("{}/{}", self.started, self.seq).as_bytes(),
                    )[..16]);
                    let ev = self
                        .builder
                        .event("item", "subscription", id)
                        .with_data("application/json", body);
                    let sz = ev.size_bytes();
                    if bytes + sz > budget.max_bytes {
                        if events.is_empty() {
                            return Err(ConnectorError::poison(format!(
                                "message {} ({sz} bytes) exceeds the batch budget",
                                ev.id
                            )));
                        }
                        // Sent first by the next call.
                        self.held = Some(ev);
                        break;
                    }
                    bytes += sz;
                    events.push(ev);
                    if deadline.is_none() {
                        deadline = Some(Instant::now() + Duration::from_millis(200));
                    }
                }
                Some("ping") => {
                    if let Some(ws) = self.ws.as_mut() {
                        ws.send_text(r#"{"type":"pong"}"#).await?;
                    }
                }
                Some("error") => {
                    self.ws = None;
                    return Err(ConnectorError::fatal(format!(
                        "subscription refused: {}",
                        shorten(&m["payload"].to_string(), 300)
                    )));
                }
                Some("complete") => {
                    self.ws = None;
                    self.finished = true;
                    break;
                }
                _ => {}
            }
        }
        if events.is_empty() {
            return Ok(None);
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(Bytes::new(), "live"),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(ws) = self.ws.as_mut() {
            ws.close().await;
        }
        self.ws = None;
        Ok(())
    }
}

// ------------------------------------------------------------------ sink --

/// One mutation per event.
pub struct MutationSink {
    cfg: Arc<GraphqlConfig>,
    ctx: ConnectorContext,
    client: OnceCell<RestClient>,
}

impl std::fmt::Debug for MutationSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MutationSink")
    }
}

/// The request body of one event's mutation.
pub fn mutation_request(m: &MutationConfig, ev: &CloudEvent) -> Result<Value, ConnectorError> {
    let data = data_json(ev)?;
    let vars = match &m.variables_pointer {
        Some(p) => data.pointer(&pointer(p)).cloned().unwrap_or(Value::Null),
        None => data,
    };
    if !vars.is_object() {
        return Err(ConnectorError::poison(format!(
            "event {}: variables must be a JSON object",
            ev.id
        )));
    }
    Ok(json!({"query": m.mutation, "variables": vars}))
}

#[async_trait]
impl Sink for MutationSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison("the GraphQL sink takes JSON events"));
        };
        let m = self
            .cfg
            .mutation
            .clone()
            .ok_or_else(|| ConnectorError::fatal("no mutation"))?;
        let client = self
            .client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await?;
        let mut ack = SinkAck::default();
        for ev in &events {
            self.ctx.observe.api_call(&self.ctx.instance);
            post(client, &mutation_request(&m, ev)?, true)
                .await
                .map_err(|e| with_context(e, &format!("mutation for event {}", ev.id)))?;
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

// --------------------------------------------------------------- factory --

struct GqlSourceFactory {
    cfg: Arc<GraphqlConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for GqlSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GqlSourceFactory")
    }
}

impl SourceFactory for GqlSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let builder = EnvelopeBuilder::new("graphql", &self.ctx.instance);
        if self.cfg.subscription.is_some() {
            return Ok(Box::new(SubscriptionSource {
                cfg: self.cfg.clone(),
                ctx: self.ctx.clone(),
                builder,
                ws: None,
                held: None,
                started: 0,
                seq: 0,
                finished: false,
            }));
        }
        Ok(Box::new(QuerySource {
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            builder,
            client: OnceCell::new(),
            pos: PagePosition::default(),
            wait_until: None,
            finished: false,
        }))
    }
}

struct MutationSinkFactory {
    cfg: Arc<GraphqlConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for MutationSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MutationSinkFactory")
    }
}

impl SinkFactory for MutationSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(MutationSink {
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
        })))
    }
}

/// `runtime.ref: loams_connector_protocols::graphql`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Graphql;

impl ConnectorFactory for Graphql {
    fn id(&self) -> &'static str {
        "graphql"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Arc::new(GraphqlConfig::parse(&ctx.config)?);
        if cfg.query.is_none() && cfg.subscription.is_none() {
            return Ok(None);
        }
        Ok(Some((
            Box::new(QueryEnumerator),
            Box::new(GqlSourceFactory {
                cfg,
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Arc::new(GraphqlConfig::parse(&ctx.config)?);
        if cfg.mutation.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(MutationSinkFactory {
            cfg,
            ctx: ctx.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_classify() {
        assert!(graphql_errors(&json!({"data": {}}), true).is_none());
        let limited =
            json!({"errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]});
        assert!(matches!(
            graphql_errors(&limited, false).expect("e").class,
            ErrorClass::Throttled { .. }
        ));
        let bad = json!({"errors": [{"message": "Field 'x' doesn't exist"}]});
        assert_eq!(
            graphql_errors(&bad, true).expect("e").class,
            ErrorClass::Poison
        );
        assert_eq!(
            graphql_errors(&bad, false).expect("e").class,
            ErrorClass::Fatal
        );
    }

    #[test]
    fn ws_url_follows_the_http_url() {
        let c = GraphqlConfig::parse(&json!({"url": "https://api.example.test/graphql",
            "subscription": {"query": "subscription { x }"}}))
        .expect("cfg");
        assert_eq!(c.ws_url(), "wss://api.example.test/graphql");
    }
}
