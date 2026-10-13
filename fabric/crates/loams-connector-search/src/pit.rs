//! The point-in-time source (Elasticsearch and OpenSearch).
//!
//! One split per index. A **pass** opens a PIT on the index and pages
//! through it with `search_after`, sorted by the cursor field (when the
//! instance names one) and then a tiebreaker (`_shard_doc` on Elasticsearch,
//! `_id` on OpenSearch; R15.4). The position is the PIT id and the last sort
//! values, plus the pass's lower bound and the greatest cursor value read:
//!
//! * resumed while the PIT lives, the source continues inside it with
//!   `search_after` — no gap, no repeat;
//! * resumed after it expired (404), a new pass starts at `cursor >= last
//!   value read`, so ties at that value are read again; their event ids
//!   (`<index>/<_id>@<seq_no>.<primary_term>`) are stable, and `ingest`
//!   drops the repeats (effectively once, §33 §6). Without a cursor field
//!   the new pass reads the whole index again, deduplicated the same way.
//!
//! Without `poll_interval_ms` the source reads one pass and ends (a batch
//! source); with it, a pass that reached the end starts again after the
//! interval from the last cursor value (incremental polling). A PIT is
//! deleted when its pass ends; one left by a stopped task expires after
//! `keep_alive`, which is what lets a quick restart resume inside it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, ErrorClass, EventBatch,
    Position, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::bulk::connect;
use crate::client::{Client, hex, read_error};
use crate::config::{Api, EsConfig};

/// The source's position (JSON in `Position::opaque`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PitPosition {
    /// The PIT this pass reads, while it is open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pit: Option<String>,
    /// The sort values of the last document read inside `pit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_after: Option<Vec<Value>>,
    /// The pass's lower bound: `cursor >= since`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<Value>,
    /// The greatest cursor value read so far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cursor: Option<Value>,
    /// The pass reached the end of the index.
    #[serde(default)]
    pub pass_done: bool,
}

impl PitPosition {
    pub fn to_position(&self) -> Position {
        let opaque = serde_json::to_vec(self).unwrap_or_default();
        let mut display = match &self.last_cursor {
            Some(c) => format!("cursor {c}"),
            None => "no cursor yet".to_owned(),
        };
        if self.pass_done {
            display.push_str(", pass done");
        } else if self.pit.is_some() {
            display.push_str(", inside a point in time");
        }
        Position::new(Bytes::from(opaque), display)
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque).map_err(|_| {
            ConnectorError::position_lost(format!(
                "the saved position ({}) is not a search position",
                p.display
            ))
        })
    }
}

impl Api {
    fn open_pit_target(self, index: &str, keep_alive: &str) -> String {
        match self {
            Api::Elasticsearch => format!("/{index}/_pit?keep_alive={keep_alive}"),
            Api::Opensearch => format!("/{index}/_search/point_in_time?keep_alive={keep_alive}"),
        }
    }

    fn pit_id(self, answer: &Value) -> Option<String> {
        let field = match self {
            Api::Elasticsearch => "id",
            Api::Opensearch => "pit_id",
        };
        answer[field].as_str().map(str::to_owned)
    }

    fn close_pit(self, id: &str) -> (&'static str, Value) {
        match self {
            Api::Elasticsearch => ("/_pit", json!({ "id": id })),
            Api::Opensearch => ("/_search/point_in_time", json!({ "pit_id": [id] })),
        }
    }
}

/// The `_search` body of one page.
pub fn search_body(cfg: &EsConfig, pos: &PitPosition, pit: &str, size: usize) -> Value {
    let tb = cfg.tiebreaker();
    let mut sort = Vec::new();
    if let Some(c) = &cfg.source.cursor_field {
        sort.push(json!({ c.as_str(): { "order": "asc" } }));
    }
    sort.push(json!({ tb: { "order": "asc" } }));
    let base = cfg
        .source
        .query
        .clone()
        .unwrap_or_else(|| json!({ "match_all": {} }));
    let query = match (&cfg.source.cursor_field, &pos.since) {
        (Some(c), Some(since)) => json!({ "bool": { "filter": [
            base,
            { "range": { c.as_str(): { "gte": since } } }
        ] } }),
        _ => base,
    };
    let mut body = json!({
        "size": size,
        "pit": { "id": pit, "keep_alive": cfg.source.keep_alive },
        "sort": sort,
        "query": query,
        "seq_no_primary_term": true,
        "track_total_hits": false,
    });
    if let Some(after) = &pos.search_after {
        body["search_after"] = Value::Array(after.clone());
    }
    body
}

/// The event of one hit: `type` `…document.v1`, `subject` = `_id`, data =
/// `_source`, and an id stable across re-reads of the same version.
pub fn hit_event(builder: &EnvelopeBuilder, index: &str, hit: &Value) -> CloudEvent {
    let doc_index = hit["_index"].as_str().unwrap_or(index);
    let id = hit["_id"].as_str().unwrap_or_default();
    let source = hit.get("_source").cloned().unwrap_or(Value::Null);
    let data = serde_json::to_vec(&source).unwrap_or_default();
    let version = match (hit["_seq_no"].as_u64(), hit["_primary_term"].as_u64()) {
        (Some(s), Some(t)) => format!("{s}.{t}"),
        _ => hex(&Sha256::digest(&data)[..8]),
    };
    builder
        .event("document", index, format!("{doc_index}/{id}@{version}"))
        .with_subject(id)
        .with_data("application/json", data)
}

/// One split per index.
#[derive(Debug)]
pub struct IndexEnumerator {
    pub index: String,
}

#[async_trait]
impl SplitEnumerator for IndexEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.index.clone(),
            json!({ "index": self.index }),
        )])
    }
}

/// Builds a [`PitSource`] per split.
pub struct PitSourceFactory {
    pub cfg: Arc<EsConfig>,
    pub ctx: ConnectorContext,
    pub connector: &'static str,
}

impl std::fmt::Debug for PitSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PitSourceFactory")
            .field("index", &self.cfg.index)
            .finish_non_exhaustive()
    }
}

impl SourceFactory for PitSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(PitSource {
            cfg: self.cfg.clone(),
            builder: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            ctx: self.ctx.clone(),
            client: OnceCell::new(),
            split: split.clone(),
            pos: PitPosition::default(),
            wait_until: None,
            finished: false,
        }))
    }
}

/// See the module documentation.
pub struct PitSource {
    cfg: Arc<EsConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    client: OnceCell<Client>,
    split: Split,
    pos: PitPosition,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for PitSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PitSource")
            .field("split", &self.split.id)
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

enum Page {
    Hits(Vec<Value>),
    /// The PIT is gone (expired, or deleted by a finished pass).
    PitGone,
}

impl PitSource {
    async fn client(&self) -> Result<&Client, ConnectorError> {
        self.client
            .get_or_try_init(|| connect(&self.cfg, &self.ctx))
            .await
    }

    fn poll_interval(&self) -> Option<Duration> {
        self.cfg.source.poll_interval_ms.map(Duration::from_millis)
    }

    /// Start a new pass from the last cursor value read.
    fn new_pass(&mut self) {
        self.pos.pit = None;
        self.pos.search_after = None;
        self.pos.pass_done = false;
        self.pos.since = if self.cfg.source.cursor_field.is_some() {
            self.pos.last_cursor.clone()
        } else {
            None
        };
    }

    async fn open_pit(&mut self) -> Result<String, ConnectorError> {
        let api = self.cfg.api;
        let target = api.open_pit_target(&self.cfg.index, &self.cfg.source.keep_alive);
        let client = self.client().await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        let answer = client
            .json(Method::POST, &target, None)
            .await
            .map_err(read_error)?;
        api.pit_id(&answer)
            .ok_or_else(|| ConnectorError::retryable("opening a point in time returned no id"))
    }

    async fn close_pit(&self, id: &str) {
        let (target, body) = self.cfg.api.close_pit(id);
        if let Ok(client) = self.client().await {
            self.ctx.observe.api_call(&self.ctx.instance);
            if let Err(e) = client.json(Method::DELETE, target, Some(&body)).await {
                tracing::debug!(index = %self.cfg.index, error = %e.message, "closing a point in time");
            }
        }
    }

    async fn page(&mut self, pit: &str, size: usize) -> Result<Page, ConnectorError> {
        let body = search_body(&self.cfg, &self.pos, pit, size);
        let client = self.client().await?;
        self.ctx.observe.api_call(&self.ctx.instance);
        let resp = client
            .send(
                Method::POST,
                "/_search",
                Bytes::from(body.to_string()),
                Some("application/json"),
            )
            .await?;
        if resp.status() == http::StatusCode::NOT_FOUND {
            return Ok(Page::PitGone);
        }
        if let Some(err) = crate::client::classify(&resp) {
            return Err(read_error(err));
        }
        let answer = crate::client::parse_body(&resp)?;
        if let Some(id) = answer["pit_id"].as_str() {
            // The PIT id may change between pages; always use the latest.
            self.pos.pit = Some(id.to_owned());
        }
        Ok(Page::Hits(
            answer["hits"]["hits"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        ))
    }

    /// The pass reached the end of the index.
    async fn end_pass(&mut self) {
        if let Some(pit) = self.pos.pit.take() {
            self.close_pit(&pit).await;
        }
        self.pos.search_after = None;
        self.pos.pass_done = true;
        self.ctx
            .observe
            .lag(&self.ctx.instance, Some(&self.split.id), Duration::ZERO);
        match self.poll_interval() {
            Some(d) => self.wait_until = Some(Instant::now() + d),
            None => self.finished = true,
        }
    }

    fn take(&mut self, hit: &Value) {
        if let Some(sort) = hit["sort"].as_array() {
            self.pos.search_after = Some(sort.clone());
            if self.cfg.source.cursor_field.is_some()
                && let Some(c) = sort.first()
            {
                self.pos.last_cursor = Some(c.clone());
            }
        }
    }
}

#[async_trait]
impl Source for PitSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.split = split.clone();
        self.pos = match &pos {
            Some(p) => PitPosition::from_position(p)?,
            None => PitPosition::default(),
        };
        self.finished = false;
        self.wait_until = None;
        if self.pos.pass_done {
            match self.poll_interval() {
                Some(d) => self.wait_until = Some(Instant::now() + d),
                None => self.finished = true,
            }
        }
        // Connect now, so bad credentials fail the open, not a read.
        self.client().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.finished {
                return Ok(None);
            }
            if let Some(t) = self.wait_until.take() {
                // Cancel-safe: a stop drops the sleep, and the next `next`
                // starts the pass at once.
                tokio::time::sleep_until(t).await;
                self.new_pass();
            }
            if self.pos.pass_done {
                self.new_pass();
            }
            let fresh = self.pos.pit.is_none();
            let pit = match self.pos.pit.clone() {
                Some(p) => p,
                None => {
                    self.pos.search_after = None;
                    let p = self.open_pit().await?;
                    self.pos.pit = Some(p.clone());
                    p
                }
            };
            let size = self.cfg.source.page_size.min(budget.max_rows.max(1));
            let hits = match self.page(&pit, size).await? {
                Page::PitGone if fresh => {
                    return Err(ConnectorError::retryable(
                        "a new point in time was gone at its first search",
                    ));
                }
                Page::PitGone => {
                    tracing::debug!(index = %self.cfg.index, "point in time gone; a new pass from the cursor");
                    self.new_pass();
                    continue;
                }
                Page::Hits(h) => h,
            };
            if hits.is_empty() {
                self.end_pass().await;
                continue;
            }
            let mut events = Vec::with_capacity(hits.len());
            let mut bytes = 0;
            for hit in &hits {
                let ev = hit_event(&self.builder, &self.cfg.index, hit);
                let sz = ev.size_bytes();
                if events.len() >= budget.max_rows || bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        // R4.7: a document larger than the batch budget can
                        // never be read; skip it as Poison and carry on.
                        self.take(hit);
                        return Err(ConnectorError::new(
                            ErrorClass::Poison,
                            format!(
                                "document {} ({sz} bytes) exceeds the batch budget",
                                hit["_id"].as_str().unwrap_or("?")
                            ),
                        ));
                    }
                    break;
                }
                bytes += sz;
                self.take(hit);
                events.push(ev);
            }
            if events.len() == hits.len() && hits.len() < size {
                // A short page: the pass is done.
                self.end_pass().await;
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(events),
                position: self.pos.to_position(),
            }));
        }
    }

    /// Nothing to commit upstream: the position is all the state.
    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// The PIT is left to expire (`keep_alive`), so a restart soon after a
    /// stop resumes inside it.
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    #[test]
    fn positions_round_trip_and_hide_the_pit() {
        let p = PitPosition {
            pit: Some("secret-looking-pit".into()),
            search_after: Some(vec![json!(5), json!(17)]),
            since: Some(json!(1)),
            last_cursor: Some(json!(5)),
            pass_done: false,
        };
        let pos = p.to_position();
        assert_eq!(PitPosition::from_position(&pos).expect("decode"), p);
        assert!(!pos.display.contains("secret-looking-pit"));
        assert_eq!(
            PitPosition::from_position(&Position::from_u64(3))
                .expect_err("foreign")
                .class,
            ErrorClass::PositionLost
        );
    }

    #[test]
    fn bodies() {
        let cfg = EsConfig::parse(&json!({
            "url": "http://x", "index": "i",
            "source": {"cursor_field": "ts", "query": {"term": {"k": "v"}}}
        }))
        .expect("cfg");
        let pos = PitPosition {
            since: Some(json!(10)),
            search_after: Some(vec![json!(12), json!(3)]),
            ..Default::default()
        };
        let b = search_body(&cfg, &pos, "p", 50);
        assert_eq!(b["size"], 50);
        assert_eq!(b["pit"], json!({"id": "p", "keep_alive": "2m"}));
        assert_eq!(b["search_after"], json!([12, 3]));
        assert_eq!(
            b["query"],
            json!({"bool": {"filter": [{"term": {"k": "v"}}, {"range": {"ts": {"gte": 10}}}]}})
        );
    }

    #[test]
    fn hit_ids_are_stable_per_version() {
        let b = EnvelopeBuilder::new("elasticsearch", &InstanceKey::new(1, "i"));
        let hit = json!({"_index": "logs-1", "_id": "a", "_seq_no": 4, "_primary_term": 2, "_source": {"x": 1}});
        let e = hit_event(&b, "logs", &hit);
        assert_eq!(e.id, "logs-1/a@4.2");
        assert_eq!(e.source, "/connectors/i/logs");
        assert_eq!(hit_event(&b, "logs", &hit).id, e.id);
        let no_seq = json!({"_id": "a", "_source": {"x": 1}});
        let e2 = hit_event(&b, "logs", &no_seq);
        assert!(e2.id.starts_with("logs/a@"), "{}", e2.id);
        let changed = json!({"_id": "a", "_source": {"x": 2}});
        assert_ne!(hit_event(&b, "logs", &changed).id, e2.id);
    }
}
