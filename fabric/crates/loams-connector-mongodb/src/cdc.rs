//! Change-stream CDC (OR-9), after an optional snapshot.
//!
//! **One split per instance**, named after the database: a database-level
//! change stream, filtered to the configured collections (or every
//! collection), with `fullDocument: updateLookup` (or the post-image) and,
//! when asked, the pre-image (6.0+).
//!
//! **The handoff.** With `snapshot: initial` the source first records a
//! cluster time T, reads every collection at T (`readConcern: snapshot`),
//! and emits each document as a `change` event with `loamsop = r`. The
//! stream then starts at T (`startAtOperationTime`) and drops the events
//! whose `clusterTime` is T or earlier: a write committed at or before T is
//! in the snapshot and not in the stream, one after T is in the stream and
//! not in the snapshot, so there is no gap and no overlap. If the snapshot ages
//! out of the server's history window mid-way (a long read, a restart), the
//! rest of it is read at a new cluster time while the stream still starts
//! after T: documents read late may arrive twice (their changes are
//! replayed), never be missed (plan ruling R16.8).
//!
//! **The position** is the snapshot's progress, then the stream's resume
//! token. After each fetched batch the token is the server's
//! `postBatchResumeToken`, so a stream filtered to a quiet collection still
//! advances; an empty batch carries a newer token to the checkpoint at most
//! every `heartbeat_ms`. `committed` has nothing to acknowledge: a change
//! stream holds no server-side state, and the oplog is the server's.
//!
//! **Events.** `id` = `<db>.<collection>/<token>` (stable across re-reads),
//! `loamsop` from `operationType`, `loamslsn` = the token's `_data` (it sorts
//! in stream order), `data` = `{ns, op, key, before, after, update}` in
//! relaxed Extended JSON. Collection DDL (`drop`, `rename`, `create`,
//! `modify`, index changes, with `showExpandedEvents` on 6.0+) becomes a
//! `schema-change` event, and each collection's document schema is tracked
//! like the batch source's (an additive field evolves; a type no column
//! holds pauses). **`invalidate`** (the database dropped or renamed) pauses
//! the instance; on resume the stream starts after the invalidate.
//!
//! **The oplog window** (Review Focus 6): every `window_ms` the source reads
//! the oldest and newest entries of `local.oplog.rs` and reports the token's
//! age as `lag` and the share of the window it has used as
//! `position_risk` (past 1 the token is gone). A token the oplog no longer
//! holds fails the resume with `ChangeStreamHistoryLost`, which is
//! `PositionLost`: the instance stops with the re-snapshot hint.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::envelope::CdcOp;
use loams_connector_sdk::schema::SchemaTracker;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position,
    Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitId, SplitState,
};
use serde_json::{Value, json};

use crate::batch::{bson_from_canonical, cluster_time, collections, parse_ts};
use crate::bson::{Bson, Document, Timestamp, unhex};
use crate::client::Client;
use crate::config::{BeforeChange, FullDocument, MongoConfig};
use crate::doc;
use crate::extjson::{self, Mode};
use crate::scan::{Scan, snapshot_too_old};
use crate::types;

/// Where the CDC split is.
#[derive(Debug, Clone, PartialEq)]
pub enum CdcPos {
    /// Reading collection `index` of `collections` at `read_at`; the stream
    /// will start just after `stream_from`.
    Snapshot {
        stream_from: Timestamp,
        read_at: Timestamp,
        collections: Vec<String>,
        index: usize,
        last: Option<Bson>,
        rows: u64,
    },
    /// Streaming after `token` (or, before the first token, at
    /// `start_at`).
    Stream {
        token: Option<Document>,
        start_at: Option<Timestamp>,
        /// A token whose `invalidate` paused the instance: the stream
        /// restarts after it.
        after_invalidate: bool,
    },
}

fn token_json(t: &Document) -> Value {
    extjson::document_to_json(t, Mode::Canonical)
}

fn token_from_json(v: &Value) -> Result<Option<Document>, ConnectorError> {
    match bson_from_canonical(v)? {
        None => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d)),
        Some(_) => Err(ConnectorError::fatal(
            "a stored resume token is not a document",
        )),
    }
}

/// The cluster time a resume token encodes: its `_data` is a hex KeyString
/// whose first value is the event's timestamp (type byte 130, then the
/// time and the increment, big-endian).
pub fn token_time(token: &Document) -> Option<Timestamp> {
    let data = token.get_str("_data")?;
    let bytes = unhex(data.get(..18)?)?;
    if bytes[0] != 130 {
        return None;
    }
    Some(Timestamp::new(
        u32::from_be_bytes(bytes[1..5].try_into().ok()?),
        u32::from_be_bytes(bytes[5..9].try_into().ok()?),
    ))
}

impl CdcPos {
    pub fn to_position(&self) -> Position {
        let (v, display) = match self {
            CdcPos::Snapshot {
                stream_from,
                read_at,
                collections,
                index,
                last,
                rows,
            } => (
                json!({
                    "phase": "snapshot",
                    "stream_from": stream_from.to_string(),
                    "read_at": read_at.to_string(),
                    "collections": collections,
                    "index": index,
                    "last": last.as_ref().map(|b| extjson::to_json(b, Mode::Canonical)),
                    "rows": rows,
                }),
                format!(
                    "snapshot at {stream_from}: {rows} documents, collection {} of {}",
                    index + 1,
                    collections.len()
                ),
            ),
            CdcPos::Stream {
                token,
                start_at,
                after_invalidate,
            } => (
                json!({
                    "phase": "stream",
                    "token": token.as_ref().map(token_json),
                    "start_at": start_at.map(|t| t.to_string()),
                    "after_invalidate": after_invalidate,
                }),
                match token.as_ref().and_then(token_time).or(*start_at) {
                    Some(t) => format!("stream at {t}"),
                    None => "stream".to_owned(),
                },
            ),
        };
        Position::new(Bytes::from(v.to_string()), display)
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        let v: Value = serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("a CDC position is not JSON: {e}")))?;
        let need = |t: Option<Timestamp>, what: &str| {
            t.ok_or_else(|| ConnectorError::fatal(format!("a CDC position has no {what}")))
        };
        match v["phase"].as_str() {
            Some("snapshot") => Ok(CdcPos::Snapshot {
                stream_from: need(parse_ts(&v["stream_from"])?, "stream_from")?,
                read_at: need(parse_ts(&v["read_at"])?, "read_at")?,
                collections: v["collections"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| c.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                index: v["index"].as_u64().unwrap_or(0) as usize,
                last: bson_from_canonical(&v["last"])?,
                rows: v["rows"].as_u64().unwrap_or(0),
            }),
            Some("stream") => Ok(CdcPos::Stream {
                token: token_from_json(&v["token"])?,
                start_at: parse_ts(&v["start_at"])?,
                after_invalidate: v["after_invalidate"].as_bool().unwrap_or(false),
            }),
            _ => Err(ConnectorError::fatal("a CDC position has no phase")),
        }
    }
}

/// The oplog window as last sampled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OplogWindow {
    pub first: Timestamp,
    pub last: Timestamp,
    /// How far behind the newest entry the position is.
    pub lag: Duration,
    /// The share of the window the position has used (0–1; past 1 the
    /// position is gone).
    pub risk: f64,
}

/// The lag and risk of a position at `at` in a window `[first, last]`.
pub fn window(first: Timestamp, last: Timestamp, at: Timestamp) -> OplogWindow {
    let span = f64::from(last.time.saturating_sub(first.time)).max(1.0);
    let behind = last.time.saturating_sub(at.time);
    let risk = if at < first {
        1.0 + f64::from(first.time - at.time) / span
    } else {
        f64::from(behind) / span
    };
    OplogWindow {
        first,
        last,
        lag: Duration::from_secs(u64::from(behind)),
        risk,
    }
}

/// One split per instance.
#[derive(Debug)]
pub struct CdcEnumerator {
    split: SplitId,
}

impl CdcEnumerator {
    pub fn new(cfg: &MongoConfig) -> Self {
        Self {
            split: SplitId::new(cfg.database.clone()),
        }
    }
}

#[async_trait]
impl SplitEnumerator for CdcEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(self.split.clone(), json!({}))])
    }
}

/// Makes the CDC source; keeps the last oplog window it sampled.
#[derive(Debug)]
pub struct CdcFactory {
    cfg: MongoConfig,
    ctx: ConnectorContext,
    connector: &'static str,
    window: Arc<Mutex<Option<OplogWindow>>>,
}

impl CdcFactory {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
            window: Arc::default(),
        }
    }

    /// The oplog window the running source last sampled.
    pub fn window(&self) -> Option<OplogWindow> {
        *self.window.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl SourceFactory for CdcFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(CdcSource {
            client: Client::new(self.cfg.clone(), self.ctx.clone()),
            window_client: Client::new(self.cfg.clone(), self.ctx.clone()),
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            tracker: SchemaTracker::new(self.cfg.schema.evolution),
            split: split.id.clone(),
            ctx: self.ctx.clone(),
            pos: None,
            scan: None,
            cursor: None,
            pending: VecDeque::new(),
            batch_token: None,
            emitted_token: None,
            last_emit: Instant::now(),
            last_seen: None,
            window: self.window.clone(),
            next_window: Instant::now(),
            window_off: false,
            expanded: false,
            invalidated: None,
            carry: None,
        }))
    }
}

/// The source of one instance's CDC split.
pub struct CdcSource {
    client: Client,
    window_client: Client,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    tracker: SchemaTracker,
    split: SplitId,
    pos: Option<CdcPos>,
    scan: Option<Scan>,
    cursor: Option<i64>,
    /// Change documents fetched and not yet delivered.
    pending: VecDeque<Document>,
    /// The `postBatchResumeToken` of the last fetch.
    batch_token: Option<Document>,
    /// The token of the last batch handed to the pipeline.
    emitted_token: Option<Document>,
    last_emit: Instant,
    last_seen: Option<Timestamp>,
    window: Arc<Mutex<Option<OplogWindow>>>,
    next_window: Instant,
    window_off: bool,
    expanded: bool,
    invalidated: Option<Document>,
    /// The events of `pending`'s front document, built but not delivered.
    carry: Option<Vec<CloudEvent>>,
}

impl std::fmt::Debug for CdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcSource")
            .field("split", &self.split)
            .field("pos", &self.pos.as_ref().map(CdcPos::to_position))
            .finish_non_exhaustive()
    }
}

fn op_of(operation: &str) -> Option<CdcOp> {
    match operation {
        "insert" => Some(CdcOp::Create),
        "update" | "replace" => Some(CdcOp::Update),
        "delete" => Some(CdcOp::Delete),
        _ => None,
    }
}

const DDL: &[&str] = &[
    "drop",
    "rename",
    "create",
    "createIndexes",
    "dropIndexes",
    "modify",
    "shardCollection",
    "reshardCollection",
    "refineCollectionShardKey",
    "dropDatabase",
];

impl CdcSource {
    fn cfg(&self) -> &MongoConfig {
        self.client.config()
    }

    /// The `$changeStream` stage's options for a start from `pos`.
    fn stream_stage(&self, pos: &CdcPos) -> Document {
        let cfg = self.cfg();
        let mut opts = Document::new();
        if cfg.full_document != FullDocument::Off {
            opts.insert("fullDocument", cfg.full_document.server_value());
        }
        if cfg.before_change != BeforeChange::Off {
            opts.insert("fullDocumentBeforeChange", cfg.before_change.server_value());
        }
        if self.expanded {
            opts.insert("showExpandedEvents", true);
        }
        match pos {
            CdcPos::Stream {
                token: Some(t),
                after_invalidate: true,
                ..
            } => opts.insert("startAfter", t.clone()),
            CdcPos::Stream { token: Some(t), .. } => opts.insert("resumeAfter", t.clone()),
            CdcPos::Stream {
                start_at: Some(at), ..
            } => opts.insert("startAtOperationTime", *at),
            _ => {}
        }
        opts
    }

    fn pipeline(&self, pos: &CdcPos) -> Vec<Bson> {
        let mut stages = vec![Bson::from(
            doc! { "$changeStream" => self.stream_stage(pos) },
        )];
        let colls = &self.cfg().collections;
        if !colls.is_empty() {
            let names: Vec<Bson> = colls.iter().map(|c| Bson::from(c.as_str())).collect();
            stages.push(Bson::from(doc! { "$match" => doc! { "$or" => vec![
                Bson::from(doc! { "ns.coll" => doc! { "$in" => names } }),
                Bson::from(doc! { "operationType" => doc! { "$in" => vec![
                    Bson::from("dropDatabase"), Bson::from("invalidate"),
                ] } }),
            ] } }));
        }
        stages
    }

    /// Open (or reopen) the change stream at the current position.
    async fn open_stream(&mut self) -> Result<(), ConnectorError> {
        let pos = self.pos.clone().expect("opened");
        let cmd = doc! {
            "aggregate" => 1,
            "pipeline" => self.pipeline(&pos),
            "cursor" => doc! { "batchSize" => 0 },
        };
        let db = self.cfg().database.clone();
        let reply = self.client.command(&db, cmd).await?;
        let cursor = reply
            .get_document("cursor")
            .ok_or_else(|| ConnectorError::fatal("aggregate: the reply has no cursor"))?;
        self.cursor = Some(cursor.get_i64("id").unwrap_or(0)).filter(|id| *id != 0);
        if let Some(t) = cursor.get_document("postBatchResumeToken") {
            self.batch_token = Some(t.clone());
        }
        if self.cursor.is_none() {
            return Err(ConnectorError::retryable(
                "the change stream closed at once",
            ));
        }
        Ok(())
    }

    /// Fetch more change documents into `pending`.
    async fn fetch(&mut self, rows: usize) -> Result<(), ConnectorError> {
        if self.cursor.is_none() {
            self.open_stream().await?;
        }
        let id = self.cursor.expect("opened above");
        let db = self.cfg().database.clone();
        let await_ms = self.cfg().await_ms as i64;
        let r = self
            .client
            .command(
                &db,
                doc! {
                    "getMore" => id,
                    "collection" => "$cmd.aggregate",
                    "batchSize" => rows.clamp(1, 10_000) as i32,
                    "maxTimeMS" => await_ms,
                },
            )
            .await;
        let reply = match r {
            Ok(r) => r,
            Err(e) => {
                // Resumable errors reopen from the last delivered token.
                self.cursor = None;
                self.pending.clear();
                self.carry = None;
                self.batch_token = None;
                return Err(e);
            }
        };
        let cursor = reply
            .get_document("cursor")
            .ok_or_else(|| ConnectorError::fatal("getMore: the reply has no cursor"))?;
        for d in cursor.get_array("nextBatch").unwrap_or_default() {
            if let Bson::Document(d) = d {
                self.pending.push_back(d.clone());
            }
        }
        if let Some(t) = cursor.get_document("postBatchResumeToken") {
            self.batch_token = Some(t.clone());
        }
        if cursor.get_i64("id").unwrap_or(0) == 0 {
            self.cursor = None;
        }
        Ok(())
    }

    /// A change document as events (a `change`, a `schema-change`, or both
    /// when a document brings new fields), or the pause of `invalidate`.
    fn events_for(&mut self, d: &Document) -> Result<Vec<CloudEvent>, ConnectorError> {
        let token = d
            .get_document("_id")
            .ok_or_else(|| ConnectorError::fatal("a change event has no resume token"))?;
        let data = token.get_str("_data").unwrap_or("").to_owned();
        let operation = d.get_str("operationType").unwrap_or("");
        let ns = d.get_document("ns");
        let db = ns
            .and_then(|n| n.get_str("db"))
            .unwrap_or(&self.client.config().database)
            .to_owned();
        let coll = ns.and_then(|n| n.get_str("coll")).map(str::to_owned);
        let table = match &coll {
            Some(c) => format!("{db}.{c}"),
            None => db.clone(),
        };
        if let Some(t) = d.get("clusterTime").and_then(Bson::as_timestamp) {
            self.last_seen = Some(self.last_seen.map_or(t, |s| s.max(t)));
        }
        let time = d
            .get("wallTime")
            .and_then(|w| match w {
                Bson::DateTime(ms) => extjson::iso_millis(*ms),
                _ => None,
            })
            .or_else(|| {
                d.get("clusterTime")
                    .and_then(Bson::as_timestamp)
                    .and_then(|t| extjson::iso_millis(i64::from(t.time) * 1000))
            });
        let rel = |k: &str| {
            d.get(k)
                .map_or(Value::Null, |v| extjson::to_json(v, Mode::Relaxed))
        };
        if operation == "invalidate" {
            self.invalidated = Some(token.clone());
            return Err(ConnectorError::schema_incompatible(format!(
                "the change stream on {db} was invalidated (the database was dropped or renamed); \
                 resume the instance to start after the invalidate"
            )));
        }
        if DDL.contains(&operation) {
            if matches!(operation, "drop" | "rename" | "dropDatabase") {
                // A recreated collection starts a new schema.
                if let Some(c) = &coll {
                    self.tracker
                        .seed(format!("{db}.{c}"), Arc::new(arrow_schema::Schema::empty()));
                }
            }
            let to = d.get_document("to").map(|t| {
                format!(
                    "{}.{}",
                    t.get_str("db").unwrap_or(""),
                    t.get_str("coll").unwrap_or("")
                )
            });
            let mut ev = self
                .envelope
                .event("schema-change", &table, format!("{table}/{data}"))
                .with_subject(table.clone())
                .with_data(
                    "application/json",
                    json!({ "table": table, "op": operation, "to": to, "at": data }).to_string(),
                );
            if let Some(t) = time {
                ev = ev.with_time(t);
            }
            return Ok(vec![ev]);
        }
        let Some(op) = op_of(operation) else {
            // An event type this version does not know: skipped, with a note.
            tracing::debug!(operation, "skipping a change stream event");
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        if let Some(Bson::Document(full)) = d.get("fullDocument") {
            let current = self.tracker.current(&table);
            let schema =
                types::union_schema(current.as_deref(), std::slice::from_ref(full), &table);
            let here = self.pos.as_ref().expect("opened").to_position();
            if let Some(change) = self.tracker.observe(&table, Arc::new(schema), &here)? {
                out.push(change.event(&self.envelope));
            }
        }
        let update = d.get_document("updateDescription").map(|u| {
            json!({
                "updated": u.get("updatedFields").map_or(Value::Null, |v| extjson::to_json(v, Mode::Relaxed)),
                "removed": u.get("removedFields").map_or(Value::Null, |v| extjson::to_json(v, Mode::Relaxed)),
                "truncated": u.get("truncatedArrays").map_or(Value::Null, |v| extjson::to_json(v, Mode::Relaxed)),
            })
        });
        let body = json!({
            "ns": table,
            "op": op.as_str(),
            "key": rel("documentKey"),
            "before": rel("fullDocumentBeforeChange"),
            "after": rel("fullDocument"),
            "update": update,
        });
        let mut ev = self
            .envelope
            .event("change", &table, format!("{table}/{data}"))
            .with_subject(coll.unwrap_or_default())
            .with_cdc(op, data)
            .with_data("application/json", body.to_string());
        if let Some(t) = time {
            ev = ev.with_time(t);
        }
        out.push(ev);
        Ok(out)
    }

    /// Sample the oplog window (best effort; off for good once the user
    /// cannot read `local.oplog.rs`).
    async fn sample_window(&mut self) {
        if self.window_off || Instant::now() < self.next_window {
            return;
        }
        self.next_window = Instant::now() + self.cfg().window_interval;
        let at = match self.pos.as_ref() {
            Some(CdcPos::Stream {
                token, start_at, ..
            }) => token
                .as_ref()
                .and_then(token_time)
                .or(self.last_seen)
                .or(*start_at),
            Some(CdcPos::Snapshot { stream_from, .. }) => Some(*stream_from),
            None => None,
        };
        let Some(at) = at else { return };
        let mut ends = Vec::with_capacity(2);
        for dir in [1, -1] {
            let r = self
                .window_client
                .command(
                    "local",
                    doc! {
                        "find" => "oplog.rs", "filter" => doc! {}, "sort" => doc! { "$natural" => dir },
                        "projection" => doc! { "ts" => 1 }, "limit" => 1, "singleBatch" => true,
                    },
                )
                .await;
            match r {
                Ok(reply) => ends.push(
                    reply
                        .get_document("cursor")
                        .and_then(|c| c.get_array("firstBatch"))
                        .and_then(|b| b.first())
                        .and_then(Bson::as_document)
                        .and_then(|d| d.get("ts"))
                        .and_then(Bson::as_timestamp),
                ),
                Err(e) if e.class.is_transient() => return,
                Err(e) => {
                    tracing::warn!(
                        "the oplog window metric is off: reading local.oplog.rs failed: {}",
                        e.message
                    );
                    self.window_off = true;
                    return;
                }
            }
        }
        let (Some(first), Some(last)) = (ends[0], ends[1]) else {
            return;
        };
        let w = window(first, last, at);
        *self.window.lock().unwrap_or_else(|p| p.into_inner()) = Some(w);
        let observe = &self.ctx.observe;
        observe.lag(&self.ctx.instance, Some(&self.split), w.lag);
        observe.position_risk(&self.ctx.instance, Some(&self.split), w.risk);
    }

    /// Start the snapshot: record T, list the collections.
    async fn begin_snapshot(&mut self) -> Result<CdcPos, ConnectorError> {
        let cfg = self.cfg().clone();
        let colls = collections(&mut self.client, &cfg).await?;
        let probe = colls
            .first()
            .cloned()
            .unwrap_or_else(|| crate::config::CHECKPOINTS.to_owned());
        let t = cluster_time(&mut self.client, &cfg.database, &probe).await?;
        Ok(CdcPos::Snapshot {
            stream_from: t,
            read_at: t,
            collections: colls,
            index: 0,
            last: None,
            rows: 0,
        })
    }

    /// The next snapshot batch, or the switch to streaming.
    async fn next_snapshot(
        &mut self,
        budget: Budget,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(CdcPos::Snapshot {
            stream_from,
            read_at,
            collections,
            index,
            last,
            rows,
        }) = self.pos.clone()
        else {
            unreachable!("called in the snapshot phase");
        };
        let db = self.cfg().database.clone();
        if index >= collections.len() {
            self.scan = None;
            self.pos = Some(CdcPos::Stream {
                token: None,
                start_at: Some(stream_from),
                after_invalidate: false,
            });
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(Vec::new()),
                position: self.pos.as_ref().expect("set").to_position(),
            }));
        }
        let coll = collections[index].clone();
        if self.scan.as_ref().is_none_or(|s| s.collection != coll) {
            let mut s = Scan::new(coll.clone(), true);
            s.at = Some(read_at);
            s.last = last.clone();
            self.scan = Some(s);
        }
        let scan = self.scan.as_mut().expect("set above");
        if let Err(e) = scan
            .fill(&mut self.client, &db, budget.max_rows, budget.max_bytes / 2)
            .await
        {
            if snapshot_too_old(&e) {
                tracing::warn!(collection = %coll, "the CDC snapshot aged out; continuing at a new cluster time");
                let t = cluster_time(&mut self.client, &db, &coll).await?;
                scan.at = Some(t);
                scan.reopen();
                if let Some(CdcPos::Snapshot { read_at, .. }) = self.pos.as_mut() {
                    *read_at = t;
                }
            }
            return Err(e);
        }
        if scan.done() {
            self.scan = None;
            self.pos = Some(CdcPos::Snapshot {
                stream_from,
                read_at,
                collections,
                index: index + 1,
                last: None,
                rows,
            });
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(Vec::new()),
                position: self.pos.as_ref().expect("set").to_position(),
            }));
        }
        let table = format!("{db}.{coll}");
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut taken = 0usize;
        let mut new_rows = rows;
        let candidates: Vec<Document> =
            scan.pending.iter().take(budget.max_rows).cloned().collect();
        // The schema of what this batch reads first, so a new field's
        // schema-change event comes before the documents that carry it.
        let current = self.tracker.current(&table);
        let schema = types::union_schema(current.as_deref(), &candidates, &table);
        let here = self.pos.as_ref().expect("snapshot").to_position();
        if let Some(change) = self.tracker.observe(&table, Arc::new(schema), &here)? {
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(vec![change.event(&self.envelope)]),
                position: here,
            }));
        }
        for d in &candidates {
            let id = d.get("_id").cloned().unwrap_or(Bson::Null);
            let key = extjson::canonical_string(&id);
            let body = json!({
                "ns": table, "op": "r",
                "key": { "_id": extjson::to_json(&id, Mode::Relaxed) },
                "before": null,
                "after": extjson::document_to_json(d, Mode::Relaxed),
                "update": null,
            });
            let ev = self
                .envelope
                .event("change", &table, format!("{table}/r/{stream_from}/{key}"))
                .with_subject(coll.clone())
                .with_cdc(CdcOp::Read, format!("r/{stream_from}"))
                .with_data("application/json", body.to_string());
            let size = ev.size_bytes();
            if taken > 0 && bytes + size > budget.max_bytes {
                break;
            }
            if size > budget.max_bytes {
                scan.take(1);
                return Err(ConnectorError::poison(format!(
                    "{table}: document {key} is {size} bytes as an event, over the batch budget of {} bytes",
                    budget.max_bytes
                )));
            }
            bytes += size;
            taken += 1;
            new_rows += 1;
            events.push(ev);
        }
        scan.take(taken);
        let read_at = scan.at.unwrap_or(read_at);
        self.pos = Some(CdcPos::Snapshot {
            stream_from,
            read_at,
            collections,
            index,
            last: scan.last.clone(),
            rows: new_rows,
        });
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.as_ref().expect("set").to_position(),
        }))
    }

    fn stream_pos(&self, token: Option<Document>) -> CdcPos {
        match self.pos.as_ref() {
            Some(CdcPos::Stream { start_at, .. }) => CdcPos::Stream {
                start_at: *start_at,
                token,
                after_invalidate: false,
            },
            _ => CdcPos::Stream {
                token,
                start_at: None,
                after_invalidate: false,
            },
        }
    }

    async fn next_stream(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(t) = self.invalidated.take() {
            // Resumed after the pause: start after the invalidate.
            self.pos = Some(CdcPos::Stream {
                token: Some(t),
                start_at: None,
                after_invalidate: true,
            });
            self.cursor = None;
            self.pending.clear();
            self.carry = None;
            self.batch_token = None;
        }
        if self.pending.is_empty() {
            self.fetch(budget.max_rows).await?;
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut last_token = None;
        while let Some(d) = self.pending.front().cloned() {
            // A stream started at the snapshot's time T: events at or before
            // T are in the snapshot already.
            if self.carry.is_none()
                && let Some(CdcPos::Stream {
                    token: None,
                    start_at: Some(t),
                    ..
                }) = &self.pos
                && d.get_str("operationType") != Some("invalidate")
                && d.get("clusterTime")
                    .and_then(Bson::as_timestamp)
                    .is_some_and(|ct| ct <= *t)
            {
                self.pending.pop_front();
                last_token = d.get_document("_id").cloned();
                continue;
            }
            // The front document's events may have been built by a call that
            // ran out of budget: the schema tracker has already moved, so
            // they are reused rather than built again.
            let built = match self.carry.take() {
                Some(evs) => Ok(evs),
                None => self.events_for(&d),
            };
            let evs = match built {
                Ok(evs) => evs,
                Err(e) if !events.is_empty() => {
                    // Deliver what came before; the failing event (an
                    // invalidate or an incompatible schema) is met again on
                    // the next call.
                    self.invalidated = None;
                    let _ = e;
                    break;
                }
                Err(e) => {
                    if self.invalidated.is_some() {
                        self.pending.clear();
                        self.carry = None;
                        self.cursor = None;
                    }
                    return Err(e);
                }
            };
            let size: usize = evs.iter().map(CloudEvent::size_bytes).sum();
            if events.len() + evs.len() > budget.max_rows
                || (!events.is_empty() && bytes + size > budget.max_bytes)
            {
                self.carry = Some(evs);
                break;
            }
            self.pending.pop_front();
            let token = d.get_document("_id").cloned();
            if size > budget.max_bytes {
                return Err(ConnectorError::poison(format!(
                    "a change event of {size} bytes is over the batch budget of {} bytes",
                    budget.max_bytes
                )));
            }
            bytes += size;
            events.extend(evs);
            last_token = token;
        }
        let token = if self.pending.is_empty() {
            self.batch_token.clone().or(last_token)
        } else {
            last_token
        };
        let advanced = token.is_some() && token != self.emitted_token;
        if events.is_empty() {
            // Idle: carry a newer token to the checkpoint now and then.
            if !advanced || self.last_emit.elapsed() < self.cfg().heartbeat {
                return Ok(None);
            }
        }
        if token.is_some() {
            self.pos = Some(self.stream_pos(token.clone()));
            self.emitted_token = token;
        }
        self.last_emit = Instant::now();
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.as_ref().expect("opened").to_position(),
        }))
    }
}

#[async_trait]
impl Source for CdcSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let hello = self.client.hello().await?;
        if !hello.supports_change_streams() {
            return Err(ConnectorError::fatal(
                "change streams need a replica set or a sharded cluster; this server is standalone \
                 (use mode: batch, or run it as a one-member replica set)",
            ));
        }
        self.expanded = hello.supports_expanded_events();
        self.pos = match pos.as_ref().map(CdcPos::from_position).transpose()? {
            Some(p) => Some(p),
            None if self.cfg().snapshot => Some(self.begin_snapshot().await?),
            None => {
                let probe = self
                    .cfg()
                    .collections
                    .first()
                    .cloned()
                    .unwrap_or_else(|| crate::config::CHECKPOINTS.to_owned());
                let db = self.cfg().database.clone();
                let t = cluster_time(&mut self.client, &db, &probe).await?;
                Some(CdcPos::Stream {
                    token: None,
                    start_at: Some(t),
                    after_invalidate: false,
                })
            }
        };
        if let Some(CdcPos::Stream { token: Some(t), .. }) = &self.pos {
            self.emitted_token = Some(t.clone());
        }
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.sample_window().await;
        match self.pos {
            Some(CdcPos::Snapshot { .. }) => self.next_snapshot(budget).await,
            Some(CdcPos::Stream { .. }) => self.next_stream(budget).await,
            None => Err(ConnectorError::fatal("the CDC source is not open")),
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let db = self.cfg().database.clone();
        if let Some(id) = self.cursor.take() {
            let _ = self
                .client
                .command(
                    &db,
                    doc! { "killCursors" => "$cmd.aggregate", "cursors" => vec![Bson::Int64(id)] },
                )
                .await;
        }
        if let Some(scan) = self.scan.as_mut() {
            scan.kill(&mut self.client, &db).await;
        }
        self.client.reset();
        self.window_client.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_round_trip() {
        let snap = CdcPos::Snapshot {
            stream_from: Timestamp::new(100, 2),
            read_at: Timestamp::new(160, 1),
            collections: vec!["a".into(), "b".into()],
            index: 1,
            last: Some(Bson::Int64(9)),
            rows: 10,
        };
        let p = snap.to_position();
        assert_eq!(
            p.display,
            "snapshot at 100.2: 10 documents, collection 2 of 2"
        );
        assert_eq!(CdcPos::from_position(&p).expect("decode"), snap);
        let token = doc! { "_data" => "8265A1B2C3000000012B022C0100296E5A1004" };
        let stream = CdcPos::Stream {
            token: Some(token),
            start_at: Some(Timestamp::new(100, 3)),
            after_invalidate: true,
        };
        let p = stream.to_position();
        assert_eq!(
            p.display,
            format!("stream at {}", Timestamp::new(0x65A1_B2C3, 1))
        );
        assert_eq!(CdcPos::from_position(&p).expect("decode"), stream);
    }

    #[test]
    fn tokens_carry_their_cluster_time() {
        let t = doc! { "_data" => "8200000064000000072B0229296E04" };
        assert_eq!(token_time(&t), Some(Timestamp::new(100, 7)));
        assert_eq!(token_time(&doc! { "_data" => "zz" }), None);
        assert_eq!(
            token_time(&doc! { "_data" => "8100000064000000072B" }),
            None
        );
        assert_eq!(token_time(&doc! {}), None);
    }

    #[test]
    fn the_window_says_how_much_of_the_oplog_a_position_used() {
        let w = window(
            Timestamp::new(1000, 0),
            Timestamp::new(2000, 0),
            Timestamp::new(1900, 5),
        );
        assert_eq!(w.lag, Duration::from_secs(100));
        assert!((w.risk - 0.1).abs() < 1e-9);
        let lost = window(
            Timestamp::new(1000, 0),
            Timestamp::new(2000, 0),
            Timestamp::new(900, 0),
        );
        assert!(lost.risk > 1.0);
        let fresh = window(
            Timestamp::new(1000, 0),
            Timestamp::new(2000, 0),
            Timestamp::new(2000, 1),
        );
        assert_eq!(fresh.risk, 0.0);
    }

    #[test]
    fn operation_types_map_to_loamsop() {
        assert_eq!(op_of("insert"), Some(CdcOp::Create));
        assert_eq!(op_of("replace"), Some(CdcOp::Update));
        assert_eq!(op_of("delete"), Some(CdcOp::Delete));
        assert_eq!(op_of("drop"), None);
    }
}
