//! The batch source: each collection read once, at one cluster time, in
//! `_id`-range splits, as Arrow `document` batches (D356's bulk path).
//!
//! **Splits.** A collection of up to `split_rows` documents (by its
//! metadata count) is one split, named after the collection. A larger one
//! is cut into up to `max_splits` `_id` ranges from a `$sample` of its ids,
//! when its smallest and largest `_id` share one type bracket (so `$gte` and
//! `$lt` see every id between them); otherwise it stays one split. Every
//! split of a plan reads at the cluster time the enumerator recorded
//! (`readConcern: snapshot`, `atClusterTime`), so the splits are one
//! consistent snapshot. A range split's id carries a hash of the plan
//! (`<collection>/<i>of<n>@<hash>`), so a new plan after a restart never
//! resumes an old plan's positions; a single split's id is stable and
//! resumes (plan ruling R16.7).
//!
//! **Positions** are the run's cluster time (for event ids), the time the
//! split reads at, the last `_id` delivered and the rows so far. A resume
//! whose snapshot aged out of the server's history window continues after
//! the last `_id` at a new cluster time, keeping the run's ids. On a
//! standalone server there are no snapshot reads: the split reads with
//! `readConcern: local`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::codecs::ARROW_STREAM;
use loams_connector_sdk::envelope::ids;
use loams_connector_sdk::schema::SchemaTracker;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use ring::digest;
use serde_json::{Value, json};

use crate::bson::{Bson, Document, Timestamp};
use crate::client::Client;
use crate::config::MongoConfig;
use crate::doc;
use crate::extjson::{self, Mode};
use crate::scan::{Scan, snapshot_too_old};
use crate::types;

/// Room left in a batch's byte budget for the envelope.
const ENVELOPE_OVERHEAD: usize = 1024;

fn ts_text(t: Option<Timestamp>) -> Value {
    t.map_or(Value::Null, |t| Value::String(t.to_string()))
}

pub(crate) fn parse_ts(v: &Value) -> Result<Option<Timestamp>, ConnectorError> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) => {
            let (t, i) = s
                .split_once('.')
                .ok_or_else(|| ConnectorError::fatal(format!("{s:?} is not a cluster time")))?;
            let p = |x: &str| {
                x.parse::<u32>()
                    .map_err(|_| ConnectorError::fatal(format!("{s:?} is not a cluster time")))
            };
            Ok(Some(Timestamp::new(p(t)?, p(i)?)))
        }
        _ => Err(ConnectorError::fatal("a cluster time must be a string")),
    }
}

pub(crate) fn bson_from_canonical(v: &Value) -> Result<Option<Bson>, ConnectorError> {
    match v {
        Value::Null => Ok(None),
        other => extjson::from_json(other)
            .map(Some)
            .map_err(|e| ConnectorError::fatal(format!("a stored _id: {e}"))),
    }
}

/// A batch split's position.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchPos {
    /// The cluster time the run started at: the event ids' as-of.
    pub run: Option<Timestamp>,
    /// The cluster time the split reads at now.
    pub at: Option<Timestamp>,
    pub last: Option<Bson>,
    pub rows: u64,
    pub done: bool,
}

impl BatchPos {
    pub fn to_position(&self, collection: &str) -> Position {
        let v = json!({
            "run": ts_text(self.run),
            "at": ts_text(self.at),
            "last": self.last.as_ref().map(|b| extjson::to_json(b, Mode::Canonical)),
            "rows": self.rows,
            "done": self.done,
        });
        let at = self
            .at
            .map_or_else(|| "local".to_owned(), |t| t.to_string());
        Position::new(
            Bytes::from(v.to_string()),
            format!(
                "{collection}: {} rows at {at}{}",
                self.rows,
                if self.done { ", done" } else { "" }
            ),
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        let v: Value = serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("a batch position is not JSON: {e}")))?;
        Ok(Self {
            run: parse_ts(&v["run"])?,
            at: parse_ts(&v["at"])?,
            last: bson_from_canonical(&v["last"])?,
            rows: v["rows"].as_u64().unwrap_or(0),
            done: v["done"].as_bool().unwrap_or(false),
        })
    }
}

/// The collections to read: the configured ones, or every collection of
/// the database (views and `system.*` left out).
pub(crate) async fn collections(
    client: &mut Client,
    cfg: &MongoConfig,
) -> Result<Vec<String>, ConnectorError> {
    if !cfg.collections.is_empty() {
        return Ok(cfg.collections.clone());
    }
    let reply = client
        .command(
            &cfg.database,
            doc! {
                "listCollections" => 1,
                "filter" => doc! { "type" => "collection" },
                "nameOnly" => true,
                "authorizedCollections" => true,
            },
        )
        .await?;
    let batch = reply
        .get_document("cursor")
        .and_then(|c| c.get_array("firstBatch"))
        .unwrap_or_default();
    let mut names: Vec<String> = batch
        .iter()
        .filter_map(|d| d.as_document()?.get_str("name").map(str::to_owned))
        .filter(|n| !n.starts_with("system."))
        .collect();
    names.sort();
    Ok(names)
}

/// The current majority-committed cluster time, from a snapshot `find`.
pub(crate) async fn cluster_time(
    client: &mut Client,
    db: &str,
    collection: &str,
) -> Result<Timestamp, ConnectorError> {
    let reply = client
        .command(
            db,
            doc! {
                "find" => collection,
                "filter" => doc! {},
                "limit" => 1,
                "singleBatch" => true,
                "readConcern" => doc! { "level" => "snapshot" },
            },
        )
        .await?;
    reply
        .get_document("cursor")
        .and_then(|c| c.get("atClusterTime"))
        .or_else(|| reply.get("operationTime"))
        .and_then(Bson::as_timestamp)
        .ok_or_else(|| {
            ConnectorError::fatal("the server did not report the snapshot's cluster time")
        })
}

async fn extreme_id(
    client: &mut Client,
    db: &str,
    coll: &str,
    dir: i32,
    at: Option<Timestamp>,
) -> Result<Option<Bson>, ConnectorError> {
    let mut cmd = doc! {
        "find" => coll, "filter" => doc! {}, "sort" => doc! { "_id" => dir },
        "projection" => doc! { "_id" => 1 }, "limit" => 1, "singleBatch" => true,
    };
    if let Some(at) = at {
        cmd.insert(
            "readConcern",
            doc! { "level" => "snapshot", "atClusterTime" => at },
        );
    }
    let reply = client.command(db, cmd).await?;
    Ok(reply
        .get_document("cursor")
        .and_then(|c| c.get_array("firstBatch"))
        .and_then(|b| b.first())
        .and_then(Bson::as_document)
        .and_then(|d| d.get("_id"))
        .cloned())
}

/// Cut `ids` (a sample) into `parts` ranges: the boundaries between them,
/// in `_id` order, without duplicates.
pub fn boundaries(mut ids: Vec<Bson>, parts: usize) -> Vec<Bson> {
    ids.sort_by(|a, b| a.partial_order(b).unwrap_or(std::cmp::Ordering::Equal));
    ids.dedup();
    let mut out: Vec<Bson> = Vec::new();
    if parts < 2 || ids.len() < parts {
        return out;
    }
    for i in 1..parts {
        let b = ids[i * ids.len() / parts].clone();
        if out.last() != Some(&b) {
            out.push(b);
        }
    }
    out
}

/// The splits of one collection (see the module documentation).
async fn plan_collection(
    client: &mut Client,
    cfg: &MongoConfig,
    coll: &str,
    at: Option<Timestamp>,
) -> Result<Vec<Split>, ConnectorError> {
    let single = |spec: Value| vec![Split::new(coll.to_owned(), spec)];
    let base = json!({ "collection": coll, "at": ts_text(at), "lo": null, "hi": null, "part": 0 });
    let count = client
        .command(&cfg.database, doc! { "count" => coll })
        .await?
        .get_i64("n")
        .unwrap_or(0)
        .max(0) as u64;
    let parts = (count.div_ceil(cfg.split_rows) as usize).clamp(1, cfg.max_splits);
    if parts == 1 {
        return Ok(single(base));
    }
    let (Some(min), Some(max)) = (
        extreme_id(client, &cfg.database, coll, 1, at).await?,
        extreme_id(client, &cfg.database, coll, -1, at).await?,
    ) else {
        return Ok(single(base));
    };
    if min.bracket() != max.bracket() || matches!(min, Bson::Document(_) | Bson::Array(_)) {
        return Ok(single(base));
    }
    let mut cmd = doc! {
        "aggregate" => coll,
        "pipeline" => vec![
            Bson::from(doc! { "$sample" => doc! { "size" => (parts * 32) as i32 } }),
            Bson::from(doc! { "$project" => doc! { "_id" => 1 } }),
        ],
        "cursor" => doc! {},
    };
    if let Some(at) = at {
        cmd.insert(
            "readConcern",
            doc! { "level" => "snapshot", "atClusterTime" => at },
        );
    }
    let reply = match client.command(&cfg.database, cmd).await {
        Ok(r) => r,
        Err(e) if e.class.is_transient() => return Err(e),
        Err(e) => {
            // A server that refuses `$sample` here reads the collection as
            // one split rather than failing the instance.
            tracing::warn!(
                collection = coll,
                "no _id sample ({}); one split",
                e.message
            );
            return Ok(single(base));
        }
    };
    let sample: Vec<Bson> = reply
        .get_document("cursor")
        .and_then(|c| c.get_array("firstBatch"))
        .unwrap_or_default()
        .iter()
        .filter_map(|d| d.as_document()?.get("_id").cloned())
        .filter(|id| id.bracket() == min.bracket())
        .collect();
    let bounds = boundaries(sample, parts);
    if bounds.is_empty() {
        return Ok(single(base));
    }
    let mut edges: Vec<Option<Bson>> = vec![None];
    edges.extend(bounds.into_iter().map(Some));
    edges.push(None);
    let n = edges.len() - 1;
    let canon = |b: &Option<Bson>| {
        b.as_ref()
            .map_or(Value::Null, |b| extjson::to_json(b, Mode::Canonical))
    };
    let mut h = digest::Context::new(&digest::SHA256);
    h.update(ts_text(at).to_string().as_bytes());
    for e in &edges {
        h.update(canon(e).to_string().as_bytes());
        h.update(b"|");
    }
    let plan: String = crate::bson::hex(&h.finish().as_ref()[..4]);
    Ok((0..n)
        .map(|i| {
            // The outer splits stay open-ended but inside the bracket of
            // `min` (`$gte`/`$lt` never cross brackets).
            let lo = if i == 0 {
                Some(min.clone())
            } else {
                edges[i].clone()
            };
            let spec = json!({
                "collection": coll, "at": ts_text(at), "part": i,
                "lo": canon(&lo), "hi": canon(&edges[i + 1]),
            });
            Split::new(format!("{coll}/{i}of{n}@{plan}"), spec)
        })
        .collect())
}

/// Plans the splits once per run (they never change during it).
#[derive(Debug)]
pub struct BatchEnumerator {
    client: Client,
    plan: Option<Vec<Split>>,
}

impl BatchEnumerator {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext) -> Self {
        Self {
            client: Client::new(cfg, ctx),
            plan: None,
        }
    }
}

#[async_trait]
impl SplitEnumerator for BatchEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        if let Some(p) = &self.plan {
            return Ok(p.clone());
        }
        let cfg = self.client.config().clone();
        let snapshot = self.client.hello().await?.supports_change_streams();
        let colls = collections(&mut self.client, &cfg).await?;
        let at = match (snapshot, colls.first()) {
            (true, Some(c)) => Some(cluster_time(&mut self.client, &cfg.database, c).await?),
            _ => None,
        };
        let mut splits = Vec::new();
        for c in &colls {
            splits.extend(plan_collection(&mut self.client, &cfg, c, at).await?);
        }
        self.client.reset();
        self.plan = Some(splits.clone());
        Ok(splits)
    }
}

/// Makes one [`BatchSource`] per split.
#[derive(Debug)]
pub struct BatchFactory {
    cfg: MongoConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl BatchFactory {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for BatchFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(BatchSource {
            client: Client::new(self.cfg.clone(), self.ctx.clone()),
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            tracker: SchemaTracker::new(self.cfg.schema.evolution),
            ctx: self.ctx.clone(),
            scan: None,
            pos: None,
            part: 0,
            prefix: String::new(),
        }))
    }
}

/// Reads one split.
pub struct BatchSource {
    client: Client,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    tracker: SchemaTracker,
    scan: Option<Scan>,
    pos: Option<BatchPos>,
    part: u32,
    prefix: String,
}

impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("collection", &self.prefix)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

/// Build the largest prefix of `docs` that fits `max_bytes` as one batch;
/// `Err` with the oversized document's index 0 when a single one does not.
pub(crate) fn batch_within(
    schema: &arrow_schema::SchemaRef,
    docs: &[Document],
    max_rows: usize,
    max_bytes: usize,
) -> Result<(RecordBatch, usize), Result<usize, String>> {
    let mut n = 0usize;
    let mut cost = 0usize;
    for d in docs {
        let c = d.encoded_len().unwrap_or(0).max(1);
        if n >= max_rows || (n > 0 && cost + c > max_bytes / 2) {
            break;
        }
        cost += c;
        n += 1;
    }
    loop {
        let batch = types::to_batch(schema.clone(), &docs[..n]).map_err(Err)?;
        if batch.get_array_memory_size() <= max_bytes {
            return Ok((batch, n));
        }
        if n == 1 {
            return Err(Ok(batch.get_array_memory_size()));
        }
        n /= 2;
    }
}

#[async_trait]
impl Source for BatchSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let spec = &split.spec;
        let coll = spec["collection"]
            .as_str()
            .ok_or_else(|| ConnectorError::fatal("a batch split names no collection"))?
            .to_owned();
        let spec_at = parse_ts(&spec["at"])?;
        let resume = pos.as_ref().map(BatchPos::from_position).transpose()?;
        let mut scan = Scan::new(coll.clone(), spec_at.is_some());
        scan.lo = bson_from_canonical(&spec["lo"])?;
        scan.hi = bson_from_canonical(&spec["hi"])?;
        let p = resume.unwrap_or(BatchPos {
            run: spec_at,
            at: spec_at,
            last: None,
            rows: 0,
            done: false,
        });
        scan.at = p.at;
        scan.last = p.last.clone();
        self.part = spec["part"].as_u64().unwrap_or(0) as u32;
        self.prefix = format!("{}.{coll}", self.client.config().database);
        self.scan = Some(scan);
        self.pos = Some(p);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let (Some(scan), Some(pos)) = (self.scan.as_mut(), self.pos.as_mut()) else {
            return Err(ConnectorError::fatal("the batch source is not open"));
        };
        if pos.done {
            return Ok(None);
        }
        let db = self.client.config().database.clone();
        let max_bytes = budget.max_bytes.saturating_sub(ENVELOPE_OVERHEAD).max(1);
        if let Err(e) = scan
            .fill(&mut self.client, &db, budget.max_rows, max_bytes / 2)
            .await
        {
            if snapshot_too_old(&e) {
                // Continue after the last `_id` at a new cluster time.
                tracing::warn!(collection = %scan.collection, "the snapshot aged out; continuing at a new cluster time");
                scan.at = None;
                scan.reopen();
                return Err(ConnectorError::retryable(format!(
                    "{}: {}",
                    scan.collection, e.message
                )));
            }
            return Err(e);
        }
        pos.at = scan.at;
        if pos.run.is_none() {
            pos.run = scan.at;
        }
        if scan.pending.is_empty() {
            if scan.done() {
                pos.done = true;
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: pos.to_position(&scan.collection),
                }));
            }
            return Ok(None);
        }
        let docs: Vec<Document> = scan.pending.iter().take(budget.max_rows).cloned().collect();
        let current = self.tracker.current(&self.prefix);
        let schema = Arc::new(types::union_schema(current.as_deref(), &docs, &self.prefix));
        let here = pos.to_position(&scan.collection);
        if let Some(change) = self.tracker.observe(&self.prefix, schema.clone(), &here)? {
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(vec![change.event(&self.envelope)]),
                position: here,
            }));
        }
        let (batch, n) = match batch_within(&schema, &docs, budget.max_rows, max_bytes) {
            Ok(r) => r,
            Err(Ok(size)) => {
                let skipped = scan.take(1);
                let id = skipped
                    .first()
                    .and_then(|d| d.get("_id"))
                    .map(extjson::canonical_string)
                    .unwrap_or_default();
                return Err(ConnectorError::poison(format!(
                    "{}: document {id} is {size} bytes as Arrow, over the batch budget of {} bytes",
                    self.prefix, budget.max_bytes
                )));
            }
            Err(Err(e)) => return Err(ConnectorError::poison(format!("{}: {e}", self.prefix))),
        };
        scan.take(n);
        let first = pos.rows;
        pos.rows += n as u64;
        pos.last = scan.last.clone();
        pos.done = scan.done();
        let run = ids::run_id(
            &self.ctx.instance,
            &format!("{}#{}", self.prefix, self.part),
            &pos.run
                .map_or_else(|| "local".to_owned(), |t| t.to_string()),
        );
        let mut envelope = self
            .envelope
            .event(
                "document",
                &self.prefix,
                ids::rows(&run, self.part, first, pos.rows - 1),
            )
            .with_subject(scan.collection.clone());
        envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
        Ok(Some(SourceBatch {
            events: EventBatch::Arrow { batch, envelope },
            position: pos.to_position(&scan.collection),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // A read has nothing to acknowledge upstream.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let db = self.client.config().database.clone();
        if let Some(scan) = self.scan.as_mut() {
            scan.kill(&mut self.client, &db).await;
        }
        self.client.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson::ObjectId;

    #[test]
    fn positions_round_trip() {
        let p = BatchPos {
            run: Some(Timestamp::new(10, 1)),
            at: Some(Timestamp::new(12, 3)),
            last: Some(Bson::ObjectId(ObjectId([5; 12]))),
            rows: 42,
            done: false,
        };
        let pos = p.to_position("items");
        assert_eq!(pos.display, "items: 42 rows at 12.3");
        assert_eq!(BatchPos::from_position(&pos).expect("decode"), p);
        let local = BatchPos {
            run: None,
            at: None,
            last: Some(Bson::Int64(7)),
            rows: 1,
            done: true,
        };
        assert_eq!(
            BatchPos::from_position(&local.to_position("c")).expect("decode"),
            local
        );
    }

    #[test]
    fn boundaries_cut_a_sample_into_ranges() {
        let ids: Vec<Bson> = (0..100).rev().map(Bson::Int32).collect();
        assert_eq!(
            boundaries(ids.clone(), 4),
            vec![Bson::Int32(25), Bson::Int32(50), Bson::Int32(75)]
        );
        assert!(boundaries(ids, 1).is_empty());
        // Too small a sample: one range.
        assert!(boundaries(vec![Bson::Int32(1)], 4).is_empty());
        // Repeated values do not make empty ranges.
        let same = vec![Bson::Int32(1); 50];
        assert!(boundaries(same, 4).is_empty());
    }

    #[test]
    fn batches_respect_the_byte_budget() {
        let docs: Vec<Document> = (0..100)
            .map(|i| doc! { "_id" => i, "s" => "x".repeat(200) })
            .collect();
        let schema = Arc::new(types::union_schema(None, &docs, "d.c"));
        let (b, n) = batch_within(&schema, &docs, 1000, 8 * 1024).expect("fits");
        assert!(n < 100 && n > 0);
        assert!(b.get_array_memory_size() <= 8 * 1024);
        let (_, n) = batch_within(&schema, &docs, 10, 1 << 20).expect("rows");
        assert_eq!(n, 10);
        let huge = vec![doc! { "_id" => 1, "s" => "x".repeat(100_000) }];
        let schema = Arc::new(types::union_schema(None, &huge, "d.c"));
        assert!(matches!(batch_within(&schema, &huge, 10, 1024), Err(Ok(_))));
    }
}
