//! Amazon Kinesis Data Streams (`kinesis` ★; CN1 Task 13's semantics, built
//! on the SDK in this family per CN2 Task 2's table and Task 8).
//!
//! **Source:** each shard is a split. A shard is read only once every parent
//! (`ParentShardId`, `AdjacentParentShardId`) is finished or has expired, so
//! after a split or merge the parent's records all come before the
//! children's. The position is the last sequence number read and whether
//! the shard is finished; a restart resumes with `AFTER_SEQUENCE_NUMBER`. An
//! expired iterator is renewed from the position. A shard that disappears
//! before it was finished is `PositionLost` (its records past the position
//! were trimmed). Event ids are `<stream>/<shard>/<sequence number>`, stable
//! across re-reads; a record that is a structured CloudEvent passes through,
//! any other is `record`.
//!
//! **Sink:** `PutRecords`, up to 500 records or 5 MiB a call, each record the
//! event in the structured CloudEvents form with its subject (else its id) as
//! partition key; only the records that failed are retried.
//!
//! Config: `stream` (or `stream_arn`), `region`, `endpoint`, `auth.*`,
//! `start` (`trim_horizon`, the default, or `latest`) and `poll_interval_ms`
//! (default 1000, which keeps a shard under Kinesis' five reads a second).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder,
    ErrorClass, EventBatch, Position, SinkFactory, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::aws::{AwsClient, AwsService, classify};
use crate::ce;
use crate::config::{str_opt, u64_in};
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};

pub const ID: &str = "kinesis";
/// The event a record becomes (CN1 Ruling 2).
pub const RECORD_EVENT: &str = "record";
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// Which stream: by name or by ARN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRef {
    key: &'static str,
    value: String,
    name: String,
}

impl StreamRef {
    pub fn from_config(c: &Value) -> Result<Self, ConnectorError> {
        if let Some(arn) = str_opt(c, "stream_arn") {
            let name = arn.rsplit('/').next().unwrap_or(&arn).to_owned();
            return Ok(Self {
                key: "StreamARN",
                value: arn,
                name,
            });
        }
        let name = str_opt(c, "stream").ok_or_else(|| {
            ConnectorError::fatal("config `stream` (or `stream_arn`) is required")
        })?;
        Ok(Self {
            key: "StreamName",
            value: name.clone(),
            name,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn with(&self, mut v: Value) -> Value {
        v[self.key] = json!(self.value);
        v
    }
}

/// A shard's position: the last sequence number read, and whether the shard
/// is finished (closed and read to its end).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPosition {
    pub seq: Option<String>,
    pub finished: bool,
}

impl ShardPosition {
    pub fn to_position(&self, shard: &str) -> Position {
        let display = match (&self.seq, self.finished) {
            (_, true) => format!("{shard} finished"),
            (Some(s), false) => format!("{shard}@{s}"),
            (None, false) => format!("{shard} start"),
        };
        Position::new(serde_json::to_vec(self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Option<Self> {
        serde_json::from_slice(&p.opaque).ok()
    }
}

#[derive(Debug, Clone)]
struct ShardInfo {
    id: String,
    parents: Vec<String>,
}

async fn list_shards(
    client: &AwsClient,
    stream: &StreamRef,
) -> Result<Vec<ShardInfo>, ConnectorError> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let req = match &token {
            Some(t) => json!({"NextToken": t}),
            None => stream.with(json!({})),
        };
        let resp = client.call("ListShards", &req).await?;
        for s in resp["Shards"].as_array().into_iter().flatten() {
            let Some(id) = s["ShardId"].as_str() else {
                continue;
            };
            let parents = ["ParentShardId", "AdjacentParentShardId"]
                .iter()
                .filter_map(|k| s[*k].as_str().map(str::to_owned))
                .collect();
            out.push(ShardInfo {
                id: id.to_owned(),
                parents,
            });
        }
        match resp["NextToken"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    Ok(out)
}

/// The shards that may be read now: those whose parents are finished or no
/// longer listed.
fn ready_shards(listed: &[ShardInfo], known: &[SplitState]) -> Vec<String> {
    let finished: BTreeSet<&str> = known
        .iter()
        .filter(|k| {
            k.position
                .as_ref()
                .and_then(ShardPosition::from_position)
                .is_some_and(|p| p.finished)
        })
        .map(|k| k.split.id.as_str())
        .collect();
    let ids: BTreeSet<&str> = listed.iter().map(|s| s.id.as_str()).collect();
    listed
        .iter()
        .filter(|s| {
            s.parents
                .iter()
                .all(|p| !ids.contains(p.as_str()) || finished.contains(p.as_str()))
        })
        .map(|s| s.id.clone())
        .collect()
}

/// Discovers shards, parents first.
#[derive(Debug)]
pub struct ShardEnumerator {
    client: AwsClient,
    stream: StreamRef,
}

#[async_trait]
impl SplitEnumerator for ShardEnumerator {
    async fn discover(&mut self, known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        let listed = list_shards(&self.client, &self.stream).await?;
        Ok(ready_shards(&listed, known)
            .into_iter()
            .map(|id| Split::new(id.clone(), json!({"shard": id})))
            .collect())
    }
}

/// Makes a [`ShardSource`] per shard.
#[derive(Debug, Clone)]
pub struct ShardSourceFactory {
    client: AwsClient,
    stream: StreamRef,
    latest: bool,
    poll: Duration,
    envelope: EnvelopeBuilder,
}

impl SourceFactory for ShardSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(ShardSource {
            f: self.clone(),
            shard: split.id.as_str().to_owned(),
            pos: ShardPosition::default(),
            iterator: None,
            end_of_shard: false,
            buffer: VecDeque::new(),
            last_empty: None,
        }))
    }
}

/// One shard.
#[derive(Debug)]
pub struct ShardSource {
    f: ShardSourceFactory,
    shard: String,
    pos: ShardPosition,
    iterator: Option<String>,
    end_of_shard: bool,
    /// Records of the last `GetRecords` that did not fit the budget.
    buffer: VecDeque<Value>,
    last_empty: Option<Instant>,
}

impl ShardSource {
    async fn iterator(&mut self) -> Result<String, ConnectorError> {
        if let Some(it) = &self.iterator {
            return Ok(it.clone());
        }
        let mut req = self.f.stream.with(json!({"ShardId": self.shard}));
        match &self.pos.seq {
            Some(seq) => {
                req["ShardIteratorType"] = json!("AFTER_SEQUENCE_NUMBER");
                req["StartingSequenceNumber"] = json!(seq);
            }
            None if self.f.latest => req["ShardIteratorType"] = json!("LATEST"),
            None => req["ShardIteratorType"] = json!("TRIM_HORIZON"),
        }
        let resp = self.f.client.call("GetShardIterator", &req).await.map_err(|e| {
            if e.message.contains("ResourceNotFound") && self.pos.seq.is_some() {
                ConnectorError::position_lost(format!(
                    "shard {} of {} is gone before it was read past {}; re-snapshot from TRIM_HORIZON",
                    self.shard,
                    self.f.stream.name(),
                    self.pos.seq.as_deref().unwrap_or_default()
                ))
            } else {
                e
            }
        })?;
        let it = resp["ShardIterator"]
            .as_str()
            .ok_or_else(|| ConnectorError::retryable("GetShardIterator returned no iterator"))?
            .to_owned();
        self.iterator = Some(it.clone());
        Ok(it)
    }

    async fn fetch(&mut self, limit: usize) -> Result<(), ConnectorError> {
        for attempt in 0..2 {
            let it = self.iterator().await?;
            match self
                .f
                .client
                .call(
                    "GetRecords",
                    &json!({"ShardIterator": it, "Limit": limit.clamp(1, 10_000)}),
                )
                .await
            {
                Ok(resp) => {
                    self.buffer
                        .extend(resp["Records"].as_array().cloned().unwrap_or_default());
                    match resp["NextShardIterator"].as_str() {
                        Some(next) => self.iterator = Some(next.to_owned()),
                        None => {
                            self.iterator = None;
                            self.end_of_shard = true;
                        }
                    }
                    return Ok(());
                }
                Err(e) if attempt == 0 && e.message.contains("ExpiredIterator") => {
                    // Renew from the position (iterators last five minutes).
                    self.iterator = None;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn event(&self, r: &Value) -> Result<(String, CloudEvent), ConnectorError> {
        let seq = r["SequenceNumber"]
            .as_str()
            .ok_or_else(|| ConnectorError::retryable("a record without a sequence number"))?
            .to_owned();
        let data = B64
            .decode(r["Data"].as_str().unwrap_or_default())
            .map_err(|_| ConnectorError::retryable("a record whose data is not base64"))?;
        let data = Bytes::from(data);
        let stream = self.f.stream.name();
        if ce::looks_structured(&data)
            && let Some(ev) = self.f.envelope.from_structured_json(&data)
        {
            return Ok((seq, ev));
        }
        let mut ev = self
            .f
            .envelope
            .event(
                RECORD_EVENT,
                stream,
                format!("{stream}/{}/{seq}", self.shard),
            )
            .with_data(ce::sniff_content_type(&data), data);
        if let Some(pk) = r["PartitionKey"].as_str() {
            ev = ev.with_subject(pk.to_owned());
        }
        if let Some(t) = r["ApproximateArrivalTimestamp"].as_f64() {
            ev = ev.with_time(ce::rfc3339_millis((t * 1000.0).round() as i64));
        }
        Ok((seq, ev))
    }
}

#[async_trait]
impl Source for ShardSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.pos = pos
            .as_ref()
            .and_then(ShardPosition::from_position)
            .unwrap_or_default();
        self.iterator = None;
        self.end_of_shard = self.pos.finished;
        self.buffer.clear();
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.pos.finished {
            return Ok(None);
        }
        if self.buffer.is_empty() && !self.end_of_shard {
            if let Some(t) = self.last_empty {
                let wait = self.f.poll.saturating_sub(t.elapsed());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
            self.fetch(budget.max_rows).await?;
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut last = None;
        while let Some(r) = self.buffer.front() {
            let (seq, ev) = self.event(r)?;
            let size = ev.size_bytes();
            if events.len() + 1 > budget.max_rows || bytes + size > budget.max_bytes {
                if events.is_empty() {
                    // Larger than a whole batch (R4.7): skipped, reported.
                    self.buffer.pop_front();
                    self.pos.seq = Some(seq.clone());
                    return Err(ConnectorError::poison(format!(
                        "record {seq} of {} is larger than the batch budget",
                        self.shard
                    )));
                }
                break;
            }
            self.buffer.pop_front();
            bytes += size;
            events.push(ev);
            last = Some(seq);
        }
        if let Some(seq) = last {
            self.pos.seq = Some(seq);
        }
        let finishing = self.end_of_shard && self.buffer.is_empty();
        if events.is_empty() && !finishing {
            self.last_empty = Some(Instant::now());
            return Ok(None);
        }
        self.last_empty = None;
        if finishing {
            self.pos.finished = true;
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.to_position(&self.shard),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // Kinesis keeps no consumer state: the checkpoint is the position.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// A partition key: the subject, else the id; Kinesis takes at most 256
/// characters, so a longer one is hashed.
pub fn partition_key(ev: &CloudEvent) -> String {
    let k = ev.subject.clone().unwrap_or_else(|| ev.id.clone());
    if !k.is_empty() && k.chars().count() <= 256 {
        k
    } else {
        Sha256::digest(k.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

/// The sink's `PutRecords` call.
#[derive(Debug, Clone)]
pub struct KinesisPut {
    client: AwsClient,
    stream: StreamRef,
}

#[async_trait]
impl BatchPut for KinesisPut {
    fn max_entries(&self) -> usize {
        500
    }

    fn max_bytes(&self) -> usize {
        5 * 1024 * 1024
    }

    fn entry_size(&self, ev: &CloudEvent) -> usize {
        ce::to_structured_bytes(ev).len() + partition_key(ev).len()
    }

    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError> {
        let records: Vec<Value> = events
            .iter()
            .map(|e| json!({"Data": B64.encode(ce::to_structured_bytes(e)), "PartitionKey": partition_key(e)}))
            .collect();
        let out = self
            .client
            .call("PutRecords", &self.stream.with(json!({"Records": records})))
            .await?;
        let results = out["Records"].as_array().cloned().unwrap_or_default();
        if results.len() != events.len() {
            return Err(ConnectorError::retryable(format!(
                "PutRecords answered {} records for {}",
                results.len(),
                events.len()
            )));
        }
        Ok(results
            .iter()
            .map(|r| match r["ErrorCode"].as_str() {
                None => EntryOutcome::Delivered,
                Some(code) => match classify(400, code) {
                    ErrorClass::Throttled { .. } | ErrorClass::Retryable { .. } => {
                        EntryOutcome::Retry(code.to_owned())
                    }
                    _ if code == "InternalFailure" => EntryOutcome::Retry(code.to_owned()),
                    _ => EntryOutcome::Refused(format!(
                        "{code}: {}",
                        r["ErrorMessage"].as_str().unwrap_or_default()
                    )),
                },
            })
            .collect())
    }
}

/// The `kinesis` factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct KinesisFactory;

impl KinesisFactory {
    pub fn new() -> Self {
        Self
    }
}

impl ConnectorFactory for KinesisFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let c = &ctx.config;
        let stream = StreamRef::from_config(c)?;
        let client = AwsClient::from_context(ctx, AwsService::Kinesis)?;
        let latest = match str_opt(c, "start").as_deref() {
            None | Some("trim_horizon") => false,
            Some("latest") => true,
            Some(other) => {
                return Err(ConnectorError::fatal(format!(
                    "config `start` must be `trim_horizon` or `latest`, not `{other}`"
                )));
            }
        };
        let poll = Duration::from_millis(u64_in(c, "poll_interval_ms", 1000, 0..=60_000)?);
        Ok(Some((
            Box::new(ShardEnumerator {
                client: client.clone(),
                stream: stream.clone(),
            }),
            Box::new(ShardSourceFactory {
                client,
                stream,
                latest,
                poll,
                envelope: EnvelopeBuilder::new(ID, &ctx.instance),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(BatchSinkFactory(KinesisPut {
            client: AwsClient::from_context(ctx, AwsService::Kinesis)?,
            stream: StreamRef::from_config(&ctx.config)?,
        }))))
    }
}

/// Which listed shards are finished according to `known`, for logs.
pub fn finished_shards(known: &[SplitState]) -> BTreeMap<String, bool> {
    known
        .iter()
        .map(|k| {
            (
                k.split.id.as_str().to_owned(),
                k.position
                    .as_ref()
                    .and_then(ShardPosition::from_position)
                    .is_some_and(|p| p.finished),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: &str, parents: &[&str]) -> ShardInfo {
        ShardInfo {
            id: id.into(),
            parents: parents.iter().map(|p| (*p).to_owned()).collect(),
        }
    }

    fn state(id: &str, finished: bool) -> SplitState {
        SplitState {
            split: Split::new(id, Value::Null),
            position: Some(
                ShardPosition {
                    seq: Some("1".into()),
                    finished,
                }
                .to_position(id),
            ),
        }
    }

    #[test]
    fn lineage() {
        let listed = vec![
            shard("a", &[]),
            shard("b", &["a"]),
            shard("c", &["a"]),
            shard("d", &["b", "c"]),
        ];
        assert_eq!(ready_shards(&listed, &[]), vec!["a"]);
        assert_eq!(ready_shards(&listed, &[state("a", false)]), vec!["a"]);
        assert_eq!(
            ready_shards(&listed, &[state("a", true)]),
            vec!["a", "b", "c"]
        );
        assert_eq!(
            ready_shards(
                &listed,
                &[state("a", true), state("b", true), state("c", false)]
            ),
            vec!["a", "b", "c"]
        );
        assert_eq!(
            ready_shards(
                &listed,
                &[state("a", true), state("b", true), state("c", true)]
            ),
            vec!["a", "b", "c", "d"]
        );
        // An expired parent no longer listed does not block its children.
        assert_eq!(ready_shards(&listed[1..2], &[]), vec!["b"]);
        assert!(finished_shards(&[state("a", true)])["a"]);
    }

    #[test]
    fn positions_and_keys() {
        let p = ShardPosition {
            seq: Some("49".into()),
            finished: false,
        };
        let pos = p.to_position("shardId-1");
        assert_eq!(pos.display, "shardId-1@49");
        assert_eq!(ShardPosition::from_position(&pos), Some(p));
        let ev = CloudEvent::new("id", "/s", "t");
        assert_eq!(partition_key(&ev), "id");
        assert_eq!(partition_key(&ev.with_subject("x".repeat(300))).len(), 64);
    }
}
