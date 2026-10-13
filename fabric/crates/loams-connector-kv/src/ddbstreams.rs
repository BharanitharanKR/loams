//! DynamoDB Streams: native CDC (OR-9).
//!
//! **Splits are shards, with lineage.** `DescribeStream` lists the shards
//! with their parents. A shard is released to the pipeline only once its
//! parent has been read to its end (or the parent is no longer listed: it
//! was trimmed), so every key's changes arrive in order across a shard
//! split. A finished shard stays a split while the stream still lists it,
//! so its position (and its children's release) survives a restart.
//!
//! **Positions are sequence numbers.** A shard is read from `TRIM_HORIZON`
//! (or `LATEST` for a root shard when `start: latest`), and after a restart
//! from `AFTER_SEQUENCE_NUMBER` the checkpointed sequence number, so nothing
//! the Fabric acknowledged is read again. An iterator that expires (15 min)
//! is re-opened at the same place. A checkpoint whose records were trimmed
//! (past the 24-hour retention) is `PositionLost`: the instance stops with a
//! re-snapshot hint.
//!
//! **Events.** Each record is a `change` event with `loamsop` from
//! `eventName` (`INSERT` → `c`, `MODIFY` → `u`, `REMOVE` → `d`), `loamslsn`
//! the sequence number, `subject` the item's key, and `id`
//! `<shard>/<sequence-number>` (stable across re-reads). `data` holds
//! `keys`, `new` and `old` images as the stream view type provides them.
//! Lag is the age of the newest record read; position risk is the age of
//! the checkpoint against the 24-hour retention.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::{AttributeValue as SAv, ShardIteratorType};
use loams_connector_sdk::{
    Budget, CdcOp, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder,
    EventBatch, Position, SinkFactory, Source, SourceBatch, SourceFactory, Split, SplitEnumerator,
    SplitId, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::aws::{self, AwsSettings, LoamsHttpClient};
use crate::config::Config;
use crate::dynamodb::{Item, ItemFormat, item_to_json};
use crate::util::{now_millis, rfc3339_millis};

/// The manifest id.
pub const ID: &str = "dynamodb-streams";
/// The event name of a change.
pub const EVENT: &str = "change";
/// DynamoDB Streams keeps records for 24 hours.
pub const RETENTION: Duration = Duration::from_secs(24 * 3600);

/// A shard as `DescribeStream` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub id: String,
    pub parent: Option<String>,
    /// The shard is closed (it has an ending sequence number).
    pub closed: bool,
}

/// Where to open a shard iterator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IteratorStart {
    TrimHorizon,
    Latest,
    After(String),
}

/// One stream record.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamRecord {
    pub event_name: String,
    pub sequence: String,
    pub keys: Item,
    pub new_image: Option<Item>,
    pub old_image: Option<Item>,
    /// `ApproximateCreationDateTime`, ms since the epoch.
    pub created_ms: Option<i64>,
}

/// A `GetRecords` page: records and the next iterator (`None` once a closed
/// shard has been read to its end).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordsPage {
    pub records: Vec<StreamRecord>,
    pub next: Option<String>,
}

/// Why `GetRecords` failed.
#[derive(Debug)]
pub enum RecordsError {
    /// The iterator is older than 15 minutes: open a new one.
    Expired,
    Other(ConnectorError),
}

impl From<ConnectorError> for RecordsError {
    fn from(e: ConnectorError) -> Self {
        RecordsError::Other(e)
    }
}

/// The DynamoDB Streams calls the connector makes.
#[async_trait]
pub trait StreamsApi: Send + Sync + std::fmt::Debug {
    /// The stream of `table` (`DescribeTable`'s `LatestStreamArn`).
    async fn stream_of(&self, table: &str) -> Result<String, ConnectorError>;
    async fn shards(&self, stream: &str) -> Result<Vec<ShardInfo>, ConnectorError>;
    async fn iterator(
        &self,
        stream: &str,
        shard: &str,
        at: &IteratorStart,
    ) -> Result<String, ConnectorError>;
    async fn records(&self, iterator: &str, limit: usize) -> Result<RecordsPage, RecordsError>;
}

// ----------------------------------------------------------------- AWS --

/// The real clients (DynamoDB for the table's stream ARN, DynamoDB Streams
/// for the rest).
#[derive(Debug, Clone)]
pub struct AwsStreams {
    streams: aws_sdk_dynamodbstreams::Client,
    tables: aws_sdk_dynamodb::Client,
}

impl AwsStreams {
    pub async fn connect(ctx: &ConnectorContext, cfg: &Config) -> Result<Self, ConnectorError> {
        let s = AwsSettings::from_config(cfg)?;
        let creds = aws::credentials(ctx, cfg).await?;
        let mut b = aws_sdk_dynamodbstreams::Config::builder()
            .behavior_version(aws_sdk_dynamodbstreams::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodbstreams::config::Region::new(
                s.region.clone(),
            ))
            .credentials_provider(creds)
            .http_client(LoamsHttpClient::new(ctx.egress.clone()))
            .retry_config(aws_sdk_dynamodbstreams::config::retry::RetryConfig::disabled());
        b.set_endpoint_url(s.endpoint_url);
        let conf = b.build();
        let tables = crate::dynamodb::AwsDynamo::connect(ctx, cfg)
            .await?
            .client()
            .clone();
        Ok(Self {
            streams: aws_sdk_dynamodbstreams::Client::from_conf(conf),
            tables,
        })
    }
}

/// A Streams attribute value as a DynamoDB one (the two crates generate the
/// same shape twice).
fn convert_value(v: &SAv) -> aws_sdk_dynamodb::types::AttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue as D;
    match v {
        SAv::S(s) => D::S(s.clone()),
        SAv::N(n) => D::N(n.clone()),
        SAv::B(b) => D::B(b.clone()),
        SAv::Bool(b) => D::Bool(*b),
        SAv::Null(n) => D::Null(*n),
        SAv::L(l) => D::L(l.iter().map(convert_value).collect()),
        SAv::M(m) => D::M(convert_item(m)),
        SAv::Ss(s) => D::Ss(s.clone()),
        SAv::Ns(s) => D::Ns(s.clone()),
        SAv::Bs(s) => D::Bs(s.clone()),
        _ => D::Null(true),
    }
}

fn convert_item(m: &std::collections::HashMap<String, SAv>) -> Item {
    m.iter()
        .map(|(k, v)| (k.clone(), convert_value(v)))
        .collect()
}

#[async_trait]
impl StreamsApi for AwsStreams {
    async fn stream_of(&self, table: &str) -> Result<String, ConnectorError> {
        let out = self
            .tables
            .describe_table()
            .table_name(table)
            .send()
            .await
            .map_err(|e| aws::classify(&e, "DescribeTable"))?;
        out.table()
            .and_then(|t| t.latest_stream_arn())
            .map(str::to_owned)
            .ok_or_else(|| ConnectorError::fatal(format!("table {table} has no stream enabled")))
    }

    async fn shards(&self, stream: &str) -> Result<Vec<ShardInfo>, ConnectorError> {
        let mut out = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let page = self
                .streams
                .describe_stream()
                .stream_arn(stream)
                .set_exclusive_start_shard_id(start.clone())
                .send()
                .await
                .map_err(|e| aws::classify(&e, "DescribeStream"))?;
            let Some(desc) = page.stream_description() else {
                break;
            };
            for s in desc.shards() {
                let Some(id) = s.shard_id() else { continue };
                out.push(ShardInfo {
                    id: id.to_owned(),
                    parent: s.parent_shard_id().map(str::to_owned),
                    closed: s
                        .sequence_number_range()
                        .and_then(|r| r.ending_sequence_number())
                        .is_some(),
                });
            }
            match desc.last_evaluated_shard_id() {
                Some(next) if Some(next) != start.as_deref() => start = Some(next.to_owned()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn iterator(
        &self,
        stream: &str,
        shard: &str,
        at: &IteratorStart,
    ) -> Result<String, ConnectorError> {
        let mut req = self
            .streams
            .get_shard_iterator()
            .stream_arn(stream)
            .shard_id(shard);
        req = match at {
            IteratorStart::TrimHorizon => req.shard_iterator_type(ShardIteratorType::TrimHorizon),
            IteratorStart::Latest => req.shard_iterator_type(ShardIteratorType::Latest),
            IteratorStart::After(seq) => req
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .sequence_number(seq),
        };
        let out = req
            .send()
            .await
            .map_err(|e| aws::classify(&e, "GetShardIterator"))?;
        out.shard_iterator()
            .map(str::to_owned)
            .ok_or_else(|| ConnectorError::retryable("GetShardIterator returned no iterator"))
    }

    async fn records(&self, iterator: &str, limit: usize) -> Result<RecordsPage, RecordsError> {
        let out = self
            .streams
            .get_records()
            .shard_iterator(iterator)
            .limit(i32::try_from(limit.clamp(1, 1000)).unwrap_or(1000))
            .send()
            .await
            .map_err(|e| match aws::code_of(&e) {
                Some("ExpiredIteratorException") => RecordsError::Expired,
                _ => RecordsError::Other(aws::classify(&e, "GetRecords")),
            })?;
        let records = out
            .records()
            .iter()
            .filter_map(|r| {
                let d = r.dynamodb()?;
                Some(StreamRecord {
                    event_name: r
                        .event_name()
                        .map(|n| n.as_str().to_owned())
                        .unwrap_or_default(),
                    sequence: d.sequence_number()?.to_owned(),
                    keys: d.keys().map(convert_item).unwrap_or_default(),
                    new_image: d.new_image().map(convert_item),
                    old_image: d.old_image().map(convert_item),
                    created_ms: d
                        .approximate_creation_date_time()
                        .map(|t| t.secs() * 1000 + i64::from(t.subsec_nanos() / 1_000_000)),
                })
            })
            .collect();
        Ok(RecordsPage {
            records,
            next: out.next_shard_iterator().map(str::to_owned),
        })
    }
}

/// Builds the API once, on first use, and shares it between splits.
#[derive(Debug, Clone)]
pub struct StreamsCell {
    ctx: ConnectorContext,
    cfg: Config,
    cell: Arc<OnceCell<Arc<dyn StreamsApi>>>,
}

impl StreamsCell {
    pub fn aws(ctx: ConnectorContext, cfg: Config) -> Self {
        Self {
            ctx,
            cfg,
            cell: Arc::new(OnceCell::new()),
        }
    }

    pub fn fixed(ctx: ConnectorContext, cfg: Config, api: Arc<dyn StreamsApi>) -> Self {
        Self {
            ctx,
            cfg,
            cell: Arc::new(OnceCell::new_with(Some(api))),
        }
    }

    pub async fn get(&self) -> Result<Arc<dyn StreamsApi>, ConnectorError> {
        self.cell
            .get_or_try_init(|| async {
                AwsStreams::connect(&self.ctx, &self.cfg)
                    .await
                    .map(|c| Arc::new(c) as Arc<dyn StreamsApi>)
            })
            .await
            .cloned()
    }
}

// --------------------------------------------------------------- lineage --

/// A shard's position (its opaque bytes are this, as JSON).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPos {
    /// The last sequence number the Fabric has.
    pub seq: Option<String>,
    /// The shard was read to its end.
    pub ended: bool,
    /// When the last record was written, ms since the epoch (for position
    /// risk).
    pub at_ms: Option<i64>,
}

impl ShardPos {
    pub fn to_position(&self, shard: &str) -> Position {
        let display = match (&self.seq, self.ended) {
            (_, true) => format!("{shard} ended"),
            (Some(s), false) => format!("{shard} after {s}"),
            (None, false) => format!("{shard} at the start"),
        };
        Position::new(serde_json::to_vec(self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Option<ShardPos> {
        serde_json::from_slice(&p.opaque).ok()
    }
}

/// Lineage: the shards to read now. A shard is released when it has no
/// parent, when its parent is no longer listed (trimmed), or when its parent
/// has been read to its end (`ended` in its checkpointed position).
pub fn release(shards: &[ShardInfo], positions: &BTreeMap<String, ShardPos>) -> Vec<ShardInfo> {
    let listed: BTreeSet<&str> = shards.iter().map(|s| s.id.as_str()).collect();
    shards
        .iter()
        .filter(|s| match &s.parent {
            None => true,
            Some(p) if !listed.contains(p.as_str()) => true,
            Some(p) => positions.get(p).is_some_and(|pos| pos.ended),
        })
        .cloned()
        .collect()
}

/// `loamsop` from a record's `eventName`.
pub fn op_of(event_name: &str) -> Option<CdcOp> {
    match event_name {
        "INSERT" => Some(CdcOp::Create),
        "MODIFY" => Some(CdcOp::Update),
        "REMOVE" => Some(CdcOp::Delete),
        _ => None,
    }
}

/// The event a record becomes.
pub fn record_event(
    envelope: &EnvelopeBuilder,
    table: &str,
    shard: &str,
    r: &StreamRecord,
) -> CloudEvent {
    let mut data = serde_json::Map::new();
    data.insert("keys".into(), item_to_json(&r.keys, ItemFormat::Json));
    if let Some(n) = &r.new_image {
        data.insert("new".into(), item_to_json(n, ItemFormat::Json));
    }
    if let Some(o) = &r.old_image {
        data.insert("old".into(), item_to_json(o, ItemFormat::Json));
    }
    let subject = item_to_json(&r.keys, ItemFormat::Json).to_string();
    let mut ev = envelope
        .event(EVENT, table, format!("{shard}/{}", r.sequence))
        .with_subject(subject)
        .with_data(
            "application/json",
            serde_json::to_vec(&Value::Object(data)).unwrap_or_default(),
        );
    if let Some(op) = op_of(&r.event_name) {
        ev = ev.with_cdc(op, r.sequence.clone());
    }
    if let Some(ms) = r.created_ms {
        ev = ev.with_time(rfc3339_millis(ms));
    }
    ev
}

/// Settings: `stream_arn` or `table`, `start` (`trim_horizon` or `latest`),
/// `poll_interval_ms` (the wait after an empty read, default 1 s).
#[derive(Debug, Clone)]
pub struct StreamSettings {
    pub stream_arn: Option<String>,
    pub table: Option<String>,
    pub latest: bool,
    pub poll_interval: Duration,
}

impl StreamSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        let stream_arn = cfg.str("stream_arn")?;
        let table = cfg.str("table")?;
        if stream_arn.is_none() && table.is_none() {
            return Err(ConnectorError::fatal(
                "config: `stream_arn` or `table` is required",
            ));
        }
        let latest = match cfg.str("start")?.as_deref() {
            None | Some("trim_horizon") => false,
            Some("latest") => true,
            Some(_) => {
                return Err(ConnectorError::fatal(
                    "config: `start` must be trim_horizon or latest",
                ));
            }
        };
        Ok(Self {
            stream_arn,
            table,
            latest,
            poll_interval: cfg
                .millis("poll_interval_ms")?
                .unwrap_or(Duration::from_secs(1)),
        })
    }

    async fn arn(&self, api: &dyn StreamsApi) -> Result<String, ConnectorError> {
        match (&self.stream_arn, &self.table) {
            (Some(a), _) => Ok(a.clone()),
            (None, Some(t)) => api.stream_of(t).await,
            (None, None) => Err(ConnectorError::fatal(
                "config: `stream_arn` or `table` is required",
            )),
        }
    }

    /// The resource name events carry: the table, or the stream's table
    /// segment of its ARN.
    fn resource(&self) -> String {
        if let Some(t) = &self.table {
            return t.clone();
        }
        self.stream_arn
            .as_deref()
            .and_then(|a| a.split(":table/").nth(1))
            .and_then(|r| r.split('/').next())
            .unwrap_or("stream")
            .to_owned()
    }
}

/// Lists shards and releases them in lineage order.
#[derive(Debug)]
pub struct ShardEnumerator {
    api: StreamsCell,
    settings: StreamSettings,
}

#[async_trait]
impl SplitEnumerator for ShardEnumerator {
    async fn discover(&mut self, known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        let api = self.api.get().await?;
        let arn = self.settings.arn(&*api).await?;
        let shards = api.shards(&arn).await?;
        let positions: BTreeMap<String, ShardPos> = known
            .iter()
            .filter_map(|s| {
                let p = s.position.as_ref().and_then(ShardPos::from_position)?;
                Some((s.split.id.as_str().to_owned(), p))
            })
            .collect();
        Ok(release(&shards, &positions)
            .into_iter()
            .map(|s| {
                Split::new(
                    s.id.as_str(),
                    json!({ "stream": arn, "parent": s.parent, "root": s.parent.is_none() }),
                )
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct ShardSourceFactory {
    api: StreamsCell,
    envelope: EnvelopeBuilder,
    settings: StreamSettings,
    ctx: ConnectorContext,
}

impl SourceFactory for ShardSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(ShardSource {
            api: self.api.clone(),
            envelope: self.envelope.clone(),
            resource: self.settings.resource(),
            latest: self.settings.latest && split.spec["root"].as_bool().unwrap_or(false),
            poll: self.settings.poll_interval,
            ctx: self.ctx.clone(),
            stream: split.spec["stream"].as_str().unwrap_or_default().to_owned(),
            shard: split.id.clone(),
            pos: ShardPos::default(),
            iterator: None,
            carry: VecDeque::new(),
            carry_end: false,
            idle_until: None,
        }))
    }
}

#[derive(Debug)]
struct ShardSource {
    api: StreamsCell,
    envelope: EnvelopeBuilder,
    resource: String,
    latest: bool,
    poll: Duration,
    ctx: ConnectorContext,
    stream: String,
    shard: SplitId,
    pos: ShardPos,
    iterator: Option<String>,
    /// Records read but not yet returned (a page larger than the budget).
    carry: VecDeque<StreamRecord>,
    /// The page that filled `carry` ended the shard.
    carry_end: bool,
    idle_until: Option<tokio::time::Instant>,
}

impl ShardSource {
    fn start(&self) -> IteratorStart {
        match &self.pos.seq {
            Some(s) => IteratorStart::After(s.clone()),
            None if self.latest => IteratorStart::Latest,
            None => IteratorStart::TrimHorizon,
        }
    }

    fn report_risk(&self) {
        if let Some(at) = self.pos.at_ms {
            let age = now_millis().saturating_sub(at).max(0) as f64 / 1000.0;
            self.ctx.observe.position_risk(
                &self.ctx.instance,
                Some(&self.shard),
                age / RETENTION.as_secs_f64(),
            );
        }
    }
}

#[async_trait]
impl Source for ShardSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        if let Some(p) = pos.as_ref().and_then(ShardPos::from_position) {
            self.pos = p;
        }
        self.api.get().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.pos.ended {
            return Ok(None);
        }
        if self.carry.is_empty() {
            if let Some(t) = self.idle_until {
                if tokio::time::Instant::now() < t {
                    return Ok(None);
                }
                self.idle_until = None;
            }
            let api = self.api.get().await?;
            let iterator = match &self.iterator {
                Some(i) => i.clone(),
                None => {
                    let i = api
                        .iterator(&self.stream, self.shard.as_str(), &self.start())
                        .await?;
                    self.iterator = Some(i.clone());
                    i
                }
            };
            let page = match api.records(&iterator, budget.max_rows.max(1)).await {
                Ok(p) => p,
                Err(RecordsError::Expired) => {
                    // Re-open at the same place on the next call.
                    self.iterator = None;
                    return Err(ConnectorError::retryable(
                        "GetRecords: the shard iterator expired",
                    ));
                }
                Err(RecordsError::Other(e)) => {
                    if e.class.is_transient() {
                        self.iterator = None;
                    }
                    return Err(e);
                }
            };
            self.carry_end = page.next.is_none();
            self.iterator = page.next;
            if page.records.is_empty() {
                if self.carry_end {
                    // The closed shard is read to its end: record it, so its
                    // children are released.
                    self.pos.ended = true;
                    return Ok(Some(SourceBatch {
                        events: EventBatch::Events(Vec::new()),
                        position: self.pos.to_position(self.shard.as_str()),
                    }));
                }
                self.ctx
                    .observe
                    .lag(&self.ctx.instance, Some(&self.shard), Duration::ZERO);
                self.report_risk();
                self.idle_until = Some(tokio::time::Instant::now() + self.poll);
                return Ok(None);
            }
            self.carry.extend(page.records);
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        while let Some(r) = self.carry.front() {
            if events.len() >= budget.max_rows {
                break;
            }
            let ev = record_event(&self.envelope, &self.resource, self.shard.as_str(), r);
            let n = ev.size_bytes();
            if bytes + n > budget.max_bytes {
                if events.is_empty() {
                    let Some(r) = self.carry.pop_front() else {
                        break;
                    };
                    self.pos.seq = Some(r.sequence.clone());
                    return Err(ConnectorError::poison(format!(
                        "record {}/{} is {n} bytes, over the batch budget of {}",
                        self.shard, r.sequence, budget.max_bytes
                    )));
                }
                break;
            }
            bytes += n;
            self.pos.seq = Some(r.sequence.clone());
            self.pos.at_ms = r.created_ms.or(self.pos.at_ms);
            events.push(ev);
            self.carry.pop_front();
        }
        if self.carry.is_empty() && self.carry_end {
            self.pos.ended = true;
        }
        if let Some(at) = self.pos.at_ms {
            let lag = now_millis().saturating_sub(at).max(0);
            self.ctx.observe.lag(
                &self.ctx.instance,
                Some(&self.shard),
                Duration::from_millis(lag as u64),
            );
        }
        self.report_risk();
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.to_position(self.shard.as_str()),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // DynamoDB Streams has no consumer-side commit: the checkpoint is
        // the position.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// The `dynamodb-streams` factory (a source only).
#[derive(Debug, Clone, Default)]
pub struct DynamoDbStreamsFactory {
    api: Option<Arc<dyn StreamsApi>>,
}

impl DynamoDbStreamsFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_api(api: Arc<dyn StreamsApi>) -> Self {
        Self { api: Some(api) }
    }
}

impl ConnectorFactory for DynamoDbStreamsFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Config::of(ctx);
        let settings = StreamSettings::from_config(&cfg)?;
        let api = match &self.api {
            Some(a) => StreamsCell::fixed(ctx.clone(), cfg, a.clone()),
            None => StreamsCell::aws(ctx.clone(), cfg),
        };
        Ok(Some((
            Box::new(ShardEnumerator {
                api: api.clone(),
                settings: settings.clone(),
            }),
            Box::new(ShardSourceFactory {
                api,
                envelope: EnvelopeBuilder::new(ID, &ctx.instance),
                settings,
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Instant;

    use aws_sdk_dynamodb::types::AttributeValue;
    use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
    use loams_connector_sdk::{
        ErrorClass, InstanceKey, Pipeline, PipelineConfig, Scrubber, StaticSecrets,
    };

    use super::*;

    fn shard(id: &str, parent: Option<&str>, closed: bool) -> ShardInfo {
        ShardInfo {
            id: id.into(),
            parent: parent.map(Into::into),
            closed,
        }
    }

    #[test]
    fn lineage_releases_children_after_their_parent_ends() {
        let shards = vec![
            shard("p", None, true),
            shard("c1", Some("p"), false),
            shard("o", Some("gone"), false),
        ];
        let none = BTreeMap::new();
        let ids = |v: Vec<ShardInfo>| v.into_iter().map(|s| s.id).collect::<Vec<_>>();
        assert_eq!(
            ids(release(&shards, &none)),
            vec!["p", "o"],
            "an orphan's parent was trimmed"
        );
        let reading = BTreeMap::from([(
            "p".to_owned(),
            ShardPos {
                seq: Some("5".into()),
                ..ShardPos::default()
            },
        )]);
        assert_eq!(ids(release(&shards, &reading)), vec!["p", "o"]);
        let ended = BTreeMap::from([(
            "p".to_owned(),
            ShardPos {
                ended: true,
                ..ShardPos::default()
            },
        )]);
        assert_eq!(ids(release(&shards, &ended)), vec!["p", "c1", "o"]);
    }

    #[test]
    fn dynamodb_streams_ops_map_to_loamsop() {
        let env = EnvelopeBuilder::new(ID, &InstanceKey::new(1, "s"));
        let keys = HashMap::from([("id".to_owned(), AttributeValue::S("a".into()))]);
        for (name, op) in [("INSERT", "c"), ("MODIFY", "u"), ("REMOVE", "d")] {
            let r = StreamRecord {
                event_name: name.into(),
                sequence: "100".into(),
                keys: keys.clone(),
                new_image: (name != "REMOVE").then(|| keys.clone()),
                old_image: (name != "INSERT").then(|| keys.clone()),
                created_ms: Some(1_000),
            };
            let ev = record_event(&env, "orders", "shard-1", &r);
            assert_eq!(ev.extensions["loamsop"], op, "{name}");
            assert_eq!(ev.extensions["loamslsn"], "100");
            assert_eq!(ev.id, "shard-1/100");
            assert_eq!(ev.ty, "io.loams.dev.flow.dynamodb-streams.change.v1");
            assert_eq!(ev.subject.as_deref(), Some(r#"{"id":"a"}"#));
            let data: Value = serde_json::from_slice(&ev.data).expect("json");
            assert_eq!(data.get("new").is_some(), name != "REMOVE");
        }
        assert!(op_of("UNKNOWN").is_none());
    }

    /// A stream in memory: shards with records, closed shards end after
    /// their records, and a shard may be marked trimmed.
    #[derive(Debug, Default)]
    struct FakeStream {
        shards: Mutex<Vec<(ShardInfo, Vec<StreamRecord>)>>,
        trimmed: Mutex<BTreeSet<String>>,
        reads: Mutex<Vec<String>>,
    }

    fn rec(seq: u64, id: &str) -> StreamRecord {
        StreamRecord {
            event_name: "INSERT".into(),
            sequence: format!("{seq:021}"),
            keys: HashMap::from([("id".to_owned(), AttributeValue::S(id.into()))]),
            new_image: None,
            old_image: None,
            created_ms: Some(now_millis()),
        }
    }

    #[async_trait]
    impl StreamsApi for FakeStream {
        async fn stream_of(&self, _table: &str) -> Result<String, ConnectorError> {
            Ok("arn:aws:dynamodb:us-east-1:0:table/t/stream/x".into())
        }
        async fn shards(&self, _stream: &str) -> Result<Vec<ShardInfo>, ConnectorError> {
            Ok(self
                .shards
                .lock()
                .expect("lock")
                .iter()
                .map(|(s, _)| s.clone())
                .collect())
        }
        async fn iterator(
            &self,
            _stream: &str,
            shard: &str,
            at: &IteratorStart,
        ) -> Result<String, ConnectorError> {
            if self.trimmed.lock().expect("lock").contains(shard)
                && matches!(at, IteratorStart::After(_))
            {
                return Err(aws::class_for_code(
                    "TrimmedDataAccessException",
                    Some(400),
                    "GetShardIterator",
                ));
            }
            let from = match at {
                IteratorStart::After(s) => s.parse::<u64>().unwrap_or(0) + 1,
                _ => 0,
            };
            Ok(format!("{shard}|{from}"))
        }
        async fn records(&self, iterator: &str, limit: usize) -> Result<RecordsPage, RecordsError> {
            let (shard, from) = iterator.split_once('|').expect("iterator");
            let from: u64 = from.parse().expect("from");
            self.reads.lock().expect("lock").push(shard.to_owned());
            let shards = self.shards.lock().expect("lock");
            let (info, recs) = shards.iter().find(|(s, _)| s.id == shard).expect("shard");
            let page: Vec<StreamRecord> = recs
                .iter()
                .filter(|r| r.sequence.parse::<u64>().unwrap_or(0) >= from)
                .take(limit)
                .cloned()
                .collect();
            let after = page
                .last()
                .map_or(from, |r| r.sequence.parse::<u64>().unwrap_or(0) + 1);
            let rest = recs
                .iter()
                .any(|r| r.sequence.parse::<u64>().unwrap_or(0) >= after);
            let next = if info.closed && !rest && page.is_empty() {
                None
            } else {
                Some(format!("{shard}|{after}"))
            };
            Ok(RecordsPage {
                records: page,
                next,
            })
        }
    }

    fn ctx() -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "streams"),
            json!({ "table": "t", "poll_interval_ms": 5 }),
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        )
    }

    async fn run_until(
        fake: Arc<FakeStream>,
        fabric: &MemoryFabric,
        store: &MemoryCheckpointStore,
        want: usize,
    ) -> loams_connector_sdk::PipelineStatus {
        let c = ctx();
        let (en, sf) = DynamoDbStreamsFactory::with_api(fake)
            .source(&c)
            .expect("source")
            .expect("some");
        let cfg = PipelineConfig {
            linger: Duration::from_millis(1),
            max_batch_rows: 2,
            ..PipelineConfig::default()
        };
        let h = Pipeline::source(c.instance.clone(), cfg)
            .enumerator(en)
            .sources(Arc::from(sf))
            .writer(Arc::new(fabric.clone()))
            .checkpoints(Arc::new(store.clone()))
            .discover_every(Duration::from_millis(10))
            .start()
            .await
            .expect("start");
        let deadline = Instant::now() + Duration::from_secs(10);
        while fabric.len() < want && Instant::now() < deadline && h.status().last_error.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.stop().await
    }

    /// `dynamodb_streams_lineage_followed`, against an in-memory stream: a
    /// child shard is read only after its parent ended, so every key's
    /// changes keep their order across the split, and a restart reads
    /// nothing twice.
    #[tokio::test]
    async fn dynamodb_streams_lineage_followed_in_memory() {
        let fake = Arc::new(FakeStream::default());
        *fake.shards.lock().expect("lock") = vec![
            (
                shard("parent", None, true),
                vec![rec(1, "a"), rec(2, "b"), rec(3, "a")],
            ),
            (
                shard("child", Some("parent"), false),
                vec![rec(10, "a"), rec(11, "c")],
            ),
        ];
        let fabric = MemoryFabric::new();
        let store = MemoryCheckpointStore::new();
        run_until(fake.clone(), &fabric, &store, 5).await;
        let ids = fabric.ids();
        assert_eq!(ids.len(), 5, "{ids:?}");
        let first_child = ids
            .iter()
            .position(|i| i.starts_with("child/"))
            .expect("child read");
        let last_parent = ids
            .iter()
            .rposition(|i| i.starts_with("parent/"))
            .expect("parent read");
        assert!(last_parent < first_child, "parent before child: {ids:?}");
        let reads = fake.reads.lock().expect("lock").clone();
        let first_child_read = reads.iter().position(|s| s == "child").expect("child read");
        assert!(
            reads[first_child_read..].iter().all(|s| s != "parent"),
            "the parent is not read after its child starts: {reads:?}"
        );

        // A restart from the checkpoint reads nothing again.
        fake.shards.lock().expect("lock")[1].1.push(rec(12, "d"));
        run_until(fake, &fabric, &store, 6).await;
        assert_eq!(fabric.len(), 6);
        assert_eq!(fabric.duplicates(), 0, "nothing re-sent after a restart");
    }

    #[tokio::test]
    async fn trimmed_checkpoint_is_position_lost() {
        let fake = Arc::new(FakeStream::default());
        *fake.shards.lock().expect("lock") =
            vec![(shard("s", None, false), vec![rec(1, "a"), rec(2, "b")])];
        let fabric = MemoryFabric::new();
        let store = MemoryCheckpointStore::new();
        run_until(fake.clone(), &fabric, &store, 2).await;
        fake.trimmed.lock().expect("lock").insert("s".into());
        let status = run_until(fake, &fabric, &store, 99).await;
        let err = status.last_error.expect("stopped");
        assert_eq!(err.class, ErrorClass::PositionLost);
        assert!(err.message.contains("re-snapshot"), "{}", err.message);
    }
}
