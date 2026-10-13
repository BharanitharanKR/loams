//! DynamoDB: a parallel `Scan` source and a `BatchWriteItem` sink.
//!
//! **Source.** `segments` (default 4) `Scan` segments are the splits
//! (`segment-<i>-of-<n>`), read in parallel by the pipeline. A segment's
//! position is its `LastEvaluatedKey` (as DynamoDB JSON) and whether it has
//! finished; a restart resumes with `ExclusiveStartKey`. Each item is a
//! `document` event whose id is the table plus a digest of the item: an
//! unchanged item re-read has the same id (`ingest` drops it), a changed
//! one is a new event. Without `poll_interval_ms` a scan runs once.
//!
//! **Sink.** Events are written with `BatchWriteItem` (25 requests a call,
//! a `loamsop = d` event as a `DeleteRequest`); `UnprocessedItems` are
//! retried with backoff, and what is still unprocessed after
//! `max_unprocessed_retries` makes the batch `Throttled` (the whole batch is
//! retried: puts are idempotent). With `version_attribute`, every put is a
//! conditional `PutItem` that only replaces an older version, so a replay
//! never overwrites a newer write (idempotent and ordered by version).
//!
//! The AWS calls sit behind [`DynamoApi`], so the scan and retry logic is
//! tested without a server; [`AwsDynamo`] is the real client.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use base64::Engine as _;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder,
    EventBatch, Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::aws::{self, AwsSettings, LoamsHttpClient};
use crate::config::Config;
use crate::util::{digest16, is_delete, json_object, rows_of};

/// The manifest id.
pub const ID: &str = "dynamodb";
/// The event name of an item.
pub const EVENT: &str = "document";

/// An item (or a key): attribute name to value.
pub type Item = HashMap<String, AttributeValue>;

/// How items are rendered as JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ItemFormat {
    /// Plain JSON (`{"id": "a", "n": 1}`). String, number and binary sets
    /// become arrays, so a round trip through plain JSON writes lists.
    #[default]
    Json,
    /// DynamoDB JSON (`{"id": {"S": "a"}, "n": {"N": "1"}}`): lossless.
    DynamoDbJson,
}

impl ItemFormat {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        match cfg.str("item_format")?.as_deref() {
            None | Some("json") => Ok(ItemFormat::Json),
            Some("dynamodb-json") => Ok(ItemFormat::DynamoDbJson),
            Some(_) => Err(ConnectorError::fatal(
                "config: `item_format` must be json or dynamodb-json",
            )),
        }
    }
}

// ----------------------------------------------------------------- JSON --

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// A DynamoDB number as JSON: a JSON number when the text survives the
/// round trip exactly, else the text (no precision is lost silently).
fn number_json(n: &str) -> Value {
    match serde_json::from_str::<Value>(n) {
        Ok(v @ Value::Number(_)) if serde_json::to_string(&v).ok().as_deref() == Some(n) => v,
        _ => Value::String(n.to_owned()),
    }
}

/// One attribute value as JSON in `format`.
pub fn av_to_json(v: &AttributeValue, format: ItemFormat) -> Value {
    if format == ItemFormat::DynamoDbJson {
        return av_to_ddb_json(v);
    }
    match v {
        AttributeValue::S(s) => json!(s),
        AttributeValue::N(n) => number_json(n),
        AttributeValue::B(b) => json!(b64(b.as_ref())),
        AttributeValue::Bool(b) => json!(b),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::L(l) => Value::Array(l.iter().map(|v| av_to_json(v, format)).collect()),
        AttributeValue::M(m) => item_to_json(m, format),
        AttributeValue::Ss(ss) => json!(sorted(ss.clone())),
        AttributeValue::Ns(ns) => {
            Value::Array(sorted(ns.clone()).iter().map(|n| number_json(n)).collect())
        }
        AttributeValue::Bs(bs) => json!(sorted(bs.iter().map(|b| b64(b.as_ref())).collect())),
        _ => Value::Null,
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn av_to_ddb_json(v: &AttributeValue) -> Value {
    match v {
        AttributeValue::S(s) => json!({ "S": s }),
        AttributeValue::N(n) => json!({ "N": n }),
        AttributeValue::B(b) => json!({ "B": b64(b.as_ref()) }),
        AttributeValue::Bool(b) => json!({ "BOOL": b }),
        AttributeValue::Null(_) => json!({ "NULL": true }),
        AttributeValue::L(l) => json!({ "L": l.iter().map(av_to_ddb_json).collect::<Vec<_>>() }),
        AttributeValue::M(m) => json!({ "M": item_to_json(m, ItemFormat::DynamoDbJson) }),
        AttributeValue::Ss(ss) => json!({ "SS": sorted(ss.clone()) }),
        AttributeValue::Ns(ns) => json!({ "NS": sorted(ns.clone()) }),
        AttributeValue::Bs(bs) => {
            json!({ "BS": sorted(bs.iter().map(|b| b64(b.as_ref())).collect()) })
        }
        _ => Value::Null,
    }
}

/// An item as a JSON object, keys sorted (a stable rendering, so the id
/// digest does not depend on the server's attribute order).
pub fn item_to_json(item: &Item, format: ItemFormat) -> Value {
    let mut keys: Vec<&String> = item.keys().collect();
    keys.sort();
    Value::Object(
        keys.into_iter()
            .map(|k| (k.clone(), av_to_json(&item[k], format)))
            .collect(),
    )
}

/// A JSON value as an attribute value in `format`.
pub fn json_to_av(v: &Value, format: ItemFormat) -> Result<AttributeValue, String> {
    if format == ItemFormat::DynamoDbJson {
        return ddb_json_to_av(v);
    }
    Ok(match v {
        Value::Null => AttributeValue::Null(true),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Number(n) => AttributeValue::N(n.to_string()),
        Value::String(s) => AttributeValue::S(s.clone()),
        Value::Array(a) => AttributeValue::L(
            a.iter()
                .map(|v| json_to_av(v, format))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(o) => AttributeValue::M(json_to_item(o, format)?),
    })
}

fn strings(v: &Value) -> Result<Vec<String>, String> {
    v.as_array()
        .ok_or("a set must be a list")?
        .iter()
        .map(|s| {
            s.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "a set member must be a string".to_owned())
        })
        .collect()
}

fn unb64(s: &str) -> Result<aws_smithy_types::Blob, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map(aws_smithy_types::Blob::new)
        .map_err(|_| "binary values must be base64".to_owned())
}

fn ddb_json_to_av(v: &Value) -> Result<AttributeValue, String> {
    let o = v
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or("a DynamoDB JSON value is an object with one type key")?;
    let (ty, inner) = o.iter().next().ok_or("empty DynamoDB JSON value")?;
    let text = || {
        inner
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("{ty} needs a string"))
    };
    Ok(match ty.as_str() {
        "S" => AttributeValue::S(text()?),
        "N" => AttributeValue::N(text()?),
        "B" => AttributeValue::B(unb64(&text()?)?),
        "BOOL" => AttributeValue::Bool(inner.as_bool().ok_or("BOOL needs true or false")?),
        "NULL" => AttributeValue::Null(true),
        "L" => AttributeValue::L(
            inner
                .as_array()
                .ok_or("L needs a list")?
                .iter()
                .map(ddb_json_to_av)
                .collect::<Result<_, _>>()?,
        ),
        "M" => AttributeValue::M(json_to_item(
            inner.as_object().ok_or("M needs an object")?,
            ItemFormat::DynamoDbJson,
        )?),
        "SS" => AttributeValue::Ss(strings(inner)?),
        "NS" => AttributeValue::Ns(strings(inner)?),
        "BS" => AttributeValue::Bs(
            strings(inner)?
                .iter()
                .map(|s| unb64(s))
                .collect::<Result<_, _>>()?,
        ),
        other => return Err(format!("unknown DynamoDB type {other}")),
    })
}

/// A JSON object as an item.
pub fn json_to_item(
    o: &serde_json::Map<String, Value>,
    format: ItemFormat,
) -> Result<Item, String> {
    o.iter()
        .map(|(k, v)| Ok((k.clone(), json_to_av(v, format)?)))
        .collect()
}

// ------------------------------------------------------------------ API --

/// A write in a `BatchWriteItem` call.
#[derive(Debug, Clone, PartialEq)]
pub enum Write {
    Put(Item),
    Delete(Item),
}

/// One page of a scan segment.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanPage {
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
}

/// The DynamoDB calls the connector makes.
#[async_trait]
pub trait DynamoApi: Send + Sync + std::fmt::Debug {
    async fn scan(
        &self,
        table: &str,
        segment: u32,
        total: u32,
        start: Option<Item>,
        limit: usize,
        consistent: bool,
    ) -> Result<ScanPage, ConnectorError>;
    /// The writes the service left unprocessed.
    async fn batch_write(
        &self,
        table: &str,
        writes: Vec<Write>,
    ) -> Result<Vec<Write>, ConnectorError>;
    /// `PutItem` only if the stored item's `attr` is absent or below
    /// `version`; `false` when the condition failed (a newer or equal
    /// version is stored).
    async fn put_if_newer(
        &self,
        table: &str,
        item: Item,
        attr: &str,
        version: &str,
    ) -> Result<bool, ConnectorError>;
    /// The table's key attribute names, partition key first.
    async fn key_schema(&self, table: &str) -> Result<Vec<String>, ConnectorError>;
}

/// The real client.
#[derive(Debug, Clone)]
pub struct AwsDynamo {
    client: aws_sdk_dynamodb::Client,
}

impl AwsDynamo {
    /// A client for `ctx`'s config: region, endpoint and the credentials its
    /// secret names resolve to, over the SDK's HTTP client.
    pub async fn connect(ctx: &ConnectorContext, cfg: &Config) -> Result<Self, ConnectorError> {
        let s = AwsSettings::from_config(cfg)?;
        let creds = aws::credentials(ctx, cfg).await?;
        let mut b = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new(s.region))
            .credentials_provider(creds)
            .http_client(LoamsHttpClient::new(ctx.egress.clone()))
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled());
        b.set_endpoint_url(s.endpoint_url);
        let conf = b.build();
        Ok(Self {
            client: aws_sdk_dynamodb::Client::from_conf(conf),
        })
    }

    pub fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

fn to_request(w: Write) -> Result<WriteRequest, ConnectorError> {
    let bad = |e: aws_sdk_dynamodb::error::BuildError| {
        ConnectorError::fatal(format!("building a write: {e}"))
    };
    Ok(match w {
        Write::Put(item) => WriteRequest::builder()
            .put_request(
                PutRequest::builder()
                    .set_item(Some(item))
                    .build()
                    .map_err(bad)?,
            )
            .build(),
        Write::Delete(key) => WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(key))
                    .build()
                    .map_err(bad)?,
            )
            .build(),
    })
}

fn from_request(w: &WriteRequest) -> Option<Write> {
    if let Some(p) = w.put_request() {
        return Some(Write::Put(p.item().clone()));
    }
    w.delete_request().map(|d| Write::Delete(d.key().clone()))
}

#[async_trait]
impl DynamoApi for AwsDynamo {
    async fn scan(
        &self,
        table: &str,
        segment: u32,
        total: u32,
        start: Option<Item>,
        limit: usize,
        consistent: bool,
    ) -> Result<ScanPage, ConnectorError> {
        let out = self
            .client
            .scan()
            .table_name(table)
            .segment(i32::try_from(segment).unwrap_or(0))
            .total_segments(i32::try_from(total).unwrap_or(1))
            .limit(i32::try_from(limit).unwrap_or(i32::MAX))
            .consistent_read(consistent)
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| aws::classify(&e, "Scan"))?;
        Ok(ScanPage {
            items: out.items().to_vec(),
            last_evaluated_key: out.last_evaluated_key().cloned(),
        })
    }

    async fn batch_write(
        &self,
        table: &str,
        writes: Vec<Write>,
    ) -> Result<Vec<Write>, ConnectorError> {
        let reqs = writes
            .into_iter()
            .map(to_request)
            .collect::<Result<Vec<_>, _>>()?;
        let out = self
            .client
            .batch_write_item()
            .request_items(table, reqs)
            .send()
            .await
            .map_err(|e| match aws::code_of(&e) {
                Some("ValidationException") => {
                    ConnectorError::poison("BatchWriteItem: ValidationException")
                }
                _ => aws::classify(&e, "BatchWriteItem"),
            })?;
        Ok(out
            .unprocessed_items()
            .and_then(|m| m.get(table))
            .map(|v| v.iter().filter_map(from_request).collect())
            .unwrap_or_default())
    }

    async fn put_if_newer(
        &self,
        table: &str,
        item: Item,
        attr: &str,
        version: &str,
    ) -> Result<bool, ConnectorError> {
        let r = self
            .client
            .put_item()
            .table_name(table)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(#v) OR #v < :v")
            .expression_attribute_names("#v", attr)
            .expression_attribute_values(":v", AttributeValue::N(version.to_owned()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e) if aws::code_of(&e) == Some("ConditionalCheckFailedException") => Ok(false),
            Err(e) if aws::code_of(&e) == Some("ValidationException") => {
                Err(ConnectorError::poison("PutItem: ValidationException"))
            }
            Err(e) => Err(aws::classify(&e, "PutItem")),
        }
    }

    async fn key_schema(&self, table: &str) -> Result<Vec<String>, ConnectorError> {
        let out = self
            .client
            .describe_table()
            .table_name(table)
            .send()
            .await
            .map_err(|e| aws::classify(&e, "DescribeTable"))?;
        let mut keys: Vec<(bool, String)> = out
            .table()
            .map(|t| {
                t.key_schema()
                    .iter()
                    .map(|k| {
                        (
                            k.key_type().as_str() != "HASH",
                            k.attribute_name().to_owned(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        keys.sort();
        Ok(keys.into_iter().map(|(_, n)| n).collect())
    }
}

/// Builds the API once, on first use (it needs the secrets, which resolve
/// asynchronously), and shares it between the instance's splits.
#[derive(Debug, Clone)]
pub struct ApiCell {
    ctx: ConnectorContext,
    cfg: Config,
    cell: Arc<OnceCell<Arc<dyn DynamoApi>>>,
}

impl ApiCell {
    pub fn aws(ctx: ConnectorContext, cfg: Config) -> Self {
        Self {
            ctx,
            cfg,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// A cell that already holds `api` (tests and other runtimes).
    pub fn fixed(ctx: ConnectorContext, cfg: Config, api: Arc<dyn DynamoApi>) -> Self {
        Self {
            ctx,
            cfg,
            cell: Arc::new(OnceCell::new_with(Some(api))),
        }
    }

    pub async fn get(&self) -> Result<Arc<dyn DynamoApi>, ConnectorError> {
        self.cell
            .get_or_try_init(|| async {
                AwsDynamo::connect(&self.ctx, &self.cfg)
                    .await
                    .map(|c| Arc::new(c) as Arc<dyn DynamoApi>)
            })
            .await
            .cloned()
    }
}

// --------------------------------------------------------------- source --

/// Scan settings: `table`, `segments` (1–1,000, default 4), `page_size`
/// (default 100), `consistent_read`, `poll_interval_ms`, `item_format`.
#[derive(Debug, Clone)]
pub struct ScanSettings {
    pub table: String,
    pub segments: u32,
    pub page_size: usize,
    pub consistent: bool,
    pub poll_interval: Option<Duration>,
    pub format: ItemFormat,
}

impl ScanSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        Ok(Self {
            table: cfg.required_str("table")?,
            segments: cfg.u64("segments")?.unwrap_or(4).clamp(1, 1_000) as u32,
            page_size: cfg.u64("page_size")?.unwrap_or(100).clamp(1, 10_000) as usize,
            consistent: cfg.bool("consistent_read")?.unwrap_or(false),
            poll_interval: cfg.millis("poll_interval_ms")?,
            format: ItemFormat::from_config(cfg)?,
        })
    }
}

/// The split id of a segment.
pub fn segment_split(i: u32, n: u32) -> String {
    format!("segment-{i}-of-{n}")
}

/// One split per `Scan` segment.
#[derive(Debug)]
pub struct SegmentEnumerator {
    total: u32,
}

impl SegmentEnumerator {
    pub fn new(total: u32) -> Self {
        Self { total }
    }
}

#[async_trait]
impl SplitEnumerator for SegmentEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok((0..self.total)
            .map(|i| {
                Split::new(
                    segment_split(i, self.total),
                    json!({ "segment": i, "total": self.total }),
                )
            })
            .collect())
    }
}

/// A segment's position (its opaque bytes are this, as JSON).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SegmentPos {
    pub pass: u64,
    /// Pages read in this pass.
    pub pages: u64,
    /// The `LastEvaluatedKey` to resume from, as DynamoDB JSON.
    pub start: Option<Value>,
    pub done: bool,
}

impl SegmentPos {
    fn to_position(&self, split: &str) -> Position {
        let display = if self.done {
            format!("{split} pass {} done after {} pages", self.pass, self.pages)
        } else {
            format!("{split} pass {} after {} pages", self.pass, self.pages)
        };
        Position::new(serde_json::to_vec(self).unwrap_or_default(), display)
    }

    fn start_key(&self) -> Result<Option<Item>, ConnectorError> {
        match &self.start {
            None => Ok(None),
            Some(Value::Object(o)) => {
                json_to_item(o, ItemFormat::DynamoDbJson)
                    .map(Some)
                    .map_err(|e| {
                        ConnectorError::fatal(format!(
                            "the checkpointed scan key does not parse: {e}"
                        ))
                    })
            }
            Some(_) => Err(ConnectorError::fatal(
                "the checkpointed scan key is not an object",
            )),
        }
    }
}

#[derive(Debug)]
pub struct ScanSourceFactory {
    api: ApiCell,
    envelope: EnvelopeBuilder,
    settings: ScanSettings,
}

impl ScanSourceFactory {
    pub fn new(api: ApiCell, envelope: EnvelopeBuilder, settings: ScanSettings) -> Self {
        Self {
            api,
            envelope,
            settings,
        }
    }
}

impl SourceFactory for ScanSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let segment = split.spec["segment"].as_u64().unwrap_or(0) as u32;
        let total = split.spec["total"].as_u64().unwrap_or(1) as u32;
        Ok(Box::new(ScanSource {
            api: self.api.clone(),
            envelope: self.envelope.clone(),
            settings: self.settings.clone(),
            split: split.id.as_str().to_owned(),
            segment,
            total,
            pos: SegmentPos::default(),
            carry: Vec::new(),
            carry_next: None,
            finished_at: None,
        }))
    }
}

#[derive(Debug)]
struct ScanSource {
    api: ApiCell,
    envelope: EnvelopeBuilder,
    settings: ScanSettings,
    split: String,
    segment: u32,
    total: u32,
    pos: SegmentPos,
    /// Items of the current page not yet returned (reversed: pop from the end).
    carry: Vec<Item>,
    /// The page's `LastEvaluatedKey`, applied once `carry` is empty.
    carry_next: Option<Option<Item>>,
    finished_at: Option<Instant>,
}

/// The event an item becomes.
pub fn item_event(
    envelope: &EnvelopeBuilder,
    table: &str,
    item: &Item,
    format: ItemFormat,
) -> CloudEvent {
    let body = serde_json::to_vec(&item_to_json(item, format)).unwrap_or_default();
    let stable =
        serde_json::to_vec(&item_to_json(item, ItemFormat::DynamoDbJson)).unwrap_or_default();
    envelope
        .event(
            EVENT,
            table,
            format!("{table}/{}", digest16(&[table.as_bytes(), &stable])),
        )
        .with_data("application/json", body)
}

#[async_trait]
impl Source for ScanSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        if let Some(p) = pos {
            self.pos = serde_json::from_slice(&p.opaque).map_err(|_| {
                ConnectorError::fatal("the checkpointed scan position does not parse")
            })?;
            if self.pos.done {
                self.finished_at = Some(Instant::now());
            }
        }
        self.api.get().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.pos.done && self.carry.is_empty() {
            match (self.settings.poll_interval, self.finished_at) {
                (Some(every), Some(at)) if at.elapsed() >= every => {
                    self.pos = SegmentPos {
                        pass: self.pos.pass + 1,
                        ..SegmentPos::default()
                    };
                    self.finished_at = None;
                }
                _ => return Ok(None),
            }
        }
        if self.carry.is_empty() {
            let api = self.api.get().await?;
            let limit = self.settings.page_size.min(budget.max_rows.max(1));
            let page = api
                .scan(
                    &self.settings.table,
                    self.segment,
                    self.total,
                    self.pos.start_key()?,
                    limit,
                    self.settings.consistent,
                )
                .await?;
            self.carry = page.items;
            self.carry.reverse();
            self.carry_next = Some(page.last_evaluated_key);
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        while let Some(item) = self.carry.last() {
            if events.len() >= budget.max_rows {
                break;
            }
            let ev = item_event(
                &self.envelope,
                &self.settings.table,
                item,
                self.settings.format,
            );
            let n = ev.size_bytes();
            if bytes + n > budget.max_bytes {
                if events.is_empty() {
                    self.carry.pop();
                    return Err(ConnectorError::poison(format!(
                        "an item of {} is {n} bytes, over the batch budget of {}",
                        self.settings.table, budget.max_bytes
                    )));
                }
                break;
            }
            bytes += n;
            events.push(ev);
            self.carry.pop();
        }
        if self.carry.is_empty()
            && let Some(next) = self.carry_next.take()
        {
            self.pos.pages += 1;
            match next {
                Some(key) => self.pos.start = Some(item_to_json(&key, ItemFormat::DynamoDbJson)),
                None => {
                    self.pos.start = None;
                    self.pos.done = true;
                    self.finished_at = Some(Instant::now());
                }
            }
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.to_position(&self.split),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ----------------------------------------------------------------- sink --

/// Sink settings: `table`, `version_attribute`, `max_unprocessed_retries`
/// (default 8), `item_format`.
#[derive(Debug, Clone)]
pub struct SinkSettings {
    pub table: String,
    pub version_attribute: Option<String>,
    /// The key attributes, partition key first (`key`); read from
    /// `DescribeTable` when the config does not give them.
    pub key: Vec<String>,
    pub max_unprocessed_retries: u32,
    pub format: ItemFormat,
    /// The first backoff after `UnprocessedItems` (doubled each time, capped
    /// at 5 s).
    pub backoff: Duration,
}

impl SinkSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        Ok(Self {
            table: cfg.required_str("table")?,
            version_attribute: cfg.str("version_attribute")?,
            key: cfg.strings("key")?,
            max_unprocessed_retries: cfg.u64("max_unprocessed_retries")?.unwrap_or(8).min(64)
                as u32,
            format: ItemFormat::from_config(cfg)?,
            backoff: Duration::from_millis(50),
        })
    }
}

#[derive(Debug)]
pub struct DynamoSinkFactory {
    api: ApiCell,
    settings: SinkSettings,
}

impl DynamoSinkFactory {
    pub fn new(api: ApiCell, settings: SinkSettings) -> Self {
        Self { api, settings }
    }
}

impl SinkFactory for DynamoSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(DynamoSink {
            api: self.api.clone(),
            settings: self.settings.clone(),
            keys: None,
        })))
    }
}

#[derive(Debug)]
struct DynamoSink {
    api: ApiCell,
    settings: SinkSettings,
    keys: Option<Vec<String>>,
}

/// The largest `BatchWriteItem` call.
pub const BATCH_LIMIT: usize = 25;

impl DynamoSink {
    fn decode(&self, ev: &CloudEvent, keys: &[String]) -> Result<(Write, Vec<u8>), ConnectorError> {
        let obj = json_object(ev)?;
        let item = json_to_item(&obj, self.settings.format)
            .map_err(|e| ConnectorError::poison(format!("event {}: {e}", ev.id)))?;
        let mut key = Item::new();
        for k in keys {
            let v = item.get(k).ok_or_else(|| {
                ConnectorError::poison(format!("event {}: the key attribute {k} is missing", ev.id))
            })?;
            key.insert(k.clone(), v.clone());
        }
        let key_id =
            serde_json::to_vec(&item_to_json(&key, ItemFormat::DynamoDbJson)).unwrap_or_default();
        Ok((
            if is_delete(ev) {
                Write::Delete(key)
            } else {
                Write::Put(item)
            },
            key_id,
        ))
    }

    /// Write `writes` (no key twice), retrying `UnprocessedItems`.
    async fn write_chunk(
        &self,
        api: &dyn DynamoApi,
        mut writes: Vec<Write>,
    ) -> Result<(), ConnectorError> {
        let mut delay = self.settings.backoff;
        for attempt in 0..=self.settings.max_unprocessed_retries {
            if attempt > 0 {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            writes = api.batch_write(&self.settings.table, writes).await?;
            if writes.is_empty() {
                return Ok(());
            }
        }
        Err(ConnectorError::throttled(
            Duration::from_secs(1),
            format!(
                "BatchWriteItem: {} writes still unprocessed after {} retries",
                writes.len(),
                self.settings.max_unprocessed_retries
            ),
        ))
    }
}

#[async_trait]
impl Sink for DynamoSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = rows_of(batch)?;
        if events.is_empty() {
            return Ok(SinkAck::default());
        }
        let api = self.api.get().await?;
        if self.keys.is_none() {
            self.keys = Some(if self.settings.key.is_empty() {
                api.key_schema(&self.settings.table).await?
            } else {
                self.settings.key.clone()
            });
        }
        let keys = self.keys.clone().unwrap_or_default();
        // Decode everything first: one bad event fails the batch as Poison
        // before anything is written.
        let mut decoded = Vec::with_capacity(events.len());
        let mut bytes = 0u64;
        for ev in &events {
            decoded.push((self.decode(ev, &keys)?, ev));
            bytes += ev.size_bytes() as u64;
        }
        if let Some(attr) = self.settings.version_attribute.clone() {
            for ((w, _), ev) in decoded {
                match w {
                    Write::Put(item) => {
                        let version = match item.get(&attr) {
                            Some(AttributeValue::N(n)) => n.clone(),
                            _ => {
                                return Err(ConnectorError::poison(format!(
                                    "event {}: the version attribute {attr} is not a number",
                                    ev.id
                                )));
                            }
                        };
                        api.put_if_newer(&self.settings.table, item, &attr, &version)
                            .await?;
                    }
                    Write::Delete(key) => {
                        self.write_chunk(&*api, vec![Write::Delete(key)]).await?;
                    }
                }
            }
        } else {
            // A key may appear once per call: a repeated key starts a new
            // chunk, so writes keep their order.
            let mut chunk: Vec<Write> = Vec::new();
            let mut seen: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
            for ((w, key), _) in decoded {
                if chunk.len() == BATCH_LIMIT || seen.contains_key(&key) {
                    self.write_chunk(&*api, std::mem::take(&mut chunk)).await?;
                    seen.clear();
                }
                seen.insert(key, ());
                chunk.push(w);
            }
            if !chunk.is_empty() {
                self.write_chunk(&*api, chunk).await?;
            }
        }
        Ok(SinkAck {
            rows: events.len() as u64,
            bytes,
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

// -------------------------------------------------------------- factory --

/// The `dynamodb` factory.
#[derive(Debug, Clone, Default)]
pub struct DynamoDbFactory {
    api: Option<Arc<dyn DynamoApi>>,
}

impl DynamoDbFactory {
    /// The factory over the real AWS client.
    pub fn new() -> Self {
        Self::default()
    }

    /// A factory whose instances all use `api` (tests).
    pub fn with_api(api: Arc<dyn DynamoApi>) -> Self {
        Self { api: Some(api) }
    }

    fn cell(&self, ctx: &ConnectorContext, cfg: Config) -> ApiCell {
        match &self.api {
            Some(api) => ApiCell::fixed(ctx.clone(), cfg, api.clone()),
            None => ApiCell::aws(ctx.clone(), cfg),
        }
    }
}

impl ConnectorFactory for DynamoDbFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Config::of(ctx);
        let settings = ScanSettings::from_config(&cfg)?;
        let envelope = EnvelopeBuilder::new(ID, &ctx.instance);
        Ok(Some((
            Box::new(SegmentEnumerator::new(settings.segments)),
            Box::new(ScanSourceFactory::new(
                self.cell(ctx, cfg),
                envelope,
                settings,
            )),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Config::of(ctx);
        let settings = SinkSettings::from_config(&cfg)?;
        Ok(Some(Box::new(DynamoSinkFactory::new(
            self.cell(ctx, cfg),
            settings,
        ))))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
    use loams_connector_sdk::{InstanceKey, Pipeline, PipelineConfig, Scrubber, StaticSecrets};

    use super::*;

    /// An in-memory table: items by their `id` attribute, segments by a
    /// stable hash, pages of `limit`, and a scripted `UnprocessedItems`.
    #[derive(Debug, Default)]
    pub(crate) struct FakeDynamo {
        pub items: Mutex<BTreeMap<String, Item>>,
        /// For each `batch_write` call, in order: how many writes to leave
        /// unprocessed.
        pub unprocessed: Mutex<Vec<usize>>,
        pub writes: Mutex<u32>,
        pub scans: Mutex<Vec<(u32, u32)>>,
    }

    fn id_of(item: &Item) -> String {
        match item.get("id") {
            Some(AttributeValue::S(s)) => s.clone(),
            _ => String::new(),
        }
    }

    #[async_trait]
    impl DynamoApi for FakeDynamo {
        async fn scan(
            &self,
            _table: &str,
            segment: u32,
            total: u32,
            start: Option<Item>,
            limit: usize,
            _consistent: bool,
        ) -> Result<ScanPage, ConnectorError> {
            self.scans.lock().expect("lock").push((segment, total));
            let after = start.as_ref().map(id_of);
            let items = self.items.lock().expect("lock");
            let mine: Vec<Item> = items
                .iter()
                .filter(|(k, _)| {
                    u32::from(k.bytes().fold(0u8, u8::wrapping_add)) % total == segment
                })
                .filter(|(k, _)| after.as_ref().is_none_or(|a| *k > a))
                .map(|(_, v)| v.clone())
                .collect();
            let page: Vec<Item> = mine.iter().take(limit).cloned().collect();
            let more = mine.len() > limit;
            let lek = if more {
                page.last()
                    .map(|i| HashMap::from([("id".to_owned(), i["id"].clone())]))
            } else {
                None
            };
            Ok(ScanPage {
                items: page,
                last_evaluated_key: lek,
            })
        }

        async fn batch_write(
            &self,
            _table: &str,
            writes: Vec<Write>,
        ) -> Result<Vec<Write>, ConnectorError> {
            assert!(writes.len() <= BATCH_LIMIT, "a call over 25 writes");
            *self.writes.lock().expect("lock") += 1;
            let leave = {
                let mut u = self.unprocessed.lock().expect("lock");
                if u.is_empty() { 0 } else { u.remove(0) }
            };
            let split = writes.len().saturating_sub(leave);
            let (done, left) = writes.split_at(split);
            let mut items = self.items.lock().expect("lock");
            for w in done {
                match w {
                    Write::Put(i) => {
                        items.insert(id_of(i), i.clone());
                    }
                    Write::Delete(k) => {
                        items.remove(&id_of(k));
                    }
                }
            }
            Ok(left.to_vec())
        }

        async fn put_if_newer(
            &self,
            _table: &str,
            item: Item,
            attr: &str,
            version: &str,
        ) -> Result<bool, ConnectorError> {
            let mut items = self.items.lock().expect("lock");
            let newer = match items.get(&id_of(&item)).and_then(|i| i.get(attr)) {
                Some(AttributeValue::N(stored)) => {
                    stored.parse::<f64>().unwrap_or(0.0) < version.parse::<f64>().unwrap_or(0.0)
                }
                _ => true,
            };
            if newer {
                items.insert(id_of(&item), item);
            }
            Ok(newer)
        }

        async fn key_schema(&self, _table: &str) -> Result<Vec<String>, ConnectorError> {
            Ok(vec!["id".to_owned()])
        }
    }

    pub(crate) fn item(id: &str, n: i64) -> Item {
        HashMap::from([
            ("id".to_owned(), AttributeValue::S(id.to_owned())),
            ("n".to_owned(), AttributeValue::N(n.to_string())),
        ])
    }

    fn ctx(config: Value) -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "ddb"),
            config,
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        )
    }

    #[test]
    fn json_conversions_roundtrip() {
        let mut it = item("a", 7);
        it.insert("s".into(), AttributeValue::Ss(vec!["y".into(), "x".into()]));
        it.insert(
            "m".into(),
            AttributeValue::M(HashMap::from([(
                "f".to_owned(),
                AttributeValue::Bool(true),
            )])),
        );
        it.insert(
            "big".into(),
            AttributeValue::N("1.000000000000000000001".into()),
        );
        let plain = item_to_json(&it, ItemFormat::Json);
        assert_eq!(plain["n"], json!(7));
        assert_eq!(plain["s"], json!(["x", "y"]));
        assert_eq!(
            plain["big"],
            json!("1.000000000000000000001"),
            "no precision lost silently"
        );
        let ddb = item_to_json(&it, ItemFormat::DynamoDbJson);
        let back = json_to_item(ddb.as_object().expect("object"), ItemFormat::DynamoDbJson)
            .expect("parse");
        assert_eq!(item_to_json(&back, ItemFormat::DynamoDbJson), ddb);
        assert!(json_to_av(&json!({"X": 1}), ItemFormat::DynamoDbJson).is_err());
    }

    /// `dynamodb_parallel_scan_segments`, against the in-memory table: every
    /// segment is a split read in parallel, and together they read every
    /// item exactly once, across pages, with the segment's last key
    /// checkpointed.
    #[tokio::test]
    async fn dynamodb_parallel_scan_segments_in_memory() {
        let fake = Arc::new(FakeDynamo::default());
        {
            let mut items = fake.items.lock().expect("lock");
            for i in 0..57 {
                items.insert(format!("k{i:03}"), item(&format!("k{i:03}"), i));
            }
        }
        let f = DynamoDbFactory::with_api(fake.clone());
        let c = ctx(json!({ "table": "t", "segments": 3, "page_size": 5 }));
        let (en, sf) = f.source(&c).expect("source").expect("some");
        let fabric = MemoryFabric::new();
        let store = MemoryCheckpointStore::new();
        let cfg = PipelineConfig {
            linger: Duration::from_millis(1),
            ..PipelineConfig::default()
        };
        let h = Pipeline::source(c.instance.clone(), cfg)
            .enumerator(en)
            .sources(Arc::from(sf))
            .writer(Arc::new(fabric.clone()))
            .checkpoints(Arc::new(store.clone()))
            .start()
            .await
            .expect("start");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let done = store.get(&c.instance).is_some_and(|cp| {
                cp.splits.len() == 3
                    && cp.splits.values().all(|p| {
                        serde_json::from_slice::<SegmentPos>(&p.opaque).is_ok_and(|s| s.done)
                    })
            });
            if done {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "scan did not finish: {:?}",
                store.get(&c.instance)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        h.stop().await;
        assert_eq!(fabric.len(), 57, "every item exactly once");
        let segs: std::collections::BTreeSet<u32> = fake
            .scans
            .lock()
            .expect("lock")
            .iter()
            .map(|s| s.0)
            .collect();
        assert_eq!(segs, [0, 1, 2].into_iter().collect());
        assert!(
            fake.scans.lock().expect("lock").len() > 3,
            "more than one page per segment"
        );
        let ev = &fabric.events()[0];
        assert_eq!(ev.ty, "io.loams.dev.flow.dynamodb.document.v1");
        assert!(ev.id.starts_with("t/"));
    }

    /// `dynamodb_unprocessed_items_retried`, against the in-memory table:
    /// writes the service leaves unprocessed are sent again until none is
    /// left, and a call never carries more than 25 writes or one key twice.
    #[tokio::test(start_paused = true)]
    async fn dynamodb_unprocessed_items_retried_in_memory() {
        let fake = Arc::new(FakeDynamo::default());
        *fake.unprocessed.lock().expect("lock") = vec![10, 4, 0, 0, 0];
        let f = DynamoDbFactory::with_api(fake.clone());
        let c = ctx(json!({ "table": "t" }));
        let SinkHandle::Plain(mut sink) = f
            .sink(&c)
            .expect("sink")
            .expect("some")
            .create()
            .expect("create")
        else {
            panic!("plain sink");
        };
        let events: Vec<CloudEvent> = (0..30)
            .map(|i| {
                CloudEvent::new(format!("e{i}"), "/s", "t")
                    .with_data("application/json", format!(r#"{{"id":"k{i}","n":{i}}}"#))
            })
            .collect();
        let ack = sink.write(EventBatch::Events(events)).await.expect("write");
        assert_eq!(ack.rows, 30);
        assert_eq!(fake.items.lock().expect("lock").len(), 30);
        assert!(
            *fake.writes.lock().expect("lock") >= 4,
            "the unprocessed writes were retried"
        );

        // Still unprocessed after the retries: Throttled, nothing lost (a
        // retry of the batch rewrites it).
        *fake.unprocessed.lock().expect("lock") = vec![1; 20];
        let e = sink
            .write(EventBatch::Events(vec![
                CloudEvent::new("x", "/s", "t")
                    .with_data("application/json", r#"{"id":"kx","n":1}"#),
            ]))
            .await
            .expect_err("throttled");
        assert!(matches!(
            e.class,
            loams_connector_sdk::ErrorClass::Throttled { .. }
        ));

        // A repeated key and a delete keep their order.
        *fake.unprocessed.lock().expect("lock") = Vec::new();
        let evs = vec![
            CloudEvent::new("a", "/s", "t").with_data("application/json", r#"{"id":"k1","n":100}"#),
            CloudEvent::new("b", "/s", "t").with_data("application/json", r#"{"id":"k1","n":101}"#),
            CloudEvent::new("c", "/s", "t")
                .with_data("application/json", r#"{"id":"k2"}"#)
                .with_extension("loamsop", "d"),
        ];
        sink.write(EventBatch::Events(evs)).await.expect("write");
        let items = fake.items.lock().expect("lock");
        assert_eq!(items["k1"]["n"], AttributeValue::N("101".into()));
        assert!(!items.contains_key("k2"));
    }

    #[tokio::test]
    async fn version_attribute_makes_writes_conditional() {
        let fake = Arc::new(FakeDynamo::default());
        let f = DynamoDbFactory::with_api(fake.clone());
        let c = ctx(json!({ "table": "t", "version_attribute": "v" }));
        let SinkHandle::Plain(mut sink) = f
            .sink(&c)
            .expect("sink")
            .expect("some")
            .create()
            .expect("create")
        else {
            panic!("plain sink");
        };
        let ev = |v: u32, n: u32| {
            CloudEvent::new(format!("e{v}"), "/s", "t").with_data(
                "application/json",
                format!(r#"{{"id":"k","v":{v},"n":{n}}}"#),
            )
        };
        sink.write(EventBatch::Events(vec![ev(2, 20)]))
            .await
            .expect("write");
        sink.write(EventBatch::Events(vec![ev(1, 10)]))
            .await
            .expect("an older replay is skipped");
        assert_eq!(
            fake.items.lock().expect("lock")["k"]["n"],
            AttributeValue::N("20".into())
        );
        let bad = CloudEvent::new("x", "/s", "t").with_data("application/json", r#"{"id":"k"}"#);
        assert_eq!(
            sink.write(EventBatch::Events(vec![bad]))
                .await
                .expect_err("no version")
                .class,
            loams_connector_sdk::ErrorClass::Poison
        );
    }
}
