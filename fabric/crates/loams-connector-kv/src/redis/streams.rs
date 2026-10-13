//! The Streams source: `XREADGROUP` in a consumer group, `XACK` only after
//! the Fabric has acknowledged the batch (`Source::committed`), and
//! `XAUTOCLAIM` for entries another consumer left pending.
//!
//! One split per stream key. The position is the id of the last entry the
//! batch carried. On (re)start the source first reads its own pending
//! entries list (`XREADGROUP … 0`): those were delivered before a crash and
//! never acknowledged, so they are delivered again (their event ids are
//! stable, `<stream>/<entry-id>`, and `ingest` drops the copies); entries at
//! or before the checkpointed position are acknowledged without being
//! re-sent. Then it reads new entries (`>`).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position,
    Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use redis::Value;
use redis::aio::MultiplexedConnection;
use serde_json::json;

use super::{classify, connect};
use crate::config::Config;
use crate::util::rfc3339_millis;

/// The event name of an entry that is not already a CloudEvent (CN1 Task
/// 13's `stream-entry`).
pub const EVENT: &str = "stream-entry";

/// A Streams entry id, `<ms>-<seq>`, ordered numerically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId {
    pub ms: u64,
    pub seq: u64,
}

impl EntryId {
    pub fn parse(s: &str) -> Option<EntryId> {
        let (ms, seq) = s.split_once('-')?;
        Some(EntryId {
            ms: ms.parse().ok()?,
            seq: seq.parse().ok()?,
        })
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// The Streams settings of an instance (CN1's `redis.config.json` keys).
#[derive(Debug, Clone)]
pub struct StreamsSettings {
    pub streams: Vec<String>,
    pub group: String,
    pub consumer: String,
    pub block: Duration,
    pub count: usize,
    pub pending_idle: Option<Duration>,
    /// `ack_mode: on-read`: `XREADGROUP … NOACK` (at most once).
    pub noack: bool,
    /// The id a new group starts from: `0` (the whole stream, the default)
    /// or `$` (only new entries).
    pub start: String,
    connector: String,
}

impl StreamsSettings {
    pub fn from_config(
        connector: &str,
        ctx: &ConnectorContext,
        cfg: &Config,
    ) -> Result<Self, ConnectorError> {
        let mut streams = cfg.strings("streams")?;
        streams.extend(cfg.strings("stream")?);
        streams.sort();
        streams.dedup();
        if streams.is_empty() {
            return Err(ConnectorError::fatal(
                "config: `stream` (or `streams`) is required",
            ));
        }
        let noack = match cfg.str("ack_mode")?.as_deref() {
            None | Some("after-ack") => false,
            Some("on-read") => true,
            Some(_) => {
                return Err(ConnectorError::fatal(
                    "config: `ack_mode` must be after-ack or on-read",
                ));
            }
        };
        Ok(Self {
            streams,
            group: cfg
                .str("consumer_group")?
                .unwrap_or_else(|| format!("loams-{}", ctx.instance.name)),
            consumer: cfg
                .str("consumer_name")?
                .unwrap_or_else(|| ctx.instance.name.clone()),
            block: cfg
                .millis("block_ms")?
                .unwrap_or(Duration::from_millis(1000))
                .min(super::MAX_BLOCK),
            count: cfg.u64("count")?.unwrap_or(100).clamp(1, 10_000) as usize,
            pending_idle: cfg.millis("pending_idle_ms")?,
            noack,
            start: cfg.str("start_id")?.unwrap_or_else(|| "0".to_owned()),
            connector: connector.to_owned(),
        })
    }
}

/// One split per configured stream.
#[derive(Debug)]
pub struct StreamsEnumerator {
    streams: Vec<String>,
}

impl StreamsEnumerator {
    pub fn new(streams: Vec<String>) -> Self {
        Self { streams }
    }
}

#[async_trait]
impl SplitEnumerator for StreamsEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(self
            .streams
            .iter()
            .map(|s| Split::new(s.as_str(), json!({ "stream": s })))
            .collect())
    }
}

#[derive(Debug)]
pub struct StreamsSourceFactory {
    ctx: ConnectorContext,
    cfg: Config,
    settings: StreamsSettings,
}

impl StreamsSourceFactory {
    pub fn new(ctx: ConnectorContext, cfg: Config, settings: StreamsSettings) -> Self {
        Self { ctx, cfg, settings }
    }
}

impl SourceFactory for StreamsSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(StreamsSource {
            ctx: self.ctx.clone(),
            cfg: self.cfg.clone(),
            settings: self.settings.clone(),
            envelope: EnvelopeBuilder::new(self.settings.connector.clone(), &self.ctx.instance),
            conn: None,
            stream: String::new(),
            floor: None,
            reading_pending: true,
            pending_after: EntryId { ms: 0, seq: 0 },
            carry: VecDeque::new(),
            delivered: VecDeque::new(),
            claim_cursor: "0-0".to_owned(),
            last_claim: None,
        }))
    }
}

/// An entry as read: its id and field/value pairs (`None` for an entry
/// deleted while it was pending).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    pub id: EntryId,
    pub fields: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

#[derive(Debug)]
struct StreamsSource {
    ctx: ConnectorContext,
    cfg: Config,
    settings: StreamsSettings,
    envelope: EnvelopeBuilder,
    conn: Option<MultiplexedConnection>,
    stream: String,
    /// The checkpointed position: pending entries at or before it were
    /// already in the Fabric.
    floor: Option<EntryId>,
    reading_pending: bool,
    pending_after: EntryId,
    /// Entries read but not yet returned (a batch larger than the budget).
    carry: VecDeque<RawEntry>,
    /// Entries returned and not yet acknowledged upstream, in order.
    delivered: VecDeque<EntryId>,
    claim_cursor: String,
    last_claim: Option<Instant>,
}

impl StreamsSource {
    async fn conn(&mut self) -> Result<MultiplexedConnection, ConnectorError> {
        if let Some(c) = &self.conn {
            return Ok(c.clone());
        }
        let mut c = connect(&self.ctx, &self.cfg).await?;
        let created: Result<Value, _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream)
            .arg(&self.settings.group)
            .arg(&self.settings.start)
            .arg("MKSTREAM")
            .query_async(&mut c)
            .await;
        match created {
            Ok(_) => {}
            Err(e) if e.code() == Some("BUSYGROUP") => {}
            Err(e) => return Err(classify(&e, "creating the consumer group")),
        }
        self.conn = Some(c.clone());
        Ok(c)
    }

    /// Fill `carry` from the server: own pending entries first, then
    /// entries claimed from idle consumers, then new ones.
    async fn fill(&mut self, count: usize) -> Result<(), ConnectorError> {
        let mut c = self.conn().await?;
        if self.reading_pending && !self.settings.noack {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(&self.settings.group)
                .arg(&self.settings.consumer)
                .arg("COUNT")
                .arg(count)
                .arg("STREAMS")
                .arg(&self.stream)
                .arg(self.pending_after.to_string());
            let reply: Value = cmd
                .query_async(&mut c)
                .await
                .map_err(|e| self.lost(classify(&e, "reading pending entries")))?;
            let entries = parse_read_reply(&reply)?;
            if entries.is_empty() {
                self.reading_pending = false;
            } else {
                if let Some(last) = entries.last() {
                    self.pending_after = last.id;
                }
                let mut stale = Vec::new();
                for e in entries {
                    if self.floor.is_some_and(|f| e.id <= f) || e.fields.is_none() {
                        stale.push(e.id);
                    } else {
                        self.carry.push_back(e);
                    }
                }
                self.ack(&mut c, &stale).await?;
                return Ok(());
            }
        }
        if let Some(idle) = self.settings.pending_idle
            && !self.settings.noack
            && self.last_claim.is_none_or(|t| t.elapsed() >= idle)
        {
            let reply: Value = redis::cmd("XAUTOCLAIM")
                .arg(&self.stream)
                .arg(&self.settings.group)
                .arg(&self.settings.consumer)
                .arg(u64::try_from(idle.as_millis()).unwrap_or(u64::MAX))
                .arg(&self.claim_cursor)
                .arg("COUNT")
                .arg(count)
                .query_async(&mut c)
                .await
                .map_err(|e| self.lost(classify(&e, "claiming idle entries")))?;
            let (next, claimed) = parse_autoclaim_reply(&reply)?;
            if next == "0-0" {
                self.last_claim = Some(Instant::now());
            }
            self.claim_cursor = next;
            let live: Vec<RawEntry> = claimed.into_iter().filter(|e| e.fields.is_some()).collect();
            if !live.is_empty() {
                self.carry.extend(live);
                return Ok(());
            }
        }
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.settings.group)
            .arg(&self.settings.consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(u64::try_from(self.settings.block.as_millis()).unwrap_or(u64::MAX));
        if self.settings.noack {
            cmd.arg("NOACK");
        }
        cmd.arg("STREAMS").arg(&self.stream).arg(">");
        let reply: Value = cmd
            .query_async(&mut c)
            .await
            .map_err(|e| self.lost(classify(&e, "reading new entries")))?;
        let entries = parse_read_reply(&reply)?;
        if entries.is_empty() {
            self.ctx
                .observe
                .lag(&self.ctx.instance, None, Duration::ZERO);
        }
        self.carry
            .extend(entries.into_iter().filter(|e| e.fields.is_some()));
        Ok(())
    }

    /// A broken connection is dropped, so the next call reconnects.
    fn lost(&mut self, e: ConnectorError) -> ConnectorError {
        if e.class.is_transient() {
            self.conn = None;
        }
        e
    }

    async fn ack(
        &mut self,
        c: &mut MultiplexedConnection,
        ids: &[EntryId],
    ) -> Result<(), ConnectorError> {
        if ids.is_empty() || self.settings.noack {
            return Ok(());
        }
        let mut cmd = redis::cmd("XACK");
        cmd.arg(&self.stream).arg(&self.settings.group);
        for id in ids {
            cmd.arg(id.to_string());
        }
        let r: Result<Value, _> = cmd.query_async(c).await;
        r.map(|_| ())
            .map_err(|e| self.lost(classify(&e, "acknowledging entries")))
    }

    fn event(&self, e: &RawEntry) -> CloudEvent {
        entry_event(&self.envelope, &self.stream, e)
    }
}

/// The event an entry becomes: pass-through when its fields are a
/// CloudEvent in binary mode (`ce_id`, `ce_source`, `ce_type`,
/// `ce_specversion`, the payload under `data`, as the `XADD` sink writes
/// it), else a `stream-entry` whose data is a JSON object of the fields.
pub fn entry_event(envelope: &EnvelopeBuilder, stream: &str, e: &RawEntry) -> CloudEvent {
    let fields = e.fields.as_deref().unwrap_or_default();
    let data = fields
        .iter()
        .find(|(k, _)| k == b"data")
        .map(|(_, v)| Bytes::copy_from_slice(v))
        .unwrap_or_default();
    let headers = fields.iter().filter_map(|(k, v)| {
        let k = std::str::from_utf8(k).ok()?;
        (k != "data").then_some((k, v.as_slice()))
    });
    if let Some(ev) = envelope.from_binary(headers, data) {
        return ev;
    }
    let mut obj = serde_json::Map::new();
    for (k, v) in fields {
        let key = String::from_utf8_lossy(k).into_owned();
        let value = match std::str::from_utf8(v) {
            Ok(s) => json!(s),
            Err(_) => {
                use base64::Engine as _;
                json!({ "$base64": base64::engine::general_purpose::STANDARD.encode(v) })
            }
        };
        obj.insert(key, value);
    }
    envelope
        .event(EVENT, stream, format!("{stream}/{}", e.id))
        .with_time(rfc3339_millis(i64::try_from(e.id.ms).unwrap_or(i64::MAX)))
        .with_data(
            "application/json",
            serde_json::to_vec(&obj).unwrap_or_default(),
        )
}

#[async_trait]
impl Source for StreamsSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.stream = split.id.as_str().to_owned();
        self.floor = pos
            .as_ref()
            .and_then(|p| std::str::from_utf8(&p.opaque).ok())
            .and_then(EntryId::parse);
        self.reading_pending = true;
        self.pending_after = EntryId { ms: 0, seq: 0 };
        self.conn().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.carry.is_empty() {
            let count = self.settings.count.min(budget.max_rows.max(1));
            self.fill(count).await?;
        }
        let mut events = Vec::new();
        let mut ids = Vec::new();
        let mut bytes = 0usize;
        while let Some(entry) = self.carry.front() {
            if events.len() >= budget.max_rows {
                break;
            }
            let ev = self.event(entry);
            let size = ev.size_bytes();
            if bytes + size > budget.max_bytes {
                if events.is_empty() {
                    // One entry larger than a whole batch: it can never be
                    // read (R4.7). Skip it, acknowledge it, and let the
                    // pipeline dead-letter the error.
                    let Some(entry) = self.carry.pop_front() else {
                        break;
                    };
                    let mut c = self.conn().await?;
                    self.ack(&mut c, &[entry.id]).await?;
                    return Err(ConnectorError::poison(format!(
                        "entry {}/{} is {size} bytes, over the batch budget of {}",
                        self.stream, entry.id, budget.max_bytes
                    )));
                }
                break;
            }
            bytes += size;
            ids.push(entry.id);
            events.push(ev);
            self.carry.pop_front();
        }
        let Some(&last) = ids.last() else {
            return Ok(None);
        };
        if !self.settings.noack {
            self.delivered.extend(ids);
        }
        let lag_ms =
            crate::util::now_millis().saturating_sub(i64::try_from(last.ms).unwrap_or(i64::MAX));
        self.ctx.observe.lag(
            &self.ctx.instance,
            None,
            Duration::from_millis(u64::try_from(lag_ms).unwrap_or(0)),
        );
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(
                Bytes::from(last.to_string()),
                format!("{}@{last}", self.stream),
            ),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some(upto) = std::str::from_utf8(&upto.opaque)
            .ok()
            .and_then(EntryId::parse)
        else {
            return Ok(());
        };
        self.floor = Some(self.floor.map_or(upto, |f| f.max(upto)));
        let mut acked = Vec::new();
        while self.delivered.front().is_some_and(|id| *id <= upto) {
            if let Some(id) = self.delivered.pop_front() {
                acked.push(id);
            }
        }
        if acked.is_empty() {
            return Ok(());
        }
        let mut c = self.conn().await?;
        if let Err(e) = self.ack(&mut c, &acked).await {
            // Retried by the pipeline: put them back so nothing is left
            // pending for good.
            for id in acked.into_iter().rev() {
                self.delivered.push_front(id);
            }
            return Err(e);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.conn = None;
        Ok(())
    }
}

fn bytes_of(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(b) => Some(b.clone()),
        Value::SimpleString(s) => Some(s.as_bytes().to_vec()),
        Value::VerbatimString { text, .. } => Some(text.as_bytes().to_vec()),
        Value::Int(i) => Some(i.to_string().into_bytes()),
        _ => None,
    }
}

fn str_of(v: &Value) -> Option<String> {
    bytes_of(v).and_then(|b| String::from_utf8(b).ok())
}

fn bad_reply(what: &str) -> ConnectorError {
    ConnectorError::fatal(format!("unexpected {what} reply from the server"))
}

/// Parse one entry, `[id, [field, value, …] | nil]`.
fn parse_entry(v: &Value) -> Result<RawEntry, ConnectorError> {
    let Value::Array(parts) = v else {
        return Err(bad_reply("stream entry"));
    };
    let id = parts
        .first()
        .and_then(str_of)
        .and_then(|s| EntryId::parse(&s))
        .ok_or_else(|| bad_reply("stream entry id"))?;
    let fields = match parts.get(1) {
        None | Some(Value::Nil) => None,
        Some(Value::Array(kv)) => {
            let mut out = Vec::with_capacity(kv.len() / 2);
            for pair in kv.chunks(2) {
                let (Some(k), Some(v)) = (
                    pair.first().and_then(bytes_of),
                    pair.get(1).and_then(bytes_of),
                ) else {
                    return Err(bad_reply("stream entry field"));
                };
                out.push((k, v));
            }
            Some(out)
        }
        Some(Value::Map(kv)) => {
            let mut out = Vec::with_capacity(kv.len());
            for (k, v) in kv {
                let (Some(k), Some(v)) = (bytes_of(k), bytes_of(v)) else {
                    return Err(bad_reply("stream entry field"));
                };
                out.push((k, v));
            }
            Some(out)
        }
        Some(_) => return Err(bad_reply("stream entry fields")),
    };
    Ok(RawEntry { id, fields })
}

fn parse_entries(v: &Value) -> Result<Vec<RawEntry>, ConnectorError> {
    match v {
        Value::Nil => Ok(Vec::new()),
        Value::Array(items) => items.iter().map(parse_entry).collect(),
        _ => Err(bad_reply("stream entries")),
    }
}

/// `XREADGROUP`'s reply for one stream: nil, or `[[stream, entries]]` (RESP2)
/// or `{stream: entries}` (RESP3).
pub fn parse_read_reply(v: &Value) -> Result<Vec<RawEntry>, ConnectorError> {
    match v {
        Value::Nil => Ok(Vec::new()),
        Value::Array(streams) => {
            let mut out = Vec::new();
            for s in streams {
                let Value::Array(pair) = s else {
                    return Err(bad_reply("XREADGROUP"));
                };
                out.extend(parse_entries(pair.get(1).unwrap_or(&Value::Nil))?);
            }
            Ok(out)
        }
        Value::Map(streams) => {
            let mut out = Vec::new();
            for (_, entries) in streams {
                out.extend(parse_entries(entries)?);
            }
            Ok(out)
        }
        _ => Err(bad_reply("XREADGROUP")),
    }
}

/// `XAUTOCLAIM`'s reply: `[next-cursor, entries, deleted-ids]`.
pub fn parse_autoclaim_reply(v: &Value) -> Result<(String, Vec<RawEntry>), ConnectorError> {
    let Value::Array(parts) = v else {
        return Err(bad_reply("XAUTOCLAIM"));
    };
    let next = parts
        .first()
        .and_then(str_of)
        .ok_or_else(|| bad_reply("XAUTOCLAIM cursor"))?;
    let entries = parse_entries(parts.get(1).unwrap_or(&Value::Nil))?;
    Ok((next, entries))
}

#[cfg(test)]
mod tests {
    use loams_connector_sdk::InstanceKey;

    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn entry_ids_order_numerically() {
        let a = EntryId::parse("9-1").expect("id");
        let b = EntryId::parse("10-0").expect("id");
        assert!(a < b);
        assert_eq!(b.to_string(), "10-0");
        assert!(EntryId::parse("x").is_none());
    }

    #[test]
    fn read_replies_parse_in_resp2_and_resp3() {
        let entries = Value::Array(vec![
            Value::Array(vec![bulk("1-0"), Value::Array(vec![bulk("k"), bulk("v")])]),
            Value::Array(vec![bulk("2-0"), Value::Nil]),
        ]);
        let resp2 = Value::Array(vec![Value::Array(vec![bulk("s"), entries.clone()])]);
        let got = parse_read_reply(&resp2).expect("resp2");
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[0].fields.as_deref(),
            Some(&[(b"k".to_vec(), b"v".to_vec())][..])
        );
        assert_eq!(got[1].fields, None);
        let resp3 = Value::Map(vec![(bulk("s"), entries)]);
        assert_eq!(parse_read_reply(&resp3).expect("resp3"), got);
        assert!(parse_read_reply(&Value::Nil).expect("nil").is_empty());
        let claim = Value::Array(vec![
            bulk("0-0"),
            Value::Array(vec![]),
            Value::Array(vec![]),
        ]);
        assert_eq!(parse_autoclaim_reply(&claim).expect("claim").0, "0-0");
    }

    #[test]
    fn entries_pass_through_or_wrap() {
        let env = EnvelopeBuilder::new("redis-streams", &InstanceKey::new(1, "i"));
        let plain = RawEntry {
            id: EntryId { ms: 1_000, seq: 2 },
            fields: Some(vec![(b"name".to_vec(), b"ada".to_vec())]),
        };
        let ev = entry_event(&env, "orders", &plain);
        assert_eq!(ev.id, "orders/1000-2");
        assert_eq!(ev.ty, "io.loams.dev.flow.redis-streams.stream-entry.v1");
        assert_eq!(ev.source, "/connectors/i/orders");
        assert_eq!(&ev.data[..], br#"{"name":"ada"}"#);
        let ce = RawEntry {
            id: EntryId { ms: 5, seq: 0 },
            fields: Some(vec![
                (b"ce_id".to_vec(), b"abc".to_vec()),
                (b"ce_source".to_vec(), b"/up".to_vec()),
                (b"ce_type".to_vec(), b"t.v1".to_vec()),
                (b"ce_specversion".to_vec(), b"1.0".to_vec()),
                (b"data".to_vec(), b"payload".to_vec()),
            ]),
        };
        let ev = entry_event(&env, "orders", &ce);
        assert_eq!(
            (ev.id.as_str(), ev.source.as_str(), ev.ty.as_str()),
            ("abc", "/up", "t.v1")
        );
        assert_eq!(&ev.data[..], b"payload");
    }
}
