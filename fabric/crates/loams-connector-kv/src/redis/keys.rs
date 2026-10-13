//! The keyspace source: `SCAN` with a `MATCH` pattern, then a typed read of
//! each key (`GET`, `HGETALL`, `LRANGE`, `SMEMBERS`, `ZRANGE … WITHSCORES`),
//! pipelined. Each key becomes a `document` event whose id is the database,
//! the key and a digest of its value: a key re-read unchanged has the same id
//! (`ingest` drops it), a changed one is a new event.
//!
//! One split, `keyspace`. The position is the `SCAN` cursor of the page being
//! read (and whether the pass has finished); `SCAN` returns every key present
//! for the whole pass at least once, from any cursor, so a restart from the
//! page's cursor loses nothing and re-reads at most one page. Without
//! `poll_interval_ms` a pass runs once (a bounded source); with it, a new
//! pass starts that long after the last one finished.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position,
    Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use redis::Value;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{classify, connect};
use crate::config::Config;
use crate::util::digest16;

/// The event name of a key (the documents row of the plan's event table).
pub const EVENT: &str = "document";
/// The one split of a keyspace source.
pub const SPLIT: &str = "keyspace";

#[derive(Debug, Clone)]
pub struct ScanSettings {
    pub pattern: String,
    pub count: usize,
    pub poll_interval: Option<Duration>,
}

impl ScanSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        Ok(Self {
            pattern: cfg.str("scan_match")?.unwrap_or_else(|| "*".to_owned()),
            count: cfg.u64("scan_count")?.unwrap_or(100).clamp(1, 10_000) as usize,
            poll_interval: cfg.millis("poll_interval_ms")?,
        })
    }
}

/// The single `keyspace` split.
#[derive(Debug)]
pub struct ScanEnumerator;

#[async_trait]
impl SplitEnumerator for ScanEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(SPLIT, json!({}))])
    }
}

#[derive(Debug)]
pub struct ScanSourceFactory {
    connector: &'static str,
    ctx: ConnectorContext,
    cfg: Config,
    settings: ScanSettings,
}

impl ScanSourceFactory {
    pub fn new(
        connector: &'static str,
        ctx: ConnectorContext,
        cfg: Config,
        settings: ScanSettings,
    ) -> Self {
        Self {
            connector,
            ctx,
            cfg,
            settings,
        }
    }
}

impl SourceFactory for ScanSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(ScanSource {
            ctx: self.ctx.clone(),
            cfg: self.cfg.clone(),
            settings: self.settings.clone(),
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            conn: None,
            db: 0,
            state: ScanPos::default(),
            next_cursor: 0,
            pending: VecDeque::new(),
            finished_at: None,
        }))
    }
}

/// The position of a keyspace source (its opaque bytes are this, as JSON).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPos {
    /// The `SCAN` pass (0 for the first).
    pub pass: u64,
    /// The cursor of the page being read: a restart re-reads from here.
    pub cursor: u64,
    /// The pass has finished.
    pub done: bool,
}

impl ScanPos {
    pub fn to_position(self) -> Position {
        let display = if self.done {
            format!("{SPLIT} pass {} done", self.pass)
        } else {
            format!("{SPLIT} pass {} at cursor {}", self.pass, self.cursor)
        };
        Position::new(serde_json::to_vec(&self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Option<ScanPos> {
        serde_json::from_slice(&p.opaque).ok()
    }
}

#[derive(Debug)]
struct ScanSource {
    ctx: ConnectorContext,
    cfg: Config,
    settings: ScanSettings,
    envelope: EnvelopeBuilder,
    conn: Option<MultiplexedConnection>,
    db: i64,
    /// What the next batch's position will say.
    state: ScanPos,
    /// The cursor the next `SCAN` call takes.
    next_cursor: u64,
    pending: VecDeque<String>,
    finished_at: Option<Instant>,
}

/// A key read with its type and value as JSON (`None` when it vanished or
/// has a type this source does not read).
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub kind: String,
    pub value: serde_json::Value,
}

impl ScanSource {
    async fn conn(&mut self) -> Result<MultiplexedConnection, ConnectorError> {
        if let Some(c) = &self.conn {
            return Ok(c.clone());
        }
        let c = connect(&self.ctx, &self.cfg).await?;
        self.conn = Some(c.clone());
        Ok(c)
    }

    fn lost(&mut self, e: ConnectorError) -> ConnectorError {
        if e.class.is_transient() {
            self.conn = None;
        }
        e
    }

    async fn scan_page(&mut self) -> Result<(), ConnectorError> {
        let mut c = self.conn().await?;
        let reply: Value = redis::cmd("SCAN")
            .arg(self.next_cursor)
            .arg("MATCH")
            .arg(&self.settings.pattern)
            .arg("COUNT")
            .arg(self.settings.count)
            .query_async(&mut c)
            .await
            .map_err(|e| self.lost(classify(&e, "scanning keys")))?;
        let (cursor, keys) = parse_scan_reply(&reply)?;
        self.state.cursor = self.next_cursor;
        self.next_cursor = cursor;
        self.pending.extend(keys);
        Ok(())
    }

    async fn read_keys(&mut self, keys: &[String]) -> Result<Vec<KeyValue>, ConnectorError> {
        let mut c = self.conn().await?;
        let mut types = redis::pipe();
        for k in keys {
            types.cmd("TYPE").arg(k);
        }
        let kinds: Vec<Value> = types
            .query_async(&mut c)
            .await
            .map_err(|e| self.lost(classify(&e, "reading key types")))?;
        let mut reads = redis::pipe();
        let mut read = Vec::new();
        for (k, kind) in keys.iter().zip(kinds) {
            let kind = match &kind {
                Value::SimpleString(s) => s.clone(),
                Value::Okay => "ok".to_owned(),
                other => String::from_utf8(bytes(other).unwrap_or_default()).unwrap_or_default(),
            };
            match kind.as_str() {
                "string" => reads.cmd("GET").arg(k),
                "hash" => reads.cmd("HGETALL").arg(k),
                "list" => reads.cmd("LRANGE").arg(k).arg(0).arg(-1),
                "set" => reads.cmd("SMEMBERS").arg(k),
                "zset" => reads.cmd("ZRANGE").arg(k).arg(0).arg(-1).arg("WITHSCORES"),
                // `none` (deleted since SCAN), streams and module types are
                // not documents.
                _ => continue,
            };
            read.push((k.clone(), kind));
        }
        if read.is_empty() {
            return Ok(Vec::new());
        }
        let values: Vec<Value> = reads
            .query_async(&mut c)
            .await
            .map_err(|e| self.lost(classify(&e, "reading keys")))?;
        Ok(read
            .into_iter()
            .zip(values)
            .filter_map(|((key, kind), v)| {
                let value = value_json(&kind, &v)?;
                Some(KeyValue { key, kind, value })
            })
            .collect())
    }

    fn event(&self, kv: &KeyValue) -> CloudEvent {
        key_event(&self.envelope, self.db, kv)
    }
}

/// The event a key becomes.
pub fn key_event(envelope: &EnvelopeBuilder, db: i64, kv: &KeyValue) -> CloudEvent {
    let data = json!({ "key": kv.key, "type": kv.kind, "value": kv.value });
    let body = serde_json::to_vec(&data).unwrap_or_default();
    let digest = digest16(&[kv.key.as_bytes(), &body]);
    envelope
        .event(
            EVENT,
            &format!("db{db}"),
            format!("{db}/{}@{digest}", kv.key),
        )
        .with_subject(kv.key.clone())
        .with_data("application/json", body)
}

fn bytes(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::BulkString(b) => Some(b.clone()),
        Value::SimpleString(s) => Some(s.as_bytes().to_vec()),
        Value::VerbatimString { text, .. } => Some(text.as_bytes().to_vec()),
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::Double(d) => Some(d.to_string().into_bytes()),
        _ => None,
    }
}

/// A value as JSON: UTF-8 text as a string, anything else as
/// `{"$base64": …}`.
pub fn text_json(b: &[u8]) -> serde_json::Value {
    match std::str::from_utf8(b) {
        Ok(s) => json!(s),
        Err(_) => {
            use base64::Engine as _;
            json!({ "$base64": base64::engine::general_purpose::STANDARD.encode(b) })
        }
    }
}

/// A typed read's reply as JSON, in a stable order (hash fields and set
/// members sorted, so the id digest does not depend on the server's order).
pub fn value_json(kind: &str, v: &Value) -> Option<serde_json::Value> {
    let items = || -> Vec<Vec<u8>> {
        match v {
            Value::Array(a) | Value::Set(a) => a.iter().filter_map(bytes).collect(),
            _ => Vec::new(),
        }
    };
    match kind {
        "string" => match v {
            Value::Nil => None,
            other => bytes(other).map(|b| text_json(&b)),
        },
        "hash" => {
            let mut pairs: Vec<(String, serde_json::Value)> = match v {
                Value::Map(kv) => kv
                    .iter()
                    .filter_map(|(k, v)| {
                        Some((
                            String::from_utf8_lossy(&bytes(k)?).into_owned(),
                            text_json(&bytes(v)?),
                        ))
                    })
                    .collect(),
                _ => items()
                    .chunks(2)
                    .filter_map(|p| {
                        Some((
                            String::from_utf8_lossy(p.first()?).into_owned(),
                            text_json(p.get(1)?),
                        ))
                    })
                    .collect(),
            };
            if pairs.is_empty() {
                return None;
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Some(serde_json::Value::Object(pairs.into_iter().collect()))
        }
        "list" => Some(serde_json::Value::Array(
            items().iter().map(|b| text_json(b)).collect(),
        )),
        "set" => {
            let mut members = items();
            members.sort();
            Some(serde_json::Value::Array(
                members.iter().map(|b| text_json(b)).collect(),
            ))
        }
        "zset" => {
            let flat = match v {
                Value::Array(a) => a.clone(),
                _ => Vec::new(),
            };
            let mut out = Vec::new();
            for pair in flat.chunks(2) {
                match pair {
                    [Value::Array(inner)] if inner.len() == 2 => {
                        out.push(json!([text_json(&bytes(&inner[0])?), score(&inner[1])?]));
                    }
                    [m, s] => out.push(json!([text_json(&bytes(m)?), score(s)?])),
                    _ => return None,
                }
            }
            Some(serde_json::Value::Array(out))
        }
        _ => None,
    }
}

fn score(v: &Value) -> Option<serde_json::Value> {
    let f = match v {
        Value::Double(d) => *d,
        other => String::from_utf8(bytes(other)?).ok()?.parse::<f64>().ok()?,
    };
    serde_json::Number::from_f64(f).map(serde_json::Value::Number)
}

/// `SCAN`'s reply: `[cursor, [key, …]]`.
pub fn parse_scan_reply(v: &Value) -> Result<(u64, Vec<String>), ConnectorError> {
    let bad = || ConnectorError::fatal("unexpected SCAN reply from the server");
    let Value::Array(parts) = v else {
        return Err(bad());
    };
    let cursor = parts
        .first()
        .and_then(bytes)
        .and_then(|b| String::from_utf8(b).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(bad)?;
    let keys = match parts.get(1) {
        Some(Value::Array(k)) => k
            .iter()
            .filter_map(bytes)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect(),
        _ => return Err(bad()),
    };
    Ok((cursor, keys))
}

#[async_trait]
impl Source for ScanSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let url = self.cfg.required_str("url")?;
        self.db = match self.cfg.u64("database")? {
            Some(d) => i64::try_from(d).unwrap_or(0),
            None => super::Endpoint::parse(&url)?.db,
        };
        if let Some(p) = pos.as_ref().and_then(ScanPos::from_position) {
            self.state = p;
            self.next_cursor = p.cursor;
            if p.done {
                self.finished_at = Some(Instant::now());
            }
        }
        self.conn().await?;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state.done {
            match (self.settings.poll_interval, self.finished_at) {
                (Some(every), Some(at)) if at.elapsed() >= every => {
                    self.state = ScanPos {
                        pass: self.state.pass + 1,
                        cursor: 0,
                        done: false,
                    };
                    self.next_cursor = 0;
                    self.finished_at = None;
                }
                _ => return Ok(None),
            }
        }
        if self.pending.is_empty() {
            self.scan_page().await?;
        }
        let take = budget.max_rows.min(self.pending.len());
        let keys: Vec<String> = self.pending.drain(..take).collect();
        let values = self.read_keys(&keys).await?;
        let mut events = Vec::with_capacity(values.len());
        let mut size = 0usize;
        let mut taken = 0usize;
        for kv in &values {
            let ev = self.event(kv);
            let n = ev.size_bytes();
            if size + n > budget.max_bytes {
                if events.is_empty() {
                    // Larger than a whole batch (R4.7): skip it, and let the
                    // pipeline dead-letter the error.
                    let rest: Vec<String> = keys
                        .iter()
                        .skip_while(|k| **k != kv.key)
                        .skip(1)
                        .cloned()
                        .collect();
                    for k in rest.into_iter().rev() {
                        self.pending.push_front(k);
                    }
                    return Err(ConnectorError::poison(format!(
                        "key {} is {n} bytes, over the batch budget of {}",
                        kv.key, budget.max_bytes
                    )));
                }
                break;
            }
            size += n;
            taken += 1;
            events.push(ev);
        }
        if taken < values.len() {
            // Put back the keys that did not fit, in order.
            let first_left = &values[taken].key;
            let rest: Vec<String> = keys
                .iter()
                .skip_while(|k| *k != first_left)
                .cloned()
                .collect();
            for k in rest.into_iter().rev() {
                self.pending.push_front(k);
            }
        }
        if self.pending.is_empty() {
            self.state.cursor = self.next_cursor;
            if self.next_cursor == 0 {
                self.state.done = true;
                self.finished_at = Some(Instant::now());
            }
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.state.to_position(),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.conn = None;
        Ok(())
    }
}

/// A keyspace event's data, for a sink that wants the value back.
pub fn document_value(ev: &CloudEvent) -> Option<(String, serde_json::Value)> {
    let v: serde_json::Value = serde_json::from_slice(&ev.data).ok()?;
    Some((v.get("key")?.as_str()?.to_owned(), v.get("value")?.clone()))
}

#[cfg(test)]
mod tests {
    use loams_connector_sdk::InstanceKey;

    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn scan_reply_parses() {
        let r = Value::Array(vec![bulk("17"), Value::Array(vec![bulk("a"), bulk("b")])]);
        assert_eq!(
            parse_scan_reply(&r).expect("scan"),
            (17, vec!["a".into(), "b".into()])
        );
        assert!(parse_scan_reply(&Value::Nil).is_err());
    }

    #[test]
    fn typed_values_are_stable_json() {
        let hash = Value::Array(vec![bulk("b"), bulk("2"), bulk("a"), bulk("1")]);
        assert_eq!(value_json("hash", &hash), Some(json!({"a": "1", "b": "2"})));
        let set = Value::Array(vec![bulk("y"), bulk("x")]);
        assert_eq!(value_json("set", &set), Some(json!(["x", "y"])));
        let z = Value::Array(vec![bulk("m"), bulk("1.5")]);
        assert_eq!(value_json("zset", &z), Some(json!([["m", 1.5]])));
        assert_eq!(value_json("string", &Value::Nil), None);
        assert_eq!(
            value_json("string", &Value::BulkString(vec![0xff])),
            Some(json!({"$base64": "/w=="}))
        );
    }

    #[test]
    fn key_ids_change_only_with_the_value() {
        let env = EnvelopeBuilder::new("valkey", &InstanceKey::new(1, "i"));
        let a = KeyValue {
            key: "k".into(),
            kind: "string".into(),
            value: json!("v1"),
        };
        let b = KeyValue {
            value: json!("v2"),
            ..a.clone()
        };
        let ea = key_event(&env, 0, &a);
        assert_eq!(ea.id, key_event(&env, 0, &a).id);
        assert_ne!(ea.id, key_event(&env, 0, &b).id);
        assert!(ea.id.starts_with("0/k@"));
        assert_eq!(ea.subject.as_deref(), Some("k"));
        assert_eq!(ea.ty, "io.loams.dev.flow.valkey.document.v1");
        assert_eq!(document_value(&ea), Some(("k".to_owned(), json!("v1"))));
    }

    #[test]
    fn positions_roundtrip() {
        let p = ScanPos {
            pass: 2,
            cursor: 99,
            done: false,
        };
        let pos = p.to_position();
        assert_eq!(ScanPos::from_position(&pos), Some(p));
        assert_eq!(pos.display, "keyspace pass 2 at cursor 99");
    }
}
