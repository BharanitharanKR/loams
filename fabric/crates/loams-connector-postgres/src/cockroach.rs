//! CockroachDB CDC: a sinkless changefeed over the SQL connection (R0.20).
//!
//! `CREATE CHANGEFEED FOR TABLE … WITH updated, diff, resolved = '<n>s'
//! [, cursor = '<ts>']` without `INTO` streams `(table, key, value)` rows to
//! the client (it needs `kv.rangefeed.enabled`). The deprecated
//! `EXPERIMENTAL CHANGEFEED FOR` the plan named is not used. Resolved
//! timestamps are the position: once one arrives, every change at or before
//! it has been emitted, so a restart with `cursor` set to the last resolved
//! timestamp the Fabric has re-emits only what may be missing, under the
//! same ids (`<table>/<updated>/<key>`), which `ingest` dedups. Rows of the
//! initial scan (before the first resolved timestamp of a fresh feed) are
//! `loamsop = r`.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position,
    Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryStream};

use crate::config::PgConfig;
use crate::conn::{classify, connect};
use crate::table::primary_key;
use crate::types::quote_literal;

/// A changefeed split's position.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FeedPos {
    /// The last resolved timestamp the Fabric has (HLC, `wall.logical`).
    pub resolved: Option<String>,
}

impl FeedPos {
    pub fn to_position(&self) -> Position {
        let display = match &self.resolved {
            Some(r) => format!("resolved {r}"),
            None => "initial scan".to_owned(),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque).map_err(|e| {
            ConnectorError::fatal(format!("not a CockroachDB changefeed position: {e}"))
        })
    }
}

/// The changefeed statement.
pub fn changefeed_sql(tables: &[String], resolved: Duration, cursor: Option<&str>) -> String {
    let mut sql = format!(
        "CREATE CHANGEFEED FOR TABLE {} WITH updated, diff, resolved = {}",
        tables.join(", "),
        quote_literal(&format!("{}ms", resolved.as_millis().max(1)))
    );
    if let Some(c) = cursor {
        sql.push_str(&format!(", cursor = {}", quote_literal(c)));
    }
    sql
}

/// A changefeed's `BYTES` column in the simple (text) protocol: `\x` hex,
/// or the bytes themselves.
pub fn feed_bytes(text: &str) -> Vec<u8> {
    match text.strip_prefix("\\x") {
        Some(hex) if hex.len() % 2 == 0 && hex.bytes().all(|b| b.is_ascii_hexdigit()) => (0..hex
            .len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
            .collect(),
        _ => text.as_bytes().to_vec(),
    }
}

/// One changefeed row, decoded.
#[derive(Debug, Clone, PartialEq)]
pub enum FeedRow {
    Resolved(String),
    Change {
        table: String,
        key: Value,
        before: Value,
        after: Value,
        updated: String,
    },
}

pub fn parse_row(
    table: Option<&str>,
    key: Option<&str>,
    value: Option<&str>,
) -> Result<FeedRow, ConnectorError> {
    let value: Value = value
        .map(|v| serde_json::from_slice(&feed_bytes(v)))
        .transpose()
        .map_err(|e| ConnectorError::poison(format!("changefeed value: {e}")))?
        .unwrap_or(Value::Null);
    match table.filter(|t| !t.is_empty()) {
        None => value
            .get("resolved")
            .and_then(Value::as_str)
            .map(|r| FeedRow::Resolved(r.to_owned()))
            .ok_or_else(|| {
                ConnectorError::fatal("changefeed: a row with no table and no resolved timestamp")
            }),
        Some(t) => {
            let key: Value = key
                .map(|k| serde_json::from_slice(&feed_bytes(k)))
                .transpose()
                .map_err(|e| ConnectorError::poison(format!("changefeed key: {e}")))?
                .unwrap_or(Value::Null);
            Ok(FeedRow::Change {
                table: t.to_owned(),
                key,
                before: value.get("before").cloned().unwrap_or(Value::Null),
                after: value.get("after").cloned().unwrap_or(Value::Null),
                updated: value
                    .get("updated")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
        }
    }
}

#[derive(Debug)]
pub struct FeedEnumerator;

#[async_trait]
impl SplitEnumerator for FeedEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new("changefeed", Value::Null)])
    }
}

#[derive(Debug)]
pub struct FeedFactory {
    cfg: PgConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl FeedFactory {
    pub fn new(cfg: PgConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for FeedFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(FeedSource {
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            client: None,
            stream: None,
            pos: FeedPos::default(),
            keys: BTreeMap::new(),
            carry: VecDeque::new(),
        }))
    }
}

/// The changefeed source.
pub struct FeedSource {
    cfg: PgConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    client: Option<Client>,
    stream: Option<Pin<Box<SimpleQueryStream>>>,
    pos: FeedPos,
    /// Each table's key column names, to name the feed's key array.
    keys: BTreeMap<String, Vec<String>>,
    carry: VecDeque<CloudEvent>,
}

impl std::fmt::Debug for FeedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedSource")
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl FeedSource {
    fn event(&self, row: FeedRow) -> Option<CloudEvent> {
        let FeedRow::Change {
            table,
            key,
            before,
            after,
            updated,
        } = row
        else {
            return None;
        };
        let op = match (&before, &after) {
            (_, Value::Null) => "d",
            (Value::Null, _) if self.pos.resolved.is_none() => "r",
            (Value::Null, _) => "c",
            _ => "u",
        };
        let names = self.keys.get(&table);
        let key_obj = match (&key, names) {
            (Value::Array(vals), Some(names)) if vals.len() == names.len() => Value::Object(
                names
                    .iter()
                    .cloned()
                    .zip(vals.iter().cloned())
                    .collect::<Map<_, _>>(),
            ),
            _ => key.clone(),
        };
        let data = json!({
            "table": table, "op": op, "key": key_obj, "before": before, "after": after, "unchanged": [],
        });
        Some(
            self.envelope
                .event("change", &table, format!("{table}/{updated}/{key}"))
                .with_subject(table.clone())
                .with_data("application/json", data.to_string())
                .with_extension("loamsop", op)
                .with_extension("loamslsn", updated),
        )
    }
}

#[async_trait]
impl Source for FeedSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        if self.cfg.tables.is_empty() {
            return Err(ConnectorError::fatal("config: tables is required for CDC"));
        }
        self.pos = pos
            .as_ref()
            .map(FeedPos::from_position)
            .transpose()?
            .unwrap_or_default();
        let client = connect(&self.cfg, &self.ctx).await?;
        for t in &self.cfg.tables {
            self.keys
                .insert(t.name.clone(), primary_key(&client, t).await?);
        }
        let tables: Vec<String> = self.cfg.tables.iter().map(|t| t.quoted()).collect();
        let sql = changefeed_sql(&tables, self.cfg.resolved, self.pos.resolved.as_deref());
        let stream = client
            .simple_query_raw(&sql)
            .await
            .map_err(|e| classify(&e, "CREATE CHANGEFEED"))?;
        self.stream = Some(Box::pin(stream));
        self.client = Some(client);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let mut events = Vec::new();
        let mut bytes = 0;
        let start = self.pos.clone();
        let mut waited = false;
        loop {
            while let Some(e) = self.carry.front() {
                let size = e.size_bytes();
                if events.is_empty() && size > budget.max_bytes {
                    let e = self.carry.pop_front().expect("front");
                    return Err(ConnectorError::poison(format!(
                        "change {} is over the batch budget",
                        e.id
                    )));
                }
                if events.len() >= budget.max_rows || bytes + size > budget.max_bytes {
                    break;
                }
                bytes += size;
                events.push(self.carry.pop_front().expect("front"));
            }
            if !self.carry.is_empty() {
                break;
            }
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| ConnectorError::fatal("the changefeed is not open"))?;
            let msg = if waited || !events.is_empty() {
                match stream.next().now_or_never() {
                    Some(m) => m,
                    None => break,
                }
            } else {
                waited = true;
                match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                    Ok(m) => m,
                    Err(_) => break,
                }
            };
            let row = match msg {
                None => return Err(ConnectorError::retryable("the changefeed ended")),
                Some(Err(e)) => return Err(classify(&e, "changefeed")),
                Some(Ok(SimpleQueryMessage::Row(r))) => parse_row(r.get(0), r.get(1), r.get(2))?,
                Some(Ok(_)) => continue,
            };
            match row {
                FeedRow::Resolved(ts) => {
                    // Everything at or before `ts` is in this batch or
                    // earlier ones: the position moves, and the batch ends.
                    self.pos.resolved = Some(ts);
                    break;
                }
                change => {
                    if let Some(ev) = self.event(change) {
                        self.carry.push_back(ev);
                    }
                }
            }
        }
        if events.is_empty() && self.pos == start {
            return Ok(None);
        }
        // Events carried past a resolved timestamp are after it, so the
        // position (the last resolved) re-reads them after a crash.
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: self.pos.to_position(),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Dropping the client closes the connection, which cancels the feed.
        self.stream = None;
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_statement_is_a_sinkless_create_changefeed() {
        let sql = changefeed_sql(
            &["\"public\".\"orders\"".into()],
            Duration::from_secs(1),
            None,
        );
        assert_eq!(
            sql,
            "CREATE CHANGEFEED FOR TABLE \"public\".\"orders\" WITH updated, diff, resolved = '1000ms'"
        );
        assert!(!sql.contains("EXPERIMENTAL"));
        assert!(!sql.contains("INTO"));
        let sql = changefeed_sql(
            &["t".into()],
            Duration::from_millis(500),
            Some("1700000000000000000.0000000001"),
        );
        assert!(
            sql.ends_with(", cursor = '1700000000000000000.0000000001'"),
            "{sql}"
        );
    }

    #[test]
    fn rows_decode_from_hex_or_text() {
        let value = r#"{"after": {"id": 1}, "before": null, "updated": "17.0000000001"}"#;
        let hex: String = value.bytes().map(|b| format!("{b:02x}")).collect();
        let row = parse_row(
            Some("orders"),
            Some("\\x5b315d"),
            Some(&format!("\\x{hex}")),
        )
        .expect("row");
        assert_eq!(
            row,
            FeedRow::Change {
                table: "orders".into(),
                key: json!([1]),
                before: Value::Null,
                after: json!({"id": 1}),
                updated: "17.0000000001".into()
            }
        );
        assert_eq!(
            parse_row(None, None, Some(r#"{"resolved": "18.0000000000"}"#)).expect("resolved"),
            FeedRow::Resolved("18.0000000000".into())
        );
        assert!(parse_row(None, None, Some("{}")).is_err());
    }

    #[test]
    fn ops_follow_before_and_after_and_the_initial_scan() {
        let cfg = PgConfig::parse(
            crate::config::Target::Cockroach,
            &loams_connector_sdk::InstanceKey::new(1, "c"),
            &json!({"host": "h", "user": "root", "mode": "cdc", "tables": ["orders"]}),
        )
        .expect("cfg");
        let ctx = ConnectorContext::new(
            loams_connector_sdk::InstanceKey::new(1, "c"),
            json!({}),
            std::sync::Arc::new(loams_connector_sdk::StaticSecrets::default()),
            loams_connector_sdk::Scrubber::new(),
        );
        let f = FeedFactory::new(cfg, ctx, "cockroachdb");
        let mut src = FeedSource {
            envelope: EnvelopeBuilder::new(f.connector, &f.ctx.instance),
            cfg: f.cfg.clone(),
            ctx: f.ctx.clone(),
            client: None,
            stream: None,
            pos: FeedPos::default(),
            keys: [("orders".to_owned(), vec!["id".to_owned()])].into(),
            carry: VecDeque::new(),
        };
        let change = |before: Value, after: Value| FeedRow::Change {
            table: "orders".into(),
            key: json!([1]),
            before,
            after,
            updated: "17.1".into(),
        };
        let op = |e: &CloudEvent| e.extensions["loamsop"].clone();
        assert_eq!(
            op(&src
                .event(change(Value::Null, json!({"id": 1})))
                .expect("ev")),
            "r"
        );
        src.pos.resolved = Some("16.0".into());
        let ins = src
            .event(change(Value::Null, json!({"id": 1})))
            .expect("ev");
        assert_eq!(op(&ins), "c");
        assert_eq!(ins.id, "orders/17.1/[1]");
        let data: Value = serde_json::from_slice(&ins.data).expect("json");
        assert_eq!(data["key"], json!({"id": 1}));
        assert_eq!(
            op(&src
                .event(change(json!({"id": 1}), json!({"id": 1})))
                .expect("ev")),
            "u"
        );
        assert_eq!(
            op(&src
                .event(change(json!({"id": 1}), Value::Null))
                .expect("ev")),
            "d"
        );
        let pos = FeedPos {
            resolved: Some("16.0".into()),
        };
        assert_eq!(
            FeedPos::from_position(&pos.to_position()).expect("round trip"),
            pos
        );
    }
}
