//! The native ClickHouse sink (OR-8, replacing Iggy's `clickhouse_sink` as
//! the default; R17.3).
//!
//! One `INSERT … FORMAT ArrowStream` per Arrow batch (the bulk path, D356),
//! or `FORMAT JSONEachRow` with one row per event's `data`, over ClickHouse's
//! HTTP interface, with `insert_deduplication_token` = the batch's id
//! ([`crate::rows::batch_id`]: a digest of its event ids, the same on every
//! retry of the batch). ClickHouse drops a retried block whose token it has
//! already seen within the table's deduplication window, so the sink is
//! `effectively_once` within that window (OR-7): replicated MergeTree tables
//! deduplicate by default; a plain MergeTree needs the table setting
//! `non_replicated_deduplication_window`.
//!
//! The same sink writes to Loams House, which serves ClickHouse's HTTP
//! interface: House is an extra target, tested after the real ClickHouse
//! (OR-2).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, Uri};
use loams_connector_sdk::http::{HttpClient, classify_status};
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ErrorClass, EventBatch, Secret, SecretResolver, Sink,
    SinkAck, SinkFactory, SinkHandle,
};
use serde::Deserialize;

use crate::common::{base_url, secret, snippet, table_name, text};
use crate::rows::{batch_id, event_objects, ipc_stream};

/// The ★ manifest id (`connectors/registry/clickhouse.yaml`).
pub const CONNECTOR: &str = "clickhouse";

/// The instance config of a ClickHouse sink.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// The HTTP interface, `https://host:8443` (or `http://host:8123`).
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    /// The target table (in `database`).
    pub table: String,
    #[serde(default = "default_user")]
    pub user: String,
    /// The password's secret **name**.
    #[serde(default)]
    pub password: Option<String>,
    /// Send `insert_deduplication_token` (on by default; the delivery
    /// guarantee rests on it).
    #[serde(default = "yes")]
    pub dedup: bool,
    /// Extra ClickHouse settings sent with every insert (`max_insert_block_size`, …).
    #[serde(default)]
    pub settings: std::collections::BTreeMap<String, String>,
    /// Per-request timeout.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub pipeline: Option<serde_json::Value>,
}

fn default_db() -> String {
    "default".to_owned()
}

fn default_user() -> String {
    "default".to_owned()
}

fn yes() -> bool {
    true
}

fn default_timeout() -> u64 {
    120
}

impl ClickHouseConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let cfg: Self = serde_json::from_value(config.clone())
            .map_err(|e| ConnectorError::fatal(format!("clickhouse config: {e}")))?;
        table_name(&cfg.database)?;
        table_name(&cfg.table)?;
        for k in cfg.settings.keys() {
            if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(ConnectorError::fatal(format!(
                    "setting {k:?} is not a name"
                )));
            }
        }
        Ok(cfg)
    }
}

/// The ClickHouse sink: one insert per batch.
pub struct ClickHouseSink {
    cfg: ClickHouseConfig,
    client: HttpClient,
    base: Uri,
    password: Option<Secret>,
    secrets: Arc<dyn SecretResolver>,
    resolved: bool,
}

impl std::fmt::Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink")
            .field("table", &self.cfg.table)
            .finish_non_exhaustive()
    }
}

impl ClickHouseSink {
    fn insert_uri(&self, format: &str, token: Option<&str>) -> Result<Uri, ConnectorError> {
        let query = format!(
            "INSERT INTO {}.{} FORMAT {format}",
            self.cfg.database, self.cfg.table
        );
        let mut params = vec![
            ("database".to_owned(), self.cfg.database.clone()),
            ("query".to_owned(), query),
            // A synchronous insert: the reply means the block is written.
            ("async_insert".to_owned(), "0".to_owned()),
        ];
        if let Some(t) = token {
            params.push(("insert_deduplicate".to_owned(), "1".to_owned()));
            params.push(("insert_deduplication_token".to_owned(), t.to_owned()));
        }
        for (k, v) in &self.cfg.settings {
            params.push((k.clone(), v.clone()));
        }
        let qs: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect();
        let b = self.base.to_string();
        format!("{}/?{}", b.trim_end_matches('/'), qs.join("&"))
            .parse()
            .map_err(|e| ConnectorError::fatal(format!("insert URL: {e}")))
    }

    async fn insert(
        &mut self,
        format: &str,
        body: Bytes,
        token: &str,
    ) -> Result<(), ConnectorError> {
        if !self.resolved {
            self.password = secret(&*self.secrets, self.cfg.password.as_ref()).await?;
            self.resolved = true;
        }
        let uri = self.insert_uri(format, self.cfg.dedup.then_some(token))?;
        let mut req = Request::post(uri)
            .header("content-type", "application/octet-stream")
            .header("x-clickhouse-user", &self.cfg.user);
        if let Some(p) = &self.password {
            let mut v = http::HeaderValue::from_str(&text(p, "the ClickHouse password")?).map_err(
                |_| ConnectorError::auth("the ClickHouse password is not a header value"),
            )?;
            v.set_sensitive(true);
            req = req.header("x-clickhouse-key", v);
        }
        let req = req
            .body(body)
            .map_err(|e| ConnectorError::fatal(format!("insert request: {e}")))?;
        let resp = self.client.send(req).await?;
        if resp.status().is_success() {
            return Ok(());
        }
        Err(classify(resp.status(), resp.headers(), resp.body()))
    }
}

/// `application/x-www-form-urlencoded`-style percent encoding of a query
/// component.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A failed insert's class: by ClickHouse's exception code (the
/// `X-ClickHouse-Exception-Code` header, else `Code: N.` in the body), then
/// by HTTP status.
pub fn classify(
    status: http::StatusCode,
    headers: &http::HeaderMap,
    body: &[u8],
) -> ConnectorError {
    let code = headers
        .get("x-clickhouse-exception-code")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u32>().ok())
        .or_else(|| {
            let s = String::from_utf8_lossy(body);
            let at = s.find("Code: ")? + 6;
            s[at..]
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        });
    let msg = format!("ClickHouse HTTP {status}: {}", snippet(body, 300));
    let class = match code {
        // AUTHENTICATION_FAILED, UNKNOWN_USER, ACCESS_DENIED, REQUIRED_PASSWORD
        Some(516 | 192 | 497 | 194) => ErrorClass::Auth,
        // Bad rows: CANNOT_PARSE_*, INCORRECT_DATA, TYPE_MISMATCH,
        // CANNOT_CONVERT_TYPE, CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, …
        Some(6 | 26 | 27 | 38 | 41 | 53 | 69 | 70 | 72 | 117 | 349) => ErrorClass::Poison,
        // The table or a column is missing, or the query is wrong: the
        // instance's config, not the data.
        Some(16 | 47 | 60 | 62 | 81) => ErrorClass::Fatal,
        // TOO_MANY_PARTS, TOO_MANY_SIMULTANEOUS_QUERIES: slow down.
        Some(252 | 202) => ErrorClass::Throttled {
            after: std::time::Duration::from_secs(1),
        },
        // MEMORY_LIMIT_EXCEEDED, TIMEOUT_EXCEEDED, SOCKET_TIMEOUT,
        // NETWORK_ERROR, TABLE_IS_READ_ONLY, UNKNOWN_STATUS_OF_INSERT,
        // KEEPER_EXCEPTION, ALL_CONNECTION_TRIES_FAILED
        Some(241 | 159 | 209 | 210 | 242 | 319 | 999 | 279) => {
            ErrorClass::Retryable { after: None }
        }
        _ => classify_status(status, None).unwrap_or(ErrorClass::Retryable { after: None }),
    };
    ConnectorError::new(class, msg)
}

#[async_trait]
impl Sink for ClickHouseSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let token = batch_id(&batch);
        let (format, body, rows) = match &batch {
            EventBatch::Arrow { batch: rb, .. } => {
                ("ArrowStream", ipc_stream(rb)?, rb.num_rows() as u64)
            }
            EventBatch::Events(events) => {
                let mut body = Vec::new();
                for v in event_objects(events)? {
                    serde_json::to_writer(&mut body, &v)
                        .map_err(|e| ConnectorError::fatal(format!("JSONEachRow: {e}")))?;
                    body.push(b'\n');
                }
                ("JSONEachRow", Bytes::from(body), events.len() as u64)
            }
        };
        let bytes = body.len() as u64;
        if rows == 0 {
            return Ok(SinkAck::default());
        }
        self.insert(format, body, &token).await?;
        Ok(SinkAck { rows, bytes })
    }

    /// Each insert is acknowledged by ClickHouse when `write` returns.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Makes the instance's [`ClickHouseSink`].
pub struct ClickHouseSinkFactory {
    cfg: ClickHouseConfig,
    client: HttpClient,
    base: Uri,
    secrets: Arc<dyn SecretResolver>,
}

impl std::fmt::Debug for ClickHouseSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSinkFactory")
            .field("table", &self.cfg.table)
            .finish_non_exhaustive()
    }
}

impl ClickHouseSinkFactory {
    pub fn new(cfg: ClickHouseConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let base = base_url(&cfg.url, &*ctx.egress)?;
        let client = loams_connector_sdk::http::client(ctx.egress.clone())
            .with_timeout(std::time::Duration::from_secs(cfg.timeout_secs));
        Ok(Self {
            cfg,
            client,
            base,
            secrets: ctx.secrets.clone(),
        })
    }
}

impl SinkFactory for ClickHouseSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(ClickHouseSink {
            cfg: self.cfg.clone(),
            client: self.client.clone(),
            base: self.base.clone(),
            password: None,
            secrets: self.secrets.clone(),
            resolved: false,
        })))
    }
}

/// The ClickHouse ★'s native sink (Task 2's ref
/// `loams_connector_analytics::clickhouse_sink`). The ★'s source stays CN1's
/// (`loams_flow::connectors::clickhouse`) until Task 8 moves it here.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClickHouse;

impl loams_connector_sdk::ConnectorFactory for ClickHouse {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<
        Option<(
            Box<dyn loams_connector_sdk::SplitEnumerator>,
            Box<dyn loams_connector_sdk::SourceFactory>,
        )>,
        ConnectorError,
    > {
        Ok(None)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = ClickHouseConfig::parse(&ctx.config)?;
        Ok(Some(Box::new(ClickHouseSinkFactory::new(cfg, ctx)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exception_codes_classify() {
        let h = |code: &str| {
            let mut m = http::HeaderMap::new();
            m.insert("x-clickhouse-exception-code", code.parse().expect("hv"));
            m
        };
        let s = http::StatusCode::INTERNAL_SERVER_ERROR;
        assert_eq!(classify(s, &h("516"), b"").class, ErrorClass::Auth);
        assert_eq!(classify(s, &h("27"), b"").class, ErrorClass::Poison);
        assert_eq!(classify(s, &h("60"), b"").class, ErrorClass::Fatal);
        assert!(matches!(
            classify(s, &h("252"), b"").class,
            ErrorClass::Throttled { .. }
        ));
        assert_eq!(
            classify(s, &http::HeaderMap::new(), b"Code: 117. DB::Exception: bad").class,
            ErrorClass::Poison
        );
        assert_eq!(
            classify(
                http::StatusCode::SERVICE_UNAVAILABLE,
                &http::HeaderMap::new(),
                b""
            )
            .class,
            ErrorClass::Retryable { after: None }
        );
    }

    #[test]
    fn config_refuses_injection() {
        let bad = serde_json::json!({"url": "http://ch:8123", "table": "t; DROP TABLE x"});
        assert!(ClickHouseConfig::parse(&bad).is_err());
        let ok =
            serde_json::json!({"url": "http://ch:8123", "table": "events", "password": "ch-pass"});
        assert_eq!(
            ClickHouseConfig::parse(&ok).expect("ok").database,
            "default"
        );
    }
}
