//! Databricks SQL through the SQL Statement Execution API
//! (`/api/2.0/sql/statements`), over the SDK's egress-checked client.
//!
//! **Source.** The statement runs with `format: ARROW_STREAM` and
//! `disposition: EXTERNAL_LINKS`; each result chunk is an Arrow IPC stream
//! behind a short-lived presigned link (fetched without the workspace
//! token; the link's host is egress-checked like any other). One page per
//! chunk; a closed statement re-runs (R17.9).
//!
//! **Sink.** Each batch is uploaded as one Parquet file to a Unity Catalog
//! volume through the Files API (named by the batch's id, never
//! overwritten), then loaded with `COPY INTO` (`append`) or `MERGE`
//! (`merge`, on the configured keys). `COPY INTO` skips a file it has
//! already loaded, so a retried batch is not loaded twice: both modes are
//! effectively-once.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode, Uri};
use loams_connector_sdk::http::{HttpClient, classify_status};
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, ErrorClass, EventBatch, Secret,
    SecretResolver, Sink, SinkAck, SinkFactory, SinkHandle, SourceFactory, SplitEnumerator,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::common::{base_url, join, quote_literal, secret, snippet, table_name, text};
use crate::query::{
    Fetch, Page, PagedQuery, QueryEnumerator, QuerySource, QuerySourceFactory, poll_pause,
};
use crate::rows::{batch_id, parquet_file, read_ipc_stream, to_record_batch};

/// The manifest id (`connectors/registry/databricks.yaml`).
pub const CONNECTOR: &str = "databricks";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Append,
    Merge,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabricksSinkConfig {
    /// `catalog.schema.table`.
    pub table: String,
    #[serde(default)]
    pub mode: WriteMode,
    #[serde(default)]
    pub keys: Vec<String>,
    /// A Unity Catalog volume directory, `/Volumes/<catalog>/<schema>/<volume>/<dir>`.
    pub volume: String,
}

/// The instance config of a Databricks instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabricksConfig {
    /// The workspace, `https://<workspace>.cloud.databricks.com`.
    pub host: String,
    pub warehouse_id: String,
    /// A personal access token's or OAuth access token's secret **name**.
    pub token: String,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub sink: Option<DatabricksSinkConfig>,
    #[serde(default = "default_statement_timeout")]
    pub statement_timeout_secs: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn default_statement_timeout() -> u64 {
    3600
}

impl DatabricksConfig {
    pub fn parse(config: &Value) -> Result<Self, ConnectorError> {
        let cfg: Self = serde_json::from_value(config.clone())
            .map_err(|e| ConnectorError::fatal(format!("databricks config: {e}")))?;
        if !cfg
            .warehouse_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return Err(ConnectorError::fatal("databricks config: bad warehouse_id"));
        }
        if let Some(s) = &cfg.sink {
            table_name(&s.table)?;
            volume_path(&s.volume)?;
            if s.mode == WriteMode::Merge && s.keys.is_empty() {
                return Err(ConnectorError::fatal("databricks config: merge needs keys"));
            }
        }
        Ok(cfg)
    }
}

/// A volume directory: `/Volumes/…`, path characters only.
fn volume_path(p: &str) -> Result<&str, ConnectorError> {
    let ok = p.starts_with("/Volumes/")
        && p.split('/').skip(1).all(|seg| {
            !seg.is_empty()
                && seg != ".."
                && seg
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        });
    if ok {
        Ok(p.trim_end_matches('/'))
    } else {
        Err(ConnectorError::fatal(format!(
            "{p:?} is not a plain Unity Catalog volume path (/Volumes/<catalog>/<schema>/<volume>/…)"
        )))
    }
}

/// The workspace's REST API for one instance.
pub struct Workspace {
    client: HttpClient,
    base: Uri,
    token_name: String,
    token: Option<Secret>,
    secrets: Arc<dyn SecretResolver>,
    warehouse_id: String,
    catalog: Option<String>,
    schema: Option<String>,
    timeout: Duration,
}

impl std::fmt::Debug for Workspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workspace")
            .field("base", &self.base.to_string())
            .finish_non_exhaustive()
    }
}

/// A finished statement: its id, chunk count and the first chunk's links.
#[derive(Debug, Clone)]
pub struct Statement {
    pub id: String,
    pub chunks: u64,
    pub first: Value,
}

impl Workspace {
    pub fn new(cfg: &DatabricksConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            client: loams_connector_sdk::http::client(ctx.egress.clone())
                .with_timeout(Duration::from_secs(120)),
            base: base_url(&cfg.host, &*ctx.egress)?,
            token_name: cfg.token.clone(),
            token: None,
            secrets: ctx.secrets.clone(),
            warehouse_id: cfg.warehouse_id.clone(),
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            timeout: Duration::from_secs(cfg.statement_timeout_secs),
        })
    }

    async fn bearer(&mut self) -> Result<http::HeaderValue, ConnectorError> {
        if self.token.is_none() {
            self.token = secret(&*self.secrets, Some(&self.token_name)).await?;
        }
        let t = self
            .token
            .as_ref()
            .ok_or_else(|| ConnectorError::auth("token names no secret"))?;
        let mut v =
            http::HeaderValue::from_str(&format!("Bearer {}", text(t, "the Databricks token")?))
                .map_err(|_| ConnectorError::auth("the Databricks token is not a header value"))?;
        v.set_sensitive(true);
        Ok(v)
    }

    /// One authenticated workspace call.
    async fn call(
        &mut self,
        req: http::request::Builder,
        body: Bytes,
    ) -> Result<http::Response<Bytes>, ConnectorError> {
        let auth = self.bearer().await?;
        let req = req
            .header(http::header::AUTHORIZATION, auth)
            .header("user-agent", "loams-connector-databricks")
            .body(body)
            .map_err(|e| ConnectorError::fatal(format!("Databricks request: {e}")))?;
        self.client.send(req).await
    }

    /// Run a statement and wait for it.
    pub async fn run(&mut self, sql: &str, arrow: bool) -> Result<Statement, ConnectorError> {
        let mut body = json!({
            "statement": sql,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        });
        if arrow {
            body["format"] = json!("ARROW_STREAM");
            body["disposition"] = json!("EXTERNAL_LINKS");
        }
        if let Some(c) = &self.catalog {
            body["catalog"] = json!(c);
        }
        if let Some(s) = &self.schema {
            body["schema"] = json!(s);
        }
        let uri = join(&self.base, "/api/2.0/sql/statements")?;
        let resp = self
            .call(
                Request::post(uri).header("content-type", "application/json"),
                Bytes::from(body.to_string()),
            )
            .await?;
        let mut v = reply(&resp)?;
        let id = v
            .get("statement_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::retryable("Databricks reply has no statement_id"))?
            .to_owned();
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            let state = v
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            match state.as_str() {
                "SUCCEEDED" => {
                    return Ok(Statement {
                        id,
                        chunks: v
                            .pointer("/manifest/total_chunk_count")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        first: v.get("result").cloned().unwrap_or(Value::Null),
                    });
                }
                "FAILED" => return Err(statement_error(&v)),
                "CANCELED" | "CLOSED" => {
                    return Err(ConnectorError::retryable(format!(
                        "Databricks statement {state}"
                    )));
                }
                _ => {}
            }
            if start.elapsed() > self.timeout {
                return Err(ConnectorError::retryable(format!(
                    "Databricks statement still running after {}s",
                    self.timeout.as_secs()
                )));
            }
            poll_pause(attempt).await;
            attempt += 1;
            let uri = join(&self.base, &format!("/api/2.0/sql/statements/{id}"))?;
            let resp = self.call(Request::get(uri), Bytes::new()).await?;
            v = reply(&resp)?;
        }
    }

    /// A chunk's links; `None` when the statement is gone.
    pub async fn chunk(&mut self, id: &str, n: u64) -> Result<Option<Value>, ConnectorError> {
        if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Ok(None);
        }
        let uri = join(
            &self.base,
            &format!("/api/2.0/sql/statements/{id}/result/chunks/{n}"),
        )?;
        let resp = self.call(Request::get(uri), Bytes::new()).await?;
        if resp.status() == StatusCode::NOT_FOUND || resp.status() == StatusCode::GONE {
            return Ok(None);
        }
        let v = reply(&resp)?;
        Ok(Some(v))
    }

    /// Download a chunk's Arrow stream from its presigned link. The link
    /// carries its own credentials, so the workspace token is not sent.
    pub async fn download(
        &mut self,
        link: &Value,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        let url = link
            .get("external_link")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::retryable("a chunk link has no external_link"))?;
        let mut req = Request::get(url);
        if let Some(h) = link.get("http_headers").and_then(Value::as_object) {
            for (k, v) in h {
                if let Some(v) = v.as_str() {
                    req = req.header(k.as_str(), v);
                }
            }
        }
        let req = req
            .body(Bytes::new())
            .map_err(|_| ConnectorError::retryable("a chunk link is not a URL"))?;
        let resp = self.client.send(req).await?;
        if !resp.status().is_success() {
            // Never echo the link: its query string is a credential. An
            // expired link (403) is fetched again with the chunk.
            return Err(ConnectorError::retryable(format!(
                "Databricks result chunk download: HTTP {}",
                resp.status()
            )));
        }
        let batches = read_ipc_stream(resp.into_body())?;
        match batches.first() {
            Some(b) => arrow_select::concat::concat_batches(&b.schema(), &batches)
                .map_err(|e| ConnectorError::fatal(format!("result chunk: {e}"))),
            None => Ok(arrow_array::RecordBatch::new_empty(Arc::new(
                arrow_schema::Schema::empty(),
            ))),
        }
    }

    /// Upload a file to a volume path; an existing file (a retry) is kept.
    pub async fn upload(&mut self, path: &str, body: Bytes) -> Result<(), ConnectorError> {
        let uri = join(
            &self.base,
            &format!("/api/2.0/fs/files{path}?overwrite=false"),
        )?;
        let resp = self
            .call(
                Request::put(uri).header("content-type", "application/octet-stream"),
                body,
            )
            .await?;
        if resp.status().is_success() || resp.status() == StatusCode::CONFLICT {
            return Ok(());
        }
        reply(&resp).map(|_| ())
    }

    /// Delete a volume file (best effort).
    pub async fn delete(&mut self, path: &str) {
        if let Ok(uri) = join(&self.base, &format!("/api/2.0/fs/files{path}")) {
            let _ = self.call(Request::delete(uri), Bytes::new()).await;
        }
    }
}

/// A workspace reply's JSON, or its error by class.
fn reply(resp: &http::Response<Bytes>) -> Result<Value, ConnectorError> {
    if resp.status().is_success() {
        return serde_json::from_slice(resp.body())
            .map_err(|e| ConnectorError::retryable(format!("Databricks reply: {e}")));
    }
    let v: Value = serde_json::from_slice(resp.body()).unwrap_or(Value::Null);
    let code = v.get("error_code").and_then(Value::as_str).unwrap_or("");
    let msg = format!(
        "Databricks HTTP {} {code}: {}",
        resp.status(),
        v.get("message")
            .and_then(Value::as_str)
            .map_or_else(|| snippet(resp.body(), 300), str::to_owned)
    );
    let retry_after = resp
        .headers()
        .get(http::header::RETRY_AFTER)
        .and_then(|h| h.to_str().ok());
    let class = match code {
        "INVALID_PARAMETER_VALUE" | "BAD_REQUEST" | "RESOURCE_DOES_NOT_EXIST" | "NOT_FOUND" => {
            ErrorClass::Fatal
        }
        "PERMISSION_DENIED" | "UNAUTHENTICATED" => ErrorClass::Auth,
        "TEMPORARILY_UNAVAILABLE" => ErrorClass::Retryable { after: None },
        _ => classify_status(resp.status(), retry_after)
            .unwrap_or(ErrorClass::Retryable { after: None }),
    };
    Err(ConnectorError::new(class, msg))
}

/// A failed statement's error: SQL and data errors are the instance's
/// (`Fatal`); a malformed row in a loaded file is `Poison`.
fn statement_error(v: &Value) -> ConnectorError {
    let code = v
        .pointer("/status/error/error_code")
        .and_then(Value::as_str)
        .unwrap_or("");
    let msg = v
        .pointer("/status/error/message")
        .and_then(Value::as_str)
        .unwrap_or("failed");
    let text = format!("Databricks statement failed ({code}): {msg}");
    if msg.contains("MALFORMED") || msg.contains("CAST_INVALID_INPUT") {
        ConnectorError::poison(text)
    } else if code == "TEMPORARILY_UNAVAILABLE" {
        ConnectorError::retryable(text)
    } else {
        ConnectorError::fatal(text)
    }
}

// ---------------------------------------------------------------------------
// Source

/// The Statement Execution API's chunks as pages.
pub struct DatabricksQuery {
    ws: Workspace,
    sql: String,
}

impl std::fmt::Debug for DatabricksQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksQuery").finish_non_exhaustive()
    }
}

impl DatabricksQuery {
    pub fn new(cfg: &DatabricksConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            ws: Workspace::new(cfg, ctx)?,
            sql: cfg.query.clone().ok_or_else(|| {
                ConnectorError::fatal("databricks config: the source needs a query")
            })?,
        })
    }

    /// The page of chunk `n` from its links.
    async fn page(&mut self, id: &str, n: u64, links: &Value) -> Result<Fetch, ConnectorError> {
        let list = links
            .get("external_links")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let link = list
            .iter()
            .find(|l| l.get("chunk_index").and_then(Value::as_u64) == Some(n))
            .or_else(|| list.first())
            .cloned()
            .ok_or_else(|| ConnectorError::retryable("a result chunk has no link"))?;
        let rows = self.ws.download(&link).await?;
        let next = link
            .get("next_chunk_index")
            .and_then(Value::as_u64)
            .map(|k| json!({"id": id, "chunk": k}));
        Ok(Fetch::Page(Page {
            at: json!({"id": id, "chunk": n}),
            rows,
            next,
        }))
    }
}

#[async_trait]
impl PagedQuery for DatabricksQuery {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        let sql = self.sql.clone();
        let st = self.ws.run(&sql, true).await?;
        if st.chunks == 0 {
            return Ok(Fetch::End);
        }
        if st.first.get("external_links").is_some() {
            return self.page(&st.id, 0, &st.first).await;
        }
        match self.ws.chunk(&st.id, 0).await? {
            Some(links) => self.page(&st.id, 0, &links).await,
            None => Ok(Fetch::Gone),
        }
    }

    async fn fetch(&mut self, cursor: &Value) -> Result<Fetch, ConnectorError> {
        let (Some(id), Some(n)) = (
            cursor.get("id").and_then(Value::as_str),
            cursor.get("chunk").and_then(Value::as_u64),
        ) else {
            return Ok(Fetch::Gone);
        };
        let id = id.to_owned();
        match self.ws.chunk(&id, n).await? {
            Some(links) => self.page(&id, n, &links).await,
            None => Ok(Fetch::Gone),
        }
    }

    fn query(&self) -> &str {
        &self.sql
    }
}

// ---------------------------------------------------------------------------
// Sink

/// The statement that loads one volume file into the table.
pub fn load_statement(
    sink: &DatabricksSinkConfig,
    schema: &arrow_schema::Schema,
    file: &str,
) -> Result<String, ConnectorError> {
    let table = table_name(&sink.table)?;
    Ok(match sink.mode {
        WriteMode::Append => format!(
            "COPY INTO {table} FROM {} FILEFORMAT = PARQUET",
            quote_literal(file)
        ),
        WriteMode::Merge => {
            for k in &sink.keys {
                if schema.field_with_name(k).is_err() {
                    return Err(ConnectorError::schema_incompatible(format!(
                        "merge key {k:?} is not in the batch"
                    )));
                }
            }
            let on = sink
                .keys
                .iter()
                .map(|k| format!("t.{q} = s.{q}", q = bq(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "MERGE INTO {table} t USING (SELECT * FROM read_files({}, format => 'parquet')) s \
                 ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
                quote_literal(file)
            )
        }
    })
}

/// A Databricks SQL identifier (backquoted).
fn bq(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The Databricks sink.
pub struct DatabricksSink {
    ws: Workspace,
    sink: DatabricksSinkConfig,
    instance: String,
}

impl std::fmt::Debug for DatabricksSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksSink")
            .field("table", &self.sink.table)
            .finish_non_exhaustive()
    }
}

impl DatabricksSink {
    pub fn new(cfg: &DatabricksConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            ws: Workspace::new(cfg, ctx)?,
            sink: cfg
                .sink
                .clone()
                .ok_or_else(|| ConnectorError::fatal("databricks config: no sink"))?,
            instance: ctx.instance.name.clone(),
        })
    }
}

#[async_trait]
impl Sink for DatabricksSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let rb = to_record_batch(&batch, None)?;
        if rb.num_rows() == 0 {
            return Ok(SinkAck::default());
        }
        let instance: String = self
            .instance
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let file = format!(
            "{}/{instance}/{}.parquet",
            volume_path(&self.sink.volume)?,
            batch_id(&batch)
        );
        let body = parquet_file(&rb)?;
        let bytes = body.len() as u64;
        self.ws.upload(&file, body).await?;
        let sql = load_statement(&self.sink, &rb.schema(), &file)?;
        self.ws.run(&sql, false).await?;
        Ok(SinkAck {
            rows: rb.num_rows() as u64,
            bytes,
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Makes the instance's [`DatabricksSink`].
pub struct DatabricksSinkFactory {
    make: Box<dyn Fn() -> Result<DatabricksSink, ConnectorError> + Send + Sync>,
}

impl std::fmt::Debug for DatabricksSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatabricksSinkFactory")
    }
}

impl SinkFactory for DatabricksSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new((self.make)()?)))
    }
}

/// The `databricks` connector (Task 2's ref
/// `loams_connector_analytics::databricks`).
#[derive(Debug, Default, Clone, Copy)]
pub struct Databricks;

impl ConnectorFactory for Databricks {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = DatabricksConfig::parse(&ctx.config)?;
        if cfg.query.is_none() {
            return Ok(None);
        }
        Workspace::new(&cfg, ctx)?;
        let ctx = ctx.clone();
        let instance = ctx.instance.clone();
        Ok(Some((
            Box::new(QueryEnumerator),
            Box::new(QuerySourceFactory::new(move || {
                let q: Box<dyn PagedQuery> = match DatabricksQuery::new(&cfg, &ctx) {
                    Ok(q) => Box::new(q),
                    Err(e) => Box::new(crate::trino::Failed(e)),
                };
                QuerySource::new(q, CONNECTOR, &instance, "query")
            })),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = DatabricksConfig::parse(&ctx.config)?;
        if cfg.sink.is_none() {
            return Ok(None);
        }
        DatabricksSink::new(&cfg, ctx)?;
        let ctx = ctx.clone();
        Ok(Some(Box::new(DatabricksSinkFactory {
            make: Box::new(move || DatabricksSink::new(&cfg, &ctx)),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn sink(mode: WriteMode) -> DatabricksSinkConfig {
        DatabricksSinkConfig {
            table: "main.sales.orders".to_owned(),
            mode,
            keys: vec!["id".to_owned()],
            volume: "/Volumes/main/sales/landing/loams".to_owned(),
        }
    }

    #[test]
    fn load_statements() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert_eq!(
            load_statement(
                &sink(WriteMode::Append),
                &schema,
                "/Volumes/main/sales/landing/loams/x.parquet"
            )
            .expect("append"),
            "COPY INTO main.sales.orders FROM '/Volumes/main/sales/landing/loams/x.parquet' FILEFORMAT = PARQUET"
        );
        assert_eq!(
            load_statement(&sink(WriteMode::Merge), &schema, "/Volumes/v/x.parquet")
                .expect("merge"),
            "MERGE INTO main.sales.orders t USING (SELECT * FROM read_files('/Volumes/v/x.parquet', \
             format => 'parquet')) s ON t.`id` = s.`id` WHEN MATCHED THEN UPDATE SET * WHEN NOT \
             MATCHED THEN INSERT *"
        );
    }

    #[test]
    fn volume_paths() {
        assert!(volume_path("/Volumes/main/sales/landing").is_ok());
        assert!(volume_path("/Volumes/main/../etc").is_err());
        assert!(volume_path("/tmp/x").is_err());
        assert!(volume_path("/Volumes/a/b'c").is_err());
    }
}
