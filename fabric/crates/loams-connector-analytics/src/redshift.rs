//! Amazon Redshift through the Redshift Data API (R17.4): JSON over HTTPS,
//! signed with SigV4, over the SDK's egress-checked client, so the AWS SDK
//! is not compiled.
//!
//! **Source.** With `query` alone, `ExecuteStatement`, `DescribeStatement`
//! until it finishes, then `GetStatementResult` page by page (`NextToken`),
//! decoded to Arrow by the column metadata. With `unload` too (the bulk
//! path, D356), `UNLOAD (query) TO 's3://…' FORMAT AS PARQUET`, then the
//! Parquet files read back from S3 as Arrow batches, one page per file.
//!
//! **Sink.** Each batch is written as one Parquet file under the staging
//! prefix (named by the batch's id), then one `BatchExecuteStatement` (a
//! single transaction): a temporary table shaped like the batch, `COPY` into
//! it, then `INSERT … SELECT` (`append`, at-least-once) or `MERGE` on the
//! configured keys (`merge`, effectively-once: a retried batch updates the
//! same rows).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, Uri};
use loams_connector_sdk::http::{HttpClient, classify_status};
use loams_connector_sdk::schema::arrow::SqlTypeMapper;
use loams_connector_sdk::schema::{NativeType, SchemaPolicy, TypeMapper};
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, EgressCheck, ErrorClass, EventBatch,
    SecretResolver, Sink, SinkAck, SinkFactory, SinkHandle, SourceFactory, SplitEnumerator,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::aws::{Credentials, amz_date, authorization};
use crate::common::{base_url, quote_ident, quote_literal, secret, snippet, table_name, text};
use crate::query::{
    Fetch, Page, PagedQuery, QueryEnumerator, QuerySource, QuerySourceFactory, poll_pause,
};
use crate::rows::{batch_id, json_rows, normalise, parquet_file, read_parquet, to_record_batch};
use crate::s3::{S3Config, map_err as s3_err, split_uri};

/// The manifest id (`connectors/registry/redshift.yaml`).
pub const CONNECTOR: &str = "redshift";

/// AWS credentials by secret **name**.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsAuth {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

/// An S3 location Redshift reads or writes, and the IAM role it uses.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Exchange {
    /// `s3://bucket/prefix`.
    pub s3_uri: String,
    /// The role Redshift assumes for `UNLOAD`/`COPY`.
    pub iam_role: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Append,
    Merge,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedshiftSinkConfig {
    pub table: String,
    #[serde(default)]
    pub mode: WriteMode,
    /// The merge keys (`merge`).
    #[serde(default)]
    pub keys: Vec<String>,
    pub staging: S3Exchange,
}

/// The instance config of a Redshift instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedshiftConfig {
    pub region: String,
    /// The Data API endpoint; `https://redshift-data.<region>.amazonaws.com`
    /// when absent (a fixture replay names its proxy here).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// A provisioned cluster (with `db_user` or `secret_arn`)…
    #[serde(default)]
    pub cluster_identifier: Option<String>,
    /// …or a Serverless workgroup.
    #[serde(default)]
    pub workgroup_name: Option<String>,
    pub database: String,
    #[serde(default)]
    pub db_user: Option<String>,
    /// A Secrets Manager ARN holding database credentials (an ARN, not a
    /// secret value).
    #[serde(default)]
    pub secret_arn: Option<String>,
    pub auth: AwsAuth,
    /// The bucket's endpoint and region, when not AWS's own.
    #[serde(default)]
    pub s3: S3Config,
    /// The source's query.
    #[serde(default)]
    pub query: Option<String>,
    /// The bulk path: `UNLOAD` to this location as Parquet.
    #[serde(default)]
    pub unload: Option<S3Exchange>,
    #[serde(default)]
    pub sink: Option<RedshiftSinkConfig>,
    /// How long a statement may run before the attempt is retried.
    #[serde(default = "default_statement_timeout")]
    pub statement_timeout_secs: u64,
    #[serde(default)]
    pub lossy: Vec<String>,
    #[serde(default)]
    pub evolution: Option<String>,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn default_statement_timeout() -> u64 {
    3600
}

fn check_role(role: &str) -> Result<(), ConnectorError> {
    if role.starts_with("arn:aws") && !role.contains(['\'', '\\', ' ']) {
        Ok(())
    } else {
        Err(ConnectorError::fatal(format!(
            "{role:?} is not an IAM role ARN"
        )))
    }
}

impl RedshiftConfig {
    pub fn parse(config: &Value) -> Result<Self, ConnectorError> {
        let cfg: Self = serde_json::from_value(config.clone())
            .map_err(|e| ConnectorError::fatal(format!("redshift config: {e}")))?;
        if cfg.cluster_identifier.is_some() == cfg.workgroup_name.is_some() {
            return Err(ConnectorError::fatal(
                "redshift config: name exactly one of cluster_identifier and workgroup_name",
            ));
        }
        if let Some(u) = &cfg.unload {
            split_uri(&u.s3_uri)?;
            check_role(&u.iam_role)?;
            if cfg.query.is_none() {
                return Err(ConnectorError::fatal(
                    "redshift config: unload needs a query",
                ));
            }
        }
        if let Some(s) = &cfg.sink {
            table_name(&s.table)?;
            split_uri(&s.staging.s3_uri)?;
            check_role(&s.staging.iam_role)?;
            if s.mode == WriteMode::Merge && s.keys.is_empty() {
                return Err(ConnectorError::fatal("redshift config: merge needs keys"));
            }
        }
        if !cfg
            .region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ConnectorError::fatal("redshift config: bad region"));
        }
        Ok(cfg)
    }
}

// ---------------------------------------------------------------------------
// The Data API

/// A Data API error reply: its `__type` (without the namespace) and message.
#[derive(Debug, Clone)]
pub struct Fault {
    pub ty: String,
    pub message: String,
}

impl Fault {
    /// The class of a Data API fault.
    pub fn error(&self, status: http::StatusCode) -> ConnectorError {
        let msg = format!("Redshift Data API {}: {}", self.ty, self.message);
        let class = match self.ty.as_str() {
            "ThrottlingException"
            | "ActiveStatementsExceededException"
            | "ActiveSessionsExceededException"
            | "TooManyRequestsException" => ErrorClass::Throttled {
                after: Duration::from_secs(1),
            },
            "AccessDeniedException"
            | "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "IncompleteSignature"
            | "MissingAuthenticationToken"
            | "ExpiredTokenException" => ErrorClass::Auth,
            "InternalServerException" | "DatabaseConnectionException" | "ServiceUnavailable" => {
                ErrorClass::Retryable { after: None }
            }
            "ValidationException"
            | "ResourceNotFoundException"
            | "ExecuteStatementException"
            | "BatchExecuteStatementException" => ErrorClass::Fatal,
            _ => classify_status(status, None).unwrap_or(ErrorClass::Retryable { after: None }),
        };
        ConnectorError::new(class, msg)
    }
}

/// A Data API client for one instance.
pub struct DataApi {
    client: HttpClient,
    endpoint: Uri,
    region: String,
    target: serde_json::Map<String, Value>,
    auth: AwsAuth,
    secrets: Arc<dyn SecretResolver>,
    creds: Option<Credentials>,
    timeout: Duration,
}

impl std::fmt::Debug for DataApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataApi")
            .field("endpoint", &self.endpoint.to_string())
            .finish_non_exhaustive()
    }
}

/// What `DescribeStatement` said when a statement finished.
#[derive(Debug, Clone, Copy)]
pub struct Finished {
    pub has_result_set: bool,
}

impl DataApi {
    pub fn new(cfg: &RedshiftConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let endpoint = cfg
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://redshift-data.{}.amazonaws.com", cfg.region));
        let endpoint = base_url(&endpoint, &*ctx.egress)?;
        let mut target = serde_json::Map::new();
        target.insert("Database".to_owned(), json!(cfg.database));
        if let Some(c) = &cfg.cluster_identifier {
            target.insert("ClusterIdentifier".to_owned(), json!(c));
        }
        if let Some(w) = &cfg.workgroup_name {
            target.insert("WorkgroupName".to_owned(), json!(w));
        }
        if let Some(u) = &cfg.db_user {
            target.insert("DbUser".to_owned(), json!(u));
        }
        if let Some(s) = &cfg.secret_arn {
            target.insert("SecretArn".to_owned(), json!(s));
        }
        Ok(Self {
            client: loams_connector_sdk::http::client(ctx.egress.clone()),
            endpoint,
            region: cfg.region.clone(),
            target,
            auth: cfg.auth.clone(),
            secrets: ctx.secrets.clone(),
            creds: None,
            timeout: Duration::from_secs(cfg.statement_timeout_secs),
        })
    }

    async fn credentials(&mut self) -> Result<Credentials, ConnectorError> {
        if let Some(c) = &self.creds {
            return Ok(c.clone());
        }
        let key = secret(&*self.secrets, Some(&self.auth.access_key_id))
            .await?
            .ok_or_else(|| ConnectorError::auth("auth.access_key_id names no secret"))?;
        let sec = secret(&*self.secrets, Some(&self.auth.secret_access_key))
            .await?
            .ok_or_else(|| ConnectorError::auth("auth.secret_access_key names no secret"))?;
        let token = secret(&*self.secrets, self.auth.session_token.as_ref()).await?;
        let c = Credentials {
            access_key_id: text(&key, "the AWS access key id")?,
            secret_access_key: sec,
            session_token: token,
        };
        self.creds = Some(c.clone());
        Ok(c)
    }

    /// One Data API call: `Ok(Ok(reply))`, `Ok(Err(fault))` for an AWS
    /// error reply, `Err` for transport failures.
    pub async fn call(
        &mut self,
        op: &str,
        mut body: serde_json::Map<String, Value>,
        with_target: bool,
    ) -> Result<Result<Value, (http::StatusCode, Fault)>, ConnectorError> {
        if with_target {
            for (k, v) in &self.target {
                body.insert(k.clone(), v.clone());
            }
        }
        let payload = serde_json::to_vec(&Value::Object(body))
            .map_err(|e| ConnectorError::fatal(format!("Data API body: {e}")))?;
        let creds = self.credentials().await?;
        let date = amz_date(SystemTime::now());
        let target = format!("RedshiftData.{op}");
        let mut headers = vec![
            (
                "content-type".to_owned(),
                "application/x-amz-json-1.1".to_owned(),
            ),
            ("x-amz-date".to_owned(), date.clone()),
            ("x-amz-target".to_owned(), target.clone()),
        ];
        if let Some(t) = &creds.session_token {
            headers.push((
                "x-amz-security-token".to_owned(),
                text(t, "the AWS session token")?,
            ));
        }
        let auth = authorization(
            "POST",
            &self.endpoint,
            &headers,
            &payload,
            &creds,
            &self.region,
            "redshift-data",
            &date,
        );
        let mut req = Request::post(self.endpoint.clone());
        for (k, v) in &headers {
            let mut hv = http::HeaderValue::from_str(v)
                .map_err(|_| ConnectorError::fatal(format!("header {k} is not a value")))?;
            if k == "x-amz-security-token" {
                hv.set_sensitive(true);
            }
            req = req.header(k.as_str(), hv);
        }
        let mut av = http::HeaderValue::from_str(&auth)
            .map_err(|_| ConnectorError::auth("the signature is not a header value"))?;
        av.set_sensitive(true);
        let req = req
            .header(http::header::AUTHORIZATION, av)
            .body(Bytes::from(payload))
            .map_err(|e| ConnectorError::fatal(format!("Data API request: {e}")))?;
        let resp = self.client.send(req).await?;
        let status = resp.status();
        if status.is_success() {
            return serde_json::from_slice(resp.body())
                .map(Ok)
                .map_err(|e| ConnectorError::retryable(format!("Data API reply: {e}")));
        }
        let v: Value = serde_json::from_slice(resp.body()).unwrap_or(Value::Null);
        let ty = v
            .get("__type")
            .and_then(Value::as_str)
            .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
            .or_else(|| {
                resp.headers()
                    .get("x-amzn-errortype")
                    .and_then(|h| h.to_str().ok())
                    .map(|t| t.split(':').next().unwrap_or(t).to_owned())
            })
            .unwrap_or_else(|| format!("HTTP {status}"));
        let message = v
            .get("message")
            .or_else(|| v.get("Message"))
            .and_then(Value::as_str)
            .map_or_else(|| snippet(resp.body(), 300), str::to_owned);
        Ok(Err((status, Fault { ty, message })))
    }

    /// A call whose faults are errors.
    pub async fn call_ok(
        &mut self,
        op: &str,
        body: serde_json::Map<String, Value>,
        with_target: bool,
    ) -> Result<Value, ConnectorError> {
        match self.call(op, body, with_target).await? {
            Ok(v) => Ok(v),
            Err((status, f)) => Err(f.error(status)),
        }
    }

    /// `ExecuteStatement`; the statement's id.
    pub async fn execute(&mut self, sql: &str) -> Result<String, ConnectorError> {
        let mut b = serde_json::Map::new();
        b.insert("Sql".to_owned(), json!(sql));
        let v = self.call_ok("ExecuteStatement", b, true).await?;
        id_of(&v)
    }

    /// `BatchExecuteStatement` (one transaction); the batch's id.
    pub async fn batch(&mut self, sqls: &[String]) -> Result<String, ConnectorError> {
        let mut b = serde_json::Map::new();
        b.insert("Sqls".to_owned(), json!(sqls));
        let v = self.call_ok("BatchExecuteStatement", b, true).await?;
        id_of(&v)
    }

    /// Poll `DescribeStatement` until the statement finishes.
    pub async fn wait(&mut self, id: &str) -> Result<Finished, ConnectorError> {
        let start = Instant::now();
        let mut attempt = 0u32;
        loop {
            let mut b = serde_json::Map::new();
            b.insert("Id".to_owned(), json!(id));
            let v = self.call_ok("DescribeStatement", b, false).await?;
            match v.get("Status").and_then(Value::as_str).unwrap_or("") {
                "FINISHED" => {
                    return Ok(Finished {
                        has_result_set: v
                            .get("HasResultSet")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    });
                }
                "FAILED" => {
                    let err = v.get("Error").and_then(Value::as_str).unwrap_or("failed");
                    let msg = format!("Redshift statement failed: {err}");
                    // A COPY that rejects rows names `stl_load_errors`: the
                    // data is at fault, not the instance.
                    return Err(
                        if err.contains("stl_load_errors") || err.contains("Load into table") {
                            ConnectorError::poison(msg)
                        } else {
                            ConnectorError::fatal(msg)
                        },
                    );
                }
                "ABORTED" => return Err(ConnectorError::retryable("Redshift statement aborted")),
                _ => {}
            }
            if start.elapsed() > self.timeout {
                return Err(ConnectorError::retryable(format!(
                    "Redshift statement {id} still running after {}s",
                    self.timeout.as_secs()
                )));
            }
            poll_pause(attempt).await;
            attempt += 1;
        }
    }
}

fn id_of(v: &Value) -> Result<String, ConnectorError> {
    v.get("Id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::retryable("Data API reply has no Id"))
}

// ---------------------------------------------------------------------------
// Types

/// A Redshift column (`ColumnMetadata`) as an Arrow field, through the
/// shared SQL mapper.
pub fn redshift_field(mapper: &SqlTypeMapper, col: &Value) -> Result<Field, ConnectorError> {
    let name = col
        .get("name")
        .or_else(|| col.get("label"))
        .and_then(Value::as_str)
        .ok_or_else(|| ConnectorError::fatal("a Redshift column has no name"))?;
    let ty = col
        .get("typeName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let precision = col.get("precision").and_then(Value::as_u64).unwrap_or(0);
    let scale = col.get("scale").and_then(Value::as_u64).unwrap_or(0);
    let native = match ty.as_str() {
        "int2" => "smallint".to_owned(),
        "int4" => "integer".to_owned(),
        "int8" => "bigint".to_owned(),
        "float4" => "real".to_owned(),
        "float8" => "double precision".to_owned(),
        "bool" => "boolean".to_owned(),
        "numeric" => format!("numeric({},{scale})", precision.max(1)),
        "varchar" | "bpchar" | "char" | "text" | "name" | "character varying" => "text".to_owned(),
        "super" => "json".to_owned(),
        "varbyte" | "binary varying" => "varbinary".to_owned(),
        "timetz" => "time".to_owned(),
        other => other.to_owned(),
    };
    Ok(mapper.to_arrow(&NativeType::new(name, native, true))?)
}

/// An Arrow field as a Redshift column type, for the staging table.
pub fn redshift_type(f: &Field) -> Result<String, ConnectorError> {
    let t = match f.data_type() {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "smallint".to_owned(),
        DataType::Int32 | DataType::UInt16 => "integer".to_owned(),
        DataType::Int64 | DataType::UInt32 => "bigint".to_owned(),
        DataType::Float32 => "real".to_owned(),
        DataType::Float64 => "double precision".to_owned(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar(65535)".to_owned(),
        DataType::Date32 => "date".to_owned(),
        DataType::Timestamp(_, None) => "timestamp".to_owned(),
        DataType::Timestamp(_, Some(_)) => "timestamptz".to_owned(),
        DataType::Binary | DataType::LargeBinary => "varbyte(1024000)".to_owned(),
        other => {
            return Err(ConnectorError::schema_incompatible(format!(
                "{}: Arrow {other} has no Redshift column type",
                f.name()
            )));
        }
    };
    Ok(t)
}

/// Data API `Records` as JSON objects by column name.
fn records(schema: &Schema, recs: &[Value]) -> Result<Vec<Value>, ConnectorError> {
    recs.iter()
        .map(|row| {
            let cells = row
                .as_array()
                .ok_or_else(|| ConnectorError::fatal("a Redshift record is not a list"))?;
            let mut m = serde_json::Map::with_capacity(cells.len());
            for (cell, f) in cells.iter().zip(schema.fields()) {
                let v = if cell.get("isNull").and_then(Value::as_bool) == Some(true) {
                    Value::Null
                } else if let Some(v) = cell.get("longValue") {
                    v.clone()
                } else if let Some(v) = cell.get("doubleValue") {
                    v.clone()
                } else if let Some(v) = cell.get("booleanValue") {
                    v.clone()
                } else if let Some(v) = cell.get("stringValue") {
                    v.clone()
                } else if let Some(v) = cell.get("blobValue") {
                    v.clone()
                } else {
                    Value::Null
                };
                m.insert(f.name().clone(), normalise(v, f)?);
            }
            Ok(Value::Object(m))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Source: rows through GetStatementResult

/// The Data API's paging.
pub struct RedshiftQuery {
    api: DataApi,
    sql: String,
    mapper: SqlTypeMapper,
    schema: Option<SchemaRef>,
}

impl std::fmt::Debug for RedshiftQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedshiftQuery").finish_non_exhaustive()
    }
}

impl RedshiftQuery {
    pub fn new(cfg: &RedshiftConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let policy =
            SchemaPolicy::from_config(&json!({"lossy": cfg.lossy, "evolution": cfg.evolution}))?;
        Ok(Self {
            api: DataApi::new(cfg, ctx)?,
            sql: cfg.query.clone().ok_or_else(|| {
                ConnectorError::fatal("redshift config: the source needs a query")
            })?,
            mapper: SqlTypeMapper::new(policy.lossy).signed_only(),
            schema: None,
        })
    }

    async fn page(&mut self, id: &str, token: Option<&str>) -> Result<Fetch, ConnectorError> {
        let mut b = serde_json::Map::new();
        b.insert("Id".to_owned(), json!(id));
        if let Some(t) = token {
            b.insert("NextToken".to_owned(), json!(t));
        }
        let v = match self.api.call("GetStatementResult", b, false).await? {
            Ok(v) => v,
            Err((_, f)) if f.ty == "ResourceNotFoundException" => return Ok(Fetch::Gone),
            Err((_, f))
                if f.ty == "ValidationException"
                    && (f.message.contains("expired") || f.message.contains("not found")) =>
            {
                return Ok(Fetch::Gone);
            }
            Err((status, f)) => return Err(f.error(status)),
        };
        if self.schema.is_none() {
            let cols = v
                .get("ColumnMetadata")
                .and_then(Value::as_array)
                .ok_or_else(|| ConnectorError::fatal("Redshift result has no ColumnMetadata"))?;
            let fields = cols
                .iter()
                .map(|c| redshift_field(&self.mapper, c))
                .collect::<Result<Vec<_>, _>>()?;
            self.schema = Some(Arc::new(Schema::new(fields)));
        }
        let schema = self
            .schema
            .clone()
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let recs = v
            .get("Records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let rows = json_rows(&schema, &records(&schema, &recs)?)?;
        let next = v
            .get("NextToken")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
            .map(|t| json!({"id": id, "token": t}));
        Ok(Fetch::Page(Page {
            at: json!({"id": id, "token": token}),
            rows,
            next,
        }))
    }
}

#[async_trait]
impl PagedQuery for RedshiftQuery {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        self.schema = None;
        let sql = self.sql.clone();
        let id = self.api.execute(&sql).await?;
        if !self.api.wait(&id).await?.has_result_set {
            return Ok(Fetch::End);
        }
        self.page(&id, None).await
    }

    async fn fetch(&mut self, cursor: &Value) -> Result<Fetch, ConnectorError> {
        let Some(id) = cursor.get("id").and_then(Value::as_str) else {
            return Ok(Fetch::Gone);
        };
        let token = cursor
            .get("token")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let id = id.to_owned();
        self.page(&id, token.as_deref()).await
    }

    fn query(&self) -> &str {
        &self.sql
    }
}

// ---------------------------------------------------------------------------
// Source: the bulk path through UNLOAD

/// `UNLOAD` to S3 Parquet, then the files as pages.
pub struct UnloadQuery {
    api: DataApi,
    sql: String,
    unload: S3Exchange,
    store: Option<Arc<dyn ObjectStore>>,
    store_cfg: (S3Config, String, AwsAuth),
    secrets: Arc<dyn SecretResolver>,
    egress: Arc<dyn EgressCheck>,
    instance: String,
}

impl std::fmt::Debug for UnloadQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnloadQuery")
            .field("s3_uri", &self.unload.s3_uri)
            .finish_non_exhaustive()
    }
}

impl UnloadQuery {
    pub fn new(cfg: &RedshiftConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            api: DataApi::new(cfg, ctx)?,
            sql: cfg.query.clone().ok_or_else(|| {
                ConnectorError::fatal("redshift config: the source needs a query")
            })?,
            unload: cfg
                .unload
                .clone()
                .ok_or_else(|| ConnectorError::fatal("redshift config: no unload"))?,
            store: None,
            store_cfg: (cfg.s3.clone(), cfg.region.clone(), cfg.auth.clone()),
            secrets: ctx.secrets.clone(),
            egress: ctx.egress.clone(),
            instance: ctx.instance.name.clone(),
        })
    }

    /// Use `store` for the unload bucket (tests; a shared client).
    pub fn with_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    async fn store(&mut self) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        if let Some(s) = &self.store {
            return Ok(s.clone());
        }
        let (bucket, _) = split_uri(&self.unload.s3_uri)?;
        let (s3, region, auth) = &self.store_cfg;
        let s = crate::s3::bucket(
            &bucket,
            s3,
            region,
            auth,
            &*self.secrets,
            self.egress.clone(),
        )
        .await?;
        self.store = Some(s.clone());
        Ok(s)
    }

    /// The Parquet files under a run's prefix, in name order.
    async fn files(&mut self, prefix: &str) -> Result<Vec<Path>, ConnectorError> {
        use futures::TryStreamExt as _;
        let store = self.store().await?;
        let mut files: Vec<Path> = store
            .list(Some(&Path::from(prefix)))
            .map_ok(|m| m.location)
            .try_collect()
            .await
            .map_err(|e| s3_err(&e, "list UNLOAD output"))?;
        files.sort();
        Ok(files)
    }

    async fn file_page(&mut self, prefix: &str, k: usize) -> Result<Fetch, ConnectorError> {
        let files = self.files(prefix).await?;
        if files.is_empty() {
            return Ok(Fetch::Gone);
        }
        let Some(path) = files.get(k).cloned() else {
            return Ok(Fetch::End);
        };
        let store = self.store().await?;
        let body = store
            .get(&path)
            .await
            .map_err(|e| s3_err(&e, "read UNLOAD output"))?
            .bytes()
            .await
            .map_err(|e| s3_err(&e, "read UNLOAD output"))?;
        let batches = read_parquet(body)?;
        let rows = match batches.first() {
            Some(b) => arrow_select::concat::concat_batches(&b.schema(), &batches)
                .map_err(|e| ConnectorError::fatal(format!("UNLOAD output: {e}")))?,
            None => RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        Ok(Fetch::Page(Page {
            at: json!({"prefix": prefix, "file": k}),
            rows: utc_relabel(rows)?,
            next: (k + 1 < files.len()).then(|| json!({"prefix": prefix, "file": k + 1})),
        }))
    }
}

/// UNLOAD writes `timestamptz` as `Timestamp(µs, "+00:00")`-style or
/// unzoned: label zoned columns `UTC`, as the rest of the family does.
fn utc_relabel(b: RecordBatch) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::{Array, ArrayRef, TimestampMicrosecondArray};
    let mut fields = Vec::with_capacity(b.num_columns());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(b.num_columns());
    for (f, c) in b.schema().fields().iter().zip(b.columns()) {
        match (
            f.data_type(),
            c.as_any().downcast_ref::<TimestampMicrosecondArray>(),
        ) {
            (DataType::Timestamp(TimeUnit::Microsecond, Some(_)), Some(a)) => {
                fields.push(Field::new(
                    f.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    f.is_nullable(),
                ));
                cols.push(Arc::new(a.clone().with_timezone("UTC")));
            }
            _ => {
                fields.push(f.as_ref().clone());
                cols.push(c.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| ConnectorError::fatal(format!("UNLOAD output: {e}")))
}

#[async_trait]
impl PagedQuery for UnloadQuery {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        let (bucket, base) = split_uri(&self.unload.s3_uri)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let prefix = if base.is_empty() {
            format!("{}/{nanos}/", self.instance)
        } else {
            format!("{base}/{}/{nanos}/", self.instance)
        };
        let sql = format!(
            "UNLOAD ({}) TO {} IAM_ROLE {} FORMAT AS PARQUET ALLOWOVERWRITE",
            quote_literal(&self.sql),
            quote_literal(&format!("s3://{bucket}/{prefix}")),
            quote_literal(&self.unload.iam_role),
        );
        let id = self.api.execute(&sql).await?;
        self.api.wait(&id).await?;
        self.file_page(&prefix, 0).await
    }

    async fn fetch(&mut self, cursor: &Value) -> Result<Fetch, ConnectorError> {
        let (Some(prefix), Some(k)) = (
            cursor.get("prefix").and_then(Value::as_str),
            cursor.get("file").and_then(Value::as_u64),
        ) else {
            return Ok(Fetch::Gone);
        };
        let prefix = prefix.to_owned();
        self.file_page(&prefix, k as usize).await
    }

    fn query(&self) -> &str {
        &self.sql
    }
}

// ---------------------------------------------------------------------------
// Sink

/// The statements that load one staged Parquet file into the table.
pub fn load_statements(
    sink: &RedshiftSinkConfig,
    schema: &Schema,
    file_uri: &str,
    batch: &str,
) -> Result<Vec<String>, ConnectorError> {
    let stage = format!("loams_stage_{}", &batch[..batch.len().min(16)]);
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    let ddl = schema
        .fields()
        .iter()
        .map(|f| Ok(format!("{} {}", quote_ident(f.name()), redshift_type(f)?)))
        .collect::<Result<Vec<_>, ConnectorError>>()?
        .join(", ");
    let mut sqls = vec![
        format!("CREATE TEMP TABLE {stage} ({ddl})"),
        format!(
            "COPY {stage} FROM {} IAM_ROLE {} FORMAT AS PARQUET",
            quote_literal(file_uri),
            quote_literal(&sink.staging.iam_role)
        ),
    ];
    let table = table_name(&sink.table)?;
    match sink.mode {
        WriteMode::Append => sqls.push(format!(
            "INSERT INTO {table} ({c}) SELECT {c} FROM {stage}",
            c = cols.join(", ")
        )),
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
                .map(|k| format!("{table}.{q} = s.{q}", q = quote_ident(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let set = schema
                .fields()
                .iter()
                .filter(|f| !sink.keys.contains(f.name()))
                .map(|f| format!("{q} = s.{q}", q = quote_ident(f.name())))
                .collect::<Vec<_>>();
            let vals = schema
                .fields()
                .iter()
                .map(|f| format!("s.{}", quote_ident(f.name())))
                .collect::<Vec<_>>()
                .join(", ");
            let matched = if set.is_empty() {
                // Keys only: a matched row is already as it should be.
                format!("UPDATE SET {q} = s.{q}", q = quote_ident(&sink.keys[0]))
            } else {
                format!("UPDATE SET {}", set.join(", "))
            };
            sqls.push(format!(
                "MERGE INTO {table} USING {stage} s ON {on} WHEN MATCHED THEN {matched} \
                 WHEN NOT MATCHED THEN INSERT ({}) VALUES ({vals})",
                cols.join(", ")
            ));
        }
    }
    sqls.push(format!("DROP TABLE {stage}"));
    Ok(sqls)
}

/// The Redshift sink.
pub struct RedshiftSink {
    api: DataApi,
    sink: RedshiftSinkConfig,
    store: Option<Arc<dyn ObjectStore>>,
    store_cfg: (S3Config, String, AwsAuth),
    secrets: Arc<dyn SecretResolver>,
    egress: Arc<dyn EgressCheck>,
    instance: String,
}

impl std::fmt::Debug for RedshiftSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedshiftSink")
            .field("table", &self.sink.table)
            .finish_non_exhaustive()
    }
}

impl RedshiftSink {
    pub fn new(cfg: &RedshiftConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            api: DataApi::new(cfg, ctx)?,
            sink: cfg
                .sink
                .clone()
                .ok_or_else(|| ConnectorError::fatal("redshift config: no sink"))?,
            store: None,
            store_cfg: (cfg.s3.clone(), cfg.region.clone(), cfg.auth.clone()),
            secrets: ctx.secrets.clone(),
            egress: ctx.egress.clone(),
            instance: ctx.instance.name.clone(),
        })
    }

    /// Use `store` for the staging bucket (tests; a shared client).
    pub fn with_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    async fn store(&mut self) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        if let Some(s) = &self.store {
            return Ok(s.clone());
        }
        let (bucket, _) = split_uri(&self.sink.staging.s3_uri)?;
        let (s3, region, auth) = &self.store_cfg;
        let s = crate::s3::bucket(
            &bucket,
            s3,
            region,
            auth,
            &*self.secrets,
            self.egress.clone(),
        )
        .await?;
        self.store = Some(s.clone());
        Ok(s)
    }
}

#[async_trait]
impl Sink for RedshiftSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let rb = to_record_batch(&batch, None)?;
        if rb.num_rows() == 0 {
            return Ok(SinkAck::default());
        }
        let id = batch_id(&batch);
        let (bucket, base) = split_uri(&self.sink.staging.s3_uri)?;
        let key = if base.is_empty() {
            format!("{}/{id}.parquet", self.instance)
        } else {
            format!("{base}/{}/{id}.parquet", self.instance)
        };
        let body = parquet_file(&rb)?;
        let bytes = body.len() as u64;
        let store = self.store().await?;
        store
            .put(&Path::from(key.as_str()), body.into())
            .await
            .map_err(|e| s3_err(&e, "stage Parquet"))?;
        let sqls = load_statements(
            &self.sink,
            &rb.schema(),
            &format!("s3://{bucket}/{key}"),
            &id,
        )?;
        let run = self.api.batch(&sqls).await?;
        self.api.wait(&run).await?;
        // The file has done its job; a leftover is harmless (same name, same
        // content on a retry).
        let _ = store.delete(&Path::from(key.as_str())).await;
        Ok(SinkAck {
            rows: rb.num_rows() as u64,
            bytes,
        })
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Makes the instance's [`RedshiftSink`].
pub struct RedshiftSinkFactory {
    make: Box<dyn Fn() -> Result<RedshiftSink, ConnectorError> + Send + Sync>,
}

impl std::fmt::Debug for RedshiftSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedshiftSinkFactory")
    }
}

impl RedshiftSinkFactory {
    pub fn new(
        make: impl Fn() -> Result<RedshiftSink, ConnectorError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            make: Box::new(make),
        }
    }
}

impl SinkFactory for RedshiftSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new((self.make)()?)))
    }
}

/// The `redshift` connector (Task 2's ref
/// `loams_connector_analytics::redshift`).
#[derive(Debug, Default, Clone, Copy)]
pub struct Redshift;

impl ConnectorFactory for Redshift {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = RedshiftConfig::parse(&ctx.config)?;
        if cfg.query.is_none() {
            return Ok(None);
        }
        DataApi::new(&cfg, ctx)?;
        let ctx = ctx.clone();
        let instance = ctx.instance.clone();
        Ok(Some((
            Box::new(QueryEnumerator),
            Box::new(QuerySourceFactory::new(move || {
                let q: Result<Box<dyn PagedQuery>, ConnectorError> = if cfg.unload.is_some() {
                    UnloadQuery::new(&cfg, &ctx).map(|q| Box::new(q) as Box<dyn PagedQuery>)
                } else {
                    RedshiftQuery::new(&cfg, &ctx).map(|q| Box::new(q) as Box<dyn PagedQuery>)
                };
                let q = q.unwrap_or_else(|e| Box::new(crate::trino::Failed(e)));
                QuerySource::new(q, CONNECTOR, &instance, "query")
            })),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = RedshiftConfig::parse(&ctx.config)?;
        if cfg.sink.is_none() {
            return Ok(None);
        }
        RedshiftSink::new(&cfg, ctx)?;
        let ctx = ctx.clone();
        Ok(Some(Box::new(RedshiftSinkFactory::new(move || {
            RedshiftSink::new(&cfg, &ctx)
        }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::schema::Lossy;

    fn sink(mode: WriteMode) -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            table: "public.orders".to_owned(),
            mode,
            keys: vec!["id".to_owned()],
            staging: S3Exchange {
                s3_uri: "s3://stage/loams".to_owned(),
                iam_role: "arn:aws:iam::123456789012:role/loams".to_owned(),
            },
        }
    }

    #[test]
    fn load_statements_append_and_merge() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let a = load_statements(
            &sink(WriteMode::Append),
            &schema,
            "s3://stage/x.parquet",
            "abcdef0123456789ff",
        )
        .expect("append");
        assert_eq!(
            a,
            [
                "CREATE TEMP TABLE loams_stage_abcdef0123456789 (\"id\" bigint, \"name\" varchar(65535))",
                "COPY loams_stage_abcdef0123456789 FROM 's3://stage/x.parquet' IAM_ROLE 'arn:aws:iam::123456789012:role/loams' FORMAT AS PARQUET",
                "INSERT INTO public.orders (\"id\", \"name\") SELECT \"id\", \"name\" FROM loams_stage_abcdef0123456789",
                "DROP TABLE loams_stage_abcdef0123456789",
            ]
        );
        let m = load_statements(
            &sink(WriteMode::Merge),
            &schema,
            "s3://stage/x.parquet",
            "ab",
        )
        .expect("merge");
        assert_eq!(
            m[2],
            "MERGE INTO public.orders USING loams_stage_ab s ON public.orders.\"id\" = s.\"id\" \
             WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\" WHEN NOT MATCHED THEN INSERT \
             (\"id\", \"name\") VALUES (s.\"id\", s.\"name\")"
        );
        let nokey = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);
        assert!(load_statements(&sink(WriteMode::Merge), &nokey, "s3://s/x", "ab").is_err());
    }

    #[test]
    fn redshift_columns_map() {
        let m = SqlTypeMapper::new(Lossy::paths(["ts"])).signed_only();
        let f = |c: Value| redshift_field(&m, &c).map(|f| f.data_type().clone());
        assert_eq!(
            f(json!({"name": "a", "typeName": "int8"})).expect("ok"),
            DataType::Int64
        );
        assert_eq!(
            f(json!({"name": "b", "typeName": "numeric", "precision": 12, "scale": 2}))
                .expect("ok"),
            DataType::Decimal128(12, 2)
        );
        assert_eq!(
            f(json!({"name": "c", "typeName": "varchar"})).expect("ok"),
            DataType::Utf8
        );
        assert!(f(json!({"name": "ts", "typeName": "timestamp"})).is_ok());
        assert!(f(json!({"name": "other", "typeName": "timestamp"})).is_err());
        assert!(f(json!({"name": "g", "typeName": "geometry"})).is_err());
    }

    #[test]
    fn faults_classify() {
        let f = |ty: &str| {
            Fault {
                ty: ty.to_owned(),
                message: "m".to_owned(),
            }
            .error(http::StatusCode::BAD_REQUEST)
            .class
        };
        assert!(matches!(
            f("ThrottlingException"),
            ErrorClass::Throttled { .. }
        ));
        assert_eq!(f("AccessDeniedException"), ErrorClass::Auth);
        assert_eq!(f("ValidationException"), ErrorClass::Fatal);
        assert_eq!(
            f("InternalServerException"),
            ErrorClass::Retryable { after: None }
        );
    }

    #[test]
    fn config_rules() {
        let base = json!({
            "region": "us-east-1", "workgroup_name": "wg", "database": "dev",
            "auth": {"access_key_id": "k", "secret_access_key": "s"},
            "query": "SELECT 1"
        });
        assert!(RedshiftConfig::parse(&base).is_ok());
        let mut both = base.clone();
        both["cluster_identifier"] = json!("c");
        assert!(RedshiftConfig::parse(&both).is_err());
        let mut bad_role = base.clone();
        bad_role["unload"] = json!({"s3_uri": "s3://b/p", "iam_role": "x' OR 1=1"});
        assert!(RedshiftConfig::parse(&bad_role).is_err());
    }
}
