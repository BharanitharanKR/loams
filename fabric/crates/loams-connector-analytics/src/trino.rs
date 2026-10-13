//! Trino, a source only: the client REST protocol (`POST /v1/statement`,
//! then `GET nextUri` until it is absent), over the SDK's egress-checked
//! HTTP client.
//!
//! Each response may carry `columns` and `data` (rows as JSON arrays). The
//! columns' type names map to Arrow through the SDK's `SqlTypeMapper` with
//! Trino's additions (`varchar` without a length, `uuid`, `json`,
//! `ipaddress`), honouring the instance's `lossy` paths; nested types
//! (`array`, `map`, `row`) are `SchemaIncompatible` until a mapping is
//! written. A `nextUri` can be fetched again until the next one is
//! requested, so a restart resumes at the page it was reading; once Trino
//! has moved past it (`404`/`410`), the query runs again (R17.9).

use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http::{Request, StatusCode, Uri};
use loams_connector_sdk::http::{HttpClient, error_for_response};
use loams_connector_sdk::schema::arrow::SqlTypeMapper;
use loams_connector_sdk::schema::{Lossy, NativeType, SchemaPolicy, TypeMapper};
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, Secret, SecretResolver, SinkFactory,
    SourceFactory, SplitEnumerator,
};
use serde::Deserialize;

use crate::common::{base_url, join, secret, snippet, text};
use crate::query::{Fetch, Page, PagedQuery, QueryEnumerator, QuerySource, QuerySourceFactory};
use crate::rows::{json_rows, normalise};

/// The manifest id (`connectors/registry/trino.yaml`).
pub const CONNECTOR: &str = "trino";

/// The instance config of a Trino source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoConfig {
    /// The coordinator, `https://trino:8443`.
    pub url: String,
    pub user: String,
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    /// The query; give it an `ORDER BY` for an exact resume after Trino has
    /// forgotten the page (R17.9).
    pub query: String,
    /// HTTP Basic: the password's secret **name** (Trino accepts passwords
    /// over HTTPS only).
    #[serde(default)]
    pub password: Option<String>,
    /// A JWT or OAuth 2 access token's secret **name** (`Authorization:
    /// Bearer`).
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Columns whose mapping is lossy and accepted (R5.1).
    #[serde(default)]
    pub lossy: Vec<String>,
    #[serde(default)]
    pub evolution: Option<String>,
    #[serde(default)]
    pub pipeline: Option<serde_json::Value>,
}

fn default_timeout() -> u64 {
    60
}

impl TrinoConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let cfg: Self = serde_json::from_value(config.clone())
            .map_err(|e| ConnectorError::fatal(format!("trino config: {e}")))?;
        if cfg.query.trim().is_empty() {
            return Err(ConnectorError::fatal("trino config: query is empty"));
        }
        if cfg.password.is_some() && cfg.token.is_some() {
            return Err(ConnectorError::fatal(
                "trino config: password and token are exclusive",
            ));
        }
        if cfg.password.is_some() && !cfg.url.starts_with("https://") {
            return Err(ConnectorError::fatal(
                "trino config: a password needs an https:// url",
            ));
        }
        Ok(cfg)
    }
}

/// Trino's type names as Arrow fields: the shared SQL mapper, after Trino's
/// own spellings.
pub fn trino_field(
    mapper: &SqlTypeMapper,
    lossy: &Lossy,
    name: &str,
    type_name: &str,
) -> Result<Field, ConnectorError> {
    let t = type_name.trim().to_ascii_lowercase();
    // `timestamp(p)`, `time(p)` (with or without a zone): Arrow keeps
    // microseconds, so p ≤ 6 is exact and finer precisions are lossy.
    let (t, finer) = match (t.find('('), t.find(')')) {
        (Some(open), Some(close)) if t.starts_with("timestamp") || t.starts_with("time") => {
            let p: u32 = t[open + 1..close].trim().parse().unwrap_or(0);
            (format!("{}{}", &t[..open], &t[close + 1..]), p > 6)
        }
        _ => (t, false),
    };
    let mapped = match t.as_str() {
        // `uuid` and `ipaddress` are read as text (Trino renders them as
        // strings).
        "varchar" | "char" | "uuid" | "json" | "ipaddress" => "text".to_owned(),
        other => other.to_owned(),
    };
    let field = mapper.to_arrow(&NativeType::new(name, mapped, true))?;
    if finer {
        return Ok(lossy.accept(
            name,
            type_name,
            field,
            "sub-microsecond precision is truncated to microseconds",
        )?);
    }
    Ok(field)
}

#[derive(Debug, Deserialize)]
struct QueryResults {
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    columns: Option<Vec<Column>>,
    data: Option<Vec<Vec<serde_json::Value>>>,
    error: Option<QueryError>,
}

#[derive(Debug, Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Debug, Deserialize)]
struct QueryError {
    message: Option<String>,
    #[serde(rename = "errorName")]
    error_name: Option<String>,
    #[serde(rename = "errorType")]
    error_type: Option<String>,
}

/// The Trino client protocol.
pub struct TrinoQuery {
    cfg: TrinoConfig,
    client: HttpClient,
    base: Uri,
    secrets: Arc<dyn SecretResolver>,
    auth: Option<Option<(bool, Secret)>>,
    mapper: SqlTypeMapper,
    lossy: Lossy,
    schema: Option<SchemaRef>,
}

impl std::fmt::Debug for TrinoQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoQuery")
            .field("url", &self.cfg.url)
            .finish_non_exhaustive()
    }
}

impl TrinoQuery {
    pub fn new(cfg: TrinoConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let base = base_url(&cfg.url, &*ctx.egress)?;
        let policy = SchemaPolicy::from_config(&serde_json::json!({
            "lossy": cfg.lossy, "evolution": cfg.evolution,
        }))?;
        Ok(Self {
            client: loams_connector_sdk::http::client(ctx.egress.clone())
                .with_timeout(Duration::from_secs(cfg.timeout_secs)),
            base,
            secrets: ctx.secrets.clone(),
            auth: None,
            mapper: SqlTypeMapper::new(policy.lossy.clone()),
            lossy: policy.lossy,
            schema: None,
            cfg,
        })
    }

    async fn request(
        &mut self,
        req: http::request::Builder,
        body: Bytes,
    ) -> Result<http::Response<Bytes>, ConnectorError> {
        if self.auth.is_none() {
            let auth = match (&self.cfg.password, &self.cfg.token) {
                (Some(p), _) => secret(&*self.secrets, Some(p)).await?.map(|s| (true, s)),
                (None, Some(t)) => secret(&*self.secrets, Some(t)).await?.map(|s| (false, s)),
                (None, None) => None,
            };
            self.auth = Some(auth);
        }
        let mut req = req
            .header("x-trino-user", &self.cfg.user)
            .header("x-trino-source", "loams")
            .header("user-agent", "loams-connector-trino");
        if let Some(c) = &self.cfg.catalog {
            req = req.header("x-trino-catalog", c);
        }
        if let Some(s) = &self.cfg.schema {
            req = req.header("x-trino-schema", s);
        }
        if let Some(Some((basic, s))) = &self.auth {
            let value = if *basic {
                let raw = format!("{}:{}", self.cfg.user, text(s, "the Trino password")?);
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(raw)
                )
            } else {
                format!("Bearer {}", text(s, "the Trino token")?)
            };
            let mut v = http::HeaderValue::from_str(&value)
                .map_err(|_| ConnectorError::auth("the Trino credential is not a header value"))?;
            v.set_sensitive(true);
            req = req.header(http::header::AUTHORIZATION, v);
        }
        let req = req
            .body(body)
            .map_err(|e| ConnectorError::fatal(format!("Trino request: {e}")))?;
        self.client.send(req).await
    }

    /// Follow `nextUri` from a response until it carries data or ends.
    async fn follow(
        &mut self,
        mut at: serde_json::Value,
        mut resp: QueryResults,
    ) -> Result<Fetch, ConnectorError> {
        loop {
            if let Some(e) = resp.error {
                let msg = format!(
                    "Trino {}: {}",
                    e.error_name.unwrap_or_default(),
                    e.message.unwrap_or_default()
                );
                return Err(match e.error_type.as_deref() {
                    Some("USER_ERROR") => ConnectorError::fatal(msg),
                    _ => ConnectorError::retryable(msg),
                });
            }
            if let Some(cols) = &resp.columns
                && self.schema.is_none()
            {
                let fields = cols
                    .iter()
                    .map(|c| trino_field(&self.mapper, &self.lossy, &c.name, &c.ty))
                    .collect::<Result<Vec<_>, _>>()?;
                self.schema = Some(Arc::new(Schema::new(fields)));
            }
            let data = resp.data.unwrap_or_default();
            if !data.is_empty() {
                let Some(schema) = self.schema.clone() else {
                    // Rows without their columns: start over.
                    return Ok(Fetch::Gone);
                };
                let rows = to_objects(&schema, data)?;
                return Ok(Fetch::Page(Page {
                    at,
                    rows: json_rows(&schema, &rows)?,
                    next: resp.next_uri.map(serde_json::Value::String),
                }));
            }
            let Some(next) = resp.next_uri else {
                return Ok(Fetch::End);
            };
            at = serde_json::Value::String(next.clone());
            match self.get(&next).await? {
                Some(r) => resp = r,
                None => return Ok(Fetch::Gone),
            }
        }
    }

    /// `GET` a `nextUri`; `None` when Trino no longer has it.
    async fn get(&mut self, uri: &str) -> Result<Option<QueryResults>, ConnectorError> {
        let u: Uri = uri
            .parse()
            .map_err(|e| ConnectorError::fatal(format!("nextUri: {e}")))?;
        // A nextUri must stay on the coordinator the instance names.
        if u.host() != self.base.host() || u.port_u16() != self.base.port_u16() {
            return Err(ConnectorError::fatal(
                "Trino's nextUri points away from the configured coordinator",
            ));
        }
        let resp = self.request(Request::get(u), Bytes::new()).await?;
        match resp.status() {
            StatusCode::NOT_FOUND | StatusCode::GONE => return Ok(None),
            StatusCode::SERVICE_UNAVAILABLE => {
                return Err(ConnectorError::retry_after(
                    Duration::from_millis(100),
                    "Trino is busy (503)",
                ));
            }
            _ => {}
        }
        parse(resp).map(Some)
    }
}

fn parse(resp: http::Response<Bytes>) -> Result<QueryResults, ConnectorError> {
    if let Some(mut e) = error_for_response(&resp) {
        e.message = format!("Trino: {}: {}", e.message, snippet(resp.body(), 300));
        return Err(e);
    }
    serde_json::from_slice(resp.body())
        .map_err(|e| ConnectorError::retryable(format!("Trino response: {e}")))
}

/// Trino's rows (JSON arrays in column order) as objects by column name.
fn to_objects(
    schema: &Schema,
    data: Vec<Vec<serde_json::Value>>,
) -> Result<Vec<serde_json::Value>, ConnectorError> {
    data.into_iter()
        .map(|row| {
            if row.len() != schema.fields().len() {
                return Err(ConnectorError::fatal(
                    "a Trino row does not match its columns",
                ));
            }
            let mut m = serde_json::Map::with_capacity(row.len());
            for (v, f) in row.into_iter().zip(schema.fields()) {
                let v = match (f.data_type(), v) {
                    // Trino renders doubles' specials as strings.
                    (DataType::Float64 | DataType::Float32, serde_json::Value::String(s))
                        if matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity") =>
                    {
                        serde_json::Value::Null
                    }
                    (_, v) => normalise(v, f)?,
                };
                m.insert(f.name().clone(), v);
            }
            Ok(serde_json::Value::Object(m))
        })
        .collect()
}

#[async_trait]
impl PagedQuery for TrinoQuery {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        self.schema = None;
        let uri = join(&self.base, "/v1/statement")?;
        let resp = self
            .request(
                Request::post(uri).header("content-type", "text/plain; charset=utf-8"),
                Bytes::from(self.cfg.query.clone()),
            )
            .await?;
        let first = parse(resp)?;
        // The POST's own page cannot be fetched again (that would be a new
        // query): its cursor is `null`, which `fetch` answers `Gone`.
        self.follow(serde_json::Value::Null, first).await
    }

    async fn fetch(&mut self, cursor: &serde_json::Value) -> Result<Fetch, ConnectorError> {
        let Some(uri) = cursor.as_str() else {
            return Ok(Fetch::Gone);
        };
        // Trino repeats `columns` on every response once it knows them, so
        // a restarted source learns them from the page it re-fetches.
        match self.get(uri).await? {
            Some(r) => self.follow(cursor.clone(), r).await,
            None => Ok(Fetch::Gone),
        }
    }

    fn query(&self) -> &str {
        &self.cfg.query
    }
}

/// The `trino` connector: a source only (Task 2's ref
/// `loams_connector_analytics::trino`).
#[derive(Debug, Default, Clone, Copy)]
pub struct Trino;

impl ConnectorFactory for Trino {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = TrinoConfig::parse(&ctx.config)?;
        // Check the URL now, so a bad config fails at start.
        TrinoQuery::new(cfg.clone(), ctx)?;
        let ctx = ctx.clone();
        let instance = ctx.instance.clone();
        Ok(Some((
            Box::new(QueryEnumerator),
            Box::new(QuerySourceFactory::new(move || {
                let q = TrinoQuery::new(cfg.clone(), &ctx);
                let q: Box<dyn PagedQuery> = match q {
                    Ok(q) => Box::new(q),
                    Err(e) => Box::new(Failed(e)),
                };
                QuerySource::new(q, CONNECTOR, &instance, "query")
            })),
        )))
    }

    /// Trino is a source only (§33 Appendix A; the manifest declares no
    /// sink).
    fn sink(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(None)
    }
}

/// A query that could not be built: every call fails with why.
#[derive(Debug)]
pub(crate) struct Failed(pub(crate) ConnectorError);

#[async_trait]
impl PagedQuery for Failed {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        Err(ConnectorError::new(
            self.0.class.clone(),
            self.0.message.clone(),
        ))
    }
    async fn fetch(&mut self, _c: &serde_json::Value) -> Result<Fetch, ConnectorError> {
        Err(ConnectorError::new(
            self.0.class.clone(),
            self.0.message.clone(),
        ))
    }
    fn query(&self) -> &str {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::ErrorClass;

    #[test]
    fn trino_types_map() {
        let l = Lossy::paths(["ts", "ns"]);
        let m = SqlTypeMapper::new(l.clone());
        assert_eq!(
            trino_field(&m, &l, "a", "bigint").expect("ok").data_type(),
            &DataType::Int64
        );
        assert_eq!(
            trino_field(&m, &l, "b", "varchar").expect("ok").data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            trino_field(&m, &l, "c", "varchar(10)")
                .expect("ok")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            trino_field(&m, &l, "d", "decimal(10,2)")
                .expect("ok")
                .data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert!(matches!(
            trino_field(&m, &l, "e", "timestamp(3) with time zone")
                .expect("ok")
                .data_type(),
            DataType::Timestamp(_, Some(_))
        ));
        assert!(trino_field(&m, &l, "ts", "timestamp(3)").is_ok());
        assert!(
            trino_field(&m, &l, "nope", "timestamp(3)").is_err(),
            "lossy unless listed"
        );
        assert!(
            trino_field(&m, &l, "ns", "timestamp(9) with time zone").is_ok(),
            "sub-microsecond accepted when listed"
        );
        assert!(trino_field(&m, &l, "e", "timestamp(9) with time zone").is_err());
        assert_eq!(
            trino_field(&m, &l, "f", "array(integer)")
                .expect_err("nested")
                .class,
            ErrorClass::SchemaIncompatible
        );
    }

    #[test]
    fn config_rules() {
        let ok =
            serde_json::json!({"url": "http://trino:8080", "user": "loams", "query": "SELECT 1"});
        assert!(TrinoConfig::parse(&ok).is_ok());
        let pw = serde_json::json!({"url": "http://trino:8080", "user": "u", "query": "SELECT 1", "password": "p"});
        assert!(TrinoConfig::parse(&pw).is_err(), "password needs https");
    }
}
