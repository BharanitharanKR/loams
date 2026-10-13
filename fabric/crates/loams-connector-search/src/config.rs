//! Instance configs of the search family. Secrets are held **by name**: a
//! credential field names a secret in the namespace's store, which the
//! connector resolves through `ConnectorContext::secrets` when it first
//! connects (R7.2's convention), so a config never carries a value.

use serde::Deserialize;
use serde_json::Value;

use loams_connector_sdk::ConnectorError;

/// Which point-in-time API the cluster speaks. An OpenSearch target profile
/// sets `api: opensearch` in its `defaults`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Api {
    #[default]
    Elasticsearch,
    Opensearch,
}

/// HTTP Basic: the user, and the **name** of the secret holding the password.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// AWS SigV4 (Amazon OpenSearch Service, the manifest's `iam` auth). The key
/// fields are secret **names**.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsAuth {
    pub region: String,
    /// `es` for OpenSearch Service domains, `aoss` for Serverless.
    #[serde(default = "default_aws_service")]
    pub service: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

fn default_aws_service() -> String {
    "es".to_owned()
}

/// The `_bulk` sink's batching and retries.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BulkConfig {
    /// Documents per `_bulk` request at most; a 429 halves the current size
    /// (never below 1) and clean requests grow it back.
    pub max_docs: usize,
    /// Bytes per `_bulk` request at most (a request is cut before it).
    pub max_bytes: usize,
    /// How often the failed items of one write are retried before the write
    /// fails (`Throttled` or `Retryable`, and the pipeline retries it).
    pub max_item_retries: u32,
    /// The first backoff between item retries; doubled each time, capped at
    /// 30 s, and a `Retry-After` overrides it.
    pub retry_backoff_ms: u64,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            max_docs: 1000,
            max_bytes: 5 << 20,
            max_item_retries: 5,
            retry_backoff_ms: 100,
        }
    }
}

/// The point-in-time source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EsSourceConfig {
    /// A field that only grows (an update timestamp, a sequence): reads are
    /// ordered by it, and a new pass starts from its last value read.
    /// Without it, every pass reads the whole index.
    pub cursor_field: Option<String>,
    /// The sort tiebreaker inside a PIT: `_shard_doc` on Elasticsearch,
    /// `_id` on OpenSearch (the profile's default; ruling R15.4).
    pub tiebreaker: Option<String>,
    pub page_size: usize,
    /// How long a PIT stays open between pages.
    pub keep_alive: String,
    /// With it, a pass that reached the end starts again this long after,
    /// from the cursor; without it, the source reads one pass and ends.
    pub poll_interval_ms: Option<u64>,
    /// An extra query (Query DSL) the read is filtered by.
    pub query: Option<Value>,
}

impl Default for EsSourceConfig {
    fn default() -> Self {
        Self {
            cursor_field: None,
            tiebreaker: None,
            page_size: 1000,
            keep_alive: "2m".to_owned(),
            poll_interval_ms: None,
            query: None,
        }
    }
}

/// An Elasticsearch or OpenSearch instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EsConfig {
    /// The cluster's base URL (`http` or `https`, no credentials in it).
    pub url: String,
    /// The index (or alias) read or written.
    pub index: String,
    #[serde(default)]
    pub api: Api,
    #[serde(default)]
    pub basic: Option<BasicAuth>,
    /// The name of the secret holding an Elasticsearch API key (the
    /// base64 `id:key` form, sent as `Authorization: ApiKey …`).
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub aws: Option<AwsAuth>,
    /// The document key: a JSON pointer (or a top-level field name) into an
    /// event's data whose value becomes `_id`. Without it, `_id` is the
    /// event's `ce_id`. Either way a replay overwrites, never duplicates.
    #[serde(default)]
    pub key: Option<String>,
    /// Apply `loamsop = d` events as deletes by `_id`.
    #[serde(default = "yes")]
    pub apply_deletes: bool,
    #[serde(default)]
    pub bulk: BulkConfig,
    #[serde(default)]
    pub source: EsSourceConfig,
    /// Per-request timeout.
    #[serde(default = "default_timeout_ms")]
    pub request_timeout_ms: u64,
    /// The SDK's `pipeline.*` overrides; read by the runtime, not here.
    #[serde(default)]
    pub pipeline: Option<Value>,
}

/// A Qdrant instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QdrantConfig {
    /// The REST endpoint (port 6333 by default).
    pub url: String,
    pub collection: String,
    /// The name of the secret holding the API key (`api-key` header).
    #[serde(default)]
    pub api_key: Option<String>,
    /// JSON pointer to the document key in an event's data; the point id is
    /// UUID v5 of it. Without it, the key is the event's `ce_id`.
    #[serde(default)]
    pub key: Option<String>,
    /// JSON pointer to the vector (an array, or an object of named vectors).
    #[serde(default = "default_vector")]
    pub vector: String,
    /// JSON pointer to the payload object. Without it, the whole data
    /// object minus the vector is the payload.
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default = "yes")]
    pub apply_deletes: bool,
    /// Points per request.
    #[serde(default = "default_points")]
    pub batch: usize,
    #[serde(default)]
    pub source: QdrantSourceConfig,
    #[serde(default = "default_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

/// The scroll source.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QdrantSourceConfig {
    pub page_size: usize,
    pub with_vector: bool,
    /// A Qdrant filter the scroll is restricted to.
    pub filter: Option<Value>,
    /// With it, a scroll that reached the end starts again from the first
    /// point this long after (unchanged points keep their event ids).
    pub poll_interval_ms: Option<u64>,
}

impl Default for QdrantSourceConfig {
    fn default() -> Self {
        Self {
            page_size: 256,
            with_vector: true,
            filter: None,
            poll_interval_ms: None,
        }
    }
}

fn yes() -> bool {
    true
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_vector() -> String {
    "/vector".to_owned()
}

fn default_points() -> usize {
    256
}

/// A key as a JSON pointer: `/a/b` as is, `a` as `/a`.
pub fn pointer(key: &str) -> String {
    if key.starts_with('/') {
        key.to_owned()
    } else {
        format!("/{key}")
    }
}

/// Parse an instance config; the error names the problem, never a value.
pub fn parse<T: serde::de::DeserializeOwned>(config: &Value) -> Result<T, ConnectorError> {
    serde_json::from_value(config.clone()).map_err(|e| {
        ConnectorError::fatal(format!("invalid instance config: {}", without_values(&e)))
    })
}

/// serde's message, cut before any quoted input it may echo.
fn without_values(e: &serde_json::Error) -> String {
    let s = e.to_string();
    match s.find("invalid value") {
        Some(i) => s[..i].trim_end().to_owned() + " invalid value",
        None => s,
    }
}

impl EsConfig {
    pub fn parse(config: &Value) -> Result<Self, ConnectorError> {
        let c: Self = parse(config)?;
        if c.bulk.max_docs == 0 || c.source.page_size == 0 {
            return Err(ConnectorError::fatal(
                "bulk.max_docs and source.page_size must be at least 1",
            ));
        }
        Ok(c)
    }

    /// The PIT sort tiebreaker, from the config or the API's default.
    pub fn tiebreaker(&self) -> &str {
        self.source.tiebreaker.as_deref().unwrap_or(match self.api {
            Api::Elasticsearch => "_shard_doc",
            Api::Opensearch => "_id",
        })
    }
}

impl QdrantConfig {
    pub fn parse(config: &Value) -> Result<Self, ConnectorError> {
        let c: Self = parse(config)?;
        if c.batch == 0 || c.source.page_size == 0 {
            return Err(ConnectorError::fatal(
                "batch and source.page_size must be at least 1",
            ));
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_and_tiebreakers() {
        let c = EsConfig::parse(&json!({"url": "http://es:9200", "index": "i"})).expect("parse");
        assert_eq!(c.api, Api::Elasticsearch);
        assert_eq!(c.tiebreaker(), "_shard_doc");
        assert!(c.apply_deletes);
        assert_eq!(c.bulk.max_docs, 1000);
        let o = EsConfig::parse(&json!({"url": "u", "index": "i", "api": "opensearch"}))
            .expect("parse");
        assert_eq!(o.tiebreaker(), "_id");
    }

    #[test]
    fn unknown_keys_refused_without_echoing_values() {
        let err = EsConfig::parse(&json!({"url": "u", "index": "i", "pasword": "hunter2-secret"}))
            .expect_err("unknown key");
        assert!(err.message.contains("pasword"), "{}", err.message);
        assert!(!err.message.contains("hunter2-secret"));
        let err = EsConfig::parse(&json!({"url": "u", "index": 7})).expect_err("bad type");
        assert!(!err.message.contains('7') || err.message.contains("invalid"));
    }

    #[test]
    fn pointers() {
        assert_eq!(pointer("id"), "/id");
        assert_eq!(pointer("/a/b"), "/a/b");
    }
}
