//! An instance's config, as the family reads it.
//!
//! Secrets are named, never given (R7.2's convention, Review Focus 8): the
//! `auth` block holds the **names** of secrets the instance's
//! [`SecretResolver`](loams_connector_sdk::SecretResolver) resolves. A
//! profile's `defaults` are merged under the instance's config (a target
//! profile such as `cloudflare-r2` sets `region: auto`).

use std::time::Duration;

use loams_connector_sdk::ConnectorError;
use loams_connector_sdk::schema::arrow::SqlTypeMapper;
use loams_connector_sdk::schema::{Lossy, NativeType, TypeMapper};
use serde::Deserialize;

/// The default roll size of the sink: 128 MiB (CN2 Task 13).
pub const DEFAULT_ROLL_BYTES: u64 = 128 << 20;
/// The default roll interval of the sink: 5 minutes (CN2 Task 13).
pub const DEFAULT_ROLL_INTERVAL_SECS: u64 = 300;
/// The largest object the source decodes or carries whole: 64 MiB.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 64 << 20;

/// One instance's settings, for every store of the family but SFTP (which
/// reads [`crate::sftp::SftpConfig`] beside these).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ObjectsConfig {
    /// The bucket (S3, R2, MinIO/RustFS, GCS), container (Azure Blob) or
    /// file system (ADLS Gen2).
    #[serde(alias = "container", alias = "filesystem")]
    pub bucket: String,
    /// The key prefix the source lists under and the sink writes under.
    pub prefix: String,
    /// Several prefixes, one source split each; `prefix` when empty.
    pub prefixes: Vec<String>,
    /// An S3-compatible, GCS or Azure endpoint (RustFS, MinIO, R2's account
    /// endpoint, fake-gcs-server, Azurite). The cloud's own when absent.
    pub endpoint: Option<String>,
    /// The S3 region (`auto` for R2).
    pub region: Option<String>,
    /// Allow plain HTTP to `endpoint` (development and CI stores only).
    pub allow_http: bool,
    /// S3 virtual-hosted-style requests (`https://<bucket>.<endpoint>`).
    pub virtual_hosted_style: bool,
    /// The Azure storage account (Azure Blob, ADLS Gen2).
    pub account: Option<String>,
    /// ADLS Gen2: the account has a hierarchical namespace, so the sink
    /// publishes by an atomic DFS rename (R0.21).
    pub hns: bool,
    /// ADLS Gen2: the DFS endpoint, `https://<account>.dfs.core.windows.net`
    /// when absent.
    pub dfs_endpoint: Option<String>,
    pub auth: AuthNames,
    pub source: SourceConfig,
    pub sink: SinkConfig,
}

/// Names of the secrets an instance authenticates with. Which ones apply
/// depends on the store; none of them is a value.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthNames {
    /// S3: a static access key id and secret access key, and an optional
    /// session token. Absent: the AWS credential chain of the pod.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// GCS: a service account key (JSON). Absent: the pod's metadata server.
    pub service_account_key: Option<String>,
    /// Azure: the account's shared key.
    pub access_key: Option<String>,
    /// Azure: a SAS token (`sv=…&sig=…`).
    pub sas_token: Option<String>,
    /// Azure: an Entra ID bearer token.
    pub bearer_token: Option<String>,
}

/// What the source makes of each listed object.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Decode {
    /// One `object` event per object, its metadata as JSON data.
    #[default]
    Metadata,
    /// One `object` event per object carrying the object's bytes.
    Bytes,
    /// Rows: Arrow batches decoded by the SDK's CSV codec (D356).
    Csv,
    /// Rows: Arrow batches decoded by the SDK's NDJSON codec.
    Ndjson,
    /// Rows: Arrow batches read from a Parquet object.
    Parquet,
}

/// A column of a CSV or NDJSON object, typed with the SDK's SQL type names
/// (`docs/guides/connectors/schema-mapping.md`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default = "yes")]
    pub nullable: bool,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceConfig {
    pub decode: Decode,
    /// The columns of CSV and NDJSON objects (Parquet carries its own schema).
    pub columns: Vec<Column>,
    /// CSV objects start with a header line.
    pub csv_header: bool,
    /// Re-list this long after a listing ends. Absent: one listing, then the
    /// source is finished (a bounded read).
    pub poll_interval_ms: Option<u64>,
    /// Objects larger than this are not decoded or carried; they go to the
    /// DLQ as `Poison` (R4.7) and the listing moves past them.
    pub max_object_bytes: u64,
    /// Rows per decoded batch before the budget cuts it.
    pub decode_batch_rows: usize,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            decode: Decode::Metadata,
            columns: Vec::new(),
            csv_header: true,
            poll_interval_ms: None,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            decode_batch_rows: 8192,
        }
    }
}

/// The file format the sink writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkFormat {
    /// Parquet; events become rows of the CloudEvents envelope schema.
    #[default]
    Parquet,
    /// NDJSON; events become structured-mode CloudEvents, one per line.
    Ndjson,
    /// The events' data, each followed by `raw_separator`.
    Raw,
}

impl SinkFormat {
    pub fn extension(self) -> &'static str {
        match self {
            SinkFormat::Parquet => "parquet",
            SinkFormat::Ndjson => "ndjson",
            SinkFormat::Raw => "bin",
        }
    }
}

/// Parquet compression.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    Zstd,
    Snappy,
    Gzip,
    None,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SinkConfig {
    pub format: SinkFormat,
    /// Roll a file once this many staged bytes are waiting (128 MiB).
    pub roll_bytes: u64,
    /// Roll a file once its first row has waited this long (5 min).
    pub roll_interval_secs: u64,
    /// Publish with `If-None-Match: *` (S3, R2, MinIO/RustFS), generation
    /// match 0 (GCS) or `If-None-Match: *` (Azure). Off only for a store that
    /// refuses conditional writes, which makes the sink at-least-once.
    pub conditional_put: bool,
    pub compression: Compression,
    /// Raw format: written after each event's data.
    pub raw_separator: String,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            format: SinkFormat::Parquet,
            roll_bytes: DEFAULT_ROLL_BYTES,
            roll_interval_secs: DEFAULT_ROLL_INTERVAL_SECS,
            conditional_put: true,
            compression: Compression::Zstd,
            raw_separator: "\n".to_owned(),
        }
    }
}

impl SinkConfig {
    pub fn roll_interval(&self) -> Duration {
        Duration::from_secs(self.roll_interval_secs)
    }
}

impl ObjectsConfig {
    /// Parse an instance config with a profile's `defaults` under it.
    pub fn parse(
        config: &serde_json::Value,
        defaults: Option<&serde_json::Value>,
    ) -> Result<Self, ConnectorError> {
        let merged = merged(config, defaults);
        let cfg: ObjectsConfig = serde_json::from_value(merged)
            .map_err(|e| ConnectorError::fatal(format!("objects config: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.sink.roll_bytes == 0 {
            return Err(ConnectorError::fatal("sink.roll_bytes must be positive"));
        }
        self.source.validate()
    }

    /// The source's prefixes (one split each), normalised without a
    /// trailing `/`.
    pub fn source_prefixes(&self) -> Vec<String> {
        let list = if self.prefixes.is_empty() {
            vec![self.prefix.clone()]
        } else {
            self.prefixes.clone()
        };
        let mut out: Vec<String> = list.iter().map(|p| normalise_prefix(p)).collect();
        out.sort();
        out.dedup();
        out
    }
}

impl SourceConfig {
    /// The Arrow schema of CSV and NDJSON objects.
    pub fn arrow_schema(&self) -> Result<arrow_schema::SchemaRef, ConnectorError> {
        let mapper = SqlTypeMapper::new(Lossy::none());
        let fields = self
            .columns
            .iter()
            .map(|c| mapper.to_arrow(&NativeType::new(&c.name, &c.type_name, c.nullable)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConnectorError::from)?;
        Ok(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.decode_batch_rows == 0 {
            return Err(ConnectorError::fatal(
                "source.decode_batch_rows must be positive",
            ));
        }
        if matches!(self.decode, Decode::Csv | Decode::Ndjson) && self.columns.is_empty() {
            return Err(ConnectorError::fatal(
                "source.columns is required to decode CSV or NDJSON objects",
            ));
        }
        Ok(())
    }
}

/// A prefix without leading or trailing `/`.
pub fn normalise_prefix(p: &str) -> String {
    p.trim_matches('/').to_owned()
}

/// `config` over `defaults`, objects merged key by key.
pub fn merged(
    config: &serde_json::Value,
    defaults: Option<&serde_json::Value>,
) -> serde_json::Value {
    fn merge(base: &mut serde_json::Value, over: &serde_json::Value) {
        match (base, over) {
            (serde_json::Value::Object(b), serde_json::Value::Object(o)) => {
                for (k, v) in o {
                    merge(b.entry(k.clone()).or_insert(serde_json::Value::Null), v);
                }
            }
            (b, o) => *b = o.clone(),
        }
    }
    let mut out = match defaults {
        Some(d @ serde_json::Value::Object(_)) => d.clone(),
        _ => serde_json::Value::Object(Default::default()),
    };
    merge(&mut out, config);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn profile_defaults_sit_under_the_instance() {
        let cfg = ObjectsConfig::parse(
            &json!({"bucket": "b", "sink": {"format": "ndjson"}}),
            Some(&json!({"region": "auto", "sink": {"roll_bytes": 10, "format": "parquet"}})),
        )
        .expect("parses");
        assert_eq!(cfg.region.as_deref(), Some("auto"));
        assert_eq!(cfg.sink.format, SinkFormat::Ndjson);
        assert_eq!(cfg.sink.roll_bytes, 10);
        assert!(cfg.sink.conditional_put);
    }

    #[test]
    fn prefixes_are_normalised_and_deduplicated() {
        let cfg = ObjectsConfig::parse(
            &json!({"bucket": "b", "prefixes": ["/a/", "a", "b/c/"]}),
            None,
        )
        .expect("parses");
        assert_eq!(cfg.source_prefixes(), vec!["a", "b/c"]);
        let one = ObjectsConfig::parse(&json!({"bucket": "b"}), None).expect("parses");
        assert_eq!(one.source_prefixes(), vec![""]);
    }

    #[test]
    fn auth_holds_names_only_and_refuses_unknown_keys() {
        let err = ObjectsConfig::parse(&json!({"bucket": "b", "auth": {"password": "x"}}), None)
            .expect_err("unknown auth key");
        assert!(err.message.contains("password"), "{}", err.message);
    }

    #[test]
    fn csv_needs_columns() {
        assert!(
            ObjectsConfig::parse(&json!({"bucket": "b", "source": {"decode": "csv"}}), None)
                .is_err()
        );
        let cfg = ObjectsConfig::parse(
            &json!({"bucket": "b", "source": {"decode": "csv", "columns": [
                {"name": "id", "type": "bigint", "nullable": false},
                {"name": "name", "type": "text"}
            ]}}),
            None,
        )
        .expect("parses");
        let schema = cfg.source.arrow_schema().expect("schema");
        assert_eq!(schema.fields().len(), 2);
        assert!(!schema.field(0).is_nullable());
    }
}
