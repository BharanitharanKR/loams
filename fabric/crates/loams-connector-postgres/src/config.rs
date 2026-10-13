//! An instance's configuration (`connectors/schemas/postgresql.config.json`
//! and its targets). Secrets are named, never given: `password` and the
//! `aws.*` credentials are secret names the instance's `SecretResolver`
//! resolves (Global Constraints, "No secret").

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, InstanceKey, Profile};
use serde_json::Value;

/// The system a factory serves (the manifest id's target profile).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// PostgreSQL itself (`postgresql`).
    Postgres,
    /// Amazon Aurora, PostgreSQL engine (`aurora`, profile `aurora-postgres`).
    Aurora,
    /// CockroachDB (`cockroachdb`): changefeeds and `SELECT` batches (R0.20).
    Cockroach,
    /// pgvector (`pgvector`): Postgres with `vector` columns.
    Pgvector,
}

impl Target {
    pub fn manifest_id(self) -> &'static str {
        match self {
            Target::Postgres => "postgresql",
            Target::Aurora => "aurora",
            Target::Cockroach => "cockroachdb",
            Target::Pgvector => "pgvector",
        }
    }

    pub fn default_port(self) -> u16 {
        match self {
            Target::Cockroach => 26257,
            _ => 5432,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    /// Encrypt, without verifying the server certificate.
    Require,
    /// Encrypt and verify the certificate chain and the host name.
    VerifyFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Bounded reads of the configured tables (Arrow batches).
    Batch,
    /// Change data capture: pgoutput on Postgres, changefeeds on CockroachDB.
    Cdc,
}

/// How the instance logs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// `password` names a secret (absent: trust or peer auth).
    Basic { password: Option<String> },
    /// AWS RDS IAM tokens (Aurora; `auth: iam`).
    Iam(AwsAuth),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsAuth {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// A table name, `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn parse(s: &str) -> Result<Self, ConnectorError> {
        let parts: Vec<&str> = s.split('.').collect();
        let (schema, name) = match parts.as_slice() {
            [t] => ("public", *t),
            [s, t] => (*s, *t),
            _ => {
                return Err(ConnectorError::fatal(format!(
                    "config: bad table name {s:?}"
                )));
            }
        };
        if schema.is_empty() || name.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "config: bad table name {s:?}"
            )));
        }
        Ok(Self {
            schema: schema.to_owned(),
            name: name.to_owned(),
        })
    }

    /// `"schema"."table"`.
    pub fn quoted(&self) -> String {
        format!(
            "{}.{}",
            crate::types::quote_ident(&self.schema),
            crate::types::quote_ident(&self.name)
        )
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// The parsed instance config.
#[derive(Debug, Clone)]
pub struct PgConfig {
    pub target: Target,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub auth: Auth,
    pub ssl: SslMode,
    /// Extra trusted roots (PEM), e.g. the Amazon RDS CA bundle.
    pub ca_pem: Option<String>,
    pub mode: Mode,
    pub tables: Vec<TableName>,
    /// Batch reads: the key to page by (default: the primary key).
    pub key: Vec<String>,
    /// CDC: a user-named publication (default `loams_<instance>`, created
    /// and owned by Loams).
    pub publication: Option<String>,
    /// CDC: the slot name (default `loams_<ns>_<instance>`).
    pub slot: Option<String>,
    /// CDC: take the initial snapshot (`snapshot: initial`, the default) or
    /// start streaming at the slot's consistent point (`never`).
    pub snapshot: bool,
    /// CDC: ask for in-progress transactions (pgoutput `streaming`, proto 2+).
    pub streaming: bool,
    /// CDC: the most a streamed in-progress transaction may buffer.
    pub stream_buffer_bytes: usize,
    /// CDC: the idle heartbeat (`pg_logical_emit_message`); zero disables.
    pub heartbeat: Duration,
    /// CDC: the Standby Status Update interval (also the keepalive).
    pub status_interval: Duration,
    /// CockroachDB changefeeds: the `resolved` interval.
    pub resolved: Duration,
    /// Sink: the target table and its upsert key.
    pub table: Option<TableName>,
    pub sink_key: Vec<String>,
    /// Sink: stage each batch with its checkpoint (exactly-once; default).
    pub transactional: bool,
    /// Sink: the schema `_loams_checkpoints` lives in.
    pub checkpoint_schema: String,
    pub instance: InstanceKey,
}

fn str_of<'a>(v: &'a Value, k: &str) -> Result<Option<&'a str>, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.as_str())),
        Some(_) => Err(ConnectorError::fatal(format!(
            "config: {k} must be a string"
        ))),
    }
}

fn list_of(v: &Value, k: &str) -> Result<Vec<String>, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(vec![s.clone()]),
        Some(Value::Array(a)) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ConnectorError::fatal(format!("config: {k} must list strings")))
            })
            .collect(),
        Some(_) => Err(ConnectorError::fatal(format!("config: {k} must be a list"))),
    }
}

fn ms_of(v: &Value, k: &str, default: Duration) -> Result<Duration, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(x) => x
            .as_u64()
            .map(Duration::from_millis)
            .ok_or_else(|| ConnectorError::fatal(format!("config: {k} must be milliseconds"))),
    }
}

fn bool_of(v: &Value, k: &str, default: bool) -> Result<bool, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(_) => Err(ConnectorError::fatal(format!(
            "config: {k} must be a boolean"
        ))),
    }
}

/// `profile.defaults` merged under `config` (the user's keys win).
pub fn merge_defaults(config: &Value, profile: Option<&Profile>) -> Value {
    match profile {
        Some(p) => p.config_with_defaults(config),
        None => config.clone(),
    }
}

/// A Postgres identifier from free text: lowercase ASCII letters, digits and
/// `_`, at most 63 bytes (NAMEDATALEN - 1).
pub fn identifier(raw: &str) -> String {
    let mut s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    s.truncate(63);
    s
}

impl PgConfig {
    pub fn parse(
        target: Target,
        instance: &InstanceKey,
        v: &Value,
    ) -> Result<Self, ConnectorError> {
        if !v.is_object() {
            return Err(ConnectorError::fatal("config: must be an object"));
        }
        let host = str_of(v, "host")?
            .ok_or_else(|| ConnectorError::fatal("config: host is required"))?
            .to_owned();
        let port = match v.get("port") {
            None | Some(Value::Null) => target.default_port(),
            Some(p) => p
                .as_u64()
                .and_then(|p| u16::try_from(p).ok())
                .ok_or_else(|| ConnectorError::fatal("config: port must be a port number"))?,
        };
        let database = str_of(v, "database")?
            .unwrap_or(if target == Target::Cockroach {
                "defaultdb"
            } else {
                "postgres"
            })
            .to_owned();
        let user = str_of(v, "user")?
            .ok_or_else(|| ConnectorError::fatal("config: user is required"))?
            .to_owned();
        let auth = match str_of(v, "auth")?.unwrap_or("basic") {
            "basic" | "password" => Auth::Basic {
                password: str_of(v, "password")?.map(str::to_owned),
            },
            "iam" => {
                let aws = v.get("aws").cloned().unwrap_or(Value::Null);
                let need = |k: &str| -> Result<String, ConnectorError> {
                    str_of(&aws, k)?.map(str::to_owned).ok_or_else(|| {
                        ConnectorError::fatal(format!("config: aws.{k} is required for auth iam"))
                    })
                };
                Auth::Iam(AwsAuth {
                    region: need("region")?,
                    access_key_id: need("access_key_id")?,
                    secret_access_key: need("secret_access_key")?,
                    session_token: str_of(&aws, "session_token")?.map(str::to_owned),
                })
            }
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: auth must be basic or iam, not {other:?}"
                )));
            }
        };
        let ssl = match str_of(v, "sslmode")?.unwrap_or(match auth {
            // IAM tokens are only accepted over TLS.
            Auth::Iam(_) => "verify-full",
            Auth::Basic { .. } => "disable",
        }) {
            "disable" => SslMode::Disable,
            "require" => SslMode::Require,
            "verify-full" => SslMode::VerifyFull,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: sslmode must be disable, require or verify-full, not {other:?}"
                )));
            }
        };
        let mode = match str_of(v, "mode")?.unwrap_or("batch") {
            "batch" => Mode::Batch,
            "cdc" => Mode::Cdc,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: mode must be batch or cdc, not {other:?}"
                )));
            }
        };
        let tables = list_of(v, "tables")?
            .iter()
            .map(|t| TableName::parse(t))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = match str_of(v, "snapshot")?.unwrap_or("initial") {
            "initial" => true,
            "never" => false,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: snapshot must be initial or never, not {other:?}"
                )));
            }
        };
        // CN1's `schema_name` qualifies an unqualified `table`.
        let schema_name = str_of(v, "schema_name")?;
        let table = match (str_of(v, "table")?, schema_name) {
            (Some(t), Some(s)) if !t.contains('.') => Some(TableName::parse(&format!("{s}.{t}"))?),
            (Some(t), _) => Some(TableName::parse(t)?),
            (None, _) => None,
        };
        Ok(Self {
            target,
            host,
            port,
            database,
            user,
            auth,
            ssl,
            ca_pem: str_of(v, "ca_pem")?.map(str::to_owned),
            mode,
            tables,
            key: list_of(v, "key")?,
            publication: str_of(v, "publication")?.map(str::to_owned),
            slot: str_of(v, "slot")?.map(str::to_owned),
            snapshot,
            streaming: bool_of(v, "streaming", false)?,
            stream_buffer_bytes: v
                .get("stream_buffer_bytes")
                .and_then(Value::as_u64)
                .map_or(64 << 20, |b| b as usize),
            heartbeat: ms_of(v, "heartbeat_ms", Duration::from_secs(10))?,
            status_interval: ms_of(v, "status_interval_ms", Duration::from_secs(10))?,
            resolved: ms_of(v, "resolved_ms", Duration::from_secs(1))?,
            sink_key: list_of(v, "key")?,
            table,
            transactional: bool_of(v, "transactional", true)?,
            checkpoint_schema: str_of(v, "checkpoint_schema")?
                .unwrap_or("public")
                .to_owned(),
            instance: instance.clone(),
        })
    }

    /// The slot: configured, or `loams_<ns>_<instance>`.
    pub fn slot_name(&self) -> String {
        self.slot.clone().unwrap_or_else(|| {
            identifier(&format!(
                "loams_{}_{}",
                self.instance.namespace, self.instance.name
            ))
        })
    }

    /// The publication: configured, or `loams_<instance>`; the second value
    /// says whether Loams owns it (creates, alters and drops it).
    pub fn publication_name(&self) -> (String, bool) {
        match &self.publication {
            Some(p) => (p.clone(), false),
            None => (identifier(&format!("loams_{}", self.instance.name)), true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn inst() -> InstanceKey {
        InstanceKey::new(7, "Orders-CDC")
    }

    #[test]
    fn parses_a_cdc_config_with_defaults() {
        let c = PgConfig::parse(
            Target::Postgres,
            &inst(),
            &json!({"host": "db", "user": "loams", "password": "pg-password", "mode": "cdc",
                    "tables": ["orders", "sales.items"]}),
        )
        .expect("config");
        assert_eq!(c.port, 5432);
        assert_eq!(c.mode, Mode::Cdc);
        assert_eq!(c.tables[0].to_string(), "public.orders");
        assert_eq!(c.tables[1].quoted(), "\"sales\".\"items\"");
        assert_eq!(c.slot_name(), "loams_7_orders_cdc");
        assert_eq!(c.publication_name(), ("loams_orders_cdc".to_owned(), true));
        assert!(c.snapshot && c.transactional && !c.streaming);
        assert_eq!(
            c.auth,
            Auth::Basic {
                password: Some("pg-password".into())
            }
        );
    }

    #[test]
    fn iam_requires_aws_secret_names_and_tls() {
        let base = json!({"host": "aurora.example", "user": "u", "auth": "iam"});
        assert!(PgConfig::parse(Target::Aurora, &inst(), &base).is_err());
        let c = PgConfig::parse(
            Target::Aurora,
            &inst(),
            &json!({"host": "aurora.example", "user": "u", "auth": "iam",
                    "aws": {"region": "eu-west-1", "access_key_id": "k", "secret_access_key": "s"}}),
        )
        .expect("iam");
        assert_eq!(c.ssl, SslMode::VerifyFull);
        assert!(matches!(c.auth, Auth::Iam(_)));
    }

    #[test]
    fn bad_values_are_refused() {
        for bad in [
            json!({"user": "u"}),
            json!({"host": "h", "user": "u", "mode": "stream"}),
            json!({"host": "h", "user": "u", "sslmode": "prefer"}),
            json!({"host": "h", "user": "u", "tables": ["a.b.c"]}),
            json!({"host": "h", "user": "u", "port": 70000}),
        ] {
            assert!(
                PgConfig::parse(Target::Postgres, &inst(), &bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn profile_defaults_merge_under_the_config() {
        let p: Profile = serde_json::from_value(json!({
            "id": "cockroachdb", "profile_kind": "target", "module": "postgres",
            "system": {"name": "CockroachDB", "service": {"compose": "cockroachdb"}},
            "defaults": {"port": 26257, "sslmode": "require"}
        }))
        .expect("profile");
        let merged = merge_defaults(&json!({"host": "h", "sslmode": "disable"}), Some(&p));
        assert_eq!(
            merged,
            json!({"host": "h", "port": 26257, "sslmode": "disable"})
        );
    }
}
