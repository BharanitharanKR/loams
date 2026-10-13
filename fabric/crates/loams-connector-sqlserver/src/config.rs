//! An instance's configuration (`connectors/schemas/sql-server.config.json`
//! and `azure-sql.config.json`, which Task 2 rewrites to these keys, ruling
//! R12.9). Secrets are named, never given: `password`, `entra.client_secret`
//! and `token` are secret names the instance's `SecretResolver` resolves
//! (Global Constraints, "No secret").

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, InstanceKey, Profile};
use serde_json::Value;

use crate::entra::TokenCache;

/// The system a factory serves (the manifest id's target profile).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// SQL Server itself (`sql-server`).
    SqlServer,
    /// Azure SQL Database (`azure-sql`, profile `azure-sql`): the same code,
    /// with Entra ID tokens and `encrypt: strict`.
    AzureSql,
}

impl Target {
    pub fn manifest_id(self) -> &'static str {
        match self {
            Target::SqlServer => "sql-server",
            Target::AzureSql => "azure-sql",
        }
    }
}

/// How the connection is protected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encrypt {
    /// TDS 8.0 `strict`: TLS (the SDK's rustls with ring, ALPN `tds/8.0`)
    /// before the first TDS byte, so the login and every packet after it are
    /// encrypted. The default.
    Strict,
    /// No TLS at all, the login included. Only for a server on a private
    /// network or loopback (the CI container); never for Azure SQL.
    Disable,
}

/// How the instance logs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// SQL Server authentication: `user` and the secret `password` names.
    Sql { password: Option<String> },
    /// Microsoft Entra ID, client credentials of a service principal: a token
    /// for `https://database.windows.net/.default` is fetched (and cached
    /// until near its expiry) from `authority`/`tenant_id`.
    Entra(EntraAuth),
    /// Microsoft Entra ID with an access token the runtime keeps fresh (a
    /// managed identity's sidecar, a workload identity): `token` names the
    /// secret holding it, resolved again at each connection.
    EntraToken { token: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntraAuth {
    pub tenant_id: String,
    pub client_id: String,
    /// The secret name of the client secret.
    pub client_secret: String,
    /// `https://login.microsoftonline.com` unless a sovereign cloud's.
    pub authority: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Reads of the configured tables in key order (Arrow batches): once, or
    /// incrementally by `cursor_column`.
    Batch,
    /// Change data capture over the capture instances' change tables.
    Cdc,
}

/// The isolation a read runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// `SNAPSHOT` (needs `ALLOW_SNAPSHOT_ISOLATION ON`): no shared locks,
    /// one consistent view per split run. The default.
    Snapshot,
    /// `READ COMMITTED`, for a database that cannot allow snapshot
    /// isolation.
    ReadCommitted,
}

/// A table name, `schema.table` (`dbo` when unqualified).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn parse(s: &str) -> Result<Self, ConnectorError> {
        let bad = || ConnectorError::fatal(format!("config: bad table name {s:?}"));
        let unbracket = |p: &str| -> String {
            p.strip_prefix('[')
                .and_then(|p| p.strip_suffix(']'))
                .map_or_else(|| p.to_owned(), |p| p.replace("]]", "]"))
        };
        let parts: Vec<&str> = s.split('.').collect();
        let (schema, name) = match parts.as_slice() {
            [t] => ("dbo".to_owned(), unbracket(t)),
            [s, t] => (unbracket(s), unbracket(t)),
            _ => return Err(bad()),
        };
        if schema.is_empty() || name.is_empty() || name.contains('\0') || schema.contains('\0') {
            return Err(bad());
        }
        Ok(Self { schema, name })
    }

    /// `[schema].[table]`.
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
pub struct SqlConfig {
    pub target: Target,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: Option<String>,
    pub auth: Auth,
    pub encrypt: Encrypt,
    /// Extra trusted roots (PEM) for `strict`.
    pub ca_pem: Option<String>,
    /// Accept any server certificate (still TLS). For a self-signed test
    /// server only; never the default.
    pub trust_server_certificate: bool,
    /// The name the certificate must carry, when it is not `host` (a
    /// private endpoint).
    pub host_name_in_certificate: Option<String>,
    pub mode: Mode,
    pub tables: Vec<TableName>,
    /// Reads: the key to page by (default: the primary key). Sink: the
    /// MERGE key (default: the primary key).
    pub key: Vec<String>,
    pub isolation: Isolation,
    /// Batch: key-range splits per table (a single integer key only).
    pub partitions: u32,
    /// Batch: read incrementally by this column (then the key).
    pub cursor_column: Option<String>,
    /// Batch with a cursor: wait this long after reaching the end before
    /// reading again (`None`: the read is bounded). CDC: the poll interval.
    pub poll_interval: Option<Duration>,
    /// CDC: take the initial snapshot (`snapshot: initial`, the default) or
    /// start at the current maximum LSN (`never`).
    pub snapshot: bool,
    /// CDC: how often lag and retention use are sampled.
    pub metrics_interval: Duration,
    /// CDC: the cleanup job's retention, when the instance cannot read
    /// `msdb.dbo.cdc_jobs` (Azure SQL, a least-privilege login).
    pub retention: Option<Duration>,
    /// Sink: the target table.
    pub table: Option<TableName>,
    /// Sink: stage each batch with its checkpoint (exactly-once; default).
    pub transactional: bool,
    /// Sink: the schema `_loams_checkpoints` lives in.
    pub checkpoint_schema: String,
    pub instance: InstanceKey,
    /// Entra tokens fetched for this instance (shared by its connections).
    pub tokens: TokenCache,
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

fn ms_of(v: &Value, k: &str) -> Result<Option<Duration>, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => x
            .as_u64()
            .map(|n| Some(Duration::from_millis(n)))
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

impl SqlConfig {
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
            None | Some(Value::Null) => 1433,
            Some(p) => p
                .as_u64()
                .and_then(|p| u16::try_from(p).ok())
                .ok_or_else(|| ConnectorError::fatal("config: port must be a port number"))?,
        };
        let database = str_of(v, "database")?
            .ok_or_else(|| ConnectorError::fatal("config: database is required"))?
            .to_owned();
        let user = str_of(v, "user")?.map(str::to_owned);
        let default_auth = match target {
            Target::AzureSql => "entra",
            Target::SqlServer => "sql",
        };
        let auth = match str_of(v, "auth")?.unwrap_or(default_auth) {
            "sql" | "basic" | "password" => {
                if user.is_none() {
                    return Err(ConnectorError::fatal(
                        "config: user is required for auth sql",
                    ));
                }
                Auth::Sql {
                    password: str_of(v, "password")?.map(str::to_owned),
                }
            }
            "entra" | "aad" => {
                let e = v.get("entra").cloned().unwrap_or(Value::Null);
                let need = |k: &str| -> Result<String, ConnectorError> {
                    str_of(&e, k)?.map(str::to_owned).ok_or_else(|| {
                        ConnectorError::fatal(format!(
                            "config: entra.{k} is required for auth entra"
                        ))
                    })
                };
                Auth::Entra(EntraAuth {
                    tenant_id: need("tenant_id")?,
                    client_id: need("client_id")?,
                    client_secret: need("client_secret")?,
                    authority: str_of(&e, "authority")?
                        .unwrap_or(crate::entra::DEFAULT_AUTHORITY)
                        .trim_end_matches('/')
                        .to_owned(),
                })
            }
            "entra-token" => Auth::EntraToken {
                token: str_of(v, "token")?
                    .ok_or_else(|| {
                        ConnectorError::fatal("config: token is required for auth entra-token")
                    })?
                    .to_owned(),
            },
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: auth must be sql, entra or entra-token, not {other:?}"
                )));
            }
        };
        let encrypt = match str_of(v, "encrypt")?.unwrap_or("strict") {
            "strict" => Encrypt::Strict,
            "disable" => Encrypt::Disable,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: encrypt must be strict or disable, not {other:?}"
                )));
            }
        };
        if encrypt == Encrypt::Disable && !matches!(auth, Auth::Sql { .. }) {
            // A bearer token in the clear is a credential anyone on the path
            // can replay; Azure SQL refuses unencrypted logins anyway.
            return Err(ConnectorError::fatal(
                "config: Entra ID authentication needs encrypt: strict",
            ));
        }
        let mode = match str_of(v, "mode")?.unwrap_or("batch") {
            "batch" => Mode::Batch,
            "cdc" => Mode::Cdc,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: mode must be batch or cdc, not {other:?}"
                )));
            }
        };
        let isolation = match str_of(v, "isolation")?.unwrap_or("snapshot") {
            "snapshot" => Isolation::Snapshot,
            "read_committed" => Isolation::ReadCommitted,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: isolation must be snapshot or read_committed, not {other:?}"
                )));
            }
        };
        let snapshot = match str_of(v, "snapshot")?.unwrap_or("initial") {
            "initial" => true,
            "never" => false,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: snapshot must be initial or never, not {other:?}"
                )));
            }
        };
        let partitions = match v.get("partitions") {
            None | Some(Value::Null) => 1,
            Some(p) => p
                .as_u64()
                .and_then(|p| u32::try_from(p).ok())
                .filter(|p| (1..=1024).contains(p))
                .ok_or_else(|| ConnectorError::fatal("config: partitions must be 1 to 1024"))?,
        };
        let tables = list_of(v, "tables")?
            .iter()
            .map(|t| TableName::parse(t))
            .collect::<Result<Vec<_>, _>>()?;
        let schema_name = str_of(v, "schema_name")?;
        let table = match (str_of(v, "table")?, schema_name) {
            (Some(t), Some(s)) if !t.contains('.') => Some(TableName::parse(&format!("{s}.{t}"))?),
            (Some(t), _) => Some(TableName::parse(t)?),
            (None, _) => None,
        };
        let cursor_column = str_of(v, "cursor_column")?.map(str::to_owned);
        if cursor_column.is_some() && partitions > 1 {
            return Err(ConnectorError::fatal(
                "config: cursor_column reads one split per table; drop partitions",
            ));
        }
        let poll_interval = ms_of(v, "poll_interval_ms")?;
        Ok(Self {
            target,
            host,
            port,
            database,
            user,
            auth,
            encrypt,
            ca_pem: str_of(v, "ca_pem")?.map(str::to_owned),
            trust_server_certificate: bool_of(v, "trust_server_certificate", false)?,
            host_name_in_certificate: str_of(v, "host_name_in_certificate")?.map(str::to_owned),
            mode,
            tables,
            key: list_of(v, "key")?,
            isolation,
            partitions,
            cursor_column,
            poll_interval,
            snapshot,
            metrics_interval: ms_of(v, "metrics_interval_ms")?.unwrap_or(Duration::from_secs(30)),
            retention: v
                .get("retention_minutes")
                .and_then(Value::as_u64)
                .map(|m| Duration::from_secs(m * 60)),
            table,
            transactional: bool_of(v, "transactional", true)?,
            checkpoint_schema: str_of(v, "checkpoint_schema")?.unwrap_or("dbo").to_owned(),
            instance: instance.clone(),
            tokens: TokenCache::default(),
        })
    }

    /// CDC: the interval between polls of the change tables.
    pub fn cdc_poll_interval(&self) -> Duration {
        self.poll_interval.unwrap_or(Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn inst() -> InstanceKey {
        InstanceKey::new(7, "orders-cdc")
    }

    #[test]
    fn parses_a_cdc_config_with_defaults() {
        let c = SqlConfig::parse(
            Target::SqlServer,
            &inst(),
            &json!({"host": "db", "database": "shop", "user": "loams", "password": "mssql-password",
                    "mode": "cdc", "tables": ["orders", "sales.[order items]"]}),
        )
        .expect("config");
        assert_eq!(c.port, 1433);
        assert_eq!(c.mode, Mode::Cdc);
        assert_eq!(c.encrypt, Encrypt::Strict);
        assert_eq!(c.isolation, Isolation::Snapshot);
        assert_eq!(c.tables[0].to_string(), "dbo.orders");
        assert_eq!(c.tables[1].quoted(), "[sales].[order items]");
        assert!(c.snapshot && c.transactional && !c.trust_server_certificate);
        assert_eq!(c.cdc_poll_interval(), Duration::from_secs(1));
        assert_eq!(
            c.auth,
            Auth::Sql {
                password: Some("mssql-password".into())
            }
        );
    }

    #[test]
    fn azure_sql_defaults_to_entra_and_needs_its_secret_names() {
        let base = json!({"host": "x.database.windows.net", "database": "db"});
        assert!(SqlConfig::parse(Target::AzureSql, &inst(), &base).is_err());
        let c = SqlConfig::parse(
            Target::AzureSql,
            &inst(),
            &json!({"host": "x.database.windows.net", "database": "db",
                    "entra": {"tenant_id": "t", "client_id": "c", "client_secret": "sp-secret"}}),
        )
        .expect("entra");
        let Auth::Entra(e) = &c.auth else {
            panic!("entra")
        };
        assert_eq!(e.authority, "https://login.microsoftonline.com");
        assert_eq!(e.client_secret, "sp-secret");
        assert!(c.user.is_none());
    }

    #[test]
    fn a_token_is_never_sent_in_the_clear() {
        let r = SqlConfig::parse(
            Target::SqlServer,
            &inst(),
            &json!({"host": "h", "database": "d", "auth": "entra-token", "token": "t",
                    "encrypt": "disable"}),
        );
        assert!(r.expect_err("refused").message.contains("strict"));
    }

    #[test]
    fn bad_values_are_refused() {
        for bad in [
            json!({"database": "d", "user": "u"}),
            json!({"host": "h", "user": "u"}),
            json!({"host": "h", "database": "d"}),
            json!({"host": "h", "database": "d", "user": "u", "mode": "stream"}),
            json!({"host": "h", "database": "d", "user": "u", "encrypt": "true"}),
            json!({"host": "h", "database": "d", "user": "u", "tables": ["a.b.c"]}),
            json!({"host": "h", "database": "d", "user": "u", "port": 70000}),
            json!({"host": "h", "database": "d", "user": "u", "partitions": 0}),
            json!({"host": "h", "database": "d", "user": "u", "partitions": 4, "cursor_column": "c"}),
            json!({"host": "h", "database": "d", "user": "u", "isolation": "serializable"}),
        ] {
            assert!(
                SqlConfig::parse(Target::SqlServer, &inst(), &bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn profile_defaults_merge_under_the_config() {
        let p: Profile = serde_json::from_value(json!({
            "id": "azure-sql", "profile_kind": "target", "module": "sqlserver",
            "system": {"name": "Azure SQL Database", "service": {"nightly": ["LOAMS_AZURE_SQL_URL"]}},
            "defaults": {"encrypt": "strict", "auth": "entra"}
        }))
        .expect("profile");
        let merged = merge_defaults(&json!({"host": "h", "auth": "sql"}), Some(&p));
        assert_eq!(
            merged,
            json!({"host": "h", "auth": "sql", "encrypt": "strict"})
        );
    }
}
