//! An instance's configuration. The keys of CN1's
//! `connectors/schemas/mysql.config.json` keep their meaning (`host`, `port`,
//! `database`, `user`, `password`, `key`, `chunk_size`, `apply_deletes`,
//! `tls`, `iam`), and CDC adds its own (the guide's table). Secrets are
//! named, never given: `password` and the `aws.*` credentials are secret
//! names the instance's `SecretResolver` resolves (Global Constraints, "No
//! secret").

use std::time::Duration;

use loams_connector_sdk::{ConnectorError, InstanceKey, Profile};
use serde_json::Value;

/// The system a factory serves (the manifest id's target profile).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// MySQL itself (`mysql`).
    Mysql,
    /// MariaDB (`mariadb`, profile `mariadb`): MariaDB's GTIDs and events.
    Mariadb,
    /// Amazon Aurora, MySQL engine (`aurora`, profile `aurora-mysql`).
    Aurora,
}

impl Target {
    pub fn manifest_id(self) -> &'static str {
        match self {
            Target::Mysql => "mysql",
            Target::Mariadb => "mariadb",
            Target::Aurora => "aurora",
        }
    }
}

/// How the connection is encrypted (CN1's `tls` block).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    /// Encrypt, without verifying the certificate (`verify_full: false`).
    Require,
    /// Encrypt and verify the chain and the host name (the default once
    /// `tls.enabled` is set).
    VerifyFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Bounded, chunked reads of the configured tables (Arrow batches).
    Batch,
    /// Row-binlog change data capture.
    Cdc,
}

/// How the initial CDC snapshot is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locking {
    /// The DBLog watermark algorithm: no lock, needs write access to the
    /// watermark table.
    Watermark,
    /// A brief `FLUSH TABLES WITH READ LOCK` around the snapshot's start.
    GlobalReadLock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// `password` names a secret (absent: an account without one).
    Basic { password: Option<String> },
    /// AWS RDS IAM tokens (Aurora; `auth: iam`, or CN1's `iam` block).
    Iam(AwsAuth),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsAuth {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// A table, `database.table`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub database: String,
    pub name: String,
}

impl TableName {
    /// `t` (in `default_db`) or `db.t`.
    pub fn parse(s: &str, default_db: &str) -> Result<Self, ConnectorError> {
        let bad = || ConnectorError::fatal(format!("config: bad table name {s:?}"));
        let (database, name) = match s.split('.').collect::<Vec<_>>().as_slice() {
            [t] => (default_db, *t),
            [d, t] => (*d, *t),
            _ => return Err(bad()),
        };
        if database.is_empty() || name.is_empty() {
            return Err(bad());
        }
        Ok(Self {
            database: database.to_owned(),
            name: name.to_owned(),
        })
    }

    pub fn new(database: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            name: name.into(),
        }
    }

    /// `` `db`.`t` ``.
    pub fn quoted(&self) -> String {
        format!(
            "{}.{}",
            crate::types::quote_ident(&self.database),
            crate::types::quote_ident(&self.name)
        )
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.name)
    }
}

/// The parsed instance config.
#[derive(Debug, Clone)]
pub struct MyConfig {
    pub target: Target,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub auth: Auth,
    pub tls: TlsMode,
    /// Extra trusted roots (PEM), e.g. the Amazon RDS CA bundle.
    pub ca_pem: Option<String>,
    pub mode: Mode,
    /// Sources: the tables read or captured (CDC with none: every base
    /// table of `database`).
    pub tables: Vec<TableName>,
    /// Batch reads: the key to page by (default: the primary key). Sink:
    /// the upsert key (default: the primary key).
    pub key: Vec<String>,
    /// Rows per chunked `SELECT` (batch reads and the CDC snapshot), capped
    /// by the pipeline's budget.
    pub chunk_size: usize,
    /// CDC: the replica server id (default: derived from the instance).
    pub server_id: u32,
    /// CDC: take the initial snapshot (`initial`, the default) or start at
    /// the server's current position (`never`).
    pub snapshot: bool,
    pub locking: Locking,
    /// CDC: the DBLog watermark table (default `<database>._loams_watermarks`).
    pub watermark_table: TableName,
    /// CDC: the server's heartbeat period while the binlog is idle.
    pub heartbeat: Duration,
    /// Sink: the target table.
    pub table: Option<TableName>,
    /// Sink: stage each batch with its checkpoint (exactly-once; default).
    pub transactional: bool,
    /// Sink: the database `_loams_checkpoints` lives in (default `database`).
    pub checkpoint_database: String,
    /// Sink: apply `loamsop = d` as deletes (CN1's `apply_deletes`).
    pub apply_deletes: bool,
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

/// A list of strings; a single string is a comma-separated list (CN1's
/// `key: "a,b"`).
fn list_of(v: &Value, k: &str) -> Result<Vec<String>, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()),
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

fn u64_of(v: &Value, k: &str) -> Result<Option<u64>, ConnectorError> {
    match v.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => x
            .as_u64()
            .map(Some)
            .ok_or_else(|| ConnectorError::fatal(format!("config: {k} must be a whole number"))),
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

/// The replica server id for an instance: in `[2^24, 2^32)`, away from the
/// small ids servers are usually given, and stable across restarts.
pub fn derived_server_id(instance: &InstanceKey) -> u32 {
    // FNV-1a over "<namespace>/<name>".
    let mut h: u32 = 0x811c_9dc5;
    for b in instance.to_string().bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    ((h >> 24).max(1) << 24) | (h & 0x00ff_ffff)
}

impl MyConfig {
    pub fn parse(
        target: Target,
        instance: &InstanceKey,
        v: &Value,
    ) -> Result<Self, ConnectorError> {
        if !v.is_object() {
            return Err(ConnectorError::fatal("config: must be an object"));
        }
        if let Some(engine) = str_of(v, "engine")?
            && !engine.eq_ignore_ascii_case("mysql")
        {
            return Err(ConnectorError::fatal(format!(
                "config: engine {engine:?} is not served by the MySQL family"
            )));
        }
        let host = str_of(v, "host")?
            .ok_or_else(|| ConnectorError::fatal("config: host is required"))?
            .to_owned();
        let port = match u64_of(v, "port")? {
            None => 3306,
            Some(p) => u16::try_from(p)
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| ConnectorError::fatal("config: port must be a port number"))?,
        };
        let database = str_of(v, "database")?
            .ok_or_else(|| ConnectorError::fatal("config: database is required"))?
            .to_owned();
        let user = str_of(v, "user")?
            .ok_or_else(|| ConnectorError::fatal("config: user is required"))?
            .to_owned();
        let iam_block = v.get("iam").filter(|x| !x.is_null());
        let auth_kind =
            str_of(v, "auth")?.unwrap_or(if iam_block.is_some() { "iam" } else { "basic" });
        let auth = match auth_kind {
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
                let region = match str_of(&aws, "region")? {
                    Some(r) => r.to_owned(),
                    None => iam_block
                        .and_then(|b| b.get("region"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            ConnectorError::fatal("config: aws.region (or iam.region) is required")
                        })?,
                };
                Auth::Iam(AwsAuth {
                    region,
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
        let tls_block = v.get("tls").cloned().unwrap_or(Value::Null);
        let enabled = bool_of(&tls_block, "enabled", matches!(auth, Auth::Iam(_)))?;
        let tls = match (enabled, bool_of(&tls_block, "verify_full", true)?) {
            (false, _) => TlsMode::Disabled,
            (true, false) => TlsMode::Require,
            (true, true) => TlsMode::VerifyFull,
        };
        if tls == TlsMode::Disabled && matches!(auth, Auth::Iam(_)) {
            return Err(ConnectorError::fatal(
                "config: IAM tokens are sent in clear text, so auth iam requires tls.enabled",
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
        let tables = list_of(v, "tables")?
            .iter()
            .map(|t| TableName::parse(t, &database))
            .collect::<Result<Vec<_>, _>>()?;
        // `snapshot` is `initial`/`never`, or `{mode, locking}`.
        let (snap_mode, locking) = match v.get("snapshot") {
            None | Some(Value::Null) => ("initial".to_owned(), "watermark".to_owned()),
            Some(Value::String(s)) => (s.clone(), "watermark".to_owned()),
            Some(o @ Value::Object(_)) => (
                str_of(o, "mode")?.unwrap_or("initial").to_owned(),
                str_of(o, "locking")?.unwrap_or("watermark").to_owned(),
            ),
            Some(_) => {
                return Err(ConnectorError::fatal(
                    "config: snapshot must be a string or an object",
                ));
            }
        };
        let snapshot = match snap_mode.as_str() {
            "initial" => true,
            "never" => false,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: snapshot must be initial or never, not {other:?}"
                )));
            }
        };
        let locking = match locking.as_str() {
            "watermark" | "none" => Locking::Watermark,
            "global-read-lock" => Locking::GlobalReadLock,
            other => {
                return Err(ConnectorError::fatal(format!(
                    "config: snapshot.locking must be watermark or global-read-lock, not {other:?}"
                )));
            }
        };
        let watermark_table = TableName::parse(
            str_of(v, "watermark_table")?.unwrap_or("_loams_watermarks"),
            &database,
        )?;
        let server_id = match u64_of(v, "server_id")? {
            None => derived_server_id(instance),
            Some(id) => u32::try_from(id)
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| ConnectorError::fatal("config: server_id must be 1 to 2^32-1"))?,
        };
        let chunk_size = u64_of(v, "chunk_size")?.unwrap_or(8192).clamp(1, 1 << 20) as usize;
        let table = str_of(v, "table")?
            .map(|t| TableName::parse(t, &database))
            .transpose()?;
        Ok(Self {
            target,
            host,
            port,
            user,
            auth,
            tls,
            ca_pem: str_of(&tls_block, "ca_pem")?.map(str::to_owned),
            mode,
            tables,
            key: list_of(v, "key")?,
            chunk_size,
            server_id,
            snapshot,
            locking,
            watermark_table,
            heartbeat: Duration::from_millis(u64_of(v, "heartbeat_ms")?.unwrap_or(10_000)),
            table,
            transactional: bool_of(v, "transactional", true)?,
            checkpoint_database: str_of(v, "checkpoint_database")?
                .unwrap_or(&database)
                .to_owned(),
            apply_deletes: bool_of(v, "apply_deletes", true)?,
            database,
            instance: instance.clone(),
        })
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
        let c = MyConfig::parse(
            Target::Mysql,
            &inst(),
            &json!({"host": "db", "user": "loams", "password": "my-password", "database": "shop",
                    "mode": "cdc", "tables": ["orders", "crm.people"]}),
        )
        .expect("config");
        assert_eq!(c.port, 3306);
        assert_eq!(c.mode, Mode::Cdc);
        assert_eq!(c.tables[0].to_string(), "shop.orders");
        assert_eq!(c.tables[1].quoted(), "`crm`.`people`");
        assert_eq!(c.watermark_table.to_string(), "shop._loams_watermarks");
        assert_eq!(c.locking, Locking::Watermark);
        assert!(c.snapshot && c.transactional && c.apply_deletes);
        assert_eq!(c.tls, TlsMode::Disabled);
        assert_eq!(c.server_id, derived_server_id(&inst()));
        assert!(c.server_id >= 1 << 24);
        assert_eq!(
            c.auth,
            Auth::Basic {
                password: Some("my-password".into())
            }
        );
    }

    #[test]
    fn snapshot_locking_and_cn1_keys() {
        let c = MyConfig::parse(
            Target::Mariadb,
            &inst(),
            &json!({"host": "db", "user": "u", "database": "d", "key": "a, b",
                    "snapshot": {"mode": "initial", "locking": "global-read-lock"},
                    "tls": {"enabled": true, "verify_full": false}}),
        )
        .expect("config");
        assert_eq!(c.key, ["a", "b"]);
        assert_eq!(c.locking, Locking::GlobalReadLock);
        assert_eq!(c.tls, TlsMode::Require);
    }

    #[test]
    fn iam_requires_aws_secret_names_and_tls() {
        let base = json!({"host": "a", "user": "u", "database": "d", "auth": "iam"});
        assert!(MyConfig::parse(Target::Aurora, &inst(), &base).is_err());
        let c = MyConfig::parse(
            Target::Aurora,
            &inst(),
            &json!({"host": "a", "user": "u", "database": "d", "iam": {"region": "eu-west-1"},
                    "aws": {"access_key_id": "k", "secret_access_key": "s"}}),
        )
        .expect("iam");
        assert_eq!(c.tls, TlsMode::VerifyFull);
        assert!(matches!(c.auth, Auth::Iam(ref a) if a.region == "eu-west-1"));
        let clear = json!({"host": "a", "user": "u", "database": "d", "auth": "iam",
            "tls": {"enabled": false},
            "aws": {"region": "r", "access_key_id": "k", "secret_access_key": "s"}});
        assert!(MyConfig::parse(Target::Aurora, &inst(), &clear).is_err());
    }

    #[test]
    fn bad_values_are_refused() {
        for bad in [
            json!({"user": "u", "database": "d"}),
            json!({"host": "h", "user": "u"}),
            json!({"host": "h", "user": "u", "database": "d", "mode": "stream"}),
            json!({"host": "h", "user": "u", "database": "d", "tables": ["a.b.c"]}),
            json!({"host": "h", "user": "u", "database": "d", "port": 70000}),
            json!({"host": "h", "user": "u", "database": "d", "server_id": 0}),
            json!({"host": "h", "user": "u", "database": "d", "engine": "postgres"}),
            json!({"host": "h", "user": "u", "database": "d", "snapshot": "sometimes"}),
        ] {
            assert!(
                MyConfig::parse(Target::Mysql, &inst(), &bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn profile_defaults_merge_under_the_config() {
        let p: Profile = serde_json::from_value(json!({
            "id": "aurora-mysql", "profile_kind": "target", "module": "mysql",
            "system": {"name": "Amazon Aurora", "service": {"nightly": ["LOAMS_AURORA_MYSQL_URL"]}},
            "defaults": {"tls": {"enabled": true, "verify_full": true}}
        }))
        .expect("profile");
        let merged = merge_defaults(
            &json!({"host": "h", "tls": {"verify_full": false}}),
            Some(&p),
        );
        assert_eq!(
            merged,
            json!({"host": "h", "tls": {"enabled": true, "verify_full": false}})
        );
    }
}
