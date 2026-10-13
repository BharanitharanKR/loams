//! ADBC targets: DuckDB (the `duckdb` target profile) and any driver the
//! namespace's admin allows (R17.7).
//!
//! A driver is native code loaded into the Loams process, so it is loaded
//! **by path from an allowlist, with a SHA-256 check** (CN1 Ruling 9): the
//! instance config names a driver (`driver: duckdb`), and only the runtime's
//! [`DriverAllowlist`] says where its library is and what it must hash to.
//! No driver is compiled into Loams; the DuckDB driver is DuckDB's own
//! shared library (`libduckdb`, entry point `duckdb_adbc_init`).
//!
//! ADBC calls block, so they run on a blocking thread. The source streams
//! the result reader through a bounded channel (one page per Arrow batch the
//! driver returns); the sink binds each batch for a bulk ingest.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use adbc_core::options::{AdbcVersion, IngestMode, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, ConnectorFactory, EventBatch, SecretResolver, Sink, SinkAck,
    SinkFactory, SinkHandle, SourceFactory, SplitEnumerator,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{OnceCell, mpsc};

use crate::common::{secret, text};
use crate::query::{Fetch, Page, PagedQuery, QueryEnumerator, QuerySource, QuerySourceFactory};
use crate::rows::to_record_batch;

/// The manifest id of the ★ ADBC connector (`connectors/registry/adbc.yaml`).
pub const CONNECTOR: &str = "adbc";
/// The DuckDB target's manifest id.
pub const DUCKDB: &str = "duckdb";

/// An allowed driver: its library, its SHA-256 and its entry point.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedDriver {
    pub path: PathBuf,
    /// Lower-case hex SHA-256 of the library file.
    pub sha256: String,
    /// The init symbol (`AdbcDriverInit` when absent; DuckDB's is
    /// `duckdb_adbc_init`).
    #[serde(default)]
    pub entrypoint: Option<String>,
}

/// The drivers a runtime allows, by name. The namespace's admin sets it;
/// an instance only names one.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DriverAllowlist {
    #[serde(default)]
    pub drivers: BTreeMap<String, AllowedDriver>,
}

impl DriverAllowlist {
    /// The allowlist in the JSON file `LOAMS_ADBC_ALLOWLIST` names, or an
    /// empty one (every driver refused).
    pub fn from_env() -> Result<Self, ConnectorError> {
        match std::env::var_os("LOAMS_ADBC_ALLOWLIST") {
            None => Ok(Self::default()),
            Some(p) => {
                let body = std::fs::read(&p).map_err(|e| {
                    ConnectorError::fatal(format!(
                        "ADBC allowlist {}: {e}",
                        PathBuf::from(&p).display()
                    ))
                })?;
                serde_json::from_slice(&body)
                    .map_err(|e| ConnectorError::fatal(format!("ADBC allowlist: {e}")))
            }
        }
    }

    pub fn with(mut self, name: impl Into<String>, d: AllowedDriver) -> Self {
        self.drivers.insert(name.into(), d);
        self
    }

    /// Load `name`'s library after checking it is allowed and hashes to
    /// its recorded SHA-256.
    pub fn load(&self, name: &str) -> Result<ManagedDriver, ConnectorError> {
        let d = self.drivers.get(name).ok_or_else(|| {
            ConnectorError::fatal(format!(
                "ADBC driver {name:?} is not in the runtime's allowlist (CN1 Ruling 9)"
            ))
        })?;
        let body = std::fs::read(&d.path).map_err(|e| {
            ConnectorError::fatal(format!("ADBC driver {name:?} at {}: {e}", d.path.display()))
        })?;
        let digest: String = Sha256::digest(&body)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if !digest.eq_ignore_ascii_case(d.sha256.trim()) {
            return Err(ConnectorError::fatal(format!(
                "ADBC driver {name:?} at {} does not match its allowlisted SHA-256",
                d.path.display()
            )));
        }
        ManagedDriver::load_dynamic_from_filename(
            &d.path,
            d.entrypoint.as_deref().map(str::as_bytes),
            AdbcVersion::V110,
        )
        .map_err(|e| ConnectorError::fatal(format!("ADBC driver {name:?}: {e}")))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ingest {
    #[default]
    Append,
    /// Create the table from the first batch when it does not exist.
    CreateAppend,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdbcSinkConfig {
    pub table: String,
    #[serde(default)]
    pub mode: Ingest,
}

/// The instance config of an ADBC instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdbcConfig {
    /// An allowlisted driver's name.
    pub driver: String,
    /// Database options, passed to the driver as strings (`path` for
    /// DuckDB's database file; `uri` for most others). Never a secret.
    #[serde(default)]
    pub database: BTreeMap<String, String>,
    /// A password's secret **name**, passed as ADBC's `password` option.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub sink: Option<AdbcSinkConfig>,
    #[serde(default)]
    pub pipeline: Option<serde_json::Value>,
}

impl AdbcConfig {
    /// The instance config, under the profile's defaults (a target profile
    /// such as `duckdb` sets `driver`).
    pub fn parse(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let mut config = ctx
            .profile
            .as_ref()
            .map(|p| p.defaults.clone())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| serde_json::json!({}));
        if let (Some(base), Some(over)) = (config.as_object_mut(), ctx.config.as_object()) {
            for (k, v) in over {
                base.insert(k.clone(), v.clone());
            }
        }
        let cfg: Self = serde_json::from_value(config)
            .map_err(|e| ConnectorError::fatal(format!("adbc config: {e}")))?;
        if let Some(s) = &cfg.sink {
            crate::common::table_name(&s.table)?;
        }
        Ok(cfg)
    }
}

/// An ADBC connection, opened on first use and shared by the instance's
/// tasks. Opening resolves the password secret (if the config names one)
/// and loads the driver on a blocking thread.
#[derive(Clone)]
pub struct AdbcTarget {
    allow: Arc<DriverAllowlist>,
    cfg: AdbcConfig,
    secrets: Arc<dyn SecretResolver>,
    conn: Arc<OnceCell<Arc<Mutex<ManagedConnection>>>>,
}

impl std::fmt::Debug for AdbcTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcTarget")
            .field("driver", &self.cfg.driver)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn adbc_err(e: adbc_core::error::Error, what: &str) -> ConnectorError {
    use adbc_core::error::Status;
    let msg = format!("{what}: {}", e.message);
    match e.status {
        Status::Unauthenticated | Status::Unauthorized => ConnectorError::auth(msg),
        Status::InvalidData | Status::Integrity => ConnectorError::poison(msg),
        Status::IO | Status::Timeout | Status::Cancelled => ConnectorError::retryable(msg),
        _ => ConnectorError::fatal(msg),
    }
}

/// Load the driver, open the database and connect (blocking).
fn open(
    allow: &DriverAllowlist,
    cfg: &AdbcConfig,
    password: Option<String>,
) -> Result<ManagedConnection, ConnectorError> {
    let mut driver = allow.load(&cfg.driver)?;
    let mut opts: Vec<(OptionDatabase, OptionValue)> = cfg
        .database
        .iter()
        .map(|(k, v)| {
            let key = match k.as_str() {
                "uri" => OptionDatabase::Uri,
                "username" => OptionDatabase::Username,
                other => OptionDatabase::Other(other.to_owned()),
            };
            (key, OptionValue::String(v.clone()))
        })
        .collect();
    if let Some(p) = password {
        opts.push((OptionDatabase::Password, OptionValue::String(p)));
    }
    let db = driver
        .new_database_with_opts(opts)
        .map_err(|e| adbc_err(e, "ADBC database"))?;
    db.new_connection()
        .map_err(|e| adbc_err(e, "ADBC connection"))
}

impl AdbcTarget {
    /// A target that connects on first use. The driver must be in `allow`.
    pub fn new(
        allow: Arc<DriverAllowlist>,
        cfg: AdbcConfig,
        secrets: Arc<dyn SecretResolver>,
    ) -> Result<Self, ConnectorError> {
        if !allow.drivers.contains_key(&cfg.driver) {
            return Err(ConnectorError::fatal(format!(
                "ADBC driver {:?} is not in the runtime's allowlist (CN1 Ruling 9)",
                cfg.driver
            )));
        }
        Ok(Self {
            allow,
            cfg,
            secrets,
            conn: Arc::new(OnceCell::new()),
        })
    }

    async fn conn(&self) -> Result<Arc<Mutex<ManagedConnection>>, ConnectorError> {
        self.conn
            .get_or_try_init(|| async {
                let password = match secret(&*self.secrets, self.cfg.password.as_ref()).await? {
                    Some(s) => Some(text(&s, "the ADBC password")?),
                    None => None,
                };
                let (allow, cfg) = (self.allow.clone(), self.cfg.clone());
                let c = tokio::task::spawn_blocking(move || open(&allow, &cfg, password))
                    .await
                    .map_err(|e| ConnectorError::fatal(format!("ADBC task: {e}")))??;
                Ok(Arc::new(Mutex::new(c)))
            })
            .await
            .cloned()
    }

    /// Run `sql` and stream its batches into a channel of `depth`.
    async fn stream(
        &self,
        sql: String,
        depth: usize,
    ) -> Result<mpsc::Receiver<Result<RecordBatch, ConnectorError>>, ConnectorError> {
        let conn = self.conn().await?;
        let (tx, rx) = mpsc::channel(depth.max(1));
        tokio::task::spawn_blocking(move || {
            let run =
                || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, ConnectorError> {
                    let mut c = lock(&conn);
                    let mut st = c
                        .new_statement()
                        .map_err(|e| adbc_err(e, "ADBC statement"))?;
                    st.set_sql_query(&sql)
                        .map_err(|e| adbc_err(e, "ADBC query"))?;
                    st.execute().map_err(|e| adbc_err(e, "ADBC execute"))
                };
            match run() {
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                }
                Ok(reader) => {
                    for b in reader {
                        let item =
                            b.map_err(|e| ConnectorError::retryable(format!("ADBC read: {e}")));
                        let stop = item.is_err();
                        if tx.blocking_send(item).is_err() || stop {
                            break;
                        }
                    }
                }
            }
        });
        Ok(rx)
    }

    /// Bulk-ingest one batch into `table`.
    pub async fn ingest(
        &self,
        table: &str,
        mode: Ingest,
        batch: RecordBatch,
    ) -> Result<(), ConnectorError> {
        let conn = self.conn().await?;
        let table = table.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut c = lock(&conn);
            let mut st = c
                .new_statement()
                .map_err(|e| adbc_err(e, "ADBC statement"))?;
            st.set_option(OptionStatement::TargetTable, OptionValue::String(table))
                .map_err(|e| adbc_err(e, "ADBC target table"))?;
            let m = match mode {
                Ingest::Append => IngestMode::Append,
                Ingest::CreateAppend => IngestMode::CreateAppend,
            };
            st.set_option(OptionStatement::IngestMode, m.into())
                .map_err(|e| adbc_err(e, "ADBC ingest mode"))?;
            st.bind(batch).map_err(|e| adbc_err(e, "ADBC bind"))?;
            st.execute_update()
                .map_err(|e| adbc_err(e, "ADBC ingest"))?;
            Ok(())
        })
        .await
        .map_err(|e| ConnectorError::fatal(format!("ADBC task: {e}")))?
    }
}

/// A query on an ADBC connection, one page per batch the driver returns.
/// A page cannot be fetched twice (the reader moves on), so a restart
/// re-runs the query and skips what was taken (R17.9).
pub struct AdbcQuery {
    target: AdbcTarget,
    sql: String,
    rx: Option<mpsc::Receiver<Result<RecordBatch, ConnectorError>>>,
    /// The page number the stream will give next.
    next: u64,
}

impl std::fmt::Debug for AdbcQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcQuery")
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

impl AdbcQuery {
    pub fn new(target: AdbcTarget, sql: impl Into<String>) -> Self {
        Self {
            target,
            sql: sql.into(),
            rx: None,
            next: 0,
        }
    }

    async fn pull(&mut self) -> Result<Fetch, ConnectorError> {
        let Some(rx) = &mut self.rx else {
            return Ok(Fetch::Gone);
        };
        match rx.recv().await {
            None => {
                self.rx = None;
                Ok(Fetch::End)
            }
            Some(Err(e)) => {
                self.rx = None;
                Err(e)
            }
            Some(Ok(rows)) => {
                let k = self.next;
                self.next += 1;
                Ok(Fetch::Page(Page {
                    at: serde_json::json!(k),
                    rows,
                    next: Some(serde_json::json!(k + 1)),
                }))
            }
        }
    }
}

#[async_trait]
impl PagedQuery for AdbcQuery {
    async fn start(&mut self) -> Result<Fetch, ConnectorError> {
        self.rx = Some(self.target.stream(self.sql.clone(), 2).await?);
        self.next = 0;
        self.pull().await
    }

    async fn fetch(&mut self, cursor: &serde_json::Value) -> Result<Fetch, ConnectorError> {
        if self.rx.is_some() && cursor.as_u64() == Some(self.next) {
            self.pull().await
        } else {
            Ok(Fetch::Gone)
        }
    }

    fn query(&self) -> &str {
        &self.sql
    }
}

/// The ADBC sink: one bulk ingest per batch (at-least-once).
#[derive(Debug)]
pub struct AdbcSink {
    target: AdbcTarget,
    sink: AdbcSinkConfig,
}

impl AdbcSink {
    pub fn new(target: AdbcTarget, sink: AdbcSinkConfig) -> Self {
        Self { target, sink }
    }
}

#[async_trait]
impl Sink for AdbcSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let rb = to_record_batch(&batch, None)?;
        if rb.num_rows() == 0 {
            return Ok(SinkAck::default());
        }
        let ack = SinkAck {
            rows: rb.num_rows() as u64,
            bytes: rb.get_array_memory_size() as u64,
        };
        self.target
            .ingest(&self.sink.table, self.sink.mode, rb)
            .await?;
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Makes [`AdbcSink`]s over one connection.
#[derive(Debug)]
pub struct AdbcSinkFactory {
    target: AdbcTarget,
    sink: AdbcSinkConfig,
}

impl AdbcSinkFactory {
    pub fn new(target: AdbcTarget, sink: AdbcSinkConfig) -> Self {
        Self { target, sink }
    }
}

impl SinkFactory for AdbcSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(AdbcSink::new(
            self.target.clone(),
            self.sink.clone(),
        ))))
    }
}

/// The ADBC module (Task 2's ref `loams_connector_analytics::adbc`), for
/// the `duckdb` target and the ★ ADBC manifest. The id is the manifest's:
/// `adbc`, or `duckdb` for the target.
#[derive(Debug, Clone)]
pub struct Adbc {
    id: &'static str,
    allow: Arc<DriverAllowlist>,
}

impl Adbc {
    pub fn new(id: &'static str, allow: Arc<DriverAllowlist>) -> Self {
        Self { id, allow }
    }

    /// The DuckDB target with the runtime's allowlist.
    pub fn duckdb() -> Self {
        Self::new(
            DUCKDB,
            Arc::new(DriverAllowlist::from_env().unwrap_or_default()),
        )
    }

    fn target(
        &self,
        ctx: &ConnectorContext,
        cfg: &AdbcConfig,
    ) -> Result<AdbcTarget, ConnectorError> {
        AdbcTarget::new(self.allow.clone(), cfg.clone(), ctx.secrets.clone())
    }
}

impl ConnectorFactory for Adbc {
    fn id(&self) -> &'static str {
        self.id
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = AdbcConfig::parse(ctx)?;
        let Some(sql) = cfg.query.clone() else {
            return Ok(None);
        };
        let target = self.target(ctx, &cfg)?;
        let id = self.id;
        let instance = ctx.instance.clone();
        Ok(Some((
            Box::new(QueryEnumerator),
            Box::new(QuerySourceFactory::new(move || {
                QuerySource::new(
                    Box::new(AdbcQuery::new(target.clone(), sql.clone())),
                    id,
                    &instance,
                    "query",
                )
            })),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = AdbcConfig::parse(ctx)?;
        let Some(sink) = cfg.sink.clone() else {
            return Ok(None);
        };
        let target = self.target(ctx, &cfg)?;
        Ok(Some(Box::new(AdbcSinkFactory::new(target, sink))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drivers_outside_the_allowlist_or_with_another_hash_are_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lib = dir.path().join("libfake.so");
        std::fs::write(&lib, b"not a library").expect("write");
        let allow = DriverAllowlist::default().with(
            "fake",
            AllowedDriver {
                path: lib,
                sha256: "00".repeat(32),
                entrypoint: None,
            },
        );
        let err = allow.load("other").expect_err("not allowlisted");
        assert!(err.message.contains("allowlist"), "{}", err.message);
        let err = allow.load("fake").expect_err("hash mismatch");
        assert!(err.message.contains("SHA-256"), "{}", err.message);
    }
}
