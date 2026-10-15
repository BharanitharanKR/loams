//! Conformance harnesses for the family (CN2 Task 6's kit, R6.2), against
//! the real systems (OR-2): MySQL 8.4 (compose service `mysql`, profile
//! `connectors-p2`), MariaDB 11.8 LTS (`mariadb`, `connectors-p2`) and
//! Amazon Aurora MySQL (nightly, `LOAMS_AURORA_MYSQL_URL`).
//!
//! [`MyHarness::cdc`] and [`MyHarness::batch`] drive a source into the SDK's
//! in-memory Fabric (FL1's `ingest` is not built; R0.3) and read back what
//! reached it; [`MyHarness::sink`] feeds events from that Fabric to the sink
//! and reads the target table back. The sink's target counts every
//! application of a row with a `BEFORE INSERT` trigger (an `INSERT … ON
//! DUPLICATE KEY UPDATE` fires it once per row, update or not), so a batch
//! applied twice shows as two copies: the exactly-once suite's proof.
//!
//! Connection details come from the environment, with the compose estate's
//! defaults: `LOAMS_MYSQL_HOST`/`LOAMS_MYSQL_PORT` (127.0.0.1:3307, `root`,
//! the compose file's fixed test password, TLS without verification: the
//! image's own certificate), `LOAMS_MARIADB_HOST`/`LOAMS_MARIADB_PORT`
//! (127.0.0.1:3308, `root`, no TLS), and `LOAMS_AURORA_MYSQL_URL`
//! (`mysql://user:password@host:port/database`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EventBatch,
    FabricReader, InstanceKey, Pipeline, PipelineConfig, PipelineHandle, PipelineState, Position,
    Scrubber, SinkFactory, Source, SourceBatch, SourceFactory, Split, StaticSecrets,
};
use loams_flow_conformance::{
    BulkReport, CdcReport, ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter,
    Direction, FaultPoint, FaultSwitch, HarnessError, KillTarget, MemoryReport, Observed, Seeded,
    ServiceKind,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};

use crate::cdc::CdcPos;
use crate::config::{TableName, Target};
use crate::conn::MyConn;
use crate::snapshot::watermark_ddl;
use crate::{MysqlFactory, conn};

/// The consumer group the sink harness reads.
pub const GROUP: &str = "flow-mysql-harness";
/// The database the harness works in.
pub const DATABASE: &str = "loams_conf";
const PASSWORD_SECRET: &str = "my-password";

/// MySQL 8.4, the open image (`connectors-p2`).
pub const MYSQL: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "mysql",
    system: "MySQL",
}];
/// MariaDB 11.8 LTS (`connectors-p2`).
pub const MARIADB: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "mariadb",
    system: "MariaDB",
}];
/// Aurora MySQL: a real cluster, nightly only.
pub const AURORA: &[ServiceKind] = &[ServiceKind::Nightly {
    env: &["LOAMS_AURORA_MYSQL_URL"],
    system: "Amazon Aurora",
}];

/// Where the system under test listens.
#[derive(Clone)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    /// `None`: no TLS; `Some(false)`: TLS without verification.
    pub tls_verify: Option<bool>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}/{}", self.host, self.port, self.database)
    }
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

impl Endpoint {
    /// The endpoint for `target` from the environment, or `None` when an
    /// Aurora URL is not configured.
    pub fn from_env(target: Target) -> Option<Self> {
        let local = |host: &str, port: &str, default: u16, tls: Option<bool>| Self {
            host: env(host).unwrap_or_else(|| "127.0.0.1".into()),
            port: env(port).and_then(|p| p.parse().ok()).unwrap_or(default),
            user: "root".into(),
            // deploy/fabric/compose.yaml's fixed test value (R6.7).
            password: Some("loams-test".into()),
            database: DATABASE.into(),
            tls_verify: tls,
        };
        match target {
            Target::Mysql => Some(local(
                "LOAMS_MYSQL_HOST",
                "LOAMS_MYSQL_PORT",
                3307,
                Some(false),
            )),
            Target::Mariadb => Some(local(
                "LOAMS_MARIADB_HOST",
                "LOAMS_MARIADB_PORT",
                3308,
                None,
            )),
            Target::Aurora => Self::from_url(&env("LOAMS_AURORA_MYSQL_URL")?),
        }
    }

    /// `mysql://user:password@host:port/database`.
    pub fn from_url(url: &str) -> Option<Self> {
        let rest = url.strip_prefix("mysql://")?;
        let (auth, rest) = rest.rsplit_once('@')?;
        let (user, password) = match auth.split_once(':') {
            Some((u, p)) => (u.to_owned(), Some(p.to_owned())),
            None => (auth.to_owned(), None),
        };
        let (hostport, database) = rest.split_once('/').unwrap_or((rest, DATABASE));
        let database = database.split('?').next().unwrap_or(DATABASE).to_owned();
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_owned(), p.parse().ok()?),
            None => (hostport.to_owned(), 3306),
        };
        Some(Self {
            host,
            port,
            user,
            password,
            database: if database.is_empty() {
                DATABASE.into()
            } else {
                database
            },
            tls_verify: Some(false),
        })
    }

    /// The instance config for this endpoint (the password by secret name).
    pub fn config(&self) -> Value {
        let mut c = json!({
            "host": self.host, "port": self.port, "user": self.user, "database": self.database,
        });
        if let Some(verify) = self.tls_verify {
            c["tls"] = json!({"enabled": true, "verify_full": verify});
        }
        if self.password.is_some() {
            c["password"] = json!(PASSWORD_SECRET);
        }
        c
    }

    pub fn secrets(&self) -> StaticSecrets {
        match &self.password {
            Some(p) => StaticSecrets::new().with(PASSWORD_SECRET, p.as_bytes().to_vec()),
            None => StaticSecrets::new(),
        }
    }
}

/// What the harness drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The CDC source (`mode: cdc`) into the Fabric.
    Cdc,
    /// The batch source (`mode: batch`) into the Fabric.
    Batch,
    /// The Fabric into the upsert sink (exactly-once unless not
    /// `transactional`).
    Sink { transactional: bool },
}

/// How long [`MyHarness::start`] waits for the source to open.
const OPEN_WAIT: Duration = Duration::from_secs(60);

/// Wraps the family's source factory to count the splits whose `open` has
/// returned. A fresh CDC start reads the server's binlog position in `open`
/// (`snapshot: never` starts there; R10), so what a test writes before that
/// is legitimately not captured: [`MyHarness::start`] waits for it.
struct Opened {
    inner: Arc<dyn SourceFactory>,
    opened: watch::Sender<usize>,
}

impl SourceFactory for Opened {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(OpenedSource {
            inner: self.inner.create(split)?,
            opened: self.opened.clone(),
        }))
    }
}

struct OpenedSource {
    inner: Box<dyn Source>,
    opened: watch::Sender<usize>,
}

#[async_trait]
impl Source for OpenedSource {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.inner.open(split, pos).await?;
        self.opened.send_modify(|n| *n += 1);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.inner.next(budget).await
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        self.inner.committed(upto).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }
}

struct State {
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    epoch: u64,
    running: Vec<PipelineHandle>,
    seeded: i64,
    peak: usize,
}

impl State {
    fn new() -> Self {
        Self {
            fabric: MemoryFabric::new(),
            store: MemoryCheckpointStore::new(),
            epoch: 0,
            running: Vec::new(),
            seeded: 0,
            peak: 0,
        }
    }

    fn note_peak(&mut self) {
        for h in &self.running {
            self.peak = self.peak.max(h.stats().peak_in_flight_bytes);
        }
    }
}

/// A harness for one manifest and role. See the module documentation.
pub struct MyHarness {
    target: Target,
    role: Role,
    endpoint: Option<Endpoint>,
    services: &'static [ServiceKind],
    table: String,
    instance: InstanceKey,
    config: PipelineConfig,
    extra: Value,
    switch: Arc<FaultSwitch>,
    state: Mutex<State>,
}

impl std::fmt::Debug for MyHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MyHarness")
            .field("target", &self.target)
            .field("role", &self.role)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

fn unavailable(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Unavailable(e.to_string())
}

fn failed(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Failed(e.to_string())
}

impl MyHarness {
    fn new(target: Target, role: Role, name: &str) -> Self {
        let services = match target {
            Target::Mysql => MYSQL,
            Target::Mariadb => MARIADB,
            Target::Aurora => AURORA,
        };
        let suffix = format!("{name}_{}", std::process::id());
        Self {
            target,
            role,
            endpoint: Endpoint::from_env(target),
            services,
            table: format!("t_{suffix}"),
            instance: InstanceKey::new(1, format!("conf_{suffix}")),
            config: PipelineConfig {
                max_batch_rows: 64,
                linger: Duration::from_millis(20),
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            extra: json!({}),
            switch: FaultSwitch::new(),
            state: Mutex::new(State::new()),
        }
    }

    /// The CDC source of `target`.
    pub fn cdc(target: Target, name: &str) -> Self {
        Self::new(target, Role::Cdc, name)
    }

    /// The batch source of `target`.
    pub fn batch(target: Target, name: &str) -> Self {
        Self::new(target, Role::Batch, name)
    }

    /// The sink of `target`.
    pub fn sink(target: Target, name: &str, transactional: bool) -> Self {
        Self::new(target, Role::Sink { transactional }, name)
    }

    /// Extra instance config merged over the harness's.
    pub fn with_config(mut self, extra: Value) -> Self {
        self.extra = extra;
        self
    }

    /// The harness's table, unqualified (it lives in [`DATABASE`]).
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The harness's table, `` `db`.`t` ``.
    pub fn qualified(&self) -> String {
        TableName::new(self.database(), &self.table).quoted()
    }

    pub fn database(&self) -> String {
        self.endpoint
            .as_ref()
            .map_or_else(|| DATABASE.to_owned(), |e| e.database.clone())
    }

    pub fn instance(&self) -> &InstanceKey {
        &self.instance
    }

    pub fn target(&self) -> Target {
        self.target
    }

    fn endpoint(&self) -> Result<&Endpoint, HarnessError> {
        self.endpoint
            .as_ref()
            .ok_or_else(|| unavailable(format!("{:?}: no endpoint configured", self.target)))
    }

    /// The instance's context.
    pub fn context(&self) -> Result<ConnectorContext, HarnessError> {
        let ep = self.endpoint()?;
        let mut config = ep.config();
        match self.role {
            Role::Cdc => {
                config["mode"] = json!("cdc");
                config["tables"] = json!([self.table]);
                config["heartbeat_ms"] = json!(1000);
                config["chunk_size"] = json!(64);
            }
            Role::Batch => {
                config["tables"] = json!([self.table]);
            }
            Role::Sink { transactional } => {
                config["table"] = json!(self.table);
                config["transactional"] = json!(transactional);
            }
        }
        if let (Value::Object(c), Value::Object(e)) = (&mut config, &self.extra) {
            for (k, v) in e {
                c.insert(k.clone(), v.clone());
            }
        }
        Ok(ConnectorContext::new(
            self.instance.clone(),
            config,
            Arc::new(ep.secrets()),
            Scrubber::new(),
        ))
    }

    /// An administrative connection to the system.
    pub async fn admin(&self) -> Result<MyConn, HarnessError> {
        let ctx = self.context()?;
        let cfg = MysqlFactory::new(self.target)
            .config(&ctx)
            .map_err(failed)?;
        conn::connect(&cfg, &ctx).await.map_err(|e| {
            if e.class.is_transient() {
                unavailable(e)
            } else {
                failed(e)
            }
        })
    }

    /// Run SQL (several statements allowed) on an admin connection.
    pub async fn exec(&self, sql: &str) -> Result<(), HarnessError> {
        let mut c = self.admin().await?;
        let r = c.query_all(sql).await.map(|_| ()).map_err(failed);
        c.quit().await;
        r
    }

    /// The Fabric the harness's pipelines use.
    pub async fn fabric(&self) -> MemoryFabric {
        self.state.lock().await.fabric.clone()
    }

    /// The CDC source's checkpointed position.
    pub async fn cdc_position(&self) -> Option<CdcPos> {
        let s = self.state.lock().await;
        let cp = s.store.get(&self.instance)?;
        cp.splits
            .values()
            .next()
            .and_then(|p| CdcPos::from_position(p).ok())
    }

    /// The pipelines' current status, for tests that expect a failure.
    pub async fn status(&self) -> Option<loams_connector_sdk::PipelineStatus> {
        let s = self.state.lock().await;
        s.running.first().map(PipelineHandle::status)
    }

    /// Every event and Arrow batch on the Fabric, in order.
    pub async fn delivered(&self) -> Vec<EventBatch> {
        let fabric = self.fabric().await;
        let mut r = fabric.reader("harness-readback");
        let mut out = Vec::new();
        while let Ok(Some((b, _))) = r
            .poll(Budget {
                max_rows: 1 << 20,
                max_bytes: 1 << 30,
            })
            .await
        {
            out.push(b);
        }
        out
    }

    /// The change events on the Fabric (CDC), in order.
    pub async fn changes(&self) -> Vec<CloudEvent> {
        self.delivered()
            .await
            .into_iter()
            .flat_map(|b| match b {
                EventBatch::Events(v) => v,
                EventBatch::Arrow { .. } => Vec::new(),
            })
            .filter(|e| e.ty.ends_with(".change.v1"))
            .collect()
    }

    /// Row id → copies that reached the Fabric (inserts and snapshot rows).
    async fn source_copies(&self) -> BTreeMap<String, u32> {
        let mut copies: BTreeMap<String, u32> = BTreeMap::new();
        for b in self.delivered().await {
            match b {
                EventBatch::Arrow { batch, .. } => {
                    if let Some(col) = batch.column_by_name("id") {
                        let ids = col.as_primitive::<Int64Type>();
                        for i in 0..ids.len() {
                            *copies.entry(ids.value(i).to_string()).or_default() += 1;
                        }
                    }
                }
                EventBatch::Events(evs) => {
                    for e in evs {
                        let op = e.extensions.get("loamsop").map(String::as_str);
                        if !matches!(op, Some("c" | "r")) {
                            continue;
                        }
                        let data: Value = serde_json::from_slice(&e.data).unwrap_or(Value::Null);
                        if let Some(id) = data["after"]["id"].as_i64() {
                            *copies.entry(id.to_string()).or_default() += 1;
                        }
                    }
                }
            }
        }
        copies
    }

    async fn target_copies(&self) -> Result<BTreeMap<String, u32>, HarnessError> {
        let mut admin = self.admin().await?;
        let rs = admin
            .query(&format!(
                "SELECT id, n FROM {}",
                TableName::new(self.database(), format!("{}_applied", self.table)).quoted()
            ))
            .await
            .map_err(failed)?;
        admin.quit().await;
        Ok((0..rs.rows.len())
            .filter_map(|i| {
                Some((
                    rs.get(i, "id")?,
                    rs.get(i, "n").and_then(|n| n.parse().ok())?,
                ))
            })
            .collect())
    }

    async fn sink_checkpoint(&self) -> Option<u64> {
        let mut admin = self.admin().await.ok()?;
        let rs = admin
            .query(&format!(
                "SELECT HEX(position) AS p FROM {} WHERE instance = '{}/{}'",
                TableName::new(self.database(), crate::sink::CHECKPOINTS).quoted(),
                self.instance.namespace,
                self.instance.name
            ))
            .await
            .ok()?;
        admin.quit().await;
        let hex = rs.get(0, "p")?;
        u64::from_str_radix(&hex, 16).ok()
    }

    async fn abort_all(&self, s: &mut State) {
        s.note_peak();
        for h in s.running.drain(..) {
            h.abort();
        }
        // Let aborted tasks drop their connections.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Insert rows `from..to` as `(id, 'v-<id>')`.
    async fn insert_range(&self, from: i64, to: i64) -> Result<(), HarnessError> {
        let mut admin = self.admin().await?;
        let mut i = from;
        while i < to {
            let end = (i + 1000).min(to);
            let values: Vec<String> = (i..end).map(|n| format!("({n}, 'v-{n}')")).collect();
            admin
                .exec(&format!(
                    "INSERT INTO {} (id, v) VALUES {}",
                    self.qualified(),
                    values.join(", ")
                ))
                .await
                .map_err(failed)?;
            i = end;
        }
        admin.quit().await;
        Ok(())
    }
}

#[async_trait]
impl ConnectorHarness for MyHarness {
    fn services(&self) -> &[ServiceKind] {
        self.services
    }

    /// The source roles are held to the manifest's source delivery
    /// (`at_least_once`, R11.9), the sink to the sink's.
    fn direction(&self) -> Option<Direction> {
        Some(match self.role {
            Role::Cdc | Role::Batch => Direction::Source,
            Role::Sink { .. } => Direction::Sink,
        })
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        let from = s.seeded;
        let to = from + n as i64;
        match self.role {
            Role::Cdc | Role::Batch => self.insert_range(from, to).await?,
            Role::Sink { .. } => {
                for i in from..to {
                    s.fabric.push(
                        CloudEvent::new(
                            format!("e-{i}"),
                            "/connectors/upstream/rows",
                            "io.loams.dev.test.row.v1",
                        )
                        .with_data(
                            "application/json",
                            json!({"id": i, "v": format!("v-{i}")}).to_string(),
                        ),
                    );
                }
            }
        }
        s.seeded = to;
        Ok(Seeded {
            ids: (0..to).map(|i| i.to_string()).collect(),
        })
    }

    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        let copies = match self.role {
            Role::Cdc | Role::Batch => self.source_copies().await,
            Role::Sink { .. } => self.target_copies().await?,
        };
        Ok(copies
            .into_iter()
            .map(|(id, copies)| Observed { id, copies })
            .collect())
    }

    async fn kill(&self, what: KillTarget) -> Result<(), HarnessError> {
        match what {
            KillTarget::Task | KillTarget::Pod => {
                let mut s = self.state.lock().await;
                self.abort_all(&mut s).await;
                Ok(())
            }
            KillTarget::System => Err(HarnessError::Unsupported(
                "the compose estate's containers are restarted by CI, not by the harness".into(),
            )),
        }
    }

    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError> {
        let ok = match (self.role, at) {
            (Role::Cdc | Role::Batch, FaultPoint::BeforeFabricAck)
            | (Role::Cdc | Role::Batch, FaultPoint::AfterFabricAckBeforeUpstreamCommit) => true,
            (Role::Sink { .. }, FaultPoint::MidBatch)
            | (Role::Sink { .. }, FaultPoint::AfterCommitBeforeFabricCommit) => true,
            (Role::Sink { transactional }, FaultPoint::AfterPrepare) => transactional,
            _ => false,
        };
        if !ok {
            return Err(HarnessError::Unsupported(format!(
                "{at:?} is not on this path"
            )));
        }
        self.switch.arm(at);
        Ok(())
    }

    async fn reset(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        self.abort_all(&mut s).await;
        *s = State::new();
        self.switch.disarm();
        let db = self.database();
        let t = self.qualified();
        let applied = TableName::new(&db, format!("{}_applied", self.table)).quoted();
        let wm = TableName::new(&db, "_loams_watermarks");
        self.exec(&format!(
            "CREATE DATABASE IF NOT EXISTS `{db}`; {}; \
             DROP TABLE IF EXISTS {t}; DROP TABLE IF EXISTS {applied}; \
             CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(255)) ENGINE=InnoDB",
            watermark_ddl(&wm)
        ))
        .await?;
        if matches!(self.role, Role::Sink { .. }) {
            let trigger = format!("`{db}`.`{}_count`", self.table);
            self.exec(&format!(
                "CREATE TABLE {applied} (id BIGINT NOT NULL PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB; \
                 DROP TRIGGER IF EXISTS {trigger}; \
                 CREATE TRIGGER {trigger} BEFORE INSERT ON {t} FOR EACH ROW \
                 INSERT INTO {applied} (id, n) VALUES (NEW.id, 1) ON DUPLICATE KEY UPDATE n = n + 1"
            ))
            .await?;
            let _ = self
                .exec(&format!(
                    "DELETE FROM {} WHERE instance = '{}/{}'",
                    TableName::new(&db, crate::sink::CHECKPOINTS).quoted(),
                    self.instance.namespace,
                    self.instance.name
                ))
                .await;
        }
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(failed("already running"));
        }
        s.epoch += 1;
        let (opened_tx, mut opened) = watch::channel(0usize);
        let ctx = self.context()?;
        let factory = MysqlFactory::new(self.target);
        let handle = match self.role {
            Role::Cdc | Role::Batch => {
                let (enumerator, sources) = factory
                    .source(&ctx)
                    .map_err(failed)?
                    .ok_or_else(|| failed("no source for this config"))?;
                let sources = Opened {
                    inner: Arc::from(sources),
                    opened: opened_tx.clone(),
                };
                Pipeline::source(self.instance.clone(), self.config.clone())
                    .enumerator(enumerator)
                    .sources(Arc::new(sources))
                    .writer(Arc::new(CrashingWriter::new(
                        s.fabric.clone(),
                        self.switch.clone(),
                    )))
                    .checkpoints(Arc::new(s.store.clone()))
                    .dead_letters(Arc::new(s.fabric.clone()))
                    .discover_every(Duration::from_secs(3600))
                    .epoch(s.epoch)
                    .start()
                    .await
                    .map_err(|e| failed(format!("source: {e}")))?
            }
            Role::Sink { .. } => {
                let sink: Arc<dyn SinkFactory> = Arc::from(
                    factory
                        .sink(&ctx)
                        .map_err(failed)?
                        .ok_or_else(|| failed("no sink for this config"))?,
                );
                Pipeline::sink(self.instance.clone(), self.config.clone())
                    .reader(Box::new(CrashingReader::new(
                        s.fabric.reader(GROUP),
                        self.switch.clone(),
                    )))
                    .sink(Arc::new(CrashingSinkFactory::new(
                        sink,
                        self.switch.clone(),
                    )))
                    .dead_letters(Arc::new(s.fabric.clone()))
                    .epoch(s.epoch)
                    .start()
                    .await
                    .map_err(|e| failed(format!("sink: {e}")))?
            }
        };
        if self.role == Role::Cdc {
            // Until the binlog split is open, its start position is not
            // fixed. Bounded, and over early if the pipeline stops: a
            // failed open is the caller's to see (settle, or the status).
            let _ = tokio::time::timeout(OPEN_WAIT, async {
                while *opened.borrow_and_update() == 0
                    && handle.status().state == PipelineState::Running
                {
                    let _ =
                        tokio::time::timeout(Duration::from_millis(100), opened.changed()).await;
                }
            })
            .await;
        }
        s.running = vec![handle];
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        for h in s.running.drain(..) {
            if h.status().state == PipelineState::Running {
                let _ = tokio::time::timeout(Duration::from_secs(20), h.stop()).await;
            }
        }
        Ok(())
    }

    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let (seeded, failure, fabric_len, committed) = {
                let mut s = self.state.lock().await;
                s.note_peak();
                let failure =
                    s.running.iter().map(|h| h.status()).find(|st| {
                        matches!(st.state, PipelineState::Failed | PipelineState::Fenced)
                    });
                (
                    s.seeded,
                    failure,
                    s.fabric.len() as u64,
                    s.fabric.committed(GROUP),
                )
            };
            if let Some(st) = failure {
                return Err(failed(format!(
                    "pipeline {:?}: {:?}",
                    st.state, st.last_error
                )));
            }
            let want: BTreeSet<String> = (0..seeded).map(|i| i.to_string()).collect();
            let done = match self.role {
                Role::Cdc => {
                    let have: BTreeSet<String> = self.source_copies().await.into_keys().collect();
                    let snapshot_done = self
                        .cdc_position()
                        .await
                        .is_some_and(|p| p.snapshot.is_none());
                    want.is_subset(&have) && snapshot_done
                }
                Role::Batch => {
                    let have: BTreeSet<String> = self.source_copies().await.into_keys().collect();
                    want.is_subset(&have)
                }
                Role::Sink { transactional } => {
                    let have: BTreeSet<String> = self.target_copies().await?.into_keys().collect();
                    let at = if transactional {
                        self.sink_checkpoint().await.unwrap_or(0)
                    } else {
                        committed
                    };
                    want.is_subset(&have) && at == fabric_len
                }
            };
            if done {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failed(format!("not settled after {timeout:?}")));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError> {
        Ok(self.switch.wait_tripped(timeout).await)
    }

    async fn memory(&self) -> Result<MemoryReport, HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        Ok(MemoryReport {
            peak_bytes: s.peak,
            budget_bytes: self.config.memory_budget_bytes,
        })
    }

    async fn bulk(&self, rows: u64) -> Result<BulkReport, HarnessError> {
        if self.role != Role::Batch {
            return Err(HarnessError::Unsupported("bulk is the batch path".into()));
        }
        self.reset().await?;
        self.seed(rows as usize).await?;
        self.start().await?;
        self.settle(Duration::from_secs(600)).await?;
        self.stop().await?;
        let mut arrow_rows = 0u64;
        let mut arrow = true;
        for b in self.delivered().await {
            match b {
                EventBatch::Arrow { batch, .. } => arrow_rows += batch.num_rows() as u64,
                EventBatch::Events(v) if !v.is_empty() => arrow = false,
                EventBatch::Events(_) => {}
            }
        }
        Ok(BulkReport {
            rows: arrow_rows,
            arrow,
        })
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        match method {
            "basic" => self.admin().await.map(|_| ()),
            "iam" if self.target == Target::Aurora => Err(HarnessError::Unsupported(
                "IAM auth runs in the nightly test aurora_mysql_iam_token_nightly".into(),
            )),
            other => Err(HarnessError::Unsupported(format!("auth {other}"))),
        }
    }

    async fn cdc(&self) -> Result<CdcReport, HarnessError> {
        if self.role != Role::Cdc {
            return Err(HarnessError::Unsupported("cdc needs the CDC role".into()));
        }
        let t = self.qualified();
        self.reset().await?;
        self.seed(3).await?;
        self.start().await?;
        self.settle(Duration::from_secs(60)).await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (100, 'new'); UPDATE {t} SET v = 'changed' WHERE id = 0; \
             DELETE FROM {t} WHERE id = 1; ALTER TABLE {t} ADD COLUMN note TEXT; \
             INSERT INTO {t} (id, v, note) VALUES (101, 'after-ddl', 'n')"
        ))
        .await?;
        let wait = |pred: Box<dyn Fn(&[CloudEvent]) -> bool + Send + Sync>| async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                let evs = self.changes().await;
                if pred(&evs) || tokio::time::Instant::now() >= deadline {
                    return evs;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let has = |evs: &[CloudEvent], op: &str, id: i64| {
            evs.iter().any(|e| {
                let d: Value = serde_json::from_slice(&e.data).unwrap_or(Value::Null);
                e.extensions.get("loamsop").map(String::as_str) == Some(op)
                    && (d["after"]["id"].as_i64() == Some(id)
                        || d["key"]["id"].as_i64() == Some(id))
            })
        };
        let evs = wait(Box::new(move |e| has(e, "c", 101))).await;
        let mut report = CdcReport {
            inserts: has(&evs, "c", 100),
            updates: has(&evs, "u", 0),
            deletes: has(&evs, "d", 1),
            ddl: false,
            resumed_without_loss: false,
        };
        report.ddl = self.delivered().await.iter().any(|b| match b {
            EventBatch::Events(v) => v.iter().any(|e| {
                e.ty.ends_with(".schema-change.v1")
                    && String::from_utf8_lossy(&e.data).contains("\"note\"")
            }),
            EventBatch::Arrow { .. } => false,
        });
        self.kill(KillTarget::Task).await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (200, 'while-down')"
        ))
        .await?;
        self.start().await?;
        let evs = wait(Box::new(move |e| has(e, "c", 200))).await;
        report.resumed_without_loss = has(&evs, "c", 200) && has(&evs, "c", 100);
        self.stop().await?;
        Ok(report)
    }
}

/// Resolve a factory id to its target, for the kit's registry walk.
pub fn target_of(id: &str) -> Option<Target> {
    match id {
        "mysql" => Some(Target::Mysql),
        "mariadb" => Some(Target::Mariadb),
        "aurora" => Some(Target::Aurora),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aurora_urls_parse_without_leaking_into_debug() {
        let ep = Endpoint::from_url(
            "mysql://loams:s3cr3t@db.cluster-x.eu-west-1.rds.amazonaws.com:3306/app?ssl=1",
        )
        .expect("url");
        assert_eq!(ep.host, "db.cluster-x.eu-west-1.rds.amazonaws.com");
        assert_eq!(
            (ep.port, ep.user.as_str(), ep.database.as_str()),
            (3306, "loams", "app")
        );
        assert!(!format!("{ep:?}").contains("s3cr3t"));
        assert_eq!(ep.config()["password"], json!(PASSWORD_SECRET));
        assert!(!ep.config().to_string().contains("s3cr3t"));
        assert!(Endpoint::from_url("postgres://x@y/z").is_none());
    }

    #[test]
    fn harnesses_name_their_real_systems() {
        assert_eq!(MyHarness::cdc(Target::Mysql, "t").services(), MYSQL);
        assert_eq!(
            MyHarness::sink(Target::Mariadb, "t", true).services(),
            MARIADB
        );
        assert_eq!(MyHarness::cdc(Target::Aurora, "t").services(), AURORA);
        assert_eq!(target_of("mariadb"), Some(Target::Mariadb));
        assert_eq!(target_of("postgresql"), None);
    }
}
