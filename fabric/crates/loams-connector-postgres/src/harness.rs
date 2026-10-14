//! Conformance harnesses for the family (CN2 Task 6's kit, R6.2), against
//! the real systems (OR-2): PostgreSQL 17 with pgvector (compose service
//! `pgvector`, profile `connectors-p2`), CockroachDB (`cockroachdb`,
//! `connectors-ci-only`) and Amazon Aurora (nightly,
//! `LOAMS_AURORA_POSTGRES_URL`).
//!
//! [`PgHarness::cdc`] drives the CDC source into the SDK's in-memory Fabric
//! (FL1's `ingest` is not built; R0.3) and reads back what reached it;
//! [`PgHarness::sink`] feeds events from that Fabric to the sink and reads
//! the target table back. On Postgres, the sink's target counts every
//! application of a row with a trigger, so a batch applied twice shows as
//! two copies (the exactly-once suite's proof). CockroachDB has no such
//! trigger here, so its copies are row counts.
//!
//! Connection details come from the environment, with the compose estate's
//! defaults: `LOAMS_PG_HOST`/`LOAMS_PG_PORT` (127.0.0.1:5433, user
//! `postgres`, the compose file's fixed test password), `LOAMS_CRDB_HOST`/
//! `LOAMS_CRDB_PORT` (127.0.0.1:26257, `root`, insecure), and
//! `LOAMS_AURORA_POSTGRES_URL` (`postgres://user:password@host:port/db`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorFactory, EventBatch, FabricReader, InstanceKey,
    Pipeline, PipelineConfig, PipelineHandle, PipelineState, Scrubber, SinkFactory, SplitId,
    StaticSecrets,
};
use loams_flow_conformance::{
    CdcReport, ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter, FaultPoint,
    FaultSwitch, HarnessError, KillTarget, MemoryReport, Observed, Seeded, ServiceKind,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::batch::BatchPos;
use crate::config::Target;
use crate::decode::CdcPos;
use crate::{PostgresFactory, conn};

/// The consumer group the sink harness reads.
pub const GROUP: &str = "flow-postgres-harness";
const PASSWORD_SECRET: &str = "pg-password";

/// PostgreSQL 17 + pgvector, the open image (`connectors-p2`).
pub const POSTGRES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "pgvector",
    system: "PostgreSQL",
}];
/// The pgvector manifest's service: the same image, as pgvector.
pub const PGVECTOR: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "pgvector",
    system: "pgvector",
}];
/// CockroachDB, a `ci-service` (`connectors-ci-only`).
pub const COCKROACH: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "cockroachdb",
    system: "CockroachDB",
}];
/// Aurora PostgreSQL: a real cluster, nightly only.
pub const AURORA: &[ServiceKind] = &[ServiceKind::Nightly {
    env: &["LOAMS_AURORA_POSTGRES_URL"],
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
    pub sslmode: &'static str,
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
        match target {
            Target::Postgres | Target::Pgvector => Some(Self {
                host: env("LOAMS_PG_HOST").unwrap_or_else(|| "127.0.0.1".into()),
                port: env("LOAMS_PG_PORT")
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(5433),
                user: "postgres".into(),
                // deploy/fabric/compose.yaml's fixed test value (R6.7).
                password: Some("loams-test".into()),
                database: "postgres".into(),
                sslmode: "disable",
            }),
            Target::Cockroach => Some(Self {
                host: env("LOAMS_CRDB_HOST").unwrap_or_else(|| "127.0.0.1".into()),
                port: env("LOAMS_CRDB_PORT")
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(26257),
                user: "root".into(),
                password: None,
                database: "defaultdb".into(),
                sslmode: "disable",
            }),
            Target::Aurora => Self::from_url(&env("LOAMS_AURORA_POSTGRES_URL")?),
        }
    }

    /// `postgres://user:password@host:port/db`.
    pub fn from_url(url: &str) -> Option<Self> {
        let rest = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))?;
        let (auth, rest) = rest.rsplit_once('@')?;
        let (user, password) = match auth.split_once(':') {
            Some((u, p)) => (u.to_owned(), Some(p.to_owned())),
            None => (auth.to_owned(), None),
        };
        let (hostport, database) = rest.split_once('/').unwrap_or((rest, "postgres"));
        let database = database.split('?').next().unwrap_or("postgres").to_owned();
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_owned(), p.parse().ok()?),
            None => (hostport.to_owned(), 5432),
        };
        Some(Self {
            host,
            port,
            user,
            password,
            database,
            sslmode: "require",
        })
    }

    /// The instance config for this endpoint (the password by secret name).
    pub fn config(&self) -> Value {
        let mut c = json!({
            "host": self.host, "port": self.port, "user": self.user,
            "database": self.database, "sslmode": self.sslmode,
        });
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
    /// The Fabric into the upsert sink (exactly-once unless `plain`).
    Sink { transactional: bool },
}

/// Whether a batch checkpoint holds only finished splits.
fn batch_finished(cp: Option<loams_connector_sdk::Checkpoint>) -> bool {
    cp.is_some_and(|cp| {
        !cp.splits.is_empty()
            && cp
                .splits
                .values()
                .all(|p| BatchPos::from_position(p).is_ok_and(|b| b.done))
    })
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
pub struct PgHarness {
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

impl std::fmt::Debug for PgHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgHarness")
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

impl PgHarness {
    fn new(target: Target, role: Role, name: &str) -> Self {
        let services = match target {
            Target::Postgres => POSTGRES,
            Target::Pgvector => PGVECTOR,
            Target::Cockroach => COCKROACH,
            Target::Aurora => AURORA,
        };
        let suffix = format!("{name}_{}", std::process::id());
        Self {
            target,
            role,
            endpoint: Endpoint::from_env(target),
            services,
            table: format!("loams_conf_{suffix}"),
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

    /// The CDC source of `target` (pgoutput, or a changefeed on CockroachDB).
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

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn instance(&self) -> &InstanceKey {
        &self.instance
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
                config["status_interval_ms"] = json!(1000);
                config["resolved_ms"] = json!(200);
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
    pub async fn admin(&self) -> Result<Client, HarnessError> {
        let ctx = self.context()?;
        let cfg = PostgresFactory::new(self.target)
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

    pub async fn exec(&self, sql: &str) -> Result<(), HarnessError> {
        self.admin()
            .await?
            .batch_execute(sql)
            .await
            .map_err(|e| failed(conn::classify(&e, sql)))
    }

    /// The Fabric the harness's pipelines use.
    pub async fn fabric(&self) -> MemoryFabric {
        self.state.lock().await.fabric.clone()
    }

    /// The source's checkpointed position, decoded (CDC).
    pub async fn cdc_position(&self) -> Option<CdcPos> {
        let s = self.state.lock().await;
        let cp = s.store.get(&self.instance)?;
        cp.splits
            .values()
            .next()
            .and_then(|p| CdcPos::from_position(p).ok())
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
        let admin = self.admin().await?;
        let sql = if self.target == Target::Cockroach {
            format!("SELECT id::text, 1::int8 FROM {}", self.table)
        } else {
            format!("SELECT id::text, n::int8 FROM {}_applied", self.table)
        };
        let rows = admin.query(sql.as_str(), &[]).await.map_err(failed)?;
        Ok(rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1) as u32))
            .collect())
    }

    async fn sink_checkpoint(&self) -> Option<u64> {
        let admin = self.admin().await.ok()?;
        let rows = admin
            .query(
                "SELECT position FROM public._loams_checkpoints WHERE instance = $1",
                &[&format!(
                    "{}/{}",
                    self.instance.namespace, self.instance.name
                )],
            )
            .await
            .ok()?;
        let opaque: Vec<u8> = rows.first()?.get(0);
        let b: [u8; 8] = opaque.as_slice().try_into().ok()?;
        Some(u64::from_be_bytes(b))
    }

    async fn abort_all(&self, s: &mut State) {
        s.note_peak();
        for h in s.running.drain(..) {
            h.abort();
        }
        // Let aborted tasks drop their connections (and so their slots).
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[async_trait]
impl ConnectorHarness for PgHarness {
    fn services(&self) -> &[ServiceKind] {
        self.services
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        let from = s.seeded;
        let to = from + n as i64;
        match self.role {
            Role::Cdc | Role::Batch => {
                if n > 0 {
                    self.exec(&format!(
                        "INSERT INTO {} (id, v) SELECT g, 'v-' || g::text FROM generate_series({from}, {}) g",
                        self.table,
                        to - 1
                    ))
                    .await?;
                }
            }
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
        let ctx = self.context()?;
        if self.role == Role::Cdc && self.target != Target::Cockroach {
            PostgresFactory::new(self.target)
                .delete_instance(&ctx)
                .await
                .map_err(|e| {
                    if e.class.is_transient() {
                        unavailable(e)
                    } else {
                        failed(e)
                    }
                })?;
        }
        let t = &self.table;
        if self.target == Target::Cockroach {
            self.exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
                .await?;
        }
        self.exec(&format!(
            "DROP TABLE IF EXISTS {t}; DROP TABLE IF EXISTS {t}_applied; \
             CREATE TABLE {t} (id bigint PRIMARY KEY, v text)"
        ))
        .await?;
        if matches!(self.role, Role::Sink { .. }) {
            if self.target != Target::Cockroach {
                self.exec(&format!(
                    "CREATE TABLE {t}_applied (id bigint PRIMARY KEY, n int NOT NULL); \
                     CREATE OR REPLACE FUNCTION {t}_count() RETURNS trigger LANGUAGE plpgsql AS $$ \
                     BEGIN INSERT INTO {t}_applied VALUES (NEW.id, 1) \
                     ON CONFLICT (id) DO UPDATE SET n = {t}_applied.n + 1; RETURN NEW; END $$; \
                     CREATE TRIGGER {t}_count AFTER INSERT OR UPDATE ON {t} \
                     FOR EACH ROW EXECUTE FUNCTION {t}_count()"
                ))
                .await?;
            }
            let _ = self
                .exec(&format!(
                    "DELETE FROM public._loams_checkpoints WHERE instance = '{}/{}'",
                    self.instance.namespace, self.instance.name
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
        // A batch run that finished has nothing left to resume: starting
        // again is the next run (a new as-of, the whole table read again),
        // as a scheduled batch job's next run is. The kit's bench restarts
        // each round after seeding more; resuming the finished run would
        // read nothing and never settle.
        if self.role == Role::Batch && batch_finished(s.store.get(&self.instance)) {
            s.store = MemoryCheckpointStore::new();
        }
        let ctx = self.context()?;
        let factory = PostgresFactory::new(self.target);
        let handle = match self.role {
            Role::Cdc | Role::Batch => {
                let (enumerator, sources) = factory
                    .source(&ctx)
                    .map_err(failed)?
                    .ok_or_else(|| failed("no source for this config"))?;
                Pipeline::source(self.instance.clone(), self.config.clone())
                    .enumerator(enumerator)
                    .sources(Arc::from(sources))
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
                Role::Cdc | Role::Batch => {
                    let have: BTreeSet<String> = self.source_copies().await.into_keys().collect();
                    let streaming = self.role == Role::Batch
                        || self.target == Target::Cockroach
                        || matches!(self.cdc_position().await, Some(CdcPos::Stream { .. }));
                    want.is_subset(&have) && streaming
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

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        match method {
            "basic" => self.admin().await.map(|_| ()),
            "iam" if self.target == Target::Aurora => Err(HarnessError::Unsupported(
                "IAM auth runs in the nightly test aurora_iam_token_nightly".into(),
            )),
            other => Err(HarnessError::Unsupported(format!("auth {other}"))),
        }
    }

    async fn cdc(&self) -> Result<CdcReport, HarnessError> {
        if self.role != Role::Cdc {
            return Err(HarnessError::Unsupported("cdc needs the CDC role".into()));
        }
        let t = self.table.clone();
        self.reset().await?;
        self.seed(3).await?;
        self.start().await?;
        self.settle(Duration::from_secs(60)).await?;
        // Three round trips: a multi-statement query is one implicit
        // transaction, and CockroachDB does not let a transaction write a
        // column it added (42703).
        self.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (100, 'new'); UPDATE {t} SET v = 'changed' WHERE id = 0; \
             DELETE FROM {t} WHERE id = 1"
        ))
        .await?;
        self.exec(&format!("ALTER TABLE {t} ADD COLUMN note text"))
            .await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v, note) VALUES (101, 'after-ddl', 'n')"
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
        }) || (self.target == Target::Cockroach && has(&evs, "c", 101));
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

/// The split id of a CDC harness's slot (for tests that read positions).
pub fn split_of(h: &PgHarness) -> SplitId {
    SplitId::new(crate::config::identifier(&format!(
        "loams_{}_{}",
        h.instance.namespace, h.instance.name
    )))
}

/// Resolve a factory id to its target, for the kit's registry walk.
pub fn target_of(id: &str) -> Option<Target> {
    crate::factories()
        .iter()
        .position(|f| f.id() == id)
        .map(|i| {
            [
                Target::Postgres,
                Target::Aurora,
                Target::Cockroach,
                Target::Pgvector,
            ][i]
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aurora_urls_parse_without_leaking_into_debug() {
        let ep = Endpoint::from_url("postgres://loams:s3cr3t@db.cluster-x.eu-west-1.rds.amazonaws.com:5432/app?sslmode=require")
            .expect("url");
        assert_eq!(ep.host, "db.cluster-x.eu-west-1.rds.amazonaws.com");
        assert_eq!(
            (ep.port, ep.user.as_str(), ep.database.as_str()),
            (5432, "loams", "app")
        );
        assert!(!format!("{ep:?}").contains("s3cr3t"));
        assert_eq!(ep.config()["password"], json!(PASSWORD_SECRET));
        assert!(Endpoint::from_url("mysql://x@y/z").is_none());
    }

    #[test]
    fn harnesses_name_their_real_systems() {
        assert_eq!(PgHarness::cdc(Target::Postgres, "t").services(), POSTGRES);
        assert_eq!(
            PgHarness::sink(Target::Cockroach, "t", true).services(),
            COCKROACH
        );
        assert_eq!(PgHarness::cdc(Target::Aurora, "t").services(), AURORA);
        assert_eq!(target_of("pgvector"), Some(Target::Pgvector));
    }
}
