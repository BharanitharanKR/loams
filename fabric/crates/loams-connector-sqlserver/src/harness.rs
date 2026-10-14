//! Conformance harnesses for the family (CN2 Task 6's kit, R6.2), against
//! the real systems (OR-2): SQL Server 2022 Developer (compose service
//! `sqlserver`, profile `connectors-ci-only`, a `ci-service`, OR-3) and Azure
//! SQL Database (nightly, `LOAMS_AZURE_SQL_URL`).
//!
//! [`SqlHarness::cdc`] and [`SqlHarness::batch`] drive a source into the
//! SDK's in-memory Fabric (FL1's `ingest` is not built; R0.3) and read back
//! what reached it; [`SqlHarness::sink`] feeds events from that Fabric to the
//! MERGE sink and reads the target back. The sink's target counts every
//! application of a row with a trigger, so a batch applied twice shows as two
//! copies (the exactly-once suite's proof).
//!
//! Connection details come from the environment, with the compose estate's
//! defaults: `LOAMS_MSSQL_HOST`/`LOAMS_MSSQL_PORT` (127.0.0.1:1433, login
//! `sa`, the compose file's fixed test password, database `loams`, created
//! with snapshot isolation and CDC on), `LOAMS_MSSQL_ENCRYPT` (`disable` by
//! default: the container's certificate is self-signed; `strict` with
//! `trust_server_certificate` is `sqlserver_strict_tls_login`'s), and
//! `LOAMS_AZURE_SQL_URL` (see [`Endpoint::from_url`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorFactory, ConnectorObserver, EventBatch,
    FabricReader, InstanceKey, Pipeline, PipelineConfig, PipelineHandle, PipelineState, Scrubber,
    SinkFactory, StaticSecrets,
};
use loams_flow_conformance::{
    CdcReport, ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter, Direction,
    FaultPoint, FaultSwitch, HarnessError, KillTarget, MemoryReport, Observed, Seeded, ServiceKind,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::cdc::CdcPos;
use crate::config::Target;
use crate::conn::{self, SqlClient};
use crate::{SqlServerFactory, types::quote_literal};

/// The consumer group the sink harness reads.
pub const GROUP: &str = "flow-sqlserver-harness";
const PASSWORD_SECRET: &str = "mssql-password";
const CLIENT_SECRET: &str = "entra-client-secret";
/// The database the CI harness creates on the container.
pub const DATABASE: &str = "loams";

/// SQL Server 2022 Developer, a `ci-service` (`connectors-ci-only`).
pub const SQL_SERVER: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "sqlserver",
    system: "SQL Server",
}];
/// Azure SQL Database: a real database, nightly only.
pub const AZURE_SQL: &[ServiceKind] = &[ServiceKind::Nightly {
    env: &["LOAMS_AZURE_SQL_URL"],
    system: "Azure SQL Database",
}];

/// How the harness logs in.
#[derive(Clone, PartialEq, Eq)]
pub enum Login {
    Sql {
        user: String,
        password: String,
    },
    Entra {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Login::Sql { user, .. } => write!(f, "Sql({user})"),
            Login::Entra { client_id, .. } => write!(f, "Entra({client_id})"),
        }
    }
}

/// Where the system under test listens.
#[derive(Clone)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub login: Login,
    pub encrypt: String,
    pub trust_server_certificate: bool,
    /// The harness may create the database (the CI container).
    pub owns_database: bool,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}/{}", self.host, self.port, self.database)
    }
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(x) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(x);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl Endpoint {
    /// The endpoint for `target` from the environment, or `None` when the
    /// Azure SQL URL is not configured.
    pub fn from_env(target: Target) -> Option<Self> {
        match target {
            Target::SqlServer => {
                let encrypt = env("LOAMS_MSSQL_ENCRYPT").unwrap_or_else(|| "disable".into());
                Some(Self {
                    host: env("LOAMS_MSSQL_HOST").unwrap_or_else(|| "127.0.0.1".into()),
                    port: env("LOAMS_MSSQL_PORT")
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(1433),
                    database: DATABASE.into(),
                    login: Login::Sql {
                        user: "sa".into(),
                        // deploy/fabric/compose.yaml's fixed test value (R6.7).
                        password: "Loams-Test-Pass-1".into(),
                    },
                    trust_server_certificate: encrypt == "strict",
                    encrypt,
                    owns_database: true,
                })
            }
            Target::AzureSql => Self::from_url(&env("LOAMS_AZURE_SQL_URL")?),
        }
    }

    /// `sqlserver://<client_id>:<client_secret>@<host>[:port]/<db>?tenant=<id>`
    /// (an Entra service principal), or without `tenant` a SQL login
    /// `sqlserver://<user>:<password>@…`. Parts are percent-decoded.
    pub fn from_url(url: &str) -> Option<Self> {
        let rest = url
            .strip_prefix("sqlserver://")
            .or_else(|| url.strip_prefix("mssql://"))?;
        let (auth, rest) = rest.rsplit_once('@')?;
        let (user, password) = auth.split_once(':')?;
        let (hostport, path) = rest.split_once('/')?;
        let (database, query) = path.split_once('?').unwrap_or((path, ""));
        let tenant = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("tenant="))
            .map(pct_decode);
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_owned(), p.parse().ok()?),
            None => (hostport.to_owned(), 1433),
        };
        let (user, password) = (pct_decode(user), pct_decode(password));
        Some(Self {
            host,
            port,
            database: pct_decode(database),
            login: match tenant {
                Some(tenant_id) => Login::Entra {
                    tenant_id,
                    client_id: user,
                    client_secret: password,
                },
                None => Login::Sql { user, password },
            },
            encrypt: "strict".into(),
            trust_server_certificate: false,
            owns_database: false,
        })
    }

    /// The instance config for this endpoint (credentials by secret name).
    pub fn config(&self, database: &str) -> Value {
        let mut c = json!({
            "host": self.host, "port": self.port, "database": database,
            "encrypt": self.encrypt, "trust_server_certificate": self.trust_server_certificate,
        });
        match &self.login {
            Login::Sql { user, .. } => {
                c["auth"] = json!("sql");
                c["user"] = json!(user);
                c["password"] = json!(PASSWORD_SECRET);
            }
            Login::Entra {
                tenant_id,
                client_id,
                ..
            } => {
                c["auth"] = json!("entra");
                c["entra"] = json!({"tenant_id": tenant_id, "client_id": client_id,
                                    "client_secret": CLIENT_SECRET});
            }
        }
        c
    }

    pub fn secrets(&self) -> StaticSecrets {
        match &self.login {
            Login::Sql { password, .. } => {
                StaticSecrets::new().with(PASSWORD_SECRET, password.as_bytes().to_vec())
            }
            Login::Entra { client_secret, .. } => {
                StaticSecrets::new().with(CLIENT_SECRET, client_secret.as_bytes().to_vec())
            }
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
    /// The Fabric into the MERGE sink (exactly-once unless `plain`).
    Sink { transactional: bool },
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
pub struct SqlHarness {
    target: Target,
    role: Role,
    endpoint: Option<Endpoint>,
    services: &'static [ServiceKind],
    table: String,
    instance: InstanceKey,
    config: PipelineConfig,
    extra: Value,
    observer: Option<Arc<dyn ConnectorObserver>>,
    switch: Arc<FaultSwitch>,
    state: Mutex<State>,
}

impl std::fmt::Debug for SqlHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlHarness")
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

fn by_class(e: loams_connector_sdk::ConnectorError) -> HarnessError {
    if e.class.is_transient() {
        unavailable(e)
    } else {
        failed(e)
    }
}

static PREPARED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

impl SqlHarness {
    fn new(target: Target, role: Role, name: &str) -> Self {
        let services = match target {
            Target::SqlServer => SQL_SERVER,
            Target::AzureSql => AZURE_SQL,
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
            observer: None,
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

    /// The pipeline's settings (batch sizes for a resume test).
    pub fn with_pipeline(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    /// The observer the instance's context carries (metrics tests).
    pub fn with_observer(mut self, o: Arc<dyn ConnectorObserver>) -> Self {
        self.observer = Some(o);
        self
    }

    /// Override the endpoint (a test of TLS or of an auth method).
    pub fn with_endpoint(mut self, e: Endpoint) -> Self {
        self.endpoint = Some(e);
        self
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn instance(&self) -> &InstanceKey {
        &self.instance
    }

    pub fn endpoint(&self) -> Result<&Endpoint, HarnessError> {
        self.endpoint
            .as_ref()
            .ok_or_else(|| unavailable(format!("{:?}: no endpoint configured", self.target)))
    }

    /// The instance's context.
    pub fn context(&self) -> Result<ConnectorContext, HarnessError> {
        let ep = self.endpoint()?;
        let mut config = ep.config(&ep.database);
        match self.role {
            Role::Cdc => {
                config["mode"] = json!("cdc");
                config["tables"] = json!([self.table]);
                config["poll_interval_ms"] = json!(200);
                config["metrics_interval_ms"] = json!(500);
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
        let mut ctx = ConnectorContext::new(
            self.instance.clone(),
            config,
            Arc::new(ep.secrets()),
            Scrubber::new(),
        );
        if let Some(o) = &self.observer {
            ctx.observe = o.clone();
        }
        Ok(ctx)
    }

    async fn connect_to(&self, database: &str) -> Result<SqlClient, HarnessError> {
        let ep = self.endpoint()?;
        let mut ctx = self.context()?;
        ctx.config = ep.config(database);
        let cfg = SqlServerFactory::new(self.target)
            .config(&ctx)
            .map_err(failed)?;
        conn::connect(&cfg, &ctx).await.map_err(by_class)
    }

    /// Create the CI database once per process: snapshot isolation and CDC
    /// on, and the capture job polling every second.
    async fn prepare(&self) -> Result<(), HarnessError> {
        if !self.endpoint()?.owns_database {
            return Ok(());
        }
        PREPARED
            .get_or_try_init(|| async {
                let mut master = self.connect_to("master").await?;
                let db = crate::types::quote_ident(DATABASE);
                conn::exec(
                    &mut master,
                    &format!(
                        "IF DB_ID({}) IS NULL CREATE DATABASE {db}; \
                         ALTER DATABASE {db} SET ALLOW_SNAPSHOT_ISOLATION ON",
                        quote_literal(DATABASE)
                    ),
                    "creating the test database",
                )
                .await
                .map_err(failed)?;
                let mut c = self.admin().await?;
                conn::exec(
                    &mut c,
                    "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 \
                     EXEC sys.sp_cdc_enable_db",
                    "enabling CDC on the database",
                )
                .await
                .map_err(failed)?;
                // Best effort: the capture job exists once a table is enabled.
                let _ = conn::exec(
                    &mut c,
                    "EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 1",
                    "capture job",
                )
                .await;
                Ok::<(), HarnessError>(())
            })
            .await?;
        Ok(())
    }

    /// An administrative connection to the database under test.
    pub async fn admin(&self) -> Result<SqlClient, HarnessError> {
        let db = self.endpoint()?.database.clone();
        self.connect_to(&db).await
    }

    pub async fn exec(&self, sql: &str) -> Result<(), HarnessError> {
        let mut c = self.admin().await?;
        conn::exec(&mut c, sql, "harness").await.map_err(failed)
    }

    /// Enable CDC on the harness's table under `capture_instance`.
    pub async fn enable_cdc(&self, capture_instance: &str) -> Result<(), HarnessError> {
        self.exec(&format!(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = {}, \
             @role_name = NULL, @supports_net_changes = 0, @capture_instance = {}",
            quote_literal(&self.table),
            quote_literal(capture_instance)
        ))
        .await?;
        // The capture job's interval applies from its next start.
        let _ = self
            .exec("EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 1")
            .await;
        Ok(())
    }

    /// The default capture instance's name.
    pub fn capture_instance(&self) -> String {
        format!("dbo_{}", self.table)
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

    /// The change and schema-change events on the Fabric, in order.
    pub async fn events(&self) -> Vec<CloudEvent> {
        self.delivered()
            .await
            .into_iter()
            .flat_map(|b| match b {
                EventBatch::Events(v) => v,
                EventBatch::Arrow { .. } => Vec::new(),
            })
            .collect()
    }

    /// The change events on the Fabric (CDC), in order.
    pub async fn changes(&self) -> Vec<CloudEvent> {
        self.events()
            .await
            .into_iter()
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
        let mut c = self.admin().await?;
        let rows = conn::rows(
            &mut c,
            &format!(
                "SELECT CAST(id AS nvarchar(40)), CAST(n AS bigint) FROM {}",
                crate::types::quote_ident(&format!("{}_applied", self.table))
            ),
            &[],
            "reading the target",
        )
        .await
        .map_err(failed)?;
        Ok(rows
            .iter()
            .filter_map(|r| Some((conn::text_at(r, 0)?, r.get::<i64, _>(1).unwrap_or(0) as u32)))
            .collect())
    }

    async fn sink_checkpoint(&self) -> Option<u64> {
        let mut c = self.admin().await.ok()?;
        let key = format!("{}/{}", self.instance.namespace, self.instance.name);
        let rows = conn::rows(
            &mut c,
            "SELECT position FROM [dbo].[_loams_checkpoints] WHERE instance = @P1",
            &[&key.as_str()],
            "reading the checkpoint",
        )
        .await
        .ok()?;
        let opaque: &[u8] = rows.first()?.get(0)?;
        let b: [u8; 8] = opaque.try_into().ok()?;
        Some(u64::from_be_bytes(b))
    }

    async fn abort_all(&self, s: &mut State) {
        s.note_peak();
        for h in s.running.drain(..) {
            h.abort();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    fn has(evs: &[CloudEvent], op: &str, id: i64) -> bool {
        evs.iter().any(|e| {
            let d: Value = serde_json::from_slice(&e.data).unwrap_or(Value::Null);
            e.extensions.get("loamsop").map(String::as_str) == Some(op)
                && (d["after"]["id"].as_i64() == Some(id) || d["key"]["id"].as_i64() == Some(id))
        })
    }

    /// Wait until `pred` holds over the change events, or `timeout`.
    pub async fn wait_for(
        &self,
        timeout: Duration,
        pred: impl Fn(&[CloudEvent]) -> bool,
    ) -> Vec<CloudEvent> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let evs = self.changes().await;
            if pred(&evs) || tokio::time::Instant::now() >= deadline {
                return evs;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Whether `evs` holds a change `op` for row `id`.
pub fn has_change(evs: &[CloudEvent], op: &str, id: i64) -> bool {
    SqlHarness::has(evs, op, id)
}

#[async_trait]
impl ConnectorHarness for SqlHarness {
    fn services(&self) -> &[ServiceKind] {
        self.services
    }

    /// The source roles are held to the manifest's source delivery
    /// (`at_least_once`, R12.9), the sink to the sink's.
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
            Role::Cdc | Role::Batch => {
                if n > 0 {
                    self.exec(&format!(
                        "WITH g AS (SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 + {from} AS i \
                         FROM sys.all_objects a CROSS JOIN sys.all_objects b) \
                         INSERT INTO [dbo].{} (id, v) SELECT i, CONCAT(N'v-', i) FROM g",
                        crate::types::quote_ident(&self.table)
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
        drop(s);
        self.prepare().await?;
        let t = crate::types::quote_ident(&self.table);
        let applied = crate::types::quote_ident(&format!("{}_applied", self.table));
        let lit = quote_literal(&self.table);
        // Disable CDC on the table first: a captured table cannot be dropped.
        let _ = self
            .exec(&format!(
                "IF EXISTS (SELECT 1 FROM sys.tables WHERE name = {lit} AND is_tracked_by_cdc = 1) \
                 EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = {lit}, \
                 @capture_instance = N'all'"
            ))
            .await;
        self.exec(&format!(
            "DROP TABLE IF EXISTS [dbo].{t}; DROP TABLE IF EXISTS [dbo].{applied}; \
             CREATE TABLE [dbo].{t} (id bigint NOT NULL PRIMARY KEY, v nvarchar(100) NULL)"
        ))
        .await?;
        match self.role {
            Role::Cdc => self.enable_cdc(&self.capture_instance()).await?,
            Role::Batch => {}
            Role::Sink { .. } => {
                self.exec(&format!(
                    "CREATE TABLE [dbo].{applied} (id bigint NOT NULL PRIMARY KEY, n int NOT NULL)"
                ))
                .await?;
                let trigger = crate::types::quote_ident(&format!("{}_count", self.table));
                self.exec(&format!(
                    "CREATE TRIGGER [dbo].{trigger} ON [dbo].{t} AFTER INSERT, UPDATE AS BEGIN \
                     SET NOCOUNT ON; MERGE [dbo].{applied} AS a \
                     USING (SELECT id, COUNT(*) AS c FROM inserted GROUP BY id) AS i ON a.id = i.id \
                     WHEN MATCHED THEN UPDATE SET n = a.n + i.c \
                     WHEN NOT MATCHED THEN INSERT (id, n) VALUES (i.id, i.c); END"
                ))
                .await?;
                let _ = self
                    .exec(&format!(
                        "IF OBJECT_ID(N'[dbo].[_loams_checkpoints]', N'U') IS NOT NULL \
                         DELETE FROM [dbo].[_loams_checkpoints] WHERE instance = N'{}/{}'",
                        self.instance.namespace, self.instance.name
                    ))
                    .await;
            }
        }
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(failed("already running"));
        }
        s.epoch += 1;
        let ctx = self.context()?;
        let factory = SqlServerFactory::new(self.target);
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
            tokio::time::sleep(Duration::from_millis(200)).await;
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
        match (method, &self.endpoint()?.login) {
            ("basic", Login::Sql { .. }) | ("aad", Login::Entra { .. }) => {
                self.admin().await.map(|_| ())
            }
            (other, _) => Err(HarnessError::Unsupported(format!(
                "auth {other} with this endpoint's login"
            ))),
        }
    }

    async fn cdc(&self) -> Result<CdcReport, HarnessError> {
        if self.role != Role::Cdc {
            return Err(HarnessError::Unsupported("cdc needs the CDC role".into()));
        }
        let t = format!("[dbo].{}", crate::types::quote_ident(&self.table));
        self.reset().await?;
        self.seed(3).await?;
        self.start().await?;
        self.settle(Duration::from_secs(120)).await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (100, N'new'); UPDATE {t} SET v = N'changed' WHERE id = 0; \
             DELETE FROM {t} WHERE id = 1"
        ))
        .await?;
        let evs = self
            .wait_for(Duration::from_secs(120), |e| Self::has(e, "d", 1))
            .await;
        let mut report = CdcReport {
            inserts: Self::has(&evs, "c", 100),
            updates: Self::has(&evs, "u", 0),
            deletes: Self::has(&evs, "d", 1),
            ddl: false,
            resumed_without_loss: false,
        };
        // DDL: a new column, then a second capture instance that has it.
        self.exec(&format!("ALTER TABLE {t} ADD note nvarchar(20) NULL"))
            .await?;
        self.enable_cdc(&format!("{}_v2", self.capture_instance()))
            .await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v, note) VALUES (101, N'after-ddl', N'n')"
        ))
        .await?;
        self.wait_for(Duration::from_secs(120), |e| Self::has(e, "c", 101))
            .await;
        report.ddl = self.events().await.iter().any(|e| {
            e.ty.ends_with(".schema-change.v1")
                && String::from_utf8_lossy(&e.data).contains("\"note\"")
        });
        self.kill(KillTarget::Task).await?;
        self.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (200, N'while-down')"
        ))
        .await?;
        self.start().await?;
        let evs = self
            .wait_for(Duration::from_secs(120), |e| Self::has(e, "c", 200))
            .await;
        report.resumed_without_loss = Self::has(&evs, "c", 200) && Self::has(&evs, "c", 100);
        self.stop().await?;
        Ok(report)
    }
}

/// Resolve a factory id to its target, for the kit's registry walk.
pub fn target_of(id: &str) -> Option<Target> {
    match id {
        "sql-server" => Some(Target::SqlServer),
        "azure-sql" => Some(Target::AzureSql),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn azure_urls_parse_without_leaking_into_debug() {
        let ep = Endpoint::from_url(
            "sqlserver://app-id:s3cr%2Ft@loams.database.windows.net:1433/loams-nightly?tenant=t-1",
        )
        .expect("url");
        assert_eq!(ep.host, "loams.database.windows.net");
        assert_eq!((ep.port, ep.database.as_str()), (1433, "loams-nightly"));
        assert_eq!(
            ep.login,
            Login::Entra {
                tenant_id: "t-1".into(),
                client_id: "app-id".into(),
                client_secret: "s3cr/t".into()
            }
        );
        assert!(!format!("{ep:?}").contains("s3cr"));
        let c = ep.config("loams-nightly");
        assert_eq!(c["entra"]["client_secret"], json!(CLIENT_SECRET));
        assert_eq!(c["encrypt"], json!("strict"));
        let sql = Endpoint::from_url("sqlserver://loams:pw@h/db").expect("sql login");
        assert!(matches!(sql.login, Login::Sql { .. }));
        assert_eq!(sql.port, 1433);
        assert!(Endpoint::from_url("postgres://x@y/z").is_none());
    }

    #[test]
    fn harnesses_name_their_real_systems() {
        assert_eq!(
            SqlHarness::cdc(Target::SqlServer, "t").services(),
            SQL_SERVER
        );
        assert_eq!(
            SqlHarness::sink(Target::AzureSql, "t", true).services(),
            AZURE_SQL
        );
        assert_eq!(target_of("azure-sql"), Some(Target::AzureSql));
        let h = SqlHarness::cdc(Target::SqlServer, "x");
        assert!(h.capture_instance().starts_with("dbo_loams_conf_x_"));
    }
}
