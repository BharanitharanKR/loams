//! The conformance harness (CN2 Task 6's kit, R6.2) against real MongoDB 8
//! (OR-2): the one-member replica set `mongodb` and the standalone
//! `mongodb-standalone` of `deploy/fabric/compose.yaml`, both `ci-service`
//! images in the `connectors-ci-only` profile (MongoDB is SSPL; OR-3).
//!
//! [`MongoHarness::cdc`] and [`MongoHarness::batch`] drive a source into the
//! SDK's in-memory Fabric (FL1's `ingest` is not built; R0.3) and read back
//! what reached it. [`MongoHarness::sink`] feeds events from that Fabric to
//! the sink and reads the target collection back. MongoDB has no triggers,
//! so the exactly-once proof uses `write: insert`: every application of an
//! event inserts a new document, so a batch applied twice shows as two
//! copies of its `n` (a replayed upsert would leave no trace).
//!
//! Each harness works in its own database, `loams_conf_<name>_<pid>`, which
//! `reset` drops. Endpoints come from `LOAMS_MONGODB_HOST` /
//! `LOAMS_MONGODB_PORT` (default 127.0.0.1:27017) and
//! `LOAMS_MONGODB_STANDALONE_PORT` (27018). The estate runs without access
//! control; `auth` creates a user and checks SCRAM-SHA-256 with it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, ConnectorObserver,
    EventBatch, FabricReader, InstanceKey, Pipeline, PipelineConfig, PipelineHandle, PipelineState,
    PipelineStatus, Scrubber, Sink, SinkAck, SinkFactory, SinkHandle, SplitId, StaticSecrets,
    TransactionalSink, TxnToken,
};
use loams_flow_conformance::{
    BulkReport, CdcReport, ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter,
    Evolved, FaultPoint, FaultSwitch, HarnessError, KillTarget, MemoryReport, Observed, SchemaStep,
    Seeded, ServiceKind, Throttle,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::bson::{Bson, Document};
use crate::cdc::CdcPos;
use crate::client::Client;
use crate::config::{MongoConfig, Write};
use crate::doc;
use crate::mongodb::MongoFactory;

/// The consumer group the sink harness reads.
pub const GROUP: &str = "flow-mongodb-harness";
/// The collection every harness reads or writes.
pub const COLLECTION: &str = "items";
const PASSWORD_SECRET: &str = "mongodb-password";
const AUTH_USER: &str = "loams_conf";
const AUTH_PASSWORD: &str = "loams-conf-password";

/// MongoDB 8 as a one-member replica set (`connectors-ci-only`).
pub const MONGODB: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "mongodb",
    system: "MongoDB",
}];
/// MongoDB 8 standalone, for the narrowing to effectively once.
pub const MONGODB_STANDALONE: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "mongodb-standalone",
    system: "MongoDB",
}];

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// Where the server under test listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub standalone: bool,
}

impl Endpoint {
    pub fn replica_set() -> Self {
        Self {
            host: env("LOAMS_MONGODB_HOST").unwrap_or_else(|| "127.0.0.1".into()),
            port: env("LOAMS_MONGODB_PORT")
                .and_then(|p| p.parse().ok())
                .unwrap_or(27017),
            standalone: false,
        }
    }

    pub fn standalone() -> Self {
        Self {
            host: env("LOAMS_MONGODB_HOST").unwrap_or_else(|| "127.0.0.1".into()),
            port: env("LOAMS_MONGODB_STANDALONE_PORT")
                .and_then(|p| p.parse().ok())
                .unwrap_or(27018),
            standalone: true,
        }
    }
}

/// What the harness drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Cdc,
    Batch,
    Sink { write: Write, transactional: bool },
}

/// Records what the connector reports through `ConnectorContext::observe`.
#[derive(Debug, Default)]
pub struct Recorder {
    risks: StdMutex<Vec<f64>>,
    lags: StdMutex<Vec<Duration>>,
}

impl Recorder {
    pub fn risks(&self) -> Vec<f64> {
        self.risks.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn lags(&self) -> Vec<Duration> {
        self.lags.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

impl ConnectorObserver for Recorder {
    fn lag(&self, _i: &InstanceKey, _s: Option<&SplitId>, lag: Duration) {
        self.lags
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(lag);
    }
    fn position_risk(&self, _i: &InstanceKey, _s: Option<&SplitId>, ratio: f64) {
        self.risks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(ratio);
    }
}

struct State {
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    epoch: u64,
    running: Vec<PipelineHandle>,
    seeded: i64,
    peak: usize,
    target_delay: Option<Duration>,
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
            target_delay: None,
        }
    }

    fn note_peak(&mut self) {
        for h in &self.running {
            self.peak = self.peak.max(h.stats().peak_in_flight_bytes);
        }
    }
}

/// A harness for one role. See the module documentation.
pub struct MongoHarness {
    role: Role,
    endpoint: Endpoint,
    services: &'static [ServiceKind],
    database: String,
    instance: InstanceKey,
    config: PipelineConfig,
    extra: Value,
    switch: Arc<FaultSwitch>,
    recorder: Arc<Recorder>,
    state: Mutex<State>,
}

impl std::fmt::Debug for MongoHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoHarness")
            .field("role", &self.role)
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

fn unavailable(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Unavailable(e.to_string())
}

fn failed(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Failed(e.to_string())
}

fn harness_error(e: ConnectorError) -> HarnessError {
    if e.class.is_transient() {
        unavailable(e)
    } else {
        failed(e)
    }
}

/// A sink that waits before every write: the backpressure suite's slow
/// target.
struct SlowSink<S: ?Sized> {
    inner: Box<S>,
    delay: Duration,
}

#[async_trait]
impl Sink for SlowSink<dyn Sink> {
    async fn write(&mut self, b: EventBatch) -> Result<SinkAck, ConnectorError> {
        tokio::time::sleep(self.delay).await;
        self.inner.write(b).await
    }
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.inner.flush().await
    }
}

#[async_trait]
impl Sink for SlowSink<dyn TransactionalSink> {
    async fn write(&mut self, b: EventBatch) -> Result<SinkAck, ConnectorError> {
        tokio::time::sleep(self.delay).await;
        self.inner.write(b).await
    }
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.inner.flush().await
    }
}

#[async_trait]
impl TransactionalSink for SlowSink<dyn TransactionalSink> {
    async fn recover(&mut self) -> Result<Option<loams_connector_sdk::Checkpoint>, ConnectorError> {
        self.inner.recover().await
    }
    async fn prepare(
        &mut self,
        cp: &loams_connector_sdk::Checkpoint,
    ) -> Result<TxnToken, ConnectorError> {
        self.inner.prepare(cp).await
    }
    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        self.inner.commit(t).await
    }
    async fn abort(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        self.inner.abort(t).await
    }
}

struct SlowSinkFactory {
    inner: Arc<dyn SinkFactory>,
    delay: Duration,
}

impl SinkFactory for SlowSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(match self.inner.create()? {
            SinkHandle::Plain(s) => SinkHandle::Plain(Box::new(SlowSink {
                inner: s,
                delay: self.delay,
            })),
            SinkHandle::Transactional(t) => SinkHandle::Transactional(Box::new(SlowSink {
                inner: t,
                delay: self.delay,
            })),
        })
    }
}

impl MongoHarness {
    fn new(role: Role, name: &str) -> Self {
        let suffix = format!("{name}_{}", std::process::id());
        Self {
            role,
            endpoint: Endpoint::replica_set(),
            services: MONGODB,
            database: format!("loams_conf_{suffix}"),
            instance: InstanceKey::new(1, format!("conf_{suffix}")),
            config: PipelineConfig {
                max_batch_rows: 64,
                linger: Duration::from_millis(20),
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            extra: json!({}),
            switch: FaultSwitch::new(),
            recorder: Arc::default(),
            state: Mutex::new(State::new()),
        }
    }

    /// The CDC source (`mode: cdc`, a snapshot then the change stream).
    pub fn cdc(name: &str) -> Self {
        Self::new(Role::Cdc, name)
    }

    /// The batch source (`mode: batch`).
    pub fn batch(name: &str) -> Self {
        Self::new(Role::Batch, name)
    }

    /// The sink.
    pub fn sink(name: &str, write: Write, transactional: bool) -> Self {
        Self::new(
            Role::Sink {
                write,
                transactional,
            },
            name,
        )
    }

    /// Against the standalone server instead of the replica set.
    pub fn standalone(mut self) -> Self {
        self.endpoint = Endpoint::standalone();
        self.services = MONGODB_STANDALONE;
        self
    }

    /// Extra instance config merged over the harness's.
    pub fn with_config(mut self, extra: Value) -> Self {
        self.extra = extra;
        self
    }

    /// Pipeline settings (batch rows, linger) for this harness.
    pub fn with_pipeline(mut self, config: PipelineConfig) -> Self {
        self.config = config;
        self
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn instance(&self) -> &InstanceKey {
        &self.instance
    }

    pub fn recorder(&self) -> Arc<Recorder> {
        self.recorder.clone()
    }

    /// The instance's context.
    pub fn context(&self) -> ConnectorContext {
        let ep = &self.endpoint;
        let mut config = json!({
            "hosts": [format!("{}:{}", ep.host, ep.port)],
            "database": self.database,
            "connect_timeout_ms": 5000,
        });
        match self.role {
            Role::Cdc => {
                config["mode"] = json!("cdc");
                config["collections"] = json!([COLLECTION]);
                config["heartbeat_ms"] = json!(500);
                config["window_ms"] = json!(1000);
                config["await_ms"] = json!(200);
            }
            Role::Batch => {
                config["collections"] = json!([COLLECTION]);
            }
            Role::Sink {
                write,
                transactional,
            } => {
                config["collection"] = json!(COLLECTION);
                config["write"] = json!(match write {
                    Write::Upsert => "upsert",
                    Write::Insert => "insert",
                });
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
            Arc::new(StaticSecrets::new().with(PASSWORD_SECRET, AUTH_PASSWORD.as_bytes().to_vec())),
            Scrubber::new(),
        );
        ctx.observe = self.recorder.clone();
        ctx
    }

    /// An administrative client of the server under test.
    pub fn admin(&self) -> Result<Client, HarnessError> {
        let ctx = self.context();
        let cfg = MongoFactory.config(&ctx).map_err(failed)?;
        Ok(Client::new(cfg, ctx))
    }

    /// Run `cmd` in `db`, retrying transient failures for up to 20 s (an
    /// election right after the replica set starts, a reconnect).
    async fn admin_command(
        &self,
        db: &str,
        cmd: Document,
        seqs: &[crate::wire::Sequence],
    ) -> Result<Document, ConnectorError> {
        let mut c = self
            .admin()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut tries = 0;
        loop {
            match c.command_with(db, cmd.clone(), seqs).await {
                Err(e) if e.class.is_transient() && tries < 40 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                r => return r,
            }
        }
    }

    /// Run a command in the harness's database.
    pub async fn command(&self, cmd: Document) -> Result<Document, HarnessError> {
        self.admin_command(&self.database, cmd, &[])
            .await
            .map_err(harness_error)
    }

    /// Insert `docs` into the harness's collection. Idempotent by `_id`: a
    /// retried chunk's duplicates are not errors.
    pub async fn insert(&self, docs: Vec<Document>) -> Result<(), HarnessError> {
        for chunk in docs.chunks(10_000) {
            let seq = crate::wire::Sequence {
                identifier: "documents".into(),
                documents: chunk.to_vec(),
            };
            let r = self
                .admin_command(
                    &self.database,
                    doc! { "insert" => COLLECTION, "ordered" => false },
                    std::slice::from_ref(&seq),
                )
                .await;
            match r {
                Ok(_) => {}
                Err(e)
                    if crate::wire::server_error(&e)
                        .is_some_and(|s| s.code == crate::wire::codes::DUPLICATE_KEY) => {}
                Err(e) => return Err(harness_error(e)),
            }
        }
        Ok(())
    }

    /// Every document of the harness's collection.
    pub async fn documents(&self) -> Result<Vec<Document>, HarnessError> {
        // One cursor on one connection: a transient failure retries the
        // whole read.
        let mut tries = 0;
        loop {
            match self.read_all().await {
                Err(e) if e.class.is_transient() && tries < 40 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                r => return r.map_err(harness_error),
            }
        }
    }

    async fn read_all(&self) -> Result<Vec<Document>, ConnectorError> {
        let mut c = self
            .admin()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let mut out = Vec::new();
        let mut reply = c
            .command(
                &self.database,
                doc! { "find" => COLLECTION, "filter" => doc! {}, "batchSize" => 10_000 },
            )
            .await?;
        loop {
            let cursor = reply
                .get_document("cursor")
                .ok_or_else(|| ConnectorError::fatal("find returned no cursor"))?
                .clone();
            for d in cursor
                .get_array("firstBatch")
                .or_else(|| cursor.get_array("nextBatch"))
                .unwrap_or_default()
            {
                if let Bson::Document(d) = d {
                    out.push(d.clone());
                }
            }
            let id = cursor.get_i64("id").unwrap_or(0);
            if id == 0 {
                return Ok(out);
            }
            reply = c
                .command(
                    &self.database,
                    doc! { "getMore" => id, "collection" => COLLECTION, "batchSize" => 10_000 },
                )
                .await?;
        }
    }

    /// The Fabric the harness's pipelines use.
    pub async fn fabric(&self) -> MemoryFabric {
        self.state.lock().await.fabric.clone()
    }

    /// The running pipeline's status.
    pub async fn status(&self) -> Option<PipelineStatus> {
        self.state
            .lock()
            .await
            .running
            .first()
            .map(PipelineHandle::status)
    }

    /// Resume a paused pipeline.
    pub async fn resume(&self) {
        if let Some(h) = self.state.lock().await.running.first() {
            h.resume();
        }
    }

    /// The source's checkpointed CDC position.
    pub async fn cdc_position(&self) -> Option<CdcPos> {
        let s = self.state.lock().await;
        let cp = s.store.get(&self.instance)?;
        cp.splits
            .values()
            .next()
            .and_then(|p| CdcPos::from_position(p).ok())
    }

    /// Every batch on the Fabric, in order.
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

    /// The JSON events on the Fabric whose type ends with `.<event>.v1`.
    pub async fn events(&self, event: &str) -> Vec<CloudEvent> {
        let suffix = format!(".{event}.v1");
        self.delivered()
            .await
            .into_iter()
            .flat_map(|b| match b {
                EventBatch::Events(v) => v,
                EventBatch::Arrow { .. } => Vec::new(),
            })
            .filter(|e| e.ty.ends_with(&suffix))
            .collect()
    }

    /// Row id → copies that reached the Fabric (documents read and inserts).
    async fn source_copies(&self) -> BTreeMap<String, u32> {
        let mut copies: BTreeMap<String, u32> = BTreeMap::new();
        for b in self.delivered().await {
            match b {
                EventBatch::Arrow { batch, .. } => {
                    if let Some(col) = batch.column_by_name("_id") {
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
                        if let Some(id) = data["after"]["_id"].as_i64() {
                            *copies.entry(id.to_string()).or_default() += 1;
                        }
                    }
                }
            }
        }
        copies
    }

    async fn target_copies(&self) -> Result<BTreeMap<String, u32>, HarnessError> {
        let field = match self.role {
            Role::Sink {
                write: Write::Insert,
                ..
            } => "n",
            _ => "_id",
        };
        let mut copies: BTreeMap<String, u32> = BTreeMap::new();
        for d in self.documents().await? {
            if let Some(n) = d.get(field).and_then(Bson::as_i64) {
                *copies.entry(n.to_string()).or_default() += 1;
            }
        }
        Ok(copies)
    }

    /// The Fabric offset in the sink's committed checkpoint.
    async fn sink_checkpoint(&self) -> Option<u64> {
        let reply = self
            .command(doc! {
                "find" => crate::config::CHECKPOINTS,
                "filter" => doc! { "instance" => MongoConfig::instance_id(&self.instance) },
            })
            .await
            .ok()?;
        let first = reply
            .get_document("cursor")?
            .get_array("firstBatch")?
            .first()?
            .as_document()?
            .clone();
        match first.get("position") {
            Some(Bson::Binary { bytes, .. }) => {
                let b: [u8; 8] = bytes.as_slice().try_into().ok()?;
                Some(u64::from_be_bytes(b))
            }
            _ => None,
        }
    }

    async fn abort_all(&self, s: &mut State) {
        s.note_peak();
        for h in s.running.drain(..) {
            h.abort();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Wait until `pred` holds over the change events, or `timeout`.
    pub async fn wait_changes(
        &self,
        timeout: Duration,
        pred: impl Fn(&[CloudEvent]) -> bool,
    ) -> Vec<CloudEvent> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let evs = self.events("change").await;
            if pred(&evs) || tokio::time::Instant::now() >= deadline {
                return evs;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait until the pipeline pauses, or `timeout`; the pause's error.
    pub async fn wait_paused(&self, timeout: Duration) -> Option<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(st) = self.status().await
                && st.state == PipelineState::Paused
            {
                return Some(st.last_error.map(|e| e.message).unwrap_or_default());
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Whether a change event of `op` for `_id = id` is in `evs`.
    pub fn has(evs: &[CloudEvent], op: &str, id: i64) -> bool {
        evs.iter().any(|e| {
            let d: Value = serde_json::from_slice(&e.data).unwrap_or(Value::Null);
            e.extensions.get("loamsop").map(String::as_str) == Some(op)
                && (d["after"]["_id"].as_i64() == Some(id) || d["key"]["_id"].as_i64() == Some(id))
        })
    }

    async fn running(&self) -> bool {
        !self.state.lock().await.running.is_empty()
    }
}

fn row(i: i64) -> Document {
    doc! { "_id" => i, "v" => format!("v-{i}") }
}

#[async_trait]
impl ConnectorHarness for MongoHarness {
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
                    self.insert((from..to).map(row).collect()).await?;
                }
            }
            Role::Sink { write, .. } => {
                for i in from..to {
                    let data = match write {
                        Write::Upsert => json!({"_id": i, "v": format!("v-{i}")}),
                        Write::Insert => json!({"n": i, "v": format!("v-{i}")}),
                    };
                    s.fabric.push(
                        CloudEvent::new(
                            format!("e-{i}"),
                            "/connectors/upstream/rows",
                            "io.loams.dev.test.row.v1",
                        )
                        .with_data("application/json", data.to_string()),
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
            (
                Role::Cdc | Role::Batch,
                FaultPoint::BeforeFabricAck | FaultPoint::AfterFabricAckBeforeUpstreamCommit,
            ) => true,
            (
                Role::Sink { .. },
                FaultPoint::MidBatch | FaultPoint::AfterCommitBeforeFabricCommit,
            ) => true,
            (Role::Sink { transactional, .. }, FaultPoint::AfterPrepare) => transactional,
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
        let delay = s.target_delay;
        *s = State::new();
        s.target_delay = delay;
        self.switch.disarm();
        drop(s);
        self.command(doc! { "dropDatabase" => 1 }).await?;
        self.command(doc! { "create" => COLLECTION }).await?;
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(failed("already running"));
        }
        s.epoch += 1;
        let ctx = self.context();
        let factory = MongoFactory;
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
                let mut sink: Arc<dyn SinkFactory> = Arc::from(
                    factory
                        .sink(&ctx)
                        .map_err(failed)?
                        .ok_or_else(|| failed("no sink for this config"))?,
                );
                if let Some(delay) = s.target_delay {
                    sink = Arc::new(SlowSinkFactory { inner: sink, delay });
                }
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
            if matches!(
                h.status().state,
                PipelineState::Running | PipelineState::Paused
            ) {
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
                Role::Sink { transactional, .. } => {
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

    async fn throttle(&self, t: Throttle) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        match t {
            Throttle::Fabric(d) => s.fabric.set_append_delay(Some(d)),
            // A source's target is the Fabric (throttled above); a sink's
            // target is slowed by a delay before every write.
            Throttle::Target(d) => s.target_delay = Some(d),
            Throttle::Off => {
                s.fabric.set_append_delay(None);
                s.target_delay = None;
            }
        }
        Ok(())
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
            return Err(HarnessError::Unsupported(
                "bulk needs the batch role".into(),
            ));
        }
        self.reset().await?;
        self.seed(rows as usize).await?;
        self.start().await?;
        let settled = self.settle(Duration::from_secs(600)).await;
        self.stop().await?;
        settled?;
        let mut n = 0u64;
        let mut arrow = true;
        for b in self.delivered().await {
            match b {
                EventBatch::Arrow { batch, .. } => n += batch.num_rows() as u64,
                EventBatch::Events(evs) => {
                    arrow &= evs.iter().all(|e| !e.ty.ends_with(".document.v1"));
                }
            }
        }
        Ok(BulkReport { rows: n, arrow })
    }

    async fn cdc(&self) -> Result<CdcReport, HarnessError> {
        if self.role != Role::Cdc {
            return Err(HarnessError::Unsupported("cdc needs the CDC role".into()));
        }
        self.reset().await?;
        self.seed(3).await?;
        self.start().await?;
        self.settle(Duration::from_secs(60)).await?;
        self.insert(vec![doc! { "_id" => 100i64, "v" => "new" }])
            .await?;
        self.command(
            doc! { "update" => COLLECTION, "updates" => vec![Bson::from(doc! {
                "q" => doc! { "_id" => 0i64 }, "u" => doc! { "$set" => doc! { "v" => "changed" } },
            })] },
        )
        .await?;
        self.command(
            doc! { "delete" => COLLECTION, "deletes" => vec![Bson::from(doc! {
                "q" => doc! { "_id" => 1i64 }, "limit" => 1,
            })] },
        )
        .await?;
        // The document model's DDL: a new field, and a collection change.
        self.insert(vec![
            doc! { "_id" => 101i64, "v" => "after-ddl", "note" => "n" },
        ])
        .await?;
        let evs = self
            .wait_changes(Duration::from_secs(60), |e| Self::has(e, "c", 101))
            .await;
        let mut report = CdcReport {
            inserts: Self::has(&evs, "c", 100),
            updates: Self::has(&evs, "u", 0),
            deletes: Self::has(&evs, "d", 1),
            ddl: false,
            resumed_without_loss: false,
        };
        report.ddl = self
            .events("schema-change")
            .await
            .iter()
            .any(|e| String::from_utf8_lossy(&e.data).contains("\"note\""));
        self.kill(KillTarget::Task).await?;
        self.insert(vec![doc! { "_id" => 200i64, "v" => "while-down" }])
            .await?;
        self.start().await?;
        let evs = self
            .wait_changes(Duration::from_secs(60), |e| Self::has(e, "c", 200))
            .await;
        report.resumed_without_loss = Self::has(&evs, "c", 200) && Self::has(&evs, "c", 100);
        self.stop().await?;
        Ok(report)
    }

    async fn evolve(&self, step: SchemaStep) -> Result<Evolved, HarnessError> {
        if self.role != Role::Cdc {
            return Err(HarnessError::Unsupported(
                "schema evolution runs on the CDC role".into(),
            ));
        }
        if !self.running().await {
            self.seed(3).await?;
            self.start().await?;
            self.settle(Duration::from_secs(60)).await?;
        }
        let id = match step {
            SchemaStep::Additive => {
                self.insert(vec![doc! { "_id" => 1000i64, "v" => "x", "extra" => 1 }])
                    .await?;
                1000
            }
            SchemaStep::Incompatible => {
                // `v` has been a string: a number is a type no column holds.
                self.insert(vec![doc! { "_id" => 1001i64, "v" => 5i64 }])
                    .await?;
                1001
            }
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(error) = self.wait_paused(Duration::from_millis(100)).await {
                return Ok(Evolved::Paused { error });
            }
            let evs = self.events("change").await;
            if Self::has(&evs, "c", id) {
                return Ok(Evolved::Evolved);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failed(format!(
                    "document {id} neither arrived nor paused the instance"
                )));
            }
        }
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        match method {
            "basic" => {
                let mut admin = self.admin()?;
                let roles = vec![Bson::from(
                    doc! { "role" => "readWrite", "db" => self.database.clone() },
                )];
                let created = admin
                    .command(
                        "admin",
                        doc! { "createUser" => AUTH_USER, "pwd" => AUTH_PASSWORD, "roles" => roles.clone(),
                               "mechanisms" => vec![Bson::from("SCRAM-SHA-256")] },
                    )
                    .await;
                if created.is_err() {
                    admin
                        .command(
                            "admin",
                            doc! { "updateUser" => AUTH_USER, "pwd" => AUTH_PASSWORD, "roles" => roles,
                                   "mechanisms" => vec![Bson::from("SCRAM-SHA-256")] },
                        )
                        .await
                        .map_err(harness_error)?;
                }
                let mut cfg = MongoFactory.config(&self.context()).map_err(failed)?;
                cfg.username = Some(AUTH_USER.into());
                cfg.password = Some(PASSWORD_SECRET.into());
                cfg.auth_mechanism = crate::config::AuthMechanism::ScramSha256;
                let mut user = Client::new(cfg.clone(), self.context());
                user.command(
                    &self.database,
                    doc! { "insert" => "auth_check", "documents" => vec![Bson::from(doc! { "ok" => true })] },
                )
                .await
                .map_err(|e| failed(format!("authenticated write: {}", e.message)))?;
                // The same user with the wrong password is refused.
                let mut ctx = self.context();
                ctx.secrets = Arc::new(
                    StaticSecrets::new().with(PASSWORD_SECRET, b"not-the-password".to_vec()),
                );
                let mut wrong = Client::new(cfg, ctx);
                match wrong.command(&self.database, doc! { "ping" => 1 }).await {
                    Err(e) if e.class == loams_connector_sdk::ErrorClass::Auth => Ok(()),
                    Err(e) => Err(failed(format!(
                        "a wrong password failed as {}: {}",
                        e.class, e.message
                    ))),
                    Ok(_) => Err(failed("a wrong password was accepted")),
                }
            }
            "mtls" => Err(HarnessError::Unsupported(
                "the estate's MongoDB runs without TLS; MONGODB-X509 runs against a TLS deployment"
                    .into(),
            )),
            other => Err(HarnessError::Unsupported(format!("auth {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harnesses_name_their_real_systems() {
        assert_eq!(MongoHarness::cdc("t").services(), MONGODB);
        assert_eq!(
            MongoHarness::sink("t", Write::Upsert, true)
                .standalone()
                .services(),
            MONGODB_STANDALONE
        );
        let h = MongoHarness::sink("t", Write::Insert, false);
        let cfg = MongoFactory.config(&h.context()).expect("config");
        let sink = cfg.sink.expect("sink");
        assert_eq!((sink.write, sink.transactional), (Write::Insert, false));
        assert!(h.database().starts_with("loams_conf_t_"));
    }
}
