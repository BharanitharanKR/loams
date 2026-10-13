//! Conformance harnesses (CN2 Task 6's kit): each manifest of the family
//! runs its suites through a [`ChainHarness`] over a [`KvSystem`], against
//! the real system `conformance/connectors/systems.toml` names for it
//! (OR-2): Valkey and Redis containers, Cassandra, and DynamoDB (with
//! Streams) on floci in PR CI or on AWS in the nightly job.
//!
//! The chain is the connector's own source → the in-memory Fabric (with
//! `ingest`'s `(source, id)` dedup) → the connector's own sink, writing to a
//! second key prefix, stream or table of the same system; a source-only
//! connector's target is the Fabric itself. The kit's crash wrappers sit on
//! the Fabric writer, the Fabric reader and the sink, so every fault point
//! but `AfterPrepare` (no sink here is transactional) has a place to trip.
//!
//! A harness whose service is not up reports `Unavailable`, so its suites
//! are skipped and never count as green (OR-23).

pub mod cassandra;
pub mod dynamodb;
pub mod redis;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EventBatch, InstanceKey,
    Pipeline, PipelineConfig, PipelineHandle, PipelineState, Profile, Scrubber, Sink, SinkAck,
    SinkFactory, SinkHandle, StaticSecrets,
};
use loams_flow::{ConnectorSpec, ManifestError};
use loams_flow_conformance::{
    ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter, FaultPoint, FaultSwitch,
    HarnessError, KillTarget, MemoryReport, Observed, Seeded, ServiceKind, Throttle,
};
use serde_json::Value;
use tokio::sync::Mutex;

/// The consumer group the harness's sink reads the Fabric with.
pub const GROUP: &str = "flow-kv-harness";

/// One system under test: how to reach it, fill its input, read its target,
/// and configure the connector against it.
#[async_trait]
pub trait KvSystem: Send + Sync + 'static {
    /// The manifest id.
    fn id(&self) -> &'static str;
    fn services(&self) -> &'static [ServiceKind];
    fn factory(&self) -> Arc<dyn ConnectorFactory>;
    fn profile(&self) -> Option<Profile> {
        None
    }
    /// The secrets the configs name.
    fn secrets(&self) -> StaticSecrets {
        StaticSecrets::new()
    }
    fn source_config(&self) -> Value;
    /// `None` for a source-only connector: the Fabric is the target.
    fn sink_config(&self) -> Option<Value>;
    /// Reach the system, or `Unavailable`.
    async fn ready(&self) -> Result<(), HarnessError>;
    /// Empty the input and the target.
    async fn clear(&self) -> Result<(), HarnessError>;
    /// Write records `from..to` into the input; return the ids the target
    /// will show for them.
    async fn write_input(&self, from: usize, to: usize) -> Result<Vec<String>, HarnessError>;
    /// The target's records (unused for a source-only connector).
    async fn read_target(&self) -> Result<Vec<Observed>, HarnessError>;
    /// A Fabric event's id as the target shows it (a source-only
    /// connector's observed ids; the default is the event id).
    fn fabric_id(&self, ev: &CloudEvent) -> Option<String> {
        Some(ev.id.clone())
    }
    /// Connect with the auth method of this slug and move one record.
    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!("auth {method}")))
    }
    /// The cdc suite's script, for a CDC source.
    async fn cdc(
        &self,
        h: &ChainHarness<Self>,
    ) -> Result<loams_flow_conformance::CdcReport, HarnessError>
    where
        Self: Sized,
    {
        let _ = h;
        Err(HarnessError::Unsupported("cdc".into()))
    }
}

/// A sink that waits before each write (the backpressure suite's slow
/// target).
#[derive(Debug, Clone, Default)]
pub struct Delay(Arc<StdMutex<Option<Duration>>>);

impl Delay {
    fn set(&self, d: Option<Duration>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = d;
    }
    fn get(&self) -> Option<Duration> {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

struct DelaySinkFactory {
    inner: Box<dyn SinkFactory>,
    delay: Delay,
}

impl std::fmt::Debug for DelaySinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DelaySinkFactory")
    }
}

impl SinkFactory for DelaySinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        match self.inner.create()? {
            SinkHandle::Plain(s) => Ok(SinkHandle::Plain(Box::new(DelaySink {
                inner: s,
                delay: self.delay.clone(),
            }))),
            t @ SinkHandle::Transactional(_) => Ok(t),
        }
    }
}

struct DelaySink {
    inner: Box<dyn Sink>,
    delay: Delay,
}

#[async_trait]
impl Sink for DelaySink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        if let Some(d) = self.delay.get() {
            tokio::time::sleep(d).await;
        }
        self.inner.write(batch).await
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.inner.flush().await
    }
}

struct State {
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    seeded: usize,
    ids: Vec<String>,
    epoch: u64,
    running: Vec<PipelineHandle>,
    peak: usize,
}

impl State {
    fn new() -> Self {
        Self {
            fabric: MemoryFabric::new(),
            store: MemoryCheckpointStore::new(),
            seeded: 0,
            ids: Vec::new(),
            epoch: 0,
            running: Vec::new(),
            peak: 0,
        }
    }

    fn note_peak(&mut self) {
        for h in &self.running {
            self.peak = self.peak.max(h.stats().peak_in_flight_bytes);
        }
    }
}

/// The connector's source → the in-memory Fabric → the connector's sink,
/// against the real system `S` (see the module documentation).
pub struct ChainHarness<S: KvSystem> {
    pub system: S,
    instance: InstanceKey,
    config: PipelineConfig,
    switch: Arc<FaultSwitch>,
    delay: Delay,
    state: Mutex<State>,
}

impl<S: KvSystem> std::fmt::Debug for ChainHarness<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainHarness")
            .field("connector", &self.system.id())
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

impl<S: KvSystem> ChainHarness<S> {
    pub fn new(system: S) -> Self {
        let instance = InstanceKey::new(1, format!("h-{}-{}", system.id(), run_tag()));
        Self {
            system,
            instance,
            config: PipelineConfig {
                max_batch_rows: 16,
                max_batch_bytes: 256 << 10,
                linger: Duration::from_millis(5),
                max_in_flight_batches: 4,
                memory_budget_bytes: 2 << 20,
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            switch: FaultSwitch::new(),
            delay: Delay::default(),
            state: Mutex::new(State::new()),
        }
    }

    pub fn instance(&self) -> &InstanceKey {
        &self.instance
    }

    /// The Fabric the chain runs through (a source-only connector's target).
    pub async fn fabric(&self) -> MemoryFabric {
        self.state.lock().await.fabric.clone()
    }

    /// A context for `config`, with this harness's secrets and profile.
    pub fn context(&self, config: Value) -> ConnectorContext {
        let mut ctx = ConnectorContext::new(
            self.instance.clone(),
            config,
            Arc::new(self.system.secrets()),
            Scrubber::new(),
        );
        ctx.profile = self.system.profile();
        ctx
    }

    fn observed_from_fabric(&self, fabric: &MemoryFabric) -> Vec<Observed> {
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        for ev in fabric.events() {
            if let Some(id) = self.system.fabric_id(&ev) {
                *counts.entry(id).or_default() += 1;
            }
        }
        counts
            .into_iter()
            .map(|(id, copies)| Observed { id, copies })
            .collect()
    }

    async fn observed(&self) -> Result<Vec<Observed>, HarnessError> {
        if self.system.sink_config().is_some() {
            self.system.read_target().await
        } else {
            let fabric = self.state.lock().await.fabric.clone();
            Ok(self.observed_from_fabric(&fabric))
        }
    }
}

fn failed(what: &str, e: impl std::fmt::Display) -> HarnessError {
    HarnessError::failed(format!("{what}: {e}"))
}

#[async_trait]
impl<S: KvSystem> ConnectorHarness for ChainHarness<S> {
    fn services(&self) -> &[ServiceKind] {
        self.system.services()
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        let from = s.seeded;
        let ids = self.system.write_input(from, from + n).await?;
        s.seeded += n;
        s.ids.extend(ids);
        Ok(Seeded { ids: s.ids.clone() })
    }

    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        self.observed().await
    }

    async fn kill(&self, what: KillTarget) -> Result<(), HarnessError> {
        match what {
            KillTarget::Task | KillTarget::Pod => {
                let mut s = self.state.lock().await;
                s.note_peak();
                for h in s.running.drain(..) {
                    h.abort();
                }
                Ok(())
            }
            KillTarget::System => Err(HarnessError::Unsupported(
                "the harness does not restart the compose service".into(),
            )),
        }
    }

    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError> {
        let has_sink = self.system.sink_config().is_some();
        match at {
            FaultPoint::AfterPrepare => {
                return Err(HarnessError::Unsupported(
                    "no sink of this family is transactional".into(),
                ));
            }
            FaultPoint::MidBatch | FaultPoint::AfterCommitBeforeFabricCommit if !has_sink => {
                return Err(HarnessError::Unsupported(format!(
                    "{} has no sink",
                    self.system.id()
                )));
            }
            _ => {}
        }
        self.switch.arm(at);
        Ok(())
    }

    async fn reset(&self) -> Result<(), HarnessError> {
        self.system.ready().await?;
        let mut s = self.state.lock().await;
        for h in s.running.drain(..) {
            h.abort();
        }
        *s = State::new();
        self.switch.disarm();
        self.delay.set(None);
        drop(s);
        self.system.clear().await
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(HarnessError::failed("already running"));
        }
        s.epoch += 1;
        let factory = self.system.factory();
        let src_ctx = self.context(self.system.source_config());
        let (en, sf) = factory
            .source(&src_ctx)
            .map_err(|e| failed("source", e))?
            .ok_or_else(|| HarnessError::failed("the factory has no source"))?;
        let source = Pipeline::source(self.instance.clone(), self.config.clone())
            .enumerator(en)
            .sources(Arc::from(sf))
            .writer(Arc::new(CrashingWriter::new(
                s.fabric.clone(),
                self.switch.clone(),
            )))
            .checkpoints(Arc::new(s.store.clone()))
            .dead_letters(Arc::new(s.fabric.clone()))
            .discover_every(Duration::from_millis(200))
            .epoch(s.epoch)
            .start()
            .await
            .map_err(|e| failed("starting the source", e))?;
        let mut running = vec![source];
        if let Some(cfg) = self.system.sink_config() {
            let ctx = self.context(cfg);
            let sink = factory
                .sink(&ctx)
                .map_err(|e| failed("sink", e))?
                .ok_or_else(|| HarnessError::failed("the factory has no sink"))?;
            let delayed: Arc<dyn SinkFactory> = Arc::new(DelaySinkFactory {
                inner: sink,
                delay: self.delay.clone(),
            });
            let sink = Pipeline::sink(self.instance.clone(), self.config.clone())
                .reader(Box::new(CrashingReader::new(
                    s.fabric.reader(GROUP),
                    self.switch.clone(),
                )))
                .sink(Arc::new(CrashingSinkFactory::new(
                    delayed,
                    self.switch.clone(),
                )))
                .dead_letters(Arc::new(s.fabric.clone()))
                .epoch(s.epoch)
                .start()
                .await
                .map_err(|e| failed("starting the sink", e))?;
            running.push(sink);
        }
        s.running = running;
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        for h in s.running.drain(..) {
            if h.status().state != PipelineState::Running {
                continue;
            }
            let _ = tokio::time::timeout(Duration::from_secs(20), h.stop()).await;
        }
        Ok(())
    }

    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let has_sink = self.system.sink_config().is_some();
        loop {
            let (want, fabric) = {
                let mut s = self.state.lock().await;
                s.note_peak();
                for h in &s.running {
                    let st = h.status();
                    if matches!(st.state, PipelineState::Failed | PipelineState::Fenced) {
                        return Err(HarnessError::failed(format!(
                            "pipeline {:?}: {:?}",
                            st.state, st.last_error
                        )));
                    }
                }
                (s.ids.clone(), s.fabric.clone())
            };
            let observed = self.observed().await?;
            let got: std::collections::BTreeSet<&str> =
                observed.iter().map(|o| o.id.as_str()).collect();
            let missing = want.iter().filter(|id| !got.contains(id.as_str())).count();
            let drained = !has_sink || fabric.committed(GROUP) >= fabric.len() as u64;
            if missing == 0 && drained {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(HarnessError::failed(format!(
                    "not settled after {timeout:?}: {missing} of {} records missing from the target; \
                     Fabric {} events, {} committed",
                    want.len(),
                    fabric.len(),
                    fabric.committed(GROUP)
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError> {
        Ok(self.switch.wait_tripped(timeout).await)
    }

    async fn throttle(&self, t: Throttle) -> Result<(), HarnessError> {
        let s = self.state.lock().await;
        match t {
            Throttle::Target(d) => self.delay.set(Some(d)),
            Throttle::Fabric(d) => s.fabric.set_append_delay(Some(d)),
            Throttle::Off => {
                self.delay.set(None);
                s.fabric.set_append_delay(None);
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

    async fn cdc(&self) -> Result<loams_flow_conformance::CdcReport, HarnessError> {
        self.system.cdc(self).await
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        self.system.ready().await?;
        self.system.authenticate(method).await
    }
}

/// A tag that keeps concurrent harness runs (and reruns) apart in a shared
/// system: the process id, the start time and a counter.
pub fn run_tag() -> String {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "{}{}{n}",
        std::process::id(),
        crate::util::now_millis() % 1_000_000
    )
}

/// Whether the variable is set and not empty.
pub fn env_set(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| !v.is_empty())
}

/// An `Unavailable` error for a service that cannot be reached.
pub fn unavailable(what: &str, e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Unavailable(format!("{what}: {e}"))
}

/// This crate's directory.
pub fn crate_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

/// The registry's manifest for `id` (ruling R14.1: staged in this crate, then
/// installed as `connectors/registry/<id>.yaml`): checked against
/// `connector.schema.json`, every semantic rule, its config schema
/// (`connectors/schemas/<id>.config.json`) and, when it names one, its
/// profile under `connectors/profiles/`.
pub fn proposed_manifest(id: &str) -> Result<(ConnectorSpec, Option<Profile>), Vec<ManifestError>> {
    let connectors = loams_flow::manifest::repository_root().join("connectors");
    let err = |m: String| vec![ManifestError::new(id, m)];
    let text = std::fs::read_to_string(connectors.join("registry").join(format!("{id}.yaml")))
        .map_err(|e| err(e.to_string()))?;
    let value: Value = serde_norway::from_str(&text).map_err(|e| err(e.to_string()))?;
    let schema_text =
        std::fs::read_to_string(connectors.join("schemas").join(format!("{id}.config.json")))
            .map_err(|e| err(e.to_string()))?;
    let config_schema: Value =
        serde_json::from_str(&schema_text).map_err(|e| err(e.to_string()))?;
    let schema =
        loams_flow::validate::ManifestSchema::embedded().map_err(|e| err(e.to_string()))?;
    let mut spec = loams_flow::validate::validate_manifest_with_config_schema(
        &value,
        schema.validator(),
        Some(&config_schema),
    )?;
    spec.config_schema = config_schema;
    let profile = match spec.runtime.profile.clone() {
        None => None,
        Some(p) => {
            let path = loams_flow::manifest::repository_root()
                .join("connectors/profiles")
                .join(format!("{p}.yaml"));
            let loaded =
                loams_flow::profile::load_profile(&path).map_err(|e| err(e.to_string()))?;
            let problems = loams_flow::profile::check_manifest_profile(&spec, &loaded);
            if !problems.is_empty() {
                return Err(problems);
            }
            Some(loaded)
        }
    };
    Ok((spec, profile))
}
