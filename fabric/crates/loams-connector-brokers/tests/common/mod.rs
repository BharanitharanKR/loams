//! The brokers family's conformance harness (CN2 Task 6's kit): one
//! `ConnectorHarness` for every broker, driven by a [`Plan`] that names the
//! broker's input and output for a run.
//!
//! A run seeds CloudEvents into the broker's input (through the connector's
//! own sink, so seeding is a real publish), runs the source pipeline
//! (input → in-memory Fabric) and the sink pipeline (Fabric → output), and
//! reads the output back through the connector's own source, counting copies
//! by event id. Faults are the kit's crash wrappers; a kill aborts both
//! pipelines, which drops the broker connections with their deliveries
//! unacknowledged, so the broker redelivers them.
//!
//! Every test here needs its broker up: the compose services of
//! `deploy/fabric/compose.yaml`, gated by `LOAMS_CONNECTORS_P2_STACK` (open
//! images) or `LOAMS_CONNECTORS_CI_ONLY_STACK` (the Service Bus emulator).
//! Without the variable a test prints why and returns, and the kit reports
//! its suites as skipped, which never counts toward `preview` (OR-23).

#![allow(dead_code, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EventBatch,
    InstanceKey, Pipeline, PipelineConfig, PipelineHandle, PipelineState, Scrubber, Sink, SinkAck,
    SinkFactory, SinkHandle, Source, StaticSecrets,
};
use loams_flow::ConnectorSpec;
use loams_flow_conformance::{
    ConnectorHarness, CrashingReader, CrashingSinkFactory, CrashingWriter, FaultPoint, FaultSwitch,
    HarnessError, KillTarget, MemoryReport, Observed, Seeded, ServiceKind, Suite, SuiteReport,
    Throttle,
};
use serde_json::Value;
use tokio::sync::Mutex;

pub const P2: &str = "LOAMS_CONNECTORS_P2_STACK";
pub const CI_ONLY: &str = "LOAMS_CONNECTORS_CI_ONLY_STACK";

/// Whether the stack a test needs is up; prints the skip otherwise.
pub fn stack_up(var: &str, test: &str) -> bool {
    let up = std::env::var(var).is_ok_and(|v| !v.is_empty());
    if !up {
        eprintln!("skipped {test}: set {var} once deploy/fabric/compose.yaml's services are up");
    }
    up
}

/// An endpoint: `LOAMS_<NAME>` if set, else the compose default.
pub fn endpoint(var: &str, default: &str) -> String {
    std::env::var(var)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// The registry's manifest (staged in this crate first, R18.1).
pub fn spec(id: &str) -> ConnectorSpec {
    let root = loams_flow::manifest::repository_root().join("connectors");
    loams_flow::load_manifest(&root.join("registry").join(format!("{id}.yaml")), &root)
        .unwrap_or_else(|e| panic!("{id}: {}", loams_flow::validate::render_errors(&e)))
}

/// Secrets the compose estate's test credentials resolve to (fixed values
/// for throwaway CI containers, R6.7).
pub fn secrets() -> Arc<StaticSecrets> {
    Arc::new(
        StaticSecrets::new()
            .with("rabbit-pw", endpoint("LOAMS_RABBITMQ_PASSWORD", "loams-test"))
            .with("artemis-pw", endpoint("LOAMS_ARTEMIS_PASSWORD", "loams-test"))
            .with(
                "sb-cs",
                endpoint(
                    "LOAMS_SERVICEBUS_CONNECTION_STRING",
                    "Endpoint=sb://localhost:5676;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
                ),
            ),
    )
}

pub fn ctx(instance: &str, config: Value) -> ConnectorContext {
    ConnectorContext::new(
        InstanceKey::new(1, instance),
        config,
        secrets(),
        Scrubber::new(),
    )
}

/// A conformance record: a CloudEvent whose id the output must carry.
pub fn record(id: &str) -> CloudEvent {
    CloudEvent::new(id, "/conformance", "io.loams.dev.conformance.record.v1").with_data(
        "application/json",
        Bytes::from(format!("{{\"id\":\"{id}\"}}")),
    )
}

/// Open the first split of a connector's source.
pub async fn open_source(
    f: &dyn ConnectorFactory,
    instance: &str,
    config: Value,
) -> Result<Box<dyn Source>, ConnectorError> {
    let c = ctx(instance, config);
    let (mut en, sf) = f
        .source(&c)?
        .ok_or_else(|| ConnectorError::fatal("no source in the config"))?;
    let split = en.discover(&[]).await?.remove(0);
    // A broker that `compose up` reports running may not serve yet: retry
    // transient failures for a while (READY), as the pipeline would.
    let deadline = tokio::time::Instant::now() + READY;
    loop {
        let mut s = sf.create(&split)?;
        match s.open(&split, None).await {
            Ok(()) => return Ok(s),
            Err(e) if e.class.is_transient() && tokio::time::Instant::now() < deadline => {
                eprintln!("waiting for the broker: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// How long a test waits for a broker to start serving.
pub const READY: Duration = Duration::from_secs(120);

/// Write `batch`, retrying transient failures until [`READY`] runs out.
pub async fn write_ready(
    sink: &mut dyn Sink,
    batch: EventBatch,
) -> Result<SinkAck, ConnectorError> {
    let deadline = tokio::time::Instant::now() + READY;
    loop {
        match sink.write(batch.clone()).await {
            Ok(a) => return Ok(a),
            Err(e) if e.class.is_transient() && tokio::time::Instant::now() < deadline => {
                eprintln!("waiting for the broker: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// A connector's sink.
pub fn open_sink(
    f: &dyn ConnectorFactory,
    instance: &str,
    config: Value,
) -> Result<Box<dyn Sink>, ConnectorError> {
    let c = ctx(instance, config);
    match f
        .sink(&c)?
        .ok_or_else(|| ConnectorError::fatal("no sink in the config"))?
        .create()?
    {
        SinkHandle::Plain(s) => Ok(s),
        SinkHandle::Transactional(_) => Err(ConnectorError::fatal("unexpected transactional sink")),
    }
}

/// The ids of a batch's events.
pub fn ids(batch: &EventBatch) -> Vec<String> {
    match batch {
        EventBatch::Events(v) => v.iter().map(|e| e.id.clone()).collect(),
        EventBatch::Arrow { .. } => Vec::new(),
    }
}

pub const BUDGET: Budget = Budget {
    max_rows: 1000,
    max_bytes: 8 << 20,
};

/// Read what a source has now, committing as it goes, until a read comes
/// back empty `idle` times in a row.
pub async fn drain(s: &mut dyn Source, idle: u32) -> Result<Vec<String>, ConnectorError> {
    let mut out = Vec::new();
    let mut empty = 0;
    while empty < idle {
        match s.next(BUDGET).await? {
            Some(b) => {
                empty = 0;
                out.extend(ids(&b.events));
                s.committed(&b.position).await?;
            }
            None => empty += 1,
        }
    }
    Ok(out)
}

/// What a run uses: the configs of the connector under test and of the
/// harness's own seeding sink and draining source, for one tag.
#[derive(Debug, Clone)]
pub struct Configs {
    /// The source under test (reads the input).
    pub source: Value,
    /// The sink under test (writes the output).
    pub sink: Value,
    /// The harness's sink that seeds the input.
    pub seed: Value,
    /// The harness's source that reads the output back.
    pub drain: Value,
}

/// A broker's part of the harness.
pub struct Plan {
    pub services: &'static [ServiceKind],
    pub factory: Arc<dyn ConnectorFactory>,
    /// The configs for a run tagged `tag`.
    pub configs: Box<dyn Fn(&str) -> Configs + Send + Sync>,
    /// Create the run's input and output (queues, streams); may be a no-op.
    pub setup: Box<dyn Fn(Configs) -> BoxFuture<'static, Result<(), String>> + Send + Sync>,
    /// Open (and close) the source under test before seeding, for brokers
    /// that keep nothing for a subscription that does not exist yet (MQTT).
    pub subscribe_first: bool,
    /// Auth methods the service supports here, each with config keys to
    /// merge over the source config to exercise it.
    pub auth: Vec<(&'static str, Value)>,
    /// How long to wait for a dropped connection's deliveries to come back.
    pub redelivery: Duration,
}

pub const GROUP: &str = "flow-brokers";

struct State {
    run: u64,
    tag: String,
    cfgs: Option<Configs>,
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    epoch: u64,
    running: Vec<PipelineHandle>,
    seeded: Vec<String>,
    observed: BTreeMap<String, u32>,
    drainer: Option<Box<dyn Source>>,
    peak: usize,
}

pub struct BrokerHarness {
    name: String,
    plan: Plan,
    config: PipelineConfig,
    switch: Arc<FaultSwitch>,
    delay: Arc<StdMutex<Option<Duration>>>,
    state: Mutex<State>,
}

impl std::fmt::Debug for BrokerHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerHarness")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// A sink that sleeps before each write while the target is throttled.
struct Delayed {
    inner: Box<dyn Sink>,
    delay: Arc<StdMutex<Option<Duration>>>,
}

#[async_trait]
impl Sink for Delayed {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let d = *self.delay.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(d) = d {
            tokio::time::sleep(d).await;
        }
        self.inner.write(batch).await
    }
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        self.inner.flush().await
    }
}

struct DelayedFactory {
    inner: Box<dyn SinkFactory>,
    delay: Arc<StdMutex<Option<Duration>>>,
}

impl SinkFactory for DelayedFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        match self.inner.create()? {
            SinkHandle::Plain(s) => Ok(SinkHandle::Plain(Box::new(Delayed {
                inner: s,
                delay: self.delay.clone(),
            }))),
            t => Ok(t),
        }
    }
}

impl BrokerHarness {
    pub fn new(name: &str, plan: Plan) -> Self {
        Self {
            name: name.to_owned(),
            plan,
            config: PipelineConfig {
                max_batch_rows: 50,
                max_batch_bytes: 256 << 10,
                linger: Duration::from_millis(20),
                max_in_flight_batches: 4,
                memory_budget_bytes: 4 << 20,
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            switch: FaultSwitch::new(),
            delay: Arc::new(StdMutex::new(None)),
            state: Mutex::new(State {
                run: 0,
                tag: String::new(),
                cfgs: None,
                fabric: MemoryFabric::new(),
                store: MemoryCheckpointStore::new(),
                epoch: 0,
                running: Vec::new(),
                seeded: Vec::new(),
                observed: BTreeMap::new(),
                drainer: None,
                peak: 0,
            }),
        }
    }

    fn instance(&self) -> InstanceKey {
        InstanceKey::new(1, self.name.clone())
    }

    fn cfgs(s: &State) -> Result<Configs, HarnessError> {
        s.cfgs
            .clone()
            .ok_or_else(|| HarnessError::failed("the harness was not reset"))
    }

    async fn pull_output(&self, s: &mut State) -> Result<(), HarnessError> {
        let Some(d) = s.drainer.as_mut() else {
            return Ok(());
        };
        let got = drain(d.as_mut(), 1)
            .await
            .map_err(|e| HarnessError::failed(format!("reading the output back: {e}")))?;
        for id in got {
            *s.observed.entry(id).or_default() += 1;
        }
        Ok(())
    }

    fn note_peak(s: &mut State) {
        for h in &s.running {
            s.peak = s.peak.max(h.stats().peak_in_flight_bytes);
        }
    }

    /// Run `suites` from the kit against this harness and the registry's
    /// manifest; skipped when the stack is down.
    pub async fn run(&self, id: &str, suites: &[Suite]) -> SuiteReport {
        let spec = spec(id);
        loams_flow_conformance::run(&spec, None, self, suites).await
    }
}

/// Assert a kit report is green, or entirely skipped (the stack is down).
pub fn assert_green_or_skipped(report: &SuiteReport) {
    eprintln!("{report}");
    if !report.results.is_empty() && report.skipped().len() == report.results.len() {
        return;
    }
    assert!(report.green(), "{report}");
}

#[async_trait]
impl ConnectorHarness for BrokerHarness {
    fn services(&self) -> &[ServiceKind] {
        self.plan.services
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        let cfgs = Self::cfgs(&s)?;
        let start = s.seeded.len();
        let ids: Vec<String> = (start..start + n)
            .map(|i| format!("{}-{i}", s.tag))
            .collect();
        let mut sink = open_sink(
            self.plan.factory.as_ref(),
            &format!("{}-seed", self.name),
            cfgs.seed,
        )
        .map_err(|e| HarnessError::Unavailable(format!("seeding sink: {e}")))?;
        for chunk in ids.chunks(100) {
            let events = chunk.iter().map(|i| record(i)).collect();
            write_ready(sink.as_mut(), EventBatch::Events(events))
                .await
                .map_err(|e| HarnessError::Unavailable(format!("seeding: {e}")))?;
        }
        sink.flush()
            .await
            .map_err(|e| HarnessError::Unavailable(format!("seeding: {e}")))?;
        s.seeded.extend(ids.iter().cloned());
        Ok(Seeded {
            ids: s.seeded.clone(),
        })
    }

    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        let mut s = self.state.lock().await;
        self.pull_output(&mut s).await?;
        Ok(s.observed
            .iter()
            .map(|(id, copies)| Observed {
                id: id.clone(),
                copies: *copies,
            })
            .collect())
    }

    async fn kill(&self, what: KillTarget) -> Result<(), HarnessError> {
        match what {
            KillTarget::Task | KillTarget::Pod => {
                let mut s = self.state.lock().await;
                Self::note_peak(&mut s);
                for h in s.running.drain(..) {
                    h.abort();
                }
                // Let the broker notice the dropped connections.
                tokio::time::sleep(self.plan.redelivery).await;
                Ok(())
            }
            KillTarget::System => Err(HarnessError::Unsupported(
                "the compose broker is shared by the run and is not restarted".into(),
            )),
        }
    }

    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError> {
        if at == FaultPoint::AfterPrepare {
            return Err(HarnessError::Unsupported(
                "broker sinks are not transactional".into(),
            ));
        }
        self.switch.arm(at);
        Ok(())
    }

    async fn reset(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        for h in s.running.drain(..) {
            h.abort();
        }
        if let Some(mut d) = s.drainer.take() {
            let _ = d.close().await;
        }
        self.switch.disarm();
        *self.delay.lock().unwrap_or_else(|p| p.into_inner()) = None;
        s.run += 1;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() % 100_000_000)
            .unwrap_or_default();
        s.tag = format!("lc{stamp}x{}", s.run);
        let cfgs = (self.plan.configs)(&s.tag);
        (self.plan.setup)(cfgs.clone())
            .await
            .map_err(HarnessError::Unavailable)?;
        if self.plan.subscribe_first {
            let mut src = open_source(self.plan.factory.as_ref(), &self.name, cfgs.source.clone())
                .await
                .map_err(|e| HarnessError::Unavailable(format!("subscribing first: {e}")))?;
            let _ = src.close().await;
        }
        let drainer = open_source(
            self.plan.factory.as_ref(),
            &format!("{}-drain", self.name),
            cfgs.drain.clone(),
        )
        .await
        .map_err(|e| HarnessError::Unavailable(format!("the output reader: {e}")))?;
        s.drainer = Some(drainer);
        s.cfgs = Some(cfgs);
        s.fabric = MemoryFabric::new();
        s.store = MemoryCheckpointStore::new();
        s.seeded.clear();
        s.observed.clear();
        s.peak = 0;
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(HarnessError::failed("already running"));
        }
        let cfgs = Self::cfgs(&s)?;
        s.epoch += 1;
        let inst = self.instance();
        let src_ctx = ctx(&self.name, cfgs.source.clone());
        let (en, sf) = self
            .plan
            .factory
            .source(&src_ctx)
            .map_err(|e| HarnessError::failed(format!("source: {e}")))?
            .ok_or_else(|| HarnessError::failed("no source"))?;
        let source = Pipeline::source(inst.clone(), self.config.clone())
            .enumerator(en)
            .sources(Arc::from(sf))
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
            .map_err(|e| HarnessError::failed(format!("source pipeline: {e}")))?;
        let sink_ctx = ctx(&format!("{}-out", self.name), cfgs.sink.clone());
        let sf = self
            .plan
            .factory
            .sink(&sink_ctx)
            .map_err(|e| HarnessError::failed(format!("sink: {e}")))?
            .ok_or_else(|| HarnessError::failed("no sink"))?;
        let delayed: Arc<dyn SinkFactory> = Arc::new(DelayedFactory {
            inner: sf,
            delay: self.delay.clone(),
        });
        let sink = Pipeline::sink(inst, self.config.clone())
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
            .map_err(|e| HarnessError::failed(format!("sink pipeline: {e}")))?;
        s.running = vec![source, sink];
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        Self::note_peak(&mut s);
        for h in s.running.drain(..) {
            if h.status().state != PipelineState::Running {
                continue;
            }
            let _ = tokio::time::timeout(Duration::from_secs(10), h.stop()).await;
        }
        Ok(())
    }

    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let mut s = self.state.lock().await;
                Self::note_peak(&mut s);
                for h in &s.running {
                    let st = h.status();
                    if matches!(st.state, PipelineState::Failed | PipelineState::Fenced) {
                        return Err(HarnessError::failed(format!(
                            "pipeline {:?}: {:?}",
                            st.state, st.last_error
                        )));
                    }
                }
                self.pull_output(&mut s).await?;
                let all = s.seeded.iter().all(|id| s.observed.contains_key(id));
                let committed = s.fabric.committed(GROUP) == s.fabric.len() as u64;
                if all && committed {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    let missing = s
                        .seeded
                        .iter()
                        .filter(|id| !s.observed.contains_key(*id))
                        .count();
                    return Err(HarnessError::failed(format!(
                        "not settled after {timeout:?}: {missing} of {} records missing from the output; Fabric {} committed {}",
                        s.seeded.len(),
                        s.fabric.len(),
                        s.fabric.committed(GROUP)
                    )));
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError> {
        Ok(self.switch.wait_tripped(timeout).await)
    }

    async fn throttle(&self, t: Throttle) -> Result<(), HarnessError> {
        let s = self.state.lock().await;
        match t {
            Throttle::Target(d) => {
                *self.delay.lock().unwrap_or_else(|p| p.into_inner()) = Some(d);
            }
            Throttle::Fabric(d) => s.fabric.set_append_delay(Some(d)),
            Throttle::Off => {
                *self.delay.lock().unwrap_or_else(|p| p.into_inner()) = None;
                s.fabric.set_append_delay(None);
            }
        }
        Ok(())
    }

    async fn memory(&self) -> Result<MemoryReport, HarnessError> {
        let mut s = self.state.lock().await;
        Self::note_peak(&mut s);
        Ok(MemoryReport {
            peak_bytes: s.peak,
            budget_bytes: self.config.memory_budget_bytes,
        })
    }

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        let Some((_, overrides)) = self.plan.auth.iter().find(|(m, _)| *m == method) else {
            return Err(HarnessError::Unsupported(format!(
                "the compose service is not configured for {method}"
            )));
        };
        let s = self.state.lock().await;
        let mut cfg = Self::cfgs(&s)?.source;
        if let (Some(c), Some(o)) = (cfg.as_object_mut(), overrides.as_object()) {
            for (k, v) in o {
                c.insert(k.clone(), v.clone());
            }
        }
        drop(s);
        let mut src = open_source(
            self.plan.factory.as_ref(),
            &format!("{}-auth", self.name),
            cfg,
        )
        .await
        .map_err(|e| HarnessError::failed(format!("{method}: {e}")))?;
        let _ = src.close().await;
        Ok(())
    }
}
