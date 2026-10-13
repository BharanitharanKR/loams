//! The kit's self-test harness: the SDK's reference connector
//! (`loams_connector_sdk::testing::reference`, ruling R4.x) as the system
//! under test. Its "system" is in memory: the counter source is the input,
//! the in-memory Fabric (with `ingest`'s `(source, id)` dedup) is the bus,
//! and the map sink, transactional or not, is the target. The harness runs
//! the whole chain — counter → Fabric → map — so every [`FaultPoint`] has a
//! place to trip.
//!
//! It is a test double for the **kit**, not for any external system: its
//! service is the emulator `reference`, which only [`ReferenceHarness::run_env`]
//! declares, and `systems.toml` never lists it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterEnumerator, CounterFactory, CounterHandle, MapHandle, MapSinkFactory,
};
use loams_connector_sdk::{
    InstanceKey, Pipeline, PipelineConfig, PipelineHandle, PipelineState, SinkFactory,
};
use tokio::sync::Mutex;

use crate::crash::{CrashingReader, CrashingSinkFactory, CrashingWriter, FaultSwitch};
use crate::services::{
    ComposeEstate, ComposeService, LoamsModules, ServiceDecl, ServiceKind, SystemEntry,
    SystemsCatalog,
};
use crate::suites::RunEnv;
use crate::{
    ConnectorHarness, FaultPoint, HarnessError, KillTarget, MemoryReport, Observed, Seeded,
    Throttle,
};

/// The consumer group the map sink reads.
pub const GROUP: &str = "flow-reference";

/// The reference harness's one service.
pub const SERVICES: &[ServiceKind] = &[ServiceKind::Emulator {
    compose_service: "reference",
    system: "Reference connector",
    vendor: "loams-connector-sdk",
}];

struct State {
    fabric: MemoryFabric,
    store: MemoryCheckpointStore,
    source: CounterHandle,
    sink: MapHandle,
    per_split: u64,
    epoch: u64,
    running: Vec<PipelineHandle>,
    peak: usize,
}

impl State {
    fn new(splits: &[String]) -> Self {
        let names: Vec<&str> = splits.iter().map(String::as_str).collect();
        Self {
            fabric: MemoryFabric::new(),
            store: MemoryCheckpointStore::new(),
            source: CounterHandle::new(&names),
            sink: MapHandle::new(),
            per_split: 0,
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

/// See the module documentation.
pub struct ReferenceHarness {
    transactional: bool,
    splits: Vec<String>,
    config: PipelineConfig,
    switch: Arc<FaultSwitch>,
    state: Mutex<State>,
}

impl std::fmt::Debug for ReferenceHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferenceHarness")
            .field("transactional", &self.transactional)
            .field("splits", &self.splits)
            .finish_non_exhaustive()
    }
}

impl ReferenceHarness {
    /// A harness over two splits, `a` and `b`, with small batches. With
    /// `transactional`, the map sink commits its checkpoint with the data
    /// (exactly-once); without, it upserts at least once.
    pub fn new(transactional: bool) -> Self {
        let splits = vec!["a".to_owned(), "b".to_owned()];
        Self {
            transactional,
            state: Mutex::new(State::new(&splits)),
            splits,
            config: PipelineConfig {
                max_batch_rows: 10,
                max_batch_bytes: 64 << 10,
                linger: Duration::from_millis(5),
                max_in_flight_batches: 4,
                memory_budget_bytes: 1 << 20,
                checkpoint_interval: Duration::ZERO,
                ..PipelineConfig::default()
            },
            switch: FaultSwitch::new(),
        }
    }

    pub fn instance() -> InstanceKey {
        InstanceKey::new(1, "reference")
    }

    /// The map sink's handle (the target).
    pub async fn target(&self) -> MapHandle {
        self.state.lock().await.sink.clone()
    }

    /// The environment in which this harness's service is up: a catalog
    /// entry `reference` and a compose service `reference` in the
    /// `connectors-p2` stack, which is marked up.
    pub fn run_env() -> RunEnv {
        let mut systems = BTreeMap::new();
        systems.insert(
            "reference".to_owned(),
            SystemEntry {
                systems: vec!["Reference connector".to_owned()],
                family: "sdk".to_owned(),
                services: SERVICES.iter().map(ServiceKind::to_decl).collect(),
                extra_targets: Vec::<ServiceDecl>::new(),
                harness: Some("loams_flow_conformance::reference::ReferenceHarness".to_owned()),
                notes: Vec::new(),
            },
        );
        let mut compose = ComposeEstate::default();
        compose.services.insert(
            "reference".to_owned(),
            ComposeService {
                image: "in-process".to_owned(),
                profiles: vec!["connectors-p2".to_owned()],
                ci_service: false,
                licence: Some("Apache-2.0".to_owned()),
            },
        );
        let mut vars = BTreeMap::new();
        vars.insert("LOAMS_CONNECTORS_P2_STACK".to_owned(), "1".to_owned());
        RunEnv {
            root: PathBuf::from("."),
            catalog: SystemsCatalog {
                loams_modules: LoamsModules::default(),
                systems,
            },
            compose,
            vars,
        }
    }

    fn total(&self, per_split: u64) -> usize {
        per_split as usize * self.splits.len()
    }
}

#[async_trait]
impl ConnectorHarness for ReferenceHarness {
    fn services(&self) -> &[ServiceKind] {
        SERVICES
    }

    async fn seed(&self, n: usize) -> Result<Seeded, HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(HarnessError::failed(
                "the reference counter is seeded before start",
            ));
        }
        s.per_split += (n as u64).div_ceil(self.splits.len() as u64);
        let ids = self
            .splits
            .iter()
            .flat_map(|sp| (0..s.per_split).map(move |i| format!("{sp}/{i}")))
            .collect();
        Ok(Seeded { ids })
    }

    async fn read_back(&self) -> Result<Vec<Observed>, HarnessError> {
        let s = self.state.lock().await;
        let st = s.sink.state.lock().unwrap_or_else(|p| p.into_inner());
        Ok(st
            .map
            .keys()
            .map(|id| Observed {
                id: id.clone(),
                copies: st.applied.get(id).copied().unwrap_or(1),
            })
            .collect())
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
                "the reference system is in memory and cannot restart".into(),
            )),
        }
    }

    async fn inject(&self, at: FaultPoint) -> Result<(), HarnessError> {
        if at == FaultPoint::AfterPrepare && !self.transactional {
            return Err(HarnessError::Unsupported(
                "AfterPrepare needs the transactional map sink".into(),
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
        *s = State::new(&self.splits);
        self.switch.disarm();
        Ok(())
    }

    async fn start(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        if !s.running.is_empty() {
            return Err(HarnessError::failed("already running"));
        }
        s.epoch += 1;
        let inst = Self::instance();
        let counter = CounterConfig {
            events_per_split: Some(s.per_split),
            payload_bytes: 16,
            ignore_budget: false,
        };
        let source = Pipeline::source(inst.clone(), self.config.clone())
            .enumerator(Box::new(CounterEnumerator::new(s.source.clone())))
            .sources(Arc::new(CounterFactory::new(
                s.source.clone(),
                counter,
                &inst,
            )))
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
            .map_err(|e| HarnessError::failed(format!("source: {e}")))?;
        let map: Arc<dyn SinkFactory> = Arc::new(MapSinkFactory {
            handle: s.sink.clone(),
            transactional: self.transactional,
        });
        let sink = Pipeline::sink(inst, self.config.clone())
            .reader(Box::new(CrashingReader::new(
                s.fabric.reader(GROUP),
                self.switch.clone(),
            )))
            .sink(Arc::new(CrashingSinkFactory::new(map, self.switch.clone())))
            .dead_letters(Arc::new(s.fabric.clone()))
            .epoch(s.epoch)
            .start()
            .await
            .map_err(|e| HarnessError::failed(format!("sink: {e}")))?;
        s.running = vec![source, sink];
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        let mut s = self.state.lock().await;
        s.note_peak();
        for h in s.running.drain(..) {
            let st = h.status();
            if st.state != PipelineState::Running {
                continue;
            }
            // A graceful stop drains in-flight work; bound it.
            let _ = tokio::time::timeout(Duration::from_secs(10), h.stop()).await;
        }
        Ok(())
    }

    async fn settle(&self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
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
                let total = self.total(s.per_split);
                let cp = s.store.get(&Self::instance());
                let source_done = self.splits.iter().all(|sp| {
                    cp.as_ref()
                        .and_then(|c| c.splits.get(&loams_connector_sdk::SplitId::new(sp.clone())))
                        .and_then(|p| p.as_u64())
                        == Some(s.per_split)
                });
                // An exactly-once sink's progress is the checkpoint it
                // committed with the data; the Fabric offset is advisory and
                // may stay behind after a crash (Shared contracts).
                let sink_at = if self.transactional {
                    s.sink
                        .checkpoint()
                        .and_then(|c| c.splits.values().next().and_then(|p| p.as_u64()))
                        .unwrap_or(0)
                } else {
                    s.fabric.committed(GROUP)
                };
                let sink_done = s.sink.len() >= total && sink_at == s.fabric.len() as u64;
                if (source_done || total == 0) && sink_done {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    let states: Vec<String> = s
                        .running
                        .iter()
                        .map(|h| format!("{:?}", h.status()))
                        .collect();
                    return Err(HarnessError::failed(format!(
                        "not settled after {timeout:?}: target {} of {total}, Fabric {} committed {}, \
                         source checkpoint {cp:?}, pipelines {states:?}",
                        s.sink.len(),
                        s.fabric.len(),
                        s.fabric.committed(GROUP)
                    )));
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn tripped(&self, timeout: Duration) -> Result<bool, HarnessError> {
        Ok(self.switch.wait_tripped(timeout).await)
    }

    async fn throttle(&self, t: Throttle) -> Result<(), HarnessError> {
        let s = self.state.lock().await;
        match t {
            Throttle::Target(d) => s.sink.set_delay(Some(d)),
            Throttle::Fabric(d) => s.fabric.set_append_delay(Some(d)),
            Throttle::Off => {
                s.sink.set_delay(None);
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

    async fn authenticate(&self, method: &str) -> Result<(), HarnessError> {
        if method == "none" {
            Ok(())
        } else {
            Err(HarnessError::Unsupported(format!(
                "the reference connector has no auth method {method}"
            )))
        }
    }
}
