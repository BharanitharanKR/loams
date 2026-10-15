//! Helpers shared by the SDK's integration tests.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterEnumerator, CounterFactory, CounterHandle, MapHandle, MapSinkFactory,
};
use loams_connector_sdk::{
    CloudEvent, ConnectorObserver, ErrorClass, InstanceKey, Pipeline, PipelineConfig,
    PipelineHandle, SourcePipeline,
};

pub const GROUP: &str = "flow-orders";

pub fn instance() -> InstanceKey {
    InstanceKey::new(7, "orders")
}

/// Small batches, checkpoints on every ack, a short linger.
pub fn small_config() -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: 10,
        max_batch_bytes: 64 << 10,
        linger: Duration::from_millis(10),
        max_in_flight_batches: 4,
        memory_budget_bytes: 1 << 20,
        checkpoint_interval: Duration::ZERO,
        ..PipelineConfig::default()
    }
}

/// Wait (up to 30 s of test time) until `cond` holds.
pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !cond() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A source pipeline over the counter source.
pub fn counter_pipeline(
    cfg: PipelineConfig,
    handle: &CounterHandle,
    counter: CounterConfig,
    fabric: &MemoryFabric,
    store: &MemoryCheckpointStore,
) -> SourcePipeline {
    let inst = instance();
    Pipeline::source(inst.clone(), cfg)
        .enumerator(Box::new(CounterEnumerator::new(handle.clone())))
        .sources(Arc::new(CounterFactory::new(
            handle.clone(),
            counter,
            &inst,
        )))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .dead_letters(Arc::new(fabric.clone()))
        .discover_every(Duration::from_secs(3600))
        .epoch(1)
}

/// `n` plain events `e/<i>` on the Fabric, starting at `from`.
pub fn push_events(fabric: &MemoryFabric, from: u64, n: u64) {
    for i in from..from + n {
        fabric.push(
            CloudEvent::new(
                format!("e/{i}"),
                "/test",
                "io.loams.dev.flow.test.record.v1",
            )
            .with_data("application/json", format!("{{\"i\":{i}}}")),
        );
    }
}

/// A sink pipeline from the Fabric's group into a map sink.
pub async fn map_sink_pipeline(
    cfg: PipelineConfig,
    fabric: &MemoryFabric,
    map: &MapHandle,
    transactional: bool,
    epoch: u64,
    observer: Option<Arc<dyn ConnectorObserver>>,
) -> PipelineHandle {
    let mut p = Pipeline::sink(instance(), cfg)
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::new(MapSinkFactory {
            handle: map.clone(),
            transactional,
        }))
        .dead_letters(Arc::new(fabric.clone()))
        .epoch(epoch);
    if let Some(o) = observer {
        p = p.observer(o);
    }
    p.start().await.expect("sink pipeline starts")
}

/// Records what the pipeline reports.
#[derive(Debug, Default)]
pub struct Recorder {
    pub retries: Mutex<Vec<(ErrorClass, u32, Duration)>>,
    pub errors: Mutex<Vec<(ErrorClass, String)>>,
    pub dead_letters: Mutex<u32>,
}

impl ConnectorObserver for Recorder {
    fn retry(&self, _i: &InstanceKey, class: &ErrorClass, attempt: u32, delay: Duration) {
        self.retries
            .lock()
            .unwrap()
            .push((class.clone(), attempt, delay));
    }
    fn error(&self, _i: &InstanceKey, class: &ErrorClass, message: &str) {
        self.errors
            .lock()
            .unwrap()
            .push((class.clone(), message.to_owned()));
    }
    fn dead_letter(&self, _i: &InstanceKey) {
        *self.dead_letters.lock().unwrap() += 1;
    }
}
