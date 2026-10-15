//! Budgets, backpressure, splits and secrets (CN2 Task 4).
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterHandle, MapHandle, ReferenceConnector,
};
use loams_connector_sdk::*;

#[tokio::test(start_paused = true)]
async fn source_respects_budget() {
    let cfg = PipelineConfig {
        max_batch_rows: 7,
        max_batch_bytes: 2_000,
        max_in_flight_batches: 2,
        memory_budget_bytes: 4_000,
        ..small_config()
    };
    let handle = CounterHandle::new(&["a", "b"]);
    let counter = CounterConfig {
        events_per_split: Some(200),
        payload_bytes: 50,
        ignore_budget: false,
    };
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let p = counter_pipeline(cfg.clone(), &handle, counter.clone(), &fabric, &store)
        .start()
        .await
        .unwrap();
    eventually("all 400 events appended", || fabric.len() == 400).await;
    let calls = handle.next_calls();
    assert!(!calls.is_empty());
    for c in &calls {
        assert!(!c.budget.is_zero(), "next called with an empty budget");
        assert!(
            c.budget.max_rows <= 7 && c.budget.max_bytes <= 2_000,
            "{c:?}"
        );
        assert!(c.budget.admits(c.rows, c.bytes), "source exceeded {c:?}");
    }
    let stats = p.stats();
    assert!(stats.peak_in_flight_batches <= 2, "{stats:?}");
    assert!(stats.peak_in_flight_bytes <= 4_000, "{stats:?}");
    assert_eq!(p.stop().await.state, PipelineState::Stopped);

    // A source that returns more than its budget is refused, not appended.
    let rogue = CounterHandle::new(&["a"]);
    let fabric = MemoryFabric::new();
    let p = counter_pipeline(
        cfg,
        &rogue,
        CounterConfig {
            ignore_budget: true,
            ..counter
        },
        &fabric,
        &MemoryCheckpointStore::new(),
    )
    .start()
    .await
    .unwrap();
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;
    let err = st.last_error.unwrap();
    assert_eq!(err.class, ErrorClass::Fatal);
    assert!(err.message.contains("budget"), "{}", err.message);
    assert!(fabric.is_empty());
}

#[tokio::test(start_paused = true)]
async fn pipeline_memory_bounded_under_slow_sink() {
    // A lease reserves a full batch (1 000 bytes) until the batch's real size
    // is known; 2 500 bytes of memory bound what is in flight.
    let cfg = PipelineConfig {
        max_batch_rows: 10,
        max_batch_bytes: 1_000,
        max_in_flight_batches: 3,
        memory_budget_bytes: 2_500,
        ..small_config()
    };

    // A fast, unbounded source into a slow Fabric.
    let handle = CounterHandle::new(&["a", "b"]);
    let fabric = MemoryFabric::new();
    fabric.set_append_delay(Some(Duration::from_millis(50)));
    let p = counter_pipeline(
        cfg.clone(),
        &handle,
        CounterConfig {
            events_per_split: None,
            payload_bytes: 20,
            ignore_budget: false,
        },
        &fabric,
        &MemoryCheckpointStore::new(),
    )
    .start()
    .await
    .unwrap();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let read = handle.emitted("a") + handle.emitted("b");
        let appended = fabric.len() as u64;
        // Sources are paused, not buffering: at most the in-flight batches'
        // worth of events is read and not yet in the Fabric.
        assert!(
            read - appended <= 3 * 10,
            "read {read}, appended {appended}"
        );
    }
    let stats = p.stats();
    assert!(stats.peak_in_flight_bytes <= 2_500, "{stats:?}");
    assert!(stats.peak_in_flight_batches <= 3, "{stats:?}");
    assert!(fabric.len() > 20, "the pipeline kept moving");
    p.stop().await;

    // A slow sink behind a full Fabric topic.
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 300);
    let map = MapHandle::new();
    map.set_delay(Some(Duration::from_millis(50)));
    let p = map_sink_pipeline(cfg, &fabric, &map, false, 1, None).await;
    eventually("all 300 written", || fabric.committed(GROUP) == 300).await;
    let stats = p.stats();
    assert!(stats.peak_in_flight_bytes <= 2_500, "{stats:?}");
    assert!(stats.peak_in_flight_batches <= 3, "{stats:?}");
    assert_eq!(map.len(), 300);
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn split_added_and_removed_without_restart() {
    let handle = CounterHandle::new(&["a"]);
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let counter = CounterConfig {
        events_per_split: Some(50),
        ..CounterConfig::default()
    };
    let p = counter_pipeline(small_config(), &handle, counter, &fabric, &store)
        .start()
        .await
        .unwrap();
    eventually("split a drained", || fabric.len() == 50).await;

    handle.add_split("b");
    p.rediscover();
    eventually("split b drained", || fabric.len() == 100).await;
    let st = p.status();
    assert_eq!(st.state, PipelineState::Running);
    assert_eq!(st.splits, vec![SplitId::new("a"), SplitId::new("b")]);

    handle.remove_split("a");
    p.rediscover();
    p.wait_for(|s| s.splits == vec![SplitId::new("b")]).await;
    eventually("a closed", || handle.closed().contains(&SplitId::new("a"))).await;
    eventually("a's position dropped", || {
        store
            .get(&instance())
            .is_some_and(|cp| !cp.splits.contains_key(&SplitId::new("a")))
    })
    .await;
    // Neither split was restarted: each was opened once.
    let opened = handle.opened();
    assert_eq!(opened.iter().filter(|(s, _)| s.as_str() == "a").count(), 1);
    assert_eq!(opened.iter().filter(|(s, _)| s.as_str() == "b").count(), 1);
    assert_eq!(p.status().state, PipelineState::Running);
    p.stop().await;
}

const SECRET: &str = "s3cr3t-hunter2-value";

/// A source that tries to leak its credential through every channel: a
/// position's display, a poison error and a fatal error.
struct LeakySource {
    secrets: Arc<dyn SecretResolver>,
    password: Option<Secret>,
    step: u32,
}

#[async_trait]
impl Source for LeakySource {
    async fn open(&mut self, _split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        self.password = Some(self.secrets.resolve(&SecretRef::new("password")).await?);
        Ok(())
    }

    async fn next(&mut self, _budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let pw = self
            .password
            .as_ref()
            .unwrap()
            .expose_str()
            .unwrap()
            .to_owned();
        self.step += 1;
        match self.step {
            1 => Ok(Some(SourceBatch {
                events: EventBatch::Events(vec![CloudEvent::new("x/1", "/leaky", "t")]),
                position: Position::new(vec![1u8], format!("lsn 1 for user:{pw}")),
            })),
            2 => Err(ConnectorError::poison(format!("bad record; password={pw}"))),
            _ => Err(ConnectorError::fatal(format!(
                "connect failed for user:{pw}@db"
            ))),
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

struct LeakyFactory(Arc<dyn SecretResolver>);

impl SourceFactory for LeakyFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(LeakySource {
            secrets: self.0.clone(),
            password: None,
            step: 0,
        }))
    }
}

struct OneSplit;

#[async_trait]
impl SplitEnumerator for OneSplit {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new("only", serde_json::json!({}))])
    }
}

#[tokio::test(start_paused = true)]
async fn no_secret_in_position_or_error() {
    let scrubber = Scrubber::new();
    let ctx = ConnectorContext::new(
        instance(),
        serde_json::json!({"password": {"secret": "password"}}),
        Arc::new(StaticSecrets::new().with("password", SECRET)),
        scrubber.clone(),
    );
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let recorder = Arc::new(Recorder::default());
    let p = Pipeline::source(instance(), small_config())
        .enumerator(Box::new(OneSplit))
        .sources(Arc::new(LeakyFactory(ctx.secrets.clone())))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .dead_letters(Arc::new(fabric.clone()))
        .observer(recorder.clone())
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap();
    let st = p.wait_for(|s| s.state == PipelineState::Failed).await;

    let mut seen = Vec::new();
    let err = st.last_error.unwrap();
    assert!(err.message.contains("<redacted>"), "{}", err.message);
    seen.push(err.message);
    let saves = store.saves();
    assert!(!saves.is_empty());
    for cp in &saves {
        for pos in cp.splits.values() {
            assert!(pos.display.contains("<redacted>"), "{}", pos.display);
            seen.push(pos.display.clone());
            seen.push(format!("{pos:?}"));
        }
    }
    let dead = fabric.dead_letters();
    assert_eq!(dead.len(), 1);
    seen.push(dead[0].error.clone());
    seen.push(format!("{:?}", dead[0]));
    for (_, m) in recorder.errors.lock().unwrap().iter() {
        seen.push(m.clone());
    }
    seen.push(format!("{ctx:?}"));
    seen.push(format!("{:?}", p.status()));
    for s in &seen {
        assert!(!s.contains(SECRET), "secret leaked: {s}");
        assert!(!s.contains("hunter2"), "secret leaked: {s}");
    }
}

#[tokio::test(start_paused = true)]
async fn reference_connector_runs_through_its_factory() {
    let connector = ReferenceConnector::new();
    let ctx = ConnectorContext::new(
        instance(),
        serde_json::json!({"splits": ["x", "y"], "events_per_split": 20}),
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    );
    assert_eq!(connector.id(), "reference");
    let (enumerator, sources) = connector.source(&ctx).unwrap().unwrap();
    let fabric = MemoryFabric::new();
    let src = Pipeline::source(instance(), small_config())
        .enumerator(enumerator)
        .sources(Arc::from(sources))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("40 events appended", || fabric.len() == 40).await;
    src.stop().await;

    let sink = connector.sink(&ctx).unwrap().unwrap();
    let p = Pipeline::sink(instance(), small_config())
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::from(sink))
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("40 events written", || connector.sink.len() == 40).await;
    let seen = Arc::new(Mutex::new(connector.sink.ids()));
    assert!(seen.lock().unwrap().contains(&"x/0".to_owned()));
    p.stop().await;
}
