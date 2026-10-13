//! Delivery mechanics (CN2 Shared contracts, OR-7).
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::testing::reference::{
    CounterConfig, CounterEnumerator, CounterFactory, CounterHandle, MapHandle,
};
use loams_connector_sdk::*;

/// The Fabric, recording every position it acknowledged.
#[derive(Clone)]
struct AckLog {
    inner: MemoryFabric,
    acked: Arc<Mutex<HashSet<Position>>>,
}

#[async_trait]
impl FabricWriter for AckLog {
    async fn append(&self, b: &SourceBatch) -> Result<FabricAck, FabricError> {
        let ack = self.inner.append(b).await?;
        self.acked.lock().unwrap().insert(b.position.clone());
        Ok(ack)
    }
}

/// A counter source that checks, on every upstream commit, that the Fabric
/// acknowledged that position first.
struct Checked {
    inner: Box<dyn Source>,
    acked: Arc<Mutex<HashSet<Position>>>,
    violations: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Source for Checked {
    async fn open(&mut self, split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.inner.open(split, pos).await
    }
    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.inner.next(budget).await
    }
    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        if !self.acked.lock().unwrap().contains(upto) {
            self.violations.lock().unwrap().push(format!(
                "committed {upto} before the Fabric acknowledged it"
            ));
        }
        self.inner.committed(upto).await
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }
}

struct CheckedFactory {
    inner: CounterFactory,
    acked: Arc<Mutex<HashSet<Position>>>,
    violations: Arc<Mutex<Vec<String>>>,
}

impl SourceFactory for CheckedFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(Checked {
            inner: self.inner.create(split)?,
            acked: self.acked.clone(),
            violations: self.violations.clone(),
        }))
    }
}

#[tokio::test(start_paused = true)]
async fn upstream_commit_only_after_fabric_ack() {
    let fabric = MemoryFabric::new();
    let writer = AckLog {
        inner: fabric.clone(),
        acked: Arc::default(),
    };
    let violations = Arc::new(Mutex::new(Vec::new()));
    let handle = CounterHandle::new(&["a", "b"]);
    let store = MemoryCheckpointStore::new();
    let inst = instance();
    let factory = CheckedFactory {
        inner: CounterFactory::new(
            handle.clone(),
            CounterConfig {
                events_per_split: Some(50),
                ..CounterConfig::default()
            },
            &inst,
        ),
        acked: writer.acked.clone(),
        violations: violations.clone(),
    };

    // The Fabric holds its acks: sources read, nothing is committed upstream
    // and nothing is checkpointed.
    fabric.hold();
    let p = Pipeline::source(inst.clone(), small_config())
        .enumerator(Box::new(CounterEnumerator::new(handle.clone())))
        .sources(Arc::new(factory))
        .writer(Arc::new(writer))
        .checkpoints(Arc::new(store.clone()))
        .epoch(1)
        .start()
        .await
        .unwrap();
    // Reads stop once the in-flight budget is used up, so a split may not
    // have read at all yet; something has.
    eventually("reads in flight", || {
        handle.emitted("a") + handle.emitted("b") > 0
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(handle.committed("a").is_empty() && handle.committed("b").is_empty());
    assert!(store.saves().is_empty(), "checkpointed before the ack");

    fabric.release();
    eventually("everything committed upstream", || {
        handle.committed("a").last() == Some(&50) && handle.committed("b").last() == Some(&50)
    })
    .await;
    assert!(
        violations.lock().unwrap().is_empty(),
        "{:?}",
        violations.lock().unwrap()
    );
    // Upstream commits only move forward.
    for split in ["a", "b"] {
        let c = handle.committed(split);
        assert!(c.windows(2).all(|w| w[0] < w[1]), "{split}: {c:?}");
    }
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn transactional_sink_recovers_and_skips_committed() {
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 100);
    let map = MapHandle::new();

    let first = map_sink_pipeline(small_config(), &fabric, &map, true, 1, None).await;
    eventually("first run committed", || fabric.committed(GROUP) == 100).await;
    assert_eq!(first.stop().await.state, PipelineState::Stopped);
    let cp = map.checkpoint().unwrap();
    assert_eq!(cp.epoch, 1);
    assert_eq!(cp.splits.values().next().unwrap().as_u64(), Some(100));

    // A crash after the target committed but before the Fabric commit: the
    // Fabric's offset (advisory) is behind. A prepared transaction was also
    // left behind by the crash.
    fabric.set_committed(GROUP, 40);
    map.leave_prepared(
        vec![CloudEvent::new("zombie", "/test", "t")],
        Checkpoint::new(instance(), 1),
    );
    push_events(&fabric, 100, 20);

    let second = map_sink_pipeline(small_config(), &fabric, &map, true, 2, None).await;
    eventually("second run committed", || fabric.committed(GROUP) == 120).await;
    second.stop().await;

    assert_eq!(map.len(), 120);
    assert_eq!(map.max_applied(), 1, "an event reached the target twice");
    assert!(
        !map.ids().contains(&"zombie".to_owned()),
        "the orphan was committed"
    );
    assert!(map.aborted() >= 1);
    let cp = map.checkpoint().unwrap();
    assert_eq!(cp.epoch, 2);
    assert_eq!(cp.splits.values().next().unwrap().as_u64(), Some(120));
}
