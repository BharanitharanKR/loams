//! The Kafka ★ connector's tests (CN2 Task 9).
//!
//! The plan's named tests run against Apache Kafka itself (OR-2) and skip
//! without `LOAMS_CONNECTORS_STACK`. In CI, `connectors-p2` brings up the
//! `connectors` compose profile's `kafka` service and runs:
//!
//! ```text
//! docker compose -f deploy/fabric/compose.yaml --profile connectors up -d --wait kafka
//! LOAMS_CONNECTORS_STACK=1 cargo test -p loams-connector-kafka --test kafka -j 2
//! ```
//!
//! The `mock_*` tests run everywhere, on librdkafka's in-process mock
//! cluster: they exercise the connector's own logic (splits, offsets,
//! commit-after-ack, record mapping, the transactional call sequence), never
//! a system's behaviour, and never count toward a suite.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::*;
use loams_connector_kafka::Kafka;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{ConnectorFactory, ErrorClass, InstanceKey, SinkHandle};
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde_json::json;

// ------------------------------------------------------------ real Kafka --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_suite_against_kafka() {
    let Some(target) = Target::kafka() else {
        return;
    };
    roundtrip_suite(&target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_txn_sink_exactly_once_under_crash() {
    let Some(target) = Target::kafka() else {
        return;
    };
    exactly_once_under_crash_case(&target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_txn_recover_reads_checkpoint() {
    let Some(target) = Target::kafka() else {
        return;
    };
    recover_reads_checkpoint_case(&target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_added_is_discovered() {
    let Some(target) = Target::kafka() else {
        return;
    };
    let (topic, _) = target.topic("kafka-grow", 1).await;
    let inst = InstanceKey::new(1, unique("grow"));
    let cfg = target.config(
        &[&topic],
        json!({ "auto_offset_reset": "earliest", "group_id_prefix": unique("grp") }),
    );
    let ingest = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let handle = source_pipeline(
        &target,
        &inst,
        cfg,
        &ingest,
        &store,
        Duration::from_millis(500),
    )
    .await;
    eventually("one split", 60, || handle.status().splits.len() == 1).await;

    target.add_partitions(&topic, 3).await;
    handle.rediscover();
    eventually("the added partitions become splits", 60, || {
        handle.status().splits.len() == 3
    })
    .await;
    // A record on a new partition is read without a restart.
    produce_raw(
        &target,
        &topic,
        Some(2),
        vec![(None, b"on-2".to_vec(), vec![])],
    )
    .await;
    let want = format!("{topic}/2/0");
    eventually("the new partition is read", 60, || {
        ingest.ids().contains(&want)
    })
    .await;
    let state = handle.stop().await;
    assert!(state.last_error.is_none(), "{:?}", state.last_error);
}

// --------------------------------------------------------- mock cluster --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_source_reads_every_partition_and_commits_after_ack() {
    let target = Target::mock();
    let (topic, _) = target.topic("mock-src", 3).await;
    for p in 0..3 {
        let records = (0..10)
            .map(|i| {
                (
                    Some(format!("k{i}")),
                    format!("{{\"p\":{p},\"i\":{i}}}").into_bytes(),
                    vec![],
                )
            })
            .collect();
        produce_raw(&target, &topic, Some(p), records).await;
    }
    let inst = InstanceKey::new(1, "mock-src");
    let group = unique("grp");
    let cfg = target.config(
        &[&topic],
        json!({ "auto_offset_reset": "earliest", "group_id": group }),
    );
    let ingest = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let handle = source_pipeline(
        &target,
        &inst,
        cfg,
        &ingest,
        &store,
        Duration::from_secs(3600),
    )
    .await;
    let want: BTreeSet<String> = (0..3)
        .flat_map(|p| (0..10).map(move |o| (p, o)))
        .map(|(p, o)| format!("{topic}/{p}/{o}"))
        .collect();
    eventually("all 30 records ingested", 60, || id_set(&ingest) == want).await;
    let mut splits = handle.status().splits;
    splits.sort();
    assert_eq!(
        splits
            .iter()
            .map(|s| s.as_str().to_owned())
            .collect::<Vec<_>>(),
        (0..3).map(|p| format!("{topic}/{p}")).collect::<Vec<_>>()
    );
    for ev in ingest.events() {
        assert_eq!(ev.ty, "io.loams.dev.flow.kafka.record.v1");
        assert!(ev.extensions.contains_key("partitionkey"));
    }

    // Offsets are committed to the group once the Fabric acknowledged.
    let mut rc = target.raw().await;
    rc.set("group.id", &group);
    let consumer: BaseConsumer = rc.create().expect("consumer");
    let mut tpl = TopicPartitionList::new();
    for p in 0..3 {
        tpl.add_partition(&topic, p);
    }
    let committed = tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let c = consumer
                .committed_offsets(tpl.clone(), Duration::from_secs(10))
                .expect("committed offsets");
            let all = c
                .elements()
                .iter()
                .all(|e| e.offset() == Offset::Offset(10));
            if all || std::time::Instant::now() > deadline {
                return c;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .await
    .expect("task");
    for e in committed.elements() {
        assert_eq!(
            e.offset(),
            Offset::Offset(10),
            "partition {}",
            e.partition()
        );
    }
    let state = handle.stop().await;
    assert!(state.last_error.is_none(), "{:?}", state.last_error);
    // The checkpoint holds each split's next offset.
    let cp = store.get(&inst).expect("checkpoint");
    assert_eq!(cp.splits.len(), 3);
    for pos in cp.splits.values() {
        assert_eq!(pos.as_u64(), Some(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_roundtrip_through_the_idempotent_sink() {
    roundtrip_suite(&Target::mock()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_transactional_sink_recovers_its_checkpoint() {
    let target = Target::mock();
    // The mock cluster has no CreateTopics: the checkpoint topic is made here.
    target
        .create_topic(loams_connector_kafka::CHECKPOINT_TOPIC, 1, true)
        .await;
    recover_reads_checkpoint_case(&target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_poison_is_refused_before_anything_is_sent() {
    let target = Target::mock();
    let (topic, _) = target.topic("mock-poison", 1).await;
    let inst = InstanceKey::new(1, "mock-poison");
    for delivery in ["effectively_once", "exactly_once"] {
        let cfg = target.config(
            &[&topic],
            json!({ "max_record_bytes": 300, "delivery": delivery }),
        );
        let factory = target
            .factory()
            .sink(&target.ctx(&inst, cfg))
            .expect("factory")
            .expect("sink");
        let mut big = batch("big", 0, 3);
        if let loams_connector_sdk::EventBatch::Events(evs) = &mut big {
            evs[1].data = bytes::Bytes::from(vec![b'x'; 1000]);
        }
        let err = match factory.create().expect("sink") {
            SinkHandle::Plain(mut s) => s.write(big).await.expect_err("poison"),
            SinkHandle::Transactional(mut s) => s.write(big).await.expect_err("poison"),
        };
        assert_eq!(err.class, ErrorClass::Poison, "{delivery}: {err}");
    }
    assert!(
        target.read_all(&topic, "read_uncommitted").await.is_empty(),
        "nothing of a poison batch was produced"
    );
}

#[test]
fn factory_offers_both_directions_and_checks_the_sink_config() {
    let target = Target::mock();
    let inst = InstanceKey::new(1, "f");
    let f = Kafka::new();
    assert_eq!(f.id(), "kafka");
    let ok = target.config(&["t"], json!({}));
    assert!(
        f.source(&target.ctx(&inst, ok.clone()))
            .expect("source")
            .is_some()
    );
    assert!(f.sink(&target.ctx(&inst, ok)).expect("sink").is_some());
    for bad in [
        target.config(&["a", "b"], json!({})),
        target.config(&["t"], json!({ "enable_idempotence": false })),
        target.config(&["t"], json!({ "acks": "1" })),
    ] {
        assert!(f.sink(&target.ctx(&inst, bad)).is_err());
    }
}
