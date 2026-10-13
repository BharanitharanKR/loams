//! Valkey and Redis, against the real servers (OR-2): Valkey 8 in
//! `connectors-p2`, Redis 8 in `connectors-ci-only` (OR-3). Without the
//! stack's variable every test here skips: nothing is pulled or run locally.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{declared, run_suites, skip, up};
use loams_connector_kv::harness::ChainHarness;
use loams_connector_kv::harness::redis::{Streams, ValkeyKeys, direct};
use loams_connector_kv::redis::RedisFactory;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    ConnectorContext, ConnectorFactory, InstanceKey, Pipeline, PipelineConfig, Scrubber,
    StaticSecrets,
};
use redis::Value;
use serde_json::json;

const P2: &str = "LOAMS_CONNECTORS_P2_STACK";
const CI_ONLY: &str = "LOAMS_CONNECTORS_CI_ONLY_STACK";

/// Valkey runs the Redis implementation under the `valkey` target profile:
/// keyspace reads by `SCAN`, keyed writes, every declared suite green
/// against real Valkey.
#[tokio::test(flavor = "multi_thread")]
async fn valkey_suite_against_valkey() {
    let h = ChainHarness::new(ValkeyKeys::new());
    run_suites("valkey", &h, &declared("valkey"), up(P2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_streams_suite_against_valkey() {
    let h = ChainHarness::new(Streams::valkey());
    run_suites("redis-streams", &h, &declared("redis-streams"), up(P2)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_streams_suite_against_redis() {
    let h = ChainHarness::new(Streams::redis());
    run_suites("redis-streams", &h, &declared("redis-streams"), up(CI_ONLY)).await;
}

async fn pending(url: &str, stream: &str, group: &str) -> i64 {
    let mut c = direct(url, json!({}), StaticSecrets::new())
        .await
        .expect("connect");
    let r: Result<Value, _> = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(&mut c)
        .await;
    match r {
        Ok(Value::Array(parts)) => match parts.first() {
            Some(Value::Int(n)) => *n,
            _ => 0,
        },
        _ => 0,
    }
}

/// The source acknowledges an entry (`XACK`) only after the Fabric has
/// acknowledged its batch: while the Fabric holds the batch the entries
/// stay pending in the consumer group; once it acknowledges, none is.
async fn ack_after_fabric(sys: Streams) {
    let mut c = direct(sys.url(), json!({}), StaticSecrets::new())
        .await
        .expect("connect");
    let input = sys.input();
    let _: Value = redis::cmd("DEL")
        .arg(&input)
        .query_async(&mut c)
        .await
        .expect("del");
    for i in 0..10 {
        let _: Value = redis::cmd("XADD")
            .arg(&input)
            .arg("*")
            .arg("n")
            .arg(i)
            .query_async(&mut c)
            .await
            .expect("xadd");
    }
    let inst = InstanceKey::new(1, format!("ack-{}", loams_connector_kv::harness::run_tag()));
    let group = format!("loams-{}", inst.name);
    let ctx = ConnectorContext::new(
        inst.clone(),
        json!({ "url": sys.url(), "stream": input, "block_ms": 100 }),
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    );
    let (en, sf) = RedisFactory::new("redis-streams")
        .source(&ctx)
        .expect("source")
        .expect("some");
    let fabric = MemoryFabric::new();
    fabric.hold();
    let h = Pipeline::source(inst, PipelineConfig::default())
        .enumerator(en)
        .sources(Arc::from(sf))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .start()
        .await
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(30);
    while pending(sys.url(), &input, &group).await < 10 {
        assert!(
            Instant::now() < deadline,
            "the source never read the entries"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        pending(sys.url(), &input, &group).await,
        10,
        "nothing is acknowledged before the Fabric acks"
    );
    assert_eq!(fabric.len(), 0);
    fabric.release();
    let deadline = Instant::now() + Duration::from_secs(30);
    while pending(sys.url(), &input, &group).await > 0 {
        assert!(
            Instant::now() < deadline,
            "entries are still pending after the Fabric acknowledged them"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(fabric.len(), 10);
    h.stop().await;
    let _: Value = redis::cmd("DEL")
        .arg(&input)
        .query_async(&mut c)
        .await
        .expect("del");
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_streams_group_ack_after_fabric() {
    if up(P2) {
        ack_after_fabric(Streams::valkey()).await;
    } else {
        skip(
            "redis_streams_group_ack_after_fabric (Valkey)",
            "LOAMS_CONNECTORS_P2_STACK is not set",
        );
    }
    if up(CI_ONLY) {
        ack_after_fabric(Streams::redis()).await;
    } else {
        skip(
            "redis_streams_group_ack_after_fabric (Redis)",
            "LOAMS_CONNECTORS_CI_ONLY_STACK is not set",
        );
    }
}
