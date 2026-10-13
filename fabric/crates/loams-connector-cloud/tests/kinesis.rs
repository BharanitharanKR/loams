//! CN1 Task 13 / CN2 Task 19: Amazon Kinesis Data Streams (the Kinesis ★,
//! built on the SDK in the cloud family). Shards are splits; a child shard is
//! read only after its parents are finished; positions are sequence numbers;
//! `PutRecords` retries only the records that failed.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use common::*;
use loams_connector_cloud::aws::{AwsClient, AwsService};
use loams_connector_cloud::kinesis::KinesisFactory;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{ConnectorFactory as _, Pipeline, PipelineHandle, StaticSecrets};
use serde_json::{Value, json};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[derive(Debug, Clone, Default)]
struct Shard {
    id: String,
    parent: Option<String>,
    adjacent: Option<String>,
    closed: bool,
    records: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Default)]
struct KState {
    shards: Vec<Shard>,
    /// iterator token → (shard, next record index)
    iterators: BTreeMap<String, (String, usize)>,
    next_token: u64,
    expire_once: bool,
    put_calls: Vec<Value>,
    put_fail_first: bool,
}

#[derive(Clone)]
struct FakeKinesis {
    api: FakeApi,
    state: Arc<Mutex<KState>>,
}

impl FakeKinesis {
    async fn start(shards: Vec<Shard>) -> Self {
        let state = Arc::new(Mutex::new(KState {
            shards,
            ..KState::default()
        }));
        let s = state.clone();
        let api = FakeApi::start(Arc::new(move |seen: &Seen| kinesis(&s, seen))).await;
        Self { api, state }
    }

    fn with<R>(&self, f: impl FnOnce(&mut KState) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }
}

fn seq(shard: usize, n: usize) -> String {
    format!("4959{shard:04}{n:010}")
}

fn shard(n: usize, parent: Option<usize>, closed: bool, records: usize) -> Shard {
    Shard {
        id: format!("shardId-{n:012}"),
        parent: parent.map(|p| format!("shardId-{p:012}")),
        adjacent: None,
        closed,
        records: (0..records)
            .map(|i| {
                (
                    seq(n, i),
                    format!("{{\"shard\":{n},\"i\":{i}}}").into_bytes(),
                )
            })
            .collect(),
    }
}

fn kinesis(state: &Arc<Mutex<KState>>, seen: &Seen) -> Reply {
    let req = seen.json();
    let mut s = state.lock().unwrap();
    match seen.op().as_str() {
        "ListShards" => {
            let shards: Vec<Value> = s
                .shards
                .iter()
                .map(|sh| {
                    let mut range = json!({"StartingSequenceNumber": seq(0, 0)});
                    if sh.closed {
                        range["EndingSequenceNumber"] = json!("99999");
                    }
                    let mut v = json!({"ShardId": sh.id, "SequenceNumberRange": range,
                        "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "1"}});
                    if let Some(p) = &sh.parent {
                        v["ParentShardId"] = json!(p);
                    }
                    if let Some(p) = &sh.adjacent {
                        v["AdjacentParentShardId"] = json!(p);
                    }
                    v
                })
                .collect();
            json_reply(200, json!({"Shards": shards}))
        }
        "GetShardIterator" => {
            let id = req["ShardId"].as_str().unwrap().to_owned();
            let sh = s.shards.iter().find(|x| x.id == id).cloned().unwrap();
            let start = match req["ShardIteratorType"].as_str().unwrap() {
                "TRIM_HORIZON" => 0,
                "LATEST" => sh.records.len(),
                "AFTER_SEQUENCE_NUMBER" => {
                    let after = req["StartingSequenceNumber"].as_str().unwrap();
                    sh.records.iter().position(|(q, _)| q == after).unwrap() + 1
                }
                other => panic!("unexpected iterator type {other}"),
            };
            s.next_token += 1;
            let tok = format!("it-{}", s.next_token);
            s.iterators.insert(tok.clone(), (id, start));
            json_reply(200, json!({"ShardIterator": tok}))
        }
        "GetRecords" => {
            let tok = req["ShardIterator"].as_str().unwrap();
            if s.expire_once {
                s.expire_once = false;
                return json_reply(
                    400,
                    json!({"__type": "ExpiredIteratorException", "message": "Iterator expired"}),
                );
            }
            let (id, idx) = s.iterators.get(tok).cloned().unwrap();
            let limit = req["Limit"].as_u64().unwrap_or(10_000) as usize;
            let sh = s.shards.iter().find(|x| x.id == id).cloned().unwrap();
            let end = (idx + limit).min(sh.records.len());
            let recs: Vec<Value> = sh.records[idx..end]
                .iter()
                .map(|(q, d)| {
                    json!({"SequenceNumber": q, "Data": B64.encode(d),
                    "PartitionKey": "pk", "ApproximateArrivalTimestamp": 1760313600.5})
                })
                .collect();
            let next = if sh.closed && end == sh.records.len() {
                Value::Null
            } else {
                s.next_token += 1;
                let t = format!("it-{}", s.next_token);
                s.iterators.insert(t.clone(), (id, end));
                json!(t)
            };
            let mut out = json!({"Records": recs, "MillisBehindLatest": 0});
            if !next.is_null() {
                out["NextShardIterator"] = next;
            } else {
                out["ChildShards"] = json!([]);
            }
            json_reply(200, out)
        }
        "PutRecords" => {
            s.put_calls.push(req.clone());
            let records = req["Records"].as_array().cloned().unwrap();
            let first = std::mem::take(&mut s.put_fail_first);
            let mut out = Vec::new();
            let mut failed = 0;
            for (i, r) in records.iter().enumerate() {
                if first && i == 1 {
                    failed += 1;
                    out.push(json!({"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded"}));
                } else {
                    let data = B64.decode(r["Data"].as_str().unwrap()).unwrap();
                    let n = s.shards[0].records.len();
                    let q = seq(0, n);
                    s.shards[0].records.push((q.clone(), data));
                    out.push(json!({"SequenceNumber": q, "ShardId": s.shards[0].id}));
                }
            }
            json_reply(200, json!({"FailedRecordCount": failed, "Records": out}))
        }
        other => json_reply(
            400,
            json!({"__type": "UnknownOperationException", "message": other}),
        ),
    }
}

fn config(fake: &FakeKinesis) -> Value {
    json!({"region": "us-east-1", "endpoint": fake.api.base, "stream": "orders",
           "poll_interval_ms": 10})
}

async fn source_with_store(
    ctx: &loams_connector_sdk::ConnectorContext,
    fabric: &MemoryFabric,
    store: &MemoryCheckpointStore,
) -> PipelineHandle {
    let (enumerator, sources) = KinesisFactory::new().source(ctx).unwrap().unwrap();
    Pipeline::source(ctx.instance.clone(), small_config())
        .enumerator(enumerator)
        .sources(Arc::from(sources))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .discover_every(Duration::from_millis(50))
        .epoch(1)
        .start()
        .await
        .unwrap()
}

fn shard_of(ev: &loams_connector_sdk::CloudEvent) -> String {
    ev.id.split('/').nth(1).unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kinesis_resharding_followed() {
    // shard 0 was split into 1 and 2; 1 and 2 were later merged into 3.
    let mut merged = shard(3, Some(1), false, 2);
    merged.adjacent = Some("shardId-000000000002".into());
    let fake = FakeKinesis::start(vec![
        shard(0, None, true, 3),
        shard(1, Some(0), true, 2),
        shard(2, Some(0), true, 2),
        merged,
    ])
    .await;
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let ctx = ctx(config(&fake), StaticSecrets::new());
    let p = source_with_store(&ctx, &fabric, &store).await;
    eventually("nine records", || fabric.len() == 9).await;
    let order: Vec<String> = fabric.events().iter().map(shard_of).collect();
    let last = |s: &str| order.iter().rposition(|x| x == s).unwrap();
    let first = |s: &str| order.iter().position(|x| x == s).unwrap();
    assert!(last("shardId-000000000000") < first("shardId-000000000001"));
    assert!(last("shardId-000000000000") < first("shardId-000000000002"));
    assert!(last("shardId-000000000001") < first("shardId-000000000003"));
    assert!(last("shardId-000000000002") < first("shardId-000000000003"));
    let ev = &fabric.events()[0];
    assert_eq!(ev.ty, "io.loams.dev.flow.kinesis.record.v1");
    assert_eq!(ev.id, format!("orders/shardId-000000000000/{}", seq(0, 0)));
    assert_eq!(ev.source, "/connectors/orders/orders");
    assert_eq!(ev.subject.as_deref(), Some("pk"));
    assert_eq!(ev.time.as_deref(), Some("2025-10-13T00:00:00.500Z"));
    p.stop().await;

    // A restart resumes after the checkpointed sequence numbers: new records
    // on the open shard arrive, nothing is read twice.
    fake.with(|s| {
        let sh = s.shards.iter_mut().find(|x| x.id.ends_with("3")).unwrap();
        sh.records.push((seq(3, 2), b"{\"late\":true}".to_vec()));
        s.expire_once = true;
    });
    let p = source_with_store(&ctx, &fabric, &store).await;
    eventually("the late record", || fabric.len() == 10).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(fabric.duplicates(), 0, "resumed after the checkpoint");
    assert!(
        fake.api
            .ops("GetShardIterator")
            .iter()
            .any(|s| s.json()["ShardIteratorType"] == json!("AFTER_SEQUENCE_NUMBER")),
        "resumed with AFTER_SEQUENCE_NUMBER"
    );
    p.stop().await;

    for (label, base, secrets) in aws_targets("kinesis_resharding_followed") {
        real_reshard(label, base, secrets).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kinesis_put_records_partial_failure() {
    let fake = FakeKinesis::start(vec![shard(0, None, false, 0)]).await;
    fake.with(|s| s.put_fail_first = true);
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 4);
    let ctx = ctx(config(&fake), StaticSecrets::new());
    let p = start_sink(&KinesisFactory::new(), &ctx, &fabric).await;
    eventually("committed", || fabric.committed(GROUP) == 4).await;
    p.stop().await;
    let calls = fake.with(|s| s.put_calls.clone());
    assert_eq!(calls.len(), 2, "one call, then the failed record alone");
    assert_eq!(calls[1]["Records"].as_array().unwrap().len(), 1);
    assert_eq!(calls[0]["StreamName"], json!("orders"));
    let r0 = &calls[0]["Records"][0];
    assert_eq!(r0["PartitionKey"], json!("order-0"));
    let ce: Value =
        serde_json::from_slice(&B64.decode(r0["Data"].as_str().unwrap()).unwrap()).unwrap();
    assert_eq!(ce["id"], json!("e-0"));
    assert_eq!(ce["data"], json!({"i": 0}));
    // Every event is in the stream once, and reading it back passes through.
    let stored = fake.with(|s| s.shards[0].records.len());
    assert_eq!(stored, 4);
    fake.with(|s| s.shards[0].closed = true);
    let out = MemoryFabric::new();
    let p = start_source(&KinesisFactory::new(), &ctx, &out).await;
    eventually("read back", || out.len() == 4).await;
    p.stop().await;
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2", "e-3"]);

    for (label, base, secrets) in aws_targets("kinesis_put_records_partial_failure") {
        real_roundtrip(label, base, secrets).await;
    }
}

async fn create_stream(k: &AwsClient, name: &str, shards: u32) {
    k.call(
        "CreateStream",
        &json!({"StreamName": name, "ShardCount": shards}),
    )
    .await
    .unwrap();
    wait_active(k, name).await;
}

async fn wait_active(k: &AwsClient, name: &str) {
    for _ in 0..120 {
        let d = k
            .call("DescribeStreamSummary", &json!({"StreamName": name}))
            .await
            .unwrap();
        if d["StreamDescriptionSummary"]["StreamStatus"] == json!("ACTIVE") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("stream {name} never became ACTIVE");
}

async fn real_roundtrip(label: &str, base: Value, secrets: Secrets) {
    let k =
        AwsClient::from_context(&ctx(base.clone(), secrets.clone()), AwsService::Kinesis).unwrap();
    let name = run_name("loams-kinesis");
    create_stream(&k, &name, 1).await;
    let mut cfg = base;
    cfg["stream"] = json!(name);
    cfg["poll_interval_ms"] = json!(250);
    let c = ctx(cfg, secrets);
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 5);
    let p = start_sink(&KinesisFactory::new(), &c, &fabric).await;
    eventually(&format!("{label}: put"), || fabric.committed(GROUP) == 5).await;
    p.stop().await;
    let out = MemoryFabric::new();
    let p = start_source(&KinesisFactory::new(), &c, &out).await;
    eventually(&format!("{label}: read"), || out.len() == 5).await;
    p.stop().await;
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2", "e-3", "e-4"], "{label}");
    let _ = k
        .call(
            "DeleteStream",
            &json!({"StreamName": name, "EnforceConsumerDeletion": true}),
        )
        .await;
}

async fn real_reshard(label: &str, base: Value, secrets: Secrets) {
    let k =
        AwsClient::from_context(&ctx(base.clone(), secrets.clone()), AwsService::Kinesis).unwrap();
    let name = run_name("loams-reshard");
    create_stream(&k, &name, 1).await;
    let mut cfg = base;
    cfg["stream"] = json!(name);
    cfg["poll_interval_ms"] = json!(250);
    let c = ctx(cfg, secrets);
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 3);
    let p = start_sink(&KinesisFactory::new(), &c, &fabric).await;
    eventually(&format!("{label}: before"), || fabric.committed(GROUP) == 3).await;
    k.call(
        "UpdateShardCount",
        &json!({"StreamName": name, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"}),
    )
    .await
    .unwrap();
    wait_active(&k, &name).await;
    push_events(&fabric, 3, 3);
    eventually(&format!("{label}: after"), || fabric.committed(GROUP) == 6).await;
    p.stop().await;
    let out = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let p = source_with_store(&c, &out, &store).await;
    eventually(&format!("{label}: all six across the reshard"), || {
        out.len() == 6
    })
    .await;
    p.stop().await;
    let order = out.ids();
    let pos = |id: &str| order.iter().position(|x| x == id).unwrap();
    for before in ["e-0", "e-1", "e-2"] {
        for after in ["e-3", "e-4", "e-5"] {
            assert!(pos(before) < pos(after), "{label}: parent before children");
        }
    }
    let _ = k
        .call(
            "DeleteStream",
            &json!({"StreamName": name, "EnforceConsumerDeletion": true}),
        )
        .await;
}
