//! DynamoDB and DynamoDB Streams, against floci 2.1.0 in `connectors-p2`
//! (R0.23) or a real AWS account in the nightly job. Without either, every
//! test here skips: nothing is pulled or run locally.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use common::{declared, run_suites, skip};
use loams_connector_kv::config::Config;
use loams_connector_kv::ddbstreams::{AwsStreams, DynamoDbStreamsFactory, ShardPos, StreamsApi};
use loams_connector_kv::dynamodb::{AwsDynamo, DynamoApi, DynamoDbFactory, Item, ScanPage, Write};
use loams_connector_kv::harness::ChainHarness;
use loams_connector_kv::harness::dynamodb::{DynamoDb, DynamoDbStreams, Endpoint, recreate};
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EventBatch, InstanceKey,
    Pipeline, PipelineConfig, Scrubber, SinkHandle,
};
use serde_json::json;

fn live() -> Option<Endpoint> {
    Endpoint::from_env()
}

fn ctx(ep: &Endpoint, extra: serde_json::Value) -> ConnectorContext {
    let mut cfg = ep.config();
    if let (Some(o), serde_json::Value::Object(e)) = (cfg.as_object_mut(), extra) {
        o.extend(e);
    }
    ConnectorContext::new(
        InstanceKey::new(1, format!("ddb-{}", loams_connector_kv::harness::run_tag())),
        cfg,
        Arc::new(ep.secrets()),
        Scrubber::new(),
    )
}

fn item(i: usize) -> Item {
    HashMap::from([
        ("id".to_owned(), AttributeValue::S(format!("k{i:04}"))),
        ("n".to_owned(), AttributeValue::N(i.to_string())),
    ])
}

async fn put_all(c: &AwsDynamo, table: &str, items: Vec<Item>) {
    for chunk in items.chunks(25) {
        let mut w: Vec<Write> = chunk.iter().cloned().map(Write::Put).collect();
        while !w.is_empty() {
            w = c.batch_write(table, w).await.expect("batch write");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_suite_against_floci_or_aws() {
    let h = ChainHarness::new(DynamoDb::new());
    run_suites("dynamodb", &h, &declared("dynamodb"), live().is_some()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_suite_against_floci_or_aws() {
    let h = ChainHarness::new(DynamoDbStreams::new());
    run_suites(
        "dynamodb-streams",
        &h,
        &declared("dynamodb-streams"),
        live().is_some(),
    )
    .await;
}

/// Parallel `Scan` segments are the splits: four segments read the table
/// together, every item exactly once, each segment's position checkpointed
/// to the end.
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_parallel_scan_segments() {
    let Some(ep) = live() else {
        return skip(
            "dynamodb_parallel_scan_segments",
            "no floci and no AWS account",
        );
    };
    let client = ep.client().await.expect("client");
    let table = format!("loams-t-{}-scan", loams_connector_kv::harness::run_tag());
    recreate(&client, &table, false).await.expect("table");
    put_all(&client, &table, (0..120).map(item).collect()).await;
    let c = ctx(
        &ep,
        json!({ "table": table, "segments": 4, "page_size": 7 }),
    );
    let (en, sf) = DynamoDbFactory::new()
        .source(&c)
        .expect("source")
        .expect("some");
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let h = Pipeline::source(c.instance.clone(), PipelineConfig::default())
        .enumerator(en)
        .sources(Arc::from(sf))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .start()
        .await
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let done = store.get(&c.instance).is_some_and(|cp| {
            cp.splits.len() == 4
                && cp
                    .splits
                    .values()
                    .all(|p| String::from_utf8_lossy(&p.opaque).contains("\"done\":true"))
        });
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scan did not finish: {:?}",
            h.status()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    h.stop().await;
    assert_eq!(fabric.len(), 120, "every item exactly once");
    let cp = store.get(&c.instance).expect("checkpoint");
    let splits: Vec<String> = cp.splits.keys().map(|s| s.as_str().to_owned()).collect();
    assert_eq!(
        splits,
        vec![
            "segment-0-of-4",
            "segment-1-of-4",
            "segment-2-of-4",
            "segment-3-of-4"
        ]
    );
    let _ = client
        .client()
        .delete_table()
        .table_name(&table)
        .send()
        .await;
}

/// Leaves the first writes of the first calls unprocessed (without sending
/// them), as DynamoDB does under load, and passes the rest through.
#[derive(Debug)]
struct Unprocessing {
    inner: AwsDynamo,
    leave: Mutex<Vec<usize>>,
    calls: Mutex<u32>,
}

#[async_trait]
impl DynamoApi for Unprocessing {
    async fn scan(
        &self,
        table: &str,
        segment: u32,
        total: u32,
        start: Option<Item>,
        limit: usize,
        consistent: bool,
    ) -> Result<ScanPage, ConnectorError> {
        self.inner
            .scan(table, segment, total, start, limit, consistent)
            .await
    }

    async fn batch_write(
        &self,
        table: &str,
        writes: Vec<Write>,
    ) -> Result<Vec<Write>, ConnectorError> {
        *self.calls.lock().expect("lock") += 1;
        let leave = {
            let mut l = self.leave.lock().expect("lock");
            if l.is_empty() { 0 } else { l.remove(0) }
        };
        let leave = leave.min(writes.len());
        let (kept, send) = writes.split_at(leave);
        let mut rest = self.inner.batch_write(table, send.to_vec()).await?;
        rest.extend(kept.iter().cloned());
        Ok(rest)
    }

    async fn put_if_newer(
        &self,
        table: &str,
        item: Item,
        attr: &str,
        version: &str,
    ) -> Result<bool, ConnectorError> {
        self.inner.put_if_newer(table, item, attr, version).await
    }

    async fn key_schema(&self, table: &str) -> Result<Vec<String>, ConnectorError> {
        self.inner.key_schema(table).await
    }
}

/// `UnprocessedItems` are retried until the service has every write.
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_unprocessed_items_retried() {
    let Some(ep) = live() else {
        return skip(
            "dynamodb_unprocessed_items_retried",
            "no floci and no AWS account",
        );
    };
    let client = ep.client().await.expect("client");
    let table = format!("loams-t-{}-unproc", loams_connector_kv::harness::run_tag());
    recreate(&client, &table, false).await.expect("table");
    let api = Arc::new(Unprocessing {
        inner: client.clone(),
        leave: Mutex::new(vec![20, 12, 5]),
        calls: Mutex::new(0),
    });
    let c = ctx(&ep, json!({ "table": table }));
    let SinkHandle::Plain(mut sink) = DynamoDbFactory::with_api(api.clone())
        .sink(&c)
        .expect("sink")
        .expect("some")
        .create()
        .expect("create")
    else {
        panic!("a plain sink");
    };
    let events: Vec<CloudEvent> = (0..25)
        .map(|i| {
            CloudEvent::new(format!("e{i}"), "/s", "t")
                .with_data("application/json", format!(r#"{{"id":"k{i:04}","n":{i}}}"#))
        })
        .collect();
    sink.write(EventBatch::Events(events)).await.expect("write");
    assert!(
        *api.calls.lock().expect("lock") >= 4,
        "the unprocessed writes were sent again"
    );
    let mut ids = Vec::new();
    let mut start = None;
    loop {
        let page = client
            .scan(&table, 0, 1, start, 100, true)
            .await
            .expect("scan");
        ids.extend(page.items.iter().filter_map(|i| match i.get("id") {
            Some(AttributeValue::S(s)) => Some(s.clone()),
            _ => None,
        }));
        match page.last_evaluated_key {
            Some(k) => start = Some(k),
            None => break,
        }
    }
    assert_eq!(ids.len(), 25, "every write landed");
    let _ = client
        .client()
        .delete_table()
        .table_name(&table)
        .send()
        .await;
}

async fn read_stream(
    ep: &Endpoint,
    table: &str,
    want: usize,
) -> (MemoryFabric, MemoryCheckpointStore, ConnectorContext) {
    let c = ctx(ep, json!({ "table": table, "poll_interval_ms": 100 }));
    let (en, sf) = DynamoDbStreamsFactory::new()
        .source(&c)
        .expect("source")
        .expect("some");
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let h = Pipeline::source(c.instance.clone(), PipelineConfig::default())
        .enumerator(en)
        .sources(Arc::from(sf))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .discover_every(Duration::from_millis(500))
        .start()
        .await
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(90);
    while fabric.len() < want && Instant::now() < deadline {
        assert!(h.status().last_error.is_none(), "{:?}", h.status());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    h.stop().await;
    (fabric, store, c)
}

/// Shards are released in lineage order: every shard the source reads has
/// no parent, a parent the stream no longer lists, or a parent read to its
/// end; and every change written arrives. (A shard split cannot be forced on
/// floci or AWS; the in-memory test `dynamodb_streams_lineage_followed_in_memory`
/// drives one.)
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_lineage_followed() {
    let Some(ep) = live() else {
        return skip(
            "dynamodb_streams_lineage_followed",
            "no floci and no AWS account",
        );
    };
    let client = ep.client().await.expect("client");
    let table = format!("loams-t-{}-lineage", loams_connector_kv::harness::run_tag());
    recreate(&client, &table, true).await.expect("table");
    put_all(&client, &table, (0..30).map(item).collect()).await;
    let (fabric, store, c) = read_stream(&ep, &table, 30).await;
    assert_eq!(fabric.len(), 30, "every change arrives");
    let cp = store.get(&c.instance).expect("checkpoint");
    let positions: BTreeMap<String, ShardPos> = cp
        .splits
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_owned(), ShardPos::from_position(v)?)))
        .collect();
    assert!(!positions.is_empty());
    let api = AwsStreams::connect(&c, &Config::of(&c))
        .await
        .expect("streams client");
    let arn = api.stream_of(&table).await.expect("stream");
    let shards = api.shards(&arn).await.expect("shards");
    let listed: Vec<&str> = shards.iter().map(|s| s.id.as_str()).collect();
    for s in &shards {
        if !positions.contains_key(&s.id) {
            continue;
        }
        if let Some(parent) = &s.parent
            && listed.contains(&parent.as_str())
        {
            assert!(
                positions.get(parent).is_some_and(|p| p.ended),
                "shard {} was read before its parent {parent} ended",
                s.id
            );
        }
    }
    let _ = client
        .client()
        .delete_table()
        .table_name(&table)
        .send()
        .await;
}

/// INSERT, MODIFY and REMOVE arrive as `loamsop` `c`, `u` and `d`.
#[tokio::test(flavor = "multi_thread")]
async fn dynamodb_streams_ops_map_to_loamsop() {
    let Some(ep) = live() else {
        return skip(
            "dynamodb_streams_ops_map_to_loamsop",
            "no floci and no AWS account",
        );
    };
    let client = ep.client().await.expect("client");
    let table = format!("loams-t-{}-ops", loams_connector_kv::harness::run_tag());
    recreate(&client, &table, true).await.expect("table");
    put_all(&client, &table, vec![item(1)]).await;
    let mut changed = item(1);
    changed.insert("n".into(), AttributeValue::N("99".into()));
    put_all(&client, &table, vec![changed]).await;
    client
        .batch_write(
            &table,
            vec![Write::Delete(HashMap::from([(
                "id".to_owned(),
                AttributeValue::S("k0001".into()),
            )]))],
        )
        .await
        .expect("delete");
    let (fabric, _, _) = read_stream(&ep, &table, 3).await;
    let ops: Vec<String> = fabric
        .events()
        .iter()
        .filter_map(|e| e.extensions.get("loamsop").cloned())
        .collect();
    assert_eq!(ops, vec!["c", "u", "d"]);
    let _ = client
        .client()
        .delete_table()
        .table_name(&table)
        .send()
        .await;
}
