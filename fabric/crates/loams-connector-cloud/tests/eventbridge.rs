//! CN2 plan Task 19: Amazon EventBridge. The `PutEvents` sink retries only the
//! entries that failed; the source is a rule targeting an SQS queue that the
//! instance reads (the guide and runbook set the rule up).
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::fake_sqs::FakeSqs;
use common::*;
use loams_connector_cloud::aws::{AwsClient, AwsService};
use loams_connector_cloud::eventbridge::EventBridgeFactory;
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{CloudEvent, StaticSecrets};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventbridge_put_events_partial_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let received: Arc<Mutex<Vec<Value>>> = Arc::default();
    let (c, r) = (calls.clone(), received.clone());
    let api = FakeApi::start(Arc::new(move |seen: &Seen| {
        assert_eq!(seen.op(), "PutEvents");
        assert_eq!(
            seen.header("content-type"),
            Some("application/x-amz-json-1.1")
        );
        let n = c.fetch_add(1, Ordering::SeqCst);
        let entries = seen.json()["Entries"].as_array().cloned().unwrap();
        let mut out = Vec::new();
        let mut failed = 0;
        for (i, e) in entries.iter().enumerate() {
            let detail = e["Detail"].as_str().unwrap();
            if detail.contains("poison") {
                failed += 1;
                out.push(json!({"ErrorCode": "MalformedDetail", "ErrorMessage": "bad"}));
            } else if n == 0 && i == 1 {
                // A transient failure of the second entry, the first time.
                failed += 1;
                out.push(json!({"ErrorCode": "ThrottlingException", "ErrorMessage": "slow down"}));
            } else {
                r.lock().unwrap().push(e.clone());
                out.push(json!({"EventId": format!("eb-{n}-{i}")}));
            }
        }
        json_reply(200, json!({"FailedEntryCount": failed, "Entries": out}))
    }))
    .await;

    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 4);
    fabric.push(
        CloudEvent::new("bad-1", "/test/upstream", "com.example.order.created.v1")
            .with_data("application/json", "{\"poison\":true}"),
    );
    let config = json!({"region": "eu-west-1", "endpoint": api.base, "event_bus": "orders"});
    let p = start_sink(
        &EventBridgeFactory::new(),
        &ctx(config, StaticSecrets::new()),
        &fabric,
    )
    .await;
    eventually("committed", || fabric.committed(GROUP) == 5).await;
    p.stop().await;

    let got = received.lock().unwrap().clone();
    let mut ids: Vec<String> = got
        .iter()
        .map(|e| {
            let d: Value = serde_json::from_str(e["Detail"].as_str().unwrap()).unwrap();
            d["id"].as_str().unwrap().to_owned()
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2", "e-3"], "each event once");
    let e0 = got
        .iter()
        .find(|e| e["Detail"].as_str().unwrap().contains("\"e-0\""))
        .unwrap();
    assert_eq!(e0["EventBusName"], json!("orders"));
    assert_eq!(e0["Source"], json!("/test/upstream"));
    assert_eq!(e0["DetailType"], json!("com.example.order.created.v1"));
    // AWS JSON timestamps are epoch seconds: 2026-10-13T00:00:00Z.
    assert_eq!(e0["Time"], json!(1_791_849_600.0));
    let detail: Value = serde_json::from_str(e0["Detail"].as_str().unwrap()).unwrap();
    assert_eq!(detail["specversion"], json!("1.0"));
    assert_eq!(detail["data"], json!({"i": 0}));
    // The throttled entry alone was retried.
    assert!(calls.load(Ordering::SeqCst) >= 2);
    // The malformed one went to the dead-letter queue with its error.
    let dead = fabric.dead_letters();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].event.as_ref().map(|e| e.id.as_str()), Some("bad-1"));
    assert!(
        dead[0].error.contains("MalformedDetail"),
        "{}",
        dead[0].error
    );

    for (label, base, secrets) in aws_targets("eventbridge_put_events_partial_failure") {
        real_rule_to_queue(label, base, secrets).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventbridge_source_reads_rule_target_queue() {
    let fake = FakeSqs::start().await;
    let plain = json!({
        "version": "0", "id": "eb-1", "detail-type": "OrderCreated",
        "source": "com.example.shop", "account": "123456789012",
        "time": "2026-10-13T00:00:00Z", "region": "eu-west-1", "resources": [],
        "detail": {"order": 42},
    });
    fake.send(&plain.to_string(), Value::Null);
    let wrapped = json!({
        "version": "0", "id": "eb-2", "detail-type": "com.example.order.created.v1",
        "source": "/test/upstream", "account": "123456789012",
        "time": "2026-10-13T00:00:00Z", "region": "eu-west-1", "resources": [],
        "detail": {"specversion": "1.0", "id": "e-9", "source": "/test/upstream",
                   "type": "com.example.order.created.v1", "data": {"i": 9}},
    });
    fake.send(&wrapped.to_string(), Value::Null);
    let fabric = MemoryFabric::new();
    let config = json!({"region": "eu-west-1", "endpoint": fake.api.base,
        "queue_url": fake.queue_url("rule-target"), "wait_time_seconds": 0});
    let p = start_source(
        &EventBridgeFactory::new(),
        &ctx(config, StaticSecrets::new()),
        &fabric,
    )
    .await;
    eventually("both read", || fabric.len() == 2).await;
    eventually("both deleted", || fake.remaining() == 0).await;
    p.stop().await;
    let evs = fabric.events();
    let a = evs.iter().find(|e| e.id == "eb-1").unwrap();
    assert_eq!(a.ty, "io.loams.dev.flow.eventbridge.event.v1");
    assert_eq!(a.subject.as_deref(), Some("OrderCreated"));
    assert_eq!(a.time.as_deref(), Some("2026-10-13T00:00:00Z"));
    let data: Value = serde_json::from_slice(&a.data).unwrap();
    assert_eq!(data["detail"]["order"], json!(42));
    let b = evs
        .iter()
        .find(|e| e.id == "e-9")
        .expect("a CloudEvent passes through");
    assert_eq!(b.ty, "com.example.order.created.v1");
    assert_eq!(b.data.as_ref(), b"{\"i\":9}");
}

/// A rule on a fresh bus sends the sink's events to a queue, which the
/// EventBridge source reads back: identity survives the round trip.
async fn real_rule_to_queue(label: &str, base: Value, secrets: Secrets) {
    let admin = ctx(base.clone(), secrets.clone());
    let events = AwsClient::from_context(&admin, AwsService::EventBridge).unwrap();
    let sqs = AwsClient::from_context(&admin, AwsService::Sqs).unwrap();
    let name = run_name("loams-eb");
    events
        .call("CreateEventBus", &json!({"Name": name}))
        .await
        .unwrap();
    let url = sqs
        .call("CreateQueue", &json!({"QueueName": name}))
        .await
        .unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned();
    let arn = sqs
        .call(
            "GetQueueAttributes",
            &json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
        )
        .await
        .unwrap()["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_owned();
    let rule = events
        .call(
            "PutRule",
            &json!({"Name": name, "EventBusName": name,
                    "EventPattern": json!({"source": ["/test/upstream"]}).to_string()}),
        )
        .await
        .unwrap();
    let policy = json!({"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
        "Principal": {"Service": "events.amazonaws.com"}, "Action": "sqs:SendMessage",
        "Resource": arn, "Condition": {"ArnEquals": {"aws:SourceArn": rule["RuleArn"]}}}]});
    sqs.call(
        "SetQueueAttributes",
        &json!({"QueueUrl": url, "Attributes": {"Policy": policy.to_string()}}),
    )
    .await
    .unwrap();
    events
        .call(
            "PutTargets",
            &json!({"Rule": name, "EventBusName": name, "Targets": [{"Id": "q", "Arn": arn}]}),
        )
        .await
        .unwrap();

    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 3);
    let mut sink_cfg = base.clone();
    sink_cfg["event_bus"] = json!(name);
    let p = start_sink(
        &EventBridgeFactory::new(),
        &ctx(sink_cfg, secrets.clone()),
        &fabric,
    )
    .await;
    eventually(&format!("{label}: put"), || fabric.committed(GROUP) == 3).await;
    p.stop().await;
    let mut src_cfg = base;
    src_cfg["queue_url"] = json!(url);
    src_cfg["wait_time_seconds"] = json!(2);
    let out = MemoryFabric::new();
    let p = start_source(&EventBridgeFactory::new(), &ctx(src_cfg, secrets), &out).await;
    eventually(&format!("{label}: routed"), || out.len() == 3).await;
    p.stop().await;
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2"], "{label}");
    let _ = events
        .call(
            "RemoveTargets",
            &json!({"Rule": name, "EventBusName": name, "Ids": ["q"]}),
        )
        .await;
    let _ = events
        .call("DeleteRule", &json!({"Name": name, "EventBusName": name}))
        .await;
    let _ = events.call("DeleteEventBus", &json!({"Name": name})).await;
    let _ = sqs.call("DeleteQueue", &json!({"QueueUrl": url})).await;
}
