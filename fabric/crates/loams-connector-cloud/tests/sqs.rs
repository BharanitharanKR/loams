//! CN2 plan Task 19: Amazon SQS. Each test runs against the fake SQS (the
//! JSON protocol, in process) and then, when the job provides one, against
//! floci (`connectors-p2`) and real AWS (`connectors-nightly`).
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::fake_sqs::{FakeSqs, string_attributes};
use common::*;
use loams_connector_cloud::aws::{AwsClient, AwsService};
use loams_connector_cloud::sqs::{SqsFactory, fifo_deduplication_id};
use loams_connector_sdk::StaticSecrets;
use loams_connector_sdk::testing::memory::MemoryFabric;
use serde_json::{Value, json};

fn fake_config(fake: &FakeSqs, queue: &str, extra: Value) -> Value {
    let mut c = json!({
        "region": "us-east-1",
        "endpoint": fake.api.base,
        "queue_url": fake.queue_url(queue),
        "wait_time_seconds": 0,
    });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        c[k] = v;
    }
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_delete_after_ack() {
    let fake = FakeSqs::start().await;
    for i in 0..5 {
        fake.send(&format!("{{\"n\":{i}}}"), Value::Null);
    }
    let fabric = MemoryFabric::new();
    fabric.hold();
    let ctx = ctx(
        fake_config(&fake, "orders", json!({})),
        StaticSecrets::new(),
    );
    let p = start_source(&SqsFactory::new(), &ctx, &fabric).await;

    eventually("received", || !fake.api.ops("ReceiveMessage").is_empty()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        fake.api.ops("DeleteMessageBatch").is_empty(),
        "nothing is deleted before the Fabric acknowledges"
    );
    assert_eq!(fake.remaining(), 5);

    fabric.release();
    eventually("all five in the Fabric", || fabric.len() == 5).await;
    eventually("all five deleted", || fake.remaining() == 0).await;
    let ids: BTreeSet<String> = fabric.ids().into_iter().collect();
    let deleted: BTreeSet<String> = fake.deleted().into_iter().collect();
    assert_eq!(ids, deleted, "event ids are the SQS message ids");
    let ev = &fabric.events()[0];
    assert_eq!(ev.ty, "io.loams.dev.flow.aws-sqs.message.v1");
    assert_eq!(ev.source, "/connectors/orders/orders");
    assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
    assert_eq!(ev.time.as_deref(), Some("2025-10-13T00:00:00.000Z"));
    p.stop().await;

    for (label, config, secrets) in aws_targets("sqs_delete_after_ack") {
        real_delete_after_ack(label, config, secrets).await;
    }
}

async fn real_delete_after_ack(label: &str, base: Value, secrets: Secrets) {
    let admin_ctx = ctx(base.clone(), secrets.clone());
    let sqs = AwsClient::from_context(&admin_ctx, AwsService::Sqs).unwrap();
    let name = run_name("loams-sqs-ack");
    let created = sqs
        .call(
            "CreateQueue",
            &json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "5"}}),
        )
        .await
        .unwrap();
    let url = created["QueueUrl"].as_str().unwrap().to_owned();
    for i in 0..5 {
        sqs.call(
            "SendMessage",
            &json!({"QueueUrl": url, "MessageBody": format!("{{\"n\":{i}}}")}),
        )
        .await
        .unwrap();
    }
    let mut config = base;
    config["queue_url"] = json!(url);
    config["wait_time_seconds"] = json!(1);
    config["visibility_timeout_seconds"] = json!(5);
    let fabric = MemoryFabric::new();
    let p = start_source(&SqsFactory::new(), &ctx(config, secrets), &fabric).await;
    eventually(&format!("{label}: five events"), || fabric.len() == 5).await;
    p.stop().await;
    // Past the visibility timeout nothing comes back: all five were deleted.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let again = sqs
        .call(
            "ReceiveMessage",
            &json!({"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 1}),
        )
        .await
        .unwrap();
    assert!(
        again["Messages"].as_array().is_none_or(Vec::is_empty),
        "{label}: deleted after the ack"
    );
    let _ = sqs.call("DeleteQueue", &json!({"QueueUrl": url})).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_visibility_extended_while_in_flight() {
    let fake = FakeSqs::start().await;
    for i in 0..3 {
        fake.send(&format!("{{\"n\":{i}}}"), Value::Null);
    }
    let fabric = MemoryFabric::new();
    fabric.hold();
    let fake_ctx = ctx(
        fake_config(&fake, "slow", json!({"visibility_timeout_seconds": 2})),
        StaticSecrets::new(),
    );
    let p = start_source(&SqsFactory::new(), &fake_ctx, &fabric).await;
    eventually("received", || {
        fake.messages().iter().all(|m| m.receives == 1)
    })
    .await;
    // Hold the Fabric well past the 2 s visibility timeout.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    let extensions = fake.api.ops("ChangeMessageVisibilityBatch");
    assert!(
        extensions.len() >= 2,
        "visibility extended while in flight ({} calls)",
        extensions.len()
    );
    assert!(
        extensions
            .iter()
            .all(|s| s.json()["Entries"][0]["VisibilityTimeout"] == json!(2)),
        "each extension renews the configured timeout"
    );
    assert!(
        fake.messages().iter().all(|m| m.receives == 1),
        "no message became visible again while the Fabric held it"
    );
    fabric.release();
    eventually("deleted", || fake.remaining() == 0).await;
    assert_eq!(fabric.len(), 3);
    let after = fake.api.ops("ChangeMessageVisibilityBatch").len();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        fake.api.ops("ChangeMessageVisibilityBatch").len(),
        after,
        "acknowledged messages are no longer extended"
    );
    p.stop().await;

    for (label, base, secrets) in aws_targets("sqs_visibility_extended_while_in_flight") {
        let sqs =
            AwsClient::from_context(&ctx(base.clone(), secrets.clone()), AwsService::Sqs).unwrap();
        let name = run_name("loams-sqs-vis");
        let url = sqs
            .call("CreateQueue", &json!({"QueueName": name}))
            .await
            .unwrap()["QueueUrl"]
            .as_str()
            .unwrap()
            .to_owned();
        sqs.call("SendMessage", &json!({"QueueUrl": url, "MessageBody": "x"}))
            .await
            .unwrap();
        let mut config = base;
        config["queue_url"] = json!(url);
        config["wait_time_seconds"] = json!(1);
        config["visibility_timeout_seconds"] = json!(2);
        let fabric = MemoryFabric::new();
        fabric.hold();
        let p = start_source(&SqsFactory::new(), &ctx(config, secrets), &fabric).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        fabric.release();
        eventually(&format!("{label}: one event"), || fabric.len() == 1).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(fabric.len(), 1, "{label}: never redelivered");
        assert_eq!(fabric.duplicates(), 0, "{label}: never redelivered");
        p.stop().await;
        let _ = sqs.call("DeleteQueue", &json!({"QueueUrl": url})).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_fifo_dedup_id_is_ce_id() {
    let fake = FakeSqs::start().await;
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 4);
    let fake_ctx = ctx(
        fake_config(&fake, "orders.fifo", json!({})),
        StaticSecrets::new(),
    );
    let p = start_sink(&SqsFactory::new(), &fake_ctx, &fabric).await;
    eventually("four sent", || fake.remaining() == 4).await;
    p.stop().await;

    let sends = fake.api.ops("SendMessageBatch");
    let entries: Vec<Value> = sends
        .iter()
        .flat_map(|s| s.json()["Entries"].as_array().cloned().unwrap())
        .collect();
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e["MessageDeduplicationId"], json!(format!("e-{i}")));
        assert_eq!(e["MessageGroupId"], json!(format!("order-{}", i % 3)));
        let attrs = string_attributes(&e["MessageAttributes"]);
        assert_eq!(attrs["ce_id"], format!("e-{i}"));
        assert_eq!(attrs["ce_type"], "com.example.order.created.v1");
        assert_eq!(attrs["ce_source"], "/test/upstream");
        assert_eq!(e["MessageBody"], json!(format!("{{\"i\":{i}}}")));
    }

    // A retried batch (the same events again) is deduplicated by the queue.
    fabric.set_committed(GROUP, 0);
    let p = start_sink(&SqsFactory::new(), &fake_ctx, &fabric).await;
    eventually("resent", || {
        fake.api.ops("SendMessageBatch").len() > sends.len()
    })
    .await;
    p.stop().await;
    assert_eq!(fake.remaining(), 4, "the FIFO queue kept one copy of each");

    // An id longer than SQS's 128 characters is hashed, deterministically.
    let long = "x".repeat(200);
    let a = fifo_deduplication_id(&long);
    assert_eq!(a, fifo_deduplication_id(&long));
    assert!(a.len() <= 128);
    assert_eq!(fifo_deduplication_id("e-1"), "e-1");

    for (label, base, secrets) in aws_targets("sqs_fifo_dedup_id_is_ce_id") {
        let sqs =
            AwsClient::from_context(&ctx(base.clone(), secrets.clone()), AwsService::Sqs).unwrap();
        let name = format!("{}.fifo", run_name("loams-sqs-fifo"));
        let url = sqs
            .call(
                "CreateQueue",
                &json!({"QueueName": name, "Attributes": {"FifoQueue": "true"}}),
            )
            .await
            .unwrap()["QueueUrl"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut config = base;
        config["queue_url"] = json!(url);
        config["wait_time_seconds"] = json!(1);
        let fabric = MemoryFabric::new();
        push_events(&fabric, 0, 4);
        let sink_ctx = common::ctx(config.clone(), secrets.clone());
        let p = start_sink(&SqsFactory::new(), &sink_ctx, &fabric).await;
        eventually(&format!("{label}: committed"), || {
            fabric.committed(GROUP) == 4
        })
        .await;
        p.stop().await;
        fabric.set_committed(GROUP, 0);
        let p = start_sink(&SqsFactory::new(), &sink_ctx, &fabric).await;
        eventually(&format!("{label}: recommitted"), || {
            fabric.committed(GROUP) == 4
        })
        .await;
        p.stop().await;
        let out = MemoryFabric::new();
        let p = start_source(&SqsFactory::new(), &common::ctx(config, secrets), &out).await;
        eventually(&format!("{label}: read back"), || out.len() == 4).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(out.len(), 4, "{label}: one copy each");
        let ids: BTreeSet<String> = out.ids().into_iter().collect();
        assert_eq!(ids, (0..4).map(|i| format!("e-{i}")).collect());
        p.stop().await;
        let _ = sqs.call("DeleteQueue", &json!({"QueueUrl": url})).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_roundtrip_keeps_cloudevent_identity() {
    let fake = FakeSqs::start().await;
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 6);
    // One event whose data is not UTF-8 travels in structured mode.
    fabric.push(
        loams_connector_sdk::CloudEvent::new("bin-1", "/test/upstream", "com.example.blob.v1")
            .with_data("application/octet-stream", vec![0xff_u8, 0x00, 0x80]),
    );
    let config = fake_config(&fake, "rt", json!({}));
    let ctx = ctx(config, StaticSecrets::new());
    let sink = start_sink(&SqsFactory::new(), &ctx, &fabric).await;
    eventually("seven sent", || fake.remaining() == 7).await;
    sink.stop().await;

    let out = MemoryFabric::new();
    let src = start_source(&SqsFactory::new(), &ctx, &out).await;
    eventually("seven read", || out.len() == 7).await;
    src.stop().await;
    let mut got = out.events();
    got.sort_by(|a, b| a.id.cmp(&b.id));
    let bin = got.iter().find(|e| e.id == "bin-1").unwrap();
    assert_eq!(bin.data.as_ref(), &[0xff, 0x00, 0x80]);
    assert_eq!(bin.ty, "com.example.blob.v1");
    for i in 0..6 {
        let e = got.iter().find(|e| e.id == format!("e-{i}")).unwrap();
        let want = event(i);
        assert_eq!(e.ty, want.ty, "type passes through");
        assert_eq!(e.source, want.source, "source passes through");
        assert_eq!(e.subject, want.subject);
        assert_eq!(e.time, want.time);
        assert_eq!(e.data, want.data);
        assert_eq!(e.extensions.get("tenant").map(String::as_str), Some("acme"));
        assert_eq!(
            e.extensions.get("loamsconnector").map(String::as_str),
            Some("aws-sqs")
        );
    }
}

#[tokio::test]
async fn sqs_config_is_checked() {
    let factory = SqsFactory::new();
    let missing = ctx(json!({"region": "eu-west-1"}), StaticSecrets::new());
    let err = factory
        .source(&missing)
        .err()
        .expect("queue_url is required");
    assert!(err.message.contains("queue_url"), "{}", err.message);
    let unsigned = ctx(
        json!({"region": "eu-west-1", "queue_url": "https://sqs.eu-west-1.amazonaws.com/1/q"}),
        StaticSecrets::new(),
    );
    let err = factory
        .sink(&unsigned)
        .err()
        .expect("credentials are required without an emulator endpoint");
    assert!(err.message.contains("auth"), "{}", err.message);
}

use loams_connector_sdk::ConnectorFactory as _;
