//! `azure-service-bus` against the Service Bus emulator (compose service
//! `servicebus` with its SQL Server sidecar, profile `connectors-ci-only`:
//! OR-3, CI only). Skipped unless `LOAMS_CONNECTORS_CI_ONLY_STACK` is set.
//! `LOAMS_SERVICEBUS_CONNECTION_STRING` points it at a real namespace
//! instead (the nightly job), with the same entity names.
//!
//! The emulator's entities are fixed by `deploy/fabric/emulators/servicebus.config.json`:
//! the queue `loams-conformance` is the input, and the topic
//! `loams-conformance-topic` with its subscription `flow-conformance` is the
//! output. Each run empties both first, and the tests take turns on them.
//!
//! A peek-lock outlives the receiver that took it: a killed consumer's
//! messages come back only when their lock expires (`LockDuration`, 10 s in
//! the emulator's config). So "empty" means quiet for longer than a lock,
//! and redelivery is awaited for longer than one.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use common::*;
use loams_connector_brokers::amqp10;
use loams_connector_sdk::EventBatch;
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const SERVICES: &[ServiceKind] = &[ServiceKind::Emulator {
    compose_service: "servicebus",
    system: "Azure Service Bus",
    vendor: "Microsoft",
}];

/// The entities' `LockDuration` (the emulator's config); a real namespace
/// for the nightly sets the same.
const LOCK: Duration = Duration::from_secs(10);
/// Empty 500 ms polls in a row that make an entity "empty": 1.5 × LOCK.
const QUIET_POLLS: u32 = 30;

const QUEUE: &str = "loams-conformance";
const TOPIC: &str = "loams-conformance-topic";
const SUBSCRIPTION: &str = "loams-conformance-topic/Subscriptions/flow-conformance";

/// The tests share the emulator's one queue (its entities are fixed), so
/// they take turns: run together, each drains the other's messages.
static QUEUE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn base() -> Value {
    json!({"connection_string": "sb-cs"})
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

async fn empty(address: &str) -> Result<(), String> {
    let f = amqp10::service_bus_factory();
    let mut s = open_source(
        f.as_ref(),
        "sb-empty",
        with(
            base(),
            "source",
            json!({"addresses": [address], "poll_wait_ms": 500}),
        ),
    )
    .await
    .map_err(|e| e.to_string())?;
    // Quiet for longer than LOCK: a message a killed consumer still holds
    // comes back within it, and must not leak into the next run.
    drain(s.as_mut(), QUIET_POLLS)
        .await
        .map_err(|e| e.to_string())?;
    let _ = s.close().await;
    Ok(())
}

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: amqp10::service_bus_factory(),
        configs: Box::new(|_tag| Configs {
            source: with(
                base(),
                "source",
                json!({"addresses": [QUEUE], "poll_wait_ms": 300}),
            ),
            sink: with(base(), "sink", json!({"address": TOPIC})),
            seed: with(base(), "sink", json!({"address": QUEUE})),
            drain: with(
                base(),
                "source",
                json!({"addresses": [SUBSCRIPTION], "poll_wait_ms": 300}),
            ),
        }),
        setup: Box::new(|_| {
            Box::pin(async {
                empty(QUEUE).await?;
                empty(SUBSCRIPTION).await
            })
        }),
        subscribe_first: false,
        auth: vec![("cs", json!({}))],
        redelivery: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn servicebus_suites_against_the_emulator() {
    let _turn = QUEUE_LOCK.lock().await;
    let h = BrokerHarness::new("servicebus", plan());
    let report = h
        .run(
            amqp10::SERVICE_BUS_ID,
            &[
                Suite::Contract,
                Suite::Roundtrip,
                Suite::KillRestart,
                Suite::DupCheck,
                Suite::Backpressure,
                Suite::Auth,
            ],
        )
        .await;
    assert_green_or_skipped(&report);
}

#[tokio::test]
async fn servicebus_peek_lock_redelivery() {
    if !stack_up(CI_ONLY, "servicebus_peek_lock_redelivery") {
        return;
    }
    let _turn = QUEUE_LOCK.lock().await;
    empty(QUEUE).await.unwrap();
    let f = amqp10::service_bus_factory();
    let mut seed = open_sink(
        f.as_ref(),
        "seed",
        with(base(), "sink", json!({"address": QUEUE})),
    )
    .unwrap();
    write_ready(
        seed.as_mut(),
        EventBatch::Events((0..5).map(|i| record(&format!("sb{i}"))).collect()),
    )
    .await
    .unwrap();
    let cfg = with(
        base(),
        "source",
        json!({"addresses": [QUEUE], "poll_wait_ms": 3000}),
    );
    // Peek-locked, never completed: the connection drops with the messages
    // still locked.
    let mut first = open_source(f.as_ref(), "src", cfg.clone()).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 5 {
        let b = first.next(BUDGET).await.unwrap().expect("messages");
        got.extend(ids(&b.events));
    }
    drop(first);
    // The locks expire (LOCK) and Service Bus redelivers every message.
    let mut second = open_source(f.as_ref(), "src", cfg).await.unwrap();
    let deadline = tokio::time::Instant::now() + LOCK * 6;
    let mut again = Vec::new();
    while again.len() < 5 && tokio::time::Instant::now() < deadline {
        again.extend(drain(second.as_mut(), 2).await.unwrap());
    }
    again.sort();
    assert_eq!(again, (0..5).map(|i| format!("sb{i}")).collect::<Vec<_>>());
    second.close().await.unwrap();
    // Completed after the (simulated) Fabric ack: the queue is empty now.
    let mut third = open_source(
        f.as_ref(),
        "src",
        with(
            base(),
            "source",
            json!({"addresses": [QUEUE], "poll_wait_ms": 500}),
        ),
    )
    .await
    .unwrap();
    assert!(drain(third.as_mut(), 2).await.unwrap().is_empty());
}
