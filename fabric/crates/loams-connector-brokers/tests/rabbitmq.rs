//! `rabbitmq-amqp` against real RabbitMQ (compose service `rabbitmq`,
//! profile `connectors-p2`). Skipped unless `LOAMS_CONNECTORS_P2_STACK` is
//! set; `LOAMS_RABBITMQ_URL` overrides the endpoint.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use common::*;
use lapin::options::{QueueDeclareOptions, QueuePurgeOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{Connection, ConnectionProperties};
use loams_connector_brokers::amqp091;
use loams_connector_sdk::{Budget, ErrorClass, EventBatch};
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "rabbitmq",
    system: "RabbitMQ",
}];

fn url() -> String {
    endpoint("LOAMS_RABBITMQ_URL", "amqp://127.0.0.1:5672/%2f")
}

fn base() -> Value {
    json!({"url": url(), "username": endpoint("LOAMS_RABBITMQ_USER", "loams"), "password": "rabbit-pw"})
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

/// A management connection for declaring and inspecting queues.
async fn admin() -> Connection {
    let user = endpoint("LOAMS_RABBITMQ_USER", "loams");
    let pass = endpoint("LOAMS_RABBITMQ_PASSWORD", "loams-test");
    let u = url().replacen("amqp://", &format!("amqp://{user}:{pass}@"), 1);
    Connection::connect(&u, ConnectionProperties::default())
        .await
        .expect("admin connection")
}

/// Declare (durable) and purge `q`; returns its ready message count after.
async fn fresh_queue(q: &str) {
    let c = admin().await;
    let ch = c.create_channel().await.unwrap();
    ch.queue_declare(
        ShortString::from(q),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_purge(ShortString::from(q), QueuePurgeOptions::default())
        .await
        .unwrap();
}

async fn ready(q: &str) -> u32 {
    let c = admin().await;
    let ch = c.create_channel().await.unwrap();
    ch.queue_declare(
        ShortString::from(q),
        QueueDeclareOptions {
            passive: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap()
    .message_count()
}

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: amqp091::factory(),
        configs: Box::new(|tag| {
            let (i, o) = (format!("{tag}-in"), format!("{tag}-out"));
            Configs {
                source: with(
                    base(),
                    "source",
                    json!({"queues": [i], "poll_wait_ms": 100}),
                ),
                sink: with(base(), "sink", json!({"routing_key": o})),
                seed: with(base(), "sink", json!({"routing_key": i})),
                drain: with(
                    base(),
                    "source",
                    json!({"queues": [o], "poll_wait_ms": 100}),
                ),
            }
        }),
        setup: Box::new(|c: Configs| {
            Box::pin(async move {
                for q in [
                    &c.source["source"]["queues"][0],
                    &c.drain["source"]["queues"][0],
                ] {
                    fresh_queue(q.as_str().unwrap()).await;
                }
                Ok(())
            })
        }),
        subscribe_first: false,
        auth: vec![("basic", json!({}))],
        redelivery: Duration::from_millis(500),
    }
}

#[tokio::test]
async fn rabbitmq_suites_against_rabbitmq() {
    let h = BrokerHarness::new("rabbit", plan());
    let report = h
        .run(
            amqp091::ID,
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
async fn amqp091_ack_after_fabric() {
    if !stack_up(P2, "amqp091_ack_after_fabric") {
        return;
    }
    let q = format!("ack-after-{}", std::process::id());
    fresh_queue(&q).await;
    let f = amqp091::factory();
    let mut seed = open_sink(
        f.as_ref(),
        "seed",
        with(base(), "sink", json!({"routing_key": q})),
    )
    .unwrap();
    write_ready(
        seed.as_mut(),
        EventBatch::Events((0..5).map(|i| record(&format!("a{i}"))).collect()),
    )
    .await
    .unwrap();
    let src_cfg = with(
        base(),
        "source",
        json!({"queues": [q], "poll_wait_ms": 1000}),
    );

    // Read without committing, then die: the broker requeues all five.
    let mut s = open_source(f.as_ref(), "src", src_cfg.clone())
        .await
        .unwrap();
    let b = s.next(BUDGET).await.unwrap().expect("a batch");
    assert_eq!(ids(&b.events).len(), 5);
    assert_eq!(ready(&q).await, 0, "delivered, unacknowledged");
    drop(s);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        ready(&q).await,
        5,
        "nothing was acked before the Fabric acknowledged"
    );

    // Read and commit (the Fabric acknowledged): gone for good.
    let mut s = open_source(f.as_ref(), "src", src_cfg).await.unwrap();
    let b = s.next(BUDGET).await.unwrap().expect("redelivered");
    assert_eq!(
        ids(&b.events),
        (0..5).map(|i| format!("a{i}")).collect::<Vec<_>>()
    );
    s.committed(&b.position).await.unwrap();
    s.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ready(&q).await, 0);
}

#[tokio::test]
async fn amqp091_publisher_confirms() {
    if !stack_up(P2, "amqp091_publisher_confirms") {
        return;
    }
    let q = format!("confirms-{}", std::process::id());
    fresh_queue(&q).await;
    let f = amqp091::factory();
    let mut sink = open_sink(
        f.as_ref(),
        "out",
        with(base(), "sink", json!({"routing_key": q})),
    )
    .unwrap();
    let ack = sink
        .write(EventBatch::Events(
            (0..20).map(|i| record(&format!("c{i}"))).collect(),
        ))
        .await
        .unwrap();
    // `write` returns only after every message was confirmed: all 20 are in
    // the queue already.
    assert_eq!(ack.rows, 20);
    assert_eq!(ready(&q).await, 20);
    // A mandatory message no queue takes comes back unroutable: poison.
    let mut lost = open_sink(
        f.as_ref(),
        "out",
        with(
            base(),
            "sink",
            json!({"routing_key": format!("{q}-nobody-binds-this")}),
        ),
    )
    .unwrap();
    let e = lost
        .write(EventBatch::Events(vec![record("x")]))
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Poison, "{e}");
}

#[tokio::test]
async fn prefetch_is_backpressure_budget_on_rabbitmq() {
    if !stack_up(P2, "prefetch_is_backpressure_budget_on_rabbitmq") {
        return;
    }
    let q = format!("prefetch-{}", std::process::id());
    fresh_queue(&q).await;
    let f = amqp091::factory();
    let mut seed = open_sink(
        f.as_ref(),
        "seed",
        with(base(), "sink", json!({"routing_key": q})),
    )
    .unwrap();
    write_ready(
        seed.as_mut(),
        EventBatch::Events((0..50).map(|i| record(&format!("p{i}"))).collect()),
    )
    .await
    .unwrap();
    // A budget of one batch of 10: the prefetch is 10, so the broker holds
    // the other 40 back until those are acknowledged.
    let cfg = json!({
        "url": url(), "username": endpoint("LOAMS_RABBITMQ_USER", "loams"), "password": "rabbit-pw",
        "source": {"queues": [q], "max_batch": 10, "poll_wait_ms": 1000},
        "pipeline": {"max_in_flight_batches": 1}
    });
    assert_eq!(amqp091::prefetch(&cfg).unwrap(), 10);
    let mut s = open_source(f.as_ref(), "src", cfg).await.unwrap();
    let wide = Budget {
        max_rows: 1000,
        max_bytes: 8 << 20,
    };
    let b = s.next(wide).await.unwrap().expect("a batch");
    assert!(ids(&b.events).len() <= 10);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(ready(&q).await, 40, "the broker stopped at the prefetch");
    assert!(
        s.next(wide).await.unwrap().is_none(),
        "nothing more until an ack"
    );
    s.committed(&b.position).await.unwrap();
    assert!(
        s.next(wide).await.unwrap().is_some(),
        "acks reopen the window"
    );
}
