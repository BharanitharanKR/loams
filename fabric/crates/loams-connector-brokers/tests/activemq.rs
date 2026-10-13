//! `activemq-jms` over AMQP 1.0 against real ActiveMQ Artemis (compose
//! service `artemis`) and ActiveMQ Classic (`activemq`), profile
//! `connectors-p2`. Skipped unless `LOAMS_CONNECTORS_P2_STACK` is set;
//! `LOAMS_ARTEMIS_URL` and `LOAMS_ACTIVEMQ_URL` override the endpoints.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use common::*;
use loams_connector_brokers::amqp10;
use loams_connector_sdk::EventBatch;
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const ARTEMIS: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "artemis",
    system: "ActiveMQ Artemis",
}];
const CLASSIC: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "activemq",
    system: "ActiveMQ Classic",
}];

fn artemis() -> Value {
    json!({
        "url": endpoint("LOAMS_ARTEMIS_URL", "amqp://127.0.0.1:5673"),
        "username": endpoint("LOAMS_ARTEMIS_USER", "loams"),
        "password": "artemis-pw"
    })
}

fn classic() -> Value {
    json!({"url": endpoint("LOAMS_ACTIVEMQ_URL", "amqp://127.0.0.1:5674")})
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

fn plan(services: &'static [ServiceKind], base: fn() -> Value, auth: &'static str) -> Plan {
    Plan {
        services,
        factory: amqp10::activemq_factory(),
        configs: Box::new(move |tag| {
            let (i, o) = (format!("{tag}.in"), format!("{tag}.out"));
            Configs {
                source: with(
                    base(),
                    "source",
                    json!({"addresses": [i], "poll_wait_ms": 200}),
                ),
                sink: with(base(), "sink", json!({"address": o})),
                seed: with(base(), "sink", json!({"address": i})),
                drain: with(
                    base(),
                    "source",
                    json!({"addresses": [o], "poll_wait_ms": 200}),
                ),
            }
        }),
        // Both brokers create a `queue`-capability address on first attach.
        setup: Box::new(|_| Box::pin(async { Ok(()) })),
        subscribe_first: false,
        auth: vec![(auth, json!({}))],
        redelivery: Duration::from_millis(500),
    }
}

const SUITES: &[Suite] = &[
    Suite::Contract,
    Suite::Roundtrip,
    Suite::KillRestart,
    Suite::DupCheck,
    Suite::Backpressure,
    Suite::Auth,
];

#[tokio::test]
async fn activemq_suites_against_artemis() {
    let h = BrokerHarness::new("artemis", plan(ARTEMIS, artemis, "basic"));
    assert_green_or_skipped(&h.run(amqp10::ACTIVEMQ_ID, SUITES).await);
}

#[tokio::test]
async fn activemq_suites_against_classic() {
    // Classic's default broker takes anonymous AMQP connections, so `basic`
    // is not exercised against it (`auth` runs on Artemis).
    let h = BrokerHarness::new("classic", plan(CLASSIC, classic, "none"));
    let report = h
        .run(
            amqp10::ACTIVEMQ_ID,
            &[
                Suite::Roundtrip,
                Suite::KillRestart,
                Suite::DupCheck,
                Suite::Backpressure,
            ],
        )
        .await;
    assert_green_or_skipped(&report);
}

async fn redelivers_on_kill(base: Value, name: &str) {
    let f = amqp10::activemq_factory();
    let q = format!("{name}.kill.{}", std::process::id());
    let mut seed = open_sink(
        f.as_ref(),
        "seed",
        with(base.clone(), "sink", json!({"address": q})),
    )
    .unwrap();
    write_ready(
        seed.as_mut(),
        EventBatch::Events((0..10).map(|i| record(&format!("k{i}"))).collect()),
    )
    .await
    .unwrap();
    let cfg = with(
        base,
        "source",
        json!({"addresses": [q], "poll_wait_ms": 2000}),
    );
    let quick = {
        let mut c = cfg.clone();
        c["source"]["poll_wait_ms"] = json!(300);
        c
    };
    let mut first = open_source(f.as_ref(), "src", cfg.clone()).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 10 {
        let b = first.next(BUDGET).await.unwrap().expect("deliveries");
        got.extend(ids(&b.events));
    }
    // Killed with every delivery unsettled: the broker redelivers them all.
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut second = open_source(f.as_ref(), "src", cfg).await.unwrap();
    let again = drain(second.as_mut(), 2).await.unwrap();
    let mut sorted = again.clone();
    sorted.sort();
    let mut want: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
    want.sort();
    assert_eq!(sorted, want, "every unsettled delivery came back");
    // `drain` accepted them: nothing comes a third time.
    second.close().await.unwrap();
    let mut third = open_source(f.as_ref(), "src", quick).await.unwrap();
    assert!(drain(third.as_mut(), 2).await.unwrap().is_empty());
}

#[tokio::test]
async fn artemis_amqp10_redelivers_on_kill() {
    if !stack_up(P2, "artemis_amqp10_redelivers_on_kill") {
        return;
    }
    redelivers_on_kill(artemis(), "artemis").await;
}

#[tokio::test]
async fn classic_amqp10_redelivers_on_kill() {
    if !stack_up(P2, "classic_amqp10_redelivers_on_kill") {
        return;
    }
    redelivers_on_kill(classic(), "classic").await;
}
