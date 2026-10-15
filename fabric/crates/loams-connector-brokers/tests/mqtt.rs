//! `mqtt` against real Eclipse Mosquitto (compose service `mosquitto`,
//! profile `connectors-p2`). Skipped unless `LOAMS_CONNECTORS_P2_STACK` is
//! set; `LOAMS_MQTT_URL` overrides the endpoint. The same scenarios run in
//! the unit tests against an in-process broker (`src/mqtt/tests.rs`), which
//! proves the protocol handling but never counts toward the suites.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use common::*;
use loams_connector_brokers::mqtt;
use loams_connector_sdk::{Budget, EventBatch};
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "mosquitto",
    system: "Eclipse Mosquitto",
}];

fn url() -> String {
    endpoint("LOAMS_MQTT_URL", "mqtt://127.0.0.1:1883")
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

fn base(client: &str) -> Value {
    json!({"url": url(), "client_id": client})
}

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: mqtt::factory(),
        configs: Box::new(|tag| {
            let (i, o) = (format!("{tag}/in"), format!("{tag}/out"));
            Configs {
                source: with(
                    base(&format!("{tag}-src")),
                    "source",
                    json!({"topics": [i], "poll_wait_ms": 200}),
                ),
                sink: with(base(&format!("{tag}-out")), "sink", json!({"topic": o})),
                seed: with(base(&format!("{tag}-seed")), "sink", json!({"topic": i})),
                drain: with(
                    base(&format!("{tag}-drain")),
                    "source",
                    json!({"topics": [o], "poll_wait_ms": 200}),
                ),
            }
        }),
        setup: Box::new(|_| Box::pin(async { Ok(()) })),
        // MQTT keeps nothing for a subscription that does not exist yet.
        subscribe_first: true,
        // The compose broker runs mosquitto-no-auth.conf (R18.10).
        auth: vec![],
        redelivery: Duration::from_millis(300),
    }
}

#[tokio::test]
async fn mqtt_suites_against_mosquitto() {
    let h = BrokerHarness::new("mqtt", plan());
    let report = h
        .run(
            mqtt::ID,
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
    eprintln!("{report}");
    assert!(report.failed().is_empty(), "{report}");
}

#[tokio::test]
async fn mqtt5_qos1_no_loss_on_restart() {
    if !stack_up(P2, "mqtt5_qos1_no_loss_on_restart") {
        return;
    }
    let tag = format!("restart{}", std::process::id());
    let topic = format!("{tag}/in");
    let f = mqtt::factory();
    let cfg = with(
        base(&format!("{tag}-src")),
        "source",
        json!({"topics": [topic], "poll_wait_ms": 1000}),
    );
    // The persistent session exists before anything is published.
    let mut first = open_source(f.as_ref(), "src", cfg.clone()).await.unwrap();
    let mut sink = open_sink(
        f.as_ref(),
        "seed",
        with(
            base(&format!("{tag}-seed")),
            "sink",
            json!({"topic": topic}),
        ),
    )
    .unwrap();
    write_ready(
        sink.as_mut(),
        EventBatch::Events((0..20).map(|i| record(&format!("m{i}"))).collect()),
    )
    .await
    .unwrap();
    let ten = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };
    let mut committed = Vec::new();
    while committed.len() < 10 {
        let b = first.next(ten).await.unwrap().expect("messages");
        committed.extend(ids(&b.events));
        first.committed(&b.position).await.unwrap();
    }
    let mut uncommitted = Vec::new();
    while committed.len() + uncommitted.len() < 20 {
        let b = first.next(ten).await.unwrap().expect("messages");
        uncommitted.extend(ids(&b.events));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Crash: no DISCONNECT, the rest never PUBACKed.
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut second = open_source(f.as_ref(), "src", cfg).await.unwrap();
    let again = drain(second.as_mut(), 2).await.unwrap();
    assert_eq!(
        again, uncommitted,
        "Mosquitto redelivers the unacknowledged QoS 1 messages"
    );
    assert!(again.iter().all(|id| !committed.contains(id)));
    second.close().await.unwrap();
}
