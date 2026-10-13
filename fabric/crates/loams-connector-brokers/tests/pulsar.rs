//! `pulsar` against real Apache Pulsar (compose service `pulsar`,
//! `bin/pulsar standalone`, profile `connectors-p2`). Skipped unless
//! `LOAMS_CONNECTORS_P2_STACK` is set; `LOAMS_PULSAR_URL` overrides the
//! endpoint.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use loams_connector_brokers::pulsar;
use loams_connector_sdk::{CloudEvent, EventBatch};
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "pulsar",
    system: "Apache Pulsar",
}];

fn url() -> String {
    endpoint("LOAMS_PULSAR_URL", "pulsar://127.0.0.1:6650")
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

fn topic(t: &str) -> String {
    format!("persistent://public/default/{t}")
}

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: pulsar::factory(),
        configs: Box::new(|tag| {
            let (i, o) = (topic(&format!("{tag}-in")), topic(&format!("{tag}-out")));
            let base = json!({"url": url()});
            Configs {
                source: with(
                    base.clone(),
                    "source",
                    json!({"topics": [i], "subscription": "loams-src", "poll_wait_ms": 200}),
                ),
                sink: with(base.clone(), "sink", json!({"topic": o})),
                seed: with(base.clone(), "sink", json!({"topic": i})),
                drain: with(
                    base,
                    "source",
                    json!({"topics": [o], "subscription": "loams-drain", "poll_wait_ms": 200}),
                ),
            }
        }),
        // Standalone auto-creates topics; subscriptions start at earliest.
        setup: Box::new(|_| Box::pin(async { Ok(()) })),
        subscribe_first: false,
        // Standalone runs without authentication (R18.10).
        auth: vec![],
        redelivery: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn pulsar_suites_against_pulsar() {
    let h = BrokerHarness::new("pulsar", plan());
    let report = h
        .run(
            pulsar::ID,
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
async fn pulsar_ce_properties_pass_through_on_pulsar() {
    if !stack_up(P2, "pulsar_ce_properties_pass_through_on_pulsar") {
        return;
    }
    let t = topic(&format!("ce{}", std::process::id()));
    let f = pulsar::factory();
    let upstream = CloudEvent::new("billing-77", "/billing", "com.example.invoice.paid.v1")
        .with_extension("tenant", "acme")
        .with_data("application/json", Bytes::from_static(b"{\"amount\":3}"));
    let mut sink = open_sink(
        f.as_ref(),
        "out",
        json!({"url": url(), "sink": {"topic": t}}),
    )
    .unwrap();
    write_ready(sink.as_mut(), EventBatch::Events(vec![upstream.clone()]))
        .await
        .unwrap();
    let mut src = open_source(
        f.as_ref(),
        "src",
        json!({"url": url(), "source": {"topics": [t], "poll_wait_ms": 2000}}),
    )
    .await
    .unwrap();
    let b = src.next(BUDGET).await.unwrap().expect("the event");
    let EventBatch::Events(v) = b.events else {
        panic!("events")
    };
    assert_eq!(v[0].id, upstream.id);
    assert_eq!(v[0].ty, upstream.ty);
    assert_eq!(v[0].source, upstream.source);
    assert_eq!(v[0].extensions["tenant"], "acme");
    assert_eq!(v[0].datacontenttype.as_deref(), Some("application/json"));
}

#[tokio::test]
async fn pulsar_producer_dedup_on_pulsar() {
    // R18.7: the sink is at least once. What must hold on a real cluster is
    // the other half: a restarted sink (a new producer) is never mistaken
    // for a repeat, so nothing it writes is dropped.
    if !stack_up(P2, "pulsar_producer_dedup_on_pulsar") {
        return;
    }
    let t = topic(&format!("dedup{}", std::process::id()));
    let f = pulsar::factory();
    for round in 0..2 {
        let mut sink = open_sink(
            f.as_ref(),
            "out",
            json!({"url": url(), "sink": {"topic": t}}),
        )
        .unwrap();
        write_ready(
            sink.as_mut(),
            EventBatch::Events((0..5).map(|i| record(&format!("r{round}-{i}"))).collect()),
        )
        .await
        .unwrap();
    }
    let mut src = open_source(
        f.as_ref(),
        "src",
        json!({"url": url(), "source": {"topics": [t], "poll_wait_ms": 2000}}),
    )
    .await
    .unwrap();
    let got = drain(src.as_mut(), 2).await.unwrap();
    assert_eq!(got.len(), 10, "{got:?}");
}
