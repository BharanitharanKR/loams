//! `nats` against real NATS with JetStream (compose service `nats`, run with
//! `-js`, profile `connectors-p2`). Skipped unless
//! `LOAMS_CONNECTORS_P2_STACK` is set; `LOAMS_NATS_URL` overrides the
//! endpoint.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
mod common;

use std::time::Duration;

use async_nats::jetstream;
use common::*;
use loams_connector_brokers::nats;
use loams_connector_sdk::EventBatch;
use loams_flow_conformance::{ServiceKind, Suite};
use serde_json::{Value, json};

const SERVICES: &[ServiceKind] = &[ServiceKind::Container {
    compose_service: "nats",
    system: "NATS",
}];

fn url() -> String {
    endpoint("LOAMS_NATS_URL", "nats://127.0.0.1:4222")
}

fn base() -> Value {
    json!({"servers": [url()]})
}

fn with(mut v: Value, k: &str, x: Value) -> Value {
    v[k] = x;
    v
}

/// The streams of a run: `<tag>` holds `<tag>.in` and `<tag>_out` holds
/// `<tag>.out`, each with a duplicate window for the dedup test. They must be
/// two streams: JetStream deduplicates `Nats-Msg-Id` per stream, and the sink
/// republishes each seeded event under its own id, so one stream holding both
/// subjects drops every output message as a duplicate of its input.
async fn streams(tag: &str) -> Result<(), String> {
    let client = async_nats::connect(url())
        .await
        .map_err(|e| e.to_string())?;
    let js = jetstream::new(client);
    for (name, subject) in [
        (tag.to_owned(), format!("{tag}.in")),
        (format!("{tag}_out"), format!("{tag}.out")),
    ] {
        let _ = js.delete_stream(&name).await;
        js.create_stream(jetstream::stream::Config {
            name,
            subjects: vec![subject],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn plan() -> Plan {
    Plan {
        services: SERVICES,
        factory: nats::factory(),
        configs: Box::new(|tag| Configs {
            source: with(
                base(),
                "source",
                json!({"stream": tag, "durable": "loams-src", "filter_subject": format!("{tag}.in"), "poll_wait_ms": 200}),
            ),
            sink: with(base(), "sink", json!({"subject": format!("{tag}.out")})),
            seed: with(base(), "sink", json!({"subject": format!("{tag}.in")})),
            drain: with(
                base(),
                "source",
                json!({"stream": format!("{tag}_out"), "durable": "loams-drain", "filter_subject": format!("{tag}.out"), "poll_wait_ms": 200}),
            ),
        }),
        setup: Box::new(|c: Configs| {
            Box::pin(async move { streams(c.source["source"]["stream"].as_str().unwrap()).await })
        }),
        subscribe_first: false,
        // The compose server has no accounts configured: key, jwt and mtls
        // cannot run against it (R18.10).
        auth: vec![],
        redelivery: Duration::from_millis(300),
    }
}

#[tokio::test]
async fn nats_suites_against_nats() {
    let h = BrokerHarness::new("nats", plan());
    let report = h
        .run(
            nats::ID,
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
    // `auth` (and `contract`, which runs it) stays skipped until the estate
    // configures NATS accounts; every other suite must pass.
    eprintln!("{report}");
    assert!(report.failed().is_empty(), "{report}");
}

#[tokio::test]
async fn nats_jetstream_resumes_and_dedupes_msg_id() {
    if !stack_up(P2, "nats_jetstream_resumes_and_dedupes_msg_id") {
        return;
    }
    let tag = format!("dedup{}", std::process::id());
    streams(&tag).await.unwrap();
    let f = nats::factory();
    let mut sink = open_sink(
        f.as_ref(),
        "out",
        with(base(), "sink", json!({"subject": format!("{tag}.in")})),
    )
    .unwrap();
    let batch = || EventBatch::Events((0..10).map(|i| record(&format!("n{i}"))).collect());
    write_ready(sink.as_mut(), batch()).await.unwrap();
    // A retried batch (same event ids): Nats-Msg-Id makes the stream drop it.
    write_ready(sink.as_mut(), batch()).await.unwrap();
    let client = async_nats::connect(url()).await.unwrap();
    let js = jetstream::new(client);
    let mut st = js.get_stream(&tag).await.unwrap();
    assert_eq!(
        st.info().await.unwrap().state.messages,
        10,
        "duplicates dropped by Nats-Msg-Id"
    );

    let cfg = with(
        base(),
        "source",
        json!({"stream": tag, "durable": "resume", "filter_subject": format!("{tag}.in"), "max_batch": 5, "poll_wait_ms": 1000}),
    );
    let mut first = open_source(f.as_ref(), "src", cfg.clone()).await.unwrap();
    let a = first.next(BUDGET).await.unwrap().expect("first five");
    let first_ids = ids(&a.events);
    assert_eq!(first_ids.len(), 5);
    first.committed(&a.position).await.unwrap();
    first.close().await.unwrap();

    // The durable consumer resumes after the acknowledged five.
    let mut second = open_source(f.as_ref(), "src", cfg).await.unwrap();
    let rest = drain(second.as_mut(), 2).await.unwrap();
    assert_eq!(rest, (5..10).map(|i| format!("n{i}")).collect::<Vec<_>>());
    assert!(rest.iter().all(|id| !first_ids.contains(id)));
}

#[tokio::test]
async fn nats_core_subjects_flow_but_are_not_resumable() {
    if !stack_up(P2, "nats_core_subjects_flow_but_are_not_resumable") {
        return;
    }
    let subject = format!("core{}.in", std::process::id());
    let f = nats::factory();
    let cfg = json!({"servers": [url()], "source": {"mode": "core", "subjects": [subject], "poll_wait_ms": 1000}});
    assert!(!nats::declared(&cfg).unwrap().source_resumable);
    let mut src = open_source(f.as_ref(), "core", cfg).await.unwrap();
    let mut sink = open_sink(
        f.as_ref(),
        "core-out",
        json!({"servers": [url()], "sink": {"mode": "core", "subject": subject}}),
    )
    .unwrap();
    write_ready(
        sink.as_mut(),
        EventBatch::Events((0..3).map(|i| record(&format!("c{i}"))).collect()),
    )
    .await
    .unwrap();
    let got = drain(src.as_mut(), 2).await.unwrap();
    assert_eq!(got, vec!["c0", "c1", "c2"]);
}
