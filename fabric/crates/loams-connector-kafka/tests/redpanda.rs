//! Redpanda as a target profile of the Kafka family (CN2 Task 9, OR-2): the
//! same code, against real Redpanda (BSL, a `ci-service`, OR-3), never
//! against Apache Kafka. Skipped without `LOAMS_CONNECTORS_CI_ONLY_STACK`; in
//! CI, `connectors-ci-only` runs:
//!
//! ```text
//! docker compose -f deploy/fabric/compose.yaml --profile connectors-ci-only up -d --wait redpanda
//! LOAMS_CONNECTORS_CI_ONLY_STACK=1 cargo test -p loams-connector-kafka --test redpanda -j 2
//! ```
//!
//! Anything that fails here is narrowed in `connectors/profiles/redpanda.yaml`
//! with a `narrows:` note on the manifest, never skipped.

mod common;

use std::time::Duration;

use common::*;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{ConnectorFactory, InstanceKey};
use serde_json::json;

/// Roundtrip, kill-restart and dup-check through the profile, and the
/// transactional sink's crash cases (Redpanda has transactions on by
/// default, R0.18).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redpanda_suite_against_redpanda() {
    let Some(target) = Target::redpanda() else {
        return;
    };
    assert_eq!(target.factory().id(), "redpanda");
    roundtrip_suite(&target).await;
    recover_reads_checkpoint_case(&target).await;
    exactly_once_under_crash_case(&target).await;
}

/// D270's `ce_` header layout survives Redpanda byte for byte, both ways
/// (spike §5.1 left this unverified; this is the proof): an upstream
/// producer's binary-mode CloudEvent keeps its `id`, `type` and `source`
/// through the source, and the sink's headers are what a consumer reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redpanda_ce_headers_pass_through() {
    let Some(target) = Target::redpanda() else {
        return;
    };
    let (topic, _) = target.topic("redpanda-ce", 1).await;
    let id = unique("ce");
    let headers: Vec<(String, String)> = vec![
        ("ce_specversion".into(), "1.0".into()),
        ("ce_id".into(), id.clone()),
        ("ce_source".into(), "/upstream/billing".into()),
        ("ce_type".into(), "com.example.invoice.paid.v1".into()),
        ("ce_subject".into(), "invoice/42".into()),
        ("ce_time".into(), "2026-10-01T12:00:00Z".into()),
        (
            "ce_traceparent".into(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        ),
        ("content-type".into(), "application/json".into()),
    ];
    produce_raw(
        &target,
        &topic,
        None,
        vec![(
            Some("invoice-42".into()),
            b"{\"paid\":true}".to_vec(),
            headers.clone(),
        )],
    )
    .await;

    // Read through the source: the identity is the producer's.
    let inst = InstanceKey::new(1, unique("ce-src"));
    let ingest = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let src = source_pipeline(
        &target,
        &inst,
        target.config(
            &[&topic],
            json!({ "auto_offset_reset": "earliest", "group_id_prefix": unique("grp") }),
        ),
        &ingest,
        &store,
        Duration::from_secs(3600),
    )
    .await;
    eventually("the event arrives", 60, || ingest.ids().contains(&id)).await;
    src.stop().await;
    let ev = ingest
        .events()
        .into_iter()
        .find(|e| e.id == id)
        .expect("the event");
    assert_eq!(ev.ty, "com.example.invoice.paid.v1");
    assert_eq!(ev.source, "/upstream/billing");
    assert_eq!(ev.subject.as_deref(), Some("invoice/42"));
    assert_eq!(ev.time.as_deref(), Some("2026-10-01T12:00:00Z"));
    assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
    assert_eq!(
        ev.extensions.get("traceparent").map(String::as_str),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(
        ev.extensions.get("partitionkey").map(String::as_str),
        Some("invoice-42")
    );
    assert_eq!(ev.data.as_ref(), b"{\"paid\":true}");

    // Write it back out through the sink: a consumer sees the same headers.
    let (out_topic, _) = target.topic("redpanda-ce-out", 1).await;
    let upstream = MemoryFabric::new();
    upstream.push(ev.clone());
    let sink_inst = InstanceKey::new(1, unique("ce-sink"));
    let sink = sink_pipeline(
        &target,
        &sink_inst,
        target.config(&[&out_topic], json!({})),
        &upstream,
        1,
    )
    .await;
    eventually("the sink writes it", 60, || upstream.committed(GROUP) == 1).await;
    sink.stop().await;
    let records = target.read_all(&out_topic, "read_committed").await;
    assert_eq!(records.len(), 1);
    let m = &records[0];
    for (k, v) in &headers {
        assert_eq!(header(m, k).as_deref(), Some(v.as_str()), "header {k}");
    }
    use rdkafka::Message;
    assert_eq!(m.key(), Some(&b"invoice-42"[..]));
    assert_eq!(m.payload(), Some(&b"{\"paid\":true}"[..]));
}
