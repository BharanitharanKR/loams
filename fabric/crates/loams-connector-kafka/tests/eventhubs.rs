//! Azure Event Hubs through its Kafka endpoint, as a target profile of the
//! Kafka family (CN2 Task 9, OR-22).
//!
//! `eventhubs_kafka_endpoint_suite` runs against the Event Hubs emulator
//! (`ci-service`) when `LOAMS_CONNECTORS_CI_ONLY_STACK` is set, and against a
//! real namespace in the nightly job when `LOAMS_EVENTHUBS_NAMESPACE` and
//! `LOAMS_EVENTHUBS_CONNECTION_STRING` are. In CI, `connectors-ci-only` runs:
//!
//! ```text
//! docker compose -f deploy/fabric/compose.yaml --profile connectors-ci-only up -d eventhubs
//! # (the emulator has no health check: CI waits for its port, then for it to log "Emulator Service is Successfully Up")
//! LOAMS_CONNECTORS_CI_ONLY_STACK=1 cargo test -p loams-connector-kafka --test eventhubs -j 2
//! ```
//!
//! `eventhubs_narrows_are_declared` needs no service and always runs.

mod common;

use common::*;
use loams_connector_kafka::Kafka;
use loams_connector_sdk::{ConnectorFactory, Delivery, InstanceKey};
use serde_json::{Value, json};

/// The source and the idempotent sink through the Kafka endpoint: roundtrip,
/// kill-restart and dup-check, on the emulator's configured hub and
/// consumer group. If Event Hubs refuses `enable.idempotence` (unverified,
/// R0.19), this fails, and the sink narrows to `at_least_once`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventhubs_kafka_endpoint_suite() {
    let Some(target) = Target::eventhubs() else {
        return;
    };
    assert_eq!(target.factory().id(), "azure-event-hubs");
    roundtrip_suite(&target).await;
}

/// Event Hubs has no Kafka transactions (R0.19), and says so everywhere a
/// claim could be made: the profile narrows `sink.transactional`, the factory
/// refuses `delivery: exactly_once` under it, the strongest sink delivery is
/// `effectively_once`, and the manifest neither declares a transactional
/// sink nor `exactly_once` (and, once it names the profile, carries the
/// `narrows:` note R7.3 requires).
#[test]
fn eventhubs_narrows_are_declared() {
    let profile = load_profile("azure-event-hubs");
    assert_eq!(profile.module, "kafka");
    assert!(
        profile.narrows.iter().any(|n| n == "sink.transactional"),
        "the profile narrows sink.transactional: {:?}",
        profile.narrows
    );
    // The Kafka endpoint's settings come from the profile, not the user.
    assert_eq!(profile.defaults["security_protocol"], "sasl_ssl");
    assert_eq!(profile.defaults["sasl"]["mechanism"], "plain");
    assert_eq!(profile.defaults["sasl"]["username"], "$ConnectionString");

    assert_eq!(
        Kafka::sink_delivery(Some(&profile)),
        Delivery::EffectivelyOnce
    );
    assert_eq!(Kafka::sink_delivery(None), Delivery::ExactlyOnce);
    assert_eq!(
        Kafka::sink_delivery(Some(&load_profile("redpanda"))),
        Delivery::ExactlyOnce,
        "Redpanda keeps transactions (R0.18)"
    );

    let factory = Kafka::for_profile(&profile).expect("a kafka profile");
    assert_eq!(factory.id(), "azure-event-hubs");
    let ctx_for = |extra: Value| {
        let mut cfg = json!({
            "brokers": "ns.servicebus.windows.net:9093",
            "topics": ["hub"],
            "sasl": { "password": "eh-connection-string" },
        });
        merge(&mut cfg, &extra);
        let mut ctx = loams_connector_sdk::ConnectorContext::new(
            InstanceKey::new(1, "eh"),
            cfg,
            std::sync::Arc::new(loams_connector_sdk::StaticSecrets::new()),
            loams_connector_sdk::Scrubber::new(),
        );
        ctx.profile = Some(profile.clone());
        ctx
    };
    let Err(refused) = factory.sink(&ctx_for(json!({ "delivery": "exactly_once" }))) else {
        panic!("exactly_once is refused under the profile");
    };
    assert!(
        refused.message.contains("narrows sink.transactional"),
        "{refused}"
    );
    assert!(factory.sink(&ctx_for(json!({}))).expect("sink").is_some());
    assert!(
        factory
            .source(&ctx_for(json!({})))
            .expect("source")
            .is_some()
    );

    // The manifest.
    let path = repo_root().join("connectors/registry/azure-event-hubs.yaml");
    let manifest: Value =
        serde_norway::from_str(&std::fs::read_to_string(path).expect("manifest")).expect("yaml");
    let caps = &manifest["capabilities"];
    assert_ne!(
        caps["sink"]["transactional"],
        Value::Bool(true),
        "Event Hubs declares no transactional sink"
    );
    assert_ne!(caps["delivery"]["sink"], "exactly_once");
    if manifest["runtime"]["profile"] == "azure-event-hubs" {
        let notes: Vec<&str> = manifest["notes"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        for n in &profile.narrows {
            assert!(
                notes
                    .iter()
                    .any(|note| note.starts_with(&format!("narrows: {n}"))),
                "the manifest explains narrows {n} with a note"
            );
        }
    }
}
