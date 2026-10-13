//! The brokers family's manifests (staged in `manifests/`, R18.1) against
//! the code: each validates with `loams-flow`'s loader (the manifest schema,
//! CN1's and CN2's semantic rules, the secret rule against its config
//! schema), names a module this crate has, claims no more than its factory
//! declares, and every capability it declares has a conformance check. Its
//! example configs pass both the config schema and the factory's parser, so
//! the two cannot drift. No server needed.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
use std::path::PathBuf;
use std::sync::Arc;

use loams_connector_brokers::{amqp10, amqp091, mqtt, nats, pulsar, websocket};
use loams_connector_sdk::{
    ConnectorContext, ConnectorFactory, InstanceKey, Scrubber, StaticSecrets,
};
use loams_flow::ConnectorSpec;
use serde_json::{Value, json};

fn manifests() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("manifests")
}

fn load(id: &str) -> ConnectorSpec {
    let root = manifests();
    loams_flow::load_manifest(&root.join(format!("{id}.yaml")), &root)
        .unwrap_or_else(|e| panic!("{id}: {}", loams_flow::validate::render_errors(&e)))
}

fn doc(spec: &ConnectorSpec) -> Value {
    spec.to_json().expect("json")
}

/// Example configs per manifest: (config, what it should parse as).
fn examples(id: &str) -> Vec<Value> {
    match id {
        "rabbitmq-amqp" => vec![json!({
            "url": "amqps://rabbit.example:5671/%2f", "username": "loams", "password": "rabbit-pw",
            "tls": {"ca_pem": "-----BEGIN CERTIFICATE-----", "cert_pem": "x", "key_pem": "rabbit-key"},
            "source": {"queues": ["orders"], "prefetch": 100},
            "sink": {"exchange": "events", "routing_key": "orders.created", "persistent": true},
            "pipeline": {"max_batch_rows": 500}
        })],
        "activemq-jms" => vec![json!({
            "url": "amqp://artemis.example:5672", "username": "loams", "password": "artemis-pw",
            "source": {"addresses": ["orders"], "credit": 50}, "sink": {"address": "out"}
        })],
        "azure-service-bus" => vec![
            json!({"connection_string": "sb-cs", "source": {"addresses": ["q"]}, "sink": {"address": "q2"}}),
            json!({
                "namespace": "ns.servicebus.windows.net",
                "entra": {"tenant_id": "t", "client_id": "c", "client_secret": "sb-secret"},
                "source": {"addresses": ["topic/Subscriptions/s"]}
            }),
        ],
        "nats" => vec![
            json!({
                "servers": ["nats://nats.example:4222"], "auth": {"nkey_seed": "nats-seed"},
                "source": {"stream": "ORDERS", "durable": "loams"}, "sink": {"subject": "out"}
            }),
            json!({
                "servers": ["tls://nats.example:4222"], "auth": {"credentials": "nats-creds"},
                "source": {"mode": "core", "subjects": ["a.>"]}, "sink": {"mode": "core"}
            }),
        ],
        "mqtt" => vec![json!({
            "url": "mqtts://mqtt.example:8883", "username": "loams", "password": "mqtt-pw",
            "source": {"topics": ["devices/#"], "receive_maximum": 100},
            "sink": {"topic": "out", "retain": false}
        })],
        "pulsar" => vec![json!({
            "url": "pulsar+ssl://pulsar.example:6651", "token": "pulsar-jwt",
            "source": {"topics": ["persistent://public/default/in"], "subscription_type": "shared"},
            "sink": {"topic": "persistent://public/default/out"}
        })],
        "websocket" => vec![json!({
            "url": "wss://feed.example/stream", "auth": {"bearer": "ws-token"},
            "headers": {"X-Client": "loams"}, "source": {"subscribe": "{\"op\":\"sub\"}"},
            "sink": {"frame": "structured"}
        })],
        other => panic!("no examples for {other}"),
    }
}

fn factory(id: &str) -> Arc<dyn ConnectorFactory> {
    loams_connector_brokers::factories()
        .into_iter()
        .find(|f| f.id() == id)
        .unwrap_or_else(|| panic!("no factory for {id}"))
}

fn declared(id: &str, config: &Value) -> loams_connector_brokers::Declared {
    match id {
        "rabbitmq-amqp" => amqp091::declared(),
        "activemq-jms" | "azure-service-bus" => amqp10::declared(),
        "nats" => nats::declared(config).unwrap(),
        "mqtt" => mqtt::declared(),
        "pulsar" => pulsar::declared(),
        "websocket" => websocket::declared(),
        other => panic!("{other}"),
    }
}

#[test]
fn every_manifest_validates_and_names_this_crate() {
    for id in loams_connector_brokers::IDS {
        let spec = load(id);
        assert_eq!(spec.id, *id);
        let d = doc(&spec);
        let reference = d["runtime"]["ref"].as_str().unwrap();
        let module = reference
            .strip_prefix("loams_connector_brokers::")
            .unwrap_or_else(|| panic!("{id}: {reference} is not in this crate"));
        assert!(
            ["amqp091", "amqp10", "nats", "mqtt", "pulsar", "websocket"].contains(&module),
            "{id}: unknown module {module}"
        );
        assert_eq!(d["runtime"]["kind"], "native", "{id}");
        assert_eq!(
            d["envelope"]["emits"],
            format!("io.loams.dev.flow.{id}.message.v1"),
            "{id}: the family emits `message` (Shared contracts, Event names)"
        );
    }
}

#[test]
fn manifests_claim_no_more_than_the_factories_declare() {
    use loams_connector_sdk::Delivery;
    for id in loams_connector_brokers::IDS {
        let spec = load(id);
        let d = doc(&spec);
        // The manifest describes the strongest config (JetStream for NATS).
        let cfg = &examples(id)[0];
        let dec = declared(id, cfg);
        let source =
            Delivery::from_slug(d["capabilities"]["delivery"]["source"].as_str().unwrap()).unwrap();
        let sink =
            Delivery::from_slug(d["capabilities"]["delivery"]["sink"].as_str().unwrap()).unwrap();
        // A source's effectively_once comes from stable ids (OR-7), which
        // the factory's at-least-once reading plus ingest's dedup gives.
        let source_floor = if d["envelope"]["stable_ids"] == true {
            Delivery::EffectivelyOnce
        } else {
            dec.source
        };
        assert!(source <= source_floor, "{id}: source claims {source:?}");
        assert!(
            sink <= dec.sink,
            "{id}: sink claims {sink:?}, factory declares {:?}",
            dec.sink
        );
        assert_eq!(
            d["capabilities"]["source"]["resumable"] == true,
            dec.source_resumable,
            "{id}: resumable"
        );
    }
}

#[test]
fn every_declared_capability_has_a_check() {
    for id in loams_connector_brokers::IDS {
        let spec = load(id);
        let unchecked = loams_flow_conformance::unchecked_capabilities(&spec, None);
        assert!(unchecked.is_empty(), "{id}: unchecked {unchecked:?}");
        let required = loams_flow_conformance::required_suites(&spec, None);
        let declared: std::collections::BTreeSet<_> =
            loams_flow_conformance::Suite::declared_by(&spec)
                .into_iter()
                .collect();
        let missing: Vec<_> = required.difference(&declared).collect();
        assert!(
            missing.is_empty(),
            "{id}: conformance lacks {missing:?} its capabilities need"
        );
    }
}

#[test]
fn example_configs_pass_the_schema_and_the_factory() {
    for id in loams_connector_brokers::IDS {
        let spec = load(id);
        let validator = jsonschema::validator_for(&spec.config_schema).expect("config schema");
        let f = factory(id);
        for cfg in examples(id) {
            let errors: Vec<String> = validator.iter_errors(&cfg).map(|e| e.to_string()).collect();
            assert!(
                errors.is_empty(),
                "{id}: {cfg} fails its schema: {errors:?}"
            );
            let ctx = ConnectorContext::new(
                InstanceKey::new(1, "example"),
                cfg.clone(),
                Arc::new(StaticSecrets::new()),
                Scrubber::new(),
            );
            let has_source = cfg.get("source").is_some();
            let has_sink = cfg.get("sink").is_some();
            assert_eq!(f.source(&ctx).unwrap().is_some(), has_source, "{id}: {cfg}");
            assert_eq!(f.sink(&ctx).unwrap().is_some(), has_sink, "{id}: {cfg}");
        }
        // A misspelt key is refused by both.
        let mut bad = examples(id)[0].clone();
        bad["sourec"] = json!({});
        assert!(
            !validator.is_valid(&bad),
            "{id}: schema accepts a misspelt key"
        );
        let ctx = ConnectorContext::new(
            InstanceKey::new(1, "example"),
            bad,
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        );
        assert!(
            f.source(&ctx).is_err(),
            "{id}: factory accepts a misspelt key"
        );
    }
}

#[test]
fn secrets_are_names_marked_write_only() {
    for id in loams_connector_brokers::IDS {
        let spec = load(id);
        assert!(
            !spec.secrets.is_empty(),
            "{id}: every family member takes a credential"
        );
        for s in &spec.secrets {
            let mut node = &spec.config_schema;
            for part in s.split('.') {
                node = &node["properties"][part];
            }
            assert_eq!(node["writeOnly"], true, "{id}: {s}");
        }
    }
}

#[test]
fn systems_toml_names_a_real_system_for_each() {
    let estate = loams_flow_conformance::Estate::load_default().expect("estate");
    for id in loams_connector_brokers::IDS {
        let entry = estate
            .catalog
            .systems
            .get(*id)
            .unwrap_or_else(|| panic!("{id} has no systems.toml entry"));
        assert_eq!(entry.family, "brokers", "{id}");
        assert!(!entry.services.is_empty(), "{id}");
    }
}
