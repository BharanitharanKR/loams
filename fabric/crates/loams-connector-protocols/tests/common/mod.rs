//! Shared test helpers: contexts, synthetic fixtures and gates.
#![allow(dead_code, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, EventBatch, InstanceKey, Scrubber, Source, SourceBatch,
    StaticSecrets,
};
use loams_flow_conformance::{Fixture, FixtureProxy, ProxyHandle, ScrubRules};
use serde_json::Value;

/// The repository root (`fabric/crates/<crate>` → three up).
pub fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

/// A fixture under `conformance/connectors/fixtures/`, its scenario, and a
/// replay of it.
pub async fn replay(rel: &str) -> (Fixture, Value, ProxyHandle) {
    let path = repo().join("conformance/connectors/fixtures").join(rel);
    let fixture = Fixture::load(&path).unwrap();
    assert!(
        fixture.synthetic && !fixture.docs.is_empty(),
        "{rel} is a documented synthetic fixture"
    );
    let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let proxy = FixtureProxy::replay(fixture.clone(), ScrubRules::default())
        .await
        .unwrap();
    (fixture, raw["scenario"].clone(), proxy)
}

/// `{proxy}` in every string of `v` replaced by the replay's URL.
pub fn with_proxy(v: &Value, base: &str) -> Value {
    match v {
        Value::String(s) => Value::String(s.replace("{proxy}", base)),
        Value::Array(a) => Value::Array(a.iter().map(|x| with_proxy(x, base)).collect()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| (k.clone(), with_proxy(x, base)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A context for `config` with the scenario's secrets.
pub fn ctx(config: Value, secrets: &Value) -> ConnectorContext {
    let mut s = StaticSecrets::new();
    if let Some(m) = secrets.as_object() {
        for (k, v) in m {
            s = s.with(k.clone(), v.as_str().unwrap().as_bytes().to_vec());
        }
    }
    ConnectorContext::new(
        InstanceKey::new(7, "test"),
        config,
        Arc::new(s),
        Scrubber::new(),
    )
}

pub fn event(id: &str, subject: Option<&str>, data: &Value) -> CloudEvent {
    let mut ev = CloudEvent::new(
        id,
        "/connectors/upstream/items",
        "io.loams.dev.test.item.v1",
    )
    .with_data("application/json", data.to_string());
    if let Some(s) = subject {
        ev = ev.with_subject(s);
    }
    ev
}

pub fn events(b: &SourceBatch) -> Vec<CloudEvent> {
    match &b.events {
        EventBatch::Events(v) => v.clone(),
        EventBatch::Arrow { .. } => panic!("JSON events expected"),
    }
}

pub fn data(ev: &CloudEvent) -> Value {
    serde_json::from_slice(&ev.data).unwrap()
}

pub const BUDGET: Budget = Budget {
    max_rows: 100,
    max_bytes: 8 << 20,
};

/// Drain a source to its end.
pub async fn drain(src: &mut dyn Source) -> Vec<CloudEvent> {
    let mut out = Vec::new();
    while let Some(b) = src.next(BUDGET).await.unwrap() {
        out.extend(events(&b));
    }
    out
}

/// Set when the `connectors-p2` compose stack is up (CI's shards).
pub fn p2_stack() -> bool {
    std::env::var_os("LOAMS_CONNECTORS_P2_STACK").is_some()
}

/// Every named variable set (nightly secrets).
pub fn env_all(names: &[&str]) -> Option<Vec<String>> {
    names
        .iter()
        .map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()))
        .collect()
}

/// Print why a gated test did nothing.
pub fn skip(test: &str, why: &str) {
    eprintln!("{test}: skipped ({why}); a skipped suite never counts toward preview");
}
