//! Helpers for the tests against real MongoDB 8. Each test is gated on the
//! stack MongoDB runs in (Global Constraints, "Tests skip without
//! services"): `LOAMS_CONNECTORS_CI_ONLY_STACK`, because MongoDB is SSPL and
//! runs only as a CI `ci-service` (OR-3). A skipped test never counts toward
//! `preview`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use loams_connector_mongodb::bson::{Bson, Document};
use loams_connector_sdk::{CloudEvent, EventBatch};
use loams_flow::manifest::repository_root;
use loams_flow::{ConnectorSpec, Registry};
use loams_flow_conformance::RunOptions;
use serde_json::Value;

pub fn ci_only() -> bool {
    let set = std::env::var("LOAMS_CONNECTORS_CI_ONLY_STACK").is_ok_and(|v| !v.is_empty());
    if !set {
        eprintln!("skipped: LOAMS_CONNECTORS_CI_ONLY_STACK not set (MongoDB is not up)");
    }
    set
}

pub fn spec(id: &str) -> ConnectorSpec {
    Registry::load(&repository_root().join("connectors/registry"))
        .expect("the registry loads")
        .get(id)
        .unwrap_or_else(|| panic!("manifest {id}"))
        .clone()
}

pub fn opts() -> RunOptions {
    RunOptions {
        records: 200,
        smoke_records: 20,
        bulk_rows: 100_000,
        smoke_bulk_rows: 2_000,
        settle: Duration::from_secs(90),
        fault: Duration::from_secs(30),
        throttle: Duration::from_millis(5),
    }
}

/// Fold what reached the Fabric into the collection it describes: change
/// events applied in order, keyed by `_id` — the `_current` table a route
/// would build (FL1's Fluss is not built; R0.3).
pub fn fold(batches: &[EventBatch]) -> BTreeMap<i64, Option<String>> {
    let mut t = BTreeMap::new();
    for b in batches {
        if let EventBatch::Events(evs) = b {
            for e in evs {
                apply(&mut t, e);
            }
        }
    }
    t
}

fn apply(t: &mut BTreeMap<i64, Option<String>>, e: &CloudEvent) {
    if !e.ty.ends_with(".change.v1") {
        return;
    }
    let d: Value = serde_json::from_slice(&e.data).expect("json");
    let op = e.extensions.get("loamsop").map(String::as_str);
    let id = d["key"]["_id"]
        .as_i64()
        .or_else(|| d["after"]["_id"].as_i64());
    match (op, id) {
        (Some("d"), Some(id)) => {
            t.remove(&id);
        }
        (Some("c" | "u" | "r"), Some(id)) => {
            t.insert(id, d["after"]["v"].as_str().map(str::to_owned));
        }
        _ => {}
    }
}

/// A collection's documents as `_id` → `v`.
pub fn table(docs: &[Document]) -> BTreeMap<i64, Option<String>> {
    docs.iter()
        .filter_map(|d| {
            let id = d.get("_id").and_then(Bson::as_i64)?;
            Some((id, d.get_str("v").map(str::to_owned)))
        })
        .collect()
}

/// Retry `f` until it holds or `timeout` passes.
pub async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
