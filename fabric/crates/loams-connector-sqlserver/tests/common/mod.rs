//! Helpers for the family's tests against real systems. Each test is gated
//! on the stack its system runs in (Global Constraints, "Tests skip without
//! services"): `LOAMS_CONNECTORS_CI_ONLY_STACK` (SQL Server Developer, a
//! `ci-service`, OR-3) or the nightly account's `LOAMS_AZURE_SQL_URL`. A
//! skipped test never counts toward `preview`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use loams_connector_sdk::{CloudEvent, ConnectorObserver, EventBatch, InstanceKey, SplitId};
use loams_flow::manifest::repository_root;
use loams_flow::{ConnectorSpec, Registry};
use loams_flow_conformance::RunOptions;
use serde_json::Value;

pub fn gate(vars: &[&str]) -> bool {
    let missing: Vec<&str> = vars
        .iter()
        .copied()
        .filter(|v| std::env::var(v).map_or(true, |x| x.is_empty()))
        .collect();
    if missing.is_empty() {
        true
    } else {
        eprintln!(
            "skipped: {} not set (the system is not up)",
            missing.join(", ")
        );
        false
    }
}

pub fn ci_only() -> bool {
    gate(&["LOAMS_CONNECTORS_CI_ONLY_STACK"])
}

pub fn azure() -> bool {
    gate(&["LOAMS_AZURE_SQL_URL"])
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
        bulk_rows: 10_000,
        smoke_bulk_rows: 1_000,
        settle: Duration::from_secs(120),
        fault: Duration::from_secs(30),
        throttle: Duration::from_millis(5),
    }
}

/// Fold what reached the Fabric into the table it describes: snapshot rows
/// (Arrow, `id` and `v`) and `change` events applied in order, keyed by
/// `id` — the `_current` table a route would build (R12.8).
pub fn fold(batches: &[EventBatch]) -> BTreeMap<i64, Option<String>> {
    let mut t = BTreeMap::new();
    for b in batches {
        match b {
            EventBatch::Arrow { batch, .. } => {
                let ids = batch
                    .column_by_name("id")
                    .expect("id")
                    .as_primitive::<Int64Type>();
                let vs = batch.column_by_name("v").expect("v").as_string::<i32>();
                for i in 0..batch.num_rows() {
                    let v = (!vs.is_null(i)).then(|| vs.value(i).to_owned());
                    t.insert(ids.value(i), v);
                }
            }
            EventBatch::Events(evs) => {
                for e in evs {
                    apply(&mut t, e);
                }
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
    let id = d["key"]["id"]
        .as_i64()
        .or_else(|| d["after"]["id"].as_i64())
        .or_else(|| d["before"]["id"].as_i64());
    match (op, id) {
        (Some("d"), Some(id)) => {
            t.remove(&id);
        }
        (Some("c" | "u" | "r"), Some(id)) => {
            let v = d["after"]["v"].as_str().map(str::to_owned);
            t.insert(id, v);
        }
        _ => {}
    }
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

/// Records what a connector reports through `ConnectorContext::observe`.
#[derive(Debug, Default)]
pub struct Recorder {
    pub risk: Mutex<Vec<f64>>,
    pub lag: Mutex<Vec<Duration>>,
}

impl ConnectorObserver for Recorder {
    fn lag(&self, _i: &InstanceKey, _s: Option<&SplitId>, lag: Duration) {
        self.lag.lock().expect("lock").push(lag);
    }

    fn position_risk(&self, _i: &InstanceKey, _s: Option<&SplitId>, ratio: f64) {
        self.risk.lock().expect("lock").push(ratio);
    }
}
