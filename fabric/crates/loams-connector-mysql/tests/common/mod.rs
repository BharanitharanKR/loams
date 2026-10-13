//! Helpers for the family's tests against real systems. Each test is gated
//! on the stack its system runs in (Global Constraints, "Tests skip without
//! services"): `LOAMS_CONNECTORS_P2_STACK` (MySQL 8.4 and MariaDB 11.8 in
//! `connectors-p2`), or the nightly account's variables (Aurora). A skipped
//! test never counts toward `preview`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use loams_connector_mysql::conn::MyConn;
use loams_connector_mysql::harness::MyHarness;
use loams_connector_sdk::{CloudEvent, EventBatch};
use loams_flow::manifest::repository_root;
use loams_flow::{ConnectorSpec, Registry};
use loams_flow_conformance::RunOptions;
use serde_json::Value;
use tokio::sync::{Mutex, MutexGuard};

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

pub fn p2() -> bool {
    gate(&["LOAMS_CONNECTORS_P2_STACK"])
}

/// Tests of one binary that share a server run one at a time: one of them
/// purges binary logs, and the others' positions must not be in them.
pub async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::const_new(());
    LOCK.lock().await
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
        settle: Duration::from_secs(90),
        fault: Duration::from_secs(30),
        throttle: Duration::from_millis(5),
    }
}

/// Fold what reached the Fabric into the table it describes: snapshot rows
/// (Arrow, `id` and `v`) and `change` events applied in order, keyed by
/// `id` — the `_current` table a route would build (as R10.8).
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

/// The harness table as `id → v`.
pub async fn source_table(h: &MyHarness) -> BTreeMap<i64, Option<String>> {
    let mut admin: MyConn = h.admin().await.expect("admin");
    let rs = admin
        .query(&format!("SELECT id, v FROM {}", h.qualified()))
        .await
        .expect("select");
    (0..rs.rows.len())
        .map(|i| {
            (
                rs.get(i, "id").and_then(|x| x.parse().ok()).expect("id"),
                rs.get(i, "v"),
            )
        })
        .collect()
}

/// A random workload of `n` upserts, updates and deletes over ids below
/// `ids` (splitmix64, so it is reproducible).
pub async fn workload(h: &MyHarness, n: u64, ids: u64, seed: u64) {
    let mut x: u64 = seed;
    let mut next = move || {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let t = h.qualified();
    let mut sql = String::new();
    for i in 0..n {
        let id = next() % ids;
        match next() % 3 {
            0 => sql.push_str(&format!(
                "INSERT INTO {t} (id, v) VALUES ({id}, 'i{i}') ON DUPLICATE KEY UPDATE v = VALUES(v);"
            )),
            1 => sql.push_str(&format!("UPDATE {t} SET v = 'u{i}' WHERE id = {id};")),
            _ => sql.push_str(&format!("DELETE FROM {t} WHERE id = {id};")),
        }
        if i % 250 == 249 || i + 1 == n {
            h.exec(&sql).await.expect("workload");
            sql.clear();
        }
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
