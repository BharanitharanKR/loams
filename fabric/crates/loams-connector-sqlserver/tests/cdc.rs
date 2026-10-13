//! Native SQL Server CDC against real SQL Server 2022 Developer (compose
//! `sqlserver`, `connectors-ci-only`, a `ci-service`). Gated on
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use common::{Recorder, ci_only, eventually, fold, opts, spec};
use loams_connector_sdk::PipelineConfig;
use loams_connector_sqlserver::Target;
use loams_connector_sqlserver::cdc::{CdcPos, LSN_MAX, parse_lsn};
use loams_connector_sqlserver::conn;
use loams_connector_sqlserver::harness::{SqlHarness, has_change};
use loams_connector_sqlserver::types::quote_ident;
use loams_flow_conformance::{ConnectorHarness, KillTarget, RunEnv, Suite, run_with};
use serde_json::{Value, json};

const SETTLE: Duration = Duration::from_secs(180);

fn t(h: &SqlHarness) -> String {
    format!("[dbo].{}", quote_ident(h.table()))
}

async fn source_table(h: &SqlHarness) -> BTreeMap<i64, Option<String>> {
    let mut c = h.admin().await.expect("admin");
    conn::rows(
        &mut c,
        &format!("SELECT id, v FROM {}", t(h)),
        &[],
        "select",
    )
    .await
    .expect("select")
    .iter()
    .map(|r| (r.get::<i64, _>(0).expect("id"), conn::text_at(r, 1)))
    .collect()
}

/// The kit's CDC and kill-restart suites.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_cdc_suites() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::cdc(Target::SqlServer, "suites");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("sql-server"),
        None,
        &h,
        &[Suite::Cdc, Suite::KillRestart],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// A random workload of 10,000 operations; folding what reached the Fabric
/// (snapshot rows, then changes by key) gives the source table (the Fluss
/// `_current` table's stand-in until FL1, R12.8).
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_cdc_converges() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::cdc(Target::SqlServer, "converge");
    h.reset().await.expect("reset");
    h.seed(500).await.expect("seed");
    h.start().await.expect("start");
    // splitmix64, so the workload is reproducible.
    let mut x: u64 = 0x5eed;
    let mut next = move || {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let table = t(&h);
    let mut sql = String::new();
    for i in 0..10_000u64 {
        let id = next() % 2_000;
        match next() % 3 {
            0 => sql.push_str(&format!(
                "MERGE {table} AS t USING (SELECT CAST({id} AS bigint) AS id) AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = N'i{i}' WHEN NOT MATCHED THEN INSERT (id, v) VALUES ({id}, N'i{i}');"
            )),
            1 => sql.push_str(&format!("UPDATE {table} SET v = N'u{i}' WHERE id = {id};")),
            _ => sql.push_str(&format!("DELETE FROM {table} WHERE id = {id};")),
        }
        if i % 250 == 249 {
            h.exec(&sql).await.expect("workload");
            sql.clear();
        }
    }
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(
        ok,
        "the folded change stream never equalled the source table"
    );
}

/// One transaction of 20 updates shares a commit LSN; with batches of three
/// the checkpoint lands inside it, at a `(lsn, seqval, operation)` triple. A
/// kill there and a restart deliver every update, none under a new id, and
/// resume after the triple rather than at the transaction's start.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_cdc_resume_lsn_triple() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::cdc(Target::SqlServer, "triple").with_pipeline(PipelineConfig {
        max_batch_rows: 3,
        linger: Duration::from_millis(20),
        checkpoint_interval: Duration::ZERO,
        ..PipelineConfig::default()
    });
    h.reset().await.expect("reset");
    h.seed(20).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settled");
    h.kill(KillTarget::Task).await.expect("kill");
    let table = t(&h);
    h.exec(&format!(
        "BEGIN TRANSACTION; {} COMMIT",
        (0..20)
            .map(|i| format!("UPDATE {table} SET v = N'tx-{i}' WHERE id = {i};"))
            .collect::<String>()
    ))
    .await
    .expect("transaction");
    h.start().await.expect("restart");
    // Wait for a checkpoint inside the transaction, then kill again.
    let mid = eventually(SETTLE, || async {
        matches!(h.cdc_position().await, Some(CdcPos::Stream { ref seq, .. }) if parse_lsn(seq).ok() != Some(LSN_MAX))
            && h.changes().await.iter().any(|e| e.extensions["loamsop"] == "u")
    })
    .await;
    assert!(mid, "no checkpoint inside the transaction");
    h.kill(KillTarget::Task)
        .await
        .expect("kill mid-transaction");
    let at_kill = h.cdc_position().await.expect("position");
    h.start().await.expect("resume");
    let updated =
        |evs: &[loams_connector_sdk::CloudEvent]| (0..20).all(|i| has_change(evs, "u", i));
    let evs = h.wait_for(SETTLE, updated).await;
    h.stop().await.expect("stop");
    assert!(updated(&evs), "an update of the transaction was lost");
    let mut by_id: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for e in evs.iter().filter(|e| e.extensions["loamsop"] == "u") {
        by_id
            .entry(e.id.clone())
            .or_default()
            .insert(String::from_utf8_lossy(&e.data).into_owned());
    }
    assert_eq!(by_id.len(), 20, "each update has exactly one id: {by_id:?}");
    assert!(
        by_id.values().all(|v| v.len() == 1),
        "a re-read changed content"
    );
    let CdcPos::Stream { lsn, .. } = at_kill else {
        panic!("not streaming at the kill")
    };
    let tx_lsn = evs
        .iter()
        .find(|e| e.extensions["loamsop"] == "u")
        .map(|e| e.extensions["loamslsn"][..20].to_owned());
    assert_eq!(Some(lsn), tx_lsn, "the kill was inside the transaction");
}

/// After `ALTER TABLE … ADD` and a second capture instance, the source
/// emits a `schema-change` with the new column, follows the new instance,
/// and its changes carry the column.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_capture_instance_switch_on_ddl() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::cdc(Target::SqlServer, "ddl");
    h.reset().await.expect("reset");
    h.seed(5).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settled");
    let table = t(&h);
    h.exec(&format!(
        "INSERT INTO {table} (id, v) VALUES (50, N'before-ddl')"
    ))
    .await
    .expect("insert");
    h.exec(&format!("ALTER TABLE {table} ADD note nvarchar(20) NULL"))
        .await
        .expect("alter");
    let v2 = format!("{}_v2", h.capture_instance());
    h.enable_cdc(&v2).await.expect("second capture instance");
    h.exec(&format!(
        "INSERT INTO {table} (id, v, note) VALUES (51, N'after-ddl', N'n')"
    ))
    .await
    .expect("insert");
    let evs = h.wait_for(SETTLE, |e| has_change(e, "c", 51)).await;
    h.stop().await.expect("stop");
    assert!(
        has_change(&evs, "c", 50),
        "the old instance's change was lost"
    );
    let new = evs
        .iter()
        .find(|e| has_change(std::slice::from_ref(e), "c", 51))
        .expect("the change after the DDL");
    assert!(new.id.starts_with(&format!("{v2}/")), "{}", new.id);
    let d: Value = serde_json::from_slice(&new.data).expect("json");
    assert_eq!(d["after"]["note"], json!("n"));
    let schema_changes: Vec<String> = h
        .events()
        .await
        .iter()
        .filter(|e| e.ty.ends_with(".schema-change.v1"))
        .map(|e| String::from_utf8_lossy(&e.data).into_owned())
        .collect();
    assert!(
        schema_changes.iter().any(|s| s.contains("\"note\"")),
        "{schema_changes:?}"
    );
    assert_eq!(h.cdc_position().await.map(|p| p.ci().to_owned()), Some(v2));
}

/// The position's age against the cleanup job's retention is reported as
/// `position_risk`: above zero while changes wait, zero once caught up.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_cdc_retention_metric() {
    if !ci_only() {
        return;
    }
    let rec = Arc::new(Recorder::default());
    let h = SqlHarness::cdc(Target::SqlServer, "retention")
        .with_config(json!({"retention_minutes": 1, "metrics_interval_ms": 200}))
        .with_observer(rec.clone());
    h.reset().await.expect("reset");
    h.seed(5).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settled");
    h.kill(KillTarget::Task).await.expect("kill");
    // Changes wait while the instance is down, and the position ages.
    h.exec(&format!(
        "INSERT INTO {} (id, v) VALUES (500, N'waiting')",
        t(&h)
    ))
    .await
    .expect("insert");
    tokio::time::sleep(Duration::from_secs(6)).await;
    rec.risk.lock().expect("lock").clear();
    h.start().await.expect("restart");
    let caught_up = eventually(SETTLE, || async {
        let r = rec.risk.lock().expect("lock").clone();
        r.iter().any(|x| *x > 0.0) && r.last() == Some(&0.0)
    })
    .await;
    h.stop().await.expect("stop");
    let risk = rec.risk.lock().expect("lock").clone();
    assert!(caught_up, "risk samples: {risk:?}");
    assert!(risk.iter().all(|x| (0.0..1.0).contains(x)), "{risk:?}");
    assert!(!rec.lag.lock().expect("lock").is_empty());
}
