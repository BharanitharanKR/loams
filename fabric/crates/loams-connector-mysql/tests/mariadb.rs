//! The same code against real MariaDB 11.8 LTS (compose `mariadb`,
//! `connectors-p2`): MariaDB's GTIDs (`domain-server-sequence`, resumed by
//! `@slave_connect_state`, R0.7), its GTID events, and its sink. Gated on
//! `LOAMS_CONNECTORS_P2_STACK`; the tests run one at a time.

mod common;

use std::time::Duration;

use common::{eventually, fold, opts, p2, serial, source_table, spec, workload};
use loams_connector_mysql::Target;
use loams_connector_mysql::cdc::CdcPos;
use loams_connector_mysql::gtid::{BinlogPos, GtidList, MariaGtid};
use loams_connector_mysql::harness::MyHarness;
use loams_connector_sdk::{ErrorClass, PipelineState};
use loams_flow_conformance::{ConnectorHarness, KillTarget, RunEnv, Suite, run_with};
use serde_json::json;

const SETTLE: Duration = Duration::from_secs(120);

async fn binlog_pos(h: &MyHarness) -> GtidList {
    let mut admin = h.admin().await.expect("admin");
    let s = admin
        .query("SELECT @@GLOBAL.gtid_binlog_pos")
        .await
        .expect("gtid_binlog_pos")
        .scalar()
        .unwrap_or_default();
    GtidList::parse(&s).expect("a GTID list")
}

#[tokio::test(flavor = "multi_thread")]
async fn mariadb_cdc_converges() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mariadb, "converge");
    h.reset().await.expect("reset");
    h.seed(500).await.expect("seed");
    h.start().await.expect("start");
    workload(&h, 10_000, 2_000, 0x3a1a).await;
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(
        ok,
        "the folded change stream never equalled the source table"
    );
}

/// The checkpoint's position is MariaDB's GTID list, it survives the
/// position's encoding, and it matches the server's `gtid_binlog_pos` once
/// the source has caught up.
#[tokio::test(flavor = "multi_thread")]
async fn mariadb_gtid_position_roundtrip() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mariadb, "position").with_config(json!({"snapshot": "never"}));
    h.reset().await.expect("reset");
    h.start().await.expect("start");
    h.seed(10).await.expect("seed");
    h.settle(SETTLE).await.expect("settle");
    let server = binlog_pos(&h).await;
    let caught_up = eventually(SETTLE, || async {
        matches!(
            h.cdc_position().await.map(|p| p.binlog),
            Some(BinlogPos::Mariadb { state, .. }) if state == server
        )
    })
    .await;
    h.stop().await.expect("stop");
    assert!(
        caught_up,
        "the checkpoint never reached gtid_binlog_pos {server}"
    );
    let pos = h.cdc_position().await.expect("a position");
    let back = CdcPos::from_position(&pos.to_position()).expect("decodes");
    assert_eq!(back, pos);
    let some: Vec<MariaGtid> = server.iter().copied().collect();
    assert!(!some.is_empty(), "MariaDB wrote no GTID");
}

/// R0.7: stopped and started again, the source resumes from the GTID list
/// through `@slave_connect_state` — what was written while it was down
/// arrives, nothing is sent twice.
#[tokio::test(flavor = "multi_thread")]
async fn mariadb_gtid_resume() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mariadb, "resume").with_config(json!({"snapshot": "never"}));
    h.reset().await.expect("reset");
    h.start().await.expect("start");
    h.seed(10).await.expect("seed");
    h.settle(SETTLE).await.expect("settle");
    h.stop().await.expect("stop");
    assert!(matches!(
        h.cdc_position().await.map(|p| p.binlog),
        Some(BinlogPos::Mariadb { state, .. }) if !state.is_empty()
    ));
    h.seed(10).await.expect("seed while down");
    h.start().await.expect("restart");
    h.settle(SETTLE).await.expect("settle after restart");
    h.stop().await.expect("stop");
    let copies = h.read_back().await.expect("read back");
    assert_eq!(copies.len(), 20);
    assert_eq!(h.fabric().await.duplicates(), 0);
}

/// A GTID MariaDB no longer has: the dump fails with 1236, the instance
/// stops with `PositionLost` and a re-snapshot hint.
#[tokio::test(flavor = "multi_thread")]
async fn mariadb_binlog_purged_is_fatal_with_resnapshot_hint() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mariadb, "purged").with_config(json!({"snapshot": "never"}));
    h.reset().await.expect("reset");
    h.start().await.expect("start");
    h.seed(5).await.expect("seed");
    h.settle(SETTLE).await.expect("settle");
    h.stop().await.expect("stop");
    h.exec("FLUSH BINARY LOGS").await.expect("flush");
    h.seed(5).await.expect("written after the checkpoint");
    h.exec("FLUSH BINARY LOGS").await.expect("flush");
    let mut admin = h.admin().await.expect("admin");
    let current = admin
        .query("SHOW MASTER STATUS")
        .await
        .expect("status")
        .get(0, "File")
        .expect("a binlog file");
    // MariaDB keeps a binlog InnoDB may still need for crash recovery
    // (note 1375) until its transactions are checkpointed, so the purge is
    // repeated until it has removed everything before `current`.
    let purged = eventually(SETTLE, || {
        let (h, current) = (&h, &current);
        async move {
            let Ok(mut admin) = h.admin().await else {
                return false;
            };
            if admin
                .exec(&format!("PURGE BINARY LOGS TO '{current}'"))
                .await
                .is_err()
            {
                return false;
            }
            admin
                .query("SHOW BINARY LOGS")
                .await
                .ok()
                .and_then(|r| r.get(0, "Log_name"))
                .is_some_and(|first| first == *current)
        }
    })
    .await;
    assert!(purged, "the binary logs before {current} were never purged");
    h.start().await.expect("restart");
    let failed = eventually(SETTLE, || async {
        h.status()
            .await
            .is_some_and(|s| s.state == PipelineState::Failed)
    })
    .await;
    assert!(failed, "the instance kept running past a purged position");
    let last = h.status().await.expect("status").last_error.expect("error");
    assert_eq!(last.class, ErrorClass::PositionLost);
    assert!(last.message.contains("new snapshot"), "{}", last.message);
    h.kill(KillTarget::Task).await.expect("clean up");
}

#[tokio::test(flavor = "multi_thread")]
async fn mariadb_sink_exactly_once_under_crash() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::sink(Target::Mariadb, "eo", true);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mariadb"),
        None,
        &h,
        &[Suite::ExactlyOnce, Suite::KillRestart, Suite::Roundtrip],
    )
    .await;
    assert!(report.green(), "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn mariadb_cdc_suites() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mariadb, "suites");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mariadb"),
        None,
        &h,
        &[Suite::Cdc, Suite::KillRestart, Suite::Roundtrip],
    )
    .await;
    assert!(report.green(), "{report}");
}
