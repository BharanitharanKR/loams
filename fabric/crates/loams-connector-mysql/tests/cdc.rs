//! Native MySQL binlog CDC against real MySQL 8.4 (compose `mysql`,
//! `connectors-p2`). Gated on `LOAMS_CONNECTORS_P2_STACK`; the tests share
//! one server and run one at a time ([`common::serial`]).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{eventually, fold, opts, p2, serial, source_table, spec, workload};
use loams_connector_mysql::Target;
use loams_connector_mysql::gtid::{BinlogPos, GtidSet};
use loams_connector_mysql::harness::MyHarness;
use loams_connector_sdk::{ErrorClass, PipelineState};
use loams_flow_conformance::{ConnectorHarness, RunEnv, Suite, run_with};
use serde_json::{Value, json};

const SETTLE: Duration = Duration::from_secs(120);

/// A random workload of 10,000 operations after a snapshot; folding what
/// reached the Fabric gives the source table (the Fluss `_current` table's
/// stand-in until FL1, as R10.8).
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cdc_converges() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "converge");
    h.reset().await.expect("reset");
    h.seed(500).await.expect("seed");
    h.start().await.expect("start");
    workload(&h, 10_000, 2_000, 0x5eed).await;
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(
        ok,
        "the folded change stream never equalled the source table"
    );
}

/// DBLog: writes that race the snapshot's chunks (updates, deletes and
/// inserts on the rows being read) neither lose nor resurrect a row, and
/// the snapshot finishes without a lock.
#[tokio::test(flavor = "multi_thread")]
async fn dblog_snapshot_interleaves_without_loss() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = Arc::new(MyHarness::cdc(Target::Mysql, "dblog").with_config(json!({"chunk_size": 50})));
    h.reset().await.expect("reset");
    h.seed(3_000).await.expect("seed");
    let writer = {
        let h = h.clone();
        tokio::spawn(async move { workload(&h, 3_000, 3_500, 0xdb10).await })
    };
    h.start().await.expect("start");
    writer.await.expect("writer");
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async {
        fold(&h.delivered().await) == want
            && h.cdc_position().await.is_some_and(|p| p.snapshot.is_none())
    })
    .await;
    h.stop().await.expect("stop");
    assert!(
        ok,
        "the snapshot and the stream did not converge on the table"
    );
    let snapshot_rows: usize = h
        .delivered()
        .await
        .iter()
        .map(|b| match b {
            loams_connector_sdk::EventBatch::Arrow { batch, .. } => batch.num_rows(),
            _ => 0,
        })
        .sum();
    assert!(snapshot_rows > 0, "no snapshot rows were delivered");
}

/// The same snapshot under `FLUSH TABLES WITH READ LOCK`, for an account
/// that cannot write the watermark table.
#[tokio::test(flavor = "multi_thread")]
async fn global_read_lock_snapshot_converges() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "locked")
        .with_config(json!({"snapshot": {"mode": "initial", "locking": "global-read-lock"}}));
    h.reset().await.expect("reset");
    h.seed(1_000).await.expect("seed");
    h.start().await.expect("start");
    workload(&h, 1_000, 1_500, 0x10c4).await;
    let want = source_table(&h).await;
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(ok, "the locked snapshot and the stream did not converge");
}

/// Stopped and started again, the source resumes from its GTID set: what
/// was written while it was down arrives, and nothing is sent twice.
#[tokio::test(flavor = "multi_thread")]
async fn binlog_resume_by_gtid() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "gtid").with_config(json!({"snapshot": "never"}));
    h.reset().await.expect("reset");
    h.start().await.expect("start");
    h.seed(10).await.expect("seed");
    h.settle(SETTLE).await.expect("settle");
    h.stop().await.expect("stop");
    let pos = h.cdc_position().await.expect("a position");
    let BinlogPos::Mysql { executed, .. } = &pos.binlog else {
        panic!("not a GTID position: {:?}", pos.binlog);
    };
    let mut admin = h.admin().await.expect("admin");
    let server = admin
        .query("SELECT @@GLOBAL.gtid_executed")
        .await
        .expect("gtid_executed")
        .scalar()
        .unwrap_or_default();
    assert!(
        GtidSet::parse(&server).expect("set").contains_set(executed),
        "the checkpoint holds GTIDs the server never executed"
    );
    h.seed(10).await.expect("seed while down");
    h.start().await.expect("restart");
    h.settle(SETTLE).await.expect("settle after restart");
    h.stop().await.expect("stop");
    let copies = h.read_back().await.expect("read back");
    assert_eq!(copies.len(), 20);
    assert_eq!(
        h.fabric().await.duplicates(),
        0,
        "a resumed stream re-sent events the Fabric had"
    );
}

/// Without `binlog_row_metadata=FULL` the binlog has no column names: the
/// schema history, stored with the position, maps each event's columns as
/// they were when it was written, across an `ADD COLUMN … AFTER` made while
/// the instance was stopped. The `DROP COLUMN` after it is a change the SDK's
/// policy refuses (`column v was dropped`, under every evolution): the
/// instance pauses on it, after delivering what came before it.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_history_maps_columns_at_position() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "history").with_config(json!({"snapshot": "never"}));
    h.reset().await.expect("reset");
    h.exec("SET GLOBAL binlog_row_metadata = 'MINIMAL'")
        .await
        .expect("minimal metadata");
    let t = h.qualified();
    let run = async {
        h.start().await.expect("start");
        h.exec(&format!("INSERT INTO {t} (id, v) VALUES (1, 'a')"))
            .await
            .expect("insert");
        let first = eventually(SETTLE, || async { h.changes().await.len() == 1 }).await;
        assert!(first, "the first change never arrived");
        h.stop().await.expect("stop");
        h.exec(&format!(
            "INSERT INTO {t} (id, v) VALUES (2, 'b'); \
             ALTER TABLE {t} ADD COLUMN note VARCHAR(20) AFTER id; \
             INSERT INTO {t} (id, note, v) VALUES (3, 'n3', 'c'); \
             ALTER TABLE {t} DROP COLUMN v; \
             INSERT INTO {t} (id, note) VALUES (4, 'n4')"
        ))
        .await
        .expect("changes while stopped");
        h.start().await.expect("restart");
        let paused = eventually(SETTLE, || async {
            h.status()
                .await
                .is_some_and(|st| st.state == PipelineState::Paused)
        })
        .await;
        let status = h.status().await;
        h.stop().await.expect("stop");
        assert!(paused, "the drop did not pause the instance: {status:?}");
        let error = status
            .and_then(|st| st.last_error)
            .expect("the pause's error");
        assert_eq!(error.class, ErrorClass::SchemaIncompatible);
        assert!(
            error.message.contains("column v was dropped"),
            "{}",
            error.message
        );
    };
    run.await;
    h.exec("SET GLOBAL binlog_row_metadata = 'FULL'")
        .await
        .expect("full metadata again");
    let after: Vec<Value> = h
        .changes()
        .await
        .iter()
        .map(|e| serde_json::from_slice::<Value>(&e.data).expect("json")["after"].clone())
        .collect();
    assert_eq!(
        after,
        vec![
            json!({"id": 1, "v": "a"}),
            json!({"id": 2, "v": "b"}),
            json!({"id": 3, "note": "n3", "v": "c"}),
        ]
    );
    let has_note = h.delivered().await.iter().any(|b| match b {
        loams_connector_sdk::EventBatch::Events(v) => v.iter().any(|e| {
            e.ty.ends_with(".schema-change.v1")
                && String::from_utf8_lossy(&e.data).contains("\"note\"")
        }),
        _ => false,
    });
    assert!(has_note, "the added column was not announced");
}

/// Binlogs purged past the checkpoint: the instance stops with
/// `PositionLost` and says to take a new snapshot (Review Focus 6).
#[tokio::test(flavor = "multi_thread")]
async fn binlog_purged_is_fatal_with_resnapshot_hint() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "purged").with_config(json!({"snapshot": "never"}));
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
        .query("SHOW BINARY LOG STATUS")
        .await
        .expect("status")
        .get(0, "File")
        .expect("a binlog file");
    admin
        .exec(&format!("PURGE BINARY LOGS TO '{current}'"))
        .await
        .expect("purge");
    h.start().await.expect("restart");
    let failed = eventually(SETTLE, || async {
        h.status()
            .await
            .is_some_and(|s| s.state == PipelineState::Failed)
    })
    .await;
    assert!(failed, "the instance kept running past a purged position");
    let status = h.status().await.expect("status");
    let last = status.last_error.expect("a last error");
    assert_eq!(last.class, ErrorClass::PositionLost);
    assert!(last.message.contains("new snapshot"), "{}", last.message);
    h.kill(loams_flow_conformance::KillTarget::Task)
        .await
        .expect("clean up");
}

/// The kit's suites on the CDC source.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cdc_suites() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::cdc(Target::Mysql, "suites");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mysql"),
        None,
        &h,
        &[
            Suite::Cdc,
            Suite::KillRestart,
            Suite::DupCheck,
            Suite::Roundtrip,
        ],
    )
    .await;
    assert!(report.green(), "{report}");
}
