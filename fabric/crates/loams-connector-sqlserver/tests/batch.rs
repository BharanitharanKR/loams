//! Batch reads against real SQL Server 2025 Developer (compose `sqlserver`,
//! `connectors-ci-only`). Gated on `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use std::time::Duration;

use common::{ci_only, opts, spec};
use loams_connector_sqlserver::Target;
use loams_connector_sqlserver::harness::SqlHarness;
use loams_flow_conformance::{ConnectorHarness, RunEnv, Suite, run_with};
use serde_json::json;

const SETTLE: Duration = Duration::from_secs(120);

/// The kit's kill-restart and dup-check suites over a snapshot-isolated
/// key-ordered read.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_batch_suites() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::batch(Target::SqlServer, "suites");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("sql-server"),
        None,
        &h,
        &[Suite::KillRestart, Suite::DupCheck],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// Four key-range splits read every row exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_key_range_splits_read_every_row_once() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::batch(Target::SqlServer, "ranges").with_config(json!({"partitions": 4}));
    h.reset().await.expect("reset");
    h.seed(1_000).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settled");
    h.stop().await.expect("stop");
    let seen = h.read_back().await.expect("read back");
    assert_eq!(seen.len(), 1_000);
    assert!(seen.iter().all(|o| o.copies == 1), "a row was read twice");
}

/// `cursor_column` with a poll interval: rows added after the first pass
/// are read by the next one, each once.
#[tokio::test(flavor = "multi_thread")]
async fn sqlserver_incremental_cursor_reads_new_rows() {
    if !ci_only() {
        return;
    }
    let h = SqlHarness::batch(Target::SqlServer, "cursor")
        .with_config(json!({"cursor_column": "id", "poll_interval_ms": 200}));
    h.reset().await.expect("reset");
    h.seed(10).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("first pass");
    h.seed(10).await.expect("more rows");
    h.settle(SETTLE).await.expect("second pass");
    h.stop().await.expect("stop");
    let seen = h.read_back().await.expect("read back");
    assert_eq!(seen.len(), 20);
    assert!(seen.iter().all(|o| o.copies == 1), "a row was read twice");
}
