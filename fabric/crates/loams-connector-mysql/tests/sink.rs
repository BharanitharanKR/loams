//! The exactly-once sink against real MySQL 8.4 (compose `mysql`,
//! `connectors-p2`), and the ★ batch source's suites. Gated on
//! `LOAMS_CONNECTORS_P2_STACK`.

mod common;

use common::{opts, p2, serial, spec};
use loams_connector_mysql::Target;
use loams_connector_mysql::harness::MyHarness;
use loams_flow_conformance::{RunEnv, Suite, run_with};

/// A crash at every fault point of the sink (mid-batch, after prepare,
/// after the commit but before the Fabric commit): the target holds every
/// record exactly once, counted by a trigger on every application.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_sink_exactly_once_under_crash() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::sink(Target::Mysql, "eo", true);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mysql"),
        None,
        &h,
        &[Suite::ExactlyOnce, Suite::KillRestart, Suite::Roundtrip],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// The control: the plain (at-least-once) sink must *fail* exactly-once
/// under the same crashes, or the trigger proves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn plain_sink_is_not_exactly_once() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::sink(Target::Mysql, "alo", false);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mysql"),
        None,
        &h,
        &[Suite::ExactlyOnce],
    )
    .await;
    assert!(
        !report.green(),
        "an at-least-once sink passed exactly-once: {report}"
    );
}

/// The ★ batch source: the manifest's source suites (roundtrip,
/// kill-restart, dup-check, bulk on the Arrow path). `contract` exercises
/// the sink's capabilities too, which this harness's role does not drive;
/// the sink's are the tests above.
#[tokio::test(flavor = "multi_thread")]
async fn mysql_batch_suites() {
    if !p2() {
        return;
    }
    let _g = serial().await;
    let h = MyHarness::batch(Target::Mysql, "batch");
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mysql"),
        None,
        &h,
        &[
            Suite::Roundtrip,
            Suite::KillRestart,
            Suite::DupCheck,
            Suite::Bulk,
        ],
    )
    .await;
    assert!(report.green(), "{report}");
}
