//! The exactly-once sink against real PostgreSQL 17 (compose `pgvector`,
//! `connectors-p2`). Gated on `LOAMS_CONNECTORS_P2_STACK`.

mod common;

use common::{opts, p2, spec};
use loams_connector_postgres::Target;
use loams_connector_postgres::harness::PgHarness;
use loams_flow_conformance::{RunEnv, Suite, run_with};

/// A crash at every fault point of the sink (mid-batch, after prepare,
/// after the commit but before the Fabric commit): the target holds every
/// record exactly once, counted by a trigger on every application.
#[tokio::test(flavor = "multi_thread")]
async fn pg_sink_exactly_once_under_crash() {
    if !p2() {
        return;
    }
    let h = PgHarness::sink(Target::Postgres, "eo", true);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("postgresql"),
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
    let h = PgHarness::sink(Target::Postgres, "alo", false);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("postgresql"),
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
