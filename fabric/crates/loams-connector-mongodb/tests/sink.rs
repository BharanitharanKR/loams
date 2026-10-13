//! The sink against real MongoDB 8 (compose `mongodb`, a one-member replica
//! set, and `mongodb-standalone`, both `connectors-ci-only`). Gated on
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use common::{ci_only, opts, spec};
use loams_connector_mongodb::conformance::MongoHarness;
use loams_connector_mongodb::{MongoFactory, Write};
use loams_connector_sdk::Delivery;
use loams_flow_conformance::{RunEnv, Suite, run_with};

/// A crash at every fault point of the sink (mid-batch, after prepare,
/// after the commit but before the Fabric commit): with `write: insert`
/// every application of an event is a new document, and each event is in
/// the collection exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_sink_exactly_once_under_crash() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::sink("eo", Write::Insert, true);
    assert_eq!(
        MongoFactory
            .sink_delivery(&h.context())
            .await
            .expect("delivery"),
        Delivery::ExactlyOnce
    );
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &h,
        &[Suite::ExactlyOnce, Suite::KillRestart, Suite::Roundtrip],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// The control: the same inserts without the transaction must *fail*
/// exactly-once under the same crashes, or the proof above proves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn plain_insert_sink_is_not_exactly_once() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::sink("alo", Write::Insert, false);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
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

/// Upserts by `_id` with deletes, the kit's roundtrip, backpressure and
/// auth (SCRAM-SHA-256 with a user the harness creates).
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_upsert_sink_suites() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::sink("upsert", Write::Upsert, true);
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &h,
        &[
            Suite::Roundtrip,
            Suite::Backpressure,
            Suite::DupCheck,
            Suite::Auth,
        ],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// A standalone server has no transactions: the sink says effectively once
/// for upserts (at least once for inserts), and its upserts still survive a
/// crash without loss or duplicates.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_standalone_narrows_to_effectively_once() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::sink("standalone", Write::Upsert, true).standalone();
    assert_eq!(
        MongoFactory
            .sink_delivery(&h.context())
            .await
            .expect("delivery"),
        Delivery::EffectivelyOnce
    );
    let insert = MongoHarness::sink("standalone_insert", Write::Insert, true).standalone();
    assert_eq!(
        MongoFactory
            .sink_delivery(&insert.context())
            .await
            .expect("delivery"),
        Delivery::AtLeastOnce
    );
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &h,
        &[Suite::Roundtrip, Suite::DupCheck],
    )
    .await;
    assert!(report.green(), "{report}");
}
