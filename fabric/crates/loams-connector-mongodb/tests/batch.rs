//! The batch source against a real MongoDB 8 replica set (compose
//! `mongodb`, `connectors-ci-only`). Gated on
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::{ci_only, opts, spec};
use loams_connector_mongodb::MongoFactory;
use loams_connector_mongodb::conformance::MongoHarness;
use loams_connector_sdk::{ConnectorFactory, EventBatch};
use loams_flow_conformance::{ConnectorHarness, RunEnv, Suite, run_with};

/// A collection larger than `split_rows` is read in `_id`-range splits,
/// all at one cluster time, and every document arrives once.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_batch_reads_id_range_splits_at_one_cluster_time() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::batch("ranges")
        .with_config(serde_json::json!({"split_rows": 1000, "max_splits": 4}));
    h.reset().await.expect("reset");
    let seeded = h.seed(5_000).await.expect("seed");
    let (mut enumerator, _) = MongoFactory
        .source(&h.context())
        .expect("source")
        .expect("a source");
    let splits = enumerator.discover(&[]).await.expect("discover");
    assert_eq!(splits.len(), 4, "{splits:?}");
    let times: BTreeSet<String> = splits
        .iter()
        .map(|s| s.spec["at"].as_str().expect("at").to_owned())
        .collect();
    assert_eq!(times.len(), 1, "the splits read at {times:?}");
    h.start().await.expect("start");
    h.settle(Duration::from_secs(90)).await.expect("settle");
    h.stop().await.expect("stop");
    let copies = h.read_back().await.expect("read back");
    assert_eq!(copies.len(), seeded.ids.len());
    assert!(copies.iter().all(|o| o.copies == 1), "{copies:?}");
    // Documents travel as Arrow batches typed `document`.
    let arrow = h
        .delivered()
        .await
        .into_iter()
        .filter_map(|b| match b {
            EventBatch::Arrow { envelope, .. } => Some(envelope.ty),
            EventBatch::Events(_) => None,
        })
        .collect::<Vec<_>>();
    assert!(!arrow.is_empty());
    assert!(
        arrow
            .iter()
            .all(|t| t == "io.loams.dev.flow.mongodb.document.v1")
    );
}

/// The kit's roundtrip, kill-restart and bulk suites on the batch path.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_batch_suites() {
    if !ci_only() {
        return;
    }
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &MongoHarness::batch("suites"),
        &[Suite::Roundtrip, Suite::KillRestart, Suite::Bulk],
    )
    .await;
    assert!(report.green(), "{report}");
}
