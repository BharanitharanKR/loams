//! Change-stream CDC against a real MongoDB 8 replica set (compose
//! `mongodb`, `connectors-ci-only`). Gated on
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ci_only, eventually, fold, opts, spec, table};
use loams_connector_mongodb::bson::Bson;
use loams_connector_mongodb::cdc::CdcPos;
use loams_connector_mongodb::conformance::{COLLECTION, MongoHarness};
use loams_connector_mongodb::doc;
use loams_flow_conformance::{ConnectorHarness, KillTarget, RunEnv, Suite, run_with};

const SETTLE: Duration = Duration::from_secs(90);

/// A random workload of 10,000 operations; folding what reached the Fabric
/// gives the collection (the Fluss `_current` table's stand-in until FL1).
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_cdc_converges() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::cdc("converge");
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
    let mut updates = Vec::new();
    let mut deletes = Vec::new();
    for i in 0..10_000u64 {
        let id = (next() % 2_000) as i64;
        match next() % 3 {
            0 => updates.push(Bson::from(doc! {
                "q" => doc! { "_id" => id },
                "u" => doc! { "_id" => id, "v" => format!("i{i}") },
                "upsert" => true,
            })),
            1 => updates.push(Bson::from(doc! {
                "q" => doc! { "_id" => id },
                "u" => doc! { "$set" => doc! { "v" => format!("u{i}") } },
            })),
            _ => deletes.push(Bson::from(
                doc! { "q" => doc! { "_id" => id }, "limit" => 1 },
            )),
        }
        // Flush in order so the collection's final state is the workload's.
        if i % 250 == 249 {
            if !updates.is_empty() {
                h.command(doc! { "update" => COLLECTION, "updates" => std::mem::take(&mut updates), "ordered" => true })
                    .await
                    .expect("updates");
            }
            if !deletes.is_empty() {
                h.command(doc! { "delete" => COLLECTION, "deletes" => std::mem::take(&mut deletes), "ordered" => true })
                    .await
                    .expect("deletes");
            }
        }
    }
    let want = table(&h.documents().await.expect("documents"));
    let ok = eventually(SETTLE, || async { fold(&h.delivered().await) == want }).await;
    h.stop().await.expect("stop");
    assert!(ok, "the folded change stream never equalled the collection");
}

/// Documents written while the snapshot runs arrive exactly once: through
/// the snapshot if committed at or before its cluster time, through the
/// stream if after.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_snapshot_stream_handoff_no_gap() {
    if !ci_only() {
        return;
    }
    let h = Arc::new(MongoHarness::cdc("handoff"));
    h.reset().await.expect("reset");
    h.seed(5_000).await.expect("seed");
    let writer = {
        let h = h.clone();
        tokio::spawn(async move {
            for i in 0..200i64 {
                h.insert(vec![doc! { "_id" => 10_000 + i, "v" => "w" }])
                    .await
                    .expect("insert");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    h.start().await.expect("start");
    writer.await.expect("writer");
    let want = h.documents().await.expect("documents").len();
    let ok = eventually(SETTLE, || async {
        h.read_back().await.expect("read back").len() == want
    })
    .await;
    h.stop().await.expect("stop");
    assert!(ok, "not every document arrived");
    let copies = h.read_back().await.expect("read back");
    let over: Vec<_> = copies.iter().filter(|o| o.copies != 1).collect();
    assert!(
        over.is_empty(),
        "documents delivered more than once: {over:?}"
    );
    assert!(matches!(
        h.cdc_position().await,
        Some(CdcPos::Stream { .. })
    ));
}

/// A restart resumes from the checkpointed resume token: changes made while
/// the source was down arrive, and nothing delivered before the stop is
/// delivered again.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_resume_token_restart() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::cdc("resume");
    h.reset().await.expect("reset");
    h.seed(10).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.insert(vec![doc! { "_id" => 100i64, "v" => "before-stop" }])
        .await
        .expect("insert");
    assert!(
        !h.wait_changes(SETTLE, |e| MongoHarness::has(e, "c", 100))
            .await
            .is_empty()
    );
    h.stop().await.expect("stop");
    let Some(CdcPos::Stream { token: Some(_), .. }) = h.cdc_position().await else {
        panic!("the checkpoint holds no resume token");
    };
    let before = h.fabric().await.len();
    h.insert(vec![doc! { "_id" => 200i64, "v" => "while-down" }])
        .await
        .expect("insert");
    h.start().await.expect("restart");
    let evs = h
        .wait_changes(SETTLE, |e| MongoHarness::has(e, "c", 200))
        .await;
    h.stop().await.expect("stop");
    assert!(
        MongoHarness::has(&evs, "c", 200),
        "the change made while down never arrived"
    );
    let fabric = h.fabric().await;
    assert_eq!(
        fabric.duplicates(),
        0,
        "the restart re-delivered events from before the stop"
    );
    assert!(fabric.len() > before);
    // And by the kit: kill-restart, dup-check and the cdc script.
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &MongoHarness::cdc("resume_suites"),
        &[Suite::KillRestart, Suite::DupCheck, Suite::Cdc],
    )
    .await;
    assert!(report.green(), "{report}");
}

/// The source reports how much of the oplog window its token has used, and
/// how far behind the newest entry it is.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_oplog_window_metric() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::cdc("window");
    h.reset().await.expect("reset");
    h.seed(5).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    let rec = h.recorder();
    let reported = eventually(Duration::from_secs(30), || async {
        !rec.risks().is_empty()
    })
    .await;
    h.stop().await.expect("stop");
    assert!(reported, "no position_risk was reported");
    let risk = *rec.risks().last().expect("risk");
    assert!(
        (0.0..=1.0).contains(&risk),
        "a fresh token used {risk} of the window"
    );
    assert!(!rec.lags().is_empty(), "no lag was reported");
}

/// Dropping the database invalidates the stream: the instance pauses, and a
/// resume starts after the invalidate.
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_invalidate_pauses() {
    if !ci_only() {
        return;
    }
    let h = MongoHarness::cdc("invalidate");
    h.reset().await.expect("reset");
    h.seed(3).await.expect("seed");
    h.start().await.expect("start");
    h.settle(SETTLE).await.expect("settle");
    h.command(doc! { "dropDatabase" => 1 }).await.expect("drop");
    let error = h.wait_paused(SETTLE).await.expect("the instance paused");
    assert!(error.contains("invalidated"), "{error}");
    // Resumed, the stream restarts after the invalidate and keeps going.
    h.resume().await;
    h.insert(vec![doc! { "_id" => 7i64, "v" => "after-invalidate" }])
        .await
        .expect("insert");
    let evs = h
        .wait_changes(SETTLE, |e| MongoHarness::has(e, "c", 7))
        .await;
    h.kill(KillTarget::Task).await.expect("kill");
    assert!(
        MongoHarness::has(&evs, "c", 7),
        "nothing arrived after the resume"
    );
}

/// The kit's suites for a streaming CDC source: schema evolution and
/// backpressure (kill-restart, dup-check and cdc run above).
#[tokio::test(flavor = "multi_thread")]
async fn mongodb_cdc_suites() {
    if !ci_only() {
        return;
    }
    let env = RunEnv::from_process().expect("estate");
    let report = run_with(
        &env,
        &opts(),
        &spec("mongodb"),
        None,
        &MongoHarness::cdc("suites"),
        &[
            Suite::SchemaEvolution,
            Suite::Backpressure,
            Suite::Roundtrip,
        ],
    )
    .await;
    assert!(report.green(), "{report}");
}
