//! Cloudflare R2: polling and the conditional publish, against a real
//! bucket nightly (R0.22: the first nightly also records whether a
//! conflicting `If-None-Match: *` PUT answers 412). PR CI replays fixtures
//! recorded from real R2 through Task 6's `FixtureProxy`, once both exist:
//! until then the replay half skips, and R2 stays `planned` (R13.5).

mod common;

use common::*;
use loams_connector_objects::store::Backend;
use loams_connector_sdk::StaticSecrets;

const FIXTURES: &str = "../../../conformance/connectors/fixtures/cloudflare-r2";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_fixture_replay_and_nightly() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURES);
    let recorded = std::fs::read_dir(&dir)
        .map(|d| {
            d.filter_map(Result::ok)
                .any(|e| e.path().extension().is_some_and(|x| x == "json"))
        })
        .unwrap_or(false);
    if !recorded {
        eprintln!(
            "skipping the R2 replay: no recording under {} (recorded by the first nightly run; replayed through Task 6's FixtureProxy)",
            dir.display()
        );
    }

    if !real(
        "LOAMS_CONNECTORS_NIGHTLY",
        &[
            "LOAMS_R2_ENDPOINT",
            "LOAMS_R2_BUCKET",
            "LOAMS_R2_ACCESS_KEY_ID",
            "LOAMS_R2_SECRET_ACCESS_KEY",
        ],
        "the R2 nightly",
    ) {
        return;
    }
    let bucket = env("LOAMS_R2_BUCKET");
    // The `cloudflare-r2` profile's defaults: region `auto`.
    let store = real_store(
        Backend::S3,
        serde_json::json!({
            "bucket": bucket, "endpoint": env("LOAMS_R2_ENDPOINT"), "region": "auto",
            "auth": {"access_key_id": "key", "secret_access_key": "secret"}
        }),
        StaticSecrets::new()
            .with("key", env("LOAMS_R2_ACCESS_KEY_ID"))
            .with("secret", env("LOAMS_R2_SECRET_ACCESS_KEY")),
    )
    .await;
    let p = run_prefix("r2");
    listing_high_water_resume(store.clone(), &Seed::Store, &bucket, &format!("{p}/list")).await;
    conditional_put_check(store.clone(), &format!("{p}/cond")).await;
    sink_crash_resume(store, &format!("{p}/crash")).await;
}
