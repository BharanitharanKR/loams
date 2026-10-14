//! S3 and its S3-compatible targets: the native sink (OR-8) and the
//! conditional publish, on `InMemory` always and on RustFS and MinIO when
//! the CI job provides them.

mod common;

use std::sync::Arc;

use common::*;
use loams_connector_objects::store::Backend;
use loams_connector_sdk::StaticSecrets;
use object_store::ObjectStore;
use object_store::memory::InMemory;

/// An S3-compatible server the CI job started: `<NAME>_ENDPOINT`,
/// `<NAME>_BUCKET`, `<NAME>_ACCESS_KEY`, `<NAME>_SECRET_KEY`.
async fn s3_compatible(stack: &str, name: &str, what: &str) -> Option<Arc<dyn ObjectStore>> {
    let vars: Vec<String> = ["ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET_KEY"]
        .iter()
        .map(|v| format!("LOAMS_{name}_{v}"))
        .collect();
    let refs: Vec<&str> = vars.iter().map(String::as_str).collect();
    if !real(stack, &refs, what) {
        return None;
    }
    let endpoint = env(&vars[0]);
    Some(
        real_store(
            Backend::S3,
            serde_json::json!({
                "bucket": env(&vars[1]),
                "endpoint": endpoint,
                "allow_http": endpoint.starts_with("http://"),
                "region": "us-east-1",
                "auth": {"access_key_id": "key", "secret_access_key": "secret"}
            }),
            StaticSecrets::new()
                .with("key", env(&vars[2]))
                .with("secret", env(&vars[3])),
        )
        .await,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_native_sink_deterministic_names_effectively_once() {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    sink_crash_resume(mem.clone(), "crash").await;
    sink_names_deterministic(mem, "names").await;
    if let Some(s) =
        s3_compatible("LOAMS_CONNECTORS_P2_STACK", "RUSTFS", "the RustFS sink run").await
    {
        let p = run_prefix("s3-sink");
        sink_crash_resume(s.clone(), &format!("{p}/crash")).await;
        sink_names_deterministic(s, &format!("{p}/names")).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_put_refuses_overwrite() {
    conditional_put_check(Arc::new(InMemory::new()), "cond").await;
    if let Some(s) = s3_compatible(
        "LOAMS_CONNECTORS_P2_STACK",
        "RUSTFS",
        "RustFS conditional PUT",
    )
    .await
    {
        conditional_put_check(s, &run_prefix("cond")).await;
    }
}

/// The `minio-rustfs` target suite: listing, the sink and the conditional
/// publish against real RustFS (PR CI) and real MinIO (`ci-service`, built
/// from its last source release when the job provides it, R0.28).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_and_minio_suites() {
    let targets = [
        ("LOAMS_CONNECTORS_P2_STACK", "RUSTFS", "the RustFS suite"),
        ("LOAMS_CONNECTORS_CI_ONLY_STACK", "MINIO", "the MinIO suite"),
    ];
    for (stack, name, what) in targets {
        let Some(s) = s3_compatible(stack, name, what).await else {
            continue;
        };
        let p = run_prefix(&name.to_ascii_lowercase());
        let bucket = env(&format!("LOAMS_{name}_BUCKET"));
        listing_high_water_resume(s.clone(), &Seed::Store, &bucket, &format!("{p}/list")).await;
        sink_crash_resume(s.clone(), &format!("{p}/crash")).await;
        conditional_put_check(s, &format!("{p}/cond")).await;
    }
}
