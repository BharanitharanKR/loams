//! Sealed chDB workers read the bucket through `house-cache` and nothing else
//! (HS1 Task 9, §49 §7, D766).
//!
//! Each test runs real worker processes in the `netns` sandbox whose forwarder
//! leads to a `house-cache` in this process. The upstream is a `file://` store,
//! and, when `LOAMS_HOUSE_S3_ENDPOINT` (with `LOAMS_HOUSE_S3_KEY`,
//! `LOAMS_HOUSE_S3_SECRET`, `LOAMS_HOUSE_S3_BUCKET`) names an S3-compatible
//! server such as RustFS, that server.

#![cfg(target_os = "linux")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::{small, statement, tmp_root};
use loams_cache::RangeCacheConfig;
use loams_house::{HouseError, ProcessLauncher, SandboxMode, WorkerLease, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use parquet::arrow::ArrowWriter;

/// A credential the front holds for its store, and that must never reach or
/// come from a worker.
const CANARY: &str = "loams-canary-secret-9f2c";

/// `rows` numbers in one Parquet file.
fn parquet(rows: u64) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
    )
    .expect("batch");
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

/// The test's upstream: RustFS when named, else a `file://` store, with the
/// canary among its options. Returns it and the URL prefix workers use for it.
async fn upstream(test: &str) -> (Arc<Upstream>, String) {
    let config = RangeCacheConfig {
        block_size: 256 * 1024,
        memory_bytes: 64 << 20,
        disk: None,
    };
    let (url, options) = match std::env::var("LOAMS_HOUSE_S3_ENDPOINT") {
        Ok(endpoint) => {
            let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name}"));
            let url = format!(
                "s3://{}/{test}-{}",
                var("LOAMS_HOUSE_S3_BUCKET"),
                std::process::id()
            );
            let options = vec![
                ("aws_endpoint".to_string(), endpoint),
                ("aws_access_key_id".to_string(), var("LOAMS_HOUSE_S3_KEY")),
                (
                    "aws_secret_access_key".to_string(),
                    var("LOAMS_HOUSE_S3_SECRET"),
                ),
                ("aws_region".to_string(), "us-east-1".to_string()),
                ("aws_allow_http".to_string(), "true".to_string()),
                (
                    "aws_virtual_hosted_style_request".to_string(),
                    "false".to_string(),
                ),
            ];
            (url, options)
        }
        Err(_) => {
            let dir = tmp_root(&format!("{test}-bucket"));
            (format!("file://{}", dir.display()), Vec::new())
        }
    };
    let mut options = options;
    // A file store ignores options; an S3 one does not know this key. Either way
    // it is a secret the front holds.
    if url.starts_with("file://") {
        options.push(("aws_session_token".to_string(), CANARY.to_string()));
    }
    let upstream = Upstream::open(&url, StoreOptions(options), config)
        .await
        .expect("the upstream opens");
    let prefix = if upstream.prefix().is_empty() {
        format!("http://127.0.0.1:9180/{}", upstream.bucket())
    } else {
        format!(
            "http://127.0.0.1:9180/{}/{}",
            upstream.bucket(),
            upstream.prefix()
        )
    };
    (Arc::new(upstream), prefix)
}

/// A pool of sealed workers reading through `proxy`.
async fn sealed(test: &str, proxy: &HouseCache) -> WorkerPool {
    let launcher = ProcessLauncher::new(common::WORKER, tmp_root(test))
        .with_forward_target(Arc::new(proxy.clone()));
    assert_eq!(launcher.sandbox(), SandboxMode::Netns, "sealed by default");
    WorkerPool::start(small(2), Arc::new(launcher))
        .await
        .expect("the pool starts")
}

/// Runs `sql` on the lease's worker with L1 off (`WorkerLease::run`), as an
/// attacker past the deny list would.
async fn run(lease: &mut WorkerLease, sql: &str) -> Result<String, HouseError> {
    let mut execute = statement(sql, "TSV");
    // chDB's S3 client retries a refused read for minutes by default.
    execute.settings = vec![("s3_retry_attempts".to_string(), "0".to_string())];
    execute.limits.max_execution_time_ms = Some(30_000);
    tokio::time::timeout(Duration::from_secs(90), lease.run(execute))
        .await
        .unwrap_or_else(|_| panic!("{sql}: bounded"))
        .map(|collected| String::from_utf8_lossy(&collected.bytes).into_owned())
}

fn s3(url: &str) -> String {
    format!("SELECT count(), sum(n) FROM s3('{url}', 'dummy', 'dummy', 'Parquet')")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chdb_reads_through_proxy_only() {
    let (upstream, prefix) = upstream("through-proxy").await;
    upstream
        .store()
        .put("ns/tenant-a/t.parquet", parquet(50_000))
        .await
        .expect("put");
    upstream
        .store()
        .put("ns/tenant-b/t.parquet", parquet(10))
        .await
        .expect("put");
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::clone(&upstream))));
    let pool = sealed("through-proxy", &proxy).await;

    let mut lease = pool.acquire("tenant-a").await.expect("worker");
    // The namespace's own object reads, through the forwarder and the proxy.
    let own = run(&mut lease, &s3(&format!("{prefix}/ns/tenant-a/t.parquet")))
        .await
        .expect("the namespace's own file reads");
    assert_eq!(own.trim(), format!("50000\t{}", 49_999u64 * 50_000 / 2));
    let metrics = proxy.metrics();
    let ours = metrics.namespaces["tenant-a"];
    assert!(ours.requests > 0 && ours.bytes_served > 0, "{metrics:?}");

    // Another namespace's object is refused by the proxy, with the deny list off.
    let foreign = run(&mut lease, &s3(&format!("{prefix}/ns/tenant-b/t.parquet"))).await;
    assert!(foreign.is_err(), "{foreign:?}");
    assert!(proxy.metrics().namespaces["tenant-a"].refused > 0);

    // And nothing but the forwarder is reachable: the store itself, named
    // directly, is not (a file store has no address; RustFS has one).
    if let Ok(endpoint) = std::env::var("LOAMS_HOUSE_S3_ENDPOINT") {
        let bucket = std::env::var("LOAMS_HOUSE_S3_BUCKET").expect("bucket");
        let direct = format!(
            "{endpoint}/{bucket}/{}/ns/tenant-a/t.parquet",
            upstream.prefix()
        );
        let bypass = run(&mut lease, &s3(&direct)).await;
        assert!(bypass.is_err(), "{bypass:?}");
    }
    pool.release(lease, loams_house::Outcome::Completed);
    pool.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_credentials_reach_the_worker() {
    let (upstream, prefix) = upstream("no-credentials").await;
    upstream
        .store()
        .put("ns/tenant-a/t.parquet", parquet(1_000))
        .await
        .expect("put");
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::clone(&upstream))));
    proxy.record_requests(true);
    let pool = sealed("no-credentials", &proxy).await;
    let mut lease = pool.acquire("tenant-a").await.expect("worker");
    let pid = lease.pid();
    run(&mut lease, &s3(&format!("{prefix}/ns/tenant-a/t.parquet")))
        .await
        .expect("reads");

    // What the worker sent: chDB's dummy keys at most, never the front's.
    let secret = std::env::var("LOAMS_HOUSE_S3_SECRET").unwrap_or_else(|_| CANARY.to_string());
    let key = std::env::var("LOAMS_HOUSE_S3_KEY").ok();
    let seen = proxy.requests_seen();
    assert!(
        !seen.is_empty(),
        "the worker's reads came through the proxy"
    );
    for request in &seen {
        for (name, value) in &request.headers {
            assert!(
                !value.contains(&secret),
                "{name} carries the front's secret"
            );
            if let Some(key) = &key {
                assert!(
                    !value.contains(key.as_str()),
                    "{name} carries the front's key"
                );
            }
            if name.eq_ignore_ascii_case("authorization") {
                assert!(value.contains("Credential=dummy/"), "{value}");
            }
        }
    }
    // Nor do the worker's arguments or environment hold it.
    for file in ["cmdline", "environ"] {
        let bytes = std::fs::read(format!("/proc/{pid}/{file}")).expect(file);
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains(&secret), "/proc/{pid}/{file}");
    }
    // The proxy's own debug output does not print the store's options.
    assert!(!format!("{proxy:?}").contains(&secret));
    pool.release(lease, loams_house::Outcome::Completed);
    pool.shutdown();
}
