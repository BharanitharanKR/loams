//! UP1 Task 4: the engine refuses to start on data it cannot read, with one
//! log line and exit code 78 (design §54 §4.3).

use std::io::BufRead as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use loams_common::meta::MetaStore;
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_migrate::formats::Store as FormatStore;
use loams_migrate::ledger::LEASE_TTL;
use loams_migrate::{Class, Ledger, LedgerEntry, LedgerState, MigrationLease};
use loams_store::Store;
use tempfile::TempDir;

fn dev(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loams"));
    command
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
            "--no-qdrant",
            "--no-es",
            "--no-durable",
        ])
        .args(if cfg!(feature = "live") {
            &["--no-live"][..]
        } else {
            &[]
        })
        .args(if cfg!(feature = "graph") {
            &["--no-graph"][..]
        } else {
            &[]
        })
        .arg("--data-dir")
        .arg(dir)
        .env("RUST_LOG", "warn");
    command
}

/// Starts `loams dev` on `dir` until it listens (a new stack: stamped), then
/// stops it.
fn first_start(dir: &Path) {
    let mut child = dev(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn loams");
    let stdout = child.stdout.take().expect("stdout");
    for line in std::io::BufReader::new(stdout).lines() {
        if line
            .expect("read stdout")
            .starts_with("loams listening on ")
        {
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Opens the stopped engine's embedded metastore and leaves a migration
/// `failed` in its ledger.
async fn fail_a_migration(dir: &Path) {
    let bucket = url::Url::from_directory_path(dir.join("bucket").canonicalize().expect("bucket"))
        .expect("url");
    let store = Store::from_url(bucket.as_str(), Vec::<(String, String)>::new()).expect("store");
    let node = MetaNode::start(MetaConfig::new(1, dir.join("meta"), store), &Router::new())
        .await
        .expect("node");
    node.wait_for_leader(Duration::from_secs(20))
        .await
        .expect("leader");
    let meta: Arc<dyn MetaStore> = Arc::new(MetaClient::new(
        node.clone(),
        Vec::new(),
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    ));
    let ledger = Ledger::open(meta.clone()).await.expect("ledger");
    assert_eq!(
        ledger
            .stack()
            .await
            .expect("stack")
            .expect("stamped")
            .release,
        env!("CARGO_PKG_VERSION"),
        "the first start stamped the new stack"
    );
    let lease = MigrationLease::acquire(meta, "test", LEASE_TTL)
        .await
        .expect("lease");
    let entry = LedgerEntry {
        seq: 1,
        id: "0001-test".to_string(),
        release_from: "0.0.0".to_string(),
        release_to: env!("CARGO_PKG_VERSION").to_string(),
        store: FormatStore::Meta,
        class: Class::Reversible,
        state: LedgerState::Failed,
        cursor: None,
        attempts: 1,
        started_at: None,
        finished_at: None,
        error: None,
        image_digest: None,
        code_digest: [0; 32],
    };
    ledger.append(&lease, &entry).await.expect("append");
    lease.release().await.expect("release");
    node.shutdown().await.expect("stop node");
}

#[tokio::test(flavor = "multi_thread")]
async fn loams_exits_78_on_refusal() {
    let dir = TempDir::new().expect("dir");
    first_start(dir.path());
    fail_a_migration(dir.path()).await;
    let output = dev(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("run loams");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(78), "{stderr}");
    assert!(stderr.contains("migration_incomplete"), "{stderr}");
    assert!(stderr.contains("0001-test is failed"), "{stderr}");
}
