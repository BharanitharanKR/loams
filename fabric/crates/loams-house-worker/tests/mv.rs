//! Materialized views over lake tables (HS1 Task 17, §49 §9): each committed
//! insert into the source is one block of the view, applied exactly once,
//! chains included; a failing view fails its `INSERT` as ClickHouse's default
//! does.
//!
//! The reference is ClickHouse itself (`clickhouse local`, from
//! `CLICKHOUSE_REFERENCE` or `PATH`) with the same tables as `MergeTree`
//! tables; the comparison is skipped, saying so, where it is not installed.

#![cfg(target_os = "linux")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::http::{Response, request, target};
use common::{small, tmp_root};
use loams_cache::RangeCacheConfig;
use loams_house::catalog::{HouseCatalog, Lake, LakeRoot, MemCatalog, PrefixGrants};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::mv::ViewRunner;
use loams_house::pipes::ViewWriter;
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_store::Store;

const NS: u64 = 2;

struct Lakehouse {
    handle: HouseHandle,
    lake: Arc<Lake>,
    pool: WorkerPool,
    config: HouseConfig,
}

impl Lakehouse {
    fn views(&self) -> ViewRunner {
        ViewRunner::new(ViewWriter {
            lake: Arc::clone(&self.lake),
            pool: self.pool.clone(),
            limits: self.config.session_limits,
            max_insert_bytes: self.config.lake_insert_max_bytes,
        })
    }
}

async fn lakehouse(test: &str, view_interval: Duration) -> Lakehouse {
    let dir = tmp_root(&format!("{test}-bucket"));
    let url = format!("file://{}", dir.display());
    let store = Store::from_url(&url, Vec::<(String, String)>::new()).expect("store");
    let catalog = Arc::new(MemCatalog::new(store.clone(), LakeRoot::local()));
    let upstream = Upstream::open(&url, StoreOptions::default(), RangeCacheConfig::default())
        .await
        .expect("upstream");
    let proxy = HouseCache::new(Arc::new(OneStore(Arc::new(upstream))));
    let launcher = ProcessLauncher::new(common::WORKER, tmp_root(test))
        .with_forward_target(Arc::new(proxy.clone()));
    let pool = WorkerPool::start(small(2), Arc::new(launcher))
        .await
        .expect("the pool starts");
    let lake = Arc::new(
        Lake::new(catalog as Arc<dyn HouseCatalog>, LakeRoot::local())
            .with_store(store)
            .with_grants(Arc::new(proxy) as Arc<dyn PrefixGrants>),
    );
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![UserMap::dev("alice", "a", NS, false)],
        tmp_dir: tmp_root(&format!("{test}-spool")),
        compaction_interval: Duration::ZERO,
        retention_interval: Duration::ZERO,
        pipe_interval: Duration::ZERO,
        view_interval,
        ..HouseConfig::default()
    };
    let handle = serve_with_lake(config.clone(), pool.clone(), lake.clone())
        .await
        .expect("serves");
    Lakehouse {
        handle,
        lake,
        pool,
        config,
    }
}

async fn post_with(addr: SocketAddr, settings: &[(&str, &str)], sql: &str) -> Response {
    let mut params = vec![("user", "alice"), ("password", "a")];
    params.extend_from_slice(settings);
    let target = target(&params);
    let body = sql.as_bytes().to_vec();
    tokio::task::spawn_blocking(move || request(addr, "POST", &target, &[], &body))
        .await
        .expect("client")
}

async fn ok(addr: SocketAddr, sql: &str) -> String {
    let response = post_with(addr, &[], sql).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response.text()
}

/// `clickhouse local` with `statements`, separated by `;`, when installed.
fn reference(statements: &str) -> Option<String> {
    let binary = std::env::var("CLICKHOUSE_REFERENCE").unwrap_or_else(|_| "clickhouse".to_string());
    let output = std::process::Command::new(&binary)
        .args(["local", "--multiquery", "--query", statements])
        .output()
        .ok()?;
    if !output.status.success() {
        panic!(
            "the reference failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

const SOURCE: &str = "CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k";
const AGG: &str = "CREATE TABLE agg (k UInt64, c UInt64, s UInt64) ENGINE = MergeTree ORDER BY k";
const VIEW: &str =
    "CREATE MATERIALIZED VIEW mv TO agg AS SELECT k, count() AS c, sum(v) AS s FROM src GROUP BY k";
const BLOCKS: [&str; 3] = [
    "INSERT INTO src VALUES (1, 10), (1, 11), (2, 20)",
    "INSERT INTO src VALUES (1, 12), (3, 30)",
    "INSERT INTO src VALUES (2, 21), (2, 22), (3, 31), (4, 40)",
];
const AGG_ROWS: &str = "SELECT k, c, s FROM agg ORDER BY k, c, s";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_per_block_matches_reference() {
    let house = lakehouse("mv-block", Duration::ZERO).await;
    let addr = house.handle.local_addr();
    ok(addr, SOURCE).await;
    // Rows from before the view are not the view's (no POPULATE).
    ok(addr, "INSERT INTO src VALUES (9, 90)").await;
    ok(addr, AGG).await;
    ok(addr, VIEW).await;
    for block in BLOCKS {
        ok(addr, block).await;
    }
    // Applied before each INSERT answered: one aggregate row per key per block.
    let got = ok(addr, AGG_ROWS).await;
    let expected = "1\t1\t12\n1\t2\t21\n2\t1\t20\n2\t2\t43\n3\t1\t30\n3\t1\t31\n4\t1\t40\n";
    assert_eq!(got, expected);
    let statements = format!(
        "{SOURCE}; INSERT INTO src VALUES (9, 90); {AGG}; {VIEW}; {}; {AGG_ROWS}",
        BLOCKS.join("; ")
    );
    match reference(&statements) {
        Some(reference) => assert_eq!(got, reference),
        None => eprintln!("mv_per_block_matches_reference: no `clickhouse` reference"),
    }
    // A snapshot of the target names the source snapshot it applied.
    let applied = ok(
        addr,
        "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'agg'",
    )
    .await;
    assert_eq!(applied, "3\n");
    // Dropped, the view stops.
    ok(addr, "DROP VIEW mv").await;
    ok(addr, "INSERT INTO src VALUES (5, 50)").await;
    assert_eq!(ok(addr, "SELECT count() FROM agg WHERE k = 5").await, "0\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_retry_does_not_double_apply() {
    let house = lakehouse("mv-retry", Duration::ZERO).await;
    let addr = house.handle.local_addr();
    ok(addr, SOURCE).await;
    ok(addr, AGG).await;
    ok(addr, VIEW).await;
    // An insert answered before its commit: its views did not run with it,
    // as when a front dies between the source's commit and the view's.
    let response = post_with(
        addr,
        &[
            ("async_insert", "1"),
            ("wait_for_async_insert", "0"),
            ("insert_deduplication_token", "t1"),
        ],
        BLOCKS[0],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    for _ in 0..100 {
        if ok(addr, "SELECT count() FROM src").await == "3\n" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ok(addr, AGG_ROWS).await, "");
    // Two fronts catch up at once, then again: the block is applied once.
    let (first, second) = (house.views(), house.views());
    let (a, b) = tokio::join!(
        first.after_commit(NS, "default", "src"),
        second.after_commit(NS, "default", "src"),
    );
    a.expect("a");
    b.expect("b");
    house
        .views()
        .after_commit(NS, "default", "src")
        .await
        .expect("again");
    assert_eq!(ok(addr, AGG_ROWS).await, "1\t2\t21\n2\t1\t20\n");
    // The client retries with its token: the source keeps one copy, and the
    // view applies nothing more.
    let response = post_with(addr, &[("insert_deduplication_token", "t1")], BLOCKS[0]).await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(ok(addr, "SELECT count() FROM src").await, "3\n");
    assert_eq!(ok(addr, AGG_ROWS).await, "1\t2\t21\n2\t1\t20\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_chain_two_levels() {
    let house = lakehouse("mv-chain", Duration::ZERO).await;
    let addr = house.handle.local_addr();
    ok(addr, SOURCE).await;
    ok(
        addr,
        "CREATE TABLE doubled (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE totals (k UInt64, total UInt64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW m1 TO doubled AS SELECT k, v * 2 AS v FROM src",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW m2 TO totals AS SELECT k, sum(v) AS total FROM doubled \
         GROUP BY k",
    )
    .await;
    ok(addr, BLOCKS[0]).await;
    ok(addr, BLOCKS[1]).await;
    assert_eq!(
        ok(addr, "SELECT k, total FROM totals ORDER BY k, total").await,
        "1\t24\n1\t42\n2\t40\n3\t60\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_failure_does_not_fail_source_insert() {
    // ClickHouse 26.9's default is materialized_views_ignore_errors = 0: the
    // INSERT answers the view's error, its own rows written; with 1 it
    // succeeds. The view skips the block it failed on and goes on.
    let house = lakehouse("mv-fail", Duration::ZERO).await;
    let addr = house.handle.local_addr();
    ok(addr, SOURCE).await;
    ok(addr, AGG).await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW mv TO agg AS SELECT k, count() AS c, \
         sum(v + throwIf(k = 13)) AS s FROM src GROUP BY k",
    )
    .await;
    let failed = post_with(addr, &[], "INSERT INTO src VALUES (13, 1)").await;
    assert_ne!(failed.status, 200);
    assert_eq!(failed.header("X-ClickHouse-Exception-Code"), Some("395"));
    assert_eq!(ok(addr, "SELECT count() FROM src").await, "1\n");
    let ignored = post_with(
        addr,
        &[("materialized_views_ignore_errors", "1")],
        "INSERT INTO src VALUES (13, 2)",
    )
    .await;
    assert_eq!(ignored.status, 200, "{}", ignored.text());
    ok(addr, "INSERT INTO src VALUES (1, 5)").await;
    assert_eq!(ok(addr, "SELECT count() FROM src").await, "3\n");
    assert_eq!(ok(addr, AGG_ROWS).await, "1\t1\t5\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_catches_up_in_the_background() {
    let house = lakehouse("mv-catch-up", Duration::from_millis(200)).await;
    let addr = house.handle.local_addr();
    ok(addr, SOURCE).await;
    ok(addr, AGG).await;
    ok(addr, VIEW).await;
    let response = post_with(
        addr,
        &[("async_insert", "1"), ("wait_for_async_insert", "0")],
        BLOCKS[1],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    let mut got = String::new();
    for _ in 0..100 {
        got = ok(addr, AGG_ROWS).await;
        if !got.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(got, "1\t1\t12\n3\t1\t30\n");
    // A view may not read and write one table.
    let looped = post_with(
        addr,
        &[],
        "CREATE MATERIALIZED VIEW loop TO src AS SELECT k, v FROM src",
    )
    .await;
    assert_eq!(looped.header("X-ClickHouse-Exception-Code"), Some("36"));
}

/// `conformance/clickhouse/corpus/mv/`: each file's statements run on a fresh
/// namespace database, and its last statement's rows equal the reference's
/// over the same statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mv_corpus_matches_reference() {
    let house = lakehouse("mv-corpus", Duration::ZERO).await;
    let addr = house.handle.local_addr();
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../conformance/clickhouse/corpus/mv"
    );
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("the corpus")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    assert!(files.len() >= 3, "{files:?}");
    for (i, file) in files.iter().enumerate() {
        let text = std::fs::read_to_string(file).expect("a corpus file");
        let script: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let statements: Vec<&str> = script
            .split(";\n")
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        // Each file in its own database.
        let database = format!("corpus_{i}");
        ok(addr, &format!("CREATE DATABASE {database}")).await;
        let mut last = String::new();
        for statement in &statements {
            let mut params = vec![("user", "alice"), ("password", "a")];
            params.push(("database", database.as_str()));
            let target = target(&params);
            let body = statement.as_bytes().to_vec();
            let response =
                tokio::task::spawn_blocking(move || request(addr, "POST", &target, &[], &body))
                    .await
                    .expect("client");
            assert_eq!(
                response.status,
                200,
                "{}: {statement}: {}",
                file.display(),
                response.text()
            );
            last = response.text();
        }
        match reference(&statements.join(";\n")) {
            Some(reference) => assert_eq!(last, reference, "{}", file.display()),
            None => eprintln!(
                "mv_corpus_matches_reference: no `clickhouse` reference for {}",
                file.display()
            ),
        }
    }
}
