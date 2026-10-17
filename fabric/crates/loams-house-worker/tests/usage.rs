//! Quotas and usage events (HS1 Task 22, design §49 §15), over the ClickHouse
//! HTTP interface with real worker processes and a `file://` bucket shared by
//! the catalog, `house-cache` and the front.
//!
//! Run locally in a cgroup of its own (`systemd-run --user --scope`, R21.8):
//! chDB's memory limit reads the whole cgroup's memory.

#![cfg(target_os = "linux")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::http::{Response, request, target};
use common::{small, tmp_root};
use loams_cache::RangeCacheConfig;
use loams_house::catalog::{HouseCatalog, Lake, LakeRoot, MemCatalog, PrefixGrants};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::quota::{ConfigQuotas, NamespaceQuota};
use loams_house::usage::{Recorder, UsageEvent};
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_store::Store;

const KV: &str = "CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k";

struct House {
    handle: HouseHandle,
    recorder: Arc<Recorder>,
    _pool: WorkerPool,
    _proxy: HouseCache,
}

impl House {
    fn addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }
}

/// A lake House whose namespace 2 (alice) has `quota`; namespace 3 (carol)
/// has none. Every usage event is recorded.
async fn lake_house(test: &str, quota: NamespaceQuota) -> House {
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
    let pool = WorkerPool::start(small(3), Arc::new(launcher))
        .await
        .expect("the pool starts");
    let lake = Arc::new(
        Lake::new(catalog as Arc<dyn HouseCatalog>, LakeRoot::local())
            .with_store(store)
            .with_grants(Arc::new(proxy.clone()) as Arc<dyn PrefixGrants>),
    );
    let recorder = Arc::new(Recorder::default());
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![
            UserMap::dev("alice", "a", 2, false),
            UserMap::dev("carol", "c", 3, false),
        ],
        tmp_dir: tmp_root(&format!("{test}-spool")),
        compaction_interval: Duration::ZERO,
        // The tests run their storage rounds themselves.
        storage_usage_interval: Duration::ZERO,
        quotas: Arc::new(ConfigQuotas {
            default: NamespaceQuota::default(),
            namespaces: [("2".to_string(), quota)].into(),
        }),
        usage: recorder.clone(),
        ..HouseConfig::default()
    };
    let handle = serve_with_lake(config, pool.clone(), lake)
        .await
        .expect("serves");
    House {
        handle,
        recorder,
        _pool: pool,
        _proxy: proxy,
    }
}

/// `POST /?query=…` as `user` (`alice` or `carol`), with `body` and `params`.
async fn post(
    addr: SocketAddr,
    user: &'static str,
    query: Option<&str>,
    body: &[u8],
    params: &[(&str, &str)],
) -> Response {
    let password = &user[..1];
    let mut all = vec![("user", user), ("password", password)];
    if let Some(query) = query {
        all.push(("query", query));
    }
    all.extend_from_slice(params);
    let target = target(&all);
    let body = body.to_vec();
    tokio::task::spawn_blocking(move || request(addr, "POST", &target, &[], &body))
        .await
        .expect("client")
}

async fn ok(addr: SocketAddr, user: &'static str, sql: &str) -> Response {
    let response = post(addr, user, None, sql.as_bytes(), &[]).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response
}

fn code(response: &Response) -> Option<&str> {
    response.header("X-ClickHouse-Exception-Code")
}

/// A quota refusal: the code, `house_quota_exceeded` and the quota named.
fn assert_quota(response: &Response, expected: &str, quota: &str) {
    assert_eq!(code(response), Some(expected), "{}", response.text());
    let text = response.text();
    assert!(
        text.contains("house_quota_exceeded") && text.contains(quota),
        "names the quota: {text}"
    );
}

/// `concurrent_queries` and `queries_per_minute`: past either, `202` at once
/// (not queued), naming `house_quota_exceeded` and the quota; another
/// namespace is not held to them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_rate_and_concurrency_202() {
    let house = lake_house(
        "quota-rate",
        NamespaceQuota {
            concurrent_queries: Some(1),
            queries_per_minute: Some(2),
            ..NamespaceQuota::default()
        },
    )
    .await;
    let addr = house.addr();
    // One running: a second is refused at once, not queued behind it.
    let slow =
        tokio::spawn(async move { post(addr, "alice", Some("SELECT sleep(2)"), b"", &[]).await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    let started = std::time::Instant::now();
    let busy = post(addr, "alice", Some("SELECT 1"), b"", &[]).await;
    assert!(started.elapsed() < Duration::from_secs(1), "not queued");
    assert_quota(&busy, "202", "concurrent_queries");
    // Namespace 3 has no quota.
    ok(addr, "carol", "SELECT 1").await;
    assert_eq!(slow.await.expect("slow").status, 200);
    // Two started in this minute (the refused one counted none): the third is
    // past the rate.
    ok(addr, "alice", "SELECT 2").await;
    let fast = post(addr, "alice", Some("SELECT 3"), b"", &[]).await;
    assert_quota(&fast, "202", "queries_per_minute");
    // A write is held to the rate the same way: 202, not 164.
    let write = post(addr, "alice", None, KV.as_bytes(), &[]).await;
    assert_quota(&write, "202", "queries_per_minute");
}

/// `scan_bytes_per_day`: the statement that crosses it runs; past it, reads
/// are `202` and writes `164`, both naming the quota; `241` never.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_scan_bytes_per_day() {
    let house = lake_house(
        "quota-scan",
        NamespaceQuota {
            scan_bytes_per_day: Some(1_000_000),
            ..NamespaceQuota::default()
        },
    )
    .await;
    let addr = house.addr();
    ok(addr, "alice", KV).await;
    // 8 MB of `numbers`: under the quota when it starts, over it when it ends.
    ok(addr, "alice", "SELECT sum(number) FROM numbers(1000000)").await;
    let read = post(addr, "alice", Some("SELECT 1"), b"", &[]).await;
    assert_quota(&read, "202", "scan_bytes_per_day");
    let write = post(
        addr,
        "alice",
        Some("INSERT INTO t VALUES (1, 'a')"),
        b"",
        &[],
    )
    .await;
    assert_quota(&write, "164", "scan_bytes_per_day");
    // Another namespace reads on.
    ok(addr, "carol", "SELECT sum(number) FROM numbers(1000000)").await;
    let used =
        house
            .handle
            .usage()
            .quotas()
            .usage("2", std::time::Instant::now(), SystemTime::now());
    assert!(used.scanned_today >= 8_000_000, "{used:?}");
}

/// Every client statement is one `house.query`, with the worker's CPU time as
/// the front measured it and the bytes it scanned and returned; an `INSERT`
/// into a lake table is also one `house.insert` with its rows and bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_event_per_query_has_cpu_and_bytes() {
    let house = lake_house("usage-query", NamespaceQuota::default()).await;
    let addr = house.addr();
    let sql = "SELECT sum(cityHash64(number)) FROM numbers(30000000)";
    post(
        addr,
        "alice",
        None,
        sql.as_bytes(),
        &[("query_id", "usage-q1")],
    )
    .await;
    let queries: Vec<_> = house
        .recorder
        .events()
        .into_iter()
        .filter_map(|e| match e {
            UsageEvent::Query(q) if q.query_id == "usage-q1" => Some(q),
            _ => None,
        })
        .collect();
    assert_eq!(queries.len(), 1, "one event per query: {queries:?}");
    let q = &queries[0];
    assert_eq!(q.namespace, "2");
    assert_eq!(q.code, 0);
    assert_eq!(q.statements, 1);
    let cpu = q.cpu.expect("a process worker's CPU is measured");
    assert!(cpu >= Duration::from_millis(10), "{q:?}");
    assert!(q.bytes_scanned >= 240_000_000, "{q:?}");
    assert_eq!(q.rows_read, 30_000_000, "{q:?}");
    assert!(q.result_bytes > 0 && q.result_rows == 1, "{q:?}");
    assert!(q.worker_time > Duration::ZERO, "{q:?}");

    // A failed statement is reported with its code.
    let failed = post(
        addr,
        "alice",
        Some("SELECT throwIf(1)"),
        b"",
        &[("query_id", "usage-q2")],
    )
    .await;
    assert_ne!(failed.status, 200);
    let events = house.recorder.events();
    let q2 = events
        .iter()
        .find_map(|e| match e {
            UsageEvent::Query(q) if q.query_id == "usage-q2" => Some(q),
            _ => None,
        })
        .expect("the failed statement's event");
    assert_eq!(q2.code, 395, "{q2:?}");

    // An INSERT: its worker statements are a `house.query`, its rows a
    // `house.insert`.
    ok(addr, "alice", KV).await;
    post(
        addr,
        "alice",
        Some("INSERT INTO t SELECT number, toString(number) FROM numbers(1000)"),
        b"",
        &[("query_id", "usage-i1")],
    )
    .await;
    let events = house.recorder.events();
    let inserts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            UsageEvent::Insert(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(inserts.len(), 1, "{inserts:?}");
    let i = inserts[0];
    assert_eq!(
        (
            i.namespace.as_str(),
            i.query_id.as_str(),
            i.database.as_str()
        ),
        ("2", "usage-i1", "default")
    );
    assert_eq!((i.table.as_str(), i.rows), ("t", 1000));
    assert!(i.bytes > 0, "{i:?}");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, UsageEvent::Query(q) if q.query_id == "usage-i1")),
        "the insert's worker part is a house.query"
    );
    let used =
        house
            .handle
            .usage()
            .quotas()
            .usage("2", std::time::Instant::now(), SystemTime::now());
    assert_eq!(used.inserted_today, i.bytes);
    assert!(used.cpu_today >= cpu, "{used:?}");
}

/// The storage round (daily by default) sends one `house.storage` per lake
/// table with its bytes and day, and sets the namespace's storage count, which
/// `storage_bytes` holds writes to (`164`) while reads go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_event_daily() {
    assert_eq!(
        HouseConfig::default().storage_usage_interval,
        Duration::from_secs(24 * 3600)
    );
    let house = lake_house(
        "usage-storage",
        NamespaceQuota {
            storage_bytes: Some(1_000_000_000),
            ..NamespaceQuota::default()
        },
    )
    .await;
    let addr = house.addr();
    ok(addr, "alice", KV).await;
    ok(
        addr,
        "alice",
        "CREATE TABLE empty (k UInt64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "alice",
        "INSERT INTO t SELECT number, toString(number) FROM numbers(10000)",
    )
    .await;
    ok(addr, "carol", KV).await;
    ok(addr, "carol", "INSERT INTO t VALUES (1, 'c')").await;

    let at = SystemTime::UNIX_EPOCH + Duration::from_secs(20_500 * 86_400 + 600);
    house.handle.storage_round(at).await;
    let storage: Vec<_> = house
        .recorder
        .events()
        .into_iter()
        .filter_map(|e| match e {
            UsageEvent::Storage(s) => Some(s),
            _ => None,
        })
        .collect();
    let of = |ns: &str, table: &str| {
        storage
            .iter()
            .find(|s| s.namespace == ns && s.table == table)
            .unwrap_or_else(|| panic!("{ns} {table}: {storage:?}"))
    };
    let t = of("2", "t");
    assert_eq!((t.database.as_str(), t.day), ("default", 20_500));
    assert!(t.bytes > 0, "{t:?}");
    assert_eq!(of("2", "empty").bytes, 0);
    assert!(of("3", "t").bytes > 0);
    assert_eq!(storage.len(), 3, "one per table: {storage:?}");
    let used =
        house
            .handle
            .usage()
            .quotas()
            .usage("2", std::time::Instant::now(), SystemTime::now());
    assert_eq!(used.storage, t.bytes, "the round's total, nothing since");

    // A namespace over its storage quota: writes are 164, reads go on.
    let small = lake_house(
        "usage-storage-full",
        NamespaceQuota {
            storage_bytes: Some(1),
            ..NamespaceQuota::default()
        },
    )
    .await;
    let addr = small.addr();
    ok(addr, "alice", KV).await;
    ok(addr, "alice", "INSERT INTO t VALUES (1, 'a')").await;
    let full = post(
        addr,
        "alice",
        Some("INSERT INTO t VALUES (2, 'b')"),
        b"",
        &[],
    )
    .await;
    assert_quota(&full, "164", "storage_bytes");
    ok(addr, "alice", "SELECT count() FROM t").await;
    // A drop frees storage and is not held to it.
    small.handle.storage_round(SystemTime::now()).await;
    ok(addr, "alice", "DROP TABLE t").await;
}
