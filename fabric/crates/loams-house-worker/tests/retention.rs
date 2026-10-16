//! `RestoreTable` over `loams.house.v1` on a House with lake tables (HS1 Task
//! 15, §49 §11 and §18.2): the operation it answers, its scope, its
//! idempotency key, its audit record, and the rows a query reads after it.

#![cfg(target_os = "linux")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use common::http::{Response, request, target};
use common::{small, tmp_root};
use connectrpc::ErrorCode;
use connectrpc::client::{ClientConfig, HttpClient};
use loams_cache::RangeCacheConfig;
use loams_house::audit::Memory;
use loams_house::catalog::{HouseCatalog, Lake, LakeRoot, MemCatalog, PrefixGrants};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_house_proto::loams::house::v1 as pb;
use loams_house_proto::loams::house::v1::__buffa::oneof::restore_table_request::Point;
use loams_house_proto::loams::house::v1::HouseServiceClient;
use loams_house_proto::loams::operations::v1::OperationState;
use loams_store::Store;

struct Lakehouse {
    handle: HouseHandle,
    audit: Arc<Memory>,
    _pool: WorkerPool,
}

async fn lakehouse(test: &str) -> Lakehouse {
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
    let audit = Arc::new(Memory::default());
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![
            UserMap::dev("alice", "a", 2, false),
            UserMap::dev("reader", "r", 2, true),
        ],
        tmp_dir: tmp_root(&format!("{test}-spool")),
        compaction_interval: Duration::ZERO,
        retention_interval: Duration::ZERO,
        audit: Some(audit.clone()),
        ..HouseConfig::default()
    };
    let handle = serve_with_lake(config, pool.clone(), lake)
        .await
        .expect("serves");
    Lakehouse {
        handle,
        audit,
        _pool: pool,
    }
}

async fn post(addr: SocketAddr, sql: &str) -> Response {
    let target = target(&[("user", "alice"), ("password", "a")]);
    let body = sql.as_bytes().to_vec();
    tokio::task::spawn_blocking(move || request(addr, "POST", &target, &[], &body))
        .await
        .expect("client")
}

async fn ok(addr: SocketAddr, sql: &str) -> String {
    let response = post(addr, sql).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response.text()
}

/// The snapshot an insert committed.
async fn insert(addr: SocketAddr, sql: &str) -> i64 {
    let response = post(addr, sql).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response
        .header("X-Loams-Snapshot-Id")
        .and_then(|h| h.strip_prefix("default.e="))
        .and_then(|id| id.parse().ok())
        .unwrap_or_else(|| panic!("{sql}: no snapshot in {:?}", response.header_names()))
}

fn client(addr: SocketAddr, user: &str, password: &str) -> HouseServiceClient<HttpClient> {
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let uri: http::Uri = format!("http://{addr}").parse().expect("uri");
    HouseServiceClient::new(
        HttpClient::plaintext(),
        ClientConfig::new(uri).with_default_header("authorization", format!("Basic {basic}")),
    )
}

fn restore(snapshot_id: i64, key: &str) -> pb::RestoreTableRequest {
    pb::RestoreTableRequest {
        table: "e".to_string(),
        point: Some(Point::SnapshotId(snapshot_id)),
        idempotency_key: key.to_string(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_table_over_the_api() {
    let house = lakehouse("restore-api").await;
    let addr = house.handle.local_addr();
    ok(
        addr,
        "CREATE TABLE e (k Int64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    let s1 = insert(addr, "INSERT INTO e VALUES (1)").await;
    insert(addr, "INSERT INTO e VALUES (2), (3)").await;
    assert_eq!(ok(addr, "SELECT count() FROM e").await, "3\n");

    let alice = client(addr, "alice", "a");
    let key = "01J9Z3Q4W5E6R7T8Y9U0I1O2P3";
    let operation = alice
        .restore_table(restore(s1, key))
        .await
        .expect("restored")
        .into_owned();
    assert!(
        operation.id.starts_with("op-") && operation.id.len() == 29,
        "{}",
        operation.id
    );
    assert_eq!(operation.kind, "house.restore_table");
    assert_eq!(operation.namespace, "2");
    assert_eq!(
        operation.state.as_known(),
        Some(OperationState::OPERATION_STATE_SUCCEEDED)
    );
    assert_eq!(
        operation.target.get("database").map(String::as_str),
        Some("default")
    );
    assert_eq!(operation.target.get("table").map(String::as_str), Some("e"));
    let result = operation.result.as_option().expect("a result");
    assert!(
        result
            .fields
            .get("snapshot_id")
            .map(|v| format!("{v:?}"))
            .is_some_and(|v| v.contains(&s1.to_string())),
        "{result:?}"
    );
    assert_eq!(ok(addr, "SELECT count() FROM e").await, "1\n");

    // New rows after the restore; the same key again changes nothing.
    insert(addr, "INSERT INTO e VALUES (4)").await;
    let again = alice
        .restore_table(restore(s1, key))
        .await
        .expect("repeat")
        .into_owned();
    assert_eq!(again.id, operation.id);
    assert_eq!(ok(addr, "SELECT k FROM e ORDER BY k").await, "1\n4\n");

    // Refusals: no admin scope, no point, an unknown snapshot, a table
    // that does not exist.
    let reader = client(addr, "reader", "r");
    let err = reader
        .restore_table(restore(s1, ""))
        .await
        .expect_err("a reader");
    assert_eq!(err.code, ErrorCode::PermissionDenied, "{err:?}");
    let err = alice
        .restore_table(pb::RestoreTableRequest {
            table: "e".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("no point");
    assert_eq!(err.code, ErrorCode::InvalidArgument, "{err:?}");
    let err = alice
        .restore_table(restore(42, ""))
        .await
        .expect_err("no such snapshot");
    assert_eq!(err.code, ErrorCode::InvalidArgument, "{err:?}");
    let err = alice
        .restore_table(pb::RestoreTableRequest {
            table: "nope".to_string(),
            point: Some(Point::SnapshotId(s1)),
            ..Default::default()
        })
        .await
        .expect_err("no such table");
    assert_eq!(err.code, ErrorCode::NotFound, "{err:?}");

    // Each call is audited, with its outcome.
    let restores: Vec<(String, i32)> = house
        .audit
        .events()
        .into_iter()
        .filter(|e| e.statement.as_deref() == Some("RestoreTable"))
        .map(|e| (e.user, e.code))
        .collect();
    assert_eq!(
        restores,
        vec![
            ("alice".to_string(), 0),
            ("alice".to_string(), 0),
            ("reader".to_string(), 497),
            ("alice".to_string(), 36),
            ("alice".to_string(), 36),
            ("alice".to_string(), 60),
        ]
    );
}
