//! Who may do what on the House (HS1 Task 20, ruling R20.5): the engine's
//! link acts for the Loams principal it names, which the House maps to a
//! namespace, scopes and tables; everything else is refused, and every
//! statement is audited with its principal. Over the ClickHouse HTTP interface
//! and `loams.house.v1`, with sealed workers and a `file://` lake.

#![cfg(target_os = "linux")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use common::http::{Response, request, target};
use common::{small, tmp_root};
use loams_cache::RangeCacheConfig;
use loams_house::audit::Memory;
use loams_house::catalog::{HouseCatalog, Lake, LakeRoot, MemCatalog, PrefixGrants};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::identity::{Grants, PrincipalMap, Scopes};
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_store::Store;

const LINK_KEY: &str = "link-secret";

struct House {
    handle: HouseHandle,
    audit: Arc<Memory>,
    _pool: WorkerPool,
}

fn principal(name: &str, scopes: &str, tables: Option<&[&str]>) -> PrincipalMap {
    PrincipalMap {
        principal: name.to_string(),
        namespace: 2,
        scopes: Scopes::parse(scopes).expect("scopes"),
        grants: tables.map(|t| Grants::parse(t).expect("grants")),
    }
}

/// A House with the engine's link user, a plain user `alice`, and principals:
/// `admin` (every scope), `reader` (read), `writer` (read, write), `builder`
/// (read, ddl), `scoped` (every scope on `default.mine` and `sales.*`).
async fn house(test: &str) -> House {
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
            UserMap::link("engine", LINK_KEY),
            UserMap::dev("alice", "a", 2, false),
        ],
        principals: vec![
            principal("tailscale:admin@x.com", "read,write,ddl,admin", None),
            principal("tailscale:reader@x.com", "read", None),
            principal("apikey:writer", "read,write", None),
            principal("email:builder@x.com", "read,ddl", None),
            principal(
                "tailscale:scoped@x.com",
                "read,write,ddl",
                Some(&["default.mine", "sales.*"]),
            ),
        ],
        audit: Some(audit.clone()),
        tmp_dir: tmp_root(&format!("{test}-spool")),
        ..HouseConfig::default()
    };
    let handle = serve_with_lake(config, pool.clone(), lake)
        .await
        .expect("serves");
    House {
        handle,
        audit,
        _pool: pool,
    }
}

/// A statement over the link, acting for `principal` (and `email`).
async fn as_principal(
    addr: SocketAddr,
    principal: &str,
    email: Option<&str>,
    sql: &str,
) -> Response {
    let mut headers = vec![
        ("Authorization".to_string(), format!("Bearer {LINK_KEY}")),
        ("X-Loams-Principal".to_string(), principal.to_string()),
    ];
    if let Some(email) = email {
        headers.push(("X-Loams-Principal-Email".to_string(), email.to_string()));
    }
    let body = sql.as_bytes().to_vec();
    tokio::task::spawn_blocking(move || {
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        request(addr, "POST", "/", &headers, &body)
    })
    .await
    .expect("client")
}

fn code(response: &Response) -> Option<&str> {
    response.header("X-ClickHouse-Exception-Code")
}

async fn ok(addr: SocketAddr, principal: &str, sql: &str) -> String {
    let response = as_principal(addr, principal, None, sql).await;
    assert_eq!(
        response.status,
        200,
        "{principal}: {sql}: {}",
        response.text()
    );
    response.text()
}

async fn refused(addr: SocketAddr, principal: &str, sql: &str, want: &str) -> String {
    let response = as_principal(addr, principal, None, sql).await;
    assert_eq!(
        code(&response),
        Some(want),
        "{principal}: {sql}: {}",
        response.text()
    );
    response.text()
}

const ADMIN: &str = "tailscale:admin@x.com";
const READER: &str = "tailscale:reader@x.com";
const WRITER: &str = "apikey:writer";
const SCOPED: &str = "tailscale:scoped@x.com";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmapped_principals_and_spoofed_headers_get_nothing() {
    let house = house("unmapped").await;
    let addr = house.handle.local_addr();
    // A principal the House does not list: no access at all, not `default`.
    for sql in [
        "SELECT 1",
        "SHOW TABLES",
        "CREATE TABLE t (k Int64) ENGINE = MergeTree ORDER BY k",
    ] {
        let text = refused(addr, "tailscale:eve@x.com", sql, "516").await;
        assert!(text.contains("no House access"), "{text}");
    }
    // The link must name a principal.
    let response = tokio::task::spawn_blocking(move || {
        request(
            addr,
            "POST",
            "/",
            &[("Authorization", "Bearer link-secret")],
            b"SELECT 1",
        )
    })
    .await
    .expect("client");
    assert_eq!(code(&response), Some("516"), "{}", response.text());
    // A wrong link key is a wrong key, whatever the principal.
    let response = tokio::task::spawn_blocking(move || {
        request(
            addr,
            "POST",
            "/",
            &[
                ("Authorization", "Bearer nope"),
                ("X-Loams-Principal", ADMIN),
            ],
            b"SELECT 1",
        )
    })
    .await
    .expect("client");
    assert_eq!(code(&response), Some("516"));
    // A plain user may not claim a principal.
    let response = tokio::task::spawn_blocking(move || {
        request(
            addr,
            "POST",
            &target(&[("user", "alice"), ("password", "a")]),
            &[("X-Loams-Principal", ADMIN)],
            b"SELECT 1",
        )
    })
    .await
    .expect("client");
    assert_eq!(code(&response), Some("516"), "{}", response.text());
    // Mapped by verified email.
    let response = as_principal(addr, "user:b1", Some("builder@x.com"), "SELECT 1").await;
    assert_eq!(response.status, 200, "{}", response.text());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scopes_are_enforced_and_cannot_be_escalated() {
    let house = house("scopes").await;
    let addr = house.handle.local_addr();
    ok(
        addr,
        ADMIN,
        "CREATE TABLE t (k Int64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(addr, ADMIN, "INSERT INTO t VALUES (1)").await;

    // read: queries only.
    assert_eq!(ok(addr, READER, "SELECT count() FROM t").await, "1\n");
    for sql in [
        "INSERT INTO t VALUES (2)",
        "CREATE TABLE r (k Int64) ENGINE = MergeTree ORDER BY k",
        "DROP TABLE t",
        "TRUNCATE TABLE t",
        "ALTER TABLE t ADD COLUMN x UInt8",
        "RENAME TABLE t TO t2",
        "CREATE DATABASE d",
        "OPTIMIZE TABLE t FINAL",
    ] {
        refused(addr, READER, sql, "497").await;
    }
    // Settings cannot lift a scope: readonly = 0 is still a reader's request.
    let response = as_principal(
        addr,
        READER,
        None,
        "INSERT INTO t SETTINGS readonly = 0 VALUES (3)",
    )
    .await;
    assert_eq!(code(&response), Some("497"), "{}", response.text());

    // write: rows, not tables.
    ok(addr, WRITER, "INSERT INTO t VALUES (2)").await;
    refused(addr, WRITER, "DROP TABLE t", "497").await;
    refused(
        addr,
        WRITER,
        "CREATE TABLE c ENGINE = MergeTree ORDER BY k AS SELECT 1 AS k",
        "497",
    )
    .await;
    // ddl without write: tables, not rows (CTAS needs both).
    let builder = "email:builder@x.com";
    ok(
        addr,
        builder,
        "CREATE TABLE b (k Int64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    refused(addr, builder, "INSERT INTO b VALUES (1)", "497").await;
    refused(
        addr,
        builder,
        "CREATE TABLE c ENGINE = MergeTree ORDER BY k AS SELECT 1 AS k",
        "497",
    )
    .await;
    assert_eq!(ok(addr, READER, "SELECT count() FROM t").await, "2\n");
    assert_eq!(ok(addr, READER, "EXISTS TABLE c").await, "0\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tables_outside_the_grants_are_neither_read_nor_written_nor_listed() {
    let house = house("grants").await;
    let addr = house.handle.local_addr();
    for sql in [
        "CREATE DATABASE sales",
        "CREATE TABLE mine (k Int64) ENGINE = MergeTree ORDER BY k",
        "CREATE TABLE secret (k Int64, s String) ENGINE = MergeTree ORDER BY k",
        "CREATE TABLE sales.orders (k Int64) ENGINE = MergeTree ORDER BY k",
        "INSERT INTO secret VALUES (1, 'classified')",
    ] {
        ok(addr, ADMIN, sql).await;
    }
    // Granted: default.mine and every table of sales.
    ok(addr, SCOPED, "INSERT INTO mine VALUES (1)").await;
    ok(addr, SCOPED, "INSERT INTO sales.orders SELECT k FROM mine").await;
    ok(
        addr,
        SCOPED,
        "CREATE TABLE sales.more (k Int64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    assert_eq!(
        ok(addr, SCOPED, "SELECT count() FROM sales.orders").await,
        "1\n"
    );

    // Not granted: default.secret, read directly, in a join, a subquery, an
    // INSERT … SELECT, or through merge().
    for sql in [
        "SELECT * FROM secret",
        "SELECT * FROM mine JOIN default.secret USING (k)",
        "SELECT count() FROM (SELECT k FROM secret)",
        "INSERT INTO mine SELECT k FROM secret",
        "INSERT INTO secret VALUES (2, 'x')",
        "DROP TABLE secret",
        "TRUNCATE TABLE secret",
        "ALTER TABLE secret ADD COLUMN x UInt8",
        "RENAME TABLE secret TO mine2",
        "RENAME TABLE mine TO secret2",
        "CREATE TABLE stolen ENGINE = MergeTree ORDER BY k AS SELECT 1 AS k",
        "DROP DATABASE sales2",
    ] {
        refused(addr, SCOPED, sql, "497").await;
    }
    let merged = ok(addr, SCOPED, "SELECT count() FROM merge('default', '.*')").await;
    assert_eq!(merged, "1\n", "merge() reads only the granted table");
    // Hidden from DESCRIBE, SHOW CREATE, EXISTS and system tables.
    refused(addr, SCOPED, "DESCRIBE TABLE secret", "60").await;
    refused(addr, SCOPED, "SHOW CREATE TABLE secret", "60").await;
    assert_eq!(ok(addr, SCOPED, "EXISTS TABLE secret").await, "0\n");
    let listed = ok(
        addr,
        SCOPED,
        "SELECT database, name FROM system.tables WHERE database IN ('default', 'sales') ORDER BY 1, 2",
    )
    .await;
    assert_eq!(listed, "default\tmine\nsales\tmore\nsales\torders\n");
    let columns = ok(
        addr,
        SCOPED,
        "SELECT DISTINCT table FROM system.columns WHERE table = 'secret'",
    )
    .await;
    assert_eq!(columns, "");
    // The admin, with no grants list, still sees everything.
    assert_eq!(
        ok(addr, ADMIN, "SELECT s FROM secret").await,
        "classified\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_statement_is_audited_with_its_principal() {
    let house = house("audit").await;
    let addr = house.handle.local_addr();
    ok(addr, ADMIN, "SELECT 1").await;
    refused(addr, READER, "DROP TABLE nothing", "497").await;
    refused(
        addr,
        "tailscale:eve@x.com",
        "SELECT 1 AS password_is_hunter2",
        "516",
    )
    .await;
    let events = house.audit.events();
    let find = |principal: &str| {
        events
            .iter()
            .find(|e| e.principal.as_deref() == Some(principal))
            .unwrap_or_else(|| panic!("no audit record for {principal}: {events:?}"))
            .clone()
    };
    let admin = find(ADMIN);
    assert_eq!((admin.code, admin.statement.as_deref()), (0, Some("Query")));
    assert_eq!(admin.namespace, Some(2));
    assert_eq!(admin.user, ADMIN);
    let reader = find(READER);
    assert_eq!(
        (reader.code, reader.statement.as_deref()),
        (497, Some("Drop"))
    );
    // A refused sign-in is recorded with the principal it claimed, and the
    // statement's text is never in the audit.
    let eve = find("tailscale:eve@x.com");
    assert_eq!(eve.code, 516);
    let all = format!("{events:?}");
    assert!(!all.contains("hunter2"), "{all}");
}
