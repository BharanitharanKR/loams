//! Lake writes (HS1 Task 12, §49 §6.1), over the ClickHouse HTTP interface with
//! sealed worker processes, a `file://` bucket shared by the catalog,
//! `house-cache` and the front, as `loams-fabric house` runs them.
//!
//! Rows are written with `INSERT` and read back with `SELECT` (HS1 Task 11's
//! reads); what reached the bucket and the catalog is checked directly.

#![cfg(target_os = "linux")]

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common::http::{Response, request, target};
use common::{small, tmp_root};
use loams_cache::RangeCacheConfig;
use loams_house::catalog::{
    CatalogError, HouseCatalog, Lake, LakeRoot, LakeTable, MemCatalog, PrefixGrants,
};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_house_lake::spec::TableMetadata;
use loams_house_lake::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use loams_store::Store;

const ALICE: (&str, &str) = ("alice", "a");
const READER: (&str, &str) = ("reader", "r");

/// A catalog whose next `fail` commits fail as the catalog being unreachable,
/// after (`landed`) or before applying them.
#[derive(Debug)]
struct Flaky {
    inner: Arc<MemCatalog>,
    fail: AtomicU32,
    landed: AtomicBool,
    commits: AtomicU32,
}

#[async_trait]
impl HouseCatalog for Flaky {
    async fn list_databases(
        &self,
        parent: &NamespaceIdent,
    ) -> Result<Vec<NamespaceIdent>, CatalogError> {
        self.inner.list_databases(parent).await
    }
    async fn create_database(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<(), CatalogError> {
        self.inner.create_database(namespace, properties).await
    }
    async fn database_properties(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<HashMap<String, String>, CatalogError> {
        self.inner.database_properties(namespace).await
    }
    async fn drop_database(&self, namespace: &NamespaceIdent) -> Result<(), CatalogError> {
        self.inner.drop_database(namespace).await
    }
    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>, CatalogError> {
        self.inner.list_tables(namespace).await
    }
    async fn load_table(&self, table: &TableIdent) -> Result<LakeTable, CatalogError> {
        self.inner.load_table(table).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<LakeTable, CatalogError> {
        self.inner.create_table(namespace, creation).await
    }
    async fn drop_table(&self, table: &TableIdent) -> Result<(), CatalogError> {
        self.inner.drop_table(table).await
    }
    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> Result<(), CatalogError> {
        self.inner.rename_table(from, to).await
    }
    async fn commit(
        &self,
        table: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<LakeTable, CatalogError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let failing = self
            .fail
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if !failing {
            return self.inner.commit(table, requirements, updates).await;
        }
        if self.landed.load(Ordering::SeqCst) {
            self.inner.commit(table, requirements, updates).await?;
        }
        Err(CatalogError::Unavailable(
            "injected: the catalog timed out".to_string(),
        ))
    }
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: &str,
    ) -> Result<LakeTable, CatalogError> {
        self.inner.register_table(table, metadata_location).await
    }
}

struct Lakehouse {
    handle: HouseHandle,
    catalog: Arc<MemCatalog>,
    flaky: Arc<Flaky>,
    store: Store,
    bucket: std::path::PathBuf,
    pool: WorkerPool,
    proxy: HouseCache,
}

impl Lakehouse {
    fn addr(&self) -> SocketAddr {
        self.handle.local_addr()
    }
}

/// A House whose catalog, `house-cache` and front share one `file://` bucket.
/// `alice` (namespace 2) writes; `reader` (namespace 2) is read-only.
async fn lakehouse_with(test: &str, edit: impl FnOnce(&mut HouseConfig)) -> Lakehouse {
    let dir = tmp_root(&format!("{test}-bucket"));
    let url = format!("file://{}", dir.display());
    let store = Store::from_url(&url, Vec::<(String, String)>::new()).expect("store");
    let catalog = Arc::new(MemCatalog::new(store.clone(), LakeRoot::local()));
    let flaky = Arc::new(Flaky {
        inner: catalog.clone(),
        fail: AtomicU32::new(0),
        landed: AtomicBool::new(false),
        commits: AtomicU32::new(0),
    });
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
        Lake::new(flaky.clone() as Arc<dyn HouseCatalog>, LakeRoot::local())
            .with_store(store.clone())
            .with_grants(Arc::new(proxy.clone()) as Arc<dyn PrefixGrants>),
    );
    let mut config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![
            UserMap::dev("alice", "a", 2, false),
            UserMap::dev("reader", "r", 2, true),
        ],
        tmp_dir: tmp_root(&format!("{test}-spool")),
        // Compaction is `compact.rs`'s: it would change these tests' files.
        compaction_interval: Duration::ZERO,
        ..HouseConfig::default()
    };
    edit(&mut config);
    let handle = serve_with_lake(config, pool.clone(), lake)
        .await
        .expect("serves");
    Lakehouse {
        handle,
        catalog,
        flaky,
        store,
        bucket: dir,
        pool,
        proxy,
    }
}

async fn lakehouse(test: &str) -> Lakehouse {
    lakehouse_with(test, |_| {}).await
}

/// `POST /?query=<query>` with `body` (an `INSERT`'s data, or a statement).
async fn post(
    addr: SocketAddr,
    user: (&str, &str),
    query: Option<&str>,
    body: &[u8],
    params: &[(&str, &str)],
) -> Response {
    let mut all = vec![("user", user.0), ("password", user.1)];
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

async fn ok(addr: SocketAddr, sql: &str) -> String {
    let response = post(addr, ALICE, None, sql.as_bytes(), &[]).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response.text()
}

async fn bytes_of(addr: SocketAddr, sql: &str) -> Vec<u8> {
    let response = post(addr, ALICE, None, sql.as_bytes(), &[]).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response.body
}

/// An `INSERT` with its data in the body; the response.
async fn insert(addr: SocketAddr, query: &str, data: &[u8], params: &[(&str, &str)]) -> Response {
    post(addr, ALICE, Some(query), data, params).await
}

async fn inserted(addr: SocketAddr, query: &str, data: &[u8], params: &[(&str, &str)]) -> Response {
    let response = insert(addr, query, data, params).await;
    assert_eq!(response.status, 200, "{query}: {}", response.text());
    response
}

fn written_rows(response: &Response) -> u64 {
    let summary: serde_json::Value =
        serde_json::from_str(response.header("X-ClickHouse-Summary").expect("summary"))
            .expect("json");
    summary["written_rows"]
        .as_str()
        .and_then(|v| v.parse().ok())
        .expect("written_rows")
}

async fn code(addr: SocketAddr, user: (&str, &str), sql: &str) -> (Option<String>, String) {
    let response = post(addr, user, None, sql.as_bytes(), &[]).await;
    (
        response
            .header("X-ClickHouse-Exception-Code")
            .map(str::to_string),
        response.text(),
    )
}

async fn refused(addr: SocketAddr, sql: &str, want: &str) -> String {
    let (code, text) = code(addr, ALICE, sql).await;
    assert_eq!(code.as_deref(), Some(want), "{sql}: {text}");
    text
}

async fn metadata(house: &Lakehouse, database: &str, name: &str) -> TableMetadata {
    house
        .catalog
        .load_table(&TableIdent::new(
            Lake::namespace(2, database),
            name.to_string(),
        ))
        .await
        .unwrap_or_else(|err| panic!("{database}.{name}: {err}"))
        .metadata
}

fn snapshots(metadata: &TableMetadata) -> usize {
    metadata.snapshots().count()
}

/// Every file under the bucket whose path contains `part`.
fn files_under(house: &Lakehouse, part: &str) -> Vec<String> {
    fn walk(dir: &std::path::Path, out: &mut Vec<String>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, out);
                } else {
                    out.push(path.display().to_string());
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(&house.bucket, &mut out);
    out.retain(|p| p.contains(part));
    out.sort();
    out
}

const KV: &str = "CREATE TABLE t (k Int64, s String) ENGINE = MergeTree ORDER BY k";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_values_lands_rows_after_the_commit() {
    let house = lakehouse("values").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let response = inserted(addr, "INSERT INTO t VALUES (2, 'b'), (1, 'a')", b"", &[]).await;
    assert_eq!(written_rows(&response), 2);
    let table = metadata(&house, "default", "t").await;
    let snapshot = table.current_snapshot().expect("a snapshot").snapshot_id();
    assert_eq!(
        response.header("X-Loams-Snapshot-Id"),
        Some(format!("default.t={snapshot}").as_str())
    );
    assert_eq!(
        ok(addr, "SELECT k, s FROM t ORDER BY k").await,
        "1\ta\n2\tb\n"
    );

    // The tuples in the body, as ClickHouse takes them.
    inserted(addr, "INSERT INTO t VALUES", b"(3, 'c')", &[]).await;
    // And in the statement text, in the body.
    let response = post(
        addr,
        ALICE,
        None,
        b"INSERT INTO default.t (s, k) VALUES ('d', 4)",
        &[],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(ok(addr, "SELECT count(), sum(k) FROM t").await, "4\t10\n");

    // The data files are Parquet under the table's own data directory, written
    // by the front.
    let files = files_under(&house, "/data/");
    assert_eq!(files.len(), 3, "{files:?}");
    assert!(
        files
            .iter()
            .all(|f| f.ends_with(".parquet") && f.contains("/ns/2/house/default/t-"))
    );
    assert_eq!(snapshots(&metadata(&house, "default", "t").await), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_every_declared_format_lands_identical_rows() {
    let house = lakehouse("formats").await;
    let addr = house.addr();
    ok(addr, KV).await;
    inserted(
        addr,
        "INSERT INTO t SELECT number, concat('s', toString(number)) FROM numbers(100000)",
        b"",
        &[],
    )
    .await;
    let want = ok(addr, "SELECT k, s FROM t ORDER BY k FORMAT TSV").await;
    assert_eq!(want.lines().count(), 100_000);
    // VALUES, JSONEachRow, CSV, TSV, RowBinary, Native, Parquet, ArrowStream
    // (FL2 Task 6's list): each body is what the House itself answers.
    for format in [
        "Values",
        "JSONEachRow",
        "CSV",
        "TSV",
        "RowBinary",
        "Native",
        "Parquet",
        "ArrowStream",
    ] {
        let name = format!("t_{}", format.to_lowercase());
        ok(addr, &KV.replace("TABLE t ", &format!("TABLE {name} "))).await;
        let body = bytes_of(
            addr,
            &format!("SELECT k, s FROM t ORDER BY k FORMAT {format}"),
        )
        .await;
        let response = inserted(
            addr,
            &format!("INSERT INTO {name} FORMAT {format}"),
            &body,
            &[],
        )
        .await;
        assert_eq!(written_rows(&response), 100_000, "{format}");
        assert_eq!(
            ok(
                addr,
                &format!("SELECT k, s FROM {name} ORDER BY k FORMAT TSV")
            )
            .await,
            want,
            "{format}"
        );
    }
}

/// The Tier 1 types of HS1 Task 10's table, written and read back: what
/// `values()` makes of the same rows is what the lake table answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn type_mapping_roundtrip_through_insert() {
    let house = lakehouse("types").await;
    let addr = house.addr();
    let columns = "i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, \
                   u64 UInt64, f32 Float32, f64 Float64, d Decimal(18, 4), d38 Decimal(38, 10), \
                   b Bool, s String, fs FixedString(3), dt Date, d32 Date32, ts DateTime, \
                   tsz DateTime('Asia/Kolkata'), t64 DateTime64(3), t64z DateTime64(6, 'UTC'), \
                   u UUID, ip4 IPv4, ip6 IPv6, e Enum8('x' = 1, 'y' = 2), lc LowCardinality(String), \
                   n Nullable(Int32), ns Nullable(String), arr Array(Int32), \
                   an Array(Nullable(String)), m Map(String, Int64), tup Tuple(a Int32, b String), \
                   tu Tuple(Int32, String)";
    let rows = "(-128, -32768, -2147483648, -9223372036854775808, 255, 65535, 4294967295, \
                18446744073709551615, 1.5, -2.25, 12345678901234.5678, \
                1234567890123456789012345678.0123456789, true, 'héllo\\tworld', 'abc', \
                '2149-06-06', '1900-01-01', '2026-10-13 10:11:12', '2026-10-13 10:11:12', \
                '2026-10-13 10:11:12.345', '2026-10-13 10:11:12.123456', \
                '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '10.0.0.1', '2001:db8::1', 'y', 'lc', \
                NULL, NULL, [1, 2, 3], ['a', NULL], {'k1': 1, 'k2': 2}, (7, 'seven'), (8, 'eight')), \
               (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, '', '', '1970-01-01', '1970-01-01', \
                '1970-01-01 00:00:00', '1970-01-01 05:30:00', '1970-01-01 00:00:00.000', \
                '1970-01-01 00:00:00.000000', '00000000-0000-0000-0000-000000000000', '0.0.0.0', '::', \
                'x', '', 42, 'n', [], [], {}, (0, ''), (0, ''))";
    ok(
        addr,
        &format!("CREATE TABLE ty ({columns}) ENGINE = MergeTree ORDER BY i64"),
    )
    .await;
    let response = post(
        addr,
        ALICE,
        None,
        format!("INSERT INTO ty VALUES {rows}").as_bytes(),
        &[],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    // The same rows, parsed by `format()` in the worker, never stored.
    let parsed = format!(
        "format(Values, '{}', $${rows}$$)",
        columns.replace('\'', "\\'")
    );
    let want = ok(
        addr,
        &format!("SELECT * FROM {parsed} ORDER BY i64 FORMAT TSV"),
    )
    .await;
    assert_eq!(want.lines().count(), 2, "{want}");
    assert_eq!(
        ok(addr, "SELECT * FROM ty ORDER BY i64 FORMAT TSV").await,
        want
    );
    let names: Vec<String> =
        "i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 d d38 b s fs dt d32 ts tsz t64 \
                               t64z u ip4 ip6 e lc n ns arr an m tup tu"
            .split_whitespace()
            .map(|n| format!("toTypeName({n})"))
            .collect();
    assert_eq!(names.len(), 33);
    let types = |from: &str| format!("SELECT {} FROM {from} LIMIT 1 FORMAT TSV", names.join(", "));
    assert_eq!(
        ok(addr, &types("ty")).await,
        ok(addr, &types(&parsed)).await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn defaults_and_column_lists_apply_as_in_clickhouse() {
    let house = lakehouse("defaults").await;
    let addr = house.addr();
    ok(
        addr,
        "CREATE TABLE d (k Int64, s String DEFAULT concat('s', toString(k)), \
         m UInt32 MATERIALIZED k * 2, n Nullable(Int32)) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    inserted(addr, "INSERT INTO d (k) VALUES", b"(1)", &[]).await;
    inserted(
        addr,
        "INSERT INTO d FORMAT JSONEachRow",
        b"{\"k\": 2}\n{\"k\": 3, \"s\": \"x\", \"n\": 5}\n",
        &[],
    )
    .await;
    inserted(addr, "INSERT INTO d (n, k) FORMAT CSV", b"7,4\n", &[]).await;
    assert_eq!(
        ok(addr, "SELECT k, s, m, n FROM d ORDER BY k").await,
        "1\ts1\t2\t\\N\n2\ts2\t4\t\\N\n3\tx\t6\t5\n4\ts4\t8\t7\n"
    );
    // What ClickHouse refuses, chDB refuses here: a MATERIALIZED column named,
    // a column the table does not have, a value that does not parse.
    let (code, text) = code(addr, ALICE, "INSERT INTO d (k, m) VALUES (9, 9)").await;
    assert!(code.is_some(), "{text}");
    refused(addr, "INSERT INTO d (k, nope) VALUES (9, 9)", "16").await;
    let response = insert(addr, "INSERT INTO d (k) FORMAT CSV", b"notanumber\n", &[]).await;
    assert_ne!(response.status, 200, "{}", response.text());
    // Nothing of those landed.
    assert_eq!(ok(addr, "SELECT count() FROM d").await, "4\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_reads_lake_tables_and_table_functions() {
    let house = lakehouse("select").await;
    let addr = house.addr();
    ok(addr, KV).await;
    ok(addr, &KV.replace("TABLE t ", "TABLE t2 ")).await;
    inserted(
        addr,
        "INSERT INTO t SELECT number, toString(number) FROM numbers(10)",
        b"",
        &[],
    )
    .await;
    // A lake table read through its pinned view, into another.
    let response = post(
        addr,
        ALICE,
        None,
        b"INSERT INTO t2 (s, k) WITH 5 AS f SELECT s, k * f FROM t WHERE k < 4",
        &[],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(written_rows(&response), 4);
    assert_eq!(
        ok(addr, "SELECT k, s FROM t2 ORDER BY k").await,
        "0\t0\n5\t1\n10\t2\n15\t3\n"
    );
    // Host functions answer the House's values, as in any query.
    ok(
        addr,
        "CREATE TABLE h (k Int64, s String) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    inserted(addr, "INSERT INTO h SELECT 1, hostName()", b"", &[]).await;
    assert_eq!(ok(addr, "SELECT s FROM h").await, "loams-house\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_paths_keep_the_deny_list_and_the_grants() {
    let house = lakehouse("deny").await;
    let addr = house.addr();
    ok(addr, KV).await;
    // A source in the query of an INSERT … SELECT, anywhere in it.
    for sql in [
        "INSERT INTO t SELECT 1, line FROM file('/etc/passwd', 'LineAsString', 'line String')",
        "INSERT INTO t SELECT 1, * FROM url('http://127.0.0.1:1/x', 'LineAsString', 'l String')",
        "INSERT INTO t SELECT 1, toString(x) FROM (SELECT * FROM s3('http://127.0.0.1:1/b/k', 'CSV', 'x UInt8'))",
        "INSERT INTO t WITH (SELECT count() FROM remote('127.0.0.1', system.one)) AS c SELECT c, ''",
        "INSERT INTO t SELECT 1, toString(x) FROM executable('cat', 'TSV', 'x UInt8')",
    ] {
        refused(addr, sql, "344").await;
    }
    // A denied setting in the statement's own SETTINGS, or the request's.
    refused(
        addr,
        "INSERT INTO t SETTINGS format_schema = 'x' VALUES (1, 'a')",
        "164",
    )
    .await;
    let response = insert(
        addr,
        "INSERT INTO t FORMAT CSV",
        b"1,a\n",
        &[("user_files_path", "/")],
    )
    .await;
    assert_eq!(response.header("X-ClickHouse-Exception-Code"), Some("164"));
    // A read-only user, and GET, write nothing.
    let (code_ro, text) = code(addr, READER, "INSERT INTO t VALUES (1, 'a')").await;
    assert_eq!(code_ro.as_deref(), Some("164"), "{text}");
    let get = tokio::task::spawn_blocking(move || {
        request(
            addr,
            "GET",
            &target(&[
                ("user", "alice"),
                ("password", "a"),
                ("query", "INSERT INTO t VALUES (1, 'a')"),
            ]),
            &[],
            b"",
        )
    })
    .await
    .expect("client");
    assert_eq!(get.header("X-ClickHouse-Exception-Code"), Some("164"));
    // An unknown format is refused before any worker parses it.
    let response = insert(addr, "INSERT INTO t FORMAT `CSV; DROP`", b"1,a\n", &[]).await;
    assert_ne!(response.status, 200, "{}", response.text());
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "0\n");
    assert!(files_under(&house, "/data/").is_empty());
}

/// Catalog text in a table's properties — a `DEFAULT` expression, the sorting
/// key — reaches the worker only printed again from its tree (R11.11), on the
/// write path too: an injection is `497` and runs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_expressions_are_reprinted_on_insert() {
    let house = lakehouse("reprint").await;
    let addr = house.addr();
    ok(
        addr,
        "CREATE TABLE e (k Int64, s String DEFAULT 'x') ENGINE = MergeTree ORDER BY k",
    )
    .await;
    let ident = TableIdent::new(Lake::namespace(2, "default"), "e".to_string());
    let table = house.catalog.load_table(&ident).await.expect("table");
    let types = table.metadata.properties()["loams.ch.types"].clone();
    for (property, from, to) in [
        ("loams.ch.types", "'x'", "1); DROP TABLE e; --"),
        (
            "loams.ch.types",
            "'x'",
            "(SELECT * FROM file('/etc/passwd'))",
        ),
        ("loams.ch.order_by", "k", "k) SETTINGS max_threads = 1 --"),
        (
            "loams.ch.order_by",
            "k",
            "url('http://127.0.0.1:1/', 'CSV', 'x UInt8')",
        ),
    ] {
        let table = house.catalog.load_table(&ident).await.expect("table");
        let current = table.metadata.properties()[property].clone();
        let poisoned = if property == "loams.ch.types" {
            types.replace(
                &serde_json::to_string(from).expect("json")[1..from.len() + 1],
                &serde_json::to_string(to).expect("json")
                    [1..serde_json::to_string(to).expect("json").len() - 1],
            )
        } else {
            to.to_string()
        };
        assert_ne!(current, poisoned, "{to}");
        house
            .catalog
            .commit(
                &ident,
                Vec::new(),
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from([(property.to_string(), poisoned)]),
                }],
            )
            .await
            .expect("poisoned");
        let (code, text) = code(addr, ALICE, "INSERT INTO e (k) VALUES (1)").await;
        assert_eq!(code.as_deref(), Some("497"), "{to}: {text}");
        house
            .catalog
            .commit(
                &ident,
                Vec::new(),
                vec![TableUpdate::SetProperties {
                    updates: HashMap::from([(property.to_string(), current)]),
                }],
            )
            .await
            .expect("restored");
    }
    inserted(addr, "INSERT INTO e (k) VALUES", b"(1)", &[]).await;
    assert_eq!(ok(addr, "SELECT k, s FROM e").await, "1\tx\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_into_temporary_tables_and_functions_stays_chdbs() {
    let house = lakehouse("temporary").await;
    let addr = house.addr();
    let session = [("session_id", "tmp")];
    for sql in [
        "CREATE TEMPORARY TABLE tmp (k Int64) ENGINE = Memory",
        "INSERT INTO tmp VALUES (1), (2)",
    ] {
        let response = post(addr, ALICE, None, sql.as_bytes(), &session).await;
        assert_eq!(response.status, 200, "{sql}: {}", response.text());
    }
    let response = post(addr, ALICE, None, b"SELECT sum(k) FROM tmp", &session).await;
    assert_eq!(response.text(), "3\n");
    // A table that is not the catalog's is chDB's to answer.
    refused(addr, "INSERT INTO nope VALUES (1)", "60").await;
    assert!(files_under(&house, "/data/").is_empty());
}

fn snapshot_of(response: &Response) -> Option<String> {
    response.header("X-Loams-Snapshot-Id").map(str::to_string)
}

/// Rows appear only with their commit: a commit that fails answers an error,
/// and nothing of the insert is visible (its files are orphans for HS1 Task 15).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_only_after_commit() {
    let house = lakehouse("ack").await;
    let addr = house.addr();
    ok(addr, KV).await;
    house.flaky.fail.store(u32::MAX, Ordering::SeqCst);
    let response = insert(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &[]).await;
    assert_eq!(
        response.header("X-ClickHouse-Exception-Code"),
        Some("210"),
        "{}",
        response.text()
    );
    assert!(snapshot_of(&response).is_none());
    house.flaky.fail.store(0, Ordering::SeqCst);
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "0\n");
    assert!(
        metadata(&house, "default", "t")
            .await
            .current_snapshot()
            .is_none()
    );
    assert_eq!(files_under(&house, "/data/").len(), 1, "the orphan");

    // A commit the catalog applied but did not confirm is found by its id on the
    // retry, and lands once.
    house.flaky.landed.store(true, Ordering::SeqCst);
    house.flaky.fail.store(1, Ordering::SeqCst);
    let response = inserted(addr, "INSERT INTO t FORMAT CSV", b"2,b\n", &[]).await;
    assert_eq!(written_rows(&response), 1);
    assert_eq!(ok(addr, "SELECT k, s FROM t").await, "2\tb\n");
    assert_eq!(snapshots(&metadata(&house, "default", "t").await), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_with_same_token_writes_once() {
    let house = lakehouse("token").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let token = [("insert_deduplication_token", "batch-1")];
    let first = inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n2,b\n", &token).await;
    assert_eq!(written_rows(&first), 2);
    // The retry, even with other data (the token is the key), writes nothing and
    // names the snapshot that holds the first.
    let retry = inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n2,b\n3,c\n", &token).await;
    assert_eq!(written_rows(&retry), 0);
    assert_eq!(snapshot_of(&retry), snapshot_of(&first));
    // Concurrent retries of one token: one lands.
    let token2 = [("insert_deduplication_token", "batch-2")];
    let tries: Vec<_> = (0..6)
        .map(|_| {
            tokio::spawn(async move {
                insert(addr, "INSERT INTO t FORMAT CSV", b"9,z\n", &token2).await
            })
        })
        .collect();
    let mut written = 0;
    for task in tries {
        let response = task.await.expect("task");
        assert_eq!(response.status, 200, "{}", response.text());
        written += written_rows(&response);
    }
    assert_eq!(written, 1);
    assert_eq!(ok(addr, "SELECT count(), sum(k) FROM t").await, "3\t12\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_hash_dedup_window() {
    let house = lakehouse("window").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let window = [("loams_dedup_window", "2")];
    let rows = |response: Response| written_rows(&response);
    assert_eq!(
        rows(inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &window).await),
        1
    );
    // The same block again: held.
    assert_eq!(
        rows(inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &window).await),
        0
    );
    // The same rows in another format or column list are another block.
    assert_eq!(
        rows(inserted(addr, "INSERT INTO t FORMAT TSV", b"1\ta\n", &window).await),
        1
    );
    assert_eq!(
        rows(inserted(addr, "INSERT INTO t FORMAT CSV", b"2,b\n", &window).await),
        1
    );
    // Two blocks later, the first has left the window.
    assert_eq!(
        rows(inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &window).await),
        1
    );
    // An INSERT … SELECT's block is its rows.
    let select = "INSERT INTO t SELECT number + 10, 'q' FROM numbers(3)";
    assert_eq!(rows(inserted(addr, select, b"", &[]).await), 3);
    assert_eq!(rows(inserted(addr, select, b"", &[]).await), 0);
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "7\n");
}

/// `TRUNCATE` then the same load writes the rows again (the local e2e seed's
/// reload, 2026-10-18, which left `nyc.yellow_trips` empty): the block's key
/// from before the `TRUNCATE` is not held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_insert_after_truncate_is_written() {
    let house = lakehouse("retruncate").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let load = |addr| async move {
        written_rows(&inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n2,b\n", &[]).await)
    };
    assert_eq!(load(addr).await, 2);
    // Without a TRUNCATE between them, the same block is a duplicate.
    assert_eq!(load(addr).await, 0);
    ok(addr, "TRUNCATE TABLE t").await;
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "0\n");
    assert_eq!(load(addr).await, 2);
    assert_eq!(ok(addr, "SELECT count(), sum(k) FROM t").await, "2\t3\n");
    assert_eq!(load(addr).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_deduplicate_0_writes_twice() {
    let house = lakehouse("nodedup").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let off = [("insert_deduplicate", "0")];
    for _ in 0..2 {
        let response = inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &off).await;
        assert_eq!(written_rows(&response), 1);
    }
    // The statement's own SETTINGS clause too.
    let response = inserted(
        addr,
        "INSERT INTO t SETTINGS insert_deduplicate = 0 FORMAT CSV",
        b"1,a\n",
        &[],
    )
    .await;
    assert_eq!(written_rows(&response), 1);
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "3\n");
}

/// 16 writers × 50 inserts through two fronts on one catalog (so their
/// commits conflict and rebase): every row lands exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commits_all_land() {
    let house = lakehouse("concurrent").await;
    let addr = house.addr();
    ok(addr, KV).await;
    // A second front: its own lake and commit queues, the same catalog and bucket.
    let lake = Arc::new(
        Lake::new(
            house.catalog.clone() as Arc<dyn HouseCatalog>,
            LakeRoot::local(),
        )
        .with_store(house.store.clone())
        .with_grants(Arc::new(house.proxy.clone()) as Arc<dyn PrefixGrants>),
    );
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![UserMap::dev("alice", "a", 2, false)],
        tmp_dir: tmp_root("concurrent-spool-2"),
        ..HouseConfig::default()
    };
    let second = serve_with_lake(config, house.pool.clone(), lake)
        .await
        .expect("serves");
    let fronts = [addr, second.local_addr()];
    let started = Instant::now();
    let writers: Vec<_> = (0..16u64)
        .map(|w| {
            let front = fronts[(w % 2) as usize];
            tokio::spawn(async move {
                for i in 0..50u64 {
                    let k = w * 1000 + i;
                    let body = format!("{k},w{w}\n");
                    let response = insert(
                        front,
                        "INSERT INTO t FORMAT CSV",
                        body.as_bytes(),
                        &[("loams_lake_commit_interval_ms", "0")],
                    )
                    .await;
                    assert_eq!(response.status, 200, "{}", response.text());
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.expect("writer");
    }
    eprintln!("800 inserts in {:?}", started.elapsed());
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        "800\t800\n"
    );
    assert!(house.flaky.commits.load(Ordering::SeqCst) >= 1);
}

/// Inserts arriving within the interval share one commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_commit_coalesces() {
    let house = lakehouse("group").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let interval = [("loams_lake_commit_interval_ms", "2000")];
    inserted(addr, "INSERT INTO t FORMAT CSV", b"0,first\n", &interval).await;
    let started = Instant::now();
    let burst: Vec<_> = (1..=8u64)
        .map(|k| {
            tokio::spawn(async move {
                let body = format!("{k},b\n");
                insert(addr, "INSERT INTO t FORMAT CSV", body.as_bytes(), &interval).await
            })
        })
        .collect();
    let mut ids = std::collections::HashSet::new();
    for task in burst {
        let response = task.await.expect("task");
        assert_eq!(response.status, 200, "{}", response.text());
        ids.insert(snapshot_of(&response));
    }
    let table = metadata(&house, "default", "t").await;
    assert!(snapshots(&table) <= 3, "{} snapshots", snapshots(&table));
    assert!(ids.len() <= 2, "{ids:?}");
    // They waited for the interval after the first commit.
    assert!(
        started.elapsed() >= Duration::from_millis(1000),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "9\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_async_insert_0_acks_early() {
    let house = lakehouse("async").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let interval = ("loams_lake_commit_interval_ms", "4000");
    inserted(addr, "INSERT INTO t FORMAT CSV", b"1,a\n", &[interval]).await;
    let started = Instant::now();
    let response = inserted(
        addr,
        "INSERT INTO t FORMAT CSV",
        b"2,b\n",
        &[
            interval,
            ("async_insert", "1"),
            ("wait_for_async_insert", "0"),
        ],
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(3000),
        "{:?}",
        started.elapsed()
    );
    assert!(
        snapshot_of(&response).is_none(),
        "answered before its commit"
    );
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "1\n");
    assert!(
        common::eventually(Duration::from_secs(15), || {
            std::thread::spawn(move || {
                let r = request(
                    addr,
                    "POST",
                    &target(&[("user", "alice"), ("password", "a")]),
                    &[],
                    b"SELECT count() FROM t",
                );
                r.text() == "2\n"
            })
            .join()
            .unwrap_or(false)
        })
        .await,
        "the queued rows were committed"
    );
}

/// One statement writes at most `lake_insert_max_bytes` (R12.2): past it the
/// INSERT is refused and nothing lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_over_the_cap_is_refused() {
    let house = lakehouse_with("cap", |config| config.lake_insert_max_bytes = 64 * 1024).await;
    let addr = house.addr();
    ok(addr, KV).await;
    refused(
        addr,
        "INSERT INTO t SELECT number, toString(number) FROM numbers(100000)",
        "36",
    )
    .await;
    inserted(
        addr,
        "INSERT INTO t SELECT number, toString(number) FROM numbers(100)",
        b"",
        &[],
    )
    .await;
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "100\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_split_files() {
    let house = lakehouse("partitions").await;
    let addr = house.addr();
    ok(
        addr,
        "CREATE TABLE p (k Int64, s String, d Date) ENGINE = MergeTree \
         PARTITION BY d ORDER BY k",
    )
    .await;
    inserted(
        addr,
        "INSERT INTO p SELECT number, toString(number), toDate('2026-10-01') + number % 3 FROM numbers(30)",
        b"",
        &[],
    )
    .await;
    let files = files_under(&house, "/data/");
    assert_eq!(files.len(), 3, "one file per partition value: {files:?}");
    let table = metadata(&house, "default", "p").await;
    let summary = &table
        .current_snapshot()
        .expect("snapshot")
        .summary()
        .additional_properties;
    assert_eq!(
        summary.get("added-data-files").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        ok(addr, "SELECT d, count() FROM p GROUP BY d ORDER BY d").await,
        "2026-10-01\t10\n2026-10-02\t10\n2026-10-03\t10\n"
    );
    // And by size: a 1 MiB target rolls files.
    ok(addr, KV).await;
    inserted(
        addr,
        "INSERT INTO t SELECT number, randomPrintableASCII(64) FROM numbers(100000)",
        b"",
        &[("loams_lake_target_file_bytes", "1048576")],
    )
    .await;
    let rolled = files_under(&house, "/t-")
        .into_iter()
        .filter(|f| f.contains("/data/"))
        .count();
    assert!(rolled > 1, "{rolled} files");
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "100000\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_as_select_and_truncate() {
    let house = lakehouse("ctas").await;
    let addr = house.addr();
    ok(addr, KV).await;
    inserted(
        addr,
        "INSERT INTO t SELECT number, toString(number) FROM numbers(5)",
        b"",
        &[],
    )
    .await;
    // Columns from the query (read through the pinned view), engine and keys
    // as written.
    let response = post(
        addr,
        ALICE,
        None,
        b"CREATE TABLE c ENGINE = MergeTree ORDER BY k AS SELECT k, s, k * 2 AS twice FROM t WHERE k > 1",
        &[],
    )
    .await;
    assert_eq!(response.status, 200, "{}", response.text());
    assert_eq!(written_rows(&response), 3);
    assert_eq!(
        ok(
            addr,
            "SELECT name, type FROM system.columns WHERE table = 'c' ORDER BY position"
        )
        .await,
        "k\tInt64\ns\tString\ntwice\tInt64\n"
    );
    assert_eq!(ok(addr, "SELECT sum(twice) FROM c").await, "18\n");
    // Named columns, and EMPTY AS.
    ok(
        addr,
        "CREATE TABLE c2 (k Int64, s String) ENGINE = MergeTree ORDER BY k AS SELECT 7, 'x'",
    )
    .await;
    assert_eq!(ok(addr, "SELECT k, s FROM c2").await, "7\tx\n");
    ok(
        addr,
        "CREATE TABLE c3 ENGINE = MergeTree ORDER BY k EMPTY AS SELECT k FROM t",
    )
    .await;
    assert_eq!(ok(addr, "SELECT count() FROM c3").await, "0\n");
    // An existing name: 57, unless IF NOT EXISTS (which writes nothing).
    refused(
        addr,
        "CREATE TABLE c ENGINE = MergeTree ORDER BY k AS SELECT 1 AS k",
        "57",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE IF NOT EXISTS c ENGINE = MergeTree ORDER BY k AS SELECT 1 AS k",
    )
    .await;
    assert_eq!(ok(addr, "SELECT count() FROM c").await, "3\n");
    // The query is held to the deny list, and its sources to the grants.
    refused(
        addr,
        "CREATE TABLE bad ENGINE = MergeTree ORDER BY l AS SELECT * FROM file('/etc/passwd', 'LineAsString', 'l String')",
        "344",
    )
    .await;
    assert_eq!(ok(addr, "EXISTS TABLE bad").await, "0\n");

    // TRUNCATE: a snapshot with no files; history stays for time travel.
    let before = metadata(&house, "default", "t")
        .await
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    ok(addr, "TRUNCATE TABLE t").await;
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "0\n");
    let table = metadata(&house, "default", "t").await;
    assert_eq!(
        table
            .current_snapshot()
            .expect("snapshot")
            .summary()
            .operation
            .as_str(),
        "delete"
    );
    let pinned = post(
        addr,
        ALICE,
        None,
        b"SELECT count() FROM t",
        &[("loams_snapshot_id", &before.to_string())],
    )
    .await;
    assert_eq!(pinned.text(), "5\n");
    ok(addr, "TRUNCATE TABLE IF EXISTS nope").await;
    inserted(addr, "INSERT INTO t VALUES (9, 'z')", b"", &[]).await;
    assert_eq!(ok(addr, "SELECT k FROM t").await, "9\n");
    let (code_ro, text) = code(addr, READER, "TRUNCATE TABLE t").await;
    assert_eq!(code_ro.as_deref(), Some("164"), "{text}");
}

/// `loams.house.v1.ExecuteQuery` writes lake tables as HTTP does (§49 §18.2):
/// the summary carries `written_rows` and the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_insert_answers_written_rows_and_snapshot() {
    use base64::Engine as _;
    use connectrpc::client::{ClientConfig, HttpClient};
    use loams_house_proto::loams::house::v1 as pb;
    use loams_house_proto::loams::house::v1::__buffa::oneof::execute_query_response;
    use loams_house_proto::loams::house::v1::HouseServiceClient;

    let house = lakehouse("api").await;
    let addr = house.addr();
    ok(addr, KV).await;
    let client = |user: &str, password: &str| {
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        let uri: http::Uri = format!("http://{addr}").parse().expect("uri");
        HouseServiceClient::new(
            HttpClient::plaintext(),
            ClientConfig::new(uri).with_default_header("authorization", format!("Basic {basic}")),
        )
    };
    let alice = client("alice", "a");
    for sql in [
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO t FORMAT CSV\n3,c\n",
        "INSERT INTO t SELECT number + 10, 'n' FROM numbers(2)",
    ] {
        let mut stream = alice
            .execute_query(pb::ExecuteQueryRequest {
                sql: sql.to_string(),
                format: pb::ResultFormat::RESULT_FORMAT_JSON_ROWS.into(),
                read_only: Some(false),
                ..Default::default()
            })
            .await
            .expect("starts");
        let mut summary = None;
        while let Some(message) = stream.message().await.expect("frames") {
            if let Some(execute_query_response::Frame::Summary(s)) =
                message.to_owned_message().frame
            {
                summary = Some(*s);
            }
        }
        let summary = summary.expect("a summary");
        assert!(summary.written_rows >= 1, "{sql}");
        let table = metadata(&house, "default", "t").await;
        assert_eq!(
            summary.snapshot_ids.get("default.t").copied(),
            table.current_snapshot().map(|s| s.snapshot_id()),
            "{sql}"
        );
    }
    assert_eq!(ok(addr, "SELECT count(), sum(k) FROM t").await, "5\t27\n");
    // Without `read_only = false` the API writes nothing.
    let refused = alice
        .execute_query(pb::ExecuteQueryRequest {
            sql: "INSERT INTO t VALUES (4, 'd')".to_string(),
            ..Default::default()
        })
        .await;
    let failed = match refused {
        Err(_) => true,
        Ok(mut stream) => stream.message().await.is_err(),
    };
    assert!(failed, "read_only refuses the write");
    assert_eq!(ok(addr, "SELECT count() FROM t").await, "5\n");
}
