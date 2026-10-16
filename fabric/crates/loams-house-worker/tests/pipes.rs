//! `LoamsStream` pipes and their materialized views (HS1 Task 16, §49 §9): a
//! stream's records land in a lake table exactly once, whatever step a front
//! dies at and however many fronts run them. `S3Queue` pipes (HS1 Task 18) do
//! the same for objects under the namespace's prefix.
//!
//! The engine's stream API is stood in for by [`MemStream`] (the HTTP client
//! itself is `loams_stream`'s unit test); the runner is driven round by
//! round, as each front's timer would.

#![cfg(target_os = "linux")]

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use common::http::{Response, request, target};
use common::{small, tmp_root};
use connectrpc::client::{ClientConfig, HttpClient};
use loams_cache::RangeCacheConfig;
use loams_house::catalog::{HouseCatalog, Lake, LakeRoot, MemCatalog, PrefixGrants};
use loams_house::config::{EngineLink, HouseConfig, UserMap};
use loams_house::errors::{ChError, HouseError};
use loams_house::http::{HouseHandle, serve_with_lake};
use loams_house::pipes::loams_stream::{cloudevent_row, row};
use loams_house::pipes::s3queue::QueueRunner;
use loams_house::pipes::{
    Bounds, Fetched, Hooks, MaterializedView, PipeFeed, PipeRunner, Ran, Step, StreamSource, pipes,
};
use loams_house::{ProcessLauncher, WorkerPool};
use loams_house_cache::{HouseCache, OneStore, StoreOptions, Upstream};
use loams_house_lake::TableIdent;
use loams_house_proto::loams::house::v1 as pb;
use loams_house_proto::loams::house::v1::HouseServiceClient;
use loams_store::Store;

const NS: u64 = 2;

/// A stream in memory: records per partition, by stream name.
#[derive(Debug, Default)]
struct MemStream {
    streams: Mutex<BTreeMap<String, Vec<Vec<Vec<u8>>>>>,
}

impl MemStream {
    fn push(&self, stream: &str, partition: usize, record: &[u8]) {
        let mut streams = self.streams.lock().expect("lock");
        let parts = streams.entry(stream.to_string()).or_default();
        while parts.len() <= partition {
            parts.push(Vec::new());
        }
        parts[partition].push(record.to_vec());
    }
}

#[async_trait]
impl StreamSource for MemStream {
    async fn partitions(&self, namespace: &str, stream: &str) -> Result<Vec<Bounds>, HouseError> {
        assert_eq!(
            namespace, "engine-ns",
            "the operator's mapping names the namespace"
        );
        let streams = self.streams.lock().expect("lock");
        let Some(parts) = streams.get(stream) else {
            return Err(HouseError::from(ChError::bad_arguments(
                "the pipe's stream does not exist in its namespace",
            )));
        };
        Ok(parts
            .iter()
            .enumerate()
            .map(|(p, records)| Bounds {
                partition: p as u32,
                log_start_offset: 0,
                high_watermark: records.len() as u64,
            })
            .collect())
    }

    async fn fetch(
        &self,
        _namespace: &str,
        stream: &str,
        partition: u32,
        offset: u64,
        max_bytes: usize,
        events: bool,
    ) -> Result<Fetched, HouseError> {
        let streams = self.streams.lock().expect("lock");
        let records = &streams[stream][partition as usize];
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut next = offset;
        // Small fetches, so a batch is several of them.
        for record in records.iter().skip(offset as usize).take(3) {
            if bytes > max_bytes {
                break;
            }
            bytes += record.len();
            rows.push(if events {
                cloudevent_row(&serde_json::from_slice(record).expect("an event"))
            } else {
                row(record.clone())
            });
            next += 1;
        }
        Ok(Fetched {
            rows,
            next_offset: next,
            high_watermark: records.len() as u64,
        })
    }
}

struct Lakehouse {
    handle: HouseHandle,
    lake: Arc<Lake>,
    catalog: Arc<MemCatalog>,
    pool: WorkerPool,
    config: HouseConfig,
    stream: Arc<MemStream>,
    store: Store,
}

impl Lakehouse {
    fn runner(&self) -> PipeRunner {
        PipeRunner::new(
            Arc::clone(&self.lake),
            self.pool.clone(),
            self.config.clone(),
            EngineLink {
                url: "http://engine.invalid".to_string(),
                key: None,
                namespaces: BTreeMap::from([(NS, "engine-ns".to_string())]),
            },
            self.stream.clone() as Arc<dyn StreamSource>,
        )
    }

    /// Rounds until a round commits nothing.
    async fn drain(&self, runner: &PipeRunner) {
        for _ in 0..50 {
            let mut busy = false;
            for pipe in pipes(&self.lake, NS).await.expect("pipes") {
                for view in &pipe.views {
                    if let Ran::Committed { .. } = runner.run(NS, &pipe, view).await.expect("run") {
                        busy = true;
                    }
                }
            }
            if !busy {
                return;
            }
        }
        panic!("the pipes never caught up");
    }

    fn queue_runner(&self) -> QueueRunner {
        QueueRunner::new(
            Arc::clone(&self.lake),
            self.pool.clone(),
            self.config.clone(),
        )
    }

    /// The `S3Queue` pipes' rounds until one commits nothing.
    async fn drain_queues(&self, runner: &QueueRunner) {
        for _ in 0..50 {
            let mut busy = false;
            for pipe in pipes(&self.lake, NS).await.expect("pipes") {
                if !matches!(pipe.source, PipeFeed::Queue(_)) {
                    continue;
                }
                for view in &pipe.views {
                    if let Ran::Committed { .. } = runner.run(NS, &pipe, view).await.expect("run") {
                        busy = true;
                    }
                }
            }
            if !busy {
                return;
            }
        }
        panic!("the queues never caught up");
    }

    async fn put(&self, key: &str, body: &str) {
        self.store
            .put(key, bytes::Bytes::from(body.to_string()))
            .await
            .expect("put");
    }

    async fn properties(&self, table: &str) -> std::collections::HashMap<String, String> {
        self.catalog
            .load_table(&TableIdent::new(
                Lake::namespace(NS, "default"),
                table.to_string(),
            ))
            .await
            .expect("table")
            .metadata
            .properties()
            .clone()
    }
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
        Lake::new(catalog.clone() as Arc<dyn HouseCatalog>, LakeRoot::local())
            .with_store(store.clone())
            .with_grants(Arc::new(proxy) as Arc<dyn PrefixGrants>),
    );
    let config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: vec![
            UserMap::dev("alice", "a", NS, false),
            UserMap::dev("reader", "r", NS, true),
        ],
        tmp_dir: tmp_root(&format!("{test}-spool")),
        compaction_interval: Duration::ZERO,
        retention_interval: Duration::ZERO,
        pipe_interval: Duration::ZERO,
        ..HouseConfig::default()
    };
    let handle = serve_with_lake(config.clone(), pool.clone(), lake.clone())
        .await
        .expect("serves");
    Lakehouse {
        handle,
        lake,
        catalog,
        pool,
        config,
        stream: Arc::new(MemStream::default()),
        store,
    }
}

async fn post(addr: SocketAddr, user: (&str, &str), sql: &str) -> Response {
    let target = target(&[("user", user.0), ("password", user.1)]);
    let body = sql.as_bytes().to_vec();
    tokio::task::spawn_blocking(move || request(addr, "POST", &target, &[], &body))
        .await
        .expect("client")
}

async fn ok(addr: SocketAddr, sql: &str) -> String {
    let response = post(addr, ("alice", "a"), sql).await;
    assert_eq!(response.status, 200, "{sql}: {}", response.text());
    response.text()
}

async fn code(addr: SocketAddr, sql: &str) -> Option<String> {
    post(addr, ("alice", "a"), sql)
        .await
        .header("X-ClickHouse-Exception-Code")
        .map(str::to_string)
}

fn order(k: u64) -> Vec<u8> {
    format!("{{\"k\": {k}, \"v\": \"item{k}\", \"extra\": 1}}").into_bytes()
}

async fn orders_pipe(house: &Lakehouse, max_rows: u64) {
    let addr = house.handle.local_addr();
    ok(
        addr,
        &format!(
            "CREATE TABLE q (k UInt64, v String) ENGINE = LoamsStream('orders', 'JSONEachRow') \
             SETTINGS loams_pipe_max_rows = {max_rows}, loams_pipe_max_delay_ms = 1"
        ),
    )
    .await;
    ok(
        addr,
        "CREATE TABLE t (k UInt64, v String, n UInt8 DEFAULT 7) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW mv TO t AS SELECT upper(v) AS v, k FROM q WHERE k % 10 != 9",
    )
    .await;
}

/// `k` for every record pushed, minus the ones the view filters out.
fn expected(records: u64) -> String {
    let kept = (0..records).filter(|k| k % 10 != 9).count();
    format!("{kept}\t{kept}\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipe_lands_every_record_once() {
    let house = lakehouse("pipe-once").await;
    let addr = house.handle.local_addr();
    orders_pipe(&house, 7).await;
    for k in 0..25 {
        house.stream.push("orders", (k % 2) as usize, &order(k));
    }
    let runner = house.runner();
    house.drain(&runner).await;
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        expected(25)
    );
    // The view's columns went in by name; the target's default filled the rest.
    assert_eq!(
        ok(addr, "SELECT k, v, n FROM t WHERE k = 3").await,
        "3\tITEM3\t7\n"
    );
    // The cursor is in the target's properties and in its snapshots' summaries.
    let properties = house.properties("t").await;
    let cursors: Vec<(&String, &String)> = properties
        .iter()
        .filter(|(k, _)| k.starts_with("loams.pipe."))
        .collect();
    assert_eq!(cursors.len(), 2, "{cursors:?}");
    assert!(cursors.iter().any(|(_, v)| *v == "13"));
    assert!(cursors.iter().any(|(_, v)| *v == "12"));

    // More records: only they land.
    for k in 25..40 {
        house.stream.push("orders", (k % 2) as usize, &order(k));
    }
    house.drain(&runner).await;
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        expected(40)
    );

    // A pipe is read only by its views.
    assert_eq!(code(addr, "SELECT * FROM q").await.as_deref(), Some("620"));
    assert_eq!(
        code(addr, "INSERT INTO q VALUES (1, 'x')").await.as_deref(),
        Some("48")
    );
    assert_eq!(
        ok(addr, "SELECT engine FROM system.tables WHERE name = 'q'").await,
        "LoamsStream\n"
    );
    let rows = ok(
        addr,
        "SELECT name, pipe, target, lag_records, committed_records, last_error = '' \
         FROM system.loams_pipes",
    )
    .await;
    assert_eq!(rows, "mv\tdefault.q\tdefault.t\t0\t40\t1\n");

    // Dropped, the view stops; the pipe and the target stay.
    ok(addr, "DROP VIEW mv").await;
    house.stream.push("orders", 0, &order(100));
    house.drain(&runner).await;
    assert_eq!(ok(addr, "SELECT count() FROM t WHERE k = 100").await, "0\n");
    assert_eq!(
        ok(addr, "SELECT count() FROM system.loams_pipes").await,
        "0\n"
    );
}

/// Stops a run at one step, once, as a front that died there.
#[derive(Debug)]
struct DieAt(Step, Mutex<bool>);

#[async_trait]
impl Hooks for DieAt {
    async fn at(&self, step: Step, _view: &MaterializedView) -> Result<(), HouseError> {
        let mut armed = self.1.lock().expect("lock");
        if step == self.0 && *armed {
            *armed = false;
            return Err(HouseError::from(ChError::network_error("the front died")));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipe_restart_is_exactly_once() {
    let house = lakehouse("pipe-restart").await;
    let addr = house.handle.local_addr();
    orders_pipe(&house, 1_000).await;
    let mut pushed = 0u64;
    for step in [
        Step::Fetched,
        Step::Uploaded,
        Step::Written,
        Step::BeforeCommit,
        Step::Committed,
    ] {
        for _ in 0..6 {
            house
                .stream
                .push("orders", (pushed % 2) as usize, &order(pushed));
            pushed += 1;
        }
        // A front dies at `step`; another (or the same, restarted) catches up.
        let dying = house
            .runner()
            .with_hooks(Arc::new(DieAt(step, Mutex::new(true))));
        let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
        assert!(
            dying.run(NS, &pipe, &pipe.views[0]).await.is_err(),
            "{step:?}"
        );
        house.drain(&house.runner()).await;
        assert_eq!(
            ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
            expected(pushed),
            "after dying at {step:?}"
        );
    }
}

/// Holds a run before its commit until it is let go.
#[derive(Debug)]
struct HoldBeforeCommit {
    held: tokio::sync::Notify,
    go: tokio::sync::Notify,
}

#[async_trait]
impl Hooks for HoldBeforeCommit {
    async fn at(&self, step: Step, _view: &MaterializedView) -> Result<(), HouseError> {
        if step == Step::BeforeCommit {
            self.held.notify_one();
            self.go.notified().await;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_runners_one_wins() {
    let house = lakehouse("pipe-race").await;
    let addr = house.handle.local_addr();
    orders_pipe(&house, 1_000).await;
    for k in 0..20 {
        house.stream.push("orders", (k % 2) as usize, &order(k));
    }
    let hold = Arc::new(HoldBeforeCommit {
        held: tokio::sync::Notify::new(),
        go: tokio::sync::Notify::new(),
    });
    let slow = house.runner().with_hooks(hold.clone());
    let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
    let view = pipe.views[0].clone();
    let slow_pipe = pipe.clone();
    let slow_run = tokio::spawn(async move { slow.run(NS, &slow_pipe, &view).await });
    hold.held.notified().await;
    // The other front reads the same records and commits first.
    let fast = house.runner();
    assert!(matches!(
        fast.run(NS, &pipe, &pipe.views[0]).await.expect("fast"),
        Ran::Committed { records: 20, .. }
    ));
    hold.go.notify_one();
    // The slow one's commit finds the cursor moved and gives its batch up.
    assert_eq!(
        slow_run.await.expect("joined").expect("slow"),
        Ran::Abandoned
    );
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        expected(20)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cloudevents_columns_layout() {
    let house = lakehouse("pipe-ce").await;
    let addr = house.handle.local_addr();
    ok(
        addr,
        "CREATE TABLE ce (_ce_id String, _ce_source String, _ce_type String, _ce_subject \
         String, _ce_time String, _ce_data String, _ce_ext Map(String, String)) \
         ENGINE = LoamsStream('events', 'CloudEvents')",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE events (id String, source String, type String, amount UInt32, region \
         String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW ce_mv TO events AS SELECT _ce_id AS id, _ce_source AS \
         source, _ce_type AS type, JSONExtractUInt(_ce_data, 'amount') AS amount, \
         _ce_ext['region'] AS region FROM ce",
    )
    .await;
    for i in 0..4u32 {
        let event = serde_json::json!({
            "specversion": "1.0",
            "id": format!("e{i}"),
            "source": "/shop",
            "type": "order.created",
            "time": "2026-10-16T10:00:00Z",
            "datacontenttype": "application/json",
            "data": {"amount": i * 10},
            "region": "eu",
        });
        house
            .stream
            .push("events", 0, &serde_json::to_vec(&event).expect("json"));
    }
    // A binary-mode event's data arrives base64.
    let binary = serde_json::json!({
        "specversion": "1.0", "id": "b1", "source": "/shop", "type": "order.created",
        "data_base64": base64::engine::general_purpose::STANDARD.encode(b"{\"amount\": 7}"),
    });
    house
        .stream
        .push("events", 0, &serde_json::to_vec(&binary).expect("json"));
    house.drain(&house.runner()).await;
    assert_eq!(
        ok(
            addr,
            "SELECT id, source, type, amount, region FROM events ORDER BY id"
        )
        .await,
        "b1\t/shop\torder.created\t7\t\n\
         e0\t/shop\torder.created\t0\teu\n\
         e1\t/shop\torder.created\t10\teu\n\
         e2\t/shop\torder.created\t20\teu\n\
         e3\t/shop\torder.created\t30\teu\n"
    );

    // A short batch waits for loams_pipe_max_delay_ms (2 s by default) after
    // the last commit.
    house
        .stream
        .push("events", 0, &serde_json::to_vec(&binary).expect("json"));
    let runner = house.runner();
    let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
    assert_eq!(
        runner.run(NS, &pipe, &pipe.views[0]).await.expect("run"),
        Ran::Idle
    );
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        runner.run(NS, &pipe, &pipe.views[0]).await.expect("run"),
        Ran::Committed { records: 1, .. }
    ));
}

/// Lag is reported by `system.loams_pipes` and `ListPipes`; the Prometheus
/// metric of it arrives with the metrics endpoint (HS1 Task 23).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipe_lag_metric() {
    let house = lakehouse("pipe-lag").await;
    let addr = house.handle.local_addr();
    orders_pipe(&house, 4).await;
    for k in 0..10 {
        house.stream.push("orders", 0, &order(k));
    }
    let runner = house.runner();
    let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
    assert!(matches!(
        runner.run(NS, &pipe, &pipe.views[0]).await.expect("run"),
        Ran::Committed { records: 4, .. }
    ));
    assert_eq!(
        ok(
            addr,
            "SELECT lag_records, committed_records FROM system.loams_pipes"
        )
        .await,
        "6\t4\n"
    );
    let basic = base64::engine::general_purpose::STANDARD.encode("alice:a");
    let uri: http::Uri = format!("http://{addr}").parse().expect("uri");
    let client = HouseServiceClient::new(
        HttpClient::plaintext(),
        ClientConfig::new(uri).with_default_header("authorization", format!("Basic {basic}")),
    );
    let listed = client
        .list_pipes(pb::ListPipesRequest::default())
        .await
        .expect("pipes")
        .into_owned();
    assert_eq!(listed.pipes.len(), 1);
    let entry = &listed.pipes[0];
    assert_eq!(entry.name, "default.mv");
    assert_eq!(entry.source, "LoamsStream('orders', 'JSONEachRow')");
    assert_eq!(entry.target, "default.t");
    assert_eq!(entry.lag_records, 6);
    assert!(entry.last_commit_at.as_option().is_some());
    house.drain(&runner).await;
    assert_eq!(
        ok(addr, "SELECT lag_records FROM system.loams_pipes").await,
        "0\n"
    );
    // A missing stream is the view's last error, not a stop of the others.
    ok(
        addr,
        "CREATE TABLE gone (k UInt64) ENGINE = LoamsStream('nope', 'JSONEachRow')",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE sink (k UInt64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW gone_mv TO sink AS SELECT k FROM gone",
    )
    .await;
    runner.round().await;
    let error = ok(
        addr,
        "SELECT last_error FROM system.loams_pipes WHERE name = 'gone_mv'",
    )
    .await;
    assert!(error.contains("does not exist"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipe_and_view_ddl_refusals() {
    let house = lakehouse("pipe-ddl").await;
    let addr = house.handle.local_addr();
    ok(
        addr,
        "CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    for (sql, want) in [
        (
            "CREATE TABLE a (k UInt64) ENGINE = LoamsStream('s', 'Avro')",
            "48",
        ),
        (
            "CREATE TABLE a (k UInt64) ENGINE = LoamsStream('s', 'Native')",
            "73",
        ),
        (
            "CREATE TABLE a (k UInt64) ENGINE = IggyTopic('s', 't', 'JSONEachRow')",
            "48",
        ),
        ("CREATE TABLE a (k UInt64) ENGINE = LoamsStream('s')", "36"),
        (
            "CREATE TABLE a (k UInt64) ENGINE = LoamsStream('s', 'CSV') ORDER BY k",
            "48",
        ),
        (
            "CREATE TABLE a (k UInt64) ENGINE = LoamsStream('s', 'CSV') SETTINGS x = 1",
            "115",
        ),
        // A view reads a pipe or a lake table, never the table it writes.
        ("CREATE MATERIALIZED VIEW v TO t AS SELECT k FROM t", "36"),
    ] {
        assert_eq!(code(addr, sql).await.as_deref(), Some(want), "{sql}");
    }
    ok(
        addr,
        "CREATE TABLE q (k UInt64) ENGINE = LoamsStream('s', 'CSV', 'c1')",
    )
    .await;
    for (sql, want) in [
        (
            "CREATE MATERIALIZED VIEW v TO t POPULATE AS SELECT k FROM q",
            "48",
        ),
        (
            "CREATE MATERIALIZED VIEW v ENGINE = MergeTree ORDER BY k POPULATE AS SELECT k \
             FROM q",
            "48",
        ),
        ("CREATE MATERIALIZED VIEW v AS SELECT k FROM q", "48"),
        (
            "CREATE MATERIALIZED VIEW v TO missing AS SELECT k FROM q",
            "60",
        ),
        ("CREATE MATERIALIZED VIEW v TO q AS SELECT k FROM q", "48"),
        ("CREATE MATERIALIZED VIEW t TO t AS SELECT k FROM q", "57"),
    ] {
        assert_eq!(code(addr, sql).await.as_deref(), Some(want), "{sql}");
    }
    ok(addr, "CREATE MATERIALIZED VIEW v TO t AS SELECT k FROM q").await;
    assert_eq!(
        code(addr, "CREATE MATERIALIZED VIEW v TO t AS SELECT k FROM q")
            .await
            .as_deref(),
        Some("57")
    );
    ok(
        addr,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS v TO t AS SELECT k FROM q",
    )
    .await;
    // A reader cannot make views; DROP TABLE drops a view as in ClickHouse.
    let refused = post(
        addr,
        ("reader", "r"),
        "CREATE MATERIALIZED VIEW w TO t AS SELECT k FROM q",
    )
    .await;
    assert_ne!(refused.status, 200);
    ok(addr, "DROP TABLE v").await;
    assert_eq!(code(addr, "DROP VIEW v").await.as_deref(), Some("60"));
    ok(addr, "DROP VIEW IF EXISTS v").await;
    assert!(
        ok(addr, "SHOW CREATE TABLE q")
            .await
            .contains("LoamsStream")
    );
}

// ---------------------------------------------------------------------------
// S3Queue (HS1 Task 18).

/// Three rows `k = 3n, 3n+1, 3n+2` as JSON lines.
fn lines(n: u64) -> String {
    (3 * n..3 * n + 3)
        .map(|k| format!("{{\"k\": {k}, \"v\": \"item{k}\"}}\n"))
        .collect()
}

async fn inbox(house: &Lakehouse) {
    let addr = house.handle.local_addr();
    ok(
        addr,
        "CREATE TABLE inbox (k UInt64, v String) ENGINE = S3Queue('ns/2/in/*.jsonl', \
         'JSONEachRow') SETTINGS mode = 'ordered', max_processed_files_before_commit = 2",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW inbox_mv TO t AS SELECT upper(v) AS v, k FROM inbox",
    )
    .await;
}

fn counted(objects: u64) -> String {
    let rows = objects * 3;
    format!("{rows}\t{rows}\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3queue_processes_each_object_once() {
    let house = lakehouse("s3queue-once").await;
    let addr = house.handle.local_addr();
    inbox(&house).await;
    for n in 0..5 {
        house.put(&format!("ns/2/in/{n:04}.jsonl"), &lines(n)).await;
    }
    // Not the pattern's: another extension, a subdirectory, another directory.
    house.put("ns/2/in/0002.csv", "1,x\n").await;
    house.put("ns/2/in/sub/0009.jsonl", &lines(90)).await;
    house.put("ns/2/other/0001.jsonl", &lines(91)).await;
    let runner = house.queue_runner();
    house.drain_queues(&runner).await;
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        counted(5)
    );
    assert_eq!(
        ok(addr, "SELECT k, v FROM t WHERE k = 4").await,
        "4\tITEM4\n"
    );
    // The cursor is the greatest key processed; the snapshot names its keys.
    let properties = house.properties("t").await;
    let cursor: Vec<(&String, &String)> = properties
        .iter()
        .filter(|(k, _)| k.starts_with("loams.s3queue."))
        .collect();
    assert_eq!(cursor.len(), 1, "{cursor:?}");
    assert_eq!(cursor[0].1, "ns/2/in/0004.jsonl");
    let table = house
        .catalog
        .load_table(&TableIdent::new(
            Lake::namespace(NS, "default"),
            "t".to_string(),
        ))
        .await
        .expect("t");
    let summary = &table
        .metadata
        .current_snapshot()
        .expect("snapshot")
        .summary()
        .additional_properties;
    assert_eq!(
        summary
            .get(&format!("{}.files", cursor[0].0))
            .map(String::as_str),
        Some("[\"ns/2/in/0004.jsonl\"]"),
        "{summary:?}"
    );

    // Nothing new: nothing runs. A new object: only it lands.
    let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
    assert_eq!(
        runner.run(NS, &pipe, &pipe.views[0]).await.expect("idle"),
        Ran::Idle
    );
    house.put("ns/2/in/0005.jsonl", &lines(5)).await;
    house.drain_queues(&runner).await;
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        counted(6)
    );
    assert_eq!(
        ok(
            addr,
            "SELECT engine FROM system.tables WHERE name = 'inbox'"
        )
        .await,
        "S3Queue\n"
    );
    assert_eq!(
        ok(
            addr,
            "SELECT name, pipe, source, target, lag_records, committed_records, last_error = '' \
             FROM system.loams_pipes"
        )
        .await,
        "inbox_mv\tdefault.inbox\tS3Queue(\\'ns/2/in/*.jsonl\\', \\'JSONEachRow\\')\tdefault.t\t0\t6\t1\n"
    );
    assert_eq!(
        code(addr, "SELECT * FROM inbox").await.as_deref(),
        Some("620")
    );
    // A stream runner leaves an S3Queue pipe alone.
    assert_eq!(
        house
            .runner()
            .run(NS, &pipe, &pipe.views[0])
            .await
            .expect("stream runner"),
        Ran::Idle
    );

    // Two fronts at once: one commits, the other gives its batch up.
    house.put("ns/2/in/0006.jsonl", &lines(6)).await;
    let hold = Arc::new(HoldBeforeCommit {
        held: tokio::sync::Notify::new(),
        go: tokio::sync::Notify::new(),
    });
    let slow = house.queue_runner().with_hooks(hold.clone());
    let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
    let (slow_pipe, slow_view) = (pipe.clone(), pipe.views[0].clone());
    let slow_run = tokio::spawn(async move { slow.run(NS, &slow_pipe, &slow_view).await });
    hold.held.notified().await;
    assert!(matches!(
        house
            .queue_runner()
            .run(NS, &pipe, &pipe.views[0])
            .await
            .expect("fast"),
        Ran::Committed {
            records: 1,
            rows: 3
        }
    ));
    hold.go.notify_one();
    assert_eq!(
        slow_run.await.expect("joined").expect("slow"),
        Ran::Abandoned
    );
    assert_eq!(
        ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
        counted(7)
    );

    // A front dies at each step; the next run lands the object once.
    let mut objects = 7;
    for step in [
        Step::Fetched,
        Step::Written,
        Step::BeforeCommit,
        Step::Committed,
    ] {
        house
            .put(&format!("ns/2/in/{objects:04}.jsonl"), &lines(objects))
            .await;
        objects += 1;
        let dying = house
            .queue_runner()
            .with_hooks(Arc::new(DieAt(step, Mutex::new(true))));
        let pipe = pipes(&house.lake, NS).await.expect("pipes").remove(0);
        assert!(
            dying.run(NS, &pipe, &pipe.views[0]).await.is_err(),
            "{step:?}"
        );
        house.drain_queues(&house.queue_runner()).await;
        assert_eq!(
            ok(addr, "SELECT count(), uniqExact(k) FROM t").await,
            counted(objects),
            "after dying at {step:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3queue_after_processing_delete() {
    let house = lakehouse("s3queue-delete").await;
    let addr = house.handle.local_addr();
    ok(
        addr,
        "CREATE TABLE drops (k UInt64, v String) ENGINE = S3Queue('ns/2/drop/day{1..2}/*.csv', \
         'CSV') SETTINGS after_processing = 'delete'",
    )
    .await;
    ok(
        addr,
        "CREATE TABLE d (k UInt64, v String) ENGINE = MergeTree ORDER BY k",
    )
    .await;
    ok(
        addr,
        "CREATE MATERIALIZED VIEW drops_mv TO d AS SELECT k, v FROM drops",
    )
    .await;
    house.put("ns/2/drop/day1/a.csv", "1,a\n2,b\n").await;
    house.put("ns/2/drop/day2/a.csv", "3,c\n").await;
    house.put("ns/2/drop/day3/a.csv", "4,d\n").await;
    house.drain_queues(&house.queue_runner()).await;
    assert_eq!(ok(addr, "SELECT sum(k) FROM d").await, "6\n");
    let left: Vec<String> = house
        .store
        .list("ns/2/drop")
        .await
        .expect("list")
        .into_iter()
        .map(|o| o.path)
        .collect();
    assert_eq!(left, vec!["ns/2/drop/day3/a.csv".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3queue_outside_prefix_is_344() {
    let house = lakehouse("s3queue-prefix").await;
    let addr = house.handle.local_addr();
    for path in [
        "s3://other-bucket/ns/2/in/*.csv",
        "https://example.com/ns/2/in/*.csv",
        "ns/3/in/*.csv",
        "ns/2/../3/in/*.csv",
        "ns/2/house/default/*.parquet",
        "/ns/2/in/*.csv",
        "in/*.csv",
    ] {
        assert_eq!(
            code(
                addr,
                &format!("CREATE TABLE q (a String) ENGINE = S3Queue('{path}', 'CSV')")
            )
            .await
            .as_deref(),
            Some("344"),
            "{path}"
        );
    }
    // Credentials are never taken: the House reads with its own.
    assert_eq!(
        code(
            addr,
            "CREATE TABLE q (a String) ENGINE = S3Queue('ns/2/in/*.csv', 'AKIA', 'secret', 'CSV')"
        )
        .await
        .as_deref(),
        Some("344")
    );
    assert_eq!(
        code(
            addr,
            "CREATE TABLE q (a String) ENGINE = S3Queue('ns/2/in/*.csv', 'CSV') SETTINGS mode = \
             'unordered'"
        )
        .await
        .as_deref(),
        Some("48")
    );
    assert_eq!(
        ok(addr, "SELECT count() FROM system.tables WHERE name = 'q'").await,
        "0\n"
    );
    ok(
        addr,
        "CREATE TABLE q (a String) ENGINE = S3Queue('ns/2/in/*.csv', 'CSV')",
    )
    .await;
}
