//! Resource governance (HS1 Task 21, design §49 §12): the watchdog, memory
//! kills, the namespace queue, request-body caps and `KILL QUERY`, over the real
//! HTTP front and real worker processes.

mod common;

use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::http::{Response, request, target};
use loams_house::config::{HouseConfig, UserMap};
use loams_house::http::{HouseHandle, serve};
use loams_house::identity::Scopes;
use loams_house::{
    CgroupLimits, ExitReason, PoolConfig, ProcessLauncher, WorkerCgroups, WorkerPool,
};
use loams_house_ipc::SandboxProbe;

/// alice and bob share namespace 2 and neither is an admin; root is namespace 2's
/// admin; carol is namespace 3.
fn users() -> Vec<UserMap> {
    let operator = Scopes {
        admin: false,
        ..Scopes::ALL
    };
    vec![
        UserMap {
            scopes: operator,
            ..UserMap::dev("alice", "a", 2, false)
        },
        UserMap {
            scopes: operator,
            ..UserMap::dev("bob", "b", 2, false)
        },
        UserMap::dev("root", "r", 2, false),
        UserMap::dev("carol", "c", 3, false),
    ]
}

async fn house(
    test: &str,
    pool_config: PoolConfig,
    adjust: impl FnOnce(&mut HouseConfig),
) -> (HouseHandle, WorkerPool) {
    let launcher = ProcessLauncher::new(common::WORKER, common::tmp_root(test));
    let pool = WorkerPool::start(pool_config, Arc::new(launcher))
        .await
        .expect("pool");
    let mut config = HouseConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        users: users(),
        tmp_dir: common::tmp_root(&format!("{test}-spool")),
        ..HouseConfig::default()
    };
    adjust(&mut config);
    (serve(config, pool.clone()).await.expect("serves"), pool)
}

/// One statement as `user`, with extra parameters, headers and body.
async fn send(
    addr: SocketAddr,
    user: &'static str,
    sql: &str,
    extra: &[(&'static str, &str)],
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
) -> Response {
    let password = &user[..1];
    let mut params = vec![
        ("user", user.to_string()),
        ("password", password.to_string()),
        ("query", sql.to_string()),
    ];
    params.extend(extra.iter().map(|(k, v)| (*k, v.to_string())));
    tokio::task::spawn_blocking(move || {
        let pairs: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let headers: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
        request(addr, "POST", &target(&pairs), &headers, &body)
    })
    .await
    .expect("client")
}

async fn run(
    addr: SocketAddr,
    user: &'static str,
    sql: &str,
    extra: &[(&'static str, &str)],
) -> Response {
    send(addr, user, sql, extra, Vec::new(), Vec::new()).await
}

fn code(response: &Response) -> Option<&str> {
    response.header("X-ClickHouse-Exception-Code")
}

/// A statement chDB itself does not stop at its `max_execution_time`: chDB
/// checks the limit between blocks (and `sleep` every second), so one block of
/// work runs to its end, here about 6 s of hashing in one row (measured: chDB
/// answered `159` only when the block ended). The watchdog kills the worker at
/// 0.5 s + 2 s and the client gets `159`; chDB's own `159` kills nothing, so the
/// `timeout` kill is the watchdog's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_kills_worker() {
    let (house, pool) = house("timeout", common::small(2), |_| {}).await;
    let addr = house.local_addr();
    let hashed = format!("{}x{}", "cityHash64(".repeat(60), ")".repeat(60));
    let one_block = format!("SELECT arraySum(arrayMap(x -> {hashed}, range(100000000)))");
    let started = Instant::now();
    let response = run(addr, "alice", &one_block, &[("max_execution_time", "0.5")]).await;
    let took = started.elapsed();
    assert_eq!(code(&response), Some("159"), "{}", response.text());
    assert!(
        took >= Duration::from_millis(2_400) && took < Duration::from_millis(4_000),
        "killed at the limit + 2 s: {took:?}"
    );
    assert_eq!(pool.stats().kills_for(ExitReason::Timeout), 1);
    // The front goes on; a statement within its limit is not touched.
    let fine = run(
        addr,
        "alice",
        "SELECT sleep(1)",
        &[("max_execution_time", "5")],
    )
    .await;
    assert_eq!(fine.status, 200, "{}", fine.text());
    assert_eq!(pool.stats().kills_for(ExitReason::Timeout), 1);
}

/// Memory: chDB's own `max_memory_usage` answers `241` and the worker serves on;
/// a worker the cgroup's OOM killer ends answers `241` too (its `memory.events`),
/// counted as `oom`, not as a crash, and the front serves on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oom_is_241_not_a_crash_of_the_front() {
    let (house, pool) = house("oom", common::small(2), |_| {}).await;
    let addr = house.local_addr();
    let blown = run(
        addr,
        "alice",
        "SELECT length(groupArray(number)) FROM numbers(50000000)",
        &[("max_memory_usage", "20000000")],
    )
    .await;
    assert_eq!(code(&blown), Some("241"), "{}", blown.text());
    assert_eq!(run(addr, "alice", "SELECT 1", &[]).await.text(), "1\n");
    assert_eq!(pool.stats().kills_for(ExitReason::Crash), 0);

    // The kernel's OOM killer, in a delegated cgroup.
    let root = match common::delegated_cgroup("oom") {
        Ok(root) => root,
        Err(why) => {
            eprintln!("oom_is_241_not_a_crash_of_the_front: cgroup half skipped ({why})");
            return;
        }
    };
    let limits = CgroupLimits {
        memory_max: Some(768 * 1024 * 1024),
        cpus: Some(2),
        pids_max: Some(2048),
    };
    let cgroups = WorkerCgroups::prepare(&root, limits).expect("the delegated cgroup");
    let launcher =
        ProcessLauncher::new(common::WORKER, common::tmp_root("oom-cgroup")).with_cgroups(cgroups);
    let pool = WorkerPool::start(common::small(1), Arc::new(launcher))
        .await
        .expect("the pool starts");
    let mut lease = pool.acquire("ns").await.expect("worker");
    let err = lease
        .probe_for_test(SandboxProbe::Allocate(4 * 1024 * 1024 * 1024))
        .await
        .expect_err("the OOM killer ends it");
    assert_eq!(err.code(), 241, "{err}");
    pool.release(lease, loams_house::Outcome::Completed);
    assert_eq!(pool.stats().kills_for(ExitReason::Oom), 1);
    assert_eq!(pool.stats().kills_for(ExitReason::Crash), 0);
    let mut again = pool.acquire("ns").await.expect("another worker");
    assert_eq!(
        again
            .run(common::statement("SELECT 1", "TSV"))
            .await
            .expect("serves")
            .bytes,
        b"1\n"
    );
    pool.release(again, loams_house::Outcome::Completed);
    pool.shutdown();
    for _ in 0..50 {
        if std::fs::remove_dir(&root).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The namespace queue over HTTP: one running, one queued, the next `202` at once;
/// another namespace is not held up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_queue_over_http() {
    let (house, _pool) = house("queue", common::small(3), |config| {
        config.admission.max_running_per_namespace = 1;
        config.admission.max_queued_per_namespace = 1;
        config.admission.queue_timeout = Duration::from_secs(10);
        config.admin_listen = Some("127.0.0.1:0".parse().expect("addr"));
    })
    .await;
    let admin = house.admin_addr().expect("the admin listener");
    let addr = house.local_addr();
    let slow = tokio::spawn(async move { run(addr, "alice", "SELECT sleep(1.5)", &[]).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let queued = tokio::spawn(async move { run(addr, "bob", "SELECT 2", &[]).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = Instant::now();
    let refused = run(addr, "alice", "SELECT 3", &[]).await;
    assert_eq!(code(&refused), Some("202"), "{}", refused.text());
    assert!(started.elapsed() < Duration::from_millis(500), "at once");
    // What Task 24's autoscaler reads.
    let metrics = tokio::task::spawn_blocking(move || request(admin, "GET", "/metrics", &[], &[]))
        .await
        .expect("metrics");
    assert!(
        metrics.text().contains("\nloams_house_queue_depth 1\n"),
        "{}",
        metrics.text()
    );
    assert!(
        metrics
            .text()
            .contains("\nloams_house_admitted_queries 1\n")
    );
    assert_eq!(run(addr, "carol", "SELECT 4", &[]).await.text(), "4\n");
    assert_eq!(slow.await.expect("slow").status, 200);
    assert_eq!(queued.await.expect("queued").text(), "2\n");
}

/// §49 §12: a request body may expand at most 100 times (`36`). The worker
/// that was taking the refused `INSERT` is killed rather than given a body that
/// did not arrive whole (review I5 of Task 3), so its session's temporary table
/// goes with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_bomb_is_36() {
    let (house, _pool) = house("zip-bomb", common::small(2), |_| {}).await;
    let addr = house.local_addr();
    let session = [("session_id", "bomb")];
    let created = run(
        addr,
        "alice",
        "CREATE TEMPORARY TABLE t (s String) ENGINE = Memory",
        &session,
    )
    .await;
    assert_eq!(created.status, 200, "{}", created.text());
    let gzip = |data: &[u8]| {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(data).expect("gzip");
        encoder.finish().expect("gzip")
    };
    let encoded = vec![("Content-Encoding", "gzip".to_string())];
    // 64 MiB of one line, about 1000:1.
    let bomb = gzip(&b"a\n".repeat(32 * 1024 * 1024));
    let honest = send(
        addr,
        "alice",
        "INSERT INTO t FORMAT TSV",
        &session,
        encoded.clone(),
        gzip(b"x\ny\n"),
    )
    .await;
    assert_eq!(honest.status, 200, "{}", honest.text());
    assert_eq!(
        run(addr, "alice", "SELECT count() FROM t", &session)
            .await
            .text(),
        "2\n"
    );
    let refused = send(
        addr,
        "alice",
        "INSERT INTO t FORMAT TSV",
        &session,
        encoded,
        bomb,
    )
    .await;
    assert_eq!(code(&refused), Some("36"), "{}", refused.text());
    assert!(
        refused.text().contains("decompresses more than 100x"),
        "the cap is named: {}",
        refused.text()
    );
    let gone = run(addr, "alice", "SELECT count() FROM t", &session).await;
    assert_eq!(code(&gone), Some("60"), "{}", gone.text());
}

/// `KILL QUERY WHERE query_id = '…'` (§49 §12): a user kills its own statements,
/// another user's only as its namespace's admin (`497` otherwise), another
/// namespace's are not seen; the killed statement answers `394` at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_query_own_and_admin() {
    let (house, pool) = house("kill", common::small(3), |_| {}).await;
    let addr = house.local_addr();
    // Thirty one-second blocks: chDB's own limit is 300 s.
    let long = "SELECT sleepEachRow(1) FROM numbers(30) SETTINGS max_block_size = 1";
    let start = |user: &'static str, id: &'static str| {
        tokio::spawn(async move {
            let started = Instant::now();
            let response = run(addr, user, long, &[("query_id", id)]).await;
            (response, started.elapsed())
        })
    };
    let alices = start("alice", "q-alice");
    // Running once `TEST` sees it.
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let test = run(
            addr,
            "alice",
            "KILL QUERY WHERE query_id = 'q-alice' TEST",
            &[],
        )
        .await;
        assert_eq!(test.status, 200, "{}", test.text());
        if !test.text().is_empty() {
            assert_eq!(
                test.text(),
                format!(
                    "unknown_status\tq-alice\talice\t{}\n",
                    long.replace('\'', "\\'")
                )
            );
            break;
        }
        assert!(Instant::now() < until, "the query never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Same namespace, another user, no admin: 497. Another namespace: nothing.
    let bob = run(addr, "bob", "KILL QUERY WHERE query_id = 'q-alice'", &[]).await;
    assert_eq!(code(&bob), Some("497"), "{}", bob.text());
    let carol = run(addr, "carol", "KILL QUERY WHERE query_id = 'q-alice'", &[]).await;
    assert_eq!(carol.status, 200, "{}", carol.text());
    assert_eq!(carol.text(), "", "another namespace's queries are not seen");
    // Its owner: SYNC answers once it is gone.
    let killed = run(
        addr,
        "alice",
        "KILL QUERY WHERE query_id = 'q-alice' SYNC",
        &[("default_format", "TSVWithNames")],
    )
    .await;
    assert_eq!(killed.status, 200, "{}", killed.text());
    assert!(
        killed
            .text()
            .starts_with("kill_status\tquery_id\tuser\tquery\nfinished\tq-alice\talice\t"),
        "{}",
        killed.text()
    );
    let (answer, took) = alices.await.expect("alice's query");
    assert_eq!(code(&answer), Some("394"), "{}", answer.text());
    assert!(took < Duration::from_secs(20), "{took:?}");

    // The admin kills bob's.
    let bobs = start("bob", "q-bob");
    let until = Instant::now() + Duration::from_secs(30);
    let killed = loop {
        let killed = run(
            addr,
            "root",
            "KILL QUERY WHERE query_id = 'q-bob' FORMAT JSONEachRow",
            &[],
        )
        .await;
        assert_eq!(killed.status, 200, "{}", killed.text());
        if !killed.text().is_empty() {
            break killed;
        }
        assert!(Instant::now() < until, "bob's query never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let row: serde_json::Value = serde_json::from_str(killed.text().trim()).expect("json");
    assert_eq!(row["kill_status"], "waiting");
    assert_eq!(row["user"], "bob");
    let (answer, _) = bobs.await.expect("bob's query");
    assert_eq!(code(&answer), Some("394"), "{}", answer.text());
    assert_eq!(pool.stats().kills_for(ExitReason::Cancel), 2);

    // Other conditions are not served; an id nobody runs is an empty answer.
    let other = run(addr, "alice", "KILL QUERY WHERE user = 'bob'", &[]).await;
    assert_eq!(code(&other), Some("48"), "{}", other.text());
    let nobody = run(addr, "alice", "KILL QUERY WHERE query_id = 'nobody'", &[]).await;
    assert_eq!((nobody.status, nobody.text()), (200, String::new()));
    // A duplicate id while the first runs is refused.
    let first = start("alice", "q-twice");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let twice = run(addr, "alice", "SELECT 1", &[("query_id", "q-twice")]).await;
    assert_eq!(code(&twice), Some("36"), "{}", twice.text());
    let _ = run(addr, "alice", "KILL QUERY WHERE query_id = 'q-twice'", &[]).await;
    let _ = first.await;
}
