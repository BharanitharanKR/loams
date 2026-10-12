//! Idle-session migration against real keyspace-mode TiDB (plan SQ1 Task
//! 6): a branch's pool of two `LocalRuntime` members on the spike stack;
//! sessions start on member 0 and are asked to move to member 1. Ignored
//! unless `LOAMS_IT_SQLDB=1`; run with `--test-threads=1`.
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use loams_sqldb::model::{BranchId, Class, Endpoints};
use loams_sqldb::runtime::SqlRuntime;
use loams_sqldb::runtime::local::{ContainerEngine, LocalRuntime, LocalRuntimeConfig};
use loams_sqlgate::auth::{ResolvedUser, Role, StaticUsers, hash_password};
use loams_sqlgate::codec::command::ErrPacket;
use loams_sqlgate::limits::ActivityCounter;
use loams_sqlgate::server::{Gate, GateConfig, GateDeps, SniCert, sni_server_config};
use loams_sqlgate::upstream::{
    Admission, Close, PoolResolver, SessionLease, UpstreamError, UpstreamMember,
};
use mysql_async::prelude::Queryable;

use super::client;
use super::pki::Pki;
use super::real::{Cleanup, Creds, keyspace};
use super::wake_real::{branch_id, stamp, upstream_member};

/// Admits every session to member 0 and lets the test ask them to move.
struct MovePools {
    first: UpstreamMember,
    leases: Mutex<Vec<tokio::sync::watch::Sender<Close>>>,
}

impl MovePools {
    fn migrate(&self, to: Vec<UpstreamMember>) {
        for tx in self.leases.lock().expect("leases").iter() {
            let _ = tx.send(Close::Migrate(to.clone()));
        }
    }
}

struct Lease(tokio::sync::watch::Receiver<Close>);

#[async_trait]
impl SessionLease for Lease {
    async fn closing(&mut self) -> Close {
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        self.0.borrow_and_update().clone()
    }
}

struct Resolver(Arc<MovePools>);

#[async_trait]
impl PoolResolver for Resolver {
    async fn ensure_running(&self, _branch: &str) -> Result<Admission, UpstreamError> {
        let (tx, rx) = tokio::sync::watch::channel(Close::Open);
        self.0.leases.lock().expect("leases").push(tx);
        Ok(Admission {
            members: vec![self.0.first.clone()],
            lease: Box::new(Lease(rx)),
        })
    }
}

struct Stack {
    pki: Pki,
    dir: PathBuf,
    rt: Arc<LocalRuntime>,
    branch: BranchId,
    pools: Arc<MovePools>,
    /// Members 0 and 1.
    members: [UpstreamMember; 2],
    gate: SocketAddr,
    _cleanup: Cleanup,
}

/// A two-member pool of a fresh keyspace (bootstrapped by member 0 alone)
/// behind a gate; user `u_a` / `pa` writes to it.
async fn stack(name: &str, port_base: u16) -> Stack {
    let pki = Pki::new();
    let s = stamp();
    let branch = branch_id(s);
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("sqlgate-{name}-{s}"));
    let tls = dir.join("tls");
    std::fs::create_dir_all(&tls).expect("tls dir");
    std::fs::write(tls.join("ca.crt"), &pki.ca_pem).expect("ca");
    std::fs::write(tls.join("tls.crt"), &pki.upstream_pem.0).expect("cert");
    std::fs::write(tls.join("tls.key"), &pki.upstream_pem.1).expect("key");
    let instance = format!("{name}-{}", std::process::id());
    let cleanup = Cleanup(instance.clone());
    let endpoints = Endpoints::new(
        vec!["127.0.0.1:29379".into()],
        vec!["127.0.0.1/32".parse().expect("cidr")],
    )
    .expect("endpoints");
    let internal: Vec<u8> = format!("ri-{s:x}-secret").into_bytes();
    let mut config =
        LocalRuntimeConfig::new(ContainerEngine::podman(), dir.join("pools"), tls, endpoints);
    config.instance = instance;
    config.mysql_port_base = port_base;
    config.status_port_base = port_base + 1000;
    config.cpu_limits = false;
    config.extra_init_sql = vec![
        format!(
            "CREATE USER 'ri_writer'@'%' IDENTIFIED WITH caching_sha2_password BY '{}'",
            String::from_utf8_lossy(&internal)
        ),
        "GRANT ALL PRIVILEGES ON *.* TO 'ri_writer'@'%'".into(),
    ];
    let rt = Arc::new(LocalRuntime::new(config).expect("runtime"));
    keyspace(branch.as_str());
    rt.ensure_pool(&branch, Class::Xs, 1).await.expect("pool");
    wait_ready(&rt, &branch, 1).await;
    rt.scale(&branch, 2).await.expect("scale to 2");
    let members = wait_ready(&rt, &branch, 2).await;
    let pools = Arc::new(MovePools {
        first: members[0].clone(),
        leases: Mutex::new(Vec::new()),
    });
    let users = StaticUsers::new(vec![(
        "u_a".into(),
        ResolvedUser {
            branch: branch.to_string(),
            role: Role::Writer,
            password_hash: hash_password(b"pa"),
        },
    )]);
    let server_tls = sni_server_config(vec![SniCert {
        names: vec!["localhost".into()],
        chain: pki.gate_chain.clone(),
        key: pki.gate_key.clone_key(),
    }])
    .expect("tls");
    let gate = Gate::new(
        GateConfig::new(server_tls, pki.client_config()),
        GateDeps {
            users: Arc::new(users),
            credentials: Arc::new(Creds(internal)),
            pools: Arc::new(Resolver(pools.clone())),
            activity: Arc::new(ActivityCounter::default()),
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(gate.serve(listener));
    Stack {
        pki,
        dir,
        rt,
        branch,
        pools,
        members: [members[0].clone(), members[1].clone()],
        gate: addr,
        _cleanup: cleanup,
    }
}

/// Waits until `n` members are ready; returns them by index.
async fn wait_ready(rt: &LocalRuntime, branch: &BranchId, n: usize) -> Vec<UpstreamMember> {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let st = rt.pool_status(branch).await.expect("status").expect("pool");
        if st.ready() == n {
            let mut members = st.members.clone();
            members.sort_by_key(|m| m.index);
            return members.iter().map(upstream_member).collect();
        }
        assert!(Instant::now() < deadline, "{branch} not ready: {st:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

impl Stack {
    fn opts(&self) -> mysql_async::OptsBuilder {
        mysql_async::OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.gate.port())
            .user(Some("u_a"))
            .pass(Some("pa"))
            .ssl_opts(Some(
                mysql_async::SslOpts::default()
                    .with_root_certs(vec![self.dir.join("tls/ca.crt").into()]),
            ))
    }

    /// The MySQL port of the member serving `c` (members differ by port).
    async fn port(c: &mut mysql_async::Conn) -> u16 {
        c.query_first::<u16, _>("SELECT @@port")
            .await
            .expect("@@port")
            .expect("a row")
    }

    /// Queries until `c` is served by member 1 (at most 30 s).
    async fn until_moved(&self, c: &mut mysql_async::Conn) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Self::port(c).await != self.members[1].addr.port() {
            assert!(Instant::now() < deadline, "the session did not move");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn teardown(self) {
        let _ = self.rt.delete_pool(&self.branch).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[cfg_attr(
    not(loams_it_sqldb),
    ignore = "needs LOAMS_IT_SQLDB=1 and the spike stack"
)]
#[tokio::test(flavor = "multi_thread")]
async fn idle_session_survives_member_replacement() {
    let st = stack("move-it", 24700).await;
    let mut c = mysql_async::Conn::new(st.opts()).await.expect("login");
    c.query_drop("CREATE DATABASE w").await.expect("db");
    c.query_drop("USE w").await.expect("use");
    c.query_drop("CREATE TABLE t (x INT PRIMARY KEY)")
        .await
        .expect("table");
    c.query_drop("INSERT INTO t VALUES (1)").await.expect("row");
    c.query_drop("SET @greeting = 'it''s a \\\\ path'")
        .await
        .expect("user variable");
    c.query_drop("SET SESSION sql_mode = 'ANSI_QUOTES'")
        .await
        .expect("sql_mode");
    c.query_drop("SET SESSION max_execution_time = 1234")
        .await
        .expect("variable");
    // A binary-protocol statement (mysql_async keeps its id) and a text one.
    let stmt = c.prep("SELECT x + ? FROM t").await.expect("prepare");
    c.query_drop("PREPARE p FROM 'SELECT 7'")
        .await
        .expect("text prepare");
    assert_eq!(Stack::port(&mut c).await, st.members[0].addr.port());

    st.pools.migrate(vec![st.members[1].clone()]);
    st.until_moved(&mut c).await;

    let greeting: Option<String> = c.query_first("SELECT @greeting").await.expect("var");
    assert_eq!(greeting.as_deref(), Some("it's a \\ path"));
    let mode: Option<String> = c.query_first("SELECT @@sql_mode").await.expect("mode");
    assert!(mode.expect("mode").contains("ANSI_QUOTES"));
    let met: Option<u64> = c
        .query_first("SELECT @@max_execution_time")
        .await
        .expect("met");
    assert_eq!(met, Some(1234));
    let db: Option<String> = c.query_first("SELECT DATABASE()").await.expect("db");
    assert_eq!(db.as_deref(), Some("w"));
    let sum: Option<i64> = c.exec_first(&stmt, (41,)).await.expect("execute");
    assert_eq!(sum, Some(42), "the prepared statement kept its id");
    let seven: Option<i64> = c.query_first("EXECUTE p").await.expect("EXECUTE p");
    assert_eq!(seven, Some(7));
    drop(c);
    st.teardown().await;
}

#[cfg_attr(
    not(loams_it_sqldb),
    ignore = "needs LOAMS_IT_SQLDB=1 and the spike stack"
)]
#[tokio::test(flavor = "multi_thread")]
async fn session_in_transaction_is_not_migrated() {
    let st = stack("txn-it", 24800).await;
    let mut c = mysql_async::Conn::new(st.opts()).await.expect("login");
    c.query_drop("CREATE DATABASE w").await.expect("db");
    c.query_drop("CREATE TABLE w.t (x INT PRIMARY KEY)")
        .await
        .expect("table");
    c.query_drop("BEGIN").await.expect("begin");
    c.query_drop("INSERT INTO w.t VALUES (1)")
        .await
        .expect("insert");

    st.pools.migrate(vec![st.members[1].clone()]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Still on member 0, inside its transaction.
    assert_eq!(Stack::port(&mut c).await, st.members[0].addr.port());
    let seen: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM w.t")
        .await
        .expect("count");
    assert_eq!(seen, Some(1), "the transaction's own write");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(Stack::port(&mut c).await, st.members[0].addr.port());

    // Committed on member 0, then moved at the next idle point.
    c.query_drop("COMMIT").await.expect("commit");
    st.until_moved(&mut c).await;
    let rows: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM w.t")
        .await
        .expect("count");
    assert_eq!(rows, Some(1));
    drop(c);
    st.teardown().await;
}

#[cfg_attr(
    not(loams_it_sqldb),
    ignore = "needs LOAMS_IT_SQLDB=1 and the spike stack"
)]
#[tokio::test(flavor = "multi_thread")]
async fn migration_failure_closes_with_1053() {
    let st = stack("fail-it", 24900).await;
    let mut c = client::connect(
        st.gate,
        "u_a",
        b"pa",
        Some((st.pki.client_config(), "localhost")),
        None,
    )
    .await
    .expect("login");
    assert_eq!(c.query_rows("SELECT 1").await, [[Some("1".to_owned())]]);
    // The new member's address answers nothing: SHOW SESSION_STATES runs
    // on member 0, then no login succeeds.
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        l.local_addr().expect("addr")
    };
    st.pools.migrate(vec![UpstreamMember {
        addr: dead,
        server_name: st.members[1].server_name.clone(),
    }]);
    let (seq, p) = c.wire.read().await.expect("an ERR before the close");
    let e = ErrPacket::decode(&p).expect("ERR");
    assert_eq!((seq, e.code, &e.sql_state), (0, 1053, b"08S01"), "{e:?}");
    assert_eq!(c.wire.read().await, None, "then closed");
    st.teardown().await;
}
