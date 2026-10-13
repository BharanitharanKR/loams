//! An endpoint's lifecycle end to end (PG2 Tasks 11 and 12): the API
//! service and the endpoint reconciler drive real computes through the
//! compose runtime and each compute's `compute_ctl`, on
//! `deploy/loams-postgres-dev`'s storage. Ignored by default; `pg2-e2e.yml` runs it beside `it_compose`:
//!
//! ```text
//! (cd deploy/loams-postgres-dev && docker compose up -d rustfs create-bucket storage_broker pageserver safekeeper1)
//! cargo test -p loams-pg-control --test it_endpoint -- --ignored
//! ```
//!
//! `StartEndpoint` reaches `running` and `SELECT 1` answers on the
//! endpoint's address; `SuspendEndpoint` stops the compute; a second start
//! runs a new compute at the same address; `DeleteEndpoint` removes it and
//! gives the address up. A query longer than the suspend timeout keeps the
//! compute running, which then suspends once idle (Task 12). Each case has
//! its own namespace and compose project. The settings are `it_compose`'s
//! (environment):
//! `NEON_PAGESERVER`, `IT_COMPOSE_NETWORK`, `IT_COMPOSE_PAGESERVER`,
//! `IT_PAGESERVER_ID`, `IT_COMPOSE_SAFEKEEPERS`, `IT_UPCALL_HOST`,
//! `IT_EXTRA_HOSTS`, `COMPUTE_IMAGE`.
#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use loams_pg_control::ids::{self, BranchId, ComputeId, ProjectId};
use loams_pg_control::model::{
    BranchRec, BranchState, ComputeKey, ComputeRec, EndpointKey, EndpointRec, EndpointState,
    EndpointType, OperationState, ProjectRec, ProjectState, WalService,
};
use loams_pg_control::reconcile::endpoint::{Activity, ComputeCtl, Computes, HttpComputeCtl};
use loams_pg_control::reconcile::{Reconciler, ReconcilerConfig};
use loams_pg_control::runtime::compose::{
    ComposeBin, ComposeConfig, ComposeRuntime, TokioCommandRunner,
};
use loams_pg_control::runtime::images;
use loams_pg_control::runtime::{ComputeRuntime, RuntimeStatus};
use loams_pg_control::service::Clock;
use loams_pg_control::service::endpoints::{CreateEndpoint, DeleteEndpoint, EndpointAction};
use loams_pg_control::store::conformance::{Factory, local_factory};
use loams_pg_control::upcall::{
    ComputeCtlConfigurer, ComputeKeys, SpecSources, UpcallConfig, UpcallServer,
};
use loams_pg_control::{KvControlStore, PgControlStore, StoreOptions};
use loams_postgres::pageserver::{NeonClient, NeonEndpoints, TenantConfig, TimelineCreate};
use loams_postgres::{TenantId, TimelineId};

use crate::common::{FakeNeon, user};

/// The backend `common` opens stores on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

fn env(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.into())
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Removes the namespace's compose project when the test ends.
struct Down(ComposeBin, PathBuf, &'static str);

impl Drop for Down {
    fn drop(&mut self) {
        let _ = std::process::Command::new(&self.0.program)
            .args(&self.0.args)
            .args([
                "-p",
                &format!("loams-pg-{}", self.2),
                "down",
                "--timeout",
                "5",
            ])
            .current_dir(&self.1)
            .status();
    }
}

/// `compose <args>` on the namespace `ns`'s project: its stdout, or a panic.
fn compose(bin: &ComposeBin, dir: &Path, ns: &str, args: &[&str]) -> String {
    let file = dir.join(format!("{ns}.yaml"));
    let out = std::process::Command::new(&bin.program)
        .args(&bin.args)
        .args([
            "-f",
            &file.display().to_string(),
            "-p",
            &format!("loams-pg-{ns}"),
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "compose {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

async fn endpoint(store: &KvControlStore, project: &str, id: &str) -> Option<EndpointRec> {
    store
        .get::<EndpointRec>(&EndpointKey {
            project_id: project.into(),
            id: id.into(),
        })
        .await
        .unwrap()
        .map(|v| v.record)
}

/// Passes over `ns`'s `project` until the endpoint is in `want` (or gone,
/// for `None`), for up to three minutes.
async fn settle(
    r: &Reconciler<KvControlStore, FakeNeon>,
    store: &KvControlStore,
    ns: &str,
    project: &str,
    id: &str,
    want: Option<EndpointState>,
) {
    let start = Instant::now();
    loop {
        let state = endpoint(store, project, id).await.map(|e| e.state);
        if state == want {
            return;
        }
        assert!(
            state != Some(EndpointState::Failed),
            "the endpoint failed: {:?}",
            endpoint(store, project, id).await
        );
        assert!(
            start.elapsed() < Duration::from_secs(180),
            "the endpoint stayed {state:?}, not {want:?}"
        );
        if let Err(e) = r.reconcile_project(ns, project).await {
            eprintln!("pass: {e}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// What both cases run on: the storage's tenant and timeline, the upcall
/// listener, a ready project and branch, the compose runtime and a
/// reconciler, for the namespace `ns` (its own compose project).
struct Stack {
    ns: &'static str,
    bin: ComposeBin,
    project: ProjectId,
    store: KvControlStore,
    h: common::Harness,
    keys: Arc<ComputeKeys>,
    rt: Arc<ComposeRuntime>,
    r: Reconciler<KvControlStore, FakeNeon>,
    compose_dir: PathBuf,
    stop_upcall: Option<tokio::sync::oneshot::Sender<()>>,
    // Dropped in this order: the computes, then their directory.
    _down: Down,
    _dir: tempfile::TempDir,
}

impl Drop for Stack {
    fn drop(&mut self) {
        if let Some(stop) = self.stop_upcall.take() {
            let _ = stop.send(());
        }
    }
}

/// Each case's own host ports: the cases run in parallel, each with its own
/// compose runtime (and `state/ports.json`), so a shared range would give
/// both first endpoints the same port and one compute could not publish it.
fn port_range(ns: &str) -> std::ops::Range<u16> {
    match ns {
        "it-ep" => 55_500..55_750,
        "it-idle" => 55_750..56_000,
        other => panic!("no port range for the case {other}"),
    }
}

async fn stack(ns: &'static str) -> Stack {
    let bin = ComposeBin::from_path().expect("docker, podman or docker-compose on PATH");
    let project = ProjectId::new();
    let main = BranchId::new();
    let tenant = TenantId(ids::tenant_id(&project));
    let timeline = TimelineId(ids::timeline_id(&main));

    // Storage: the tenant and its first timeline, as Task 7's reconciler
    // makes them.
    let neon = NeonClient::new(
        NeonEndpoints {
            pageserver: env("NEON_PAGESERVER", "http://127.0.0.1:9898")
                .parse()
                .unwrap(),
            storcon: None,
        },
        None,
    )
    .unwrap();
    neon.attach_tenant(tenant, 1, &TenantConfig::default())
        .await
        .unwrap();
    neon.create_timeline(
        tenant,
        &TimelineCreate {
            new_timeline_id: timeline,
            ancestor: None,
            ancestor_start_lsn: None,
            pg_version: Some(17),
        },
    )
    .await
    .unwrap();

    let dir = tempfile::Builder::new()
        .prefix("it-endpoint-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let store = local_factory(dir.path())
        .store(StoreOptions::default())
        .await
        .unwrap();
    let h = common::harness_on(store.clone());
    let keys = Arc::new(ComputeKeys::generate().unwrap());
    let pageserver_id: u64 = env("IT_PAGESERVER_ID", "1234").parse().unwrap();
    let sources = SpecSources {
        pageservers: BTreeMap::from([(
            pageserver_id,
            env(
                "IT_COMPOSE_PAGESERVER",
                "postgresql://no_user@pageserver:6400",
            ),
        )]),
        default_pageserver: Some(pageserver_id),
        wal_pools: BTreeMap::from([(
            "default".to_owned(),
            env("IT_COMPOSE_SAFEKEEPERS", "safekeeper1:5454")
                .split(',')
                .map(str::to_owned)
                .collect(),
        )]),
    };
    let upcall = UpcallServer::bind(
        store.clone(),
        keys.clone(),
        Arc::new(ComputeCtlConfigurer::new(keys.clone(), Clock::system())),
        UpcallConfig {
            pageservers: sources.pageservers.clone(),
            default_pageserver: sources.default_pageserver,
            wal_pools: sources.wal_pools.clone(),
            ..UpcallConfig::defaults("0.0.0.0:0".parse().unwrap())
        },
    )
    .await
    .unwrap();
    let upcall_port = upcall.local_addr().port();
    let (stop_upcall, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(upcall.serve(async move {
        let _ = stopped.await;
    }));

    // The project and its branch, ready.
    let now = now_ms();
    let api = store.api_writer();
    api.put(
        &ProjectRec {
            namespace: ns.into(),
            id: project.to_string(),
            name: "it".into(),
            tenant_id: tenant.0,
            pg_version: 17,
            wal: WalService::LoamsWal {
                pool: "default".into(),
            },
            history_retention_s: 24 * 3600,
            region: "local".into(),
            default_branch_id: Some(main.to_string()),
            settings: BTreeMap::new(),
            state: ProjectState::Ready,
            created_at_ms: now,
            updated_at_ms: now,
        },
        None,
    )
    .await
    .unwrap();
    api.put(
        &BranchRec {
            project_id: project.to_string(),
            id: main.to_string(),
            name: "main".into(),
            timeline_id: timeline.0,
            parent_id: None,
            ancestor_lsn: None,
            expires_at_ms: None,
            protected: false,
            stripe_size: None,
            shards: Vec::new(),
            state: BranchState::Ready,
            created_at_ms: now,
            parent_time_ms: None,
            updated_at_ms: now,
        },
        None,
    )
    .await
    .unwrap();

    // The compose runtime and the reconciler.
    let compose_dir = dir.path().join("pg").join("compose");
    let host = env("IT_UPCALL_HOST", "host.containers.internal");
    let rt = Arc::new(
        ComposeRuntime::open(
            ComposeConfig {
                network: Some(env("IT_COMPOSE_NETWORK", "loams-neon_default")),
                extra_hosts: std::env::var("IT_EXTRA_HOSTS")
                    .map(|h| h.split(',').map(str::to_owned).collect())
                    .unwrap_or_default(),
                port_range: port_range(ns),
                ..ComposeConfig::new(
                    bin.clone(),
                    compose_dir.clone(),
                    format!("http://{host}:{upcall_port}"),
                )
            },
            Arc::new(TokioCommandRunner),
        )
        .unwrap(),
    );
    let down = Down(bin.clone(), dir.path().to_path_buf(), ns);
    let mut images = images::pinned().unwrap();
    if let Ok(image) = std::env::var("COMPUTE_IMAGE") {
        images.insert(17, image);
    }
    let r = Reconciler::new(
        store.clone(),
        h.neon.clone(),
        h.secrets.clone(),
        ReconcilerConfig::new(format!("it-{ns}")),
    )
    .with_computes(Computes::new(
        rt.clone(),
        Arc::new(HttpComputeCtl::new(keys.clone(), Clock::system())),
        keys.clone(),
        images,
        sources,
    ));
    Stack {
        ns,
        bin,
        project,
        store,
        h,
        keys,
        rt,
        r,
        compose_dir,
        stop_upcall: Some(stop_upcall),
        _down: down,
        _dir: dir,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs deploy/loams-postgres-dev running, and docker or podman"]
async fn it_endpoint_starts_suspends_and_deletes() {
    let st = stack("it-ep").await;
    let (ns, bin, project, store, h, rt, r, compose_dir) = (
        st.ns,
        &st.bin,
        st.project,
        &st.store,
        &st.h,
        &st.rt,
        &st.r,
        &st.compose_dir,
    );

    // CreateEndpoint, StartEndpoint: running.
    let ep = h
        .service
        .create_endpoint(
            &user(),
            CreateEndpoint {
                namespace: ns.into(),
                project_id: project.to_string(),
                kind: EndpointType::ReadWrite,
                idempotency_key: "ep".into(),
                ..CreateEndpoint::default()
            },
        )
        .await
        .unwrap();
    let id = ep.endpoint.record.id.clone();
    let action = |key: &str| EndpointAction {
        namespace: ns.into(),
        project_id: project.to_string(),
        endpoint_id: id.clone(),
        idempotency_key: key.into(),
    };
    let start = h
        .service
        .start_endpoint(&user(), action("start1"))
        .await
        .unwrap();
    settle(
        r,
        store,
        ns,
        &project.to_string(),
        &id,
        Some(EndpointState::Running),
    )
    .await;
    assert_eq!(
        h.service.get_operation(&start.id).await.unwrap().state,
        OperationState::Succeeded
    );
    let running = endpoint(store, &project.to_string(), &id).await.unwrap();
    let first: ComputeId = running.compute_id.clone().unwrap().parse().unwrap();
    let addr = running.backend_addr.clone().unwrap();

    // A login role made inside the compute (role passwords are Task 24's),
    // then SELECT 1 through the endpoint's address.
    let service = first.to_string().to_lowercase();
    let password = format!("it-{}", now_ms());
    compose(
        bin,
        compose_dir,
        ns,
        &[
            "exec",
            "-T",
            &service,
            "/usr/local/bin/psql",
            "-h",
            "127.0.0.1",
            "-p",
            "55433",
            "-U",
            "cloud_admin",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &format!("CREATE ROLE it_select LOGIN PASSWORD '{password}'"),
        ],
    );
    let (host, port) = addr.rsplit_once(':').unwrap();
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host={host} port={port} user=it_select password={password} dbname=postgres \
             connect_timeout=10"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    let one: i32 = client.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(one, 1);
    drop(client);

    // SuspendEndpoint: the compute is gone.
    let suspend = h
        .service
        .suspend_endpoint(&user(), action("suspend1"))
        .await
        .unwrap();
    settle(
        r,
        store,
        ns,
        &project.to_string(),
        &id,
        Some(EndpointState::Suspended),
    )
    .await;
    assert_eq!(
        h.service.get_operation(&suspend.id).await.unwrap().state,
        OperationState::Succeeded
    );
    assert_eq!(rt.status(&first).await.unwrap(), RuntimeStatus::Missing);

    // A second start: a new compute at the same address.
    h.service
        .start_endpoint(&user(), action("start2"))
        .await
        .unwrap();
    settle(
        r,
        store,
        ns,
        &project.to_string(),
        &id,
        Some(EndpointState::Running),
    )
    .await;
    let again = endpoint(store, &project.to_string(), &id).await.unwrap();
    assert_ne!(
        again.compute_id.as_deref(),
        Some(first.to_string().as_str())
    );
    assert_eq!(again.backend_addr.as_deref(), Some(addr.as_str()));

    // DeleteEndpoint: stopped, the address given up, the records gone.
    let second: ComputeId = again.compute_id.unwrap().parse().unwrap();
    h.service
        .delete_endpoint(
            &user(),
            DeleteEndpoint {
                namespace: ns.into(),
                project_id: project.to_string(),
                endpoint_id: id.clone(),
                expected_version: None,
                idempotency_key: "del".into(),
            },
        )
        .await
        .unwrap();
    settle(r, store, ns, &project.to_string(), &id, None).await;
    assert_eq!(rt.status(&second).await.unwrap(), RuntimeStatus::Missing);
}

/// Idle detection on a real compute (Task 12). The fork's `compute_ctl`
/// reports `last_active` in a form `pg-control` reads, and moves it while a
/// query runs that sends no new command, so the endpoint stays running
/// through a query longer than its suspend timeout; once the query is done
/// and the client gone, it suspends after the timeout.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs deploy/loams-postgres-dev running, and docker or podman"]
async fn it_idle_long_query_is_activity_then_suspends() {
    let st = stack("it-idle").await;
    let (ns, project, store, h, r) = (st.ns, st.project.to_string(), &st.store, &st.h, &st.r);

    // Running, with no suspend while the role is made.
    let ep = h
        .service
        .create_endpoint(
            &user(),
            CreateEndpoint {
                namespace: ns.into(),
                project_id: project.clone(),
                kind: EndpointType::ReadWrite,
                suspend_timeout_s: Some(0),
                idempotency_key: "ep".into(),
                ..CreateEndpoint::default()
            },
        )
        .await
        .unwrap();
    let id = ep.endpoint.record.id.clone();
    h.service
        .start_endpoint(
            &user(),
            EndpointAction {
                namespace: ns.into(),
                project_id: project.clone(),
                endpoint_id: id.clone(),
                idempotency_key: "start".into(),
            },
        )
        .await
        .unwrap();
    settle(r, store, ns, &project, &id, Some(EndpointState::Running)).await;
    let running = endpoint(store, &project, &id).await.unwrap();
    let compute_id = running.compute_id.clone().unwrap();
    let password = format!("it-{}", now_ms());
    compose(
        &st.bin,
        &st.compose_dir,
        ns,
        &[
            "exec",
            "-T",
            &compute_id.to_lowercase(),
            "/usr/local/bin/psql",
            "-h",
            "127.0.0.1",
            "-p",
            "55433",
            "-U",
            "cloud_admin",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            &format!("CREATE ROLE it_idle LOGIN PASSWORD '{password}'"),
        ],
    );
    let addr = running.backend_addr.clone().unwrap();
    let (host, port) = addr.rsplit_once(':').unwrap();
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host={host} port={port} user=it_idle password={password} dbname=postgres \
             connect_timeout=10"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    // A 5 s timeout, set on the record (the API's minimum is 60 s), then a
    // 15 s query: one command, nothing sent while it runs.
    let key = EndpointKey {
        project_id: project.clone(),
        id: id.clone(),
    };
    let got = store.get::<EndpointRec>(&key).await.unwrap().unwrap();
    let mut rec = got.record;
    rec.suspend_timeout_s = 5;
    store
        .api_writer()
        .put(&rec, Some(got.version))
        .await
        .unwrap();
    let query = tokio::spawn(async move {
        client
            .query_one("SELECT 1 FROM pg_sleep(15)", &[])
            .await
            .map(|_| ())
    });
    let ctl = HttpComputeCtl::new(st.keys.clone(), Clock::system());
    let compute = store
        .get::<ComputeRec>(&ComputeKey {
            id: compute_id.clone(),
        })
        .await
        .unwrap()
        .unwrap()
        .record;
    let begun = Instant::now();
    let mut seen = Vec::new();
    while !query.is_finished() {
        if let Err(e) = r.reconcile_project(ns, &project).await {
            eprintln!("pass: {e}");
        }
        let now = endpoint(store, &project, &id).await.unwrap();
        assert_eq!(now.state, EndpointState::Running, "{now:?}");
        assert_eq!(now.compute_id.as_deref(), Some(compute_id.as_str()));
        match ctl.status(&compute).await.unwrap().last_active {
            Activity::At(at) => seen.push(at),
            other => panic!("compute_ctl's last_active during a query: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    query.await.unwrap().unwrap();
    assert!(
        begun.elapsed() >= Duration::from_secs(14),
        "the query ran {:?}",
        begun.elapsed()
    );
    let moves = seen.windows(2).filter(|w| w[1] > w[0]).count();
    assert!(moves >= 10, "last_active moved {moves} times: {seen:?}");

    // Done, and the client gone: suspended after the timeout.
    let ended = Instant::now();
    settle(r, store, ns, &project, &id, Some(EndpointState::Suspended)).await;
    assert!(
        ended.elapsed() >= Duration::from_secs(4),
        "suspended {:?} after the query",
        ended.elapsed()
    );
    let id: ComputeId = compute_id.parse().unwrap();
    assert_eq!(st.rt.status(&id).await.unwrap(), RuntimeStatus::Missing);
}
