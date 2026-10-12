//! An endpoint's lifecycle end to end (PG2 Task 11): the API service and
//! the endpoint reconciler drive real computes through the compose runtime
//! and each compute's `compute_ctl`, on `deploy/loams-postgres-dev`'s
//! storage. Ignored by default; `pg2-e2e.yml` runs it beside `it_compose`:
//!
//! ```text
//! (cd deploy/loams-postgres-dev && docker compose up -d rustfs create-bucket storage_broker pageserver safekeeper1)
//! cargo test -p loams-pg-control --test it_endpoint -- --ignored
//! ```
//!
//! `StartEndpoint` reaches `running` and `SELECT 1` answers on the
//! endpoint's address; `SuspendEndpoint` stops the compute; a second start
//! runs a new compute at the same address; `DeleteEndpoint` removes it and
//! gives the address up. The settings are `it_compose`'s (environment):
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
    BranchRec, BranchState, EndpointKey, EndpointRec, EndpointState, EndpointType, OperationState,
    ProjectRec, ProjectState, WalService,
};
use loams_pg_control::reconcile::endpoint::{Computes, HttpComputeCtl};
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

const NS: &str = "it-ep";

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
struct Down(ComposeBin, PathBuf);

impl Drop for Down {
    fn drop(&mut self) {
        let _ = std::process::Command::new(&self.0.program)
            .args(&self.0.args)
            .args(["-p", &format!("loams-pg-{NS}"), "down", "--timeout", "5"])
            .current_dir(&self.1)
            .status();
    }
}

/// `compose <args>` on the namespace's project: its stdout, or a panic.
fn compose(bin: &ComposeBin, dir: &Path, args: &[&str]) -> String {
    let file = dir.join(format!("{NS}.yaml"));
    let out = std::process::Command::new(&bin.program)
        .args(&bin.args)
        .args([
            "-f",
            &file.display().to_string(),
            "-p",
            &format!("loams-pg-{NS}"),
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

/// Passes until the endpoint is in `want` (or gone, for `None`), for up to
/// three minutes.
async fn settle(
    r: &Reconciler<KvControlStore, FakeNeon>,
    store: &KvControlStore,
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
        if let Err(e) = r.reconcile_project(NS, project).await {
            eprintln!("pass: {e}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs deploy/loams-postgres-dev running, and docker or podman"]
async fn it_endpoint_starts_suspends_and_deletes() {
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
            namespace: NS.into(),
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
    let _down = Down(bin.clone(), dir.path().to_path_buf());
    let mut images = images::pinned().unwrap();
    if let Ok(image) = std::env::var("COMPUTE_IMAGE") {
        images.insert(17, image);
    }
    let r = Reconciler::new(
        store.clone(),
        h.neon.clone(),
        h.secrets.clone(),
        ReconcilerConfig::new("it-endpoint"),
    )
    .with_computes(Computes::new(
        rt.clone(),
        Arc::new(HttpComputeCtl::new(keys.clone(), Clock::system())),
        keys.clone(),
        images,
        sources,
    ));

    // CreateEndpoint, StartEndpoint: running.
    let ep = h
        .service
        .create_endpoint(
            &user(),
            CreateEndpoint {
                namespace: NS.into(),
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
        namespace: NS.into(),
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
        &r,
        &store,
        &project.to_string(),
        &id,
        Some(EndpointState::Running),
    )
    .await;
    assert_eq!(
        h.service.get_operation(&start.id).await.unwrap().state,
        OperationState::Succeeded
    );
    let running = endpoint(&store, &project.to_string(), &id).await.unwrap();
    let first: ComputeId = running.compute_id.clone().unwrap().parse().unwrap();
    let addr = running.backend_addr.clone().unwrap();

    // A login role made inside the compute (role passwords are Task 24's),
    // then SELECT 1 through the endpoint's address.
    let service = first.to_string().to_lowercase();
    let password = format!("it-{}", now_ms());
    compose(
        &bin,
        &compose_dir,
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
        &r,
        &store,
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
        &r,
        &store,
        &project.to_string(),
        &id,
        Some(EndpointState::Running),
    )
    .await;
    let again = endpoint(&store, &project.to_string(), &id).await.unwrap();
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
                namespace: NS.into(),
                project_id: project.to_string(),
                endpoint_id: id.clone(),
                expected_version: None,
                idempotency_key: "del".into(),
            },
        )
        .await
        .unwrap();
    settle(&r, &store, &project.to_string(), &id, None).await;
    assert_eq!(rt.status(&second).await.unwrap(), RuntimeStatus::Missing);
    let _ = stop_upcall.send(());
}
