//! The PG2a exit (PG2 Task 9): a project and a branch made through the
//! service reach `ready`, the pageserver of `deploy/loams-postgres-dev`
//! (pinned digests) lists both timelines, and `loams-wal` answers the
//! branch's WAL heads. Ignored by default; `pg2-e2e.yml` runs it:
//!
//! ```text
//! (cd deploy/loams-postgres-dev && docker compose up -d rustfs create-bucket storage_broker pageserver)
//! cargo build -p loams-safekeeper --features server --bin loams-wal
//! target/debug/loams-wal --store mem --listen-pg 127.0.0.1:5455 --listen-http 127.0.0.1:7677 &
//! NEON_WAL=http://127.0.0.1:7677 cargo test -p loams-pg-control --test e2e_deploy_neon -- --ignored
//! ```
//!
//! `NEON_PAGESERVER` (default `http://127.0.0.1:9898`) and `NEON_WAL`
//! (default `http://127.0.0.1:7677`) override the addresses. `NEON_WAL` is
//! `loams-wal`, or the stack's stock `safekeeper1` (`http://127.0.0.1:7676`),
//! which the release stack's `pg-control` uses: `pg2-e2e.yml` runs both
//! (0.1.0 blocker B2, CreateProject stuck in CREATING on `missing field
//! mconf`).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use loams_pg_control::StoreOptions;
use loams_pg_control::ids::{BranchId, ProjectId, tenant_id, timeline_id};
use loams_pg_control::model::{BranchState, ProjectState};
use loams_pg_control::neon::NeonClients;
use loams_pg_control::reconcile::{Reconciler, ReconcilerConfig};
use loams_pg_control::secrets::{FileSecretStore, KeySource};
use loams_pg_control::service::branches::CreateBranch;
use loams_pg_control::service::projects::{CreateProject, DeleteProject};
use loams_pg_control::service::{Caller, PgService, ServiceConfig};
use loams_pg_control::store::conformance::local_factory;
use loams_postgres::pageserver::{NeonClient, NeonEndpoints};
use loams_postgres::wal::WalClient;
use loams_postgres::{TenantId, TimelineId};

fn url(var: &str, default: &str) -> url::Url {
    std::env::var(var)
        .unwrap_or_else(|_| default.into())
        .parse()
        .unwrap()
}

fn clients() -> NeonClients {
    NeonClients {
        storage: NeonClient::new(
            NeonEndpoints {
                pageserver: url("NEON_PAGESERVER", "http://127.0.0.1:9898"),
                storcon: None,
            },
            None,
        )
        .unwrap(),
        wal: WalClient::new(url("NEON_WAL", "http://127.0.0.1:7677"), None).unwrap(),
    }
}

/// Passes over the project until `done` says so (at most a minute); then
/// fails with the namespace's operations.
async fn reconcile_until<S, N, F, Fut>(
    r: &Reconciler<S, N>,
    service: &PgService<NeonClients>,
    ns: &str,
    id: &str,
    done: F,
) where
    S: loams_pg_control::PgControlStore,
    N: loams_pg_control::neon::NeonWrite,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..120 {
        if let Err(e) = r.reconcile_project(ns, id).await {
            eprintln!("pass: {e}");
        }
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (ops, _) = service.list_operations(ns, 0, "").await.unwrap();
    panic!("project {id} did not converge: {ops:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs deploy/loams-postgres-dev running"]
async fn e2e_create_project_and_branch() {
    let dir = tempfile::Builder::new()
        .prefix("e2e-pg-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let store = local_factory(dir.path())
        .store(StoreOptions::default())
        .await
        .unwrap();
    let secrets = Arc::new(
        FileSecretStore::open(
            dir.path().join("secrets.age"),
            &KeySource::File(dir.path().join("secrets.key")),
        )
        .unwrap(),
    );
    let neon = clients();
    let service = PgService::new(
        store.clone(),
        neon.clone(),
        secrets.clone(),
        ServiceConfig::default(),
    );
    let reconciler = Reconciler::new(
        store.clone(),
        neon.clone(),
        secrets,
        ReconcilerConfig::new("e2e"),
    );
    let caller = Caller {
        principal: "user:e2e".into(),
        admin: true,
    };
    let ns = "e2e";

    let created = service
        .create_project(
            &caller,
            CreateProject {
                namespace: ns.into(),
                name: format!("p{}", ulid::Ulid::generate().to_string().to_lowercase()),
                idempotency_key: "p1".into(),
                ..CreateProject::default()
            },
        )
        .await
        .unwrap();
    let project_id = created.project.record.id.clone();
    reconcile_until(&reconciler, &service, ns, &project_id, || async {
        service
            .get_project(ns, &project_id)
            .await
            .unwrap()
            .record
            .state
            == ProjectState::Ready
    })
    .await;

    let branch = service
        .create_branch(
            &caller,
            CreateBranch {
                namespace: ns.into(),
                project_id: project_id.clone(),
                name: "dev".into(),
                idempotency_key: "b1".into(),
                ..CreateBranch::default()
            },
        )
        .await
        .unwrap();
    let branch_id = branch.branch.branch.record.id.clone();
    reconcile_until(&reconciler, &service, ns, &project_id, || async {
        service
            .get_branch(ns, &project_id, &branch_id)
            .await
            .unwrap()
            .branch
            .record
            .state
            == BranchState::Ready
    })
    .await;

    // The pageserver lists both timelines of the project's tenant.
    let pid: ProjectId = project_id.parse().unwrap();
    let main_id: BranchId = created
        .project
        .record
        .default_branch_id
        .clone()
        .unwrap()
        .parse()
        .unwrap();
    let dev_id: BranchId = branch_id.parse().unwrap();
    let mut listed: Vec<TimelineId> = neon
        .storage
        .list_timelines(TenantId(tenant_id(&pid)))
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.timeline_id)
        .collect();
    listed.sort();
    let mut want = vec![
        TimelineId(timeline_id(&main_id)),
        TimelineId(timeline_id(&dev_id)),
    ];
    want.sort();
    assert_eq!(listed, want);
    // GetBranch reads the WAL heads of a ready branch.
    let got = service
        .get_branch(ns, &project_id, &branch_id)
        .await
        .unwrap();
    assert!(got.wal.is_some(), "{got:?}");

    // Clean up: the project's timelines go (its tenant stays, R7.10).
    service
        .delete_project(
            &caller,
            DeleteProject {
                namespace: ns.into(),
                project_id: project_id.clone(),
                expected_version: None,
                idempotency_key: "d1".into(),
            },
        )
        .await
        .unwrap();
    reconcile_until(&reconciler, &service, ns, &project_id, || async {
        service.get_project(ns, &project_id).await.is_err()
    })
    .await;
}
