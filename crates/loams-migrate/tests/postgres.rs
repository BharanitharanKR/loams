//! Loams Postgres storage in an upgrade snapshot (UP1 Task 9): the drain
//! before `safekeeper1` is recreated, and a branch per timeline, against
//! fake pageserver and safekeeper management APIs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::routing::{get, patch};
use axum::{Json, Router};
use loams_migrate::snapshot::postgres::PostgresStorage;
use serde_json::{Value, json};

const TENANT: &str = "62fd82b32a1e0095934934b0921a2301";
const TIMELINE: &str = "3957e2ba3f5269e8eb03906ea3b3d0b7";

/// What the fakes saw and serve.
#[derive(Default)]
struct Fake {
    /// The pageserver's uploaded LSN, as a number; it gains `step` per read.
    remote: u64,
    step: u64,
    /// The safekeeper's flushed LSN.
    flush: u64,
    /// Every `PATCH /v1/tenant/config` body.
    patches: Vec<Value>,
    /// Every timeline created (branches).
    created: Vec<Value>,
    /// The tenant's own `checkpoint_timeout` override, if any.
    own_override: Option<Value>,
}

fn lsn(n: u64) -> String {
    format!("{:X}/{:X}", n >> 32, n & 0xffff_ffff)
}

fn timeline_info(fake: &Fake) -> Value {
    json!({
        "tenant_id": TENANT, "timeline_id": TIMELINE,
        "ancestor_timeline_id": null, "ancestor_lsn": null,
        "last_record_lsn": lsn(fake.flush), "prev_record_lsn": null,
        "disk_consistent_lsn": lsn(fake.remote), "remote_consistent_lsn": lsn(fake.remote),
        "initdb_lsn": lsn(1), "min_readable_lsn": lsn(1), "applied_gc_cutoff_lsn": lsn(1),
        "current_logical_size": 0, "pg_version": 17, "state": "Active"
    })
}

type Shared = Arc<Mutex<Fake>>;

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });
    format!("http://{addr}")
}

async fn fakes(fake: Fake) -> (Shared, String, String) {
    let shared: Shared = Arc::new(Mutex::new(fake));
    let pageserver = Router::new()
        .route("/v1/tenant", get(|| async { Json(json!([{"id": TENANT}])) }))
        .route(
            "/v1/tenant/{t}/config",
            get(|State(s): State<Shared>, Path(_t): Path<String>| async move {
                let f = s.lock().expect("lock");
                let mut overrides = serde_json::Map::new();
                if let Some(v) = &f.own_override {
                    overrides.insert("checkpoint_timeout".into(), v.clone());
                }
                Json(json!({"tenant_specific_overrides": overrides, "effective_config": {}}))
            }),
        )
        .route(
            "/v1/tenant/config",
            patch(|State(s): State<Shared>, Json(body): Json<Value>| async move {
                s.lock().expect("lock").patches.push(body);
                Json(json!(null))
            }),
        )
        .route(
            "/v1/tenant/{t}/timeline",
            get(|State(s): State<Shared>, Path(_t): Path<String>| async move {
                Json(json!([timeline_info(&s.lock().expect("lock"))]))
            })
            .post(
                |State(s): State<Shared>, Path(_t): Path<String>, Json(body): Json<Value>| async move {
                    let mut f = s.lock().expect("lock");
                    f.created.push(body.clone());
                    let mut info = timeline_info(&f);
                    info["timeline_id"] = body["new_timeline_id"].clone();
                    Json(info)
                },
            ),
        )
        .route(
            "/v1/tenant/{t}/timeline/{tl}",
            get(
                |State(s): State<Shared>, Path((_t, _tl)): Path<(String, String)>| async move {
                    let mut f = s.lock().expect("lock");
                    let info = timeline_info(&f);
                    f.remote = (f.remote + f.step).min(f.flush);
                    Json(info)
                },
            ),
        )
        .with_state(shared.clone());
    let safekeeper = Router::new()
        .route(
            "/v1/tenant/{t}/timeline/{tl}",
            get(
                |State(s): State<Shared>, Path((_t, _tl)): Path<(String, String)>| async move {
                    let f = s.lock().expect("lock");
                    Json(json!({
                        "tenant_id": TENANT, "timeline_id": TIMELINE, "term": 1,
                        "last_log_term": 1, "flush_lsn": lsn(f.flush), "commit_lsn": lsn(f.flush),
                        "backup_lsn": lsn(1), "remote_consistent_lsn": lsn(f.remote),
                        "peer_horizon_lsn": lsn(f.flush), "timeline_start_lsn": lsn(1),
                        "trimmed_lsn": lsn(1), "pg_version": 170000, "system_id": 7,
                        "wal_seg_size": 16777216
                    }))
                },
            ),
        )
        .with_state(shared.clone());
    (shared, serve(pageserver).await, serve(safekeeper).await)
}

#[tokio::test]
async fn safekeeper_recreate_waits_for_drain() {
    // A timeline whose upload lags: it catches up after a few polls.
    let (shared, pageserver, safekeeper) = fakes(Fake {
        remote: 0x100,
        step: 0x100,
        flush: 0x400,
        ..Fake::default()
    })
    .await;
    let storage = PostgresStorage::new(&pageserver, &safekeeper).expect("clients");
    let drained = storage
        .drain(Duration::from_secs(10), Duration::from_millis(10))
        .await
        .expect("drained");
    assert!(drained.iter().all(|t| t.drained), "{drained:?}");
    {
        let f = shared.lock().expect("lock");
        assert_eq!(f.patches.len(), 2, "lowered, then put back");
        assert_eq!(f.patches[0]["checkpoint_timeout"], "5s");
        assert_eq!(
            f.patches[1]["checkpoint_timeout"],
            Value::Null,
            "no override before"
        );
    }

    // A timeline that never catches up: refused, and the tenant's own
    // setting is put back anyway.
    let (shared, pageserver, safekeeper) = fakes(Fake {
        remote: 0x100,
        step: 0,
        flush: 0x400,
        own_override: Some(json!("1m")),
        ..Fake::default()
    })
    .await;
    let storage = PostgresStorage::new(&pageserver, &safekeeper).expect("clients");
    let err = storage
        .drain(Duration::from_millis(100), Duration::from_millis(10))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("postgres_not_drained"), "{err}");
    assert!(err.to_string().contains(TIMELINE), "{err}");
    let f = shared.lock().expect("lock");
    assert_eq!(
        f.patches.last().expect("restored")["checkpoint_timeout"],
        "1m"
    );
}

#[tokio::test]
async fn postgres_branch_per_timeline() {
    let (shared, pageserver, safekeeper) = fakes(Fake {
        remote: 0x400,
        flush: 0x400,
        ..Fake::default()
    })
    .await;
    let storage = PostgresStorage::new(&pageserver, &safekeeper).expect("clients");
    let branches = storage.branch_every_timeline().await.expect("branches");
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].timeline, TIMELINE);
    assert_eq!(branches[0].lsn, "0/400");
    let f = shared.lock().expect("lock");
    assert_eq!(f.created.len(), 1);
    assert_eq!(f.created[0]["ancestor_timeline_id"], TIMELINE);
    assert_eq!(f.created[0]["ancestor_start_lsn"], "0/400");
    assert_eq!(f.created[0]["new_timeline_id"], branches[0].branch.as_str());
}

#[test]
fn tikv_restore_targets_snapshot_version() {
    use loams_migrate::snapshot::tikv::{
        TikvMethod, check_restore_target, component, component_version,
    };
    let taken = component(TikvMethod::Br, "v8.5.8", 1024);
    assert_eq!(taken.store, "tikv");
    assert_eq!(component_version(&taken), Some("v8.5.8"));
    assert!(check_restore_target("v8.5.8", "8.5.8").is_ok());
    let err = check_restore_target("v8.5.8", "v8.5.9").unwrap_err();
    assert!(err.to_string().contains("component_downgrade"), "{err}");
    assert!(check_restore_target("v8.5.9", "v8.5.8").is_err());
}

/// Against a real Loams Postgres storage stack (`deploy/loams-postgres-dev`):
/// `LOAMS_TEST_PAGESERVER=http://127.0.0.1:9898
/// LOAMS_TEST_SAFEKEEPER=http://127.0.0.1:7676 cargo test -p loams-migrate
/// --test postgres -- --ignored`. The compose gate (UP1 Task 20) runs it.
#[tokio::test]
#[ignore = "needs a Loams Postgres storage stack"]
async fn postgres_drain_and_branch_on_a_real_stack() {
    let (Ok(pageserver), Ok(safekeeper)) = (
        std::env::var("LOAMS_TEST_PAGESERVER"),
        std::env::var("LOAMS_TEST_SAFEKEEPER"),
    ) else {
        panic!("set LOAMS_TEST_PAGESERVER and LOAMS_TEST_SAFEKEEPER");
    };
    let storage = PostgresStorage::new(&pageserver, &safekeeper).expect("clients");
    let drained = storage
        .drain(Duration::from_secs(300), Duration::from_secs(2))
        .await
        .expect("drained");
    assert!(drained.iter().all(|t| t.drained));
    let branches = storage.branch_every_timeline().await.expect("branches");
    assert_eq!(branches.len(), drained.len());
}
