//! Fake pageserver and safekeeper management APIs for the Postgres tests.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde_json::{Value, json};

pub const TENANT: &str = "62fd82b32a1e0095934934b0921a2301";
pub const TIMELINE: &str = "3957e2ba3f5269e8eb03906ea3b3d0b7";

/// What the fakes saw and serve.
#[derive(Default)]
pub struct Fake {
    /// The pageserver's uploaded LSN, as a number; it gains `step` per read.
    pub remote: u64,
    pub step: u64,
    /// The safekeeper's flushed LSN.
    pub flush: u64,
    /// Every `PATCH /v1/tenant/config` body.
    pub patches: Vec<Value>,
    /// Every timeline created (branches).
    pub created: Vec<Value>,
    /// The tenant's own `checkpoint_timeout` override, if any.
    pub own_override: Option<Value>,
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

pub type Shared = Arc<Mutex<Fake>>;

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });
    format!("http://{addr}")
}

pub async fn fakes(fake: Fake) -> (Shared, String, String) {
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
