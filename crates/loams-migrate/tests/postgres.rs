//! Loams Postgres storage in an upgrade snapshot (UP1 Task 9): the drain
//! before `safekeeper1` is recreated, and a branch per timeline, against
//! fake pageserver and safekeeper management APIs.

#[path = "support/neon.rs"]
mod neon;

use std::time::Duration;

use loams_migrate::snapshot::postgres::PostgresStorage;
use neon::{Fake, TIMELINE, fakes};
use serde_json::{Value, json};

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
