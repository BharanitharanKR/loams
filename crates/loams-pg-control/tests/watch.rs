//! `WatchProject` (PG2 Task 11): a snapshot, then changes, then heartbeats;
//! resumable from a cursor. On the local store with a fake Neon.

#[macro_use]
mod common;

use std::time::Duration;

use futures::StreamExt;
use loams_pg_control::model::{EndpointKey, EndpointRec, EndpointType, ProjectKey, ProjectRec};
use loams_pg_control::service::Reason;
use loams_pg_control::service::endpoints::CreateEndpoint;
use loams_pg_control::service::projects::CreateProject;
use loams_pg_control::service::watch::{ResourceKind, WatchEvent, WatchItem, WatchOptions};
use loams_pg_control::store::conformance::{Factory, local_factory};
use loams_pg_control::{PgControlStore, StoreError};

use crate::common::{Harness, user};

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

const NS: &str = "acme";

fn quick() -> WatchOptions {
    WatchOptions {
        poll: Duration::from_millis(20),
        heartbeat: Duration::from_millis(300),
    }
}

async fn project(h: &Harness) -> (String, String) {
    let created = h
        .service
        .create_project(
            &user(),
            CreateProject {
                namespace: NS.into(),
                name: "shop".into(),
                idempotency_key: "create-shop".into(),
                ..CreateProject::default()
            },
        )
        .await
        .expect("a project");
    (
        created.project.record.id.clone(),
        created
            .project
            .record
            .default_branch_id
            .clone()
            .expect("main"),
    )
}

async fn next<S>(watch: &mut S) -> WatchItem
where
    S: futures::Stream<Item = Result<WatchItem, loams_pg_control::service::ServiceError>> + Unpin,
{
    tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await
        .expect("an answer within 10 s")
        .expect("the watch goes on")
        .expect("an item")
}

/// A snapshot of the project and its branch, then each change as it
/// happens (an endpoint created, then removed), then heartbeats that carry
/// the last cursor.
#[tokio::test]
async fn watch_sends_snapshot_then_changes_then_heartbeat() {
    let h = harness!();
    let (project_id, main) = project(&h).await;
    let mut watch = Box::pin(
        h.service
            .watch_project(NS, &project_id, "", quick())
            .await
            .expect("a watch"),
    );
    let first = next(&mut watch).await;
    let WatchEvent::Snapshot(snap) = &first.event else {
        panic!("a snapshot first: {first:?}");
    };
    assert!(!first.snapshot_reset);
    assert_eq!(snap.project.record.id, project_id);
    assert_eq!(snap.branches.len(), 1);
    assert_eq!(snap.branches[0].branch.record.id, main);
    assert!(snap.branches[0].is_default);
    assert!(snap.endpoints.is_empty());

    let ep = h
        .service
        .create_endpoint(
            &user(),
            CreateEndpoint {
                namespace: NS.into(),
                project_id: project_id.clone(),
                kind: EndpointType::ReadWrite,
                idempotency_key: "ep1".into(),
                ..CreateEndpoint::default()
            },
        )
        .await
        .expect("an endpoint");
    let id = ep.endpoint.record.id.clone();
    let created = next(&mut watch).await;
    match &created.event {
        WatchEvent::Endpoint(v) => assert_eq!(v.endpoint.record.id, id),
        other => panic!("the endpoint: {other:?}"),
    }
    assert_ne!(created.cursor, first.cursor);

    h.store
        .api_writer()
        .delete::<EndpointRec>(
            &EndpointKey {
                project_id: project_id.clone(),
                id: id.clone(),
            },
            ep.endpoint.version,
        )
        .await
        .expect("removed");
    let removed = next(&mut watch).await;
    assert_eq!(
        removed.event,
        WatchEvent::Removed {
            kind: ResourceKind::Endpoint,
            id
        }
    );
    // Removing the endpoint brings the client back to the first state.
    assert_eq!(removed.cursor, first.cursor);

    let beat = next(&mut watch).await;
    assert_eq!(beat.event, WatchEvent::Heartbeat);
    assert_eq!(beat.cursor, removed.cursor);
}

/// A cursor naming the current state resumes without a snapshot; any other
/// gets a snapshot that replaces what the client held.
#[tokio::test]
async fn resume_from_cursor() {
    let h = harness!();
    let (project_id, _) = project(&h).await;
    let mut watch = Box::pin(
        h.service
            .watch_project(NS, &project_id, "", quick())
            .await
            .expect("a watch"),
    );
    let first = next(&mut watch).await;
    drop(watch);

    let mut resumed = Box::pin(
        h.service
            .watch_project(NS, &project_id, &first.cursor, quick())
            .await
            .expect("a watch"),
    );
    let beat = next(&mut resumed).await;
    assert_eq!(beat.event, WatchEvent::Heartbeat, "no snapshot: {beat:?}");
    assert_eq!(beat.cursor, first.cursor);
    drop(resumed);

    for stale in ["w1.00000000000000000000000000000000", "garbage"] {
        let mut stale_watch = Box::pin(
            h.service
                .watch_project(NS, &project_id, stale, quick())
                .await
                .expect("a watch"),
        );
        let snap = next(&mut stale_watch).await;
        assert!(matches!(snap.event, WatchEvent::Snapshot(_)), "{snap:?}");
        assert!(snap.snapshot_reset);
        assert_eq!(snap.cursor, first.cursor);
    }
}

/// An unknown project is `project_not_found`; a project deleted under a
/// watch ends it with `project_not_found`.
#[tokio::test]
async fn a_watch_ends_with_its_project() {
    let h = harness!();
    let err = h
        .service
        .watch_project(NS, "prj-01K7ZZZZZZZZZZZZZZZZZZZZZZ", "", quick())
        .await
        .err()
        .expect("refused");
    assert_eq!(err.reason, Reason::ProjectNotFound);

    let (project_id, _) = project(&h).await;
    let mut watch = Box::pin(
        h.service
            .watch_project(NS, &project_id, "", quick())
            .await
            .expect("a watch"),
    );
    next(&mut watch).await;
    let key = ProjectKey {
        namespace: NS.into(),
        id: project_id.clone(),
    };
    let p = h.store.get::<ProjectRec>(&key).await.unwrap().unwrap();
    match h
        .store
        .api_writer()
        .delete::<ProjectRec>(&key, p.version)
        .await
    {
        Ok(()) | Err(StoreError::NotFound) => {}
        Err(e) => panic!("{e}"),
    }
    let end = tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await
        .expect("an answer")
        .expect("the error");
    assert_eq!(end.expect_err("the end").reason, Reason::ProjectNotFound);
    assert!(watch.next().await.is_none(), "then the stream ends");
}
