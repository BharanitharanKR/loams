//! Idle detection, the suspend and the lifecycle observer (PG2 Task 12), on
//! the local (embedded) store with `endpoints/env.rs`'s fakes: the
//! reconciler is driven pass by pass on a hand-moved clock, its compute's
//! `compute_ctl` reports a `last_active`, and a fake PgDog reports the
//! endpoint's clients.

#[macro_use]
mod common;

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

use std::time::{Duration, SystemTime};

use loams_pg_control::model::{Cu, DesiredState, EndpointState, EndpointType, OperationState};
use loams_pg_control::observer::{ComputeLifecycleEvent, LifecycleKind};
use loams_pg_control::reconcile::Pass;
use loams_pg_control::reconcile::endpoint::CtlStatus;
use loams_pg_control::service::endpoints::{CreateEndpoint, DeleteEndpoint};
use loams_pg_control::store::conformance::{Factory, local_factory};

use crate::common::user;
use crate::env::{Env, FakeActivity, NS, ORG};

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

const S: u64 = 1000;

impl Env {
    /// A ready project with a running read-write endpoint on its default
    /// branch, whose suspend timeout is `timeout_s`: (project, endpoint).
    async fn running_endpoint(&self, timeout_s: u32) -> (String, String) {
        let (project, _) = self.ready_project().await;
        let id = self
            .create_with(CreateEndpoint {
                namespace: NS.into(),
                project_id: project.clone(),
                kind: EndpointType::ReadWrite,
                suspend_timeout_s: Some(timeout_s),
                idempotency_key: "ep1".into(),
                ..CreateEndpoint::default()
            })
            .await
            .endpoint
            .record
            .id;
        self.start(&project, &id, "start1").await;
        self.running(&project, &id).await;
        (project, id)
    }

    /// Moves the clock by `ms`, then passes once.
    async fn after(&self, ms: u64, project: &str) -> Pass {
        self.h.clock.advance_ms(ms);
        self.pass(project).await
    }

    fn now(&self) -> u64 {
        self.h.clock.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn compute_id(&self, project: &str, id: &str) -> Option<String> {
        self.endpoint(project, id).await.and_then(|e| e.compute_id)
    }

    /// Passes every 30 s (the resync) for `total_s`, the endpoint running
    /// with the same compute throughout.
    async fn stays_running(&self, project: &str, id: &str, total_s: u64) {
        let compute = self.compute_id(project, id).await;
        for _ in 0..total_s / 30 {
            self.after(30 * S, project).await;
            let rec = self.endpoint(project, id).await.expect("the endpoint");
            assert_eq!(rec.state, EndpointState::Running, "{rec:?}");
            assert_eq!(rec.compute_id, compute, "the same compute");
        }
    }

    /// The endpoint was suspended by idle detection: no compute, its
    /// container gone after a clean `/terminate`, asked to stay suspended.
    async fn suspended_by_idle(&self, project: &str, id: &str, compute: &str) {
        let rec = self.endpoint(project, id).await.expect("the endpoint");
        assert_eq!(rec.state, EndpointState::Suspended, "{rec:?}");
        assert_eq!(rec.desired, DesiredState::Suspended);
        assert_eq!(rec.compute_id, None);
        assert!(rec.open_ops.is_empty(), "no operation: {:?}", rec.open_ops);
        assert!(self.runtime.running().is_empty());
        assert_eq!(
            self.runtime.stops().last().map(String::as_str),
            Some(compute)
        );
        assert!(self.ctl.terminated().iter().any(|c| c == compute));
        assert!(
            self.compute(compute).await.is_none(),
            "its C/ record removed"
        );
    }
}

/// A compute with no activity (no client at PgDog, `compute_ctl`'s
/// `last_active` not moving) is suspended once `suspend_timeout_s` has
/// passed since it started, not before; a pass comes back when the suspend
/// is due; a later `StartEndpoint` runs a new compute behind the same
/// address.
#[tokio::test]
async fn suspends_after_timeout_when_idle() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let compute = e.compute_id(&project, &id).await.unwrap();
    let addr = e.endpoint(&project, &id).await.unwrap().backend_addr;

    let p = e.after(30 * S, &project).await;
    assert!(
        !matches!(p, Pass::Again(_)),
        "not due before the resync: {p:?}"
    );
    // 50 s idle: due in 10 s, sooner than the resync.
    assert_eq!(
        e.after(20 * S, &project).await,
        Pass::Again(Duration::from_secs(10))
    );
    assert_eq!(
        e.after(9 * S, &project).await,
        Pass::Again(Duration::from_secs(1))
    );
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    e.after(S, &project).await;
    e.suspended_by_idle(&project, &id, &compute).await;
    assert!(e.pgdog.reads() > 0, "PgDog's pools were read");

    // At rest: passes do nothing more.
    assert_eq!(e.after(600 * S, &project).await, Pass::Idle);
    assert_eq!(e.runtime.starts().len(), 1);

    // A start (the waker's, or the API's) runs a new compute.
    e.start(&project, &id, "start2").await;
    e.running(&project, &id).await;
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert_ne!(rec.compute_id.as_deref(), Some(compute.as_str()));
    assert_eq!(rec.backend_addr, addr, "the same address");
}

/// A `StartEndpoint` on an endpoint that is already running is activity:
/// asked just before the timeout runs out, it is not followed by a suspend
/// (the client that asked is about to connect); the timeout runs again from
/// the pass that settled it (PR #463 review).
#[tokio::test]
async fn a_start_on_a_running_endpoint_is_activity() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let compute = e.compute_id(&project, &id).await.unwrap();
    e.after(30 * S, &project).await;
    e.h.clock.advance_ms(29 * S);
    let op = e.start(&project, &id, "start2").await;
    // 64 s idle when the pass settles the start: not suspended.
    e.after(5 * S, &project).await;
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    e.after(59 * S, &project).await;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    e.after(S, &project).await;
    e.suspended_by_idle(&project, &id, &compute).await;
}

/// `suspend_timeout_s = 0`: never suspended, however long it sits idle, and
/// PgDog is not even read.
#[tokio::test]
async fn zero_timeout_never_suspends() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(0).await;
    e.ctl.activity(FakeActivity::At(e.now() - 3600 * S));
    e.stays_running(&project, &id, 3600).await;
    for _ in 0..5 {
        e.after(24 * 3600 * S, &project).await;
    }
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    assert_eq!(e.runtime.stops(), Vec::<String>::new());
    assert_eq!(e.pgdog.reads(), 0);
}

/// PgDog's active or waiting clients on the endpoint's pools keep it
/// running. The same client, active on every sample through one long
/// query that sends no new command, counts each time (the SQ1 lesson). A
/// client of another project's pool does not count. Once the clients are
/// gone, the timeout runs from the last sample that saw one.
#[tokio::test]
async fn active_client_blocks_suspend() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let compute = e.compute_id(&project, &id).await.unwrap();

    // `shop` routes to the default branch's read-write endpoint.
    e.pgdog.clients("shop", 1, 0);
    e.stays_running(&project, &id, 600).await;
    // Only waiting clients (the pool is saturated).
    e.pgdog.clients("shop", 0, 0);
    e.pgdog.clients("shop__main.app", 0, 2);
    e.stays_running(&project, &id, 300).await;

    // Gone, and only another project's clients left.
    e.pgdog.clients("shop__main.app", 0, 0);
    e.pgdog.clients("other", 7, 7);
    e.after(30 * S, &project).await;
    e.after(29 * S, &project).await;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    e.after(S, &project).await;
    e.suspended_by_idle(&project, &id, &compute).await;
}

/// A logical replication subscriber keeps the compute running: while one
/// streams from it (a walsender other than walproposer), or a subscription
/// of its own runs, the fork's `compute_ctl` reports `last_active` as now
/// on every check (`compute_tools/src/monitor.rs`), with no client at
/// PgDog. Once it stops, `last_active` stays put and the compute suspends.
#[tokio::test]
async fn logical_subscriber_blocks_suspend() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let compute = e.compute_id(&project, &id).await.unwrap();

    e.ctl.activity(FakeActivity::Now);
    e.stays_running(&project, &id, 1200).await;

    // The subscriber went away 10 s before this sample.
    e.ctl.activity(FakeActivity::At(e.now() + 20 * S));
    e.after(30 * S, &project).await;
    e.after(49 * S, &project).await;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Running));
    e.after(S, &project).await;
    e.suspended_by_idle(&project, &id, &compute).await;
}

/// A sample that cannot be taken never suspends: `compute_ctl` not
/// answering, an unreadable `last_active`, PgDog not answering.
#[tokio::test]
async fn unanswered_samples_never_suspend() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let compute = e.compute_id(&project, &id).await.unwrap();

    e.ctl.lock().default = None;
    e.stays_running(&project, &id, 300).await;
    e.ctl.answer_by_default(CtlStatus::Running);
    e.ctl.activity(FakeActivity::Unreadable);
    e.stays_running(&project, &id, 300).await;
    e.ctl.activity(FakeActivity::NotYet);
    e.pgdog.down(true);
    e.stays_running(&project, &id, 300).await;

    // Answering again, still idle: suspended.
    e.pgdog.down(false);
    e.after(30 * S, &project).await;
    e.suspended_by_idle(&project, &id, &compute).await;
}

/// The observer hears a compute start (with its CU) and, once it was idle,
/// its suspend; then the next compute's start and its suspend through the
/// API. Every event names the org, namespace, project, endpoint and
/// compute, at the reconciler's clock, after the batch that recorded it.
#[tokio::test]
async fn observer_sees_start_and_suspend() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(60).await;
    let first = e.compute_id(&project, &id).await.unwrap();
    let started_at = e.now();
    e.after(60 * S, &project).await;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Suspended));
    let suspended_at = e.now();

    e.start(&project, &id, "start2").await;
    e.running(&project, &id).await;
    let second = e.compute_id(&project, &id).await.unwrap();
    e.suspend(&project, &id, "suspend2").await;
    e.until(&project, "suspended", || async {
        e.state(&project, &id).await == Some(EndpointState::Suspended)
    })
    .await;

    let at = |ms: u64| SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
    let events = e.events.events();
    let summary: Vec<(String, LifecycleKind)> = events
        .iter()
        .map(|ev| (ev.compute.to_string(), ev.event.clone()))
        .collect();
    assert_eq!(
        summary,
        vec![
            (first.clone(), LifecycleKind::Started { cu: Cu(1) }),
            (first.clone(), LifecycleKind::Suspended),
            (second.clone(), LifecycleKind::Started { cu: Cu(1) }),
            (second, LifecycleKind::Suspended),
        ]
    );
    let ComputeLifecycleEvent {
        org,
        namespace,
        project: p,
        endpoint,
        at: when,
        ..
    } = &events[0];
    assert_eq!(
        (
            org.as_str(),
            namespace.as_str(),
            p.to_string(),
            endpoint.to_string()
        ),
        (ORG, NS, project.clone(), id.clone())
    );
    assert_eq!(*when, at(started_at));
    assert_eq!(events[1].at, at(suspended_at));
}

/// Every compute ends with one event: a crash is `Failed` with what
/// failed, a restart's old compute and the removed endpoint's are
/// `Suspended`; a compute that never ran sends no `Started`.
#[tokio::test]
async fn every_compute_ends_once() {
    let e = fakes!();
    let (project, id) = e.running_endpoint(300).await;
    let first = e.compute_id(&project, &id).await.unwrap();

    e.runtime.crash(&first);
    e.until(&project, "replaced after the crash", || async {
        e.state(&project, &id).await == Some(EndpointState::Running)
            && e.compute_id(&project, &id).await.as_deref() != Some(first.as_str())
    })
    .await;
    let second = e.compute_id(&project, &id).await.unwrap();

    e.h.service
        .restart_endpoint(&user(), Env::action(&project, &id, "restart1"))
        .await
        .unwrap();
    e.until(&project, "restarted", || async {
        e.state(&project, &id).await == Some(EndpointState::Running)
            && e.compute_id(&project, &id).await.as_deref() != Some(second.as_str())
    })
    .await;
    let third = e.compute_id(&project, &id).await.unwrap();

    // A fourth compute that never runs: compute_ctl stays starting until
    // the endpoint is deleted.
    e.ctl.answer_by_default(CtlStatus::Starting);
    e.runtime.crash(&third);
    e.until(&project, "a fourth compute", || async {
        e.compute_id(&project, &id)
            .await
            .is_some_and(|c| c != third)
    })
    .await;
    let fourth = e.compute_id(&project, &id).await.unwrap();
    e.h.service
        .delete_endpoint(
            &user(),
            DeleteEndpoint {
                namespace: NS.into(),
                project_id: project.clone(),
                endpoint_id: id.clone(),
                expected_version: None,
                idempotency_key: "del".into(),
            },
        )
        .await
        .unwrap();
    e.until(&project, "removed", || async {
        e.endpoint(&project, &id).await.is_none()
    })
    .await;

    let summary: Vec<(String, LifecycleKind)> = e
        .events
        .events()
        .iter()
        .map(|ev| (ev.compute.to_string(), ev.event.clone()))
        .collect();
    let crashed = LifecycleKind::Failed {
        reason: "the compute's container exited (Some(1))".into(),
    };
    assert_eq!(
        summary,
        vec![
            (first.clone(), LifecycleKind::Started { cu: Cu(1) }),
            (first, crashed.clone()),
            (second.clone(), LifecycleKind::Started { cu: Cu(1) }),
            (second, LifecycleKind::Suspended),
            (third.clone(), LifecycleKind::Started { cu: Cu(1) }),
            (third, crashed),
            (fourth, LifecycleKind::Suspended),
        ]
    );
    assert!(e.idle.is_empty(), "every ended compute is forgotten");
}
