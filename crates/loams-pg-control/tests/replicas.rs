//! Read-only endpoints and promotion (PG2 Task 16), on the local store
//! with `endpoints/env.rs`'s fakes: a read-only endpoint's compute is a
//! hot standby (`mode: Replica`); `RestartEndpoint{promote}` makes it the
//! branch's read-write endpoint once that one is down, after prewarming
//! its file cache, and swaps the two endpoints' types.

#[macro_use]
mod common;

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

use loams_pg_control::model::{EndpointState, EndpointType, OperationState};
use loams_pg_control::reconcile::endpoint::Prewarm;
use loams_pg_control::service::Reason;
use loams_pg_control::store::conformance::{Factory, local_factory};

use crate::common::user;
use crate::env::Env;

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

impl Env {
    /// A ready project with a read-write and a read-only endpoint on its
    /// default branch, both running: (project, rw, ro, ro's compute).
    async fn primary_and_replica(&self) -> (String, String, String, String) {
        let (project, _) = self.ready_project().await;
        let rw = self
            .create(&project, "", EndpointType::ReadWrite, "rw")
            .await
            .endpoint
            .record
            .id;
        let ro = self
            .create(&project, "", EndpointType::ReadOnly, "ro")
            .await
            .endpoint
            .record
            .id;
        self.start(&project, &rw, "start-rw").await;
        self.start(&project, &ro, "start-ro").await;
        self.running(&project, &rw).await;
        self.running(&project, &ro).await;
        let compute = self
            .endpoint(&project, &ro)
            .await
            .and_then(|e| e.compute_id)
            .expect("a compute");
        (project, rw, ro, compute)
    }

    async fn promote(&self, project: &str, endpoint: &str, key: &str) -> Result<String, Reason> {
        self.h
            .service
            .promote_endpoint(&user(), Self::action(project, endpoint, key))
            .await
            .map(|op| op.id)
            .map_err(|e| e.reason)
    }

    async fn kind(&self, project: &str, endpoint: &str) -> EndpointType {
        self.endpoint(project, endpoint)
            .await
            .expect("the endpoint")
            .kind
    }
}

/// A read-only endpoint's compute gets a hot-standby spec (`Replica`), and
/// any number of them run beside the branch's read-write endpoint.
#[tokio::test]
async fn ro_endpoint_starts_replica_spec() {
    let e = fakes!();
    let (project, rw, ro, compute) = e.primary_and_replica().await;
    let spec = e.upcall_spec(&compute).await;
    assert_eq!(spec["status"], "attached");
    assert_eq!(spec["spec"]["mode"], "Replica");
    // It streams the branch's WAL from the pool's safekeepers.
    let settings: std::collections::BTreeMap<String, String> = spec["spec"]["cluster"]["settings"]
        .as_array()
        .expect("settings")
        .iter()
        .map(|s| {
            (
                s["name"].as_str().unwrap_or_default().to_owned(),
                s["value"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    let conninfo = settings.get("primary_conninfo").expect("primary_conninfo");
    assert!(
        conninfo.starts_with("host=wal port=5454 options='-c timeline_id="),
        "{conninfo}"
    );
    assert!(conninfo.contains(" tenant_id="), "{conninfo}");
    assert!(
        conninfo.ends_with("application_name=replica replication=true"),
        "{conninfo}"
    );
    assert!(
        settings
            .get("primary_slot_name")
            .is_some_and(|s| s.starts_with("repl_"))
    );
    assert_eq!(settings.get("hot_standby").map(String::as_str), Some("on"));
    let primary = e
        .endpoint(&project, &rw)
        .await
        .and_then(|r| r.compute_id)
        .expect("a compute");
    let primary_spec = e.upcall_spec(&primary).await;
    assert_eq!(primary_spec["spec"]["mode"], "Primary");
    assert!(
        !primary_spec.to_string().contains("primary_conninfo"),
        "a primary streams from no one"
    );
    // A second one on the same branch.
    let ro2 = e
        .create(&project, "", EndpointType::ReadOnly, "ro2")
        .await
        .endpoint
        .record
        .id;
    e.start(&project, &ro2, "start-ro2").await;
    e.running(&project, &ro2).await;
    assert_eq!(e.state(&project, &ro).await, Some(EndpointState::Running));
}

/// With the read-write endpoint down, a promotion prewarms the replica's
/// file cache (from the read-write endpoint's), asks `compute_ctl` to
/// promote it with a primary spec at the WAL's flushed LSN, and swaps the
/// two endpoints' types; the operation succeeds and the compute keeps
/// running.
#[tokio::test]
async fn promote_swaps_types() {
    let e = fakes!();
    let (project, rw, ro, compute) = e.primary_and_replica().await;
    e.suspend(&project, &rw, "suspend-rw").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;

    let op = e.promote(&project, &ro, "promote").await.expect("promote");
    e.until(&project, "the promotion done", || async {
        e.op(&op).await == OperationState::Succeeded
    })
    .await;
    assert_eq!(e.kind(&project, &ro).await, EndpointType::ReadWrite);
    assert_eq!(e.kind(&project, &rw).await, EndpointType::ReadOnly);
    assert_eq!(
        e.ctl.lock().prewarms,
        vec![(compute.clone(), Some(rw.clone()))],
        "prewarmed from the read-write endpoint's cache"
    );
    let promoted = e.ctl.lock().promotes.clone();
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].0, compute);
    assert_eq!(promoted[0].1, "Primary");
    assert_eq!(
        e.endpoint(&project, &ro).await.and_then(|r| r.compute_id),
        Some(compute.clone()),
        "the same compute serves on"
    );
    assert_eq!(e.state(&project, &ro).await, Some(EndpointState::Running));
    assert!(!e.runtime.stops().contains(&compute));
    // The pinned compute_ctl panics applying the catalog in `/promote`
    // (R16.6): the promotion's spec leaves it alone, and the next pass
    // pushes the full primary spec through `/configure`.
    assert!(e.ctl.lock().promote_specs[0].skip_pg_catalog_updates);
    let pushed = e.ctl.configured().len();
    e.pass(&project).await;
    let configured = e.ctl.configured();
    assert!(configured.len() > pushed, "the full spec pushed");
    let (to, full) = configured.last().expect("a push");
    assert_eq!(to, &compute);
    assert!(!full.skip_pg_catalog_updates);
    // Its spec is now a primary's.
    assert_eq!(e.upcall_spec(&compute).await["spec"]["mode"], "Primary");
    // The old read-write endpoint starts as a replica now.
    e.start(&project, &rw, "start-old-rw").await;
    e.running(&project, &rw).await;
}

/// While the branch's read-write endpoint runs (or is asked to), a
/// promotion is refused; only a running read-only endpoint is promoted.
#[tokio::test]
async fn promote_refused_while_rw_running() {
    let e = fakes!();
    let (project, rw, ro, _) = e.primary_and_replica().await;
    assert_eq!(
        e.promote(&project, &ro, "p1").await,
        Err(Reason::FailedPrecondition)
    );
    assert_eq!(
        e.promote(&project, &rw, "p2").await,
        Err(Reason::FailedPrecondition),
        "a read-write endpoint is not promoted"
    );
    // Asked to run, though not running yet, counts as running.
    e.suspend(&project, &rw, "s").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;
    e.start(&project, &rw, "s2").await;
    assert_eq!(
        e.promote(&project, &ro, "p3").await,
        Err(Reason::FailedPrecondition)
    );
    assert!(e.ctl.lock().promotes.is_empty());
    assert_eq!(e.kind(&project, &ro).await, EndpointType::ReadOnly);

    // Once a promotion is asked, the read-write endpoint does not start
    // until it ends (`rw_start_refused_while_promoting`).
}

/// `compute_ctl` refuses to promote a replica whose file cache is not
/// prewarmed (R2.13): the reconciler asks for the prewarm first, waits for
/// it (bounded), and when the promotion is still refused the operation
/// fails `failed_precondition` and nothing is swapped.
#[tokio::test]
async fn promote_refuses_an_unprewarmed_replica() {
    let e = fakes!();
    let (project, rw, ro, compute) = e.primary_and_replica().await;
    e.suspend(&project, &rw, "s").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;
    e.ctl.lock().prewarm_sticks = true;
    let op = e.promote(&project, &ro, "promote").await.expect("promote");
    e.pass(&project).await;
    assert_eq!(
        e.op(&op).await,
        OperationState::Running,
        "waiting for the prewarm"
    );
    assert!(e.ctl.lock().promotes.is_empty());
    assert_eq!(e.ctl.lock().prewarm_of(&compute), Prewarm::Prewarming);
    // The prewarm never finishes: past the wait, compute_ctl is asked and
    // refuses.
    e.h.clock.advance_ms(
        u64::try_from(loams_pg_control::reconcile::endpoint::PREWARM_WAIT.as_millis()).unwrap(),
    );
    e.until(&project, "the promotion failed", || async {
        e.op(&op).await == OperationState::Failed
    })
    .await;
    let failed = e.h.service.get_operation(&op).await.expect("the op");
    assert_eq!(
        failed.error.map(|err| err.reason).as_deref(),
        Some("failed_precondition")
    );
    assert_eq!(e.ctl.lock().promotes.len(), 1, "asked once, refused");
    assert_eq!(e.kind(&project, &ro).await, EndpointType::ReadOnly);
    assert_eq!(e.kind(&project, &rw).await, EndpointType::ReadWrite);
    // A later promotion may try again.
    e.ctl.lock().prewarm_sticks = false;
    let again = e
        .promote(&project, &ro, "promote-2")
        .await
        .expect("promote");
    e.until(&project, "the promotion done", || async {
        e.op(&again).await == OperationState::Succeeded
    })
    .await;
}

/// A suspended replica ends its pending promotion (`aborted`), and nothing
/// is left asking for it (CodeRabbit on #635).
#[tokio::test]
async fn suspend_ends_a_pending_promotion() {
    let e = fakes!();
    let (project, rw, ro, _) = e.primary_and_replica().await;
    e.suspend(&project, &rw, "s").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;
    e.ctl.lock().prewarm_sticks = true;
    let op = e.promote(&project, &ro, "promote").await.expect("promote");
    e.pass(&project).await;
    e.suspend(&project, &ro, "suspend-ro").await;
    e.until(&project, "the promotion ended", || async {
        e.op(&op).await == OperationState::Failed
    })
    .await;
    let ended = e.h.service.get_operation(&op).await.expect("the op");
    assert_eq!(
        ended.error.map(|err| err.reason).as_deref(),
        Some("aborted")
    );
    let rec = e.endpoint(&project, &ro).await.expect("the endpoint");
    assert!(!rec.promote);
    assert_eq!(rec.kind, EndpointType::ReadOnly);
    assert!(e.ctl.lock().promotes.is_empty());
}

/// While a read-only endpoint is being promoted, the branch's read-write
/// endpoint is not started or restarted (two primaries could result).
#[tokio::test]
async fn rw_start_refused_while_promoting() {
    let e = fakes!();
    let (project, rw, ro, _) = e.primary_and_replica().await;
    e.suspend(&project, &rw, "s").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;
    e.ctl.lock().prewarm_sticks = true;
    e.promote(&project, &ro, "promote").await.expect("promote");
    let start =
        e.h.service
            .start_endpoint(&user(), Env::action(&project, &rw, "start-rw-again"))
            .await
            .expect_err("refused");
    assert_eq!(start.reason, Reason::FailedPrecondition);
    let restart =
        e.h.service
            .restart_endpoint(&user(), Env::action(&project, &rw, "restart-rw-again"))
            .await
            .expect_err("refused");
    assert_eq!(restart.reason, Reason::FailedPrecondition);
}

/// When `/promote`'s outcome is unknown (its answer lost, a failure after
/// `pg_promote()`), the compute may be a primary already: the operation
/// fails, the endpoint stays read-only, and a fresh replica replaces the
/// compute (CodeRabbit on #635).
#[tokio::test]
async fn an_unknown_promotion_outcome_replaces_the_compute() {
    let e = fakes!();
    let (project, rw, ro, compute) = e.primary_and_replica().await;
    e.suspend(&project, &rw, "s").await;
    e.until(&project, "the read-write endpoint down", || async {
        e.state(&project, &rw).await == Some(EndpointState::Suspended)
    })
    .await;
    e.ctl.lock().promote_lost = true;
    let op = e.promote(&project, &ro, "promote").await.expect("promote");
    e.until(&project, "the promotion failed", || async {
        e.op(&op).await == OperationState::Failed
    })
    .await;
    e.until(&project, "the compute replaced", || async {
        e.endpoint(&project, &ro)
            .await
            .and_then(|r| r.compute_id)
            .is_some_and(|c| c != compute)
            && e.state(&project, &ro).await == Some(EndpointState::Running)
    })
    .await;
    assert!(
        e.runtime.stops().contains(&compute),
        "the maybe-primary stopped"
    );
    assert_eq!(e.kind(&project, &ro).await, EndpointType::ReadOnly);
    assert_eq!(e.kind(&project, &rw).await, EndpointType::ReadWrite);
}
