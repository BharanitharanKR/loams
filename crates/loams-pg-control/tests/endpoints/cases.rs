//! The endpoint cases (PG2 Task 11): the endpoint RPCs of the API service,
//! and the endpoint reconciler driving a fake `ComputeRuntime` and a fake
//! `compute_ctl`; run by `endpoints` (local store) and `endpoints_tikv`.
//!
//! The reconciler is driven pass by pass (`reconcile_project`), so each
//! case sees every state the endpoint goes through.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{
    BranchRec, ComputeKey, ComputeRec, ComputeStatus, Cu, DesiredState, EndpointKey, EndpointRec,
    EndpointState, EndpointType, OperationState, ProjectState, ShardAttachment,
};
use loams_pg_control::reconcile::endpoint::{ComputeCtl, Computes, CtlError, CtlStatus};
use loams_pg_control::reconcile::{Pass, Reconciler, ReconcilerConfig};
use loams_pg_control::runtime::{
    BackendAddr, ComputeHandle, ComputeLaunch, ComputeRuntime, ResizeOutcome, RuntimeError,
    RuntimeStatus,
};
use loams_pg_control::service::branches::{BranchPoint, CreateBranch, DeleteBranch};
use loams_pg_control::service::endpoints::{
    CreateEndpoint, DeleteEndpoint, EndpointAction, EndpointView, UpdateEndpoint,
};
use loams_pg_control::service::projects::{CreateProject, DeleteProject};
use loams_pg_control::service::{Clock, Reason};
use loams_pg_control::upcall::{COMPUTE_TOKEN_TTL, ComputeKeys, SpecSources};
use loams_pg_control::{KvControlStore, PgControlStore};
use loams_postgres::spec::ComputeSpec;

use crate::common::{FakeNeon, Harness, user};

const NS: &str = "acme";
const IMAGE: &str = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";

// ---- Fakes ----

#[derive(Debug, Default)]
struct RuntimeInner {
    ports: BTreeMap<String, u16>,
    /// Compute id → (endpoint id, status).
    computes: BTreeMap<String, (String, RuntimeStatus)>,
    starts: Vec<String>,
    stops: Vec<String>,
    removed_addresses: Vec<String>,
}

/// A `ComputeRuntime` in memory: ports from 55500, computes that run until
/// stopped or crashed.
#[derive(Debug, Clone, Default)]
struct FakeRuntime(Arc<Mutex<RuntimeInner>>);

impl FakeRuntime {
    fn lock(&self) -> std::sync::MutexGuard<'_, RuntimeInner> {
        self.0.lock().expect("runtime lock")
    }
    fn starts(&self) -> Vec<String> {
        self.lock().starts.clone()
    }
    fn stops(&self) -> Vec<String> {
        self.lock().stops.clone()
    }
    fn running(&self) -> Vec<String> {
        self.lock()
            .computes
            .iter()
            .filter(|(_, (_, s))| *s == RuntimeStatus::Running)
            .map(|(id, _)| id.clone())
            .collect()
    }
    fn has_address(&self, endpoint: &str) -> bool {
        self.lock().ports.contains_key(endpoint)
    }
    /// The compute's container exits by itself.
    fn crash(&self, compute: &str) {
        if let Some((_, s)) = self.lock().computes.get_mut(compute) {
            *s = RuntimeStatus::Exited { code: Some(1) };
        }
    }
}

#[async_trait]
impl ComputeRuntime for FakeRuntime {
    async fn ensure_address(&self, ep: &EndpointRec) -> Result<BackendAddr, RuntimeError> {
        let mut inner = self.lock();
        let next = 55500 + u16::try_from(inner.ports.len()).expect("few ports");
        let port = *inner.ports.entry(ep.id.clone()).or_insert(next);
        Ok(BackendAddr::new("127.0.0.1", port))
    }
    async fn remove_address(&self, ep: &EndpointRec) -> Result<(), RuntimeError> {
        let mut inner = self.lock();
        if inner
            .computes
            .values()
            .any(|(e, s)| *e == ep.id && *s == RuntimeStatus::Running)
        {
            return Err(RuntimeError::InvalidArgument("a compute still runs".into()));
        }
        inner.ports.remove(&ep.id);
        inner.removed_addresses.push(ep.id.clone());
        Ok(())
    }
    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError> {
        assert_eq!(c.image, IMAGE, "the pinned image of the project's major");
        assert!(c.upcall_token.expose().len() > 20, "an upcall token");
        let binding = c.binding.as_ref().expect("a bound compute");
        let id = c.compute_id.to_string();
        let mut inner = self.lock();
        if !inner.computes.contains_key(&id) {
            inner.starts.push(id.clone());
            inner.computes.insert(
                id.clone(),
                (binding.endpoint_id.clone(), RuntimeStatus::Running),
            );
        }
        Ok(ComputeHandle {
            compute_id: c.compute_id,
            ctl_url: format!("http://ctl.local/{id}"),
            address: c.address.clone(),
        })
    }
    async fn bind_address(&self, ep: &EndpointRec, c: &ComputeHandle) -> Result<(), RuntimeError> {
        let port = self.lock().ports.get(&ep.id).copied();
        match (port, &c.address) {
            (Some(p), Some(a)) if a.port == p => Ok(()),
            _ => Err(RuntimeError::InvalidArgument("not the endpoint's".into())),
        }
    }
    async fn stop(&self, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        let id = compute_id.to_string();
        let mut inner = self.lock();
        if inner.computes.remove(&id).is_some() {
            inner.stops.push(id);
        }
        Ok(())
    }
    async fn status(&self, compute_id: &ComputeId) -> Result<RuntimeStatus, RuntimeError> {
        Ok(self
            .lock()
            .computes
            .get(&compute_id.to_string())
            .map_or(RuntimeStatus::Missing, |(_, s)| *s))
    }
    async fn resize(&self, _: &ComputeId, _: Cu) -> Result<ResizeOutcome, RuntimeError> {
        Ok(ResizeOutcome::Unsupported)
    }
}

#[derive(Debug, Default)]
struct CtlInner {
    /// What a compute not named in `status` answers.
    default: Option<CtlStatus>,
    status: BTreeMap<String, CtlStatus>,
    configured: Vec<(String, ComputeSpec)>,
    terminated: Vec<String>,
}

/// A fake `compute_ctl` of every compute.
#[derive(Debug, Clone, Default)]
struct FakeCtl(Arc<Mutex<CtlInner>>);

impl FakeCtl {
    fn lock(&self) -> std::sync::MutexGuard<'_, CtlInner> {
        self.0.lock().expect("ctl lock")
    }
    fn answer_by_default(&self, s: CtlStatus) {
        self.lock().default = Some(s);
    }
    fn set(&self, compute: &str, s: CtlStatus) {
        self.lock().status.insert(compute.into(), s);
    }
    fn configured(&self) -> Vec<(String, ComputeSpec)> {
        self.lock().configured.clone()
    }
    fn terminated(&self) -> Vec<String> {
        self.lock().terminated.clone()
    }
}

#[async_trait]
impl ComputeCtl for FakeCtl {
    async fn status(&self, c: &ComputeRec) -> Result<CtlStatus, CtlError> {
        assert!(c.ctl_url.is_some(), "status of a compute with an address");
        let inner = self.lock();
        inner
            .status
            .get(&c.id)
            .cloned()
            .or_else(|| inner.default.clone())
            .ok_or_else(|| CtlError("connection refused".into()))
    }
    async fn configure(&self, c: &ComputeRec, spec: &ComputeSpec) -> Result<(), CtlError> {
        self.lock().configured.push((c.id.clone(), spec.clone()));
        Ok(())
    }
    async fn terminate(&self, c: &ComputeRec) -> Result<(), CtlError> {
        self.lock().terminated.push(c.id.clone());
        Ok(())
    }
}

// ---- The harness ----

struct Env {
    h: Harness,
    runtime: FakeRuntime,
    ctl: FakeCtl,
    keys: Arc<ComputeKeys>,
    r: Reconciler<KvControlStore, FakeNeon>,
}

fn sources() -> SpecSources {
    SpecSources {
        pageservers: BTreeMap::from([
            (1, "postgresql://no_user@ps1:6400".to_string()),
            (2, "postgresql://no_user@ps2:6400".to_string()),
        ]),
        default_pageserver: Some(1),
        wal_pools: BTreeMap::from([("default".to_string(), vec!["wal:5454".to_string()])]),
    }
}

fn env_on(h: Harness) -> Env {
    let runtime = FakeRuntime::default();
    let ctl = FakeCtl::default();
    ctl.answer_by_default(CtlStatus::Running);
    let keys = Arc::new(ComputeKeys::generate().expect("a key"));
    let at = h.clock.0.clone();
    let config = ReconcilerConfig {
        retry_initial: Duration::from_millis(10),
        clock: Clock::from_fn(move || at.load(std::sync::atomic::Ordering::SeqCst)),
        ..ReconcilerConfig::new("pg-control-a")
    };
    let computes = Computes::new(
        Arc::new(runtime.clone()),
        Arc::new(ctl.clone()),
        keys.clone(),
        BTreeMap::from([(17, IMAGE.to_string())]),
        sources(),
    );
    let r = Reconciler::new(h.store.clone(), h.neon.clone(), h.secrets.clone(), config)
        .with_computes(computes);
    Env {
        h,
        runtime,
        ctl,
        keys,
        r,
    }
}

/// `let e = fakes!();`: the harness with a reconciler on fakes, or return
/// (skipped).
macro_rules! fakes {
    () => {{
        let h = harness!();
        env_on(h)
    }};
}

impl Env {
    /// One pass over `project`.
    async fn pass(&self, project: &str) -> Pass {
        self.r
            .reconcile_project(NS, project)
            .await
            .expect("a pass succeeds")
    }

    /// Passes until `f` holds (at most 20).
    async fn until<F, Fut>(&self, project: &str, what: &str, f: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..20 {
            if f().await {
                return;
            }
            self.pass(project).await;
        }
        assert!(f().await, "after 20 passes: {what}");
    }

    async fn ready_project(&self) -> (String, String) {
        let created = self
            .h
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
            .expect("create project");
        let id = created.project.record.id.clone();
        let main = created
            .project
            .record
            .default_branch_id
            .clone()
            .expect("main");
        self.until(&id, "the project ready", || async {
            self.h
                .service
                .get_project(NS, &id)
                .await
                .is_ok_and(|p| p.record.state == ProjectState::Ready)
        })
        .await;
        (id, main)
    }

    async fn create(
        &self,
        project: &str,
        branch: &str,
        kind: EndpointType,
        key: &str,
    ) -> EndpointView {
        self.h
            .service
            .create_endpoint(
                &user(),
                CreateEndpoint {
                    namespace: NS.into(),
                    project_id: project.into(),
                    branch_id: branch.into(),
                    kind,
                    idempotency_key: key.into(),
                    ..CreateEndpoint::default()
                },
            )
            .await
            .expect("create endpoint")
    }

    async fn endpoint(&self, project: &str, id: &str) -> Option<EndpointRec> {
        self.h
            .store
            .get::<EndpointRec>(&EndpointKey {
                project_id: project.into(),
                id: id.into(),
            })
            .await
            .expect("get")
            .map(|v| v.record)
    }

    async fn compute(&self, id: &str) -> Option<ComputeRec> {
        self.h
            .store
            .get::<ComputeRec>(&ComputeKey { id: id.into() })
            .await
            .expect("get")
            .map(|v| v.record)
    }

    async fn state(&self, project: &str, id: &str) -> Option<EndpointState> {
        self.endpoint(project, id).await.map(|e| e.state)
    }

    fn action(project: &str, endpoint: &str, key: &str) -> EndpointAction {
        EndpointAction {
            namespace: NS.into(),
            project_id: project.into(),
            endpoint_id: endpoint.into(),
            idempotency_key: key.into(),
        }
    }

    async fn start(&self, project: &str, endpoint: &str, key: &str) -> String {
        self.h
            .service
            .start_endpoint(&user(), Self::action(project, endpoint, key))
            .await
            .expect("start")
            .id
    }

    async fn suspend(&self, project: &str, endpoint: &str, key: &str) -> String {
        self.h
            .service
            .suspend_endpoint(&user(), Self::action(project, endpoint, key))
            .await
            .expect("suspend")
            .id
    }

    async fn op(&self, id: &str) -> OperationState {
        self.h
            .service
            .get_operation(id)
            .await
            .expect("the op")
            .state
    }

    async fn running(&self, project: &str, endpoint: &str) {
        self.until(project, "the endpoint running", || async {
            self.state(project, endpoint).await == Some(EndpointState::Running)
        })
        .await;
    }
}

// ---- The named cases ----

/// `StartEndpoint`: a `C/` record (bound, the signing key's id, the
/// compute's `compute_ctl` address), the runtime starts the pinned image
/// at the endpoint's address, and once `compute_ctl` answers `running` the
/// endpoint is `running` and the operation succeeded.
#[tokio::test]
async fn start_reaches_running() {
    let e = fakes!();
    let (project, main) = e.ready_project().await;
    let ep = e.create(&project, "", EndpointType::ReadWrite, "ep1").await;
    let id = ep.endpoint.record.id.clone();
    assert_eq!(ep.endpoint.record.branch_id, main, "the default branch");
    assert_eq!(ep.endpoint.record.state, EndpointState::Idle);
    assert_eq!(ep.endpoint.record.min_cu, Cu(1));
    assert_eq!(ep.endpoint.record.suspend_timeout_s, 300);

    // Starting: the compute is created and started, compute_ctl not yet
    // running.
    e.ctl.answer_by_default(CtlStatus::Starting);
    let op = e.start(&project, &id, "start1").await;
    assert_eq!(e.op(&op).await, OperationState::Pending);
    e.until(&project, "a compute started", || async {
        !e.runtime.starts().is_empty()
    })
    .await;
    let rec = e.endpoint(&project, &id).await.expect("the endpoint");
    assert_eq!(rec.state, EndpointState::Starting);
    let compute_id = rec.compute_id.clone().expect("a compute");
    assert_eq!(e.runtime.starts(), vec![compute_id.clone()]);
    let compute = e.compute(&compute_id).await.expect("the C/ record");
    assert_eq!(compute.status, ComputeStatus::Pending);
    assert_eq!(compute.jwt_key_id.as_deref(), Some(e.keys.kid()));
    assert_eq!(
        compute.ctl_url.as_deref(),
        Some(format!("http://ctl.local/{compute_id}").as_str())
    );
    let binding = compute.binding.clone().expect("bound");
    assert_eq!(
        (
            binding.namespace.as_str(),
            binding.project_id.as_str(),
            binding.endpoint_id.as_str()
        ),
        (NS, project.as_str(), id.as_str())
    );
    assert_eq!(rec.backend_addr.as_deref(), Some("127.0.0.1:55500"));

    e.ctl.set(&compute_id, CtlStatus::Running);
    e.running(&project, &id).await;
    let rec = e.endpoint(&project, &id).await.expect("the endpoint");
    assert_eq!(rec.compute_id.as_deref(), Some(compute_id.as_str()));
    assert!(rec.open_ops.is_empty(), "{:?}", rec.open_ops);
    assert_eq!(
        e.compute(&compute_id).await.map(|c| c.status),
        Some(ComputeStatus::Running)
    );
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
    assert_eq!(e.runtime.starts().len(), 1, "one compute, started once");
    // A running endpoint's passes start nothing more.
    for _ in 0..3 {
        e.pass(&project).await;
    }
    assert_eq!(e.runtime.starts().len(), 1);
    let view =
        e.h.service
            .get_endpoint(NS, &project, &id)
            .await
            .expect("get");
    assert_eq!(view.compute.map(|c| c.cu), Some(Cu(1)));
}

/// `SuspendEndpoint`: `suspending`, `/terminate`, the runtime stops the
/// compute, and the endpoint is `suspended` with no compute; the `C/`
/// record goes, the address stays.
#[tokio::test]
async fn suspend_reaches_suspended_and_stops_compute() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let compute = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();

    let op = e.suspend(&project, &id, "suspend1").await;
    e.until(&project, "the endpoint suspended", || async {
        e.state(&project, &id).await == Some(EndpointState::Suspended)
    })
    .await;
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
    assert_eq!(e.ctl.terminated(), vec![compute.clone()]);
    assert_eq!(e.runtime.stops(), vec![compute.clone()]);
    assert!(e.runtime.running().is_empty());
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert_eq!(rec.compute_id, None);
    assert_eq!(rec.desired, DesiredState::Suspended);
    assert_eq!(rec.backend_addr.as_deref(), Some("127.0.0.1:55500"));
    assert!(e.compute(&compute).await.is_none(), "the C/ record goes");
    assert!(e.runtime.has_address(&id), "the address stays");

    // A second suspend of a suspended endpoint succeeds with nothing to do.
    let again = e.suspend(&project, &id, "suspend2").await;
    e.until(&project, "the second suspend done", || async {
        e.op(&again).await == OperationState::Succeeded
    })
    .await;
    assert_eq!(e.runtime.stops().len(), 1);
}

/// At most one read-write endpoint per branch: the second is
/// `endpoint_exists_for_branch`, naming the first; read-only ones are not
/// limited, and another branch takes its own.
#[tokio::test]
async fn second_rw_endpoint_refused() {
    let e = fakes!();
    let (project, main) = e.ready_project().await;
    let first = e
        .create(&project, &main, EndpointType::ReadWrite, "ep1")
        .await;
    let err =
        e.h.service
            .create_endpoint(
                &user(),
                CreateEndpoint {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    branch_id: main.clone(),
                    kind: EndpointType::ReadWrite,
                    idempotency_key: "ep2".into(),
                    ..CreateEndpoint::default()
                },
            )
            .await
            .expect_err("refused");
    assert_eq!(err.reason, Reason::EndpointExistsForBranch);
    assert_eq!(
        err.metadata.get("endpoint").map(String::as_str),
        Some(first.endpoint.record.id.as_str())
    );
    e.create(&project, &main, EndpointType::ReadOnly, "ro1")
        .await;
    e.create(&project, &main, EndpointType::ReadOnly, "ro2")
        .await;
    // A replay of the first create answers the first endpoint.
    let replay = e
        .create(&project, &main, EndpointType::ReadWrite, "ep1")
        .await;
    assert_eq!(replay.endpoint.record.id, first.endpoint.record.id);

    let dev =
        e.h.service
            .create_branch(
                &user(),
                CreateBranch {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    name: "dev".into(),
                    point: BranchPoint::Head,
                    idempotency_key: "dev".into(),
                    ..CreateBranch::default()
                },
            )
            .await
            .expect("a branch");
    e.create(
        &project,
        &dev.branch.branch.record.id,
        EndpointType::ReadWrite,
        "ep-dev",
    )
    .await;
    let (all, _) =
        e.h.service
            .list_endpoints(NS, &project, "", 0, "")
            .await
            .expect("list");
    assert_eq!(all.len(), 4);
    let (on_main, _) =
        e.h.service
            .list_endpoints(NS, &project, &main, 0, "")
            .await
            .expect("list");
    assert_eq!(on_main.len(), 3);
}

/// Three crashes within 10 minutes: each crash replaces the compute, the
/// third puts the endpoint in `failed` with the reason, and the start's
/// operation fails `compute_start_failed`.
#[tokio::test]
async fn crash_loop_goes_failed_after_three() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    // compute_ctl fails every start.
    e.ctl
        .answer_by_default(CtlStatus::Failed("could not start postgres".into()));
    let op = e.start(&project, &id, "start1").await;
    e.until(&project, "the endpoint failed", || async {
        e.state(&project, &id).await == Some(EndpointState::Failed)
    })
    .await;
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert_eq!(e.runtime.starts().len(), 3, "three computes, each replaced");
    assert_eq!(e.runtime.stops().len(), 3, "each crashed compute stopped");
    assert_eq!(rec.compute_id, None);
    assert_eq!(rec.crashes.len(), 3);
    let failure = rec.failure.expect("a reason");
    assert!(failure.contains("3 times"), "{failure}");
    let op = e.h.service.get_operation(&op).await.unwrap();
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(
        op.error.map(|e| e.reason).as_deref(),
        Some("compute_start_failed")
    );
    // A failed endpoint stays failed: no more computes.
    for _ in 0..3 {
        e.pass(&project).await;
    }
    assert_eq!(e.runtime.starts().len(), 3);
}

/// Crashes older than 10 minutes do not count; a crashed running compute
/// is replaced, not restarted.
#[tokio::test]
async fn crashes_outside_the_window_do_not_count() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.start(&project, &id, "start1").await;
    for round in 0..4 {
        e.running(&project, &id).await;
        let compute = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();
        e.runtime.crash(&compute);
        e.h.clock.advance_ms(6 * 60 * 1000);
        e.until(&project, "the crashed compute replaced", || async {
            e.endpoint(&project, &id)
                .await
                .unwrap()
                .compute_id
                .is_some_and(|c| c != compute)
        })
        .await;
        assert_eq!(e.runtime.starts().len(), round + 2);
    }
    e.running(&project, &id).await;
}

/// `StartEndpoint` on a `failed` endpoint clears the failure and its crash
/// history, and starts a new compute.
#[tokio::test]
async fn start_clears_failed() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.ctl.answer_by_default(CtlStatus::Failed("boom".into()));
    e.start(&project, &id, "start1").await;
    e.until(&project, "failed", || async {
        e.state(&project, &id).await == Some(EndpointState::Failed)
    })
    .await;

    e.ctl.answer_by_default(CtlStatus::Running);
    let op = e.start(&project, &id, "start2").await;
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert_eq!(rec.failure, None);
    assert!(rec.crashes.is_empty());
    assert_ne!(rec.state, EndpointState::Failed);
    e.running(&project, &id).await;
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
    assert_eq!(e.runtime.starts().len(), 4);
}

/// `RestartEndpoint` replaces the compute: the old one stops, a new one
/// (a new id) starts at the same address.
#[tokio::test]
async fn restart_replaces_compute_keeps_address() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let before = e.endpoint(&project, &id).await.unwrap();
    let old = before.compute_id.clone().unwrap();

    let op =
        e.h.service
            .restart_endpoint(&user(), Env::action(&project, &id, "restart1"))
            .await
            .expect("restart")
            .id;
    e.until(&project, "a new compute running", || async {
        let rec = e.endpoint(&project, &id).await.unwrap();
        rec.state == EndpointState::Running && rec.compute_id.as_deref() != Some(old.as_str())
    })
    .await;
    let after = e.endpoint(&project, &id).await.unwrap();
    assert_eq!(after.backend_addr, before.backend_addr);
    assert!(!after.replace_compute);
    assert_eq!(e.runtime.stops(), vec![old.clone()]);
    assert_eq!(e.runtime.running(), vec![after.compute_id.clone().unwrap()]);
    assert!(e.compute(&old).await.is_none());
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
}

/// `DeleteEndpoint`: the compute is stopped, the address given up, and the
/// endpoint's and compute's records removed; the operation succeeds.
#[tokio::test]
async fn delete_endpoint_stops_and_removes_address() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let ep = e.create(&project, "", EndpointType::ReadWrite, "ep1").await;
    let id = ep.endpoint.record.id.clone();
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let compute = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();

    let op =
        e.h.service
            .delete_endpoint(
                &user(),
                DeleteEndpoint {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    endpoint_id: id.clone(),
                    expected_version: None,
                    idempotency_key: "del1".into(),
                },
            )
            .await
            .expect("delete")
            .id;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Deleting));
    e.until(&project, "the endpoint removed", || async {
        e.endpoint(&project, &id).await.is_none()
    })
    .await;
    assert_eq!(e.runtime.stops(), vec![compute.clone()]);
    assert!(!e.runtime.has_address(&id));
    assert!(e.compute(&compute).await.is_none());
    assert_eq!(e.op(&op).await, OperationState::Succeeded);
    let err =
        e.h.service
            .get_endpoint(NS, &project, &id)
            .await
            .expect_err("gone");
    assert_eq!(err.reason, Reason::NotFound);
}

// ---- Further cases ----

/// A compute that does not reach `running` within 60 s fails the
/// endpoint (§46 §7.1), and is stopped.
#[tokio::test]
async fn start_timeout_goes_failed() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.ctl.answer_by_default(CtlStatus::Starting);
    let op = e.start(&project, &id, "start1").await;
    e.until(&project, "a compute started", || async {
        !e.runtime.starts().is_empty()
    })
    .await;
    e.pass(&project).await;
    assert_eq!(e.state(&project, &id).await, Some(EndpointState::Starting));
    e.h.clock.advance_ms(61_000);
    e.until(&project, "failed", || async {
        e.state(&project, &id).await == Some(EndpointState::Failed)
    })
    .await;
    assert!(e.runtime.running().is_empty());
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert!(rec.failure.unwrap().contains("60 s"));
    assert_eq!(e.op(&op).await, OperationState::Failed);
}

/// A compute that read its spec just before a `notify-attach` moved the
/// branch to another pageserver gets the new spec once it is running
/// (Task 8's race): the reconciler pushes a spec whose inputs changed.
#[tokio::test]
async fn a_changed_spec_is_pushed_again() {
    let e = fakes!();
    let (project, main) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.ctl.answer_by_default(CtlStatus::Starting);
    e.start(&project, &id, "start1").await;
    e.until(&project, "a compute started", || async {
        !e.runtime.starts().is_empty()
    })
    .await;
    let compute = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();
    // The storage controller attaches the tenant to node 2 while the
    // compute starts (it is not running, so notify-attach skips it).
    let key = loams_pg_control::model::BranchKey {
        project_id: project.clone(),
        id: main.clone(),
    };
    let b = e.h.store.get::<BranchRec>(&key).await.unwrap().unwrap();
    let mut moved = b.record.clone();
    moved.shards = vec![ShardAttachment {
        shard_number: 0,
        node_id: 2,
    }];
    e.h.store
        .api_writer()
        .put(&moved, Some(b.version))
        .await
        .unwrap();

    e.ctl.set(&compute, CtlStatus::Running);
    e.running(&project, &id).await;
    e.pass(&project).await;
    let pushed = e.ctl.configured();
    assert_eq!(pushed.len(), 1, "one push");
    assert_eq!(pushed[0].0, compute);
    let json = serde_json::to_string(&pushed[0].1).unwrap();
    assert!(json.contains("ps2"), "the new pageserver: {json}");
    // Nothing changed since: no second push.
    e.pass(&project).await;
    e.pass(&project).await;
    assert_eq!(e.ctl.configured().len(), 1);

    // An endpoint setting changed through the API reaches it too.
    e.h.service
        .update_endpoint(
            &user(),
            UpdateEndpoint {
                namespace: NS.into(),
                project_id: project.clone(),
                endpoint_id: id.clone(),
                pg_settings: Some(BTreeMap::from([("work_mem".into(), "16MB".into())])),
                idempotency_key: "u1".into(),
                ..UpdateEndpoint::default()
            },
        )
        .await
        .expect("update");
    e.pass(&project).await;
    let pushed = e.ctl.configured();
    assert_eq!(pushed.len(), 2);
    assert!(
        serde_json::to_string(&pushed[1].1)
            .unwrap()
            .contains("16MB")
    );
}

/// Endpoint settings are an allow-list: nothing overrides
/// `shared_preload_libraries` or the other settings the platform owns, and
/// a value cannot break out of its line in `postgresql.conf`.
#[tokio::test]
async fn pg_settings_outside_the_allow_list_refused() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    for (name, value) in [
        ("shared_preload_libraries", "neon,evil"),
        ("synchronous_standby_names", "''"),
        ("listen_addresses", "*"),
        ("port", "5432"),
        ("neon.pageserver_connstring", "x"),
        ("restart_after_crash", "on"),
        ("max_connections", "10000"),
        ("Work_Mem", "8MB"),
        ("work_mem", "8MB'\nshared_preload_libraries='evil"),
        ("work_mem", ""),
    ] {
        let err =
            e.h.service
                .create_endpoint(
                    &user(),
                    CreateEndpoint {
                        namespace: NS.into(),
                        project_id: project.clone(),
                        kind: EndpointType::ReadWrite,
                        pg_settings: BTreeMap::from([(name.to_string(), value.to_string())]),
                        idempotency_key: format!("bad-{name}-{}", value.len()),
                        ..CreateEndpoint::default()
                    },
                )
                .await
                .expect_err(name);
        assert_eq!(err.reason, Reason::InvalidArgument, "{name}");
        assert_eq!(
            err.metadata.get("field").map(String::as_str),
            Some("pg_settings")
        );
    }
    let ok =
        e.h.service
            .create_endpoint(
                &user(),
                CreateEndpoint {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    kind: EndpointType::ReadWrite,
                    pg_settings: BTreeMap::from([
                        ("work_mem".to_string(), "64MB".to_string()),
                        ("statement_timeout".to_string(), "30s".to_string()),
                        ("search_path".to_string(), "app, public".to_string()),
                    ]),
                    idempotency_key: "good".into(),
                    ..CreateEndpoint::default()
                },
            )
            .await
            .expect("allowed");
    assert_eq!(ok.endpoint.record.pg_settings.len(), 3);
}

/// CU and suspend timeouts are checked: 0.25 to 16 CU, min ≤ max, a
/// timeout of 0 (never) or at least 60 s.
#[tokio::test]
async fn sizes_and_timeouts_are_checked() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let bad = [
        (Some(Cu(0)), None, None, "min_cu"),
        (Some(Cu(8)), Some(Cu(4)), None, "max_cu"),
        (None, Some(Cu(65)), None, "max_cu"),
        (None, None, Some(30), "suspend_timeout"),
    ];
    for (i, (min, max, timeout, field)) in bad.into_iter().enumerate() {
        let err =
            e.h.service
                .create_endpoint(
                    &user(),
                    CreateEndpoint {
                        namespace: NS.into(),
                        project_id: project.clone(),
                        kind: EndpointType::ReadOnly,
                        min_cu: min,
                        max_cu: max,
                        suspend_timeout_s: timeout,
                        idempotency_key: format!("bad{i}"),
                        ..CreateEndpoint::default()
                    },
                )
                .await
                .expect_err(field);
        assert_eq!(err.metadata.get("field").map(String::as_str), Some(field));
    }
    let never =
        e.h.service
            .create_endpoint(
                &user(),
                CreateEndpoint {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    kind: EndpointType::ReadOnly,
                    min_cu: Some(Cu(2)),
                    max_cu: Some(Cu(8)),
                    suspend_timeout_s: Some(0),
                    idempotency_key: "never".into(),
                    ..CreateEndpoint::default()
                },
            )
            .await
            .expect("ok");
    assert_eq!(never.endpoint.record.suspend_timeout_s, 0);
    assert_eq!(never.endpoint.record.max_cu, Cu(8));
}

/// R7.1's cascade: a deleting branch's endpoints, then a deleting
/// project's, are stopped and removed, so the branch and the project go.
#[tokio::test]
async fn deleting_a_branch_or_project_removes_its_endpoints() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let dev =
        e.h.service
            .create_branch(
                &user(),
                CreateBranch {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    name: "dev".into(),
                    point: BranchPoint::Head,
                    idempotency_key: "dev".into(),
                    ..CreateBranch::default()
                },
            )
            .await
            .expect("a branch")
            .branch
            .branch
            .record
            .id;
    let on_dev = e
        .create(&project, &dev, EndpointType::ReadWrite, "ep-dev")
        .await
        .endpoint
        .record
        .id;
    let on_main = e
        .create(&project, "", EndpointType::ReadWrite, "ep-main")
        .await
        .endpoint
        .record
        .id;
    let start_dev = e.start(&project, &on_dev, "s1").await;
    e.start(&project, &on_main, "s2").await;
    e.running(&project, &on_dev).await;
    e.running(&project, &on_main).await;

    e.h.service
        .delete_branch(
            &user(),
            DeleteBranch {
                namespace: NS.into(),
                project_id: project.clone(),
                branch_id: dev.clone(),
                idempotency_key: "del-dev".into(),
                ..DeleteBranch::default()
            },
        )
        .await
        .expect("delete the branch");
    e.until(&project, "the branch gone", || async {
        e.h.service.get_branch(NS, &project, &dev).await.is_err()
    })
    .await;
    assert!(e.endpoint(&project, &on_dev).await.is_none());
    assert!(!e.runtime.has_address(&on_dev));
    assert_eq!(
        e.state(&project, &on_main).await,
        Some(EndpointState::Running)
    );
    assert_eq!(e.op(&start_dev).await, OperationState::Succeeded);
    // No endpoint on a deleting branch.
    e.h.service
        .delete_project(
            &user(),
            DeleteProject {
                namespace: NS.into(),
                project_id: project.clone(),
                expected_version: None,
                idempotency_key: "del-prj".into(),
            },
        )
        .await
        .expect("delete the project");
    e.until(&project, "the project gone", || async {
        e.h.service.get_project(NS, &project).await.is_err()
    })
    .await;
    assert!(e.runtime.running().is_empty());
    assert_eq!(e.runtime.stops().len(), 2);
}

/// A compute whose upcall token is about to expire is replaced
/// (R8.9: `COMPUTE_TOKEN_TTL`).
#[tokio::test]
async fn an_old_compute_is_replaced() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let old = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();
    e.h.clock
        .advance_ms(u64::try_from(COMPUTE_TOKEN_TTL.as_millis()).unwrap());
    e.until(&project, "a new compute", || async {
        let rec = e.endpoint(&project, &id).await.unwrap();
        rec.state == EndpointState::Running && rec.compute_id.as_deref() != Some(old.as_str())
    })
    .await;
    assert_eq!(e.runtime.stops(), vec![old]);
    let rec = e.endpoint(&project, &id).await.unwrap();
    assert!(rec.crashes.is_empty(), "a replacement is not a crash");
}

/// A start while the endpoint is suspending finishes the suspend, then
/// starts a new compute; a suspend asked after a start that has not
/// happened yet supersedes it (the start's operation is aborted).
#[tokio::test]
async fn starts_and_suspends_supersede_each_other() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let id = e
        .create(&project, "", EndpointType::ReadWrite, "ep1")
        .await
        .endpoint
        .record
        .id;
    let start = e.start(&project, &id, "start1").await;
    let suspend = e.suspend(&project, &id, "suspend1").await;
    e.pass(&project).await;
    let start_op = e.h.service.get_operation(&start).await.unwrap();
    assert_eq!(start_op.state, OperationState::Failed);
    assert_eq!(start_op.error.unwrap().reason, "aborted");
    assert_eq!(e.op(&suspend).await, OperationState::Succeeded);
    assert!(e.runtime.starts().is_empty());

    e.start(&project, &id, "start2").await;
    e.running(&project, &id).await;
    let first = e.endpoint(&project, &id).await.unwrap().compute_id.unwrap();
    // Suspending, and a start before the reconciler finished it.
    let mut rec = e.endpoint(&project, &id).await.unwrap();
    let v =
        e.h.store
            .get::<EndpointRec>(&EndpointKey {
                project_id: project.clone(),
                id: id.clone(),
            })
            .await
            .unwrap()
            .unwrap()
            .version;
    rec.state = EndpointState::Suspending;
    e.h.store.api_writer().put(&rec, Some(v)).await.unwrap();
    let wake = e.start(&project, &id, "start3").await;
    e.until(&project, "running on a new compute", || async {
        let rec = e.endpoint(&project, &id).await.unwrap();
        rec.state == EndpointState::Running && rec.compute_id.as_deref() != Some(first.as_str())
    })
    .await;
    assert_eq!(e.op(&wake).await, OperationState::Succeeded);
    assert!(e.ctl.terminated().contains(&first));
    let running: BTreeSet<String> = e.runtime.running().into_iter().collect();
    assert_eq!(running.len(), 1);
}

/// No endpoint on a branch that is being deleted, and no start of a
/// deleting endpoint.
#[tokio::test]
async fn deleting_things_take_no_endpoint() {
    let e = fakes!();
    let (project, _) = e.ready_project().await;
    let dev =
        e.h.service
            .create_branch(
                &user(),
                CreateBranch {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    name: "dev".into(),
                    point: BranchPoint::Head,
                    idempotency_key: "dev".into(),
                    ..CreateBranch::default()
                },
            )
            .await
            .expect("a branch")
            .branch
            .branch
            .record
            .id;
    let ep = e
        .create(&project, &dev, EndpointType::ReadWrite, "ep-dev")
        .await
        .endpoint
        .record
        .id;
    e.h.service
        .delete_endpoint(
            &user(),
            DeleteEndpoint {
                namespace: NS.into(),
                project_id: project.clone(),
                endpoint_id: ep.clone(),
                expected_version: None,
                idempotency_key: "del".into(),
            },
        )
        .await
        .expect("delete");
    let err =
        e.h.service
            .start_endpoint(&user(), Env::action(&project, &ep, "s"))
            .await
            .expect_err("deleting");
    assert_eq!(err.reason, Reason::FailedPrecondition);
    e.h.service
        .delete_branch(
            &user(),
            DeleteBranch {
                namespace: NS.into(),
                project_id: project.clone(),
                branch_id: dev.clone(),
                idempotency_key: "del-dev".into(),
                ..DeleteBranch::default()
            },
        )
        .await
        .expect("delete the branch");
    let err =
        e.h.service
            .create_endpoint(
                &user(),
                CreateEndpoint {
                    namespace: NS.into(),
                    project_id: project.clone(),
                    branch_id: dev.clone(),
                    kind: EndpointType::ReadOnly,
                    idempotency_key: "late".into(),
                    ..CreateEndpoint::default()
                },
            )
            .await
            .expect_err("refused");
    assert_eq!(err.reason, Reason::FailedPrecondition);
}
