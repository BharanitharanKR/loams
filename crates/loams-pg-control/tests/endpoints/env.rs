//! What the endpoint cases (`endpoints/cases.rs`, Task 11) and the idle
//! cases (`idle.rs`, Task 12) share: a fake `ComputeRuntime`, a fake
//! `compute_ctl` (with its `last_active`), a fake PgDog, an observer that
//! records every lifecycle event, and a reconciler on them, driven pass by
//! pass on the harness's clock.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use loams_pg_control::idle::IdleMonitor;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{
    ComputeKey, ComputeRec, Cu, EndpointKey, EndpointRec, EndpointState, EndpointType,
    OperationState, ProjectState,
};
use loams_pg_control::observer::{ComputeLifecycleEvent, ComputeLifecycleObserver, Lifecycle};
use loams_pg_control::pgdog::{InstanceId, PgDogAdmin, PgDogError, PoolRow};
use loams_pg_control::reconcile::endpoint::{
    Activity, ComputeCtl, Computes, CtlError, CtlReport, CtlStatus,
};
use loams_pg_control::reconcile::{Pass, Reconciler, ReconcilerConfig};
use loams_pg_control::runtime::{
    BackendAddr, ComputeHandle, ComputeLaunch, ComputeRuntime, ResizeOutcome, RuntimeError,
    RuntimeStatus,
};
use loams_pg_control::service::Clock;
use loams_pg_control::service::endpoints::{CreateEndpoint, EndpointAction, EndpointView};
use loams_pg_control::service::projects::CreateProject;
use loams_pg_control::upcall::{ComputeKeys, SpecSources};
use loams_pg_control::{KvControlStore, PgControlStore};
use loams_postgres::spec::ComputeSpec;

use crate::common::{FakeNeon, Harness, user};

pub const NS: &str = "acme";
pub const IMAGE: &str = "ghcr.io/neondatabase/compute-node-v17@sha256:13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
/// The org the reconciler's lifecycle events name.
pub const ORG: &str = "org-acme";

// ---- Fakes ----

#[derive(Debug, Default)]
pub struct RuntimeInner {
    pub ports: BTreeMap<String, u16>,
    /// Compute id → (endpoint id, status).
    pub computes: BTreeMap<String, (String, RuntimeStatus)>,
    pub starts: Vec<String>,
    pub stops: Vec<String>,
    pub removed_addresses: Vec<String>,
}

/// A `ComputeRuntime` in memory: ports from 55500, computes that run until
/// stopped or crashed.
#[derive(Debug, Clone, Default)]
pub struct FakeRuntime(pub Arc<Mutex<RuntimeInner>>);

impl FakeRuntime {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, RuntimeInner> {
        self.0.lock().expect("runtime lock")
    }
    pub fn starts(&self) -> Vec<String> {
        self.lock().starts.clone()
    }
    pub fn stops(&self) -> Vec<String> {
        self.lock().stops.clone()
    }
    pub fn running(&self) -> Vec<String> {
        self.lock()
            .computes
            .iter()
            .filter(|(_, (_, s))| *s == RuntimeStatus::Running)
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub fn has_address(&self, endpoint: &str) -> bool {
        self.lock().ports.contains_key(endpoint)
    }
    /// The compute's container exits by itself.
    pub fn crash(&self, compute: &str) {
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

/// What the fake `compute_ctl` answers as `last_active`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FakeActivity {
    /// No activity since the compute started.
    #[default]
    NotYet,
    /// The clock's now on every answer: what the fork's monitor reports
    /// while a backend is not idle (a query running), a logical subscriber
    /// streams, or a subscription's worker runs.
    Now,
    /// Frozen at this time.
    At(u64),
    /// Not a time.
    Unreadable,
}

#[derive(Debug, Default)]
pub struct CtlInner {
    /// What a compute not named in `status` answers.
    pub default: Option<CtlStatus>,
    pub status: BTreeMap<String, CtlStatus>,
    pub activity: FakeActivity,
    /// The harness's clock, for [`FakeActivity::Now`].
    pub clock: Option<Arc<AtomicU64>>,
    pub configured: Vec<(String, ComputeSpec)>,
    pub terminated: Vec<String>,
}

/// A fake `compute_ctl` of every compute.
#[derive(Debug, Clone, Default)]
pub struct FakeCtl(pub Arc<Mutex<CtlInner>>);

impl FakeCtl {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, CtlInner> {
        self.0.lock().expect("ctl lock")
    }
    pub fn answer_by_default(&self, s: CtlStatus) {
        self.lock().default = Some(s);
    }
    pub fn set(&self, compute: &str, s: CtlStatus) {
        self.lock().status.insert(compute.into(), s);
    }
    /// What every compute answers as `last_active` from now on.
    pub fn activity(&self, a: FakeActivity) {
        self.lock().activity = a;
    }
    pub fn configured(&self) -> Vec<(String, ComputeSpec)> {
        self.lock().configured.clone()
    }
    pub fn terminated(&self) -> Vec<String> {
        self.lock().terminated.clone()
    }
}

#[async_trait]
impl ComputeCtl for FakeCtl {
    async fn status(&self, c: &ComputeRec) -> Result<CtlReport, CtlError> {
        assert!(c.ctl_url.is_some(), "status of a compute with an address");
        let inner = self.lock();
        let status = inner
            .status
            .get(&c.id)
            .cloned()
            .or_else(|| inner.default.clone())
            .ok_or_else(|| CtlError("connection refused".into()))?;
        let last_active = match inner.activity {
            FakeActivity::NotYet => Activity::NotYet,
            FakeActivity::Now => Activity::At(
                inner
                    .clock
                    .as_ref()
                    .expect("the harness's clock")
                    .load(Ordering::SeqCst),
            ),
            FakeActivity::At(at) => Activity::At(at),
            FakeActivity::Unreadable => Activity::Unreadable,
        };
        Ok(CtlReport {
            status,
            last_active,
        })
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

#[derive(Debug, Default)]
pub struct PgDogInner {
    /// `SHOW POOLS` of the namespace's one replica, `pgdog-0`.
    pub pools: Vec<PoolRow>,
    /// The replica does not answer.
    pub down: bool,
    /// Calls to `show_pools`.
    pub reads: usize,
}

/// A fake PgDog: one replica for [`NS`], none elsewhere.
#[derive(Debug, Clone, Default)]
pub struct FakePgDog(pub Arc<Mutex<PgDogInner>>);

impl FakePgDog {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, PgDogInner> {
        self.0.lock().expect("pgdog lock")
    }
    /// One pool's clients: `database` (a routed name) with `active` and
    /// `waiting` clients.
    pub fn clients(&self, database: &str, active: u64, waiting: u64) {
        let mut inner = self.lock();
        inner.pools.retain(|p| p.database != database);
        inner.pools.push(PoolRow {
            database: database.into(),
            user: "app".into(),
            cl_active: active,
            cl_waiting: waiting,
            sv_active: active,
        });
    }
    pub fn down(&self, down: bool) {
        self.lock().down = down;
    }
    pub fn reads(&self) -> usize {
        self.lock().reads
    }
}

#[async_trait]
impl PgDogAdmin for FakePgDog {
    async fn instances(&self, namespace: &str) -> Result<Vec<InstanceId>, PgDogError> {
        Ok(if namespace == NS {
            vec![InstanceId("pgdog-0".into())]
        } else {
            Vec::new()
        })
    }
    async fn reload(&self, _: &InstanceId) -> Result<(), PgDogError> {
        Ok(())
    }
    async fn show_pools(&self, _: &InstanceId) -> Result<Vec<PoolRow>, PgDogError> {
        let mut inner = self.lock();
        inner.reads += 1;
        if inner.down {
            return Err(PgDogError("connection refused".into()));
        }
        Ok(inner.pools.clone())
    }
    async fn show_databases(&self, _: &InstanceId) -> Result<Vec<String>, PgDogError> {
        Ok(self
            .lock()
            .pools
            .iter()
            .map(|p| p.database.clone())
            .collect())
    }
}

/// An observer that keeps every event.
#[derive(Debug, Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Vec<ComputeLifecycleEvent>>>);

impl Recorder {
    pub fn events(&self) -> Vec<ComputeLifecycleEvent> {
        self.0.lock().expect("recorder lock").clone()
    }
}

impl ComputeLifecycleObserver for Recorder {
    fn on_event(&self, e: &ComputeLifecycleEvent) {
        self.0.lock().expect("recorder lock").push(e.clone());
    }
}

// ---- The harness ----

pub struct Env {
    pub h: Harness,
    pub runtime: FakeRuntime,
    pub ctl: FakeCtl,
    pub pgdog: FakePgDog,
    pub events: Recorder,
    /// The reconciler's idle monitor.
    pub idle: Arc<IdleMonitor>,
    pub keys: Arc<ComputeKeys>,
    pub r: Reconciler<KvControlStore, FakeNeon>,
}

pub fn sources() -> SpecSources {
    SpecSources {
        pageservers: BTreeMap::from([
            (1, "postgresql://no_user@ps1:6400".to_string()),
            (2, "postgresql://no_user@ps2:6400".to_string()),
        ]),
        default_pageserver: Some(1),
        wal_pools: BTreeMap::from([("default".to_string(), vec!["wal:5454".to_string()])]),
    }
}

pub fn env_on(h: Harness) -> Env {
    let runtime = FakeRuntime::default();
    let ctl = FakeCtl::default();
    ctl.answer_by_default(CtlStatus::Running);
    ctl.lock().clock = Some(h.clock.0.clone());
    let pgdog = FakePgDog::default();
    let events = Recorder::default();
    let keys = Arc::new(ComputeKeys::generate().expect("a key"));
    let at = h.clock.0.clone();
    let config = ReconcilerConfig {
        retry_initial: Duration::from_millis(10),
        clock: Clock::from_fn(move || at.load(Ordering::SeqCst)),
        ..ReconcilerConfig::new("pg-control-a")
    };
    let mut computes = Computes::new(
        Arc::new(runtime.clone()),
        Arc::new(ctl.clone()),
        keys.clone(),
        BTreeMap::from([(17, IMAGE.to_string())]),
        sources(),
    );
    let idle = Arc::new(IdleMonitor::new(Arc::new(pgdog.clone())));
    computes.idle = idle.clone();
    computes.lifecycle = Lifecycle {
        observer: Arc::new(events.clone()),
        org: ORG.into(),
    };
    let r = Reconciler::new(h.store.clone(), h.neon.clone(), h.secrets.clone(), config)
        .with_computes(computes);
    Env {
        h,
        runtime,
        ctl,
        pgdog,
        events,
        idle,
        keys,
        r,
    }
}

/// `let e = fakes!();`: the harness with a reconciler on fakes, or return
/// (skipped).
macro_rules! fakes {
    () => {{
        let h = harness!();
        crate::env::env_on(h)
    }};
}

impl Env {
    /// One pass over `project`.
    pub async fn pass(&self, project: &str) -> Pass {
        self.r
            .reconcile_project(NS, project)
            .await
            .expect("a pass succeeds")
    }

    /// Passes until `f` holds (at most 20).
    pub async fn until<F, Fut>(&self, project: &str, what: &str, f: F)
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

    pub async fn ready_project(&self) -> (String, String) {
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

    pub async fn create(
        &self,
        project: &str,
        branch: &str,
        kind: EndpointType,
        key: &str,
    ) -> EndpointView {
        self.create_with(CreateEndpoint {
            namespace: NS.into(),
            project_id: project.into(),
            branch_id: branch.into(),
            kind,
            idempotency_key: key.into(),
            ..CreateEndpoint::default()
        })
        .await
    }

    pub async fn create_with(&self, req: CreateEndpoint) -> EndpointView {
        self.h
            .service
            .create_endpoint(&user(), req)
            .await
            .expect("create endpoint")
    }

    pub async fn endpoint(&self, project: &str, id: &str) -> Option<EndpointRec> {
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

    pub async fn compute(&self, id: &str) -> Option<ComputeRec> {
        self.h
            .store
            .get::<ComputeRec>(&ComputeKey { id: id.into() })
            .await
            .expect("get")
            .map(|v| v.record)
    }

    pub async fn state(&self, project: &str, id: &str) -> Option<EndpointState> {
        self.endpoint(project, id).await.map(|e| e.state)
    }

    pub fn action(project: &str, endpoint: &str, key: &str) -> EndpointAction {
        EndpointAction {
            namespace: NS.into(),
            project_id: project.into(),
            endpoint_id: endpoint.into(),
            idempotency_key: key.into(),
        }
    }

    pub async fn start(&self, project: &str, endpoint: &str, key: &str) -> String {
        self.h
            .service
            .start_endpoint(&user(), Self::action(project, endpoint, key))
            .await
            .expect("start")
            .id
    }

    pub async fn suspend(&self, project: &str, endpoint: &str, key: &str) -> String {
        self.h
            .service
            .suspend_endpoint(&user(), Self::action(project, endpoint, key))
            .await
            .expect("suspend")
            .id
    }

    pub async fn op(&self, id: &str) -> OperationState {
        self.h
            .service
            .get_operation(id)
            .await
            .expect("the op")
            .state
    }

    pub async fn running(&self, project: &str, endpoint: &str) {
        self.until(project, "the endpoint running", || async {
            self.state(project, endpoint).await == Some(EndpointState::Running)
        })
        .await;
    }
}
