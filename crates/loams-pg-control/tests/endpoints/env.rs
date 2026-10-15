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
use loams_pg_control::autoscale::{ComputeMetrics, Policy, Scaling};
use loams_pg_control::idle::IdleMonitor;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{
    ComputeKey, ComputeRec, Cu, EndpointKey, EndpointRec, EndpointState, EndpointType,
    OperationState, ProjectState,
};
use loams_pg_control::observer::{ComputeLifecycleEvent, ComputeLifecycleObserver, Lifecycle};
use loams_pg_control::pgdog::{InstanceId, PgDogAdmin, PgDogError, PoolRow};
use loams_pg_control::pool::{PoolConfig, WarmPool};
use loams_pg_control::reconcile::endpoint::{
    Activity, ComputeCtl, Computes, CtlError, CtlReport, CtlStatus, Prewarm,
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
/// The next pinned image of the same major (the warm pool's image change).
pub const IMAGE_NEXT: &str = "ghcr.io/ostrium-labs/loams-postgres-compute-v17@sha256:00000000000000000000000000000000000000000000000000000000000000aa";
/// The org the reconciler's lifecycle events name.
pub const ORG: &str = "org-acme";

// ---- Fakes ----

#[derive(Debug, Default)]
pub struct RuntimeInner {
    pub ports: BTreeMap<String, u16>,
    /// Compute id → (endpoint id, status); an unbound warm-pool compute's
    /// endpoint is empty until it is bound.
    pub computes: BTreeMap<String, (String, RuntimeStatus)>,
    /// Compute id → (namespace, image) it was started with.
    pub launched: BTreeMap<String, (String, String)>,
    /// Computes whose stop fails (`Unavailable`).
    pub fail_stop: std::collections::BTreeSet<String>,
    /// What the next resizes answer, in order; `Applied` once empty.
    pub resize: std::collections::VecDeque<ResizeOutcome>,
    /// Every resize asked: (compute, size).
    pub resized: Vec<(String, Cu)>,
    /// Resizes and (through the fake `compute_ctl`) `/configure`s, in
    /// order.
    pub order: Vec<String>,
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
    async fn ensure_address(
        &self,
        _ns: &str,
        ep: &EndpointRec,
    ) -> Result<BackendAddr, RuntimeError> {
        let mut inner = self.lock();
        let next = 55500 + u16::try_from(inner.ports.len()).expect("few ports");
        let port = *inner.ports.entry(ep.id.clone()).or_insert(next);
        Ok(BackendAddr::new("127.0.0.1", port))
    }
    async fn remove_address(&self, _ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError> {
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
        assert!(
            c.image == IMAGE || c.image == IMAGE_NEXT,
            "the pinned image of the project's major"
        );
        assert!(c.upcall_token.expose().len() > 20, "an upcall token");
        let ns = c.namespace()?.to_owned();
        let endpoint = c
            .binding
            .as_ref()
            .map_or_else(String::new, |b| b.endpoint_id.clone());
        let id = c.compute_id.to_string();
        let mut inner = self.lock();
        if !inner.computes.contains_key(&id) {
            inner.starts.push(id.clone());
            inner
                .computes
                .insert(id.clone(), (endpoint, RuntimeStatus::Running));
            inner.launched.insert(id.clone(), (ns, c.image.clone()));
        }
        Ok(ComputeHandle {
            compute_id: c.compute_id,
            ctl_url: format!("http://ctl.local/{id}"),
            address: c.address.clone(),
        })
    }
    async fn bind_address(
        &self,
        _ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError> {
        let mut inner = self.lock();
        let port = inner.ports.get(&ep.id).copied();
        match (port, &c.address) {
            (Some(p), Some(a)) if a.port == p => {
                if let Some((endpoint, _)) = inner.computes.get_mut(&c.compute_id.to_string()) {
                    endpoint.clone_from(&ep.id);
                }
                Ok(())
            }
            _ => Err(RuntimeError::InvalidArgument("not the endpoint's".into())),
        }
    }
    async fn stop(&self, _ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        let id = compute_id.to_string();
        let mut inner = self.lock();
        if inner.fail_stop.contains(&id) {
            return Err(RuntimeError::Unavailable("the API server timed out".into()));
        }
        if inner.computes.remove(&id).is_some() {
            inner.stops.push(id);
        }
        Ok(())
    }
    async fn status(
        &self,
        _ns: &str,
        compute_id: &ComputeId,
    ) -> Result<RuntimeStatus, RuntimeError> {
        Ok(self
            .lock()
            .computes
            .get(&compute_id.to_string())
            .map_or(RuntimeStatus::Missing, |(_, s)| *s))
    }
    async fn resize(&self, _: &str, id: &ComputeId, cu: Cu) -> Result<ResizeOutcome, RuntimeError> {
        let mut inner = self.lock();
        inner.resized.push((id.to_string(), cu));
        inner.order.push(format!("resize {id} {}", cu.0));
        Ok(inner.resize.pop_front().unwrap_or(ResizeOutcome::Applied))
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
    /// The runtime whose `order` hears each `/configure` (Task 15).
    pub runtime: Option<FakeRuntime>,
    /// Each compute's prewarm (Task 16); not prewarmed by default.
    pub prewarm: BTreeMap<String, Prewarm>,
    /// A prewarm that is asked never ends.
    pub prewarm_sticks: bool,
    /// Every `/lfc/prewarm`: (compute, from endpoint).
    pub prewarms: Vec<(String, Option<String>)>,
    /// Every `/promote`: (compute, the spec's mode).
    pub promotes: Vec<(String, String)>,
    /// The specs `/promote` got.
    pub promote_specs: Vec<ComputeSpec>,
}

/// A fake `compute_ctl` of every compute.
#[derive(Debug, Clone, Default)]
pub struct FakeCtl(pub Arc<Mutex<CtlInner>>);

impl CtlInner {
    pub fn prewarm_of(&self, compute: &str) -> Prewarm {
        self.prewarm
            .get(compute)
            .cloned()
            .unwrap_or(Prewarm::NotPrewarmed)
    }
}

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
        // A warm-pool compute waits for its spec until it is configured.
        let waits = c.pool.is_some() && !inner.configured.iter().any(|(id, _)| *id == c.id);
        let status = inner
            .status
            .get(&c.id)
            .cloned()
            .or_else(|| waits.then_some(CtlStatus::Empty))
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
        let mut inner = self.lock();
        inner.configured.push((c.id.clone(), spec.clone()));
        if let Some(r) = &inner.runtime {
            r.lock().order.push("configure".into());
        }
        Ok(())
    }
    async fn terminate(&self, c: &ComputeRec) -> Result<(), CtlError> {
        self.lock().terminated.push(c.id.clone());
        Ok(())
    }
    async fn prewarm(&self, c: &ComputeRec, from: Option<&str>) -> Result<(), CtlError> {
        let mut inner = self.lock();
        inner.prewarms.push((c.id.clone(), from.map(str::to_owned)));
        let next = if inner.prewarm_sticks {
            Prewarm::Prewarming
        } else {
            Prewarm::Done("completed".into())
        };
        inner.prewarm.insert(c.id.clone(), next);
        Ok(())
    }
    async fn prewarm_state(&self, c: &ComputeRec) -> Result<Prewarm, CtlError> {
        let mut inner = self.lock();
        // A prewarm that no longer sticks ends.
        if !inner.prewarm_sticks && inner.prewarm_of(&c.id) == Prewarm::Prewarming {
            inner
                .prewarm
                .insert(c.id.clone(), Prewarm::Done("completed".into()));
        }
        Ok(inner.prewarm_of(&c.id))
    }
    async fn promote(
        &self,
        c: &ComputeRec,
        spec: &ComputeSpec,
        _wal_flush_lsn: loams_postgres::Lsn,
    ) -> Result<(), CtlError> {
        let mut inner = self.lock();
        let mode = serde_json::to_value(spec).expect("a spec")["mode"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        inner.promotes.push((c.id.clone(), mode));
        inner.promote_specs.push(spec.clone());
        // The pinned compute_ctl refuses an unprewarmed replica (R2.13).
        match inner.prewarm_of(&c.id) {
            Prewarm::Done(_) => Ok(()),
            other => Err(CtlError(format!("compute {other:?}"))),
        }
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
    /// The reconciler's warm pool (Task 14), when it has one.
    pub pool: Option<Arc<WarmPool>>,
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
        endpoint_storage: None,
    }
}

pub fn env_on(h: Harness) -> Env {
    env_with(h, None)
}

/// As [`env_on`], with an autoscaler on `metrics` (Task 15).
pub fn env_scaling(h: Harness, metrics: Arc<dyn ComputeMetrics>) -> Env {
    let mut env = env_with(h, None);
    env.ctl.lock().runtime = Some(env.runtime.clone());
    let mut computes = env.computes(IMAGE, None);
    computes.scaling = Some(Arc::new(Scaling::new(metrics, Policy::default())));
    env.r = Reconciler::new(
        env.h.store.clone(),
        env.h.neon.clone(),
        env.h.secrets.clone(),
        reconciler_config(&env.h),
    )
    .with_computes(computes);
    env
}

/// The reconciler's settings: instance `pg-control-a` on the harness's
/// clock.
fn reconciler_config(h: &Harness) -> ReconcilerConfig {
    let at = h.clock.0.clone();
    ReconcilerConfig {
        retry_initial: Duration::from_millis(10),
        clock: Clock::from_fn(move || at.load(Ordering::SeqCst)),
        ..ReconcilerConfig::new("pg-control-a")
    }
}

/// As [`env_on`], with a warm pool of `pool`'s settings (Task 14).
pub fn env_with(h: Harness, pool: Option<PoolConfig>) -> Env {
    let runtime = FakeRuntime::default();
    let ctl = FakeCtl::default();
    ctl.answer_by_default(CtlStatus::Running);
    ctl.lock().clock = Some(h.clock.0.clone());
    let pgdog = FakePgDog::default();
    let events = Recorder::default();
    let keys = Arc::new(ComputeKeys::generate().expect("a key"));
    let idle = Arc::new(IdleMonitor::new(Arc::new(pgdog.clone())));
    let pool = pool.map(|c| Arc::new(WarmPool::new(c)));
    let mut env = Env {
        r: Reconciler::new(
            h.store.clone(),
            h.neon.clone(),
            h.secrets.clone(),
            reconciler_config(&h),
        ),
        h,
        runtime,
        ctl,
        pgdog,
        events,
        idle,
        keys,
        pool: pool.clone(),
    };
    let computes = env.computes(IMAGE, pool);
    env.r = Reconciler::new(
        env.h.store.clone(),
        env.h.neon.clone(),
        env.h.secrets.clone(),
        reconciler_config(&env.h),
    )
    .with_computes(computes);
    env
}

/// `let e = fakes!();`: the harness with a reconciler on fakes, or return
/// (skipped).
#[allow(unused_macros)]
macro_rules! fakes {
    () => {{
        let h = harness!();
        crate::env::env_on(h)
    }};
}

impl Env {
    /// What a reconciler drives computes with: this environment's fakes,
    /// `image` for Postgres 17, and `pool`.
    pub fn computes(&self, image: &str, pool: Option<Arc<WarmPool>>) -> Computes {
        let mut computes = Computes::new(
            Arc::new(self.runtime.clone()),
            Arc::new(self.ctl.clone()),
            self.keys.clone(),
            BTreeMap::from([(17, image.to_string())]),
            sources(),
        );
        computes.idle = self.idle.clone();
        computes.lifecycle = Lifecycle {
            observer: Arc::new(self.events.clone()),
            org: ORG.into(),
        };
        computes.pool = pool;
        computes
    }

    /// The same instance restarted with `image` for Postgres 17 and a new
    /// warm pool of `pool`'s settings, on the same store and fakes.
    pub fn restarted(
        &self,
        image: &str,
        pool: PoolConfig,
    ) -> (Reconciler<KvControlStore, FakeNeon>, Arc<WarmPool>) {
        let pool = Arc::new(WarmPool::new(pool));
        let r = Reconciler::new(
            self.h.store.clone(),
            self.h.neon.clone(),
            self.h.secrets.clone(),
            reconciler_config(&self.h),
        )
        .with_computes(self.computes(image, Some(pool.clone())));
        (r, pool)
    }

    /// `compute`'s spec upcall as the compute would read it, through a real
    /// upcall listener on this store (`{status: empty}` or `{status:
    /// attached, spec, ...}`).
    pub async fn upcall_spec(&self, compute: &str) -> serde_json::Value {
        use loams_pg_control::upcall::{
            COMPUTE_TOKEN_TTL, ComputeCtlConfigurer, UpcallConfig, UpcallServer,
        };
        let at = self.h.clock.0.clone();
        let src = sources();
        let server = UpcallServer::bind(
            self.h.store.clone(),
            self.keys.clone(),
            Arc::new(ComputeCtlConfigurer::new(
                self.keys.clone(),
                Clock::from_fn(|| 0),
            )),
            UpcallConfig {
                pageservers: src.pageservers.clone(),
                default_pageserver: src.default_pageserver,
                wal_pools: src.wal_pools.clone(),
                clock: Clock::from_fn(move || at.load(Ordering::SeqCst)),
                ..UpcallConfig::defaults("127.0.0.1:0".parse().expect("an address"))
            },
        )
        .await
        .expect("the upcall listener");
        let base = format!("http://{}", server.local_addr());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(server.serve(async move {
            let _ = stopped.await;
        }));
        let id: ComputeId = compute.parse().expect("a compute id");
        let token = self
            .keys
            .issue_upcall_token(
                &id,
                self.h.clock.0.load(Ordering::SeqCst),
                COMPUTE_TOKEN_TTL,
            )
            .expect("a token");
        let resp = reqwest::Client::new()
            .get(format!("{base}/compute/api/v2/computes/{id}/spec"))
            .bearer_auth(token.expose())
            .send()
            .await
            .expect("the upcall");
        assert_eq!(resp.status().as_u16(), 200, "the spec upcall");
        let body = resp.json().await.expect("json");
        let _ = stop.send(());
        body
    }

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
