//! The warm pool (PG2 Task 14), on the local (embedded) store with
//! `endpoints/env.rs`'s fakes: the pool is ticked and the reconciler passed
//! by hand, on the harness's clock. A warm compute's fake `compute_ctl`
//! answers `empty` until it is configured.

#[macro_use]
mod common;

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use loams_pg_control::PgControlStore;
use loams_pg_control::ids::ComputeId;
use loams_pg_control::model::{AllComputes, ComputeRec, ComputeStatus, Cu, EndpointType, PoolKey};
use loams_pg_control::pool::{POOL_LEASE, PoolConfig, PoolTick};
use loams_pg_control::reconcile::endpoint::CtlStatus;
use loams_pg_control::service::Clock;
use loams_pg_control::service::endpoints::CreateEndpoint;
use loams_pg_control::store::Page;
use loams_pg_control::store::conformance::{Factory, local_factory};
use loams_pg_control::upcall::{
    COMPUTE_TOKEN_TTL, ComputeCtlConfigurer, UpcallConfig, UpcallServer,
};
use serde_json::Value;

use crate::env::{Env, IMAGE, IMAGE_NEXT, NS};

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

/// `let e = pooled!(size);`: the harness with a reconciler whose warm pool
/// keeps `size` computes per key, or return (skipped).
macro_rules! pooled {
    ($size:expr) => {{
        let h = harness!();
        crate::env::env_with(h, Some(PoolConfig::new($size)))
    }};
}

fn key(image: &str) -> PoolKey {
    PoolKey {
        namespace: NS.into(),
        pg_version: 17,
        image: image.into(),
    }
}

impl Env {
    async fn tick(&self) -> PoolTick {
        self.r
            .tick_pool()
            .await
            .expect("a tick succeeds")
            .expect("a pool")
    }

    /// Every compute record.
    async fn computes_now(&self) -> Vec<ComputeRec> {
        let (all, next) = self
            .h
            .store
            .list::<ComputeRec>(&AllComputes, Page::first(1000))
            .await
            .expect("list");
        assert!(next.is_none());
        all.into_iter().map(|c| c.record).collect()
    }

    /// The unbound computes.
    async fn unbound(&self) -> Vec<ComputeRec> {
        self.computes_now()
            .await
            .into_iter()
            .filter(|c| c.binding.is_none())
            .collect()
    }

    /// A ready project with an endpoint at the pool's size, passed over
    /// once, so its namespace's pool is wanted: (project, endpoint).
    async fn wanted(&self) -> (String, String) {
        let (project, _) = self.ready_project().await;
        let ep = self
            .create(&project, "", EndpointType::ReadWrite, "ep1")
            .await;
        self.pass(&project).await;
        (project, ep.endpoint.record.id)
    }

    /// Ticks until the pool holds `n` warm computes (at most 5 ticks).
    async fn warm(&self, n: usize) -> Vec<ComputeRec> {
        for _ in 0..5 {
            let t = self.tick().await;
            if t.warm == n && t.pending == 0 {
                break;
            }
        }
        let warm: Vec<ComputeRec> = self
            .unbound()
            .await
            .into_iter()
            .filter(|c| c.status == ComputeStatus::Running)
            .collect();
        assert_eq!(warm.len(), n, "{warm:?}");
        warm
    }
}

/// A wanted key gets `size` computes, recorded unbound before the runtime
/// starts them, with the pool's key, size and signing key; they are warm
/// once `compute_ctl` waits for a spec. One that dies is drained and
/// replaced. Nothing is started before any endpoint wants the pool.
#[tokio::test]
async fn pool_refills_to_size() {
    let e = pooled!(2);
    assert_eq!(e.tick().await, PoolTick::default(), "nothing wanted yet");
    assert!(e.runtime.starts().is_empty());

    e.wanted().await;
    let t = e.tick().await;
    assert!(t.maintained);
    assert_eq!((t.started, t.pending, t.warm), (2, 2, 0));
    let started = e.unbound().await;
    assert_eq!(started.len(), 2);
    for c in &started {
        assert_eq!(c.status, ComputeStatus::Pending);
        assert_eq!(c.pool.as_ref(), Some(&key(IMAGE)));
        assert_eq!(c.cu, Cu(1));
        assert_eq!(c.jwt_key_id.as_deref(), Some(e.keys.kid()));
        assert!(c.ctl_url.is_some());
        assert_eq!(
            e.runtime.lock().launched.get(&c.id),
            Some(&(NS.to_string(), IMAGE.to_string())),
            "started unbound in its namespace with the major's image"
        );
    }

    let t = e.tick().await;
    assert_eq!((t.started, t.warm, t.pending), (0, 2, 0));
    let warm = e.warm(2).await;
    assert_eq!(e.r.tick_pool().await.unwrap().unwrap().started, 0);

    // One dies: drained, and the pool refills.
    e.runtime.crash(&warm[0].id);
    let t = e.tick().await;
    assert_eq!((t.drained, t.started, t.warm), (1, 1, 1));
    assert!(e.runtime.stops().contains(&warm[0].id));
    assert!(e.compute(&warm[0].id).await.is_none(), "its record removed");
    e.warm(2).await;

    // One that never waits for a spec is drained at the start timeout.
    let before = e.runtime.starts().len();
    e.runtime.crash(&e.warm(2).await[0].id);
    e.tick().await;
    let fresh = e.runtime.starts()[before].clone();
    e.ctl.set(&fresh, CtlStatus::Starting);
    e.h.clock
        .advance_ms(u64::try_from(PoolConfig::default().start_timeout.as_millis()).unwrap());
    let t = e.tick().await;
    assert_eq!(t.drained, 1);
    assert!(e.runtime.stops().contains(&fresh));
    assert_eq!(t.started, 1, "{t:?}");
}

/// Starting an endpoint binds a warm compute instead of starting one: its
/// record is bound and names the endpoint, it gets its spec through
/// `/configure`, it runs behind the endpoint's address, and the pool
/// refills.
#[tokio::test]
async fn take_binds_and_refills() {
    let e = pooled!(2);
    let (project, id) = e.wanted().await;
    let warm = e.warm(2).await;
    let starts = e.runtime.starts().len();

    e.start(&project, &id, "start1").await;
    e.pass(&project).await;
    let rec = e.endpoint(&project, &id).await.expect("the endpoint");
    let taken = rec.compute_id.clone().expect("a compute");
    assert!(warm.iter().any(|c| c.id == taken), "a warm one was taken");
    assert_eq!(e.runtime.starts().len(), starts, "nothing new started");
    let c = e.compute(&taken).await.expect("the C/ record");
    let binding = c.binding.clone().expect("bound");
    assert_eq!(
        (
            binding.namespace.as_str(),
            binding.project_id.as_str(),
            binding.endpoint_id.as_str()
        ),
        (NS, project.as_str(), id.as_str())
    );
    assert_eq!(c.bound_at_ms, Some(e.h.clock.0.load(Ordering::SeqCst)));
    assert_eq!(c.pool.as_ref(), Some(&key(IMAGE)), "its pool is kept");
    assert!(
        e.ctl.configured().iter().any(|(c, _)| *c == taken),
        "the spec was pushed"
    );

    e.running(&project, &id).await;
    let c = e.compute(&taken).await.expect("the C/ record");
    assert_eq!(c.status, ComputeStatus::Running);
    assert_eq!(
        e.runtime
            .lock()
            .computes
            .get(&taken)
            .map(|(ep, _)| ep.clone()),
        Some(id.clone()),
        "the endpoint's address points at it"
    );
    let t = e.tick().await;
    assert_eq!((t.started, t.warm), (1, 1));
    let rewarmed = e.warm(2).await;
    assert!(rewarmed.iter().all(|c| c.id != taken));

    // Suspended, it stops like any compute; the next start takes another.
    e.suspend(&project, &id, "suspend1").await;
    e.until(&project, "the endpoint suspended", || async {
        e.compute(&taken).await.is_none()
    })
    .await;
    assert!(e.runtime.stops().contains(&taken));
    e.start(&project, &id, "start2").await;
    e.running(&project, &id).await;
    let next = e
        .endpoint(&project, &id)
        .await
        .and_then(|r| r.compute_id)
        .expect("a compute");
    assert!(rewarmed.iter().any(|c| c.id == next));
}

/// A new image for the major (a restart with a new pin) drains the warm
/// computes of the old one and fills the pool with the new; a compute an
/// endpoint took keeps running.
#[tokio::test]
async fn image_change_drains_pool() {
    let e = pooled!(2);
    let (project, id) = e.wanted().await;
    e.warm(2).await;
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let taken = e
        .endpoint(&project, &id)
        .await
        .and_then(|r| r.compute_id)
        .expect("a compute");
    let old = e.warm(2).await;

    let (r2, _) = e.restarted(IMAGE_NEXT, PoolConfig::new(2));
    r2.reconcile_project(NS, &project).await.expect("a pass");
    let t = r2.tick_pool().await.unwrap().unwrap();
    assert_eq!((t.drained, t.started), (2, 2), "{t:?}");
    for c in &old {
        assert!(e.runtime.stops().contains(&c.id), "{} drained", c.id);
        assert!(e.compute(&c.id).await.is_none());
    }
    assert!(!e.runtime.stops().contains(&taken), "the taken one runs on");
    let unbound = e.unbound().await;
    assert_eq!(unbound.len(), 2);
    for c in unbound {
        assert_eq!(c.pool, Some(key(IMAGE_NEXT)));
        assert_eq!(
            e.runtime.lock().launched.get(&c.id).map(|(_, i)| i.clone()),
            Some(IMAGE_NEXT.to_string())
        );
    }
}

/// Size 0: nothing is started, a start makes its own compute, and a pool
/// left from before is drained.
#[tokio::test]
async fn pool_size_zero_disables() {
    let e = pooled!(0);
    let (project, id) = e.wanted().await;
    let t = e.tick().await;
    assert_eq!(t, PoolTick::default());
    e.start(&project, &id, "start1").await;
    e.running(&project, &id).await;
    let compute = e
        .endpoint(&project, &id)
        .await
        .and_then(|r| r.compute_id)
        .expect("a compute");
    assert_eq!(e.runtime.starts(), vec![compute.clone()]);
    assert_eq!(e.compute(&compute).await.and_then(|c| c.pool), None);

    // A pool of 2 left behind, then a restart with size 0.
    let (r2, _) = e.restarted(IMAGE, PoolConfig::new(2));
    r2.reconcile_project(NS, &project).await.expect("a pass");
    r2.tick_pool().await.unwrap();
    r2.tick_pool().await.unwrap();
    assert_eq!(e.unbound().await.len(), 2);
    let (r3, _) = e.restarted(IMAGE, PoolConfig::new(0));
    r3.reconcile_project(NS, &project).await.expect("a pass");
    let t = r3.tick_pool().await.unwrap().unwrap();
    assert_eq!((t.drained, t.started), (2, 0));
    assert!(e.unbound().await.is_empty());
}

/// A warm compute's spec upcall answers `empty`; once bound, `attached`,
/// with the spec the reconciler pushed.
#[tokio::test]
async fn bound_compute_gets_attached_spec() {
    let e = pooled!(1);
    let (project, id) = e.wanted().await;
    let warm = e.warm(1).await.remove(0);

    let at = e.h.clock.0.clone();
    let listen = "127.0.0.1:0".parse().unwrap();
    let env_sources = crate::env::sources();
    let server = UpcallServer::bind(
        e.h.store.clone(),
        e.keys.clone(),
        Arc::new(ComputeCtlConfigurer::new(
            e.keys.clone(),
            Clock::from_fn(|| 0),
        )),
        UpcallConfig {
            pageservers: env_sources.pageservers.clone(),
            default_pageserver: env_sources.default_pageserver,
            wal_pools: env_sources.wal_pools.clone(),
            clock: Clock::from_fn(move || at.load(Ordering::SeqCst)),
            ..UpcallConfig::defaults(listen)
        },
    )
    .await
    .expect("the upcall listener");
    let base = format!("http://{}", server.local_addr());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(server.serve(async move {
        let _ = stopped.await;
    }));
    let compute: ComputeId = warm.id.parse().unwrap();
    let token = e
        .keys
        .issue_upcall_token(
            &compute,
            e.h.clock.0.load(Ordering::SeqCst),
            COMPUTE_TOKEN_TTL,
        )
        .unwrap();
    let spec = || async {
        let resp = reqwest::Client::new()
            .get(format!("{base}/compute/api/v2/computes/{compute}/spec"))
            .bearer_auth(token.expose())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        resp.json::<Value>().await.unwrap()
    };
    assert_eq!(spec().await["status"], "empty");

    e.start(&project, &id, "start1").await;
    e.pass(&project).await;
    assert_eq!(
        e.endpoint(&project, &id).await.and_then(|r| r.compute_id),
        Some(warm.id.clone())
    );
    let answer = spec().await;
    assert_eq!(answer["status"], "attached");
    let pushed: BTreeMap<String, Value> = e
        .ctl
        .configured()
        .into_iter()
        .map(|(c, s)| (c, serde_json::to_value(s).unwrap()))
        .collect();
    assert_eq!(pushed.get(&warm.id), Some(&answer["spec"]));
    let _ = stop.send(());
}

/// An endpoint of another size starts its own compute; the pool keeps its
/// warm ones.
#[tokio::test]
async fn other_sizes_start_their_own() {
    let e = pooled!(1);
    let (project, _) = e.ready_project().await;
    let big = e
        .create_with(CreateEndpoint {
            namespace: NS.into(),
            project_id: project.clone(),
            branch_id: String::new(),
            kind: EndpointType::ReadWrite,
            min_cu: Some(Cu(4)),
            max_cu: Some(Cu(4)),
            idempotency_key: "big".into(),
            ..CreateEndpoint::default()
        })
        .await;
    let id = big.endpoint.record.id;
    e.pass(&project).await;
    let warm = e.warm(1).await.remove(0);
    e.start(&project, &id, "start-big").await;
    e.running(&project, &id).await;
    let compute = e
        .endpoint(&project, &id)
        .await
        .and_then(|r| r.compute_id)
        .expect("a compute");
    assert_ne!(compute, warm.id);
    assert_eq!(e.compute(&compute).await.and_then(|c| c.pool), None);
    assert_eq!(e.warm(1).await[0].id, warm.id);
}

/// The pool is maintained by one instance: another holding the lease
/// leaves it alone (it still reads which computes are warm).
#[tokio::test]
async fn one_instance_maintains() {
    let e = pooled!(2);
    e.wanted().await;
    let fence =
        e.h.store
            .acquire_lease(POOL_LEASE, "pg-control-b", Duration::from_secs(60))
            .await
            .unwrap();
    let t = e.tick().await;
    assert!(!t.maintained);
    assert_eq!(t.started, 0);
    assert!(e.runtime.starts().is_empty());
    drop(fence);
}

/// A namespace no endpoint wants any more is drained once its want
/// expires; an instance that just started (and has not passed over every
/// project yet) drains nothing for want of demand.
#[tokio::test]
async fn unwanted_pool_drains_once_settled() {
    let e = pooled!(2);
    e.wanted().await;
    e.warm(2).await;
    let ttl = u64::try_from(PoolConfig::default().want_ttl.as_millis()).unwrap();

    // Restarted, past the want's expiry, before any pass: nothing drained.
    e.h.clock.advance_ms(ttl);
    let (r2, _) = e.restarted(IMAGE, PoolConfig::new(2));
    let t = r2.tick_pool().await.unwrap().unwrap();
    assert_eq!((t.drained, t.warm), (0, 2), "{t:?}");

    // Still unwanted once the instance has been up a whole want's life.
    e.h.clock.advance_ms(ttl);
    let t = r2.tick_pool().await.unwrap().unwrap();
    assert_eq!((t.drained, t.started), (2, 0), "{t:?}");
    assert!(e.unbound().await.is_empty());
}

/// A warm compute the runtime fails on does not hold up the others: the
/// tick drains the rest, and the failed one is retried at the next tick.
#[tokio::test]
async fn one_failing_compute_does_not_stop_the_tick() {
    let e = pooled!(2);
    e.wanted().await;
    let warm = e.warm(2).await;
    let (stuck, other) = (&warm[0].id, &warm[1].id);
    e.runtime.crash(stuck);
    e.runtime.crash(other);
    e.runtime.lock().fail_stop.insert(stuck.clone());
    let t = e.tick().await;
    assert_eq!(t.drained, 1, "{t:?}");
    assert!(e.compute(other).await.is_none());
    assert_eq!(
        e.compute(stuck).await.map(|c| c.status),
        Some(ComputeStatus::Stopping)
    );

    e.runtime.lock().fail_stop.clear();
    let t = e.tick().await;
    assert_eq!(t.drained, 1, "{t:?}");
    assert!(e.compute(stuck).await.is_none());
    e.warm(2).await;
}
