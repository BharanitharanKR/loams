//! The autoscaler (PG2 Task 15, design §46 §7.4): the policy as a pure
//! function of a simulated metrics feed, then the applier on
//! `endpoints/env.rs`'s fakes (a runtime that resizes, refuses or cannot,
//! and a fake `compute_ctl`).
#![allow(clippy::unwrap_used)]

#[macro_use]
mod common;

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loams_pg_control::autoscale::{
    Autoscaler, ComputeMetrics, Decision, MetricsError, Observation, Policy, Sample, TICK,
    file_cache_mb,
};
use loams_pg_control::model::{ComputeRec, Cu, EndpointType};
use loams_pg_control::observer::LifecycleKind;
use loams_pg_control::runtime::ResizeOutcome;
use loams_pg_control::service::endpoints::CreateEndpoint;
use loams_pg_control::store::conformance::{Factory, local_factory};

use crate::env::{Env, NS};

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

const S: u64 = 1000;
const TICK_MS: u64 = 5 * S;

fn obs(cu: u16, min: u16, max: u16, cpu: Option<f64>) -> Observation {
    Observation {
        namespace: NS.into(),
        compute: "cmp-a".into(),
        cu: Cu(cu),
        min_cu: Cu(min),
        max_cu: Cu(max),
        sample: Sample {
            cpu,
            ..Sample::default()
        },
    }
}

/// Feeds `o` every tick from `from_ms` to `to_ms` (both included) and
/// answers the decisions with the time each came.
fn feed(a: &mut Autoscaler, from_ms: u64, to_ms: u64, o: &Observation) -> Vec<(u64, Decision)> {
    let mut out = Vec::new();
    let mut t = from_ms;
    while t <= to_ms {
        out.extend(
            a.tick(t, std::slice::from_ref(o))
                .into_iter()
                .map(|d| (t, d)),
        );
        t += TICK_MS;
    }
    out
}

// ---- The policy ----

/// CPU above 70 % for 15 s doubles the compute, not sooner; a dip resets
/// the window.
#[test]
fn scales_up_after_15s_high_cpu() {
    assert_eq!(TICK.as_millis(), 5000);
    let mut a = Autoscaler::new(Policy::default());
    let hot = obs(4, 1, 16, Some(0.9));
    assert!(feed(&mut a, 0, 10 * S, &hot).is_empty(), "not before 15 s");
    // A dip below the threshold starts the window again.
    assert!(a.tick(12 * S, &[obs(4, 1, 16, Some(0.5))]).is_empty());
    assert!(feed(&mut a, 13 * S, 27 * S, &hot).is_empty());
    let d = a.tick(28 * S, std::slice::from_ref(&hot));
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!((d[0].from, d[0].to), (Cu(4), Cu(8)));
    assert!(d[0].is_up());
    // Applied: the window starts again.
    a.applied("cmp-a");
    assert!(a.tick(29 * S, &[obs(8, 1, 16, Some(0.9))]).is_empty());
}

/// CPU below 30 % for 2 minutes shrinks the compute by a quarter (at
/// least one 0.25 CU step), not sooner.
#[test]
fn scales_down_after_2min_low_cpu() {
    let mut a = Autoscaler::new(Policy::default());
    let cold = obs(8, 1, 16, Some(0.1));
    assert!(
        feed(&mut a, 0, 115 * S, &cold).is_empty(),
        "not before 2 min"
    );
    let d = a.tick(120 * S, std::slice::from_ref(&cold));
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!((d[0].from, d[0].to), (Cu(8), Cu(6)));
    assert!(!d[0].is_up());
    a.applied("cmp-a");
    // Below 1 CU a quarter of the size is less than a step: one step.
    let mut a = Autoscaler::new(Policy::default());
    let d = feed(&mut a, 0, 120 * S, &obs(2, 1, 16, Some(0.0)));
    assert_eq!(d.last().map(|(_, d)| d.to), Some(Cu(1)));
    // Between the thresholds nothing happens.
    let mut a = Autoscaler::new(Policy::default());
    assert!(feed(&mut a, 0, 600 * S, &obs(8, 1, 16, Some(0.5))).is_empty());
}

/// Never below `min_cu` nor above `max_cu`: at the bounds a hot or cold
/// compute stays, and one outside them (the endpoint's range changed) is
/// moved inside at once.
#[test]
fn never_outside_min_max() {
    let mut a = Autoscaler::new(Policy::default());
    assert!(feed(&mut a, 0, 600 * S, &obs(16, 1, 16, Some(1.0))).is_empty());
    let mut a = Autoscaler::new(Policy::default());
    assert!(feed(&mut a, 0, 600 * S, &obs(4, 4, 8, Some(0.0))).is_empty());
    // 6 quarters doubled would be 12; the range ends at 8.
    let mut a = Autoscaler::new(Policy::default());
    let d = feed(&mut a, 0, 15 * S, &obs(6, 1, 8, Some(1.0)));
    assert_eq!(d.last().map(|(_, d)| d.to), Some(Cu(8)));
    // 5 quarters less a quarter is 4 (floored); the range starts at 5.
    let mut a = Autoscaler::new(Policy::default());
    assert!(feed(&mut a, 0, 600 * S, &obs(5, 5, 8, Some(0.0))).is_empty());
    // Outside the range: moved in at the first tick, whatever the load.
    let mut a = Autoscaler::new(Policy::default());
    let d = a.tick(0, &[obs(2, 4, 8, Some(0.5))]);
    assert_eq!((d[0].from, d[0].to), (Cu(2), Cu(4)));
    let d = a.tick(0, &[obs(12, 4, 8, None)]);
    assert_eq!((d[0].from, d[0].to), (Cu(12), Cu(8)));
}

/// Each decision is at most ×2 up and −25 % down (one step at least), in
/// 0.25 CU steps, and moves by at least one step.
#[test]
fn step_limits_respected() {
    for cu in 1..=16u16 {
        let mut a = Autoscaler::new(Policy::default());
        if let Some((_, d)) = feed(&mut a, 0, 15 * S, &obs(cu, 1, 64, Some(1.0))).pop() {
            assert!(d.to.0 <= cu * 2 && d.to.0 > cu, "{cu}: {d:?}");
        }
        let mut a = Autoscaler::new(Policy::default());
        if let Some((_, d)) = feed(&mut a, 0, 120 * S, &obs(cu, 1, 64, Some(0.0))).pop() {
            let step = (cu / 4).max(1);
            assert_eq!(d.to.0, cu - step, "{cu}: {d:?}");
        }
    }
    // A file cache whose working set passes 75 % of it scales up at once.
    let mut a = Autoscaler::new(Policy::default());
    let mut o = obs(4, 1, 16, Some(0.5));
    let size = file_cache_mb(Cu(4)) * 1024 * 1024;
    o.sample.lfc_size = Some(size);
    o.sample.lfc_working_set = Some(size * 8 / 10);
    let d = a.tick(0, std::slice::from_ref(&o));
    assert_eq!((d[0].from, d[0].to), (Cu(4), Cu(8)));
    // And a cold compute whose working set would not fit a smaller cache
    // stays.
    let mut a = Autoscaler::new(Policy::default());
    let mut o = obs(8, 1, 16, Some(0.0));
    o.sample.lfc_size = Some(file_cache_mb(Cu(8)) * 1024 * 1024);
    o.sample.lfc_working_set = Some(file_cache_mb(Cu(6)) * 1024 * 1024);
    assert!(feed(&mut a, 0, 600 * S, &o).is_empty());
}

/// A namespace's cap on its computes' total CU (the limits record, Task
/// 47) bounds every scale-up: a compute gets what is left, or nothing.
#[test]
fn quota_caps_total_cu() {
    let mut a = Autoscaler::new(Policy::default());
    a.set_cap(NS, Some(Cu(14)));
    let hot = |id: &str, cu: u16| Observation {
        compute: id.into(),
        ..obs(cu, 1, 16, Some(1.0))
    };
    let all = [hot("cmp-a", 4), hot("cmp-b", 4)];
    let mut decisions = Vec::new();
    for t in (0..=15).step_by(5) {
        decisions = a.tick(t * S, &all);
    }
    // 8 in use, 6 left: the first doubles (+4), the second gets the 2 left.
    let to: BTreeMap<String, Cu> = decisions
        .iter()
        .map(|d| (d.compute.clone(), d.to))
        .collect();
    assert_eq!(to.get("cmp-a"), Some(&Cu(8)));
    assert_eq!(to.get("cmp-b"), Some(&Cu(6)));
    // At the cap nothing scales up; another namespace is not capped.
    let mut a = Autoscaler::new(Policy::default());
    a.set_cap(NS, Some(Cu(8)));
    let other = Observation {
        namespace: "other".into(),
        ..hot("cmp-c", 4)
    };
    let mut decisions = Vec::new();
    for t in (0..=15).step_by(5) {
        decisions = a.tick(t * S, &[hot("cmp-a", 4), hot("cmp-b", 4), other.clone()]);
    }
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert_eq!(decisions[0].compute, "cmp-c");
    // Lifting the cap lets them grow.
    a.set_cap(NS, None);
    let d = a.tick(20 * S, &[hot("cmp-a", 4)]);
    assert_eq!(d.len(), 1);
}

// ---- The applier ----

/// A metrics feed the test sets.
#[derive(Debug, Clone, Default)]
struct FakeMetrics(Arc<Mutex<BTreeMap<String, Sample>>>);

impl FakeMetrics {
    fn set(&self, compute: &str, s: Sample) {
        self.0.lock().unwrap().insert(compute.into(), s);
    }
}

#[async_trait]
impl ComputeMetrics for FakeMetrics {
    async fn sample(&self, _ns: &str, c: &ComputeRec) -> Result<Sample, MetricsError> {
        self.0
            .lock()
            .unwrap()
            .get(&c.id)
            .cloned()
            .ok_or_else(|| MetricsError("no sample".into()))
    }
}

/// `let (e, m) = scaling!();`: the harness with a reconciler whose
/// computes are autoscaled on a fake metrics feed.
macro_rules! scaling {
    () => {{
        let h = harness!();
        let m = FakeMetrics::default();
        let e = crate::env::env_scaling(h, Arc::new(m.clone()));
        (e, m)
    }};
}

impl Env {
    /// A running endpoint of 1 CU (0.25 to 4 CU): (project, endpoint,
    /// compute).
    async fn scaled_endpoint(&self) -> (String, String, String) {
        let (project, _) = self.ready_project().await;
        let ep = self
            .create_with(CreateEndpoint {
                namespace: NS.into(),
                project_id: project.clone(),
                branch_id: String::new(),
                kind: EndpointType::ReadWrite,
                min_cu: Some(Cu(4)),
                max_cu: Some(Cu(16)),
                idempotency_key: "scaled".into(),
                ..CreateEndpoint::default()
            })
            .await;
        let id = ep.endpoint.record.id;
        self.start(&project, &id, "start").await;
        self.running(&project, &id).await;
        let compute = self
            .endpoint(&project, &id)
            .await
            .and_then(|e| e.compute_id)
            .expect("a compute");
        (project, id, compute)
    }

    /// Ticks the autoscaler every 5 s for `secs`.
    async fn scale_for(&self, secs: u64) {
        let mut t = 0;
        while t <= secs {
            self.r
                .tick_autoscaler()
                .await
                .expect("a tick")
                .expect("an autoscaler");
            self.h.clock.advance_ms(TICK_MS);
            t += 5;
        }
    }
}

fn hot() -> Sample {
    Sample {
        cpu: Some(0.95),
        ..Sample::default()
    }
}

/// A refused resize (no room on the node) is logged and retried at the
/// next tick, without waiting for another window; once applied, the
/// compute's record, its file cache (`/configure`) and the observer see
/// the new size.
#[tokio::test]
async fn refused_resize_retries_and_logs() {
    let (e, m) = scaling!();
    let (_, _, compute) = e.scaled_endpoint().await;
    m.set(&compute, hot());
    e.runtime
        .lock()
        .resize
        .push_back(ResizeOutcome::Refused("no room on the node".into()));
    e.scale_for(15).await;
    assert_eq!(e.runtime.lock().resized.len(), 1, "asked once");
    assert_eq!(e.compute(&compute).await.map(|c| c.cu), Some(Cu(4)));
    // The next tick asks again, and the runtime now applies it.
    e.scale_for(0).await;
    assert_eq!(
        e.runtime.lock().resized,
        vec![(compute.clone(), Cu(8)), (compute.clone(), Cu(8))]
    );
    let rec = e.compute(&compute).await.expect("the compute");
    assert_eq!(rec.cu, Cu(8));
    let pushed = e
        .ctl
        .configured()
        .into_iter()
        .rfind(|(c, _)| *c == compute)
        .expect("a spec pushed")
        .1;
    let limit = serde_json::to_value(&pushed).unwrap().to_string();
    assert!(
        limit.contains(&format!(
            "\"name\":\"neon.file_cache_size_limit\",\"value\":\"{}\"",
            file_cache_mb(Cu(8))
        )),
        "{limit}"
    );
    assert!(
        e.events
            .events()
            .iter()
            .any(|ev| ev.compute.to_string() == compute
                && ev.event == LifecycleKind::Resized { cu: Cu(8) })
    );
}

/// A runtime that cannot resize (compose) is asked once; the compute is not
/// decided on again.
#[tokio::test]
async fn unsupported_resize_stops_deciding() {
    let (e, m) = scaling!();
    let (_, _, compute) = e.scaled_endpoint().await;
    m.set(&compute, hot());
    e.runtime
        .lock()
        .resize
        .push_back(ResizeOutcome::Unsupported);
    e.scale_for(60).await;
    assert_eq!(e.runtime.lock().resized.len(), 1);
    assert_eq!(e.compute(&compute).await.map(|c| c.cu), Some(Cu(4)));
}

/// Scaling down shrinks the file cache before the memory.
#[tokio::test]
async fn scale_down_shrinks_the_cache_first() {
    let (e, m) = scaling!();
    let (_, _, compute) = e.scaled_endpoint().await;
    m.set(&compute, hot());
    e.scale_for(15).await;
    assert_eq!(e.compute(&compute).await.map(|c| c.cu), Some(Cu(8)));
    m.set(
        &compute,
        Sample {
            cpu: Some(0.05),
            ..Sample::default()
        },
    );
    let pushes = e.ctl.configured().len();
    e.runtime.lock().order.clear();
    e.scale_for(120).await;
    assert_eq!(e.compute(&compute).await.map(|c| c.cu), Some(Cu(6)));
    assert!(e.ctl.configured().len() > pushes);
    assert_eq!(
        e.runtime.lock().order,
        vec!["configure".to_string(), format!("resize {compute} 6")]
    );
}
