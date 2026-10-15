//! The autoscaler (design §46 §7.4, D712; PG2 Task 15).
//!
//! - **The policy** ([`Autoscaler::tick`], pure): every [`TICK`] each
//!   running compute is sampled. It scales up when its CPU is above 70 % of
//!   its size for 15 s, or its local file cache's working set passes 75 %
//!   of the cache; down when its CPU is below 30 % for 2 minutes and its
//!   working set would fit the smaller cache. A step is at most ×2 up and
//!   −25 % down, and at least one 0.25 CU step (below 1 CU a quarter of the
//!   size is less than a step: one step it is). Never outside the
//!   endpoint's `[min_cu, max_cu]`: a compute outside it (the range changed)
//!   is moved in at once. A namespace's cap on its computes' total CU (the
//!   limits record, Task 47: [`Autoscaler::set_cap`]) bounds every
//!   scale-up.
//! - **Retries.** A decision's window is kept until it is
//!   [`applied`](Autoscaler::applied), so a resize the runtime refused (no
//!   room on the node) is asked again at the next tick. A runtime that
//!   cannot resize at all ([`unsupported`](Autoscaler::unsupported): the
//!   compose runtime) is not asked again for that compute.
//! - **The mechanism** (the reconciler's applier, under the project's
//!   lease): Kubernetes in-place Pod resize ([`ComputeRuntime::resize`]),
//!   then `/configure` with the new `neon.file_cache_size_limit`
//!   ([`file_cache_mb`]); down, the cache shrinks first and the memory
//!   after. The compute's record takes the new size, and the lifecycle
//!   observer hears `Resized`.
//! - **Samples** ([`ComputeMetrics`]): CPU from the Kubernetes metrics API
//!   (`metrics.k8s.io`, the runtime's), the file cache's size and working
//!   set from `compute_ctl`'s `/autoscaling_metrics`
//!   ([`CtlMetrics`]); [`Combined`] joins them.
//! - **Q644** (R15.1): in-place Pod resize, never NeonVM (it needs KVM
//!   nodes, which BYOC and Autopilot-class clusters lack); 0.25 CU stays the
//!   smallest size.
//!
//! [`ComputeRuntime::resize`]: crate::runtime::ComputeRuntime::resize

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::model::{ComputeRec, Cu};
use crate::reconcile::endpoint::ComputeCtl;

/// How often computes are sampled and decided on.
pub const TICK: Duration = Duration::from_secs(5);

/// The local file cache per 0.25 CU, in MiB: 75 % of its 1 GiB of memory,
/// as Neon sizes it.
pub const FILE_CACHE_MB_PER_QUARTER: u64 = 768;

/// The largest compute (16 CU), whose file cache sizes every compute's
/// `neon.max_file_cache_size` (a start-time setting; the limit moves under
/// it).
pub const MAX_CU: Cu = Cu(64);

/// `neon.file_cache_size_limit` of a compute of `cu`, in MiB.
pub fn file_cache_mb(cu: Cu) -> u64 {
    u64::from(cu.0) * FILE_CACHE_MB_PER_QUARTER
}

/// `max_connections` of a compute whose endpoint goes up to `cu` (PG2
/// Task 20, R20.3): Neon's table, about 450 per CU (112 at 0.25 CU, 450 at
/// 1 CU, 1802 at 4 CU), at most 4,000. A start-time setting, so the
/// endpoint's largest size sets it; PgDog's pools take 80 % of it.
pub fn max_connections(cu: Cu) -> u32 {
    (u32::from(cu.0) * 4505 / 40).clamp(1, 4000)
}

/// The thresholds of §46 §7.4.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Up when CPU (a fraction of the compute's size) is above this ...
    pub up_cpu: f64,
    /// ... for this long.
    pub up_for: Duration,
    /// Down when CPU is below this ...
    pub down_cpu: f64,
    /// ... for this long.
    pub down_for: Duration,
    /// Up when the file cache's working set passes this fraction of it.
    pub cache_up: f64,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            up_cpu: 0.70,
            up_for: Duration::from_secs(15),
            down_cpu: 0.30,
            down_for: Duration::from_secs(120),
            cache_up: 0.75,
        }
    }
}

/// One sample of a compute; `None` where it could not be read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    /// CPU used, as a fraction of the compute's size.
    pub cpu: Option<f64>,
    /// The local file cache's working set, in bytes.
    pub lfc_working_set: Option<u64>,
    /// The local file cache's size limit, in bytes.
    pub lfc_size: Option<u64>,
}

/// What [`Autoscaler::tick`] reads of one running compute.
#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub namespace: String,
    pub compute: String,
    pub cu: Cu,
    pub min_cu: Cu,
    pub max_cu: Cu,
    pub sample: Sample,
}

/// Resize `compute` from `from` to `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub namespace: String,
    pub compute: String,
    pub from: Cu,
    pub to: Cu,
}

impl Decision {
    pub fn is_up(&self) -> bool {
        self.to > self.from
    }
}

/// One compute's windows.
#[derive(Debug, Default)]
struct Windows {
    hot_since: Option<u64>,
    cold_since: Option<u64>,
}

/// The policy's state: each compute's windows, the computes that cannot
/// be resized, and the namespaces' caps.
#[derive(Debug, Default)]
pub struct Autoscaler {
    policy: Policy,
    windows: HashMap<String, Windows>,
    unsupported: HashSet<String>,
    caps: BTreeMap<String, Cu>,
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl Autoscaler {
    pub fn new(policy: Policy) -> Self {
        Autoscaler {
            policy,
            ..Autoscaler::default()
        }
    }

    /// Caps the total CU of `namespace`'s computes (`None`: no cap).
    pub fn set_cap(&mut self, namespace: &str, cap: Option<Cu>) {
        match cap {
            Some(cap) => self.caps.insert(namespace.to_owned(), cap),
            None => self.caps.remove(namespace),
        };
    }

    /// A decision for `compute` was applied: its windows start again.
    pub fn applied(&mut self, compute: &str) {
        self.windows.remove(compute);
    }

    /// The runtime cannot resize `compute`: it is not decided on again.
    pub fn unsupported(&mut self, compute: &str) {
        self.unsupported.insert(compute.to_owned());
        self.windows.remove(compute);
    }

    /// `compute` ended.
    pub fn forget(&mut self, compute: &str) {
        self.windows.remove(compute);
        self.unsupported.remove(compute);
    }

    /// The decisions for `observations` at `now_ms` (see the module docs),
    /// in the observations' order. Up decisions are cut to what each
    /// namespace's cap leaves.
    pub fn tick(&mut self, now_ms: u64, observations: &[Observation]) -> Vec<Decision> {
        let mut totals: BTreeMap<&str, u32> = BTreeMap::new();
        for o in observations {
            *totals.entry(o.namespace.as_str()).or_default() += u32::from(o.cu.0);
        }
        let mut out = Vec::new();
        for o in observations {
            if self.unsupported.contains(&o.compute) {
                continue;
            }
            let Some(to) = self.wanted(now_ms, o) else {
                continue;
            };
            let to = if to > o.cu {
                match self.caps.get(&o.namespace) {
                    Some(cap) => {
                        let total = totals.get(o.namespace.as_str()).copied().unwrap_or(0);
                        let room = u32::from(cap.0).saturating_sub(total);
                        let most = u32::from(o.cu.0) + room;
                        Cu(u16::try_from(most.min(u32::from(to.0))).unwrap_or(to.0))
                    }
                    None => to,
                }
            } else {
                to
            };
            if to == o.cu {
                continue;
            }
            if to > o.cu
                && let Some(t) = totals.get_mut(o.namespace.as_str())
            {
                *t += u32::from(to.0 - o.cu.0);
            }
            out.push(Decision {
                namespace: o.namespace.clone(),
                compute: o.compute.clone(),
                from: o.cu,
                to,
            });
        }
        out
    }

    /// The size `o` should have now, if not its own.
    fn wanted(&mut self, now_ms: u64, o: &Observation) -> Option<Cu> {
        let (min, max) = (o.min_cu, o.max_cu.max(o.min_cu));
        if o.cu < min {
            return Some(min);
        }
        if o.cu > max {
            return Some(max);
        }
        let p = &self.policy;
        let w = self.windows.entry(o.compute.clone()).or_default();
        match o.sample.cpu {
            Some(cpu) if cpu > p.up_cpu => {
                w.hot_since.get_or_insert(now_ms);
                w.cold_since = None;
            }
            Some(cpu) if cpu < p.down_cpu => {
                w.cold_since.get_or_insert(now_ms);
                w.hot_since = None;
            }
            _ => *w = Windows::default(),
        }
        let hot = w
            .hot_since
            .is_some_and(|t| now_ms.saturating_sub(t) >= ms(p.up_for));
        let cold = w
            .cold_since
            .is_some_and(|t| now_ms.saturating_sub(t) >= ms(p.down_for));
        let cache_full = match (o.sample.lfc_working_set, o.sample.lfc_size) {
            (Some(ws), Some(size)) if size > 0 => (ws as f64) > p.cache_up * size as f64,
            _ => false,
        };
        if (hot || cache_full) && o.cu < max {
            return Some(Cu(o.cu.0.saturating_mul(2).min(max.0)));
        }
        if cold && o.cu > min {
            let step = (o.cu.0 / 4).max(1);
            let to = Cu(o.cu.0.saturating_sub(step).max(min.0));
            // The working set must fit the smaller cache.
            let fits = o.sample.lfc_working_set.is_none_or(|ws| {
                (ws as f64) <= p.cache_up * (file_cache_mb(to) * 1024 * 1024) as f64
            });
            if fits {
                return Some(to);
            }
        }
        None
    }
}

/// Why a sample could not be read (for logs).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MetricsError(pub String);

/// Where samples come from.
#[async_trait]
pub trait ComputeMetrics: Send + Sync + 'static {
    /// One sample of `compute`, in Loams namespace `ns`.
    async fn sample(&self, ns: &str, compute: &ComputeRec) -> Result<Sample, MetricsError>;
}

/// The file cache's size and working set from `compute_ctl`'s
/// `/autoscaling_metrics` ([`ComputeCtl::file_cache`]).
pub struct CtlMetrics(pub Arc<dyn ComputeCtl>);

impl std::fmt::Debug for CtlMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CtlMetrics").finish_non_exhaustive()
    }
}

#[async_trait]
impl ComputeMetrics for CtlMetrics {
    async fn sample(&self, _ns: &str, compute: &ComputeRec) -> Result<Sample, MetricsError> {
        let fc = self
            .0
            .file_cache(compute)
            .await
            .map_err(|e| MetricsError(e.to_string()))?;
        Ok(Sample {
            cpu: None,
            lfc_working_set: fc.working_set_bytes,
            lfc_size: fc.size_bytes,
        })
    }
}

/// CPU from one source, the file cache from another; a source that fails
/// leaves its fields `None`.
pub struct Combined {
    pub cpu: Arc<dyn ComputeMetrics>,
    pub cache: Arc<dyn ComputeMetrics>,
}

impl std::fmt::Debug for Combined {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Combined").finish_non_exhaustive()
    }
}

#[async_trait]
impl ComputeMetrics for Combined {
    async fn sample(&self, ns: &str, compute: &ComputeRec) -> Result<Sample, MetricsError> {
        let (cpu, cache) =
            tokio::join!(self.cpu.sample(ns, compute), self.cache.sample(ns, compute));
        if let (Err(e), Err(_)) = (&cpu, &cache) {
            return Err(e.clone());
        }
        let cpu = cpu.ok().and_then(|s| s.cpu);
        let cache = cache.unwrap_or_default();
        Ok(Sample {
            cpu,
            lfc_working_set: cache.lfc_working_set,
            lfc_size: cache.lfc_size,
        })
    }
}

/// A running compute the autoscaler follows: what the reconciler last saw
/// of it and its endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tracked {
    pub namespace: String,
    pub project_id: String,
    pub endpoint_id: String,
    pub compute: ComputeRec,
    pub min_cu: Cu,
    pub max_cu: Cu,
}

/// The autoscaler of one `pg-control` instance: the computes its
/// reconciler runs ([`track`](Self::track)), their samples and the policy.
pub struct Scaling {
    pub metrics: Arc<dyn ComputeMetrics>,
    pub(crate) autoscaler: Mutex<Autoscaler>,
    tracked: Mutex<BTreeMap<String, Tracked>>,
}

impl std::fmt::Debug for Scaling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scaling").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Scaling {
    pub fn new(metrics: Arc<dyn ComputeMetrics>, policy: Policy) -> Self {
        Scaling {
            metrics,
            autoscaler: Mutex::new(Autoscaler::new(policy)),
            tracked: Mutex::new(BTreeMap::new()),
        }
    }

    /// Follows (or refreshes) a running compute.
    pub fn track(&self, t: Tracked) {
        lock(&self.tracked).insert(t.compute.id.clone(), t);
    }

    /// Stops following `compute` (it ended, or another instance runs it).
    pub fn forget(&self, compute: &str) {
        lock(&self.tracked).remove(compute);
        lock(&self.autoscaler).forget(compute);
    }

    /// Caps `namespace`'s total CU (Task 47's limits feed it).
    pub fn set_cap(&self, namespace: &str, cap: Option<Cu>) {
        lock(&self.autoscaler).set_cap(namespace, cap);
    }

    /// The computes followed now.
    pub fn tracked(&self) -> Vec<Tracked> {
        lock(&self.tracked).values().cloned().collect()
    }

    pub(crate) fn decide(&self, now_ms: u64, obs: &[Observation]) -> Vec<Decision> {
        lock(&self.autoscaler).tick(now_ms, obs)
    }

    pub(crate) fn applied(&self, compute: &str, cu: Cu) {
        lock(&self.autoscaler).applied(compute);
        if let Some(t) = lock(&self.tracked).get_mut(compute) {
            t.compute.cu = cu;
        }
    }

    pub(crate) fn unsupported(&self, compute: &str) {
        lock(&self.autoscaler).unsupported(compute);
    }
}
