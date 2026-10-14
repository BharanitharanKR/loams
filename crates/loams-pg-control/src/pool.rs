//! The warm pool (design §46 §7.3; PG2 Task 14): computes started ahead of
//! need, running `compute_ctl` with no spec, so that starting an endpoint
//! binds one instead of waiting for a container or Pod to start.
//!
//! - **Keys.** A pool is kept per [`PoolKey`]: a Loams namespace, a
//!   Postgres major and that major's pinned image. The plan's key is
//!   (region, `pg_version`, image); the namespace joins it because a
//!   Kubernetes compute Pod lives in `loams-pg-<ns>` and only that
//!   namespace's Services can select it (§46 §15.1, R14.1). The region is
//!   the deployment's.
//! - **Which keys.** A namespace's pool is kept while one of its projects
//!   has an endpoint: every pass over a project with endpoints
//!   ([`WarmPool::want`]) keeps its (namespace, major) wanted for
//!   [`PoolConfig::want_ttl`]. Every instance passes over every project at
//!   each resync, so each knows the same keys.
//! - **Unbound computes.** Each has a `C/` record with no binding and its
//!   [`pool`](crate::model::ComputeRec::pool) key, written before the
//!   runtime starts it, so a crash never leaves a compute without a record.
//!   Its spec upcall answers `empty` (Task 8), and `compute_ctl` waits.
//!   `Pending` until `compute_ctl` reports `empty`, then `Running` (warm),
//!   and `Stopping` while it drains.
//! - **The maintainer.** One instance holds the lease [`POOL_LEASE`] and
//!   refills, checks and drains every pool each [`PoolConfig::every`]
//!   ([`WarmPool::tick`]): it starts computes up to
//!   [`PoolConfig::size_per_key`] per wanted key, marks the ones whose
//!   `compute_ctl` waits `Running`, and drains the ones that died, did not
//!   start within the start timeout, are older than
//!   [`PoolConfig::max_age`], or whose key is no longer wanted or whose
//!   image is no longer the major's (an image change drains the pool). A
//!   drain first writes the record `Stopping` (compare-and-set), so a
//!   compute an endpoint took meanwhile is never stopped: one of the two
//!   writes fails.
//! - **Taking** (the endpoint reconciler, under the project's lease): a
//!   warm compute of the endpoint's key and size is bound in the same batch
//!   that names it on the endpoint (compare-and-set on its record, so two
//!   endpoints never take one), then handed its spec through `/configure`,
//!   and the pool is kicked to refill. Every instance keeps the warm
//!   computes it saw at its last tick ([`WarmPool::pop`]); a stale one
//!   fails the compare-and-set, and the endpoint's pass tries again.
//! - **Compose has no pool** (R14.2): compose fixes a container's ports
//!   when it starts, and an unbound compute has no endpoint port to
//!   publish. The size is 0 there, and the compose runtime refuses an
//!   unbound start.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use crate::ids::ComputeId;
use crate::model::{AllComputes, ComputeRec, ComputeStatus, Cu, PoolKey, Record as _};
use crate::reconcile::endpoint::{Computes, CtlStatus};
use crate::reconcile::{ReconcileError, all};
use crate::runtime::{ComputeLaunch, RuntimeError, RuntimeStatus};
use crate::store::{Fence, PgControlStore, StoreError, Versioned};
use crate::upcall::COMPUTE_TOKEN_TTL;

/// The lease of the instance that maintains the pools.
pub const POOL_LEASE: &str = "e/pg/warm-pool";

/// Warm computes per key by default (§46 §7.3).
pub const DEFAULT_POOL_SIZE: u32 = 2;

/// The most computes one tick starts; the rest wait for the next tick,
/// which then comes after [`PoolConfig::poll`].
pub const MAX_STARTS_PER_TICK: usize = 16;

/// How the warm pool runs.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Warm computes per key ([`DEFAULT_POOL_SIZE`]); 0 turns the pool
    /// off.
    pub size_per_key: u32,
    /// Every warm compute's size: 0.25 CU, an endpoint's default
    /// `min_cu` (R11.2). Only an endpoint starting at this size takes one.
    pub cu: Cu,
    /// How often the maintainer ticks (default 10 s), and every instance
    /// reads the pool.
    pub every: Duration,
    /// How soon the next tick comes while computes start (default 1 s).
    pub poll: Duration,
    /// A warm compute older than this is drained and replaced (default
    /// 24 h), so a taken one starts with a fresh image and token.
    pub max_age: Duration,
    /// How long a (namespace, major) stays wanted after a pass saw it
    /// (default 10 minutes). An instance drains nothing for want of
    /// demand before it has been up this long.
    pub want_ttl: Duration,
    /// How long a warm compute has to report `empty` (default 60 s, the
    /// endpoint's start timeout).
    pub start_timeout: Duration,
    /// How long the reconciler waits on `/configure` when it binds one
    /// (default 2 s): `compute_ctl` answers only once Postgres runs, but it
    /// takes the spec at once, so a later answer is not waited for.
    pub push_wait: Duration,
}

impl PoolConfig {
    /// The defaults, with `size_per_key` warm computes per key.
    pub fn new(size_per_key: u32) -> Self {
        PoolConfig {
            size_per_key,
            cu: Cu(1),
            every: Duration::from_secs(10),
            poll: Duration::from_secs(1),
            max_age: Duration::from_secs(24 * 3600),
            want_ttl: Duration::from_secs(600),
            start_timeout: crate::reconcile::endpoint::START_TIMEOUT,
            push_wait: Duration::from_secs(2),
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new(DEFAULT_POOL_SIZE)
    }
}

/// What one [`WarmPool::tick`] came to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolTick {
    /// This instance held [`POOL_LEASE`] and maintained the pools.
    pub maintained: bool,
    /// Computes started.
    pub started: usize,
    /// Computes drained (stopped and removed).
    pub drained: usize,
    /// Warm computes after the tick.
    pub warm: usize,
    /// Computes still starting after the tick: the next tick comes after
    /// [`PoolConfig::poll`].
    pub pending: usize,
}

/// The warm pools (see the module docs). One per `pg-control` instance,
/// shared by its reconciler's passes and its maintainer loop.
#[derive(Debug)]
pub struct WarmPool {
    config: PoolConfig,
    /// When this instance first ticked (ms).
    started_ms: Mutex<Option<u64>>,
    /// (namespace, major) → when a pass last saw an endpoint there (ms).
    wanted: Mutex<HashMap<(String, u32), u64>>,
    /// Warm computes as the last tick read them, by key.
    warm: Mutex<BTreeMap<PoolKey, Vec<String>>>,
    kick: tokio::sync::Notify,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // Every critical section is a single insert, remove or swap.
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Why a compute is drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drain {
    /// A drain already began.
    Draining,
    Unwanted,
    ImageChanged,
    Old,
    /// Its upcall token was signed with another key.
    OldKey,
    Dead,
    StartTimeout,
}

impl Drain {
    fn as_str(self) -> &'static str {
        match self {
            Drain::Draining => "draining",
            Drain::Unwanted => "no endpoint in its namespace wants it",
            Drain::ImageChanged => "its image is no longer the major's",
            Drain::Old => "older than the pool's max age",
            Drain::OldKey => "its upcall token is signed with an older key",
            Drain::Dead => "its container or compute_ctl stopped",
            Drain::StartTimeout => "it did not start in time",
        }
    }
}

impl WarmPool {
    pub fn new(config: PoolConfig) -> Self {
        WarmPool {
            config,
            started_ms: Mutex::new(None),
            wanted: Mutex::new(HashMap::new()),
            warm: Mutex::new(BTreeMap::new()),
            kick: tokio::sync::Notify::new(),
        }
    }

    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Whether the pool is on.
    pub fn enabled(&self) -> bool {
        self.config.size_per_key > 0
    }

    /// A pass saw a project with endpoints in `namespace`, on Postgres
    /// `pg_version`: its pool is wanted.
    pub fn want(&self, namespace: &str, pg_version: u32, now_ms: u64) {
        if self.enabled() {
            lock(&self.wanted).insert((namespace.to_owned(), pg_version), now_ms);
        }
    }

    /// Asks the maintainer loop for a tick now (a compute was taken).
    pub fn kick(&self) {
        self.kick.notify_one();
    }

    /// Resolves at the next [`kick`](Self::kick).
    pub async fn kicked(&self) {
        self.kick.notified().await;
    }

    /// A warm compute of `key` this instance saw at its last tick, removed
    /// from its list: the caller checks the record before binding it.
    pub fn pop(&self, key: &PoolKey) -> Option<String> {
        let mut warm = lock(&self.warm);
        let ids = warm.get_mut(key)?;
        let id = ids.pop();
        if ids.is_empty() {
            warm.remove(key);
        }
        id
    }

    /// The warm computes this instance saw at its last tick, by key.
    pub fn warm(&self) -> BTreeMap<PoolKey, Vec<String>> {
        lock(&self.warm).clone()
    }

    /// The wanted keys: the fresh (namespace, major) pairs, each with its
    /// major's image.
    fn wanted_keys(&self, images: &BTreeMap<u32, String>, now_ms: u64) -> BTreeSet<PoolKey> {
        if !self.enabled() {
            return BTreeSet::new();
        }
        let ttl = ms(self.config.want_ttl);
        let mut wanted = lock(&self.wanted);
        wanted.retain(|_, seen| now_ms.saturating_sub(*seen) < ttl);
        wanted
            .keys()
            .filter_map(|(ns, major)| {
                images.get(major).map(|image| PoolKey {
                    namespace: ns.clone(),
                    pg_version: *major,
                    image: image.clone(),
                })
            })
            .collect()
    }

    /// One tick: reads every unbound compute, remembers the warm ones, and
    /// when this instance holds (or takes) [`POOL_LEASE`] maintains the
    /// pools (see the module docs).
    ///
    /// # Errors
    ///
    /// [`ReconcileError`] when the store could not be read or the lease
    /// moved on; the next tick starts again.
    pub async fn tick<S: PgControlStore>(
        &self,
        store: &S,
        computes: &Computes,
        holder: &str,
        lease_ttl: Duration,
        now_ms: u64,
    ) -> Result<PoolTick, ReconcileError> {
        let first = *lock(&self.started_ms).get_or_insert(now_ms);
        let pooled: Vec<Versioned<ComputeRec>> = all::<S, ComputeRec>(store, &AllComputes)
            .await?
            .into_iter()
            .filter(|c| c.record.binding.is_none() && c.record.pool.is_some())
            .collect();
        let wanted = self.wanted_keys(&computes.images, now_ms);
        let mut out = PoolTick::default();
        if pooled.is_empty() && wanted.is_empty() {
            self.remember([]);
            return Ok(out);
        }
        let fence = match store.acquire_lease(POOL_LEASE, holder, lease_ttl).await {
            Ok(fence) => fence,
            Err(StoreError::Held { .. }) => {
                let warm = pooled
                    .iter()
                    .filter(|c| is_warm(&c.record))
                    .filter_map(|c| Some((c.record.pool.clone()?, c.record.id.clone())));
                out.warm = self.remember(warm);
                return Ok(out);
            }
            Err(e) => return Err(e.into()),
        };
        out.maintained = true;
        let m = Maintainer {
            pool: self,
            store,
            computes,
            fence,
            lease_ttl,
            now_ms,
            settled: now_ms.saturating_sub(first) >= ms(self.config.want_ttl),
        };
        let mut kept: BTreeMap<PoolKey, usize> = BTreeMap::new();
        let mut warm = Vec::new();
        for c in pooled {
            let Some(key) = c.record.pool.clone() else {
                continue;
            };
            let id = c.record.id.clone();
            match m.check(c, &wanted).await {
                Err(e) => {
                    self.remember(warm);
                    return Err(e);
                }
                Ok(Kept::Warm) => {
                    *kept.entry(key.clone()).or_default() += 1;
                    warm.push((key, id));
                }
                Ok(Kept::Pending) => {
                    out.pending += 1;
                    *kept.entry(key).or_default() += 1;
                }
                Ok(Kept::Drained) => out.drained += 1,
                Ok(Kept::Gone) => {}
            }
        }
        out.warm = self.remember(warm);
        let size = self.config.size_per_key as usize;
        'keys: for key in &wanted {
            let have = kept.get(key).copied().unwrap_or(0);
            for _ in have..size {
                if out.started == MAX_STARTS_PER_TICK {
                    out.pending += 1;
                    break 'keys;
                }
                if m.start(key).await? {
                    out.started += 1;
                    out.pending += 1;
                } else {
                    // The runtime refused this key: the next tick tries again.
                    continue 'keys;
                }
            }
        }
        Ok(out)
    }

    /// Keeps `warm` (key, compute id) as the computes a start may take;
    /// answers how many.
    fn remember(&self, warm: impl IntoIterator<Item = (PoolKey, String)>) -> usize {
        let mut by_key: BTreeMap<PoolKey, Vec<String>> = BTreeMap::new();
        let mut n = 0;
        for (key, id) in warm {
            by_key.entry(key).or_default().push(id);
            n += 1;
        }
        *lock(&self.warm) = by_key;
        n
    }
}

/// Whether an unbound compute can be taken: `compute_ctl` waits for its
/// spec.
fn is_warm(c: &ComputeRec) -> bool {
    c.binding.is_none() && c.status == ComputeStatus::Running && c.ctl_url.is_some()
}

/// What [`Maintainer::check`] did with one compute.
enum Kept {
    Warm,
    Pending,
    Drained,
    /// Taken by an endpoint, or removed, meanwhile.
    Gone,
}

/// The maintaining instance's tick, under [`POOL_LEASE`]'s fence.
struct Maintainer<'a, S> {
    pool: &'a WarmPool,
    store: &'a S,
    computes: &'a Computes,
    fence: Fence,
    lease_ttl: Duration,
    now_ms: u64,
    /// Up long enough to drain for want of demand.
    settled: bool,
}

/// A write that lost to an endpoint's take (the record now names a
/// project, which the pool's fence does not cover) or to another write.
fn lost(e: &StoreError) -> bool {
    matches!(
        e,
        StoreError::Conflict { .. } | StoreError::NotFound | StoreError::InvalidArgument(_)
    )
}

impl<S: PgControlStore> Maintainer<'_, S> {
    /// Renews the lease before a step; a lost lease ends the tick.
    async fn renew(&self) -> Result<(), ReconcileError> {
        match self.store.renew_lease(&self.fence, self.lease_ttl).await {
            Ok(_) => Ok(()),
            Err(StoreError::LeaseLost) => Err(ReconcileError::Fenced),
            Err(e) => Err(ReconcileError::Store(e)),
        }
    }

    fn ns(c: &ComputeRec) -> &str {
        c.pool.as_ref().map_or("", |k| k.namespace.as_str())
    }

    /// Checks one unbound compute: warm, still starting, or drained.
    async fn check(
        &self,
        c: Versioned<ComputeRec>,
        wanted: &BTreeSet<PoolKey>,
    ) -> Result<Kept, ReconcileError> {
        let Some(key) = &c.record.pool else {
            return Ok(Kept::Gone);
        };
        let image_now = self.computes.images.get(&key.pg_version);
        let age = self.now_ms.saturating_sub(c.record.created_at_ms);
        let drain = if c.record.status == ComputeStatus::Stopping {
            Some(Drain::Draining)
        } else if image_now != Some(&key.image) {
            Some(Drain::ImageChanged)
        } else if !wanted.contains(key) && (self.settled || !self.pool.enabled()) {
            Some(Drain::Unwanted)
        } else if age >= ms(self.pool.config.max_age) {
            Some(Drain::Old)
        } else if c.record.jwt_key_id.as_deref() != Some(self.computes.keys.kid()) {
            Some(Drain::OldKey)
        } else {
            None
        };
        if let Some(why) = drain {
            return self.drain(c, why).await;
        }
        let id = parse_id(&c.record)?;
        if c.record.ctl_url.is_none() {
            // Recorded, but the runtime's start did not finish: again
            // (idempotent by id) until the start timeout.
            if age >= ms(self.pool.config.start_timeout) {
                return self.drain(c, Drain::StartTimeout).await;
            }
            return self.launch(c, id).await;
        }
        self.renew().await?;
        match self.computes.runtime.status(Self::ns(&c.record), &id).await {
            Ok(RuntimeStatus::Exited { .. } | RuntimeStatus::Missing) => {
                return self.drain(c, Drain::Dead).await;
            }
            Ok(RuntimeStatus::Pending | RuntimeStatus::Running) => {}
            Err(e) => {
                tracing::info!(compute = %c.record.id, error = %e, "the pool could not read a compute's status");
                return Ok(if is_warm(&c.record) {
                    Kept::Warm
                } else {
                    Kept::Pending
                });
            }
        }
        if c.record.status == ComputeStatus::Running {
            return Ok(Kept::Warm);
        }
        match self.computes.ctl.status(&c.record).await.map(|r| r.status) {
            Ok(CtlStatus::Empty) => {
                let mut warm = c.record.clone();
                warm.status = ComputeStatus::Running;
                self.renew().await?;
                match self.store.put(&warm, Some(c.version), &self.fence).await {
                    Ok(_) => Ok(Kept::Warm),
                    Err(e) if lost(&e) => Ok(Kept::Gone),
                    Err(e) => Err(e.into()),
                }
            }
            Ok(CtlStatus::Failed(_) | CtlStatus::Terminated) => self.drain(c, Drain::Dead).await,
            Ok(CtlStatus::Starting | CtlStatus::Running) | Err(_) => {
                if age >= ms(self.pool.config.start_timeout) {
                    self.drain(c, Drain::StartTimeout).await
                } else {
                    Ok(Kept::Pending)
                }
            }
        }
    }

    /// `Stopping` (compare-and-set: an endpoint that took the compute
    /// meanwhile wins), then the runtime's stop, then the record removed.
    async fn drain(&self, c: Versioned<ComputeRec>, why: Drain) -> Result<Kept, ReconcileError> {
        let mut c = c;
        if c.record.status != ComputeStatus::Stopping {
            let mut stopping = c.record.clone();
            stopping.status = ComputeStatus::Stopping;
            self.renew().await?;
            match self
                .store
                .put(&stopping, Some(c.version), &self.fence)
                .await
            {
                Ok(version) => {
                    c = Versioned {
                        record: stopping,
                        version,
                    };
                }
                Err(e) if lost(&e) => return Ok(Kept::Gone),
                Err(e) => return Err(e.into()),
            }
        }
        let id = parse_id(&c.record)?;
        self.renew().await?;
        self.computes
            .runtime
            .stop(Self::ns(&c.record), &id)
            .await
            .map_err(|e| ReconcileError::Runtime(format!("stopping a warm compute: {e}")))?;
        match self
            .store
            .delete::<ComputeRec>(&c.record.key(), c.version, &self.fence)
            .await
        {
            Ok(()) => {}
            Err(e) if lost(&e) => return Ok(Kept::Gone),
            Err(e) => return Err(e.into()),
        }
        tracing::info!(compute = %c.record.id, why = why.as_str(), "warm compute drained");
        Ok(Kept::Drained)
    }

    /// Records a new warm compute of `key`, then starts it. `false` when
    /// the runtime refused it (logged; its record removed).
    async fn start(&self, key: &PoolKey) -> Result<bool, ReconcileError> {
        let id = ComputeId::new();
        let rec = ComputeRec {
            id: id.to_string(),
            binding: None,
            pool: Some(key.clone()),
            bound_at_ms: None,
            spec_version: 0,
            status: ComputeStatus::Pending,
            jwt_key_id: Some(self.computes.keys.kid().to_owned()),
            ctl_url: None,
            cu: self.pool.config.cu,
            created_at_ms: self.now_ms,
            last_active_ms: None,
        };
        self.renew().await?;
        let version = self.store.put(&rec, None, &self.fence).await?;
        let c = Versioned {
            record: rec,
            version,
        };
        Ok(!matches!(self.launch(c, id).await?, Kept::Drained))
    }

    /// Starts a recorded warm compute and records `compute_ctl`'s address.
    async fn launch(
        &self,
        c: Versioned<ComputeRec>,
        id: ComputeId,
    ) -> Result<Kept, ReconcileError> {
        let Some(key) = c.record.pool.clone() else {
            return Ok(Kept::Gone);
        };
        let token = self
            .computes
            .keys
            .issue_upcall_token(&id, c.record.created_at_ms, COMPUTE_TOKEN_TTL)
            .map_err(|e| ReconcileError::Runtime(format!("signing the upcall token: {e}")))?;
        let launch = ComputeLaunch {
            compute_id: id,
            namespace: key.namespace.clone(),
            binding: None,
            address: None,
            image: key.image.clone(),
            cu: c.record.cu,
            upcall_token: token,
        };
        self.renew().await?;
        let handle = match self.computes.runtime.start(&launch).await {
            Ok(h) => h,
            Err(RuntimeError::InvalidArgument(why)) => {
                tracing::error!(compute = %id, namespace = %key.namespace, why, "the runtime refused a warm compute");
                return self.drain(c, Drain::Dead).await;
            }
            Err(e) => {
                tracing::warn!(compute = %id, error = %e, "a warm compute did not start; retrying");
                return Ok(Kept::Pending);
            }
        };
        let mut started = c.record.clone();
        started.ctl_url = Some(handle.ctl_url);
        self.renew().await?;
        match self.store.put(&started, Some(c.version), &self.fence).await {
            Ok(_) => {}
            Err(e) if lost(&e) => return Ok(Kept::Gone),
            Err(e) => return Err(e.into()),
        }
        tracing::info!(compute = %id, namespace = %key.namespace, pg_version = key.pg_version, "warm compute started");
        Ok(Kept::Pending)
    }
}

fn parse_id(c: &ComputeRec) -> Result<ComputeId, ReconcileError> {
    c.id.parse()
        .map_err(|_| ReconcileError::Runtime(format!("compute id {}", c.id)))
}
