//! The repository sequencers of one node: placement, ownership and
//! forwarding (§36 §4.4; plan GT1 Tasks 13 and 14, Rulings 26–28).
//!
//! - **Placement.** A repository's sequencer belongs on one node, the first
//!   of its rendezvous ranking over the `gateway` nodes ([`SeqPlacement`];
//!   the server ranks `PlacementKey::repo(ns, repo_placement_hash(repo))`).
//! - **Ownership.** The owner takes the lease `task/git-seq/<ns>/<repo_id>`
//!   ([`SeqLeases`]) before it opens the sequencer, renews it every
//!   `renew_every`, and closes the sequencer when the lease is lost (calls
//!   still holding it are answered `Unavailable` and retry).
//! - **Forwarding.** Any other node posts the commit to the owner
//!   ([`SeqForwarder`], [`crate::forward`]). When the owner cannot be
//!   reached within `forward_timeout` (`FORWARD_TIMEOUT`, 2 s), the node
//!   marks it unreachable and commits through its own, unleased sequencer
//!   (Ruling 27), counted in [`OwnerStats::fallbacks`].
//! - **Idle eviction.** A sequencer unused for `idle_evict`
//!   (`SEQ_IDLE_EVICT`, 10 min) is closed and its lease released (Ruling
//!   28); the next commit loads it again.
//!
//! None of this is needed for correctness (§36 §4.2): a second sequencer, a
//! zombie owner or a serverless helper writing the bucket is fenced by the
//! create-only segment PUT. Placement and the lease only keep one repository's
//! commits in one group-commit queue.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loams_store::Store;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;

use crate::blob::StoreBlobStore;
use crate::clock::Clock;
use crate::format::pb;
use crate::forward::{
    SeqCommit, SeqForwarder, decode_answer, decode_request, encode_answer, encode_request,
};
use crate::ids::{IdemKey, NamespaceId, RepoId, Seq};
use crate::limits::{FORWARD_TIMEOUT, SEQ_IDLE_EVICT};
use crate::mirror_events::{GitEventSink, MirrorCursor, run_mirror};
use crate::paths::RepoPaths;
use crate::reflog::{CommittedTxn, Receipt, RefError, RefLog, RefTxn};
use crate::sequencer::{BucketRefLog, SequencerConfig};
use crate::wal::StoreWalStore;

/// Where a repository's sequencer runs, as this node sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeqOwner {
    /// On this node.
    Here,
    /// On another node.
    There {
        /// Its id.
        node_id: u64,
        /// Where its internal routes listen (`host:port`).
        addr: String,
    },
}

/// Which node runs a repository's sequencer (Ruling 26).
pub trait SeqPlacement: Send + Sync + fmt::Debug {
    /// The owner of `repo` in `ns`.
    fn owner(&self, ns: NamespaceId, repo: &RepoId) -> SeqOwner;
    /// A forward to `node_id` failed: skip it for a while.
    fn mark_unreachable(&self, node_id: u64) {
        let _ = node_id;
    }
}

/// Every repository's sequencer runs here: one node, `loams dev`, tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysHere;

impl SeqPlacement for AlwaysHere {
    fn owner(&self, _: NamespaceId, _: &RepoId) -> SeqOwner {
        SeqOwner::Here
    }
}

/// The placement id of `repo`: the first 8 bytes, big-endian, of
/// `sha256(repo_id)` (T0-13), for `PlacementKey::repo`.
pub fn repo_placement_hash(repo: &RepoId) -> u64 {
    let digest = Sha256::digest(repo.as_str().as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(first)
}

/// The owner lease of `repo`'s sequencer: `task/git-seq/<ns>/<repo_id>`
/// (§36 §4.4; `loams-worker`'s `task/<key>` form).
pub fn seq_lease_key(ns: NamespaceId, repo: &RepoId) -> String {
    format!("task/git-seq/{ns}/{repo}")
}

/// Why a lease call failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Another owner holds it.
    #[error("the lease is held by {holder}")]
    Held {
        /// Who holds it.
        holder: String,
    },
    /// This node no longer holds it at its epoch (it expired or was taken).
    #[error("the lease was lost")]
    Lost,
    /// The lease store could not answer.
    #[error("the lease store is unavailable: {0}")]
    Unavailable(String),
}

/// The metastore's leases, as the sequencers use them (the server
/// implements this over `MetaStore`'s `acquire_lease`, `renew_lease` and
/// `release_lease`, with an owner string unique to this process).
#[async_trait]
pub trait SeqLeases: Send + Sync + fmt::Debug {
    /// Takes `key` for `ttl`; returns its epoch.
    async fn acquire(&self, key: &str, ttl: Duration) -> Result<u64, LeaseError>;
    /// Extends `key`, held at `epoch`, by `ttl` from now.
    async fn renew(&self, key: &str, epoch: u64, ttl: Duration) -> Result<(), LeaseError>;
    /// Gives `key` up (best effort).
    async fn release(&self, key: &str, epoch: u64);
}

/// How sequencers are owned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerConfig {
    /// How long the owner lease lasts without a renewal (10 s).
    pub lease_ttl: Duration,
    /// How often a held lease is renewed (3 s).
    pub renew_every: Duration,
    /// How long a forward may take before this node falls back to its own
    /// sequencer (`FORWARD_TIMEOUT`, 2 s).
    pub forward_timeout: Duration,
    /// How long an unused sequencer stays open (`SEQ_IDLE_EVICT`, 10 min).
    pub idle_evict: Duration,
}

impl Default for OwnerConfig {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(10),
            renew_every: Duration::from_secs(3),
            forward_timeout: FORWARD_TIMEOUT,
            idle_evict: SEQ_IDLE_EVICT,
        }
    }
}

/// The cluster seams of [`RepoSequencers::clustered`].
#[derive(Clone, Debug)]
pub struct Cluster {
    /// Which node owns a repository.
    pub placement: Arc<dyn SeqPlacement>,
    /// The owner leases.
    pub leases: Arc<dyn SeqLeases>,
    /// How commits reach the owner.
    pub forwarder: Arc<dyn SeqForwarder>,
    /// The timings.
    pub config: OwnerConfig,
}

/// Counters for metrics (Task 74 exports them).
#[derive(Debug, Default)]
pub struct OwnerStats {
    /// Commits this node forwarded to an owner.
    pub forwarded_out: AtomicU64,
    /// Forwarded commits this node served.
    pub forwarded_in: AtomicU64,
    /// Commits made through this node's own sequencer because the owner
    /// could not be reached (`loams_git_owner_fallbacks_total`).
    pub fallbacks: AtomicU64,
    /// Sequencers closed after `idle_evict` unused.
    pub evictions: AtomicU64,
    /// Owner leases lost while held.
    pub leases_lost: AtomicU64,
}

/// A copy of [`OwnerStats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OwnerStatsSnapshot {
    /// See [`OwnerStats::forwarded_out`].
    pub forwarded_out: u64,
    /// See [`OwnerStats::forwarded_in`].
    pub forwarded_in: u64,
    /// See [`OwnerStats::fallbacks`].
    pub fallbacks: u64,
    /// See [`OwnerStats::evictions`].
    pub evictions: u64,
    /// See [`OwnerStats::leases_lost`].
    pub leases_lost: u64,
}

/// A held owner lease and its renewal task.
#[derive(Debug)]
struct Held {
    epoch: u64,
    renew: JoinHandle<()>,
}

/// One repository's sequencer on this node.
#[derive(Debug, Default)]
struct Entry {
    log: OnceCell<BucketRefLog>,
    lease: Mutex<Option<Held>>,
    /// The `_git` mirror of this repository, while this node commits it (Task 15).
    mirror: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Calls using the sequencer now: an entry in use is never evicted.
    busy: AtomicUsize,
    /// When it was last used, in ms since [`Inner::epoch`].
    last_used_ms: AtomicU64,
}

/// Marks an entry in use for as long as it lives.
struct InUse<'a>(&'a Entry);

impl Drop for InUse<'_> {
    fn drop(&mut self) {
        self.0.busy.fetch_sub(1, Ordering::SeqCst);
    }
}

type Key = (NamespaceId, RepoId);

struct Inner {
    store: Store,
    clock: Arc<dyn Clock>,
    placement: Arc<dyn SeqPlacement>,
    leases: Option<Arc<dyn SeqLeases>>,
    forwarder: Option<Arc<dyn SeqForwarder>>,
    /// Where committed transactions are mirrored (`_git`, Task 15).
    mirror: Option<Arc<dyn GitEventSink>>,
    config: OwnerConfig,
    /// One entry per repository: the map's lock is held only to find or add
    /// one, so a slow cold open of one repository never blocks another.
    entries: Mutex<HashMap<Key, Arc<Entry>>>,
    stats: OwnerStats,
    /// Test hook: renewals are skipped while set (a paused, zombie owner).
    renewals_paused: AtomicBool,
    started: Instant,
    sweeper: OnceLock<JoinHandle<()>>,
}

/// The repository sequencers of this node.
#[derive(Clone)]
pub struct RepoSequencers {
    inner: Arc<Inner>,
}

impl fmt::Debug for RepoSequencers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepoSequencers").finish_non_exhaustive()
    }
}

impl RepoSequencers {
    /// One node's sequencers: every repository's sequencer runs here, with
    /// no lease and no forwarding (`loams dev`, a single server, tests).
    pub fn new(store: Store, clock: Arc<dyn Clock>) -> Self {
        Self::build(
            store,
            clock,
            Arc::new(AlwaysHere),
            None,
            None,
            OwnerConfig::default(),
        )
    }

    /// A cluster node's sequencers (Task 14).
    pub fn clustered(store: Store, clock: Arc<dyn Clock>, cluster: Cluster) -> Self {
        Self::build(
            store,
            clock,
            cluster.placement,
            Some(cluster.leases),
            Some(cluster.forwarder),
            cluster.config,
        )
    }

    /// Mirrors every transaction this node commits into its namespace's `_git` stream through
    /// `sink` (§36 §4.8, Task 15). Call before the first commit.
    pub fn with_mirror(mut self, sink: Arc<dyn GitEventSink>) -> Self {
        match Arc::get_mut(&mut self.inner) {
            Some(inner) => inner.mirror = Some(sink),
            None => tracing::warn!("with_mirror after the sequencers were shared: ignored"),
        }
        self
    }

    /// The committed transactions of `repo` from `from` on, in order, without gaps, at least
    /// once (`GitEvents::subscribe`, Task 15): an in-process consumer that stops resumes from
    /// the seq after the last one it handled.
    pub async fn subscribe(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
        from: Seq,
    ) -> Result<BoxStream<'static, Result<CommittedTxn, RefError>>, RefError> {
        Ok(self.get(ns, tenant, repo).await?.watch(from))
    }

    fn build(
        store: Store,
        clock: Arc<dyn Clock>,
        placement: Arc<dyn SeqPlacement>,
        leases: Option<Arc<dyn SeqLeases>>,
        forwarder: Option<Arc<dyn SeqForwarder>>,
        config: OwnerConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                clock,
                placement,
                leases,
                forwarder,
                mirror: None,
                config,
                entries: Mutex::new(HashMap::new()),
                stats: OwnerStats::default(),
                renewals_paused: AtomicBool::new(false),
                started: Instant::now(),
                sweeper: OnceLock::new(),
            }),
        }
    }

    /// The ref log of `repo` in `ns`, opened here (with `tenant`, `<org>/<ns>`,
    /// as its events' `tenantid`) if this node has none, for **reads**
    /// (`snapshot`, `watch`): reads never wait for the owner (§36 §4.6).
    /// Commits go through [`RepoSequencers::commit`] and
    /// [`RepoSequencers::commit_config`], which find the owner. `NotFound`
    /// when the repository has no checkpoint.
    pub async fn get(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
    ) -> Result<BucketRefLog, RefError> {
        let entry = self.entry(ns, repo).await;
        let _use = self.use_entry(&entry);
        self.open(&entry, ns, tenant, repo).await.cloned()
    }

    /// Commits `txn` on `repo`'s owner: here, forwarded, or (owner
    /// unreachable) here without the lease.
    pub async fn commit(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
        txn: RefTxn,
    ) -> Result<Receipt, RefError> {
        self.route(ns, tenant, repo, SeqCommit::Ref(txn)).await
    }

    /// Commits a configuration change on `repo`'s owner (as
    /// [`RepoSequencers::commit`]).
    pub async fn commit_config(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
        key: IdemKey,
        change: pb::ConfigChange,
    ) -> Result<Receipt, RefError> {
        self.route(ns, tenant, repo, SeqCommit::Config { key, change })
            .await
    }

    /// Serves a forwarded commit (the body of `POST
    /// /internal/v1/git/commit`) here, whatever this node's placement says,
    /// so a commit is never forwarded twice; returns the answer body.
    pub async fn serve_forwarded(&self, body: &[u8]) -> Bytes {
        self.inner
            .stats
            .forwarded_in
            .fetch_add(1, Ordering::Relaxed);
        let answer = match decode_request(body) {
            Ok((ns, tenant, repo, commit)) => {
                self.commit_here(ns, &tenant, &repo, commit, true).await
            }
            Err(err) => Err(RefError::Invalid {
                reason: err.to_string(),
            }),
        };
        encode_answer(&answer)
    }

    async fn route(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
        commit: SeqCommit,
    ) -> Result<Receipt, RefError> {
        let (node_id, addr) = match self.inner.placement.owner(ns, repo) {
            SeqOwner::Here => return self.commit_here(ns, tenant, repo, commit, true).await,
            SeqOwner::There { node_id, addr } => (node_id, addr),
        };
        let Some(forwarder) = &self.inner.forwarder else {
            return self.commit_here(ns, tenant, repo, commit, false).await;
        };
        let body = encode_request(ns, tenant, repo, &commit);
        let forwarded = tokio::time::timeout(
            self.inner.config.forward_timeout,
            forwarder.forward(&addr, body),
        )
        .await;
        let failure = match forwarded {
            Ok(Ok(answer)) => match decode_answer(&answer) {
                Ok(answer) => {
                    self.inner
                        .stats
                        .forwarded_out
                        .fetch_add(1, Ordering::Relaxed);
                    return answer;
                }
                Err(err) => err.to_string(),
            },
            Ok(Err(err)) => err,
            Err(_) => format!("no answer within {:?}", self.inner.config.forward_timeout),
        };
        // The owner may still commit the forwarded call: the fallback uses
        // the same idempotency key, so the second commit replays the first.
        self.inner.placement.mark_unreachable(node_id);
        self.inner.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            %ns, %repo, node_id, %addr, error = %failure,
            "forwarding a commit to the repository's owner failed; committing here (fenced)"
        );
        self.commit_here(ns, tenant, repo, commit, false).await
    }

    /// Commits through this node's sequencer, taking the owner lease first
    /// when `lease` is set and leases are configured. A lease held by
    /// another node (a zombie, or a placement change in flight) does not
    /// stop the commit: fencing keeps it safe.
    async fn commit_here(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
        commit: SeqCommit,
        lease: bool,
    ) -> Result<Receipt, RefError> {
        let entry = self.entry(ns, repo).await;
        let _use = self.use_entry(&entry);
        let log = self.open(&entry, ns, tenant, repo).await?.clone();
        if lease {
            self.ensure_lease(&entry, ns, repo).await;
        }
        self.ensure_mirror(&entry, &log, ns, repo);
        match commit {
            SeqCommit::Ref(txn) => log.commit(txn).await,
            SeqCommit::Config { key, change } => log.commit_config(key, change).await,
        }
    }

    async fn entry(&self, ns: NamespaceId, repo: &RepoId) -> Arc<Entry> {
        self.start_sweeper();
        Arc::clone(
            self.inner
                .entries
                .lock()
                .await
                .entry((ns, repo.clone()))
                .or_default(),
        )
    }

    fn use_entry<'a>(&self, entry: &'a Entry) -> InUse<'a> {
        entry.busy.fetch_add(1, Ordering::SeqCst);
        entry
            .last_used_ms
            .store(self.inner.now_ms(), Ordering::SeqCst);
        InUse(entry)
    }

    async fn open<'a>(
        &self,
        entry: &'a Entry,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
    ) -> Result<&'a BucketRefLog, RefError> {
        // A failed open leaves the cell empty: the next call tries again.
        entry
            .log
            .get_or_try_init(|| async {
                let paths = RepoPaths::new(ns, repo);
                let wal = StoreWalStore::new(self.inner.store.clone(), paths.clone())
                    .with_clock(self.inner.clock.clone());
                BucketRefLog::open(
                    Arc::new(wal),
                    Arc::new(StoreBlobStore::new(self.inner.store.clone(), paths)),
                    SequencerConfig::new(tenant, ns, repo.clone()),
                    self.inner.clock.clone(),
                )
                .await
            })
            .await
    }

    /// Takes the owner lease if this node does not hold it, and starts its
    /// renewals. Failing to take it only logs: the commit is fenced anyway.
    async fn ensure_lease(&self, entry: &Entry, ns: NamespaceId, repo: &RepoId) {
        let Some(leases) = &self.inner.leases else {
            return;
        };
        let mut held = entry.lease.lock().await;
        if held.is_some() {
            return;
        }
        let key = seq_lease_key(ns, repo);
        match leases.acquire(&key, self.inner.config.lease_ttl).await {
            Ok(epoch) => {
                let renew = tokio::spawn(renew_lease(
                    Arc::downgrade(&self.inner),
                    ns,
                    repo.clone(),
                    key,
                    epoch,
                ));
                *held = Some(Held { epoch, renew });
            }
            Err(err) => {
                tracing::debug!(%ns, %repo, error = %err, "committing without the sequencer lease");
            }
        }
    }

    /// Starts the `_git` mirror of a repository this node commits, if it has none.
    fn ensure_mirror(&self, entry: &Entry, log: &BucketRefLog, ns: NamespaceId, repo: &RepoId) {
        let Some(sink) = &self.inner.mirror else {
            return;
        };
        let mut mirror = entry
            .mirror
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if mirror.is_none() {
            *mirror = Some(tokio::spawn(run_mirror(
                Arc::new(log.clone()),
                MirrorCursor::new(self.inner.store.clone(), ns, repo),
                Arc::clone(sink),
                ns,
                repo.clone(),
            )));
        }
    }

    fn start_sweeper(&self) {
        self.inner.sweeper.get_or_init(|| {
            tokio::spawn(sweep(
                Arc::downgrade(&self.inner),
                self.inner.config.idle_evict,
            ))
        });
    }

    /// Stops `repo`'s sequencer, if this node runs one (the repository was
    /// deleted). Calls that still hold its handle are answered `Unavailable`.
    pub async fn close(&self, ns: NamespaceId, repo: &RepoId) {
        let entry = self.inner.entries.lock().await.remove(&(ns, repo.clone()));
        if let Some(entry) = entry {
            self.inner.retire(ns, repo, &entry).await;
        }
    }

    /// Stops every sequencer and releases every lease (server shutdown).
    pub async fn shutdown(&self) {
        if let Some(sweeper) = self.inner.sweeper.get() {
            sweeper.abort();
        }
        let entries: Vec<(Key, Arc<Entry>)> = self.inner.entries.lock().await.drain().collect();
        for ((ns, repo), entry) in entries {
            self.inner.retire(ns, &repo, &entry).await;
        }
    }

    /// How many sequencers this node runs.
    pub async fn len(&self) -> usize {
        self.inner
            .entries
            .lock()
            .await
            .values()
            .filter(|entry| entry.log.initialized())
            .count()
    }

    /// Whether this node runs no sequencer.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// The repositories whose owner lease this node holds, with the epochs.
    pub async fn leased(&self) -> Vec<(NamespaceId, RepoId, u64)> {
        let entries: Vec<(Key, Arc<Entry>)> = self
            .inner
            .entries
            .lock()
            .await
            .iter()
            .map(|(key, entry)| (key.clone(), Arc::clone(entry)))
            .collect();
        let mut out = Vec::new();
        for ((ns, repo), entry) in entries {
            if let Some(held) = entry.lease.lock().await.as_ref() {
                out.push((ns, repo, held.epoch));
            }
        }
        out.sort();
        out
    }

    /// The counters.
    pub fn stats(&self) -> OwnerStatsSnapshot {
        let s = &self.inner.stats;
        OwnerStatsSnapshot {
            forwarded_out: s.forwarded_out.load(Ordering::Relaxed),
            forwarded_in: s.forwarded_in.load(Ordering::Relaxed),
            fallbacks: s.fallbacks.load(Ordering::Relaxed),
            evictions: s.evictions.load(Ordering::Relaxed),
            leases_lost: s.leases_lost.load(Ordering::Relaxed),
        }
    }

    /// Test hook: stop (or resume) renewing held leases, as a paused or
    /// partitioned owner would.
    #[doc(hidden)]
    pub fn pause_renewals(&self, paused: bool) {
        self.inner.renewals_paused.store(paused, Ordering::SeqCst);
    }
}

impl Inner {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Closes a removed entry: stops its renewals, releases its lease, and
    /// shuts its sequencer down.
    async fn retire(&self, ns: NamespaceId, repo: &RepoId, entry: &Entry) {
        stop_mirror(entry);
        if let Some(held) = entry.lease.lock().await.take() {
            held.renew.abort();
            if let Some(leases) = &self.leases {
                leases.release(&seq_lease_key(ns, repo), held.epoch).await;
            }
        }
        if let Some(log) = entry.log.get() {
            log.clone().shutdown().await;
        }
    }
}

/// Renews `repo`'s lease every `renew_every` while this node holds it. A
/// refusal, or a lease store that stays unreachable until the lease would
/// have expired, closes the sequencer.
async fn renew_lease(inner: Weak<Inner>, ns: NamespaceId, repo: RepoId, key: String, epoch: u64) {
    let (every, ttl) = match inner.upgrade() {
        Some(inner) => (inner.config.renew_every, inner.config.lease_ttl),
        None => return,
    };
    let mut renewed = Instant::now();
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if inner.renewals_paused.load(Ordering::SeqCst) {
            continue;
        }
        let Some(leases) = inner.leases.clone() else {
            return;
        };
        let lost = match leases.renew(&key, epoch, ttl).await {
            Ok(()) => {
                renewed = Instant::now();
                false
            }
            Err(LeaseError::Unavailable(err)) => {
                tracing::warn!(%ns, %repo, error = %err, "renewing the sequencer lease failed");
                renewed.elapsed() >= ttl
            }
            Err(_) => true,
        };
        if !lost {
            continue;
        }
        inner.stats.leases_lost.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(%ns, %repo, epoch, "the sequencer lease was lost; closing the sequencer");
        let entry = {
            let mut entries = inner.entries.lock().await;
            match entries.get(&(ns, repo.clone())) {
                // Only the entry this renewal belongs to.
                Some(entry)
                    if entry.lease.try_lock().is_ok_and(|held| {
                        held.as_ref().is_some_and(|held| held.epoch == epoch)
                    }) =>
                {
                    entries.remove(&(ns, repo.clone()))
                }
                _ => None,
            }
        };
        if let Some(entry) = entry {
            // This task is the renewal: forget it rather than abort itself.
            entry.lease.lock().await.take();
            stop_mirror(&entry);
            if let Some(log) = entry.log.get() {
                log.clone().shutdown().await;
            }
        }
        return;
    }
}

/// Closes sequencers unused for `idle` (Ruling 28), until the sequencers
/// are dropped.
async fn sweep(inner: Weak<Inner>, idle: Duration) {
    let every = (idle / 4).max(Duration::from_millis(10));
    let idle_ms = u64::try_from(idle.as_millis()).unwrap_or(u64::MAX);
    loop {
        tokio::time::sleep(every).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let now = inner.now_ms();
        let idle_entries: Vec<(Key, Arc<Entry>)> = {
            let mut entries = inner.entries.lock().await;
            let keys: Vec<Key> = entries
                .iter()
                .filter(|(_, entry)| {
                    entry.busy.load(Ordering::SeqCst) == 0
                        && now.saturating_sub(entry.last_used_ms.load(Ordering::SeqCst)) >= idle_ms
                })
                .map(|(key, _)| key.clone())
                .collect();
            keys.into_iter()
                .filter_map(|key| entries.remove(&key).map(|entry| (key, entry)))
                .collect()
        };
        for ((ns, repo), entry) in idle_entries {
            if entry.log.initialized() {
                inner.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
            inner.retire(ns, &repo, &entry).await;
        }
    }
}

/// Stops a repository's `_git` mirror, if it runs.
fn stop_mirror(entry: &Entry) {
    if let Some(mirror) = entry
        .mirror
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        mirror.abort();
    }
}
