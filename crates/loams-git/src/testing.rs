//! Test helpers (plan GT1 Task 10): a clock tests set, and a ref log over
//! an in-memory store.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use loams_store::Store;

use crate::blob::StoreBlobStore;
use crate::checkpointer::create_repo;
use crate::clock::{Clock, SystemClock};
use crate::format::pb;
use crate::ids::{NamespaceId, RepoId, Seq};
use crate::paths::RepoPaths;
use crate::reflog::RefError;
use crate::sequencer::{BucketRefLog, SequencerConfig};
use crate::wal::{StoreWalStore, WalStore};

/// A clock tests set by hand. With a step, every read advances it by that
/// many ms (so two reads never agree).
#[derive(Debug, Default)]
pub struct ManualClock {
    now: AtomicI64,
    step: i64,
}

impl ManualClock {
    /// A clock stopped at `now_unix_ms`.
    pub fn new(now_unix_ms: i64) -> Self {
        Self::stepping(now_unix_ms, 0)
    }

    /// A clock at `now_unix_ms` that advances by `step_ms` after each read.
    pub fn stepping(now_unix_ms: i64, step_ms: i64) -> Self {
        Self {
            now: AtomicI64::new(now_unix_ms),
            step: step_ms,
        }
    }

    /// Sets the time.
    pub fn set(&self, now_unix_ms: i64) {
        self.now.store(now_unix_ms, Ordering::SeqCst);
    }

    /// Moves the time forward by `ms`.
    pub fn advance(&self, ms: i64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> i64 {
        self.now.fetch_add(self.step, Ordering::SeqCst)
    }
}

/// The namespace of [`mem_reflog`]'s repository.
pub const TEST_NS: NamespaceId = NamespaceId(1);
/// The id of [`mem_reflog`]'s repository.
pub const TEST_REPO: &str = "r01jtest000000000000000000";

/// The paths of [`mem_reflog`]'s repository.
pub fn test_paths() -> RepoPaths {
    RepoPaths::new(TEST_NS, &test_repo())
}

/// [`TEST_REPO`] as an id.
pub fn test_repo() -> RepoId {
    RepoId::parse(TEST_REPO).expect("a valid repository id")
}

/// Writes checkpoint 0 of an empty SHA-1 repository, unless it has a
/// checkpoint already.
pub async fn ensure_repo(wal: &dyn WalStore) -> Result<(), RefError> {
    if wal.latest_checkpoint(Seq(u64::MAX)).await?.is_none() {
        create_repo(wal, pb::ObjectFormat::Sha1).await?;
    }
    Ok(())
}

/// A ref log of [`TEST_REPO`] in `store`, created if it does not exist,
/// with the system clock and the default configuration.
pub async fn mem_reflog(store: Store) -> BucketRefLog {
    mem_reflog_with(store, Arc::new(SystemClock), |_| {}).await
}

/// [`mem_reflog`] with `clock`, and `configure` applied to the default
/// configuration.
pub async fn mem_reflog_with(
    store: Store,
    clock: Arc<dyn Clock>,
    configure: impl FnOnce(&mut SequencerConfig),
) -> BucketRefLog {
    let paths = test_paths();
    let wal = Arc::new(StoreWalStore::new(store.clone(), paths.clone()).with_clock(clock.clone()));
    ensure_repo(wal.as_ref())
        .await
        .expect("create the test repository");
    let blobs = Arc::new(StoreBlobStore::new(store, paths));
    let mut config = SequencerConfig::new(format!("org/{TEST_NS}"), TEST_NS, test_repo());
    configure(&mut config);
    BucketRefLog::open(wal, blobs, config, clock)
        .await
        .expect("open the test repository")
}

/// One shared table of owner leases, as a metastore holds them, for the
/// ownership tests (plan GT1 Task 14). Each node takes leases through its
/// own [`MemLeases`] client.
#[derive(Debug, Default)]
pub struct MemLeaseTable {
    leases: std::sync::Mutex<std::collections::HashMap<String, MemLease>>,
}

#[derive(Debug, Clone)]
struct MemLease {
    owner: Option<String>,
    epoch: u64,
    deadline: std::time::Instant,
}

impl MemLeaseTable {
    /// A client that takes leases as `owner`.
    pub fn client(self: &Arc<Self>, owner: &str) -> Arc<MemLeases> {
        Arc::new(MemLeases {
            table: Arc::clone(self),
            owner: owner.to_string(),
        })
    }

    /// Ends `key`'s lease now, as if its holder had stopped renewing.
    pub fn expire(&self, key: &str) {
        if let Some(lease) = self.lock().get_mut(key) {
            lease.deadline = std::time::Instant::now();
        }
    }

    /// Who holds `key` now, and at which epoch.
    pub fn holder(&self, key: &str) -> Option<(String, u64)> {
        let now = std::time::Instant::now();
        self.lock().get(key).and_then(|lease| {
            let owner = lease.owner.clone()?;
            (now < lease.deadline).then_some((owner, lease.epoch))
        })
    }

    /// Whether `key`'s holder released it (rather than letting it expire).
    pub fn released(&self, key: &str) -> bool {
        self.lock()
            .get(key)
            .is_some_and(|lease| lease.owner.is_none())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, MemLease>> {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One node's client of a [`MemLeaseTable`].
#[derive(Debug)]
pub struct MemLeases {
    table: Arc<MemLeaseTable>,
    owner: String,
}

#[async_trait::async_trait]
impl crate::owner::SeqLeases for MemLeases {
    async fn acquire(
        &self,
        key: &str,
        ttl: std::time::Duration,
    ) -> Result<u64, crate::owner::LeaseError> {
        let now = std::time::Instant::now();
        let mut leases = self.table.lock();
        let lease = leases.entry(key.to_string()).or_insert(MemLease {
            owner: None,
            epoch: 0,
            deadline: now,
        });
        let held = lease.owner.is_some() && now < lease.deadline;
        match &lease.owner {
            Some(owner) if held && *owner != self.owner => Err(crate::owner::LeaseError::Held {
                holder: owner.clone(),
            }),
            Some(owner) if held && *owner == self.owner => {
                lease.deadline = now + ttl;
                Ok(lease.epoch)
            }
            _ => {
                lease.owner = Some(self.owner.clone());
                lease.epoch += 1;
                lease.deadline = now + ttl;
                Ok(lease.epoch)
            }
        }
    }

    async fn renew(
        &self,
        key: &str,
        epoch: u64,
        ttl: std::time::Duration,
    ) -> Result<(), crate::owner::LeaseError> {
        let now = std::time::Instant::now();
        let mut leases = self.table.lock();
        match leases.get_mut(key) {
            Some(lease)
                if lease.owner.as_deref() == Some(self.owner.as_str())
                    && lease.epoch == epoch
                    && now < lease.deadline =>
            {
                lease.deadline = now + ttl;
                Ok(())
            }
            _ => Err(crate::owner::LeaseError::Lost),
        }
    }

    async fn release(&self, key: &str, epoch: u64) {
        let mut leases = self.table.lock();
        if let Some(lease) = leases.get_mut(key)
            && lease.owner.as_deref() == Some(self.owner.as_str())
            && lease.epoch == epoch
        {
            lease.owner = None;
        }
    }
}

/// A placement a test sets by hand, which records the nodes marked
/// unreachable.
#[derive(Debug)]
pub struct SetPlacement {
    owner: std::sync::Mutex<crate::owner::SeqOwner>,
    unreachable: std::sync::Mutex<Vec<u64>>,
}

impl SetPlacement {
    /// Every repository owned by `owner`.
    pub fn new(owner: crate::owner::SeqOwner) -> Arc<Self> {
        Arc::new(Self {
            owner: std::sync::Mutex::new(owner),
            unreachable: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Moves every repository to `owner`.
    pub fn set(&self, owner: crate::owner::SeqOwner) {
        *self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = owner;
    }

    /// The nodes marked unreachable, in order.
    pub fn unreachable(&self) -> Vec<u64> {
        self.unreachable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crate::owner::SeqPlacement for SetPlacement {
    fn owner(&self, _: NamespaceId, _: &RepoId) -> crate::owner::SeqOwner {
        self.owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn mark_unreachable(&self, node_id: u64) {
        self.unreachable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(node_id);
    }
}

/// Forwards in process to the sequencers registered under an address; an
/// address marked down refuses, as a dead or partitioned node would.
#[derive(Debug, Default)]
pub struct LoopbackForwarder {
    nodes: std::sync::Mutex<std::collections::HashMap<String, crate::owner::RepoSequencers>>,
    down: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl LoopbackForwarder {
    /// Serves `addr` with `node`.
    pub fn register(&self, addr: &str, node: crate::owner::RepoSequencers) {
        self.nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(addr.to_string(), node);
    }

    /// Marks `addr` unreachable (`true`) or reachable again.
    pub fn set_down(&self, addr: &str, down: bool) {
        let mut set = self
            .down
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if down {
            set.insert(addr.to_string());
        } else {
            set.remove(addr);
        }
    }
}

#[async_trait::async_trait]
impl crate::forward::SeqForwarder for LoopbackForwarder {
    async fn forward(&self, addr: &str, body: bytes::Bytes) -> Result<bytes::Bytes, String> {
        if self
            .down
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(addr)
        {
            return Err(format!("{addr}: connection refused"));
        }
        let node = self
            .nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(addr)
            .cloned()
            .ok_or_else(|| format!("{addr}: no such node"))?;
        Ok(node.serve_forwarded(&body).await)
    }
}
