//! Test helpers (plan GT1 Task 10): a clock tests set, and a ref log over
//! an in-memory store.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use loams_store::Store;

use crate::blob::StoreBlobStore;
use crate::clock::{Clock, SystemClock};
use crate::format::pb;
use crate::ids::{NamespaceId, RepoId, Seq};
use crate::paths::RepoPaths;
use crate::reflog::RefError;
use crate::sequencer::{BucketRefLog, SequencerConfig};
use crate::state::RepoState;
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

/// Writes checkpoint 0 of an empty SHA-1 repository, unless one exists.
pub async fn ensure_repo(wal: &dyn WalStore) -> Result<(), RefError> {
    if wal.latest_checkpoint(Seq::ZERO).await?.is_none() {
        wal.put_checkpoint(&RepoState::empty(pb::ObjectFormat::Sha1).to_checkpoint(0))
            .await?;
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
