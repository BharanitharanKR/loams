//! The repository sequencers of one node (§36 §4.4; plan GT1 Task 13).
//!
//! [`RepoSequencers`] opens a repository's [`BucketRefLog`] on first use and
//! keeps it, so every call on this node shares one sequencer and its group
//! commit. Correctness never depends on it being the only one (§36 §4.2): a
//! sequencer on another node, or a serverless helper writing the bucket, is
//! fenced by the create-only segment PUT. GT1 Task 14 adds placement (the
//! rendezvous owner under the `task/git-seq/<ns>/<repo_id>` lease),
//! forwarding to the owner and idle eviction.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use loams_store::Store;
use tokio::sync::Mutex;

use crate::blob::StoreBlobStore;
use crate::clock::Clock;
use crate::ids::{NamespaceId, RepoId};
use crate::paths::RepoPaths;
use crate::reflog::RefError;
use crate::sequencer::{BucketRefLog, SequencerConfig};
use crate::wal::StoreWalStore;

/// The open sequencers of this node, by repository.
pub struct RepoSequencers {
    store: Store,
    clock: Arc<dyn Clock>,
    logs: Mutex<HashMap<(NamespaceId, RepoId), BucketRefLog>>,
}

impl fmt::Debug for RepoSequencers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepoSequencers").finish_non_exhaustive()
    }
}

impl RepoSequencers {
    /// Sequencers of the repositories in `store`, with `clock`.
    pub fn new(store: Store, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            clock,
            logs: Mutex::new(HashMap::new()),
        }
    }

    /// The ref log of `repo` in `ns`, opened (with `tenant`, `<org>/<ns>`,
    /// as its events' `tenantid`) if this node has none. `NotFound` when the
    /// repository has no checkpoint.
    pub async fn get(
        &self,
        ns: NamespaceId,
        tenant: &str,
        repo: &RepoId,
    ) -> Result<BucketRefLog, RefError> {
        let mut logs = self.logs.lock().await;
        if let Some(log) = logs.get(&(ns, repo.clone())) {
            return Ok(log.clone());
        }
        let paths = RepoPaths::new(ns, repo);
        let wal =
            StoreWalStore::new(self.store.clone(), paths.clone()).with_clock(self.clock.clone());
        let log = BucketRefLog::open(
            Arc::new(wal),
            Arc::new(StoreBlobStore::new(self.store.clone(), paths)),
            SequencerConfig::new(tenant, ns, repo.clone()),
            self.clock.clone(),
        )
        .await?;
        logs.insert((ns, repo.clone()), log.clone());
        Ok(log)
    }

    /// Stops `repo`'s sequencer, if this node runs one (the repository was
    /// deleted). Calls that still hold its handle are answered `Unavailable`.
    pub async fn close(&self, ns: NamespaceId, repo: &RepoId) {
        let log = self.logs.lock().await.remove(&(ns, repo.clone()));
        if let Some(log) = log {
            log.shutdown().await;
        }
    }

    /// Stops every sequencer (server shutdown).
    pub async fn shutdown(&self) {
        let logs: Vec<BucketRefLog> = self.logs.lock().await.drain().map(|(_, log)| log).collect();
        for log in logs {
            log.shutdown().await;
        }
    }

    /// How many sequencers this node runs.
    pub async fn len(&self) -> usize {
        self.logs.lock().await.len()
    }

    /// Whether this node runs no sequencer.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}
