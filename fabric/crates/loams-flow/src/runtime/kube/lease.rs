//! `InstanceLease` and `SplitLease` (OR-16, Review Focus 12): fenced leases
//! on `_fabric.flow_objects` rows, compare-and-swap on `version`, with a
//! holder and an expiry. Every acquisition by a new holder raises the
//! lease's `epoch`, and every checkpoint write carries that epoch (CN2 Task
//! 4), so a stale holder's write is refused ([`FencedCheckpointStore`]).
//!
//! [`LeaseStore`] is the trait FL3's metastore leases replace these behind.
//! [`MemoryLeaseStore`] serves the tests; the `_fabric.flow_objects`
//! implementation waits for FL1 (R26.1).
//!
//! Keys: `instance/<ns>/<instance>`, `split/<ns>/<instance>/<split>`,
//! `member/<holder>` (a flow pod's liveness, which sizes each pod's fair
//! share). Times are milliseconds on the store's clock, passed in as `now`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loams_connector_sdk::{Checkpoint, CheckpointError, CheckpointStore, InstanceKey, SplitId};

/// One `_fabric.flow_objects` lease row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub key: String,
    /// Raised by one on every write; the compare-and-swap's subject.
    pub version: u64,
    /// `None` once released.
    pub holder: Option<String>,
    /// Raised on every acquisition; carried by every checkpoint write.
    pub epoch: u64,
    pub expires_at_ms: u64,
}

/// Why a lease operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseError {
    /// Another holder (or a newer acquisition) owns the lease: stop work on
    /// it at once.
    #[error("lease {key} fenced: held at epoch {current}, not {epoch}")]
    Fenced {
        key: String,
        epoch: u64,
        current: u64,
    },
    /// The store could not be reached; the lease stays valid until its
    /// expiry and no longer.
    #[error("lease store unavailable: {0}")]
    Unavailable(String),
}

/// The lease rows (`_fabric.flow_objects`; FL3's metastore later).
#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError>;
    /// Every row whose key starts with `prefix`, sorted by key.
    async fn list(&self, prefix: &str) -> Result<Vec<LeaseRecord>, LeaseError>;
    /// Writes `new` if the stored row's version is `expected` (`None`: no
    /// row). Returns whether it wrote.
    async fn compare_and_swap(
        &self,
        expected: Option<u64>,
        new: &LeaseRecord,
    ) -> Result<bool, LeaseError>;
}

/// An in-memory [`LeaseStore`].
#[derive(Debug, Default)]
pub struct MemoryLeaseStore {
    rows: Mutex<BTreeMap<String, LeaseRecord>>,
}

impl MemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn rows(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, LeaseRecord>> {
        self.rows.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl LeaseStore for MemoryLeaseStore {
    async fn get(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(self.rows().get(key).cloned())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<LeaseRecord>, LeaseError> {
        Ok(self
            .rows()
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn compare_and_swap(
        &self,
        expected: Option<u64>,
        new: &LeaseRecord,
    ) -> Result<bool, LeaseError> {
        let mut rows = self.rows();
        if rows.get(&new.key).map(|r| r.version) != expected {
            return Ok(false);
        }
        rows.insert(new.key.clone(), new.clone());
        Ok(true)
    }
}

/// A lease this process holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub key: String,
    pub holder: String,
    pub epoch: u64,
    pub version: u64,
    pub expires_at_ms: u64,
}

impl Lease {
    /// Work under the lease may run only while this is true.
    pub fn is_live(&self, now_ms: u64) -> bool {
        now_ms < self.expires_at_ms
    }
}

/// A native instance's lease: its holder runs the enumerator and the
/// instance's checkpoints.
pub type InstanceLease = Lease;
/// A split's lease: its holder reads the split.
pub type SplitLease = Lease;

/// Takes, renews and releases leases as one holder (a flow pod).
#[derive(Clone)]
pub struct LeaseClient {
    store: Arc<dyn LeaseStore>,
    holder: String,
    ttl_ms: u64,
}

impl std::fmt::Debug for LeaseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseClient")
            .field("holder", &self.holder)
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

/// How often a CAS that lost a race is retried before giving up for this
/// tick.
const CAS_ATTEMPTS: usize = 3;

impl LeaseClient {
    pub fn new(store: Arc<dyn LeaseStore>, holder: impl Into<String>, ttl_ms: u64) -> Self {
        Self {
            store,
            holder: holder.into(),
            ttl_ms,
        }
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn store(&self) -> &Arc<dyn LeaseStore> {
        &self.store
    }

    /// Takes `key` if it is free, released or expired, with the next epoch.
    /// `None` while another holder's lease is live.
    pub async fn acquire(&self, key: &str, now_ms: u64) -> Result<Option<Lease>, LeaseError> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.store.get(key).await?;
            if let Some(r) = &current
                && r.holder.as_deref().is_some_and(|h| h != self.holder)
                && now_ms < r.expires_at_ms
            {
                return Ok(None);
            }
            let next = LeaseRecord {
                key: key.to_owned(),
                version: current.as_ref().map_or(1, |r| r.version + 1),
                holder: Some(self.holder.clone()),
                // A new acquisition is a new epoch, even by the same holder
                // name (a restarted process must not share its predecessor's).
                epoch: current.as_ref().map_or(1, |r| r.epoch + 1),
                expires_at_ms: now_ms + self.ttl_ms,
            };
            if self
                .store
                .compare_and_swap(current.as_ref().map(|r| r.version), &next)
                .await?
            {
                return Ok(Some(self.lease(&next)));
            }
        }
        Ok(None)
    }

    /// Extends a lease this holder still owns at the same epoch; `Fenced`
    /// once anyone else acquired it.
    pub async fn renew(&self, lease: &Lease, now_ms: u64) -> Result<Lease, LeaseError> {
        for _ in 0..CAS_ATTEMPTS {
            let r = self.owned(lease).await?;
            let next = LeaseRecord {
                version: r.version + 1,
                expires_at_ms: now_ms + self.ttl_ms,
                ..r.clone()
            };
            if self.store.compare_and_swap(Some(r.version), &next).await? {
                return Ok(self.lease(&next));
            }
        }
        Err(LeaseError::Unavailable(format!(
            "lease {}: renewal kept losing races",
            lease.key
        )))
    }

    /// Gives a lease up; `Fenced` if it is no longer this holder's.
    pub async fn release(&self, lease: &Lease) -> Result<(), LeaseError> {
        for _ in 0..CAS_ATTEMPTS {
            let r = self.owned(lease).await?;
            let next = LeaseRecord {
                version: r.version + 1,
                holder: None,
                expires_at_ms: 0,
                ..r.clone()
            };
            if self.store.compare_and_swap(Some(r.version), &next).await? {
                return Ok(());
            }
        }
        Err(LeaseError::Unavailable(format!(
            "lease {}: release kept losing races",
            lease.key
        )))
    }

    /// The holders of the live leases under `prefix`.
    pub async fn live_holders(&self, prefix: &str, now_ms: u64) -> Result<Vec<String>, LeaseError> {
        Ok(self
            .store
            .list(prefix)
            .await?
            .into_iter()
            .filter(|r| now_ms < r.expires_at_ms)
            .filter_map(|r| r.holder)
            .collect())
    }

    async fn owned(&self, lease: &Lease) -> Result<LeaseRecord, LeaseError> {
        let r = self.store.get(&lease.key).await?;
        match r {
            Some(r) if r.holder.as_deref() == Some(&self.holder) && r.epoch == lease.epoch => Ok(r),
            other => Err(LeaseError::Fenced {
                key: lease.key.clone(),
                epoch: lease.epoch,
                current: other.map_or(0, |r| r.epoch),
            }),
        }
    }

    fn lease(&self, r: &LeaseRecord) -> Lease {
        Lease {
            key: r.key.clone(),
            holder: self.holder.clone(),
            epoch: r.epoch,
            version: r.version,
            expires_at_ms: r.expires_at_ms,
        }
    }
}

/// `instance/<ns>/<instance>`.
pub fn instance_lease_key(i: &InstanceKey) -> String {
    format!("instance/{}/{}", i.namespace, i.name)
}

/// `split/<ns>/<instance>/<split>`.
pub fn split_lease_key(i: &InstanceKey, s: &SplitId) -> String {
    format!("split/{}/{}/{}", i.namespace, i.name, s)
}

/// `member/<holder>`.
pub fn member_lease_key(holder: &str) -> String {
    format!("member/{holder}")
}

/// The checkpoint key of a split read on its own (a pod holding the split's
/// lease runs a pipeline over that split alone): `<instance>/<split>`.
/// Instance names never contain `/`, so the two kinds of key cannot collide
/// (R26.9).
pub fn split_checkpoint_key(i: &InstanceKey, s: &SplitId) -> InstanceKey {
    InstanceKey::new(i.namespace, format!("{}/{}", i.name, s))
}

/// The lease that fences a checkpoint key: the split's for a
/// [`split_checkpoint_key`], else the instance's.
pub fn checkpoint_lease_key(k: &InstanceKey) -> String {
    if k.name.contains('/') {
        format!("split/{}/{}", k.namespace, k.name)
    } else {
        instance_lease_key(k)
    }
}

/// A [`CheckpointStore`] that refuses a save whose epoch is not the current
/// epoch of the lease that fences it, before the inner store's own epoch
/// check. A stale holder is refused as soon as its successor acquires, not
/// only after the successor's first save. The `_fabric.flow_positions` store
/// does both checks in one transaction (R26.9).
pub struct FencedCheckpointStore {
    inner: Arc<dyn CheckpointStore>,
    leases: Arc<dyn LeaseStore>,
}

impl FencedCheckpointStore {
    pub fn new(inner: Arc<dyn CheckpointStore>, leases: Arc<dyn LeaseStore>) -> Self {
        Self { inner, leases }
    }
}

impl std::fmt::Debug for FencedCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedCheckpointStore")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CheckpointStore for FencedCheckpointStore {
    async fn load(&self, i: &InstanceKey) -> Result<Option<Checkpoint>, CheckpointError> {
        self.inner.load(i).await
    }

    async fn save(&self, cp: &Checkpoint) -> Result<(), CheckpointError> {
        let key = checkpoint_lease_key(&cp.instance);
        let lease = self.leases.get(&key).await.map_err(|e| match e {
            LeaseError::Unavailable(m) => CheckpointError::Unavailable(m),
            other => CheckpointError::Other(other.to_string()),
        })?;
        match lease {
            Some(r) if r.epoch == cp.epoch && r.holder.is_some() => self.inner.save(cp).await,
            Some(r) if r.epoch > cp.epoch => Err(CheckpointError::Fenced {
                stored: r.epoch,
                attempted: cp.epoch,
            }),
            Some(_) => Err(CheckpointError::Other(format!(
                "checkpoint epoch {} is not lease {key}'s",
                cp.epoch
            ))),
            None => Err(CheckpointError::Other(format!("no lease {key}"))),
        }
    }
}

/// Holds a fair share of a set of lease keys: `ceil(keys / live members)`.
/// Each tick renews what it holds, gives up what is gone or over its share
/// (so a new pod gets work), and takes free or expired keys up to its share
/// (so a lost pod's work moves once its leases expire).
#[derive(Debug, Default)]
pub struct Balancer {
    held: BTreeMap<String, Lease>,
}

impl Balancer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn held(&self) -> &BTreeMap<String, Lease> {
        &self.held
    }

    pub async fn balance(
        &mut self,
        client: &LeaseClient,
        keys: &BTreeSet<String>,
        members: usize,
        now_ms: u64,
    ) -> Result<(), LeaseError> {
        let held: Vec<String> = self.held.keys().cloned().collect();
        for key in held {
            let Some(lease) = self.held.get(&key).cloned() else {
                continue;
            };
            if !keys.contains(&key) {
                self.held.remove(&key);
                // Gone from the set; a fenced release means someone else has
                // it already.
                match client.release(&lease).await {
                    Ok(()) | Err(LeaseError::Fenced { .. }) => {}
                    Err(e) => return Err(e),
                }
                continue;
            }
            match client.renew(&lease, now_ms).await {
                Ok(l) => {
                    self.held.insert(key, l);
                }
                Err(LeaseError::Fenced { .. }) => {
                    self.held.remove(&key);
                }
                Err(e) => return Err(e),
            }
        }

        let share = keys.len().div_ceil(members.max(1));
        while self.held.len() > share {
            let Some((_, lease)) = self.held.pop_last() else {
                break;
            };
            match client.release(&lease).await {
                Ok(()) | Err(LeaseError::Fenced { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        for key in keys {
            if self.held.len() >= share {
                break;
            }
            if self.held.contains_key(key) {
                continue;
            }
            if let Some(l) = client.acquire(key, now_ms).await? {
                self.held.insert(key.clone(), l);
            }
        }
        Ok(())
    }
}
