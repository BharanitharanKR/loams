//! The migration ledger, the stack record's home and the migration lease, in
//! the `_system` namespace of the metastore (design §54 §6.2, D928; UP1
//! Task 2).
//!
//! Everything is built on the metastore's existing pointers and leases (UP1
//! ruling T0-2), so neither backend's stored format changes and the previous
//! release still reads the metastore after a migration ran:
//!
//! | What | Where |
//! |---|---|
//! | the stack record | pointer `upgrade/stack` |
//! | one ledger entry | pointer `upgrade/ledger/<seq:08>-<id>` |
//! | a snapshot's root | pointer `upgrade/snapshots/<ulid>` (Task 8) |
//! | the migration lease | lease `_system/upgrade/lease` |
//!
//! `_system` already exists on stacks whose auth directory was written (its
//! pointer `principal-directory`); the ledger shares the namespace under
//! `upgrade/`.
//!
//! A pointer value is at most [`MAX_KEY_LEN`] bytes: one value version byte
//! and the record's postcard encoding, base64url without padding. Fields that
//! could grow are capped ([`MAX_ERROR_MESSAGE`], [`MAX_CURSOR`]).
//!
//! Every write carries the lease's fence, so a holder whose lease was taken
//! over can no longer change the ledger. The metastore has no multi-key
//! transaction: the stack record is the commit point. [`Ledger::finish`]
//! marks the last entry `Done`, then writes the stack record; a crash between
//! the two leaves every entry `Done` and the stack at the old release, which
//! [`Ledger::awaiting_stack`] reports and a rerun completes.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use loams_common::NamespaceId;
use loams_common::meta::{
    ApplyError, Consistency, Fence, MAX_KEY_LEN, MetaError, MetaStore, Pointer, PointerCas,
};
use serde::{Deserialize, Serialize};

use crate::formats::Store;
use crate::stack::StackRecord;

/// The reserved namespace (shared with the auth directory).
pub use loams_common::meta::SYSTEM_NAMESPACE;
/// The stack record's pointer key.
pub const STACK_KEY: &str = "upgrade/stack";
/// The ledger entries' pointer key prefix.
pub const LEDGER_PREFIX: &str = "upgrade/ledger/";
/// The snapshot roots' pointer key prefix.
pub const SNAPSHOTS_PREFIX: &str = "upgrade/snapshots/";
/// The migration lease's key (leases are global, not per namespace).
pub const LEASE_KEY: &str = "_system/upgrade/lease";
/// How long a migration lease lasts without a renewal.
pub const LEASE_TTL: Duration = Duration::from_secs(30);
/// How often a holder renews it.
pub const LEASE_RENEW_EVERY: Duration = Duration::from_secs(10);
/// The value version in front of an encoded ledger entry.
pub const ENTRY_VALUE_VERSION: u8 = 0x01;
/// The longest error message a ledger entry keeps, in bytes.
pub const MAX_ERROR_MESSAGE: usize = 256;
/// The longest cursor a migration may checkpoint, in bytes.
pub const MAX_CURSOR: usize = 256;

/// Whether the previous release opens the data after a migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Reversible,
    PointOfNoReturn,
}

/// Where a migration is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerState {
    Pending,
    Running,
    Done,
    Failed,
}

/// Why a migration failed, as the ledger keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerError {
    pub code: String,
    pub message: String,
}

impl LedgerError {
    /// An error with its message cut to [`MAX_ERROR_MESSAGE`] bytes (on a
    /// character boundary).
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > MAX_ERROR_MESSAGE {
            let mut end = MAX_ERROR_MESSAGE;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        LedgerError {
            code: code.into(),
            message,
        }
    }
}

/// One applied (or applying) migration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub seq: u32,
    pub id: String,
    pub release_from: String,
    pub release_to: String,
    pub store: Store,
    pub class: Class,
    pub state: LedgerState,
    pub cursor: Option<Vec<u8>>,
    pub attempts: u32,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub error: Option<LedgerError>,
    pub image_digest: Option<String>,
    pub code_digest: [u8; 32],
}

impl LedgerEntry {
    /// The entry's pointer key: `upgrade/ledger/<seq:08>-<id>`.
    pub fn key(&self) -> String {
        format!("{LEDGER_PREFIX}{:08}-{}", self.seq, self.id)
    }
}

/// What can go wrong reading or writing the ledger.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    /// Another holder has the migration lease (`upgrade_in_progress`).
    #[error("upgrade_in_progress: the migration lease is held by {owner}")]
    LeaseHeld { owner: String },
    /// This holder's lease was taken over or released; nothing was written.
    #[error("the migration lease was lost; nothing was written")]
    Fenced,
    /// A write raced with another writer of the same key.
    #[error("ledger key {key} changed underneath this write")]
    Conflict { key: String },
    #[error("ledger entry {seq} does not exist")]
    NoEntry { seq: u32 },
    #[error("ledger entry {seq} is not the next one (expected {expected})")]
    OutOfOrder { seq: u32, expected: u32 },
    /// A value would not fit a pointer (`MAX_KEY_LEN` bytes).
    #[error("ledger value for {key} is {len} bytes, more than {MAX_KEY_LEN}")]
    TooLarge { key: String, len: usize },
    #[error("cursor of {len} bytes is longer than {MAX_CURSOR}")]
    CursorTooLong { len: usize },
    #[error("corrupt ledger value at {key}: {why}")]
    Corrupt { key: String, why: String },
}

type Result<T> = std::result::Result<T, Error>;

fn encode_value<T: Serialize>(key: &str, version: u8, value: &T) -> Result<String> {
    let bytes = postcard::to_extend(value, vec![version]).map_err(|e| Error::Corrupt {
        key: key.to_string(),
        why: e.to_string(),
    })?;
    let text = URL_SAFE_NO_PAD.encode(bytes);
    if text.len() > MAX_KEY_LEN {
        return Err(Error::TooLarge {
            key: key.to_string(),
            len: text.len(),
        });
    }
    Ok(text)
}

fn decode_bytes(key: &str, value: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(value).map_err(|e| Error::Corrupt {
        key: key.to_string(),
        why: e.to_string(),
    })
}

fn decode_entry(key: &str, value: &str) -> Result<LedgerEntry> {
    let bytes = decode_bytes(key, value)?;
    let corrupt = |why: String| Error::Corrupt {
        key: key.to_string(),
        why,
    };
    match bytes.split_first() {
        Some((&ENTRY_VALUE_VERSION, body)) => {
            postcard::from_bytes(body).map_err(|e| corrupt(e.to_string()))
        }
        Some((v, _)) => Err(corrupt(format!("value version {v}"))),
        None => Err(corrupt("empty value".to_string())),
    }
}

/// The holder of the migration lease. Every ledger write is fenced by it.
#[derive(Clone, Debug)]
pub struct MigrationLease {
    meta: Arc<dyn MetaStore>,
    holder: String,
    epoch: u64,
}

impl MigrationLease {
    /// Takes the lease for `holder` (unique per process incarnation) for
    /// `ttl`; [`Error::LeaseHeld`] while someone else holds it.
    pub async fn acquire(
        meta: Arc<dyn MetaStore>,
        holder: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let holder = holder.into();
        match meta.acquire_lease(LEASE_KEY, &holder, ttl).await {
            Ok(grant) => Ok(MigrationLease {
                meta,
                holder,
                epoch: grant.epoch,
            }),
            Err(MetaError::Rejected(ApplyError::LeaseHeld { owner, .. })) => {
                Err(Error::LeaseHeld { owner })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Extends the lease by `ttl` from now ([`LEASE_RENEW_EVERY`] apart);
    /// [`Error::Fenced`] once it was lost.
    pub async fn renew(&self, ttl: Duration) -> Result<()> {
        match self
            .meta
            .renew_lease(LEASE_KEY, &self.holder, self.epoch, ttl)
            .await
        {
            Ok(_) => Ok(()),
            Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => Err(Error::Fenced),
            Err(e) => Err(e.into()),
        }
    }

    /// Gives the lease up.
    pub async fn release(self) -> Result<()> {
        match self
            .meta
            .release_lease(LEASE_KEY, &self.holder, self.epoch)
            .await
        {
            Ok(()) => Ok(()),
            Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => Err(Error::Fenced),
            Err(e) => Err(e.into()),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// The fence a ledger write carries.
    pub fn fence(&self) -> Fence {
        Fence {
            lease: LEASE_KEY.to_string(),
            epoch: self.epoch,
        }
    }
}

/// The ledger and the stack record of one metastore.
#[derive(Clone, Debug)]
pub struct Ledger {
    meta: Arc<dyn MetaStore>,
    namespace: NamespaceId,
}

impl Ledger {
    /// The ledger, creating the `_system` namespace when it does not exist
    /// yet.
    pub async fn open(meta: Arc<dyn MetaStore>) -> Result<Self> {
        let namespace = match meta.create_namespace(SYSTEM_NAMESPACE).await {
            Ok(id) | Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) => id,
            Err(e) => return Err(e.into()),
        };
        Ok(Ledger { meta, namespace })
    }

    /// The ledger if `_system` exists, without creating anything (for
    /// read-only callers: `status`, `plan`, the startup gate).
    pub async fn open_existing(meta: Arc<dyn MetaStore>) -> Result<Option<Self>> {
        let found = meta
            .namespace_by_name(Consistency::Linearizable, SYSTEM_NAMESPACE)
            .await?;
        Ok(found.map(|ns| Ledger {
            meta,
            namespace: ns.id,
        }))
    }

    /// The `_system` namespace's id.
    pub fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    /// Every entry, in sequence order.
    pub async fn entries(&self) -> Result<Vec<LedgerEntry>> {
        let pointers = self
            .meta
            .pointers_with_prefix(Consistency::Linearizable, self.namespace, LEDGER_PREFIX)
            .await?;
        let mut entries = pointers
            .iter()
            .map(|(key, pointer)| decode_entry(key, &pointer.value))
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|e| e.seq);
        Ok(entries)
    }

    /// The sequence number the next appended entry must carry.
    pub async fn next_seq(&self) -> Result<u32> {
        Ok(self.entries().await?.last().map_or(1, |e| e.seq + 1))
    }

    /// Appends `entry`, whose `seq` must be [`Ledger::next_seq`].
    pub async fn append(&self, lease: &MigrationLease, entry: &LedgerEntry) -> Result<()> {
        check_cursor(entry)?;
        let expected = self.next_seq().await?;
        if entry.seq != expected {
            return Err(Error::OutOfOrder {
                seq: entry.seq,
                expected,
            });
        }
        let key = entry.key();
        let value = encode_value(&key, ENTRY_VALUE_VERSION, entry)?;
        self.cas(lease, &key, None, value).await
    }

    /// Changes entry `seq` with `change` (its `seq` and `id` are kept) and
    /// returns the entry as written.
    pub async fn update(
        &self,
        lease: &MigrationLease,
        seq: u32,
        change: impl FnOnce(&mut LedgerEntry),
    ) -> Result<LedgerEntry> {
        let prefix = format!("{LEDGER_PREFIX}{seq:08}-");
        let found = self
            .meta
            .pointers_with_prefix(Consistency::Linearizable, self.namespace, &prefix)
            .await?;
        let Some((key, pointer)) = found.into_iter().next() else {
            return Err(Error::NoEntry { seq });
        };
        let mut entry = decode_entry(&key, &pointer.value)?;
        let (keep_seq, keep_id) = (entry.seq, entry.id.clone());
        change(&mut entry);
        entry.seq = keep_seq;
        entry.id = keep_id;
        check_cursor(&entry)?;
        let value = encode_value(&key, ENTRY_VALUE_VERSION, &entry)?;
        self.cas(lease, &key, Some(pointer.version), value).await?;
        Ok(entry)
    }

    /// The stack record, if one was written.
    pub async fn stack(&self) -> Result<Option<StackRecord>> {
        let Some(pointer) = self.stack_pointer().await? else {
            return Ok(None);
        };
        let bytes = decode_bytes(STACK_KEY, &pointer.value)?;
        StackRecord::decode(&bytes)
            .map(Some)
            .map_err(|e| Error::Corrupt {
                key: STACK_KEY.to_string(),
                why: e.to_string(),
            })
    }

    /// Writes the stack record.
    pub async fn set_stack(&self, lease: &MigrationLease, record: &StackRecord) -> Result<()> {
        let text = URL_SAFE_NO_PAD.encode(record.encode());
        if text.len() > MAX_KEY_LEN {
            return Err(Error::TooLarge {
                key: STACK_KEY.to_string(),
                len: text.len(),
            });
        }
        let current = self.stack_pointer().await?.map(|p| p.version);
        self.cas(lease, STACK_KEY, current, text).await
    }

    /// Marks entry `seq` `Done` at `now`, then writes `record`: the stack
    /// record is the commit point of a migrate run (ruling T0-2).
    pub async fn finish(
        &self,
        lease: &MigrationLease,
        seq: u32,
        now: SystemTime,
        record: &StackRecord,
    ) -> Result<()> {
        self.update(lease, seq, |e| {
            e.state = LedgerState::Done;
            e.finished_at = Some(now);
            e.error = None;
        })
        .await?;
        self.set_stack(lease, record).await
    }

    /// Whether every entry for `release` is `Done` but the stack record does
    /// not name it yet: a migrate run that stopped between its last entry and
    /// its commit point. A rerun only writes the stack record.
    pub async fn awaiting_stack(&self, release: &str) -> Result<bool> {
        let entries = self.entries().await?;
        let mut to_release = entries
            .iter()
            .filter(|e| e.release_to == release)
            .peekable();
        if to_release.peek().is_none() {
            return Ok(false);
        }
        let all_done = to_release.all(|e| e.state == LedgerState::Done);
        let stacked = self.stack().await?.is_some_and(|s| s.release == release);
        Ok(all_done && !stacked)
    }

    async fn stack_pointer(&self) -> Result<Option<Pointer>> {
        Ok(self
            .meta
            .pointer(Consistency::Linearizable, self.namespace, STACK_KEY)
            .await?)
    }

    async fn cas(
        &self,
        lease: &MigrationLease,
        key: &str,
        expected: Option<u64>,
        value: String,
    ) -> Result<()> {
        let cas = PointerCas {
            namespace: self.namespace,
            key: key.to_string(),
            expected,
            value: value.clone(),
            fence: Some(lease.fence()),
            fresh: None,
        };
        match self.meta.cas_pointer(cas).await.into_result() {
            Ok(_) => Ok(()),
            Err(MetaError::Rejected(ApplyError::Fenced { .. })) => Err(Error::Fenced),
            // A retry after a lost acknowledgement finds its own value.
            Err(MetaError::Rejected(ApplyError::VersionMismatch {
                current: Some(current),
            })) if current.value == value => Ok(()),
            Err(MetaError::Rejected(ApplyError::VersionMismatch { .. })) => Err(Error::Conflict {
                key: key.to_string(),
            }),
            Err(e) => Err(e.into()),
        }
    }
}

fn check_cursor(entry: &LedgerEntry) -> Result<()> {
    match &entry.cursor {
        Some(cursor) if cursor.len() > MAX_CURSOR => {
            Err(Error::CursorTooLong { len: cursor.len() })
        }
        _ => Ok(()),
    }
}
