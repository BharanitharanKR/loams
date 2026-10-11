//! Object databases (§36 §5.3; plan GT1 Task 9, old GT1 Task 5): object
//! lookup over a repository's pack set, with fork fall-through (§36 §4.7).
//!
//! [`ObjectDb`] is what readers need: whether an object is held, its kind
//! and bytes (deltas resolved), and the connectivity check of the receive
//! path (§36 §6.3 step 3). [`PackCacheOdb`] implements it by downloading
//! whole pack sections into a [`PackCache`]; it serves the helper,
//! compaction and tests. `RangeOdb` (Task 17) serves Smart HTTP with range
//! reads.

mod pack_cache;

use std::collections::{HashSet, VecDeque};
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use gix_object::Kind;

use crate::blob::BlobError;
use crate::ids::ObjectId;

pub use pack_cache::{PackCache, PackCacheOdb};

/// Errors of an [`ObjectDb`].
#[derive(Debug, thiserror::Error)]
pub enum OdbError {
    /// No pack in the lookup chain holds the object.
    #[error("object {0} not found")]
    NotFound(ObjectId),
    /// The fork chain is deeper than `MAX_FORK_DEPTH` (8) parents.
    #[error("fork chain is deeper than {max} parents")]
    ForkTooDeep {
        /// The deepest chain served.
        max: usize,
    },
    /// The parent offered does not match the snapshot's fork parent.
    #[error("fork parent mismatch: {reason}")]
    ParentMismatch {
        /// What does not match.
        reason: String,
    },
    /// A stored pack section that cannot be read as the `PackRef` says.
    #[error("corrupt pack {pack}: {reason}")]
    CorruptPack {
        /// The pack checksum.
        pack: String,
        /// What is wrong.
        reason: String,
    },
    /// An object whose bytes do not parse as its kind.
    #[error("corrupt object {oid}: {reason}")]
    CorruptObject {
        /// The object.
        oid: ObjectId,
        /// What is wrong.
        reason: String,
    },
    /// The blob store failed.
    #[error(transparent)]
    Blob(#[from] BlobError),
}

/// Object lookup shared by the pack-cache `Odb` (Task 9) and `RangeOdb`
/// (Task 17) (plan GT1 shared contracts).
#[async_trait]
pub trait ObjectDb: Send + Sync + fmt::Debug {
    /// Whether the object is held.
    async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError>;
    /// The object's kind and bytes, deltas resolved; `NotFound` when absent.
    async fn read(&self, oid: &ObjectId) -> Result<(Kind, Bytes), OdbError>;
    /// The connectivity check (§36 §6.3 step 3): walks commits, trees and
    /// tags from `tips` and returns the objects found in neither this
    /// database nor `extra` (the pushed pack), in the order they were met.
    /// A missing object is not descended into; submodule commits in trees
    /// are not followed.
    async fn missing_from_closure(
        &self,
        tips: &[ObjectId],
        extra: Option<&dyn ObjectDb>,
    ) -> Result<Vec<ObjectId>, OdbError>;
}

/// What the closure walk needs from a database: existence, and the bytes
/// of objects it descends into.
#[async_trait]
pub(crate) trait Lookup: Send + Sync {
    async fn has(&self, oid: &ObjectId) -> Result<bool, OdbError>;
    async fn get(&self, oid: &ObjectId) -> Result<Option<(Kind, Bytes)>, OdbError>;
}

/// An [`ObjectDb`] seen as a [`Lookup`].
pub(crate) struct DbLookup<'a>(pub(crate) &'a dyn ObjectDb);

#[async_trait]
impl Lookup for DbLookup<'_> {
    async fn has(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        self.0.contains(oid).await
    }

    async fn get(&self, oid: &ObjectId) -> Result<Option<(Kind, Bytes)>, OdbError> {
        match self.0.read(oid).await {
            Ok(found) => Ok(Some(found)),
            Err(OdbError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

/// The objects reachable from `tips` that neither `primary` nor `extra`
/// holds (see [`ObjectDb::missing_from_closure`]). `extra` is asked first:
/// it is the small pushed pack. A blob named by a tree is only checked for
/// existence, never read.
pub(crate) async fn walk_closure(
    primary: &dyn Lookup,
    extra: Option<&dyn Lookup>,
    tips: &[ObjectId],
) -> Result<Vec<ObjectId>, OdbError> {
    let hash = gix_hash::Kind::Sha1;
    let corrupt = |oid: ObjectId, err: &dyn fmt::Display| OdbError::CorruptObject {
        oid,
        reason: err.to_string(),
    };
    let mut seen: HashSet<ObjectId> = HashSet::new();
    // `true`: a tree entry says the object is a blob.
    let mut queue: VecDeque<(ObjectId, bool)> = tips.iter().map(|tip| (*tip, false)).collect();
    let mut missing = Vec::new();
    while let Some((oid, blob)) = queue.pop_front() {
        if !seen.insert(oid) {
            continue;
        }
        if blob {
            let held = match extra {
                Some(extra) if extra.has(&oid).await? => true,
                _ => primary.has(&oid).await?,
            };
            if !held {
                missing.push(oid);
            }
            continue;
        }
        let found = match extra {
            Some(extra) => match extra.get(&oid).await? {
                Some(found) => Some(found),
                None => primary.get(&oid).await?,
            },
            None => primary.get(&oid).await?,
        };
        let Some((kind, data)) = found else {
            missing.push(oid);
            continue;
        };
        match kind {
            Kind::Blob => {}
            Kind::Commit => {
                let commit = gix_object::CommitRef::from_bytes(&data, hash)
                    .map_err(|err| corrupt(oid, &err))?;
                queue.push_back((commit.tree(), false));
                queue.extend(commit.parents().map(|parent| (parent, false)));
            }
            Kind::Tree => {
                let tree = gix_object::TreeRef::from_bytes(&data, hash)
                    .map_err(|err| corrupt(oid, &err))?;
                for entry in &tree.entries {
                    if entry.mode.is_commit() {
                        // A submodule's commit lives in another repository.
                        continue;
                    }
                    queue.push_back((entry.oid.to_owned(), !entry.mode.is_tree()));
                }
            }
            Kind::Tag => {
                let tag = gix_object::TagRef::from_bytes(&data, hash)
                    .map_err(|err| corrupt(oid, &err))?;
                queue.push_back((tag.target(), tag.target_kind == Kind::Blob));
            }
        }
    }
    Ok(missing)
}
