//! What `SaveSnapshot` checks before it appends a snapshot (plan GT1 Task
//! 25, §36 §19.5.3): the objects it names exist, in the repository or in
//! the workspace packs it names, and the trees are connected.

use std::collections::HashSet;

use gix_object::Kind;

use crate::format::pb;
use crate::ids::ObjectId;
use crate::objects::kind_name;
use crate::odb::{Location, ObjectDb, OdbError, RangeOdb};

/// git's mode of a tree entry that is a submodule commit, which lives in
/// another repository.
const GITLINK_MODE: u16 = 0o160000;

/// Why a snapshot's objects are refused.
#[derive(Debug, thiserror::Error)]
pub enum SaveCheckError {
    /// A field that must hold an object id does not.
    #[error("{field} is not a 20-byte object id")]
    Invalid {
        /// The field.
        field: &'static str,
    },
    /// An object the snapshot needs is in neither the repository nor its
    /// workspace packs (`object_missing`).
    #[error("{what} {oid} is not in the repository or the workspace's packs")]
    Missing {
        /// What names it: a field, or `entry of <tree>`.
        what: String,
        /// The object.
        oid: ObjectId,
    },
    /// An object of the wrong kind where a tree or commit is named.
    #[error("{field} {oid} is a {found}, not a {expected}")]
    WrongKind {
        /// The field.
        field: &'static str,
        /// The object.
        oid: ObjectId,
        /// The kind it must be.
        expected: &'static str,
        /// The kind it is.
        found: &'static str,
    },
    /// Reading objects failed.
    #[error(transparent)]
    Odb(#[from] OdbError),
}

/// The object id in snapshot field `field`: `None` when empty and
/// `optional`.
fn field_oid(
    field: &'static str,
    bytes: &[u8],
    optional: bool,
) -> Result<Option<ObjectId>, SaveCheckError> {
    if bytes.is_empty() && optional {
        return Ok(None);
    }
    ObjectId::try_from(bytes)
        .map(Some)
        .map_err(|_| SaveCheckError::Invalid { field })
}

/// Checks the objects of `snap` against `odb`, the repository with the
/// workspace packs `snap` names searched last (`objects::workspace_odb`):
/// `worktree_tree` and `index_tree` are trees, `head` (unless the workspace
/// is unborn) and `snapshot_commit` are commits, and every tree reachable
/// from the two trees through the workspace's own packs names only objects
/// that exist. Trees in the repository's packs are connected already
/// (pushes are), so the walk stops there and costs reads of the
/// workspace's new trees only. The empty tree needs no object.
pub async fn check_save_objects(
    odb: &RangeOdb,
    snap: &pb::WorkspaceSnapshot,
) -> Result<(), SaveCheckError> {
    let worktree = field_oid("worktree_tree", &snap.worktree_tree, false)?.expect("required");
    let index = field_oid("index_tree", &snap.index_tree, false)?.expect("required");
    let head = field_oid("head", &snap.head, true)?;
    let commit = field_oid("snapshot_commit", &snap.snapshot_commit, false)?.expect("required");
    let ws_layer = odb.layers().saturating_sub(1);
    let mut walk = Vec::new();
    for (field, oid) in [("worktree_tree", worktree), ("index_tree", index)] {
        if oid == empty_tree() {
            continue;
        }
        let loc = expect_kind(odb, field, &oid, Kind::Tree).await?;
        if loc.layer == ws_layer {
            walk.push(oid);
        }
    }
    for (field, oid) in [("head", head), ("snapshot_commit", Some(commit))] {
        if let Some(oid) = oid {
            expect_kind(odb, field, &oid, Kind::Commit).await?;
        }
    }
    let mut seen: HashSet<ObjectId> = walk.iter().copied().collect();
    while let Some(tree) = walk.pop() {
        let (_, data) = odb.read(&tree).await?;
        let parsed =
            gix_object::TreeRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
                OdbError::CorruptObject {
                    oid: tree,
                    reason: err.to_string(),
                }
            })?;
        for entry in &parsed.entries {
            let mode = entry.mode.value();
            if mode == GITLINK_MODE {
                continue;
            }
            let oid = entry.oid.to_owned();
            if !seen.insert(oid) {
                continue;
            }
            let Some(loc) = odb.locate(&oid).await? else {
                return Err(SaveCheckError::Missing {
                    what: format!("an entry of tree {tree}"),
                    oid,
                });
            };
            if entry.mode.is_tree() && loc.layer == ws_layer {
                walk.push(oid);
            }
        }
    }
    Ok(())
}

/// The empty tree, which an unborn workspace starts from and no pack
/// needs to hold.
pub fn empty_tree() -> ObjectId {
    ObjectId::empty_tree(gix_hash::Kind::Sha1)
}

/// The tree of commit `oid` (`field` names it in errors): `Missing` when
/// absent, `WrongKind` when it is not a commit.
pub async fn commit_tree(
    odb: &RangeOdb,
    field: &'static str,
    oid: &ObjectId,
) -> Result<ObjectId, SaveCheckError> {
    let loc = expect_kind(odb, field, oid, Kind::Commit).await?;
    let (_, data) = odb.read_at(&loc).await?;
    let commit = gix_object::CommitRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
        OdbError::CorruptObject {
            oid: *oid,
            reason: err.to_string(),
        }
    })?;
    Ok(commit.tree())
}

/// Where `oid` is, refusing an absent object or one not of `kind`.
async fn expect_kind(
    odb: &RangeOdb,
    field: &'static str,
    oid: &ObjectId,
    kind: Kind,
) -> Result<Location, SaveCheckError> {
    let Some(loc) = odb.locate(oid).await? else {
        return Err(SaveCheckError::Missing {
            what: field.to_owned(),
            oid: *oid,
        });
    };
    let found = odb.header(&loc).await?.kind;
    if found != kind {
        return Err(SaveCheckError::WrongKind {
            field,
            oid: *oid,
            expected: kind_name(kind),
            found: kind_name(found),
        });
    }
    Ok(loc)
}
