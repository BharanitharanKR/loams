//! `RestoreSalvage` (§36 §19.4.3, plan GT1 Task 25): a fenced writer's
//! unsaved state replayed as edits on the workspace's current snapshot.
//!
//! A salvage record holds the snapshot the fenced writer could not save
//! (number `n`); it was built on snapshot `n - 1`, its base. The changes
//! from the base to the salvaged state are replayed on the current
//! snapshot, path by path, for the worktree and the index alike:
//!
//! - where the current snapshot still has the base's entry, the salvaged
//!   entry is written (or the path removed);
//! - where it already has the salvaged entry, nothing changes;
//! - anywhere else both sides changed the path: the current entry stays,
//!   and the path is recorded as a [`pb::Conflict`] (base, ours = current,
//!   theirs = salvaged), as §19.4.3 says. No content merge is attempted
//!   (`Rebase`'s `merge3` is Task 26's).

use std::collections::BTreeMap;

use bytes::Bytes;
use gix_object::Kind;

use super::tree_build::{Edit, FileMode, TreeBuildError, TreeBuilder};
use crate::format::pb;
use crate::history::{Pathspec, TreeReader, diff_trees};
use crate::ids::{ObjectId, RepoPath};
use crate::odb::{OdbError, RangeOdb};

/// Errors of [`replay_salvage`].
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// A snapshot names something that is not an object id, or a path
    /// that cannot be edited.
    #[error("{0}")]
    Invalid(String),
    /// Reading trees failed.
    #[error(transparent)]
    Odb(#[from] OdbError),
    /// Building the new trees failed.
    #[error(transparent)]
    Build(#[from] TreeBuildError),
}

/// What [`replay_salvage`] answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replayed {
    /// The current worktree with the salvaged changes.
    pub worktree_tree: ObjectId,
    /// The current index with the salvaged changes.
    pub index_tree: ObjectId,
    /// The trees the replay made, to store in a workspace pack.
    pub objects: Vec<(Kind, Bytes)>,
    /// The paths both sides changed, by path (a worktree conflict before
    /// an index conflict at the same path).
    pub conflicts: Vec<pb::Conflict>,
    /// How many path changes were written.
    pub applied: usize,
}

/// Replays the changes from `base` to `salvaged` on `current` (see the
/// module docs). `odb` reads every tree the three snapshots name: the
/// repository and the workspace packs of both `current` and `salvaged`.
pub async fn replay_salvage(
    odb: &RangeOdb,
    base: &pb::WorkspaceSnapshot,
    salvaged: &pb::WorkspaceSnapshot,
    current: &pb::WorkspaceSnapshot,
) -> Result<Replayed, ReplayError> {
    let reader = TreeReader::new(odb);
    let tree = |field: &str, bytes: &[u8]| {
        ObjectId::try_from(bytes)
            .map_err(|_| ReplayError::Invalid(format!("{field} is not a 20-byte object id")))
    };
    let mut out = Replayed {
        worktree_tree: empty_tree(),
        index_tree: empty_tree(),
        objects: Vec::new(),
        conflicts: Vec::new(),
        applied: 0,
    };
    let mut conflicts: BTreeMap<String, pb::Conflict> = BTreeMap::new();
    for (which, base_tree, salvaged_tree, current_tree) in [
        (
            "worktree_tree",
            &base.worktree_tree,
            &salvaged.worktree_tree,
            &current.worktree_tree,
        ),
        (
            "index_tree",
            &base.index_tree,
            &salvaged.index_tree,
            &current.index_tree,
        ),
    ] {
        let (root, objects, applied) = replay_tree(
            &reader,
            odb,
            tree(which, base_tree)?,
            tree(which, salvaged_tree)?,
            tree(which, current_tree)?,
            &mut conflicts,
        )
        .await?;
        match which {
            "worktree_tree" => out.worktree_tree = root,
            _ => out.index_tree = root,
        }
        out.objects.extend(objects);
        out.applied += applied;
    }
    out.conflicts = conflicts.into_values().collect();
    Ok(out)
}

fn empty_tree() -> ObjectId {
    ObjectId::empty_tree(gix_hash::Kind::Sha1)
}

/// `None` for the empty tree, which no pack holds.
fn stored(tree: ObjectId) -> Option<ObjectId> {
    (tree != empty_tree()).then_some(tree)
}

/// One tree's replay: its new root, the trees it made, and the changes it
/// wrote; conflicts go to `conflicts`, keyed by path (the first kept).
async fn replay_tree(
    reader: &TreeReader<'_>,
    odb: &RangeOdb,
    base: ObjectId,
    salvaged: ObjectId,
    current: ObjectId,
    conflicts: &mut BTreeMap<String, pb::Conflict>,
) -> Result<(ObjectId, Vec<(Kind, Bytes)>, usize), ReplayError> {
    let changes = diff_trees(
        reader,
        stored(base),
        stored(salvaged),
        &Pathspec::all(),
        None,
    )
    .await?;
    let mut edits = Vec::new();
    for change in changes {
        let path = String::from_utf8(change.path)
            .map_err(|_| ReplayError::Invalid("a path that is not UTF-8".into()))?;
        let now = match stored(current) {
            Some(root) => reader.entry_at(&root, path.as_bytes()).await?,
            None => None,
        };
        // A directory where the base had a file (or nothing) matches
        // neither side: a conflict, so the other writer's directory stays.
        if now == change.new {
            continue;
        }
        if now == change.old {
            let edit = match change.new {
                Some((mode, oid)) => Edit::Write {
                    mode: FileMode::from_bits(mode).ok_or_else(|| {
                        ReplayError::Invalid(format!("{path:?} has mode {mode:o}"))
                    })?,
                    oid,
                },
                None => Edit::Remove,
            };
            let path =
                RepoPath::parse(&path).map_err(|err| ReplayError::Invalid(err.to_string()))?;
            edits.push((path, edit));
            continue;
        }
        let bytes = |entry: Option<(u32, ObjectId)>| {
            entry
                .map(|(_, oid)| oid.as_bytes().to_vec())
                .unwrap_or_default()
        };
        let mode = change
            .new
            .or(now)
            .or(change.old)
            .map_or(0, |(mode, _)| mode);
        conflicts.entry(path.clone()).or_insert(pb::Conflict {
            path,
            base: bytes(change.old),
            ours: bytes(now),
            theirs: bytes(change.new),
            mode,
        });
    }
    let applied = edits.len();
    if edits.is_empty() {
        return Ok((current, Vec::new(), 0));
    }
    let (root, trees) = TreeBuilder::apply(current, edits, odb).await?;
    let objects = trees
        .into_iter()
        .map(|(_, body)| (Kind::Tree, body))
        .collect();
    Ok((root, objects, applied))
}
