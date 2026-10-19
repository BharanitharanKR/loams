//! Mount-less editing (§36 §19.10, §19.11, D834; plan GT1 Task 26): what
//! `ReadFiles`, `ApplyEdits`, `Diff` and `Rebase` do to a workspace's
//! trees, without a transport.
//!
//! - [`read_file`] reads a path of a snapshot's worktree, rendering `env/`
//!   files through [`EnvHooks`] (Task 62; [`NoEnv`] redacts them).
//! - [`apply_edits`] applies writes, deletes and renames to the worktree
//!   (and, when staging, the index) with [`TreeBuilder`], sealing what is
//!   written under `env/` through [`EnvHooks`] ([`NoEnv`] refuses it).
//! - [`file_patch`] prints one changed path as `git diff` does
//!   ([`crate::diff`]).
//! - [`rebase_trees`] moves a workspace's changes from its base onto a new
//!   commit: only paths both sides changed are read, and those are merged
//!   with [`merge3`]; what does not merge cleanly becomes a conflict.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use gix_object::Kind;

use super::merge3::{MergeLabels, merge3};
use super::tree_build::{Edit, FileMode, TreeBuildError, TreeBuilder};
use crate::diff::{DiffSide, write_file_patch};
use crate::format::pb;
use crate::history::{Change, Pathspec, TreeReader, diff_trees};
use crate::ids::{ObjectId, RepoPath};
use crate::limits::MAX_EDITS;
use crate::odb::{ObjectDb, OdbError, RangeOdb};

/// git's mode of a tree entry that is a tree.
const TREE_MODE: u32 = 0o040000;
/// Bytes git looks at for a NUL to call a file binary.
const BINARY_PROBE: usize = 8000;
/// Lines of context in a [`file_patch`], as `git diff`'s default.
pub const DIFF_CONTEXT: usize = 3;

/// Errors of mount-less editing.
#[derive(Debug, thiserror::Error)]
pub enum EditError {
    /// An edit that cannot be applied; `index` is its position in the
    /// request.
    #[error("edit {index}: {reason}")]
    Invalid {
        /// The edit's position.
        index: usize,
        /// What is wrong.
        reason: String,
    },
    /// More edits than one call takes.
    #[error("{got} edits, more than the {max} one call takes")]
    TooMany {
        /// How many were offered.
        got: usize,
        /// The most taken (`MAX_EDITS`).
        max: usize,
    },
    /// A write under `env/` that could not be sealed (`env_unsealed`).
    #[error(transparent)]
    Env(#[from] EnvRefusal),
    /// A snapshot field that is not an object id.
    #[error("the snapshot's {0} is not a 20-byte object id")]
    Snapshot(&'static str),
    /// Reading objects failed.
    #[error(transparent)]
    Odb(#[from] OdbError),
    /// Building trees failed.
    #[error(transparent)]
    Build(#[from] TreeBuildError),
}

/// A refused `env/` write (`env_unsealed`, §36 §19.13.2).
#[derive(Debug, thiserror::Error)]
#[error("env file {path} is not sealed: {reason}")]
pub struct EnvRefusal {
    /// The path.
    pub path: String,
    /// Why.
    pub reason: String,
}

/// An `env/` file as a reader may see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendered {
    /// The contents shown.
    pub data: Bytes,
    /// Whether values were withheld.
    pub redacted: bool,
}

/// Sealing and rendering of `env/` files (§36 §19.13; Task 62 implements
/// it over the vault, Task 63 enforces it).
#[async_trait]
pub trait EnvHooks: Send + Sync + fmt::Debug {
    /// Whether `path` is an env file. Until Task 61's policy, every path
    /// under `env/`.
    fn is_env(&self, path: &RepoPath) -> bool {
        is_env_path(path)
    }

    /// The sealed form of `plaintext` written at env path `path`, or a
    /// refusal.
    async fn seal(&self, path: &RepoPath, plaintext: Bytes) -> Result<Bytes, EnvRefusal>;

    /// Env file `path` (as stored) as the reader may see it.
    async fn render(&self, path: &RepoPath, stored: Bytes) -> Rendered;
}

/// Whether `path` is under the repository's `env/` directory.
pub fn is_env_path(path: &RepoPath) -> bool {
    path.as_str().starts_with("env/")
}

/// The hooks until Task 62: nothing is sealed, so a write under `env/` is
/// refused, and every value an env file shows is redacted.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoEnv;

#[async_trait]
impl EnvHooks for NoEnv {
    async fn seal(&self, path: &RepoPath, _plaintext: Bytes) -> Result<Bytes, EnvRefusal> {
        Err(EnvRefusal {
            path: path.to_string(),
            reason: "this server does not seal env files yet; write them through a mount".into(),
        })
    }

    async fn render(&self, _path: &RepoPath, stored: Bytes) -> Rendered {
        Rendered {
            data: redact(&stored),
            redacted: true,
        }
    }
}

/// Every `KEY=value` line as `KEY=<redacted>` (§36 §19.13.7's form without
/// the fingerprint, which Task 62 adds); comments and blank lines stay.
fn redact(stored: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(stored.len());
    for line in stored.split_inclusive(|b| *b == b'\n') {
        let body = line.strip_suffix(b"\n").unwrap_or(line);
        let trimmed = body.trim_ascii_start();
        match body.iter().position(|b| *b == b'=') {
            Some(eq) if !trimmed.starts_with(b"#") => {
                out.extend_from_slice(&body[..=eq]);
                out.extend_from_slice(b"<redacted>");
            }
            _ => out.extend_from_slice(body),
        }
        if line.ends_with(b"\n") {
            out.push(b'\n');
        }
    }
    Bytes::from(out)
}

/// One path read from a tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRead {
    /// The mode and object at the path; `None` when nothing is there.
    pub entry: Option<(u32, ObjectId)>,
    /// The contents of a file or symlink (a symlink's target); `None` for
    /// a directory, a submodule, or nothing.
    pub data: Option<Bytes>,
    /// Whether an env file's values were withheld.
    pub redacted: bool,
}

/// Path `path` of tree `tree` (`ReadFiles`).
pub async fn read_file(
    reader: &TreeReader<'_>,
    tree: ObjectId,
    path: &RepoPath,
    env: &dyn EnvHooks,
) -> Result<FileRead, EditError> {
    let entry = entry_of(reader, tree, path).await?;
    let mut read = FileRead {
        entry,
        data: None,
        redacted: false,
    };
    let Some((mode, oid)) = entry else {
        return Ok(read);
    };
    if mode == TREE_MODE || mode == FileMode::Gitlink.bits() {
        return Ok(read);
    }
    let (_, data) = reader.odb().read(&oid).await?;
    if env.is_env(path) {
        let rendered = env.render(path, data).await;
        read.data = Some(rendered.data);
        read.redacted = rendered.redacted;
    } else {
        read.data = Some(data);
    }
    Ok(read)
}

/// The entry at `path` below `tree` (the root itself for the empty path).
async fn entry_of(
    reader: &TreeReader<'_>,
    tree: ObjectId,
    path: &RepoPath,
) -> Result<Option<(u32, ObjectId)>, EditError> {
    if path.as_str().is_empty() {
        return Ok(Some((TREE_MODE, tree)));
    }
    if tree == empty_tree() {
        return Ok(None);
    }
    Ok(reader.entry_at(&tree, path.as_str().as_bytes()).await?)
}

/// One change `ApplyEdits` makes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileEdit {
    /// Writes a file (or a symlink, whose contents are its target).
    Write {
        /// The path.
        path: RepoPath,
        /// Its mode: not a submodule.
        mode: FileMode,
        /// Its contents.
        data: Bytes,
    },
    /// Deletes a file, or a directory and everything below it.
    Delete {
        /// The path.
        path: RepoPath,
    },
    /// Moves a file (or symlink) to a new path, replacing what is there.
    Rename {
        /// The file moved.
        from: RepoPath,
        /// Where it goes.
        to: RepoPath,
    },
}

/// What [`apply_edits`] made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Edited {
    /// The new worktree.
    pub worktree_tree: ObjectId,
    /// The new index: the old one unless the edits were staged.
    pub index_tree: ObjectId,
    /// The new blobs and trees, for a workspace pack.
    pub objects: Vec<(Kind, Bytes)>,
    /// Every path an edit wrote or removed, in order.
    pub touched: Vec<RepoPath>,
}

/// Applies `edits`, in order, to `snap`'s worktree, and to its index too
/// when `stage` (as `git add -A` of the touched paths). `odb` reads the
/// repository and the workspace's packs.
pub async fn apply_edits(
    odb: &RangeOdb,
    snap: &pb::WorkspaceSnapshot,
    edits: Vec<FileEdit>,
    stage: bool,
    env: &dyn EnvHooks,
) -> Result<Edited, EditError> {
    if edits.len() > MAX_EDITS {
        return Err(EditError::TooMany {
            got: edits.len(),
            max: MAX_EDITS,
        });
    }
    let worktree = tree_field(&snap.worktree_tree, "worktree_tree")?;
    let index = tree_field(&snap.index_tree, "index_tree")?;
    let reader = TreeReader::new(odb);
    // Paths the edits have touched so far, and what each holds now.
    let mut now: BTreeMap<String, Option<(u32, ObjectId)>> = BTreeMap::new();
    let mut tree_edits: Vec<(RepoPath, Edit)> = Vec::new();
    let mut objects: Vec<(Kind, Bytes)> = Vec::new();
    let mut touched = Vec::new();
    for (index_of, edit) in edits.into_iter().enumerate() {
        let invalid = |reason: String| EditError::Invalid {
            index: index_of,
            reason,
        };
        match edit {
            FileEdit::Write { path, mode, data } => {
                if path.as_str().is_empty() {
                    return Err(invalid("a file cannot be written at the root".into()));
                }
                if mode == FileMode::Gitlink {
                    return Err(invalid("submodules are not written by ApplyEdits".into()));
                }
                let data = if env.is_env(&path) {
                    env.seal(&path, data).await?
                } else {
                    data
                };
                let oid = blob_id(&data);
                objects.push((Kind::Blob, data));
                now.insert(path.to_string(), Some((mode.bits(), oid)));
                tree_edits.push((path.clone(), Edit::Write { mode, oid }));
                touched.push(path);
            }
            FileEdit::Delete { path } => {
                if path.as_str().is_empty() {
                    return Err(invalid("the root cannot be deleted".into()));
                }
                if current(&reader, worktree, &now, &path).await?.is_none() {
                    return Err(invalid(format!("nothing at {path}")));
                }
                now.insert(path.to_string(), None);
                tree_edits.push((path.clone(), Edit::Remove));
                touched.push(path);
            }
            FileEdit::Rename { from, to } => {
                if from == to || to.as_str().is_empty() {
                    return Err(invalid("a rename needs a new path".into()));
                }
                let Some((mode, oid)) = current(&reader, worktree, &now, &from).await? else {
                    return Err(invalid(format!("nothing at {from}")));
                };
                let Some(file_mode) = FileMode::from_bits(mode).filter(|m| *m != FileMode::Gitlink)
                else {
                    return Err(invalid(format!(
                        "{from} is not a file; rename a directory's files one by one"
                    )));
                };
                let oid = if env.is_env(&to) && !env.is_env(&from) {
                    let (_, data) = odb.read(&oid).await?;
                    let sealed = env.seal(&to, data).await?;
                    let sealed_id = blob_id(&sealed);
                    objects.push((Kind::Blob, sealed));
                    sealed_id
                } else {
                    oid
                };
                now.insert(from.to_string(), None);
                now.insert(to.to_string(), Some((mode, oid)));
                tree_edits.push((from.clone(), Edit::Remove));
                tree_edits.push((
                    to.clone(),
                    Edit::Write {
                        mode: file_mode,
                        oid,
                    },
                ));
                touched.push(from);
                touched.push(to);
            }
        }
    }
    let (worktree_tree, trees) = TreeBuilder::apply(worktree, tree_edits.clone(), odb).await?;
    objects.extend(trees.into_iter().map(|(_, body)| (Kind::Tree, body)));
    let index_tree = if stage {
        let (root, trees) = TreeBuilder::apply(index, tree_edits, odb).await?;
        objects.extend(trees.into_iter().map(|(_, body)| (Kind::Tree, body)));
        root
    } else {
        index
    };
    Ok(Edited {
        worktree_tree,
        index_tree,
        objects,
        touched,
    })
}

/// What `path` holds after the edits so far: an edit's result, nothing
/// when an edit removed or replaced a directory above it, else the tree's.
async fn current(
    reader: &TreeReader<'_>,
    tree: ObjectId,
    now: &BTreeMap<String, Option<(u32, ObjectId)>>,
    path: &RepoPath,
) -> Result<Option<(u32, ObjectId)>, EditError> {
    let path_str = path.as_str();
    if let Some(entry) = now.get(path_str) {
        return Ok(*entry);
    }
    for (at, _) in path_str.match_indices('/') {
        if now.contains_key(&path_str[..at]) {
            // Removed, or a file now: nothing below it.
            return Ok(None);
        }
    }
    entry_of(reader, tree, path).await
}

/// A snapshot's tree field.
fn tree_field(bytes: &[u8], field: &'static str) -> Result<ObjectId, EditError> {
    ObjectId::try_from(bytes).map_err(|_| EditError::Snapshot(field))
}

fn blob_id(data: &[u8]) -> ObjectId {
    gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, data)
        .expect("hashing a blob in memory does not fail")
}

fn empty_tree() -> ObjectId {
    ObjectId::empty_tree(gix_hash::Kind::Sha1)
}

/// `None` for the empty tree, which no pack holds.
fn stored(tree: ObjectId) -> Option<ObjectId> {
    (tree != empty_tree()).then_some(tree)
}

/// The changed paths from tree `old` to tree `new`, restricted to `spec`.
pub async fn changed_paths(
    reader: &TreeReader<'_>,
    old: ObjectId,
    new: ObjectId,
    spec: &Pathspec,
) -> Result<Vec<Change>, EditError> {
    Ok(diff_trees(reader, stored(old), stored(new), spec, None).await?)
}

/// One changed path's patch, as `git diff` prints it.
pub async fn file_patch(odb: &RangeOdb, change: &Change) -> Result<Vec<u8>, EditError> {
    let read = |side: Option<(u32, ObjectId)>| async move {
        match side {
            Some((mode, oid)) if mode != FileMode::Gitlink.bits() => {
                Ok::<_, OdbError>(Some(odb.read(&oid).await?.1))
            }
            _ => Ok(None),
        }
    };
    let before = read(change.old).await?;
    let after = read(change.new).await?;
    fn side(entry: Option<(u32, ObjectId)>, data: &Option<Bytes>) -> Option<DiffSide<'_>> {
        entry.map(|(mode, oid)| DiffSide {
            mode,
            oid,
            data: data.as_deref().unwrap_or_default(),
        })
    }
    let mut out = Vec::new();
    write_file_patch(
        &change.path,
        side(change.old, &before),
        side(change.new, &after),
        DIFF_CONTEXT,
        &mut out,
    );
    Ok(out)
}

/// What [`rebase_trees`] made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rebased {
    /// The worktree on the new base.
    pub worktree_tree: ObjectId,
    /// The index on the new base.
    pub index_tree: ObjectId,
    /// The merged blobs and new trees, for a workspace pack.
    pub objects: Vec<(Kind, Bytes)>,
    /// The paths that did not merge cleanly (a worktree conflict before
    /// an index conflict at the same path): base, ours (the workspace's)
    /// and theirs (the new base's).
    pub conflicts: Vec<pb::Conflict>,
}

/// How one path both sides changed resolves.
#[derive(Clone, Debug)]
struct Resolution {
    entry: Option<(u32, ObjectId)>,
    conflict: bool,
}

/// Moves the changes from tree `base` to `worktree` and to `index` onto
/// tree `onto` (§36 §19.7.5's pull, for a workspace without local
/// commits). A path only the workspace changed takes its entry; one only
/// `onto` changed keeps `onto`'s; one both changed alike is left. Where
/// both changed a path differently, two text files are merged with
/// [`merge3`] (markers labelled `labels` where they conflict); anything
/// else (a deletion against a change, a binary file, a symlink, a
/// submodule, a change of type, a file against a directory) keeps the
/// workspace's entry, or the changed one against a deletion. Each is
/// recorded as a conflict, and the index keeps the workspace's entry
/// there. Only the paths both sides changed are read.
pub async fn rebase_trees(
    odb: &RangeOdb,
    base: ObjectId,
    onto: ObjectId,
    worktree: ObjectId,
    index: ObjectId,
    labels: MergeLabels<'_>,
) -> Result<Rebased, EditError> {
    let reader = TreeReader::new(odb);
    let upstream: HashMap<Vec<u8>, Change> = changed_paths(&reader, base, onto, &Pathspec::all())
        .await?
        .into_iter()
        .map(|change| (change.path.clone(), change))
        .collect();
    let mut merged: HashMap<MergeKey, Resolution> = HashMap::new();
    let mut objects = Vec::new();
    let mut conflicts: BTreeMap<String, pb::Conflict> = BTreeMap::new();
    let mut roots = [onto, onto];
    for (slot, side) in [worktree, index].into_iter().enumerate() {
        let mut edits = Vec::new();
        for change in changed_paths(&reader, base, side, &Pathspec::all()).await? {
            let path = String::from_utf8(change.path.clone())
                .map_err(|_| EditError::Snapshot("tree (a path that is not UTF-8)"))?;
            let theirs = match upstream.get(&change.path) {
                Some(up) if up.new == change.new => continue,
                Some(up) => up.new,
                None => {
                    // Untouched upstream as a file; a directory or a file
                    // above it may still clash.
                    if clashes(&reader, onto, &change.path).await? {
                        let theirs = entry_at(&reader, onto, &change.path).await?;
                        record(&mut conflicts, &path, &change, theirs);
                        continue;
                    }
                    edits.push((repo_path(&path)?, edit_of(change.new, &path)?));
                    continue;
                }
            };
            let key = (change.old, change.new, theirs);
            let resolution = match merged.get(&key) {
                Some(done) => done.clone(),
                None => {
                    let done = resolve(odb, key, labels, &mut objects).await?;
                    merged.insert(key, done.clone());
                    done
                }
            };
            // The worktree takes the merge (with markers); the index keeps the
            // workspace's entry at a path that conflicts.
            let entry = if resolution.conflict {
                record(&mut conflicts, &path, &change, theirs);
                if slot == 0 {
                    resolution.entry
                } else {
                    change.new
                }
            } else {
                resolution.entry
            };
            if entry != theirs {
                edits.push((repo_path(&path)?, edit_of(entry, &path)?));
            }
        }
        if !edits.is_empty() {
            let (root, trees) = TreeBuilder::apply(onto, edits, odb).await?;
            objects.extend(trees.into_iter().map(|(_, body)| (Kind::Tree, body)));
            roots[slot] = root;
        }
    }
    Ok(Rebased {
        worktree_tree: roots[0],
        index_tree: roots[1],
        objects,
        conflicts: conflicts.into_values().collect(),
    })
}

/// A path's base, workspace and upstream entries.
type MergeKey = (
    Option<(u32, ObjectId)>,
    Option<(u32, ObjectId)>,
    Option<(u32, ObjectId)>,
);

/// Resolves a path both sides changed differently (see [`rebase_trees`]);
/// a merged blob goes to `objects`.
async fn resolve(
    odb: &RangeOdb,
    (base, ours, theirs): MergeKey,
    labels: MergeLabels<'_>,
    objects: &mut Vec<(Kind, Bytes)>,
) -> Result<Resolution, EditError> {
    let keep = |entry| Resolution {
        entry,
        conflict: true,
    };
    if ours == base {
        return Ok(Resolution {
            entry: theirs,
            conflict: false,
        });
    }
    if theirs == base {
        return Ok(Resolution {
            entry: ours,
            conflict: false,
        });
    }
    let (Some((our_mode, our_oid)), Some((their_mode, their_oid))) = (ours, theirs) else {
        // A deletion against a change keeps the change.
        return Ok(keep(ours.or(theirs)));
    };
    let regular = |mode: u32| mode == 0o100644 || mode == 0o100755;
    if !regular(our_mode) || !regular(their_mode) || base.is_some_and(|(m, _)| !regular(m)) {
        return Ok(keep(ours));
    }
    let (mode, mode_conflict) = if our_mode == their_mode {
        (our_mode, false)
    } else {
        match base {
            Some((base_mode, _)) if base_mode == our_mode => (their_mode, false),
            Some((base_mode, _)) if base_mode == their_mode => (our_mode, false),
            _ => (our_mode, true),
        }
    };
    if our_oid == their_oid {
        return Ok(Resolution {
            entry: Some((mode, our_oid)),
            conflict: mode_conflict,
        });
    }
    let base_data = match base {
        Some((_, oid)) => odb.read(&oid).await?.1,
        None => Bytes::new(),
    };
    let our_data = odb.read(&our_oid).await?.1;
    let their_data = odb.read(&their_oid).await?.1;
    if [&base_data, &our_data, &their_data]
        .iter()
        .any(|data| data[..data.len().min(BINARY_PROBE)].contains(&0))
    {
        return Ok(keep(ours));
    }
    let result = merge3(&base_data, &our_data, &their_data, labels);
    let oid = blob_id(&result.content);
    objects.push((Kind::Blob, Bytes::from(result.content)));
    Ok(Resolution {
        entry: Some((mode, oid)),
        conflict: result.conflicts > 0 || mode_conflict,
    })
}

/// Whether writing the workspace's change at `path` onto `onto` would
/// replace a directory there or a file above it.
async fn clashes(reader: &TreeReader<'_>, onto: ObjectId, path: &[u8]) -> Result<bool, EditError> {
    if stored(onto).is_none() {
        return Ok(false);
    }
    if let Some((mode, _)) = reader.entry_at(&onto, path).await? {
        return Ok(mode == TREE_MODE);
    }
    for (at, byte) in path.iter().enumerate() {
        if *byte == b'/' {
            if let Some((mode, _)) = reader.entry_at(&onto, &path[..at]).await? {
                if mode != TREE_MODE {
                    return Ok(true);
                }
            } else {
                return Ok(false);
            }
        }
    }
    Ok(false)
}

async fn entry_at(
    reader: &TreeReader<'_>,
    tree: ObjectId,
    path: &[u8],
) -> Result<Option<(u32, ObjectId)>, EditError> {
    match stored(tree) {
        Some(root) => Ok(reader.entry_at(&root, path).await?),
        None => Ok(None),
    }
}

/// Records a conflict at `path` unless one is recorded already.
fn record(
    conflicts: &mut BTreeMap<String, pb::Conflict>,
    path: &str,
    change: &Change,
    theirs: Option<(u32, ObjectId)>,
) {
    let bytes = |entry: Option<(u32, ObjectId)>| {
        entry
            .map(|(_, oid)| oid.as_bytes().to_vec())
            .unwrap_or_default()
    };
    let mode = change
        .new
        .or(theirs)
        .or(change.old)
        .map_or(0, |(mode, _)| mode);
    conflicts.entry(path.to_owned()).or_insert(pb::Conflict {
        path: path.to_owned(),
        base: bytes(change.old),
        ours: bytes(change.new),
        theirs: bytes(theirs),
        mode,
    });
}

fn repo_path(path: &str) -> Result<RepoPath, EditError> {
    RepoPath::parse(path).map_err(|err| EditError::Invalid {
        index: 0,
        reason: err.to_string(),
    })
}

/// The tree edit that makes a path hold `entry`.
fn edit_of(entry: Option<(u32, ObjectId)>, path: &str) -> Result<Edit, EditError> {
    match entry {
        None => Ok(Edit::Remove),
        Some((mode, oid)) => Ok(Edit::Write {
            mode: FileMode::from_bits(mode).ok_or_else(|| EditError::Invalid {
                index: 0,
                reason: format!("{path:?} has mode {mode:o}"),
            })?,
            oid,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_keeps_keys_and_comments() {
        let out = redact(b"# db\nURL=postgres://u:p@h/db\n\nTOKEN=abc");
        assert_eq!(&out[..], b"# db\nURL=<redacted>\n\nTOKEN=<redacted>");
    }

    #[test]
    fn env_paths() {
        assert!(is_env_path(&RepoPath::parse("env/prod.env").expect("path")));
        assert!(!is_env_path(&RepoPath::parse("src/env/x").expect("path")));
        assert!(!is_env_path(&RepoPath::parse("env").expect("path")));
    }
}
