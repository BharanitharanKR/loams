//! Building trees from an overlay's edits (§36 §19.4.2, §19.5.3 step 4;
//! plan GT1 Task 24).
//!
//! [`TreeBuilder::apply`] takes a base tree and a list of path edits and
//! answers the new root tree and the tree objects it had to write. Only the
//! trees along changed paths are read and written: a change to `k` files at
//! depth `d` reads and writes about `k × d` trees, whatever the size of the
//! repository. Unchanged subtrees keep their object ids, so they are never
//! read.
//!
//! The result is what `git update-index` followed by `git write-tree` gives
//! for the same edits on an index read from the base tree: entries sorted in
//! git's tree order (a tree's name compares as if it ended in `/`), a
//! directory left empty disappears, writing a file where a directory was
//! replaces the directory (and the other way round), and removing a path
//! that does not exist changes nothing. The format was written from git's
//! documented tree format (`gitformat-loose(5)`, `git-update-index(1)`), not
//! its code.

use std::collections::BTreeMap;

use bytes::Bytes;
use gix_object::Kind;

use crate::ids::{ObjectId, RepoPath};
use crate::odb::{ObjectDb, OdbError};

/// git's mode of a tree entry that is a tree.
const TREE_MODE: u32 = 0o040000;

/// What a file entry is: git's four non-tree modes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileMode {
    /// A regular file, `100644`.
    Regular,
    /// An executable file, `100755`.
    Executable,
    /// A symbolic link, `120000`: the blob holds the target.
    Symlink,
    /// A submodule commit, `160000`.
    Gitlink,
}

impl FileMode {
    /// git's mode bits.
    pub fn bits(self) -> u32 {
        match self {
            Self::Regular => 0o100644,
            Self::Executable => 0o100755,
            Self::Symlink => 0o120000,
            Self::Gitlink => 0o160000,
        }
    }

    /// The mode of git's mode bits, or `None` for a tree or an unknown mode.
    pub fn from_bits(bits: u32) -> Option<Self> {
        match bits {
            0o100644 => Some(Self::Regular),
            0o100755 => Some(Self::Executable),
            0o120000 => Some(Self::Symlink),
            0o160000 => Some(Self::Gitlink),
            _ => None,
        }
    }
}

/// One change to a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edit {
    /// The path becomes a file (or a symlink, or a submodule commit) with
    /// this mode and object. A directory at the path is replaced, and a
    /// file where a parent directory should be is replaced by a directory.
    Write {
        /// The entry's mode.
        mode: FileMode,
        /// The blob (the commit, for a gitlink).
        oid: ObjectId,
    },
    /// The path, and everything below it when it is a directory, is
    /// removed. Removing a path that does not exist changes nothing.
    Remove,
}

/// Errors of [`TreeBuilder::apply`].
#[derive(Debug, thiserror::Error)]
pub enum TreeBuildError {
    /// An edit that cannot be applied, such as a write to the root.
    #[error("invalid edit of {path:?}: {reason}")]
    Invalid {
        /// The edit's path.
        path: String,
        /// What is wrong.
        reason: String,
    },
    /// A tree the edits pass through is not a tree in the database.
    #[error("{oid} is a {found}, not a tree")]
    NotATree {
        /// The object.
        oid: ObjectId,
        /// Its kind.
        found: &'static str,
    },
    /// Reading a tree failed, or a stored tree does not parse.
    #[error(transparent)]
    Odb(#[from] OdbError),
}

/// A directory being edited: its entries, with the subdirectories the edits
/// pass through read and held as [`Node::Dir`].
#[derive(Debug, Default)]
struct Dir {
    /// The tree this directory was read from; `None` for a new directory.
    base: Option<ObjectId>,
    entries: BTreeMap<Vec<u8>, Node>,
}

#[derive(Debug)]
enum Node {
    /// An entry as stored: a file, or a tree no edit has entered.
    Entry { mode: u32, oid: ObjectId },
    /// A directory an edit entered.
    Dir(Dir),
}

/// Builds new trees from a base tree and edits (plan GT1 Task 24).
#[derive(Debug)]
pub struct TreeBuilder;

impl TreeBuilder {
    /// Applies `edits`, in order, to `base_tree` (read from `odb`), and
    /// answers the new root tree and the trees written: each new tree once,
    /// children before parents, as `(oid, body)` (the body without git's
    /// `tree <len>\0` header). A tree that comes out the same as one the
    /// base held at that path is not new. Only the trees along the edits'
    /// paths are read. The empty tree needs no object in `odb`.
    pub async fn apply(
        base_tree: ObjectId,
        edits: impl IntoIterator<Item = (RepoPath, Edit)>,
        odb: &dyn ObjectDb,
    ) -> Result<(ObjectId, Vec<(ObjectId, Bytes)>), TreeBuildError> {
        let mut root = read_dir(odb, base_tree).await?;
        for (path, edit) in edits {
            if path.as_str().is_empty() {
                return Err(root_edit());
            }
            let parts: Vec<&[u8]> = path.as_str().split('/').map(str::as_bytes).collect();
            match edit {
                Edit::Write { mode, oid } => {
                    write_path(odb, &mut root, &parts, mode.bits(), oid).await?;
                }
                Edit::Remove => {
                    remove_path(odb, &mut root, &parts).await?;
                }
            }
        }
        let mut written = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let root_oid = encode(root, &mut written, &mut seen)?;
        Ok((root_oid, written))
    }
}

fn root_edit() -> TreeBuildError {
    TreeBuildError::Invalid {
        path: String::new(),
        reason: "the root is a directory; edit paths below it".into(),
    }
}

fn empty_tree() -> ObjectId {
    ObjectId::empty_tree(gix_hash::Kind::Sha1)
}

/// The entries of tree `oid`.
async fn read_dir(odb: &dyn ObjectDb, oid: ObjectId) -> Result<Dir, TreeBuildError> {
    let mut dir = Dir {
        base: Some(oid),
        entries: BTreeMap::new(),
    };
    if oid == empty_tree() {
        return Ok(dir);
    }
    let (kind, data) = odb.read(&oid).await?;
    if kind != Kind::Tree {
        return Err(TreeBuildError::NotATree {
            oid,
            found: crate::objects::kind_name(kind),
        });
    }
    let parsed = gix_object::TreeRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
        OdbError::CorruptObject {
            oid,
            reason: err.to_string(),
        }
    })?;
    for entry in &parsed.entries {
        dir.entries.insert(
            entry.filename.to_vec(),
            Node::Entry {
                mode: u32::from(entry.mode.value()),
                oid: entry.oid.to_owned(),
            },
        );
    }
    Ok(dir)
}

/// The directory `name` of `dir`, read or created so an edit can enter it:
/// a stored tree is read, anything else (absent, a file) becomes a new
/// empty directory when `create`, else `None`.
async fn enter<'a>(
    odb: &dyn ObjectDb,
    dir: &'a mut Dir,
    name: &[u8],
    create: bool,
) -> Result<Option<&'a mut Dir>, TreeBuildError> {
    let replace = match dir.entries.get(name) {
        Some(Node::Dir(_)) => None,
        Some(Node::Entry { mode, oid }) if *mode == TREE_MODE => Some(read_dir(odb, *oid).await?),
        _ if create => Some(Dir::default()),
        _ => return Ok(None),
    };
    if let Some(new) = replace {
        dir.entries.insert(name.to_vec(), Node::Dir(new));
    }
    match dir.entries.get_mut(name) {
        Some(Node::Dir(sub)) => Ok(Some(sub)),
        // Every arm above that did not return left a `Node::Dir` here.
        _ => unreachable!("the entry was just made a directory"),
    }
}

async fn write_path(
    odb: &dyn ObjectDb,
    root: &mut Dir,
    parts: &[&[u8]],
    mode: u32,
    oid: ObjectId,
) -> Result<(), TreeBuildError> {
    let Some((last, parents)) = parts.split_last() else {
        return Err(root_edit());
    };
    let mut dir = root;
    for name in parents {
        // `enter` with `create` always makes the entry a directory, so it
        // never answers `None`.
        dir = enter(odb, dir, name, true)
            .await?
            .expect("enter creates the directory");
    }
    dir.entries.insert(last.to_vec(), Node::Entry { mode, oid });
    Ok(())
}

async fn remove_path(
    odb: &dyn ObjectDb,
    root: &mut Dir,
    parts: &[&[u8]],
) -> Result<(), TreeBuildError> {
    let Some((last, parents)) = parts.split_last() else {
        return Err(root_edit());
    };
    let mut dir = root;
    for name in parents {
        match enter(odb, dir, name, false).await? {
            Some(sub) => dir = sub,
            None => return Ok(()),
        }
    }
    dir.entries.remove(*last);
    Ok(())
}

/// git's tree order: by name, a tree's name as if it ended in `/`.
fn tree_order(a: &(Vec<u8>, u32, ObjectId), b: &(Vec<u8>, u32, ObjectId)) -> std::cmp::Ordering {
    let key = |(name, mode, _): &(Vec<u8>, u32, ObjectId)| {
        let mut key = name.clone();
        if *mode == TREE_MODE {
            key.push(b'/');
        }
        key
    };
    key(a).cmp(&key(b))
}

/// Writes `dir` and the directories below it that edits entered; answers
/// its object id, or `None` when it came out empty (and is not the root).
fn encode_dir(
    dir: Dir,
    written: &mut Vec<(ObjectId, Bytes)>,
    seen: &mut std::collections::HashSet<ObjectId>,
) -> Result<Option<ObjectId>, TreeBuildError> {
    let mut entries: Vec<(Vec<u8>, u32, ObjectId)> = Vec::with_capacity(dir.entries.len());
    for (name, node) in dir.entries {
        match node {
            Node::Entry { mode, oid } => entries.push((name, mode, oid)),
            Node::Dir(sub) => {
                if let Some(oid) = encode_dir(sub, written, seen)? {
                    entries.push((name, TREE_MODE, oid));
                }
            }
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    Ok(Some(finish(dir.base, entries, written, seen)?))
}

fn encode(
    root: Dir,
    written: &mut Vec<(ObjectId, Bytes)>,
    seen: &mut std::collections::HashSet<ObjectId>,
) -> Result<ObjectId, TreeBuildError> {
    let base = root.base;
    match encode_dir(root, written, seen)? {
        Some(oid) => Ok(oid),
        None => finish(base, Vec::new(), written, seen),
    }
}

/// Serializes and hashes a tree; records it as written unless it is the
/// tree it was read from or was already written.
fn finish(
    base: Option<ObjectId>,
    mut entries: Vec<(Vec<u8>, u32, ObjectId)>,
    written: &mut Vec<(ObjectId, Bytes)>,
    seen: &mut std::collections::HashSet<ObjectId>,
) -> Result<ObjectId, TreeBuildError> {
    entries.sort_by(tree_order);
    let mut body = Vec::with_capacity(entries.len() * 40);
    for (name, mode, oid) in &entries {
        // git writes modes in octal without leading zeros (`40000`).
        body.extend_from_slice(format!("{mode:o} ").as_bytes());
        body.extend_from_slice(name);
        body.push(0);
        body.extend_from_slice(oid.as_bytes());
    }
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(format!("tree {}\0", body.len()).as_bytes());
    hasher.update(&body);
    let oid = hasher
        .try_finalize()
        .map_err(|err| TreeBuildError::Invalid {
            path: String::new(),
            reason: format!("hashing a tree: {err}"),
        })?;
    if base != Some(oid) && seen.insert(oid) {
        written.push((oid, Bytes::from(body)));
    }
    Ok(oid)
}
