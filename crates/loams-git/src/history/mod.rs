//! History on the server (§36 §19.8, D833; plan GT1 Tasks 20–21): `Log`
//! and `DiffStat` over the commit graph ([`crate::derived::graph`]) and tree
//! diffs, without a transport.
//!
//! This module holds what both share: [`Pathspec`] (literal paths only),
//! [`TreeReader`] (parsed trees, cached per request and counted), the
//! recursive tree diff [`diff_trees`], and revision resolution
//! ([`resolve_rev`]).

pub mod diffstat;
pub mod log;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use gix_object::Kind;

use crate::derived::DerivedError;
use crate::ids::{ObjectId, RefName};
use crate::limits::MAX_HISTORY_PATHS;
use crate::odb::{ObjectDb, OdbError, RangeOdb};
use crate::reflog::RefSnapshot;

pub use diffstat::{FileStat, diff_stat};
pub use log::{ChangeKind, ChangedPath, LogEntry, LogFields, LogQuery, LogStats, Signature, log};

/// Errors of the history queries.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// A pathspec this server does not evaluate (it takes literal paths).
    #[error("pathspec {spec:?}: {reason}")]
    Pathspec {
        /// The pathspec.
        spec: String,
        /// Why it is refused.
        reason: &'static str,
    },
    /// A revision that does not parse or names nothing.
    #[error("revision {rev:?}: {reason}")]
    Rev {
        /// The revision.
        rev: String,
        /// Why it is refused.
        reason: String,
    },
    /// More revisions or pathspecs than one call takes.
    #[error("{got} {what}, more than the {max} one call takes")]
    TooMany {
        /// What there are too many of.
        what: &'static str,
        /// The most taken.
        max: usize,
        /// How many were given.
        got: usize,
    },
    /// Reading objects failed (a missing object is `Odb(NotFound)`).
    #[error(transparent)]
    Odb(#[from] OdbError),
    /// The commit graph failed.
    #[error(transparent)]
    Derived(#[from] DerivedError),
}

/// Literal pathspecs (plan GT1 Task 20, ruling T20.4): each names a file or
/// a directory, and matches it and everything below it. Glob characters and
/// pathspec magic are refused, so the shim falls through to git for them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pathspec {
    items: Vec<Vec<u8>>,
}

impl Pathspec {
    /// Everything.
    pub fn all() -> Self {
        Self::default()
    }

    /// Parses `items` (at most `MAX_HISTORY_PATHS`). A trailing `/` is
    /// dropped; empty items, absolute paths, `.` and `..` components, glob
    /// characters (`*`, `?`, `[`, `\`) and a leading `:` are refused.
    pub fn parse(items: &[String]) -> Result<Self, HistoryError> {
        if items.len() > MAX_HISTORY_PATHS {
            return Err(HistoryError::TooMany {
                what: "pathspecs",
                max: MAX_HISTORY_PATHS,
                got: items.len(),
            });
        }
        let mut parsed = Vec::with_capacity(items.len());
        for item in items {
            let refuse = |reason| HistoryError::Pathspec {
                spec: item.clone(),
                reason,
            };
            let trimmed = item.strip_suffix('/').unwrap_or(item);
            if trimmed.is_empty() {
                return Err(refuse("empty"));
            }
            if trimmed.starts_with('/') {
                return Err(refuse("absolute"));
            }
            if trimmed.starts_with(':') {
                return Err(refuse("pathspec magic is not served"));
            }
            if trimmed.contains(['*', '?', '[', '\\']) {
                return Err(refuse("glob pathspecs are not served"));
            }
            if trimmed
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            {
                return Err(refuse("empty, `.` or `..` component"));
            }
            parsed.push(trimmed.as_bytes().to_vec());
        }
        parsed.sort();
        parsed.dedup();
        Ok(Self { items: parsed })
    }

    /// Whether it is empty (matches everything).
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// The literal paths.
    pub fn items(&self) -> &[Vec<u8>] {
        &self.items
    }

    /// Whether `path` is matched: equal to an item or below one.
    pub fn matches(&self, path: &[u8]) -> bool {
        self.is_empty() || self.items.iter().any(|item| within(path, item))
    }

    /// Whether a directory at `dir` may hold matched paths: it is matched,
    /// or it leads to an item.
    pub fn may_contain(&self, dir: &[u8]) -> bool {
        self.is_empty()
            || self
                .items
                .iter()
                .any(|item| within(dir, item) || within(item, dir))
    }
}

/// Whether `path` is `base` or below it.
fn within(path: &[u8], base: &[u8]) -> bool {
    path.len() >= base.len()
        && path.starts_with(base)
        && (path.len() == base.len() || path[base.len()] == b'/')
}

/// One entry of a tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntry {
    /// Its name.
    pub name: Vec<u8>,
    /// git's mode bits.
    pub mode: u32,
    /// Its object.
    pub oid: ObjectId,
}

impl TreeEntry {
    /// Whether it is a tree.
    pub fn is_tree(&self) -> bool {
        self.mode == 0o40000
    }
}

/// The parsed trees one query reads, cached and counted
/// (`log_pathspec_uses_bloom_then_diff` counts them).
#[derive(Debug)]
pub struct TreeReader<'a> {
    odb: &'a RangeOdb,
    cache: Mutex<HashMap<ObjectId, Arc<Vec<TreeEntry>>>>,
    reads: AtomicU64,
}

/// Trees one reader keeps; it forgets them all when it holds more.
const TREE_CACHE: usize = 4096;

impl<'a> TreeReader<'a> {
    /// A reader over `odb`.
    pub fn new(odb: &'a RangeOdb) -> Self {
        Self {
            odb,
            cache: Mutex::new(HashMap::new()),
            reads: AtomicU64::new(0),
        }
    }

    /// The object database.
    pub fn odb(&self) -> &'a RangeOdb {
        self.odb
    }

    /// Trees read from the object database (cache misses).
    pub fn reads(&self) -> u64 {
        self.reads.load(AtomicOrdering::Relaxed)
    }

    /// The entries of tree `oid`, in the tree's order.
    pub async fn tree(&self, oid: &ObjectId) -> Result<Arc<Vec<TreeEntry>>, OdbError> {
        if let Some(hit) = self.lock().get(oid) {
            return Ok(Arc::clone(hit));
        }
        self.reads.fetch_add(1, AtomicOrdering::Relaxed);
        let (kind, data) = self.odb.read(oid).await?;
        if kind != Kind::Tree {
            return Err(OdbError::CorruptObject {
                oid: *oid,
                reason: format!("a {} where a tree was expected", kind_name(kind)),
            });
        }
        let parsed =
            gix_object::TreeRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
                OdbError::CorruptObject {
                    oid: *oid,
                    reason: err.to_string(),
                }
            })?;
        let entries: Arc<Vec<TreeEntry>> = Arc::new(
            parsed
                .entries
                .iter()
                .map(|entry| TreeEntry {
                    name: entry.filename.to_vec(),
                    mode: u32::from(entry.mode.value()),
                    oid: entry.oid.to_owned(),
                })
                .collect(),
        );
        let mut cache = self.lock();
        if cache.len() >= TREE_CACHE {
            cache.clear();
        }
        cache.insert(*oid, Arc::clone(&entries));
        Ok(entries)
    }

    /// The entry at `path` below tree `root`, or `None`.
    pub async fn entry_at(
        &self,
        root: &ObjectId,
        path: &[u8],
    ) -> Result<Option<(u32, ObjectId)>, OdbError> {
        let mut tree = *root;
        let mut parts = path.split(|b| *b == b'/').peekable();
        while let Some(part) = parts.next() {
            let entries = self.tree(&tree).await?;
            let Some(entry) = entries.iter().find(|entry| entry.name == part) else {
                return Ok(None);
            };
            if parts.peek().is_none() {
                return Ok(Some((entry.mode, entry.oid)));
            }
            if !entry.is_tree() {
                return Ok(None);
            }
            tree = entry.oid;
        }
        Ok(None)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ObjectId, Arc<Vec<TreeEntry>>>> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// git's name of a kind.
pub(crate) fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Blob => "blob",
        Kind::Tree => "tree",
        Kind::Commit => "commit",
        Kind::Tag => "tag",
    }
}

/// One changed path of a tree diff: a file, symlink or submodule (trees
/// are recursed into, never reported).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    /// The path.
    pub path: Vec<u8>,
    /// The old mode and object, `None` when added.
    pub old: Option<(u32, ObjectId)>,
    /// The new mode and object, `None` when deleted.
    pub new: Option<(u32, ObjectId)>,
}

/// git's tree order: names compare as bytes, a tree's name as if it ended
/// in `/`.
fn tree_order(a: &TreeEntry, b: &TreeEntry) -> Ordering {
    let common = a.name.len().min(b.name.len());
    match a.name[..common].cmp(&b.name[..common]) {
        Ordering::Equal => {}
        other => return other,
    }
    let next = |entry: &TreeEntry| -> u8 {
        match entry.name.get(common) {
            Some(byte) => *byte,
            None if entry.is_tree() => b'/',
            None => 0,
        }
    };
    next(a).cmp(&next(b))
}

fn join(prefix: &[u8], name: &[u8]) -> Vec<u8> {
    if prefix.is_empty() {
        return name.to_vec();
    }
    let mut path = Vec::with_capacity(prefix.len() + 1 + name.len());
    path.extend_from_slice(prefix);
    path.push(b'/');
    path.extend_from_slice(name);
    path
}

/// The files that differ between trees `old` and `new` (`None` is the
/// empty tree), restricted to `spec`, in git's diff order (each tree's
/// order, depth first). With `stop_after`, the diff stops once it has
/// found more changes than that.
pub async fn diff_trees(
    reader: &TreeReader<'_>,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    spec: &Pathspec,
    stop_after: Option<usize>,
) -> Result<Vec<Change>, OdbError> {
    let mut out = Vec::new();
    diff_rec(reader, Vec::new(), old, new, spec, stop_after, &mut out).await?;
    Ok(out)
}

fn diff_rec<'r, 'a: 'r>(
    reader: &'r TreeReader<'a>,
    prefix: Vec<u8>,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    spec: &'r Pathspec,
    stop_after: Option<usize>,
    out: &'r mut Vec<Change>,
) -> BoxFuture<'r, Result<(), OdbError>> {
    Box::pin(async move {
        if old == new {
            return Ok(());
        }
        let empty = Arc::new(Vec::new());
        let old_entries = match &old {
            Some(oid) => reader.tree(oid).await?,
            None => Arc::clone(&empty),
        };
        let new_entries = match &new {
            Some(oid) => reader.tree(oid).await?,
            None => empty,
        };
        let (mut i, mut j) = (0, 0);
        while i < old_entries.len() || j < new_entries.len() {
            if stop_after.is_some_and(|max| out.len() > max) {
                return Ok(());
            }
            let order = match (old_entries.get(i), new_entries.get(j)) {
                (Some(a), Some(b)) => tree_order(a, b),
                (Some(_), None) => Ordering::Less,
                (None, _) => Ordering::Greater,
            };
            let (a, b) = match order {
                Ordering::Less => {
                    i += 1;
                    (Some(&old_entries[i - 1]), None)
                }
                Ordering::Greater => {
                    j += 1;
                    (None, Some(&new_entries[j - 1]))
                }
                Ordering::Equal => {
                    i += 1;
                    j += 1;
                    (Some(&old_entries[i - 1]), Some(&new_entries[j - 1]))
                }
            };
            let name = &a.or(b).expect("one side").name;
            let path = join(&prefix, name);
            let is_tree = a.or(b).expect("one side").is_tree();
            if is_tree {
                if !spec.may_contain(&path) {
                    continue;
                }
                let (a, b) = (a.map(|e| e.oid), b.map(|e| e.oid));
                if a != b {
                    diff_rec(reader, path, a, b, spec, stop_after, out).await?;
                }
                continue;
            }
            if !spec.matches(&path) {
                continue;
            }
            let a = a.map(|e| (e.mode, e.oid));
            let b = b.map(|e| (e.mode, e.oid));
            if a != b {
                out.push(Change {
                    path,
                    old: a,
                    new: b,
                });
            }
        }
        Ok(())
    })
}

/// A revision the history queries take: a full ref name, `HEAD`, a short
/// name (git's order: `refs/<n>`, `refs/tags/<n>`, `refs/heads/<n>`,
/// `refs/remotes/<n>`, `refs/remotes/<n>/HEAD`), or 40 hex digits.
pub fn resolve_rev(snapshot: &RefSnapshot, rev: &str) -> Result<ObjectId, HistoryError> {
    let refuse = |reason: &str| HistoryError::Rev {
        rev: rev.to_owned(),
        reason: reason.to_owned(),
    };
    if rev.is_empty() {
        return Err(refuse("empty"));
    }
    if rev.len() == 40 && rev.bytes().all(|b| b.is_ascii_hexdigit()) {
        return ObjectId::from_hex(rev.to_ascii_lowercase().as_bytes())
            .map_err(|err| refuse(&err.to_string()));
    }
    let lookup = |name: &str| -> Option<ObjectId> {
        let name = RefName::parse(name).ok()?;
        snapshot.refs.get(&name).copied()
    };
    if rev == "HEAD" {
        let target = snapshot
            .symrefs
            .get("HEAD")
            .ok_or_else(|| refuse("HEAD is not set"))?;
        return snapshot
            .refs
            .get(target)
            .copied()
            .ok_or_else(|| refuse("HEAD points at a branch with no commits"));
    }
    let candidates = [
        rev.to_owned(),
        format!("refs/{rev}"),
        format!("refs/tags/{rev}"),
        format!("refs/heads/{rev}"),
        format!("refs/remotes/{rev}"),
        format!("refs/remotes/{rev}/HEAD"),
    ];
    let candidates = if rev.starts_with("refs/") {
        &candidates[..1]
    } else {
        &candidates[1..]
    };
    candidates
        .iter()
        .find_map(|name| lookup(name))
        .ok_or_else(|| refuse("no such ref"))
}

/// The tree `rev` names (a commit or tag is peeled to its tree), for
/// `DiffStat`.
pub async fn resolve_tree(
    odb: &RangeOdb,
    snapshot: &RefSnapshot,
    rev: &str,
) -> Result<ObjectId, HistoryError> {
    let oid = resolve_rev(snapshot, rev)?;
    let (kind, oid) = peel(odb, &oid).await?;
    match kind {
        Kind::Tree => Ok(oid),
        Kind::Commit => {
            let (_, data) = odb.read(&oid).await?;
            let commit =
                gix_object::CommitRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
                    OdbError::CorruptObject {
                        oid,
                        reason: err.to_string(),
                    }
                })?;
            Ok(commit.tree())
        }
        other => Err(HistoryError::Rev {
            rev: rev.to_owned(),
            reason: format!("names a {}, not a tree or commit", kind_name(other)),
        }),
    }
}

/// What `oid` names, peeled through tags: `(kind, oid)` of the first
/// object that is not a tag.
pub(crate) async fn peel(odb: &RangeOdb, oid: &ObjectId) -> Result<(Kind, ObjectId), OdbError> {
    let mut cur = *oid;
    for _ in 0..16 {
        let loc = odb.locate(&cur).await?.ok_or(OdbError::NotFound(cur))?;
        let header = odb.header(&loc).await?;
        if header.kind != Kind::Tag {
            return Ok((header.kind, cur));
        }
        let (_, data) = odb.read_at(&loc).await?;
        let tag = gix_object::TagRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
            OdbError::CorruptObject {
                oid: cur,
                reason: err.to_string(),
            }
        })?;
        cur = tag.target();
    }
    Err(OdbError::CorruptObject {
        oid: *oid,
        reason: "tags nest more than 16 deep".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, tree: bool) -> TreeEntry {
        TreeEntry {
            name: name.as_bytes().to_vec(),
            mode: if tree { 0o40000 } else { 0o100644 },
            oid: ObjectId::null(gix_hash::Kind::Sha1),
        }
    }

    #[test]
    fn tree_order_puts_a_tree_after_its_name_as_a_file() {
        assert_eq!(
            tree_order(&entry("d", false), &entry("d", true)),
            Ordering::Less
        );
        assert_eq!(
            tree_order(&entry("a.txt", false), &entry("a", true)),
            Ordering::Less
        );
        assert_eq!(
            tree_order(&entry("a", true), &entry("a0", false)),
            Ordering::Less
        );
        assert_eq!(
            tree_order(&entry("a", false), &entry("a", false)),
            Ordering::Equal
        );
    }

    #[test]
    fn pathspecs_are_literal() {
        let spec = Pathspec::parse(&["src/".into(), "README".into()]).expect("parse");
        assert!(spec.matches(b"src/lib.rs"));
        assert!(spec.matches(b"src"));
        assert!(!spec.matches(b"srcx/lib.rs"));
        assert!(spec.matches(b"README"));
        assert!(spec.may_contain(b"src"));
        let deep = Pathspec::parse(&["a/b/c".into()]).expect("parse");
        assert!(deep.may_contain(b"a"));
        assert!(deep.may_contain(b"a/b"));
        assert!(!deep.may_contain(b"a/x"));
        assert!(!deep.matches(b"a/b"));
        for bad in ["", "/abs", ":(glob)x", "*.rs", "a/../b", "a//b", "./a"] {
            assert!(Pathspec::parse(&[bad.into()]).is_err(), "{bad:?}");
        }
    }
}
