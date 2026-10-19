//! `Log` (§36 §19.8, D833; plan GT1 Task 20): git's default `git log`
//! walk, over the commit graph.
//!
//! The walk follows `git-log(1)`'s documented behaviour (rulings T20.2,
//! T20.3), checked against git itself by `tests/history.rs`:
//!
//! - **Order.** Commits come newest committer date first; commits with the
//!   same date come in the order they were reached (the tips in the order
//!   given, then parents in parent order).
//! - **Ranges.** `^X` and `A..B` exclude every commit reachable from `X`
//!   (`A`). The excluded side is walked with the included one, in the same
//!   date order, and marks what it reaches: an excluded commit's parents and
//!   their parents, and on through every commit already read; the walk stops once every queued
//!   commit is excluded (after a few more steps, as git does), and only
//!   then are the included commits answered.
//! - **Pathspecs** (default history simplification). A commit is shown when
//!   it differs, in the paths named, from each of its parents; a merge that
//!   is the same as one of its parents follows only the first such parent
//!   and is not shown; a root commit is shown when it has any of the
//!   paths. Excluded parents do not count, except the excluded tips
//!   themselves: a merge the same as only such parents keeps all its
//!   parents, and is shown when any parent differs. The first parent is
//!   checked with the commit's changed-path Bloom filter first, and the
//!   trees are compared only when the filter says "maybe".
//!
//! `parents` are a commit's true parents (as `--format=raw` prints them),
//! never the simplified ones.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use bytes::Bytes;
use gix_object::Kind;
use tokio::sync::mpsc;

use super::{Change, HistoryError, Pathspec, TreeReader, diff_trees, peel, resolve_rev};
use crate::derived::{DerivedStore, GraphCommit, GraphView, graph_view};
use crate::ids::ObjectId;
use crate::limits::MAX_LOG_REVS;
use crate::odb::{ObjectDb, OdbError, RangeOdb};
use crate::reflog::RefSnapshot;

/// What a `Log` call asks for.
#[derive(Clone, Debug, Default)]
pub struct LogQuery {
    /// Revisions: `X`, `^X` (excluded), or `A..B` (`A` excluded; an empty
    /// side is `HEAD`). Empty means `HEAD`.
    pub revs: Vec<String>,
    /// The paths whose history is asked for (empty: all).
    pub pathspec: Pathspec,
    /// The most commits answered (`None`: all).
    pub limit: Option<usize>,
    /// The fields filled in.
    pub fields: LogFields,
}

/// The optional fields of a [`LogEntry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogFields {
    /// `author` and `committer`.
    pub signatures: bool,
    /// `message`.
    pub message: bool,
    /// `changed`: the paths changed against the first parent.
    pub changed: bool,
}

impl Default for LogFields {
    fn default() -> Self {
        Self {
            signatures: true,
            message: true,
            changed: false,
        }
    }
}

/// An author or committer line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    /// The name, as stored.
    pub name: Vec<u8>,
    /// The email, without `<` and `>`.
    pub email: Vec<u8>,
    /// Seconds since the epoch.
    pub time: i64,
    /// The time zone as stored (`+0530`).
    pub tz: String,
}

/// How a path changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    /// Added.
    Added,
    /// Deleted.
    Deleted,
    /// Changed contents or mode.
    Modified,
    /// Changed between a file, a symlink and a submodule.
    TypeChanged,
}

/// One path a commit changed against its first parent (the empty tree for
/// a root), restricted to the pathspec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedPath {
    /// The path.
    pub path: Vec<u8>,
    /// How it changed.
    pub kind: ChangeKind,
    /// The old mode and object (`None` when added).
    pub old: Option<(u32, ObjectId)>,
    /// The new mode and object (`None` when deleted).
    pub new: Option<(u32, ObjectId)>,
}

impl From<Change> for ChangedPath {
    fn from(change: Change) -> Self {
        let class = |mode: u32| mode & 0o170000;
        let kind = match (change.old, change.new) {
            (None, _) => ChangeKind::Added,
            (_, None) => ChangeKind::Deleted,
            (Some((a, _)), Some((b, _))) if class(a) != class(b) => ChangeKind::TypeChanged,
            _ => ChangeKind::Modified,
        };
        Self {
            path: change.path,
            kind,
            old: change.old,
            new: change.new,
        }
    }
}

/// One commit of a `Log` answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// The commit.
    pub oid: ObjectId,
    /// Its root tree.
    pub tree: ObjectId,
    /// Its parents, in order (never simplified).
    pub parents: Vec<ObjectId>,
    /// The author (with `fields.signatures`).
    pub author: Option<Signature>,
    /// The committer (with `fields.signatures`).
    pub committer: Option<Signature>,
    /// The message, as stored after the headers (with `fields.message`).
    pub message: Option<Bytes>,
    /// What changed (with `fields.changed`).
    pub changed: Option<Vec<ChangedPath>>,
}

/// Counters of one `Log` call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    /// Commits taken off the walk's queue (included and excluded).
    pub walked: u64,
    /// Trees read from the object database.
    pub tree_reads: u64,
    /// First-parent checks a Bloom filter answered "not changed".
    pub bloom_definitely_not: u64,
    /// First-parent checks a Bloom filter answered "maybe".
    pub bloom_maybe: u64,
    /// Whether the commit graph stored a new layer for this call.
    pub built_layer: bool,
}

/// The parsed `revs`: the included tips and the excluded ones, in order.
fn parse_revs(
    snapshot: &RefSnapshot,
    revs: &[String],
) -> Result<Vec<(ObjectId, bool, String)>, HistoryError> {
    if revs.len() > MAX_LOG_REVS {
        return Err(HistoryError::TooMany {
            what: "revisions",
            max: MAX_LOG_REVS,
            got: revs.len(),
        });
    }
    fn side(rev: &str) -> &str {
        if rev.is_empty() { "HEAD" } else { rev }
    }
    let mut out = Vec::new();
    if revs.is_empty() {
        out.push((resolve_rev(snapshot, "HEAD")?, false, "HEAD".to_owned()));
    }
    for rev in revs {
        if rev.contains("...") {
            return Err(HistoryError::Rev {
                rev: rev.clone(),
                reason: "symmetric differences are not served".into(),
            });
        }
        if let Some((from, to)) = rev.split_once("..") {
            out.push((resolve_rev(snapshot, side(from))?, true, rev.clone()));
            out.push((resolve_rev(snapshot, side(to))?, false, rev.clone()));
        } else if let Some(excluded) = rev.strip_prefix('^') {
            out.push((resolve_rev(snapshot, excluded)?, true, rev.clone()));
        } else {
            out.push((resolve_rev(snapshot, rev)?, false, rev.clone()));
        }
    }
    Ok(out)
}

const SEEN: u8 = 1;
const EXCLUDED: u8 = 2;
const PROCESSED: u8 = 4;
/// The walk knows the commit's parents (git: the commit is parsed).
const READ: u8 = 8;
/// Excluded commits taken off the queue, once every queued commit is
/// excluded, before the walk stops (git's "slop").
const SLOP: u32 = 5;

struct Walk<'v, 'r, 'a> {
    view: &'v GraphView,
    reader: &'r TreeReader<'a>,
    spec: &'r Pathspec,
    flags: HashMap<ObjectId, u8>,
    walked_parents: HashMap<ObjectId, Vec<ObjectId>>,
    queue: BinaryHeap<(u64, Reverse<u64>, ObjectId)>,
    counter: u64,
    included_queued: usize,
    /// The excluded tips (git's "bottom" commits), which stay relevant.
    bottoms: std::collections::HashSet<ObjectId>,
    stats: LogStats,
}

impl Walk<'_, '_, '_> {
    fn flag(&self, oid: &ObjectId) -> u8 {
        self.flags.get(oid).copied().unwrap_or(0)
    }

    fn excluded(&self, oid: &ObjectId) -> bool {
        self.flag(oid) & EXCLUDED != 0
    }

    fn commit(&self, oid: &ObjectId) -> Result<GraphCommit, HistoryError> {
        self.view
            .commit(oid)
            .ok_or(HistoryError::Odb(OdbError::NotFound(*oid)))
    }

    fn push(&mut self, oid: ObjectId) -> Result<(), HistoryError> {
        let flag = self.flag(&oid);
        if flag & SEEN != 0 {
            return Ok(());
        }
        let time = self.commit(&oid)?.commit_time;
        self.flags.insert(oid, flag | SEEN | READ);
        if flag & EXCLUDED == 0 {
            self.included_queued += 1;
        }
        self.counter += 1;
        self.queue.push((time, Reverse(self.counter), oid));
        Ok(())
    }

    /// Marks `oid` excluded (one commit, no further).
    fn mark(&mut self, oid: ObjectId) -> bool {
        let flag = self.flag(&oid);
        if flag & EXCLUDED != 0 {
            return false;
        }
        self.flags.insert(oid, flag | EXCLUDED);
        if flag & SEEN != 0 && flag & PROCESSED == 0 {
            self.included_queued -= 1;
        }
        true
    }

    /// The parents the walk knows `oid` by: the simplified ones once it is
    /// walked, else its own.
    fn parents_of(&self, oid: &ObjectId) -> Vec<ObjectId> {
        match self.walked_parents.get(oid) {
            Some(parents) => parents.clone(),
            None => self
                .view
                .commit(oid)
                .map(|commit| commit.parents)
                .unwrap_or_default(),
        }
    }

    /// Marks the parents of `oid` excluded, and on through every commit
    /// already read (queued, compared or walked), as git does: what the
    /// walk has not read yet is marked when it is reached.
    fn paint_parents(&mut self, oid: &ObjectId) {
        let mut stack = self.parents_of(oid);
        while let Some(oid) = stack.pop() {
            if self.mark(oid) && self.flag(&oid) & READ != 0 {
                stack.extend(self.parents_of(&oid));
            }
        }
    }

    fn read(&mut self, oid: ObjectId) {
        let flag = self.flag(&oid);
        self.flags.insert(oid, flag | READ);
    }

    /// Whether `commit` and `parent` have the same entries at every path of
    /// the pathspec; the first parent asks the Bloom filter first.
    async fn same(
        &mut self,
        oid: &ObjectId,
        commit: &GraphCommit,
        parent: &ObjectId,
        first: bool,
    ) -> Result<bool, HistoryError> {
        let parent_tree = self.commit(parent)?.tree;
        if parent_tree == commit.tree {
            return Ok(true);
        }
        if first {
            match self.view.bloom_may_touch(oid, self.spec.items()) {
                Some(false) => {
                    self.stats.bloom_definitely_not += 1;
                    return Ok(true);
                }
                Some(true) => self.stats.bloom_maybe += 1,
                None => {}
            }
        }
        for item in self.spec.items() {
            let a = self.reader.entry_at(&commit.tree, item).await?;
            let b = self.reader.entry_at(&parent_tree, item).await?;
            if a != b {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Which parents to walk and whether to show `oid` (default history
    /// simplification).
    async fn simplify(
        &mut self,
        oid: &ObjectId,
        commit: &GraphCommit,
    ) -> Result<(Vec<ObjectId>, bool), HistoryError> {
        if self.spec.is_empty() {
            return Ok((commit.parents.clone(), true));
        }
        if commit.parents.is_empty() {
            for item in self.spec.items() {
                if self.reader.entry_at(&commit.tree, item).await?.is_some() {
                    return Ok((Vec::new(), true));
                }
            }
            return Ok((Vec::new(), false));
        }
        // An excluded parent is irrelevant unless it is an excluded tip: a
        // merge the same as an irrelevant parent keeps all its parents.
        let mut relevant = false;
        let mut changed = false;
        for (n, parent) in commit.parents.iter().enumerate() {
            self.read(*parent);
            let same = self.same(oid, commit, parent, n == 0).await?;
            let is_relevant = !self.excluded(parent) || self.bottoms.contains(parent);
            if same && is_relevant {
                return Ok((vec![*parent], false));
            }
            relevant |= is_relevant;
            changed |= !same;
        }
        // With relevant parents, every one of them differs here; with none,
        // the commit is shown when any parent differs.
        let show = relevant || changed;
        Ok((commit.parents.clone(), show))
    }
}

/// The signature of a raw `author` or `committer` header value.
pub(crate) fn parse_signature(raw: &[u8]) -> Signature {
    let (ident, rest) = match raw.iter().rposition(|b| *b == b'>') {
        Some(gt) => (&raw[..gt], &raw[gt + 1..]),
        None => (raw, &raw[raw.len()..]),
    };
    let (name, email) = match ident.iter().rposition(|b| *b == b'<') {
        Some(lt) => (ident[..lt].trim_ascii_end(), &ident[lt + 1..]),
        None => (ident.trim_ascii_end(), &ident[ident.len()..]),
    };
    let rest = String::from_utf8_lossy(rest);
    let mut parts = rest.split_ascii_whitespace();
    let time = parts.next().and_then(|t| t.parse().ok()).unwrap_or(0);
    let tz = parts.next().unwrap_or("+0000").to_owned();
    Signature {
        name: name.to_vec(),
        email: email.to_vec(),
        time,
        tz,
    }
}

/// The seconds of a raw signature (0 when they do not parse).
pub(crate) fn signature_seconds(raw: &[u8]) -> i64 {
    parse_signature(raw).time
}

async fn entry(
    odb: &RangeOdb,
    reader: &TreeReader<'_>,
    spec: &Pathspec,
    fields: LogFields,
    view: &GraphView,
    oid: &ObjectId,
) -> Result<LogEntry, HistoryError> {
    let graph = view
        .commit(oid)
        .ok_or(HistoryError::Odb(OdbError::NotFound(*oid)))?;
    let (author, committer, message) = if fields.signatures || fields.message {
        let (kind, data) = odb.read(oid).await?;
        if kind != Kind::Commit {
            return Err(OdbError::CorruptObject {
                oid: *oid,
                reason: "not a commit".into(),
            }
            .into());
        }
        let parsed =
            gix_object::CommitRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
                OdbError::CorruptObject {
                    oid: *oid,
                    reason: err.to_string(),
                }
            })?;
        let author = fields
            .signatures
            .then(|| parse_signature(parsed.author.as_ref()));
        let committer = fields
            .signatures
            .then(|| parse_signature(parsed.committer.as_ref()));
        let message = fields
            .message
            .then(|| Bytes::copy_from_slice(parsed.message.as_ref()));
        (author, committer, message)
    } else {
        (None, None, None)
    };
    let changed = if fields.changed {
        let parent_tree = match graph.parents.first() {
            Some(parent) => Some(
                view.commit(parent)
                    .ok_or(HistoryError::Odb(OdbError::NotFound(*parent)))?
                    .tree,
            ),
            None => None,
        };
        let changes = diff_trees(reader, parent_tree, Some(graph.tree), spec, None).await?;
        Some(changes.into_iter().map(ChangedPath::from).collect())
    } else {
        None
    };
    Ok(LogEntry {
        oid: *oid,
        tree: graph.tree,
        parents: graph.parents,
        author,
        committer,
        message,
        changed,
    })
}

/// Runs `query` over `snapshot` and sends the commits to `out`, in git's
/// order, stopping early when `out` is closed. The commit graph is opened
/// (and a layer built, when needed) first.
pub async fn log(
    odb: &RangeOdb,
    derived: &DerivedStore,
    snapshot: &RefSnapshot,
    query: &LogQuery,
    out: &mpsc::Sender<LogEntry>,
) -> Result<LogStats, HistoryError> {
    let revs = parse_revs(snapshot, &query.revs)?;
    let mut tips = Vec::with_capacity(revs.len());
    for (oid, excluded, rev) in revs {
        let (kind, commit) = peel(odb, &oid).await?;
        if kind != Kind::Commit {
            return Err(HistoryError::Rev {
                rev,
                reason: format!("names a {}, not a commit", super::kind_name(kind)),
            });
        }
        tips.push((commit, excluded));
    }
    let mut view = graph_view(odb, derived, snapshot).await?;
    let built_layer = view.built_layer();
    let missing: Vec<ObjectId> = tips
        .iter()
        .map(|(oid, _)| *oid)
        .filter(|oid| !view.contains(oid))
        .collect();
    if !missing.is_empty() {
        view.extend(odb, &missing).await?;
    }
    let reader = TreeReader::new(odb);
    let limited = tips.iter().any(|(_, excluded)| *excluded);
    let mut walk = Walk {
        view: &view,
        reader: &reader,
        spec: &query.pathspec,
        flags: HashMap::new(),
        walked_parents: HashMap::new(),
        queue: BinaryHeap::new(),
        counter: 0,
        included_queued: 0,
        bottoms: tips
            .iter()
            .filter(|(_, excluded)| *excluded)
            .map(|(oid, _)| *oid)
            .collect(),
        stats: LogStats {
            built_layer,
            ..LogStats::default()
        },
    };
    for (oid, excluded) in &tips {
        if *excluded {
            walk.mark(*oid);
            walk.read(*oid);
            walk.paint_parents(oid);
        }
        walk.push(*oid)?;
    }
    let limit = query.limit.unwrap_or(usize::MAX);
    let mut sent = 0usize;
    let mut shown: Vec<ObjectId> = Vec::new();
    let mut slop = SLOP;
    'walk: while let Some((_, _, oid)) = walk.queue.pop() {
        walk.stats.walked += 1;
        let flag = walk.flag(&oid);
        walk.flags.insert(oid, flag | PROCESSED);
        let commit = walk.commit(&oid)?;
        if flag & EXCLUDED != 0 {
            // Parents are marked and read, and their parents marked too.
            for parent in &commit.parents {
                walk.mark(*parent);
                walk.read(*parent);
                walk.paint_parents(parent);
                walk.push(*parent)?;
            }
            walk.walked_parents.insert(oid, commit.parents.clone());
            if walk.included_queued == 0 {
                slop -= 1;
                if slop == 0 {
                    break;
                }
            } else {
                slop = SLOP;
            }
            continue;
        }
        walk.included_queued -= 1;
        let (parents, show) = walk.simplify(&oid, &commit).await?;
        for parent in &parents {
            walk.push(*parent)?;
        }
        walk.walked_parents.insert(oid, parents);
        if !show {
            continue;
        }
        if limited {
            shown.push(oid);
            continue;
        }
        let entry = entry(odb, &reader, &query.pathspec, query.fields, &view, &oid).await?;
        if sent >= limit || out.send(entry).await.is_err() {
            break 'walk;
        }
        sent += 1;
        if sent >= limit {
            break;
        }
    }
    if limited {
        for oid in shown {
            if walk.excluded(&oid) {
                continue;
            }
            if sent >= limit {
                break;
            }
            let entry = entry(odb, &reader, &query.pathspec, query.fields, &view, &oid).await?;
            if out.send(entry).await.is_err() {
                break;
            }
            sent += 1;
        }
    }
    let mut stats = walk.stats;
    stats.tree_reads = reader.reads();
    Ok(stats)
}
