//! `Blame` (§36 §19.8, D833; plan GT1 Task 21): which commit wrote each
//! line of a file, as `git blame --porcelain` attributes them.
//!
//! The walk is `gix-blame`'s (MIT OR Apache-2.0), with git's default Myers
//! diff and whole-file rename following; it is checked against `git blame
//! --porcelain` by `tests/history.rs` (ruling T21.1). `gix-blame` reads
//! objects synchronously, so it runs on a blocking thread and reads the
//! repository's packs through [`RangeOdb`] by blocking on the runtime.
//!
//! A file's blame at a commit never changes, so it is computed once for the
//! whole file and stored create-only as
//! `derived/blame/<commit>/<sha256(path)>.lbl`; a line range is cut from
//! it.
//!
//! ```text
//! header   magic "LGITBLAM" (8) | format_version u16 LE = 1 | commit oid [20]
//! body     loams.git.v1.Blame (protobuf)
//! trailer  crc32c u32 LE over header ‖ body | magic "LGITBEND" (8)
//! ```

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use gix_error::ErrorExt as _;
use gix_object::Kind;
use prost::Message;

use super::{DerivedError, DerivedStore};
use crate::format::{FormatError, Reader, crc32c_of, pb};
use crate::history::log::{Signature, parse_signature};
use crate::history::{HistoryError, TreeReader, kind_name, peel, resolve_rev};
use crate::ids::{ObjectId, RepoPath};
use crate::odb::{ObjectDb, OdbError, RangeOdb};
use crate::reflog::RefSnapshot;

/// The header's magic.
pub const BLAME_MAGIC: &[u8; 8] = b"LGITBLAM";
/// The trailer's magic.
pub const BLAME_END: &[u8; 8] = b"LGITBEND";
/// The format version this crate writes.
pub const BLAME_VERSION: u16 = 1;
/// The oldest format version this crate reads.
pub const BLAME_MIN_VERSION: u16 = 1;
/// Header bytes before the body.
pub const BLAME_HEADER_LEN: usize = 8 + 2 + 20;
/// Trailer bytes after the body.
pub const BLAME_TRAILER_LEN: usize = 4 + 8;

/// A run of lines one commit wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlameHunk {
    /// The first line, 0-based, in the blamed file.
    pub start: u32,
    /// The same line, 0-based, in the file as `commit` wrote it.
    pub source_start: u32,
    /// Lines in the run (at least one).
    pub len: u32,
    /// The commit that wrote them.
    pub commit: ObjectId,
    /// The path in `commit`, when the file was renamed since.
    pub source_path: Option<Vec<u8>>,
    /// The parent `commit` was compared against, and the path there: what
    /// porcelain prints as `previous`. `None` when `commit` added the file.
    pub previous: Option<(ObjectId, Vec<u8>)>,
}

/// A parent and a path in it.
type Previous = (ObjectId, Vec<u8>);

/// The blame of a whole file at a commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileBlame {
    /// The commit blamed at.
    pub commit: ObjectId,
    /// The file's path there.
    pub path: Vec<u8>,
    /// The file's blob.
    pub blob: ObjectId,
    /// Lines in the blob (a last line without a newline counts).
    pub lines: u32,
    /// Runs in the file's order, covering every line once.
    pub hunks: Vec<BlameHunk>,
}

fn oid_from(bytes: &[u8], what: &str) -> Result<ObjectId, FormatError> {
    ObjectId::try_from(bytes).map_err(|_| FormatError::corrupt(format!("{what}: not a SHA-1 id")))
}

/// Encodes `blame`; an equal blame has equal bytes.
pub fn encode_blame(blame: &FileBlame) -> Bytes {
    let body = pb::Blame {
        commit: blame.commit.as_bytes().to_vec(),
        path: blame.path.clone(),
        blob: blame.blob.as_bytes().to_vec(),
        lines: blame.lines,
        hunks: blame
            .hunks
            .iter()
            .map(|hunk| pb::BlameHunk {
                start: hunk.start,
                source_start: hunk.source_start,
                len: hunk.len,
                commit: hunk.commit.as_bytes().to_vec(),
                source_path: hunk.source_path.clone(),
                previous: hunk
                    .previous
                    .as_ref()
                    .map(|(oid, _)| oid.as_bytes().to_vec())
                    .unwrap_or_default(),
                previous_path: hunk
                    .previous
                    .as_ref()
                    .map(|(_, path)| path.clone())
                    .unwrap_or_default(),
            })
            .collect(),
    }
    .encode_to_vec();
    let mut out = BytesMut::with_capacity(BLAME_HEADER_LEN + body.len() + BLAME_TRAILER_LEN);
    out.put_slice(BLAME_MAGIC);
    out.put_u16_le(BLAME_VERSION);
    out.put_slice(blame.commit.as_bytes());
    out.put_slice(&body);
    let crc = crc32c_of(&[&out]);
    out.put_u32_le(crc);
    out.put_slice(BLAME_END);
    out.freeze()
}

/// Decodes a blame. Errors: `UnknownVersion`; `Corrupt` for a bad magic,
/// CRC or body, a header commit that differs from the body's, or hunks that
/// do not cover the file's lines once in order.
pub fn decode_blame(bytes: &[u8]) -> Result<FileBlame, FormatError> {
    if bytes.len() < BLAME_HEADER_LEN + BLAME_TRAILER_LEN {
        return Err(FormatError::corrupt("truncated"));
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *BLAME_MAGIC {
        return Err(FormatError::corrupt("not a blame: bad magic"));
    }
    let version = reader.u16()?;
    if !(BLAME_MIN_VERSION..=BLAME_VERSION).contains(&version) {
        return Err(FormatError::UnknownVersion { found: version });
    }
    let commit = oid_from(&reader.array::<20>()?, "commit")?;
    let body_end = bytes.len() - BLAME_TRAILER_LEN;
    let mut trailer = Reader::new(&bytes[body_end..]);
    let crc = trailer.u32()?;
    if trailer.array::<8>()? != *BLAME_END {
        return Err(FormatError::corrupt("bad blame end magic"));
    }
    if crc != crc32c_of(&[&bytes[..body_end]]) {
        return Err(FormatError::corrupt("blame CRC mismatch"));
    }
    let body = pb::Blame::decode(&bytes[BLAME_HEADER_LEN..body_end])
        .map_err(|err| FormatError::corrupt(format!("blame body: {err}")))?;
    if oid_from(&body.commit, "commit")? != commit {
        return Err(FormatError::corrupt(
            "blame header names another commit than its body",
        ));
    }
    let mut next = 0u32;
    let mut hunks = Vec::with_capacity(body.hunks.len());
    for hunk in body.hunks {
        if hunk.start != next || hunk.len == 0 {
            return Err(FormatError::corrupt("blame hunks do not cover the file"));
        }
        next = hunk.start.saturating_add(hunk.len);
        let previous = if hunk.previous.is_empty() {
            None
        } else {
            Some((oid_from(&hunk.previous, "previous")?, hunk.previous_path))
        };
        hunks.push(BlameHunk {
            start: hunk.start,
            source_start: hunk.source_start,
            len: hunk.len,
            commit: oid_from(&hunk.commit, "hunk commit")?,
            source_path: hunk.source_path,
            previous,
        });
    }
    if next != body.lines {
        return Err(FormatError::corrupt("blame hunks do not cover the file"));
    }
    Ok(FileBlame {
        commit,
        path: body.path,
        blob: oid_from(&body.blob, "blob")?,
        lines: body.lines,
        hunks,
    })
}

/// Counters of one blame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlameStats {
    /// Whether the blame came from `derived/blame/`.
    pub cached: bool,
    /// Commits the walk took (0 when cached).
    pub commits_traversed: u64,
}

/// The blame of the file at `path` in `commit` (plan GT1 Task 21): from
/// `derived/blame/` when it is there, else computed and stored create-only.
/// A stored blame that does not decode, or names another file, is logged
/// and recomputed (Task 55's scrub removes it), as tree aux data is.
pub async fn file_blame(
    odb: &RangeOdb,
    derived: &DerivedStore,
    commit: ObjectId,
    path: &RepoPath,
) -> Result<(Arc<FileBlame>, BlameStats), DerivedError> {
    let key = derived.paths().derived_blame(&commit, path);
    if let Some(bytes) = derived.get(&key).await? {
        match decode_blame(&bytes) {
            Ok(blame) if blame.commit == commit && blame.path == path.as_str().as_bytes() => {
                let stats = BlameStats {
                    cached: true,
                    commits_traversed: 0,
                };
                return Ok((Arc::new(blame), stats));
            }
            Ok(_) => tracing::warn!(path = key, "a stored blame names another file; recomputing"),
            Err(err) => tracing::warn!(path = key, error = %err, "corrupt blame; recomputing"),
        }
    }
    let (blame, stats) = compute(odb, commit, path).await?;
    let bytes = encode_blame(&blame);
    let stored = derived.put(&key, bytes.clone()).await?;
    if stored != bytes {
        tracing::warn!(
            path = key,
            "a stored blame differs from a fresh computation"
        );
    }
    Ok((Arc::new(blame), stats))
}

/// The error `gix-blame` sees when a read fails; the real error is kept in
/// [`SyncOdb::failure`].
#[derive(Debug, thiserror::Error)]
#[error("reading an object failed")]
struct ReadFailed;

/// [`RangeOdb`] as the synchronous object database `gix-blame` reads, for
/// use on a blocking thread only.
struct SyncOdb {
    odb: RangeOdb,
    runtime: tokio::runtime::Handle,
    failure: Mutex<Option<OdbError>>,
}

impl SyncOdb {
    fn fail(&self, err: OdbError) -> gix_error::Exn {
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        failure.get_or_insert(err);
        ReadFailed.raise_erased()
    }
}

impl gix_object::Find for SyncOdb {
    fn try_find<'a>(
        &self,
        id: &gix_hash::oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::ExnResult<Option<gix_object::Data<'a>>> {
        match self.runtime.block_on(self.odb.read(&id.to_owned())) {
            Ok((kind, data)) => {
                buffer.clear();
                buffer.extend_from_slice(&data);
                Ok(Some(gix_object::Data {
                    kind,
                    object_hash: gix_hash::Kind::Sha1,
                    data: buffer,
                }))
            }
            Err(OdbError::NotFound(_)) => Ok(None),
            Err(err) => Err(self.fail(err)),
        }
    }
}

impl gix_object::FindHeader for SyncOdb {
    fn try_header(&self, id: &gix_hash::oid) -> gix_error::ExnResult<Option<gix_object::Header>> {
        let read = async {
            let Some(loc) = self.odb.locate(&id.to_owned()).await? else {
                return Ok(None);
            };
            let header = self.odb.header(&loc).await?;
            Ok(Some(gix_object::Header {
                kind: header.kind,
                size: header.size,
            }))
        };
        self.runtime.block_on(read).map_err(|err| self.fail(err))
    }
}

/// The diff driver every path is given, so no blob is treated as binary:
/// git blame diffs a file with NUL bytes as text too.
const TEXT_DRIVER: &str = "loams-blame-text";

/// A blob diff platform that reads objects as git stores them, with no
/// worktree or filters, and every blob diffed as text.
fn platform() -> gix_diff::blob::Platform {
    let mut collection = gix_attributes::search::MetadataCollection::default();
    let mut globals = gix_attributes::Search::default();
    globals.add_patterns_buffer(
        format!("* diff={TEXT_DRIVER}").as_bytes(),
        "[loams]".into(),
        None,
        &mut collection,
        false,
    );
    let attributes = gix_worktree::stack::state::Attributes::new(
        globals,
        None,
        gix_worktree::stack::state::attributes::Source::IdMapping,
        collection,
    );
    let text = gix_diff::blob::Driver {
        name: TEXT_DRIVER.into(),
        command: None,
        algorithm: None,
        binary_to_text_command: None,
        is_binary: Some(false),
    };
    gix_diff::blob::Platform::new(
        Default::default(),
        gix_diff::blob::Pipeline::new(
            Default::default(),
            gix_filter::Pipeline::default(),
            vec![text],
            Default::default(),
        ),
        gix_diff::blob::pipeline::Mode::ToGit,
        gix_worktree::Stack::new(
            std::path::PathBuf::new(),
            gix_worktree::stack::State::AttributesStack(attributes),
            Default::default(),
            Vec::new(),
            Vec::new(),
        ),
    )
}

/// Lines in `data`, as git counts them for blame.
fn count_lines(data: &[u8]) -> u32 {
    let newlines = data.iter().filter(|byte| **byte == b'\n').count();
    let last = usize::from(!data.is_empty() && !data.ends_with(b"\n"));
    u32::try_from(newlines + last).unwrap_or(u32::MAX)
}

async fn compute(
    odb: &RangeOdb,
    commit: ObjectId,
    path: &RepoPath,
) -> Result<(FileBlame, BlameStats), DerivedError> {
    let file = path.as_str().as_bytes().to_vec();
    let sync = SyncOdb {
        odb: odb.clone(),
        runtime: tokio::runtime::Handle::current(),
        failure: Mutex::new(None),
    };
    let walk_path = file.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let options = gix_blame::Options {
            diff_algorithm: gix_diff::blob::Algorithm::Myers,
            ranges: gix_blame::BlameRanges::WholeFile,
            since: None,
            rewrites: Some(gix_diff::Rewrites::default()),
            // The diffs it took name each commit's parent and path there:
            // porcelain's `previous`.
            debug_track_path: true,
        };
        let outcome = gix_blame::file(
            &sync,
            gix_blame::Start::Commit(commit),
            None,
            &mut platform(),
            walk_path.as_slice().into(),
            options,
        );
        let failure = sync
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        (outcome, failure)
    })
    .await;
    let (outcome, failure) = match joined {
        Ok(done) => done,
        // `gix-blame` 0.18 asserts that a path it found in a commit is never
        // deleted against a parent, which a file that is a directory in an
        // ancestor breaks (ruling T21.1): not served, so git answers it.
        Err(err) if err.is_panic() => {
            let reason = err
                .into_panic()
                .downcast::<String>()
                .map(|text| *text)
                .or_else(|panic| panic.downcast::<&str>().map(|text| (*text).to_owned()))
                .unwrap_or_else(|_| "gix-blame panicked".to_owned());
            tracing::warn!(%commit, path = path.as_str(), reason, "blame is not served for this file");
            return Err(DerivedError::BlameUnsupported(reason));
        }
        Err(err) => return Err(DerivedError::Blame(err.to_string())),
    };
    if let Some(err) = failure {
        return Err(err.into());
    }
    let outcome = outcome.map_err(|err| DerivedError::Blame(err.to_string()))?;
    let reader = TreeReader::new(odb);
    let commit_tree = commit_of(odb, &commit).await?.tree;
    let blob = match reader.entry_at(&commit_tree, &file).await? {
        Some((_, blob)) => blob,
        None => {
            return Err(DerivedError::Blame(format!(
                "{} has no file {}",
                commit,
                path.as_str()
            )));
        }
    };
    let mut hunks: Vec<BlameHunk> = Vec::with_capacity(outcome.entries.len());
    let mut previous_of: HashMap<(ObjectId, Vec<u8>), Option<Previous>> = HashMap::new();
    let mut steps: HashMap<(ObjectId, Vec<u8>), (usize, Vec<u8>)> = HashMap::new();
    for step in outcome.blame_path.iter().flatten() {
        let Some(before) = &step.previous_source_file_path else {
            continue;
        };
        let key = (step.commit_id, step.source_file_path.to_vec());
        // The first parent the walk compared the commit against.
        match steps.get(&key) {
            Some((index, _)) if *index <= step.parent_index => {}
            _ => {
                steps.insert(key, (step.parent_index, before.to_vec()));
            }
        }
    }
    for (key, (index, before)) in steps {
        let parents = commit_of(odb, &key.0).await?.parents;
        if let Some(parent) = parents.get(index) {
            previous_of.insert(key, Some((*parent, before)));
        }
    }
    for entry in outcome.entries {
        let source_path = entry.source_file_name.map(|name| name.to_vec());
        let key = (
            entry.commit_id,
            source_path.clone().unwrap_or_else(|| file.clone()),
        );
        let previous = match previous_of.get(&key) {
            Some(known) => known.clone(),
            None => {
                let found = previous(odb, &reader, &key.0, &key.1).await?;
                previous_of.insert(key, found.clone());
                found
            }
        };
        let hunk = BlameHunk {
            start: entry.start_in_blamed_file,
            source_start: entry.start_in_source_file,
            len: entry.len.get(),
            commit: entry.commit_id,
            source_path,
            previous,
        };
        // `gix-blame` coalesces runs; equal neighbours are merged again in
        // case it did not.
        if let Some(last) = hunks.last_mut()
            && last.commit == hunk.commit
            && last.source_path == hunk.source_path
            && last.start + last.len == hunk.start
            && last.source_start + last.len == hunk.source_start
        {
            last.len += hunk.len;
            continue;
        }
        hunks.push(hunk);
    }
    let lines = count_lines(&outcome.blob);
    // Release builds of `gix-blame` 0.18 can end a walk with lines left
    // unblamed (debug builds assert); such an answer is never served.
    let mut next = 0u32;
    for hunk in &hunks {
        if hunk.start != next {
            break;
        }
        next = hunk.start + hunk.len;
    }
    if next != lines {
        tracing::warn!(%commit, path = path.as_str(), "gix-blame left lines unblamed");
        return Err(DerivedError::BlameUnsupported(
            "the walk left lines unblamed".into(),
        ));
    }
    let blame = FileBlame {
        commit,
        path: file,
        blob,
        lines,
        hunks,
    };
    let stats = BlameStats {
        cached: false,
        commits_traversed: u64::try_from(outcome.statistics.commits_traversed).unwrap_or(u64::MAX),
    };
    Ok((blame, stats))
}

/// A parsed commit's tree, parents and raw headers.
struct CommitInfo {
    tree: ObjectId,
    parents: Vec<ObjectId>,
    author: Signature,
    committer: Signature,
    message: Vec<u8>,
}

async fn commit_of(odb: &RangeOdb, oid: &ObjectId) -> Result<CommitInfo, OdbError> {
    let (kind, data) = odb.read(oid).await?;
    if kind != Kind::Commit {
        return Err(OdbError::CorruptObject {
            oid: *oid,
            reason: format!("a {} where a commit was expected", kind_name(kind)),
        });
    }
    let parsed = gix_object::CommitRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
        OdbError::CorruptObject {
            oid: *oid,
            reason: err.to_string(),
        }
    })?;
    Ok(CommitInfo {
        tree: parsed.tree(),
        parents: parsed.parents().collect(),
        author: parse_signature(parsed.author.as_ref()),
        committer: parse_signature(parsed.committer.as_ref()),
        message: parsed.message.to_vec(),
    })
}

/// The first parent of `commit` that has a file at `path`, as porcelain's
/// `previous` names it.
async fn previous(
    odb: &RangeOdb,
    reader: &TreeReader<'_>,
    commit: &ObjectId,
    path: &[u8],
) -> Result<Option<(ObjectId, Vec<u8>)>, OdbError> {
    let info = commit_of(odb, commit).await?;
    for parent in info.parents {
        let tree = commit_of(odb, &parent).await?.tree;
        if let Some((mode, _)) = reader.entry_at(&tree, path).await?
            && mode != 0o40000
            && mode != 0o160000
        {
            return Ok(Some((parent, path.to_vec())));
        }
    }
    Ok(None)
}

/// What a `Blame` call asks for.
#[derive(Clone, Debug)]
pub struct BlameQuery {
    /// The revision (as `Log` takes one; a tag is peeled); empty is `HEAD`.
    pub rev: String,
    /// The file.
    pub path: RepoPath,
    /// The lines, 1-based and inclusive (`git blame -L a,b`); `None` is
    /// the whole file. An end past the file's last line is cut to it.
    pub lines: Option<RangeInclusive<u32>>,
}

/// A commit a blame names, with what porcelain prints of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlameCommit {
    /// The commit.
    pub oid: ObjectId,
    /// Its author.
    pub author: Signature,
    /// Its committer.
    pub committer: Signature,
    /// Its subject: the message's first paragraph on one line.
    pub summary: Vec<u8>,
    /// Whether it is a root commit (porcelain's `boundary`).
    pub boundary: bool,
}

/// A `Blame` answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlameAnswer {
    /// The commit blamed at.
    pub commit: ObjectId,
    /// The file's blob there.
    pub blob: ObjectId,
    /// Lines in the file.
    pub lines: u32,
    /// The runs of the asked lines, in order.
    pub hunks: Vec<BlameHunk>,
    /// Every commit the runs name, in the order they first appear.
    pub commits: Vec<BlameCommit>,
    /// Counters.
    pub stats: BlameStats,
}

/// git's subject of a message: its first paragraph, lines joined by spaces.
pub fn summary(message: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    let mut started = false;
    for line in message.split(|byte| *byte == b'\n') {
        let blank = line.iter().all(u8::is_ascii_whitespace);
        if blank {
            if started {
                break;
            }
            continue;
        }
        if started {
            out.push(b' ');
        }
        out.extend_from_slice(line.trim_ascii_end());
        started = true;
    }
    out
}

/// Runs `query` over `snapshot` (plan GT1 Task 21). Errors: `Rev` for a
/// revision that names no commit; `Path` for a path that is not a file
/// there; `Lines` for a range that starts past the file's end.
pub async fn blame(
    odb: &RangeOdb,
    derived: &DerivedStore,
    snapshot: &RefSnapshot,
    query: &BlameQuery,
) -> Result<BlameAnswer, HistoryError> {
    let rev = if query.rev.is_empty() {
        "HEAD"
    } else {
        query.rev.as_str()
    };
    let (kind, commit) = peel(odb, &resolve_rev(snapshot, rev)?).await?;
    if kind != Kind::Commit {
        return Err(HistoryError::Rev {
            rev: rev.to_owned(),
            reason: format!("names a {}, not a commit", kind_name(kind)),
        });
    }
    let path = query.path.as_str().as_bytes();
    let reader = TreeReader::new(odb);
    let tree = commit_of(odb, &commit).await?.tree;
    match reader.entry_at(&tree, path).await? {
        Some((mode, _)) if mode != 0o40000 && mode != 0o160000 => {}
        Some(_) => {
            return Err(HistoryError::Path {
                path: query.path.as_str().to_owned(),
                reason: format!("not a file in {rev}"),
            });
        }
        None => {
            return Err(HistoryError::Path {
                path: query.path.as_str().to_owned(),
                reason: format!("no such path in {rev}"),
            });
        }
    }
    let (file, stats) = file_blame(odb, derived, commit, &query.path).await?;
    let hunks = match &query.lines {
        None => file.hunks.clone(),
        Some(range) => cut(&file, range)?,
    };
    let mut commits: Vec<BlameCommit> = Vec::new();
    for hunk in &hunks {
        if commits.iter().any(|known| known.oid == hunk.commit) {
            continue;
        }
        let info = commit_of(odb, &hunk.commit).await?;
        commits.push(BlameCommit {
            oid: hunk.commit,
            author: info.author,
            committer: info.committer,
            summary: summary(&info.message),
            boundary: info.parents.is_empty(),
        });
    }
    Ok(BlameAnswer {
        commit,
        blob: file.blob,
        lines: file.lines,
        hunks,
        commits,
        stats,
    })
}

/// The runs of `blame` within the 1-based inclusive `range`.
fn cut(blame: &FileBlame, range: &RangeInclusive<u32>) -> Result<Vec<BlameHunk>, HistoryError> {
    let (start, end) = (*range.start(), *range.end());
    if start == 0 || end < start {
        return Err(HistoryError::Lines {
            reason: format!("{start},{end} is not a range of lines (1-based, start ≤ end)"),
        });
    }
    if start > blame.lines {
        return Err(HistoryError::Lines {
            reason: format!("the file has only {} lines", blame.lines),
        });
    }
    let (from, to) = (start - 1, end.min(blame.lines));
    let mut out = Vec::new();
    for hunk in &blame.hunks {
        let (a, b) = (hunk.start.max(from), (hunk.start + hunk.len).min(to));
        if a >= b {
            continue;
        }
        out.push(BlameHunk {
            start: a,
            source_start: hunk.source_start + (a - hunk.start),
            len: b - a,
            ..hunk.clone()
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_is_the_first_paragraph() {
        assert_eq!(summary(b"one\n"), b"one");
        assert_eq!(summary(b"\n\none\ntwo\n\nbody\n"), b"one two");
        assert_eq!(summary(b""), b"");
    }

    #[test]
    fn lines_count_a_last_line_without_newline() {
        assert_eq!(count_lines(b""), 0);
        assert_eq!(count_lines(b"a\n"), 1);
        assert_eq!(count_lines(b"a\nb"), 2);
    }
}
