//! The commit graph (§36 §19.8, D833; plan GT1 Task 20): what `Log` walks.
//!
//! The graph is a chain of layers in git's commit-graph format
//! (`gitformat-commit-graph(5)`; implemented from that page, not git's
//! code), so `gix-commitgraph`'s reader serves every layer. Each layer is
//! one create-only object `derived/graph/<seq:020>.graph`, built on demand
//! the first time a query needs the commits of seq `seq` that no stored
//! layer holds. Its chunks:
//!
//! - `OIDF`, `OIDL`, `CDAT` (root tree, parent positions, topological level
//!   and commit time) and `EDGE` (octopus parents), as git writes them;
//! - `BIDX` and `BDAT`: changed-path Bloom filters, hash version 2
//!   ([`super::bloom`]), against each commit's first parent;
//! - `BASE`: the checksums of the layers below it, bottom first;
//! - `LOAM` (Loams' own; git and `gix-commitgraph` ignore chunks they do not
//!   know): one 16-byte entry per base, `kind` u64 BE (1 = a derived layer)
//!   and `seq` u64 BE, so a reader finds the base objects without a
//!   listing.
//!
//! **Building** (ruling T20.1). The layer of seq `s` holds every commit
//! reachable from `s`'s refs that the newest stored layer's chain lacks.
//! Layers merge geometrically: while the top layer below has fewer than
//! twice the new layer's commits, or the chain would be longer than
//! `MAX_GRAPH_LAYERS`, its commits join the new layer, so a chain stays
//! about `log2(commits)` layers long. Commit data never changes, so any
//! layer is correct for any seq: extra commits are harmless.
//!
//! Compaction's own `commit-graph/<seq>.graph` (§36 §7) is not named by
//! snapshots yet (the state machine does not apply `PackSetChange.
//! commit_graph`; Task 53 wires it, as T17.2 for the multi-pack index), so
//! every chain is derived layers only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::path::PathBuf;

use bytes::{BufMut, Bytes, BytesMut};
use gix_object::Kind;

use super::bloom::{BloomSettings, ChangedPaths, build_filter, maybe_contains};
use super::{DerivedError, DerivedStore};
use crate::format::FormatError;
use crate::history::{Pathspec, TreeReader, diff_trees, peel};
use crate::ids::{ObjectId, Seq};
use crate::limits::{BLOOM_MAX_CHANGED_PATHS, MAX_GRAPH_LAYERS};
use crate::odb::{OdbError, RangeOdb};
use crate::reflog::RefSnapshot;

/// The file signature.
pub const GRAPH_SIGNATURE: &[u8; 4] = b"CGPH";
/// The largest topological level the `CDAT` chunk holds (30 bits).
pub const GENERATION_V1_MAX: u32 = 0x3fff_ffff;
/// The largest commit time the `CDAT` chunk holds (34 bits).
const COMMIT_TIME_MAX: u64 = (1 << 34) - 1;
const NO_PARENT: u32 = 0x7000_0000;
const OCTOPUS: u32 = 0x8000_0000;
const OIDF: [u8; 4] = *b"OIDF";
const OIDL: [u8; 4] = *b"OIDL";
const CDAT: [u8; 4] = *b"CDAT";
const EDGE: [u8; 4] = *b"EDGE";
const BIDX: [u8; 4] = *b"BIDX";
const BDAT: [u8; 4] = *b"BDAT";
const BASE: [u8; 4] = *b"BASE";
/// Loams' chunk naming each base layer's object.
pub const LOAM_CHUNK: [u8; 4] = *b"LOAM";
/// A `LOAM` entry's kind: a layer at `derived/graph/<seq>.graph`.
const SOURCE_DERIVED: u64 = 1;

/// One commit of a layer, as it is written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerCommit {
    /// The commit.
    pub oid: ObjectId,
    /// Its root tree.
    pub tree: ObjectId,
    /// Its parents, in order.
    pub parents: Vec<ObjectId>,
    /// Its topological level: 1 for a root, else one more than its
    /// parents' highest (capped at [`GENERATION_V1_MAX`]).
    pub generation: u32,
    /// Its committer time, in seconds (clamped to 0 ..= 2^34 − 1).
    pub commit_time: u64,
    /// Its changed-path Bloom filter.
    pub bloom: Vec<u8>,
}

/// A commit as the graph knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphCommit {
    /// Its root tree.
    pub tree: ObjectId,
    /// Its parents, in order.
    pub parents: Vec<ObjectId>,
    /// Its topological level.
    pub generation: u32,
    /// Its committer time, in seconds.
    pub commit_time: u64,
}

/// A base of a layer: where its object is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LayerSource {
    /// The seq of `derived/graph/<seq:020>.graph`.
    pub seq: Seq,
}

/// Encodes a layer of `commits` (any order) above `bases` (bottom first,
/// with their checksums and commit counts). `position` answers a parent's
/// position in the bases; a parent in neither `commits` nor the bases is
/// `NotFound`.
pub fn encode_layer(
    mut commits: Vec<LayerCommit>,
    bases: &[(LayerSource, ObjectId, u32)],
    position: impl Fn(&ObjectId) -> Option<u32>,
) -> Result<Bytes, DerivedError> {
    commits.sort_by_key(|commit| commit.oid);
    commits.dedup_by(|a, b| a.oid == b.oid);
    let below: u32 = bases.iter().map(|(_, _, count)| count).sum();
    let local: HashMap<ObjectId, u32> = commits
        .iter()
        .enumerate()
        .map(|(i, commit)| (commit.oid, below + i as u32))
        .collect();
    let pos = |oid: &ObjectId| -> Result<u32, DerivedError> {
        local
            .get(oid)
            .copied()
            .or_else(|| position(oid))
            .ok_or(DerivedError::Odb(OdbError::NotFound(*oid)))
    };

    let mut fan = BytesMut::with_capacity(1024);
    let mut counts = [0u32; 256];
    for commit in &commits {
        counts[usize::from(commit.oid.as_bytes()[0])] += 1;
    }
    let mut total = 0u32;
    for count in counts {
        total += count;
        fan.put_u32(total);
    }
    let mut oidl = BytesMut::with_capacity(commits.len() * 20);
    let mut cdat = BytesMut::with_capacity(commits.len() * 36);
    let mut edge = BytesMut::new();
    let mut bidx = BytesMut::with_capacity(commits.len() * 4);
    let mut bdat = BytesMut::new();
    let settings = BloomSettings::DEFAULT;
    bdat.put_u32(settings.hash_version);
    bdat.put_u32(settings.num_hashes);
    bdat.put_u32(settings.bits_per_entry);
    let mut filters = 0u32;
    for commit in &commits {
        oidl.put_slice(commit.oid.as_bytes());
        cdat.put_slice(commit.tree.as_bytes());
        let first = match commit.parents.first() {
            Some(parent) => pos(parent)?,
            None => NO_PARENT,
        };
        let second = match commit.parents.len() {
            0 | 1 => NO_PARENT,
            2 => pos(&commit.parents[1])?,
            _ => {
                let at = (edge.len() / 4) as u32;
                let rest = &commit.parents[1..];
                for (i, parent) in rest.iter().enumerate() {
                    let mut value = pos(parent)?;
                    if i + 1 == rest.len() {
                        value |= OCTOPUS;
                    }
                    edge.put_u32(value);
                }
                OCTOPUS | at
            }
        };
        cdat.put_u32(first);
        cdat.put_u32(second);
        let time = commit.commit_time.min(COMMIT_TIME_MAX);
        let generation = commit.generation.min(GENERATION_V1_MAX);
        cdat.put_u32((generation << 2) | (time >> 32) as u32);
        cdat.put_u32(time as u32);
        filters += commit.bloom.len() as u32;
        bidx.put_u32(filters);
        bdat.put_slice(&commit.bloom);
    }
    let mut base = BytesMut::with_capacity(bases.len() * 20);
    let mut loam = BytesMut::with_capacity(bases.len() * 16);
    for (source, checksum, _) in bases {
        base.put_slice(checksum.as_bytes());
        loam.put_u64(SOURCE_DERIVED);
        loam.put_u64(source.seq.0);
    }
    let mut chunks: Vec<([u8; 4], Bytes)> = vec![
        (OIDF, fan.freeze()),
        (OIDL, oidl.freeze()),
        (CDAT, cdat.freeze()),
    ];
    if !edge.is_empty() {
        chunks.push((EDGE, edge.freeze()));
    }
    chunks.push((BIDX, bidx.freeze()));
    chunks.push((BDAT, bdat.freeze()));
    if !bases.is_empty() {
        chunks.push((BASE, base.freeze()));
        chunks.push((LOAM_CHUNK, loam.freeze()));
    }
    Ok(write_chunk_file(&chunks, bases.len() as u8))
}

fn write_chunk_file(chunks: &[([u8; 4], Bytes)], bases: u8) -> Bytes {
    let toc = (chunks.len() + 1) * 12;
    let body: usize = chunks.iter().map(|(_, data)| data.len()).sum();
    let mut out = BytesMut::with_capacity(8 + toc + body + 20);
    out.put_slice(GRAPH_SIGNATURE);
    out.put_u8(1); // format version
    out.put_u8(1); // SHA-1
    out.put_u8(chunks.len() as u8);
    out.put_u8(bases);
    let mut offset = (8 + toc) as u64;
    for (id, data) in chunks {
        out.put_slice(id);
        out.put_u64(offset);
        offset += data.len() as u64;
    }
    out.put_slice(&[0; 4]);
    out.put_u64(offset);
    for (_, data) in chunks {
        out.put_slice(data);
    }
    let checksum = sha1(&out);
    out.put_slice(checksum.as_bytes());
    out.freeze()
}

fn sha1(bytes: &[u8]) -> ObjectId {
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(bytes);
    hasher
        .try_finalize()
        .unwrap_or_else(|_| ObjectId::null(gix_hash::Kind::Sha1))
}

/// One parsed layer.
#[derive(Debug)]
pub struct Layer {
    bytes: Bytes,
    checksum: ObjectId,
    commits: u32,
    bases: Vec<ObjectId>,
    sources: Vec<LayerSource>,
    bidx: Option<Range<usize>>,
    bdat: Option<Range<usize>>,
    /// The parsed file, until a view takes it.
    file: Option<gix_commitgraph::File>,
}

impl Layer {
    /// Parses a layer and checks its checksum.
    pub fn parse(bytes: Bytes) -> Result<Self, FormatError> {
        let corrupt = |reason: &str| FormatError::corrupt(format!("commit graph: {reason}"));
        if bytes.len() < 8 + 12 + 20 || &bytes[..4] != GRAPH_SIGNATURE {
            return Err(corrupt("not a commit-graph file"));
        }
        if bytes[4] != 1 || bytes[5] != 1 {
            return Err(corrupt("not a version 1 SHA-1 commit graph"));
        }
        let trailer = bytes.len() - 20;
        let checksum = ObjectId::try_from(&bytes[trailer..]).map_err(|_| corrupt("trailer"))?;
        if sha1(&bytes[..trailer]) != checksum {
            return Err(corrupt("checksum mismatch"));
        }
        let count = usize::from(bytes[6]);
        let base_count = usize::from(bytes[7]);
        let toc_end = 8 + (count + 1) * 12;
        if toc_end > trailer {
            return Err(corrupt("truncated table of contents"));
        }
        let mut chunks: BTreeMap<[u8; 4], Range<usize>> = BTreeMap::new();
        for i in 0..count {
            let at = 8 + i * 12;
            let id: [u8; 4] = bytes[at..at + 4].try_into().expect("four bytes");
            let start = u64::from_be_bytes(bytes[at + 4..at + 12].try_into().expect("8"));
            let end = u64::from_be_bytes(bytes[at + 16..at + 24].try_into().expect("8"));
            let (start, end) = (start as usize, end as usize);
            if start > end || end > trailer || start < toc_end {
                return Err(corrupt("chunk out of bounds"));
            }
            chunks.insert(id, start..end);
        }
        let commits = chunks
            .get(&OIDL)
            .map(|range| (range.len() / 20) as u32)
            .ok_or_else(|| corrupt("no OIDL chunk"))?;
        let bases: Vec<ObjectId> = match chunks.get(&BASE) {
            Some(range) => bytes[range.clone()]
                .chunks(20)
                .map(|id| ObjectId::try_from(id).map_err(|_| corrupt("BASE")))
                .collect::<Result<_, _>>()?,
            None => Vec::new(),
        };
        if bases.len() != base_count {
            return Err(corrupt("BASE does not list the header's base count"));
        }
        let sources: Vec<LayerSource> = match chunks.get(&LOAM_CHUNK) {
            Some(range) if range.len() == base_count * 16 => bytes[range.clone()]
                .chunks(16)
                .map(|entry| {
                    let kind = u64::from_be_bytes(entry[..8].try_into().expect("8"));
                    let seq = u64::from_be_bytes(entry[8..].try_into().expect("8"));
                    if kind == SOURCE_DERIVED {
                        Ok(LayerSource { seq: Seq(seq) })
                    } else {
                        Err(corrupt("unknown base kind"))
                    }
                })
                .collect::<Result<_, _>>()?,
            None if base_count == 0 => Vec::new(),
            _ => return Err(corrupt("no LOAM chunk naming the bases")),
        };
        let (bidx, bdat) = match (chunks.get(&BIDX), chunks.get(&BDAT)) {
            (Some(bidx), Some(bdat)) if bidx.len() == commits as usize * 4 && bdat.len() >= 12 => {
                let header = &bytes[bdat.start..bdat.start + 12];
                let settings = BloomSettings {
                    hash_version: u32::from_be_bytes(header[..4].try_into().expect("4")),
                    num_hashes: u32::from_be_bytes(header[4..8].try_into().expect("4")),
                    bits_per_entry: u32::from_be_bytes(header[8..].try_into().expect("4")),
                };
                if settings == BloomSettings::DEFAULT {
                    (Some(bidx.clone()), Some(bdat.start + 12..bdat.end))
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        };
        let mut map = memmap2::MmapMut::map_anon(bytes.len())
            .map_err(|err| corrupt(&format!("mapping: {err}")))?;
        map.copy_from_slice(&bytes);
        let map = map
            .make_read_only()
            .map_err(|err| corrupt(&format!("mapping: {err}")))?;
        let file =
            gix_commitgraph::File::new(map, PathBuf::from(format!("graph-{checksum}.graph")))
                .map_err(|err| corrupt(&err.to_string()))?;
        if file.num_commits() != commits {
            return Err(corrupt("OIDL and CDAT disagree"));
        }
        Ok(Self {
            bytes,
            checksum,
            commits,
            bases,
            sources,
            bidx,
            bdat,
            file: Some(file),
        })
    }

    /// The trailing checksum, which names the layer in a chain.
    pub fn checksum(&self) -> ObjectId {
        self.checksum
    }

    /// Commits in this layer.
    pub fn commits(&self) -> u32 {
        self.commits
    }

    /// The bases' objects, bottom first.
    pub fn sources(&self) -> &[LayerSource] {
        &self.sources
    }

    /// Whether the layer has (version 2) Bloom filters.
    pub fn has_bloom(&self) -> bool {
        self.bdat.is_some()
    }

    /// The filter of the commit at `local` (its position in this layer).
    fn bloom(&self, local: u32) -> Option<&[u8]> {
        let (bidx, bdat) = (self.bidx.as_ref()?, self.bdat.as_ref()?);
        let read = |i: u32| -> usize {
            let at = bidx.start + i as usize * 4;
            u32::from_be_bytes(self.bytes[at..at + 4].try_into().expect("4")) as usize
        };
        let start = if local == 0 { 0 } else { read(local - 1) };
        let end = read(local);
        (start <= end && bdat.start + end <= bdat.end)
            .then(|| &self.bytes[bdat.start + start..bdat.start + end])
    }
}

/// A commit not in any stored layer, read for one query.
#[derive(Clone, Debug)]
struct MemCommit {
    tree: ObjectId,
    parents: Vec<ObjectId>,
    commit_time: u64,
    generation: u32,
}

/// The commit graph of a repository at a seq (plan GT1 Task 20): a chain
/// of stored layers, plus commits read for one query that no layer holds.
pub struct GraphView {
    graph: Option<gix_commitgraph::Graph>,
    /// Layers bottom first, with their first global position.
    layers: Vec<(u32, LayerSource, Layer)>,
    extra: HashMap<ObjectId, MemCommit>,
    built: bool,
}

impl std::fmt::Debug for GraphView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphView")
            .field("layers", &self.sources())
            .field("stored_commits", &self.stored_commits())
            .field("extra", &self.extra.len())
            .field("built", &self.built)
            .finish()
    }
}

impl GraphView {
    fn new(chain: Vec<(LayerSource, Layer)>) -> Result<Self, FormatError> {
        let mut layers = Vec::with_capacity(chain.len());
        let mut start = 0u32;
        let mut files = Vec::with_capacity(chain.len());
        for (source, mut layer) in chain {
            files.push(
                layer
                    .file
                    .take()
                    .ok_or_else(|| FormatError::corrupt("commit graph layer used twice"))?,
            );
            let commits = layer.commits;
            layers.push((start, source, layer));
            start += commits;
        }
        let graph = if files.is_empty() {
            None
        } else {
            Some(
                gix_commitgraph::Graph::new(files)
                    .map_err(|err| FormatError::corrupt(format!("commit graph chain: {err}")))?,
            )
        };
        Ok(Self {
            graph,
            layers,
            extra: HashMap::new(),
            built: false,
        })
    }

    /// The layers, bottom first.
    pub fn sources(&self) -> Vec<LayerSource> {
        self.layers.iter().map(|(_, source, _)| *source).collect()
    }

    /// Whether opening this view stored a new layer.
    pub fn built_layer(&self) -> bool {
        self.built
    }

    /// Commits in the stored layers.
    pub fn stored_commits(&self) -> u32 {
        self.layers.iter().map(|(_, _, layer)| layer.commits).sum()
    }

    /// Whether the graph knows `oid`.
    pub fn contains(&self, oid: &ObjectId) -> bool {
        self.extra.contains_key(oid) || self.position(oid).is_some()
    }

    fn position(&self, oid: &ObjectId) -> Option<u32> {
        self.graph.as_ref()?.lookup(oid).map(|pos| pos.0)
    }

    /// The commit `oid`, or `None` when the graph does not know it.
    pub fn commit(&self, oid: &ObjectId) -> Option<GraphCommit> {
        if let Some(mem) = self.extra.get(oid) {
            return Some(GraphCommit {
                tree: mem.tree,
                parents: mem.parents.clone(),
                generation: mem.generation,
                commit_time: mem.commit_time,
            });
        }
        let graph = self.graph.as_ref()?;
        let pos = graph.lookup(oid)?;
        let commit = graph.commit_at(pos);
        let parents = commit
            .iter_parents()
            .map(|parent| parent.ok().map(|pos| graph.id_at(pos).to_owned()))
            .collect::<Option<Vec<_>>>()?;
        Some(GraphCommit {
            tree: commit.root_tree_id().to_owned(),
            parents,
            generation: commit.generation(),
            commit_time: commit.committer_timestamp(),
        })
    }

    /// The changed-path filter of `oid` against its first parent, or `None`
    /// when no layer has one for it.
    pub fn bloom(&self, oid: &ObjectId) -> Option<&[u8]> {
        let pos = self.position(oid)?;
        let at = self
            .layers
            .partition_point(|(start, _, _)| *start <= pos)
            .checked_sub(1)?;
        let (start, _, layer) = &self.layers[at];
        layer.bloom(pos - start)
    }

    /// Whether `oid`'s first-parent diff may touch `paths` (any of them):
    /// `Some(false)` is certain, `None` means no filter answers.
    pub fn bloom_may_touch(&self, oid: &ObjectId, paths: &[Vec<u8>]) -> Option<bool> {
        let filter = self.bloom(oid)?;
        let settings = BloomSettings::DEFAULT;
        Some(
            paths
                .iter()
                .any(|path| maybe_contains(&settings, filter, path)),
        )
    }

    /// Adds the commits reachable from `tips` that the graph lacks, read
    /// from `odb` for this query only (not stored).
    pub async fn extend(&mut self, odb: &RangeOdb, tips: &[ObjectId]) -> Result<(), DerivedError> {
        let found = collect(odb, self, tips).await?;
        let generations = generations(self, &found);
        for (oid, commit) in found {
            let generation = generations[&oid];
            self.extra.insert(
                oid,
                MemCommit {
                    tree: commit.tree,
                    parents: commit.parents,
                    commit_time: commit.commit_time,
                    generation,
                },
            );
        }
        Ok(())
    }
}

/// A commit read from the object database.
#[derive(Clone, Debug)]
struct ReadCommit {
    tree: ObjectId,
    parents: Vec<ObjectId>,
    commit_time: u64,
}

/// The commit `oid` from `odb`: its tree, parents and committer time.
pub(crate) fn parse_commit(
    oid: &ObjectId,
    data: &[u8],
) -> Result<(ObjectId, Vec<ObjectId>, u64), OdbError> {
    let commit = gix_object::CommitRef::from_bytes(data, gix_hash::Kind::Sha1).map_err(|err| {
        OdbError::CorruptObject {
            oid: *oid,
            reason: err.to_string(),
        }
    })?;
    let time = crate::history::log::signature_seconds(commit.committer.as_ref());
    Ok((
        commit.tree(),
        commit.parents().collect(),
        u64::try_from(time).unwrap_or(0),
    ))
}

/// Every commit reachable from `tips` (tags peeled; tips that are not
/// commits skipped) that `view` lacks, read from `odb`.
async fn collect(
    odb: &RangeOdb,
    view: &GraphView,
    tips: &[ObjectId],
) -> Result<HashMap<ObjectId, ReadCommit>, DerivedError> {
    let mut found: HashMap<ObjectId, ReadCommit> = HashMap::new();
    let mut stack: Vec<ObjectId> = Vec::new();
    for tip in tips {
        let (kind, oid) = peel(odb, tip).await?;
        if kind == Kind::Commit {
            stack.push(oid);
        }
    }
    while let Some(oid) = stack.pop() {
        if found.contains_key(&oid) || view.contains(&oid) {
            continue;
        }
        let (kind, data) = crate::odb::ObjectDb::read(odb, &oid).await?;
        if kind != Kind::Commit {
            return Err(DerivedError::WrongKind {
                oid,
                expected: "commit",
                found: crate::history::kind_name(kind),
            });
        }
        let (tree, parents, commit_time) = parse_commit(&oid, &data)?;
        stack.extend(parents.iter().copied());
        found.insert(
            oid,
            ReadCommit {
                tree,
                parents,
                commit_time,
            },
        );
    }
    Ok(found)
}

/// The topological levels of `found`, whose parents are in `found` or
/// `view`.
fn generations(view: &GraphView, found: &HashMap<ObjectId, ReadCommit>) -> HashMap<ObjectId, u32> {
    let mut out: HashMap<ObjectId, u32> = HashMap::with_capacity(found.len());
    for start in found.keys() {
        if out.contains_key(start) {
            continue;
        }
        // An explicit post-order walk: (commit, parents visited).
        let mut stack = vec![(*start, false)];
        while let Some((oid, expanded)) = stack.pop() {
            if out.contains_key(&oid) {
                continue;
            }
            let commit = &found[&oid];
            if !expanded {
                stack.push((oid, true));
                for parent in &commit.parents {
                    if found.contains_key(parent) && !out.contains_key(parent) {
                        stack.push((*parent, false));
                    }
                }
                continue;
            }
            let highest = commit
                .parents
                .iter()
                .map(|parent| {
                    out.get(parent)
                        .copied()
                        .or_else(|| view.commit(parent).map(|c| c.generation))
                        .unwrap_or(0)
                })
                .max()
                .unwrap_or(0);
            out.insert(oid, highest.saturating_add(1).min(GENERATION_V1_MAX));
        }
    }
    out
}

/// The Bloom filter of a commit with root tree `tree` whose first parent
/// has root tree `parent` (`None` for a root commit).
async fn filter_for(
    reader: &TreeReader<'_>,
    parent: Option<ObjectId>,
    tree: ObjectId,
) -> Result<Vec<u8>, DerivedError> {
    let changes = diff_trees(
        reader,
        parent,
        Some(tree),
        &Pathspec::all(),
        Some(BLOOM_MAX_CHANGED_PATHS),
    )
    .await?;
    let changed = if changes.len() > BLOOM_MAX_CHANGED_PATHS {
        ChangedPaths::TooMany
    } else {
        ChangedPaths::Files(changes.into_iter().map(|change| change.path).collect())
    };
    Ok(build_filter(&BloomSettings::DEFAULT, &changed))
}

/// Reads the layer at `source` and its chain, bottom first.
async fn load_chain(
    derived: &DerivedStore,
    source: LayerSource,
    top: Bytes,
) -> Result<Vec<(LayerSource, Layer)>, DerivedError> {
    let top = Layer::parse(top)?;
    let mut chain = Vec::with_capacity(top.bases.len() + 1);
    for (i, (base_source, checksum)) in top.sources.iter().zip(&top.bases).enumerate() {
        let path = derived.paths().derived_graph(base_source.seq);
        let bytes = derived
            .get(&path)
            .await?
            .ok_or_else(|| FormatError::corrupt(format!("commit graph base {path} is missing")))?;
        let base = Layer::parse(bytes)?;
        if base.checksum != *checksum || base.bases.len() != i || base.bases != top.bases[..i] {
            return Err(FormatError::corrupt(format!(
                "commit graph base {path} does not match the chain"
            ))
            .into());
        }
        chain.push((*base_source, base));
    }
    chain.push((source, top));
    Ok(chain)
}

/// The newest stored layer, by its seq.
async fn newest_layer(derived: &DerivedStore) -> Result<Option<LayerSource>, DerivedError> {
    let prefix = derived.paths().derived_graph_prefix();
    let listed = derived.list(&prefix).await?;
    Ok(listed
        .iter()
        .filter_map(|path| path.strip_prefix(&prefix))
        .filter_map(|name| name.strip_suffix(".graph"))
        .filter_map(|seq| seq.parse::<u64>().ok())
        .max()
        .map(|seq| LayerSource { seq: Seq(seq) }))
}

/// The commit graph of `snapshot` (plan GT1 Task 20): the layer of its
/// seq if one is stored, else the newest stored chain, extended by a new
/// layer `derived/graph/<seq>.graph` (create-only) when `snapshot`'s refs
/// reach commits the chain lacks. Two nodes building one seq at once store
/// one layer: the PUT that loses reads the winner's. A stored layer that
/// does not parse is logged and built around (Task 55's scrub removes it).
pub async fn graph_view(
    odb: &RangeOdb,
    derived: &DerivedStore,
    snapshot: &RefSnapshot,
) -> Result<GraphView, DerivedError> {
    let here = LayerSource { seq: snapshot.seq };
    let path = derived.paths().derived_graph(snapshot.seq);
    if let Some(bytes) = derived.get(&path).await? {
        match load_chain(derived, here, bytes).await {
            Ok(chain) => return Ok(GraphView::new(chain)?),
            Err(err) => {
                tracing::warn!(path, error = %err, "unreadable commit graph layer; rebuilding")
            }
        }
    }
    let mut chain = Vec::new();
    if let Some(newest) = newest_layer(derived).await?
        && newest != here
    {
        let newest_path = derived.paths().derived_graph(newest.seq);
        if let Some(bytes) = derived.get(&newest_path).await? {
            match load_chain(derived, newest, bytes).await {
                Ok(loaded) => chain = loaded,
                Err(err) => {
                    tracing::warn!(path = newest_path, error = %err, "unreadable commit graph layer; building without it");
                }
            }
        }
    }
    let view = GraphView::new(chain)?;
    let tips: Vec<ObjectId> = snapshot
        .refs
        .values()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let found = collect(odb, &view, &tips).await?;
    if found.is_empty() {
        return Ok(view);
    }
    let generations = generations(&view, &found);
    let reader = TreeReader::new(odb);
    let mut commits = Vec::with_capacity(found.len());
    for (oid, commit) in &found {
        let parent_tree = match commit.parents.first() {
            None => None,
            Some(parent) => Some(match found.get(parent) {
                Some(read) => read.tree,
                None => {
                    view.commit(parent)
                        .ok_or(DerivedError::Odb(OdbError::NotFound(*parent)))?
                        .tree
                }
            }),
        };
        commits.push(LayerCommit {
            oid: *oid,
            tree: commit.tree,
            parents: commit.parents.clone(),
            generation: generations[oid],
            commit_time: commit.commit_time,
            bloom: filter_for(&reader, parent_tree, commit.tree).await?,
        });
    }
    // Merge the layers below while they are small (ruling T20.1).
    let mut keep = view.layers.len();
    let mut merged = commits.len() as u64;
    while keep > 0 {
        let below = u64::from(view.layers[keep - 1].2.commits);
        if below >= 2 * merged && keep < MAX_GRAPH_LAYERS {
            break;
        }
        merged += below;
        keep -= 1;
    }
    for (start, _, layer) in &view.layers[keep..] {
        for local in 0..layer.commits {
            let pos = start + local;
            let graph = view.graph.as_ref().expect("a graph with layers");
            let oid = graph.id_at(gix_commitgraph::Position(pos)).to_owned();
            let commit = view.commit(&oid).expect("a stored commit");
            let bloom = match layer.bloom(local) {
                Some(filter) => filter.to_vec(),
                None => {
                    let parent_tree = match commit.parents.first() {
                        Some(parent) => Some(
                            view.commit(parent)
                                .ok_or(DerivedError::Odb(OdbError::NotFound(*parent)))?
                                .tree,
                        ),
                        None => None,
                    };
                    filter_for(&reader, parent_tree, commit.tree).await?
                }
            };
            commits.push(LayerCommit {
                oid,
                tree: commit.tree,
                parents: commit.parents,
                generation: commit.generation,
                commit_time: commit.commit_time,
                bloom,
            });
        }
    }
    let bases: Vec<(LayerSource, ObjectId, u32)> = view.layers[..keep]
        .iter()
        .map(|(_, source, layer)| (*source, layer.checksum, layer.commits))
        .collect();
    let below: u32 = bases.iter().map(|(_, _, count)| count).sum();
    let graph = view.graph.as_ref();
    let bytes = encode_layer(commits, &bases, |oid| {
        graph?
            .lookup(oid)
            .map(|pos| pos.0)
            .filter(|pos| *pos < below)
    })?;
    let stored = derived.put(&path, bytes.clone()).await?;
    let chain_of = |bytes: Bytes| load_chain(derived, here, bytes);
    let mut built = if stored == bytes {
        GraphView::new(chain_of(bytes).await?)?
    } else {
        // Another node stored this seq's layer first; use it, unless it
        // does not read, in which case this build stands.
        match chain_of(stored).await {
            Ok(chain) => GraphView::new(chain)?,
            Err(err) => {
                tracing::warn!(path, error = %err, "stored commit graph layer is unreadable; using a fresh build");
                GraphView::new(chain_of(bytes).await?)?
            }
        }
    };
    built.built = true;
    Ok(built)
}
