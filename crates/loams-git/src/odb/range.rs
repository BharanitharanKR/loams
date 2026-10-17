//! The range-read object database (§36 §5.3, §19.5.1; plan GT1 Task 17,
//! old GT2 Task 3).
//!
//! [`RangeOdb`] finds and reads objects in a snapshot's packs with range
//! reads, never downloading a whole pack or idx:
//!
//! - **Lookup.** A pack's idx (version 2) is read lazily: the fan-out
//!   table (1 KiB) once per pack, then, per lookup, the 4 KiB-aligned
//!   pages of the oid table that hold the object's first-byte bucket, then
//!   its 4-byte offset (and an 8-byte large offset when the pack is over
//!   2 GiB). A multi-pack index ([`RangeOdb::with_midx`]), when present,
//!   answers for the packs it covers in the same way.
//! - **Entries.** An entry is read from its offset in ranges of at least
//!   `MIN_COALESCED_RANGE` (64 KiB): its compressed length is unknown until
//!   it is inflated, so a read takes more when the zlib stream runs on.
//! - **Headers.** [`RangeOdb::header`] answers an object's kind and size
//!   from entry headers alone. A delta's result size is the second varint
//!   of its delta data, so a few bytes of it are inflated; the object is
//!   never inflated whole, and its kind is its base's, found by following
//!   the chain's headers.
//! - **Deltas.** [`ObjectDb::read`] resolves offset and ref deltas; bases
//!   that were resolved are kept in a small cache in memory.
//!
//! Every range goes through the [`BlobStore`], which serves it from the H1
//! cache when it has one ([`crate::StoreBlobStore::with_range_cache`]).
//! Packs are immutable, so nothing is invalidated, and every cache here
//! (fan-outs, the multi-pack index's tables, resolved bases) can be dropped
//! at any time ([`RangeOdb::drop_caches`]).
//!
//! Lookup order is this repository's packs (newest first), then each fork
//! parent's at its seq (§36 §4.7), and last, for a [`super::WsOdb`], the
//! workspace's packs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;
use gix_object::Kind;
use gix_pack::data::entry::Header;
use gix_zlib::{Decompress, FlushDecompress, Status};
use loams_store::Store;

use super::{DbLookup, Lookup, ObjectDb, OdbError, walk_closure};
use crate::blob::{BlobId, BlobStore};
use crate::format::pb;
use crate::ids::{ObjectId, RepoId, Seq};
use crate::limits::{MAX_FORK_DEPTH, MIN_COALESCED_RANGE};
use crate::reflog::RefSnapshot;

/// Bytes of a SHA-1 object id.
const OID_LEN: u64 = 20;
/// Idx (version 2) header: magic and version.
const IDX_HEADER: u64 = 8;
/// The fan-out table: 256 big-endian counts.
const FANOUT_LEN: u64 = 256 * 4;
/// The idx version 2 magic, `\377tOc`.
const IDX_MAGIC: [u8; 4] = [0xff, b't', b'O', b'c'];
/// Oid-table reads are aligned to pages of this size.
const PAGE: u64 = 4096;
/// A pack's trailer: the SHA-1 of everything before it.
const PACK_TRAILER: u64 = 20;
/// The largest single read made to continue an entry's zlib stream.
const MAX_ENTRY_READ: u64 = 8 * 1024 * 1024;
/// A delta chain longer than this is corrupt (git's own limit is 4,095).
const MAX_DELTA_CHAIN: usize = 10_000;
/// Bytes of resolved delta bases kept in memory per database.
const BASE_CACHE_BYTES: usize = 32 * 1024 * 1024;
/// zlib's largest expansion is about 1032 to 1; a little more is allowed.
const MAX_INFLATE_RATIO: u64 = 1100;
/// Delta-data bytes inflated to read a delta's two size varints (at most
/// ten bytes each).
const DELTA_SIZES_PREFIX: usize = 20;

/// Where an object's entry is: which layer of the lookup chain (0 is the
/// repository, then its fork parents, then a workspace's packs), which pack
/// of that layer, and the entry's offset in the pack.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Location {
    /// The layer: 0 for the repository's own packs, `n` for its `n`th fork
    /// parent, and the last for a workspace's packs.
    pub layer: usize,
    /// The pack within the layer, newest first.
    pub pack: usize,
    /// The entry's offset in the pack.
    pub offset: u64,
}

/// How an entry is stored, when it is a delta.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaBase {
    /// An offset delta: the base is the entry at this offset of the same pack.
    Offset(u64),
    /// A ref delta: the base is this object.
    Ref(ObjectId),
}

/// What an entry's header says (plan GT1 Task 17).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryHeader {
    /// The object's kind (a delta's is its chain's base's).
    pub kind: Kind,
    /// The object's size (a delta's is its result size), as `git cat-file -s`.
    pub size: u64,
    /// The base, when the entry is stored as a delta.
    pub base: Option<DeltaBase>,
}

/// Counters of a [`RangeOdb`], for tests and metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OdbStats {
    /// Entries inflated whole (object and delta data read in full).
    pub inflates: u64,
    /// Delta entries of which only the size prefix was inflated.
    pub prefix_inflates: u64,
    /// Delta bases served from the resolved-base cache.
    pub base_hits: u64,
    /// Lookups the multi-pack index answered.
    pub midx_hits: u64,
}

#[derive(Debug, Default)]
struct Counters {
    inflates: AtomicU64,
    prefix_inflates: AtomicU64,
    base_hits: AtomicU64,
    midx_hits: AtomicU64,
}

/// A source of byte ranges of one immutable object: how a multi-pack
/// index is read ([`RangeOdb::with_midx`]).
#[async_trait]
pub trait RangeSource: Send + Sync + fmt::Debug {
    /// The object's length.
    fn len(&self) -> u64;
    /// Whether the object is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Bytes `range`, which lies inside the object.
    async fn read(&self, range: Range<u64>) -> Result<Bytes, OdbError>;
}

/// A [`RangeSource`] over an object of a `Store` (for example compaction's
/// `midx/<seq:020>.midx`, §36 §4.1).
#[derive(Clone, Debug)]
pub struct StoreRange {
    store: Store,
    path: String,
    len: u64,
}

impl StoreRange {
    /// The object at `path`, which is `len` bytes.
    pub fn new(store: Store, path: impl Into<String>, len: u64) -> Self {
        Self {
            store,
            path: path.into(),
            len,
        }
    }
}

#[async_trait]
impl RangeSource for StoreRange {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read(&self, range: Range<u64>) -> Result<Bytes, OdbError> {
        let bytes = self
            .store
            .get_range(&self.path, range.clone())
            .await
            .map_err(|err| OdbError::Blob(err.into()))?;
        if bytes.len() as u64 != range.end - range.start {
            return Err(OdbError::CorruptPack {
                pack: self.path.clone(),
                reason: format!(
                    "range {}..{} read {} bytes",
                    range.start,
                    range.end,
                    bytes.len()
                ),
            });
        }
        Ok(bytes)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn be_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn be_u64(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_be_bytes(word)
}

/// A fan-out table: `fanout[b]` objects have a first byte of at most `b`.
type Fanout = Arc<[u32; 256]>;

fn parse_fanout(bytes: &[u8]) -> Result<Fanout, String> {
    if bytes.len() < FANOUT_LEN as usize {
        return Err("fan-out table truncated".into());
    }
    let mut fanout = [0u32; 256];
    for (i, slot) in fanout.iter_mut().enumerate() {
        *slot = be_u32(bytes, i * 4);
    }
    if fanout.windows(2).any(|w| w[0] > w[1]) {
        return Err("fan-out table is not monotonic".into());
    }
    Ok(Arc::new(fanout))
}

/// The entries whose oid starts with `oid`'s first byte, as an index range.
fn bucket(fanout: &[u32; 256], oid: &ObjectId) -> Range<u64> {
    let first = usize::from(oid.as_bytes()[0]);
    let lo = if first == 0 { 0 } else { fanout[first - 1] };
    u64::from(lo)..u64::from(fanout[first])
}

/// Reads the oid-table entries `entries` (whose table starts at `table`)
/// in 4 KiB-aligned pages bounded by `limit`, and binary-searches `oid`;
/// returns its index. `label` names the index in errors.
async fn search_oids<F, Fut>(
    read: F,
    table: u64,
    entries: Range<u64>,
    limit: u64,
    oid: &ObjectId,
    label: &str,
) -> Result<Option<u64>, OdbError>
where
    F: FnOnce(Range<u64>) -> Fut,
    Fut: std::future::Future<Output = Result<Bytes, OdbError>>,
{
    if entries.is_empty() {
        return Ok(None);
    }
    let start = table + entries.start * OID_LEN;
    let end = table + entries.end * OID_LEN;
    let page_start = start - start % PAGE;
    if end > limit {
        return Err(OdbError::CorruptPack {
            pack: label.to_string(),
            reason: "the oid table runs past its index".into(),
        });
    }
    let page_end = end.div_ceil(PAGE).saturating_mul(PAGE).min(limit);
    let pages = read(page_start..page_end).await?;
    let at = (start - page_start) as usize;
    let names = &pages[at..at + ((end - start) as usize)];
    let target = oid.as_bytes();
    let count = names.len() / OID_LEN as usize;
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let name = &names[mid * OID_LEN as usize..(mid + 1) * OID_LEN as usize];
        match name.cmp(target) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Some(entries.start + mid as u64)),
        }
    }
    Ok(None)
}

/// One pack of a layer, with its lazily read fan-out table.
#[derive(Debug)]
struct PackState {
    pack: pb::PackRef,
    id: BlobId,
    objects: u64,
    fanout: Mutex<Option<Fanout>>,
}

/// A multi-pack index over some of a layer's packs (git's `MIDX` format,
/// version 1 or 2, SHA-1), read by range.
#[derive(Debug)]
struct Midx {
    source: Arc<dyn RangeSource>,
    tables: Mutex<Option<Arc<MidxTables>>>,
}

/// What a lookup in a multi-pack index needs, read once.
#[derive(Debug)]
struct MidxTables {
    fanout: Fanout,
    /// Offsets of the `OIDL`, `OOFF` and (when present) `LOFF` chunks.
    oidl: u64,
    ooff: u64,
    loff: Option<Range<u64>>,
    /// The layer's pack index of each pack the multi-pack index names, or
    /// `None` for one the snapshot does not hold.
    packs: Vec<Option<usize>>,
    /// The layer's packs the index covers.
    covered: HashSet<usize>,
}

/// What a multi-pack index says of an object.
enum MidxAnswer {
    /// In this pack of the layer, at this offset.
    Found(usize, u64),
    /// Not in any pack it covers (the tables say which).
    Absent(Arc<MidxTables>),
    /// In a pack the snapshot does not hold: the index is stale, so every
    /// pack is searched.
    Stale,
}

/// A chunk id of the multi-pack index format.
fn chunk_id(name: &[u8; 4]) -> u32 {
    u32::from_be_bytes(*name)
}

impl Midx {
    fn corrupt(&self, reason: impl Into<String>) -> OdbError {
        OdbError::CorruptPack {
            pack: "multi-pack-index".into(),
            reason: reason.into(),
        }
    }

    async fn tables(&self, packs: &[PackState]) -> Result<Arc<MidxTables>, OdbError> {
        if let Some(tables) = lock(&self.tables).clone() {
            return Ok(tables);
        }
        let len = self.source.len();
        let head = self.source.read(0..PAGE.min(len)).await?;
        if head.len() < 12 || &head[..4] != b"MIDX" {
            return Err(self.corrupt("not a multi-pack index"));
        }
        if !(1..=2).contains(&head[4]) || head[5] != 1 {
            return Err(self.corrupt(format!(
                "version {} with hash {} is not served",
                head[4], head[5]
            )));
        }
        if head[7] != 0 {
            return Err(self.corrupt("incremental multi-pack indexes are not served"));
        }
        let chunks = usize::from(head[6]);
        let pack_count = be_u32(&head, 8) as usize;
        let table_end = 12 + (chunks + 1) * 12;
        let head = if head.len() < table_end {
            self.source.read(0..table_end as u64).await?
        } else {
            head
        };
        let mut found: HashMap<u32, Range<u64>> = HashMap::new();
        for i in 0..chunks {
            let at = 12 + i * 12;
            let id = be_u32(&head, at);
            let start = be_u64(&head, at + 4);
            let end = be_u64(&head, at + 16);
            if start > end || end > len {
                return Err(self.corrupt("chunk table out of bounds"));
            }
            found.insert(id, start..end);
        }
        let chunk = |name: &[u8; 4]| {
            found
                .get(&chunk_id(name))
                .cloned()
                .ok_or_else(|| self.corrupt(format!("no {} chunk", String::from_utf8_lossy(name))))
        };
        let pnam = chunk(b"PNAM")?;
        let oidf = chunk(b"OIDF")?;
        let oidl = chunk(b"OIDL")?;
        let ooff = chunk(b"OOFF")?;
        let loff = found.get(&chunk_id(b"LOFF")).cloned();
        if oidf.end - oidf.start != FANOUT_LEN {
            return Err(self.corrupt("OIDF is not 1 KiB"));
        }
        let fanout = parse_fanout(&self.source.read(oidf).await?).map_err(|e| self.corrupt(e))?;
        let objects = u64::from(fanout[255]);
        if oidl.end - oidl.start != objects * OID_LEN || ooff.end - ooff.start != objects * 8 {
            return Err(self.corrupt("OIDL or OOFF does not match the object count"));
        }
        let names = self.source.read(pnam).await?;
        let by_checksum: HashMap<&str, usize> = packs
            .iter()
            .enumerate()
            .map(|(i, state)| (state.pack.checksum.as_str(), i))
            .collect();
        let named: Vec<Option<usize>> = names
            .split(|b| *b == 0)
            .filter(|name| !name.is_empty())
            .map(|name| {
                let name = std::str::from_utf8(name).unwrap_or_default();
                let stem = name.strip_prefix("pack-").unwrap_or(name);
                let stem = stem.rsplit_once('.').map_or(stem, |(stem, _)| stem);
                by_checksum.get(stem).copied()
            })
            .collect();
        if named.len() != pack_count {
            return Err(self.corrupt(format!(
                "PNAM names {} packs, the header {pack_count}",
                named.len()
            )));
        }
        let covered = named.iter().flatten().copied().collect();
        let tables = Arc::new(MidxTables {
            fanout,
            oidl: oidl.start,
            ooff: ooff.start,
            loff,
            packs: named,
            covered,
        });
        *lock(&self.tables) = Some(Arc::clone(&tables));
        Ok(tables)
    }

    /// What the index says of `oid`.
    async fn lookup(&self, packs: &[PackState], oid: &ObjectId) -> Result<MidxAnswer, OdbError> {
        let tables = self.tables(packs).await?;
        let len = self.source.len();
        let read = |range: Range<u64>| self.source.read(range);
        let Some(index) = search_oids(
            read,
            tables.oidl,
            bucket(&tables.fanout, oid),
            len,
            oid,
            "multi-pack-index",
        )
        .await?
        else {
            return Ok(MidxAnswer::Absent(tables));
        };
        let at = tables.ooff + index * 8;
        let entry = self.source.read(at..at + 8).await?;
        let pack = be_u32(&entry, 0) as usize;
        let small = be_u32(&entry, 4);
        let offset = if small & 0x8000_0000 == 0 {
            u64::from(small)
        } else {
            let loff = tables
                .loff
                .clone()
                .ok_or_else(|| self.corrupt("a large offset without LOFF"))?;
            let at = loff.start + u64::from(small & 0x7fff_ffff) * 8;
            if at + 8 > loff.end {
                return Err(self.corrupt("large offset out of bounds"));
            }
            be_u64(&self.source.read(at..at + 8).await?, 0)
        };
        match tables.packs.get(pack) {
            Some(Some(local)) => Ok(MidxAnswer::Found(*local, offset)),
            Some(None) => Ok(MidxAnswer::Stale),
            None => Err(self.corrupt(format!("pack id {pack} out of range"))),
        }
    }
}

/// One layer of the lookup chain: a blob store and the packs in it.
#[derive(Debug)]
pub(crate) struct Layer {
    blobs: Arc<dyn BlobStore>,
    /// Newest first.
    packs: Vec<PackState>,
    midx: Option<Midx>,
}

impl Layer {
    pub(crate) fn new(blobs: Arc<dyn BlobStore>, packs: &[pb::PackRef]) -> Result<Self, OdbError> {
        let packs = packs
            .iter()
            .rev()
            .map(|pack| {
                let id = BlobId::parse(&pack.checksum).map_err(|err| corrupt_pack(pack, &err))?;
                Ok(PackState {
                    pack: pack.clone(),
                    id,
                    objects: pack.object_count,
                    fanout: Mutex::new(None),
                })
            })
            .collect::<Result<Vec<_>, OdbError>>()?;
        Ok(Self {
            blobs,
            packs,
            midx: None,
        })
    }

    fn drop_caches(&self) {
        for pack in &self.packs {
            *lock(&pack.fanout) = None;
        }
        if let Some(midx) = &self.midx {
            *lock(&midx.tables) = None;
        }
    }

    /// Bytes `range` of `pack`'s idx section.
    async fn idx_range(&self, state: &PackState, range: Range<u64>) -> Result<Bytes, OdbError> {
        let pack = &state.pack;
        if range.end > pack.idx_len {
            return Err(corrupt_pack(pack, &"idx read past its section"));
        }
        let at = pack.idx_offset;
        Ok(self
            .blobs
            .get_range(&state.id, at + range.start..at + range.end)
            .await?)
    }

    /// Bytes `range` of `pack`'s pack section.
    async fn pack_range(&self, state: &PackState, range: Range<u64>) -> Result<Bytes, OdbError> {
        if range.end > state.pack.pack_len {
            return Err(corrupt_pack(&state.pack, &"read past the pack section"));
        }
        Ok(self.blobs.get_range(&state.id, range).await?)
    }

    async fn fanout(&self, state: &PackState) -> Result<Fanout, OdbError> {
        if let Some(fanout) = lock(&state.fanout).clone() {
            return Ok(fanout);
        }
        let head = self.idx_range(state, 0..IDX_HEADER + FANOUT_LEN).await?;
        if head[..4] != IDX_MAGIC || be_u32(&head, 4) != 2 {
            return Err(corrupt_pack(&state.pack, &"not a version 2 idx"));
        }
        let fanout = parse_fanout(&head[IDX_HEADER as usize..])
            .map_err(|reason| corrupt_pack(&state.pack, &reason))?;
        if u64::from(fanout[255]) != state.objects {
            return Err(corrupt_pack(
                &state.pack,
                &format!(
                    "idx holds {} objects, the PackRef {}",
                    fanout[255], state.objects
                ),
            ));
        }
        *lock(&state.fanout) = Some(Arc::clone(&fanout));
        Ok(fanout)
    }

    /// The offset of `oid` in pack `state`, from its idx.
    async fn idx_lookup(&self, state: &PackState, oid: &ObjectId) -> Result<Option<u64>, OdbError> {
        let fanout = self.fanout(state).await?;
        let n = state.objects;
        let table = IDX_HEADER + FANOUT_LEN;
        let read = |range: Range<u64>| self.idx_range(state, range);
        let limit = state.pack.idx_len;
        let Some(index) = search_oids(
            read,
            table,
            bucket(&fanout, oid),
            limit,
            oid,
            &state.pack.checksum,
        )
        .await?
        else {
            return Ok(None);
        };
        // The CRC table follows the names, then the 32-bit offsets.
        let offsets = table + n * OID_LEN + n * 4;
        let at = offsets + index * 4;
        let small = be_u32(&self.idx_range(state, at..at + 4).await?, 0);
        if small & 0x8000_0000 == 0 {
            return Ok(Some(u64::from(small)));
        }
        let at = offsets + n * 4 + u64::from(small & 0x7fff_ffff) * 8;
        Ok(Some(be_u64(&self.idx_range(state, at..at + 8).await?, 0)))
    }

    /// The pack and offset of `oid` in this layer, and whether the
    /// multi-pack index answered.
    async fn locate(&self, oid: &ObjectId) -> Result<Option<(usize, u64, bool)>, OdbError> {
        let covered = match &self.midx {
            Some(midx) => match midx.lookup(&self.packs, oid).await? {
                MidxAnswer::Found(pack, offset) => return Ok(Some((pack, offset, true))),
                MidxAnswer::Absent(tables) => Some(tables),
                MidxAnswer::Stale => None,
            },
            None => None,
        };
        for (i, state) in self.packs.iter().enumerate() {
            if covered.as_ref().is_some_and(|t| t.covered.contains(&i)) {
                continue;
            }
            if let Some(offset) = self.idx_lookup(state, oid).await? {
                return Ok(Some((i, offset, false)));
            }
        }
        Ok(None)
    }
}

fn corrupt_pack(pack: &pb::PackRef, err: &dyn fmt::Display) -> OdbError {
    OdbError::CorruptPack {
        pack: pack.checksum.clone(),
        reason: err.to_string(),
    }
}

/// An entry's header and the bytes read from its start.
struct Raw {
    entry: gix_pack::data::Entry,
    /// Bytes of the pack from the entry's offset.
    chunk: Bytes,
}

/// How much of an entry's zlib stream to inflate.
#[derive(Clone, Copy)]
enum Want {
    /// All of it.
    All,
    /// At most this many output bytes.
    Prefix(usize),
}

/// Resolved delta bases, first in first out within a byte budget.
#[derive(Debug, Default)]
struct BaseCache {
    entries: HashMap<Location, (Kind, Bytes)>,
    order: VecDeque<Location>,
    bytes: usize,
}

impl BaseCache {
    fn get(&self, loc: &Location) -> Option<(Kind, Bytes)> {
        self.entries.get(loc).cloned()
    }

    fn insert(&mut self, loc: Location, kind: Kind, data: Bytes) {
        if data.len() > BASE_CACHE_BYTES / 4 || self.entries.contains_key(&loc) {
            return;
        }
        self.bytes += data.len();
        self.entries.insert(loc, (kind, data));
        self.order.push_back(loc);
        while self.bytes > BASE_CACHE_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some((_, data)) = self.entries.remove(&old) {
                self.bytes -= data.len();
            }
        }
    }
}

/// An [`ObjectDb`] that reads a snapshot's packs by range (plan GT1
/// Task 17), with fork fall-through.
#[derive(Clone)]
pub struct RangeOdb {
    repo: RepoId,
    seq: Seq,
    /// Fork parents below this database.
    depth: usize,
    /// The lookup chain: this repository, its parents, and for a
    /// workspace's view its packs last.
    layers: Vec<Arc<Layer>>,
    bases: Arc<Mutex<BaseCache>>,
    counters: Arc<Counters>,
}

impl fmt::Debug for RangeOdb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeOdb")
            .field("repo", &self.repo)
            .field("seq", &self.seq)
            .field("depth", &self.depth)
            .field("layers", &self.layers.len())
            .finish_non_exhaustive()
    }
}

impl RangeOdb {
    /// The objects of repository `repo`'s `snapshot`, whose packs are in
    /// `blobs` (with the H1 cache, when the blob store has one), falling
    /// through to `parent`, the snapshot's fork parent opened at the
    /// parent's seq on the parent's blob store. Refuses a parent the
    /// snapshot does not name, a missing one, another repository, one at
    /// another seq (`ParentMismatch`), and a chain deeper than
    /// `MAX_FORK_DEPTH` (`ForkTooDeep`), as [`super::PackCacheOdb`] does.
    /// Nothing is read until the first lookup.
    pub async fn open(
        repo: RepoId,
        blobs: Arc<dyn BlobStore>,
        snapshot: &RefSnapshot,
        parent: Option<Arc<RangeOdb>>,
    ) -> Result<Self, OdbError> {
        let mismatch = |reason: String| OdbError::ParentMismatch { reason };
        let depth = match (&snapshot.parent, &parent) {
            (None, None) => 0,
            (Some(named), Some(parent)) => {
                if named.repo_id != parent.repo.as_str() {
                    return Err(mismatch(format!(
                        "the snapshot forks {}, the parent offered is {}",
                        named.repo_id, parent.repo
                    )));
                }
                if named.seq != parent.seq.0 {
                    return Err(mismatch(format!(
                        "the snapshot forks {} at seq {}, the parent offered is at seq {}",
                        named.repo_id, named.seq, parent.seq
                    )));
                }
                parent.depth + 1
            }
            (Some(named), None) => {
                return Err(mismatch(format!(
                    "the snapshot forks {} at seq {}, and no parent was offered",
                    named.repo_id, named.seq
                )));
            }
            (None, Some(_)) => {
                return Err(mismatch(
                    "a parent was offered for a snapshot that is not a fork".into(),
                ));
            }
        };
        if depth > MAX_FORK_DEPTH {
            return Err(OdbError::ForkTooDeep {
                max: MAX_FORK_DEPTH,
            });
        }
        let mut layers = vec![Arc::new(Layer::new(blobs, &snapshot.packs)?)];
        if let Some(parent) = &parent {
            layers.extend(parent.layers.iter().cloned());
        }
        Ok(Self {
            repo,
            seq: snapshot.seq,
            depth,
            layers,
            bases: Arc::default(),
            counters: Arc::default(),
        })
    }

    /// Looks objects of this repository's own packs up in `midx` first, a
    /// multi-pack index (compaction's `midx/<seq:020>.midx`, §36 §4.1) over
    /// some or all of them. Packs it does not cover are still searched by
    /// their idx. Packs it names that the snapshot does not hold are
    /// ignored, and an object it places in one is searched for in every
    /// pack, as if there were no index.
    pub fn with_midx(mut self, midx: Arc<dyn RangeSource>) -> Self {
        let own = &self.layers[0];
        let layer = Layer {
            blobs: Arc::clone(&own.blobs),
            packs: own
                .packs
                .iter()
                .map(|state| PackState {
                    pack: state.pack.clone(),
                    id: state.id.clone(),
                    objects: state.objects,
                    fanout: Mutex::new(None),
                })
                .collect(),
            midx: Some(Midx {
                source: midx,
                tables: Mutex::new(None),
            }),
        };
        self.layers[0] = Arc::new(layer);
        self
    }

    /// A database of `packs` in `blobs` alone, with no snapshot or fork
    /// parent: how an uploaded workspace pack is verified before it is
    /// stored (Task 24). Its one layer is layer 0.
    pub(crate) fn of_packs(
        repo: RepoId,
        blobs: Arc<dyn BlobStore>,
        packs: &[pb::PackRef],
    ) -> Result<Self, OdbError> {
        Ok(Self {
            repo,
            seq: Seq::ZERO,
            depth: 0,
            layers: vec![Arc::new(Layer::new(blobs, packs)?)],
            bases: Arc::default(),
            counters: Arc::default(),
        })
    }

    /// This database with `layer` searched after every other: a
    /// workspace's packs (Task 17's `WsOdb`).
    pub(crate) fn with_top_layer(&self, layer: Layer) -> Self {
        let mut layers = self.layers.clone();
        layers.push(Arc::new(layer));
        Self {
            repo: self.repo.clone(),
            seq: self.seq,
            depth: self.depth,
            layers,
            bases: Arc::default(),
            counters: Arc::default(),
        }
    }

    /// The repository.
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }

    /// The snapshot's seq.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// How many fork parents lie below this database.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// How many layers the lookup chain has (the repository, its fork
    /// parents, and a workspace's packs).
    pub fn layers(&self) -> usize {
        self.layers.len()
    }

    /// The counters.
    pub fn stats(&self) -> OdbStats {
        OdbStats {
            inflates: self.counters.inflates.load(Ordering::Relaxed),
            prefix_inflates: self.counters.prefix_inflates.load(Ordering::Relaxed),
            base_hits: self.counters.base_hits.load(Ordering::Relaxed),
            midx_hits: self.counters.midx_hits.load(Ordering::Relaxed),
        }
    }

    /// Drops every cache this database keeps in memory (fan-out tables,
    /// multi-pack index tables, resolved bases). Reads stay correct; they
    /// read again what they need.
    pub fn drop_caches(&self) {
        for layer in &self.layers {
            layer.drop_caches();
        }
        *lock(&self.bases) = BaseCache::default();
    }

    /// The `PackRef` of the pack `loc` names.
    pub fn pack_ref(&self, loc: &Location) -> Option<&pb::PackRef> {
        self.layers
            .get(loc.layer)
            .and_then(|layer| layer.packs.get(loc.pack))
            .map(|state| &state.pack)
    }

    fn state(&self, loc: &Location) -> Result<(&Layer, &PackState), OdbError> {
        self.layers
            .get(loc.layer)
            .and_then(|layer| Some((&**layer, layer.packs.get(loc.pack)?)))
            .ok_or_else(|| OdbError::CorruptPack {
                pack: format!("layer {} pack {}", loc.layer, loc.pack),
                reason: "a location this database did not give".into(),
            })
    }

    /// Where `oid`'s entry is: the first layer, and in it the first pack
    /// (or the multi-pack index), that holds it.
    pub async fn locate(&self, oid: &ObjectId) -> Result<Option<Location>, OdbError> {
        for (index, layer) in self.layers.iter().enumerate() {
            if let Some((pack, offset, by_midx)) = layer.locate(oid).await? {
                if by_midx {
                    self.counters.midx_hits.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(Some(Location {
                    layer: index,
                    pack,
                    offset,
                }));
            }
        }
        Ok(None)
    }

    /// Reads the entry at `loc`: its header and at least 64 KiB from its
    /// start (or the rest of the pack).
    async fn raw(&self, loc: &Location) -> Result<Raw, OdbError> {
        let (layer, state) = self.state(loc)?;
        let end = state.pack.pack_len.saturating_sub(PACK_TRAILER);
        if loc.offset < 12 || loc.offset >= end {
            return Err(corrupt_pack(
                &state.pack,
                &format!("entry offset {} outside the pack", loc.offset),
            ));
        }
        let chunk = layer
            .pack_range(state, loc.offset..end.min(loc.offset + MIN_COALESCED_RANGE))
            .await?;
        let entry = gix_pack::data::Entry::from_bytes(&chunk, loc.offset, gix_hash::Kind::Sha1)
            .map_err(|err| corrupt_pack(&state.pack, &err))?;
        Ok(Raw { entry, chunk })
    }

    /// Inflates the entry `raw` (at `loc`), reading more of the pack while
    /// the zlib stream runs on. Returns the output and the compressed bytes
    /// consumed.
    async fn inflate(
        &self,
        loc: &Location,
        raw: &Raw,
        want: Want,
    ) -> Result<(Vec<u8>, u64), OdbError> {
        let (layer, state) = self.state(loc)?;
        let corrupt = |reason: String| corrupt_pack(&state.pack, &reason);
        let size = usize::try_from(raw.entry.decompressed_size)
            .map_err(|_| corrupt("entry too large for this platform".into()))?;
        let (limit, counter) = match want {
            Want::All => (size, &self.counters.inflates),
            Want::Prefix(n) => (size.min(n), &self.counters.prefix_inflates),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let pack_end = state.pack.pack_len.saturating_sub(PACK_TRAILER);
        // zlib expands at most about 1032 to 1, so a size the rest of the
        // pack cannot hold is corrupt; refusing it bounds the allocation.
        let room = pack_end.saturating_sub(raw.entry.data_offset);
        if raw.entry.decompressed_size > room.saturating_mul(MAX_INFLATE_RATIO) + 64 {
            return Err(corrupt(format!(
                "entry at {} claims {size} bytes, more than the rest of the pack inflates to",
                loc.offset
            )));
        }
        let data_start = raw.entry.data_offset - loc.offset;
        let mut input = raw.chunk.slice(data_start as usize..);
        let mut next = loc.offset + raw.chunk.len() as u64;
        let mut read_len = MIN_COALESCED_RANGE;
        // One spare byte, so a stream longer than its header says is caught.
        let mut out = vec![0u8; limit + 1];
        let mut written = 0usize;
        let mut zlib = Decompress::new();
        let mut consumed = 0u64;
        loop {
            let before_in = zlib.total_in();
            let before_out = zlib.total_out();
            let status = zlib
                .decompress(&input, &mut out[written..], FlushDecompress::None)
                .map_err(|err| corrupt(format!("inflating the entry at {}: {err}", loc.offset)))?;
            let took = (zlib.total_in() - before_in) as usize;
            let made = (zlib.total_out() - before_out) as usize;
            consumed += took as u64;
            written += made;
            input = input.slice(took..);
            match status {
                Status::StreamEnd => {
                    if written != size {
                        return Err(corrupt(format!(
                            "entry at {} inflates to {written} bytes, its header says {size}",
                            loc.offset
                        )));
                    }
                    out.truncate(written);
                    return Ok((out, consumed));
                }
                _ if written > size => {
                    return Err(corrupt(format!(
                        "entry at {} inflates past its {size} bytes",
                        loc.offset
                    )));
                }
                _ if matches!(want, Want::Prefix(_)) && written >= limit => {
                    out.truncate(written);
                    return Ok((out, consumed));
                }
                _ => {}
            }
            if input.is_empty() || (took == 0 && made == 0) {
                if next >= pack_end {
                    return Err(corrupt(format!("entry at {} is truncated", loc.offset)));
                }
                let mut more = layer
                    .pack_range(state, next..pack_end.min(next + read_len))
                    .await?;
                next += more.len() as u64;
                read_len = (read_len * 2).min(MAX_ENTRY_READ);
                if !input.is_empty() {
                    let mut joined = Vec::with_capacity(input.len() + more.len());
                    joined.extend_from_slice(&input);
                    joined.extend_from_slice(&more);
                    more = Bytes::from(joined);
                }
                input = more;
            }
        }
    }

    /// The entry's header (plan GT1 Task 17): kind and size without
    /// inflating the object. For a delta, the result size is read from the
    /// first bytes of its delta data and the kind from its chain's base.
    pub async fn header(&self, loc: &Location) -> Result<EntryHeader, OdbError> {
        let raw = self.raw(loc).await?;
        let base = delta_base(loc, &raw)?;
        let Some(base_ref) = base else {
            return Ok(EntryHeader {
                kind: base_kind(&raw.entry.header).ok_or_else(|| {
                    corrupt_pack_at(self, loc, "an entry that is neither object nor delta")
                })?,
                size: raw.entry.decompressed_size,
                base: None,
            });
        };
        let (prefix, _) = self
            .inflate(loc, &raw, Want::Prefix(DELTA_SIZES_PREFIX))
            .await?;
        let (_, rest) = delta_varint(&prefix)
            .ok_or_else(|| corrupt_pack_at(self, loc, "delta base size truncated"))?;
        let (size, _) = delta_varint(&prefix[rest..])
            .ok_or_else(|| corrupt_pack_at(self, loc, "delta result size truncated"))?;
        // The kind is the chain's base's.
        let mut cur = self.base_location(loc, base_ref).await?;
        for _ in 0..MAX_DELTA_CHAIN {
            let raw = self.raw(&cur).await?;
            match delta_base(&cur, &raw)? {
                Some(next) => cur = self.base_location(&cur, next).await?,
                None => {
                    let kind = base_kind(&raw.entry.header).ok_or_else(|| {
                        corrupt_pack_at(self, &cur, "an entry that is neither object nor delta")
                    })?;
                    return Ok(EntryHeader {
                        kind,
                        size,
                        base: Some(base_ref),
                    });
                }
            }
        }
        Err(corrupt_pack_at(self, loc, "delta chain too long"))
    }

    /// The entry's stored bytes (header and compressed data), for copying
    /// it into another pack.
    pub async fn raw_entry(&self, loc: &Location) -> Result<Bytes, OdbError> {
        let raw = self.raw(loc).await?;
        let (_, consumed) = self.inflate(loc, &raw, Want::All).await?;
        let header = raw.entry.data_offset - loc.offset;
        let len = header + consumed;
        if len <= raw.chunk.len() as u64 {
            return Ok(raw.chunk.slice(..len as usize));
        }
        let (layer, state) = self.state(loc)?;
        layer.pack_range(state, loc.offset..loc.offset + len).await
    }

    /// The location of a delta's base.
    async fn base_location(&self, loc: &Location, base: DeltaBase) -> Result<Location, OdbError> {
        match base {
            DeltaBase::Offset(offset) => Ok(Location { offset, ..*loc }),
            DeltaBase::Ref(oid) => {
                // A stored pack is self-contained, so look in it first.
                let (layer, state) = self.state(loc)?;
                if let Some(offset) = layer.idx_lookup(state, &oid).await? {
                    return Ok(Location { offset, ..*loc });
                }
                self.locate(&oid).await?.ok_or_else(|| {
                    corrupt_pack_at(self, loc, &format!("delta base {oid} not found"))
                })
            }
        }
    }

    /// The object at `loc`, deltas resolved.
    pub async fn read_at(&self, loc: &Location) -> Result<(Kind, Bytes), OdbError> {
        let mut chain: Vec<(Location, Raw)> = Vec::new();
        let mut cur = *loc;
        let (kind, mut data) = loop {
            if let Some(hit) = lock(&self.bases).get(&cur) {
                self.counters.base_hits.fetch_add(1, Ordering::Relaxed);
                break hit;
            }
            let raw = self.raw(&cur).await?;
            match delta_base(&cur, &raw)? {
                None => {
                    let kind = base_kind(&raw.entry.header).ok_or_else(|| {
                        corrupt_pack_at(self, &cur, "an entry that is neither object nor delta")
                    })?;
                    let (out, _) = self.inflate(&cur, &raw, Want::All).await?;
                    let out = Bytes::from(out);
                    if !chain.is_empty() {
                        lock(&self.bases).insert(cur, kind, out.clone());
                    }
                    break (kind, out);
                }
                Some(base) => {
                    let next = self.base_location(&cur, base).await?;
                    chain.push((cur, raw));
                    if chain.len() > MAX_DELTA_CHAIN {
                        return Err(corrupt_pack_at(self, loc, "delta chain too long"));
                    }
                    cur = next;
                }
            }
        };
        while let Some((at, raw)) = chain.pop() {
            let (delta, _) = self.inflate(&at, &raw, Want::All).await?;
            data = Bytes::from(
                apply_delta(&data, &delta).map_err(|reason| corrupt_pack_at(self, &at, &reason))?,
            );
            if !chain.is_empty() {
                lock(&self.bases).insert(at, kind, data.clone());
            }
        }
        Ok((kind, data))
    }
}

fn corrupt_pack_at(odb: &RangeOdb, loc: &Location, reason: &str) -> OdbError {
    OdbError::CorruptPack {
        pack: odb.pack_ref(loc).map_or_else(
            || format!("layer {} pack {}", loc.layer, loc.pack),
            |p| p.checksum.clone(),
        ),
        reason: format!("entry at {}: {reason}", loc.offset),
    }
}

/// The object kind of a non-delta entry.
fn base_kind(header: &Header) -> Option<Kind> {
    header.as_kind()
}

/// The entry's delta base, when it is a delta.
fn delta_base(loc: &Location, raw: &Raw) -> Result<Option<DeltaBase>, OdbError> {
    match raw.entry.header {
        Header::OfsDelta { base_distance } => {
            let base =
                Header::verified_base_pack_offset(loc.offset, base_distance).ok_or_else(|| {
                    OdbError::CorruptPack {
                        pack: format!("layer {} pack {}", loc.layer, loc.pack),
                        reason: format!("entry at {}: delta base before the pack", loc.offset),
                    }
                })?;
            Ok(Some(DeltaBase::Offset(base)))
        }
        Header::RefDelta { base_id } => Ok(Some(DeltaBase::Ref(base_id))),
        _ => Ok(None),
    }
}

/// A delta's size varint (seven bits per byte, least significant first)
/// and the bytes it took.
fn delta_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Applies git's delta encoding (copy and insert instructions after the
/// base and result sizes) to `base`.
fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, String> {
    let (base_size, used) = delta_varint(delta).ok_or("delta base size truncated")?;
    if base_size != base.len() as u64 {
        return Err(format!(
            "delta expects a base of {base_size} bytes, the base has {}",
            base.len()
        ));
    }
    let mut at = used;
    let (result_size, used) = delta_varint(&delta[at..]).ok_or("delta result size truncated")?;
    at += used;
    let result_size = usize::try_from(result_size).map_err(|_| "delta result too large")?;
    let mut out = Vec::with_capacity(result_size.min(delta.len().saturating_mul(64) + base.len()));
    while at < delta.len() {
        let cmd = delta[at];
        at += 1;
        if cmd & 0x80 != 0 {
            let mut take = |bit: u8, shift: u32, value: &mut usize| -> Result<(), String> {
                if cmd & bit != 0 {
                    let byte = *delta.get(at).ok_or("delta copy truncated")?;
                    at += 1;
                    *value |= usize::from(byte) << shift;
                }
                Ok(())
            };
            let mut offset = 0usize;
            let mut size = 0usize;
            take(0x01, 0, &mut offset)?;
            take(0x02, 8, &mut offset)?;
            take(0x04, 16, &mut offset)?;
            take(0x08, 24, &mut offset)?;
            take(0x10, 0, &mut size)?;
            take(0x20, 8, &mut size)?;
            take(0x40, 16, &mut size)?;
            if size == 0 {
                size = 0x10000;
            }
            let end = offset
                .checked_add(size)
                .filter(|end| *end <= base.len())
                .ok_or("delta copies past its base")?;
            out.extend_from_slice(&base[offset..end]);
        } else if cmd != 0 {
            let end = at + usize::from(cmd);
            let insert = delta.get(at..end).ok_or("delta insert truncated")?;
            out.extend_from_slice(insert);
            at = end;
        } else {
            return Err("delta has a reserved instruction".into());
        }
        if out.len() > result_size {
            return Err("delta result runs past its size".into());
        }
    }
    if out.len() != result_size {
        return Err(format!(
            "delta result has {} bytes, the delta says {result_size}",
            out.len()
        ));
    }
    Ok(out)
}

#[async_trait]
impl ObjectDb for RangeOdb {
    async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        Ok(self.locate(oid).await?.is_some())
    }

    async fn read(&self, oid: &ObjectId) -> Result<(Kind, Bytes), OdbError> {
        let loc = self.locate(oid).await?.ok_or(OdbError::NotFound(*oid))?;
        self.read_at(&loc).await
    }

    async fn missing_from_closure(
        &self,
        tips: &[ObjectId],
        known: &[ObjectId],
        extra: Option<&dyn ObjectDb>,
    ) -> Result<Vec<ObjectId>, OdbError> {
        let primary = DbLookup(self);
        let extra_db = extra.map(DbLookup);
        let extra: Option<&dyn Lookup> = extra_db.as_ref().map(|db| db as &dyn Lookup);
        walk_closure(&primary, extra, tips, known).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_insert_and_copy() {
        let base = b"hello, world";
        // base 12, result 10: copy 0..5, insert "!!", copy 7..10.
        let delta = [12, 10, 0x90, 5, 2, b'!', b'!', 0x91, 7, 3];
        assert_eq!(apply_delta(base, &delta).expect("apply"), b"hello!!wor");
    }

    #[test]
    fn delta_errors() {
        assert!(
            apply_delta(b"ab", &[3, 1, 1, b'x']).is_err(),
            "wrong base size"
        );
        assert!(
            apply_delta(b"ab", &[2, 1, 0x91, 1, 5]).is_err(),
            "copy past base"
        );
        assert!(apply_delta(b"ab", &[2, 1, 0]).is_err(), "reserved");
        assert!(
            apply_delta(b"ab", &[2, 2, 1, b'x']).is_err(),
            "short result"
        );
    }

    #[test]
    fn varint() {
        assert_eq!(delta_varint(&[0x7f]), Some((127, 1)));
        assert_eq!(delta_varint(&[0x80, 0x01]), Some((128, 2)));
        assert_eq!(delta_varint(&[0x80]), None);
    }
}
