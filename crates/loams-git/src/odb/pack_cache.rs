//! The pack-cache object database (plan GT1 Task 9, old GT1 Task 5).
//!
//! - [`PackCache`] keeps whole pack and idx sections as files
//!   `<checksum>.pack` and `<checksum>.idx` in an [`Fs`], within a byte
//!   budget, evicting the least recently used. Files are named by content
//!   and checked against their `PackRef` when read, so the cache can be
//!   deleted at any time: a missing or wrong file is downloaded again.
//! - [`PackCacheOdb`] looks objects up in a snapshot's packs, newest
//!   first, then in its fork parent's at the parent's seq (§36 §4.7), at
//!   most `MAX_FORK_DEPTH` (8) parents deep. An idx is downloaded to find
//!   an object; a pack's data only when it holds one. `gix-pack` decodes
//!   entries and resolves deltas in memory.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;
use gix_object::Kind;
use gix_pack::data::decode::entry::ResolvedBase;
use loams_fs::{Fs, FsPath, WriteMode};
use tokio::sync::OnceCell;

use super::{DbLookup, Lookup, ObjectDb, OdbError, walk_closure};
use crate::blob::{BlobId, BlobStore, read_idx, read_pack};
use crate::format::pb;
use crate::ids::{ObjectId, RepoId, Seq};
use crate::limits::MAX_FORK_DEPTH;
use crate::reflog::RefSnapshot;

/// File names per listing page when the cache scans its `Fs`.
const SCAN_PAGE: usize = 1000;
/// Bytes of a SHA-1 pack checksum.
const CHECKSUM_LEN: usize = 20;

/// Which section of a pack object a cache file holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Section {
    Pack,
    Idx,
}

impl Section {
    fn ext(self) -> &'static str {
        match self {
            Section::Pack => "pack",
            Section::Idx => "idx",
        }
    }

    /// The section's length, as the `PackRef` states it.
    fn len(self, pack: &pb::PackRef) -> u64 {
        match self {
            Section::Pack => pack.pack_len,
            Section::Idx => pack.idx_len,
        }
    }
}

/// One cached file: its length and when it was last used.
#[derive(Clone, Copy, Debug)]
struct Entry {
    len: u64,
    tick: u64,
}

/// The cache's accounting of its files.
#[derive(Debug, Default)]
struct Lru {
    entries: HashMap<String, Entry>,
    used: u64,
    tick: u64,
}

impl Lru {
    /// Marks `name` used; `false` when it is not cached.
    fn touch(&mut self, name: &str) -> bool {
        self.tick += 1;
        let tick = self.tick;
        match self.entries.get_mut(name) {
            Some(entry) => {
                entry.tick = tick;
                true
            }
            None => false,
        }
    }

    fn insert(&mut self, name: String, len: u64) {
        self.forget(&name);
        self.tick += 1;
        self.used += len;
        self.entries.insert(
            name,
            Entry {
                len,
                tick: self.tick,
            },
        );
    }

    fn forget(&mut self, name: &str) {
        if let Some(old) = self.entries.remove(name) {
            self.used -= old.len;
        }
    }

    /// Drops least recently used entries until `len` more bytes fit in
    /// `max`; returns their names, whose files the caller removes.
    fn make_room(&mut self, len: u64, max: u64) -> Vec<String> {
        let mut evicted = Vec::new();
        while self.used + len > max {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.tick)
                .map(|(name, _)| name.clone())
            else {
                break;
            };
            self.forget(&oldest);
            evicted.push(oldest);
        }
        evicted
    }
}

/// Whole pack sections cached in an [`Fs`] (plan GT1 Task 9): content-keyed,
/// least recently used first out, and safe to delete at any time.
pub struct PackCache {
    fs: Arc<dyn Fs>,
    max_bytes: u64,
    lru: Mutex<Lru>,
    /// Files left by an earlier process are counted once, on first use.
    scanned: OnceCell<()>,
}

impl fmt::Debug for PackCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackCache")
            .field("fs", &self.fs)
            .field("max_bytes", &self.max_bytes)
            .field("cached_bytes", &self.cached_bytes())
            .finish()
    }
}

impl PackCache {
    /// A cache of at most `max_bytes` of files at the root of `fs`, which it
    /// owns: files it did not name are left alone and not counted.
    pub fn new(fs: Arc<dyn Fs>, max_bytes: u64) -> Self {
        Self {
            fs,
            max_bytes,
            lru: Mutex::new(Lru::default()),
            scanned: OnceCell::new(),
        }
    }

    /// The budget.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// The bytes of the files the cache holds now.
    pub fn cached_bytes(&self) -> u64 {
        self.lru().used
    }

    fn lru(&self) -> MutexGuard<'_, Lru> {
        // The accounting stays consistent across a panic elsewhere: every
        // change is a whole insert or remove.
        self.lru
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Counts the cache's files already in the `Fs`, oldest first, then
    /// trims them to the budget.
    async fn scan(&self) {
        let mut found: Vec<(i64, String, u64)> = Vec::new();
        let mut after: Option<FsPath> = None;
        loop {
            let page = match self
                .fs
                .list(&FsPath::root(), after.as_ref(), SCAN_PAGE)
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    tracing::warn!(error = %err, "listing the pack cache failed; starting empty");
                    return;
                }
            };
            let Some((last, _)) = page.last() else {
                break;
            };
            after = Some(last.clone());
            for (path, meta) in page {
                if is_cache_name(path.as_str()) {
                    found.push((meta.modified_unix_ms, path.as_str().to_string(), meta.len));
                }
            }
        }
        found.sort();
        let evicted = {
            let mut lru = self.lru();
            for (_, name, len) in found {
                lru.insert(name, len);
            }
            lru.make_room(0, self.max_bytes)
        };
        self.remove_files(evicted).await;
    }

    async fn remove_files(&self, names: Vec<String>) {
        for name in names {
            let Ok(path) = FsPath::new(name.as_str()) else {
                continue;
            };
            if let Err(err) = self.fs.remove(&path).await {
                tracing::warn!(file = %name, error = %err, "evicting a pack cache file failed");
            }
        }
    }

    /// `section` of `pack`, from the cache or else downloaded from `blobs`
    /// (and cached when it fits the budget).
    async fn section(
        &self,
        blobs: &dyn BlobStore,
        pack: &pb::PackRef,
        section: Section,
    ) -> Result<Bytes, OdbError> {
        self.scanned.get_or_init(|| self.scan()).await;
        let id = BlobId::parse(&pack.checksum).map_err(|err| OdbError::CorruptPack {
            pack: pack.checksum.clone(),
            reason: err.to_string(),
        })?;
        let name = format!("{id}.{}", section.ext());
        let path = FsPath::new(name.as_str()).map_err(|err| OdbError::CorruptPack {
            pack: pack.checksum.clone(),
            reason: err.to_string(),
        })?;
        if self.lru().touch(&name) {
            match self.fs.read(&path).await {
                Ok(bytes) if check_section(pack, section, &bytes).is_ok() => return Ok(bytes),
                Ok(_) => {
                    tracing::warn!(file = %name, "dropping a pack cache file that does not match its pack");
                    self.lru().forget(&name);
                    self.remove_files(vec![name.clone()]).await;
                }
                // Deleted under the cache, or unreadable: download again.
                Err(_) => self.lru().forget(&name),
            }
        }
        let bytes = match section {
            Section::Pack => read_pack(blobs, pack).await?,
            Section::Idx => read_idx(blobs, pack).await?,
        };
        check_section(pack, section, &bytes).map_err(|reason| OdbError::CorruptPack {
            pack: pack.checksum.clone(),
            reason,
        })?;
        let len = bytes.len() as u64;
        if len <= self.max_bytes {
            let evicted = {
                let mut lru = self.lru();
                lru.forget(&name);
                let evicted = lru.make_room(len, self.max_bytes);
                lru.insert(name.clone(), len);
                evicted
            };
            self.remove_files(evicted).await;
            if let Err(err) = self
                .fs
                .write(&path, bytes.clone(), WriteMode::Overwrite)
                .await
            {
                tracing::warn!(file = %name, error = %err, "caching a pack section failed");
                self.lru().forget(&name);
            }
        }
        Ok(bytes)
    }
}

/// Whether `name` is `<40 lowercase hex>.pack` or `.idx`.
fn is_cache_name(name: &str) -> bool {
    let Some((stem, ext)) = name.rsplit_once('.') else {
        return false;
    };
    (ext == "pack" || ext == "idx") && BlobId::parse(stem).is_ok()
}

/// Checks that `bytes` is `section` of `pack`: its length, and the pack
/// checksum in its trailer (the pack's last 20 bytes; the idx's 20 bytes
/// before its own checksum).
fn check_section(pack: &pb::PackRef, section: Section, bytes: &[u8]) -> Result<(), String> {
    let expected = section.len(pack);
    if bytes.len() as u64 != expected {
        return Err(format!(
            "{} section of {} bytes, the PackRef says {expected}",
            section.ext(),
            bytes.len()
        ));
    }
    let end = match section {
        Section::Pack => bytes.len(),
        Section::Idx => bytes.len().saturating_sub(CHECKSUM_LEN),
    };
    let trailer = end
        .checked_sub(CHECKSUM_LEN)
        .map(|start| &bytes[start..end])
        .ok_or_else(|| format!("{} section too short", section.ext()))?;
    let named = ObjectId::from_hex(pack.checksum.as_bytes())
        .map_err(|err| format!("checksum {:?}: {err}", pack.checksum))?;
    if trailer != named.as_bytes() {
        return Err(format!("{} section names another pack", section.ext()));
    }
    Ok(())
}

type IndexFile = gix_pack::index::File<Bytes>;
type DataFile = gix_pack::data::File<Bytes>;

/// Pack sections parsed during one call, so a walk reads each once.
#[derive(Default)]
struct Memo {
    idx: Mutex<HashMap<String, Arc<IndexFile>>>,
    data: Mutex<HashMap<String, Arc<DataFile>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// An [`ObjectDb`] over a snapshot's pack set, through a [`PackCache`], with
/// fork fall-through (plan GT1 Task 9).
#[derive(Debug)]
pub struct PackCacheOdb {
    repo: RepoId,
    blobs: Arc<dyn BlobStore>,
    cache: Arc<PackCache>,
    /// Newest first.
    packs: Vec<pb::PackRef>,
    seq: Seq,
    parent: Option<Arc<PackCacheOdb>>,
    /// Fork parents below this one.
    depth: usize,
}

impl PackCacheOdb {
    /// The objects of repository `repo`'s `snapshot`, whose packs are in
    /// `blobs`, falling through to `parent`, which must be the snapshot's
    /// fork parent opened at the parent's seq (the caller opens it with the
    /// parent's blob store). Refuses a parent the snapshot does not name, a
    /// missing one, another repository, one at another seq
    /// (`ParentMismatch`), and a chain deeper than `MAX_FORK_DEPTH`
    /// (`ForkTooDeep`).
    pub async fn open(
        repo: RepoId,
        blobs: Arc<dyn BlobStore>,
        cache: Arc<PackCache>,
        snapshot: &RefSnapshot,
        parent: Option<Arc<PackCacheOdb>>,
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
        Ok(Self {
            repo,
            blobs,
            cache,
            packs: snapshot.packs.iter().rev().cloned().collect(),
            seq: snapshot.seq,
            parent,
            depth,
        })
    }

    /// The repository.
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }

    /// The snapshot's seq.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// How many fork parents lie below this database (0 for a repository
    /// that is not a fork).
    pub fn depth(&self) -> usize {
        self.depth
    }

    async fn index(&self, pack: &pb::PackRef, memo: &Memo) -> Result<Arc<IndexFile>, OdbError> {
        if let Some(index) = lock(&memo.idx).get(&pack.checksum) {
            return Ok(Arc::clone(index));
        }
        let bytes = self.cache.section(&*self.blobs, pack, Section::Idx).await?;
        let index =
            IndexFile::from_data(bytes, file_name(pack, Section::Idx), gix_hash::Kind::Sha1)
                .map_err(|err| corrupt_pack(pack, &err))?;
        let index = Arc::new(index);
        lock(&memo.idx).insert(pack.checksum.clone(), Arc::clone(&index));
        Ok(index)
    }

    async fn data(&self, pack: &pb::PackRef, memo: &Memo) -> Result<Arc<DataFile>, OdbError> {
        if let Some(data) = lock(&memo.data).get(&pack.checksum) {
            return Ok(Arc::clone(data));
        }
        let bytes = self
            .cache
            .section(&*self.blobs, pack, Section::Pack)
            .await?;
        let data = DataFile::from_data(bytes, file_name(pack, Section::Pack), gix_hash::Kind::Sha1)
            .map_err(|err| corrupt_pack(pack, &err))?;
        let data = Arc::new(data);
        lock(&memo.data).insert(pack.checksum.clone(), Arc::clone(&data));
        Ok(data)
    }

    /// The pack (in this database or a parent) and idx entry of `oid`.
    async fn locate(
        &self,
        oid: &ObjectId,
        memo: &Memo,
    ) -> Result<Option<(&PackCacheOdb, pb::PackRef, Arc<IndexFile>, u32)>, OdbError> {
        let mut odb = self;
        loop {
            for pack in &odb.packs {
                let index = odb.index(pack, memo).await?;
                if let Some(entry) = index.lookup(oid) {
                    return Ok(Some((odb, pack.clone(), index, entry)));
                }
            }
            match &odb.parent {
                Some(parent) => odb = parent,
                None => return Ok(None),
            }
        }
    }

    async fn find(&self, oid: &ObjectId, memo: &Memo) -> Result<Option<(Kind, Bytes)>, OdbError> {
        let Some((odb, pack, index, entry)) = self.locate(oid, memo).await? else {
            return Ok(None);
        };
        let data = odb.data(&pack, memo).await?;
        decode(&pack, &index, &data, entry).map(Some)
    }
}

/// The path gix-pack is told a section was read from; it only names the
/// file in messages and seeds the pack's id.
fn file_name(pack: &pb::PackRef, section: Section) -> PathBuf {
    PathBuf::from(format!("{}.{}", pack.checksum, section.ext()))
}

fn corrupt_pack(pack: &pb::PackRef, err: &dyn fmt::Display) -> OdbError {
    OdbError::CorruptPack {
        pack: pack.checksum.clone(),
        reason: err.to_string(),
    }
}

/// Decodes idx entry `entry` from `data`, resolving deltas inside the pack.
fn decode(
    pack: &pb::PackRef,
    index: &IndexFile,
    data: &DataFile,
    entry: u32,
) -> Result<(Kind, Bytes), OdbError> {
    let found = data
        .entry(index.pack_offset_at_index(entry))
        .map_err(|err| corrupt_pack(pack, &err))?;
    let mut out = Vec::new();
    let mut inflate = gix_zlib::Inflate::default();
    // Stored packs are self-contained (thin packs are completed before
    // they are stored), so a ref-delta's base is in the same pack.
    let resolve = |base: &gix_hash::oid, _: &mut Vec<u8>| {
        let at = index.lookup(base)?;
        data.entry(index.pack_offset_at_index(at))
            .ok()
            .map(ResolvedBase::InPack)
    };
    let outcome = data
        .decode_entry(
            found,
            &mut out,
            &mut inflate,
            &resolve,
            &mut gix_pack::cache::Never,
        )
        .map_err(|err| corrupt_pack(pack, &err))?;
    Ok((outcome.kind, Bytes::from(out)))
}

/// One call's view of a [`PackCacheOdb`], sharing parsed sections.
struct Session<'a> {
    odb: &'a PackCacheOdb,
    memo: Memo,
}

#[async_trait]
impl Lookup for Session<'_> {
    async fn has(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        Ok(self.odb.locate(oid, &self.memo).await?.is_some())
    }

    async fn get(&self, oid: &ObjectId) -> Result<Option<(Kind, Bytes)>, OdbError> {
        self.odb.find(oid, &self.memo).await
    }
}

#[async_trait]
impl ObjectDb for PackCacheOdb {
    async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        Ok(self.locate(oid, &Memo::default()).await?.is_some())
    }

    async fn read(&self, oid: &ObjectId) -> Result<(Kind, Bytes), OdbError> {
        self.find(oid, &Memo::default())
            .await?
            .ok_or(OdbError::NotFound(*oid))
    }

    async fn missing_from_closure(
        &self,
        tips: &[ObjectId],
        known: &[ObjectId],
        extra: Option<&dyn ObjectDb>,
    ) -> Result<Vec<ObjectId>, OdbError> {
        let session = Session {
            odb: self,
            memo: Memo::default(),
        };
        let extra = extra.map(DbLookup);
        walk_closure(
            &session,
            extra.as_ref().map(|e| e as &dyn Lookup),
            tips,
            known,
        )
        .await
    }
}
