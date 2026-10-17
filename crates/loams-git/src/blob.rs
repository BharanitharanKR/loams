//! `BlobStore` (§36 §5.3, D390; plan GT1 Task 6, Ruling 1): create-only,
//! content-named blobs on the bucket, and the one-object pack bundles
//! (`packs/<checksum>.lpk` = pack ‖ idx ‖ footer) stored in them.
//!
//! - A blob of at most [`MULTIPART_THRESHOLD`] (64 MiB) is written with a
//!   create-only PUT. If the name exists, the stored length decides:
//!   equal is [`Put::Existed`], different is [`BlobError::Mismatch`] (two
//!   contents under one content-derived name is corruption).
//! - A larger blob is written with an unconditional multipart upload: two
//!   writers of one content name write identical bytes, so either outcome
//!   is correct. A store marked as not completing multipart uploads
//!   atomically ([`StoreBlobStore::with_atomic_multipart`]) refuses it with
//!   [`BlobError::TooLarge`], because a reader could see a partial object.
//!   RustFS 1.0.0 was checked and is atomic (Q385, ruling T6.2).
//! - Transient store errors are retried three times (50, 200 and 800 ms,
//!   jittered). A retried create-only PUT whose first attempt was applied
//!   finds the name taken and answers `Existed`.

use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use loams_cache::{CacheError, RangeCache};
use loams_fs::{Fs, FsError, FsPath, MemFs, SpillDir};
use loams_store::{Store, StoreError};

use crate::error::IdError;
use crate::format::lpk::{LPK_FOOTER_LEN, encode_footer, pack_checksum};
use crate::format::pb;
use crate::ids::{ObjectId, WsId};
use crate::limits::MULTIPART_THRESHOLD;
use crate::paths::RepoPaths;
use crate::retry::{self, BACKOFF, is_transient, pause};

/// A content-derived blob name (§36 §5.3): a pack checksum, which names
/// `packs/<hex>.lpk`, in lowercase hex (40 digits for SHA-1, 64 for
/// SHA-256).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(String);

impl BlobId {
    /// The blob named by a pack checksum.
    pub fn from_checksum(checksum: &ObjectId) -> Self {
        Self(checksum.to_hex().to_string())
    }

    /// Parses 40 or 64 lowercase hex digits.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let hex = s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if hex && (s.len() == 40 || s.len() == 64) {
            Ok(Self(s.to_string()))
        } else {
            Err(IdError::new(
                "blob id",
                s,
                "expected 40 or 64 lowercase hex digits",
            ))
        }
    }

    /// The name's string form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a create-only put found (§36 §5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Put {
    /// This call wrote the blob.
    Created,
    /// The blob was already there, with the offered length.
    Existed,
}

/// Errors of a [`BlobStore`].
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// Another content is stored under this content-derived name.
    #[error("blob {id} is stored with {stored} bytes, not the {offered} offered")]
    Mismatch {
        /// The blob.
        id: BlobId,
        /// The stored length.
        stored: u64,
        /// The offered length.
        offered: u64,
    },
    /// A read range that does not lie inside the blob.
    #[error("range {}..{} is outside blob {id} ({len} bytes)", range.start, range.end)]
    OutOfRange {
        /// The blob.
        id: BlobId,
        /// The requested range.
        range: Range<u64>,
        /// The blob's length.
        len: u64,
    },
    /// No blob has this name.
    #[error("blob {id} not found")]
    NotFound {
        /// The blob.
        id: BlobId,
    },
    /// A blob over the multipart threshold on a store configured as not
    /// completing multipart uploads atomically (Ruling 1).
    #[error(
        "blob {id} has {len} bytes, more than the {max} this store accepts: it is configured as \
         not completing multipart uploads atomically"
    )]
    TooLarge {
        /// The blob.
        id: BlobId,
        /// Its length.
        len: u64,
        /// The largest accepted length.
        max: u64,
    },
    /// A streamed body was not as long as declared.
    #[error("body of blob {id} has {got} bytes, not the {declared} declared")]
    LengthMismatch {
        /// The blob.
        id: BlobId,
        /// The declared length.
        declared: u64,
        /// The bytes received, up to the first one past `declared`.
        got: u64,
    },
    /// A pack and an idx that do not form a pack bundle.
    #[error("invalid pack bundle: {0}")]
    InvalidPack(String),
    /// The H1 range cache failed in a way that is not a store error
    /// (plan GT1 Task 17).
    #[error("range cache: {0}")]
    Cache(String),
    /// A streamed body failed.
    #[error("blob body failed: {0}")]
    Body(#[source] io::Error),
    /// The spill buffer failed.
    #[error("spill buffer: {0}")]
    Spill(#[source] FsError),
    /// The object store failed (after retries, when transient).
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Content-named, create-only blobs (§36 §5.3). There is no `delete`: only
/// GC deletes, through `Store`, after the grace period and a GC claim.
#[async_trait]
pub trait BlobStore: Send + Sync + fmt::Debug {
    /// Create-only. If the name exists, returns `Existed` after checking that the stored length matches
    /// (`Mismatch` otherwise: two contents under one content-derived name is corruption).
    async fn put(&self, id: &BlobId, bytes: Bytes) -> Result<Put, BlobError>;
    /// Same, as a multipart upload for bodies over 64 MiB; the parts are buffered in an `Fs` (§17).
    async fn put_stream(
        &self,
        id: &BlobId,
        body: BoxStream<'static, io::Result<Bytes>>,
        len: u64,
    ) -> Result<Put, BlobError>;
    /// `range` must lie inside the blob; reads go through the H1 cache (§04) where one exists.
    async fn get_range(&self, id: &BlobId, range: Range<u64>) -> Result<Bytes, BlobError>;
    /// The blob's length, or `None` if there is no blob of that name.
    async fn head(&self, id: &BlobId) -> Result<Option<u64>, BlobError>;
}

/// The part size of multipart uploads.
const PART_SIZE: usize = 8 * 1024 * 1024;

/// The spill file's name inside its [`SpillDir`].
const SPILL_BODY: &str = "body";

/// The largest body the default in-memory spill holds. A store that takes
/// larger streamed blobs names a disk with [`StoreBlobStore::with_spill`].
pub const DEFAULT_MEMORY_SPILL_BYTES: u64 = 256 * 1024 * 1024;

/// [`BlobStore`] over a `loams_store::Store`: blobs are a repository's pack
/// objects, `ns/<ns>/repos/<repo_id>/packs/<id>.lpk`, or a workspace's,
/// `…/ws/<ws_id>/packs/<id>.lpk` ([`StoreBlobStore::workspace`]).
#[derive(Clone, Debug)]
pub struct StoreBlobStore {
    store: Store,
    /// Where the blobs live: `…/packs/` or `…/ws/<ws_id>/packs/`.
    prefix: String,
    /// The H1 cache range reads go through, when there is one (Task 17).
    h1: Option<RangeCache>,
    spill: Arc<dyn Fs>,
    spill_parent: FsPath,
    atomic_multipart: bool,
}

impl StoreBlobStore {
    /// Blobs of the repository at `paths`. Large streamed bodies spill to
    /// memory, at most [`DEFAULT_MEMORY_SPILL_BYTES`] each, until
    /// [`StoreBlobStore::with_spill`] names a disk; multipart completion is
    /// assumed atomic until [`StoreBlobStore::with_atomic_multipart`] says
    /// otherwise.
    pub fn new(store: Store, paths: RepoPaths) -> Self {
        Self::at(store, paths.pack_prefix())
    }

    /// The packs of workspace `ws` of the repository at `paths`
    /// (`…/ws/<ws_id>/packs/<id>.lpk`, §36 §19.4.1), configured as
    /// [`StoreBlobStore::new`] is.
    pub fn workspace(store: Store, paths: &RepoPaths, ws: &WsId) -> Self {
        Self::at(store, format!("{}packs/", paths.ws_prefix(ws)))
    }

    fn at(store: Store, prefix: String) -> Self {
        Self {
            store,
            prefix,
            h1: None,
            spill: Arc::new(MemFs::new(DEFAULT_MEMORY_SPILL_BYTES)),
            spill_parent: FsPath::root(),
            atomic_multipart: true,
        }
    }

    /// Buffers [`BlobStore::put_stream`] bodies in spill directories under
    /// `parent` on `fs`, so a failed upload is retried without the caller's
    /// stream.
    pub fn with_spill(mut self, fs: Arc<dyn Fs>, parent: FsPath) -> Self {
        self.spill = fs;
        self.spill_parent = parent;
        self
    }

    /// Serves [`BlobStore::get_range`] through `cache`, the H1 range cache
    /// (§04; plan GT1 Task 17), which must read the same store this blob
    /// store writes. Blobs are immutable, so nothing is ever invalidated;
    /// puts and heads go to the store.
    pub fn with_range_cache(mut self, cache: RangeCache) -> Self {
        self.h1 = Some(cache);
        self
    }

    /// Declares whether the store completes a multipart upload atomically,
    /// so no reader sees part of the object. S3, R2, GCS and Azure document
    /// it, and RustFS 1.0.0 was checked (Q385, ruling T6.2). When `false`,
    /// blobs over 64 MiB are refused with [`BlobError::TooLarge`] (Ruling 1):
    /// for a store not known to be atomic.
    pub fn with_atomic_multipart(mut self, atomic: bool) -> Self {
        self.atomic_multipart = atomic;
        self
    }

    /// The object path of blob `id`.
    pub fn path(&self, id: &BlobId) -> String {
        format!("{}{id}.lpk", self.prefix)
    }

    /// `range` of blob `id` (at `path`) through the H1 cache, retrying
    /// transient store errors as the direct path does.
    async fn get_range_cached(
        &self,
        h1: &RangeCache,
        id: &BlobId,
        path: &str,
        range: Range<u64>,
    ) -> Result<Bytes, BlobError> {
        let mut attempt = 0;
        loop {
            match h1.read(path, range.clone()).await {
                Ok(bytes) => return Ok(bytes),
                Err(CacheError::OutOfRange { size, .. }) => {
                    return Err(BlobError::OutOfRange {
                        id: id.clone(),
                        range,
                        len: size,
                    });
                }
                Err(CacheError::Store(StoreError::NotFound { .. })) => {
                    return Err(BlobError::NotFound { id: id.clone() });
                }
                Err(CacheError::Store(err)) if is_transient(&err) && attempt < BACKOFF.len() => {
                    pause(attempt).await;
                    attempt += 1;
                }
                Err(CacheError::Store(err)) => return Err(err.into()),
                Err(err) => return Err(BlobError::Cache(err.to_string())),
            }
        }
    }

    /// Refuses a large blob where multipart completion may not be atomic.
    fn check_large(&self, id: &BlobId, len: u64) -> Result<(), BlobError> {
        if self.atomic_multipart {
            return Ok(());
        }
        Err(BlobError::TooLarge {
            id: id.clone(),
            len,
            max: MULTIPART_THRESHOLD,
        })
    }

    /// `Some(Existed)` if the blob is stored with length `len`, `None` if it
    /// is absent, `Mismatch` otherwise.
    async fn existing(&self, id: &BlobId, len: u64) -> Result<Option<Put>, BlobError> {
        match self.head(id).await? {
            None => Ok(None),
            Some(stored) if stored == len => Ok(Some(Put::Existed)),
            Some(stored) => Err(BlobError::Mismatch {
                id: id.clone(),
                stored,
                offered: len,
            }),
        }
    }

    /// The create-only PUT of a body of at most 64 MiB.
    async fn put_small(&self, id: &BlobId, bytes: Bytes) -> Result<Put, BlobError> {
        let path = self.path(id);
        let len = bytes.len() as u64;
        match retry::retry(|| self.store.put_if_absent(&path, bytes.clone())).await {
            Ok(_) => Ok(Put::Created),
            Err(StoreError::AlreadyExists { .. }) => match self.existing(id, len).await? {
                Some(put) => Ok(put),
                // The name was taken and is gone again: only GC deletes, and
                // never a blob a writer still offers, so report the race.
                None => Err(StoreError::NotFound { path }.into()),
            },
            Err(err) => Err(err.into()),
        }
    }

    /// An unconditional multipart upload of `len` bytes, retried with a
    /// fresh body from `body` after a transient failure.
    async fn put_large<F>(&self, id: &BlobId, len: u64, body: F) -> Result<Put, BlobError>
    where
        F: Fn() -> BoxStream<'static, io::Result<Bytes>> + Send + Sync,
    {
        self.check_large(id, len)?;
        if let Some(put) = self.existing(id, len).await? {
            return Ok(put);
        }
        let path = self.path(id);
        retry::retry(|| self.store.put_multipart(&path, body(), PART_SIZE)).await?;
        Ok(Put::Created)
    }

    /// Writes `body` to a new spill file, checking it is exactly `len`
    /// bytes.
    async fn spill_body(
        &self,
        id: &BlobId,
        spill: &SpillDir,
        mut body: BoxStream<'static, io::Result<Bytes>>,
        len: u64,
    ) -> Result<FsPath, BlobError> {
        let file = spill
            .path(SPILL_BODY)
            .map_err(|err| BlobError::Spill(FsError::Backend(err.to_string())))?;
        let mut got = 0u64;
        while let Some(chunk) = body.try_next().await.map_err(BlobError::Body)? {
            got += chunk.len() as u64;
            if got > len {
                return Err(BlobError::LengthMismatch {
                    id: id.clone(),
                    declared: len,
                    got,
                });
            }
            self.spill
                .append(&file, chunk)
                .await
                .map_err(BlobError::Spill)?;
        }
        if got != len {
            return Err(BlobError::LengthMismatch {
                id: id.clone(),
                declared: len,
                got,
            });
        }
        Ok(file)
    }
}

/// A stream of `file`'s bytes on `fs`, read in parts.
fn read_spill(fs: Arc<dyn Fs>, file: FsPath, len: u64) -> BoxStream<'static, io::Result<Bytes>> {
    stream::try_unfold(0u64, move |offset| {
        let (fs, file) = (Arc::clone(&fs), file.clone());
        async move {
            if offset >= len {
                return Ok(None);
            }
            let end = len.min(offset + PART_SIZE as u64);
            let part = fs
                .read_range(&file, offset..end)
                .await
                .map_err(io::Error::other)?;
            Ok(Some((part, end)))
        }
    })
    .boxed()
}

/// Collects a body of at most `len` bytes, checking its length.
async fn collect_body(
    id: &BlobId,
    mut body: BoxStream<'static, io::Result<Bytes>>,
    len: u64,
) -> Result<Bytes, BlobError> {
    // `len` is at most the multipart threshold here, so it fits in memory.
    let mut out = BytesMut::with_capacity(len as usize);
    while let Some(chunk) = body.try_next().await.map_err(BlobError::Body)? {
        let got = (out.len() + chunk.len()) as u64;
        if got > len {
            return Err(BlobError::LengthMismatch {
                id: id.clone(),
                declared: len,
                got,
            });
        }
        out.extend_from_slice(&chunk);
    }
    if out.len() as u64 != len {
        return Err(BlobError::LengthMismatch {
            id: id.clone(),
            declared: len,
            got: out.len() as u64,
        });
    }
    Ok(out.freeze())
}

#[async_trait]
impl BlobStore for StoreBlobStore {
    async fn put(&self, id: &BlobId, bytes: Bytes) -> Result<Put, BlobError> {
        let len = bytes.len() as u64;
        if len <= MULTIPART_THRESHOLD {
            return self.put_small(id, bytes).await;
        }
        self.put_large(id, len, || {
            let bytes = bytes.clone();
            stream::once(async move { Ok(bytes) }).boxed()
        })
        .await
    }

    async fn put_stream(
        &self,
        id: &BlobId,
        body: BoxStream<'static, io::Result<Bytes>>,
        len: u64,
    ) -> Result<Put, BlobError> {
        if len <= MULTIPART_THRESHOLD {
            let bytes = collect_body(id, body, len).await?;
            return self.put_small(id, bytes).await;
        }
        // Refused before any byte is read or spilled.
        self.check_large(id, len)?;
        let max_file = self.spill.caps().max_file;
        if len > max_file {
            return Err(BlobError::Spill(FsError::TooLarge(len)));
        }
        if let Some(put) = self.existing(id, len).await? {
            return Ok(put);
        }
        let spill = SpillDir::new(Arc::clone(&self.spill), &self.spill_parent)
            .map_err(|err| BlobError::Spill(FsError::Backend(err.to_string())))?;
        let result = match self.spill_body(id, &spill, body, len).await {
            Ok(file) => {
                let fs = Arc::clone(&self.spill);
                self.put_large(id, len, || read_spill(Arc::clone(&fs), file.clone(), len))
                    .await
            }
            Err(err) => Err(err),
        };
        if let Err(err) = spill.close().await {
            tracing::warn!(blob = %id, error = %err, "removing a blob's spill directory failed");
        }
        result
    }

    async fn get_range(&self, id: &BlobId, range: Range<u64>) -> Result<Bytes, BlobError> {
        let out_of_range = |len| BlobError::OutOfRange {
            id: id.clone(),
            range: range.clone(),
            len,
        };
        let not_found = || BlobError::NotFound { id: id.clone() };
        if range.start >= range.end {
            // No bytes to read, but the range must still lie inside the blob.
            let len = self.head(id).await?.ok_or_else(not_found)?;
            if range.start > range.end || range.end > len {
                return Err(out_of_range(len));
            }
            return Ok(Bytes::new());
        }
        let path = self.path(id);
        if let Some(h1) = &self.h1 {
            return self.get_range_cached(h1, id, &path, range).await;
        }
        let mut attempt = 0;
        loop {
            match self.store.get_range_with_info(&path, range.clone()).await {
                Ok((bytes, info)) => {
                    // A range that runs past the end is cut by the store.
                    if range.end > info.size {
                        return Err(out_of_range(info.size));
                    }
                    return Ok(bytes);
                }
                Err(StoreError::NotFound { .. }) => return Err(not_found()),
                Err(err) if is_transient(&err) => {
                    // A range that starts past the end is refused by the
                    // store as an error; that is not worth retrying.
                    if attempt == 0 {
                        let len = self.head(id).await?.ok_or_else(not_found)?;
                        if range.end > len {
                            return Err(out_of_range(len));
                        }
                    }
                    if attempt >= BACKOFF.len() {
                        return Err(err.into());
                    }
                    pause(attempt).await;
                    attempt += 1;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn head(&self, id: &BlobId) -> Result<Option<u64>, BlobError> {
        let path = self.path(id);
        match retry::retry(|| self.store.head(&path)).await {
            Ok(info) => Ok(Some(info.size)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

/// Builds pack bundles (`.lpk`, D390): the pack, its version 2 idx, and the
/// footer.
#[derive(Debug)]
pub struct LpkWriter;

/// The idx version 2 magic, `\377tOc`.
const IDX_MAGIC: [u8; 4] = [0xff, b't', b'O', b'c'];
/// Idx header (magic, version) and fan-out table bytes.
const IDX_HEADER_LEN: usize = 8 + 256 * 4;
/// Idx trailer: the pack checksum, then the idx checksum (SHA-1).
const IDX_TRAILER_LEN: usize = 20 + 20;
/// Idx bytes per object: name, CRC32 and 32-bit offset.
const IDX_ENTRY_LEN: usize = 20 + 4 + 4;

fn be_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

impl LpkWriter {
    /// The bundle `pack ‖ idx ‖ footer`, its blob id (the pack checksum) and
    /// its `PackRef`. Checks that `idx` is a version 2 idx of `pack`: the
    /// same object count and the pack's checksum in the idx trailer.
    pub fn from_parts(pack: Bytes, idx: Bytes) -> Result<(BlobId, pb::PackRef, Bytes), BlobError> {
        let (id, pack_ref) = Self::check_parts(&pack, &idx)?;
        let footer = encode_footer(&pack, &idx);
        let mut bundle = BytesMut::with_capacity(pack.len() + idx.len() + LPK_FOOTER_LEN);
        bundle.extend_from_slice(&pack);
        bundle.extend_from_slice(&idx);
        bundle.extend_from_slice(&footer);
        Ok((id, pack_ref, bundle.freeze()))
    }

    /// The checks of [`LpkWriter::from_parts`] without building the bundle:
    /// its blob id and `PackRef` (Task 24 verifies an uploaded bundle in
    /// place).
    pub fn check_parts(pack: &[u8], idx: &[u8]) -> Result<(BlobId, pb::PackRef), BlobError> {
        let invalid = |reason: String| BlobError::InvalidPack(reason);
        let checksum = pack_checksum(pack).map_err(|err| invalid(err.to_string()))?;
        let pack_version = be_u32(pack, 4);
        if !(2..=3).contains(&pack_version) {
            return Err(invalid(format!("pack version {pack_version}")));
        }
        let count = be_u32(pack, 8);
        if idx.len() < IDX_HEADER_LEN + IDX_TRAILER_LEN || idx[..4] != IDX_MAGIC {
            return Err(invalid("not a version 2 idx".into()));
        }
        let idx_version = be_u32(idx, 4);
        if idx_version != 2 {
            return Err(invalid(format!("idx version {idx_version}")));
        }
        let fanout: Vec<u32> = (0..256).map(|i| be_u32(idx, 8 + i * 4)).collect();
        if fanout.windows(2).any(|w| w[0] > w[1]) {
            return Err(invalid("idx fan-out is not monotonic".into()));
        }
        if fanout[255] != count {
            return Err(invalid(format!(
                "idx holds {} objects, the pack {count}",
                fanout[255]
            )));
        }
        // The fixed part, then 8 bytes per large offset.
        // Checked: `count` comes from the pushed pack, and `usize` may be
        // 32 bits.
        let fixed = usize::try_from(count)
            .ok()
            .and_then(|count| count.checked_mul(IDX_ENTRY_LEN))
            .and_then(|entries| entries.checked_add(IDX_HEADER_LEN + IDX_TRAILER_LEN))
            .ok_or_else(|| invalid(format!("{count} objects overflow the idx size")))?;
        if idx.len() < fixed || !(idx.len() - fixed).is_multiple_of(8) {
            return Err(invalid(format!(
                "idx of {} bytes does not fit {count} objects",
                idx.len()
            )));
        }
        let trailer = idx.len() - IDX_TRAILER_LEN;
        if idx[trailer..trailer + 20] != *checksum.as_bytes() {
            return Err(invalid("the idx names another pack".into()));
        }
        let pack_ref = pb::PackRef {
            checksum: checksum.to_hex().to_string(),
            object_count: u64::from(count),
            pack_len: pack.len() as u64,
            idx_offset: pack.len() as u64,
            idx_len: idx.len() as u64,
            has_bitmap: false,
        };
        Ok((BlobId::from_checksum(&checksum), pack_ref))
    }
}

/// The blob of `pack` and a section of it.
fn section(pack: &pb::PackRef, offset: u64, len: u64) -> Result<(BlobId, Range<u64>), BlobError> {
    let id =
        BlobId::parse(&pack.checksum).map_err(|err| BlobError::InvalidPack(err.to_string()))?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| BlobError::InvalidPack(format!("pack {id}: section overflows")))?;
    Ok((id, offset..end))
}

/// The idx section of `pack`, in one range read.
pub async fn read_idx(blobs: &dyn BlobStore, pack: &pb::PackRef) -> Result<Bytes, BlobError> {
    let (id, range) = section(pack, pack.idx_offset, pack.idx_len)?;
    blobs.get_range(&id, range).await
}

/// The pack section of `pack`, in one range read.
pub async fn read_pack(blobs: &dyn BlobStore, pack: &pb::PackRef) -> Result<Bytes, BlobError> {
    let (id, range) = section(pack, 0, pack.pack_len)?;
    blobs.get_range(&id, range).await
}
