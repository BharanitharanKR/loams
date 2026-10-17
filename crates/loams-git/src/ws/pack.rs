//! Workspace packs (§36 §19.4.1, §19.5.2; plan GT1 Task 24): the objects a
//! workspace made (blobs, trees, commits), uploaded as one `.lpk` bundle per
//! save and stored create-only under `ws/<ws_id>/packs/<checksum>.lpk`.
//!
//! [`put_workspace_pack`] is `PutWorkspacePack` without a transport. Before
//! anything is stored it checks the bundle as the receive path checks a
//! pushed pack (§36 §6.3): at most `MAX_PACK_BYTES`; the footer, the pack's
//! and the idx's SHA-1 trailers; that the idx names every entry of the pack
//! exactly once; that every object, deltas resolved, hashes to the id the
//! idx gives it and parses as its kind; and that no delta's base lies
//! outside the pack (a workspace pack is self-contained). Each object is
//! then offered to the secret scanner's hook ([`PackScan`]; Task 67 fills
//! it, [`NoScan`] until then). The snapshot that names the pack is the
//! commit point; a pack no snapshot names is garbage after `GC_GRACE`.
//!
//! **Forks** (ruling T23.3). A fork's snapshot names its source's packs by
//! checksum, and they stay under the source's prefix. [`WsPackBlobs`] reads
//! a workspace's packs from its own prefix, then from each fork source's
//! ([`fork_sources`]), so a forked workspace reads every pack it names.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use gix_object::Kind;
use loams_cache::RangeCache;
use loams_store::Store;

use super::{WorkspaceStore, WsError};
use crate::blob::{BlobError, BlobId, BlobStore, LpkWriter, Put, StoreBlobStore};
use crate::format::lpk::{LPK_FOOTER_LEN, decode_footer};
use crate::format::pb;
use crate::ids::{ObjectId, RepoId, SnapN, WsId};
use crate::limits::{MAX_FORK_DEPTH, MAX_PACK_BYTES};
use crate::odb::{Location, OdbError, RangeOdb};
use crate::paths::RepoPaths;

/// A pack's header: `PACK`, the version and the object count.
const PACK_HEADER: u64 = 12;
/// A pack's (and an idx's) SHA-1 trailer.
const TRAILER: usize = 20;
/// Idx version 2: magic and version, then the fan-out table.
const IDX_NAMES: usize = 8 + 256 * 4;

/// The scanner found something that must not be stored (§19.13.9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRefusal {
    /// The object that matched.
    pub oid: ObjectId,
    /// The rule that matched.
    pub rule: String,
}

/// The secret scanner's hook on uploaded objects (§19.13.9; plan GT1
/// Task 67). Called once per object of a verified pack, deltas resolved.
pub trait PackScan: Send + Sync + fmt::Debug {
    /// `Err` refuses the whole pack.
    fn object(&self, oid: &ObjectId, kind: Kind, data: &[u8]) -> Result<(), ScanRefusal>;
}

/// The hook until Task 67: every object passes.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoScan;

impl PackScan for NoScan {
    fn object(&self, _: &ObjectId, _: Kind, _: &[u8]) -> Result<(), ScanRefusal> {
        Ok(())
    }
}

/// Errors of [`put_workspace_pack`].
#[derive(Debug, thiserror::Error)]
pub enum WsPackError {
    /// The bundle is over `MAX_PACK_BYTES` (2 GiB).
    #[error("the pack has {len} bytes, more than the {max} accepted")]
    TooLarge {
        /// Its length.
        len: u64,
        /// The largest accepted.
        max: u64,
    },
    /// Not a well-formed, self-contained pack bundle.
    #[error("invalid workspace pack: {0}")]
    Invalid(String),
    /// An object does not hash to the id the idx gives it.
    #[error("object at offset {offset} hashes to {actual}, not the {claimed} its idx names")]
    BadHash {
        /// The id the idx gives.
        claimed: ObjectId,
        /// The object's real id.
        actual: ObjectId,
        /// Its entry's offset in the pack.
        offset: u64,
    },
    /// The secret scanner refused an object.
    #[error("object {} refused by the secret scanner (rule {})", .0.oid, .0.rule)]
    Refused(ScanRefusal),
    /// Storing the pack failed.
    #[error(transparent)]
    Blob(#[from] BlobError),
}

/// A pack that passed [`verify_workspace_pack`].
#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedPack {
    /// Its blob id (the pack checksum).
    pub id: BlobId,
    /// Its `PackRef`, as a snapshot names it.
    pub pack: pb::PackRef,
    /// The object ids it holds, in idx order.
    pub objects: Vec<ObjectId>,
}

/// Refuses a bundle over `MAX_PACK_BYTES` (2 GiB, §36 §6.3), before or
/// while it is received.
pub fn check_pack_len(len: u64) -> Result<(), WsPackError> {
    if len > MAX_PACK_BYTES {
        return Err(WsPackError::TooLarge {
            len,
            max: MAX_PACK_BYTES,
        });
    }
    Ok(())
}

/// Verifies `lpk` (see the module docs), offering every object to `scan`;
/// `repo` names the repository in errors only.
pub async fn verify_workspace_pack(
    repo: &RepoId,
    lpk: &Bytes,
    scan: &dyn PackScan,
) -> Result<VerifiedPack, WsPackError> {
    let len = lpk.len() as u64;
    check_pack_len(len)?;
    let invalid = |reason: &dyn fmt::Display| WsPackError::Invalid(reason.to_string());
    if lpk.len() < LPK_FOOTER_LEN {
        return Err(invalid(&"shorter than a pack bundle's footer"));
    }
    let tail: &[u8; LPK_FOOTER_LEN] = lpk[lpk.len() - LPK_FOOTER_LEN..]
        .try_into()
        .expect("the footer's length");
    let footer = decode_footer(tail).map_err(|err| invalid(&err))?;
    if footer.object_len() != len {
        return Err(invalid(&format!(
            "the footer describes {} bytes, the bundle has {len}",
            footer.object_len()
        )));
    }
    let pack = lpk.slice(..footer.pack_len as usize);
    let idx = lpk.slice(footer.pack_len as usize..(footer.pack_len + footer.idx_len) as usize);
    let (id, pack_ref, bundle) =
        LpkWriter::from_parts(pack.clone(), idx.clone()).map_err(|err| invalid(&err))?;
    if bundle != *lpk {
        return Err(invalid(&"the footer's CRC32C does not match its sections"));
    }
    check_trailer(&pack, "pack")?;
    check_trailer(&idx, "idx")?;
    let entries = idx_entries(&idx, pack_ref.object_count)?;

    // Every entry, read through a database of this pack alone, so a delta
    // whose base is elsewhere is refused and an object the repository
    // already holds is still read from here.
    let blobs: Arc<dyn BlobStore> = Arc::new(OneBlob {
        id: id.clone(),
        bytes: lpk.clone(),
    });
    let odb = RangeOdb::of_packs(repo.clone(), blobs, std::slice::from_ref(&pack_ref))
        .map_err(|err| invalid(&err))?;
    let mut by_offset: Vec<(u64, ObjectId)> = entries
        .iter()
        .map(|(oid, offset)| (*offset, *oid))
        .collect();
    by_offset.sort_unstable();
    let mut next = PACK_HEADER;
    for (offset, claimed) in &by_offset {
        if *offset != next {
            return Err(invalid(&format!(
                "the idx places an entry at {offset}, the pack's next entry is at {next}"
            )));
        }
        let loc = Location {
            layer: 0,
            pack: 0,
            offset: *offset,
        };
        let (kind, data) = odb.read_at(&loc).await.map_err(odb_invalid)?;
        let actual = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &data)
            .map_err(|err| invalid(&err))?;
        if actual != *claimed {
            return Err(WsPackError::BadHash {
                claimed: *claimed,
                actual,
                offset: *offset,
            });
        }
        if kind != Kind::Blob {
            gix_object::ObjectRef::from_bytes(&data, kind, gix_hash::Kind::Sha1)
                .map_err(|err| invalid(&format!("object {actual} is not a valid {kind}: {err}")))?;
        }
        scan.object(&actual, kind, &data)
            .map_err(WsPackError::Refused)?;
        let raw = odb.raw_entry(&loc).await.map_err(odb_invalid)?;
        next = offset + raw.len() as u64;
    }
    if next != footer.pack_len - TRAILER as u64 {
        return Err(invalid(&format!(
            "the pack's entries end at {next}, its trailer starts at {}",
            footer.pack_len - TRAILER as u64
        )));
    }
    Ok(VerifiedPack {
        id,
        pack: pack_ref,
        objects: entries.into_iter().map(|(oid, _)| oid).collect(),
    })
}

/// `PutWorkspacePack` (§19.5.2): verifies `lpk` and stores it in `blobs`
/// (the workspace's, `StoreBlobStore::workspace`), create-only. A pack
/// already stored there answers `Put::Existed`.
pub async fn put_workspace_pack(
    repo: &RepoId,
    blobs: &dyn BlobStore,
    lpk: Bytes,
    scan: &dyn PackScan,
) -> Result<(VerifiedPack, Put), WsPackError> {
    let verified = verify_workspace_pack(repo, &lpk, scan).await?;
    let put = blobs.put(&verified.id, lpk).await?;
    Ok((verified, put))
}

fn odb_invalid(err: OdbError) -> WsPackError {
    match err {
        OdbError::Blob(err) => WsPackError::Blob(err),
        other => WsPackError::Invalid(other.to_string()),
    }
}

/// Checks a SHA-1 trailer over everything before it.
fn check_trailer(bytes: &[u8], what: &str) -> Result<(), WsPackError> {
    let (body, trailer) = bytes.split_at(bytes.len() - TRAILER);
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(body);
    let sum = hasher
        .try_finalize()
        .map_err(|err| WsPackError::Invalid(format!("hashing the {what}: {err}")))?;
    if sum.as_bytes() != trailer {
        return Err(WsPackError::Invalid(format!(
            "the {what}'s SHA-1 trailer does not match its contents"
        )));
    }
    Ok(())
}

/// The names and offsets of a version 2 idx that `LpkWriter::from_parts`
/// accepted. Names must be strictly ascending and agree with the fan-out.
fn idx_entries(idx: &[u8], count: u64) -> Result<Vec<(ObjectId, u64)>, WsPackError> {
    let invalid = |reason: String| WsPackError::Invalid(reason);
    let be32 = |at: usize| u32::from_be_bytes(idx[at..at + 4].try_into().expect("4 bytes"));
    let count = usize::try_from(count).map_err(|_| invalid("too many objects".into()))?;
    let names = IDX_NAMES;
    let offsets = names + count * (20 + 4);
    let large = offsets + count * 4;
    let large_count = (idx.len() - 2 * TRAILER - large) / 8;
    let mut out = Vec::with_capacity(count);
    let mut fanout = [0u32; 256];
    for i in 0..count {
        let at = names + i * 20;
        let oid = ObjectId::try_from(&idx[at..at + 20])
            .map_err(|err| invalid(format!("idx name {i}: {err}")))?;
        if let Some((prev, _)) = out.last()
            && *prev >= oid
        {
            return Err(invalid("idx names are not strictly ascending".into()));
        }
        fanout[usize::from(oid.as_bytes()[0])] += 1;
        let word = be32(offsets + i * 4);
        let offset = if word & 0x8000_0000 == 0 {
            u64::from(word)
        } else {
            let slot = (word & 0x7fff_ffff) as usize;
            if slot >= large_count {
                return Err(invalid(format!("idx large offset {slot} is missing")));
            }
            let at = large + slot * 8;
            u64::from_be_bytes(idx[at..at + 8].try_into().expect("8 bytes"))
        };
        out.push((oid, offset));
    }
    let mut running = 0u32;
    for (byte, n) in fanout.iter().enumerate() {
        running += n;
        if be32(8 + byte * 4) != running {
            return Err(invalid(format!(
                "idx fan-out entry {byte} does not count its names"
            )));
        }
    }
    let distinct: BTreeSet<u64> = out.iter().map(|(_, offset)| *offset).collect();
    if distinct.len() != out.len() {
        return Err(invalid("idx names two objects at one offset".into()));
    }
    Ok(out)
}

/// One bundle in memory, as a blob store: what verification reads.
#[derive(Debug)]
struct OneBlob {
    id: BlobId,
    bytes: Bytes,
}

#[async_trait]
impl BlobStore for OneBlob {
    async fn put(&self, id: &BlobId, _: Bytes) -> Result<Put, BlobError> {
        Err(BlobError::NotFound { id: id.clone() })
    }

    async fn put_stream(
        &self,
        id: &BlobId,
        _: BoxStream<'static, io::Result<Bytes>>,
        _: u64,
    ) -> Result<Put, BlobError> {
        Err(BlobError::NotFound { id: id.clone() })
    }

    async fn get_range(&self, id: &BlobId, range: Range<u64>) -> Result<Bytes, BlobError> {
        if *id != self.id {
            return Err(BlobError::NotFound { id: id.clone() });
        }
        let len = self.bytes.len() as u64;
        if range.start > range.end || range.end > len {
            return Err(BlobError::OutOfRange {
                id: id.clone(),
                range,
                len,
            });
        }
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }

    async fn head(&self, id: &BlobId) -> Result<Option<u64>, BlobError> {
        Ok((*id == self.id).then_some(self.bytes.len() as u64))
    }
}

/// A workspace's packs as one blob store (ruling T23.3): its own prefix
/// first, then each fork source's, nearest first. Puts go to its own
/// prefix. Which prefix holds a pack is learned on the first read and kept.
#[derive(Debug)]
pub struct WsPackBlobs {
    layers: Vec<StoreBlobStore>,
    found: Mutex<HashMap<BlobId, usize>>,
}

impl WsPackBlobs {
    /// The packs of workspace `ws` of the repository at `paths`, then of
    /// its fork `sources` (nearest first, as [`fork_sources`] answers),
    /// read through `h1` when given.
    pub fn new(
        store: &Store,
        paths: &RepoPaths,
        ws: &WsId,
        sources: &[WsId],
        h1: Option<&RangeCache>,
    ) -> Self {
        let layers = std::iter::once(ws)
            .chain(sources)
            .map(|ws| {
                let blobs = StoreBlobStore::workspace(store.clone(), paths, ws);
                match h1 {
                    Some(h1) => blobs.with_range_cache(h1.clone()),
                    None => blobs,
                }
            })
            .collect();
        Self {
            layers,
            found: Mutex::default(),
        }
    }

    fn known(&self, id: &BlobId) -> Option<usize> {
        self.found
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .copied()
    }

    fn learn(&self, id: &BlobId, layer: usize) {
        self.found
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id.clone(), layer);
    }
}

#[async_trait]
impl BlobStore for WsPackBlobs {
    async fn put(&self, id: &BlobId, bytes: Bytes) -> Result<Put, BlobError> {
        self.layers[0].put(id, bytes).await
    }

    async fn put_stream(
        &self,
        id: &BlobId,
        body: BoxStream<'static, io::Result<Bytes>>,
        len: u64,
    ) -> Result<Put, BlobError> {
        self.layers[0].put_stream(id, body, len).await
    }

    async fn get_range(&self, id: &BlobId, range: Range<u64>) -> Result<Bytes, BlobError> {
        if let Some(layer) = self.known(id) {
            return self.layers[layer].get_range(id, range).await;
        }
        for (index, layer) in self.layers.iter().enumerate() {
            match layer.get_range(id, range.clone()).await {
                Ok(bytes) => {
                    self.learn(id, index);
                    return Ok(bytes);
                }
                Err(BlobError::NotFound { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(BlobError::NotFound { id: id.clone() })
    }

    async fn head(&self, id: &BlobId) -> Result<Option<u64>, BlobError> {
        for (index, layer) in self.layers.iter().enumerate() {
            if let Some(len) = layer.head(id).await? {
                self.learn(id, index);
                return Ok(Some(len));
            }
        }
        Ok(None)
    }
}

/// The workspaces `ws` was forked from, nearest first (ruling T23.3): the
/// `fork` origin of its snapshot 0 (or of its latest snapshot, once
/// retention has collected snapshot 0), and so on up the chain. At most
/// `MAX_FORK_DEPTH` (8) sources; a longer chain is `Invalid`.
pub async fn fork_sources(
    store: &dyn WorkspaceStore,
    repo: &RepoId,
    ws: &WsId,
) -> Result<Vec<WsId>, WsError> {
    let mut out = Vec::new();
    let mut cur = ws.clone();
    loop {
        let snap = match store.get(repo, &cur, SnapN(0)).await? {
            Some(snap) => snap,
            None => store.latest(repo, &cur).await?,
        };
        let Some(fork) = &snap.fork else {
            return Ok(out);
        };
        if fork.repo_id != repo.as_str() {
            return Err(WsError::Corrupt {
                path: format!("workspace {cur}"),
                reason: format!("forked from repository {}, not {repo}", fork.repo_id),
            });
        }
        if out.len() == MAX_FORK_DEPTH {
            return Err(WsError::Invalid {
                reason: format!("workspace {ws} is forked more than {MAX_FORK_DEPTH} deep"),
            });
        }
        let source = WsId::parse(&fork.ws_id).map_err(|err| WsError::Corrupt {
            path: format!("workspace {cur}"),
            reason: err.to_string(),
        })?;
        out.push(source.clone());
        cur = source;
    }
}
