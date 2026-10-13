//! `fetch` (Ruling 4): download every live pack the local repository lacks,
//! the fork parents' included, and write each stored pack and its stored
//! idx into `objects/pack/` with no `index-pack` run. Packs are immutable
//! and content-named, and their idx was built when they were pushed.

use std::io::Write;
use std::sync::Arc;

use loams_git::format::pb;
use loams_git::limits::MAX_FORK_DEPTH;
use loams_git::{
    BlobId, BlobStore, NamespaceId, RefSnapshot, RepoId, RepoPaths, Seq, StoreBlobStore,
    StoreWalStore, state_at,
};
use loams_store::Store;

use crate::HelperError;
use crate::local::Local;

/// Bytes downloaded per range read, so a large pack never sits in memory.
const CHUNK: u64 = 16 * 1024 * 1024;

/// A stored pack and the blob store of the repository that holds it.
struct Located {
    blobs: Arc<StoreBlobStore>,
    pack: pb::PackRef,
}

/// The live packs of `snapshot` and of every fork parent at the seq its
/// child names, at most `MAX_FORK_DEPTH` parents deep.
async fn live_packs(
    store: &Store,
    ns: NamespaceId,
    repo: &RepoId,
    snapshot: &RefSnapshot,
) -> Result<Vec<Located>, HelperError> {
    let mut out = Vec::new();
    let mut current = (repo.clone(), snapshot.clone());
    for depth in 0..=MAX_FORK_DEPTH {
        let (id, snap) = current;
        let paths = RepoPaths::new(ns, &id);
        let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
        out.extend(snap.packs.iter().map(|pack| Located {
            blobs: Arc::clone(&blobs),
            pack: pack.clone(),
        }));
        let Some(parent) = snap.parent else {
            return Ok(out);
        };
        if depth == MAX_FORK_DEPTH {
            break;
        }
        let parent_id = RepoId::parse(&parent.repo_id)
            .map_err(|err| HelperError::store("read the fork parent", err))?;
        let wal = StoreWalStore::new(store.clone(), RepoPaths::new(ns, &parent_id));
        let state = state_at(&wal, Seq(parent.seq))
            .await
            .map_err(|err| HelperError::store(format!("read fork parent {parent_id}"), err))?;
        current = (parent_id, state.snapshot());
    }
    Err(HelperError::store(
        "read the fork parents",
        format!("more than {MAX_FORK_DEPTH} fork parents"),
    ))
}

/// Downloads `range` of blob `id` into `file`, `CHUNK` bytes at a time.
async fn download(
    blobs: &StoreBlobStore,
    id: &BlobId,
    start: u64,
    len: u64,
    file: &mut std::fs::File,
) -> Result<(), HelperError> {
    let end = start + len;
    let mut at = start;
    while at < end {
        let next = (at + CHUNK).min(end);
        let bytes = blobs
            .get_range(id, at..next)
            .await
            .map_err(|err| HelperError::store(format!("download pack {id}"), err))?;
        file.write_all(&bytes)
            .map_err(|err| HelperError::io(format!("write pack {id}"), err))?;
        at = next;
    }
    file.sync_all()
        .map_err(|err| HelperError::io(format!("write pack {id}"), err))
}

/// Fetches every pack of `snapshot` (and its fork parents) that `local`
/// lacks, and records them. Returns how many were downloaded.
pub async fn fetch(
    store: &Store,
    ns: NamespaceId,
    repo: &RepoId,
    snapshot: &RefSnapshot,
    local: &Local,
    progress: bool,
) -> Result<usize, HelperError> {
    let recorded = local.recorded_packs()?;
    let mut have = Vec::new();
    let mut downloaded = 0;
    for Located { blobs, pack } in live_packs(store, ns, repo, snapshot).await? {
        let checksum = pack.checksum.clone();
        if recorded.contains(&checksum) || local.has_pack(&checksum)? {
            have.push(checksum);
            continue;
        }
        let id =
            BlobId::parse(&checksum).map_err(|err| HelperError::store("read the pack set", err))?;
        if progress {
            eprintln!(
                "loams: fetching pack {checksum} ({} objects, {} bytes)",
                pack.object_count, pack.pack_len
            );
        }
        let (pack_path, mut pack_file) = local.temp_pack_file("pack")?;
        let (idx_path, mut idx_file) = local.temp_pack_file("idx")?;
        let written = async {
            download(&blobs, &id, 0, pack.pack_len, &mut pack_file).await?;
            download(&blobs, &id, pack.idx_offset, pack.idx_len, &mut idx_file).await?;
            check_trailer(&pack_path, &checksum)
        }
        .await;
        drop((pack_file, idx_file));
        if let Err(err) = written {
            let _ = std::fs::remove_file(&pack_path);
            let _ = std::fs::remove_file(&idx_path);
            return Err(err);
        }
        local.install_pack(&checksum, &pack_path, &idx_path)?;
        have.push(checksum);
        downloaded += 1;
    }
    local.record_packs(&have)?;
    Ok(downloaded)
}

/// Checks that the downloaded pack ends in the checksum it is named by.
fn check_trailer(path: &std::path::Path, checksum: &str) -> Result<(), HelperError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)
        .map_err(|err| HelperError::io(format!("check pack {checksum}"), err))?;
    let mut trailer = [0u8; 20];
    file.seek(SeekFrom::End(-20))
        .and_then(|_| file.read_exact(&mut trailer))
        .map_err(|err| HelperError::io(format!("check pack {checksum}"), err))?;
    if hex::encode(trailer) == checksum {
        Ok(())
    } else {
        Err(HelperError::store(
            format!("download pack {checksum}"),
            "the downloaded pack does not end in its checksum",
        ))
    }
}
