//! Native snapshots (GR1 Task 13; design §48 §6.4): Grafeo's own `backup_full` of the
//! checkpointed database, one `.grafeo` file, uploaded to the bucket.
//!
//! `backup_full` checkpoints the database and copies its file (R0.12), so the copy holds what the
//! store holds, including what a replay wrote directly to the store (R9.5). The caller holds the
//! graph's gate shared while it runs, so the copy is the durable state (R9.3) and matches the
//! durable offset read under the same hold. Restoring is the copy itself: Grafeo's own
//! `restore_to_epoch` of a full backup with no increments copies the file to its output, and the
//! file then opens as the graph's database.
//!
//! A native snapshot is read only by an engine that reads its file format: its manifest entry
//! carries the engine version that wrote it, and a reader whose engine differs uses a portable
//! snapshot instead (§48 §6.5, Task 14).

use std::io::Read as _;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::StreamExt as _;
use grafeo::GrafeoDB;
use loams_store::Store;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{SnapshotEntry, SnapshotError, SnapshotKind, unavailable};
use crate::engine::GraphError;

/// The object name of a native snapshot's database file.
pub const FILE_NAME: &str = "graph.grafeo";

/// The part size of an upload and the range size of a download.
const CHUNK: usize = 8 << 20;

/// A native snapshot taken into a local staging directory, not yet uploaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Taken {
    /// The database file.
    pub file: PathBuf,
    /// Its size, in bytes.
    pub bytes: u64,
    /// Its CRC-32C.
    pub crc32c: u32,
    /// Grafeo's epoch the backup covers.
    pub epoch: u64,
}

/// Takes a native snapshot of `db` into `staging`, a directory that does not exist yet. The
/// caller holds the graph's gate shared, so nothing commits meanwhile.
///
/// # Errors
///
/// [`SnapshotError::Engine`] when Grafeo cannot back the database up (an in-memory database
/// cannot: R0.12), or the file cannot be read back.
pub fn take(db: &GrafeoDB, staging: &Path) -> Result<Taken, SnapshotError> {
    let segment = db
        .backup_full(staging)
        .map_err(|err| GraphError::engine(format!("taking a native snapshot: {err}")))?;
    let file = staging.join(&segment.filename);
    let (bytes, crc32c) = checksum(&file)
        .map_err(|err| GraphError::engine(format!("reading a native snapshot back: {err}")))?;
    Ok(Taken {
        file,
        bytes,
        crc32c,
        epoch: segment.end_epoch.as_u64(),
    })
}

/// A file's size and CRC-32C, read in chunks.
fn checksum(file: &Path) -> std::io::Result<(u64, u32)> {
    let mut reader = std::fs::File::open(file)?;
    let mut buffer = vec![0; CHUNK];
    let (mut bytes, mut crc) = (0u64, 0u32);
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok((bytes, crc));
        }
        crc = crc32c::crc32c_append(crc, &buffer[..read]);
        bytes += read as u64;
    }
}

/// Uploads `file` to `key`: in one request up to [`CHUNK`] bytes, as a multipart upload above.
///
/// # Errors
///
/// [`SnapshotError::Unavailable`] when the file cannot be read or the bucket refuses it.
pub async fn upload(store: &Store, key: &str, file: &Path) -> Result<(), SnapshotError> {
    let size = tokio::fs::metadata(file)
        .await
        .map_err(|err| unavailable("reading a native snapshot", &err))?
        .len();
    if size <= CHUNK as u64 {
        let bytes = tokio::fs::read(file)
            .await
            .map_err(|err| unavailable("reading a native snapshot", &err))?;
        store
            .put(key, Bytes::from(bytes))
            .await
            .map_err(|err| unavailable("uploading a native snapshot", &err))?;
        return Ok(());
    }
    let reader = tokio::fs::File::open(file)
        .await
        .map_err(|err| unavailable("reading a native snapshot", &err))?;
    let body = futures::stream::unfold(Some(reader), |reader| async move {
        let mut reader = reader?;
        let mut buffer = vec![0; CHUNK];
        let mut filled = 0;
        while filled < CHUNK {
            match reader.read(&mut buffer[filled..]).await {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(err) => return Some((Err(err), None)),
            }
        }
        if filled == 0 {
            return None;
        }
        buffer.truncate(filled);
        Some((Ok(Bytes::from(buffer)), Some(reader)))
    })
    .boxed();
    store
        .put_multipart(key, body, CHUNK)
        .await
        .map_err(|err| unavailable("uploading a native snapshot", &err))?;
    Ok(())
}

/// Downloads native snapshot `entry` to `file` (which must not exist), and checks its size and
/// CRC-32C against the entry. The file then opens as the graph's database.
///
/// # Errors
///
/// [`SnapshotError::Unavailable`] when the bucket fails (including a snapshot swept since its
/// manifest was read), or [`SnapshotError::Corrupt`] when `entry` is not one native file or the
/// download does not match it.
pub async fn restore(
    store: &Store,
    entry: &SnapshotEntry,
    file: &Path,
) -> Result<(), SnapshotError> {
    let [key] = entry.keys.as_slice() else {
        return Err(SnapshotError::Corrupt(format!(
            "snapshot {} is not one native file",
            entry.id
        )));
    };
    if entry.kind != SnapshotKind::Native {
        return Err(SnapshotError::Corrupt(format!(
            "snapshot {} is not native",
            entry.id
        )));
    }
    let mut out = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(file)
        .await
        .map_err(|err| unavailable("writing a native snapshot locally", &err))?;
    let (mut offset, mut crc) = (0u64, 0u32);
    while offset < entry.bytes {
        let end = (offset + CHUNK as u64).min(entry.bytes);
        let bytes = store
            .get_range(key, offset..end)
            .await
            .map_err(|err| unavailable("downloading a native snapshot", &err))?;
        if bytes.is_empty() {
            break;
        }
        crc = crc32c::crc32c_append(crc, &bytes);
        offset += bytes.len() as u64;
        out.write_all(&bytes)
            .await
            .map_err(|err| unavailable("writing a native snapshot locally", &err))?;
    }
    out.sync_all()
        .await
        .map_err(|err| unavailable("writing a native snapshot locally", &err))?;
    let size = store
        .head(key)
        .await
        .map_err(|err| unavailable("downloading a native snapshot", &err))?
        .size;
    if offset != entry.bytes || size != entry.bytes || entry.crc32c.is_some_and(|c| c != crc) {
        return Err(SnapshotError::Corrupt(format!(
            "native snapshot {} does not match its manifest entry",
            entry.id
        )));
    }
    Ok(())
}

/// Opens a downloaded native snapshot as a database with Grafeo's default configuration, which
/// also checks that this engine reads it. Tests and tools only: a graph opens the file through
/// its own configuration.
///
/// # Errors
///
/// [`SnapshotError::Engine`] when Grafeo cannot open it.
pub fn open(file: &Path) -> Result<GrafeoDB, SnapshotError> {
    GrafeoDB::open(file)
        .map_err(|err| GraphError::engine(format!("opening a native snapshot: {err}")).into())
}
