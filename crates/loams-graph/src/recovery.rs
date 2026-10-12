//! Recovery and rehydration (GR1 Task 14; design §48 §6.5; D742, I4).
//!
//! A durable graph is its stream. Its engine starts from a **base** and replays the stream's
//! records after it ([`crate::durable::Durable::open`]). [`open`] chooses the base, first that
//! works:
//!
//! 1. **The local cache** (`<data_dir>/graphs/<gr_id>/`), when a clean close left it stamped
//!    ([`CACHE_STAMP`]): the stamp names the offset the files hold, the next `txn_seq`, the
//!    idempotency index, the engine version, and the size and CRC-32C of the database file. It
//!    is trusted only when every one of those still holds, no sidecar WAL is there (an engine
//!    that did not close cleanly), and the stream still starts at or below its offset. The stamp
//!    is consumed before the engine opens the files, so files that an engine has written to
//!    since stand for nothing until the next clean close stamps them again. This is R9.5's rule:
//!    a replayed engine is checkpointed (and closed) before its local files are trusted.
//! 2. **The newest usable snapshot** the manifest names ([`crate::snapshot::restore`]): native
//!    when the graph is on disk and this engine reads it, else portable; an older one, then a
//!    portable one, when one fails to download, check or load. A failure re-reads the manifest
//!    (a sweep may have retired what an older manifest named, R13.7) at most twice.
//! 3. **Nothing**: the whole stream, when it starts at offset 0.
//!
//! Records the base holds are never applied again: replay starts at the base's offset (R9.5).
//! A graph from before the log is adopted from its local files instead (R11.5).
//!
//! What the engine started from is kept as its [`Recovery`], for `GetGraph`, eviction's
//! rehydration records ([`crate::evict::Rehydration`]) and the tests.

use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use grafeo::GrafeoDB;
use serde::{Deserialize, Serialize};

use crate::GraphId;
use crate::durable::idempotency::Outcome;
use crate::durable::{Base, Durable, DurableSpec};
use crate::engine::{GraphError, as_engine_error};
use crate::snapshot::{SnapshotKind, restore};

/// The stamp a clean close leaves in a graph's local directory: what its files hold.
pub const CACHE_STAMP: &str = "cache.json";

/// The graph's database file in its local directory.
pub const DATABASE_FILE: &str = "graph.grafeo";

/// The stamp's format.
const STAMP_FORMAT: u32 = 1;

/// Snapshot staging directories a checkpoint left behind (GR1 Task 13), cleared on open.
const STAGING: &str = "snapshots";

/// How many times an open re-reads the manifest after every snapshot it named failed.
const MANIFEST_REREADS: usize = 2;

/// What a durable engine started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySource {
    /// A graph from before the log: its local files, appended as its first record (R11.5).
    Adopted,
    /// Its whole stream, into a fresh engine.
    Log,
    /// Its local cache, left stamped by a clean close.
    LocalCache,
    /// A snapshot the manifest names.
    Snapshot(SnapshotKind),
}

impl RecoverySource {
    /// A short name, for logs and metrics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Adopted => "adopted",
            Self::Log => "log",
            Self::LocalCache => "local_cache",
            Self::Snapshot(SnapshotKind::Native) => "native_snapshot",
            Self::Snapshot(SnapshotKind::Portable) => "portable_snapshot",
        }
    }
}

/// How a durable engine was recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    /// What it started from.
    pub source: RecoverySource,
    /// The offset its base covered: replay started here.
    pub from_offset: u64,
    /// The stream's end when it opened: it serves from here.
    pub to_offset: u64,
    /// How many records were applied on top of the base (those in `[from_offset, to_offset)`).
    pub replayed: u64,
    /// How long the open took, base and replay.
    pub elapsed: Duration,
}

/// What a clean close records beside a graph's local files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Stamp {
    format: u32,
    graph: String,
    stream: u64,
    /// Every record below this offset is in the files, and none at or above it.
    offset: u64,
    next_txn_seq: u64,
    engine_version: String,
    /// The database file's size and CRC-32C after the close.
    bytes: u64,
    crc32c: u32,
    keys: Vec<StampKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StampKey {
    key: String,
    recorded_ms: u64,
    token_offset: u64,
    commit_epoch: u64,
}

/// A durable graph's state read just before a clean close: what [`write_stamp`] records once the
/// engine has closed.
#[derive(Debug, Clone)]
pub(crate) struct PendingStamp {
    stamp: Stamp,
}

impl PendingStamp {
    /// The stamp for `durable`'s graph as it is now. The caller holds the only handle on the
    /// graph, so nothing changes it before the close.
    pub(crate) fn of(durable: &Durable) -> Self {
        Self {
            stamp: Stamp {
                format: STAMP_FORMAT,
                graph: durable.graph.to_string(),
                stream: durable.stream.0,
                offset: durable.position.offset(),
                next_txn_seq: durable
                    .next_txn_seq
                    .load(std::sync::atomic::Ordering::Acquire),
                engine_version: crate::ENGINE_VERSION.to_string(),
                bytes: 0,
                crc32c: 0,
                keys: durable
                    .idempotency_entries()
                    .into_iter()
                    .map(|(key, recorded_ms, outcome)| StampKey {
                        key,
                        recorded_ms,
                        token_offset: outcome.token_offset,
                        commit_epoch: outcome.commit_epoch,
                    })
                    .collect(),
            },
        }
    }
}

/// Stamps `dir`, a graph's local directory whose engine has just closed cleanly: its files now
/// stand for `pending`'s offset. Written to a temporary file and renamed, so a stamp is whole or
/// absent.
///
/// # Errors
///
/// The I/O error, or `InvalidData` when the close left a sidecar WAL (the files alone do not hold
/// the state).
pub(crate) fn write_stamp(dir: &Path, pending: PendingStamp) -> std::io::Result<()> {
    if sidecar(dir).exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the engine's sidecar WAL is still there after its close",
        ));
    }
    let (bytes, crc32c) = checksum(&dir.join(DATABASE_FILE))?;
    let stamp = Stamp {
        bytes,
        crc32c,
        ..pending.stamp
    };
    let text = serde_json::to_vec(&stamp)?;
    let tmp = dir.join(format!("{CACHE_STAMP}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&text)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(CACHE_STAMP))?;
    sync_dir(dir)
}

/// Opens graph `graph`'s engine durably: chooses its base (module docs), opens the engine on it
/// with `config` (given the database file, or `None` in memory), and replays the records after
/// it. `dir` is the graph's local directory, `None` in memory.
///
/// # Errors
///
/// [`GraphError::LogUnavailable`] when the stream cannot be read, or its records continue from
/// no base this node can open; the engine's error when a record does not decode or apply.
pub(crate) fn open(
    graph: GraphId,
    durable: &DurableSpec,
    dir: Option<&Path>,
    config: &dyn Fn(Option<PathBuf>) -> grafeo::Config,
) -> Result<(GrafeoDB, Durable, Recovery), GraphError> {
    let started = Instant::now();
    let bounds = durable.log.bounds(durable.stream)?;
    let file = dir.map(|dir| dir.join(DATABASE_FILE));
    let persistent = dir.is_some();
    let recovered = |source, from_offset, db, state: Durable| {
        let recovery = Recovery {
            source,
            from_offset,
            to_offset: state.position.offset(),
            replayed: state.replayed,
            elapsed: started.elapsed(),
        };
        tracing::info!(%graph, source = source.as_str(), from_offset, to_offset = recovery.to_offset, elapsed_ms = recovery.elapsed.as_millis() as u64, "recovered a graph");
        Ok((db, state, recovery))
    };

    // A graph from before the log, whose stream is still empty, keeps its files (R11.5).
    if durable.adopt && bounds.1 == 0 {
        if let Some(dir) = dir {
            create(dir)?;
        }
        let db = GrafeoDB::with_config(config(file)).map_err(as_engine_error)?;
        let state = Durable::open(&db, durable, graph, bounds, true, None, persistent)?;
        return recovered(RecoverySource::Adopted, 0, db, state);
    }

    // 1. The local cache.
    if let Some(dir) = dir {
        if let Some(base) = take_stamp(dir, graph, durable, bounds) {
            let from = base.offset;
            match attempt(
                graph,
                durable,
                bounds,
                config(file.clone()),
                base,
                persistent,
            ) {
                Ok((db, state)) => return recovered(RecoverySource::LocalCache, from, db, state),
                Err(Failed::Fatal(err)) => return Err(err),
                Err(Failed::Unusable(why)) => {
                    tracing::warn!(%graph, %why, "a graph's local cache did not open; rebuilding it");
                }
            }
        }
        clear(dir)?;
    }

    // 2. The newest usable snapshot.
    let mut rereads = 0;
    loop {
        let candidates = match restore::candidates(durable, graph, bounds, persistent) {
            Ok(candidates) => candidates,
            // An untrimmed stream is the graph on its own.
            Err(err) if bounds.0 == 0 => {
                tracing::warn!(%graph, error = %err, "a graph's manifest could not be read; replaying its whole stream");
                Vec::new()
            }
            Err(err) => return Err(err),
        };
        let mut failed = false;
        for chosen in candidates {
            let kind = chosen.entry.kind;
            let from = chosen.entry.offset;
            if let Some(dir) = dir {
                clear(dir)?;
                create(dir)?;
            }
            match from_snapshot(graph, durable, bounds, &chosen, file.as_deref(), config) {
                Ok((db, state)) => {
                    return recovered(RecoverySource::Snapshot(kind), from, db, state);
                }
                Err(Failed::Fatal(err)) => return Err(err),
                Err(Failed::Unusable(why)) => {
                    tracing::warn!(%graph, kind = kind.as_str(), offset = from, snapshot = %chosen.entry.id, %why, "a graph's snapshot did not open; trying the next");
                    failed = true;
                }
            }
        }
        if !failed || rereads == MANIFEST_REREADS {
            break;
        }
        rereads += 1;
    }

    // 3. The whole stream.
    if bounds.0 > 0 {
        return Err(GraphError::LogUnavailable(format!(
            "the graph's stream starts at offset {}, and no snapshot it continues from could be \
             opened; retry",
            bounds.0
        )));
    }
    if let Some(dir) = dir {
        clear(dir)?;
        create(dir)?;
    }
    let db = GrafeoDB::with_config(config(file)).map_err(as_engine_error)?;
    let state = Durable::open(&db, durable, graph, bounds, false, None, persistent)?;
    recovered(RecoverySource::Log, 0, db, state)
}

/// Why an attempt at a base did not open: the log failed (no other base helps), or the base was
/// unusable (the next one is tried).
enum Failed {
    Fatal(GraphError),
    Unusable(String),
}

impl From<GraphError> for Failed {
    fn from(err: GraphError) -> Self {
        match err {
            GraphError::LogUnavailable(_) => Self::Fatal(err),
            other => Self::Unusable(other.to_string()),
        }
    }
}

/// Opens the engine on `config` (a cache's or a restored native snapshot's file) as `base`, and
/// replays the records after it.
fn attempt(
    graph: GraphId,
    durable: &DurableSpec,
    bounds: (u64, u64),
    config: grafeo::Config,
    base: Base,
    persistent: bool,
) -> Result<(GrafeoDB, Durable), Failed> {
    let db = GrafeoDB::with_config(config).map_err(|err| Failed::Unusable(err.to_string()))?;
    match Durable::open(&db, durable, graph, bounds, false, Some(base), persistent) {
        Ok(state) => Ok((db, state)),
        Err(err) => {
            // Its files are cleared before the next base: close it first (it holds their lock).
            drop(db);
            Err(err.into())
        }
    }
}

/// Opens the engine from snapshot `chosen`: a native one is downloaded as the database file
/// before the engine opens it, a portable one is imported into a fresh engine.
fn from_snapshot(
    graph: GraphId,
    durable: &DurableSpec,
    bounds: (u64, u64),
    chosen: &restore::Chosen,
    file: Option<&Path>,
    config: &dyn Fn(Option<PathBuf>) -> grafeo::Config,
) -> Result<(GrafeoDB, Durable), Failed> {
    let persistent = file.is_some();
    if let Some(file) = file {
        // A download failure is the bucket's (or a sweep's): another snapshot may still open.
        restore::native_file(durable, chosen, file)
            .map_err(|err| Failed::Unusable(err.to_string()))?;
    }
    let db = GrafeoDB::with_config(config(file.map(Path::to_path_buf)))
        .map_err(|err| Failed::Unusable(err.to_string()))?;
    let base = match restore::into_engine(durable, chosen, &db) {
        Ok(base) => base,
        Err(err) => {
            drop(db);
            return Err(Failed::Unusable(err.to_string()));
        }
    };
    match Durable::open(&db, durable, graph, bounds, false, Some(base), persistent) {
        Ok(state) => Ok((db, state)),
        Err(err) => {
            drop(db);
            Err(err.into())
        }
    }
}

/// Reads and consumes `dir`'s stamp, and answers the base its files are, when every check holds
/// (module docs). Whatever it answers, the stamp is gone afterwards.
fn take_stamp(
    dir: &Path,
    graph: GraphId,
    durable: &DurableSpec,
    bounds: (u64, u64),
) -> Option<Base> {
    let path = dir.join(CACHE_STAMP);
    let text = match std::fs::read(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            tracing::warn!(%graph, error = %err, "a graph's cache stamp could not be read; not trusted");
            return None;
        }
    };
    // Consumed before anything opens the files: from now on they stand for nothing until a
    // clean close stamps them again.
    if let Err(err) = std::fs::remove_file(&path).and_then(|()| sync_dir(dir)) {
        tracing::warn!(%graph, error = %err, "a graph's cache stamp could not be removed; not trusted");
        return None;
    }
    let distrust = |why: &str| {
        tracing::info!(%graph, why, "a graph's local cache is not trusted; rebuilding it");
        None
    };
    let Ok(stamp) = serde_json::from_slice::<Stamp>(&text) else {
        return distrust("its stamp does not parse");
    };
    if stamp.format != STAMP_FORMAT {
        return distrust("its stamp's format is unknown");
    }
    if stamp.graph != graph.to_string() || stamp.stream != durable.stream.0 {
        return distrust("its stamp names another graph or stream");
    }
    if stamp.engine_version != crate::ENGINE_VERSION {
        return distrust("another engine version wrote it");
    }
    let (start, end) = bounds;
    if stamp.offset < start || stamp.offset > end {
        return distrust("the stream no longer continues from its offset");
    }
    if sidecar(dir).exists() {
        return distrust("its engine did not close cleanly (a sidecar WAL is there)");
    }
    match checksum(&dir.join(DATABASE_FILE)) {
        Ok((bytes, crc)) if bytes == stamp.bytes && crc == stamp.crc32c => {}
        Ok(_) => return distrust("its database file does not match its stamp"),
        Err(_) => return distrust("its database file cannot be read"),
    }
    Some(Base {
        offset: stamp.offset,
        next_txn_seq: stamp.next_txn_seq,
        keys: Some(
            stamp
                .keys
                .into_iter()
                .map(|key| {
                    (
                        key.key,
                        key.recorded_ms,
                        Outcome {
                            token_offset: key.token_offset,
                            commit_epoch: key.commit_epoch,
                        },
                    )
                })
                .collect(),
        ),
    })
}

/// Grafeo's sidecar WAL directory beside the database file (present while an engine has the file
/// open, and after a crash).
fn sidecar(dir: &Path) -> PathBuf {
    dir.join(format!("{DATABASE_FILE}.wal"))
}

/// A file's size and CRC-32C.
fn checksum(file: &Path) -> std::io::Result<(u64, u32)> {
    let mut reader = std::fs::File::open(file)?;
    let mut buffer = vec![0; 1 << 20];
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

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Removes a graph's local directory (its files, a stale stamp, staging left by a checkpoint).
pub(crate) fn clear(dir: &Path) -> Result<(), GraphError> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(GraphError::engine(format!(
            "clearing the graph's local cache: {err}"
        ))),
    }
}

fn create(dir: &Path) -> Result<(), GraphError> {
    std::fs::create_dir_all(dir)
        .map_err(|err| GraphError::engine(format!("creating the graph's storage: {err}")))?;
    // Staging a checkpoint left behind (it was cut short) is no part of the cache.
    match std::fs::remove_dir_all(dir.join(STAGING)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(GraphError::engine(format!(
            "clearing a checkpoint's staging: {err}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamp_round_trips_and_names_its_fields() {
        let stamp = Stamp {
            format: STAMP_FORMAT,
            graph: GraphId::new().to_string(),
            stream: 7,
            offset: 12,
            next_txn_seq: 13,
            engine_version: crate::ENGINE_VERSION.to_string(),
            bytes: 4096,
            crc32c: 0xDEAD_BEEF,
            keys: vec![StampKey {
                key: "k".to_string(),
                recorded_ms: 5,
                token_offset: 3,
                commit_epoch: 2,
            }],
        };
        let text = serde_json::to_string(&stamp).expect("encode");
        assert!(text.contains("\"offset\":12") && text.contains("\"crc32c\""));
        assert_eq!(serde_json::from_str::<Stamp>(&text).expect("decode"), stamp);
    }
}
