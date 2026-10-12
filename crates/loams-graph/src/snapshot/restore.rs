//! Opening a graph whose stream was trimmed (GR1 Task 13): from the newest usable snapshot, then
//! the records after it.
//!
//! Once the checkpointer trims a graph's stream, the records below the trim point are gone, so
//! the graph can no longer be rebuilt from its stream alone (R11.4). It opens from a snapshot the
//! manifest names at or above the stream's first offset: a native one when the graph is on disk
//! and the snapshot's engine version is this build's, a portable one otherwise. The general open
//! from snapshots (every open, local cache reuse, eviction) is GR1 Task 14's.

use std::path::Path;

use grafeo::GrafeoDB;

use super::{SnapshotEntry, SnapshotKind, SnapshotSource, native, portable};
use crate::GraphId;
use crate::durable::{Base, DurableSpec};
use crate::engine::GraphError;

/// The snapshot a trimmed graph opens from.
#[derive(Debug, Clone)]
pub(crate) struct Chosen {
    entry: SnapshotEntry,
    source: SnapshotSource,
}

/// Chooses the snapshot graph `graph` opens from, its stream holding `bounds` (`start > 0`).
///
/// # Errors
///
/// [`GraphError::LogUnavailable`] when the manifest cannot be read, or names no snapshot the
/// stream's records continue from.
pub(crate) fn choose(
    durable: &DurableSpec,
    graph: GraphId,
    bounds: (u64, u64),
    persistent: bool,
) -> Result<Chosen, GraphError> {
    let (start, end) = bounds;
    let source = durable.snapshots.clone().ok_or_else(|| {
        GraphError::LogUnavailable(format!(
            "the graph's stream starts at offset {start}, and no snapshot store is configured"
        ))
    })?;
    let manifest = durable
        .log
        .wait(source.manifests.load(source.namespace, graph))?
        .ok_or_else(|| {
            GraphError::LogUnavailable(format!(
                "the graph's stream starts at offset {start}, and it has no manifest"
            ))
        })?
        .manifest;
    let usable = |entry: &&SnapshotEntry| entry.offset >= start && entry.offset <= end;
    let native = manifest
        .snapshots
        .iter()
        .rev()
        .filter(usable)
        .find(|entry| {
            entry.kind == SnapshotKind::Native
                && persistent
                && entry.engine_version == crate::ENGINE_VERSION
        });
    let entry = native
        .or_else(|| {
            manifest
                .snapshots
                .iter()
                .rev()
                .filter(usable)
                .find(|entry| entry.kind == SnapshotKind::Portable)
        })
        .cloned()
        .ok_or_else(|| {
            GraphError::LogUnavailable(format!(
                "the graph's stream starts at offset {start}, and its manifest names no snapshot \
                 this node can open from"
            ))
        })?;
    tracing::info!(%graph, kind = entry.kind.as_str(), offset = entry.offset, "opening a trimmed graph from its snapshot");
    Ok(Chosen { entry, source })
}

/// Downloads a native snapshot to `file`, the graph's database file, before the engine opens
/// it. Does nothing for a portable one.
///
/// # Errors
///
/// [`GraphError::LogUnavailable`] when the download fails or does not match its entry.
pub(crate) fn native_file(
    durable: &DurableSpec,
    chosen: &Chosen,
    file: &Path,
) -> Result<(), GraphError> {
    if chosen.entry.kind != SnapshotKind::Native {
        return Ok(());
    }
    durable
        .log
        .wait(native::restore(
            chosen.source.manifests.store(),
            &chosen.entry,
            file,
        ))
        .map_err(|err| GraphError::LogUnavailable(err.to_string()))
}

/// Loads a portable snapshot into `db`, a fresh engine (a native one is already its file), and
/// answers where the stream's records continue.
///
/// # Errors
///
/// [`GraphError::LogUnavailable`] when the snapshot cannot be read, or the engine's error when
/// its rows do not apply.
pub(crate) fn into_engine(
    durable: &DurableSpec,
    chosen: &Chosen,
    db: &GrafeoDB,
) -> Result<Base, GraphError> {
    if chosen.entry.kind == SnapshotKind::Portable {
        let (_, rows) = durable
            .log
            .wait(portable::read(
                chosen.source.manifests.store(),
                &chosen.entry,
            ))
            .map_err(|err| GraphError::LogUnavailable(err.to_string()))?;
        rows.import(db)?;
    }
    Ok(Base {
        offset: chosen.entry.offset,
        next_txn_seq: chosen.entry.next_txn_seq,
    })
}
