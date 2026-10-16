//! Opening a graph from a snapshot (GR1 Task 13, generalised by GR1 Task 14): the snapshots the
//! manifest names that the stream's records continue from, in the order an open tries them, and
//! the two ways a chosen one becomes the engine's base.
//!
//! A snapshot is usable when its offset is inside the stream (`[start, end]`): every record after
//! it is still readable. A native one is usable only when the graph is on disk and its engine
//! version is this build's; a portable one always is (§48 §6.5, I6). [`crate::recovery`] decides
//! when to open from a snapshot at all (no trusted local cache) and what to do when one fails.

use std::path::Path;

use grafeo::GrafeoDB;

use super::{SnapshotEntry, SnapshotKind, SnapshotSource, native, portable};
use crate::GraphId;
use crate::durable::{Base, DurableSpec};
use crate::engine::GraphError;

/// A snapshot a graph may open from.
#[derive(Debug, Clone)]
pub(crate) struct Chosen {
    pub(crate) entry: SnapshotEntry,
    source: SnapshotSource,
}

/// The snapshots graph `graph` may open from, its stream holding `bounds`, in the order to try
/// them: native ones newest first (when `persistent`, and only those this engine reads), then
/// portable ones newest first. Empty when the graph has no manifest, or no snapshot store is
/// configured.
///
/// # Errors
///
/// [`GraphError::LogUnavailable`] when the manifest cannot be read.
pub(crate) fn candidates(
    durable: &DurableSpec,
    graph: GraphId,
    bounds: (u64, u64),
    persistent: bool,
) -> Result<Vec<Chosen>, GraphError> {
    let (start, end) = bounds;
    let Some(source) = durable.snapshots.clone() else {
        return Ok(Vec::new());
    };
    let Some(loaded) = durable
        .log
        .wait(source.manifests.load(source.namespace, graph))?
    else {
        return Ok(Vec::new());
    };
    let usable = |entry: &&SnapshotEntry| entry.offset >= start && entry.offset <= end;
    let native = loaded
        .manifest
        .snapshots
        .iter()
        .rev()
        .filter(usable)
        .filter(|entry| {
            entry.kind == SnapshotKind::Native
                && persistent
                && entry.engine_version == crate::ENGINE_VERSION
        });
    let portable = loaded
        .manifest
        .snapshots
        .iter()
        .rev()
        .filter(usable)
        .filter(|entry| entry.kind == SnapshotKind::Portable);
    Ok(native
        .chain(portable)
        .map(|entry| Chosen {
            entry: entry.clone(),
            source: source.clone(),
        })
        .collect())
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
        keys: None,
        links: super::manifest::LinkProgress::by_link(&chosen.entry.links),
    })
}
