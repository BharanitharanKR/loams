//! Snapshots in the bucket and the graph manifest (GR1 Task 13; design §48 §6.4, §14.2; D742).
//!
//! A graph is its stream (I4). A snapshot is a copy of the graph at one durable position, so a
//! graph can be rebuilt from the snapshot and the records after it instead of from its whole
//! stream, and the stream can be trimmed below it:
//!
//! * [`native`]: Grafeo's own `backup_full` of the checkpointed database, one `.grafeo` file.
//!   Fast to open, readable only by an engine that reads that file format.
//! * [`portable`]: Parquet files of nodes and edges with the typed values of the log's records,
//!   plus a `manifest.json`. Engine-neutral (I6): the escape hatch for engine upgrades and the
//!   export format (§14.3).
//! * [`manifest`]: [`GraphManifest`], the list of a graph's retained snapshots and where its log
//!   was trimmed, committed by a compare-and-swap of a metastore pointer under the graph's owner
//!   lease, and the retention rule ([`retain`]) that decides what is kept and where the log may
//!   be trimmed.
//! * [`checkpoint`]: [`Checkpointer`], which takes snapshots on the owner (every 15 minutes or
//!   64 MiB of log, a portable one weekly, and on demand), commits them to the manifest and trims
//!   the log.
//!
//! Every object of a graph lives under [`graph_prefix`], `ns/<ns_id>/graphs/<gr_id>/`. A
//! snapshot's objects are under `snap/<offset>/<kind>-<ULID>/`: the offset is the durable epoch it
//! covers (R9.5: the durable epoch is a log offset, not Grafeo's epoch), and the ULID makes every
//! attempt's objects its own, so a retry or a lost acknowledgement never overwrites an object a
//! manifest names.

pub mod checkpoint;
pub mod manifest;
pub mod native;
pub mod portable;
pub(crate) mod restore;

use loams_common::NamespaceId;

pub use checkpoint::{CheckpointPolicy, Checkpointed, Checkpointer};
pub use manifest::{
    GraphManifest, ManifestStore, Retained, SnapshotEntry, SnapshotKind, SnapshotSource, retain,
};

use crate::GraphId;
use crate::engine::GraphError;

/// The bucket prefix of every object of graph `graph` in namespace `namespace`:
/// `ns/<ns_id>/graphs/<gr_id>/`.
#[must_use]
pub fn graph_prefix(namespace: NamespaceId, graph: GraphId) -> String {
    format!("ns/{namespace}/graphs/{graph}/")
}

/// Why a snapshot could not be taken, committed, read or trimmed to.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// The bucket or the metastore failed, or the manifest kept moving; retry.
    #[error("{0}")]
    Unavailable(String),
    /// The graph's owner lease moved on: this node may no longer write its manifest or trim its
    /// stream (I5). Nothing was changed.
    #[error("the graph's owner lease moved on; the snapshot was not committed")]
    Fenced,
    /// A manifest or a snapshot that does not read back as written.
    #[error("{0}")]
    Corrupt(String),
    /// The graph is not durable here, or not serving (reloading, fenced).
    #[error("{0}")]
    NotServing(String),
    /// The engine failed while taking or loading the snapshot.
    #[error(transparent)]
    Engine(#[from] GraphError),
}

impl From<SnapshotError> for GraphError {
    fn from(err: SnapshotError) -> Self {
        match err {
            SnapshotError::Unavailable(message) => Self::LogUnavailable(message),
            SnapshotError::Fenced => Self::OwnerMoved(
                "the graph's owner lease moved on while it was being snapshotted; retry"
                    .to_string(),
            ),
            SnapshotError::Corrupt(message) => Self::engine(message),
            SnapshotError::NotServing(_) => Self::Reloading,
            SnapshotError::Engine(err) => err,
        }
    }
}

/// The bucket failed: the detail goes to the server log, the caller gets a generic message.
pub(crate) fn unavailable(what: &str, err: &dyn std::fmt::Display) -> SnapshotError {
    tracing::warn!(error = %err, "{what} failed");
    SnapshotError::Unavailable(format!("{what} failed; retry"))
}
