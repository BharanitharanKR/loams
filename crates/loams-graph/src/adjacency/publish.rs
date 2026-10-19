//! Publishing an adjacency index (GR1 Task 22; design §48 §26.3 steps 5 and 6).
//!
//! 1. The file is written once, under its content hash, at
//!    `ns/<ns_id>/graphs/<gr_id>/adj/<N as 20 digits>-<sha256>.adj`, with a create-only PUT. The
//!    name says what the bytes are, so a second PUT of the same name is the same bytes and is
//!    taken as done.
//! 2. The manifest's `adjacency` entry is set by the fenced CAS of `graph-manifest/<gr_id>`
//!    (R13.2), only when the new `covered_offset` is above the current one: an older index never
//!    replaces a newer one, and a build that loses is dropped (its file is left to the sweep).
//! 3. The current entry and the ones before it, `keep` in all, stay named by the manifest, so a
//!    reader that pinned an older one still finds its file. [`super::super::snapshot::ManifestStore::sweep`]
//!    removes every index file no entry names, after the sweep's grace.

use bytes::Bytes;
use loams_common::NamespaceId;
use loams_common::meta::Fence;

use super::format::{AdjacencyIndex, FORMAT_NAME, sha256_hex};
use crate::GraphId;
use crate::snapshot::manifest::{AdjacencyEntry, AdjacencyMode, ManifestStore};
use crate::snapshot::{SnapshotError, graph_prefix, unavailable};

/// The directory of a graph's index files, under its prefix.
pub const ADJ_DIR: &str = "adj/";

/// The object key of the index of graph `graph` covering `covered_offset` with SHA-256 `sha256`.
#[must_use]
pub fn object_key(
    namespace: NamespaceId,
    graph: GraphId,
    covered_offset: u64,
    sha256: &str,
) -> String {
    format!(
        "{}{ADJ_DIR}{covered_offset:020}-{sha256}.adj",
        graph_prefix(namespace, graph)
    )
}

/// What [`publish`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    /// The entry of the index it wrote.
    pub entry: AdjacencyEntry,
    /// Whether the manifest now names it: `false` when the manifest already named an index at
    /// the same or a higher offset, so this one was dropped.
    pub advanced: bool,
}

/// Writes `index` (the canonical bytes `bytes`) and advances graph `graph`'s manifest to it under
/// `fence`, keeping `keep` entries in all.
///
/// # Errors
///
/// [`SnapshotError::Unavailable`] when the bucket or the metastore fails,
/// [`SnapshotError::Fenced`] when the fence's lease moved on (nothing is committed; the file is
/// left to the sweep).
#[allow(clippy::too_many_arguments)]
pub async fn publish(
    manifests: &ManifestStore,
    namespace: NamespaceId,
    graph: GraphId,
    fence: Option<&Fence>,
    index: &AdjacencyIndex,
    bytes: Vec<u8>,
    mode: AdjacencyMode,
    keep: usize,
) -> Result<Published, SnapshotError> {
    let sha256 = sha256_hex(&bytes);
    let key = object_key(namespace, graph, index.covered_offset, &sha256);
    let entry = AdjacencyEntry {
        key: key.clone(),
        covered_offset: index.covered_offset,
        sha256,
        bytes: bytes.len() as u64,
        nodes: index.vertices.len() as u64,
        edges: index.edge_count(),
        format: FORMAT_NAME.to_string(),
        mode,
        created_at_ms: manifests.meta().now_ms(),
    };
    match manifests
        .store()
        .put_if_absent(&key, Bytes::from(bytes))
        .await
    {
        Ok(_) | Err(loams_store::StoreError::AlreadyExists { .. }) => {}
        Err(err) => return Err(unavailable("writing the graph's adjacency index", &err)),
    }
    let keep = keep.max(1);
    let (_, advanced) = manifests
        .update(namespace, graph, fence, |manifest| {
            if let Some(current) = &manifest.adjacency {
                if current.key == entry.key {
                    // A retry after an unknown outcome: already in.
                    return Ok((false, true));
                }
                if current.covered_offset >= entry.covered_offset {
                    return Ok((false, false));
                }
            }
            let mut previous: Vec<AdjacencyEntry> = manifest.adjacency.take().into_iter().collect();
            previous.append(&mut manifest.adjacency_previous);
            previous.truncate(keep - 1);
            manifest.adjacency_previous = previous;
            manifest.adjacency = Some(entry.clone());
            Ok((true, true))
        })
        .await?;
    Ok(Published { entry, advanced })
}

/// Reads the index `entry` names from the bucket and checks it: its SHA-256 against the entry,
/// then its own CRC-32C and canonical form.
///
/// # Errors
///
/// `Ok(Err(decline))` when the file is gone, does not check, or is of a version this build does
/// not read (the caller builds in full); `Err` when the bucket fails.
pub async fn fetch(
    manifests: &ManifestStore,
    entry: &AdjacencyEntry,
) -> Result<Result<(AdjacencyIndex, Vec<u8>), super::Decline>, SnapshotError> {
    let bytes = match manifests.store().get(&entry.key).await {
        Ok((bytes, _)) => bytes.to_vec(),
        // Deleted: the index is derived state, so it is built again.
        Err(loams_store::StoreError::NotFound { .. }) => {
            return Ok(Err(super::Decline::HashMismatch));
        }
        Err(err) => return Err(unavailable("reading the graph's adjacency index", &err)),
    };
    Ok(check(entry, bytes))
}

/// Checks `bytes` against `entry` and decodes them.
///
/// # Errors
///
/// [`super::Decline::UnknownVersion`] for another format, [`super::Decline::HashMismatch`] for
/// bytes that are not the entry's or not canonical.
pub fn check(
    entry: &AdjacencyEntry,
    bytes: Vec<u8>,
) -> Result<(AdjacencyIndex, Vec<u8>), super::Decline> {
    if entry.format != FORMAT_NAME {
        return Err(super::Decline::UnknownVersion);
    }
    if sha256_hex(&bytes) != entry.sha256 {
        return Err(super::Decline::HashMismatch);
    }
    let index = AdjacencyIndex::decode(&bytes)?;
    if index.covered_offset != entry.covered_offset {
        return Err(super::Decline::HashMismatch);
    }
    Ok((index, bytes))
}
