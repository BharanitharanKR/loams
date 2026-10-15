//! The checkpointer (GR1 Task 13; design §48 §6.4): takes a graph's snapshots on its owner,
//! commits them to the graph's manifest and trims the graph's stream below them.
//!
//! A checkpoint of a graph:
//!
//! 1. **Takes** each snapshot under the graph's gate, held shared: the write lane holds it
//!    exclusively until its writes are durable, so under the shared hold the engine's state is the
//!    durable state (R9.3), and the durable offset read under the same hold is exactly what the
//!    snapshot covers. Client reads go on meanwhile; writes wait for the hold to end. A native
//!    snapshot is Grafeo's `backup_full` into a local staging directory; a portable one is a copy
//!    of every node and edge.
//! 2. **Uploads** each snapshot to `ns/<ns_id>/graphs/<gr_id>/snap/<offset>/<kind>-<ULID>/`, after
//!    the hold. Every attempt has its own prefix, so nothing a manifest names is ever overwritten.
//! 3. **Commits** the new entries to the manifest under the owner lease (the CAS's fence), with
//!    the retention rule applied ([`retain`]): a stale owner commits nothing (I5), and a retry
//!    after an unknown outcome finds its entries already in.
//! 4. **Trims** the graph's stream below the manifest's `log_trim_offset`, under the same fence.
//!    The manifest names a snapshot at or below that offset before the trim happens, and a trim
//!    is idempotent (the log start only moves forward), so a failed trim is retried by the next
//!    checkpoint.
//!
//! The objects of retired snapshots, and of attempts that never committed, are removed later by
//! the maintenance sweep ([`super::ManifestStore::sweep`]), never at once: a reader may still be
//! reading them.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use loams_common::NamespaceId;
use loams_common::meta::{ApplyError, MetaError};

use super::manifest::{GraphManifest, ManifestStore, SnapshotEntry, SnapshotKind, retain};
use super::portable::{GraphRows, Header};
use super::{SnapshotError, graph_prefix, native, portable};
use crate::GraphId;
use crate::durable::faults::KillPoint;
use crate::durable::idempotency::IDEMPOTENCY_WINDOW_MS;
use crate::engine::{Graph, GraphError};

/// When a graph's snapshots are taken and how long its history is kept (§48 §6.4, §14.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPolicy {
    /// A native snapshot is due this long after the newest snapshot, once the graph has changed
    /// since (default 15 minutes).
    pub interval: Duration,
    /// ... or once this much log has been appended since the last snapshot (default 64 MiB).
    pub log_bytes: u64,
    /// A portable snapshot is due this long after the newest portable one (default 7 days), and
    /// at once when a graph that has changed has none.
    pub portable_every: Duration,
    /// The PITR window (default 7 days): the stream is never trimmed inside it. Outside tests it
    /// is at least the idempotency window (24 h), so a graph opened from a snapshot can still
    /// rebuild its idempotency keys from the stream.
    pub pitr_window: Duration,
    /// The most snapshots a manifest keeps (default 48; see [`retain`]).
    pub max_snapshots: usize,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15 * 60),
            log_bytes: 64 << 20,
            portable_every: Duration::from_secs(7 * 24 * 3600),
            pitr_window: Duration::from_secs(7 * 24 * 3600),
            max_snapshots: 48,
        }
    }
}

impl CheckpointPolicy {
    /// The PITR window in force, in milliseconds: [`CheckpointPolicy::pitr_window`], raised to
    /// the idempotency window outside `test-hooks` builds.
    #[must_use]
    pub fn window_ms(&self) -> u64 {
        let window = u64::try_from(self.pitr_window.as_millis()).unwrap_or(u64::MAX);
        if cfg!(feature = "test-hooks") {
            window
        } else {
            window.max(IDEMPOTENCY_WINDOW_MS)
        }
    }
}

/// What a checkpoint did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpointed {
    /// The snapshots it took, now in the manifest.
    pub taken: Vec<SnapshotEntry>,
    /// The manifest it committed.
    pub manifest: GraphManifest,
    /// The stream's first readable offset after its trim; `None` when nothing was trimmed or the
    /// trim failed (the next checkpoint retries it).
    pub log_start: Option<u64>,
}

/// One snapshot taken under the gate, not yet uploaded.
enum Taken {
    Native {
        staging: std::path::PathBuf,
        taken: native::Taken,
    },
    Portable(GraphRows),
}

/// What was read under the gate, besides the snapshots.
struct Point {
    offset: u64,
    epoch: u64,
    next_txn_seq: u64,
    lease_epoch: u64,
    created_at_ms: u64,
    counts: (u64, u64),
    log_bytes: u64,
}

/// The checkpointer of one node's graphs. Cheap to clone.
#[derive(Clone)]
pub struct Checkpointer {
    manifests: ManifestStore,
    policy: CheckpointPolicy,
    /// One checkpoint per graph at a time.
    running: Arc<std::sync::Mutex<HashMap<GraphId, Arc<tokio::sync::Mutex<()>>>>>,
}

impl std::fmt::Debug for Checkpointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Checkpointer")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl Checkpointer {
    /// A checkpointer committing to `manifests` under `policy`.
    #[must_use]
    pub fn new(manifests: ManifestStore, policy: CheckpointPolicy) -> Self {
        Self {
            manifests,
            policy,
            running: Arc::default(),
        }
    }

    /// The manifests it commits to.
    #[must_use]
    pub fn manifests(&self) -> &ManifestStore {
        &self.manifests
    }

    /// Its policy.
    #[must_use]
    pub fn policy(&self) -> CheckpointPolicy {
        self.policy
    }

    /// The snapshot kinds due for `graph` (open here, in `namespace`) under the policy. A graph
    /// that is not durable here, not serving, or unchanged since its newest snapshot has none.
    ///
    /// # Errors
    ///
    /// What [`ManifestStore::load`] answers.
    pub async fn due(
        &self,
        graph: &Graph,
        namespace: NamespaceId,
    ) -> Result<Vec<SnapshotKind>, SnapshotError> {
        let (Some(id), Some(durable)) = (graph.id(), graph.durable_state()) else {
            return Ok(Vec::new());
        };
        let offset = durable.position.offset();
        if offset == 0 || graph.needs_reopen() || graph.is_fenced() {
            return Ok(Vec::new());
        }
        let manifest = self
            .manifests
            .load(namespace, id)
            .await?
            .map(|loaded| loaded.manifest)
            .unwrap_or_else(|| GraphManifest::new(id));
        let now = self.manifests.meta().now_ms();
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let mut due = Vec::new();
        let newest = manifest.newest(None);
        if newest.is_none_or(|entry| entry.offset < offset) {
            let aged = match newest {
                Some(entry) => now.saturating_sub(entry.created_at_ms) >= ms(self.policy.interval),
                None => durable.opened_at.elapsed() >= self.policy.interval,
            };
            if aged || durable.log_bytes.load(Ordering::Acquire) >= self.policy.log_bytes {
                due.push(native_or_portable(graph));
            }
        }
        let portable = manifest.newest(Some(SnapshotKind::Portable));
        let portable_due = match portable {
            None => true,
            Some(entry) => {
                entry.offset < offset
                    && now.saturating_sub(entry.created_at_ms) >= ms(self.policy.portable_every)
            }
        };
        if portable_due && !due.contains(&SnapshotKind::Portable) {
            due.push(SnapshotKind::Portable);
        }
        Ok(due)
    }

    /// Takes the snapshots `kinds` of `graph` (open and owned here, in `namespace`), commits them
    /// to its manifest and trims its stream. A native snapshot of a graph held in memory is
    /// taken as a portable one (Grafeo backs up only a database on disk, R0.12).
    ///
    /// # Errors
    ///
    /// [`SnapshotError::NotServing`] for a graph that is not durable here or is reloading,
    /// [`SnapshotError::Fenced`] when its owner lease moved on (the graph is then fenced here and
    /// nothing was committed), or what taking, uploading or committing answers. Nothing a failed
    /// checkpoint uploaded is ever named by the manifest; the sweep removes it.
    pub async fn checkpoint(
        &self,
        graph: Arc<Graph>,
        namespace: NamespaceId,
        kinds: &[SnapshotKind],
    ) -> Result<Checkpointed, SnapshotError> {
        let (Some(id), true) = (graph.id(), graph.is_durable()) else {
            return Err(SnapshotError::NotServing(
                "the graph is not durable here".to_string(),
            ));
        };
        let lock = {
            let mut running = self
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(running.entry(id).or_default())
        };
        let _one = lock.lock().await;
        let mut wanted: Vec<SnapshotKind> = Vec::new();
        for kind in kinds {
            let kind = match kind {
                SnapshotKind::Native => native_or_portable(&graph),
                SnapshotKind::Portable => SnapshotKind::Portable,
            };
            if !wanted.contains(&kind) {
                wanted.push(kind);
            }
        }
        let (point, taken) = {
            let held = Arc::clone(&graph);
            let wanted = wanted.clone();
            // Under the gate, shared, with the engine's panic containment: a panic poisons the
            // graph, as one in a statement does.
            tokio::task::spawn_blocking(move || held.read_call(|| Ok(take(&held, &wanted))))
                .await
                .map_err(|_| SnapshotError::Engine(GraphError::EnginePanic))???
        };
        reach(&graph, KillPoint::CheckpointUpload).await;
        let entries = self.upload(&graph, namespace, id, &point, taken).await?;
        reach(&graph, KillPoint::ManifestCas).await;
        let fence = graph
            .durable_state()
            .and_then(|durable| durable.fence.clone());
        let window = self.policy.window_ms();
        let max = self.policy.max_snapshots;
        let meta = Arc::clone(self.manifests.meta());
        let committed = self
            .manifests
            .update(namespace, id, fence.as_ref(), |manifest| {
                if entries
                    .iter()
                    .all(|entry| manifest.snapshots.iter().any(|e| e.id == entry.id))
                {
                    return Ok((false, ()));
                }
                let mut snapshots = manifest.snapshots.clone();
                snapshots.extend(entries.iter().cloned());
                let retained = retain(
                    &snapshots,
                    manifest.log_trim_offset,
                    meta.now_ms(),
                    window,
                    max,
                );
                manifest.snapshots = retained.kept;
                manifest.log_trim_offset = retained.trim;
                Ok((true, ()))
            })
            .await;
        let manifest = match committed {
            Ok((manifest, ())) => manifest,
            Err(SnapshotError::Fenced) => {
                tracing::warn!(graph = %id, "a checkpoint was refused: the graph's owner lease moved on; the graph is fenced here");
                graph.mark_fenced();
                return Err(SnapshotError::Fenced);
            }
            Err(err) => return Err(err),
        };
        if let Some(durable) = graph.durable_state() {
            let _ = durable
                .log_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                    Some(bytes.saturating_sub(point.log_bytes))
                });
        }
        reach(&graph, KillPoint::LogTrim).await;
        let log_start = self.trim(&graph, &manifest).await?;
        Ok(Checkpointed {
            taken: entries,
            manifest,
            log_start,
        })
    }

    /// Uploads what was taken and answers its manifest entries.
    async fn upload(
        &self,
        graph: &Graph,
        namespace: NamespaceId,
        id: GraphId,
        point: &Point,
        taken: Vec<Taken>,
    ) -> Result<Vec<SnapshotEntry>, SnapshotError> {
        let store = self.manifests.store();
        let mut entries = Vec::with_capacity(taken.len());
        for snapshot in taken {
            let ulid = ulid::Ulid::generate();
            let kind = match &snapshot {
                Taken::Native { .. } => SnapshotKind::Native,
                Taken::Portable(_) => SnapshotKind::Portable,
            };
            let prefix = format!(
                "{}snap/{:020}/{}-{ulid}/",
                graph_prefix(namespace, id),
                point.offset,
                kind.as_str()
            );
            let mut entry = SnapshotEntry {
                id: ulid.to_string(),
                kind,
                offset: point.offset,
                epoch: point.epoch,
                next_txn_seq: point.next_txn_seq,
                lease_epoch: point.lease_epoch,
                keys: Vec::new(),
                bytes: 0,
                crc32c: None,
                engine_version: crate::ENGINE_VERSION.to_string(),
                created_at_ms: point.created_at_ms,
                nodes: point.counts.0,
                edges: point.counts.1,
            };
            match snapshot {
                Taken::Native { staging, taken } => {
                    let key = format!("{prefix}{}", native::FILE_NAME);
                    let uploaded = native::upload(store, &key, &taken.file).await;
                    remove_staging(&staging);
                    uploaded?;
                    entry.epoch = taken.epoch;
                    entry.keys = vec![key];
                    entry.bytes = taken.bytes;
                    entry.crc32c = Some(taken.crc32c);
                }
                Taken::Portable(rows) => {
                    let header = Header {
                        graph: id.to_string(),
                        offset: point.offset,
                        next_txn_seq: point.next_txn_seq,
                        created_at_ms: point.created_at_ms,
                    };
                    let (keys, bytes) = portable::write(store, &prefix, &rows, &header).await?;
                    entry.keys = keys;
                    entry.bytes = bytes;
                }
            }
            tracing::debug!(graph = %id, kind = kind.as_str(), offset = point.offset, bytes = entry.bytes, storage = ?graph.storage_dir(), "uploaded a graph snapshot");
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Trims `graph`'s stream below `manifest.log_trim_offset`, under its owner lease.
    async fn trim(
        &self,
        graph: &Graph,
        manifest: &GraphManifest,
    ) -> Result<Option<u64>, SnapshotError> {
        let Some(durable) = graph.durable_state() else {
            return Ok(None);
        };
        if manifest.log_trim_offset == 0 {
            return Ok(None);
        }
        match self
            .manifests
            .meta()
            .trim_partition(
                durable.stream,
                0,
                manifest.log_trim_offset,
                durable.fence.clone(),
            )
            .await
        {
            Ok(start) => Ok(Some(start)),
            Err(MetaError::Rejected(ApplyError::Fenced { .. })) => {
                graph.mark_fenced();
                Err(SnapshotError::Fenced)
            }
            Err(err) => {
                tracing::warn!(stream = %durable.stream, error = %err, "trimming a graph's stream failed; the next checkpoint retries");
                Ok(None)
            }
        }
    }
}

/// Reaches kill point `point` on `graph`'s node (GR1 Task 16; nothing without `test-hooks`).
async fn reach(graph: &Graph, point: KillPoint) {
    if let Some(durable) = graph.durable_state() {
        durable.log.reach(point).await;
    }
}

/// A native snapshot when the graph is on disk, a portable one when it is in memory.
fn native_or_portable(graph: &Graph) -> SnapshotKind {
    if graph.storage_dir().is_some() {
        SnapshotKind::Native
    } else {
        SnapshotKind::Portable
    }
}

/// Takes `kinds` of `graph` (step 1). The caller holds the graph's gate shared
/// ([`Graph::read_call`]). Blocks.
fn take(graph: &Graph, kinds: &[SnapshotKind]) -> Result<(Point, Vec<Taken>), SnapshotError> {
    let not_serving =
        || SnapshotError::NotServing("the graph is reloading or no longer owned here".to_string());
    if graph.needs_reopen() || graph.is_fenced() {
        return Err(not_serving());
    }
    let durable = graph.durable_state().ok_or_else(not_serving)?;
    let point = Point {
        offset: durable.position.offset(),
        epoch: durable.position.engine_epoch(),
        next_txn_seq: durable.next_txn_seq.load(Ordering::Acquire),
        lease_epoch: durable.lease_epoch(),
        // Read under the gate: every record below `offset` was stamped (on the same metastore
        // clock) before it, which is what lets the PITR window be checked from snapshots alone.
        created_at_ms: durable.log.now_ms(),
        counts: durable.position.counts(),
        log_bytes: durable.log_bytes.load(Ordering::Acquire),
    };
    let mut taken = Vec::with_capacity(kinds.len());
    for kind in kinds {
        match kind {
            SnapshotKind::Native => {
                let dir = graph.storage_dir().ok_or_else(not_serving)?;
                let staging = dir
                    .join("snapshots")
                    .join(ulid::Ulid::generate().to_string());
                match native::take(graph.db(), &staging) {
                    Ok(native) => taken.push(Taken::Native {
                        staging,
                        taken: native,
                    }),
                    Err(err) => {
                        remove_staging(&staging);
                        for earlier in &taken {
                            if let Taken::Native { staging, .. } = earlier {
                                remove_staging(staging);
                            }
                        }
                        return Err(err);
                    }
                }
            }
            SnapshotKind::Portable => taken.push(Taken::Portable(GraphRows::read(graph.db()))),
        }
    }
    Ok((point, taken))
}

fn remove_staging(staging: &std::path::Path) {
    match std::fs::remove_dir_all(staging) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(dir = %staging.display(), error = %err, "could not remove a snapshot's staging directory");
        }
    }
}
