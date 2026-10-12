//! The graph manifest (GR1 Task 13; design §48 §6.4): which snapshots of a graph are retained,
//! what each covers, and where the graph's stream was trimmed.
//!
//! **Where it lives.** The manifest of a graph is a JSON document in the bucket, written as an
//! immutable object `ns/<ns_id>/graphs/<gr_id>/manifest/<ULID>.json` and committed by a
//! compare-and-swap of the namespace's metastore pointer `graph-manifest/<gr_id>` to that key:
//! the catalog's idiom (R4.1), because a pointer's value holds at most 1 KiB and a manifest lists
//! every retained snapshot with its object keys. The CAS carries the graph's owner lease as its
//! fence, so only the owner commits a snapshot (I5), and a lost acknowledgement is recognised by
//! the pointer already naming the write's own object.
//!
//! **Retention** ([`retain`]) decides which snapshots are kept and where the stream may be trimmed:
//! below the oldest retained snapshot, never inside the PITR window, and never past the newest
//! portable snapshot (the fallback for an engine that cannot read the native one, §48 §6.5).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loams_common::NamespaceId;
use loams_common::meta::{ApplyError, Consistency, Fence, MetaError, MetaStore, PointerCas};
use loams_store::Store;
use serde::{Deserialize, Serialize};

use super::{SnapshotError, graph_prefix, unavailable};
use crate::GraphId;

/// The manifest document's format; a reader refuses another.
pub const MANIFEST_FORMAT: u32 = 1;

/// How many times a manifest write retries a CAS it lost or whose outcome is unknown.
const MAX_ATTEMPTS: usize = 16;

/// The metastore pointer, per namespace, that names graph `graph`'s current manifest.
#[must_use]
pub fn pointer_key(graph: GraphId) -> String {
    format!("graph-manifest/{graph}")
}

/// What a snapshot is made of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    /// Grafeo's own `backup_full` of the checkpointed database: one `.grafeo` file.
    Native,
    /// Parquet files of nodes and edges with the log's typed values, and a `manifest.json` (I6).
    Portable,
}

impl SnapshotKind {
    /// The kind's name in object keys.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Portable => "portable",
        }
    }
}

/// One retained snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    /// Unique per checkpoint attempt (a ULID): how a retried manifest write recognises its own
    /// entry.
    pub id: String,
    /// What the snapshot is made of.
    pub kind: SnapshotKind,
    /// The durable epoch it covers (R9.5): the graph's stream offset after the last record in the
    /// snapshot. The graph is the snapshot plus the records from `offset` on.
    pub offset: u64,
    /// Grafeo's epoch when the snapshot was taken. Informational, like a record's
    /// `commit_epoch`: it does not survive a rebuild (R9.5).
    pub epoch: u64,
    /// The `txn_seq` the next record takes (R9.5: Loams' own per-graph counter).
    pub next_txn_seq: u64,
    /// The owner lease epoch it was taken under.
    pub lease_epoch: u64,
    /// The bucket objects it is made of.
    pub keys: Vec<String>,
    /// Their size, in bytes.
    pub bytes: u64,
    /// The CRC-32C of a native snapshot's file, checked when it is read back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<u32>,
    /// The engine that wrote it (`grafeo::VERSION`): a native snapshot is read only by an engine
    /// that reads that version's file format (§48 §6.5).
    pub engine_version: String,
    /// When it was taken, on the metastore clock: every record below `offset` is older.
    pub created_at_ms: u64,
    /// Nodes in the snapshot.
    pub nodes: u64,
    /// Edges in the snapshot.
    pub edges: u64,
}

/// A graph's manifest (shared contract `GraphManifest`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphManifest {
    /// [`MANIFEST_FORMAT`].
    pub format: u32,
    /// The graph, `gr_<ULID>`.
    pub graph: String,
    /// The retained snapshots, oldest first (by offset, then time, then kind).
    pub snapshots: Vec<SnapshotEntry>,
    /// The graph's stream is trimmed below this offset: every offset below it is unreadable.
    pub log_trim_offset: u64,
}

impl GraphManifest {
    /// An empty manifest of `graph`.
    #[must_use]
    pub fn new(graph: GraphId) -> Self {
        Self {
            format: MANIFEST_FORMAT,
            graph: graph.to_string(),
            snapshots: Vec::new(),
            log_trim_offset: 0,
        }
    }

    /// The newest retained snapshot, of `kind` when given.
    #[must_use]
    pub fn newest(&self, kind: Option<SnapshotKind>) -> Option<&SnapshotEntry> {
        self.snapshots
            .iter()
            .rev()
            .find(|entry| kind.is_none_or(|kind| entry.kind == kind))
    }

    /// Every object a retained snapshot is made of.
    #[must_use]
    pub fn keys(&self) -> BTreeSet<&str> {
        self.snapshots
            .iter()
            .flat_map(|entry| entry.keys.iter().map(String::as_str))
            .collect()
    }

    fn sort(&mut self) {
        self.snapshots.sort_by(order);
    }
}

/// The manifest's order: by offset, then time, then kind (native first), then id.
fn order(a: &SnapshotEntry, b: &SnapshotEntry) -> std::cmp::Ordering {
    (a.offset, a.created_at_ms, a.kind, &a.id).cmp(&(b.offset, b.created_at_ms, b.kind, &b.id))
}

/// What [`retain`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// The snapshots kept, oldest first.
    pub kept: Vec<SnapshotEntry>,
    /// The snapshots dropped from the manifest: their objects go to the sweep.
    pub retired: Vec<SnapshotEntry>,
    /// The graph's stream may be trimmed below this offset.
    pub trim: u64,
}

/// The retention rule (§48 §6.4, §14.1): which snapshots are kept and where the stream may be
/// trimmed, at `now_ms` with a PITR window of `window_ms` and at most `max_snapshots` kept.
///
/// * The **base** is the newest snapshot taken at least `window_ms` ago. Every record below its
///   offset is older than the window, so trimming there never cuts into it, and a restore to any
///   time inside the window starts from the base or a newer snapshot.
/// * The trim point is the base's offset, or the newest portable snapshot's when that is lower
///   (a graph whose engine cannot read its native snapshots rebuilds from the portable one and
///   the records after it). With no base or no portable snapshot the trim point stays at
///   `previous_trim`. It never moves back.
/// * Every snapshot at or above the trim point is kept, so the stream is never trimmed past the
///   oldest retained snapshot. When that is more than `max_snapshots`, the oldest are dropped,
///   except the one at the trim point, the newest portable and the newest snapshot: a restore
///   then replays more of the log, but it still finds a snapshot below any time in the window.
#[must_use]
pub fn retain(
    snapshots: &[SnapshotEntry],
    previous_trim: u64,
    now_ms: u64,
    window_ms: u64,
    max_snapshots: usize,
) -> Retained {
    let mut sorted = snapshots.to_vec();
    sorted.sort_by(order);
    let base = sorted
        .iter()
        .filter(|entry| entry.created_at_ms.saturating_add(window_ms) <= now_ms)
        .map(|entry| entry.offset)
        .max();
    let portable = sorted
        .iter()
        .rev()
        .find(|entry| entry.kind == SnapshotKind::Portable)
        .map(|entry| entry.offset);
    let trim = match (base, portable) {
        (Some(base), Some(portable)) => previous_trim.max(base.min(portable)),
        _ => previous_trim,
    };
    let (mut kept, mut retired): (Vec<_>, Vec<_>) =
        sorted.into_iter().partition(|entry| entry.offset >= trim);
    if kept.len() > max_snapshots.max(1) {
        let protected: BTreeSet<String> = [
            kept.iter().find(|entry| entry.offset == trim),
            kept.iter().rev().find(|e| e.kind == SnapshotKind::Portable),
            kept.last(),
        ]
        .into_iter()
        .flatten()
        .map(|entry| entry.id.clone())
        .collect();
        let mut excess = kept.len() - max_snapshots.max(protected.len());
        let mut thinned = Vec::with_capacity(kept.len());
        for entry in kept {
            if excess > 0 && !protected.contains(&entry.id) {
                excess -= 1;
                retired.push(entry);
            } else {
                thinned.push(entry);
            }
        }
        kept = thinned;
    }
    Retained {
        kept,
        retired,
        trim,
    }
}

/// A loaded manifest and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loaded {
    /// The pointer's version.
    pub version: u64,
    /// The document's object key.
    pub key: String,
    /// The manifest.
    pub manifest: GraphManifest,
}

/// What the manifest store counts, for tests and (Task 27) metrics.
#[derive(Debug, Default)]
pub struct ManifestCounters {
    /// Writes retried after losing a CAS or an unknown outcome.
    pub cas_retries: std::sync::atomic::AtomicU64,
    /// Manifest documents fetched from the bucket (a load whose pointer version was not cached).
    pub document_gets: std::sync::atomic::AtomicU64,
}

/// Where a graph's manifest is read and written: the metastore's pointers and the bucket. Cheap
/// to clone.
#[derive(Clone)]
pub struct ManifestStore {
    meta: Arc<dyn MetaStore>,
    store: Store,
    counters: Arc<ManifestCounters>,
    /// The last manifest read per graph, keyed by the pointer version it was read at.
    cache: Arc<std::sync::Mutex<HashMap<GraphId, Loaded>>>,
    #[cfg(feature = "test-hooks")]
    hooks: Arc<std::sync::Mutex<Hooks>>,
}

#[cfg(feature = "test-hooks")]
#[derive(Default)]
struct Hooks {
    before_cas: Option<crate::catalog::AckHook>,
    after_cas: Option<crate::catalog::AckHook>,
}

impl std::fmt::Debug for ManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestStore").finish_non_exhaustive()
    }
}

/// Where a graph's snapshots are read from when it opens (GR1 Task 13): its manifest store and
/// its namespace.
#[derive(Debug, Clone)]
pub struct SnapshotSource {
    /// The manifests.
    pub manifests: ManifestStore,
    /// The graph's namespace.
    pub namespace: NamespaceId,
}

impl ManifestStore {
    /// Manifests over `meta` and `store`.
    #[must_use]
    pub fn new(meta: Arc<dyn MetaStore>, store: Store) -> Self {
        Self {
            meta,
            store,
            counters: Arc::default(),
            cache: Arc::default(),
            #[cfg(feature = "test-hooks")]
            hooks: Arc::default(),
        }
    }

    /// **Tests only** (feature `test-hooks`): `before` runs between a write's read of the manifest
    /// and its CAS (a concurrent writer can be injected there); `after` runs after a committed CAS,
    /// and answering `true` makes the write behave as if its acknowledgement were lost.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_hooks(
        &self,
        before: Option<crate::catalog::AckHook>,
        after: Option<crate::catalog::AckHook>,
    ) {
        if let Ok(mut hooks) = self.hooks.lock() {
            hooks.before_cas = before;
            hooks.after_cas = after;
        }
    }

    #[cfg(feature = "test-hooks")]
    async fn hook(&self, after: bool) -> bool {
        let hook = self.hooks.lock().ok().and_then(|hooks| match after {
            false => hooks.before_cas.clone(),
            true => hooks.after_cas.clone(),
        });
        match hook {
            Some(hook) => hook().await,
            None => false,
        }
    }

    /// The bucket.
    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The metastore.
    #[must_use]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta
    }

    /// The counters.
    #[must_use]
    pub fn counters(&self) -> &ManifestCounters {
        &self.counters
    }

    /// Graph `graph`'s current manifest, or `None` when it has none yet.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Unavailable`], or [`SnapshotError::Corrupt`] when the document does not
    /// parse or the pointer names a document that is gone.
    pub async fn load(
        &self,
        namespace: NamespaceId,
        graph: GraphId,
    ) -> Result<Option<Loaded>, SnapshotError> {
        let key = pointer_key(graph);
        let mut pointer = match self
            .meta
            .pointer(Consistency::Linearizable, namespace, &key)
            .await
        {
            Ok(Some(pointer)) => pointer,
            Ok(None) => return Ok(None),
            Err(err) => return Err(unavailable("reading the graph's manifest pointer", &err)),
        };
        if let Some(cached) = self.cached(graph, pointer.version) {
            return Ok(Some(cached));
        }
        for _ in 0..MAX_ATTEMPTS {
            self.counters
                .document_gets
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match self.store.get(&pointer.value).await {
                Ok((bytes, _)) => {
                    let manifest: GraphManifest =
                        serde_json::from_slice(&bytes).map_err(|err| {
                            SnapshotError::Corrupt(format!(
                                "graph manifest {}: {err}",
                                pointer.value
                            ))
                        })?;
                    if manifest.format != MANIFEST_FORMAT || manifest.graph != graph.to_string() {
                        return Err(SnapshotError::Corrupt(format!(
                            "graph manifest {} is format {} of {}, not format {MANIFEST_FORMAT} \
                             of {graph}",
                            pointer.value, manifest.format, manifest.graph
                        )));
                    }
                    let loaded = Loaded {
                        version: pointer.version,
                        key: pointer.value,
                        manifest,
                    };
                    if let Ok(mut cache) = self.cache.lock() {
                        cache.insert(graph, loaded.clone());
                    }
                    return Ok(Some(loaded));
                }
                // Superseded and swept between the pointer read and the GET: follow the pointer
                // again, as the catalog does.
                Err(loams_store::StoreError::NotFound { .. }) => {
                    match self
                        .meta
                        .pointer(Consistency::Linearizable, namespace, &key)
                        .await
                    {
                        Ok(Some(next)) if next.value != pointer.value => pointer = next,
                        Ok(_) => {
                            return Err(SnapshotError::Corrupt(format!(
                                "the current graph manifest {} is missing",
                                pointer.value
                            )));
                        }
                        Err(err) => {
                            return Err(unavailable("reading the graph's manifest pointer", &err));
                        }
                    }
                }
                Err(err) => return Err(unavailable("reading the graph's manifest", &err)),
            }
        }
        Err(SnapshotError::Unavailable(
            "the graph's manifest kept moving under this read; retry".to_string(),
        ))
    }

    fn cached(&self, graph: GraphId, version: u64) -> Option<Loaded> {
        let cache = self.cache.lock().ok()?;
        cache
            .get(&graph)
            .filter(|loaded| loaded.version == version)
            .cloned()
    }

    /// Reads graph `graph`'s manifest (an empty one when it has none), applies `change` and
    /// commits it under `fence`. `change` answers `(false, out)` to answer without writing, which
    /// is how a retry after an unknown outcome recognises that its change is already in.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Fenced`] when the fence's lease moved on (nothing is committed),
    /// [`SnapshotError::Unavailable`], or what `change` or [`ManifestStore::load`] answers.
    pub async fn update<T>(
        &self,
        namespace: NamespaceId,
        graph: GraphId,
        fence: Option<&Fence>,
        change: impl Fn(&mut GraphManifest) -> Result<(bool, T), SnapshotError>,
    ) -> Result<(GraphManifest, T), SnapshotError> {
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                self.counters
                    .cas_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(backoff(attempt)).await;
            }
            let loaded = self.load(namespace, graph).await?;
            let (expected, mut manifest) = match loaded {
                Some(loaded) => (Some(loaded.version), loaded.manifest),
                None => (None, GraphManifest::new(graph)),
            };
            let (write, out) = change(&mut manifest)?;
            if !write {
                return Ok((manifest, out));
            }
            manifest.sort();
            let object = format!(
                "{}manifest/{}.json",
                graph_prefix(namespace, graph),
                ulid::Ulid::generate()
            );
            let bytes = serde_json::to_vec(&manifest)
                .map_err(|err| SnapshotError::Corrupt(err.to_string()))?;
            self.store
                .put(&object, Bytes::from(bytes))
                .await
                .map_err(|err| unavailable("writing the graph's manifest", &err))?;
            #[cfg(feature = "test-hooks")]
            self.hook(false).await;
            let cas = self
                .meta
                .cas_pointer(PointerCas {
                    namespace,
                    key: pointer_key(graph),
                    expected,
                    value: object.clone(),
                    fence: fence.cloned(),
                    fresh: None,
                })
                .await;
            match cas.result {
                Ok(version) => {
                    #[cfg(feature = "test-hooks")]
                    if self.hook(true).await {
                        // Behave as if the acknowledgement were lost: try again.
                        continue;
                    }
                    self.remember(graph, version, object, &manifest);
                    return Ok((manifest, out));
                }
                // A lost acknowledgement of this very write: the pointer names its object.
                Err(MetaError::Rejected(ApplyError::VersionMismatch {
                    current: Some(current),
                })) if current.value == object => {
                    self.remember(graph, current.version, object, &manifest);
                    return Ok((manifest, out));
                }
                // Another writer won: redo the change on its manifest. This attempt's document is
                // left for the sweep.
                Err(MetaError::Rejected(ApplyError::VersionMismatch { .. })) => {}
                Err(MetaError::Rejected(ApplyError::Fenced { .. })) => {
                    return Err(SnapshotError::Fenced);
                }
                // Unknown outcome: retry, and `change` recognises its own effect.
                Err(
                    MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_),
                ) => {}
                Err(err) => return Err(unavailable("committing the graph's manifest", &err)),
            }
        }
        Err(SnapshotError::Unavailable(
            "the graph's manifest kept changing under this write; retry".to_string(),
        ))
    }

    fn remember(&self, graph: GraphId, version: u64, key: String, manifest: &GraphManifest) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(
                graph,
                Loaded {
                    version,
                    key,
                    manifest: manifest.clone(),
                },
            );
        }
    }

    /// Deletes graph `graph`'s objects no reader can still need: every snapshot object no
    /// retained snapshot names, and every manifest document older than the current one by
    /// upload order, each only when written at least `grace` before the current manifest
    /// document (bucket timestamps, never this node's clock). A checkpoint in flight wrote its
    /// objects after the current document, so they are never deleted; the objects of a
    /// checkpoint refused by its fence, and retired snapshots, are. Answers how many went.
    ///
    /// A reader that read the manifest before a sweep and finds a snapshot gone reads the
    /// manifest again (GR1 Tasks 14 and 29).
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Unavailable`] when the bucket cannot be listed or the manifest read.
    pub async fn sweep(
        &self,
        namespace: NamespaceId,
        graph: GraphId,
        grace: Duration,
    ) -> Result<usize, SnapshotError> {
        let prefix = graph_prefix(namespace, graph);
        let objects = self
            .store
            .list(&prefix)
            .await
            .map_err(|err| unavailable("listing the graph's snapshots", &err))?;
        // The manifest after the listing: an object written after this read is not in the list.
        let Some(current) = self.load(namespace, graph).await? else {
            return Ok(0);
        };
        let Some(current_written) = objects
            .iter()
            .find(|object| object.path == current.key)
            .map(|object| object.last_modified_ms)
        else {
            return Ok(0);
        };
        let manifests = format!("{prefix}manifest/");
        let snapshots = format!("{prefix}snap/");
        let ulid_of = |path: &str| {
            path.strip_prefix(&manifests)
                .and_then(|name| name.strip_suffix(".json"))
                .and_then(|id| id.parse::<ulid::Ulid>().ok())
        };
        let current_ulid = ulid_of(&current.key);
        let referenced = current.manifest.keys();
        let grace_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
        let mut deleted = 0;
        for object in &objects {
            let aged = object.last_modified_ms.saturating_add(grace_ms) <= current_written;
            let stale = if object.path.starts_with(&manifests) {
                object.path != current.key
                    && matches!((ulid_of(&object.path), current_ulid), (Some(a), Some(b)) if a < b)
            } else {
                object.path.starts_with(&snapshots) && !referenced.contains(object.path.as_str())
            };
            if !aged || !stale {
                continue;
            }
            match self.store.delete(&object.path).await {
                Ok(()) => deleted += 1,
                Err(err) => {
                    tracing::debug!(object = %object.path, error = %err, "could not sweep a graph snapshot object");
                }
            }
        }
        Ok(deleted)
    }

    /// Deletes every object of graph `graph` (a purged graph's snapshots and manifests). Its
    /// manifest pointer stays: the metastore cannot delete a pointer, and a graph id is never
    /// reused. Answers how many objects went.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::Unavailable`] when the bucket cannot be listed or an object not deleted,
    /// so the purge is retried.
    pub async fn purge(
        &self,
        namespace: NamespaceId,
        graph: GraphId,
    ) -> Result<usize, SnapshotError> {
        let prefix = graph_prefix(namespace, graph);
        let objects = self
            .store
            .list(&prefix)
            .await
            .map_err(|err| unavailable("listing a purged graph's objects", &err))?;
        for object in &objects {
            self.store
                .delete(&object.path)
                .await
                .map_err(|err| unavailable("deleting a purged graph's objects", &err))?;
        }
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(&graph);
        }
        Ok(objects.len())
    }
}

/// Jittered exponential backoff before retry `attempt`, as the catalog's (Task 4 review M4).
fn backoff(attempt: usize) -> Duration {
    let base_ms = (5u64 << attempt.min(6)).min(200);
    let random = (u128::from(ulid::Ulid::generate()) & 0xffff) as u64;
    Duration::from_micros(base_ms * 1000 / 2 + base_ms * 1000 * random / 2 / 0x1_0000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, kind: SnapshotKind, offset: u64, created_at_ms: u64) -> SnapshotEntry {
        SnapshotEntry {
            id: id.to_string(),
            kind,
            offset,
            epoch: 0,
            next_txn_seq: 1,
            lease_epoch: 1,
            keys: vec![format!("snap/{offset}/{id}")],
            bytes: 1,
            crc32c: None,
            engine_version: crate::ENGINE_VERSION.to_string(),
            created_at_ms,
            nodes: 0,
            edges: 0,
        }
    }

    #[test]
    fn thinning_keeps_the_trim_point_the_newest_portable_and_the_newest() {
        use SnapshotKind::{Native, Portable};
        let snapshots = vec![
            entry("a", Portable, 10, 100),
            entry("b", Native, 20, 200),
            entry("c", Native, 30, 300),
            entry("d", Native, 40, 400),
            entry("e", Native, 50, 500),
        ];
        // Window 1000 at 1100: only "a" is outside it, so it is the base and the trim point.
        let retained = retain(&snapshots, 0, 1100, 1000, 3);
        assert_eq!(retained.trim, 10);
        let kept: Vec<&str> = retained.kept.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(kept, ["a", "d", "e"]);
        let retired: Vec<&str> = retained.retired.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(retired, ["b", "c"]);
    }

    #[test]
    fn without_a_portable_snapshot_nothing_is_trimmed() {
        let snapshots = vec![entry("a", SnapshotKind::Native, 10, 0)];
        let retained = retain(&snapshots, 0, 10_000, 1, 8);
        assert_eq!(retained.trim, 0);
        assert_eq!(retained.kept.len(), 1);
    }
}
