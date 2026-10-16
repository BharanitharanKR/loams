//! The change-log adjacency index (GR1 Task 22; design §48 §26.3; D1063, D1064; R22.4).
//!
//! The graph algorithms (`pagerank`, `wcc`, `leiden`; [`crate::retrieval::algos`]) run over a
//! CSR/CSC of the graph at one offset of its stream. That structure is not a scan of the engine
//! per call: it is an **index built from the graph's own change sets** (`_graph.<gr_id>`),
//! published to the bucket and named by the graph manifest, and kept fresh incrementally.
//!
//! * [`format`]: `loams.graph.adjacency` v1, the canonical bytes, so one graph state is one hash
//!   however it was built.
//! * [`build`]: the full build (an engine scan under the shared gate) and the patch (the
//!   previous index plus the stream's records `[N, M)`), with [`Decline`] as control flow.
//! * [`publish`]: the create-only PUT, the manifest CAS that only moves forward, retention.
//! * [`overlay`]: a reader's [`PinnedAdjacency`]: the index plus the tail up to the pinned offset
//!   `M`, or the canonical fallback (a full build at `M`) when the tail is trimmed or long.
//!
//! The index is derived state: I4 holds without it, and deleting it loses nothing. A graph gets
//! one the first time an algorithm runs on it ([`Indexer::pin`]); after that the checkpointer's
//! maintenance pass ([`Indexer::maintain`]) keeps it within [`AdjacencyConfig::rebuild_after_records`]
//! and [`AdjacencyConfig::interval`]. It runs on the owner, off the write lane; a new owner uses
//! the published index and rebuilds nothing.

pub mod build;
pub mod format;
pub mod overlay;
pub mod publish;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use loams_common::NamespaceId;

pub use build::{Decline, Patched, Tail, build_full, patch};
pub use format::{AdjacencyIndex, Topology, VertexKey};
pub use overlay::{PinSource, PinnedAdjacency};

use crate::GraphId;
use crate::engine::{Graph, GraphError};
use crate::snapshot::SnapshotError;
use crate::snapshot::manifest::{AdjacencyEntry, AdjacencyMode, ManifestStore};

/// When the index is patched, overlaid or rebuilt (§48 §26.3).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdjacencyConfig {
    /// The longest tail an incremental build patches (default 100 000 records).
    pub incremental_max_records: u64,
    /// The most topology operations a patch takes, as a fraction of the index's edges (default
    /// 0.25).
    pub incremental_max_fraction: f64,
    /// The longest tail a reader overlays; above it the reader builds in full (default 50 000).
    pub overlay_max_records: u64,
    /// The maintenance pass rebuilds an index this many records behind (default 10 000) ...
    pub rebuild_after_records: u64,
    /// ... or this long after it was published, once the graph has changed (default 15 min).
    pub interval: Duration,
    /// Index entries the manifest keeps, the current one included (default 2).
    pub keep: usize,
}

impl Default for AdjacencyConfig {
    fn default() -> Self {
        Self {
            incremental_max_records: 100_000,
            incremental_max_fraction: 0.25,
            overlay_max_records: 50_000,
            rebuild_after_records: 10_000,
            interval: Duration::from_secs(15 * 60),
            keep: 2,
        }
    }
}

/// How a build went (shared contract `BuildOutcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    /// The index already covered the durable offset: nothing was built.
    Unchanged,
    /// The previous index patched with the tail.
    Incremental,
    /// A scan of the engine.
    Full,
}

impl BuildMode {
    /// The mode's name, as the counters label it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Incremental => "incremental",
            Self::Full => "full",
        }
    }
}

/// What [`Indexer::run`] did (shared contract `BuildOutcome`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOutcome {
    /// How.
    pub mode: BuildMode,
    /// Why a build was full.
    pub reason: Option<Decline>,
    /// The offset the graph's index now covers.
    pub covered_offset: u64,
    /// Edge operations applied from the tail.
    pub delta_edges: u64,
    /// Edges read from the engine.
    pub scanned_edges: u64,
    /// Whether the manifest now names the index built (`false` when a newer one was already
    /// published, or nothing was built).
    pub published: bool,
}

/// The adjacency index's counters (§48 §26.3), exported by Task 27 as the
/// `loams_graph_adjacency_*` families.
#[derive(Debug, Default)]
pub struct AdjacencyStats {
    /// `loams_graph_adjacency_builds_total{mode, reason}`.
    builds: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    /// `loams_graph_adjacency_delta_edges_total`.
    delta_edges: AtomicU64,
    /// `loams_graph_adjacency_scanned_edges_total`.
    scanned_edges: AtomicU64,
    /// `loams_graph_adjacency_overlay_records`, a histogram: every observation.
    overlay_records: Mutex<Vec<u64>>,
    /// `loams_graph_adjacency_lag_records`, per graph.
    lag: Mutex<BTreeMap<GraphId, u64>>,
    /// `loams_graph_adjacency_fallbacks_total{reason}`.
    fallbacks: Mutex<BTreeMap<&'static str, u64>>,
}

/// A copy of [`AdjacencyStats`] at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdjacencySnapshot {
    /// Builds by `(mode, reason)`; an incremental build's reason is `""`.
    pub builds: BTreeMap<(String, String), u64>,
    /// Edge operations applied from tails, by incremental builds and overlays.
    pub delta_edges: u64,
    /// Edges read by full builds and fallbacks.
    pub scanned_edges: u64,
    /// Records each overlay applied.
    pub overlay_records: Vec<u64>,
    /// `M − N` per graph with an index, as last seen.
    pub lag: BTreeMap<GraphId, u64>,
    /// Readers that took the canonical path, by reason.
    pub fallbacks: BTreeMap<String, u64>,
}

impl AdjacencySnapshot {
    /// Builds of `mode` (and of `reason`, when given).
    #[must_use]
    pub fn builds_of(&self, mode: &str, reason: Option<&str>) -> u64 {
        self.builds
            .iter()
            .filter(|((m, r), _)| m == mode && reason.is_none_or(|reason| r == reason))
            .map(|(_, n)| n)
            .sum()
    }

    /// Every build.
    #[must_use]
    pub fn builds_total(&self) -> u64 {
        self.builds.values().sum()
    }
}

impl AdjacencyStats {
    fn build(&self, mode: BuildMode, reason: Option<Decline>) {
        let mut builds = self.builds.lock().unwrap_or_else(PoisonError::into_inner);
        *builds
            .entry((mode.as_str(), reason.map_or("", Decline::as_str)))
            .or_default() += 1;
    }

    fn fallback(&self, reason: Decline) {
        let mut fallbacks = self
            .fallbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *fallbacks.entry(reason.as_str()).or_default() += 1;
    }

    fn overlay(&self, records: u64) {
        self.overlay_records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(records);
    }

    fn lag(&self, graph: GraphId, lag: u64) {
        self.lag
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(graph, lag);
    }

    /// The counters now.
    #[must_use]
    pub fn snapshot(&self) -> AdjacencySnapshot {
        AdjacencySnapshot {
            builds: self
                .builds
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|((m, r), n)| ((m.to_string(), r.to_string()), *n))
                .collect(),
            delta_edges: self.delta_edges.load(Ordering::Relaxed),
            scanned_edges: self.scanned_edges.load(Ordering::Relaxed),
            overlay_records: self
                .overlay_records
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
            lag: self
                .lag
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
            fallbacks: self
                .fallbacks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|(r, n)| (r.to_string(), *n))
                .collect(),
        }
    }
}

/// The most graphs whose index a node keeps in memory between algorithm runs.
pub const CACHED_GRAPHS: usize = 16;

/// An index kept in memory.
struct Cached {
    sha256: String,
    index: Arc<AdjacencyIndex>,
    stored: u64,
}

/// Builds, publishes and serves a node's adjacency indexes (shared contract `Indexer`). Cheap to
/// clone; clones share the counters and the cache.
#[derive(Clone)]
pub struct Indexer {
    manifests: ManifestStore,
    config: AdjacencyConfig,
    stats: Arc<AdjacencyStats>,
    /// The last index loaded or built per graph, by its SHA-256, for at most [`CACHED_GRAPHS`]
    /// graphs (the least recently stored go first).
    cache: Arc<Mutex<HashMap<GraphId, Cached>>>,
    /// The node's data directory: index files are cached under `<data>/graphs/<gr_id>/adj/`.
    data_dir: Option<PathBuf>,
    /// One build per graph at a time.
    running: Arc<Mutex<HashMap<GraphId, Arc<tokio::sync::Mutex<()>>>>>,
    #[cfg(feature = "test-hooks")]
    pin_hook: Arc<Mutex<Option<crate::catalog::AckHook>>>,
}

impl std::fmt::Debug for Indexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Indexer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Indexer {
    /// An indexer publishing to `manifests`, caching files under `data_dir` when given.
    #[must_use]
    pub fn new(
        manifests: ManifestStore,
        config: AdjacencyConfig,
        data_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            manifests,
            config,
            stats: Arc::default(),
            cache: Arc::default(),
            data_dir,
            running: Arc::default(),
            #[cfg(feature = "test-hooks")]
            pin_hook: Arc::default(),
        }
    }

    /// Its configuration.
    #[must_use]
    pub fn config(&self) -> AdjacencyConfig {
        self.config
    }

    /// The same indexer with `config` (its counters and cache shared).
    #[must_use]
    pub fn with_config(mut self, config: AdjacencyConfig) -> Self {
        self.config = config;
        self
    }

    /// Its counters.
    #[must_use]
    pub fn stats(&self) -> &Arc<AdjacencyStats> {
        &self.stats
    }

    /// **Tests only** (feature `test-hooks`): runs on a reader's index path after the tail is
    /// read and before the view is answered, holding nothing (`index_path_holds_no_gate`).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_pin_hook(&self, hook: Option<crate::catalog::AckHook>) {
        *self.pin_hook.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    #[cfg(feature = "test-hooks")]
    async fn pin_hook(&self) {
        let hook = self
            .pin_hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook().await;
        }
    }

    /// Graph `graph`'s current index entry, if it has one.
    ///
    /// # Errors
    ///
    /// What [`ManifestStore::load`] answers.
    pub async fn entry(
        &self,
        namespace: NamespaceId,
        graph: GraphId,
    ) -> Result<Option<AdjacencyEntry>, SnapshotError> {
        Ok(self
            .manifests
            .load(namespace, graph)
            .await?
            .and_then(|loaded| loaded.manifest.adjacency))
    }

    /// Brings `graph`'s index (open and owned here, in `namespace`) up to its durable offset,
    /// incrementally when it can, in full otherwise, and publishes it (§48 §26.3 steps 1–6).
    ///
    /// # Errors
    ///
    /// [`SnapshotError::NotServing`] for a graph that is not durable here,
    /// [`SnapshotError::Fenced`] when its owner lease moved on (the graph is then fenced here),
    /// or what reading the stream, scanning the engine or publishing answers.
    pub async fn run(
        &self,
        graph: Arc<Graph>,
        namespace: NamespaceId,
    ) -> Result<BuildOutcome, SnapshotError> {
        Ok(self.build(graph, namespace).await?.0)
    }

    /// [`Indexer::run`], answering the index too.
    async fn build(
        &self,
        graph: Arc<Graph>,
        namespace: NamespaceId,
    ) -> Result<(BuildOutcome, Arc<AdjacencyIndex>), SnapshotError> {
        let (id, durable) = match (graph.id(), graph.durable_state()) {
            (Some(id), Some(durable)) => (id, durable),
            _ => {
                return Err(SnapshotError::NotServing(
                    "the graph is not durable here".to_string(),
                ));
            }
        };
        let lock = {
            let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(running.entry(id).or_default())
        };
        let _one = lock.lock().await;
        let (log, stream, fence) = (durable.log.clone(), durable.stream, durable.fence.clone());
        let entry = self.entry(namespace, id).await?;
        let target = durable.position.offset();
        // Step 3: patch when everything holds.
        let mut reason = Decline::NoIndex;
        if let Some(entry) = &entry {
            match self.load(id, entry).await? {
                Err(decline) => reason = decline,
                Ok(prev) if prev.covered_offset >= target => {
                    self.stats.lag(id, 0);
                    let outcome = BuildOutcome {
                        mode: BuildMode::Unchanged,
                        reason: None,
                        covered_offset: prev.covered_offset,
                        delta_edges: 0,
                        scanned_edges: 0,
                        published: false,
                    };
                    return Ok((outcome, prev));
                }
                Ok(prev) => {
                    match self
                        .tail(
                            &log,
                            stream,
                            prev.covered_offset,
                            target,
                            self.config.incremental_max_records,
                        )
                        .await?
                    {
                        Err(decline) => reason = decline,
                        Ok(tail) => {
                            let limit =
                                self.config.incremental_max_fraction * prev.edge_count() as f64;
                            if tail.topology_ops() as f64 > limit {
                                reason = Decline::TailTooLong;
                            } else {
                                match patch(&prev, &tail) {
                                    Err(decline) => reason = decline,
                                    Ok(patched) => {
                                        self.stats
                                            .delta_edges
                                            .fetch_add(patched.delta_edges, Ordering::Relaxed);
                                        return self
                                            .finish(
                                                &graph,
                                                namespace,
                                                fence.as_ref(),
                                                patched.index,
                                                AdjacencyMode::Incremental,
                                                None,
                                                patched.delta_edges,
                                                0,
                                            )
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // Step 4: in full, as control flow.
        let (index, scanned) = self.scan(&graph).await?;
        self.finish(
            &graph,
            namespace,
            fence.as_ref(),
            index,
            AdjacencyMode::Full,
            Some(reason),
            0,
            scanned,
        )
        .await
    }

    /// A full build of `graph`, counted as scanned edges.
    async fn scan(&self, graph: &Arc<Graph>) -> Result<(AdjacencyIndex, u64), SnapshotError> {
        let held = Arc::clone(graph);
        let (index, scanned) = tokio::task::spawn_blocking(move || build_full(&held))
            .await
            .map_err(|_| SnapshotError::Engine(GraphError::EnginePanic))??;
        self.stats
            .scanned_edges
            .fetch_add(scanned, Ordering::Relaxed);
        Ok((index, scanned))
    }

    /// Publishes a built index and counts the build.
    #[allow(clippy::too_many_arguments)]
    async fn finish(
        &self,
        graph: &Graph,
        namespace: NamespaceId,
        fence: Option<&loams_common::meta::Fence>,
        index: AdjacencyIndex,
        mode: AdjacencyMode,
        reason: Option<Decline>,
        delta_edges: u64,
        scanned_edges: u64,
    ) -> Result<(BuildOutcome, Arc<AdjacencyIndex>), SnapshotError> {
        let id = index.graph;
        let bytes = index.encode();
        let published = match publish::publish(
            &self.manifests,
            namespace,
            id,
            fence,
            &index,
            bytes.clone(),
            mode,
            self.config.keep,
        )
        .await
        {
            Ok(published) => published,
            Err(SnapshotError::Fenced) => {
                tracing::warn!(graph = %id, "an adjacency index was refused: the graph's owner lease moved on; the graph is fenced here");
                graph.mark_fenced();
                return Err(SnapshotError::Fenced);
            }
            Err(err) => return Err(err),
        };
        let build_mode = match mode {
            AdjacencyMode::Full => BuildMode::Full,
            AdjacencyMode::Incremental => BuildMode::Incremental,
        };
        self.stats.build(build_mode, reason);
        let lag = graph
            .durable_state()
            .map_or(0, |d| d.position.offset())
            .saturating_sub(index.covered_offset);
        self.stats.lag(id, lag);
        let index = Arc::new(index);
        self.remember(id, &published.entry.sha256, &index, &bytes);
        tracing::debug!(graph = %id, mode = mode.as_str(), reason = reason.map_or("", Decline::as_str), offset = index.covered_offset, advanced = published.advanced, "built a graph adjacency index");
        Ok((
            BuildOutcome {
                mode: build_mode,
                reason,
                covered_offset: index.covered_offset,
                delta_edges,
                scanned_edges,
                published: published.advanced,
            },
            index,
        ))
    }

    /// The maintenance pass's step for `graph` (open and owned here): when it has an index and
    /// the index is over [`AdjacencyConfig::rebuild_after_records`] behind, or the graph changed
    /// and [`AdjacencyConfig::interval`] passed, brings it up to date. A graph no algorithm has
    /// touched has no index and is left alone.
    ///
    /// # Errors
    ///
    /// What [`Indexer::run`] answers.
    pub async fn maintain(
        &self,
        graph: Arc<Graph>,
        namespace: NamespaceId,
    ) -> Result<Option<BuildOutcome>, SnapshotError> {
        let (Some(id), Some(durable)) = (graph.id(), graph.durable_state()) else {
            return Ok(None);
        };
        if graph.needs_reopen() || graph.is_fenced() {
            return Ok(None);
        }
        let Some(entry) = self.entry(namespace, id).await? else {
            return Ok(None);
        };
        let lag = durable
            .position
            .offset()
            .saturating_sub(entry.covered_offset);
        self.stats.lag(id, lag);
        let aged = self
            .manifests
            .meta()
            .now_ms()
            .saturating_sub(entry.created_at_ms)
            >= u64::try_from(self.config.interval.as_millis()).unwrap_or(u64::MAX);
        if lag == 0 || (lag <= self.config.rebuild_after_records && !aged) {
            return Ok(None);
        }
        self.run(graph, namespace).await.map(Some)
    }

    /// The tail `[from, to)` of `stream`, decoded, or why it cannot be had: trimmed past `from`,
    /// or longer than `max_records`.
    async fn tail(
        &self,
        log: &crate::durable::GraphLog,
        stream: loams_common::StreamId,
        from: u64,
        to: u64,
        max_records: u64,
    ) -> Result<Result<Tail, Decline>, SnapshotError> {
        if to.saturating_sub(from) > max_records {
            return Ok(Err(Decline::TailTooLong));
        }
        let log = log.clone();
        tokio::task::spawn_blocking(move || {
            let (start, _) = log.bounds(stream)?;
            if start > from {
                return Ok(Err(Decline::Trimmed));
            }
            let records = match log.read(stream, from, to) {
                Ok(records) => records,
                // Trimmed between the bounds and the read.
                Err(_) if log.bounds(stream).is_ok_and(|(start, _)| start > from) => {
                    return Ok(Err(Decline::Trimmed));
                }
                Err(err) => return Err(err),
            };
            Ok(Tail::decode(from, to, &records))
        })
        .await
        .map_err(|_| SnapshotError::Engine(GraphError::EnginePanic))?
        .map_err(SnapshotError::from_graph)
    }

    /// The index `entry` names: from memory, the local cache, or the bucket, checked.
    async fn load(
        &self,
        graph: GraphId,
        entry: &AdjacencyEntry,
    ) -> Result<Result<Arc<AdjacencyIndex>, Decline>, SnapshotError> {
        if entry.format != format::FORMAT_NAME {
            return Ok(Err(Decline::UnknownVersion));
        }
        if let Some(cached) = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&graph)
            && cached.sha256 == entry.sha256
        {
            return Ok(Ok(Arc::clone(&cached.index)));
        }
        if let Some(path) = self.local_path(graph, &entry.sha256)
            && let Ok(bytes) = std::fs::read(&path)
            && let Ok((index, bytes)) = publish::check(entry, bytes)
        {
            let index = Arc::new(index);
            self.remember(graph, &entry.sha256, &index, &bytes);
            return Ok(Ok(index));
        }
        match publish::fetch(&self.manifests, entry).await? {
            Ok((index, bytes)) => {
                let index = Arc::new(index);
                self.remember(graph, &entry.sha256, &index, &bytes);
                Ok(Ok(index))
            }
            Err(decline) => Ok(Err(decline)),
        }
    }

    fn local_path(&self, graph: GraphId, sha256: &str) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|dir| {
            dir.join("graphs")
                .join(graph.to_string())
                .join("adj")
                .join(format!("{sha256}.adj"))
        })
    }

    /// Keeps `index` as `graph`'s, in memory and in the local cache (best effort).
    fn remember(&self, graph: GraphId, sha256: &str, index: &Arc<AdjacencyIndex>, bytes: &[u8]) {
        {
            let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            let stored = cache.values().map(|c| c.stored).max().map_or(0, |s| s + 1);
            cache.insert(
                graph,
                Cached {
                    sha256: sha256.to_string(),
                    index: Arc::clone(index),
                    stored,
                },
            );
            while cache.len() > CACHED_GRAPHS {
                let Some(oldest) = cache.iter().min_by_key(|(_, c)| c.stored).map(|(g, _)| *g)
                else {
                    break;
                };
                cache.remove(&oldest);
            }
        }
        if let Some(path) = self.local_path(graph, sha256)
            && !path.exists()
            && let Some(dir) = path.parent()
        {
            // Only the newest file is kept locally: older ones are in the bucket.
            if let Ok(entries) = std::fs::read_dir(dir) {
                for old in entries.flatten() {
                    let _ = std::fs::remove_file(old.path());
                }
            }
            let staged = path.with_extension("tmp");
            let written = std::fs::create_dir_all(dir)
                .and_then(|()| std::fs::write(&staged, bytes))
                .and_then(|()| std::fs::rename(&staged, &path));
            if let Err(err) = written {
                tracing::debug!(path = %path.display(), error = %err, "could not cache an adjacency index locally");
            }
        }
    }

    /// Drops what this node keeps of `graph`'s index (an evicted or deleted graph).
    pub fn forget(&self, graph: GraphId) {
        self.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&graph);
    }
}

impl SnapshotError {
    /// A graph error met while building an index, as a snapshot error.
    fn from_graph(err: GraphError) -> Self {
        match err {
            GraphError::LogUnavailable(message) => Self::Unavailable(message),
            other => Self::Engine(other),
        }
    }
}

impl crate::service::admin::GraphAdmin {
    /// Brings graph `namespace`/`name`'s adjacency index up to its durable offset now (GR1 Task
    /// 22): on demand, as [`crate::service::admin::GraphAdmin::checkpoint`] takes a snapshot.
    /// Runs on the graph's owner: the graph is opened here if it is not.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` when graphs are not durable in this server, what opening the graph
    /// answers, and `UNAVAILABLE` when the log, the bucket or the metastore fails or the owner
    /// lease moved on.
    pub async fn build_adjacency(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<BuildOutcome, connectrpc::ConnectError> {
        let Some(indexer) = self.indexer() else {
            return Err(crate::service::errors::refuse(
                connectrpc::ErrorCode::FailedPrecondition,
                "failed_precondition",
                "graphs are not durable in this server, so they have no adjacency index",
            ));
        };
        let graph = self.open(namespace, name).await?;
        let ns = self.namespace_of(namespace, name).await?;
        indexer
            .run(graph, ns)
            .await
            .map_err(|err| crate::service::errors::map_engine(err.into()))
    }
}
