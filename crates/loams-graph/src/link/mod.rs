//! The links that write LINKED graphs (GR1 Tasks 18–19; design §48 §10, D743).
//!
//! A LINKED graph is declared over collections by its [`GraphMapping`]. Each collection the
//! mapping names feeds the graph through a **link**: the collection's implicit stream applied
//! into the graph, exactly once, by `loams-link`'s [`LinkTarget`](loams_link::LinkTarget) contract
//! ([`collection::CollectionLinkTarget`]). A commit is one record of the graph's own stream that
//! carries the batch's change operations *and* the link's progress (R0.17), appended through the
//! same write lane as a GQL write ([`crate::durable::lane`]), so the graph's durable state always
//! says how far each link got.
//!
//! **Where links run** (R18.1). The metastore refuses a link whose source is a collection's
//! implicit stream (the link would dangle once the collection is dropped), so a graph's
//! collection links are not metastore links. They are the graph's own: the mapping names them,
//! [`LinkRunner`] applies them on the node that owns the graph (or that the rendezvous places it
//! on), and their progress is keyed by a graph-local link id, [`collection_link_id`]. Only the
//! owner can append to the graph's stream (its lease fences every append), so two runners never
//! both commit; the target's version, checked under the lane, makes a retried or concurrent
//! commit apply once.
//!
//! **What a link writes** ([`collection`], §48 §10.1). Vertices carry `_collection` and `_key`
//! (and `_pk` when they are keyed by another field than `_pk`); edges carry `_collection`, `_key`
//! (the edge document's primary key) and `_edge` (the mapping's edge name); a stub vertex, made
//! for an edge whose endpoint has no document yet, carries `_key` and `_stub = true`. Those
//! properties are how the link finds its elements again ([`LinkIndex`]) and how GraphRAG joins a
//! vertex back to its document.

pub mod collection;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use grafeo::{GrafeoDB, Value};
use loams_common::meta::{Consistency, Fence, Link, LinkId, MetaStore, TargetRef};
use loams_common::{CollectionId, NamespaceId, StreamId};
use loams_link::{LinkConfig, TargetState};
use tokio_util::sync::CancellationToken;

use crate::GraphId;
use crate::catalog::{GraphMeta, GraphMode};
use crate::mapping::GraphMapping;
use crate::service::admin::GraphAdmin;

/// The target kind of a graph's links, as their synthetic [`Link`] names it.
pub const GRAPH_KIND: &str = "graph";

/// The property naming the collection a vertex or an edge came from.
pub const COLLECTION_PROPERTY: &str = "_collection";
/// The property holding a vertex's key, or an edge document's primary key.
pub const KEY_PROPERTY: &str = "_key";
/// The property holding a vertex document's primary key when the vertex is keyed by a field.
pub const PK_PROPERTY: &str = "_pk";
/// The property naming the mapping's edge an edge came from.
pub const EDGE_PROPERTY: &str = "_edge";
/// The property marking a stub vertex (§48 §10.1).
pub const STUB_PROPERTY: &str = "_stub";

/// The properties a link sets itself: never taken from a document, and dropped from an element
/// when the link rebuilds its document for a patch.
pub const LINK_PROPERTIES: [&str; 5] = [
    COLLECTION_PROPERTY,
    KEY_PROPERTY,
    PK_PROPERTY,
    EDGE_PROPERTY,
    STUB_PROPERTY,
];

/// The high bit of a graph-local collection link id (R18.1): metastore link ids are small
/// sequential numbers, so the two never meet in a graph's records.
const COLLECTION_LINK_BIT: u64 = 1 << 63;

/// The graph-local id of the link from collection `collection` (R18.1).
#[must_use]
pub fn collection_link_id(collection: CollectionId) -> LinkId {
    LinkId(COLLECTION_LINK_BIT | collection.0)
}

/// A vertex key, or a primary key as a link stores it: an integer or a string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    /// An integer key (`_pk` of a `u64` collection that fits `i64`, an integer field).
    Int(i64),
    /// A string key (a keyword, a UUID in its hyphenated lower-case form).
    Str(String),
}

impl Key {
    /// The key as a property value.
    #[must_use]
    pub fn value(&self) -> Value {
        match self {
            Self::Int(n) => Value::Int64(*n),
            Self::Str(s) => Value::String(s.as_str().into()),
        }
    }

    /// The key a property value holds, if it holds one.
    #[must_use]
    pub fn of_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int64(n) => Some(Self::Int(*n)),
            Value::String(s) => Some(Self::Str(s.to_string())),
            _ => None,
        }
    }

    /// The key a document field holds: an integer or a string.
    #[must_use]
    pub fn of_json(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::String(s) => Some(Self::Str(s.clone())),
            serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64()) {
                (Some(i), _) => Some(Self::Int(i)),
                (None, Some(u)) => Some(Self::Str(u.to_string())),
                _ => None,
            },
            _ => None,
        }
    }

    /// The key of a primary key: a `u64` as an integer (as a string above `i64::MAX`), a UUID
    /// hyphenated in lower case, a string as it is.
    #[must_use]
    pub fn of_pk(pk: &loams_collection::PrimaryKey) -> Self {
        match pk {
            loams_collection::PrimaryKey::U64(n) => {
                i64::try_from(*n).map_or_else(|_| Self::Str(n.to_string()), Self::Int)
            }
            loams_collection::PrimaryKey::Uuid(bytes) => Self::Str(hyphenated(bytes)),
            loams_collection::PrimaryKey::Str(s) => Self::Str(s.clone()),
        }
    }
}

/// A UUID's 16 bytes as `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
fn hyphenated(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Where a graph's link-written elements are (GR1 Task 18): built once from the engine the first
/// time a link writes it, then kept by every link commit. It lives with one engine instance
/// ([`crate::durable::Durable`]), so a reload, which may drop commits the index saw, starts a new
/// one.
#[derive(Debug, Default)]
pub struct LinkIndex {
    /// `(label, key)` → vertex, stubs included.
    vertices: HashMap<(String, Key), u64>,
    /// `(label, primary key)` → vertex, for vertices keyed by a field.
    by_pk: HashMap<(String, Key), u64>,
    /// `(edge name, edge document's primary key)` → edge.
    edges: HashMap<(String, Key), u64>,
    /// Every edge's endpoints.
    ends: HashMap<u64, (u64, u64)>,
    /// Every node's edges, in and out.
    incident: HashMap<u64, BTreeSet<u64>>,
    /// The stub vertices.
    stubs: HashSet<u64>,
}

impl LinkIndex {
    /// The index of every element of `db`.
    #[must_use]
    pub fn build(db: &GrafeoDB) -> Self {
        let store = db.store();
        let mut index = Self::default();
        for node in store.all_nodes() {
            let id = node.id.as_u64();
            let prop = |name: &str| {
                node.properties
                    .iter()
                    .find(|(key, _)| key.as_str() == name)
                    .map(|(_, value)| value.clone())
            };
            let Some(label) = node.labels.first().map(ToString::to_string) else {
                continue;
            };
            if let Some(key) = prop(KEY_PROPERTY).as_ref().and_then(Key::of_value) {
                index.vertices.insert((label.clone(), key), id);
            }
            if let Some(pk) = prop(PK_PROPERTY).as_ref().and_then(Key::of_value) {
                index.by_pk.insert((label, pk), id);
            }
            if matches!(prop(STUB_PROPERTY), Some(Value::Bool(true))) {
                index.stubs.insert(id);
            }
        }
        for edge in store.all_edges() {
            let id = edge.id.as_u64();
            let (src, dst) = (edge.src.as_u64(), edge.dst.as_u64());
            index.ends.insert(id, (src, dst));
            index.incident.entry(src).or_default().insert(id);
            index.incident.entry(dst).or_default().insert(id);
            let prop = |name: &str| {
                edge.properties
                    .iter()
                    .find(|(key, _)| key.as_str() == name)
                    .map(|(_, value)| value.clone())
            };
            if let (Some(Value::String(name)), Some(key)) = (
                prop(EDGE_PROPERTY),
                prop(KEY_PROPERTY).as_ref().and_then(Key::of_value),
            ) {
                index.edges.insert((name.to_string(), key), id);
            }
        }
        index
    }

    fn vertex(&self, label: &str, key: &Key) -> Option<u64> {
        self.vertices
            .get(&(label.to_string(), key.clone()))
            .copied()
    }

    fn vertex_by_pk(&self, label: &str, pk: &Key) -> Option<u64> {
        self.by_pk.get(&(label.to_string(), pk.clone())).copied()
    }

    fn edge(&self, name: &str, pk: &Key) -> Option<u64> {
        self.edges.get(&(name.to_string(), pk.clone())).copied()
    }

    fn is_stub(&self, node: u64) -> bool {
        self.stubs.contains(&node)
    }

    fn degree(&self, node: u64) -> usize {
        self.incident.get(&node).map_or(0, BTreeSet::len)
    }

    fn edges_of(&self, node: u64) -> Vec<u64> {
        self.incident
            .get(&node)
            .map(|edges| edges.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// How far one of a LINKED graph's links is behind its source (GR1 Task 18).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkLag {
    /// The collection, as the mapping names it.
    pub collection: String,
    /// The link's graph-local id.
    pub link: LinkId,
    /// The next source offset the graph applies, by partition.
    pub applied: BTreeMap<u32, u64>,
    /// The source's high watermarks, by partition.
    pub high_watermarks: BTreeMap<u32, u64>,
    /// Records of the source not applied yet.
    pub records: u64,
    /// The collection no longer exists: its link is stopped where it was.
    pub missing: bool,
}

/// One source of a LINKED graph: a collection and the mapping names that resolve to it.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    pub(crate) collection: loams_common::meta::Collection,
    /// The names the mapping uses for it (a name and an alias of the same collection are one
    /// source).
    pub(crate) names: BTreeSet<String>,
}

impl Source {
    /// The link's synthetic metastore shape, for `loams-link`'s apply loop.
    pub(crate) fn link(&self, graph: GraphId) -> Link {
        Link {
            id: collection_link_id(self.collection.id),
            namespace: self.collection.namespace,
            name: format!("{graph}/{}", self.collection.name),
            source: self.collection.stream,
            target: TargetRef {
                kind: GRAPH_KIND.to_string(),
                name: graph.to_string(),
            },
            options: BTreeMap::new(),
        }
    }
}

/// The collections `mapping` names in `namespace`, each once, with the names that reach it, and
/// the names that resolve to no collection.
pub(crate) async fn sources(
    meta: &dyn MetaStore,
    namespace: NamespaceId,
    mapping: &GraphMapping,
) -> Result<(Vec<Source>, BTreeSet<String>), loams_common::meta::MetaError> {
    let names: BTreeSet<&str> = mapping
        .vertices
        .iter()
        .map(|v| v.collection.as_str())
        .chain(mapping.edges.iter().map(|e| e.collection.as_str()))
        .collect();
    let mut by_id: BTreeMap<CollectionId, Source> = BTreeMap::new();
    let mut missing = BTreeSet::new();
    for name in names {
        match meta
            .resolve_collection(Consistency::Local, namespace, name)
            .await?
        {
            Some(collection) => {
                by_id
                    .entry(collection.id)
                    .or_insert_with(|| Source {
                        collection,
                        names: BTreeSet::new(),
                    })
                    .names
                    .insert(name.to_string());
            }
            None => {
                missing.insert(name.to_string());
            }
        }
    }
    Ok((by_id.into_values().collect(), missing))
}

/// The high watermarks of stream `stream`'s partitions that have records, or `None` when the
/// stream is gone.
pub(crate) async fn high_watermarks(
    meta: &dyn MetaStore,
    stream: StreamId,
) -> Result<Option<BTreeMap<u32, u64>>, loams_common::meta::MetaError> {
    Ok(meta
        .stream_state(Consistency::Local, stream)
        .await?
        .map(|state| {
            state
                .partitions
                .into_iter()
                .enumerate()
                .filter_map(|(p, bounds)| {
                    let hwm = bounds.map_or(0, |b| b.high_watermark);
                    (hwm > 0).then(|| (u32::try_from(p).unwrap_or(u32::MAX), hwm))
                })
                .collect()
        }))
}

/// Records of a source not yet applied, given its high watermarks and the applied offsets.
pub(crate) fn lag(hwms: &BTreeMap<u32, u64>, applied: &BTreeMap<u32, u64>) -> u64 {
    hwms.iter()
        .map(|(p, hwm)| hwm.saturating_sub(applied.get(p).copied().unwrap_or(0)))
        .sum()
}

/// How often [`LinkRunner`] re-reads the catalog for LINKED graphs.
const RELIST_EVERY: Duration = Duration::from_secs(5);

/// Applies the links of the LINKED graphs this node serves (R18.1): each graph whose owner lease
/// this node holds, or that the rendezvous places here when nobody holds it. The server runs
/// [`LinkRunner::run_once`] in a loop; a test calls it, or [`LinkRunner::apply_graph`], directly.
#[derive(Clone)]
pub struct LinkRunner {
    admin: GraphAdmin,
    config: LinkConfig,
    stop: CancellationToken,
    state: Arc<Mutex<RunnerState>>,
}

#[derive(Default)]
struct RunnerState {
    /// The LINKED graphs, as last listed.
    graphs: Vec<GraphMeta>,
    listed_at: Option<Instant>,
    /// Per link, the applied offsets last seen: a link whose source has not moved is skipped
    /// without opening its graph, so an idle graph can still be evicted.
    seen: HashMap<(GraphId, LinkId), BTreeMap<u32, u64>>,
}

impl std::fmt::Debug for LinkRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkRunner")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LinkRunner {
    /// How often the server runs a pass ([`LinkRunner::run_once`]).
    pub const EVERY: Duration = Duration::from_millis(500);

    /// A runner over `admin`'s graphs, with `loams-link`'s batch limits `config`. `admin` must
    /// have a log: a link writes only durable graphs.
    #[must_use]
    pub fn new(admin: GraphAdmin, config: LinkConfig) -> Self {
        Self {
            admin,
            config,
            stop: CancellationToken::new(),
            state: Arc::default(),
        }
    }

    /// Stops every apply in flight (shutdown).
    pub fn stop(&self) {
        self.stop.cancel();
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RunnerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The LINKED graphs, listed again when the last list is older than [`RELIST_EVERY`].
    async fn linked_graphs(&self) -> Vec<GraphMeta> {
        {
            let state = self.state();
            if state
                .listed_at
                .is_some_and(|at| at.elapsed() < RELIST_EVERY)
            {
                return state.graphs.clone();
            }
        }
        let catalog = self.admin.catalog();
        let mut graphs = Vec::new();
        match catalog.namespaces().await {
            Ok(namespaces) => {
                for namespace in namespaces {
                    let mut token = String::new();
                    loop {
                        match catalog.list(&namespace, 0, &token).await {
                            Ok(page) => {
                                graphs.extend(
                                    page.graphs
                                        .into_iter()
                                        .filter(|g| g.mode == GraphMode::Linked),
                                );
                                match page.next_page_token {
                                    Some(next) => token = next,
                                    None => break,
                                }
                            }
                            Err(err) => {
                                tracing::warn!(%namespace, error = %err, "listing a namespace's graphs for their links failed");
                                break;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "listing the namespaces for graph links failed");
                return self.state().graphs.clone();
            }
        }
        let mut state = self.state();
        state.graphs = graphs.clone();
        state.listed_at = Some(Instant::now());
        let live: HashSet<GraphId> = graphs.iter().filter_map(|g| g.graph_id().ok()).collect();
        state.seen.retain(|(graph, _), _| live.contains(graph));
        graphs
    }

    /// One pass: applies every link with records to apply of every LINKED graph this node
    /// serves. Answers how many links committed. A failing graph or link is logged and skipped.
    pub async fn run_once(&self) -> usize {
        let mut committed = 0;
        for graph in self.linked_graphs().await {
            if self.stop.is_cancelled() {
                break;
            }
            let Ok(id) = graph.graph_id() else { continue };
            if !self.serves(id) {
                continue;
            }
            match self.apply_meta(&graph, false).await {
                Ok(n) => committed += n,
                Err(err) => {
                    tracing::warn!(namespace = %graph.namespace, graph = %graph.name, error = %err, "applying a graph's links failed; retried on the next pass");
                }
            }
        }
        committed
    }

    /// Whether this node applies `graph`'s links: it holds the graph's owner lease, or nobody
    /// does and the rendezvous places the graph here. Without ownership (a single node), always.
    fn serves(&self, graph: GraphId) -> bool {
        match self.admin.ownership() {
            None => true,
            Some(ownership) => ownership.held(graph).is_some() || ownership.prefers_me(graph),
        }
    }

    /// Applies every link of graph `name` in `namespace` until each has caught up with its
    /// source as it was when the call started (or a pass found nothing to apply). Answers how
    /// many links committed.
    ///
    /// # Errors
    ///
    /// The catalog's, the metastore's or a link's error, as text.
    pub async fn apply_graph(&self, namespace: &str, name: &str) -> Result<usize, String> {
        let meta = self
            .admin
            .catalog()
            .get_by_name(namespace, name)
            .await
            .map_err(|err| err.to_string())?;
        self.apply_meta(&meta, true).await
    }

    /// [`LinkRunner::apply_graph`] for a catalog record. Unless `force`, a link whose source has
    /// not moved since its last apply is skipped.
    async fn apply_meta(&self, graph: &GraphMeta, force: bool) -> Result<usize, String> {
        let id = graph.graph_id().map_err(|err| err.to_string())?;
        let Some(mapping) = graph.mapping.as_ref() else {
            return Ok(0);
        };
        let meta = self.admin.catalog().meta().clone();
        let namespace = self
            .admin
            .catalog()
            .namespace_id(&graph.namespace)
            .await
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("namespace {} is gone", graph.namespace))?;
        let (sources, missing) = sources(&*meta, namespace, mapping)
            .await
            .map_err(|err| err.to_string())?;
        for name in &missing {
            tracing::debug!(namespace = %graph.namespace, graph = %graph.name, collection = %name, "a LINKED graph's collection is gone; its link is stopped");
        }
        let reader = self
            .admin
            .log()
            .ok_or("a link writes only a durable graph")?
            .reader()
            .clone();
        // The links with records to apply, as last seen.
        let mut due = Vec::new();
        for source in sources {
            let link = source.link(id);
            let Some(hwms) = high_watermarks(&*meta, link.source)
                .await
                .map_err(|err| err.to_string())?
            else {
                continue;
            };
            let caught_up = self
                .state()
                .seen
                .get(&(id, link.id))
                .is_some_and(|applied| lag(&hwms, applied) == 0);
            if force || !caught_up {
                due.push((source, link));
            }
        }
        if due.is_empty() {
            return Ok(0);
        }
        // Another node owns the graph: its links are that node's (R18.1).
        if !force
            && let Some(ownership) = self.admin.ownership()
            && ownership.held(id).is_none()
            && ownership.forwarder().is_some()
            && matches!(
                ownership.place(id, true).await,
                Ok(crate::owner::Placement::There(_)) | Err(_)
            )
        {
            return Ok(0);
        }
        let mut committed = 0;
        for (source, link) in due {
            let target = collection::CollectionLinkTarget::new(
                self.admin.clone(),
                graph.clone(),
                mapping.clone(),
                source,
                link.id,
            );
            let fence = Fence {
                lease: crate::owner::owner_lease_key(id),
                epoch: 0,
            };
            loop {
                let outcome = loams_link::apply_link(
                    &reader,
                    &*meta,
                    &self.config,
                    &link,
                    &target,
                    &fence,
                    &self.stop,
                    |state: &TargetState| {
                        self.state()
                            .seen
                            .insert((id, link.id), state.applied.clone());
                    },
                )
                .await
                .map_err(|err| format!("link {}: {err}", link.name))?;
                if outcome != loams_worker::TaskOutcome::MoreWork {
                    break;
                }
            }
            committed += usize::from(target.commits() > 0);
        }
        Ok(committed)
    }

    /// How far each of graph `name`'s links is behind its source (GR1 Task 18): the records of
    /// each mapped collection the graph has not applied yet. Read on the graph's owner, from its
    /// durable state; a collection that no longer exists is listed as `missing`.
    ///
    /// # Errors
    ///
    /// The catalog's, the metastore's, or the open's error (the graph is owned elsewhere), as
    /// text.
    pub async fn lag(&self, namespace: &str, name: &str) -> Result<Vec<LinkLag>, String> {
        let graph = self
            .admin
            .catalog()
            .get_by_name(namespace, name)
            .await
            .map_err(|err| err.to_string())?;
        let id = graph.graph_id().map_err(|err| err.to_string())?;
        let Some(mapping) = graph.mapping.as_ref() else {
            return Ok(Vec::new());
        };
        let meta = self.admin.catalog().meta().clone();
        let namespace_id = self
            .admin
            .catalog()
            .namespace_id(namespace)
            .await
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("namespace {namespace} is gone"))?;
        let (sources, missing) = sources(&*meta, namespace_id, mapping)
            .await
            .map_err(|err| err.to_string())?;
        let open = self
            .admin
            .link_graph(&graph)
            .await
            .map_err(|err| err.to_string())?;
        let mut out = Vec::new();
        for source in sources {
            let link = collection_link_id(source.collection.id);
            let applied = open
                .link_progress(link)
                .map(|progress| progress.applied_after)
                .unwrap_or_default();
            let hwms = high_watermarks(&*meta, source.collection.stream)
                .await
                .map_err(|err| err.to_string())?;
            let records = hwms.as_ref().map_or(0, |hwms| lag(hwms, &applied));
            for collection in &source.names {
                out.push(LinkLag {
                    collection: collection.clone(),
                    link,
                    applied: applied.clone(),
                    high_watermarks: hwms.clone().unwrap_or_default(),
                    records,
                    missing: hwms.is_none(),
                });
            }
            tracing::debug!(graph = %id, collection = %source.collection.name, lag_records = records, "graph link lag");
        }
        for collection in missing {
            out.push(LinkLag {
                collection,
                link: LinkId(0),
                applied: BTreeMap::new(),
                high_watermarks: BTreeMap::new(),
                records: 0,
                missing: true,
            });
        }
        out.sort_by(|a, b| a.collection.cmp(&b.collection));
        Ok(out)
    }
}
