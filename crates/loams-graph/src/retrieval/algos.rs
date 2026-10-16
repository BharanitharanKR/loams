//! The graph algorithms (GR1 Task 22; design §07 §5.3, §48 §26.3; R22.0, R22.3, R22.4):
//! `pagerank`, `wcc` and `leiden`, run on a [`PinnedAdjacency`](crate::adjacency::PinnedAdjacency)'s
//! view, never through Grafeo's `algos`.
//!
//! * [`pagerank`] is Loams' own power iteration, networkx's convention (R22.0): a uniform start
//!   and teleport, the dangling vertices' mass spread uniformly, every edge counted once
//!   (parallel edges each, a self-loop as an out-edge), on the subgraph the `label` and
//!   `edge_types` keep. It stops when the L1 change is below `N × tolerance`, or after
//!   `max_iterations`, and answers its last iterate either way (R22.6).
//! * [`wcc`] is Loams' own union-find over both directions; components are numbered 0, 1, … by
//!   their smallest member in canonical order (the canonical key bytes, then the element id;
//!   keyless vertices after every keyed one).
//! * [`leiden`] is `network_partitions::leiden::hierarchical_leiden` (graspologic-native, MIT;
//!   R22.3), the algorithm GraphRAG runs, with graspologic's defaults (randomness 0.001, one
//!   iteration, modularity) and a `SmallRng` seeded from `seed`, as graspologic's Python binding
//!   seeds it, so a seed gives graspologic's answer. It runs on the undirected projection: one
//!   edge per vertex pair, weighing the sum of the weights of every edge between them either way
//!   (each 1 unless `weight` is asked for), fed in ordinal order.
//!
//! Every answer depends only on the view and the parameters (`algorithms_deterministic_for_seed_and_snapshot`).

use std::collections::BTreeMap;
use std::sync::Arc;

use loams_collection::PrimaryKey;
use network_partitions::network::prelude::{LabeledNetwork, LabeledNetworkBuilder};
use rand_010::SeedableRng;

use connectrpc::{ConnectError, ErrorCode};

use crate::adjacency::AdjacencyIndex;
use crate::adjacency::format::WEIGHT_PROPERTY;
use crate::engine::GraphError;
use crate::service::admin::GraphAdmin;
use crate::service::errors::{map_catalog, map_engine, refuse};

/// `pagerank`'s parameters (§07 §5.3).
#[derive(Debug, Clone, PartialEq)]
pub struct PageRank {
    /// Only vertices with this label (and the edges between them).
    pub label: Option<String>,
    /// Only edges of these types; empty is every type.
    pub edge_types: Vec<String>,
    /// The damping factor (default 0.85).
    pub damping: f64,
    /// The most iterations (default 20).
    pub max_iterations: u32,
    /// The convergence tolerance (default 1e-6).
    pub tolerance: f64,
}

impl Default for PageRank {
    fn default() -> Self {
        Self {
            label: None,
            edge_types: Vec::new(),
            damping: 0.85,
            max_iterations: 20,
            tolerance: 1e-6,
        }
    }
}

/// `leiden`'s parameters (§07 §5.3).
#[derive(Debug, Clone, PartialEq)]
pub struct Leiden {
    /// Only edges of these types; empty is every type.
    pub edge_types: Vec<String>,
    /// The edge property that weighs an edge: `weight` (R22.5), or none for 1 each.
    pub weight: Option<String>,
    /// The resolution (default 1.0).
    pub resolution: f64,
    /// Levels answered, from 0 (default 4).
    pub max_levels: u32,
    /// A community larger than this is split again, one level down (default 1000, graspologic's).
    pub max_cluster_size: u32,
    /// The random seed (default 42).
    pub seed: u64,
}

impl Default for Leiden {
    fn default() -> Self {
        Self {
            edge_types: Vec::new(),
            weight: None,
            resolution: 1.0,
            max_levels: 4,
            max_cluster_size: 1000,
            seed: 42,
        }
    }
}

/// graspologic's `randomness` default.
const LEIDEN_RANDOMNESS: f64 = 0.001;

/// One row of `leiden`'s answer, by ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Community {
    /// The level, from 0.
    pub level: u32,
    /// The vertex's ordinal in the view.
    pub ordinal: usize,
    /// Its community at that level.
    pub community: u64,
    /// The community it splits, one level up; none at level 0.
    pub parent: Option<u64>,
    /// Whether this is the vertex's deepest level.
    pub last: bool,
}

/// The edge types kept: `None` for every type.
fn type_filter(view: &AdjacencyIndex, edge_types: &[String]) -> Option<Vec<bool>> {
    (!edge_types.is_empty()).then(|| {
        view.edge_types
            .iter()
            .map(|t| edge_types.iter().any(|kept| kept == t))
            .collect()
    })
}

fn kept(filter: Option<&Vec<bool>>, edge_type: u32) -> bool {
    filter.is_none_or(|f| f[edge_type as usize])
}

/// PageRank on `view`: each kept vertex's ordinal and rank, in ordinal order.
///
/// # Errors
///
/// [`GraphError::InvalidValue`] for a damping factor outside `[0, 1]` or a tolerance that is not
/// positive.
pub fn pagerank(view: &AdjacencyIndex, params: &PageRank) -> Result<Vec<(usize, f64)>, GraphError> {
    if !(0.0..=1.0).contains(&params.damping) {
        return Err(GraphError::InvalidValue(
            "pagerank: damping is between 0 and 1".to_string(),
        ));
    }
    if params.tolerance.is_nan() || params.tolerance <= 0.0 {
        return Err(GraphError::InvalidValue(
            "pagerank: tolerance must be positive".to_string(),
        ));
    }
    let label = match &params.label {
        Some(label) => match view.labels.iter().position(|l| l == label) {
            Some(id) => Some(id as u32),
            None => return Ok(Vec::new()),
        },
        None => None,
    };
    let members: Vec<usize> = (0..view.vertices.len())
        .filter(|v| label.is_none_or(|l| view.vertices[*v].labels.contains(&l)))
        .collect();
    let n = members.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    // Dense indexes of the kept vertices.
    let mut dense = vec![usize::MAX; view.vertices.len()];
    for (i, v) in members.iter().enumerate() {
        dense[*v] = i;
    }
    let filter = type_filter(view, &params.edge_types);
    let out: Vec<Vec<usize>> = members
        .iter()
        .map(|v| {
            view.csr
                .run(*v)
                .iter()
                .filter(|a| kept(filter.as_ref(), a.edge_type))
                .map(|a| dense[a.other as usize])
                .filter(|d| *d != usize::MAX)
                .collect()
        })
        .collect();
    let alpha = params.damping;
    let start = 1.0 / n as f64;
    let mut x = vec![start; n];
    for _ in 0..params.max_iterations {
        let dangling: f64 = (0..n).filter(|u| out[*u].is_empty()).map(|u| x[u]).sum();
        let mut next = vec![0.0; n];
        for (u, targets) in out.iter().enumerate() {
            if targets.is_empty() {
                continue;
            }
            let share = x[u] / targets.len() as f64;
            for t in targets {
                next[*t] += share;
            }
        }
        let base = alpha * dangling * start + (1.0 - alpha) * start;
        for value in &mut next {
            *value = alpha * *value + base;
        }
        let err: f64 = next.iter().zip(&x).map(|(a, b)| (a - b).abs()).sum();
        x = next;
        if err < n as f64 * params.tolerance {
            break;
        }
    }
    Ok(members.into_iter().zip(x).collect())
}

/// The canonical order of `view`'s vertices: by the canonical bytes of their `_key`, then
/// element id; keyless vertices after the keyed ones, by element id.
#[must_use]
pub fn canonical_order(view: &AdjacencyIndex) -> Vec<usize> {
    let mut order: Vec<(u8, Vec<u8>, u64, usize)> = view
        .vertices
        .iter()
        .enumerate()
        .map(|(i, v)| match &v.key {
            Some(key) => (0, key.primary_key().canonical(), v.id, i),
            None => (1, Vec::new(), v.id, i),
        })
        .collect();
    order.sort();
    order.into_iter().map(|(_, _, _, i)| i).collect()
}

/// Weakly connected components of `view`: each vertex's ordinal and component, in ordinal order.
#[must_use]
pub fn wcc(view: &AdjacencyIndex, edge_types: &[String]) -> Vec<(usize, u64)> {
    let n = view.vertices.len();
    let mut parent: Vec<usize> = (0..n).collect();
    fn find(parent: &mut [usize], mut v: usize) -> usize {
        while parent[v] != v {
            parent[v] = parent[parent[v]];
            v = parent[v];
        }
        v
    }
    let filter = type_filter(view, edge_types);
    for u in 0..n {
        for adj in view.csr.run(u) {
            if !kept(filter.as_ref(), adj.edge_type) {
                continue;
            }
            let (a, b) = (find(&mut parent, u), find(&mut parent, adj.other as usize));
            if a != b {
                parent[a.max(b)] = a.min(b);
            }
        }
    }
    let mut numbered: BTreeMap<usize, u64> = BTreeMap::new();
    let mut component = vec![0; n];
    for v in canonical_order(view) {
        let root = find(&mut parent, v);
        let next = numbered.len() as u64;
        component[v] = *numbered.entry(root).or_insert(next);
    }
    component.into_iter().enumerate().collect()
}

/// Hierarchical Leiden on `view`: rows by level, then ordinal. Vertices with no kept edge are
/// not in the answer.
///
/// # Errors
///
/// [`GraphError::InvalidValue`] for a weight property other than `weight` (R22.5), a resolution
/// that is not positive, `max_levels` or `max_cluster_size` of 0, or what Leiden refuses.
pub fn leiden(view: &AdjacencyIndex, params: &Leiden) -> Result<Vec<Community>, GraphError> {
    let weighted = match params.weight.as_deref() {
        None => false,
        Some(WEIGHT_PROPERTY) => true,
        Some(other) => {
            return Err(GraphError::InvalidValue(format!(
                "leiden: weight is '{WEIGHT_PROPERTY}' or NULL, not '{other}': the adjacency index \
                 keeps each edge's '{WEIGHT_PROPERTY}' only"
            )));
        }
    };
    if params.resolution.is_nan() || params.resolution <= 0.0 {
        return Err(GraphError::InvalidValue(
            "leiden: resolution must be positive".to_string(),
        ));
    }
    if params.max_levels == 0 || params.max_cluster_size == 0 {
        return Err(GraphError::InvalidValue(
            "leiden: max_levels and max_cluster_size are at least 1".to_string(),
        ));
    }
    let filter = type_filter(view, &params.edge_types);
    let mut pairs: BTreeMap<(usize, usize), f64> = BTreeMap::new();
    for u in 0..view.vertices.len() {
        for adj in view.csr.run(u) {
            if !kept(filter.as_ref(), adj.edge_type) {
                continue;
            }
            let v = adj.other as usize;
            let key = (u.min(v), u.max(v));
            *pairs.entry(key).or_insert(0.0) += if weighted { adj.weight } else { 1.0 };
        }
    }
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    let mut builder: LabeledNetworkBuilder<usize> = LabeledNetworkBuilder::new();
    let network: LabeledNetwork<usize> =
        builder.build(pairs.iter().map(|((a, b), w)| (*a, *b, *w)), true);
    let mut rng = rand_010::rngs::SmallRng::seed_from_u64(params.seed);
    let clusters = network_partitions::leiden::hierarchical_leiden(
        network.compact(),
        None,
        Some(1),
        Some(params.resolution),
        Some(LEIDEN_RANDOMNESS),
        &mut rng,
        true,
        params.max_cluster_size,
        None,
    )
    .map_err(|err| GraphError::InvalidValue(format!("leiden: {err:?}")))?;
    let mut rows: Vec<Community> = clusters
        .iter()
        .filter(|c| c.level < params.max_levels)
        .map(|c| Community {
            level: c.level,
            ordinal: *network.label_for(c.node),
            community: c.cluster as u64,
            parent: c.parent_cluster.map(|p| p as u64),
            last: false,
        })
        .collect();
    rows.sort();
    // Each vertex's deepest answered level (what a write-back writes).
    let mut deepest: BTreeMap<usize, u32> = BTreeMap::new();
    for row in &rows {
        let level = deepest.entry(row.ordinal).or_insert(row.level);
        *level = (*level).max(row.level);
    }
    for row in &mut rows {
        row.last = deepest.get(&row.ordinal) == Some(&row.level);
    }
    Ok(rows)
}

/// Newman's modularity of the partition `community` (by ordinal) of `view`'s undirected
/// projection (as [`leiden`] builds it): the quality `leiden` maximises, for the tests' reference
/// check.
#[must_use]
pub fn modularity(
    view: &AdjacencyIndex,
    edge_types: &[String],
    weighted: bool,
    resolution: f64,
    community: &BTreeMap<usize, u64>,
) -> f64 {
    let filter = type_filter(view, edge_types);
    let mut pairs: BTreeMap<(usize, usize), f64> = BTreeMap::new();
    for u in 0..view.vertices.len() {
        for adj in view.csr.run(u) {
            if kept(filter.as_ref(), adj.edge_type) {
                let v = adj.other as usize;
                *pairs.entry((u.min(v), u.max(v))).or_insert(0.0) +=
                    if weighted { adj.weight } else { 1.0 };
            }
        }
    }
    let mut degree: BTreeMap<u64, f64> = BTreeMap::new();
    let mut inside: BTreeMap<u64, f64> = BTreeMap::new();
    let mut m = 0.0;
    for ((a, b), w) in &pairs {
        let (ca, cb) = (community.get(a), community.get(b));
        m += w;
        if let Some(c) = ca {
            *degree.entry(*c).or_default() += w;
        }
        if let Some(c) = cb {
            *degree.entry(*c).or_default() += w;
        }
        if ca.is_some() && ca == cb {
            *inside.entry(*ca.unwrap_or(&0)).or_default() += w;
        }
    }
    if m == 0.0 {
        return 0.0;
    }
    degree
        .iter()
        .map(|(c, d)| {
            inside.get(c).copied().unwrap_or(0.0) / m - resolution * (d / (2.0 * m)).powi(2)
        })
        .sum()
}

/// A vertex's identity in an algorithm's answer: its label (the smallest of its labels, or `""`)
/// and its `_key`; `None` for a keyless vertex, which is computed with but never answered.
#[must_use]
pub fn identity(view: &AdjacencyIndex, ordinal: usize) -> Option<(String, PrimaryKey)> {
    let vertex = &view.vertices[ordinal];
    let key = vertex.key.as_ref()?;
    let label = vertex
        .labels
        .first()
        .map(|l| view.labels[*l as usize].clone())
        .unwrap_or_default();
    Some((label, key.primary_key()))
}

/// An algorithm and its parameters.
#[derive(Debug, Clone, PartialEq)]
pub enum Algorithm {
    /// `pagerank`.
    PageRank(PageRank),
    /// `wcc`, over edges of these types (empty is every type).
    Wcc {
        /// Only edges of these types.
        edge_types: Vec<String>,
    },
    /// `leiden`.
    Leiden(Leiden),
}

/// What an algorithm answers for one vertex (one level of it, for `leiden`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Score {
    /// A PageRank.
    Rank(f64),
    /// A component.
    Component(u64),
    /// A community at one level.
    Community(Community),
}

/// One vertex's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Scored {
    /// The vertex's ordinal in the view.
    pub ordinal: usize,
    /// Its element id.
    pub element: u64,
    /// What the algorithm answers for it.
    pub score: Score,
}

/// Runs `algorithm` on `view`: rows in ordinal order (by level, then ordinal, for `leiden`).
///
/// # Errors
///
/// What the algorithm refuses ([`GraphError::InvalidValue`]).
pub fn compute(view: &AdjacencyIndex, algorithm: &Algorithm) -> Result<Vec<Scored>, GraphError> {
    let element = |ordinal: usize| view.vertices[ordinal].id;
    Ok(match algorithm {
        Algorithm::PageRank(params) => pagerank(view, params)?
            .into_iter()
            .map(|(ordinal, rank)| Scored {
                ordinal,
                element: element(ordinal),
                score: Score::Rank(rank),
            })
            .collect(),
        Algorithm::Wcc { edge_types } => wcc(view, edge_types)
            .into_iter()
            .map(|(ordinal, component)| Scored {
                ordinal,
                element: element(ordinal),
                score: Score::Component(component),
            })
            .collect(),
        Algorithm::Leiden(params) => leiden(view, params)?
            .into_iter()
            .map(|community| Scored {
                ordinal: community.ordinal,
                element: element(community.ordinal),
                score: Score::Community(community),
            })
            .collect(),
    })
}

/// An algorithm's answer on a graph: the view it ran on and its rows.
#[derive(Debug, Clone)]
pub struct AlgorithmAnswer {
    /// The pinned view.
    pub pinned: crate::adjacency::PinnedAdjacency,
    /// The rows.
    pub rows: Vec<Scored>,
}

/// Where an algorithm's write-back patches a LINKED graph's vertex documents (§07 §5.3: "a job
/// that patches the vertex source"): the server's collections ([`loams_query::CollectionService`]).
#[async_trait::async_trait]
pub trait DocumentPatcher: Send + Sync + std::fmt::Debug {
    /// Sets field `field` of each document `(primary key, value)` of collection `collection` in
    /// `namespace`, in one atomic write (a document that is gone is left alone).
    ///
    /// # Errors
    ///
    /// A message for the job's answer when the write is refused or fails.
    async fn patch(
        &self,
        namespace: &str,
        collection: &str,
        field: &str,
        values: Vec<(PrimaryKey, serde_json::Value)>,
    ) -> Result<(), String>;
}

#[async_trait::async_trait]
impl DocumentPatcher for loams_query::CollectionService {
    async fn patch(
        &self,
        namespace: &str,
        collection: &str,
        field: &str,
        values: Vec<(PrimaryKey, serde_json::Value)>,
    ) -> Result<(), String> {
        let ops = values
            .into_iter()
            .map(|(pk, value)| loams_collection::DocOp::Patch {
                pk,
                mode: loams_collection::PatchMode::MergeTop,
                source: [(field.to_string(), value)].into_iter().collect(),
                delete_keys: Vec::new(),
                vectors: BTreeMap::new(),
                sparse_vectors: BTreeMap::new(),
                upsert: None,
            })
            .collect();
        let options = loams_query::WriteOptions {
            atomic: true,
            ..Default::default()
        };
        self.write(namespace, collection, ops, options)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

/// A write-back job (§07 §5.3): the algorithm, and the vertex field its answer is written to.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteBack {
    /// What runs.
    pub algorithm: Algorithm,
    /// The field written: `pagerank`'s rank (a float), `wcc`'s component or `leiden`'s
    /// community at each vertex's deepest level answered (integers).
    pub field: String,
}

/// What a write-back did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBackOutcome {
    /// The offset the algorithm ran at.
    pub offset: u64,
    /// Vertices written.
    pub written: u64,
    /// For an OWNED graph: the offset after its one change set (the job's consistency token);
    /// `None` for a LINKED graph, whose links carry the documents' patches back.
    pub committed: Option<u64>,
    /// For a LINKED graph: the collections patched, one atomic write each.
    pub collections: Vec<String>,
}

/// The value written for a score.
fn written(score: &Score) -> (grafeo::Value, serde_json::Value) {
    match score {
        Score::Rank(rank) => (grafeo::Value::Float64(*rank), serde_json::json!(rank)),
        Score::Component(c) => (
            grafeo::Value::Int64(i64::try_from(*c).unwrap_or(i64::MAX)),
            serde_json::json!(c),
        ),
        Score::Community(c) => (
            grafeo::Value::Int64(i64::try_from(c.community).unwrap_or(i64::MAX)),
            serde_json::json!(c.community),
        ),
    }
}

/// An algorithm run's lease key: one write-back per graph at a time.
fn job_key(graph: crate::GraphId) -> String {
    format!("graph-algorithm/{graph}")
}

impl GraphAdmin {
    /// Runs `algorithm` on graph `namespace`/`name` (opened here, its owner) at `consistency`,
    /// on its adjacency index (§48 §26.3; R22.4).
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` when graphs are not durable in this server, what opening the graph
    /// answers, `INVALID_ARGUMENT` for parameters the algorithm refuses, and `UNAVAILABLE` when
    /// the log, the bucket or the metastore fails.
    pub async fn run_algorithm(
        &self,
        namespace: &str,
        name: &str,
        algorithm: &Algorithm,
        consistency: &crate::consistency::Consistency,
    ) -> Result<AlgorithmAnswer, ConnectError> {
        let Some(indexer) = self.indexer() else {
            return Err(refuse(
                ErrorCode::FailedPrecondition,
                "failed_precondition",
                "graph algorithms run on the adjacency index, which needs durable graphs",
            ));
        };
        let graph = self.open(namespace, name).await?;
        let ns = self.namespace_of(namespace, name).await?;
        let pinned = indexer
            .pin(graph, ns, consistency, self.consistency_wait())
            .await
            .map_err(|err| map_engine(err.into()))?;
        let view = Arc::clone(&pinned.view);
        let algorithm = algorithm.clone();
        let rows = tokio::task::spawn_blocking(move || compute(&view, &algorithm))
            .await
            .map_err(|_| map_engine(GraphError::EnginePanic))?
            .map_err(map_engine)?;
        Ok(AlgorithmAnswer { pinned, rows })
    }

    /// Runs `job` and writes its answer back (§07 §5.3), as one `loams-worker` task under the
    /// lease `task/graph-algorithm/<gr_id>`, so one write-back of a graph runs at a time:
    ///
    /// * an OWNED graph gets the field as a property of every vertex answered, in **one change
    ///   set** through its write lane: all of it is durable, or none;
    /// * a LINKED graph's vertex documents (`_collection`, `_pk` or `_key`) get the field, one
    ///   atomic write per collection; its links bring the values into the graph.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a field that is empty or starts with `_` (the links' own
    /// properties), `FAILED_PRECONDITION` for a LINKED graph when this server has no
    /// collections to patch, `RESOURCE_EXHAUSTED` when another write-back of the graph holds
    /// the lease, or what [`GraphAdmin::run_algorithm`] and the writes answer.
    pub async fn write_back(
        &self,
        namespace: &str,
        name: &str,
        job: &WriteBack,
    ) -> Result<WriteBackOutcome, ConnectError> {
        if job.field.is_empty() || job.field.starts_with('_') {
            return Err(refuse(
                ErrorCode::InvalidArgument,
                "invalid_argument",
                "a write-back's field is a name that does not start with '_'",
            ));
        }
        let meta = self
            .catalog()
            .get_by_name(namespace, name)
            .await
            .map_err(map_catalog)?;
        let id = meta.graph_id().map_err(map_catalog)?;
        let Some(log) = self.log() else {
            return Err(refuse(
                ErrorCode::FailedPrecondition,
                "failed_precondition",
                "write-backs need durable graphs",
            ));
        };
        let task = Arc::new(WriteBackTask {
            admin: self.clone(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            job: job.clone(),
            outcome: std::sync::Mutex::new(None),
        });
        let source = OneTask {
            key: loams_worker::TaskKey::cluster(job_key(id)),
            task: Arc::clone(&task),
        };
        let owner = format!("graph-write-back/{}", ulid::Ulid::generate());
        let ran = loams_worker::run_once(
            Arc::clone(log.meta()),
            &owner,
            std::time::Duration::from_secs(30),
            &source,
        )
        .await
        .map_err(|err| refuse(ErrorCode::Unavailable, "unavailable", err.to_string()))?;
        match ran.into_iter().next().map(|(_, result)| result) {
            Some(loams_worker::RunResult::LeaseHeld) => Err(refuse(
                ErrorCode::ResourceExhausted,
                "write_back_running",
                format!("another write-back of graph {name} is running; retry when it ends"),
            )),
            _ => task
                .outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| {
                    Err(refuse(
                        ErrorCode::Unavailable,
                        "unavailable",
                        "the write-back did not run; retry",
                    ))
                }),
        }
    }

    /// The body of a write-back, under its task lease.
    async fn write_back_now(
        &self,
        namespace: &str,
        name: &str,
        job: &WriteBack,
    ) -> Result<WriteBackOutcome, ConnectError> {
        let meta = self
            .catalog()
            .get_by_name(namespace, name)
            .await
            .map_err(map_catalog)?;
        let answer = self
            .run_algorithm(
                namespace,
                name,
                &job.algorithm,
                &crate::consistency::Consistency::Strong,
            )
            .await?;
        // One value per vertex: a community at the deepest level answered.
        let rows: Vec<&Scored> = answer
            .rows
            .iter()
            .filter(|row| !matches!(row.score, Score::Community(c) if !c.last))
            .collect();
        match meta.mode {
            crate::catalog::GraphMode::Owned => {
                let ops: Vec<crate::changeset::ChangeOp> = rows
                    .iter()
                    .map(|row| crate::changeset::ChangeOp::SetProps {
                        id: row.element,
                        kind: crate::changeset::ElementKind::Node,
                        set: [(job.field.clone(), written(&row.score).0)]
                            .into_iter()
                            .collect(),
                        removed: Vec::new(),
                    })
                    .collect();
                let written_count = ops.len() as u64;
                let graph = self.open(namespace, name).await?;
                let committed = tokio::task::spawn_blocking(move || {
                    graph.commit_link(crate::durable::lane::LinkJob {
                        progress: None,
                        apply: Box::new(move |db, _| {
                            // A vertex deleted since the pin is skipped, not an error (which
                            // would reload the graph).
                            let store = db.store();
                            let ops: Vec<_> = ops
                                .into_iter()
                                .filter(|op| match op {
                                    crate::changeset::ChangeOp::SetProps { id, .. } => {
                                        store.get_node(grafeo::NodeId::new(*id)).is_some()
                                    }
                                    _ => true,
                                })
                                .collect();
                            crate::changeset::apply_to_store(db, &ops)?;
                            Ok(ops)
                        }),
                    })
                })
                .await
                .map_err(|_| map_engine(GraphError::EnginePanic))?
                .map_err(map_engine)?;
                Ok(WriteBackOutcome {
                    offset: answer.pinned.offset,
                    written: written_count,
                    committed: Some(committed),
                    collections: Vec::new(),
                })
            }
            crate::catalog::GraphMode::Linked => {
                let Some(documents) = self.documents() else {
                    return Err(refuse(
                        ErrorCode::FailedPrecondition,
                        "failed_precondition",
                        "a LINKED graph's write-back patches its collections, which this server \
                         does not serve",
                    ));
                };
                let graph = self.open(namespace, name).await?;
                let elements: Vec<(u64, serde_json::Value)> = rows
                    .iter()
                    .map(|row| (row.element, written(&row.score).1))
                    .collect();
                let sources = tokio::task::spawn_blocking(move || document_keys(&graph, &elements))
                    .await
                    .map_err(|_| map_engine(GraphError::EnginePanic))?
                    .map_err(map_engine)?;
                let mut written_count = 0;
                let mut collections = Vec::new();
                for (collection, values) in sources {
                    written_count += values.len() as u64;
                    documents
                        .patch(namespace, &collection, &job.field, values)
                        .await
                        .map_err(|message| {
                            refuse(ErrorCode::Unavailable, "unavailable", message)
                        })?;
                    collections.push(collection);
                }
                Ok(WriteBackOutcome {
                    offset: answer.pinned.offset,
                    written: written_count,
                    committed: None,
                    collections,
                })
            }
        }
    }
}

/// The source documents of `elements` (vertex ids and values), by collection: a vertex's
/// `_collection` and its `_pk` (or `_key`), read under the gate. Stubs and stream vertices (no
/// `_collection`) have no document and are skipped.
fn document_keys(
    graph: &crate::engine::Graph,
    elements: &[(u64, serde_json::Value)],
) -> Result<BTreeMap<String, Vec<(PrimaryKey, serde_json::Value)>>, GraphError> {
    use crate::link::{COLLECTION_PROPERTY, KEY_PROPERTY, PK_PROPERTY};
    graph.read_call(|| {
        let store = graph.db().store();
        let mut out: BTreeMap<String, Vec<(PrimaryKey, serde_json::Value)>> = BTreeMap::new();
        for (id, value) in elements {
            let Some(node) = store.get_node(grafeo::NodeId::new(*id)) else {
                continue;
            };
            let prop = |name: &str| {
                node.properties
                    .iter()
                    .find(|(k, _)| k.as_str() == name)
                    .map(|(_, v)| v.clone())
            };
            let Some(grafeo::Value::String(collection)) = prop(COLLECTION_PROPERTY) else {
                continue;
            };
            let Some(pk) = prop(PK_PROPERTY)
                .or_else(|| prop(KEY_PROPERTY))
                .as_ref()
                .and_then(crate::adjacency::VertexKey::of)
                .map(|key| key.primary_key())
            else {
                continue;
            };
            out.entry(collection.to_string())
                .or_default()
                .push((pk, value.clone()));
        }
        Ok(out)
    })
}

/// A write-back as a `loams-worker` task.
struct WriteBackTask {
    admin: GraphAdmin,
    namespace: String,
    name: String,
    job: WriteBack,
    outcome: std::sync::Mutex<Option<Result<WriteBackOutcome, ConnectError>>>,
}

#[async_trait::async_trait]
impl loams_worker::Task for WriteBackTask {
    async fn run(
        &self,
        _ctx: loams_worker::TaskContext,
    ) -> Result<loams_worker::TaskOutcome, loams_worker::TaskError> {
        let outcome = self
            .admin
            .write_back_now(&self.namespace, &self.name, &self.job)
            .await;
        *self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
        Ok(loams_worker::TaskOutcome::Done)
    }
}

/// A task source proposing one task once.
struct OneTask {
    key: loams_worker::TaskKey,
    task: Arc<WriteBackTask>,
}

#[async_trait::async_trait]
impl loams_worker::TaskSource for OneTask {
    fn priority(&self) -> loams_worker::Priority {
        loams_worker::Priority::Maintenance
    }

    async fn candidates(
        &self,
        _meta: &dyn loams_common::meta::MetaStore,
    ) -> Result<Vec<loams_worker::Candidate>, loams_worker::TaskError> {
        Ok(vec![(
            self.key.clone(),
            Arc::clone(&self.task) as Arc<dyn loams_worker::Task>,
        )])
    }
}
