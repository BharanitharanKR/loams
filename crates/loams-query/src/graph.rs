//! The graph table functions' backend (GR1 Task 21; design §07 §5.1, §48 §10.3; D743).
//!
//! `graph_neighbors`, `graph_degree` and `graph_shortest_path` read a Loams Graph graph through a
//! [`GraphTables`], which the server hands the service when it runs graphs
//! ([`crate::CollectionService::set_graph_tables`]); `graph_expand` runs on the
//! [`GraphExpander`](crate::expand::GraphExpander) the `expand` stage uses.

use async_trait::async_trait;
use loams_collection::PrimaryKey;

use crate::error::ServiceError;
use crate::expand::ExpandDirection;
use crate::ir::Query;

/// The most hops `graph_shortest_path` searches.
pub const MAX_PATH_HOPS: u8 = 16;

/// `graph_shortest_path`'s default depth bound (§07 §4).
pub const DEFAULT_PATH_HOPS: u8 = 6;

/// `graph_neighbors`' default `limit_per_key`.
pub const DEFAULT_LIMIT_PER_KEY: usize = 100;

/// One incident edge of a vertex (`graph_neighbors`' row).
#[derive(Clone, Debug, PartialEq)]
pub struct IncidentEdge {
    /// The vertex's key, as the call named it.
    pub key: PrimaryKey,
    /// `Out` when the vertex is the edge's source, `In` when it is its destination.
    pub direction: ExpandDirection,
    pub edge_type: String,
    pub edge: PrimaryKey,
    pub other_label: String,
    pub other_key: PrimaryKey,
}

/// One vertex of a shortest path (`graph_shortest_path`'s row).
#[derive(Clone, Debug, PartialEq)]
pub struct PathStep {
    /// 0 for `from`.
    pub step: u32,
    pub label: String,
    pub key: PrimaryKey,
    /// The edge that reached this vertex; `None` for `from`.
    pub edge_type: Option<String>,
    pub edge: Option<PrimaryKey>,
}

/// `graph_neighbors`' call.
#[derive(Clone, Debug)]
pub struct NeighborsCall<'a> {
    pub namespace: &'a str,
    pub graph: &'a str,
    pub keys: Vec<PrimaryKey>,
    pub label: Option<String>,
    pub direction: ExpandDirection,
    pub edge_types: Vec<String>,
    pub edge_filter: Option<Query>,
    pub limit_per_key: usize,
}

/// `graph_degree`'s call.
#[derive(Clone, Debug)]
pub struct DegreeCall<'a> {
    pub namespace: &'a str,
    pub graph: &'a str,
    pub keys: Vec<PrimaryKey>,
    pub label: Option<String>,
    pub direction: ExpandDirection,
    pub edge_types: Vec<String>,
}

/// `graph_shortest_path`'s call.
#[derive(Clone, Debug)]
pub struct PathCall<'a> {
    pub namespace: &'a str,
    pub graph: &'a str,
    pub from: PrimaryKey,
    pub to: PrimaryKey,
    pub label: Option<String>,
    pub direction: ExpandDirection,
    pub edge_types: Vec<String>,
    pub max_hops: u8,
}

/// Reads a graph for the graph table functions: Loams Graph's (`loams-graph`), when the server
/// runs it.
#[async_trait]
pub trait GraphTables: Send + Sync + std::fmt::Debug {
    /// Each key's incident edges, keys in call order, each key's edges ordered by direction
    /// (out first), type, the other end's canonical key bytes and the edge's; at most
    /// `limit_per_key` per key.
    async fn neighbors(&self, call: NeighborsCall<'_>) -> Result<Vec<IncidentEdge>, ServiceError>;

    /// Each key's degree, keys in call order (each once); a key with no vertex has no row.
    async fn degree(&self, call: DegreeCall<'_>) -> Result<Vec<(PrimaryKey, u64)>, ServiceError>;

    /// A shortest path from `from` to `to` of at most `max_hops` edges, empty when there is none:
    /// the one whose vertices, then edges, have the smallest canonical key bytes at each step.
    async fn shortest_path(&self, call: PathCall<'_>) -> Result<Vec<PathStep>, ServiceError>;
}
