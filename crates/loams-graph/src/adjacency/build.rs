//! The two ways an adjacency index is made (GR1 Task 22; design §48 §26.3 steps 3 and 4):
//!
//! * [`build_full`]: a scan of the engine under the graph's gate, held shared, as the
//!   checkpointer's take holds it (R13.4): client reads go on, the write lane waits, and the
//!   durable offset read under the same hold is exactly what the scan covers.
//! * [`patch`]: the previous index plus the graph's own change sets `[N, M)`, decoded from its
//!   stream. GQL writes and link commits are the same records, so one decoder serves owned and
//!   linked graphs. `UpsertNode`, `DeleteNode`, `UpsertEdge`, `DeleteEdge`, and a `SetProps`
//!   that touches a vertex's `_key` or an edge's `weight`, change the topology; every other
//!   operation and every `SchemaChange` is skipped.
//!
//! Both end in [`AdjacencyIndex::from_topology`], the one canonical encoder, so a patched index
//! and a full build of the same state are the same bytes (`incremental_build_equals_full_build`).
//! A patch that cannot be made is a [`Decline`], not an error: the caller builds in full and
//! counts the reason.

use grafeo::GrafeoDB;
use loams_log::OffsetRecord;

use super::format::{
    AdjacencyIndex, EdgeState, FormatError, Topology, VertexKey, VertexState, WEIGHT_PROPERTY,
    weight_of,
};
use crate::changeset::{ChangeOp, CodecError, ElementKind, GraphRecord, decode};
use crate::engine::{Graph, GraphError};
use crate::link::KEY_PROPERTY;

/// Why an index was not patched, and so was built in full (§48 §26.3 step 4). Control flow, not
/// failure: each is counted by [`super::AdjacencyStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Decline {
    /// The graph has no index yet.
    NoIndex,
    /// The stream no longer holds `[N, M)`: it was trimmed past the index's offset.
    Trimmed,
    /// The tail is over `incremental_max_records`, or its topology operations over
    /// `incremental_max_fraction` of the index's edges (or, for a reader, over
    /// `overlay_max_records`).
    TailTooLong,
    /// A record, or the index file, of a version this build does not read.
    UnknownVersion,
    /// The index file is gone or does not check (its SHA-256 or CRC-32C), or the tail does not
    /// apply to it.
    HashMismatch,
}

impl Decline {
    /// The reason's name, as the counters label it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoIndex => "no_index",
            Self::Trimmed => "trimmed",
            Self::TailTooLong => "tail_too_long",
            Self::UnknownVersion => "unknown_version",
            Self::HashMismatch => "hash_mismatch",
        }
    }

    /// Every reason, in order.
    pub const ALL: [Decline; 5] = [
        Self::NoIndex,
        Self::Trimmed,
        Self::TailTooLong,
        Self::UnknownVersion,
        Self::HashMismatch,
    ];
}

impl From<FormatError> for Decline {
    fn from(err: FormatError) -> Self {
        match err {
            FormatError::UnknownVersion(_) => Self::UnknownVersion,
            _ => Self::HashMismatch,
        }
    }
}

/// The topology of `db`'s store. The caller holds the graph's gate (shared), so it is the
/// durable state. Blocks.
#[must_use]
pub fn scan(db: &GrafeoDB) -> Topology {
    let store = db.store();
    let mut topology = Topology::default();
    for node in store.all_nodes() {
        let key = node
            .properties
            .iter()
            .find(|(k, _)| k.as_str() == KEY_PROPERTY)
            .and_then(|(_, v)| VertexKey::of(v));
        topology.vertices.insert(
            node.id.as_u64(),
            VertexState {
                labels: node.labels.iter().map(ToString::to_string).collect(),
                key,
            },
        );
    }
    for edge in store.all_edges() {
        let weight = weight_of(
            edge.properties
                .iter()
                .find(|(k, _)| k.as_str() == WEIGHT_PROPERTY)
                .map(|(_, v)| v),
        );
        topology.edges.insert(
            edge.id.as_u64(),
            EdgeState {
                ty: edge.edge_type.to_string(),
                src: edge.src.as_u64(),
                dst: edge.dst.as_u64(),
                weight,
            },
        );
    }
    topology
}

/// A full build of `graph` (open here), at its durable offset, read under its gate held shared:
/// the index and the number of edges scanned. Blocks: run it off the runtime's threads.
///
/// # Errors
///
/// What [`Graph::rows`] answers for a poisoned or panicking engine, or
/// [`GraphError::Engine`] for a store whose edges name missing vertices.
pub fn build_full(graph: &Graph) -> Result<(AdjacencyIndex, u64), GraphError> {
    let id = graph
        .id()
        .ok_or_else(|| GraphError::engine("a graph without an id has no adjacency index"))?;
    let (offset, topology) = graph.read_call(|| {
        let offset = graph
            .durable_state()
            .map_or(0, |durable| durable.position.offset());
        Ok((offset, scan(graph.db())))
    })?;
    let scanned = topology.edges.len() as u64;
    let index = AdjacencyIndex::from_topology(id, offset, &topology)
        .map_err(|err| GraphError::engine(format!("building the adjacency index: {err}")))?;
    Ok((index, scanned))
}

/// A decoded tail: the graph's records `[from, to)`.
#[derive(Debug, Clone)]
pub struct Tail {
    /// The first offset.
    pub from: u64,
    /// The offset after the last.
    pub to: u64,
    /// The records, in offset order.
    pub records: Vec<GraphRecord>,
}

impl Tail {
    /// Decodes `records`, which must be exactly the offsets `[from, to)`.
    ///
    /// # Errors
    ///
    /// [`Decline::Trimmed`] when an offset is missing, [`Decline::UnknownVersion`] for a record
    /// of a version this build does not read, [`Decline::HashMismatch`] for one that does not
    /// decode.
    pub fn decode(from: u64, to: u64, records: &[OffsetRecord]) -> Result<Self, Decline> {
        let mut decoded = Vec::with_capacity(records.len());
        let mut next = from;
        for record in records {
            if record.offset != next {
                return Err(Decline::Trimmed);
            }
            next += 1;
            let value = record.record.value.as_deref().unwrap_or_default();
            match decode(value) {
                Ok(record) => decoded.push(record),
                Err(CodecError::UnknownVersion(_)) => return Err(Decline::UnknownVersion),
                Err(_) => return Err(Decline::HashMismatch),
            }
        }
        if next != to {
            return Err(Decline::Trimmed);
        }
        Ok(Self {
            from,
            to,
            records: decoded,
        })
    }

    /// The operations that change the topology.
    #[must_use]
    pub fn topology_ops(&self) -> u64 {
        self.ops().filter(|op| changes_topology(op)).count() as u64
    }

    /// The operations on edges ([`super::AdjacencyStats`]' delta edges).
    #[must_use]
    pub fn edge_ops(&self) -> u64 {
        self.ops().filter(|op| is_edge_op(op)).count() as u64
    }

    fn ops(&self) -> impl Iterator<Item = &ChangeOp> {
        self.records.iter().flat_map(|record| match record {
            GraphRecord::ChangeSet(change) => change.ops.as_slice(),
            GraphRecord::Schema(_) => &[],
        })
    }
}

fn changes_topology(op: &ChangeOp) -> bool {
    match op {
        ChangeOp::SetProps {
            kind, set, removed, ..
        } => {
            let name = match kind {
                ElementKind::Node => KEY_PROPERTY,
                ElementKind::Edge => WEIGHT_PROPERTY,
            };
            set.contains_key(name) || removed.iter().any(|r| r == name)
        }
        _ => true,
    }
}

fn is_edge_op(op: &ChangeOp) -> bool {
    match op {
        ChangeOp::UpsertEdge { .. } | ChangeOp::DeleteEdge { .. } => true,
        ChangeOp::SetProps {
            kind: ElementKind::Edge,
            ..
        } => changes_topology(op),
        _ => false,
    }
}

/// A patched index, and what the patch applied.
#[derive(Debug, Clone)]
pub struct Patched {
    /// The index at the tail's end.
    pub index: AdjacencyIndex,
    /// The records applied.
    pub records: u64,
    /// The edge operations applied.
    pub delta_edges: u64,
}

/// Applies `tail` to `topology` in order, as replay applies it to an engine
/// ([`crate::changeset::apply_to_store`]): an upsert replaces, a delete of an absent element is
/// nothing.
///
/// # Errors
///
/// [`Decline::HashMismatch`] when an operation names an element that must exist and does not:
/// the index and the stream disagree.
pub fn apply(topology: &mut Topology, tail: &Tail) -> Result<(), Decline> {
    for op in tail.ops() {
        match op {
            ChangeOp::UpsertNode { id, labels, props } => {
                topology.vertices.insert(
                    *id,
                    VertexState {
                        labels: labels.iter().cloned().collect(),
                        key: props.get(KEY_PROPERTY).and_then(VertexKey::of),
                    },
                );
            }
            ChangeOp::DeleteNode { id } => {
                topology.vertices.remove(id);
            }
            ChangeOp::UpsertEdge {
                id,
                ty,
                src,
                dst,
                props,
            } => {
                topology.edges.insert(
                    *id,
                    EdgeState {
                        ty: ty.clone(),
                        src: *src,
                        dst: *dst,
                        weight: weight_of(props.get(WEIGHT_PROPERTY)),
                    },
                );
            }
            ChangeOp::DeleteEdge { id } => {
                topology.edges.remove(id);
            }
            ChangeOp::SetProps {
                id,
                kind: ElementKind::Node,
                set,
                removed,
            } => {
                let vertex = topology.vertices.get_mut(id).ok_or(Decline::HashMismatch)?;
                if removed.iter().any(|r| r == KEY_PROPERTY) {
                    vertex.key = None;
                }
                if let Some(value) = set.get(KEY_PROPERTY) {
                    vertex.key = VertexKey::of(value);
                }
            }
            ChangeOp::SetProps {
                id,
                kind: ElementKind::Edge,
                set,
                removed,
            } => {
                let edge = topology.edges.get_mut(id).ok_or(Decline::HashMismatch)?;
                if removed.iter().any(|r| r == WEIGHT_PROPERTY) {
                    edge.weight = 1.0;
                }
                if let Some(value) = set.get(WEIGHT_PROPERTY) {
                    edge.weight = weight_of(Some(value));
                }
            }
        }
    }
    Ok(())
}

/// The index `prev` patched with `tail`, which must start at `prev`'s offset (shared contract
/// `patch(prev, tail)`).
///
/// # Errors
///
/// [`Decline::Trimmed`] when the tail does not start at `prev`'s offset, or what [`apply`]
/// answers.
pub fn patch(prev: &AdjacencyIndex, tail: &Tail) -> Result<Patched, Decline> {
    if tail.from != prev.covered_offset {
        return Err(Decline::Trimmed);
    }
    let mut topology = prev.topology();
    apply(&mut topology, tail)?;
    let index = AdjacencyIndex::from_topology(prev.graph, tail.to, &topology)?;
    Ok(Patched {
        index,
        records: tail.records.len() as u64,
        delta_edges: tail.edge_ops(),
    })
}
