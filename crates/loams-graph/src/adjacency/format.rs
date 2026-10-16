//! `loams.graph.adjacency` version 1: the canonical byte form of an [`AdjacencyIndex`] (GR1 Task
//! 22; design §48 §26.3; specified in `conformance/graph/adjacency/v1/README.md` and frozen by
//! its golden vectors).
//!
//! **Canonical.** One graph state at one offset has exactly one encoding:
//!
//! * vertices are in element-id order, and a vertex's ordinal is its rank in that order;
//! * the edge-type and label tables hold exactly the names in use, in UTF-8 byte order, so a
//!   type id or a label id is a rank too;
//! * each vertex's run, in the CSR (out-edges) and the CSC (in-edges), is sorted by edge type,
//!   then the other end's ordinal, then the edge id.
//!
//! So a patched index and a full build of the same state encode to the same bytes and the same
//! SHA-256 ([`AdjacencyIndex::encode`]), and a decoder refuses every other byte string
//! ([`AdjacencyIndex::decode`]): unordered tables or runs, an unused name, a CSC that is not the
//! CSR's transpose, a bad CRC-32C or trailing bytes. Decoding is total (it never panics) and
//! bounded (every count is checked against the bytes left before anything is allocated).

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::GraphId;

/// The file's first eight bytes.
pub const MAGIC: [u8; 8] = *b"LOAMSADJ";

/// The format version this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// The format's name, as a manifest entry records it.
pub const FORMAT_NAME: &str = "loams.graph.adjacency/1";

/// A vertex's `_key`, as the index keeps it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VertexKey {
    /// An integer key.
    Int(i64),
    /// A string key.
    Str(String),
}

impl VertexKey {
    /// The key as the engine value `value` holds it: an integer or a string; other values are no
    /// key.
    #[must_use]
    pub fn of(value: &grafeo::Value) -> Option<Self> {
        match value {
            grafeo::Value::Int64(n) => Some(Self::Int(*n)),
            grafeo::Value::String(s) => Some(Self::Str(s.to_string())),
            _ => None,
        }
    }

    /// The key as a document primary key (a non-negative integer, else its text), as
    /// `Search.expand` reads `_key` (R20.4).
    #[must_use]
    pub fn primary_key(&self) -> loams_collection::PrimaryKey {
        match self {
            Self::Int(n) => u64::try_from(*n).map_or_else(
                |_| loams_collection::PrimaryKey::Str(n.to_string()),
                loams_collection::PrimaryKey::U64,
            ),
            Self::Str(s) => loams_collection::PrimaryKey::Str(s.clone()),
        }
    }

    /// The key's text, as the graph table functions answer keys (R21.4).
    #[must_use]
    pub fn text(&self) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Str(s) => s.clone(),
        }
    }
}

/// An edge's weight from its `weight` property: a finite number, else 1 (R22.5).
#[must_use]
pub fn weight_of(value: Option<&grafeo::Value>) -> f64 {
    match value {
        Some(grafeo::Value::Int64(n)) => *n as f64,
        Some(grafeo::Value::Float64(x)) if x.is_finite() => *x,
        _ => 1.0,
    }
}

/// The edge property an edge's weight is read from (§07 §5.3's `weight => 'weight'`).
pub const WEIGHT_PROPERTY: &str = "weight";

/// One vertex of the index.
#[derive(Debug, Clone, PartialEq)]
pub struct Vertex {
    /// Its element id.
    pub id: u64,
    /// Its labels, as ids into [`AdjacencyIndex::labels`], ascending.
    pub labels: Vec<u32>,
    /// Its `_key`, when it has one.
    pub key: Option<VertexKey>,
}

/// One entry of a vertex's run in the CSR or the CSC.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Adj {
    /// The edge's type, as an id into [`AdjacencyIndex::edge_types`].
    pub edge_type: u32,
    /// The other end's ordinal: the destination in the CSR, the source in the CSC.
    pub other: u32,
    /// The edge's element id.
    pub edge: u64,
    /// The edge's weight (R22.5). The CSC's entries carry the CSR's.
    pub weight: f64,
}

/// A compressed sparse row: vertex `v`'s run is `entries[offsets[v]..offsets[v + 1]]`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Csr {
    /// `vertices + 1` offsets, from 0 to the edge count.
    pub offsets: Vec<u64>,
    /// Every edge, run by run.
    pub entries: Vec<Adj>,
}

impl Csr {
    /// Vertex `v`'s run.
    #[must_use]
    pub fn run(&self, v: usize) -> &[Adj] {
        let start = self.offsets[v] as usize;
        let end = self.offsets[v + 1] as usize;
        &self.entries[start..end]
    }
}

/// A graph's adjacency at one offset of its stream (shared contract `AdjacencyIndex`).
#[derive(Debug, Clone, PartialEq)]
pub struct AdjacencyIndex {
    /// The graph.
    pub graph: GraphId,
    /// Every record of the graph's stream below this offset is in the index.
    pub covered_offset: u64,
    /// The edge-type table, in UTF-8 byte order.
    pub edge_types: Vec<String>,
    /// The label table, in UTF-8 byte order.
    pub labels: Vec<String>,
    /// Every vertex, in element-id order.
    pub vertices: Vec<Vertex>,
    /// Out-edges.
    pub csr: Csr,
    /// In-edges.
    pub csc: Csr,
}

/// A vertex as the builds see it, before ordinals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VertexState {
    /// Its labels.
    pub labels: BTreeSet<String>,
    /// Its `_key`.
    pub key: Option<VertexKey>,
}

/// An edge as the builds see it, before ordinals.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeState {
    /// Its type.
    pub ty: String,
    /// Its source's element id.
    pub src: u64,
    /// Its destination's element id.
    pub dst: u64,
    /// Its weight (R22.5).
    pub weight: f64,
}

/// A graph's topology by element id: what a full build reads from the engine, and what a patch
/// changes ([`super::build`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Topology {
    /// Every vertex.
    pub vertices: BTreeMap<u64, VertexState>,
    /// Every edge.
    pub edges: BTreeMap<u64, EdgeState>,
}

/// Why bytes are not a version-1 index.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    /// Not an adjacency index at all.
    #[error("not a Loams Graph adjacency index")]
    Magic,
    /// A version this build does not read.
    #[error("adjacency index version {0} is not readable by this build, which reads version 1")]
    UnknownVersion(u32),
    /// The input ended early, or a count promised more than it holds.
    #[error("adjacency index truncated at byte {0}")]
    Truncated(usize),
    /// The CRC-32C does not match.
    #[error("adjacency index checksum mismatch")]
    Checksum,
    /// Bytes that are not the canonical encoding.
    #[error("malformed adjacency index at byte {at}: {what}")]
    Malformed {
        /// Where.
        at: usize,
        /// What is wrong.
        what: String,
    },
    /// A topology the format cannot hold: an edge naming a missing vertex, or more vertices or
    /// names than 32 bits count.
    #[error("{0}")]
    Inconsistent(String),
}

impl AdjacencyIndex {
    /// The canonical index of `topology` at `covered_offset`.
    ///
    /// # Errors
    ///
    /// [`FormatError::Inconsistent`] when an edge names a vertex the topology does not hold.
    pub fn from_topology(
        graph: GraphId,
        covered_offset: u64,
        topology: &Topology,
    ) -> Result<Self, FormatError> {
        let too_many = |what: &str| FormatError::Inconsistent(format!("too many {what}"));
        let labels: Vec<String> = topology
            .vertices
            .values()
            .flat_map(|v| v.labels.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let edge_types: Vec<String> = topology
            .edges
            .values()
            .map(|e| e.ty.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let label_id: BTreeMap<&str, u32> = labels
            .iter()
            .enumerate()
            .map(|(i, l)| {
                Ok((
                    l.as_str(),
                    u32::try_from(i).map_err(|_| too_many("labels"))?,
                ))
            })
            .collect::<Result<_, FormatError>>()?;
        let type_id: BTreeMap<&str, u32> = edge_types
            .iter()
            .enumerate()
            .map(|(i, t)| Ok((t.as_str(), u32::try_from(i).map_err(|_| too_many("types"))?)))
            .collect::<Result<_, FormatError>>()?;
        let mut ordinal: BTreeMap<u64, u32> = BTreeMap::new();
        let mut vertices = Vec::with_capacity(topology.vertices.len());
        for (i, (id, state)) in topology.vertices.iter().enumerate() {
            ordinal.insert(*id, u32::try_from(i).map_err(|_| too_many("vertices"))?);
            vertices.push(Vertex {
                id: *id,
                labels: state.labels.iter().map(|l| label_id[l.as_str()]).collect(),
                key: state.key.clone(),
            });
        }
        let n = vertices.len();
        let mut out: Vec<Vec<Adj>> = vec![Vec::new(); n];
        let mut inc: Vec<Vec<Adj>> = vec![Vec::new(); n];
        for (id, edge) in &topology.edges {
            let end = |vertex: u64| {
                ordinal.get(&vertex).copied().ok_or_else(|| {
                    FormatError::Inconsistent(format!("edge {id} names missing vertex {vertex}"))
                })
            };
            let (src, dst) = (end(edge.src)?, end(edge.dst)?);
            let edge_type = type_id[edge.ty.as_str()];
            out[src as usize].push(Adj {
                edge_type,
                other: dst,
                edge: *id,
                weight: edge.weight,
            });
            inc[dst as usize].push(Adj {
                edge_type,
                other: src,
                edge: *id,
                weight: edge.weight,
            });
        }
        Ok(Self {
            graph,
            covered_offset,
            edge_types,
            labels,
            vertices,
            csr: compress(out),
            csc: compress(inc),
        })
    }

    /// The topology the index holds, by element id: what a patch starts from.
    #[must_use]
    pub fn topology(&self) -> Topology {
        let mut topology = Topology::default();
        for vertex in &self.vertices {
            topology.vertices.insert(
                vertex.id,
                VertexState {
                    labels: vertex
                        .labels
                        .iter()
                        .map(|l| self.labels[*l as usize].clone())
                        .collect(),
                    key: vertex.key.clone(),
                },
            );
        }
        for (src, vertex) in self.vertices.iter().enumerate() {
            for adj in self.csr.run(src) {
                topology.edges.insert(
                    adj.edge,
                    EdgeState {
                        ty: self.edge_types[adj.edge_type as usize].clone(),
                        src: vertex.id,
                        dst: self.vertices[adj.other as usize].id,
                        weight: adj.weight,
                    },
                );
            }
        }
        topology
    }

    /// The number of edges.
    #[must_use]
    pub fn edge_count(&self) -> u64 {
        self.csr.entries.len() as u64
    }

    /// The ordinal of the vertex with element id `id`.
    #[must_use]
    pub fn ordinal(&self, id: u64) -> Option<usize> {
        self.vertices.binary_search_by_key(&id, |v| v.id).ok()
    }

    /// The bytes held in memory, roughly: what Task 26 counts as the graph's memory.
    #[must_use]
    pub fn memory_bytes(&self) -> u64 {
        let adj = std::mem::size_of::<Adj>() as u64;
        let names: usize = self
            .labels
            .iter()
            .chain(self.edge_types.iter())
            .map(String::len)
            .sum();
        (self.vertices.len() * (std::mem::size_of::<Vertex>() + 8)) as u64
            + 2 * self.edge_count() * adj
            + 2 * 8 * (self.vertices.len() as u64 + 1)
            + names as u64
    }

    /// The canonical bytes, with their CRC-32C.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            64 + self.vertices.len() * 24 + self.csr.entries.len() * (24 + 16) + 16,
        );
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&u128::from(self.graph.0).to_be_bytes());
        out.extend_from_slice(&self.covered_offset.to_le_bytes());
        for table in [&self.edge_types, &self.labels] {
            put_u32(&mut out, table.len());
            for name in table {
                put_u32(&mut out, name.len());
                out.extend_from_slice(name.as_bytes());
            }
        }
        out.extend_from_slice(&(self.vertices.len() as u64).to_le_bytes());
        for vertex in &self.vertices {
            out.extend_from_slice(&vertex.id.to_le_bytes());
            put_u32(&mut out, vertex.labels.len());
            for label in &vertex.labels {
                out.extend_from_slice(&label.to_le_bytes());
            }
            match &vertex.key {
                None => out.push(0),
                Some(VertexKey::Int(n)) => {
                    out.push(1);
                    out.extend_from_slice(&n.to_le_bytes());
                }
                Some(VertexKey::Str(s)) => {
                    out.push(2);
                    put_u32(&mut out, s.len());
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
        out.extend_from_slice(&self.edge_count().to_le_bytes());
        for offset in &self.csr.offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for adj in &self.csr.entries {
            out.extend_from_slice(&adj.edge_type.to_le_bytes());
            out.extend_from_slice(&adj.other.to_le_bytes());
            out.extend_from_slice(&adj.edge.to_le_bytes());
            out.extend_from_slice(&adj.weight.to_bits().to_le_bytes());
        }
        for offset in &self.csc.offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for adj in &self.csc.entries {
            out.extend_from_slice(&adj.edge_type.to_le_bytes());
            out.extend_from_slice(&adj.other.to_le_bytes());
            out.extend_from_slice(&adj.edge.to_le_bytes());
        }
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes canonical bytes (see the module documentation).
    ///
    /// # Errors
    ///
    /// A [`FormatError`] for anything but the canonical encoding of an index.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < 12 || bytes[..8] != MAGIC {
            return Err(FormatError::Magic);
        }
        let mut r = Reader { bytes, at: 8 };
        let version = r.u32()?;
        if version != FORMAT_VERSION {
            return Err(FormatError::UnknownVersion(version));
        }
        if bytes.len() < 4 + 8 + 4 {
            return Err(FormatError::Truncated(bytes.len()));
        }
        let body = bytes.len() - 4;
        let crc = u32::from_le_bytes(
            bytes[body..]
                .try_into()
                .map_err(|_| FormatError::Checksum)?,
        );
        if crc32c::crc32c(&bytes[..body]) != crc {
            return Err(FormatError::Checksum);
        }
        let mut r = Reader {
            bytes: &bytes[..body],
            at: 12,
        };
        let graph = GraphId(ulid::Ulid::from(u128::from_be_bytes(
            r.take(16)?.try_into().map_err(|_| r.truncated())?,
        )));
        let covered_offset = r.u64()?;
        let edge_types = r.names()?;
        let labels = r.names()?;
        let n = r.u64()?;
        let vertex_count = r.count(8 + 4 + 1, n)?;
        if u32::try_from(vertex_count).is_err() {
            return Err(r.malformed("more vertices than 32 bits count"));
        }
        let mut vertices = Vec::with_capacity(vertex_count);
        let mut used_labels = vec![false; labels.len()];
        for _ in 0..vertex_count {
            let at = r.at;
            let id = r.u64()?;
            if vertices.last().is_some_and(|v: &Vertex| v.id >= id) {
                return Err(FormatError::Malformed {
                    at,
                    what: "vertex ids are not strictly increasing".to_string(),
                });
            }
            let n = r.u32()?;
            let count = r.count(4, u64::from(n))?;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                let label = r.u32()?;
                if label as usize >= labels.len() || ids.last().is_some_and(|l| *l >= label) {
                    return Err(r.malformed("a vertex's label ids are not increasing table ids"));
                }
                used_labels[label as usize] = true;
                ids.push(label);
            }
            let key = match r.u8()? {
                0 => None,
                1 => Some(VertexKey::Int(r.u64()? as i64)),
                2 => Some(VertexKey::Str(r.string()?)),
                _ => return Err(r.malformed("an unknown key tag")),
            };
            vertices.push(Vertex {
                id,
                labels: ids,
                key,
            });
        }
        if used_labels.iter().any(|used| !used) {
            return Err(r.malformed("the label table names a label no vertex has"));
        }
        let n = r.u64()?;
        let edge_count = r.count(24 + 16, n)?;
        let csr = r.csr(vertex_count, edge_count, edge_types.len(), true)?;
        let csc = r.csr(vertex_count, edge_count, edge_types.len(), false)?;
        if r.at != r.bytes.len() {
            return Err(r.malformed("bytes after the index"));
        }
        let mut used_types = vec![false; edge_types.len()];
        for adj in &csr.entries {
            used_types[adj.edge_type as usize] = true;
        }
        if used_types.iter().any(|used| !used) {
            return Err(r.malformed("the type table names a type no edge has"));
        }
        // The CSC must be the CSR's transpose: rebuild it and compare, weights taken from the CSR.
        let mut inc: Vec<Vec<Adj>> = vec![Vec::new(); vertex_count];
        for v in 0..vertex_count {
            for adj in csr.run(v) {
                if !adj.weight.is_finite() {
                    return Err(r.malformed("an edge weight that is not finite"));
                }
                inc[adj.other as usize].push(Adj {
                    edge_type: adj.edge_type,
                    other: v as u32,
                    edge: adj.edge,
                    weight: adj.weight,
                });
            }
        }
        let transpose = compress(inc);
        if transpose.offsets != csc.offsets
            || transpose
                .entries
                .iter()
                .zip(&csc.entries)
                .any(|(a, b)| (a.edge_type, a.other, a.edge) != (b.edge_type, b.other, b.edge))
        {
            return Err(r.malformed("the CSC is not the CSR's transpose"));
        }
        let mut ids: Vec<u64> = csr.entries.iter().map(|a| a.edge).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|w| w[0] == w[1]) {
            return Err(r.malformed("an edge id appears twice"));
        }
        Ok(Self {
            graph,
            covered_offset,
            edge_types,
            labels,
            vertices,
            csr,
            csc: transpose,
        })
    }
}

/// The SHA-256 of `bytes`, in lowercase hex: an index file's content address.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Runs sorted canonically (type, other end, edge id) and compressed.
fn compress(mut runs: Vec<Vec<Adj>>) -> Csr {
    let mut offsets = Vec::with_capacity(runs.len() + 1);
    let mut entries = Vec::with_capacity(runs.iter().map(Vec::len).sum());
    offsets.push(0);
    for run in &mut runs {
        run.sort_by_key(|a| (a.edge_type, a.other, a.edge));
        entries.extend_from_slice(run);
        offsets.push(entries.len() as u64);
    }
    Csr { offsets, entries }
}

fn put_u32(out: &mut Vec<u8>, n: usize) {
    // Every length the encoder writes was counted from in-memory data well under 4 GiB per
    // item; a longer one is refused by `from_topology`'s counts or cannot be built.
    out.extend_from_slice(&u32::try_from(n).unwrap_or(u32::MAX).to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn truncated(&self) -> FormatError {
        FormatError::Truncated(self.at)
    }

    fn malformed(&self, what: &str) -> FormatError {
        FormatError::Malformed {
            at: self.at,
            what: what.to_string(),
        }
    }

    fn take(&mut self, n: usize) -> Result<&[u8], FormatError> {
        let end = self.at.checked_add(n).ok_or_else(|| self.truncated())?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| self.truncated())?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, FormatError> {
        let at = self.at;
        let b = self.take(4)?;
        b.try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| FormatError::Truncated(at))
    }

    fn u64(&mut self) -> Result<u64, FormatError> {
        let at = self.at;
        let b = self.take(8)?;
        b.try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| FormatError::Truncated(at))
    }

    /// A count of items at least `min_bytes` each, refused when the bytes left cannot hold it.
    fn count(&self, min_bytes: usize, count: u64) -> Result<usize, FormatError> {
        let left = self.bytes.len().saturating_sub(self.at) as u64;
        if count.saturating_mul(min_bytes as u64) > left {
            return Err(self.truncated());
        }
        usize::try_from(count).map_err(|_| self.truncated())
    }

    fn string(&mut self) -> Result<String, FormatError> {
        let n = self.u32()?;
        let len = self.count(1, u64::from(n))?;
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| self.malformed("a name that is not UTF-8"))
    }

    /// A name table: strictly increasing UTF-8 byte order.
    fn names(&mut self) -> Result<Vec<String>, FormatError> {
        let n = self.u32()?;
        let count = self.count(4, u64::from(n))?;
        let mut names: Vec<String> = Vec::with_capacity(count);
        for _ in 0..count {
            let name = self.string()?;
            if names
                .last()
                .is_some_and(|last| last.as_bytes() >= name.as_bytes())
            {
                return Err(self.malformed("a name table is not strictly increasing"));
            }
            names.push(name);
        }
        Ok(names)
    }

    fn csr(
        &mut self,
        vertices: usize,
        edges: usize,
        types: usize,
        weighted: bool,
    ) -> Result<Csr, FormatError> {
        let offsets_count = self.count(8, vertices as u64 + 1)?;
        let mut offsets = Vec::with_capacity(offsets_count);
        for i in 0..offsets_count {
            let offset = self.u64()?;
            let ok = match offsets.last() {
                None => offset == 0,
                Some(prev) => offset >= *prev && offset <= edges as u64,
            } && (i + 1 < offsets_count || offset == edges as u64);
            if !ok {
                return Err(self.malformed("offsets are not 0 to the edge count, nondecreasing"));
            }
            offsets.push(offset);
        }
        let width = if weighted { 24 } else { 16 };
        let count = self.count(width, edges as u64)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let edge_type = self.u32()?;
            let other = self.u32()?;
            let edge = self.u64()?;
            let weight = if weighted {
                f64::from_bits(self.u64()?)
            } else {
                1.0
            };
            if edge_type as usize >= types || other as usize >= vertices {
                return Err(self.malformed("an edge names a type or vertex out of range"));
            }
            entries.push(Adj {
                edge_type,
                other,
                edge,
                weight,
            });
        }
        let csr = Csr { offsets, entries };
        for v in 0..vertices {
            let run = csr.run(v);
            if run.windows(2).any(|w| {
                (w[0].edge_type, w[0].other, w[0].edge) >= (w[1].edge_type, w[1].other, w[1].edge)
            }) {
                return Err(self.malformed("a run is not sorted by type, other end and edge"));
            }
        }
        Ok(csr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> Topology {
        let mut t = Topology::default();
        for (id, label, key) in [
            (3, "b", Some(VertexKey::Str("x".into()))),
            (1, "a", Some(VertexKey::Int(-4))),
            (9, "a", None),
        ] {
            t.vertices.insert(
                id,
                VertexState {
                    labels: [label.to_string()].into(),
                    key,
                },
            );
        }
        for (id, ty, src, dst) in [
            (20, "R", 1, 3),
            (10, "R", 1, 3),
            (11, "M", 9, 9),
            (5, "R", 3, 1),
        ] {
            t.edges.insert(
                id,
                EdgeState {
                    ty: ty.into(),
                    src,
                    dst,
                    weight: 1.5,
                },
            );
        }
        t
    }

    #[test]
    fn round_trips_and_is_canonical() {
        let graph = GraphId(ulid::Ulid::from(7u128));
        let index = AdjacencyIndex::from_topology(graph, 42, &topology()).expect("index");
        assert_eq!(index.edge_types, ["M", "R"]);
        let bytes = index.encode();
        let back = AdjacencyIndex::decode(&bytes).expect("decodes");
        assert_eq!(back, index);
        assert_eq!(back.topology(), topology());
        assert_eq!(back.encode(), bytes);
    }

    #[test]
    fn every_truncation_and_bit_flip_is_refused_not_panics() {
        let graph = GraphId(ulid::Ulid::from(7u128));
        let bytes = AdjacencyIndex::from_topology(graph, 42, &topology())
            .expect("index")
            .encode();
        for len in 0..bytes.len() {
            assert!(
                AdjacencyIndex::decode(&bytes[..len]).is_err(),
                "prefix {len}"
            );
        }
        for i in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[i] ^= 0x10;
            assert!(AdjacencyIndex::decode(&flipped).is_err(), "byte {i}");
        }
    }

    #[test]
    fn an_edge_to_a_missing_vertex_is_inconsistent() {
        let mut t = topology();
        t.vertices.remove(&9);
        assert!(matches!(
            AdjacencyIndex::from_topology(GraphId(ulid::Ulid::nil()), 0, &t),
            Err(FormatError::Inconsistent(_))
        ));
    }
}
