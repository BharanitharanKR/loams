//! Portable snapshots (GR1 Task 13; design §48 §6.4, §14.3; I6): a graph as Parquet files of its
//! nodes and edges, readable without Grafeo.
//!
//! The layout under a snapshot's prefix (`.../snap/<offset>/portable-<ULID>/`):
//!
//! * `manifest.json`: a [`PortableManifest`] naming every file, the durable offset the snapshot
//!   covers and the counts.
//! * `nodes/part-<n>.parquet`: columns `id` (UInt64), `labels` (List of Utf8, sorted) and
//!   `properties` (Binary).
//! * `edges/part-<n>.parquet`: columns `id` (UInt64), `type` (Utf8), `src` (UInt64), `dst`
//!   (UInt64) and `properties` (Binary).
//!
//! Rows are in id order, at most [`ROWS_PER_FILE`] per file. A `properties` cell is a property map
//! in the log record's encoding ([`crate::changeset::codec::encode_props`]: the version byte 1,
//! then the map as `conformance/graph/records/v1/README.md` specifies it), so a snapshot holds the
//! same typed values as the log, exactly, and one specification describes both. The Parquet and
//! Arrow crates are the workspace's (58), never Grafeo's.
//!
//! A portable snapshot is read back into a fresh engine with [`GraphRows::import`], which applies
//! the rows as upserts through the replay path (`apply_to_store`), keeping every element id.

use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, BinaryArray, ListArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use grafeo::GrafeoDB;
use loams_store::Store;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use super::{SnapshotEntry, SnapshotError, SnapshotKind, unavailable};
use crate::changeset::{ChangeOp, PropMap, apply_to_store, decode_props, encode_props};
use crate::engine::GraphError;

/// The portable snapshot's format name and version, in its `manifest.json`.
pub const PORTABLE_FORMAT: &str = "loams.graph.portable";
/// The version of [`PORTABLE_FORMAT`] this build writes and reads.
pub const PORTABLE_VERSION: u32 = 1;
/// The object name of a portable snapshot's manifest.
pub const MANIFEST_FILE: &str = "manifest.json";
/// The most rows one Parquet file holds.
pub const ROWS_PER_FILE: usize = 65_536;

/// One node.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRow {
    /// Its element id.
    pub id: u64,
    /// Its labels, sorted.
    pub labels: Vec<String>,
    /// Its properties.
    pub props: PropMap,
}

/// One edge.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeRow {
    /// Its element id.
    pub id: u64,
    /// Its type.
    pub ty: String,
    /// Its source node.
    pub src: u64,
    /// Its destination node.
    pub dst: u64,
    /// Its properties.
    pub props: PropMap,
}

/// A whole graph, by id: what a portable snapshot holds, and a canonical dump of an engine.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphRows {
    /// Every node, in id order.
    pub nodes: Vec<NodeRow>,
    /// Every edge, in id order.
    pub edges: Vec<EdgeRow>,
}

impl GraphRows {
    /// Every node and edge of `db`'s store, in id order. On a graph, the caller holds its gate
    /// shared, so this is the durable state ([`crate::Graph::rows`] does).
    #[must_use]
    pub fn read(db: &GrafeoDB) -> Self {
        let store = db.store();
        let mut nodes: Vec<NodeRow> = store
            .all_nodes()
            .map(|node| {
                let mut labels: Vec<String> = node.labels.iter().map(ToString::to_string).collect();
                labels.sort();
                NodeRow {
                    id: node.id.as_u64(),
                    labels,
                    props: props_of(node.properties.iter()),
                }
            })
            .collect();
        nodes.sort_by_key(|node| node.id);
        let mut edges: Vec<EdgeRow> = store
            .all_edges()
            .map(|edge| EdgeRow {
                id: edge.id.as_u64(),
                ty: edge.edge_type.to_string(),
                src: edge.src.as_u64(),
                dst: edge.dst.as_u64(),
                props: props_of(edge.properties.iter()),
            })
            .collect();
        edges.sort_by_key(|edge| edge.id);
        Self { nodes, edges }
    }

    /// The rows as change operations: every node, then every edge, as upserts.
    #[must_use]
    pub fn ops(&self) -> Vec<ChangeOp> {
        let nodes = self.nodes.iter().map(|node| ChangeOp::UpsertNode {
            id: node.id,
            labels: node.labels.clone(),
            props: node.props.clone(),
        });
        let edges = self.edges.iter().map(|edge| ChangeOp::UpsertEdge {
            id: edge.id,
            ty: edge.ty.clone(),
            src: edge.src,
            dst: edge.dst,
            props: edge.props.clone(),
        });
        nodes.chain(edges).collect()
    }

    /// Writes the rows into `db`, a fresh engine no client reads yet, keeping every element id
    /// (the replay path, R9.4). The engine must be checkpointed before its local files stand for
    /// anything (R9.5).
    ///
    /// # Errors
    ///
    /// [`GraphError::Engine`] when an edge names a node the rows do not have, or Grafeo refuses an
    /// id.
    pub fn import(&self, db: &GrafeoDB) -> Result<(), GraphError> {
        for chunk in self.ops().chunks(ROWS_PER_FILE) {
            apply_to_store(db, chunk)?;
        }
        Ok(())
    }
}

fn props_of<'a>(
    properties: impl Iterator<Item = (&'a grafeo_common::types::PropertyKey, &'a grafeo::Value)>,
) -> PropMap {
    properties
        .map(|(key, value)| (key.as_str().to_string(), value.clone()))
        .collect()
}

/// What a portable snapshot's `manifest.json` says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortableManifest {
    /// [`PORTABLE_FORMAT`].
    pub format: String,
    /// [`PORTABLE_VERSION`].
    pub version: u32,
    /// The graph, `gr_<ULID>`.
    pub graph: String,
    /// The durable epoch the snapshot covers: the graph's stream offset after its last record.
    pub offset: u64,
    /// The `txn_seq` the next record takes.
    pub next_txn_seq: u64,
    /// The engine that wrote it (informational: the format does not depend on it).
    pub engine_version: String,
    /// When it was taken, on the metastore clock.
    pub created_at_ms: u64,
    /// Nodes in the snapshot.
    pub nodes: u64,
    /// Edges in the snapshot.
    pub edges: u64,
    /// The node files, relative to the manifest, in order.
    pub node_files: Vec<String>,
    /// The edge files, relative to the manifest, in order.
    pub edge_files: Vec<String>,
}

/// What a portable snapshot is taken at, besides its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The graph.
    pub graph: String,
    /// The durable offset covered.
    pub offset: u64,
    /// The next `txn_seq`.
    pub next_txn_seq: u64,
    /// When it was taken.
    pub created_at_ms: u64,
}

fn node_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
        Field::new("properties", DataType::Binary, false),
    ]))
}

fn edge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
        Field::new("properties", DataType::Binary, false),
    ]))
}

fn corrupt(what: impl std::fmt::Display) -> SnapshotError {
    SnapshotError::Corrupt(format!("portable snapshot: {what}"))
}

fn encoded(props: &PropMap) -> Result<Vec<u8>, SnapshotError> {
    encode_props(props)
        .map(|bytes| bytes.to_vec())
        .map_err(|err| SnapshotError::Engine(GraphError::engine(err.to_string())))
}

fn parquet(batch: &RecordBatch) -> Result<Bytes, SnapshotError> {
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).map_err(corrupt)?;
    writer.write(batch).map_err(corrupt)?;
    writer.close().map_err(corrupt)?;
    Ok(Bytes::from(out))
}

fn node_file(rows: &[NodeRow]) -> Result<Bytes, SnapshotError> {
    let ids = UInt64Array::from_iter_values(rows.iter().map(|row| row.id));
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let mut labels = ListBuilder::new(StringBuilder::new()).with_field(item);
    for row in rows {
        for label in &row.labels {
            labels.values().append_value(label);
        }
        labels.append(true);
    }
    let props = rows
        .iter()
        .map(|row| encoded(&row.props))
        .collect::<Result<Vec<_>, _>>()?;
    let props = BinaryArray::from_iter_values(props);
    let columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(labels.finish()), Arc::new(props)];
    parquet(&RecordBatch::try_new(node_schema(), columns).map_err(corrupt)?)
}

fn edge_file(rows: &[EdgeRow]) -> Result<Bytes, SnapshotError> {
    let props = rows
        .iter()
        .map(|row| encoded(&row.props))
        .collect::<Result<Vec<_>, _>>()?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.id))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.ty))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.src))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.dst))),
        Arc::new(BinaryArray::from_iter_values(props)),
    ];
    parquet(&RecordBatch::try_new(edge_schema(), columns).map_err(corrupt)?)
}

/// Encodes `rows` as a portable snapshot: every file by its name relative to the snapshot's
/// prefix, the manifest last.
///
/// # Errors
///
/// [`SnapshotError::Engine`] for a value the record encoding refuses (R10.2), or
/// [`SnapshotError::Corrupt`] when Parquet refuses a batch.
pub fn encode(rows: &GraphRows, header: &Header) -> Result<Vec<(String, Bytes)>, SnapshotError> {
    let mut files = Vec::new();
    let mut manifest = PortableManifest {
        format: PORTABLE_FORMAT.to_string(),
        version: PORTABLE_VERSION,
        graph: header.graph.clone(),
        offset: header.offset,
        next_txn_seq: header.next_txn_seq,
        engine_version: crate::ENGINE_VERSION.to_string(),
        created_at_ms: header.created_at_ms,
        nodes: rows.nodes.len() as u64,
        edges: rows.edges.len() as u64,
        node_files: Vec::new(),
        edge_files: Vec::new(),
    };
    for (index, chunk) in rows.nodes.chunks(ROWS_PER_FILE).enumerate() {
        let name = format!("nodes/part-{index:05}.parquet");
        files.push((name.clone(), node_file(chunk)?));
        manifest.node_files.push(name);
    }
    for (index, chunk) in rows.edges.chunks(ROWS_PER_FILE).enumerate() {
        let name = format!("edges/part-{index:05}.parquet");
        files.push((name.clone(), edge_file(chunk)?));
        manifest.edge_files.push(name);
    }
    let json = serde_json::to_vec_pretty(&manifest).map_err(corrupt)?;
    files.push((MANIFEST_FILE.to_string(), Bytes::from(json)));
    Ok(files)
}

fn batches(bytes: Bytes, schema: &SchemaRef) -> Result<Vec<RecordBatch>, SnapshotError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(corrupt)?
        .build()
        .map_err(corrupt)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(corrupt)?;
        if batch.schema().fields() != schema.fields() {
            return Err(corrupt("a file's columns are not the format's"));
        }
        out.push(batch);
    }
    Ok(out)
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, SnapshotError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .filter(|_| batch.column(index).null_count() == 0)
        .ok_or_else(|| corrupt(format!("column {index} has the wrong type or a null")))
}

fn decoded(bytes: &[u8]) -> Result<PropMap, SnapshotError> {
    decode_props(bytes).map_err(corrupt)
}

fn read_nodes(bytes: Bytes, out: &mut Vec<NodeRow>) -> Result<(), SnapshotError> {
    for batch in batches(bytes, &node_schema())? {
        let ids = column::<UInt64Array>(&batch, 0)?;
        let labels = column::<ListArray>(&batch, 1)?;
        let props = column::<BinaryArray>(&batch, 2)?;
        for row in 0..batch.num_rows() {
            let values = labels.value(row);
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| corrupt("labels are not strings"))?;
            out.push(NodeRow {
                id: ids.value(row),
                labels: values.iter().flatten().map(str::to_string).collect(),
                props: decoded(props.value(row))?,
            });
        }
    }
    Ok(())
}

fn read_edges(bytes: Bytes, out: &mut Vec<EdgeRow>) -> Result<(), SnapshotError> {
    for batch in batches(bytes, &edge_schema())? {
        let ids = column::<UInt64Array>(&batch, 0)?;
        let types = column::<StringArray>(&batch, 1)?;
        let srcs = column::<UInt64Array>(&batch, 2)?;
        let dsts = column::<UInt64Array>(&batch, 3)?;
        let props = column::<BinaryArray>(&batch, 4)?;
        for row in 0..batch.num_rows() {
            out.push(EdgeRow {
                id: ids.value(row),
                ty: types.value(row).to_string(),
                src: srcs.value(row),
                dst: dsts.value(row),
                props: decoded(props.value(row))?,
            });
        }
    }
    Ok(())
}

/// Decodes a portable snapshot from its files, fetched by name relative to its prefix.
///
/// # Errors
///
/// [`SnapshotError::Corrupt`] when the manifest or a file is not the format, or the counts or the
/// id order do not hold; what `fetch` answers.
pub async fn decode<F, Fut>(fetch: F) -> Result<(PortableManifest, GraphRows), SnapshotError>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<Bytes, SnapshotError>>,
{
    let manifest: PortableManifest =
        serde_json::from_slice(&fetch(MANIFEST_FILE.to_string()).await?).map_err(corrupt)?;
    if manifest.format != PORTABLE_FORMAT || manifest.version != PORTABLE_VERSION {
        return Err(corrupt(format!(
            "{} version {} is not {PORTABLE_FORMAT} version {PORTABLE_VERSION}",
            manifest.format, manifest.version
        )));
    }
    let mut rows = GraphRows::default();
    for name in &manifest.node_files {
        read_nodes(fetch(name.clone()).await?, &mut rows.nodes)?;
    }
    for name in &manifest.edge_files {
        read_edges(fetch(name.clone()).await?, &mut rows.edges)?;
    }
    let ordered = rows.nodes.windows(2).all(|w| w[0].id < w[1].id)
        && rows.edges.windows(2).all(|w| w[0].id < w[1].id);
    if !ordered
        || rows.nodes.len() as u64 != manifest.nodes
        || rows.edges.len() as u64 != manifest.edges
    {
        return Err(corrupt("its rows do not match its manifest"));
    }
    Ok((manifest, rows))
}

/// Uploads a portable snapshot of `rows` under `prefix` (ending in `/`), and answers its object
/// keys (the manifest last) and their total size.
///
/// # Errors
///
/// What [`encode`] answers, or [`SnapshotError::Unavailable`] when the bucket refuses a file.
pub async fn write(
    store: &Store,
    prefix: &str,
    rows: &GraphRows,
    header: &Header,
) -> Result<(Vec<String>, u64), SnapshotError> {
    let files = encode(rows, header)?;
    let mut keys = Vec::with_capacity(files.len());
    let mut bytes = 0;
    for (name, body) in files {
        let key = format!("{prefix}{name}");
        bytes += body.len() as u64;
        store
            .put(&key, body)
            .await
            .map_err(|err| unavailable("uploading a portable snapshot", &err))?;
        keys.push(key);
    }
    Ok((keys, bytes))
}

/// Reads portable snapshot `entry` back from the bucket.
///
/// # Errors
///
/// [`SnapshotError::Corrupt`] when `entry` is not a portable snapshot or does not read back as
/// one, [`SnapshotError::Unavailable`] when the bucket fails (including a snapshot swept since
/// its manifest was read).
pub async fn read(
    store: &Store,
    entry: &SnapshotEntry,
) -> Result<(PortableManifest, GraphRows), SnapshotError> {
    let manifest_key = entry
        .keys
        .iter()
        .find(|key| key.ends_with(&format!("/{MANIFEST_FILE}")))
        .filter(|_| entry.kind == SnapshotKind::Portable)
        .ok_or_else(|| corrupt(format!("snapshot {} has no {MANIFEST_FILE}", entry.id)))?;
    let prefix = &manifest_key[..manifest_key.len() - MANIFEST_FILE.len()];
    let (manifest, rows) = decode(|name| async move {
        store
            .get(&format!("{prefix}{name}"))
            .await
            .map(|(bytes, _)| bytes)
            .map_err(|err| unavailable("downloading a portable snapshot", &err))
    })
    .await?;
    if manifest.offset != entry.offset || manifest.graph.is_empty() {
        return Err(corrupt(format!(
            "snapshot {} covers offset {}, its manifest entry says {}",
            entry.id, manifest.offset, entry.offset
        )));
    }
    Ok((manifest, rows))
}
