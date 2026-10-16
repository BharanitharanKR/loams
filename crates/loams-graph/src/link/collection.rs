//! The `collection → graph` link (GR1 Task 18; design §48 §10.1, D743).
//!
//! [`CollectionLinkTarget`] is `loams-link`'s [`LinkTarget`] for one collection of a LINKED
//! graph: its `load` answers the link's progress from the graph's durable state, and its
//! `commit` turns the batch's `DocOp`s into change operations and appends them, with the batch's
//! offsets, as one record of the graph's stream ([`Graph::commit_link`]).
//!
//! How a `DocOp` becomes graph operations ([`Plan`]):
//!
//! * **A vertex document** (a collection a `VERTEX` names) is a vertex of that label keyed by its
//!   key field: an upsert is `UpsertNode` with the document's fields as properties (so a second
//!   upsert of the same key replaces the first: the entity is merged, never duplicated); a delete
//!   is a `DeleteEdge` per incident edge and then a `DeleteNode`; a patch is applied to the
//!   document the vertex holds (`apply_patch`) and written whole.
//! * **An edge document** (a collection an `EDGE` names) is an edge from the vertex its source
//!   field keys to the one its destination field keys, of the declared type or its `TYPE FROM`
//!   field's. An endpoint with no vertex yet is a **stub**: a vertex of the endpoint's label with
//!   only `_key` and `_stub = true`, which the vertex's own upsert later fills. A stub that loses
//!   its last edge goes.
//! * A document whose key, endpoint or type field is missing or not a key is skipped (and an
//!   element it made before is removed), with a warning.
//!
//! Each operation is applied to the engine as it is planned, so the next one sees it; the record
//! holds them in that order, and a replay applies them the same way.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use grafeo::{GrafeoDB, Value};
use grafeo_common::types::PropertyKey;
use loams_collection::{DocOp, Document, PrimaryKey, apply_patch};
use loams_common::meta::{Fence, LinkId};
use loams_link::{ApplyBatch, CommitError, LinkError, LinkTarget, TargetState};

use super::{
    COLLECTION_PROPERTY, EDGE_PROPERTY, KEY_PROPERTY, Key, LINK_PROPERTIES, LinkIndex, PK_PROPERTY,
    STUB_PROPERTY, Source,
};
use crate::catalog::GraphMeta;
use crate::changeset::{ChangeOp, LinkApply, PropMap, apply_to_store};
use crate::engine::{Graph, GraphError};
use crate::mapping::{EdgeMap, GraphMapping, PRIMARY_KEY, VertexMap};
use crate::service::admin::GraphAdmin;

/// The most source records one record of the graph's stream applies: a larger batch is
/// committed in several records, each with its own offsets.
pub const CHUNK_RECORDS: usize = 2_048;

/// The most source bytes (record values) one record of the graph's stream applies, well below
/// the 32 MiB a graph record may hold.
pub const CHUNK_BYTES: usize = 4 << 20;

/// One collection of a LINKED graph, as a link target.
pub struct CollectionLinkTarget {
    admin: GraphAdmin,
    graph: GraphMeta,
    plan: Arc<Plan>,
    link: LinkId,
    commits: AtomicUsize,
}

impl std::fmt::Debug for CollectionLinkTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionLinkTarget")
            .field("graph", &self.graph.id)
            .field("collection", &self.plan.collection)
            .field("link", &self.link)
            .finish_non_exhaustive()
    }
}

impl CollectionLinkTarget {
    pub(crate) fn new(
        admin: GraphAdmin,
        graph: GraphMeta,
        mapping: GraphMapping,
        source: Source,
        link: LinkId,
    ) -> Self {
        Self {
            admin,
            graph,
            plan: Arc::new(Plan::new(&mapping, &source)),
            link,
            commits: AtomicUsize::new(0),
        }
    }

    /// Records of the graph's stream this target has committed.
    #[must_use]
    pub fn commits(&self) -> usize {
        self.commits.load(Ordering::Acquire)
    }

    /// The graph, open here as its owner.
    async fn open(&self) -> Result<Arc<Graph>, LinkError> {
        self.admin.link_graph(&self.graph).await.map_err(retryable)
    }
}

fn retryable(err: impl std::error::Error + Send + Sync + 'static) -> LinkError {
    LinkError::Target {
        retryable: true,
        source: Box::new(err),
    }
}

#[async_trait]
impl LinkTarget for CollectionLinkTarget {
    async fn load(&self) -> Result<TargetState, LinkError> {
        let graph = self.open().await?;
        Ok(graph
            .link_progress(self.link)
            .map(|progress| TargetState {
                version: progress.version,
                applied: progress.applied_after,
            })
            .unwrap_or_default())
    }

    /// Commits `batch` in one record of the graph's stream, or several when it is larger than
    /// [`CHUNK_RECORDS`] or [`CHUNK_BYTES`]: each record carries its own records' offsets, so a
    /// failure between two leaves the graph at a consistent point. The fence is not used: the
    /// graph's owner lease fences every append, and the version, checked in the write lane,
    /// refuses a stale commit.
    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        _fence: &Fence,
    ) -> Result<u64, CommitError> {
        let graph = self.open().await.map_err(CommitError::Other)?;
        let current = graph.link_progress(self.link);
        let mut version = current.as_ref().map_or(0, |progress| progress.version);
        if version != expected_version {
            return Err(CommitError::Conflict);
        }
        let mut applied = current
            .map(|progress| progress.applied_after)
            .unwrap_or_default();
        for chunk in chunks(&batch, &self.plan.collection) {
            applied.extend(chunk.advance);
            let plan = Arc::clone(&self.plan);
            let ops = chunk.ops;
            let job = crate::durable::lane::LinkJob {
                progress: LinkApply {
                    link: self.link,
                    version: version + 1,
                    applied_after: applied.clone(),
                },
                apply: Box::new(move |db, index| plan.apply(db, index, &ops)),
            };
            match self.admin.link_commit(Arc::clone(&graph), job).await {
                Ok(Ok(_)) => {
                    version += 1;
                    self.commits.fetch_add(1, Ordering::AcqRel);
                }
                Ok(Err(GraphError::VersionMismatch { .. })) => return Err(CommitError::Conflict),
                Ok(Err(err)) => return Err(CommitError::Other(retryable(err))),
                Err(err) => return Err(CommitError::Other(retryable(err))),
            }
        }
        Ok(version)
    }
}

/// Part of a batch committed as one record.
struct Chunk {
    ops: Vec<DocOp>,
    /// The next offset to apply, per partition, once the chunk is durable.
    advance: BTreeMap<u32, u64>,
}

/// Splits `batch` into chunks of at most [`CHUNK_RECORDS`] records and [`CHUNK_BYTES`] bytes,
/// in batch order. The last chunk carries the whole batch's `applied_after`, which also covers
/// offsets trimmed before the link reached them. A record that does not decode is skipped with
/// a warning: a collection's writer encodes every record it appends, so one that does not
/// decode is corrupt, and it must not stop the link.
fn chunks(batch: &ApplyBatch, collection: &str) -> Vec<Chunk> {
    let mut out = Vec::new();
    let mut current = Chunk {
        ops: Vec::new(),
        advance: BTreeMap::new(),
    };
    let mut records = 0;
    let mut bytes = 0;
    for (partition, record) in &batch.records {
        let size = record.record.value.as_ref().map_or(0, bytes::Bytes::len);
        if records > 0 && (records >= CHUNK_RECORDS || bytes + size > CHUNK_BYTES) {
            out.push(std::mem::replace(
                &mut current,
                Chunk {
                    ops: Vec::new(),
                    advance: BTreeMap::new(),
                },
            ));
            records = 0;
            bytes = 0;
        }
        records += 1;
        bytes += size;
        current.advance.insert(*partition, record.offset + 1);
        match loams_collection::decode(&record.record) {
            Ok(op) => current.ops.push(op),
            Err(err) => {
                tracing::warn!(%collection, partition, offset = record.offset, error = %err, "a record of a LINKED graph's collection does not decode; the link skips it");
            }
        }
    }
    current.advance.extend(batch.applied_after.clone());
    out.push(current);
    out
}

/// What one collection writes into a graph: the mapping's vertices and edges over it, and where
/// its fields are in a document.
#[derive(Debug)]
pub(crate) struct Plan {
    /// The collection's name (`_collection` on everything it writes).
    collection: String,
    vertices: Vec<VertexMap>,
    edges: Vec<EdgeMap>,
    /// Field name → its dot-separated source path, from the collection's schema.
    paths: HashMap<String, String>,
}

impl Plan {
    fn new(mapping: &GraphMapping, source: &Source) -> Self {
        Self {
            collection: source.collection.name.clone(),
            vertices: mapping
                .vertices
                .iter()
                .filter(|v| source.names.contains(&v.collection))
                .cloned()
                .collect(),
            edges: mapping
                .edges
                .iter()
                .filter(|e| source.names.contains(&e.collection))
                .cloned()
                .collect(),
            paths: source
                .collection
                .schema
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.source_path.clone()))
                .collect(),
        }
    }

    /// Applies `ops` to `db`, in order, and answers the change operations that did it.
    pub(crate) fn apply(
        &self,
        db: &GrafeoDB,
        index: &mut LinkIndex,
        ops: &[DocOp],
    ) -> Result<Vec<crate::changeset::ChangeOp>, GraphError> {
        let mut writer = Writer {
            db,
            index,
            ops: Vec::new(),
        };
        for op in ops {
            for vertex in &self.vertices {
                self.vertex(&mut writer, vertex, op)?;
            }
            for edge in &self.edges {
                self.edge(&mut writer, edge, op)?;
            }
        }
        Ok(writer.ops)
    }

    /// The key a document's `field` holds: its primary key for `_pk`, else the field's value
    /// at its source path.
    fn key_of(&self, doc: &Document, field: &str) -> Option<Key> {
        if field == PRIMARY_KEY {
            return Some(Key::of_pk(&doc.pk));
        }
        self.field(doc, field).and_then(Key::of_json)
    }

    fn field<'a>(&self, doc: &'a Document, field: &str) -> Option<&'a serde_json::Value> {
        let path = self.paths.get(field).map_or(field, String::as_str);
        let mut segments = path.split('.');
        let mut value = doc.source.get(segments.next()?)?;
        for segment in segments {
            value = value.as_object()?.get(segment)?;
        }
        Some(value)
    }

    fn vertex(&self, w: &mut Writer<'_>, map: &VertexMap, op: &DocOp) -> Result<(), GraphError> {
        let label = map.label.as_str();
        let pk = Key::of_pk(op.pk());
        let keyed_by_pk = map.key_field == PRIMARY_KEY;
        let current = if keyed_by_pk {
            w.index.vertex(label, &pk)
        } else {
            w.index.vertex_by_pk(label, &pk)
        }
        .filter(|id| !w.index.is_stub(*id));
        let before = current.map(|id| w.node_document(id, op.pk()));
        let Some(doc) = apply_patch(before.as_ref(), op) else {
            if let Some(id) = current {
                w.delete_vertex(id)?;
            }
            return Ok(());
        };
        let Some(key) = self.key_of(&doc, &map.key_field) else {
            tracing::warn!(collection = %self.collection, %label, field = %map.key_field, "a document has no key for its vertex; the link skips it");
            if let Some(id) = current {
                w.delete_vertex(id)?;
            }
            return Ok(());
        };
        // Its key changed: it is another vertex now.
        if let Some(id) = current
            && w.index.vertex(label, &key) != Some(id)
        {
            w.delete_vertex(id)?;
        }
        let mut props = properties(&doc);
        props.insert(
            COLLECTION_PROPERTY.to_string(),
            Value::String(self.collection.as_str().into()),
        );
        props.insert(KEY_PROPERTY.to_string(), key.value());
        let by_pk = (!keyed_by_pk).then_some(pk);
        if let Some(pk) = &by_pk {
            props.insert(PK_PROPERTY.to_string(), pk.value());
        }
        w.upsert_vertex(label, key, props, by_pk)
    }

    fn edge(&self, w: &mut Writer<'_>, map: &EdgeMap, op: &DocOp) -> Result<(), GraphError> {
        let pk = Key::of_pk(op.pk());
        let current = w.index.edge(&map.name, &pk);
        let before = current.map(|id| w.edge_document(id, op.pk()));
        let shape = apply_patch(before.as_ref(), op).map(|doc| {
            let ty = match &map.type_field {
                None => Some(map.name.clone()),
                Some(field) => self
                    .field(&doc, field)
                    .and_then(serde_json::Value::as_str)
                    .filter(|ty| !ty.is_empty())
                    .map(str::to_string),
            };
            let src = self.key_of(&doc, &map.source.field);
            let dst = self.key_of(&doc, &map.destination.field);
            (doc, ty, src, dst)
        });
        match shape {
            None => {
                if let Some(id) = current {
                    w.delete_edge(id)?;
                }
                Ok(())
            }
            Some((doc, Some(ty), Some(src), Some(dst))) => {
                let src = w.endpoint(&map.source.label, src)?;
                let dst = w.endpoint(&map.destination.label, dst)?;
                let mut props = properties(&doc);
                props.insert(
                    COLLECTION_PROPERTY.to_string(),
                    Value::String(self.collection.as_str().into()),
                );
                props.insert(KEY_PROPERTY.to_string(), pk.value());
                props.insert(
                    EDGE_PROPERTY.to_string(),
                    Value::String(map.name.as_str().into()),
                );
                w.upsert_edge(&map.name, pk, current, ty, src, dst, props)
            }
            Some(_) => {
                tracing::warn!(collection = %self.collection, edge = %map.name, "a document has no type or endpoint key for its edge; the link skips it");
                if let Some(id) = current {
                    w.delete_edge(id)?;
                }
                Ok(())
            }
        }
    }
}

/// A document's fields as properties: the link's own properties are never taken from it, and
/// `null`s are absent.
fn properties(doc: &Document) -> PropMap {
    doc.source
        .iter()
        .filter(|(key, value)| !LINK_PROPERTIES.contains(&key.as_str()) && !value.is_null())
        .map(|(key, value)| (key.clone(), json_value(value)))
        .collect()
}

/// A JSON value as a property value: integers that fit `i64` stay integers, other numbers are
/// floats, arrays are lists and objects maps.
fn json_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Value::Int64(i),
            None => Value::Float64(n.as_f64().unwrap_or(f64::NAN)),
        },
        serde_json::Value::String(s) => Value::String(s.as_str().into()),
        serde_json::Value::Array(items) => Value::List(items.iter().map(json_value).collect()),
        serde_json::Value::Object(map) => Value::Map(Arc::new(
            map.iter()
                .map(|(key, value)| (PropertyKey::new(key.as_str()), json_value(value)))
                .collect(),
        )),
    }
}

/// A property value as JSON: the inverse of [`json_value`] for what a link writes.
fn value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(i) => serde_json::Value::from(*i),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_json).collect()),
        Value::Map(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (key.as_str().to_string(), value_json(value)))
                .collect(),
        ),
        other => serde_json::Value::String(other.to_string()),
    }
}

/// Applies change operations to the engine one at a time, keeping the link index in step.
struct Writer<'a> {
    db: &'a GrafeoDB,
    index: &'a mut LinkIndex,
    ops: Vec<ChangeOp>,
}

impl Writer<'_> {
    fn emit(&mut self, op: ChangeOp) -> Result<(), GraphError> {
        apply_to_store(self.db, std::slice::from_ref(&op))?;
        self.ops.push(op);
        Ok(())
    }

    fn node_props(&self, id: u64) -> PropMap {
        self.db
            .store()
            .get_node(grafeo::NodeId::new(id))
            .map(|node| {
                node.properties
                    .iter()
                    .map(|(key, value)| (key.as_str().to_string(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn edge_props(&self, id: u64) -> PropMap {
        self.db
            .store()
            .get_edge(grafeo::EdgeId::new(id))
            .map(|edge| {
                edge.properties
                    .iter()
                    .map(|(key, value)| (key.as_str().to_string(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The document an element holds: its properties without the link's own.
    fn document(props: &PropMap, pk: &PrimaryKey) -> Document {
        Document {
            pk: pk.clone(),
            source: props
                .iter()
                .filter(|(key, _)| !LINK_PROPERTIES.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect(),
            vectors: BTreeMap::new(),
            sparse_vectors: BTreeMap::new(),
        }
    }

    fn node_document(&self, id: u64, pk: &PrimaryKey) -> Document {
        Self::document(&self.node_props(id), pk)
    }

    fn edge_document(&self, id: u64, pk: &PrimaryKey) -> Document {
        Self::document(&self.edge_props(id), pk)
    }

    fn upsert_vertex(
        &mut self,
        label: &str,
        key: Key,
        props: PropMap,
        by_pk: Option<Key>,
    ) -> Result<(), GraphError> {
        let id = self
            .index
            .vertex(label, &key)
            .unwrap_or_else(|| self.db.store().next_node_id());
        self.emit(ChangeOp::UpsertNode {
            id,
            labels: vec![label.to_string()],
            props,
        })?;
        self.index.stubs.remove(&id);
        self.index.vertices.insert((label.to_string(), key), id);
        if let Some(pk) = by_pk {
            self.index.by_pk.insert((label.to_string(), pk), id);
        }
        Ok(())
    }

    /// The vertex of `label` keyed `key`, made as a stub when there is none.
    fn endpoint(&mut self, label: &str, key: Key) -> Result<u64, GraphError> {
        if let Some(id) = self.index.vertex(label, &key) {
            return Ok(id);
        }
        let id = self.db.store().next_node_id();
        let props = PropMap::from([
            (KEY_PROPERTY.to_string(), key.value()),
            (STUB_PROPERTY.to_string(), Value::Bool(true)),
        ]);
        self.emit(ChangeOp::UpsertNode {
            id,
            labels: vec![label.to_string()],
            props,
        })?;
        self.index.vertices.insert((label.to_string(), key), id);
        self.index.stubs.insert(id);
        Ok(id)
    }

    /// Deletes vertex `id` and its edges (and the stubs only those edges kept).
    fn delete_vertex(&mut self, id: u64) -> Result<(), GraphError> {
        for edge in self.index.edges_of(id) {
            self.delete_edge(edge)?;
        }
        let props = self.node_props(id);
        let label = self
            .db
            .store()
            .get_node(grafeo::NodeId::new(id))
            .and_then(|node| node.labels.first().map(ToString::to_string));
        self.emit(ChangeOp::DeleteNode { id })?;
        if let Some(label) = label {
            for (property, map) in [
                (KEY_PROPERTY, &mut self.index.vertices),
                (PK_PROPERTY, &mut self.index.by_pk),
            ] {
                if let Some(key) = props.get(property).and_then(Key::of_value) {
                    let entry = (label.clone(), key);
                    if map.get(&entry) == Some(&id) {
                        map.remove(&entry);
                    }
                }
            }
        }
        self.index.stubs.remove(&id);
        self.index.incident.remove(&id);
        Ok(())
    }

    /// Deletes edge `id`, and an endpoint that is a stub with no other edge.
    fn delete_edge(&mut self, id: u64) -> Result<(), GraphError> {
        let props = self.edge_props(id);
        self.emit(ChangeOp::DeleteEdge { id })?;
        if let (Some(Value::String(name)), Some(key)) = (
            props.get(EDGE_PROPERTY),
            props.get(KEY_PROPERTY).and_then(Key::of_value),
        ) {
            let entry = (name.to_string(), key);
            if self.index.edges.get(&entry) == Some(&id) {
                self.index.edges.remove(&entry);
            }
        }
        if let Some((src, dst)) = self.index.ends.remove(&id) {
            for end in [src, dst] {
                if let Some(edges) = self.index.incident.get_mut(&end) {
                    edges.remove(&id);
                }
            }
            for end in [src, dst] {
                if self.index.is_stub(end) && self.index.degree(end) == 0 {
                    self.delete_vertex(end)?;
                }
            }
        }
        Ok(())
    }

    /// Writes the edge of mapping edge `name` and document key `pk`: in place when it already
    /// joins `src` to `dst` with type `ty`, else as a new edge that replaces `current`.
    #[allow(clippy::too_many_arguments)]
    fn upsert_edge(
        &mut self,
        name: &str,
        pk: Key,
        current: Option<u64>,
        ty: String,
        src: u64,
        dst: u64,
        props: PropMap,
    ) -> Result<(), GraphError> {
        let same = current.filter(|id| {
            self.index.ends.get(id) == Some(&(src, dst))
                && self
                    .db
                    .store()
                    .get_edge(grafeo::EdgeId::new(*id))
                    .is_some_and(|edge| edge.edge_type.as_str() == ty)
        });
        let id = same.unwrap_or_else(|| self.db.store().next_edge_id());
        self.emit(ChangeOp::UpsertEdge {
            id,
            ty,
            src,
            dst,
            props,
        })?;
        if same.is_none() {
            self.index.ends.insert(id, (src, dst));
            self.index.incident.entry(src).or_default().insert(id);
            self.index.incident.entry(dst).or_default().insert(id);
            if let Some(old) = current {
                self.delete_edge(old)?;
            }
        }
        self.index.edges.insert((name.to_string(), pk), id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_values_round_trip_through_properties() {
        let doc = serde_json::json!({
            "s": "x", "i": -3, "f": 1.5, "b": true,
            "l": [1, "a", null], "m": {"k": [2.5]},
        });
        for (_, value) in doc.as_object().expect("object") {
            assert_eq!(&value_json(&json_value(value)), value);
        }
    }

    #[test]
    fn keys_of_primary_keys() {
        assert_eq!(Key::of_pk(&PrimaryKey::U64(7)), Key::Int(7));
        assert_eq!(
            Key::of_pk(&PrimaryKey::U64(u64::MAX)),
            Key::Str(u64::MAX.to_string())
        );
        assert_eq!(
            Key::of_pk(&PrimaryKey::Uuid([0xab; 16])),
            Key::Str("abababab-abab-abab-abab-abababababab".to_string())
        );
        assert_eq!(Key::of_json(&serde_json::json!(4)), Some(Key::Int(4)));
        assert_eq!(Key::of_json(&serde_json::json!([4])), None);
    }
}
