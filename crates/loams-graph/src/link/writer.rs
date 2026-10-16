//! Writing a link's change operations into a graph's engine, one at a time, with the link
//! index kept in step (GR1 Tasks 18 and 19).

use std::collections::BTreeMap;
use std::sync::Arc;

use grafeo::{GrafeoDB, Value};
use grafeo_common::types::PropertyKey;
use loams_collection::{Document, PrimaryKey};

use super::{
    EDGE_PROPERTY, KEY_PROPERTY, Key, LINK_PROPERTIES, LinkIndex, PK_PROPERTY, STUB_PROPERTY,
};
use crate::changeset::{ChangeOp, PropMap, apply_to_store};
use crate::engine::GraphError;

/// A JSON value as a property value: integers that fit `i64` stay integers, other numbers are
/// floats, arrays are lists and objects maps.
pub(super) fn json_value(value: &serde_json::Value) -> Value {
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
pub(super) fn value_json(value: &Value) -> serde_json::Value {
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
pub(super) struct Writer<'a> {
    pub(super) db: &'a GrafeoDB,
    pub(super) index: &'a mut LinkIndex,
    pub(super) ops: Vec<ChangeOp>,
}

impl Writer<'_> {
    pub(super) fn emit(&mut self, op: ChangeOp) -> Result<(), GraphError> {
        apply_to_store(self.db, std::slice::from_ref(&op))?;
        self.ops.push(op);
        Ok(())
    }

    pub(super) fn node_props(&self, id: u64) -> PropMap {
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

    pub(super) fn edge_props(&self, id: u64) -> PropMap {
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
    pub(super) fn document(props: &PropMap, pk: &PrimaryKey) -> Document {
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

    pub(super) fn node_document(&self, id: u64, pk: &PrimaryKey) -> Document {
        Self::document(&self.node_props(id), pk)
    }

    pub(super) fn edge_document(&self, id: u64, pk: &PrimaryKey) -> Document {
        Self::document(&self.edge_props(id), pk)
    }

    pub(super) fn upsert_vertex(
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
    pub(super) fn endpoint(&mut self, label: &str, key: Key) -> Result<u64, GraphError> {
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
    pub(super) fn delete_vertex(&mut self, id: u64) -> Result<(), GraphError> {
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
    pub(super) fn delete_edge(&mut self, id: u64) -> Result<(), GraphError> {
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
    pub(super) fn upsert_edge(
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
