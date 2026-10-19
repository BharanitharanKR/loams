//! The `collection → graph` link (GR1 Task 18; design §48 §10.1, D743).
//!
//! A collection's records are `DocOp`s; [`super::target::GraphLinkTarget`] commits them, and
//! [`Plan`] turns them into change operations.
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

use std::collections::{BTreeSet, HashMap};

use grafeo::Value;
use loams_collection::{DocOp, Document, apply_patch};

use super::writer::{Writer, json_value};
use super::{
    COLLECTION_PROPERTY, EDGE_PROPERTY, KEY_PROPERTY, Key, LINK_PROPERTIES, LinkIndex, PK_PROPERTY,
};
use crate::engine::GraphError;
use crate::mapping::{EdgeMap, GraphMapping, PRIMARY_KEY, VertexMap};

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
    /// The collection's name.
    pub(crate) fn collection(&self) -> &str {
        &self.collection
    }

    pub(crate) fn new(
        mapping: &GraphMapping,
        collection: &loams_common::meta::Collection,
        names: &BTreeSet<String>,
    ) -> Self {
        Self {
            collection: collection.name.clone(),
            vertices: mapping
                .vertices
                .iter()
                .filter(|v| names.contains(&v.collection))
                .cloned()
                .collect(),
            edges: mapping
                .edges
                .iter()
                .filter(|e| names.contains(&e.collection))
                .cloned()
                .collect(),
            paths: collection
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
        db: &grafeo::GrafeoDB,
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
fn properties(doc: &Document) -> crate::changeset::PropMap {
    doc.source
        .iter()
        .filter(|(key, value)| !LINK_PROPERTIES.contains(&key.as_str()) && !value.is_null())
        .map(|(key, value)| (key.clone(), json_value(value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::writer::value_json;
    use super::*;
    use loams_collection::PrimaryKey;

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
