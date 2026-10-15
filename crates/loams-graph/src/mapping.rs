//! A LINKED graph's mapping over collections (GR1 Task 17; design §07 §2.1, §48 §10.1; D743).
//!
//! A LINKED graph owns no data of its own: vertex labels are declared over keyed collections, and
//! edge types over collections whose documents name their two endpoints' keys. The links of
//! Tasks 18 and 19 write it; GQL may only read it (`graph_is_linked`).
//!
//! ```sql
//! CREATE GRAPH kg
//!   VERTEX entity   KEY (_pk)                                  FROM COLLECTION entities
//!   EDGE   RELATED  SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations;
//! ```
//!
//! * A vertex key is the collection's primary key (`_pk`) or a keyword, integer or UUID field.
//! * An edge endpoint field holds a key of the endpoint's label: `_pk`, or a keyword, integer or
//!   UUID field.
//! * `TYPE FROM (field)` takes each document's edge type from a keyword or text field; otherwise
//!   the declared name is every edge's type.
//! * A vertex's properties are its document's fields, and an edge's its edge document's (§07
//!   §2.1): there is no projection.
//!
//! [`GraphMapping::check`] validates the declaration on its own; [`GraphMapping::check_against`]
//! validates it against the collections' schemas when the graph is created.

use std::collections::BTreeSet;

use loams_common::schema::{CollectionSchema, FieldKind};
use loams_proto::loams::graph::v1 as pb;
use serde::{Deserialize, Serialize};

/// The primary-key column of every collection.
pub const PRIMARY_KEY: &str = "_pk";

/// The most vertex labels or edge types one mapping declares.
pub const MAX_MAPPINGS: usize = 64;

/// How collections feed a LINKED graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphMapping {
    /// Vertex labels, each over one collection.
    pub vertices: Vec<VertexMap>,
    /// Edge types, each over one collection.
    #[serde(default)]
    pub edges: Vec<EdgeMap>,
}

/// `VERTEX <label> KEY (<key_field>) FROM COLLECTION <collection>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VertexMap {
    /// The vertex label.
    pub label: String,
    /// The collection whose documents are the vertices.
    pub collection: String,
    /// The field that keys a vertex: `_pk` or a keyword, integer or UUID field.
    pub key_field: String,
}

/// `EDGE <name> SOURCE <label>(<field>) DESTINATION <label>(<field>) [TYPE FROM (<field>)]
/// FROM COLLECTION <collection>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeMap {
    /// The declared edge name: every edge's type, unless `type_field` is set.
    pub name: String,
    /// When set, each document's edge type is this field's value (`TYPE FROM`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_field: Option<String>,
    /// The collection whose documents are the edges.
    pub collection: String,
    /// The source endpoint.
    pub source: Endpoint,
    /// The destination endpoint.
    pub destination: Endpoint,
}

/// One end of an edge: a declared vertex label, and the edge document's field holding its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// A vertex label of the same mapping.
    pub label: String,
    /// The edge document's field that holds the endpoint's key.
    pub field: String,
}

/// A mapping that cannot be served.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MappingError(pub String);

fn invalid(message: impl Into<String>) -> MappingError {
    MappingError(message.into())
}

/// A label or edge name: `[A-Za-z][A-Za-z0-9_]{0,63}`. A leading `_` is reserved for the
/// properties the links add (`_stub`, `_collection`, `_key`; §48 §10.1).
fn check_name(what: &str, name: &str) -> Result<(), MappingError> {
    let mut chars = name.chars();
    let ok = name.len() <= 64
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(invalid(format!(
            "{what} {name:?} must start with a letter and hold only letters, digits and `_` (at \
             most 64)"
        )))
    }
}

/// A collection or field name: not empty, at most 255 bytes, no control characters.
fn check_reference(what: &str, name: &str) -> Result<(), MappingError> {
    if name.is_empty() || name.len() > 255 || name.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{what} {name:?} must be 1 to 255 bytes with no control characters"
        )));
    }
    Ok(())
}

/// Whether a field of `kind` can hold a vertex key.
fn keys_a_vertex(kind: &FieldKind) -> bool {
    matches!(kind, FieldKind::Keyword | FieldKind::I64 | FieldKind::Uuid)
}

/// The kind of field `field` in `schema`, `None` for `_pk` (always a key).
fn field_kind<'a>(
    schema: &'a CollectionSchema,
    collection: &str,
    field: &str,
) -> Result<Option<&'a FieldKind>, MappingError> {
    if field == PRIMARY_KEY {
        return Ok(None);
    }
    schema
        .fields
        .iter()
        .find(|f| f.name == field)
        .map(|f| Some(&f.kind))
        .ok_or_else(|| invalid(format!("collection {collection:?} has no field {field:?}")))
}

impl GraphMapping {
    /// Checks the mapping on its own: at least one vertex label, at most [`MAX_MAPPINGS`] of
    /// each, unique labels and edge names, valid names, and edges between declared labels.
    ///
    /// # Errors
    ///
    /// [`MappingError`] naming the first problem.
    pub fn check(&self) -> Result<(), MappingError> {
        if self.vertices.is_empty() {
            return Err(invalid(
                "a LINKED graph's mapping declares at least one vertex label",
            ));
        }
        if self.vertices.len() > MAX_MAPPINGS || self.edges.len() > MAX_MAPPINGS {
            return Err(invalid(format!(
                "a mapping declares at most {MAX_MAPPINGS} vertex labels and {MAX_MAPPINGS} edges"
            )));
        }
        let mut labels = BTreeSet::new();
        for vertex in &self.vertices {
            check_name("vertex label", &vertex.label)?;
            check_reference("collection", &vertex.collection)?;
            check_reference("key field", &vertex.key_field)?;
            if !labels.insert(vertex.label.as_str()) {
                return Err(invalid(format!(
                    "vertex label {:?} is declared twice",
                    vertex.label
                )));
            }
        }
        let mut names = BTreeSet::new();
        for edge in &self.edges {
            check_name("edge name", &edge.name)?;
            check_reference("collection", &edge.collection)?;
            if let Some(field) = &edge.type_field {
                check_reference("TYPE FROM field", field)?;
            }
            if !names.insert(edge.name.as_str()) {
                return Err(invalid(format!("edge {:?} is declared twice", edge.name)));
            }
            for (end, endpoint) in [("SOURCE", &edge.source), ("DESTINATION", &edge.destination)] {
                check_reference("endpoint field", &endpoint.field)?;
                if !labels.contains(endpoint.label.as_str()) {
                    return Err(invalid(format!(
                        "edge {:?}'s {end} label {:?} is not a declared vertex label",
                        edge.name, endpoint.label
                    )));
                }
            }
        }
        Ok(())
    }

    /// Checks the mapping against its collections' schemas: each collection exists (`schema`
    /// answers it), a vertex key and each endpoint field is `_pk` or a keyword, integer or UUID
    /// field, and a `TYPE FROM` field is a keyword or text field.
    ///
    /// # Errors
    ///
    /// [`MappingError`] naming the first problem.
    pub fn check_against(
        &self,
        mut schema: impl FnMut(&str) -> Option<CollectionSchema>,
    ) -> Result<(), MappingError> {
        self.check()?;
        let mut lookup = |collection: &str| {
            schema(collection)
                .ok_or_else(|| invalid(format!("no collection {collection:?} in the namespace")))
        };
        let key_kind =
            |what: &str, kind: Option<&FieldKind>, collection: &str, field: &str| match kind {
                None => Ok(()),
                Some(kind) if keys_a_vertex(kind) => Ok(()),
                Some(kind) => Err(invalid(format!(
                    "{what} {collection}.{field} is {kind:?}; a key is `_pk` or a keyword, integer \
                     or UUID field"
                ))),
            };
        for vertex in &self.vertices {
            let fields = lookup(&vertex.collection)?;
            let kind = field_kind(&fields, &vertex.collection, &vertex.key_field)?;
            key_kind("vertex key", kind, &vertex.collection, &vertex.key_field)?;
        }
        for edge in &self.edges {
            let fields = lookup(&edge.collection)?;
            for endpoint in [&edge.source, &edge.destination] {
                let kind = field_kind(&fields, &edge.collection, &endpoint.field)?;
                key_kind("endpoint field", kind, &edge.collection, &endpoint.field)?;
            }
            if let Some(field) = &edge.type_field {
                match field_kind(&fields, &edge.collection, field)? {
                    Some(FieldKind::Keyword | FieldKind::Text { .. }) => {}
                    other => {
                        return Err(invalid(format!(
                            "TYPE FROM field {}.{field} is {}; it must be a keyword or text field",
                            edge.collection,
                            other.map_or_else(
                                || "the primary key".to_string(),
                                |k| format!("{k:?}")
                            )
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// The mapping a request carries.
    #[must_use]
    pub fn from_proto(mapping: &pb::GraphMapping) -> Self {
        Self {
            vertices: mapping
                .vertices
                .iter()
                .map(|v| VertexMap {
                    label: v.label.clone(),
                    collection: v.collection.clone(),
                    key_field: v.key_field.clone(),
                })
                .collect(),
            edges: mapping
                .edges
                .iter()
                .map(|e| EdgeMap {
                    name: e.r#type.clone(),
                    type_field: (!e.type_field.is_empty()).then(|| e.type_field.clone()),
                    collection: e.collection.clone(),
                    source: Endpoint {
                        label: e.source_label.clone(),
                        field: e.source_field.clone(),
                    },
                    destination: Endpoint {
                        label: e.destination_label.clone(),
                        field: e.destination_field.clone(),
                    },
                })
                .collect(),
        }
    }

    /// The mapping on the wire.
    #[must_use]
    pub fn to_proto(&self) -> pb::GraphMapping {
        pb::GraphMapping {
            vertices: self
                .vertices
                .iter()
                .map(|v| pb::VertexMapping {
                    label: v.label.clone(),
                    collection: v.collection.clone(),
                    key_field: v.key_field.clone(),
                    ..Default::default()
                })
                .collect(),
            edges: self
                .edges
                .iter()
                .map(|e| pb::EdgeMapping {
                    r#type: e.name.clone(),
                    collection: e.collection.clone(),
                    source_label: e.source.label.clone(),
                    source_field: e.source.field.clone(),
                    destination_label: e.destination.label.clone(),
                    destination_field: e.destination.field.clone(),
                    type_field: e.type_field.clone().unwrap_or_default(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use loams_common::schema::{DynamicMapping, FieldSpec};

    use super::*;

    fn field(name: &str, kind: FieldKind) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            source_path: name.to_string(),
            kind,
            indexed: true,
            fast: true,
            ignore_malformed: false,
        }
    }

    fn kg() -> GraphMapping {
        GraphMapping {
            vertices: vec![VertexMap {
                label: "entity".to_string(),
                collection: "entities".to_string(),
                key_field: PRIMARY_KEY.to_string(),
            }],
            edges: vec![EdgeMap {
                name: "RELATED".to_string(),
                type_field: Some("rel_type".to_string()),
                collection: "relations".to_string(),
                source: Endpoint {
                    label: "entity".to_string(),
                    field: "src".to_string(),
                },
                destination: Endpoint {
                    label: "entity".to_string(),
                    field: "dst".to_string(),
                },
            }],
        }
    }

    fn schemas(collection: &str) -> Option<CollectionSchema> {
        let fields = match collection {
            "entities" => vec![field("name", FieldKind::Keyword)],
            "relations" => vec![
                field("src", FieldKind::Keyword),
                field("dst", FieldKind::Keyword),
                field("rel_type", FieldKind::Keyword),
                field("weight", FieldKind::F64),
            ],
            _ => return None,
        };
        Some(CollectionSchema::new(
            fields,
            Vec::new(),
            DynamicMapping::Ignore,
        ))
    }

    #[test]
    fn a_mapping_round_trips_through_the_wire_and_serde() {
        let mapping = kg();
        assert_eq!(GraphMapping::from_proto(&mapping.to_proto()), mapping);
        let json = serde_json::to_string(&mapping).expect("json");
        assert_eq!(
            serde_json::from_str::<GraphMapping>(&json).expect("parse"),
            mapping
        );
        assert_eq!(mapping.check_against(schemas), Ok(()));
    }

    #[test]
    fn structural_mistakes_are_refused() {
        let mut empty = kg();
        empty.vertices.clear();
        empty.edges.clear();
        assert!(empty.check().is_err());
        let mut twice = kg();
        twice.vertices.push(twice.vertices[0].clone());
        assert!(twice.check().is_err());
        let mut undeclared = kg();
        undeclared.edges[0].destination.label = "chunk".to_string();
        assert!(undeclared.check().is_err());
        let mut reserved = kg();
        reserved.vertices[0].label = "_stub".to_string();
        assert!(reserved.check().is_err());
    }

    #[test]
    fn schema_mistakes_are_refused() {
        let mut missing = kg();
        missing.vertices[0].collection = "nowhere".to_string();
        assert!(missing.check_against(schemas).is_err());
        let mut no_field = kg();
        no_field.edges[0].source.field = "from".to_string();
        assert!(no_field.check_against(schemas).is_err());
        let mut float_key = kg();
        float_key.edges[0].destination.field = "weight".to_string();
        assert!(float_key.check_against(schemas).is_err());
        let mut float_type = kg();
        float_type.edges[0].type_field = Some("weight".to_string());
        assert!(float_type.check_against(schemas).is_err());
    }
}
