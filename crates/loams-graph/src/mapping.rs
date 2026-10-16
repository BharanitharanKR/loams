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

/// How collections and streams feed a LINKED graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphMapping {
    /// Vertex labels, each over one collection.
    pub vertices: Vec<VertexMap>,
    /// Edge types, each over one collection.
    #[serde(default)]
    pub edges: Vec<EdgeMap>,
    /// `stream → graph` links (GR1 Task 19; §48 §10.2).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<StreamMap>,
}

/// One stream feeding a LINKED graph: its records, JSON or CloudEvents, through rules (GR1
/// Task 19). See `loams.graph.v1.StreamMapping`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMap {
    /// The stream's name in the graph's namespace.
    pub stream: String,
    /// The rules, in order: every rule a record matches applies.
    pub rules: Vec<StreamRule>,
}

/// What a stream rule does with a record it matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamOp {
    /// Creates the node, or merges the record's properties into it.
    UpsertNode,
    /// Deletes the node and its edges.
    DeleteNode,
    /// Creates the edge (and stub endpoints), or merges its properties.
    UpsertEdge,
    /// Deletes the edge.
    DeleteEdge,
}

impl StreamOp {
    /// Whether the rule writes an edge.
    #[must_use]
    pub fn is_edge(self) -> bool {
        matches!(self, Self::UpsertEdge | Self::DeleteEdge)
    }
}

/// One rule of a [`StreamMap`]. Paths are dot-separated field paths from the record's root (a
/// CloudEvent's attributes and `data.<field>`); an empty string is an absent field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRule {
    /// The rule's name, unique among the mapping's edges and rules.
    pub name: String,
    /// Only CloudEvents of this `type`; empty matches every record.
    #[serde(default)]
    pub event_type: String,
    /// What the rule does.
    pub op: StreamOp,
    /// Node operations: the label.
    #[serde(default)]
    pub label: String,
    /// Node operations: the path of the key.
    #[serde(default)]
    pub key_field: String,
    /// Edge operations: the source's label.
    #[serde(default)]
    pub source_label: String,
    /// Edge operations: the path of the source's key.
    #[serde(default)]
    pub source_key_field: String,
    /// Edge operations: the destination's label.
    #[serde(default)]
    pub destination_label: String,
    /// Edge operations: the path of the destination's key.
    #[serde(default)]
    pub destination_key_field: String,
    /// Edge operations: the type, unless `edge_type_field` names it.
    #[serde(default)]
    pub edge_type: String,
    /// Edge operations: the path of a field holding the type.
    #[serde(default)]
    pub edge_type_field: String,
    /// Edge operations: the path of the edge's own key, which identifies it among the stream's
    /// edges; empty identifies an edge by its type and endpoints.
    #[serde(default)]
    pub edge_key_field: String,
    /// Upserts: the path of the object whose fields are the properties; empty is `data` for a
    /// CloudEvent and the whole record otherwise.
    #[serde(default)]
    pub properties_field: String,
}

/// The most rules one mapping's streams declare.
pub const MAX_STREAM_RULES: usize = 64;

/// A field path: 1 to 255 bytes of dot-separated, non-empty segments.
fn check_path(what: &str, path: &str) -> Result<(), MappingError> {
    check_reference(what, path)?;
    if path.split('.').any(str::is_empty) {
        return Err(invalid(format!(
            "{what} {path:?} must be dot-separated non-empty field names"
        )));
    }
    Ok(())
}

impl StreamRule {
    /// The rule on its own (GR1 Task 19).
    fn check(&self) -> Result<(), MappingError> {
        check_name("stream rule name", &self.name)?;
        if self.event_type.len() > 255 {
            return Err(invalid(format!(
                "rule {:?}'s event type is longer than 255 bytes",
                self.name
            )));
        }
        let optional_path = |what: &str, path: &str| {
            if path.is_empty() {
                Ok(())
            } else {
                check_path(what, path)
            }
        };
        optional_path("properties path", &self.properties_field)?;
        if self.op.is_edge() {
            for (end, label, path) in [
                ("source", &self.source_label, &self.source_key_field),
                (
                    "destination",
                    &self.destination_label,
                    &self.destination_key_field,
                ),
            ] {
                check_name(&format!("rule {:?}'s {end} label", self.name), label)?;
                check_path(&format!("rule {:?}'s {end} key path", self.name), path)?;
            }
            match (self.edge_type.is_empty(), self.edge_type_field.is_empty()) {
                (false, true) => check_reference("edge type", &self.edge_type)?,
                (true, false) => check_path("edge type path", &self.edge_type_field)?,
                _ => {
                    return Err(invalid(format!(
                        "edge rule {:?} names its type or the path of its type, one of the two",
                        self.name
                    )));
                }
            }
            optional_path("edge key path", &self.edge_key_field)?;
        } else {
            check_name(&format!("rule {:?}'s label", self.name), &self.label)?;
            check_path(&format!("rule {:?}'s key path", self.name), &self.key_field)?;
        }
        Ok(())
    }

    fn from_proto(rule: &pb::StreamRule) -> Result<Self, MappingError> {
        let op = match rule.operation.as_known() {
            Some(pb::StreamOperation::UpsertNode) => StreamOp::UpsertNode,
            Some(pb::StreamOperation::DeleteNode) => StreamOp::DeleteNode,
            Some(pb::StreamOperation::UpsertEdge) => StreamOp::UpsertEdge,
            Some(pb::StreamOperation::DeleteEdge) => StreamOp::DeleteEdge,
            _ => {
                return Err(invalid(format!(
                    "stream rule {:?} names no operation",
                    rule.name
                )));
            }
        };
        Ok(Self {
            name: rule.name.clone(),
            event_type: rule.event_type.clone(),
            op,
            label: rule.label.clone(),
            key_field: rule.key_field.clone(),
            source_label: rule.source_label.clone(),
            source_key_field: rule.source_key_field.clone(),
            destination_label: rule.destination_label.clone(),
            destination_key_field: rule.destination_key_field.clone(),
            edge_type: rule.edge_type.clone(),
            edge_type_field: rule.edge_type_field.clone(),
            edge_key_field: rule.edge_key_field.clone(),
            properties_field: rule.properties_field.clone(),
        })
    }

    fn to_proto(&self) -> pb::StreamRule {
        pb::StreamRule {
            name: self.name.clone(),
            event_type: self.event_type.clone(),
            operation: match self.op {
                StreamOp::UpsertNode => pb::StreamOperation::UpsertNode,
                StreamOp::DeleteNode => pb::StreamOperation::DeleteNode,
                StreamOp::UpsertEdge => pb::StreamOperation::UpsertEdge,
                StreamOp::DeleteEdge => pb::StreamOperation::DeleteEdge,
            }
            .into(),
            label: self.label.clone(),
            key_field: self.key_field.clone(),
            source_label: self.source_label.clone(),
            source_key_field: self.source_key_field.clone(),
            destination_label: self.destination_label.clone(),
            destination_key_field: self.destination_key_field.clone(),
            edge_type: self.edge_type.clone(),
            edge_type_field: self.edge_type_field.clone(),
            edge_key_field: self.edge_key_field.clone(),
            properties_field: self.properties_field.clone(),
            ..Default::default()
        }
    }
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
        if self.vertices.is_empty() && self.streams.is_empty() {
            return Err(invalid(
                "a LINKED graph's mapping declares at least one vertex label or stream",
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
        // Rules are named among the edges, so an error or a dead letter names one thing.
        let rules: usize = self.streams.iter().map(|s| s.rules.len()).sum();
        if rules > MAX_STREAM_RULES {
            return Err(invalid(format!(
                "a mapping's streams declare at most {MAX_STREAM_RULES} rules"
            )));
        }
        let mut streams = BTreeSet::new();
        for stream in &self.streams {
            check_reference("stream", &stream.stream)?;
            if stream.stream.starts_with('_') {
                return Err(invalid(format!(
                    "stream {:?} is reserved; a LINKED graph reads a collection through its                      collection mapping",
                    stream.stream
                )));
            }
            if !streams.insert(stream.stream.as_str()) {
                return Err(invalid(format!(
                    "stream {:?} is mapped twice",
                    stream.stream
                )));
            }
            if stream.rules.is_empty() {
                return Err(invalid(format!("stream {:?} has no rules", stream.stream)));
            }
            for rule in &stream.rules {
                rule.check()?;
                if !names.insert(rule.name.as_str()) {
                    return Err(invalid(format!(
                        "the name {:?} is declared twice among the edges and stream rules",
                        rule.name
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

    /// The mapping a request carries, refusing a stream rule with no operation.
    ///
    /// # Errors
    ///
    /// [`MappingError`] for a stream rule whose operation is unspecified or unknown.
    pub fn try_from_proto(mapping: &pb::GraphMapping) -> Result<Self, MappingError> {
        let mut out = Self::from_proto(mapping);
        out.streams = mapping
            .streams
            .iter()
            .map(|s| {
                Ok(StreamMap {
                    stream: s.stream.clone(),
                    rules: s
                        .rules
                        .iter()
                        .map(StreamRule::from_proto)
                        .collect::<Result<_, MappingError>>()?,
                })
            })
            .collect::<Result<_, MappingError>>()?;
        Ok(out)
    }

    /// The mapping a request carries. A stream rule with no known operation is dropped; a
    /// request goes through [`GraphMapping::try_from_proto`], which refuses it.
    #[must_use]
    pub fn from_proto(mapping: &pb::GraphMapping) -> Self {
        Self {
            streams: mapping
                .streams
                .iter()
                .map(|s| StreamMap {
                    stream: s.stream.clone(),
                    rules: s
                        .rules
                        .iter()
                        .filter_map(|r| StreamRule::from_proto(r).ok())
                        .collect(),
                })
                .collect(),
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
            streams: self
                .streams
                .iter()
                .map(|s| pb::StreamMapping {
                    stream: s.stream.clone(),
                    rules: s.rules.iter().map(StreamRule::to_proto).collect(),
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
            streams: Vec::new(),
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

    fn streamed() -> GraphMapping {
        let rule = |name: &str, op| StreamRule {
            name: name.to_string(),
            event_type: "person.upserted".to_string(),
            op,
            label: "Person".to_string(),
            key_field: "data.id".to_string(),
            source_label: "Person".to_string(),
            source_key_field: "data.from".to_string(),
            destination_label: "Person".to_string(),
            destination_key_field: "data.to".to_string(),
            edge_type: "KNOWS".to_string(),
            edge_type_field: String::new(),
            edge_key_field: String::new(),
            properties_field: String::new(),
        };
        GraphMapping {
            streams: vec![StreamMap {
                stream: "events".to_string(),
                rules: vec![
                    rule("person", StreamOp::UpsertNode),
                    rule("knows", StreamOp::UpsertEdge),
                ],
            }],
            ..kg()
        }
    }

    #[test]
    fn stream_mappings_round_trip_and_are_checked() {
        let mapping = streamed();
        assert_eq!(mapping.check(), Ok(()));
        assert_eq!(
            GraphMapping::try_from_proto(&mapping.to_proto()),
            Ok(mapping.clone())
        );
        let json = serde_json::to_string(&mapping).expect("json");
        assert_eq!(
            serde_json::from_str::<GraphMapping>(&json).expect("parse"),
            mapping
        );
        // A streams-only mapping needs no vertex label.
        let only = GraphMapping {
            vertices: Vec::new(),
            edges: Vec::new(),
            ..streamed()
        };
        assert_eq!(only.check(), Ok(()));
        // An operation the wire does not name is refused.
        let mut wire = mapping.to_proto();
        wire.streams[0].rules[0].operation = buffa::EnumValue::from(0);
        assert!(GraphMapping::try_from_proto(&wire).is_err());
        let refused: Vec<(&str, GraphMapping)> = vec![
            ("a reserved stream", {
                let mut m = streamed();
                m.streams[0].stream = "_c1".to_string();
                m
            }),
            ("a rule named as an edge", {
                let mut m = streamed();
                m.streams[0].rules[0].name = "RELATED".to_string();
                m
            }),
            ("both a type and its path", {
                let mut m = streamed();
                m.streams[0].rules[1].edge_type_field = "data.ty".to_string();
                m
            }),
            ("an empty path segment", {
                let mut m = streamed();
                m.streams[0].rules[0].key_field = "data..id".to_string();
                m
            }),
            ("no rules", {
                let mut m = streamed();
                m.streams[0].rules.clear();
                m
            }),
        ];
        for (what, mapping) in refused {
            assert!(mapping.check().is_err(), "{what}");
        }
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
