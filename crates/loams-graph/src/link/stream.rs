//! The `stream → graph` link (GR1 Task 19; design §48 §10.2).
//!
//! A [`StreamMap`] reads a Loams stream whose records are JSON objects or CloudEvents: in
//! structured mode (a JSON object with `specversion` and `type`), or in binary mode (`ce_*` or
//! `ce-*` headers, the data as the value). Every rule a record matches applies, in order; a rule
//! with an `event_type` matches only CloudEvents of that type. A record no rule matches is
//! skipped.
//!
//! Each rule is one operation ([`StreamOp`]), keyed by field paths from the record's root (a
//! CloudEvent's attributes and `data.<field>`):
//!
//! * a node upsert merges the record's properties into the node `(label, key)`, creating it (or
//!   filling a stub); a node delete removes the node and its edges;
//! * an edge upsert merges into the edge the stream knows by its key (the rule's edge key path,
//!   or its type and endpoints), between the nodes its endpoint paths key, making stubs for endpoints that do not exist, and replaces the edge when its type
//!   or endpoints changed; an edge delete removes it, and stubs it leaves without edges.
//!
//! A record a matching rule cannot read (not JSON, a key that is missing or not an integer or a
//! string, a type that is not a string, properties that are not an object) is **dead-lettered**:
//! written as one JSON object under the graph's bucket prefix, `deadletters/<link>/`, and the
//! link goes on past it. Nothing of a dead-lettered record is applied.

use grafeo::Value;
use loams_common::NamespaceId;
use loams_common::meta::LinkId;
use loams_link::LinkError;
use loams_log::OffsetRecord;
use loams_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

use super::writer::{Writer, json_value};
use super::{
    EDGE_PROPERTY, KEY_PROPERTY, Key, LINK_PROPERTIES, LinkIndex, STREAM_PROPERTY, STUB_PROPERTY,
};
use crate::catalog::GraphMeta;
use crate::changeset::{ChangeOp, PropMap};
use crate::engine::GraphError;
use crate::mapping::{StreamMap, StreamOp, StreamRule};

/// The most bytes of a dead-lettered record's value its dead letter keeps.
pub const DEAD_LETTER_VALUE_BYTES: usize = 4096;

/// One operation a record resolved to, ready to apply.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Resolved {
    UpsertNode {
        label: String,
        key: Key,
        props: PropMap,
    },
    DeleteNode {
        label: String,
        key: Key,
    },
    UpsertEdge {
        name: String,
        key: Key,
        ty: String,
        source: (String, Key),
        destination: (String, Key),
        props: PropMap,
    },
    DeleteEdge {
        name: String,
        key: Key,
    },
}

/// A record the link could not read, as its dead-letter object holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The stream partition.
    pub partition: u32,
    /// The record's offset.
    pub offset: u64,
    /// The rule that could not read it, when one matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    /// Why.
    pub reason: String,
    /// The record's value, as UTF-8 (lossy), at most [`DEAD_LETTER_VALUE_BYTES`].
    pub value: String,
}

/// What one stream writes into a graph.
#[derive(Debug)]
pub(crate) struct Plan {
    stream: String,
    /// The edges' identity space in the link index: `stream:<name>`, which no collection edge
    /// name can be (`:` is not in one), so a stream's edges are its own and every rule of the
    /// stream finds them.
    edges: String,
    rules: Vec<StreamRule>,
}

/// The path of a link's dead letters under its graph's prefix.
pub(crate) fn dead_letter_prefix(
    namespace: NamespaceId,
    graph: &GraphMeta,
    link: LinkId,
) -> String {
    let prefix = graph.graph_id().map_or_else(
        |_| format!("ns/{namespace}/graphs/{}/", graph.id),
        |id| crate::snapshot::graph_prefix(namespace, id),
    );
    format!("{prefix}deadletters/{}/", link.0)
}

/// Writes `letter` as its own object, named by its partition and offset, so a retried batch
/// writes the same object again rather than a second one.
pub(crate) async fn write_dead_letter(
    store: &Store,
    namespace: NamespaceId,
    graph: &GraphMeta,
    link: LinkId,
    letter: &DeadLetter,
) -> Result<(), LinkError> {
    let path = format!(
        "{}{:010}-{:020}.json",
        dead_letter_prefix(namespace, graph, link),
        letter.partition,
        letter.offset
    );
    let body = serde_json::to_vec(letter).map_err(|err| LinkError::Corrupt(err.to_string()))?;
    store.put(&path, bytes::Bytes::from(body)).await?;
    tracing::warn!(stream_partition = letter.partition, offset = letter.offset, reason = %letter.reason, "a stream record a LINKED graph cannot read was dead-lettered");
    Ok(())
}

/// Reads the dead letters under `prefix`, in partition and offset order.
pub(crate) async fn read_dead_letters(
    store: &Store,
    prefix: &str,
) -> Result<Vec<DeadLetter>, LinkError> {
    let mut objects = store.list(prefix).await?;
    objects.sort_by(|a, b| a.path.cmp(&b.path));
    let mut out = Vec::with_capacity(objects.len());
    for object in objects {
        let (body, _) = store.get(&object.path).await?;
        out.push(serde_json::from_slice(&body).map_err(|err| LinkError::Corrupt(err.to_string()))?);
    }
    Ok(out)
}

/// The record's root: a JSON object, a CloudEvent in structured mode, or one in binary mode
/// rebuilt from its `ce_` headers with its value as `data`. Also whether it is a CloudEvent.
fn root(record: &OffsetRecord) -> Result<(Map<String, Json>, bool), String> {
    let value = record.record.value.as_deref().unwrap_or_default();
    let mut attributes = Map::new();
    for (name, header) in &record.record.headers {
        let Some(attribute) = name
            .strip_prefix("ce_")
            .or_else(|| name.strip_prefix("ce-"))
        else {
            continue;
        };
        let text = header
            .as_deref()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .unwrap_or_default();
        attributes.insert(attribute.to_string(), Json::String(text));
    }
    if attributes.contains_key("type") {
        let data = if value.is_empty() {
            Json::Null
        } else {
            serde_json::from_slice(value)
                .map_err(|err| format!("the CloudEvent's data is not JSON: {err}"))?
        };
        attributes.insert("data".to_string(), data);
        return Ok((attributes, true));
    }
    match serde_json::from_slice::<Json>(value) {
        Ok(Json::Object(map)) => {
            let event = map.get("specversion").is_some_and(Json::is_string)
                && map.get("type").is_some_and(Json::is_string);
            Ok((map, event))
        }
        Ok(_) => Err("the record is not a JSON object".to_string()),
        Err(err) => Err(format!("the record is not JSON: {err}")),
    }
}

fn at<'a>(root: &'a Map<String, Json>, path: &str) -> Option<&'a Json> {
    let mut segments = path.split('.');
    let mut value = root.get(segments.next()?)?;
    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }
    Some(value)
}

fn key_at(root: &Map<String, Json>, path: &str) -> Result<Key, String> {
    at(root, path)
        .and_then(Key::of_json)
        .ok_or_else(|| format!("{path:?} holds no key (an integer or a string)"))
}

/// A key's text, for an edge identified by its type and endpoints.
fn key_text(key: &Key) -> String {
    match key {
        Key::Int(n) => format!("i{n}"),
        Key::Str(s) => format!("s{s}"),
    }
}

impl Plan {
    pub(crate) fn new(map: &StreamMap) -> Self {
        Self {
            stream: map.stream.clone(),
            edges: format!("stream:{}", map.stream),
            rules: map.rules.clone(),
        }
    }

    /// What `record` asks for, or its dead letter.
    pub(crate) fn resolve(
        &self,
        partition: u32,
        record: &OffsetRecord,
    ) -> Result<Vec<Resolved>, DeadLetter> {
        let letter = |rule: Option<&StreamRule>, reason: String| {
            let value = record.record.value.as_deref().unwrap_or_default();
            let mut text =
                String::from_utf8_lossy(&value[..value.len().min(DEAD_LETTER_VALUE_BYTES)])
                    .into_owned();
            if text.ends_with('\u{fffd}') && value.len() > DEAD_LETTER_VALUE_BYTES {
                text.pop();
            }
            DeadLetter {
                partition,
                offset: record.offset,
                rule: rule.map(|r| r.name.clone()),
                reason,
                value: text,
            }
        };
        let (root, event) = root(record).map_err(|reason| letter(None, reason))?;
        let event_type = event
            .then(|| root.get("type").and_then(Json::as_str))
            .flatten();
        let mut out = Vec::new();
        for rule in &self.rules {
            if !rule.event_type.is_empty() && event_type != Some(rule.event_type.as_str()) {
                continue;
            }
            out.push(
                self.rule(rule, &root, event)
                    .map_err(|reason| letter(Some(rule), reason))?,
            );
        }
        Ok(out)
    }

    fn rule(
        &self,
        rule: &StreamRule,
        root: &Map<String, Json>,
        event: bool,
    ) -> Result<Resolved, String> {
        let props = || -> Result<PropMap, String> {
            let object = if rule.properties_field.is_empty() {
                if event {
                    match root.get("data") {
                        Some(Json::Object(data)) => Some(data),
                        _ => None,
                    }
                } else {
                    Some(root)
                }
            } else {
                match at(root, &rule.properties_field) {
                    Some(Json::Object(object)) => Some(object),
                    None => None,
                    Some(_) => {
                        return Err(format!(
                            "{:?} is not an object of properties",
                            rule.properties_field
                        ));
                    }
                }
            };
            Ok(object
                .map(|object| {
                    object
                        .iter()
                        .filter(|(k, v)| !v.is_null() && !LINK_PROPERTIES.contains(&k.as_str()))
                        .map(|(k, v)| (k.clone(), json_value(v)))
                        .collect()
                })
                .unwrap_or_default())
        };
        match rule.op {
            StreamOp::UpsertNode => Ok(Resolved::UpsertNode {
                label: rule.label.clone(),
                key: key_at(root, &rule.key_field)?,
                props: props()?,
            }),
            StreamOp::DeleteNode => Ok(Resolved::DeleteNode {
                label: rule.label.clone(),
                key: key_at(root, &rule.key_field)?,
            }),
            StreamOp::UpsertEdge | StreamOp::DeleteEdge => {
                let ty = if rule.edge_type_field.is_empty() {
                    rule.edge_type.clone()
                } else {
                    at(root, &rule.edge_type_field)
                        .and_then(Json::as_str)
                        .filter(|ty| !ty.is_empty())
                        .ok_or_else(|| format!("{:?} holds no edge type", rule.edge_type_field))?
                        .to_string()
                };
                let source = (
                    rule.source_label.clone(),
                    key_at(root, &rule.source_key_field)?,
                );
                let destination = (
                    rule.destination_label.clone(),
                    key_at(root, &rule.destination_key_field)?,
                );
                let key = if rule.edge_key_field.is_empty() {
                    Key::Str(format!(
                        "{ty}\u{1f}{}\u{1f}{}",
                        key_text(&source.1),
                        key_text(&destination.1)
                    ))
                } else {
                    key_at(root, &rule.edge_key_field)?
                };
                Ok(if rule.op == StreamOp::UpsertEdge {
                    Resolved::UpsertEdge {
                        name: self.edges.clone(),
                        key,
                        ty,
                        source,
                        destination,
                        props: props()?,
                    }
                } else {
                    Resolved::DeleteEdge {
                        name: self.edges.clone(),
                        key,
                    }
                })
            }
        }
    }

    /// Applies `ops` to `db`, in order, and answers the change operations that did it.
    pub(crate) fn apply(
        &self,
        db: &grafeo::GrafeoDB,
        index: &mut LinkIndex,
        ops: &[Resolved],
    ) -> Result<Vec<ChangeOp>, GraphError> {
        let mut w = Writer {
            db,
            index,
            ops: Vec::new(),
        };
        let stream = Value::String(self.stream.as_str().into());
        for op in ops {
            match op {
                Resolved::UpsertNode { label, key, props } => {
                    let mut merged = match w.index.vertex(label, key) {
                        Some(id) => w.node_props(id),
                        None => PropMap::new(),
                    };
                    merged.remove(STUB_PROPERTY);
                    merged.extend(props.clone());
                    merged.insert(STREAM_PROPERTY.to_string(), stream.clone());
                    merged.insert(KEY_PROPERTY.to_string(), key.value());
                    w.upsert_vertex(label, key.clone(), merged, None)?;
                }
                Resolved::DeleteNode { label, key } => {
                    if let Some(id) = w.index.vertex(label, key) {
                        w.delete_vertex(id)?;
                    }
                }
                Resolved::UpsertEdge {
                    name,
                    key,
                    ty,
                    source,
                    destination,
                    props,
                } => {
                    let src = w.endpoint(&source.0, source.1.clone())?;
                    let dst = w.endpoint(&destination.0, destination.1.clone())?;
                    let current = w.index.edge(name, key);
                    let mut merged = current.map(|id| w.edge_props(id)).unwrap_or_default();
                    merged.extend(props.clone());
                    merged.insert(STREAM_PROPERTY.to_string(), stream.clone());
                    merged.insert(KEY_PROPERTY.to_string(), key.value());
                    merged.insert(
                        EDGE_PROPERTY.to_string(),
                        Value::String(name.as_str().into()),
                    );
                    w.upsert_edge(name, key.clone(), current, ty.clone(), src, dst, merged)?;
                }
                Resolved::DeleteEdge { name, key } => {
                    if let Some(id) = w.index.edge(name, key) {
                        w.delete_edge(id)?;
                    }
                }
            }
        }
        Ok(w.ops)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use loams_log::Record;

    use super::*;
    use crate::mapping::StreamRule;

    fn record(value: &str, headers: Vec<(&str, &str)>) -> OffsetRecord {
        OffsetRecord {
            offset: 7,
            record: Record {
                key: None,
                value: Some(Bytes::from(value.to_string())),
                headers: headers
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), Some(Bytes::from(v.to_string()))))
                    .collect(),
                timestamp_ms: 0,
            },
        }
    }

    fn node_rule() -> StreamRule {
        StreamRule {
            name: "person".to_string(),
            event_type: "person.updated".to_string(),
            op: StreamOp::UpsertNode,
            label: "Person".to_string(),
            key_field: "data.id".to_string(),
            source_label: String::new(),
            source_key_field: String::new(),
            destination_label: String::new(),
            destination_key_field: String::new(),
            edge_type: String::new(),
            edge_type_field: String::new(),
            edge_key_field: String::new(),
            properties_field: String::new(),
        }
    }

    #[test]
    fn structured_and_binary_cloudevents_resolve_alike() {
        let plan = Plan::new(&StreamMap {
            stream: "events".to_string(),
            rules: vec![node_rule()],
        });
        let structured = record(
            r#"{"specversion":"1.0","type":"person.updated","id":"e1","data":{"id":4,"name":"Ann"}}"#,
            vec![],
        );
        let binary = record(
            r#"{"id":4,"name":"Ann"}"#,
            vec![
                ("ce_specversion", "1.0"),
                ("ce_type", "person.updated"),
                ("ce_id", "e1"),
            ],
        );
        let a = plan.resolve(0, &structured).expect("structured");
        assert_eq!(a, plan.resolve(0, &binary).expect("binary"));
        assert!(matches!(
            &a[..],
            [Resolved::UpsertNode {
                key: Key::Int(4),
                ..
            }]
        ));
        // Another type matches no rule.
        let other = record(
            r#"{"specversion":"1.0","type":"x","data":{"id":4}}"#,
            vec![],
        );
        assert_eq!(plan.resolve(0, &other).expect("skipped"), Vec::new());
        // A matching record without the key is dead-lettered.
        let bad = record(
            r#"{"specversion":"1.0","type":"person.updated","data":{}}"#,
            vec![],
        );
        let letter = plan.resolve(3, &bad).expect_err("no key");
        assert_eq!((letter.partition, letter.offset), (3, 7));
        assert_eq!(letter.rule.as_deref(), Some("person"));
        let not_json = record("{", vec![]);
        assert!(plan.resolve(0, &not_json).is_err());
    }
}
