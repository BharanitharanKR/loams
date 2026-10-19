//! `QueryService/Search`'s `expand` stage on Loams Graph (GR1 Task 20; design §07 §4, §5.2,
//! §48 §10.3; D743).
//!
//! [`LoamsGraphExpander`] answers `loams-query`'s [`GraphExpander`] with Loams-generated GQL:
//! three read-only, parameterized statements (the seeds' vertices, the first hop, the second
//! hop), each run through the graph's own `Execute` path, so routing, followers and consistency
//! tokens are a client read's (§48 §7.2). Every value is a parameter; the statement text varies
//! only in the arrow of the edge pattern, one of three fixed strings.
//!
//! The answer is shaped in Loams, not by the engine (§07 §4):
//!
//! * a vertex is identified by its label and its `_key` (what the links write; an OWNED graph's
//!   vertices need a `_key` to be reached), and a seed is the vertices with its key (of
//!   `seed_label`, when given);
//! * each seed's reached vertices are deduplicated at the smallest hop; among equal-hop paths,
//!   the one whose previous vertex (`via`), then edge, has the smallest canonical key bytes is
//!   kept, so the answer never depends on the engine's iteration order;
//! * `edge_filter` prunes the edges walked and `vertex_filter` the vertices reached (a vertex
//!   that fails it is neither answered nor walked through);
//! * each seed keeps `limit_per_seed` vertices ordered by hop, then canonical key bytes (no
//!   mapping declares an edge weight, so §07 §4's weight order does not apply), and the whole
//!   answer keeps `limit` in the same order, seed order breaking ties.
//!
//! The second statement and the third read at least the state the first one answered, so one
//! expansion never sees the graph go back in time.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
use connectrpc::{ConnectError, ErrorCode};
use grafeo::Value;
use loams_collection::PrimaryKey;
use loams_proto::loams::graph::v1 as pb;
use loams_query::ServiceError;
use loams_query::expand::{ExpandCall, ExpandDirection, Expanded, GraphExpander, Reached};
use serde_json::Map;

use super::filter;
use crate::consistency::Consistency;
use crate::service::admin::GraphAdmin;

/// Loams Graph as `loams-query`'s graph expander.
#[derive(Clone)]
pub struct LoamsGraphExpander {
    admin: GraphAdmin,
}

impl std::fmt::Debug for LoamsGraphExpander {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoamsGraphExpander").finish_non_exhaustive()
    }
}

impl LoamsGraphExpander {
    /// The graphs it reads.
    #[must_use]
    pub fn admin(&self) -> &GraphAdmin {
        &self.admin
    }

    /// An expander over `admin`'s graphs.
    #[must_use]
    pub fn new(admin: GraphAdmin) -> Self {
        Self { admin }
    }
}

/// The edge pattern of a direction.
pub(super) fn arrow(direction: ExpandDirection) -> &'static str {
    match direction {
        ExpandDirection::Out => "-[e]->",
        ExpandDirection::In => "<-[e]-",
        ExpandDirection::Both => "-[e]-",
    }
}

/// A key as a property value: a `u64` that fits as an integer, a UUID hyphenated, a string.
pub(super) fn key_value(pk: &PrimaryKey) -> Value {
    crate::link::Key::of_pk(pk).value()
}

/// A key's identity as a map key (`Value` is neither `Hash` nor `Ord`).
pub(super) fn ident(value: &Value) -> String {
    format!("{value:?}")
}

/// A property value as a key: a non-negative integer, else its text.
pub(super) fn primary_key(value: &Value) -> Option<PrimaryKey> {
    match value {
        Value::Int64(n) => {
            Some(u64::try_from(*n).map_or_else(|_| PrimaryKey::Str(n.to_string()), PrimaryKey::U64))
        }
        Value::String(s) => Some(PrimaryKey::Str(s.to_string())),
        _ => None,
    }
}

/// A property map as JSON, for the filters.
pub(super) fn json_props(value: &Value) -> Map<String, serde_json::Value> {
    match value {
        Value::Map(map) => map
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), json(v)))
            .collect(),
        _ => Map::new(),
    }
}

fn json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(i) => serde_json::Value::from(*i),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(json).collect()),
        Value::Map(_) => serde_json::Value::Object(json_props(value)),
        other => serde_json::Value::String(other.to_string()),
    }
}

pub(super) fn prop<'a>(props: &'a Value, name: &str) -> Option<&'a Value> {
    match props {
        Value::Map(map) => map.iter().find(|(k, _)| k.as_str() == name).map(|(_, v)| v),
        _ => None,
    }
}

pub(super) fn first_label(labels: &Value) -> Option<String> {
    match labels {
        Value::List(items) => items.iter().find_map(|v| match v {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        }),
        _ => None,
    }
}

pub(super) fn graph_error(err: &ConnectError, graph: &str) -> ServiceError {
    let message = err
        .message
        .clone()
        .unwrap_or_else(|| format!("{:?}", err.code));
    match err.code {
        ErrorCode::NotFound => ServiceError::NotFound {
            kind: "graph",
            name: graph.to_string(),
        },
        ErrorCode::InvalidArgument | ErrorCode::FailedPrecondition | ErrorCode::OutOfRange => {
            ServiceError::InvalidArgument(format!("expand on graph {graph}: {message}"))
        }
        ErrorCode::Unavailable | ErrorCode::DeadlineExceeded | ErrorCode::Aborted => {
            ServiceError::Unavailable(format!("expand on graph {graph}: {message}"))
        }
        ErrorCode::ResourceExhausted => ServiceError::InvalidArgument(format!(
            "expand on graph {graph} reaches more than one statement may answer: {message}; \
             narrow edge_types, hops or seeds"
        )),
        _ => ServiceError::Internal(format!("expand on graph {graph}: {message}")),
    }
}

/// An edge's key: its `_key`, else its engine id.
pub(super) fn edge_pk(props: &Value, id: &Value) -> PrimaryKey {
    prop(props, crate::link::KEY_PROPERTY)
        .and_then(primary_key)
        .or_else(|| primary_key(id))
        .unwrap_or_else(|| PrimaryKey::Str(String::new()))
}

/// A vertex from its labels and properties: its label, its `_key`, its key as a document id
/// (`_pk` when a field keys it) and its collection; `None` without a `_key`.
pub(super) fn vertex_of(
    labels: &Value,
    props: &Value,
) -> Option<(String, Value, PrimaryKey, Option<String>)> {
    let key = prop(props, crate::link::KEY_PROPERTY)?.clone();
    let pk = prop(props, crate::link::PK_PROPERTY)
        .and_then(primary_key)
        .or_else(|| primary_key(&key))?;
    let collection = match prop(props, crate::link::COLLECTION_PROPERTY) {
        Some(Value::String(c)) => Some(c.to_string()),
        _ => None,
    };
    Some((first_label(labels)?, key, pk, collection))
}

/// One reached vertex while a seed is expanded: the identity and the path kept.
#[derive(Clone, Debug)]
struct Candidate {
    label: String,
    pk: PrimaryKey,
    hop: u8,
    via: PrimaryKey,
    edge_type: String,
    edge: PrimaryKey,
    collection: Option<String>,
}

impl Candidate {
    /// Whether this path beats `other` to the same vertex (§07 §4).
    fn better_than(&self, other: &Self) -> bool {
        (self.hop, self.via.canonical(), self.edge.canonical())
            < (other.hop, other.via.canonical(), other.edge.canonical())
    }

    fn order(&self) -> (u8, Vec<u8>, String) {
        (self.hop, self.pk.canonical(), self.label.clone())
    }
}

/// The rows a statement answered, with the graph state it read.
pub(super) struct Answer {
    pub(super) rows: Vec<Vec<Value>>,
    pub(super) token: String,
}

impl LoamsGraphExpander {
    pub(super) async fn run(
        &self,
        namespace: &str,
        graph: &str,
        statement: &str,
        parameters: Vec<(&str, Value)>,
        consistency: &Consistency,
    ) -> Result<Answer, ServiceError> {
        let mut request = pb::ExecuteRequest {
            namespace: namespace.to_string(),
            graph: graph.to_string(),
            statement: statement.to_string(),
            read_only: true,
            consistency: consistency.to_proto().into(),
            ..Default::default()
        };
        for (name, value) in parameters {
            request
                .parameters
                .insert(name.to_string(), crate::value::to_proto(&value));
        }
        let response = self
            .admin
            .execute(request)
            .await
            .map_err(|err| graph_error(&err, graph))?;
        if response.truncated {
            return Err(ServiceError::InvalidArgument(format!(
                "expand on graph {graph} reaches more edges than one statement answers; narrow \
                 edge_types, hops or seeds"
            )));
        }
        let rows = response
            .rows
            .as_option()
            .map(|set| {
                set.rows
                    .iter()
                    .map(|row| {
                        row.values
                            .iter()
                            .map(|v| crate::value::from_proto(v).unwrap_or(Value::Null))
                            .collect()
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Answer {
            rows,
            token: response.consistency_token,
        })
    }
}

/// The clauses every hop statement shares: the seed or frontier label, the edge types.
pub(super) fn clauses(label: bool, types: bool) -> String {
    let mut out = String::new();
    if label {
        out.push_str(" AND $label IN labels(s)");
    }
    if types {
        out.push_str(" AND type(e) IN $types");
    }
    out
}

#[async_trait]
impl GraphExpander for LoamsGraphExpander {
    async fn expand(&self, call: ExpandCall<'_>) -> Result<Expanded, ServiceError> {
        let e = call.expand;
        // Before anything is read (GR1 R24.12): a principal needs `reader` on the graph.
        call.caller
            .authorize_read("expand", call.namespace, &e.graph)
            .await?;
        if let Some(q) = &e.edge_filter {
            filter::check(q, "edge_filter")?;
        }
        if let Some(q) = &e.vertex_filter {
            filter::check(q, "vertex_filter")?;
        }
        let ns = call.namespace;
        let mut consistency = match &e.consistency_token {
            Some(token) => Consistency::from_proto(Some(&pb::Consistency {
                level: Some(
                    loams_proto::loams::graph::v1::__buffa::oneof::consistency::Level::AtLeast(
                        token.clone(),
                    ),
                ),
                ..Default::default()
            }))
            .map_err(|err| {
                ServiceError::InvalidArgument(format!("expand.consistency_token: {err}"))
            })?,
            None => Consistency::Strong,
        };
        let mut seed_keys: Vec<Value> = Vec::new();
        for pk in call.seeds.iter().flatten() {
            let value = key_value(pk);
            if !seed_keys.contains(&value) {
                seed_keys.push(value);
            }
        }
        let label = e.seed_label.clone();
        let types: Vec<Value> = e
            .edge_types
            .iter()
            .map(|t| Value::String(t.as_str().into()))
            .collect();
        let mut base = vec![("seeds", Value::List(seed_keys.clone().into()))];
        if let Some(label) = &label {
            base.push(("label", Value::String(label.as_str().into())));
        }
        // 1. Which seeds have a vertex.
        let statement = format!(
            "MATCH (s) WHERE s._key IN $seeds{} RETURN DISTINCT s._key AS key",
            clauses(label.is_some(), false)
        );
        let found = self
            .run(ns, &e.graph, &statement, base.clone(), &consistency)
            .await?;
        if !found.token.is_empty()
            && let Ok(at) = found.token.parse::<crate::durable::ConsistencyToken>()
        {
            consistency = Consistency::AtLeast(at);
        }
        let existing: Vec<Value> = found
            .rows
            .into_iter()
            .filter_map(|r| r.into_iter().next())
            .collect();
        let skipped: Vec<usize> = call
            .seeds
            .iter()
            .enumerate()
            .filter(|(_, pk)| {
                pk.as_ref()
                    .is_some_and(|pk| !existing.contains(&key_value(pk)))
            })
            .map(|(i, _)| i)
            .collect();
        if existing.is_empty() {
            return Ok(Expanded {
                reached: Vec::new(),
                skipped,
                token: Some(found.token).filter(|t| !t.is_empty()),
            });
        }
        let pattern = arrow(e.direction);
        let vertex_ok = |props: &Value| {
            e.vertex_filter
                .as_ref()
                .is_none_or(|q| filter::matches(q, &json_props(props)))
        };
        let edge_ok = |props: &Value| {
            e.edge_filter
                .as_ref()
                .is_none_or(|q| filter::matches(q, &json_props(props)))
        };
        let reached = vertex_of;
        // 2. The first hop from every seed vertex.
        let mut params = base.clone();
        params[0] = ("seeds", Value::List(existing.clone().into()));
        if !types.is_empty() {
            params.push(("types", Value::List(types.clone().into())));
        }
        let statement = format!(
            "MATCH (s){pattern}(n) WHERE s._key IN $seeds{} RETURN s._key AS seed, id(e) AS eid, \
             type(e) AS ty, properties(e) AS ep, labels(n) AS nl, properties(n) AS np",
            clauses(label.is_some(), !types.is_empty())
        );
        let hop1 = self
            .run(ns, &e.graph, &statement, params, &consistency)
            .await?;
        let mut token = hop1.token.clone();
        if let Ok(at) = hop1.token.parse::<crate::durable::ConsistencyToken>() {
            consistency = Consistency::AtLeast(at);
        }
        // Per seed key: its reached vertices by (label, key).
        let mut best: HashMap<String, BTreeMap<(String, Vec<u8>), Candidate>> = HashMap::new();
        let seed_pk =
            |key: &Value| primary_key(key).unwrap_or_else(|| PrimaryKey::Str(String::new()));
        let is_seed = |seed: &Value, label: &str, key: &Value| {
            key == seed && e.seed_label.as_deref().is_none_or(|l| l == label)
        };
        let mut frontier: BTreeSet<(String, Vec<u8>)> = BTreeSet::new();
        let mut frontier_keys: Vec<Value> = Vec::new();
        for row in &hop1.rows {
            let [seed, eid, ty, ep, nl, np] = row.as_slice() else {
                continue;
            };
            if !edge_ok(ep) || !vertex_ok(np) {
                continue;
            }
            let Some((nlabel, nkey, npk, collection)) = reached(nl, np) else {
                continue;
            };
            if is_seed(seed, &nlabel, &nkey) {
                continue;
            }
            let candidate = Candidate {
                label: nlabel.clone(),
                pk: npk,
                hop: 1,
                via: seed_pk(seed),
                edge_type: match ty {
                    Value::String(s) => s.to_string(),
                    _ => String::new(),
                },
                edge: edge_pk(ep, eid),
                collection,
            };
            let id = (nlabel, candidate.pk.canonical());
            if frontier.insert(id.clone()) && !frontier_keys.contains(&nkey) {
                frontier_keys.push(nkey);
            }
            let per_seed = best.entry(ident(seed)).or_default();
            match per_seed.get(&id) {
                Some(kept) if !candidate.better_than(kept) => {}
                _ => {
                    per_seed.insert(id, candidate);
                }
            }
        }
        // 3. The second hop from the first hop's vertices.
        if e.hops == 2 && !frontier_keys.is_empty() {
            let mut params = vec![("frontier", Value::List(frontier_keys.into()))];
            if !types.is_empty() {
                params.push(("types", Value::List(types.clone().into())));
            }
            let statement = format!(
                "MATCH (s){pattern}(n) WHERE s._key IN $frontier{} RETURN labels(s) AS ml, \
                 properties(s) AS mp, id(e) AS eid, type(e) AS ty, properties(e) AS ep, \
                 labels(n) AS nl, properties(n) AS np",
                clauses(false, !types.is_empty())
            );
            let hop2 = self
                .run(ns, &e.graph, &statement, params, &consistency)
                .await?;
            token = hop2.token.clone();
            // Rows by the first-hop vertex they start from.
            let mut from: HashMap<(String, Vec<u8>), Vec<&Vec<Value>>> = HashMap::new();
            for row in &hop2.rows {
                let [ml, mp, ..] = row.as_slice() else {
                    continue;
                };
                let Some((mlabel, _, mpk, _)) = reached(ml, mp) else {
                    continue;
                };
                from.entry((mlabel, mpk.canonical())).or_default().push(row);
            }
            for (seed, per_seed) in &mut best {
                let seed = existing
                    .iter()
                    .find(|k| &ident(k) == seed)
                    .cloned()
                    .unwrap_or(Value::Null);
                let firsts: Vec<Candidate> = per_seed.values().cloned().collect();
                for first in firsts {
                    let Some(rows) = from.get(&(first.label.clone(), first.pk.canonical())) else {
                        continue;
                    };
                    for row in rows {
                        let [_, _, eid, ty, ep, nl, np] = row.as_slice() else {
                            continue;
                        };
                        if !edge_ok(ep) || !vertex_ok(np) {
                            continue;
                        }
                        let Some((nlabel, nkey, npk, collection)) = reached(nl, np) else {
                            continue;
                        };
                        if is_seed(&seed, &nlabel, &nkey) {
                            continue;
                        }
                        let candidate = Candidate {
                            label: nlabel.clone(),
                            pk: npk,
                            hop: 2,
                            via: first.pk.clone(),
                            edge_type: match ty {
                                Value::String(s) => s.to_string(),
                                _ => String::new(),
                            },
                            edge: edge_pk(ep, eid),
                            collection,
                        };
                        let id = (nlabel, candidate.pk.canonical());
                        match per_seed.get(&id) {
                            Some(kept) if !candidate.better_than(kept) => {}
                            _ => {
                                per_seed.insert(id, candidate);
                            }
                        }
                    }
                }
            }
        }
        // Per seed (in seed order), truncated; then the whole answer.
        let mut all: Vec<(usize, Candidate)> = Vec::new();
        let mut done: Vec<Value> = Vec::new();
        for (index, pk) in call.seeds.iter().enumerate() {
            let Some(pk) = pk else { continue };
            let key = key_value(pk);
            // A seed named twice is expanded once, for its first hit.
            if done.contains(&key) {
                continue;
            }
            done.push(key.clone());
            let Some(per_seed) = best.get(&ident(&key)) else {
                continue;
            };
            let mut list: Vec<Candidate> = per_seed.values().cloned().collect();
            list.sort_by_key(Candidate::order);
            list.truncate(e.limit_per_seed);
            all.extend(list.into_iter().map(|c| (index, c)));
        }
        all.sort_by(|(a, ca), (b, cb)| ca.order().cmp(&cb.order()).then(a.cmp(b)));
        all.truncate(e.limit);
        all.sort_by(|(a, ca), (b, cb)| a.cmp(b).then_with(|| ca.order().cmp(&cb.order())));
        Ok(Expanded {
            reached: all
                .into_iter()
                .map(|(seed, c)| Reached {
                    seed,
                    label: c.label,
                    key: c.pk,
                    hop: c.hop,
                    via: c.via,
                    edge_type: c.edge_type,
                    edge: c.edge,
                    collection: c.collection,
                })
                .collect(),
            skipped,
            token: Some(token).filter(|t| !t.is_empty()),
        })
    }
}
