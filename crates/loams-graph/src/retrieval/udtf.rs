//! The graph table functions' backend on Loams Graph (GR1 Task 21; design §07 §5.1; D743):
//! `loams-query`'s [`GraphTables`] for `graph_neighbors`, `graph_degree` and
//! `graph_shortest_path` (`graph_expand` is the `expand` stage's [`super::LoamsGraphExpander`]).
//!
//! Like the expansion, every read is a parameterized, read-only GQL statement through the graph's
//! own `Execute` path, and every order is Loams': a key's edges by direction (out first), type,
//! the other end's canonical key bytes, then the edge's; a shortest path is found breadth first,
//! one statement per depth, keeping for each vertex the first parent in canonical key order
//! (then edge), so the path answered never depends on the engine's iteration order.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use grafeo::Value;
use loams_collection::PrimaryKey;
use loams_query::ServiceError;
use loams_query::expand::ExpandDirection;
use loams_query::graph::{
    AlgorithmCall, AlgorithmRow, AlgorithmSpec, AlgorithmValue, DegreeCall, GraphTables,
    IncidentEdge, NeighborsCall, PathCall, PathStep,
};

use super::expand::{
    LoamsGraphExpander, arrow, clauses, edge_pk, ident, json_props, key_value, vertex_of,
};
use super::{algos, filter};
use crate::consistency::Consistency;

/// The directions a call walks, each a fixed edge arrow: out before in.
fn directions(direction: ExpandDirection) -> Vec<ExpandDirection> {
    match direction {
        ExpandDirection::Both => vec![ExpandDirection::Out, ExpandDirection::In],
        one => vec![one],
    }
}

fn type_name(value: &Value) -> String {
    match value {
        Value::String(s) => s.to_string(),
        _ => String::new(),
    }
}

/// One edge from a vertex of the frontier, as a statement answered it.
struct Step {
    from: (String, Vec<u8>),
    /// The `_key` of the vertex it starts from.
    from_key: Value,
    direction: ExpandDirection,
    edge_type: String,
    edge: PrimaryKey,
    to_label: String,
    to_key: Value,
    to_pk: PrimaryKey,
    edge_props: Value,
}

impl LoamsGraphExpander {
    /// Every edge, in `direction`, of the vertices keyed `keys` (of `label` when given).
    #[allow(clippy::too_many_arguments)]
    async fn steps(
        &self,
        namespace: &str,
        graph: &str,
        keys: Vec<Value>,
        label: Option<&str>,
        direction: ExpandDirection,
        edge_types: &[String],
        consistency: &mut Consistency,
    ) -> Result<Vec<Step>, ServiceError> {
        let mut out = Vec::new();
        if keys.is_empty() {
            return Ok(out);
        }
        for dir in directions(direction) {
            let mut params = vec![("seeds", Value::List(keys.clone().into()))];
            if let Some(label) = label {
                params.push(("label", Value::String(label.into())));
            }
            if !edge_types.is_empty() {
                params.push((
                    "types",
                    Value::List(
                        edge_types
                            .iter()
                            .map(|t| Value::String(t.as_str().into()))
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                ));
            }
            let statement = format!(
                "MATCH (s){}(n) WHERE s._key IN $seeds{} RETURN labels(s) AS sl, properties(s) \
                 AS sp, id(e) AS eid, type(e) AS ty, properties(e) AS ep, labels(n) AS nl, \
                 properties(n) AS np",
                arrow(dir),
                clauses(label.is_some(), !edge_types.is_empty())
            );
            let answer = self
                .run(namespace, graph, &statement, params, consistency)
                .await?;
            if let Ok(at) = answer.token.parse::<crate::durable::ConsistencyToken>() {
                *consistency = Consistency::AtLeast(at);
            }
            for row in answer.rows {
                let [sl, sp, eid, ty, ep, nl, np] = row.as_slice() else {
                    continue;
                };
                let (Some((slabel, skey, spk, _)), Some((nlabel, nkey, npk, _))) =
                    (vertex_of(sl, sp), vertex_of(nl, np))
                else {
                    continue;
                };
                out.push(Step {
                    from: (slabel, spk.canonical()),
                    from_key: skey,
                    direction: dir,
                    edge_type: type_name(ty),
                    edge: edge_pk(ep, eid),
                    to_label: nlabel,
                    to_key: nkey,
                    to_pk: npk,
                    edge_props: ep.clone(),
                });
            }
        }
        Ok(out)
    }

    /// The vertices keyed `keys` (of `label` when given), each as `(label, canonical key)`.
    async fn vertices(
        &self,
        namespace: &str,
        graph: &str,
        keys: &[Value],
        label: Option<&str>,
        consistency: &mut Consistency,
    ) -> Result<Vec<(Value, (String, PrimaryKey))>, ServiceError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut params = vec![("seeds", Value::List(keys.to_vec().into()))];
        if let Some(label) = label {
            params.push(("label", Value::String(label.into())));
        }
        let statement = format!(
            "MATCH (s) WHERE s._key IN $seeds{} RETURN labels(s) AS sl, properties(s) AS sp",
            clauses(label.is_some(), false)
        );
        let answer = self
            .run(namespace, graph, &statement, params, consistency)
            .await?;
        if let Ok(at) = answer.token.parse::<crate::durable::ConsistencyToken>() {
            *consistency = Consistency::AtLeast(at);
        }
        Ok(answer
            .rows
            .into_iter()
            .filter_map(|row| {
                let [sl, sp] = row.as_slice() else {
                    return None;
                };
                let (label, key, pk, _) = vertex_of(sl, sp)?;
                Some((key, (label, pk)))
            })
            .collect())
    }
}

/// `keys` deduplicated in call order, with their property values.
fn distinct(keys: &[PrimaryKey]) -> Vec<(PrimaryKey, Value)> {
    let mut out: Vec<(PrimaryKey, Value)> = Vec::new();
    for pk in keys {
        if !out.iter().any(|(k, _)| k == pk) {
            out.push((pk.clone(), key_value(pk)));
        }
    }
    out
}

#[async_trait]
impl GraphTables for LoamsGraphExpander {
    async fn neighbors(&self, call: NeighborsCall<'_>) -> Result<Vec<IncidentEdge>, ServiceError> {
        call.caller
            .authorize_read("graph_neighbors", call.namespace, call.graph)
            .await?;
        if let Some(q) = &call.edge_filter {
            filter::check(q, "edge_filter")?;
        }
        let keys = distinct(&call.keys);
        let mut consistency = Consistency::Strong;
        let steps = self
            .steps(
                call.namespace,
                call.graph,
                keys.iter().map(|(_, v)| v.clone()).collect(),
                call.label.as_deref(),
                call.direction,
                &call.edge_types,
                &mut consistency,
            )
            .await?;
        let mut out = Vec::new();
        for (pk, value) in &keys {
            let mut mine: Vec<&Step> = steps
                .iter()
                .filter(|s| ident(&s.from_key) == ident(value))
                .filter(|s| {
                    call.edge_filter
                        .as_ref()
                        .is_none_or(|q| filter::matches(q, &json_props(&s.edge_props)))
                })
                .collect();
            mine.sort_by(|a, b| {
                let d = |s: &Step| u8::from(s.direction == ExpandDirection::In);
                (d(a), &a.edge_type, a.to_pk.canonical(), a.edge.canonical()).cmp(&(
                    d(b),
                    &b.edge_type,
                    b.to_pk.canonical(),
                    b.edge.canonical(),
                ))
            });
            for step in mine.into_iter().take(call.limit_per_key) {
                out.push(IncidentEdge {
                    key: pk.clone(),
                    direction: step.direction,
                    edge_type: step.edge_type.clone(),
                    edge: step.edge.clone(),
                    other_label: step.to_label.clone(),
                    other_key: step.to_pk.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn degree(&self, call: DegreeCall<'_>) -> Result<Vec<(PrimaryKey, u64)>, ServiceError> {
        call.caller
            .authorize_read("graph_degree", call.namespace, call.graph)
            .await?;
        let keys = distinct(&call.keys);
        let values: Vec<Value> = keys.iter().map(|(_, v)| v.clone()).collect();
        let mut consistency = Consistency::Strong;
        let found = self
            .vertices(
                call.namespace,
                call.graph,
                &values,
                call.label.as_deref(),
                &mut consistency,
            )
            .await?;
        let steps = self
            .steps(
                call.namespace,
                call.graph,
                values,
                call.label.as_deref(),
                call.direction,
                &call.edge_types,
                &mut consistency,
            )
            .await?;
        Ok(keys
            .iter()
            .filter(|(_, value)| found.iter().any(|(key, _)| ident(key) == ident(value)))
            .map(|(pk, _)| {
                let value = key_value(pk);
                let degree = steps
                    .iter()
                    .filter(|s| ident(&s.from_key) == ident(&value))
                    .count();
                (pk.clone(), degree as u64)
            })
            .collect())
    }

    async fn shortest_path(&self, call: PathCall<'_>) -> Result<Vec<PathStep>, ServiceError> {
        call.caller
            .authorize_read("graph_shortest_path", call.namespace, call.graph)
            .await?;
        let mut consistency = Consistency::Strong;
        let label = call.label.as_deref();
        let ends = self
            .vertices(
                call.namespace,
                call.graph,
                &[key_value(&call.from), key_value(&call.to)],
                label,
                &mut consistency,
            )
            .await?;
        let starts: Vec<(String, PrimaryKey, Value)> = ends
            .iter()
            .filter(|(key, _)| ident(key) == ident(&key_value(&call.from)))
            .map(|(key, (l, pk))| (l.clone(), pk.clone(), key.clone()))
            .collect();
        let target_key = ident(&key_value(&call.to));
        let is_target =
            |l: &str, key: &Value| ident(key) == target_key && label.is_none_or(|want| want == l);
        if starts.is_empty() || !ends.iter().any(|(key, (l, _))| is_target(l, key)) {
            return Ok(Vec::new());
        }
        // The smallest start, in canonical key order, when two labels share the key.
        let mut starts = starts;
        starts.sort_by(|a, b| (a.1.canonical(), &a.0).cmp(&(b.1.canonical(), &b.0)));
        let start = starts.remove(0);
        if is_target(&start.0, &start.2) {
            return Ok(vec![PathStep {
                step: 0,
                label: start.0,
                key: start.1,
                edge_type: None,
                edge: None,
            }]);
        }
        type Ident = (String, Vec<u8>);
        // vertex → (its parent, edge type, edge, label, key)
        let mut parent: BTreeMap<Ident, Option<(Ident, String, PrimaryKey)>> = BTreeMap::new();
        let mut info: BTreeMap<Ident, (String, PrimaryKey)> = BTreeMap::new();
        let start_id = (start.0.clone(), start.1.canonical());
        parent.insert(start_id.clone(), None);
        info.insert(start_id.clone(), (start.0.clone(), start.1.clone()));
        let mut frontier: BTreeSet<Ident> = BTreeSet::from([start_id]);
        let mut frontier_keys: Vec<Value> = vec![start.2];
        for _ in 0..call.max_hops {
            let steps = self
                .steps(
                    call.namespace,
                    call.graph,
                    std::mem::take(&mut frontier_keys),
                    None,
                    call.direction,
                    &call.edge_types,
                    &mut consistency,
                )
                .await?;
            let mut mine: Vec<&Step> = steps
                .iter()
                .filter(|s| frontier.contains(&s.from))
                .collect();
            mine.sort_by(|a, b| {
                (
                    &a.from.1,
                    &a.from.0,
                    a.edge.canonical(),
                    a.to_pk.canonical(),
                )
                    .cmp(&(
                        &b.from.1,
                        &b.from.0,
                        b.edge.canonical(),
                        b.to_pk.canonical(),
                    ))
            });
            let mut next: BTreeSet<Ident> = BTreeSet::new();
            let mut found = None;
            for step in mine {
                let id = (step.to_label.clone(), step.to_pk.canonical());
                if parent.contains_key(&id) {
                    continue;
                }
                parent.insert(
                    id.clone(),
                    Some((step.from.clone(), step.edge_type.clone(), step.edge.clone())),
                );
                info.insert(id.clone(), (step.to_label.clone(), step.to_pk.clone()));
                if found.is_none() && is_target(&step.to_label, &step.to_key) {
                    found = Some(id.clone());
                }
                if next.insert(id) && !frontier_keys.contains(&step.to_key) {
                    frontier_keys.push(step.to_key.clone());
                }
            }
            if let Some(target) = found {
                let mut path = Vec::new();
                let mut at = Some(target);
                while let Some(id) = at {
                    let (l, pk) = info[&id].clone();
                    let entry = parent[&id].clone();
                    path.push((
                        l,
                        pk,
                        entry.as_ref().map(|(_, t, e)| (t.clone(), e.clone())),
                    ));
                    at = entry.map(|(p, _, _)| p);
                }
                path.reverse();
                return Ok(path
                    .into_iter()
                    .enumerate()
                    .map(|(i, (label, key, via))| PathStep {
                        step: u32::try_from(i).unwrap_or(u32::MAX),
                        label,
                        key,
                        edge_type: via.as_ref().map(|(t, _)| t.clone()),
                        edge: via.map(|(_, e)| e),
                    })
                    .collect());
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok(Vec::new())
    }

    async fn algorithm(&self, call: AlgorithmCall<'_>) -> Result<Vec<AlgorithmRow>, ServiceError> {
        let what = match call.spec {
            AlgorithmSpec::PageRank { .. } => "pagerank",
            AlgorithmSpec::Wcc => "wcc",
            AlgorithmSpec::Leiden { .. } => "leiden",
        };
        call.caller
            .authorize_read(what, call.namespace, call.graph)
            .await?;
        let edge_types = call.edge_types.clone();
        let (algorithm, label) = match call.spec {
            AlgorithmSpec::PageRank {
                label,
                damping,
                max_iterations,
                tolerance,
            } => (
                algos::Algorithm::PageRank(algos::PageRank {
                    label: label.clone(),
                    edge_types,
                    damping,
                    max_iterations,
                    tolerance,
                }),
                label,
            ),
            AlgorithmSpec::Wcc => (algos::Algorithm::Wcc { edge_types }, None),
            AlgorithmSpec::Leiden {
                weight,
                resolution,
                max_levels,
                max_cluster_size,
                seed,
            } => (
                algos::Algorithm::Leiden(algos::Leiden {
                    edge_types,
                    weight,
                    resolution,
                    max_levels,
                    max_cluster_size,
                    seed,
                }),
                None,
            ),
        };
        let answer = self
            .admin()
            .run_algorithm(call.namespace, call.graph, &algorithm, &Consistency::Strong)
            .await
            .map_err(|err| algorithm_error(&err, call.graph))?;
        let view = &answer.pinned.view;
        Ok(answer
            .rows
            .iter()
            .filter_map(|row| {
                let (first, key) = algos::identity(view, row.ordinal)?;
                let value = match row.score {
                    algos::Score::Rank(rank) => AlgorithmValue::Rank(rank),
                    algos::Score::Component(c) => AlgorithmValue::Component(c),
                    algos::Score::Community(c) => AlgorithmValue::Community {
                        level: c.level,
                        community: c.community,
                        parent_community: c.parent,
                    },
                };
                Some(AlgorithmRow {
                    label: label.clone().unwrap_or(first),
                    key,
                    value,
                })
            })
            .collect())
    }
}

/// A graph's refusal of an algorithm, as the SQL layer reports it.
fn algorithm_error(err: &connectrpc::ConnectError, graph: &str) -> ServiceError {
    use connectrpc::ErrorCode;
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
            ServiceError::InvalidArgument(format!("an algorithm on graph {graph}: {message}"))
        }
        ErrorCode::Unavailable | ErrorCode::DeadlineExceeded | ErrorCode::Aborted => {
            ServiceError::Unavailable(format!("an algorithm on graph {graph}: {message}"))
        }
        _ => ServiceError::Internal(format!("an algorithm on graph {graph}: {message}")),
    }
}
