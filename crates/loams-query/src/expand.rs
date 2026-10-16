//! The `expand` and `rerank` stages of a search (GR1 Task 20; design §07 §5.2, §48 §10.3; D743).
//!
//! One request, one plan: retrievers → fusion → `Limit(seeds)` → **expand** → document fetch →
//! **rerank** → limit. The base search is the request without its two stages, limited to the
//! seeds. The expansion is a [`GraphExpander`]'s: this crate is in every build and Loams Graph is
//! not (the `graph` feature), so the server hands the service one when it runs graphs
//! ([`crate::CollectionService::set_graph_expander`]). Without one, a request that expands is
//! refused.
//!
//! The expander answers each seed's reached vertices already deduplicated and truncated in §07
//! §4's order (hop ascending, then the vertex's canonical key bytes); this module scores them,
//! fetches the documents `select` asks for, reranks and shapes the answer
//! ([`ExpandOutput::Nested`] or [`ExpandOutput::Flat`]).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use async_trait::async_trait;
use loams_collection::PrimaryKey;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::ir::{Hit, Query};
use crate::types::Projection;

fn default_seeds() -> usize {
    10
}

fn default_hops() -> u8 {
    1
}

fn default_limit_per_seed() -> usize {
    50
}

fn default_expand_limit() -> usize {
    100
}

fn default_decay() -> f32 {
    0.5
}

/// The most seeds one expansion takes.
pub const MAX_SEEDS: usize = 1_000;
/// The most neighbors one seed keeps.
pub const MAX_LIMIT_PER_SEED: usize = 1_000;
/// The most neighbors one expansion answers.
pub const MAX_EXPAND_LIMIT: usize = 10_000;

/// Which way an expansion walks the edges.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandDirection {
    /// Along the edges (source to destination).
    #[default]
    Out,
    /// Against them.
    In,
    /// Either way.
    Both,
}

/// How the neighbors come back.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandOutput {
    /// Under each seed hit, in `Hit.neighbors`.
    #[default]
    Nested,
    /// Seeds and neighbors in one ranked list, each neighbor a hit with `Hit.graph`.
    Flat,
}

/// The `expand` stage (§07 §5.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Expand {
    /// The graph, in the request's namespace.
    pub graph: String,
    /// The hit's source field holding the seed vertex's key; absent: the hit's primary key.
    #[serde(default)]
    pub from_field: Option<String>,
    /// The seeds' vertex label; absent: a vertex of any label with the key.
    #[serde(default)]
    pub seed_label: Option<String>,
    /// How many of the top hits seed the expansion. Default 10.
    #[serde(default = "default_seeds")]
    pub seeds: usize,
    /// 1 or 2. Default 1.
    #[serde(default = "default_hops")]
    pub hops: u8,
    /// Default `out`.
    #[serde(default)]
    pub direction: ExpandDirection,
    /// Edge types walked; empty walks every type.
    #[serde(default)]
    pub edge_types: Vec<String>,
    /// Over the edges' properties (their documents' fields); an edge that fails it is not
    /// walked.
    #[serde(default)]
    pub edge_filter: Option<Query>,
    /// Over the reached vertices' properties; a vertex that fails it is neither answered nor
    /// walked through.
    #[serde(default)]
    pub vertex_filter: Option<Query>,
    /// Neighbors kept per seed. Default 50.
    #[serde(default = "default_limit_per_seed")]
    pub limit_per_seed: usize,
    /// Neighbors kept in all. Default 100.
    #[serde(default = "default_expand_limit")]
    pub limit: usize,
    /// The neighbors' document fields fetched from their collections; absent fetches none.
    #[serde(default)]
    pub select: Option<Projection>,
    /// Default `nested`.
    #[serde(default)]
    pub output: ExpandOutput,
    /// A graph consistency token: the expansion sees at least its writes. Absent: `strong`.
    #[serde(default)]
    pub consistency_token: Option<String>,
}

/// The `rerank` stage (§07 §5.2): scores the final candidates, seeds and neighbors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Rerank {
    /// A neighbor scores the most, over its seeds, of `seed score × decay^hop`. Without a
    /// rerank, neighbors are scored so with `decay` 0.5.
    Inherit {
        #[serde(default = "default_decay")]
        decay: f32,
    },
    /// The cosine similarity of each candidate's vector `field` to `query` (0 without one).
    Vector { field: String, query: Vec<f32> },
    /// An external reranker. Off by default (§05 §4): always refused.
    Model {
        endpoint: String,
        text_field: String,
        query: String,
        top_n: usize,
    },
}

/// A vertex reached from a seed hit (`Hit.neighbors`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Neighbor {
    pub label: String,
    /// The vertex's key: its document's primary key when it has one.
    #[serde(with = "crate::json::pk")]
    pub key: PrimaryKey,
    pub hop: u8,
    /// The vertex it was reached from: the seed for hop 1.
    #[serde(with = "crate::json::pk")]
    pub via: PrimaryKey,
    pub edge_type: String,
    /// The edge's key (its document's primary key).
    #[serde(with = "crate::json::pk")]
    pub edge: PrimaryKey,
    pub score: f32,
    /// The vertex's collection, when a collection link wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Its document, when `select` asked for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Map<String, Value>>,
}

/// What a flat answer's hit is in the graph (`Hit.graph`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphHit {
    /// The vertex label (the seed label, or empty when the request named none, for a seed).
    pub label: String,
    /// 0 for a seed.
    pub hop: u8,
    /// The seeds it was reached from, in seed order.
    #[serde(with = "crate::json::pk::vec")]
    pub seeds: Vec<PrimaryKey>,
    /// The vertex's collection, when a collection link wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

/// What an expansion did (`SearchResponse.expand`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpandStats {
    /// Seeds taken from the hits.
    pub seeds: usize,
    /// Seeds with no key or no vertex in the graph.
    pub seeds_skipped: usize,
    /// Neighbors answered.
    pub neighbors: usize,
    /// The graph state the expansion read, as a graph consistency token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_token: Option<String>,
}

/// One expansion for a [`GraphExpander`].
#[derive(Clone, Debug)]
pub struct ExpandCall<'a> {
    pub namespace: &'a str,
    pub expand: &'a Expand,
    /// The seeds' keys, in hit order; `None` for a hit without one.
    pub seeds: Vec<Option<PrimaryKey>>,
}

/// A vertex an expander reached.
#[derive(Clone, Debug, PartialEq)]
pub struct Reached {
    /// The index of its seed in [`ExpandCall::seeds`].
    pub seed: usize,
    pub label: String,
    pub key: PrimaryKey,
    pub hop: u8,
    pub via: PrimaryKey,
    pub edge_type: String,
    pub edge: PrimaryKey,
    /// The collection of its document, when a collection link wrote it.
    pub collection: Option<String>,
}

/// An expander's answer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Expanded {
    /// Per seed in seed order, each seed's vertices in §07 §4's order, already truncated.
    pub reached: Vec<Reached>,
    /// The seeds with no vertex in the graph.
    pub skipped: Vec<usize>,
    /// The graph state read.
    pub token: Option<String>,
}

/// Runs expansions on graphs: Loams Graph's (`loams-graph`), when the server runs it.
#[async_trait]
pub trait GraphExpander: Send + Sync + std::fmt::Debug {
    /// Expands `call`.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` or `NotFound` for the request, `Unavailable` when the graph is.
    async fn expand(&self, call: ExpandCall<'_>) -> Result<Expanded, ServiceError>;
}

fn invalid(message: impl Into<String>) -> ServiceError {
    ServiceError::InvalidArgument(message.into())
}

/// Checks the two stages of a request (GR1 Task 20).
///
/// # Errors
///
/// `InvalidArgument` naming the first problem.
pub fn validate(
    expand: Option<&Expand>,
    rerank: Option<&Rerank>,
    grouped: bool,
    aggregated: bool,
    sorted: bool,
) -> Result<(), ServiceError> {
    if expand.is_none() && rerank.is_none() {
        return Ok(());
    }
    if grouped || aggregated || sorted {
        return Err(invalid(
            "expand and rerank score a ranked list: they take no group_by, aggregations, sort or \
             search_after",
        ));
    }
    if let Some(expand) = expand {
        if expand.graph.is_empty() {
            return Err(invalid("expand.graph must name a graph"));
        }
        if !(1..=2).contains(&expand.hops) {
            return Err(invalid("expand.hops is 1 or 2"));
        }
        for (what, value, max) in [
            ("seeds", expand.seeds, MAX_SEEDS),
            ("limit_per_seed", expand.limit_per_seed, MAX_LIMIT_PER_SEED),
            ("limit", expand.limit, MAX_EXPAND_LIMIT),
        ] {
            if value == 0 || value > max {
                return Err(invalid(format!("expand.{what} is 1 to {max}")));
            }
        }
    }
    match rerank {
        Some(Rerank::Model { .. }) => Err(invalid(
            "rerank.model calls an external reranker, which is off on this server (§05 §4); use \
             inherit or vector",
        )),
        Some(Rerank::Vector { query, .. }) if query.is_empty() => {
            Err(invalid("rerank.vector.query must not be empty"))
        }
        Some(Rerank::Inherit { decay }) if !(0.0..=1.0).contains(decay) => {
            Err(invalid("rerank.inherit.decay is 0 to 1"))
        }
        _ => Ok(()),
    }
}

/// The seed key a hit holds: its primary key, or the value of `from_field` in its source (an
/// unsigned integer, a string, or `{"uuid": …}`).
pub fn seed_key(hit: &Hit, from_field: Option<&str>) -> Option<PrimaryKey> {
    let Some(field) = from_field else {
        return Some(hit.pk.clone());
    };
    let mut segments = field.split('.');
    let mut value = hit.source.as_ref()?.get(segments.next()?)?;
    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }
    crate::json::pk::from_json(value).ok()
}

/// The cosine similarity of `a` and `b`, 0 when either is all zeros or they differ in length.
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        dot += f64::from(*x) * f64::from(*y);
        na += f64::from(*x) * f64::from(*x);
        nb += f64::from(*y) * f64::from(*y);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    (dot / (na.sqrt() * nb.sqrt())) as f32
}

/// Score descending, then the key's canonical bytes ascending (overview R10).
fn by_score(a: (f32, &PrimaryKey), b: (f32, &PrimaryKey)) -> Ordering {
    b.0.total_cmp(&a.0)
        .then_with(|| a.1.canonical().cmp(&b.1.canonical()))
}

/// A reached vertex's document and vector, fetched for the answer and the rerank.
#[derive(Clone, Debug, Default)]
pub struct Fetched {
    pub source: Option<Map<String, Value>>,
    pub vector: Option<Vec<f32>>,
}

/// What [`assemble`] needs besides the seed hits and the expansion.
#[derive(Debug)]
pub struct Assembly<'a> {
    pub expand: Option<&'a Expand>,
    pub rerank: Option<&'a Rerank>,
    /// Each reached vertex's fetched document, by `(collection, key)`.
    pub fetched: &'a BTreeMap<(Option<String>, PrimaryKey), Fetched>,
    /// Each seed hit's vector `rerank.vector.field`, in hit order.
    pub seed_vectors: Vec<Option<Vec<f32>>>,
    pub offset: usize,
    pub limit: usize,
}

/// Scores, reranks and shapes the answer: the seed `hits` (in rank order), what the expansion
/// reached, and the page. Answers the hits and how many neighbors they carry.
pub fn assemble(mut hits: Vec<Hit>, expanded: &Expanded, with: &Assembly<'_>) -> (Vec<Hit>, usize) {
    let decay = match with.rerank {
        Some(Rerank::Inherit { decay }) => *decay,
        _ => default_decay(),
    };
    let vector = match with.rerank {
        Some(Rerank::Vector { query, .. }) => Some(query.as_slice()),
        _ => None,
    };
    // Seeds rescored by a vector rerank.
    if let Some(query) = vector {
        for (hit, seed_vector) in hits.iter_mut().zip(&with.seed_vectors) {
            hit.score = seed_vector.as_deref().map_or(0.0, |v| cosine(v, query));
        }
    }
    let neighbor_score = |reached: &Reached, seed_score: f32| -> f32 {
        match vector {
            Some(query) => with
                .fetched
                .get(&(reached.collection.clone(), reached.key.clone()))
                .and_then(|f| f.vector.as_deref())
                .map_or(0.0, |v| cosine(v, query)),
            None => seed_score * decay.powi(i32::from(reached.hop)),
        }
    };
    let neighbor = |reached: &Reached, score: f32| Neighbor {
        label: reached.label.clone(),
        key: reached.key.clone(),
        hop: reached.hop,
        via: reached.via.clone(),
        edge_type: reached.edge_type.clone(),
        edge: reached.edge.clone(),
        score,
        collection: reached.collection.clone(),
        source: with
            .fetched
            .get(&(reached.collection.clone(), reached.key.clone()))
            .and_then(|f| f.source.clone()),
    };
    let output = with.expand.map_or(ExpandOutput::Nested, |e| e.output);
    let mut count = 0;
    match output {
        ExpandOutput::Nested => {
            for reached in &expanded.reached {
                if let Some(hit) = hits.get_mut(reached.seed) {
                    let score = neighbor_score(reached, hit.score);
                    hit.neighbors.push(neighbor(reached, score));
                }
            }
            for hit in &mut hits {
                hit.neighbors.sort_by(|a, b| {
                    by_score((a.score, &a.key), (b.score, &b.key)).then(a.hop.cmp(&b.hop))
                });
            }
            hits.sort_by(|a, b| by_score((a.score, &a.pk), (b.score, &b.pk)));
            let page: Vec<Hit> = hits
                .into_iter()
                .skip(with.offset)
                .take(with.limit)
                .collect();
            count = page.iter().map(|h| h.neighbors.len()).sum();
            (page, count)
        }
        ExpandOutput::Flat => {
            let seed_label = with
                .expand
                .and_then(|e| e.seed_label.clone())
                .unwrap_or_default();
            for hit in &mut hits {
                hit.graph = Some(GraphHit {
                    label: seed_label.clone(),
                    hop: 0,
                    seeds: vec![hit.pk.clone()],
                    collection: None,
                });
            }
            // One hit per reached vertex: its best score over its seeds, its smallest hop.
            let mut merged: BTreeMap<(String, Vec<u8>), (Hit, Vec<usize>)> = BTreeMap::new();
            for reached in &expanded.reached {
                let Some(seed) = hits.get(reached.seed) else {
                    continue;
                };
                let is_seed = hits.iter().any(|h| {
                    h.pk == reached.key && (seed_label.is_empty() || seed_label == reached.label)
                });
                if is_seed {
                    continue;
                }
                let score = neighbor_score(reached, seed.score);
                let entry = merged
                    .entry((reached.label.clone(), reached.key.canonical()))
                    .or_insert_with(|| {
                        let n = neighbor(reached, score);
                        (
                            Hit {
                                pk: reached.key.clone(),
                                score,
                                sort_values: Vec::new(),
                                source: n.source,
                                vectors: BTreeMap::new(),
                                sparse_vectors: BTreeMap::new(),
                                highlight: BTreeMap::new(),
                                fields: BTreeMap::new(),
                                neighbors: Vec::new(),
                                graph: Some(GraphHit {
                                    label: reached.label.clone(),
                                    hop: reached.hop,
                                    seeds: Vec::new(),
                                    collection: reached.collection.clone(),
                                }),
                            },
                            Vec::new(),
                        )
                    });
                entry.0.score = entry.0.score.max(score);
                if let Some(graph) = &mut entry.0.graph {
                    graph.hop = graph.hop.min(reached.hop);
                }
                if !entry.1.contains(&reached.seed) {
                    entry.1.push(reached.seed);
                }
            }
            for (mut hit, mut seeds) in merged.into_values() {
                seeds.sort_unstable();
                if let Some(graph) = &mut hit.graph {
                    graph.seeds = seeds.iter().map(|s| hits[*s].pk.clone()).collect();
                }
                count += 1;
                hits.push(hit);
            }
            hits.sort_by(|a, b| {
                by_score((a.score, &a.pk), (b.score, &b.pk)).then_with(|| {
                    let hop = |h: &Hit| h.graph.as_ref().map_or(0, |g| g.hop);
                    hop(a).cmp(&hop(b))
                })
            });
            let page: Vec<Hit> = hits
                .into_iter()
                .skip(with.offset)
                .take(with.limit)
                .collect();
            let neighbors = page
                .iter()
                .filter(|h| h.graph.as_ref().is_some_and(|g| g.hop > 0))
                .count();
            (page, neighbors.min(count))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(pk: u64, score: f32) -> Hit {
        Hit {
            pk: PrimaryKey::U64(pk),
            score,
            sort_values: Vec::new(),
            source: None,
            vectors: BTreeMap::new(),
            sparse_vectors: BTreeMap::new(),
            highlight: BTreeMap::new(),
            fields: BTreeMap::new(),
            neighbors: Vec::new(),
            graph: None,
        }
    }

    fn reached(seed: usize, key: u64, hop: u8) -> Reached {
        Reached {
            seed,
            label: "entity".to_string(),
            key: PrimaryKey::U64(key),
            hop,
            via: PrimaryKey::U64(0),
            edge_type: "R".to_string(),
            edge: PrimaryKey::U64(key + 100),
            collection: None,
        }
    }

    fn expand(output: ExpandOutput) -> Expand {
        Expand {
            graph: "kg".to_string(),
            from_field: None,
            seed_label: Some("entity".to_string()),
            seeds: 10,
            hops: 2,
            direction: ExpandDirection::Out,
            edge_types: Vec::new(),
            edge_filter: None,
            vertex_filter: None,
            limit_per_seed: 50,
            limit: 100,
            select: None,
            output,
            consistency_token: None,
        }
    }

    #[test]
    fn nested_neighbors_inherit_their_seed_score() {
        let expanded = Expanded {
            reached: vec![reached(0, 5, 1), reached(0, 6, 2), reached(1, 5, 1)],
            ..Expanded::default()
        };
        let e = expand(ExpandOutput::Nested);
        let fetched = BTreeMap::new();
        let (hits, n) = assemble(
            vec![hit(1, 1.0), hit(2, 0.5)],
            &expanded,
            &Assembly {
                expand: Some(&e),
                rerank: Some(&Rerank::Inherit { decay: 0.5 }),
                fetched: &fetched,
                seed_vectors: Vec::new(),
                offset: 0,
                limit: 10,
            },
        );
        assert_eq!(n, 3);
        assert_eq!(
            hits[0]
                .neighbors
                .iter()
                .map(|n| (n.score, n.hop))
                .collect::<Vec<_>>(),
            vec![(0.5, 1), (0.25, 2)]
        );
        assert_eq!(hits[1].neighbors[0].score, 0.25);
    }

    #[test]
    fn flat_answers_one_hit_per_vertex_with_its_best_seed_score() {
        let expanded = Expanded {
            reached: vec![reached(0, 5, 1), reached(1, 5, 2), reached(1, 1, 1)],
            ..Expanded::default()
        };
        let e = expand(ExpandOutput::Flat);
        let fetched = BTreeMap::new();
        let (hits, n) = assemble(
            vec![hit(1, 1.0), hit(2, 0.8)],
            &expanded,
            &Assembly {
                expand: Some(&e),
                rerank: None,
                fetched: &fetched,
                seed_vectors: Vec::new(),
                offset: 0,
                limit: 10,
            },
        );
        // Seed 1 reached from seed 2 is the seed, not a neighbor.
        let order: Vec<(u64, f32)> = hits
            .iter()
            .map(|h| match h.pk {
                PrimaryKey::U64(k) => (k, h.score),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(order, vec![(1, 1.0), (2, 0.8), (5, 0.5)]);
        assert_eq!(n, 1);
        let graph = hits[2].graph.as_ref().expect("graph");
        assert_eq!(graph.hop, 1);
        assert_eq!(graph.seeds, vec![PrimaryKey::U64(1), PrimaryKey::U64(2)]);
    }

    #[test]
    fn stages_are_validated() {
        let e = expand(ExpandOutput::Nested);
        assert!(validate(Some(&e), None, false, false, false).is_ok());
        assert!(validate(Some(&e), None, true, false, false).is_err());
        let mut three = e.clone();
        three.hops = 3;
        assert!(validate(Some(&three), None, false, false, false).is_err());
        let model = Rerank::Model {
            endpoint: "x".to_string(),
            text_field: "t".to_string(),
            query: "q".to_string(),
            top_n: 3,
        };
        assert!(validate(None, Some(&model), false, false, false).is_err());
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
    }
}
