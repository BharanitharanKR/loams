//! Retrieval on Loams Graph (GR1 Task 20; design §07 §4, §5.2, §48 §10.3; D743): `Search.expand`
//! through [`LoamsGraphExpander`], checked against the networkx reference in
//! `conformance/graph/reference/` (`expand.py` writes the fixture and the expected answers).

mod common;

use std::path::PathBuf;
use std::time::Duration;

use loams_collection::PrimaryKey;
use loams_graph::retrieval::LoamsGraphExpander;
use loams_proto::loams::graph::v1 as pb;
use loams_query::expand::{
    Expand, ExpandCall, ExpandDirection, ExpandOutput, Expanded, GraphExpander,
};
use serde_json::{Value, json};

use common::{Cluster, SHORT, create, run};

fn reference(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/graph/reference")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(&path).expect("the reference file"))
        .expect("JSON")
}

/// A GQL literal of a fixture value (the fixture's own strings hold no quote).
fn literal(value: &Value) -> String {
    match value {
        Value::String(s) => format!("'{s}'"),
        other => other.to_string(),
    }
}

fn map(props: &serde_json::Map<String, Value>, key: &Value) -> String {
    let mut parts = vec![format!("_key: {}", literal(key))];
    for (k, v) in props {
        parts.push(format!("{k}: {}", literal(v)));
    }
    format!("{{{}}}", parts.join(", "))
}

fn end(v: &Value) -> (String, Value) {
    (v[0].as_str().expect("label").to_string(), v[1].clone())
}

/// Loads the fixture into OWNED graph `g` through GQL.
async fn load(cluster: &Cluster, fixture: &Value) {
    let admin = &cluster.node(1).admin;
    create(admin, "g").await;
    for node in fixture["nodes"].as_array().expect("nodes") {
        let props = node["props"].as_object().expect("props");
        let statement = format!(
            "INSERT (:{} {})",
            node["label"].as_str().expect("label"),
            map(props, &node["key"])
        );
        run(admin, "g", &statement).await;
    }
    for edge in fixture["edges"].as_array().expect("edges") {
        let (sl, sk) = end(&edge["src"]);
        let (dl, dk) = end(&edge["dst"]);
        let props = edge["props"].as_object().expect("props");
        let statement = if (&sl, &sk) == (&dl, &dk) {
            format!(
                "MATCH (a:{sl} {{_key: {}}}) INSERT (a)-[:{} {}]->(a)",
                literal(&sk),
                edge["type"].as_str().expect("type"),
                map(props, &edge["key"])
            )
        } else {
            format!(
                "MATCH (a:{sl} {{_key: {}}}), (b:{dl} {{_key: {}}}) INSERT (a)-[:{} {}]->(b)",
                literal(&sk),
                literal(&dk),
                edge["type"].as_str().expect("type"),
                map(props, &edge["key"])
            )
        };
        run(admin, "g", &statement).await;
    }
}

async fn delete(cluster: &Cluster, fixture: &Value) {
    let admin = &cluster.node(1).admin;
    for edge in fixture["deletes"]["edges"].as_array().expect("edges") {
        run(
            admin,
            "g",
            &format!("MATCH ()-[e {{_key: {}}}]->() DELETE e", literal(edge)),
        )
        .await;
    }
    for node in fixture["deletes"]["nodes"].as_array().expect("nodes") {
        let (label, key) = end(node);
        run(
            admin,
            "g",
            &format!(
                "MATCH (n:{label} {{_key: {}}}) DETACH DELETE n",
                literal(&key)
            ),
        )
        .await;
    }
}

fn pk(value: &Value) -> Option<PrimaryKey> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(PrimaryKey::Str(s.clone())),
        Value::Number(n) => Some(PrimaryKey::U64(n.as_u64().expect("an unsigned key"))),
        other => panic!("{other}"),
    }
}

fn expand_of(case: &Value) -> Expand {
    let direction = match case["direction"].as_str().unwrap_or("out") {
        "out" => ExpandDirection::Out,
        "in" => ExpandDirection::In,
        _ => ExpandDirection::Both,
    };
    Expand {
        graph: "g".to_string(),
        from_field: None,
        seed_label: case["seed_label"].as_str().map(str::to_string),
        seeds: 10,
        hops: u8::try_from(case["hops"].as_u64().unwrap_or(1)).expect("hops"),
        direction,
        edge_types: case["edge_types"]
            .as_array()
            .map(|t| {
                t.iter()
                    .map(|t| t.as_str().expect("type").to_string())
                    .collect()
            })
            .unwrap_or_default(),
        edge_filter: (!case["edge_filter"].is_null())
            .then(|| serde_json::from_value(case["edge_filter"].clone()).expect("a query")),
        vertex_filter: (!case["vertex_filter"].is_null())
            .then(|| serde_json::from_value(case["vertex_filter"].clone()).expect("a query")),
        limit_per_seed: usize::try_from(case["limit_per_seed"].as_u64().unwrap_or(50))
            .expect("small"),
        limit: usize::try_from(case["limit"].as_u64().unwrap_or(100)).expect("small"),
        select: None,
        output: ExpandOutput::Nested,
        consistency_token: None,
    }
}

fn answer(expanded: &Expanded) -> Value {
    let key = |pk: &PrimaryKey| loams_query::json::pk::to_json(pk);
    json!({
        "reached": expanded.reached.iter().map(|r| json!({
            "seed": r.seed, "label": r.label, "key": key(&r.key), "hop": r.hop,
            "via": key(&r.via), "edge_type": r.edge_type, "edge": key(&r.edge),
        })).collect::<Vec<_>>(),
        "skipped": expanded.skipped,
    })
}

async fn expand(expander: &LoamsGraphExpander, case: &Value) -> Expanded {
    let expand = expand_of(case);
    let seeds = case["seeds"]
        .as_array()
        .expect("seeds")
        .iter()
        .map(pk)
        .collect();
    expander
        .expand(ExpandCall {
            caller: &GraphCaller::Internal,
            namespace: "acme",
            expand: &expand,
            seeds,
        })
        .await
        .unwrap_or_else(|err| panic!("{}: {err}", case["name"]))
}

/// 1 and 2 hops, every direction, edge types, edge and vertex filters, both truncations, seeds
/// missing, repeated or keyless, a key two labels share, and deletes: each answer equals the
/// networkx reference's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expand_matches_networkx_reference() {
    let fixture = reference("expand_fixture.json");
    let expected = reference("expand_expected.json");
    let cluster = Cluster::start(1, SHORT).await;
    load(&cluster, &fixture).await;
    let expander = LoamsGraphExpander::new(cluster.node(1).admin.clone());
    for phase in ["before", "after"] {
        if phase == "after" {
            delete(&cluster, &fixture).await;
        }
        for case in fixture["cases"].as_array().expect("cases") {
            let name = case["name"].as_str().expect("name");
            let got = answer(&expand(&expander, case).await);
            assert_eq!(got, expected[phase][name], "{phase} {name}");
        }
    }
}

/// The same expansion answers the same, run after run and after the graph is evicted and
/// rebuilt from its storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncation_is_deterministic() {
    let fixture = reference("expand_fixture.json");
    let cluster = Cluster::start(1, SHORT).await;
    load(&cluster, &fixture).await;
    let expander = LoamsGraphExpander::new(cluster.node(1).admin.clone());
    let cases: Vec<&Value> = fixture["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .filter(|c| c["name"] == "limit_per_seed" || c["name"] == "limit")
        .collect();
    let first: Vec<Value> = futures::future::join_all(
        cases
            .iter()
            .map(|c| async { answer(&expand(&expander, c).await) }),
    )
    .await;
    for _ in 0..100 {
        for (case, want) in cases.iter().zip(&first) {
            assert_eq!(&answer(&expand(&expander, case).await), want);
        }
    }
    assert!(
        cluster
            .node(1)
            .admin
            .evict("acme", "g")
            .await
            .expect("evict")
    );
    for (case, want) in cases.iter().zip(&first) {
        assert_eq!(
            &answer(&expand(&expander, case).await),
            want,
            "after a reload"
        );
    }
}

/// A seed with no vertex is skipped and counted; one without a key is not looked up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_without_vertex_is_skipped_and_counted() {
    let cluster = Cluster::start(1, SHORT).await;
    let admin = &cluster.node(1).admin;
    create(admin, "g").await;
    run(
        admin,
        "g",
        "INSERT (:entity {_key: 'a'})-[:R {_key: 'r1'}]->(:entity {_key: 'b'})",
    )
    .await;
    let expander = LoamsGraphExpander::new(admin.clone());
    let case = json!({"name": "skip", "seeds": ["nobody", "a", null, "b"], "hops": 1});
    let got = expand(&expander, &case).await;
    assert_eq!(got.skipped, vec![0]);
    assert_eq!(got.reached.len(), 1);
    assert_eq!(got.reached[0].seed, 1);
    assert!(got.token.is_some(), "the graph state read is answered");
}

/// An expansion that names a graph token reads at least that state: here, on a follower that
/// must first apply the write the token names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expand_respects_consistency_token() {
    let cluster = Cluster::start(2, SHORT).await;
    let owner = &cluster.node(1).admin;
    owner
        .create_graph(pb::CreateGraphRequest {
            namespace: "acme".to_string(),
            name: "g".to_string(),
            replicas: 1,
            ..Default::default()
        })
        .await
        .expect("create");
    run(owner, "g", "INSERT (:entity {_key: 'a'})").await;
    let graph = cluster.graph_id("g").await;
    let reader = 3 - cluster.preferred(graph);
    let expander = LoamsGraphExpander::new(cluster.node(reader).admin.clone());
    // Opens the follower, then the write it has not yet seen.
    let mut case = json!({"name": "token", "seeds": ["a"], "hops": 1});
    expand(&expander, &case).await;
    let wrote = run(
        &cluster.node(cluster.preferred(graph)).admin,
        "g",
        "MATCH (a:entity {_key: 'a'}) INSERT (a)-[:R {_key: 'r1'}]->(:entity {_key: 'b'})",
    )
    .await;
    case["seeds"] = json!(["a"]);
    let mut expand_req = expand_of(&case);
    expand_req.consistency_token = Some(wrote.consistency_token.clone());
    let got = tokio::time::timeout(
        Duration::from_secs(20),
        expander.expand(ExpandCall {
            caller: &GraphCaller::Internal,
            namespace: "acme",
            expand: &expand_req,
            seeds: vec![Some(PrimaryKey::Str("a".to_string()))],
        }),
    )
    .await
    .expect("in time")
    .expect("expand");
    assert_eq!(got.reached.len(), 1, "the token's write is seen");
    let read: loams_graph::durable::ConsistencyToken =
        got.token.expect("a token").parse().expect("a graph token");
    let written: loams_graph::durable::ConsistencyToken =
        wrote.consistency_token.parse().expect("a graph token");
    assert!(read.offset >= written.offset);
    // A token of another stream is refused.
    expand_req.consistency_token = Some("v1:s999999/p0@1".to_string());
    assert!(
        expander
            .expand(ExpandCall {
                caller: &GraphCaller::Internal,
                namespace: "acme",
                expand: &expand_req,
                seeds: vec![Some(PrimaryKey::Str("a".to_string()))],
            })
            .await
            .is_err()
    );
}

// ---------------------------------------------------------------------------------------------
// GR1 Task 21: the graph table functions' backend
// ---------------------------------------------------------------------------------------------

use loams_query::graph::{DegreeCall, GraphCaller, GraphTables, NeighborsCall, PathCall};

fn direction(case: &Value) -> ExpandDirection {
    match case["direction"].as_str().unwrap_or("both") {
        "out" => ExpandDirection::Out,
        "in" => ExpandDirection::In,
        _ => ExpandDirection::Both,
    }
}

fn types(case: &Value) -> Vec<String> {
    case["edge_types"]
        .as_array()
        .map(|t| {
            t.iter()
                .map(|t| t.as_str().expect("type").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn keys(case: &Value) -> Vec<PrimaryKey> {
    case["keys"]
        .as_array()
        .expect("keys")
        .iter()
        .filter_map(pk)
        .collect()
}

/// Runs one table-function case of the fixture and answers its rows as the reference's JSON.
async fn table(expander: &LoamsGraphExpander, case: &Value) -> Value {
    let key = |pk: &PrimaryKey| loams_query::json::pk::to_json(pk);
    let label = case["label"].as_str().map(str::to_string);
    match case["function"].as_str().expect("function") {
        "neighbors" => {
            let rows = expander
                .neighbors(NeighborsCall {
                    caller: &GraphCaller::Internal,
                    namespace: "acme",
                    graph: "g",
                    keys: keys(case),
                    label,
                    direction: direction(case),
                    edge_types: types(case),
                    edge_filter: (!case["edge_filter"].is_null()).then(|| {
                        serde_json::from_value(case["edge_filter"].clone()).expect("a query")
                    }),
                    limit_per_key: usize::try_from(case["limit_per_key"].as_u64().unwrap_or(100))
                        .expect("small"),
                })
                .await
                .expect("neighbors");
            json!(
                rows.iter()
                    .map(|r| json!({
                        "key": key(&r.key),
                        "direction": if r.direction == ExpandDirection::In { "in" } else { "out" },
                        "edge_type": r.edge_type, "edge": key(&r.edge),
                        "other_label": r.other_label, "other_key": key(&r.other_key),
                    }))
                    .collect::<Vec<_>>()
            )
        }
        "degree" => {
            let rows = expander
                .degree(DegreeCall {
                    caller: &GraphCaller::Internal,
                    namespace: "acme",
                    graph: "g",
                    keys: keys(case),
                    label,
                    direction: direction(case),
                    edge_types: types(case),
                })
                .await
                .expect("degree");
            json!(
                rows.iter()
                    .map(|(k, d)| json!({"key": key(k), "degree": d}))
                    .collect::<Vec<_>>()
            )
        }
        _ => {
            let rows = expander
                .shortest_path(PathCall {
                    caller: &GraphCaller::Internal,
                    namespace: "acme",
                    graph: "g",
                    from: pk(&case["from"]).expect("from"),
                    to: pk(&case["to"]).expect("to"),
                    label,
                    direction: direction(case),
                    edge_types: types(case),
                    max_hops: u8::try_from(case["max_hops"].as_u64().unwrap_or(6)).expect("small"),
                })
                .await
                .expect("path");
            json!(
                rows.iter()
                    .map(|s| json!({
                        "step": s.step, "label": s.label, "key": key(&s.key),
                        "edge_type": s.edge_type, "edge": s.edge.as_ref().map(key),
                    }))
                    .collect::<Vec<_>>()
            )
        }
    }
}

/// `graph_neighbors`, `graph_degree` and `graph_shortest_path` answer the networkx reference's
/// rows, before and after the fixture's deletes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udtf_results_match_reference() {
    let fixture = reference("expand_fixture.json");
    let expected = reference("expand_expected.json");
    let cluster = Cluster::start(1, SHORT).await;
    load(&cluster, &fixture).await;
    let expander = LoamsGraphExpander::new(cluster.node(1).admin.clone());
    for phase in ["before", "after"] {
        if phase == "after" {
            delete(&cluster, &fixture).await;
        }
        for case in fixture["tables"].as_array().expect("tables") {
            let name = case["name"].as_str().expect("name");
            assert_eq!(
                table(&expander, case).await,
                expected[phase][name],
                "{phase} {name}"
            );
        }
    }
}

/// A shortest path longer than `max_hops` is not found; within the bound it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortest_path_depth_bound() {
    let cluster = Cluster::start(1, SHORT).await;
    let admin = &cluster.node(1).admin;
    create(admin, "g").await;
    run(
        admin,
        "g",
        "INSERT (:v {_key: 'a'})-[:R {_key: 'r1'}]->(:v {_key: 'b'})-[:R {_key: 'r2'}]->(:v {_key: 'c'})-[:R {_key: 'r3'}]->(:v {_key: 'd'})",
    )
    .await;
    let expander = LoamsGraphExpander::new(admin.clone());
    let path = |max_hops: u8| PathCall {
        caller: &GraphCaller::Internal,
        namespace: "acme",
        graph: "g",
        from: PrimaryKey::Str("a".to_string()),
        to: PrimaryKey::Str("d".to_string()),
        label: None,
        direction: ExpandDirection::Out,
        edge_types: Vec::new(),
        max_hops,
    };
    assert!(
        expander
            .shortest_path(path(2))
            .await
            .expect("path")
            .is_empty()
    );
    let found = expander.shortest_path(path(3)).await.expect("path");
    assert_eq!(
        found
            .iter()
            .map(|s| (s.step, s.key.clone(), s.edge.clone()))
            .collect::<Vec<_>>(),
        vec![
            (0, PrimaryKey::Str("a".to_string()), None),
            (
                1,
                PrimaryKey::Str("b".to_string()),
                Some(PrimaryKey::Str("r1".to_string()))
            ),
            (
                2,
                PrimaryKey::Str("c".to_string()),
                Some(PrimaryKey::Str("r2".to_string()))
            ),
            (
                3,
                PrimaryKey::Str("d".to_string()),
                Some(PrimaryKey::Str("r3".to_string()))
            ),
        ]
    );
    // Against the edges' direction, there is none.
    let mut back = path(6);
    back.direction = ExpandDirection::In;
    assert!(expander.shortest_path(back).await.expect("path").is_empty());
}

// ---------------------------------------------------------------------------------------------
// The graph algorithms (GR1 Task 22; §07 §5.3; R22.0, R22.3, R22.4)
// ---------------------------------------------------------------------------------------------

mod algorithms {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use loams_graph::GraphId;
    use loams_graph::adjacency::format::{
        AdjacencyIndex, EdgeState, Topology, VertexKey, VertexState,
    };
    use loams_graph::adjacency::{AdjacencyConfig, BuildMode, Decline, PinSource, build};
    use loams_graph::changeset::{ChangeOp, GraphRecord, decode};
    use loams_graph::consistency::Consistency;
    use loams_graph::owner::OwnerConfig;
    use loams_graph::retrieval::algos::{
        self, Algorithm, Leiden, PageRank, Score, Scored, WriteBack, compute,
    };
    use serde_json::Value;

    use super::common::{Cluster, create, run, token};
    use super::reference;

    /// The 1,000-vertex fixture `algos.py` wrote, as an index.
    fn fixture() -> AdjacencyIndex {
        let fx = reference("algos_fixture.json");
        let mut topology = Topology::default();
        for node in fx["nodes"].as_array().expect("nodes") {
            let key = match &node["key"] {
                Value::Null => None,
                Value::String(s) => Some(VertexKey::Str(s.clone())),
                n => Some(VertexKey::Int(n.as_i64().expect("an integer key"))),
            };
            topology.vertices.insert(
                node["id"].as_u64().expect("id"),
                VertexState {
                    labels: node["labels"]
                        .as_array()
                        .expect("labels")
                        .iter()
                        .map(|l| l.as_str().expect("label").to_string())
                        .collect(),
                    key,
                },
            );
        }
        for e in fx["edges"].as_array().expect("edges") {
            topology.edges.insert(
                e["id"].as_u64().expect("id"),
                EdgeState {
                    ty: e["ty"].as_str().expect("type").to_string(),
                    src: e["src"].as_u64().expect("src"),
                    dst: e["dst"].as_u64().expect("dst"),
                    weight: e["weight"].as_f64().unwrap_or(1.0),
                },
            );
        }
        AdjacencyIndex::from_topology(GraphId(ulid::Ulid::nil()), 0, &topology).expect("index")
    }

    fn types(case: &Value) -> Vec<String> {
        case["edge_types"]
            .as_array()
            .map(|t| {
                t.iter()
                    .map(|t| t.as_str().expect("type").to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn pagerank_matches_networkx_within_1e_6() {
        let view = fixture();
        for case in reference("algos_expected.json")["pagerank"]
            .as_array()
            .expect("cases")
        {
            let name = case["name"].as_str().expect("name");
            let params = PageRank {
                label: case["label"].as_str().map(str::to_string),
                edge_types: types(case),
                damping: case["damping"].as_f64().expect("damping"),
                max_iterations: case["max_iterations"].as_u64().expect("iterations") as u32,
                tolerance: case["tolerance"].as_f64().expect("tolerance"),
            };
            let ranks = algos::pagerank(&view, &params).expect("pagerank");
            let expected = case["ranks"].as_object().expect("networkx converged");
            assert_eq!(ranks.len(), expected.len(), "{name}: the vertices ranked");
            let mut worst = 0f64;
            for (ordinal, rank) in ranks {
                let want = expected[&view.vertices[ordinal].id.to_string()]
                    .as_f64()
                    .expect("rank");
                worst = worst.max((rank - want).abs());
            }
            assert!(worst < 1e-6, "{name}: off by {worst}");
        }
    }

    #[test]
    fn wcc_matches_reference() {
        let view = fixture();
        for case in reference("algos_expected.json")["wcc"]
            .as_array()
            .expect("cases")
        {
            let name = case["name"].as_str().expect("name");
            let expected = case["components"].as_object().expect("components");
            let got = algos::wcc(&view, &types(case));
            assert_eq!(got.len(), expected.len(), "{name}");
            for (ordinal, component) in got {
                assert_eq!(
                    Some(component),
                    expected[&view.vertices[ordinal].id.to_string()].as_u64(),
                    "{name}: vertex {}",
                    view.vertices[ordinal].id
                );
            }
        }
    }

    /// R22.2's spike, kept as the test: `network_partitions` gives graspologic-native's answer on
    /// the 1,000-vertex graph, row for row, and the modularity networkx measures.
    #[test]
    fn leiden_modularity_within_reference_tolerance() {
        let view = fixture();
        for case in reference("algos_expected.json")["leiden"]
            .as_array()
            .expect("cases")
        {
            let name = case["name"].as_str().expect("name");
            let weighted = case["weighted"].as_bool().expect("weighted");
            let resolution = case["resolution"].as_f64().expect("resolution");
            let params = Leiden {
                edge_types: types(case),
                weight: weighted.then(|| "weight".to_string()),
                resolution,
                max_levels: 64,
                max_cluster_size: case["max_cluster_size"].as_u64().expect("size") as u32,
                seed: case["seed"].as_u64().expect("seed"),
            };
            let rows = algos::leiden(&view, &params).expect("leiden");
            let got: Vec<(u32, u64, u64, Option<u64>)> = rows
                .iter()
                .map(|r| (r.level, view.vertices[r.ordinal].id, r.community, r.parent))
                .collect();
            let mut want: Vec<(u32, u64, u64, Option<u64>)> = case["rows"]
                .as_array()
                .expect("rows")
                .iter()
                .map(|r| {
                    // [level, vertex, community, parent, final]
                    (
                        r[0].as_u64().expect("level") as u32,
                        r[1].as_u64().expect("vertex"),
                        r[2].as_u64().expect("community"),
                        r[3].as_u64(),
                    )
                })
                .collect();
            want.sort_by_key(|(level, vertex, ..)| (*level, view.ordinal(*vertex)));
            assert_eq!(got, want, "{name}: graspologic-native's hierarchy");
            let partition = |pick: &dyn Fn(&algos::Community) -> bool| -> BTreeMap<usize, u64> {
                rows.iter()
                    .filter(|r| pick(r))
                    .map(|r| (r.ordinal, r.community))
                    .collect()
            };
            let level0 = algos::modularity(
                &view,
                &params.edge_types,
                weighted,
                resolution,
                &partition(&|r| r.level == 0),
            );
            let last = algos::modularity(
                &view,
                &params.edge_types,
                weighted,
                resolution,
                &partition(&|r| r.last),
            );
            let tolerance = 1e-9;
            let reference0 = case["modularity_level0"].as_f64().expect("modularity");
            let reference_final = case["modularity_final"].as_f64().expect("modularity");
            assert!(
                (level0 - reference0).abs() < tolerance,
                "{name}: level 0 {level0} vs {reference0}"
            );
            assert!(
                (last - reference_final).abs() < tolerance,
                "{name}: final {last} vs {reference_final}"
            );
            assert!(
                level0 > 0.8,
                "{name}: the planted communities are found ({level0})"
            );
        }
    }

    /// Writes `n` vertices in two rings joined by one edge, each with a `_key`, and weights.
    async fn rings(cluster: &Cluster, from: u64, n: u64) {
        let admin = &cluster.node(1).admin;
        for k in from..from + n {
            run(admin, "kg", &format!("INSERT (:N {{_key: {k}, k: {k}}})")).await;
            if k > 0 {
                let prev = if k % 10 == 0 { k - 10 } else { k - 1 };
                run(
                    admin,
                    "kg",
                    &format!(
                        "MATCH (a:N {{k: {prev}}}), (b:N {{k: {k}}}) INSERT (a)-[:R {{weight: {}}}]->(b)",
                        1 + k % 3
                    ),
                )
                .await;
            }
        }
    }

    fn all_algorithms() -> Vec<Algorithm> {
        vec![
            Algorithm::PageRank(PageRank::default()),
            Algorithm::Wcc {
                edge_types: Vec::new(),
            },
            Algorithm::Leiden(Leiden {
                weight: Some("weight".to_string()),
                max_cluster_size: 4,
                ..Leiden::default()
            }),
        ]
    }

    fn answers(view: &AdjacencyIndex) -> Vec<Vec<Scored>> {
        all_algorithms()
            .iter()
            .map(|a| compute(view, a).expect("computes"))
            .collect()
    }

    async fn cluster(config: AdjacencyConfig) -> Cluster {
        let mut cluster = Cluster::start(1, OwnerConfig::default()).await;
        let node = &mut cluster.nodes[0];
        node.admin = node.admin.clone().with_adjacency_config(config);
        create(&cluster.node(1).admin, "kg").await;
        cluster
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn algorithms_deterministic_for_seed_and_snapshot() {
        let cluster = cluster(AdjacencyConfig {
            incremental_max_fraction: 100.0,
            ..AdjacencyConfig::default()
        })
        .await;
        let admin = cluster.node(1).admin.clone();
        rings(&cluster, 0, 12).await;
        let first = admin
            .run_algorithm("acme", "kg", &all_algorithms()[0], &Consistency::Strong)
            .await
            .expect("pagerank");
        assert_eq!(first.pinned.source, PinSource::Built);
        rings(&cluster, 12, 12).await;
        // (a) The incrementally built index.
        let built = admin.build_adjacency("acme", "kg").await.expect("build");
        assert_eq!(built.mode, BuildMode::Incremental);
        let indexer = admin.indexer().expect("indexer").clone();
        let ns = admin.namespace_of("acme", "kg").await.expect("ns");
        let graph = admin.open("acme", "kg").await.expect("open");
        let incremental = indexer
            .pin(
                Arc::clone(&graph),
                ns,
                &Consistency::Strong,
                Duration::from_secs(5),
            )
            .await
            .expect("pin");
        assert_eq!(incremental.source, PinSource::Index);
        // (b) A full build.
        let held = Arc::clone(&graph);
        let full = tokio::task::spawn_blocking(move || build::build_full(&held).expect("full").0)
            .await
            .expect("join");
        assert_eq!(full.covered_offset, incremental.offset);
        // (c) The fallback: the published file no longer checks.
        let id = cluster.graph_id("kg").await;
        admin
            .checkpointer()
            .expect("checkpointer")
            .manifests()
            .update(ns, id, None, |m| {
                if let Some(entry) = &mut m.adjacency {
                    entry.sha256 = "f".repeat(64);
                }
                Ok((true, ()))
            })
            .await
            .expect("edit");
        indexer.forget(id);
        let fallback = indexer
            .pin(graph, ns, &Consistency::Strong, Duration::from_secs(5))
            .await
            .expect("pin");
        assert_eq!(fallback.source, PinSource::Fallback(Decline::HashMismatch));
        let expected = answers(&full);
        assert_eq!(answers(&incremental.view), expected, "incremental = full");
        assert_eq!(answers(&fallback.view), expected, "fallback = full");
        assert_eq!(answers(&full), expected, "the same seed, the same answer");
        // Through the admin, at the same snapshot.
        for (algorithm, want) in all_algorithms().iter().zip(&expected) {
            let answer = admin
                .run_algorithm("acme", "kg", algorithm, &Consistency::Strong)
                .await
                .expect("runs");
            assert_eq!(&answer.rows, want);
        }
        // Another seed may answer otherwise, and is again repeatable.
        let other = Algorithm::Leiden(Leiden {
            seed: 7,
            max_cluster_size: 4,
            ..Leiden::default()
        });
        assert_eq!(
            compute(&full, &other).expect("leiden"),
            compute(&incremental.view, &other).expect("leiden")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_back_is_one_atomic_job() {
        let cluster = cluster(AdjacencyConfig::default()).await;
        let admin = cluster.node(1).admin.clone();
        rings(&cluster, 0, 20).await;
        let before = token(&run(&admin, "kg", "MATCH (n:N) RETURN count(n)").await).offset;
        let job = WriteBack {
            algorithm: Algorithm::PageRank(PageRank::default()),
            field: "rank".to_string(),
        };
        let outcome = admin
            .write_back("acme", "kg", &job)
            .await
            .expect("write-back");
        assert_eq!(outcome.offset, before);
        assert_eq!(outcome.written, 20);
        assert_eq!(
            outcome.committed,
            Some(before + 1),
            "one record: all of it, or none"
        );
        // The one record holds every vertex's rank.
        let stream = cluster.stream_of("kg").await;
        let records = cluster.records(stream).await;
        let last = records.last().expect("a record");
        assert_eq!(last.offset, before);
        let GraphRecord::ChangeSet(change) =
            decode(last.record.value.as_deref().expect("value")).expect("decodes")
        else {
            panic!("a change set");
        };
        assert_eq!(change.ops.len(), 20);
        assert!(change.link.is_none());
        let answer = admin
            .run_algorithm("acme", "kg", &job.algorithm, &Consistency::Strong)
            .await
            .expect("pagerank");
        let ranks: BTreeMap<u64, f64> = answer
            .rows
            .iter()
            .map(|r| match r.score {
                Score::Rank(rank) => (r.element, rank),
                _ => panic!("a rank"),
            })
            .collect();
        for op in &change.ops {
            let ChangeOp::SetProps { id, set, .. } = op else {
                panic!("{op:?}");
            };
            assert_eq!(set.get("rank"), Some(&grafeo::Value::Float64(ranks[id])));
        }
        // The engine has them, and the write-back's own record does not change the ranks (no
        // topology changed).
        let rows = run(
            &admin,
            "kg",
            "MATCH (n:N) WHERE n.rank IS NOT NULL RETURN count(n)",
        )
        .await;
        assert_eq!(
            format!(
                "{:?}",
                rows.rows.as_option().expect("rows").rows[0].values[0].kind
            ),
            "Some(Int64(20))"
        );
        // A link's own property is refused.
        let refused = admin
            .write_back(
                "acme",
                "kg",
                &WriteBack {
                    field: "_key".to_string(),
                    ..job.clone()
                },
            )
            .await
            .expect_err("refused");
        assert_eq!(refused.code, connectrpc::ErrorCode::InvalidArgument);
    }

    /// GR1 R24.12 (amending R24.4): every read `loams-query` makes for a principal (a search's
    /// `expand`, the SQL graph and algorithm functions) needs the principal's `reader` relation on
    /// the graph, decided as an RPC's (fence, relation, scopes) and audited; a refusal is
    /// `permission_denied` with need `reader` and nothing is read. Loams' own calls are not
    /// checked: `GraphCaller::Internal`, and a write-back under an authorizer that allows nobody.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn principal_reads_need_reader() {
        use loams_collection::PrimaryKey;
        use loams_graph::authz::{self, Caller, CallerKind, PrincipalReads, Relation, StaticRbac};
        use loams_graph::retrieval::LoamsGraphExpander;
        use loams_query::ServiceError;
        use loams_query::expand::{ExpandCall, ExpandDirection, GraphExpander};
        use loams_query::graph::{
            AlgorithmCall, AlgorithmSpec, DegreeCall, GraphCaller, GraphTables, NeighborsCall,
            PathCall,
        };

        let cluster = cluster(AdjacencyConfig::default()).await;
        // Loams itself (no caller) writes and administers, under an authorizer of nobody.
        let admin = cluster
            .node(1)
            .admin
            .clone()
            .with_authorizer(Arc::new(StaticRbac::new()));
        rings(&cluster, 0, 6).await;
        let rbac: Arc<dyn authz::GraphAuthorizer> = Arc::new(
            StaticRbac::new()
                .grant("reader", "acme", Relation::Reader)
                .grant("elsewhere", "other", Relation::Admin),
        );
        let principal =
            |caller: Caller| PrincipalReads::new(rbac.clone(), caller).into_graph_caller();
        let scoped = Caller {
            scopes: Some(["collections:read".to_string()].into()),
            ..Caller::new("reader", CallerKind::ApiKey)
        };
        let refused = [
            (
                "no relation",
                principal(Caller::new("stranger", CallerKind::Session)),
            ),
            (
                "another namespace's admin",
                principal(Caller::new("elsewhere", CallerKind::Session)),
            ),
            ("a reader without a graph scope", principal(scoped)),
            ("anonymous", principal(Caller::anonymous())),
        ];
        let reader = principal(Caller::new("reader", CallerKind::Session));
        let expander = LoamsGraphExpander::new(admin.clone());
        let key = |k: u64| PrimaryKey::U64(k);
        let expand = loams_query::expand::Expand {
            graph: "kg".to_string(),
            from_field: None,
            seed_label: None,
            seeds: 1,
            hops: 1,
            direction: ExpandDirection::Out,
            edge_types: Vec::new(),
            edge_filter: None,
            vertex_filter: None,
            limit_per_seed: 50,
            limit: 100,
            select: None,
            output: loams_query::expand::ExpandOutput::Nested,
            consistency_token: None,
        };
        // Every read, for `caller`: what it is, and how many rows it answered.
        let reads = |caller: GraphCaller| {
            let (expander, expand) = (expander.clone(), expand.clone());
            async move {
                let mut out: Vec<(&'static str, Result<usize, ServiceError>)> = Vec::new();
                let caller = &caller;
                out.push((
                    "expand",
                    expander
                        .expand(ExpandCall {
                            caller,
                            namespace: "acme",
                            expand: &expand,
                            seeds: vec![Some(key(0))],
                        })
                        .await
                        .map(|e| e.reached.len()),
                ));
                out.push((
                    "graph_neighbors",
                    expander
                        .neighbors(NeighborsCall {
                            caller,
                            namespace: "acme",
                            graph: "kg",
                            keys: vec![key(0)],
                            label: None,
                            direction: ExpandDirection::Out,
                            edge_types: Vec::new(),
                            edge_filter: None,
                            limit_per_key: 100,
                        })
                        .await
                        .map(|e| e.len()),
                ));
                out.push((
                    "graph_degree",
                    expander
                        .degree(DegreeCall {
                            caller,
                            namespace: "acme",
                            graph: "kg",
                            keys: vec![key(0)],
                            label: None,
                            direction: ExpandDirection::Out,
                            edge_types: Vec::new(),
                        })
                        .await
                        .map(|d| d.len()),
                ));
                out.push((
                    "graph_shortest_path",
                    expander
                        .shortest_path(PathCall {
                            caller,
                            namespace: "acme",
                            graph: "kg",
                            from: key(0),
                            to: key(2),
                            label: None,
                            direction: ExpandDirection::Out,
                            edge_types: Vec::new(),
                            max_hops: 6,
                        })
                        .await
                        .map(|p| p.len()),
                ));
                for (name, spec) in [
                    (
                        "pagerank",
                        AlgorithmSpec::PageRank {
                            label: None,
                            damping: 0.85,
                            max_iterations: 20,
                            tolerance: 1e-6,
                        },
                    ),
                    ("wcc", AlgorithmSpec::Wcc),
                    (
                        "leiden",
                        AlgorithmSpec::Leiden {
                            weight: None,
                            resolution: 1.0,
                            max_levels: 4,
                            max_cluster_size: 1000,
                            seed: 42,
                        },
                    ),
                ] {
                    out.push((
                        name,
                        expander
                            .algorithm(AlgorithmCall {
                                caller,
                                namespace: "acme",
                                graph: "kg",
                                edge_types: Vec::new(),
                                spec,
                            })
                            .await
                            .map(|rows| rows.len()),
                    ));
                }
                out
            }
        };
        for (who, caller) in refused {
            let before = authz::denials();
            let answers = reads(caller).await;
            assert_eq!(answers.len(), 7);
            for (what, answer) in &answers {
                match answer {
                    Err(ServiceError::PermissionDenied {
                        need,
                        object,
                        message,
                    }) => {
                        assert_eq!(need, "reader", "{who}: {what}");
                        assert_eq!(object, "graph:acme/kg", "{who}: {what}");
                        assert!(
                            message.starts_with(&format!("{what} on graph:acme/kg")),
                            "{message}"
                        );
                    }
                    other => panic!("{who}: {what} is refused, got {other:?}"),
                }
            }
            assert!(
                authz::denials() >= before + 7,
                "{who}: every refusal is counted"
            );
        }
        // A reader, and Loams itself, read all of it, and alike.
        let allowed = reads(reader).await;
        let internal = reads(GraphCaller::Internal).await;
        for ((what, a), (_, b)) in allowed.iter().zip(&internal) {
            let a = a
                .as_ref()
                .unwrap_or_else(|err| panic!("reader: {what}: {err}"));
            let b = b
                .as_ref()
                .unwrap_or_else(|err| panic!("internal: {what}: {err}"));
            assert_eq!(a, b, "{what}");
            assert!(*a > 0, "{what} answered rows");
        }
        // A write-back is Loams' own task: no principal, and the authorizer allows nobody.
        let outcome = admin
            .write_back(
                "acme",
                "kg",
                &WriteBack {
                    algorithm: Algorithm::Wcc {
                        edge_types: Vec::new(),
                    },
                    field: "component".to_string(),
                },
            )
            .await
            .expect("an internal write-back");
        assert_eq!(outcome.written, 6);
    }
}
