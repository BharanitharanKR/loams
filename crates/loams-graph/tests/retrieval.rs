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
                namespace: "acme",
                expand: &expand_req,
                seeds: vec![Some(PrimaryKey::Str("a".to_string()))],
            })
            .await
            .is_err()
    );
}
