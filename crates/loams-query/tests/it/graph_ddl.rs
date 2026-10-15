//! `CREATE GRAPH … FROM COLLECTION …` (design §07 §2.1, GR1 Task 17): parsed into the very
//! `loams.graph.v1.GraphMapping` a LINKED graph's `CreateGraph` takes.

use loams_proto::loams::graph::v1 as pb;
use loams_query::sql::{is_create_graph, parse_create_graph};

/// §07 §2.1's example, as `CreateGraph` takes it over the RPC (`loams-graph`'s
/// `tests/links.rs` creates the same graph from the same mapping).
fn rpc_mapping() -> pb::GraphMapping {
    let vertex = |label: &str, collection: &str| pb::VertexMapping {
        label: label.to_string(),
        collection: collection.to_string(),
        key_field: "_pk".to_string(),
        ..Default::default()
    };
    pb::GraphMapping {
        vertices: vec![vertex("entity", "entities"), vertex("chunk", "chunks")],
        edges: vec![
            pb::EdgeMapping {
                r#type: "RELATED".to_string(),
                collection: "relations".to_string(),
                source_label: "entity".to_string(),
                source_field: "src".to_string(),
                destination_label: "entity".to_string(),
                destination_field: "dst".to_string(),
                type_field: "rel_type".to_string(),
                ..Default::default()
            },
            pb::EdgeMapping {
                r#type: "MENTIONS".to_string(),
                collection: "mentions".to_string(),
                source_label: "chunk".to_string(),
                source_field: "chunk_id".to_string(),
                destination_label: "entity".to_string(),
                destination_field: "entity_id".to_string(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

const KG: &str = "CREATE GRAPH kg
  VERTEX entity   KEY (_pk)                                  FROM COLLECTION entities
  VERTEX chunk    KEY (_pk)                                  FROM COLLECTION chunks
  EDGE   RELATED  SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations
  EDGE   MENTIONS SOURCE chunk(chunk_id) DESTINATION entity(entity_id)          FROM COLLECTION mentions;";

#[test]
fn sql_create_graph_equals_rpc_mapping() {
    assert!(is_create_graph(KG));
    assert!(is_create_graph("/* a graph */ create\n\tgraph kg"));
    assert!(!is_create_graph("SELECT 1"));
    assert!(!is_create_graph("CREATE \"GRAPH\" kg"));
    assert!(!is_create_graph("CREATE 'unterminated"));
    let parsed = parse_create_graph(KG).expect("§07 §2.1's example parses");
    assert_eq!(parsed.name, "kg");
    assert_eq!(parsed.mapping, rpc_mapping());
    // Keywords in any case, commas between declarations and no semicolon parse the same.
    let relaxed = "create graph kg vertex entity key (_pk) from collection entities, \
                   vertex chunk key (_pk) from collection chunks, \
                   edge RELATED source entity(src) destination entity(dst) type from (rel_type) \
                   from collection relations, \
                   edge MENTIONS source chunk(chunk_id) destination entity(entity_id) \
                   from collection mentions";
    assert_eq!(parse_create_graph(relaxed).expect("relaxed"), parsed);
}

#[test]
fn type_from_column_maps_many_types() {
    let parsed = parse_create_graph(KG).expect("parse");
    // `TYPE FROM (rel_type)`: each document's type is its `rel_type`, under one declaration.
    assert_eq!(parsed.mapping.edges[0].r#type, "RELATED");
    assert_eq!(parsed.mapping.edges[0].type_field, "rel_type");
    // No `TYPE FROM`: the declared name is every edge's type.
    assert_eq!(parsed.mapping.edges[1].type_field, "");
}

#[test]
fn malformed_create_graph_is_refused_with_what_was_expected() {
    for (sql, expected) in [
        ("CREATE GRAPH kg", "VERTEX or EDGE"),
        (
            "CREATE GRAPH kg VERTEX entity KEY _pk FROM COLLECTION entities",
            "`(`",
        ),
        (
            "CREATE GRAPH kg VERTEX entity KEY (_pk) FROM entities",
            "COLLECTION",
        ),
        (
            "CREATE GRAPH kg VERTEX e KEY (_pk) FROM COLLECTION c EDGE R SOURCE e(a) FROM COLLECTION r",
            "DESTINATION",
        ),
        (
            "CREATE GRAPH kg VERTEX e KEY (_pk) FROM COLLECTION c; DROP TABLE x",
            "the end of the statement",
        ),
    ] {
        let err = parse_create_graph(sql).expect_err(sql);
        assert!(err.to_string().contains(expected), "{sql}: {err}");
    }
}
