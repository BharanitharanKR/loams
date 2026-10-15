//! LINKED graphs (GR1 Task 17; design §07 §2.1, §48 §10.1; D743): a graph declared over
//! collections by a mapping, checked against their schemas when it is created, and read-only to
//! GQL. The links that write it are Tasks 18 and 19.

use std::sync::Arc;
use std::time::Duration;

use connectrpc::{ConnectError, ErrorCode};
use loams_common::meta::MetaStore;
use loams_common::schema::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec};
use loams_graph::Engine;
use loams_graph::catalog::GraphCatalog;
use loams_graph::mapping::{EdgeMap, Endpoint, GraphMapping, PRIMARY_KEY, VertexMap};
use loams_graph::service::admin::GraphAdmin;
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_proto::loams::errors::v1::ErrorInfo;
use loams_proto::loams::graph::v1 as pb;
use loams_store::Store;
use tempfile::TempDir;

/// A metastore with namespace `acme` and its collections, and an admin over it.
struct Fixture {
    _node: MetaNode,
    _meta_dir: TempDir,
    _data_dir: TempDir,
    admin: GraphAdmin,
}

fn field(name: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        // A text field cannot be fast.
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    }
}

impl Fixture {
    async fn start() -> Self {
        let meta_dir = TempDir::new().expect("temp dir");
        let clock = Arc::new(SystemClock);
        let mut config = MetaConfig::new(1, meta_dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(Duration::from_secs(30))
            .await
            .expect("leader");
        let meta: Arc<dyn MetaStore> = Arc::new(MetaClient::new(
            node.clone(),
            vec![],
            clock,
            MetaClientConfig::default(),
        ));
        let namespace = meta.create_namespace("acme").await.expect("namespace");
        for (name, fields) in [
            (
                "entities",
                vec![
                    field("name", FieldKind::Keyword),
                    field("entity_type", FieldKind::Keyword),
                ],
            ),
            (
                "chunks",
                vec![field(
                    "text",
                    FieldKind::Text {
                        analyzer: "standard".to_string(),
                        positions: true,
                    },
                )],
            ),
            (
                "relations",
                vec![
                    field("src", FieldKind::Keyword),
                    field("dst", FieldKind::Keyword),
                    field("rel_type", FieldKind::Keyword),
                    field("weight", FieldKind::F64),
                ],
            ),
            (
                "mentions",
                vec![
                    field("chunk_id", FieldKind::Keyword),
                    field("entity_id", FieldKind::Keyword),
                ],
            ),
        ] {
            meta.create_collection(
                namespace,
                name,
                CollectionSchema::new(fields, Vec::new(), DynamicMapping::Ignore),
                1,
            )
            .await
            .expect("collection");
        }
        let data_dir = TempDir::new().expect("data dir");
        let admin = GraphAdmin::new(
            Arc::new(Engine::with_data_dir(data_dir.path())),
            GraphCatalog::new(meta, Store::in_memory()),
        );
        Self {
            _node: node,
            _meta_dir: meta_dir,
            _data_dir: data_dir,
            admin,
        }
    }
}

fn reason(err: &ConnectError) -> String {
    use base64::Engine as _;
    use buffa::Message as _;
    let detail = err
        .details
        .iter()
        .find(|d| d.type_url == "loams.errors.v1.ErrorInfo")
        .unwrap_or_else(|| panic!("no ErrorInfo in {err:?}"));
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(detail.value.as_deref().unwrap_or_default())
        .expect("unpadded base64");
    ErrorInfo::decode_from_slice(&bytes)
        .expect("an ErrorInfo")
        .reason
}

/// §07 §2.1's example graph.
fn kg() -> GraphMapping {
    let endpoint = |label: &str, field: &str| Endpoint {
        label: label.to_string(),
        field: field.to_string(),
    };
    GraphMapping {
        vertices: vec![
            VertexMap {
                label: "entity".to_string(),
                collection: "entities".to_string(),
                key_field: PRIMARY_KEY.to_string(),
            },
            VertexMap {
                label: "chunk".to_string(),
                collection: "chunks".to_string(),
                key_field: PRIMARY_KEY.to_string(),
            },
        ],
        edges: vec![
            EdgeMap {
                name: "RELATED".to_string(),
                type_field: Some("rel_type".to_string()),
                collection: "relations".to_string(),
                source: endpoint("entity", "src"),
                destination: endpoint("entity", "dst"),
            },
            EdgeMap {
                name: "MENTIONS".to_string(),
                type_field: None,
                collection: "mentions".to_string(),
                source: endpoint("chunk", "chunk_id"),
                destination: endpoint("entity", "entity_id"),
            },
        ],
    }
}

fn linked(name: &str, mapping: &GraphMapping) -> pb::CreateGraphRequest {
    pb::CreateGraphRequest {
        namespace: "acme".to_string(),
        name: name.to_string(),
        mode: pb::GraphMode::Linked.into(),
        mapping: mapping.to_proto().into(),
        ..Default::default()
    }
}

async fn refused(fixture: &Fixture, name: &str, mapping: &GraphMapping) -> ConnectError {
    fixture
        .admin
        .create_graph(linked(name, mapping))
        .await
        .expect_err("an invalid mapping")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mapping_validates_against_schema() {
    let fixture = Fixture::start().await;
    let created = fixture
        .admin
        .create_graph(linked("kg", &kg()))
        .await
        .expect("a valid mapping");
    assert_eq!(created.mode.as_known(), Some(pb::GraphMode::Linked));
    let got = fixture
        .admin
        .get_graph(pb::GetGraphRequest {
            namespace: "acme".to_string(),
            name: "kg".to_string(),
            ..Default::default()
        })
        .await
        .expect("get");
    assert_eq!(
        got.mapping.as_option().map(GraphMapping::from_proto),
        Some(kg()),
        "the catalog keeps the mapping"
    );

    // Each mistake against the schemas is INVALID_ARGUMENT, and creates nothing.
    let cases: Vec<(&str, GraphMapping)> = vec![
        ("no such collection", {
            let mut m = kg();
            m.vertices[1].collection = "pages".to_string();
            m
        }),
        ("no such key field", {
            let mut m = kg();
            m.vertices[0].key_field = "id".to_string();
            m
        }),
        ("a text key", {
            let mut m = kg();
            m.vertices[1].key_field = "text".to_string();
            m
        }),
        ("a float endpoint", {
            let mut m = kg();
            m.edges[0].destination.field = "weight".to_string();
            m
        }),
        ("a float type field", {
            let mut m = kg();
            m.edges[0].type_field = Some("weight".to_string());
            m
        }),
        ("an undeclared endpoint label", {
            let mut m = kg();
            m.edges[1].source.label = "document".to_string();
            m
        }),
    ];
    for (n, (what, mapping)) in cases.iter().enumerate() {
        let err = refused(&fixture, &format!("bad{n}"), mapping).await;
        assert_eq!(err.code, ErrorCode::InvalidArgument, "{what}: {err:?}");
        assert_eq!(reason(&err), "invalid_argument", "{what}");
    }

    // A mode the wire does not name is refused, not read as OWNED.
    let err = fixture
        .admin
        .create_graph(pb::CreateGraphRequest {
            mode: buffa::EnumValue::from(42),
            ..linked("unknown", &kg())
        })
        .await
        .expect_err("an unknown mode");
    assert_eq!(err.code, ErrorCode::InvalidArgument);

    // A LINKED graph needs a mapping; an OWNED one may not have one.
    let err = fixture
        .admin
        .create_graph(pb::CreateGraphRequest {
            namespace: "acme".to_string(),
            name: "nomap".to_string(),
            mode: pb::GraphMode::Linked.into(),
            ..Default::default()
        })
        .await
        .expect_err("no mapping");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    let err = fixture
        .admin
        .create_graph(pb::CreateGraphRequest {
            mode: pb::GraphMode::Owned.into(),
            ..linked("owned", &kg())
        })
        .await
        .expect_err("an OWNED graph with a mapping");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    let listed = fixture
        .admin
        .list_graphs(pb::ListGraphsRequest {
            namespace: "acme".to_string(),
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(
        listed
            .graphs
            .iter()
            .map(|g| g.name.as_str())
            .collect::<Vec<_>>(),
        vec!["kg"]
    );
}

/// One edge collection holds many relation types: `TYPE FROM (rel_type)` keeps the field, and
/// the declared name stays the mapping's name; without it, the name is every edge's type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn type_from_column_maps_many_types() {
    let fixture = Fixture::start().await;
    let created = fixture
        .admin
        .create_graph(linked("kg", &kg()))
        .await
        .expect("create");
    let mapping = GraphMapping::from_proto(created.mapping.as_option().expect("a mapping"));
    let related = &mapping.edges[0];
    assert_eq!(related.name, "RELATED");
    assert_eq!(related.type_field.as_deref(), Some("rel_type"));
    let mentions = &mapping.edges[1];
    assert_eq!(mentions.name, "MENTIONS");
    assert_eq!(mentions.type_field, None);
    // On the wire, an absent TYPE FROM is the empty field.
    let wire = mapping.to_proto();
    assert_eq!(wire.edges[0].type_field, "rel_type");
    assert_eq!(wire.edges[1].type_field, "");
    // A replay of the create with the same key and mapping answers the same graph; another
    // mapping under that key is refused.
    let keyed = |mapping: &GraphMapping| pb::CreateGraphRequest {
        idempotency_key: "k1".to_string(),
        ..linked("keyed", mapping)
    };
    let first = fixture
        .admin
        .create_graph(keyed(&kg()))
        .await
        .expect("create");
    let again = fixture
        .admin
        .create_graph(keyed(&kg()))
        .await
        .expect("replay");
    assert_eq!(first.id, again.id);
    let mut other = kg();
    other.edges[0].type_field = None;
    let err = fixture
        .admin
        .create_graph(keyed(&other))
        .await
        .expect_err("another mapping under the key");
    assert_eq!(err.code, ErrorCode::InvalidArgument);
}

/// GQL may only read a LINKED graph: its links write it (D743).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linked_graph_refuses_gql_writes() {
    let fixture = Fixture::start().await;
    fixture
        .admin
        .create_graph(linked("kg", &kg()))
        .await
        .expect("create");
    let statement = |text: &str| pb::ExecuteRequest {
        namespace: "acme".to_string(),
        graph: "kg".to_string(),
        statement: text.to_string(),
        ..Default::default()
    };
    for write in [
        "INSERT (:entity {name: 'x'})",
        "MATCH (n) SET n.name = 'y'",
        "MATCH (n) DETACH DELETE n",
    ] {
        let err = fixture
            .admin
            .execute(statement(write))
            .await
            .expect_err("a write on a LINKED graph");
        assert_eq!(err.code, ErrorCode::FailedPrecondition, "{write}: {err:?}");
        assert_eq!(reason(&err), "graph_is_linked", "{write}");
    }
    let batch = fixture
        .admin
        .execute_batch(pb::ExecuteBatchRequest {
            namespace: "acme".to_string(),
            graph: "kg".to_string(),
            statements: vec![pb::Statement {
                statement: "INSERT (:entity {name: 'x'})".to_string(),
                ..Default::default()
            }],
            atomic: true,
            ..Default::default()
        })
        .await
        .expect_err("an atomic batch that writes");
    assert_eq!(reason(&batch), "graph_is_linked");
    // Reads are served.
    let read = fixture
        .admin
        .execute(statement("MATCH (n) RETURN count(n) AS n"))
        .await
        .expect("a read");
    assert_eq!(read.rows.as_option().expect("rows").rows.len(), 1);
}
