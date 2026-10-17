//! Who a graph read is for (GR1 R24.12, amending R24.4): a search's `expand` and every SQL graph
//! and algorithm table function hand the graph backend the request's [`GraphCaller`], and a
//! refusal fails the whole request (`permission_denied`, need `reader`); Loams' own calls are
//! `GraphCaller::Internal`. The backend here is a recorder that checks the caller as Loams
//! Graph's does and answers nothing.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loams_collection::{CollectionSchema, DynamicMapping, FieldKind, PrimaryKey};
use loams_query::expand::{ExpandCall, Expanded, GraphExpander};
use loams_query::graph::{
    AlgorithmCall, AlgorithmRow, DegreeCall, GraphCaller, GraphReadGate, GraphTables, IncidentEdge,
    NeighborsCall, PathCall, PathStep,
};
use loams_query::json::hybrid::parse_query_body;
use loams_query::sql::run_read_only;
use loams_query::{CollectionService, ServiceError, WriteOptions};
use serde_json::json;

use crate::common::{Fixture, field, upsert};

const NS: &str = "acme";

/// The graphs a backend was asked to read, after their caller passed.
#[derive(Debug, Default)]
struct Recorder {
    reads: Mutex<Vec<(String, String)>>,
}

impl Recorder {
    async fn read(
        &self,
        caller: &GraphCaller,
        what: &str,
        ns: &str,
        graph: &str,
    ) -> Result<(), ServiceError> {
        caller.authorize_read(what, ns, graph).await?;
        self.reads
            .lock()
            .expect("the recorder's lock")
            .push((what.to_string(), graph.to_string()));
        Ok(())
    }

    fn take(&self) -> Vec<(String, String)> {
        std::mem::take(&mut *self.reads.lock().expect("the recorder's lock"))
    }
}

#[async_trait]
impl GraphExpander for Recorder {
    async fn expand(&self, call: ExpandCall<'_>) -> Result<Expanded, ServiceError> {
        self.read(call.caller, "expand", call.namespace, &call.expand.graph)
            .await?;
        Ok(Expanded::default())
    }
}

#[async_trait]
impl GraphTables for Recorder {
    async fn neighbors(&self, call: NeighborsCall<'_>) -> Result<Vec<IncidentEdge>, ServiceError> {
        self.read(call.caller, "graph_neighbors", call.namespace, call.graph)
            .await?;
        Ok(Vec::new())
    }

    async fn degree(&self, call: DegreeCall<'_>) -> Result<Vec<(PrimaryKey, u64)>, ServiceError> {
        self.read(call.caller, "graph_degree", call.namespace, call.graph)
            .await?;
        Ok(Vec::new())
    }

    async fn shortest_path(&self, call: PathCall<'_>) -> Result<Vec<PathStep>, ServiceError> {
        self.read(
            call.caller,
            "graph_shortest_path",
            call.namespace,
            call.graph,
        )
        .await?;
        Ok(Vec::new())
    }

    async fn algorithm(&self, call: AlgorithmCall<'_>) -> Result<Vec<AlgorithmRow>, ServiceError> {
        self.read(call.caller, "algorithm", call.namespace, call.graph)
            .await?;
        Ok(Vec::new())
    }
}

/// A principal who is a `reader` of the graphs named, and of no other.
#[derive(Debug)]
struct ReaderOf(BTreeSet<String>);

#[async_trait]
impl GraphReadGate for ReaderOf {
    async fn authorize_read(
        &self,
        what: &str,
        namespace: &str,
        graph: &str,
    ) -> Result<(), ServiceError> {
        if self.0.contains(graph) {
            return Ok(());
        }
        GraphCaller::nobody()
            .authorize_read(what, namespace, graph)
            .await
    }
}

fn reader_of(graphs: &[&str]) -> GraphCaller {
    GraphCaller::Principal(Arc::new(ReaderOf(
        graphs.iter().map(ToString::to_string).collect(),
    )))
}

fn assert_denied<T: std::fmt::Debug>(result: Result<T, ServiceError>, graph: &str, what: &str) {
    match result {
        Err(ServiceError::PermissionDenied { need, object, .. }) => {
            assert_eq!(need, "reader", "{what}");
            assert_eq!(object, format!("graph:{NS}/{graph}"), "{what}");
        }
        other => panic!("{what}: expected PermissionDenied, got {other:?}"),
    }
}

/// A service over `kb` (two documents), with the recorder as its graph backend.
async fn with_graphs() -> (Fixture, Arc<CollectionService>, Arc<Recorder>) {
    let f = Fixture::start().await;
    let service = f.service();
    let schema = CollectionSchema::new(
        vec![field("name", FieldKind::Keyword)],
        vec![],
        DynamicMapping::Ignore,
    );
    service
        .create_collection(NS, "kb", schema, Some(1))
        .await
        .expect("create kb");
    service
        .write(
            NS,
            "kb",
            vec![
                upsert(1, json!({"name": "a"})),
                upsert(2, json!({"name": "b"})),
            ],
            WriteOptions::default(),
        )
        .await
        .expect("write");
    f.settle().await;
    let recorder = Arc::new(Recorder::default());
    service.set_graph_expander(recorder.clone());
    service.set_graph_tables(recorder.clone());
    (f, service, recorder)
}

fn expanding(graph: &str) -> loams_query::SearchRequest {
    parse_query_body(
        json!({"collection": "kb", "limit": 5, "expand": {"graph": graph}}),
        0,
    )
    .expect("a search")
}

/// R24.12: an expansion runs only for a caller who may read its graph; a refusal fails the
/// whole search, and the graph is not read.
#[tokio::test]
async fn search_expand_needs_reader_on_its_graph() {
    let (_f, service, recorder) = with_graphs().await;
    assert_denied(
        service
            .search_as(NS, expanding("kg"), &GraphCaller::nobody())
            .await,
        "kg",
        "a caller with no relation",
    );
    assert_denied(
        service
            .search_as(NS, expanding("kg"), &reader_of(&["other"]))
            .await,
        "kg",
        "a reader of another graph",
    );
    assert!(recorder.take().is_empty(), "nothing was read");
    let response = service
        .search_as(NS, expanding("kg"), &reader_of(&["kg"]))
        .await
        .expect("a reader expands");
    assert_eq!(response.hits.len(), 2);
    assert_eq!(
        recorder.take(),
        vec![("expand".to_string(), "kg".to_string())]
    );
    // Loams' own expansion is not checked.
    service
        .search_as(NS, expanding("kg"), &GraphCaller::Internal)
        .await
        .expect("internal");
    // A search that names no caller cannot expand at all.
    assert!(matches!(
        service.search(NS, expanding("kg")).await,
        Err(ServiceError::Internal(_))
    ));
}

/// R24.12: each SQL graph and algorithm function needs `reader` on its graph, the key query of
/// `graph_expand` included, and a refusal fails the statement.
#[tokio::test]
async fn sql_graph_functions_need_reader_on_their_graph() {
    let (_f, service, recorder) = with_graphs().await;
    let statements = [
        "SELECT * FROM graph_expand('kg', 'a')",
        "SELECT * FROM graph_neighbors('kg', ARRAY['a'])",
        "SELECT * FROM graph_degree('kg', ARRAY['a'])",
        "SELECT * FROM graph_shortest_path('kg', 'a', 'b')",
        "SELECT * FROM pagerank('kg')",
        "SELECT * FROM wcc('kg')",
        "SELECT * FROM leiden('kg')",
    ];
    let config = service.config().sql.clone();
    for statement in statements {
        for caller in [GraphCaller::nobody(), reader_of(&["other"])] {
            let ctx = service.sql_context(NS, caller);
            assert_denied(
                run_read_only(&ctx, statement, &config).await,
                "kg",
                statement,
            );
        }
        assert!(recorder.take().is_empty(), "{statement}: nothing was read");
        let ctx = service.sql_context(NS, reader_of(&["kg"]));
        run_read_only(&ctx, statement, &config)
            .await
            .unwrap_or_else(|err| panic!("{statement}: {err}"));
        assert_eq!(recorder.take().len(), 1, "{statement}");
        let ctx = service.sql_context(NS, GraphCaller::Internal);
        run_read_only(&ctx, statement, &config)
            .await
            .unwrap_or_else(|err| panic!("{statement} (internal): {err}"));
        recorder.take();
    }
    // A graph read inside the key query is the same caller's.
    let nested = "SELECT * FROM graph_expand('kg', \
                  'SELECT other_key FROM graph_neighbors(''secret'', ARRAY[''a''])')";
    let ctx = service.sql_context(NS, reader_of(&["kg"]));
    assert_denied(run_read_only(&ctx, nested, &config).await, "secret", nested);
    let ctx = service.sql_context(NS, reader_of(&["kg", "secret"]));
    run_read_only(&ctx, nested, &config)
        .await
        .expect("a reader of both");
}
