//! Neo4j over the HTTP Query API: the documented request shapes (synthetic
//! fixture) everywhere, and real Neo4j Community from the `connectors-p2`
//! stack (`neo4j` on :7474) when it is up.
#![allow(clippy::unwrap_used)]

mod common;

use common::*;
use loams_connector_protocols::neo4j::Neo4j;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, ErrorClass, EventBatch, Position, Sink, SinkHandle,
    Source,
};
use serde_json::{Value, json};

fn sink(ctx: &ConnectorContext) -> Box<dyn Sink> {
    match Neo4j.sink(ctx).unwrap().unwrap().create().unwrap() {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("plain"),
    }
}

async fn source(ctx: &ConnectorContext, pos: Option<Position>) -> Box<dyn Source> {
    let (mut en, factory) = Neo4j.source(ctx).unwrap().unwrap();
    let split = en.discover(&[]).await.unwrap().remove(0);
    let mut s = factory.create(&split).unwrap();
    s.open(&split, pos).await.unwrap();
    s
}

#[tokio::test]
async fn neo4j_query_api_request_shapes_fixture() {
    let (_, scenario, proxy) = replay("neo4j/query-api.json").await;
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let ctx = ctx(config, &scenario["secrets"]);

    // Sink: one request per batch with the rows as `$rows`.
    let rows = scenario["rows"].as_array().unwrap();
    let evs: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| event(&format!("e{i}"), None, r))
        .collect();
    let mut s = sink(&ctx);
    assert_eq!(s.write(EventBatch::Events(evs)).await.unwrap().rows, 2);

    // Source: two pages in (cursor, elementId) order, resumed from the
    // position after the first.
    let mut src = source(&ctx, None).await;
    let first = src.next(BUDGET).await.unwrap().unwrap();
    let evs = events(&first);
    assert_eq!(evs.len(), 2);
    assert_eq!(evs[0].ty, "io.loams.dev.flow.neo4j.rows.v1");
    assert_eq!(evs[0].id, "Person/4:synthetic:1@100");
    assert_eq!(evs[0].subject.as_deref(), Some("4:synthetic:1"));
    assert_eq!(data(&evs[1])["n"]["properties"]["name"], "Grace");
    assert_eq!(first.position.display, "cursor 200 after 4:synthetic:2");
    drop(src);
    let mut resumed = source(&ctx, Some(first.position)).await;
    let rest = drain(&mut *resumed).await;
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].id, "Person/4:synthetic:3@300");

    // Basic auth by secret reference, typed JSON for the reads.
    let sent = proxy.received();
    let basic = sent.iter().find(|r| r.method == "POST").unwrap();
    assert!(
        basic
            .headers
            .get("authorization")
            .unwrap()
            .starts_with("Basic ")
    );
    let read = sent.iter().filter(|r| r.method == "POST").nth(1).unwrap();
    assert_eq!(
        read.headers.get("content-type").map(String::as_str),
        Some("application/vnd.neo4j.query")
    );
    proxy.finish().unwrap();
}

#[tokio::test]
async fn neo4j_older_than_5_19_is_refused() {
    use loams_flow_conformance::{Fixture, FixtureProxy, ScrubRules};
    let fixture: Fixture = serde_json::from_value(json!({
        "recorded_from": "Neo4j", "synthetic": true,
        "docs": ["https://neo4j.com/docs/query-api/current/"],
        "exchanges": [{
            "request": {"method": "GET", "path": "/", "body": "empty"},
            "response": {"status": 200, "body": {"json": {"neo4j_version": "5.18.1", "neo4j_edition": "community"}}}
        }]
    }))
    .unwrap();
    let proxy = FixtureProxy::replay(fixture, ScrubRules::default())
        .await
        .unwrap();
    let ctx = ctx(
        json!({"url": proxy.base_url(), "sink": {"statement": "UNWIND $rows AS r MERGE (n {id: r.id})"}}),
        &json!({}),
    );
    let err = sink(&ctx)
        .write(EventBatch::Events(vec![event(
            "e",
            None,
            &json!({"id": 1}),
        )]))
        .await
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Fatal);
    assert!(err.message.contains("5.19"), "{}", err.message);
}

// ------------------------------------------------- real Neo4j (gated) --

fn real() -> Option<ConnectorContext> {
    if !p2_stack() {
        return None;
    }
    let url = std::env::var("LOAMS_NEO4J_URL").unwrap_or_else(|_| "http://localhost:7474".into());
    let label = format!("LoamsT{}", std::process::id());
    Some(ctx(
        json!({
            "url": url,
            "auth": {"basic": {"username": "neo4j", "password": "pw"}},
            "sink": {"statement": format!("UNWIND $rows AS row MERGE (p:`{label}` {{id: row.id}}) SET p.name = row.name, p.updatedAt = row.updatedAt"),
                     "delete_statement": format!("UNWIND $rows AS row MATCH (p:`{label}` {{id: row.id}}) DETACH DELETE p")},
            "source": {"label": label, "cursor": "updatedAt", "page_size": 2}
        }),
        // The compose estate's fixed test password (deploy/fabric/compose.yaml).
        &json!({"pw": std::env::var("LOAMS_NEO4J_PASSWORD").unwrap_or_else(|_| "loams-test-pass".into())}),
    ))
}

fn person(i: u64) -> Value {
    json!({"id": format!("p{i}"), "name": format!("Person {i}"), "updatedAt": i * 10})
}

#[tokio::test]
async fn neo4j_sink_merge_idempotent() {
    let Some(ctx) = real() else {
        return skip(
            "neo4j_sink_merge_idempotent",
            "LOAMS_CONNECTORS_P2_STACK unset",
        );
    };
    let evs: Vec<_> = (1..=3)
        .map(|i| event(&format!("e{i}"), None, &person(i)))
        .collect();
    let mut s = sink(&ctx);
    // The same batch twice (a retry after a lost ack) converges.
    s.write(EventBatch::Events(evs.clone())).await.unwrap();
    s.write(EventBatch::Events(evs)).await.unwrap();
    let mut src = source(&ctx, None).await;
    let read = drain(&mut *src).await;
    assert_eq!(read.len(), 3, "MERGE wrote each node once");
    // Clean up through the delete statement.
    let dels: Vec<_> = (1..=3)
        .map(|i| {
            let mut e = event(&format!("d{i}"), None, &person(i));
            e.extensions.insert("loamsop".into(), "d".into());
            e
        })
        .collect();
    s.write(EventBatch::Events(dels)).await.unwrap();
    let mut src = source(&ctx, None).await;
    assert!(drain(&mut *src).await.is_empty());
}

#[tokio::test]
async fn neo4j_source_http_query_api_cursor() {
    let Some(ctx) = real() else {
        return skip(
            "neo4j_source_http_query_api_cursor",
            "LOAMS_CONNECTORS_P2_STACK unset",
        );
    };
    let mut s = sink(&ctx);
    let evs: Vec<_> = (1..=5)
        .map(|i| event(&format!("e{i}"), None, &person(i)))
        .collect();
    s.write(EventBatch::Events(evs)).await.unwrap();
    let mut src = source(&ctx, None).await;
    let first = src
        .next(Budget {
            max_rows: 2,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events(&first).len(), 2);
    // A restart resumes after the last row: no gap, no repeat.
    let mut src = source(&ctx, Some(first.position.clone())).await;
    let rest = drain(&mut *src).await;
    let ids: Vec<_> = rest
        .iter()
        .map(|e| data(e)["n"]["properties"]["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!("p3"), json!("p4"), json!("p5")]);
    // An update moves the node past the cursor: it is read again, new id.
    let mut p2 = person(2);
    p2["updatedAt"] = json!(99);
    s.write(EventBatch::Events(vec![event("u", None, &p2)]))
        .await
        .unwrap();
    let mut src = source(&ctx, Some(first.position)).await;
    let again = drain(&mut *src).await;
    assert!(again.iter().any(|e| e.id.ends_with("@99")));
    let dels: Vec<_> = (1..=5)
        .map(|i| {
            let mut e = event(&format!("d{i}"), None, &person(i));
            e.extensions.insert("loamsop".into(), "d".into());
            e
        })
        .collect();
    s.write(EventBatch::Events(dels)).await.unwrap();
}
