//! Qdrant (CN2 Task 15): an upsert sink with UUID v5 point ids and a scroll
//! source. The request-shape tests need no server; the suites and the two
//! plan tests run against real Qdrant (`connectors-p2`) and skip without
//! `LOAMS_CONNECTORS_P2_STACK`.

mod common;

use common::{Fake, ctx, doc_event, ok, stack_up};
use loams_connector_sdk::{
    Budget, ConnectorFactory, ErrorClass, EventBatch, SinkHandle, StaticSecrets,
};
use loams_connector_search::conformance::SearchHarness;
use loams_connector_search::qdrant::{Qdrant, point_id};
use loams_flow_conformance::{Suite, run};
use serde_json::{Value, json};

fn sink_cfg(url: &str) -> Value {
    json!({
        "url": url, "collection": "items",
        "key": "/payload/id", "vector": "/vector", "payload": "/payload",
        "api_key": "qdrant-key"
    })
}

fn secrets() -> StaticSecrets {
    StaticSecrets::new().with("qdrant-key", "loams-test-key")
}

/// UUID v5 is deterministic and version 5, and the derivation matches the
/// RFC 9562 algorithm (checked against a published vector).
#[test]
fn qdrant_point_ids_are_uuid_v5_of_key() {
    let a = point_id("doc-1");
    assert_eq!(a, point_id("doc-1"));
    assert_ne!(a, point_id("doc-2"));
    assert_eq!(a.get_version_num(), 5);
    // RFC 9562 / Python's uuid.uuid5(NAMESPACE_DNS, "python.org").
    assert_eq!(
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"python.org").to_string(),
        "886313e1-3b8a-5372-9b90-0c9aee199e5d"
    );
    assert_eq!(
        a,
        uuid::Uuid::new_v5(&loams_connector_search::qdrant::POINT_NAMESPACE, b"doc-1")
    );
}

#[tokio::test]
async fn qdrant_sink_request_shapes() {
    let fake =
        Fake::start(|_, _| ok(json!({"status": "ok", "result": {"status": "completed"}}))).await;
    let factory = Qdrant::new()
        .sink(&ctx(sink_cfg(&fake.url()), secrets()))
        .expect("factory")
        .expect("sink");
    let SinkHandle::Plain(mut sink) = factory.create().expect("create") else {
        panic!("plain");
    };
    let up = |k: &str| {
        doc_event(
            &format!("ce-{k}"),
            json!({"vector": [0.1, 0.2], "payload": {"id": k}}),
        )
    };
    let del = doc_event("ce-z", json!({"payload": {"id": "z"}})).with_extension("loamsop", "d");
    let ack = sink
        .write(EventBatch::Events(vec![up("a"), up("b"), del, up("c")]))
        .await
        .expect("written");
    assert_eq!(ack.rows, 4);
    let reqs = fake.requests();
    let paths: Vec<(String, String)> = reqs
        .iter()
        .map(|r| (r.method.clone(), r.target.clone()))
        .collect();
    assert_eq!(
        paths,
        [
            ("PUT".into(), "/collections/items/points?wait=true".into()),
            (
                "POST".into(),
                "/collections/items/points/delete?wait=true".into()
            ),
            ("PUT".into(), "/collections/items/points?wait=true".into()),
        ],
        "per-key order kept: consecutive upserts batch, a delete splits them"
    );
    let first = reqs[0].json();
    assert_eq!(first["points"][0]["id"], point_id("a").to_string());
    assert_eq!(first["points"][0]["vector"], json!([0.1, 0.2]));
    assert_eq!(first["points"][0]["payload"], json!({"id": "a"}));
    assert_eq!(
        reqs[1].json(),
        json!({"points": [point_id("z").to_string()]})
    );
    assert_eq!(
        reqs[0].headers.get("api-key").map(String::as_str),
        Some("loams-test-key")
    );
}

/// An event with no vector cannot be upserted: `Poison`, never sent.
#[tokio::test]
async fn qdrant_event_without_vector_is_poison() {
    let fake = Fake::start(|_, _| ok(json!({"status": "ok"}))).await;
    let factory = Qdrant::new()
        .sink(&ctx(sink_cfg(&fake.url()), secrets()))
        .expect("factory")
        .expect("sink");
    let SinkHandle::Plain(mut sink) = factory.create().expect("create") else {
        panic!("plain");
    };
    let err = sink
        .write(EventBatch::Events(vec![doc_event(
            "e",
            json!({"payload": {"id": "a"}}),
        )]))
        .await
        .expect_err("no vector");
    assert_eq!(err.class, ErrorClass::Poison);
    assert!(fake.requests().is_empty());
}

/// The scroll source pages by `next_page_offset`, trims a page to the
/// budget, and resumes from the saved offset.
#[tokio::test]
async fn qdrant_scroll_request_shapes_and_resume() {
    let ids: Vec<String> = (0..5)
        .map(|i| point_id(&format!("p{i}")).to_string())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    let all = sorted.clone();
    let fake = Fake::start(move |req, _| {
        let body = req.json();
        let limit = body["limit"].as_u64().unwrap_or(10) as usize;
        let start = body["offset"]
            .as_str()
            .and_then(|o| all.iter().position(|x| x == o))
            .unwrap_or(0);
        let page: Vec<Value> = all[start..]
            .iter()
            .take(limit)
            .map(|id| json!({"id": id, "payload": {"k": id}, "vector": [1.0, 0.0]}))
            .collect();
        let next = all.get(start + limit).map_or(Value::Null, |n| json!(n));
        ok(json!({"status": "ok", "result": {"points": page, "next_page_offset": next}}))
    })
    .await;
    let cfg = json!({"url": fake.url(), "collection": "items", "source": {"page_size": 2}});
    let (mut en, sources) = Qdrant::new()
        .source(&ctx(cfg, StaticSecrets::new()))
        .expect("factory")
        .expect("source");
    let splits = en.discover(&[]).await.expect("discover");
    assert_eq!(splits[0].id.as_str(), "items");
    let mut src = sources.create(&splits[0]).expect("source");
    src.open(&splits[0], None).await.expect("open");
    // A one-row budget trims the two-point page; the position is the first
    // point not taken.
    let b1 = src
        .next(Budget {
            max_rows: 1,
            max_bytes: 1 << 20,
        })
        .await
        .expect("next")
        .expect("batch");
    let EventBatch::Events(evs) = &b1.events else {
        panic!("events")
    };
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].subject.as_deref(), Some(sorted[0].as_str()));
    assert!(
        evs[0].id.starts_with(&format!("items/{}@", sorted[0])),
        "{}",
        evs[0].id
    );
    assert_eq!(evs[0].ty, "io.loams.dev.flow.qdrant.document.v1");
    let first_body = fake.requests()[0].json();
    assert_eq!(first_body["with_payload"], true);
    assert_eq!(first_body["with_vector"], true);

    let mut again = sources.create(&splits[0]).expect("source");
    again
        .open(&splits[0], Some(b1.position.clone()))
        .await
        .expect("reopen");
    let mut seen = Vec::new();
    while let Some(b) = again
        .next(Budget {
            max_rows: 10,
            max_bytes: 1 << 20,
        })
        .await
        .expect("next")
    {
        let EventBatch::Events(evs) = b.events else {
            panic!("events")
        };
        seen.extend(evs.into_iter().filter_map(|e| e.subject));
    }
    assert_eq!(
        seen,
        sorted[1..].to_vec(),
        "resumed at the first point not taken"
    );
}

/// Plan test, against real Qdrant: writing the same events twice leaves one
/// point per key, at its UUID v5 id.
#[tokio::test]
async fn qdrant_upsert_idempotent_uuid_v5() {
    if !stack_up(
        "LOAMS_CONNECTORS_P2_STACK",
        "qdrant_upsert_idempotent_uuid_v5",
    ) {
        return;
    }
    SearchHarness::qdrant()
        .check_upsert_idempotent(20)
        .await
        .expect("idempotent upserts");
}

/// Plan test, against real Qdrant: a scroll source stopped part-way resumes
/// from its offset with nothing lost or repeated.
#[tokio::test]
async fn qdrant_scroll_resumes() {
    if !stack_up("LOAMS_CONNECTORS_P2_STACK", "qdrant_scroll_resumes") {
        return;
    }
    SearchHarness::qdrant()
        .check_scroll_resume(25, 4)
        .await
        .expect("resumes without gap or repeat");
}

#[tokio::test]
async fn qdrant_suite_against_qdrant() {
    let registry = loams_flow::Registry::load(
        &loams_flow::manifest::repository_root().join("connectors/registry"),
    )
    .expect("registry");
    let spec = registry.get("qdrant").expect("manifest");
    let h = SearchHarness::qdrant();
    let suites = [
        Suite::Roundtrip,
        Suite::KillRestart,
        Suite::DupCheck,
        Suite::Auth,
    ];
    let report = run(spec, None, &h, &suites).await;
    eprintln!("{report}");
    if !stack_up("LOAMS_CONNECTORS_P2_STACK", "qdrant_suite_against_qdrant") {
        assert!(!report.green(), "a skipped suite is never green");
        return;
    }
    assert!(report.proves(&suites), "{report}");
}
