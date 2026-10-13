//! Elasticsearch ★ (CN2 Task 15, OR-8): the `_bulk` sink and the
//! point-in-time source.
//!
//! The request-shape tests run against a scripted fake on loopback and need
//! no server. The suites and `es_source_pit_search_after_resumes` run against
//! real Elasticsearch (`docker.elastic.co/elasticsearch/elasticsearch`, a
//! `ci-service`, OR-3) in the `connectors-ci-only` stack, and skip without
//! `LOAMS_CONNECTORS_CI_ONLY_STACK`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{Fake, ctx, doc_event, ok, stack_up};
use loams_connector_sdk::{
    Budget, ConnectorFactory, ErrorClass, EventBatch, SinkHandle, Split, StaticSecrets,
};
use loams_connector_search::conformance::SearchHarness;
use loams_connector_search::elasticsearch::Elasticsearch;
use loams_flow_conformance::{Suite, run, services::SystemsCatalog};
use serde_json::{Value, json};

fn sink_config(url: &str) -> Value {
    json!({
        "url": url,
        "index": "docs",
        "key": "/id",
        "bulk": { "max_docs": 100, "retry_backoff_ms": 1 }
    })
}

async fn plain_sink(cfg: Value) -> Box<dyn loams_connector_sdk::Sink> {
    let factory = Elasticsearch::new()
        .sink(&ctx(cfg, StaticSecrets::new()))
        .expect("sink factory")
        .expect("a sink");
    match factory.create().expect("create") {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("the bulk sink is not transactional"),
    }
}

fn item(op: &str, id: &str, status: u16) -> Value {
    if status < 300 {
        json!({ op: { "_index": "docs", "_id": id, "status": status, "result": "created" } })
    } else {
        json!({ op: { "_index": "docs", "_id": id, "status": status,
                      "error": { "type": "es_rejected_execution_exception", "reason": "queue full" } } })
    }
}

/// Plan test: a partial failure retries only the items that failed.
#[tokio::test]
async fn es_bulk_partial_failure_retries_only_failed() {
    let fake = Fake::start(|req, i| {
        let ids: Vec<String> = req
            .ndjson()
            .iter()
            .filter_map(|l| {
                l.get("index")
                    .and_then(|a| a["_id"].as_str())
                    .map(str::to_owned)
            })
            .collect();
        // First request: "b" is rejected (429) and "c" hits a 503; "a" lands.
        let items: Vec<Value> = ids
            .iter()
            .map(|id| match (i, id.as_str()) {
                (0, "b") => item("index", id, 429),
                (0, "c") => item("index", id, 503),
                _ => item("index", id, 201),
            })
            .collect();
        ok(json!({ "took": 1, "errors": i == 0, "items": items }))
    })
    .await;
    let mut sink = plain_sink(sink_config(&fake.url())).await;
    let batch = EventBatch::Events(vec![
        doc_event("e1", json!({"id": "a", "n": 1})),
        doc_event("e2", json!({"id": "b", "n": 2})),
        doc_event("e3", json!({"id": "c", "n": 3})),
    ]);
    let ack = sink.write(batch).await.expect("all three land in the end");
    assert_eq!(ack.rows, 3);

    let bulks = fake.to("/_bulk");
    assert_eq!(bulks.len(), 2, "one retry for the two failed items");
    let first: Vec<Value> = bulks[0].ndjson();
    assert_eq!(first.len(), 6, "three actions, three documents");
    let retried = bulks[1].ndjson();
    let retried_ids: Vec<&str> = retried
        .iter()
        .filter_map(|l| l.get("index").and_then(|a| a["_id"].as_str()))
        .collect();
    assert_eq!(retried_ids, ["b", "c"], "only the failed items are resent");
    assert_eq!(retried[1]["n"], 2);
    assert_eq!(
        bulks[0].headers.get("content-type").map(String::as_str),
        Some("application/x-ndjson")
    );
}

/// A 429 for the whole request halves the batch before it is retried.
#[tokio::test]
async fn es_bulk_shrinks_batch_on_429() {
    let rejected = Arc::new(AtomicUsize::new(0));
    let seen = rejected.clone();
    let fake = Fake::start(move |req, _| {
        let lines = req.ndjson();
        let n = lines.len() / 2;
        if n > 2 {
            seen.fetch_add(1, Ordering::SeqCst);
            return (
                429,
                vec![("retry-after".into(), "0".into())],
                json!({"status": 429}).to_string(),
            );
        }
        let items: Vec<Value> = lines
            .iter()
            .filter_map(|l| l.get("index").and_then(|a| a["_id"].as_str()))
            .map(|id| item("index", id, 201))
            .collect();
        ok(json!({ "errors": false, "items": items }))
    })
    .await;
    let mut cfg = sink_config(&fake.url());
    cfg["bulk"]["max_docs"] = json!(8);
    let mut sink = plain_sink(cfg).await;
    let events = (0..8)
        .map(|i| doc_event(&format!("e{i}"), json!({"id": format!("d{i}")})))
        .collect();
    let ack = sink
        .write(EventBatch::Events(events))
        .await
        .expect("written");
    assert_eq!(ack.rows, 8);
    assert!(rejected.load(Ordering::SeqCst) >= 2, "8 → 4 → 2");
    let sent: usize = fake
        .to("/_bulk")
        .iter()
        .filter(|r| r.ndjson().len() <= 4)
        .map(|r| r.ndjson().len() / 2)
        .sum();
    assert_eq!(sent, 8, "every document is sent once the batch fits");
}

/// A document Elasticsearch refuses (a mapping conflict) is `Poison`, so the
/// pipeline isolates it event by event into the DLQ; the others land.
#[tokio::test]
async fn es_bulk_refused_item_is_poison() {
    let fake = Fake::start(|req, _| {
        let items: Vec<Value> = req
            .ndjson()
            .iter()
            .filter_map(|l| l.get("index").and_then(|a| a["_id"].as_str()))
            .map(|id| {
                if id == "bad" {
                    json!({"index": {"_id": id, "status": 400,
                        "error": {"type": "document_parsing_exception", "reason": "failed to parse field [n]"}}})
                } else {
                    item("index", id, 201)
                }
            })
            .collect();
        ok(json!({ "errors": true, "items": items }))
    })
    .await;
    let mut sink = plain_sink(sink_config(&fake.url())).await;
    let err = sink
        .write(EventBatch::Events(vec![
            doc_event("e1", json!({"id": "good"})),
            doc_event("e2", json!({"id": "bad", "n": "x"})),
        ]))
        .await
        .expect_err("the refused document fails the batch");
    assert_eq!(err.class, ErrorClass::Poison);
    assert!(
        err.message.contains("document_parsing_exception"),
        "{}",
        err.message
    );
    assert_eq!(fake.to("/_bulk").len(), 1, "a 400 item is never retried");
}

/// `loamsop = d` becomes a delete by `_id`; a 404 on delete is success.
#[tokio::test]
async fn es_bulk_deletes_on_loamsop_d() {
    let fake = Fake::start(|req, _| {
        let items: Vec<Value> = req
            .ndjson()
            .iter()
            .filter_map(|l| {
                l.get("delete")
                    .map(|a| json!({"delete": {"_id": a["_id"], "status": 404, "result": "not_found"}}))
                    .or_else(|| l.get("index").map(|a| json!({"index": {"_id": a["_id"], "status": 200}})))
            })
            .collect();
        ok(json!({ "errors": false, "items": items }))
    })
    .await;
    let mut sink = plain_sink(sink_config(&fake.url())).await;
    let delete = doc_event("e9", json!({"id": "gone"})).with_extension("loamsop", "d");
    sink.write(EventBatch::Events(vec![
        doc_event("e8", json!({"id": "kept"})),
        delete,
    ]))
    .await
    .expect("written");
    let lines = fake.to("/_bulk")[0].ndjson();
    assert_eq!(lines.len(), 3, "index + doc, then a delete with no body");
    assert_eq!(lines[0]["index"]["_id"], "kept");
    assert_eq!(lines[0]["index"]["_index"], "docs");
    assert_eq!(lines[2]["delete"]["_id"], "gone");
}

/// Without `key`, `_id` is the event's `ce_id` (effectively once on replay).
#[tokio::test]
async fn es_bulk_id_defaults_to_ce_id() {
    let fake = Fake::start(|_, _| {
        ok(json!({"errors": false, "items": [{"index": {"_id": "x", "status": 201}}]}))
    })
    .await;
    let mut cfg = sink_config(&fake.url());
    cfg.as_object_mut().expect("object").remove("key");
    let mut sink = plain_sink(cfg).await;
    sink.write(EventBatch::Events(vec![doc_event(
        "ce-42",
        json!({"n": 1}),
    )]))
    .await
    .expect("written");
    assert_eq!(fake.to("/_bulk")[0].ndjson()[0]["index"]["_id"], "ce-42");
}

/// Basic credentials are resolved from the secret store by name and sent as
/// a sensitive header; the secret never reaches an error message.
#[tokio::test]
async fn es_basic_auth_resolved_by_reference() {
    let fake = Fake::start(|_, _| {
        (
            401,
            Vec::new(),
            json!({"error": "unauthorized"}).to_string(),
        )
    })
    .await;
    let mut cfg = sink_config(&fake.url());
    cfg["basic"] = json!({"username": "loams", "password": "es-password"});
    let factory = Elasticsearch::new()
        .sink(&ctx(
            cfg,
            StaticSecrets::new().with("es-password", "s3cr3t-value"),
        ))
        .expect("factory")
        .expect("sink");
    let SinkHandle::Plain(mut sink) = factory.create().expect("create") else {
        panic!("plain");
    };
    let err = sink
        .write(EventBatch::Events(vec![doc_event(
            "e1",
            json!({"id": "a"}),
        )]))
        .await
        .expect_err("401");
    assert_eq!(err.class, ErrorClass::Auth);
    assert!(!err.message.contains("s3cr3t-value"));
    let auth = fake.requests()[0]
        .headers
        .get("authorization")
        .cloned()
        .expect("header");
    assert!(auth.starts_with("Basic "), "{auth}");
}

/// The source's requests against a fake Elasticsearch: open a PIT on the
/// index, sort by the cursor then `_shard_doc`, page with `search_after`,
/// and resume inside the same PIT from a saved position.
#[tokio::test]
async fn es_source_pit_request_shapes_and_resume() {
    let fake = Fake::start(|req, _| {
        if req.path() == "/docs/_pit" {
            return ok(json!({"id": "pit-1"}));
        }
        if req.method == "DELETE" {
            return ok(json!({"succeeded": true, "num_freed": 1}));
        }
        let body = req.json();
        let after = body["search_after"][0].as_i64().unwrap_or(-1);
        let size = body["size"].as_u64().unwrap_or(10) as i64;
        let hits: Vec<Value> = (after + 1..5)
            .take(size as usize)
            .map(|n| {
                json!({"_index": "docs", "_id": format!("d{n}"), "_seq_no": n, "_primary_term": 1,
                       "_source": {"id": format!("d{n}"), "seq": n}, "sort": [n, n * 10]})
            })
            .collect();
        ok(json!({"pit_id": "pit-1", "hits": {"hits": hits}}))
    })
    .await;
    let cfg = json!({
        "url": fake.url(), "index": "docs",
        "source": { "cursor_field": "seq", "page_size": 2 }
    });
    let (mut enumerator, sources) = Elasticsearch::new()
        .source(&ctx(cfg, StaticSecrets::new()))
        .expect("factory")
        .expect("a source");
    let splits = enumerator.discover(&[]).await.expect("discover");
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].id.as_str(), "docs");

    let mut src = sources.create(&splits[0]).expect("source");
    src.open(&splits[0], None).await.expect("open");
    let budget = Budget {
        max_rows: 100,
        max_bytes: 1 << 20,
    };
    let first = src.next(budget).await.expect("next").expect("a batch");
    let EventBatch::Events(evs) = &first.events else {
        panic!("events")
    };
    assert_eq!(evs.len(), 2);
    assert_eq!(
        evs[0].id, "docs/d0@0.1",
        "stable id: index, _id, seq_no, primary term"
    );
    assert_eq!(evs[0].subject.as_deref(), Some("d0"));
    assert_eq!(evs[0].ty, "io.loams.dev.flow.elasticsearch.document.v1");
    assert!(
        !first.position.display.contains("pit-1"),
        "the PIT id stays opaque"
    );
    src.close().await.expect("close");

    let searches = fake.to("/_search");
    let body = searches[0].json();
    assert_eq!(body["pit"]["id"], "pit-1");
    assert_eq!(
        body["sort"],
        json!([{"seq": {"order": "asc"}}, {"_shard_doc": {"order": "asc"}}])
    );
    assert_eq!(body["seq_no_primary_term"], true);
    assert!(body.get("search_after").is_none());

    // A new source from the saved position continues inside the PIT.
    let mut again = sources.create(&splits[0]).expect("source");
    again
        .open(&splits[0], Some(first.position.clone()))
        .await
        .expect("reopen");
    let second = again.next(budget).await.expect("next").expect("a batch");
    let EventBatch::Events(evs) = &second.events else {
        panic!("events")
    };
    assert_eq!(evs[0].subject.as_deref(), Some("d2"), "no gap, no repeat");
    let last = fake.to("/_search").pop().expect("search").json();
    assert_eq!(last["search_after"], json!([1, 10]));
    assert_eq!(fake.to("/docs/_pit").len(), 1, "the live PIT is reused");
}

/// An expired PIT (404) starts a new pass from the last cursor value
/// (`gte`, so a tie is re-read and deduplicated by its stable id).
#[tokio::test]
async fn es_source_expired_pit_restarts_from_cursor() {
    let fake = Fake::start(|req, _| {
        if req.path() == "/docs/_pit" {
            return ok(json!({"id": "pit-2"}));
        }
        if req.method == "DELETE" {
            return ok(json!({"succeeded": true}));
        }
        let body = req.json();
        if body["pit"]["id"] == "pit-gone" {
            return (404, Vec::new(), json!({"error": {"type": "search_context_missing_exception"}}).to_string());
        }
        ok(json!({"pit_id": "pit-2", "hits": {"hits": [
            {"_index": "docs", "_id": "d3", "_seq_no": 3, "_primary_term": 1, "_source": {"seq": 3}, "sort": [3, 1]}
        ]}}))
    })
    .await;
    let cfg = json!({"url": fake.url(), "index": "docs", "source": {"cursor_field": "seq"}});
    let (_, sources) = Elasticsearch::new()
        .source(&ctx(cfg, StaticSecrets::new()))
        .expect("factory")
        .expect("source");
    let split = Split::new("docs", json!({"index": "docs"}));
    let pos = loams_connector_search::pit::PitPosition {
        pit: Some("pit-gone".into()),
        search_after: Some(vec![json!(3), json!(99)]),
        since: None,
        last_cursor: Some(json!(3)),
        pass_done: false,
    }
    .to_position();
    let mut src = sources.create(&split).expect("source");
    src.open(&split, Some(pos)).await.expect("open");
    let batch = src
        .next(Budget {
            max_rows: 10,
            max_bytes: 1 << 20,
        })
        .await
        .expect("next")
        .expect("batch");
    assert_eq!(batch.events.rows(), 1);
    let last = fake.to("/_search").pop().expect("search").json();
    assert_eq!(last["pit"]["id"], "pit-2");
    assert_eq!(
        last["query"]["bool"]["filter"][1],
        json!({"range": {"seq": {"gte": 3}}}),
        "{last}"
    );
}

/// Plan test, against real Elasticsearch: a source stopped mid-index
/// resumes from its position with nothing lost or repeated.
#[tokio::test]
async fn es_source_pit_search_after_resumes() {
    if !stack_up(
        "LOAMS_CONNECTORS_CI_ONLY_STACK",
        "es_source_pit_search_after_resumes",
    ) {
        return;
    }
    let h = SearchHarness::elasticsearch();
    h.check_pit_resume(25, 4)
        .await
        .expect("resumes without gap or repeat");
}

/// The kit's suites against real Elasticsearch: documents seeded into a
/// source index travel source → Fabric → `_bulk` sink → target index.
#[tokio::test]
async fn elasticsearch_suite_against_elasticsearch() {
    let registry = loams_flow::Registry::load(
        &loams_flow::manifest::repository_root().join("connectors/registry"),
    )
    .expect("registry");
    let spec = registry.get("elasticsearch").expect("manifest");
    let h = SearchHarness::elasticsearch();
    let report = run(
        spec,
        None,
        &h,
        &[
            Suite::Roundtrip,
            Suite::KillRestart,
            Suite::DupCheck,
            Suite::Auth,
        ],
    )
    .await;
    eprintln!("{report}");
    if !stack_up(
        "LOAMS_CONNECTORS_CI_ONLY_STACK",
        "elasticsearch_suite_against_elasticsearch",
    ) {
        assert!(!report.green(), "a skipped suite is never green");
        return;
    }
    assert!(
        report.proves(&[
            Suite::Roundtrip,
            Suite::KillRestart,
            Suite::DupCheck,
            Suite::Auth
        ]),
        "{report}"
    );
}

/// Plan test (OR-2): Loams' own collections ES and Qdrant APIs appear for
/// this family only as extra targets, and a harness that names one as its
/// service is refused before any suite runs.
#[test]
fn loams_endpoints_are_extra_targets_only() {
    let root = loams_flow::manifest::repository_root();
    let catalog = SystemsCatalog::load(&root.join(SystemsCatalog::PATH)).expect("systems.toml");
    assert!(catalog.problems().is_empty(), "{:?}", catalog.problems());
    for id in ["elasticsearch", "opensearch", "qdrant"] {
        let e = catalog
            .get(id)
            .unwrap_or_else(|| panic!("{id} has no systems.toml entry"));
        assert_eq!(e.family, "search", "{id}");
        assert!(
            e.services.iter().all(|s| !catalog.is_loams_module(s)),
            "{id}: a Loams module among the real services"
        );
        assert!(
            e.extra_targets.iter().any(|s| catalog.is_loams_module(s)),
            "{id}: the Loams endpoint is an extra target"
        );
        assert_eq!(
            e.harness.as_deref(),
            Some("loams_connector_search::conformance::SearchHarness"),
            "{id}"
        );
    }
    // The harnesses themselves name only the real systems.
    for (id, h) in [
        ("elasticsearch", SearchHarness::elasticsearch()),
        ("opensearch", SearchHarness::opensearch()),
        ("qdrant", SearchHarness::qdrant()),
    ] {
        let services = loams_flow_conformance::ConnectorHarness::services(&h);
        loams_flow_conformance::check_services(id, None, services, &catalog)
            .unwrap_or_else(|p| panic!("{id}: {p:?}"));
    }
    // A harness standing a Loams endpoint in for Elasticsearch is refused.
    let stand_in = [loams_flow_conformance::ServiceKind::Container {
        compose_service: "loams-collections",
        system: "Loams collections (Elasticsearch API)",
    }];
    let problems =
        loams_flow_conformance::check_services("elasticsearch", None, &stand_in, &catalog)
            .expect_err("a Loams module never counts as the real system");
    assert!(
        problems
            .iter()
            .any(|p| p.contains("extra targets never count")),
        "{problems:?}"
    );
}
