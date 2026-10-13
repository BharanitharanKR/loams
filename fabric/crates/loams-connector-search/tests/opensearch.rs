//! OpenSearch: a target profile (`connectors/profiles/opensearch.yaml`) on
//! the Elasticsearch code (CN2 Task 15, OR-2). Its suite runs against real
//! OpenSearch 3 (`connectors-p2`); Amazon OpenSearch Service with SigV4 runs
//! nightly. The request-shape tests need no server.

mod common;

use common::{Fake, ctx, doc_event, env_or, ok, stack_up};
use loams_connector_sdk::{
    Budget, ConnectorFactory, EventBatch, ProfileKind, SinkHandle, StaticSecrets,
};
use loams_connector_search::conformance::SearchHarness;
use loams_connector_search::elasticsearch::Elasticsearch;
use loams_flow_conformance::{Suite, run};
use serde_json::{Value, json};

fn profile() -> loams_connector_sdk::Profile {
    let root = loams_flow::manifest::repository_root();
    loams_flow::profile::load_profile(&root.join("connectors/profiles/opensearch.yaml"))
        .expect("the OpenSearch profile loads and validates")
}

#[test]
fn opensearch_profile_is_a_search_target() {
    let p = profile();
    assert_eq!(p.id, "opensearch");
    assert_eq!(p.kind, ProfileKind::Target);
    assert_eq!(p.module, "search");
    assert_eq!(p.system.name, "OpenSearch");
    assert_eq!(p.defaults["api"], "opensearch");
    let f = Elasticsearch::for_profile(&p).expect("a search-family target");
    assert_eq!(f.id(), "opensearch");
}

/// The profile's defaults select OpenSearch's point-in-time API: `POST
/// /<index>/_search/point_in_time`, `pit_id` in the answer, `DELETE
/// /_search/point_in_time` with a `pit_id` list, and `_id` as tiebreaker.
#[tokio::test]
async fn opensearch_pit_api_shapes() {
    let fake = Fake::start(|req, _| {
        if req.path() == "/docs/_search/point_in_time" {
            return ok(json!({"pit_id": "os-pit", "creation_time": 1}));
        }
        if req.method == "DELETE" {
            return ok(json!({"pits": [{"successful": true, "pit_id": "os-pit"}]}));
        }
        let body = req.json();
        let hits: Vec<Value> = if body.get("search_after").is_some() {
            Vec::new()
        } else {
            vec![
                json!({"_index": "docs", "_id": "a", "_seq_no": 0, "_primary_term": 1,
                        "_source": {"id": "a"}, "sort": ["a"]}),
            ]
        };
        ok(json!({"pit_id": "os-pit", "hits": {"hits": hits}}))
    })
    .await;
    let p = profile();
    let mut c = ctx(
        json!({"url": fake.url(), "index": "docs"}),
        StaticSecrets::new(),
    );
    c.profile = Some(p.clone());
    let (_, sources) = Elasticsearch::for_profile(&p)
        .expect("factory")
        .source(&c)
        .expect("source factory")
        .expect("source");
    let split = loams_connector_sdk::Split::new("docs", json!({"index": "docs"}));
    let mut src = sources.create(&split).expect("source");
    src.open(&split, None).await.expect("open");
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };
    let batch = src.next(budget).await.expect("next").expect("batch");
    let EventBatch::Events(evs) = &batch.events else {
        panic!("events")
    };
    assert_eq!(evs[0].ty, "io.loams.dev.flow.opensearch.document.v1");
    assert!(
        src.next(budget).await.expect("next").is_none(),
        "bounded: one pass"
    );
    src.close().await.expect("close");

    let opened = fake.to("/docs/_search/point_in_time");
    assert_eq!(opened.len(), 1);
    assert!(
        opened[0].target.contains("keep_alive="),
        "{}",
        opened[0].target
    );
    let search = &fake.to("/_search")[0];
    assert_eq!(search.path(), "/_search");
    assert_eq!(search.json()["pit"]["id"], "os-pit");
    assert_eq!(search.json()["sort"], json!([{"_id": {"order": "asc"}}]));
    let deleted: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|r| r.method == "DELETE")
        .collect();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].path(), "/_search/point_in_time");
    assert_eq!(deleted[0].json(), json!({"pit_id": ["os-pit"]}));
}

/// `auth.aws` signs every request with SigV4 (service `es`), with the
/// payload hash and the date as signed headers; the secret key is never
/// sent.
#[tokio::test]
async fn opensearch_sigv4_signs_requests() {
    let fake = Fake::start(|req, _| {
        let items: Vec<Value> = req
            .ndjson()
            .iter()
            .filter_map(|l| {
                l.get("index")
                    .map(|a| json!({"index": {"_id": a["_id"], "status": 201}}))
            })
            .collect();
        ok(json!({"errors": false, "items": items}))
    })
    .await;
    let cfg = json!({
        "url": fake.url(), "index": "docs", "key": "/id",
        "aws": {"region": "eu-west-1", "access_key_id": "aws-id", "secret_access_key": "aws-secret"}
    });
    let secrets = StaticSecrets::new()
        .with("aws-id", "AKIDEXAMPLE")
        .with("aws-secret", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");
    let factory = Elasticsearch::opensearch()
        .sink(&ctx(cfg, secrets))
        .expect("factory")
        .expect("sink");
    let SinkHandle::Plain(mut sink) = factory.create().expect("create") else {
        panic!("plain");
    };
    sink.write(EventBatch::Events(vec![doc_event(
        "e1",
        json!({"id": "a"}),
    )]))
    .await
    .expect("written");
    let req = &fake.to("/_bulk")[0];
    let auth = req.headers.get("authorization").expect("signed");
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"),
        "{auth}"
    );
    assert!(auth.contains("/eu-west-1/es/aws4_request"), "{auth}");
    assert!(
        auth.contains("SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date"),
        "{auth}"
    );
    assert!(req.headers.contains_key("x-amz-date"));
    let body_hash = req
        .headers
        .get("x-amz-content-sha256")
        .expect("payload hash");
    assert_eq!(body_hash.len(), 64);
    assert!(!format!("{:?}", req.headers).contains("wJalrXUtnFEMI"));
}

/// Plan test: the kit's suites against real OpenSearch 3, with the target
/// profile (its suite runs against OpenSearch itself, never Elasticsearch).
#[tokio::test]
async fn opensearch_suite_against_opensearch() {
    let registry = loams_flow::Registry::load(
        &loams_flow::manifest::repository_root().join("connectors/registry"),
    )
    .expect("registry");
    let spec = registry.get("opensearch").expect("manifest");
    let p = profile();
    let h = SearchHarness::opensearch();
    let suites = [Suite::Roundtrip, Suite::KillRestart, Suite::DupCheck];
    let report = run(spec, Some(&p), &h, &suites).await;
    eprintln!("{report}");
    if !stack_up(
        "LOAMS_CONNECTORS_P2_STACK",
        "opensearch_suite_against_opensearch",
    ) {
        assert!(!report.green(), "a skipped suite is never green");
        return;
    }
    assert!(report.proves(&suites), "{report}");
    h.check_pit_resume(25, 4)
        .await
        .expect("the PIT source resumes on OpenSearch");
}

/// Plan test: Amazon OpenSearch Service with SigV4, in the scheduled
/// `connectors-nightly` job only (repository secrets, OR-3).
#[tokio::test]
async fn opensearch_sigv4_nightly() {
    let vars = [
        "LOAMS_AMAZON_OPENSEARCH_URL",
        "LOAMS_AWS_ACCESS_KEY_ID",
        "LOAMS_AWS_SECRET_ACCESS_KEY",
        "LOAMS_AWS_REGION",
    ];
    if let Some(missing) = vars
        .iter()
        .find(|v| std::env::var(v).map_or(true, |x| x.is_empty()))
    {
        eprintln!("opensearch_sigv4_nightly: skipped, {missing} is not set (nightly only)");
        return;
    }
    let h = SearchHarness::amazon_opensearch(
        &env_or("LOAMS_AMAZON_OPENSEARCH_URL", ""),
        &env_or("LOAMS_AWS_REGION", ""),
        &env_or("LOAMS_AWS_ACCESS_KEY_ID", ""),
        &env_or("LOAMS_AWS_SECRET_ACCESS_KEY", ""),
    );
    h.check_pit_resume(10, 3)
        .await
        .expect("signed PIT reads resume");
    h.check_sink_roundtrip(10)
        .await
        .expect("signed bulk writes land");
}
