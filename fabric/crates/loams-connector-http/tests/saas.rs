//! CN2 plan Task 20: the SaaS and service rows as HTTP-API profiles.
//!
//! Every profile under `connectors/profiles/` that Task 20 writes is loaded
//! through `loams-flow`'s loader (schema and rules) and run by the profile
//! engine against the fixtures under `conformance/connectors/fixtures/<id>/`,
//! replayed by the conformance kit's `FixtureProxy` (R6.9). Those fixtures are
//! **synthetic** (R20.1): built by hand from each vendor's published request
//! and response shapes, marked `synthetic: true` with the documentation they
//! follow, and never counted toward `preview`; the `connectors-nightly` job
//! records the real ones. Each fixture carries the `scenario` it plays: the
//! resource, the instance config (`{proxy}` is the replay's URL), the secrets
//! (by field; the test names them), and the items written or expected.
//!
//! The rows with an open image (stripe-mock, OpenBao, Keycloak) also run
//! against it when `LOAMS_CONNECTORS_P2_STACK` is set (the `connectors-p2`
//! shard); without it that part is skipped and says so.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ::http::{HeaderMap, HeaderName, HeaderValue};
use loams_connector_http::profile::HttpApiFactory;
use loams_connector_http::route::ImportRoute;
use loams_connector_http::webhook::{self, WebhookError, WebhookRequest};
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::*;
use loams_flow_conformance::fixture::{Body, Fixture, FixtureProxy, RecordedRequest, ScrubRules};
use serde_json::{Value, json};

/// The twenty rows of Task 20.
const SAAS: &[&str] = &[
    "kestra-companion",
    "segment",
    "sendgrid",
    "posthog",
    "hubspot",
    "shopify",
    "gitlab",
    "amazon-ses",
    "twilio-sms",
    "salesforce",
    "zendesk",
    "servicenow",
    "stripe",
    "github",
    "jira",
    "slack",
    "google-workspace",
    "keycloak",
    "vault",
    "splunk",
];

// ----------------------------------------------------------------- helpers --

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap()
}

fn profile(id: &str) -> Profile {
    loams_flow::profile::load_profile(&root().join(format!("connectors/profiles/{id}.yaml")))
        .unwrap_or_else(|e| panic!("profile {id}: {e}"))
}

fn factory(id: &str) -> HttpApiFactory {
    HttpApiFactory::new(profile(id)).unwrap()
}

fn fixture_path(id: &str, name: &str) -> PathBuf {
    root().join(format!("conformance/connectors/fixtures/{id}/{name}.json"))
}

/// The fixture and the scenario it carries.
fn fixture(id: &str, name: &str) -> (Fixture, Value) {
    let path = fixture_path(id, name);
    let f = Fixture::load(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let raw: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert!(
        f.synthetic && !f.counts_toward_preview() && !f.docs.is_empty(),
        "{id}/{name}: a hand-built fixture is marked synthetic and cites its docs (R20.1)"
    );
    (f, raw["scenario"].clone())
}

fn with_proxy(v: &Value, proxy: &str) -> Value {
    match v {
        Value::String(s) => Value::String(s.replace("{proxy}", proxy)),
        Value::Array(a) => Value::Array(a.iter().map(|x| with_proxy(x, proxy)).collect()),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, x)| (k.clone(), with_proxy(x, proxy)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The instance config and secret store of a scenario: each secret field
/// `f` is named `fx-<f>` in `auth`.
fn instance_of(scenario: &Value, proxy: &str) -> (Value, StaticSecrets) {
    let mut config = with_proxy(&scenario["config"], proxy);
    if let Some(r) = scenario.get("resource") {
        config["resource"] = r.clone();
    }
    let mut secrets = StaticSecrets::new();
    let mut auth = serde_json::Map::new();
    for (field, value) in scenario["secrets"].as_object().into_iter().flatten() {
        let name = format!("fx-{field}");
        secrets = secrets.with(&name, value.as_str().unwrap());
        auth.insert(field.clone(), Value::String(name));
    }
    if config.get("auth").is_none() {
        config["auth"] = Value::Object(auth);
    }
    (config, secrets)
}

fn pipeline_config(rows: usize) -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: rows,
        max_batch_bytes: 1 << 20,
        linger: Duration::from_millis(10),
        max_in_flight_batches: 2,
        memory_budget_bytes: 8 << 20,
        checkpoint_interval: Duration::ZERO,
        retry: RetryPolicy {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(200),
            ..RetryPolicy::default()
        },
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// What a scenario produced.
struct Run {
    events: Vec<CloudEvent>,
    received: Vec<RecordedRequest>,
    rows: u64,
}

/// Run a fixture's source or sink scenario through the profile engine
/// against the replay, and check the replay saw only recorded requests.
async fn run(id: &str, name: &str) -> Run {
    run_with(id, name, None).await
}

async fn run_with(id: &str, name: &str, override_config: Option<Value>) -> Run {
    let (fx, scenario) = fixture(id, name);
    let factory = factory(id);
    let scrubber = Scrubber::new();
    let proxy = FixtureProxy::replay(fx, ScrubRules::default().with_values(scrubber.clone()))
        .await
        .unwrap();
    let (mut config, secrets) = instance_of(&scenario, &proxy.base_url());
    if let Some(Value::Object(o)) = override_config {
        for (k, v) in o {
            config[k] = with_proxy(&v, &proxy.base_url());
        }
    }
    let ctx = ConnectorContext::new(
        InstanceKey::new(7, format!("{id}-{name}")),
        config,
        Arc::new(secrets),
        scrubber.clone(),
    );
    let expect = scenario["expect"].as_u64().unwrap();
    let fabric = MemoryFabric::new();
    let mut rows = 0;
    match scenario["direction"].as_str().unwrap() {
        "source" => {
            let (enumerator, sources) = factory.source(&ctx).unwrap().expect("a source");
            let handle = Pipeline::source(ctx.instance.clone(), pipeline_config(16))
                .enumerator(enumerator)
                .sources(Arc::from(sources))
                .writer(Arc::new(fabric.clone()))
                .checkpoints(Arc::new(MemoryCheckpointStore::new()))
                .dead_letters(Arc::new(fabric.clone()))
                .scrubber(scrubber)
                .epoch(1)
                .start()
                .await
                .unwrap();
            eventually(&format!("{id}/{name}: {expect} items"), || {
                fabric.len() as u64 >= expect
            })
            .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let st = handle.stop().await;
            assert!(st.last_error.is_none(), "{id}/{name}: {:?}", st.last_error);
            assert_eq!(fabric.len() as u64, expect, "{id}/{name}");
        }
        "sink" => {
            for (i, item) in scenario["input"].as_array().unwrap().iter().enumerate() {
                fabric.push(
                    CloudEvent::new(format!("ev-{i}"), "/test", "io.loams.dev.flow.test.item.v1")
                        .with_data("application/json", item.to_string()),
                );
            }
            let sink = factory.sink(&ctx).unwrap().expect("a sink");
            let handle = Pipeline::sink(ctx.instance.clone(), pipeline_config(100))
                .reader(Box::new(fabric.reader("flow-test")))
                .sink(Arc::from(sink))
                .dead_letters(Arc::new(fabric.clone()))
                .scrubber(scrubber)
                .epoch(1)
                .start()
                .await
                .unwrap();
            eventually(&format!("{id}/{name}: {expect} rows written"), || {
                fabric.committed("flow-test") >= expect
            })
            .await;
            let st = handle.stop().await;
            assert!(st.last_error.is_none(), "{id}/{name}: {:?}", st.last_error);
            rows = fabric.committed("flow-test");
        }
        other => panic!("{id}/{name}: not a source or sink scenario: {other}"),
    }
    let received = proxy.received();
    proxy
        .finish()
        .unwrap_or_else(|e| panic!("{id}/{name}: {e}"));
    Run {
        events: fabric.events(),
        received,
        rows,
    }
}

fn data(ev: &CloudEvent) -> Value {
    serde_json::from_slice(&ev.data).unwrap()
}

fn header<'a>(r: &'a RecordedRequest, name: &str) -> Option<&'a str> {
    r.headers.get(name).map(String::as_str)
}

fn json_body(r: &RecordedRequest) -> Value {
    match &r.body {
        Body::Json(v) => v.clone(),
        other => panic!("not a JSON body: {other:?}"),
    }
}

/// A webhook fixture's delivery, verified as the webhooks source would.
struct Hook {
    scheme: String,
    header: String,
    secret: String,
    url: String,
    method: String,
    headers: HeaderMap,
    body: Vec<u8>,
    now: SystemTime,
}

impl Hook {
    fn load(id: &str) -> Self {
        let (fx, sc) = fixture(id, "webhook");
        let req = &fx.exchanges[0].request;
        let mut headers = HeaderMap::new();
        for (k, v) in &req.headers {
            headers.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let body = match &req.body {
            Body::Text(t) => t.as_bytes().to_vec(),
            other => panic!("a webhook fixture keeps its exact body as text: {other:?}"),
        };
        let p = profile(id);
        let spec = p.http.as_ref().unwrap().webhook.as_ref().unwrap();
        assert_eq!(
            sc["scheme"], spec.scheme,
            "{id}: the fixture's scheme is the profile's"
        );
        assert_eq!(
            sc["header"], spec.header,
            "{id}: the fixture's header is the profile's"
        );
        Self {
            scheme: spec.scheme.clone(),
            header: spec.header.clone(),
            secret: sc["secret"].as_str().unwrap().to_string(),
            url: sc["url"].as_str().unwrap().to_string(),
            method: req.method.clone(),
            headers,
            body,
            now: UNIX_EPOCH + Duration::from_secs(sc["now"].as_u64().unwrap()),
        }
    }

    fn verify_with(&self, secret: &[u8], body: &[u8], now: SystemTime) -> Result<(), WebhookError> {
        webhook::verify(
            &self.scheme,
            &self.header,
            secret,
            &WebhookRequest {
                method: &self.method,
                url: &self.url,
                headers: &self.headers,
                body,
            },
            now,
        )
    }

    /// Accepted as delivered; refused with a changed body or another secret.
    fn check(&self, id: &str) {
        self.verify_with(self.secret.as_bytes(), &self.body, self.now)
            .unwrap_or_else(|e| panic!("{id}: {e}"));
        let mut tampered = self.body.clone();
        tampered.push(b' ');
        // A shared token authenticates the sender, not the body (GitLab,
        // Google channels, Kestra): only the signing schemes refuse a change.
        let expected = if self.scheme == "token" {
            Ok(())
        } else {
            Err(WebhookError::Mismatch)
        };
        assert_eq!(
            self.verify_with(self.secret.as_bytes(), &tampered, self.now),
            expected,
            "{id}: a changed body is refused"
        );
        assert!(
            self.verify_with(b"another-secret", &self.body, self.now)
                .is_err(),
            "{id}: another secret is refused"
        );
    }
}

fn stack_up() -> bool {
    std::env::var_os("LOAMS_CONNECTORS_P2_STACK").is_some()
}

fn read_yaml(path: &Path) -> Value {
    serde_norway::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

// ------------------------------------------------------------------- tests --

/// Every Task 20 manifest names its own profile, the profile loads through
/// `loams-flow` (schema and rules), and each direction the manifest declares
/// is served: a source by a `list` or `async_job` resource or by a webhook,
/// a sink by an `upsert` or `create` resource.
#[test]
fn every_saas_manifest_names_its_profile() {
    for id in SAAS {
        let manifest = read_yaml(&root().join(format!("connectors/registry/{id}.yaml")));
        assert_eq!(manifest["runtime"]["profile"], *id, "{id}");
        assert_eq!(
            manifest["runtime"]["ref"], "loams_connector_http::http",
            "{id}"
        );
        let p = profile(id);
        assert_eq!(p.kind, ProfileKind::HttpApi, "{id}");
        let http = p.http.as_ref().unwrap();
        let caps = &manifest["capabilities"];
        if caps["source"].is_object() {
            assert!(
                http.webhook.is_some()
                    || http
                        .resources
                        .values()
                        .any(|r| r.list.is_some() || r.async_job.is_some()),
                "{id}: a source direction needs a listing or a webhook"
            );
        }
        if caps["sink"].is_object() {
            assert!(
                http.resources
                    .values()
                    .any(|r| r.upsert.is_some() || r.create.is_some()),
                "{id}: a sink direction needs a write resource"
            );
        }
        // Every resource is exercised by at least one fixture scenario.
        let dir = root().join(format!("conformance/connectors/fixtures/{id}"));
        let mut covered = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let raw: Value =
                serde_json::from_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap())
                    .unwrap();
            if let Some(r) = raw["scenario"]["resource"].as_str() {
                covered.insert(r.to_string());
            }
        }
        for name in http.resources.keys() {
            // Second listings of the same shape (HubSpot companies, Shopify
            // orders, Stripe charges, Zendesk users, Google groups) share their
            // first listing's engine path and wait for the nightly recordings.
            let shares_shape = matches!(
                (*id, name.as_str()),
                ("hubspot", "companies")
                    | ("shopify", "orders")
                    | ("stripe", "charges")
                    | ("zendesk", "users")
                    | ("google-workspace", "groups")
                    | ("keycloak", "groups")
            );
            assert!(
                covered.contains(name) || shares_shape,
                "{id}: resource {name} has no fixture scenario"
            );
        }
    }
}

/// Every source and sink scenario of every Task 20 fixture replays through
/// the profile engine with no unmatched request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_saas_profile_replays_its_fixtures() {
    let mut ran = 0;
    for id in SAAS {
        let dir = root().join(format!("conformance/connectors/fixtures/{id}"));
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .map(|p| p.file_stem().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert!(!names.is_empty(), "{id}: no fixture");
        for name in names {
            let (_, sc) = fixture(id, &name);
            if sc["direction"] == "webhook" {
                Hook::load(id).check(id);
            } else {
                let r = run(id, &name).await;
                if sc["direction"] == "source" {
                    let event = sc["event"].as_str().unwrap_or("item");
                    for ev in &r.events {
                        assert_eq!(ev.ty, format!("io.loams.dev.flow.{id}.{event}.v1"));
                    }
                }
            }
            ran += 1;
        }
    }
    assert!(ran >= 50, "{ran} scenarios");
}

/// HubSpot: contacts go to `batch/upsert` 100 at a time under `/inputs`, keyed
/// by `id`, so the sink is effectively once; reads page by `paging.next.after`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hubspot_profile_upsert_batches() {
    let f = factory("hubspot");
    assert_eq!(f.sink_delivery("contacts"), Some(Delivery::EffectivelyOnce));
    let r = run("hubspot", "contacts-upsert").await;
    assert_eq!(r.rows, 3);
    assert_eq!(r.received.len(), 1, "three contacts, one batch request");
    let req = &r.received[0];
    assert_eq!(req.path, "/crm/v3/objects/contacts/batch/upsert");
    assert_eq!(
        header(req, "authorization"),
        Some("Bearer synthetic-hubspot-token")
    );
    let inputs = json_body(req)["inputs"].as_array().unwrap().clone();
    assert_eq!(inputs.len(), 3);
    assert!(inputs.iter().all(|i| i["idProperty"] == "email"));

    let r = run("hubspot", "contacts").await;
    assert_eq!(r.received.len(), 2);
    assert!(r.received[1].path.contains("after=103"));
    let subjects: Vec<_> = r
        .events
        .iter()
        .map(|e| e.subject.clone().unwrap())
        .collect();
    assert_eq!(subjects, ["101", "102", "103"]);
}

/// Salesforce Bulk API 2.0: the job is created with the instance's SOQL,
/// polled until `JobComplete`, and its CSV results are paged by
/// `Sforce-Locator` until `null`; the token comes from the refresh-token
/// grant first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn salesforce_bulk2_async_job_fixture() {
    let r = run("salesforce", "bulk-query").await;
    let paths: Vec<&str> = r.received.iter().map(|q| q.path.as_str()).collect();
    assert_eq!(paths[0], "/services/oauth2/token");
    assert_eq!(paths[1], "/services/data/v62.0/jobs/query");
    let create = json_body(&r.received[1]);
    assert_eq!(create["query"], "SELECT Id, Name FROM Account");
    let polls = paths
        .iter()
        .filter(|p| p.ends_with("/jobs/query/750R0000000zlh9IAA"))
        .count();
    assert_eq!(polls, 2, "polled until JobComplete: {paths:?}");
    let first_result = paths.iter().position(|p| p.contains("/results")).unwrap();
    let last_poll = paths
        .iter()
        .rposition(|p| p.ends_with("/jobs/query/750R0000000zlh9IAA"))
        .unwrap();
    assert!(
        last_poll < first_result,
        "results only after the job is done"
    );
    assert!(paths.last().unwrap().contains("locator=MTAwMDA"));
    for q in &r.received[1..] {
        assert_eq!(header(q, "authorization"), Some("Bearer <scrubbed>"));
    }
    let rows: Vec<Value> = r.events.iter().map(data).collect();
    assert_eq!(
        rows,
        vec![
            json!({"Id": "001R0000006hfeZIAQ", "Name": "Acme, Inc."}),
            json!({"Id": "001R0000006hfeaIAA", "Name": "Globex"}),
            json!({"Id": "001R0000006hfebIAA", "Name": "Initech \"Labs\""}),
        ]
    );
    assert_eq!(r.events[0].subject.as_deref(), Some("001R0000006hfeZIAQ"));
    assert_eq!(r.events[0].ty, "io.loams.dev.flow.salesforce.item.v1");
}

/// Stripe: a customer is created form-encoded with `Idempotency-Key` (the
/// event id), and customers are listed by `starting_after` the last id while
/// `has_more`. Against stripe-mock too when the `connectors-p2` stack is up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stripe_mock_roundtrip() {
    let f = factory("stripe");
    assert_eq!(
        f.sink_delivery("customers"),
        Some(Delivery::EffectivelyOnce)
    );
    let w = run("stripe", "customers-create").await;
    let req = &w.received[0];
    assert_eq!(header(req, "idempotency-key"), Some("ev-0"));
    assert_eq!(
        header(req, "content-type"),
        Some("application/x-www-form-urlencoded")
    );
    let r = run("stripe", "customers").await;
    assert!(r.received[1].path.contains("starting_after=cus_TB2"));
    let ids: Vec<Value> = r.events.iter().map(|e| data(e)["id"].clone()).collect();
    assert_eq!(ids, [json!("cus_TA1"), json!("cus_TB2"), json!("cus_TC3")]);

    if !stack_up() {
        eprintln!(
            "stripe_mock_roundtrip: stripe-mock part skipped (LOAMS_CONNECTORS_P2_STACK unset)"
        );
        return;
    }
    // stripe-mock accepts any test-mode key and answers from Stripe's OpenAPI
    // fixtures, so the roundtrip checks shapes, not stored state.
    let scrubber = Scrubber::new();
    let ctx = |resource: &str| {
        ConnectorContext::new(
            InstanceKey::new(7, "stripe-mock"),
            json!({"resource": resource, "base_url": "http://localhost:12111",
                   "auth": {"token": "stripe-key"}}),
            Arc::new(StaticSecrets::new().with("stripe-key", "sk_test_123")),
            scrubber.clone(),
        )
    };
    let fabric = MemoryFabric::new();
    fabric.push(
        CloudEvent::new("ev-live", "/test", "io.loams.dev.flow.test.item.v1").with_data(
            "application/json",
            json!({"email": "ada@example.test"}).to_string(),
        ),
    );
    let sink = f.sink(&ctx("customers")).unwrap().unwrap();
    let h = Pipeline::sink(InstanceKey::new(7, "stripe-mock"), pipeline_config(10))
        .reader(Box::new(fabric.reader("flow-live")))
        .sink(Arc::from(sink))
        .dead_letters(Arc::new(fabric.clone()))
        .scrubber(scrubber.clone())
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("stripe-mock created a customer", || {
        fabric.committed("flow-live") == 1
    })
    .await;
    assert!(h.stop().await.last_error.is_none());
    let out = MemoryFabric::new();
    let (en, src) = f.source(&ctx("customers")).unwrap().unwrap();
    let h = Pipeline::source(InstanceKey::new(7, "stripe-mock"), pipeline_config(16))
        .enumerator(en)
        .sources(Arc::from(src))
        .writer(Arc::new(out.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(out.clone()))
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("stripe-mock listed customers", || !out.is_empty()).await;
    assert!(h.stop().await.last_error.is_none());
    assert!(
        data(&out.events()[0])["id"]
            .as_str()
            .unwrap()
            .starts_with("cus_")
    );
}

/// Shopify's `X-Shopify-Hmac-Sha256`: base64 HMAC-SHA256 of the raw body.
#[test]
fn shopify_webhook_verifies() {
    let d = Hook::load("shopify");
    assert_eq!(d.scheme, "shopify");
    d.check("shopify");
    // Not a hex digest, not of a re-serialised body: the raw bytes.
    let reserialised =
        serde_json::to_vec(&serde_json::from_slice::<Value>(&d.body).unwrap()).unwrap();
    if reserialised != d.body {
        assert!(
            d.verify_with(d.secret.as_bytes(), &reserialised, d.now)
                .is_err()
        );
    }
}

/// The four schemes Task 20 adds, each on its fixture: HubSpot v3 (method,
/// URI, body and timestamp; five minutes), Zendesk (timestamp + body),
/// Twilio (URL + sorted form pairs, HMAC-SHA1) and Segment (HMAC-SHA1 hex).
#[test]
fn hubspot_zendesk_twilio_segment_signatures() {
    for id in ["hubspot", "zendesk", "twilio-sms", "segment"] {
        Hook::load(id).check(id);
    }
    let hs = Hook::load("hubspot");
    assert_eq!(
        hs.verify_with(
            hs.secret.as_bytes(),
            &hs.body,
            hs.now + Duration::from_secs(600)
        ),
        Err(WebhookError::Stale),
        "HubSpot refuses a timestamp older than five minutes"
    );
    // HubSpot signs the URI: another path is refused.
    let other = WebhookRequest {
        method: "POST",
        url: "https://hooks.loams.test/fabric/hubspot/other?portal=7",
        headers: &hs.headers,
        body: &hs.body,
    };
    assert!(webhook::verify(&hs.scheme, &hs.header, hs.secret.as_bytes(), &other, hs.now).is_err());
    // Twilio signs every form parameter: one changed value is refused.
    let tw = Hook::load("twilio-sms");
    let changed = String::from_utf8(tw.body.clone())
        .unwrap()
        .replace("delivered", "failed");
    assert_eq!(
        tw.verify_with(tw.secret.as_bytes(), changed.as_bytes(), tw.now),
        Err(WebhookError::Mismatch)
    );
    // Twilio's JSON callbacks sign the URL, whose `bodySHA256` must match.
    let body = br#"{"CallSid":"CA1234567890ABCDE","Caller":"+12349013030"}"#;
    let sha = "5ccde7145dfb8f56479710896586cb9d5911809d83afbe34627818790db0aec9";
    let url = format!("https://example.com/myapp?bodySHA256={sha}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"token");
    let sig = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        ring::hmac::sign(&key, url.as_bytes()),
    );
    let mut h = HeaderMap::new();
    h.insert("x-twilio-signature", sig.parse().unwrap());
    h.insert("content-type", "application/json".parse().unwrap());
    let req = |b: &'static [u8]| WebhookRequest {
        method: "POST",
        url: &url,
        headers: &h,
        body: b,
    };
    let now = SystemTime::now();
    webhook::verify("twilio", "X-Twilio-Signature", b"token", &req(body), now).unwrap();
    assert_eq!(
        webhook::verify("twilio", "X-Twilio-Signature", b"token", &req(b"{}"), now),
        Err(WebhookError::Mismatch),
        "the body must hash to bodySHA256 (Twilio's documented example)"
    );
}

/// Every other profile's webhook fixture verifies under its scheme, and the
/// timestamped ones refuse a stale delivery.
#[test]
fn stripe_github_slack_gitlab_kestra_webhooks_verify() {
    for id in ["stripe", "github", "slack", "gitlab", "kestra-companion"] {
        Hook::load(id).check(id);
    }
    for id in ["stripe", "slack"] {
        let d = Hook::load(id);
        assert_eq!(
            d.verify_with(
                d.secret.as_bytes(),
                &d.body,
                d.now + Duration::from_secs(3600)
            ),
            Err(WebhookError::Stale),
            "{id}"
        );
    }
}

/// Slack's own worked example from "Verifying requests from Slack".
#[test]
fn slack_signature_matches_vendor_example() {
    let body = "token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
    let mut h = HeaderMap::new();
    h.insert("x-slack-request-timestamp", "1531420618".parse().unwrap());
    h.insert(
        "x-slack-signature",
        "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"
            .parse()
            .unwrap(),
    );
    let req = WebhookRequest {
        method: "POST",
        url: "https://example.test/slack",
        headers: &h,
        body: body.as_bytes(),
    };
    let then = UNIX_EPOCH + Duration::from_secs(1_531_420_618 + 30);
    webhook::verify(
        "slack",
        "X-Slack-Signature",
        b"8f742231b10e8888abcd99yyyzzz85a5",
        &req,
        then,
    )
    .unwrap();
}

/// SendGrid's signed Event Webhook: ECDSA P-256 over timestamp + body,
/// checked with the base64 DER public key SendGrid shows.
#[test]
fn sendgrid_event_webhook_ecdsa_verifies() {
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, KeyPair};
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng).unwrap();
    let pair =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    // SubjectPublicKeyInfo for a P-256 key: the fixed prefix and the point.
    let mut spki = vec![
        0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08,
        0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
    ];
    spki.extend_from_slice(pair.public_key().as_ref());
    let b64 = |b: &[u8]| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b);
    let key = b64(&spki);
    let body = br#"[{"email":"ada@example.test","event":"delivered","sg_event_id":"x1"}]"#;
    let ts = "1759320000";
    let mut msg = ts.as_bytes().to_vec();
    msg.extend_from_slice(body);
    let sig = pair.sign(&rng, &msg).unwrap();
    let mut h = HeaderMap::new();
    h.insert(
        "x-twilio-email-event-webhook-signature",
        b64(sig.as_ref()).parse().unwrap(),
    );
    h.insert(
        "x-twilio-email-event-webhook-timestamp",
        ts.parse().unwrap(),
    );
    let p = profile("sendgrid");
    let spec = p.http.as_ref().unwrap().webhook.clone().unwrap();
    let req = |b: &'static [u8]| WebhookRequest {
        method: "POST",
        url: "https://hooks.loams.test/fabric/sendgrid/events",
        headers: &h,
        body: b,
    };
    let now = SystemTime::now();
    webhook::verify(&spec.scheme, &spec.header, key.as_bytes(), &req(body), now).unwrap();
    assert_eq!(
        webhook::verify(&spec.scheme, &spec.header, key.as_bytes(), &req(b"[]"), now),
        Err(WebhookError::Mismatch)
    );
    assert!(matches!(
        webhook::verify(&spec.scheme, &spec.header, b"bm90IGEga2V5", &req(body), now),
        Err(WebhookError::BadKey(_))
    ));
}

/// Splunk HEC: a batch goes as newline-separated `{"event": …}` objects on the
/// instance's channel, and is written only once Splunk acknowledges its
/// `ackId` (polled until `true`). HEC does not deduplicate, so the sink and its
/// manifest declare at-least-once (R20.8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn splunk_hec_ack_declared() {
    let f = factory("splunk");
    assert_eq!(f.sink_delivery("events"), Some(Delivery::AtLeastOnce));
    let manifest = read_yaml(&root().join("connectors/registry/splunk.yaml"));
    assert_eq!(
        manifest["capabilities"]["delivery"]["sink"],
        "at_least_once"
    );
    let r = run("splunk", "hec").await;
    assert_eq!(r.rows, 2);
    let paths: Vec<&str> = r.received.iter().map(|q| q.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "/services/collector/event",
            "/services/collector/ack",
            "/services/collector/ack"
        ],
        "one batch, then acks until true"
    );
    let channel = header(&r.received[0], "x-splunk-request-channel").unwrap();
    assert_eq!(channel.len(), 36, "a GUID: {channel}");
    assert!(
        r.received
            .iter()
            .all(|q| header(q, "x-splunk-request-channel") == Some(channel))
    );
    assert_eq!(
        header(&r.received[0], "authorization"),
        Some("Splunk 00000000-0000-4000-8000-00000000c0de")
    );
    assert_eq!(
        header(&r.received[0], "content-type"),
        Some("application/x-ndjson")
    );
    // The search-export source emits `log` events.
    let s = run("splunk", "search").await;
    assert!(
        s.events
            .iter()
            .all(|e| e.ty == "io.loams.dev.flow.splunk.log.v1")
    );
    assert_eq!(data(&s.events[0])["host"], "web-1");
}

/// SES v2 `SendEmail` is signed with SigV4 for the `ses` service in the
/// instance's region.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ses_sigv4_send_fixture() {
    let r = run("amazon-ses", "send").await;
    assert_eq!(r.rows, 1);
    let req = &r.received[0];
    let auth = header(req, "authorization").unwrap();
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"),
        "{auth}"
    );
    assert!(auth.contains("/us-east-1/ses/aws4_request"), "{auth}");
    assert!(
        auth.contains("SignedHeaders=content-type;host;x-amz-date"),
        "{auth}"
    );
    assert!(header(req, "x-amz-date").is_some_and(|d| d.len() == 16 && d.ends_with('Z')));
    assert!(!auth.contains("synthetic-secret-access-key"));
    // Another region signs for that region.
    let r = run_with(
        "amazon-ses",
        "send",
        Some(json!({"region": "eu-west-1", "base_url": "{proxy}"})),
    )
    .await;
    assert!(
        header(&r.received[0], "authorization")
            .unwrap()
            .contains("/eu-west-1/ses/aws4_request")
    );
}

/// Vault KV v2 through OpenBao's API: an item `{path, data}` is written to
/// `/v1/<mount>/data/<path>` as `{"data": …}` with `X-Vault-Token`, and the
/// mount's keys are listed. Against OpenBao too when the stack is up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openbao_kv_roundtrip() {
    let f = factory("vault");
    assert_eq!(f.sink_delivery("kv"), Some(Delivery::EffectivelyOnce));
    let w = run("vault", "kv").await;
    let req = &w.received[0];
    assert_eq!(req.path, "/v1/secret/data/app/db");
    assert_eq!(header(req, "x-vault-token"), Some("synthetic-vault-token"));
    assert_eq!(
        json_body(req),
        json!({"data": {"user": "app", "dsn": "postgres://db.internal:5432/app"}})
    );
    let r = run("vault", "keys").await;
    let keys: Vec<Value> = r.events.iter().map(data).collect();
    assert_eq!(keys, [json!("app/"), json!("billing")]);
    let manifest = read_yaml(&root().join("connectors/registry/vault.yaml"));
    let notes = manifest["notes"].as_array().cloned().unwrap_or_default();
    assert!(
        notes.iter().any(|n| n
            .as_str()
            .is_some_and(|n| n.contains("not Loams' own secret store"))),
        "Vault's manifest says it is not Loams' secret store: {notes:?}"
    );

    if !stack_up() {
        eprintln!("openbao_kv_roundtrip: OpenBao part skipped (LOAMS_CONNECTORS_P2_STACK unset)");
        return;
    }
    let scrubber = Scrubber::new();
    let ctx = |resource: &str| {
        ConnectorContext::new(
            InstanceKey::new(7, "openbao"),
            json!({"resource": resource, "address": "http://localhost:8200",
                   "auth": {"api_key": "bao-token"}}),
            Arc::new(StaticSecrets::new().with("bao-token", "loams-dev-root")),
            scrubber.clone(),
        )
    };
    let path = format!("loams-t20-{}/db", std::process::id());
    let fabric = MemoryFabric::new();
    fabric.push(
        CloudEvent::new("ev-live", "/test", "io.loams.dev.flow.test.item.v1").with_data(
            "application/json",
            json!({"path": path, "data": {"user": "app"}}).to_string(),
        ),
    );
    let sink = f.sink(&ctx("kv")).unwrap().unwrap();
    let h = Pipeline::sink(InstanceKey::new(7, "openbao"), pipeline_config(10))
        .reader(Box::new(fabric.reader("flow-live")))
        .sink(Arc::from(sink))
        .dead_letters(Arc::new(fabric.clone()))
        .scrubber(scrubber.clone())
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("OpenBao stored the secret", || {
        fabric.committed("flow-live") == 1
    })
    .await;
    assert!(h.stop().await.last_error.is_none());
    let out = MemoryFabric::new();
    let (en, src) = f.source(&ctx("keys")).unwrap().unwrap();
    let h = Pipeline::source(InstanceKey::new(7, "openbao"), pipeline_config(16))
        .enumerator(en)
        .sources(Arc::from(src))
        .writer(Arc::new(out.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(out.clone()))
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap();
    let top = format!("loams-t20-{}/", std::process::id());
    eventually("OpenBao listed the new key", || {
        out.events().iter().any(|e| data(e) == json!(top))
    })
    .await;
    assert!(h.stop().await.last_error.is_none());
}

/// Keycloak: admin events are read with a client-credentials token, paged by
/// `first`/`max`; users are created, and a `409` (the user exists) counts as
/// written. Against Keycloak too when the stack is up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keycloak_admin_events_source() {
    let r = run("keycloak", "admin-events").await;
    assert_eq!(
        r.received[0].path,
        "/realms/master/protocol/openid-connect/token"
    );
    match &r.received[0].body {
        Body::Text(t) => assert!(t.starts_with("grant_type=client_credentials&"), "{t}"),
        other => panic!("{other:?}"),
    }
    assert!(r.received[1].path.contains("first=0") && r.received[1].path.contains("max=100"));
    let types: Vec<Value> = r
        .events
        .iter()
        .map(|e| data(e)["resourceType"].clone())
        .collect();
    assert_eq!(types, [json!("USER"), json!("GROUP")]);
    assert!(
        r.events
            .iter()
            .all(|e| e.ty == "io.loams.dev.flow.keycloak.item.v1" && e.subject.is_some())
    );
    let f = factory("keycloak");
    assert_eq!(f.sink_delivery("users"), Some(Delivery::EffectivelyOnce));
    let w = run("keycloak", "users-create").await;
    assert_eq!(w.rows, 2, "the 409 for an existing user counts as written");

    if !stack_up() {
        eprintln!(
            "keycloak_admin_events_source: Keycloak part skipped (LOAMS_CONNECTORS_P2_STACK unset)"
        );
        return;
    }
    keycloak_live().await;
}

/// The live half: the compose service's bootstrap service-account client
/// (`loams-ci`) enables admin events on `master`, a group is created through
/// the sink, and the source reads the admin event it caused.
async fn keycloak_live() {
    use bytes::Bytes;
    let base = "http://localhost:8180";
    let client = loams_connector_sdk::http::client(Arc::new(AllowAll));
    let token_req = ::http::Request::post(format!(
        "{base}/realms/master/protocol/openid-connect/token"
    ))
    .header("content-type", "application/x-www-form-urlencoded")
    .body(Bytes::from_static(
        b"grant_type=client_credentials&client_id=loams-ci&client_secret=loams-test",
    ))
    .unwrap();
    let resp = client.send(token_req).await.unwrap();
    let token: Value = serde_json::from_slice(resp.body()).unwrap();
    let token = token["access_token"].as_str().unwrap().to_string();
    let enable = ::http::Request::put(format!("{base}/admin/realms/master"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Bytes::from(
            json!({"adminEventsEnabled": true, "adminEventsDetailsEnabled": true}).to_string(),
        ))
        .unwrap();
    assert!(client.send(enable).await.unwrap().status().is_success());

    let f = factory("keycloak");
    let scrubber = Scrubber::new();
    let ctx = |resource: &str| {
        ConnectorContext::new(
            InstanceKey::new(7, "keycloak"),
            json!({"resource": resource, "server_url": base, "realm": "master",
                   "auth": {"client_id": "kc-id", "client_secret": "kc-secret"}}),
            Arc::new(
                StaticSecrets::new()
                    .with("kc-id", "loams-ci")
                    .with("kc-secret", "loams-test"),
            ),
            scrubber.clone(),
        )
    };
    let group = format!("loams-t20-{}", std::process::id());
    let fabric = MemoryFabric::new();
    fabric.push(
        CloudEvent::new("ev-live", "/test", "io.loams.dev.flow.test.item.v1")
            .with_data("application/json", json!({"name": group}).to_string()),
    );
    let sink = f.sink(&ctx("groups")).unwrap().unwrap();
    let h = Pipeline::sink(InstanceKey::new(7, "keycloak"), pipeline_config(10))
        .reader(Box::new(fabric.reader("flow-live")))
        .sink(Arc::from(sink))
        .dead_letters(Arc::new(fabric.clone()))
        .scrubber(scrubber.clone())
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("Keycloak created the group", || {
        fabric.committed("flow-live") == 1
    })
    .await;
    assert!(h.stop().await.last_error.is_none());
    let out = MemoryFabric::new();
    let (en, src) = f.source(&ctx("admin_events")).unwrap().unwrap();
    let h = Pipeline::source(InstanceKey::new(7, "keycloak"), pipeline_config(16))
        .enumerator(en)
        .sources(Arc::from(src))
        .writer(Arc::new(out.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(out.clone()))
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap();
    eventually("the group's admin event", || {
        out.events()
            .iter()
            .any(|e| data(e)["resourceType"] == "GROUP" && data(e)["operationType"] == "CREATE")
    })
    .await;
    assert!(h.stop().await.last_error.is_none());
}

/// Kestra companion: an event triggers a flow through the webhook trigger,
/// whose key is a secret resolved into the path at send time (never in a
/// profile, a position or an error); Kestra's own webhooks verify by shared
/// token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kestra_companion_triggers_flow() {
    let f = factory("kestra-companion");
    assert_eq!(f.sink_delivery("trigger"), Some(Delivery::AtLeastOnce));
    let w = run("kestra-companion", "trigger").await;
    assert_eq!(w.rows, 1);
    let req = &w.received[0];
    assert_eq!(req.method, "POST");
    assert_eq!(
        req.path,
        "/api/v1/main/executions/webhook/loams/import/synthetic-webhook-key"
    );
    assert_eq!(json_body(req), json!({"order": "A-1", "amount": 19}));
    let text =
        std::fs::read_to_string(root().join("connectors/profiles/kestra-companion.yaml")).unwrap();
    assert!(text.contains("{auth.webhook_key}") && !text.contains("synthetic-webhook-key"));
    let r = run("kestra-companion", "executions").await;
    assert!(r.received[0].path.contains("page=1") && r.received[0].path.contains("size=100"));
    Hook::load("kestra-companion").check("kestra-companion");
}

/// The source half of an import route, replayed: the route's source instance
/// (with the deployment's parameters) runs the profile's listing against the
/// fixture and its projection shapes each item for the target.
async fn import_replays(
    route_id: &str,
    source: &str,
    fixture_name: &str,
    params: &[(&str, &str)],
) -> (ImportRoute, Vec<CloudEvent>) {
    let route: ImportRoute = serde_norway::from_str(
        &std::fs::read_to_string(root().join(format!("connectors/routes/{route_id}.yaml")))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(route.kind, "ImportRoute");
    assert!(!route.topic.is_empty());
    // The sink is the target's own manifest (CN1 Task 15).
    assert!(
        root()
            .join(format!("connectors/registry/{}.yaml", route.sink.connector))
            .exists()
    );
    // The Camel template it replaces is gone.
    assert!(
        !root()
            .join(format!("connect/routes/templates/{route_id}.yaml.tmpl"))
            .exists()
    );
    let values: BTreeMap<String, String> = params
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    for p in &route.parameters {
        assert!(
            values.contains_key(p),
            "{route_id}: parameter {p} has no test value"
        );
    }
    let instance = route
        .sources
        .iter()
        .find(|s| s.connector == source)
        .unwrap_or_else(|| panic!("{route_id}: no {source} source"));
    let mut config = route.instance_config(instance, &values).unwrap();
    // A replay reads the listing once.
    config["poll_interval_ms"] = Value::Null;
    // The fixture's own secrets, under the names the route gives them.
    let (_, sc) = fixture(source, fixture_name);
    let mut secrets = StaticSecrets::new();
    for (field, name) in config["auth"].as_object().unwrap().clone() {
        secrets = secrets.with(
            name.as_str().unwrap(),
            sc["secrets"][&field].as_str().unwrap(),
        );
    }
    let (fx, _) = fixture(source, fixture_name);
    let scrubber = Scrubber::new();
    let proxy = FixtureProxy::replay(fx, ScrubRules::default().with_values(scrubber.clone()))
        .await
        .unwrap();
    config["base_url"] = Value::String(proxy.base_url());
    let ctx = ConnectorContext::new(
        InstanceKey::new(7, route_id),
        config,
        Arc::new(secrets),
        scrubber.clone(),
    );
    let fabric = MemoryFabric::new();
    let (en, src) = factory(source).source(&ctx).unwrap().unwrap();
    let h = Pipeline::source(ctx.instance.clone(), pipeline_config(16))
        .enumerator(en)
        .sources(Arc::from(src))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(fabric.clone()))
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap();
    let expect = sc["expect"].as_u64().unwrap() as usize;
    eventually("the projected items", || fabric.len() >= expect).await;
    let st = h.stop().await;
    assert!(st.last_error.is_none(), "{:?}", st.last_error);
    proxy.finish().unwrap();
    (route, fabric.events())
}

/// GitHub issues → Forgejo: each issue becomes only the fields a Forgejo
/// issue has, and keeps the raw issue's content id (the sink's idempotency
/// key), so a projection change never re-imports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn github_to_forgejo_import_replays() {
    let (route, events) = import_replays(
        "forgejo-import",
        "github",
        "issues",
        &[
            ("owner", "loams-labs"),
            ("repo", "demo"),
            ("forgejo_owner", "loams"),
            ("forgejo_repo", "mirror"),
        ],
    )
    .await;
    assert_eq!(route.sink.connector, "forgejo");
    let issues: Vec<Value> = events.iter().map(data).collect();
    assert_eq!(
        issues[0],
        json!({"title": "Crash on start", "body": "Steps to reproduce", "labels": ["bug"],
               "milestone": "v1", "assignees": ["octo-ada"], "state": "open",
               "source_url": "https://github.com/loams-labs/demo/issues/1"})
    );
    assert_eq!(issues[1]["state"], "closed");
    assert_eq!(issues[1]["labels"], json!(["docs", "good first issue"]));
    assert_eq!(issues[1]["milestone"], Value::Null);
    assert_eq!(issues[2]["assignees"], json!([]));
    // The id is the raw item's: the same as without a projection.
    let plain = run("github", "issues").await;
    let ids = |e: &[CloudEvent]| e.iter().map(|x| x.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&events), ids(&plain.events));
}

/// Jira issues → ItsPlane, through the route that also takes GitHub issues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jira_to_itsplane_import_replays() {
    let (route, events) = import_replays(
        "itsplane-import",
        "jira",
        "issues",
        &[
            ("owner", "loams-labs"),
            ("repo", "demo"),
            ("jira_site", "loams-test"),
            ("jira_jql", "order by updated ASC"),
            ("itsplane_project", "MKT"),
        ],
    )
    .await;
    assert_eq!(route.sink.connector, "itsplane");
    assert_eq!(
        route
            .sources
            .iter()
            .map(|s| s.connector.as_str())
            .collect::<Vec<_>>(),
        ["github", "jira"]
    );
    let issues: Vec<Value> = events.iter().map(data).collect();
    assert_eq!(
        issues[0],
        json!({"name": "Set up CI", "description": "Imported from Jira LOAMS-1 (To Do)",
               "priority": "medium", "labels": ["import"],
               "source_url": "https://loams-test.atlassian.net/rest/api/3/issue/10001"})
    );
    assert_eq!(issues.len(), 3);
}

/// Slack messages → Zulip: a stream message per Slack message, topic per
/// thread (`thread_ts`, else the message's own `ts`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slack_to_zulip_import_replays() {
    let (route, events) = import_replays(
        "zulip-import",
        "slack",
        "messages",
        &[
            ("slack_channel", "C0123456789"),
            ("zulip_stream", "imported"),
        ],
    )
    .await;
    assert_eq!(route.sink.connector, "zulip");
    let msgs: Vec<Value> = events.iter().map(data).collect();
    assert_eq!(
        msgs[0],
        json!({"type": "stream", "to": "imported", "topic": "1759300000.000100",
               "content": "*<U012AB3CD>*: Deploy is green"})
    );
    assert_eq!(
        msgs[1]["topic"], "1759300000.000100",
        "a reply joins its thread's topic"
    );
    assert_eq!(msgs[2]["topic"], "1759300120.000300");
}
