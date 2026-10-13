//! CN2 plan Task 7: the declarative HTTP-API profile engine, against a local
//! HTTP server that plays the API (the real APIs' suites are Task 20's).

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use loams_connector_http::profile::HttpApiFactory;
use loams_connector_http::profile::sigv4::{self, Credentials, SigningParams};
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::*;
use serde_json::{Value, json};

// ------------------------------------------------------------------ server --

/// One request as the fake API saw it.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
    body: Value,
    at: Instant,
}

type Handler = Arc<dyn Fn(&Seen, usize) -> (u16, Vec<(&'static str, String)>, Value) + Send + Sync>;

/// A fake HTTP API on 127.0.0.1: `handler` gets each request and how many came
/// before it, and returns the status, headers and JSON body.
#[derive(Clone)]
struct FakeApi {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl FakeApi {
    async fn start(handler: Handler) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let log = log.clone();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let svc =
                        hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                            let log = log.clone();
                            let handler = handler.clone();
                            async move {
                                let (parts, body) = req.into_parts();
                                let body = body.collect().await.unwrap().to_bytes();
                                let query = parts
                                    .uri
                                    .query()
                                    .unwrap_or("")
                                    .split('&')
                                    .filter(|kv| !kv.is_empty())
                                    .map(|kv| {
                                        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                                        (k.to_string(), v.to_string())
                                    })
                                    .collect();
                                let seen = Seen {
                                    method: parts.method.to_string(),
                                    path: parts.uri.path().to_string(),
                                    query,
                                    headers: parts
                                        .headers
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.as_str().to_string(),
                                                v.to_str().unwrap_or("").to_string(),
                                            )
                                        })
                                        .collect(),
                                    body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                                    at: Instant::now(),
                                };
                                let n = {
                                    let mut l = log.lock().unwrap();
                                    l.push(seen.clone());
                                    l.len() - 1
                                };
                                let (status, headers, body) = handler(&seen, n);
                                let mut resp = Response::builder()
                                    .status(StatusCode::from_u16(status).unwrap())
                                    .header("content-type", "application/json");
                                for (k, v) in headers {
                                    resp = resp.header(k, v);
                                }
                                Ok::<_, Infallible>(
                                    resp.body(Full::new(Bytes::from(body.to_string()))).unwrap(),
                                )
                            }
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Self { base, seen }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

// ----------------------------------------------------------------- helpers --

fn instance() -> InstanceKey {
    InstanceKey::new(3, "crm")
}

/// An HTTP-API profile over `http`, as `loams-flow` would load it.
fn profile(http: Value) -> Profile {
    serde_json::from_value(json!({
        "id": "fake-crm",
        "profile_kind": "http-api",
        "module": "http",
        "system": {"name": "Fake CRM", "service": {"fixture": "conformance/connectors/fixtures/fake-crm"}},
        "defaults": {"poll_interval_ms": null},
        "narrows": [],
        "http": http,
    }))
    .unwrap()
}

fn context(config: Value, secrets: StaticSecrets) -> (ConnectorContext, Arc<Scrubber>) {
    let scrubber = Scrubber::new();
    let ctx = ConnectorContext::new(instance(), config, Arc::new(secrets), scrubber.clone());
    (ctx, scrubber)
}

fn config() -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: 2,
        max_batch_bytes: 64 << 10,
        linger: Duration::from_millis(10),
        max_in_flight_batches: 2,
        memory_budget_bytes: 1 << 20,
        checkpoint_interval: Duration::ZERO,
        retry: RetryPolicy {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(200),
            ..RetryPolicy::default()
        },
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn source_pipeline(
    factory: &HttpApiFactory,
    ctx: &ConnectorContext,
    fabric: &MemoryFabric,
    store: &MemoryCheckpointStore,
    scrubber: Arc<Scrubber>,
) -> PipelineHandle {
    let (enumerator, sources) = factory.source(ctx).unwrap().expect("a source direction");
    Pipeline::source(ctx.instance.clone(), config())
        .enumerator(enumerator)
        .sources(Arc::from(sources))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(store.clone()))
        .dead_letters(Arc::new(fabric.clone()))
        .scrubber(scrubber)
        .epoch(1)
        .start()
        .await
        .unwrap()
}

fn data(ev: &CloudEvent) -> Value {
    serde_json::from_slice(&ev.data).unwrap()
}

// ------------------------------------------------------------------- tests --

/// A `list` resource pages by cursor; a restart resumes from the checkpointed
/// cursor rather than from the first page, and the credential is resolved from
/// the secret the instance config names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_profile_paginates_and_resumes() {
    let release = Arc::new(Mutex::new(false));
    let gate = release.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen, _| {
        let page = req.query.get("after").map(String::as_str);
        let (ids, next): (&[u32], Option<&str>) = match page {
            None => (&[1, 2, 3], Some("p1")),
            Some("p1") => (&[4, 5, 6], Some("p2")),
            Some("p2") if !*gate.lock().unwrap() => return (503, vec![], json!({})),
            Some("p2") => (&[7, 8], None),
            _ => return (400, vec![], json!({"error": "bad cursor"})),
        };
        let results: Vec<Value> = ids
            .iter()
            .map(|i| json!({"id": i.to_string(), "n": i}))
            .collect();
        let paging = next.map_or(Value::Null, |n| json!({"next": {"after": n}}));
        (200, vec![], json!({"results": results, "paging": paging}))
    }))
    .await;
    let p = profile(json!({
        "base_url": api.base,
        "auth": {"scheme": "bearer"},
        "headers": {"Accept": "application/json"},
        "resources": {"contacts": {"list": {
            "method": "GET", "path": "/crm/v3/objects/contacts", "items": "/results",
            "cursor": {"next": "/paging/next/after", "param": "after"}, "key": "/id",
            "query": {"limit": "3"}
        }}}
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    let secrets = StaticSecrets::new().with("crm-token", "tok-0123456789");
    let (ctx, scrubber) = context(
        json!({"resource": "contacts", "auth": {"token": "crm-token"}}),
        secrets,
    );

    // First run: pages 1 and 2 arrive, page 3 is unavailable; then the process dies.
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let run = source_pipeline(&factory, &ctx, &fabric, &store, scrubber.clone()).await;
    eventually("six records", || fabric.len() == 6).await;
    eventually("the second page checkpointed", || {
        store
            .get(&instance())
            .is_some_and(|cp| cp.splits.values().any(|p| p.display.contains("p2")))
    })
    .await;
    run.abort();
    let first = api.seen();
    assert!(first.iter().all(
        |r| r.headers.get("authorization").map(String::as_str) == Some("Bearer tok-0123456789")
    ));
    assert!(
        first
            .iter()
            .all(|r| r.query.get("limit").map(String::as_str) == Some("3"))
    );
    assert!(
        first
            .iter()
            .all(|r| r.headers.get("accept").map(String::as_str) == Some("application/json"))
    );
    let got: Vec<Value> = fabric
        .events()
        .iter()
        .map(|e| data(e)["n"].clone())
        .collect();
    assert_eq!(got, (1..=6).map(|n| json!(n)).collect::<Vec<_>>());
    let ev = &fabric.events()[0];
    assert_eq!(ev.subject.as_deref(), Some("1"));
    assert_eq!(ev.ty, "io.loams.dev.flow.fake-crm.record.v1");
    // No position or status names the credential.
    for cp in store.saves() {
        for pos in cp.splits.values() {
            assert!(!pos.display.contains("tok-0123456789"));
        }
    }

    // Second run, from the same checkpoint: it starts at page 3, not page 1.
    *release.lock().unwrap() = true;
    let before = api.seen().len();
    let fabric2 = MemoryFabric::new();
    let run = source_pipeline(&factory, &ctx, &fabric2, &store, scrubber).await;
    eventually("the last page", || fabric2.len() == 2).await;
    let st = run.stop().await;
    assert!(st.last_error.is_none(), "{:?}", st.last_error);
    let resumed = &api.seen()[before..];
    assert!(
        resumed
            .iter()
            .all(|r| r.query.get("after").map(String::as_str) == Some("p2")),
        "a resumed run starts from the checkpointed cursor: {:?}",
        resumed.iter().map(|r| r.query.clone()).collect::<Vec<_>>()
    );
    let got: Vec<Value> = fabric2
        .events()
        .iter()
        .map(|e| data(e)["n"].clone())
        .collect();
    assert_eq!(got, vec![json!(7), json!(8)]);
}

/// `Retry-After` on a 429 becomes `Throttled` with that delay, and no request
/// goes before it has passed; a `*-Remaining: 0` with a `*-Reset` holds the next
/// request until the window resets. Headers the profile does not list are not
/// honoured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_profile_honours_rate_limit_headers() {
    let api = FakeApi::start(Arc::new(|req: &Seen, n| match n {
        0 => (429, vec![("retry-after", "1".to_string())], json!({})),
        1 => (
            200,
            vec![
                ("x-ratelimit-remaining", "0".to_string()),
                ("x-ratelimit-reset", "1".to_string()),
            ],
            json!({"items": [{"id": "a"}], "next": "c2"}),
        ),
        _ => {
            assert_eq!(req.query.get("cursor").map(String::as_str), Some("c2"));
            (
                200,
                vec![("x-ignored-remaining", "0".to_string())],
                json!({"items": [{"id": "b"}], "next": null}),
            )
        }
    }))
    .await;
    let p = profile(json!({
        "base_url": api.base,
        "auth": {"scheme": "header", "header": "X-Api-Key"},
        "resources": {"things": {"list": {
            "method": "GET", "path": "/things", "items": "/items",
            "cursor": {"next": "/next", "param": "cursor"}
        }}},
        "rate_limit": {"requests": 50, "per_seconds": 1,
                       "honour_headers": ["Retry-After", "X-RateLimit-Remaining", "X-RateLimit-Reset"]}
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    let (ctx, _) = context(
        json!({"resource": "things", "auth": {"api_key": "things-key"}}),
        StaticSecrets::new().with("things-key", "key-abcdef"),
    );
    let (mut enumerator, sources) = factory.source(&ctx).unwrap().unwrap();
    let splits = enumerator.discover(&[]).await.unwrap();
    assert_eq!(splits.len(), 1);
    let mut source = sources.create(&splits[0]).unwrap();
    source.open(&splits[0], None).await.unwrap();
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };

    let err = source.next(budget).await.expect_err("429 is throttled");
    assert_eq!(
        err.class,
        ErrorClass::Throttled {
            after: Duration::from_secs(1)
        }
    );
    let batch = source.next(budget).await.unwrap().expect("page 1");
    assert_eq!(batch.events.rows(), 1);
    let batch = source.next(budget).await.unwrap().expect("page 2");
    assert_eq!(batch.events.rows(), 1);

    let seen = api.seen();
    assert_eq!(seen.len(), 3);
    assert!(
        seen.iter()
            .all(|r| r.headers.get("x-api-key").map(String::as_str) == Some("key-abcdef"))
    );
    let gap1 = seen[1].at.duration_since(seen[0].at);
    let gap2 = seen[2].at.duration_since(seen[1].at);
    assert!(
        gap1 >= Duration::from_millis(900),
        "Retry-After not honoured: {gap1:?}"
    );
    assert!(
        gap2 >= Duration::from_millis(900),
        "Remaining/Reset not honoured: {gap2:?}"
    );

    // The unlisted `x-ignored-remaining: 0` holds nothing back: the listing is
    // over, and a bounded source just reports nothing more.
    let t = Instant::now();
    assert!(source.next(budget).await.unwrap().is_none());
    assert!(t.elapsed() < Duration::from_millis(500));
}

/// An `upsert` resource batches up to `batch` items under `body`, sends
/// `Idempotency-Key` where the API accepts it, and a retry after a lost response
/// sends the same key and writes nothing twice (`effectively_once`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_profile_upsert_idempotent_on_retry() {
    let stored: Arc<Mutex<BTreeMap<String, Value>>> = Arc::default();
    let by_key: Arc<Mutex<BTreeMap<String, Value>>> = Arc::default();
    let (s, k) = (stored.clone(), by_key.clone());
    let api = FakeApi::start(Arc::new(move |req: &Seen, n| {
        let key = req
            .headers
            .get("idempotency-key")
            .cloned()
            .unwrap_or_default();
        if let Some(previous) = k.lock().unwrap().get(&key) {
            return (200, vec![], previous.clone());
        }
        for item in req.body["inputs"].as_array().unwrap() {
            s.lock()
                .unwrap()
                .insert(item["id"].as_str().unwrap().to_string(), item.clone());
        }
        let reply = json!({"status": "COMPLETE"});
        k.lock().unwrap().insert(key, reply.clone());
        if n == 0 {
            // Applied, but the response is lost.
            (503, vec![], json!({}))
        } else {
            (200, vec![], reply)
        }
    }))
    .await;
    let p = profile(json!({
        "base_url": format!("{}/v3", api.base),
        "auth": {"scheme": "bearer"},
        "resources": {
            "contacts": {"upsert": {
                "method": "POST", "path": "/contacts/batch/upsert", "batch": 2,
                "key": "/id", "body": "/inputs", "idempotency_key": true
            }},
            "notes": {"create": {"method": "POST", "path": "/notes"}}
        }
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    assert_eq!(
        factory.sink_delivery("contacts"),
        Some(Delivery::EffectivelyOnce)
    );
    assert_eq!(factory.sink_delivery("notes"), Some(Delivery::AtLeastOnce));
    assert_eq!(factory.sink_delivery("nothing"), None);

    let (ctx, scrubber) = context(
        json!({"resource": "contacts", "auth": {"token": "crm-token"}}),
        StaticSecrets::new().with("crm-token", "tok-0123456789"),
    );
    let fabric = MemoryFabric::new();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        fabric.push(
            CloudEvent::new(
                format!("ev-{i}"),
                "/test",
                "io.loams.dev.flow.test.record.v1",
            )
            .with_data("application/json", json!({"id": id, "v": i}).to_string()),
        );
    }
    let sink = factory.sink(&ctx).unwrap().expect("a sink direction");
    let run = Pipeline::sink(
        ctx.instance.clone(),
        PipelineConfig {
            max_batch_rows: 3,
            ..config()
        },
    )
    .reader(Box::new(fabric.reader("flow-crm")))
    .sink(Arc::from(sink))
    .dead_letters(Arc::new(fabric.clone()))
    .scrubber(scrubber)
    .epoch(1)
    .start()
    .await
    .unwrap();
    eventually("all three committed", || fabric.committed("flow-crm") == 3).await;
    let st = run.stop().await;
    assert!(st.last_error.is_none(), "{:?}", st.last_error);

    let seen = api.seen();
    assert!(
        seen.iter()
            .all(|r| r.path == "/v3/contacts/batch/upsert" && r.method == "POST")
    );
    let keys: Vec<&str> = seen
        .iter()
        .map(|r| r.headers["idempotency-key"].as_str())
        .collect();
    // [a, b] (lost), [a, b] again with the same key, then [c] with the event's id.
    assert_eq!(keys.len(), 3, "{keys:?}");
    assert_eq!(keys[0], keys[1], "a retry must resend the same key");
    assert_ne!(keys[0], keys[2]);
    assert_eq!(keys[2], "ev-2");
    assert_eq!(seen[0].body["inputs"].as_array().unwrap().len(), 2);
    assert_eq!(seen[2].body["inputs"].as_array().unwrap().len(), 1);
    let stored = stored.lock().unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored["b"]["v"], json!(1));
}

/// An `async_job` resource creates a job, polls it until it reports done, then
/// pages through its results; a failed job stops the instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_profile_async_job_polls_to_completion() {
    let polls = Arc::new(Mutex::new(0u32));
    let pc = polls.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen, _| {
        match (req.method.as_str(), req.path.as_str()) {
            ("POST", "/jobs") => {
                let id = if req.body["object"] == "broken" {
                    "bad-1"
                } else {
                    "750x"
                };
                (200, vec![], json!({"id": id, "state": "UploadComplete"}))
            }
            ("GET", "/jobs/bad-1") => (200, vec![], json!({"state": "Failed"})),
            ("GET", "/jobs/750x") => {
                let mut p = pc.lock().unwrap();
                *p += 1;
                let state = if *p < 3 { "InProgress" } else { "JobComplete" };
                (200, vec![], json!({"state": state}))
            }
            ("GET", "/jobs/750x/results") => match req.query.get("locator").map(String::as_str) {
                None => (
                    200,
                    vec![],
                    json!({"records": [{"r": 1}, {"r": 2}], "next": "L2"}),
                ),
                Some("L2") => (200, vec![], json!({"records": [{"r": 3}], "next": null})),
                _ => (400, vec![], json!({})),
            },
            _ => (404, vec![], json!({})),
        }
    }))
    .await;
    let job = |object: &str| {
        json!({
            "create": {"method": "POST", "path": "/jobs", "body": {"object": object, "operation": "query"}},
            "id": "/id",
            "poll": {"method": "GET", "path": "/jobs/{id}", "state": "/state",
                     "done": ["JobComplete"], "failed": ["Failed", "Aborted"], "interval_ms": 50},
            "results": {"method": "GET", "path": "/jobs/{id}/results", "items": "/records",
                        "cursor": {"next": "/next", "param": "locator"}}
        })
    };
    let p = profile(json!({
        "base_url": api.base,
        "auth": {"scheme": "basic"},
        "resources": {"export": {"async_job": job("contacts")}, "broken": {"async_job": job("broken")}}
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    let secrets = || {
        StaticSecrets::new()
            .with("u", "loams-user")
            .with("pw", "pw-0123456789")
    };
    let (ctx, scrubber) = context(
        json!({"resource": "export", "auth": {"username": "u", "password": "pw"}}),
        secrets(),
    );
    let fabric = MemoryFabric::new();
    let store = MemoryCheckpointStore::new();
    let run = source_pipeline(&factory, &ctx, &fabric, &store, scrubber).await;
    eventually("three result rows", || fabric.len() == 3).await;
    let st = run.stop().await;
    assert!(st.last_error.is_none(), "{:?}", st.last_error);
    let rows: Vec<Value> = fabric
        .events()
        .iter()
        .map(|e| data(e)["r"].clone())
        .collect();
    assert_eq!(rows, vec![json!(1), json!(2), json!(3)]);

    let seen = api.seen();
    let creates = seen.iter().filter(|r| r.method == "POST").count();
    assert_eq!(creates, 1, "one job for a bounded run");
    assert_eq!(*polls.lock().unwrap(), 3, "polled until JobComplete");
    let first_result = seen
        .iter()
        .position(|r| r.path.ends_with("/results"))
        .unwrap();
    let last_poll = seen.iter().rposition(|r| r.path == "/jobs/750x").unwrap();
    assert!(
        last_poll < first_result,
        "results only after the job is done"
    );
    let expected = format!(
        "Basic {}",
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "loams-user:pw-0123456789"
        )
    );
    assert!(
        seen.iter()
            .all(|r| r.headers.get("authorization") == Some(&expected))
    );

    // A failed job is fatal, and says which state it ended in.
    let (ctx, scrubber) = context(
        json!({"resource": "broken", "auth": {"username": "u", "password": "pw"}}),
        secrets(),
    );
    let fabric = MemoryFabric::new();
    let run = source_pipeline(
        &factory,
        &ctx,
        &fabric,
        &MemoryCheckpointStore::new(),
        scrubber,
    )
    .await;
    let st = run.wait_for(|s| s.state == PipelineState::Failed).await;
    let err = st.last_error.unwrap();
    assert_eq!(err.class, ErrorClass::Fatal);
    assert!(err.message.contains("Failed"), "{}", err.message);
    assert!(fabric.is_empty());
}

/// SigV4 against AWS's published examples: the signing key of the
/// "derive a signing key" example, the IAM `ListUsers` example of "Create a
/// canonical request", and the `get-vanilla`, `get-vanilla-query-order-key-case`
/// and `post-vanilla` cases of the Signature Version 4 test suite (credentials
/// `AKIDEXAMPLE`, 2015-08-30T12:36:00Z, us-east-1). Then the engine signs a real
/// request with it.
#[tokio::test]
async fn aws_sigv4_signs_like_reference_vectors() {
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    assert_eq!(
        hex(&sigv4::signing_key(
            SECRET.as_bytes(),
            "20120215",
            "us-east-1",
            "iam"
        )),
        "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
    );
    assert_eq!(
        hex(&sigv4::signing_key(
            SECRET.as_bytes(),
            "20150830",
            "us-east-1",
            "iam"
        )),
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
    );
    let creds = Credentials {
        access_key_id: "AKIDEXAMPLE".to_string(),
        secret_access_key: Secret::new(SECRET),
        session_token: None,
    };
    let date = "20150830T123600Z";

    // IAM ListUsers.
    let sig = sigv4::sign(
        "GET",
        &"https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08"
            .parse()
            .unwrap(),
        &[
            (
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
            ("X-Amz-Date".to_string(), date.to_string()),
        ],
        b"",
        &creds,
        &SigningParams {
            region: "us-east-1",
            service: "iam",
            amz_date: date,
        },
    );
    assert_eq!(
        sig.canonical_request,
        "GET\n/\nAction=ListUsers&Version=2010-05-08\n\
         content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\n\
         x-amz-date:20150830T123600Z\n\ncontent-type;host;x-amz-date\n\
         e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        sig.string_to_sign,
        "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n\
         f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
    );
    assert_eq!(
        sig.signature,
        "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
    );
    assert_eq!(
        sig.authorization,
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
         SignedHeaders=content-type;host;x-amz-date, \
         Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
    );

    // The test suite's three plain cases, service "service".
    let suite = |method: &str, url: &str| {
        sigv4::sign(
            method,
            &url.parse().unwrap(),
            &[("X-Amz-Date".to_string(), date.to_string())],
            b"",
            &creds,
            &SigningParams {
                region: "us-east-1",
                service: "service",
                amz_date: date,
            },
        )
        .signature
    };
    assert_eq!(
        suite("GET", "https://example.amazonaws.com/"),
        "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
    );
    assert_eq!(
        suite(
            "GET",
            "https://example.amazonaws.com/?Param2=value2&Param1=value1"
        ),
        "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
    );
    assert_eq!(
        suite("POST", "https://example.amazonaws.com/"),
        "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
    );

    // X-Amz-Date from the clock.
    assert_eq!(
        sigv4::amz_date(UNIX_EPOCH + Duration::from_secs(1_440_938_160)),
        "20150830T123600Z"
    );

    // The engine signs each request with the instance's credentials, by reference.
    let api = FakeApi::start(Arc::new(|_, _| (200, vec![], json!([{"id": "x"}])))).await;
    let p = profile(json!({
        "base_url": api.base,
        "auth": {"scheme": "aws-sigv4", "service": "execute-api", "region": "eu-west-1"},
        "resources": {"items": {"list": {"method": "GET", "path": "/prod/items"}}}
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    let (ctx, _) = context(
        json!({"resource": "items", "auth": {
            "access_key_id": "aws-id", "secret_access_key": "aws-secret", "session_token": "aws-session"
        }}),
        StaticSecrets::new()
            .with("aws-id", "AKIDEXAMPLE")
            .with("aws-secret", SECRET)
            .with("aws-session", "session-token-value"),
    );
    let (mut enumerator, sources) = factory.source(&ctx).unwrap().unwrap();
    let split = enumerator.discover(&[]).await.unwrap().remove(0);
    let mut source = sources.create(&split).unwrap();
    source.open(&split, None).await.unwrap();
    let before = SystemTime::now();
    let batch = source
        .next(Budget {
            max_rows: 10,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.events.rows(), 1);
    let req = &api.seen()[0];
    let auth = &req.headers["authorization"];
    let day = &sigv4::amz_date(before)[..8];
    assert!(
        auth.starts_with(&format!(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/{day}/eu-west-1/execute-api/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token, Signature="
        )),
        "{auth}"
    );
    assert_eq!(req.headers["x-amz-security-token"], "session-token-value");
    assert!(req.headers["x-amz-date"].starts_with(day));
}

/// OAuth 2 client credentials: one token request, the token cached for the
/// requests that follow, and a refresh after a 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauth2_token_fetched_once_and_refreshed_on_401() {
    let tokens = Arc::new(Mutex::new(0u32));
    let t = tokens.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen, _| match req.path.as_str() {
        "/oauth/token" => {
            let mut n = t.lock().unwrap();
            *n += 1;
            (200, vec![], json!({"access_token": format!("at-{n}"), "expires_in": 3600, "token_type": "bearer"}))
        }
        _ => {
            // The first token is revoked after one use.
            if req.headers.get("authorization").map(String::as_str) == Some("Bearer at-1")
                && req.query.contains_key("cursor")
            {
                return (401, vec![], json!({}));
            }
            match req.query.get("cursor").map(String::as_str) {
                None => (200, vec![], json!({"items": [{"id": 1}], "next": "c2"})),
                _ => (200, vec![], json!({"items": [{"id": 2}], "next": null})),
            }
        }
    }))
    .await;
    let p = profile(json!({
        "base_url": api.base,
        "auth": {"scheme": "oauth2", "token_url": format!("{}/oauth/token", api.base), "scopes": ["crm.read"]},
        "resources": {"things": {"list": {"method": "GET", "path": "/things", "items": "/items",
                                          "cursor": {"next": "/next", "param": "cursor"}}}}
    }));
    let factory = HttpApiFactory::new(p).unwrap();
    let (ctx, _) = context(
        json!({"resource": "things", "auth": {"client_id": "cid", "client_secret": "csec"}}),
        StaticSecrets::new()
            .with("cid", "client-abc")
            .with("csec", "secret-0123456789"),
    );
    let (mut enumerator, sources) = factory.source(&ctx).unwrap().unwrap();
    let split = enumerator.discover(&[]).await.unwrap().remove(0);
    let mut source = sources.create(&split).unwrap();
    source.open(&split, None).await.unwrap();
    let budget = Budget {
        max_rows: 10,
        max_bytes: 1 << 20,
    };
    assert_eq!(source.next(budget).await.unwrap().unwrap().events.rows(), 1);
    let err = source
        .next(budget)
        .await
        .expect_err("the revoked token is refused");
    assert!(err.class.is_transient(), "{err}");
    assert_eq!(source.next(budget).await.unwrap().unwrap().events.rows(), 1);
    assert_eq!(*tokens.lock().unwrap(), 2);
    let token_req = api
        .seen()
        .into_iter()
        .find(|r| r.path == "/oauth/token")
        .unwrap();
    assert_eq!(token_req.method, "POST");
    assert_eq!(
        token_req.headers["content-type"],
        "application/x-www-form-urlencoded"
    );
}
