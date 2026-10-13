//! CN2 plan Task 19: Azure Event Grid. The sink posts the CloudEvents schema
//! (SAS key or Entra ID); the webhook source completes Event Grid's
//! validation handshakes and passes CloudEvents through; a namespace topic's
//! pull delivery is a queue the Azure Blob notifications of Task 13 can read.
//! Pushes from Azure reach Loams only once Task 28 opens remote ingress, so
//! the push source runs here on requests shaped like Event Grid's and, when a
//! recording exists, on fixtures; the sink runs nightly on a real topic.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use loams_connector_cloud::event_grid::EventGridFactory;
use loams_connector_sdk::StaticSecrets;
use loams_connector_sdk::testing::memory::MemoryFabric;
use serde_json::{Value, json};

fn request(method: &str, ct: &str, body: Value) -> http::Request<Bytes> {
    http::Request::builder()
        .method(method)
        .uri("/hooks/event-grid")
        .header("content-type", ct)
        .body(Bytes::from(body.to_string()))
        .unwrap()
}

async fn webhook_source(
    config: Value,
    secrets: StaticSecrets,
) -> (
    EventGridFactory,
    MemoryFabric,
    loams_connector_sdk::PipelineHandle,
) {
    let factory = EventGridFactory::new();
    let fabric = MemoryFabric::new();
    let p = start_source(&factory, &ctx(config, secrets), &fabric).await;
    (factory, fabric, p)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventgrid_validation_handshake() {
    let (factory, fabric, p) = webhook_source(
        json!({"auth": {"webhook_key": "eg-hook"}}),
        StaticSecrets::new().with("eg-hook", "s3cret-hook-key"),
    )
    .await;
    let hook = factory.webhook(&instance()).unwrap();
    let authed = |mut r: http::Request<Bytes>| {
        *r.uri_mut() = "/hooks/event-grid?code=s3cret-hook-key".parse().unwrap();
        r
    };

    // The Event Grid schema's SubscriptionValidationEvent.
    let validation = json!([{
        "id": "2d1781af-3a4c-4d7c-bd0c-e34b19da4e66",
        "topic": "/subscriptions/x/resourceGroups/rg/providers/Microsoft.EventGrid/topics/t",
        "subject": "",
        "data": {"validationCode": "512d38b6-c7b8-40c8-89fe-f46f9e9622b6",
                 "validationUrl": "https://rp-eastus2.eventgrid.azure.net/x"},
        "eventType": "Microsoft.EventGrid.SubscriptionValidationEvent",
        "eventTime": "2018-01-25T22:12:19.4556811Z",
        "metadataVersion": "1", "dataVersion": "1"
    }]);
    let resp = hook
        .handle(authed(request(
            "POST",
            "application/json",
            validation.clone(),
        )))
        .await;
    assert_eq!(resp.status(), 200);
    let body: Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(
        body,
        json!({"validationResponse": "512d38b6-c7b8-40c8-89fe-f46f9e9622b6"})
    );

    // The CloudEvents schema's abuse-protection handshake (HTTP OPTIONS).
    let mut options = authed(request("OPTIONS", "application/json", Value::Null));
    options.headers_mut().insert(
        "webhook-request-origin",
        "eventemitter.example.com".parse().unwrap(),
    );
    let resp = hook.handle(options).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("webhook-allowed-origin").unwrap(),
        "eventemitter.example.com"
    );
    assert!(resp.headers().contains_key("webhook-allowed-rate"));

    // Without the key, nothing is validated and nothing is delivered.
    let resp = hook
        .handle(request("POST", "application/json", validation))
        .await;
    assert_eq!(resp.status(), 401);
    let mut wrong = request("POST", "application/json", json!([]));
    *wrong.uri_mut() = "/hooks/event-grid?code=nope".parse().unwrap();
    assert_eq!(hook.handle(wrong).await.status(), 401);
    assert!(fabric.is_empty(), "handshakes are not events");
    p.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventgrid_cloudevents_pass_through() {
    let (factory, fabric, p) = webhook_source(json!({}), StaticSecrets::new()).await;
    let hook = factory.webhook(&instance()).unwrap();
    let batch = json!([
        {"specversion": "1.0", "id": "ce-1", "source": "/test/upstream",
         "type": "com.example.order.created.v1", "subject": "order-1",
         "time": "2026-10-13T00:00:00Z", "datacontenttype": "application/json",
         "data": {"i": 1}, "tenant": "acme"},
        {"specversion": "1.0", "id": "ce-2", "source": "/test/upstream",
         "type": "com.example.order.created.v1", "data_base64": "AAEC"},
    ]);
    let resp = hook
        .handle(request(
            "POST",
            "application/cloudevents-batch+json; charset=utf-8",
            batch,
        ))
        .await;
    assert_eq!(resp.status(), 200);
    let single = json!({"specversion": "1.0", "id": "ce-3", "source": "/test/upstream",
        "type": "com.example.order.created.v1", "data": "text"});
    assert_eq!(
        hook.handle(request("POST", "application/cloudevents+json", single))
            .await
            .status(),
        200
    );
    // An Event Grid schema event (not a CloudEvent) becomes `event`.
    let eg = json!([{"id": "eg-1", "topic": "/subscriptions/x/topics/t", "subject": "/blobs/a.txt",
        "eventType": "Microsoft.Storage.BlobCreated", "eventTime": "2026-10-13T00:00:00Z",
        "data": {"url": "https://acct.blob.core.windows.net/c/a.txt"},
        "dataVersion": "1", "metadataVersion": "1"}]);
    assert_eq!(
        hook.handle(request("POST", "application/json", eg))
            .await
            .status(),
        200
    );
    let evs = fabric.events();
    assert_eq!(evs.len(), 4);
    let a = &evs[0];
    assert_eq!(
        (a.id.as_str(), a.source.as_str(), a.ty.as_str()),
        ("ce-1", "/test/upstream", "com.example.order.created.v1")
    );
    assert_eq!(a.data.as_ref(), b"{\"i\":1}");
    assert_eq!(a.extensions.get("tenant").map(String::as_str), Some("acme"));
    assert_eq!(evs[1].data.as_ref(), &[0, 1, 2]);
    assert_eq!(evs[2].id, "ce-3");
    let e = &evs[3];
    assert_eq!(e.ty, "io.loams.dev.flow.azure-event-grid.event.v1");
    assert_eq!(e.id, "eg-1");
    assert_eq!(e.subject.as_deref(), Some("/blobs/a.txt"));
    let data: Value = serde_json::from_slice(&e.data).unwrap();
    assert_eq!(data["eventType"], json!("Microsoft.Storage.BlobCreated"));

    // A redelivery of the same CloudEvent is the same event (ingest dedups).
    let again = json!({"specversion": "1.0", "id": "ce-3", "source": "/test/upstream",
        "type": "com.example.order.created.v1", "data": "text"});
    assert_eq!(
        hook.handle(request("POST", "application/cloudevents+json", again))
            .await
            .status(),
        200
    );
    assert_eq!(fabric.duplicates(), 1);

    // Malformed input is refused with 400, never half-delivered.
    let bad = json!([{"specversion": "1.0", "id": "x"}]);
    assert_eq!(
        hook.handle(request("POST", "application/cloudevents-batch+json", bad))
            .await
            .status(),
        400
    );
    p.stop().await;

    // Fixtures recorded from real Event Grid (Task 6's FixtureProxy) replay
    // here once a recording exists (R13.5's precedent: none is fabricated).
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../conformance/connectors/fixtures/azure-event-grid");
    if !dir.join("deliveries.json").exists() {
        eprintln!("skipping the Event Grid fixture replay: no recording yet");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventgrid_sink_posts_cloudevents_batches() {
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let s = seen.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen| {
        s.lock().unwrap().push(req.clone());
        (200, vec![], Bytes::new())
    }))
    .await;
    let input = MemoryFabric::new();
    push_events(&input, 0, 3);
    let config = json!({"endpoint": format!("{}/api/events?api-version=2018-01-01", api.base),
        "auth": {"key": "eg-key"}});
    let secrets = StaticSecrets::new().with("eg-key", "topic-key-value");
    let p = start_sink(&EventGridFactory::new(), &ctx(config, secrets), &input).await;
    eventually("posted", || input.committed(GROUP) == 3).await;
    p.stop().await;
    let calls = seen.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let c = &calls[0];
    assert_eq!(c.path, "/api/events");
    assert_eq!(c.query, "api-version=2018-01-01");
    assert_eq!(
        c.header("content-type"),
        Some("application/cloudevents-batch+json; charset=utf-8")
    );
    assert_eq!(c.header("aeg-sas-key"), Some("topic-key-value"));
    let body = c.json();
    assert_eq!(body.as_array().unwrap().len(), 3);
    assert_eq!(body[0]["id"], json!("e-0"));
    assert_eq!(body[0]["type"], json!("com.example.order.created.v1"));
    assert_eq!(body[0]["data"], json!({"i": 0}));
    assert_eq!(body[0]["tenant"], json!("acme"));

    // Entra ID: a client-credentials token, cached, as a bearer.
    let tokens: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let t = tokens.clone();
    let posts: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let po = posts.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen| {
        if req.path.ends_with("/oauth2/v2.0/token") {
            t.lock().unwrap().push(req.clone());
            return (
                200,
                vec![("content-type", "application/json".to_owned())],
                Bytes::from(json!({"access_token": "entra-token", "expires_in": 3600}).to_string()),
            );
        }
        po.lock().unwrap().push(req.clone());
        (200, vec![], Bytes::new())
    }))
    .await;
    let input = MemoryFabric::new();
    push_events(&input, 0, 2);
    let config = json!({"endpoint": format!("{}/api/events", api.base),
        "auth": {"tenant_id": "tenant-1", "client_id": "eg-client", "client_secret": "eg-secret",
                 "authority": api.base}});
    let secrets = StaticSecrets::new()
        .with("eg-client", "client-id-value")
        .with("eg-secret", "client-secret-value");
    let p = start_sink(&EventGridFactory::new(), &ctx(config, secrets), &input).await;
    eventually("posted", || input.committed(GROUP) == 2).await;
    p.stop().await;
    let tok = tokens.lock().unwrap().clone();
    assert_eq!(tok.len(), 1);
    assert_eq!(tok[0].path, "/tenant-1/oauth2/v2.0/token");
    let form = tok[0].form();
    assert_eq!(form["grant_type"], "client_credentials");
    assert_eq!(form["scope"], "https://eventgrid.azure.net/.default");
    assert_eq!(
        posts.lock().unwrap()[0].header("authorization"),
        Some("Bearer entra-token")
    );

    if real(
        &["LOAMS_EVENTGRID_ENDPOINT", "LOAMS_EVENTGRID_KEY"],
        "the Event Grid sink on a real topic",
    ) {
        let input = MemoryFabric::new();
        push_events(&input, 0, 3);
        let config =
            json!({"endpoint": env_or("LOAMS_EVENTGRID_ENDPOINT", ""), "auth": {"key": "eg-key"}});
        let secrets = StaticSecrets::new().with("eg-key", env_or("LOAMS_EVENTGRID_KEY", ""));
        let p = start_sink(&EventGridFactory::new(), &ctx(config, secrets), &input).await;
        eventually("accepted by Event Grid", || input.committed(GROUP) == 3).await;
        assert!(input.dead_letters().is_empty());
        p.stop().await;
    }
}

/// The fake namespace: (lock token, event) waiting, tokens acknowledged,
/// receive calls.
type PullState = (Vec<(String, Value)>, Vec<String>, u32);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eventgrid_namespace_pull_acks_after_fabric() {
    let state: Arc<Mutex<PullState>> = Arc::new(Mutex::new((
        (0..3)
            .map(|i| {
                (
                    format!("lock-{i}"),
                    json!({"specversion": "1.0", "id": format!("n-{i}"), "source": "/ns/topic",
                           "type": "com.example.ping", "data": {"i": i}}),
                )
            })
            .collect(),
        Vec::new(),
        0,
    )));
    let st = state.clone();
    let api = FakeApi::start(Arc::new(move |req: &Seen| {
        let mut s = st.lock().unwrap();
        assert_eq!(req.header("authorization"), Some("SharedAccessKey ns-key-value"));
        if req.path.ends_with(":receive") {
            let first = s.2 == 0;
            s.2 += 1;
            let value: Vec<Value> = if first {
                s.0.iter()
                    .map(|(l, e)| json!({"brokerProperties": {"lockToken": l, "deliveryCount": 1}, "event": e}))
                    .collect()
            } else {
                Vec::new()
            };
            return json_reply(200, json!({"value": value}));
        }
        if req.path.ends_with(":acknowledge") {
            let toks: Vec<String> = req.json()["lockTokens"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            s.1.extend(toks.iter().cloned());
            return json_reply(200, json!({"succeededLockTokens": toks, "failedLockTokens": []}));
        }
        if req.path.ends_with(":renewLock") {
            return json_reply(200, json!({"succeededLockTokens": [], "failedLockTokens": []}));
        }
        json_reply(404, json!({}))
    }))
    .await;
    let fabric = MemoryFabric::new();
    fabric.hold();
    let config = json!({"delivery": "pull", "namespace_endpoint": api.base, "topic": "orders",
        "subscription": "loams", "lock_seconds": 2, "auth": {"key": "ns-key"}});
    let secrets = StaticSecrets::new().with("ns-key", "ns-key-value");
    let p = start_source(&EventGridFactory::new(), &ctx(config, secrets), &fabric).await;
    eventually("received", || state.lock().unwrap().2 >= 1).await;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert!(
        state.lock().unwrap().1.is_empty(),
        "no ack before the Fabric's"
    );
    let renewals = api
        .seen()
        .into_iter()
        .filter(|s| s.path.ends_with(":renewLock"))
        .count();
    assert!(renewals >= 1, "locks renewed while held");
    let r = api
        .seen()
        .into_iter()
        .find(|s| s.path.ends_with(":receive"))
        .unwrap();
    assert_eq!(r.path, "/topics/orders/eventsubscriptions/loams:receive");
    assert!(r.query.contains("api-version=2024-06-01"));
    fabric.release();
    eventually("acked", || state.lock().unwrap().1.len() == 3).await;
    let mut ids = fabric.ids();
    ids.sort();
    assert_eq!(ids, vec!["n-0", "n-1", "n-2"]);
    p.stop().await;
}
