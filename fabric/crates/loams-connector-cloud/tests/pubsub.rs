//! CN2 plan Task 19: Google Cloud Pub/Sub over its REST API (CN2 R19.1).
//! Pull with ack after the Fabric and ack-deadline extension, CloudEvents
//! attributes (`ce-*`) passed through, publish with ordering keys. The unit
//! halves run against a fake service; the real halves against the Pub/Sub
//! emulator (`connectors-ci-only`) and a real project (`connectors-nightly`).
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use common::*;
use loams_connector_cloud::crypto;
use loams_connector_cloud::pubsub::{PubSubClient, PubSubFactory};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{AllowAll, StaticSecrets};
use serde_json::{Value, json};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
const CERT_PEM: &str = include_str!("data/test-only-cert.pem");
const KEY_PEM: &str = include_str!("data/test-only-rsa-key.pem");

#[derive(Debug, Clone)]
struct PMsg {
    id: String,
    data: Vec<u8>,
    attributes: BTreeMap<String, String>,
    ordering_key: Option<String>,
    ack_id: Option<String>,
    deadline: Instant,
    deliveries: u32,
}

#[derive(Debug, Default)]
struct PState {
    messages: Vec<PMsg>,
    next: u64,
    acked: Vec<String>,
    published: Vec<Value>,
}

#[derive(Clone)]
struct FakePubSub {
    api: FakeApi,
    state: Arc<Mutex<PState>>,
}

impl FakePubSub {
    async fn start() -> Self {
        let state: Arc<Mutex<PState>> = Arc::default();
        let s = state.clone();
        let api = FakeApi::start(Arc::new(move |seen: &Seen| pubsub(&s, seen))).await;
        Self { api, state }
    }

    fn add(&self, data: &[u8], attributes: &[(&str, &str)]) {
        let mut s = self.state.lock().unwrap();
        s.next += 1;
        let id = format!("{}", 1000 + s.next);
        s.messages.push(PMsg {
            id,
            data: data.to_vec(),
            attributes: attributes
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            ordering_key: None,
            ack_id: None,
            deadline: Instant::now(),
            deliveries: 0,
        });
    }

    fn calls(&self, suffix: &str) -> Vec<Seen> {
        self.api
            .seen()
            .into_iter()
            .filter(|s| s.path.ends_with(suffix))
            .collect()
    }

    fn with<R>(&self, f: impl FnOnce(&mut PState) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }
}

fn reply(v: Value) -> Reply {
    (
        200,
        vec![("content-type", "application/json".to_owned())],
        Bytes::from(v.to_string()),
    )
}

fn pubsub(state: &Arc<Mutex<PState>>, seen: &Seen) -> Reply {
    let req = seen.json();
    let mut s = state.lock().unwrap();
    let now = Instant::now();
    let path = seen.path.as_str();
    if path.ends_with(":pull") {
        let max = req["maxMessages"].as_u64().unwrap_or(1) as usize;
        let mut out = Vec::new();
        let mut n = s.next;
        for m in s.messages.iter_mut() {
            if out.len() >= max {
                break;
            }
            if m.deadline > now {
                continue;
            }
            n += 1;
            let ack = format!("ack-{}-{n}", m.id);
            m.ack_id = Some(ack.clone());
            m.deliveries += 1;
            m.deadline = now + Duration::from_secs(10);
            let mut msg = json!({"data": B64.encode(&m.data), "attributes": m.attributes,
                "messageId": m.id, "publishTime": "2026-10-13T00:00:00.123Z"});
            if let Some(k) = &m.ordering_key {
                msg["orderingKey"] = json!(k);
            }
            out.push(json!({"ackId": ack, "message": msg, "deliveryAttempt": m.deliveries}));
        }
        s.next = n;
        return reply(json!({"receivedMessages": out}));
    }
    if path.ends_with(":acknowledge") {
        let ids: Vec<String> = req["ackIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();
        let mut gone = Vec::new();
        s.messages.retain(|m| {
            let hit = m.ack_id.as_ref().is_some_and(|a| ids.contains(a));
            if hit {
                gone.push(m.id.clone());
            }
            !hit
        });
        s.acked.extend(gone);
        return reply(json!({}));
    }
    if path.ends_with(":modifyAckDeadline") {
        let secs = req["ackDeadlineSeconds"].as_u64().unwrap();
        for a in req["ackIds"].as_array().unwrap() {
            for m in s.messages.iter_mut() {
                if m.ack_id.as_deref() == a.as_str() {
                    m.deadline = now + Duration::from_secs(secs);
                }
            }
        }
        return reply(json!({}));
    }
    if path.ends_with(":publish") {
        let mut ids = Vec::new();
        for m in req["messages"].as_array().unwrap() {
            s.published.push(m.clone());
            s.next += 1;
            let id = format!("{}", 1000 + s.next);
            ids.push(json!(id));
            s.messages.push(PMsg {
                id,
                data: B64.decode(m["data"].as_str().unwrap_or("")).unwrap(),
                attributes: m["attributes"]
                    .as_object()
                    .map(|o| {
                        o.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                            .collect()
                    })
                    .unwrap_or_default(),
                ordering_key: m["orderingKey"].as_str().map(str::to_owned),
                ack_id: None,
                deadline: now,
                deliveries: 0,
            });
        }
        return reply(json!({"messageIds": ids}));
    }
    if path.ends_with("/token") {
        return reply(
            json!({"access_token": "ya29.test-token", "expires_in": 3600, "token_type": "Bearer"}),
        );
    }
    (404, vec![], Bytes::new())
}

fn fake_config(fake: &FakePubSub, extra: Value) -> Value {
    let mut c = json!({"project": "loams-test", "subscription": "orders-sub", "topic": "orders",
        "endpoint": fake.api.base});
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        c[k] = v;
    }
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_ack_after_fabric_and_deadline_extension() {
    let fake = FakePubSub::start().await;
    for i in 0..3 {
        fake.add(format!("{{\"n\":{i}}}").as_bytes(), &[("origin", "test")]);
    }
    let fabric = MemoryFabric::new();
    fabric.hold();
    let ctx = ctx(
        fake_config(&fake, json!({"ack_deadline_seconds": 2})),
        StaticSecrets::new(),
    );
    let p = start_source(&PubSubFactory::new(), &ctx, &fabric).await;
    eventually("pulled", || {
        fake.with(|s| s.messages.iter().all(|m| m.deliveries == 1))
    })
    .await;
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    assert!(
        fake.calls(":acknowledge").is_empty(),
        "no ack before the Fabric's"
    );
    let ext = fake.calls(":modifyAckDeadline");
    assert!(ext.len() >= 2, "deadline extended ({} calls)", ext.len());
    assert!(
        ext.iter()
            .all(|s| s.json()["ackDeadlineSeconds"] == json!(2))
    );
    assert!(
        fake.with(|s| s.messages.iter().all(|m| m.deliveries == 1)),
        "nothing redelivered while held"
    );
    fabric.release();
    eventually("acked", || fake.with(|s| s.messages.is_empty())).await;
    assert_eq!(fabric.len(), 3);
    let ev = &fabric.events()[0];
    assert_eq!(ev.ty, "io.loams.dev.flow.google-pub-sub.message.v1");
    assert_eq!(ev.id, "1001");
    assert_eq!(ev.source, "/connectors/orders/orders-sub");
    assert_eq!(ev.time.as_deref(), Some("2026-10-13T00:00:00.123Z"));
    assert_eq!(
        ev.extensions.get("origin").map(String::as_str),
        Some("test")
    );
    assert_eq!(ev.data.as_ref(), b"{\"n\":0}");
    p.stop().await;

    // Stopping with messages in flight hands them back (deadline 0).
    fake.add(b"late", &[]);
    fabric.hold();
    let p = start_source(&PubSubFactory::new(), &ctx, &fabric).await;
    eventually("pulled again", || {
        fake.with(|s| s.messages[0].deliveries == 1)
    })
    .await;
    let stopper = tokio::spawn(p.stop());
    tokio::time::sleep(Duration::from_millis(300)).await;
    fabric.release();
    let _ = stopper.await;
    // Never lost: either the Fabric got it and it was acknowledged, or it
    // was handed back with a zero deadline.
    let handed_back = fake
        .calls(":modifyAckDeadline")
        .iter()
        .any(|s| s.json()["ackDeadlineSeconds"] == json!(0));
    assert!(
        fabric.len() == 4 || handed_back,
        "a stopped source keeps or hands back what it holds"
    );

    if real(
        &["LOAMS_CONNECTORS_CI_ONLY_STACK"],
        "pubsub on the emulator",
    ) {
        real_ack_after_fabric("emulator", emulator_config(), Secrets::default()).await;
    }
    if let Some((cfg, secrets)) = nightly() {
        real_ack_after_fabric("gcp", cfg, secrets).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_ce_attributes_pass_through() {
    let fake = FakePubSub::start().await;
    fake.add(
        b"{\"i\":1}",
        &[
            ("ce-id", "e-1"),
            ("ce-source", "/test/upstream"),
            ("ce-type", "com.example.order.created.v1"),
            ("ce-specversion", "1.0"),
            ("ce-tenant", "acme"),
            ("content-type", "application/json"),
        ],
    );
    let fabric = MemoryFabric::new();
    let ctx = ctx(
        fake_config(&fake, json!({"ordering_key": "subject"})),
        StaticSecrets::new(),
    );
    let p = start_source(&PubSubFactory::new(), &ctx, &fabric).await;
    eventually("read", || fabric.len() == 1).await;
    p.stop().await;
    let ev = &fabric.events()[0];
    assert_eq!(
        (ev.id.as_str(), ev.source.as_str(), ev.ty.as_str()),
        ("e-1", "/test/upstream", "com.example.order.created.v1")
    );
    assert_eq!(
        ev.extensions.get("tenant").map(String::as_str),
        Some("acme")
    );
    assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));

    // The sink writes the same binding, with the subject as ordering key.
    let input = MemoryFabric::new();
    push_events(&input, 0, 3);
    let p = start_sink(&PubSubFactory::new(), &ctx, &input).await;
    eventually("published", || input.committed(GROUP) == 3).await;
    p.stop().await;
    let published = fake.with(|s| s.published.clone());
    assert_eq!(published.len(), 3);
    assert_eq!(published[0]["attributes"]["ce-id"], json!("e-0"));
    assert_eq!(
        published[0]["attributes"]["ce-type"],
        json!("com.example.order.created.v1")
    );
    assert_eq!(published[0]["attributes"]["ce-tenant"], json!("acme"));
    assert_eq!(
        published[0]["attributes"]["content-type"],
        json!("application/json")
    );
    assert_eq!(published[0]["orderingKey"], json!("order-0"));
    assert_eq!(
        B64.decode(published[0]["data"].as_str().unwrap()).unwrap(),
        b"{\"i\":0}"
    );

    let out = MemoryFabric::new();
    let p = start_source(&PubSubFactory::new(), &ctx, &out).await;
    eventually("round trip", || out.len() == 3).await;
    p.stop().await;
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2"]);

    if real(
        &["LOAMS_CONNECTORS_CI_ONLY_STACK"],
        "pubsub pass-through on the emulator",
    ) {
        real_roundtrip("emulator", emulator_config(), Secrets::default()).await;
    }
    if let Some((cfg, secrets)) = nightly() {
        real_roundtrip("gcp", cfg, secrets).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_account_token_is_a_signed_jwt_exchange() {
    let fake = FakePubSub::start().await;
    let creds = json!({
        "type": "service_account",
        "client_email": "loams-test-sa",
        "private_key": KEY_PEM,
        "token_uri": format!("{}/token", fake.api.base),
    });
    let secrets = StaticSecrets::new().with("gcp-sa", creds.to_string());
    let config = fake_config(&fake, json!({"auth": {"credentials_json": "gcp-sa"}}));
    let input = MemoryFabric::new();
    push_events(&input, 0, 2);
    let p = start_sink(&PubSubFactory::new(), &ctx(config, secrets), &input).await;
    eventually("published", || input.committed(GROUP) == 2).await;
    p.stop().await;
    let token_calls = fake.calls("/token");
    assert_eq!(token_calls.len(), 1, "the token is cached");
    let form = token_calls[0].form();
    assert_eq!(
        form["grant_type"],
        "urn:ietf:params:oauth:grant-type:jwt-bearer"
    );
    let jwt = &form["assertion"];
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3);
    let b64u = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header: Value = serde_json::from_slice(&b64u.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["alg"], json!("RS256"));
    let claims: Value = serde_json::from_slice(&b64u.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["iss"], json!("loams-test-sa"));
    assert_eq!(
        claims["scope"],
        json!("https://www.googleapis.com/auth/pubsub")
    );
    assert_eq!(claims["aud"], json!(format!("{}/token", fake.api.base)));
    let key = crypto::rsa_public_key_from_certificate(
        &crypto::pem_block(CERT_PEM, "CERTIFICATE").unwrap(),
    )
    .unwrap();
    ring::signature::UnparsedPublicKey::new(&ring::signature::RSA_PKCS1_2048_8192_SHA256, &key)
        .verify(
            format!("{}.{}", parts[0], parts[1]).as_bytes(),
            &b64u.decode(parts[2]).unwrap(),
        )
        .expect("the JWT verifies with the service account's key");
    let publish = fake.calls(":publish");
    assert_eq!(
        publish[0].header("authorization"),
        Some("Bearer ya29.test-token")
    );
}

// ------------------------------------------------------------ real halves --

fn emulator_config() -> Value {
    json!({"project": "loams-test", "endpoint": pubsub_endpoint()})
}

fn nightly() -> Option<(Value, Secrets)> {
    real(
        &["LOAMS_GCP_PROJECT", "LOAMS_GCP_CREDENTIALS_JSON"],
        "pubsub on a real project",
    )
    .then(|| {
        (
            json!({"project": env_or("LOAMS_GCP_PROJECT", ""),
                   "auth": {"credentials_json": "gcp-sa"}}),
            Secrets::default().with("gcp-sa", env_or("LOAMS_GCP_CREDENTIALS_JSON", "")),
        )
    })
}

/// A topic and a subscription for one run, created through the crate's own
/// client, and deleted by the returned guard's `delete`.
async fn topic_and_subscription(base: &Value, secrets: &Secrets) -> (PubSubClient, String) {
    let client = PubSubClient::from_config(
        base,
        Arc::new(secrets.clone().into_static()),
        Arc::new(AllowAll),
    )
    .unwrap();
    let name = run_name("loams-ps");
    let project = base["project"].as_str().unwrap();
    client
        .request(
            http::Method::PUT,
            &format!("projects/{project}/topics/{name}"),
            &json!({}),
        )
        .await
        .unwrap();
    client
        .request(
            http::Method::PUT,
            &format!("projects/{project}/subscriptions/{name}"),
            &json!({"topic": format!("projects/{project}/topics/{name}"),
                    "ackDeadlineSeconds": 10, "enableMessageOrdering": true}),
        )
        .await
        .unwrap();
    (client, name)
}

async fn cleanup(client: &PubSubClient, base: &Value, name: &str) {
    let project = base["project"].as_str().unwrap();
    let _ = client
        .request(
            http::Method::DELETE,
            &format!("projects/{project}/subscriptions/{name}"),
            &Value::Null,
        )
        .await;
    let _ = client
        .request(
            http::Method::DELETE,
            &format!("projects/{project}/topics/{name}"),
            &Value::Null,
        )
        .await;
}

async fn real_ack_after_fabric(label: &str, base: Value, secrets: Secrets) {
    let (client, name) = topic_and_subscription(&base, &secrets).await;
    let mut cfg = base.clone();
    cfg["topic"] = json!(name);
    cfg["subscription"] = json!(name);
    cfg["ack_deadline_seconds"] = json!(10);
    let c = ctx(cfg, secrets);
    let input = MemoryFabric::new();
    push_events(&input, 0, 3);
    let p = start_sink(&PubSubFactory::new(), &c, &input).await;
    eventually(&format!("{label}: published"), || {
        input.committed(GROUP) == 3
    })
    .await;
    p.stop().await;
    let out = MemoryFabric::new();
    out.hold();
    let p = start_source(&PubSubFactory::new(), &c, &out).await;
    // Hold past the 10 s deadline: the lease keeps the messages ours.
    tokio::time::sleep(Duration::from_secs(14)).await;
    out.release();
    eventually(&format!("{label}: three"), || out.len() == 3).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(out.duplicates(), 0, "{label}: not redelivered");
    p.stop().await;
    cleanup(&client, &base, &name).await;
}

async fn real_roundtrip(label: &str, base: Value, secrets: Secrets) {
    let (client, name) = topic_and_subscription(&base, &secrets).await;
    let mut cfg = base.clone();
    cfg["topic"] = json!(name);
    cfg["subscription"] = json!(name);
    cfg["ordering_key"] = json!("subject");
    let c = ctx(cfg, secrets);
    let input = MemoryFabric::new();
    push_events(&input, 0, 3);
    let p = start_sink(&PubSubFactory::new(), &c, &input).await;
    eventually(&format!("{label}: published"), || {
        input.committed(GROUP) == 3
    })
    .await;
    p.stop().await;
    let out = MemoryFabric::new();
    let p = start_source(&PubSubFactory::new(), &c, &out).await;
    eventually(&format!("{label}: read"), || out.len() == 3).await;
    p.stop().await;
    for ev in out.events() {
        let i: u64 = ev.id.trim_start_matches("e-").parse().unwrap();
        let want = event(i);
        assert_eq!(
            (ev.ty.clone(), ev.source.clone()),
            (want.ty, want.source),
            "{label}"
        );
        assert_eq!(
            ev.extensions.get("tenant").map(String::as_str),
            Some("acme")
        );
    }
    cleanup(&client, &base, &name).await;
}
