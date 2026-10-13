//! Helpers shared by the family's integration tests.
//!
//! Each named test runs its logic against a fake service on 127.0.0.1 that
//! speaks the documented protocol (never counted toward a suite, OR-2), and
//! again against the real system when the CI job provides one:
//!
//! | Variables | System | Job |
//! |---|---|---|
//! | `LOAMS_CONNECTORS_P2_STACK` (+ `LOAMS_FLOCI_ENDPOINT`, default `http://127.0.0.1:4566`) | floci 2.1.0: SQS, SNS, EventBridge, Kinesis | `connectors-p2` (cloud) |
//! | `LOAMS_CONNECTORS_CI_ONLY_STACK` (+ `LOAMS_PUBSUB_ENDPOINT`, default `http://127.0.0.1:8085`) | the Pub/Sub emulator (`ci-service`, R0.27) | `connectors-ci-only` (cloud) |
//! | `LOAMS_AWS_ACCESS_KEY_ID`, `LOAMS_AWS_SECRET_ACCESS_KEY`, `LOAMS_AWS_REGION` | real AWS | `connectors-nightly` |
//! | `LOAMS_GCP_PROJECT`, `LOAMS_GCP_CREDENTIALS_JSON` | a real Google Cloud project | `connectors-nightly` |
//! | `LOAMS_EVENTGRID_ENDPOINT`, `LOAMS_EVENTGRID_KEY` | a real Event Grid topic | `connectors-nightly` |
//!
//! Without them, the real-system half prints why it skipped; a skipped
//! suite never counts toward `preview`.
#![allow(dead_code, clippy::unwrap_used)]

pub mod fake_sqs;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorFactory, InstanceKey, Pipeline, PipelineConfig,
    PipelineHandle, Scrubber, StaticSecrets,
};
use serde_json::Value;

pub const GROUP: &str = "flow-cloud";

pub fn instance() -> InstanceKey {
    InstanceKey::new(5, "orders")
}

pub fn ctx(config: Value, secrets: impl IntoSecrets) -> ConnectorContext {
    ConnectorContext::new(
        instance(),
        config,
        Arc::new(secrets.into_static()),
        Scrubber::new(),
    )
}

/// Secrets a test can clone (the SDK's `StaticSecrets` is not `Clone`).
#[derive(Clone, Default)]
pub struct Secrets(Vec<(String, String)>);

impl Secrets {
    pub fn with(mut self, name: &str, value: impl Into<String>) -> Self {
        self.0.push((name.to_owned(), value.into()));
        self
    }
}

pub trait IntoSecrets {
    fn into_static(self) -> StaticSecrets;
}

impl IntoSecrets for StaticSecrets {
    fn into_static(self) -> StaticSecrets {
        self
    }
}

impl IntoSecrets for Secrets {
    fn into_static(self) -> StaticSecrets {
        self.0
            .into_iter()
            .fold(StaticSecrets::new(), |s, (k, v)| s.with(k, v))
    }
}

pub fn small_config() -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: 10,
        max_batch_bytes: 256 << 10,
        linger: Duration::from_millis(5),
        max_in_flight_batches: 2,
        memory_budget_bytes: 4 << 20,
        checkpoint_interval: Duration::ZERO,
        ..PipelineConfig::default()
    }
}

pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    eventually_within(what, Duration::from_secs(60), &mut cond).await;
}

pub async fn eventually_within(what: &str, limit: Duration, cond: &mut impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while !cond() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A source pipeline over `factory`'s source, into `fabric`.
pub async fn start_source(
    factory: &dyn ConnectorFactory,
    ctx: &ConnectorContext,
    fabric: &MemoryFabric,
) -> PipelineHandle {
    let (enumerator, sources) = factory
        .source(ctx)
        .expect("source builds")
        .expect("the connector has a source");
    Pipeline::source(ctx.instance.clone(), small_config())
        .enumerator(enumerator)
        .sources(Arc::from(sources))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .dead_letters(Arc::new(fabric.clone()))
        .discover_every(Duration::from_millis(100))
        .epoch(1)
        .start()
        .await
        .expect("source pipeline starts")
}

/// A sink pipeline from `fabric`'s group [`GROUP`] into `factory`'s sink.
pub async fn start_sink(
    factory: &dyn ConnectorFactory,
    ctx: &ConnectorContext,
    fabric: &MemoryFabric,
) -> PipelineHandle {
    let sink = factory
        .sink(ctx)
        .expect("sink builds")
        .expect("the connector has a sink");
    Pipeline::sink(ctx.instance.clone(), small_config())
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::from(sink))
        .dead_letters(Arc::new(fabric.clone()))
        .epoch(1)
        .start()
        .await
        .expect("sink pipeline starts")
}

pub fn event(i: u64) -> CloudEvent {
    CloudEvent::new(
        format!("e-{i}"),
        "/test/upstream",
        "com.example.order.created.v1",
    )
    .with_subject(format!("order-{}", i % 3))
    .with_time("2026-10-13T00:00:00Z")
    .with_data("application/json", format!("{{\"i\":{i}}}"))
    .with_extension("tenant", "acme")
}

pub fn push_events(fabric: &MemoryFabric, from: u64, n: u64) {
    for i in from..from + n {
        fabric.push(event(i));
    }
}

// ------------------------------------------------------------ real systems --

/// `true` when every variable is set; otherwise says why the real half is
/// skipped.
pub fn real(vars: &[&str], what: &str) -> bool {
    let missing: Vec<&str> = vars
        .iter()
        .copied()
        .filter(|v| std::env::var(v).map_or(true, |s| s.is_empty()))
        .collect();
    if missing.is_empty() {
        true
    } else {
        eprintln!("skipping {what}: {} not set", missing.join(", "));
        false
    }
}

pub fn env_or(v: &str, default: &str) -> String {
    std::env::var(v)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

pub fn floci_endpoint() -> String {
    env_or("LOAMS_FLOCI_ENDPOINT", "http://127.0.0.1:4566")
}

pub fn pubsub_endpoint() -> String {
    env_or("LOAMS_PUBSUB_ENDPOINT", "http://127.0.0.1:8085")
}

/// A unique name per run, so reruns against a shared account do not meet.
pub fn run_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{prefix}-{nanos}")
}

/// The AWS systems a test can run against: floci in `connectors-p2`, real
/// AWS in `connectors-nightly`. Each entry is (label, instance config, secrets).
pub fn aws_targets(what: &str) -> Vec<(&'static str, Value, Secrets)> {
    let mut out = Vec::new();
    if real(&["LOAMS_CONNECTORS_P2_STACK"], &format!("{what} on floci")) {
        out.push((
            "floci",
            serde_json::json!({"region": "us-east-1", "endpoint": floci_endpoint()}),
            Secrets::default(),
        ));
    }
    if real(
        &[
            "LOAMS_AWS_ACCESS_KEY_ID",
            "LOAMS_AWS_SECRET_ACCESS_KEY",
            "LOAMS_AWS_REGION",
        ],
        &format!("{what} on AWS"),
    ) {
        out.push((
            "aws",
            serde_json::json!({
                "region": env_or("LOAMS_AWS_REGION", ""),
                "auth": {"access_key_id": "aws-key-id", "secret_access_key": "aws-secret"},
            }),
            Secrets::default()
                .with("aws-key-id", env_or("LOAMS_AWS_ACCESS_KEY_ID", ""))
                .with("aws-secret", env_or("LOAMS_AWS_SECRET_ACCESS_KEY", "")),
        ));
    }
    out
}

// ------------------------------------------------------------ fake service --

/// One request as the fake service saw it.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
    pub at: Instant,
}

impl Seen {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// The `X-Amz-Target` operation, without its service prefix.
    pub fn op(&self) -> String {
        self.header("x-amz-target")
            .and_then(|t| t.rsplit('.').next())
            .unwrap_or_default()
            .to_owned()
    }

    /// The form fields of an AWS Query request.
    pub fn form(&self) -> BTreeMap<String, String> {
        std::str::from_utf8(&self.body)
            .unwrap_or("")
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                (pct_decode(k), pct_decode(v))
            })
            .collect()
    }
}

pub fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let h =
                    u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap(), 16).unwrap();
                out.push(h);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).unwrap()
}

/// What a handler answers: status, headers and body.
pub type Reply = (u16, Vec<(&'static str, String)>, Bytes);

pub fn json_reply(status: u16, v: Value) -> Reply {
    (
        status,
        vec![("content-type", "application/x-amz-json-1.0".to_owned())],
        Bytes::from(v.to_string()),
    )
}

pub fn xml_reply(status: u16, body: &str) -> Reply {
    (
        status,
        vec![("content-type", "text/xml".to_owned())],
        Bytes::from(body.to_owned()),
    )
}

pub type Handler = Arc<dyn Fn(&Seen) -> Reply + Send + Sync>;

/// A fake HTTP service on 127.0.0.1.
#[derive(Clone)]
pub struct FakeApi {
    pub base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl FakeApi {
    pub async fn start(handler: Handler) -> Self {
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
                                let seen = Seen {
                                    method: parts.method.to_string(),
                                    path: parts.uri.path().to_owned(),
                                    query: parts.uri.query().unwrap_or("").to_owned(),
                                    headers: parts
                                        .headers
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.as_str().to_owned(),
                                                v.to_str().unwrap_or("").to_owned(),
                                            )
                                        })
                                        .collect(),
                                    body,
                                    at: Instant::now(),
                                };
                                log.lock().unwrap().push(seen.clone());
                                let (status, headers, body) = handler(&seen);
                                let mut resp = Response::builder()
                                    .status(StatusCode::from_u16(status).unwrap());
                                for (k, v) in headers {
                                    resp = resp.header(k, v);
                                }
                                Ok::<_, Infallible>(resp.body(Full::new(body)).unwrap())
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

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The requests whose AWS operation is `op`.
    pub fn ops(&self, op: &str) -> Vec<Seen> {
        self.seen().into_iter().filter(|s| s.op() == op).collect()
    }
}
