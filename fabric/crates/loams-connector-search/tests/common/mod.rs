//! Shared test support: a fake HTTP API on loopback that records every
//! request and answers from a closure, the stack gates, and small builders.
//!
//! The fake answers only what each test scripts; it stands in for nothing in
//! a conformance suite (OR-2): the suites run against the real systems of
//! `deploy/fabric/compose.yaml`, behind the gates below.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use loams_connector_sdk::{CloudEvent, ConnectorContext, InstanceKey, Scrubber, StaticSecrets};

/// One request the fake received.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// Path and query.
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
}

impl Recorded {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or("")
    }

    /// The NDJSON lines of a `_bulk` body.
    pub fn ndjson(&self) -> Vec<serde_json::Value> {
        String::from_utf8_lossy(&self.body)
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("an NDJSON line"))
            .collect()
    }
}

type Handler = dyn Fn(&Recorded, usize) -> (u16, Vec<(String, String)>, String) + Send + Sync;

/// A fake API: `handler(request, index)` gives the status, headers and body;
/// `index` counts requests from 0.
#[derive(Clone)]
pub struct Fake {
    pub addr: SocketAddr,
    pub requests: Arc<Mutex<Vec<Recorded>>>,
}

impl Fake {
    pub async fn start(
        handler: impl Fn(&Recorded, usize) -> (u16, Vec<(String, String)>, String)
        + Send
        + Sync
        + 'static,
    ) -> Fake {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let requests: Arc<Mutex<Vec<Recorded>>> = Arc::default();
        let handler: Arc<Handler> = Arc::new(handler);
        let reqs = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let reqs = reqs.clone();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let reqs = reqs.clone();
                        let handler = handler.clone();
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(|b| b.to_bytes())
                                .unwrap_or_default();
                            let rec = Recorded {
                                method: parts.method.to_string(),
                                target: parts
                                    .uri
                                    .path_and_query()
                                    .map_or("/".to_owned(), |p| p.to_string()),
                                headers: parts
                                    .headers
                                    .iter()
                                    .map(|(k, v)| {
                                        (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned())
                                    })
                                    .collect(),
                                body,
                            };
                            let index = {
                                let mut r = reqs.lock().expect("lock");
                                r.push(rec.clone());
                                r.len() - 1
                            };
                            let (status, headers, body) = handler(&rec, index);
                            let mut resp = Response::builder()
                                .status(status)
                                .header("content-type", "application/json");
                            for (k, v) in headers {
                                resp = resp.header(k, v);
                            }
                            Ok::<_, Infallible>(
                                resp.body(Full::new(Bytes::from(body))).expect("response"),
                            )
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Fake { addr, requests }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("lock").clone()
    }

    /// The requests whose path ends with `suffix`.
    pub fn to(&self, suffix: &str) -> Vec<Recorded> {
        self.requests()
            .into_iter()
            .filter(|r| r.path().ends_with(suffix))
            .collect()
    }
}

/// A 200 with a JSON body and no extra headers.
pub fn ok(body: serde_json::Value) -> (u16, Vec<(String, String)>, String) {
    (200, Vec::new(), body.to_string())
}

/// A connector context over `config`, with `secrets`.
pub fn ctx(config: serde_json::Value, secrets: StaticSecrets) -> ConnectorContext {
    ConnectorContext::new(
        InstanceKey::new(7, "search-test"),
        config,
        Arc::new(secrets),
        Scrubber::new(),
    )
}

/// A JSON document event.
pub fn doc_event(id: &str, data: serde_json::Value) -> CloudEvent {
    CloudEvent::new(id, "/test", "io.loams.dev.flow.test.document.v1")
        .with_data("application/json", serde_json::to_vec(&data).expect("json"))
}

/// Whether a compose stack is up (Global Constraints: tests skip without
/// services). Prints why a test skipped.
pub fn stack_up(var: &str, test: &str) -> bool {
    if std::env::var(var).is_ok_and(|v| !v.is_empty()) {
        true
    } else {
        eprintln!("{test}: skipped, {var} is not set (the service is not up)");
        false
    }
}

/// A variable, or a default (the compose estate's fixed test values).
pub fn env_or(var: &str, default: &str) -> String {
    std::env::var(var)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}
