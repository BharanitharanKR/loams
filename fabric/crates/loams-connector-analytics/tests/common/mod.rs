//! Helpers shared by the family's integration tests.
//!
//! The request-shape and logic halves run against a local HTTP endpoint
//! that answers like the system's documented API. They never count toward a
//! suite (OR-2). The real-system halves run when the CI job's environment
//! names the system:
//!
//! | Variables | System | Job |
//! |---|---|---|
//! | `LOAMS_CONNECTORS_P2_STACK` (+ `LOAMS_TRINO_URL`, default `http://localhost:8080`) | Trino (compose `trino`) | `connectors-p2` (analytics) |
//! | `LOAMS_CONNECTORS_STACK` + `LOAMS_CLICKHOUSE_URL` (+ `_USER`, `_PASSWORD`) | ClickHouse (CN1's `connectors` profile) | `connectors-it` |
//! | `LOAMS_HOUSE_CLICKHOUSE_URL` | Loams House's ClickHouse endpoint (an extra target) | `connectors-it` |
//! | `LOAMS_REDSHIFT_*`, `LOAMS_AWS_*` (repository secrets) | Amazon Redshift | `connectors-nightly` |
//! | `LOAMS_DATABRICKS_*` (repository secrets) | Databricks | `connectors-nightly` |
//! | `LOAMS_DUCKDB_DRIVER` (+ `LOAMS_DUCKDB_DRIVER_SHA256`) | DuckDB's own `libduckdb` | any |
//!
//! Without them, the real half prints why it skipped; a skipped suite never
//! counts toward `preview`.
#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use loams_connector_sdk::{ConnectorContext, InstanceKey, Scrubber, StaticSecrets};

pub fn instance() -> InstanceKey {
    InstanceKey::new(7, "orders")
}

pub fn ctx(config: serde_json::Value, secrets: StaticSecrets) -> ConnectorContext {
    ConnectorContext::new(instance(), config, Arc::new(secrets), Scrubber::new())
}

pub fn env(v: &str) -> String {
    std::env::var(v).unwrap_or_default()
}

/// `true` when every variable is set; otherwise says why `what` skipped.
pub fn real(vars: &[&str], what: &str) -> bool {
    let missing: Vec<&str> = vars.iter().copied().filter(|v| env(v).is_empty()).collect();
    if missing.is_empty() {
        true
    } else {
        eprintln!("skipping {what}: {} not set", missing.join(", "));
        false
    }
}

/// Whether `conformance/connectors/fixtures/<id>/` holds a recording.
pub fn recorded(id: &str) -> bool {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../conformance/connectors/fixtures")
        .join(id);
    let found = std::fs::read_dir(&dir)
        .map(|d| {
            d.filter_map(Result::ok)
                .any(|e| e.path().extension().is_some_and(|x| x == "json"))
        })
        .unwrap_or(false);
    if !found {
        eprintln!(
            "skipping the {id} replay: no recording under {} (recorded from the real system by the nightly job, CN2 R17.10)",
            dir.display()
        );
    }
    found
}

/// A request the fake endpoint received.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: String,
    /// Path and query.
    pub uri: String,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

impl Seen {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    }

    /// A query parameter, percent-decoded.
    pub fn param(&self, name: &str) -> Option<String> {
        let q = self.uri.split_once('?')?.1;
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (k == name).then(|| decode(v))
        })
    }
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) =
                u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

type Handler = dyn Fn(&Seen, usize) -> Response<Bytes> + Send + Sync;

/// A local HTTP/1.1 endpoint: every request is recorded and answered by
/// `handler(request, index)`.
pub struct Fake {
    pub addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Fake {
    pub async fn start(
        handler: impl Fn(&Seen, usize) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Fake {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let handler: Arc<Handler> = Arc::new(handler);
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (log, handler) = (log.clone(), handler.clone());
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                        let (log, handler) = (log.clone(), handler.clone());
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(|b| b.to_bytes())
                                .unwrap_or_default();
                            let seen = Seen {
                                method: parts.method.to_string(),
                                uri: parts
                                    .uri
                                    .path_and_query()
                                    .map_or("/".to_owned(), |p| p.to_string()),
                                headers: parts.headers,
                                body,
                            };
                            let n = {
                                let mut l = log.lock().unwrap_or_else(|p| p.into_inner());
                                l.push(seen.clone());
                                l.len() - 1
                            };
                            let resp = handler(&seen, n);
                            Ok::<_, Infallible>(resp.map(Full::new))
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        Fake { addr, seen }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

pub fn json_reply(status: StatusCode, v: serde_json::Value) -> Response<Bytes> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Bytes::from(v.to_string()))
        .expect("response")
}

pub fn ok(v: serde_json::Value) -> Response<Bytes> {
    json_reply(StatusCode::OK, v)
}

pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
