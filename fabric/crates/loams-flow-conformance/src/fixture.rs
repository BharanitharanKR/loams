//! `FixtureProxy`: record-and-replay HTTP for cloud and SaaS systems (CN2
//! Task 6).
//!
//! - **Recording** runs only against the real system: the proxy forwards
//!   each request to it and keeps the exchange, and the fixture names the
//!   system it was recorded from (`recorded_from`). Secrets are scrubbed
//!   before anything is kept: header values by name, query parameters by
//!   name, JSON body fields by JSON pointer (requests and responses), and any
//!   secret value the connector resolved, wherever it appears.
//! - **Replay** serves the recorded exchanges on loopback. A request matches
//!   an exchange by method, path (query parameters sorted and scrubbed) and
//!   canonical body (JSON with sorted keys, scrubbed); headers are not
//!   matched. An unmatched request is answered `500` and recorded, and
//!   [`ProxyHandle::finish`] fails the test with it.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use loams_connector_sdk::Scrubber;
use loams_connector_sdk::http::HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What scrubbed values are replaced with.
pub const SCRUBBED: &str = "<scrubbed>";

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A request or response body as recorded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Body {
    Empty,
    Json(Value),
    Text(String),
}

impl Body {
    fn from_bytes(b: &[u8]) -> Body {
        if b.is_empty() {
            return Body::Empty;
        }
        if let Ok(v) = serde_json::from_slice::<Value>(b) {
            return Body::Json(v);
        }
        Body::Text(String::from_utf8_lossy(b).into_owned())
    }

    fn to_bytes(&self) -> Bytes {
        match self {
            Body::Empty => Bytes::new(),
            Body::Json(v) => Bytes::from(v.to_string()),
            Body::Text(t) => Bytes::from(t.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedRequest {
    pub method: String,
    /// Path and query, as sent (scrubbed).
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    pub request: RecordedRequest,
    pub response: RecordedResponse,
}

/// A recording: one file per scenario under
/// `conformance/connectors/fixtures/<id>/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fixture {
    /// The real system the exchanges came from (OR-2). Never a mock and
    /// never a Loams module.
    pub recorded_from: String,
    /// Built by hand from the vendor's published request and response
    /// shapes (`docs`), not recorded (CN2 Task 20, R20.1). A synthetic
    /// fixture replays like a recording but never counts toward `preview`
    /// (OR-23): the `connectors-nightly` job replaces it with a recording.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub synthetic: bool,
    /// The vendor documentation a synthetic fixture was built from.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub docs: Vec<String>,
    #[serde(default)]
    pub exchanges: Vec<Exchange>,
}

/// A fixture failure.
#[derive(Debug, thiserror::Error)]
pub enum FixtureError {
    #[error("replay received requests no recording matches: {0:?}")]
    Unmatched(Vec<String>),
    #[error("{0}")]
    Io(String),
    #[error("a fixture must name the real system it was recorded from")]
    NoSystem,
}

impl Fixture {
    pub fn load(path: &Path) -> Result<Fixture, FixtureError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| FixtureError::Io(format!("{}: {e}", path.display())))?;
        let f: Fixture = serde_json::from_str(&text)
            .map_err(|e| FixtureError::Io(format!("{}: {e}", path.display())))?;
        if f.recorded_from.trim().is_empty() {
            return Err(FixtureError::NoSystem);
        }
        if f.synthetic && f.docs.is_empty() {
            return Err(FixtureError::Io(format!(
                "{}: a synthetic fixture names the vendor documentation it was built from",
                path.display()
            )));
        }
        Ok(f)
    }

    /// Whether replaying this fixture can count toward `preview` (OR-23):
    /// only a recording of the real system can.
    pub fn counts_toward_preview(&self) -> bool {
        !self.synthetic
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> Result<(), FixtureError> {
        std::fs::write(path, self.to_json() + "\n")
            .map_err(|e| FixtureError::Io(format!("{}: {e}", path.display())))
    }
}

/// What is scrubbed from a recording.
#[derive(Clone)]
pub struct ScrubRules {
    /// Header names (any case) whose values are replaced.
    pub headers: Vec<String>,
    /// Query parameter names whose values are replaced.
    pub query_params: Vec<String>,
    /// JSON pointers into request and response bodies whose values are
    /// replaced. A pointer segment `*` matches every array element.
    pub json_pointers: Vec<String>,
    /// Secret values the connector resolved; replaced wherever they appear.
    pub values: Option<Arc<Scrubber>>,
}

impl std::fmt::Debug for ScrubRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrubRules")
            .field("headers", &self.headers)
            .field("query_params", &self.query_params)
            .field("json_pointers", &self.json_pointers)
            .finish_non_exhaustive()
    }
}

impl Default for ScrubRules {
    /// The headers, parameters and fields that carry credentials in the
    /// APIs CN2 records (AWS, Azure, Google, OAuth 2.0 token endpoints,
    /// Stripe, Salesforce, Slack and the rest).
    fn default() -> Self {
        let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
        Self {
            headers: s(&[
                "authorization",
                "proxy-authorization",
                "cookie",
                "set-cookie",
                "x-api-key",
                "api-key",
                "x-amz-security-token",
                "x-goog-api-key",
                "x-ms-authorization-auxiliary",
                "ocp-apim-subscription-key",
                "x-auth-token",
                "x-vault-token",
                "private-token",
                "x-shopify-access-token",
                "stripe-signature",
                "x-hub-signature-256",
            ]),
            query_params: s(&[
                "access_token",
                "api_key",
                "apikey",
                "key",
                "token",
                "sig",
                "signature",
                "client_secret",
                "code",
                "x-amz-signature",
                "x-amz-credential",
                "x-amz-security-token",
            ]),
            json_pointers: s(&[
                "/access_token",
                "/refresh_token",
                "/id_token",
                "/client_secret",
                "/password",
                "/token",
                "/api_key",
                "/secret",
                "/credentials",
            ]),
            values: None,
        }
    }
}

impl ScrubRules {
    pub fn with_values(mut self, scrubber: Arc<Scrubber>) -> Self {
        self.values = Some(scrubber);
        self
    }

    fn text(&self, s: &str) -> String {
        match &self.values {
            Some(v) => v.scrub(s),
            None => s.to_owned(),
        }
    }

    fn headers(&self, h: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        h.iter()
            .map(|(k, v)| {
                let v = if self.headers.iter().any(|n| n.eq_ignore_ascii_case(k)) {
                    SCRUBBED.to_owned()
                } else {
                    self.text(v)
                };
                (k.to_ascii_lowercase(), v)
            })
            .collect()
    }

    /// The path with its query parameters scrubbed and sorted.
    pub fn path(&self, path: &str) -> String {
        let path = self.text(path);
        let Some((p, q)) = path.split_once('?') else {
            return path;
        };
        let mut params: Vec<String> = q
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| match kv.split_once('=') {
                Some((k, _)) if self.query_params.iter().any(|n| n.eq_ignore_ascii_case(k)) => {
                    format!("{k}={SCRUBBED}")
                }
                _ => kv.to_owned(),
            })
            .collect();
        params.sort();
        format!("{p}?{}", params.join("&"))
    }

    fn json(&self, v: &mut Value) {
        for ptr in &self.json_pointers {
            let segs: Vec<&str> = ptr.split('/').skip(1).collect();
            scrub_pointer(v, &segs);
        }
        scrub_strings(v, self);
    }

    pub fn body(&self, b: &Body) -> Body {
        match b {
            Body::Empty => Body::Empty,
            Body::Json(v) => {
                let mut v = v.clone();
                self.json(&mut v);
                Body::Json(v)
            }
            Body::Text(t) => Body::Text(self.text(t)),
        }
    }

    /// The exchange as it may be kept.
    pub fn exchange(&self, e: &Exchange) -> Exchange {
        Exchange {
            request: RecordedRequest {
                method: e.request.method.to_ascii_uppercase(),
                path: self.path(&e.request.path),
                headers: self.headers(&e.request.headers),
                body: self.body(&e.request.body),
            },
            response: RecordedResponse {
                status: e.response.status,
                headers: self.headers(&e.response.headers),
                body: self.body(&e.response.body),
            },
        }
    }

    /// The key a replayed request is matched by: method, scrubbed and
    /// sorted path, canonical scrubbed body.
    pub fn key(&self, method: &str, path: &str, body: &Body) -> String {
        let body = match self.body(body) {
            Body::Empty => String::new(),
            Body::Json(v) => canonical(&v),
            Body::Text(t) => t,
        };
        format!(
            "{} {}\n{}",
            method.to_ascii_uppercase(),
            self.path(path),
            body
        )
    }
}

fn scrub_pointer(v: &mut Value, segs: &[&str]) {
    let Some((first, rest)) = segs.split_first() else {
        if !v.is_null() {
            *v = Value::String(SCRUBBED.to_owned());
        }
        return;
    };
    match v {
        Value::Object(m) => {
            if let Some(child) = m.get_mut(*first) {
                scrub_pointer(child, rest);
            }
        }
        Value::Array(a) => {
            if *first == "*" {
                for child in a {
                    scrub_pointer(child, rest);
                }
            } else if let Some(child) = first.parse::<usize>().ok().and_then(|i| a.get_mut(i)) {
                scrub_pointer(child, rest);
            }
        }
        _ => {}
    }
}

fn scrub_strings(v: &mut Value, rules: &ScrubRules) {
    if rules.values.is_none() {
        return;
    }
    match v {
        Value::String(s) => *s = rules.text(s),
        Value::Array(a) => a.iter_mut().for_each(|c| scrub_strings(c, rules)),
        Value::Object(m) => m.values_mut().for_each(|c| scrub_strings(c, rules)),
        _ => {}
    }
}

/// JSON with object keys sorted at every level.
fn canonical(v: &Value) -> String {
    fn sort(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                let sorted: BTreeMap<&String, Value> =
                    m.iter().map(|(k, v)| (k, sort(v))).collect();
                serde_json::to_value(sorted).unwrap_or(Value::Null)
            }
            Value::Array(a) => Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    sort(v).to_string()
}

#[derive(Debug, Default)]
struct ProxyState {
    used: Vec<bool>,
    unmatched: Vec<String>,
    recorded: Vec<Exchange>,
    /// Every request a replay received, as received (in memory only, for a
    /// test's assertions on headers the matcher ignores; never saved).
    received: Vec<RecordedRequest>,
}

enum Mode {
    Replay(Fixture),
    Record {
        upstream: String,
        client: HttpClient,
    },
}

/// The proxy. See the module documentation.
#[derive(Debug)]
pub struct FixtureProxy;

impl FixtureProxy {
    /// Serve `fixture` on a loopback port.
    pub async fn replay(fixture: Fixture, rules: ScrubRules) -> Result<ProxyHandle, FixtureError> {
        if fixture.recorded_from.trim().is_empty() {
            return Err(FixtureError::NoSystem);
        }
        let from = fixture.recorded_from.clone();
        start(Mode::Replay(fixture), rules, from).await
    }

    /// Forward every request to `upstream` (the real system's base URL, no
    /// trailing slash) and record the scrubbed exchanges.
    pub async fn record(
        upstream: &str,
        recorded_from: &str,
        rules: ScrubRules,
        client: HttpClient,
    ) -> Result<ProxyHandle, FixtureError> {
        if recorded_from.trim().is_empty() {
            return Err(FixtureError::NoSystem);
        }
        let mode = Mode::Record {
            upstream: upstream.trim_end_matches('/').to_owned(),
            client,
        };
        start(mode, rules, recorded_from.to_owned()).await
    }
}

/// A running proxy.
#[derive(Debug)]
pub struct ProxyHandle {
    addr: SocketAddr,
    recorded_from: String,
    state: Arc<Mutex<ProxyState>>,
    task: tokio::task::JoinHandle<()>,
}

impl ProxyHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The base URL a connector under test is pointed at.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The requests a replay received, unscrubbed and in order. They stay
    /// in memory for the test's assertions (a signature, a content type);
    /// nothing writes them out.
    pub fn received(&self) -> Vec<RecordedRequest> {
        lock(&self.state).received.clone()
    }

    pub fn unmatched(&self) -> Vec<String> {
        lock(&self.state).unmatched.clone()
    }

    /// Stop; fail if any request went unmatched (replay).
    pub fn finish(self) -> Result<(), FixtureError> {
        self.task.abort();
        let unmatched = lock(&self.state).unmatched.clone();
        if unmatched.is_empty() {
            Ok(())
        } else {
            Err(FixtureError::Unmatched(unmatched))
        }
    }

    /// Stop recording and return the fixture (scrubbed).
    pub fn into_fixture(self) -> Fixture {
        self.task.abort();
        let exchanges = std::mem::take(&mut lock(&self.state).recorded);
        Fixture {
            recorded_from: self.recorded_from,
            synthetic: false,
            docs: Vec::new(),
            exchanges,
        }
    }
}

async fn start(mode: Mode, rules: ScrubRules, from: String) -> Result<ProxyHandle, FixtureError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| FixtureError::Io(e.to_string()))?;
    let addr = listener
        .local_addr()
        .map_err(|e| FixtureError::Io(e.to_string()))?;
    let state = Arc::new(Mutex::new(ProxyState::default()));
    if let Mode::Replay(f) = &mode {
        lock(&state).used = vec![false; f.exchanges.len()];
    }
    let shared = Arc::new((mode, rules));
    let st = state.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let shared = shared.clone();
            let st = st.clone();
            let svc = hyper::service::service_fn(move |req| {
                let shared = shared.clone();
                let st = st.clone();
                async move { Ok::<_, Infallible>(handle(req, &shared.0, &shared.1, &st).await) }
            });
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    Ok(ProxyHandle {
        addr,
        recorded_from: from,
        state,
        task,
    })
}

fn respond(status: u16, headers: &BTreeMap<String, String>, body: Bytes) -> Response<Full<Bytes>> {
    let mut b = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("content-length") || k.eq_ignore_ascii_case("transfer-encoding") {
            continue;
        }
        b = b.header(k, v);
    }
    b.body(Full::new(body))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

fn unmatched(msg: String) -> Response<Full<Bytes>> {
    let mut h = BTreeMap::new();
    h.insert("x-loams-fixture".to_owned(), "unmatched".to_owned());
    respond(500, &h, Bytes::from(msg))
}

async fn handle(
    req: Request<Incoming>,
    mode: &Mode,
    rules: &ScrubRules,
    st: &Mutex<ProxyState>,
) -> Response<Full<Bytes>> {
    let (parts, body) = req.into_parts();
    let bytes = match body.collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => return unmatched(format!("cannot read the request body: {e}")),
    };
    let method = parts.method.as_str().to_owned();
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());
    let headers: BTreeMap<String, String> = parts
        .headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|v| (k.as_str().to_owned(), v.to_owned()))
        })
        .collect();
    let req_body = Body::from_bytes(&bytes);
    match mode {
        Mode::Replay(fixture) => {
            let key = rules.key(&method, &path, &req_body);
            let mut s = lock(st);
            s.received.push(RecordedRequest {
                method: method.clone(),
                path: path.clone(),
                headers: headers.clone(),
                body: req_body.clone(),
            });
            let candidates: Vec<usize> = fixture
                .exchanges
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    rules.key(&e.request.method, &e.request.path, &e.request.body) == key
                })
                .map(|(i, _)| i)
                .collect();
            // The first unused recording, else the last one (a retry).
            let pick = candidates
                .iter()
                .copied()
                .find(|i| !s.used[*i])
                .or_else(|| candidates.last().copied());
            match pick {
                Some(i) => {
                    s.used[i] = true;
                    let r = &fixture.exchanges[i].response;
                    respond(r.status, &r.headers, r.body.to_bytes())
                }
                None => {
                    let line = format!("{} {}", method, rules.path(&path));
                    s.unmatched.push(line.clone());
                    unmatched(format!(
                        "no recording from {} matches {line}",
                        fixture.recorded_from
                    ))
                }
            }
        }
        Mode::Record { upstream, client } => {
            let mut fwd = Request::builder()
                .method(parts.method.clone())
                .uri(format!("{upstream}{path}"));
            for (k, v) in &parts.headers {
                if k == http::header::HOST {
                    continue;
                }
                fwd = fwd.header(k, v);
            }
            let Ok(fwd) = fwd.body(bytes.clone()) else {
                return unmatched("cannot build the forwarded request".to_owned());
            };
            let resp = match client.send(fwd).await {
                Ok(r) => r,
                Err(e) => return unmatched(format!("upstream: {e}")),
            };
            let (rp, rb) = resp.into_parts();
            let resp_headers: BTreeMap<String, String> = rp
                .headers
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str()
                        .ok()
                        .map(|v| (k.as_str().to_owned(), v.to_owned()))
                })
                .collect();
            let exchange = Exchange {
                request: RecordedRequest {
                    method,
                    path,
                    headers,
                    body: req_body,
                },
                response: RecordedResponse {
                    status: rp.status.as_u16(),
                    headers: resp_headers.clone(),
                    body: Body::from_bytes(&rb),
                },
            };
            lock(st).recorded.push(rules.exchange(&exchange));
            respond(rp.status.as_u16(), &resp_headers, rb)
        }
    }
}
