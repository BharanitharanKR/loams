//! gRPC (OR-12): a dynamic client over a user-supplied `FileDescriptorSet`
//! or server reflection, with messages built by `prost-reflect`.
//!
//! The transport is a minimal gRPC-over-HTTP/2 client written here on
//! hyper's `http2` connection, over a socket `sdk::net` opened after the
//! egress check (ruling R21.2): `tonic` would dial outside that check and add
//! its own tree to `fabric/Cargo.lock`. It speaks the
//! [gRPC HTTP/2 protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md):
//! `content-type: application/grpc`, `te: trailers`, `grpc-timeout`,
//! length-prefixed messages, and `grpc-status` / `grpc-message` in the
//! trailers (or the headers of a Trailers-Only answer). TLS uses ALPN `h2`;
//! `tls.client_cert`/`tls.client_key` (secret names, PEM) give mTLS, and
//! `jwt` (a secret name) sends `authorization: Bearer`.
//!
//! * **Sink (unary calls, `sink.reply`).** Each event's JSON data (or the
//!   object at `request_pointer`) is the request message; the answer,
//!   rendered as proto3 JSON, is the reply event's data.
//! * **Source (server-streaming calls).** One split per method. The source
//!   keeps the call's stream open across `next` calls; each streamed message
//!   becomes a `message` event. With `cursor` (`request_field` set from the
//!   last message's `response_pointer`) the source is resumable: the
//!   position is that cursor and a restart re-opens the call from it, and
//!   event ids are `<method>@<cursor>`. Without one it is not resumable
//!   (`resumable: false`): the position is empty and a restart opens a new
//!   call. When the server ends the stream, the source re-opens it after
//!   `reconnect_ms`, or finishes without it.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EgressCheck,
    EnvelopeBuilder, ErrorClass, EventBatch, Position, SinkFactory, SinkHandle, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState, net,
};
use prost::Message as _;
use prost_reflect::prost_types::{FileDescriptorProto, FileDescriptorSet};
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::client::{hex, resolve, shorten};
use crate::json_proto;
use crate::reply::{Caller, Reply, ReplySink, ReplyTopics};
use crate::template::{data_json, pointer};

// ----------------------------------------------------------------- config --

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Secret names of a PEM client certificate chain and key (mTLS).
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    /// A PEM CA bundle's secret name, replacing the web PKI roots.
    #[serde(default)]
    pub ca: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    /// The request field set to the cursor (a field name or a pointer).
    pub request_field: String,
    /// Where the cursor is in each streamed message (a pointer into its
    /// proto3 JSON).
    pub response_pointer: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrpcSourceConfig {
    /// The server-streaming method, `package.Service/Method`.
    pub method: String,
    /// The request message, as proto3 JSON.
    #[serde(default = "empty_object")]
    pub request: Value,
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
    /// Re-open the call this long after the server ended it; finish
    /// without it.
    #[serde(default)]
    pub reconnect_ms: Option<u64>,
    /// How long `next` waits for more messages once it has one.
    #[serde(default = "default_linger")]
    pub linger_ms: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrpcSinkConfig {
    /// The unary method, `package.Service/Method`.
    pub method: String,
    /// Where the request is in the event's data; default the whole data.
    #[serde(default)]
    pub request_pointer: Option<String>,
}

fn empty_object() -> Value {
    json!({})
}

fn default_linger() -> u64 {
    200
}

fn default_timeout() -> u64 {
    30_000
}

/// The instance config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    /// `http://host:port` (h2c) or `https://host:port`.
    pub url: String,
    /// A `FileDescriptorSet`, base64; without it the server's reflection
    /// service is asked.
    #[serde(default)]
    pub descriptor_set: Option<String>,
    #[serde(default)]
    pub reflection: bool,
    #[serde(default)]
    pub tls: TlsConfig,
    /// A bearer JWT's secret name.
    #[serde(default)]
    pub jwt: Option<String>,
    /// Fixed, non-secret request metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub source: Option<GrpcSourceConfig>,
    #[serde(default)]
    pub sink: Option<GrpcSinkConfig>,
    #[serde(default)]
    pub reply_topic: Option<String>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

impl GrpcConfig {
    pub fn parse(v: &Value) -> Result<Self, ConnectorError> {
        let cfg: GrpcConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("grpc config: {e}")))?;
        if cfg.descriptor_set.is_none() && !cfg.reflection {
            return Err(ConnectorError::fatal(
                "grpc needs `descriptor_set` (a base64 FileDescriptorSet) or `reflection: true`",
            ));
        }
        if cfg.tls.client_cert.is_some() != cfg.tls.client_key.is_some() {
            return Err(ConnectorError::fatal(
                "tls.client_cert and tls.client_key go together",
            ));
        }
        for m in cfg
            .source
            .iter()
            .map(|s| &s.method)
            .chain(cfg.sink.iter().map(|s| &s.method))
        {
            split_method(m)?;
        }
        if cfg.url.contains('@') {
            return Err(ConnectorError::fatal("url must not carry credentials"));
        }
        Ok(cfg)
    }
}

/// `package.Service/Method` → (`package.Service`, `Method`).
pub fn split_method(m: &str) -> Result<(&str, &str), ConnectorError> {
    let m = m.trim_start_matches('/');
    m.split_once('/')
        .filter(|(s, n)| !s.is_empty() && !n.is_empty() && !n.contains('/'))
        .ok_or_else(|| ConnectorError::fatal(format!("{m:?} is not package.Service/Method")))
}

// ----------------------------------------------------------- transport --

/// `grpc-status` as a connector error (`None` for OK). `write` decides
/// whether a request the server refuses is `Poison` (a sink's bad event) or
/// `Fatal` (a source's bad request).
pub fn status_error(code: u32, message: &str, write: bool) -> Option<ConnectorError> {
    let msg = format!("grpc status {code}: {}", shorten(message, 300));
    let bad = |m: String| {
        if write {
            ConnectorError::poison(m)
        } else {
            ConnectorError::fatal(m)
        }
    };
    Some(match code {
        0 => return None,
        1 | 2 | 4 | 10 | 13 | 14 => ConnectorError::retryable(msg),
        7 | 16 => ConnectorError::auth(msg),
        8 => ConnectorError::throttled(Duration::from_secs(1), msg),
        12 | 15 => ConnectorError::fatal(msg),
        _ => bad(msg),
    })
}

/// `grpc-message` is percent-encoded.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One length-prefixed gRPC message.
pub fn frame(msg: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(5 + msg.len());
    b.put_u8(0);
    b.put_u32(msg.len() as u32);
    b.put_slice(msg);
    b.freeze()
}

/// A connection to one gRPC server.
#[derive(Clone)]
pub struct Channel {
    https: bool,
    host: String,
    port: u16,
    authority: String,
    egress: Arc<dyn EgressCheck>,
    tls: Arc<rustls::ClientConfig>,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    timeout: Duration,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

fn pem_certs(
    pem: &[u8],
    what: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, ConnectorError> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ConnectorError::fatal(format!("{what} is not PEM certificates")))
}

impl Channel {
    /// Resolve the credentials the config names and build the TLS config.
    pub async fn connect(cfg: &GrpcConfig, ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let uri: Uri = cfg
            .url
            .parse()
            .map_err(|_| ConnectorError::fatal("url is not a valid URL"))?;
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => {
                return Err(ConnectorError::fatal(
                    "url must be http:// (h2c) or https://",
                ));
            }
        };
        let host = uri
            .host()
            .ok_or_else(|| ConnectorError::fatal("url has no host"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let authority = uri
            .authority()
            .map(|a| a.as_str().to_owned())
            .unwrap_or_default();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        match &cfg.tls.ca {
            Some(name) => {
                let ca = resolve(&*ctx.secrets, name).await?;
                for c in pem_certs(ca.expose(), "tls.ca")? {
                    roots
                        .add(c)
                        .map_err(|e| ConnectorError::fatal(format!("tls.ca: {e}")))?;
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| ConnectorError::fatal(format!("TLS: {e}")))?
            .with_root_certificates(roots);
        let mut tls = match (&cfg.tls.client_cert, &cfg.tls.client_key) {
            (Some(c), Some(k)) => {
                use rustls::pki_types::pem::PemObject;
                let chain =
                    pem_certs(resolve(&*ctx.secrets, c).await?.expose(), "tls.client_cert")?;
                let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(
                    resolve(&*ctx.secrets, k).await?.expose(),
                )
                .map_err(|_| ConnectorError::fatal("tls.client_key is not a PEM private key"))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| ConnectorError::fatal(format!("mTLS: {e}")))?
            }
            _ => builder.with_no_client_auth(),
        };
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let mut headers = Vec::new();
        for (k, v) in &cfg.metadata {
            headers.push((
                HeaderName::from_bytes(k.to_ascii_lowercase().as_bytes())
                    .map_err(|_| ConnectorError::fatal(format!("metadata key {k:?}")))?,
                HeaderValue::from_str(v)
                    .map_err(|_| ConnectorError::fatal(format!("metadata {k}")))?,
            ));
        }
        if let Some(name) = &cfg.jwt {
            let token = resolve(&*ctx.secrets, name).await?;
            let mut value = b"Bearer ".to_vec();
            value.extend_from_slice(token.expose());
            let mut hv = HeaderValue::from_bytes(&value)
                .map_err(|_| ConnectorError::fatal("jwt is not a header value"))?;
            value.fill(0);
            hv.set_sensitive(true);
            headers.push((http::header::AUTHORIZATION, hv));
        }
        Ok(Self {
            https,
            host,
            port,
            authority,
            egress: ctx.egress.clone(),
            tls: Arc::new(tls),
            headers: Arc::new(headers),
            timeout: Duration::from_millis(cfg.request_timeout_ms),
        })
    }

    /// Open a call: send `body` (framed messages) to `path` and return the
    /// answer's stream.
    pub async fn call(
        &self,
        path: &str,
        body: Bytes,
        deadline: bool,
    ) -> Result<Call, ConnectorError> {
        let scheme = if self.https { "https" } else { "http" };
        let uri: Uri = format!("{scheme}://{}{path}", self.authority)
            .parse()
            .map_err(|_| ConnectorError::fatal(format!("bad method path {path}")))?;
        let mut req = Request::post(uri)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .header(http::header::USER_AGENT, "loams-flow-grpc/1");
        if deadline {
            req = req.header(
                "grpc-timeout",
                format!("{}m", self.timeout.as_millis().max(1)),
            );
        }
        for (k, v) in self.headers.iter() {
            req = req.header(k.clone(), v.clone());
        }
        let req = req
            .body(Full::new(body))
            .map_err(|e| ConnectorError::fatal(format!("building a call: {e}")))?;
        let open = async {
            if self.https {
                let s =
                    net::connect_tls(&self.host, self.port, &*self.egress, Some(self.tls.clone()))
                        .await?;
                if s.get_ref().1.alpn_protocol() != Some(b"h2") {
                    return Err(ConnectorError::fatal(format!(
                        "{} did not agree to HTTP/2 (ALPN h2)",
                        self.authority
                    )));
                }
                send(TokioIo::new(s), req).await
            } else {
                let s = net::connect(&self.host, self.port, &*self.egress).await?;
                send(TokioIo::new(s), req).await
            }
        };
        tokio::time::timeout(self.timeout, open)
            .await
            .map_err(|_| ConnectorError::retryable(format!("gRPC call {path} timed out")))?
    }

    /// A unary call: one request message, one answer message.
    pub async fn unary(
        &self,
        path: &str,
        msg: &[u8],
        write: bool,
    ) -> Result<Bytes, ConnectorError> {
        let mut call = self.call(path, frame(msg), true).await?;
        let read = async {
            let first = call.message(write).await?;
            while call.message(write).await?.is_some() {}
            first.ok_or_else(|| ConnectorError::retryable(format!("{path} answered no message")))
        };
        tokio::time::timeout(self.timeout, read)
            .await
            .map_err(|_| ConnectorError::retryable(format!("gRPC call {path} timed out")))?
    }
}

async fn send<T>(io: TokioIo<T>, req: Request<Full<Bytes>>) -> Result<Call, ConnectorError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .map_err(|e| ConnectorError::retryable(format!("HTTP/2 handshake: {e}")))?;
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| ConnectorError::retryable(format!("gRPC request: {e}")))?;
    let (parts, body) = resp.into_parts();
    if parts.status != http::StatusCode::OK {
        driver.abort();
        let code = match parts.status.as_u16() {
            400 => 13,
            401 => 16,
            403 => 7,
            404 => 12,
            429 | 502..=504 => 14,
            _ => 2,
        };
        return Err(status_error(code, &format!("HTTP {}", parts.status), false)
            .unwrap_or_else(|| ConnectorError::retryable("HTTP error")));
    }
    let ct = parts
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !ct.starts_with("application/grpc") {
        driver.abort();
        return Err(ConnectorError::fatal(format!(
            "the server is not gRPC (content-type {ct:?})"
        )));
    }
    Ok(Call {
        headers: parts.headers,
        body,
        buf: BytesMut::new(),
        done: false,
        driver,
    })
}

/// An open call's answer stream.
pub struct Call {
    headers: HeaderMap,
    body: Incoming,
    buf: BytesMut,
    done: bool,
    driver: JoinHandle<()>,
}

impl std::fmt::Debug for Call {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Call")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

fn status_of(h: &HeaderMap, write: bool) -> Option<Result<(), ConnectorError>> {
    let code: u32 = h.get("grpc-status")?.to_str().ok()?.trim().parse().ok()?;
    let msg = h
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(percent_decode)
        .unwrap_or_default();
    Some(match status_error(code, &msg, write) {
        None => Ok(()),
        Some(e) => Err(e),
    })
}

impl Call {
    /// The next message, or `None` once the call ended with status OK. This
    /// is cancel-safe: bytes read stay in the call's buffer.
    pub async fn message(&mut self, write: bool) -> Result<Option<Bytes>, ConnectorError> {
        loop {
            if self.buf.len() >= 5 {
                if self.buf[0] & 1 != 0 {
                    return Err(ConnectorError::fatal(
                        "the server sent a compressed message unasked",
                    ));
                }
                let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                    as usize;
                if self.buf.len() >= 5 + len {
                    self.buf.advance(5);
                    return Ok(Some(self.buf.split_to(len).freeze()));
                }
            }
            if self.done {
                if !self.buf.is_empty() {
                    return Err(ConnectorError::retryable(
                        "the stream ended inside a message",
                    ));
                }
                return Ok(None);
            }
            match self.body.frame().await {
                None => {
                    self.done = true;
                    // Trailers-Only: the status is in the headers.
                    match status_of(&self.headers, write) {
                        Some(r) => r?,
                        None => {
                            return Err(ConnectorError::retryable(
                                "the call ended without grpc-status",
                            ));
                        }
                    }
                }
                Some(Err(e)) => return Err(ConnectorError::retryable(format!("gRPC stream: {e}"))),
                Some(Ok(f)) => match f.into_data() {
                    Ok(d) => self.buf.extend_from_slice(&d),
                    Err(f) => {
                        if let Ok(t) = f.into_trailers() {
                            self.done = true;
                            match status_of(&t, write) {
                                Some(r) => r?,
                                None => {
                                    return Err(ConnectorError::retryable(
                                        "trailers without grpc-status",
                                    ));
                                }
                            }
                        }
                    }
                },
            }
        }
    }
}

// ----------------------------------------------------------- reflection --

#[derive(Clone, PartialEq, prost::Message)]
pub struct ReflectionRequest {
    #[prost(string, tag = "1")]
    pub host: String,
    #[prost(string, optional, tag = "3")]
    pub file_by_filename: Option<String>,
    #[prost(string, optional, tag = "4")]
    pub file_containing_symbol: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ReflectionResponse {
    #[prost(string, tag = "1")]
    pub valid_host: String,
    #[prost(message, optional, tag = "4")]
    pub file_descriptor_response: Option<FileDescriptorResponse>,
    #[prost(message, optional, tag = "7")]
    pub error_response: Option<ReflectionError>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct FileDescriptorResponse {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub file_descriptor_proto: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ReflectionError {
    #[prost(int32, tag = "1")]
    pub error_code: i32,
    #[prost(string, tag = "2")]
    pub error_message: String,
}

const REFLECTION: [&str; 2] = [
    "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
    "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
];

async fn reflect_once(
    ch: &Channel,
    path: &str,
    req: &ReflectionRequest,
) -> Result<Vec<FileDescriptorProto>, ConnectorError> {
    let answer = ch.unary(path, &req.encode_to_vec(), false).await?;
    let resp = ReflectionResponse::decode(answer)
        .map_err(|e| ConnectorError::retryable(format!("reflection answer: {e}")))?;
    if let Some(e) = resp.error_response {
        return Err(ConnectorError::fatal(format!(
            "reflection refused ({}): {}",
            e.error_code, e.error_message
        )));
    }
    resp.file_descriptor_response
        .map(|f| f.file_descriptor_proto)
        .unwrap_or_default()
        .iter()
        .map(|b| {
            FileDescriptorProto::decode(b.as_slice())
                .map_err(|e| ConnectorError::retryable(format!("a reflected file: {e}")))
        })
        .collect()
}

/// Ask the server's reflection service for `services` and every file they
/// depend on.
pub async fn reflect(ch: &Channel, services: &[&str]) -> Result<DescriptorPool, ConnectorError> {
    let mut files: HashMap<String, FileDescriptorProto> = HashMap::new();
    let mut path = REFLECTION[0];
    for (i, svc) in services.iter().enumerate() {
        let req = ReflectionRequest {
            file_containing_symbol: Some((*svc).to_owned()),
            ..Default::default()
        };
        let got = match reflect_once(ch, path, &req).await {
            Err(e) if i == 0 && e.class == ErrorClass::Fatal && e.message.contains("status 12") => {
                path = REFLECTION[1];
                reflect_once(ch, path, &req).await?
            }
            other => other?,
        };
        for f in got {
            files.insert(f.name().to_owned(), f);
        }
    }
    loop {
        let missing: Vec<String> = files
            .values()
            .flat_map(|f| f.dependency.iter().cloned())
            .filter(|d| !files.contains_key(d))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if missing.is_empty() {
            break;
        }
        for name in missing {
            let req = ReflectionRequest {
                file_by_filename: Some(name.clone()),
                ..Default::default()
            };
            let got = reflect_once(ch, path, &req).await?;
            if !got.iter().any(|f| f.name() == name) {
                return Err(ConnectorError::fatal(format!(
                    "reflection has no file {name}"
                )));
            }
            for f in got {
                files.insert(f.name().to_owned(), f);
            }
        }
    }
    pool_from(files.into_values().collect())
}

/// A pool from files in any order (dependencies first).
pub fn pool_from(files: Vec<FileDescriptorProto>) -> Result<DescriptorPool, ConnectorError> {
    let by_name: HashMap<String, FileDescriptorProto> = files
        .into_iter()
        .map(|f| (f.name().to_owned(), f))
        .collect();
    let mut ordered = Vec::with_capacity(by_name.len());
    let mut seen = HashSet::new();
    fn visit(
        name: &str,
        by_name: &HashMap<String, FileDescriptorProto>,
        seen: &mut HashSet<String>,
        out: &mut Vec<FileDescriptorProto>,
    ) {
        if !seen.insert(name.to_owned()) {
            return;
        }
        if let Some(f) = by_name.get(name) {
            for d in &f.dependency {
                visit(d, by_name, seen, out);
            }
            out.push(f.clone());
        }
    }
    let mut names: Vec<&String> = by_name.keys().collect();
    names.sort();
    for n in names {
        visit(n, &by_name, &mut seen, &mut ordered);
    }
    DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: ordered })
        .map_err(|e| ConnectorError::fatal(format!("the descriptors do not build: {e}")))
}

/// The method descriptor of `package.Service/Method`.
pub fn find_method(pool: &DescriptorPool, m: &str) -> Result<MethodDescriptor, ConnectorError> {
    let (svc, name) = split_method(m)?;
    let service = pool
        .get_service_by_name(svc)
        .ok_or_else(|| ConnectorError::fatal(format!("no service {svc}")))?;
    let method = service
        .methods()
        .find(|x| x.name() == name)
        .ok_or_else(|| ConnectorError::fatal(format!("{svc} has no method {name}")))?;
    Ok(method)
}

/// The channel and the descriptor pool of an instance, resolved once.
pub struct Resolved {
    pub channel: Channel,
    pub pool: DescriptorPool,
}

impl std::fmt::Debug for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolved")
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}

async fn resolve_all(cfg: &GrpcConfig, ctx: &ConnectorContext) -> Result<Resolved, ConnectorError> {
    let channel = Channel::connect(cfg, ctx).await?;
    let pool = match &cfg.descriptor_set {
        Some(b64) => {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(b64.trim())
                .map_err(|_| ConnectorError::fatal("descriptor_set is not base64"))?;
            let set = FileDescriptorSet::decode(raw.as_slice())
                .map_err(|e| ConnectorError::fatal(format!("descriptor_set: {e}")))?;
            pool_from(set.file)?
        }
        None => {
            let methods: Vec<&str> = cfg
                .source
                .iter()
                .map(|s| s.method.as_str())
                .chain(cfg.sink.iter().map(|s| s.method.as_str()))
                .collect();
            let services: Vec<&str> = methods
                .iter()
                .map(|m| split_method(m).map(|(s, _)| s))
                .collect::<Result<_, _>>()?;
            reflect(&channel, &services).await?
        }
    };
    Ok(Resolved { channel, pool })
}

fn method_path(m: &str) -> String {
    format!("/{}", m.trim_start_matches('/'))
}

// ------------------------------------------------------------------ sink --

/// One unary call per event.
pub struct UnaryCaller {
    cfg: Arc<GrpcConfig>,
    ctx: ConnectorContext,
    resolved: OnceCell<(Resolved, MethodDescriptor)>,
}

impl std::fmt::Debug for UnaryCaller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnaryCaller").finish_non_exhaustive()
    }
}

impl UnaryCaller {
    pub fn new(cfg: Arc<GrpcConfig>, ctx: &ConnectorContext) -> Self {
        Self {
            cfg,
            ctx: ctx.clone(),
            resolved: OnceCell::new(),
        }
    }

    async fn resolved(&self) -> Result<&(Resolved, MethodDescriptor), ConnectorError> {
        self.resolved
            .get_or_try_init(|| async {
                let sink = self
                    .cfg
                    .sink
                    .as_ref()
                    .ok_or_else(|| ConnectorError::fatal("no sink config"))?;
                let r = resolve_all(&self.cfg, &self.ctx).await?;
                let m = find_method(&r.pool, &sink.method)?;
                if m.is_client_streaming() || m.is_server_streaming() {
                    return Err(ConnectorError::fatal(format!(
                        "{} is not a unary method",
                        sink.method
                    )));
                }
                Ok((r, m))
            })
            .await
    }
}

#[async_trait]
impl Caller for UnaryCaller {
    async fn call(&self, ev: &CloudEvent) -> Result<Reply, ConnectorError> {
        let sink = self
            .cfg
            .sink
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("no sink config"))?;
        let (r, method) = self.resolved().await?;
        let data = data_json(ev)?;
        let request = match &sink.request_pointer {
            Some(p) => data.pointer(&pointer(p)).cloned().ok_or_else(|| {
                ConnectorError::poison(format!("event {}: nothing at {p}", ev.id))
            })?,
            None => data,
        };
        let msg = json_proto::from_json(&method.input(), &request)
            .map_err(|e| ConnectorError::poison(format!("event {}: {e}", ev.id)))?;
        let answer = r
            .channel
            .unary(&method_path(&sink.method), &msg.encode_to_vec(), true)
            .await?;
        let out = DynamicMessage::decode(method.output(), answer)
            .map_err(|e| ConnectorError::retryable(format!("decoding the answer: {e}")))?;
        Ok(Reply::json(sink.method.clone(), &json_proto::to_json(&out)))
    }
}

// ---------------------------------------------------------------- source --

/// The source's position.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamPosition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Value>,
}

impl StreamPosition {
    pub fn to_position(&self) -> Position {
        let display = self
            .cursor
            .as_ref()
            .map_or_else(|| "live".to_owned(), |c| format!("cursor {c}"));
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }
}

/// One split per method.
#[derive(Debug)]
pub struct MethodEnumerator {
    pub method: String,
}

#[async_trait]
impl SplitEnumerator for MethodEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(
            self.method.clone(),
            json!({ "method": self.method }),
        )])
    }
}

/// See the module documentation.
pub struct StreamSource {
    cfg: Arc<GrpcConfig>,
    ctx: ConnectorContext,
    builder: EnvelopeBuilder,
    resolved: OnceCell<(Resolved, MethodDescriptor)>,
    call: Option<Call>,
    pos: StreamPosition,
    pending: VecDeque<(CloudEvent, StreamPosition)>,
    opened_at: u128,
    seq: u64,
    wait_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for StreamSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSource")
            .field("position", &self.pos.to_position().display)
            .finish_non_exhaustive()
    }
}

impl StreamSource {
    fn src(&self) -> Result<&GrpcSourceConfig, ConnectorError> {
        self.cfg
            .source
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("no source config"))
    }

    async fn open_call(&mut self) -> Result<(), ConnectorError> {
        let src = self.src()?.clone();
        let (r, method) = self
            .resolved
            .get_or_try_init(|| async {
                let r = resolve_all(&self.cfg, &self.ctx).await?;
                let m = find_method(&r.pool, &src.method)?;
                if !m.is_server_streaming() || m.is_client_streaming() {
                    return Err(ConnectorError::fatal(format!(
                        "{} is not a server-streaming method",
                        src.method
                    )));
                }
                Ok((r, m))
            })
            .await?;
        let mut request = src.request.clone();
        if let (Some(c), Some(cursor)) = (&src.cursor, &self.pos.cursor) {
            let field = c.request_field.trim_start_matches('/');
            if let Value::Object(m) = &mut request {
                m.insert(field.to_owned(), cursor.clone());
            }
        }
        let msg = json_proto::from_json(&method.input(), &request)
            .map_err(|e| ConnectorError::fatal(format!("source.request: {e}")))?;
        self.ctx.observe.api_call(&self.ctx.instance);
        self.call = Some(
            r.channel
                .call(
                    &method_path(&src.method),
                    frame(&msg.encode_to_vec()),
                    false,
                )
                .await?,
        );
        self.opened_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        self.seq = 0;
        Ok(())
    }

    fn event(&mut self, msg: &DynamicMessage) -> (CloudEvent, StreamPosition) {
        let src = self
            .cfg
            .source
            .as_ref()
            .map(|s| (s.method.clone(), s.cursor.clone()));
        let (method, cursor_cfg) = src.unwrap_or_default();
        let j = json_proto::to_json(msg);
        let body = serde_json::to_vec(&j).unwrap_or_default();
        let cursor = cursor_cfg.and_then(|c| j.pointer(&pointer(&c.response_pointer)).cloned());
        self.seq += 1;
        let id = match &cursor {
            Some(c) => format!("{method}@{c}"),
            None => format!(
                "{method}@{}",
                hex(&Sha256::digest(format!("{}/{}", self.opened_at, self.seq).as_bytes())[..12])
            ),
        };
        let mut ev = self
            .builder
            .event("message", &method, id)
            .with_data("application/json", body);
        if let Some(c) = &cursor {
            ev = ev.with_subject(match c {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            });
        }
        let pos = if cursor.is_some() {
            StreamPosition { cursor }
        } else {
            self.pos.clone()
        };
        (ev, pos)
    }

    /// Read from the open call into `pending` until `want` events or the
    /// linger after the first one. `Ok(false)` when the call ended.
    async fn fill(&mut self, want: usize) -> Result<bool, ConnectorError> {
        let linger = Duration::from_millis(self.src()?.linger_ms);
        let (_, method) = self
            .resolved
            .get()
            .ok_or_else(|| ConnectorError::fatal("not resolved"))?;
        let output = method.output();
        let mut deadline: Option<Instant> = None;
        while self.pending.len() < want {
            let Some(call) = self.call.as_mut() else {
                return Ok(false);
            };
            let next = match deadline {
                Some(d) => match tokio::time::timeout_at(d, call.message(false)).await {
                    Ok(r) => r,
                    Err(_) => return Ok(true),
                },
                None => call.message(false).await,
            };
            match next {
                Ok(Some(bytes)) => {
                    let msg = DynamicMessage::decode(output.clone(), bytes)
                        .map_err(|e| ConnectorError::poison(format!("a streamed message: {e}")))?;
                    let item = self.event(&msg);
                    self.pending.push_back(item);
                    if deadline.is_none() {
                        deadline = Some(Instant::now() + linger);
                    }
                }
                Ok(None) => {
                    self.call = None;
                    return Ok(false);
                }
                Err(e) => {
                    self.call = None;
                    return Err(e);
                }
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl Source for StreamSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.pos = match &pos {
            Some(p) if !p.opaque.is_empty() => serde_json::from_slice(&p.opaque).map_err(|_| {
                ConnectorError::position_lost(format!(
                    "the saved position ({}) is not a gRPC cursor",
                    p.display
                ))
            })?,
            _ => StreamPosition::default(),
        };
        self.call = None;
        self.pending.clear();
        self.finished = false;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.pending.is_empty() {
                if self.finished {
                    return Ok(None);
                }
                if let Some(t) = self.wait_until.take() {
                    tokio::time::sleep_until(t).await;
                }
                if self.call.is_none() {
                    self.open_call().await?;
                }
                let live = self.fill(budget.max_rows.max(1)).await?;
                if !live && self.pending.is_empty() {
                    match self.src()?.reconnect_ms {
                        Some(ms) => {
                            self.wait_until = Some(Instant::now() + Duration::from_millis(ms))
                        }
                        None => self.finished = true,
                    }
                    continue;
                }
                if !live {
                    match self.src()?.reconnect_ms {
                        Some(ms) => {
                            self.wait_until = Some(Instant::now() + Duration::from_millis(ms))
                        }
                        None => self.finished = true,
                    }
                }
            }
            let mut events = Vec::new();
            let mut bytes = 0;
            while let Some((ev, _)) = self.pending.front() {
                let sz = ev.size_bytes();
                if events.len() >= budget.max_rows || bytes + sz > budget.max_bytes {
                    if events.is_empty() {
                        let (ev, pos) = self.pending.pop_front().unwrap_or_else(|| unreachable!());
                        self.pos = pos;
                        return Err(ConnectorError::poison(format!(
                            "message {} ({sz} bytes) exceeds the batch budget",
                            ev.id
                        )));
                    }
                    break;
                }
                bytes += sz;
                if let Some((ev, pos)) = self.pending.pop_front() {
                    self.pos = pos;
                    events.push(ev);
                }
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(events),
                position: self.pos.to_position(),
            }));
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.call = None;
        Ok(())
    }
}

struct StreamSourceFactory {
    cfg: Arc<GrpcConfig>,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for StreamSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StreamSourceFactory")
    }
}

impl SourceFactory for StreamSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(StreamSource {
            cfg: self.cfg.clone(),
            builder: EnvelopeBuilder::new("grpc", &self.ctx.instance),
            ctx: self.ctx.clone(),
            resolved: OnceCell::new(),
            call: None,
            pos: StreamPosition::default(),
            pending: VecDeque::new(),
            opened_at: 0,
            seq: 0,
            wait_until: None,
            finished: false,
        }))
    }
}

struct UnarySinkFactory {
    cfg: Arc<GrpcConfig>,
    ctx: ConnectorContext,
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for UnarySinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UnarySinkFactory")
    }
}

impl SinkFactory for UnarySinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(ReplySink::new(
            "grpc",
            UnaryCaller::new(self.cfg.clone(), &self.ctx),
            &self.ctx,
            self.cfg.reply_topic.as_deref(),
            self.replies.as_ref(),
        )?)))
    }
}

/// `runtime.ref: loams_connector_protocols::grpc`.
#[derive(Clone, Default)]
pub struct Grpc {
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for Grpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Grpc")
    }
}

impl Grpc {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_replies(mut self, replies: Arc<dyn ReplyTopics>) -> Self {
        self.replies = Some(replies);
        self
    }
}

impl ConnectorFactory for Grpc {
    fn id(&self) -> &'static str {
        "grpc"
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Arc::new(GrpcConfig::parse(&ctx.config)?);
        let Some(src) = &cfg.source else {
            return Ok(None);
        };
        Ok(Some((
            Box::new(MethodEnumerator {
                method: src.method.clone(),
            }),
            Box::new(StreamSourceFactory {
                cfg: cfg.clone(),
                ctx: ctx.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Arc::new(GrpcConfig::parse(&ctx.config)?);
        if cfg.sink.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(UnarySinkFactory {
            cfg,
            ctx: ctx.clone(),
            replies: self.replies.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_and_methods() {
        assert!(status_error(0, "", true).is_none());
        assert_eq!(
            status_error(3, "bad", true).expect("e").class,
            ErrorClass::Poison
        );
        assert_eq!(
            status_error(3, "bad", false).expect("e").class,
            ErrorClass::Fatal
        );
        assert_eq!(
            status_error(16, "", true).expect("e").class,
            ErrorClass::Auth
        );
        assert!(matches!(
            status_error(14, "", true).expect("e").class,
            ErrorClass::Retryable { .. }
        ));
        assert_eq!(split_method("/pkg.Svc/Do").expect("ok"), ("pkg.Svc", "Do"));
        assert!(split_method("pkg.Svc").is_err());
        assert_eq!(percent_decode("a%20b%zz"), "a b%zz");
        assert_eq!(&frame(b"hi")[..], &[0, 0, 0, 0, 2, b'h', b'i']);
        let no_desc = json!({"url": "http://g:50051"});
        assert!(GrpcConfig::parse(&no_desc).is_err());
    }
}
