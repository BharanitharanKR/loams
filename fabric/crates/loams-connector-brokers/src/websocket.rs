//! WebSocket (`websocket`, RFC 6455), with `tokio-tungstenite` over the
//! SDK's egress-checked socket (TLS from `sdk::net` for `wss://`).
//!
//! - **Source:** one split, the URL. Each text or binary frame is an event:
//!   a text frame holding a structured CloudEvent passes through; any other
//!   frame becomes a `message` event (`text/plain`, `application/json` when
//!   it parses as JSON, or `application/octet-stream`). WebSocket has no
//!   acknowledgement and no replay, so the source is declared **not
//!   resumable** and at most once (`websocket_declares_not_resumable`); a
//!   reconnect starts from whatever the server sends next.
//! - **Sink:** one frame per event: text when the data is textual, else
//!   binary (`frame: auto`), or always `text`, `binary`, or the event in
//!   structured mode (`structured`). A frame written is not a frame the peer
//!   processed, so the sink is at most once too.
//! - **Auth:** `key` (an API key in a header, by secret name) and `jwt` (a
//!   bearer token, by secret name), sent on the opening handshake.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EventBatch,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator,
};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::common::{self, Declared, Env, StaticSplits, new_session, url_host_port};
use crate::mapping::{self, Incoming};
use crate::tls::TlsSettings;

pub const ID: &str = "websocket";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    /// `ws://…` or `wss://…`.
    pub url: String,
    /// Fixed, non-secret handshake headers.
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub auth: WsAuth,
    #[serde(default)]
    pub tls: TlsSettings,
    /// The largest frame accepted.
    #[serde(default = "default_max_frame")]
    pub max_frame_bytes: usize,
    #[serde(default)]
    pub source: Option<WsSource>,
    #[serde(default)]
    pub sink: Option<WsSink>,
}

/// `key`: a header carrying an API key; `jwt`: `Authorization: Bearer`.
/// Both values are secret names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsAuth {
    #[serde(default)]
    pub key_header: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub bearer: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsSource {
    /// A text frame sent after the handshake (a subscription request).
    #[serde(default)]
    pub subscribe: Option<String>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FrameMode {
    #[default]
    Auto,
    Text,
    Binary,
    Structured,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsSink {
    #[serde(default)]
    pub frame: FrameMode,
}

fn default_max_frame() -> usize {
    16 << 20
}
fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}

impl WebSocketConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let c: Self = common::parse_config(ID, config)?;
        if !(c.url.starts_with("ws://") || c.url.starts_with("wss://")) {
            return Err(ConnectorError::fatal(
                "websocket: url must be ws:// or wss://",
            ));
        }
        if common::url_has_userinfo(&c.url) {
            return Err(ConnectorError::fatal(
                "websocket: credentials go in `auth` (secret names), not in the url",
            ));
        }
        for k in c.headers.keys() {
            let lower = k.to_ascii_lowercase();
            if [
                "authorization",
                "cookie",
                "x-api-key",
                "proxy-authorization",
            ]
            .contains(&lower.as_str())
            {
                return Err(ConnectorError::fatal(format!(
                    "websocket: header {k} carries a credential; use `auth` with a secret name"
                )));
            }
        }
        if c.auth.key.is_some() != c.auth.key_header.is_some() {
            return Err(ConnectorError::fatal(
                "websocket: auth.key and auth.key_header go together",
            ));
        }
        Ok(c)
    }

    fn tls(&self) -> bool {
        self.url.starts_with("wss://")
    }
}

/// WebSocket has no acknowledgement and no replay (R18.6).
pub fn declared() -> Declared {
    Declared {
        source_resumable: false,
        source: Delivery::AtMostOnce,
        sink: Delivery::AtMostOnce,
    }
}

fn ws_err(context: &str, e: impl std::fmt::Display) -> ConnectorError {
    let text = e.to_string();
    if text.contains("401") || text.contains("403") {
        ConnectorError::auth(format!("{context}: {text}"))
    } else {
        ConnectorError::retryable(format!("{context}: {text}"))
    }
}

/// A connected socket of either kind.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Io for T {}

type Ws = WebSocketStream<Box<dyn Io>>;

async fn connect(cfg: &WebSocketConfig, env: &Env) -> Result<Ws, ConnectorError> {
    let (host, port) = url_host_port(&cfg.url, if cfg.tls() { 443 } else { 80 })?;
    let stream: Box<dyn Io> = if cfg.tls() {
        let tls = cfg.tls.client_config(&env.secrets).await?;
        Box::new(
            loams_connector_sdk::net::connect_tls(&host, port, env.egress.as_ref(), tls).await?,
        )
    } else {
        Box::new(loams_connector_sdk::net::connect(&host, port, env.egress.as_ref()).await?)
    };
    let mut req = cfg
        .url
        .as_str()
        .into_client_request()
        .map_err(|_| ConnectorError::fatal("websocket: url is not a WebSocket URL"))?;
    for (k, v) in &cfg.headers {
        let name = tokio_tungstenite::tungstenite::http::HeaderName::try_from(k.as_str())
            .map_err(|_| ConnectorError::fatal(format!("websocket: bad header name {k}")))?;
        let value = HeaderValue::try_from(v.as_str())
            .map_err(|_| ConnectorError::fatal(format!("websocket: bad value for header {k}")))?;
        req.headers_mut().insert(name, value);
    }
    if let (Some(h), Some(k)) = (&cfg.auth.key_header, &cfg.auth.key) {
        let name = tokio_tungstenite::tungstenite::http::HeaderName::try_from(h.as_str())
            .map_err(|_| ConnectorError::fatal("websocket: bad auth.key_header"))?;
        let mut value = HeaderValue::try_from(common::resolve_str(&env.secrets, k).await?)
            .map_err(|_| ConnectorError::auth("websocket: auth.key is not a header value"))?;
        value.set_sensitive(true);
        req.headers_mut().insert(name, value);
    }
    if let Some(b) = &cfg.auth.bearer {
        let token = common::resolve_str(&env.secrets, b).await?;
        let mut value = HeaderValue::try_from(format!("Bearer {token}"))
            .map_err(|_| ConnectorError::auth("websocket: auth.bearer is not a header value"))?;
        value.set_sensitive(true);
        req.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            value,
        );
    }
    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.max_frame_size = Some(cfg.max_frame_bytes);
    config.max_message_size = Some(cfg.max_frame_bytes);
    let (ws, _resp) = tokio_tungstenite::client_async_with_config(req, stream, Some(config))
        .await
        .map_err(|e| ws_err("opening the WebSocket", e))?;
    Ok(ws)
}

/// The event a frame carries.
pub fn frame_event(env: &Env, url: &str, session: u64, n: u64, m: Message) -> Option<CloudEvent> {
    let (data, ct) = match m {
        Message::Text(t) => {
            let s = t.as_str();
            let ct = if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                if let Some(ev) = env.envelope.from_structured_json(s.as_bytes()) {
                    return Some(ev);
                }
                "application/json"
            } else {
                "text/plain"
            };
            (Bytes::copy_from_slice(s.as_bytes()), ct)
        }
        Message::Binary(b) => (b, "application/octet-stream"),
        _ => return None,
    };
    Some(mapping::to_event(
        &env.envelope,
        Incoming {
            headers: Vec::new(),
            content_type: Some(ct.to_owned()),
            data,
            fallback_id: format!("{url}/{session:x}/{n}"),
            resource: "frames".to_owned(),
            subject: None,
        },
    ))
}

/// The frame for an event.
pub fn event_frame(ev: &CloudEvent, mode: FrameMode) -> Message {
    let text = |b: &Bytes| String::from_utf8(b.to_vec()).ok();
    match mode {
        FrameMode::Structured => Message::Text(
            String::from_utf8(mapping::structured_json(ev))
                .unwrap_or_default()
                .into(),
        ),
        FrameMode::Binary => Message::Binary(ev.data.clone()),
        FrameMode::Text => match text(&ev.data) {
            Some(s) => Message::Text(s.into()),
            None => Message::Binary(ev.data.clone()),
        },
        FrameMode::Auto => match (mapping::is_text(ev), text(&ev.data)) {
            (true, Some(s)) => Message::Text(s.into()),
            _ => Message::Binary(ev.data.clone()),
        },
    }
}

// ------------------------------------------------------------------ source --

pub struct WsSourceTask {
    cfg: WebSocketConfig,
    src: WsSource,
    env: Env,
    ws: Option<(Ws, u64)>,
    frames: u64,
}

impl std::fmt::Debug for WsSourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSource")
            .field("frames", &self.frames)
            .finish_non_exhaustive()
    }
}

impl WsSourceTask {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if self.ws.is_some() {
            return Ok(());
        }
        let mut ws = connect(&self.cfg, &self.env).await?;
        if let Some(sub) = &self.src.subscribe {
            ws.send(Message::Text(sub.clone().into()))
                .await
                .map_err(|e| ws_err("sending the subscription", e))?;
        }
        self.ws = Some((ws, new_session()));
        Ok(())
    }
}

#[async_trait]
impl Source for WsSourceTask {
    async fn open(&mut self, _split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        // Nothing to resume from: the server sends what it sends next.
        self.ensure().await
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.ensure().await?;
        let rows = common::take_rows(budget, self.src.max_batch);
        let mut wait = Duration::from_millis(self.src.poll_wait_ms);
        let mut events = Vec::new();
        let mut bytes = 0usize;
        while events.len() < rows {
            let (ws, session) = self.ws.as_mut().expect("ensured");
            let session = *session;
            let m = match tokio::time::timeout(wait, ws.next()).await {
                Err(_) => break,
                Ok(None) | Ok(Some(Ok(Message::Close(_)))) => {
                    self.ws = None;
                    if events.is_empty() {
                        return Err(ConnectorError::retryable(
                            "websocket: the server closed the connection",
                        ));
                    }
                    break;
                }
                Ok(Some(Err(e))) => {
                    self.ws = None;
                    return Err(ws_err("reading a frame", e));
                }
                Ok(Some(Ok(m))) => m,
            };
            wait = Duration::ZERO;
            self.frames += 1;
            let Some(ev) = frame_event(&self.env, &self.cfg.url, session, self.frames, m) else {
                continue;
            };
            let size = ev.size_bytes();
            if size > budget.max_bytes {
                return Err(ConnectorError::poison(format!(
                    "websocket: a {size}-byte frame exceeds the batch budget"
                )));
            }
            // A frame cannot be put back; the budget's bytes are a soft cap
            // here, bounded by `max_frame_bytes` beyond it.
            bytes += size;
            events.push(ev);
            if bytes >= budget.max_bytes {
                break;
            }
        }
        if events.is_empty() {
            return Ok(None);
        }
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::new(
                self.frames.to_be_bytes().to_vec(),
                format!("frame {}", self.frames),
            ),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // WebSocket has no acknowledgement.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some((mut ws, _)) = self.ws.take() {
            let _ = ws.close(None).await;
        }
        Ok(())
    }
}

struct SourceMaker {
    cfg: WebSocketConfig,
    src: WsSource,
    env: Env,
}

impl SourceFactory for SourceMaker {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(WsSourceTask {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            env: self.env.clone(),
            ws: None,
            frames: 0,
        }))
    }
}

// -------------------------------------------------------------------- sink --

pub struct WsSinkTask {
    cfg: WebSocketConfig,
    sink: WsSink,
    env: Env,
    ws: Option<Ws>,
}

impl std::fmt::Debug for WsSinkTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSink")
            .field("frame", &self.sink.frame)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for WsSinkTask {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, ID)?;
        if self.ws.is_none() {
            self.ws = Some(connect(&self.cfg, &self.env).await?);
        }
        let ws = self.ws.as_mut().expect("connected");
        let mut ack = SinkAck::default();
        for ev in &events {
            if let Err(e) = ws.feed(event_frame(ev, self.sink.frame)).await {
                self.ws = None;
                return Err(ws_err("sending a frame", e));
            }
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        if let Err(e) = ws.flush().await {
            self.ws = None;
            return Err(ws_err("sending frames", e));
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: WebSocketConfig,
    sink: WsSink,
    env: Env,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(WsSinkTask {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            ws: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::websocket::WebSocket`.
#[derive(Debug, Default, Clone, Copy)]
pub struct WebSocket;

impl ConnectorFactory for WebSocket {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = WebSocketConfig::parse(&ctx.config)?;
        let Some(src) = cfg.source.clone() else {
            return Ok(None);
        };
        Ok(Some((
            Box::new(StaticSplits::new(vec!["frames".to_owned()])),
            Box::new(SourceMaker {
                cfg,
                src,
                env: Env::new(ID, ctx),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = WebSocketConfig::parse(&ctx.config)?;
        let Some(sink) = cfg.sink.clone() else {
            return Ok(None);
        };
        Ok(Some(Box::new(SinkMaker {
            cfg,
            sink,
            env: Env::new(ID, ctx),
        })))
    }
}

pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(WebSocket)
}

#[cfg(test)]
// The handshake callback's error type is tungstenite's, not ours.
#[allow(clippy::unwrap_used, clippy::result_large_err)]
mod tests {
    use super::*;
    use loams_connector_sdk::{InstanceKey, Scrubber, StaticSecrets};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    fn ctx(config: serde_json::Value) -> ConnectorContext {
        let secrets = Arc::new(StaticSecrets::new().with("ws-token", "tok-123456"));
        ConnectorContext::new(
            InstanceKey::new(1, "feed"),
            config,
            secrets,
            Scrubber::new(),
        )
    }

    /// A WebSocket server that sends `frames`, then forwards what it
    /// receives and the handshake's Authorization header.
    async fn server(
        frames: Vec<Message>,
    ) -> (
        String,
        mpsc::UnboundedReceiver<Message>,
        mpsc::UnboundedReceiver<String>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let (htx, hrx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let cb = |req: &Request, resp: Response| {
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let _ = htx.send(auth);
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(sock, cb).await.unwrap();
            for f in frames {
                ws.send(f).await.unwrap();
            }
            while let Some(Ok(m)) = ws.next().await {
                if tx.send(m).is_err() {
                    break;
                }
            }
        });
        (format!("ws://{addr}/feed"), rx, hrx)
    }

    #[test]
    fn websocket_declares_not_resumable() {
        let d = declared();
        assert!(!d.source_resumable);
        assert_eq!(d.source, Delivery::AtMostOnce);
        assert_eq!(d.sink, Delivery::AtMostOnce);
    }

    #[tokio::test]
    async fn frames_become_events_and_cloudevents_pass_through() {
        let ce = CloudEvent::new("up-1", "/ticker", "com.example.tick.v1")
            .with_data("application/json", Bytes::from_static(b"{\"p\":1}"));
        let (url, _rx, mut auth) = server(vec![
            Message::Text("hello".into()),
            Message::Text("{\"price\": 3}".into()),
            Message::Binary(Bytes::from_static(&[1, 2, 3])),
            Message::Text(
                String::from_utf8(mapping::structured_json(&ce))
                    .unwrap()
                    .into(),
            ),
        ])
        .await;
        let c = ctx(
            json!({"url": url, "auth": {"bearer": "ws-token"}, "source": {"poll_wait_ms": 500}}),
        );
        let (mut en, f) = WebSocket.source(&c).unwrap().unwrap();
        let split = en.discover(&[]).await.unwrap().remove(0);
        let mut s = f.create(&split).unwrap();
        s.open(&split, None).await.unwrap();
        assert_eq!(auth.recv().await.unwrap(), "Bearer tok-123456");
        let mut got = Vec::new();
        while got.len() < 4 {
            let b = s
                .next(Budget {
                    max_rows: 10,
                    max_bytes: 1 << 20,
                })
                .await
                .unwrap()
                .expect("frames");
            let EventBatch::Events(v) = b.events else {
                panic!()
            };
            got.extend(v);
        }
        assert_eq!(got[0].datacontenttype.as_deref(), Some("text/plain"));
        assert_eq!(got[1].datacontenttype.as_deref(), Some("application/json"));
        assert_eq!(
            got[2].datacontenttype.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(&got[2].data[..], &[1, 2, 3]);
        assert_eq!(got[3].id, "up-1");
        assert_eq!(got[3].ty, "com.example.tick.v1");
        assert_eq!(got[0].ty, "io.loams.dev.flow.websocket.message.v1");
    }

    #[tokio::test]
    async fn the_sink_writes_one_frame_per_event() {
        let (url, mut rx, _auth) = server(vec![]).await;
        let c = ctx(json!({"url": url, "sink": {"frame": "auto"}}));
        let SinkHandle::Plain(mut sink) = WebSocket.sink(&c).unwrap().unwrap().create().unwrap()
        else {
            panic!()
        };
        let evs = vec![
            CloudEvent::new("a", "/s", "t").with_data("text/plain", Bytes::from_static(b"hi")),
            CloudEvent::new("b", "/s", "t")
                .with_data("application/octet-stream", Bytes::from_static(&[0xff, 0])),
        ];
        assert_eq!(sink.write(EventBatch::Events(evs)).await.unwrap().rows, 2);
        assert_eq!(rx.recv().await.unwrap(), Message::Text("hi".into()));
        assert_eq!(
            rx.recv().await.unwrap(),
            Message::Binary(Bytes::from_static(&[0xff, 0]))
        );
    }

    #[test]
    fn structured_frames_and_config_checks() {
        let ev = CloudEvent::new("a", "/s", "t")
            .with_data("application/json", Bytes::from_static(b"{}"));
        let Message::Text(t) = event_frame(&ev, FrameMode::Structured) else {
            panic!()
        };
        assert!(t.as_str().contains("\"id\":\"a\""));
        assert!(WebSocketConfig::parse(&json!({"url": "http://h"})).is_err());
        assert!(
            WebSocketConfig::parse(&json!({"url": "ws://h", "headers": {"Authorization": "x"}}))
                .is_err()
        );
        assert!(WebSocketConfig::parse(&json!({"url": "ws://h", "auth": {"key": "k"}})).is_err());
        WebSocketConfig::parse(&json!({
            "url": "wss://h/x", "auth": {"key_header": "X-Key", "key": "ws-key"},
            "headers": {"X-Client": "loams"}
        }))
        .unwrap();
    }
}
