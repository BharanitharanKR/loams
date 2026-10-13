//! NATS (`nats`), with `async-nats`.
//!
//! - **JetStream source** (`source.mode: jetstream`): a durable pull
//!   consumer with `ack_policy: explicit`. A batch is fetched within the
//!   budget (`max_messages`, `max_bytes`), and its messages are acknowledged
//!   after the Fabric acknowledges them, the last one double-acked so the
//!   server has confirmed the batch. (A cumulative `all` policy could cover a
//!   message lost to a cancelled fetch.) The durable consumer keeps the ack
//!   floor on the server, so a restart resumes there (`resumable: true`), and
//!   event ids are `<stream>/<stream sequence>`, stable across redeliveries.
//!   `max_ack_pending` is the pipeline's budget, so a slow Fabric stops
//!   delivery.
//! - **Core source** (`source.mode: core`): plain subscriptions. Core NATS
//!   keeps nothing for a subscriber that is not connected, so this mode is
//!   declared **not resumable** and at most once (`nats_core_declares_not_resumable`).
//! - **JetStream sink:** publishes with `Nats-Msg-Id` = the event id and
//!   waits for each publish ack; the stream drops a repeated id within its
//!   duplicate window, so the sink is `effectively_once` there
//!   (`nats_jetstream_resumes_and_dedupes_msg_id`). **Core sink:**
//!   publish and flush, at most once.
//! - CloudEvents: `ce_` headers both ways.
//! - Egress: every configured server is checked before the client is built,
//!   and the client ignores servers the cluster advertises (R18.4).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{self, consumer::AckPolicy, consumer::pull};
use async_nats::{Client, ConnectOptions, HeaderMap};
use async_trait::async_trait;
use futures::StreamExt;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EventBatch,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator,
};
use serde::Deserialize;

use crate::common::{
    self, Declared, Env, Pending, StaticSplits, egress_precheck, new_session,
    parse_session_position, session_position, split_name, url_has_userinfo, url_host_port,
};
use crate::mapping::{self, Incoming};
use crate::tls::TlsSettings;

pub const ID: &str = "nats";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// `nats://host:4222` or `tls://host:4222`, without credentials.
    pub servers: Vec<String>,
    #[serde(default)]
    pub auth: NatsAuth,
    #[serde(default)]
    pub tls: TlsSettings,
    #[serde(default)]
    pub source: Option<NatsSource>,
    #[serde(default)]
    pub sink: Option<NatsSink>,
}

/// Secret names for the manifest's `key` (an nkey seed) and `jwt` (a user
/// credentials file's contents); `mtls` is `tls.cert_pem` with `tls.key_pem`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsAuth {
    #[serde(default)]
    pub nkey_seed: Option<String>,
    #[serde(default)]
    pub credentials: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Jetstream,
    Core,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsSource {
    #[serde(default)]
    pub mode: Mode,
    /// JetStream: the stream.
    #[serde(default)]
    pub stream: Option<String>,
    /// JetStream: the durable consumer (default `loams-<instance>`).
    #[serde(default)]
    pub durable: Option<String>,
    /// JetStream: a filter subject.
    #[serde(default)]
    pub filter_subject: Option<String>,
    /// JetStream: seconds before an unacknowledged message is redelivered.
    #[serde(default = "default_ack_wait_s")]
    pub ack_wait_s: u64,
    /// JetStream: `max_ack_pending`; by default the pipeline's budget.
    #[serde(default)]
    pub max_ack_pending: Option<u32>,
    /// Core: one split per subject.
    #[serde(default)]
    pub subjects: Vec<String>,
    /// Core: a queue group.
    #[serde(default)]
    pub queue_group: Option<String>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsSink {
    #[serde(default)]
    pub mode: Mode,
    /// A fixed subject; without it, the event's `subject`.
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
}

fn default_ack_wait_s() -> u64 {
    30
}
fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}
fn default_ack_timeout_ms() -> u64 {
    10_000
}

impl NatsConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let c: Self = common::parse_config(ID, config)?;
        if c.servers.is_empty() {
            return Err(ConnectorError::fatal("nats: servers is empty"));
        }
        for s in &c.servers {
            if url_has_userinfo(s) {
                return Err(ConnectorError::fatal(
                    "nats: credentials go in `auth` (secret names), not in a server URL",
                ));
            }
        }
        if let Some(s) = &c.source {
            match s.mode {
                Mode::Jetstream if s.stream.is_none() => {
                    return Err(ConnectorError::fatal(
                        "nats: a JetStream source needs source.stream",
                    ));
                }
                Mode::Core if s.subjects.is_empty() => {
                    return Err(ConnectorError::fatal(
                        "nats: a core source needs source.subjects",
                    ));
                }
                _ => {}
            }
        }
        Ok(c)
    }
}

/// What a NATS config delivers (R18.6). The manifest declares the JetStream
/// values; a core-mode source or sink delivers less, and says so here.
pub fn declared(config: &serde_json::Value) -> Result<Declared, ConnectorError> {
    let c = NatsConfig::parse(config)?;
    let source_mode = c.source.as_ref().map(|s| s.mode).unwrap_or_default();
    let sink_mode = c.sink.as_ref().map(|s| s.mode).unwrap_or_default();
    Ok(Declared {
        source_resumable: source_mode == Mode::Jetstream,
        source: match source_mode {
            Mode::Jetstream => Delivery::AtLeastOnce,
            Mode::Core => Delivery::AtMostOnce,
        },
        sink: match sink_mode {
            Mode::Jetstream => Delivery::EffectivelyOnce,
            Mode::Core => Delivery::AtMostOnce,
        },
    })
}

fn nats_err(context: &str, e: impl std::fmt::Display) -> ConnectorError {
    let text = e.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("authorization")
        || lower.contains("authentication")
        || lower.contains("nonce")
    {
        ConnectorError::auth(format!("{context}: {text}"))
    } else if lower.contains("not found") {
        ConnectorError::fatal(format!("{context}: {text}"))
    } else {
        ConnectorError::retryable(format!("{context}: {text}"))
    }
}

async fn connect(cfg: &NatsConfig, env: &Env, name: &str) -> Result<Client, ConnectorError> {
    let mut tls = false;
    for s in &cfg.servers {
        let s_url = if s.contains("://") {
            s.clone()
        } else {
            format!("nats://{s}")
        };
        tls |= s_url.starts_with("tls://");
        let (host, port) = url_host_port(&s_url, 4222)?;
        egress_precheck(&host, port, env.egress.as_ref()).await?;
    }
    let mut opts = ConnectOptions::new()
        .name(name)
        .ignore_discovered_servers()
        .connection_timeout(Duration::from_secs(10));
    if let Some(seed) = &cfg.auth.nkey_seed {
        opts = opts.nkey(common::resolve_str(&env.secrets, seed).await?);
    }
    if let Some(creds) = &cfg.auth.credentials {
        let text = common::resolve_str(&env.secrets, creds).await?;
        opts = opts.credentials(&text).map_err(|_| {
            ConnectorError::auth("nats: auth.credentials is not a credentials file")
        })?;
    }
    if let Some(tls_config) = cfg.tls.client_config(&env.secrets).await? {
        opts = opts
            .tls_client_config((*tls_config).clone())
            .require_tls(true);
    } else if tls {
        opts = opts.require_tls(true);
    }
    opts.connect(cfg.servers.clone())
        .await
        .map_err(|e| nats_err("connecting to NATS", e))
}

fn header_list(h: Option<&HeaderMap>) -> (Vec<(String, String)>, Option<String>) {
    let mut out = Vec::new();
    let mut ct = None;
    if let Some(h) = h {
        for (k, vs) in h.iter() {
            let name = k.to_string();
            if let Some(v) = vs.last() {
                if name.eq_ignore_ascii_case("content-type") {
                    ct = Some(v.as_str().to_owned());
                } else {
                    out.push((name, v.as_str().to_owned()));
                }
            }
        }
    }
    (out, ct)
}

/// The NATS headers of an event: `ce_` attributes and `Content-Type`.
pub fn headers(ev: &CloudEvent) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in mapping::binary_headers(ev, "ce_") {
        h.insert(k.as_str(), v.as_str());
    }
    h.insert("Content-Type", mapping::content_type(ev));
    h
}

// ------------------------------------------------------------------ source --

/// A fetched message with, for JetStream, its stream sequence and acker.
type Fetched = (
    async_nats::Message,
    Option<(u64, jetstream::message::Acker)>,
);

enum Reader {
    Js {
        consumer: Box<jetstream::consumer::Consumer<pull::Config>>,
    },
    Core {
        sub: async_nats::Subscriber,
    },
}

struct Live {
    _client: Client,
    reader: Reader,
    session: u64,
}

pub struct NatsSourceTask {
    cfg: NatsConfig,
    src: NatsSource,
    window: u32,
    env: Env,
    instance: String,
    split: String,
    live: Option<Live>,
    /// JetStream's ackers by sequence (`None` for core messages).
    pending: Pending<Option<jetstream::message::Acker>>,
    /// Messages fetched but not yet handed out (they did not fit a budget).
    held: VecDeque<Fetched>,
    counter: u64,
}

impl std::fmt::Debug for NatsSourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSource")
            .field("split", &self.split)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl NatsSourceTask {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if self.live.is_some() {
            return Ok(());
        }
        self.pending.clear();
        self.held.clear();
        let client = connect(&self.cfg, &self.env, &format!("loams-{}", self.instance)).await?;
        let reader = match self.src.mode {
            Mode::Jetstream => {
                let js = jetstream::new(client.clone());
                let stream_name = self.src.stream.clone().unwrap_or_default();
                let stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|e| nats_err("looking up the stream", e))?;
                let durable = self
                    .src
                    .durable
                    .clone()
                    .unwrap_or_else(|| format!("loams-{}", self.instance));
                let consumer = stream
                    .get_or_create_consumer(
                        &durable,
                        pull::Config {
                            durable_name: Some(durable.clone()),
                            ack_policy: AckPolicy::Explicit,
                            ack_wait: Duration::from_secs(self.src.ack_wait_s.max(1)),
                            max_ack_pending: i64::from(self.window),
                            filter_subject: self.src.filter_subject.clone().unwrap_or_default(),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| nats_err("creating the durable consumer", e))?;
                Reader::Js {
                    consumer: Box::new(consumer),
                }
            }
            Mode::Core => {
                let sub = match &self.src.queue_group {
                    Some(g) => client
                        .queue_subscribe(self.split.clone(), g.clone())
                        .await
                        .map_err(|e| nats_err("subscribing", e))?,
                    None => client
                        .subscribe(self.split.clone())
                        .await
                        .map_err(|e| nats_err("subscribing", e))?,
                };
                Reader::Core { sub }
            }
        };
        self.live = Some(Live {
            _client: client,
            reader,
            session: new_session(),
        });
        Ok(())
    }

    /// Up to `rows` messages, waiting at most `wait` for the first.
    async fn pull(
        &mut self,
        rows: usize,
        max_bytes: usize,
        wait: Duration,
    ) -> Result<Vec<Fetched>, ConnectorError> {
        let live = self.live.as_mut().expect("ensured");
        let mut out = Vec::new();
        match &mut live.reader {
            Reader::Js { consumer } => {
                let mut batch = consumer
                    .fetch()
                    .max_messages(rows)
                    .max_bytes(max_bytes)
                    .expires(wait.max(Duration::from_millis(10)))
                    .messages()
                    .await
                    .map_err(|e| nats_err("fetching", e))?;
                while let Some(m) = batch.next().await {
                    let m = m.map_err(|e| nats_err("fetching", e))?;
                    let seq = m
                        .info()
                        .map(|i| i.stream_sequence)
                        .map_err(|e| nats_err("reading the message's metadata", e))?;
                    let (msg, acker) = m.split();
                    out.push((msg, Some((seq, acker))));
                }
            }
            Reader::Core { sub } => {
                let mut w = wait;
                while out.len() < rows {
                    match tokio::time::timeout(w, sub.next()).await {
                        Err(_) => break,
                        Ok(None) => {
                            self.live = None;
                            return Err(ConnectorError::retryable("nats: the subscription ended"));
                        }
                        Ok(Some(m)) => out.push((m, None)),
                    }
                    w = Duration::ZERO;
                }
            }
        }
        Ok(out)
    }

    fn incoming(
        &mut self,
        session: u64,
        m: &async_nats::Message,
        stream_seq: Option<u64>,
    ) -> Incoming {
        let (headers, content_type) = header_list(m.headers.as_ref());
        self.counter += 1;
        let fallback_id = match stream_seq {
            Some(seq) => format!("{}/{seq}", self.src.stream.as_deref().unwrap_or_default()),
            None => format!("{}/{session:x}/{}", m.subject, self.counter),
        };
        Incoming {
            headers,
            content_type,
            data: m.payload.clone(),
            fallback_id,
            resource: self.split.clone(),
            subject: Some(m.subject.to_string()),
        }
    }
}

#[async_trait]
impl Source for NatsSourceTask {
    async fn open(&mut self, split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        // JetStream's durable consumer holds the ack floor; core NATS has no
        // position to return to.
        self.split = split_name(split)?;
        self.ensure().await
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.ensure().await?;
        let rows = common::take_rows(budget, self.src.max_batch);
        let session = self.live.as_ref().map(|l| l.session).unwrap_or_default();
        // Fetch only when nothing fetched earlier is still waiting, so the
        // queue never grows beyond one fetch (which the budget bounds).
        if self.held.is_empty() {
            let wait = Duration::from_millis(self.src.poll_wait_ms);
            match self.pull(rows, budget.max_bytes, wait).await {
                Ok(m) => self.held.extend(m),
                Err(e) => {
                    self.live = None;
                    return Err(e);
                }
            }
        }
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut last: Option<(u64, String)> = None;
        while events.len() < rows {
            let Some((m, ack)) = self.held.pop_front() else {
                break;
            };
            let stream_seq = ack.as_ref().map(|(s, _)| *s);
            let inc = self.incoming(session, &m, stream_seq);
            let ev = mapping::to_event(&self.env.envelope, inc);
            let size = ev.size_bytes();
            if size > budget.max_bytes && events.is_empty() {
                // Too large for any batch: terminate it (JetStream would
                // redeliver it forever) and report it as poison.
                if let Some((_, acker)) = &ack {
                    let _ = acker.ack_with(jetstream::AckKind::Term).await;
                }
                return Err(ConnectorError::poison(format!(
                    "nats: a {size}-byte message on {} exceeds the batch budget",
                    m.subject
                )));
            }
            if bytes + size > budget.max_bytes {
                self.held.push_front((m, ack));
                break;
            }
            bytes += size;
            let display = match &ack {
                Some((stream_seq, _)) => format!("{}@{stream_seq}", self.split),
                None => format!("{}#{}", self.split, self.counter),
            };
            let seq = self.pending.push(ack.map(|(_, a)| a));
            last = Some((seq, display));
            events.push(ev);
        }
        let Some((seq, display)) = last else {
            return Ok(None);
        };
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: session_position(session, seq, display),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some((session, seq)) = parse_session_position(upto) else {
            return Ok(());
        };
        if self.live.as_ref().map(|l| l.session) != Some(session) {
            return Ok(());
        }
        let mut ackers: Vec<jetstream::message::Acker> = self
            .pending
            .take_upto(seq)
            .into_iter()
            .filter_map(|(_, a)| a)
            .collect();
        // `ack_policy: explicit`: each message is acknowledged on its own
        // (a cumulative ack could cover a message this source dropped on a
        // cancelled fetch). The last is double-acked, so the server has
        // confirmed the batch before the pipeline moves on.
        let Some(last) = ackers.pop() else {
            return Ok(());
        };
        for a in ackers {
            a.ack().await.map_err(|e| nats_err("acknowledging", e))?;
        }
        last.double_ack()
            .await
            .map_err(|e| nats_err("acknowledging", e))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.live = None;
        Ok(())
    }
}

struct SourceMaker {
    cfg: NatsConfig,
    src: NatsSource,
    window: u32,
    env: Env,
    instance: String,
}

impl SourceFactory for SourceMaker {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(NatsSourceTask {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            window: self.window,
            env: self.env.clone(),
            instance: self.instance.clone(),
            split: split_name(split)?,
            live: None,
            pending: Pending::new(),
            held: VecDeque::new(),
            counter: 0,
        }))
    }
}

// -------------------------------------------------------------------- sink --

pub struct NatsSinkTask {
    cfg: NatsConfig,
    sink: NatsSink,
    env: Env,
    instance: String,
    client: Option<Client>,
}

impl std::fmt::Debug for NatsSinkTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSink")
            .field("mode", &self.sink.mode)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for NatsSinkTask {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, ID)?;
        if self.client.is_none() {
            self.client = Some(
                connect(
                    &self.cfg,
                    &self.env,
                    &format!("loams-{}-sink", self.instance),
                )
                .await?,
            );
        }
        let client = self.client.clone().expect("connected");
        let mut ack = SinkAck::default();
        let subject_of = |ev: &CloudEvent| -> Result<String, ConnectorError> {
            self.sink
                .subject
                .clone()
                .or_else(|| ev.subject.clone())
                .ok_or_else(|| {
                    ConnectorError::poison("nats: no sink.subject and the event has no subject")
                })
        };
        match self.sink.mode {
            Mode::Jetstream => {
                let js = jetstream::new(client);
                let timeout = Duration::from_millis(self.sink.ack_timeout_ms);
                let mut futs = Vec::with_capacity(events.len());
                for ev in &events {
                    let msg = jetstream::message::PublishMessage::build()
                        .payload(ev.data.clone())
                        .headers(headers(ev))
                        .message_id(&ev.id);
                    let fut = js
                        .send_publish(subject_of(ev)?, msg)
                        .await
                        .map_err(|e| nats_err("publishing", e))?;
                    futs.push(fut);
                    ack.rows += 1;
                    ack.bytes += ev.size_bytes() as u64;
                }
                for fut in futs {
                    tokio::time::timeout(timeout, fut)
                        .await
                        .map_err(|_| ConnectorError::retryable("nats: no publish ack in time"))?
                        .map_err(|e| nats_err("waiting for the publish ack", e))?;
                }
            }
            Mode::Core => {
                for ev in &events {
                    client
                        .publish_with_headers(subject_of(ev)?, headers(ev), ev.data.clone())
                        .await
                        .map_err(|e| nats_err("publishing", e))?;
                    ack.rows += 1;
                    ack.bytes += ev.size_bytes() as u64;
                }
                client.flush().await.map_err(|e| nats_err("flushing", e))?;
            }
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: NatsConfig,
    sink: NatsSink,
    env: Env,
    instance: String,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(NatsSinkTask {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            instance: self.instance.clone(),
            client: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::nats::Nats`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Nats;

impl ConnectorFactory for Nats {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = NatsConfig::parse(&ctx.config)?;
        let Some(src) = cfg.source.clone() else {
            return Ok(None);
        };
        let pipeline = common::pipeline_config(&ctx.config)?;
        let window = common::window_for(&pipeline, src.max_batch, src.max_ack_pending, 1_000_000);
        let splits = match src.mode {
            Mode::Jetstream => vec![src.stream.clone().unwrap_or_default()],
            Mode::Core => src.subjects.clone(),
        };
        Ok(Some((
            Box::new(StaticSplits::new(splits)),
            Box::new(SourceMaker {
                cfg,
                src,
                window,
                env: Env::new(ID, ctx),
                instance: ctx.instance.name.clone(),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = NatsConfig::parse(&ctx.config)?;
        let Some(sink) = cfg.sink.clone() else {
            return Ok(None);
        };
        Ok(Some(Box::new(SinkMaker {
            cfg,
            sink,
            env: Env::new(ID, ctx),
            instance: ctx.instance.name.clone(),
        })))
    }
}

pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(Nats)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    #[test]
    fn nats_core_declares_not_resumable() {
        let core = json!({
            "servers": ["nats://h:4222"],
            "source": {"mode": "core", "subjects": ["orders.>"]},
            "sink": {"mode": "core", "subject": "out"}
        });
        let d = declared(&core).unwrap();
        assert!(
            !d.source_resumable,
            "core NATS keeps nothing for a gone subscriber"
        );
        assert_eq!(d.source, Delivery::AtMostOnce);
        assert_eq!(d.sink, Delivery::AtMostOnce);
        let js = json!({
            "servers": ["nats://h:4222"],
            "source": {"stream": "ORDERS"},
            "sink": {"subject": "out"}
        });
        let d = declared(&js).unwrap();
        assert!(d.source_resumable);
        assert_eq!(d.source, Delivery::AtLeastOnce);
        assert_eq!(d.sink, Delivery::EffectivelyOnce);
    }

    #[test]
    fn configs_are_checked() {
        assert!(NatsConfig::parse(&json!({"servers": []})).is_err());
        assert!(NatsConfig::parse(&json!({"servers": ["nats://u:p@h"]})).is_err());
        assert!(
            NatsConfig::parse(&json!({"servers": ["h"], "source": {"mode": "jetstream"}})).is_err()
        );
        assert!(NatsConfig::parse(&json!({"servers": ["h"], "source": {"mode": "core"}})).is_err());
        NatsConfig::parse(&json!({
            "servers": ["tls://h:4222"], "auth": {"nkey_seed": "nats-seed"},
            "source": {"stream": "S", "durable": "d"}
        }))
        .unwrap();
    }

    #[test]
    fn headers_carry_the_event_id_for_dedup_and_passthrough() {
        let ev = CloudEvent::new("evt-3", "/s", "t.v1")
            .with_data("application/json", Bytes::from_static(b"{}"));
        let h = headers(&ev);
        assert_eq!(h.get("ce_id").map(|v| v.as_str()), Some("evt-3"));
        let (list, ct) = header_list(Some(&h));
        assert_eq!(ct.as_deref(), Some("application/json"));
        assert_eq!(mapping::header_id(&list), Some("evt-3"));
    }
}
