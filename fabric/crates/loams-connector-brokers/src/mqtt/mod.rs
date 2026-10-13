//! MQTT 5 (`mqtt`), with this crate's own client ([`client`], R18.3).
//!
//! - **Source:** one split per topic filter, each its own persistent session
//!   (`clean_start = false`, a session expiry, a stable client id
//!   `loams-<instance>-<n>`), subscribed at QoS 1. A message's PUBACK is
//!   sent only after the Fabric acknowledges it, so a crash leaves it in the
//!   session and the broker redelivers it on reconnect
//!   (`mqtt5_qos1_no_loss_on_restart`). The CONNECT's Receive Maximum is the
//!   pipeline's budget: the protocol's own flow control is the backpressure.
//! - **Sink:** QoS 1 publishes; a batch is acknowledged when every PUBACK
//!   has arrived with a success reason code (below 0x80; 0x10, "no matching
//!   subscribers", is a success).
//! - **CloudEvents:** MQTT 5 user properties carry `ce_` attributes both
//!   ways, and the Content Type property carries `datacontenttype`.
//! - **Auth:** `basic` (user name and a password by secret name) and `mtls`
//!   (`tls.cert_pem` with `tls.key_pem`), over the SDK's egress-checked
//!   socket.

pub mod client;
pub mod codec;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EventBatch,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator,
};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};

use self::client::{Client, ClientError, Options};
use self::codec::{Connect, Publish};
use crate::common::{
    self, Declared, Env, Pending, StaticSplits, new_session, parse_session_position,
    session_position, split_name, url_has_userinfo, url_host_port,
};
use crate::mapping::{self, Incoming};
use crate::tls::TlsSettings;

pub const ID: &str = "mqtt";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    /// `mqtt://host:1883` or `mqtts://host:8883`.
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    /// The secret name of the password.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: TlsSettings,
    /// Seconds between keep-alives.
    #[serde(default = "default_keep_alive")]
    pub keep_alive_s: u16,
    /// Seconds the broker keeps a source's session after a disconnect.
    #[serde(default = "default_session_expiry")]
    pub session_expiry_s: u32,
    /// The client id prefix (default `loams-<instance>`).
    #[serde(default)]
    pub client_id: Option<String>,
    /// The largest packet accepted from the broker.
    #[serde(default = "default_max_packet")]
    pub max_packet_bytes: usize,
    #[serde(default)]
    pub source: Option<MqttSource>,
    #[serde(default)]
    pub sink: Option<MqttSink>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSource {
    /// One split (one session) per topic filter.
    pub topics: Vec<String>,
    /// Receive Maximum; by default the pipeline's budget.
    #[serde(default)]
    pub receive_maximum: Option<u16>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSink {
    /// A fixed topic; without it, the event's `subject`.
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub retain: bool,
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
}

fn default_keep_alive() -> u16 {
    30
}
fn default_session_expiry() -> u32 {
    3600
}
fn default_max_packet() -> usize {
    16 << 20
}
fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}
fn default_ack_timeout_ms() -> u64 {
    30_000
}

impl MqttConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let c: Self = common::parse_config(ID, config)?;
        if !(c.url.starts_with("mqtt://") || c.url.starts_with("mqtts://")) {
            return Err(ConnectorError::fatal(
                "mqtt: url must be mqtt:// or mqtts://",
            ));
        }
        if url_has_userinfo(&c.url) {
            return Err(ConnectorError::fatal(
                "mqtt: credentials go in `username` and `password` (a secret name), not in the url",
            ));
        }
        if c.source.as_ref().is_some_and(|s| s.topics.is_empty()) {
            return Err(ConnectorError::fatal("mqtt: source.topics is empty"));
        }
        if c.password.is_some() && c.username.is_none() {
            return Err(ConnectorError::fatal("mqtt: a password needs a username"));
        }
        Ok(c)
    }

    fn tls(&self) -> bool {
        self.url.starts_with("mqtts://")
    }
}

/// What an MQTT config delivers: the persistent session makes the source
/// resumable; QoS 1 is at least once both ways.
pub fn declared() -> Declared {
    Declared {
        source_resumable: true,
        source: Delivery::AtLeastOnce,
        sink: Delivery::AtLeastOnce,
    }
}

fn client_err(context: &str, e: ClientError) -> ConnectorError {
    if e.is_auth() {
        ConnectorError::auth(format!("{context}: {e}"))
    } else if matches!(
        e,
        ClientError::Refused(_) | ClientError::SubscribeRefused(_)
    ) {
        ConnectorError::fatal(format!("{context}: {e}"))
    } else {
        ConnectorError::retryable(format!("{context}: {e}"))
    }
}

/// Open a session over the SDK's socket.
async fn open(
    cfg: &MqttConfig,
    env: &Env,
    client_id: String,
    clean_start: bool,
    receive_maximum: Option<u16>,
    subscribe: Vec<String>,
) -> Result<Client, ConnectorError> {
    let (host, port) = url_host_port(&cfg.url, if cfg.tls() { 8883 } else { 1883 })?;
    let password = match &cfg.password {
        Some(p) => Some(common::resolve(&env.secrets, p).await?.expose().to_vec()),
        None => None,
    };
    let opts = Options {
        connect: Connect {
            client_id,
            clean_start,
            keep_alive: cfg.keep_alive_s,
            session_expiry: if clean_start { 0 } else { cfg.session_expiry_s },
            receive_maximum,
            username: cfg.username.clone(),
            password,
        },
        subscribe,
        max_packet: cfg.max_packet_bytes,
    };
    if cfg.tls() || !cfg.tls.is_default() {
        let tls = cfg.tls.client_config(&env.secrets).await?;
        let s =
            loams_connector_sdk::net::connect_tls(&host, port, env.egress.as_ref(), tls).await?;
        handshake(s, opts).await
    } else {
        let s = loams_connector_sdk::net::connect(&host, port, env.egress.as_ref()).await?;
        handshake(s, opts).await
    }
}

async fn handshake<S>(s: S, opts: Options) -> Result<Client, ConnectorError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::time::timeout(Duration::from_secs(30), Client::connect(s, opts))
        .await
        .map_err(|_| ConnectorError::retryable("mqtt: no CONNACK in time"))?
        .map_err(|e| client_err("connecting to the MQTT broker", e))
}

/// The MQTT message for an event.
pub fn publish(ev: &CloudEvent, topic: String, retain: bool) -> Publish {
    Publish {
        topic,
        qos: 1,
        retain,
        content_type: ev.datacontenttype.clone(),
        payload_format: mapping::is_text(ev).then_some(1),
        user_properties: mapping::binary_headers(ev, "ce_"),
        payload: ev.data.clone(),
        ..Publish::default()
    }
}

// ------------------------------------------------------------------ source --

struct Live {
    client: Client,
    session: u64,
}

pub struct MqttSourceTask {
    cfg: MqttConfig,
    src: MqttSource,
    receive_maximum: u16,
    env: Env,
    client_id: String,
    filter: String,
    live: Option<Live>,
    /// Packet ids to acknowledge (`None` for QoS 0 messages).
    pending: Pending<Option<u16>>,
    held: Option<Publish>,
    counter: u64,
}

impl std::fmt::Debug for MqttSourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttSource")
            .field("filter", &self.filter)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl MqttSourceTask {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if self.live.as_ref().is_some_and(|l| !l.client.is_closed()) {
            return Ok(());
        }
        self.live = None;
        self.pending.clear();
        self.held = None;
        let client = open(
            &self.cfg,
            &self.env,
            self.client_id.clone(),
            false,
            Some(self.receive_maximum),
            vec![self.filter.clone()],
        )
        .await?;
        self.live = Some(Live {
            client,
            session: new_session(),
        });
        Ok(())
    }
}

#[async_trait]
impl Source for MqttSourceTask {
    async fn open(&mut self, split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        // The broker's session is the position.
        self.filter = split_name(split)?;
        self.ensure().await
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        self.ensure().await?;
        let rows = common::take_rows(budget, self.src.max_batch);
        let wait = Duration::from_millis(self.src.poll_wait_ms);
        let session = self.live.as_ref().map(|l| l.session).unwrap_or_default();
        let mut events = Vec::new();
        let mut bytes = 0usize;
        let mut last = None;
        while events.len() < rows {
            let p = match self.held.take() {
                Some(p) => p,
                None => {
                    let live = self.live.as_mut().expect("ensured");
                    let w = if events.is_empty() {
                        wait
                    } else {
                        Duration::ZERO
                    };
                    match live.client.recv(w).await {
                        Ok(None) => break,
                        Ok(Some(p)) => p,
                        Err(e) => {
                            self.live = None;
                            return Err(client_err("receiving", e));
                        }
                    }
                }
            };
            self.counter += 1;
            let ev = mapping::to_event(
                &self.env.envelope,
                Incoming {
                    headers: p.user_properties.clone(),
                    content_type: p.content_type.clone(),
                    data: p.payload.clone(),
                    fallback_id: format!("{}/{session:x}/{}", p.topic, self.counter),
                    resource: self.filter.clone(),
                    subject: Some(p.topic.clone()),
                },
            );
            let size = ev.size_bytes();
            if size > budget.max_bytes && events.is_empty() {
                // Too large for any batch: acknowledge it away (the broker
                // would redeliver it) and report it as poison.
                if let (Some(id), Some(l)) = (p.packet_id, &self.live) {
                    let _ = l.client.ack(id);
                }
                return Err(ConnectorError::poison(format!(
                    "mqtt: a {size}-byte message on {} exceeds the batch budget",
                    p.topic
                )));
            }
            if bytes + size > budget.max_bytes {
                self.held = Some(p);
                break;
            }
            bytes += size;
            let seq = self.pending.push(p.packet_id.filter(|_| p.qos > 0));
            last = Some(seq);
            events.push(ev);
        }
        let Some(seq) = last else {
            return Ok(None);
        };
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: session_position(session, seq, format!("{}#{}", self.filter, self.counter)),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some((session, seq)) = parse_session_position(upto) else {
            return Ok(());
        };
        let Some(live) = &self.live else {
            return Ok(());
        };
        if live.session != session {
            return Ok(());
        }
        // PUBACKs go out in the order the PUBLISHes arrived (MQTT 5 §4.6).
        for (_, id) in self.pending.take_upto(seq) {
            if let Some(id) = id {
                live.client
                    .ack(id)
                    .map_err(|e| client_err("acknowledging", e))?;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(l) = self.live.take() {
            l.client.disconnect().await;
        }
        Ok(())
    }
}

struct SourceMaker {
    cfg: MqttConfig,
    src: MqttSource,
    receive_maximum: u16,
    env: Env,
    client_prefix: String,
}

impl SourceFactory for SourceMaker {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let filter = split_name(split)?;
        let index = self
            .src
            .topics
            .iter()
            .position(|t| *t == filter)
            .unwrap_or_default();
        Ok(Box::new(MqttSourceTask {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            receive_maximum: self.receive_maximum,
            env: self.env.clone(),
            client_id: format!("{}-{index}", self.client_prefix),
            filter,
            live: None,
            pending: Pending::new(),
            held: None,
            counter: 0,
        }))
    }
}

// -------------------------------------------------------------------- sink --

pub struct MqttSinkTask {
    cfg: MqttConfig,
    sink: MqttSink,
    env: Env,
    client_id: String,
    client: Option<Client>,
}

impl std::fmt::Debug for MqttSinkTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttSink")
            .field("topic", &self.sink.topic)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for MqttSinkTask {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, ID)?;
        if self.client.as_ref().is_none_or(Client::is_closed) {
            self.client = Some(
                open(
                    &self.cfg,
                    &self.env,
                    self.client_id.clone(),
                    true,
                    None,
                    vec![],
                )
                .await?,
            );
        }
        let client = self.client.as_ref().expect("connected");
        let mut waits = Vec::with_capacity(events.len());
        let mut ack = SinkAck::default();
        for ev in &events {
            let topic = self
                .sink
                .topic
                .clone()
                .or_else(|| ev.subject.clone())
                .ok_or_else(|| {
                    ConnectorError::poison("mqtt: no sink.topic and the event has no subject")
                })?;
            waits.push(
                client
                    .publish(publish(ev, topic, self.sink.retain))
                    .await
                    .map_err(|e| client_err("publishing", e))?,
            );
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        let timeout = Duration::from_millis(self.sink.ack_timeout_ms);
        for w in waits {
            let reason = tokio::time::timeout(timeout, w)
                .await
                .map_err(|_| ConnectorError::retryable("mqtt: no PUBACK in time"))?
                .map_err(|_| ConnectorError::retryable("mqtt: the connection closed"))?
                .map_err(|e| client_err("publishing", e))?;
            match reason {
                r if r < 0x80 => {}
                0x87 => return Err(ConnectorError::auth("mqtt: not authorized to publish")),
                0x90 | 0x95 | 0x99 => {
                    return Err(ConnectorError::poison(format!(
                        "mqtt: the broker refused a message (reason {reason:#04x})"
                    )));
                }
                0x97 => {
                    return Err(ConnectorError::throttled(
                        Duration::from_secs(1),
                        "mqtt: quota exceeded",
                    ));
                }
                r => {
                    return Err(ConnectorError::retryable(format!(
                        "mqtt: the broker refused a message (reason {r:#04x})"
                    )));
                }
            }
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: MqttConfig,
    sink: MqttSink,
    env: Env,
    client_prefix: String,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(MqttSinkTask {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            client_id: format!("{}-sink", self.client_prefix),
            client: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::mqtt::Mqtt`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Mqtt;

impl ConnectorFactory for Mqtt {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = MqttConfig::parse(&ctx.config)?;
        let Some(src) = cfg.source.clone() else {
            return Ok(None);
        };
        let pipeline = common::pipeline_config(&ctx.config)?;
        let receive_maximum = common::window_for(
            &pipeline,
            src.max_batch,
            src.receive_maximum.map(u32::from),
            u32::from(u16::MAX),
        ) as u16;
        let client_prefix = cfg
            .client_id
            .clone()
            .unwrap_or_else(|| format!("loams-{}", ctx.instance.name));
        Ok(Some((
            Box::new(StaticSplits::new(src.topics.clone())),
            Box::new(SourceMaker {
                cfg,
                src,
                receive_maximum,
                env: Env::new(ID, ctx),
                client_prefix,
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = MqttConfig::parse(&ctx.config)?;
        let Some(sink) = cfg.sink.clone() else {
            return Ok(None);
        };
        let client_prefix = cfg
            .client_id
            .clone()
            .unwrap_or_else(|| format!("loams-{}", ctx.instance.name));
        Ok(Some(Box::new(SinkMaker {
            cfg,
            sink,
            env: Env::new(ID, ctx),
            client_prefix,
        })))
    }
}

pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(Mqtt)
}

#[cfg(test)]
pub(crate) mod fake;

#[cfg(test)]
mod tests;
