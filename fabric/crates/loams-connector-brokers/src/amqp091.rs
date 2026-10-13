//! RabbitMQ over AMQP 0-9-1 (`rabbitmq-amqp`), with `lapin`.
//!
//! - **Source:** one split per queue. `basic.consume` with manual acks; a
//!   delivery is acknowledged (`basic.ack`, `multiple`) only after the Fabric
//!   acknowledges it (`committed`). `basic.qos` sets the prefetch to the
//!   pipeline's budget (`common::window_for`), so a slow Fabric stops the
//!   broker from delivering rather than the source buffering
//!   (`prefetch_is_backpressure_budget`). A crash leaves the deliveries
//!   unacknowledged, and RabbitMQ redelivers them: at least once.
//! - **Sink:** publisher confirms (`confirm.select`); a batch is acknowledged
//!   only when the broker has confirmed every message in it. A nack is
//!   retryable; an unroutable `mandatory` message is poison.
//! - CloudEvents: `ce_` headers both ways (binary mode, D270).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions,
};
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::uri::AMQPUri;
use lapin::{BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, Consumer};
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, Position, SinkAck,
    SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split, SplitEnumerator,
};
use loams_connector_sdk::{EventBatch, Sink};
use serde::Deserialize;

use crate::common::{
    self, Declared, Env, Pending, StaticSplits, egress_precheck, new_session,
    parse_session_position, session_position, split_name, url_has_userinfo, url_host_port,
};
use crate::mapping::{self, Incoming};

/// The manifest id.
pub const ID: &str = "rabbitmq-amqp";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp091Config {
    /// `amqp://host:5672/<vhost>` or `amqps://…`, without credentials.
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    /// The secret name of the password.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub tls: Amqp091Tls,
    #[serde(default)]
    pub source: Option<Amqp091Source>,
    #[serde(default)]
    pub sink: Option<Amqp091Sink>,
}

/// TLS for `amqps://`: a CA bundle and a client certificate (mTLS, the
/// manifest's `mtls`), as lapin's `OwnedTLSConfig` takes them.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp091Tls {
    #[serde(default)]
    pub ca_pem: Option<String>,
    #[serde(default)]
    pub cert_pem: Option<String>,
    /// The secret name of the client's PEM private key.
    #[serde(default)]
    pub key_pem: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp091Source {
    /// One split per queue.
    pub queues: Vec<String>,
    /// `basic.qos` prefetch; by default the pipeline's budget.
    #[serde(default)]
    pub prefetch: Option<u16>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Amqp091Sink {
    /// The exchange (`""` is the default exchange, which routes by queue
    /// name).
    #[serde(default)]
    pub exchange: String,
    /// A fixed routing key; without it, the event's `subject`.
    #[serde(default)]
    pub routing_key: Option<String>,
    /// `delivery_mode = 2` (persistent).
    #[serde(default = "yes")]
    pub persistent: bool,
    /// Ask the broker to return an unroutable message (it is then poison).
    #[serde(default = "yes")]
    pub mandatory: bool,
    #[serde(default = "default_confirm_timeout_ms")]
    pub confirm_timeout_ms: u64,
}

fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}
fn default_confirm_timeout_ms() -> u64 {
    30_000
}
fn yes() -> bool {
    true
}

impl Amqp091Config {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let c: Self = common::parse_config(ID, config)?;
        if !(c.url.starts_with("amqp://") || c.url.starts_with("amqps://")) {
            return Err(ConnectorError::fatal(
                "rabbitmq-amqp: url must be amqp:// or amqps://",
            ));
        }
        if url_has_userinfo(&c.url) {
            return Err(ConnectorError::fatal(
                "rabbitmq-amqp: put the username in `username` and the password's secret name in `password`, not in the url",
            ));
        }
        if let Some(s) = &c.source
            && s.queues.is_empty()
        {
            return Err(ConnectorError::fatal(
                "rabbitmq-amqp: source.queues is empty",
            ));
        }
        Ok(c)
    }

    fn tls(&self) -> bool {
        self.url.starts_with("amqps://")
    }
}

/// What a RabbitMQ config delivers: the broker holds unacknowledged
/// deliveries, so a restart resumes without loss; neither side deduplicates.
pub fn declared() -> Declared {
    Declared {
        source_resumable: true,
        source: Delivery::AtLeastOnce,
        sink: Delivery::AtLeastOnce,
    }
}

/// The `basic.qos` prefetch for a config: the explicit setting, else the
/// pipeline's in-flight budget (R18.5).
pub fn prefetch(config: &serde_json::Value) -> Result<u16, ConnectorError> {
    let c = Amqp091Config::parse(config)?;
    let s = c
        .source
        .ok_or_else(|| ConnectorError::fatal("rabbitmq-amqp: no source"))?;
    let pipeline = common::pipeline_config(config)?;
    Ok(common::window_for(
        &pipeline,
        s.max_batch,
        s.prefetch.map(u32::from),
        u32::from(u16::MAX),
    ) as u16)
}

/// lapin's errors carry the broker's reply text; sort them into classes.
fn classify(context: &str, e: lapin::Error) -> ConnectorError {
    let text = e.to_string();
    let upper = text.to_ascii_uppercase();
    let err = if upper.contains("ACCESS_REFUSED") || upper.contains("AUTHENTICATION") {
        ConnectorError::auth(format!("{context}: {text}"))
    } else if upper.contains("NOT_FOUND") || upper.contains("PRECONDITION_FAILED") {
        ConnectorError::fatal(format!("{context}: {text}"))
    } else {
        ConnectorError::retryable(format!("{context}: {text}"))
    };
    err.with_source(e)
}

async fn connect(cfg: &Amqp091Config, env: &Env) -> Result<Connection, ConnectorError> {
    let (host, port) = url_host_port(&cfg.url, if cfg.tls() { 5671 } else { 5672 })?;
    egress_precheck(&host, port, env.egress.as_ref()).await?;
    let mut uri: AMQPUri = cfg
        .url
        .parse()
        .map_err(|_| ConnectorError::fatal("rabbitmq-amqp: url is not an AMQP URI"))?;
    if let Some(u) = &cfg.username {
        uri.authority.userinfo.username = u.clone();
    }
    if let Some(p) = &cfg.password {
        uri.authority.userinfo.password = common::resolve_str(&env.secrets, p).await?;
    }
    let mut tls = OwnedTLSConfig {
        identity: None,
        cert_chain: cfg.tls.ca_pem.clone(),
    };
    match (&cfg.tls.cert_pem, &cfg.tls.key_pem) {
        (Some(cert), Some(key)) => {
            let key = common::resolve(&env.secrets, key).await?;
            tls.identity = Some(OwnedIdentity::PKCS8 {
                pem: cert.as_bytes().to_vec(),
                key: key.expose().to_vec(),
            });
        }
        (None, None) => {}
        _ => {
            return Err(ConnectorError::fatal(
                "rabbitmq-amqp: tls.cert_pem and tls.key_pem go together",
            ));
        }
    }
    let runtime = lapin::runtime::default_runtime()
        .map_err(|e| ConnectorError::fatal(format!("rabbitmq-amqp: no runtime: {e}")))?;
    Connection::connect_uri_with_config(uri, ConnectionProperties::default(), tls, runtime)
        .await
        .map_err(|e| classify("connecting to RabbitMQ", e))
}

// ------------------------------------------------------------------ source --

struct Live {
    conn: Connection,
    channel: Channel,
    consumer: Consumer,
    session: u64,
}

/// One queue.
pub struct RabbitSource {
    cfg: Amqp091Config,
    src: Amqp091Source,
    prefetch: u16,
    env: Env,
    queue: String,
    live: Option<Live>,
    /// Deliveries handed to the pipeline, by sequence: their delivery tags.
    pending: Pending<u64>,
    /// A delivery taken from the stream that did not fit the last budget.
    held: Option<lapin::message::Delivery>,
}

impl std::fmt::Debug for RabbitSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitSource")
            .field("queue", &self.queue)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl RabbitSource {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if self
            .live
            .as_ref()
            .is_some_and(|l| l.channel.status().connected())
        {
            return Ok(());
        }
        // A new connection: the broker redelivers whatever the old one held.
        self.pending.clear();
        self.held = None;
        let conn = connect(&self.cfg, &self.env).await?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| classify("opening a channel", e))?;
        channel
            .basic_qos(self.prefetch, BasicQosOptions { global: false })
            .await
            .map_err(|e| classify("basic.qos", e))?;
        let queue = ShortString::try_new(self.queue.clone())
            .map_err(|_| ConnectorError::fatal("rabbitmq-amqp: queue name over 255 bytes"))?;
        let consumer = channel
            .basic_consume(
                queue,
                ShortString::from(""),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| classify("basic.consume", e))?;
        self.live = Some(Live {
            conn,
            channel,
            consumer,
            session: new_session(),
        });
        Ok(())
    }

    fn incoming(&self, session: u64, d: &lapin::message::Delivery) -> Incoming {
        let mut headers = Vec::new();
        if let Some(table) = d.properties.headers() {
            for (k, v) in table {
                if let Some(text) = value_text(v) {
                    headers.push((k.as_str().to_owned(), text));
                }
            }
        }
        let fallback_id = match d.properties.message_id() {
            Some(id) => format!("{}/{}", self.queue, id.as_str()),
            None => format!("{}/{session:x}/{}", self.queue, d.delivery_tag),
        };
        Incoming {
            headers,
            content_type: d
                .properties
                .content_type()
                .as_ref()
                .map(|c| c.as_str().to_owned()),
            data: Bytes::from(d.data.clone()),
            fallback_id,
            resource: self.queue.clone(),
            subject: (!d.routing_key.as_str().is_empty())
                .then(|| d.routing_key.as_str().to_owned()),
        }
    }

    async fn reject(&self, tag: u64) {
        if let Some(l) = &self.live {
            let _ = l
                .channel
                .basic_nack(
                    tag,
                    BasicNackOptions {
                        multiple: false,
                        requeue: false,
                    },
                )
                .await;
        }
    }
}

/// An AMQP 0-9-1 table value as text, for CloudEvents headers.
fn value_text(v: &AMQPValue) -> Option<String> {
    match v {
        AMQPValue::LongString(s) => std::str::from_utf8(s.as_bytes()).ok().map(str::to_owned),
        AMQPValue::ShortString(s) => Some(s.as_str().to_owned()),
        AMQPValue::Boolean(b) => Some(b.to_string()),
        AMQPValue::LongInt(n) => Some(n.to_string()),
        AMQPValue::LongLongInt(n) => Some(n.to_string()),
        AMQPValue::Timestamp(n) => Some(n.to_string()),
        AMQPValue::ByteArray(b) => std::str::from_utf8(b.as_slice()).ok().map(str::to_owned),
        _ => None,
    }
}

#[async_trait]
impl Source for RabbitSource {
    async fn open(&mut self, split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        // RabbitMQ keeps the position: unacknowledged deliveries are
        // redelivered, so a checkpoint is never replayed here.
        self.queue = split_name(split)?;
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
            let d = match self.held.take() {
                Some(d) => d,
                None => {
                    let live = self.live.as_mut().expect("ensured");
                    let first = events.is_empty();
                    let got = if first {
                        tokio::time::timeout(wait, live.consumer.next()).await
                    } else {
                        // Take only what is already buffered after the first.
                        tokio::time::timeout(Duration::ZERO, live.consumer.next()).await
                    };
                    match got {
                        Err(_) => break,
                        Ok(None) => {
                            self.live = None;
                            return Err(ConnectorError::retryable(
                                "rabbitmq-amqp: the consumer was cancelled by the broker",
                            ));
                        }
                        Ok(Some(Err(e))) => {
                            self.live = None;
                            return Err(classify("consuming", e));
                        }
                        Ok(Some(Ok(d))) => d,
                    }
                }
            };
            let ev = mapping::to_event(&self.env.envelope, self.incoming(session, &d));
            let size = ev.size_bytes();
            if size > budget.max_bytes {
                if events.is_empty() {
                    // Larger than any batch can be (R4.7): reject it so the
                    // broker dead-letters it, and report it as poison.
                    self.reject(d.delivery_tag).await;
                    return Err(ConnectorError::poison(format!(
                        "rabbitmq-amqp: a {size}-byte message on {} exceeds the batch budget",
                        self.queue
                    )));
                }
                self.held = Some(d);
                break;
            }
            if bytes + size > budget.max_bytes {
                self.held = Some(d);
                break;
            }
            bytes += size;
            let seq = self.pending.push(d.delivery_tag);
            last = Some((seq, d.delivery_tag));
            events.push(ev);
        }
        let Some((seq, tag)) = last else {
            return Ok(None);
        };
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: session_position(session, seq, format!("{}@{tag}", self.queue)),
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
            // Deliveries of a closed channel: the broker has requeued them.
            return Ok(());
        }
        let Some((_, tag)) = self.pending.take_upto(seq).pop() else {
            return Ok(());
        };
        live.channel
            .basic_ack(tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|e| classify("basic.ack", e))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(l) = self.live.take() {
            let _ = l.channel.close(200, ShortString::from("bye")).await;
            let _ = l.conn.close(200, ShortString::from("bye")).await;
        }
        Ok(())
    }
}

struct SourceMaker {
    cfg: Amqp091Config,
    src: Amqp091Source,
    prefetch: u16,
    env: Env,
}

impl SourceFactory for SourceMaker {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(RabbitSource {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            prefetch: self.prefetch,
            env: self.env.clone(),
            queue: split_name(split)?,
            live: None,
            pending: Pending::new(),
            held: None,
        }))
    }
}

// -------------------------------------------------------------------- sink --

/// Publishes with confirms.
pub struct RabbitSink {
    cfg: Amqp091Config,
    sink: Amqp091Sink,
    env: Env,
    live: Option<(Connection, Channel)>,
}

impl std::fmt::Debug for RabbitSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitSink")
            .field("exchange", &self.sink.exchange)
            .finish_non_exhaustive()
    }
}

impl RabbitSink {
    async fn channel(&mut self) -> Result<&Channel, ConnectorError> {
        if !self
            .live
            .as_ref()
            .is_some_and(|(_, c)| c.status().connected())
        {
            let conn = connect(&self.cfg, &self.env).await?;
            let ch = conn
                .create_channel()
                .await
                .map_err(|e| classify("opening a channel", e))?;
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| classify("confirm.select", e))?;
            self.live = Some((conn, ch));
        }
        Ok(&self.live.as_ref().expect("connected").1)
    }
}

/// The AMQP 0-9-1 properties of an event (binary mode).
pub fn properties(ev: &loams_connector_sdk::CloudEvent, persistent: bool) -> BasicProperties {
    let mut table = FieldTable::default();
    for (k, v) in mapping::binary_headers(ev, "ce_") {
        if let Ok(k) = ShortString::try_new(k) {
            table.insert(k, AMQPValue::LongString(LongString::from(v.into_bytes())));
        }
    }
    let mut p = BasicProperties::default()
        .with_headers(table)
        .with_content_type(ShortString::from(
            mapping::content_type(ev)
                .chars()
                .take(255)
                .collect::<String>(),
        ));
    if let Ok(id) = ShortString::try_new(ev.id.clone()) {
        p = p.with_message_id(id);
    }
    if persistent {
        p = p.with_delivery_mode(2);
    }
    p
}

#[async_trait]
impl Sink for RabbitSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, ID)?;
        let sink = self.sink.clone();
        let exchange = ShortString::try_new(sink.exchange.clone())
            .map_err(|_| ConnectorError::fatal("rabbitmq-amqp: exchange name over 255 bytes"))?;
        let channel = self.channel().await?.clone();
        let mut confirms = Vec::with_capacity(events.len());
        let mut ack = SinkAck::default();
        for ev in &events {
            let key = sink
                .routing_key
                .clone()
                .or_else(|| ev.subject.clone())
                .unwrap_or_default();
            let key = ShortString::try_new(key)
                .map_err(|_| ConnectorError::poison("rabbitmq-amqp: routing key over 255 bytes"))?;
            let confirm = channel
                .basic_publish(
                    exchange.clone(),
                    key,
                    BasicPublishOptions {
                        mandatory: sink.mandatory,
                        immediate: false,
                    },
                    &ev.data,
                    properties(ev, sink.persistent),
                )
                .await
                .map_err(|e| {
                    self.live = None;
                    classify("basic.publish", e)
                })?;
            confirms.push(confirm);
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        let timeout = Duration::from_millis(sink.confirm_timeout_ms);
        for confirm in confirms {
            let outcome = tokio::time::timeout(timeout, confirm)
                .await
                .map_err(|_| {
                    ConnectorError::retryable("rabbitmq-amqp: no publisher confirm in time")
                })?
                .map_err(|e| classify("publisher confirm", e))?;
            match outcome {
                Confirmation::Ack(None) | Confirmation::NotRequested => {}
                Confirmation::Ack(Some(_)) => {
                    return Err(ConnectorError::poison(
                        "rabbitmq-amqp: the broker returned a mandatory message as unroutable",
                    ));
                }
                Confirmation::Nack(_) => {
                    return Err(ConnectorError::retryable(
                        "rabbitmq-amqp: the broker nacked a message",
                    ));
                }
            }
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Every write waits for its confirms.
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: Amqp091Config,
    sink: Amqp091Sink,
    env: Env,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(RabbitSink {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            live: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::amqp091::RabbitMq`.
#[derive(Debug, Default, Clone, Copy)]
pub struct RabbitMq;

impl ConnectorFactory for RabbitMq {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = Amqp091Config::parse(&ctx.config)?;
        let Some(src) = cfg.source.clone() else {
            return Ok(None);
        };
        let prefetch = prefetch(&ctx.config)?;
        Ok(Some((
            Box::new(StaticSplits::new(src.queues.clone())),
            Box::new(SourceMaker {
                cfg,
                src,
                prefetch,
                env: Env::new(ID, ctx),
            }),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = Amqp091Config::parse(&ctx.config)?;
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

/// For harnesses: the factory as a shared trait object.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(RabbitMq)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::{CloudEvent, ErrorClass};
    use serde_json::json;

    #[test]
    fn config_refuses_credentials_in_the_url() {
        let e = Amqp091Config::parse(&json!({"url": "amqp://guest:guest@h/"})).unwrap_err();
        assert_eq!(e.class, ErrorClass::Fatal);
        assert!(!e.message.contains("guest:guest"));
        assert!(Amqp091Config::parse(&json!({"url": "http://h"})).is_err());
        assert!(
            Amqp091Config::parse(&json!({"url": "amqp://h", "source": {"queues": []}})).is_err()
        );
        let c = Amqp091Config::parse(&json!({
            "url": "amqps://h:5671/%2f", "username": "loams", "password": "rabbit-pw",
            "source": {"queues": ["a", "b"]}, "sink": {"routing_key": "out"},
            "pipeline": {"max_batch_rows": 10}
        }))
        .unwrap();
        assert!(c.tls());
        assert_eq!(c.sink.unwrap().exchange, "");
    }

    #[test]
    fn prefetch_is_backpressure_budget() {
        // The prefetch is the pipeline's in-flight budget unless set: once
        // that many deliveries are unacknowledged (acks wait for the
        // Fabric), the broker stops delivering.
        let base = json!({"url": "amqp://h", "source": {"queues": ["q"], "max_batch": 100}});
        assert_eq!(prefetch(&base).unwrap(), 400);
        let mut tight = base.clone();
        tight["pipeline"] = json!({"max_in_flight_batches": 1, "max_batch_rows": 25});
        assert_eq!(prefetch(&tight).unwrap(), 25);
        let mut explicit = base.clone();
        explicit["source"]["prefetch"] = json!(7);
        assert_eq!(prefetch(&explicit).unwrap(), 7);
        let mut huge = base;
        huge["source"]["max_batch"] = json!(1_000_000);
        assert_eq!(prefetch(&huge).unwrap(), u16::MAX);
    }

    #[test]
    fn properties_carry_ce_headers() {
        let ev = CloudEvent::new("evt-1", "/s", "t.v1")
            .with_subject("k")
            .with_data("application/json", Bytes::from_static(b"{}"));
        let p = properties(&ev, true);
        assert_eq!(p.delivery_mode(), &Some(2));
        assert_eq!(p.message_id().as_ref().map(|m| m.as_str()), Some("evt-1"));
        let h = p.headers().as_ref().unwrap();
        assert_eq!(value_text(&h.inner()["ce_id"]).as_deref(), Some("evt-1"));
        assert_eq!(value_text(&h.inner()["ce_subject"]).as_deref(), Some("k"));
        assert_eq!(
            p.content_type().as_ref().map(|c| c.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn declares_at_least_once_both_ways() {
        let d = declared();
        assert!(d.source_resumable);
        assert_eq!(d.sink, Delivery::AtLeastOnce);
    }
}
