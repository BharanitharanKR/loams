//! Apache Pulsar (`pulsar`), with the `pulsar` crate on tokio and rustls.
//!
//! - **Source:** one split per topic, each a consumer on a durable
//!   subscription (`failover` by default, or `shared`/`key_shared`). Messages
//!   are acknowledged individually after the Fabric acknowledges them, so the
//!   subscription's cursor is the position and a restart resumes there.
//!   Event ids for messages that are not CloudEvents are the message id
//!   (`<topic>/<ledger>:<entry>:<partition>:<batch>`), stable across
//!   redeliveries. The consumer's receiver queue is the pipeline's budget.
//! - **CloudEvents:** Pulsar has no headers, only string properties; `ce_`
//!   properties (and the other recognised prefixes) pass through
//!   (`pulsar_ce_properties_pass_through`), and the sink writes them.
//! - **Sink:** a producer per instance; a batch is acknowledged once every
//!   send receipt has arrived. It is **at least once** (R18.7): `pulsar`
//!   6.9.0 numbers sequence ids from 0 for every producer and renumbers a
//!   retried send, so broker-side deduplication would drop new messages after
//!   a restart; the plan's `effectively_once` by sequence id is narrowed
//!   (`pulsar_producer_dedup`).
//! - **Auth:** `jwt` (a token, by secret name). The crate has no client
//!   certificates and its OAuth 2 support pulls a second HTTP stack, so `mtls`
//!   and `oauth2` are narrowed (R18.7).
//! - Egress: the service URL's host is checked before the client is built;
//!   the brokers a topic lookup returns are dialled by the crate (R18.4).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EventBatch,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split,
    SplitEnumerator,
};
use pulsar::consumer::InitialPosition;
use pulsar::proto::MessageIdData;
use pulsar::{Authentication, Consumer, ConsumerOptions, Producer, SubType, TokioExecutor};
use serde::Deserialize;

use crate::common::{
    self, Declared, Env, Pending, StaticSplits, egress_precheck, new_session,
    parse_session_position, session_position, split_name, url_has_userinfo, url_host_port,
};
use crate::mapping::{self, Incoming};

pub const ID: &str = "pulsar";

/// The property a sink writes the content type in (Pulsar messages have no
/// content-type field), and a source reads it from.
pub const CONTENT_TYPE_PROPERTY: &str = "content-type";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    /// `pulsar://host:6650` or `pulsar+ssl://host:6651`.
    pub url: String,
    /// The secret name of a JWT (the manifest's `jwt`).
    #[serde(default)]
    pub token: Option<String>,
    /// PEM CA bundle for `pulsar+ssl://`.
    #[serde(default)]
    pub ca_pem: Option<String>,
    #[serde(default)]
    pub source: Option<PulsarSource>,
    #[serde(default)]
    pub sink: Option<PulsarSink>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subscription {
    #[default]
    Failover,
    Shared,
    KeyShared,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Initial {
    #[default]
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarSource {
    pub topics: Vec<String>,
    /// The subscription (default `loams-<instance>`).
    #[serde(default)]
    pub subscription: Option<String>,
    #[serde(default)]
    pub subscription_type: Subscription,
    /// Where a new subscription starts.
    #[serde(default)]
    pub initial_position: Initial,
    /// The receiver queue; by default the pipeline's budget.
    #[serde(default)]
    pub receiver_queue_size: Option<u32>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_poll_wait_ms")]
    pub poll_wait_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PulsarSink {
    pub topic: String,
    #[serde(default)]
    pub producer_name: Option<String>,
    #[serde(default = "default_send_timeout_ms")]
    pub send_timeout_ms: u64,
}

fn default_max_batch() -> usize {
    1000
}
fn default_poll_wait_ms() -> u64 {
    common::DEFAULT_POLL_WAIT.as_millis() as u64
}
fn default_send_timeout_ms() -> u64 {
    30_000
}

impl PulsarConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let c: Self = common::parse_config(ID, config)?;
        if !(c.url.starts_with("pulsar://") || c.url.starts_with("pulsar+ssl://")) {
            return Err(ConnectorError::fatal(
                "pulsar: url must be pulsar:// or pulsar+ssl://",
            ));
        }
        if url_has_userinfo(&c.url) {
            return Err(ConnectorError::fatal(
                "pulsar: the token goes in `token` (a secret name), not in the url",
            ));
        }
        if c.source.as_ref().is_some_and(|s| s.topics.is_empty()) {
            return Err(ConnectorError::fatal("pulsar: source.topics is empty"));
        }
        Ok(c)
    }
}

/// What a Pulsar config delivers (R18.7): the subscription cursor makes the
/// source resumable; the sink cannot deduplicate with this client.
pub fn declared() -> Declared {
    Declared {
        source_resumable: true,
        source: Delivery::AtLeastOnce,
        sink: Delivery::AtLeastOnce,
    }
}

/// `pulsar_producer_dedup`'s finding as a refusal: a config asking for
/// deduplicated delivery is told why it cannot have it.
pub fn check_sink_delivery(wanted: Delivery) -> Result<(), ConnectorError> {
    if wanted > Delivery::AtLeastOnce {
        return Err(ConnectorError::fatal(
            "pulsar: the sink is at least once; the pulsar 6.9.0 client numbers sequence ids from 0 \
             for every producer and renumbers retried sends, so broker deduplication would drop \
             new messages after a restart (R18.7)",
        ));
    }
    Ok(())
}

fn pulsar_err(context: &str, e: impl std::fmt::Display) -> ConnectorError {
    let text = e.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("authentication") || lower.contains("authorization") || lower.contains("auth")
    {
        ConnectorError::auth(format!("{context}: {text}"))
    } else if lower.contains("topic not found") || lower.contains("topicnotfound") {
        ConnectorError::fatal(format!("{context}: {text}"))
    } else {
        ConnectorError::retryable(format!("{context}: {text}"))
    }
}

async fn client(
    cfg: &PulsarConfig,
    env: &Env,
) -> Result<pulsar::Pulsar<TokioExecutor>, ConnectorError> {
    let tls = cfg.url.starts_with("pulsar+ssl://");
    let (host, port) = url_host_port(&cfg.url, if tls { 6651 } else { 6650 })?;
    egress_precheck(&host, port, env.egress.as_ref()).await?;
    let mut b = pulsar::Pulsar::builder(cfg.url.clone(), TokioExecutor);
    if let Some(t) = &cfg.token {
        let token = common::resolve(&env.secrets, t).await?;
        b = b.with_auth(Authentication {
            name: "token".to_owned(),
            data: token.expose().to_vec(),
        });
    }
    if let Some(ca) = &cfg.ca_pem {
        b = b.with_certificate_chain(ca.as_bytes().to_vec());
    }
    b.build()
        .await
        .map_err(|e| pulsar_err("connecting to Pulsar", e))
}

/// The properties of a Pulsar message as header pairs, with the content type
/// split out.
pub fn property_list(props: &[pulsar::proto::KeyValue]) -> (Vec<(String, String)>, Option<String>) {
    let mut out = Vec::new();
    let mut ct = None;
    for kv in props {
        if kv.key.eq_ignore_ascii_case(CONTENT_TYPE_PROPERTY) {
            ct = Some(kv.value.clone());
        } else {
            out.push((kv.key.clone(), kv.value.clone()));
        }
    }
    (out, ct)
}

/// A message id as text.
pub fn message_id_text(id: &MessageIdData) -> String {
    format!(
        "{}:{}:{}:{}",
        id.ledger_id,
        id.entry_id,
        id.partition.unwrap_or(-1),
        id.batch_index.unwrap_or(-1)
    )
}

/// The Pulsar message for an event: `ce_` properties, the content type
/// property, the subject as the partition key, the data as the payload.
pub fn message(ev: &CloudEvent) -> pulsar::producer::Message {
    let mut properties: std::collections::HashMap<String, String> =
        mapping::binary_headers(ev, "ce_").into_iter().collect();
    properties.insert(
        CONTENT_TYPE_PROPERTY.to_owned(),
        mapping::content_type(ev).to_owned(),
    );
    pulsar::producer::Message {
        payload: ev.data.to_vec(),
        properties,
        partition_key: ev.subject.clone(),
        ..Default::default()
    }
}

// ------------------------------------------------------------------ source --

struct Live {
    _client: pulsar::Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    session: u64,
}

pub struct PulsarSourceTask {
    cfg: PulsarConfig,
    src: PulsarSource,
    window: u32,
    env: Env,
    instance: String,
    topic: String,
    live: Option<Live>,
    pending: Pending<MessageIdData>,
    held: Option<pulsar::consumer::Message<Vec<u8>>>,
}

impl std::fmt::Debug for PulsarSourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSource")
            .field("topic", &self.topic)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl PulsarSourceTask {
    async fn ensure(&mut self) -> Result<(), ConnectorError> {
        if self.live.is_some() {
            return Ok(());
        }
        self.pending.clear();
        self.held = None;
        let client = client(&self.cfg, &self.env).await?;
        let sub = self
            .src
            .subscription
            .clone()
            .unwrap_or_else(|| format!("loams-{}", self.instance));
        let options = ConsumerOptions {
            receiver_queue_size: Some(self.window),
            durable: Some(true),
            initial_position: match self.src.initial_position {
                Initial::Earliest => InitialPosition::Earliest,
                Initial::Latest => InitialPosition::Latest,
            },
            ..ConsumerOptions::default()
        };
        let consumer: Consumer<Vec<u8>, TokioExecutor> = client
            .consumer()
            .with_topic(self.topic.clone())
            .with_subscription(sub)
            .with_subscription_type(match self.src.subscription_type {
                Subscription::Failover => SubType::Failover,
                Subscription::Shared => SubType::Shared,
                Subscription::KeyShared => SubType::KeyShared,
            })
            .with_consumer_name(format!("loams-{}", self.instance))
            .with_options(options)
            .build()
            .await
            .map_err(|e| pulsar_err("subscribing", e))?;
        self.live = Some(Live {
            _client: client,
            consumer,
            session: new_session(),
        });
        Ok(())
    }

    fn incoming(&self, m: &pulsar::consumer::Message<Vec<u8>>) -> Incoming {
        let (headers, content_type) = property_list(&m.payload.metadata.properties);
        Incoming {
            headers,
            content_type,
            data: Bytes::from(m.payload.data.clone()),
            fallback_id: format!("{}/{}", self.topic, message_id_text(&m.message_id.id)),
            resource: self.topic.clone(),
            subject: m.payload.metadata.partition_key.clone(),
        }
    }
}

#[async_trait]
impl Source for PulsarSourceTask {
    async fn open(&mut self, split: &Split, _pos: Option<Position>) -> Result<(), ConnectorError> {
        // The durable subscription's cursor is the position.
        self.topic = split_name(split)?;
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
            let m = match self.held.take() {
                Some(m) => m,
                None => {
                    let live = self.live.as_mut().expect("ensured");
                    let w = if events.is_empty() {
                        wait
                    } else {
                        Duration::ZERO
                    };
                    match tokio::time::timeout(w, live.consumer.next()).await {
                        Err(_) => break,
                        Ok(None) => {
                            self.live = None;
                            return Err(ConnectorError::retryable("pulsar: the consumer ended"));
                        }
                        Ok(Some(Err(e))) => {
                            self.live = None;
                            return Err(pulsar_err("receiving", e));
                        }
                        Ok(Some(Ok(m))) => m,
                    }
                }
            };
            let ev = mapping::to_event(&self.env.envelope, self.incoming(&m));
            let size = ev.size_bytes();
            if size > budget.max_bytes && events.is_empty() {
                // Too large for any batch: acknowledge it away (Pulsar would
                // redeliver it) and report it as poison.
                if let Some(l) = self.live.as_mut() {
                    let _ = l.consumer.ack(&m).await;
                }
                return Err(ConnectorError::poison(format!(
                    "pulsar: a {size}-byte message on {} exceeds the batch budget",
                    self.topic
                )));
            }
            if bytes + size > budget.max_bytes {
                self.held = Some(m);
                break;
            }
            bytes += size;
            let id_text = message_id_text(&m.message_id.id);
            let seq = self.pending.push(m.message_id.id.clone());
            last = Some((seq, id_text));
            events.push(ev);
        }
        let Some((seq, id)) = last else {
            return Ok(None);
        };
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: session_position(session, seq, format!("{}@{id}", self.topic)),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some((session, seq)) = parse_session_position(upto) else {
            return Ok(());
        };
        let Some(live) = self.live.as_mut() else {
            return Ok(());
        };
        if live.session != session {
            return Ok(());
        }
        for (_, id) in self.pending.take_upto(seq) {
            live.consumer
                .ack_with_id(&self.topic, id)
                .await
                .map_err(|e| pulsar_err("acknowledging", e))?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut l) = self.live.take() {
            let _ = l.consumer.close().await;
        }
        Ok(())
    }
}

struct SourceMaker {
    cfg: PulsarConfig,
    src: PulsarSource,
    window: u32,
    env: Env,
    instance: String,
}

impl SourceFactory for SourceMaker {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(PulsarSourceTask {
            cfg: self.cfg.clone(),
            src: self.src.clone(),
            window: self.window,
            env: self.env.clone(),
            instance: self.instance.clone(),
            topic: split_name(split)?,
            live: None,
            pending: Pending::new(),
            held: None,
        }))
    }
}

// -------------------------------------------------------------------- sink --

pub struct PulsarSinkTask {
    cfg: PulsarConfig,
    sink: PulsarSink,
    env: Env,
    instance: String,
    live: Option<(pulsar::Pulsar<TokioExecutor>, Producer<TokioExecutor>)>,
}

impl std::fmt::Debug for PulsarSinkTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarSink")
            .field("topic", &self.sink.topic)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Sink for PulsarSinkTask {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let events = mapping::events(batch, ID)?;
        if self.live.is_none() {
            let client = client(&self.cfg, &self.env).await?;
            let mut b = client.producer().with_topic(self.sink.topic.clone());
            b =
                b.with_name(self.sink.producer_name.clone().unwrap_or_else(|| {
                    format!("loams-{}-{}", self.instance, common::new_session())
                }));
            let producer = b
                .build()
                .await
                .map_err(|e| pulsar_err("creating the producer", e))?;
            self.live = Some((client, producer));
        }
        let (_, producer) = self.live.as_mut().expect("connected");
        let timeout = Duration::from_millis(self.sink.send_timeout_ms);
        let mut receipts = Vec::with_capacity(events.len());
        let mut ack = SinkAck::default();
        for ev in &events {
            match producer.send_non_blocking(message(ev)).await {
                Ok(r) => receipts.push(r),
                Err(e) => {
                    self.live = None;
                    return Err(pulsar_err("sending", e));
                }
            }
            ack.rows += 1;
            ack.bytes += ev.size_bytes() as u64;
        }
        for r in receipts {
            tokio::time::timeout(timeout, r)
                .await
                .map_err(|_| ConnectorError::retryable("pulsar: no send receipt in time"))?
                .map_err(|e| pulsar_err("waiting for the send receipt", e))?;
        }
        Ok(ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

struct SinkMaker {
    cfg: PulsarConfig,
    sink: PulsarSink,
    env: Env,
    instance: String,
}

impl SinkFactory for SinkMaker {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(PulsarSinkTask {
            cfg: self.cfg.clone(),
            sink: self.sink.clone(),
            env: self.env.clone(),
            instance: self.instance.clone(),
            live: None,
        })))
    }
}

// ----------------------------------------------------------------- factory --

/// `loams_connector_brokers::pulsar::Pulsar`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Pulsar;

impl ConnectorFactory for Pulsar {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = PulsarConfig::parse(&ctx.config)?;
        let Some(src) = cfg.source.clone() else {
            return Ok(None);
        };
        let pipeline = common::pipeline_config(&ctx.config)?;
        let window =
            common::window_for(&pipeline, src.max_batch, src.receiver_queue_size, 1_000_000);
        Ok(Some((
            Box::new(StaticSplits::new(src.topics.clone())),
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
        let cfg = PulsarConfig::parse(&ctx.config)?;
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
    Arc::new(Pulsar)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::{EnvelopeBuilder, InstanceKey};
    use serde_json::json;

    fn kv(k: &str, v: &str) -> pulsar::proto::KeyValue {
        pulsar::proto::KeyValue {
            key: k.to_owned(),
            value: v.to_owned(),
        }
    }

    #[test]
    fn pulsar_ce_properties_pass_through() {
        // Properties as a CloudEvents-aware Pulsar producer writes them.
        let props = vec![
            kv("ce_specversion", "1.0"),
            kv("ce_id", "upstream-1"),
            kv("ce_source", "/billing"),
            kv("ce_type", "com.example.invoice.paid.v1"),
            kv("ce_tenant", "acme"),
            kv("content-type", "application/json"),
            kv("x-unrelated", "kept out of the envelope"),
        ];
        let (headers, ct) = property_list(&props);
        let env = EnvelopeBuilder::new(ID, &InstanceKey::new(1, "billing"));
        let ev = mapping::to_event(
            &env,
            Incoming {
                headers,
                content_type: ct,
                data: Bytes::from_static(b"{\"amount\":3}"),
                fallback_id: "persistent://t/n/topic/1:2:-1:-1".into(),
                resource: "persistent://t/n/topic".into(),
                subject: Some("key-1".into()),
            },
        );
        assert_eq!(ev.id, "upstream-1");
        assert_eq!(ev.source, "/billing");
        assert_eq!(ev.ty, "com.example.invoice.paid.v1");
        assert_eq!(ev.extensions["tenant"], "acme");
        assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
        assert!(!ev.extensions.contains_key("x-unrelated"));
        // And the sink writes the same properties back.
        let m = message(&ev);
        assert_eq!(m.properties["ce_id"], "upstream-1");
        assert_eq!(m.properties["ce_type"], "com.example.invoice.paid.v1");
        assert_eq!(m.properties[CONTENT_TYPE_PROPERTY], "application/json");
    }

    #[test]
    fn pulsar_producer_dedup() {
        // R18.7: the client cannot set sequence ids, so the sink stays at
        // least once and refuses a deduplicated claim.
        assert_eq!(declared().sink, Delivery::AtLeastOnce);
        assert!(check_sink_delivery(Delivery::AtLeastOnce).is_ok());
        let e = check_sink_delivery(Delivery::EffectivelyOnce).unwrap_err();
        assert!(e.message.contains("sequence ids"), "{}", e.message);
    }

    #[test]
    fn message_ids_are_stable_text() {
        let id = MessageIdData {
            ledger_id: 7,
            entry_id: 9,
            partition: Some(2),
            batch_index: None,
            ..Default::default()
        };
        assert_eq!(message_id_text(&id), "7:9:2:-1");
    }

    #[test]
    fn configs_are_checked() {
        assert!(PulsarConfig::parse(&json!({"url": "http://h"})).is_err());
        assert!(PulsarConfig::parse(&json!({"url": "pulsar://tok@h"})).is_err());
        assert!(
            PulsarConfig::parse(&json!({"url": "pulsar://h", "source": {"topics": []}})).is_err()
        );
        let c = PulsarConfig::parse(&json!({
            "url": "pulsar+ssl://h:6651", "token": "pulsar-jwt",
            "source": {"topics": ["persistent://public/default/a"], "subscription_type": "key_shared"},
            "sink": {"topic": "persistent://public/default/b"}
        }))
        .unwrap();
        assert_eq!(c.source.unwrap().subscription_type, Subscription::KeyShared);
    }
}
