//! Amazon SQS (`aws-sqs`, CN2 Task 19) over the SQS JSON protocol.
//!
//! **Source:** long polling (`wait_time_seconds`, default 20), each received
//! message invisible for `visibility_timeout_seconds` (default 30) and
//! extended at half that while its events wait for the Fabric, deleted only
//! after the Fabric acknowledges ([`crate::queue::QueueSource`]). Event ids
//! are SQS message ids. A message carrying CloudEvents attributes (`ce_*`),
//! a structured CloudEvent, or an SNS notification envelope (SNS → SQS
//! without raw delivery) passes through; anything else is `message`.
//!
//! **Sink:** `SendMessageBatch`, the binary mode as message attributes when
//! the event fits SQS's ten attributes and its data is text, else the
//! structured mode. For a FIFO queue (`.fifo`) the deduplication id is the
//! event's `ce_id` (hashed when SQS would refuse it), so a retried batch is
//! not enqueued twice within SQS's five-minute window (`effectively_once`),
//! and the group id is `message_group_id`, else the subject, else `loams`.
//!
//! Config: `queue_url`, `region`, `endpoint`, `auth.*` (see [`crate::aws`]),
//! `wait_time_seconds`, `visibility_timeout_seconds`, `receivers` (parallel
//! receive loops, default 1), `message_group_id`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, SinkFactory,
    SourceFactory, SplitEnumerator,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::aws::{AwsClient, AwsService, classify};
use crate::ce;
use crate::config::{str_opt, str_req, u64_in};
use crate::queue::{Decode, Queue, QueueMessage, QueueSourceFactory, QueueSplits};
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};

pub const ID: &str = "aws-sqs";
/// The event a wrapped message becomes.
pub const MESSAGE_EVENT: &str = "message";
/// SQS's limit on message attributes.
const MAX_ATTRIBUTES: usize = 10;
/// A batch call's payload limit (the classic 256 KiB, which every region and
/// emulator accepts).
const MAX_BATCH_BYTES: usize = 256 * 1024;

/// An SQS queue as a [`Queue`].
#[derive(Debug, Clone)]
pub struct SqsQueue {
    client: AwsClient,
    url: String,
    name: String,
    wait: u64,
    visibility: u64,
}

impl SqsQueue {
    pub fn new(
        client: AwsClient,
        queue_url: &str,
        wait_seconds: u64,
        visibility_seconds: u64,
    ) -> Self {
        Self {
            client,
            url: queue_url.to_owned(),
            name: queue_name(queue_url),
            wait: wait_seconds.min(20),
            visibility: visibility_seconds.clamp(1, 43_200),
        }
    }

    /// The queue an instance's config names (`queue_url` and the timings).
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let c = &ctx.config;
        let url = str_req(c, "queue_url")?;
        let client = AwsClient::from_context(ctx, AwsService::Sqs)?;
        Ok(Self::new(
            client,
            &url,
            u64_in(c, "wait_time_seconds", 20, 0..=20)?,
            u64_in(c, "visibility_timeout_seconds", 30, 1..=43_200)?,
        ))
    }

    async fn batch(&self, op: &str, entries: Vec<Value>) -> Result<(), ConnectorError> {
        for chunk in entries.chunks(10) {
            let out = self
                .client
                .call(op, &json!({"QueueUrl": self.url, "Entries": chunk}))
                .await?;
            for f in out["Failed"].as_array().into_iter().flatten() {
                let code = f["Code"].as_str().unwrap_or("Unknown");
                if f["SenderFault"].as_bool().unwrap_or(true) {
                    // A receipt handle whose visibility ran out: the message
                    // comes back and its event is deduplicated.
                    tracing::warn!(queue = %self.name, "{op}: {code}");
                } else {
                    return Err(ConnectorError::retryable(format!("{op}: {code}")));
                }
            }
        }
        Ok(())
    }

    async fn visibility(&self, handles: &[String], secs: u64) -> Result<(), ConnectorError> {
        let entries = handles
            .iter()
            .enumerate()
            .map(|(i, h)| json!({"Id": i.to_string(), "ReceiptHandle": h, "VisibilityTimeout": secs}))
            .collect();
        self.batch("ChangeMessageVisibilityBatch", entries).await
    }
}

/// The last path segment of a queue URL (its name).
pub fn queue_name(url: &str) -> String {
    url.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(url)
        .to_owned()
}

#[async_trait]
impl Queue for SqsQueue {
    fn name(&self) -> &str {
        &self.name
    }

    fn lease(&self) -> Duration {
        Duration::from_secs(self.visibility)
    }

    fn max_receive(&self) -> usize {
        10
    }

    async fn receive(&self, max: usize) -> Result<Vec<QueueMessage>, ConnectorError> {
        let out = self
            .client
            .call(
                "ReceiveMessage",
                &json!({
                    "QueueUrl": self.url,
                    "MaxNumberOfMessages": max.clamp(1, 10),
                    "WaitTimeSeconds": self.wait,
                    "VisibilityTimeout": self.visibility,
                    "MessageAttributeNames": ["All"],
                    "MessageSystemAttributeNames": ["SentTimestamp", "MessageGroupId"],
                }),
            )
            .await?;
        Ok(out["Messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(message)
            .collect())
    }

    async fn ack(&self, handles: &[String]) -> Result<(), ConnectorError> {
        let entries = handles
            .iter()
            .enumerate()
            .map(|(i, h)| json!({"Id": i.to_string(), "ReceiptHandle": h}))
            .collect();
        self.batch("DeleteMessageBatch", entries).await
    }

    async fn extend(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.visibility(handles, self.visibility).await
    }

    async fn release(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.visibility(handles, 0).await
    }
}

fn message(m: &Value) -> Option<QueueMessage> {
    let attributes = m["MessageAttributes"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, a)| Some((k.clone(), a["StringValue"].as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default();
    let sys = &m["Attributes"];
    Some(QueueMessage {
        handle: m["ReceiptHandle"].as_str()?.to_owned(),
        id: m["MessageId"].as_str()?.to_owned(),
        body: Bytes::from(m["Body"].as_str().unwrap_or_default().to_owned()),
        attributes,
        time: sys["SentTimestamp"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map(ce::rfc3339_millis),
        subject: sys["MessageGroupId"].as_str().map(str::to_owned),
    })
}

/// SQS's decoding: an SNS envelope is unwrapped, a CloudEvent passes
/// through, anything else becomes `message` with the message id.
pub fn decode_message(
    env: &EnvelopeBuilder,
    resource: &str,
    m: &QueueMessage,
) -> Result<Vec<CloudEvent>, ConnectorError> {
    if let Some(n) = crate::sns::Notification::from_json(&m.body) {
        return Ok(vec![n.to_event(env, resource)]);
    }
    if let Some(ev) = ce::passthrough(env, &m.attributes, &m.body) {
        return Ok(vec![ev]);
    }
    Ok(vec![wrap(env, resource, m)])
}

/// A plain message as a `message` event.
pub fn wrap(env: &EnvelopeBuilder, resource: &str, m: &QueueMessage) -> CloudEvent {
    let ct = m
        .attributes
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| ce::sniff_content_type(&m.body).to_owned());
    let mut ev = env
        .event(MESSAGE_EVENT, resource, m.id.clone())
        .with_data(ct, m.body.clone());
    if let Some(t) = &m.time {
        ev = ev.with_time(t.clone());
    }
    if let Some(s) = &m.subject {
        ev = ev.with_subject(s.clone());
    }
    for (k, v) in ce::attribute_extensions(&m.attributes) {
        ev = ev.with_extension(k, v);
    }
    ev
}

/// A FIFO deduplication id for `id`: `id` itself when SQS accepts it (at
/// most 128 alphanumeric or punctuation characters), else the hex SHA-256 of
/// it, so the same event always gets the same id.
pub fn fifo_deduplication_id(id: &str) -> String {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b.is_ascii_punctuation());
    if ok {
        id.to_owned()
    } else {
        Sha256::digest(id.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

/// The body and attributes of `ev` as an SQS or SNS message: the binary
/// mode when it fits (UTF-8 data, at most ten non-empty attributes), else
/// the structured mode.
pub fn encode_message(ev: &CloudEvent) -> (String, Vec<(String, String)>) {
    let attrs: Vec<(String, String)> = ce::binary_attributes(ev, "ce_")
        .into_iter()
        .filter(|(_, v)| !v.is_empty())
        .collect();
    match std::str::from_utf8(&ev.data) {
        Ok(text) if attrs.len() <= MAX_ATTRIBUTES && !text.is_empty() => (text.to_owned(), attrs),
        _ => (
            String::from_utf8(ce::to_structured_bytes(ev)).unwrap_or_default(),
            vec![("content-type".to_owned(), ce::STRUCTURED.to_owned())],
        ),
    }
}

/// The sink's batch call.
#[derive(Debug, Clone)]
pub struct SqsPut {
    client: AwsClient,
    url: String,
    fifo: bool,
    group: Option<String>,
}

impl SqsPut {
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let url = str_req(&ctx.config, "queue_url")?;
        Ok(Self {
            client: AwsClient::from_context(ctx, AwsService::Sqs)?,
            fifo: queue_name(&url).ends_with(".fifo"),
            url,
            group: str_opt(&ctx.config, "message_group_id"),
        })
    }

    fn entry(&self, i: usize, ev: &CloudEvent) -> Value {
        let (body, attrs) = encode_message(ev);
        let attributes: BTreeMap<String, Value> = attrs
            .into_iter()
            .map(|(k, v)| (k, json!({"DataType": "String", "StringValue": v})))
            .collect();
        let mut e =
            json!({"Id": i.to_string(), "MessageBody": body, "MessageAttributes": attributes});
        if self.fifo {
            e["MessageDeduplicationId"] = json!(fifo_deduplication_id(&ev.id));
            let group = self
                .group
                .clone()
                .or_else(|| ev.subject.clone())
                .unwrap_or_else(|| "loams".to_owned());
            e["MessageGroupId"] = json!(fifo_deduplication_id(&group));
        }
        e
    }
}

#[async_trait]
impl BatchPut for SqsPut {
    fn max_entries(&self) -> usize {
        10
    }

    fn max_bytes(&self) -> usize {
        MAX_BATCH_BYTES
    }

    fn entry_size(&self, ev: &CloudEvent) -> usize {
        let (body, attrs) = encode_message(ev);
        body.len()
            + attrs
                .iter()
                .map(|(k, v)| k.len() + v.len() + 6)
                .sum::<usize>()
    }

    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError> {
        let entries: Vec<Value> = events
            .iter()
            .enumerate()
            .map(|(i, e)| self.entry(i, e))
            .collect();
        let out = self
            .client
            .call(
                "SendMessageBatch",
                &json!({"QueueUrl": self.url, "Entries": entries}),
            )
            .await?;
        let mut outcomes = vec![EntryOutcome::Delivered; events.len()];
        for f in out["Failed"].as_array().into_iter().flatten() {
            let Some(i) = f["Id"].as_str().and_then(|s| s.parse::<usize>().ok()) else {
                continue;
            };
            let code = f["Code"].as_str().unwrap_or("Unknown").to_owned();
            let why = format!("{code}: {}", f["Message"].as_str().unwrap_or_default());
            if let Some(o) = outcomes.get_mut(i) {
                *o = if f["SenderFault"].as_bool().unwrap_or(false)
                    && !classify(400, &code).is_transient()
                {
                    EntryOutcome::Refused(why)
                } else {
                    EntryOutcome::Retry(code)
                };
            }
        }
        Ok(outcomes)
    }
}

fn decode() -> Decode {
    Arc::new(decode_message)
}

/// The `aws-sqs` factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct SqsFactory;

impl SqsFactory {
    pub fn new() -> Self {
        Self
    }
}

/// What `ConnectorFactory::source` returns.
pub type SourceParts = (Box<dyn SplitEnumerator>, Box<dyn SourceFactory>);

/// A queue source over `queue` with `decode`, for every queue-shaped source
/// (SQS, EventBridge through SQS, Pub/Sub, Event Grid pull).
pub fn queue_source(
    ctx: &ConnectorContext,
    connector: &str,
    queue: Arc<dyn Queue>,
    decode: Decode,
) -> Result<SourceParts, ConnectorError> {
    let receivers = u64_in(&ctx.config, "receivers", 1, 1..=64)? as usize;
    let envelope = EnvelopeBuilder::new(connector, &ctx.instance);
    Ok((
        Box::new(QueueSplits { receivers }),
        Box::new(QueueSourceFactory::new(queue, decode, envelope)),
    ))
}

impl ConnectorFactory for SqsFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let queue = Arc::new(SqsQueue::from_context(ctx)?);
        queue_source(ctx, ID, queue, decode()).map(Some)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(BatchSinkFactory(SqsPut::from_context(ctx)?))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_modes() {
        let ev = CloudEvent::new("e", "/s", "t").with_data("text/plain", "hi");
        let (body, attrs) = encode_message(&ev);
        assert_eq!(body, "hi");
        assert!(attrs.iter().any(|(k, v)| k == "ce_id" && v == "e"));
        let bin = CloudEvent::new("e", "/s", "t").with_data("application/octet-stream", vec![0xff]);
        let (body, attrs) = encode_message(&bin);
        assert!(body.contains("data_base64"));
        assert_eq!(attrs, vec![("content-type".into(), ce::STRUCTURED.into())]);
        let mut many = CloudEvent::new("e", "/s", "t").with_data("text/plain", "x");
        for i in 0..10 {
            many = many.with_extension(format!("x{i}"), "v");
        }
        assert!(encode_message(&many).0.starts_with('{'));
        assert_eq!(
            queue_name("https://sqs.eu-west-1.amazonaws.com/1/orders.fifo"),
            "orders.fifo"
        );
        assert_eq!(fifo_deduplication_id("a b").len(), 64);
    }
}
