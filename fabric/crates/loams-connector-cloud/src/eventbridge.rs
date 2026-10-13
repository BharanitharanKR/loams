//! Amazon EventBridge (`eventbridge`, CN2 Task 19).
//!
//! **Sink:** `PutEvents` (AWS JSON 1.1), ten entries a call, to `event_bus`
//! (a name or ARN; default `default`). Each entry's `Source` is the event's
//! `source`, `DetailType` its `type`, `Time` its `time`, and `Detail` the
//! whole event in the structured CloudEvents form, so a rule can match on
//! any attribute and a Loams source reading the rule's target gets the
//! event back with its identity. Entries that fail transiently are retried
//! alone; an entry EventBridge refuses is `Poison` (the DLQ).
//!
//! **Source:** EventBridge pushes to targets, so the source is a rule
//! targeting an SQS queue that the instance reads (`queue_url` and the SQS
//! settings; the guide and runbook set the rule up). An EventBridge event
//! whose `detail` is a CloudEvent passes through; any other is `event`, with
//! EventBridge's event id, `detail-type` as subject, and the whole event as
//! data.

use std::sync::Arc;

use async_trait::async_trait;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, SinkFactory,
    SourceFactory, SplitEnumerator,
};
use serde_json::{Value, json};

use crate::aws::{AwsClient, AwsService, classify};
use crate::ce;
use crate::config::str_opt;
use crate::queue::QueueMessage;
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};
use crate::sqs::{SqsQueue, queue_source};

pub const ID: &str = "eventbridge";
/// The event a non-CloudEvents EventBridge event becomes.
pub const EVENT: &str = "event";
/// `PutEvents`' limit on a call's total entry size.
const MAX_BATCH_BYTES: usize = 256 * 1024;

/// An EventBridge event read from a rule's SQS target: pass-through, or
/// `event`. A message that is not an EventBridge event is decoded as SQS
/// would.
pub fn decode_message(
    env: &EnvelopeBuilder,
    resource: &str,
    m: &QueueMessage,
) -> Result<Vec<CloudEvent>, ConnectorError> {
    let Ok(v) = serde_json::from_slice::<Value>(&m.body) else {
        return crate::sqs::decode_message(env, resource, m);
    };
    let (Some(id), Some(detail_type), Some(detail)) = (
        v.get("id").and_then(Value::as_str),
        v.get("detail-type").and_then(Value::as_str),
        v.get("detail"),
    ) else {
        return crate::sqs::decode_message(env, resource, m);
    };
    if detail.is_object()
        && let Ok(bytes) = serde_json::to_vec(detail)
        && let Some(ev) = env.from_structured_json(&bytes)
    {
        return Ok(vec![ev]);
    }
    let mut ev = env
        .event(EVENT, resource, id.to_owned())
        .with_subject(detail_type.to_owned())
        .with_data("application/json", m.body.clone());
    if let Some(t) = v.get("time").and_then(Value::as_str) {
        ev = ev.with_time(t.to_owned());
    }
    Ok(vec![ev])
}

/// The sink's `PutEvents` call.
#[derive(Debug, Clone)]
pub struct EventBridgePut {
    client: AwsClient,
    bus: String,
}

impl EventBridgePut {
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Ok(Self {
            client: AwsClient::from_context(ctx, AwsService::EventBridge)?,
            bus: str_opt(&ctx.config, "event_bus").unwrap_or_else(|| "default".to_owned()),
        })
    }

    fn entry(&self, ev: &CloudEvent) -> Value {
        let mut e = json!({
            "EventBusName": self.bus,
            "Source": ev.source,
            "DetailType": ev.ty,
            "Detail": String::from_utf8(ce::to_structured_bytes(ev)).unwrap_or_default(),
        });
        if let Some(t) = ev.time.as_deref().and_then(ce::parse_rfc3339) {
            e["Time"] = json!(t);
        }
        e
    }
}

#[async_trait]
impl BatchPut for EventBridgePut {
    fn max_entries(&self) -> usize {
        10
    }

    fn max_bytes(&self) -> usize {
        MAX_BATCH_BYTES
    }

    /// EventBridge's own size rule: `Source`, `DetailType` and `Detail`
    /// bytes, plus 14 for `Time`.
    fn entry_size(&self, ev: &CloudEvent) -> usize {
        ev.source.len() + ev.ty.len() + ce::to_structured_bytes(ev).len() + 14
    }

    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError> {
        let entries: Vec<Value> = events.iter().map(|e| self.entry(e)).collect();
        let out = self
            .client
            .call("PutEvents", &json!({"Entries": entries}))
            .await?;
        let results = out["Entries"].as_array().cloned().unwrap_or_default();
        if results.len() != events.len() {
            return Err(ConnectorError::retryable(format!(
                "PutEvents answered {} entries for {}",
                results.len(),
                events.len()
            )));
        }
        Ok(results
            .iter()
            .map(|r| match r["ErrorCode"].as_str() {
                None => EntryOutcome::Delivered,
                Some(code) => {
                    let msg = r["ErrorMessage"].as_str().unwrap_or_default();
                    let transient = classify(400, code).is_transient()
                        || matches!(code, "InternalFailure" | "InternalException");
                    if transient {
                        EntryOutcome::Retry(code.to_owned())
                    } else {
                        EntryOutcome::Refused(format!("{code}: {msg}"))
                    }
                }
            })
            .collect())
    }
}

/// The `eventbridge` factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct EventBridgeFactory;

impl EventBridgeFactory {
    pub fn new() -> Self {
        Self
    }
}

impl ConnectorFactory for EventBridgeFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let queue = Arc::new(SqsQueue::from_context(ctx)?);
        queue_source(ctx, ID, queue, Arc::new(decode_message)).map(Some)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(BatchSinkFactory(
            EventBridgePut::from_context(ctx)?,
        ))))
    }
}
