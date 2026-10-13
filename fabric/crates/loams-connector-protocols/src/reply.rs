//! Request-reply sinks (`sink.reply`, OR-19): gRPC unary calls, function
//! invocations and AI calls. Each input event is one call; its answer becomes
//! one **reply event** on the instance's declared reply topic (ruling R21.6).
//!
//! * A reply keeps the input's `subject`, and carries the input's `id` and
//!   `source` in the `loamsinreplyto` and `loamsinreplytosource` extensions.
//!   Its `type` is `io.loams.dev.flow.<connector>.reply.v1` and its `id` is
//!   `<input id>/reply`, so a call repeated after a retry yields the same
//!   reply id and `ingest`'s `(source, id)` dedup drops the second copy.
//! * A reply never holds a credential: its data is built from the answer
//!   body only, and [`ReplySink`] refuses (as `Fatal`) to append a reply
//!   whose bytes contain any credential value the call sent.
//! * The reply topic is reached through [`ReplyTopics`], which the runtime
//!   passes to the family's factories (`loams-flow`'s `fabric_io.rs` once CN1
//!   Task 3 lands); a sink with no `reply_topic` in its config sends no
//!   replies.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, FabricWriter,
    InstanceKey, Position, Sink, SinkAck, SourceBatch,
};

/// Gives a sink the Fabric writer of a reply topic.
pub trait ReplyTopics: Send + Sync {
    fn writer(
        &self,
        instance: &InstanceKey,
        topic: &str,
    ) -> Result<Arc<dyn FabricWriter>, ConnectorError>;
}

/// Every topic is the one writer (tests, and a single-topic runtime).
pub struct OneTopic(pub Arc<dyn FabricWriter>);

impl std::fmt::Debug for OneTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OneTopic")
    }
}

impl ReplyTopics for OneTopic {
    fn writer(
        &self,
        _instance: &InstanceKey,
        _topic: &str,
    ) -> Result<Arc<dyn FabricWriter>, ConnectorError> {
        Ok(self.0.clone())
    }
}

/// The answer to one call.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    /// What was called (a gRPC method, a function, a model): the reply
    /// event's `source` resource.
    pub resource: String,
    pub content_type: String,
    pub data: Bytes,
}

impl Reply {
    pub fn json(resource: impl Into<String>, v: &serde_json::Value) -> Self {
        Self {
            resource: resource.into(),
            content_type: "application/json".to_owned(),
            data: Bytes::from(serde_json::to_vec(v).unwrap_or_default()),
        }
    }
}

/// One call per event.
#[async_trait]
pub trait Caller: Send + Sync {
    async fn call(&self, ev: &CloudEvent) -> Result<Reply, ConnectorError>;
    /// The credential values the calls send, which no reply may contain.
    async fn secret_values(&self) -> Vec<Vec<u8>> {
        Vec::new()
    }
}

/// The reply event of `input`.
pub fn reply_event(builder: &EnvelopeBuilder, input: &CloudEvent, reply: &Reply) -> CloudEvent {
    let mut ev = builder
        .event("reply", &reply.resource, format!("{}/reply", input.id))
        .with_data(reply.content_type.clone(), reply.data.clone())
        .with_extension("loamsinreplyto", input.id.clone())
        .with_extension("loamsinreplytosource", input.source.clone());
    if let Some(s) = &input.subject {
        ev = ev.with_subject(s.clone());
    }
    ev
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
}

/// A sink that calls once per event, in order, and appends one reply per
/// call to the reply topic.
pub struct ReplySink<C: Caller> {
    caller: C,
    builder: EnvelopeBuilder,
    replies: Option<Arc<dyn FabricWriter>>,
    ctx: ConnectorContext,
}

impl<C: Caller> std::fmt::Debug for ReplySink<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplySink")
            .field("instance", &self.ctx.instance)
            .field("replies", &self.replies.is_some())
            .finish_non_exhaustive()
    }
}

impl<C: Caller> ReplySink<C> {
    /// `reply_topic` from the config, resolved through `topics`. A
    /// `reply_topic` without a `ReplyTopics` is a configuration error.
    pub fn new(
        connector: &str,
        caller: C,
        ctx: &ConnectorContext,
        reply_topic: Option<&str>,
        topics: Option<&Arc<dyn ReplyTopics>>,
    ) -> Result<Self, ConnectorError> {
        let replies = match (reply_topic, topics) {
            (None, _) => None,
            (Some(t), Some(topics)) => Some(topics.writer(&ctx.instance, t)?),
            (Some(t), None) => {
                return Err(ConnectorError::fatal(format!(
                    "reply_topic {t:?} is set but the runtime gave this sink no reply topics"
                )));
            }
        };
        Ok(Self {
            caller,
            builder: EnvelopeBuilder::new(connector, &ctx.instance),
            replies,
            ctx: ctx.clone(),
        })
    }

    pub fn caller(&self) -> &C {
        &self.caller
    }

    async fn emit(&self, replies: Vec<CloudEvent>) -> Result<(), ConnectorError> {
        let Some(w) = &self.replies else {
            return Ok(());
        };
        if replies.is_empty() {
            return Ok(());
        }
        let secrets = self.caller.secret_values().await;
        for r in &replies {
            let mut bytes = r.data.to_vec();
            bytes.extend(r.extensions.values().flat_map(|v| v.bytes()));
            if secrets.iter().any(|s| contains(&bytes, s)) {
                return Err(ConnectorError::fatal(format!(
                    "the reply to {} would carry a credential; refused",
                    r.extensions
                        .get("loamsinreplyto")
                        .map_or("an event", String::as_str)
                )));
            }
        }
        let batch = SourceBatch {
            events: EventBatch::Events(replies),
            position: Position::new(Bytes::new(), "replies"),
        };
        w.append(&batch).await.map_err(|e| {
            ConnectorError::new(e.class, format!("appending replies: {}", e.message))
        })?;
        Ok(())
    }
}

#[async_trait]
impl<C: Caller + 'static> Sink for ReplySink<C> {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let EventBatch::Events(events) = batch else {
            return Err(ConnectorError::poison(
                "a request-reply sink takes JSON events, not Arrow batches",
            ));
        };
        let mut replies = Vec::with_capacity(events.len());
        let mut ack = SinkAck::default();
        for ev in &events {
            self.ctx.observe.api_call(&self.ctx.instance);
            match self.caller.call(ev).await {
                Ok(reply) => {
                    replies.push(reply_event(&self.builder, ev, &reply));
                    ack.rows += 1;
                    ack.bytes += ev.size_bytes() as u64;
                }
                Err(e) => {
                    // The replies of the calls that succeeded go out first,
                    // so a retry of the batch repeats them under the same
                    // ids (deduplicated) rather than losing them.
                    self.emit(replies).await?;
                    return Err(e);
                }
            }
        }
        self.emit(replies).await?;
        Ok(ack)
    }

    /// Every call has returned before `write` does.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_keeps_subject_and_correlates() {
        let b = EnvelopeBuilder::new("openai", &InstanceKey::new(1, "ai"));
        let input = CloudEvent::new("e-1", "/orders", "t").with_subject("order-7");
        let r = reply_event(
            &b,
            &input,
            &Reply::json("gpt", &serde_json::json!({"text": "hi"})),
        );
        assert_eq!(r.id, "e-1/reply");
        assert_eq!(r.ty, "io.loams.dev.flow.openai.reply.v1");
        assert_eq!(r.source, "/connectors/ai/gpt");
        assert_eq!(r.subject.as_deref(), Some("order-7"));
        assert_eq!(
            r.extensions.get("loamsinreplyto").map(String::as_str),
            Some("e-1")
        );
        assert!(contains(b"abcdef", b"cde"));
        assert!(!contains(b"abc", b""));
    }
}
