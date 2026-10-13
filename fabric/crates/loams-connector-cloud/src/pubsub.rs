//! Google Cloud Pub/Sub (`google-pub-sub`, CN2 Task 19) over its REST API
//! (`pubsub.googleapis.com/v1`; CN2 R19.1 records why not gRPC
//! `StreamingPull`).
//!
//! **Source:** `pull` on `subscription`; each message's ack deadline is
//! extended (`modifyAckDeadline`) at half of `ack_deadline_seconds` (default
//! 60) while its event waits for the Fabric, and it is acknowledged only
//! after the Fabric's ack ([`crate::queue::QueueSource`]); a stopping source
//! hands its messages back (deadline 0). Event ids are Pub/Sub message ids.
//! A message in the CloudEvents Pub/Sub binding (`ce-*` attributes, or a
//! structured body) passes through; any other is `message`, its ordering
//! key as subject and its attributes as extensions where their names allow.
//!
//! **Sink:** `publish` on `topic`, up to 1,000 messages a call, in the
//! binding's binary mode (`ce-*` attributes, the data as the message data),
//! with the event's subject as ordering key when `ordering_key: subject`.
//! Exactly-once delivery on a subscription is the subscription's setting;
//! the emulator does not prove it, so it is nightly-only (spike §5.10).
//!
//! Config: `project`, `subscription`, `topic`, `endpoint` (the emulator's
//! `http://host:8085`; default `https://pubsub.googleapis.com`),
//! `auth.credentials_json` (the secret holding a service account key),
//! `ack_deadline_seconds`, `max_messages`, `receivers`, `ordering_key`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::http::HttpClient;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EgressCheck, EnvelopeBuilder,
    ErrorClass, SecretResolver, SinkFactory, SourceFactory, SplitEnumerator,
};
use serde_json::{Value, json};

use crate::ce;
use crate::config::{LazySecret, str_opt, str_req, u64_in};
use crate::gcp::TokenSource;
use crate::queue::{Queue, QueueMessage};
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};
use crate::sqs::queue_source;

pub const ID: &str = "google-pub-sub";
pub const MESSAGE_EVENT: &str = "message";
const DEFAULT_ENDPOINT: &str = "https://pubsub.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/pubsub";
const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// A Pub/Sub REST client bound to an instance's egress check.
#[derive(Clone)]
pub struct PubSubClient {
    base: String,
    http: HttpClient,
    tokens: Arc<TokenSource>,
}

impl fmt::Debug for PubSubClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubClient")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl PubSubClient {
    pub fn from_config(
        config: &Value,
        secrets: Arc<dyn SecretResolver>,
        egress: Arc<dyn EgressCheck>,
    ) -> Result<Self, ConnectorError> {
        let endpoint = str_opt(config, "endpoint");
        let key = LazySecret::from_auth(config, "credentials_json", &secrets);
        if key.is_none() && endpoint.is_none() {
            return Err(ConnectorError::fatal(
                "config `auth.credentials_json` (a secret name) is required without an emulator `endpoint`",
            ));
        }
        let http = loams_connector_sdk::http::client(egress).with_timeout(Duration::from_secs(90));
        Ok(Self {
            base: format!(
                "{}/v1/",
                endpoint
                    .as_deref()
                    .unwrap_or(DEFAULT_ENDPOINT)
                    .trim_end_matches('/')
            ),
            tokens: Arc::new(TokenSource::new(key, SCOPE, http.clone())),
            http,
        })
    }

    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        Self::from_config(&ctx.config, ctx.secrets.clone(), ctx.egress.clone())
    }

    /// One REST call: `path` is relative to `/v1/`; `body` is sent as JSON
    /// unless it is `null`. A 401 drops the cached token and is retried once.
    pub async fn request(
        &self,
        method: http::Method,
        path: &str,
        body: &Value,
    ) -> Result<Value, ConnectorError> {
        for attempt in 0..2 {
            let mut req = http::Request::builder()
                .method(method.clone())
                .uri(format!("{}{path}", self.base));
            let payload = if body.is_null() {
                Bytes::new()
            } else {
                req = req.header("content-type", "application/json");
                Bytes::from(body.to_string())
            };
            if let Some(token) = self.tokens.bearer().await? {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            let mut req = req
                .body(payload)
                .map_err(|e| ConnectorError::fatal(format!("bad Pub/Sub request: {e}")))?;
            if let Some(v) = req.headers_mut().get_mut("authorization") {
                v.set_sensitive(true);
            }
            let resp = self.http.send(req).await?;
            if resp.status().is_success() {
                if resp.body().is_empty() {
                    return Ok(json!({}));
                }
                return serde_json::from_slice(resp.body()).map_err(|e| {
                    ConnectorError::retryable(format!("unreadable Pub/Sub answer: {e}"))
                });
            }
            if resp.status() == http::StatusCode::UNAUTHORIZED && attempt == 0 {
                self.tokens.invalidate().await;
                continue;
            }
            return Err(google_error(resp.status().as_u16(), resp.body()));
        }
        Err(ConnectorError::auth("Pub/Sub refused the credentials"))
    }
}

/// A Google API error (`{"error": {"code", "message", "status"}}`) as a
/// connector error, classed by its canonical status.
pub fn google_error(http_status: u16, body: &[u8]) -> ConnectorError {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let status = v["error"]["status"].as_str().unwrap_or_default().to_owned();
    let mut message = v["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    message.truncate(512);
    let class = match status.as_str() {
        "RESOURCE_EXHAUSTED" => ErrorClass::Throttled {
            after: Duration::from_secs(1),
        },
        "UNAVAILABLE" | "INTERNAL" | "DEADLINE_EXCEEDED" | "ABORTED" | "CANCELLED" => {
            ErrorClass::Retryable { after: None }
        }
        "UNAUTHENTICATED" | "PERMISSION_DENIED" => ErrorClass::Auth,
        "NOT_FOUND" | "FAILED_PRECONDITION" | "UNIMPLEMENTED" => ErrorClass::Fatal,
        "INVALID_ARGUMENT" | "OUT_OF_RANGE" => ErrorClass::Poison,
        _ => loams_connector_sdk::http::classify_status(
            http::StatusCode::from_u16(http_status).unwrap_or(http::StatusCode::BAD_GATEWAY),
            None,
        )
        .unwrap_or(ErrorClass::Fatal),
    };
    let label = if status.is_empty() {
        format!("HTTP {http_status}")
    } else {
        status
    };
    ConnectorError::new(class, format!("Pub/Sub {label}: {message}"))
}

/// A subscription as a [`Queue`].
#[derive(Debug, Clone)]
pub struct PubSubSubscription {
    client: PubSubClient,
    path: String,
    name: String,
    deadline: u64,
    max: usize,
}

impl PubSubSubscription {
    pub fn new(
        client: PubSubClient,
        project: &str,
        subscription: &str,
        deadline_seconds: u64,
        max: usize,
    ) -> Self {
        Self {
            client,
            path: format!("projects/{project}/subscriptions/{subscription}"),
            name: subscription.to_owned(),
            deadline: deadline_seconds.clamp(1, 600),
            max: max.clamp(1, 1000),
        }
    }

    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let c = &ctx.config;
        Ok(Self::new(
            PubSubClient::from_context(ctx)?,
            &str_req(c, "project")?,
            &str_req(c, "subscription")?,
            u64_in(c, "ack_deadline_seconds", 60, 1..=600)?,
            u64_in(c, "max_messages", 100, 1..=1000)? as usize,
        ))
    }

    async fn post(&self, verb: &str, body: Value) -> Result<Value, ConnectorError> {
        self.client
            .request(http::Method::POST, &format!("{}:{verb}", self.path), &body)
            .await
    }

    async fn deadline(&self, handles: &[String], secs: u64) -> Result<(), ConnectorError> {
        for chunk in handles.chunks(1000) {
            if let Err(e) = self
                .post(
                    "modifyAckDeadline",
                    json!({"ackIds": chunk, "ackDeadlineSeconds": secs}),
                )
                .await
            {
                if e.class.is_transient() {
                    return Err(e);
                }
                tracing::warn!(subscription = %self.name, "modifyAckDeadline: {}", e.message);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Queue for PubSubSubscription {
    fn name(&self) -> &str {
        &self.name
    }

    fn lease(&self) -> Duration {
        Duration::from_secs(self.deadline)
    }

    fn max_receive(&self) -> usize {
        self.max
    }

    async fn receive(&self, max: usize) -> Result<Vec<QueueMessage>, ConnectorError> {
        let out = self
            .post("pull", json!({"maxMessages": max.clamp(1, self.max)}))
            .await
            .map_err(|e| match e.class {
                // A refused pull is the configuration's fault, not a message's.
                ErrorClass::Poison => ConnectorError::fatal(e.message),
                _ => e,
            })?;
        let mut msgs = Vec::new();
        for r in out["receivedMessages"].as_array().into_iter().flatten() {
            let m = &r["message"];
            let (Some(handle), Some(id)) = (r["ackId"].as_str(), m["messageId"].as_str()) else {
                continue;
            };
            let data = B64
                .decode(m["data"].as_str().unwrap_or_default())
                .map_err(|_| {
                    ConnectorError::retryable("a Pub/Sub message whose data is not base64")
                })?;
            msgs.push(QueueMessage {
                handle: handle.to_owned(),
                id: id.to_owned(),
                body: Bytes::from(data),
                attributes: m["attributes"]
                    .as_object()
                    .map(|o| {
                        o.iter()
                            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                            .collect()
                    })
                    .unwrap_or_default(),
                time: m["publishTime"].as_str().map(str::to_owned),
                subject: m["orderingKey"]
                    .as_str()
                    .filter(|k| !k.is_empty())
                    .map(str::to_owned),
            });
        }
        Ok(msgs)
    }

    async fn ack(&self, handles: &[String]) -> Result<(), ConnectorError> {
        for chunk in handles.chunks(1000) {
            if let Err(e) = self.post("acknowledge", json!({"ackIds": chunk})).await {
                if e.class.is_transient() {
                    return Err(e);
                }
                // An expired ack id (on an exactly-once subscription, a
                // FAILED_PRECONDITION): the message comes back and its
                // event is deduplicated.
                tracing::warn!(subscription = %self.name, "acknowledge: {}", e.message);
            }
        }
        Ok(())
    }

    async fn extend(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.deadline(handles, self.deadline).await
    }

    async fn release(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.deadline(handles, 0).await
    }
}

/// Pub/Sub's decoding: pass-through, else `message`.
pub fn decode_message(
    env: &EnvelopeBuilder,
    resource: &str,
    m: &QueueMessage,
) -> Result<Vec<CloudEvent>, ConnectorError> {
    if let Some(ev) = ce::passthrough(env, &m.attributes, &m.body) {
        return Ok(vec![ev]);
    }
    // `message`, typed for this connector by its envelope builder.
    Ok(vec![crate::sqs::wrap(env, resource, m)])
}

/// How the sink sets ordering keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    None,
    Subject,
}

/// The sink's `publish` call.
#[derive(Debug, Clone)]
pub struct PubSubPut {
    client: PubSubClient,
    topic: String,
    ordering: Ordering,
}

/// Pub/Sub's attribute value limit.
const MAX_ATTRIBUTE_VALUE: usize = 1024;

/// The data and attributes of `ev` in the CloudEvents Pub/Sub binding: the
/// binary mode, or the structured mode when an attribute would be too long.
pub fn encode_message(ev: &CloudEvent) -> (Bytes, BTreeMap<String, String>) {
    let attrs = ce::binary_attributes(ev, "ce-");
    if attrs
        .iter()
        .all(|(k, v)| k.len() <= 256 && v.len() <= MAX_ATTRIBUTE_VALUE)
    {
        (ev.data.clone(), attrs.into_iter().collect())
    } else {
        (
            Bytes::from(ce::to_structured_bytes(ev)),
            [("content-type".to_owned(), ce::STRUCTURED.to_owned())].into(),
        )
    }
}

#[async_trait]
impl BatchPut for PubSubPut {
    fn max_entries(&self) -> usize {
        1000
    }

    fn max_bytes(&self) -> usize {
        // 10 MB a request, less room for base64 and JSON.
        7 * 1024 * 1024
    }

    fn entry_size(&self, ev: &CloudEvent) -> usize {
        let (data, attrs) = encode_message(ev);
        data.len() * 4 / 3
            + attrs
                .iter()
                .map(|(k, v)| k.len() + v.len() + 6)
                .sum::<usize>()
    }

    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError> {
        let messages: Vec<Value> = events
            .iter()
            .map(|ev| {
                let (data, attrs) = encode_message(ev);
                let mut m = json!({"data": B64.encode(&data), "attributes": attrs});
                if self.ordering == Ordering::Subject
                    && let Some(s) = ev.subject.as_ref().filter(|s| !s.is_empty())
                {
                    m["orderingKey"] = json!(s);
                }
                m
            })
            .collect();
        self.client
            .request(
                http::Method::POST,
                &format!("{}:publish", self.topic),
                &json!({"messages": messages}),
            )
            .await?;
        // `publish` is all or nothing.
        Ok(vec![EntryOutcome::Delivered; events.len()])
    }
}

/// The `google-pub-sub` factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct PubSubFactory;

impl PubSubFactory {
    pub fn new() -> Self {
        Self
    }
}

impl ConnectorFactory for PubSubFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let sub = Arc::new(PubSubSubscription::from_context(ctx)?);
        queue_source(ctx, ID, sub, Arc::new(decode_message)).map(Some)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let c = &ctx.config;
        let ordering = match str_opt(c, "ordering_key").as_deref() {
            None | Some("none") => Ordering::None,
            Some("subject") => Ordering::Subject,
            Some(other) => {
                return Err(ConnectorError::fatal(format!(
                    "config `ordering_key` must be `subject` or `none`, not `{other}`"
                )));
            }
        };
        Ok(Some(Box::new(BatchSinkFactory(PubSubPut {
            client: PubSubClient::from_context(ctx)?,
            topic: format!(
                "projects/{}/topics/{}",
                str_req(c, "project")?,
                str_req(c, "topic")?
            ),
            ordering,
        }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_errors_classify() {
        let e = google_error(
            429,
            br#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"slow"}}"#,
        );
        assert!(matches!(e.class, ErrorClass::Throttled { .. }));
        assert_eq!(
            google_error(404, br#"{"error":{"status":"NOT_FOUND"}}"#).class,
            ErrorClass::Fatal
        );
        assert_eq!(
            google_error(400, br#"{"error":{"status":"INVALID_ARGUMENT"}}"#).class,
            ErrorClass::Poison
        );
        assert_eq!(
            google_error(503, b"").class,
            ErrorClass::Retryable { after: None }
        );
    }

    #[test]
    fn binding() {
        let ev = CloudEvent::new("e", "/s", "t").with_data("text/plain", "x");
        let (data, attrs) = encode_message(&ev);
        assert_eq!(data.as_ref(), b"x");
        assert_eq!(attrs["ce-id"], "e");
        assert_eq!(attrs["content-type"], "text/plain");
        let long = ev.clone().with_subject("s".repeat(2000));
        let (data, attrs) = encode_message(&long);
        assert!(data.starts_with(b"{"));
        assert_eq!(attrs.len(), 1);
    }
}
