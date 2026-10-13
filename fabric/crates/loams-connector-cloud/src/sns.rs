//! Amazon SNS (`aws-sns`, CN2 Task 19).
//!
//! **Sink:** `PublishBatch` (Query protocol), ten entries a call, with the
//! event's CloudEvents attributes as `ce_*` message attributes (the binary
//! mode, as [`crate::sqs::encode_message`] builds it). A FIFO topic gets the
//! event id as its deduplication id. Entries that fail on SNS's side are
//! retried alone.
//!
//! **Source:** an HTTPS subscription on the webhooks route ([`SnsWebhook`]):
//! every message's signature is verified (SignatureVersion 1, SHA1withRSA,
//! and 2, SHA256withRSA) with the certificate at `SigningCertURL`, which must
//! be SNS's own HTTPS host; messages from another topic than the
//! instance's `topic_arn` are refused; a `SubscriptionConfirmation` is
//! confirmed by visiting its `SubscribeURL` (also checked to be SNS's). A
//! notification is answered `200` only after the Fabric has its event.
//!
//! SNS → SQS fan-out without raw delivery wraps each message in SNS's JSON
//! envelope; [`Notification::from_json`] unwraps it for the SQS source.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::http::HttpClient;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, InstanceKey,
    SinkFactory, SourceFactory, SplitEnumerator,
};
use serde_json::Value;

use crate::aws::{AwsClient, AwsService, classify, xml_blocks, xml_text};
use crate::ce;
use crate::config::{str_opt, str_req};
use crate::crypto;
use crate::push::{self, PushInbox, WebhookHandler, WebhookRegistry, response};
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};
use crate::sqs::{encode_message, fifo_deduplication_id};

pub const ID: &str = "aws-sns";
pub const MESSAGE_EVENT: &str = "message";
const MAX_BATCH_BYTES: usize = 256 * 1024;

// ------------------------------------------------------------ the envelope --

/// An SNS message as SNS delivers it to HTTP endpoints, and to SQS queues
/// without raw delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub kind: String,
    pub message_id: String,
    pub topic_arn: String,
    pub subject: Option<String>,
    pub message: String,
    pub timestamp: Option<String>,
    /// `ce_*` and other message attributes, as strings.
    pub attributes: BTreeMap<String, String>,
}

impl Notification {
    /// A `Notification` envelope, or `None` when `body` is not one.
    pub fn from_json(body: &[u8]) -> Option<Self> {
        let v: Value = serde_json::from_slice(body).ok()?;
        let n = Self::from_value(&v)?;
        (n.kind == "Notification").then_some(n)
    }

    fn from_value(v: &Value) -> Option<Self> {
        let s = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
        Some(Self {
            kind: s("Type")?,
            message_id: s("MessageId")?,
            topic_arn: s("TopicArn")?,
            subject: s("Subject"),
            message: s("Message")?,
            timestamp: s("Timestamp"),
            attributes: v
                .get("MessageAttributes")
                .and_then(Value::as_object)
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, a)| {
                            Some((k.clone(), a.get("Value")?.as_str()?.to_owned()))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// Its event: a CloudEvent passes through (attributes or a structured
    /// body); anything else is `message` with SNS's message id.
    pub fn to_event(&self, env: &EnvelopeBuilder, resource: &str) -> CloudEvent {
        let body = Bytes::from(self.message.clone());
        if let Some(ev) = ce::passthrough(env, &self.attributes, &body) {
            return ev;
        }
        let ct = self
            .attributes
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| ce::sniff_content_type(&body).to_owned());
        let mut ev = env
            .event(MESSAGE_EVENT, resource, self.message_id.clone())
            .with_data(ct, body);
        if let Some(s) = &self.subject {
            ev = ev.with_subject(s.clone());
        }
        if let Some(t) = &self.timestamp {
            ev = ev.with_time(t.clone());
        }
        for (k, v) in ce::attribute_extensions(&self.attributes) {
            ev = ev.with_extension(k, v);
        }
        ev
    }
}

/// The string SNS signs for a message's string fields (`Type` decides which
/// fields, in this order). `None` for an unknown type or a missing field.
pub fn string_to_sign(fields: &BTreeMap<String, String>) -> Option<String> {
    let keys: &[&str] = match fields.get("Type")?.as_str() {
        "Notification" => &[
            "Message",
            "MessageId",
            "Subject",
            "Timestamp",
            "TopicArn",
            "Type",
        ],
        "SubscriptionConfirmation" | "UnsubscribeConfirmation" => &[
            "Message",
            "MessageId",
            "SubscribeURL",
            "Timestamp",
            "Token",
            "TopicArn",
            "Type",
        ],
        _ => return None,
    };
    let mut out = String::new();
    for k in keys {
        match fields.get(*k) {
            Some(v) => {
                out.push_str(k);
                out.push('\n');
                out.push_str(v);
                out.push('\n');
            }
            None if *k == "Subject" => {}
            None => return None,
        }
    }
    Some(out)
}

// ------------------------------------------------------------------- trust --

/// Which URLs an SNS message may point at: its signing certificate and its
/// `SubscribeURL`. The default is SNS's own HTTPS hosts,
/// `sns.<region>.amazonaws.com` (and `.com.cn`).
#[derive(Debug, Clone, Default)]
pub struct SnsTrust {
    /// Only for tests: one exact `host:port` over plain HTTP.
    test_authority: Option<String>,
}

impl SnsTrust {
    /// Trust `authority` (`127.0.0.1:port`) over plain HTTP, and nothing else.
    /// A test seam: no instance config can select it.
    pub fn insecure_for_tests(authority: &str) -> Self {
        Self {
            test_authority: Some(authority.to_owned()),
        }
    }

    fn url_ok(&self, url: &str) -> bool {
        let Ok(uri) = url.parse::<http::Uri>() else {
            return false;
        };
        let Some(auth) = uri.authority() else {
            return false;
        };
        if auth.as_str().contains('@') {
            return false;
        }
        if let Some(t) = &self.test_authority {
            return uri.scheme_str() == Some("http") && auth.as_str() == t;
        }
        if uri.scheme_str() != Some("https") || auth.port_u16().is_some_and(|p| p != 443) {
            return false;
        }
        let host = auth.host();
        let rest = host
            .strip_suffix(".amazonaws.com")
            .or_else(|| host.strip_suffix(".amazonaws.com.cn"));
        match rest.and_then(|r| r.strip_prefix("sns.")) {
            Some(region) => {
                !region.is_empty()
                    && region
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            }
            None => false,
        }
    }

    /// A `SigningCertURL` SNS could have sent.
    pub fn cert_url_ok(&self, url: &str) -> bool {
        self.url_ok(url) && url.split('?').next().is_some_and(|p| p.ends_with(".pem"))
    }

    /// A `SubscribeURL` SNS could have sent.
    pub fn subscribe_url_ok(&self, url: &str) -> bool {
        self.url_ok(url) && url.contains("Action=ConfirmSubscription")
    }
}

// ----------------------------------------------------------------- webhook --

/// The HTTPS-subscription endpoint of an `aws-sns` source.
pub struct SnsWebhook {
    topic_arn: String,
    trust: SnsTrust,
    http: HttpClient,
    inbox: PushInbox,
    envelope: EnvelopeBuilder,
    resource: String,
    keys: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl fmt::Debug for SnsWebhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnsWebhook")
            .field("topic_arn", &self.topic_arn)
            .finish_non_exhaustive()
    }
}

/// Why a message was refused (logged; the answer is a bare `403`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    Malformed(String),
    WrongTopic,
    UntrustedUrl,
    Certificate(String),
    BadSignature,
}

impl SnsWebhook {
    async fn public_key(&self, url: &str) -> Result<Vec<u8>, Refusal> {
        if let Some(k) = self.keys.lock().unwrap_or_else(|p| p.into_inner()).get(url) {
            return Ok(k.clone());
        }
        let req = http::Request::get(url)
            .body(Bytes::new())
            .map_err(|e| Refusal::Certificate(e.to_string()))?;
        let resp = self
            .http
            .send(req)
            .await
            .map_err(|e| Refusal::Certificate(e.message))?;
        if !resp.status().is_success() {
            return Err(Refusal::Certificate(format!("HTTP {}", resp.status())));
        }
        let pem = String::from_utf8_lossy(resp.body());
        let der = crypto::pem_block(&pem, "CERTIFICATE")
            .ok_or_else(|| Refusal::Certificate("no PEM certificate".into()))?;
        let key = crypto::rsa_public_key_from_certificate(&der).map_err(Refusal::Certificate)?;
        self.keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(url.to_owned(), key.clone());
        Ok(key)
    }

    /// Check a message's topic, certificate URL and signature.
    pub async fn verify(&self, v: &Value) -> Result<BTreeMap<String, String>, Refusal> {
        let fields: BTreeMap<String, String> = v
            .as_object()
            .ok_or_else(|| Refusal::Malformed("not a JSON object".into()))?
            .iter()
            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
            .collect();
        if fields.get("TopicArn") != Some(&self.topic_arn) {
            return Err(Refusal::WrongTopic);
        }
        let cert_url = fields
            .get("SigningCertURL")
            .or_else(|| fields.get("SigningCertUrl"))
            .ok_or_else(|| Refusal::Malformed("no SigningCertURL".into()))?;
        if !self.trust.cert_url_ok(cert_url) {
            return Err(Refusal::UntrustedUrl);
        }
        let alg: &dyn ring::signature::VerificationAlgorithm =
            match fields.get("SignatureVersion").map(String::as_str) {
                Some("1") => &ring::signature::RSA_PKCS1_2048_8192_SHA1_FOR_LEGACY_USE_ONLY,
                Some("2") => &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                _ => return Err(Refusal::Malformed("unknown SignatureVersion".into())),
            };
        let sig = fields
            .get("Signature")
            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
            .ok_or_else(|| Refusal::Malformed("no Signature".into()))?;
        let text = string_to_sign(&fields)
            .ok_or_else(|| Refusal::Malformed("missing signed fields".into()))?;
        let key = self.public_key(cert_url).await?;
        ring::signature::UnparsedPublicKey::new(alg, &key)
            .verify(text.as_bytes(), &sig)
            .map_err(|_| Refusal::BadSignature)?;
        Ok(fields)
    }

    async fn confirm(&self, url: &str) -> http::Response<Bytes> {
        if !self.trust.subscribe_url_ok(url) {
            tracing::warn!(topic = %self.topic_arn, "SNS SubscribeURL is not SNS's");
            return response(403, "");
        }
        let Ok(req) = http::Request::get(url).body(Bytes::new()) else {
            return response(400, "");
        };
        match self.http.send(req).await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(topic = %self.topic_arn, "SNS subscription confirmed");
                response(200, "")
            }
            Ok(r) => response(502, &format!("confirmation answered {}", r.status())),
            Err(e) => response(502, &e.message),
        }
    }
}

#[async_trait]
impl WebhookHandler for SnsWebhook {
    async fn handle(&self, req: http::Request<Bytes>) -> http::Response<Bytes> {
        if req.method() != http::Method::POST {
            return response(405, "");
        }
        let Ok(v) = serde_json::from_slice::<Value>(req.body()) else {
            return response(400, "not JSON");
        };
        let fields = match self.verify(&v).await {
            Ok(f) => f,
            Err(r) => {
                tracing::warn!(topic = %self.topic_arn, "SNS message refused: {r:?}");
                return response(403, "");
            }
        };
        match fields.get("Type").map(String::as_str) {
            Some("SubscriptionConfirmation") => match fields.get("SubscribeURL") {
                Some(url) => self.confirm(url).await,
                None => response(400, "no SubscribeURL"),
            },
            Some("Notification") => {
                let Some(n) = Notification::from_value(&v) else {
                    return response(400, "malformed notification");
                };
                match self
                    .inbox
                    .deliver(vec![n.to_event(&self.envelope, &self.resource)])
                    .await
                {
                    Ok(()) => response(200, ""),
                    Err(e) => e.response(),
                }
            }
            _ => response(200, ""),
        }
    }
}

// -------------------------------------------------------------------- sink --

/// The sink's `PublishBatch` call.
#[derive(Debug, Clone)]
pub struct SnsPut {
    client: AwsClient,
    topic_arn: String,
    fifo: bool,
    group: Option<String>,
}

impl SnsPut {
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let topic_arn = str_req(&ctx.config, "topic_arn")?;
        Ok(Self {
            client: AwsClient::from_context(ctx, AwsService::Sns)?,
            fifo: topic_arn.ends_with(".fifo"),
            topic_arn,
            group: str_opt(&ctx.config, "message_group_id"),
        })
    }
}

#[async_trait]
impl BatchPut for SnsPut {
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
        let mut p: Vec<(String, String)> = vec![
            ("Action".into(), "PublishBatch".into()),
            ("TopicArn".into(), self.topic_arn.clone()),
        ];
        for (i, ev) in events.iter().enumerate() {
            let m = format!("PublishBatchRequestEntries.member.{}", i + 1);
            let (body, attrs) = encode_message(ev);
            p.push((format!("{m}.Id"), i.to_string()));
            p.push((format!("{m}.Message"), body));
            for (a, (k, v)) in attrs.into_iter().enumerate() {
                let e = format!("{m}.MessageAttributes.entry.{}", a + 1);
                p.push((format!("{e}.Name"), k));
                p.push((format!("{e}.Value.DataType"), "String".into()));
                p.push((format!("{e}.Value.StringValue"), v));
            }
            if self.fifo {
                p.push((
                    format!("{m}.MessageDeduplicationId"),
                    fifo_deduplication_id(&ev.id),
                ));
                let group = self
                    .group
                    .clone()
                    .or_else(|| ev.subject.clone())
                    .unwrap_or_else(|| "loams".into());
                p.push((format!("{m}.MessageGroupId"), fifo_deduplication_id(&group)));
            }
        }
        let xml = self.client.query_owned(p).await?;
        let mut outcomes = vec![EntryOutcome::Delivered; events.len()];
        let failed = xml_text(&xml, "Failed").unwrap_or_default();
        for member in xml_blocks(&failed, "member") {
            let Some(i) = xml_text(&member, "Id").and_then(|s| s.parse::<usize>().ok()) else {
                continue;
            };
            let code = xml_text(&member, "Code").unwrap_or_else(|| "Unknown".into());
            let sender = xml_text(&member, "SenderFault").is_some_and(|s| s == "true");
            let why = format!(
                "{code}: {}",
                xml_text(&member, "Message").unwrap_or_default()
            );
            if let Some(o) = outcomes.get_mut(i) {
                *o = if sender && !classify(400, &code).is_transient() {
                    EntryOutcome::Refused(why)
                } else {
                    EntryOutcome::Retry(code)
                };
            }
        }
        Ok(outcomes)
    }
}

// ----------------------------------------------------------------- factory --

/// The `aws-sns` factory. Its sources register their [`SnsWebhook`]s, which
/// the webhooks route reaches through [`SnsFactory::webhook`].
#[derive(Debug, Clone, Default)]
pub struct SnsFactory {
    trust: SnsTrust,
    hooks: Arc<WebhookRegistry>,
}

impl SnsFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_trust(mut self, trust: SnsTrust) -> Self {
        self.trust = trust;
        self
    }

    /// The webhook handler of `instance`'s running source.
    pub fn webhook(&self, instance: &InstanceKey) -> Option<Arc<dyn WebhookHandler>> {
        self.hooks.get(instance)
    }
}

impl ConnectorFactory for SnsFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let topic_arn = str_req(&ctx.config, "topic_arn")?;
        let resource = topic_arn
            .rsplit(':')
            .next()
            .unwrap_or(&topic_arn)
            .to_owned();
        let (inbox, sources) = push::channel(64, Duration::from_secs(10));
        let hook = SnsWebhook {
            topic_arn,
            trust: self.trust.clone(),
            http: loams_connector_sdk::http::client(ctx.egress.clone())
                .with_timeout(Duration::from_secs(10)),
            inbox,
            envelope: EnvelopeBuilder::new(ID, &ctx.instance),
            resource,
            keys: Mutex::new(BTreeMap::new()),
        };
        self.hooks.insert(ctx.instance.clone(), Arc::new(hook));
        Ok(Some((Box::new(push::OneSplit), Box::new(sources))))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(BatchSinkFactory(SnsPut::from_context(ctx)?))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_to_sign_orders_fields() {
        let f: BTreeMap<String, String> = [
            ("Type", "Notification"),
            ("MessageId", "m"),
            ("TopicArn", "t"),
            ("Message", "hi"),
            ("Timestamp", "ts"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        assert_eq!(
            string_to_sign(&f).as_deref(),
            Some("Message\nhi\nMessageId\nm\nTimestamp\nts\nTopicArn\nt\nType\nNotification\n")
        );
        let mut c = f.clone();
        c.insert("Type".into(), "SubscriptionConfirmation".into());
        assert!(
            string_to_sign(&c).is_none(),
            "Token and SubscribeURL are required"
        );
    }

    #[test]
    fn envelope_unwraps() {
        let body = br#"{"Type":"Notification","MessageId":"m","TopicArn":"arn","Message":"x","MessageAttributes":{"k":{"Type":"String","Value":"v"}}}"#;
        let n = Notification::from_json(body).expect("an envelope");
        assert_eq!(n.attributes["k"], "v");
        assert!(Notification::from_json(b"{\"Type\":\"Other\"}").is_none());
        assert!(Notification::from_json(b"plain").is_none());
    }
}
