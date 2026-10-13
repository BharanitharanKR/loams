//! Azure Event Grid (`azure-event-grid`, CN2 Task 19).
//!
//! **Sink:** posts batches in the CloudEvents schema
//! (`application/cloudevents-batch+json`) to a topic `endpoint`, with the
//! topic's access key (`auth.key`, sent as `aeg-sas-key`) or an Entra ID
//! token (`auth.tenant_id`, and `auth.client_id` and `auth.client_secret`
//! naming secrets: the client-credentials grant for
//! `https://eventgrid.azure.net/.default`). A namespace topic
//! (`namespace_endpoint` and `topic`) is published to with
//! `Authorization: SharedAccessKey`. Event Grid takes a batch whole, so a
//! refused batch is `Poison` and the pipeline isolates the event.
//!
//! **Source**, `delivery: webhook` (the default): Event Grid's push delivery
//! to the webhooks route ([`EventGridWebhook`]). It answers both validation
//! handshakes, the Event Grid schema's `SubscriptionValidationEvent`
//! (`{"validationResponse": code}`) and the CloudEvents schema's HTTP
//! `OPTIONS` abuse protection (`WebHook-Allowed-Origin`); CloudEvents pass
//! through, and Event Grid schema events become `event`. When
//! `auth.webhook_key` names a secret, a request must carry it as the
//! `code` query parameter (set in the subscription's endpoint URL).
//!
//! **Source**, `delivery: pull`: a namespace topic's event subscription
//! ([`NamespaceQueue`]: `receive`, `acknowledge` after the Fabric,
//! `renewLock` while waiting, `release` on stop), which is also the queue
//! the object-storage family can read Blob notifications through (R19.4).

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::http::HttpClient;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, EnvelopeBuilder, ErrorClass,
    InstanceKey, SinkFactory, SourceFactory, SplitEnumerator,
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::aws::sigv4::uri_encode;
use crate::ce;
use crate::config::{LazySecret, auth_str, str_opt, str_req, u64_in};
use crate::gcp::token_response;
use crate::push::{
    self, PushInbox, WebhookHandler, WebhookRegistry, constant_time_eq, json_response, query_param,
    response,
};
use crate::queue::{Queue, QueueMessage};
use crate::sink::{BatchPut, BatchSinkFactory, EntryOutcome};
use crate::sqs::queue_source;

pub const ID: &str = "azure-event-grid";
/// The event an Event Grid schema event becomes.
pub const EVENT: &str = "event";
const NAMESPACE_API: &str = "2024-06-01";
const ENTRA_SCOPE: &str = "https://eventgrid.azure.net/.default";
const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com";
const VALIDATION_EVENT: &str = "Microsoft.EventGrid.SubscriptionValidationEvent";

// -------------------------------------------------------------------- auth --

/// How requests to Event Grid are authorised.
#[derive(Clone)]
enum Auth {
    /// A custom or domain topic's access key: `aeg-sas-key`.
    TopicKey(LazySecret),
    /// A namespace's access key: `Authorization: SharedAccessKey`.
    NamespaceKey(LazySecret),
    /// Entra ID client credentials: a bearer token.
    Entra(Arc<EntraTokens>),
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Auth::TopicKey(_) => "TopicKey",
            Auth::NamespaceKey(_) => "NamespaceKey",
            Auth::Entra(_) => "Entra",
        })
    }
}

/// Entra ID's client-credentials grant, cached.
pub struct EntraTokens {
    url: String,
    client_id: LazySecret,
    client_secret: LazySecret,
    http: HttpClient,
    cached: Mutex<Option<(String, Instant)>>,
}

impl fmt::Debug for EntraTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntraTokens")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl EntraTokens {
    async fn bearer(&self) -> Result<String, ConnectorError> {
        let mut cached = self.cached.lock().await;
        if let Some((t, until)) = cached.as_ref()
            && Instant::now() < *until
        {
            return Ok(t.clone());
        }
        let id = self.client_id.text().await?;
        let secret = self.client_secret.text().await?;
        let body = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
            uri_encode(id.expose(), true),
            uri_encode(secret.expose(), true),
            uri_encode(ENTRA_SCOPE, true)
        );
        let req = http::Request::post(&self.url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Bytes::from(body))
            .map_err(|e| ConnectorError::fatal(format!("bad Entra authority: {e}")))?;
        let (token, ttl) = token_response(self.http.send(req).await?)?;
        *cached = Some((
            token.clone(),
            Instant::now() + ttl.saturating_sub(Duration::from_secs(60)),
        ));
        Ok(token)
    }
}

fn auth_from(
    ctx: &ConnectorContext,
    namespace: bool,
    http: &HttpClient,
) -> Result<Auth, ConnectorError> {
    let c = &ctx.config;
    if let Some(key) = LazySecret::from_auth(c, "key", &ctx.secrets) {
        return Ok(if namespace {
            Auth::NamespaceKey(key)
        } else {
            Auth::TopicKey(key)
        });
    }
    match (
        auth_str(c, "tenant_id"),
        LazySecret::from_auth(c, "client_id", &ctx.secrets),
        LazySecret::from_auth(c, "client_secret", &ctx.secrets),
    ) {
        (Some(tenant), Some(client_id), Some(client_secret)) => {
            let authority = auth_str(c, "authority").unwrap_or_else(|| DEFAULT_AUTHORITY.into());
            Ok(Auth::Entra(Arc::new(EntraTokens {
                url: format!(
                    "{}/{}/oauth2/v2.0/token",
                    authority.trim_end_matches('/'),
                    uri_encode(&tenant, true)
                ),
                client_id,
                client_secret,
                http: http.clone(),
                cached: Mutex::new(None),
            })))
        }
        _ => Err(ConnectorError::fatal(
            "config `auth.key`, or `auth.tenant_id`, `auth.client_id` and `auth.client_secret`, is required",
        )),
    }
}

async fn authorise(
    auth: &Auth,
    mut req: http::request::Builder,
) -> Result<http::request::Builder, ConnectorError> {
    req = match auth {
        Auth::TopicKey(k) => req.header("aeg-sas-key", k.text().await?.expose()),
        Auth::NamespaceKey(k) => req.header(
            "authorization",
            format!("SharedAccessKey {}", k.text().await?.expose()),
        ),
        Auth::Entra(t) => req.header("authorization", format!("Bearer {}", t.bearer().await?)),
    };
    Ok(req)
}

/// Send `body` (JSON) with the instance's auth, as `content_type`.
async fn send(
    http: &HttpClient,
    auth: &Auth,
    url: &str,
    content_type: &str,
    body: Bytes,
) -> Result<http::Response<Bytes>, ConnectorError> {
    let req = http::Request::post(url).header("content-type", content_type);
    let mut req = authorise(auth, req)
        .await?
        .body(body)
        .map_err(|e| ConnectorError::fatal(format!("bad Event Grid request: {e}")))?;
    for v in req.headers_mut().values_mut() {
        v.set_sensitive(true);
    }
    http.send(req).await
}

fn status_error(resp: &http::Response<Bytes>, what: &str) -> ConnectorError {
    let mut detail = String::from_utf8_lossy(resp.body()).into_owned();
    detail.truncate(300);
    let class = match resp.status().as_u16() {
        400 | 413 => ErrorClass::Poison,
        _ => loams_connector_sdk::http::error_for_response(resp)
            .map(|e| e.class)
            .unwrap_or(ErrorClass::Fatal),
    };
    ConnectorError::new(class, format!("{what}: HTTP {}: {detail}", resp.status()))
}

// -------------------------------------------------------------------- sink --

/// The sink's batch post.
#[derive(Debug, Clone)]
pub struct EventGridPut {
    url: String,
    auth: Auth,
    http: HttpClient,
}

impl EventGridPut {
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let c = &ctx.config;
        let http = loams_connector_sdk::http::client(ctx.egress.clone());
        let (url, namespace) = match (str_opt(c, "endpoint"), str_opt(c, "namespace_endpoint")) {
            (Some(e), _) => (e, false),
            (None, Some(ns)) => (
                format!(
                    "{}/topics/{}:publish?api-version={NAMESPACE_API}",
                    ns.trim_end_matches('/'),
                    str_req(c, "topic")?
                ),
                true,
            ),
            (None, None) => {
                return Err(ConnectorError::fatal(
                    "config `endpoint` (a topic) or `namespace_endpoint` and `topic` is required",
                ));
            }
        };
        Ok(Self {
            auth: auth_from(ctx, namespace, &http)?,
            url,
            http,
        })
    }
}

#[async_trait]
impl BatchPut for EventGridPut {
    fn max_entries(&self) -> usize {
        5000
    }

    fn max_bytes(&self) -> usize {
        1_000_000
    }

    fn entry_size(&self, ev: &CloudEvent) -> usize {
        ce::to_structured_bytes(ev).len() + 1
    }

    async fn put(&mut self, events: &[CloudEvent]) -> Result<Vec<EntryOutcome>, ConnectorError> {
        let batch: Vec<Value> = events.iter().map(ce::to_structured).collect();
        let resp = send(
            &self.http,
            &self.auth,
            &self.url,
            "application/cloudevents-batch+json; charset=utf-8",
            Bytes::from(Value::Array(batch).to_string()),
        )
        .await?;
        if resp.status().is_success() {
            Ok(vec![EntryOutcome::Delivered; events.len()])
        } else {
            Err(status_error(&resp, "publishing to Event Grid"))
        }
    }
}

// ----------------------------------------------------------------- webhook --

/// The webhook endpoint of an Event Grid push source.
pub struct EventGridWebhook {
    key: Option<LazySecret>,
    inbox: PushInbox,
    envelope: EnvelopeBuilder,
    resource: String,
}

impl fmt::Debug for EventGridWebhook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventGridWebhook")
            .field("resource", &self.resource)
            .finish_non_exhaustive()
    }
}

/// What a POST carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// The Event Grid schema's subscription validation: answer the code.
    Validation(String),
    Events(Vec<CloudEvent>),
}

impl EventGridWebhook {
    async fn authorised(&self, uri: &http::Uri) -> bool {
        let Some(key) = &self.key else {
            return true;
        };
        let Ok(want) = key.text().await else {
            return false;
        };
        query_param(uri, "code")
            .is_some_and(|got| constant_time_eq(got.as_bytes(), want.expose().as_bytes()))
    }

    /// Parse a POST body (either schema; one event or an array).
    pub fn parse(&self, content_type: &str, body: &[u8]) -> Result<Delivery, String> {
        let v: Value = serde_json::from_slice(body).map_err(|_| "not JSON".to_owned())?;
        let items = match v {
            Value::Array(a) => a,
            one @ Value::Object(_) => vec![one],
            _ => return Err("not an event or an array of events".into()),
        };
        let cloudevents = content_type.contains("cloudevents")
            || items
                .first()
                .is_some_and(|i| i.get("specversion").is_some());
        let mut events = Vec::with_capacity(items.len());
        for item in &items {
            if cloudevents {
                let bytes = serde_json::to_vec(item).map_err(|e| e.to_string())?;
                let ev = self.envelope.from_structured_json(&bytes).ok_or_else(|| {
                    "a CloudEvent without specversion, id, source or type".to_owned()
                })?;
                events.push(ev);
                continue;
            }
            let s = |k: &str| item.get(k).and_then(Value::as_str);
            let (Some(id), Some(ty)) = (s("id"), s("eventType")) else {
                return Err("an Event Grid event without id or eventType".into());
            };
            if ty == VALIDATION_EVENT {
                let code = item["data"]["validationCode"]
                    .as_str()
                    .ok_or("a validation event without validationCode")?;
                return Ok(Delivery::Validation(code.to_owned()));
            }
            let mut ev = self
                .envelope
                .event(EVENT, &self.resource, id.to_owned())
                .with_data("application/json", item.to_string());
            if let Some(sub) = s("subject").filter(|x| !x.is_empty()) {
                ev = ev.with_subject(sub.to_owned());
            }
            if let Some(t) = s("eventTime") {
                ev = ev.with_time(t.to_owned());
            }
            events.push(ev);
        }
        Ok(Delivery::Events(events))
    }
}

#[async_trait]
impl WebhookHandler for EventGridWebhook {
    async fn handle(&self, req: http::Request<Bytes>) -> http::Response<Bytes> {
        if !self.authorised(req.uri()).await {
            return response(401, "");
        }
        if req.method() == http::Method::OPTIONS {
            // CloudEvents webhook abuse protection (Event Grid's CloudEvents
            // schema validation).
            let Some(origin) = req
                .headers()
                .get("webhook-request-origin")
                .and_then(|v| v.to_str().ok())
            else {
                return response(400, "no WebHook-Request-Origin");
            };
            let mut r = response(200, "");
            if let Ok(v) = http::HeaderValue::from_str(origin) {
                r.headers_mut().insert("webhook-allowed-origin", v);
            }
            r.headers_mut()
                .insert("webhook-allowed-rate", http::HeaderValue::from_static("*"));
            return r;
        }
        if req.method() != http::Method::POST {
            return response(405, "");
        }
        let ct = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        match self.parse(&ct, req.body()) {
            Err(why) => response(400, &why),
            Ok(Delivery::Validation(code)) => {
                json_response(200, &json!({"validationResponse": code}))
            }
            Ok(Delivery::Events(events)) => match self.inbox.deliver(events).await {
                Ok(()) => response(200, ""),
                Err(e) => e.response(),
            },
        }
    }
}

// ------------------------------------------------------------ pull queue --

/// A namespace topic's event subscription as a [`Queue`] (pull delivery).
#[derive(Debug, Clone)]
pub struct NamespaceQueue {
    base: String,
    name: String,
    auth: Auth,
    http: HttpClient,
    lock: u64,
    wait: u64,
}

impl NamespaceQueue {
    pub fn from_context(ctx: &ConnectorContext) -> Result<Self, ConnectorError> {
        let c = &ctx.config;
        let ns = str_req(c, "namespace_endpoint")?;
        let topic = str_req(c, "topic")?;
        let sub = str_req(c, "subscription")?;
        let http = loams_connector_sdk::http::client(ctx.egress.clone())
            .with_timeout(Duration::from_secs(90));
        Ok(Self {
            base: format!(
                "{}/topics/{}/eventsubscriptions/{}",
                ns.trim_end_matches('/'),
                uri_encode(&topic, true),
                uri_encode(&sub, true)
            ),
            name: sub,
            auth: auth_from(ctx, true, &http)?,
            http,
            lock: u64_in(c, "lock_seconds", 60, 1..=300)?,
            wait: u64_in(c, "max_wait_seconds", 10, 0..=60)?,
        })
    }

    async fn post(&self, verb: &str, query: &str, body: Value) -> Result<Value, ConnectorError> {
        let url = format!("{}:{verb}?api-version={NAMESPACE_API}{query}", self.base);
        let resp = send(
            &self.http,
            &self.auth,
            &url,
            "application/json",
            Bytes::from(body.to_string()),
        )
        .await?;
        if !resp.status().is_success() {
            let mut e = status_error(&resp, verb);
            if e.class == ErrorClass::Poison {
                e.class = ErrorClass::Fatal;
            }
            return Err(e);
        }
        Ok(serde_json::from_slice(resp.body()).unwrap_or(Value::Null))
    }

    async fn tokens(&self, verb: &str, handles: &[String]) -> Result<(), ConnectorError> {
        for chunk in handles.chunks(100) {
            let out = self.post(verb, "", json!({"lockTokens": chunk})).await?;
            for f in out["failedLockTokens"].as_array().into_iter().flatten() {
                tracing::warn!(subscription = %self.name, "{verb}: {}", f["error"]["code"].as_str().unwrap_or("failed"));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Queue for NamespaceQueue {
    fn name(&self) -> &str {
        &self.name
    }

    fn lease(&self) -> Duration {
        Duration::from_secs(self.lock)
    }

    fn max_receive(&self) -> usize {
        100
    }

    async fn receive(&self, max: usize) -> Result<Vec<QueueMessage>, ConnectorError> {
        let out = self
            .post(
                "receive",
                &format!("&maxEvents={}&maxWaitTime={}", max.clamp(1, 100), self.wait),
                json!({}),
            )
            .await?;
        Ok(out["value"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| {
                let event = &d["event"];
                Some(QueueMessage {
                    handle: d["brokerProperties"]["lockToken"].as_str()?.to_owned(),
                    id: event["id"].as_str().unwrap_or_default().to_owned(),
                    body: Bytes::from(event.to_string()),
                    attributes: [("content-type".to_owned(), ce::STRUCTURED.to_owned())].into(),
                    time: event["time"].as_str().map(str::to_owned),
                    subject: event["subject"].as_str().map(str::to_owned),
                })
            })
            .collect())
    }

    async fn ack(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.tokens("acknowledge", handles).await
    }

    async fn extend(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.tokens("renewLock", handles).await
    }

    async fn release(&self, handles: &[String]) -> Result<(), ConnectorError> {
        self.tokens("release", handles).await
    }
}

/// A pulled event: a CloudEvent passes through; anything else is `event`.
pub fn decode_pulled(
    env: &EnvelopeBuilder,
    resource: &str,
    m: &QueueMessage,
) -> Result<Vec<CloudEvent>, ConnectorError> {
    if let Some(ev) = ce::passthrough(env, &m.attributes, &m.body) {
        return Ok(vec![ev]);
    }
    let mut ev = env
        .event(EVENT, resource, m.id.clone())
        .with_data("application/json", m.body.clone());
    if let Some(s) = &m.subject {
        ev = ev.with_subject(s.clone());
    }
    Ok(vec![ev])
}

// ----------------------------------------------------------------- factory --

/// The `azure-event-grid` factory. Webhook sources register their
/// [`EventGridWebhook`]s, which the webhooks route reaches through
/// [`EventGridFactory::webhook`].
#[derive(Debug, Clone, Default)]
pub struct EventGridFactory {
    hooks: Arc<WebhookRegistry>,
}

impl EventGridFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn webhook(&self, instance: &InstanceKey) -> Option<Arc<dyn WebhookHandler>> {
        self.hooks.get(instance)
    }
}

impl ConnectorFactory for EventGridFactory {
    fn id(&self) -> &'static str {
        ID
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let c = &ctx.config;
        match str_opt(c, "delivery").as_deref() {
            None | Some("webhook") => {
                let (inbox, sources) = push::channel(64, Duration::from_secs(25));
                let hook = EventGridWebhook {
                    key: LazySecret::from_auth(c, "webhook_key", &ctx.secrets),
                    inbox,
                    envelope: EnvelopeBuilder::new(ID, &ctx.instance),
                    resource: str_opt(c, "topic").unwrap_or_else(|| "webhook".into()),
                };
                self.hooks.insert(ctx.instance.clone(), Arc::new(hook));
                Ok(Some((Box::new(push::OneSplit), Box::new(sources))))
            }
            Some("pull") => {
                let q = Arc::new(NamespaceQueue::from_context(ctx)?);
                queue_source(ctx, ID, q, Arc::new(decode_pulled)).map(Some)
            }
            Some(other) => Err(ConnectorError::fatal(format!(
                "config `delivery` must be `webhook` or `pull`, not `{other}`"
            ))),
        }
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(BatchSinkFactory(
            EventGridPut::from_context(ctx)?,
        ))))
    }
}
