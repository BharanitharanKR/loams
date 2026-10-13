//! The declarative HTTP-API profile engine (CN2 plan Task 7, OR-11).
//!
//! [`HttpApiFactory`] is a [`ConnectorFactory`] over an `http-api` profile
//! (`connectors/profiles/<id>.yaml`, loaded and checked by `loams-flow`'s
//! `profile` module). An instance names one of the profile's resources with
//! `resource` in its config; the profile's `defaults` are merged under the
//! config first.
//!
//! * `list` → a polling source, one split per resource: cursor pagination (a
//!   JSON pointer to the next cursor, sent back as a query parameter) or the
//!   `Link: rel="next"` header; the position is the page and the items already
//!   taken from it, so a restart resumes there. Event ids are derived from the
//!   item's content, so a re-read of an unchanged item has the same id and
//!   `ingest` drops it.
//! * `async_job` → a source that creates a job, polls it until it is done and
//!   pages through its results like a `list`.
//! * `upsert` / `create` → a sink: batches of up to `batch` items,
//!   `Idempotency-Key` (the event id, or a digest of a batch's ids) where the
//!   API accepts it, otherwise dedup by the API's own `key` upsert; either makes
//!   the sink `effectively_once` ([`HttpApiFactory::sink_delivery`]).
//! * `rate_limit` → a token bucket, plus the headers it lists: `Retry-After` on
//!   a 429 or 503, and `*-Remaining` / `*-Reset`.
//! * `webhook` → [`HttpApiFactory::webhook`], for the webhooks source to
//!   route with; the signature schemes are [`crate::webhook`] (Task 20).
//! * `{config.<key>}`, `{auth.<field>}` and `{item:<pointer>}` placeholders
//!   (Task 20, [`template`]); `next_url`, `offset`, last-item and header
//!   cursors, CSV and NDJSON listings; form and NDJSON writes, `select`,
//!   `wrap`, `envelope`, `conflict_ok` and acknowledged writes (Splunk HEC);
//!   `ok` for APIs that report errors in a `200` body (Slack).
//! * `auth` → `oauth2`, `bearer`, `basic`, `header` or `aws-sigv4`, with every
//!   credential a secret the instance config names.
//!
//! Every request goes through `sdk::http::client`, so it is egress-checked
//! after DNS (Review Focus 9), and follows no redirect.

mod auth;
pub mod sigv4;
mod sink;
mod source;
mod template;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Request, Response, StatusCode};
use loams_connector_sdk::http::{HttpClient, classify_status};
use loams_connector_sdk::{
    AuthScheme, ConnectorContext, ConnectorError, ConnectorFactory, Delivery, EnvelopeBuilder,
    HttpMethod, HttpProfile, HttpResource, InstanceKey, Profile, ProfileKind, Rate, RateLimit,
    RateLimiter, SinkFactory, SourceFactory, SplitEnumerator, WebhookSpec,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use self::auth::Authenticator;

/// The event name of an item read from a resource:
/// `io.loams.dev.flow.<connector>.item.v1` (Shared contracts, "Event names":
/// SaaS and HTTP-API profiles emit `item`; a listing may name another with
/// `event`, as Splunk's search export says `log`). R20.10.
pub const ITEM_EVENT: &str = "item";

/// A [`ConnectorFactory`] over one `http-api` profile.
#[derive(Debug, Clone)]
pub struct HttpApiFactory {
    id: &'static str,
    profile: Profile,
    http: HttpProfile,
}

/// Factory ids are `&'static str` ([`ConnectorFactory::id`]) and a profile's id
/// is read at run time, so each distinct id is interned once.
fn intern(id: &str) -> &'static str {
    static IDS: OnceLock<Mutex<BTreeSet<&'static str>>> = OnceLock::new();
    let mut ids = IDS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some(found) = ids.get(id) {
        return found;
    }
    let leaked: &'static str = Box::leak(id.to_owned().into_boxed_str());
    ids.insert(leaked);
    leaked
}

impl HttpApiFactory {
    /// The factory for `profile`, which must be an `http-api` profile with an
    /// `http` block. Its id (the manifest's, `runtime.profile` names the
    /// profile of the same id) becomes the connector id in event types.
    pub fn new(profile: Profile) -> Result<Self, ConnectorError> {
        if profile.kind != ProfileKind::HttpApi {
            return Err(ConnectorError::fatal(format!(
                "profile {:?} is not an http-api profile",
                profile.id
            )));
        }
        let http = profile.http.clone().ok_or_else(|| {
            ConnectorError::fatal(format!("profile {:?} has no http block", profile.id))
        })?;
        Ok(Self {
            id: intern(&profile.id),
            profile,
            http,
        })
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// The delivery a sink on `resource` can declare (OR-7): `effectively_once`
    /// when the API dedups a retry (`Idempotency-Key`, or an upsert by `key`),
    /// `at_least_once` otherwise; `None` when the resource cannot be written.
    pub fn sink_delivery(&self, resource: &str) -> Option<Delivery> {
        let op = write_op(self.http.resources.get(resource)?)?;
        Some(if op.idempotency_key || op.key.is_some() {
            Delivery::EffectivelyOnce
        } else {
            Delivery::AtLeastOnce
        })
    }

    /// The webhook the API sends, which the webhooks source verifies with the
    /// named scheme.
    pub fn webhook(&self) -> Option<&WebhookSpec> {
        self.http.webhook.as_ref()
    }

    fn engine(&self, ctx: &ConnectorContext) -> Result<(Arc<Engine>, Instance), ConnectorError> {
        let config = self.profile.config_with_defaults(&ctx.config);
        let name = config
            .get("resource")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::fatal("config.resource names no resource"))?;
        let resource = self.http.resources.get(name).ok_or_else(|| {
            ConnectorError::fatal(format!(
                "config.resource {name:?} is not a resource of the profile {:?}",
                self.profile.id
            ))
        })?;
        // `{config.<key>}` placeholders are filled once, for the resource the
        // instance runs (another resource may need keys this one does not).
        let bad = |e: serde_json::Error| ConnectorError::fatal(format!("resource {name:?}: {e}"));
        let rendered = template::render(&serde_json::to_value(resource).map_err(bad)?, &config)?;
        let resource: HttpResource = serde_json::from_value(rendered).map_err(bad)?;
        let poll_interval = match config.get("poll_interval_ms") {
            None | Some(Value::Null) => None,
            Some(v) => Some(Duration::from_millis(v.as_u64().ok_or_else(|| {
                ConnectorError::fatal("config.poll_interval_ms must be a positive integer")
            })?)),
        };
        // The profile's own base URL, filled; when it can be filled, its
        // origin is the one a next-page link may name and be rebased from.
        let declared = template::config(&self.http.base_url, &config, false).ok();
        let base_url = match config.get("base_url").and_then(Value::as_str) {
            Some(b) => b.to_string(),
            None => template::config(&self.http.base_url, &config, false)?,
        }
        .trim_end_matches('/')
        .to_string();
        let mut auth_spec = self.http.auth.clone();
        for field in [&mut auth_spec.token_url, &mut auth_spec.region]
            .into_iter()
            .flatten()
        {
            *field = template::config(field, &config, false)?;
        }
        // A sandbox has its own token endpoint (Salesforce's
        // test.salesforce.com), as it has its own `base_url`.
        if let Some(url) = config.get("token_url").and_then(Value::as_str) {
            auth_spec.token_url = Some(url.to_string());
        }
        let client = loams_connector_sdk::http::client(ctx.egress.clone());
        let auth = Authenticator::new(auth_spec, &config, ctx.secrets.clone(), client.clone());
        let gate = Gate::new(&self.http);
        let mut headers = Vec::new();
        for (k, v) in &self.http.headers {
            headers.push((
                HeaderName::from_bytes(k.as_bytes())
                    .map_err(|_| ConnectorError::fatal(format!("header {k:?} is not a header")))?,
                HeaderValue::from_str(v)
                    .map_err(|_| ConnectorError::fatal(format!("header {k} has a bad value")))?,
            ));
        }
        let project = match config.get("projection") {
            None | Some(Value::Null) => None,
            Some(p) => Some(serde_json::from_value(p.clone()).map_err(|e| {
                ConnectorError::fatal(format!("config.projection is not a projection: {e}"))
            })?),
        };
        let channel = channel_id(self.id, &ctx.instance, name);
        let engine = Arc::new(Engine {
            declared_origin: declared.as_deref().and_then(origin),
            base_url,
            headers,
            client,
            auth,
            gate,
            envelope: EnvelopeBuilder::new(self.id, &ctx.instance),
            ok: self.http.ok.clone(),
            project,
            channel,
        });
        Ok((
            engine,
            Instance {
                name: name.to_string(),
                resource,
                poll_interval,
            },
        ))
    }
}

/// The resource an instance runs on, and how it polls.
#[derive(Debug, Clone)]
struct Instance {
    name: String,
    resource: HttpResource,
    poll_interval: Option<Duration>,
}

fn write_op(r: &HttpResource) -> Option<&loams_connector_sdk::WriteOp> {
    r.upsert.as_ref().or(r.create.as_ref())
}

impl ConnectorFactory for HttpApiFactory {
    fn id(&self) -> &'static str {
        self.id
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let (engine, inst) = self.engine(ctx)?;
        let mode = match (&inst.resource.list, &inst.resource.async_job) {
            (Some(list), _) => source::Mode::List(Box::new(list.clone())),
            (None, Some(job)) => source::Mode::Job(Box::new(job.clone())),
            (None, None) => return Ok(None),
        };
        Ok(Some((
            Box::new(source::ResourceEnumerator::new(&inst.name)),
            Box::new(source::HttpApiSourceFactory::new(
                engine,
                mode,
                inst.name,
                inst.poll_interval,
            )),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let (engine, inst) = self.engine(ctx)?;
        let Some(op) = write_op(&inst.resource).cloned() else {
            return Ok(None);
        };
        Ok(Some(Box::new(sink::HttpApiSinkFactory::new(
            engine, op, inst.name,
        ))))
    }
}

/// A stable channel id for an instance's resource, in UUID form (Splunk HEC
/// wants a GUID): the first 16 bytes of SHA-256 over the connector, the
/// instance and the resource.
fn channel_id(connector: &str, instance: &InstanceKey, resource: &str) -> String {
    let d = Sha256::digest(
        format!(
            "{connector}\n{}\n{}\n{resource}",
            instance.namespace, instance.name
        )
        .as_bytes(),
    );
    let h: String = d.iter().take(16).map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// `scheme://host[:port]` of a URL.
fn origin(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    Some(format!("{scheme}://{}", rest.split('/').next()?))
}

/// How [`Engine::send`] treats a request.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SendOpts<'a> {
    /// The body's media type (default `application/json`).
    pub content_type: Option<&'a str>,
    /// A `409 Conflict` counts as success (the item exists).
    pub conflict_ok: bool,
    /// A write: an `ok: false` body is poison rather than fatal.
    pub write: bool,
    /// What error messages name instead of the URL (the path template, so
    /// no resolved secret is ever in one).
    pub label: Option<&'a str>,
}

/// What every source and sink of one instance shares: the client, the
/// authenticator and the rate gate.
pub(crate) struct Engine {
    base_url: String,
    /// The origin of the profile's own `base_url`, when the instance
    /// overrides it: a next-page URL on it is rebased onto `base_url`.
    declared_origin: Option<String>,
    headers: Vec<(HeaderName, HeaderValue)>,
    client: HttpClient,
    auth: Authenticator,
    gate: Gate,
    envelope: EnvelopeBuilder,
    ok: Option<String>,
    project: Option<crate::route::Projection>,
    channel: String,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// `base_url` + `path` (`{id}` replaced) + the query parameters.
    pub(crate) fn url(&self, path: &str, job: Option<&str>, query: &[(&str, &str)]) -> String {
        let path = match job {
            Some(id) => path.replace("{id}", &sigv4::uri_encode(id, true)),
            None => path.to_string(),
        };
        let mut url = format!("{}{path}", self.base_url);
        for (i, (k, v)) in query.iter().enumerate() {
            let sep = if i == 0 && !url.contains('?') {
                '?'
            } else {
                '&'
            };
            url.push(sep);
            url.push_str(&sigv4::uri_encode(k, true));
            url.push('=');
            url.push_str(&sigv4::uri_encode(v, true));
        }
        url
    }

    /// A next-page URL (a `Link` header's, or a body's `next_url`), absolute
    /// or relative to the origin, as the URL to fetch: it must be on
    /// `base_url`'s origin, or on the profile's own origin when the instance
    /// overrides `base_url` (a sandbox, or a fixture replay), in which case it
    /// is rebased there. `None` for any other origin: never followed.
    pub(crate) fn next_page(&self, url: &str) -> Option<String> {
        let here = origin(&self.base_url)?;
        if url.starts_with('/') {
            return Some(format!("{here}{url}"));
        }
        let there = origin(url)?;
        if there == here {
            return Some(url.to_string());
        }
        match &self.declared_origin {
            Some(declared) if *declared == there => Some(format!("{here}{}", &url[there.len()..])),
            _ => None,
        }
    }

    /// The channel id of the instance's resource (an ack protocol's).
    pub(crate) fn channel(&self) -> &str {
        &self.channel
    }

    /// The instance's projection (`config.projection`), if any.
    pub(crate) fn project(&self) -> Option<&crate::route::Projection> {
        self.project.as_ref()
    }

    /// `template` with its `{auth.<field>}` placeholders resolved through the
    /// instance's secrets (percent-encoded in a path).
    pub(crate) async fn with_secrets(
        &self,
        template: &str,
        path: bool,
    ) -> Result<String, ConnectorError> {
        let fields = template::auth_fields(template);
        if fields.is_empty() {
            return Ok(template.to_string());
        }
        let mut values = std::collections::BTreeMap::new();
        for f in fields {
            let secret = self.auth.secret(&f).await?;
            let text = secret
                .expose_str()
                .ok_or_else(|| ConnectorError::auth(format!("auth.{f} is not UTF-8")))?
                .to_string();
            values.insert(f, text);
        }
        template::auth(template, |f| values.get(f).cloned(), path)
    }

    /// One request through the gate, the authenticator and the egress-checked
    /// client. A status that is not a success becomes the error of its class.
    pub(crate) async fn send(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Bytes>,
        extra: &[(HeaderName, HeaderValue)],
        opts: SendOpts<'_>,
    ) -> Result<Response<Bytes>, ConnectorError> {
        self.gate.wait().await;
        let mut builder = Request::builder().method(method.as_str()).uri(url);
        for (k, v) in self.headers.iter().chain(extra) {
            builder = builder.header(k, v);
        }
        if body.is_some() {
            builder = builder.header(
                CONTENT_TYPE,
                opts.content_type.unwrap_or("application/json"),
            );
        }
        let label = opts.label.unwrap_or(url);
        let mut req = builder
            .body(body.unwrap_or_default())
            .map_err(|e| ConnectorError::fatal(format!("bad request to {label}: {e}")))?;
        self.auth.apply(&mut req).await?;
        let path = match opts.label {
            Some(l) => l.split('?').next().unwrap_or(l).to_string(),
            None => req.uri().path().to_string(),
        };
        let resp = self.client.send(req).await?;
        self.gate.observe(&resp);
        let status = resp.status();
        if status == StatusCode::UNAUTHORIZED && self.auth.scheme() == AuthScheme::Oauth2 {
            // The token was revoked or expired early: the next attempt fetches
            // a new one.
            self.auth.invalidate().await;
            return Err(ConnectorError::retryable(format!(
                "HTTP 401 from {} {path}: the access token was refused, refreshing it",
                method.as_str()
            )));
        }
        let after = self
            .gate
            .retry_after(&resp)
            .map(|d| d.as_secs().to_string());
        if status == StatusCode::CONFLICT && opts.conflict_ok {
            self.gate.succeeded();
            return Ok(resp);
        }
        match classify_status(status, after.as_deref()) {
            None => {
                self.gate.succeeded();
                self.check_ok(&resp, method, &path, opts.write)?;
                Ok(resp)
            }
            Some(class) => Err(ConnectorError::new(
                class,
                format!("HTTP {status} from {} {path}", method.as_str()),
            )),
        }
    }
}

impl Engine {
    /// `ok`: a JSON body whose pointer is `false` is an error, named by the
    /// body's `error` string (Slack's shape): an authentication error is
    /// `Auth`, any other is poison for a write (the item goes to the DLQ) and
    /// fatal for a read.
    fn check_ok(
        &self,
        resp: &Response<Bytes>,
        method: HttpMethod,
        path: &str,
        write: bool,
    ) -> Result<(), ConnectorError> {
        let Some(ptr) = &self.ok else {
            return Ok(());
        };
        let Ok(doc) = serde_json::from_slice::<Value>(resp.body()) else {
            return Ok(());
        };
        if doc.pointer(ptr) != Some(&Value::Bool(false)) {
            return Ok(());
        }
        let error = doc
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown_error");
        let msg = format!("{} {path} answered {ptr} = false: {error}", method.as_str());
        const AUTH: &[&str] = &[
            "invalid_auth",
            "not_authed",
            "token_revoked",
            "token_expired",
            "account_inactive",
            "missing_scope",
            "no_permission",
        ];
        Err(if AUTH.contains(&error) {
            ConnectorError::auth(msg)
        } else if write {
            ConnectorError::poison(msg)
        } else {
            ConnectorError::fatal(msg)
        })
    }
}

/// The profile's `rate_limit`: a token bucket of `requests` per `per_seconds`,
/// and a hold set by the response headers it lists.
#[derive(Debug)]
struct Gate {
    limiter: Option<RateLimiter>,
    window: Duration,
    /// The listed headers, lower case.
    honour: Vec<String>,
    hold_until: Mutex<Option<Instant>>,
}

impl Gate {
    fn new(http: &HttpProfile) -> Self {
        let Some(rl) = &http.rate_limit else {
            return Self {
                limiter: None,
                window: Duration::ZERO,
                honour: Vec::new(),
                hold_until: Mutex::default(),
            };
        };
        let per = Duration::from_secs(u64::from(rl.per_seconds.max(1)));
        Self {
            limiter: Some(RateLimiter::new(RateLimit {
                requests: Some(Rate {
                    amount: f64::from(rl.requests.max(1)),
                    per,
                }),
                bytes: None,
            })),
            window: per,
            honour: rl
                .honour_headers
                .iter()
                .map(|h| h.to_ascii_lowercase())
                .collect(),
            hold_until: Mutex::default(),
        }
    }

    async fn wait(&self) {
        let until = *self.hold_until.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(until) = until {
            tokio::time::sleep_until(until).await;
        }
        if let Some(l) = &self.limiter {
            l.acquire(0).await;
        }
    }

    fn succeeded(&self) {
        if let Some(l) = &self.limiter {
            l.succeeded();
        }
    }

    fn hold(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut h = self.hold_until.lock().unwrap_or_else(|p| p.into_inner());
        *h = Some(h.map_or(until, |u| u.max(until)));
    }

    fn honours(&self, name: &str) -> bool {
        self.honour.iter().any(|h| h == name)
    }

    /// `Retry-After` (seconds or an HTTP date), when the profile lists it.
    fn retry_after(&self, resp: &Response<Bytes>) -> Option<Duration> {
        if !self.honours("retry-after") {
            return None;
        }
        let v = resp
            .headers()
            .get(http::header::RETRY_AFTER)?
            .to_str()
            .ok()?
            .trim();
        v.parse::<u64>().map(Duration::from_secs).ok().or_else(|| {
            let at = httpdate::parse_http_date(v).ok()?;
            Some(at.duration_since(SystemTime::now()).unwrap_or_default())
        })
    }

    /// Hold the next request as the response's listed headers say.
    fn observe(&self, resp: &Response<Bytes>) {
        let status = resp.status();
        if (status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE)
            && let Some(after) = self.retry_after(resp)
        {
            if let Some(l) = &self.limiter
                && status == StatusCode::TOO_MANY_REQUESTS
            {
                l.throttled(after);
            }
            self.hold(after);
        }
        let number = |name: &str| -> Option<f64> {
            resp.headers().get(name)?.to_str().ok()?.trim().parse().ok()
        };
        let exhausted = self
            .honour
            .iter()
            .filter(|h| h.ends_with("-remaining"))
            .any(|h| number(h).is_some_and(|n| n <= 0.0));
        if exhausted {
            let reset = self
                .honour
                .iter()
                .filter(|h| h.ends_with("-reset"))
                .find_map(|h| number(h))
                .map(|n| {
                    // An epoch timestamp, or seconds from now.
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_or(0.0, |d| d.as_secs_f64());
                    if n > 1e9 {
                        (n - now).max(0.0)
                    } else {
                        n.max(0.0)
                    }
                })
                .map(Duration::from_secs_f64);
            self.hold(reset.unwrap_or(self.window));
        }
    }
}

/// `value` at the JSON pointer `pointer`, building objects on the way.
pub(crate) fn put_at(root: &mut Value, pointer: &str, value: Value) {
    let mut at = root;
    for token in pointer.split('/').skip(1) {
        let token = token.replace("~1", "/").replace("~0", "~");
        if !at.is_object() {
            *at = Value::Object(serde_json::Map::new());
        }
        let Value::Object(map) = at else {
            return;
        };
        at = map.entry(token).or_insert(Value::Null);
    }
    *at = value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn put_at_builds_objects() {
        let mut v = Value::Null;
        put_at(&mut v, "/inputs", json!([1]));
        assert_eq!(v, json!({"inputs": [1]}));
        let mut v = Value::Null;
        put_at(&mut v, "", json!([1]));
        assert_eq!(v, json!([1]));
        let mut v = Value::Null;
        put_at(&mut v, "/a/b~1c", json!(2));
        assert_eq!(v, json!({"a": {"b/c": 2}}));
    }

    #[test]
    fn ids_are_interned() {
        assert!(std::ptr::eq(intern("hubspot"), intern("hubspot")));
    }
}
