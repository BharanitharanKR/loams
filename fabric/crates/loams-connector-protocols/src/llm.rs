//! OpenAI and Anthropic: native HTTP sinks with `sink.reply` (OR-19), over
//! the providers' public REST APIs through the SDK's egress-checked client
//! (ruling R21.10).
//!
//! * **OpenAI** — `POST /v1/chat/completions` with `Authorization: Bearer`.
//! * **Anthropic** — `POST /v1/messages` with `x-api-key` and
//!   `anthropic-version: 2023-06-01`.
//!
//! Each input event is one call. The prompt is the instance's template
//! (`prompt.system`, `prompt.user`) filled from the event
//! ([`crate::template`]); `params` adds fixed, non-secret request fields
//! (`temperature`, `output_config`, …). The answer becomes one reply event
//! (`io.loams.dev.flow.<openai|anthropic>.reply.v1`) whose data is
//! `{provider, model, id, text, stop_reason, usage}`: built from the answer
//! only, so it never carries the key ([`crate::reply`]), and the reply keeps
//! the input's `subject`.
//!
//! Rate limits come from the providers' headers: `retry-after` on 429 (and
//! Anthropic's 529), and a `*-remaining-requests: 0` /
//! `anthropic-ratelimit-requests-remaining: 0` with its reset holds every
//! later request of the instance until the window reopens.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, Response};
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, ErrorClass, SinkFactory,
    SinkHandle, SourceFactory, SplitEnumerator,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;

use crate::client::{AuthSpec, RestClient, classify, json_bytes, parse_body, retry_after};
use crate::reply::{Caller, Reply, ReplySink, ReplyTopics};
use crate::template;

/// Which provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    OpenAi,
    Anthropic,
}

impl Provider {
    pub fn id(self) -> &'static str {
        match self {
            Provider::OpenAi => "openai",
            Provider::Anthropic => "anthropic",
        }
    }

    fn default_base(self) -> &'static str {
        match self {
            Provider::OpenAi => "https://api.openai.com",
            Provider::Anthropic => "https://api.anthropic.com",
        }
    }

    fn path(self) -> &'static str {
        match self {
            Provider::OpenAi => "/v1/chat/completions",
            Provider::Anthropic => "/v1/messages",
        }
    }
}

/// The prompt templates.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Prompt {
    #[serde(default)]
    pub system: Option<String>,
    pub user: String,
}

/// An OpenAI or Anthropic instance's config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LlmConfig {
    pub model: String,
    /// The **name** of the secret holding the API key.
    pub api_key: String,
    pub prompt: Prompt,
    /// Anthropic requires it; OpenAI sends it as `max_completion_tokens`
    /// when set.
    #[serde(default)]
    pub max_tokens: Option<u64>,
    /// Fixed request fields merged into every request body. Must not name a
    /// field the connector sets (`model`, `messages`, `system`,
    /// `max_tokens`, `max_completion_tokens`, `stream`).
    #[serde(default)]
    pub params: Map<String, Value>,
    /// The provider's API root, for a gateway or a test fixture.
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub reply_topic: Option<String>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    /// The SDK pipeline's own settings (`pipeline.*`), read by the runtime.
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn default_timeout() -> u64 {
    120_000
}

const RESERVED: &[&str] = &[
    "model",
    "messages",
    "system",
    "max_tokens",
    "max_completion_tokens",
    "stream",
];

impl LlmConfig {
    pub fn parse(provider: Provider, v: &Value) -> Result<Self, ConnectorError> {
        let cfg: LlmConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("{} config: {e}", provider.id())))?;
        if cfg.model.trim().is_empty() {
            return Err(ConnectorError::fatal("model is required"));
        }
        if provider == Provider::Anthropic && cfg.max_tokens.is_none() {
            return Err(ConnectorError::fatal(
                "max_tokens is required for the Anthropic Messages API",
            ));
        }
        if let Some(k) = cfg.params.keys().find(|k| RESERVED.contains(&k.as_str())) {
            return Err(ConnectorError::fatal(format!(
                "params must not set {k}; the connector sets it"
            )));
        }
        template::check(&cfg.prompt.user)?;
        if let Some(s) = &cfg.prompt.system {
            template::check(s)?;
        }
        Ok(cfg)
    }
}

/// The request body for one event.
pub fn request_body(
    provider: Provider,
    cfg: &LlmConfig,
    ev: &CloudEvent,
) -> Result<Value, ConnectorError> {
    let data = template::data_json(ev)?;
    let user = template::render(&cfg.prompt.user, ev, &data)?;
    let system = match &cfg.prompt.system {
        Some(s) => Some(template::render(s, ev, &data)?),
        None => None,
    };
    let mut body = Map::new();
    body.insert("model".into(), json!(cfg.model));
    match provider {
        Provider::OpenAi => {
            let mut messages = Vec::new();
            if let Some(s) = system {
                messages.push(json!({"role": "system", "content": s}));
            }
            messages.push(json!({"role": "user", "content": user}));
            body.insert("messages".into(), Value::Array(messages));
            if let Some(m) = cfg.max_tokens {
                body.insert("max_completion_tokens".into(), json!(m));
            }
        }
        Provider::Anthropic => {
            body.insert("max_tokens".into(), json!(cfg.max_tokens.unwrap_or(1024)));
            if let Some(s) = system {
                body.insert("system".into(), json!(s));
            }
            body.insert(
                "messages".into(),
                json!([{"role": "user", "content": user}]),
            );
        }
    }
    for (k, v) in &cfg.params {
        body.insert(k.clone(), v.clone());
    }
    Ok(Value::Object(body))
}

/// The reply data of an answer.
pub fn reply_data(provider: Provider, answer: &Value) -> Result<Value, ConnectorError> {
    let (text, stop) = match provider {
        Provider::OpenAi => {
            let choice = answer["choices"]
                .get(0)
                .ok_or_else(|| ConnectorError::retryable("the answer has no choices"))?;
            (
                choice["message"]["content"]
                    .as_str()
                    .unwrap_or("")
                    .to_owned(),
                choice["finish_reason"].clone(),
            )
        }
        Provider::Anthropic => {
            let blocks = answer["content"]
                .as_array()
                .ok_or_else(|| ConnectorError::retryable("the answer has no content"))?;
            let text: String = blocks
                .iter()
                .filter(|b| b["type"] == "text")
                .filter_map(|b| b["text"].as_str())
                .collect();
            (text, answer["stop_reason"].clone())
        }
    };
    Ok(json!({
        "provider": provider.id(),
        "model": answer["model"],
        "id": answer["id"],
        "text": text,
        "stop_reason": stop,
        "usage": answer["usage"],
    }))
}

/// Seconds in an OpenAI reset value: `1s`, `6m0s`, `20ms`, `1h2m3.5s`.
pub fn go_duration(s: &str) -> Option<Duration> {
    let mut total = 0f64;
    let mut num = String::new();
    let mut chars = s.trim().chars().peekable();
    let mut any = false;
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            continue;
        }
        let n: f64 = num.parse().ok()?;
        num.clear();
        let unit = match c {
            'h' => 3600.0,
            'm' if chars.peek() == Some(&'s') => {
                chars.next();
                0.001
            }
            'm' => 60.0,
            's' => 1.0,
            _ => return None,
        };
        total += n * unit;
        any = true;
    }
    if !num.is_empty() {
        total += num.parse::<f64>().ok()?;
        any = true;
    }
    (any && total.is_finite()).then(|| Duration::from_secs_f64(total.clamp(0.0, 3600.0)))
}

/// An RFC 3339 UTC instant (`2026-10-13T08:00:30Z`, fractions ignored) as
/// seconds since the epoch.
pub fn rfc3339_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let (y, m, day): (i64, u32, u32) = (
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
        d.next()?.parse().ok()?,
    );
    let time = time.trim_end_matches('Z');
    let time = time.split(['+', '.']).next()?;
    let mut t = time.split(':');
    let (hh, mm, ss): (u64, u64, u64) = (
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
        t.next()?.parse().ok()?,
    );
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (i64::from(m) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days)
        .ok()
        .map(|d| d * 86_400 + hh * 3600 + mm * 60 + ss)
}

/// How long the provider's headers say to wait before the next request.
pub fn header_hold(provider: Provider, headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let get = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
    match provider {
        Provider::OpenAi => {
            let exhausted = ["requests", "tokens"].into_iter().find(|kind| {
                get(&format!("x-ratelimit-remaining-{kind}"))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    == Some(0)
            })?;
            go_duration(get(&format!("x-ratelimit-reset-{exhausted}"))?)
        }
        Provider::Anthropic => {
            let exhausted = ["requests", "tokens", "input-tokens", "output-tokens"]
                .into_iter()
                .find(|kind| {
                    get(&format!("anthropic-ratelimit-{kind}-remaining"))
                        .and_then(|v| v.trim().parse::<u64>().ok())
                        == Some(0)
                })?;
            let reset = rfc3339_secs(get(&format!("anthropic-ratelimit-{exhausted}-reset"))?)?;
            let now = now.duration_since(UNIX_EPOCH).ok()?.as_secs();
            Some(Duration::from_secs(reset.saturating_sub(now).min(3600)))
        }
    }
}

/// One provider call per event.
pub struct LlmCaller {
    provider: Provider,
    cfg: Arc<LlmConfig>,
    ctx: ConnectorContext,
    client: OnceCell<RestClient>,
}

impl std::fmt::Debug for LlmCaller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmCaller")
            .field("provider", &self.provider)
            .field("model", &self.cfg.model)
            .finish_non_exhaustive()
    }
}

impl LlmCaller {
    pub fn new(provider: Provider, cfg: Arc<LlmConfig>, ctx: &ConnectorContext) -> Self {
        Self {
            provider,
            cfg,
            ctx: ctx.clone(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&RestClient, ConnectorError> {
        self.client
            .get_or_try_init(|| async {
                let base = self
                    .cfg
                    .base_url
                    .clone()
                    .unwrap_or_else(|| self.provider.default_base().to_owned());
                let auth = match self.provider {
                    Provider::OpenAi => AuthSpec::Bearer(self.cfg.api_key.clone()),
                    Provider::Anthropic => AuthSpec::Header {
                        header: "x-api-key".into(),
                        prefix: String::new(),
                        secret: self.cfg.api_key.clone(),
                    },
                };
                let c = RestClient::connect(
                    &base,
                    &auth,
                    &*self.ctx.secrets,
                    self.ctx.egress.clone(),
                    Duration::from_millis(self.cfg.request_timeout_ms),
                    "",
                )
                .await?;
                match self.provider {
                    Provider::OpenAi => Ok(c),
                    Provider::Anthropic => c.with_headers(&[("anthropic-version", "2023-06-01")]),
                }
            })
            .await
    }

    async fn note_limits(&self, client: &RestClient, resp: &Response<Bytes>) {
        if let Some(d) = header_hold(self.provider, resp.headers(), SystemTime::now()) {
            client.pacer().hold_for(d).await;
        }
        if matches!(resp.status().as_u16(), 429 | 529)
            && let Some(d) = retry_after(resp)
        {
            client.pacer().hold_for(d).await;
        }
    }
}

#[async_trait]
impl Caller for LlmCaller {
    async fn call(&self, ev: &CloudEvent) -> Result<Reply, ConnectorError> {
        let body = request_body(self.provider, &self.cfg, ev)?;
        let client = self.client().await?;
        let resp = client
            .send(
                Method::POST,
                self.provider.path(),
                json_bytes(&body)?,
                Some("application/json"),
                &[],
            )
            .await?;
        self.note_limits(client, &resp).await;
        if let Some(mut err) = classify(&resp) {
            if resp.status().as_u16() == 529 {
                err.class = ErrorClass::Retryable {
                    after: retry_after(&resp),
                };
            }
            err.message = format!("{} {}: {}", self.provider.id(), ev.id, err.message);
            return Err(err);
        }
        let answer = parse_body(&resp)?;
        Ok(Reply::json(
            self.cfg.model.clone(),
            &reply_data(self.provider, &answer)?,
        ))
    }

    async fn secret_values(&self) -> Vec<Vec<u8>> {
        match self.client.get() {
            Some(c) => c.secret_values(),
            None => Vec::new(),
        }
    }
}

/// The connector factory for one provider.
#[derive(Clone, Default)]
pub struct LlmFactory {
    provider: Option<Provider>,
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for LlmFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmFactory")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl LlmFactory {
    pub fn new(provider: Provider) -> Self {
        Self {
            provider: Some(provider),
            replies: None,
        }
    }

    /// The runtime's reply topics.
    pub fn with_replies(mut self, replies: Arc<dyn ReplyTopics>) -> Self {
        self.replies = Some(replies);
        self
    }

    fn provider(&self) -> Provider {
        self.provider.unwrap_or(Provider::OpenAi)
    }
}

struct LlmSinkFactory {
    provider: Provider,
    cfg: Arc<LlmConfig>,
    ctx: ConnectorContext,
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for LlmSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmSinkFactory")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl SinkFactory for LlmSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let caller = LlmCaller::new(self.provider, self.cfg.clone(), &self.ctx);
        Ok(SinkHandle::Plain(Box::new(ReplySink::new(
            self.provider.id(),
            caller,
            &self.ctx,
            self.cfg.reply_topic.as_deref(),
            self.replies.as_ref(),
        )?)))
    }
}

impl ConnectorFactory for LlmFactory {
    fn id(&self) -> &'static str {
        self.provider().id()
    }

    fn source(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        Ok(None)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let provider = self.provider();
        Ok(Some(Box::new(LlmSinkFactory {
            provider,
            cfg: Arc::new(LlmConfig::parse(provider, &ctx.config)?),
            ctx: ctx.clone(),
            replies: self.replies.clone(),
        })))
    }
}

/// `runtime.ref: loams_connector_protocols::openai`.
pub mod openai {
    pub use super::{LlmCaller, LlmConfig, LlmFactory, Provider, reply_data, request_body};

    /// The OpenAI factory.
    pub fn factory() -> LlmFactory {
        LlmFactory::new(Provider::OpenAi)
    }
}

/// `runtime.ref: loams_connector_protocols::anthropic`.
pub mod anthropic {
    pub use super::{LlmCaller, LlmConfig, LlmFactory, Provider, reply_data, request_body};

    /// The Anthropic factory.
    pub fn factory() -> LlmFactory {
        LlmFactory::new(Provider::Anthropic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_formats() {
        assert_eq!(go_duration("1s"), Some(Duration::from_secs(1)));
        assert_eq!(go_duration("6m0s"), Some(Duration::from_secs(360)));
        assert_eq!(go_duration("20ms"), Some(Duration::from_millis(20)));
        assert_eq!(go_duration("2m3.5s"), Some(Duration::from_secs_f64(123.5)));
        assert_eq!(
            go_duration("2h"),
            Some(Duration::from_secs(3600)),
            "capped at an hour"
        );
        assert_eq!(go_duration("x"), None);
        assert_eq!(rfc3339_secs("1970-01-02T00:00:01Z"), Some(86_401));
        assert_eq!(
            rfc3339_secs("2026-10-13T08:00:30.123Z"),
            Some(1_791_878_430)
        );
    }

    #[test]
    fn headers_hold_requests() {
        let mut h = HeaderMap::new();
        h.insert("x-ratelimit-remaining-requests", "0".parse().expect("v"));
        h.insert("x-ratelimit-reset-requests", "2s".parse().expect("v"));
        assert_eq!(
            header_hold(Provider::OpenAi, &h, SystemTime::now()),
            Some(Duration::from_secs(2))
        );
        let mut a = HeaderMap::new();
        a.insert(
            "anthropic-ratelimit-requests-remaining",
            "0".parse().expect("v"),
        );
        a.insert(
            "anthropic-ratelimit-requests-reset",
            "1970-01-01T00:00:30Z".parse().expect("v"),
        );
        let now = UNIX_EPOCH + Duration::from_secs(10);
        assert_eq!(
            header_hold(Provider::Anthropic, &a, now),
            Some(Duration::from_secs(20))
        );
        a.insert(
            "anthropic-ratelimit-requests-remaining",
            "5".parse().expect("v"),
        );
        assert_eq!(header_hold(Provider::Anthropic, &a, now), None);
    }

    #[test]
    fn config_rules() {
        let ok = json!({"model": "m", "api_key": "k", "prompt": {"user": "{{/q}}"}});
        assert!(LlmConfig::parse(Provider::OpenAi, &ok).is_ok());
        assert!(
            LlmConfig::parse(Provider::Anthropic, &ok).is_err(),
            "max_tokens required"
        );
        let reserved = json!({"model": "m", "api_key": "k", "prompt": {"user": "x"},
            "params": {"model": "other"}});
        assert!(LlmConfig::parse(Provider::OpenAi, &reserved).is_err());
        let typo = json!({"model": "m", "api_key": "k", "prompt": {"user": "x"}, "apikey": "v"});
        assert!(LlmConfig::parse(Provider::OpenAi, &typo).is_err());
    }
}
