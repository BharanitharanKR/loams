//! Functions (`lambda-cloud-run-azure-functions`): AWS Lambda, Google Cloud
//! Run and Azure Functions as request-reply sinks (`sink.reply`, OR-19).
//!
//! * **Lambda** — `Invoke` over Lambda's REST API
//!   (`POST /2015-03-31/functions/{name}/invocations`,
//!   `X-Amz-Invocation-Type: RequestResponse`), signed with SigV4 by the HTTP
//!   family's signer (ruling R21.8: no `aws-sdk-lambda`, which would dial
//!   outside the SDK's egress check and build a second TLS stack).
//!   `endpoint` points it at floci in CI. A function error
//!   (`X-Amz-Function-Error`) is `Poison` (the event goes to the DLQ) or,
//!   with `function_error: reply`, a reply `{function_error, payload}`.
//! * **Cloud Run** — `POST url` with `Authorization: Bearer <ID token>`.
//! * **Azure Functions** — `POST url` with `x-functions-key`.
//!
//! The request body is the event's data as is, or the `body` template filled
//! from the event ([`crate::template`]). The answer body is the reply's data.
//! A 404 (no such function) is `Fatal`; 429 is `Throttled` with
//! `Retry-After`; 5xx is `Retryable`; another 4xx is `Poison`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use loams_connector_sdk::{
    CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory, ErrorClass, SinkFactory,
    SinkHandle, SourceFactory, SplitEnumerator,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use crate::client::{AuthSpec, AwsAuth, RestClient, classify, shorten};
use crate::reply::{Caller, Reply, ReplySink, ReplyTopics};
use crate::template;

/// Which platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Lambda,
    CloudRun,
    AzureFunctions,
}

/// What a function error becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnFunctionError {
    #[default]
    Poison,
    Reply,
}

/// The instance config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FunctionsConfig {
    pub provider: Platform,
    /// Lambda: the function name or ARN.
    #[serde(default)]
    pub function: Option<String>,
    /// Lambda: a version or alias.
    #[serde(default)]
    pub qualifier: Option<String>,
    /// Lambda: the API endpoint; default `https://lambda.<region>.amazonaws.com`.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Lambda: credentials by secret name, and the region.
    #[serde(default)]
    pub aws: Option<AwsAuth>,
    /// Cloud Run and Azure Functions: the function's URL.
    #[serde(default)]
    pub url: Option<String>,
    /// Cloud Run: the **name** of the secret holding an ID token whose
    /// audience is the service (minted and rotated by the secret store).
    #[serde(default)]
    pub id_token: Option<String>,
    /// Azure Functions: the **name** of the secret holding the function key.
    #[serde(default)]
    pub function_key: Option<String>,
    /// A body template; default the event's data as is.
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default = "default_ct")]
    pub content_type: String,
    #[serde(default)]
    pub function_error: OnFunctionError,
    #[serde(default)]
    pub reply_topic: Option<String>,
    #[serde(default = "default_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub pipeline: Option<Value>,
}

fn default_ct() -> String {
    "application/json".into()
}

fn default_timeout() -> u64 {
    // Lambda's own maximum run time is 900 s.
    900_000
}

impl FunctionsConfig {
    pub fn parse(v: &Value) -> Result<Self, ConnectorError> {
        let cfg: FunctionsConfig = serde_json::from_value(v.clone())
            .map_err(|e| ConnectorError::fatal(format!("functions config: {e}")))?;
        match cfg.provider {
            Platform::Lambda => {
                if cfg.function.as_deref().is_none_or(str::is_empty) {
                    return Err(ConnectorError::fatal("lambda needs `function`"));
                }
                if cfg.aws.is_none() {
                    return Err(ConnectorError::fatal(
                        "lambda needs `aws` (credentials by secret name and the region)",
                    ));
                }
            }
            Platform::CloudRun => {
                if cfg.url.is_none() || cfg.id_token.is_none() {
                    return Err(ConnectorError::fatal(
                        "cloud_run needs `url` and `id_token`",
                    ));
                }
            }
            Platform::AzureFunctions => {
                if cfg.url.is_none() || cfg.function_key.is_none() {
                    return Err(ConnectorError::fatal(
                        "azure_functions needs `url` and `function_key`",
                    ));
                }
            }
        }
        if let Some(b) = &cfg.body {
            template::check(b)?;
        }
        Ok(cfg)
    }

    /// The base URL, the request target below it, and the auth.
    fn target(&self) -> Result<(String, String, AuthSpec), ConnectorError> {
        Ok(match self.provider {
            Platform::Lambda => {
                let aws = self
                    .aws
                    .clone()
                    .ok_or_else(|| ConnectorError::fatal("no aws"))?;
                let base = self
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| format!("https://lambda.{}.amazonaws.com", aws.region));
                let name = self.function.clone().unwrap_or_default();
                let mut target = format!(
                    "/2015-03-31/functions/{}/invocations",
                    loams_connector_http::profile::sigv4::uri_encode(&name, true)
                );
                if let Some(q) = &self.qualifier {
                    target.push_str("?Qualifier=");
                    target.push_str(&loams_connector_http::profile::sigv4::uri_encode(q, true));
                }
                (base, target, AuthSpec::Aws(aws))
            }
            Platform::CloudRun => (
                self.url.clone().unwrap_or_default(),
                String::new(),
                AuthSpec::Bearer(self.id_token.clone().unwrap_or_default()),
            ),
            Platform::AzureFunctions => (
                self.url.clone().unwrap_or_default(),
                String::new(),
                AuthSpec::Header {
                    header: "x-functions-key".into(),
                    prefix: String::new(),
                    secret: self.function_key.clone().unwrap_or_default(),
                },
            ),
        })
    }

    /// The reply's resource: the function name, or the URL's host and path.
    fn resource(&self) -> String {
        match self.provider {
            Platform::Lambda => self.function.clone().unwrap_or_default(),
            _ => self
                .url
                .as_deref()
                .and_then(|u| u.split_once("://").map(|(_, r)| r.to_owned()))
                .unwrap_or_default(),
        }
    }
}

/// One invocation per event.
pub struct FunctionCaller {
    cfg: Arc<FunctionsConfig>,
    ctx: ConnectorContext,
    client: OnceCell<(RestClient, String)>,
}

impl std::fmt::Debug for FunctionCaller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionCaller")
            .field("provider", &self.cfg.provider)
            .field("resource", &self.cfg.resource())
            .finish_non_exhaustive()
    }
}

impl FunctionCaller {
    pub fn new(cfg: Arc<FunctionsConfig>, ctx: &ConnectorContext) -> Self {
        Self {
            cfg,
            ctx: ctx.clone(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&(RestClient, String), ConnectorError> {
        self.client
            .get_or_try_init(|| async {
                let (base, target, auth) = self.cfg.target()?;
                let c = RestClient::connect(
                    &base,
                    &auth,
                    &*self.ctx.secrets,
                    self.ctx.egress.clone(),
                    Duration::from_millis(self.cfg.request_timeout_ms),
                    "lambda",
                )
                .await?;
                Ok((c, target))
            })
            .await
    }

    fn body(&self, ev: &CloudEvent) -> Result<Bytes, ConnectorError> {
        match &self.cfg.body {
            Some(t) => {
                let data = template::data_json(ev)?;
                Ok(Bytes::from(template::render(t, ev, &data)?))
            }
            None => Ok(ev.data.clone()),
        }
    }
}

#[async_trait]
impl Caller for FunctionCaller {
    async fn call(&self, ev: &CloudEvent) -> Result<Reply, ConnectorError> {
        let body = self.body(ev)?;
        let (client, target) = self.client().await?;
        let extra: Vec<(&str, String)> = match self.cfg.provider {
            Platform::Lambda => vec![("x-amz-invocation-type", "RequestResponse".into())],
            _ => Vec::new(),
        };
        let resp = client
            .send(
                Method::POST,
                target,
                body,
                Some(&self.cfg.content_type),
                &extra,
            )
            .await?;
        if let Some(mut err) = classify(&resp) {
            if resp.status().as_u16() == 404 {
                err.class = ErrorClass::Fatal;
            }
            err.message = format!("invoking {}: {}", self.cfg.resource(), err.message);
            return Err(err);
        }
        let ct = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json")
            .to_owned();
        let function_error = resp
            .headers()
            .get("x-amz-function-error")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let resource = self.cfg.resource();
        if let Some(kind) = function_error {
            return match self.cfg.function_error {
                OnFunctionError::Poison => Err(ConnectorError::poison(format!(
                    "{resource} failed ({kind}) on event {}: {}",
                    ev.id,
                    shorten(&String::from_utf8_lossy(resp.body()), 300)
                ))),
                OnFunctionError::Reply => {
                    let payload =
                        serde_json::from_slice::<Value>(resp.body()).unwrap_or_else(|_| {
                            Value::String(String::from_utf8_lossy(resp.body()).into_owned())
                        });
                    Ok(Reply::json(
                        resource,
                        &json!({"function_error": kind, "payload": payload}),
                    ))
                }
            };
        }
        Ok(Reply {
            resource,
            content_type: ct,
            data: resp.body().clone(),
        })
    }

    async fn secret_values(&self) -> Vec<Vec<u8>> {
        match self.client.get() {
            Some((c, _)) => c.secret_values(),
            None => Vec::new(),
        }
    }
}

/// `runtime.ref: loams_connector_protocols::functions`.
#[derive(Clone, Default)]
pub struct Functions {
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for Functions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Functions")
    }
}

impl Functions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_replies(mut self, replies: Arc<dyn ReplyTopics>) -> Self {
        self.replies = Some(replies);
        self
    }
}

struct FunctionSinkFactory {
    cfg: Arc<FunctionsConfig>,
    ctx: ConnectorContext,
    replies: Option<Arc<dyn ReplyTopics>>,
}

impl std::fmt::Debug for FunctionSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionSinkFactory")
            .field("provider", &self.cfg.provider)
            .finish_non_exhaustive()
    }
}

impl SinkFactory for FunctionSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Plain(Box::new(ReplySink::new(
            "lambda-cloud-run-azure-functions",
            FunctionCaller::new(self.cfg.clone(), &self.ctx),
            &self.ctx,
            self.cfg.reply_topic.as_deref(),
            self.replies.as_ref(),
        )?)))
    }
}

impl ConnectorFactory for Functions {
    fn id(&self) -> &'static str {
        "lambda-cloud-run-azure-functions"
    }

    /// A function calling back into Loams is a webhook delivery (CN1 Task
    /// 4's webhooks source), not this connector's source (ruling R21.8).
    fn source(
        &self,
        _ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        Ok(None)
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        Ok(Some(Box::new(FunctionSinkFactory {
            cfg: Arc::new(FunctionsConfig::parse(&ctx.config)?),
            ctx: ctx.clone(),
            replies: self.replies.clone(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configs_need_their_platform_fields() {
        let lambda = json!({"provider": "lambda", "function": "arn:aws:lambda:eu-west-1:000000000000:function:echo",
            "qualifier": "live",
            "aws": {"access_key_id": "k", "secret_access_key": "s", "region": "eu-west-1"}});
        let cfg = FunctionsConfig::parse(&lambda).expect("lambda");
        let (base, target, _) = cfg.target().expect("target");
        assert_eq!(base, "https://lambda.eu-west-1.amazonaws.com");
        assert_eq!(
            target,
            "/2015-03-31/functions/arn%3Aaws%3Alambda%3Aeu-west-1%3A000000000000%3Afunction%3Aecho/invocations?Qualifier=live"
        );
        assert!(FunctionsConfig::parse(&json!({"provider": "lambda", "function": "f"})).is_err());
        assert!(
            FunctionsConfig::parse(&json!({"provider": "cloud_run", "url": "https://x"})).is_err()
        );
        let az = json!({"provider": "azure_functions", "url": "https://app.example.test/api/fn",
            "function_key": "fn-key"});
        let cfg = FunctionsConfig::parse(&az).expect("azure");
        assert_eq!(cfg.resource(), "app.example.test/api/fn");
        assert!(FunctionsConfig::parse(&json!({"provider": "gcf"})).is_err());
    }
}
