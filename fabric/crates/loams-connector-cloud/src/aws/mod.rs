//! A minimal AWS client over the documented wire protocols (CN2 R19.1):
//! AWS JSON 1.0 (SQS) and 1.1 (EventBridge, Kinesis) with `X-Amz-Target`,
//! and the Query protocol (SNS), each signed with SigV4 and sent through
//! `sdk::http::client`, so every connection passes the instance's egress
//! check after DNS.
//!
//! Instance config: `region`, optional `endpoint` (an emulator such as floci,
//! or a VPC endpoint), and `auth.access_key_id`, `auth.secret_access_key`,
//! `auth.session_token` naming secrets. Without `endpoint`, `auth` is
//! required; with one and no `auth`, requests are signed with a placeholder
//! key, which emulators accept (R19.3).

pub mod sigv4;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http::{Method, Request};
use loams_connector_sdk::http::HttpClient;
use loams_connector_sdk::{
    ConnectorContext, ConnectorError, EgressCheck, ErrorClass, Secret, SecretResolver,
};
use serde_json::Value;

use crate::config::{LazySecret, str_opt};
use sigv4::{Credentials, SigningParams};

/// The AWS services this family speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsService {
    Sqs,
    Sns,
    EventBridge,
    Kinesis,
}

impl AwsService {
    /// The endpoint prefix and SigV4 service name.
    pub fn signing_name(self) -> &'static str {
        match self {
            AwsService::Sqs => "sqs",
            AwsService::Sns => "sns",
            AwsService::EventBridge => "events",
            AwsService::Kinesis => "kinesis",
        }
    }

    /// `X-Amz-Target`'s prefix and the JSON protocol version, for the JSON
    /// services; `None` for SNS, which speaks the Query protocol.
    fn json(self) -> Option<(&'static str, &'static str)> {
        match self {
            AwsService::Sqs => Some(("AmazonSQS", "1.0")),
            AwsService::EventBridge => Some(("AWSEvents", "1.1")),
            AwsService::Kinesis => Some(("Kinesis_20131202", "1.1")),
            AwsService::Sns => None,
        }
    }
}

/// The SNS Query API version.
const SNS_VERSION: &str = "2010-03-31";

#[derive(Clone)]
enum CredSource {
    Secrets {
        key_id: LazySecret,
        secret: LazySecret,
        token: Option<LazySecret>,
    },
    /// An emulator endpoint without `auth`: a placeholder key (R19.3).
    Placeholder,
}

/// A signed client for one AWS service, bound to an instance's egress check.
#[derive(Clone)]
pub struct AwsClient {
    service: AwsService,
    region: String,
    url: String,
    creds: CredSource,
    http: HttpClient,
}

impl fmt::Debug for AwsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsClient")
            .field("service", &self.service)
            .field("region", &self.region)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl AwsClient {
    /// The client an instance's config describes.
    pub fn from_context(
        ctx: &ConnectorContext,
        service: AwsService,
    ) -> Result<Self, ConnectorError> {
        Self::new(&ctx.config, service, &ctx.secrets, ctx.egress.clone())
    }

    pub fn new(
        config: &Value,
        service: AwsService,
        secrets: &Arc<dyn SecretResolver>,
        egress: Arc<dyn EgressCheck>,
    ) -> Result<Self, ConnectorError> {
        let endpoint = str_opt(config, "endpoint");
        let region = match (str_opt(config, "region"), &endpoint) {
            (Some(r), _) => r,
            (None, Some(_)) => "us-east-1".to_owned(),
            (None, None) => return Err(ConnectorError::fatal("config `region` is required")),
        };
        if !region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(ConnectorError::fatal(
                "config `region` is not an AWS region",
            ));
        }
        let url = match endpoint {
            Some(e) => format!("{}/", e.trim_end_matches('/')),
            None => {
                let suffix = if region.starts_with("cn-") {
                    "amazonaws.com.cn"
                } else {
                    "amazonaws.com"
                };
                format!("https://{}.{region}.{suffix}/", service.signing_name())
            }
        };
        let creds = match (
            LazySecret::from_auth(config, "access_key_id", secrets),
            LazySecret::from_auth(config, "secret_access_key", secrets),
        ) {
            (Some(key_id), Some(secret)) => CredSource::Secrets {
                key_id,
                secret,
                token: LazySecret::from_auth(config, "session_token", secrets),
            },
            (None, None) if str_opt(config, "endpoint").is_some() => CredSource::Placeholder,
            _ => {
                return Err(ConnectorError::fatal(
                    "config `auth.access_key_id` and `auth.secret_access_key` (secret names) are required",
                ));
            }
        };
        Ok(Self {
            service,
            region,
            url,
            creds,
            http: loams_connector_sdk::http::client(egress).with_timeout(Duration::from_secs(40)),
        })
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    async fn credentials(&self) -> Result<Credentials, ConnectorError> {
        match &self.creds {
            CredSource::Placeholder => Ok(Credentials {
                access_key_id: "loams".to_owned(),
                secret_access_key: Secret::new("loams-placeholder"),
                session_token: None,
            }),
            CredSource::Secrets {
                key_id,
                secret,
                token,
            } => {
                let id = key_id.text().await?;
                let token = match token {
                    Some(t) => Some(t.get().await?),
                    None => None,
                };
                Ok(Credentials {
                    access_key_id: id.expose().to_owned(),
                    secret_access_key: secret.get().await?,
                    session_token: token,
                })
            }
        }
    }

    async fn send(
        &self,
        content_type: &str,
        extra: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Result<Bytes, ConnectorError> {
        let creds = self.credentials().await?;
        let uri: http::Uri = self
            .url
            .parse()
            .map_err(|e| ConnectorError::fatal(format!("bad endpoint: {e}")))?;
        let date = sigv4::amz_date(SystemTime::now());
        let mut headers: Vec<(String, String)> = vec![
            ("content-type".into(), content_type.into()),
            ("x-amz-date".into(), date.clone()),
        ];
        for (k, v) in extra {
            headers.push(((*k).to_owned(), (*v).to_owned()));
        }
        if let Some(t) = &creds.session_token {
            headers.push((
                "x-amz-security-token".into(),
                t.expose_str().unwrap_or_default().to_owned(),
            ));
        }
        let sig = sigv4::sign(
            "POST",
            &uri,
            &headers,
            &body,
            &creds,
            &SigningParams {
                region: &self.region,
                service: self.service.signing_name(),
                amz_date: &date,
            },
        );
        let mut req = Request::builder().method(Method::POST).uri(uri);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let mut req = req
            .header("authorization", sig.authorization)
            .body(Bytes::from(body))
            .map_err(|e| ConnectorError::fatal(format!("building the request: {e}")))?;
        for v in req.headers_mut().values_mut() {
            v.set_sensitive(true);
        }
        let resp = self.http.send(req).await?;
        if resp.status().is_success() {
            return Ok(resp.into_body());
        }
        let status = resp.status().as_u16();
        let query_error = resp
            .headers()
            .get("x-amzn-query-error")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        Err(error_from_response(status, query_error.as_deref(), resp.body()).into())
    }

    /// A JSON-protocol operation (`ReceiveMessage`, `PutEvents`,
    /// `GetRecords`…) with `body` as its input; the output JSON.
    pub async fn call(&self, op: &str, body: &Value) -> Result<Value, ConnectorError> {
        let Some((prefix, version)) = self.service.json() else {
            return Err(ConnectorError::fatal(format!(
                "{} speaks the Query protocol",
                self.service.signing_name()
            )));
        };
        let target = format!("{prefix}.{op}");
        let ct = format!("application/x-amz-json-{version}");
        let raw = self
            .send(
                &ct,
                &[("x-amz-target", &target)],
                serde_json::to_vec(body).unwrap_or_default(),
            )
            .await?;
        if raw.is_empty() {
            return Ok(Value::Object(Default::default()));
        }
        serde_json::from_slice(&raw)
            .map_err(|e| ConnectorError::retryable(format!("{op}: unreadable response: {e}")))
    }

    /// A Query-protocol action (SNS): `params` form-encoded with `Version`;
    /// the XML response.
    pub async fn query(&self, params: &[(&str, &str)]) -> Result<String, ConnectorError> {
        let owned: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        self.query_owned(owned).await
    }

    pub async fn query_owned(
        &self,
        mut params: Vec<(String, String)>,
    ) -> Result<String, ConnectorError> {
        if !params.iter().any(|(k, _)| k == "Version") {
            params.push(("Version".into(), SNS_VERSION.into()));
        }
        let body = params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    sigv4::uri_encode(k, true),
                    sigv4::uri_encode(v, true)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        let raw = self
            .send(
                "application/x-www-form-urlencoded; charset=utf-8",
                &[],
                body.into_bytes(),
            )
            .await?;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }
}

/// An error an AWS service answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsError {
    pub status: u16,
    /// The error code without its namespace (`QueueDoesNotExist`).
    pub code: String,
    pub message: String,
}

impl AwsError {
    pub fn class(&self) -> ErrorClass {
        classify(self.status, &self.code)
    }
}

impl From<AwsError> for ConnectorError {
    fn from(e: AwsError) -> Self {
        let msg = if e.message.is_empty() {
            format!("AWS {} ({})", e.code, e.status)
        } else {
            format!("AWS {} ({}): {}", e.code, e.status, e.message)
        };
        ConnectorError::new(e.class(), msg)
    }
}

/// The error in a failed response: the JSON protocols' `__type` (or the
/// `x-amzn-query-error` header SQS adds), or the Query protocol's
/// `<Error><Code>`.
pub fn error_from_response(status: u16, query_error: Option<&str>, body: &[u8]) -> AwsError {
    let mut code = String::new();
    let mut message;
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        if let Some(t) = v.get("__type").and_then(Value::as_str) {
            code = t.to_owned();
        }
        message = v
            .get("message")
            .or_else(|| v.get("Message"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
    } else {
        let text = String::from_utf8_lossy(body);
        if let Some(c) = xml_text(&text, "Code") {
            code = c;
        }
        message = xml_text(&text, "Message").unwrap_or_default();
    }
    if let Some(q) = query_error {
        // `AWS.SimpleQueueService.NonExistentQueue;Sender`
        code = q.split(';').next().unwrap_or(q).to_owned();
    }
    // `com.amazonaws.sqs#QueueDoesNotExist` or `Code:http://…`.
    let code = code
        .rsplit('#')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_owned();
    let code = if code.is_empty() {
        format!("HTTP{status}")
    } else {
        code
    };
    message.truncate(512);
    AwsError {
        status,
        code,
        message,
    }
}

/// An AWS error code's class (CN2 Task 4's retries by class).
pub fn classify(status: u16, code: &str) -> ErrorClass {
    let short = code.rsplit('.').next().unwrap_or(code);
    match short {
        "Throttling"
        | "ThrottlingException"
        | "ThrottledException"
        | "RequestThrottled"
        | "RequestThrottledException"
        | "TooManyRequestsException"
        | "ProvisionedThroughputExceededException"
        | "LimitExceededException"
        | "KMSThrottlingException"
        | "RequestLimitExceeded"
        | "SlowDown" => ErrorClass::Throttled {
            after: Duration::from_secs(1),
        },
        "UnrecognizedClientException"
        | "InvalidClientTokenId"
        | "InvalidSignatureException"
        | "SignatureDoesNotMatch"
        | "AccessDenied"
        | "AccessDeniedException"
        | "AuthorizationError"
        | "ExpiredToken"
        | "ExpiredTokenException"
        | "MissingAuthenticationToken"
        | "IncompleteSignature"
        | "KMSAccessDeniedException"
        | "KMSDisabledException"
        | "KMSInvalidStateException" => ErrorClass::Auth,
        "ResourceNotFoundException"
        | "QueueDoesNotExist"
        | "NonExistentQueue"
        | "NotFound"
        | "NotFoundException"
        | "InvalidAction"
        | "UnsupportedOperation" => ErrorClass::Fatal,
        "InvalidParameterValue"
        | "InvalidParameterException"
        | "InvalidParameter"
        | "ValidationException"
        | "ValidationError"
        | "InvalidMessageContents"
        | "InvalidAttributeValue"
        | "InvalidAttributeName"
        | "MessageTooLong"
        | "BatchRequestTooLong"
        | "KMSInvalidKeyUsageException" => ErrorClass::Poison,
        "InternalFailure"
        | "InternalError"
        | "InternalErrorException"
        | "ServiceUnavailable"
        | "ServiceUnavailableException"
        | "KMSInternalException" => ErrorClass::Retryable { after: None },
        _ if status >= 500 || status == 408 => ErrorClass::Retryable { after: None },
        _ if status == 429 => ErrorClass::Throttled {
            after: Duration::from_secs(1),
        },
        _ if status == 401 || status == 403 => ErrorClass::Auth,
        _ => ErrorClass::Fatal,
    }
}

/// The text of the first `<tag>…</tag>` in `doc`, entities decoded.
pub fn xml_text(doc: &str, tag: &str) -> Option<String> {
    xml_blocks(doc, tag)
        .into_iter()
        .next()
        .map(|s| xml_unescape(&s))
}

/// The raw contents of every `<tag>…</tag>` in `doc`, in order (no nesting
/// of the same tag; the SNS responses this reads have none).
pub fn xml_blocks(doc: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = doc;
    while let Some(start) = rest.find(&open) {
        let body = &rest[start + open.len()..];
        let Some(end) = body.find(&close) else {
            break;
        };
        out.push(body[..end].to_owned());
        rest = &body[end + close.len()..];
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#xD;", "\r")
        .replace("&#xA;", "\n")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_parse_and_classify() {
        let e = error_from_response(
            400,
            None,
            br#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"no queue"}"#,
        );
        assert_eq!(e.code, "QueueDoesNotExist");
        assert_eq!(e.class(), ErrorClass::Fatal);
        let e = error_from_response(
            400,
            Some("AWS.SimpleQueueService.NonExistentQueue;Sender"),
            b"{}",
        );
        assert_eq!(e.class(), ErrorClass::Fatal);
        let e = error_from_response(
            400,
            None,
            b"<ErrorResponse><Error><Type>Sender</Type><Code>Throttling</Code><Message>Rate exceeded</Message></Error></ErrorResponse>",
        );
        assert_eq!(e.code, "Throttling");
        assert!(matches!(e.class(), ErrorClass::Throttled { .. }));
        assert_eq!(
            classify(400, "ProvisionedThroughputExceededException"),
            ErrorClass::Throttled {
                after: Duration::from_secs(1)
            }
        );
        assert_eq!(classify(403, "InvalidSignatureException"), ErrorClass::Auth);
        assert_eq!(classify(400, "InvalidParameterValue"), ErrorClass::Poison);
        assert_eq!(
            classify(503, "Whatever"),
            ErrorClass::Retryable { after: None }
        );
        assert_eq!(classify(400, "Whatever"), ErrorClass::Fatal);
        let c: ConnectorError = error_from_response(500, None, b"").into();
        assert!(c.message.contains("HTTP500"));
    }

    #[test]
    fn xml_helpers() {
        let doc = "<a><Id>1</Id><Id>2 &amp; 3</Id></a>";
        assert_eq!(xml_blocks(doc, "Id"), vec!["1", "2 &amp; 3"]);
        assert_eq!(xml_text(doc, "Id").as_deref(), Some("1"));
        assert_eq!(xml_blocks(doc, "Nope"), Vec::<String>::new());
        assert_eq!(xml_unescape("2 &amp; 3 &lt;x&gt;"), "2 & 3 <x>");
    }

    #[test]
    fn endpoints_and_auth() {
        let r: Arc<dyn SecretResolver> = Arc::new(loams_connector_sdk::StaticSecrets::new());
        let egress: Arc<dyn EgressCheck> = Arc::new(loams_connector_sdk::AllowAll);
        let c = AwsClient::new(
            &serde_json::json!({"region": "eu-west-1", "auth": {"access_key_id": "a", "secret_access_key": "b"}}),
            AwsService::EventBridge,
            &r,
            egress.clone(),
        )
        .expect("client");
        assert_eq!(c.url, "https://events.eu-west-1.amazonaws.com/");
        let c = AwsClient::new(
            &serde_json::json!({"region": "cn-north-1", "auth": {"access_key_id": "a", "secret_access_key": "b"}}),
            AwsService::Sqs,
            &r,
            egress.clone(),
        )
        .expect("client");
        assert_eq!(c.url, "https://sqs.cn-north-1.amazonaws.com.cn/");
        let c = AwsClient::new(
            &serde_json::json!({"endpoint": "http://floci:4566/"}),
            AwsService::Kinesis,
            &r,
            egress.clone(),
        )
        .expect("an emulator needs no auth");
        assert_eq!(
            (c.url.as_str(), c.region()),
            ("http://floci:4566/", "us-east-1")
        );
        assert!(
            AwsClient::new(
                &serde_json::json!({"region": "eu-west-1"}),
                AwsService::Sqs,
                &r,
                egress.clone()
            )
            .is_err()
        );
        assert!(
            AwsClient::new(
                &serde_json::json!({"region": "eu-west-1/../x", "endpoint": "http://x"}),
                AwsService::Sqs,
                &r,
                egress
            )
            .is_err()
        );
    }
}
