//! What the DynamoDB and DynamoDB Streams clients share: the AWS SDK's
//! `HttpClient` implemented over `sdk::http::client` (so every request is
//! egress-checked after DNS and TLS is the SDK's rustls with ring — ruling
//! R14.3), static credentials resolved from the secrets the config names,
//! and the mapping of AWS errors to SDK error classes.

use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::{ConnectorError as AwsConnectorError, SdkError};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use loams_connector_sdk::{ConnectorContext, ConnectorError, EgressCheck};

use crate::config::{self, Config};

/// The AWS settings of an instance: `region`, optional `endpoint_url` (an
/// emulator such as floci, or a VPC endpoint), and the secret names
/// `access_key_id`, `secret_access_key` and optional `session_token`.
#[derive(Debug, Clone)]
pub struct AwsSettings {
    pub region: String,
    pub endpoint_url: Option<String>,
}

impl AwsSettings {
    pub fn from_config(cfg: &Config) -> Result<Self, ConnectorError> {
        Ok(Self {
            region: cfg.str("region")?.unwrap_or_else(|| "us-east-1".to_owned()),
            endpoint_url: cfg.str("endpoint_url")?,
        })
    }
}

/// Resolve the instance's credentials from the secret names in its config.
pub async fn credentials(
    ctx: &ConnectorContext,
    cfg: &Config,
) -> Result<Credentials, ConnectorError> {
    let ak = config::secret(ctx, cfg, "access_key_id")
        .await?
        .ok_or_else(|| {
            ConnectorError::auth("config: `access_key_id` (a secret name) is required")
        })?;
    let sk = config::secret(ctx, cfg, "secret_access_key")
        .await?
        .ok_or_else(|| {
            ConnectorError::auth("config: `secret_access_key` (a secret name) is required")
        })?;
    let token = match config::secret(ctx, cfg, "session_token").await? {
        Some(t) => Some(config::secret_text(&t, "session_token")?),
        None => None,
    };
    Ok(Credentials::new(
        config::secret_text(&ak, "access_key_id")?,
        config::secret_text(&sk, "secret_access_key")?,
        token,
        None,
        "loams-connector-kv",
    ))
}

/// The AWS SDK's HTTP client, over `sdk::http::client`.
#[derive(Clone)]
pub struct LoamsHttpClient {
    egress: Arc<dyn EgressCheck>,
}

impl std::fmt::Debug for LoamsHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LoamsHttpClient")
    }
}

impl LoamsHttpClient {
    pub fn new(egress: Arc<dyn EgressCheck>) -> Self {
        Self { egress }
    }
}

impl HttpClient for LoamsHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let timeout = settings
            .read_timeout()
            .map(|t| t + settings.connect_timeout().unwrap_or(Duration::ZERO))
            .unwrap_or(Duration::from_secs(30));
        SharedHttpConnector::new(LoamsConnector {
            client: loams_connector_sdk::http::client(self.egress.clone()).with_timeout(timeout),
        })
    }
}

#[derive(Debug, Clone)]
struct LoamsConnector {
    client: loams_connector_sdk::http::HttpClient,
}

impl HttpConnector for LoamsConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        HttpConnectorFuture::new(async move {
            let req = to_http(&request).map_err(|e| AwsConnectorError::user(e.into()))?;
            let resp = client
                .send(req)
                .await
                .map_err(|e| AwsConnectorError::io(e.message.into()))?;
            from_http(resp).map_err(|e| AwsConnectorError::other(e.into(), None))
        })
    }
}

fn to_http(request: &HttpRequest) -> Result<http::Request<Bytes>, String> {
    let body = request
        .body()
        .bytes()
        .ok_or("a streaming request body is not supported")?
        .to_vec();
    let mut b = http::Request::builder()
        .method(request.method())
        .uri(request.uri());
    for (k, v) in request.headers().iter() {
        b = b.header(k, v);
    }
    b.body(Bytes::from(body)).map_err(|e| e.to_string())
}

fn from_http(resp: http::Response<Bytes>) -> Result<HttpResponse, String> {
    let (parts, body) = resp.into_parts();
    let status = StatusCode::try_from(parts.status.as_u16()).map_err(|e| e.to_string())?;
    let mut out = HttpResponse::new(status, SdkBody::from(body));
    for (k, v) in &parts.headers {
        if let Ok(v) = v.to_str() {
            out.headers_mut()
                .append(k.as_str().to_owned(), v.to_owned());
        }
    }
    Ok(out)
}

/// An AWS error code (the service error's `code`) as an error class.
pub fn class_for_code(code: &str, status: Option<u16>, what: &str) -> ConnectorError {
    let msg = format!("{what}: {code}");
    match code {
        "ProvisionedThroughputExceededException"
        | "ThrottlingException"
        | "RequestLimitExceeded"
        | "LimitExceededException" => ConnectorError::throttled(Duration::from_secs(1), msg),
        "UnrecognizedClientException"
        | "InvalidSignatureException"
        | "AccessDeniedException"
        | "ExpiredTokenException"
        | "MissingAuthenticationToken"
        | "IncompleteSignature" => ConnectorError::auth(msg),
        "TrimmedDataAccessException" => ConnectorError::position_lost(format!(
            "{msg}: the stream records at the checkpoint are past the 24-hour retention; re-snapshot the table"
        )),
        "InternalServerError" | "ServiceUnavailable" | "InternalFailure" => {
            ConnectorError::retryable(msg)
        }
        _ if status.is_some_and(|s| s >= 500) => ConnectorError::retryable(msg),
        _ => ConnectorError::fatal(msg),
    }
}

/// An AWS SDK error as an SDK error class, with the service's error code but
/// never the request.
pub fn classify<E, R>(e: &SdkError<E, R>, what: &str) -> ConnectorError
where
    E: ProvideErrorMetadata,
    R: StatusOf,
{
    match e {
        SdkError::ServiceError(se) => {
            let code = se.err().code().unwrap_or("UnknownError");
            class_for_code(code, se.raw().status_u16(), what)
        }
        SdkError::TimeoutError(_) => ConnectorError::retryable(format!("{what}: timed out")),
        SdkError::DispatchFailure(_) => {
            ConnectorError::retryable(format!("{what}: the request could not be sent"))
        }
        SdkError::ResponseError(_) => {
            ConnectorError::retryable(format!("{what}: the response could not be read"))
        }
        SdkError::ConstructionFailure(_) => {
            ConnectorError::fatal(format!("{what}: the request could not be built"))
        }
        _ => ConnectorError::fatal(format!("{what}: failed")),
    }
}

/// The error code of a service error, if `e` is one.
pub fn code_of<E: ProvideErrorMetadata, R>(e: &SdkError<E, R>) -> Option<&str> {
    match e {
        SdkError::ServiceError(se) => se.err().code(),
        _ => None,
    }
}

/// The HTTP status of a raw response.
pub trait StatusOf {
    fn status_u16(&self) -> Option<u16>;
}

impl StatusOf for HttpResponse {
    fn status_u16(&self) -> Option<u16> {
        Some(self.status().as_u16())
    }
}

#[cfg(test)]
mod tests {
    use loams_connector_sdk::ErrorClass;

    use super::*;

    #[test]
    fn aws_codes_map_to_classes() {
        let c = |code: &str| class_for_code(code, Some(400), "op").class;
        assert!(matches!(
            c("ProvisionedThroughputExceededException"),
            ErrorClass::Throttled { .. }
        ));
        assert_eq!(c("UnrecognizedClientException"), ErrorClass::Auth);
        assert_eq!(c("TrimmedDataAccessException"), ErrorClass::PositionLost);
        assert_eq!(c("ResourceNotFoundException"), ErrorClass::Fatal);
        assert!(matches!(
            class_for_code("Weird", Some(503), "op").class,
            ErrorClass::Retryable { .. }
        ));
    }

    #[test]
    fn requests_and_responses_convert() {
        let mut req = HttpRequest::new(SdkBody::from("{}"));
        req.set_uri("https://dynamodb.us-east-1.amazonaws.com/")
            .expect("uri");
        req.headers_mut()
            .insert("x-amz-target", "DynamoDB_20120810.Scan");
        let h = to_http(&req).expect("convert");
        assert_eq!(h.uri().host(), Some("dynamodb.us-east-1.amazonaws.com"));
        assert_eq!(h.headers()["x-amz-target"], "DynamoDB_20120810.Scan");
        assert_eq!(&h.body()[..], b"{}");
        let resp = http::Response::builder()
            .status(400)
            .header("x-amzn-requestid", "r1")
            .body(Bytes::from_static(b"{}"))
            .expect("resp");
        let r = from_http(resp).expect("convert");
        assert_eq!(r.status().as_u16(), 400);
        assert_eq!(r.headers().get("x-amzn-requestid"), Some("r1"));
    }
}
