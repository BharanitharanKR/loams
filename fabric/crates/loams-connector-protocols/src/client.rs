//! The REST client the family's HTTP connectors share (GraphQL, Neo4j,
//! Grafeo's Connect API, Prometheus remote write, Cloud Run, Azure Functions,
//! Lambda, OpenAI, Anthropic): the SDK's `sdk::http` client, so every
//! connection is egress-checked after DNS (ruling R21.1), plus credentials
//! resolved by secret name and sent as sensitive headers, and AWS SigV4.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Method, Request, Response, Uri};
use loams_connector_http::profile::sigv4;
use loams_connector_sdk::http::{HttpClient, error_for_response};
use loams_connector_sdk::{
    ConnectorError, EgressCheck, ErrorClass, Secret, SecretRef, SecretResolver,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// HTTP Basic: the user name, and the **name** of the secret holding the
/// password.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// AWS credentials by secret name, and the signing scope.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AwsAuth {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    pub region: String,
    #[serde(default)]
    pub service: Option<String>,
}

/// How a request is authenticated. Every string names a secret, never a
/// value (R7.5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum AuthSpec {
    #[default]
    None,
    Basic(BasicAuth),
    /// `Authorization: Bearer <secret>`.
    Bearer(String),
    /// `<header>: <prefix><secret>` (OpenAI-style keys, Anthropic's
    /// `x-api-key`, Azure Functions' `x-functions-key`).
    Header {
        header: String,
        prefix: String,
        secret: String,
    },
    Aws(AwsAuth),
}

/// The resolved form of an [`AuthSpec`].
pub(crate) enum Credentials {
    None,
    Header {
        name: HeaderName,
        value: Secret,
    },
    Aws {
        creds: sigv4::Credentials,
        region: String,
        service: String,
    },
}

pub(crate) async fn resolve(
    secrets: &dyn SecretResolver,
    name: &str,
) -> Result<Secret, ConnectorError> {
    secrets.resolve(&SecretRef::new(name)).await
}

pub(crate) fn secret_str(s: &Secret, what: &str) -> Result<String, ConnectorError> {
    s.expose_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::fatal(format!("{what} is not UTF-8")))
}

impl Credentials {
    pub(crate) async fn resolve(
        auth: &AuthSpec,
        secrets: &dyn SecretResolver,
        default_aws_service: &str,
    ) -> Result<Self, ConnectorError> {
        Ok(match auth {
            AuthSpec::None => Credentials::None,
            AuthSpec::Basic(basic) => {
                let password = resolve(secrets, &basic.password).await?;
                let mut raw = basic.username.as_bytes().to_vec();
                raw.push(b':');
                raw.extend_from_slice(password.expose());
                let value = format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(&raw)
                );
                raw.fill(0);
                Credentials::Header {
                    name: AUTHORIZATION,
                    value: Secret::new(value.into_bytes()),
                }
            }
            AuthSpec::Bearer(name) => {
                let token = resolve(secrets, name).await?;
                let mut value = b"Bearer ".to_vec();
                value.extend_from_slice(token.expose());
                Credentials::Header {
                    name: AUTHORIZATION,
                    value: Secret::new(value),
                }
            }
            AuthSpec::Header {
                header,
                prefix,
                secret,
            } => {
                let token = resolve(secrets, secret).await?;
                let mut value = prefix.as_bytes().to_vec();
                value.extend_from_slice(token.expose());
                Credentials::Header {
                    name: HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
                        ConnectorError::fatal(format!("{header:?} is not a header name"))
                    })?,
                    value: Secret::new(value),
                }
            }
            AuthSpec::Aws(aws) => {
                let key_id = resolve(secrets, &aws.access_key_id).await?;
                Credentials::Aws {
                    creds: sigv4::Credentials {
                        access_key_id: secret_str(&key_id, "the AWS access key id")?,
                        secret_access_key: resolve(secrets, &aws.secret_access_key).await?,
                        session_token: match &aws.session_token {
                            Some(n) => Some(resolve(secrets, n).await?),
                            None => None,
                        },
                    },
                    region: aws.region.clone(),
                    service: aws
                        .service
                        .clone()
                        .unwrap_or_else(|| default_aws_service.to_owned()),
                }
            }
        })
    }

    /// The bytes of every credential value, so a reply can be checked for
    /// them (`openai_reply_has_no_key_and_keeps_subject`).
    pub(crate) fn values(&self) -> Vec<Vec<u8>> {
        match self {
            Credentials::None => Vec::new(),
            Credentials::Header { value, .. } => vec![value.expose().to_vec()],
            Credentials::Aws { creds, .. } => {
                let mut v = vec![creds.secret_access_key.expose().to_vec()];
                if let Some(t) = &creds.session_token {
                    v.push(t.expose().to_vec());
                }
                v
            }
        }
    }
}

/// Check and normalise a base URL: `http` or `https`, a host, no user
/// information (credentials go through the secret store), no query.
pub fn base_url(url: &str) -> Result<String, ConnectorError> {
    let uri: Uri = url
        .parse()
        .map_err(|_| ConnectorError::fatal("the URL is not valid"))?;
    match uri.scheme_str() {
        Some("http" | "https") => {}
        _ => return Err(ConnectorError::fatal("the URL must be http:// or https://")),
    }
    let authority = uri
        .authority()
        .ok_or_else(|| ConnectorError::fatal("the URL has no host"))?;
    if authority.as_str().contains('@') {
        return Err(ConnectorError::fatal(
            "the URL must not carry credentials; name a secret instead",
        ));
    }
    if uri.query().is_some() {
        return Err(ConnectorError::fatal("the URL must not carry a query"));
    }
    Ok(url.trim_end_matches('/').to_owned())
}

/// A client for one base URL with its credentials and fixed headers.
#[derive(Clone)]
pub struct RestClient {
    http: HttpClient,
    base: String,
    creds: Arc<Credentials>,
    headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    pacer: Arc<Pacer>,
}

impl std::fmt::Debug for RestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestClient")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl RestClient {
    /// Resolve the credentials `auth` names and bind the client to the
    /// instance's egress check. `default_aws_service` is the SigV4 service
    /// when the config names none.
    pub async fn connect(
        url: &str,
        auth: &AuthSpec,
        secrets: &dyn SecretResolver,
        egress: Arc<dyn EgressCheck>,
        timeout: Duration,
        default_aws_service: &str,
    ) -> Result<Self, ConnectorError> {
        let base = base_url(url)?;
        let creds = Credentials::resolve(auth, secrets, default_aws_service).await?;
        Ok(Self {
            http: loams_connector_sdk::http::client(egress).with_timeout(timeout),
            base,
            creds: Arc::new(creds),
            headers: Arc::new(Vec::new()),
            pacer: Arc::new(Pacer::default()),
        })
    }

    /// Fixed, non-secret headers sent with every request (an API version).
    pub fn with_headers(mut self, headers: &[(&str, &str)]) -> Result<Self, ConnectorError> {
        let mut v = Vec::with_capacity(headers.len());
        for (k, val) in headers {
            v.push((
                HeaderName::from_bytes(k.as_bytes())
                    .map_err(|_| ConnectorError::fatal(format!("{k:?} is not a header name")))?,
                HeaderValue::from_str(val)
                    .map_err(|_| ConnectorError::fatal(format!("bad value for header {k}")))?,
            ));
        }
        self.headers = Arc::new(v);
        Ok(self)
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn pacer(&self) -> &Pacer {
        &self.pacer
    }

    /// The credential values this client sends (for reply checks).
    pub fn secret_values(&self) -> Vec<Vec<u8>> {
        self.creds.values()
    }

    /// Send `method target` (a path and query below the base URL, or `""`
    /// for the base itself) and return the response whatever its status.
    /// Waits out any hold the provider's rate-limit headers set first.
    pub async fn send(
        &self,
        method: Method,
        target: &str,
        body: Bytes,
        content_type: Option<&str>,
        extra: &[(&str, String)],
    ) -> Result<Response<Bytes>, ConnectorError> {
        self.pacer.wait().await;
        let url = format!("{}{target}", self.base);
        let uri: Uri = url
            .parse()
            .map_err(|_| ConnectorError::fatal(format!("bad request path {target}")))?;
        let mut req = Request::builder().method(method.clone()).uri(uri.clone());
        if let Some(ct) = content_type {
            req = req.header(CONTENT_TYPE, ct);
        }
        for (k, v) in self.headers.iter() {
            req = req.header(k.clone(), v.clone());
        }
        for (k, v) in extra {
            req = req.header(*k, v.as_str());
        }
        match &*self.creds {
            Credentials::None => {}
            Credentials::Header { name, value } => {
                let mut v = HeaderValue::from_bytes(value.expose())
                    .map_err(|_| ConnectorError::fatal("a credential is not a valid header"))?;
                v.set_sensitive(true);
                req = req.header(name.clone(), v);
            }
            Credentials::Aws {
                creds,
                region,
                service,
            } => {
                let date = sigv4::amz_date(SystemTime::now());
                let payload_hash = hex(&Sha256::digest(&body));
                let mut signed = vec![
                    ("x-amz-date".to_owned(), date.clone()),
                    ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
                ];
                if let Some(ct) = content_type {
                    signed.push(("content-type".to_owned(), ct.to_owned()));
                }
                for (k, v) in extra {
                    if k.to_ascii_lowercase().starts_with("x-amz-") {
                        signed.push((k.to_ascii_lowercase(), v.clone()));
                    }
                }
                let token = match &creds.session_token {
                    Some(t) => Some(secret_str(t, "the AWS session token")?),
                    None => None,
                };
                if let Some(t) = &token {
                    signed.push(("x-amz-security-token".to_owned(), t.clone()));
                }
                let sig = sigv4::sign(
                    method.as_str(),
                    &uri,
                    &signed,
                    &body,
                    creds,
                    &sigv4::SigningParams {
                        region,
                        service,
                        amz_date: &date,
                    },
                );
                let mut auth = HeaderValue::from_str(&sig.authorization)
                    .map_err(|_| ConnectorError::fatal("SigV4 produced a bad header"))?;
                auth.set_sensitive(true);
                req = req
                    .header(AUTHORIZATION, auth)
                    .header("x-amz-date", date)
                    .header("x-amz-content-sha256", payload_hash);
                if let Some(t) = token {
                    let mut v = HeaderValue::from_str(&t)
                        .map_err(|_| ConnectorError::fatal("bad AWS session token"))?;
                    v.set_sensitive(true);
                    req = req.header("x-amz-security-token", v);
                }
            }
        }
        let req = req
            .body(body)
            .map_err(|e| ConnectorError::fatal(format!("building a request: {e}")))?;
        self.http.send(req).await
    }

    /// Send a JSON request and parse a JSON answer; a non-success status is
    /// an error of its class ([`classify`]).
    pub async fn json(
        &self,
        method: Method,
        target: &str,
        body: Option<&Value>,
    ) -> Result<Value, ConnectorError> {
        let (bytes, ct) = match body {
            Some(v) => (json_bytes(v)?, Some("application/json")),
            None => (Bytes::new(), None),
        };
        let resp = self.send(method.clone(), target, bytes, ct, &[]).await?;
        if let Some(err) = classify(&resp) {
            return Err(with_context(
                err,
                &format!("{method} {}", path_only(target)),
            ));
        }
        parse_body(&resp)
    }
}

pub(crate) fn json_bytes(v: &Value) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(v)
        .map(Bytes::from)
        .map_err(|e| ConnectorError::fatal(e.to_string()))
}

/// The JSON body of a response (`null` when empty).
pub fn parse_body(resp: &Response<Bytes>) -> Result<Value, ConnectorError> {
    if resp.body().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(resp.body())
        .map_err(|e| ConnectorError::retryable(format!("the answer is not JSON: {e}")))
}

/// The error a response stands for, with a short reason from its body, or
/// `None` for a success.
pub fn classify(resp: &Response<Bytes>) -> Option<ConnectorError> {
    let mut err = error_for_response(resp)?;
    if let Some(detail) = error_detail(resp.body()) {
        err.message = format!("{}: {detail}", err.message);
    }
    Some(err)
}

/// A short reason from an error body: `error.type: error.message` (OpenAI,
/// Anthropic), `errors[0].code: message` (Neo4j), `error` / `message`.
pub fn error_detail(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let e = &v["error"];
    let text = if e.is_object() {
        let ty = e["type"].as_str().or(e["code"].as_str()).unwrap_or("error");
        match e["message"].as_str() {
            Some(m) => format!("{ty}: {m}"),
            None => ty.to_owned(),
        }
    } else if let Some(s) = e.as_str() {
        s.to_owned()
    } else if let Some(first) = v["errors"].get(0) {
        match (first["code"].as_str(), first["message"].as_str()) {
            (Some(c), Some(m)) => format!("{c}: {m}"),
            (None, Some(m)) => m.to_owned(),
            (Some(c), None) => c.to_owned(),
            (None, None) => return None,
        }
    } else {
        v["message"].as_str()?.to_owned()
    };
    Some(shorten(&text, 300))
}

pub fn shorten(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

pub(crate) fn path_only(target: &str) -> &str {
    target.split('?').next().unwrap_or(target)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn with_context(mut e: ConnectorError, ctx: &str) -> ConnectorError {
    e.message = format!("{ctx}: {}", e.message);
    e
}

/// A 4xx answer to a read is the request's fault (a bad query, a missing
/// database): `Fatal`, never a poison record.
pub fn read_error(e: ConnectorError) -> ConnectorError {
    if e.class == ErrorClass::Poison {
        ConnectorError::new(ErrorClass::Fatal, e.message)
    } else {
        e
    }
}

/// Holds every request of an instance until a provider's rate-limit window
/// reopens (ruling R21.9): set from `Retry-After`, or from a
/// `*-remaining: 0` header with its `*-reset`.
#[derive(Debug, Default)]
pub struct Pacer {
    until: Mutex<Option<Instant>>,
}

impl Pacer {
    /// Hold every request for `d` from now (the longer hold wins).
    pub async fn hold_for(&self, d: Duration) {
        let t = Instant::now() + d;
        let mut g = self.until.lock().await;
        if g.is_none_or(|u| u < t) {
            *g = Some(t);
        }
    }

    /// How long until the hold ends (zero when none).
    pub async fn remaining(&self) -> Duration {
        let g = self.until.lock().await;
        g.map_or(Duration::ZERO, |u| {
            u.saturating_duration_since(Instant::now())
        })
    }

    pub async fn wait(&self) {
        let until = *self.until.lock().await;
        if let Some(t) = until {
            tokio::time::sleep_until(t).await;
        }
    }
}

/// A `Retry-After` value in seconds (fractions allowed).
pub fn retry_after(resp: &Response<Bytes>) -> Option<Duration> {
    let v = resp
        .headers()
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    let secs: f64 = v.trim().parse().ok()?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs.min(3600.0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_urls() {
        assert_eq!(base_url("http://h:7474/").expect("ok"), "http://h:7474");
        let e = base_url("http://user:pw-value@h").expect_err("userinfo");
        assert!(!e.message.contains("pw-value"));
        assert!(base_url("ftp://h").is_err());
        assert!(base_url("http://h/?a=1").is_err());
    }

    #[test]
    fn error_details() {
        assert_eq!(
            error_detail(br#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#)
                .as_deref(),
            Some("rate_limit_error: slow down")
        );
        assert_eq!(
            error_detail(
                br#"{"errors":[{"code":"Neo.ClientError.Statement.SyntaxError","message":"bad"}]}"#
            )
            .as_deref(),
            Some("Neo.ClientError.Statement.SyntaxError: bad")
        );
        assert_eq!(error_detail(b"nope"), None);
        assert_eq!(shorten("ééé", 3), "é…");
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_holds_and_releases() {
        let p = Pacer::default();
        assert_eq!(p.remaining().await, Duration::ZERO);
        p.hold_for(Duration::from_secs(5)).await;
        p.hold_for(Duration::from_secs(1)).await;
        assert_eq!(p.remaining().await, Duration::from_secs(5));
        let t = Instant::now();
        p.wait().await;
        assert_eq!(t.elapsed(), Duration::from_secs(5));
    }
}
