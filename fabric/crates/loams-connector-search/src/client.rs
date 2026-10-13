//! The REST client the family speaks Elasticsearch, OpenSearch and Qdrant
//! with: the SDK's `sdk::http` client (every connection egress-checked after
//! DNS, rustls with ring; ruling R15.1), plus credentials resolved by name
//! and set as sensitive headers, and AWS SigV4 for Amazon OpenSearch
//! Service.

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
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{AwsAuth, BasicAuth};

/// How requests are authenticated.
enum Credentials {
    None,
    /// A fixed header (`Authorization: Basic …`, `ApiKey …`, Qdrant's
    /// `api-key`), sent sensitive.
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

/// What a connector config says about auth (each field a secret's name).
#[derive(Debug, Clone, Default)]
pub struct AuthSpec {
    pub basic: Option<BasicAuth>,
    /// Sent as `Authorization: ApiKey <value>`.
    pub es_api_key: Option<String>,
    /// Sent as `api-key: <value>` (Qdrant).
    pub qdrant_api_key: Option<String>,
    pub aws: Option<AwsAuth>,
}

/// A connected client for one base URL.
#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base: String,
    creds: Arc<Credentials>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

async fn resolve(secrets: &dyn SecretResolver, name: &str) -> Result<Secret, ConnectorError> {
    secrets.resolve(&SecretRef::new(name)).await
}

fn secret_str(s: &Secret, what: &str) -> Result<String, ConnectorError> {
    s.expose_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::fatal(format!("{what} is not UTF-8")))
}

/// Check and normalise a base URL: `http` or `https`, a host, no user
/// information (credentials go through the secret store), no query.
pub fn base_url(url: &str) -> Result<String, ConnectorError> {
    let uri: Uri = url
        .parse()
        .map_err(|_| ConnectorError::fatal("url is not a valid URL"))?;
    match uri.scheme_str() {
        Some("http" | "https") => {}
        _ => return Err(ConnectorError::fatal("url must be http:// or https://")),
    }
    let authority = uri
        .authority()
        .ok_or_else(|| ConnectorError::fatal("url has no host"))?;
    if authority.as_str().contains('@') {
        return Err(ConnectorError::fatal(
            "url must not carry credentials; name a secret instead",
        ));
    }
    if uri.query().is_some() {
        return Err(ConnectorError::fatal("url must not carry a query"));
    }
    Ok(url.trim_end_matches('/').to_owned())
}

impl Client {
    /// Resolve the credentials `auth` names and bind the client to the
    /// instance's egress check.
    pub async fn connect(
        url: &str,
        auth: &AuthSpec,
        secrets: &dyn SecretResolver,
        egress: Arc<dyn EgressCheck>,
        timeout: Duration,
    ) -> Result<Self, ConnectorError> {
        let base = base_url(url)?;
        let creds = if let Some(aws) = &auth.aws {
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
                service: aws.service.clone(),
            }
        } else if let Some(basic) = &auth.basic {
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
        } else if let Some(name) = &auth.es_api_key {
            let key = resolve(secrets, name).await?;
            let mut value = b"ApiKey ".to_vec();
            value.extend_from_slice(key.expose());
            Credentials::Header {
                name: AUTHORIZATION,
                value: Secret::new(value),
            }
        } else if let Some(name) = &auth.qdrant_api_key {
            Credentials::Header {
                name: HeaderName::from_static("api-key"),
                value: resolve(secrets, name).await?,
            }
        } else {
            Credentials::None
        };
        Ok(Self {
            http: loams_connector_sdk::http::client(egress).with_timeout(timeout),
            base,
            creds: Arc::new(creds),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// Send `method target` (a path and query below the base URL) and return
    /// the response whatever its status.
    pub async fn send(
        &self,
        method: Method,
        target: &str,
        body: Bytes,
        content_type: Option<&str>,
    ) -> Result<Response<Bytes>, ConnectorError> {
        let url = format!("{}{target}", self.base);
        let uri: Uri = url
            .parse()
            .map_err(|_| ConnectorError::fatal(format!("bad request path {target}")))?;
        let mut req = Request::builder().method(method.clone()).uri(uri.clone());
        if let Some(ct) = content_type {
            req = req.header(CONTENT_TYPE, ct);
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
            Some(v) => (
                Bytes::from(
                    serde_json::to_vec(v).map_err(|e| ConnectorError::fatal(e.to_string()))?,
                ),
                Some("application/json"),
            ),
            None => (Bytes::new(), None),
        };
        let resp = self.send(method.clone(), target, bytes, ct).await?;
        if let Some(err) = classify(&resp) {
            return Err(err.with_context(&format!("{method} {}", path_only(target))));
        }
        parse_body(&resp)
    }
}

/// The JSON body of a response.
pub fn parse_body(resp: &Response<Bytes>) -> Result<Value, ConnectorError> {
    if resp.body().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(resp.body())
        .map_err(|e| ConnectorError::retryable(format!("the answer is not JSON: {e}")))
}

/// The error a response stands for, with the server's error type and a
/// short reason (Elasticsearch's `error.type`/`reason`, Qdrant's
/// `status.error`), or `None` for a success.
pub fn classify(resp: &Response<Bytes>) -> Option<ConnectorError> {
    let mut err = error_for_response(resp)?;
    if let Some(detail) = error_detail(resp.body()) {
        err.message = format!("{}: {detail}", err.message);
    }
    Some(err)
}

/// `type: reason`, shortened, from an error body.
pub fn error_detail(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let e = &v["error"];
    let text = if let Some(ty) = e["type"].as_str() {
        match e["reason"].as_str() {
            Some(r) => format!("{ty}: {r}"),
            None => ty.to_owned(),
        }
    } else if let Some(s) = e.as_str() {
        s.to_owned()
    } else {
        v["status"]["error"].as_str()?.to_owned()
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

fn path_only(target: &str) -> &str {
    target.split('?').next().unwrap_or(target)
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Context for an error message.
trait WithContext {
    fn with_context(self, ctx: &str) -> Self;
}

impl WithContext for ConnectorError {
    fn with_context(mut self, ctx: &str) -> Self {
        self.message = format!("{ctx}: {}", self.message);
        self
    }
}

/// Whether an error is the server refusing a request it will never accept
/// (a 4xx other than 408/425/429): for a read, that is `Fatal` (a bad
/// query or a missing index), not a poison record.
pub fn read_error(e: ConnectorError) -> ConnectorError {
    if e.class == ErrorClass::Poison {
        ConnectorError::new(ErrorClass::Fatal, e.message)
    } else {
        e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_urls() {
        assert_eq!(base_url("http://es:9200/").expect("ok"), "http://es:9200");
        assert_eq!(
            base_url("https://h/prefix").expect("ok"),
            "https://h/prefix"
        );
        let e = base_url("http://user:pw-value@es:9200").expect_err("userinfo");
        assert!(!e.message.contains("pw-value"));
        assert!(base_url("ftp://es").is_err());
        assert!(base_url("http://es/?a=1").is_err());
    }

    #[test]
    fn error_details() {
        assert_eq!(
            error_detail(
                br#"{"error":{"type":"index_not_found_exception","reason":"no such index [x]"}}"#
            )
            .as_deref(),
            Some("index_not_found_exception: no such index [x]")
        );
        assert_eq!(
            error_detail(br#"{"status":{"error":"Not found: Collection `c` doesn't exist!"}}"#)
                .as_deref(),
            Some("Not found: Collection `c` doesn't exist!")
        );
        assert_eq!(error_detail(b"nope"), None);
        assert_eq!(shorten("ééé", 3), "é…");
    }
}
