//! Google Cloud credentials for the Pub/Sub client: a service account's key
//! (the JSON key file, held as a secret named at `auth.credentials_json`)
//! exchanged for an OAuth 2.0 access token with the JWT bearer grant
//! (RFC 7523; RS256 with the account's key), cached until a minute before
//! it expires. Without `auth`, requests carry no credentials, which is what
//! the Pub/Sub emulator expects.

use std::fmt;
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::ConnectorError;
use loams_connector_sdk::http::HttpClient;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::aws::sigv4::uri_encode;
use crate::ce::now_secs;
use crate::config::LazySecret;
use crate::crypto;

const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const B64URL: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Where a request's bearer token comes from.
pub struct TokenSource {
    key: Option<LazySecret>,
    scope: String,
    http: HttpClient,
    cached: Mutex<Option<(String, Instant)>>,
}

impl fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSource")
            .field("key", &self.key)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl TokenSource {
    pub fn new(key: Option<LazySecret>, scope: &str, http: HttpClient) -> Self {
        Self {
            key,
            scope: scope.to_owned(),
            http,
            cached: Mutex::new(None),
        }
    }

    /// The bearer token to send, or `None` without credentials.
    pub async fn bearer(&self) -> Result<Option<String>, ConnectorError> {
        let Some(key) = &self.key else {
            return Ok(None);
        };
        let mut cached = self.cached.lock().await;
        if let Some((t, until)) = cached.as_ref()
            && Instant::now() < *until
        {
            return Ok(Some(t.clone()));
        }
        let (token, ttl) = self.fetch(key).await?;
        *cached = Some((
            token.clone(),
            Instant::now() + ttl.saturating_sub(Duration::from_secs(60)),
        ));
        Ok(Some(token))
    }

    /// Drop the cached token (after a 401).
    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
    }

    async fn fetch(&self, key: &LazySecret) -> Result<(String, Duration), ConnectorError> {
        let secret = key.text().await?;
        let sa: Value = serde_json::from_str(secret.expose()).map_err(|_| {
            ConnectorError::auth(format!(
                "secret `{}` is not a service account key file",
                key.name()
            ))
        })?;
        let field = |k: &str| {
            sa.get(k).and_then(Value::as_str).ok_or_else(|| {
                ConnectorError::auth(format!("the service account key has no `{k}`"))
            })
        };
        let email = field("client_email")?;
        let pem = field("private_key")?;
        let token_uri = sa
            .get("token_uri")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_TOKEN_URI);
        let der = crypto::pem_block(pem, "PRIVATE KEY").ok_or_else(|| {
            ConnectorError::auth("the service account's private_key is not PEM PKCS #8")
        })?;
        let now = now_secs();
        let mut header = json!({"alg": "RS256", "typ": "JWT"});
        if let Some(kid) = sa.get("private_key_id").and_then(Value::as_str) {
            header["kid"] = json!(kid);
        }
        let claims = json!({"iss": email, "scope": self.scope, "aud": token_uri,
                            "iat": now, "exp": now + 3600});
        let signing_input = format!(
            "{}.{}",
            B64URL.encode(header.to_string()),
            B64URL.encode(claims.to_string())
        );
        let sig =
            crypto::rs256_sign(&der, signing_input.as_bytes()).map_err(ConnectorError::auth)?;
        let jwt = format!("{signing_input}.{}", B64URL.encode(sig));
        let body = format!(
            "grant_type={}&assertion={}",
            uri_encode("urn:ietf:params:oauth:grant-type:jwt-bearer", true),
            uri_encode(&jwt, true)
        );
        let req = http::Request::post(token_uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Bytes::from(body))
            .map_err(|e| ConnectorError::fatal(format!("bad token_uri: {e}")))?;
        token_response(self.http.send(req).await?)
    }
}

/// An OAuth 2.0 token response: the access token and its lifetime. Its
/// error is `Auth` on 400 and 401 (the grant was refused), else by status.
pub fn token_response(resp: http::Response<Bytes>) -> Result<(String, Duration), ConnectorError> {
    let status = resp.status();
    if !status.is_success() {
        let class = match status.as_u16() {
            400 | 401 | 403 => loams_connector_sdk::ErrorClass::Auth,
            _ => loams_connector_sdk::http::classify_status(status, None)
                .unwrap_or(loams_connector_sdk::ErrorClass::Fatal),
        };
        return Err(ConnectorError::new(
            class,
            format!("token request answered {status}"),
        ));
    }
    let v: Value = serde_json::from_slice(resp.body())
        .map_err(|_| ConnectorError::retryable("unreadable token response"))?;
    let token = v["access_token"]
        .as_str()
        .ok_or_else(|| ConnectorError::auth("token response without access_token"))?
        .to_owned();
    let ttl = v["expires_in"].as_u64().unwrap_or(3600);
    Ok((token, Duration::from_secs(ttl)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_responses() {
        let ok = http::Response::new(Bytes::from_static(
            br#"{"access_token":"t","expires_in":10}"#,
        ));
        let (t, ttl) = token_response(ok).expect("a token");
        assert_eq!((t.as_str(), ttl), ("t", Duration::from_secs(10)));
        let mut refused = http::Response::new(Bytes::new());
        *refused.status_mut() = http::StatusCode::BAD_REQUEST;
        assert_eq!(
            token_response(refused).expect_err("refused").class,
            loams_connector_sdk::ErrorClass::Auth
        );
    }
}
