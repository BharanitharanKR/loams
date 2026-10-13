//! The `auth` block: how each request authenticates. Credentials are never in
//! the profile; the instance config names a secret per field (`auth.token`,
//! `auth.client_secret`, …) and the instance's `SecretResolver` resolves it
//! when the first request needs it.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Request, StatusCode};
use loams_connector_sdk::http::HttpClient;
use loams_connector_sdk::{
    AuthScheme, ConnectorError, HttpAuth, Secret, SecretRef, SecretResolver,
};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::sigv4;

/// A token is refreshed this long before the expiry its endpoint gave.
const EXPIRY_SKEW: Duration = Duration::from_secs(30);

pub(crate) struct Authenticator {
    spec: HttpAuth,
    /// The instance config's `auth` object: field → secret name (and
    /// `region`, which is not a secret).
    refs: serde_json::Map<String, Value>,
    secrets: Arc<dyn SecretResolver>,
    client: HttpClient,
    token: Mutex<Option<(Secret, Instant)>>,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("scheme", &self.spec.scheme)
            .finish_non_exhaustive()
    }
}

fn header_value(value: &[u8]) -> Result<HeaderValue, ConnectorError> {
    let mut v = HeaderValue::from_bytes(value)
        .map_err(|_| ConnectorError::auth("a credential is not a valid header value"))?;
    v.set_sensitive(true);
    Ok(v)
}

impl Authenticator {
    pub(crate) fn new(
        spec: HttpAuth,
        config: &Value,
        secrets: Arc<dyn SecretResolver>,
        client: HttpClient,
    ) -> Self {
        let refs = config
            .get("auth")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        Self {
            spec,
            refs,
            secrets,
            client,
            token: Mutex::new(None),
        }
    }

    pub(crate) fn scheme(&self) -> AuthScheme {
        self.spec.scheme
    }

    /// The secret the instance config names under `auth.<field>`.
    async fn secret(&self, field: &str) -> Result<Secret, ConnectorError> {
        let name = self.optional_ref(field).ok_or_else(|| {
            ConnectorError::auth(format!(
                "auth.{field} names no secret in the instance config ({:?} auth)",
                self.spec.scheme
            ))
        })?;
        self.secrets.resolve(&SecretRef::new(name)).await
    }

    fn optional_ref(&self, field: &str) -> Option<&str> {
        self.refs
            .get(field)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    }

    async fn optional_secret(&self, field: &str) -> Result<Option<Secret>, ConnectorError> {
        match self.optional_ref(field) {
            Some(name) => self.secrets.resolve(&SecretRef::new(name)).await.map(Some),
            None => Ok(None),
        }
    }

    /// Forget a cached OAuth token (its API refused it).
    pub(crate) async fn invalidate(&self) {
        *self.token.lock().await = None;
    }

    /// Add the credential to `req` (for `aws-sigv4`, the signature over the
    /// request as it now is: call this last).
    pub(crate) async fn apply(&self, req: &mut Request<Bytes>) -> Result<(), ConnectorError> {
        match self.spec.scheme {
            AuthScheme::Bearer => {
                let token = self.secret("token").await?;
                let header = self.header_name(Some(AUTHORIZATION))?;
                let prefix = self.spec.prefix.as_deref().unwrap_or("Bearer ");
                self.set(req, header, prefix, token.expose())
            }
            AuthScheme::Header => {
                let key = self.secret("api_key").await?;
                let header = self.header_name(None)?;
                let prefix = self.spec.prefix.as_deref().unwrap_or("");
                self.set(req, header, prefix, key.expose())
            }
            AuthScheme::Basic => {
                let user = self.secret("username").await?;
                let pass = self.secret("password").await?;
                let mut raw = zeroizing(user.expose());
                raw.push(b':');
                raw.extend_from_slice(pass.expose());
                let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
                raw.fill(0);
                let header = self.header_name(Some(AUTHORIZATION))?;
                self.set(req, header, "Basic ", encoded.as_bytes())
            }
            AuthScheme::Oauth2 => {
                let token = self.oauth_token().await?;
                let header = self.header_name(Some(AUTHORIZATION))?;
                let prefix = self.spec.prefix.as_deref().unwrap_or("Bearer ");
                self.set(req, header, prefix, token.expose())
            }
            AuthScheme::AwsSigv4 => self.sign_v4(req).await,
        }
    }

    fn header_name(&self, default: Option<HeaderName>) -> Result<HeaderName, ConnectorError> {
        match (&self.spec.header, default) {
            (Some(h), _) => HeaderName::from_bytes(h.as_bytes())
                .map_err(|_| ConnectorError::fatal(format!("auth.header {h:?} is not a header"))),
            (None, Some(d)) => Ok(d),
            (None, None) => Err(ConnectorError::fatal("the profile's auth names no header")),
        }
    }

    fn set(
        &self,
        req: &mut Request<Bytes>,
        header: HeaderName,
        prefix: &str,
        credential: &[u8],
    ) -> Result<(), ConnectorError> {
        let mut value = zeroizing(prefix.as_bytes());
        value.extend_from_slice(credential);
        let hv = header_value(&value);
        value.fill(0);
        req.headers_mut().insert(header, hv?);
        Ok(())
    }

    /// The cached access token, or a new one from `token_url`: the
    /// refresh-token grant when the instance names a refresh token, client
    /// credentials otherwise.
    async fn oauth_token(&self) -> Result<Secret, ConnectorError> {
        let mut cached = self.token.lock().await;
        if let Some((token, until)) = cached.as_ref()
            && Instant::now() < *until
        {
            return Ok(token.clone());
        }
        let url = self
            .spec
            .token_url
            .as_deref()
            .ok_or_else(|| ConnectorError::fatal("an oauth2 profile needs auth.token_url"))?;
        let client_id = self.secret("client_id").await?;
        let client_secret = self.secret("client_secret").await?;
        let refresh = self.optional_secret("refresh_token").await?;
        let mut form: Vec<(&str, Vec<u8>)> = Vec::new();
        match &refresh {
            Some(r) => {
                form.push(("grant_type", b"refresh_token".to_vec()));
                form.push(("refresh_token", r.expose().to_vec()));
            }
            None => form.push(("grant_type", b"client_credentials".to_vec())),
        }
        form.push(("client_id", client_id.expose().to_vec()));
        form.push(("client_secret", client_secret.expose().to_vec()));
        if !self.spec.scopes.is_empty() {
            form.push(("scope", self.spec.scopes.join(" ").into_bytes()));
        }
        let mut body = Vec::new();
        for (i, (k, v)) in form.iter_mut().enumerate() {
            if i > 0 {
                body.push(b'&');
            }
            body.extend_from_slice(k.as_bytes());
            body.push(b'=');
            body.extend_from_slice(sigv4::uri_encode(&String::from_utf8_lossy(v), true).as_bytes());
            v.fill(0);
        }
        let req = Request::post(url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(http::header::ACCEPT, "application/json")
            .body(Bytes::from(body))
            .map_err(|e| ConnectorError::fatal(format!("token request: {e}")))?;
        let resp = self.client.send(req).await?;
        match resp.status() {
            s if s.is_success() => {}
            StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(ConnectorError::auth(format!(
                    "the token endpoint refused the credentials (HTTP {})",
                    resp.status()
                )));
            }
            s => {
                return Err(
                    loams_connector_sdk::http::error_for_response(&resp).unwrap_or_else(|| {
                        ConnectorError::retryable(format!("token endpoint: HTTP {s}"))
                    }),
                );
            }
        }
        let doc: Value = serde_json::from_slice(resp.body())
            .map_err(|_| ConnectorError::auth("the token endpoint's answer is not JSON"))?;
        let token = doc
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::auth("the token endpoint gave no access_token"))?;
        let lifetime = doc
            .get("expires_in")
            .and_then(Value::as_u64)
            .map_or(Duration::from_secs(3600), Duration::from_secs);
        let token = Secret::new(token.as_bytes());
        *cached = Some((
            token.clone(),
            Instant::now() + lifetime.saturating_sub(EXPIRY_SKEW),
        ));
        Ok(token)
    }

    async fn sign_v4(&self, req: &mut Request<Bytes>) -> Result<(), ConnectorError> {
        let access_key_id = self.secret("access_key_id").await?;
        let creds = sigv4::Credentials {
            access_key_id: access_key_id
                .expose_str()
                .ok_or_else(|| ConnectorError::auth("auth.access_key_id is not UTF-8"))?
                .to_string(),
            secret_access_key: self.secret("secret_access_key").await?,
            session_token: self.optional_secret("session_token").await?,
        };
        let region = self
            .optional_ref("region")
            .or(self.spec.region.as_deref())
            .ok_or_else(|| {
                ConnectorError::fatal("aws-sigv4 needs a region (auth.region or the profile's)")
            })?
            .to_string();
        let service =
            self.spec.service.clone().ok_or_else(|| {
                ConnectorError::fatal("aws-sigv4 needs the profile's auth.service")
            })?;
        let date = sigv4::amz_date(SystemTime::now());
        let headers = req.headers_mut();
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&date).map_err(|e| ConnectorError::fatal(e.to_string()))?,
        );
        if let Some(token) = &creds.session_token {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                header_value(token.expose())?,
            );
        }
        let mut list = Vec::new();
        for (k, v) in req.headers() {
            let k = k.as_str();
            if k == "content-type" || k.starts_with("x-amz-") || k == "host" {
                let v = std::str::from_utf8(v.as_bytes())
                    .map_err(|_| ConnectorError::fatal(format!("header {k} is not UTF-8")))?;
                list.push((k.to_string(), v.to_string()));
            }
        }
        let sig = sigv4::sign(
            req.method().as_str(),
            req.uri(),
            &list,
            req.body(),
            &creds,
            &sigv4::SigningParams {
                region: &region,
                service: &service,
                amz_date: &date,
            },
        );
        req.headers_mut()
            .insert(AUTHORIZATION, header_value(sig.authorization.as_bytes())?);
        Ok(())
    }
}

/// A scratch buffer the caller zeroes after use.
fn zeroizing(start: &[u8]) -> Vec<u8> {
    start.to_vec()
}
