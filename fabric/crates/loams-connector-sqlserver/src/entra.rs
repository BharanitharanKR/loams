//! Microsoft Entra ID tokens for Azure SQL (client credentials of a service
//! principal, OAuth 2.0 RFC 6749 §4.4). The token request goes through
//! `sdk::http`, so the authority is egress-checked after DNS like every other
//! connection; the client secret is resolved by name and never logged.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Method, Request};
use loams_connector_sdk::http::{classify_status, client};
use loams_connector_sdk::{ConnectorContext, ConnectorError, ErrorClass, SecretRef};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::EntraAuth;

/// The public cloud's authority.
pub const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com";
/// The resource every Azure SQL token is for.
pub const SQL_SCOPE: &str = "https://database.windows.net/.default";
/// A cached token is renewed this long before it expires.
const RENEW_BEFORE: Duration = Duration::from_secs(300);

/// A token and when it stops being usable.
struct Cached {
    token: String,
    until: Instant,
}

/// The instance's tokens, shared by its connections (one fetch per hour,
/// not one per connection).
#[derive(Clone, Default)]
pub struct TokenCache(Arc<Mutex<Option<Cached>>>);

impl std::fmt::Debug for TokenCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenCache(<redacted>)")
    }
}

impl TokenCache {
    /// Forget the token (the server refused it).
    pub async fn clear(&self) {
        *self.0.lock().await = None;
    }

    /// A valid token, fetched when none is cached or it is about to expire.
    pub async fn token(
        &self,
        auth: &EntraAuth,
        ctx: &ConnectorContext,
    ) -> Result<String, ConnectorError> {
        let mut slot = self.0.lock().await;
        if let Some(c) = slot.as_ref()
            && Instant::now() + RENEW_BEFORE < c.until
        {
            return Ok(c.token.clone());
        }
        let (token, lifetime) = fetch(auth, ctx).await?;
        *slot = Some(Cached {
            token: token.clone(),
            until: Instant::now() + lifetime,
        });
        Ok(token)
    }
}

/// `application/x-www-form-urlencoded` (WHATWG URL §5.2).
pub fn form_encode(pairs: &[(&str, &str)]) -> String {
    let enc = |s: &str| {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                    out.push(b as char)
                }
                b' ' => out.push('+'),
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    };
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The token endpoint's URL.
pub fn token_url(auth: &EntraAuth) -> String {
    format!(
        "{}/{}/oauth2/v2.0/token",
        auth.authority.trim_end_matches('/'),
        auth.tenant_id
    )
}

/// The client-credentials request for `scope`.
pub fn token_request(
    auth: &EntraAuth,
    client_secret: &str,
) -> Result<Request<Bytes>, ConnectorError> {
    let body = form_encode(&[
        ("grant_type", "client_credentials"),
        ("client_id", &auth.client_id),
        ("client_secret", client_secret),
        ("scope", SQL_SCOPE),
    ]);
    Request::builder()
        .method(Method::POST)
        .uri(token_url(auth))
        .header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .header(http::header::ACCEPT, "application/json")
        .body(Bytes::from(body))
        .map_err(|e| ConnectorError::fatal(format!("config: entra.authority: {e}")))
}

/// The access token and its lifetime from a token response. An error names
/// the OAuth `error` code only, never the body (which echoes request data).
pub fn parse_token_response(
    status: u16,
    body: &[u8],
) -> Result<(String, Duration), ConnectorError> {
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    if !(200..300).contains(&status) {
        let code = v["error"].as_str().unwrap_or("unknown_error");
        let msg = format!("Entra ID token request failed: HTTP {status}, {code}");
        let class = match classify_status(
            http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::BAD_REQUEST),
            None,
        ) {
            Some(c @ (ErrorClass::Retryable { .. } | ErrorClass::Throttled { .. })) => c,
            _ => ErrorClass::Auth,
        };
        return Err(ConnectorError::new(class, msg));
    }
    let token = v["access_token"]
        .as_str()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| ConnectorError::auth("Entra ID token response has no access_token"))?
        .to_owned();
    let expires = v["expires_in"]
        .as_u64()
        .or_else(|| v["expires_in"].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(3600);
    Ok((token, Duration::from_secs(expires)))
}

/// Fetch a token for the instance.
pub async fn fetch(
    auth: &EntraAuth,
    ctx: &ConnectorContext,
) -> Result<(String, Duration), ConnectorError> {
    let secret = ctx
        .secrets
        .resolve(&SecretRef::new(&auth.client_secret))
        .await?;
    let secret = secret
        .expose_str()
        .ok_or_else(|| ConnectorError::auth("entra.client_secret is not UTF-8"))?
        .to_owned();
    let req = token_request(auth, &secret)?;
    let resp = client(ctx.egress.clone()).send(req).await?;
    parse_token_response(resp.status().as_u16(), resp.body())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> EntraAuth {
        EntraAuth {
            tenant_id: "00000000-0000-0000-0000-00000000000a".into(),
            client_id: "app".into(),
            client_secret: "sp-secret".into(),
            authority: DEFAULT_AUTHORITY.into(),
        }
    }

    #[test]
    fn the_request_is_a_client_credentials_grant_for_azure_sql() {
        let req = token_request(&auth(), "s3cr&t =").expect("request");
        assert_eq!(req.method(), Method::POST);
        assert_eq!(
            req.uri().to_string(),
            "https://login.microsoftonline.com/00000000-0000-0000-0000-00000000000a/oauth2/v2.0/token"
        );
        assert_eq!(
            std::str::from_utf8(req.body()).expect("utf8"),
            "grant_type=client_credentials&client_id=app&client_secret=s3cr%26t+%3D\
             &scope=https%3A%2F%2Fdatabase.windows.net%2F.default"
        );
    }

    #[test]
    fn responses_parse_and_failures_never_echo_the_body() {
        let (t, d) = parse_token_response(
            200,
            br#"{"token_type":"Bearer","expires_in":3599,"access_token":"eyJ0"}"#,
        )
        .expect("token");
        assert_eq!((t.as_str(), d), ("eyJ0", Duration::from_secs(3599)));
        let e = parse_token_response(
            401,
            br#"{"error":"invalid_client","error_description":"secret s3cr3t is wrong"}"#,
        )
        .expect_err("refused");
        assert_eq!(e.class, ErrorClass::Auth);
        assert!(e.message.contains("invalid_client") && !e.message.contains("s3cr3t"));
        let e = parse_token_response(503, b"").expect_err("unavailable");
        assert!(e.class.is_transient(), "{:?}", e.class);
        assert!(parse_token_response(200, b"{}").is_err());
    }

    /// A token is fetched once from the (fake, loopback) authority through
    /// the SDK's HTTP client and then served from the cache.
    #[tokio::test]
    async fn tokens_are_fetched_once_and_cached() {
        use loams_connector_sdk::{InstanceKey, Scrubber, StaticSecrets};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let hits = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let (h, b) = (hits.clone(), bodies.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                h.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 8192];
                let n = s.read(&mut buf).await.unwrap_or(0);
                b.lock()
                    .await
                    .push(String::from_utf8_lossy(&buf[..n]).into_owned());
                let body = r#"{"access_token":"tok-1","expires_in":3600}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
            }
        });
        let mut a = auth();
        a.authority = format!("http://127.0.0.1:{port}");
        let ctx = ConnectorContext::new(
            InstanceKey::new(1, "i"),
            serde_json::json!({}),
            Arc::new(StaticSecrets::new().with("sp-secret", b"s3cr3t-value".to_vec())),
            Scrubber::new(),
        );
        let cache = TokenCache::default();
        assert_eq!(cache.token(&a, &ctx).await.expect("token"), "tok-1");
        assert_eq!(cache.token(&a, &ctx).await.expect("cached"), "tok-1");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let req = bodies.lock().await[0].clone();
        assert!(req.starts_with("POST /00000000-0000-0000-0000-00000000000a/oauth2/v2.0/token"));
        assert!(req.contains("client_secret=s3cr3t-value"));
        assert!(!format!("{cache:?}").contains("tok-1"));
    }
}
