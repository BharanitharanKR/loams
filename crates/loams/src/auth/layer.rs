//! The authentication layer: one axum middleware for every HTTP surface
//! (the main port's Connect, gRPC, gRPC-Web and REST, the Qdrant and
//! Elasticsearch gateways, and Flight SQL through tonic's axum routes).
//!
//! A refusal is written in the protocol the caller speaks: gRPC and gRPC-Web
//! get a trailers-only answer (`grpc-status` 16, 7 or 14), Connect gets its
//! JSON error with HTTP 401/403/503, anything else a JSON body with the same
//! status and `WWW-Authenticate: Bearer`. Every refusal carries the reason in
//! `loams-auth-reason`.
//!
//! Browsers on the console's origins (`LOAMS_CORS_ORIGINS`; any origin when
//! it is empty and authentication is on, since credentials are bearer
//! headers and never cookies) get CORS: the preflight is answered here (it
//! carries no credential), and every answer to an allowed origin, refusals
//! included, carries `Access-Control-Allow-Origin`.
//!
//! Tailscale identity headers are stripped on every listener but the
//! `tailscale serve` target ([`AuthLayerState::tailscale_target`]).

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use base64::Engine as _;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};

use super::{Auth, AuthError, AuthErrorKind, Credential, api_key, reason, tailscale};

/// The response header naming why a request was refused.
pub const REASON_HEADER: &str = "loams-auth-reason";

/// Query parameters that would carry a credential (refused: URLs are logged).
const QUERY_CREDENTIALS: &[&str] = &["access_token", "token", "api_key", "apikey", "api-key"];

/// The main port's unauthenticated paths (API2 `PUBLIC_RPCS`, plus the
/// probes).
pub const MAIN_PUBLIC_PATHS: &[&str] = &[
    "/health",
    "/ready",
    "/grpc.health.v1.Health/Check",
    "/grpc.health.v1.Health/Watch",
    "/loams.instance.v1.InstanceService/GetInstance",
];

const ALLOW_METHODS: &str = "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS";
const ALLOW_HEADERS: &str = "authorization, content-type, accept, connect-protocol-version, \
     connect-timeout-ms, connect-content-encoding, connect-accept-encoding, grpc-timeout, \
     x-grpc-web, x-user-agent, loams-consistency-token, loams-hot, idempotency-key";
const EXPOSE_HEADERS: &str = "grpc-status, grpc-message, grpc-status-details-bin, \
     loams-consistency-token, loams-request-id, loams-auth-reason, www-authenticate, \
     retry-after";

/// The allowed browser origins.
#[derive(Clone, Debug, Default)]
pub struct Cors {
    origins: Arc<BTreeSet<String>>,
    any: bool,
}

impl Cors {
    pub fn new(origins: BTreeSet<String>) -> Self {
        Self {
            origins: Arc::new(origins),
            any: false,
        }
    }

    /// Every origin (an authenticated engine with no `LOAMS_CORS_ORIGINS`:
    /// a self-hosted console may live on any domain).
    pub fn any() -> Self {
        Self {
            origins: Arc::default(),
            any: true,
        }
    }

    fn allows(&self, origin: &str) -> bool {
        self.any || self.origins.contains(origin.trim_end_matches('/'))
    }
}

/// The middleware's state: the authenticators and the paths it lets through.
#[derive(Clone, Debug)]
pub struct AuthLayerState {
    auth: Arc<Auth>,
    public_paths: &'static [&'static str],
    /// The `tailscale serve` target: Tailscale identity headers count.
    trust_tailscale: bool,
}

impl AuthLayerState {
    pub fn new(auth: Arc<Auth>, public_paths: &'static [&'static str]) -> Self {
        Self {
            auth,
            public_paths,
            trust_tailscale: false,
        }
    }

    /// The layer of the `LOAMS_TAILSCALE_LISTEN` listener: the only one on
    /// which `Tailscale-User-Login` names the caller.
    pub fn tailscale_target(auth: Arc<Auth>, public_paths: &'static [&'static str]) -> Self {
        Self {
            auth,
            public_paths,
            trust_tailscale: true,
        }
    }

    /// `router` behind the layer (every route and the fallback).
    pub fn wrap(&self, router: Router) -> Router {
        router.layer(middleware::from_fn_with_state(self.clone(), guard))
    }

    /// tonic `routes` behind the layer.
    pub fn wrap_grpc(&self, routes: tonic::service::Routes) -> tonic::service::Routes {
        self.wrap(routes.into_axum_router()).into()
    }
}

async fn guard(State(state): State<AuthLayerState>, mut request: Request, next: Next) -> Response {
    // Read (on the tailscale serve target only), then strip everywhere: no
    // handler or proxy behind this layer ever sees them.
    let tailnet_user = state
        .trust_tailscale
        .then(|| tailscale::identity(request.headers()))
        .flatten();
    tailscale::strip(request.headers_mut());
    let cors = state.auth.cors();
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let allowed_origin = origin
        .as_deref()
        .filter(|o| cors.allows(o))
        .map(str::to_owned);
    let with_cors = |mut response: Response| {
        if let Some(origin) = &allowed_origin {
            add_cors(response.headers_mut(), origin);
        }
        response
    };
    if request.method() == Method::OPTIONS
        && origin.is_some()
        && request
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
    {
        return match &allowed_origin {
            Some(origin) => preflight(origin, request.headers()),
            None => refuse(
                request.headers(),
                &AuthError::forbidden(
                    reason::CORS_ORIGIN_NOT_ALLOWED,
                    "this origin is not in LOAMS_CORS_ORIGINS",
                ),
            ),
        };
    }
    if !state.auth.enforced() {
        request.extensions_mut().insert(Auth::dev_loopback());
        return with_cors(next.run(request).await);
    }
    if credential_in_query(request.uri().query()) {
        let err = AuthError::unauthenticated(
            reason::CREDENTIAL_IN_QUERY_STRING,
            "credentials in the URL are refused; send Authorization: Bearer",
        );
        return with_cors(refuse(request.headers(), &err));
    }
    if state.public_paths.contains(&request.uri().path()) {
        return with_cors(next.run(request).await);
    }
    let result = match (
        credential(request.headers()),
        &tailnet_user,
        state.auth.tailscale(),
    ) {
        // An explicit credential wins; otherwise the tailnet identity.
        (Err(err), Some((login, name)), Some(ts)) if err.reason == reason::TOKEN_MISSING => {
            ts.admit(login, name.as_deref())
        }
        (Ok(credential), _, _) => state.auth.authenticate(&credential).await,
        (Err(err), _, _) => Err(err),
    };
    match result {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            with_cors(next.run(request).await)
        }
        Err(err) => {
            tracing::debug!(
                reason = err.reason,
                path = request.uri().path(),
                "request refused"
            );
            with_cors(refuse(request.headers(), &err))
        }
    }
}

fn credential_in_query(query: Option<&str>) -> bool {
    query.is_some_and(|q| {
        q.split('&').any(|pair| {
            let name = pair.split('=').next().unwrap_or_default();
            QUERY_CREDENTIALS
                .iter()
                .any(|c| name.eq_ignore_ascii_case(c))
        })
    })
}

/// A bearer, Basic password, `ApiKey` or `api-key` value as a credential.
fn classify(value: &str) -> Result<Credential, AuthError> {
    let value = value.trim();
    if api_key::looks_like_key(value) {
        Ok(Credential::ApiKey(value.to_owned()))
    } else if value.starts_with(api_key::PREFIX) {
        Err(AuthError::unauthenticated(
            reason::TOKEN_MALFORMED,
            "an API key is lk_<26-char id>_<32 characters>",
        ))
    } else if value.split('.').count() == 3 {
        Ok(Credential::Jwt(value.to_owned()))
    } else {
        Err(AuthError::unauthenticated(
            reason::TOKEN_MALFORMED,
            "the credential is neither an API key nor a JWT",
        ))
    }
}

fn malformed(message: &str) -> AuthError {
    AuthError::unauthenticated(reason::TOKEN_MALFORMED, message)
}

/// The request's credential: `Authorization` (Bearer, Basic with the key or
/// token as the password, Elasticsearch's `ApiKey`) or Qdrant's `api-key`.
pub(crate) fn credential(headers: &HeaderMap) -> Result<Credential, AuthError> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    if let Some(value) = values.next() {
        if values.next().is_some() {
            return Err(malformed("more than one Authorization header"));
        }
        let value = value
            .to_str()
            .map_err(|_| malformed("the Authorization header is not text"))?;
        let (scheme, rest) = value.trim().split_once(' ').unwrap_or((value, ""));
        let rest = rest.trim();
        return match scheme.to_ascii_lowercase().as_str() {
            "bearer" => classify(rest),
            "basic" => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(rest)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or_else(|| malformed("the Basic credential is not base64 text"))?;
                let (_, password) = decoded
                    .split_once(':')
                    .ok_or_else(|| malformed("the Basic credential has no password"))?;
                classify(password)
            }
            "apikey" => {
                if api_key::looks_like_key(rest) {
                    return classify(rest);
                }
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(rest)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or_else(|| malformed("the ApiKey credential is not an API key"))?;
                classify(decoded.split_once(':').map_or(decoded.as_str(), |(_, k)| k))
            }
            _ => Err(malformed(
                "unsupported Authorization scheme; use Bearer, Basic or ApiKey",
            )),
        };
    }
    if let Some(value) = headers.get("api-key") {
        let value = value
            .to_str()
            .map_err(|_| malformed("the api-key header is not text"))?;
        return classify(value);
    }
    // Cloudflare Access in front of the engine sends its JWT here.
    if let Some(value) = headers.get("cf-access-jwt-assertion") {
        let value = value
            .to_str()
            .map_err(|_| malformed("the Cf-Access-Jwt-Assertion header is not text"))?;
        return match classify(value)? {
            jwt @ Credential::Jwt(_) => Ok(jwt),
            Credential::ApiKey(_) => Err(malformed("Cf-Access-Jwt-Assertion is not a JWT")),
        };
    }
    Err(AuthError::unauthenticated(
        reason::TOKEN_MISSING,
        "this engine needs a credential: Authorization: Bearer <API key or OIDC token>",
    ))
}

fn add_cors(headers: &mut HeaderMap, origin: &str) {
    if let Ok(origin) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    headers.append(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(EXPOSE_HEADERS),
    );
}

fn preflight(origin: &str, request: &HeaderMap) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    let headers = response.headers_mut();
    add_cors(headers, origin);
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(ALLOW_METHODS),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ALLOW_HEADERS),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    // Chrome's Private Network Access: the console is public, the engine on
    // a tailnet address.
    if request
        .get("access-control-request-private-network")
        .is_some_and(|v| v == "true")
    {
        headers.insert(
            HeaderName::from_static("access-control-allow-private-network"),
            HeaderValue::from_static("true"),
        );
    }
    response
}

/// The refusal in the caller's protocol.
pub(crate) fn refuse(request: &HeaderMap, err: &AuthError) -> Response {
    let content_type = request
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mut response = if content_type.starts_with("application/grpc") {
        grpc_refusal(&content_type, err)
    } else if request.contains_key("connect-protocol-version")
        || content_type.starts_with("application/connect+")
    {
        let code = match err.kind {
            AuthErrorKind::Unauthenticated => "unauthenticated",
            AuthErrorKind::Forbidden => "permission_denied",
            AuthErrorKind::Unavailable => "unavailable",
        };
        json(
            err.status(),
            &serde_json::json!({
                "code": code,
                "message": format!("{}: {}", err.reason, err.message),
            }),
        )
    } else {
        json(
            err.status(),
            &serde_json::json!({
                "error": match err.kind {
                    AuthErrorKind::Unauthenticated => "unauthenticated",
                    AuthErrorKind::Forbidden => "permission_denied",
                    AuthErrorKind::Unavailable => "unavailable",
                },
                "reason": err.reason,
                "message": err.message,
            }),
        )
    };
    let headers = response.headers_mut();
    headers.insert(REASON_HEADER, HeaderValue::from_static(err.reason));
    match err.kind {
        AuthErrorKind::Unauthenticated => {
            let challenge = if err.reason == reason::TOKEN_MISSING {
                "Bearer realm=\"loams\"".to_owned()
            } else {
                format!(
                    "Bearer realm=\"loams\", error=\"invalid_token\", error_description=\"{}\"",
                    err.reason
                )
            };
            if let Ok(value) = HeaderValue::from_str(&challenge) {
                headers.insert(header::WWW_AUTHENTICATE, value);
            }
        }
        AuthErrorKind::Unavailable => {
            headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        AuthErrorKind::Forbidden => {}
    }
    response
}

fn json(status: StatusCode, body: &serde_json::Value) -> Response {
    let mut response = Response::new(Body::from(body.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn grpc_refusal(content_type: &str, err: &AuthError) -> Response {
    let status = match err.kind {
        AuthErrorKind::Unauthenticated => "16",
        AuthErrorKind::Forbidden => "7",
        AuthErrorKind::Unavailable => "14",
    };
    let message = format!("{}: {}", err.reason, err.message);
    let mut response = Response::new(Body::empty());
    let headers = response.headers_mut();
    let content_type = if content_type.starts_with("application/grpc-web") {
        content_type
    } else {
        "application/grpc"
    };
    if let Ok(value) = HeaderValue::from_str(content_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    headers.insert("grpc-status", HeaderValue::from_static(status));
    if let Ok(value) = HeaderValue::from_str(&percent_encode(&message)) {
        headers.insert("grpc-message", value);
    }
    response
}

/// gRPC's `grpc-message` encoding: printable ASCII except `%` as is.
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.append(*k, HeaderValue::from_str(v).unwrap());
        }
        map
    }

    const KEY: &str = "lk_01J9Z8Y7X6W5V4T3S2R1Q0P9N8_abcdefghijklmnopqrstuvwxyzABCDEF";

    #[test]
    fn credentials_are_found_in_every_carrier() {
        assert!(api_key::looks_like_key(KEY));
        let bearer = format!("Bearer {KEY}");
        assert_eq!(
            credential(&headers(&[("authorization", &bearer)])).unwrap(),
            Credential::ApiKey(KEY.to_owned())
        );
        let basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("loams:{KEY}"))
        );
        assert_eq!(
            credential(&headers(&[("authorization", &basic)])).unwrap(),
            Credential::ApiKey(KEY.to_owned())
        );
        let es = format!(
            "ApiKey {}",
            base64::engine::general_purpose::STANDARD.encode(format!("id:{KEY}"))
        );
        assert_eq!(
            credential(&headers(&[("authorization", &es)])).unwrap(),
            Credential::ApiKey(KEY.to_owned())
        );
        assert_eq!(
            credential(&headers(&[("api-key", KEY)])).unwrap(),
            Credential::ApiKey(KEY.to_owned())
        );
        assert_eq!(
            credential(&headers(&[("authorization", "Bearer a.b.c")])).unwrap(),
            Credential::Jwt("a.b.c".to_owned())
        );
        assert_eq!(
            credential(&headers(&[("cf-access-jwt-assertion", "a.b.c")])).unwrap(),
            Credential::Jwt("a.b.c".to_owned())
        );
    }

    #[test]
    fn bad_credentials_get_clear_reasons() {
        let reason_of = |h: HeaderMap| credential(&h).unwrap_err().reason;
        assert_eq!(reason_of(headers(&[])), reason::TOKEN_MISSING);
        assert_eq!(
            reason_of(headers(&[("authorization", "Bearer nope")])),
            reason::TOKEN_MALFORMED
        );
        assert_eq!(
            reason_of(headers(&[("authorization", "Digest x")])),
            reason::TOKEN_MALFORMED
        );
        assert_eq!(
            reason_of(headers(&[
                ("authorization", "Bearer a.b.c"),
                ("authorization", "Bearer d.e.f")
            ])),
            reason::TOKEN_MALFORMED
        );
    }

    #[test]
    fn query_string_credentials_are_detected() {
        assert!(credential_in_query(Some("access_token=x")));
        assert!(credential_in_query(Some("a=1&Token=x")));
        assert!(!credential_in_query(Some("tokens=1&limit=2")));
        assert!(!credential_in_query(None));
    }

    #[test]
    fn grpc_refusal_is_trailers_only() {
        let err = AuthError::unauthenticated(reason::TOKEN_MISSING, "100% missing");
        let response = refuse(
            &headers(&[("content-type", "application/grpc+proto")]),
            &err,
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["grpc-status"], "16");
        assert_eq!(
            response.headers()["grpc-message"],
            "token_missing: 100%25 missing"
        );
        assert_eq!(response.headers()[REASON_HEADER], "token_missing");
    }

    #[test]
    fn connect_and_rest_refusals_use_http_status() {
        let err = AuthError::forbidden(reason::PRINCIPAL_NOT_ALLOWED, "no");
        let response = refuse(&headers(&[("connect-protocol-version", "1")]), &err);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let err = AuthError::unauthenticated(reason::TOKEN_EXPIRED, "old");
        let response = refuse(&headers(&[]), &err);
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(
            response.headers()[header::WWW_AUTHENTICATE]
                .to_str()
                .unwrap()
                .contains("token_expired")
        );
    }
}
