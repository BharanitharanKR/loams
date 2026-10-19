//! Who is calling Live (LV1 plan Tasks 14 and 16, design §45 §5).
//!
//! **Platform principals.** Live verifies no platform credential itself:
//! the engine's authentication layer (`crates/loams/src/auth`: console
//! sessions, OIDC tokens, API keys, tailnet users) admits the request in
//! front of both Live services and hands Live a [`Caller`] in the request's
//! extensions, with what the credential reaches (the engine principal's
//! grant: org, environment, scopes, expiry). Where a source names none of
//! them, the caller has the self-hosted default: the install's one org and
//! environment, and every scope. A request with no `Caller` (a server
//! started without the engine's layer, as the crate's own tests do) is
//! [`Caller::unauthenticated`].
//!
//! **App end users** (Task 16, §45 §5.3) are not Loams principals: they
//! bring an OIDC token from an issuer the addressed app trusts
//! ([`TrustedIssuer`]). On `LiveService` the engine's layer leaves a
//! request it cannot admit (no credential, or a JWT from an issuer it does
//! not trust) to the app, as [`Caller::anonymous`]; [`identify`] then
//! verifies its bearer against the app's issuers ([`EndUsers`]: discovery,
//! the JWKS cached for 10 minutes and fetched again for an unknown `kid` at
//! most every 30 s), or lets it through without one only when the app
//! allows anonymous calls. Anything else is `UNAUTHENTICATED`. End users and
//! anonymous callers may call only public functions ([`crate::authz`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use connectrpc::RequestContext;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use tokio::sync::{Mutex, RwLock};

use crate::apps::{AppRecord, TrustedIssuer};
use crate::{Identity, LiveError, LiveValue};

/// The id a request without a [`Caller`] is recorded as.
pub use crate::deploy::UNAUTHENTICATED;

/// The id of an anonymous caller (Task 16).
pub const ANONYMOUS: &str = "anonymous";

/// How long a fetched JWKS is used before it is fetched again (§45 §5.3).
pub const JWKS_TTL: Duration = Duration::from_secs(600);
/// The shortest gap between two JWKS fetches of one issuer; an unknown
/// `kid` forces one no sooner.
pub const JWKS_MIN_REFETCH: Duration = Duration::from_secs(30);
/// Clock skew allowed on an end user's `exp` and `nbf` (§45 §5.4).
pub const LEEWAY_SECS: u64 = 60;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// The reasons of an end user's refusal (the engine's own, registered in
/// `docs/api/reasons.md`).
pub mod reason {
    pub const TOKEN_MISSING: &str = "token_missing";
    pub const TOKEN_MALFORMED: &str = "token_malformed";
    pub const TOKEN_EXPIRED: &str = "token_expired";
    pub const TOKEN_REVOKED: &str = "token_revoked";
    pub const TOKEN_NOT_YET_VALID: &str = "token_not_yet_valid";
    pub const TOKEN_BAD_SIGNATURE: &str = "token_bad_signature";
    pub const TOKEN_WRONG_AUDIENCE: &str = "token_wrong_audience";
    pub const TOKEN_WRONG_ISSUER: &str = "token_wrong_issuer";
    pub const TOKEN_UNKNOWN_KEY: &str = "token_unknown_key";
    pub const TOKEN_ALGORITHM: &str = "token_algorithm_not_allowed";
    pub const AUTH_UNAVAILABLE: &str = "auth_unavailable";
}

/// What kind of credential admitted the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallerKind {
    /// An OIDC or Loams token of a person.
    User,
    /// A console session.
    Session,
    /// An API key.
    ApiKey,
    /// A tailnet user, as `tailscale serve` named them.
    Tailscale,
    /// The engine runs without authentication, on loopback.
    DevLoopback,
    /// No authentication layer ran (a bare Live server).
    Unauthenticated,
    /// An app end user, verified against the app's trusted issuers (Task
    /// 16). Not a platform principal.
    EndUser,
    /// No platform credential: the engine's layer left the request to the
    /// app ([`identify`] makes it an end user, or keeps it anonymous where
    /// the app allows that). Not a platform principal.
    Anonymous,
}

impl CallerKind {
    /// The kind's name in logs and audit lines.
    pub fn as_str(self) -> &'static str {
        match self {
            CallerKind::User => "user",
            CallerKind::Session => "session",
            CallerKind::ApiKey => "api_key",
            CallerKind::Tailscale => "tailscale",
            CallerKind::DevLoopback => "dev_loopback",
            CallerKind::Unauthenticated => "unauthenticated",
            CallerKind::EndUser => "end_user",
            CallerKind::Anonymous => "anonymous",
        }
    }
}

/// Who is calling: the principal the engine's authentication layer
/// admitted, put in the request's extensions. It never holds a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The principal's id (an OIDC subject, `apikey:<id>`, a user id,
    /// `tailscale:<login>` or `dev-loopback`), an end user's
    /// `tokenIdentifier`, or `anonymous`.
    pub id: String,
    pub kind: CallerKind,
    /// The org the credential belongs to, when its source names one.
    pub org: Option<String>,
    /// The environment the credential is bound to (`<project>/<env>`),
    /// when its source names one. Such a credential reaches only the app of
    /// that environment (LV1 plan Task 15).
    pub environment: Option<String>,
    /// The scopes it carries; `None` is every scope.
    pub scopes: Option<BTreeSet<String>>,
    /// When the credential expires, Unix ms, when known.
    pub expires_at_ms: Option<u64>,
    /// The end user's identity ([`CallerKind::EndUser`] only).
    pub identity: Option<Identity>,
}

impl Caller {
    /// A caller of `kind` with id `id` and the self-hosted default grant.
    pub fn new(id: impl Into<String>, kind: CallerKind) -> Self {
        Caller {
            id: id.into(),
            kind,
            org: None,
            environment: None,
            scopes: None,
            expires_at_ms: None,
            identity: None,
        }
    }

    /// A request the engine's layer left to the app (Task 16): no platform
    /// credential.
    pub fn anonymous() -> Self {
        Caller::new(ANONYMOUS, CallerKind::Anonymous)
    }

    /// The end user `identity`, whose token expires at `expires_at_ms`.
    pub fn end_user(identity: Identity, expires_at_ms: Option<u64>) -> Self {
        Caller {
            expires_at_ms,
            identity: Some(identity.clone()),
            ..Caller::new(identity.token_identifier, CallerKind::EndUser)
        }
    }

    /// Who a subscription runs for ([`SubKey::identity`](crate::SubKey),
    /// LV1 plan Task 17): `end_user:<issuer length>:<issuer><subject>` for
    /// an end user (the issuer and subject unambiguously, which
    /// `tokenIdentifier`'s `|` is not), `anonymous`, or `principal:<id>`
    /// for a platform principal. Two callers with one key see one result.
    pub fn subscription_key(&self) -> String {
        match (&self.kind, &self.identity) {
            (CallerKind::EndUser, Some(identity)) => format!(
                "end_user:{}:{}{}",
                identity.issuer.len(),
                identity.issuer,
                identity.subject
            ),
            (CallerKind::Anonymous, _) => ANONYMOUS.to_string(),
            _ => format!("principal:{}", self.id),
        }
    }

    /// Whether this is a platform principal (Loams token, session, API key,
    /// tailnet user, the developer on loopback, or a bare server's
    /// unauthenticated caller), not an app end user or anonymous.
    pub fn is_platform(&self) -> bool {
        !matches!(self.kind, CallerKind::EndUser | CallerKind::Anonymous)
    }

    /// The caller of a request no authentication layer admitted.
    pub fn unauthenticated() -> Self {
        Caller::new(UNAUTHENTICATED, CallerKind::Unauthenticated)
    }

    /// The caller of `ctx`: the layer's [`Caller`], else
    /// [`unauthenticated`](Self::unauthenticated).
    pub fn of(ctx: &RequestContext) -> Self {
        ctx.extensions()
            .get::<Caller>()
            .cloned()
            .unwrap_or_else(Caller::unauthenticated)
    }

    /// Whether the credential carries `scope` (every scope when it names
    /// none).
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.as_ref().is_none_or(|s| s.contains(scope))
    }
}

/// The bearer token of `ctx`'s `Authorization: Bearer` header, when it is
/// shaped like a JWT (three dot-separated parts).
pub fn bearer_jwt(ctx: &RequestContext) -> Option<String> {
    let value = ctx.header("authorization")?.to_str().ok()?.trim();
    let (scheme, token) = value.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && token.split('.').count() == 3)
        .then(|| token.to_string())
}

/// Who `caller` is in `app` (Task 16, §45 §5.3), given the request's bearer
/// JWT:
///
/// - a principal the engine's layer admitted stays that principal;
/// - the developer on loopback, or a bare server's caller, becomes the end
///   user its bearer names when the token's issuer is one the app trusts
///   (so `ctx.auth` works in `loams dev`), and stays itself otherwise;
/// - a request the layer left to the app ([`Caller::anonymous`]) becomes
///   the end user its bearer names (`UNAUTHENTICATED` if the issuer is not
///   the app's or the token does not verify), or, with no bearer, stays
///   anonymous only where the app allows anonymous calls.
pub async fn identify(
    end_users: &EndUsers,
    caller: Caller,
    bearer: Option<&str>,
    app: &AppRecord,
) -> Result<Caller, LiveError> {
    let issuers = &app.settings.issuers;
    match caller.kind {
        CallerKind::DevLoopback | CallerKind::Unauthenticated => match bearer {
            Some(token) if trusted(issuers, token).is_some() => {
                end_users.verify(token, issuers).await
            }
            _ => Ok(caller),
        },
        CallerKind::Anonymous => match bearer {
            Some(token) => end_users.verify(token, issuers).await,
            None if app.settings.allow_anonymous => Ok(caller),
            None => Err(LiveError::Unauthenticated {
                reason: reason::TOKEN_MISSING,
                message: format!(
                    "the Live app {} takes no anonymous calls: send Authorization: Bearer <a \
                     token from one of its issuers>",
                    app.name
                ),
            }),
        },
        _ => Ok(caller),
    }
}

/// The issuer of `issuers` that `token`'s (unverified) `iss` names.
fn trusted<'a>(issuers: &'a [TrustedIssuer], token: &str) -> Option<&'a TrustedIssuer> {
    let iss = unverified_issuer(token)?;
    issuers.iter().find(|i| same_issuer(&i.issuer, &iss))
}

/// Issuers compare without a trailing `/`.
fn same_issuer(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

/// The token's `iss`, unverified: it only picks the issuer's keys.
fn unverified_issuer(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = data_encoding::BASE64URL_NOPAD
        .decode(payload.trim_end_matches('=').as_bytes())
        .ok()?;
    match serde_json::from_slice::<serde_json::Value>(&bytes).ok()? {
        serde_json::Value::Object(mut claims) => match claims.remove("iss")? {
            serde_json::Value::String(iss) => Some(iss),
            _ => None,
        },
        _ => None,
    }
}

fn unauthenticated(reason: &'static str, message: impl Into<String>) -> LiveError {
    LiveError::Unauthenticated {
        reason,
        message: message.into(),
    }
}

/// One issuer's signing keys.
#[derive(Debug)]
struct IssuerKeys {
    issuer: String,
    jwks_uri: Option<String>,
    cache: RwLock<Option<(JwkSet, Instant)>>,
    /// Serializes fetches; holds when the last one started.
    fetching: Mutex<Option<Instant>>,
}

#[derive(Debug, serde::Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: String,
}

/// Verifies app end users' OIDC tokens against the issuers each app trusts
/// (Task 16): RS256 or EdDSA only, the `alg` matching its key's family;
/// `iss`, `aud`, `exp` and `sub` required, `nbf` checked, [`LEEWAY_SECS`] of
/// skew; `aud` one of the issuer's audiences. Keys come from the issuer's
/// JWKS (its `jwks_uri`, else OIDC discovery), shared by every app that
/// trusts the issuer, cached for [`JWKS_TTL`] and fetched early for an
/// unknown `kid` at most every [`JWKS_MIN_REFETCH`]; a failed fetch keeps
/// the cached keys.
#[derive(Debug)]
pub struct EndUsers {
    /// The HTTP client of discovery and JWKS fetches (an error when no TLS
    /// provider could be set up: every fetch then fails).
    http: Result<reqwest::Client, String>,
    ttl: Duration,
    min_refetch: Duration,
    keys: StdMutex<HashMap<IssuerId, Arc<IssuerKeys>>>,
}

/// An issuer's keys are shared by its URL (without a trailing `/`) and its
/// JWKS URL.
type IssuerId = (String, Option<String>);

impl Default for EndUsers {
    fn default() -> Self {
        Self::with_timing(JWKS_TTL, JWKS_MIN_REFETCH)
    }
}

impl EndUsers {
    /// A verifier whose JWKS cache holds keys for `ttl` and fetches at most
    /// every `min_refetch` (tests shorten both).
    pub fn with_timing(ttl: Duration, min_refetch: Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("the OIDC HTTP client: {e}"));
        EndUsers {
            http,
            ttl,
            min_refetch,
            keys: StdMutex::new(HashMap::new()),
        }
    }

    /// The end user `token` names, verified against `issuers`.
    pub async fn verify(
        &self,
        token: &str,
        issuers: &[TrustedIssuer],
    ) -> Result<Caller, LiveError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| {
            unauthenticated(reason::TOKEN_MALFORMED, "the bearer token is not a JWT")
        })?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::EdDSA) {
            return Err(unauthenticated(
                reason::TOKEN_ALGORITHM,
                format!(
                    "tokens signed with {:?} are refused; use RS256 or EdDSA",
                    header.alg
                ),
            ));
        }
        let iss = unverified_issuer(token).ok_or_else(|| {
            unauthenticated(reason::TOKEN_MALFORMED, "the token has no iss claim")
        })?;
        let trusted = issuers
            .iter()
            .find(|i| same_issuer(&i.issuer, &iss))
            .ok_or_else(|| {
                unauthenticated(
                    reason::TOKEN_WRONG_ISSUER,
                    "the token is from an issuer this Live app does not trust",
                )
            })?;
        let at = self.issuer_keys(trusted);
        let kid = header.kid.as_deref();
        let mut jwk = find(&self.keys_of(&at, false).await?, kid, header.alg).cloned();
        if jwk.is_none() {
            jwk = find(&self.keys_of(&at, true).await?, kid, header.alg).cloned();
        }
        let jwk = jwk.ok_or_else(|| {
            unauthenticated(
                reason::TOKEN_UNKNOWN_KEY,
                "the token is signed with a key the issuer does not publish",
            )
        })?;
        if !family_matches(&jwk, header.alg) {
            return Err(unauthenticated(
                reason::TOKEN_ALGORITHM,
                "the token's alg does not match its signing key",
            ));
        }
        let key = DecodingKey::from_jwk(&jwk).map_err(|_| {
            unauthenticated(
                reason::TOKEN_UNKNOWN_KEY,
                "the issuer's signing key is not usable",
            )
        })?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&iss]);
        validation.set_audience(&trusted.audiences);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = LEEWAY_SECS;
        let data = jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(
            token,
            &key,
            &validation,
        )
        .map_err(|e| map_error(&e))?;
        let claims = data.claims;
        let subject = match claims.get("sub") {
            Some(serde_json::Value::String(sub)) if !sub.is_empty() => sub.clone(),
            _ => {
                return Err(unauthenticated(
                    reason::TOKEN_MALFORMED,
                    "the token's sub is not a non-empty string",
                ));
            }
        };
        let expires_at_ms = claims
            .get("exp")
            .and_then(serde_json::Value::as_u64)
            .map(|s| s.saturating_mul(1000));
        let claims = json_value(&serde_json::Value::Object(claims));
        Ok(Caller::end_user(
            Identity::new(&iss, &subject, claims),
            expires_at_ms,
        ))
    }

    fn issuer_keys(&self, issuer: &TrustedIssuer) -> Arc<IssuerKeys> {
        let key = (
            issuer.issuer.trim_end_matches('/').to_string(),
            issuer.jwks_uri.clone(),
        );
        let mut keys = self.keys.lock().unwrap_or_else(|p| p.into_inner());
        keys.entry(key)
            .or_insert_with(|| {
                Arc::new(IssuerKeys {
                    issuer: issuer.issuer.clone(),
                    jwks_uri: issuer.jwks_uri.clone(),
                    cache: RwLock::new(None),
                    fetching: Mutex::new(None),
                })
            })
            .clone()
    }

    /// The cached keys, fetched when absent or stale, or (`force`, an
    /// unknown `kid`) when the last fetch is old enough.
    async fn keys_of(&self, at: &IssuerKeys, force: bool) -> Result<JwkSet, LiveError> {
        if !force
            && let Some((keys, fetched)) = at.cache.read().await.as_ref()
            && fetched.elapsed() < self.ttl
        {
            return Ok(keys.clone());
        }
        let mut last = at.fetching.lock().await;
        // Another request may have fetched while this one waited; and a
        // fetch within `min_refetch` (even a failed one) is not repeated, so
        // a flood of tokens never hammers an issuer that is down.
        let recent = last.is_some_and(|t| t.elapsed() < self.min_refetch);
        match at.cache.read().await.as_ref() {
            Some((keys, fetched)) => {
                let fresh = fetched.elapsed() < self.ttl;
                if (fresh && !force) || recent {
                    return Ok(keys.clone());
                }
            }
            None if recent => {
                return Err(LiveError::AuthUnavailable(
                    "the app issuer's signing keys cannot be fetched; try again".into(),
                ));
            }
            None => {}
        }
        *last = Some(Instant::now());
        match self.fetch(at).await {
            Ok(keys) => {
                *at.cache.write().await = Some((keys.clone(), Instant::now()));
                Ok(keys)
            }
            Err(err) => {
                tracing::warn!(%err, issuer = %at.issuer, "fetching an app issuer's signing keys failed");
                match at.cache.read().await.as_ref() {
                    Some((keys, _)) => Ok(keys.clone()),
                    None => Err(LiveError::AuthUnavailable(
                        "the app issuer's signing keys cannot be fetched; try again".into(),
                    )),
                }
            }
        }
    }

    async fn fetch(&self, at: &IssuerKeys) -> Result<JwkSet, String> {
        let http = self.http.as_ref().map_err(String::clone)?;
        let jwks_uri = match &at.jwks_uri {
            Some(uri) => uri.clone(),
            None => {
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    at.issuer.trim_end_matches('/')
                );
                let discovery: Discovery = http
                    .get(&url)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                    .map_err(|e| format!("OIDC discovery: {e}"))?
                    .json()
                    .await
                    .map_err(|e| format!("OIDC discovery: {e}"))?;
                if !same_issuer(&discovery.issuer, &at.issuer) {
                    return Err(format!(
                        "OIDC discovery names the issuer {:?}, not {:?}",
                        discovery.issuer, at.issuer
                    ));
                }
                if !crate::apps::https_or_loopback(&discovery.jwks_uri) {
                    return Err(format!(
                        "OIDC discovery's jwks_uri {:?} is not https (http only on loopback)",
                        discovery.jwks_uri
                    ));
                }
                discovery.jwks_uri
            }
        };
        http.get(&jwks_uri)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| format!("JWKS: {e}"))?
            .json::<JwkSet>()
            .await
            .map_err(|e| format!("JWKS: {e}"))
    }
}

/// The key `kid` names; with no `kid`, only an unambiguous key of `alg`'s
/// family.
fn find<'a>(keys: &'a JwkSet, kid: Option<&str>, alg: Algorithm) -> Option<&'a Jwk> {
    match kid {
        Some(kid) => keys.find(kid),
        None => {
            let mut candidates = keys
                .keys
                .iter()
                .filter(|k| DecodingKey::from_jwk(k).is_ok() && family_matches(k, alg));
            let first = candidates.next();
            candidates.next().is_none().then_some(first).flatten()
        }
    }
}

fn family_matches(jwk: &Jwk, alg: Algorithm) -> bool {
    use jsonwebtoken::jwk::AlgorithmParameters as P;
    match (&jwk.algorithm, alg) {
        (P::RSA(_), Algorithm::RS256) => true,
        (P::OctetKeyPair(p), Algorithm::EdDSA) => {
            matches!(p.curve, jsonwebtoken::jwk::EllipticCurve::Ed25519)
        }
        _ => false,
    }
}

fn map_error(err: &jsonwebtoken::errors::Error) -> LiveError {
    use jsonwebtoken::errors::ErrorKind;
    let (why, message) = match err.kind() {
        ErrorKind::ExpiredSignature => (
            reason::TOKEN_EXPIRED,
            "the token has expired; sign in again",
        ),
        ErrorKind::ImmatureSignature => (reason::TOKEN_NOT_YET_VALID, "the token is not valid yet"),
        ErrorKind::InvalidSignature => (
            reason::TOKEN_BAD_SIGNATURE,
            "the token's signature does not verify",
        ),
        ErrorKind::InvalidAudience => (
            reason::TOKEN_WRONG_AUDIENCE,
            "the token is not for this Live app (its issuer's audiences)",
        ),
        ErrorKind::InvalidIssuer => (
            reason::TOKEN_WRONG_ISSUER,
            "the token is from an issuer this Live app does not trust",
        ),
        ErrorKind::InvalidAlgorithm => (
            reason::TOKEN_ALGORITHM,
            "the token's alg does not match its signing key",
        ),
        ErrorKind::MissingRequiredClaim(_) => (
            reason::TOKEN_MALFORMED,
            "the token lacks a required claim (exp, iss, aud, sub)",
        ),
        _ => (reason::TOKEN_MALFORMED, "the token cannot be decoded"),
    };
    unauthenticated(why, message)
}

/// A JSON claim as a Live value: numbers are doubles (JavaScript numbers,
/// as the claims of a token are in the browser).
fn json_value(v: &serde_json::Value) -> LiveValue {
    match v {
        serde_json::Value::Null => LiveValue::Null,
        serde_json::Value::Bool(b) => LiveValue::Bool(*b),
        serde_json::Value::Number(n) => LiveValue::F64(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => LiveValue::Str(s.clone()),
        serde_json::Value::Array(items) => LiveValue::Array(items.iter().map(json_value).collect()),
        serde_json::Value::Object(fields) => LiveValue::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), json_value(v)))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_caller_has_every_scope_and_no_environment() {
        let caller = Caller::new("apikey:01", CallerKind::ApiKey);
        assert!(caller.has_scope("live:deploy"));
        assert_eq!(caller.environment, None);
        let narrow = Caller {
            scopes: Some(["live:call".to_string()].into()),
            ..caller
        };
        assert!(narrow.has_scope("live:call") && !narrow.has_scope("live:admin"));
        assert_eq!(Caller::unauthenticated().id, UNAUTHENTICATED);
        assert!(Caller::unauthenticated().is_platform());
        assert!(!Caller::anonymous().is_platform());
    }

    #[test]
    fn the_unverified_issuer_is_read_from_the_payload() {
        let payload =
            data_encoding::BASE64URL_NOPAD.encode(br#"{"iss":"https://id.example/","sub":"x"}"#);
        let token = format!("e30.{payload}.sig");
        assert_eq!(
            unverified_issuer(&token).as_deref(),
            Some("https://id.example/")
        );
        let issuers = [TrustedIssuer {
            issuer: "https://id.example".into(),
            audiences: vec!["app".into()],
            jwks_uri: None,
        }];
        assert!(trusted(&issuers, &token).is_some());
        assert!(trusted(&issuers, "e30.e30.sig").is_none());
        assert_eq!(unverified_issuer("garbage"), None);
    }

    #[test]
    fn claims_become_live_values_with_numbers_as_doubles() {
        let v = json_value(&serde_json::json!({"exp": 5, "roles": ["a"], "ok": true}));
        let LiveValue::Object(fields) = v else {
            panic!("an object");
        };
        assert_eq!(fields["exp"], LiveValue::F64(5.0));
        assert_eq!(
            fields["roles"],
            LiveValue::Array(vec![LiveValue::Str("a".into())])
        );
        assert_eq!(fields["ok"], LiveValue::Bool(true));
    }
}
