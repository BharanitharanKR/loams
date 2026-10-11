//! Engine authentication for the preview release (API2-Q1, design §44 §5).
//!
//! One [`Auth`] guards every HTTP surface the engine serves to the network:
//! the main port (Connect, gRPC, gRPC-Web, the native REST and the House
//! proxy), the Qdrant REST and gRPC gateways, the Elasticsearch gateway and
//! Flight SQL. The wire protocols (PostgreSQL, MySQL, native stream gRPC,
//! Live, durable) already refuse any address but loopback, and the SQL gate
//! has its own users; none of them is reachable from a tailnet.
//!
//! The mode comes from `LOAMS_AUTH_MODE` ([`AuthConfig::from_env`]):
//!
//! - `apikey`: `Authorization: Bearer lk_…` (or the key as a Basic password,
//!   an Elasticsearch `ApiKey` or Qdrant's `api-key` header), checked against
//!   argon2id hashes in `LOAMS_API_KEYS_FILE` ([`api_key`]);
//! - `oidc`: an RS256 or EdDSA JWT from `LOAMS_OIDC_ISSUER` for
//!   `LOAMS_OIDC_AUDIENCE`, verified against the issuer's JWKS ([`oidc`]),
//!   and admitted only when its subject, verified email or group is on the
//!   allowlist (a shared issuer signs tokens for everyone's console);
//! - `oidc,apikey`: both (the compose default);
//! - `off` (the default): no authentication, and the server refuses to start
//!   unless every listener is on loopback.
//!
//! [`Authenticator`] is the hook MT1 extends (API2 Task 6): the layer
//! ([`layer`]) turns a request's credential into a [`Principal`] in the
//! request extensions, and no handler parses a header.

pub mod api_key;
pub mod layer;
pub mod oidc;

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

pub use layer::{AuthLayerState, Cors};

/// The environment variable that picks the mode.
pub const MODE_ENV: &str = "LOAMS_AUTH_MODE";

/// Who made a request (API2 shared contract).
#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    /// The OIDC `sub`, the API key's id, or `dev-loopback`.
    pub subject: String,
    pub kind: PrincipalKind,
    /// A name to show: the token's `name` or `preferred_username`, or the
    /// API key's label.
    pub display_name: String,
    /// A verified email, for users.
    pub email: Option<String>,
}

impl fmt::Debug for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Principal")
            .field("subject", &self.subject)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// What kind of credential a [`Principal`] presented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalKind {
    /// An OIDC user (a JWT from the issuer).
    User,
    /// An API key from `LOAMS_API_KEYS_FILE`.
    ApiKey,
    /// `LOAMS_AUTH_MODE=off` on loopback: everyone on this host.
    DevLoopback,
}

/// How a refusal is answered: `401`, `403` or `503`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// No usable credential: `401`, gRPC `UNAUTHENTICATED`.
    Unauthenticated,
    /// A valid credential this engine does not admit: `403`, gRPC
    /// `PERMISSION_DENIED`.
    Forbidden,
    /// The credential could not be checked now (the JWKS is unreachable):
    /// `503`, gRPC `UNAVAILABLE`.
    Unavailable,
}

/// A refused request. `reason` is a stable machine-readable code; `message`
/// says what to do. Neither ever contains the credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthError {
    pub kind: AuthErrorKind,
    pub reason: &'static str,
    pub message: String,
}

impl AuthError {
    pub(crate) fn unauthenticated(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: AuthErrorKind::Unauthenticated,
            reason,
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: AuthErrorKind::Forbidden,
            reason,
            message: message.into(),
        }
    }

    pub(crate) fn unavailable(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: AuthErrorKind::Unavailable,
            reason,
            message: message.into(),
        }
    }

    /// The HTTP status of the refusal.
    pub fn status(&self) -> StatusCode {
        match self.kind {
            AuthErrorKind::Unauthenticated => StatusCode::UNAUTHORIZED,
            AuthErrorKind::Forbidden => StatusCode::FORBIDDEN,
            AuthErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// The reasons a refusal carries (`loams-auth-reason`, the body's `reason`).
pub mod reason {
    pub const TOKEN_MISSING: &str = "token_missing";
    pub const TOKEN_MALFORMED: &str = "token_malformed";
    pub const TOKEN_EXPIRED: &str = "token_expired";
    pub const TOKEN_NOT_YET_VALID: &str = "token_not_yet_valid";
    pub const TOKEN_BAD_SIGNATURE: &str = "token_bad_signature";
    pub const TOKEN_WRONG_AUDIENCE: &str = "token_wrong_audience";
    pub const TOKEN_WRONG_ISSUER: &str = "token_wrong_issuer";
    pub const TOKEN_UNKNOWN_KEY: &str = "token_unknown_key";
    pub const TOKEN_ALGORITHM: &str = "token_algorithm_not_allowed";
    pub const API_KEY_INVALID: &str = "api_key_invalid";
    pub const CREDENTIAL_NOT_ACCEPTED: &str = "credential_not_accepted";
    pub const CREDENTIAL_IN_QUERY_STRING: &str = "credential_in_query_string";
    pub const PRINCIPAL_NOT_ALLOWED: &str = "principal_not_allowed";
    pub const AUTH_UNAVAILABLE: &str = "auth_unavailable";
    pub const CORS_ORIGIN_NOT_ALLOWED: &str = "cors_origin_not_allowed";
}

/// A credential as the request carried it.
#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    /// An `lk_…` API key (from a bearer, a Basic password, an `ApiKey` or an
    /// `api-key` header).
    ApiKey(String),
    /// A bearer that is not an API key: a JWT.
    Jwt(String),
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey(_) => f.write_str("ApiKey([redacted])"),
            Self::Jwt(_) => f.write_str("Jwt([redacted])"),
        }
    }
}

/// Turns a credential into a principal. One implementation per credential
/// kind; MT1 adds its own without touching handlers (API2 Task 6).
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// `None`: this authenticator does not take this kind of credential.
    async fn authenticate(&self, credential: &Credential) -> Option<Result<Principal, AuthError>>;
}

/// Which credentials the engine takes (`LOAMS_AUTH_MODE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication; loopback only.
    Off,
    /// API keys, OIDC JWTs, or both.
    On { api_keys: bool, oidc: bool },
}

impl AuthMode {
    /// Parses `off`, `apikey`, `oidc`, or a comma list of the last two.
    pub fn parse(value: &str) -> Result<Self, String> {
        let value = value.trim();
        if value.is_empty() || value.eq_ignore_ascii_case("off") {
            return Ok(Self::Off);
        }
        let (mut api_keys, mut oidc) = (false, false);
        for part in value.split(',').map(str::trim) {
            match part.to_ascii_lowercase().as_str() {
                "apikey" | "api-key" | "api_key" => api_keys = true,
                "oidc" | "jwt" => oidc = true,
                "off" => return Err(format!("{MODE_ENV}={value:?}: off cannot be combined")),
                other => {
                    return Err(format!(
                        "{MODE_ENV}={value:?}: unknown mode {other:?}; expected off, apikey, \
                         oidc or oidc,apikey"
                    ));
                }
            }
        }
        Ok(Self::On { api_keys, oidc })
    }
}

/// The authentication settings ([`AuthConfig::from_env`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthConfig {
    pub mode: AuthMode,
    /// `LOAMS_API_KEYS_FILE`: `<id> <argon2id PHC> [label]` per line.
    pub api_keys_file: Option<PathBuf>,
    pub oidc: Option<oidc::OidcConfig>,
    /// `LOAMS_CORS_ORIGINS`: the browser origins (the console's) allowed to
    /// call the engine cross-origin. Empty: no CORS headers at all.
    pub cors_origins: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::off()
    }
}

impl AuthConfig {
    /// No authentication (loopback only).
    pub fn off() -> Self {
        Self {
            mode: AuthMode::Off,
            api_keys_file: None,
            oidc: None,
            cors_origins: Vec::new(),
        }
    }

    /// API keys from `file` only.
    pub fn api_keys(file: impl Into<PathBuf>) -> Self {
        Self {
            mode: AuthMode::On {
                api_keys: true,
                oidc: false,
            },
            api_keys_file: Some(file.into()),
            ..Self::off()
        }
    }

    /// Whether requests are authenticated at all.
    pub fn enforced(&self) -> bool {
        self.mode != AuthMode::Off
    }

    /// Reads `LOAMS_AUTH_MODE`, `LOAMS_API_KEYS_FILE`, `LOAMS_OIDC_*` and
    /// `LOAMS_CORS_ORIGINS` through `var`. Errors name the variable.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mode = AuthMode::parse(&var(MODE_ENV).unwrap_or_default())?;
        let list = |name: &str| -> Vec<String> {
            var(name)
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        };
        let mut config = Self {
            mode: mode.clone(),
            api_keys_file: var("LOAMS_API_KEYS_FILE")
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from),
            oidc: None,
            cors_origins: list("LOAMS_CORS_ORIGINS")
                .into_iter()
                .map(|o| o.trim_end_matches('/').to_owned())
                .collect(),
        };
        if let AuthMode::On { api_keys, oidc } = mode {
            if api_keys && config.api_keys_file.is_none() {
                return Err(format!(
                    "{MODE_ENV} includes apikey but LOAMS_API_KEYS_FILE is not set; create one \
                     with `loams auth new-key`"
                ));
            }
            if oidc {
                let required = |name: &str| {
                    var(name)
                        .map(|v| v.trim().to_owned())
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| format!("{MODE_ENV} includes oidc but {name} is not set"))
                };
                config.oidc = Some(oidc::OidcConfig {
                    issuer: required("LOAMS_OIDC_ISSUER")?,
                    audience: required("LOAMS_OIDC_AUDIENCE")?,
                    jwks_url: var("LOAMS_OIDC_JWKS_URL").filter(|v| !v.trim().is_empty()),
                    allowed_subjects: list("LOAMS_OIDC_ALLOWED_SUBJECTS").into_iter().collect(),
                    allowed_emails: list("LOAMS_OIDC_ALLOWED_EMAILS")
                        .into_iter()
                        .map(|e| e.to_ascii_lowercase())
                        .collect(),
                    allowed_groups: list("LOAMS_OIDC_ALLOWED_GROUPS").into_iter().collect(),
                });
            }
        }
        Ok(config)
    }

    /// [`AuthConfig::from_vars`] on the process environment.
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Refuses `off` unless every listener in `listeners` is loopback. The
    /// error names the first one that is not.
    pub fn check_listeners(&self, listeners: &[(&str, SocketAddr)]) -> Result<(), String> {
        if self.enforced() {
            return Ok(());
        }
        match listeners.iter().find(|(_, addr)| !addr.ip().is_loopback()) {
            Some((name, addr)) => Err(format!(
                "{name} {addr} is not a loopback address and {MODE_ENV} is off; bind it to \
                 127.0.0.1, or set {MODE_ENV}=apikey (with LOAMS_API_KEYS_FILE) or oidc \
                 (with LOAMS_OIDC_ISSUER and LOAMS_OIDC_AUDIENCE)"
            )),
            None => Ok(()),
        }
    }
}

/// The engine's authenticators, built from an [`AuthConfig`].
pub struct Auth {
    authenticators: Vec<Arc<dyn Authenticator>>,
    enforced: bool,
    cors: Cors,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("authenticators", &self.authenticators.len())
            .field("enforced", &self.enforced)
            .finish_non_exhaustive()
    }
}

impl Auth {
    /// Builds the authenticators: reads and checks the API key file and the
    /// OIDC settings (the JWKS is fetched on first use).
    pub fn from_config(config: &AuthConfig) -> Result<Arc<Self>, String> {
        let mut authenticators: Vec<Arc<dyn Authenticator>> = Vec::new();
        if let AuthMode::On { api_keys, oidc } = &config.mode {
            if *api_keys {
                let file = config
                    .api_keys_file
                    .as_ref()
                    .ok_or("apikey mode needs LOAMS_API_KEYS_FILE")?;
                authenticators.push(Arc::new(api_key::ApiKeyAuthenticator::from_file(file)?));
            }
            if *oidc {
                let oidc = config.oidc.as_ref().ok_or("oidc mode needs LOAMS_OIDC_*")?;
                authenticators.push(Arc::new(oidc::OidcAuthenticator::new(oidc.clone())?));
            }
        }
        Ok(Arc::new(Self {
            authenticators,
            enforced: config.enforced(),
            cors: Cors::new(config.cors_origins.iter().cloned().collect::<BTreeSet<_>>()),
        }))
    }

    /// The given authenticators (tests, and MT1's own).
    pub fn with_authenticators(
        authenticators: Vec<Arc<dyn Authenticator>>,
        cors_origins: BTreeSet<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            authenticators,
            enforced: true,
            cors: Cors::new(cors_origins),
        })
    }

    /// No authentication: every request is [`PrincipalKind::DevLoopback`].
    pub fn off() -> Arc<Self> {
        Arc::new(Self {
            authenticators: Vec::new(),
            enforced: false,
            cors: Cors::new(BTreeSet::new()),
        })
    }

    pub fn enforced(&self) -> bool {
        self.enforced
    }

    pub(crate) fn cors(&self) -> &Cors {
        &self.cors
    }

    /// The principal for `credential`: the first authenticator that takes
    /// it decides.
    pub async fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthError> {
        for authenticator in &self.authenticators {
            if let Some(result) = authenticator.authenticate(credential).await {
                return result;
            }
        }
        let what = match credential {
            Credential::ApiKey(_) => "API keys are not enabled on this engine (LOAMS_AUTH_MODE)",
            Credential::Jwt(_) => "OIDC tokens are not enabled on this engine (LOAMS_AUTH_MODE)",
        };
        Err(AuthError::unauthenticated(
            reason::CREDENTIAL_NOT_ACCEPTED,
            what,
        ))
    }

    /// The principal of an unauthenticated engine.
    pub(crate) fn dev_loopback() -> Principal {
        Principal {
            subject: "dev-loopback".to_owned(),
            kind: PrincipalKind::DevLoopback,
            display_name: "Local developer".to_owned(),
            email: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn mode_parses() {
        assert_eq!(AuthMode::parse("").unwrap(), AuthMode::Off);
        assert_eq!(AuthMode::parse("off").unwrap(), AuthMode::Off);
        assert_eq!(
            AuthMode::parse("oidc, apikey").unwrap(),
            AuthMode::On {
                api_keys: true,
                oidc: true
            }
        );
        assert!(AuthMode::parse("off,apikey").is_err());
        assert!(AuthMode::parse("basic").is_err());
    }

    #[test]
    fn apikey_mode_needs_a_file() {
        let err = AuthConfig::from_vars(vars(&[(MODE_ENV, "apikey")])).unwrap_err();
        assert!(err.contains("LOAMS_API_KEYS_FILE"), "{err}");
    }

    #[test]
    fn oidc_mode_needs_issuer_and_audience() {
        let err = AuthConfig::from_vars(vars(&[(MODE_ENV, "oidc")])).unwrap_err();
        assert!(err.contains("LOAMS_OIDC_ISSUER"), "{err}");
        let err = AuthConfig::from_vars(vars(&[
            (MODE_ENV, "oidc"),
            (
                "LOAMS_OIDC_ISSUER",
                "https://auth.loams.dev/application/o/loams/",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("LOAMS_OIDC_AUDIENCE"), "{err}");
    }

    #[test]
    fn off_mode_refuses_non_loopback_listeners() {
        let config = AuthConfig::off();
        let lo: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert!(config.check_listeners(&[("--listen", lo)]).is_ok());
        for bad in ["0.0.0.0:8080", "[::]:8080", "100.64.0.7:8080"] {
            let err = config
                .check_listeners(&[
                    ("--listen", lo),
                    ("--flight-sql-listen", bad.parse().unwrap()),
                ])
                .unwrap_err();
            assert!(err.contains("--flight-sql-listen"), "{err}");
            assert!(err.contains("loopback"), "{err}");
            assert!(err.contains(MODE_ENV), "{err}");
        }
        let on = AuthConfig::api_keys("/dev/null");
        assert!(
            on.check_listeners(&[("--listen", "0.0.0.0:8080".parse().unwrap())])
                .is_ok()
        );
    }
}
