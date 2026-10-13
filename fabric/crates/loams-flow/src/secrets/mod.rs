//! Secret stores (CN1 Task 3's `SecretStore`, D189; CN2 Task 25, OR-21).
//!
//! A [`SecretRef`] names `<secret>/<key>`, or `<secret>` for the key
//! `value`. `<secret>` is a DNS-1123 subdomain (a Kubernetes `Secret` name)
//! and `<key>` a Kubernetes `Secret` data key, so the same reference works in
//! every store and as a mounted file path (R25.5):
//! - [`file::FileSecretStore`]: dev, `<root>/<namespace>/<secret>/<key>`,
//!   owner-only files (`deploy/fabric/secrets/`, CN1);
//! - [`kube::KubeSecretStore`]: production, the `Secret` `<secret>` in the
//!   instance's Kubernetes namespace, read through the API server.
//!
//! Native connectors read either through the SDK's `SecretResolver`
//! ([`NamespaceResolver`]); fallback pods get the same `Secret`s mounted
//! read-only ([`kube::secret_mounts`]), and the supervisor filters their logs
//! ([`logs::FallbackLogFilter`]).

pub mod file;
pub mod kube;
pub mod logs;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use loams_connector_sdk::{ConnectorError, Secret, SecretRef, SecretResolver};

/// Why a secret could not be read. Never carries a secret's value, a token,
/// or a response body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretError {
    #[error("secret reference {name:?} is not valid: {reason}")]
    InvalidName { name: String, reason: String },
    #[error("secret {name:?} is not defined in namespace {namespace}")]
    NotFound { namespace: u64, name: String },
    #[error("secret {name:?} in namespace {namespace} may not be read: {reason}")]
    Denied {
        namespace: u64,
        name: String,
        reason: String,
    },
    #[error("the secret store is unavailable: {0}")]
    Unavailable(String),
}

/// A namespace-scoped secret store (CN1 Task 3's contract).
#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn resolve(&self, ns: u64, r: &SecretRef) -> Result<Secret, SecretError>;
}

/// `<secret>/<key>` split and checked: `<secret>` a DNS-1123 subdomain of at
/// most 253 characters, `<key>` 1–253 characters of `[-._a-zA-Z0-9]`, not
/// `.` or `..`. A bare `<secret>` means the key `value`.
pub fn parse_ref(r: &SecretRef) -> Result<(String, String), SecretError> {
    let bad = |reason: &str| SecretError::InvalidName {
        name: r.name.clone(),
        reason: reason.to_owned(),
    };
    let (secret, key) = match r.name.split_once('/') {
        Some((s, k)) => (s, k),
        None => (r.name.as_str(), "value"),
    };
    let dns_subdomain = !secret.is_empty()
        && secret.len() <= 253
        && secret.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        });
    if !dns_subdomain {
        return Err(bad("the secret name must be a DNS-1123 subdomain"));
    }
    let key_ok = !key.is_empty()
        && key.len() <= 253
        && key != "."
        && key != ".."
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'));
    if !key_ok {
        return Err(bad("the key must be 1-253 characters of [-._a-zA-Z0-9]"));
    }
    Ok((secret.to_owned(), key.to_owned()))
}

/// A [`SecretStore`] seen from one namespace, as the SDK's
/// `SecretResolver`: what the runtime puts in `ConnectorContext::secrets`
/// (wrapped in a `ScrubbingResolver`). `NotFound`, `Denied` and
/// `InvalidName` are `Auth` errors; `Unavailable` is `Retryable`.
pub struct NamespaceResolver {
    store: Arc<dyn SecretStore>,
    namespace: u64,
}

impl NamespaceResolver {
    pub fn new(store: Arc<dyn SecretStore>, namespace: u64) -> Self {
        Self { store, namespace }
    }
}

impl fmt::Debug for NamespaceResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceResolver")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretResolver for NamespaceResolver {
    async fn resolve(&self, r: &SecretRef) -> Result<Secret, ConnectorError> {
        self.store
            .resolve(self.namespace, r)
            .await
            .map_err(|e| match e {
                SecretError::Unavailable(_) => ConnectorError::retryable(e.to_string()),
                _ => ConnectorError::auth(e.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refs_parse() {
        let p = |s: &str| parse_ref(&SecretRef::new(s));
        assert_eq!(
            p("pg-creds/password"),
            Ok(("pg-creds".into(), "password".into()))
        );
        assert_eq!(p("api-token"), Ok(("api-token".into(), "value".into())));
        assert_eq!(p("a.b/tls.key"), Ok(("a.b".into(), "tls.key".into())));
        for bad in [
            "", "/x", "x/", "x/..", "../x", "X/y", "a/b/c", "-a/b", "a..b/c",
        ] {
            assert!(p(bad).is_err(), "{bad:?}");
        }
    }
}
