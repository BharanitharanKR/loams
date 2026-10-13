//! Secrets (CN1 Task 3's `Secret`, moved here by OR-5) and the scrubber that
//! keeps them out of positions, errors, DLQ records and status.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::error::ConnectorError;

/// A secret value, zeroed on drop. `Debug` prints `<redacted>`, and there is
/// no `Display`.
#[derive(Clone)]
pub struct Secret(Zeroizing<Vec<u8>>);

impl Secret {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    /// The secret's bytes, for the one call that needs them (a login, a
    /// signature). Never format them.
    pub fn expose(&self) -> &[u8] {
        &self.0
    }

    /// The secret as UTF-8, when it is.
    pub fn expose_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A reference to a secret by name (an instance never holds the value).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    pub name: String,
}

impl SecretRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// Resolves an instance's secrets inside the runtime, at start (CN1 Task 3;
/// `loams-flow`'s `SecretStore` implements it per namespace; Task 25 adds the
/// production stores).
#[async_trait]
pub trait SecretResolver: Send + Sync {
    async fn resolve(&self, r: &SecretRef) -> Result<Secret, ConnectorError>;
}

const REDACTED: &str = "<redacted>";

/// The secret values an instance has resolved, so the pipeline can scrub
/// them from anything it stores or reports: `Position::display`, error
/// messages, DLQ records and status. Secrets shorter than four bytes are not
/// scrubbed (they would redact ordinary text and are not credentials).
#[derive(Debug, Default)]
pub struct Scrubber {
    values: Mutex<Vec<Secret>>,
}

impl Scrubber {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn add(&self, s: &Secret) {
        if s.expose().len() >= 4 {
            self.values
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(s.clone());
        }
    }

    /// `text` with every known secret replaced by `<redacted>`.
    pub fn scrub(&self, text: &str) -> String {
        let values = self.values.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = text.to_owned();
        for v in values.iter() {
            if let Some(v) = v.expose_str()
                && out.contains(v)
            {
                out = out.replace(v, REDACTED);
            }
        }
        out
    }
}

/// A resolver that records every secret it hands out in a [`Scrubber`].
/// `ConnectorContext::secrets` is wrapped in one by the runtime.
pub struct ScrubbingResolver {
    inner: Arc<dyn SecretResolver>,
    scrubber: Arc<Scrubber>,
}

impl ScrubbingResolver {
    pub fn new(inner: Arc<dyn SecretResolver>, scrubber: Arc<Scrubber>) -> Self {
        Self { inner, scrubber }
    }
}

impl fmt::Debug for ScrubbingResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScrubbingResolver")
    }
}

#[async_trait]
impl SecretResolver for ScrubbingResolver {
    async fn resolve(&self, r: &SecretRef) -> Result<Secret, ConnectorError> {
        let s = self.inner.resolve(r).await?;
        self.scrubber.add(&s);
        Ok(s)
    }
}

/// A resolver over a fixed map, for tests and for the reference connector.
#[derive(Default)]
pub struct StaticSecrets {
    values: std::collections::BTreeMap<String, Secret>,
}

impl StaticSecrets {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.values.insert(name.into(), Secret::new(value));
        self
    }
}

impl fmt::Debug for StaticSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.values.keys()).finish()
    }
}

#[async_trait]
impl SecretResolver for StaticSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<Secret, ConnectorError> {
        self.values
            .get(&r.name)
            .cloned()
            .ok_or_else(|| ConnectorError::auth(format!("secret {:?} is not defined", r.name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_is_redacted_and_scrubbed() {
        let s = Secret::new("hunter2-pass");
        assert_eq!(format!("{s:?}"), "<redacted>");
        let sc = Scrubber::new();
        sc.add(&s);
        assert_eq!(
            sc.scrub("login hunter2-pass failed"),
            "login <redacted> failed"
        );
    }
}
