//! Reading an instance's config. Credentials are never in the config: its
//! `auth.*` keys name secrets, which the instance's `SecretResolver` resolves
//! (once, when first needed), so the SDK's scrubber records every value
//! (CN2 R7.5's convention).

use std::fmt;
use std::sync::Arc;

use loams_connector_sdk::{ConnectorError, Secret, SecretRef, SecretResolver};
use serde_json::Value;
use tokio::sync::OnceCell;

fn bad(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::fatal(msg)
}

/// A string at `key`, if present and non-empty.
pub fn str_opt(c: &Value, key: &str) -> Option<String> {
    c.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// A required string at `key`.
pub fn str_req(c: &Value, key: &str) -> Result<String, ConnectorError> {
    str_opt(c, key).ok_or_else(|| bad(format!("config `{key}` is required")))
}

/// An unsigned integer at `key` (a number or a numeric string), or `default`.
pub fn u64_or(c: &Value, key: &str, default: u64) -> Result<u64, ConnectorError> {
    match c.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| bad(format!("config `{key}` must be a non-negative integer"))),
        Some(Value::String(s)) => s
            .parse()
            .map_err(|_| bad(format!("config `{key}` must be a non-negative integer"))),
        Some(_) => Err(bad(format!(
            "config `{key}` must be a non-negative integer"
        ))),
    }
}

/// [`u64_or`], checked against an inclusive range.
pub fn u64_in(
    c: &Value,
    key: &str,
    default: u64,
    range: std::ops::RangeInclusive<u64>,
) -> Result<u64, ConnectorError> {
    let v = u64_or(c, key, default)?;
    if range.contains(&v) {
        Ok(v)
    } else {
        Err(bad(format!(
            "config `{key}` must be between {} and {}",
            range.start(),
            range.end()
        )))
    }
}

/// The secret name at `auth.<key>`, if any.
pub fn auth_ref(c: &Value, key: &str) -> Option<String> {
    c.get("auth").and_then(|a| str_opt(a, key))
}

/// A non-secret value at `auth.<key>` (a tenant id, an authority URL).
pub fn auth_str(c: &Value, key: &str) -> Option<String> {
    auth_ref(c, key)
}

/// A secret named in the config, resolved the first time it is needed and
/// kept for the instance's life.
#[derive(Clone)]
pub struct LazySecret {
    resolver: Arc<dyn SecretResolver>,
    name: String,
    cell: Arc<OnceCell<Secret>>,
}

impl fmt::Debug for LazySecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazySecret")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl LazySecret {
    pub fn new(resolver: Arc<dyn SecretResolver>, name: impl Into<String>) -> Self {
        Self {
            resolver,
            name: name.into(),
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// The secret named at `auth.<key>`, if the config names one.
    pub fn from_auth(c: &Value, key: &str, resolver: &Arc<dyn SecretResolver>) -> Option<Self> {
        auth_ref(c, key).map(|name| Self::new(resolver.clone(), name))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn get(&self) -> Result<Secret, ConnectorError> {
        self.cell
            .get_or_try_init(|| async { self.resolver.resolve(&SecretRef::new(&self.name)).await })
            .await
            .cloned()
    }

    /// The secret as UTF-8 text (keys, tokens and passwords are).
    pub async fn text(&self) -> Result<SecretText, ConnectorError> {
        let s = self.get().await?;
        if s.expose_str().is_none() {
            return Err(ConnectorError::auth(format!(
                "secret `{}` is not UTF-8 text",
                self.name
            )));
        }
        Ok(SecretText(s))
    }
}

/// A secret known to be UTF-8.
#[derive(Clone, Debug)]
pub struct SecretText(Secret);

impl SecretText {
    pub fn expose(&self) -> &str {
        self.0.expose_str().unwrap_or_default()
    }

    pub fn secret(&self) -> &Secret {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::StaticSecrets;
    use serde_json::json;

    #[test]
    fn readers() {
        let c = json!({"a": "x", "n": 5, "s": "7", "e": "", "auth": {"key": "k"}});
        assert_eq!(str_opt(&c, "a").as_deref(), Some("x"));
        assert_eq!(str_opt(&c, "e"), None);
        assert!(str_req(&c, "zz").is_err());
        assert_eq!(u64_or(&c, "n", 1).ok(), Some(5));
        assert_eq!(u64_or(&c, "s", 1).ok(), Some(7));
        assert_eq!(u64_or(&c, "missing", 9).ok(), Some(9));
        assert!(u64_or(&c, "a", 1).is_err());
        assert!(u64_in(&c, "n", 1, 0..=4).is_err());
        assert_eq!(auth_ref(&c, "key").as_deref(), Some("k"));
    }

    #[tokio::test]
    async fn lazy_secret_resolves_once() {
        let r: Arc<dyn SecretResolver> = Arc::new(StaticSecrets::new().with("k", "value"));
        let s = LazySecret::new(r, "k");
        assert_eq!(
            s.text()
                .await
                .ok()
                .map(|t| t.expose().to_owned())
                .as_deref(),
            Some("value")
        );
        let missing = LazySecret::new(Arc::new(StaticSecrets::new()), "nope");
        assert!(missing.get().await.is_err());
        assert!(!format!("{s:?}").contains("value"));
    }
}
