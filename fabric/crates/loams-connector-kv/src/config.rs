//! Reading an instance's config: typed getters that refuse a wrong type with
//! a `Fatal` error naming the key (never its value), and secrets resolved by
//! the name the config gives (secrets by reference only, Review Focus 8).

use std::time::Duration;

use loams_connector_sdk::{ConnectorContext, ConnectorError, Secret, SecretRef};
use serde_json::Value;

/// An instance config, with the profile's `defaults` merged under it.
#[derive(Debug, Clone)]
pub struct Config {
    value: Value,
}

impl Config {
    /// The context's config, with its profile's defaults under it.
    pub fn of(ctx: &ConnectorContext) -> Self {
        let value = match &ctx.profile {
            Some(p) => p.config_with_defaults(&ctx.config),
            None => ctx.config.clone(),
        };
        Self { value }
    }

    pub fn new(value: Value) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The value at a dotted path (`tls.ca_pem`).
    pub fn get(&self, path: &str) -> Option<&Value> {
        let mut v = &self.value;
        for part in path.split('.') {
            v = v.get(part)?;
        }
        if v.is_null() { None } else { Some(v) }
    }

    pub fn str(&self, path: &str) -> Result<Option<String>, ConnectorError> {
        match self.get(path) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(bad(path, "a string")),
        }
    }

    pub fn required_str(&self, path: &str) -> Result<String, ConnectorError> {
        self.str(path)?
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ConnectorError::fatal(format!("config: `{path}` is required")))
    }

    pub fn u64(&self, path: &str) -> Result<Option<u64>, ConnectorError> {
        match self.get(path) {
            None => Ok(None),
            Some(v) => v
                .as_u64()
                .map(Some)
                .ok_or_else(|| bad(path, "a whole number")),
        }
    }

    pub fn bool(&self, path: &str) -> Result<Option<bool>, ConnectorError> {
        match self.get(path) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(_) => Err(bad(path, "true or false")),
        }
    }

    pub fn millis(&self, path: &str) -> Result<Option<Duration>, ConnectorError> {
        Ok(self.u64(path)?.map(Duration::from_millis))
    }

    /// A string or a list of strings.
    pub fn strings(&self, path: &str) -> Result<Vec<String>, ConnectorError> {
        match self.get(path) {
            None => Ok(Vec::new()),
            Some(Value::String(s)) => Ok(vec![s.clone()]),
            Some(Value::Array(items)) => items
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| bad(path, "a list of strings"))
                })
                .collect(),
            Some(_) => Err(bad(path, "a string or a list of strings")),
        }
    }
}

fn bad(path: &str, what: &str) -> ConnectorError {
    ConnectorError::fatal(format!("config: `{path}` must be {what}"))
}

/// Resolve the secret whose *name* the config gives at `path`, if any.
pub async fn secret(
    ctx: &ConnectorContext,
    cfg: &Config,
    path: &str,
) -> Result<Option<Secret>, ConnectorError> {
    match cfg.str(path)? {
        Some(name) if !name.is_empty() => {
            ctx.secrets.resolve(&SecretRef::new(name)).await.map(Some)
        }
        _ => Ok(None),
    }
}

/// A secret's text, or an `Auth` error naming the config key.
pub fn secret_text(s: &Secret, path: &str) -> Result<String, ConnectorError> {
    s.expose_str()
        .map(str::to_owned)
        .ok_or_else(|| ConnectorError::auth(format!("the secret named by `{path}` is not UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getters_refuse_wrong_types_without_echoing_values() {
        let c = Config::new(serde_json::json!({
            "url": "redis://h:1", "count": "many", "tls": { "verify": true }, "streams": ["a", "b"]
        }));
        assert_eq!(c.required_str("url").expect("url"), "redis://h:1");
        let e = c.u64("count").expect_err("not a number");
        assert!(e.message.contains("`count`") && !e.message.contains("many"));
        assert_eq!(c.bool("tls.verify").expect("bool"), Some(true));
        assert_eq!(c.strings("streams").expect("list"), vec!["a", "b"]);
        assert!(c.required_str("missing").is_err());
    }
}
