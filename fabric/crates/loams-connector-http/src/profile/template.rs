//! Placeholders in a profile (CN2 Task 20, R20.2):
//!
//! * `{config.<key>}` — a value of the instance config (after `defaults`),
//!   filled in once when the instance starts: in `base_url`, `token_url`,
//!   `region`, paths (percent-encoded, `/` included), query values and JSON
//!   bodies (as is). A missing value stops the instance with its name.
//! * `{auth.<field>}` — the secret the instance config names under
//!   `auth.<field>`, resolved at send time through the instance's
//!   `SecretResolver` (so the scrubber knows it). Only in a path or an
//!   `envelope`, never in an item: an item is data, and data never pulls a
//!   secret into a request.
//! * `{item:<pointer>}` — in a write's path, a value of the item being
//!   written (percent-encoded, `/` kept).

use loams_connector_sdk::ConnectorError;
use serde_json::Value;

use super::sigv4::uri_encode;

const CONFIG: &str = "{config.";
const AUTH: &str = "{auth.";
const ITEM: &str = "{item:";

/// Replace every `{<prefix>…}` in `s` with `fill(name)`.
fn replace(
    s: &str,
    prefix: &str,
    mut fill: impl FnMut(&str) -> Result<String, ConnectorError>,
) -> Result<String, ConnectorError> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find(prefix) {
        out.push_str(&rest[..start]);
        let after = &rest[start + prefix.len()..];
        let end = after.find('}').ok_or_else(|| {
            ConnectorError::fatal(format!("an unclosed {prefix}…}} placeholder in {s:?}"))
        })?;
        out.push_str(&fill(&after[..end])?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// `s` with its `{config.<key>}` placeholders filled from `config`; in a
/// path the value is percent-encoded.
pub(crate) fn config(s: &str, config: &Value, path: bool) -> Result<String, ConnectorError> {
    replace(s, CONFIG, |key| {
        let v = config.get(key).and_then(scalar).ok_or_else(|| {
            ConnectorError::fatal(format!(
                "config.{key} is required by the profile (it fills {{config.{key}}})"
            ))
        })?;
        Ok(if path { uri_encode(&v, true) } else { v })
    })
}

/// Every string of `v` with its `{config.<key>}` placeholders filled; the
/// values of `path` keys are percent-encoded.
pub(crate) fn render(v: &Value, cfg: &Value) -> Result<Value, ConnectorError> {
    fn walk(v: &Value, cfg: &Value, path: bool) -> Result<Value, ConnectorError> {
        Ok(match v {
            Value::String(s) => Value::String(config(s, cfg, path)?),
            Value::Array(a) => Value::Array(
                a.iter()
                    .map(|x| walk(x, cfg, false))
                    .collect::<Result<_, _>>()?,
            ),
            Value::Object(m) => {
                let mut out = serde_json::Map::new();
                for (k, x) in m {
                    out.insert(k.clone(), walk(x, cfg, k == "path")?);
                }
                Value::Object(out)
            }
            other => other.clone(),
        })
    }
    walk(v, cfg, false)
}

/// The `{auth.<field>}` fields `s` names.
pub(crate) fn auth_fields(s: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let _ = replace(s, AUTH, |f| {
        fields.push(f.to_string());
        Ok(String::new())
    });
    fields
}

/// `s` with each `{auth.<field>}` replaced by `values(field)`.
pub(crate) fn auth(
    s: &str,
    mut values: impl FnMut(&str) -> Option<String>,
    path: bool,
) -> Result<String, ConnectorError> {
    replace(s, AUTH, |f| {
        let v = values(f).ok_or_else(|| {
            ConnectorError::auth(format!("auth.{f} names no secret in the instance config"))
        })?;
        Ok(if path { uri_encode(&v, true) } else { v })
    })
}

/// Whether a write's path takes values from its item.
pub(crate) fn has_item(s: &str) -> bool {
    s.contains(ITEM)
}

/// `s` with each `{item:<pointer>}` replaced by the item's value there.
pub(crate) fn item(s: &str, item: &Value) -> Result<String, ConnectorError> {
    replace(s, ITEM, |ptr| {
        let v = item.pointer(ptr).and_then(scalar).ok_or_else(|| {
            ConnectorError::poison(format!("the item has no value at {ptr} for the path"))
        })?;
        Ok(uri_encode(&v, false))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_values_fill_paths_encoded_and_bodies_as_is() {
        let cfg = json!({"project": "group/app", "n": 3, "soql": "SELECT Id FROM Account"});
        let v = render(
            &json!({"path": "/projects/{config.project}/issues", "body": {"query": "{config.soql}"},
                    "query": {"limit": "{config.n}"}}),
            &cfg,
        )
        .expect("renders");
        assert_eq!(v["path"], "/projects/group%2Fapp/issues");
        assert_eq!(v["body"]["query"], "SELECT Id FROM Account");
        assert_eq!(v["query"]["limit"], "3");
        let err = config("/{config.missing}", &cfg, true).unwrap_err();
        assert!(err.message.contains("config.missing"), "{}", err.message);
    }

    #[test]
    fn auth_and_item_placeholders() {
        let p = "/hook/{config.ns}/{auth.webhook_key}";
        assert_eq!(auth_fields(p), vec!["webhook_key".to_string()]);
        let filled = auth(p, |_| Some("k/1".into()), true).expect("fills");
        assert_eq!(filled, "/hook/{config.ns}/k%2F1");
        assert!(auth("/{auth.nope}", |_| None, true).is_err());
        let path = item("/v1/secret/data/{item:/path}", &json!({"path": "app/db"})).expect("item");
        assert_eq!(path, "/v1/secret/data/app/db");
        assert!(has_item("/{item:/path}"));
        assert!(item("/{item:/nope}", &json!({})).is_err());
    }
}
