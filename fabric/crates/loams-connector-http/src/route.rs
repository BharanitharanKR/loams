//! Import routes on native profile sources (CN2 plan Task 20): CN1 Task 15's
//! Slack → Zulip, GitHub → Forgejo and Jira → ItsPlane imports, which were
//! Camel route templates, are now a pair of instances joined by a Fabric
//! topic until FL3's route format exists (`connectors/routes/*.yaml`):
//! `{route.<name>}` in an instance's config is a deployment's parameter.
//!
//! The source instance runs an HTTP-API profile's `list` resource with a
//! [`Projection`] in its config (`projection`): each item is reduced to the
//! fields the target needs before it becomes an event (D356: an import never
//! forwards a payload whole). The event id is still the raw item's, so a
//! re-read of an unchanged item is the same event and `ingest` drops it, and
//! the sink's `Idempotency-Key` (the event id) is stable across re-reads.
//!
//! A projection maps each output field to one of:
//! * a JSON pointer (`/title`), or alternatives tried in order
//!   (`/thread_ts|/ts`); a `*` segment maps over an array
//!   (`/labels/*/name`). A missing value is `null`.
//! * a template string with `{/pointer}` holes (`"Imported from {/html_url}"`);
//!   a missing or `null` value is empty.
//! * anything else (a number, a boolean, an object, a string with neither):
//!   a constant.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Output field → how it is computed (see the module documentation).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Projection(pub BTreeMap<String, Value>);

impl Projection {
    pub fn apply(&self, item: &Value) -> Value {
        let mut out = serde_json::Map::new();
        for (field, spec) in &self.0 {
            out.insert(field.clone(), eval(spec, item));
        }
        Value::Object(out)
    }
}

fn eval(spec: &Value, item: &Value) -> Value {
    let Value::String(s) = spec else {
        return spec.clone();
    };
    if s.starts_with('/') {
        return s
            .split('|')
            .map(|alt| select(item, alt))
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null);
    }
    if s.contains("{/") {
        return Value::String(template(s, item));
    }
    spec.clone()
}

/// The value at `pointer`, a `*` segment mapping over an array.
fn select(v: &Value, pointer: &str) -> Value {
    match pointer.split_once("/*") {
        None => v.pointer(pointer).cloned().unwrap_or(Value::Null),
        Some((head, tail)) => match v.pointer(head) {
            Some(Value::Array(items)) => Value::Array(
                items
                    .iter()
                    .map(|x| {
                        if tail.is_empty() {
                            x.clone()
                        } else {
                            select(x, tail)
                        }
                    })
                    .filter(|x| !x.is_null())
                    .collect(),
            ),
            _ => Value::Array(Vec::new()),
        },
    }
}

fn template(s: &str, item: &Value) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{/") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        match select(item, &after[..end]) {
            Value::Null => {}
            Value::String(v) => out.push_str(&v),
            other => out.push_str(&other.to_string()),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    out
}

/// One half of a route: a connector instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteInstance {
    /// The manifest id (`github`, `forgejo`).
    pub connector: String,
    /// The instance config; `{route.<key>}` is filled from the route's
    /// parameters by [`ImportRoute::instance_config`].
    #[serde(default)]
    pub config: Value,
    /// The secrets the instance names (names only, never values).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<String>,
}

/// `connectors/routes/<id>.yaml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportRoute {
    pub api_version: String,
    pub kind: String,
    pub id: String,
    /// The route parameters a deployment sets (`owner`, `repo`, …).
    #[serde(default)]
    pub parameters: Vec<String>,
    /// The Fabric topic the instances meet on.
    pub topic: String,
    /// The source instances (one per upstream system: ItsPlane imports from
    /// GitHub and from Jira); each writes the topic.
    pub sources: Vec<RouteInstance>,
    /// The sink instance that reads the topic.
    pub sink: RouteInstance,
}

impl ImportRoute {
    /// `instance.config` with `{route.<name>}` replaced by `values[name]`;
    /// the error names a parameter that has no value.
    pub fn instance_config(
        &self,
        instance: &RouteInstance,
        values: &BTreeMap<String, String>,
    ) -> Result<Value, String> {
        fn walk(v: &Value, values: &BTreeMap<String, String>) -> Result<Value, String> {
            Ok(match v {
                Value::String(s) if s.contains("{route.") => {
                    let mut out = String::new();
                    let mut rest = s.as_str();
                    while let Some(start) = rest.find("{route.") {
                        out.push_str(&rest[..start]);
                        let after = &rest[start + "{route.".len()..];
                        let end = after
                            .find('}')
                            .ok_or_else(|| format!("unclosed in {s:?}"))?;
                        let name = &after[..end];
                        out.push_str(
                            values
                                .get(name)
                                .ok_or_else(|| format!("route parameter {name:?} has no value"))?,
                        );
                        rest = &after[end + 1..];
                    }
                    out.push_str(rest);
                    Value::String(out)
                }
                Value::Array(a) => Value::Array(
                    a.iter()
                        .map(|x| walk(x, values))
                        .collect::<Result<_, _>>()?,
                ),
                Value::Object(m) => {
                    let mut out = serde_json::Map::new();
                    for (k, x) in m {
                        out.insert(k.clone(), walk(x, values)?);
                    }
                    Value::Object(out)
                }
                other => other.clone(),
            })
        }
        walk(&instance.config, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projection_pointers_templates_and_constants() {
        let p: Projection = serde_json::from_value(json!({
            "title": "/title",
            "labels": "/labels/*/name",
            "topic": "/thread_ts|/ts",
            "body": "{/body}\n\nImported from {/html_url}",
            "type": "stream",
            "priority": 3
        }))
        .expect("projection");
        let out = p.apply(&json!({
            "title": "T", "labels": [{"name": "a"}, {"x": 1}, {"name": "b"}],
            "ts": "1.2", "body": null, "html_url": "https://x"
        }));
        assert_eq!(
            out,
            json!({"title": "T", "labels": ["a", "b"], "topic": "1.2",
                   "body": "\n\nImported from https://x", "type": "stream", "priority": 3})
        );
    }
}
