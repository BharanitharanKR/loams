//! Untrusted fallback content (CN2 Task 25's threat model, "malicious profile
//! or overlay content"). A Camel overlay (`connect/routes/overlays/<id>.yaml`,
//! Task 22) or a user-edited route runs inside a fallback pod, where an
//! endpoint or an expression language can execute commands, read the pod's
//! files and environment (its mounted secrets), or load arbitrary classes.
//! [`check_overlay`] refuses all of them before anything is rendered:
//! - endpoint URIs with the schemes of [`REFUSED_SCHEMES`] (`exec:`, `file:`,
//!   `bean:`, `class:`, `language:`, …), anywhere in the document;
//! - bean and class references (`#bean:`, `#class:`, `#type:`,
//!   `#valueAs(`) in any string, such as an endpoint option;
//! - property functions that read the pod (`{{env:`, `{{sys:`, `{{file:`);
//! - a `beans:` entry with `type:` (a class to instantiate);
//! - scripting expression languages ([`REFUSED_LANGUAGES`]) used as keys.
//!
//! Task 22's `templates_use_only_allowed_languages` adds the positive list
//! (`simple`, `constant`, `header`, `jq`, `jsonpath`, `jsonata`) on top.

use serde_norway::Value;

/// Endpoint schemes no overlay may use.
pub const REFUSED_SCHEMES: &[&str] = &[
    "exec", "file", "bean", "class", "language", "jndi", "ssh", "scp", "docker", "stub", "ref",
];

/// Substrings no overlay string may contain.
pub const REFUSED_FRAGMENTS: &[&str] = &[
    "#bean:",
    "#class:",
    "#type:",
    "#valueas(",
    "{{env:",
    "{{sys:",
    "{{file:",
    "{{bean:",
];

/// Expression languages that execute code.
pub const REFUSED_LANGUAGES: &[&str] = &[
    "groovy",
    "java",
    "joor",
    "js",
    "javascript",
    "mvel",
    "ognl",
    "spel",
    "python",
    "kotlin",
    "script",
];

/// One refused construct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayViolation {
    /// Where, as a dotted path (`[n]` for sequence items).
    pub path: String,
    pub reason: String,
}

impl std::fmt::Display for OverlayViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.reason)
    }
}

/// Every refused construct in an overlay's YAML; `Err` lists them all.
pub fn check_overlay(yaml: &str) -> Result<(), Vec<OverlayViolation>> {
    let doc: Value = serde_norway::from_str(yaml).map_err(|e| {
        vec![OverlayViolation {
            path: String::new(),
            reason: format!("not YAML: {e}"),
        }]
    })?;
    check_overlay_value(&doc)
}

/// [`check_overlay`] over a parsed document (an overlay, or a profile's
/// values).
pub fn check_overlay_value(doc: &Value) -> Result<(), Vec<OverlayViolation>> {
    let mut out = Vec::new();
    walk(doc, "", &mut out);
    if out.is_empty() { Ok(()) } else { Err(out) }
}

fn refused_scheme(s: &str) -> Option<&'static str> {
    let t = s.trim().to_ascii_lowercase();
    REFUSED_SCHEMES.iter().copied().find(|scheme| {
        t.strip_prefix(scheme)
            .is_some_and(|rest| rest.starts_with(':'))
    })
}

fn check_string(s: &str, path: &str, out: &mut Vec<OverlayViolation>) {
    if let Some(scheme) = refused_scheme(s) {
        out.push(OverlayViolation {
            path: path.to_owned(),
            reason: format!("the `{scheme}:` scheme is refused"),
        });
    }
    let lower = s.to_ascii_lowercase();
    for f in REFUSED_FRAGMENTS {
        if lower.contains(f) {
            out.push(OverlayViolation {
                path: path.to_owned(),
                reason: format!("`{f}` is refused"),
            });
        }
    }
}

fn walk(v: &Value, path: &str, out: &mut Vec<OverlayViolation>) {
    match v {
        Value::String(s) => check_string(s, path, out),
        Value::Sequence(items) => {
            for (i, x) in items.iter().enumerate() {
                walk(x, &format!("{path}[{i}]"), out);
            }
        }
        Value::Mapping(map) => {
            for (k, x) in map {
                let key = match k {
                    Value::String(s) => s.clone(),
                    other => serde_norway::to_string(other)
                        .unwrap_or_default()
                        .trim()
                        .to_owned(),
                };
                let p = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                check_string(&key, &p, out);
                let lower = key.to_ascii_lowercase();
                if REFUSED_LANGUAGES.contains(&lower.as_str()) {
                    out.push(OverlayViolation {
                        path: p.clone(),
                        reason: format!("the `{key}` expression language executes code"),
                    });
                }
                if lower == "beans"
                    && let Value::Sequence(beans) = x
                {
                    for (i, b) in beans.iter().enumerate() {
                        if b.get("type").is_some() || b.get("builderClass").is_some() {
                            out.push(OverlayViolation {
                                path: format!("{p}[{i}]"),
                                reason: "a bean with a `type` instantiates a class".into(),
                            });
                        }
                    }
                }
                walk(x, &p, out);
            }
        }
        Value::Tagged(t) => walk(&t.value, path, out),
        _ => {}
    }
}
