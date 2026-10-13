//! Event data as JSON, JSON pointers, and the prompt and body templates the
//! AI and function sinks fill from an event (ruling R21.10).
//!
//! A template is text with `{{ … }}` placeholders:
//! * `{{/a/b}}` — the value at that JSON pointer in the event's data (a
//!   string as is, anything else as compact JSON; a missing value is an
//!   error, so a typo cannot send an empty prompt);
//! * `{{data}}` — the whole data (a string as is, else compact JSON);
//! * `{{subject}}`, `{{id}}`, `{{type}}`, `{{source}}` — the envelope.

use loams_connector_sdk::{CloudEvent, ConnectorError};
use serde_json::Value;

/// The event's data as JSON: parsed when it is JSON, else a JSON string of
/// the UTF-8 text. Empty data is `null`.
pub fn data_json(ev: &CloudEvent) -> Result<Value, ConnectorError> {
    if ev.data.is_empty() {
        return Ok(Value::Null);
    }
    let json_ct = ev
        .datacontenttype
        .as_deref()
        .is_none_or(|ct| ct.contains("json"));
    if json_ct && let Ok(v) = serde_json::from_slice::<Value>(&ev.data) {
        return Ok(v);
    }
    std::str::from_utf8(&ev.data)
        .map(|s| Value::String(s.to_owned()))
        .map_err(|_| {
            ConnectorError::poison(format!("event {}: data is neither JSON nor text", ev.id))
        })
}

/// A field name or a JSON pointer, as a pointer (`a` is `/a`).
pub fn pointer(p: &str) -> String {
    if p.is_empty() || p.starts_with('/') {
        p.to_owned()
    } else {
        format!("/{p}")
    }
}

/// A JSON value as template text: a string as is, else compact JSON.
pub fn as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Fill `template` from `ev` (see the module documentation).
pub fn render(template: &str, ev: &CloudEvent, data: &Value) -> Result<String, ConnectorError> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            return Err(ConnectorError::fatal("a template has an unclosed `{{`"));
        };
        let key = after[..end].trim();
        let text = match key {
            "data" => as_text(data),
            "subject" => ev.subject.clone().unwrap_or_default(),
            "id" => ev.id.clone(),
            "type" => ev.ty.clone(),
            "source" => ev.source.clone(),
            p if p.starts_with('/') => match data.pointer(p) {
                Some(v) => as_text(v),
                None => {
                    return Err(ConnectorError::poison(format!(
                        "event {}: no value at {p} for the template",
                        ev.id
                    )));
                }
            },
            other => {
                return Err(ConnectorError::fatal(format!(
                    "unknown template placeholder {{{{{other}}}}}"
                )));
            }
        };
        out.push_str(&text);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Check a template's placeholders without an event (config validation).
pub fn check(template: &str) -> Result<(), ConnectorError> {
    let ev = CloudEvent::new("check", "/check", "check").with_subject("s");
    match render(template, &ev, &Value::Null) {
        Ok(_) => Ok(()),
        // A pointer has nothing to point into here; that is not a config
        // error.
        Err(e) if e.message.contains("for the template") => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn templates_fill_from_the_event() {
        let ev = CloudEvent::new("e1", "/s", "t")
            .with_subject("order-7")
            .with_data("application/json", r#"{"text":"hello","n":3}"#);
        let data = data_json(&ev).expect("json");
        assert_eq!(
            render("Summarise {{ /text }} ({{/n}}) for {{subject}}", &ev, &data).expect("ok"),
            "Summarise hello (3) for order-7"
        );
        assert_eq!(
            render("{{data}}", &ev, &data).expect("ok"),
            r#"{"n":3,"text":"hello"}"#
        );
        assert!(render("{{/missing}}", &ev, &data).is_err());
        assert!(render("{{nope}}", &ev, &data).is_err());
        assert!(render("{{/text", &ev, &data).is_err());
        assert!(check("ok {{/x}} {{subject}}").is_ok());
        assert!(check("{{bogus}}").is_err());
    }

    #[test]
    fn data_as_json_or_text() {
        let text = CloudEvent::new("e", "/s", "t").with_data("text/plain", "plain words");
        assert_eq!(data_json(&text).expect("text"), json!("plain words"));
        assert_eq!(pointer("a"), "/a");
        assert_eq!(pointer("/a/b"), "/a/b");
    }
}
