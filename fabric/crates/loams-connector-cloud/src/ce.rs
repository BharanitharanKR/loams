//! CloudEvents bindings for the family's message systems (design §33 §6,
//! D355): the binary content mode as message attributes (`ce_` for the AWS
//! services, after the Kafka binding of D270; `ce-` for Pub/Sub, after its
//! CloudEvents binding) and the structured JSON mode, plus the time formats
//! the services report.

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::{CloudEvent, EnvelopeBuilder};
use serde_json::{Map, Value};

/// The structured content mode's media type.
pub const STRUCTURED: &str = "application/cloudevents+json";
/// The batched structured mode's media type (Event Grid).
pub const BATCH: &str = "application/cloudevents-batch+json";

/// Whether `ct` names JSON (`application/json`, `…+json`, `text/json`).
pub fn is_json_type(ct: &str) -> bool {
    let base = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    base == "application/json" || base == "text/json" || base.ends_with("+json")
}

/// A content type for a payload that came without one: JSON when it parses,
/// text when it is UTF-8, else bytes.
pub fn sniff_content_type(body: &[u8]) -> &'static str {
    if serde_json::from_slice::<Value>(body).is_ok() {
        "application/json"
    } else if std::str::from_utf8(body).is_ok() {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

/// A CloudEvents extension name: lower-case ASCII letters and digits, at
/// most 20 characters (CloudEvents 1.0 §2).
pub fn is_extension_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 20
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// The structured JSON form of `ev`: `data` as JSON when the content type is
/// JSON and it parses, as a string when it is UTF-8 text, else
/// `data_base64`.
pub fn to_structured(ev: &CloudEvent) -> Value {
    let mut m = Map::new();
    m.insert("specversion".into(), Value::String(ev.specversion.clone()));
    m.insert("id".into(), Value::String(ev.id.clone()));
    m.insert("source".into(), Value::String(ev.source.clone()));
    m.insert("type".into(), Value::String(ev.ty.clone()));
    if let Some(s) = &ev.subject {
        m.insert("subject".into(), Value::String(s.clone()));
    }
    if let Some(t) = &ev.time {
        m.insert("time".into(), Value::String(t.clone()));
    }
    if let Some(ct) = &ev.datacontenttype {
        m.insert("datacontenttype".into(), Value::String(ct.clone()));
    }
    for (k, v) in &ev.extensions {
        m.insert(k.clone(), Value::String(v.clone()));
    }
    if !ev.data.is_empty() || ev.datacontenttype.is_some() {
        let json_type = ev.datacontenttype.as_deref().is_none_or(is_json_type);
        match (
            json_type,
            serde_json::from_slice::<Value>(&ev.data),
            std::str::from_utf8(&ev.data),
        ) {
            (true, Ok(v), _) => {
                m.insert("data".into(), v);
            }
            (_, _, Ok(text)) => {
                m.insert("data".into(), Value::String(text.to_owned()));
            }
            _ => {
                m.insert(
                    "data_base64".into(),
                    Value::String(base64::engine::general_purpose::STANDARD.encode(&ev.data)),
                );
            }
        }
    }
    Value::Object(m)
}

/// [`to_structured`] as bytes.
pub fn to_structured_bytes(ev: &CloudEvent) -> Vec<u8> {
    serde_json::to_vec(&to_structured(ev)).unwrap_or_default()
}

/// The binary mode's attributes for `ev`, with `prefix` (`ce_` or `ce-`):
/// the context attributes, then each extension, then `content-type` for
/// `datacontenttype`. The payload travels as the message body.
pub fn binary_attributes(ev: &CloudEvent, prefix: &str) -> Vec<(String, String)> {
    let mut out = vec![
        (format!("{prefix}id"), ev.id.clone()),
        (format!("{prefix}source"), ev.source.clone()),
        (format!("{prefix}type"), ev.ty.clone()),
        (format!("{prefix}specversion"), ev.specversion.clone()),
    ];
    if let Some(s) = &ev.subject {
        out.push((format!("{prefix}subject"), s.clone()));
    }
    if let Some(t) = &ev.time {
        out.push((format!("{prefix}time"), t.clone()));
    }
    for (k, v) in &ev.extensions {
        out.push((format!("{prefix}{k}"), v.clone()));
    }
    if let Some(ct) = &ev.datacontenttype {
        out.push(("content-type".to_owned(), ct.clone()));
    }
    out
}

/// Whether attributes carry the binary mode (a `ce_id` or `ce-id`).
pub fn has_binary(attrs: &BTreeMap<String, String>) -> bool {
    attrs
        .keys()
        .any(|k| k.eq_ignore_ascii_case("ce_id") || k.eq_ignore_ascii_case("ce-id"))
}

/// Pass-through of a message that already is a CloudEvent: the binary mode
/// from its attributes, or the structured mode from its body (when the
/// attributes say so, or the body is a CloudEvents JSON object). `None` when
/// it is not one; the caller then wraps the message.
pub fn passthrough(
    env: &EnvelopeBuilder,
    attrs: &BTreeMap<String, String>,
    body: &Bytes,
) -> Option<CloudEvent> {
    if has_binary(attrs) {
        return env.from_binary(
            attrs.iter().map(|(k, v)| (k.as_str(), v.as_bytes())),
            body.clone(),
        );
    }
    let structured = attrs
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("content-type") && v.starts_with(STRUCTURED));
    if structured || looks_structured(body) {
        return env.from_structured_json(body);
    }
    None
}

/// A JSON object with `specversion`, `id`, `source` and `type`.
pub fn looks_structured(body: &[u8]) -> bool {
    let trimmed = body.iter().position(|b| !b.is_ascii_whitespace());
    if trimmed.is_none_or(|i| body[i] != b'{') {
        return false;
    }
    serde_json::from_slice::<Value>(body).is_ok_and(|v| {
        ["specversion", "id", "source", "type"]
            .iter()
            .all(|k| v.get(k).is_some_and(Value::is_string))
    })
}

/// The non-`ce` attributes that are valid extension names, for a wrapped
/// message's envelope (message attributes become extensions where they can).
pub fn attribute_extensions(attrs: &BTreeMap<String, String>) -> Vec<(String, String)> {
    attrs
        .iter()
        .filter(|(k, _)| is_extension_name(k))
        .filter(|(k, _)| !matches!(k.as_str(), "loamsconnector" | "loamsinstance"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// RFC 3339 UTC with milliseconds, from milliseconds since the epoch.
pub fn rfc3339_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian (Howard
/// Hinnant's `civil_from_days`).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// (year, month, day) → days since 1970-01-01 (Hinnant's `days_from_civil`).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Seconds since the epoch of an RFC 3339 timestamp
/// (`2026-10-13T00:00:00.5Z`, `…+02:00`), or `None`.
pub fn parse_rfc3339(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut rest = &s[19..];
    let mut frac = 0.0;
    if let Some(r) = rest.strip_prefix('.') {
        let digits: String = r.chars().take_while(char::is_ascii_digit).collect();
        frac = format!("0.{digits}").parse().ok()?;
        rest = &r[digits.len()..];
    }
    let offset = match rest {
        "Z" | "z" => 0,
        o if o.len() == 6 && (o.starts_with('+') || o.starts_with('-')) => {
            let oh: i64 = o.get(1..3)?.parse().ok()?;
            let om: i64 = o.get(4..6)?.parse().ok()?;
            let sign = if o.starts_with('-') { -1 } else { 1 };
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };
    let days = days_from_civil(y, mo as u32, d as u32);
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec - offset;
    Some(secs as f64 + frac)
}

/// Seconds since the epoch, now.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    fn ev() -> CloudEvent {
        CloudEvent::new("e-1", "/s", "t.v1")
            .with_subject("sub")
            .with_data("application/json", "{\"a\":1}")
            .with_extension("tenant", "acme")
    }

    #[test]
    fn structured_round_trip() {
        let env = EnvelopeBuilder::new("x", &InstanceKey::new(1, "i"));
        let back = env
            .from_structured_json(&to_structured_bytes(&ev()))
            .unwrap();
        assert_eq!(back.id, "e-1");
        assert_eq!(back.data.as_ref(), b"{\"a\":1}");
        assert_eq!(
            back.extensions.get("tenant").map(String::as_str),
            Some("acme")
        );
        let bin = CloudEvent::new("b", "/s", "t").with_data("application/octet-stream", vec![0xff]);
        let v = to_structured(&bin);
        assert_eq!(v["data_base64"], "/w==");
        let text = CloudEvent::new("b", "/s", "t").with_data("text/plain", "hi");
        assert_eq!(to_structured(&text)["data"], "hi");
    }

    #[test]
    fn binary_attributes_and_passthrough() {
        let env = EnvelopeBuilder::new("x", &InstanceKey::new(1, "i"));
        let attrs: BTreeMap<String, String> = binary_attributes(&ev(), "ce_").into_iter().collect();
        assert_eq!(attrs["ce_id"], "e-1");
        assert_eq!(attrs["ce_tenant"], "acme");
        assert_eq!(attrs["content-type"], "application/json");
        let back = passthrough(&env, &attrs, &Bytes::from_static(b"{\"a\":1}")).unwrap();
        assert_eq!((back.id.as_str(), back.ty.as_str()), ("e-1", "t.v1"));
        assert_eq!(back.subject.as_deref(), Some("sub"));
        assert!(passthrough(&env, &BTreeMap::new(), &Bytes::from_static(b"plain")).is_none());
        assert!(passthrough(&env, &BTreeMap::new(), &Bytes::from_static(b"{\"a\":1}")).is_none());
    }

    #[test]
    fn times_and_names() {
        assert_eq!(rfc3339_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339_millis(1_760_313_600_500),
            "2025-10-13T00:00:00.500Z"
        );
        assert_eq!(
            parse_rfc3339("2025-10-13T00:00:00.5Z"),
            Some(1_760_313_600.5)
        );
        assert_eq!(
            parse_rfc3339("2025-10-13T02:00:00+02:00"),
            Some(1_760_313_600.0)
        );
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(parse_rfc3339("yesterday"), None);
        assert!(is_extension_name("tenant1"));
        assert!(!is_extension_name("Tenant"));
        assert!(!is_extension_name("content-type"));
        assert_eq!(sniff_content_type(b"{}"), "application/json");
        assert_eq!(sniff_content_type(b"hi"), "text/plain; charset=utf-8");
        assert_eq!(sniff_content_type(&[0xff]), "application/octet-stream");
        assert!(is_json_type("application/cloudevents+json; charset=utf-8"));
    }
}
