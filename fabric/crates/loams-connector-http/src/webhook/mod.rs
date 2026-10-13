//! Webhook signature schemes (CN2 plan Task 20): how the webhooks source
//! checks that a delivery came from the API a profile describes. A profile's
//! `webhook: { scheme, header }` names one of [`SCHEMES`]; the webhooks
//! source (CN1 Task 4's, moved into this crate and routed by Task 8, R7.8)
//! calls [`verify`] with the raw request and the secret the instance names,
//! before anything is parsed.
//!
//! The first five names are CN1 Task 4's (its webhooks source's schemes);
//! the rest are added here.
//!
//! | Scheme | Systems | Signed content and encoding |
//! |---|---|---|
//! | `hmac-sha256` | any | HMAC-SHA256 of the body, hex, in the profile's header (an optional `sha256=` prefix) |
//! | `stripe` | Stripe | `Stripe-Signature: t=…,v1=…`; HMAC-SHA256 of `t.body`, hex; 5 min |
//! | `github` | GitHub, Jira | `sha256=` + HMAC-SHA256 of the body, hex |
//! | `slack` | Slack | `v0=` + HMAC-SHA256 of `v0:ts:body`, hex; `X-Slack-Request-Timestamp`, 5 min |
//! | `shopify` | Shopify | HMAC-SHA256 of the body, base64 |
//! | `hubspot-v3` | HubSpot | HMAC-SHA256 of method + URI + body + `X-HubSpot-Request-Timestamp`, base64; 5 min |
//! | `zendesk` | Zendesk | HMAC-SHA256 of `X-Zendesk-Webhook-Signature-Timestamp` + body, base64 |
//! | `twilio` | Twilio | HMAC-SHA1 of the URL + sorted form `keyvalue` pairs (or the URL alone with `bodySHA256` for JSON), base64 |
//! | `segment` | Segment | HMAC-SHA1 of the body, hex |
//! | `sendgrid` | SendGrid | ECDSA P-256/SHA-256 over `X-Twilio-Email-Event-Webhook-Timestamp` + body; the "secret" is the base64 DER public key |
//! | `token` | GitLab, Google push channels, Kestra | the header equals the shared token |
//!
//! Every comparison is constant-time. HMAC and ECDSA come from `ring` 0.17.14,
//! already in `fabric/Cargo.lock` through rustls (R20.9).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use http::HeaderMap;
use ring::{hmac, signature};
use sha2::{Digest, Sha256};

/// The scheme names a profile's `webhook.scheme` may use; the profile schema's
/// enum lists the same (a unit test keeps them equal).
pub const SCHEMES: &[&str] = &[
    "hmac-sha256",
    "stripe",
    "github",
    "slack",
    "shopify",
    "hubspot-v3",
    "zendesk",
    "twilio",
    "segment",
    "sendgrid",
    "token",
];

/// The replay window of the schemes that sign a timestamp (Stripe's, Slack's
/// and HubSpot's documented five minutes).
pub const TOLERANCE: Duration = Duration::from_secs(300);

/// A delivery as the webhooks source received it, before parsing.
#[derive(Debug, Clone, Copy)]
pub struct WebhookRequest<'a> {
    pub method: &'a str,
    /// The full URL the sender addressed (scheme, host, path and query), as
    /// configured on the sender's side: Twilio and HubSpot sign it.
    pub url: &'a str,
    pub headers: &'a HeaderMap,
    pub body: &'a [u8],
}

/// Why a delivery is refused. Never carries the secret or the expected value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WebhookError {
    #[error("unknown webhook scheme {0:?}")]
    UnknownScheme(String),
    #[error("the {0} header is missing or not text")]
    MissingHeader(String),
    #[error("the {0} header is malformed")]
    Malformed(String),
    #[error("the signature does not match")]
    Mismatch,
    #[error("the signed timestamp is outside the {}-second window", TOLERANCE.as_secs())]
    Stale,
    #[error("the verification key is not usable: {0}")]
    BadKey(&'static str),
}

/// Check a delivery against `scheme`. `header` is the profile's
/// `webhook.header` (the signature's header); `secret` the signing secret the
/// instance names (for `sendgrid`, the base64 DER public key).
pub fn verify(
    scheme: &str,
    header: &str,
    secret: &[u8],
    req: &WebhookRequest<'_>,
    now: SystemTime,
) -> Result<(), WebhookError> {
    let sig = || header_str(req.headers, header);
    match scheme {
        "stripe" => {
            let value = sig()?;
            let mut t = None;
            let mut v1 = Vec::new();
            for part in value.split(',') {
                match part.trim().split_once('=') {
                    Some(("t", ts)) => t = Some(ts),
                    Some(("v1", s)) => v1.push(s),
                    _ => {}
                }
            }
            let t = t.ok_or_else(|| WebhookError::Malformed(header.to_string()))?;
            fresh(t, now)?;
            let mut msg = t.as_bytes().to_vec();
            msg.push(b'.');
            msg.extend_from_slice(req.body);
            let expected = hex(hmac::sign(&key256(secret), &msg).as_ref());
            if v1.iter().any(|s| ct_eq(s.as_bytes(), expected.as_bytes())) {
                Ok(())
            } else {
                Err(WebhookError::Mismatch)
            }
        }
        "hmac-sha256" => {
            let value = sig()?;
            let given = value.strip_prefix("sha256=").unwrap_or(value);
            let expected = hex(hmac::sign(&key256(secret), req.body).as_ref());
            matches(given.as_bytes(), expected.as_bytes())
        }
        "github" => {
            let value = sig()?;
            let given = value
                .strip_prefix("sha256=")
                .ok_or_else(|| WebhookError::Malformed(header.to_string()))?;
            let expected = hex(hmac::sign(&key256(secret), req.body).as_ref());
            matches(given.as_bytes(), expected.as_bytes())
        }
        "slack" => {
            let value = sig()?;
            let ts = header_str(req.headers, "X-Slack-Request-Timestamp")?;
            fresh(ts, now)?;
            let mut msg = format!("v0:{ts}:").into_bytes();
            msg.extend_from_slice(req.body);
            let expected = format!("v0={}", hex(hmac::sign(&key256(secret), &msg).as_ref()));
            matches(value.as_bytes(), expected.as_bytes())
        }
        "shopify" => {
            let value = sig()?;
            let expected = b64(hmac::sign(&key256(secret), req.body).as_ref());
            matches(value.as_bytes(), expected.as_bytes())
        }
        "hubspot-v3" => {
            let value = sig()?;
            let ts = header_str(req.headers, "X-HubSpot-Request-Timestamp")?;
            // HubSpot's timestamp is in milliseconds.
            let ms: u64 = ts
                .parse()
                .map_err(|_| WebhookError::Malformed("X-HubSpot-Request-Timestamp".into()))?;
            fresh(&(ms / 1000).to_string(), now)?;
            let mut msg = req.method.to_ascii_uppercase().into_bytes();
            msg.extend_from_slice(hubspot_uri(req.url).as_bytes());
            msg.extend_from_slice(req.body);
            msg.extend_from_slice(ts.as_bytes());
            let expected = b64(hmac::sign(&key256(secret), &msg).as_ref());
            matches(value.as_bytes(), expected.as_bytes())
        }
        "zendesk" => {
            let value = sig()?;
            let ts = header_str(req.headers, "X-Zendesk-Webhook-Signature-Timestamp")?;
            let mut msg = ts.as_bytes().to_vec();
            msg.extend_from_slice(req.body);
            let expected = b64(hmac::sign(&key256(secret), &msg).as_ref());
            matches(value.as_bytes(), expected.as_bytes())
        }
        "twilio" => {
            let value = sig()?;
            let msg = twilio_payload(req)?;
            let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret);
            let expected = b64(hmac::sign(&key, msg.as_bytes()).as_ref());
            matches(value.as_bytes(), expected.as_bytes())
        }
        "segment" => {
            let value = sig()?;
            let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret);
            let expected = hex(hmac::sign(&key, req.body).as_ref());
            matches(value.as_bytes(), expected.as_bytes())
        }
        "sendgrid" => {
            let value = sig()?;
            let ts = header_str(req.headers, "X-Twilio-Email-Event-Webhook-Timestamp")?;
            let der = base64::engine::general_purpose::STANDARD
                .decode(value.trim())
                .map_err(|_| WebhookError::Malformed(header.to_string()))?;
            let point = p256_point(secret)?;
            let mut msg = ts.as_bytes().to_vec();
            msg.extend_from_slice(req.body);
            signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_ASN1, point)
                .verify(&msg, &der)
                .map_err(|_| WebhookError::Mismatch)
        }
        "token" => {
            let value = sig()?;
            matches(value.as_bytes(), secret)
        }
        other => Err(WebhookError::UnknownScheme(other.to_string())),
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, WebhookError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| WebhookError::MissingHeader(name.to_string()))
}

fn key256(secret: &[u8]) -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, secret)
}

/// A timestamp in seconds since the epoch, within [`TOLERANCE`] of `now`.
fn fresh(ts: &str, now: SystemTime) -> Result<(), WebhookError> {
    let t: u64 = ts
        .trim()
        .parse()
        .map_err(|_| WebhookError::Malformed("timestamp".into()))?;
    let now = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    if now.abs_diff(t) > TOLERANCE.as_secs() {
        return Err(WebhookError::Stale);
    }
    Ok(())
}

fn matches(given: &[u8], expected: &[u8]) -> Result<(), WebhookError> {
    if ct_eq(given, expected) {
        Ok(())
    } else {
        Err(WebhookError::Mismatch)
    }
}

/// Constant-time equality (length is not secret).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// HubSpot decodes these percent-escapes in the URI before signing (its
/// "validating requests" guide); the `?` that starts the query stays.
fn hubspot_uri(url: &str) -> String {
    const DECODE: &[(&str, &str)] = &[
        ("%3A", ":"),
        ("%2F", "/"),
        ("%3F", "?"),
        ("%40", "@"),
        ("%21", "!"),
        ("%24", "$"),
        ("%27", "'"),
        ("%28", "("),
        ("%29", ")"),
        ("%2A", "*"),
        ("%2C", ","),
        ("%3B", ";"),
    ];
    let mut out = url.to_string();
    for (from, to) in DECODE {
        out = out
            .replace(from, to)
            .replace(&from.to_ascii_lowercase(), to);
    }
    out
}

/// What Twilio signs: for a form POST, the URL followed by every parameter's
/// name and value, sorted by name; for a JSON body, the URL alone, whose
/// `bodySHA256` parameter must be the body's SHA-256 (hex).
fn twilio_payload(req: &WebhookRequest<'_>) -> Result<String, WebhookError> {
    let is_form = req
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"));
    if !is_form || req.body.is_empty() {
        if let Some(query) = req.url.split_once('?').map(|(_, q)| q) {
            let given = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("bodySHA256="));
            if let Some(given) = given {
                let actual = hex(&Sha256::digest(req.body));
                if !ct_eq(given.as_bytes(), actual.as_bytes()) {
                    return Err(WebhookError::Mismatch);
                }
            }
        }
        return Ok(req.url.to_string());
    }
    let body =
        std::str::from_utf8(req.body).map_err(|_| WebhookError::Malformed("form body".into()))?;
    let mut pairs: Vec<(String, String)> = body
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (form_decode(k), form_decode(v))
        })
        .collect();
    pairs.sort();
    let mut out = req.url.to_string();
    for (k, v) in pairs {
        out.push_str(&k);
        out.push_str(&v);
    }
    Ok(out)
}

/// `application/x-www-form-urlencoded` decoding (`+` is a space).
fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The uncompressed P-256 point of a base64 DER `SubjectPublicKeyInfo`
/// (SendGrid's verification key): its last 65 bytes, starting `0x04`.
fn p256_point(spki_b64: &[u8]) -> Result<Vec<u8>, WebhookError> {
    let text = std::str::from_utf8(spki_b64).map_err(|_| WebhookError::BadKey("not text"))?;
    let der = base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .map_err(|_| WebhookError::BadKey("not base64"))?;
    // id-ecPublicKey (1.2.840.10045.2.1) with prime256v1 (1.2.840.10045.3.1.7).
    const P256_OIDS: &[u8] = &[
        0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce,
        0x3d, 0x03, 0x01, 0x07,
    ];
    if der.len() < 65 || !der.windows(P256_OIDS.len()).any(|w| w == P256_OIDS) {
        return Err(WebhookError::BadKey("not a P-256 public key"));
    }
    let point = der[der.len() - 65..].to_vec();
    if point[0] != 0x04 {
        return Err(WebhookError::BadKey("not an uncompressed point"));
    }
    Ok(point)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemes_match_the_profile_schema() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../connectors/schema/profile.schema.json"
        );
        let schema: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("schema")).expect("json");
        let listed: Vec<&str> =
            schema["$defs"]["http"]["properties"]["webhook"]["properties"]["scheme"]["enum"]
                .as_array()
                .expect("scheme enum")
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect();
        assert_eq!(listed, SCHEMES);
    }

    #[test]
    fn form_decoding() {
        assert_eq!(form_decode("a%2Bb+c%3D"), "a+b c=");
        assert_eq!(form_decode("%"), "%");
        assert_eq!(form_decode("%4"), "%4");
    }

    #[test]
    fn hubspot_uri_decodes_the_listed_escapes_only() {
        assert_eq!(
            hubspot_uri("https://x.test/hook%3Fa%3Ab%20c?q=1"),
            "https://x.test/hook?a:b%20c?q=1"
        );
    }
}
