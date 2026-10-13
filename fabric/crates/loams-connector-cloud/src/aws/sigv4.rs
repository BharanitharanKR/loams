//! AWS Signature Version 4: the canonical request, the string to sign, the
//! derived signing key and the `Authorization` header, as AWS's "Signature
//! Version 4 signing process" documents them. HMAC-SHA256 is RFC 2104 over
//! `sha2`.
//!
//! This is the HTTP family's implementation (`loams_connector_http::profile::
//! sigv4`, CN2 R7.9), carried here because a family crate depends on the SDK
//! and its protocol crates only (OR-6); CN2 R19.2 records the copy and its
//! way out (moving both into the SDK).

use std::time::{SystemTime, UNIX_EPOCH};

use http::Uri;
use loams_connector_sdk::Secret;
use sha2::{Digest, Sha256};

use crate::ce::civil_from_days;

/// An access key: the id, the secret and an optional session token. The
/// secrets are [`Secret`]s, so `Debug` never prints them.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: Secret,
    pub session_token: Option<Secret>,
}

/// What a signature is scoped to.
#[derive(Debug, Clone, Copy)]
pub struct SigningParams<'a> {
    pub region: &'a str,
    pub service: &'a str,
    /// `YYYYMMDD'T'HHMMSS'Z'`, the request's `X-Amz-Date`.
    pub amz_date: &'a str,
}

/// A computed signature, with the intermediate strings for inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub canonical_request: String,
    pub string_to_sign: String,
    pub signed_headers: String,
    pub signature: String,
    /// The `Authorization` header's value.
    pub authorization: String,
}

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// The signing key for `date` (`YYYYMMDD`), `region` and `service`.
pub fn signing_key(secret: &[u8], date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut first = b"AWS4".to_vec();
    first.extend_from_slice(secret);
    let k = hmac_sha256(&first, date.as_bytes());
    let k = hmac_sha256(&k, region.as_bytes());
    let k = hmac_sha256(&k, service.as_bytes());
    hmac_sha256(&k, b"aws4_request")
}

/// Sign a request. `headers` are the headers the request will carry (at
/// least `X-Amz-Date`; `Host` is taken from `uri` when absent). The signed
/// headers are `host`, `content-type` when present, and every `x-amz-*`
/// header.
pub fn sign(
    method: &str,
    uri: &Uri,
    headers: &[(String, String)],
    payload: &[u8],
    creds: &Credentials,
    params: &SigningParams<'_>,
) -> Signature {
    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), canonical_value(v)))
        .filter(|(k, _)| k == "host" || k == "content-type" || k.starts_with("x-amz-"))
        .collect();
    if !signed.iter().any(|(k, _)| k == "host") {
        signed.push(("host".to_string(), host_of(uri)));
    }
    signed.sort();
    let signed_headers = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let canonical_request = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        canonical_uri(uri.path()),
        canonical_query(uri.query().unwrap_or("")),
        hex(&Sha256::digest(payload)),
    );
    let date = params.amz_date.get(..8).unwrap_or(params.amz_date);
    let scope = format!("{date}/{}/{}/aws4_request", params.region, params.service);
    let string_to_sign = format!(
        "{ALGORITHM}\n{}\n{scope}\n{}",
        params.amz_date,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let key = signing_key(
        creds.secret_access_key.expose(),
        date,
        params.region,
        params.service,
    );
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    );
    Signature {
        canonical_request,
        string_to_sign,
        signed_headers,
        signature,
        authorization,
    }
}

/// `X-Amz-Date` for `t`: `YYYYMMDD'T'HHMMSS'Z'` in UTC.
pub fn amz_date(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The `Host` a client sends for `uri`: the port only when it is not the
/// scheme's default.
pub fn host_of(uri: &Uri) -> String {
    let host = uri.host().unwrap_or_default();
    match (uri.scheme_str(), uri.port_u16()) {
        (Some("https"), Some(443)) | (Some("http"), Some(80)) | (_, None) => host.to_string(),
        (_, Some(port)) => format!("{host}:{port}"),
    }
}

/// Trimmed, with runs of spaces collapsed to one.
fn canonical_value(v: &str) -> String {
    v.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Each path segment URI-encoded (the request's path is already encoded
/// once, so this is the double encoding AWS asks of every service but S3).
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    path.split('/')
        .map(|segment| uri_encode(segment, true))
        .collect::<Vec<_>>()
        .join("/")
}

/// Query parameters decoded, re-encoded with AWS's rules and sorted.
fn canonical_query(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (
                uri_encode(&percent_decode(k), true),
                uri_encode(&percent_decode(v), true),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// AWS's `UriEncode`: everything but `A-Za-z0-9-_.~` as `%XX` (upper case);
/// `/` too when `slash`.
pub fn uri_encode(s: &str, slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (digit(bytes[i + 1]), digit(bytes[i + 2]))
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn dates_and_encoding() {
        assert_eq!(amz_date(UNIX_EPOCH), "19700101T000000Z");
        assert_eq!(
            amz_date(UNIX_EPOCH + Duration::from_secs(1_440_938_160)),
            "20150830T123600Z"
        );
        assert_eq!(uri_encode("a b/c~", true), "a%20b%2Fc~");
        assert_eq!(canonical_query("b=2&a=x%20y&a=1"), "a=1&a=x%20y&b=2");
    }

    /// AWS's published values: "derive a signing key" and the Signature
    /// Version 4 test suite's `get-vanilla`, `get-vanilla-query-order-key-case`
    /// and `post-vanilla` (credentials `AKIDEXAMPLE`, 2015-08-30T12:36:00Z,
    /// us-east-1), the same vectors the HTTP family checks.
    #[test]
    fn signs_like_reference_vectors() {
        const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        assert_eq!(
            hex(&signing_key(
                SECRET.as_bytes(),
                "20120215",
                "us-east-1",
                "iam"
            )),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: Secret::new(SECRET),
            session_token: None,
        };
        let date = "20150830T123600Z";
        let suite = |method: &str, url: &str| {
            sign(
                method,
                &url.parse().expect("a URL"),
                &[("X-Amz-Date".to_string(), date.to_string())],
                b"",
                &creds,
                &SigningParams {
                    region: "us-east-1",
                    service: "service",
                    amz_date: date,
                },
            )
            .signature
        };
        assert_eq!(
            suite("GET", "https://example.amazonaws.com/"),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        assert_eq!(
            suite(
                "GET",
                "https://example.amazonaws.com/?Param2=value2&Param1=value1"
            ),
            "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
        );
        assert_eq!(
            suite("POST", "https://example.amazonaws.com/"),
            "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
        );
    }
}
