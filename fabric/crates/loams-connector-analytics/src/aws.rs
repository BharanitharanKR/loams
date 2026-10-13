//! AWS Signature Version 4 for the Redshift Data API (R17.4): the canonical
//! request, the string to sign, the derived signing key and the
//! `Authorization` header, as AWS's "Signature Version 4 signing process"
//! documents them. HMAC-SHA256 is RFC 2104 over `sha2`.
//!
//! The same algorithm as `loams-connector-http`'s `profile::sigv4` (R7.9):
//! OR-6 keeps family crates from depending on each other, so the signer is
//! repeated here, with the same published test values, until it moves into
//! the SDK (a follow-up named in R17.4).

use std::time::{SystemTime, UNIX_EPOCH};

use http::Uri;
use loams_connector_sdk::Secret;
use sha2::{Digest, Sha256};

/// An access key: the id, the secret and an optional session token. The
/// secrets are [`Secret`]s, so `Debug` never prints them.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: Secret,
    pub session_token: Option<Secret>,
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

/// The `Authorization` header for a request. `headers` are the headers the
/// request carries (at least `X-Amz-Date`; `Host` is taken from `uri` when
/// absent). The signed headers are `host`, `content-type` and every
/// `x-amz-*` header.
#[allow(clippy::too_many_arguments)]
pub fn authorization(
    method: &str,
    uri: &Uri,
    headers: &[(String, String)],
    payload: &[u8],
    creds: &Credentials,
    region: &str,
    service: &str,
    amz_date: &str,
) -> String {
    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| {
            (
                k.to_ascii_lowercase(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
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
    let path = match uri.path() {
        "" => "/".to_owned(),
        p => p.split('/').map(uri_encode).collect::<Vec<_>>().join("/"),
    };
    let canonical_request = format!(
        "{method}\n{path}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        uri.query().unwrap_or(""),
        hex(&Sha256::digest(payload)),
    );
    let date = amz_date.get(..8).unwrap_or(amz_date);
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let key = signing_key(creds.secret_access_key.expose(), date, region, service);
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key_id
    )
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

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
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

fn host_of(uri: &Uri) -> String {
    let host = uri.host().unwrap_or_default();
    match (uri.scheme_str(), uri.port_u16()) {
        (Some("https"), Some(443)) | (Some("http"), Some(80)) | (_, None) => host.to_string(),
        (_, Some(port)) => format!("{host}:{port}"),
    }
}

/// AWS's `UriEncode` of a path segment: everything but `A-Za-z0-9-_.~` as
/// `%XX`.
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// AWS's "Examples of how to derive a signing key for Signature Version
    /// 4" (secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`, 20120215,
    /// us-east-1, iam).
    #[test]
    fn signing_key_matches_aws_example() {
        let k = signing_key(
            b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex(&k),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    /// AWS's signature test suite, `get-vanilla` (example credentials,
    /// 20150830T123600Z, us-east-1, service `service`).
    #[test]
    fn get_vanilla_matches_aws_suite() {
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: Secret::new("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),
            session_token: None,
        };
        let uri: Uri = "https://example.amazonaws.com/".parse().expect("uri");
        let auth = authorization(
            "GET",
            &uri,
            &[("X-Amz-Date".to_owned(), "20150830T123600Z".to_owned())],
            b"",
            &creds,
            "us-east-1",
            "service",
            "20150830T123600Z",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        assert_eq!(amz_date(UNIX_EPOCH), "19700101T000000Z");
    }
}
