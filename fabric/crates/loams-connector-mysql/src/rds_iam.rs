//! Amazon RDS IAM authentication tokens (Aurora MySQL, `auth: iam`).
//!
//! A token is a SigV4 *presigned* URL for the `rds-db:connect` action,
//! without its `https://` prefix, sent as the password through
//! `mysql_clear_password` over TLS (the server's `AWSAuthenticationPlugin`
//! asks for it); it is valid for 15 minutes and is minted again for every
//! connection. HMAC-SHA256 is written over `sha2` (RFC 2104), as
//! `loams-connector-http` and `loams-connector-postgres` do (R7.9, R10.7):
//! a family crate cannot depend on another family crate (OR-6), so the
//! presigner is local, the same code as the Postgres family's.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::types::civil_from_days;

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.iter().map(|b| b ^ 0x36).collect::<Vec<_>>());
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.iter().map(|b| b ^ 0x5c).collect::<Vec<_>>());
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// RFC 3986 percent-encoding of a query component (SigV4's `UriEncode`).
fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `(YYYYMMDD, YYYYMMDDTHHMMSSZ)` of a Unix time.
pub fn amz_dates(unix: u64) -> (String, String) {
    let days = (unix / 86_400) as i64;
    let secs = unix % 86_400;
    let (y, m, d) = civil_from_days(days);
    let date = format!("{y:04}{m:02}{d:02}");
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    (date, stamp)
}

/// AWS credentials, resolved from the instance's secrets.
pub struct Credentials<'a> {
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub session_token: Option<&'a str>,
}

/// A SigV4 query-string presign of `method host path` with `params`, and
/// the signature (for tests). `payload_hash` is the hex SHA-256 of the body
/// or `UNSIGNED-PAYLOAD`.
#[allow(clippy::too_many_arguments)]
pub fn presign(
    method: &str,
    host: &str,
    path: &str,
    params: &[(&str, String)],
    region: &str,
    service: &str,
    creds: &Credentials<'_>,
    unix: u64,
    expires: u64,
    payload_hash: &str,
) -> (String, String) {
    let (date, stamp) = amz_dates(unix);
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let mut q: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    q.push(("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()));
    q.push((
        "X-Amz-Credential".into(),
        format!("{}/{scope}", creds.access_key_id),
    ));
    q.push(("X-Amz-Date".into(), stamp.clone()));
    q.push(("X-Amz-Expires".into(), expires.to_string()));
    if let Some(t) = creds.session_token {
        q.push(("X-Amz-Security-Token".into(), t.to_owned()));
    }
    q.push(("X-Amz-SignedHeaders".into(), "host".into()));
    let mut enc: Vec<(String, String)> = q
        .iter()
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    enc.sort();
    let query = enc
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!("{method}\n{path}\n{query}\nhost:{host}\n\nhost\n{payload_hash}");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, to_sign.as_bytes()));
    (
        format!("{host}{path}?{query}&X-Amz-Signature={signature}"),
        signature,
    )
}

/// An RDS IAM token for `user` at `host:port`, minted at `unix` (15 minutes).
/// The payload hash is that of the empty body, as AWS's own SDKs (Go, Java)
/// sign it.
pub fn rds_token(
    host: &str,
    port: u16,
    user: &str,
    region: &str,
    creds: &Credentials<'_>,
    unix: u64,
) -> String {
    let host_port = format!("{host}:{port}");
    presign(
        "GET",
        &host_port,
        "/",
        &[
            ("Action", "connect".to_owned()),
            ("DBUser", user.to_owned()),
        ],
        region,
        "rds-db",
        creds,
        unix,
        900,
        &sha256_hex(b""),
    )
    .0
}

/// Now, as Unix seconds.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl std::fmt::Debug for Credentials<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
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

    #[test]
    fn amz_dates_format() {
        // 2013-05-24T00:00:00Z, the S3 presign example's date.
        assert_eq!(
            amz_dates(1_369_353_600),
            ("20130524".to_owned(), "20130524T000000Z".to_owned())
        );
    }

    /// AWS's published query-string presign example ("Authenticating
    /// Requests: Using Query Parameters", Amazon S3 API reference): GET
    /// `/test.txt` from `examplebucket`, 86400 s, us-east-1.
    #[test]
    fn presign_matches_the_aws_s3_reference_example() {
        let creds = Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE",
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token: None,
        };
        let (_, sig) = presign(
            "GET",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            &[],
            "us-east-1",
            "s3",
            &creds,
            1_369_353_600,
            86_400,
            "UNSIGNED-PAYLOAD",
        );
        assert_eq!(
            sig,
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn rds_token_has_the_connect_action_and_no_scheme() {
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE",
            secret_access_key: "secret",
            session_token: Some("tok/en"),
        };
        let t = rds_token(
            "db.cluster.eu-west-1.rds.amazonaws.com",
            3306,
            "loams",
            "eu-west-1",
            &creds,
            1_700_000_000,
        );
        assert!(t.starts_with("db.cluster.eu-west-1.rds.amazonaws.com:3306/?Action=connect&DBUser=loams&X-Amz-Algorithm=AWS4-HMAC-SHA256"), "{t}");
        assert!(t.contains("X-Amz-Expires=900"));
        assert!(t.contains("X-Amz-Security-Token=tok%2Fen"));
        assert!(t.contains("%2Feu-west-1%2Frds-db%2Faws4_request"));
        assert!(!t.contains("secret"));
    }
}
