//! ADLS Gen2: Azure Blob's flat API through `object_store` for listing,
//! reading and writing, and the DFS REST API for what only a hierarchical
//! namespace has: directories and an atomic rename (R0.21). `object_store`
//! renames by copy and delete, and Azurite has no hierarchical namespace, so
//! these calls are written here over the SDK's HTTP client (which connects
//! through the egress check) and run against a real HNS account in the
//! nightly job.
//!
//! Requests are signed with the account's Shared Key (HMAC-SHA256 over the
//! canonical string of the 2009-09-19+ scheme), or carry a SAS token or an
//! Entra ID bearer token.

use std::sync::Arc;
use std::time::SystemTime;

use base64::Engine as _;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use loams_connector_sdk::http::{HttpClient, classify_status};
use loams_connector_sdk::{
    ConnectorError, EgressCheck, ErrorClass, Secret, SecretRef, SecretResolver,
};
use tokio::sync::OnceCell;

use crate::config::AuthNames;

/// The DFS REST API version the requests name.
pub const DFS_VERSION: &str = "2023-11-03";

/// How DFS requests authenticate.
#[derive(Clone)]
pub enum DfsAuth {
    SharedKey { account: String, key: Secret },
    Sas(String),
    Bearer(Secret),
}

impl std::fmt::Debug for DfsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DfsAuth::SharedKey { .. } => "DfsAuth::SharedKey",
            DfsAuth::Sas(_) => "DfsAuth::Sas",
            DfsAuth::Bearer(_) => "DfsAuth::Bearer",
        })
    }
}

/// What a rename did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameOutcome {
    Renamed,
    /// The destination exists, and the rename was refused.
    DestinationExists,
    /// The source does not exist (an earlier attempt already renamed it).
    SourceMissing,
}

/// Credentials resolved on first use, from the instance's secret names.
struct LazyAuth {
    account: String,
    names: AuthNames,
    secrets: Arc<dyn SecretResolver>,
}

/// A client for one file system of an HNS account's DFS endpoint.
pub struct DfsClient {
    http: HttpClient,
    /// `https://<account>.dfs.core.windows.net` (no trailing `/`).
    endpoint: String,
    filesystem: String,
    auth: OnceCell<DfsAuth>,
    lazy: Option<LazyAuth>,
}

impl std::fmt::Debug for DfsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsClient")
            .field("endpoint", &self.endpoint)
            .field("filesystem", &self.filesystem)
            .field("auth", &self.auth.get())
            .finish_non_exhaustive()
    }
}

/// Percent-encode a path segment the way the DFS endpoint expects.
fn encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for b in path.trim_matches('/').bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl DfsClient {
    pub fn new(
        egress: Arc<dyn EgressCheck>,
        endpoint: impl Into<String>,
        filesystem: impl Into<String>,
        auth: DfsAuth,
    ) -> Self {
        Self {
            http: loams_connector_sdk::http::client(egress),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            filesystem: filesystem.into(),
            auth: OnceCell::from(auth),
            lazy: None,
        }
    }

    /// A client whose credentials are resolved from the instance's secret
    /// names on the first request: the account key (Shared Key), else a SAS
    /// token, else an Entra ID bearer token.
    pub fn lazy(
        egress: Arc<dyn EgressCheck>,
        endpoint: impl Into<String>,
        filesystem: impl Into<String>,
        account: impl Into<String>,
        names: AuthNames,
        secrets: Arc<dyn SecretResolver>,
    ) -> Self {
        Self {
            http: loams_connector_sdk::http::client(egress),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            filesystem: filesystem.into(),
            auth: OnceCell::new(),
            lazy: Some(LazyAuth {
                account: account.into(),
                names,
                secrets,
            }),
        }
    }

    async fn auth(&self) -> Result<&DfsAuth, ConnectorError> {
        self.auth
            .get_or_try_init(|| async {
                let l = self
                    .lazy
                    .as_ref()
                    .ok_or_else(|| ConnectorError::fatal("DFS client has no credentials"))?;
                let get = |n: &Option<String>| {
                    let n = n.clone();
                    async move {
                        match n {
                            Some(n) => l.secrets.resolve(&SecretRef::new(n)).await.map(Some),
                            None => Ok(None),
                        }
                    }
                };
                if let Some(key) = get(&l.names.access_key).await? {
                    return Ok(DfsAuth::SharedKey {
                        account: l.account.clone(),
                        key,
                    });
                }
                if let Some(sas) = get(&l.names.sas_token).await? {
                    let sas = sas
                        .expose_str()
                        .ok_or_else(|| ConnectorError::auth("SAS token is not UTF-8"))?
                        .trim()
                        .to_owned();
                    return Ok(DfsAuth::Sas(sas));
                }
                if let Some(t) = get(&l.names.bearer_token).await? {
                    return Ok(DfsAuth::Bearer(t));
                }
                Err(ConnectorError::auth(
                    "ADLS Gen2 DFS calls need auth.access_key, auth.sas_token or auth.bearer_token",
                ))
            })
            .await
    }

    /// The default endpoint of an account.
    pub fn default_endpoint(account: &str) -> String {
        format!("https://{account}.dfs.core.windows.net")
    }

    fn url(&self, auth: &DfsAuth, path: &str, query: &[(&str, &str)]) -> String {
        let mut url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            encode_path(path)
        );
        let mut q: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if let DfsAuth::Sas(sas) = auth {
            q.push(sas.trim_start_matches('?').to_owned());
        }
        if !q.is_empty() {
            url.push('?');
            url.push_str(&q.join("&"));
        }
        url
    }

    /// Sign and send. `headers` are `x-ms-*` and conditional headers.
    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        headers: &[(&str, String)],
    ) -> Result<http::Response<Bytes>, ConnectorError> {
        let auth = self.auth().await?;
        let url = self.url(auth, path, query);
        let date = httpdate::fmt_http_date(SystemTime::now());
        let mut b = Request::builder()
            .method(method.clone())
            .uri(&url)
            .header("x-ms-date", &date)
            .header("x-ms-version", DFS_VERSION)
            .header(http::header::CONTENT_LENGTH, "0");
        for (k, v) in headers {
            b = b.header(*k, v);
        }
        match auth {
            DfsAuth::SharedKey { account, key } => {
                let mut all: Vec<(String, String)> = vec![
                    ("x-ms-date".to_owned(), date.clone()),
                    ("x-ms-version".to_owned(), DFS_VERSION.to_owned()),
                ];
                all.extend(
                    headers
                        .iter()
                        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone())),
                );
                let sts = string_to_sign(
                    method.as_str(),
                    account,
                    &format!("/{}/{}", self.filesystem, encode_path(path)),
                    query,
                    &all,
                );
                let sig = sign(key.expose(), &sts)?;
                let mut v = http::HeaderValue::from_str(&format!("SharedKey {account}:{sig}"))
                    .map_err(|_| ConnectorError::fatal("bad Authorization header"))?;
                v.set_sensitive(true);
                b = b.header(http::header::AUTHORIZATION, v);
            }
            DfsAuth::Bearer(t) => {
                let t = t
                    .expose_str()
                    .ok_or_else(|| ConnectorError::auth("bearer token is not UTF-8"))?;
                let mut v = http::HeaderValue::from_str(&format!("Bearer {t}"))
                    .map_err(|_| ConnectorError::auth("bearer token is not a header value"))?;
                v.set_sensitive(true);
                b = b.header(http::header::AUTHORIZATION, v);
            }
            DfsAuth::Sas(_) => {}
        }
        let req = b
            .body(Bytes::new())
            .map_err(|e| ConnectorError::fatal(format!("DFS request: {e}")))?;
        self.http.send(req).await
    }

    /// Create a directory (and its parents). An existing one is not an error.
    pub async fn create_directory(&self, path: &str) -> Result<(), ConnectorError> {
        let resp = self
            .send(Method::PUT, path, &[("resource", "directory")], &[])
            .await?;
        match resp.status() {
            s if s.is_success() => Ok(()),
            StatusCode::CONFLICT => Ok(()),
            s => Err(status_error(s, &resp, "create directory")),
        }
    }

    /// Atomically rename `from` to `to` (both relative to the file system),
    /// refusing an existing destination (`If-None-Match: *`).
    pub async fn rename(&self, from: &str, to: &str) -> Result<RenameOutcome, ConnectorError> {
        if let Some((parent, _)) = to.trim_matches('/').rsplit_once('/') {
            self.create_directory(parent).await?;
        }
        let source = format!("/{}/{}", self.filesystem, encode_path(from));
        let resp = self
            .send(
                Method::PUT,
                to,
                &[("mode", "legacy")],
                &[
                    ("x-ms-rename-source", source),
                    ("If-None-Match", "*".to_owned()),
                ],
            )
            .await?;
        let code = resp
            .headers()
            .get("x-ms-error-code")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        match resp.status() {
            s if s.is_success() => Ok(RenameOutcome::Renamed),
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
                if code != "SourcePathNotFound" =>
            {
                Ok(RenameOutcome::DestinationExists)
            }
            StatusCode::NOT_FOUND => Ok(RenameOutcome::SourceMissing),
            _ if code == "SourcePathNotFound" => Ok(RenameOutcome::SourceMissing),
            s => Err(status_error(s, &resp, "rename")),
        }
    }
}

fn status_error(s: StatusCode, resp: &http::Response<Bytes>, what: &str) -> ConnectorError {
    let code = resp
        .headers()
        .get("x-ms-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let retry_after = resp
        .headers()
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok());
    let class = classify_status(s, retry_after).unwrap_or(ErrorClass::Fatal);
    ConnectorError::new(class, format!("DFS {what}: HTTP {} {code}", s.as_u16()))
}

/// The Shared Key string to sign (Blob and DFS, version 2009-09-19 and
/// later). `headers` are lower-case names; `resource` is the URL path
/// (already percent-encoded), without the account.
pub fn string_to_sign(
    method: &str,
    account: &str,
    resource: &str,
    query: &[(&str, &str)],
    headers: &[(String, String)],
) -> String {
    let h = |name: &str| -> String {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let content_length = h("content-length");
    let content_length = if content_length == "0" {
        String::new()
    } else {
        content_length
    };
    let mut ms: Vec<(String, String)> = headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-ms-"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    ms.sort();
    let mut s = String::new();
    for part in [
        method.to_owned(),
        h("content-encoding"),
        h("content-language"),
        content_length,
        h("content-md5"),
        h("content-type"),
        String::new(), // Date: x-ms-date is used instead
        h("if-modified-since"),
        h("if-match"),
        h("if-none-match"),
        h("if-unmodified-since"),
        h("range"),
    ] {
        s.push_str(&part);
        s.push('\n');
    }
    for (k, v) in ms {
        s.push_str(&format!("{k}:{v}\n"));
    }
    s.push_str(&format!("/{account}{resource}"));
    let mut q: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), crate::store::pct_decode(v)))
        .collect();
    q.sort();
    for (k, v) in q {
        s.push_str(&format!("\n{k}:{v}"));
    }
    s
}

/// base64(HMAC-SHA256(base64-decoded key, string to sign)).
pub fn sign(key_b64: &[u8], string_to_sign: &str) -> Result<String, ConnectorError> {
    let key = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|_| ConnectorError::auth("the account key is not base64"))?;
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &key);
    let tag = ring::hmac::sign(&k, string_to_sign.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(tag.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_to_sign_follows_the_shared_key_scheme() {
        let headers = vec![
            ("x-ms-version".to_owned(), DFS_VERSION.to_owned()),
            (
                "x-ms-date".to_owned(),
                "Mon, 12 Oct 2026 10:00:00 GMT".to_owned(),
            ),
            ("x-ms-rename-source".to_owned(), "/fs/a/b.tmp".to_owned()),
            ("if-none-match".to_owned(), "*".to_owned()),
            ("content-length".to_owned(), "0".to_owned()),
        ];
        let s = string_to_sign(
            "PUT",
            "acct",
            "/fs/a/c.parquet",
            &[("mode", "legacy")],
            &headers,
        );
        assert_eq!(
            s,
            "PUT\n\n\n\n\n\n\n\n\n*\n\n\n\
             x-ms-date:Mon, 12 Oct 2026 10:00:00 GMT\n\
             x-ms-rename-source:/fs/a/b.tmp\n\
             x-ms-version:2023-11-03\n\
             /acct/fs/a/c.parquet\nmode:legacy"
        );
    }

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?".
        let key = base64::engine::general_purpose::STANDARD.encode("Jefe");
        let sig = sign(key.as_bytes(), "what do ya want for nothing?").expect("signs");
        let raw = base64::engine::general_purpose::STANDARD
            .decode(sig)
            .expect("base64");
        let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn paths_are_percent_encoded() {
        assert_eq!(encode_path("/a b/c%d/"), "a%20b/c%25d");
    }
}
