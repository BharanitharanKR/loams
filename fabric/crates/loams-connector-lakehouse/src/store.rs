//! Object storage for Delta tables: S3 and its targets (RustFS and MinIO in
//! CI, R2), a local directory, or memory (tests).
//!
//! The family builds the store itself and hands it to deltalake as the
//! table's storage backend, so deltalake never builds a client of its own.
//! Every connection the S3 client opens resolves through the instance's
//! [`EgressCheck`] ([`EgressConnector`]), the pattern of the objects family
//! (CN2 R13.3): `object_store` brings its own `reqwest` client, so the family
//! builds that client with a DNS resolver that drops every address the egress
//! policy refuses. An endpoint given as an IP literal is checked when the
//! store is built, because no DNS lookup happens for it.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use loams_connector_sdk::{ConnectorError, EgressCheck, SecretRef, SecretResolver};
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpConnector};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{BackoffConfig, ClientOptions, ObjectStore, RetryConfig};
use serde::Deserialize;
use url::Url;

/// Where a table's storage is and how to reach it (instance config `storage`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// An S3-compatible endpoint (RustFS, MinIO, R2); AWS when absent.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    /// Plain `http://` endpoints (a local RustFS); off by default.
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default)]
    pub virtual_hosted_style: bool,
    #[serde(default)]
    pub auth: StorageAuth,
}

/// Secret **names**, resolved through the instance's `SecretResolver`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageAuth {
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub session_token: Option<String>,
}

/// An `object_store::Error` as a connector error. The message never carries
/// a URL's query string (a presigned or SAS URL's credentials travel there).
pub fn map_err(e: &object_store::Error, what: &str) -> ConnectorError {
    use object_store::Error as E;
    let msg = format!("{what}: {}", redact_urls(&e.to_string()));
    match e {
        E::Unauthenticated { .. } | E::PermissionDenied { .. } => ConnectorError::auth(msg),
        E::NotSupported { .. } | E::NotImplemented { .. } | E::UnknownConfigurationKey { .. } => {
            ConnectorError::fatal(msg)
        }
        E::InvalidPath { .. } => ConnectorError::poison(msg),
        _ => ConnectorError::retryable(msg),
    }
}

/// Cut the query string off every URL in `text`.
pub fn redact_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("http") {
        let (head, tail) = rest.split_at(at);
        out.push_str(head);
        if !(tail.starts_with("http://") || tail.starts_with("https://")) {
            out.push_str("http");
            rest = &tail[4..];
            continue;
        }
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ')')
            .unwrap_or(tail.len());
        let url = &tail[..end];
        match url.find('?') {
            Some(q) => {
                out.push_str(&url[..q]);
                out.push_str("?<redacted>");
            }
            None => out.push_str(url),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

/// Builds `object_store`'s HTTP client with DNS answers filtered by the
/// instance's egress check (R13.3).
#[derive(Clone)]
pub struct EgressConnector {
    egress: Arc<dyn EgressCheck>,
    allow_http: bool,
}

impl std::fmt::Debug for EgressConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressConnector")
            .field("allow_http", &self.allow_http)
            .finish_non_exhaustive()
    }
}

impl EgressConnector {
    pub fn new(egress: Arc<dyn EgressCheck>, allow_http: bool) -> Self {
        Self { egress, allow_http }
    }
}

impl HttpConnector for EgressConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(EgressResolver {
                egress: self.egress.clone(),
            }))
            .https_only(!self.allow_http)
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .pool_idle_timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| object_store::Error::Generic {
                store: "loams-egress",
                source: Box::new(e),
            })?;
        Ok(HttpClient::new(client))
    }
}

/// Drops every address the egress check refuses, and fails when none is
/// left (after DNS, as `sdk::net::connect` does).
struct EgressResolver {
    egress: Arc<dyn EgressCheck>,
}

impl reqwest::dns::Resolve for EgressResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let egress = self.egress.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            egress
                .check_host(&host, 0)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let (allowed, refused): (Vec<_>, Vec<_>) = addrs
                .into_iter()
                .partition(|a| egress.check_addr(a).is_ok());
            if allowed.is_empty() {
                let why = refused
                    .first()
                    .and_then(|a| egress.check_addr(a).err())
                    .map_or_else(
                        || format!("{host} resolved to no address"),
                        |e| e.to_string(),
                    );
                return Err(why.into());
            }
            Ok(Box::new(allowed.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Check an endpoint URL's host against the egress policy before any
/// request: by name, and by address when the host is an IP literal.
pub fn check_endpoint(url: &str, egress: &dyn EgressCheck) -> Result<(), ConnectorError> {
    let u = Url::parse(url).map_err(|e| ConnectorError::fatal(format!("endpoint: {e}")))?;
    let host = u
        .host_str()
        .ok_or_else(|| ConnectorError::fatal("endpoint has no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = u.port_or_known_default().unwrap_or(443);
    egress
        .check_host(host, port)
        .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        egress
            .check_addr(&SocketAddr::new(ip, port))
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    }
    Ok(())
}

async fn secret(
    secrets: &dyn SecretResolver,
    name: Option<&String>,
) -> Result<Option<String>, ConnectorError> {
    let Some(name) = name else { return Ok(None) };
    let s = secrets.resolve(&SecretRef::new(name)).await?;
    let v = s
        .expose_str()
        .ok_or_else(|| ConnectorError::auth(format!("secret {name:?} is not UTF-8")))?;
    Ok(Some(v.trim().to_owned()))
}

/// The root store for a table URL: `s3://bucket/…` (the bucket's root),
/// `file:///…` (the file system's root) or `memory://…`. deltalake prefixes
/// the table's path itself.
pub async fn root_store(
    table: &Url,
    cfg: &StorageConfig,
    secrets: &dyn SecretResolver,
    egress: Arc<dyn EgressCheck>,
) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
    match table.scheme() {
        "s3" | "s3a" => {
            let bucket = table
                .host_str()
                .ok_or_else(|| ConnectorError::fatal("an s3:// table URL names a bucket"))?;
            if let Some(endpoint) = &cfg.endpoint {
                check_endpoint(endpoint, &*egress)?;
            }
            let mut b = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(cfg.region.clone().unwrap_or_else(|| "us-east-1".to_owned()))
                .with_allow_http(cfg.allow_http)
                .with_virtual_hosted_style_request(cfg.virtual_hosted_style)
                .with_retry(RetryConfig {
                    backoff: BackoffConfig::default(),
                    max_retries: 3,
                    retry_timeout: Duration::from_secs(30),
                })
                .with_http_connector(EgressConnector::new(egress, cfg.allow_http));
            if let Some(e) = &cfg.endpoint {
                b = b.with_endpoint(e);
            }
            let a = &cfg.auth;
            let key = secret(secrets, a.access_key_id.as_ref()).await?;
            let sec = secret(secrets, a.secret_access_key.as_ref()).await?;
            match (key, sec) {
                (Some(k), Some(s)) => {
                    b = b.with_access_key_id(k).with_secret_access_key(s);
                    if let Some(t) = secret(secrets, a.session_token.as_ref()).await? {
                        b = b.with_token(t);
                    }
                }
                (None, None) => {}
                _ => {
                    return Err(ConnectorError::fatal(
                        "storage.auth.access_key_id and storage.auth.secret_access_key go together",
                    ));
                }
            }
            Ok(Arc::new(b.build().map_err(|e| map_err(&e, "S3 client"))?))
        }
        "file" => Ok(Arc::new(LocalFileSystem::new())),
        "memory" => Ok(Arc::new(InMemory::new())),
        other => Err(ConnectorError::fatal(format!(
            "table URL scheme {other:?} is not supported (s3, file or memory)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::DenyLocal;

    #[test]
    fn urls_lose_their_query() {
        assert_eq!(
            redact_urls("GET https://b.s3/k?X-Amz-Signature=abc failed"),
            "GET https://b.s3/k?<redacted> failed"
        );
    }

    #[test]
    fn literal_endpoints_are_checked() {
        assert!(check_endpoint("http://127.0.0.1:9000", &DenyLocal).is_err());
        assert!(check_endpoint("https://s3.example.com", &DenyLocal).is_ok());
    }
}
