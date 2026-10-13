//! Building an `object_store` client for an instance: S3 (and its targets
//! R2 and MinIO/RustFS), GCS and Azure Blob (and ADLS Gen2's flat Blob API).
//!
//! Every connection the client opens resolves through the instance's
//! [`EgressCheck`] ([`EgressConnector`]): `object_store` brings its own
//! `reqwest` client, so the family builds that client itself, with a DNS
//! resolver that refuses what the egress policy refuses (Task 25's hook, the
//! same rule as `sdk::net::connect`). An endpoint given as an IP literal is
//! checked when the store is built, because no DNS lookup happens for it.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use loams_connector_sdk::{ConnectorError, EgressCheck, SecretRef, SecretResolver};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::client::{HttpClient, HttpConnector};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{BackoffConfig, ClientOptions, ObjectStore, RetryConfig};
use tokio::sync::OnceCell;

use crate::config::ObjectsConfig;

/// Which API a store speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// S3 and the S3 targets (R2, MinIO, RustFS).
    S3,
    Gcs,
    /// Azure Blob, and ADLS Gen2 over its Blob endpoint.
    Azure,
}

/// An `object_store::Error` as a connector error. The message never carries
/// a URL's query string: an Azure SAS token travels there.
pub fn map_err(e: object_store::Error, what: &str) -> ConnectorError {
    use object_store::Error as E;
    let msg = format!("{what}: {}", redact_urls(&e.to_string()));
    match e {
        E::Unauthenticated { .. } | E::PermissionDenied { .. } => ConnectorError::auth(msg),
        E::NotSupported { .. } | E::NotImplemented { .. } | E::UnknownConfigurationKey { .. } => {
            ConnectorError::fatal(msg)
        }
        E::InvalidPath { .. } => ConnectorError::poison(msg),
        // `object_store` has already retried transient HTTP failures with
        // its own (short, see `retry_config`) policy; the SDK retries again
        // by class.
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

/// `object_store`'s retries are kept short: the SDK's pipeline retries by
/// error class with its own policy and backoff (R4.9).
fn retry_config() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig::default(),
        max_retries: 3,
        retry_timeout: Duration::from_secs(30),
    }
}

/// Builds `object_store`'s HTTP client with DNS answers filtered by the
/// instance's egress check.
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

/// A resolver that drops every address the egress check refuses, and fails
/// when none is left (after DNS, as `sdk::net::connect` does).
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
    let uri: http::Uri = url
        .parse()
        .map_err(|e| ConnectorError::fatal(format!("endpoint is not a URL: {e}")))?;
    let host = uri
        .host()
        .ok_or_else(|| ConnectorError::fatal("endpoint has no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let https = uri.scheme_str() == Some("https");
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
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
    name: &Option<String>,
) -> Result<Option<String>, ConnectorError> {
    let Some(name) = name else { return Ok(None) };
    let s = secrets.resolve(&SecretRef::new(name)).await?;
    let v = s
        .expose_str()
        .ok_or_else(|| ConnectorError::auth(format!("secret {name:?} is not UTF-8")))?;
    Ok(Some(v.trim().to_owned()))
}

/// Build the store an instance's config names.
pub async fn build(
    backend: Backend,
    cfg: &ObjectsConfig,
    secrets: &dyn SecretResolver,
    egress: Arc<dyn EgressCheck>,
) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
    if cfg.bucket.is_empty() {
        return Err(ConnectorError::fatal("bucket is required"));
    }
    if let Some(endpoint) = &cfg.endpoint {
        check_endpoint(endpoint, &*egress)?;
    }
    let connector = EgressConnector::new(egress, cfg.allow_http);
    let a = &cfg.auth;
    let store: Arc<dyn ObjectStore> = match backend {
        Backend::S3 => {
            let mut b = AmazonS3Builder::new()
                .with_bucket_name(&cfg.bucket)
                .with_region(cfg.region.clone().unwrap_or_else(|| "us-east-1".to_owned()))
                .with_allow_http(cfg.allow_http)
                .with_virtual_hosted_style_request(cfg.virtual_hosted_style)
                .with_retry(retry_config())
                .with_http_connector(connector);
            if let Some(e) = &cfg.endpoint {
                b = b.with_endpoint(e);
            }
            let key = secret(secrets, &a.access_key_id).await?;
            let sec = secret(secrets, &a.secret_access_key).await?;
            match (key, sec) {
                (Some(k), Some(s)) => {
                    b = b.with_access_key_id(k).with_secret_access_key(s);
                    if let Some(t) = secret(secrets, &a.session_token).await? {
                        b = b.with_token(t);
                    }
                }
                (None, None) => {}
                _ => {
                    return Err(ConnectorError::fatal(
                        "auth.access_key_id and auth.secret_access_key go together",
                    ));
                }
            }
            Arc::new(b.build().map_err(|e| map_err(e, "S3 client"))?)
        }
        Backend::Gcs => {
            let mut b = GoogleCloudStorageBuilder::new()
                .with_bucket_name(&cfg.bucket)
                .with_retry(retry_config())
                .with_client_options(ClientOptions::new().with_allow_http(cfg.allow_http))
                .with_http_connector(connector);
            let key = secret(secrets, &a.service_account_key).await?;
            if let Some(key) = gcs_key(key, cfg.endpoint.as_deref())? {
                b = b.with_service_account_key(key);
            }
            Arc::new(b.build().map_err(|e| map_err(e, "GCS client"))?)
        }
        Backend::Azure => {
            let account = cfg
                .account
                .clone()
                .ok_or_else(|| ConnectorError::fatal("account is required for Azure"))?;
            let mut b = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(&cfg.bucket)
                .with_allow_http(cfg.allow_http)
                .with_retry(retry_config())
                .with_http_connector(connector);
            if let Some(e) = &cfg.endpoint {
                b = b.with_endpoint(e.clone());
            }
            if let Some(k) = secret(secrets, &a.access_key).await? {
                b = b.with_access_key(k);
            } else if let Some(sas) = secret(secrets, &a.sas_token).await? {
                b = b.with_sas_authorization(sas_pairs(&sas));
            } else if let Some(t) = secret(secrets, &a.bearer_token).await? {
                b = b.with_bearer_token_authorization(t);
            }
            Arc::new(b.build().map_err(|e| map_err(e, "Azure client"))?)
        }
    };
    Ok(store)
}

/// A SAS token's query pairs (`sv=…&sig=…`, with or without a leading `?`).
pub fn sas_pairs(sas: &str) -> Vec<(String, String)> {
    sas.trim_start_matches('?')
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

pub(crate) fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(hex) = std::str::from_utf8(&b[i + 1..i + 3])
            && let Ok(v) = u8::from_str_radix(hex, 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The GCS service account key, pointed at `endpoint` when one is set
/// (fake-gcs-server in CI): `object_store` reads `gcs_base_url` from the key,
/// and an emulator without a key gets one with `disable_oauth`.
fn gcs_key(key: Option<String>, endpoint: Option<&str>) -> Result<Option<String>, ConnectorError> {
    let Some(endpoint) = endpoint else {
        return Ok(key);
    };
    let mut v: serde_json::Value = match &key {
        Some(k) => serde_json::from_str(k)
            .map_err(|_| ConnectorError::auth("the GCS service account key is not JSON"))?,
        None => serde_json::json!({
            "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""
        }),
    };
    v["gcs_base_url"] = serde_json::Value::String(endpoint.trim_end_matches('/').to_owned());
    Ok(Some(v.to_string()))
}

/// The store of an instance, built on first use (the SDK's factories are
/// synchronous, and resolving secrets is not).
pub struct StoreHandle {
    cell: OnceCell<Arc<dyn ObjectStore>>,
    init: Option<StoreInit>,
}

struct StoreInit {
    backend: Backend,
    cfg: ObjectsConfig,
    secrets: Arc<dyn SecretResolver>,
    egress: Arc<dyn EgressCheck>,
}

impl std::fmt::Debug for StoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreHandle")
            .field("built", &self.cell.initialized())
            .finish_non_exhaustive()
    }
}

impl StoreHandle {
    /// A handle that builds the store from the config on first use.
    pub fn lazy(
        backend: Backend,
        cfg: ObjectsConfig,
        secrets: Arc<dyn SecretResolver>,
        egress: Arc<dyn EgressCheck>,
    ) -> Self {
        Self {
            cell: OnceCell::new(),
            init: Some(StoreInit {
                backend,
                cfg,
                secrets,
                egress,
            }),
        }
    }

    /// A handle over a store built elsewhere (tests: `InMemory`).
    pub fn ready(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cell: OnceCell::from(store),
            init: None,
        }
    }

    pub async fn get(&self) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        self.cell
            .get_or_try_init(|| async {
                let init = self
                    .init
                    .as_ref()
                    .ok_or_else(|| ConnectorError::fatal("store handle has no config"))?;
                build(init.backend, &init.cfg, &*init.secrets, init.egress.clone()).await
            })
            .await
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::{AllowAll, DenyLocal, StaticSecrets};

    #[test]
    fn urls_lose_their_query() {
        let m = redact_urls(
            "error sending request for url (https://acct.blob.core.windows.net/c/x?sv=1&sig=abc): boom",
        );
        assert!(!m.contains("sig=abc"), "{m}");
        assert!(
            m.contains("https://acct.blob.core.windows.net/c/x?<redacted>"),
            "{m}"
        );
        assert_eq!(redact_urls("no url here, httpx"), "no url here, httpx");
    }

    #[test]
    fn sas_tokens_split_into_pairs() {
        assert_eq!(
            sas_pairs("?sv=2022-11-02&sig=a%2Bb%3D"),
            vec![
                ("sv".to_owned(), "2022-11-02".to_owned()),
                ("sig".to_owned(), "a+b=".to_owned())
            ]
        );
    }

    #[test]
    fn endpoint_ip_literals_are_checked_before_any_request() {
        assert!(check_endpoint("http://127.0.0.1:9000", &AllowAll).is_ok());
        let e = check_endpoint("http://127.0.0.1:9000", &DenyLocal).expect_err("loopback");
        assert!(e.message.contains("egress refused"), "{}", e.message);
    }

    #[test]
    fn gcs_emulator_key_points_at_the_endpoint() {
        let k = gcs_key(None, Some("http://fake-gcs:4443/"))
            .expect("key")
            .expect("some");
        let v: serde_json::Value = serde_json::from_str(&k).expect("json");
        assert_eq!(v["gcs_base_url"], "http://fake-gcs:4443");
        assert_eq!(v["disable_oauth"], true);
        assert_eq!(
            gcs_key(Some("{}".into()), None).expect("key").as_deref(),
            Some("{}")
        );
    }

    #[tokio::test]
    async fn s3_store_builds_from_secret_names() {
        let cfg = ObjectsConfig::parse(
            &serde_json::json!({
                "bucket": "b", "endpoint": "http://127.0.0.1:9000", "allow_http": true,
                "auth": {"access_key_id": "k", "secret_access_key": "s"}
            }),
            None,
        )
        .expect("config");
        let secrets = StaticSecrets::new()
            .with("k", "AKIDEXAMPLE")
            .with("s", "secret-value");
        build(Backend::S3, &cfg, &secrets, Arc::new(AllowAll))
            .await
            .expect("builds without a request");
        let half = ObjectsConfig::parse(
            &serde_json::json!({"bucket": "b", "auth": {"access_key_id": "k"}}),
            None,
        )
        .expect("config");
        assert!(
            build(Backend::S3, &half, &secrets, Arc::new(AllowAll))
                .await
                .is_err()
        );
    }
}
