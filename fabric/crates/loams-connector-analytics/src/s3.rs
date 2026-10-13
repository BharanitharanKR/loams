//! S3 for Redshift's staging and `UNLOAD` bucket, through `object_store`
//! with the egress-checked HTTP client (CN2 R13.3's pattern): `object_store`
//! brings its own `reqwest` client, so the family builds that client with a
//! DNS resolver that drops every address the instance's egress check
//! refuses. An endpoint given as an IP literal is checked when the store is
//! built.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use loams_connector_sdk::{ConnectorError, EgressCheck, SecretResolver};
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpConnector};
use object_store::{BackoffConfig, ClientOptions, ObjectStore, RetryConfig};

use crate::common::{base_url, secret, text};

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

impl HttpConnector for EgressConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(EgressResolver {
                egress: self.egress.clone(),
            }))
            .https_only(!self.allow_http)
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| object_store::Error::Generic {
                store: "loams-egress",
                source: Box::new(e),
            })?;
        Ok(HttpClient::new(client))
    }
}

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
            let allowed: Vec<SocketAddr> = addrs
                .iter()
                .copied()
                .filter(|a| egress.check_addr(a).is_ok())
                .collect();
            if allowed.is_empty() {
                let why = addrs
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

/// Where a bucket is and how to reach it.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// An S3-compatible endpoint; AWS when absent.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
}

/// An `object_store::Error` as a connector error. Query strings (presigned
/// credentials) never reach the message.
pub fn map_err(e: &object_store::Error, what: &str) -> ConnectorError {
    use object_store::Error as E;
    let full = e.to_string();
    let msg = format!(
        "{what}: {}",
        full.split('?').next().unwrap_or_default().trim()
    );
    match e {
        E::Unauthenticated { .. } | E::PermissionDenied { .. } => ConnectorError::auth(msg),
        E::NotSupported { .. } | E::NotImplemented { .. } => ConnectorError::fatal(msg),
        _ => ConnectorError::retryable(msg),
    }
}

/// `s3://bucket/prefix` → (`bucket`, `prefix` without slashes at its ends).
pub fn split_uri(uri: &str) -> Result<(String, String), ConnectorError> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| ConnectorError::fatal(format!("{uri:?} is not an s3:// URI")))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty()
        || !bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        || prefix.contains(['\'', '\\'])
    {
        return Err(ConnectorError::fatal(format!(
            "{uri:?} is not a plain s3:// URI"
        )));
    }
    Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
}

/// The bucket's store, with the instance's AWS credentials.
pub async fn bucket(
    bucket: &str,
    cfg: &S3Config,
    region: &str,
    creds: &crate::redshift::AwsAuth,
    secrets: &dyn SecretResolver,
    egress: Arc<dyn EgressCheck>,
) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
    if let Some(e) = &cfg.endpoint {
        base_url(e, &*egress)?;
    }
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(cfg.region.clone().unwrap_or_else(|| region.to_owned()))
        .with_allow_http(cfg.allow_http)
        .with_retry(RetryConfig {
            backoff: BackoffConfig::default(),
            max_retries: 3,
            retry_timeout: Duration::from_secs(30),
        })
        .with_http_connector(EgressConnector {
            egress,
            allow_http: cfg.allow_http,
        });
    if let Some(e) = &cfg.endpoint {
        b = b.with_endpoint(e);
    }
    let key = secret(secrets, Some(&creds.access_key_id)).await?;
    let sec = secret(secrets, Some(&creds.secret_access_key)).await?;
    if let (Some(k), Some(s)) = (key, sec) {
        b = b
            .with_access_key_id(text(&k, "the AWS access key id")?)
            .with_secret_access_key(text(&s, "the AWS secret access key")?);
        if let Some(t) = secret(secrets, creds.session_token.as_ref()).await? {
            b = b.with_token(text(&t, "the AWS session token")?);
        }
    }
    Ok(Arc::new(b.build().map_err(|e| map_err(&e, "S3 client"))?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_uris_split() {
        assert_eq!(
            split_uri("s3://my-bucket/unload/run/").expect("ok"),
            ("my-bucket".to_owned(), "unload/run".to_owned())
        );
        assert!(split_uri("s3://Bad_Bucket/x").is_err());
        assert!(split_uri("s3://b/x'y").is_err());
        assert!(split_uri("https://b/x").is_err());
    }
}
