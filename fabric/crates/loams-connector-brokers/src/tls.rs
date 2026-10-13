//! TLS settings shared by the families that open their sockets through the
//! SDK (`sdk::net::connect_tls`): AMQP 1.0, MQTT and WebSocket. A custom CA
//! bundle (PEM, not a secret) replaces the webpki roots, and a client
//! certificate (PEM) with its key (a secret *name*, PEM) enables mTLS.

use std::sync::Arc;

use loams_connector_sdk::{ConnectorError, SecretResolver};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;

use crate::common::resolve;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    /// PEM CA bundle to trust instead of the webpki roots.
    #[serde(default)]
    pub ca_pem: Option<String>,
    /// PEM client certificate chain for mTLS.
    #[serde(default)]
    pub cert_pem: Option<String>,
    /// The secret name of the client's PEM private key.
    #[serde(default)]
    pub key_pem: Option<String>,
}

impl TlsSettings {
    pub fn is_default(&self) -> bool {
        self.ca_pem.is_none() && self.cert_pem.is_none() && self.key_pem.is_none()
    }

    /// The rustls client config these settings describe, or `None` for the
    /// SDK's default (webpki roots, no client certificate).
    pub async fn client_config(
        &self,
        secrets: &Arc<dyn SecretResolver>,
    ) -> Result<Option<Arc<rustls::ClientConfig>>, ConnectorError> {
        if self.is_default() {
            return Ok(None);
        }
        let mut roots = rustls::RootCertStore::empty();
        match &self.ca_pem {
            Some(pem) => {
                for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                    let cert =
                        cert.map_err(|_| ConnectorError::fatal("tls.ca_pem is not a PEM bundle"))?;
                    roots
                        .add(cert)
                        .map_err(|e| ConnectorError::fatal(format!("tls.ca_pem: {e}")))?;
                }
                if roots.is_empty() {
                    return Err(ConnectorError::fatal("tls.ca_pem holds no certificate"));
                }
            }
            None => roots.extend(webpki_roots_compat()),
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| ConnectorError::fatal(format!("tls: {e}")))?
            .with_root_certificates(roots);
        let config = match (&self.cert_pem, &self.key_pem) {
            (Some(cert), Some(key_name)) => {
                let chain: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_slice_iter(cert.as_bytes())
                        .collect::<Result<_, _>>()
                        .map_err(|_| ConnectorError::fatal("tls.cert_pem is not a PEM chain"))?;
                let key = resolve(secrets, key_name).await?;
                let key = PrivateKeyDer::from_pem_slice(key.expose())
                    .map_err(|_| ConnectorError::auth("tls.key_pem is not a PEM private key"))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| ConnectorError::auth(format!("tls client certificate: {e}")))?
            }
            (None, None) => builder.with_no_client_auth(),
            _ => {
                return Err(ConnectorError::fatal(
                    "tls.cert_pem and tls.key_pem go together",
                ));
            }
        };
        Ok(Some(Arc::new(config)))
    }
}

/// The webpki roots the SDK trusts by default, for a config that adds a
/// client certificate without its own CA bundle.
fn webpki_roots_compat() -> Vec<rustls::pki_types::TrustAnchor<'static>> {
    webpki_roots::TLS_SERVER_ROOTS.to_vec()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::StaticSecrets;

    #[tokio::test]
    async fn default_settings_use_the_sdk_config() {
        let secrets: Arc<dyn SecretResolver> = Arc::new(StaticSecrets::new());
        assert!(
            TlsSettings::default()
                .client_config(&secrets)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn bad_pem_is_refused_without_echoing_it() {
        let secrets: Arc<dyn SecretResolver> =
            Arc::new(StaticSecrets::new().with("k", "not-a-key-hunter2"));
        let s = TlsSettings {
            ca_pem: Some("garbage".into()),
            ..TlsSettings::default()
        };
        let e = s.client_config(&secrets).await.unwrap_err();
        assert!(!e.message.contains("garbage"));
        let s = TlsSettings {
            cert_pem: Some(String::new()),
            key_pem: Some("k".into()),
            ..TlsSettings::default()
        };
        let e = s.client_config(&secrets).await.unwrap_err();
        assert!(!e.message.contains("hunter2"), "{}", e.message);
        let s = TlsSettings {
            cert_pem: Some(String::new()),
            ..TlsSettings::default()
        };
        assert!(s.client_config(&secrets).await.is_err());
    }
}
