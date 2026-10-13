//! `sdk::net`: the one way a family crate opens a TCP (or TLS) connection.
//! The host is checked before DNS, and every resolved address is checked
//! after DNS, at connect time, so a name that resolves to a forbidden address
//! is refused (Review Focus 9).

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};

use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::egress::EgressCheck;
use crate::error::ConnectorError;

/// Connect to `host:port`: egress-check the host, resolve it through the
/// check's resolver hook (an IP literal is not resolved), egress-check each
/// address, and dial the first allowed one that answers. Every call resolves
/// afresh, so each connection is checked against the answer it dials.
pub async fn connect(
    host: &str,
    port: u16,
    egress: &dyn EgressCheck,
) -> Result<TcpStream, ConnectorError> {
    egress
        .check_host(host, port)
        .map_err(|e| ConnectorError::fatal(e.to_string()))?;
    let addrs: Vec<SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => egress
            .resolver()
            .resolve(host, port)
            .await
            .map_err(|e| ConnectorError::retryable(format!("resolving {host}: {e}")))?,
    };
    if addrs.is_empty() {
        return Err(ConnectorError::retryable(format!(
            "{host} resolved to no address"
        )));
    }
    let mut last_refusal = None;
    let mut last_io = None;
    for addr in addrs {
        if let Err(e) = egress.check_addr(&addr) {
            last_refusal = Some(e);
            continue;
        }
        match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Err(e) => last_io = Some(e),
        }
    }
    match (last_io, last_refusal) {
        (Some(e), _) => Err(ConnectorError::retryable(format!(
            "connecting to {host}:{port}: {e}"
        ))),
        (None, Some(refused)) => Err(ConnectorError::fatal(format!("{host}:{port}: {refused}"))),
        (None, None) => Err(ConnectorError::retryable(format!(
            "connecting to {host}:{port} failed"
        ))),
    }
}

/// The default TLS client configuration: the webpki root set, ring.
pub fn default_tls_config() -> Arc<rustls::ClientConfig> {
    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            Arc::new(
                rustls::ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .expect("ring supports rustls' safe default protocol versions")
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

/// [`connect`], then TLS with `config` (or [`default_tls_config`]), verifying
/// the server's name `host`.
pub async fn connect_tls(
    host: &str,
    port: u16,
    egress: &dyn EgressCheck,
    config: Option<Arc<rustls::ClientConfig>>,
) -> Result<TlsStream<TcpStream>, ConnectorError> {
    let tcp = connect(host, port, egress).await?;
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| ConnectorError::fatal(format!("{host} is not a TLS server name: {e}")))?;
    TlsConnector::from(config.unwrap_or_else(default_tls_config))
        .connect(name, tcp)
        .await
        .map_err(|e| ConnectorError::retryable(format!("TLS with {host}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::egress::{AllowAll, DenyLocal};
    use crate::error::ErrorClass;

    #[tokio::test]
    async fn egress_is_checked_after_dns() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        assert!(connect("127.0.0.1", port, &AllowAll).await.is_ok());
        // "localhost" passes a host-name check but resolves to loopback,
        // which the address check refuses.
        let err = connect("localhost", port, &DenyLocal)
            .await
            .expect_err("loopback refused after DNS");
        assert_eq!(err.class, ErrorClass::Fatal);
        assert!(err.message.contains("egress refused"), "{}", err.message);
    }
}
