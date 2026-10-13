//! Connections. Every connection goes through `sdk::net::connect` (the
//! egress check after DNS, Review Focus 9); TLS is rustls with ring, as in
//! the SDK, adapted to `tokio-postgres`'s `TlsConnect` and used directly by
//! the replication client.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use loams_connector_sdk::{ConnectorContext, ConnectorError, ErrorClass, SecretRef, net};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_postgres::Client;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};

use crate::config::{Auth, PgConfig, SslMode};
use crate::rds_iam;

/// `sslmode=require`: encrypt, accept any certificate (libpq's meaning),
/// still checking the handshake signatures.
#[derive(Debug)]
struct AcceptAnyCert(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// The rustls configuration for `cfg.ssl` (`None` for `disable`).
pub fn tls_config(cfg: &PgConfig) -> Result<Option<Arc<ClientConfig>>, ConnectorError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| ConnectorError::fatal(format!("TLS: {e}")))?;
    Ok(match cfg.ssl {
        SslMode::Disable => None,
        SslMode::Require => Some(Arc::new(
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
                .with_no_client_auth(),
        )),
        SslMode::VerifyFull => {
            let mut roots = rustls::RootCertStore {
                roots: root_anchors(),
            };
            if let Some(pem) = &cfg.ca_pem {
                use rustls::pki_types::pem::PemObject;
                for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                    let cert =
                        cert.map_err(|e| ConnectorError::fatal(format!("config: ca_pem: {e}")))?;
                    roots
                        .add(cert)
                        .map_err(|e| ConnectorError::fatal(format!("config: ca_pem: {e}")))?;
                }
            }
            Some(Arc::new(
                builder.with_root_certificates(roots).with_no_client_auth(),
            ))
        }
    })
}

fn root_anchors() -> Vec<rustls::pki_types::TrustAnchor<'static>> {
    webpki_roots::TLS_SERVER_ROOTS.to_vec()
}

/// `tokio-postgres`'s TLS hook over tokio-rustls.
#[derive(Clone)]
pub struct RustlsConnect {
    config: Arc<ClientConfig>,
    host: String,
}

impl TlsConnect<TcpStream> for RustlsConnect {
    type Stream = PgStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<PgStream, io::Error>> + Send>>;

    fn connect(self, stream: TcpStream) -> Self::Future {
        Box::pin(async move {
            let name = ServerName::try_from(self.host.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
            let tls = tokio_rustls::TlsConnector::from(self.config)
                .connect(name, stream)
                .await?;
            Ok(PgStream::Tls(Box::new(tls)))
        })
    }
}

/// A connection's byte stream: plain TCP or TLS over it.
pub enum PgStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for PgStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            PgStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PgStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            PgStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            PgStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Plain(s) => Pin::new(s).poll_flush(cx),
            PgStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            PgStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            PgStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl tokio_postgres::tls::TlsStream for PgStream {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

/// The password for a connection: the named secret, or a fresh RDS IAM
/// token. `None` when no password is configured.
pub async fn password(
    cfg: &PgConfig,
    ctx: &ConnectorContext,
) -> Result<Option<Vec<u8>>, ConnectorError> {
    match &cfg.auth {
        Auth::Basic { password: None } => Ok(None),
        Auth::Basic {
            password: Some(name),
        } => Ok(Some(
            ctx.secrets
                .resolve(&SecretRef::new(name))
                .await?
                .expose()
                .to_vec(),
        )),
        Auth::Iam(aws) => {
            let key = ctx
                .secrets
                .resolve(&SecretRef::new(&aws.access_key_id))
                .await?;
            let secret = ctx
                .secrets
                .resolve(&SecretRef::new(&aws.secret_access_key))
                .await?;
            let token = match &aws.session_token {
                Some(t) => Some(ctx.secrets.resolve(&SecretRef::new(t)).await?),
                None => None,
            };
            let utf8 = |s: &loams_connector_sdk::Secret| {
                s.expose_str()
                    .map(str::to_owned)
                    .ok_or_else(|| ConnectorError::auth("an AWS credential is not UTF-8"))
            };
            let (key, secret) = (utf8(&key)?, utf8(&secret)?);
            let token = token.as_ref().map(utf8).transpose()?;
            let creds = rds_iam::Credentials {
                access_key_id: &key,
                secret_access_key: &secret,
                session_token: token.as_deref(),
            };
            Ok(Some(
                rds_iam::rds_token(
                    &cfg.host,
                    cfg.port,
                    &cfg.user,
                    &aws.region,
                    &creds,
                    rds_iam::now(),
                )
                .into_bytes(),
            ))
        }
    }
}

/// A `tokio-postgres` client for `cfg`, its connection driven on a task.
pub async fn connect(cfg: &PgConfig, ctx: &ConnectorContext) -> Result<Client, ConnectorError> {
    let password = password(cfg, ctx).await?;
    let mut pg = tokio_postgres::Config::new();
    pg.user(&cfg.user)
        .dbname(&cfg.database)
        .application_name("loams")
        .host(&cfg.host)
        .port(cfg.port);
    if let Some(p) = &password {
        pg.password(p);
    }
    let tcp = net::connect(&cfg.host, cfg.port, ctx.egress.as_ref()).await?;
    let client = match tls_config(cfg)? {
        None => {
            pg.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client, conn) = pg
                .connect_raw(tcp, tokio_postgres::NoTls)
                .await
                .map_err(|e| classify(&e, "connecting"))?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            client
        }
        Some(config) => {
            pg.ssl_mode(tokio_postgres::config::SslMode::Require);
            let tls = RustlsConnect {
                config,
                host: cfg.host.clone(),
            };
            let (client, conn) = pg
                .connect_raw(tcp, tls)
                .await
                .map_err(|e| classify(&e, "connecting"))?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            client
        }
    };
    Ok(client)
}

/// The class of a server error from its SQLSTATE (PostgreSQL appendix A),
/// shared by every path.
pub fn class_of_sqlstate(code: &str) -> ErrorClass {
    match code {
        // Serialization failure, deadlock (CockroachDB's retry error is 40001).
        "40001" | "40P01" => ErrorClass::Retryable { after: None },
        // Object in use (an active replication slot), lock not available.
        "55006" | "55P03" => ErrorClass::Retryable { after: None },
        // Too many connections, out of memory or disk.
        c if c.starts_with("53") => ErrorClass::Throttled {
            after: std::time::Duration::from_secs(1),
        },
        // Connection exceptions, shutdown, cannot connect now.
        c if c.starts_with("08") || c.starts_with("57P") => ErrorClass::Retryable { after: None },
        // Invalid authorization, insufficient privilege.
        c if c.starts_with("28") || c == "42501" => ErrorClass::Auth,
        // Data exceptions and constraint violations: this record.
        c if c.starts_with("22") || c.starts_with("23") => ErrorClass::Poison,
        _ => ErrorClass::Fatal,
    }
}

/// A `tokio-postgres` error as a [`ConnectorError`]. The message is the
/// server's message and SQLSTATE, never the configuration.
pub fn classify(e: &tokio_postgres::Error, what: &str) -> ConnectorError {
    match e.as_db_error() {
        Some(db) => ConnectorError::new(
            class_of_sqlstate(db.code().code()),
            format!("{what}: {} ({})", db.message(), db.code().code()),
        ),
        // No server error: a closed connection or an I/O failure.
        None if e.is_closed() || std::error::Error::source(e).is_some() => {
            ConnectorError::retryable(format!("{what}: connection lost"))
        }
        None => ConnectorError::retryable(format!("{what}: {e}")),
    }
}

impl std::fmt::Debug for RustlsConnect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustlsConnect").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PgStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStream").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstates_map_to_classes() {
        assert_eq!(
            class_of_sqlstate("40001"),
            ErrorClass::Retryable { after: None }
        );
        assert_eq!(class_of_sqlstate("28P01"), ErrorClass::Auth);
        assert_eq!(class_of_sqlstate("42501"), ErrorClass::Auth);
        assert_eq!(class_of_sqlstate("23505"), ErrorClass::Poison);
        assert_eq!(class_of_sqlstate("22P02"), ErrorClass::Poison);
        assert!(matches!(
            class_of_sqlstate("53300"),
            ErrorClass::Throttled { .. }
        ));
        assert_eq!(
            class_of_sqlstate("08006"),
            ErrorClass::Retryable { after: None }
        );
        assert_eq!(class_of_sqlstate("42P01"), ErrorClass::Fatal);
    }
}
