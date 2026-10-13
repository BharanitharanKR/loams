//! Connections. Every TDS connection is opened by `sdk::net::connect` (the
//! egress check before and after DNS, Review Focus 9), including a gateway
//! redirect's second hop. `encrypt: strict` puts TLS — the SDK's rustls with
//! ring, ALPN `tds/8.0` — on that stream before the first TDS byte (TDS 8.0),
//! and `tiberius`, built without a TLS backend of its own, speaks TDS over
//! it (ruling R12.1).

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use loams_connector_sdk::{ConnectorContext, ConnectorError, ErrorClass, SecretRef, net};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Row, ToSql};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::{Auth, Encrypt, SqlConfig, TableName};
use crate::types::{Cell, Column, declared_type};

/// The ALPN protocol of TDS 8.0 (MS-TDS 2.1).
pub const TDS_ALPN: &[u8] = b"tds/8.0";
/// Gateway redirects followed before giving up.
const MAX_REDIRECTS: usize = 2;

/// A client over the instance's stream.
pub type SqlClient = Client<Compat<SqlStream>>;

/// `trust_server_certificate`: encrypt, accept any certificate, still check
/// the handshake signatures.
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

/// The rustls configuration of `encrypt: strict` (`None` for `disable`).
pub fn tls_config(cfg: &SqlConfig) -> Result<Option<Arc<ClientConfig>>, ConnectorError> {
    if cfg.encrypt == Encrypt::Disable {
        return Ok(None);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| ConnectorError::fatal(format!("TLS: {e}")))?;
    let mut config = if cfg.trust_server_certificate {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
            .with_no_client_auth()
    } else {
        let mut roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        if let Some(pem) = &cfg.ca_pem {
            use rustls::pki_types::pem::PemObject;
            let mut n = 0;
            for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                let cert =
                    cert.map_err(|e| ConnectorError::fatal(format!("config: ca_pem: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| ConnectorError::fatal(format!("config: ca_pem: {e}")))?;
                n += 1;
            }
            if n == 0 {
                return Err(ConnectorError::fatal("config: ca_pem holds no certificate"));
            }
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    config.alpn_protocols = vec![TDS_ALPN.to_vec()];
    Ok(Some(Arc::new(config)))
}

/// A connection's byte stream: plain TCP or TLS over it.
pub enum SqlStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl std::fmt::Debug for SqlStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SqlStream::Plain(_) => "SqlStream::Plain",
            SqlStream::Tls(_) => "SqlStream::Tls",
        })
    }
}

impl AsyncRead for SqlStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SqlStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            SqlStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SqlStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            SqlStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            SqlStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SqlStream::Plain(s) => Pin::new(s).poll_flush(cx),
            SqlStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            SqlStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            SqlStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Open the byte stream to `host:port`: `sdk::net::connect`, then TLS when
/// the config says `strict`.
pub async fn open_stream(
    cfg: &SqlConfig,
    ctx: &ConnectorContext,
    host: &str,
    port: u16,
) -> Result<SqlStream, ConnectorError> {
    let tcp = net::connect(host, port, ctx.egress.as_ref()).await?;
    let Some(tls) = tls_config(cfg)? else {
        return Ok(SqlStream::Plain(tcp));
    };
    let cert_name = cfg.host_name_in_certificate.as_deref().unwrap_or(host);
    let name = ServerName::try_from(cert_name.to_owned())
        .map_err(|e| ConnectorError::fatal(format!("{cert_name} is not a TLS server name: {e}")))?;
    let stream = tokio_rustls::TlsConnector::from(tls)
        .connect(name, tcp)
        .await
        .map_err(|e| {
            let msg = format!("TLS (TDS 8.0 strict) with {host}: {e}");
            if e.kind() == io::ErrorKind::InvalidData {
                // A certificate the roots do not trust is a configuration
                // problem, not a network one: retrying will not help.
                ConnectorError::fatal(msg)
            } else {
                ConnectorError::retryable(msg)
            }
        })?;
    Ok(SqlStream::Tls(Box::new(stream)))
}

/// The `tiberius` config for a connection (no address: the stream is ours).
pub async fn tds_config(cfg: &SqlConfig, ctx: &ConnectorContext) -> Result<Config, ConnectorError> {
    let mut c = Config::new();
    c.host(&cfg.host);
    c.port(cfg.port);
    c.database(&cfg.database);
    c.application_name("loams");
    // TLS, when configured, is already on the stream (strict) or absent
    // (disable): `tiberius` must never negotiate its own.
    c.encryption(EncryptionLevel::NotSupported);
    c.handshake_timeout(Some(Duration::from_secs(30)));
    let auth = match &cfg.auth {
        Auth::Sql { password } => {
            let user = cfg.user.clone().unwrap_or_default();
            let password = match password {
                Some(name) => {
                    let s = ctx.secrets.resolve(&SecretRef::new(name)).await?;
                    s.expose_str()
                        .ok_or_else(|| ConnectorError::auth("password is not UTF-8"))?
                        .to_owned()
                }
                None => String::new(),
            };
            AuthMethod::sql_server(user, password)
        }
        Auth::Entra(e) => AuthMethod::aad_token(cfg.tokens.token(e, ctx).await?),
        Auth::EntraToken { token } => {
            let s = ctx.secrets.resolve(&SecretRef::new(token)).await?;
            AuthMethod::aad_token(
                s.expose_str()
                    .ok_or_else(|| ConnectorError::auth("token is not UTF-8"))?
                    .trim(),
            )
        }
    };
    c.authentication(auth);
    Ok(c)
}

/// Connect, log in and apply the session settings the family relies on.
pub async fn connect(cfg: &SqlConfig, ctx: &ConnectorContext) -> Result<SqlClient, ConnectorError> {
    let mut host = cfg.host.clone();
    let mut port = cfg.port;
    for _ in 0..=MAX_REDIRECTS {
        let stream = open_stream(cfg, ctx, &host, port).await?;
        let mut config = tds_config(cfg, ctx).await?;
        config.host(&host);
        config.port(port);
        match Client::connect(config, stream.compat_write()).await {
            Ok(mut client) => {
                // XACT_ABORT OFF keeps a statement error from dooming the
                // open transaction, so the sink's savepoints work.
                client
                    .simple_query(
                        "SET XACT_ABORT OFF; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; \
                         SET ANSI_WARNINGS ON; SET ARITHABORT ON; SET NOCOUNT ON",
                    )
                    .await
                    .map_err(|e| classify(&e, "session settings"))?
                    .into_results()
                    .await
                    .map_err(|e| classify(&e, "session settings"))?;
                return Ok(client);
            }
            // Azure SQL's gateway hands out the database's own node: a new
            // connection, egress-checked again.
            Err(tiberius::error::Error::Routing { host: h, port: p }) => {
                host = h;
                port = p;
            }
            Err(e) => {
                let err = classify(&e, &format!("connecting to {host}:{port}"));
                if err.class == ErrorClass::Auth
                    && let Auth::Entra(_) = cfg.auth
                {
                    cfg.tokens.clear().await;
                }
                return Err(err);
            }
        }
    }
    Err(ConnectorError::fatal(format!(
        "{}: more than {MAX_REDIRECTS} gateway redirects",
        cfg.host
    )))
}

/// The class of a server error number (MS-TDS ERROR token; `sys.messages`).
pub fn class_of_code(code: u32) -> ErrorClass {
    match code {
        // Deadlock victim, lock timeout, snapshot update conflict, Azure
        // SQL's transient errors (failover, throttling, resource limits).
        1205 | 1222 | 3960 | 3961 | 4060 | 233 | 64 | 10053 | 10054 | 10060 | 10928 | 10929
        | 10936 | 40143 | 40197 | 40501 | 40540 | 40613 | 49918 | 49919 | 49920 | 8628 | 701
        | 1204 => ErrorClass::Retryable { after: None },
        // Login failures, including Entra token problems.
        18456 | 18452 | 18470 | 18486 | 18487 | 18488 | 33155 | 33156 | 33157 | 40532 => {
            ErrorClass::Auth
        }
        // A row the target refuses: duplicate key, NULL into NOT NULL,
        // constraint, conversion, overflow, truncation, bad date or XML.
        2627 | 2601 | 515 | 547 | 245 | 8114 | 8115 | 8152 | 2628 | 241 | 242 | 517 | 9455
        | 9436 | 9420 | 220 | 232 | 8169 => ErrorClass::Poison,
        // The target's shape no longer matches: a column is gone or new.
        207 | 213 | 4901 | 110 | 109 | 120 | 121 => ErrorClass::SchemaIncompatible,
        _ => ErrorClass::Fatal,
    }
}

/// A `tiberius` error as a connector error. Server messages carry no
/// credentials; the login packet is never echoed.
pub fn classify(e: &tiberius::error::Error, what: &str) -> ConnectorError {
    use tiberius::error::Error as E;
    let class = match e {
        E::Io { .. } => ErrorClass::Retryable { after: None },
        E::Server(t) => class_of_code(t.code()),
        E::Conversion(_) | E::Utf8 | E::Utf16 | E::ParseInt(_) | E::BulkInput(_) => {
            ErrorClass::Poison
        }
        E::Routing { .. } => ErrorClass::Retryable { after: None },
        _ => ErrorClass::Fatal,
    };
    let mut msg = format!("{what}: {e}");
    if let E::Server(t) = e
        && t.code() == 3952
    {
        msg.push_str(
            " (snapshot isolation is off: ALTER DATABASE … SET ALLOW_SNAPSHOT_ISOLATION ON, \
             or set isolation: read_committed)",
        );
    }
    ConnectorError::new(class, msg)
}

/// Run `sql` with `params` and return every row of its first result set.
pub async fn rows(
    client: &mut SqlClient,
    sql: &str,
    params: &[&dyn ToSql],
    what: &str,
) -> Result<Vec<Row>, ConnectorError> {
    client
        .query(sql, params)
        .await
        .map_err(|e| classify(&e, what))?
        .into_first_result()
        .await
        .map_err(|e| classify(&e, what))
}

/// Run a batch of statements without parameters, draining every result.
pub async fn exec(client: &mut SqlClient, sql: &str, what: &str) -> Result<(), ConnectorError> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| classify(&e, what))?
        .into_results()
        .await
        .map_err(|e| classify(&e, what))?;
    Ok(())
}

/// One row's cells, in column order.
pub fn cells(row: &Row) -> Vec<Cell> {
    row.cells().map(|(_, d)| Cell::from_column(d)).collect()
}

/// The text of column `i` of `row` (`None` for NULL or a non-text value).
pub fn text_at(row: &Row, i: usize) -> Option<String> {
    row.get::<&str, _>(i).map(str::to_owned)
}

/// The columns of `table` (any user table, or a CDC change table) in order,
/// with the declared type of each.
pub async fn describe(
    client: &mut SqlClient,
    table: &TableName,
) -> Result<Vec<Column>, ConnectorError> {
    let name = table.quoted();
    let rows = rows(
        client,
        "SELECT c.name, TYPE_NAME(c.system_type_id), CAST(c.max_length AS int), \
         CAST(c.precision AS int), CAST(c.scale AS int), c.is_nullable, c.is_identity, \
         CAST(CASE WHEN c.is_computed = 1 OR c.system_type_id = 189 THEN 1 ELSE 0 END AS bit) \
         FROM sys.columns c WHERE c.object_id = OBJECT_ID(@P1) ORDER BY c.column_id",
        &[&name.as_str()],
        &format!("describing {table}"),
    )
    .await?;
    if rows.is_empty() {
        return Err(ConnectorError::fatal(format!(
            "{table} does not exist or the login cannot see it"
        )));
    }
    Ok(rows
        .iter()
        .map(|r| {
            let base: &str = r.get(1).unwrap_or("sql_variant");
            let max_length = r.get::<i32, _>(2).unwrap_or(0) as i16;
            let precision = r.get::<i32, _>(3).unwrap_or(0) as u8;
            let scale = r.get::<i32, _>(4).unwrap_or(0) as u8;
            Column {
                name: r.get::<&str, _>(0).unwrap_or_default().to_owned(),
                declared: declared_type(base, max_length, precision, scale),
                nullable: r.get(5).unwrap_or(true),
                identity: r.get(6).unwrap_or(false),
                generated: r.get(7).unwrap_or(false),
            }
        })
        .collect())
}

/// The primary key's columns of `table`, in key order (empty without one).
pub async fn primary_key(
    client: &mut SqlClient,
    table: &TableName,
) -> Result<Vec<String>, ConnectorError> {
    let name = table.quoted();
    let rows = rows(
        client,
        "SELECT c.name FROM sys.indexes i \
         JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         WHERE i.object_id = OBJECT_ID(@P1) AND i.is_primary_key = 1 ORDER BY ic.key_ordinal",
        &[&name.as_str()],
        &format!("reading the primary key of {table}"),
    )
    .await?;
    Ok(rows.iter().filter_map(|r| text_at(r, 0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Target;
    use loams_connector_sdk::{DenyLocal, InstanceKey, Scrubber, StaticSecrets};
    use serde_json::json;
    use tokio::io::AsyncReadExt;

    fn cfg(extra: serde_json::Value) -> SqlConfig {
        let mut c = json!({"host": "127.0.0.1", "database": "d", "user": "sa", "password": "pw"});
        for (k, v) in extra.as_object().expect("object") {
            c[k] = v.clone();
        }
        SqlConfig::parse(Target::SqlServer, &InstanceKey::new(1, "i"), &c).expect("config")
    }

    fn ctx() -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "i"),
            json!({}),
            Arc::new(StaticSecrets::new().with("pw", b"Loams-Test-Pass-1".to_vec())),
            Scrubber::new(),
        )
    }

    #[test]
    fn errors_are_classed_by_server_number() {
        assert!(class_of_code(1205).is_transient());
        assert!(class_of_code(40613).is_transient());
        assert_eq!(class_of_code(18456), ErrorClass::Auth);
        assert_eq!(class_of_code(2627), ErrorClass::Poison);
        assert_eq!(class_of_code(8152), ErrorClass::Poison);
        assert_eq!(class_of_code(207), ErrorClass::SchemaIncompatible);
        assert_eq!(class_of_code(208), ErrorClass::Fatal);
        let io = tiberius::error::Error::Io {
            kind: io::ErrorKind::ConnectionReset,
            message: "reset".into(),
        };
        assert!(classify(&io, "x").class.is_transient());
    }

    #[test]
    fn strict_offers_tds8_alpn_and_disable_has_no_tls() {
        let strict = tls_config(&cfg(json!({}))).expect("tls").expect("strict");
        assert_eq!(strict.alpn_protocols, vec![TDS_ALPN.to_vec()]);
        assert!(
            tls_config(&cfg(json!({"encrypt": "disable"})))
                .expect("tls")
                .is_none()
        );
        assert!(tls_config(&cfg(json!({"ca_pem": "not a pem"}))).is_err());
    }

    /// The connection is dialled by `sdk::net::connect`, so the egress check
    /// refuses a local address after DNS, before any TDS byte is sent.
    #[tokio::test]
    async fn connections_go_through_the_egress_check() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let mut c = cfg(json!({"encrypt": "disable"}));
        c.port = port;
        let mut ctx = ctx();
        ctx.egress = Arc::new(DenyLocal);
        let Err(err) = connect(&c, &ctx).await else {
            panic!("refused")
        };
        assert_eq!(err.class, ErrorClass::Fatal);
        assert!(err.message.contains("egress refused"), "{}", err.message);
    }

    /// `strict`: the first bytes on the wire are a TLS ClientHello offering
    /// ALPN `tds/8.0`, not a TDS PRELOGIN; `disable`: a PRELOGIN packet
    /// (type 0x12). Neither carries the password.
    #[tokio::test]
    async fn strict_starts_with_tls_and_disable_with_prelogin() {
        for (encrypt, want_tls) in [("strict", true), ("disable", false)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let port = listener.local_addr().expect("addr").port();
            let server = tokio::spawn(async move {
                let (mut s, _) = listener.accept().await.expect("accept");
                let mut buf = vec![0u8; 4096];
                let n = s.read(&mut buf).await.expect("read");
                buf.truncate(n);
                buf
            });
            let mut c = cfg(json!({"encrypt": encrypt, "host": "localhost"}));
            c.port = port;
            let ctx = ctx();
            let client = tokio::spawn(async move { connect(&c, &ctx).await.map(|_| ()) });
            let first = server.await.expect("server");
            client.abort();
            if want_tls {
                assert_eq!(first[0], 0x16, "a TLS handshake record");
                assert!(first.windows(TDS_ALPN.len()).any(|w| w == TDS_ALPN));
            } else {
                assert_eq!(first[0], 0x12, "a TDS PRELOGIN packet");
            }
            assert!(!first.windows(5).any(|w| w == b"Loams"));
        }
    }
}
