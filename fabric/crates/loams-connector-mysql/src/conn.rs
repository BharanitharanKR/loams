//! The MySQL client protocol, over `mysql_common`'s packets (ruling R11.1).
//!
//! `mysql_async` dials the server itself, past `sdk::net::connect`'s egress
//! check after DNS (Review Focus 9), and cannot resume a MariaDB GTID
//! (R0.7), so this crate speaks the protocol itself: framing (3-byte length,
//! sequence id, 16 MiB continuation), the handshake with `CLIENT_SSL` and a
//! TLS upgrade (rustls with ring), `mysql_native_password`,
//! `caching_sha2_password` (fast path; full authentication over TLS, or with
//! the server's RSA key otherwise) and `mysql_clear_password` (only over TLS:
//! Aurora's IAM tokens), the text protocol for queries (several statements
//! and result sets per call), and the binlog dump commands. Every value the
//! crate sends is a hex literal or a quoted identifier
//! ([`crate::types::quote_str`]), so there are no prepared statements.
//!
//! A command interrupted half-way (a cancelled future) leaves the connection
//! `broken`; the next call refuses it as retryable, and the caller connects
//! again.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use loams_connector_sdk::{ConnectorContext, ConnectorError, ErrorClass, SecretRef, net};
use mysql_common::constants::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags};
use mysql_common::io::ParseBuf;
use mysql_common::packets::{
    AuthPlugin, AuthSwitchRequest, Column, CommonOkPacket, ErrPacket, HandshakePacket,
    HandshakeResponse, OkPacketDeserializer, SslRequest,
};
use mysql_common::proto::MySerialize;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::config::{Auth, MyConfig, TlsMode};
use crate::rds_iam;

/// The largest payload of one physical packet.
const MAX_CHUNK: usize = 0x00ff_ffff;
/// What the client tells the server it accepts.
const MAX_ALLOWED_PACKET: u32 = 64 << 20;
const UTF8MB4_GENERAL_CI: u8 = 45;

/// A server's error packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerErr {
    pub code: u16,
    pub state: String,
    pub message: String,
}

/// What can go wrong on a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MyError {
    /// The server answered with an error packet.
    Server(ServerErr),
    /// The connection failed or closed.
    Io(String),
    /// The server sent something this client does not understand.
    Protocol(String),
    /// Authentication could not proceed (an unsupported plugin, clear text
    /// without TLS).
    Auth(String),
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyError::Server(e) => write!(f, "{} ({}, {})", e.message, e.code, e.state),
            MyError::Io(e) => write!(f, "connection lost: {e}"),
            MyError::Protocol(e) => write!(f, "protocol error: {e}"),
            MyError::Auth(e) => write!(f, "authentication: {e}"),
        }
    }
}

impl From<io::Error> for MyError {
    fn from(e: io::Error) -> Self {
        MyError::Io(e.to_string())
    }
}

pub type MyResult<T> = Result<T, MyError>;

/// The class of a server error code (MySQL and MariaDB server error
/// reference).
pub fn class_of_code(code: u16) -> ErrorClass {
    match code {
        // Access denied (password, database, user), plugin refusals,
        // expired passwords.
        1044 | 1045 | 1698 | 1251 | 1862 | 1820 | 2049 => ErrorClass::Auth,
        // The binlog position is gone (purged files, an unknown GTID).
        1236 => ErrorClass::PositionLost,
        // Deadlock, lock wait timeout, interrupted, shutting down, lost
        // connection, read-only for now.
        1205 | 1213 | 1317 | 1053 | 2013 | 2006 | 1927 | 1290 | 1836 | 3100 => {
            ErrorClass::Retryable { after: None }
        }
        // Too many connections, out of resources.
        1040 | 1041 | 1203 | 1226 | 1135 => ErrorClass::Throttled {
            after: std::time::Duration::from_secs(1),
        },
        // This record's data: out of range, truncated, bad value, too long,
        // NOT NULL, invalid JSON, foreign keys, duplicate on another unique
        // key, check constraints.
        1048 | 1062 | 1263 | 1264 | 1265 | 1292 | 1366 | 1367 | 1406 | 1451 | 1452 | 1525
        | 3140 | 3819 | 4025 | 1690 => ErrorClass::Poison,
        // Unknown column: the target's schema moved.
        1054 => ErrorClass::SchemaIncompatible,
        _ => ErrorClass::Fatal,
    }
}

impl MyError {
    pub fn code(&self) -> Option<u16> {
        match self {
            MyError::Server(e) => Some(e.code),
            _ => None,
        }
    }

    pub fn class(&self) -> ErrorClass {
        match self {
            MyError::Server(e) => class_of_code(e.code),
            MyError::Io(_) => ErrorClass::Retryable { after: None },
            MyError::Protocol(_) => ErrorClass::Fatal,
            MyError::Auth(_) => ErrorClass::Auth,
        }
    }

    /// As a [`ConnectorError`]: `what` and the server's message, never the
    /// configuration.
    pub fn to_connector(&self, what: &str) -> ConnectorError {
        ConnectorError::new(self.class(), format!("{what}: {self}"))
    }
}

// ------------------------------------------------------------------ TLS --

/// `tls.verify_full: false`: encrypt, accept any certificate, still
/// checking the handshake signatures.
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

/// The rustls configuration for `cfg.tls` (`None` when disabled).
pub fn tls_config(cfg: &MyConfig) -> Result<Option<Arc<ClientConfig>>, ConnectorError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| ConnectorError::fatal(format!("TLS: {e}")))?;
    Ok(match cfg.tls {
        TlsMode::Disabled => None,
        TlsMode::Require => Some(Arc::new(
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
                .with_no_client_auth(),
        )),
        TlsMode::VerifyFull => {
            let mut roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            if let Some(pem) = &cfg.ca_pem {
                use rustls::pki_types::pem::PemObject;
                for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                    let cert = cert
                        .map_err(|e| ConnectorError::fatal(format!("config: tls.ca_pem: {e}")))?;
                    roots
                        .add(cert)
                        .map_err(|e| ConnectorError::fatal(format!("config: tls.ca_pem: {e}")))?;
                }
            }
            Some(Arc::new(
                builder.with_root_certificates(roots).with_no_client_auth(),
            ))
        }
    })
}

/// A connection's byte stream: plain TCP, or TLS over it.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    /// Between the TCP stream's move into TLS and TLS's return.
    Closed,
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "the stream is closed")
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            Stream::Closed => Poll::Ready(Err(closed())),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            Stream::Closed => Poll::Ready(Err(closed())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            Stream::Closed => Poll::Ready(Err(closed())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            Stream::Closed => Poll::Ready(Ok(())),
        }
    }
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Stream::Plain(_) => "Stream::Plain",
            Stream::Tls(_) => "Stream::Tls",
            Stream::Closed => "Stream::Closed",
        })
    }
}

// ------------------------------------------------------------- results --

/// A result set's column, as the server describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    pub name: String,
    pub column_type: ColumnType,
    pub flags: ColumnFlags,
    pub charset: u16,
}

/// A text-protocol row: each value's bytes, `None` for NULL.
pub type TextRow = Vec<Option<Vec<u8>>>;

/// One statement's result: rows (possibly none), or an OK.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<Col>,
    pub rows: Vec<TextRow>,
    pub affected_rows: u64,
}

impl ResultSet {
    /// The first column of the first row, as text.
    pub fn scalar(&self) -> Option<String> {
        self.rows
            .first()
            .and_then(|r| r.first().cloned().flatten())
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    /// The text of `row`'s value in the column called `name`.
    pub fn get(&self, row: usize, name: &str) -> Option<String> {
        let i = self
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))?;
        self.rows
            .get(row)?
            .get(i)?
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
    }
}

/// A length-encoded integer at the front of `b`, and the bytes it took.
pub fn lenenc_int(b: &[u8]) -> Option<(u64, usize)> {
    match *b.first()? {
        x @ 0..=0xfa => Some((u64::from(x), 1)),
        0xfc => Some((
            u64::from(u16::from_le_bytes(b.get(1..3)?.try_into().ok()?)),
            3,
        )),
        0xfd => {
            let s = b.get(1..4)?;
            Some((
                u64::from(s[0]) | u64::from(s[1]) << 8 | u64::from(s[2]) << 16,
                4,
            ))
        }
        0xfe => Some((u64::from_le_bytes(b.get(1..9)?.try_into().ok()?), 9)),
        _ => None,
    }
}

/// A text-protocol row of `n` values.
pub fn parse_text_row(p: &[u8], n: usize) -> MyResult<TextRow> {
    let mut out = Vec::with_capacity(n);
    let mut at = 0;
    for _ in 0..n {
        if p.get(at) == Some(&0xfb) {
            out.push(None);
            at += 1;
            continue;
        }
        let (len, used) = lenenc_int(&p[at.min(p.len())..])
            .ok_or_else(|| MyError::Protocol("a truncated row".into()))?;
        let start = at + used;
        let end = start
            .checked_add(usize::try_from(len).unwrap_or(usize::MAX))
            .filter(|e| *e <= p.len())
            .ok_or_else(|| MyError::Protocol("a truncated row".into()))?;
        out.push(Some(p[start..end].to_vec()));
        at = end;
    }
    Ok(out)
}

fn parse_err_with(p: &[u8], caps: CapabilityFlags) -> Option<ServerErr> {
    match ParseBuf(p).parse::<ErrPacket>(caps) {
        Ok(ErrPacket::Error(e)) => Some(ServerErr {
            code: e.error_code(),
            state: e
                .sql_state_ref()
                .map(|s| s.as_str().into_owned())
                .unwrap_or_default(),
            message: e.message_str().into_owned(),
        }),
        _ => None,
    }
}

/// An error packet after the handshake (protocol 4.1, with a SQL state).
pub fn parse_err(p: &[u8]) -> Option<ServerErr> {
    parse_err_with(p, CapabilityFlags::CLIENT_PROTOCOL_41)
}

/// An EOF packet (`0xFE`, shorter than 9 bytes): its status flags.
fn eof_status(p: &[u8]) -> Option<StatusFlags> {
    (p.first() == Some(&0xfe) && p.len() < 9).then(|| {
        let s = p.get(3..5).map_or(0, |b| u16::from_le_bytes([b[0], b[1]]));
        StatusFlags::from_bits_truncate(s)
    })
}

// ---------------------------------------------------------- connection --

/// Who to log in as.
#[derive(Clone)]
pub struct Login {
    pub user: String,
    pub password: Option<Vec<u8>>,
    pub database: Option<String>,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("user", &self.user)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

/// One connection. See the module documentation.
pub struct MyConn {
    stream: Stream,
    rbuf: BytesMut,
    seq: u8,
    caps: CapabilityFlags,
    tls: bool,
    broken: bool,
    /// The server's version string (`8.4.10`, `11.8.8-MariaDB-…`).
    pub server_version: String,
    /// MariaDB (from the version string).
    pub mariadb: bool,
    pub connection_id: u32,
}

impl std::fmt::Debug for MyConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MyConn")
            .field("server_version", &self.server_version)
            .field("connection_id", &self.connection_id)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

fn client_caps(database: bool, tls: bool) -> CapabilityFlags {
    let mut c = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CapabilityFlags::CLIENT_LONG_PASSWORD
        | CapabilityFlags::CLIENT_LONG_FLAG
        | CapabilityFlags::CLIENT_TRANSACTIONS
        | CapabilityFlags::CLIENT_MULTI_STATEMENTS
        | CapabilityFlags::CLIENT_MULTI_RESULTS;
    if database {
        c |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
    }
    if tls {
        c |= CapabilityFlags::CLIENT_SSL;
    }
    c
}

impl MyConn {
    /// Handshake and authenticate over `tcp`; with `tls`, upgrade first
    /// (`host` is the name the certificate must carry).
    pub async fn handshake(
        tcp: TcpStream,
        host: &str,
        tls: Option<Arc<ClientConfig>>,
        login: &Login,
    ) -> MyResult<Self> {
        let mut conn = MyConn {
            stream: Stream::Plain(tcp),
            rbuf: BytesMut::with_capacity(16 << 10),
            seq: 0,
            caps: CapabilityFlags::empty(),
            tls: false,
            broken: false,
            server_version: String::new(),
            mariadb: false,
            connection_id: 0,
        };
        let first = conn.read_packet().await?;
        if first.first() == Some(&0xff) {
            return Err(conn.err_packet(&first));
        }
        let hs = ParseBuf(&first)
            .parse::<HandshakePacket>(())
            .map_err(|e| MyError::Protocol(format!("handshake: {e}")))?;
        conn.server_version = hs.server_version_str().into_owned();
        conn.mariadb = conn.server_version.contains("MariaDB");
        conn.connection_id = hs.connection_id();
        let mut nonce = hs.nonce();
        nonce.resize(20, 0);
        conn.caps = hs.capabilities() & client_caps(login.database.is_some(), tls.is_some());
        let version = hs
            .maria_db_server_version_parsed()
            .or_else(|| hs.server_version_parsed())
            .unwrap_or((5, 7, 0));
        let mut plugin = match hs.auth_plugin() {
            Some(AuthPlugin::CachingSha2Password) => AuthPlugin::CachingSha2Password,
            _ => AuthPlugin::MysqlNativePassword,
        };
        if let Some(config) = tls {
            if !hs.capabilities().contains(CapabilityFlags::CLIENT_SSL) {
                return Err(MyError::Auth(
                    "the server does not offer TLS, which the instance requires".into(),
                ));
            }
            let mut buf = Vec::new();
            SslRequest::new(conn.caps, MAX_ALLOWED_PACKET, UTF8MB4_GENERAL_CI).serialize(&mut buf);
            conn.write_packet(&buf).await?;
            let name = ServerName::try_from(host.to_owned())
                .map_err(|e| MyError::Auth(format!("{host} is not a TLS server name: {e}")))?;
            let Stream::Plain(tcp) = std::mem::replace(&mut conn.stream, Stream::Closed) else {
                return Err(MyError::Protocol("TLS twice".into()));
            };
            let tls = tokio_rustls::TlsConnector::from(config)
                .connect(name, tcp)
                .await
                .map_err(|e| MyError::Io(format!("TLS with {host}: {e}")))?;
            conn.stream = Stream::Tls(Box::new(tls));
            conn.tls = true;
        }
        let password = login
            .password
            .as_deref()
            .map(|p| {
                std::str::from_utf8(p)
                    .map(str::to_owned)
                    .map_err(|_| MyError::Auth("the password is not UTF-8".into()))
            })
            .transpose()?;
        let data = plugin.gen_data(password.as_deref(), &nonce);
        let mut buf = Vec::new();
        HandshakeResponse::new(
            data.as_deref(),
            version,
            Some(login.user.as_bytes()),
            login.database.as_deref().map(str::as_bytes),
            Some(plugin.borrow()),
            conn.caps,
            None,
            MAX_ALLOWED_PACKET,
        )
        .serialize(&mut buf);
        conn.write_packet(&buf).await?;
        conn.authenticate(&mut plugin, &mut nonce, password.as_deref())
            .await?;
        Ok(conn)
    }

    async fn authenticate(
        &mut self,
        plugin: &mut AuthPlugin<'static>,
        nonce: &mut Vec<u8>,
        password: Option<&str>,
    ) -> MyResult<()> {
        let mut switched = false;
        loop {
            let p = self.read_packet().await?;
            match p.first() {
                Some(0x00) => return Ok(()),
                Some(0xff) => return Err(self.err_packet(&p)),
                Some(0xfe) if !switched => {
                    switched = true;
                    let req = ParseBuf(&p)
                        .parse::<AuthSwitchRequest>(())
                        .map_err(|e| MyError::Protocol(format!("auth switch: {e}")))?;
                    *nonce = req.plugin_data().to_vec();
                    if nonce.last() == Some(&0) {
                        nonce.pop();
                    }
                    *plugin = match req.auth_plugin() {
                        AuthPlugin::MysqlNativePassword => AuthPlugin::MysqlNativePassword,
                        AuthPlugin::CachingSha2Password => AuthPlugin::CachingSha2Password,
                        AuthPlugin::MysqlClearPassword if self.tls => {
                            AuthPlugin::MysqlClearPassword
                        }
                        AuthPlugin::MysqlClearPassword => {
                            return Err(MyError::Auth(
                                "the server asks for mysql_clear_password without TLS".into(),
                            ));
                        }
                        other => {
                            return Err(MyError::Auth(format!(
                                "unsupported authentication plugin {}",
                                String::from_utf8_lossy(other.as_bytes())
                            )));
                        }
                    };
                    let mut buf = Vec::new();
                    if let Some(d) = plugin.gen_data(password, nonce) {
                        d.serialize(&mut buf);
                    }
                    self.write_packet(&buf).await?;
                }
                Some(0x01) if *plugin == AuthPlugin::CachingSha2Password => match p.get(1) {
                    // Fast authentication succeeded; an OK follows.
                    Some(0x03) => {}
                    // Full authentication.
                    Some(0x04) => {
                        let mut pass = password.unwrap_or_default().as_bytes().to_vec();
                        pass.push(0);
                        if self.tls {
                            self.write_packet(&pass).await?;
                        } else {
                            self.write_packet(&[0x02]).await?;
                            let key = self.read_packet().await?;
                            if key.first() == Some(&0xff) {
                                return Err(self.err_packet(&key));
                            }
                            let pem = key.get(1..).unwrap_or_default().to_vec();
                            for (i, b) in pass.iter_mut().enumerate() {
                                *b ^= nonce[i % nonce.len().max(1)];
                            }
                            // mysql_common panics on a malformed key; a
                            // server's bad answer must not take the task down.
                            let enc = std::panic::catch_unwind(|| {
                                mysql_common::crypto::encrypt(&pass, &pem)
                            })
                            .map_err(|_| {
                                MyError::Auth("the server's RSA public key is unusable".into())
                            })?;
                            self.write_packet(&enc).await?;
                        }
                    }
                    _ => return Err(MyError::Protocol("unexpected auth data".into())),
                },
                _ => {
                    return Err(MyError::Protocol(format!(
                        "unexpected packet during authentication (0x{:02x})",
                        p.first().copied().unwrap_or(0)
                    )));
                }
            }
        }
    }

    fn err_packet(&self, p: &[u8]) -> MyError {
        match parse_err_with(p, self.caps) {
            Some(e) => MyError::Server(e),
            None => MyError::Protocol("a malformed error packet".into()),
        }
    }

    async fn fill(&mut self, n: usize) -> MyResult<()> {
        while self.rbuf.len() < n {
            let read = self.stream.read_buf(&mut self.rbuf).await?;
            if read == 0 {
                return Err(MyError::Io("the server closed the connection".into()));
            }
        }
        Ok(())
    }

    /// One logical packet (continuations joined).
    pub async fn read_packet(&mut self) -> MyResult<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            self.fill(4).await?;
            let len = usize::from(self.rbuf[0])
                | usize::from(self.rbuf[1]) << 8
                | usize::from(self.rbuf[2]) << 16;
            self.fill(4 + len).await?;
            self.seq = self.rbuf[3].wrapping_add(1);
            self.rbuf.advance(4);
            out.extend_from_slice(&self.rbuf[..len]);
            self.rbuf.advance(len);
            if len < MAX_CHUNK {
                return Ok(out);
            }
        }
    }

    /// One logical packet, split into chunks as needed.
    pub async fn write_packet(&mut self, payload: &[u8]) -> MyResult<()> {
        let mut out = Vec::with_capacity(payload.len() + 8);
        let mut rest = payload;
        loop {
            let n = rest.len().min(MAX_CHUNK);
            out.extend_from_slice(&[n as u8, (n >> 8) as u8, (n >> 16) as u8, self.seq]);
            self.seq = self.seq.wrapping_add(1);
            out.extend_from_slice(&rest[..n]);
            rest = &rest[n..];
            if n < MAX_CHUNK {
                break;
            }
        }
        self.stream.write_all(&out).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Start a command: a fresh sequence, and the connection marked busy
    /// until its answer is read.
    pub async fn command(&mut self, body: &[u8]) -> MyResult<()> {
        if self.broken {
            return Err(MyError::Io("an earlier command was interrupted".into()));
        }
        self.broken = true;
        self.seq = 0;
        self.write_packet(body).await
    }

    /// The answer has been read in full.
    pub fn done(&mut self) {
        self.broken = false;
    }

    pub fn is_broken(&self) -> bool {
        self.broken
    }

    pub fn is_tls(&self) -> bool {
        self.tls
    }

    /// Run `sql` (one or more statements) and return every result.
    pub async fn query_all(&mut self, sql: &str) -> MyResult<Vec<ResultSet>> {
        let mut body = Vec::with_capacity(sql.len() + 1);
        body.push(0x03);
        body.extend_from_slice(sql.as_bytes());
        self.command(&body).await?;
        let mut out = Vec::new();
        loop {
            let p = self.read_packet().await?;
            let more = match p.first() {
                Some(0x00) => {
                    let ok = ParseBuf(&p)
                        .parse::<OkPacketDeserializer<CommonOkPacket>>(self.caps)
                        .map_err(|e| MyError::Protocol(format!("OK packet: {e}")))?
                        .into_inner();
                    out.push(ResultSet {
                        affected_rows: ok.affected_rows(),
                        ..ResultSet::default()
                    });
                    ok.status_flags()
                        .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
                }
                Some(0xff) => {
                    let e = self.err_packet(&p);
                    self.done();
                    return Err(e);
                }
                Some(0xfb) => {
                    // LOCAL INFILE: refuse by sending no data.
                    self.write_packet(&[]).await?;
                    let _ = self.read_packet().await?;
                    self.done();
                    return Err(MyError::Protocol("LOCAL INFILE is not allowed".into()));
                }
                _ => {
                    let (n, _) = lenenc_int(&p)
                        .ok_or_else(|| MyError::Protocol("a bad column count".into()))?;
                    let n = usize::try_from(n)
                        .map_err(|_| MyError::Protocol("a bad column count".into()))?;
                    let mut columns = Vec::with_capacity(n);
                    for _ in 0..n {
                        let c = self.read_packet().await?;
                        let col = ParseBuf(&c)
                            .parse::<Column>(())
                            .map_err(|e| MyError::Protocol(format!("column: {e}")))?;
                        columns.push(Col {
                            name: col.name_str().into_owned(),
                            column_type: col.column_type(),
                            flags: col.flags(),
                            charset: col.character_set(),
                        });
                    }
                    let eof = self.read_packet().await?;
                    if eof_status(&eof).is_none() {
                        return Err(MyError::Protocol("no EOF after the columns".into()));
                    }
                    let mut rows = Vec::new();
                    let status = loop {
                        let r = self.read_packet().await?;
                        if let Some(s) = eof_status(&r) {
                            break s;
                        }
                        if r.first() == Some(&0xff) {
                            let e = self.err_packet(&r);
                            self.done();
                            return Err(e);
                        }
                        rows.push(parse_text_row(&r, n)?);
                    };
                    out.push(ResultSet {
                        columns,
                        rows,
                        affected_rows: 0,
                    });
                    status.contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
                }
            };
            if !more {
                break;
            }
        }
        self.done();
        Ok(out)
    }

    /// Run `sql`; the last result.
    pub async fn query(&mut self, sql: &str) -> MyResult<ResultSet> {
        Ok(self.query_all(sql).await?.pop().unwrap_or_default())
    }

    /// Run `sql` for its effect; the affected rows of the last statement.
    pub async fn exec(&mut self, sql: &str) -> MyResult<u64> {
        Ok(self.query(sql).await?.affected_rows)
    }

    /// `COM_QUIT`, best effort.
    pub async fn quit(mut self) {
        if !self.broken {
            let _ = self.command(&[0x01]).await;
        }
        let _ = self.stream.shutdown().await;
    }
}

// -------------------------------------------------------------- connect --

/// The password for a connection: the named secret, or a fresh RDS IAM
/// token (15 minutes, minted per connection).
pub async fn password(
    cfg: &MyConfig,
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
            let get = |name: String| {
                let secrets = ctx.secrets.clone();
                async move {
                    let s = secrets.resolve(&SecretRef::new(&name)).await?;
                    s.expose_str()
                        .map(str::to_owned)
                        .ok_or_else(|| ConnectorError::auth("an AWS credential is not UTF-8"))
                }
            };
            let key = get(aws.access_key_id.clone()).await?;
            let secret = get(aws.secret_access_key.clone()).await?;
            let token = match &aws.session_token {
                Some(t) => Some(get(t.clone()).await?),
                None => None,
            };
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

/// A connection for `cfg`, through `sdk::net::connect` (egress after DNS),
/// in UTC (`time_zone = '+00:00'`) and `utf8mb4`.
pub async fn connect(cfg: &MyConfig, ctx: &ConnectorContext) -> Result<MyConn, ConnectorError> {
    let login = Login {
        user: cfg.user.clone(),
        password: password(cfg, ctx).await?,
        database: None,
    };
    let tcp = net::connect(&cfg.host, cfg.port, ctx.egress.as_ref()).await?;
    let mut conn = MyConn::handshake(tcp, &cfg.host, tls_config(cfg)?, &login)
        .await
        .map_err(|e| e.to_connector(&format!("connecting to {}:{}", cfg.host, cfg.port)))?;
    conn.query_all(
        "SET NAMES utf8mb4; SET time_zone = '+00:00'; \
         SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')",
    )
    .await
    .map_err(|e| e.to_connector("setting up the session"))?;
    Ok(conn)
}

#[cfg(test)]
pub(crate) mod fake {
    //! A loopback server that speaks just enough of the protocol for the
    //! client's unit tests.
    use super::*;
    use mysql_common::scramble::scramble_native;
    use tokio::net::TcpListener;

    pub const NONCE: [u8; 20] = *b"abcdefghij0123456789";

    pub struct FakeServer {
        pub stream: TcpStream,
        pub seq: u8,
    }

    impl FakeServer {
        pub async fn read(&mut self) -> Vec<u8> {
            let mut h = [0u8; 4];
            self.stream.read_exact(&mut h).await.expect("header");
            let len = usize::from(h[0]) | usize::from(h[1]) << 8 | usize::from(h[2]) << 16;
            self.seq = h[3].wrapping_add(1);
            let mut b = vec![0u8; len];
            self.stream.read_exact(&mut b).await.expect("body");
            b
        }

        pub async fn send(&mut self, payload: &[u8]) {
            let n = payload.len();
            let mut out = vec![n as u8, (n >> 8) as u8, (n >> 16) as u8, self.seq];
            self.seq = self.seq.wrapping_add(1);
            out.extend_from_slice(payload);
            self.stream.write_all(&out).await.expect("send");
        }

        pub async fn ok(&mut self, affected: u8) {
            self.send(&[0x00, affected, 0, 0x02, 0, 0, 0]).await;
        }

        pub async fn eof(&mut self) {
            self.send(&[0xfe, 0, 0, 0x02, 0]).await;
        }

        pub async fn column(&mut self, name: &str, ty: ColumnType) {
            let mut b = Vec::new();
            Column::new(ty).with_name(name.as_bytes()).serialize(&mut b);
            self.send(&b).await;
        }

        pub async fn row(&mut self, values: &[Option<&str>]) {
            let mut b = Vec::new();
            for v in values {
                match v {
                    None => b.push(0xfb),
                    Some(s) => {
                        b.push(s.len() as u8);
                        b.extend_from_slice(s.as_bytes());
                    }
                }
            }
            self.send(&b).await;
        }
    }

    /// Accept one client, send a handshake, check `password` with
    /// `mysql_native_password`, answer OK.
    pub async fn serve(password: &'static str) -> (u16, tokio::task::JoinHandle<FakeServer>) {
        serve_as("8.4.10-fake", password).await
    }

    /// One answer to a statement.
    #[derive(Debug, Clone)]
    pub enum Reply {
        Ok(u8),
        Err(u16, &'static str),
        Rows(Vec<&'static str>, Vec<Vec<Option<String>>>),
    }

    /// What the scripted server saw: each command packet.
    pub type Seen = std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>;

    /// A server that logs in anyone, then answers each `COM_QUERY` with
    /// `answer(sql)` (one reply per statement), `COM_REGISTER_SLAVE` with OK,
    /// and a binlog dump with the `dump` packets (then holds the line).
    pub async fn script(
        version: &'static str,
        answer: impl Fn(&str) -> Vec<Reply> + Send + Sync + 'static,
        dump: Vec<Vec<u8>>,
    ) -> (u16, Seen) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let seen: Seen = Default::default();
        let log = seen.clone();
        tokio::spawn(async move {
            let answer = std::sync::Arc::new(answer);
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let answer = answer.clone();
                let log = log.clone();
                let dump = dump.clone();
                tokio::spawn(async move {
                    let mut s = handshake_any(stream, version).await;
                    loop {
                        let mut h = [0u8; 4];
                        if s.stream.read_exact(&mut h).await.is_err() {
                            return;
                        }
                        let len =
                            usize::from(h[0]) | usize::from(h[1]) << 8 | usize::from(h[2]) << 16;
                        s.seq = h[3].wrapping_add(1);
                        let mut p = vec![0u8; len];
                        if s.stream.read_exact(&mut p).await.is_err() {
                            return;
                        }
                        log.lock().expect("log").push(p.clone());
                        match p.first() {
                            Some(0x03) => {
                                let sql = String::from_utf8_lossy(&p[1..]).into_owned();
                                let replies = answer(&sql);
                                let n = replies.len();
                                for (i, r) in replies.into_iter().enumerate() {
                                    let more = if i + 1 < n { 0x08 } else { 0 };
                                    s.reply(r, more).await;
                                }
                            }
                            Some(0x15) => s.ok(0).await,
                            Some(0x12 | 0x1e) => {
                                for d in &dump {
                                    s.send(d).await;
                                }
                                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                            }
                            _ => return,
                        }
                    }
                });
            }
        });
        (port, seen)
    }

    async fn handshake_any(stream: TcpStream, version: &'static str) -> FakeServer {
        let mut s = FakeServer { stream, seq: 0 };
        let caps = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS;
        let mut nonce2 = NONCE[8..].to_vec();
        nonce2.push(0);
        let hs = HandshakePacket::new(
            10,
            version.as_bytes(),
            7,
            NONCE[..8].try_into().expect("8"),
            Some(nonce2),
            caps,
            UTF8MB4_GENERAL_CI,
            StatusFlags::SERVER_STATUS_AUTOCOMMIT,
            Some(&b"mysql_native_password"[..]),
        );
        let mut b = Vec::new();
        hs.serialize(&mut b);
        s.send(&b).await;
        let _ = s.read().await;
        s.ok(0).await;
        s
    }

    impl FakeServer {
        async fn reply(&mut self, r: Reply, more: u8) {
            match r {
                Reply::Ok(n) => self.send(&[0x00, n, 0, 0x02 | more, 0, 0, 0]).await,
                Reply::Err(code, msg) => {
                    let mut b = vec![0xff];
                    b.extend(code.to_le_bytes());
                    b.extend(b"#HY000");
                    b.extend(msg.as_bytes());
                    self.send(&b).await;
                }
                Reply::Rows(cols, rows) => {
                    self.send(&[cols.len() as u8]).await;
                    for c in &cols {
                        self.column(c, ColumnType::MYSQL_TYPE_VAR_STRING).await;
                    }
                    self.eof().await;
                    for r in &rows {
                        let vals: Vec<Option<&str>> = r.iter().map(|v| v.as_deref()).collect();
                        self.row(&vals).await;
                    }
                    self.send(&[0xfe, 0, 0, 0x02 | more, 0]).await;
                }
            }
        }
    }

    /// One row of text values.
    pub fn row(vals: &[&str]) -> Vec<Option<String>> {
        vals.iter().map(|v| Some((*v).to_owned())).collect()
    }

    async fn serve_as(
        version: &'static str,
        password: &'static str,
    ) -> (u16, tokio::task::JoinHandle<FakeServer>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut s = FakeServer { stream, seq: 0 };
            let caps = CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_SECURE_CONNECTION
                | CapabilityFlags::CLIENT_PLUGIN_AUTH
                | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
                | CapabilityFlags::CLIENT_LONG_PASSWORD
                | CapabilityFlags::CLIENT_TRANSACTIONS
                | CapabilityFlags::CLIENT_MULTI_STATEMENTS
                | CapabilityFlags::CLIENT_MULTI_RESULTS;
            let mut nonce2 = NONCE[8..].to_vec();
            nonce2.push(0);
            let hs = HandshakePacket::new(
                10,
                version.as_bytes(),
                42,
                NONCE[..8].try_into().expect("8"),
                Some(nonce2),
                caps,
                UTF8MB4_GENERAL_CI,
                StatusFlags::SERVER_STATUS_AUTOCOMMIT,
                Some(&b"mysql_native_password"[..]),
            );
            let mut b = Vec::new();
            hs.serialize(&mut b);
            s.send(&b).await;
            let resp = s.read().await;
            let want = scramble_native(&NONCE, password.as_bytes()).expect("scramble");
            let found = resp.windows(20).any(|w| w == want);
            if found {
                s.ok(0).await;
            } else {
                s.send(b"\xff\x15\x04#28000Access denied for user").await;
            }
            s
        });
        (port, task)
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;

    fn login(p: &str) -> Login {
        Login {
            user: "loams".into(),
            password: Some(p.as_bytes().to_vec()),
            database: None,
        }
    }

    #[test]
    fn lenenc_and_rows() {
        assert_eq!(lenenc_int(&[0x05]), Some((5, 1)));
        assert_eq!(lenenc_int(&[0xfc, 0x00, 0x01]), Some((256, 3)));
        assert_eq!(lenenc_int(&[0xfd, 1, 0, 1]), Some((65537, 4)));
        assert_eq!(lenenc_int(&[0xfc, 0x00]), None);
        let row = parse_text_row(&[0x02, b'h', b'i', 0xfb, 0x00], 3).expect("row");
        assert_eq!(row, vec![Some(b"hi".to_vec()), None, Some(Vec::new())]);
        assert!(parse_text_row(&[0x05, b'x'], 1).is_err());
    }

    #[test]
    fn codes_map_to_classes() {
        assert_eq!(class_of_code(1045), ErrorClass::Auth);
        assert_eq!(class_of_code(1236), ErrorClass::PositionLost);
        assert_eq!(class_of_code(1213), ErrorClass::Retryable { after: None });
        assert_eq!(class_of_code(1366), ErrorClass::Poison);
        assert_eq!(class_of_code(1054), ErrorClass::SchemaIncompatible);
        assert!(matches!(class_of_code(1040), ErrorClass::Throttled { .. }));
        assert_eq!(class_of_code(1146), ErrorClass::Fatal);
    }

    #[tokio::test]
    async fn native_password_login_and_a_result_set() {
        let (port, server) = serve("s3cret").await;
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
        let mut conn = MyConn::handshake(tcp, "127.0.0.1", None, &login("s3cret"))
            .await
            .expect("login");
        assert_eq!(conn.server_version, "8.4.10-fake");
        assert!(!conn.mariadb);
        let mut s = server.await.expect("server");
        let answer = tokio::spawn(async move {
            let q = s.read().await;
            assert_eq!(&q[..], b"\x03SELECT id, v FROM t");
            s.send(&[2]).await;
            s.column("id", ColumnType::MYSQL_TYPE_LONGLONG).await;
            s.column("v", ColumnType::MYSQL_TYPE_VAR_STRING).await;
            s.eof().await;
            s.row(&[Some("1"), Some("a")]).await;
            s.row(&[Some("2"), None]).await;
            s.eof().await;
            let q = s.read().await;
            assert_eq!(q[0], 0x03);
            s.send(b"\xff\x7a\x04#42S02Table 'x' doesn't exist").await;
            s
        });
        let rs = conn.query("SELECT id, v FROM t").await.expect("query");
        assert_eq!(rs.columns.len(), 2);
        assert_eq!(rs.columns[1].name, "v");
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.get(1, "v"), None);
        assert_eq!(rs.get(0, "ID"), Some("1".into()));
        let err = conn.query("SELECT 1").await.expect_err("error packet");
        assert_eq!(err.code(), Some(1146));
        assert_eq!(err.class(), ErrorClass::Fatal);
        assert!(!conn.is_broken(), "an error answer completes the command");
        answer.await.expect("answer");
    }

    #[tokio::test]
    async fn a_wrong_password_is_an_auth_error() {
        let (port, server) = serve("right").await;
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
        let err = MyConn::handshake(tcp, "127.0.0.1", None, &login("wrong"))
            .await
            .expect_err("denied");
        assert_eq!(err.code(), Some(1045));
        assert_eq!(err.to_connector("connecting").class, ErrorClass::Auth);
        assert!(!err.to_connector("connecting").message.contains("wrong"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn an_interrupted_command_breaks_the_connection() {
        let (port, server) = serve("p").await;
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
        let mut conn = MyConn::handshake(tcp, "127.0.0.1", None, &login("p"))
            .await
            .expect("login");
        let _s = server.await.expect("server");
        // The server never answers: the query times out half-way.
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            conn.query("SELECT SLEEP(10)"),
        )
        .await;
        assert!(r.is_err());
        assert!(conn.is_broken());
        let err = conn.query("SELECT 1").await.expect_err("broken");
        assert_eq!(err.class(), ErrorClass::Retryable { after: None });
    }
}
