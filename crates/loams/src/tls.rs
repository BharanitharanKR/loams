//! TLS on the main port (LV1 plan Task 18, design §45 §6.1; the rule of MT1
//! Task 7).
//!
//! The plan's `[tls]` section is read from the environment, beside
//! `LOAMS_AUTH_*` (the `[auth]` section):
//!
//! - `LOAMS_TLS_CERT_FILE` and `LOAMS_TLS_KEY_FILE`: a PEM certificate chain
//!   and its private key. `--listen` then speaks only TLS (HTTP/2 and
//!   HTTP/1.1 by ALPN).
//! - `LOAMS_TLS_CLIENT_CA_FILE` (optional): PEM roots; every client must
//!   present a certificate they sign (mutual TLS).
//! - `LOAMS_TLS_TERMINATED_BY_PROXY=true` with `LOAMS_TLS_TRUSTED_PROXIES`
//!   (a comma-separated list of addresses or CIDR blocks): TLS ends at a
//!   reverse proxy, and `--listen` takes plain connections from those peers
//!   only, closing every other one at accept.
//!
//! [`MainListener`] serves either way. Handshakes run beside the accept
//! loop, bounded in time, so a slow client never holds the port.

use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;

pub const CERT_FILE_ENV: &str = "LOAMS_TLS_CERT_FILE";
pub const KEY_FILE_ENV: &str = "LOAMS_TLS_KEY_FILE";
pub const CLIENT_CA_FILE_ENV: &str = "LOAMS_TLS_CLIENT_CA_FILE";
pub const TERMINATED_BY_PROXY_ENV: &str = "LOAMS_TLS_TERMINATED_BY_PROXY";
pub const TRUSTED_PROXIES_ENV: &str = "LOAMS_TLS_TRUSTED_PROXIES";

/// How long a client has to finish its TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Handshakes in flight at most; further connections wait in the backlog.
const MAX_HANDSHAKES: usize = 1024;

/// An address block: `10.0.0.0/8`, `fd00::/8`, or one address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    /// `a.b.c.d`, `a.b.c.d/n`, an IPv6 address, or one with `/n`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let bad = || format!("{text:?} is not an address or a CIDR block");
        let (addr, prefix) = match text.trim().split_once('/') {
            Some((addr, prefix)) => (addr, Some(prefix)),
            None => (text.trim(), None),
        };
        let addr: IpAddr = addr.parse().map_err(|_| bad())?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p.parse::<u8>().ok().filter(|p| *p <= max).ok_or_else(bad)?,
            None => max,
        };
        Ok(Self { addr, prefix })
    }

    /// Whether `ip` is in the block (an IPv4-mapped IPv6 peer counts as its
    /// IPv4 address).
    pub fn contains(&self, ip: IpAddr) -> bool {
        let same_prefix = |a: u128, b: u128, width: u32| {
            let shift = width - u32::from(self.prefix);
            shift >= width || (a >> shift) == (b >> shift)
        };
        match (self.addr, ip.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(peer)) => {
                same_prefix(u32::from(net).into(), u32::from(peer).into(), 32)
            }
            (IpAddr::V6(net), IpAddr::V6(peer)) => same_prefix(net.into(), peer.into(), 128),
            _ => false,
        }
    }
}

impl fmt::Display for IpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

/// The main port's TLS (`[tls]`). [`TlsConfig::off`] by default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub client_ca_file: Option<PathBuf>,
    pub terminated_by_proxy: bool,
    pub trusted_proxies: Vec<IpNet>,
}

impl TlsConfig {
    /// No TLS: `--listen` speaks plain HTTP to everyone.
    pub fn off() -> Self {
        Self::default()
    }

    /// TLS with `cert_file` and `key_file`.
    pub fn files(cert_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            cert_file: Some(cert_file.into()),
            key_file: Some(key_file.into()),
            ..Self::default()
        }
    }

    /// TLS terminated by a reverse proxy at one of `trusted_proxies`.
    pub fn behind_proxy(trusted_proxies: Vec<IpNet>) -> Self {
        Self {
            terminated_by_proxy: true,
            trusted_proxies,
            ..Self::default()
        }
    }

    /// Whether `--listen` is secured: TLS here, or at a trusted proxy.
    pub fn secures(&self) -> bool {
        self.cert_file.is_some() || self.terminated_by_proxy
    }

    /// Whether any `LOAMS_TLS_*` setting is given.
    pub fn configured(&self) -> bool {
        *self != Self::default()
    }

    /// Reads the `LOAMS_TLS_*` variables through `var`, and checks they fit
    /// together. Errors name the variable.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let set = |name: &str| var(name).filter(|v| !v.trim().is_empty());
        let path = |name: &str| set(name).map(|v| PathBuf::from(v.trim()));
        let terminated_by_proxy = match set(TERMINATED_BY_PROXY_ENV).as_deref().map(str::trim) {
            None | Some("false" | "0") => false,
            Some("true" | "1") => true,
            Some(other) => {
                return Err(format!(
                    "{TERMINATED_BY_PROXY_ENV}={other:?}: expected true or false"
                ));
            }
        };
        let trusted_proxies = match set(TRUSTED_PROXIES_ENV) {
            None => Vec::new(),
            Some(list) => list
                .split(',')
                .filter(|p| !p.trim().is_empty())
                .map(IpNet::parse)
                .collect::<Result<_, _>>()
                .map_err(|e| format!("{TRUSTED_PROXIES_ENV}: {e}"))?,
        };
        let config = Self {
            cert_file: path(CERT_FILE_ENV),
            key_file: path(KEY_FILE_ENV),
            client_ca_file: path(CLIENT_CA_FILE_ENV),
            terminated_by_proxy,
            trusted_proxies,
        };
        config.check()?;
        Ok(config)
    }

    /// [`TlsConfig::from_vars`] on the process environment.
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// The settings fit together: a certificate and its key, or a proxy
    /// with the addresses it connects from; never both.
    pub fn check(&self) -> Result<(), String> {
        if self.cert_file.is_some() != self.key_file.is_some() {
            return Err(format!(
                "{CERT_FILE_ENV} and {KEY_FILE_ENV} go together: set both or neither"
            ));
        }
        if self.client_ca_file.is_some() && self.cert_file.is_none() {
            return Err(format!(
                "{CLIENT_CA_FILE_ENV} needs {CERT_FILE_ENV} and {KEY_FILE_ENV}: client \
                 certificates are checked by this engine's own TLS"
            ));
        }
        if self.terminated_by_proxy && self.cert_file.is_some() {
            return Err(format!(
                "{TERMINATED_BY_PROXY_ENV} and {CERT_FILE_ENV} exclude each other: TLS ends at \
                 the proxy or here, not both"
            ));
        }
        if self.terminated_by_proxy && self.trusted_proxies.is_empty() {
            return Err(format!(
                "{TERMINATED_BY_PROXY_ENV} needs {TRUSTED_PROXIES_ENV}: the addresses the proxy \
                 connects from (others are refused)"
            ));
        }
        if !self.terminated_by_proxy && !self.trusted_proxies.is_empty() {
            return Err(format!(
                "{TRUSTED_PROXIES_ENV} is for {TERMINATED_BY_PROXY_ENV}=true"
            ));
        }
        Ok(())
    }

    /// The acceptor of this configuration's certificate, or `None` without
    /// one. Errors name the file.
    pub fn acceptor(&self) -> Result<Option<TlsAcceptor>, String> {
        let (Some(cert_file), Some(key_file)) = (&self.cert_file, &self.key_file) else {
            return Ok(None);
        };
        use rustls::pki_types::pem::PemObject as _;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let read = |path: &Path| {
            std::fs::read(path).map_err(|e| format!("reading {}: {e}", path.display()))
        };
        let certs = |path: &Path| -> Result<Vec<CertificateDer<'static>>, String> {
            let pem = read(path)?;
            let certs = CertificateDer::pem_slice_iter(&pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("{}: {e}", path.display()))?;
            if certs.is_empty() {
                return Err(format!("{} holds no PEM certificate", path.display()));
            }
            Ok(certs)
        };
        let chain = certs(cert_file)?;
        let key = PrivateKeyDer::from_pem_slice(&read(key_file)?)
            .map_err(|e| format!("{}: no PEM private key ({e})", key_file.display()))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("TLS: {e}"))?;
        let builder = match &self.client_ca_file {
            None => builder.with_no_client_auth(),
            Some(ca_file) => {
                let mut roots = rustls::RootCertStore::empty();
                for cert in certs(ca_file)? {
                    roots
                        .add(cert)
                        .map_err(|e| format!("{}: {e}", ca_file.display()))?;
                }
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .map_err(|e| format!("{}: {e}", ca_file.display()))?;
                builder.with_client_cert_verifier(verifier)
            }
        };
        let mut server = builder
            .with_single_cert(chain, key)
            .map_err(|e| format!("{}: {e}", cert_file.display()))?;
        server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Some(TlsAcceptor::from(Arc::new(server))))
    }
}

/// A connection on the main port: plain, or TLS.
pub type MainIo = Either<TcpStream, TlsStream<TcpStream>>;

/// The main port's listener: TLS when configured, and plain connections
/// from trusted proxies only when TLS ends at a proxy.
pub struct MainListener {
    tcp: TcpListener,
    acceptor: Option<TlsAcceptor>,
    trusted_proxies: Option<Vec<IpNet>>,
    handshakes: JoinSet<Option<(TlsStream<TcpStream>, SocketAddr)>>,
}

impl fmt::Debug for MainListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MainListener")
            .field("tls", &self.acceptor.is_some())
            .field("trusted_proxies", &self.trusted_proxies)
            .finish_non_exhaustive()
    }
}

impl MainListener {
    /// `tcp` served as `config` says. Errors name the file that failed.
    pub fn new(tcp: TcpListener, config: &TlsConfig) -> Result<Self, String> {
        Ok(Self {
            tcp,
            acceptor: config.acceptor()?,
            trusted_proxies: config
                .terminated_by_proxy
                .then(|| config.trusted_proxies.clone()),
            handshakes: JoinSet::new(),
        })
    }

    fn trusted(&self, peer: SocketAddr) -> bool {
        self.trusted_proxies
            .as_ref()
            .is_none_or(|nets| nets.iter().any(|n| n.contains(peer.ip())))
    }
}

impl axum::serve::Listener for MainListener {
    type Io = MainIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (MainIo, SocketAddr) {
        loop {
            let room = self.handshakes.len() < MAX_HANDSHAKES;
            tokio::select! {
                Some(done) = self.handshakes.join_next(), if !self.handshakes.is_empty() => {
                    if let Ok(Some((tls, peer))) = done {
                        return (Either::Right(tls), peer);
                    }
                }
                (stream, peer) = <TcpListener as axum::serve::Listener>::accept(&mut self.tcp),
                    if room =>
                {
                    if !self.trusted(peer) {
                        tracing::debug!(%peer, "refused a connection from outside the trusted proxies");
                        continue;
                    }
                    match &self.acceptor {
                        None => return (Either::Left(stream), peer),
                        Some(acceptor) => {
                            let accept = acceptor.accept(stream);
                            self.handshakes.spawn(async move {
                                match tokio::time::timeout(HANDSHAKE_TIMEOUT, accept).await {
                                    Ok(Ok(tls)) => Some((tls, peer)),
                                    Ok(Err(err)) => {
                                        tracing::debug!(%peer, %err, "a TLS handshake failed");
                                        None
                                    }
                                    Err(_) => {
                                        tracing::debug!(%peer, "a TLS handshake timed out");
                                        None
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn cidr_blocks_contain_their_addresses() {
        let net = IpNet::parse("10.1.0.0/16").expect("a block");
        assert!(net.contains("10.1.200.3".parse().unwrap()));
        assert!(!net.contains("10.2.0.1".parse().unwrap()));
        assert!(net.contains("::ffff:10.1.0.9".parse().unwrap()));
        let one = IpNet::parse("192.168.1.7").expect("an address");
        assert!(one.contains("192.168.1.7".parse().unwrap()));
        assert!(!one.contains("192.168.1.8".parse().unwrap()));
        let v6 = IpNet::parse("fd00::/8").expect("a block");
        assert!(v6.contains("fd12::1".parse().unwrap()));
        assert!(!v6.contains("fe80::1".parse().unwrap()));
        assert!(!v6.contains("10.0.0.1".parse().unwrap()));
        assert!(
            IpNet::parse("0.0.0.0/0")
                .unwrap()
                .contains("8.8.8.8".parse().unwrap())
        );
        for bad in ["10.0.0.0/33", "nope", "::/129", "10.0.0.0/x"] {
            assert!(IpNet::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_variables_must_fit_together() {
        assert_eq!(TlsConfig::from_vars(vars(&[])).unwrap(), TlsConfig::off());
        let files =
            TlsConfig::from_vars(vars(&[(CERT_FILE_ENV, "/c.pem"), (KEY_FILE_ENV, "/k.pem")]))
                .unwrap();
        assert_eq!(files, TlsConfig::files("/c.pem", "/k.pem"));
        assert!(files.secures());
        let proxy = TlsConfig::from_vars(vars(&[
            (TERMINATED_BY_PROXY_ENV, "true"),
            (TRUSTED_PROXIES_ENV, "10.0.0.0/8, 127.0.0.1"),
        ]))
        .unwrap();
        assert!(proxy.secures() && proxy.trusted_proxies.len() == 2);
        for (pairs, names) in [
            (&[(CERT_FILE_ENV, "/c.pem")][..], KEY_FILE_ENV),
            (&[(CLIENT_CA_FILE_ENV, "/ca.pem")][..], CLIENT_CA_FILE_ENV),
            (
                &[(TERMINATED_BY_PROXY_ENV, "true")][..],
                TRUSTED_PROXIES_ENV,
            ),
            (
                &[(TRUSTED_PROXIES_ENV, "10.0.0.1")][..],
                TRUSTED_PROXIES_ENV,
            ),
            (
                &[(TERMINATED_BY_PROXY_ENV, "yes")][..],
                TERMINATED_BY_PROXY_ENV,
            ),
            (
                &[
                    (TERMINATED_BY_PROXY_ENV, "true"),
                    (TRUSTED_PROXIES_ENV, "10.0.0.1"),
                    (CERT_FILE_ENV, "/c.pem"),
                    (KEY_FILE_ENV, "/k.pem"),
                ][..],
                TERMINATED_BY_PROXY_ENV,
            ),
        ] {
            let err = TlsConfig::from_vars(vars(pairs)).unwrap_err();
            assert!(err.contains(names), "{err}");
        }
    }

    #[test]
    fn a_missing_certificate_file_is_named() {
        let err = TlsConfig::files("/nonexistent/c.pem", "/nonexistent/k.pem")
            .acceptor()
            .err()
            .expect("refused");
        assert!(err.contains("/nonexistent/c.pem"), "{err}");
    }
}
