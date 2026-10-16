//! Shared test helpers: a CA, a fake PgDog (it does TLS only after
//! `SSLRequest` and negotiates no ALPN, as pinned PgDog does), clients, and
//! a gateway on loopback ports.
#![allow(clippy::unwrap_used, dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use loams_gateway::routes::with_generation;
use loams_gateway::{Gateway, GatewayConfig};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::client::TlsStream;

pub const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
pub const WILDCARD: &str = "*.gw.pg.test";

/// A test CA.
pub struct Ca {
    key: KeyPair,
    params: CertificateParams,
    pub pem: String,
    /// One leaf for [`WILDCARD`], shared by every fake PgDog of this CA and,
    /// by default, by the gateway (SCRAM channel binding needs the gateway
    /// to present PgDog's certificate).
    pub wildcard: (String, String),
}

impl Ca {
    pub fn new(name: &str) -> Ca {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let pem = params.clone().self_signed(&key).unwrap().pem();
        let mut ca = Ca {
            key,
            params,
            pem,
            wildcard: (String::new(), String::new()),
        };
        ca.wildcard = ca.leaf(&[WILDCARD]);
        ca
    }

    /// A leaf for `names`: (certificate PEM, key PEM).
    pub fn leaf(&self, names: &[&str]) -> (String, String) {
        let k = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>())
            .unwrap()
            .signed_by(&k, &rcgen::Issuer::new(self.params.clone(), &self.key))
            .unwrap();
        (cert.pem(), k.serialize_pem())
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A stand-in for PgDog: `SSLRequest` → `S` → TLS (no ALPN) → answers each
/// read with `<tag>:<bytes>`. Plaintext cancels are recorded.
pub struct FakePgDog {
    pub addr: SocketAddr,
    pub cancels: Arc<Mutex<Vec<[u8; 16]>>>,
    pub connections: Arc<Mutex<usize>>,
}

pub async fn fake_pgdog(ca: &Ca, names: &[&str], tag: &'static str) -> FakePgDog {
    fake_pgdog_on(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ca,
        names,
        tag,
        b'S',
    )
    .await
}

/// A fake PgDog that answers `SSLRequest` with `answer`.
pub async fn fake_pgdog_on(
    listener: TcpListener,
    ca: &Ca,
    names: &[&str],
    tag: &'static str,
    answer: u8,
) -> FakePgDog {
    let (cert, key) = if names == [WILDCARD] {
        ca.wildcard.clone()
    } else {
        ca.leaf(names)
    };
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert.as_bytes())
        .collect::<Result<_, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let addr = listener.local_addr().unwrap();
    let cancels = Arc::new(Mutex::new(Vec::new()));
    let connections = Arc::new(Mutex::new(0));
    let (c2, n2) = (cancels.clone(), connections.clone());
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            *n2.lock().unwrap() += 1;
            let acceptor = acceptor.clone();
            let cancels = c2.clone();
            tokio::spawn(async move {
                let mut head = [0u8; 8];
                if s.read_exact(&mut head).await.is_err() {
                    return;
                }
                if head[..4] == [0, 0, 0, 16] {
                    let mut rest = [0u8; 8];
                    s.read_exact(&mut rest).await.unwrap();
                    let mut packet = [0u8; 16];
                    packet[..8].copy_from_slice(&head);
                    packet[8..].copy_from_slice(&rest);
                    cancels.lock().unwrap().push(packet);
                    return;
                }
                assert_eq!(head, SSL_REQUEST, "the gateway must send SSLRequest first");
                s.write_all(&[answer]).await.unwrap();
                if answer != b'S' {
                    return;
                }
                let Ok(mut tls) = acceptor.accept(s).await else {
                    return;
                };
                let mut buf = [0u8; 1024];
                loop {
                    match tls.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            let mut out = format!("{tag}:").into_bytes();
                            out.extend_from_slice(&buf[..n]);
                            if tls.write_all(&out).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    FakePgDog {
        addr,
        cancels,
        connections,
    }
}

fn client_config(ca_pem: &str, alpn: Option<&[u8]>, sni: bool) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let mut config = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    if let Some(p) = alpn {
        config.alpn_protocols = vec![p.to_vec()];
    }
    config.enable_sni = sni;
    Arc::new(config)
}

/// libpq's default negotiation: `SSLRequest`, `S`, then TLS. `sni = false`
/// verifies `host` but sends no SNI (old clients, `sslsni=0`).
pub async fn sslrequest_client(
    addr: SocketAddr,
    host: &str,
    sni: bool,
    trust: &str,
) -> std::io::Result<TlsStream<TcpStream>> {
    sslrequest_client_alpn(addr, host, sni, trust, None).await
}

/// [`sslrequest_client`] offering `alpn` (libpq 17 offers `postgresql`).
pub async fn sslrequest_client_alpn(
    addr: SocketAddr,
    host: &str,
    sni: bool,
    trust: &str,
    alpn: Option<&[u8]>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(&SSL_REQUEST).await?;
    let answer = s.read_u8().await?;
    if answer != b'S' {
        return Err(std::io::Error::other(format!("answer {answer}")));
    }
    let name = ServerName::try_from(host.to_owned()).unwrap();
    tokio_rustls::TlsConnector::from(client_config(trust, alpn, sni))
        .connect(name, s)
        .await
}

/// `sslnegotiation=direct`: a ClientHello first, with `alpn`.
pub async fn direct_client(
    addr: SocketAddr,
    host: &str,
    trust: &str,
    alpn: Option<&[u8]>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let s = TcpStream::connect(addr).await?;
    let name = ServerName::try_from(host.to_owned()).unwrap();
    tokio_rustls::TlsConnector::from(client_config(trust, alpn, true))
        .connect(name, s)
        .await
}

/// Writes `ping` and reads the fake PgDog's answer.
pub async fn ping<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(s: &mut S) -> String {
    s.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 256];
    let n = s.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// A loopback port nothing listens on (bound, then released).
pub async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A gateway on loopback with its files in a temporary directory.
pub struct Harness {
    pub dir: tempfile::TempDir,
    pub gateway: Gateway,
    pub routes_path: PathBuf,
}

pub struct Options {
    pub tls_mode: &'static str,
    pub handshake_timeout_ms: u64,
    pub max_connections: usize,
    pub drain_timeout_ms: u64,
    /// Give the gateway a certificate of its own CA instead of PgDog's.
    pub own_gateway_cert: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            tls_mode: "auto",
            handshake_timeout_ms: 3_000,
            max_connections: 100,
            drain_timeout_ms: 2_000,
            own_gateway_cert: false,
        }
    }
}

pub async fn start(upstream_ca: &Ca, routes: &str, opts: Options) -> Harness {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let (cert, key) = if opts.own_gateway_cert {
        Ca::new("gateway's own ca").wildcard
    } else {
        upstream_ca.wildcard.clone()
    };
    std::fs::write(dir.path().join("tls.crt"), cert).unwrap();
    std::fs::write(dir.path().join("tls.key"), key).unwrap();
    std::fs::write(dir.path().join("upstream-ca.crt"), &upstream_ca.pem).unwrap();
    let routes_path = dir.path().join("routes.toml");
    std::fs::write(&routes_path, with_generation(routes)).unwrap();
    let d = dir.path().display();
    let config = format!(
        r#"
[admin]
listen = "127.0.0.1:0"

[routes]
path = "{d}/routes.toml"
poll_interval_ms = 20

[pg]
listen = "127.0.0.1:0"
dedicated_bind = "127.0.0.1"
dedicated_ports = "1024-65535"
tls_mode = "{mode}"
handshake_timeout_ms = {hs}
max_connections = {max}
drain_timeout_ms = {drain}

[[pg.certs]]
names = ["{WILDCARD}"]
cert = "{d}/tls.crt"
key = "{d}/tls.key"

[pg.upstream_tls]
ca = "{d}/upstream-ca.crt"
"#,
        mode = opts.tls_mode,
        hs = opts.handshake_timeout_ms,
        max = opts.max_connections,
        drain = opts.drain_timeout_ms,
    );
    let gateway = Gateway::start(GatewayConfig::from_toml(&config).unwrap())
        .await
        .unwrap();
    let mut rx = gateway.routes();
    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|t| t.is_some()))
        .await
        .unwrap()
        .unwrap();
    Harness {
        dir,
        gateway,
        routes_path,
    }
}

impl Harness {
    /// Rewrites the routes file and waits until the gateway serves it.
    pub async fn set_routes(&self, routes: &str) {
        let tmp = self.dir.path().join("routes.toml.tmp");
        std::fs::write(&tmp, with_generation(routes)).unwrap();
        std::fs::rename(&tmp, &self.routes_path).unwrap();
        let want = loams_gateway::routes::generation_of(routes);
        let mut rx = self.gateway.routes();
        tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|t| t.as_ref().is_some_and(|t| t.generation == want)),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

/// One namespace's routes.
pub fn ns(name: &str, upstream: SocketAddr, port: Option<u16>) -> String {
    let port = port.map(|p| format!("port = {p}\n")).unwrap_or_default();
    format!(
        "[[pg.namespaces]]\nname = \"{name}\"\nhostnames = [\"{name}.gw.pg.test\"]\n{port}upstreams = [\"{upstream}\"]\n\n"
    )
}
