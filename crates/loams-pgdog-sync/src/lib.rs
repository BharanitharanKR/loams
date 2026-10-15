//! `pgdog-config-sync`, the sidecar in each PgDog Pod (design §46 §8.2; PG2
//! Task 21). PgDog stays unmodified: this separate process (Apache-2.0)
//! receives the two files `pg-control` renders and writes them where PgDog
//! reads them, then `pg-control` sends `RELOAD` through PgDog's admin
//! database.
//!
//! - **`PUT /v1/config`** (JSON `{generation, pgdog_toml, users_toml}`, at
//!   most [`MAX_BODY`]): each file is written to a temporary file in the same
//!   directory and renamed over the old one, so a reader sees a whole old
//!   file or a whole new one, never part of one ([`write_config`]). The
//!   generation file is written last.
//! - **`GET /v1/generation`**: the generation last written (`null` before
//!   any), which `pg-control` waits for before `RELOAD`.
//! - **Mutual TLS only** ([`server_tls`]): a client must present a
//!   certificate the configured CA signed (`pg-control`'s); anything else
//!   fails the handshake. The files hold secrets (R20.5).
//! - **The persisted copy.** At start, the files in `--persisted` (the
//!   Secret Kubernetes mounts) are copied into the directory, so a restarted
//!   PgDog serves the last pushed config before `pg-control` pushes again
//!   ([`load_persisted`]).

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// PgDog's main file.
pub const PGDOG_TOML: &str = "pgdog.toml";
/// PgDog's users file.
pub const USERS_TOML: &str = "users.toml";
/// The generation of the files beside it.
pub const GENERATION: &str = "generation";
/// The largest config accepted.
pub const MAX_BODY: usize = 16 * 1024 * 1024;

/// What `pg-control` sends.
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub generation: u64,
    pub pgdog_toml: String,
    pub users_toml: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// `GET /v1/generation`'s answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generation {
    pub generation: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
}

/// Writes `contents` to `dir/name` whole: a temporary file in `dir`,
/// flushed, then renamed over the old one (mode 0640).
fn write_file(dir: &Path, name: &str, contents: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!(".{name}.tmp"));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            opts.mode(0o640);
        }
        let mut f = opts.open(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))
}

/// Writes the two files, then the generation, each atomically.
///
/// # Errors
///
/// The file system's.
pub fn write_config(dir: &Path, c: &Config) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    write_file(dir, PGDOG_TOML, c.pgdog_toml.as_bytes())?;
    write_file(dir, USERS_TOML, c.users_toml.as_bytes())?;
    write_file(dir, GENERATION, c.generation.to_string().as_bytes())
}

/// The generation written in `dir`, if any.
pub fn read_generation(dir: &Path) -> Option<u64> {
    std::fs::read_to_string(dir.join(GENERATION))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Copies the persisted config (both files; its generation if it has one,
/// else 0) into `dir`, unless `dir` already holds a generation. Answers the
/// generation now in `dir`.
///
/// # Errors
///
/// The file system's.
pub fn load_persisted(dir: &Path, persisted: &Path) -> std::io::Result<Option<u64>> {
    if let Some(g) = read_generation(dir) {
        return Ok(Some(g));
    }
    let (Ok(pgdog), Ok(users)) = (
        std::fs::read_to_string(persisted.join(PGDOG_TOML)),
        std::fs::read_to_string(persisted.join(USERS_TOML)),
    ) else {
        return Ok(None);
    };
    let generation = read_generation(persisted).unwrap_or(0);
    write_config(
        dir,
        &Config {
            generation,
            pgdog_toml: pgdog,
            users_toml: users,
        },
    )?;
    Ok(Some(generation))
}

/// A TLS server config that requires a client certificate signed by
/// `client_ca` (PEM).
///
/// # Errors
///
/// [`SyncError::Tls`] for unreadable PEM or a config rustls refuses.
pub fn server_tls(
    cert_pem: &[u8],
    key_pem: &[u8],
    client_ca_pem: &[u8],
) -> Result<rustls::ServerConfig, SyncError> {
    let tls = |e: &dyn std::fmt::Display| SyncError::Tls(e.to_string());
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<_, _>>()
        .map_err(|e| tls(&e))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| tls(&e))?;
    let mut roots = rustls::RootCertStore::empty();
    for ca in CertificateDer::pem_slice_iter(client_ca_pem) {
        roots.add(ca.map_err(|e| tls(&e))?).map_err(|e| tls(&e))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .map_err(|e| tls(&e))?;
    rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| tls(&e))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| tls(&e))
}

/// Where the server writes, and the TLS it requires.
#[derive(Clone)]
pub struct Server {
    pub dir: PathBuf,
    pub tls: Arc<rustls::ServerConfig>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

fn reply(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r
}

impl Server {
    async fn handle(&self, req: Request<Incoming>) -> Response<Full<Bytes>> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/v1/generation") => reply(
                StatusCode::OK,
                serde_json::json!(Generation {
                    generation: read_generation(&self.dir)
                })
                .to_string(),
            ),
            (&Method::PUT, "/v1/config") => {
                let body = match Limited::new(req.into_body(), MAX_BODY).collect().await {
                    Ok(b) => b.to_bytes(),
                    Err(_) => return reply(StatusCode::PAYLOAD_TOO_LARGE, String::new()),
                };
                let config: Config = match serde_json::from_slice(&body) {
                    Ok(c) => c,
                    Err(_) => return reply(StatusCode::BAD_REQUEST, "not a config".into()),
                };
                let dir = self.dir.clone();
                let generation = config.generation;
                match tokio::task::spawn_blocking(move || write_config(&dir, &config)).await {
                    Ok(Ok(())) => {
                        tracing::info!(generation, "config written");
                        reply(
                            StatusCode::OK,
                            serde_json::json!(Generation {
                                generation: Some(generation)
                            })
                            .to_string(),
                        )
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "the config was not written");
                        reply(StatusCode::INTERNAL_SERVER_ERROR, String::new())
                    }
                    Err(_) => reply(StatusCode::INTERNAL_SERVER_ERROR, String::new()),
                }
            }
            _ => reply(StatusCode::NOT_FOUND, String::new()),
        }
    }

    /// Serves `listener` until `shutdown` resolves. A connection that fails
    /// the TLS handshake (no client certificate, another CA's) is dropped.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        let acceptor = TlsAcceptor::from(self.tls.clone());
        let me = Arc::new(self);
        tokio::pin!(shutdown);
        loop {
            let (tcp, peer): (_, SocketAddr) = tokio::select! {
                () = &mut shutdown => return,
                accepted = listener.accept() => match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        continue;
                    }
                },
            };
            let (acceptor, me) = (acceptor.clone(), me.clone());
            tokio::spawn(async move {
                let tls = match acceptor.accept(tcp).await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::info!(%peer, error = %e, "refused a connection");
                        return;
                    }
                };
                let service = hyper::service::service_fn(move |req| {
                    let me = me.clone();
                    async move { Ok::<_, std::convert::Infallible>(me.handle(req).await) }
                });
                let io = hyper_util::rt::TokioIo::new(tls);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::debug!(%peer, error = %e, "connection ended");
                }
            });
        }
    }
}
