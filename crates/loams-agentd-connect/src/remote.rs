//! The remote Connect listener (design §53 §6.3, §8.2, D909, D921; plan SD1
//! Task 7, Ruling 2).
//!
//! `loams-agentd serve --remote` serves `loams.agentd.v1` to one person's
//! browser console over TLS. It is a separate listener from the loopback
//! ones, which keep D788's "no browsers" rule; this one amends it for itself
//! only, and is off unless configured. Before any handler runs it checks, in
//! order:
//! 1. `Origin`: absent (a non-browser client), or one of the configured
//!    console origins. Any other origin gets a 403 `origin_not_allowed` and
//!    no CORS header ([`crate::cors`]).
//! 2. A preflight (`OPTIONS`) from an allowed origin is answered here, before
//!    authentication: browsers never send credentials on one.
//! 3. `Authorization: Bearer <access token>`, an OIDC JWT for the enrolled
//!    subject ([`crate::oidc`]): 401 when it does not verify, 403 for another
//!    subject, after the loopback listener's delay, one failure at a time.
//! 4. The role `studio` ([`crate::roles`]): a method outside the Studio set
//!    is a 403 `forbidden_role`; a path that is not a `loams.agentd.v1`
//!    method is a 404. The ndjson wire is never served here.
//!
//! `/preview/…` (Q806, SD1 Task 26) takes the same origin and token checks in
//! [`crate::preview`], which also admits the listener's own origin there and
//! a preview ticket in place of the bearer token.
//!
//! Answers to an allowed origin carry its CORS headers, refusals included,
//! so the console can read them.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use connectrpc::{ConnectError, ErrorCode};
use http::{HeaderValue, Method, StatusCode, header};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::{Mutex, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use crate::cors::{self, CorsPolicy};
use crate::oidc::{OidcOptions, Rejection, Verifier};
use crate::roles::{Access, studio_access};
use crate::services::Agentd;

/// How long a client has to finish the TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How the remote listener runs.
#[derive(Debug, Clone)]
pub struct RemoteOptions {
    /// `--listen`.
    pub listen: SocketAddr,
    /// From `--tls-cert` and `--tls-key` ([`tls_config`]).
    pub tls: Arc<rustls::ServerConfig>,
    pub oidc: OidcOptions,
    /// `--allow-origin`, each checked.
    pub cors: CorsPolicy,
    /// Added after each refused token (the ndjson listener's
    /// `Limits::auth_failure_delay`).
    pub auth_failure_delay: Duration,
    /// The daemon's loopback preview proxy (`PREVIEW_PROXY_PORT`), served
    /// under `/preview/` (Q806); `None` turns previews off.
    pub preview_port: Option<u16>,
}

/// Why the remote listener cannot start.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    #[error("{0}")]
    Config(String),
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

/// A TLS 1.2+ server config for the PEM certificate chain at `cert` and the
/// private key at `key`, offering HTTP/2 and HTTP/1.1.
///
/// # Errors
///
/// A file that cannot be read, holds no certificate or key, or a key that
/// does not match.
pub fn tls_config(cert: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>, RemoteError> {
    let read = |path: &Path, what: &str| {
        std::fs::read(path)
            .map_err(|e| RemoteError::Config(format!("--tls-{what} {}: {e}", path.display())))
    };
    let chain = CertificateDer::pem_slice_iter(&read(cert, "cert")?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RemoteError::Config(format!("--tls-cert {}: {e}", cert.display())))?;
    if chain.is_empty() {
        return Err(RemoteError::Config(format!(
            "--tls-cert {}: no certificate in the file",
            cert.display()
        )));
    }
    let private = PrivateKeyDer::from_pem_slice(&read(key, "key")?).map_err(|e| match e {
        pem::Error::NoItemsFound => RemoteError::Config(format!(
            "--tls-key {}: no private key in the file",
            key.display()
        )),
        e => RemoteError::Config(format!("--tls-key {}: {e}", key.display())),
    })?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| RemoteError::Config(format!("TLS: {e}")))?
        .with_no_client_auth()
        .with_single_cert(chain, private)
        .map_err(|e| RemoteError::Config(format!("--tls-cert/--tls-key: {e}")))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// The checks in front of the services.
#[derive(Debug)]
pub struct RemoteGuard {
    verifier: Verifier,
    cors: CorsPolicy,
    delay: Duration,
    /// Serializes refused tokens, so a failure's delay holds back the next.
    gate: Mutex<()>,
    previews: crate::preview::Previews,
}

impl RemoteGuard {
    /// The guard for `options`' OIDC and CORS settings.
    ///
    /// # Errors
    ///
    /// Invalid OIDC options.
    pub fn new(
        oidc: OidcOptions,
        cors: CorsPolicy,
        delay: Duration,
        preview_port: Option<u16>,
    ) -> Result<Arc<Self>, RemoteError> {
        Ok(Arc::new(Self {
            verifier: Verifier::new(oidc).map_err(RemoteError::Config)?,
            previews: crate::preview::Previews::new(preview_port, &cors),
            cors,
            delay,
            gate: Mutex::new(()),
        }))
    }

    /// The configured console origins.
    pub fn cors(&self) -> &CorsPolicy {
        &self.cors
    }

    /// The preview proxy and its tickets.
    pub fn previews(&self) -> &crate::preview::Previews {
        &self.previews
    }

    /// The refused-credential delay, one failure at a time.
    pub async fn fail_delay(&self) {
        let _gate = self.gate.lock().await;
        tokio::time::sleep(self.delay).await;
    }
}

/// A running remote listener.
#[derive(Debug)]
pub struct RemoteListener {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl RemoteListener {
    /// Binds `options.listen` and serves `agentd` there over TLS, in the
    /// background.
    ///
    /// # Errors
    ///
    /// Invalid OIDC options, or the address cannot be bound.
    pub async fn bind(agentd: Agentd, options: RemoteOptions) -> Result<Self, RemoteError> {
        let guard = RemoteGuard::new(
            options.oidc,
            options.cors,
            options.auth_failure_delay,
            options.preview_port,
        )?;
        let listener = tokio::net::TcpListener::bind(options.listen)
            .await
            .map_err(|source| RemoteError::Bind {
                addr: options.listen,
                source,
            })?;
        let addr = listener.local_addr().map_err(|source| RemoteError::Bind {
            addr: options.listen,
            source,
        })?;
        let acceptor = tokio_rustls::TlsAcceptor::from(options.tls);
        let app = app(agentd, guard);
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(accept_loop(listener, acceptor, app, stopped));
        tracing::info!(%addr, "loams.agentd.v1 listening for the remote console (TLS)");
        Ok(Self {
            addr,
            stop: Some(stop),
            task,
        })
    }

    /// The bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops accepting and drops every connection after a short grace.
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if tokio::time::timeout(Duration::from_millis(500), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

async fn accept_loop(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    mut stopped: oneshot::Receiver<()>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut stopped => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (tcp, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "remote: accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let service = TowerToHyperService::new(app.clone());
                connections.spawn(async move {
                    let tls = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(tls)) => tls,
                        Ok(Err(error)) => {
                            tracing::debug!(%error, peer_port = peer.port(), "remote: TLS handshake failed");
                            return;
                        }
                        Err(_) => {
                            tracing::debug!(peer_port = peer.port(), "remote: TLS handshake timed out");
                            return;
                        }
                    };
                    let served = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(TokioIo::new(tls), service)
                        .await;
                    if let Err(error) = served {
                        tracing::debug!(%error, peer_port = peer.port(), "remote: connection ended");
                    }
                });
            }
        }
    }
    connections.shutdown().await;
}

/// The guarded app (no TLS; [`RemoteListener`] adds it).
pub fn app(agentd: Agentd, guard: Arc<RemoteGuard>) -> axum::Router {
    axum::Router::new()
        .fallback_service(agentd.router().into_axum_service())
        .layer(axum::middleware::from_fn_with_state(guard, check))
}

/// A Connect error body with the HTTP status of its code.
pub(crate) fn refuse(code: ErrorCode, reason: Option<&str>, message: &str) -> Response {
    let error = match reason {
        Some(reason) => crate::error::refuse(code, reason, message),
        None => ConnectError::new(code, message),
    };
    let mut response = Response::new(Body::from(error.to_json()));
    *response.status_mut() = code.http_status();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// `Bearer <token>`, as the loopback listener reads it.
fn bearer(value: &[u8]) -> Option<&str> {
    const SCHEME: &[u8] = b"bearer ";
    if value.len() <= SCHEME.len() || !value[..SCHEME.len()].eq_ignore_ascii_case(SCHEME) {
        return None;
    }
    let token = std::str::from_utf8(&value[SCHEME.len()..]).ok()?;
    (!token.is_empty() && !token.contains(' ')).then_some(token)
}

async fn check(State(guard): State<Arc<RemoteGuard>>, request: Request, next: Next) -> Response {
    if studio_access(request.uri().path()) == Access::Preview {
        return crate::preview::serve(&guard, request).await;
    }
    let origin = request.headers().get(header::ORIGIN).cloned();
    if let Some(origin) = &origin
        && !guard.cors.allows(origin)
    {
        tracing::debug!("remote: refused a request from an origin not allowed (403)");
        return refuse(
            ErrorCode::PermissionDenied,
            Some("origin_not_allowed"),
            "this origin may not call this agentd (--allow-origin)",
        );
    }
    let mut response = guarded(&guard, origin.is_some(), request, next).await;
    if let Some(origin) = &origin {
        if response.status() == StatusCode::NO_CONTENT
            && response.extensions().get::<Preflight>().is_some()
        {
            cors::preflight_headers(origin, response.headers_mut());
        } else {
            cors::response_headers(origin, response.headers_mut());
        }
    }
    response
}

/// Marks this module's own preflight answers.
#[derive(Debug, Clone, Copy)]
struct Preflight;

/// Step 3: the bearer token, refused after the failure delay; `Ok` is the
/// token's `exp` (seconds since the epoch).
pub(crate) async fn authenticate(
    guard: &RemoteGuard,
    headers: &http::HeaderMap,
) -> Result<u64, Response> {
    let mut authorizations = headers.get_all(header::AUTHORIZATION).iter();
    let presented = match (authorizations.next(), authorizations.next()) {
        (Some(value), None) => bearer(value.as_bytes()),
        _ => None,
    };
    let verdict = match presented {
        Some(token) => guard.verifier.verify_until(token).await,
        None => Err(Rejection {
            code: ErrorCode::Unauthenticated,
            reason: Some("unauthenticated"),
            message: "a bearer access token is required".to_owned(),
        }),
    };
    match verdict {
        Ok(exp) => Ok(exp),
        Err(rejection) => {
            if rejection.code != ErrorCode::Unavailable {
                tracing::warn!(
                    code = rejection.code.as_str(),
                    "remote: refused a request without a valid token"
                );
                guard.fail_delay().await;
            }
            Err(refuse(rejection.code, rejection.reason, &rejection.message))
        }
    }
}

async fn guarded(guard: &RemoteGuard, has_origin: bool, request: Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        let asks = request
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|value| value.to_str().ok());
        if !has_origin || !matches!(asks, Some("POST" | "GET")) {
            return refuse(
                ErrorCode::PermissionDenied,
                Some("origin_not_allowed"),
                "only a CORS preflight from an allowed origin is answered",
            );
        }
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        response.extensions_mut().insert(Preflight);
        return response;
    }

    if let Err(refusal) = authenticate(guard, request.headers()).await {
        return refusal;
    }

    match studio_access(request.uri().path()) {
        Access::Allowed => next.run(request).await,
        Access::Forbidden => refuse(
            ErrorCode::PermissionDenied,
            Some("forbidden_role"),
            "the studio role may not call this method",
        ),
        Access::Unknown | Access::Preview => refuse(
            ErrorCode::Unimplemented,
            Some("unknown_method"),
            "not a loams.agentd.v1 method",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_reads_one_token() {
        assert_eq!(bearer(b"Bearer a.b.c"), Some("a.b.c"));
        assert_eq!(bearer(b"bearer a.b.c"), Some("a.b.c"));
        assert_eq!(bearer(b"Bearer "), None);
        assert_eq!(bearer(b"Bearer a b"), None);
        assert_eq!(bearer(b"Basic abc"), None);
    }
}
