//! The loopback Connect listener (design §53 §6.2, D908; plan SD1 Task 5,
//! Ruling 2).
//!
//! It keeps the ndjson listener's rules (§50 §7), checked in this order before
//! any handler runs:
//! 1. the peer is loopback (the listener binds `127.0.0.1:0`; defence in
//!    depth), else 403;
//! 2. no `Origin` header: a browser never reaches the daemon directly, and
//!    Electron main drops `Origin` when it proxies, else 403;
//! 3. `Host` is exactly `127.0.0.1:<port>` (DNS rebinding), else 403;
//! 4. `Authorization: Bearer <owner token>`, compared in constant time, else
//!    401 after the same delay the ndjson listener adds, one failure at a time.
//!
//! The listener takes the owner token only: the MCP shim's scoped run tokens
//! stay on the ndjson wire. Refusals are Connect error bodies
//! (`{"code": …, "message": …}`), so a Connect client reads them as errors.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::{HeaderMap, StatusCode, header};
use loams_agentd_rpc::Secret;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use crate::services::Agentd;

/// How the loopback listener checks its callers.
#[derive(Debug, Clone)]
pub struct LoopbackOptions {
    /// The daemon's main token (`agentd.token`).
    pub token: Secret,
    /// Added after each failed authentication (the ndjson listener's
    /// `Limits::auth_failure_delay`).
    pub auth_failure_delay: Duration,
}

impl LoopbackOptions {
    /// The ndjson listener's delay, with `token`.
    pub fn new(token: Secret) -> Self {
        Self {
            token,
            auth_failure_delay: loams_agentd_rpc::Limits::default().auth_failure_delay,
        }
    }
}

struct Guard {
    port: u16,
    token: Secret,
    delay: Duration,
    /// Serializes failed checks, so a failure's delay holds back the next.
    gate: Mutex<()>,
}

/// A running listener.
#[derive(Debug)]
pub struct ConnectListener {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl ConnectListener {
    /// Binds `127.0.0.1:0` and serves `agentd` there in the background.
    ///
    /// # Errors
    ///
    /// When the port cannot be bound.
    pub async fn bind(agentd: Agentd, options: LoopbackOptions) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let app = app(agentd, addr.port(), options);
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let serve = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            });
            if let Err(error) = serve.await {
                tracing::warn!(%error, "the Connect listener failed");
            }
        });
        tracing::info!(port = addr.port(), "loams.agentd.v1 listening on loopback");
        Ok(Self {
            addr,
            stop: Some(stop),
            task,
        })
    }

    /// The bound port (`connectPort` in `agentd.json`).
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Stops accepting and closes the listener. Open streams never end by
    /// themselves, so connections still open after a short grace are dropped.
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

/// The guarded app for a listener on `port`.
pub fn app(agentd: Agentd, port: u16, options: LoopbackOptions) -> axum::Router {
    let guard = Arc::new(Guard {
        port,
        token: options.token,
        delay: options.auth_failure_delay,
        gate: Mutex::new(()),
    });
    axum::Router::new()
        .fallback_service(agentd.router().into_axum_service())
        .layer(axum::middleware::from_fn_with_state(guard, check))
}

/// A Connect error body with an HTTP status.
fn refuse(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({ "code": code, "message": message }).to_string();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}

/// `Bearer <token>` (scheme case-insensitive, one space), as the ndjson
/// listener reads it.
fn bearer(value: &[u8]) -> Option<&[u8]> {
    const SCHEME: &[u8] = b"bearer ";
    if value.len() <= SCHEME.len() || !value[..SCHEME.len()].eq_ignore_ascii_case(SCHEME) {
        return None;
    }
    let token = &value[SCHEME.len()..];
    (!token.is_empty() && !token.contains(&b' ')).then_some(token)
}

/// The one `Host` (or HTTP/2 authority) of a request.
fn host<'a>(headers: &'a HeaderMap, request: &'a Request) -> Option<&'a str> {
    let mut hosts = headers.get_all(header::HOST).iter();
    match (hosts.next(), hosts.next()) {
        (Some(host), None) => host.to_str().ok(),
        (None, _) => request
            .uri()
            .authority()
            .map(|authority| authority.as_str()),
        (Some(_), Some(_)) => None,
    }
}

async fn check(
    State(guard): State<Arc<Guard>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if !peer.ip().is_loopback() {
        tracing::warn!("connect: refused a non-loopback peer (403)");
        return refuse(StatusCode::FORBIDDEN, "permission_denied", "loopback only");
    }
    let headers = request.headers();
    if headers.contains_key(header::ORIGIN) {
        tracing::debug!(
            peer_port = peer.port(),
            "connect: refused a request with an Origin (403)"
        );
        return refuse(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "requests with an Origin are refused",
        );
    }
    let expected = format!("127.0.0.1:{}", guard.port);
    if host(headers, &request) != Some(expected.as_str()) {
        tracing::debug!(
            peer_port = peer.port(),
            "connect: refused a foreign Host (403)"
        );
        return refuse(StatusCode::FORBIDDEN, "permission_denied", "foreign Host");
    }
    let mut authorizations = headers.get_all(header::AUTHORIZATION).iter();
    let presented = match (authorizations.next(), authorizations.next()) {
        (Some(value), None) => bearer(value.as_bytes()),
        _ => None,
    };
    if !presented.is_some_and(|token| guard.token.matches(token)) {
        let _gate = guard.gate.lock().await;
        tracing::warn!(
            peer_port = peer.port(),
            "connect: refused a request without a valid token (401)"
        );
        tokio::time::sleep(guard.delay).await;
        return refuse(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "a valid bearer token is required",
        );
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_reads_one_token() {
        assert_eq!(bearer(b"Bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer(b"bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer(b"Bearer "), None);
        assert_eq!(bearer(b"Bearer a b"), None);
        assert_eq!(bearer(b"Basic abc"), None);
    }
}
