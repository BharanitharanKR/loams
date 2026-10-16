//! The admin listener (design §57 §5): `/healthz`, `/readyz` and
//! `/v1/routes`. Plain HTTP/1.1 on the pod network; never on the Service.

use std::convert::Infallible;
use std::future::Future;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::listeners::SharedStatus;
use crate::reload::RouteRx;

/// What the admin routes read.
#[derive(Debug, Clone)]
pub struct State {
    pub routes: RouteRx,
    pub listeners: SharedStatus,
}

/// Serves until `shutdown` resolves.
pub async fn serve(listener: TcpListener, state: State, shutdown: impl Future<Output = ()>) {
    tokio::pin!(shutdown);
    loop {
        let (stream, _) = tokio::select! {
            _ = &mut shutdown => return,
            a = listener.accept() => match a {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "admin accept failed");
                    // A persistent error (EMFILE) must not spin a core.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            },
        };
        let state = state.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle(&state, &req)) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

fn handle(state: &State, req: &Request<Incoming>) -> Response<Full<Bytes>> {
    if req.method() != Method::GET {
        return reply(StatusCode::METHOD_NOT_ALLOWED, "GET only\n".into());
    }
    match req.uri().path() {
        "/healthz" => reply(StatusCode::OK, "ok\n".into()),
        "/readyz" => readyz(state),
        "/v1/routes" => routes(state),
        _ => reply(StatusCode::NOT_FOUND, "not found\n".into()),
    }
}

/// Ready once a route table is loaded. Dedicated ports that failed to bind
/// are reported, but the shared port still serves, so the replica stays
/// ready.
fn readyz(state: &State) -> Response<Full<Bytes>> {
    if state.routes.borrow().is_none() {
        return reply(StatusCode::SERVICE_UNAVAILABLE, "no routes loaded\n".into());
    }
    let failures = status_failures(state);
    if failures.is_empty() {
        reply(StatusCode::OK, "ready\n".into())
    } else {
        reply(
            StatusCode::OK,
            format!("ready, degraded: dedicated ports not bound: {failures:?}\n"),
        )
    }
}

/// The generation and counts: no hostnames, no upstreams.
fn routes(state: &State) -> Response<Full<Bytes>> {
    let table = state.routes.borrow().clone();
    let (bound, failures) = {
        let s = state
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            s.dedicated_bound.iter().copied().collect::<Vec<_>>(),
            s.bind_failures.keys().copied().collect::<Vec<_>>(),
        )
    };
    let body = serde_json::json!({
        "generation": table.as_ref().map(|t| t.generation.clone()),
        "pg_namespaces": table.as_ref().map_or(0, |t| t.pg.namespaces.len()),
        "dedicated_ports_bound": bound,
        "dedicated_ports_failed": failures,
    });
    let mut r = reply(StatusCode::OK, format!("{body}\n"));
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}

fn status_failures(state: &State) -> Vec<u16> {
    state
        .listeners
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .bind_failures
        .keys()
        .copied()
        .collect()
}

fn reply(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r
}
