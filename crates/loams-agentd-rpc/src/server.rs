//! Server side: dispatch loop over string frames + the WebSocket acceptor.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as HandshakeRequest, Response as HandshakeResponse,
};
use tokio_tungstenite::tungstenite::http::StatusCode;

use crate::{ClientFrame, RpcError, RpcReply, RpcService, ServerFrame};

/// Serve one connection: read client frames from `inbound`, write server frames to `out`.
/// Returns when `inbound` closes, or after a request failed with [`RpcError::Fatal`]
/// (its `{err}` frame is queued on `out` first); in-flight requests are aborted and
/// awaited before it returns.
pub async fn serve_connection(
    service: Arc<dyn RpcService>,
    out: mpsc::Sender<String>,
    inbound: mpsc::Receiver<String>,
) {
    serve_connection_until(service, out, inbound, None).await;
}

/// [`serve_connection`], which also ends when `stop` turns `true`.
async fn serve_connection_until(
    service: Arc<dyn RpcService>,
    out: mpsc::Sender<String>,
    mut inbound: mpsc::Receiver<String>,
    mut stop: Option<watch::Receiver<bool>>,
) {
    let mut tasks = JoinSet::new();
    let mut running: HashMap<u64, tokio::task::AbortHandle> = HashMap::new();
    // `close_tx` lives as long as this loop, so `close_rx` only yields on a fatal error.
    let (close_tx, mut close_rx) = mpsc::channel::<()>(1);
    'connection: loop {
        let payload = tokio::select! {
            biased;
            _ = close_rx.recv() => break,
            () = stopped(&mut stop) => break,
            payload = inbound.recv() => match payload {
                Some(payload) => payload,
                None => break,
            },
        };
        // ndjson: a transport may batch several frames per message.
        for line in payload.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let frame: ClientFrame = match serde_json::from_str(line) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::warn!(error = %err, "rpc: dropping malformed client frame");
                    continue;
                }
            };
            while tasks.try_join_next().is_some() {}
            running.retain(|_, task| !task.is_finished());
            if frame.cancel {
                if let Some(task) = running.remove(&frame.id) {
                    task.abort();
                }
                continue;
            }
            let Some(method) = frame.method else {
                tracing::warn!(id = frame.id, "rpc: frame has neither method nor cancel");
                continue;
            };
            let request = handle_request(
                service.clone(),
                out.clone(),
                close_tx.clone(),
                frame.id,
                method.clone(),
                frame.params,
            );
            if method == crate::methods::HELLO {
                // The one method that can end the connection: nothing after a
                // fatal `Hello` may be dispatched, even in the same payload.
                request.await;
                if close_rx.try_recv().is_ok() {
                    break 'connection;
                }
                continue;
            }
            let task = tasks.spawn(request);
            running.insert(frame.id, task);
        }
    }
    tasks.shutdown().await;
}

/// Resolves once `stop` is `true` (or its sender is gone); never without one.
async fn stopped(stop: &mut Option<watch::Receiver<bool>>) {
    match stop {
        Some(stop) => {
            let _ = stop.wait_for(|stopped| *stopped).await;
        }
        None => std::future::pending().await,
    }
}

async fn handle_request(
    service: Arc<dyn RpcService>,
    out: mpsc::Sender<String>,
    close: mpsc::Sender<()>,
    id: u64,
    method: String,
    params: serde_json::Value,
) {
    let send = |frame: ServerFrame| {
        let out = out.clone();
        async move {
            match serde_json::to_string(&frame) {
                Ok(json) => out.send(json).await.map_err(|_| RpcError::Closed),
                Err(err) => {
                    tracing::error!(error = %err, "rpc: failed to serialize server frame");
                    Err(RpcError::Closed)
                }
            }
        }
    };
    match service.handle(&method, params).await {
        Ok(RpcReply::Value(value)) => {
            let _ = send(ServerFrame {
                id,
                ok: Some(value),
                ..Default::default()
            })
            .await;
        }
        Ok(RpcReply::Stream(mut stream)) => {
            // Only the versioned checkout-PR stream uses an explicit readiness
            // frame. Sending it for legacy streams would make older clients remove
            // their pending stream as if it were a unary response.
            if method == crate::methods::WATCH_CHECKOUT_CHANGE_REQUEST
                && send(ServerFrame {
                    id,
                    ok: Some(serde_json::json!({ "stream": true })),
                    ..Default::default()
                })
                .await
                .is_err()
            {
                return;
            }
            while let Some(item) = stream.next().await {
                if send(ServerFrame {
                    id,
                    item: Some(item),
                    ..Default::default()
                })
                .await
                .is_err()
                {
                    return; // connection gone
                }
            }
            let _ = send(ServerFrame {
                id,
                done: true,
                ..Default::default()
            })
            .await;
        }
        Err(err) => {
            let fatal = matches!(err, RpcError::Fatal(_));
            let _ = send(ServerFrame {
                id,
                err: Some(err.to_string()),
                ..Default::default()
            })
            .await;
            if fatal {
                let _ = close.try_send(());
            }
        }
    }
}

/// Accept WebSocket connections forever, serving each with `service`.
pub async fn serve_ws_listener(listener: TcpListener, service: Arc<dyn RpcService>) {
    serve_ws_until(listener, service, std::future::pending()).await;
}

/// Accept WebSocket connections until `stop` resolves. Then stop accepting, end
/// every open connection (its in-flight requests are aborted and awaited) and
/// return, so the caller can tear down what the service uses.
pub async fn serve_ws_until(
    listener: TcpListener,
    service: Arc<dyn RpcService>,
    stop: impl std::future::Future<Output = ()>,
) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    tracing::debug!(%peer, "rpc: connection accepted");
                    connections.spawn(serve_ws_socket(stream, service.clone(), stop_rx.clone()));
                }
                Err(err) => {
                    tracing::warn!(error = %err, "rpc: accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            },
        }
    }
    drop(listener);
    let _ = stop_tx.send(true);
    while connections.join_next().await.is_some() {}
}

async fn serve_ws_socket(
    stream: TcpStream,
    service: Arc<dyn RpcService>,
    mut stop: watch::Receiver<bool>,
) {
    // Native viewports dial this socket with a bare `connect_async` and send
    // no `Origin` header. A browser always attaches `Origin` to a WebSocket
    // handshake and cannot forge or suppress it from script, and WebSockets
    // are exempt from the Same-Origin Policy — so only rejecting any handshake
    // that carries `Origin` keeps a page the user happens to visit from
    // reaching this local socket. Keep this check.
    //
    // The large `Err` (ErrorResponse) is the shape tungstenite's Callback
    // trait requires; it can't be boxed away here.
    #[allow(clippy::result_large_err)]
    let reject_cross_origin = |req: &HandshakeRequest, resp: HandshakeResponse| {
        if let Some(origin) = req.headers().get("origin") {
            tracing::warn!(
                origin = %String::from_utf8_lossy(origin.as_bytes()),
                "rpc: rejecting handshake carrying an Origin header (cross-origin browser dial)"
            );
            let mut err = ErrorResponse::new(Some("origin not allowed on local IPC".to_string()));
            *err.status_mut() = StatusCode::FORBIDDEN;
            return Err(err);
        }
        Ok(resp)
    };
    let handshake = tokio_tungstenite::accept_hdr_async(stream, reject_cross_origin);
    let ws = tokio::select! {
        result = handshake => match result {
            Ok(ws) => ws,
            Err(err) => {
                tracing::warn!(error = %err, "rpc: websocket handshake failed");
                return;
            }
        },
        _ = stop.wait_for(|stopped| *stopped) => return,
    };
    let (mut sink, mut ws_stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    let (in_tx, in_rx) = mpsc::channel::<String>(256);

    // Pump: socket <-> string channels. Ends when either side closes.
    let pump = tokio::spawn(async move {
        loop {
            tokio::select! {
                frame = out_rx.recv() => match frame {
                    Some(text) => {
                        if sink.send(WsMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break;
                    }
                },
                message = ws_stream.next() => match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if in_tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {} // ping/pong/binary — ignored
                },
            }
        }
    });

    serve_connection_until(service, out_tx, in_rx, Some(stop)).await;
    // Let the pump flush what is queued (a fatal error's frame) and send Close:
    // it ends once every sender is dropped. A peer that stopped reading cannot
    // hold the task for longer than the grace period.
    let mut pump = pump;
    if tokio::time::timeout(CLOSE_GRACE, &mut pump).await.is_err() {
        pump.abort();
    }
}

/// How long a closing connection may take to flush its last frames.
const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);
