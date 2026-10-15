//! loams-agentd-rpc — the typed control plane (UiRpc / ControlRpc) over the loopback
//! WebSocket and in-memory transports.
//!
//! Framing: ndjson envelopes, one JSON object per WebSocket text message (or per line on
//! byte transports), matching the shape of loams-desktop's Effect RPC without the Effect runtime:
//!
//! - client → server: `{id, method, params}` to invoke, `{id, cancel: true}` to stop a stream;
//! - server → client: `{id, ok}` / `{id, err}` for unary calls,
//!   `{id, item}`* then `{id, done: true}` (or `{id, err}`) for streams.
//!
//! The server dispatches into an [`RpcService`]; the [`RpcClient`] offers `call` and
//! `subscribe`. Both ends run over any pair of string channels, so the in-memory transport
//! ([`memory_client`]) exercises the exact same code path as the WebSocket one.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use loams_agentd_proto::rpc::{Method, WireAs};
use serde::{Deserialize, Serialize};

pub mod auth;
mod client;
pub mod fuzz;
mod handshake;
pub mod limits;
pub mod roles;
mod server;

pub use auth::{Credentials, Identity, RunScope, RunToken, ScopedTokens, Secret};
pub use client::{RpcClient, RpcSubscription, connect_ws, connect_ws_with_token};
pub use limits::Limits;
pub use roles::Policy;
pub use server::{Connections, WsServer, serve_connection};

/// `tracing` directives every process using this crate must add to its
/// filter, whatever `RUST_LOG` says: tungstenite's client handshake logs the
/// whole upgrade request at TRACE, `Authorization: Bearer <token>` included
/// (DD1 Task 7). A more specific directive wins over a global level.
pub const SECRET_SAFE_LOG_DIRECTIVES: &[&str] = &["tungstenite::handshake=off"];

/// RPC method names, generated from the method table
/// (`loams_agentd_proto::rpc::METHODS`, plan DD1 Task 9).
pub use loams_agentd_proto::rpc::methods;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("unknown method: {0}")]
    UnknownMethod(String),
    #[error("bad params: {0}")]
    BadParams(String),
    #[error("{0}")]
    Failed(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("connection closed")]
    Closed,
    /// Sent as `{err}` like [`Self::Failed`], after which the server closes the
    /// connection (a `Hello` with another protocol, design §50 §5.4).
    #[error("{0}")]
    Fatal(String),
}

/// A client-originated frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientFrame {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancel: bool,
}

/// A server-originated frame. Exactly one of `ok` / `err` / `item` / `done` is meaningful.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerFrame {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub done: bool,
}

/// What a service returns for one invocation.
pub enum RpcReply {
    /// Unary response — sent as `{id, ok}`.
    Value(serde_json::Value),
    /// Stream — each item sent as `{id, item}`, then `{id, done: true}` when it ends.
    Stream(BoxStream<'static, serde_json::Value>),
}

impl std::fmt::Debug for RpcReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(value) => f.debug_tuple("Value").field(value).finish(),
            Self::Stream(_) => f.debug_tuple("Stream").finish_non_exhaustive(),
        }
    }
}

impl RpcReply {
    /// Serialize a value into a unary reply.
    pub fn value<T: Serialize>(value: &T) -> Result<Self, RpcError> {
        serde_json::to_value(value)
            .map(RpcReply::Value)
            .map_err(|e| RpcError::Failed(format!("serialize response: {e}")))
    }
}

/// Server-side dispatch: one implementation serves every transport.
#[async_trait]
pub trait RpcService: Send + Sync + 'static {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError>;
}

/// Deserialize typed params out of the envelope's `params` value.
pub fn parse_params<T: serde::de::DeserializeOwned>(
    params: serde_json::Value,
) -> Result<T, RpcError> {
    serde_json::from_value(params).map_err(|e| RpcError::BadParams(e.to_string()))
}

/// Parse a call's params into the type its method-table row names.
pub fn params_of<M: Method>(params: serde_json::Value) -> Result<M::Params, RpcError> {
    parse_params(params)
}

/// A unary reply of the type the method's row names (or its wire
/// equivalent, such as the `Vec` behind a list newtype).
pub fn reply<M: Method, T: Serialize + WireAs<M::Result>>(value: &T) -> Result<RpcReply, RpcError> {
    RpcReply::value(value)
}

/// A stream reply whose items are the type the method's row names. An item
/// that fails to serialize is dropped.
pub fn stream_reply<M, T, S>(items: S) -> RpcReply
where
    M: Method,
    T: Serialize + WireAs<M::Result>,
    S: futures::Stream<Item = T> + Send + 'static,
{
    use futures::StreamExt;
    RpcReply::Stream(
        items
            .filter_map(|item| std::future::ready(serde_json::to_value(item).ok()))
            .boxed(),
    )
}

/// Spawn an in-memory server for `service` and return a connected client.
/// Same envelopes, same dispatch loop as the WebSocket path — the in-process UI
/// transport (ARCHITECTURE §1 "zero serialization shortcuts").
pub fn memory_client(service: Arc<dyn RpcService>) -> RpcClient {
    let (client_out, server_in) = tokio::sync::mpsc::channel::<String>(256);
    let (server_out, client_in) = tokio::sync::mpsc::channel::<String>(256);
    tokio::spawn(serve_connection(service, server_out, server_in));
    RpcClient::new(client_out, client_in)
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod security_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Mutex;

    struct TestService;

    struct CancelAwareService {
        dropped: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(dropped) = self.0.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[async_trait]
    impl RpcService for CancelAwareService {
        async fn handle(
            &self,
            method: &str,
            _params: serde_json::Value,
        ) -> Result<RpcReply, RpcError> {
            if !matches!(
                method,
                methods::WATCH_CHECKOUT_CHANGE_REQUEST | methods::WATCH_HARNESS_UPDATES | "Silent"
            ) {
                return Err(RpcError::UnknownMethod(method.into()));
            }
            let guard = DropSignal(self.dropped.lock().unwrap().take());
            let stream = futures::stream::unfold(guard, |guard| async move {
                let item = std::future::pending::<Option<(serde_json::Value, DropSignal)>>().await;
                drop(guard);
                item
            });
            if method == methods::WATCH_HARNESS_UPDATES {
                // Update watches send an initial status snapshot, then may stay
                // quiet indefinitely. Legacy peers do not send a readiness frame.
                return Ok(RpcReply::Stream(
                    futures::stream::once(async { serde_json::json!([]) })
                        .chain(stream)
                        .boxed(),
                ));
            }
            Ok(RpcReply::Stream(stream.boxed()))
        }
    }

    #[async_trait]
    impl RpcService for TestService {
        async fn handle(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<RpcReply, RpcError> {
            match method {
                "Echo" => Ok(RpcReply::Value(params)),
                "Count" => {
                    let n = params.get("n").and_then(|v| v.as_u64()).unwrap_or(0);
                    Ok(RpcReply::Stream(
                        futures::stream::iter((0..n).map(|i| serde_json::json!(i))).boxed(),
                    ))
                }
                "Never" => Ok(RpcReply::Stream(futures::stream::pending().boxed())),
                "Boom" => Err(RpcError::Failed("boom".into())),
                "Fatal" => Err(RpcError::Fatal("fatal: goodbye".into())),
                other => Err(RpcError::UnknownMethod(other.into())),
            }
        }
    }

    #[tokio::test]
    async fn memory_call_stream_and_error() {
        let client = memory_client(Arc::new(TestService));

        let echoed = client
            .call("Echo", serde_json::json!({"x": 1}))
            .await
            .unwrap();
        assert_eq!(echoed, serde_json::json!({"x": 1}));

        let mut items = client
            .subscribe("Count", serde_json::json!({"n": 3}))
            .await
            .unwrap();
        let mut seen = Vec::new();
        while let Some(v) = items.recv().await {
            seen.push(v);
        }
        assert_eq!(
            seen,
            vec![
                serde_json::json!(0),
                serde_json::json!(1),
                serde_json::json!(2)
            ]
        );

        let err = client
            .call("Boom", serde_json::Value::Null)
            .await
            .unwrap_err();
        assert!(matches!(err, RpcError::Failed(m) if m == "boom"));
    }

    #[tokio::test]
    async fn checked_stream_acknowledges_support_and_preserves_unknown_method() {
        let client = memory_client(Arc::new(TestService));

        let mut items = client
            .subscribe_checked("Count", serde_json::json!({"n": 1}))
            .await
            .unwrap();
        assert_eq!(items.recv().await, Some(serde_json::json!(0)));
        assert_eq!(items.recv().await, None);

        let error = match client
            .subscribe_checked("FutureStream", serde_json::Value::Null)
            .await
        {
            Ok(_) => panic!("old service must reject unknown stream"),
            Err(error) => error,
        };
        assert!(matches!(error, RpcError::UnknownMethod(method) if method == "FutureStream"));
    }

    #[tokio::test]
    async fn dropping_checked_subscription_cancels_pending_server_stream() {
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let client = memory_client(Arc::new(CancelAwareService {
            dropped: Mutex::new(Some(dropped_tx)),
        }));
        let stream = client
            .subscribe_checked(
                methods::WATCH_CHECKOUT_CHANGE_REQUEST,
                serde_json::Value::Null,
            )
            .await
            .unwrap();

        drop(stream);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("server stream cancelled")
            .expect("drop signal");
    }

    #[tokio::test]
    async fn scoped_subscription_returns_before_first_item_and_cancels_silence() {
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let service = Arc::new(CancelAwareService {
            dropped: Mutex::new(Some(dropped_tx)),
        });
        let client = memory_client(service.clone());
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.subscribe_scoped("Silent", serde_json::Value::Null),
        )
        .await
        .unwrap()
        .unwrap();
        while service.dropped.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_one_device_update_watch_preserves_the_other() {
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
        let first = memory_client(Arc::new(CancelAwareService {
            dropped: Mutex::new(Some(first_tx)),
        }));
        let second = memory_client(Arc::new(CancelAwareService {
            dropped: Mutex::new(Some(second_tx)),
        }));
        let first_watch = first
            .subscribe_checked(methods::WATCH_HARNESS_UPDATES, serde_json::Value::Null)
            .await
            .unwrap();
        let second_watch = second
            .subscribe_checked(methods::WATCH_HARNESS_UPDATES, serde_json::Value::Null)
            .await
            .unwrap();

        drop(first_watch);
        tokio::time::timeout(std::time::Duration::from_secs(1), first_rx)
            .await
            .expect("first device's quiet stream cancelled")
            .expect("first drop signal");
        assert!(matches!(
            second_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        drop(second_watch);
        tokio::time::timeout(std::time::Duration::from_secs(1), second_rx)
            .await
            .expect("second device's quiet stream cancelled")
            .expect("second drop signal");
    }

    #[tokio::test]
    async fn connect_ws_dials_loopback_only() {
        for url in [
            "ws://example.com:27654",
            "ws://10.0.0.1:27654",
            "ws://[2001:db8::1]:27654",
            "wss://127.0.0.1:27654",
            "ws://user@127.0.0.1.example.com:1",
            "ws://127.0.0.1@example.com:1",
        ] {
            let Err(error) = connect_ws(url).await else {
                panic!("{url} was dialed");
            };
            assert!(error.to_string().contains("loopback"), "{url}: {error}");
        }
    }

    #[tokio::test]
    async fn websocket_round_trip() {
        let served = test_support::serve(Arc::new(TestService)).await;
        let client = test_support::dial(&served.url(), test_support::OWNER_TOKEN).await;
        let echoed = client
            .call("Echo", serde_json::json!("hello"))
            .await
            .unwrap();
        assert_eq!(echoed, serde_json::json!("hello"));

        let mut items = client
            .subscribe("Count", serde_json::json!({"n": 2}))
            .await
            .unwrap();
        assert_eq!(items.recv().await, Some(serde_json::json!(0)));
        assert_eq!(items.recv().await, Some(serde_json::json!(1)));
        assert_eq!(items.recv().await, None);
    }

    #[tokio::test]
    async fn handshake_with_origin_header_is_rejected() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let served = test_support::serve(Arc::new(TestService)).await;

        // A browser page opening ws://127.0.0.1:{port} always sends Origin;
        // the server must refuse the handshake before serving any RPC, even
        // with the right token.
        let mut req = served.url().into_client_request().unwrap();
        req.headers_mut()
            .insert("origin", "https://evil.example".parse().unwrap());
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {}", test_support::OWNER_TOKEN)
                .parse()
                .unwrap(),
        );
        let loopback = req;
        let result = tokio_tungstenite::connect_async(loopback).await;
        assert!(
            result.is_err(),
            "handshake carrying an Origin header must be rejected"
        );

        // A native client (no Origin) still connects and can call RPC — the
        // reject must not be a blanket denial.
        let client = test_support::dial(&served.url(), test_support::OWNER_TOKEN).await;
        let echoed = client.call("Echo", serde_json::json!("ok")).await.unwrap();
        assert_eq!(echoed, serde_json::json!("ok"));
    }

    #[tokio::test]
    async fn fatal_error_is_sent_then_the_websocket_closes() {
        let served = test_support::serve(Arc::new(TestService)).await;
        let client = test_support::dial(&served.url(), test_support::OWNER_TOKEN).await;
        // A stream left open must not keep the connection alive.
        let _never = client
            .subscribe("Never", serde_json::Value::Null)
            .await
            .unwrap();

        let err = client
            .call("Fatal", serde_json::Value::Null)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, RpcError::Failed(text) if text == "fatal: goodbye"),
            "{err:?}"
        );
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call("Echo", serde_json::json!(1)),
        )
        .await
        .expect("the closed connection answers at once");
        assert!(next.is_err(), "the server kept the connection: {next:?}");
    }

    /// `Hello` is fatal; `Record` counts its calls.
    struct HelloGate {
        recorded: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl RpcService for HelloGate {
        async fn handle(
            &self,
            method: &str,
            _params: serde_json::Value,
        ) -> Result<RpcReply, RpcError> {
            if method == methods::HELLO {
                return Err(RpcError::Fatal("protocol_mismatch".into()));
            }
            self.recorded
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(RpcReply::Value(serde_json::json!({})))
        }
    }

    #[tokio::test]
    async fn nothing_after_a_fatal_hello_is_dispatched() {
        let service = Arc::new(HelloGate {
            recorded: std::sync::atomic::AtomicU32::new(0),
        });
        let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(8);
        // The fatal Hello and a request batched behind it, then another payload.
        in_tx
            .send(format!(
                "{{\"id\":1,\"method\":\"{}\"}}\n{{\"id\":2,\"method\":\"Record\"}}",
                methods::HELLO
            ))
            .await
            .unwrap();
        in_tx
            .send(r#"{"id":3,"method":"Record"}"#.to_string())
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            serve_connection(service.clone(), out_tx, in_rx),
        )
        .await
        .expect("the fatal Hello ends the connection");
        let mut frames = Vec::new();
        while let Some(frame) = out_rx.recv().await {
            frames.push(serde_json::from_str::<ServerFrame>(&frame).unwrap());
        }
        assert_eq!(frames.len(), 1, "{frames:?}");
        assert_eq!(frames[0].id, 1);
        assert_eq!(frames[0].err.as_deref(), Some("protocol_mismatch"));
        assert_eq!(
            service.recorded.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn serve_until_ends_open_connections_and_their_requests() {
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let service = Arc::new(CancelAwareService {
            dropped: Mutex::new(Some(dropped_tx)),
        });
        let served = test_support::serve(service.clone()).await;
        let port = served.port;
        let client = test_support::dial(&served.url(), test_support::OWNER_TOKEN).await;
        let _silent = client
            .subscribe_scoped("Silent", serde_json::Value::Null)
            .await
            .unwrap();
        while service.dropped.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }

        tokio::time::timeout(std::time::Duration::from_secs(10), served.stop())
            .await
            .expect("the server returns once its connections end");
        // The in-flight request was dropped before the server returned.
        dropped_rx.await.expect("the open request was ended");
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call("Echo", serde_json::json!(1)),
        )
        .await
        .expect("a closed connection fails at once");
        assert!(next.is_err());
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err(),
            "the listener still accepts"
        );
    }

    #[tokio::test]
    async fn fatal_error_closes_a_memory_connection() {
        let client = memory_client(Arc::new(TestService));
        let err = client
            .call("Fatal", serde_json::Value::Null)
            .await
            .unwrap_err();
        assert!(matches!(&err, RpcError::Failed(text) if text == "fatal: goodbye"));
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call("Echo", serde_json::json!(1)),
        )
        .await
        .expect("the closed connection answers at once");
        assert!(next.is_err(), "the server kept the connection: {next:?}");
    }

    #[tokio::test]
    async fn dropping_stream_receiver_cancels_server_side() {
        let client = memory_client(Arc::new(TestService));
        let items = client
            .subscribe("Never", serde_json::Value::Null)
            .await
            .unwrap();
        drop(items);
        // The next unary call still works — the dead stream didn't wedge the connection.
        let echoed = client.call("Echo", serde_json::json!(2)).await.unwrap();
        assert_eq!(echoed, serde_json::json!(2));
    }
}
