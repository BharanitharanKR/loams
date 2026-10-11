//! A [`WsServer`] on loopback for the unit tests: a fixed owner token, a
//! `Hello` that always succeeds, and a policy that refuses `Secret` to `mcp`.

use std::sync::Arc;

use async_trait::async_trait;
use loams_agentd_proto::rpc::{FORBIDDEN_ROLE, Role};

use crate::{
    Connections, Credentials, Identity, Limits, Policy, RpcClient, RpcError, RpcReply, RpcService,
    ScopedTokens, Secret, WsServer, connect_ws_with_token, methods,
};

pub const OWNER_TOKEN: &str = "test-owner-token-0123456789abcdefghijklmnopq";

/// Answers `Hello` with the connection's role; everything else goes to the service.
struct WithHello {
    role: Role,
    inner: Arc<dyn RpcService>,
}

#[async_trait]
impl RpcService for WithHello {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if method == methods::HELLO {
            return Ok(RpcReply::Value(serde_json::json!({ "role": self.role })));
        }
        self.inner.handle(method, params).await
    }
}

struct Open(Arc<dyn RpcService>);

impl Connections for Open {
    fn open(&self, identity: &Identity) -> Arc<dyn RpcService> {
        Arc::new(WithHello {
            role: identity.role,
            inner: self.0.clone(),
        })
    }
}

/// `Secret` is owner-only.
pub struct OwnerSecrets;

#[async_trait]
impl Policy for OwnerSecrets {
    async fn authorize(
        &self,
        identity: &Identity,
        method: &str,
        _params: &serde_json::Value,
    ) -> Result<(), RpcError> {
        if method == "Secret" && identity.role != Role::Owner {
            return Err(RpcError::Failed(format!(
                "{FORBIDDEN_ROLE}: Secret needs role owner"
            )));
        }
        Ok(())
    }
}

/// A running test server.
pub struct Served {
    pub port: u16,
    pub scoped: Arc<ScopedTokens>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    pub task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Served {
    pub fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    /// Stop the server and wait for it to return.
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.task).await.expect("server task").expect("serve");
    }
}

pub async fn serve(service: Arc<dyn RpcService>) -> Served {
    serve_with(service, Limits::default()).await
}

pub async fn serve_with(service: Arc<dyn RpcService>, limits: Limits) -> Served {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let scoped = Arc::new(ScopedTokens::new());
    let server = WsServer::new(
        Arc::new(Credentials::new(Secret::new(OWNER_TOKEN), scoped.clone())),
        Arc::new(Open(service)),
        Arc::new(OwnerSecrets),
    )
    .with_limits(limits);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(server.serve_until(listener, async {
        let _ = stop_rx.await;
    }));
    Served {
        port,
        scoped,
        stop: Some(stop_tx),
        task,
    }
}

pub fn hello_params() -> serde_json::Value {
    serde_json::json!({ "client": "cli", "clientVersion": "test", "protocol": 1 })
}

/// Connect with `token` and say `Hello`.
pub async fn dial(url: &str, token: &str) -> RpcClient {
    let client = connect_ws_with_token(url, token).await.expect("connect");
    client
        .call(methods::HELLO, hello_params())
        .await
        .expect("Hello");
    client
}
