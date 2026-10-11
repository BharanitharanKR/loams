//! `Hello`, the handshake (design §50 §5.4).
//!
//! [`HelloService`] answers `Hello` in front of the engine's methods. A client
//! with another protocol gets `protocol_mismatch: …` and its connection is
//! closed ([`RpcError::Fatal`]). [`probe`] is the client side, used by a
//! second `run` and by `status` to confirm a daemon found through discovery.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loams_agentd_proto::daemon::DaemonMode;
use loams_agentd_proto::rpc::{
    AGENTD_PROTOCOL, ClientKind, Hello, HelloReply, PROTOCOL_MISMATCH, Role,
};
use loams_agentd_rpc::{RpcError, RpcReply, RpcService, methods, parse_params};

/// The daemon's version: `LOAMS_AGENTD_VERSION` at build time (the Electron
/// app's version, set by the packaging script), else the crate's (T0-10b).
pub const VERSION: &str = match option_env!("LOAMS_AGENTD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

/// The commit the daemon was built from (`LOAMS_BUILD_SHA`), else `unknown`.
pub const BUILD_SHA: &str = match option_env!("LOAMS_BUILD_SHA") {
    Some(sha) => sha,
    None => "unknown",
};

/// How long [`probe`] waits for a daemon to answer.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// What this process reports about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaemonInfo {
    pub mode: DaemonMode,
    pub pid: u32,
    pub started_at_ms: i64,
}

impl DaemonInfo {
    /// `Hello`'s reply. Every connection is `owner` until the RPC checks
    /// tokens (DD1 Task 7).
    pub fn reply(&self) -> HelloReply {
        HelloReply {
            protocol: AGENTD_PROTOCOL,
            version: VERSION.to_string(),
            build_sha: BUILD_SHA.to_string(),
            mode: self.mode,
            pid: self.pid,
            started_at_ms: self.started_at_ms,
            role: Role::Owner,
            features: Vec::new(),
        }
    }
}

/// Serves `Hello`; every other method goes to `inner`.
pub struct HelloService {
    inner: Arc<dyn RpcService>,
    info: DaemonInfo,
}

impl HelloService {
    pub fn new(inner: Arc<dyn RpcService>, info: DaemonInfo) -> Self {
        Self { inner, info }
    }
}

impl std::fmt::Debug for HelloService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HelloService")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RpcService for HelloService {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if method != methods::HELLO {
            return self.inner.handle(method, params).await;
        }
        let hello: Hello = parse_params(params)?;
        if hello.protocol != AGENTD_PROTOCOL {
            return Err(RpcError::Fatal(format!(
                "{PROTOCOL_MISMATCH}: this daemon speaks protocol {AGENTD_PROTOCOL}, \
                 the client protocol {}",
                hello.protocol
            )));
        }
        RpcReply::value(&self.info.reply())
    }
}

/// Say `Hello` to the daemon on loopback `port` as the CLI.
pub async fn probe(port: u16) -> Result<HelloReply, RpcError> {
    let hello = Hello {
        client: ClientKind::Cli,
        client_version: VERSION.to_string(),
        protocol: AGENTD_PROTOCOL,
    };
    let params = serde_json::to_value(&hello).map_err(|e| RpcError::BadParams(e.to_string()))?;
    tokio::time::timeout(PROBE_TIMEOUT, async {
        let client = loams_agentd_rpc::connect_ws(&format!("ws://127.0.0.1:{port}")).await?;
        client.call_as(methods::HELLO, params).await
    })
    .await
    .map_err(|_| RpcError::Transport(format!("no Hello from port {port}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Engine;

    #[async_trait]
    impl RpcService for Engine {
        async fn handle(
            &self,
            method: &str,
            _params: serde_json::Value,
        ) -> Result<RpcReply, RpcError> {
            Ok(RpcReply::Value(serde_json::json!({ "engine": method })))
        }
    }

    fn service() -> Arc<HelloService> {
        Arc::new(HelloService::new(
            Arc::new(Engine),
            DaemonInfo {
                mode: DaemonMode::Service,
                pid: 42,
                started_at_ms: 7,
            },
        ))
    }

    #[tokio::test]
    async fn hello_answers_and_other_methods_pass_through() {
        let client = loams_agentd_rpc::memory_client(service());
        let reply: HelloReply = client
            .call_as(
                methods::HELLO,
                serde_json::json!({"client": "electron-main", "clientVersion": "1", "protocol": 1}),
            )
            .await
            .expect("hello");
        assert_eq!(reply.mode, DaemonMode::Service);
        assert_eq!(reply.pid, 42);
        assert_eq!(reply.started_at_ms, 7);
        assert_eq!(reply.version, VERSION);
        assert_eq!(reply.role, Role::Owner);
        assert_eq!(
            client
                .call("ListHarnesses", serde_json::json!({}))
                .await
                .expect("pass through"),
            serde_json::json!({ "engine": "ListHarnesses" })
        );
    }

    #[tokio::test]
    async fn malformed_hello_is_bad_params_and_keeps_the_connection() {
        let client = loams_agentd_rpc::memory_client(service());
        let err = client
            .call(methods::HELLO, serde_json::json!({"client": "browser"}))
            .await
            .expect_err("bad params");
        assert!(err.to_string().starts_with("bad params"), "{err}");
        assert!(
            client
                .call(
                    methods::HELLO,
                    serde_json::json!({"client": "cli", "clientVersion": "1", "protocol": 1})
                )
                .await
                .is_ok()
        );
    }
}
