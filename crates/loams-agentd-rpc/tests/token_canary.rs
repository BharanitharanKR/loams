//! No token reaches a log line (design §50 §7, plan DD1 Task 7). A test binary
//! of its own: the capture is the process-wide subscriber, so it sees every
//! thread, and no other test's thread can race `tracing`'s callsite cache.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use loams_agentd_proto::rpc::{FORBIDDEN_ROLE, Role};
use loams_agentd_rpc::{
    Connections, Credentials, Identity, Policy, RpcError, RpcReply, RpcService, ScopedTokens,
    Secret, WsServer, connect_ws, connect_ws_with_token, methods,
};

const OWNER_TOKEN: &str = "canary-owner-token-0123456789abcdefghijklmn";

struct Service {
    role: Role,
}

#[async_trait]
impl RpcService for Service {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        match method {
            methods::HELLO => Ok(RpcReply::Value(serde_json::json!({ "role": self.role }))),
            "Echo" | "Secret" => Ok(RpcReply::Value(params)),
            other => Err(RpcError::UnknownMethod(other.into())),
        }
    }
}

struct Open;

impl Connections for Open {
    fn open(&self, identity: &Identity) -> Arc<dyn RpcService> {
        Arc::new(Service {
            role: identity.role,
        })
    }
}

struct OwnerSecrets;

#[async_trait]
impl Policy for OwnerSecrets {
    async fn authorize(
        &self,
        identity: &Identity,
        method: &str,
        _params: &serde_json::Value,
    ) -> Result<(), RpcError> {
        if method == "Secret" && identity.role != Role::Owner {
            return Err(RpcError::Failed(format!("{FORBIDDEN_ROLE}: no")));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn dial(url: &str, token: &str) -> loams_agentd_rpc::RpcClient {
    let client = connect_ws_with_token(url, token).await.unwrap();
    client
        .call(
            methods::HELLO,
            serde_json::json!({ "client": "cli", "clientVersion": "t", "protocol": 1 }),
        )
        .await
        .unwrap();
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn token_never_logged() {
    let capture = Capture::default();
    // TRACE everywhere, as `RUST_LOG=trace` would, with the crate's directives.
    let mut filter = tracing_subscriber::EnvFilter::new("trace");
    for directive in loams_agentd_rpc::SECRET_SAFE_LOG_DIRECTIVES {
        filter = filter.add_directive(directive.parse().unwrap());
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(capture.clone())
        .init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let scoped = Arc::new(ScopedTokens::new());
    let server = WsServer::new(
        Arc::new(Credentials::new(Secret::new(OWNER_TOKEN), scoped.clone())),
        Arc::new(Open),
        Arc::new(OwnerSecrets),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(server.serve_until(listener, async {
        let _ = stopped.await;
    }));

    let run = scoped.issue("chat-1").unwrap();
    let near_miss = format!("{}x", &OWNER_TOKEN[..OWNER_TOKEN.len() - 1]);
    // Refusals: no token, a near miss, and a revoked run token.
    assert!(connect_ws(&url).await.is_err());
    assert!(connect_ws_with_token(&url, &near_miss).await.is_err());
    let revoked = scoped.issue("chat-2").unwrap();
    scoped.revoke(&revoked.scope.run_id);
    assert!(
        connect_ws_with_token(&url, revoked.token.expose())
            .await
            .is_err()
    );
    // Successes, a refused call and an error reply.
    let owner = dial(&url, OWNER_TOKEN).await;
    let mcp = dial(&url, run.token.expose()).await;
    assert!(mcp.call("Secret", serde_json::json!(1)).await.is_err());
    assert!(owner.call("Missing", serde_json::json!(1)).await.is_err());
    drop((owner, mcp));
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("401"), "the refusals were logged: {logs}");
    for secret in [
        OWNER_TOKEN,
        near_miss.as_str(),
        run.token.expose(),
        revoked.token.expose(),
    ] {
        assert!(!logs.contains(secret), "a token reached the log:\n{logs}");
    }
}
