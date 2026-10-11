//! Per-call access control (design §50 §7, plan DD1 Task 7).
//!
//! Every call on an authenticated connection passes [`Gated`]: a scoped run
//! token that was revoked ends the connection (`unauthenticated`), and the
//! server's [`Policy`] decides whether the connection's role may call the
//! method with these params (`forbidden_role`). `Hello` is open to every
//! role. That `Hello` comes first is the connection loop's rule
//! (`server.rs`), since it must hold for frames batched in one message.

use std::sync::Arc;

use async_trait::async_trait;
use loams_agentd_proto::rpc::UNAUTHENTICATED;

use crate::auth::{Credentials, Identity};
use crate::{RpcError, RpcReply, RpcService, methods};

/// Which methods a role may call, and on what.
#[async_trait]
pub trait Policy: Send + Sync + 'static {
    /// `Ok` lets the call through; an error (`forbidden_role: …`) is the reply.
    async fn authorize(
        &self,
        identity: &Identity,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<(), RpcError>;
}

/// One connection's service behind its identity's checks.
pub struct Gated {
    identity: Identity,
    credentials: Arc<Credentials>,
    policy: Arc<dyn Policy>,
    inner: Arc<dyn RpcService>,
}

impl Gated {
    pub fn new(
        identity: Identity,
        credentials: Arc<Credentials>,
        policy: Arc<dyn Policy>,
        inner: Arc<dyn RpcService>,
    ) -> Self {
        Self {
            identity,
            credentials,
            policy,
            inner,
        }
    }
}

impl std::fmt::Debug for Gated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gated")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RpcService for Gated {
    async fn handle(&self, method: &str, params: serde_json::Value) -> Result<RpcReply, RpcError> {
        if !self.credentials.still_valid(&self.identity) {
            return Err(RpcError::Fatal(format!(
                "{UNAUTHENTICATED}: this run's token was revoked"
            )));
        }
        if method != methods::HELLO {
            self.policy
                .authorize(&self.identity, method, &params)
                .await?;
        }
        self.inner.handle(method, params).await
    }
}
