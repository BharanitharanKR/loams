//! Which role may call what (design §50 §7, plan DD1 Task 7).
//!
//! Role `owner` (the main token) may call every method. Role `mcp` (a scoped
//! run token) may call only the methods the injected MCP shim uses, and only
//! on its own session or on the side chats under it: the shim's tools create
//! side chats, message them and read them back, so the session's tree is
//! what a run may touch. Everything else answers `forbidden_role`, including
//! `ResolveApproval` (Ruling 8) and the daemon's lifecycle methods.
//!
//! The method set is written out here until Task 9 generates the method
//! table (`METHODS`) with a role per method.

use std::sync::Arc;

use async_trait::async_trait;
use loams_agentd_proto::rpc::{FORBIDDEN_ROLE, Role};
use loams_agentd_rpc::{Identity, Policy, RpcError, RunScope, methods};
use loams_agentd_sessions::WorkspaceHost;

/// The methods role `mcp` may call: the MCP shim's.
pub const MCP_METHODS: &[&str] = &[
    methods::LOCAL_DEVICE,
    methods::ENGINE_INFO,
    methods::LIST_HARNESSES,
    methods::LIST_MODELS,
    methods::WATCH_SPACES,
    methods::WATCH_CHATS,
    methods::WATCH_SESSIONS,
    methods::WATCH_DOC_MESSAGES,
    methods::QUEUE_COMMAND,
    methods::QUEUE_MESSAGE,
    methods::MUTATE,
];

/// Methods that act on one session, named by `sessionId` or the fork's `chatId`.
const SESSION_METHODS: &[&str] = &[
    methods::WATCH_DOC_MESSAGES,
    methods::QUEUE_COMMAND,
    methods::QUEUE_MESSAGE,
];

/// The `Mutate` ops role `mcp` may send.
const MCP_MUTATIONS: &[&str] = &["createChat", "renameChat", "setChatArchived"];

/// How far up a chat's parents the session check walks.
const MAX_DEPTH: usize = 8;

/// The chat tree, as far as the policy needs it.
pub trait ChatTree: Send + Sync + 'static {
    /// `None`: no such chat. `Some(None)`: a top-level chat.
    fn parent_of(&self, chat_id: &str) -> Option<Option<String>>;
}

impl ChatTree for WorkspaceHost {
    fn parent_of(&self, chat_id: &str) -> Option<Option<String>> {
        match self.chat(chat_id) {
            Ok(chat) => chat.map(|chat| chat.parent_chat_id),
            Err(error) => {
                tracing::warn!(%error, "role check could not read the chat tree");
                None
            }
        }
    }
}

/// The daemon's [`Policy`].
pub struct DaemonPolicy {
    chats: Arc<dyn ChatTree>,
}

impl std::fmt::Debug for DaemonPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonPolicy").finish_non_exhaustive()
    }
}

impl DaemonPolicy {
    pub fn new(chats: Arc<dyn ChatTree>) -> Self {
        Self { chats }
    }

    /// Whether `target` is the run's session or a chat under it.
    fn in_session(&self, run: &RunScope, target: &str) -> bool {
        let mut chat = target.to_owned();
        for _ in 0..MAX_DEPTH {
            if chat == run.session_id {
                return true;
            }
            match self.chats.parent_of(&chat) {
                Some(Some(parent)) => chat = parent,
                _ => return false,
            }
        }
        false
    }

    fn authorize_mcp(
        &self,
        run: &RunScope,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<(), RpcError> {
        if !MCP_METHODS.contains(&method) {
            return Err(forbidden(format!("{method} needs role owner")));
        }
        let outside = || forbidden(format!("{method} on a session outside this run's"));
        if method == methods::MUTATE {
            let op = text(params, "op").unwrap_or_default();
            if !MCP_MUTATIONS.contains(&op) {
                return Err(forbidden(format!("Mutate {op} needs role owner")));
            }
            let chat_id = text(params, "chatId").ok_or_else(outside)?;
            if op == "createChat" {
                // A new side chat under the run's session tree; an existing
                // id must already be in it.
                let parent = text(params, "parentChatId").ok_or_else(outside)?;
                let exists = self.chats.parent_of(chat_id).is_some();
                if !self.in_session(run, parent) || (exists && !self.in_session(run, chat_id)) {
                    return Err(outside());
                }
                return Ok(());
            }
            return if self.in_session(run, chat_id) {
                Ok(())
            } else {
                Err(outside())
            };
        }
        let mut named = false;
        for key in ["sessionId", "chatId"] {
            if let Some(target) = params.get(key) {
                named = true;
                match target.as_str() {
                    Some(target) if self.in_session(run, target) => {}
                    _ => return Err(outside()),
                }
            }
        }
        if SESSION_METHODS.contains(&method) && !named {
            return Err(outside());
        }
        Ok(())
    }
}

#[async_trait]
impl Policy for DaemonPolicy {
    async fn authorize(
        &self,
        identity: &Identity,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<(), RpcError> {
        match (identity.role, identity.run.as_ref()) {
            (Role::Owner, _) => Ok(()),
            (Role::Mcp, Some(run)) => self.authorize_mcp(run, method, params),
            (Role::Mcp, None) => Err(forbidden(format!("{method} needs a run"))),
        }
    }
}

fn text<'p>(params: &'p serde_json::Value, key: &str) -> Option<&'p str> {
    params.get(key).and_then(serde_json::Value::as_str)
}

fn forbidden(detail: String) -> RpcError {
    RpcError::Failed(format!("{FORBIDDEN_ROLE}: {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    /// `main` and `other` are top-level; `side` is under `main`.
    struct Tree(HashMap<&'static str, Option<&'static str>>);

    impl ChatTree for Tree {
        fn parent_of(&self, chat_id: &str) -> Option<Option<String>> {
            self.0.get(chat_id).map(|parent| parent.map(str::to_owned))
        }
    }

    fn policy() -> DaemonPolicy {
        DaemonPolicy::new(Arc::new(Tree(HashMap::from([
            ("main", None),
            ("other", None),
            ("side", Some("main")),
            ("other-side", Some("other")),
        ]))))
    }

    fn mcp(session: &str) -> Identity {
        Identity {
            role: Role::Mcp,
            run: Some(RunScope {
                run_id: "run-1".into(),
                session_id: session.into(),
            }),
        }
    }

    async fn allowed(identity: &Identity, method: &str, params: serde_json::Value) -> bool {
        match policy().authorize(identity, method, &params).await {
            Ok(()) => true,
            Err(error) => {
                assert!(error.to_string().starts_with(FORBIDDEN_ROLE), "{error}");
                false
            }
        }
    }

    #[tokio::test]
    async fn owner_may_call_anything() {
        for method in ["ResolveApproval", "StopEngine", "Mutate", "AnythingAtAll"] {
            assert!(allowed(&Identity::owner(), method, json!({"op": "deleteChat"})).await);
        }
    }

    #[tokio::test]
    async fn mcp_may_call_only_the_shims_methods() {
        let run = mcp("main");
        for method in [
            "ResolveApproval",
            methods::STOP_ENGINE,
            methods::WRITE_WORKSPACE_FILE,
            methods::OPEN_TERMINAL,
            methods::INSTALL_HARNESS,
            methods::FETCH_TOOL_BLOB,
        ] {
            assert!(
                !allowed(&run, method, json!({"chatId": "main"})).await,
                "{method}"
            );
        }
        for method in [
            methods::LIST_HARNESSES,
            methods::WATCH_CHATS,
            methods::LOCAL_DEVICE,
        ] {
            assert!(allowed(&run, method, json!({})).await, "{method}");
        }
    }

    #[tokio::test]
    async fn mcp_is_bound_to_its_session_tree() {
        let run = mcp("main");
        for method in SESSION_METHODS {
            assert!(allowed(&run, method, json!({"chatId": "main"})).await);
            assert!(allowed(&run, method, json!({"chatId": "side"})).await);
            assert!(allowed(&run, method, json!({"sessionId": "side"})).await);
            assert!(!allowed(&run, method, json!({"chatId": "other"})).await);
            assert!(!allowed(&run, method, json!({"chatId": "other-side"})).await);
            assert!(!allowed(&run, method, json!({"chatId": "missing"})).await);
            assert!(!allowed(&run, method, json!({"chatId": 7})).await);
            assert!(!allowed(&run, method, json!({})).await);
            // Both names must agree with the session.
            assert!(
                !allowed(
                    &run,
                    method,
                    json!({"sessionId": "main", "chatId": "other"})
                )
                .await
            );
        }
        // A side chat's run reaches only itself.
        let side = mcp("side");
        assert!(allowed(&side, methods::QUEUE_MESSAGE, json!({"chatId": "side"})).await);
        assert!(!allowed(&side, methods::QUEUE_MESSAGE, json!({"chatId": "main"})).await);
    }

    #[tokio::test]
    async fn mcp_mutations_stay_in_the_tree() {
        let run = mcp("main");
        let create = |chat: &str, parent: Option<&str>| {
            let mut params = json!({"op": "createChat", "chatId": chat});
            if let Some(parent) = parent {
                params["parentChatId"] = json!(parent);
            }
            params
        };
        assert!(allowed(&run, methods::MUTATE, create("new", Some("main"))).await);
        assert!(!allowed(&run, methods::MUTATE, create("new", None)).await);
        assert!(!allowed(&run, methods::MUTATE, create("new", Some("other"))).await);
        // An existing id outside the tree cannot be re-created under it.
        assert!(!allowed(&run, methods::MUTATE, create("other", Some("main"))).await);
        for op in ["renameChat", "setChatArchived"] {
            assert!(allowed(&run, methods::MUTATE, json!({"op": op, "chatId": "side"})).await);
            assert!(!allowed(&run, methods::MUTATE, json!({"op": op, "chatId": "other"})).await);
        }
        for op in [
            "deleteChat",
            "deleteSpace",
            "createSpace",
            "setChatCwd",
            "renameDevice",
        ] {
            assert!(!allowed(&run, methods::MUTATE, json!({"op": op, "chatId": "side"})).await);
        }
    }
}
