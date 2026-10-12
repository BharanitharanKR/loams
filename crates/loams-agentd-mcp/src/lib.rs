//! loams-agentd-mcp — a Model Context Protocol server over the running engine.
//!
//! `loams-agentd mcp` speaks MCP (JSON-RPC 2.0, newline-delimited) on stdin/stdout
//! and proxies every tool into the engine's localhost IPC — the same
//! `loams_agentd_rpc` WebSocket the headed app and `loams-desktop sync` dial. Nothing here
//! talks to the edge or touches the filesystem: the engine stays the single
//! authority for chats, devices, projects, and the command plane.
//!
//! The server is hand-rolled rather than an SDK: the MCP surface a stdio
//! tool server needs is five methods (`initialize`, `ping`, `tools/list`,
//! `tools/call`, and the `initialized` notification), and the repo already
//! owns JSON-RPC framing for the Codex and ACP drivers. Zero new crates.
//!
//! When the daemon launches a harness it injects this server into the agent's
//! MCP config with three keys in its environment: `LOAMS_AGENTD_RUN_TOKEN` (a
//! token scoped to that run, role `mcp`), `LOAMS_AGENTD_PORT` and
//! `LOAMS_AGENTD_SESSION_ID` (design §50 §7). The token reaches only the
//! shim's methods on the run's own session and its side chats. Every send
//! from such an agent is attributed to its originating chat, and a chat can
//! never message itself.

mod jsonrpc;
mod loams_desktop;
mod tools;
mod transcript;

pub use jsonrpc::serve_stdio;
pub use loams_desktop::{Endpoint, LoamsDesktop, Origin};
pub use tools::{ToolDef, Tools};
pub use transcript::{RenderOptions, RenderedMessage, render_entries};

/// How `loams-agentd mcp` finds the daemon and who it speaks for.
#[derive(Debug, Clone)]
pub struct McpConfig {
    pub endpoint: Endpoint,
    /// The chat whose agent spawned this server, when injected by the daemon.
    pub origin: Origin,
}

impl McpConfig {
    /// Resolve from the process environment: `LOAMS_AGENTD_PORT` and
    /// `LOAMS_AGENTD_RUN_TOKEN` for the daemon, `LOAMS_AGENTD_SESSION_ID` for
    /// the originating chat. The caller may fill a missing port from the
    /// daemon's discovery file.
    pub fn from_env() -> Self {
        let read = |key: &str| {
            std::env::var(key)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        Self {
            endpoint: Endpoint {
                port: read(loams_agentd_proto::rpc::ENV_PORT).and_then(|p| p.parse().ok()),
                token: read(loams_agentd_proto::rpc::ENV_RUN_TOKEN)
                    .map(loams_agentd_rpc::Secret::new),
            },
            origin: Origin::from_env(),
        }
    }
}

/// Run the MCP server on this process's stdin/stdout until stdin closes.
pub async fn run(config: McpConfig) -> anyhow::Result<()> {
    let loams_desktop = LoamsDesktop::new(config.endpoint, config.origin);
    let tools = Tools::new(std::sync::Arc::new(loams_desktop));
    serve_stdio(std::sync::Arc::new(tools)).await
}
