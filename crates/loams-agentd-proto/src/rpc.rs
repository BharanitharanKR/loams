//! The RPC handshake (design §50 §5.4, plan DD1 Task 6).
//!
//! `Hello` is the first call on a connection. A client whose `protocol`
//! differs from [`AGENTD_PROTOCOL`] is answered with an error whose text
//! starts with [`PROTOCOL_MISMATCH`], and the daemon closes the connection.

use serde::{Deserialize, Serialize};

use crate::daemon::DaemonMode;

/// The wire protocol. It changes only on a breaking change (§50 §5.3).
pub const AGENTD_PROTOCOL: u32 = 1;

/// Error reason (the `{err}` text prefix) for a `Hello` with another protocol.
pub const PROTOCOL_MISMATCH: &str = "protocol_mismatch";

/// Who is calling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientKind {
    /// Loams Desktop's main process, the one general client.
    ElectronMain,
    /// `loams-agentd` itself: `status`, or a second `run` checking the first.
    Cli,
    /// The MCP shim injected into a harness run.
    Mcp,
}

/// What a connection may call (§50 §7). The main token gives `Owner`; a
/// scoped run token gives `Mcp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Mcp,
}

/// `Hello`'s params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hello {
    pub client: ClientKind,
    pub client_version: String,
    pub protocol: u32,
}

/// `Hello`'s reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloReply {
    pub protocol: u32,
    pub version: String,
    pub build_sha: String,
    pub mode: DaemonMode,
    pub pid: u32,
    pub started_at_ms: i64,
    pub role: Role,
    pub features: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_wire_shape_matches_the_design() {
        let hello: Hello = serde_json::from_value(serde_json::json!({
            "client": "electron-main", "clientVersion": "0.2.1", "protocol": 1
        }))
        .expect("parse Hello");
        assert_eq!(hello.client, ClientKind::ElectronMain);
        let reply = HelloReply {
            protocol: AGENTD_PROTOCOL,
            version: "0.2.1".into(),
            build_sha: "abc123".into(),
            mode: DaemonMode::Child,
            pid: 7,
            started_at_ms: 1,
            role: Role::Owner,
            features: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&reply).expect("serialize"),
            serde_json::json!({
                "protocol": 1, "version": "0.2.1", "buildSha": "abc123", "mode": "child",
                "pid": 7, "startedAtMs": 1, "role": "owner", "features": []
            })
        );
    }
}
