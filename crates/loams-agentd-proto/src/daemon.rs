//! The daemon's process model on the wire: its mode and its discovery file
//! (design §50 §5.4, plan DD1 Task 6).

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// How the daemon was started: by the per-user service manager, or by Loams
/// Desktop as its child process (§50 §5.5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum DaemonMode {
    Service,
    #[default]
    Child,
}

/// `agentd.json`: written atomically (0600) after the RPC listener is bound,
/// removed on a clean exit. A file whose `pid` does not hold the
/// single-instance lock is stale and ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Discovery {
    pub protocol: u32,
    pub version: String,
    pub pid: u32,
    /// The loopback port of the RPC WebSocket (`127.0.0.1`).
    pub port: u16,
    pub started_at_ms: i64,
    pub mode: DaemonMode,
    /// The loopback port of the `loams.agentd.v1` Connect listener
    /// (`127.0.0.1`; design §53 §6.2, plan SD1 Task 5). Absent in a file an
    /// older daemon wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_port: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_wire_shape_matches_the_design() {
        let discovery = Discovery {
            protocol: 1,
            version: "0.2.1".into(),
            pid: 41234,
            port: 50217,
            started_at_ms: 1_791_500_000_000,
            mode: DaemonMode::Service,
            connect_port: Some(50218),
        };
        assert_eq!(
            serde_json::to_value(&discovery).expect("serialize"),
            serde_json::json!({
                "protocol": 1, "version": "0.2.1", "pid": 41234, "port": 50217,
                "startedAtMs": 1_791_500_000_000_i64, "mode": "service", "connectPort": 50218
            })
        );
        // A file an older daemon wrote has no `connectPort`.
        let older: Discovery = serde_json::from_value(serde_json::json!({
            "protocol": 1, "version": "0.2.1", "pid": 41234, "port": 50217,
            "startedAtMs": 1, "mode": "child"
        }))
        .expect("parse");
        assert_eq!(older.connect_port, None);
    }
}
