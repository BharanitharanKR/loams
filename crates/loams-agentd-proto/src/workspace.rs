//! Workspace lifecycle types shared by the engine and its clients.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Protocol features are advertised explicitly because personal/integration
/// builds may share a semver with upstream while exposing a different RPC and
/// document surface.
pub mod capabilities {
    /// The host decodes durable composer references at the harness boundary.
    pub const COMPOSER_REFERENCES_V1: &str = "composer-references-v1";
    pub const MESSAGE_QUEUE_V1: &str = "message-queue-v1";
    pub const MESSAGE_QUEUE_ACTIONS_V1: &str = "message-queue-actions-v1";
    pub const MESSAGE_QUEUE_ATTACHMENTS_V1: &str = "message-queue-attachments-v1";
    pub const MESSAGE_QUEUE_CLEAN_ATTACHMENT_TEXT_V1: &str =
        "message-queue-clean-attachment-text-v1";
    pub const MESSAGE_QUEUE_EDIT_LEASE_V1: &str = "message-queue-edit-lease-v1";
    pub const HARNESS_UPDATES_V1: &str = "harness-updates-v1";

    pub const CURRENT: &[&str] = &[
        COMPOSER_REFERENCES_V1,
        MESSAGE_QUEUE_V1,
        MESSAGE_QUEUE_ACTIONS_V1,
        MESSAGE_QUEUE_ATTACHMENTS_V1,
        MESSAGE_QUEUE_CLEAN_ATTACHMENT_TEXT_V1,
        MESSAGE_QUEUE_EDIT_LEASE_V1,
        HARNESS_UPDATES_V1,
    ];

    pub fn current() -> Vec<String> {
        CURRENT.iter().map(|value| (*value).to_string()).collect()
    }
}

/// The data boundary of the engine runtime. The daemon has one, `local`
/// (plan DD1 rulings T2-2, T3-1): the fork's `synced` and `development`
/// scopes went with the edge and WorkOS (trimmed in Task 9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceScope {
    #[default]
    Local,
}

/// Stable information about the engine runtime reached by a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EngineInfo {
    pub device_id: String,
    pub workspace_scope: WorkspaceScope,
    /// Supported protocol/document features. Missing on older engines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

impl EngineInfo {
    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|value| value == capability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_scope_is_local_only() {
        assert_eq!(
            serde_json::to_string(&WorkspaceScope::Local).unwrap(),
            "\"local\""
        );
        for gone in ["\"synced\"", "\"development\""] {
            assert!(
                serde_json::from_str::<WorkspaceScope>(gone).is_err(),
                "{gone}"
            );
        }
    }

    #[test]
    fn engine_info_uses_camel_case_fields() {
        let info = EngineInfo {
            device_id: "device-1".into(),
            workspace_scope: WorkspaceScope::Local,
            capabilities: capabilities::current(),
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "workspaceScope": "local",
                "capabilities": [
                    "composer-references-v1",
                    "message-queue-v1",
                    "message-queue-actions-v1",
                    "message-queue-attachments-v1",
                    "message-queue-clean-attachment-text-v1",
                    "message-queue-edit-lease-v1",
                    "harness-updates-v1"
                ],
            })
        );
    }

    #[test]
    fn old_engine_info_defaults_to_no_capabilities() {
        let info: EngineInfo = serde_json::from_value(serde_json::json!({
            "deviceId": "old",
            "workspaceScope": "local"
        }))
        .unwrap();
        assert!(info.capabilities.is_empty());
        assert!(!info.supports(capabilities::MESSAGE_QUEUE_V1));
        assert!(!info.supports(capabilities::COMPOSER_REFERENCES_V1));
    }
}
