//! Supervised approvals (design §53 §8.1, §6.5 G17, G18, G22, D911; plan
//! SD1 Task 13, Ruling 7), on DD1 Task 24's contract.
//!
//! Each thread has an approval mode. In `full-access` the harness drivers
//! answer every tool permission themselves, as they always have. In
//! `supervised` they ask instead: each request becomes a pending
//! [`Approval`], announced in the run's events as
//! [`crate::AgentEvent::ApprovalRequested`], and the tool waits until a
//! person answers it with `ResolveApproval` (roles `owner` and `studio`,
//! never `mcp`: DD1 Ruling 8) or until it times out, which denies it.
//!
//! - `WatchApprovals` (G17) streams the pending approvals, again on each
//!   change; with a `chatId` also that thread's effective mode.
//! - `ResolveApproval` (G18) answers one: `once`, `always` (remembered for
//!   the thread and the tool) or `deny`; `approval_not_pending` when nothing
//!   waits under that id.
//! - `SetThreadApprovalMode` (G22) sets a thread's mode.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The daemon feature these methods add to `Hello` and `GetHost`.
pub const FEATURE: &str = "approvals.supervised";

/// Error reason: `ResolveApproval` names no pending approval (Connect
/// `failed_precondition`).
pub const APPROVAL_NOT_PENDING: &str = "approval_not_pending";

/// How a thread's agent tool calls are answered (§53 §8.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
pub enum ApprovalMode {
    /// The harness answers every tool permission itself (the behaviour before
    /// SD1 Task 13), labelled plainly in the thread header.
    #[default]
    #[serde(rename = "full-access")]
    FullAccess,
    /// Every tool permission waits for a person's decision.
    #[serde(rename = "supervised")]
    Supervised,
}

impl ApprovalMode {
    /// The mode's wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullAccess => "full-access",
            Self::Supervised => "supervised",
        }
    }

    /// The mode a wire name names.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "full-access" => Some(Self::FullAccess),
            "supervised" => Some(Self::Supervised),
            _ => None,
        }
    }
}

/// What running a tool may do (DD1's `ToolRisk`, with §53's `destructive`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ToolRisk {
    /// Reads only.
    Read,
    /// Writes files or runs a command.
    Write,
    /// A command that removes or rewrites what may not come back
    /// (`rm -r`, a forced push, a hard reset, …).
    Destructive,
}

/// A person's answer (DD1's `ApprovalDecision`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Allow this call.
    Once,
    /// Allow this call, and every later call of the same tool in this thread.
    Always,
    /// Refuse this call.
    Deny,
}

impl ApprovalDecision {
    /// Whether the tool may run.
    pub fn allows(self) -> bool {
        !matches!(self, Self::Deny)
    }
}

/// One pending approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    /// The tool call's id, as the harness names it (`ResolveApproval.callId`).
    pub id: String,
    pub chat_id: String,
    /// The user message whose turn asked, when known.
    #[serde(default)]
    pub turn_id: Option<String>,
    /// The harness's tool name (`Bash`, `commandExecution`, `edit`, …).
    pub tool: String,
    /// The tool's arguments, redacted (never stored or sent unredacted).
    pub args: serde_json::Value,
    pub risk: ToolRisk,
    pub created_at_ms: i64,
}

/// `WatchApprovals` (G17).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WatchApprovalsParams {
    /// Only this thread's approvals; the item then carries its mode too.
    #[serde(default)]
    pub chat_id: Option<String>,
}

/// `WatchApprovals`' item: every pending approval, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalList {
    pub pending: Vec<Approval>,
    /// The watched thread's effective mode (with `chatId` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ApprovalMode>,
}

/// `ResolveApproval` (G18): DD1's `{callId, decision}`, plus the thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResolveApprovalParams {
    pub call_id: String,
    /// The thread. Optional while the call id is unique among the pending
    /// approvals; DD1's `sessionId` is accepted as its other name.
    #[serde(default, alias = "sessionId")]
    pub chat_id: Option<String>,
    pub decision: ApprovalDecision,
}

/// `SetThreadApprovalMode` (G22).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SetThreadApprovalModeParams {
    pub chat_id: String,
    pub mode: ApprovalMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names() {
        assert_eq!(
            serde_json::to_value(ApprovalMode::FullAccess).unwrap(),
            "full-access"
        );
        assert_eq!(
            ApprovalMode::parse("supervised"),
            Some(ApprovalMode::Supervised)
        );
        assert_eq!(
            serde_json::to_value(ToolRisk::Destructive).unwrap(),
            "destructive"
        );
        assert_eq!(
            serde_json::to_value(ApprovalDecision::Always).unwrap(),
            "always"
        );
        // DD1's `sessionId` names the thread too.
        let params: ResolveApprovalParams = serde_json::from_value(
            serde_json::json!({"callId": "c1", "sessionId": "chat-1", "decision": "once"}),
        )
        .unwrap();
        assert_eq!(params.chat_id.as_deref(), Some("chat-1"));
    }
}
