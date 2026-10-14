//! Codex in a supervised thread (plan SD1 Task 13): the app server runs with
//! `approvalPolicy: "on-request"` in the thread's own sandbox, and each
//! `item/commandExecution/requestApproval` or `item/fileChange/requestApproval`
//! becomes an approval whose decision is the request's answer.

use loams_agentd_proto::approvals::ToolRisk;
use serde_json::{Value, json};

use crate::ApprovalAsk;

/// The approval policy on the wire: `on-request` when supervised, else
/// `never` (unattended full access).
pub(crate) fn approval_policy(supervised: bool) -> &'static str {
    if supervised { "on-request" } else { "never" }
}

/// The approval an app-server approval request asks for. The tool is the
/// item type (`commandExecution`, `fileChange`), the call id the item's id.
pub(crate) fn ask(method: &str, id: &Value, params: &Value) -> ApprovalAsk {
    let tool = if method.contains("commandExecution") {
        "commandExecution"
    } else {
        "fileChange"
    };
    let call_id = ["itemId", "callId", "approvalId"]
        .iter()
        .find_map(|key| params.get(*key).and_then(Value::as_str))
        .map(str::to_owned)
        .unwrap_or_else(|| match id {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        });
    let risk = if tool == "commandExecution" {
        crate::approvals::command_of(params)
            .map_or(ToolRisk::Write, |command| crate::approvals::command_risk(&command))
    } else {
        ToolRisk::Write
    };
    ApprovalAsk {
        call_id,
        tool: tool.into(),
        args: params.clone(),
        risk,
    }
}

/// The approval request's answer.
pub(crate) fn decision(allowed: bool) -> Value {
    json!({ "decision": if allowed { "accept" } else { "decline" } })
}
