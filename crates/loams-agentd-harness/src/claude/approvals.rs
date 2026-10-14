//! Claude Code in a supervised thread (plan SD1 Task 13): a `can_use_tool`
//! control request becomes an approval, and the person's decision becomes
//! the control response.

use serde_json::{Value, json};

use super::wire::ControlRequestFrame;
use crate::ApprovalAsk;

/// The approval a `can_use_tool` request asks for. The call id is the tool
/// use's own (`tool_use_id`, so the card sits beside its tool chip), else
/// the control request's.
pub(crate) fn ask(req: &ControlRequestFrame) -> ApprovalAsk {
    let call_id = req
        .request
        .tool_use_id
        .clone()
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| req.request_id.clone());
    ApprovalAsk {
        call_id,
        tool: req.request.tool_name.clone(),
        args: req.request.input.clone(),
        risk: crate::approvals::tool_risk(&req.request.tool_name, &req.request.input),
    }
}

/// `can_use_tool`'s answer to a refused call: the CLI reports it to the
/// model as a denial and the turn goes on.
pub(crate) fn deny_response() -> Value {
    json!({
        "behavior": "deny",
        "message": "The person supervising this thread denied this tool call.",
    })
}
