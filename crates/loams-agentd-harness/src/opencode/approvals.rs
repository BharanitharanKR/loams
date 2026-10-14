//! opencode in a supervised thread (plan SD1 Task 13): its `permission.asked`
//! event becomes an approval, and the person's decision becomes the reply.

use serde_json::Value;

use crate::ApprovalAsk;

/// The approval a `permission.asked` event asks for. The tool is the
/// permission's name (`bash`, `edit`, …; 1.x also spells it `type`); the call
/// id is the tool call's (`tool.callID`, so the card sits beside its tool
/// chip), else the permission's own id. The arguments are the event's
/// properties: the patterns, the metadata and the title.
pub(crate) fn ask(permission_id: &str, props: &Value) -> ApprovalAsk {
    let tool = ["permission", "type", "tool"]
        .iter()
        .find_map(|key| props.get(*key).and_then(Value::as_str))
        .unwrap_or("permission")
        .to_owned();
    let call_id = props
        .pointer("/tool/callID")
        .or_else(|| props.get("callID"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .unwrap_or(permission_id)
        .to_owned();
    let mut args = serde_json::Map::new();
    for key in ["title", "patterns", "pattern", "metadata"] {
        if let Some(value) = props.get(key) {
            args.insert(key.into(), value.clone());
        }
    }
    let args = Value::Object(args);
    let risk = crate::approvals::tool_risk(&tool, &args);
    ApprovalAsk {
        call_id,
        tool,
        args,
        risk,
    }
}

/// The reply opencode is sent. "always" is the daemon's to remember (per
/// thread and tool), so opencode is told `once` and writes no rule of its own.
pub(crate) fn reply(allowed: bool) -> &'static str {
    if allowed { "once" } else { "reject" }
}
