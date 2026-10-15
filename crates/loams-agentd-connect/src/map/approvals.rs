//! approvals.proto: supervised approvals (G17, G18, G22; SD1 Task 13).

use loams_agentd_proto::approvals as rs;

use super::{MapError, Wire, from_msg, msg};
use crate::proto::loams::agentd::v1 as pb;

wire_enum!(rs::ApprovalMode => pb::ApprovalMode {
    FullAccess => APPROVAL_MODE_FULL_ACCESS,
    Supervised => APPROVAL_MODE_SUPERVISED,
});

wire_enum!(rs::ToolRisk => pb::ApprovalRisk {
    Read => APPROVAL_RISK_READ,
    Write => APPROVAL_RISK_WRITE,
    Destructive => APPROVAL_RISK_DESTRUCTIVE,
});

wire_enum!(rs::ApprovalDecision => pb::ApprovalDecision {
    Once => APPROVAL_DECISION_ONCE,
    Always => APPROVAL_DECISION_ALWAYS,
    Deny => APPROVAL_DECISION_DENY,
});

/// By hand: the proto's `turn_id` is a plain string, empty when the turn is
/// not known.
impl Wire for rs::Approval {
    type Pb = pb::Approval;
    fn to_pb(&self) -> pb::Approval {
        let Self {
            id,
            chat_id,
            turn_id,
            tool,
            args,
            risk,
            created_at_ms,
        } = self;
        pb::Approval {
            id: id.clone(),
            chat_id: chat_id.clone(),
            turn_id: turn_id.clone().unwrap_or_default(),
            tool: tool.clone(),
            args: msg(args),
            risk: risk.to_pb(),
            created_at_ms: *created_at_ms,
            ..Default::default()
        }
    }
    fn from_pb(pb: pb::Approval) -> Result<Self, MapError> {
        Ok(Self {
            id: pb.id,
            chat_id: pb.chat_id,
            turn_id: Some(pb.turn_id).filter(|turn| !turn.is_empty()),
            tool: pb.tool,
            args: from_msg(pb.args, "args")?,
            risk: Wire::from_pb(pb.risk)?,
            created_at_ms: pb.created_at_ms,
        })
    }
}

wire_struct!(rs::WatchApprovalsParams => pb::WatchApprovalsRequest {
    chat_id => chat_id,
});

wire_struct!(rs::ApprovalList => pb::WatchApprovalsResponse {
    pending => pending,
    mode => mode,
});

wire_struct!(rs::ResolveApprovalParams => pb::ResolveApprovalRequest {
    call_id => call_id,
    chat_id => chat_id,
    decision => decision,
});

wire_struct!(rs::SetThreadApprovalModeParams => pb::SetThreadApprovalModeRequest {
    chat_id => chat_id,
    mode => mode,
});
