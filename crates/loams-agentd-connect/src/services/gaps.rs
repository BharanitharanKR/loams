//! The services that are §53 §6.5 gaps only: `ApprovalService` (G17, G18,
//! G22, SD1 Task 13). Each method answers `unimplemented` with reason
//! `not_implemented` until its task replaces it here (`NotificationService`,
//! G19, is served since SD1 Task 14: `notifications.rs`).

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};

use super::Agentd;
use crate::error::not_implemented;
use crate::proto::loams::agentd::v1 as pb;

/// One gap method: its name, its gap and task, its request and response.
macro_rules! gaps {
    ($trait:ident { $($fn:ident($rpc:literal, $gap:literal): $req:ident -> $resp:ty;)* }) => {
        impl pb::$trait for Agentd {
            $(
                async fn $fn(
                    &self,
                    _ctx: RequestContext,
                    _request: ServiceRequest<'_, pb::$req>,
                ) -> ServiceResult<$resp> {
                    Err(not_implemented($rpc, $gap))
                }
            )*
        }
    };
}

gaps!(ApprovalService {
    watch_approvals("WatchApprovals", "G17, SD1 Task 13"):
        WatchApprovalsRequest -> ServiceStream<pb::WatchApprovalsResponse>;
    resolve_approval("ResolveApproval", "G18, SD1 Task 13"):
        ResolveApprovalRequest -> pb::ResolveApprovalResponse;
    set_thread_approval_mode("SetThreadApprovalMode", "G22, SD1 Task 13"):
        SetThreadApprovalModeRequest -> pb::SetThreadApprovalModeResponse;
});
