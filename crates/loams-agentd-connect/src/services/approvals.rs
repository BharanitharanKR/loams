//! `ApprovalService` (G17, G18, G22; SD1 Task 13): each method's ndjson twin
//! is the method of the same name. `ResolveApproval` is DD1 Task 24's; on
//! the loopback listener only the owner token reaches it, and on the remote
//! listener the `studio` role does (it is not in `STUDIO_EXCLUDED`).

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};

use super::{Agentd, stream, unary};
use crate::map::Wire;
use crate::proto::loams::agentd::v1 as pb;

impl pb::ApprovalService for Agentd {
    async fn watch_approvals(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchApprovalsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchApprovalsResponse>> {
        stream!(
            self,
            request,
            WatchApprovals,
            |req| Wire::from_pb(req)?,
            |list| list.to_pb()
        )
    }

    async fn resolve_approval(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ResolveApprovalRequest>,
    ) -> ServiceResult<pb::ResolveApprovalResponse> {
        unary!(
            self,
            request,
            ResolveApproval,
            |req| Wire::from_pb(req)?,
            |ok| pb::ResolveApprovalResponse::default()
        )
    }

    async fn set_thread_approval_mode(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetThreadApprovalModeRequest>,
    ) -> ServiceResult<pb::SetThreadApprovalModeResponse> {
        unary!(
            self,
            request,
            SetThreadApprovalMode,
            |req| Wire::from_pb(req)?,
            |ok| pb::SetThreadApprovalModeResponse::default()
        )
    }
}
