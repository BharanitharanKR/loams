//! The services that are §53 §6.5 gaps only: `ReviewService` (G8–G11, SD1
//! Task 11), `ApprovalService` (G17, G18, G22, SD1 Task 13) and
//! `NotificationService` (G19, SD1 Task 14). Each method answers
//! `unimplemented` with reason `not_implemented` until its task replaces it
//! here. `RunGroupService` (G12–G16) is served since SD1 Task 12
//! (`groups.rs`).

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

gaps!(ReviewService {
    watch_review_comments("WatchReviewComments", "G8, SD1 Task 11"):
        WatchReviewCommentsRequest -> ServiceStream<pb::WatchReviewCommentsResponse>;
    upsert_review_comment("UpsertReviewComment", "G9, SD1 Task 11"):
        UpsertReviewCommentRequest -> pb::UpsertReviewCommentResponse;
    delete_review_comment("DeleteReviewComment", "G9, SD1 Task 11"):
        DeleteReviewCommentRequest -> pb::DeleteReviewCommentResponse;
    resolve_review_comment("ResolveReviewComment", "G9, SD1 Task 11"):
        ResolveReviewCommentRequest -> pb::ResolveReviewCommentResponse;
    send_review_to_thread("SendReviewToThread", "G10, SD1 Task 11"):
        SendReviewToThreadRequest -> pb::SendReviewToThreadResponse;
    post_review_to_change_request("PostReviewToChangeRequest", "G11, SD1 Task 11"):
        PostReviewToChangeRequestRequest -> pb::PostReviewToChangeRequestResponse;
});

gaps!(ApprovalService {
    watch_approvals("WatchApprovals", "G17, SD1 Task 13"):
        WatchApprovalsRequest -> ServiceStream<pb::WatchApprovalsResponse>;
    resolve_approval("ResolveApproval", "G18, SD1 Task 13"):
        ResolveApprovalRequest -> pb::ResolveApprovalResponse;
    set_thread_approval_mode("SetThreadApprovalMode", "G22, SD1 Task 13"):
        SetThreadApprovalModeRequest -> pb::SetThreadApprovalModeResponse;
});

gaps!(NotificationService {
    watch_notifications("WatchNotifications", "G19, SD1 Task 14"):
        WatchNotificationsRequest -> ServiceStream<pb::WatchNotificationsResponse>;
});
