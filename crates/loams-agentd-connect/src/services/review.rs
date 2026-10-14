//! `ReviewService` (§53 §6.5 G8–G11, SD1 Task 11).

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};

use super::{Agentd, stream, unary};
use crate::map::{MapError, Wire};
use crate::proto::loams::agentd::v1 as pb;

impl pb::ReviewService for Agentd {
    async fn watch_review_comments(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchReviewCommentsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchReviewCommentsResponse>> {
        stream!(
            self,
            request,
            WatchReviewComments,
            |req| Wire::from_pb(req)?,
            |frame| frame.to_pb()
        )
    }

    async fn upsert_review_comment(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::UpsertReviewCommentRequest>,
    ) -> ServiceResult<pb::UpsertReviewCommentResponse> {
        unary!(
            self,
            request,
            UpsertReviewComment,
            |req| Wire::from_pb(
                req.comment
                    .into_option()
                    .ok_or_else(|| MapError::missing("comment"))?
            )?,
            |reply| pb::UpsertReviewCommentResponse {
                id: reply.id,
                ..Default::default()
            }
        )
    }

    async fn delete_review_comment(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteReviewCommentRequest>,
    ) -> ServiceResult<pb::DeleteReviewCommentResponse> {
        unary!(
            self,
            request,
            DeleteReviewComment,
            |req| Wire::from_pb(req)?,
            |reply| pb::DeleteReviewCommentResponse {
                id: reply.id,
                ..Default::default()
            }
        )
    }

    async fn resolve_review_comment(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ResolveReviewCommentRequest>,
    ) -> ServiceResult<pb::ResolveReviewCommentResponse> {
        unary!(
            self,
            request,
            ResolveReviewComment,
            |req| Wire::from_pb(req)?,
            |reply| pb::ResolveReviewCommentResponse {
                id: reply.id,
                ..Default::default()
            }
        )
    }

    async fn send_review_to_thread(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SendReviewToThreadRequest>,
    ) -> ServiceResult<pb::SendReviewToThreadResponse> {
        unary!(
            self,
            request,
            SendReviewToThread,
            |req| Wire::from_pb(req)?,
            |reply| pb::SendReviewToThreadResponse {
                command_id: reply.command_id,
                ..Default::default()
            }
        )
    }

    async fn post_review_to_change_request(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::PostReviewToChangeRequestRequest>,
    ) -> ServiceResult<pb::PostReviewToChangeRequestResponse> {
        unary!(
            self,
            request,
            PostReviewToChangeRequest,
            |req| Wire::from_pb(req)?,
            |reply| reply.to_pb()
        )
    }
}
