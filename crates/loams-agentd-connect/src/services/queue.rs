//! `QueueService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::QueueService for Agentd {
    async fn watch_queue(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchQueueRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchQueueResponse>> {
        stream!(
            self,
            request,
            WatchQueue,
            |req| rs::ChatIdParams {
                chat_id: req.chat_id,
            },
            |snapshot| pb::WatchQueueResponse {
                items: snapshot.items.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn queue_message(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::QueueMessageRequest>,
    ) -> ServiceResult<pb::QueueMessageResponse> {
        unary!(
            self,
            request,
            QueueMessage,
            |req| Wire::from_pb(req)?,
            |reply| pb::QueueMessageResponse {
                id: reply.id,
                ..Default::default()
            }
        )
    }

    async fn update_queued_message(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::UpdateQueuedMessageRequest>,
    ) -> ServiceResult<pb::UpdateQueuedMessageResponse> {
        unary!(
            self,
            request,
            UpdateQueuedMessage,
            |req| Wire::from_pb(req)?,
            |reply| pb::UpdateQueuedMessageResponse {
                changed: reply.changed,
                ..Default::default()
            }
        )
    }

    async fn begin_queued_message_edit(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::BeginQueuedMessageEditRequest>,
    ) -> ServiceResult<pb::BeginQueuedMessageEditResponse> {
        unary!(
            self,
            request,
            BeginQueuedMessageEdit,
            |req| Wire::from_pb(req)?,
            |outcome| pb::BeginQueuedMessageEditResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn renew_queued_message_edit(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RenewQueuedMessageEditRequest>,
    ) -> ServiceResult<pb::RenewQueuedMessageEditResponse> {
        unary!(
            self,
            request,
            RenewQueuedMessageEdit,
            |req| Wire::from_pb(req)?,
            |outcome| pb::RenewQueuedMessageEditResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn finish_queued_message_edit(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::FinishQueuedMessageEditRequest>,
    ) -> ServiceResult<pb::FinishQueuedMessageEditResponse> {
        unary!(
            self,
            request,
            FinishQueuedMessageEdit,
            |req| Wire::from_pb(req)?,
            |outcome| pb::FinishQueuedMessageEditResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn move_queued_message(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::MoveQueuedMessageRequest>,
    ) -> ServiceResult<pb::MoveQueuedMessageResponse> {
        unary!(
            self,
            request,
            MoveQueuedMessage,
            |req| Wire::from_pb(req)?,
            |reply| pb::MoveQueuedMessageResponse {
                changed: reply.changed,
                ..Default::default()
            }
        )
    }

    async fn remove_queued_message(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RemoveQueuedMessageRequest>,
    ) -> ServiceResult<pb::RemoveQueuedMessageResponse> {
        unary!(
            self,
            request,
            RemoveQueuedMessage,
            |req| rs::QueuedMessageRef {
                chat_id: req.chat_id,
                id: req.id,
            },
            |reply| pb::RemoveQueuedMessageResponse {
                removed: reply.removed,
                ..Default::default()
            }
        )
    }

    async fn send_queued_message_now(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SendQueuedMessageNowRequest>,
    ) -> ServiceResult<pb::SendQueuedMessageNowResponse> {
        unary!(
            self,
            request,
            SendQueuedMessageNow,
            |req| rs::QueuedMessageRef {
                chat_id: req.chat_id,
                id: req.id,
            },
            |reply| pb::SendQueuedMessageNowResponse {
                sent: reply.sent,
                ..Default::default()
            }
        )
    }

    async fn steer_queued_message_now(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SteerQueuedMessageNowRequest>,
    ) -> ServiceResult<pb::SteerQueuedMessageNowResponse> {
        unary!(
            self,
            request,
            SteerQueuedMessageNow,
            |req| rs::QueuedMessageRef {
                chat_id: req.chat_id,
                id: req.id,
            },
            |reply| pb::SteerQueuedMessageNowResponse {
                sent: reply.sent,
                ..Default::default()
            }
        )
    }

    async fn retry_delivery(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RetryDeliveryRequest>,
    ) -> ServiceResult<pb::RetryDeliveryResponse> {
        unary!(
            self,
            request,
            RetryDelivery,
            |req| rs::ChatIdParams {
                chat_id: req.chat_id,
            },
            |reply| pb::RetryDeliveryResponse::default()
        )
    }
}
