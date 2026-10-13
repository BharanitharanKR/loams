//! `TerminalService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::TerminalService for Agentd {
    async fn open_terminal(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::OpenTerminalRequest>,
    ) -> ServiceResult<pb::OpenTerminalResponse> {
        unary!(
            self,
            request,
            OpenTerminal,
            |req| rs::OpenTerminalParams {
                chat_id: req.chat_id,
                cols: Wire::from_pb(req.cols)?,
                rows: Wire::from_pb(req.rows)?,
                cwd: req.cwd,
            },
            |terminal| pb::OpenTerminalResponse {
                terminal: msg(&terminal),
                ..Default::default()
            }
        )
    }

    async fn subscribe_terminal(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SubscribeTerminalRequest>,
    ) -> ServiceResult<ServiceStream<pb::SubscribeTerminalResponse>> {
        stream!(
            self,
            request,
            SubscribeTerminal,
            |req| rs::SubscribeTerminalParams {
                terminal_id: req.terminal_id,
                after_seq: req.after_seq,
            },
            |event| pb::SubscribeTerminalResponse {
                event: msg(&event),
                ..Default::default()
            }
        )
    }

    async fn write_terminal(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WriteTerminalRequest>,
    ) -> ServiceResult<pb::WriteTerminalResponse> {
        unary!(
            self,
            request,
            WriteTerminal,
            |req| rs::WriteTerminalParams {
                terminal_id: req.terminal_id,
                data: req.data,
            },
            |reply| pb::WriteTerminalResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }

    async fn resize_terminal(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ResizeTerminalRequest>,
    ) -> ServiceResult<pb::ResizeTerminalResponse> {
        unary!(
            self,
            request,
            ResizeTerminal,
            |req| rs::ResizeTerminalParams {
                terminal_id: req.terminal_id,
                cols: Wire::from_pb(req.cols)?,
                rows: Wire::from_pb(req.rows)?,
            },
            |reply| pb::ResizeTerminalResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }

    async fn close_terminal(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CloseTerminalRequest>,
    ) -> ServiceResult<pb::CloseTerminalResponse> {
        unary!(
            self,
            request,
            CloseTerminal,
            |req| rs::TerminalIdParams {
                terminal_id: req.terminal_id,
            },
            |reply| pb::CloseTerminalResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }
}
