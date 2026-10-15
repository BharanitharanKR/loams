//! `ThreadService`.

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::error;
use crate::map::{MapError, Wire, from_msg, from_opt_msg, msg, opt_msg};
use crate::proto::loams::agentd::v1 as pb;

/// The `ok` of a Mutate-backed response.
macro_rules! mutated {
    ($svc:expr, $params:expr, $response:ident) => {{
        #[allow(clippy::redundant_closure_call)]
        let params: Result<rs::MutateParams, MapError> = (|| Ok($params))();
        let reply = $svc.mutate(params.map_err(error::from_map)?).await?;
        Response::ok(pb::$response {
            ok: reply.ok,
            ..Default::default()
        })
    }};
}

impl pb::ThreadService for Agentd {
    async fn watch_threads(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchThreadsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchThreadsResponse>> {
        stream!(self, request, WatchChats, |req| rs::Empty {}, |chats| {
            pb::WatchThreadsResponse {
                chats: chats.0.to_pb(),
                ..Default::default()
            }
        })
    }

    async fn watch_sessions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchSessionsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchSessionsResponse>> {
        stream!(
            self,
            request,
            WatchSessions,
            |req| rs::Empty {},
            |sessions| {
                pb::WatchSessionsResponse {
                    sessions: sessions.0.to_pb(),
                    ..Default::default()
                }
            }
        )
    }

    async fn watch_sidebar_preferences(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchSidebarPreferencesRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchSidebarPreferencesResponse>> {
        stream!(
            self,
            request,
            WatchSidebarPreferences,
            |req| rs::Empty {},
            |preferences| pb::WatchSidebarPreferencesResponse {
                preferences: msg(&preferences),
                ..Default::default()
            }
        )
    }

    async fn create_thread(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateThreadRequest>,
    ) -> ServiceResult<pb::CreateThreadResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::CreateChat {
                chat_id: req.chat_id,
                space_id: req.space_id,
                device_id: req.device_id,
                config: from_opt_msg(req.config)?,
                branch: req.branch,
                cwd: req.cwd,
                parent_chat_id: req.parent_chat_id,
            },
            CreateThreadResponse
        )
    }

    async fn rename_thread(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RenameThreadRequest>,
    ) -> ServiceResult<pb::RenameThreadResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::RenameChat {
                chat_id: req.chat_id,
                title: req.title,
            },
            RenameThreadResponse
        )
    }

    async fn set_thread_archived(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetThreadArchivedRequest>,
    ) -> ServiceResult<pb::SetThreadArchivedResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::SetChatArchived {
                chat_id: req.chat_id,
                archived: req.archived,
            },
            SetThreadArchivedResponse
        )
    }

    async fn delete_thread(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteThreadRequest>,
    ) -> ServiceResult<pb::DeleteThreadResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::DeleteChat {
                chat_id: req.chat_id,
            },
            DeleteThreadResponse
        )
    }

    async fn change_sidebar_pin(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ChangeSidebarPinRequest>,
    ) -> ServiceResult<pb::ChangeSidebarPinResponse> {
        let req = request.to_owned_message();
        let change = from_msg(req.change, "change").map_err(error::from_map)?;
        let reply = self
            .mutate(rs::MutateParams::ChangeSidebarPin { change })
            .await?;
        Response::ok(pb::ChangeSidebarPinResponse {
            ok: reply.ok,
            preferences: opt_msg(&reply.sidebar_preferences),
            ..Default::default()
        })
    }

    async fn mark_thread_seen(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::MarkThreadSeenRequest>,
    ) -> ServiceResult<pb::MarkThreadSeenResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::MarkChatSeen {
                chat_id: req.chat_id,
                at: req.at_ms,
            },
            MarkThreadSeenResponse
        )
    }

    async fn set_thread_config(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetThreadConfigRequest>,
    ) -> ServiceResult<pb::SetThreadConfigResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::SetChatConfig {
                chat_id: req.chat_id,
                config: from_msg(req.config, "config")?,
            },
            SetThreadConfigResponse
        )
    }

    async fn set_thread_branch(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetThreadBranchRequest>,
    ) -> ServiceResult<pb::SetThreadBranchResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::SetChatBranch {
                chat_id: req.chat_id,
                branch: req.branch,
            },
            SetThreadBranchResponse
        )
    }

    async fn set_thread_cwd(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetThreadCwdRequest>,
    ) -> ServiceResult<pb::SetThreadCwdResponse> {
        let req = request.to_owned_message();
        mutated!(
            self,
            rs::MutateParams::SetChatCwd {
                chat_id: req.chat_id,
                cwd: req.cwd,
            },
            SetThreadCwdResponse
        )
    }

    async fn fork_thread(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ForkThreadRequest>,
    ) -> ServiceResult<pb::ForkThreadResponse> {
        unary!(
            self,
            request,
            ForkSideChat,
            |req| Wire::from_pb(req)?,
            |chat| pb::ForkThreadResponse {
                chat: msg(&chat),
                ..Default::default()
            }
        )
    }

    async fn send(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SendRequest>,
    ) -> ServiceResult<pb::SendResponse> {
        let req = request.to_owned_message();
        let run = from_msg(req.request, "request").map_err(error::from_map)?;
        let command_id = self
            .command(
                req.chat_id,
                rs::SessionCommandPayload::Run {
                    request: run,
                    message_id: req.message_id,
                },
            )
            .await?;
        Response::ok(pb::SendResponse {
            command_id,
            ..Default::default()
        })
    }

    async fn steer(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SteerRequest>,
    ) -> ServiceResult<pb::SteerResponse> {
        let req = request.to_owned_message();
        let command_id = self
            .command(
                req.chat_id,
                rs::SessionCommandPayload::Steer {
                    prompt: req.prompt,
                    message_id: req.message_id,
                },
            )
            .await?;
        Response::ok(pb::SteerResponse {
            command_id,
            ..Default::default()
        })
    }

    async fn interrupt(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::InterruptRequest>,
    ) -> ServiceResult<pb::InterruptResponse> {
        let req = request.to_owned_message();
        let command_id = self
            .command(req.chat_id, rs::SessionCommandPayload::Interrupt {})
            .await?;
        Response::ok(pb::InterruptResponse {
            command_id,
            ..Default::default()
        })
    }

    async fn answer_question(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::AnswerQuestionRequest>,
    ) -> ServiceResult<pb::AnswerQuestionResponse> {
        let req = request.to_owned_message();
        let answers = Wire::from_pb(req.answers).map_err(error::from_map)?;
        let command_id = self
            .command(
                req.chat_id,
                rs::SessionCommandPayload::RespondInput {
                    request_id: req.request_id,
                    answers,
                },
            )
            .await?;
        Response::ok(pb::AnswerQuestionResponse {
            command_id,
            ..Default::default()
        })
    }

    async fn watch_transcript(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchTranscriptRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchTranscriptResponse>> {
        stream!(
            self,
            request,
            WatchDocMessages,
            |req| Wire::from_pb(req)?,
            |update| pb::WatchTranscriptResponse {
                update: msg(&update),
                ..Default::default()
            }
        )
    }

    async fn list_checkpoints(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListCheckpointsRequest>,
    ) -> ServiceResult<pb::ListCheckpointsResponse> {
        unary!(
            self,
            request,
            ListCheckpoints,
            |req| Wire::from_pb(req)?,
            |list| list.to_pb()
        )
    }

    async fn restore_checkpoint(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RestoreCheckpointRequest>,
    ) -> ServiceResult<pb::RestoreCheckpointResponse> {
        unary!(
            self,
            request,
            RestoreCheckpoint,
            |req| Wire::from_pb(req)?,
            |restored| restored.to_pb()
        )
    }

    async fn get_thread_usage(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetThreadUsageRequest>,
    ) -> ServiceResult<pb::GetThreadUsageResponse> {
        unary!(
            self,
            request,
            GetThreadUsage,
            |req| Wire::from_pb(req)?,
            |usage| usage.to_pb()
        )
    }
}
