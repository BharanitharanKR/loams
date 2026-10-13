//! `HarnessService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, msg};
use crate::proto::loams::agentd::v1 as pb;

fn catalog_params(
    harness: buffa::EnumValue<pb::HarnessId>,
    chat_id: Option<String>,
    space_id: Option<String>,
    path: Option<String>,
) -> Result<rs::HarnessCatalogParams, crate::map::MapError> {
    Ok(rs::HarnessCatalogParams {
        harness: Wire::from_pb(harness)?,
        chat_id,
        space_id,
        path,
    })
}

impl pb::HarnessService for Agentd {
    async fn list_harnesses(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListHarnessesRequest>,
    ) -> ServiceResult<pb::ListHarnessesResponse> {
        unary!(
            self,
            request,
            ListHarnesses,
            |req| rs::Empty {},
            |catalog| {
                pb::ListHarnessesResponse {
                    harnesses: catalog.0.to_pb(),
                    ..Default::default()
                }
            }
        )
    }

    async fn list_models(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListModelsRequest>,
    ) -> ServiceResult<pb::ListModelsResponse> {
        unary!(
            self,
            request,
            ListModels,
            |req| Wire::from_pb(req)?,
            |models| pb::ListModelsResponse {
                models: models.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn list_skills(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListSkillsRequest>,
    ) -> ServiceResult<pb::ListSkillsResponse> {
        unary!(
            self,
            request,
            ListSkills,
            |req| catalog_params(req.harness, req.chat_id, req.space_id, req.path)?,
            |skills| pb::ListSkillsResponse {
                unavailable: skills.0.is_none(),
                skills: skills.0.unwrap_or_default().to_pb(),
                ..Default::default()
            }
        )
    }

    async fn list_commands(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListCommandsRequest>,
    ) -> ServiceResult<pb::ListCommandsResponse> {
        unary!(
            self,
            request,
            ListCommands,
            |req| catalog_params(req.harness, req.chat_id, req.space_id, req.path)?,
            |commands| pb::ListCommandsResponse {
                commands: commands.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn set_harness_enabled(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetHarnessEnabledRequest>,
    ) -> ServiceResult<pb::SetHarnessEnabledResponse> {
        unary!(
            self,
            request,
            SetHarnessEnabled,
            |req| Wire::from_pb(req)?,
            |catalog| pb::SetHarnessEnabledResponse {
                harnesses: catalog.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn install_harness(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::InstallHarnessRequest>,
    ) -> ServiceResult<pb::InstallHarnessResponse> {
        unary!(
            self,
            request,
            InstallHarness,
            |req| rs::HarnessParams {
                harness: Wire::from_pb(req.harness)?,
            },
            |catalog| pb::InstallHarnessResponse {
                harnesses: catalog.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn cancel_install(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CancelInstallRequest>,
    ) -> ServiceResult<pb::CancelInstallResponse> {
        unary!(
            self,
            request,
            CancelInstall,
            |req| rs::HarnessParams {
                harness: Wire::from_pb(req.harness)?,
            },
            |reply| pb::CancelInstallResponse::default()
        )
    }

    async fn watch_harness_updates(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchHarnessUpdatesRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchHarnessUpdatesResponse>> {
        stream!(
            self,
            request,
            WatchHarnessUpdates,
            |req| rs::Empty {},
            |updates| {
                pb::WatchHarnessUpdatesResponse {
                    updates: updates.0.to_pb(),
                    ..Default::default()
                }
            }
        )
    }

    async fn check_harness_updates(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CheckHarnessUpdatesRequest>,
    ) -> ServiceResult<pb::CheckHarnessUpdatesResponse> {
        unary!(
            self,
            request,
            CheckHarnessUpdates,
            |req| Wire::from_pb(req)?,
            |updates| pb::CheckHarnessUpdatesResponse {
                updates: updates.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn apply_harness_update(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ApplyHarnessUpdateRequest>,
    ) -> ServiceResult<pb::ApplyHarnessUpdateResponse> {
        unary!(
            self,
            request,
            ApplyHarnessUpdate,
            |req| rs::HarnessParams {
                harness: Wire::from_pb(req.harness)?,
            },
            |reply| pb::ApplyHarnessUpdateResponse {
                ok: reply.ok,
                version: reply.version,
                ..Default::default()
            }
        )
    }

    async fn cancel_harness_update(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CancelHarnessUpdateRequest>,
    ) -> ServiceResult<pb::CancelHarnessUpdateResponse> {
        unary!(
            self,
            request,
            CancelHarnessUpdate,
            |req| rs::HarnessParams {
                harness: Wire::from_pb(req.harness)?,
            },
            |reply| pb::CancelHarnessUpdateResponse {
                cancelled: reply.cancelled,
                ..Default::default()
            }
        )
    }

    async fn dismiss_harness_update(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DismissHarnessUpdateRequest>,
    ) -> ServiceResult<pb::DismissHarnessUpdateResponse> {
        unary!(
            self,
            request,
            DismissHarnessUpdate,
            |req| Wire::from_pb(req)?,
            |status| pb::DismissHarnessUpdateResponse {
                status: msg(&status),
                ..Default::default()
            }
        )
    }

    async fn list_agent_accounts(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListAgentAccountsRequest>,
    ) -> ServiceResult<pb::ListAgentAccountsResponse> {
        unary!(
            self,
            request,
            ListAgentAccounts,
            |req| Wire::from_pb(req)?,
            |snapshot| pb::ListAgentAccountsResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn activate_agent_account(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ActivateAgentAccountRequest>,
    ) -> ServiceResult<pb::ActivateAgentAccountResponse> {
        unary!(
            self,
            request,
            ActivateAgentAccount,
            |req| rs::AgentAccountParams {
                harness: Wire::from_pb(req.harness)?,
                account_id: req.account_id,
            },
            |snapshot| pb::ActivateAgentAccountResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn forget_agent_account(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ForgetAgentAccountRequest>,
    ) -> ServiceResult<pb::ForgetAgentAccountResponse> {
        unary!(
            self,
            request,
            ForgetAgentAccount,
            |req| rs::AgentAccountParams {
                harness: Wire::from_pb(req.harness)?,
                account_id: req.account_id,
            },
            |snapshot| pb::ForgetAgentAccountResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn start_agent_login(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::StartAgentLoginRequest>,
    ) -> ServiceResult<pb::StartAgentLoginResponse> {
        unary!(
            self,
            request,
            StartAgentLogin,
            |req| Wire::from_pb(req)?,
            |login| pb::StartAgentLoginResponse {
                login: msg(&login),
                ..Default::default()
            }
        )
    }

    async fn complete_agent_login(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CompleteAgentLoginRequest>,
    ) -> ServiceResult<pb::CompleteAgentLoginResponse> {
        unary!(
            self,
            request,
            CompleteAgentLogin,
            |req| Wire::from_pb(req)?,
            |snapshot| pb::CompleteAgentLoginResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn poll_agent_login(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::PollAgentLoginRequest>,
    ) -> ServiceResult<pb::PollAgentLoginResponse> {
        unary!(
            self,
            request,
            PollAgentLogin,
            |req| rs::LoginIdParams {
                login_id: req.login_id,
            },
            |poll| pb::PollAgentLoginResponse {
                poll: msg(&poll),
                ..Default::default()
            }
        )
    }

    async fn cancel_agent_login(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CancelAgentLoginRequest>,
    ) -> ServiceResult<pb::CancelAgentLoginResponse> {
        unary!(
            self,
            request,
            CancelAgentLogin,
            |req| rs::LoginIdParams {
                login_id: req.login_id,
            },
            |reply| pb::CancelAgentLoginResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }
}
