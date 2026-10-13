//! `ProjectService`.

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, from_msg, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::ProjectService for Agentd {
    async fn watch_projects(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchProjectsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchProjectsResponse>> {
        stream!(self, request, WatchSpaces, |req| rs::Empty {}, |spaces| {
            pb::WatchProjectsResponse {
                spaces: spaces.0.to_pb(),
                ..Default::default()
            }
        })
    }

    async fn create_project(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateProjectRequest>,
    ) -> ServiceResult<pb::CreateProjectResponse> {
        let req = request.to_owned_message();
        let reply = self
            .mutate(rs::MutateParams::CreateSpace {
                space_id: req.space_id,
                device_id: req.device_id,
                path: req.path,
                name: req.name,
                git_detected: req.git_detected,
            })
            .await?;
        Response::ok(pb::CreateProjectResponse {
            ok: reply.ok,
            ..Default::default()
        })
    }

    async fn rename_project(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RenameProjectRequest>,
    ) -> ServiceResult<pb::RenameProjectResponse> {
        let req = request.to_owned_message();
        let reply = self
            .mutate(rs::MutateParams::RenameSpace {
                space_id: req.space_id,
                name: req.name,
            })
            .await?;
        Response::ok(pb::RenameProjectResponse {
            ok: reply.ok,
            ..Default::default()
        })
    }

    async fn delete_project(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteProjectRequest>,
    ) -> ServiceResult<pb::DeleteProjectResponse> {
        let req = request.to_owned_message();
        let reply = self
            .mutate(rs::MutateParams::DeleteSpace {
                space_id: req.space_id,
            })
            .await?;
        Response::ok(pb::DeleteProjectResponse {
            ok: reply.ok,
            ..Default::default()
        })
    }

    async fn list_repos(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListReposRequest>,
    ) -> ServiceResult<pb::ListReposResponse> {
        unary!(self, request, ListRepos, |req| rs::Empty {}, |repos| {
            pb::ListReposResponse {
                repos: repos.0.to_pb(),
                ..Default::default()
            }
        })
    }

    async fn add_repo(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::AddRepoRequest>,
    ) -> ServiceResult<pb::AddRepoResponse> {
        unary!(
            self,
            request,
            AddRepo,
            |req| rs::AddRepoParams { path: req.path },
            |repo| pb::AddRepoResponse {
                repo: msg(&repo),
                ..Default::default()
            }
        )
    }

    async fn clone_repo(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CloneRepoRequest>,
    ) -> ServiceResult<pb::CloneRepoResponse> {
        unary!(
            self,
            request,
            CloneRepo,
            |req| rs::CloneRepoParams { url: req.url },
            |repo| pb::CloneRepoResponse {
                repo: msg(&repo),
                ..Default::default()
            }
        )
    }

    async fn create_repo(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateRepoRequest>,
    ) -> ServiceResult<pb::CreateRepoResponse> {
        unary!(
            self,
            request,
            CreateRepo,
            |req| rs::CreateRepoParams { name: req.name },
            |repo| pb::CreateRepoResponse {
                repo: msg(&repo),
                ..Default::default()
            }
        )
    }

    async fn list_project_actions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListProjectActionsRequest>,
    ) -> ServiceResult<pb::ListProjectActionsResponse> {
        unary!(
            self,
            request,
            ListProjectActions,
            |req| rs::ListProjectActionsParams {
                space_id: req.space_id,
            },
            |snapshot| pb::ListProjectActionsResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn upsert_project_action(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::UpsertProjectActionRequest>,
    ) -> ServiceResult<pb::UpsertProjectActionResponse> {
        unary!(
            self,
            request,
            UpsertProjectAction,
            |req| rs::UpsertProjectActionParams {
                space_id: req.space_id,
                action_id: req.action_id,
                action: from_msg(req.action, "action")?,
            },
            |snapshot| pb::UpsertProjectActionResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn delete_project_action(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteProjectActionRequest>,
    ) -> ServiceResult<pb::DeleteProjectActionResponse> {
        unary!(
            self,
            request,
            DeleteProjectAction,
            |req| rs::DeleteProjectActionParams {
                space_id: req.space_id,
                action_id: req.action_id,
            },
            |snapshot| pb::DeleteProjectActionResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }

    async fn run_project_action(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RunProjectActionRequest>,
    ) -> ServiceResult<pb::RunProjectActionResponse> {
        unary!(
            self,
            request,
            RunProjectAction,
            |req| rs::RunProjectActionParams {
                space_id: req.space_id,
                chat_id: req.chat_id,
                action_id: req.action_id,
                cols: Wire::from_pb(req.cols)?,
                rows: Wire::from_pb(req.rows)?,
            },
            |run| pb::RunProjectActionResponse {
                run: msg(&run),
                ..Default::default()
            }
        )
    }

    async fn take_project_action_setup(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::TakeProjectActionSetupRequest>,
    ) -> ServiceResult<pb::TakeProjectActionSetupResponse> {
        unary!(
            self,
            request,
            TakeProjectActionSetup,
            |req| rs::TakeProjectActionSetupParams {
                chat_id: req.chat_id,
                command_id: req.command_id,
            },
            |setup| pb::TakeProjectActionSetupResponse {
                setup: msg(&setup),
                ..Default::default()
            }
        )
    }
}
