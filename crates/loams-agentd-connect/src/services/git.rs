//! `GitService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::error;
use crate::map::{MapError, Wire, from_default_msg, msg, opt_msg};
use crate::proto::loams::agentd::v1 as pb;

/// A page size: unset is the ndjson wire's default (100).
fn limit(limit: Option<u64>) -> Result<usize, MapError> {
    limit.map_or(Ok(rs::GIT_HISTORY_DEFAULT_LIMIT), Wire::from_pb)
}

impl pb::GitService for Agentd {
    async fn list_branches(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListBranchesRequest>,
    ) -> ServiceResult<pb::ListBranchesResponse> {
        unary!(
            self,
            request,
            ListBranches,
            |req| rs::RepoPathParams {
                repo_path: req.repo_path,
            },
            |branches| pb::ListBranchesResponse {
                branches: branches.0,
                ..Default::default()
            }
        )
    }

    async fn list_refs(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListRefsRequest>,
    ) -> ServiceResult<pb::ListRefsResponse> {
        unary!(
            self,
            request,
            ListRefs,
            |req| rs::RepoPathParams {
                repo_path: req.repo_path,
            },
            |refs| pb::ListRefsResponse {
                refs: refs.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn list_git_history(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListGitHistoryRequest>,
    ) -> ServiceResult<pb::ListGitHistoryResponse> {
        unary!(
            self,
            request,
            ListGitHistory,
            |req| rs::ListGitHistoryParams {
                cwd: req.cwd,
                cursor: Wire::from_pb(req.cursor)?,
                limit: limit(req.limit)?,
            },
            |page| pb::ListGitHistoryResponse {
                page: msg(&page),
                ..Default::default()
            }
        )
    }

    async fn search_git_history(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SearchGitHistoryRequest>,
    ) -> ServiceResult<pb::SearchGitHistoryResponse> {
        unary!(
            self,
            request,
            SearchGitHistory,
            |req| rs::SearchGitHistoryParams {
                cwd: req.cwd,
                query: req.query,
                cursor: Wire::from_pb(req.cursor)?,
                limit: limit(req.limit)?,
            },
            |page| pb::SearchGitHistoryResponse {
                page: msg(&page),
                ..Default::default()
            }
        )
    }

    async fn resolve_git_avatars(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ResolveGitAvatarsRequest>,
    ) -> ServiceResult<pb::ResolveGitAvatarsResponse> {
        unary!(
            self,
            request,
            ResolveGitAvatars,
            |req| rs::ResolveGitAvatarsParams {
                cwd: req.cwd,
                authors: Wire::from_pb(req.authors)?,
                cursor: Wire::from_pb(req.cursor)?,
                limit: limit(req.limit)?,
            },
            |avatars| pb::ResolveGitAvatarsResponse {
                avatars: avatars.0.into_iter().collect(),
                ..Default::default()
            }
        )
    }

    async fn switch_ref(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SwitchRefRequest>,
    ) -> ServiceResult<pb::SwitchRefResponse> {
        unary!(
            self,
            request,
            SwitchRef,
            |req| Wire::from_pb(req)?,
            |reply| pb::SwitchRefResponse {
                branch: reply.branch,
                ..Default::default()
            }
        )
    }

    async fn fetch_all(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::FetchAllRequest>,
    ) -> ServiceResult<pb::FetchAllResponse> {
        unary!(
            self,
            request,
            FetchAll,
            |req| rs::RepoPathParams {
                repo_path: req.repo_path,
            },
            |reply| pb::FetchAllResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }

    async fn create_worktree(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateWorktreeRequest>,
    ) -> ServiceResult<pb::CreateWorktreeResponse> {
        unary!(
            self,
            request,
            CreateWorktree,
            |req| Wire::from_pb(req)?,
            |outcome| pb::CreateWorktreeResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn delete_worktree(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteWorktreeRequest>,
    ) -> ServiceResult<pb::DeleteWorktreeResponse> {
        unary!(
            self,
            request,
            DeleteWorktree,
            |req| Wire::from_pb(req)?,
            |reply| pb::DeleteWorktreeResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }

    async fn watch_status(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchStatusRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchStatusResponse>> {
        stream!(
            self,
            request,
            WatchWorkspaceGitStatus,
            |req| rs::WatchWorkspaceFilesRequest {
                target: from_default_msg(req.target)?,
            },
            |frame| pb::WatchStatusResponse {
                status: opt_msg(&frame.status),
                ..Default::default()
            }
        )
    }

    async fn watch_change_request(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchChangeRequestRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchChangeRequestResponse>> {
        stream!(
            self,
            request,
            WatchCheckoutChangeRequest,
            |req| Wire::from_pb(req)?,
            |status| pb::WatchChangeRequestResponse {
                status: msg(&status),
                ..Default::default()
            }
        )
    }

    async fn list_worktrees(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListWorktreesRequest>,
    ) -> ServiceResult<pb::ListWorktreesResponse> {
        Err(error::not_implemented("ListWorktrees", "G1, SD1 Task 9"))
    }

    async fn commit_worktree(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CommitWorktreeRequest>,
    ) -> ServiceResult<pb::CommitWorktreeResponse> {
        Err(error::not_implemented("CommitWorktree", "G2, SD1 Task 9"))
    }

    async fn push_branch(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::PushBranchRequest>,
    ) -> ServiceResult<pb::PushBranchResponse> {
        Err(error::not_implemented("PushBranch", "G3, SD1 Task 9"))
    }

    async fn create_change_request(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CreateChangeRequestRequest>,
    ) -> ServiceResult<pb::CreateChangeRequestResponse> {
        Err(error::not_implemented(
            "CreateChangeRequest",
            "G4, SD1 Task 9",
        ))
    }

    async fn merge_branch(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::MergeBranchRequest>,
    ) -> ServiceResult<pb::MergeBranchResponse> {
        Err(error::not_implemented("MergeBranch", "G5, SD1 Task 9"))
    }
}
