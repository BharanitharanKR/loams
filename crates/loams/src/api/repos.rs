// The generated service traits return `impl Encodable<_>`; these impls name the concrete message
// type, which is the intended refinement (as `connect.rs` does).
#![allow(refining_impl_trait)]

//! `loams.repos.v1` on the main port (Loams Git, GT1 Task 12; design §36 §19.10, D834).
//!
//! Without the `git` feature every RPC of `RepoService` answers `unimplemented` with the reason
//! `feature_not_in_variant` ([`ReposAbsent`]), so a caller can tell "not in this build" from a
//! missing route, and reflection lists a service a route answers. With it, the RPCs answer
//! `not_implemented` until GT1 Task 13 serves them. The package's other services
//! (`WorkspaceService`, `ObjectService`, `EnvService`) have no RPCs until GT1 Tasks 25, 18 and
//! 61 fill their stub files in.

use std::sync::Arc;

use connectrpc::{
    ConnectError, RequestContext, Router, ServiceRequest, ServiceResult, ServiceStream,
};
use loams_proto::loams::repos::v1 as pb;
use loams_proto::loams::repos::v1::{RepoService, RepoServiceExt};

use super::AppState;

/// The package, as the catalogue names it.
pub(crate) const PACKAGE: &str = "loams.repos.v1";

/// Whether this binary serves `loams.repos.v1` (the `git` feature).
pub(crate) const SERVED: bool = cfg!(feature = "git");

/// Registers `RepoService`: the `feature_not_in_variant` stub without the `git` feature.
pub(super) fn register(router: Router, state: &AppState) -> Router {
    let _ = state;
    Arc::new(ReposAbsent).register(router)
}

/// `RepoService` when this binary does not serve it (yet).
#[derive(Debug)]
struct ReposAbsent;

/// What every RPC answers: `feature_not_in_variant` without the `git` feature, `not_implemented`
/// with it until GT1 Task 13.
fn absent(method: &str) -> ConnectError {
    let rpc = format!("{PACKAGE}.RepoService/{method}");
    if SERVED {
        super::connect::not_implemented(&rpc)
    } else {
        super::connect::not_in_variant(&rpc)
    }
}

impl RepoService for ReposAbsent {
    async fn create_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CreateRepoRequest>,
    ) -> ServiceResult<pb::CreateRepoResponse> {
        Err(absent("CreateRepo"))
    }

    async fn get_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetRepoRequest>,
    ) -> ServiceResult<pb::GetRepoResponse> {
        Err(absent("GetRepo"))
    }

    async fn list_repos(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListReposRequest>,
    ) -> ServiceResult<pb::ListReposResponse> {
        Err(absent("ListRepos"))
    }

    async fn fork_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ForkRepoRequest>,
    ) -> ServiceResult<pb::ForkRepoResponse> {
        Err(absent("ForkRepo"))
    }

    async fn delete_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DeleteRepoRequest>,
    ) -> ServiceResult<pb::DeleteRepoResponse> {
        Err(absent("DeleteRepo"))
    }

    async fn update_repo_config(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::UpdateRepoConfigRequest>,
    ) -> ServiceResult<pb::UpdateRepoConfigResponse> {
        Err(absent("UpdateRepoConfig"))
    }

    async fn list_refs(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListRefsRequest>,
    ) -> ServiceResult<pb::ListRefsResponse> {
        Err(absent("ListRefs"))
    }

    async fn compact_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::CompactRepoRequest>,
    ) -> ServiceResult<pb::CompactRepoResponse> {
        Err(absent("CompactRepo"))
    }

    async fn watch_repo(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::WatchRepoRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchRepoResponse>> {
        Err(absent("WatchRepo"))
    }
}
