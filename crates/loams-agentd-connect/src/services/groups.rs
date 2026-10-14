//! `RunGroupService` (G12–G16; SD1 Task 12): each method's ndjson twin is the
//! method of the same name.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};

use super::{Agentd, stream, unary};
use crate::map::Wire;
use crate::proto::loams::agentd::v1 as pb;

impl pb::RunGroupService for Agentd {
    async fn create_run_group(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::CreateRunGroupRequest>,
    ) -> ServiceResult<pb::CreateRunGroupResponse> {
        unary!(
            self,
            request,
            CreateRunGroup,
            |req| Wire::from_pb(req)?,
            |created| created.to_pb()
        )
    }

    async fn watch_run_groups(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchRunGroupsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchRunGroupsResponse>> {
        stream!(
            self,
            request,
            WatchRunGroups,
            |req| Wire::from_pb(req)?,
            |groups| groups.to_pb()
        )
    }

    async fn get_run_group_comparison(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetRunGroupComparisonRequest>,
    ) -> ServiceResult<pb::GetRunGroupComparisonResponse> {
        unary!(
            self,
            request,
            GetRunGroupComparison,
            |req| Wire::from_pb(req)?,
            |comparison| comparison.to_pb()
        )
    }

    async fn run_group_check(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::RunGroupCheckRequest>,
    ) -> ServiceResult<pb::RunGroupCheckResponse> {
        unary!(
            self,
            request,
            RunGroupCheck,
            |req| Wire::from_pb(req)?,
            |runs| runs.to_pb()
        )
    }

    async fn resolve_run_group(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ResolveRunGroupRequest>,
    ) -> ServiceResult<pb::ResolveRunGroupResponse> {
        unary!(
            self,
            request,
            ResolveRunGroup,
            |req| Wire::from_pb(req)?,
            |resolved| resolved.to_pb()
        )
    }
}
