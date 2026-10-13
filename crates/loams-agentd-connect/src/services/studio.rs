//! `StudioService`.

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use loams_agentd_proto as rs;
use loams_agentd_proto::rpc::calls;

use super::{Agentd, unary};
use crate::engine;
use crate::map::{Wire, from_msg, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::StudioService for Agentd {
    async fn get_host(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::GetHostRequest>,
    ) -> ServiceResult<pb::GetHostResponse> {
        let info = engine::call::<calls::EngineInfo>(&*self.engine, &rs::Empty {}).await?;
        let host = &self.host;
        let rs::EngineInfo {
            device_id,
            workspace_scope,
            capabilities,
        } = info;
        Response::ok(pb::GetHostResponse {
            host: buffa::MessageField::some(pb::Host {
                device_id,
                workspace_scope: workspace_scope.to_pb(),
                capabilities,
                protocol: host.protocol,
                version: host.version.clone(),
                build_sha: host.build_sha.clone(),
                mode: host.mode.to_pb(),
                pid: host.pid,
                started_at_ms: host.started_at_ms,
                features: host.features.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn get_title_settings(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetTitleSettingsRequest>,
    ) -> ServiceResult<pb::GetTitleSettingsResponse> {
        unary!(
            self,
            request,
            GetTitleSettings,
            |req| rs::Empty {},
            |settings| {
                pb::GetTitleSettingsResponse {
                    settings: msg(&settings),
                    ..Default::default()
                }
            }
        )
    }

    async fn set_title_settings(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SetTitleSettingsRequest>,
    ) -> ServiceResult<pb::SetTitleSettingsResponse> {
        unary!(
            self,
            request,
            SetTitleSettings,
            |req| from_msg(req.settings, "settings")?,
            |settings| pb::SetTitleSettingsResponse {
                settings: msg(&settings),
                ..Default::default()
            }
        )
    }
}
