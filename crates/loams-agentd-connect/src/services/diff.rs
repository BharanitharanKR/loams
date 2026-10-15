//! `DiffService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::DiffService for Agentd {
    async fn watch_checkout_diffs(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchCheckoutDiffsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchCheckoutDiffsResponse>> {
        stream!(
            self,
            request,
            WatchCheckoutDiffs,
            |req| rs::Empty {},
            |diffs| {
                pb::WatchCheckoutDiffsResponse {
                    diffs: diffs.0.to_pb(),
                    ..Default::default()
                }
            }
        )
    }

    async fn get_checkout_diff(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetCheckoutDiffRequest>,
    ) -> ServiceResult<pb::GetCheckoutDiffResponse> {
        unary!(
            self,
            request,
            GetCheckoutDiff,
            |req| rs::GetCheckoutDiffParams {
                cwd: req.cwd,
                mode: req.mode,
                base_ref: req.base_ref,
                chat_id: req.chat_id,
                commit_sha: req.commit_sha,
            },
            |diff| pb::GetCheckoutDiffResponse {
                diff: msg(&diff),
                ..Default::default()
            }
        )
    }

    async fn get_checkout_file_diff_text(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetCheckoutFileDiffTextRequest>,
    ) -> ServiceResult<pb::GetCheckoutFileDiffTextResponse> {
        unary!(
            self,
            request,
            GetCheckoutFileDiffText,
            |req| rs::GetCheckoutFileDiffTextRequest {
                checkout_id: req.checkout_id,
                cwd: req.cwd,
                path: req.path,
                mode: req.mode,
                base_ref: req.base_ref,
                chat_id: req.chat_id,
                commit_sha: req.commit_sha,
                diff_checksum: req.diff_checksum,
            },
            |text| pb::GetCheckoutFileDiffTextResponse {
                text: msg(&text),
                ..Default::default()
            }
        )
    }

    async fn discard(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DiscardRequest>,
    ) -> ServiceResult<pb::DiscardResponse> {
        unary!(
            self,
            request,
            DiscardWorkingTree,
            |req| rs::DiscardWorkingTreeParams {
                chat_id: req.chat_id,
                checkout_id: req.checkout_id,
                expected_checksum: req.expected_checksum,
            },
            |reply| pb::DiscardResponse {
                ok: reply.ok,
                checksum: reply.checksum,
                ..Default::default()
            }
        )
    }

    async fn discard_paths(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DiscardPathsRequest>,
    ) -> ServiceResult<pb::DiscardPathsResponse> {
        unary!(
            self,
            request,
            DiscardPaths,
            |req| Wire::from_pb(req)?,
            |_empty| pb::DiscardPathsResponse::default()
        )
    }
}
