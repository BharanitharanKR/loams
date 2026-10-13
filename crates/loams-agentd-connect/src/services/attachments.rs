//! `AttachmentService` and `PreviewService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::msg;
use crate::proto::loams::agentd::v1 as pb;

impl pb::AttachmentService for Agentd {
    async fn upload_chunk(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::UploadChunkRequest>,
    ) -> ServiceResult<pb::UploadChunkResponse> {
        unary!(
            self,
            request,
            UploadChunk,
            |req| rs::UploadChunkParams {
                upload_id: req.upload_id,
                data: req.data,
                seq: req.seq,
            },
            |reply| pb::UploadChunkResponse {
                ok: reply.ok,
                ..Default::default()
            }
        )
    }

    async fn upload_commit(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::UploadCommitRequest>,
    ) -> ServiceResult<pb::UploadCommitResponse> {
        unary!(
            self,
            request,
            UploadCommit,
            |req| rs::UploadCommitParams {
                upload_id: req.upload_id,
                file_name: req.file_name,
            },
            |reply| pb::UploadCommitResponse {
                path: reply.path,
                ..Default::default()
            }
        )
    }

    async fn read_attachment_chunk(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ReadAttachmentChunkRequest>,
    ) -> ServiceResult<pb::ReadAttachmentChunkResponse> {
        unary!(
            self,
            request,
            ReadAttachmentChunk,
            |req| rs::ReadAttachmentChunkParams {
                path: req.path,
                offset: req.offset,
            },
            |chunk| pb::ReadAttachmentChunkResponse {
                chunk: msg(&chunk),
                ..Default::default()
            }
        )
    }

    async fn fetch_tool_blob(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::FetchToolBlobRequest>,
    ) -> ServiceResult<pb::FetchToolBlobResponse> {
        unary!(
            self,
            request,
            FetchToolBlob,
            |req| rs::FetchToolBlobParams {
                blob_ref: req.blob_ref,
                offset: req.offset,
                len: req.len,
            },
            |chunk| pb::FetchToolBlobResponse {
                chunk: msg(&chunk),
                ..Default::default()
            }
        )
    }
}

impl pb::PreviewService for Agentd {
    async fn watch_previews(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchPreviewsRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchPreviewsResponse>> {
        stream!(
            self,
            request,
            WatchPreviews,
            |req| rs::WatchPreviewsParams {
                chat_id: req.chat_id,
            },
            |snapshot| pb::WatchPreviewsResponse {
                snapshot: msg(&snapshot),
                ..Default::default()
            }
        )
    }
}
