//! `FileService`.

use connectrpc::{RequestContext, ServiceRequest, ServiceResult, ServiceStream};
use loams_agentd_proto as rs;

use super::{Agentd, stream, unary};
use crate::map::{Wire, msg};
use crate::proto::loams::agentd::v1 as pb;

impl pb::FileService for Agentd {
    async fn list_folders(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListFoldersRequest>,
    ) -> ServiceResult<pb::ListFoldersResponse> {
        unary!(
            self,
            request,
            ListFolders,
            |req| Wire::from_pb(req)?,
            |listing| pb::ListFoldersResponse {
                listing: msg(&listing),
                ..Default::default()
            }
        )
    }

    async fn list_drives(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDrivesRequest>,
    ) -> ServiceResult<pb::ListDrivesResponse> {
        unary!(self, request, ListDrives, |req| rs::Empty {}, |listing| {
            pb::ListDrivesResponse {
                drives: listing.drives.to_pb(),
                ..Default::default()
            }
        })
    }

    async fn search_mentions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SearchMentionsRequest>,
    ) -> ServiceResult<pb::SearchMentionsResponse> {
        unary!(
            self,
            request,
            SearchFiles,
            |req| Wire::from_pb(req)?,
            |matches| pb::SearchMentionsResponse {
                matches: matches.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn search(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SearchRequest>,
    ) -> ServiceResult<pb::SearchResponse> {
        unary!(
            self,
            request,
            SearchWorkspaceFiles,
            |req| Wire::from_pb(req)?,
            |matches| pb::SearchResponse {
                matches: matches.0.to_pb(),
                ..Default::default()
            }
        )
    }

    async fn list_directory(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDirectoryRequest>,
    ) -> ServiceResult<pb::ListDirectoryResponse> {
        unary!(
            self,
            request,
            ListWorkspaceDirectory,
            |req| Wire::from_pb(req)?,
            |page| pb::ListDirectoryResponse {
                page: msg(&page),
                ..Default::default()
            }
        )
    }

    async fn read_file(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ReadFileRequest>,
    ) -> ServiceResult<pb::ReadFileResponse> {
        unary!(
            self,
            request,
            ReadWorkspaceFile,
            |req| Wire::from_pb(req)?,
            |file| pb::ReadFileResponse {
                file: msg(&file),
                ..Default::default()
            }
        )
    }

    async fn read_image(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ReadImageRequest>,
    ) -> ServiceResult<pb::ReadImageResponse> {
        unary!(
            self,
            request,
            ReadWorkspaceImage,
            |req| Wire::from_pb(req)?,
            |chunk| pb::ReadImageResponse {
                chunk: msg(&chunk),
                ..Default::default()
            }
        )
    }

    async fn write_file(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WriteFileRequest>,
    ) -> ServiceResult<pb::WriteFileResponse> {
        unary!(
            self,
            request,
            WriteWorkspaceFile,
            |req| Wire::from_pb(req)?,
            |outcome| pb::WriteFileResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn move_entry(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::MoveEntryRequest>,
    ) -> ServiceResult<pb::MoveEntryResponse> {
        unary!(
            self,
            request,
            MoveWorkspaceEntry,
            |req| Wire::from_pb(req)?,
            |outcome| pb::MoveEntryResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn delete_entry(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::DeleteEntryRequest>,
    ) -> ServiceResult<pb::DeleteEntryResponse> {
        unary!(
            self,
            request,
            DeleteWorkspaceEntry,
            |req| Wire::from_pb(req)?,
            |outcome| pb::DeleteEntryResponse {
                outcome: msg(&outcome),
                ..Default::default()
            }
        )
    }

    async fn watch_files(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchFilesRequest>,
    ) -> ServiceResult<ServiceStream<pb::WatchFilesResponse>> {
        stream!(
            self,
            request,
            WatchWorkspaceFiles,
            |req| Wire::from_pb(req)?,
            |changes| pb::WatchFilesResponse {
                changes: msg(&changes),
                ..Default::default()
            }
        )
    }

    async fn search_content(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SearchContentRequest>,
    ) -> ServiceResult<ServiceStream<pb::SearchContentResponse>> {
        stream!(
            self,
            request,
            SearchWorkspaceContent,
            |req| Wire::from_pb(req)?,
            |found| found.to_pb()
        )
    }
}
