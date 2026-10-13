//! files.proto: folders, the file tree, reads, writes, moves, deletes and
//! search, and the requests that name a `WorkspaceTarget`.

use loams_agentd_proto as rs;

use super::{MapError, Wire, from_msg, from_opt_msg, msg, opt_msg};
use crate::proto::loams::agentd::v1 as pb;
use crate::proto::loams::agentd::v1::__buffa::oneof::{
    workspace_mutation_outcome::Outcome as MutationOutcome,
    write_workspace_file_outcome::Outcome as WriteOutcome,
};

wire_struct!(rs::WorkspaceTarget => pb::WorkspaceTarget {
    chat_id => chat_id,
    space_id => space_id,
    checkout_path => checkout_path,
});

wire_struct!(rs::FolderEntry => pb::FolderEntry {
    name => name,
    is_dir => is_dir,
    is_repo => is_repo,
});

wire_struct!(rs::FolderListing => pb::FolderListing {
    path => path,
    entries => entries,
    truncated => truncated,
});

wire_struct!(rs::DriveEntry => pb::DriveEntry {
    name => name,
    path => path,
});

wire_struct!(rs::FileSearchMatch => pb::FileSearchMatch {
    path => path,
    is_dir => is_dir,
});

wire_enum!(rs::WorkspaceEntryKind => pb::WorkspaceEntryKind {
    File => WORKSPACE_ENTRY_KIND_FILE,
    Directory => WORKSPACE_ENTRY_KIND_DIRECTORY,
    Symlink => WORKSPACE_ENTRY_KIND_SYMLINK,
});

wire_struct!(rs::WorkspaceEntry => pb::WorkspaceEntry {
    mutation_revision => mutation_revision,
    path => path,
    name => name,
    kind => kind,
    size => size,
    modified_at => modified_at_ms,
    ignored => ignored,
    read_only => read_only,
});

wire_struct!(rs::WorkspaceMutationCapabilities => pb::WorkspaceMutationCapabilities {
    move_entry => move_entry,
    delete_entry => delete_entry,
});

wire_struct!(rs::WorkspaceDirectoryPage => pb::WorkspaceDirectoryPage {
    checkout_id => checkout_id,
    mutation_capabilities => mutation_capabilities as opt_msg,
    directory => directory,
    entries => entries,
    next_cursor => next_cursor,
    truncated => truncated,
});

wire_struct!(rs::WorkspaceFileSearchMatch => pb::WorkspaceFileSearchMatch {
    path => path,
    name => name,
    kind => kind,
    score => score,
});

wire_enum!(rs::WorkspaceTextEncoding => pb::WorkspaceTextEncoding {
    Utf8 => WORKSPACE_TEXT_ENCODING_UTF8,
    Utf8Bom => WORKSPACE_TEXT_ENCODING_UTF8_BOM,
    Binary => WORKSPACE_TEXT_ENCODING_BINARY,
    Unsupported => WORKSPACE_TEXT_ENCODING_UNSUPPORTED,
});

wire_enum!(rs::WorkspaceLineEnding => pb::WorkspaceLineEnding {
    Lf => WORKSPACE_LINE_ENDING_LF,
    Crlf => WORKSPACE_LINE_ENDING_CRLF,
    Mixed => WORKSPACE_LINE_ENDING_MIXED,
    None => WORKSPACE_LINE_ENDING_NONE,
});

wire_enum!(rs::WorkspaceReadOnlyReason => pb::WorkspaceReadOnlyReason {
    Binary => WORKSPACE_READ_ONLY_REASON_BINARY,
    UnsupportedEncoding => WORKSPACE_READ_ONLY_REASON_UNSUPPORTED_ENCODING,
    MixedLineEndings => WORKSPACE_READ_ONLY_REASON_MIXED_LINE_ENDINGS,
    Symlink => WORKSPACE_READ_ONLY_REASON_SYMLINK,
    TooLarge => WORKSPACE_READ_ONLY_REASON_TOO_LARGE,
    PermissionDenied => WORKSPACE_READ_ONLY_REASON_PERMISSION_DENIED,
    NotRegularFile => WORKSPACE_READ_ONLY_REASON_NOT_REGULAR_FILE,
    OutsideWorkspace => WORKSPACE_READ_ONLY_REASON_OUTSIDE_WORKSPACE,
});

wire_struct!(rs::WorkspaceFileText => pb::WorkspaceFileText {
    checkout_id => checkout_id,
    path => path,
    text => text,
    content_hash => content_hash,
    size => size,
    modified_at => modified_at_ms,
    encoding => encoding,
    line_ending => line_ending,
    read_only_reason => read_only_reason,
    truncated => truncated,
});

wire_struct!(rs::WorkspaceImageChunk => pb::WorkspaceImageChunk {
    checkout_id => checkout_id,
    content_hash => content_hash,
    mime_type => mime_type,
    data => data,
    next_offset => next_offset,
    size => size,
    done => done,
});

wire_enum!(rs::WorkspaceWritableEncoding => pb::WorkspaceWritableEncoding {
    Utf8 => WORKSPACE_WRITABLE_ENCODING_UTF8,
    Utf8Bom => WORKSPACE_WRITABLE_ENCODING_UTF8_BOM,
});

wire_enum!(rs::WorkspaceWritableLineEnding => pb::WorkspaceWritableLineEnding {
    Lf => WORKSPACE_WRITABLE_LINE_ENDING_LF,
    Crlf => WORKSPACE_WRITABLE_LINE_ENDING_CRLF,
});

wire_enum!(rs::WorkspaceFileConflictReason => pb::WorkspaceFileConflictReason {
    Changed => WORKSPACE_FILE_CONFLICT_REASON_CHANGED,
    Deleted => WORKSPACE_FILE_CONFLICT_REASON_DELETED,
    Replaced => WORKSPACE_FILE_CONFLICT_REASON_REPLACED,
    NotRegularFile => WORKSPACE_FILE_CONFLICT_REASON_NOT_REGULAR_FILE,
});

wire_struct!(rs::WorkspaceFileWriteResult => pb::WorkspaceFileWriteResult {
    path => path,
    content_hash => content_hash,
    size => size,
    modified_at => modified_at_ms,
});

impl Wire for rs::WriteWorkspaceFileOutcome {
    type Pb = pb::WriteWorkspaceFileOutcome;
    fn to_pb(&self) -> Self::Pb {
        let outcome = match self {
            Self::Written { file } => WriteOutcome::from(pb::WorkspaceFileWritten {
                file: msg(file),
                ..Default::default()
            }),
            Self::Conflict {
                reason,
                current_content_hash,
                current_modified_at,
            } => WriteOutcome::from(pb::WorkspaceFileConflict {
                reason: reason.to_pb(),
                current_content_hash: current_content_hash.to_pb(),
                current_modified_at_ms: current_modified_at.to_pb(),
                ..Default::default()
            }),
        };
        pb::WriteWorkspaceFileOutcome {
            outcome: Some(outcome),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.outcome {
            Some(WriteOutcome::Written(written)) => Ok(Self::Written {
                file: from_msg(written.file, "file")?,
            }),
            Some(WriteOutcome::Conflict(conflict)) => Ok(Self::Conflict {
                reason: Wire::from_pb(conflict.reason)?,
                current_content_hash: conflict.current_content_hash,
                current_modified_at: Wire::from_pb(conflict.current_modified_at_ms)?,
            }),
            None => Err(MapError::empty_oneof("WriteWorkspaceFileOutcome.outcome")),
        }
    }
}

wire_enum!(rs::WorkspaceFileChangeKind => pb::WorkspaceFileChangeKind {
    Created => WORKSPACE_FILE_CHANGE_KIND_CREATED,
    Modified => WORKSPACE_FILE_CHANGE_KIND_MODIFIED,
    Removed => WORKSPACE_FILE_CHANGE_KIND_REMOVED,
    Renamed => WORKSPACE_FILE_CHANGE_KIND_RENAMED,
});

wire_struct!(rs::WorkspaceFileChange => pb::WorkspaceFileChange {
    operation_id => operation_id,
    kind => kind,
    path => path,
    old_path => old_path,
});

wire_struct!(rs::WorkspaceFileChanges => pb::WorkspaceFileChanges {
    sequence => sequence,
    resync_required => resync_required,
    changes => changes,
});

wire_enum!(rs::WorkspaceMutationRejection => pb::WorkspaceMutationRejection {
    InvalidPath => WORKSPACE_MUTATION_REJECTION_INVALID_PATH,
    WorkspaceChanged => WORKSPACE_MUTATION_REJECTION_WORKSPACE_CHANGED,
    SourceMissing => WORKSPACE_MUTATION_REJECTION_SOURCE_MISSING,
    SourceChanged => WORKSPACE_MUTATION_REJECTION_SOURCE_CHANGED,
    DestinationExists => WORKSPACE_MUTATION_REJECTION_DESTINATION_EXISTS,
    InvalidDestination => WORKSPACE_MUTATION_REJECTION_INVALID_DESTINATION,
    PermissionDenied => WORKSPACE_MUTATION_REJECTION_PERMISSION_DENIED,
    Unsupported => WORKSPACE_MUTATION_REJECTION_UNSUPPORTED,
    Busy => WORKSPACE_MUTATION_REJECTION_BUSY,
    PartialFailure => WORKSPACE_MUTATION_REJECTION_PARTIAL_FAILURE,
});

impl Wire for rs::WorkspaceMutationOutcome {
    type Pb = pb::WorkspaceMutationOutcome;
    fn to_pb(&self) -> Self::Pb {
        let outcome = match self {
            Self::Applied {
                operation_id,
                checkout_id,
                change,
                entry,
            } => MutationOutcome::from(pb::WorkspaceMutationApplied {
                operation_id: operation_id.clone(),
                checkout_id: checkout_id.clone(),
                change: msg(change),
                entry: opt_msg(entry),
                ..Default::default()
            }),
            Self::Rejected {
                operation_id,
                reason,
                message,
            } => MutationOutcome::from(pb::WorkspaceMutationRejected {
                operation_id: operation_id.clone(),
                reason: reason.to_pb(),
                message: message.clone(),
                ..Default::default()
            }),
        };
        pb::WorkspaceMutationOutcome {
            outcome: Some(outcome),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        match pb.outcome {
            Some(MutationOutcome::Applied(applied)) => {
                let applied = *applied;
                Ok(Self::Applied {
                    operation_id: applied.operation_id,
                    checkout_id: applied.checkout_id,
                    change: from_msg(applied.change, "change")?,
                    entry: from_opt_msg(applied.entry)?,
                })
            }
            Some(MutationOutcome::Rejected(rejected)) => Ok(Self::Rejected {
                operation_id: rejected.operation_id,
                reason: Wire::from_pb(rejected.reason)?,
                message: rejected.message,
            }),
            None => Err(MapError::empty_oneof("WorkspaceMutationOutcome.outcome")),
        }
    }
}

// Requests.

wire_struct!(rs::ListWorkspaceDirectoryRequest => pb::ListDirectoryRequest {
    target => target as default_msg,
    directory => directory,
    include_ignored => include_ignored,
    cursor => cursor,
});

wire_struct!(rs::SearchWorkspaceFilesRequest => pb::SearchRequest {
    target => target as default_msg,
    query => query,
    include_ignored => include_ignored,
    limit => limit,
});

wire_struct!(rs::ReadWorkspaceFileRequest => pb::ReadFileRequest {
    target => target as default_msg,
    path => path,
});

wire_struct!(rs::ReadWorkspaceImageRequest => pb::ReadImageRequest {
    target => target as default_msg,
    path => path,
    expected_checkout_id => expected_checkout_id,
    offset => offset,
    expected_content_hash => expected_content_hash,
});

wire_struct!(rs::WriteWorkspaceFileRequest => pb::WriteFileRequest {
    expected_checkout_id => expected_checkout_id,
    target => target as default_msg,
    path => path,
    text => text,
    expected_content_hash => expected_content_hash,
    encoding => encoding,
    line_ending => line_ending,
});

wire_struct!(rs::MoveWorkspaceEntryRequest => pb::MoveEntryRequest {
    target => target as default_msg,
    operation_id => operation_id,
    expected_checkout_id => expected_checkout_id,
    source_path => source_path,
    destination_path => destination_path,
    expected_source_revision => expected_source_revision,
    expected_kind => expected_kind,
});

wire_struct!(rs::DeleteWorkspaceEntryRequest => pb::DeleteEntryRequest {
    target => target as default_msg,
    operation_id => operation_id,
    expected_checkout_id => expected_checkout_id,
    path => path,
    expected_source_revision => expected_source_revision,
    expected_kind => expected_kind,
    recursive => recursive,
});

wire_struct!(rs::WatchWorkspaceFilesRequest => pb::WatchFilesRequest {
    target => target as default_msg,
});

wire_struct!(rs::ListFoldersParams => pb::ListFoldersRequest {
    path => path,
});

wire_struct!(rs::SearchFilesParams => pb::SearchMentionsRequest {
    query => query,
    chat_id => chat_id,
    space_id => space_id,
    path => path,
});
