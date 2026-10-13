//! git.proto: refs, history, worktrees, status and change requests.

use loams_agentd_proto as rs;

use super::{MapError, Wire, from_opt_msg, opt_msg};
use crate::proto::loams::agentd::v1 as pb;

wire_struct!(rs::RepoRef => pb::RepoRef {
    name => name,
    current => current,
    worktree_path => worktree_path,
});

wire_enum!(rs::GitHistoryRefKind => pb::GitHistoryRefKind {
    Branch => GIT_HISTORY_REF_KIND_BRANCH,
    Remote => GIT_HISTORY_REF_KIND_REMOTE,
    Tag => GIT_HISTORY_REF_KIND_TAG,
});

wire_struct!(rs::GitHistoryRef => pb::GitHistoryRef {
    kind => kind,
    label => label,
});

wire_struct!(rs::GitHistoryCommit => pb::GitHistoryCommit {
    sha => sha,
    parent_shas => parent_shas,
    subject => subject,
    author_name => author_name,
    author_email => author_email,
    authored_at => authored_at,
    refs => refs,
});

wire_struct!(rs::GitHistoryComparison => pb::GitHistoryComparison {
    base => base,
    ahead => ahead,
    behind => behind,
});

wire_struct!(rs::GitHistoryPage => pb::GitHistoryPage {
    commits => commits,
    branch_tips => branch_tips,
    head_sha => head_sha,
    next_cursor => next_cursor,
    total_count => total_count,
    head_commit_count => head_commit_count,
    comparison => comparison as opt_msg,
});

wire_struct!(rs::GitAvatarAuthor => pb::GitAvatarAuthor {
    sha => sha,
    email => email,
});

impl Wire for rs::CreateWorktreeOutcome {
    type Pb = pb::CreateWorktreeOutcome;
    fn to_pb(&self) -> Self::Pb {
        // `worktree` is flattened on the ndjson wire, so its fields sit
        // beside the setup's here too.
        let rs::CreateWorktreeOutcome {
            worktree:
                rs::Worktree {
                    repo_path,
                    path,
                    branch,
                    name,
                    checkout_id,
                },
            setup_action,
            setup_error,
        } = self;
        pb::CreateWorktreeOutcome {
            repo_path: repo_path.clone(),
            path: path.clone(),
            branch: branch.clone(),
            name: name.clone(),
            checkout_id: checkout_id.clone(),
            setup_action: opt_msg(setup_action),
            setup_error: setup_error.clone(),
            ..Default::default()
        }
    }
    fn from_pb(pb: Self::Pb) -> Result<Self, MapError> {
        Ok(rs::CreateWorktreeOutcome {
            worktree: rs::Worktree {
                repo_path: pb.repo_path,
                path: pb.path,
                branch: pb.branch,
                name: pb.name,
                checkout_id: pb.checkout_id,
            },
            setup_action: from_opt_msg(pb.setup_action)?,
            setup_error: pb.setup_error,
        })
    }
}

wire_enum!(rs::GitFileState => pb::GitFileState {
    Unchanged => GIT_FILE_STATE_UNCHANGED,
    Added => GIT_FILE_STATE_ADDED,
    Modified => GIT_FILE_STATE_MODIFIED,
    Deleted => GIT_FILE_STATE_DELETED,
    Renamed => GIT_FILE_STATE_RENAMED,
    Copied => GIT_FILE_STATE_COPIED,
    Unmerged => GIT_FILE_STATE_UNMERGED,
    Untracked => GIT_FILE_STATE_UNTRACKED,
    TypeChanged => GIT_FILE_STATE_TYPE_CHANGED,
});

wire_struct!(rs::GitFileStatus => pb::GitFileStatus {
    path => path,
    old_path => old_path,
    index => index,
    worktree => worktree,
});

wire_struct!(rs::CheckoutGitStatus => pb::CheckoutGitStatus {
    checkout_id => checkout_id,
    device_id => device_id,
    revision => revision,
    complete => complete,
    files => files,
});

wire_enum!(rs::ChangeRequestState => pb::ChangeRequestState {
    Open => CHANGE_REQUEST_STATE_OPEN,
    Closed => CHANGE_REQUEST_STATE_CLOSED,
    Merged => CHANGE_REQUEST_STATE_MERGED,
});

wire_struct!(rs::ChangeRequestSummary => pb::ChangeRequestSummary {
    provider => provider,
    number => number,
    title => title,
    url => url,
    state => state,
    base_ref => base_ref,
    head_ref => head_ref,
});

wire_struct!(rs::CheckoutChangeRequestStatus => pb::CheckoutChangeRequestStatus {
    checkout_id => checkout_id,
    device_id => device_id,
    cwd => cwd,
    branch => branch,
    change_request => change_request as opt_msg,
    updated_at => updated_at_ms,
});

// Requests whose message is theirs alone.

wire_struct!(rs::SwitchRefParams => pb::SwitchRefRequest {
    repo_path => repo_path,
    ref_name => ref_name,
});

wire_struct!(rs::CreateWorktreeParams => pb::CreateWorktreeRequest {
    repo_path => repo_path,
    branch => branch,
    space_id => space_id,
});

wire_struct!(rs::DeleteWorktreeParams => pb::DeleteWorktreeRequest {
    repo_path => repo_path,
    worktree_path => worktree_path,
});

wire_struct!(rs::WatchCheckoutChangeRequestParams => pb::WatchChangeRequestRequest {
    cwd => cwd,
    branch => branch,
});
