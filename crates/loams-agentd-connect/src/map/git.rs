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

// Worktrees and git writes (§53 §6.5 G1–G5, SD1 Task 9).

wire_struct!(rs::WorktreeInfo => pb::WorktreeInfo {
    path => path,
    branch => branch,
    head => head,
    checkout_id => checkout_id,
    chat_ids => chat_ids,
    dirty => dirty,
    ahead => ahead,
    behind => behind,
    last_activity_ms => last_activity_ms,
});

wire_struct!(rs::CommitWorktreeParams => pb::CommitWorktreeRequest {
    checkout_id => checkout_id,
    cwd => cwd,
    message => message,
    paths => paths,
    all => all,
    amend => amend,
    signoff => signoff,
    expected_checksum => expected_checksum,
});

wire_struct!(rs::CommitWorktreeReply => pb::CommitWorktreeResponse {
    sha => sha,
    summary => summary,
});

wire_struct!(rs::PushBranchParams => pb::PushBranchRequest {
    cwd => cwd,
    remote => remote,
    branch => branch,
    set_upstream => set_upstream,
    force_with_lease => force_with_lease,
});

wire_struct!(rs::PushBranchReply => pb::PushBranchResponse {
    remote => remote,
    ref_name => r#ref,
    url => url,
});

wire_enum!(rs::ChangeRequestProvider => pb::ChangeRequestProvider {
    Forgejo => CHANGE_REQUEST_PROVIDER_FORGEJO,
    Github => CHANGE_REQUEST_PROVIDER_GITHUB,
});

wire_struct!(rs::CreateChangeRequestParams => pb::CreateChangeRequestRequest {
    cwd => cwd,
    provider => provider,
    base => base,
    head => head,
    title => title,
    body => body,
    draft => draft,
    link_issue => link_issue,
});

wire_struct!(rs::CreateChangeRequestReply => pb::CreateChangeRequestResponse {
    provider => provider,
    number => number,
    url => url,
    state => state,
});

wire_enum!(rs::MergeStrategy => pb::MergeStrategy {
    Merge => MERGE_STRATEGY_MERGE,
    Squash => MERGE_STRATEGY_SQUASH,
    Rebase => MERGE_STRATEGY_REBASE,
    Ff => MERGE_STRATEGY_FAST_FORWARD,
});

wire_struct!(rs::MergeBranchParams => pb::MergeBranchRequest {
    repo_path => repo_path,
    source => source,
    target => target,
    strategy => strategy,
    message => message,
    delete_source => delete_source,
    remove_worktree => remove_worktree,
    expected_source_head => expected_source_head,
});

// `conflicts` stays empty: a conflict is the `merge_conflict` error, whose
// `ErrorInfo.metadata["paths"]` lists them (SD1 Shared contracts).
wire_struct!(rs::MergeBranchReply => pb::MergeBranchResponse {
    sha => sha,
});
