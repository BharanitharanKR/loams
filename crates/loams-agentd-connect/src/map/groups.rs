//! groups.proto: run groups (G12–G16; SD1 Task 12).

use loams_agentd_proto::groups as rs_groups;

use crate::proto::loams::agentd::v1 as pb;

wire_enum!(rs_groups::RunGroupState => pb::RunGroupState {
    Running => RUN_GROUP_STATE_RUNNING,
    Ready => RUN_GROUP_STATE_READY,
    Resolved => RUN_GROUP_STATE_RESOLVED,
});

wire_enum!(rs_groups::RunMemberStatus => pb::RunMemberStatus {
    Queued => RUN_MEMBER_STATUS_QUEUED,
    Running => RUN_MEMBER_STATUS_RUNNING,
    AwaitingInput => RUN_MEMBER_STATUS_AWAITING_INPUT,
    Done => RUN_MEMBER_STATUS_DONE,
    Errored => RUN_MEMBER_STATUS_ERRORED,
    Cancelled => RUN_MEMBER_STATUS_CANCELLED,
});

wire_enum!(rs_groups::CheckState => pb::CheckState {
    Pending => CHECK_STATE_PENDING,
    Running => CHECK_STATE_RUNNING,
    Passed => CHECK_STATE_PASSED,
    Failed => CHECK_STATE_FAILED,
});

wire_enum!(rs_groups::ResolveAction => pb::ResolveAction {
    Merge => RESOLVE_ACTION_MERGE,
    Pr => RESOLVE_ACTION_CHANGE_REQUEST,
    Keep => RESOLVE_ACTION_KEEP,
});

wire_struct!(rs_groups::RunVariant => pb::RunVariant {
    harness => harness,
    model => model,
    reasoning => reasoning,
    model_options => model_options as map,
});

wire_struct!(rs_groups::MemberCheck => pb::MemberCheck {
    state => state,
    exit_code => exit_code,
    terminal_id => terminal_id,
    log_tail => log_tail,
});

wire_struct!(rs_groups::RunGroupMember => pb::RunGroupMember {
    chat_id => chat_id,
    harness => harness,
    model => model,
    status => status,
    files_changed => files_changed,
    additions => additions,
    deletions => deletions,
    elapsed_ms => elapsed_ms,
    started_at_ms => started_at_ms,
    tokens => tokens,
    check => check as opt_msg,
    cwd => cwd,
    branch => branch,
    detached => detached,
    archived => archived,
});

wire_struct!(rs_groups::RunGroupResolution => pb::RunGroupResolution {
    action => action,
    winner_chat_id => winner_chat_id,
    result => result,
    sha => sha,
    url => url,
    resolved_at_ms => resolved_at_ms,
});

wire_struct!(rs_groups::RunGroup => pb::RunGroup {
    id => id,
    space_id => space_id,
    repo_path => repo_path,
    prompt => prompt,
    base => base,
    base_sha => base_sha,
    created_at_ms => created_at_ms,
    state => state,
    approval_mode => approval_mode,
    check_action_id => check_action_id,
    members => members,
    resolution => resolution as opt_msg,
});

wire_struct!(rs_groups::CreateRunGroupParams => pb::CreateRunGroupRequest {
    space_id => space_id,
    repo_path => repo_path,
    base => base,
    prompt => prompt,
    attachments => attachments,
    variants => variants,
    check_action_id => check_action_id,
    approval_mode => approval_mode,
});

wire_struct!(rs_groups::CreateRunGroupReply => pb::CreateRunGroupResponse {
    group_id => group_id,
    chat_ids => chat_ids,
});

wire_struct!(rs_groups::WatchRunGroupsParams => pb::WatchRunGroupsRequest {
    space_id => space_id,
});

wire_struct!(rs_groups::RunGroupList => pb::WatchRunGroupsResponse {
    groups => groups,
});

wire_struct!(rs_groups::GetRunGroupComparisonParams => pb::GetRunGroupComparisonRequest {
    group_id => group_id,
    a => a,
    b => b,
    path => path,
});

wire_struct!(rs_groups::MemberDelta => pb::MemberDelta {
    chat_id => chat_id,
    additions => additions,
    deletions => deletions,
});

wire_struct!(rs_groups::ComparisonRow => pb::ComparisonRow {
    path => path,
    per_member => per_member,
});

wire_struct!(rs_groups::RunGroupComparison => pb::GetRunGroupComparisonResponse {
    matrix => matrix,
    pair_diff => pair_diff as opt_msg,
});

wire_struct!(rs_groups::RunGroupCheckParams => pb::RunGroupCheckRequest {
    group_id => group_id,
    chat_id => chat_id,
});

wire_struct!(rs_groups::CheckRun => pb::CheckRun {
    chat_id => chat_id,
    terminal_id => terminal_id,
});

wire_struct!(rs_groups::RunGroupCheckReply => pb::RunGroupCheckResponse {
    runs => runs,
});

wire_struct!(rs_groups::ResolveRunGroupParams => pb::ResolveRunGroupRequest {
    group_id => group_id,
    winner_chat_id => winner_chat_id,
    action => action,
    merge => merge as opt_msg,
    pr => change_request as opt_msg,
    archive_others => archive_others,
    remove_other_worktrees => remove_other_worktrees,
    force => force,
    expected_checksum => expected_checksum,
});

wire_struct!(rs_groups::ResolveRunGroupReply => pb::ResolveRunGroupResponse {
    result => result,
    sha => sha,
    url => url,
});
