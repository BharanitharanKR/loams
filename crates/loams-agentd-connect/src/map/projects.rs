//! projects.proto: spaces, repositories and project actions.

use loams_agentd_proto as rs;

use crate::proto::loams::agentd::v1 as pb;

wire_struct!(rs::Space => pb::Space {
    id => id,
    device_id => device_id,
    path => path,
    name => name,
    git_detected => git_detected,
    git_checked_at => git_checked_at_ms,
    checkout_id => checkout_id,
    created_at => created_at_ms,
});

wire_struct!(rs::Repo => pb::Repo {
    path => path,
    name => name,
    default_branch => default_branch,
});

wire_enum!(rs::ProjectActionIcon => pb::ProjectActionIcon {
    Play => PROJECT_ACTION_ICON_PLAY,
    Test => PROJECT_ACTION_ICON_TEST,
    Lint => PROJECT_ACTION_ICON_LINT,
    Configure => PROJECT_ACTION_ICON_CONFIGURE,
    Build => PROJECT_ACTION_ICON_BUILD,
    Debug => PROJECT_ACTION_ICON_DEBUG,
});

wire_struct!(rs::ProjectAction => pb::ProjectAction {
    id => id,
    name => name,
    command => command,
    icon => icon,
    run_on_worktree_create => run_on_worktree_create,
});

wire_struct!(rs::ProjectActionDraft => pb::ProjectActionDraft {
    name => name,
    command => command,
    icon => icon,
    run_on_worktree_create => run_on_worktree_create,
});

wire_struct!(rs::ProjectActionsSnapshot => pb::ProjectActionsSnapshot {
    space_id => space_id,
    actions => actions,
    importable_actions => importable_actions,
    project_file_issue => project_file_issue,
});

wire_struct!(rs::ProjectActionRun => pb::ProjectActionRun {
    action_id => action_id,
    action_name => action_name,
    terminal => terminal as msg,
});

wire_struct!(rs::ProjectActionSetup => pb::ProjectActionSetup {
    ready => ready,
    setup_action => setup_action as opt_msg,
    setup_error => setup_error,
});
