//! Run groups: one prompt run in parallel by several harnesses or models,
//! each in its own worktree, compared and resolved together (design §53
//! §5.5, §6.5 G12–G16, D904; plan SD1 Task 12, Ruling 5).
//!
//! - `CreateRunGroup` (G12) makes one chat and one worktree per variant from
//!   the same base, and queues the same run in each, within the daemon's
//!   concurrency limits; a member over a limit waits `queued`.
//! - `WatchRunGroups` (G13) streams every group, again on each change.
//! - `GetRunGroupComparison` (G14) answers the diff matrix and a pair's diff,
//!   computed from snapshot trees, so nothing is committed.
//! - `RunGroupCheck` (G15) runs the group's check action in each member's
//!   worktree and records the exit code and the output's tail.
//! - `ResolveRunGroup` (G16) merges the winner (Task 9's `MergeBranch`),
//!   pushes it and opens a change request, or keeps it as an ordinary thread;
//!   it refuses while members run (`group_not_ready`) unless `force`.
//!
//! None of these is open to the `mcp` role.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::git_write::{CreateChangeRequestParams, MergeBranchParams};
use crate::{CheckoutDiff, HarnessId, ReasoningLevel};

/// The daemon feature these methods add to `Hello` and `GetHost`.
pub const FEATURE: &str = "run-groups";

/// Error reason: `ResolveRunGroup` while members are still queued or
/// running, without `force` (Connect `failed_precondition`).
pub const GROUP_NOT_READY: &str = "group_not_ready";

/// The lines of a check's output a group keeps.
pub const CHECK_TAIL_LINES: usize = 200;

/// How every member of a group answers its agent's tool calls (§53 §8.1):
/// one mode for the whole group, set on each member's thread (SD1 Task 13).
pub use crate::approvals::ApprovalMode;

/// One member's harness and model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunVariant {
    pub harness: HarnessId,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub reasoning: Option<ReasoningLevel>,
    #[serde(default)]
    pub model_options: serde_json::Map<String, serde_json::Value>,
}

/// `CreateRunGroup` (G12).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateRunGroupParams {
    /// The project. Required with `checkActionId` (actions are a project's);
    /// unset for a registered repository that is not a project.
    #[serde(default)]
    pub space_id: Option<String>,
    /// The repository the worktrees are made in.
    pub repo_path: String,
    /// The ref every member's worktree starts from.
    pub base: String,
    pub prompt: String,
    /// Staged uploads, as for any run.
    #[serde(default)]
    pub attachments: Vec<String>,
    /// One member per variant, in order.
    pub variants: Vec<RunVariant>,
    /// The project action `RunGroupCheck` runs.
    #[serde(default)]
    pub check_action_id: Option<String>,
    #[serde(default)]
    pub approval_mode: ApprovalMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateRunGroupReply {
    pub group_id: String,
    /// The members' chats, in the variants' order.
    pub chat_ids: Vec<String>,
}

/// `WatchRunGroups` (G13).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WatchRunGroupsParams {
    /// Only this project's groups.
    #[serde(default)]
    pub space_id: Option<String>,
}

/// `WatchRunGroups`' item: every group, newest first.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupList {
    pub groups: Vec<RunGroup>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum RunGroupState {
    /// A member is queued, running or waiting for input.
    Running,
    /// Every member has finished; the group can be resolved.
    Ready,
    Resolved,
}

/// One member's run, as the group sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum RunMemberStatus {
    /// Over the concurrency limits: its run waits to be sent.
    Queued,
    Running,
    AwaitingInput,
    Done,
    Errored,
    /// Never sent: the group was resolved with `force` first.
    Cancelled,
}

impl RunMemberStatus {
    /// Still queued or running: the group is not ready.
    pub fn is_live(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::AwaitingInput)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum CheckState {
    Pending,
    Running,
    Passed,
    Failed,
}

/// A member's check (G15).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemberCheck {
    pub state: CheckState,
    #[serde(default)]
    pub exit_code: Option<i32>,
    /// The check's terminal (`SubscribeTerminal`).
    #[serde(default)]
    pub terminal_id: Option<String>,
    /// The output's last [`CHECK_TAIL_LINES`] lines, as text.
    #[serde(default)]
    pub log_tail: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupMember {
    pub chat_id: String,
    pub harness: HarnessId,
    #[serde(default)]
    pub model: Option<String>,
    pub status: RunMemberStatus,
    /// The member's checkout diff (`GetCheckoutDiff`).
    pub files_changed: u32,
    pub additions: u32,
    pub deletions: u32,
    /// The running time so far, or of the whole run once it ended.
    pub elapsed_ms: i64,
    /// When the run started, for a live elapsed time.
    #[serde(default)]
    pub started_at_ms: Option<i64>,
    /// Unset until the harness reports usage.
    #[serde(default)]
    pub tokens: Option<u64>,
    /// Unset until a check runs.
    #[serde(default)]
    pub check: Option<MemberCheck>,
    /// The member's own worktree and its branch.
    pub cwd: String,
    pub branch: String,
    /// Resolved with `keep`: an ordinary thread now.
    #[serde(default)]
    pub detached: bool,
    #[serde(default)]
    pub archived: bool,
}

/// What `ResolveRunGroup` does with the winner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum ResolveAction {
    /// Merge its branch (Task 9's `MergeBranch`).
    Merge,
    /// Push its branch and open a change request (`PushBranch`,
    /// `CreateChangeRequest`).
    Pr,
    /// Detach it from the group as an ordinary thread.
    Keep,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupResolution {
    pub action: ResolveAction,
    pub winner_chat_id: String,
    /// What happened, in a sentence.
    pub result: String,
    /// The merge target's new head.
    #[serde(default)]
    pub sha: Option<String>,
    /// The change request.
    #[serde(default)]
    pub url: Option<String>,
    pub resolved_at_ms: i64,
}

/// A group record (Ruling 5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroup {
    pub id: String,
    #[serde(default)]
    pub space_id: Option<String>,
    pub repo_path: String,
    pub prompt: String,
    /// The ref the members' worktrees were made from.
    pub base: String,
    /// `base`'s commit when the group was created: the comparison's base.
    pub base_sha: String,
    pub created_at_ms: i64,
    pub state: RunGroupState,
    pub approval_mode: ApprovalMode,
    #[serde(default)]
    pub check_action_id: Option<String>,
    pub members: Vec<RunGroupMember>,
    #[serde(default)]
    pub resolution: Option<RunGroupResolution>,
}

/// `GetRunGroupComparison` (G14).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GetRunGroupComparisonParams {
    pub group_id: String,
    /// A member's chat.
    pub a: String,
    /// Another member's chat; unset compares `a` with the group's base.
    #[serde(default)]
    pub b: Option<String>,
    /// Only this path (relative to the repository).
    #[serde(default)]
    pub path: Option<String>,
}

/// One member's change to one path, against the group's base.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemberDelta {
    pub chat_id: String,
    pub additions: u32,
    pub deletions: u32,
}

/// One path, and the members that changed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ComparisonRow {
    pub path: String,
    pub per_member: Vec<MemberDelta>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupComparison {
    /// Sorted by path.
    pub matrix: Vec<ComparisonRow>,
    /// From `a`'s result to `b`'s (or from the base to `a`'s).
    #[serde(default)]
    pub pair_diff: Option<CheckoutDiff>,
}

/// `RunGroupCheck` (G15).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupCheckParams {
    pub group_id: String,
    /// Only this member; unset runs the check in every member.
    #[serde(default)]
    pub chat_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CheckRun {
    pub chat_id: String,
    pub terminal_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunGroupCheckReply {
    pub runs: Vec<CheckRun>,
}

/// `ResolveRunGroup` (G16).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRunGroupParams {
    pub group_id: String,
    pub winner_chat_id: String,
    pub action: ResolveAction,
    /// With `merge`; its `source` must be the winner's branch and its
    /// `repoPath` the group's repository.
    #[serde(default)]
    pub merge: Option<MergeBranchParams>,
    /// With `pr`; its `cwd` and `head` are the winner's.
    #[serde(default)]
    pub pr: Option<CreateChangeRequestParams>,
    #[serde(default)]
    pub archive_others: bool,
    #[serde(default)]
    pub remove_other_worktrees: bool,
    /// Resolve while members still run: queued members are cancelled and
    /// running ones interrupted.
    #[serde(default)]
    pub force: bool,
    /// The winner's checkout diff checksum the person confirmed; required
    /// when the winner's worktree has uncommitted changes, which `merge` and
    /// `pr` commit first (Task 9's `CommitWorktree`).
    #[serde(default)]
    pub expected_checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRunGroupReply {
    pub result: String,
    #[serde(default)]
    pub sha: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_shapes() {
        let params: ResolveRunGroupParams = serde_json::from_value(serde_json::json!({
            "groupId": "g", "winnerChatId": "c", "action": "pr"
        }))
        .expect("parse");
        assert_eq!(params.action, ResolveAction::Pr);
        assert!(!params.force && params.merge.is_none());
        assert_eq!(
            serde_json::to_value(ApprovalMode::FullAccess).expect("serialize"),
            serde_json::json!("full-access")
        );
        assert_eq!(
            serde_json::to_value(RunMemberStatus::AwaitingInput).expect("serialize"),
            serde_json::json!("awaitingInput")
        );
        assert!(RunMemberStatus::Queued.is_live() && !RunMemberStatus::Done.is_live());
    }
}
