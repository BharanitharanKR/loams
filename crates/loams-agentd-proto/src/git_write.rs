//! Worktrees and git writes: Loams Studio's git toolbar (design §53 §6.5
//! G1–G5 and G21; plan SD1 Task 9).
//!
//! - `ListWorktrees` (G1): a repository's worktrees, joined with the chats
//!   working in each.
//! - `CommitWorktree` (G2), `DiscardPaths` (G21): checksum-guarded writes to
//!   one checkout. A stale checksum is `stale_diff`; an empty index is
//!   `nothing_to_commit`.
//! - `PushBranch` (G3): never a plain `--force`; a forced push is always
//!   `--force-with-lease` against the remote-tracking ref (`push_rejected`
//!   when the lease or a fast-forward fails).
//! - `CreateChangeRequest` (G4): Forgejo through its REST API with the
//!   daemon's credential, GitHub through `gh` (`provider_not_configured`
//!   without one).
//! - `MergeBranch` (G5): stops on conflicts with a clean tree
//!   (`merge_conflict`, whose text carries the paths as a JSON array).
//!
//! None of these is open to the `mcp` role.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::ChangeRequestState;

/// Error reason: a checksum doesn't match (commit, discard).
pub const STALE_DIFF: &str = "stale_diff";
/// Error reason: `MergeBranch` stopped on conflicts. The text is
/// `merge_conflict: <JSON array of paths>`.
pub const MERGE_CONFLICT: &str = "merge_conflict";
/// Error reason: `CommitWorktree` found nothing staged.
pub const NOTHING_TO_COMMIT: &str = "nothing_to_commit";
/// Error reason: a non-fast-forward push, or a lease that failed.
pub const PUSH_REJECTED: &str = "push_rejected";
/// Error reason: no Forgejo credential (or no `gh`) for `CreateChangeRequest`.
pub const PROVIDER_NOT_CONFIGURED: &str = "provider_not_configured";

/// One worktree of a repository (G1). The main checkout is listed first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeInfo {
    pub path: String,
    /// The branch checked out, or `HEAD` when detached.
    pub branch: String,
    /// The commit checked out (empty before the first commit).
    pub head: String,
    /// The checkout's identity, when the worktree exists on disk.
    pub checkout_id: Option<String>,
    /// This device's chats working in it.
    pub chat_ids: Vec<String>,
    /// Uncommitted changes, untracked files included.
    pub dirty: bool,
    /// Commits ahead of and behind the branch's upstream (0 without one).
    pub ahead: u32,
    pub behind: u32,
    /// The latest of the head commit's time and its chats' activity.
    pub last_activity_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct WorktreeList(pub Vec<WorktreeInfo>);

/// `CommitWorktree` (G2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommitWorktreeParams {
    /// The checkout the confirmation was opened on.
    pub checkout_id: String,
    pub cwd: String,
    pub message: String,
    /// Stage these paths (relative to the checkout) first. Empty with `all`
    /// false commits the index as it is.
    #[serde(default)]
    pub paths: Vec<String>,
    /// Stage every change first, untracked files included.
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub amend: bool,
    /// Add `Signed-off-by` with the user's own git identity.
    #[serde(default)]
    pub signoff: bool,
    /// The checkout diff's checksum the person reviewed.
    pub expected_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CommitWorktreeReply {
    pub sha: String,
    /// The commit's subject line.
    pub summary: String,
}

/// `PushBranch` (G3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PushBranchParams {
    pub cwd: String,
    /// A configured remote's name; unset means `origin`. Never a URL.
    #[serde(default)]
    pub remote: Option<String>,
    pub branch: String,
    #[serde(default)]
    pub set_upstream: bool,
    /// Overwrite the remote branch, but only if it is still where this
    /// checkout last saw it (`--force-with-lease=<ref>:<expected>`).
    #[serde(default)]
    pub force_with_lease: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PushBranchReply {
    pub remote: String,
    /// The pushed ref, `refs/heads/<branch>`.
    #[serde(rename = "ref")]
    pub ref_name: String,
    /// The remote's URL without any credentials.
    pub url: Option<String>,
}

/// Where `CreateChangeRequest` opens the change request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum ChangeRequestProvider {
    Forgejo,
    Github,
}

impl ChangeRequestProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forgejo => "forgejo",
            Self::Github => "github",
        }
    }
}

/// `CreateChangeRequest` (G4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateChangeRequestParams {
    pub cwd: String,
    pub provider: ChangeRequestProvider,
    /// The branch to merge into.
    pub base: String,
    /// The branch with the changes (already pushed).
    pub head: String,
    pub title: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub draft: bool,
    /// An issue number (`42` or `#42`); the body gains `Closes #42`.
    #[serde(default)]
    pub link_issue: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CreateChangeRequestReply {
    pub provider: ChangeRequestProvider,
    pub number: u64,
    pub url: String,
    pub state: ChangeRequestState,
}

/// How `MergeBranch` brings `source` into `target`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum MergeStrategy {
    /// A merge commit, always (`--no-ff`).
    Merge,
    /// One new commit with the source's changes.
    Squash,
    /// The source's commits replayed onto the target, then a fast-forward.
    Rebase,
    /// A fast-forward only.
    Ff,
}

/// `MergeBranch` (G5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MergeBranchParams {
    pub repo_path: String,
    pub source: String,
    pub target: String,
    pub strategy: MergeStrategy,
    #[serde(default)]
    pub message: Option<String>,
    /// Delete the source branch after a successful merge.
    #[serde(default)]
    pub delete_source: bool,
    /// Remove the source branch's worktree after a successful merge.
    #[serde(default)]
    pub remove_worktree: bool,
    /// The source commit the person chose to merge.
    pub expected_source_head: String,
}

/// A conflict is the `merge_conflict` error, not a reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct MergeBranchReply {
    /// The target's new head.
    pub sha: String,
}

/// `DiscardPaths` (G21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DiscardPathsParams {
    pub checkout_id: String,
    /// Paths relative to the checkout.
    pub paths: Vec<String>,
    pub expected_checksum: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_shapes_are_camel_case() {
        let reply = PushBranchReply {
            remote: "origin".into(),
            ref_name: "refs/heads/main".into(),
            url: None,
        };
        assert_eq!(
            serde_json::to_value(&reply).expect("serialize"),
            serde_json::json!({"remote": "origin", "ref": "refs/heads/main", "url": null})
        );
        let params: MergeBranchParams = serde_json::from_value(serde_json::json!({
            "repoPath": "/r", "source": "a", "target": "main", "strategy": "ff",
            "expectedSourceHead": "abc"
        }))
        .expect("parse");
        assert_eq!(params.strategy, MergeStrategy::Ff);
        assert!(!params.delete_source);
        let provider: ChangeRequestProvider = serde_json::from_str("\"github\"").expect("parse");
        assert_eq!(provider, ChangeRequestProvider::Github);
    }
}
