//! `ListWorktrees` (design §53 §5.4, §6.5 G1; plan SD1 Task 9): a
//! repository's worktrees from `git worktree list --porcelain`, each with its
//! checkout identity, the chats of this device working in it, whether it has
//! uncommitted changes, and how far it is ahead of and behind its upstream.
//! Studio's "Clean up" and its worktree picker read it.

use std::path::Path;

use loams_agentd_proto::Chat;
use loams_agentd_proto::git_write::WorktreeInfo;

use crate::git_write::{GitWriteError, git_try, worktrees};
use crate::repos::Repos;

/// The worktrees of the repository at `repo`, the main one first, joined
/// with `chats` (this device's chats; a chat belongs to a worktree when its
/// `checkoutId` or its `cwd` names it).
///
/// # Errors
///
/// When `git worktree list` itself fails.
pub async fn list_worktrees(
    repos: &Repos,
    repo: &Path,
    chats: &[Chat],
) -> Result<Vec<WorktreeInfo>, GitWriteError> {
    let mut out = Vec::new();
    for stanza in worktrees(repo).await? {
        if stanza.bare {
            continue;
        }
        let path = std::fs::canonicalize(&stanza.path).unwrap_or_else(|_| stanza.path.clone());
        let exists = path.is_dir();
        let checkout_id = if exists {
            repos
                .checkout_identity(&path)
                .await
                .ok()
                .map(|identity| identity.id)
        } else {
            None
        };

        let mut chat_ids = Vec::new();
        let mut last_activity_ms = 0i64;
        for chat in chats {
            let by_id = checkout_id.is_some() && chat.checkout_id == checkout_id;
            let by_cwd = chat
                .cwd
                .as_deref()
                .is_some_and(|cwd| std::fs::canonicalize(cwd).is_ok_and(|cwd| cwd == path));
            if by_id || by_cwd {
                chat_ids.push(chat.id.clone());
                let at = chat.last_message_at.unwrap_or(chat.created_at);
                last_activity_ms = last_activity_ms.max(at.timestamp_millis());
            }
        }
        chat_ids.sort();

        let (dirty, ahead, behind) = if exists {
            let dirty = git_try(
                &path,
                &["status", "--porcelain", "--untracked-files=normal"],
            )
            .await
            .is_some_and(|status| !status.is_empty());
            let (ahead, behind) = git_try(
                &path,
                &["rev-list", "--left-right", "--count", "HEAD...@{upstream}"],
            )
            .await
            .and_then(|counts| {
                let mut counts = counts.split_whitespace().map(str::parse::<u32>);
                Some((counts.next()?.ok()?, counts.next()?.ok()?))
            })
            .unwrap_or((0, 0));
            if let Some(seconds) = git_try(&path, &["log", "-1", "--format=%ct", "HEAD"])
                .await
                .and_then(|seconds| seconds.parse::<i64>().ok())
            {
                last_activity_ms = last_activity_ms.max(seconds.saturating_mul(1000));
            }
            (dirty, ahead, behind)
        } else {
            (false, 0, 0)
        };

        out.push(WorktreeInfo {
            path: path.to_string_lossy().into_owned(),
            branch: stanza.branch.unwrap_or_else(|| "HEAD".to_owned()),
            head: stanza.head,
            checkout_id,
            chat_ids,
            dirty,
            ahead,
            behind,
            last_activity_ms,
        });
    }
    Ok(out)
}
