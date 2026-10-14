//! Review comments and content search: Loams Studio's diff review and its
//! search dialog (design §53 §5.6, §6.5 G8–G11 and G23; plan SD1 Task 11).
//!
//! - `WatchReviewComments` (G8): a checkout's comments as a reset, then
//!   upserts and removals. `outdated` is computed by the daemon, never
//!   stored: the comment's diff checksum is not the checkout's current one,
//!   and its anchored lines are no longer in the current diff.
//! - `UpsertReviewComment`, `DeleteReviewComment`, `ResolveReviewComment`
//!   (G9): stored in the daemon's SQLite store, keyed by checkout.
//! - `SendReviewToThread` (G10): queues one run whose prompt is a fixed
//!   template; the bodies are quoted as data.
//! - `PostReviewToChangeRequest` (G11): posts the comments as a review on the
//!   checkout branch's open Forgejo or GitHub pull request.
//! - `SearchWorkspaceContent` (G23): a streamed search of one workspace's
//!   files, honouring `.gitignore`, with a result, size and time cap.
//!
//! None of these is open to the `mcp` role.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Which side of the diff a comment is anchored to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum DiffSide {
    /// The base's line numbers (a removed or context line).
    Old,
    /// The working tree's line numbers (an added or context line).
    New,
}

/// One review comment (G9), anchored to `{checkoutId, path, side, line,
/// endLine?, diffChecksum}` (§53 §5.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ReviewComment {
    /// Empty on an upsert creates a comment; the daemon assigns the id.
    #[serde(default)]
    pub id: String,
    pub checkout_id: String,
    /// Relative to the checkout, `/`-separated.
    pub path: String,
    pub side: DiffSide,
    /// 1-based.
    pub line: u32,
    /// The last line of a range, at least `line`.
    #[serde(default)]
    pub end_line: Option<u32>,
    pub body: String,
    /// The checksum of the diff the comment was written against.
    pub diff_checksum: String,
    /// Set by the daemon when the comment is created; kept on an update.
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub resolved: bool,
    /// Computed by the daemon from the checkout's current diff; ignored on
    /// an upsert.
    #[serde(default)]
    pub outdated: bool,
}

/// `WatchReviewComments` (G8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WatchReviewCommentsParams {
    pub checkout_id: String,
}

/// One `WatchReviewComments` item: the first is always a reset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(untagged)]
pub enum ReviewCommentsFrame {
    /// Every comment of the checkout, oldest first.
    Reset { reset: Vec<ReviewComment> },
    /// Comments added or changed (also when only `outdated` changed), and
    /// the ids of comments removed.
    Delta {
        #[serde(default)]
        upsert: Vec<ReviewComment>,
        #[serde(default)]
        remove: Vec<String>,
    },
}

/// `DeleteReviewComment` (G9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DeleteReviewCommentParams {
    pub checkout_id: String,
    pub id: String,
}

/// `ResolveReviewComment` (G9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ResolveReviewCommentParams {
    pub checkout_id: String,
    pub id: String,
    pub resolved: bool,
}

/// `SendReviewToThread` (G10). The comments must be on the thread's
/// checkout. The reply is the queued run's `QueueCommandReply`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SendReviewToThreadParams {
    pub chat_id: String,
    pub comment_ids: Vec<String>,
}

/// What a posted review asks of the pull request's author.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ReviewEvent {
    Comment,
    RequestChanges,
}

/// `PostReviewToChangeRequest` (G11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PostReviewToChangeRequestParams {
    pub checkout_id: String,
    pub comment_ids: Vec<String>,
    pub event: ReviewEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PostReviewToChangeRequestReply {
    /// The posted review (or its pull request) on the provider.
    pub url: String,
}

/// `SearchWorkspaceContent` (G23): exactly one of `spaceId` and `chatId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SearchWorkspaceContentParams {
    #[serde(default)]
    pub space_id: Option<String>,
    #[serde(default)]
    pub chat_id: Option<String>,
    pub query: String,
    /// `query` is a regular expression (Rust `regex` syntax); otherwise a
    /// literal.
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
    /// Only paths matching one of these globs (gitignore syntax; a leading
    /// `!` excludes).
    #[serde(default)]
    pub globs: Vec<String>,
    /// 0 is the cap, 2 000; more is clamped to it.
    #[serde(default)]
    pub max_results: u32,
}

/// One `SearchWorkspaceContent` item: one match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ContentSearchMatch {
    /// Relative to the workspace root, `/`-separated.
    pub path: String,
    /// 1-based.
    pub line: u32,
    /// 1-based, in characters.
    pub column: u32,
    /// The matching line, without its line ending, at most 400 characters.
    pub preview: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_shapes() {
        let frame: ReviewCommentsFrame =
            serde_json::from_value(serde_json::json!({"upsert": [], "remove": ["c1"]}))
                .expect("parse");
        assert_eq!(
            frame,
            ReviewCommentsFrame::Delta {
                upsert: Vec::new(),
                remove: vec!["c1".into()]
            }
        );
        let frame: ReviewCommentsFrame =
            serde_json::from_value(serde_json::json!({"reset": []})).expect("parse");
        assert_eq!(frame, ReviewCommentsFrame::Reset { reset: Vec::new() });
        let event: ReviewEvent = serde_json::from_str("\"request_changes\"").expect("parse");
        assert_eq!(event, ReviewEvent::RequestChanges);
        let comment: ReviewComment = serde_json::from_value(serde_json::json!({
            "checkoutId": "c", "path": "a.txt", "side": "new", "line": 2,
            "body": "Why?", "diffChecksum": "sum"
        }))
        .expect("parse");
        assert!(comment.id.is_empty() && !comment.outdated && comment.end_line.is_none());
    }
}
