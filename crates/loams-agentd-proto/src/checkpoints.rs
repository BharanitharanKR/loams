//! Per-turn checkpoints and restore (design §53 §6.5 G6, G7; plan SD1
//! Task 10).
//!
//! At each turn's start and end the daemon writes a hidden commit under
//! `refs/loams/checkpoints/<chatId>/turn/<n>/{pre,post}` (plan SD1 Ruling 6,
//! T3 Code's scheme rewritten in Rust). A turn is named on the wire by its
//! `turnId`, the id of the user message that started it, so a client joins a
//! checkpoint to its transcript entry.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The daemon feature these methods add to `Hello` and `GetHost`.
pub const FEATURE: &str = "checkpoints";

/// Error reason: no checkpoint for that turn (Connect `not_found`).
pub const CHECKPOINT_MISSING: &str = "checkpoint_missing";

/// Error reason: the checkout's diff checksum no longer matches the one the
/// caller confirmed (Connect `failed_precondition`).
pub const STALE_DIFF: &str = "stale_diff";

/// The ref namespace every checkpoint lives under.
pub const REF_PREFIX: &str = "refs/loams/checkpoints";

/// `ListCheckpoints`' params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ListCheckpointsParams {
    pub chat_id: String,
}

/// One turn's restore point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    /// The id of the user message that started the turn.
    pub turn_id: String,
    /// The turn's `pre` ref, `refs/loams/checkpoints/<chatId>/turn/<n>/pre`.
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// When the `pre` commit was written.
    pub created_at_ms: i64,
    /// Paths that differ between the turn's `pre` and its end (its `post`,
    /// else the next turn's `pre`); 0 while the turn has no end yet.
    pub files_changed: u32,
}

/// `ListCheckpoints`' result, oldest turn first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointList {
    pub checkpoints: Vec<Checkpoint>,
}

/// What `RestoreCheckpoint` restores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub enum CheckpointScope {
    /// The checkout only.
    #[serde(rename = "worktree")]
    Worktree,
    /// The checkout, and a fork of the thread at that turn. History is never
    /// deleted.
    #[serde(rename = "worktree+transcript")]
    WorktreeAndTranscript,
}

/// `RestoreCheckpoint`'s params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RestoreCheckpointParams {
    pub chat_id: String,
    pub turn_id: String,
    /// The checkout's diff checksum (`CheckoutDiff.checksum`) the person
    /// confirmed the restore against. Required: a restore throws away the
    /// checkout's current state.
    pub expected_checksum: String,
    pub scope: CheckpointScope,
}

/// `RestoreCheckpoint`'s result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RestoreCheckpointReply {
    /// The commit the checkout was restored to (the turn's `pre`).
    pub restored_to: String,
    /// The new thread, for `worktree+transcript`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_chat_id: Option<String>,
}
