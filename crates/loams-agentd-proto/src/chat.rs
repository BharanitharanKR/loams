//! The session doc's wire model: transcript entries and their parts, the
//! commands a client queues, and the pending-message queue (plan DD1 Task 9).
//!
//! Moved from `loams-agentd-doc` (which re-exports every type here), so the
//! method table and the TypeScript bindings can name them. The doc crate keeps
//! the Loro storage and the folds over these types.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{RunRequest, ToolCall, ToolDiff, UserInputAnswer, UserInputQuestion};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

/// One entry in the doc's `messages` list (`SessionMessageEntry` in TS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SessionMessageEntry {
    pub id: String,
    pub role: MessageRole,
    pub parts: Vec<MessagePart>,
    /// Epoch millis.
    pub created_at: i64,
    pub device_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<MessageStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_of: Option<String>,
    /// Wall-clock length of this assistant turn, stamped when the segment
    /// finishes. Absent on user rows, live streams, and docs written before
    /// the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
}

/// Per-file diff stats persisted in place of inline diff text (t3's shape).
/// The inline diff was the bigger bomb than outputs — 32KB/edit, unexercised
/// only because the claude harness emits none. Full diff text lives in the
/// sidecar behind `diff_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ToolDiffStat {
    pub path: String,
    pub additions: u64,
    pub deletions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum MessageStatus {
    Streaming,
    Complete,
    Aborted,
}

/// Lifecycle of a spawned subagent, carried on its spawn chip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum SubagentStatus {
    Running,
    Done,
    Failed,
}

/// One rendered part of an assistant message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "camelCase")]
// `Tool` is the common part, not an outlier; boxing it would add an allocation per part.
#[allow(clippy::large_enum_variant)]
pub enum MessagePart {
    Text {
        id: String,
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Image {
        id: String,
        path: String,
        name: String,
        mime_type: String,
    },
    /// Model thinking. Carries its body in a dedicated doc field (`reasoning`,
    /// never `text`) so pre-reasoning desktop builds — whose unknown-kind
    /// fallback renders `text` as prose — degrade to an invisible empty text
    /// part instead of leaking raw thinking into the transcript. iOS drops
    /// unknown kinds entirely.
    Reasoning {
        id: String,
        text: String,
    },
    #[serde(rename_all = "camelCase")]
    Tool {
        id: String,
        call: ToolCall,
        #[serde(default)]
        is_error: bool,
        /// True once a ToolResult arrived.
        #[serde(default)]
        resolved: bool,
        /// One-line tool output summary ([`summarize_tool_output`]). Old
        /// entries (pre-strip) still carry up to 4KB here; old app versions
        /// render this field either way, so the strip is invisible to them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// Inline file diff — written by pre-strip app versions only; new
        /// folds persist [`Self::Tool::diff_stats`] + `diff_ref` instead.
        /// Kept so old docs render their diffs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff: Option<ToolDiff>,
        /// Sidecar key (`{chatId}/{partId}`) of the full output — additive;
        /// stamped by [`apply_sidecar_refs`] (the fold is chat-agnostic).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_ref: Option<String>,
        /// Full-output byte length, so the UI can say "Show full output (12 KB)".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_bytes: Option<u64>,
        /// Sidecar key (`{chatId}/{partId}.diff`) of the full diff JSON.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff_ref: Option<String>,
        /// Per-file diff stats (additive replacement for inline `diff`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff_stats: Option<Vec<ToolDiffStat>>,
        /// The SUBAGENT doc id this spawn chip indexes (additive; stamped by
        /// the engine like the sidecar refs — the fold is chat-agnostic).
        /// The chip IS the index: the client learns the doc/blob id from it,
        /// there is no listing endpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_ref: Option<String>,
        /// Subagent lifecycle, DISTINCT from `resolved`: under the eager-done
        /// policy the spawn tool's own result lands while the subagent still
        /// runs. `running` → the ref is a live doc (watch it); `done`/
        /// `failed` → frozen (blob first, doc as fallback).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_status: Option<SubagentStatus>,
        /// One-line live tail of the subagent's latest output, folded from
        /// its tagged text deltas (capped; display-only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_tail: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Input {
        id: String,
        request_id: String,
        questions: Vec<UserInputQuestion>,
        #[serde(default)]
        resolved: bool,
    },
    Error {
        id: String,
        message: String,
    },
    /// The seam in a forked chat's transcript: everything above was copied
    /// from `source_chat_id` when the fork was cut, everything below is this
    /// chat's own. Written once by the fork RPC; renders as a labeled
    /// divider. Old desktop builds' unknown-kind fallback yields an empty
    /// text part (invisible); iOS drops unknown kinds.
    #[serde(rename_all = "camelCase")]
    Fork {
        id: String,
        source_chat_id: String,
        /// The source's title as of the fork — the transcript reads it
        /// without a registry lookup, and a later rename or delete of the
        /// source does not rewrite history.
        source_title: String,
    },
}

impl MessagePart {
    pub fn id(&self) -> &str {
        match self {
            MessagePart::Text { id, .. }
            | MessagePart::Image { id, .. }
            | MessagePart::Reasoning { id, .. }
            | MessagePart::Tool { id, .. }
            | MessagePart::Input { id, .. }
            | MessagePart::Error { id, .. }
            | MessagePart::Fork { id, .. } => id,
        }
    }

    pub fn byte_len(&self) -> usize {
        match self {
            MessagePart::Text { text, .. } | MessagePart::Reasoning { text, .. } => text.len(),
            MessagePart::Tool {
                call,
                output,
                diff,
                diff_stats,
                ..
            } => {
                serde_json::to_vec(call).map_or(0, |v| v.len())
                    + output.as_ref().map_or(0, String::len)
                    + diff
                        .as_ref()
                        .map_or(0, |d| serde_json::to_vec(d).map_or(0, |v| v.len()))
                    + diff_stats
                        .as_ref()
                        .map_or(0, |s| serde_json::to_vec(s).map_or(0, |v| v.len()))
            }
            MessagePart::Input { questions, .. } => {
                serde_json::to_vec(questions).map_or(0, |v| v.len())
            }
            MessagePart::Image {
                id,
                path,
                name,
                mime_type,
            } => id.len() + path.len() + name.len() + mime_type.len(),
            MessagePart::Error { message, .. } => message.len(),
            MessagePart::Fork {
                source_chat_id,
                source_title,
                ..
            } => source_chat_id.len() + source_title.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum SessionCommandKind {
    Run,
    Steer,
    Interrupt,
    RespondInput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "camelCase")]
// `Run` is the common command; a command is built once per user action, so its size is moot.
#[allow(clippy::large_enum_variant)]
pub enum SessionCommandPayload {
    #[serde(rename_all = "camelCase")]
    Run {
        request: RunRequest,
        /// Client-minted message id for the optimistic user entry (dedup key).
        message_id: String,
    },
    #[serde(rename_all = "camelCase")]
    Steer {
        prompt: String,
        message_id: Option<String>,
    },
    Interrupt {},
    #[serde(rename_all = "camelCase")]
    RespondInput {
        request_id: String,
        answers: Vec<UserInputAnswer>,
    },
}

impl SessionCommandPayload {
    pub fn kind(&self) -> SessionCommandKind {
        match self {
            SessionCommandPayload::Run { .. } => SessionCommandKind::Run,
            SessionCommandPayload::Steer { .. } => SessionCommandKind::Steer,
            SessionCommandPayload::Interrupt {} => SessionCommandKind::Interrupt,
            SessionCommandPayload::RespondInput { .. } => SessionCommandKind::RespondInput,
        }
    }
}

/// One unsent message waiting its turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueuedMessage {
    /// Stable through promotion: the host uses this as the transcript user
    /// message id when the row is finally dispatched.
    pub id: String,
    /// What the user typed. Never empty — emptying it deletes the row.
    pub text: String,
    /// Committed upload paths, staged at queue time so the row never points at
    /// files that only exist on the device that typed it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
    /// Do not automatically steer this row into a live turn. The row remains
    /// visible until turn end or an explicit Steer now / Send now action.
    #[serde(default, skip_serializing_if = "is_false")]
    pub hold_for_turn_end: bool,
    /// Device that queued it.
    pub issued_by: String,
    /// Epoch millis.
    pub issued_at: i64,
    /// Epoch millis of the last text edit, when there has been one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<i64>,
    /// Host-authoritative barrier preventing this row from reaching the
    /// agent while a client has an edit open. Expired edits fail closed into
    /// `ReviewRequired`; they never silently become sendable again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_gate: Option<QueueDeliveryGate>,
}

/// Why a queued row is not currently eligible for automatic or explicit
/// delivery. Kept on the row so moves preserve it and deleting the row cannot
/// leave an orphaned lease behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum QueueDeliveryGate {
    Editing {
        lease_id: String,
        owner_device_id: String,
        owner_instance_id: String,
        acquired_at_ms: i64,
        expires_at_ms: i64,
        base_text_hash: String,
    },
    ReviewRequired {
        previous_lease_id: String,
        owner_device_id: String,
        since_ms: i64,
        base_text_hash: String,
    },
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl QueuedMessage {
    pub fn new(
        id: impl Into<String>,
        text: impl Into<String>,
        issued_by: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            attachments: Vec::new(),
            hold_for_turn_end: false,
            issued_by: issued_by.into(),
            issued_at: 0,
            edited_at: None,
            delivery_gate: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "outcome",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
/// `BeginQueuedMessageEdit`'s reply.
pub enum BeginQueueEditOutcome {
    Acquired {
        lease_id: String,
        text: String,
        attachments: Vec<String>,
        base_text_hash: String,
        expires_at_ms: i64,
    },
    Locked {
        owner_device_id: String,
        expires_at_ms: i64,
    },
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "outcome",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
/// `RenewQueuedMessageEdit`'s reply.
pub enum RenewQueueEditOutcome {
    Renewed { expires_at_ms: i64 },
    Lost,
    Missing,
}

/// What `FinishQueuedMessageEdit` does with an acquired edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum FinishQueuedMessageEditAction {
    Commit,
    Cancel,
    Discard,
    ReleaseUnchanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(
    tag = "outcome",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
/// `FinishQueuedMessageEdit`'s reply.
pub enum FinishQueueEditOutcome {
    Committed,
    Cancelled,
    Discarded,
    Released,
    Conflict { current_text: String },
    Lost,
    Missing,
}
