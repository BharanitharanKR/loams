//! Studio's notifications (design §53 §6.5 G19; plan SD1 Task 14).
//!
//! The daemon tells its clients when something needs the person: a turn
//! ended, an approval or a question is waiting, a turn failed, a run group is
//! ready, or a group's check failed. `WatchNotifications` sends the last
//! [`BUFFER`] notifications the daemon holds in memory, oldest first, then
//! each new one as it happens. A client tells them apart by `id`.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The daemon feature these methods add to `Hello` and `GetHost`.
pub const FEATURE: &str = "notifications";

/// How many notifications the daemon keeps in memory, for a client that
/// connects later. Older ones are dropped; nothing is persisted.
pub const BUFFER: usize = 200;

/// What a notification is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    /// A thread's turn finished.
    TurnDone,
    /// A tool call waits for the person's approval (supervised mode).
    Approval,
    /// The agent asked the person a question.
    Question,
    /// A thread's turn ended with an error.
    Error,
    /// Every run of a run group has finished.
    GroupReady,
    /// A run group's check failed.
    CheckFailed,
}

/// One notification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Notification {
    /// Unique within the daemon's run.
    pub id: String,
    pub kind: NotificationKind,
    /// The thread it is about, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_id: Option<String>,
    /// The run group it is about, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// One line for the person: the thread's title for a thread (the kind says
    /// what happened), else what the notification is about.
    pub title: String,
    pub created_at_ms: i64,
}
