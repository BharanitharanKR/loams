//! Studio's notifications (design §53 §6.5 G19; plan SD1 Task 14).
//!
//! [`Notifications`] keeps the last [`BUFFER`] notifications in memory and
//! sends each new one to every watcher. A watcher first receives what the
//! buffer holds, oldest first, then the live ones, with no gap and no
//! duplicate between the two (the snapshot and the subscription are taken
//! under one lock).
//!
//! The engine's own turns feed it through [`TurnNotifier`], a
//! [`TurnObserver`]: a turn that completes is `turn_done`, a turn that fails is
//! `error`, and a question from the agent is `question`. An interrupted turn
//! is the person's own doing and says nothing. Approvals (SD1 Task 13) and run
//! groups (SD1 Task 12) call [`Notifications::push`] with their kinds.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::StreamExt;
use futures::stream::BoxStream;
use loams_agentd_proto::notify::{BUFFER, Notification, NotificationKind};
use loams_agentd_proto::{AgentEvent, DoneStatus};
use tokio::sync::broadcast;

use crate::sessions::TurnObserver;

/// The longest title, in characters; a thread's title is cut to fit.
const MAX_TITLE: usize = 160;

/// The daemon's notifications: a bounded buffer and a live feed.
#[derive(Clone)]
pub struct Notifications {
    inner: Arc<Inner>,
}

struct Inner {
    buffer: Mutex<VecDeque<Notification>>,
    live: broadcast::Sender<Notification>,
}

impl std::fmt::Debug for Notifications {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifications").finish_non_exhaustive()
    }
}

impl Default for Notifications {
    fn default() -> Self {
        Self::new()
    }
}

impl Notifications {
    pub fn new() -> Self {
        let (live, _) = broadcast::channel(BUFFER);
        Self {
            inner: Arc::new(Inner {
                buffer: Mutex::new(VecDeque::with_capacity(BUFFER)),
                live,
            }),
        }
    }

    fn buffer(&self) -> MutexGuard<'_, VecDeque<Notification>> {
        self.inner
            .buffer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Post a notification about a thread (`chat_id`) or a run group
    /// (`group_id`). The oldest is dropped once [`BUFFER`] are held.
    pub fn push(
        &self,
        kind: NotificationKind,
        chat_id: Option<String>,
        group_id: Option<String>,
        title: impl Into<String>,
    ) -> Notification {
        let notification = Notification {
            id: uuid::Uuid::new_v4().to_string(),
            kind,
            chat_id,
            group_id,
            title: clip(&title.into()),
            created_at_ms: crate::now_ms(),
        };
        let mut buffer = self.buffer();
        if buffer.len() == BUFFER {
            buffer.pop_front();
        }
        buffer.push_back(notification.clone());
        // Sent under the lock, so a watcher's snapshot and its feed meet
        // exactly. No receiver is fine.
        let _ = self.inner.live.send(notification.clone());
        notification
    }

    /// What the buffer holds, oldest first.
    pub fn recent(&self) -> Vec<Notification> {
        self.buffer().iter().cloned().collect()
    }

    /// The buffer, then every new notification. A watcher that falls more
    /// than [`BUFFER`] behind skips what it missed rather than ending.
    pub fn watch(&self) -> BoxStream<'static, Notification> {
        let (snapshot, receiver) = {
            let buffer = self.buffer();
            (
                buffer.iter().cloned().collect::<Vec<_>>(),
                self.inner.live.subscribe(),
            )
        };
        let live = futures::stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(notification) => return Some((notification, receiver)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        futures::stream::iter(snapshot).chain(live).boxed()
    }
}

fn clip(title: &str) -> String {
    let title = title.trim();
    if title.chars().count() <= MAX_TITLE {
        return title.to_owned();
    }
    let mut cut: String = title.chars().take(MAX_TITLE - 1).collect();
    cut.push('…');
    cut
}

/// A thread's title for a notification, from its chat row.
pub type ThreadTitles = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Turns the engine's turns into notifications.
pub struct TurnNotifier {
    notifications: Notifications,
    titles: ThreadTitles,
}

impl std::fmt::Debug for TurnNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnNotifier").finish_non_exhaustive()
    }
}

impl TurnNotifier {
    pub fn new(notifications: Notifications, titles: ThreadTitles) -> Self {
        Self {
            notifications,
            titles,
        }
    }

    /// The title is the thread's: the kind already says what happened (the
    /// desktop's OS notification puts the kind in its headline, SD1 Task 28).
    fn post(&self, kind: NotificationKind, chat_id: &str) {
        let thread = (self.titles)(chat_id)
            .map(|t| t.trim().to_owned())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| "New thread".to_owned());
        self.notifications
            .push(kind, Some(chat_id.to_owned()), None, thread);
    }
}

impl TurnObserver for TurnNotifier {
    fn turn_start(&self, _chat_id: &str, _message_id: &str) {}

    fn event(&self, chat_id: &str, event: &AgentEvent) {
        // A subagent's events (`AgentEvent::Subagent`) belong to its parent
        // turn, which notifies once for the whole.
        match event {
            AgentEvent::Done {
                status: DoneStatus::Completed,
                ..
            } => self.post(NotificationKind::TurnDone, chat_id),
            AgentEvent::Done {
                status: DoneStatus::Errored,
                ..
            } => self.post(NotificationKind::Error, chat_id),
            AgentEvent::InputRequested { .. } => {
                self.post(NotificationKind::Question, chat_id);
            }
            _ => {}
        }
    }

    fn turn_end(&self, _chat_id: &str) {}
}
