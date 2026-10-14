//! Supervised approvals, the engine's half (design §53 §8.1, §6.5 G17, G18,
//! G22, D911; plan SD1 Task 13, on DD1 Task 24's contract).
//!
//! - **Modes.** Each thread has an approval mode, set with
//!   `SetThreadApprovalMode` and kept in the store. A thread without one
//!   takes its parent's (a side chat is supervised when the thread that
//!   spawned it is), else the daemon's default: `full-access` (the behaviour
//!   before Task 13) on the desktop, `supervised` with `serve --remote`.
//! - **The bridge.** A supervised thread's run gets
//!   [`loams_agentd_harness::RunControls::request_approval`]. Each ask is
//!   redacted ([`loams_agentd_harness::redact::redact_json`]), stored, shown
//!   in the run's events as `ApprovalRequested`, posted as an `approval`
//!   notification, and listed by `WatchApprovals` until a person answers it
//!   with `ResolveApproval` (roles `owner` and `studio`, never `mcp`), until
//!   it times out (a denial), or until its turn ends (a denial).
//! - **"Always"** is remembered per thread and tool, in the store; a later
//!   ask of that tool in that thread is allowed without asking.
//! - **Restarts.** The store keeps what is pending, so it is still listed
//!   after the daemon restarts. Its run is gone by then: answering it clears
//!   it (and an "always" is still remembered), and when the revived turn asks
//!   the same thing again, the new ask takes the old one's place.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use loams_agentd_harness::{ApprovalAsk, RequestApproval};
use loams_agentd_proto::AgentEvent;
use loams_agentd_proto::approvals::{
    APPROVAL_NOT_PENDING, Approval, ApprovalDecision, ApprovalList, ApprovalMode, ToolRisk,
};
use loams_agentd_proto::notify::NotificationKind;
use loams_agentd_store::{DocsStore, PendingApprovalRow};
use tokio::sync::{mpsc, oneshot, watch};

use crate::EngineError;
use crate::notify::{Notifications, ThreadTitles};
use crate::sessions::TurnObserver;

/// How long an approval waits for a person before it is denied.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// How many parents a thread's mode is looked up through.
const MAX_PARENTS: usize = 16;

/// The daemon's approval settings (`config.toml` `[approvals]`, and
/// `serve --remote`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalSettings {
    /// The mode of a thread that has none of its own and no parent's.
    pub default_mode: ApprovalMode,
    /// How long an approval waits before it is denied.
    pub timeout: Duration,
}

impl Default for ApprovalSettings {
    fn default() -> Self {
        Self {
            default_mode: ApprovalMode::FullAccess,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

/// A thread's parent, for the mode of a side chat.
pub type ParentOf = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Why `ResolveApproval` was refused.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("{APPROVAL_NOT_PENDING}: no approval {0} is pending")]
    NotPending(String),
    #[error("several threads have an approval {0} pending: name the chatId")]
    Ambiguous(String),
    #[error(transparent)]
    Engine(#[from] EngineError),
}

/// The engine's approvals: modes, the pending list and "always".
#[derive(Clone)]
pub struct Approvals {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Approvals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Approvals").finish_non_exhaustive()
    }
}

struct Inner {
    store: Arc<DocsStore>,
    settings: Mutex<ApprovalSettings>,
    state: Mutex<State>,
    /// Bumped on every change of the pending list or a mode.
    changed: watch::Sender<u64>,
    notifications: OnceLock<(Notifications, ThreadTitles)>,
    parent_of: OnceLock<ParentOf>,
    /// Set at shutdown: a turn that ends then keeps its approvals, so they
    /// are listed again after the restart.
    frozen: AtomicBool,
    tokens: AtomicU64,
}

#[derive(Default)]
struct State {
    /// Oldest first.
    pending: Vec<Entry>,
    modes: HashMap<String, ApprovalMode>,
    always: HashSet<(String, String)>,
    /// Each thread's turn in progress (its user message id).
    turns: HashMap<String, String>,
}

struct Entry {
    approval: Approval,
    /// The run waiting for the answer; `None` after a restart.
    waiter: Option<Waiter>,
    /// Matches the entry's timer to the entry.
    token: u64,
}

struct Waiter {
    answer: oneshot::Sender<ApprovalDecision>,
    events: mpsc::UnboundedSender<AgentEvent>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn wire_name<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

impl Approvals {
    /// Load the modes, "always" decisions and pending approvals the store
    /// holds. Pending ones wait for a person again, with what is left of
    /// their time (in a Tokio runtime).
    pub fn open(store: Arc<DocsStore>, settings: ApprovalSettings) -> Result<Self, EngineError> {
        let mut state = State::default();
        for (chat_id, mode) in store.approval_modes()? {
            if let Some(mode) = ApprovalMode::parse(&mode) {
                state.modes.insert(chat_id, mode);
            }
        }
        state.always = store.approval_always()?.into_iter().collect();
        let (changed, _) = watch::channel(0);
        let me = Self {
            inner: Arc::new(Inner {
                store,
                settings: Mutex::new(settings),
                state: Mutex::new(State::default()),
                changed,
                notifications: OnceLock::new(),
                parent_of: OnceLock::new(),
                frozen: AtomicBool::new(false),
                tokens: AtomicU64::new(0),
            }),
        };
        let mut timers = Vec::new();
        for row in me.inner.store.pending_approvals()? {
            let Ok(risk) = serde_json::from_value::<ToolRisk>(serde_json::Value::String(row.risk))
            else {
                continue;
            };
            let token = me.inner.tokens.fetch_add(1, Ordering::Relaxed);
            timers.push((
                row.chat_id.clone(),
                row.call_id.clone(),
                token,
                row.created_at,
            ));
            state.pending.push(Entry {
                approval: Approval {
                    id: row.call_id,
                    chat_id: row.chat_id,
                    turn_id: row.turn_id,
                    tool: row.tool,
                    args: serde_json::from_str(&row.args).unwrap_or(serde_json::Value::Null),
                    risk,
                    created_at_ms: row.created_at,
                },
                waiter: None,
                token,
            });
        }
        *lock(&me.inner.state) = state;
        let timeout = settings.timeout;
        for (chat_id, call_id, token, created_at) in timers {
            let waited = u64::try_from(crate::now_ms() - created_at).unwrap_or(0);
            let left = timeout.saturating_sub(Duration::from_millis(waited));
            me.arm_timer(chat_id, call_id, token, left);
        }
        Ok(me)
    }

    /// Change the settings (`config.toml`, `serve --remote`).
    pub fn set_settings(&self, settings: ApprovalSettings) {
        *lock(&self.inner.settings) = settings;
        self.bump();
    }

    pub fn settings(&self) -> ApprovalSettings {
        *lock(&self.inner.settings)
    }

    /// Post `approval` notifications, titled with the thread's title.
    pub fn set_notifications(&self, notifications: Notifications, titles: ThreadTitles) {
        let _ = self.inner.notifications.set((notifications, titles));
    }

    /// Look up a thread's parent, for the mode of a side chat.
    pub fn set_parent_of(&self, parent_of: ParentOf) {
        let _ = self.inner.parent_of.set(parent_of);
    }

    /// Keep what is pending when turns end from now on (the engine is
    /// shutting down; the approvals are listed again after the restart).
    pub fn freeze(&self) {
        self.inner.frozen.store(true, Ordering::SeqCst);
    }

    /// A thread's effective mode, and whether it was set (on the thread or a
    /// parent) rather than the daemon's default.
    pub fn resolved_mode(&self, chat_id: &str) -> (ApprovalMode, bool) {
        let parent_of = self.inner.parent_of.get();
        let mut chat = chat_id.to_owned();
        for _ in 0..MAX_PARENTS {
            if let Some(mode) = lock(&self.inner.state).modes.get(&chat) {
                return (*mode, true);
            }
            match parent_of.and_then(|parent_of| parent_of(&chat)) {
                Some(parent) if parent != chat => chat = parent,
                _ => return (self.settings().default_mode, false),
            }
        }
        // A chain deeper than the walk (side chats of side chats, each made
        // by a run's MCP shim) may hang under a supervised thread: fail safe
        // rather than fall back to a full-access default (SD1 Task 30).
        (ApprovalMode::Supervised, false)
    }

    /// A thread's effective mode.
    pub fn mode(&self, chat_id: &str) -> ApprovalMode {
        self.resolved_mode(chat_id).0
    }

    /// Set a thread's mode (`SetThreadApprovalMode`, G22).
    pub fn set_mode(&self, chat_id: &str, mode: ApprovalMode) -> Result<(), EngineError> {
        self.inner.store.set_approval_mode(chat_id, mode.as_str())?;
        lock(&self.inner.state)
            .modes
            .insert(chat_id.to_owned(), mode);
        self.bump();
        Ok(())
    }

    /// The approval bridge of one supervised run of `chat_id`. Asks go to
    /// the person; the run's `ApprovalRequested` and `ApprovalResolved` go
    /// to `events` (the run's own event pipeline).
    pub fn bridge(
        &self,
        chat_id: &str,
        events: mpsc::UnboundedSender<AgentEvent>,
    ) -> RequestApproval {
        let me = self.clone();
        let chat_id = chat_id.to_owned();
        Box::new(move |ask: ApprovalAsk| me.ask(&chat_id, &events, ask))
    }

    fn ask(
        &self,
        chat_id: &str,
        events: &mpsc::UnboundedSender<AgentEvent>,
        ask: ApprovalAsk,
    ) -> oneshot::Receiver<ApprovalDecision> {
        let (answer, receiver) = oneshot::channel();
        let args = loams_agentd_harness::redact::redact_json(&ask.args);
        let mut state = lock(&self.inner.state);
        if state
            .always
            .contains(&(chat_id.to_owned(), ask.tool.clone()))
        {
            let _ = answer.send(ApprovalDecision::Always);
            return receiver;
        }
        let token = self.inner.tokens.fetch_add(1, Ordering::Relaxed);
        let waiter = Waiter {
            answer,
            events: events.clone(),
        };
        // A revived turn asking what its predecessor asked takes its place.
        let adopted = state.pending.iter_mut().find(|e| {
            e.waiter.is_none()
                && e.approval.chat_id == chat_id
                && e.approval.tool == ask.tool
                && e.approval.args == args
        });
        let approval = if let Some(entry) = adopted {
            entry.waiter = Some(waiter);
            entry.token = token;
            entry.approval.clone()
        } else {
            // The same call asked twice: the first ask is denied.
            if let Some(at) = state
                .pending
                .iter()
                .position(|e| e.approval.chat_id == chat_id && e.approval.id == ask.call_id)
            {
                let old = state.pending.remove(at);
                if let Some(waiter) = old.waiter {
                    let _ = waiter.answer.send(ApprovalDecision::Deny);
                }
            }
            let approval = Approval {
                id: ask.call_id,
                chat_id: chat_id.to_owned(),
                turn_id: state.turns.get(chat_id).cloned(),
                tool: ask.tool,
                args,
                risk: ask.risk,
                created_at_ms: crate::now_ms(),
            };
            if let Err(error) = self.inner.store.put_pending_approval(&PendingApprovalRow {
                chat_id: approval.chat_id.clone(),
                call_id: approval.id.clone(),
                turn_id: approval.turn_id.clone(),
                tool: approval.tool.clone(),
                args: approval.args.to_string(),
                risk: wire_name(&approval.risk),
                created_at: approval.created_at_ms,
            }) {
                tracing::warn!(chat = %chat_id, %error, "the pending approval was not stored");
            }
            state.pending.push(Entry {
                approval: approval.clone(),
                waiter: Some(waiter),
                token,
            });
            approval
        };
        drop(state);
        let _ = events.send(AgentEvent::ApprovalRequested {
            call_id: approval.id.clone(),
            tool: approval.tool.clone(),
            args: approval.args.clone(),
            risk: approval.risk,
        });
        if let Some((notifications, titles)) = self.inner.notifications.get() {
            let title = titles(chat_id)
                .map(|t| t.trim().to_owned())
                .filter(|t| !t.is_empty())
                .unwrap_or_else(|| "New thread".to_owned());
            notifications.push(
                NotificationKind::Approval,
                Some(chat_id.to_owned()),
                None,
                title,
            );
        }
        self.arm_timer(
            chat_id.to_owned(),
            approval.id,
            token,
            self.settings().timeout,
        );
        self.bump();
        receiver
    }

    /// Deny the approval when it is still pending under `token` after `after`.
    fn arm_timer(&self, chat_id: String, call_id: String, token: u64, after: Duration) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let weak: Weak<Inner> = Arc::downgrade(&self.inner);
        runtime.spawn(async move {
            tokio::time::sleep(after).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let me = Approvals { inner };
            let current = lock(&me.inner.state).pending.iter().any(|e| {
                e.token == token && e.approval.chat_id == chat_id && e.approval.id == call_id
            });
            if current {
                tracing::info!(chat = %chat_id, call = %call_id, "an approval timed out: denied");
                let _ = me.settle(Some(&chat_id), &call_id, ApprovalDecision::Deny);
            }
        });
    }

    /// Answer a pending approval (`ResolveApproval`, G18).
    pub fn resolve(
        &self,
        chat_id: Option<&str>,
        call_id: &str,
        decision: ApprovalDecision,
    ) -> Result<(), ResolveError> {
        self.settle(chat_id, call_id, decision)
    }

    fn settle(
        &self,
        chat_id: Option<&str>,
        call_id: &str,
        decision: ApprovalDecision,
    ) -> Result<(), ResolveError> {
        let entry = {
            let mut state = lock(&self.inner.state);
            let matches: Vec<usize> = state
                .pending
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    e.approval.id == call_id && chat_id.is_none_or(|c| e.approval.chat_id == c)
                })
                .map(|(at, _)| at)
                .collect();
            let at = match matches.as_slice() {
                [] => return Err(ResolveError::NotPending(call_id.to_owned())),
                [at] => *at,
                _ => return Err(ResolveError::Ambiguous(call_id.to_owned())),
            };
            let entry = state.pending.remove(at);
            if decision == ApprovalDecision::Always {
                state
                    .always
                    .insert((entry.approval.chat_id.clone(), entry.approval.tool.clone()));
            }
            entry
        };
        let chat = &entry.approval.chat_id;
        if let Err(error) = self.inner.store.delete_pending_approval(chat, call_id) {
            tracing::warn!(%chat, %error, "the answered approval was not removed");
        }
        if decision == ApprovalDecision::Always {
            self.inner
                .store
                .add_approval_always(chat, &entry.approval.tool)
                .map_err(EngineError::from)?;
        }
        if let Some(waiter) = entry.waiter {
            let _ = waiter.events.send(AgentEvent::ApprovalResolved {
                call_id: call_id.to_owned(),
                decision,
            });
            let _ = waiter.answer.send(decision);
        }
        self.bump();
        Ok(())
    }

    /// Whether `call_id` of `chat_id` waits on a live run (the engine drops
    /// an `ApprovalRequested` that no ask of its own announced).
    pub fn is_waiting(&self, chat_id: &str, call_id: &str) -> bool {
        lock(&self.inner.state).pending.iter().any(|e| {
            e.waiter.is_some() && e.approval.chat_id == chat_id && e.approval.id == call_id
        })
    }

    /// Deny what `chat_id`'s live run waits for (an interrupt, a turn that
    /// ended, a request after the turn). Unless the engine is shutting down,
    /// the thread's approvals are forgotten too: their turn is over.
    pub fn deny_chat(&self, chat_id: &str) {
        let frozen = self.inner.frozen.load(Ordering::SeqCst);
        let mut waiters = Vec::new();
        let mut removed = Vec::new();
        {
            let mut state = lock(&self.inner.state);
            let mut kept = Vec::with_capacity(state.pending.len());
            for mut entry in std::mem::take(&mut state.pending) {
                if entry.approval.chat_id != chat_id {
                    kept.push(entry);
                    continue;
                }
                if let Some(waiter) = entry.waiter.take() {
                    waiters.push((entry.approval.id.clone(), waiter));
                }
                if frozen {
                    kept.push(entry);
                } else {
                    removed.push(entry.approval.id);
                }
            }
            state.pending = kept;
        }
        if waiters.is_empty() && removed.is_empty() {
            return;
        }
        for call_id in &removed {
            if let Err(error) = self.inner.store.delete_pending_approval(chat_id, call_id) {
                tracing::warn!(chat = %chat_id, %error, "an ended turn's approval was not removed");
            }
        }
        for (call_id, waiter) in waiters {
            if !frozen {
                let _ = waiter.events.send(AgentEvent::ApprovalResolved {
                    call_id,
                    decision: ApprovalDecision::Deny,
                });
            }
            let _ = waiter.answer.send(ApprovalDecision::Deny);
        }
        self.bump();
    }

    /// Deny one approval whose request arrived when no turn was running.
    pub fn deny(&self, chat_id: &str, call_id: &str) {
        let _ = self.settle(Some(chat_id), call_id, ApprovalDecision::Deny);
    }

    /// Forget a deleted thread's mode, "always" and approvals.
    pub fn forget_chat(&self, chat_id: &str) {
        self.deny_chat(chat_id);
        {
            let mut state = lock(&self.inner.state);
            state.pending.retain(|e| e.approval.chat_id != chat_id);
            state.modes.remove(chat_id);
            state.always.retain(|(chat, _)| chat != chat_id);
            state.turns.remove(chat_id);
        }
        if let Err(error) = self.inner.store.delete_chat_approvals(chat_id) {
            tracing::warn!(chat = %chat_id, %error, "a deleted thread's approvals were not removed");
        }
        self.bump();
    }

    /// The pending approvals (of `chat_id` only, with its mode, when given).
    pub fn snapshot(&self, chat_id: Option<&str>) -> ApprovalList {
        let pending = lock(&self.inner.state)
            .pending
            .iter()
            .filter(|e| chat_id.is_none_or(|c| e.approval.chat_id == c))
            .map(|e| e.approval.clone())
            .collect();
        ApprovalList {
            pending,
            mode: chat_id.map(|c| self.mode(c)),
        }
    }

    /// The pending approvals now, then again on each change (`WatchApprovals`).
    pub fn watch(&self, chat_id: Option<String>) -> BoxStream<'static, ApprovalList> {
        let receiver = self.inner.changed.subscribe();
        let me = self.clone();
        futures::stream::unfold(
            (receiver, None::<ApprovalList>, true),
            move |(mut receiver, last, first)| {
                let me = me.clone();
                let chat_id = chat_id.clone();
                async move {
                    loop {
                        if !first || last.is_some() {
                            receiver.changed().await.ok()?;
                        }
                        receiver.borrow_and_update();
                        let list = me.snapshot(chat_id.as_deref());
                        if last.as_ref() != Some(&list) {
                            return Some((list.clone(), (receiver, Some(list), false)));
                        }
                    }
                }
            },
        )
        .boxed()
    }

    fn bump(&self) {
        self.inner.changed.send_modify(|version| *version += 1);
    }
}

/// The turn a thread is in, for an approval's `turnId`; and a turn that
/// ends denies (and forgets) what it still waits for.
impl TurnObserver for Approvals {
    fn turn_start(&self, chat_id: &str, message_id: &str) {
        lock(&self.inner.state)
            .turns
            .insert(chat_id.to_owned(), message_id.to_owned());
    }

    fn event(&self, _chat_id: &str, _event: &AgentEvent) {}

    fn turn_end(&self, chat_id: &str) {
        self.deny_chat(chat_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SD1 Task 30: a side chat deeper than the walk, under a supervised
    /// thread, is supervised, not the full-access default; a chain that ends
    /// with no mode still takes the default.
    #[test]
    fn deep_side_chats_fail_safe_to_supervised() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let approvals = Approvals::open(store, ApprovalSettings::default()).unwrap();
        // c0 <- c1 <- … <- c40: each chat's parent is the one before it.
        approvals.set_parent_of(Arc::new(|chat: &str| {
            let n: usize = chat.strip_prefix('c')?.parse().ok()?;
            (n > 0).then(|| format!("c{}", n - 1))
        }));
        assert_eq!(approvals.mode("c10"), ApprovalMode::FullAccess);
        approvals.set_mode("c0", ApprovalMode::Supervised).unwrap();
        assert_eq!(approvals.mode("c3"), ApprovalMode::Supervised);
        assert_eq!(
            approvals.resolved_mode("c40"),
            (ApprovalMode::Supervised, false),
            "a chain deeper than the walk must not fall back to full access"
        );
        assert_eq!(approvals.mode("other"), ApprovalMode::FullAccess);
    }
}
