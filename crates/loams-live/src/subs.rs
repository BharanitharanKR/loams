//! [`Subscriptions`], the subscription manager of one app on one node
//! (design §20 §8.2–§8.3; R1 plan Task 11).
//!
//! One task per app owns every subscription, the [`ReadSetIndex`] of their
//! read sets and a journal [`Tailer`]. It **ticks** on a TSO timestamp `T`:
//! right after a commit of the node's [`Runner`] (or [`Subscriptions::wake`]),
//! otherwise polling with backoff from `poll_min` to `poll_max`. A tick
//! reads the journal up to `T` (over several bounded batches when a backlog
//! is large; subscriptions are evaluated at `T` only after the batch that
//! reaches the heads visible at `T`), stabs every write's document key and
//! removed and added index keys into the index, **reruns** the invalidated
//! subscriptions at `T` (at most `rerun_concurrency` at once), replaces
//! their read sets and publishes one [`Tick`] with the results that
//! changed. Every subscription is then valid at `T` (§20 §8.3).
//!
//! - **Sharing.** Subscriptions with one [`SubKey`] (function and argument
//!   digest) share one entry and one rerun, with a reference count.
//! - **Coalescing.** A tick that would rerun waits until `min_rerun_interval`
//!   after the previous rerun, then reads at a fresh timestamp, so a burst
//!   of writes costs one rerun at the newest tick; intermediate results are
//!   skipped, never reordered (R1 plan row T11-4).
//! - **Errors.** A query that fails holds its error as its result and is
//!   rerun on every tick until it succeeds (it has no read set to match).
//! - **Trimmed journal.** When the janitor trimmed past the tailer's
//!   position ([`LiveError::JournalTrimmed`]), the manager restarts its
//!   tailer at a fresh timestamp and reruns every subscription there.
//! - **Safety rerun.** Every `safety_rerun` (5 min) every subscription is
//!   rerun and compared; a difference is a missed invalidation, which is
//!   logged with both read sets, counted
//!   ([`SubsStats::missed_invalidations`], the `live_missed_invalidation_total`
//!   metric) and repaired.
//! - **Activations** (LV1 plan Task 7). A manager started with
//!   [`Subscriptions::spawn_for`] follows its app's [`AppEngine`]: the
//!   deployment it has *adopted* runs every subscription of a deployed
//!   function, whichever deployment the subscriber resolved against. When
//!   the engine serves another deployment, the next tick reads at or after
//!   the activation's commit, resolves every such subscription again by
//!   name in the new deployment (a function it does not export holds
//!   `NOT_FOUND` until one does), reruns **every** subscription once and
//!   publishes them all, so a session's Transition at that tick carries no
//!   result of the old deployment. While an activation of the engine is in
//!   flight, ticks are held: the tick's timestamp is taken first, so a tick
//!   that sees no activation in flight reads before any later one commits.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt};
use loams_kv::{Ts, TxnError};
use tokio::sync::{Notify, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::deploy::{AppEngine, Serving};
use crate::journal::Tailer;
use crate::readset::{ReadSetIndex, SubId};
use crate::txn::Queried;
use crate::{FnKind, Function, LiveError, LiveValue, ReadSet, Runner};

/// Ticks the [`Subscriptions::updates`] channel buffers per receiver; a
/// receiver that falls further behind gets `Lagged` and must resubscribe.
pub const TICK_CHANNEL: usize = 1024;

/// How often the tailer writes its checkpoint when
/// [`SubsConfig::consumer`] is set.
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

/// How long a node's checkpoint protects the journal after its last write
/// (a crashed node's lapses, §20 §13).
pub const CHECKPOINT_TTL: Duration = Duration::from_secs(120);

/// How far in the past each tick reads by default (owner ruling on row
/// T11-3, row T12-1): commits still in flight at `now` hold locks on the
/// journal heads, and a read at `now` waits for them; a read 50 ms back
/// finds almost all of them finished.
pub const DEFAULT_TICK_READ_LAG: Duration = Duration::from_millis(50);

/// Attempts per query evaluation when the storage fails (a region error,
/// a lost TSO stream); the last failure becomes the result.
const EVAL_ATTEMPTS: u32 = 3;

/// The subscription manager's settings (§20 §8.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsConfig {
    /// The first poll interval after a tick that saw commits (20 ms).
    pub poll_min: Duration,
    /// The longest poll interval when idle (200 ms).
    pub poll_max: Duration,
    /// Reruns at once (16).
    pub rerun_concurrency: usize,
    /// The shortest time between two ticks that rerun (50 ms).
    pub min_rerun_interval: Duration,
    /// How often every subscription is rerun and compared (5 min).
    pub safety_rerun: Duration,
    /// The journal consumer id the tailer checkpoints under, with
    /// [`CHECKPOINT_TTL`], every [`CHECKPOINT_INTERVAL`]; `None` writes no
    /// checkpoint (entries are then kept only by the janitor's retention).
    pub consumer: Option<String>,
    /// How far behind a fresh TSO timestamp each tick reads
    /// ([`DEFAULT_TICK_READ_LAG`]); a local commit is seen by the tick this
    /// long after it. Zero reads at `now`.
    pub tick_read_lag: Duration,
}

impl Default for SubsConfig {
    fn default() -> Self {
        SubsConfig {
            poll_min: Duration::from_millis(20),
            poll_max: Duration::from_millis(200),
            rerun_concurrency: 16,
            min_rerun_interval: Duration::from_millis(50),
            safety_rerun: Duration::from_secs(300),
            consumer: None,
            tick_read_lag: DEFAULT_TICK_READ_LAG,
        }
    }
}

/// A subscription's identity: the deployment that serves the function
/// (LV1 plan Task 6; the nil ULID for system functions), the function and
/// the digest of its arguments, so two deployments never share a result.
/// The caller's identity joins it in R3.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubKey {
    pub deployment: Ulid,
    pub function: String,
    pub args_digest: [u8; 32],
}

impl SubKey {
    /// The key of `function` called with `args` ([`LiveValue::digest`]),
    /// outside any deployment.
    pub fn new(function: &str, args: &LiveValue) -> Self {
        SubKey {
            deployment: Ulid::nil(),
            function: function.to_string(),
            args_digest: args.digest(),
        }
    }

    /// The key of `f`, in the deployment that serves it, called with
    /// `args`.
    pub fn for_function(f: &dyn Function, args: &LiveValue) -> Self {
        SubKey {
            deployment: f.deployment(),
            ..SubKey::new(f.name(), args)
        }
    }
}

/// A subscription's current result, the tick it was computed at, and the
/// deployment whose code computed it (nil for system functions). It stays
/// the result at every later tick until a [`Tick`] replaces it.
#[derive(Debug, Clone, PartialEq)]
pub struct SubResult {
    pub result: Result<LiveValue, LiveError>,
    pub ts: Ts,
    pub deployment: Ulid,
}

/// What a subscription runs.
#[derive(Clone)]
pub enum Target {
    /// A query function.
    Function(Arc<dyn Function>),
    /// A function name the app does not serve now, and the error resolving
    /// it gave. A manager following an engine resolves it again at every
    /// activation; otherwise it holds the error.
    Missing { name: String, error: LiveError },
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Function(func) => f.debug_tuple("Function").field(&func.name()).finish(),
            Target::Missing { name, error } => f
                .debug_struct("Missing")
                .field("name", name)
                .field("error", error)
                .finish(),
        }
    }
}

/// One completed tick: every subscription is valid at `at`, and `changed`
/// holds the ones whose result changed since the previous tick.
#[derive(Debug, Clone)]
pub struct Tick {
    pub at: Ts,
    pub changed: Vec<(SubId, Arc<SubResult>)>,
    /// The journal was trimmed past the tailer, and every subscription was
    /// rerun at `at`.
    pub resynced: bool,
}

/// Counters of one manager.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubsStats {
    /// Completed ticks.
    pub ticks: u64,
    /// Journal entries read.
    pub entries: u64,
    /// Query evaluations after the first (invalidations, errors, resyncs
    /// and safety reruns).
    pub reruns: u64,
    /// Restarts after a trimmed journal.
    pub resyncs: u64,
    /// Safety passes.
    pub safety_passes: u64,
    /// Safety reruns whose result differed: missed invalidations
    /// (`live_missed_invalidation_total`; each one is a bug).
    pub missed_invalidations: u64,
    /// Ticks that failed (storage errors); the next tick retries.
    pub tick_errors: u64,
    /// Journal batches acknowledged without matching their writes, through
    /// the [`Subscriptions::drop_next_batch`] test hook.
    pub dropped_batches: u64,
    /// Live subscriptions.
    pub subscriptions: u64,
}

#[derive(Debug, Default)]
struct Counters {
    ticks: AtomicU64,
    entries: AtomicU64,
    reruns: AtomicU64,
    resyncs: AtomicU64,
    safety_passes: AtomicU64,
    missed: AtomicU64,
    tick_errors: AtomicU64,
    dropped_batches: AtomicU64,
    subscriptions: AtomicU64,
}

#[derive(Debug, Default)]
struct Shared {
    counters: Counters,
    wake: Notify,
    drop_next_batch: AtomicBool,
}

type Reply = oneshot::Sender<Result<(SubId, Arc<SubResult>), LiveError>>;

enum Cmd {
    Subscribe {
        key: SubKey,
        target: Target,
        args: LiveValue,
        reply: Reply,
    },
    Unsubscribe(SubId),
}

/// The subscriptions of one app on this node, and the task that keeps them
/// current. Dropping it (or cancelling its token) stops the task.
pub struct Subscriptions {
    cmds: mpsc::UnboundedSender<Cmd>,
    updates: broadcast::Sender<Tick>,
    current: watch::Receiver<Option<Ts>>,
    shared: Arc<Shared>,
    follows: bool,
}

impl fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriptions")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Subscriptions {
    /// Starts the manager of `runner`'s app, until `shutdown` is cancelled
    /// or the returned handle drops. Must be called inside a Tokio runtime.
    pub fn spawn(runner: Runner, config: SubsConfig, shutdown: CancellationToken) -> Self {
        Self::start(runner, config, None, shutdown)
    }

    /// Starts the manager of `app`'s subscriptions, following what its
    /// engine serves (LV1 plan Task 7; the module documentation has the
    /// rules), until `shutdown` is cancelled or the handle drops.
    pub fn spawn_for(app: &AppEngine, config: SubsConfig, shutdown: CancellationToken) -> Self {
        Self::start(app.runner().clone(), config, Some(app.serving()), shutdown)
    }

    fn start(
        runner: Runner,
        config: SubsConfig,
        follow: Option<watch::Receiver<Serving>>,
        shutdown: CancellationToken,
    ) -> Self {
        let follows = follow.is_some();
        let serving = follow
            .as_ref()
            .map(|f| f.borrow().clone())
            .unwrap_or_default();
        let (cmds, rx) = mpsc::unbounded_channel();
        let (updates, _) = broadcast::channel(TICK_CHANNEL);
        let (current_tx, current) = watch::channel(None);
        let shared = Arc::new(Shared::default());
        let manager = Manager {
            commits: runner.commits(),
            follow,
            follow_open: follows,
            serving,
            index: ReadSetIndex::new(runner.app().clone()),
            runner,
            config,
            shutdown,
            cmds: rx,
            updates: updates.clone(),
            current: current_tx,
            shared: shared.clone(),
            tailer: None,
            at: None,
            valid: false,
            subs: HashMap::new(),
            by_key: HashMap::new(),
            next_id: 1,
            pending: HashSet::new(),
            waiting: Vec::new(),
            last_rerun: None,
            last_safety: Instant::now(),
            last_checkpoint: None,
        };
        tokio::spawn(manager.run());
        Subscriptions {
            cmds,
            updates,
            current,
            shared,
            follows,
        }
    }

    /// Whether the manager follows an engine ([`spawn_for`](Self::spawn_for)):
    /// subscribing a [`Target::Missing`] is then worth it.
    pub fn follows(&self) -> bool {
        self.follows
    }

    /// Subscribes to query `f` with `args` under `key`: evaluates it at the
    /// manager's current tick (or joins the subscription already holding
    /// `key`), and returns its id and result. Later results arrive in
    /// [`updates`](Self::updates). A query that fails is still subscribed,
    /// with its error as the result.
    pub async fn subscribe(
        &self,
        key: SubKey,
        f: Arc<dyn Function>,
        args: LiveValue,
    ) -> Result<(SubId, Arc<SubResult>), LiveError> {
        self.subscribe_target(key, Target::Function(f), args).await
    }

    /// [`subscribe`](Self::subscribe) to `target`: a function, or a name
    /// that does not resolve now, which holds its error as the result (a
    /// manager following an engine resolves it again at every activation).
    /// A manager following an engine runs a deployed function in the
    /// deployment it has adopted, and keys the subscription by it.
    pub async fn subscribe_target(
        &self,
        key: SubKey,
        target: Target,
        args: LiveValue,
    ) -> Result<(SubId, Arc<SubResult>), LiveError> {
        if let Target::Function(f) = &target {
            check_query(&**f)?;
        }
        let (reply, answer) = oneshot::channel();
        self.cmds
            .send(Cmd::Subscribe {
                key,
                target,
                args,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// Drops one reference to subscription `id`; the last one removes it.
    pub fn unsubscribe(&self, id: SubId) {
        let _ = self.cmds.send(Cmd::Unsubscribe(id));
    }

    /// The completed ticks, from now on.
    pub fn updates(&self) -> broadcast::Receiver<Tick> {
        self.updates.subscribe()
    }

    /// The latest completed tick, or `None` before the first.
    pub fn current(&self) -> Option<Ts> {
        *self.current.borrow()
    }

    /// Ticks now, as a local commit does (Task 12's sessions, and a commit
    /// made through another runner).
    pub fn wake(&self) {
        self.shared.wake.notify_one();
    }

    /// The manager's counters.
    pub fn stats(&self) -> SubsStats {
        let c = &self.shared.counters;
        SubsStats {
            ticks: c.ticks.load(Ordering::Relaxed),
            entries: c.entries.load(Ordering::Relaxed),
            reruns: c.reruns.load(Ordering::Relaxed),
            resyncs: c.resyncs.load(Ordering::Relaxed),
            safety_passes: c.safety_passes.load(Ordering::Relaxed),
            missed_invalidations: c.missed.load(Ordering::Relaxed),
            tick_errors: c.tick_errors.load(Ordering::Relaxed),
            dropped_batches: c.dropped_batches.load(Ordering::Relaxed),
            subscriptions: c.subscriptions.load(Ordering::Relaxed),
        }
    }

    /// Test hook: the next journal batch with entries is acknowledged
    /// without matching its writes, as a lost invalidation would be (the
    /// safety rerun must find it).
    #[doc(hidden)]
    pub fn drop_next_batch(&self) {
        self.shared.drop_next_batch.store(true, Ordering::SeqCst);
    }
}

/// The timestamp `lag` before `ts` (its physical part moved back, the
/// logical part zero); `ts` itself when `lag` is zero.
pub fn lagged(ts: &Ts, lag: Duration) -> Ts {
    if lag.is_zero() {
        return *ts;
    }
    let back = u64::try_from(lag.as_millis()).unwrap_or(u64::MAX);
    Ts::from_parts(ts.physical_ms().saturating_sub(back), 0)
}

fn stopped() -> LiveError {
    LiveError::Internal("the subscription manager has stopped".into())
}

fn check_query(f: &dyn Function) -> Result<(), LiveError> {
    if f.kind() == FnKind::Query {
        Ok(())
    } else {
        Err(LiveError::invalid(format!(
            "{} is a mutation; only queries can be subscribed to",
            f.name()
        )))
    }
}

/// A subscription's function, or why its name does not resolve.
type Resolved = Result<Arc<dyn Function>, LiveError>;

/// What a subscription runs, once placed: its key, its function's name, the
/// function (or why the name does not resolve), and whether an activation
/// resolves it again (a deployment's function, or a missing one).
struct Placed {
    key: SubKey,
    name: String,
    f: Resolved,
    deployed: bool,
}

struct Sub {
    key: SubKey,
    name: String,
    /// The function, or why its name does not resolve (the result then
    /// holds that error; it is not rerun until the next activation).
    f: Resolved,
    /// Resolved again at every activation.
    deployed: bool,
    args: LiveValue,
    refs: usize,
    result: Arc<SubResult>,
    read_set: ReadSet,
    /// The last evaluation failed: rerun on every tick.
    errored: bool,
}

/// A new key waiting for its first evaluation, and everyone asking for it.
struct NewSub {
    placed: Placed,
    args: LiveValue,
    replies: Vec<Reply>,
}

struct Manager {
    runner: Runner,
    config: SubsConfig,
    shutdown: CancellationToken,
    cmds: mpsc::UnboundedReceiver<Cmd>,
    updates: broadcast::Sender<Tick>,
    current: watch::Sender<Option<Ts>>,
    shared: Arc<Shared>,
    commits: watch::Receiver<u64>,
    /// What the app's engine serves, when following one.
    follow: Option<watch::Receiver<Serving>>,
    /// The engine is still there to follow.
    follow_open: bool,
    /// The deployment adopted: it runs every deployed subscription.
    serving: Serving,
    tailer: Option<Tailer>,
    /// The tick every subscription is valid at.
    at: Option<Ts>,
    /// Whether the tailer's positions are those of `at` (false while a tick
    /// that acknowledged batches has not finished its reruns): a new
    /// subscription is evaluated at `at` only when true.
    valid: bool,
    index: ReadSetIndex,
    subs: HashMap<SubId, Sub>,
    by_key: HashMap<SubKey, SubId>,
    next_id: u64,
    /// Subscriptions invalidated by acknowledged batches, not rerun yet.
    pending: HashSet<SubId>,
    /// New keys waiting for a valid tick.
    waiting: Vec<NewSub>,
    last_rerun: Option<Instant>,
    last_safety: Instant,
    last_checkpoint: Option<Instant>,
}

impl Manager {
    async fn run(mut self) {
        let mut poll = self.config.poll_min;
        let mut next_tick = tokio::time::Instant::now();
        let mut commits_open = true;
        loop {
            // Unbiased: neither a stream of commands nor a stream of commits
            // starves the other.
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                cmd = self.cmds.recv() => match cmd {
                    None => break,
                    Some(cmd) => {
                        self.commands(cmd).await;
                        continue;
                    }
                },
                changed = self.commits.changed(), if commits_open => {
                    if changed.is_err() {
                        commits_open = false;
                        continue;
                    }
                    poll = self.config.poll_min;
                    if let Some(at) = self.woken() {
                        next_tick = next_tick.min(at);
                        continue;
                    }
                }
                () = self.shared.wake.notified() => {
                    poll = self.config.poll_min;
                    if let Some(at) = self.woken() {
                        next_tick = next_tick.min(at);
                        continue;
                    }
                }
                // The engine serves something else, or an activation began
                // or ended: tick now (a held tick is retried at the end).
                changed = follow_changed(&mut self.follow), if self.follow_open => {
                    if changed.is_err() {
                        self.follow_open = false;
                        continue;
                    }
                    poll = self.config.poll_min;
                }
                () = tokio::time::sleep_until(next_tick) => {}
            }
            let moved = match self.tick().await {
                Ok(moved) => moved,
                Err(e) => {
                    self.shared
                        .counters
                        .tick_errors
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "a subscription tick failed; retrying");
                    false
                }
            };
            poll = if moved {
                self.config.poll_min
            } else {
                (poll * 2).clamp(self.config.poll_min, self.config.poll_max)
            };
            next_tick = tokio::time::Instant::now() + poll;
        }
        tracing::debug!("the subscription manager stopped");
    }

    /// Whether a tick at `at` may run, and the deployment to adopt with it.
    /// `at` moves up to the timestamp from which the deployment (adopted or
    /// to adopt) is served: a tick never runs a deployment's code before
    /// its activation. `None` holds the tick: an activation of the engine
    /// is in flight. Called after `at` is taken, so a tick that sees none
    /// in flight reads before any later activation commits.
    fn follow(&mut self, at: Ts) -> Option<(Ts, Option<Serving>)> {
        let Some(follow) = &mut self.follow else {
            return Some((at, None));
        };
        let serving = follow.borrow_and_update();
        if serving.switching > 0 {
            return None;
        }
        let adopt = (serving.generation != self.serving.generation).then(|| serving.clone());
        drop(serving);
        let from = adopt.as_ref().map_or(self.serving.at, |s| s.at);
        let at = if from.0 > at.0 { from } else { at };
        Some((at, adopt))
    }

    /// Places a subscription: a deployed function (or a missing one) of a
    /// manager following an engine runs in the adopted deployment.
    fn place(&self, key: SubKey, target: Target, args: &LiveValue) -> Placed {
        match target {
            Target::Function(f) => {
                let name = f.name().to_string();
                let deployment = f.deployment();
                if deployment.is_nil() {
                    return Placed {
                        key,
                        name,
                        f: Ok(f),
                        deployed: false,
                    };
                }
                if self.follow.is_none() || self.serving.deployment() == Some(deployment) {
                    return Placed {
                        key,
                        name,
                        f: Ok(f),
                        deployed: true,
                    };
                }
                self.resolve(name, args)
            }
            Target::Missing { name, error } => {
                if self.follow.is_none() {
                    Placed {
                        key,
                        name,
                        f: Err(error),
                        deployed: true,
                    }
                } else {
                    self.resolve(name, args)
                }
            }
        }
    }

    /// `name` resolved in the adopted deployment.
    fn resolve(&self, name: String, args: &LiveValue) -> Placed {
        resolve_in(&self.serving, name, args)
    }

    /// Adopts what the engine serves: every deployed subscription (and every
    /// new one waiting) is resolved again by name in it.
    fn adopt(&mut self, serving: Serving) {
        self.serving = serving;
        let ids: Vec<SubId> = self
            .subs
            .iter()
            .filter(|(_, s)| s.deployed)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let Some((name, args, old)) = self
                .subs
                .get(&id)
                .map(|s| (s.name.clone(), s.args.clone(), s.key.clone()))
            else {
                continue;
            };
            let placed = self.resolve(name, &args);
            if self.by_key.get(&old) == Some(&id) {
                self.by_key.remove(&old);
            }
            self.by_key.entry(placed.key.clone()).or_insert(id);
            if placed.f.is_err() {
                self.index.remove(id);
            }
            if let Some(sub) = self.subs.get_mut(&id) {
                if placed.f.is_err() {
                    sub.read_set = ReadSet::default();
                    sub.errored = false;
                }
                sub.key = placed.key;
                sub.f = placed.f;
            }
        }
        for n in &mut self.waiting {
            if n.placed.deployed {
                n.placed = resolve_in(&self.serving, n.placed.name.clone(), &n.args);
            }
        }
    }

    /// When to tick after a wake-up: `tick_read_lag` from now, since a tick
    /// reads that far back and would not see the commit sooner (however many
    /// commits follow, the tick is not pushed later); `None` (tick now)
    /// without a lag.
    fn woken(&self) -> Option<tokio::time::Instant> {
        let lag = self.config.tick_read_lag;
        (!lag.is_zero()).then(|| tokio::time::Instant::now() + lag)
    }

    /// Handles `first` and every command already queued behind it; new
    /// keys are evaluated together at the current tick.
    async fn commands(&mut self, first: Cmd) {
        let mut fresh: Vec<NewSub> = Vec::new();
        let mut next = Some(first);
        while let Some(cmd) = next {
            match cmd {
                Cmd::Subscribe {
                    key,
                    target,
                    args,
                    reply,
                } => {
                    let placed = self.place(key, target, &args);
                    if let Some(id) = self.by_key.get(&placed.key).copied()
                        && let Some(sub) = self.subs.get_mut(&id)
                    {
                        sub.refs += 1;
                        let _ = reply.send(Ok((id, sub.result.clone())));
                    } else if let Some(n) = fresh
                        .iter_mut()
                        .chain(self.waiting.iter_mut())
                        .find(|n| n.placed.key == placed.key)
                    {
                        // A key already asked for (in this batch, or waiting
                        // for a valid tick) gets no second entry.
                        n.replies.push(reply);
                    } else {
                        fresh.push(NewSub {
                            placed,
                            args,
                            replies: vec![reply],
                        });
                    }
                }
                Cmd::Unsubscribe(id) => self.unsubscribe(id),
            }
            next = self.cmds.try_recv().ok();
        }
        if fresh.is_empty() {
            return;
        }
        match (&self.at, self.valid) {
            (Some(at), true) => {
                let at = *at;
                self.evaluate_new(fresh, &at).await;
            }
            _ => self.waiting.extend(fresh),
        }
    }

    fn unsubscribe(&mut self, id: SubId) {
        let Some(sub) = self.subs.get_mut(&id) else {
            return;
        };
        sub.refs = sub.refs.saturating_sub(1);
        if sub.refs == 0 {
            if let Some(sub) = self.subs.remove(&id)
                && self.by_key.get(&sub.key) == Some(&id)
            {
                self.by_key.remove(&sub.key);
            }
            self.index.remove(id);
            self.pending.remove(&id);
            self.count_subs();
        }
    }

    fn count_subs(&self) {
        self.shared
            .counters
            .subscriptions
            .store(self.subs.len() as u64, Ordering::Relaxed);
    }

    /// Evaluates new keys at `at` and subscribes them.
    async fn evaluate_new(&mut self, fresh: Vec<NewSub>, at: &Ts) {
        let jobs = fresh
            .iter()
            .map(|n| (n.placed.f.clone(), n.args.clone()))
            .collect::<Vec<_>>();
        let results = self.run_all(jobs, at).await;
        let mut replies = Vec::new();
        for (n, result) in fresh.into_iter().zip(results) {
            let id = SubId(self.next_id);
            self.next_id += 1;
            let (result, read_set, errored) = split(result);
            let errored = errored && n.placed.f.is_ok();
            let result = Arc::new(SubResult {
                result,
                ts: *at,
                deployment: n.placed.key.deployment,
            });
            if !errored && n.placed.f.is_ok() {
                self.index.insert(id, &read_set);
            }
            self.by_key.insert(n.placed.key.clone(), id);
            self.subs.insert(
                id,
                Sub {
                    key: n.placed.key,
                    name: n.placed.name,
                    f: n.placed.f,
                    deployed: n.placed.deployed,
                    args: n.args,
                    refs: n.replies.len(),
                    result: result.clone(),
                    read_set,
                    errored,
                },
            );
            replies.extend(n.replies.into_iter().map(|r| (r, id, result.clone())));
        }
        // The state (and its counters) is complete before anyone hears back.
        self.count_subs();
        for (reply, id, result) in replies {
            let _ = reply.send(Ok((id, result)));
        }
    }

    /// Runs `jobs` at `at`: a function is evaluated (see
    /// [`evaluate`](Self::evaluate), counted as a rerun), a name that does
    /// not resolve answers its error.
    async fn run_all(
        &self,
        jobs: Vec<(Resolved, LiveValue)>,
        at: &Ts,
    ) -> Vec<Result<Queried, LiveError>> {
        let runnable = jobs
            .iter()
            .filter_map(|(f, args)| f.as_ref().ok().map(|f| (f.clone(), args.clone())))
            .collect::<Vec<_>>();
        let mut evaluated = self.evaluate(runnable, at).await.into_iter();
        jobs.into_iter()
            .map(|(f, _)| match f {
                Ok(_) => evaluated.next().unwrap_or_else(|| {
                    Err(LiveError::Internal("an evaluation went missing".into()))
                }),
                Err(e) => Err(e),
            })
            .collect()
    }

    /// Runs `jobs` at `at`, `rerun_concurrency` at once, in order. Boxed:
    /// the stream's closure otherwise hides the future's `Send` from
    /// `tokio::spawn` (a higher-ranked lifetime).
    fn evaluate<'a>(
        &'a self,
        jobs: Vec<(Arc<dyn Function>, LiveValue)>,
        at: &'a Ts,
    ) -> BoxFuture<'a, Vec<Result<Queried, LiveError>>> {
        let runner = self.runner.clone();
        let concurrency = self.config.rerun_concurrency.max(1);
        futures::stream::iter(jobs)
            .map(move |(f, args)| {
                let runner = runner.clone();
                let at = *at;
                async move { evaluate_one(&runner, &*f, args, &at).await }.boxed()
            })
            .buffered(concurrency)
            .collect()
            .boxed()
    }

    /// The next tick's timestamp: a fresh TSO timestamp `tick_read_lag`
    /// back, and never before the current tick.
    async fn now(&self) -> Result<Ts, LiveError> {
        let now = self
            .runner
            .store()
            .now()
            .await
            .map_err(|e| LiveError::Internal(format!("a tick timestamp: {e}")))?;
        let at = lagged(&now, self.config.tick_read_lag);
        Ok(match &self.at {
            Some(current) if current.0 > at.0 => *current,
            _ => at,
        })
    }

    /// One tick; returns whether the journal moved (or the tick adopted a
    /// deployment). A tick held for an activation in flight returns false.
    async fn tick(&mut self) -> Result<bool, LiveError> {
        if self.tailer.is_none() {
            return self.restart(false).await;
        }
        if let Some(last) = self.last_rerun {
            let wait = self
                .config
                .min_rerun_interval
                .saturating_sub(last.elapsed());
            if !wait.is_zero() {
                tokio::select! {
                    () = self.shutdown.cancelled() => return Ok(false),
                    () = tokio::time::sleep(wait) => {}
                }
            }
        }
        self.commits.borrow_and_update();
        let at = self.now().await?;
        let Some((at, adopt)) = self.follow(at) else {
            return Ok(false);
        };
        let mut moved = adopt.is_some();
        loop {
            let Some(tailer) = self.tailer.as_mut() else {
                return Err(LiveError::Internal("the tailer is gone".into()));
            };
            let batch = match tailer.tick(at).await {
                Ok(batch) => batch,
                Err(LiveError::JournalTrimmed {
                    shard,
                    position,
                    first,
                }) => {
                    tracing::warn!(
                        shard,
                        position,
                        ?first,
                        "the journal was trimmed past the subscription tailer; rerunning every subscription"
                    );
                    return self.restart(true).await;
                }
                Err(e) => return Err(e),
            };
            if !batch.is_empty() {
                moved = true;
                self.valid = false;
            }
            self.shared
                .counters
                .entries
                .fetch_add(batch.entries.len() as u64, Ordering::Relaxed);
            let drop = !batch.entries.is_empty()
                && self.shared.drop_next_batch.swap(false, Ordering::SeqCst);
            if drop {
                self.shared
                    .counters
                    .dropped_batches
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                for (_, _, entry) in &batch.entries {
                    for w in &entry.writes {
                        self.index.stab(w, &mut self.pending);
                    }
                }
            }
            tailer.ack(&batch)?;
            if batch.complete {
                break;
            }
        }
        // An activation reruns every subscription once, on the new
        // deployment, and publishes them all.
        let force = adopt.is_some();
        let mut ids: Vec<SubId> = match adopt {
            Some(serving) => {
                self.adopt(serving);
                self.subs.keys().copied().collect()
            }
            None => self
                .subs
                .iter()
                .filter(|(id, sub)| sub.errored || self.pending.contains(id))
                .map(|(id, _)| *id)
                .collect(),
        };
        ids.sort_unstable();
        let mut changed = self.rerun(&ids, &at, force).await;
        if !ids.is_empty() {
            self.last_rerun = Some(Instant::now());
        }
        self.pending.clear();
        if self.last_safety.elapsed() >= self.config.safety_rerun {
            self.last_safety = Instant::now();
            let repaired = self.safety(&at).await;
            for (id, result) in repaired {
                changed.retain(|(c, _)| *c != id);
                changed.push((id, result));
            }
        }
        self.finish(at, changed, false).await;
        Ok(moved)
    }

    /// Starts (or restarts, after a trimmed journal) the tailer at a fresh
    /// timestamp and reruns every subscription there; returns false when
    /// held for an activation in flight (the next tick tries again).
    async fn restart(&mut self, resync: bool) -> Result<bool, LiveError> {
        let at = self.now().await?;
        let Some((at, adopt)) = self.follow(at) else {
            return Ok(false);
        };
        let journal = self.runner.journal().await?;
        let tailer = Tailer::start(self.runner.store().clone(), journal, at).await?;
        self.tailer = Some(tailer);
        self.pending.clear();
        let force = adopt.is_some();
        if let Some(serving) = adopt {
            self.adopt(serving);
        }
        let mut ids: Vec<SubId> = self.subs.keys().copied().collect();
        ids.sort_unstable();
        let changed = self.rerun(&ids, &at, force).await;
        if resync {
            self.shared.counters.resyncs.fetch_add(1, Ordering::Relaxed);
        }
        self.finish(at, changed, resync).await;
        Ok(true)
    }

    /// Every subscription is valid at `at`: publish, evaluate waiting keys,
    /// checkpoint when due.
    async fn finish(&mut self, at: Ts, changed: Vec<(SubId, Arc<SubResult>)>, resynced: bool) {
        self.at = Some(at);
        self.valid = true;
        self.shared.counters.ticks.fetch_add(1, Ordering::Relaxed);
        // The tick is published before `current` moves and before anyone
        // waiting hears back: whoever reads `current() == t`, or is answered
        // at `t`, finds every tick up to `t` in a receiver it held before
        // (sessions rely on this, row T12-3).
        let _ = self.updates.send(Tick {
            at,
            changed,
            resynced,
        });
        self.current.send_replace(Some(at));
        if !self.waiting.is_empty() {
            let waiting = std::mem::take(&mut self.waiting);
            self.evaluate_new(waiting, &at).await;
        }
        self.checkpoint().await;
    }

    async fn checkpoint(&mut self) {
        let (Some(consumer), Some(tailer)) = (&self.config.consumer, &self.tailer) else {
            return;
        };
        if self
            .last_checkpoint
            .is_some_and(|t| t.elapsed() < CHECKPOINT_INTERVAL)
        {
            return;
        }
        match tailer.checkpoint(consumer, Some(CHECKPOINT_TTL)).await {
            Ok(()) => self.last_checkpoint = Some(Instant::now()),
            Err(e) => {
                tracing::warn!(error = %e, consumer, "the subscription tailer's checkpoint failed")
            }
        }
    }

    /// Reruns `ids` at `at`, replaces their read sets, and returns those
    /// whose result changed (every one, with `force`: an activation).
    async fn rerun(&mut self, ids: &[SubId], at: &Ts, force: bool) -> Vec<(SubId, Arc<SubResult>)> {
        let (ids, jobs): (Vec<SubId>, Vec<_>) = ids
            .iter()
            .filter_map(|id| {
                self.subs
                    .get(id)
                    .map(|s| (*id, (s.f.clone(), s.args.clone())))
            })
            .unzip();
        let evaluations = jobs.iter().filter(|(f, _)| f.is_ok()).count();
        let results = self.run_all(jobs, at).await;
        self.shared
            .counters
            .reruns
            .fetch_add(evaluations as u64, Ordering::Relaxed);
        let mut changed = Vec::new();
        for (id, result) in ids.into_iter().zip(results) {
            if let Some(result) = self.apply(id, result, at, force) {
                changed.push((id, result));
            }
        }
        changed
    }

    /// Takes a fresh evaluation of `id`; returns its result if it changed
    /// (or always, with `force`).
    fn apply(
        &mut self,
        id: SubId,
        evaluated: Result<Queried, LiveError>,
        at: &Ts,
        force: bool,
    ) -> Option<Arc<SubResult>> {
        let sub = self.subs.get_mut(&id)?;
        let (result, read_set, errored) = split(evaluated);
        // A name that does not resolve is not rerun until an activation.
        let errored = errored && sub.f.is_ok();
        if errored || sub.f.is_err() {
            self.index.remove(id);
        } else {
            self.index.insert(id, &read_set);
        }
        sub.read_set = read_set;
        sub.errored = errored;
        if !force && sub.result.result == result {
            return None;
        }
        sub.result = Arc::new(SubResult {
            result,
            ts: *at,
            deployment: sub.key.deployment,
        });
        Some(sub.result.clone())
    }

    /// Reruns every subscription at `at` and compares (§20 §8.2, the safety
    /// net); returns the repaired ones.
    async fn safety(&mut self, at: &Ts) -> Vec<(SubId, Arc<SubResult>)> {
        self.shared
            .counters
            .safety_passes
            .fetch_add(1, Ordering::Relaxed);
        let mut ids: Vec<SubId> = self
            .subs
            .iter()
            .filter(|(_, s)| !s.errored && s.f.is_ok())
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        let jobs = ids
            .iter()
            .filter_map(|id| {
                let s = self.subs.get(id)?;
                Some((s.f.as_ref().ok()?.clone(), s.args.clone()))
            })
            .collect();
        let results = self.evaluate(jobs, at).await;
        self.shared
            .counters
            .reruns
            .fetch_add(results.len() as u64, Ordering::Relaxed);
        let mut repaired = Vec::new();
        for (id, fresh) in ids.into_iter().zip(results) {
            let Ok(fresh) = fresh else {
                // A failure now says nothing about the result held.
                continue;
            };
            let Some(sub) = self.subs.get(&id) else {
                continue;
            };
            if sub.result.result.as_ref() != Ok(&fresh.result) {
                self.shared.counters.missed.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    subscription = %id,
                    function = %sub.name,
                    at = at.0,
                    held_since = sub.result.ts.0,
                    held_read_set = ?sub.read_set,
                    fresh_read_set = ?fresh.read_set,
                    "live_missed_invalidation_total: a safety rerun found a different result"
                );
            }
            if let Some(result) = self.apply(id, Ok(fresh), at, false) {
                repaired.push((id, result));
            }
        }
        repaired
    }
}

/// `name` resolved in `serving`, as a query subscription with `args`.
fn resolve_in(serving: &Serving, name: String, args: &LiveValue) -> Placed {
    let f = serving.resolve(&name).and_then(|f| {
        check_query(&*f)?;
        Ok(f)
    });
    let deployment = match &f {
        Ok(f) => f.deployment(),
        Err(_) => serving.deployment().unwrap_or_else(Ulid::nil),
    };
    Placed {
        key: SubKey {
            deployment,
            function: name.clone(),
            args_digest: args.digest(),
        },
        name,
        f,
        deployed: true,
    }
}

/// The next change of what a followed engine serves (never, without one).
async fn follow_changed(
    follow: &mut Option<watch::Receiver<Serving>>,
) -> Result<(), watch::error::RecvError> {
    match follow {
        Some(follow) => follow.changed().await,
        None => std::future::pending().await,
    }
}

/// A query at `at`, retried on storage failures. What its final attempt
/// wrote goes to the app's log ring (LV1 plan Task 10), under a request id
/// of its own.
async fn evaluate_one(
    runner: &Runner,
    f: &dyn Function,
    args: LiveValue,
    at: &Ts,
) -> Result<Queried, LiveError> {
    let mut attempt = 1;
    let evaluated = loop {
        match runner.query(f, args.clone(), *at).await {
            Err(LiveError::Txn(e)) if attempt < EVAL_ATTEMPTS && transient(&e) => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt))).await;
            }
            other => break other,
        }
    };
    let call = crate::logs::CallInfo {
        request_id: None,
        function: f.name(),
        deployment: f.deployment(),
    };
    runner
        .logs()
        .record(&call, evaluated.as_ref().map(|q| &q.output));
    evaluated
}

fn transient(e: &TxnError) -> bool {
    matches!(e, TxnError::Conflict | TxnError::NotApplied(_))
}

/// An evaluation as (result, read set, errored).
fn split(evaluated: Result<Queried, LiveError>) -> (Result<LiveValue, LiveError>, ReadSet, bool) {
    match evaluated {
        Ok(q) => (Ok(q.result), q.read_set, false),
        Err(e) => (Err(e), ReadSet::default(), true),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn sub_keys_are_canonical() {
        let a = LiveValue::Object(BTreeMap::from([
            ("b".to_string(), LiveValue::I64(1)),
            ("a".to_string(), LiveValue::Str("x".into())),
        ]));
        let b = LiveValue::Object(BTreeMap::from([
            ("a".to_string(), LiveValue::Str("x".into())),
            ("b".to_string(), LiveValue::I64(1)),
        ]));
        assert_eq!(SubKey::new("f", &a), SubKey::new("f", &b));
        assert_ne!(SubKey::new("f", &a), SubKey::new("g", &a));
        assert_ne!(
            SubKey::new("f", &LiveValue::I64(1)),
            SubKey::new("f", &LiveValue::F64(1.0))
        );
        assert_ne!(
            SubKey::new("f", &LiveValue::Array(vec![LiveValue::Str("ab".into())])),
            SubKey::new(
                "f",
                &LiveValue::Array(vec![LiveValue::Str("a".into()), LiveValue::Str("b".into())])
            )
        );
        assert_eq!(
            SubKey::new("f", &LiveValue::F64(f64::NAN)),
            SubKey::new("f", &LiveValue::F64(-f64::NAN))
        );
    }

    #[test]
    fn defaults_follow_the_plan() {
        let c = SubsConfig::default();
        assert_eq!(c.poll_min, Duration::from_millis(20));
        assert_eq!(c.poll_max, Duration::from_millis(200));
        assert_eq!(c.rerun_concurrency, 16);
        assert_eq!(c.min_rerun_interval, Duration::from_millis(50));
        assert_eq!(c.safety_rerun, Duration::from_secs(300));
        assert_eq!(c.consumer, None);
        assert_eq!(c.tick_read_lag, Duration::from_millis(50));
    }

    #[test]
    fn lagged_moves_the_physical_part_back() {
        let ts = Ts::from_parts(10_000, 7);
        let back = lagged(&ts, Duration::from_millis(50));
        assert_eq!((back.physical_ms(), back.logical()), (9_950, 0));
        assert_eq!(lagged(&ts, Duration::ZERO), ts);
        assert!(back.0 < ts.0);
    }
}
