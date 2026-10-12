//! Deployments (design §45 §3.3, D683; LV1 plan Task 6): the functions an
//! app serves, their versioned records, the bundle store, and activation by
//! compare-and-swap.
//!
//! A deployment is immutable but for its [`DeploymentState`]. Its record
//! lives in the app's catalog; its bundle is an object of the bundle store
//! at `live/<app>/deployments/<id>.js` (R1 plan Ruling 10), checked against
//! the record's SHA-256 whenever a node loads it. The app's deployment
//! pointer names the active one. [`deploy`] runs §45's order:
//!
//! 1. validate the bundle by loading it in a scratch runtime (in process, or
//!    in a sandboxed worker with `isolation = "isolated"`) — the loaded
//!    bundle then serves, if this deploy activates;
//! 2. store the bundle and read it back to check its hash;
//! 3. write the `pending` record;
//! 4. when the schema makes a table's validator stricter (Task 8, design
//!    §45 §4.1) or adds an index to an existing table (Task 9, §4.2): in
//!    one transaction, check the pointer, mark the record `validating` (or
//!    `backfilling`, with only indexes), [enforce](schema::enforce) the
//!    new validators on new writes and add the new indexes `backfilling`
//!    ([`schema::begin_backfills`]); fence the journal after it
//!    ([`Journal::fence_after`]); then scan each
//!    such table in a [`TableJob`](backfill::TableJob): the validations
//!    first, then the backfills (the record moves to `backfilling`). A
//!    document that does not match, or whose new index entry is too large,
//!    fails the deployment (listing up to 100 ids), and the new validators
//!    and indexes are [released](schema::release);
//! 5. in one transaction: compare-and-swap the pointer against
//!    `expected_active` (a mismatch fails the deployment with
//!    `FAILED_PRECONDITION`, reason `live_deploy_conflict`), apply the
//!    schema ([`schema::apply`]: additive table creation, the validators,
//!    the backfilled indexes made ready and the dropped ones `dropping`),
//!    mark the deployment `active`
//!    and the one it replaces `superseded`. A failure at 4 or 5 marks the
//!    record `failed` with the reason;
//! 6. a dropped index's entries are deleted in the background
//!    ([`backfill::drop_indexes`], also each janitor round);
//! 7. the `catalog` journal entry that switches every node at one tick is
//!    Task 11's. Until then a node picks up another node's deploy when it
//!    [refreshes](AppEngine::refresh) (the server does each janitor round).
//!
//! **Rollback** (LV1 plan Task 7) is [`activate`]: an earlier deployment
//! goes through the same path with its stored bundle and schema (its bundle
//! is read and hash-checked, loaded in a scratch runtime, its schema
//! re-applied and the pointer compared-and-swapped). Its record is not
//! rewritten; only states move.
//!
//! **Subscriptions follow the engine.** What a node serves is a
//! [`Serving`] in a `watch` channel ([`AppEngine::serving`]); the
//! subscription manager of the app follows it
//! ([`Subscriptions::spawn_for`](crate::Subscriptions::spawn_for)). At the
//! first tick at or after an activation's commit timestamp, every
//! subscription reruns once on the new deployment, and the tick publishes
//! all of them, so a session sees one Transition with only new results.
//! While an activation of this node is in flight ([`Serving::switching`]),
//! the manager holds its ticks, so no tick past the activation's commit
//! runs the old code.
//!
//! The `_system:*` functions resolve before any deployment ([`resolve`]),
//! so no bundle can shadow them. [`retain`] is the janitor's part: it keeps
//! the newest [`KEEP_DEPLOYMENTS`] deployments, the active one and its
//! parent, and deletes the rest with their bundles; it also fails a
//! `pending` record older than [`PENDING_GRACE`], left by a deploy that
//! crashed before activating, and releases the validator checks whose jobs'
//! leases expired, failing their `validating` records.
//!
//! The runtime lives in `loams-live-js`, which depends on this crate, so
//! it is reached through [`BundleLoader`] and [`LoadedBundle`]. A bundle
//! served by isolated workers reports when its workers crashed five times
//! within a minute (design §45 §3.1); the engine then marks the
//! deployment's record [`DeploymentState::Degraded`]. Its calls are still
//! served.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use buffa::Message;
use bytes::Bytes;
use futures::future::BoxFuture;
use loams_kv::{Ts, Txn};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use ulid::Ulid;

use crate::backfill::{self, Finding, IndexBackfill, JobKind, MAX_FINDINGS, TableJob};
use crate::docs::Reads;
use crate::journal::Journal;
use crate::keys::{AppKeys, KeyRange};
use crate::schema::{self, Conformance, Schema, SchemaChanges};
use crate::txn::{Refusal, atomic, catalog_txn, latest_snapshot, lift, snapshot};
use crate::{
    FnKind, Function, LiveError, LiveTxn, LiveValue, Runner, Visibility, catalog, pb, system,
};

/// How many deployments the janitor keeps, newest first (design §45 §3.3,
/// Q636); the active one and its parent are kept besides.
pub const KEEP_DEPLOYMENTS: usize = 50;

/// How old a bundle that no record names must be before the janitor
/// deletes it: younger, it may belong to a deploy that has stored its
/// bundle and not yet written its record.
pub const ORPHAN_BUNDLE_AGE: Duration = Duration::from_secs(60 * 60);

/// How old a `pending` record must be before the janitor fails it: a
/// deploy that is still running reaches its activation within seconds (one
/// transaction after the pending one), so a record this old was left by a
/// deploy that crashed or lost its node.
pub const PENDING_GRACE: Duration = Duration::from_secs(10 * 60);

/// The reason the janitor gives a `pending` record it fails.
const ABANDONED: &str = "the deploy did not finish (its record stayed pending past the grace \
                         period; it was failed by the janitor)";

/// The reason the janitor gives a `backfilling` record whose backfills it
/// released (LV1 plan Task 9).
const ABANDONED_BACKFILL: &str = "the deploy did not finish backfilling its new indexes (its \
                                  backfill jobs' leases expired; the janitor dropped them)";

/// The reason the janitor gives a `validating` record whose checks it
/// released.
const ABANDONED_VALIDATION: &str = "the deploy did not finish validating its stricter \
                                    validators (its validation jobs' leases expired; the \
                                    janitor released them)";

/// The records a [`PageRequest`] returns by default.
pub const DEFAULT_PAGE: usize = 100;

/// The most records one page returns.
pub const MAX_PAGE: usize = 1000;

/// Who deployed, while the Live API has no authentication (D111; the
/// principal arrives with LV1 Tasks 14–15).
pub const UNAUTHENTICATED: &str = "unauthenticated";

/// Where deployment `id` of `app` keeps its bundle in the bundle store.
pub fn bundle_path(app: &str, id: Ulid) -> String {
    format!("{}{id}.js", bundles_prefix(app))
}

fn bundles_prefix(app: &str) -> String {
    format!("live/{app}/deployments/")
}

/// A deployment's record (design §45 §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRecord {
    pub id: Ulid,
    pub bundle_sha256: [u8; 32],
    pub schema: Schema,
    /// The TSO physical time of the deploy's first transaction, in ms.
    pub created_at_ms: u64,
    /// The principal that deployed it.
    pub created_by: String,
    pub message: String,
    /// The deployment the client built against (`expected_active`): the
    /// one this deployment replaced, once active.
    pub parent: Option<Ulid>,
    pub state: DeploymentState,
}

/// Where a deployment is in its life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentState {
    /// Its record is written; its schema is not applied yet.
    Pending,
    /// A stricter validator scans existing data (Task 8).
    Validating,
    /// A new index is backfilled (Task 9).
    Backfilling,
    /// The pointer names it.
    Active,
    /// A later deployment replaced it.
    Superseded,
    /// It never became active.
    Failed { reason: String },
    /// It is active, and its isolated workers crashed five times within a
    /// minute; its calls are still served.
    Degraded,
}

/// A deploy: the bundle (one ES module), the schema it declares, the
/// deployment the client built against (`None`: the app has none active),
/// a message for the record, and whether to only validate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployRequest {
    pub bundle: Vec<u8>,
    pub schema: Schema,
    pub expected_active: Option<Ulid>,
    pub message: String,
    pub dry_run: bool,
}

/// What a deploy did: the deployment's record (in a dry run, the record it
/// would have written, `pending`) and the schema changes it applied (or
/// would apply).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployOutcome {
    pub record: DeploymentRecord,
    pub changes: SchemaChanges,
    pub dry_run: bool,
}

/// A page of [`list_deployments`]: up to `limit` records older than
/// `after` (the newest when `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    pub after: Option<Ulid>,
    pub limit: usize,
}

impl Default for PageRequest {
    fn default() -> Self {
        PageRequest {
            after: None,
            limit: DEFAULT_PAGE,
        }
    }
}

/// A page of records, newest first; `next` is the `after` of the next page
/// when there is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Ulid>,
}

/// What a [`retain`] run deleted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Retained {
    /// The deployment records deleted.
    pub records_deleted: Vec<Ulid>,
    /// The bundle objects deleted: those of the deleted records, and old
    /// ones no record names.
    pub bundles_deleted: usize,
    /// The `pending` records older than the grace period that were failed
    /// (deploys that never activated).
    pub abandoned: Vec<Ulid>,
    /// The deployments whose validator checks or index backfills were
    /// released because their jobs' leases expired (LV1 plan Tasks 8 and
    /// 9); a `validating` or `backfilling` record among them is failed.
    pub validations_released: Vec<Ulid>,
    /// The dropped indexes whose entries and definitions this round deleted
    /// (LV1 plan Task 9).
    pub indexes_removed: Vec<(crate::TableId, crate::IndexId)>,
}

/// Turns a bundle into functions: the function runtime (`loams-live-js`'s
/// `JsLoader`), behind a trait because that crate depends on this one.
pub trait BundleLoader: Send + Sync {
    /// Validates `bundle` — evaluates it within its limits and checks its
    /// exports — and starts what serves its functions: runtime slots in
    /// this process, or sandboxed worker processes. `app` labels the
    /// runtime's log lines.
    fn load<'a>(
        &'a self,
        app: &'a str,
        bundle: &'a [u8],
    ) -> BoxFuture<'a, Result<Arc<dyn LoadedBundle>, LiveError>>;
}

/// A loaded bundle: what serves one deployment's functions on this node.
pub trait LoadedBundle: Send + Sync {
    /// The function `path` (`module:export`), or `None`.
    fn function(&self, path: &str) -> Option<Arc<dyn Function>>;

    /// For a bundle served by isolated workers: a signal that turns `true`
    /// once five of them crashed within a minute (design §45 §3.1).
    fn degraded(&self) -> Option<watch::Receiver<bool>> {
        None
    }
}

/// How an app's deployments run: the bundle store and the function
/// runtime.
#[derive(Clone)]
pub struct Runtime {
    pub bundles: loams_store::Store,
    pub loader: Arc<dyn BundleLoader>,
}

impl Runtime {
    /// No function runtime: deploys are refused, and only the `_system:*`
    /// functions serve.
    pub fn none() -> Self {
        Runtime {
            bundles: loams_store::Store::in_memory(),
            loader: Arc::new(NoLoader),
        }
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("bundles", &self.bundles)
            .finish_non_exhaustive()
    }
}

struct NoLoader;

impl BundleLoader for NoLoader {
    fn load<'a>(
        &'a self,
        _app: &'a str,
        _bundle: &'a [u8],
    ) -> BoxFuture<'a, Result<Arc<dyn LoadedBundle>, LiveError>> {
        Box::pin(async {
            Err(LiveError::FailedPrecondition(
                "this Live server has no function runtime: bundles cannot be deployed or loaded"
                    .into(),
            ))
        })
    }
}

/// One app on this node: its runner, its runtime and the deployment it
/// serves. Cheap to clone. (LV1 plan Task 13 adds the subscription
/// manager, the sessions and the tailer, and starts engines lazily.)
#[derive(Clone)]
pub struct AppEngine {
    inner: Arc<Engine>,
}

struct Engine {
    name: String,
    runner: Runner,
    runtime: Runtime,
    /// What this node serves; the subscription manager follows it.
    serving: watch::Sender<Serving>,
    /// Serializes changing what this node serves (its deploys' activations
    /// and its refreshes), so an older deployment never replaces a newer
    /// one here.
    switch: tokio::sync::Mutex<()>,
    /// Deployment ids, monotonic on this node.
    ids: Mutex<ulid::Generator>,
    /// Testing: the next validation stops right after its fence commits,
    /// as if its node died ([`AppEngine::crash_after_next_fence`]).
    crash_after_fence: std::sync::atomic::AtomicBool,
}

/// What an app's engine serves on this node (LV1 plan Task 7), as the
/// subscription manager follows it through [`AppEngine::serving`].
#[derive(Clone, Default)]
pub struct Serving {
    /// Bumped whenever what is served changes.
    pub generation: u64,
    /// From when it is served: the commit timestamp of the activation that
    /// made it active on this node, or the snapshot at which this node read
    /// the pointer when it picked it up (open, refresh).
    pub at: Ts,
    /// Activations of this node in flight. While one is, the subscription
    /// manager holds its ticks: a tick that reads past the activation's
    /// commit must run the new deployment.
    pub switching: usize,
    served: Served,
}

impl Serving {
    /// The deployment served, if one is loaded.
    pub fn deployment(&self) -> Option<Ulid> {
        match &self.served {
            Served::Loaded(l) => Some(l.id),
            Served::Nothing | Served::Failed { .. } => None,
        }
    }

    /// The function `name` as served here: a `_system:*` function, else a
    /// function of the deployment. A function the deployment does not
    /// export is [`LiveError::NotFound`]; while the active deployment could
    /// not be loaded, its load error.
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn Function>, LiveError> {
        if let Some(f) = system::lookup(name) {
            return Ok(f);
        }
        match &self.served {
            Served::Nothing => Err(not_deployed(name)),
            Served::Failed { error } => Err(error.clone()),
            Served::Loaded(loaded) => {
                let inner = loaded
                    .bundle
                    .function(name)
                    .ok_or_else(|| not_deployed(name))?;
                Ok(Arc::new(Deployed {
                    id: loaded.id,
                    inner,
                }))
            }
        }
    }
}

impl fmt::Debug for Serving {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Serving")
            .field("generation", &self.generation)
            .field("at", &self.at)
            .field("switching", &self.switching)
            .field("deployment", &self.deployment())
            .finish_non_exhaustive()
    }
}

/// What this node serves.
#[derive(Clone, Default)]
enum Served {
    /// No deployment is active.
    #[default]
    Nothing,
    Loaded(Arc<Loaded>),
    /// The active deployment could not be loaded and nothing was loaded
    /// before: its functions fail with `error`.
    Failed {
        error: LiveError,
    },
}

/// An activation of this node in flight: [`Serving::switching`] counts it
/// until it drops (also if its task panics).
struct Switching<'a>(&'a watch::Sender<Serving>);

impl<'a> Switching<'a> {
    fn begin(serving: &'a watch::Sender<Serving>) -> Self {
        serving.send_modify(|s| s.switching += 1);
        Switching(serving)
    }
}

impl Drop for Switching<'_> {
    fn drop(&mut self) {
        self.0
            .send_modify(|s| s.switching = s.switching.saturating_sub(1));
    }
}

/// A deployment loaded on this node, and the task that watches its
/// degraded signal.
struct Loaded {
    id: Ulid,
    bundle: Arc<dyn LoadedBundle>,
    watcher: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Loaded {
    fn drop(&mut self) {
        if let Some(watcher) = &self.watcher {
            watcher.abort();
        }
    }
}

impl fmt::Debug for AppEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppEngine")
            .field("app", &self.inner.name)
            .field("active", &self.active())
            .finish_non_exhaustive()
    }
}

impl AppEngine {
    /// App `app` served by `runner` with `runtime`: reads the deployment
    /// pointer and loads the active deployment. A deployment that cannot be
    /// loaded (its bundle is missing or corrupt, or no runtime) is logged,
    /// and its functions fail with that error until a
    /// [`refresh`](Self::refresh) loads it; the `_system:*` functions serve
    /// either way. A storage error reading the catalog fails the open.
    pub async fn open(app: &str, runner: Runner, runtime: Runtime) -> Result<Self, LiveError> {
        catalog::check_name("app", app)?;
        let engine = AppEngine {
            inner: Arc::new(Engine {
                name: app.to_string(),
                runner,
                runtime,
                serving: watch::Sender::new(Serving::default()),
                switch: tokio::sync::Mutex::default(),
                ids: Mutex::new(ulid::Generator::new()),
                crash_after_fence: std::sync::atomic::AtomicBool::new(false),
            }),
        };
        // Nothing else holds the engine yet, so `switch` needs no lock.
        let (pointer, at) = engine.read_active_at().await?;
        if let Some(id) = pointer {
            // A load failure is logged and kept as the deployment's error.
            let _ = engine.serve_id(id, at).await;
        }
        Ok(engine)
    }

    /// Testing: the next deploy's validation (LV1 plan Task 8) stops right
    /// after its fence commits, without scanning or releasing anything, as
    /// if this node died there; the deploy returns
    /// [`LiveError::Internal`]. The janitor's release is tested this way.
    #[doc(hidden)]
    pub fn crash_after_next_fence(&self) {
        self.inner
            .crash_after_fence
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// The app's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The app's runner.
    pub fn runner(&self) -> &Runner {
        &self.inner.runner
    }

    /// The deployment this node serves, if one is loaded.
    pub fn active(&self) -> Option<Ulid> {
        self.inner.serving.borrow().deployment()
    }

    /// What this node serves, and every change of it (the subscription
    /// manager follows it, LV1 plan Task 7).
    pub fn serving(&self) -> watch::Receiver<Serving> {
        self.inner.serving.subscribe()
    }

    /// The active deployment as the catalog names it now.
    pub async fn read_active(&self) -> Result<Option<Ulid>, LiveError> {
        Ok(self.read_active_at().await?.0)
    }

    /// The active deployment as the catalog names it now, and the
    /// snapshot's timestamp.
    async fn read_active_at(&self) -> Result<(Option<Ulid>, Ts), LiveError> {
        let mut snap = latest_snapshot(self.inner.runner.store()).await?;
        let at = snap.ts();
        Ok((read_pointer(&mut snap, self.app_keys()).await?, at))
    }

    /// Loads the deployment the pointer names, if this node does not serve
    /// it yet, and serves it. If it cannot be loaded (for example its
    /// bundle does not match its record's SHA-256), the error is logged and
    /// returned, and the deployment this node served before keeps serving.
    pub async fn refresh(&self) -> Result<Option<Ulid>, LiveError> {
        let _switch = self.inner.switch.lock().await;
        let (pointer, at) = self.read_active_at().await.inspect_err(|error| {
            tracing::warn!(
                app = %self.inner.name,
                %error,
                "reading the deployment pointer failed; this node keeps serving what it served"
            );
        })?;
        match pointer {
            Some(id) => self.serve_id(id, at).await,
            None => Ok(None),
        }
    }

    /// Serves deployment `id`, active since `at` or earlier, loading it
    /// unless it is served already. The caller holds `switch`.
    async fn serve_id(&self, id: Ulid, at: Ts) -> Result<Option<Ulid>, LiveError> {
        if self.active() == Some(id) {
            return Ok(Some(id));
        }
        match self.load(id).await {
            Ok(loaded) => {
                self.serve(Served::Loaded(loaded), at);
                tracing::info!(app = %self.inner.name, deployment = %id, "serving deployment");
                Ok(Some(id))
            }
            Err(error) => {
                tracing::error!(
                    app = %self.inner.name,
                    deployment = %id,
                    %error,
                    "the active deployment could not be loaded; this node keeps serving what it served"
                );
                let unchanged = matches!(
                    &self.inner.serving.borrow().served,
                    Served::Failed { error: held } if *held == error
                );
                if self.active().is_none() && !unchanged {
                    self.serve(
                        Served::Failed {
                            error: error.clone(),
                        },
                        at,
                    );
                }
                Err(error)
            }
        }
    }

    fn app_keys(&self) -> &AppKeys {
        self.inner.runner.app()
    }

    /// Serves `served` from `at` on: one change of the `watch`, so whoever
    /// resolves the new functions finds the generation moved too.
    fn serve(&self, served: Served, at: Ts) {
        self.inner.serving.send_modify(|s| {
            s.served = served;
            s.generation += 1;
            if at.0 > s.at.0 {
                s.at = at;
            }
        });
    }

    fn next_id(&self) -> Ulid {
        let mut ids = self
            .inner
            .ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        ids.generate()
            .unwrap_or_else(ulid::Overflow::commit_overflow_increment)
    }

    /// Loads deployment `id`: its record, its bundle (hash checked) and the
    /// runtime that serves it.
    async fn load(&self, id: Ulid) -> Result<Arc<Loaded>, LiveError> {
        let mut snap = latest_snapshot(self.inner.runner.store()).await?;
        let record = read_record(&mut snap, self.app_keys(), id)
            .await?
            .ok_or_else(|| {
                LiveError::Corrupt(format!(
                    "the deployment pointer names {id}, which has no record"
                ))
            })?;
        let bundle = self.fetch_bundle(id, &record.bundle_sha256).await?;
        let loaded = self
            .inner
            .runtime
            .loader
            .load(&self.inner.name, &bundle)
            .await?;
        Ok(self.loaded(id, loaded))
    }

    /// `bundle` serving deployment `id`, with a task that marks the record
    /// degraded when the bundle's degraded signal fires.
    fn loaded(&self, id: Ulid, bundle: Arc<dyn LoadedBundle>) -> Arc<Loaded> {
        let watcher = bundle.degraded().map(|signal| {
            tokio::spawn(watch_degraded(
                self.inner.runner.clone(),
                self.inner.name.clone(),
                id,
                signal,
            ))
        });
        Arc::new(Loaded {
            id,
            bundle,
            watcher,
        })
    }

    /// Reads deployment `id`'s bundle and checks it against `sha256`.
    async fn fetch_bundle(&self, id: Ulid, sha256: &[u8; 32]) -> Result<Bytes, LiveError> {
        let path = bundle_path(&self.inner.name, id);
        let (bytes, _) = self
            .inner
            .runtime
            .bundles
            .get(&path)
            .await
            .map_err(|e| match e {
                loams_store::StoreError::NotFound { .. } => {
                    LiveError::Corrupt(format!("deployment {id}: its bundle {path} is missing"))
                }
                e => LiveError::Internal(format!("reading the bundle of deployment {id}: {e}")),
            })?;
        if Sha256::digest(&bytes).as_slice() != sha256 {
            tracing::error!(
                app = %self.inner.name,
                deployment = %id,
                path,
                "a stored bundle does not match its deployment record's SHA-256; refusing it"
            );
            return Err(LiveError::Corrupt(format!(
                "deployment {id}: the stored bundle's SHA-256 does not match its record"
            )));
        }
        Ok(bytes)
    }
}

/// Deploys `req` to `app` as principal `by` (design §45 §3.3; the module
/// documentation has the order). A dry run validates the bundle and the
/// schema, checks `expected_active` and reports the schema changes, and
/// writes nothing.
///
/// Errors: the bundle's own (`INVALID_ARGUMENT`, `FUNCTION_ERROR`, a
/// limit) and an invalid schema, before anything is written;
/// [`LiveError::DeployConflict`] when the active deployment is not
/// `expected_active`, and a schema the catalog refuses
/// ([`LiveError::FailedPrecondition`]), after which the record is
/// `failed`; storage errors.
pub async fn deploy(
    app: &AppEngine,
    req: DeployRequest,
    by: &str,
) -> Result<DeployOutcome, LiveError> {
    let engine = &app.inner;
    req.schema.check(engine.runner.limits())?;
    // 1. Validate the bundle in a scratch runtime, which then serves it.
    let loaded = engine
        .runtime
        .loader
        .load(&engine.name, &req.bundle)
        .await?;
    let id = app.next_id();
    let sha: [u8; 32] = Sha256::digest(&req.bundle).into();
    let mut record = DeploymentRecord {
        id,
        bundle_sha256: sha,
        schema: req.schema.clone(),
        created_at_ms: 0,
        created_by: by.to_string(),
        message: req.message.clone(),
        parent: req.expected_active,
        state: DeploymentState::Pending,
    };
    if req.dry_run {
        let mut snap = latest_snapshot(engine.runner.store()).await?;
        record.created_at_ms = snap.ts().physical_ms();
        let active = read_pointer(&mut snap, app.app_keys()).await?;
        if active != req.expected_active {
            return Err(conflict(req.expected_active, active));
        }
        let changes = schema::plan(&mut snap, app.app_keys(), &req.schema).await?;
        return Ok(DeployOutcome {
            record,
            changes,
            dry_run: true,
        });
    }

    // 2. Store the bundle and check its hash.
    engine
        .runtime
        .bundles
        .put(&bundle_path(&engine.name, id), Bytes::from(req.bundle))
        .await
        .map_err(|e| LiveError::Internal(format!("storing the bundle of deployment {id}: {e}")))?;
    app.fetch_bundle(id, &sha).await?;

    // 3. The pending record.
    record.created_at_ms = write_pending(&engine.runner, &record).await?;

    // 4. Check stricter validators against the tables' documents; 5.
    // apply the schema and swap the pointer, in one transaction.
    finish(app, loaded, id, req.expected_active, by, Path::Deploy).await
}

/// Activates deployment `id` of `app` as principal `by`: rollback (design
/// §45 §3.3; LV1 plan Task 7). It runs the deploy path with the stored
/// bundle and schema: the bundle is read and checked against the record's
/// SHA-256, loaded in a scratch runtime, the schema re-applied and the
/// pointer compared-and-swapped against `expected_active`. The record keeps
/// its contents (creator, message, parent); it becomes `active` and the
/// deployment it replaces `superseded`. Activating the active deployment
/// (with itself expected) loads it afresh.
///
/// Errors: [`LiveError::NotFound`] for an unknown id;
/// [`LiveError::FailedPrecondition`] for a deployment still `pending` (its
/// deploy has not finished) or a schema the catalog refuses;
/// [`LiveError::Corrupt`] for a missing or altered bundle; the bundle's own
/// load errors; [`LiveError::DeployConflict`] when the active deployment is
/// not `expected_active`. None of them changes the pointer or a record.
pub async fn activate(
    app: &AppEngine,
    id: Ulid,
    expected_active: Option<Ulid>,
    by: &str,
) -> Result<DeployOutcome, LiveError> {
    let engine = &app.inner;
    let mut snap = latest_snapshot(engine.runner.store()).await?;
    let record = read_record(&mut snap, app.app_keys(), id)
        .await?
        .ok_or_else(|| LiveError::NotFound(format!("deployment {id} does not exist")))?;
    check_state(&record, Path::Activate)?;
    record.schema.check(engine.runner.limits())?;
    let bundle = app.fetch_bundle(id, &record.bundle_sha256).await?;
    let loaded = engine.runtime.loader.load(&engine.name, &bundle).await?;
    finish(app, loaded, id, expected_active, by, Path::Activate).await
}

/// Which path reaches [`finish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path {
    /// A new deployment: its record must still be `pending` (or
    /// `validating`, its own step 4).
    Deploy,
    /// An existing deployment: its record must not be `pending` or
    /// `validating` (its deploy has not finished).
    Activate,
}

/// Refuses deployment `record` for `path` in a state that path cannot take.
fn check_state(record: &DeploymentRecord, path: Path) -> Result<(), LiveError> {
    let in_flight = matches!(
        record.state,
        DeploymentState::Pending | DeploymentState::Validating | DeploymentState::Backfilling
    );
    match path {
        Path::Deploy if !in_flight => Err(LiveError::FailedPrecondition(format!(
            "deployment {} is no longer pending ({}): its deploy took too long and the janitor \
             failed it",
            record.id,
            record.state.name()
        ))),
        Path::Activate if in_flight => Err(still_pending(record.id)),
        _ => Ok(()),
    }
}

/// Step 4 for deployment `id` (LV1 plan Tasks 8 and 9, design §45 §4.1,
/// §4.2): when its schema makes a table's validator stricter, checks the
/// table's documents first, and when it adds an index to an existing
/// table, backfills it. One transaction compares the pointer with
/// `expected`, marks a new deployment's record `validating` (or
/// `backfilling`), [enforces](schema::enforce) the new validators on new
/// writes (alongside the old ones) and adds the new indexes `backfilling`
/// ([`schema::begin_backfills`]). Then the journal is fenced
/// ([`Journal::fence_after`]), so every mutation in flight reruns and sees
/// them. Then each validated table is scanned by a [`TableJob`] with the
/// [`Conformance`] visitor, and each backfilled one with the
/// [`IndexBackfill`] visitor, at batches that all read after the fence.
/// Returns whether there was anything to do.
///
/// A document that does not match, or whose new index entry would be too
/// large (up to [`MAX_FINDINGS`] are listed), is
/// [`LiveError::FailedPrecondition`] naming them; the checks and backfills
/// are released and a new deployment's record is `failed`, as on a
/// conflict or any other error of this step. The pointer never changes
/// here.
async fn validate(
    app: &AppEngine,
    id: Ulid,
    expected: Option<Ulid>,
    path: Path,
) -> Result<bool, LiveError> {
    let runner = &app.inner.runner;
    let keys = app.app_keys().clone();
    let fail_reason = |err: &LiveError| (path == Path::Deploy).then(|| err.to_string());
    let planned = async {
        let mut snap = latest_snapshot(runner.store()).await?;
        let record = read_record(&mut snap, &keys, id)
            .await?
            .ok_or_else(|| LiveError::NotFound(format!("deployment {id} does not exist")))?;
        schema::plan(&mut snap, &keys, &record.schema).await
    };
    let planned = match planned.await {
        Ok(planned) => planned,
        // A schema the catalog refuses fails the deployment, as at step 5;
        // after a storage error a new record stays `pending`.
        Err(err) => {
            if !matches!(err, LiveError::Txn(_)) {
                fail(runner, id, fail_reason(&err).as_deref(), false).await;
            }
            return Err(err);
        }
    };
    if planned.validate.is_empty() && planned.indexes_added.is_empty() {
        return Ok(false);
    }
    let owner = app.next_id().to_string();
    let lease = runner.options().jobs.lease;
    let fenced = atomic(runner, "live.deploy.validate", |txn| {
        let (keys, owner) = (keys.clone(), owner.clone());
        Box::pin(async move {
            let active = read_pointer(txn, &keys).await?;
            if active != expected {
                return Err(conflict(expected, active));
            }
            let mut record = read_record(txn, &keys, id)
                .await?
                .ok_or_else(|| LiveError::NotFound(format!("deployment {id} does not exist")))?;
            check_state(&record, path)?;
            let checks = schema::enforce(txn, &keys, &record.schema, id, &owner, lease).await?;
            let backfills =
                schema::begin_backfills(txn, &keys, &record.schema, id, &owner, lease).await?;
            if path == Path::Deploy {
                record.state = if checks.is_empty() {
                    DeploymentState::Backfilling
                } else {
                    DeploymentState::Validating
                };
                txn.put(&keys.deployment(&id.to_bytes()), record.encode())
                    .await?;
            }
            let shards = catalog::load_journal_shards(txn, &keys)
                .await?
                .ok_or_else(|| LiveError::Internal("the app has no journal record".into()))?;
            Ok((checks, backfills, shards))
        })
    })
    .await;
    let (checks, backfills, shards) = match fenced {
        Ok(committed) => committed.value,
        Err(Refusal::Own(err)) => {
            if err.reason() == Some("live_deploy_conflict") {
                // This node's view was stale: serve what is active.
                let _ = app.refresh().await;
            }
            fail(runner, id, fail_reason(&err).as_deref(), false).await;
            return Err(err);
        }
        // The outcome may be unknown: release whatever it enforced.
        Err(Refusal::Storage(err)) => {
            fail(runner, id, None, true).await;
            return Err(err);
        }
    };
    // The fence (row T9-3): every mutation that read the catalog before the
    // step-4 commit has committed, or will rerun past it, before any scan.
    let fence = async {
        Journal::new(keys.clone(), shards)?
            .fence_after(runner.store(), runner.options().commit_mode)
            .await
    };
    if let Err(err) = fence.await {
        fail(runner, id, fail_reason(&err).as_deref(), true).await;
        return Err(err);
    }
    if app
        .inner
        .crash_after_fence
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(LiveError::Internal(
            "a simulated crash right after the validation's fence".into(),
        ));
    }
    let mut findings: Vec<(String, Finding)> = Vec::new();
    for check in &checks {
        let job = TableJob::new(
            runner.clone(),
            JobKind::Validate,
            check.table,
            owner.clone(),
        );
        let visitor = Arc::new(Conformance {
            validator: check.validator.clone(),
        });
        match job.run(visitor, MAX_FINDINGS - findings.len()).await {
            Ok(done) => findings.extend(
                done.findings
                    .into_iter()
                    .take(MAX_FINDINGS - findings.len())
                    .map(|f| (check.name.clone(), f)),
            ),
            Err(err) => {
                fail(runner, id, fail_reason(&err).as_deref(), true).await;
                return Err(err);
            }
        }
        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }
    if !findings.is_empty() {
        let err = LiveError::FailedPrecondition(describe_findings(id, &findings));
        tracing::warn!(
            app = %app.inner.name,
            deployment = %id,
            documents = findings.len(),
            "a deploy's stricter validators do not match existing documents; it failed"
        );
        fail(runner, id, fail_reason(&err).as_deref(), true).await;
        return Err(err);
    }
    if backfills.is_empty() {
        return Ok(true);
    }
    if path == Path::Deploy && !checks.is_empty() {
        let moved = atomic(runner, "live.deploy.backfilling", |txn| {
            let keys = keys.clone();
            Box::pin(async move {
                set_state(txn, &keys, id, |s| {
                    (*s == DeploymentState::Validating).then_some(DeploymentState::Backfilling)
                })
                .await
            })
        })
        .await;
        if let Err(Refusal::Own(err) | Refusal::Storage(err)) = moved {
            fail(runner, id, fail_reason(&err).as_deref(), true).await;
            return Err(err);
        }
    }
    let visitor = Arc::new(IndexBackfill::new(id, runner.limits().clone()));
    for table in &backfills {
        let job = TableJob::new(
            runner.clone(),
            JobKind::Backfill,
            table.table,
            owner.clone(),
        );
        match job
            .run(visitor.clone(), MAX_FINDINGS - findings.len())
            .await
        {
            Ok(done) => findings.extend(
                done.findings
                    .into_iter()
                    .take(MAX_FINDINGS - findings.len())
                    .map(|f| (table.name.clone(), f)),
            ),
            Err(err) => {
                fail(runner, id, fail_reason(&err).as_deref(), true).await;
                return Err(err);
            }
        }
        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }
    if !findings.is_empty() {
        let err = LiveError::FailedPrecondition(describe_index_findings(id, &findings));
        tracing::warn!(
            app = %app.inner.name,
            deployment = %id,
            documents = findings.len(),
            "a deploy's new indexes cannot hold existing documents; it failed"
        );
        fail(runner, id, fail_reason(&err).as_deref(), true).await;
        return Err(err);
    }
    Ok(true)
}

/// The error text of a failed backfill: the documents whose new index entry
/// cannot be written, by table, and why for the first (sizes and field
/// names, never values).
fn describe_index_findings(id: Ulid, findings: &[(String, Finding)]) -> String {
    let listed = findings
        .iter()
        .map(|(table, f)| format!("{} of '{table}'", f.id))
        .collect::<Vec<_>>()
        .join(", ");
    let (table, first) = &findings[0];
    format!(
        "deployment {id}: existing documents cannot be indexed by its new indexes: {listed}; \
         document {} of table '{table}': {}. Fix the documents (a migration), or change the \
         index, then deploy again",
        first.id, first.detail
    )
}

/// The error text of a failed check: the offending ids by table, and what
/// is wrong with the first (field paths and types, never values).
fn describe_findings(id: Ulid, findings: &[(String, Finding)]) -> String {
    let mut by_table: Vec<(&str, Vec<String>)> = Vec::new();
    for (table, finding) in findings {
        match by_table.iter_mut().find(|(t, _)| *t == table.as_str()) {
            Some((_, ids)) => ids.push(finding.id.to_string()),
            None => by_table.push((table, vec![finding.id.to_string()])),
        }
    }
    let listed = by_table
        .iter()
        .map(|(table, ids)| format!("table '{table}': {}", ids.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    let count = if findings.len() >= MAX_FINDINGS {
        format!("at least {MAX_FINDINGS} (the check stopped there)")
    } else {
        findings.len().to_string()
    };
    let (table, first) = &findings[0];
    format!(
        "deployment {id}: existing documents do not match its stricter validators: {count} \
         found ({listed}); document {} of table '{table}': {}. Fix the documents (a \
         migration), or declare the table with schemaValidation: false, then deploy again",
        first.id, first.detail
    )
}

/// Steps 4 and 5 for deployment `id`, loaded as `loaded`: check its
/// stricter validators ([`validate`]), then apply its schema and swap the
/// pointer against `expected`, and serve it ([`commit_now`]). While the
/// swap is in flight the engine reports [`Serving::switching`], so the
/// subscription manager holds its ticks until it knows which deployment
/// serves past the swap's commit.
async fn finish(
    app: &AppEngine,
    loaded: Arc<dyn LoadedBundle>,
    id: Ulid,
    expected: Option<Ulid>,
    by: &str,
    path: Path,
) -> Result<DeployOutcome, LiveError> {
    // Detached: once they start, steps 4 and 5 run to the end even if the
    // caller's future is dropped (a client that disconnects). So a
    // committed activation is always served here before the hold on the
    // subscription manager's ticks ends, and a validation never leaves its
    // stricter validator enforced past its own outcome.
    let (app, by) = (app.clone(), by.to_string());
    tokio::spawn(async move {
        let validated = validate(&app, id, expected, path).await?;
        commit_now(&app, loaded, id, expected, &by, path, validated).await
    })
    .await
    .map_err(|e| LiveError::Internal(format!("the activation task failed: {e}")))?
}

/// Step 5, in [`finish`]'s task.
async fn commit_now(
    app: &AppEngine,
    loaded: Arc<dyn LoadedBundle>,
    id: Ulid,
    expected: Option<Ulid>,
    by: &str,
    path: Path,
    validated: bool,
) -> Result<DeployOutcome, LiveError> {
    let engine = &app.inner;
    let switch = engine.switch.lock().await;
    let switching = Switching::begin(&engine.serving);
    match swap(&engine.runner, id, expected, path).await {
        Ok(Swapped {
            changes,
            record,
            at,
        }) => {
            app.serve(Served::Loaded(app.loaded(id, loaded)), at);
            drop(switching);
            drop(switch);
            if !changes.indexes_dropped.is_empty() {
                // The dropped indexes' entries go in the background (and in
                // the janitor's rounds, should this node die first).
                let runner = engine.runner.clone();
                let app_name = engine.name.clone();
                tokio::spawn(async move {
                    if let Err(error) = backfill::drop_indexes(&runner).await {
                        tracing::warn!(app = %app_name, %error, "deleting dropped indexes failed");
                    }
                });
            }
            match path {
                Path::Deploy => {
                    tracing::info!(app = %engine.name, deployment = %id, by, "deployed")
                }
                Path::Activate => {
                    tracing::info!(app = %engine.name, deployment = %id, by, "activated an earlier deployment")
                }
            }
            Ok(DeployOutcome {
                record,
                changes,
                dry_run: false,
            })
        }
        Err(SwapError::Conflict(active)) => {
            drop(switch);
            // This node's view was stale: serve what is active.
            let _ = app.refresh().await;
            drop(switching);
            let err = conflict(expected, active);
            let reason = (path == Path::Deploy).then(|| err.to_string());
            fail(&engine.runner, id, reason.as_deref(), validated).await;
            Err(err)
        }
        Err(SwapError::Refused(err)) => {
            drop(switching);
            drop(switch);
            let reason = (path == Path::Deploy).then(|| err.to_string());
            fail(&engine.runner, id, reason.as_deref(), validated).await;
            Err(err)
        }
        // The outcome may be unknown: a new record stays `pending` (or
        // `validating`) unless the activation committed. If it did, the
        // refresh serves it now, before the subscription manager may tick
        // again; if not, its validator checks are released (a committed
        // swap ended them itself).
        Err(SwapError::Storage(err)) => {
            drop(switch);
            let _ = app.refresh().await;
            drop(switching);
            if validated {
                fail(&engine.runner, id, None, true).await;
            }
            Err(err)
        }
    }
}

fn still_pending(id: Ulid) -> LiveError {
    LiveError::FailedPrecondition(format!(
        "deployment {id} is pending: its deploy has not finished"
    ))
}

/// The app's deployments, newest first, a page at a time.
pub async fn list_deployments(
    app: &AppEngine,
    page: PageRequest,
) -> Result<Page<DeploymentRecord>, LiveError> {
    let limit = page.limit.clamp(1, MAX_PAGE);
    let keys = app.app_keys();
    let all = keys.deployments();
    let range = match page.after {
        None => all,
        Some(after) => KeyRange {
            lo: all.lo,
            hi: keys.deployment(&after.to_bytes()),
        },
    };
    let mut snap = latest_snapshot(app.inner.runner.store()).await?;
    let pairs = Reads::scan(&mut snap, &range, limit + 1, true).await?;
    let mut items = pairs
        .iter()
        .map(|(_, v)| DeploymentRecord::decode(v))
        .collect::<Result<Vec<_>, _>>()?;
    let next = if items.len() > limit {
        items.truncate(limit);
        items.last().map(|r| r.id)
    } else {
        None
    };
    Ok(Page { items, next })
}

/// The function `name` as `app` serves it on this node: a `_system:*`
/// function, else a function of the active deployment. A function the
/// deployment does not export is [`LiveError::NotFound`]; while the active
/// deployment could not be loaded, its load error.
pub fn resolve(app: &AppEngine, name: &str) -> Result<Arc<dyn Function>, LiveError> {
    app.inner.serving.borrow().resolve(name)
}

/// The function `name` where no deployment serves: a `_system:*`
/// function, else [`LiveError::NotFound`].
pub fn resolve_system(name: &str) -> Result<Arc<dyn Function>, LiveError> {
    system::lookup(name).ok_or_else(|| not_deployed(name))
}

fn not_deployed(name: &str) -> LiveError {
    LiveError::NotFound(format!("function {name:?} is not deployed"))
}

/// The janitor's part (design §45 §3.3): keeps the newest
/// [`KEEP_DEPLOYMENTS`] records, the active deployment and its parent;
/// deletes every other record, then the bundles of the deleted records and
/// those no record names that are older than [`ORPHAN_BUNDLE_AGE`]. A kept
/// `pending` record older than [`PENDING_GRACE`] (by the store's clock) is
/// failed: its deploy crashed before activating (LV1 plan Task 7). A
/// deploy still running whose record is failed this way cannot activate it
/// any more.
pub async fn retain(app: &AppEngine) -> Result<Retained, LiveError> {
    retain_with(app, PENDING_GRACE).await
}

/// [`retain`] with another grace period for `pending` records (tests).
#[doc(hidden)]
pub async fn retain_with(app: &AppEngine, pending_grace: Duration) -> Result<Retained, LiveError> {
    let engine = &app.inner;
    let keys = app.app_keys().clone();
    let grace_ms = u64::try_from(pending_grace.as_millis()).unwrap_or(u64::MAX);
    let trimmed = engine
        .runner
        .store()
        .run(
            catalog_txn("live.deploy.retain", engine.runner.options()),
            move |txn| {
                let keys = keys.clone();
                Box::pin(async move { lift(trim_records(txn, &keys, grace_ms).await) })
            },
        )
        .await?
        .value?;
    let kept = trimmed.kept;
    let mut report = Retained {
        records_deleted: trimmed.deleted,
        bundles_deleted: 0,
        abandoned: trimmed.abandoned,
        validations_released: trimmed.released,
        // A failure here leaves the indexes `dropping` for the next round.
        indexes_removed: backfill::drop_indexes(&engine.runner)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(app = %engine.name, %error, "deleting dropped indexes failed");
                Vec::new()
            }),
    };
    for id in &report.abandoned {
        tracing::warn!(
            app = %engine.name,
            deployment = %id,
            "a deploy never activated; the janitor failed its pending record"
        );
    }
    for id in &report.validations_released {
        tracing::warn!(
            app = %engine.name,
            deployment = %id,
            "a deploy's validator checks or index backfills were abandoned; the janitor \
             released them"
        );
    }
    let objects = engine
        .runtime
        .bundles
        .list(&bundles_prefix(&engine.name))
        .await
        .map_err(|e| LiveError::Internal(format!("listing the app's bundles: {e}")))?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    for object in objects {
        let Some(id) = id_of_path(&engine.name, &object.path) else {
            continue;
        };
        let orphan_old = !kept.contains(&id)
            && now_ms.saturating_sub(id.timestamp_ms())
                > u64::try_from(ORPHAN_BUNDLE_AGE.as_millis()).unwrap_or(u64::MAX);
        if report.records_deleted.contains(&id) || orphan_old {
            engine
                .runtime
                .bundles
                .delete(&object.path)
                .await
                .map_err(|e| LiveError::Internal(format!("deleting bundle {id}: {e}")))?;
            report.bundles_deleted += 1;
        }
    }
    if !report.records_deleted.is_empty() || report.bundles_deleted > 0 {
        tracing::info!(
            app = %engine.name,
            records = report.records_deleted.len(),
            bundles = report.bundles_deleted,
            "the deployment janitor deleted old deployments"
        );
    }
    Ok(report)
}

/// What [`trim_records`] did.
struct Trimmed {
    deleted: Vec<Ulid>,
    kept: Vec<Ulid>,
    abandoned: Vec<Ulid>,
    released: Vec<Ulid>,
}

/// Releases the validator checks whose jobs' leases expired, deletes the
/// records retention does not keep, fails the kept `pending` ones at least
/// `grace_ms` old (by the transaction's start timestamp) and the
/// `validating` ones with no live check.
async fn trim_records(txn: &mut Txn, keys: &AppKeys, grace_ms: u64) -> Result<Trimmed, LiveError> {
    let now_ms = txn.start_ts().physical_ms();
    let checks = schema::release_abandoned(txn, keys, now_ms).await?;
    let active = read_pointer(txn, keys).await?;
    let records = Reads::scan(txn, &keys.deployments(), usize::MAX, true).await?;
    let records = records
        .iter()
        .map(|(_, v)| DeploymentRecord::decode(v))
        .collect::<Result<Vec<_>, _>>()?;
    let parent = active.and_then(|a| records.iter().find(|r| r.id == a)?.parent);
    let mut trimmed = Trimmed {
        deleted: Vec::new(),
        kept: Vec::new(),
        abandoned: Vec::new(),
        released: checks.released,
    };
    for (n, record) in records.into_iter().enumerate() {
        let key = keys.deployment(&record.id.to_bytes());
        if n < KEEP_DEPLOYMENTS || Some(record.id) == active || Some(record.id) == parent {
            trimmed.kept.push(record.id);
            if record.state == DeploymentState::Pending
                && now_ms.saturating_sub(record.created_at_ms) >= grace_ms
            {
                trimmed.abandoned.push(record.id);
                let failed = DeploymentRecord {
                    state: DeploymentState::Failed {
                        reason: ABANDONED.to_string(),
                    },
                    ..record
                };
                txn.put(&key, failed.encode()).await?;
            } else if matches!(
                record.state,
                DeploymentState::Validating | DeploymentState::Backfilling
            ) && !checks.live.contains(&record.id)
            {
                if !trimmed.released.contains(&record.id) {
                    trimmed.released.push(record.id);
                }
                let reason = if record.state == DeploymentState::Validating {
                    ABANDONED_VALIDATION
                } else {
                    ABANDONED_BACKFILL
                };
                let failed = DeploymentRecord {
                    state: DeploymentState::Failed {
                        reason: reason.to_string(),
                    },
                    ..record
                };
                txn.put(&key, failed.encode()).await?;
            }
        } else {
            txn.delete(&key).await?;
            trimmed.deleted.push(record.id);
        }
    }
    Ok(trimmed)
}

/// The deployment id of a bundle object of `app`, if the path is one.
fn id_of_path(app: &str, path: &str) -> Option<Ulid> {
    let name = path
        .strip_prefix(&bundles_prefix(app))?
        .strip_suffix(".js")?;
    Ulid::from_string(name).ok()
}

/// A deployed function: the deployment's id rides with it, in its
/// subscription key and in the call's [`CallCtx`](crate::CallCtx).
struct Deployed {
    id: Ulid,
    inner: Arc<dyn Function>,
}

impl Function for Deployed {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn kind(&self) -> FnKind {
        self.inner.kind()
    }

    fn visibility(&self) -> Visibility {
        self.inner.visibility()
    }

    fn deployment(&self) -> Ulid {
        self.id
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        let mut ctx = txn.ctx().clone();
        ctx.deployment = Some(self.id.to_string());
        txn.set_ctx(ctx);
        self.inner.call(txn, args)
    }
}

/// The conflict of a deploy that expected `expected` while `active` is
/// active.
fn conflict(expected: Option<Ulid>, active: Option<Ulid>) -> LiveError {
    let name =
        |id: Option<Ulid>| id.map_or_else(|| "no deployment".to_string(), |id| id.to_string());
    LiveError::DeployConflict(format!(
        "deploy conflict: the deploy expected {} to be active, but {} is",
        name(expected),
        name(active)
    ))
}

/// What a swap committed: the schema changes, the record as it now is,
/// and the commit timestamp.
struct Swapped {
    changes: SchemaChanges,
    record: DeploymentRecord,
    at: Ts,
}

/// Why a swap did not commit.
enum SwapError {
    /// The pointer names another deployment (or none).
    Conflict(Option<Ulid>),
    /// The record or the schema refused it; nothing was written.
    Refused(LiveError),
    /// A storage error, after the runner's retries.
    Storage(LiveError),
}

/// What a swap's transaction body decided.
enum Swap {
    Done(Box<(SchemaChanges, DeploymentRecord)>),
    Conflict(Option<Ulid>),
}

/// Steps 4 and 5: compare-and-swap the pointer against `expected`, apply
/// deployment `id`'s schema, and mark it active and the one it replaces
/// superseded, in one transaction. The record is read inside it: a new
/// deployment's must still be `pending` or `validating` (the janitor may
/// have failed it), an existing one's must not be. A refusal (the record's
/// state, a schema error) rolls the transaction back.
async fn swap(
    runner: &Runner,
    id: Ulid,
    expected: Option<Ulid>,
    path: Path,
) -> Result<Swapped, SwapError> {
    let keys = runner.app().clone();
    let limits = runner.limits().clone();
    let run = atomic(runner, "live.deploy.activate", |txn| {
        let keys = keys.clone();
        let limits = limits.clone();
        Box::pin(async move {
            let active = read_pointer(txn, &keys).await?;
            if active != expected {
                return Ok(Swap::Conflict(active));
            }
            let mut record = read_record(txn, &keys, id)
                .await?
                .ok_or_else(|| LiveError::NotFound(format!("deployment {id} does not exist")))?;
            check_state(&record, path)?;
            let changes = schema::apply(txn, &keys, &record.schema, &limits, id).await?;
            if let Some(previous) = active.filter(|p| *p != id) {
                set_state(txn, &keys, previous, |s| {
                    matches!(s, DeploymentState::Active | DeploymentState::Degraded)
                        .then_some(DeploymentState::Superseded)
                })
                .await?;
            }
            record.state = DeploymentState::Active;
            txn.put(&keys.deployment(&id.to_bytes()), record.encode())
                .await?;
            txn.put(&keys.deployment_pointer(), id.to_bytes().to_vec())
                .await?;
            Ok(Swap::Done(Box::new((changes, record))))
        })
    })
    .await;
    match run {
        Ok(committed) => match committed.value {
            Swap::Done(done) => {
                let (changes, record) = *done;
                Ok(Swapped {
                    changes,
                    record,
                    at: committed.commit_ts,
                })
            }
            Swap::Conflict(active) => Err(SwapError::Conflict(active)),
        },
        Err(Refusal::Own(err)) => Err(SwapError::Refused(err)),
        Err(Refusal::Storage(err)) => Err(SwapError::Storage(err)),
    }
}

/// The deployment the pointer names at `at` (the reactive checker's fresh
/// evaluations).
pub(crate) async fn active_at(runner: &Runner, at: Ts) -> Result<Option<Ulid>, LiveError> {
    let mut snap = snapshot(runner.store(), at).await?;
    read_pointer(&mut snap, runner.app()).await
}

/// Step 3: writes `record` (pending); returns its creation time, the
/// transaction's TSO physical time.
async fn write_pending(runner: &Runner, record: &DeploymentRecord) -> Result<u64, LiveError> {
    let keys = runner.app().clone();
    let record = record.clone();
    runner
        .store()
        .run(
            catalog_txn("live.deploy.pending", runner.options()),
            move |txn| {
                let keys = keys.clone();
                let mut record = record.clone();
                Box::pin(async move {
                    record.created_at_ms = txn.start_ts().physical_ms();
                    txn.put(&keys.deployment(&record.id.to_bytes()), record.encode())
                        .await?;
                    Ok(record.created_at_ms)
                })
            },
        )
        .await
        .map(|c| c.value)
        .map_err(LiveError::Txn)
}

/// Ends deployment `id`'s deploy after a failure, in one transaction:
/// with `release`, its validator checks ([`schema::release`]); with a
/// `reason`, its record `failed` if it is still `pending` or `validating`.
/// Logged, not returned, on a storage error (the deploy's own error is what
/// the caller needs; the janitor releases what is left).
async fn fail(runner: &Runner, id: Ulid, reason: Option<&str>, release: bool) {
    if reason.is_none() && !release {
        return;
    }
    let keys = runner.app().clone();
    let failed = reason.map(|reason| DeploymentState::Failed {
        reason: reason.to_string(),
    });
    let done = atomic(runner, "live.deploy.failed", |txn| {
        let keys = keys.clone();
        let failed = failed.clone();
        Box::pin(async move {
            if release {
                schema::release(txn, &keys, id).await?;
            }
            if let Some(failed) = failed {
                set_state(txn, &keys, id, |s| {
                    matches!(
                        s,
                        DeploymentState::Pending
                            | DeploymentState::Validating
                            | DeploymentState::Backfilling
                    )
                    .then(|| failed.clone())
                })
                .await?;
            }
            Ok(())
        })
    })
    .await;
    if let Err(Refusal::Own(error) | Refusal::Storage(error)) = done {
        tracing::warn!(deployment = %id, %error, "ending a failed deploy failed");
    }
}

/// Waits for a loaded deployment's degraded signal, then marks its record
/// degraded (if it is still the active one).
async fn watch_degraded(runner: Runner, app: String, id: Ulid, mut signal: watch::Receiver<bool>) {
    if signal.wait_for(|degraded| *degraded).await.is_err() {
        return;
    }
    let marked = transition(&runner, "live.deploy.degraded", id, |s| {
        (*s == DeploymentState::Active).then_some(DeploymentState::Degraded)
    })
    .await;
    match marked {
        Ok(true) => tracing::error!(
            %app,
            deployment = %id,
            "the deployment is degraded: its isolated workers crashed five times within a minute"
        ),
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(%app, deployment = %id, %error, "marking a degraded deployment failed")
        }
    }
}

/// Changes deployment `id`'s state to `next(state)` when that is `Some`;
/// returns whether it changed.
async fn transition<F>(
    runner: &Runner,
    op: &'static str,
    id: Ulid,
    next: F,
) -> Result<bool, LiveError>
where
    F: Fn(&DeploymentState) -> Option<DeploymentState> + Clone + Send + Sync + 'static,
{
    let keys = runner.app().clone();
    runner
        .store()
        .run(catalog_txn(op, runner.options()), move |txn| {
            let keys = keys.clone();
            let next = next.clone();
            Box::pin(async move { lift(set_state(txn, &keys, id, next).await) })
        })
        .await?
        .value
}

async fn set_state(
    txn: &mut Txn,
    keys: &AppKeys,
    id: Ulid,
    next: impl Fn(&DeploymentState) -> Option<DeploymentState>,
) -> Result<bool, LiveError> {
    let Some(mut record) = read_record(txn, keys, id).await? else {
        return Ok(false);
    };
    let Some(state) = next(&record.state) else {
        return Ok(false);
    };
    record.state = state;
    txn.put(&keys.deployment(&id.to_bytes()), record.encode())
        .await?;
    Ok(true)
}

async fn read_pointer(r: &mut impl Reads, keys: &AppKeys) -> Result<Option<Ulid>, LiveError> {
    match r.get(&keys.deployment_pointer()).await? {
        None => Ok(None),
        Some(bytes) => ulid_of(&bytes, "deployment pointer").map(Some),
    }
}

async fn read_record(
    r: &mut impl Reads,
    keys: &AppKeys,
    id: Ulid,
) -> Result<Option<DeploymentRecord>, LiveError> {
    match r.get(&keys.deployment(&id.to_bytes())).await? {
        None => Ok(None),
        Some(bytes) => DeploymentRecord::decode(&bytes).map(Some),
    }
}

fn ulid_of(bytes: &[u8], what: &str) -> Result<Ulid, LiveError> {
    let raw: [u8; 16] = bytes
        .try_into()
        .map_err(|_| LiveError::Corrupt(format!("{what}: {} bytes, expected 16", bytes.len())))?;
    Ok(Ulid::from_bytes(raw))
}

impl DeploymentState {
    /// The state's name, as the API spells it.
    pub fn name(&self) -> &'static str {
        match self {
            DeploymentState::Pending => "pending",
            DeploymentState::Validating => "validating",
            DeploymentState::Backfilling => "backfilling",
            DeploymentState::Active => "active",
            DeploymentState::Superseded => "superseded",
            DeploymentState::Failed { .. } => "failed",
            DeploymentState::Degraded => "degraded",
        }
    }

    fn to_proto(&self) -> pb::DeploymentState {
        match self {
            DeploymentState::Pending => pb::DeploymentState::DEPLOYMENT_STATE_PENDING,
            DeploymentState::Validating => pb::DeploymentState::DEPLOYMENT_STATE_VALIDATING,
            DeploymentState::Backfilling => pb::DeploymentState::DEPLOYMENT_STATE_BACKFILLING,
            DeploymentState::Active => pb::DeploymentState::DEPLOYMENT_STATE_ACTIVE,
            DeploymentState::Superseded => pb::DeploymentState::DEPLOYMENT_STATE_SUPERSEDED,
            DeploymentState::Failed { .. } => pb::DeploymentState::DEPLOYMENT_STATE_FAILED,
            DeploymentState::Degraded => pb::DeploymentState::DEPLOYMENT_STATE_DEGRADED,
        }
    }
}

impl DeploymentRecord {
    /// The catalog record.
    pub fn to_proto(&self) -> pb::DeploymentRecord {
        pb::DeploymentRecord {
            format: 1,
            id: self.id.to_bytes().to_vec(),
            bundle_sha256: self.bundle_sha256.to_vec(),
            schema: buffa::MessageField::some(self.schema.to_proto()),
            created_at_ms: self.created_at_ms,
            created_by: self.created_by.clone(),
            message: self.message.clone(),
            parent: self
                .parent
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default(),
            state: self.state.to_proto().into(),
            failure_reason: match &self.state {
                DeploymentState::Failed { reason } => reason.clone(),
                _ => String::new(),
            },
            ..Default::default()
        }
    }

    /// The record of a catalog record.
    pub fn from_proto(p: pb::DeploymentRecord) -> Result<Self, LiveError> {
        if p.format != 1 {
            return Err(LiveError::Corrupt(format!(
                "deployment record format {} (expected 1)",
                p.format
            )));
        }
        let state = match p.state.as_known() {
            Some(pb::DeploymentState::DEPLOYMENT_STATE_PENDING) => DeploymentState::Pending,
            Some(pb::DeploymentState::DEPLOYMENT_STATE_VALIDATING) => DeploymentState::Validating,
            Some(pb::DeploymentState::DEPLOYMENT_STATE_BACKFILLING) => DeploymentState::Backfilling,
            Some(pb::DeploymentState::DEPLOYMENT_STATE_ACTIVE) => DeploymentState::Active,
            Some(pb::DeploymentState::DEPLOYMENT_STATE_SUPERSEDED) => DeploymentState::Superseded,
            Some(pb::DeploymentState::DEPLOYMENT_STATE_FAILED) => DeploymentState::Failed {
                reason: p.failure_reason,
            },
            Some(pb::DeploymentState::DEPLOYMENT_STATE_DEGRADED) => DeploymentState::Degraded,
            _ => {
                return Err(LiveError::Corrupt(format!(
                    "deployment record: unknown state {:?}",
                    p.state
                )));
            }
        };
        let sha: [u8; 32] = p.bundle_sha256.as_slice().try_into().map_err(|_| {
            LiveError::Corrupt(format!(
                "deployment record: a {}-byte bundle hash",
                p.bundle_sha256.len()
            ))
        })?;
        Ok(DeploymentRecord {
            id: ulid_of(&p.id, "deployment record id")?,
            bundle_sha256: sha,
            schema: p
                .schema
                .as_option()
                .map(Schema::from_proto)
                .transpose()
                .map_err(|e| LiveError::Corrupt(format!("deployment record: {e}")))?
                .unwrap_or_default(),
            created_at_ms: p.created_at_ms,
            created_by: p.created_by,
            message: p.message,
            parent: if p.parent.is_empty() {
                None
            } else {
                Some(ulid_of(&p.parent, "deployment record parent")?)
            },
            state,
        })
    }

    fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let p = pb::DeploymentRecord::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("deployment record: {e}")))?;
        DeploymentRecord::from_proto(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(state: DeploymentState) -> DeploymentRecord {
        DeploymentRecord {
            id: Ulid::from_parts(1_700_000_000_000, 42),
            bundle_sha256: [7; 32],
            schema: Schema::default(),
            created_at_ms: 1_700_000_000_123,
            created_by: "alice".into(),
            message: "first".into(),
            parent: Some(Ulid::from_parts(1_600_000_000_000, 1)),
            state,
        }
    }

    #[test]
    fn records_round_trip_in_every_state() {
        for state in [
            DeploymentState::Pending,
            DeploymentState::Validating,
            DeploymentState::Backfilling,
            DeploymentState::Active,
            DeploymentState::Superseded,
            DeploymentState::Failed {
                reason: "conflict".into(),
            },
            DeploymentState::Degraded,
        ] {
            let r = record(state);
            assert_eq!(DeploymentRecord::decode(&r.encode()).expect("decodes"), r);
        }
        let first = DeploymentRecord {
            parent: None,
            ..record(DeploymentState::Active)
        };
        assert_eq!(
            DeploymentRecord::decode(&first.encode()).expect("decodes"),
            first
        );
    }

    #[test]
    fn corrupt_records_are_refused() {
        let mut p = record(DeploymentState::Active).to_proto();
        p.format = 2;
        assert!(DeploymentRecord::from_proto(p).is_err());
        let mut p = record(DeploymentState::Active).to_proto();
        p.bundle_sha256.pop();
        assert!(DeploymentRecord::from_proto(p).is_err());
        let mut p = record(DeploymentState::Active).to_proto();
        p.id = vec![1, 2, 3];
        assert!(DeploymentRecord::from_proto(p).is_err());
        let mut p = record(DeploymentState::Active).to_proto();
        p.state = pb::DeploymentState::DEPLOYMENT_STATE_UNSPECIFIED.into();
        assert!(DeploymentRecord::from_proto(p).is_err());
    }

    #[test]
    fn bundle_paths_name_the_app_and_the_id() {
        let id = Ulid::from_parts(1_700_000_000_000, 42);
        let path = bundle_path("chat", id);
        assert_eq!(path, format!("live/chat/deployments/{id}.js"));
        assert_eq!(id_of_path("chat", &path), Some(id));
        assert_eq!(id_of_path("other", &path), None);
        assert_eq!(id_of_path("chat", "live/chat/deployments/x.js"), None);
    }

    #[test]
    fn conflicts_name_both_deployments() {
        let a = Ulid::from_parts(1, 1);
        let err = conflict(Some(a), None);
        assert_eq!(err.reason(), Some("live_deploy_conflict"));
        assert_eq!(err.code(), pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION);
        assert!(err.to_string().contains(&a.to_string()), "{err}");
        assert!(err.to_string().contains("no deployment"), "{err}");
    }
}
