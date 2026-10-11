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
//! 4. and 5., in one transaction: compare-and-swap the pointer against
//!    `expected_active` (a mismatch fails the deployment with
//!    `FAILED_PRECONDITION`, reason `live_deploy_conflict`), apply the
//!    schema ([`schema::apply`], additive table creation only until Tasks 8
//!    and 9), mark the deployment `active` and the one it replaces
//!    `superseded`. A failure at 4 or 5 marks the record `failed` with the
//!    reason;
//! 6. the `catalog` journal entry that switches every node at one tick is
//!    Task 11's. Until then a node picks up another node's deploy when it
//!    [refreshes](AppEngine::refresh) (the server does each janitor round).
//!
//! The `_system:*` functions resolve before any deployment ([`resolve`]),
//! so no bundle can shadow them. [`retain`] is the janitor's part: it keeps
//! the newest [`KEEP_DEPLOYMENTS`] deployments, the active one and its
//! parent, and deletes the rest with their bundles.
//!
//! The runtime lives in `loams-live-js`, which depends on this crate, so
//! it is reached through [`BundleLoader`] and [`LoadedBundle`]. A bundle
//! served by isolated workers reports when its workers crashed five times
//! within a minute (design §45 §3.1); the engine then marks the
//! deployment's record [`DeploymentState::Degraded`]. Its calls are still
//! served.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use buffa::Message;
use bytes::Bytes;
use futures::future::BoxFuture;
use loams_kv::{Txn, TxnError};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use ulid::Ulid;

use crate::docs::Reads;
use crate::keys::{AppKeys, KeyRange};
use crate::schema::{self, Schema, SchemaChanges};
use crate::txn::{catalog_txn, latest_snapshot, lift};
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
    serving: RwLock<Serving>,
    /// Serializes changing what this node serves (its deploys' activations
    /// and its refreshes), so an older deployment never replaces a newer
    /// one here.
    switch: tokio::sync::Mutex<()>,
    /// Deployment ids, monotonic on this node.
    ids: Mutex<ulid::Generator>,
}

/// What this node serves.
#[derive(Clone, Default)]
enum Serving {
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
                serving: RwLock::default(),
                switch: tokio::sync::Mutex::default(),
                ids: Mutex::new(ulid::Generator::new()),
            }),
        };
        // Nothing else holds the engine yet, so `switch` needs no lock.
        if let Some(id) = engine.read_active().await? {
            // A load failure is logged and kept as the deployment's error.
            let _ = engine.serve_id(id).await;
        }
        Ok(engine)
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
        match self.serving() {
            Serving::Loaded(l) => Some(l.id),
            Serving::Nothing | Serving::Failed { .. } => None,
        }
    }

    /// The active deployment as the catalog names it now.
    pub async fn read_active(&self) -> Result<Option<Ulid>, LiveError> {
        let mut snap = latest_snapshot(self.inner.runner.store()).await?;
        read_pointer(&mut snap, self.app_keys()).await
    }

    /// Loads the deployment the pointer names, if this node does not serve
    /// it yet, and serves it. If it cannot be loaded (for example its
    /// bundle does not match its record's SHA-256), the error is logged and
    /// returned, and the deployment this node served before keeps serving.
    pub async fn refresh(&self) -> Result<Option<Ulid>, LiveError> {
        let _switch = self.inner.switch.lock().await;
        let pointer = self.read_active().await.inspect_err(|error| {
            tracing::warn!(
                app = %self.inner.name,
                %error,
                "reading the deployment pointer failed; this node keeps serving what it served"
            );
        })?;
        match pointer {
            Some(id) => self.serve_id(id).await,
            None => Ok(None),
        }
    }

    /// Serves deployment `id`, loading it unless it is served already. The
    /// caller holds `switch`.
    async fn serve_id(&self, id: Ulid) -> Result<Option<Ulid>, LiveError> {
        if self.active() == Some(id) {
            return Ok(Some(id));
        }
        match self.load(id).await {
            Ok(loaded) => {
                self.serve(Serving::Loaded(loaded));
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
                if self.active().is_none() {
                    self.serve(Serving::Failed {
                        error: error.clone(),
                    });
                }
                Err(error)
            }
        }
    }

    fn app_keys(&self) -> &AppKeys {
        self.inner.runner.app()
    }

    fn serving(&self) -> Serving {
        self.inner
            .serving
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn serve(&self, serving: Serving) {
        *self
            .inner
            .serving
            .write()
            .unwrap_or_else(PoisonError::into_inner) = serving;
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

    // 4. and 5. Apply the schema and swap the pointer, in one transaction.
    let switch = engine.switch.lock().await;
    match activate(&engine.runner, &record).await {
        Ok(changes) => {
            app.serve(Serving::Loaded(app.loaded(id, loaded)));
            drop(switch);
            tracing::info!(app = %engine.name, deployment = %id, by, "deployed");
            record.state = DeploymentState::Active;
            Ok(DeployOutcome {
                record,
                changes,
                dry_run: false,
            })
        }
        Err(Activation::Conflict(active)) => {
            drop(switch);
            let err = conflict(req.expected_active, active);
            mark_failed(&engine.runner, id, &err.to_string()).await;
            // This node's view was stale: serve what is active.
            let _ = app.refresh().await;
            Err(err)
        }
        Err(Activation::Failed(err)) => {
            drop(switch);
            mark_failed(&engine.runner, id, &err.to_string()).await;
            Err(err)
        }
        // The outcome may be unknown: the record stays `pending` unless
        // the activation committed. If it did, the refresh serves it now.
        Err(Activation::Storage(err)) => {
            drop(switch);
            let _ = app.refresh().await;
            Err(err)
        }
    }
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
    if let Some(f) = system::lookup(name) {
        return Ok(f);
    }
    let not_deployed = || not_deployed(name);
    match app.serving() {
        Serving::Nothing => Err(not_deployed()),
        Serving::Failed { error, .. } => Err(error),
        Serving::Loaded(loaded) => {
            let inner = loaded.bundle.function(name).ok_or_else(not_deployed)?;
            Ok(Arc::new(Deployed {
                id: loaded.id,
                inner,
            }))
        }
    }
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
/// those no record names that are older than [`ORPHAN_BUNDLE_AGE`].
pub async fn retain(app: &AppEngine) -> Result<Retained, LiveError> {
    let engine = &app.inner;
    let keys = app.app_keys().clone();
    let (deleted, kept) = engine
        .runner
        .store()
        .run(
            catalog_txn("live.deploy.retain", engine.runner.options()),
            move |txn| {
                let keys = keys.clone();
                Box::pin(async move { lift(trim_records(txn, &keys).await) })
            },
        )
        .await?
        .value?;
    let mut report = Retained {
        records_deleted: deleted,
        bundles_deleted: 0,
    };
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

/// Deletes the records retention does not keep; returns the deleted ids
/// and the kept ones.
async fn trim_records(txn: &mut Txn, keys: &AppKeys) -> Result<(Vec<Ulid>, Vec<Ulid>), LiveError> {
    let active = read_pointer(txn, keys).await?;
    let records = Reads::scan(txn, &keys.deployments(), usize::MAX, true).await?;
    let records = records
        .iter()
        .map(|(_, v)| DeploymentRecord::decode(v))
        .collect::<Result<Vec<_>, _>>()?;
    let parent = active.and_then(|a| records.iter().find(|r| r.id == a)?.parent);
    let (mut deleted, mut kept) = (Vec::new(), Vec::new());
    for (n, record) in records.iter().enumerate() {
        if n < KEEP_DEPLOYMENTS || Some(record.id) == active || Some(record.id) == parent {
            kept.push(record.id);
        } else {
            txn.delete(&keys.deployment(&record.id.to_bytes())).await?;
            deleted.push(record.id);
        }
    }
    Ok((deleted, kept))
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

/// Why an activation did not commit.
enum Activation {
    /// The pointer names another deployment (or none).
    Conflict(Option<Ulid>),
    /// The schema could not apply; nothing was written.
    Failed(LiveError),
    /// A storage error, after the runner's retries.
    Storage(LiveError),
}

/// Steps 4 and 5: compare-and-swap the pointer, apply the schema, and mark
/// the deployment active and the one it replaces superseded, in one
/// transaction. A schema error rolls the transaction back.
async fn activate(runner: &Runner, record: &DeploymentRecord) -> Result<SchemaChanges, Activation> {
    let failure: Arc<Mutex<Option<LiveError>>> = Arc::default();
    let keys = runner.app().clone();
    let limits = runner.limits().clone();
    let body_failure = failure.clone();
    let record = record.clone();
    let run = runner
        .store()
        .run(
            catalog_txn("live.deploy.activate", runner.options()),
            move |txn| {
                let keys = keys.clone();
                let limits = limits.clone();
                let record = record.clone();
                let failure = body_failure.clone();
                Box::pin(async move {
                    let attempt = async {
                        let active = read_pointer(txn, &keys).await?;
                        if active != record.parent {
                            return Ok::<_, LiveError>(Err(active));
                        }
                        let changes = schema::apply(txn, &keys, &record.schema, &limits).await?;
                        if let Some(previous) = active {
                            set_state(txn, &keys, previous, |s| {
                                matches!(s, DeploymentState::Active | DeploymentState::Degraded)
                                    .then_some(DeploymentState::Superseded)
                            })
                            .await?;
                        }
                        let mut activated = record.clone();
                        activated.state = DeploymentState::Active;
                        txn.put(&keys.deployment(&record.id.to_bytes()), activated.encode())
                            .await?;
                        txn.put(&keys.deployment_pointer(), record.id.to_bytes().to_vec())
                            .await?;
                        Ok(Ok(changes))
                    };
                    match attempt.await {
                        Ok(done) => Ok(done),
                        Err(e) => match e.into_txn() {
                            Err(storage) => Err(storage),
                            Ok(own) => {
                                *failure.lock().unwrap_or_else(PoisonError::into_inner) = Some(own);
                                Err(TxnError::Fatal("the schema could not apply".into()))
                            }
                        },
                    }
                })
            },
        )
        .await;
    match run {
        Ok(committed) => committed.value.map_err(Activation::Conflict),
        Err(e) => {
            let own = failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            Err(own.map_or_else(
                || Activation::Storage(LiveError::Txn(e)),
                Activation::Failed,
            ))
        }
    }
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

/// Marks a pending deployment failed with `reason`; logged, not returned,
/// on a storage error (the deploy's own error is what the caller needs).
async fn mark_failed(runner: &Runner, id: Ulid, reason: &str) {
    let failed = DeploymentState::Failed {
        reason: reason.to_string(),
    };
    let marked = transition(runner, "live.deploy.failed", id, move |s| {
        (*s == DeploymentState::Pending).then(|| failed.clone())
    })
    .await;
    if let Err(error) = marked {
        tracing::warn!(deployment = %id, %error, "marking a failed deployment failed");
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
