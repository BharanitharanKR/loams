//! Many apps per node (design §45 §7, D690; LV1 plan Task 13).
//!
//! [`Engines`] starts an app's [`AppRuntime`] (its store, runner,
//! [`AppEngine`], subscription manager, sessions and janitor) on the first
//! request that addresses it ([`Engines::resolve`]), and stops it after
//! [`idle_after`](crate::LiveConfig::idle_after) with no session and no
//! request, so a node's memory follows its active apps. The server's own
//! app (`--live-app`) starts with the server and always runs.
//!
//! A request names its app with the `loams-live-app` header
//! ([`APP_HEADER`](crate::apps::APP_HEADER)); without one it addresses the
//! server's own app. A disabled or deleted app refuses every call
//! (`live_app_disabled`). Each node polls the directory's change counter
//! every second ([`Apps::sync`]): when it moved, the node stops the engines
//! of apps that are no longer active, or whose limits changed (they start
//! again with the new ones). The directory janitor purges deleted apps
//! once their retention ends, with their bundles.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use loams_kv::Store;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::apps::{AppRecord, AppState, Apps, Keyspaces};
use crate::deploy::{self, AppEngine, Runtime};
use crate::session::Sessions;
use crate::subs::{SubsConfig, Subscriptions};
use crate::{Janitor, LiveConfig, LiveError, Runner, catalog};

/// How often a node reads the directory's change counter.
pub const DIRECTORY_POLL: Duration = Duration::from_secs(1);

/// One app running on this node.
pub struct AppRuntime {
    record: AppRecord,
    store: Store,
    runner: Runner,
    engine: AppEngine,
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    stop: CancellationToken,
    janitor: Mutex<Option<JoinHandle<()>>>,
    last_used: Mutex<Instant>,
}

impl std::fmt::Debug for AppRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppRuntime")
            .field("app", &self.record.name)
            .field("keyspace", &self.record.keyspace)
            .finish_non_exhaustive()
    }
}

impl AppRuntime {
    /// The app's directory record, as it was when the engine started.
    pub fn record(&self) -> &AppRecord {
        &self.record
    }

    /// The app's name.
    pub fn name(&self) -> &str {
        &self.record.name
    }

    /// The app's store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The app's runner.
    pub fn runner(&self) -> &Runner {
        &self.runner
    }

    /// The app's engine (its active deployment).
    pub fn engine(&self) -> &AppEngine {
        &self.engine
    }

    /// The app's subscription manager.
    pub fn subs(&self) -> &Arc<Subscriptions> {
        &self.subs
    }

    /// The app's sessions on this node.
    pub fn sessions(&self) -> &Sessions {
        &self.sessions
    }

    /// The token that stops the app's tasks (a child of the server's).
    pub fn stopping(&self) -> &CancellationToken {
        &self.stop
    }

    fn touch(&self) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed()
    }

    /// Opens the app's store and runner (with its limits), loads its active
    /// deployment, and starts its subscription manager, sessions and
    /// janitor under `parent`.
    async fn start(
        template: &LiveConfig,
        keyspaces: &Keyspaces,
        record: AppRecord,
        runtime: Runtime,
        parent: &CancellationToken,
    ) -> Result<Arc<AppRuntime>, LiveError> {
        let config = LiveConfig {
            app: record.name.clone(),
            store: keyspaces.config(&record.keyspace),
            limits: record.settings.limits.apply(&template.limits),
            ..template.clone()
        };
        let store = keyspaces.open(&record.keyspace).await?;
        let runner = Runner::open(store.clone(), &config).await?;
        let engine = AppEngine::open(&config.app, runner.clone(), runtime).await?;
        let stop = parent.child_token();
        let subs_config = SubsConfig {
            consumer: config
                .subs
                .consumer
                .clone()
                .or_else(|| Some(config.node.clone())),
            ..config.subs.clone()
        };
        // The manager follows the engine: at an activation's tick every
        // subscription reruns on the new deployment (LV1 plan Task 7).
        let subs = Arc::new(Subscriptions::spawn_for(&engine, subs_config, stop.clone()));
        let resolver = engine.clone();
        let sessions = Sessions::new(
            subs.clone(),
            Arc::new(move |name: &str| deploy::resolve(&resolver, name)),
            config.session.clone(),
            config.node.clone(),
            stop.clone(),
        );
        let janitor = tokio::spawn(janitor_loop(
            engine.clone(),
            config.janitor_interval,
            stop.clone(),
        ));
        tracing::info!(app = %record.name, keyspace = %record.keyspace, "started a Live app engine");
        Ok(Arc::new(AppRuntime {
            record,
            store,
            runner,
            engine,
            subs,
            sessions,
            stop,
            janitor: Mutex::new(Some(janitor)),
            last_used: Mutex::new(Instant::now()),
        }))
    }

    /// Stops the app's manager, sessions and janitor, and waits for the
    /// janitor.
    async fn shut(&self) {
        self.stop.cancel();
        let janitor = self
            .janitor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(janitor) = janitor {
            let _ = janitor.await;
        }
    }
}

/// Runs the journal janitor and the deployment janitor every `interval`
/// (the first run one interval after start), and refreshes the deployment
/// this node serves: a backstop for a deploy made on another node, which
/// the subscription manager already picks up at the activation's tick
/// from its catalog journal entry (LV1 plan Task 11).
async fn janitor_loop(engine: AppEngine, interval: Duration, stop: CancellationToken) {
    let runner = engine.runner().clone();
    let mut every = tokio::time::interval_at(Instant::now() + interval, interval);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let run = async {
            let journal = runner.journal().await?;
            Janitor::new(runner.store().clone(), journal)
                .run_once()
                .await
        };
        match run.await {
            Ok(report) => tracing::debug!(?report, "the Live journal janitor ran"),
            Err(e) => tracing::warn!(error = %e, "the Live journal janitor failed; retrying"),
        }
        // A refresh logs its own failures; the next round retries.
        let _ = engine.refresh().await;
        if let Err(e) = deploy::retain(&engine).await {
            tracing::warn!(error = %e, "the Live deployment janitor failed; retrying");
        }
    }
}

type Slot = Arc<tokio::sync::OnceCell<Arc<AppRuntime>>>;

/// The apps this node serves. Cheap to clone.
#[derive(Clone)]
pub struct Engines {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Engines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engines")
            .field("default_app", &self.inner.default_app)
            .field("running", &self.running())
            .finish_non_exhaustive()
    }
}

struct Inner {
    apps: Apps,
    template: LiveConfig,
    runtime: Runtime,
    default_app: String,
    default: Arc<AppRuntime>,
    running: Mutex<HashMap<String, Slot>>,
    stop: CancellationToken,
    background: Mutex<Vec<JoinHandle<()>>>,
}

impl Engines {
    /// Opens the app directory beside `config.store` (its system keyspace,
    /// [`config.layout`](LiveConfig::layout)), adopts the server's own app
    /// `config.app` into it with the keyspace it uses, starts that app's
    /// engine, and starts the idle reaper, the directory poll and the
    /// directory janitor, all until `stop`.
    pub async fn start(
        config: LiveConfig,
        runtime: Runtime,
        stop: CancellationToken,
    ) -> Result<Engines, LiveError> {
        let keyspaces = Keyspaces::new(config.store.clone(), config.layout);
        let apps = Apps::open(keyspaces.clone(), config.apps.clone()).await?;
        let record = apps.adopt(&config.app, config.store.keyspace()).await?;
        let default =
            AppRuntime::start(&config, &keyspaces, record, runtime.clone(), &stop).await?;
        let default_app = config.app.clone();
        let mut running = HashMap::new();
        running.insert(
            default_app.clone(),
            Arc::new(tokio::sync::OnceCell::new_with(Some(default.clone()))),
        );
        let engines = Engines {
            inner: Arc::new(Inner {
                apps,
                template: config,
                runtime,
                default_app,
                default,
                running: Mutex::new(running),
                stop,
                background: Mutex::new(Vec::new()),
            }),
        };
        let tasks = vec![
            tokio::spawn(reap_loop(engines.clone())),
            tokio::spawn(directory_loop(engines.clone())),
            tokio::spawn(directory_janitor_loop(engines.clone())),
        ];
        *engines
            .inner
            .background
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = tasks;
        Ok(engines)
    }

    /// The app directory.
    pub fn apps(&self) -> &Apps {
        &self.inner.apps
    }

    /// The server's own app (`--live-app`).
    pub fn default_app(&self) -> &str {
        &self.inner.default_app
    }

    /// The server's own app's engine, which always runs.
    pub fn default_runtime(&self) -> &Arc<AppRuntime> {
        &self.inner.default
    }

    fn running_map(&self) -> std::sync::MutexGuard<'_, HashMap<String, Slot>> {
        self.inner
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The apps whose engines run on this node, by name.
    pub fn running(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .running_map()
            .iter()
            .filter(|(_, slot)| slot.initialized())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    /// The engine of the app a request addresses: `app` (the
    /// `loams-live-app` header) or, without it, the server's own app;
    /// started when it does not run here yet. Refused for an unknown app
    /// (`NOT_FOUND`) and a disabled or deleted one (`live_app_disabled`).
    pub async fn resolve(&self, app: Option<&str>) -> Result<Arc<AppRuntime>, LiveError> {
        let name = app.unwrap_or(&self.inner.default_app);
        catalog::check_name("app", name)?;
        let record = self
            .inner
            .apps
            .get_by_name(name)
            .await?
            .ok_or_else(|| LiveError::NotFound(format!("no Live app is named {name}")))?;
        match record.state {
            AppState::Active => {}
            AppState::Disabled | AppState::Deleting { .. } => {
                return Err(LiveError::AppDisabled(format!(
                    "the Live app {name} is {}",
                    record.state.name()
                )));
            }
        }
        let slot = self
            .running_map()
            .entry(name.to_string())
            .or_default()
            .clone();
        let runtime = slot
            .get_or_try_init(|| {
                AppRuntime::start(
                    &self.inner.template,
                    self.inner.apps.keyspaces(),
                    record,
                    self.inner.runtime.clone(),
                    &self.inner.stop,
                )
            })
            .await?
            .clone();
        runtime.touch();
        Ok(runtime)
    }

    /// Stops the engine of `name` on this node, if it runs (never the
    /// server's own app's).
    pub async fn stop_app(&self, name: &str) {
        if name == self.inner.default_app {
            return;
        }
        let slot = self.running_map().remove(name);
        if let Some(runtime) = slot.as_ref().and_then(|s| s.get()) {
            runtime.shut().await;
            tracing::info!(app = %name, "stopped a Live app engine");
        }
    }

    /// Stops every engine (but the server's own app's) idle for
    /// [`idle_after`](LiveConfig::idle_after): no session, no request in
    /// flight and none since. Returns the apps it stopped.
    pub async fn reap_idle(&self) -> Vec<String> {
        let idle_after = self.inner.template.idle_after;
        let idle: Vec<String> = self
            .running_map()
            .iter()
            .filter(|(name, _)| **name != self.inner.default_app)
            .filter_map(|(name, slot)| {
                let runtime = slot.get()?;
                // The slot holds one reference; a request holds another.
                let unused = Arc::strong_count(runtime) == 1
                    && runtime.sessions.is_empty()
                    && runtime.idle_for() >= idle_after;
                unused.then(|| name.clone())
            })
            .collect();
        for name in &idle {
            self.stop_app(name).await;
        }
        idle
    }

    /// Brings this node in line with the directory: stops the engines of
    /// apps that are gone, no longer active, or whose limits changed.
    pub async fn reconcile(&self) -> Result<(), LiveError> {
        let running: Vec<(String, AppRecord)> = self
            .running_map()
            .iter()
            .filter_map(|(name, slot)| Some((name.clone(), slot.get()?.record.clone())))
            .collect();
        for (name, was) in running {
            let now = self.inner.apps.get_by_name(&name).await?;
            let keep = now.is_some_and(|now| {
                now.id == was.id
                    && now.state == AppState::Active
                    && now.settings.limits == was.settings.limits
            });
            if !keep {
                self.stop_app(&name).await;
            }
        }
        Ok(())
    }

    /// One directory janitor round: purges the deleted apps whose
    /// retention ended (and their bundles) and reports orphan keyspaces.
    pub async fn janitor_once(&self) -> Result<crate::apps::DirectoryReport, LiveError> {
        let report = self.inner.apps.janitor_once().await?;
        self.purge_bundles(&report).await;
        Ok(report)
    }

    /// [`janitor_once`](Self::janitor_once) at `now_ms` (tests).
    pub async fn janitor_at(&self, now_ms: u64) -> Result<crate::apps::DirectoryReport, LiveError> {
        let report = self.inner.apps.janitor_at(now_ms).await?;
        self.purge_bundles(&report).await;
        Ok(report)
    }

    async fn purge_bundles(&self, report: &crate::apps::DirectoryReport) {
        let bundles = &self.inner.runtime.bundles;
        for record in &report.purged {
            self.stop_app(&record.name).await;
            let prefix = deploy::bundles_prefix(&record.name);
            let objects = match bundles.list(&prefix).await {
                Ok(objects) => objects,
                Err(e) => {
                    tracing::warn!(app = %record.name, error = %e, "listing a purged app's bundles failed");
                    continue;
                }
            };
            for object in objects {
                if let Err(e) = bundles.delete(&object.path).await {
                    tracing::warn!(app = %record.name, error = %e, "deleting a purged app's bundle failed");
                }
            }
        }
    }

    /// Stops every engine and background task, and waits for them.
    pub async fn shutdown(&self) {
        self.inner.stop.cancel();
        let tasks = std::mem::take(
            &mut *self
                .inner
                .background
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for task in tasks {
            let _ = task.await;
        }
        let slots: Vec<Slot> = self.running_map().drain().map(|(_, s)| s).collect();
        for slot in slots {
            if let Some(runtime) = slot.get() {
                runtime.shut().await;
            }
        }
        self.inner.default.shut().await;
    }
}

/// Stops idle engines, checking a few times per idle period (at most every
/// minute).
async fn reap_loop(engines: Engines) {
    let period = (engines.inner.template.idle_after / 4)
        .clamp(Duration::from_millis(50), Duration::from_secs(60));
    let mut every = tokio::time::interval_at(Instant::now() + period, period);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = engines.inner.stop.cancelled() => return,
            _ = every.tick() => {}
        }
        engines.reap_idle().await;
    }
}

/// Polls the directory's change counter, and reconciles when it moved.
async fn directory_loop(engines: Engines) {
    let mut every = tokio::time::interval(DIRECTORY_POLL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = engines.inner.stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let synced = async {
            if engines.inner.apps.sync().await? {
                engines.reconcile().await?;
            }
            Ok::<_, LiveError>(())
        };
        if let Err(e) = synced.await {
            tracing::warn!(error = %e, "reading the Live app directory failed; retrying");
        }
    }
}

/// Runs the directory janitor every journal janitor interval.
async fn directory_janitor_loop(engines: Engines) {
    let interval = engines.inner.template.janitor_interval;
    let mut every = tokio::time::interval_at(Instant::now() + interval, interval);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = engines.inner.stop.cancelled() => return,
            _ = every.tick() => {}
        }
        match engines.janitor_once().await {
            Ok(report) => tracing::debug!(?report, "the Live app directory janitor ran"),
            Err(e) => tracing::warn!(error = %e, "the Live app directory janitor failed; retrying"),
        }
    }
}
