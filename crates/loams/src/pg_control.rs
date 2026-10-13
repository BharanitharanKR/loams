//! The `pg-control` role (design §46 §6.1; PG2 Task 9): `loams pg-control`,
//! Loams Postgres's control plane in one process, behind the feature
//! `postgres`.
//!
//! It runs, over one [`PgControlStore`](loams_pg_control::PgControlStore):
//!
//! - the **API listener** (`--listen`): `loams.postgres.v1.PostgresService`
//!   and `loams.operations.v1`'s reads of its operations
//!   (`api::connect_postgres`), every call authenticated, authorized and
//!   audited; `grpc.health.v1`, and `grpc.reflection.v1` with
//!   `--reflection`;
//! - the **upcall listener** (`--pg-upcall-listen`): the routes Neon's
//!   components call ([`UpcallServer`]), never on the API listener;
//! - the **reconciler** of projects, branches and endpoints
//!   ([`Reconciler`]); with `--runtime compose` it drives computes through
//!   the compose runtime ([`ComputesConfig`], Task 11), or through a
//!   compute runner that holds the container runtime's socket instead of
//!   this process (`--compute-runner`, [`PgRunner`], R-REL.1), each compute
//!   running the pinned image of its project's Postgres major, and
//!   suspends the computes that sit idle for their endpoint's suspend
//!   timeout (Task 12; no PgDog fronts them until PG2c, so their activity
//!   is `compute_ctl`'s alone), its lifecycle events counted by the default
//!   observer;
//! - **upkeep** on a timer: the idempotency ledger's expired entries and
//!   finished operations past their retention are removed.
//!
//! **One signing key** (R8.9): every instance reads `pg-control`'s Ed25519
//! key from the secret store ([`SIGNING_KEY_REF`]), and the first start
//! creates it there, so every instance, before and after a restart,
//! accepts the tokens any of them issued. The secret store is the file
//! store under `--data-dir` with `--runtime compose` (single node), and
//! Kubernetes Secrets with `--runtime kubernetes`.
//!
//! **Who may call** (`--pg-authz`): `dev` (the default) answers every call
//! as one principal and refuses to start unless `--listen` is loopback;
//! `api-key` checks `authorization: Bearer lk_…` against `--pg-api-keys`,
//! whose grants it authorizes with; `openfga` waits for MT1.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router as AxumRouter;
use loams_pg_control::audit::{AuditSink, LogAuditSink};
use loams_pg_control::authz::{
    ApiKeyAuthenticator, Authenticator, Authorizer, DevAuthorizer, DevLoopbackAuthenticator,
    GrantsAuthorizer, KeysFile,
};
use loams_pg_control::neon::NeonClients;
use loams_pg_control::reconcile::endpoint::{Computes, HttpComputeCtl};
use loams_pg_control::reconcile::{Reconciler, ReconcilerConfig};
use loams_pg_control::runtime::compose::{
    ComposeBin, ComposeConfig, ComposeRuntime, CtlReach, TokioCommandRunner,
};
use loams_pg_control::runtime::remote::RemoteRuntime;
use loams_pg_control::runtime::runner::{RunnerPolicy, RunnerServer};
use loams_pg_control::runtime::{ComputeRuntime, images};
use loams_pg_control::secrets::{FileSecretStore, KeySource, Secret, SecretRef, SecretStore};
use loams_pg_control::service::operations::OPERATION_RETENTION;
use loams_pg_control::service::{Clock, PgService, ServiceConfig};
use loams_pg_control::upcall::{
    ComputeCtlConfigurer, ComputeKeys, SIGNING_KEY_REF, SpecSources, UpcallConfig, UpcallServer,
    load_or_create_signing_key,
};
use loams_pg_control::{KvControlStore, StoreOptions};
use loams_postgres::pageserver::{NeonClient, NeonEndpoints};
use loams_postgres::wal::WalClient;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::api::connect_postgres::{self, Gate};

/// `--listen`'s default: the API listener.
pub const DEFAULT_LISTEN: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    7680,
));

/// The services the API listener serves.
pub const SERVED: [&str; 2] = [
    "loams.postgres.v1.PostgresService",
    "loams.operations.v1.OperationsService",
];

/// Where the records live (`--store`).
#[derive(Clone)]
pub enum PgStore {
    /// One redb file under the data directory (single-node mode).
    Local,
    /// TiKV, on the metastore's keyspace and root (`--meta tikv://…`).
    #[cfg(feature = "tikv")]
    Tikv(loams_tikv::TikvConfig),
}

impl fmt::Debug for PgStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgStore::Local => f.write_str("Local"),
            #[cfg(feature = "tikv")]
            PgStore::Tikv(t) => f.debug_tuple("Tikv").field(&t.keyspace).finish(),
        }
    }
}

/// Where computes run (`--runtime`), and so where secrets live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgRuntime {
    /// compose: computes are compose services ([`ComputesConfig`]);
    /// secrets in the age-encrypted file under the data directory.
    Compose,
    /// Kubernetes: secrets as Secrets in `namespace`. Computes on
    /// Kubernetes are Task 13's: until then endpoints are recorded but no
    /// compute starts.
    Kubernetes { namespace: String },
}

/// How the compose runtime runs computes (Task 11, R10.8).
#[derive(Clone, Debug)]
pub struct ComputesConfig {
    /// The compose command; `None`: the first of `docker compose`, `podman
    /// compose` and `docker-compose` on `PATH`.
    pub compose_bin: Option<ComposeBin>,
    /// Where the compose files live; `None`: `<data_dir>/compose`.
    pub compose_dir: Option<PathBuf>,
    /// The storage stack's network the computes join
    /// (`loams-neon_default` for `deploy/loams-postgres-dev`).
    pub network: Option<String>,
    /// The upcall listener as the computes reach it; `None`:
    /// `http://host.docker.internal:<upcall port>` (Docker, with
    /// `host.docker.internal:host-gateway` added to `extra_hosts`) or
    /// `http://host.containers.internal:<upcall port>` (Podman).
    pub control_plane_uri: Option<String>,
    pub extra_hosts: Vec<String>,
    /// Where endpoints' ports are published (loopback by default).
    pub publish_ip: IpAddr,
    /// How `pg-control` reaches each compute's `compute_ctl`.
    pub ctl_reach: CtlReach,
    /// Compute images by Postgres major, over the pin file's
    /// ([`images::pinned`]).
    pub image_overrides: BTreeMap<u32, String>,
    /// A compute runner to drive computes through, instead of a compose
    /// command here (R-REL.1). The compose settings above are then the
    /// runner's, and unused.
    pub runner: Option<RunnerClient>,
}

/// A compute runner, as `pg-control` reaches it (`--compute-runner`).
#[derive(Clone)]
pub struct RunnerClient {
    pub url: Url,
    pub token: Secret<String>,
}

impl fmt::Debug for RunnerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnerClient")
            .field("url", &self.url.as_str())
            .finish_non_exhaustive()
    }
}

impl Default for ComputesConfig {
    fn default() -> Self {
        ComputesConfig {
            compose_bin: None,
            compose_dir: None,
            network: None,
            control_plane_uri: None,
            extra_hosts: Vec::new(),
            publish_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            ctl_reach: CtlReach::Host,
            image_overrides: BTreeMap::new(),
            runner: None,
        }
    }
}

/// Who may call (`--pg-authz`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgAuth {
    /// One principal, every relation; loopback listeners only.
    Dev,
    /// API keys and their grants, from this keys file (`--pg-api-keys`).
    ApiKeys(PathBuf),
    /// OpenFGA, when MT1 lands; refused until then.
    OpenFga,
}

/// How `pg-control` runs.
#[derive(Clone)]
pub struct PgControlConfig {
    /// The local store's file and the file secret store live here.
    pub data_dir: PathBuf,
    pub store: PgStore,
    pub runtime: PgRuntime,
    /// The API listener.
    pub listen: SocketAddr,
    /// The upcall listener; must not overlap `listen`.
    pub upcall_listen: SocketAddr,
    pub auth: PgAuth,
    /// `grpc.reflection.v1` on the API listener.
    pub reflection: bool,
    /// A pageserver's management API.
    pub pageserver: Url,
    /// The storage controller, which every storage call then goes through.
    pub storage_controller: Option<Url>,
    /// `loams-wal`'s HTTP API.
    pub wal: Url,
    /// Pageserver node id → the libpq URL computes read pages from.
    pub pageservers: BTreeMap<u64, String>,
    /// The node of a branch no `notify-attach` placed (single node).
    pub default_pageserver: Option<u64>,
    /// `loams-wal` pool name → the connection strings computes write to.
    pub wal_pools: BTreeMap<String, Vec<String>>,
    /// The storage controller's token for `notify-attach`; without it the
    /// route is not served.
    pub notify_token: Option<Secret<String>>,
    /// Where audit events go.
    pub audit: Arc<dyn AuditSink>,
    /// How often the upkeep runs.
    pub upkeep_every: Duration,
    /// How long a finished operation stays readable.
    pub operation_retention: Duration,
    /// With `--runtime compose`: how computes run. `None`: endpoints are
    /// recorded but no compute starts (tests of the API alone).
    pub computes: Option<ComputesConfig>,
}

impl fmt::Debug for PgControlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgControlConfig")
            .field("data_dir", &self.data_dir)
            .field("runtime", &self.runtime)
            .field("listen", &self.listen)
            .field("upcall_listen", &self.upcall_listen)
            .field("auth", &self.auth)
            .field("pageserver", &self.pageserver.as_str())
            .field("wal", &self.wal.as_str())
            .field("notify_token", &self.notify_token)
            .finish_non_exhaustive()
    }
}

impl PgControlConfig {
    /// Single-node defaults in `data_dir`: the local store, compose, dev
    /// authorization on loopback, and `deploy/loams-postgres-dev`'s
    /// addresses.
    ///
    /// # Panics
    ///
    /// Never: the default URLs and addresses are literals.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        PgControlConfig {
            data_dir: data_dir.into(),
            store: PgStore::Local,
            runtime: PgRuntime::Compose,
            listen: DEFAULT_LISTEN,
            upcall_listen: loams_pg_control::upcall::DEFAULT_UPCALL_LISTEN
                .parse()
                .expect("a literal address"),
            auth: PgAuth::Dev,
            reflection: false,
            pageserver: Url::parse("http://127.0.0.1:9898").expect("a literal URL"),
            storage_controller: None,
            wal: Url::parse("http://127.0.0.1:7676").expect("a literal URL"),
            pageservers: BTreeMap::new(),
            default_pageserver: None,
            wal_pools: BTreeMap::new(),
            notify_token: None,
            audit: Arc::new(LogAuditSink),
            upkeep_every: Duration::from_secs(600),
            operation_retention: OPERATION_RETENTION,
            computes: Some(ComputesConfig::default()),
        }
    }
}

/// Why `pg-control` did not start.
#[derive(Debug, thiserror::Error)]
pub enum PgControlError {
    #[error("{0}")]
    Config(String),
    #[error("the store: {0}")]
    Store(#[from] loams_pg_control::StoreError),
    #[error("the secret store: {0}")]
    Secrets(#[from] loams_pg_control::secrets::SecretError),
    #[error("the signing key: {0}")]
    SigningKey(#[from] loams_pg_control::upcall::SigningKeyError),
    #[error("the keys file: {0}")]
    Keys(#[from] loams_pg_control::authz::KeysError),
    #[error("the Neon clients: {0}")]
    Neon(#[from] loams_postgres::NeonError),
    #[error("the upcall listener: {0}")]
    Upcall(#[from] loams_pg_control::upcall::UpcallError),
    #[error("the compute runtime: {0}")]
    Runtime(String),
    #[error("binding the API listener {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

/// A running `pg-control`.
pub struct PgControl {
    api_addr: SocketAddr,
    upcall_addr: SocketAddr,
    keys: Arc<ComputeKeys>,
    stop: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl fmt::Debug for PgControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgControl")
            .field("api_addr", &self.api_addr)
            .field("upcall_addr", &self.upcall_addr)
            .field("kid", &self.keys.kid())
            .finish_non_exhaustive()
    }
}

/// Who calls, and what they may do.
type Gatekeepers = (Arc<dyn Authenticator>, Arc<dyn Authorizer>);

/// The authenticator and authorizer of `auth`, for a listener on `listen`.
fn gatekeepers(auth: &PgAuth, listen: SocketAddr) -> Result<Gatekeepers, PgControlError> {
    match auth {
        PgAuth::Dev => Ok((
            Arc::new(DevLoopbackAuthenticator::new(listen).map_err(PgControlError::Config)?),
            Arc::new(DevAuthorizer),
        )),
        PgAuth::ApiKeys(path) => {
            let keys = KeysFile::read(path)?;
            Ok((
                Arc::new(ApiKeyAuthenticator::new(&keys)),
                Arc::new(GrantsAuthorizer::new(&keys)?),
            ))
        }
        PgAuth::OpenFga => Err(PgControlError::Config(
            "--pg-authz openfga needs MT1's OpenFGA model, which has not landed; \
             use --pg-authz api-key (or dev on loopback)"
                .into(),
        )),
    }
}

async fn open_store(config: &PgControlConfig) -> Result<KvControlStore, PgControlError> {
    let options = StoreOptions::default();
    match &config.store {
        PgStore::Local => Ok(loams_pg_control::store::local::open(
            config.data_dir.join("pg-control.redb"),
            options,
        )
        .await?),
        #[cfg(feature = "tikv")]
        PgStore::Tikv(tikv) => {
            Ok(loams_pg_control::store::tikv::open(tikv.clone(), options).await?)
        }
    }
}

async fn open_secrets(config: &PgControlConfig) -> Result<Arc<dyn SecretStore>, PgControlError> {
    match &config.runtime {
        PgRuntime::Compose => Ok(Arc::new(FileSecretStore::open(
            config.data_dir.join("secrets.age"),
            &KeySource::File(config.data_dir.join("secrets.key")),
        )?)),
        #[cfg(feature = "postgres-kubernetes")]
        PgRuntime::Kubernetes { namespace } => Ok(Arc::new(
            loams_pg_control::secrets::KubeSecretStore::from_environment(namespace).await?,
        )),
        #[cfg(not(feature = "postgres-kubernetes"))]
        PgRuntime::Kubernetes { .. } => Err(PgControlError::Config(
            "--runtime kubernetes: this loams was built without the postgres-kubernetes \
             feature; rebuild with `--features postgres-kubernetes`"
                .into(),
        )),
    }
}

/// The compute images by Postgres major: the pin file's, with `overrides`.
fn compute_images(
    overrides: &BTreeMap<u32, String>,
) -> Result<BTreeMap<u32, String>, PgControlError> {
    let runtime_error = |e: &dyn fmt::Display| PgControlError::Runtime(e.to_string());
    let mut images = images::pinned().map_err(|e| runtime_error(&e))?;
    for (major, image) in overrides {
        images::check_image(image).map_err(|e| runtime_error(&e))?;
        images.insert(*major, image.clone());
    }
    Ok(images)
}

/// The compose runtime of `settings`, run here. Without a control-plane
/// URI, the computes reach `upcall_addr`'s port on the host.
fn compose_runtime(
    settings: &ComputesConfig,
    default_dir: PathBuf,
    upcall_addr: SocketAddr,
) -> Result<ComposeRuntime, PgControlError> {
    let runtime_error = |e: &dyn fmt::Display| PgControlError::Runtime(e.to_string());
    let bin = match &settings.compose_bin {
        Some(b) => b.clone(),
        None => ComposeBin::from_path().ok_or_else(|| {
            PgControlError::Runtime(
                "--runtime compose needs docker compose, podman compose or docker-compose on \
                 PATH"
                    .into(),
            )
        })?,
    };
    let podman = bin
        .program
        .file_name()
        .is_some_and(|n| n.to_string_lossy().contains("podman"));
    let mut extra_hosts = settings.extra_hosts.clone();
    let control_plane_uri = match &settings.control_plane_uri {
        Some(uri) => uri.clone(),
        None if podman => format!("http://host.containers.internal:{}", upcall_addr.port()),
        None => {
            if !extra_hosts
                .iter()
                .any(|h| h.starts_with("host.docker.internal:"))
            {
                extra_hosts.push("host.docker.internal:host-gateway".into());
            }
            format!("http://host.docker.internal:{}", upcall_addr.port())
        }
    };
    let mut compose = ComposeConfig::new(
        bin,
        settings.compose_dir.clone().unwrap_or(default_dir),
        control_plane_uri,
    );
    compose.network = settings.network.clone();
    compose.extra_hosts = extra_hosts;
    compose.publish_ip = settings.publish_ip;
    compose.ctl_reach = settings.ctl_reach;
    ComposeRuntime::open(compose, Arc::new(TokioCommandRunner)).map_err(|e| runtime_error(&e))
}

/// The compute runtime (the compose runtime here, or a compute runner) and
/// what the reconciler drives computes with.
fn computes(
    config: &PgControlConfig,
    settings: &ComputesConfig,
    keys: &Arc<ComputeKeys>,
    clock: &Clock,
    upcall_addr: SocketAddr,
) -> Result<Computes, PgControlError> {
    if upcall_addr.ip().is_loopback() {
        tracing::warn!(
            %upcall_addr,
            "computes run in containers and cannot reach a loopback upcall listener; \
             listen where they reach it (--pg-upcall-listen 0.0.0.0:7690)"
        );
    }
    let runtime: Arc<dyn ComputeRuntime> = match &settings.runner {
        Some(runner) => Arc::new(
            RemoteRuntime::new(runner.url.clone(), runner.token.clone())
                .map_err(|e| PgControlError::Runtime(e.to_string()))?,
        ),
        None => Arc::new(compose_runtime(
            settings,
            config.data_dir.join("compose"),
            upcall_addr,
        )?),
    };
    let images = compute_images(&settings.image_overrides)?;
    Ok(Computes::new(
        runtime,
        Arc::new(HttpComputeCtl::new(keys.clone(), clock.clone())),
        keys.clone(),
        images,
        SpecSources {
            pageservers: config.pageservers.clone(),
            default_pageserver: config.default_pageserver,
            wal_pools: config.wal_pools.clone(),
        },
    ))
}

/// The API listener's routes: every RPC path its own route, so any other
/// path is axum's 404 (an upcall path among them).
fn api_routes(service: PgService<NeonClients>, gate: Arc<Gate>, reflection: bool) -> AxumRouter {
    let rpc = connect_postgres::register(connectrpc::Router::new(), service, gate);
    let (rpc, _health) = connectrpc_health::install_static(rpc, SERVED.to_vec());
    let rpc = if reflection {
        crate::api::connect::install_reflection(rpc, &[])
    } else {
        rpc
    };
    let paths: Vec<String> = rpc.methods().map(|path| format!("/{path}")).collect();
    let service = rpc.into_axum_service();
    let mut axum = AxumRouter::new();
    for path in paths {
        axum = axum.route_service(&path, service.clone());
    }
    axum
}

impl PgControl {
    /// Opens the store and the secret store, reads (or creates) the signing
    /// key, binds both listeners and starts the reconciler and the upkeep.
    ///
    /// # Errors
    ///
    /// [`PgControlError`]; nothing is left running.
    pub async fn start(config: PgControlConfig) -> Result<Self, PgControlError> {
        let (authenticator, authorizer) = gatekeepers(&config.auth, config.listen)?;
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            PgControlError::Config(format!("--data-dir {}: {e}", config.data_dir.display()))
        })?;
        let store = open_store(&config).await?;
        let secrets = open_secrets(&config).await?;
        let signing_ref = SecretRef::parse(SIGNING_KEY_REF)?;
        let keys = Arc::new(load_or_create_signing_key(secrets.as_ref(), &signing_ref).await?);
        let neon = NeonClients {
            storage: NeonClient::new(
                NeonEndpoints {
                    pageserver: config.pageserver.clone(),
                    storcon: config.storage_controller.clone(),
                },
                None,
            )?,
            wal: WalClient::new(config.wal.clone(), None)?,
        };
        let clock = Clock::system();
        let service = PgService::new(
            store.clone(),
            neon.clone(),
            secrets.clone(),
            ServiceConfig::default(),
        );

        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .map_err(|source| PgControlError::Bind {
                addr: config.listen,
                source,
            })?;
        let api_addr = listener
            .local_addr()
            .map_err(|source| PgControlError::Bind {
                addr: config.listen,
                source,
            })?;
        let upcall = UpcallServer::bind(
            store.clone(),
            keys.clone(),
            Arc::new(ComputeCtlConfigurer::new(keys.clone(), clock.clone())),
            UpcallConfig {
                api_listen: Some(api_addr),
                notify_token: config.notify_token.clone(),
                pageservers: config.pageservers.clone(),
                default_pageserver: config.default_pageserver,
                wal_pools: config.wal_pools.clone(),
                ..UpcallConfig::defaults(config.upcall_listen)
            },
        )
        .await?;
        let upcall_addr = upcall.local_addr();
        let computes = match (&config.runtime, &config.computes) {
            (PgRuntime::Compose, Some(settings)) => {
                Some(computes(&config, settings, &keys, &clock, upcall_addr)?)
            }
            (PgRuntime::Kubernetes { .. }, Some(_)) => {
                tracing::warn!(
                    "--runtime kubernetes runs no computes until PG2 Task 13: endpoints are \
                     recorded, and none starts"
                );
                None
            }
            (_, None) => None,
        };

        let gate = Arc::new(Gate {
            authenticator,
            authorizer,
            audit: config.audit.clone(),
            clock: clock.clone(),
        });
        let app = api_routes(service.clone(), gate, config.reflection);
        let stop = CancellationToken::new();
        let mut tasks = Vec::new();

        let token = stop.clone();
        tasks.push(tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "the pg-control API listener stopped");
            }
        }));
        let token = stop.clone();
        tasks.push(tokio::spawn(async move {
            upcall.serve(async move { token.cancelled().await }).await;
        }));
        let holder = format!("pg-control-{}", ulid::Ulid::generate());
        let mut reconciler = Reconciler::new(store, neon, secrets, ReconcilerConfig::new(holder));
        if let Some(computes) = computes {
            reconciler = reconciler.with_computes(computes);
        }
        let reconciler = Arc::new(reconciler);
        let token = stop.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = reconciler.run(async move { token.cancelled().await }).await {
                tracing::error!(error = %e, "the pg-control reconciler stopped");
            }
        }));
        tasks.push(tokio::spawn(upkeep(
            service,
            clock,
            config.upkeep_every,
            config.operation_retention,
            stop.clone(),
        )));
        tracing::info!(%api_addr, %upcall_addr, kid = keys.kid(), "pg-control started");
        Ok(PgControl {
            api_addr,
            upcall_addr,
            keys,
            stop,
            tasks,
        })
    }

    /// The API listener's bound address.
    pub fn api_addr(&self) -> SocketAddr {
        self.api_addr
    }

    /// The upcall listener's bound address.
    pub fn upcall_addr(&self) -> SocketAddr {
        self.upcall_addr
    }

    /// The signing key's id: the same for every instance on one secret
    /// store, and across restarts.
    pub fn signing_kid(&self) -> &str {
        self.keys.kid()
    }

    /// Stops the listeners, the reconciler and the upkeep, and waits for
    /// them.
    pub async fn shutdown(self) {
        self.stop.cancel();
        for task in self.tasks {
            if let Err(e) = task.await
                && e.is_panic()
            {
                tracing::error!(error = %e, "a pg-control task panicked");
            }
        }
    }
}

/// Every `every`: the ledger's expired entries, then finished operations
/// past `retention` (R5.2, R5.13).
async fn upkeep(
    service: PgService<NeonClients>,
    clock: Clock,
    every: Duration,
    retention: Duration,
    stop: CancellationToken,
) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = tick.tick() => {}
        }
        let now = clock.now_ms();
        match service.ledger().prune(now).await {
            Ok(n) if n > 0 => tracing::info!(removed = n, "pruned the idempotency ledger"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "pruning the idempotency ledger failed"),
        }
        match service.collect_operations(now, retention).await {
            Ok(n) if n > 0 => tracing::info!(removed = n, "collected finished operations"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "collecting finished operations failed"),
        }
    }
}

/// Reads the storage controller's token from `path` (trimmed).
///
/// # Errors
///
/// A message when it cannot be read or is empty.
pub fn read_token(path: &Path) -> Result<Secret<String>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!("{} is empty", path.display()));
    }
    Ok(Secret::new(token.to_owned()))
}

/// How the compute runner runs (`loams pg-runner`, R-REL.1).
#[derive(Clone, Debug)]
pub struct PgRunnerConfig {
    /// Where pg-control calls it.
    pub listen: SocketAddr,
    /// The token pg-control presents.
    pub token: Secret<String>,
    /// The compose runtime it runs computes with. `control_plane_uri` must
    /// be set: the runner does not know where pg-control's upcall listener
    /// is. `compose_dir` defaults to `<data_dir>/compose`.
    pub computes: ComputesConfig,
    pub data_dir: PathBuf,
}

/// A running compute runner.
#[derive(Debug)]
pub struct PgRunner {
    addr: SocketAddr,
    images: Vec<String>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl PgRunner {
    /// Opens the compose runtime and binds the runner's listener. Its
    /// policy runs the pin file's compute images, with the overrides.
    ///
    /// # Errors
    ///
    /// [`PgControlError::Config`] without a control-plane URI;
    /// [`PgControlError::Runtime`] when the compose runtime or the images
    /// are unusable; [`PgControlError::Bind`].
    pub async fn start(config: PgRunnerConfig) -> Result<Self, PgControlError> {
        let settings = &config.computes;
        if settings.control_plane_uri.is_none() {
            return Err(PgControlError::Config(
                "loams pg-runner needs --compute-control-plane-uri: pg-control's upcall listener \
                 as the computes reach it"
                    .into(),
            ));
        }
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            PgControlError::Config(format!("--data-dir {}: {e}", config.data_dir.display()))
        })?;
        let images = compute_images(&settings.image_overrides)?;
        let policy = RunnerPolicy::new(images.into_values())
            .map_err(|e| PgControlError::Runtime(e.to_string()))?;
        // The upcall address only names a default URI, which is set.
        let runtime = compose_runtime(
            settings,
            config.data_dir.join("compose"),
            SocketAddr::from(([127, 0, 0, 1], 0)),
        )?;
        let server = RunnerServer::bind(
            config.listen,
            Arc::new(runtime),
            policy.clone(),
            config.token,
        )
        .await
        .map_err(|source| PgControlError::Bind {
            addr: config.listen,
            source,
        })?;
        let addr = server.local_addr();
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(server.serve(async move { token.cancelled().await }));
        let images: Vec<String> = policy.images().map(str::to_owned).collect();
        tracing::info!(%addr, ?images, "compute runner started");
        Ok(PgRunner {
            addr,
            images,
            stop,
            task,
        })
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The images computes may run.
    pub fn images(&self) -> &[String] {
        &self.images
    }

    /// Stops the listener once the calls in flight finish.
    pub async fn shutdown(self) {
        self.stop.cancel();
        if let Err(e) = self.task.await
            && e.is_panic()
        {
            tracing::error!(error = %e, "the compute runner panicked");
        }
    }
}
