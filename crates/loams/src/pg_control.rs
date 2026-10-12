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
//! - the **reconciler** of projects and branches ([`Reconciler`]);
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
use std::net::SocketAddr;
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
use loams_pg_control::reconcile::{Reconciler, ReconcilerConfig};
use loams_pg_control::secrets::{FileSecretStore, KeySource, Secret, SecretRef, SecretStore};
use loams_pg_control::service::operations::OPERATION_RETENTION;
use loams_pg_control::service::{Clock, PgService, ServiceConfig};
use loams_pg_control::upcall::{
    ComputeCtlConfigurer, ComputeKeys, SIGNING_KEY_REF, UpcallConfig, UpcallServer,
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

/// Where computes run (`--runtime`). Until Task 10 it decides only the
/// secret store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgRuntime {
    /// compose: secrets in the age-encrypted file under the data directory.
    Compose,
    /// Kubernetes: secrets as Secrets in `namespace`.
    Kubernetes { namespace: String },
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
        let reconciler = Arc::new(Reconciler::new(
            store,
            neon,
            secrets,
            ReconcilerConfig::new(holder),
        ));
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
