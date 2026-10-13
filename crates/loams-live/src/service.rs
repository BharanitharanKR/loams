//! The Loams Live sync service (design §20 §7, D121; R1 plan Task 12):
//! [`LiveServer`] serves `loams.live.v1.LiveService` and
//! `LiveAdminService` (LV1 plan Task 10, [`crate::admin`]) over connect-rust
//! and axum on its own loopback listener, with one [`Subscriptions`]
//! manager, one journal janitor and the [`Sessions`] of the app, and
//! `grpc.reflection.v1` listing those two services.
//!
//! `Watch` opens a session and streams its Transitions; `ModifyQuerySet`
//! changes a session's query set; `Query` and `Mutate` run through the
//! app's [`Runner`], resolving functions through its [`AppEngine`]: the
//! `_system:*` functions, else the active deployment's. Each `Query` and
//! `Mutate` gets a request id, answered in the `loams-request-id` header,
//! under which its function's console lines go to the app's log ring
//! ([`crate::logs`]). Deploys need a function [`Runtime`].
//! Connect, gRPC and gRPC-Web, JSON and binary, over HTTP/1.1 and HTTP/2
//! (cleartext, loopback only), behind the [`Guard`] the engine passes (its
//! authentication layer).
//!
//! The engine also serves both services on its main Connect port (LV1 plan
//! Task 12, D688): [`LiveHandle::routes`] hands it the same service
//! instances as [`LiveRoutes`], which it registers on its own router,
//! behind that port's authentication layer.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::{
    ConnectError, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};
use futures::StreamExt;
use loams_kv::Store;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admin::Admin;
use crate::deploy::{self, AppEngine, Runtime};
use crate::logs::{CallInfo, REQUEST_ID_HEADER, Ring};
use crate::pb::{self, LiveAdminServiceExt, LiveService, LiveServiceExt};
use crate::session::{Outbox, SESSION_HEADER, Sessions, Start, args_of, chunks, ts_of};
use crate::subs::{SubsConfig, SubsStats, Subscriptions};
use crate::{Janitor, LiveConfig, LiveError, Runner, check_isolation, check_listen};

/// How long stopping waits for in-flight requests before aborting them.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// Wraps the Live listener's routes before they serve (LV1 plan Task 10):
/// the engine puts its authentication layer here, which also names the
/// caller with an [`admin::Caller`](crate::admin::Caller).
pub type Guard = Arc<dyn Fn(axum::Router) -> axum::Router + Send + Sync>;

/// The services the Live listener's reflection lists.
const REFLECTED: [&str; 4] = [
    pb::LIVE_SERVICE_SERVICE_NAME,
    pb::LIVE_ADMIN_SERVICE_SERVICE_NAME,
    "grpc.reflection.v1.ServerReflection",
    "grpc.reflection.v1alpha.ServerReflection",
];

/// Starts Loams Live servers.
#[derive(Debug)]
pub struct LiveServer;

/// A running Live server.
#[derive(Debug)]
pub struct LiveHandle {
    /// The address the sync API listens on.
    pub addr: SocketAddr,
    store: Store,
    routes: LiveRoutes,
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    stop: CancellationToken,
    serve: JoinHandle<()>,
    janitor: JoinHandle<()>,
}

impl LiveServer {
    /// [`start_with`](Self::start_with) without a function runtime: only
    /// the `_system:*` functions serve, and deploys are refused.
    pub async fn start(
        config: LiveConfig,
        shutdown: CancellationToken,
    ) -> Result<LiveHandle, LiveError> {
        LiveServer::start_with(config, Runtime::none(), shutdown).await
    }

    /// [`start_guarded`](Self::start_guarded) with no guard: every local
    /// process may call every RPC.
    pub async fn start_with(
        config: LiveConfig,
        runtime: Runtime,
        shutdown: CancellationToken,
    ) -> Result<LiveHandle, LiveError> {
        LiveServer::start_guarded(config, runtime, None, shutdown).await
    }

    /// Checks that `config.listen` is loopback, opens the app's store
    /// (whichever backend `config.store` names), opens its runner and its
    /// [`AppEngine`] on `runtime` (loading the active deployment), starts
    /// the subscription manager (its consumer is `config.node` unless set)
    /// and the janitor, binds the listener and serves, behind `guard` when
    /// given, until `shutdown` is cancelled or [`LiveHandle::stop`]. Without
    /// a guard it logs once that the API is unauthenticated.
    pub async fn start_guarded(
        config: LiveConfig,
        runtime: Runtime,
        guard: Option<Guard>,
        shutdown: CancellationToken,
    ) -> Result<LiveHandle, LiveError> {
        check_listen(config.listen)?;
        check_isolation(&config)?;
        let store = Store::open(config.store.clone()).await.map_err(|e| {
            LiveError::Internal(format!(
                "opening the Live store (keyspace {}): {e}",
                config.store.keyspace()
            ))
        })?;
        let runner = Runner::open(store.clone(), &config).await?;
        let engine = AppEngine::open(&config.app, runner.clone(), runtime).await?;
        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .map_err(|e| LiveError::Internal(format!("live listen on {}: {e}", config.listen)))?;
        let addr = listener
            .local_addr()
            .map_err(|e| LiveError::Internal(format!("live listen on {}: {e}", config.listen)))?;
        let stop = shutdown.child_token();
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
        let service = Live {
            runner,
            engine: engine.clone(),
            subs: subs.clone(),
            sessions: sessions.clone(),
            max_transition_bytes: config.session.max_transition_bytes,
        };
        let admin = Admin {
            engine,
            stop: stop.clone(),
        };
        let routes = LiveRoutes {
            live: Arc::new(service),
            admin: Arc::new(admin),
        };
        let rpc = reflection(routes.register(connectrpc::Router::new()));
        let mut app = axum::Router::new().fallback_service(rpc.into_axum_service());
        let guarded = guard.is_some();
        if let Some(guard) = guard {
            app = guard(app);
        }
        let until = stop.clone();
        let serve = tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(until.cancelled_owned())
                .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "the Live sync API failed");
            }
        });
        if !guarded {
            tracing::warn!(
                %addr,
                app = %config.app,
                "Loams Live API is unauthenticated (D111): Query, Mutate and the admin calls are \
                 open to every local process; it listens on loopback only"
            );
        }
        Ok(LiveHandle {
            addr,
            store,
            routes,
            subs,
            sessions,
            stop,
            serve,
            janitor,
        })
    }
}

impl LiveHandle {
    /// The app's store (on TiKV, the cluster GC loop sweeps its commit
    /// tokens through `Store::as_tikv`, feature `tikv`; the embedded store runs its own
    /// GC).
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The subscription manager's counters. `missed_invalidations` is the
    /// `live_missed_invalidation_total` counter (row T12-8).
    pub fn stats(&self) -> SubsStats {
        self.subs.stats()
    }

    /// Open sessions.
    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    /// The two services this server answers, for another router (the
    /// engine's main port, LV1 plan Task 12). They share this server's
    /// sessions, engine and log ring, and stop with it.
    pub fn routes(&self) -> LiveRoutes {
        self.routes.clone()
    }

    /// Stops serving: ends every session's stream, stops the manager and the
    /// janitor, and waits up to 10 s for in-flight requests.
    pub async fn stop(self) {
        self.stop.cancel();
        let mut serve = self.serve;
        if tokio::time::timeout(STOP_GRACE, &mut serve).await.is_err() {
            tracing::warn!("Live requests did not finish; aborting them");
            serve.abort();
        }
        let _ = self.janitor.await;
    }
}

/// `LiveService` and `LiveAdminService` of one running [`LiveServer`], to
/// register on another Connect router (LV1 plan Task 12). The router's
/// owner puts its authentication in front of them, and names the caller
/// with an [`admin::Caller`](crate::admin::Caller) for the admin service.
#[derive(Clone)]
pub struct LiveRoutes {
    live: Arc<Live>,
    admin: Arc<Admin>,
}

impl std::fmt::Debug for LiveRoutes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveRoutes").finish_non_exhaustive()
    }
}

impl LiveRoutes {
    /// `rpc` with both services' routes added.
    pub fn register(&self, rpc: connectrpc::Router) -> connectrpc::Router {
        self.admin.clone().register(self.live.clone().register(rpc))
    }
}

/// Runs the journal janitor and the deployment janitor every `interval`
/// (the first run one interval after start), and refreshes the deployment
/// this node serves, so it picks up a deploy made on another node (until
/// LV1 plan Task 11 switches every node at the activation's tick).
async fn janitor_loop(engine: AppEngine, interval: Duration, stop: CancellationToken) {
    let runner = engine.runner().clone();
    let mut every = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
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

/// Installs `grpc.reflection.v1` on `rpc`, listing the two Live services
/// (and itself). Reflection is a convenience: a descriptor set that does
/// not parse is logged and the listener serves without it.
fn reflection(rpc: connectrpc::Router) -> connectrpc::Router {
    match connectrpc_reflection::Reflector::from_descriptor_set_bytes(
        loams_live_proto::FILE_DESCRIPTOR_SET,
    ) {
        Ok(reflector) => connectrpc_reflection::install(rpc, reflector.with_services(REFLECTED)),
        Err(e) => {
            tracing::warn!(error = %e, "Live reflection is off: the descriptor set does not parse");
            rpc
        }
    }
}

/// The `LiveService` implementation.
struct Live {
    runner: Runner,
    engine: AppEngine,
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    max_transition_bytes: usize,
}

impl Live {
    /// Records what a call of `f` wrote in the app's log ring.
    fn record(
        &self,
        request_id: &str,
        f: &dyn crate::Function,
        outcome: Result<&crate::CallOutput, &LiveError>,
    ) {
        let call = CallInfo {
            request_id: Some(request_id),
            function: f.name(),
            deployment: f.deployment(),
        };
        self.runner.logs().record(&call, outcome);
    }
}

/// The Connect error of `e`, answered with the call's request id.
fn with_request_id(e: &LiveError, request_id: &str) -> ConnectError {
    let mut headers = axum::http::HeaderMap::new();
    if let Ok(value) = axum::http::HeaderValue::from_str(request_id) {
        headers.insert(REQUEST_ID_HEADER, value);
    }
    connect_error(e).with_headers(headers)
}

/// The Connect error of `e` (the codes of §20 §7.1).
pub fn connect_error(e: &LiveError) -> ConnectError {
    let message = e.to_string();
    match e.code() {
        pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT => ConnectError::invalid_argument(message),
        pb::ErrorCode::ERROR_CODE_NOT_FOUND => ConnectError::not_found(message),
        pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION => ConnectError::failed_precondition(message),
        pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED => ConnectError::resource_exhausted(message),
        pb::ErrorCode::ERROR_CODE_UNAVAILABLE => ConnectError::unavailable(message),
        _ => ConnectError::internal(message),
    }
}

/// The stream of one session's Transitions: pops its outbox, chunks what is
/// too large, and tells the session when the client goes away.
struct Stream {
    outbox: Arc<Outbox>,
    chunks: std::collections::VecDeque<pb::Transition>,
    max_bytes: usize,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.outbox.client_gone();
    }
}

fn transitions(outbox: Arc<Outbox>, max_bytes: usize) -> ServiceStream<pb::Transition> {
    let state = Stream {
        outbox,
        chunks: std::collections::VecDeque::new(),
        max_bytes,
    };
    futures::stream::unfold(state, |mut s| async move {
        if let Some(c) = s.chunks.pop_front() {
            return Some((Ok(c), s));
        }
        match s.outbox.pop().await? {
            Err(e) => Some((Err(connect_error(&e)), s)),
            Ok(t) => {
                s.chunks = chunks(t, s.max_bytes).into();
                let first = s.chunks.pop_front()?;
                Some((Ok(first), s))
            }
        }
    })
    .boxed()
}

// The generated trait returns `impl Encodable`; plain `async fn`s with the
// owned messages are how connect-rust documents implementing it, and `Live`
// is private, so the refinement is not API.
#[allow(refining_impl_trait)]
impl LiveService for Live {
    async fn watch(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchRequest>,
    ) -> ServiceResult<ServiceStream<pb::Transition>> {
        let start =
            Start::from_request(request.to_owned_message()).map_err(|e| connect_error(&e))?;
        let session = self.sessions.open(start).map_err(|e| connect_error(&e))?;
        Ok(Response::new(transitions(
            session.outbox,
            self.max_transition_bytes,
        )))
    }

    async fn modify_query_set(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ModifyQuerySetRequest>,
    ) -> ServiceResult<pb::ModifyQuerySetResponse> {
        self.sessions
            .modify(request.to_owned_message())
            .await
            .map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::ModifyQuerySetResponse::default()))
    }

    async fn query(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::QueryRequest>,
    ) -> ServiceResult<pb::QueryResponse> {
        let req = request.to_owned_message();
        let request_id = Ring::request_id();
        let run = async {
            let f = deploy::resolve(&self.engine, &req.function)?;
            let args = args_of(req.args.into_option())?;
            let at = match req.ts {
                Some(ts) => ts_of(ts),
                None => match self.subs.current() {
                    Some(at) => at,
                    None => self
                        .runner
                        .store()
                        .now()
                        .await
                        .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?,
                },
            };
            let queried = self.runner.query(&*f, args, at).await;
            self.record(&request_id, &*f, queried.as_ref().map(|q| &q.output));
            queried
        };
        let queried = run.await.map_err(|e| with_request_id(&e, &request_id))?;
        Ok(Response::new(pb::QueryResponse {
            ts: queried.ts.0,
            result: buffa::MessageField::some(queried.result.to_proto()),
            ..Default::default()
        })
        .with_header(REQUEST_ID_HEADER, request_id))
    }

    async fn mutate(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::MutateRequest>,
    ) -> ServiceResult<pb::MutateResponse> {
        let req = request.to_owned_message();
        let request_id = Ring::request_id();
        let run = async {
            let f = deploy::resolve(&self.engine, &req.function)?;
            let args = args_of(req.args.into_option())?;
            let mutated = self
                .runner
                .mutate(f.clone(), args, req.idempotency_key)
                .await;
            self.record(&request_id, &*f, mutated.as_ref().map(|m| &m.output));
            mutated
        };
        let mutated = run.await.map_err(|e| with_request_id(&e, &request_id))?;
        let commit_ts = mutated.commit_ts.0;
        if let Some(session) = ctx.header(SESSION_HEADER).and_then(|v| v.to_str().ok()) {
            self.sessions.mutation_committed(session, commit_ts);
        }
        Ok(Response::new(pb::MutateResponse {
            commit_ts,
            result: buffa::MessageField::some(mutated.result.to_proto()),
            ..Default::default()
        })
        .with_header(REQUEST_ID_HEADER, request_id))
    }
}
