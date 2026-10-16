//! The Loams Live sync service (design §20 §7, D121; R1 plan Task 12):
//! [`LiveServer`] serves `loams.live.v1.LiveService` and
//! `LiveAdminService` (LV1 plan Task 10, [`crate::admin`]) over connect-rust
//! and axum on its own loopback listener, with the [`Engines`] of its apps
//! (each with one subscription manager, one journal janitor and its
//! sessions), and `grpc.reflection.v1` listing those two services.
//!
//! `Watch` opens a session and streams its Transitions; `ModifyQuerySet`
//! changes a session's query set; `Query` and `Mutate` run through the
//! app's [`Runner`](crate::Runner), resolving functions through its
//! [`AppEngine`](crate::AppEngine): the
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
//!
//! A server serves many apps (LV1 plan Task 13, D690): each call names its
//! app with the `loams-live-app` header (the server's own app without it),
//! and [`Engines`] runs that app's engine, starting it on first use.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::{
    ConnectError, ErrorCode, ErrorDetail, RequestContext, Response, ServiceRequest, ServiceResult,
    ServiceStream,
};
use futures::StreamExt;
use loams_kv::Store;
use loams_proto::loams::errors::v1::ErrorInfo;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::admin::Admin;
use crate::apps::APP_HEADER;
use crate::auth::Caller;
use crate::authz::{self, ScopeAuthorizer};
use crate::deploy::{self, Runtime};
use crate::engines::{AppRuntime, Engines};
use crate::logs::{CallInfo, REQUEST_ID_HEADER, Ring};
use crate::pb::{self, LiveAdminServiceExt, LiveService, LiveServiceExt};
use crate::session::{Outbox, SESSION_HEADER, Start, args_of, chunks, ts_of};
use crate::subs::SubsStats;
use crate::{LiveConfig, LiveError, check_isolation, check_listen};

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
    engines: Engines,
    routes: LiveRoutes,
    stop: CancellationToken,
    serve: JoinHandle<()>,
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

    /// Checks that `config.listen` is loopback, starts the app [`Engines`]
    /// on `runtime` (the app directory beside `config.store`, and the
    /// server's own app `config.app`: its store, runner and engine with the
    /// active deployment, its subscription manager, whose consumer is
    /// `config.node` unless set, and its janitor), binds the listener and
    /// serves, behind `guard` when given, until `shutdown` is cancelled or
    /// [`LiveHandle::stop`]. Without a guard it logs once that the API is
    /// unauthenticated.
    pub async fn start_guarded(
        config: LiveConfig,
        runtime: Runtime,
        guard: Option<Guard>,
        shutdown: CancellationToken,
    ) -> Result<LiveHandle, LiveError> {
        check_listen(config.listen)?;
        check_isolation(&config)?;
        let stop = shutdown.child_token();
        let listen = config.listen;
        let app_name = config.app.clone();
        let max_transition_bytes = config.session.max_transition_bytes;
        let engines = Engines::start(config, runtime, stop.clone()).await?;
        let bound = async {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(|e| LiveError::Internal(format!("live listen on {listen}: {e}")))?;
            let addr = listener
                .local_addr()
                .map_err(|e| LiveError::Internal(format!("live listen on {listen}: {e}")))?;
            Ok::<_, LiveError>((listener, addr))
        };
        let (listener, addr) = match bound.await {
            Ok(bound) => bound,
            Err(e) => {
                engines.shutdown().await;
                return Err(e);
            }
        };
        let service = Live {
            engines: engines.clone(),
            max_transition_bytes,
        };
        let admin = Admin {
            engines: engines.clone(),
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
                app = %app_name,
                "Loams Live API is unauthenticated (D111): Query, Mutate and the admin calls are \
                 open to every local process; it listens on loopback only"
            );
        }
        Ok(LiveHandle {
            addr,
            engines,
            routes,
            stop,
            serve,
        })
    }
}

impl LiveHandle {
    /// The server's own app's store (on TiKV, the cluster GC loop sweeps
    /// its commit tokens through `Store::as_tikv`, feature `tikv`; the
    /// embedded store runs its own GC).
    pub fn store(&self) -> &Store {
        self.engines.default_runtime().store()
    }

    /// The server's own app's subscription counters.
    /// `missed_invalidations` is the `live_missed_invalidation_total`
    /// counter (row T12-8).
    pub fn stats(&self) -> SubsStats {
        self.engines.default_runtime().subs().stats()
    }

    /// The server's own app's open sessions.
    pub fn sessions(&self) -> usize {
        self.engines.default_runtime().sessions().len()
    }

    /// The apps this server serves (LV1 plan Task 13).
    pub fn engines(&self) -> &Engines {
        &self.engines
    }

    /// The two services this server answers, for another router (the
    /// engine's main port, LV1 plan Task 12). They share this server's
    /// sessions, engine and log ring, and stop with it.
    pub fn routes(&self) -> LiveRoutes {
        self.routes.clone()
    }

    /// Stops serving: ends every session's stream, stops every app's
    /// manager and janitor, and waits up to 10 s for in-flight requests.
    pub async fn stop(self) {
        self.stop.cancel();
        let mut serve = self.serve;
        if tokio::time::timeout(STOP_GRACE, &mut serve).await.is_err() {
            tracing::warn!("Live requests did not finish; aborting them");
            serve.abort();
        }
        self.engines.shutdown().await;
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

/// The largest function arguments a call carries (LV1 plan Task 19).
pub const MAX_ARGS_BYTES: usize = 4 * 1024 * 1024;

/// The largest bundle a deploy carries (LV1 plan Task 19).
pub const MAX_BUNDLE_BYTES: usize = 16 * 1024 * 1024;

/// What a request holds beside its arguments or bundle (the function name,
/// a query set's other entries, a schema), within the message limit.
const REQUEST_HEADROOM: usize = 1024 * 1024;

/// The limits of a route whose largest field is `payload` bytes: the
/// message (after decompression) and the body on the wire (twice that, for
/// JSON's base64 and escapes), both checked before the message is decoded.
fn limits(payload: usize) -> connectrpc::Limits {
    connectrpc::Limits::default()
        .with_max_message_size(payload + REQUEST_HEADROOM)
        .with_max_request_body_size(2 * (payload + REQUEST_HEADROOM))
}

impl LiveRoutes {
    /// `rpc` with both services' routes added, each with its size limits
    /// (LV1 plan Task 19): arguments up to [`MAX_ARGS_BYTES`] on
    /// `LiveService`, a bundle up to [`MAX_BUNDLE_BYTES`] on `Deploy`.
    pub fn register(&self, rpc: connectrpc::Router) -> connectrpc::Router {
        let mut rpc = self.admin.clone().register(self.live.clone().register(rpc));
        for spec in [
            pb::LIVE_SERVICE_WATCH_SPEC,
            pb::LIVE_SERVICE_MODIFY_QUERY_SET_SPEC,
            pb::LIVE_SERVICE_QUERY_SPEC,
            pb::LIVE_SERVICE_MUTATE_SPEC,
        ] {
            rpc = rpc.with_route_limits(spec.procedure, limits(MAX_ARGS_BYTES));
        }
        rpc.with_route_limits(
            pb::LIVE_ADMIN_SERVICE_DEPLOY_SPEC.procedure,
            limits(MAX_BUNDLE_BYTES),
        )
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
    engines: Engines,
    max_transition_bytes: usize,
}

/// The app a request addresses (its `loams-live-app` header, else the
/// server's own app), its engine started when it does not run here yet,
/// and who the request runs as there (LV1 plan Task 16: an app end user
/// when its bearer is one of the app's issuers' tokens).
pub(crate) async fn app_of(
    engines: &Engines,
    ctx: &RequestContext,
) -> Result<(Arc<AppRuntime>, Caller), LiveError> {
    let header = match ctx.header(APP_HEADER) {
        None => None,
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| LiveError::invalid(format!("the {APP_HEADER} header is not text")))?,
        ),
    };
    let bearer = crate::auth::bearer_jwt(ctx);
    engines
        .resolve_as(Caller::of(ctx), bearer.as_deref(), header)
        .await
}

/// Checks that the caller may run each query of a session's query set
/// (LV1 plan Task 15). A function that does not resolve is left to the
/// session, which answers it `NOT_FOUND` in that query's result.
fn check_specs<'a>(
    app: &AppRuntime,
    caller: &Caller,
    specs: impl IntoIterator<Item = &'a pb::QuerySpec>,
) -> Result<(), LiveError> {
    for spec in specs {
        let Ok(f) = deploy::resolve(app.engine(), &spec.function) else {
            continue;
        };
        let args = args_of(spec.args.as_option().cloned())?;
        authz::check_call(&ScopeAuthorizer, caller, app.name(), &*f, &args)?;
    }
    Ok(())
}

/// How far ahead of the store's clock a client's read timestamp may be.
const MAX_TS_AHEAD_MS: u64 = 1_000;

/// Refuses a client-supplied read timestamp ahead of the store's clock, at
/// the edge and on both backends (LV1 plan Task 19, row T21-14): on TiKV a
/// far-future read would push the max-ts every later commit must exceed.
async fn check_read_ts(store: &loams_kv::Store, at: loams_kv::Ts) -> Result<(), LiveError> {
    let now = store
        .now()
        .await
        .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?;
    if at.physical_ms() > now.physical_ms().saturating_add(MAX_TS_AHEAD_MS) {
        return Err(LiveError::invalid(format!(
            "the read timestamp {} is ahead of the store's clock ({})",
            at.0, now.0
        )));
    }
    Ok(())
}

/// Records what a call of `f` wrote in the app's log ring.
fn record(
    app: &AppRuntime,
    request_id: &str,
    f: &dyn crate::Function,
    outcome: Result<&crate::CallOutput, &LiveError>,
) {
    let call = CallInfo {
        request_id: Some(request_id),
        function: f.name(),
        deployment: f.deployment(),
    };
    app.runner().logs().record(&call, outcome);
}

/// The Connect error of `e`, answered with the call's request id (the
/// `loams-request-id` header, and `request_id` in its `ErrorInfo`).
fn with_request_id(e: &LiveError, request_id: &str) -> ConnectError {
    let mut headers = axum::http::HeaderMap::new();
    if let Ok(value) = axum::http::HeaderValue::from_str(request_id) {
        headers.insert(REQUEST_ID_HEADER, value);
    }
    error_with(e, Some(request_id)).with_headers(headers)
}

/// The Connect error of `e` (LV1 plan Task 19): its Connect code, its
/// message ([`LiveError::public_message`]: an internal error's scrubbed,
/// with a request id), and one `loams.errors.v1.ErrorInfo` carrying its
/// [`wire_reason`](LiveError::wire_reason), plus `request_id` and, for a
/// limit, `limit` (design §44 §7.4).
pub fn connect_error(e: &LiveError) -> ConnectError {
    error_with(e, None)
}

fn error_with(e: &LiveError, request_id: Option<&str>) -> ConnectError {
    let (message, request_id) = e.public_message(request_id);
    let code = match e.code() {
        _ if matches!(e, LiveError::PermissionDenied { .. }) => ErrorCode::PermissionDenied,
        _ if matches!(e, LiveError::Unauthenticated { .. }) => ErrorCode::Unauthenticated,
        pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT => ErrorCode::InvalidArgument,
        pb::ErrorCode::ERROR_CODE_NOT_FOUND => ErrorCode::NotFound,
        pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION => ErrorCode::FailedPrecondition,
        pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED
        | pb::ErrorCode::ERROR_CODE_FUNCTION_OUT_OF_MEMORY => ErrorCode::ResourceExhausted,
        pb::ErrorCode::ERROR_CODE_UNAVAILABLE => ErrorCode::Unavailable,
        pb::ErrorCode::ERROR_CODE_FUNCTION_TIMEOUT => ErrorCode::DeadlineExceeded,
        // A function's own error is the application's, not the server's.
        pb::ErrorCode::ERROR_CODE_FUNCTION_ERROR if !matches!(e, LiveError::WorkerCrashed(_)) => {
            ErrorCode::Unknown
        }
        _ => ErrorCode::Internal,
    };
    let mut metadata = std::collections::HashMap::new();
    if let Some(id) = request_id {
        metadata.insert("request_id".to_string(), id);
    }
    if let LiveError::LimitExceeded { limit, .. } = e {
        metadata.insert("limit".to_string(), (*limit).to_string());
    }
    let info = ErrorInfo {
        reason: e.wire_reason().to_string(),
        metadata: metadata.into_iter().collect(),
        ..Default::default()
    };
    ConnectError::new(code, message).with_detail(ErrorDetail::from_message(
        "loams.errors.v1.ErrorInfo",
        &info,
    ))
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
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchRequest>,
    ) -> ServiceResult<ServiceStream<pb::Transition>> {
        let (app, caller) = app_of(&self.engines, &ctx)
            .await
            .map_err(|e| connect_error(&e))?;
        let start =
            Start::from_request(request.to_owned_message()).map_err(|e| connect_error(&e))?;
        let set = match &start {
            Start::Initial(set) | Start::Resume { set, .. } => set,
        };
        check_specs(&app, &caller, &set.queries).map_err(|e| connect_error(&e))?;
        if let Start::Resume { last, .. } = &start {
            check_read_ts(app.store(), ts_of(last.ts))
                .await
                .map_err(|e| connect_error(&e))?;
        }
        let session = app.sessions().open(start).map_err(|e| connect_error(&e))?;
        Ok(Response::new(transitions(
            session.outbox,
            self.max_transition_bytes,
        )))
    }

    async fn modify_query_set(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::ModifyQuerySetRequest>,
    ) -> ServiceResult<pb::ModifyQuerySetResponse> {
        let (app, caller) = app_of(&self.engines, &ctx)
            .await
            .map_err(|e| connect_error(&e))?;
        let req = request.to_owned_message();
        let added = req.changes.iter().filter_map(|c| match &c.change {
            Some(pb::__buffa::oneof::query_set_change::Change::Add(spec)) => Some(&**spec),
            _ => None,
        });
        check_specs(&app, &caller, added).map_err(|e| connect_error(&e))?;
        app.sessions()
            .modify(req)
            .await
            .map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::ModifyQuerySetResponse::default()))
    }

    async fn query(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::QueryRequest>,
    ) -> ServiceResult<pb::QueryResponse> {
        let req = request.to_owned_message();
        let request_id = Ring::request_id();
        let run = async {
            let (app, caller) = app_of(&self.engines, &ctx).await?;
            let f = deploy::resolve(app.engine(), &req.function)?;
            let args = args_of(req.args.into_option())?;
            authz::check_call(&ScopeAuthorizer, &caller, app.name(), &*f, &args)?;
            let at = match req.ts {
                Some(ts) => {
                    let at = ts_of(ts);
                    check_read_ts(app.store(), at).await?;
                    at
                }
                None => match app.subs().current() {
                    Some(at) => at,
                    None => app
                        .runner()
                        .store()
                        .now()
                        .await
                        .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?,
                },
            };
            let queried = app.runner().query_as(&*f, args, at, caller.identity).await;
            record(&app, &request_id, &*f, queried.as_ref().map(|q| &q.output));
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
            let (app, caller) = app_of(&self.engines, &ctx).await?;
            let f = deploy::resolve(app.engine(), &req.function)?;
            let args = args_of(req.args.into_option())?;
            authz::check_call(&ScopeAuthorizer, &caller, app.name(), &*f, &args)?;
            let mutated = app
                .runner()
                .mutate_as(f.clone(), args, req.idempotency_key, caller.identity)
                .await;
            record(&app, &request_id, &*f, mutated.as_ref().map(|m| &m.output));
            mutated.map(|m| (app, m))
        };
        let (app, mutated) = run.await.map_err(|e| with_request_id(&e, &request_id))?;
        let commit_ts = mutated.commit_ts.0;
        if let Some(session) = ctx.header(SESSION_HEADER).and_then(|v| v.to_str().ok()) {
            app.sessions().mutation_committed(session, commit_ts);
        }
        Ok(Response::new(pb::MutateResponse {
            commit_ts,
            result: buffa::MessageField::some(mutated.result.to_proto()),
            ..Default::default()
        })
        .with_header(REQUEST_ID_HEADER, request_id))
    }
}
