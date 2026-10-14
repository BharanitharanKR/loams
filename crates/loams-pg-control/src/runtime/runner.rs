//! The compute runner (R-REL.1): [`ComputeRuntime`] served over HTTP by
//! the one process that holds the container runtime's socket, so that
//! `pg-control` (and the engine) never hold it.
//!
//! In the preview release stack, `loams pg-runner` runs the compose
//! runtime with the Docker or Podman socket mounted, on a compose network
//! of its own that only `pg-control` joins and that has no route out
//! (`internal: true`). `pg-control` drives it with
//! [`RemoteRuntime`](super::remote::RemoteRuntime). The runner decides
//! everything that reaches the container runtime: the compose files are
//! its own deterministic rendering (no mounts, no privileges, no host
//! network), and before a call reaches its runtime, [`RunnerPolicy`]
//! checks it:
//!
//! - a compute's image is one of the runner's list, exactly: the pin
//!   file's compute image of each supported major
//!   (`deploy/pins/loams-postgres-images.toml`, compiled in), or the
//!   runner's own `--compute-image` overrides; never a `neon-archive` copy
//!   or an unpinned tag;
//! - a compute is bound to an endpoint, its namespace a DNS label, its
//!   project and endpoint ids canonical, its size 0.25 to 16 CU, and its
//!   upcall token one line of base64url ([`check_upcall_token`]);
//! - an endpoint's project, branch and endpoint ids are canonical, and
//!   every call's namespace is a DNS label.
//!
//! Every call but `GET /health` needs `Authorization: Bearer <token>`, the
//! secret both sides read from one file ([`read_or_create_token`]),
//! compared in constant time.
//!
//! | Route | Body | Answer |
//! |---|---|---|
//! | `POST /v1/ensure-address` | endpoint | [`BackendAddr`] |
//! | `POST /v1/remove-address` | endpoint | `null` |
//! | `POST /v1/start` | launch | [`ComputeHandle`] |
//! | `POST /v1/bind-address` | `{endpoint, handle}` | `null` |
//! | `POST /v1/stop` | `{compute_id}` | `null` |
//! | `POST /v1/status` | `{compute_id}` | [`RuntimeStatus`](super::RuntimeStatus) |
//! | `POST /v1/resize` | `{compute_id, cu}` | [`ResizeOutcome`](super::ResizeOutcome) |
//!
//! A refused or failed call answers `{"error": <RuntimeError>}` with 400
//! (`InvalidArgument`), 409 (`PortsExhausted`), 503 (`Unavailable`) or
//! 504 (`Timeout`); a missing or wrong token answers 401.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;

use super::compose::check_namespace;
use super::images::{ImageError, check_image};
use super::{
    BackendAddr, ComputeHandle, ComputeLaunch, ComputeRuntime, RuntimeError, check_upcall_token,
};
use crate::ids::{BranchId, ComputeId, EndpointId, ProjectId};
use crate::model::{ComputeBinding, Cu, EndpointRec, EndpointType};
use crate::secrets::{Secret, generate_password};
use crate::service::endpoints::{MAX_CU, MIN_CU};

/// `loams pg-runner --listen`'s default.
pub const DEFAULT_RUNNER_LISTEN: &str = "127.0.0.1:7691";

/// The largest request body the runner reads.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// The longest `Authorization` header the runner reads.
const MAX_AUTH_BYTES: usize = 1024;

/// What the runner lets through to its runtime. See the module doc.
#[derive(Debug, Clone)]
pub struct RunnerPolicy {
    images: BTreeSet<String>,
}

impl RunnerPolicy {
    /// A policy running exactly `images`.
    ///
    /// # Errors
    ///
    /// [`check_image`]'s, for an image that is not pinned by digest or is
    /// a `neon-archive` copy; [`ImageError::Pins`] for an empty list.
    pub fn new(images: impl IntoIterator<Item = String>) -> Result<Self, ImageError> {
        let mut set = BTreeSet::new();
        for image in images {
            check_image(&image)?;
            set.insert(image);
        }
        if set.is_empty() {
            return Err(ImageError::Pins(
                "the runner needs at least one compute image".into(),
            ));
        }
        Ok(RunnerPolicy { images: set })
    }

    /// The images computes may run, in order.
    pub fn images(&self) -> impl Iterator<Item = &str> {
        self.images.iter().map(String::as_str)
    }

    /// Whether the runner starts `c`.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`], saying why; it never repeats the
    /// token.
    pub fn check_launch(&self, c: &ComputeLaunch) -> Result<(), RuntimeError> {
        let invalid = |why: String| Err(RuntimeError::InvalidArgument(why));
        if !self.images.contains(&c.image) {
            return invalid(format!(
                "image {:?} is not one the runner runs (deploy/pins/loams-postgres-images.toml, \
                 or the runner's --compute-image)",
                c.image
            ));
        }
        let Some(binding) = &c.binding else {
            return invalid("the runner starts computes bound to an endpoint only".into());
        };
        check_namespace(&binding.namespace)?;
        canonical::<ProjectId>(&binding.project_id, "project")?;
        canonical::<EndpointId>(&binding.endpoint_id, "endpoint")?;
        if c.cu < MIN_CU || c.cu > MAX_CU {
            return invalid(format!(
                "a compute is 0.25 to 16 CU, not {} quarters",
                c.cu.0
            ));
        }
        check_upcall_token(c.upcall_token.expose())
    }

    /// Whether the runner takes `ep`: its ids are canonical.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`].
    pub fn check_endpoint(ep: &EndpointRec) -> Result<(), RuntimeError> {
        canonical::<ProjectId>(&ep.project_id, "project")?;
        canonical::<BranchId>(&ep.branch_id, "branch")?;
        canonical::<EndpointId>(&ep.id, "endpoint")?;
        Ok(())
    }
}

/// `text` parses as a `T` id.
fn canonical<T: std::str::FromStr>(text: &str, what: &str) -> Result<(), RuntimeError> {
    text.parse::<T>().map(drop).map_err(|_| {
        RuntimeError::InvalidArgument(format!("{text:?} is not a canonical {what} id"))
    })
}

// The wire shapes, shared with `remote`.

/// What a runtime reads of an endpoint: its namespace, ids, kind and
/// address. The runner rebuilds the record from these alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WireEndpoint {
    pub namespace: String,
    pub project_id: String,
    pub id: String,
    pub branch_id: String,
    pub kind: EndpointType,
    pub backend_addr: Option<String>,
}

impl WireEndpoint {
    pub(crate) fn of(ns: &str, ep: &EndpointRec) -> Self {
        WireEndpoint {
            namespace: ns.to_owned(),
            project_id: ep.project_id.clone(),
            id: ep.id.clone(),
            branch_id: ep.branch_id.clone(),
            kind: ep.kind,
            backend_addr: ep.backend_addr.clone(),
        }
    }

    fn record(self) -> (String, EndpointRec) {
        let mut ep = EndpointRec::new(self.project_id, self.id, self.branch_id, self.kind);
        ep.backend_addr = self.backend_addr;
        (self.namespace, ep)
    }
}

/// [`ComputeLaunch`] on the wire. No `Debug`: it holds the token.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireLaunch {
    pub compute_id: ComputeId,
    pub binding: Option<ComputeBinding>,
    pub address: Option<BackendAddr>,
    pub image: String,
    pub cu: Cu,
    pub upcall_token: String,
}

impl WireLaunch {
    pub(crate) fn of(c: &ComputeLaunch) -> Self {
        WireLaunch {
            compute_id: c.compute_id,
            binding: c.binding.clone(),
            address: c.address.clone(),
            image: c.image.clone(),
            cu: c.cu,
            upcall_token: c.upcall_token.expose().clone(),
        }
    }

    fn launch(self) -> ComputeLaunch {
        ComputeLaunch {
            compute_id: self.compute_id,
            binding: self.binding,
            address: self.address,
            image: self.image,
            cu: self.cu,
            upcall_token: Secret::new(self.upcall_token),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireBind {
    pub endpoint: WireEndpoint,
    pub handle: ComputeHandle,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireCompute {
    pub namespace: String,
    pub compute_id: ComputeId,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireResize {
    pub namespace: String,
    pub compute_id: ComputeId,
    pub cu: Cu,
}

/// A failed call's body.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WireError {
    pub error: RuntimeError,
}

/// The status a failed call answers with.
fn status_of(e: &RuntimeError) -> StatusCode {
    match e {
        RuntimeError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        RuntimeError::PortsExhausted { .. } => StatusCode::CONFLICT,
        RuntimeError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        RuntimeError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
    }
}

fn failure(e: RuntimeError) -> Response {
    (status_of(&e), axum::Json(WireError { error: e })).into_response()
}

fn answer<T: Serialize>(r: Result<T, RuntimeError>) -> Response {
    match r {
        Ok(v) => axum::Json(v).into_response(),
        Err(e) => failure(e),
    }
}

/// Why a call never reached the runtime.
enum Refusal {
    /// No token, or not the runner's: 401.
    Unauthorized,
    /// A body the runner does not take.
    Invalid(RuntimeError),
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        match self {
            Refusal::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response(),
            Refusal::Invalid(e) => failure(e),
        }
    }
}

struct Inner {
    runtime: Arc<dyn ComputeRuntime>,
    policy: RunnerPolicy,
    token_digest: [u8; 32],
}

impl Inner {
    /// The request's body, once its token is checked.
    fn decode<T: DeserializeOwned>(&self, headers: &HeaderMap, body: &[u8]) -> Result<T, Refusal> {
        let presented = headers
            .get(header::AUTHORIZATION)
            .filter(|v| v.len() <= MAX_AUTH_BYTES)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| Sha256::digest(token.trim().as_bytes()));
        let authorized = presented.is_some_and(|got| bool::from(got.ct_eq(&self.token_digest)));
        if !authorized {
            tracing::warn!("compute runner: a call without the runner's token was refused");
            return Err(Refusal::Unauthorized);
        }
        serde_json::from_slice(body).map_err(|e| {
            Refusal::Invalid(RuntimeError::InvalidArgument(format!(
                "the request body: {e}"
            )))
        })
    }

    fn endpoint(&self, w: WireEndpoint) -> Result<(String, EndpointRec), Refusal> {
        let (ns, ep) = w.record();
        check_namespace(&ns).map_err(Refusal::Invalid)?;
        RunnerPolicy::check_endpoint(&ep).map_err(Refusal::Invalid)?;
        Ok((ns, ep))
    }

    /// A compute call's body, its namespace checked.
    fn compute<T: DeserializeOwned + Scoped>(
        &self,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<T, Refusal> {
        let w: T = self.decode(headers, body)?;
        check_namespace(w.namespace()).map_err(Refusal::Invalid)?;
        Ok(w)
    }
}

/// A body naming a namespace.
trait Scoped {
    fn namespace(&self) -> &str;
}

impl Scoped for WireCompute {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Scoped for WireResize {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

type Shared = State<Arc<Inner>>;

async fn ensure_address(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let (ns, ep) = match s.decode(&headers, &body).and_then(|w| s.endpoint(w)) {
        Ok(v) => v,
        Err(refused) => return refused.into_response(),
    };
    answer(s.runtime.ensure_address(&ns, &ep).await)
}

async fn remove_address(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let (ns, ep) = match s.decode(&headers, &body).and_then(|w| s.endpoint(w)) {
        Ok(v) => v,
        Err(refused) => return refused.into_response(),
    };
    answer(s.runtime.remove_address(&ns, &ep).await)
}

async fn start(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let launch = match s.decode::<WireLaunch>(&headers, &body) {
        Ok(w) => w.launch(),
        Err(refused) => return refused.into_response(),
    };
    if let Err(e) = s.policy.check_launch(&launch) {
        tracing::warn!(compute = %launch.compute_id, error = %e, "compute runner: start refused");
        return failure(e);
    }
    tracing::info!(compute = %launch.compute_id, image = %launch.image, "compute runner: start");
    answer(s.runtime.start(&launch).await)
}

async fn bind_address(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let ((ns, ep), handle) = match s
        .decode::<WireBind>(&headers, &body)
        .and_then(|w| Ok((s.endpoint(w.endpoint)?, w.handle)))
    {
        Ok(v) => v,
        Err(refused) => return refused.into_response(),
    };
    answer(s.runtime.bind_address(&ns, &ep, &handle).await)
}

async fn stop(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let w = match s.compute::<WireCompute>(&headers, &body) {
        Ok(w) => w,
        Err(refused) => return refused.into_response(),
    };
    tracing::info!(compute = %w.compute_id, "compute runner: stop");
    answer(s.runtime.stop(&w.namespace, &w.compute_id).await)
}

async fn status(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let w = match s.compute::<WireCompute>(&headers, &body) {
        Ok(w) => w,
        Err(refused) => return refused.into_response(),
    };
    answer(s.runtime.status(&w.namespace, &w.compute_id).await)
}

async fn resize(State(s): Shared, headers: HeaderMap, body: Bytes) -> Response {
    let w = match s.compute::<WireResize>(&headers, &body) {
        Ok(w) => w,
        Err(refused) => return refused.into_response(),
    };
    if w.cu < MIN_CU || w.cu > MAX_CU {
        return failure(RuntimeError::InvalidArgument(format!(
            "a compute is 0.25 to 16 CU, not {} quarters",
            w.cu.0
        )));
    }
    answer(s.runtime.resize(&w.namespace, &w.compute_id, w.cu).await)
}

async fn health() -> &'static str {
    "ok\n"
}

/// The runner's listener, bound. [`serve`](Self::serve) runs it.
pub struct RunnerServer {
    listener: TcpListener,
    router: Router,
}

impl fmt::Debug for RunnerServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnerServer")
            .field("listen", &self.listener.local_addr().ok())
            .finish_non_exhaustive()
    }
}

impl RunnerServer {
    /// Binds `listen` for `runtime`, behind `policy` and `token`.
    ///
    /// # Errors
    ///
    /// The bind's.
    pub async fn bind(
        listen: SocketAddr,
        runtime: Arc<dyn ComputeRuntime>,
        policy: RunnerPolicy,
        token: Secret<String>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(listen).await?;
        let inner = Arc::new(Inner {
            runtime,
            policy,
            token_digest: Sha256::digest(token.expose().as_bytes()).into(),
        });
        let router = Router::new()
            .route("/health", get(health))
            .route("/v1/ensure-address", post(ensure_address))
            .route("/v1/remove-address", post(remove_address))
            .route("/v1/start", post(start))
            .route("/v1/bind-address", post(bind_address))
            .route("/v1/stop", post(stop))
            .route("/v1/status", post(status))
            .route("/v1/resize", post(resize))
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
            .with_state(inner);
        Ok(RunnerServer { listener, router })
    }

    /// The bound address (port 0 resolved).
    ///
    /// # Panics
    ///
    /// Never: the socket is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("a bound listener has an address")
    }

    /// Serves until `shutdown` resolves, then lets the calls in flight
    /// finish.
    pub async fn serve(self, shutdown: impl Future<Output = ()> + Send + 'static) {
        if let Err(e) = axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::error!(error = %e, "the compute runner's listener stopped");
        }
    }
}

/// The runner's token from `path` (trimmed). With `create`, a missing file
/// is created first: 32 random bytes as base64url, mode 0600, its
/// directory created 0700 when missing.
///
/// # Errors
///
/// A message when the file cannot be read or written, or holds no token.
pub fn read_or_create_token(path: &Path, create: bool) -> Result<Secret<String>, String> {
    let shown = path.display();
    if create && !path.exists() {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            let mut b = std::fs::DirBuilder::new();
            b.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                b.mode(0o700);
            }
            b.create(dir)
                .map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let token = generate_password().map_err(|e| e.to_string())?;
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(path) {
            Ok(mut f) => {
                f.write_all(format!("{}\n", token.expose()).as_bytes())
                    .and_then(|()| f.sync_all())
                    .map_err(|e| format!("{shown}: {e}"))?;
                return Ok(token);
            }
            // Another process made it first: read theirs.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(format!("{shown}: {e}")),
        }
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{shown}: {e}"))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!("{shown} holds no token"));
    }
    Ok(Secret::new(token.to_owned()))
}
