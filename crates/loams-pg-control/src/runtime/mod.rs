//! Where computes run (design §46 §7.2, D707; PG2 Task 10).
//!
//! [`ComputeRuntime`] is the seam the endpoint reconciler (Task 11) drives:
//! it gives each endpoint a stable address, starts and stops computes, and
//! reports what it sees. Two runtimes implement it: [`compose`] (single-node
//! mode, the desktop, CI, and the preview release's Docker Compose stack)
//! and Kubernetes (`kubernetes`, feature `kubernetes`, Task 13). [`certs`] issues the computes' TLS
//! certificates from a local CA; [`images`] names the pinned compute image
//! of each Postgres major (Task 11).
//!
//! **The compute runner** (the preview release stack, R-REL.1): `pg-control`
//! never holds the container runtime's socket there. A separate process,
//! `loams pg-runner`, holds it and serves this trait over HTTP
//! ([`runner`]), checking every call against the images it may run;
//! `pg-control` drives it through [`remote::RemoteRuntime`].
//!
//! **Replace, never restart** (§46 §7.1). A compute that stops is not
//! started again under the same id: the reconciler starts a new compute
//! (a new [`ComputeId`]) behind the same address. A runtime therefore never
//! restarts a container or Pod by itself.
//!
//! **Every compute runs `compute_ctl`** with `--control-plane-uri` (the
//! upcall listener, Task 8), its compute id, and its upcall token in
//! `NEON_CONTROL_PLANE_TOKEN`. Postgres listens on [`COMPUTE_PG_PORT`] on
//! every address of the compute (the upcall's spec says so), and
//! `compute_ctl`'s external API on [`COMPUTE_CTL_PORT`].

pub mod certs;
pub mod compose;
pub mod images;
#[cfg(feature = "kubernetes")]
pub mod kubernetes;
pub mod remote;
pub mod runner;

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ids::ComputeId;
use crate::model::{ComputeBinding, Cu, EndpointRec};
use crate::secrets::Secret;

/// The port Postgres listens on inside a compute. The upcall's spec sets
/// `port` and `listen_addresses = '*'`, and `compute_ctl`'s own connection
/// string names it. `deploy/loams-postgres-dev`'s computes use the same.
pub const COMPUTE_PG_PORT: u16 = 55433;

/// `compute_ctl`'s external HTTP API (`/status`, `/configure`,
/// `/terminate`), its default `--external-http-port`.
pub const COMPUTE_CTL_PORT: u16 = 3080;

/// `compute_ctl`'s internal HTTP API (the `neon` extension and
/// `local_proxy`), its default `--internal-http-port`.
pub const COMPUTE_CTL_INTERNAL_PORT: u16 = 3081;

/// The longest a runtime command (`compose up`, `compose rm`, …) may take
/// before it is killed and reported as [`RuntimeError::Timeout`].
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// An endpoint's stable address: a compose host port, or a Kubernetes
/// Service. It is what `E/`'s `backend_addr` records, as `host:port`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackendAddr {
    pub host: String,
    pub port: u16,
}

impl BackendAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        BackendAddr {
            host: host.into(),
            port,
        }
    }

    /// `host:port`, as `E/`'s `backend_addr` holds it (an IPv6 host in
    /// brackets).
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`] for anything else.
    pub fn parse(s: &str) -> Result<Self, RuntimeError> {
        let bad = || RuntimeError::InvalidArgument(format!("{s:?} is not host:port"));
        let (host, port) = s.rsplit_once(':').ok_or_else(bad)?;
        let host = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host);
        if host.is_empty() || (host.contains(':') && !s.starts_with('[')) {
            return Err(bad());
        }
        let port = port.parse().map_err(|_| bad())?;
        Ok(BackendAddr::new(host, port))
    }
}

impl fmt::Display for BackendAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// What a runtime needs to start one compute.
#[derive(Debug, Clone)]
pub struct ComputeLaunch {
    pub compute_id: ComputeId,
    /// The Loams namespace it runs in (R13.1). A bound launch's binding
    /// names the same one ([`namespace`](Self::namespace) checks it); an
    /// unbound warm-pool compute (Task 14) has only this.
    pub namespace: String,
    /// The endpoint it serves; `None` for an unbound warm-pool compute
    /// (Task 14), whose spec upcall answers `empty`.
    pub binding: Option<ComputeBinding>,
    /// The endpoint's address ([`ComputeRuntime::ensure_address`]); `None`
    /// for an unbound compute.
    pub address: Option<BackendAddr>,
    /// The compute image, pinned by digest (`repo@sha256:<64 hex>`).
    pub image: String,
    /// The compute's size (§46 §7.4: 1 CU is 1 vCPU and 4 GiB).
    pub cu: Cu,
    /// The compute's upcall token (`ComputeKeys::issue_upcall_token`,
    /// `COMPUTE_TOKEN_TTL`), handed to `compute_ctl` as
    /// `NEON_CONTROL_PLANE_TOKEN`. Never logged or written where anything
    /// but the compute reads it.
    pub upcall_token: Secret<String>,
}

impl ComputeLaunch {
    /// The launch's Loams namespace, once checked against its binding's.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`] when the binding names another
    /// namespace, or an unbound launch names an address.
    pub fn namespace(&self) -> Result<&str, RuntimeError> {
        match &self.binding {
            Some(b) if b.namespace != self.namespace => {
                Err(RuntimeError::InvalidArgument(format!(
                    "the launch's namespace {:?} is not its binding's ({:?})",
                    self.namespace, b.namespace
                )))
            }
            None if self.address.is_some() => Err(RuntimeError::InvalidArgument(
                "an unbound compute has no address".into(),
            )),
            _ => Ok(&self.namespace),
        }
    }
}

/// A started compute, as the runtime reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeHandle {
    pub compute_id: ComputeId,
    /// `compute_ctl`'s external API, as `pg-control` reaches it: the `C/`
    /// record's `ctl_url`.
    pub ctl_url: String,
    /// The address the compute serves, when it serves one.
    pub address: Option<BackendAddr>,
}

/// What the runtime sees of a compute: its container or Pod, not
/// `compute_ctl`'s own state (that is `/status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeStatus {
    /// No container or Pod: never started, removed, or unknown.
    Missing,
    /// Created and not running yet, or in a transition.
    Pending,
    Running,
    /// Stopped by itself or killed; a runtime does not restart it.
    Exited {
        code: Option<i32>,
    },
}

/// The answer to [`ComputeRuntime::resize`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResizeOutcome {
    Applied,
    /// The runtime could not resize now (for example, no room on the
    /// node); the autoscaler retries (Task 15).
    Refused(String),
    /// The runtime cannot resize a running compute at all.
    Unsupported,
}

/// Why a runtime call failed. The text never holds a token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum RuntimeError {
    /// A launch, endpoint or address the runtime cannot use; retrying the
    /// same call fails the same way.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Every port of the range is taken.
    #[error("no free host port in {start}..{end}")]
    PortsExhausted { start: u16, end: u16 },
    /// The runtime's tool or API failed, or its files could not be read or
    /// written; a retry may succeed.
    #[error("compute runtime unavailable: {0}")]
    Unavailable(String),
    /// A command did not finish in time and was killed.
    #[error("{what} did not finish within {after:?}")]
    Timeout { what: String, after: Duration },
}

/// Where computes run (PG2's shared contract).
///
/// Every call but `start` names the Loams namespace of the endpoint or
/// compute it is about (`ns`): a Kubernetes runtime keeps each namespace's
/// computes and Services in `loams-pg-<ns>`, and its Role reaches no other
/// (R13.1; a launch names it in its binding).
#[async_trait]
pub trait ComputeRuntime: Send + Sync + 'static {
    /// The endpoint's stable address, allocated on first call and the same
    /// on every later one. An endpoint whose record already names an
    /// address keeps it. Idempotent.
    async fn ensure_address(&self, ns: &str, ep: &EndpointRec)
    -> Result<BackendAddr, RuntimeError>;

    /// Gives up the endpoint's address (the endpoint is being deleted).
    /// Idempotent. *(Task 10: not in the contract's first text; Task 11's
    /// `delete_endpoint_stops_and_removes_address` needs it.)*
    async fn remove_address(&self, ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError>;

    /// Starts the compute. Idempotent by compute id: a second call for a
    /// compute that exists answers its handle and starts nothing.
    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError>;

    /// Points the endpoint's address at the compute.
    async fn bind_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError>;

    /// Stops and removes the compute. Idempotent: an unknown compute is
    /// not an error.
    async fn stop(&self, ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError>;

    async fn status(&self, ns: &str, compute_id: &ComputeId)
    -> Result<RuntimeStatus, RuntimeError>;

    /// Changes a running compute's CPU and memory (§46 §7.4).
    async fn resize(
        &self,
        ns: &str,
        compute_id: &ComputeId,
        cu: Cu,
    ) -> Result<ResizeOutcome, RuntimeError>;
}

/// `compute_ctl`'s arguments in every runtime (R10.3): the data
/// directory, the binary, its own connection string, the compute's id, the
/// upcall listener and its two HTTP ports.
pub(crate) fn compute_ctl_args(compute_id: &str, control_plane_uri: &str) -> Vec<String> {
    [
        "--pgdata",
        "/var/db/postgres/compute",
        "--pgbin",
        "/usr/local/bin/postgres",
        "--connstr",
        &format!("postgresql://cloud_admin@127.0.0.1:{COMPUTE_PG_PORT}/postgres"),
        "--compute-id",
        compute_id,
        "--control-plane-uri",
        control_plane_uri,
        "--external-http-port",
        &COMPUTE_CTL_PORT.to_string(),
        "--internal-http-port",
        &COMPUTE_CTL_INTERNAL_PORT.to_string(),
    ]
    .map(str::to_owned)
    .to_vec()
}

/// The longest upcall token a runtime takes ([`check_upcall_token`]).
pub const MAX_UPCALL_TOKEN_BYTES: usize = 8192;

/// Checks a compute's upcall token before a runtime writes it anywhere: 1
/// to [`MAX_UPCALL_TOKEN_BYTES`] bytes of the characters a JWT (base64url
/// and dots) or a base64 token uses. Anything else could leave its line in
/// the compose runtime's env file and set another variable.
///
/// # Errors
///
/// [`RuntimeError::InvalidArgument`], which never repeats the token.
pub fn check_upcall_token(token: &str) -> Result<(), RuntimeError> {
    let ok = !token.is_empty()
        && token.len() <= MAX_UPCALL_TOKEN_BYTES
        && token.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'+' | b'/' | b'=')
        });
    if ok {
        Ok(())
    } else {
        Err(RuntimeError::InvalidArgument(format!(
            "the upcall token must be 1 to {MAX_UPCALL_TOKEN_BYTES} bytes of base64url, base64 \
             and dots"
        )))
    }
}

/// Whether `image` is pinned by digest: `<name>@sha256:<64 lower-case
/// hex>` (Global Constraints: images are pinned by digest).
pub fn is_pinned(image: &str) -> bool {
    image.split_once("@sha256:").is_some_and(|(name, hex)| {
        !name.is_empty()
            && !name.contains(char::is_whitespace)
            && hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_addrs_round_trip() {
        for s in [
            "127.0.0.1:55500",
            "ep-x.loams-pg-acme.svc:5432",
            "[::1]:55500",
        ] {
            assert_eq!(BackendAddr::parse(s).expect(s).to_string(), s);
        }
        for s in ["", "host", ":1", "host:x", "host:70000", "::1:5"] {
            assert!(BackendAddr::parse(s).is_err(), "{s}");
        }
    }

    #[test]
    fn only_digests_are_pinned() {
        let hex = "13ab146d3e7bbabb25a8532f315ac443e7512351d1ede0bab586def5c70e26c3";
        assert!(is_pinned(&format!(
            "ghcr.io/x/compute-node-v17@sha256:{hex}"
        )));
        assert!(!is_pinned("ghcr.io/x/compute-node-v17:latest"));
        assert!(!is_pinned(&format!("@sha256:{hex}")));
        assert!(!is_pinned(&format!("x@sha256:{}", &hex[1..])));
        assert!(!is_pinned(&format!("x@sha256:{}", hex.to_uppercase())));
    }
}
