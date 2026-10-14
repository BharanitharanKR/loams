//! [`ComputeRuntime`] over HTTP: `pg-control`'s side of the compute runner
//! ([`super::runner`], R-REL.1). `loams pg-control --compute-runner <url>`
//! drives computes through it, holding no container runtime socket itself.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use super::runner::{WireBind, WireCompute, WireEndpoint, WireError, WireLaunch, WireResize};
use super::{
    BackendAddr, COMMAND_TIMEOUT, ComputeHandle, ComputeLaunch, ComputeRuntime, ResizeOutcome,
    RuntimeError, RuntimeStatus,
};
use crate::ids::ComputeId;
use crate::model::{Cu, EndpointRec};
use crate::secrets::Secret;

/// How long one call may take: a compose start can remove the endpoint's
/// previous compute, look, and bring the new one up, each command up to
/// [`COMMAND_TIMEOUT`].
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(4 * COMMAND_TIMEOUT.as_secs() + 30);

/// A compute runner, reached over HTTP. See the module doc.
pub struct RemoteRuntime {
    base: Url,
    token: Secret<String>,
    http: reqwest::Client,
}

impl fmt::Debug for RemoteRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteRuntime")
            .field("base", &self.base.as_str())
            .finish_non_exhaustive()
    }
}

impl RemoteRuntime {
    /// The runner at `base` (`http://` or `https://`), called with `token`.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidArgument`] for another scheme;
    /// [`RuntimeError::Unavailable`] when the HTTP client cannot be built.
    pub fn new(base: Url, token: Secret<String>) -> Result<Self, RuntimeError> {
        if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
            return Err(RuntimeError::InvalidArgument(format!(
                "the compute runner's URL must be http:// or https://, not {:?}",
                base.as_str()
            )));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| RuntimeError::Unavailable(format!("the compute runner's client: {e}")))?;
        Ok(RemoteRuntime { base, token, http })
    }

    async fn call<B: Serialize, T: DeserializeOwned>(
        &self,
        route: &str,
        body: &B,
    ) -> Result<T, RuntimeError> {
        let url = self
            .base
            .join(route)
            .map_err(|e| RuntimeError::InvalidArgument(format!("{route}: {e}")))?;
        let unavailable =
            |e: reqwest::Error| RuntimeError::Unavailable(format!("the compute runner: {e}"));
        let resp = self
            .http
            .post(url)
            .bearer_auth(self.token.expose())
            .json(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    RuntimeError::Timeout {
                        what: format!("the compute runner's {route}"),
                        after: REQUEST_TIMEOUT,
                    }
                } else {
                    unavailable(e)
                }
            })?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(unavailable)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|e| {
                RuntimeError::Unavailable(format!(
                    "the compute runner's {route} answered unreadable JSON: {e}"
                ))
            });
        }
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RuntimeError::Unavailable(
                "the compute runner refused the token (--compute-runner-token-file)".into(),
            ));
        }
        match serde_json::from_slice::<WireError>(&bytes) {
            Ok(w) => Err(w.error),
            Err(_) => Err(RuntimeError::Unavailable(format!(
                "the compute runner's {route} answered {status}"
            ))),
        }
    }
}

#[async_trait]
impl ComputeRuntime for RemoteRuntime {
    async fn ensure_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
    ) -> Result<BackendAddr, RuntimeError> {
        self.call("v1/ensure-address", &WireEndpoint::of(ns, ep))
            .await
    }

    async fn remove_address(&self, ns: &str, ep: &EndpointRec) -> Result<(), RuntimeError> {
        self.call("v1/remove-address", &WireEndpoint::of(ns, ep))
            .await
    }

    async fn start(&self, c: &ComputeLaunch) -> Result<ComputeHandle, RuntimeError> {
        self.call("v1/start", &WireLaunch::of(c)).await
    }

    async fn bind_address(
        &self,
        ns: &str,
        ep: &EndpointRec,
        c: &ComputeHandle,
    ) -> Result<(), RuntimeError> {
        let body = WireBind {
            endpoint: WireEndpoint::of(ns, ep),
            handle: c.clone(),
        };
        self.call("v1/bind-address", &body).await
    }

    async fn stop(&self, ns: &str, compute_id: &ComputeId) -> Result<(), RuntimeError> {
        let body = WireCompute {
            namespace: ns.to_owned(),
            compute_id: *compute_id,
        };
        self.call("v1/stop", &body).await
    }

    async fn status(
        &self,
        ns: &str,
        compute_id: &ComputeId,
    ) -> Result<RuntimeStatus, RuntimeError> {
        let body = WireCompute {
            namespace: ns.to_owned(),
            compute_id: *compute_id,
        };
        self.call("v1/status", &body).await
    }

    async fn resize(
        &self,
        ns: &str,
        compute_id: &ComputeId,
        cu: Cu,
    ) -> Result<ResizeOutcome, RuntimeError> {
        let body = WireResize {
            namespace: ns.to_owned(),
            compute_id: *compute_id,
            cu,
        };
        self.call("v1/resize", &body).await
    }
}
