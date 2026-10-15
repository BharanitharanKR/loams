//! `compute_ctl`'s HTTP API, with the per-compute JWT `pg-control` signs
//! (§46 §4.3). Shapes from the fork's `libs/compute_api/src/{requests.rs,
//! responses.rs}` and routes from `compute_tools/src/http/server.rs`.
//!
//! `configure`, `terminate` and `promote` block in `compute_ctl` until the
//! compute gets there, so they have long timeouts of their own. A timeout
//! does not mean the call failed: the caller polls [`ComputeCtlClient::status`]
//! and repeats the call if the compute did not move (each of the three is
//! safe to repeat: `compute_ctl` joins a promotion already running).

use std::time::Duration;

use reqwest::Method;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::Op;
use crate::http::{DEFAULT_TIMEOUT, Http};
use crate::spec::ComputeSpec;
use crate::{Component, Lsn, NeonError, Secret};

/// A client of one compute's `compute_ctl`.
#[derive(Clone, Debug)]
pub struct ComputeCtlClient {
    http: Http,
}

/// `ComputeStatus`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    /// Waiting for a spec (a warm-pool compute, §46 §7.3).
    Empty,
    ConfigurationPending,
    Init,
    Running,
    Configuration,
    Failed,
    TerminationPendingFast,
    TerminationPendingImmediate,
    Terminated,
    RefreshConfigurationPending,
    RefreshConfiguration,
}

/// `ComputeStatusResponse`, the fields Loams reads.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ComputeStatusResponse {
    pub status: ComputeStatus,
    pub tenant: Option<String>,
    pub timeline: Option<String>,
    pub error: Option<String>,
    /// The last moment Postgres had activity that should keep the compute
    /// from suspending, as `compute_ctl`'s monitor (`compute_tools/src/
    /// monitor.rs`, every 500 ms) last saw it: it is now while a client
    /// backend is not `idle` (a query running, a transaction open), a
    /// walsender other than walproposer exists, a logical subscription's
    /// worker runs, or autovacuum works; otherwise the latest `idle`
    /// backend's `state_change`. It only moves forward. RFC 3339 as chrono's
    /// `to_rfc3339` writes it (`compute_api/src/responses.rs`
    /// `rfc3339_serialize`); `None` before any activity. Read it with
    /// [`last_active_ms`](Self::last_active_ms).
    #[serde(default)]
    pub last_active: Option<String>,
}

impl ComputeStatusResponse {
    /// [`last_active`](Self::last_active) in ms since the Unix epoch (0 for
    /// a time before it).
    ///
    /// # Errors
    ///
    /// When `compute_ctl` sent a time that is not RFC 3339.
    pub fn last_active_ms(&self) -> Result<Option<u64>, time::error::Parse> {
        let Some(at) = &self.last_active else {
            return Ok(None);
        };
        let at = time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339)?;
        let ms = at.unix_timestamp_nanos() / 1_000_000;
        Ok(Some(u64::try_from(ms).unwrap_or(0)))
    }
}

/// `ComputeCtlConfig`: the keys `compute_ctl` verifies callers' JWTs with,
/// and its TLS settings.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComputeCtlConfig {
    pub jwks: serde_json::Value,
    pub tls: Option<serde_json::Value>,
}

impl Default for ComputeCtlConfig {
    fn default() -> Self {
        Self {
            jwks: serde_json::json!({ "keys": [] }),
            tls: None,
        }
    }
}

/// `COMPUTE_AUDIENCE` (`compute_api/src/requests.rs`): the `aud` of a token
/// for `compute_ctl`'s own API. `compute_ctl` checks only the token's
/// `compute_id` for a token without the admin scope, as `pg-control` signs
/// them.
pub const COMPUTE_AUDIENCE: &str = "compute";

/// `ControlPlaneComputeStatus` (`compute_api/src/responses.rs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneComputeStatus {
    /// Known, but bound to no endpoint yet: `compute_ctl` waits for
    /// `/configure` (a warm-pool compute, §46 §7.3).
    Empty,
    /// Bound: `spec` is what it runs.
    Attached,
}

/// `ControlPlaneConfigResponse` (`compute_api/src/responses.rs`): the answer
/// to `compute_ctl`'s `GET {control-plane-uri}/compute/api/v2/computes/{id}/spec`
/// (`compute_tools/src/spec.rs` `get_config_from_control_plane`), which
/// `pg-control`'s upcall listener serves (§28 §5.2). `spec` is `null` when
/// `status` is `empty`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ControlPlaneConfigResponse {
    pub spec: Option<ComputeSpec>,
    pub status: ControlPlaneComputeStatus,
    pub compute_ctl_config: ComputeCtlConfig,
}

/// `TerminateMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminateMode {
    /// Wait 30 s before answering, so a control plane sees an error.
    Fast,
    /// Answer once everything has stopped.
    Immediate,
}

/// How long `configure` waits: `compute_ctl` answers once the spec is
/// applied (a cold start includes the basebackup).
pub const CONFIGURE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long `terminate` waits: `fast` holds the answer 30 s on purpose.
pub const TERMINATE_TIMEOUT: Duration = Duration::from_secs(120);
/// How long `promote` waits: the replica first replays to `wal_flush_lsn`.
pub const PROMOTE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Serialize)]
struct ConfigurationRequest<'a> {
    spec: &'a ComputeSpec,
    compute_ctl_config: &'a ComputeCtlConfig,
}

#[derive(Serialize)]
struct PromoteConfig<'a> {
    spec: &'a ComputeSpec,
    wal_flush_lsn: Lsn,
}

/// `PromoteState`: `completed` (with its timings), `failed` (a 500 with
/// `error`) or `not_promoted`.
#[derive(Deserialize)]
struct PromoteState {
    status: String,
}

#[derive(Deserialize)]
struct TerminateResponse {
    lsn: Option<Lsn>,
}

/// `LfcPrewarmState`: `status` is `not_prewarmed`, `prewarming`,
/// `completed`, `skipped`, `failed` (with `error`) or `cancelled`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct PrewarmState {
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
}

impl ComputeCtlClient {
    /// `jwt` is the compute's own token, signed by `pg-control`. A URL with
    /// credentials is refused.
    pub fn new(base: Url, jwt: Secret<String>) -> Result<Self, NeonError> {
        Ok(Self {
            http: Http::new(Component::ComputeCtl, base, Some(jwt))?,
        })
    }

    /// `GET /status`.
    pub async fn status(&self) -> Result<ComputeStatusResponse, NeonError> {
        self.http.get(Op::ComputeStatus, "/status").await
    }

    /// `POST /configure`: hand a waiting compute its spec, or reconfigure a
    /// running one. Waits up to [`CONFIGURE_TIMEOUT`].
    pub async fn configure(
        &self,
        spec: &ComputeSpec,
        config: &ComputeCtlConfig,
    ) -> Result<ComputeStatusResponse, NeonError> {
        let body = ConfigurationRequest {
            spec,
            compute_ctl_config: config,
        };
        let req = self
            .http
            .request(Method::POST, "/configure", CONFIGURE_TIMEOUT)
            .json(&body);
        self.http.send(Op::Configure, req).await
    }

    /// `POST /terminate?mode=`: the LSN Postgres stopped at, when it knows
    /// it. Waits up to [`TERMINATE_TIMEOUT`].
    pub async fn terminate(&self, mode: TerminateMode) -> Result<Option<Lsn>, NeonError> {
        let req = self
            .http
            .request(Method::POST, "/terminate", TERMINATE_TIMEOUT)
            .query(&[("mode", mode)]);
        let resp: TerminateResponse = self.http.send(Op::Terminate, req).await?;
        Ok(resp.lsn)
    }

    /// `POST /promote`: a replica becomes the primary, with `spec` (in
    /// `Primary` mode) once its WAL reaches `wal_flush_lsn`. Waits up to
    /// [`PROMOTE_TIMEOUT`].
    ///
    /// `Ok` only when `compute_ctl` answers `completed`. `compute_ctl`
    /// refuses a replica whose local file cache is not prewarmed ("compute
    /// NotPrewarmed"), so a failover target is started with `autoprewarm`
    /// and endpoint storage, or prewarmed with [`Self::prewarm`] first
    /// (R2.13). A refusal or a failed promotion is `failed_precondition`:
    /// replace the replica (or prewarm it and retry).
    pub async fn promote(&self, spec: &ComputeSpec, wal_flush_lsn: Lsn) -> Result<(), NeonError> {
        let op = Op::Promote;
        let body = PromoteConfig {
            spec,
            wal_flush_lsn,
        };
        let req = self
            .http
            .request(Method::POST, "/promote", PROMOTE_TIMEOUT)
            .json(&body);
        let state: PromoteState = self.http.send(op, req).await?;
        if state.status == "completed" {
            Ok(())
        } else {
            Err(NeonError {
                status: 200,
                msg: format!("promotion answered {:?}", state.status),
                component: Component::ComputeCtl,
                op,
            })
        }
    }

    /// `POST /lfc/prewarm[?from_endpoint=]`: start filling the local file
    /// cache from the state endpoint storage holds (this endpoint's, or
    /// `from_endpoint`'s). Accepted (202); a second request while one runs
    /// is 429 (`aborted`). Follow it with [`Self::prewarm_state`].
    pub async fn prewarm(&self, from_endpoint: Option<&str>) -> Result<(), NeonError> {
        let mut req = self
            .http
            .request(Method::POST, "/lfc/prewarm", DEFAULT_TIMEOUT);
        if let Some(ep) = from_endpoint {
            req = req.query(&[("from_endpoint", ep)]);
        }
        let _: serde_json::Value = self.http.send(Op::Prewarm, req).await?;
        Ok(())
    }

    /// `GET /lfc/prewarm`.
    pub async fn prewarm_state(&self) -> Result<PrewarmState, NeonError> {
        self.http.get(Op::PrewarmState, "/lfc/prewarm").await
    }

    /// `GET /autoscaling_metrics`: the local file cache's size and working
    /// set, which the `neon` extension's communicator exports (unauthenticated
    /// in `compute_ctl`).
    pub async fn autoscaling_metrics(&self) -> Result<FileCacheMetrics, NeonError> {
        let req = self
            .http
            .request(Method::GET, "/autoscaling_metrics", DEFAULT_TIMEOUT);
        let text = self.http.text(Op::AutoscalingMetrics, req).await?;
        Ok(FileCacheMetrics::parse(&text))
    }
}

/// The local file cache, as `/autoscaling_metrics` reports it
/// (`pgxn/neon/communicator/src/worker_process/lfc_metrics.rs`). `None`
/// where the exposition lacks the series.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileCacheMetrics {
    /// `lfc_cache_size_limit`, in bytes.
    pub size_bytes: Option<u64>,
    /// `lfc_approximate_working_set_size_windows{duration_seconds="300"}`
    /// (pages of 8 KiB), in bytes: the pages touched in the last 5 minutes.
    pub working_set_bytes: Option<u64>,
}

/// The working-set window the autoscaler reads.
pub const WORKING_SET_WINDOW_S: u32 = 300;

impl FileCacheMetrics {
    /// Reads the two series out of a Prometheus text exposition; anything
    /// else is ignored.
    pub fn parse(text: &str) -> Self {
        let window = format!(
            "lfc_approximate_working_set_size_windows{{duration_seconds=\"{WORKING_SET_WINDOW_S}\"}}"
        );
        let mut out = FileCacheMetrics::default();
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let Some((series, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.trim().parse::<f64>() else {
                continue;
            };
            if !value.is_finite() || value < 0.0 {
                continue;
            }
            // Gauges are integers here; f64 reads the exposition's format.
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let value = value as u64;
            match series.trim() {
                "lfc_cache_size_limit" => out.size_bytes = Some(value),
                s if s == window => out.working_set_bytes = Some(value.saturating_mul(8192)),
                _ => {}
            }
        }
        out
    }
}
