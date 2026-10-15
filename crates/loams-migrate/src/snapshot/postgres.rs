//! Loams Postgres storage in an upgrade (design §54 §7.2, §9.3; D945; UP1
//! Task 9), through the pageserver's and the safekeeper's management APIs
//! (ruling T0-9). `migrate snapshot` calls them from its own container; no
//! other image reaches them.
//!
//! - **Drain** before `safekeeper1` is recreated: with the computes stopped,
//!   each tenant's `checkpoint_timeout` is lowered (the pinned pageserver's
//!   checkpoint API is testing-only), every timeline waits until the
//!   pageserver's `remote_consistent_lsn` reaches the safekeeper's
//!   `flush_lsn`, and the tenant's own setting is put back. A timeline still
//!   behind at the timeout is `postgres_not_drained`.
//! - **Branch** every timeline at its last record LSN: the snapshot's
//!   Postgres component. Restoring points the computes (or `pg-control`'s
//!   `RestoreBranch`) back at the branches.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use loams_postgres::pageserver::{NeonClient, NeonEndpoints, TenantConfig, TimelineCreate};
use loams_postgres::wal::WalClient;
use loams_postgres::{Lsn, TenantId, TimelineId};
use serde::{Deserialize, Serialize};
use url::Url;

use super::SnapshotError;

/// The `checkpoint_timeout` a drain sets, so the pageserver uploads what it
/// ingested promptly.
pub const DRAIN_CHECKPOINT_TIMEOUT: &str = "5s";
/// How long a drain waits by default (§54 §7.2's target).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);

const CHECKPOINT_TIMEOUT: &str = "checkpoint_timeout";

fn neon(what: &str, e: impl std::fmt::Display) -> SnapshotError {
    SnapshotError::Io(format!("{what}: {e}"))
}

/// The Postgres storage of a stack: one pageserver and one safekeeper.
#[derive(Clone, Debug)]
pub struct PostgresStorage {
    pageserver: NeonClient,
    pageserver_url: Url,
    safekeeper: WalClient,
}

/// One timeline's state in a drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineDrain {
    pub tenant: String,
    pub timeline: String,
    pub remote_consistent_lsn: String,
    pub flush_lsn: String,
    pub drained: bool,
}

/// A branch an upgrade snapshot made.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBranch {
    pub tenant: String,
    /// The timeline the branch was made from.
    pub timeline: String,
    /// The branch's timeline id.
    pub branch: String,
    /// Where: the timeline's last record LSN.
    pub lsn: String,
}

/// `<snapshot>/postgres/branches.json`: the drain's final state and the
/// branches made after it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Branches {
    pub drained: Vec<TimelineDrain>,
    pub branches: Vec<SnapshotBranch>,
}

impl PostgresStorage {
    /// Clients of the pageserver (`http://pageserver:9898`) and the
    /// safekeeper (`http://safekeeper1:7676`).
    pub fn new(pageserver: &str, safekeeper: &str) -> Result<Self, SnapshotError> {
        let url = |u: &str| Url::parse(u).map_err(|e| neon(u, e));
        let pageserver_url = url(pageserver)?;
        let pageserver = NeonClient::new(
            NeonEndpoints {
                pageserver: pageserver_url.clone(),
                storcon: None,
            },
            None,
        )
        .map_err(|e| neon("the pageserver", e))?;
        let safekeeper =
            WalClient::new(url(safekeeper)?, None).map_err(|e| neon("the safekeeper", e))?;
        Ok(PostgresStorage {
            pageserver,
            pageserver_url,
            safekeeper,
        })
    }

    /// Whether this stack runs Loams Postgres: `false` when the pageserver's
    /// name does not resolve (a stack composed without it,
    /// `postgres-off.yaml`); any other failure is an error, never "absent".
    pub async fn present(&self) -> Result<bool, SnapshotError> {
        let host = self
            .pageserver_url
            .host_str()
            .unwrap_or_default()
            .to_string();
        let port = self.pageserver_url.port_or_known_default().unwrap_or(80);
        if tokio::net::lookup_host((host.as_str(), port))
            .await
            .is_err()
        {
            return Ok(false);
        }
        self.pageserver
            .list_tenants()
            .await
            .map(|_| true)
            .map_err(|e| neon("the pageserver", e))
    }

    /// Every (tenant, timeline) the pageserver holds, archived ones left out.
    async fn timelines(&self) -> Result<Vec<(TenantId, TimelineId)>, SnapshotError> {
        let mut out = Vec::new();
        for tenant in self
            .pageserver
            .list_tenants()
            .await
            .map_err(|e| neon("listing tenants", e))?
        {
            for info in self
                .pageserver
                .list_timelines(tenant)
                .await
                .map_err(|e| neon("listing timelines", e))?
            {
                if info.is_archived != Some(true) {
                    out.push((tenant, info.timeline_id));
                }
            }
        }
        Ok(out)
    }

    /// One timeline's drain state: drained once the pageserver has
    /// uploaded everything the safekeeper holds (or, for a timeline the
    /// safekeeper never served, everything the pageserver ingested).
    async fn timeline_drain(
        &self,
        tenant: TenantId,
        timeline: TimelineId,
    ) -> Result<TimelineDrain, SnapshotError> {
        let info = self
            .pageserver
            .timeline(tenant, timeline)
            .await
            .map_err(|e| neon("reading a timeline", e))?;
        let flush: Lsn = match self.safekeeper.timeline_status(tenant, timeline).await {
            Ok(status) => status.flush_lsn,
            Err(e) if e.status == 404 => info.last_record_lsn,
            Err(e) => return Err(neon("reading the safekeeper", e)),
        };
        Ok(TimelineDrain {
            tenant: tenant.to_string(),
            timeline: timeline.to_string(),
            remote_consistent_lsn: info.remote_consistent_lsn.to_string(),
            flush_lsn: flush.to_string(),
            drained: info.remote_consistent_lsn >= flush,
        })
    }

    /// Waits until every timeline is drained (see the module documentation),
    /// polling every `poll`, for at most `timeout`. The tenants'
    /// `checkpoint_timeout` is lowered for the wait and put back after it,
    /// whatever the outcome.
    pub async fn drain(
        &self,
        timeout: Duration,
        poll: Duration,
    ) -> Result<Vec<TimelineDrain>, SnapshotError> {
        let timelines = self.timelines().await?;
        let mut tenants: Vec<TenantId> = timelines.iter().map(|(t, _)| *t).collect();
        tenants.sort();
        tenants.dedup();
        let mut saved = BTreeMap::new();
        for tenant in &tenants {
            let overrides = self
                .pageserver
                .tenant_overrides(*tenant)
                .await
                .map_err(|e| neon("reading a tenant's config", e))?;
            saved.insert(*tenant, overrides.get(CHECKPOINT_TIMEOUT).cloned());
            self.set_checkpoint_timeout(*tenant, serde_json::json!(DRAIN_CHECKPOINT_TIMEOUT))
                .await?;
        }
        let result = self.wait_drained(&timelines, timeout, poll).await;
        for (tenant, value) in saved {
            let restored = self
                .set_checkpoint_timeout(tenant, value.unwrap_or(serde_json::Value::Null))
                .await;
            if let Err(e) = restored {
                tracing::warn!(%tenant, %e, "putting a tenant's checkpoint_timeout back");
            }
        }
        result
    }

    async fn set_checkpoint_timeout(
        &self,
        tenant: TenantId,
        value: serde_json::Value,
    ) -> Result<(), SnapshotError> {
        let conf = TenantConfig {
            pitr_interval: None,
            extra: BTreeMap::from([(CHECKPOINT_TIMEOUT.to_string(), value)]),
        };
        self.pageserver
            .tenant_config(tenant, &conf)
            .await
            .map_err(|e| neon("setting a tenant's checkpoint_timeout", e))
    }

    async fn wait_drained(
        &self,
        timelines: &[(TenantId, TimelineId)],
        timeout: Duration,
        poll: Duration,
    ) -> Result<Vec<TimelineDrain>, SnapshotError> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut states = Vec::with_capacity(timelines.len());
            for (tenant, timeline) in timelines {
                states.push(self.timeline_drain(*tenant, *timeline).await?);
            }
            if states.iter().all(|s| s.drained) {
                return Ok(states);
            }
            if Instant::now() >= deadline {
                let lagging: Vec<String> = states
                    .iter()
                    .filter(|s| !s.drained)
                    .map(|s| {
                        format!(
                            "{}/{} at {} of {}",
                            s.tenant, s.timeline, s.remote_consistent_lsn, s.flush_lsn
                        )
                    })
                    .collect();
                return Err(SnapshotError::Io(format!(
                    "postgres_not_drained: the pageserver has not uploaded what the safekeeper \
                     holds after {timeout:?}: {}; the safekeeper is not recreated",
                    lagging.join(", ")
                )));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Branches every timeline at its last record LSN; returns the
    /// branches made.
    pub async fn branch_every_timeline(&self) -> Result<Vec<SnapshotBranch>, SnapshotError> {
        let mut branches = Vec::new();
        for (tenant, timeline) in self.timelines().await? {
            let info = self
                .pageserver
                .timeline(tenant, timeline)
                .await
                .map_err(|e| neon("reading a timeline", e))?;
            let lsn = info.last_record_lsn;
            let branch: TimelineId = format!("{:032x}", u128::from(ulid::Ulid::generate()))
                .parse()
                .map_err(|e| neon("a branch id", e))?;
            self.pageserver
                .create_timeline(tenant, &TimelineCreate::branch(branch, timeline, Some(lsn)))
                .await
                .map_err(|e| neon("branching a timeline", e))?;
            branches.push(SnapshotBranch {
                tenant: tenant.to_string(),
                timeline: timeline.to_string(),
                branch: branch.to_string(),
                lsn: lsn.to_string(),
            });
        }
        Ok(branches)
    }
}
