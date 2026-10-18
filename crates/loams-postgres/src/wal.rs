//! The WAL service's timeline API: `loams-wal`'s (`crates/loams-safekeeper/src/http.rs`,
//! PG2 ruling R2.11), and the stock safekeeper's that the release stack still
//! runs (`safekeeper1` of `deploy/loams-postgres-dev`, the fork's
//! `safekeeper/src/http/routes.rs`; 0.1.0 blocker B2).
//!
//! One request shape serves both: the stock safekeeper requires `mconf` and a
//! full `pg_version` (170000), and `loams-wal` ignores the one and accepts the
//! other. The answers differ, and are read into one [`WalTimelineStatus`]:
//! - a create: `loams-wal` answers the head, the stock safekeeper `null`, so
//!   the client then reads the head;
//! - a head: `loams-wal`'s is flat (`term`, `last_log_term`, `pg_version`, …,
//!   `trimmed_lsn`); the stock safekeeper's nests `acceptor_state` (`term`,
//!   `epoch`) and `pg_info`, and has no `trimmed_lsn`: its `local_start_lsn`,
//!   where its own WAL starts, stands in.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::Op;
use crate::http::Http;
use crate::{Component, Lsn, NeonError, Secret, TenantId, TimelineId};

/// A client of one `loams-wal` (an acceptor or a pool's Service).
#[derive(Clone, Debug)]
pub struct WalClient {
    http: Http,
}

/// `POST /v1/tenant/timeline`'s body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalTimelineCreate {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    /// A major (17) or a full version (170005).
    pub pg_version: u32,
    /// Where the timeline's WAL starts.
    pub start_lsn: Lsn,
    pub system_id: Option<u64>,
    pub wal_seg_size: Option<u32>,
    pub commit_lsn: Option<Lsn>,
}

/// A full Postgres version (170000) from a major (17) or a full one.
fn full_pg_version(v: u32) -> u32 {
    if v < 100 { v * 10_000 } else { v }
}

/// The stock safekeeper's membership configuration (`safekeeper_api`
/// `membership::Configuration`): generation 0 with no members is its
/// "pre-generations" configuration, the one a compute without a generation
/// (`neon.safekeepers` without `g#N:`) connects to.
#[derive(Serialize)]
struct Mconf {
    generation: u32,
    members: [(); 0],
    new_members: Option<()>,
}

/// The wire body: `WalTimelineCreate` plus `mconf`, `pg_version` in full.
#[derive(Serialize)]
struct CreateWire<'a> {
    tenant_id: &'a TenantId,
    timeline_id: &'a TimelineId,
    mconf: Mconf,
    pg_version: u32,
    start_lsn: &'a Lsn,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_seg_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit_lsn: Option<Lsn>,
}

impl Serialize for WalTimelineCreate {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        CreateWire {
            tenant_id: &self.tenant_id,
            timeline_id: &self.timeline_id,
            mconf: Mconf {
                generation: 0,
                members: [],
                new_members: None,
            },
            pg_version: full_pg_version(self.pg_version),
            start_lsn: &self.start_lsn,
            system_id: self.system_id,
            wal_seg_size: self.wal_seg_size,
            commit_lsn: self.commit_lsn,
        }
        .serialize(s)
    }
}

/// A timeline's head (`TimelineStatus`), from either WAL service.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "StatusWire")]
pub struct WalTimelineStatus {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub term: u64,
    pub last_log_term: u64,
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub timeline_start_lsn: Lsn,
    pub trimmed_lsn: Lsn,
    pub pg_version: u32,
    pub system_id: u64,
    pub wal_seg_size: u32,
}

/// Either service's head on the wire (see the module's doc).
#[derive(Deserialize)]
struct StatusWire {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    flush_lsn: Lsn,
    commit_lsn: Lsn,
    backup_lsn: Lsn,
    remote_consistent_lsn: Lsn,
    peer_horizon_lsn: Lsn,
    timeline_start_lsn: Lsn,
    // loams-wal
    term: Option<u64>,
    last_log_term: Option<u64>,
    trimmed_lsn: Option<Lsn>,
    pg_version: Option<u32>,
    system_id: Option<u64>,
    wal_seg_size: Option<u32>,
    // the stock safekeeper
    acceptor_state: Option<AcceptorWire>,
    pg_info: Option<PgInfoWire>,
    local_start_lsn: Option<Lsn>,
}

#[derive(Deserialize)]
struct AcceptorWire {
    term: u64,
    /// `last_log_term`, under its old name.
    epoch: u64,
}

#[derive(Deserialize)]
struct PgInfoWire {
    pg_version: u32,
    system_id: u64,
    wal_seg_size: u32,
}

impl TryFrom<StatusWire> for WalTimelineStatus {
    type Error = String;

    fn try_from(w: StatusWire) -> Result<Self, String> {
        let missing = |f: &str| format!("a timeline status without {f}");
        let (term, last_log_term) = match (w.term, w.last_log_term, &w.acceptor_state) {
            (Some(t), Some(l), _) => (t, l),
            (_, _, Some(a)) => (a.term, a.epoch),
            _ => return Err(missing("term and last_log_term (or acceptor_state)")),
        };
        let (pg_version, system_id, wal_seg_size) =
            match (w.pg_version, w.system_id, w.wal_seg_size, &w.pg_info) {
                (Some(v), Some(s), Some(z), _) => (v, s, z),
                (_, _, _, Some(i)) => (i.pg_version, i.system_id, i.wal_seg_size),
                _ => {
                    return Err(missing(
                        "pg_version, system_id and wal_seg_size (or pg_info)",
                    ));
                }
            };
        let trimmed_lsn = w
            .trimmed_lsn
            .or(w.local_start_lsn)
            .ok_or_else(|| missing("trimmed_lsn (or local_start_lsn)"))?;
        Ok(Self {
            tenant_id: w.tenant_id,
            timeline_id: w.timeline_id,
            term,
            last_log_term,
            flush_lsn: w.flush_lsn,
            commit_lsn: w.commit_lsn,
            backup_lsn: w.backup_lsn,
            remote_consistent_lsn: w.remote_consistent_lsn,
            peer_horizon_lsn: w.peer_horizon_lsn,
            timeline_start_lsn: w.timeline_start_lsn,
            trimmed_lsn,
            pg_version,
            system_id,
            wal_seg_size,
        })
    }
}

impl WalClient {
    /// `auth` is `loams-wal`'s `--auth-token`, sent as a bearer token. A URL
    /// with credentials is refused.
    pub fn new(base: Url, auth: Option<Secret<String>>) -> Result<Self, NeonError> {
        Ok(Self {
            http: Http::new(Component::Wal, base, auth)?,
        })
    }

    /// `POST /v1/tenant/timeline`: create the timeline, or answer the
    /// existing one's head unchanged (200), even when the request's
    /// parameters differ (R2.10). A caller that cares compares the answer
    /// with what it asked for. The stock safekeeper answers `null`; the head
    /// is then read with [`Self::timeline_status`].
    pub async fn create_timeline(
        &self,
        create: &WalTimelineCreate,
    ) -> Result<WalTimelineStatus, NeonError> {
        let head: Option<WalTimelineStatus> = self
            .http
            .json(
                Op::WalCreateTimeline,
                Method::POST,
                "/v1/tenant/timeline",
                create,
            )
            .await?;
        match head {
            Some(head) => Ok(head),
            None => {
                self.timeline_status(create.tenant_id, create.timeline_id)
                    .await
            }
        }
    }

    /// `GET /v1/tenant/{t}/timeline/{tl}`.
    pub async fn timeline_status(
        &self,
        t: TenantId,
        tl: TimelineId,
    ) -> Result<WalTimelineStatus, NeonError> {
        self.http
            .get(
                Op::WalTimelineStatus,
                &format!("/v1/tenant/{t}/timeline/{tl}"),
            )
            .await
    }
}
