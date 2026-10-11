//! The spec a bound compute runs, built from the records (§28 §5.2–§5.3;
//! PG2 Task 8). A pure function of the records it reads: the same records
//! give the same spec, byte for byte, so a compute reads the same spec
//! until a record changes.
//!
//! What it reads: the project (tenant, `loams-wal` pool), the endpoint
//! (branch, kind, settings, suspend timeout, current compute), the branch
//! (timeline, the pageserver of `notify-attach`), and the branch's roles
//! and databases in key order. Roles carry no password verifier yet: wiring
//! the role secrets into the spec as SCRAM verifiers is Task 24's.

use std::collections::BTreeMap;

use loams_postgres::spec::{
    ComputeMode, ComputeSpec, ComputeSpecBuilder, DatabaseRec as SpecDatabase, RoleRec as SpecRole,
    Setting,
};
use loams_postgres::{TenantId, TimelineId};

use crate::model::{
    BranchKey, BranchRec, BranchScope, BranchState, ComputeBinding, ComputeRec, DatabaseRec,
    EndpointKey, EndpointRec, EndpointType, ProjectKey, ProjectRec, ProjectState, Record, RoleRec,
    WalService,
};
use crate::store::{MAX_PAGE_SIZE, Page, PgControlStore, StoreError, Versioned};

/// Where computes find storage: the pageservers by node id, and the
/// `loams-wal` pools by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Sources {
    /// Node id → libpq URL (`postgresql://no_user@host:6400`).
    pub(crate) pageservers: BTreeMap<u64, String>,
    /// The node a branch with no shard map uses: single-node mode, where no
    /// storage controller sends `notify-attach`.
    pub(crate) default_pageserver: Option<u64>,
    /// Pool name → the connection strings of its acceptors or Service.
    pub(crate) wal_pools: BTreeMap<String, Vec<String>>,
}

/// Why no spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpecError {
    /// The compute is not its endpoint's current compute, or the endpoint
    /// is gone: it must not run (409).
    NotCurrent(&'static str),
    /// Something it needs is not there yet (503: `compute_ctl` retries).
    NotReady(String),
    Store(StoreError),
}

impl From<StoreError> for SpecError {
    fn from(e: StoreError) -> Self {
        SpecError::Store(e)
    }
}

/// Every record under `prefix`, page by page, in key order.
pub(crate) async fn list_all<S: PgControlStore, R: Record>(
    store: &S,
    prefix: &R::Prefix,
) -> Result<Vec<Versioned<R>>, StoreError> {
    let mut out = Vec::new();
    let mut page = Page::first(MAX_PAGE_SIZE);
    loop {
        let (records, next) = store.list::<R>(prefix, page).await?;
        out.extend(records);
        match next {
            Some(token) => page = Page::after(MAX_PAGE_SIZE, token),
            None => return Ok(out),
        }
    }
}

/// The spec of `compute`, bound by `binding`.
pub(crate) async fn attached<S: PgControlStore>(
    store: &S,
    sources: &Sources,
    compute: &ComputeRec,
    binding: &ComputeBinding,
) -> Result<ComputeSpec, SpecError> {
    let project = store
        .get::<ProjectRec>(&ProjectKey {
            namespace: binding.namespace.clone(),
            id: binding.project_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the project is gone"))?
        .record;
    let endpoint = store
        .get::<EndpointRec>(&EndpointKey {
            project_id: binding.project_id.clone(),
            id: binding.endpoint_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the endpoint is gone"))?
        .record;
    if endpoint.compute_id.as_deref() != Some(compute.id.as_str()) {
        return Err(SpecError::NotCurrent(
            "the compute is not its endpoint's current compute",
        ));
    }
    if project.state != ProjectState::Ready {
        return Err(SpecError::NotReady("the project is not ready".into()));
    }
    let branch = store
        .get::<BranchRec>(&BranchKey {
            project_id: binding.project_id.clone(),
            id: endpoint.branch_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the branch is gone"))?
        .record;
    if branch.state != BranchState::Ready {
        return Err(SpecError::NotReady("the branch is not ready".into()));
    }
    let scope = BranchScope {
        branch_id: branch.id.clone(),
    };
    let roles = list_all::<S, RoleRec>(store, &scope).await?;
    let databases = list_all::<S, DatabaseRec>(store, &scope).await?;
    build(
        sources, compute, &project, &endpoint, &branch, &roles, &databases,
    )
}

/// The pageserver of an unsharded branch: its `notify-attach` shard 0, or
/// the default node when it has none.
fn pageserver(sources: &Sources, branch: &BranchRec) -> Result<(u64, String), SpecError> {
    let node = match branch.shards.as_slice() {
        [] => sources
            .default_pageserver
            .ok_or_else(|| SpecError::NotReady("no pageserver is attached yet".into()))?,
        [one] if one.shard_number == 0 && branch.stripe_size.is_none() => one.node_id,
        _ => {
            // Sharded tenants come with storage HA (Task 49, R2.14).
            return Err(SpecError::NotReady(
                "sharded tenants are not supported".into(),
            ));
        }
    };
    let url = sources
        .pageservers
        .get(&node)
        .ok_or_else(|| SpecError::NotReady(format!("no address for pageserver node {node}")))?;
    Ok((node, url.clone()))
}

fn build(
    sources: &Sources,
    compute: &ComputeRec,
    project: &ProjectRec,
    endpoint: &EndpointRec,
    branch: &BranchRec,
    roles: &[Versioned<RoleRec>],
    databases: &[Versioned<DatabaseRec>],
) -> Result<ComputeSpec, SpecError> {
    let (node, url) = pageserver(sources, branch)?;
    let WalService::LoamsWal { pool } = &project.wal;
    let safekeepers = sources
        .wal_pools
        .get(pool)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SpecError::NotReady(format!("no address for loams-wal pool {pool}")))?;
    let mode = match endpoint.kind {
        EndpointType::ReadWrite => ComputeMode::Primary,
        EndpointType::ReadOnly => ComputeMode::Replica,
    };
    let suspend = match endpoint.suspend_timeout_s {
        0 => -1,
        s => i64::from(s),
    };
    let mut builder =
        ComputeSpecBuilder::new(TenantId(project.tenant_id), TimelineId(branch.timeline_id))
            .ids(&project.id, &branch.id, &endpoint.id)
            .compute_id(&compute.id)
            .safekeepers(safekeepers.iter().cloned())
            .pageserver(node, &url)
            .suspend_timeout_seconds(suspend)
            .mode(mode);
    for (name, value) in &endpoint.pg_settings {
        builder = builder.setting(Setting::new(name, value, "string"));
    }
    // Where Postgres listens, after the endpoint's settings so that they
    // win (Task 10): every address of the compute, so the runtime's address
    // (a published port, a Service) reaches it, on the port `compute_ctl`'s
    // connection string names.
    builder = builder
        .setting(Setting::new("listen_addresses", "*", "string"))
        .setting(Setting::new(
            "port",
            &crate::runtime::COMPUTE_PG_PORT.to_string(),
            "integer",
        ));
    for r in roles {
        builder = builder.role(SpecRole {
            name: r.record.name.clone(),
            encrypted_password: None,
        });
    }
    for d in databases {
        builder = builder.database(SpecDatabase {
            name: d.record.name.clone(),
            owner: d.record.owner.clone(),
        });
    }
    Ok(builder.build())
}
