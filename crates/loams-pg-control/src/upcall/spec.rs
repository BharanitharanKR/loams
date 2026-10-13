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
//!
//! Task 11: endpoint settings outside the allow-list
//! ([`ALLOWED_PG_SETTINGS`](crate::service::endpoints::ALLOWED_PG_SETTINGS))
//! never reach the spec, and [`build`] also answers the version of the
//! spec's inputs, which the endpoint reconciler keeps in the compute's
//! record to push a spec whose inputs changed.

use std::collections::BTreeMap;

use loams_postgres::spec::{
    ComputeMode, ComputeSpec, ComputeSpecBuilder, DatabaseRec as SpecDatabase, RoleRec as SpecRole,
    Setting,
};
use loams_postgres::{TenantId, TimelineId};
use sha2::{Digest, Sha256};

use crate::model::{
    BranchKey, BranchRec, BranchScope, BranchState, ComputeBinding, ComputeRec, DatabaseRec,
    EndpointKey, EndpointRec, EndpointType, ProjectKey, ProjectRec, ProjectState, Record, RoleRec,
    WalService,
};
use crate::service::endpoints::setting_allowed;
use crate::store::{MAX_PAGE_SIZE, Page, PgControlStore, StoreError, Versioned};

/// Where computes find storage: the pageservers by node id, and the
/// `loams-wal` pools by name. The upcall listener and the endpoint
/// reconciler (Task 11) build specs from the same.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sources {
    /// Node id → libpq URL (`postgresql://no_user@host:6400`).
    pub pageservers: BTreeMap<u64, String>,
    /// The node a branch with no shard map uses: single-node mode, where no
    /// storage controller sends `notify-attach`.
    pub default_pageserver: Option<u64>,
    /// Pool name → the connection strings of its acceptors or Service.
    pub wal_pools: BTreeMap<String, Vec<String>>,
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

/// What a bound compute's spec is built from: the records of its project,
/// endpoint, branch, and the branch's roles and databases (in key order).
#[derive(Debug, Clone)]
pub(crate) struct Inputs {
    pub(crate) project: Versioned<ProjectRec>,
    pub(crate) endpoint: EndpointRec,
    pub(crate) branch: Versioned<BranchRec>,
    pub(crate) roles: Vec<Versioned<RoleRec>>,
    pub(crate) databases: Vec<Versioned<DatabaseRec>>,
}

/// The records a compute bound by `binding` is built from. With
/// `current`, the endpoint must name that compute as its current one
/// (`NotCurrent` otherwise): a replaced compute must not run.
pub(crate) async fn read_inputs<S: PgControlStore>(
    store: &S,
    binding: &ComputeBinding,
    current: Option<&str>,
) -> Result<Inputs, SpecError> {
    let project = store
        .get::<ProjectRec>(&ProjectKey {
            namespace: binding.namespace.clone(),
            id: binding.project_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the project is gone"))?;
    let endpoint = store
        .get::<EndpointRec>(&EndpointKey {
            project_id: binding.project_id.clone(),
            id: binding.endpoint_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the endpoint is gone"))?
        .record;
    if let Some(id) = current
        && endpoint.compute_id.as_deref() != Some(id)
    {
        return Err(SpecError::NotCurrent(
            "the compute is not its endpoint's current compute",
        ));
    }
    if project.record.state != ProjectState::Ready {
        return Err(SpecError::NotReady("the project is not ready".into()));
    }
    let branch = store
        .get::<BranchRec>(&BranchKey {
            project_id: binding.project_id.clone(),
            id: endpoint.branch_id.clone(),
        })
        .await?
        .ok_or(SpecError::NotCurrent("the branch is gone"))?;
    if branch.record.state != BranchState::Ready {
        return Err(SpecError::NotReady("the branch is not ready".into()));
    }
    let scope = BranchScope {
        branch_id: branch.record.id.clone(),
    };
    let roles = list_all::<S, RoleRec>(store, &scope).await?;
    let databases = list_all::<S, DatabaseRec>(store, &scope).await?;
    Ok(Inputs {
        project,
        endpoint,
        branch,
        roles,
        databases,
    })
}

/// The spec of `compute`, bound by `binding`.
pub(crate) async fn attached<S: PgControlStore>(
    store: &S,
    sources: &Sources,
    compute: &ComputeRec,
    binding: &ComputeBinding,
) -> Result<ComputeSpec, SpecError> {
    let inputs = read_inputs(store, binding, Some(&compute.id)).await?;
    Ok(build(sources, &compute.id, &inputs)?.0)
}

/// What a spec's inputs come to, as a number: a SHA-256 of the spec's
/// bytes and of the versions of the records it was built from (never 0,
/// which means "unknown"). Versions are never reused (R3.5), so inputs that
/// changed and changed back still differ, and the endpoint reconciler
/// pushes the spec again (Task 11).
fn inputs_version(spec: &ComputeSpec, inputs: &Inputs) -> u64 {
    let mut h = Sha256::new();
    h.update(serde_json::to_vec(spec).unwrap_or_default());
    h.update(inputs.project.version.to_be_bytes());
    h.update(inputs.branch.version.to_be_bytes());
    h.update(inputs.endpoint.settings_version.to_be_bytes());
    for r in &inputs.roles {
        h.update(r.record.name.as_bytes());
        h.update([0]);
        h.update(r.version.to_be_bytes());
    }
    h.update([0xff]);
    for d in &inputs.databases {
        h.update(d.record.name.as_bytes());
        h.update([0]);
        h.update(d.version.to_be_bytes());
    }
    let digest = h.finalize();
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(first).max(1)
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

/// The spec of compute `compute_id` from `inputs`, and its inputs' version.
pub(crate) fn build(
    sources: &Sources,
    compute_id: &str,
    inputs: &Inputs,
) -> Result<(ComputeSpec, u64), SpecError> {
    let project = &inputs.project.record;
    let endpoint = &inputs.endpoint;
    let branch = &inputs.branch.record;
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
            .compute_id(compute_id)
            .safekeepers(safekeepers.iter().cloned())
            .pageserver(node, &url)
            .suspend_timeout_seconds(suspend)
            .mode(mode);
    for (name, value) in &endpoint.pg_settings {
        // The API refuses these (Task 11); a record written before the
        // allow-list still never overrides what the platform owns.
        if !setting_allowed(name, value) {
            tracing::warn!(endpoint = %endpoint.id, setting = %name, "an endpoint setting outside the allow-list is left out of the spec");
            continue;
        }
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
    for r in &inputs.roles {
        builder = builder.role(SpecRole {
            name: r.record.name.clone(),
            encrypted_password: None,
        });
    }
    for d in &inputs.databases {
        builder = builder.database(SpecDatabase {
            name: d.record.name.clone(),
            owner: d.record.owner.clone(),
        });
    }
    let spec = builder.build();
    let version = inputs_version(&spec, inputs);
    Ok((spec, version))
}
