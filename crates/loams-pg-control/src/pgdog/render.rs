//! PgDog's two config files for one Loams namespace (design §46 §8.1–§8.2;
//! PG2 Task 20): `pgdog.toml` (`[general]`, `[admin]`, one `[[databases]]`
//! entry per routed name and endpoint) and `users.toml` (one `[[users]]`
//! entry per login role, naming the routed names its grants allow).
//!
//! - **Pure and deterministic.** [`render_namespace`] reads only its input,
//!   in a fixed order, so the same records give byte-identical files
//!   (golden tests), and [`RenderedConfig::generation`] is a digest of both.
//! - **Routed names** come from [`format_routed`] alone (§46 §8.1):
//!   `<project>` (the default branch), `<project>__<branch>`,
//!   `<project>__<branch>.<database>` and `<project>__<branch>__ro`. The
//!   first two reach the branch's `postgres` database (R20.2).
//! - **Hosts** are the endpoints' recorded backend addresses. A read-write
//!   endpoint is `role = "primary"`, each read-only endpoint a `replica`
//!   (`read_only`) of its branch's `__ro` name, so PgDog balances them. An
//!   endpoint without an address yet is left out.
//! - **Pool sizes** follow the endpoint's largest size: 80 % of its
//!   `max_connections` ([`max_connections`], which the spec sets).
//! - **Users.** A login role, not a system one, is granted every routed name
//!   of its own branch (R20.4; Task 24 brings explicit grants), and only
//!   those: PgDog refuses any other database before it opens a backend
//!   connection. Its client password is checked against its SCRAM verifier
//!   (`password_hash`); PgDog logs into the compute as the same role with a
//!   backend-only password (R18.3). A role without credentials is left out.
//! - **`[general]`**: the listener, OpenMetrics, SCRAM, transaction pooling,
//!   TLS, and Task 18's timeouts. Health checks never keep a compute awake:
//!   no idle health checks, no connection kept open, idle connections
//!   closed after 30 s (R18.9).
//! - **Sharded projects** (a shard map) are left out until storage HA
//!   (Task 49, R2.14), and listed in [`RenderedConfig::left_out`].
//!
//! Both files are secrets: `users.toml` holds the backend passwords and
//! `pgdog.toml` the admin database's password.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use crate::autoscale::max_connections;
use crate::model::{
    BranchPrefix, BranchRec, BranchScope, BranchState, DatabaseRec, EndpointPrefix, EndpointRec,
    EndpointType, PoolMode, ProjectPrefix, ProjectRec, ProjectState, RoleRec,
};
use crate::names::{Routed, format_routed};
use crate::reconcile::all;
use crate::secrets::Secret;
use crate::store::{PgControlStore, StoreError};

/// Where `<project>` and `<project>__<branch>` connect: the database every
/// compute has (R20.2).
pub const DEFAULT_DATABASE: &str = "postgres";

/// Task 18's settings (R18.9), in ms.
pub const CHECKOUT_TIMEOUT_MS: u64 = 20_000;
pub const CONNECT_TIMEOUT_MS: u64 = 1_000;
pub const CONNECT_ATTEMPT_DELAY_MS: u64 = 500;
pub const BAN_TIMEOUT_MS: u64 = 1_000;
pub const HEALTHCHECK_INTERVAL_MS: u64 = 30_000;
pub const IDLE_TIMEOUT_MS: u64 = 30_000;

/// A role's credentials as PgDog uses them (Task 24 derives them).
#[derive(Clone)]
pub struct RoleCredential {
    /// The SCRAM-SHA-256 verifier of the role's password, as Postgres
    /// stores it (`SCRAM-SHA-256$<iterations>:<salt>$<stored>:<server>`).
    pub verifier: String,
    /// The password PgDog logs into the compute with, as the same role;
    /// the role's user never sees it.
    pub backend_password: Secret<String>,
}

impl std::fmt::Debug for RoleCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoleCredential").finish_non_exhaustive()
    }
}

/// A role: its branch and name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoleKey {
    pub branch_id: String,
    pub name: String,
}

/// One namespace's records, as the renderer reads them.
#[derive(Debug, Clone, Default)]
pub struct NamespaceRecords {
    pub namespace: String,
    pub projects: Vec<ProjectRec>,
    pub branches: Vec<BranchRec>,
    pub endpoints: Vec<EndpointRec>,
    pub roles: Vec<RoleRec>,
    pub databases: Vec<DatabaseRec>,
    pub credentials: BTreeMap<RoleKey, RoleCredential>,
}

impl NamespaceRecords {
    /// The namespace's records from the store: its projects, their
    /// branches and endpoints, and the branches' roles and databases. No
    /// credentials: Task 24 adds them.
    ///
    /// # Errors
    ///
    /// The store's.
    pub async fn read<S: PgControlStore>(store: &S, namespace: &str) -> Result<Self, StoreError> {
        let mut out = NamespaceRecords {
            namespace: namespace.to_owned(),
            ..NamespaceRecords::default()
        };
        let projects = all::<S, ProjectRec>(
            store,
            &ProjectPrefix {
                namespace: namespace.to_owned(),
            },
        )
        .await?;
        for p in projects {
            let p = p.record;
            let branches = all::<S, BranchRec>(
                store,
                &BranchPrefix {
                    project_id: p.id.clone(),
                },
            )
            .await?;
            for b in &branches {
                let scope = BranchScope {
                    branch_id: b.record.id.clone(),
                };
                out.roles.extend(
                    all::<S, RoleRec>(store, &scope)
                        .await?
                        .into_iter()
                        .map(|v| v.record),
                );
                out.databases.extend(
                    all::<S, DatabaseRec>(store, &scope)
                        .await?
                        .into_iter()
                        .map(|v| v.record),
                );
            }
            out.branches.extend(branches.into_iter().map(|v| v.record));
            out.endpoints.extend(
                all::<S, EndpointRec>(
                    store,
                    &EndpointPrefix {
                        project_id: p.id.clone(),
                    },
                )
                .await?
                .into_iter()
                .map(|v| v.record),
            );
            out.projects.push(p);
        }
        Ok(out)
    }
}

/// TLS toward clients (Task 23).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTls {
    pub certificate: String,
    pub private_key: String,
}

/// The deployment's side of the files.
#[derive(Debug, Clone)]
pub struct RenderSettings {
    /// PgDog's listener port.
    pub port: u16,
    pub openmetrics_port: u16,
    /// With TLS, plaintext clients are refused (Task 23).
    pub client_tls: Option<ClientTls>,
    /// A CA that signs the computes' certificates: backends are verified
    /// against it (`verify_ca`); without one, backend TLS is off (Task 23).
    pub backend_ca: Option<String>,
    pub admin_user: String,
    pub admin_password: Secret<String>,
}

impl RenderSettings {
    /// Port 6432, metrics on 9090, no TLS, the admin user `admin`.
    pub fn new(admin_password: Secret<String>) -> Self {
        RenderSettings {
            port: 6432,
            openmetrics_port: 9090,
            client_tls: None,
            backend_ca: None,
            admin_user: "admin".into(),
            admin_password,
        }
    }
}

/// The two files, and what was left out.
#[derive(Debug, Clone)]
pub struct RenderedConfig {
    pub pgdog_toml: Secret<String>,
    pub users_toml: Secret<String>,
    /// A digest of both files (never 0): the same records give the same.
    pub generation: u64,
    /// Projects left out, with why (sharded).
    pub left_out: Vec<(String, String)>,
}

/// Why the namespace could not be rendered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    /// A record names something the routed-name grammar refuses.
    #[error("{0}")]
    Name(String),
}

/// A TOML basic string.
fn q(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn pool_mode(m: PoolMode) -> &'static str {
    match m {
        PoolMode::Transaction => "transaction",
        PoolMode::Session => "session",
        PoolMode::Statement => "statement",
    }
}

/// `host:port` as recorded (an IPv6 host in brackets).
fn host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    Some((host.to_owned(), port.parse().ok()?))
}

/// One `[[databases]]` entry.
struct Entry {
    name: String,
    host: String,
    port: u16,
    database: String,
    replica: bool,
    pool_size: u32,
    pool_mode: PoolMode,
}

/// The namespace's files (see the module docs).
///
/// # Errors
///
/// [`RenderError::Name`] when a project, branch or database name is not
/// one the routed-name grammar accepts.
pub fn render_namespace(
    records: &NamespaceRecords,
    settings: &RenderSettings,
) -> Result<RenderedConfig, RenderError> {
    let name_err = |e: crate::names::NameError| RenderError::Name(e.to_string());
    let mut projects: Vec<&ProjectRec> = records
        .projects
        .iter()
        .filter(|p| p.namespace == records.namespace && p.state == ProjectState::Ready)
        .collect();
    projects.sort_by(|a, b| a.name.cmp(&b.name));

    let mut entries: Vec<Entry> = Vec::new();
    // Routed names per branch, for the users.
    let mut granted: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut left_out = Vec::new();
    for p in projects {
        let mut branches: Vec<&BranchRec> = records
            .branches
            .iter()
            .filter(|b| b.project_id == p.id && b.state == BranchState::Ready)
            .collect();
        branches.sort_by(|a, b| a.name.cmp(&b.name));
        if branches
            .iter()
            .any(|b| b.stripe_size.is_some() || b.shards.len() > 1)
        {
            left_out.push((
                p.id.clone(),
                "sharded projects wait for storage HA (Task 49)".to_owned(),
            ));
            continue;
        }
        for b in branches {
            let mut endpoints: Vec<&EndpointRec> = records
                .endpoints
                .iter()
                .filter(|e| e.branch_id == b.id && e.project_id == p.id)
                .collect();
            endpoints.sort_by(|x, y| x.id.cmp(&y.id));
            let mut databases: Vec<&DatabaseRec> = records
                .databases
                .iter()
                .filter(|d| d.branch_id == b.id)
                .collect();
            databases.sort_by(|x, y| x.name.cmp(&y.name));
            let names = granted.entry(b.id.clone()).or_default();
            for e in endpoints {
                let Some((host, port)) = e.backend_addr.as_deref().and_then(host_port) else {
                    continue;
                };
                // 80 % of the compute's connections at its largest size.
                let pool_size = max_connections(e.max_cu.max(e.min_cu)) * 4 / 5;
                let mut add = |routed: Routed, database: &str, replica: bool| {
                    let name = format_routed(&routed).map_err(name_err)?;
                    if !names.contains(&name) {
                        names.push(name.clone());
                    }
                    entries.push(Entry {
                        name,
                        host: host.clone(),
                        port,
                        database: database.to_owned(),
                        replica,
                        pool_size,
                        pool_mode: e.pool_mode,
                    });
                    Ok::<(), RenderError>(())
                };
                let (project, branch) = (p.name.clone(), b.name.clone());
                match e.kind {
                    EndpointType::ReadWrite => {
                        if p.default_branch_id.as_deref() == Some(b.id.as_str()) {
                            add(
                                Routed::Project {
                                    project: project.clone(),
                                },
                                DEFAULT_DATABASE,
                                false,
                            )?;
                        }
                        add(
                            Routed::Branch {
                                project: project.clone(),
                                branch: branch.clone(),
                            },
                            DEFAULT_DATABASE,
                            false,
                        )?;
                        for d in &databases {
                            add(
                                Routed::Database {
                                    project: project.clone(),
                                    branch: branch.clone(),
                                    database: d.name.clone(),
                                },
                                &d.name,
                                false,
                            )?;
                        }
                    }
                    EndpointType::ReadOnly => {
                        add(Routed::ReadOnly { project, branch }, DEFAULT_DATABASE, true)?;
                    }
                }
            }
        }
    }
    // Entries of one name together (a cluster), primaries first, in a fixed
    // order.
    entries.sort_by(|a, b| {
        (&a.name, a.replica, &a.host, a.port).cmp(&(&b.name, b.replica, &b.host, b.port))
    });

    let pgdog = render_pgdog(settings, &entries);
    let users = render_users(records, &granted);
    let mut h = Sha256::new();
    h.update(pgdog.as_bytes());
    h.update([0]);
    h.update(users.as_bytes());
    let digest = h.finalize();
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    Ok(RenderedConfig {
        pgdog_toml: Secret::new(pgdog),
        users_toml: Secret::new(users),
        generation: u64::from_be_bytes(first).max(1),
        left_out,
    })
}

fn render_pgdog(s: &RenderSettings, entries: &[Entry]) -> String {
    let mut o = String::new();
    let _ = writeln!(o, "# Rendered by pg-control (PG2 Task 20); do not edit.");
    let _ = writeln!(o, "[general]");
    let _ = writeln!(o, "host = \"0.0.0.0\"");
    let _ = writeln!(o, "port = {}", s.port);
    let _ = writeln!(o, "openmetrics_port = {}", s.openmetrics_port);
    let _ = writeln!(o, "auth_type = \"scram\"");
    let _ = writeln!(o, "pooler_mode = \"transaction\"");
    let _ = writeln!(o, "checkout_timeout = {CHECKOUT_TIMEOUT_MS}");
    let _ = writeln!(o, "connect_timeout = {CONNECT_TIMEOUT_MS}");
    let _ = writeln!(o, "connect_attempts = 1");
    let _ = writeln!(o, "connect_attempt_delay = {CONNECT_ATTEMPT_DELAY_MS}");
    let _ = writeln!(o, "ban_timeout = {BAN_TIMEOUT_MS}");
    let _ = writeln!(o, "healthcheck_interval = {HEALTHCHECK_INTERVAL_MS}");
    let _ = writeln!(o, "idle_healthcheck_interval = 0");
    let _ = writeln!(o, "min_pool_size = 0");
    let _ = writeln!(o, "idle_timeout = {IDLE_TIMEOUT_MS}");
    match &s.client_tls {
        Some(tls) => {
            let _ = writeln!(o, "tls_certificate = {}", q(&tls.certificate));
            let _ = writeln!(o, "tls_private_key = {}", q(&tls.private_key));
            let _ = writeln!(o, "tls_client_required = true");
        }
        None => {
            let _ = writeln!(o, "tls_client_required = false");
        }
    }
    match &s.backend_ca {
        Some(ca) => {
            let _ = writeln!(o, "tls_verify = \"verify_ca\"");
            let _ = writeln!(o, "tls_server_ca_certificate = {}", q(ca));
        }
        None => {
            let _ = writeln!(o, "tls_verify = \"disabled\"");
        }
    }
    let _ = writeln!(o);
    let _ = writeln!(o, "[admin]");
    let _ = writeln!(o, "name = \"admin\"");
    let _ = writeln!(o, "user = {}", q(&s.admin_user));
    let _ = writeln!(o, "password = {}", q(s.admin_password.expose()));
    for e in entries {
        let _ = writeln!(o);
        let _ = writeln!(o, "[[databases]]");
        let _ = writeln!(o, "name = {}", q(&e.name));
        let _ = writeln!(o, "host = {}", q(&e.host));
        let _ = writeln!(o, "port = {}", e.port);
        let _ = writeln!(o, "database_name = {}", q(&e.database));
        let _ = writeln!(
            o,
            "role = {}",
            q(if e.replica { "replica" } else { "primary" })
        );
        if e.replica {
            let _ = writeln!(o, "read_only = true");
        }
        let _ = writeln!(o, "pool_size = {}", e.pool_size);
        let _ = writeln!(o, "pooler_mode = {}", q(pool_mode(e.pool_mode)));
    }
    o
}

fn render_users(records: &NamespaceRecords, granted: &BTreeMap<String, Vec<String>>) -> String {
    let mut roles: Vec<&RoleRec> = records
        .roles
        .iter()
        .filter(|r| r.login && !r.system)
        .collect();
    roles.sort_by(|a, b| (&a.branch_id, &a.name).cmp(&(&b.branch_id, &b.name)));
    let mut o = String::new();
    let _ = writeln!(o, "# Rendered by pg-control (PG2 Task 20); do not edit.");
    for r in roles {
        let Some(names) = granted.get(&r.branch_id).filter(|n| !n.is_empty()) else {
            continue;
        };
        let key = RoleKey {
            branch_id: r.branch_id.clone(),
            name: r.name.clone(),
        };
        let Some(cred) = records.credentials.get(&key) else {
            continue;
        };
        let mut names = names.clone();
        names.sort();
        let list = names.iter().map(|n| q(n)).collect::<Vec<_>>().join(", ");
        let _ = writeln!(o);
        let _ = writeln!(o, "[[users]]");
        let _ = writeln!(o, "name = {}", q(&r.name));
        let _ = writeln!(o, "databases = [{list}]");
        let _ = writeln!(o, "password_hash = {}", q(&cred.verifier));
        let _ = writeln!(o, "server_user = {}", q(&r.name));
        let _ = writeln!(o, "server_password = {}", q(cred.backend_password.expose()));
        if let Some(mode) = r.pool_mode {
            let _ = writeln!(o, "pooler_mode = {}", q(pool_mode(mode)));
        }
    }
    o
}
