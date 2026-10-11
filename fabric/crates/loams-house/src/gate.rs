//! The checks a statement passes on every House surface (HS1 Tasks 4, 5 and 8):
//! what the front answers itself, which worker runs it, and whether a statement a
//! worker analysed may run. The ClickHouse HTTP interface ([`crate::http`]) and
//! `loams.house.v1` ([`crate::api`]) both call these, so a statement refused on
//! one surface is refused on the other with the same code.

use std::collections::HashSet;

use loams_house_ipc::QueryClass;

use crate::admission::{WorkerLease, WorkerPool};
use crate::classify::{Expect, Stmt, decide};
use crate::deny;
use crate::errors::{ChError, HouseError};
use crate::session::SessionGuard;

/// The text chDB runs for a statement: the host functions answer the House's
/// values (HS1 Task 5), rewritten in the text and refused by the deny list in any
/// form the rewrite did not take; and what runs is what the worker explains,
/// which it ends with its own `FORMAT`, so no trailing `;` or comment (Task 5
/// fix round 1).
pub fn statement_text(sql: &str, user: &str) -> String {
    let sql = deny::rewrite_host_functions(sql, &host_values(user));
    crate::classify::without_trailer(&sql).to_string()
}

/// What the host functions answer for `user`.
pub fn host_values(user: &str) -> deny::HostValues<'_> {
    deny::HostValues {
        display_name: crate::config::DISPLAY_NAME,
        timezone: "UTC",
        user,
    }
}

/// Whether a statement a worker analysed may run: the deny list over its trees
/// (HS1 Task 5: `344`, or `164` for a denied setting), then ClickHouse's class
/// ([`decide`]). A statement refused on its class alone (more than one, or one
/// ClickHouse cannot parse) has no trees, and `decide` refuses it.
pub fn admit(
    analysis: &loams_house_ipc::Analysis,
    sql: &str,
    expect: Expect,
    readonly: bool,
    unparsed: Option<&str>,
) -> Result<(), HouseError> {
    let classification = analysis.classification;
    if classification.statements == 1 && classification.class != QueryClass::Unknown {
        // A `CREATE` of a lake table or a view never runs in chDB (the front owns
        // it, or `decide` refuses the form): its engine is held to the denied
        // engines, not to a temporary table's Memory or Null.
        let tree = deny::QueryTree::from_explain(&analysis.ast, analysis.query_tree.as_deref())
            .shared_create(crate::classify::creates_shared_object(sql))
            .statement(sql);
        deny::check(&tree).map_err(HouseError::from)?;
    }
    decide(classification, expect, readonly, unparsed)
}

/// The task that will serve an owned statement this front does not yet (`48`).
fn later(stmt: &Stmt) -> Option<String> {
    let (what, task) = match stmt {
        Stmt::CreateDatabase { .. } => ("CREATE DATABASE", "HS1 Task 10"),
        Stmt::CreateTable { .. } => ("CREATE TABLE (a lake table)", "HS1 Task 10"),
        Stmt::CreateTableAs { .. } => ("CREATE TABLE … AS SELECT", "HS1 Task 12"),
        Stmt::CreatePipe { .. } => (
            "A pipe (LoamsStream, IggyTopic, S3Queue)",
            "HS1 Tasks 16 and 18",
        ),
        Stmt::CreateMaterializedView { .. } => ("CREATE MATERIALIZED VIEW", "HS1 Task 17"),
        Stmt::Drop(drop) if !drop.temporary => ("DROP (a lake object)", "HS1 Task 10"),
        Stmt::Truncate { .. } => ("TRUNCATE", "HS1 Task 12"),
        Stmt::AlterAddColumns { .. } => ("ALTER TABLE … ADD COLUMN", "HS1 Task 10"),
        Stmt::Rename { .. } => ("RENAME TABLE", "HS1 Task 10"),
        Stmt::Undrop { .. } => ("UNDROP TABLE", "HS1 Task 15"),
        Stmt::Optimize { .. } => ("OPTIMIZE TABLE", "HS1 Task 13"),
        Stmt::KillQuery(_) => ("KILL QUERY", "HS1 Task 21"),
        Stmt::Unsupported { kind } => {
            return Some(format!("{kind} is not on the House's ClickHouse surface"));
        }
        _ => return None,
    };
    Some(format!(
        "{what} is served from {task}; this House does not serve it yet"
    ))
}

/// What the front answers itself: `Some(Ok)` for an empty success (`SET`, `USE`),
/// `Some(Err)` for an error, `None` for a statement a worker runs.
pub fn front_answer(
    stmt: &Stmt,
    session: Option<&SessionGuard>,
    limits: &crate::settings::SessionLimits,
    known: &HashSet<String>,
) -> Option<Result<(), HouseError>> {
    if let Some(message) = later(stmt) {
        return Some(Err(HouseError::from(ChError::not_implemented(message))));
    }
    match stmt {
        Stmt::Set(pairs) => Some((|| {
            for (name, value) in pairs {
                crate::settings::check(name, value, limits, known)?;
            }
            // Without a session a SET has nothing to keep it, as in ClickHouse.
            if let Some(guard) = session {
                let mut state = guard.state();
                for (name, value) in pairs {
                    state.settings.apply(name, value, limits, known)?;
                }
            }
            Ok(())
        })()),
        Stmt::Use(database) => Some(if database != "default" {
            Err(HouseError::from(ChError::unknown_database(format!(
                "Database {database} does not exist"
            ))))
        } else {
            if let Some(guard) = session {
                guard.state().database = database.clone();
            }
            Ok(())
        }),
        _ => None,
    }
}

/// The worker for a statement: the session's pinned worker (a lost pin falls back
/// to any worker: its temporary tables are gone), a newly pinned one for the
/// session's first temporary table, or any worker of the namespace.
pub async fn acquire_for(
    pool: &WorkerPool,
    namespace: &str,
    session: Option<&mut SessionGuard>,
    pins: bool,
) -> Result<WorkerLease, HouseError> {
    let Some(guard) = session else {
        return pool.acquire(namespace).await;
    };
    let pinned = guard.state().pinned.clone();
    if let Some(worker) = pinned {
        if let Some(lease) = pool.acquire_pinned(namespace, &worker).await? {
            return Ok(lease);
        }
        guard.state().pinned = None;
    }
    if pins {
        let lease = pool.acquire_and_pin(namespace).await?;
        // Kept only if the statement succeeds (fix round 1, M1).
        guard.pin_provisionally(lease.worker_id().to_string());
        return Ok(lease);
    }
    pool.acquire(namespace).await
}
