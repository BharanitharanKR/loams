//! The checks a statement passes on every House surface (HS1 Tasks 4, 5 and 8):
//! what the front answers itself, which worker runs it, and whether a statement a
//! worker analysed may run. The ClickHouse HTTP interface ([`crate::http`]) and
//! `loams.house.v1` ([`crate::api`]) both call these, so a statement refused on
//! one surface is refused on the other with the same code.

use std::collections::HashSet;

use loams_house_ipc::QueryClass;

use async_trait::async_trait;
use loams_house_ipc::{Execute, Limits, Output};

use crate::admission::{Outcome, WorkerLease, WorkerPool};
use crate::catalog::Lake;
use crate::classify::{Expect, Stmt, decide};
use crate::ddl::{self, ChColumn, Described, Kind, Oracle};
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
/// The catalog's statements reach here only when the House has no catalog
/// ([`catalog_answer`] takes them otherwise).
fn later(stmt: &Stmt) -> Option<String> {
    if catalog_kind(stmt).is_some_and(|kind| kind.needs_catalog()) {
        return Some(
            "This House has no catalog (start it with --catalog or --single-node), so it holds \
             no lake tables"
                .to_string(),
        );
    }
    let (what, task) = match stmt {
        Stmt::CreateTableAs { .. } => ("CREATE TABLE … AS SELECT", "HS1 Task 12"),
        Stmt::CreatePipe { .. } => (
            "A pipe (LoamsStream, IggyTopic, S3Queue)",
            "HS1 Tasks 16 and 18",
        ),
        Stmt::CreateMaterializedView { .. } => ("CREATE MATERIALIZED VIEW", "HS1 Task 17"),
        Stmt::Truncate { .. } => ("TRUNCATE", "HS1 Task 12"),
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

/// The catalog statement a [`Stmt`] is (HS1 Task 10), if any.
pub fn catalog_kind(stmt: &Stmt) -> Option<Kind> {
    Some(match stmt {
        Stmt::CreateDatabase { .. } => Kind::CreateDatabase,
        Stmt::CreateTable { .. } => Kind::CreateTable,
        Stmt::Drop(drop) if !drop.temporary => Kind::Drop,
        Stmt::Undrop { .. } => Kind::Undrop,
        Stmt::Rename { .. } => Kind::Rename,
        Stmt::AlterAddColumns { .. } => Kind::AlterAdd,
        Stmt::Describe { .. } => Kind::Describe,
        Stmt::Show { .. } => Kind::Show,
        Stmt::Exists { .. } => Kind::Exists,
        Stmt::Insert(_) => Kind::Insert,
        _ => return None,
    })
}

impl Kind {
    /// Whether the statement means nothing without a catalog (`48` then); the
    /// others fall through to chDB.
    pub fn needs_catalog(self) -> bool {
        !matches!(
            self,
            Kind::Describe | Kind::Show | Kind::Exists | Kind::Insert
        )
    }
}

/// What the catalog made of a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogOutcome {
    /// Done: an empty success.
    Done,
    /// Answer with this query's rows (a `SELECT` over constants).
    Rows(String),
    /// Not the catalog's: on to [`front_answer`] and chDB.
    NotOurs,
}

/// The catalog's answer to a statement (HS1 Task 10): DDL, `USE`, and
/// `DESCRIBE`/`SHOW CREATE`/`EXISTS`/`INSERT` naming a catalog table. `text` is
/// the statement without its `FORMAT`; `current` the request's database.
#[allow(clippy::too_many_arguments)]
pub async fn catalog_answer(
    lake: Option<&Lake>,
    pool: &WorkerPool,
    namespace: u64,
    current: &str,
    stmt: &Stmt,
    text: &str,
    session: Option<&SessionGuard>,
    query_id: &str,
) -> Result<CatalogOutcome, HouseError> {
    let Some(lake) = lake else {
        return Ok(CatalogOutcome::NotOurs);
    };
    if let Stmt::Use(database) = stmt {
        lake.require_database(namespace, database).await?;
        if let Some(guard) = session {
            guard.state().database = database.clone();
        }
        return Ok(CatalogOutcome::Done);
    }
    let Some(kind) = catalog_kind(stmt) else {
        return Ok(CatalogOutcome::NotOurs);
    };
    let text = match stmt {
        Stmt::Insert(insert) => insert.text.as_str(),
        _ => text,
    };
    let mut oracle = WorkerOracle::new(pool, namespace, query_id);
    let answer = ddl::execute(lake, namespace, current, kind, text, &mut oracle).await;
    oracle.finish();
    Ok(match answer? {
        ddl::Answer::Done => CatalogOutcome::Done,
        ddl::Answer::Rows(query) => CatalogOutcome::Rows(query),
        ddl::Answer::PassThrough => CatalogOutcome::NotOurs,
    })
}

/// The deny list over a catalog statement's syntax tree (HS1 Tasks 5 and 10):
/// one statement that ClickHouse can parse, nothing denied anywhere in it.
pub fn admit_ddl(analysis: &loams_house_ipc::Analysis, sql: &str) -> Result<(), HouseError> {
    let classification = analysis.classification;
    if classification.statements > 1 {
        return Err(HouseError::from(ChError::syntax_error(
            "Multi-statements are not allowed: send one statement per request",
        )));
    }
    if classification.class == QueryClass::Unknown {
        return Err(HouseError::from(ChError::syntax_error(format!(
            "ClickHouse's parser cannot parse this statement: {}",
            crate::classify::OUTSIDE_SURFACE
        ))));
    }
    let tree = deny::QueryTree::from_explain(&analysis.ast, analysis.query_tree.as_deref())
        .shared_create(crate::classify::creates_shared_object(sql))
        .statement(sql);
    deny::check(&tree).map_err(HouseError::from)
}

/// [`Oracle`] on a worker of the namespace, taken at the first question and
/// given back by [`WorkerOracle::finish`] (or when dropped).
struct WorkerOracle<'a> {
    pool: &'a WorkerPool,
    namespace: String,
    query_id: String,
    lease: Option<WorkerLease>,
}

impl<'a> WorkerOracle<'a> {
    fn new(pool: &'a WorkerPool, namespace: u64, query_id: &str) -> Self {
        Self {
            pool,
            namespace: namespace.to_string(),
            query_id: query_id.to_string(),
            lease: None,
        }
    }

    async fn lease(&mut self) -> Result<&mut WorkerLease, HouseError> {
        if self.lease.is_none() {
            self.lease = Some(self.pool.acquire(&self.namespace).await?);
        }
        self.lease
            .as_mut()
            .ok_or_else(|| HouseError::from(ChError::network_error("no worker")))
    }

    fn finish(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.pool.release(lease, Outcome::Completed);
        }
    }

    /// Runs an internal read and answers its `JSONCompactEachRow` lines.
    async fn rows(
        &mut self,
        sql: &str,
        params: Vec<(String, String)>,
        settings: Vec<(String, String)>,
    ) -> Result<Vec<Vec<String>>, HouseError> {
        let execute = Execute {
            query_id: format!("{}-ddl", self.query_id),
            session: None,
            settings,
            views: Vec::new(),
            sql: sql.to_string(),
            format: "JSONCompactEachRow".to_string(),
            params,
            limits: Limits::default(),
            input: None,
            output: Output::Blocks,
        };
        let collected = self.lease().await?.run(execute).await?;
        let text = String::from_utf8_lossy(&collected.bytes);
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<Vec<serde_json::Value>>(line)
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s,
                                other => other.to_string(),
                            })
                            .collect()
                    })
                    .map_err(|err| {
                        HouseError::from(ChError::network_error(format!(
                            "the worker's answer is unreadable: {err}"
                        )))
                    })
            })
            .collect()
    }
}

impl Drop for WorkerOracle<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl std::fmt::Debug for WorkerOracle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerOracle")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

/// `Array(String)` as a query parameter's text.
fn string_array(values: &[String]) -> String {
    let quoted: Vec<String> = values
        .iter()
        .map(|v| format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect();
    format!("[{}]", quoted.join(","))
}

#[async_trait]
impl Oracle for WorkerOracle<'_> {
    async fn check(&mut self, sql: &str) -> Result<(), HouseError> {
        let analysis = self
            .lease()
            .await?
            .analyze(sql, &[])
            .await
            .map_err(deny::as_setting_refusal)?;
        admit_ddl(&analysis, sql)
    }

    async fn describe(&mut self, columns: &str) -> Result<Vec<Described>, HouseError> {
        // `flatten_nested = 0`: a `Nested` column stays one column, which the type
        // table refuses, rather than arrays under dotted names.
        let rows = self
            .rows(
                "DESCRIBE TABLE null({columns:String})",
                vec![("columns".to_string(), columns.to_string())],
                vec![("flatten_nested".to_string(), "0".to_string())],
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                let get = |i: usize| row.get(i).cloned().unwrap_or_default();
                if row.len() < 7 {
                    return Err(HouseError::from(ChError::network_error(
                        "the worker's DESCRIBE answered too few columns",
                    )));
                }
                Ok(Described {
                    column: ChColumn {
                        name: get(0),
                        ch_type: get(1),
                        default_kind: get(2),
                        default_expression: get(3),
                        comment: get(4),
                        codec: get(5),
                    },
                    ttl: get(6),
                })
            })
            .collect()
    }

    async fn merge_tree_settings(&mut self, names: &[String]) -> Result<Vec<String>, HouseError> {
        let rows = self
            .rows(
                "SELECT name FROM system.merge_tree_settings WHERE name IN {names:Array(String)}",
                vec![("names".to_string(), string_array(names))],
                Vec::new(),
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| r.into_iter().next())
            .collect())
    }
}
