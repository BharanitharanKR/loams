//! What `loams.house.v1`'s browsing RPCs read (HS1 Task 8): databases, tables,
//! the query log and pipes.
//!
//! Their sources are later tasks' — the catalog (Task 10), the query log (Task 23)
//! and pipes (Task 16) — so the front reads them through [`ApiCatalog`], and until
//! those land it answers from [`FixtureCatalog`]: what this House really has (the
//! namespace's `default` database, with no lake tables yet), or, in tests, the
//! fixtures a test fills in. A source that does not exist yet answers `None`, and
//! the RPC answers `unimplemented` (reason `not_implemented`) naming the task that
//! serves it, so a client can tell "nothing yet" from "empty".

use std::collections::BTreeMap;

use loams_house_proto::loams::house::v1 as pb;

use crate::errors::{ChError, HouseError};

/// The read side of the House's metadata, per namespace.
pub trait ApiCatalog: Send + Sync + std::fmt::Debug {
    /// The namespace's databases, by name.
    fn databases(&self, namespace: u64) -> Result<Vec<pb::Database>, HouseError>;
    /// A database's tables, by name; `81` when there is no such database.
    fn tables(&self, namespace: u64, database: &str) -> Result<Vec<pb::Table>, HouseError>;
    /// One table; `81` or `60` when it does not exist.
    fn describe(
        &self,
        namespace: u64,
        database: &str,
        table: &str,
    ) -> Result<pb::DescribeTableResponse, HouseError>;
    /// The namespace's query log, newest first, or `None` until it exists.
    fn query_log(&self, namespace: u64) -> Option<Vec<pb::QueryRecord>>;
    /// The namespace's pipes, by name, or `None` until they exist.
    fn pipes(&self, namespace: u64) -> Option<Vec<pb::Pipe>>;
}

/// The task that serves the query log (§49 §15).
pub const QUERY_LOG_TASK: &str = "HS1 Task 23";
/// The task that serves pipes (§49 §9).
pub const PIPES_TASK: &str = "HS1 Task 16";
/// The task that serves `RestoreTable` (§49 §8).
pub const RESTORE_TASK: &str = "HS1 Task 15";

/// The catalog until Tasks 10, 16 and 23: every namespace has `default`, and
/// whatever a test adds.
#[derive(Debug, Default, Clone)]
pub struct FixtureCatalog {
    /// Tables and their descriptions, by namespace, database and name.
    tables: BTreeMap<(u64, String, String), pb::DescribeTableResponse>,
    /// Extra databases, by namespace and name (`default` always exists).
    databases: BTreeMap<(u64, String), String>,
    query_log: BTreeMap<u64, Vec<pb::QueryRecord>>,
    pipes: BTreeMap<u64, Vec<pb::Pipe>>,
}

impl FixtureCatalog {
    /// What this House has before Task 10: `default`, empty.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Adds a database (with a comment) to a namespace.
    pub fn with_database(mut self, namespace: u64, name: &str, comment: &str) -> Self {
        self.databases
            .insert((namespace, name.to_string()), comment.to_string());
        self
    }

    /// Adds a table; its database is the one its `table` entry names.
    pub fn with_table(mut self, namespace: u64, described: pb::DescribeTableResponse) -> Self {
        let table = described.table.as_option().cloned().unwrap_or_default();
        if table.database != "default" {
            self.databases
                .entry((namespace, table.database.clone()))
                .or_default();
        }
        self.tables
            .insert((namespace, table.database, table.name), described);
        self
    }

    /// Gives a namespace a query log (newest first).
    pub fn with_query_log(mut self, namespace: u64, log: Vec<pb::QueryRecord>) -> Self {
        self.query_log.insert(namespace, log);
        self
    }

    /// Gives a namespace pipes.
    pub fn with_pipes(mut self, namespace: u64, pipes: Vec<pb::Pipe>) -> Self {
        self.pipes.insert(namespace, pipes);
        self
    }

    fn has_database(&self, namespace: u64, database: &str) -> bool {
        database == "default"
            || self
                .databases
                .contains_key(&(namespace, database.to_string()))
    }
}

fn unknown_database(database: &str) -> HouseError {
    HouseError::from(ChError::unknown_database(format!(
        "Database {database} does not exist"
    )))
}

impl ApiCatalog for FixtureCatalog {
    fn databases(&self, namespace: u64) -> Result<Vec<pb::Database>, HouseError> {
        let mut names: BTreeMap<String, String> = self
            .databases
            .iter()
            .filter(|((ns, _), _)| *ns == namespace)
            .map(|((_, name), comment)| (name.clone(), comment.clone()))
            .collect();
        names.entry("default".to_string()).or_default();
        Ok(names
            .into_iter()
            .map(|(name, comment)| pb::Database {
                table_count: self
                    .tables
                    .keys()
                    .filter(|(ns, db, _)| *ns == namespace && *db == name)
                    .count() as u64,
                name,
                comment,
                ..Default::default()
            })
            .collect())
    }

    fn tables(&self, namespace: u64, database: &str) -> Result<Vec<pb::Table>, HouseError> {
        if !self.has_database(namespace, database) {
            return Err(unknown_database(database));
        }
        Ok(self
            .tables
            .iter()
            .filter(|((ns, db, _), _)| *ns == namespace && db == database)
            .filter_map(|(_, described)| described.table.as_option().cloned())
            .collect())
    }

    fn describe(
        &self,
        namespace: u64,
        database: &str,
        table: &str,
    ) -> Result<pb::DescribeTableResponse, HouseError> {
        if !self.has_database(namespace, database) {
            return Err(unknown_database(database));
        }
        self.tables
            .get(&(namespace, database.to_string(), table.to_string()))
            .cloned()
            .ok_or_else(|| {
                HouseError::from(ChError::unknown_table(format!(
                    "Table {database}.{table} does not exist"
                )))
            })
    }

    fn query_log(&self, namespace: u64) -> Option<Vec<pb::QueryRecord>> {
        self.query_log.get(&namespace).cloned()
    }

    fn pipes(&self, namespace: u64) -> Option<Vec<pb::Pipe>> {
        self.pipes.get(&namespace).cloned()
    }
}
