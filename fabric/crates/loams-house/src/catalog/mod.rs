//! The catalog of record for lake tables (HS1 Task 10, design §49 §5).
//!
//! Every table the House writes is an Iceberg table in the namespace's catalog,
//! with its files in the bucket. [`HouseCatalog`] is the Iceberg REST catalog's
//! operations, as the House uses them, with three implementations:
//!
//! * [`RestCatalog`] — an Iceberg REST catalog (Lakekeeper in production), which
//!   writes metadata files itself;
//! * [`LocalCatalog`] — the SQLite catalog of `--single-node` (§49 §17): the House
//!   writes each metadata file to the store and swaps the table's pointer with
//!   compare-and-set in SQLite;
//! * [`MemCatalog`] — the same in memory, for tests.
//!
//! The ClickHouse view of it — databases, tables, their classes and columns,
//! `DROP` and `UNDROP` — is [`Lake`], which maps a namespace's ClickHouse database
//! `d` onto the Iceberg namespace `ns_<id>.d` (`default` onto `ns_<id>`, §49 §5.3)
//! and places every table under `s3://<bucket>/<prefix>/ns/<id>/house/`, the form
//! `house-cache` serves to that namespace's workers as is (R9.5).

mod files;
mod lake;
mod local;
mod mem;
mod rest;

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use loams_house_lake::spec::{Schema, TableMetadata};
use loams_house_lake::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};

pub use lake::{DatabaseInfo, Lake, LakeRoot, PrefixGrants, RESERVED_PREFIX, register_all};
pub use local::LocalCatalog;
pub use mem::MemCatalog;
pub use rest::{RestCatalog, RestOptions};

use crate::errors::{ChError, HouseError};

/// A catalog operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    /// No such namespace.
    #[error("namespace {0} does not exist")]
    NoSuchNamespace(String),
    /// No such table.
    #[error("table {0} does not exist")]
    NoSuchTable(String),
    /// The namespace exists already.
    #[error("namespace {0} already exists")]
    NamespaceExists(String),
    /// The table exists already.
    #[error("table {0} already exists")]
    TableExists(String),
    /// The namespace still holds tables or namespaces.
    #[error("namespace {0} is not empty")]
    NotEmpty(String),
    /// A commit's requirement failed: someone else committed first.
    #[error("commit conflict on {table}: {message}")]
    Conflict {
        /// The table.
        table: String,
        /// The catalog's words.
        message: String,
    },
    /// The request is not valid (a bad schema, a location outside the store).
    #[error("{0}")]
    Invalid(String),
    /// The catalog or the store could not be reached, or failed (`210`, §49 §11).
    #[error("{0}")]
    Unavailable(String),
}

impl From<CatalogError> for HouseError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::NoSuchNamespace(_) => {
                HouseError::from(ChError::unknown_database(err.to_string()))
            }
            CatalogError::NoSuchTable(_) => {
                HouseError::from(ChError::unknown_table(err.to_string()))
            }
            CatalogError::TableExists(_) => {
                HouseError::from(ChError::table_already_exists(err.to_string()))
            }
            CatalogError::NamespaceExists(_) => {
                crate::ddl::clickhouse_error(82, "DATABASE_ALREADY_EXISTS", err.to_string())
            }
            CatalogError::NotEmpty(_) | CatalogError::Invalid(_) => {
                HouseError::from(ChError::bad_arguments(err.to_string()))
            }
            CatalogError::Conflict { .. } | CatalogError::Unavailable(_) => {
                HouseError::from(ChError::network_error(format!("The catalog failed: {err}")))
            }
        }
    }
}

/// A table as a catalog holds it: its identifier, the metadata file the catalog
/// points at, and that file's contents.
#[derive(Debug, Clone, PartialEq)]
pub struct LakeTable {
    /// Namespace and name.
    pub ident: TableIdent,
    /// The current metadata file.
    pub metadata_location: String,
    /// Its contents.
    pub metadata: TableMetadata,
}

/// The Iceberg catalog operations the House uses (the Shared contracts' catalog
/// trait). Namespaces nest (`ns_7` holds `ns_7.sales`), as the REST catalog's do;
/// `list_databases(parent)` lists a namespace's direct children.
///
/// No operation deletes a data or metadata file: `drop_table` forgets a table
/// (Iceberg's drop without purge), and the House purges only after the retention
/// of §49 §11 (HS1 Task 15).
#[async_trait]
pub trait HouseCatalog: Send + Sync + fmt::Debug {
    /// The namespaces directly under `parent`.
    async fn list_databases(
        &self,
        parent: &NamespaceIdent,
    ) -> Result<Vec<NamespaceIdent>, CatalogError>;
    /// Creates a namespace. Its parent must exist (except for a top-level one).
    async fn create_database(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<(), CatalogError>;
    /// A namespace's properties; [`CatalogError::NoSuchNamespace`] when it does not
    /// exist.
    async fn database_properties(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<HashMap<String, String>, CatalogError>;
    /// Drops an empty namespace ([`CatalogError::NotEmpty`] otherwise).
    async fn drop_database(&self, namespace: &NamespaceIdent) -> Result<(), CatalogError>;
    /// The tables of a namespace.
    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>, CatalogError>;
    /// A table's current metadata.
    async fn load_table(&self, table: &TableIdent) -> Result<LakeTable, CatalogError>;
    /// Creates a table. The creation names its location.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<LakeTable, CatalogError>;
    /// Forgets a table; its files stay.
    async fn drop_table(&self, table: &TableIdent) -> Result<(), CatalogError>;
    /// Renames a table, across namespaces of the catalog too.
    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> Result<(), CatalogError>;
    /// Commits `updates` if every requirement holds against the current metadata
    /// ([`CatalogError::Conflict`] otherwise), as one compare-and-set of the
    /// table's metadata pointer.
    async fn commit(
        &self,
        table: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<LakeTable, CatalogError>;
    /// Adds an existing metadata file as a table (Iceberg REST `register`).
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: &str,
    ) -> Result<LakeTable, CatalogError>;

    /// Makes `schema` the table's current schema, on the condition that the schema
    /// `table` was loaded with is still current, with `properties` set in the same
    /// commit.
    async fn update_schema(
        &self,
        table: &LakeTable,
        schema: Schema,
        properties: HashMap<String, String>,
    ) -> Result<LakeTable, CatalogError> {
        let mut updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        if !properties.is_empty() {
            updates.push(TableUpdate::SetProperties {
                updates: properties,
            });
        }
        self.commit(
            &table.ident,
            vec![TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata.current_schema_id(),
            }],
            updates,
        )
        .await
    }
}

/// A namespace as text, for messages: its levels joined by `.`.
pub fn namespace_text(namespace: &NamespaceIdent) -> String {
    namespace.as_ref().join(".")
}

/// A table as text, for messages.
pub fn table_text(table: &TableIdent) -> String {
    format!("{}.{}", namespace_text(table.namespace()), table.name())
}
