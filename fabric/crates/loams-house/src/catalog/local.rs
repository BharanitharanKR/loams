//! [`LocalCatalog`]: the file catalog with its pointers in one SQLite file, the
//! catalog of `--single-node` (§49 §17).
//!
//! Two tables: `namespaces (name, properties)` and `tables (namespace, name,
//! metadata_location)`, a namespace's levels joined by the unit separator U+001F
//! (as the REST protocol joins them in a path). A commit's pointer swap is one
//! `UPDATE … WHERE metadata_location = <the file it read>`: zero rows changed is
//! a conflict. SQLite serialises writers across processes, so two fronts on one
//! file stay correct (though `--single-node` runs one).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use loams_store::Store;
use rusqlite::{Connection, OptionalExtension, params};

use super::files::{FileCatalog, Pointers};
use super::{CatalogError, LakeRoot};

const SEPARATOR: &str = "\u{1f}";

/// The catalog's schema version, `PRAGMA user_version` (registry id
/// `house.catalog`, UP1 Task 6). A catalog from before UP1 reads 0, which
/// means 1: it is stamped on open.
pub const CATALOG_USER_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS namespaces (
    name TEXT PRIMARY KEY NOT NULL,
    properties TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tables (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata_location TEXT NOT NULL,
    PRIMARY KEY (namespace, name)
);
";

fn key(namespace: &[String]) -> String {
    namespace.join(SEPARATOR)
}

fn failed(err: rusqlite::Error) -> CatalogError {
    CatalogError::Unavailable(format!("the local catalog failed: {err}"))
}

#[derive(Debug)]
struct SqlitePointers {
    path: PathBuf,
    connection: Mutex<Connection>,
}

impl SqlitePointers {
    fn open(path: &Path) -> Result<Self, CatalogError> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|err| {
                CatalogError::Unavailable(format!(
                    "the local catalog's directory {}: {err}",
                    dir.display()
                ))
            })?;
        }
        let connection = Connection::open(path).map_err(failed)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(failed)?;
        connection.execute_batch(SCHEMA).map_err(failed)?;
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(failed)?;
        if version == 0 {
            // Absent means 1 (design §54 §4.1): stamp it, changing nothing else.
            connection
                .execute_batch(&format!("PRAGMA user_version = {CATALOG_USER_VERSION}"))
                .map_err(failed)?;
        } else if version != CATALOG_USER_VERSION {
            return Err(CatalogError::Unavailable(format!(
                "format_unreadable: the local catalog is at user_version {version}; this release \
                 reads {CATALOG_USER_VERSION}"
            )));
        }
        Ok(Self {
            path: path.to_path_buf(),
            connection: Mutex::new(connection),
        })
    }

    fn db(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn properties_json(properties: &HashMap<String, String>) -> Result<String, CatalogError> {
    serde_json::to_string(properties).map_err(|err| CatalogError::Invalid(err.to_string()))
}

impl Pointers for SqlitePointers {
    fn namespaces(&self) -> Result<Vec<Vec<String>>, CatalogError> {
        let db = self.db();
        let mut statement = db
            .prepare("SELECT name FROM namespaces ORDER BY name")
            .map_err(failed)?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(failed)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(failed)?;
        Ok(names
            .into_iter()
            .map(|n| n.split(SEPARATOR).map(str::to_string).collect())
            .collect())
    }

    fn namespace(
        &self,
        namespace: &[String],
    ) -> Result<Option<HashMap<String, String>>, CatalogError> {
        let text: Option<String> = self
            .db()
            .query_row(
                "SELECT properties FROM namespaces WHERE name = ?1",
                params![key(namespace)],
                |row| row.get(0),
            )
            .optional()
            .map_err(failed)?;
        text.map(|t| serde_json::from_str(&t).map_err(|err| CatalogError::Invalid(err.to_string())))
            .transpose()
    }

    fn add_namespace(
        &self,
        namespace: &[String],
        properties: &HashMap<String, String>,
    ) -> Result<bool, CatalogError> {
        let changed = self
            .db()
            .execute(
                "INSERT OR IGNORE INTO namespaces (name, properties) VALUES (?1, ?2)",
                params![key(namespace), properties_json(properties)?],
            )
            .map_err(failed)?;
        Ok(changed == 1)
    }

    fn remove_namespace(&self, namespace: &[String]) -> Result<bool, CatalogError> {
        let changed = self
            .db()
            .execute(
                "DELETE FROM namespaces WHERE name = ?1",
                params![key(namespace)],
            )
            .map_err(failed)?;
        Ok(changed == 1)
    }

    fn tables(&self, namespace: &[String]) -> Result<Vec<String>, CatalogError> {
        let db = self.db();
        let mut statement = db
            .prepare("SELECT name FROM tables WHERE namespace = ?1 ORDER BY name")
            .map_err(failed)?;
        statement
            .query_map(params![key(namespace)], |row| row.get::<_, String>(0))
            .map_err(failed)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(failed)
    }

    fn table(&self, namespace: &[String], name: &str) -> Result<Option<String>, CatalogError> {
        self.db()
            .query_row(
                "SELECT metadata_location FROM tables WHERE namespace = ?1 AND name = ?2",
                params![key(namespace), name],
                |row| row.get(0),
            )
            .optional()
            .map_err(failed)
    }

    fn add_table(
        &self,
        namespace: &[String],
        name: &str,
        location: &str,
    ) -> Result<bool, CatalogError> {
        let changed = self
            .db()
            .execute(
                "INSERT OR IGNORE INTO tables (namespace, name, metadata_location) \
                 VALUES (?1, ?2, ?3)",
                params![key(namespace), name, location],
            )
            .map_err(failed)?;
        Ok(changed == 1)
    }

    fn swap_table(
        &self,
        namespace: &[String],
        name: &str,
        old: &str,
        new: &str,
    ) -> Result<bool, CatalogError> {
        let changed = self
            .db()
            .execute(
                "UPDATE tables SET metadata_location = ?4 \
                 WHERE namespace = ?1 AND name = ?2 AND metadata_location = ?3",
                params![key(namespace), name, old, new],
            )
            .map_err(failed)?;
        Ok(changed == 1)
    }

    fn remove_table(&self, namespace: &[String], name: &str) -> Result<bool, CatalogError> {
        let changed = self
            .db()
            .execute(
                "DELETE FROM tables WHERE namespace = ?1 AND name = ?2",
                params![key(namespace), name],
            )
            .map_err(failed)?;
        Ok(changed == 1)
    }

    fn move_table(
        &self,
        from: (&[String], &str),
        to: (&[String], &str),
    ) -> Result<(), CatalogError> {
        let mut db = self.db();
        let tx = db.transaction().map_err(failed)?;
        let exists = |ns: &[String], name: &str| -> Result<bool, CatalogError> {
            tx.query_row(
                "SELECT 1 FROM tables WHERE namespace = ?1 AND name = ?2",
                params![key(ns), name],
                |_| Ok(()),
            )
            .optional()
            .map(|found| found.is_some())
            .map_err(failed)
        };
        if !exists(from.0, from.1)? {
            return Err(CatalogError::NoSuchTable(format!(
                "{}.{}",
                from.0.join("."),
                from.1
            )));
        }
        if exists(to.0, to.1)? {
            return Err(CatalogError::TableExists(format!(
                "{}.{}",
                to.0.join("."),
                to.1
            )));
        }
        tx.execute(
            "UPDATE tables SET namespace = ?3, name = ?4 WHERE namespace = ?1 AND name = ?2",
            params![key(from.0), from.1, key(to.0), to.1],
        )
        .map_err(failed)?;
        tx.commit().map_err(failed)
    }
}

/// The SQLite catalog of `--single-node` (`--catalog=local:<path>`), writing its
/// metadata files to the House's store.
#[derive(Debug)]
pub struct LocalCatalog(FileCatalog<SqlitePointers>);

impl LocalCatalog {
    /// Opens (creating if needed) the catalog file at `path`; metadata files go to
    /// `store`, whose objects workers reach as `root`.
    pub fn open(path: &Path, store: Store, root: LakeRoot) -> Result<Self, CatalogError> {
        Ok(Self(FileCatalog::new(
            SqlitePointers::open(path)?,
            store,
            root,
        )))
    }

    /// The SQLite file.
    pub fn path(&self) -> &Path {
        &self.0.pointers().path
    }
}

super::files::delegate_house_catalog!(LocalCatalog);
