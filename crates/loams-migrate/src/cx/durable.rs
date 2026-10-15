//! The Durable SQLite store (S11): read its schema level, write a new
//! database beside it, and switch with one `rename`.

use std::path::{Path, PathBuf};

use super::files::{beside, rename_over};
use super::io_error;
use crate::migration::MigrationError;

/// See the module documentation.
#[derive(Clone, Debug)]
pub struct DurableCx {
    path: PathBuf,
}

impl DurableCx {
    pub fn new(path: PathBuf) -> Self {
        DurableCx { path }
    }

    /// The database file (`<data-dir>/durable/default.db`).
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// A read-only connection to the current database.
    pub fn conn(&self) -> Result<rusqlite::Connection, MigrationError> {
        rusqlite::Connection::open_with_flags(
            &self.path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| io_error(&self.path.display().to_string(), e))
    }

    /// `PRAGMA user_version` (0 when never set).
    pub fn user_version(&self) -> Result<i64, MigrationError> {
        self.conn()?
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|e| io_error("user_version", e))
    }

    /// The Resonate migrations the database lists as applied, in order.
    pub fn applied_migrations(&self) -> Result<Vec<i64>, MigrationError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .map_err(|e| io_error("_sqlx_migrations", e))?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| io_error("_sqlx_migrations", e))?;
        rows.collect::<Result<_, _>>()
            .map_err(|e| io_error("_sqlx_migrations", e))
    }

    /// Where the new database is written before the switch.
    pub fn new_path(&self) -> PathBuf {
        beside(&self.path)
    }

    /// Copies the current database to [`DurableCx::new_path`] with SQLite's
    /// backup API (consistent even if a reader has it open) and returns a
    /// read-write connection to the copy, which a migration then changes.
    pub fn copy_beside(&self) -> Result<rusqlite::Connection, MigrationError> {
        let new = self.new_path();
        let _ = std::fs::remove_file(&new);
        let source = self.conn()?;
        let mut target = rusqlite::Connection::open(&new)
            .map_err(|e| io_error(&new.display().to_string(), e))?;
        {
            let backup = rusqlite::backup::Backup::new(&source, &mut target)
                .map_err(|e| io_error("backup", e))?;
            backup
                .run_to_completion(256, std::time::Duration::ZERO, None)
                .map_err(|e| io_error("backup", e))?;
        }
        Ok(target)
    }

    /// The switch: the new database replaces the current one in one
    /// `rename`. Its WAL and shared-memory files are checkpointed away first
    /// (the engine is stopped, so nothing else writes).
    pub fn swap_file(&self) -> Result<(), MigrationError> {
        let new = self.new_path();
        {
            let conn = rusqlite::Connection::open(&new)
                .map_err(|e| io_error(&new.display().to_string(), e))?;
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .map_err(|e| io_error("checkpoint", e))?;
        }
        for suffix in ["-wal", "-shm"] {
            let mut stale = self.path.clone().into_os_string();
            stale.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(stale));
        }
        rename_over(&new, &self.path)
    }

    /// Removes a partial new database (`abandon`).
    pub fn discard_new(&self) -> Result<(), MigrationError> {
        match std::fs::remove_file(self.new_path()) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("discarding the new database", e)),
        }
    }
}
