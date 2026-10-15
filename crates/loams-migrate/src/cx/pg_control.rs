//! `pg-control`'s records (S9): the record file in its data directory,
//! replaced as a whole by a new file beside it.

use std::path::{Path, PathBuf};

use super::files::{beside, rename_over, write_synced};
use crate::migration::MigrationError;

/// `pg-control`'s embedded record file (`--store=local`).
pub const RECORD_FILE: &str = "pg-control.redb";

/// See the module documentation.
#[derive(Clone, Debug)]
pub struct PgControlCx {
    dir: PathBuf,
}

impl PgControlCx {
    pub fn new(dir: PathBuf) -> Self {
        PgControlCx { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The record file.
    pub fn records(&self) -> PathBuf {
        self.dir.join(RECORD_FILE)
    }

    /// Writes the new record file beside the current one.
    pub fn write_new(&self, bytes: &[u8]) -> Result<(), MigrationError> {
        write_synced(&beside(&self.records()), bytes)
    }

    /// The switch: the new record file replaces the current one.
    pub fn swap_file(&self) -> Result<(), MigrationError> {
        let records = self.records();
        rename_over(&beside(&records), &records)
    }
}
