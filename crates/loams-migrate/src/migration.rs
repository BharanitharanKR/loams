//! The migration contract (design §54 §6.3, D929; UP1 Task 3).
//!
//! A migration names its store, its format version changes and its class. It
//! is idempotent and resumable: [`Migration::apply`] does bounded work and
//! returns [`Step::Continue`] with a cursor the runner checkpoints in the
//! ledger, or [`Step::Done`] once it has switched. It writes beside the old
//! data and switches with one atomic step at the end (a pointer CAS, a
//! metastore transaction, a rename), so until then the old release reads
//! everything. Migrations are compiled in ([`crate::migrations::MIGRATIONS`]);
//! nothing is downloaded at upgrade time.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use loams_common::meta::MetaStore;
use sha2::{Digest, Sha256};

pub use crate::formats::{FormatId, Store};
pub use crate::ledger::Class;

/// Where an interrupted migration resumes; at most
/// [`MAX_CURSOR`](crate::ledger::MAX_CURSOR) bytes.
pub type Cursor = Vec<u8>;

/// What one call of [`Migration::apply`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// More to do; resume from this cursor.
    Continue(Cursor),
    /// Switched: the migration is complete.
    Done,
}

/// What a migration expects to cost (shown by `loams upgrade check`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Estimate {
    pub rows: u64,
    pub bytes: u64,
    pub seconds: u64,
}

/// A migration's failure: a stable `code` and a message for the ledger.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct MigrationError {
    pub code: String,
    pub message: String,
}

impl MigrationError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        MigrationError {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// What a migration may touch. UP1 Task 6 adds the typed store contexts; a
/// migration reaches only its own store's.
#[derive(Clone, Debug)]
pub struct Cx {
    pub meta: Arc<dyn MetaStore>,
    /// The engine's data directory (`--data-dir`), when the store has one.
    pub data_dir: Option<PathBuf>,
    /// The release the operator says the stack is at (`--declared-release`).
    pub declared_release: Option<String>,
}

impl Cx {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Cx {
            meta,
            data_dir: None,
            declared_release: None,
        }
    }

    /// Called by a migration right before its atomic switch: the crash
    /// matrix's `migrate::before_switch` failpoint.
    pub fn before_switch(&self) -> Result<(), MigrationError> {
        crate::failpoint!("migrate::before_switch");
        Ok(())
    }
}

/// One migration (§54 §6.3).
#[async_trait]
pub trait Migration: Send + Sync {
    /// `NNNN-kebab-name`, unique, listed once in `release/upgrade-path.toml`.
    fn id(&self) -> &'static str;
    fn store(&self) -> Store;
    /// `(format, from, to)` for every format whose enabled version it changes.
    fn formats(&self) -> &'static [(FormatId, u16, u16)];
    fn class(&self) -> Class;
    /// The release whose unstamped data this migration adopts (writes the
    /// stack record of), when it is an adoption (`0000-adopt-preview-2`).
    fn adopts(&self) -> Option<&'static str> {
        None
    }
    fn estimate(&self, cx: &Cx) -> Result<Estimate, MigrationError>;
    async fn apply(&self, cx: &mut Cx, cursor: Option<Cursor>) -> Result<Step, MigrationError>;
    async fn verify(&self, cx: &Cx) -> Result<(), MigrationError>;
    /// Removes what a failed run wrote beside the old data, before a
    /// rollback or a fresh start.
    async fn abandon(&self, cx: &mut Cx) -> Result<(), MigrationError>;

    /// A hash of the migration's identity (id, store, class, format changes):
    /// a binary whose migration of the same id differs refuses to resume
    /// another binary's run (`migration_mismatch`).
    fn code_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(self.id().as_bytes());
        hash.update([0]);
        hash.update(self.store().as_str().as_bytes());
        hash.update([0]);
        hash.update(match self.class() {
            Class::Reversible => b"reversible".as_slice(),
            Class::PointOfNoReturn => b"point_of_no_return".as_slice(),
        });
        for (id, from, to) in self.formats() {
            hash.update([0]);
            hash.update(id.as_str().as_bytes());
            hash.update(from.to_be_bytes());
            hash.update(to.to_be_bytes());
        }
        hash.finalize().into()
    }
}
