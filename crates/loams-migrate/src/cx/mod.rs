//! What a migration may touch (design §54 §6.3, D929; UP1 Task 6): one typed
//! context per store, each with an expand-then-switch helper, so a
//! migration writes beside the old data and switches with one atomic step:
//!
//! | Store | Context | Writes beside | Switches with |
//! |---|---|---|---|
//! | bucket | [`BucketCx`] | `write_object_new` (put-if-absent) | `cas_pointer` |
//! | metastore | [`MetaCx`] | new pointer keys | `switch` (one pointer CAS) |
//! | Live | [`LiveCx`] | new keys in a `txn` | the `txn` that writes the switch key |
//! | Durable | [`DurableCx`] | a new SQLite file | `swap_file` (one `rename`) |
//! | House | [`HouseCx`] | a sibling directory | `swap_dir` (one `renameat2` exchange) |
//! | `pg-control` | [`PgControlCx`] | a new record file | `swap_file` |
//! | host | [`HostCx`] | — | records changes the CLI applies |
//!
//! The metastore has no multi-key transaction (UP1 ruling T0-2): its
//! all-or-nothing unit is one pointer CAS, so a migration writes its new keys
//! first and switches one pointer last.

mod bucket;
mod durable;
mod files;
mod host;
mod house;
mod live;
mod meta;
mod pg_control;

use std::path::PathBuf;
use std::sync::Arc;

pub use bucket::BucketCx;
pub use durable::DurableCx;
pub use host::{HostChange, HostCx};
pub use house::HouseCx;
pub use live::LiveCx;
use loams_common::meta::MetaStore;
pub use meta::MetaCx;
pub use pg_control::PgControlCx;

use crate::migration::MigrationError;

/// The stores of one `migrate` run. A migration asks for its own store's
/// context; a store the run was not given is `store_missing`.
#[derive(Clone, Debug)]
pub struct Cx {
    pub meta: Arc<dyn MetaStore>,
    /// The bucket (`--bucket`).
    pub bucket: Option<loams_store::Store>,
    /// Live's store (`--live-store`).
    pub live: Option<loams_kv::Store>,
    /// The engine's data directory (`--data-dir`): Durable's SQLite store.
    pub data_dir: Option<PathBuf>,
    /// The House data directory (`--house-dir`).
    pub house_dir: Option<PathBuf>,
    /// `pg-control`'s data directory (`--pg-control-dir`).
    pub pg_control_dir: Option<PathBuf>,
    /// The release the operator says the stack is at (`--declared-release`).
    pub declared_release: Option<String>,
    /// Loams Postgres's pageserver and safekeeper APIs (`--pageserver`,
    /// `--safekeeper`), when the stack runs Postgres.
    pub postgres: Option<(String, String)>,
    /// What host migrations asked the CLI to change.
    pub host: HostCx,
}

fn missing(store: &str, flag: &str) -> MigrationError {
    MigrationError::new(
        "store_missing",
        format!("this migration needs the {store} ({flag})"),
    )
}

impl Cx {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Cx {
            meta,
            bucket: None,
            live: None,
            data_dir: None,
            house_dir: None,
            pg_control_dir: None,
            declared_release: None,
            postgres: None,
            host: HostCx::default(),
        }
    }

    /// Called by a migration right before its atomic switch: the crash
    /// matrix's `migrate::before_switch` failpoint.
    pub fn before_switch(&self) -> Result<(), MigrationError> {
        crate::failpoint!("migrate::before_switch");
        Ok(())
    }

    pub fn bucket_cx(&self) -> Result<BucketCx<'_>, MigrationError> {
        let store = self
            .bucket
            .as_ref()
            .ok_or_else(|| missing("bucket", "--bucket"))?;
        Ok(BucketCx::new(&*self.meta, store))
    }

    pub fn meta_cx(&self) -> MetaCx<'_> {
        MetaCx::new(&*self.meta)
    }

    pub fn live_cx(&self) -> Result<LiveCx<'_>, MigrationError> {
        let store = self
            .live
            .as_ref()
            .ok_or_else(|| missing("Live store", "--live-store"))?;
        Ok(LiveCx::new(store))
    }

    pub fn durable_cx(&self) -> Result<DurableCx, MigrationError> {
        let dir = self
            .data_dir
            .as_ref()
            .ok_or_else(|| missing("data directory", "--data-dir"))?;
        Ok(DurableCx::new(dir.join("durable").join("default.db")))
    }

    pub fn house_cx(&self) -> Result<HouseCx, MigrationError> {
        let dir = self
            .house_dir
            .as_ref()
            .ok_or_else(|| missing("House directory", "--house-dir"))?;
        Ok(HouseCx::new(dir.clone()))
    }

    pub fn pg_control_cx(&self) -> Result<PgControlCx, MigrationError> {
        let dir = self
            .pg_control_dir
            .as_ref()
            .ok_or_else(|| missing("pg-control directory", "--pg-control-dir"))?;
        Ok(PgControlCx::new(dir.clone()))
    }

    pub fn host_cx(&mut self) -> &mut HostCx {
        &mut self.host
    }
}

fn io_error(what: &str, e: impl std::fmt::Display) -> MigrationError {
    MigrationError::new("io", format!("{what}: {e}"))
}
