//! Loams upgrades, the server side (design §54, UP1).
//!
//! - [`formats`]: the format registry, re-exported from
//!   [`loams_common::formats`], its only definition (D924).
//! - [`stack`]: the stack record, `_system/upgrade/stack` (§54 §4.2), and the
//!   check that this binary reads every format version it names.
//!
//! Later UP1 tasks add the ledger and lease, the runner, the startup gate,
//! preview.2's adoption, the store contexts, snapshots and restores.

pub mod formats;
pub mod stack;

pub use stack::{StackRecord, StoredVersions, Unreadable};
