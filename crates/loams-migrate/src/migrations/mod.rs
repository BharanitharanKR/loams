//! The migrations compiled into this binary, in order (design §54 §6.3,
//! D929). Each is listed once in `release/upgrade-path.toml` under the
//! release that introduced it. UP1 Task 5 adds `0000-adopt-preview-2` and
//! Task 10 `0001-safekeeper-volume`.

mod m0000_adopt_preview_2;

pub use m0000_adopt_preview_2::AdoptPreview2;

use crate::migration::Migration;

/// Every migration of this binary, oldest first.
pub static MIGRATIONS: &[&dyn Migration] = &[&AdoptPreview2];
