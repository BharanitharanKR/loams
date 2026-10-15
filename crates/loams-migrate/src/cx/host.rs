//! The host (S13): migrations never write the stack directory themselves.
//! They record what the CLI must change (`.env`, the compose bundle, a
//! container to recreate), and `loams upgrade` applies it, with the old
//! values kept in its snapshot.

use serde::Serialize;

/// One change a host migration asks the CLI to make.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostChange {
    /// Add or rename a `.env` variable (the user's value is kept).
    Env { name: String, default: String },
    /// Recreate a compose service (after its preconditions hold).
    Recreate { service: String, reason: String },
}

/// See the module documentation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostCx {
    changes: Vec<HostChange>,
}

impl HostCx {
    /// Records a change for the CLI; idempotent (a change asked twice is
    /// listed once).
    pub fn request(&mut self, change: HostChange) {
        if !self.changes.contains(&change) {
            self.changes.push(change);
        }
    }

    /// Every change asked for, in order.
    pub fn changes(&self) -> &[HostChange] {
        &self.changes
    }
}
