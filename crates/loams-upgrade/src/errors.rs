//! `loams upgrade`'s errors: a stable code and its exit code (design §54
//! §10.1, following §30 §6.3; 10 and 11 are new to `upgrade`).

use std::fmt;

/// Every error code with its exit code.
pub const CODES: &[(&str, u8)] = &[
    ("internal", 1),
    ("usage", 2),
    ("confirmation_required", 3),
    ("stack_not_found", 4),
    ("release_not_found", 4),
    ("snapshot_not_found", 4),
    ("upgrade_step_not_adjacent", 5),
    ("component_downgrade", 5),
    ("insufficient_space", 5),
    ("stack_unhealthy", 5),
    ("migration_incomplete", 5),
    ("upgrade_in_progress", 5),
    ("snapshot_incomplete", 5),
    ("rollback_needs_snapshot", 6),
    ("stack_unrecognised", 6),
    ("platform_unsupported", 6),
    ("timeout", 7),
    ("network", 7),
    ("registry", 7),
    ("checksum_mismatch", 9),
    ("signature_invalid", 9),
    ("manifest_invalid", 9),
    ("upgrade_failed_rolled_back", 10),
    ("rollback_failed", 11),
];

/// The exit code of `code` (1 for an unknown one: a bug).
pub fn exit_for(code: &str) -> u8 {
    CODES
        .iter()
        .find(|(c, _)| *c == code)
        .map_or(1, |(_, exit)| *exit)
}

/// An `upgrade` failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpgradeError {
    pub code: &'static str,
    pub message: String,
}

impl UpgradeError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        UpgradeError {
            code,
            message: message.into(),
        }
    }

    pub fn exit_code(&self) -> u8 {
        exit_for(self.code)
    }

    /// The JSON form (`--output json`): never a secret, only what failed.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"error": self.code, "message": self.message})
    }
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for UpgradeError {}

pub type Result<T> = std::result::Result<T, UpgradeError>;
