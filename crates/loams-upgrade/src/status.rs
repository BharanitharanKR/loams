//! `loams upgrade status` (design §54 §10.1): read-only. The release `.env`
//! names, what `loams migrate status` says of the stores (the stack record
//! and the ledger, read by a helper of the stack's own image), and the last
//! attempt from the host history file.

use schemars::JsonSchema;
use serde::Serialize;

use crate::bundle::loams_image;
use crate::errors::Result;
use crate::helpers::{migrate_helper, migrate_store_args};
use crate::runtime::Runtime;
use crate::stackdir::StackDir;

/// The host history file, relative to the stack directory (one JSON object
/// per attempt).
pub const HISTORY: &str = ".loams-upgrade/history.jsonl";

/// `status`'s JSON.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
pub struct StatusReport {
    /// The release `.env` names.
    pub release: String,
    /// `loams migrate status`'s output (the stack record and the ledger);
    /// `null` when the helper could not read the stores.
    pub migrate: Option<serde_json::Value>,
    /// Why `migrate` is `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrate_error: Option<String>,
    /// The last line of the history file, if any.
    pub last_attempt: Option<serde_json::Value>,
}

/// The last attempt recorded in the stack's history file.
pub fn last_attempt(stack: &StackDir) -> Option<serde_json::Value> {
    let text = std::fs::read_to_string(stack.dir.join(HISTORY)).ok()?;
    let line = text.lines().rfind(|l| !l.trim().is_empty())?;
    serde_json::from_str(line).ok()
}

/// `status` through `runtime`.
pub fn status(runtime: &dyn Runtime, stack: &StackDir, current: &str) -> Result<StatusReport> {
    let image = loams_image(&stack.env, current);
    let mut sub = vec!["status".to_string()];
    sub.extend(migrate_store_args(stack, false));
    sub.push("--output=json".to_string());
    let out = runtime.engine(&migrate_helper(stack, &image, &sub).render())?;
    let (migrate, migrate_error) = if out.ok() {
        match crate::runtime::helper_json(&out.stdout) {
            Ok(value) => (Some(value), None),
            Err(e) => (None, Some(format!("loams migrate status's output: {e}"))),
        }
    } else {
        // preview.2's image has no `migrate` command, and a stopped TiKV
        // cannot be read: both are reported, not fatal.
        let last = out.stderr.lines().last().unwrap_or_default().trim();
        (None, Some(format!("loams migrate status: {last}")))
    };
    Ok(StatusReport {
        release: current.to_string(),
        migrate,
        migrate_error,
        last_attempt: last_attempt(stack),
    })
}
