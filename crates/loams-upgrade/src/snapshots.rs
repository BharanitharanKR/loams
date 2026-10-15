//! `loams upgrade snapshots list|prune` (design §54 §7.3, §10.1): the
//! snapshots kept on this host, listed by id, time, size and digest only;
//! a prune removes a snapshot's GC root, then its files.

use crate::errors::{Result, UpgradeError};
use crate::helpers::{SNAPSHOTS, migrate_helper, migrate_store_args};
use crate::runtime::Runtime;
use crate::stackdir::StackDir;

/// The snapshots, as `loams migrate snapshots list` reports them.
pub fn list(runtime: &dyn Runtime, stack: &StackDir, image: &str) -> Result<serde_json::Value> {
    let sub = vec![
        "snapshots".to_string(),
        "list".to_string(),
        format!("--dir={SNAPSHOTS}"),
    ];
    let out = runtime.engine(&migrate_helper(stack, image, &sub).render())?;
    if !out.ok() {
        return Err(UpgradeError::new(
            "internal",
            format!("listing snapshots: {}", out.stderr.trim()),
        ));
    }
    serde_json::from_str(out.stdout.trim())
        .map_err(|e| UpgradeError::new("internal", format!("the snapshot listing: {e}")))
}

/// Removes snapshot `id` (its GC root, then its files). Needs `--yes` or a
/// confirmation (`confirmed`).
pub fn prune(
    runtime: &dyn Runtime,
    stack: &StackDir,
    image: &str,
    id: &str,
    confirmed: bool,
) -> Result<()> {
    if !confirmed {
        return Err(UpgradeError::new(
            "confirmation_required",
            format!("pruning snapshot {id} removes the only way back; pass --yes"),
        ));
    }
    let mut sub = vec!["snapshots".to_string(), "prune".to_string()];
    sub.extend(migrate_store_args(stack, false));
    sub.push(format!("--dir={SNAPSHOTS}"));
    sub.push(format!("--id={id}"));
    let out = runtime.engine(&migrate_helper(stack, image, &sub).render())?;
    if !out.ok() {
        let code = if out.stderr.contains("snapshot_not_found") {
            "snapshot_not_found"
        } else {
            "internal"
        };
        return Err(UpgradeError::new(code, out.stderr.trim().to_string()));
    }
    Ok(())
}
