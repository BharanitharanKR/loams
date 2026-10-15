//! Restoring a stack from an upgrade snapshot (design §54 §8): stop
//! everything, put back the old bundle, `.env` and `secrets/`, restore TiKV
//! into its volumes at the snapshot's version (the cold copy unpacked, or
//! fresh volumes and `br restore txn`), restore the Loams-owned stores with
//! `loams migrate restore`, start the previous release, verify, open the
//! door. Automatic after a failed upgrade (Q823); `loams upgrade rollback`
//! (UP1 Task 14) runs it on request.

use std::path::Path;

use crate::compose;
use crate::door;
use crate::errors::{Result, UpgradeError};
use crate::helpers::{self, SNAPSHOTS, volume};
use crate::install;
use crate::runtime::Runtime;
use crate::stackdir::{StackDir, read_env};

/// What to restore.
#[derive(Clone, Debug)]
pub struct Restore {
    pub snapshot: String,
    /// How TiKV was copied (`br` or `cold`) and at which version.
    pub tikv: Option<(String, String)>,
    /// The image that runs `loams migrate restore`: the one that took the
    /// snapshot (the upgrade's target), which reads its format.
    pub image: String,
    pub br_image: String,
}

fn run(runtime: &dyn Runtime, spec: &helpers::HelperSpec, what: &str) -> Result<()> {
    let out = runtime.engine(&spec.render())?;
    if out.ok() {
        Ok(())
    } else {
        let last = out.stderr.lines().last().unwrap_or_default().trim();
        Err(UpgradeError::new(
            "rollback_failed",
            format!("{what} (exit {}): {last}", out.status),
        ))
    }
}

/// Whether the snapshot is complete (its manifest written last), before
/// anything is changed.
fn complete(runtime: &dyn Runtime, stack: &StackDir, image: &str, id: &str) -> Result<()> {
    let listing = crate::snapshots::list(runtime, stack, image)?;
    let found = listing
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["id"] == id);
    match found {
        Some(s) if s["complete"] == true => Ok(()),
        Some(_) => Err(UpgradeError::new(
            "snapshot_incomplete",
            format!("snapshot {id} has no manifest: it cannot be restored"),
        )),
        None => Err(UpgradeError::new(
            "snapshot_not_found",
            format!("no snapshot {id} in the snapshot volume"),
        )),
    }
}

/// Restores `stack` from `restore`. See the module documentation.
pub fn restore(runtime: &dyn Runtime, stack: &mut StackDir, restore: &Restore) -> Result<()> {
    let id = &restore.snapshot;
    complete(runtime, stack, &restore.image, id)?;
    let pd_tikv = vec!["pd".to_string(), "tikv".to_string()];
    // Everything stops (the door was closed by the upgrade, or is now).
    let config = compose::config(runtime, &stack.dir)?;
    door::close(runtime, &stack.dir, &compose::services(&config))?;
    compose::stop(runtime, &stack.dir, &compose::services(&config))?;
    install::restore_host(&stack.dir, id)?;
    stack.env = read_env(&stack.dir.join(".env"));
    let tikv_version = match &restore.tikv {
        Some((method, version)) if method == "cold" => {
            run(
                runtime,
                &helpers::cold_copy_helper(stack, &restore.image, true, id),
                "unpacking TiKV's cold copy",
            )?;
            compose::up(runtime, &stack.dir, &pd_tikv)?;
            Some(version.clone())
        }
        Some((_, version)) => {
            // BR restores into a fresh cluster at the snapshot's version.
            let out = runtime.engine(&[
                "volume".to_string(),
                "rm".to_string(),
                "-f".to_string(),
                volume("pd-data"),
                volume("tikv-data"),
            ])?;
            if !out.ok() {
                return Err(UpgradeError::new(
                    "rollback_failed",
                    format!("removing TiKV's volumes: {}", out.stderr.trim()),
                ));
            }
            compose::up(runtime, &stack.dir, &pd_tikv)?;
            run(
                runtime,
                &helpers::br_helper(stack, &restore.br_image, true, id),
                "BR's restore of TiKV",
            )?;
            Some(version.clone())
        }
        None => {
            compose::up(runtime, &stack.dir, &pd_tikv)?;
            None
        }
    };
    let mut sub = vec!["restore".to_string()];
    sub.extend(helpers::migrate_store_args(stack, false));
    sub.push(format!("--from={SNAPSHOTS}/{id}"));
    if let Some(version) = tikv_version {
        sub.push(format!("--tikv-version={version}"));
    }
    sub.push("--output=json".to_string());
    run(
        runtime,
        &helpers::migrate_helper(stack, &restore.image, &sub),
        "loams migrate restore",
    )?;
    // The previous release, then its health, then the door.
    let config = compose::config(runtime, &stack.dir)?;
    let enabled = compose::services(&config);
    let services: Vec<String> = compose::long_running(&config)
        .into_iter()
        .filter(|s| !door::SIDECARS.contains(&s.as_str()))
        .collect();
    compose::up(runtime, &stack.dir, &services)?;
    let bad: Vec<String> = compose::unhealthy(&compose::ps(runtime, &stack.dir)?)
        .into_iter()
        .filter(|s| !door::SIDECARS.contains(&s.as_str()))
        .collect();
    if !bad.is_empty() {
        return Err(UpgradeError::new(
            "rollback_failed",
            format!("not healthy after the restore: {}", bad.join(", ")),
        ));
    }
    crate::run::engine_ready(runtime, &stack.dir)?;
    door::open(runtime, &stack.dir, &enabled)
}

/// `rollback_failed` (exit 11): the failure, the rollback's failure, where
/// the snapshot is and the manual steps.
pub fn failed(
    stack_dir: &Path,
    id: &str,
    cause: &UpgradeError,
    rollback: &UpgradeError,
) -> UpgradeError {
    let host = install::host_copy_dir(stack_dir, id);
    UpgradeError::new(
        "rollback_failed",
        format!(
            "{} ({}); the rollback failed too: {}. The snapshot is {id} (`loams upgrade \
             snapshots list`), its host files are in {}. By hand: `docker compose stop`; copy \
             the host files back into {}; restore TiKV (the cold copy's volumes.tar into \
             pd-data and tikv-data, or `br restore txn` into fresh volumes); run `loams migrate \
             restore --from {SNAPSHOTS}/{id}` in the loams image; `docker compose up -d`. See \
             docs/release/upgrades.md.",
            cause.message,
            cause.code,
            rollback.message,
            host.display(),
            stack_dir.display()
        ),
    )
}
