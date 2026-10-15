//! The startup gate (design §54 §4.3, D924; UP1 Task 4): before any listener
//! opens, a Loams process reads the stack record and the ledger and refuses
//! to start (exit 78, `EX_CONFIG`) on data it cannot read.
//!
//! - a stored format outside this binary's readable range:
//!   `format_unreadable`;
//! - a migration left `running` or `failed`, or a finished run whose stack
//!   record was not written yet, or migrations this release needs that have
//!   not run: `migration_incomplete`;
//! - a store with data but no stack record that is not a recognised
//!   preview.2 stack: `stack_unrecognised`.
//!
//! An empty store is a new stack: the caller stamps it with
//! [`stamp_new_stack`] and serves.

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use loams_common::meta::{Consistency, MetaStore, SYSTEM_NAMESPACE};

use crate::formats::REGISTRY;
use crate::ledger::{self, LEASE_TTL, Ledger, LedgerState, MigrationLease};
use crate::migration::Migration;
use crate::path::UpgradePath;
use crate::stack::StackRecord;

/// The exit code of a refused start (`EX_CONFIG`).
pub const EXIT_REFUSED: u8 = 78;

/// What the gate decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupVerdict {
    /// The data is readable: serve.
    Ready,
    /// The store is empty: stamp it at this release, then serve.
    NewStack,
    /// Do not serve: log `code` and `detail`, exit [`EXIT_REFUSED`].
    Refuse { code: &'static str, detail: String },
}

fn refuse(code: &'static str, detail: impl Into<String>) -> StartupVerdict {
    StartupVerdict::Refuse {
        code,
        detail: detail.into(),
    }
}

/// The gate for a process of `release` over `meta`, with this binary's
/// compiled migrations and release train. `data_dir` is the process's data
/// directory (UP1 Task 6 checks its stamped stores there).
pub async fn check_startup(
    meta: Arc<dyn MetaStore>,
    data_dir: &Path,
    release: &str,
) -> Result<StartupVerdict, ledger::Error> {
    check_startup_with(
        meta,
        data_dir,
        release,
        crate::migrations::MIGRATIONS,
        UpgradePath::compiled(),
    )
    .await
}

/// [`check_startup`] with explicit migrations and train (tests).
pub async fn check_startup_with(
    meta: Arc<dyn MetaStore>,
    _data_dir: &Path,
    release: &str,
    migrations: &[&dyn Migration],
    path: &UpgradePath,
) -> Result<StartupVerdict, ledger::Error> {
    let ledger = Ledger::open_existing(meta.clone()).await?;
    let (stack, entries) = match &ledger {
        Some(ledger) => (ledger.stack().await?, ledger.entries().await?),
        None => (None, Vec::new()),
    };
    if let Some(entry) = entries
        .iter()
        .find(|e| matches!(e.state, LedgerState::Running | LedgerState::Failed))
    {
        return Ok(refuse(
            "migration_incomplete",
            format!(
                "migration {} is {}; run `loams migrate run` (or `loams upgrade`) to finish it, \
                 or restore the snapshot",
                entry.id,
                if entry.state == LedgerState::Running {
                    "running"
                } else {
                    "failed"
                }
            ),
        ));
    }
    let Some(stack) = stack else {
        return Ok(if has_user_data(&*meta).await? {
            refuse(
                "stack_unrecognised",
                "the store has data but no stack record; run `loams migrate run \
                 --declared-release <its release>` to adopt a preview.2 stack, or start a fresh \
                 stack",
            )
        } else {
            StartupVerdict::NewStack
        });
    };
    if let Err(unreadable) = stack.stored().readable_by(REGISTRY) {
        let hint = if unreadable.is_newer() {
            "the data was written by a newer release: run this release's successor, or restore \
             the snapshot taken before the upgrade"
        } else {
            "the data is older than this release reads: run `loams upgrade` one step at a time"
        };
        return Ok(refuse("format_unreadable", format!("{unreadable}; {hint}")));
    }
    if stack.release != release {
        let pending: Vec<&str> = migrations
            .iter()
            .filter(|m| {
                !entries
                    .iter()
                    .any(|e| e.id == m.id() && e.state == LedgerState::Done)
            })
            .filter(|m| !path.introduced_at_or_before(m.id(), &stack.release))
            .map(|m| m.id())
            .collect();
        let finished_unstamped = !entries.is_empty()
            && entries.iter().any(|e| e.release_to == release)
            && entries
                .iter()
                .filter(|e| e.release_to == release)
                .all(|e| e.state == LedgerState::Done);
        if !pending.is_empty() || finished_unstamped {
            return Ok(refuse(
                "migration_incomplete",
                format!(
                    "the data is at {} and {release} needs `loams migrate run` first{}",
                    stack.release,
                    if pending.is_empty() {
                        String::new()
                    } else {
                        format!(" (pending: {})", pending.join(", "))
                    }
                ),
            ));
        }
    }
    Ok(StartupVerdict::Ready)
}

/// Whether the metastore holds anything a user wrote: a namespace other than
/// `_system`.
pub async fn has_user_data(meta: &dyn MetaStore) -> Result<bool, ledger::Error> {
    Ok(meta
        .namespaces(Consistency::Linearizable)
        .await?
        .iter()
        .any(|ns| ns.name != SYSTEM_NAMESPACE))
}

/// Stamps a new stack at `release` (the [`StartupVerdict::NewStack`] path).
/// A concurrent stamp by another process is fine: both write the same
/// release.
pub async fn stamp_new_stack(
    meta: Arc<dyn MetaStore>,
    release: &str,
    holder: &str,
    image_digest: Option<String>,
) -> Result<(), ledger::Error> {
    let ledger = Ledger::open(meta.clone()).await?;
    let lease = MigrationLease::acquire(meta, holder, LEASE_TTL).await?;
    let result = async {
        if ledger.stack().await?.is_none() {
            let record = StackRecord::current(release, REGISTRY, SystemTime::now(), image_digest);
            ledger.set_stack(&lease, &record).await?;
        }
        Ok(())
    }
    .await;
    let _ = lease.release().await;
    result
}
