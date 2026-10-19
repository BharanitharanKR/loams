//! The startup gate (UP1 Task 4).

mod support;

use std::collections::BTreeMap;
use std::time::SystemTime;

use loams_migrate::formats::Store;
use loams_migrate::ledger::LEASE_TTL;
use loams_migrate::startup::{StartupVerdict, check_startup_with, stamp_new_stack};
use loams_migrate::{Class, LedgerEntry, LedgerState, Migration, MigrationLease, StackRecord};
use support::{FileMigration, Stack, path};

const P2: &str = "0.1.0-preview.2";
const P3: &str = "0.1.0-preview.3";

async fn gate(stack: &Stack, release: &str, migrations: &[&dyn Migration]) -> StartupVerdict {
    check_startup_with(
        stack.meta.clone(),
        stack.data.path(),
        release,
        migrations,
        &path("prerelease"),
    )
    .await
    .expect("gate")
}

fn code(verdict: StartupVerdict) -> &'static str {
    match verdict {
        StartupVerdict::Refuse { code, .. } => code,
        other => panic!("not refused: {other:?}"),
    }
}

/// Writes a stack record at `release` with `formats` as stored.
async fn stamp_with(stack: &Stack, release: &str, formats: &[(&str, u16)]) {
    let ledger = stack.ledger().await;
    let lease = MigrationLease::acquire(stack.meta.clone(), "test", LEASE_TTL)
        .await
        .expect("lease");
    let record = StackRecord {
        release: release.to_string(),
        formats: formats
            .iter()
            .map(|(id, v)| ((*id).to_string(), *v))
            .collect::<BTreeMap<_, _>>(),
        migrated_at: SystemTime::now(),
        image_digest: None,
    };
    ledger.set_stack(&lease, &record).await.expect("stamp");
    lease.release().await.expect("release");
}

async fn entry_in(stack: &Stack, state: LedgerState) {
    let ledger = stack.ledger().await;
    let lease = MigrationLease::acquire(stack.meta.clone(), "test", LEASE_TTL)
        .await
        .expect("lease");
    let entry = LedgerEntry {
        seq: 1,
        id: "0001-a".to_string(),
        release_from: P2.to_string(),
        release_to: P3.to_string(),
        store: Store::Host,
        class: Class::Reversible,
        state,
        cursor: None,
        attempts: 1,
        started_at: None,
        finished_at: None,
        error: None,
        image_digest: None,
        code_digest: [0; 32],
    };
    ledger.append(&lease, &entry).await.expect("append");
    lease.release().await.expect("release");
}

#[tokio::test]
async fn empty_store_is_new_stack_and_stamped() {
    let stack = Stack::new().await;
    assert_eq!(gate(&stack, P3, &[]).await, StartupVerdict::NewStack);
    stamp_new_stack(stack.meta.clone(), P3, "engine-test", None)
        .await
        .expect("stamp");
    let record = stack
        .ledger()
        .await
        .stack()
        .await
        .expect("stack")
        .expect("stamped");
    assert_eq!(record.release, P3);
    assert_eq!(gate(&stack, P3, &[]).await, StartupVerdict::Ready);
    // `_system` alone (an auth directory on an empty stack) is still empty.
    let other = Stack::new().await;
    other
        .meta
        .create_namespace("_system")
        .await
        .expect("create");
    assert_eq!(gate(&other, P3, &[]).await, StartupVerdict::NewStack);
}

#[tokio::test]
async fn current_stack_is_ready() {
    let stack = Stack::new().await;
    stack.stamp(P3).await;
    assert_eq!(gate(&stack, P3, &[]).await, StartupVerdict::Ready);
    // A release's own migrations are not pending on a stack stamped at it.
    let a = FileMigration::new("0001-a", 1);
    assert_eq!(gate(&stack, P3, &[&a]).await, StartupVerdict::Ready);
}

#[tokio::test]
async fn newer_format_refused_at_startup() {
    let stack = Stack::new().await;
    stamp_with(&stack, P3, &[("meta.raft_snapshot", 9)]).await;
    let verdict = gate(&stack, P3, &[]).await;
    let StartupVerdict::Refuse {
        code: refused,
        detail,
    } = verdict
    else {
        panic!("not refused");
    };
    assert_eq!(refused, "format_unreadable");
    assert!(detail.contains("meta.raft_snapshot version 9"), "{detail}");
    assert!(detail.contains("newer release"), "{detail}");
    // A format this binary has never heard of is newer data too.
    let other = Stack::new().await;
    stamp_with(&other, P3, &[("live.from_the_future", 1)]).await;
    assert_eq!(code(gate(&other, P3, &[]).await), "format_unreadable");
}

#[tokio::test]
async fn older_than_n_minus_one_refused_at_startup() {
    let stack = Stack::new().await;
    stamp_with(&stack, P3, &[("meta.raft_snapshot", 4)]).await;
    let StartupVerdict::Refuse {
        code: refused,
        detail,
    } = gate(&stack, P3, &[]).await
    else {
        panic!("not refused");
    };
    assert_eq!(refused, "format_unreadable");
    assert!(detail.contains("one step at a time"), "{detail}");
}

#[tokio::test]
async fn failed_migration_blocks_startup() {
    let stack = Stack::new().await;
    stack.stamp(P2).await;
    entry_in(&stack, LedgerState::Failed).await;
    assert_eq!(code(gate(&stack, P3, &[]).await), "migration_incomplete");
    // The previous release is blocked as well: the data is half migrated.
    assert_eq!(code(gate(&stack, P2, &[]).await), "migration_incomplete");
}

#[tokio::test]
async fn running_migration_blocks_startup() {
    let stack = Stack::new().await;
    stack.stamp(P2).await;
    entry_in(&stack, LedgerState::Running).await;
    let StartupVerdict::Refuse {
        code: refused,
        detail,
    } = gate(&stack, P3, &[]).await
    else {
        panic!("not refused");
    };
    assert_eq!(refused, "migration_incomplete");
    assert!(detail.contains("0001-a is running"), "{detail}");
}

#[tokio::test]
async fn pending_migrations_block_the_new_release() {
    let stack = Stack::new().await;
    stack.stamp(P2).await;
    let a = FileMigration::new("0001-a", 1);
    // preview.3 with a migration that has not run: `migrate` first.
    assert_eq!(code(gate(&stack, P3, &[&a]).await), "migration_incomplete");
    // preview.2 itself still serves its own data.
    assert_eq!(gate(&stack, P2, &[&a]).await, StartupVerdict::Ready);
    // A finished run whose stack record was not written yet.
    entry_in(&stack, LedgerState::Done).await;
    assert_eq!(code(gate(&stack, P3, &[]).await), "migration_incomplete");
}

#[tokio::test]
async fn unstamped_nonempty_store_refused_unless_preview2_fingerprint() {
    let stack = Stack::new().await;
    stack.meta.create_namespace("app").await.expect("create");
    let StartupVerdict::Refuse {
        code: refused,
        detail,
    } = gate(&stack, P3, &[]).await
    else {
        panic!("not refused");
    };
    assert_eq!(refused, "stack_unrecognised");
    assert!(detail.contains("--declared-release"), "{detail}");
}
