//! The crash matrix (UP1 Task 3, Review Focus 2): a run stopped at every
//! failpoint, then run again without it, ends exactly as a clean run does.
//! Needs `--features failpoints`. `fail`'s configuration is process-wide,
//! so this binary holds this one test only.

mod support;

use loams_migrate::{LedgerState, Migration, RunError, RunOptions, Runner};
use support::{FileMigration, Stack, path, read_dir};

const FAILPOINTS: [&str; 5] = [
    "migrate::before_apply",
    "migrate::after_apply_before_checkpoint",
    "migrate::after_checkpoint",
    "migrate::before_switch",
    "migrate::after_switch_before_done",
];

/// The data directory and the ledger's states after migrating `ids` to
/// preview.3, with `failpoint` set for the first run (and, for a failpoint
/// in a loop, `hits` times).
async fn outcome(failpoint: Option<(&str, &str)>) -> (String, Vec<(String, LedgerState)>) {
    let stack = Stack::new().await;
    stack.stamp("0.1.0-preview.2").await;
    let a = FileMigration::new("0001-a", 5);
    let b = FileMigration::new("0002-b", 3);
    a.seed(stack.data.path());
    b.seed(stack.data.path());
    let migrations: [&dyn Migration; 2] = [&a, &b];
    let train = path("prerelease");
    let runner = Runner::new(&migrations, &train);
    // One holder: a restarted process takes the lease over once it expires;
    // the same holder re-takes it at once, which keeps the test fast.
    let opts = RunOptions::new("migrate-crash");
    if let Some((name, action)) = failpoint {
        fail::cfg(name, action).expect("configure failpoint");
        let crashed = runner
            .run(&mut stack.cx(), "0.1.0-preview.3", &opts)
            .await
            .expect_err("the failpoint stops the run");
        assert!(matches!(crashed, RunError::Crashed(_)), "{name}: {crashed}");
        fail::remove(name);
    }
    runner
        .run(&mut stack.cx(), "0.1.0-preview.3", &opts)
        .await
        .expect("a run after the crash converges");
    let ledger = stack.ledger().await;
    assert_eq!(
        ledger
            .stack()
            .await
            .expect("stack")
            .expect("stamped")
            .release,
        "0.1.0-preview.3"
    );
    let states = ledger
        .entries()
        .await
        .expect("entries")
        .into_iter()
        .map(|e| (e.id, e.state))
        .collect();
    (format!("{:?}", read_dir(stack.data.path())), states)
}

#[tokio::test]
async fn crash_at_every_failpoint_converges() {
    let scenario = fail::FailScenario::setup();
    let clean = outcome(None).await;
    // Each failpoint, on the first hit and on a later one (the second
    // migration, or a later call of a loop).
    for name in FAILPOINTS {
        for action in ["return", "1*off->return"] {
            let crashed = outcome(Some((name, action))).await;
            assert_eq!(crashed, clean, "{name} ({action})");
        }
    }
    scenario.teardown();
}
