//! `loams migrate` (UP1 Task 7): the one-shot service's command, on a local
//! data directory with the embedded metastore.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

fn migrate(args: &[&str], dir: &Path) -> (Option<i32>, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_loams"))
        .arg("migrate")
        .args(args)
        .arg("--data-dir")
        .arg(dir)
        .env("RUST_LOG", "warn")
        .output()
        .expect("run loams migrate");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn migrate_run_on_current_stack_exits_quickly() {
    let dir = TempDir::new().expect("dir");
    // An empty store is a new stack: stamped at this release.
    let (code, out, err) = migrate(&["run", "--output", "json"], dir.path());
    assert_eq!(code, Some(0), "{out}{err}");
    let first: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    assert_eq!(first["to"], env!("CARGO_PKG_VERSION"));
    assert_eq!(first["stack_written"], true);
    // Again, on the current stack: nothing to do, well under 2 s.
    let started = Instant::now();
    let (code, out, err) = migrate(&["run"], dir.path());
    let took = started.elapsed();
    assert_eq!(code, Some(0), "{out}{err}");
    assert!(out.contains("nothing to do"), "{out}");
    assert!(took < Duration::from_secs(2), "took {took:?}");
    // The status shows the stamp and an empty ledger.
    let (code, out, _) = migrate(&["status"], dir.path());
    assert_eq!(code, Some(0));
    let status: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    assert_eq!(status["stack"]["release"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["ledger"], serde_json::json!([]));
}

/// Logs go to stderr, so `--output json` stays parseable however verbose
/// the log filter is (a warning on stdout once broke the JSON).
#[test]
fn migrate_logs_never_reach_stdout() {
    let dir = TempDir::new().expect("dir");
    let output = Command::new(env!("CARGO_BIN_EXE_loams"))
        .args(["migrate", "run", "--output", "json", "--data-dir"])
        .arg(dir.path())
        .env("RUST_LOG", "debug")
        .output()
        .expect("run loams migrate");
    let out = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{out}{err}");
    let result: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    assert_eq!(result["stack_written"], true);
    assert!(
        err.contains("new stack") || err.contains("DEBUG"),
        "the logs are on stderr: {err:?}"
    );
}

#[test]
fn migrate_plan_json_matches_schema() {
    let dir = TempDir::new().expect("dir");
    let (code, out, err) = migrate(&["plan"], dir.path());
    assert_eq!(code, Some(0), "{out}{err}");
    let plan: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    let keys: Vec<&str> = plan
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["from", "to", "steps", "point_of_no_return"]);
    assert_eq!(plan["from"], Value::Null, "an unstamped store");
    assert_eq!(plan["to"], env!("CARGO_PKG_VERSION"));
    for step in plan["steps"].as_array().expect("steps") {
        let keys: Vec<&str> = step
            .as_object()
            .expect("a step")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["id", "store", "class", "estimate", "state"]);
    }
}

#[test]
fn migrate_refuses_a_non_adjacent_target() {
    let dir = TempDir::new().expect("dir");
    let (code, _, err) = migrate(&["run"], dir.path());
    assert_eq!(code, Some(0), "{err}");
    let (code, _, err) = migrate(&["run", "--target-release", "9.9.9"], dir.path());
    assert_eq!(code, Some(78), "{err}");
    assert!(err.contains("release_not_found"), "{err}");
}

/// UP1 Task 8: `snapshot`, `snapshots list`, `restore` and `snapshots prune`
/// on a local stack with the embedded metastore (copied and put back while
/// no node has it open).
#[test]
fn migrate_snapshot_restore_round_trip_on_a_local_stack() {
    let dir = TempDir::new().expect("dir");
    let snapshots = TempDir::new().expect("snapshots");
    let snapshots_dir = snapshots.path().join("upgrade-snapshots");
    let (code, _, err) = migrate(&["run"], dir.path());
    assert_eq!(code, Some(0), "{err}");
    let to = snapshots_dir.to_str().expect("utf-8");
    let (code, out, err) = migrate(&["snapshot", "--to", to], dir.path());
    assert_eq!(code, Some(0), "{out}{err}");
    let taken: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    let id = taken["id"].as_str().expect("id").to_string();
    let stores: Vec<&str> = taken["components"]
        .as_array()
        .expect("components")
        .iter()
        .map(|c| c["store"].as_str().expect("store"))
        .collect();
    assert!(stores.contains(&"raft"), "{stores:?}");
    let listed = Command::new(env!("CARGO_BIN_EXE_loams"))
        .args(["migrate", "snapshots", "list", "--dir", to])
        .output()
        .expect("list");
    let listed: Value = serde_json::from_slice(&listed.stdout).expect("json");
    assert_eq!(listed[0]["id"], id.as_str());
    assert_eq!(listed[0]["complete"], true);
    let from = snapshots_dir.join(&id);
    let (code, out, err) = migrate(
        &["restore", "--from", from.to_str().expect("utf-8")],
        dir.path(),
    );
    assert_eq!(code, Some(0), "{out}{err}");
    let restored: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    assert_eq!(restored["restored"][0], "raft");
    // The stack still opens, at its release.
    let (code, out, _) = migrate(&["status"], dir.path());
    assert_eq!(code, Some(0));
    let status: Value =
        serde_json::from_str(out.trim()).unwrap_or_else(|e| panic!("json: {e}: {out:?}"));
    assert_eq!(status["stack"]["release"], env!("CARGO_PKG_VERSION"));
    let (code, _, err) = migrate(
        &["snapshots", "prune", "--dir", to, "--id", &id],
        dir.path(),
    );
    assert_eq!(code, Some(0), "{err}");
    assert!(!from.exists());
}
