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
    let first: Value = serde_json::from_str(out.trim()).expect("json");
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
    let status: Value = serde_json::from_str(out.trim()).expect("json");
    assert_eq!(status["stack"]["release"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["ledger"], serde_json::json!([]));
}

#[test]
fn migrate_plan_json_matches_schema() {
    let dir = TempDir::new().expect("dir");
    let (code, out, err) = migrate(&["plan"], dir.path());
    assert_eq!(code, Some(0), "{out}{err}");
    let plan: Value = serde_json::from_str(out.trim()).expect("json");
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
