//! `loams upgrade` end to end on a real compose stack (UP1 Tasks 13 and 14;
//! design §54 §14). The gate (`Upgrade: previous release → candidate`, UP1
//! Task 20) runs these with a stack started from the previous release's
//! bundle and the candidate's bundle and images built locally:
//!
//! ```text
//! LOAMS_UPGRADE_E2E_STACK=<stack dir> LOAMS_UPGRADE_E2E_BUNDLE=<candidate deploy/release> \
//! LOAMS_UPGRADE_E2E_TO=<candidate version> \
//!   cargo test -p loams --test upgrade_e2e -- --ignored --test-threads=1
//! ```

use std::process::Command;

struct E2e {
    stack: String,
    bundle: String,
    to: String,
}

fn e2e() -> E2e {
    let var = |name: &str| {
        std::env::var(name).unwrap_or_else(|_| panic!("set {name} (see the module documentation)"))
    };
    E2e {
        stack: var("LOAMS_UPGRADE_E2E_STACK"),
        bundle: var("LOAMS_UPGRADE_E2E_BUNDLE"),
        to: var("LOAMS_UPGRADE_E2E_TO"),
    }
}

fn upgrade(args: &[&str]) -> (i32, serde_json::Value) {
    let out = Command::new(env!("CARGO_BIN_EXE_loams"))
        .arg("upgrade")
        .args(args)
        .output()
        .expect("loams upgrade runs");
    let value = serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null);
    eprintln!("{}", String::from_utf8_lossy(&out.stderr));
    (out.status.code().unwrap_or(-1), value)
}

/// preview.2 → the candidate, with a snapshot first; the stack serves the
/// candidate at the end.
#[test]
#[ignore = "needs a compose stack at the previous release (the upgrade gate)"]
fn preview2_to_candidate_e2e() {
    let e = e2e();
    let (code, check) = upgrade(&[
        "check", "--dir", &e.stack, "--to", &e.to, "--bundle", &e.bundle,
    ]);
    assert_eq!(code, 0, "{check}");
    assert_eq!(check["blockers"], serde_json::json!([]), "{check}");
    let (code, report) = upgrade(&[
        "--dir", &e.stack, "--to", &e.to, "--bundle", &e.bundle, "--yes",
    ]);
    assert_eq!(code, 0, "{report}");
    let step = &report["upgraded"][0];
    assert_eq!(step["to"], e.to.as_str());
    assert_eq!(step["bundle"], "local, unverified");
    assert!(step["snapshot"].as_str().is_some_and(|s| !s.is_empty()));
}

/// After `preview2_to_candidate_e2e`: `rollback` restores the snapshot and
/// the previous release serves again. The gate's fixture checker (UP1 Task
/// 19, `run.py check --expect-new-writes absent`) then reads every fixture.
#[test]
#[ignore = "needs the upgraded stack of preview2_to_candidate_e2e (the upgrade gate)"]
fn rollback_e2e_restores_fixtures() {
    let e = e2e();
    let (code, report) = upgrade(&["rollback", "--dir", &e.stack, "--yes"]);
    assert_eq!(code, 0, "{report}");
    assert_eq!(report["from"], e.to.as_str());
    assert!(
        report["lost_writes"]
            .as_str()
            .is_some_and(|w| w.contains("will be lost"))
    );
    let (code, status) = upgrade(&["status", "--dir", &e.stack]);
    assert_eq!(code, 0, "{status}");
    assert_eq!(status["release"], report["to"]);
}
