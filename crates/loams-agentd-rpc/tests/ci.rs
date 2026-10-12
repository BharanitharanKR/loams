//! The CI fuzz job (.github/workflows/ci.yml, `agentd-fuzz`) runs every
//! cargo-fuzz target of this crate, pinned (DD1 Task 7).
#![allow(clippy::unwrap_used)]

#[test]
fn ci_fuzzes_every_target_pinned() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let ci = std::fs::read_to_string(format!("{root}/.github/workflows/ci.yml")).unwrap();
    let start = ci.find("\n  agentd-fuzz:\n").expect("agentd-fuzz job");
    let len = ci[start + 1..].find("\n  deny:\n").expect("the next job");
    let job = &ci[start..start + 1 + len];
    let manifest =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/fuzz/Cargo.toml")).unwrap();
    let targets: Vec<&str> = manifest
        .lines()
        .filter_map(|l| l.strip_prefix("name = \""))
        .filter_map(|l| l.strip_suffix('"'))
        .filter(|n| *n != "loams-agentd-rpc-fuzz")
        .collect();
    assert_eq!(targets, ["frame", "handshake"]);
    for t in targets {
        assert!(job.contains(t), "CI does not fuzz {t}");
    }
    for needle in [
        "cargo-fuzz@0.13.2",
        "--locked",
        "timeout-minutes:",
        "fuzz/artifacts",
        "nightly-2026-09-24",
        "-max_total_time=60",
    ] {
        assert!(job.contains(needle), "agentd-fuzz lacks {needle}");
    }
    assert!(
        ci.contains("      - agentd-fuzz\n"),
        "agentd-fuzz is not required"
    );
}
