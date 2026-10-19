//! `loams upgrade`'s error and exit codes, and the client's dependency
//! boundary (UP1 Task 12).

use std::collections::BTreeSet;
use std::process::Command;

use loams_upgrade::errors::{CODES, UpgradeError, exit_for};

/// Design §54 §10.1's table, verbatim.
#[test]
fn error_code_and_exit_code_table() {
    let table: &[(u8, &[&str])] = &[
        (3, &["confirmation_required"]),
        (
            4,
            &["stack_not_found", "release_not_found", "snapshot_not_found"],
        ),
        (
            5,
            &[
                "upgrade_step_not_adjacent",
                "component_downgrade",
                "insufficient_space",
                "stack_unhealthy",
                "migration_incomplete",
                "upgrade_in_progress",
                "snapshot_incomplete",
            ],
        ),
        (
            6,
            &[
                "rollback_needs_snapshot",
                "stack_unrecognised",
                "platform_unsupported",
            ],
        ),
        (7, &["timeout", "network", "registry"]),
        (
            9,
            &["checksum_mismatch", "signature_invalid", "manifest_invalid"],
        ),
        (10, &["upgrade_failed_rolled_back"]),
        (11, &["rollback_failed"]),
    ];
    let mut listed = BTreeSet::new();
    for (exit, codes) in table {
        for code in *codes {
            assert_eq!(exit_for(code), *exit, "{code}");
            listed.insert(*code);
        }
    }
    // Nothing beyond the table but internal (1) and usage (2).
    for (code, exit) in CODES {
        assert!(
            listed.contains(code)
                || (*code == "internal" && *exit == 1)
                || (*code == "usage" && *exit == 2),
            "{code} is not in §54 §10.1"
        );
    }
    assert_eq!(exit_for("not_a_code"), 1);
    let err = UpgradeError::new("component_downgrade", "tikv v8.5.9 -> v8.5.8");
    assert_eq!(err.exit_code(), 5);
    assert_eq!(
        err.to_json(),
        serde_json::json!({"error": "component_downgrade", "message": "tikv v8.5.9 -> v8.5.8"})
    );
}

/// Global Constraints: the client links no storage crate.
#[test]
fn upgrade_client_has_no_storage_dependencies() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .args([
            "tree",
            "-p",
            "loams-upgrade",
            "-e",
            "normal",
            "--prefix",
            "none",
            "--locked",
            "--offline",
        ])
        .output()
        .expect("cargo tree runs");
    assert!(
        out.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let tree = String::from_utf8(out.stdout).expect("utf-8");
    let packages: BTreeSet<&str> = tree
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    assert!(packages.contains("loams-upgrade"), "{packages:?}");
    for banned in [
        "loams-migrate",
        "loams-store",
        "loams-collection",
        "loams-graph",
        "loams-durable",
        "loams-pg-control",
        "loams-tikv",
        "rusqlite",
    ] {
        assert!(!packages.contains(banned), "{banned} is linked");
    }
    for prefix in ["loams-meta", "loams-live", "grafeo"] {
        assert!(
            !packages.iter().any(|p| p.starts_with(prefix)),
            "a {prefix}* crate is linked"
        );
    }
}
