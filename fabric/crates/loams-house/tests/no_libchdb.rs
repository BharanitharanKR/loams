//! `no_libchdb_in_front` (HS1 Task 2, D761, Global Constraint "libchdb only in the
//! worker"): the front's crates do not have `loams-chdb-sys` (and so
//! `libchdb.so`) in their normal or build dependency graph.
//!
//! `loams-fabric` is checked too once it exists (HS1 Task 7 creates it). The
//! `inproc-worker` feature is the one deliberate way libchdb enters `loams-house`,
//! and the last test shows the check sees it, so the check is not vacuous.

use std::path::{Path, PathBuf};
use std::process::Command;

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the fabric workspace")
}

/// `cargo tree -p <package> -e normal,build` as package names, one per line.
fn tree(package: &str, features: &[&str]) -> Vec<String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .current_dir(workspace())
        .args(["tree", "--offline", "--locked", "-p", package])
        .args(["-e", "normal,build", "--prefix", "none", "--format", "{p}"]);
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }
    let output = command.output().expect("cargo tree runs");
    assert!(
        output.status.success(),
        "cargo tree -p {package} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

fn links_libchdb(packages: &[String]) -> bool {
    packages
        .iter()
        .any(|p| p == "loams-chdb-sys" || p == "loams-chdb" || p == "loams-house-worker")
}

#[test]
fn no_libchdb_in_front() {
    let mut front = vec!["loams-house", "loams-house-ipc"];
    if workspace().join("crates/loams-fabric/Cargo.toml").is_file() {
        front.push("loams-fabric");
    }
    for package in front {
        let packages = tree(package, &[]);
        assert!(
            packages.contains(&package.to_string()),
            "{package}: {packages:?}"
        );
        assert!(
            !links_libchdb(&packages),
            "{package} reaches libchdb (D761 puts it in the worker only): {packages:?}"
        );
    }
}

#[test]
fn inproc_worker_feature_is_where_libchdb_comes_in() {
    assert!(
        links_libchdb(&tree("loams-house", &["inproc-worker"])),
        "the check must see libchdb when it is there"
    );
}

/// iceberg 0.10 brings arrow 58 while the workspace is on arrow 59 (HS1 R0.3): no
/// crate of this workspace may depend on an arrow 58 crate itself, except
/// `loams-house-lake`, which writes data files with iceberg's writers (HS1 Task
/// 12, R12.1) and names no Arrow type in its signatures. So arrow 58 types never
/// meet arrow 59 ones in a Loams signature.
#[test]
fn arrow_58_stays_in_the_lake() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .current_dir(workspace())
        .args([
            "tree",
            "--offline",
            "--locked",
            "--workspace",
            "-e",
            "normal",
        ])
        .args(["--prefix", "depth", "--format", "{p}"])
        .output()
        .expect("cargo tree runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    // `<depth><name> v<version> …`, one package per line.
    let lines: Vec<(usize, String, String)> = text
        .lines()
        .filter_map(|line| {
            let digits = line.chars().take_while(char::is_ascii_digit).count();
            let depth = line[..digits].parse().ok()?;
            let mut parts = line[digits..].split_whitespace();
            Some((depth, parts.next()?.to_string(), parts.next()?.to_string()))
        })
        .collect();
    let mut seen_lake = false;
    for (at, (depth, name, _)) in lines.iter().enumerate() {
        if !name.starts_with("loams-") {
            continue;
        }
        seen_lake |= name == "loams-house-lake";
        let children = lines[at + 1..]
            .iter()
            .take_while(|(d, _, _)| d > depth)
            .filter(|(d, _, _)| *d == depth + 1);
        if name == "loams-house-lake" {
            continue;
        }
        for (_, child, version) in children {
            assert!(
                !(child.starts_with("arrow") || child == "parquet") || !version.starts_with("v58"),
                "{name} depends on {child} {version} itself: arrow 58 must stay inside iceberg (R0.3)"
            );
        }
    }
    assert!(seen_lake, "the check saw loams-house-lake");
}
