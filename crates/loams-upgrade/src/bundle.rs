//! The target release's bundle (design §54 §11.2): its compose files, its
//! `upgrade-path.toml` and its images. A downloaded bundle is checked
//! against the release's `SHA256SUMS`; `--bundle DIR` takes a local one,
//! unverified (CI, and a git checkout at the target's tag, Q827), and says
//! so. The signed release manifest (CLI2, D292) replaces the checksum check
//! once it exists; until then only previews are upgraded, from the
//! checksummed bundle.

use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

use crate::errors::{Result, UpgradeError};
use crate::path::UpgradePath;

/// Where releases are downloaded from.
pub const RELEASES: &str = "https://github.com/ostrium-labs/loams/releases/download";

/// A target bundle on disk.
#[derive(Clone, Debug)]
pub struct Bundle {
    /// The directory with the target's `compose.yaml` (the stack directory's
    /// new contents).
    pub release_dir: PathBuf,
    pub path: UpgradePath,
    /// Whether it was checked against the release's checksums.
    pub verified: bool,
}

fn read_path(candidates: &[PathBuf]) -> Result<UpgradePath> {
    for c in candidates {
        if let Ok(text) = std::fs::read_to_string(c) {
            return UpgradePath::parse(&text);
        }
    }
    Err(UpgradeError::new(
        "manifest_invalid",
        "the bundle has no upgrade-path.toml",
    ))
}

impl Bundle {
    /// A local bundle: `dir` is its `release` directory (or a checkout's
    /// `deploy/release`). Unverified.
    pub fn local(dir: &Path) -> Result<Self> {
        if !dir.join("compose.yaml").exists() {
            return Err(UpgradeError::new(
                "release_not_found",
                format!("{} has no compose.yaml", dir.display()),
            ));
        }
        let path = read_path(&[
            dir.join("upgrade-path.toml"),
            dir.join("../upgrade-path.toml"),
            dir.join("../../release/upgrade-path.toml"),
        ])?;
        Ok(Bundle {
            release_dir: dir.to_path_buf(),
            path,
            verified: false,
        })
    }

    /// Downloads `loams-compose-<version>.tar.gz` and `SHA256SUMS` into
    /// `work`, checks the archive's digest and unpacks it.
    pub fn download(version: &str, work: &Path) -> Result<Self> {
        std::fs::create_dir_all(work)
            .map_err(|e| UpgradeError::new("internal", format!("{}: {e}", work.display())))?;
        let name = format!("loams-compose-{version}.tar.gz");
        let base = format!("{RELEASES}/v{version}");
        for file in [name.as_str(), "SHA256SUMS"] {
            let out = Command::new("curl")
                .args(["-fsSL", "--retry", "3", "-o"])
                .arg(work.join(file))
                .arg(format!("{base}/{file}"))
                .output()
                .map_err(|e| UpgradeError::new("network", format!("curl: {e}")))?;
            if !out.status.success() {
                return Err(UpgradeError::new(
                    if file == "SHA256SUMS" {
                        "network"
                    } else {
                        "release_not_found"
                    },
                    format!(
                        "{base}/{file}: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ),
                ));
            }
        }
        verify_sum(&work.join(&name), &work.join("SHA256SUMS"))?;
        let out = Command::new("tar")
            .arg("-xzf")
            .arg(work.join(&name))
            .arg("-C")
            .arg(work)
            .output()
            .map_err(|e| UpgradeError::new("internal", format!("tar: {e}")))?;
        if !out.status.success() {
            return Err(UpgradeError::new(
                "manifest_invalid",
                "the bundle does not unpack",
            ));
        }
        let root = work.join(format!("loams-compose-{version}"));
        let path = read_path(&[root.join("upgrade-path.toml")])?;
        Ok(Bundle {
            release_dir: root.join("release"),
            path,
            verified: true,
        })
    }
}

/// Checks `file`'s sha256 against its line in `sums`.
pub fn verify_sum(file: &Path, sums: &Path) -> Result<()> {
    let name = file
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let text = std::fs::read_to_string(sums)
        .map_err(|e| UpgradeError::new("manifest_invalid", format!("SHA256SUMS: {e}")))?;
    let expected = text
        .lines()
        .filter_map(|l| l.split_once(char::is_whitespace))
        .find(|(_, n)| n.trim().trim_start_matches('*') == name)
        .map(|(sum, _)| sum.to_string())
        .ok_or_else(|| {
            UpgradeError::new("manifest_invalid", format!("SHA256SUMS has no {name}"))
        })?;
    let bytes = std::fs::read(file)
        .map_err(|e| UpgradeError::new("internal", format!("{}: {e}", file.display())))?;
    let actual: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if actual == expected {
        Ok(())
    } else {
        Err(UpgradeError::new(
            "checksum_mismatch",
            format!("{name} is {actual}, SHA256SUMS says {expected}"),
        ))
    }
}

/// The `loams` image of `version` in the stack's registry.
pub fn loams_image(env: &std::collections::BTreeMap<String, String>, version: &str) -> String {
    let registry = env
        .get("LOAMS_REGISTRY")
        .filter(|r| !r.is_empty())
        .map_or("ghcr.io/ostrium-labs", String::as_str);
    format!("{registry}/loams:{version}")
}
