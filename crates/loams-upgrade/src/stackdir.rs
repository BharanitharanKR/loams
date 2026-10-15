//! The compose stack an upgrade drives (design §54 §10.1): a directory with
//! `compose.yaml` whose project is `loams`, and its `.env`. Found from
//! `--dir`, else `LOAMS_STACK_DIR`, else the current directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::errors::{Result, UpgradeError};

/// The compose project name of a Loams stack.
pub const PROJECT: &str = "loams";

/// A Loams compose stack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StackDir {
    pub dir: PathBuf,
    /// `.env`'s settings (values are never printed).
    pub env: BTreeMap<String, String>,
}

/// Whether `dir` holds a Loams stack: a `compose.yaml` whose top-level
/// `name:` is `loams`.
pub fn is_loams_stack(dir: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(dir.join("compose.yaml")) else {
        return false;
    };
    text.lines().any(|line| {
        let line = line.trim_end();
        line.strip_prefix("name:")
            .is_some_and(|name| name.trim().trim_matches(['"', '\'']) == PROJECT)
    })
}

/// `.env` as name → value (comments and blank lines skipped, quotes kept).
pub fn read_env(path: &Path) -> BTreeMap<String, String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

impl StackDir {
    /// The stack: `flag` (`--dir`), else `env_dir` (`LOAMS_STACK_DIR`), else
    /// `cwd` when it holds one. `stack_not_found` otherwise.
    pub fn discover(flag: Option<&Path>, env_dir: Option<&Path>, cwd: &Path) -> Result<Self> {
        let (dir, source) = match (flag, env_dir) {
            (Some(dir), _) => (dir.to_path_buf(), "--dir"),
            (None, Some(dir)) => (dir.to_path_buf(), "LOAMS_STACK_DIR"),
            (None, None) => (cwd.to_path_buf(), "the current directory"),
        };
        if !is_loams_stack(&dir) {
            return Err(UpgradeError::new(
                "stack_not_found",
                format!(
                    "{} ({source}) has no compose.yaml of project `{PROJECT}`; pass --dir with the \
                     stack's directory (deploy/release in the bundle)",
                    dir.display()
                ),
            ));
        }
        let env = read_env(&dir.join(".env"));
        Ok(StackDir { dir, env })
    }

    /// The release the stack runs: `.env`'s `LOAMS_VERSION`, else the
    /// compose file's default.
    pub fn release(&self) -> Option<String> {
        if let Some(v) = self.env.get("LOAMS_VERSION").filter(|v| !v.is_empty()) {
            return Some(v.clone());
        }
        let text = std::fs::read_to_string(self.dir.join("compose.yaml")).ok()?;
        let start = text.find("${LOAMS_VERSION:-")? + "${LOAMS_VERSION:-".len();
        let end = text[start..].find('}')?;
        Some(text[start..start + end].to_string())
    }

    /// The snapshot directory on the host, when `.env` names one
    /// (`LOAMS_UPGRADE_SNAPSHOT_DIR`); else the `upgrade-snapshots` volume.
    pub fn snapshot_host_dir(&self) -> Option<PathBuf> {
        self.env
            .get("LOAMS_UPGRADE_SNAPSHOT_DIR")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    }
}
