//! The host history of upgrades (design §54 §8):
//! `<stack>/.loams-upgrade/history.jsonl`, one event per line. It survives a
//! rollback (the metastore's ledger does not), and it is how a rerun finds
//! an attempt a killed CLI left (`upgrade_in_progress`).

use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::errors::{Result, UpgradeError};

/// The directory, relative to the stack directory.
pub const DIR: &str = ".loams-upgrade";
/// The file in it.
pub const FILE: &str = "history.jsonl";

/// What happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    Started,
    /// A step finished.
    Step,
    Done,
    /// Stopped with the door closed (`--no-auto-rollback`, or a rollback
    /// that failed): a rerun resumes, or `rollback` undoes it.
    Failed,
    RolledBack,
    RollbackFailed,
}

/// One line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// The attempt (one per step of a chain).
    pub attempt: String,
    pub from: String,
    pub to: String,
    pub event: Event,
    /// The step that finished (`Step`) or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
    /// How TiKV was copied (`br` or `cold`) and at which version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tikv: Option<(String, String)>,
    /// The error code of a failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Unix seconds.
    pub at: u64,
}

/// The stack's history.
#[derive(Clone, Debug)]
pub struct History {
    path: PathBuf,
}

/// Unix seconds now.
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// `<stack>/.loams-upgrade`, made private.
pub fn private_dir(stack_dir: &Path) -> Result<PathBuf> {
    let dir = stack_dir.join(DIR);
    std::fs::create_dir_all(&dir)
        .and_then(|()| std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)))
        .map_err(|e| UpgradeError::new("internal", format!("{}: {e}", dir.display())))?;
    Ok(dir)
}

impl History {
    pub fn of(stack_dir: &Path) -> Self {
        History {
            path: stack_dir.join(DIR).join(FILE),
        }
    }

    /// Every entry, oldest first (unreadable lines skipped).
    pub fn entries(&self) -> Vec<Entry> {
        std::fs::read_to_string(&self.path)
            .map(|text| {
                text.lines()
                    .filter_map(|l| serde_json::from_str(l).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Appends `entry` (the file is 0600).
    pub fn append(&self, entry: &Entry) -> Result<()> {
        if let Some(dir) = self.path.parent().and_then(Path::parent) {
            private_dir(dir)?;
        }
        let line = serde_json::to_string(entry)
            .map_err(|e| UpgradeError::new("internal", e.to_string()))?;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&self.path)
            .and_then(|mut f| writeln!(f, "{line}"))
            .map_err(|e| UpgradeError::new("internal", format!("{}: {e}", self.path.display())))
    }

    /// The last attempt's entries, if it has not ended (`Done`,
    /// `RolledBack`): a killed CLI, `--no-auto-rollback`, or a failed
    /// rollback left it.
    pub fn open_attempt(&self) -> Option<Vec<Entry>> {
        let entries = self.entries();
        let last = entries.last()?;
        if matches!(last.event, Event::Done | Event::RolledBack) {
            return None;
        }
        let attempt = last.attempt.clone();
        Some(
            entries
                .into_iter()
                .filter(|e| e.attempt == attempt)
                .collect(),
        )
    }

    /// The last attempt that ended `Done`, with its snapshot.
    pub fn last_done(&self) -> Option<Entry> {
        self.entries()
            .into_iter()
            .rev()
            .find(|e| e.event == Event::Done)
    }
}

/// The steps an attempt finished.
pub fn finished_steps(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter(|e| e.event == Event::Step)
        .filter_map(|e| e.step.clone())
        .collect()
}

/// The snapshot an attempt took, with its TiKV method and version.
pub fn snapshot_of(entries: &[Entry]) -> Option<(String, Option<(String, String)>)> {
    entries
        .iter()
        .rev()
        .find_map(|e| e.snapshot.clone().map(|s| (s, e.tikv.clone())))
}
