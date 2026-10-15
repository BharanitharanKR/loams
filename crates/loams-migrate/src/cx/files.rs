//! File helpers the directory and file contexts share: write beside, then
//! switch with one `rename`.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use super::io_error;
use crate::migration::MigrationError;

/// The sibling a new version of `target` is written to before the switch.
pub(crate) fn beside(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".migrate-new");
    target.with_file_name(name)
}

/// Writes `bytes` to `path` durably (file and directory synced), replacing
/// whatever partial attempt was there.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), MigrationError> {
    let mut file = fs::File::create(path).map_err(|e| io_error(&path.display().to_string(), e))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| io_error(&path.display().to_string(), e))?;
    sync_parent(path)
}

/// Replaces `target` with `new` in one `rename`, then syncs the directory.
pub(crate) fn rename_over(new: &Path, target: &Path) -> Result<(), MigrationError> {
    fs::rename(new, target).map_err(|e| {
        io_error(
            &format!("renaming {} to {}", new.display(), target.display()),
            e,
        )
    })?;
    sync_parent(target)
}

pub(crate) fn sync_parent(path: &Path) -> Result<(), MigrationError> {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    if let Some(dir) = dir {
        fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io_error(&dir.display().to_string(), e))?;
    }
    Ok(())
}
