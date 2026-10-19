//! The House data directory (S10): a new version is built in a sibling
//! directory and exchanged with the current one in one `renameat2`
//! (`RENAME_EXCHANGE`), so a reader sees the old directory or the new one,
//! never a mix. The directory's format is its `FORMAT` stamp
//! (`house.dir <version>`, absent means 1).

use std::path::{Path, PathBuf};

use crate::formats::{self, HOUSE_DIR, STAMP_FILE};
use crate::migration::MigrationError;

use super::files::{rename_over, sync_parent, write_synced};
use super::io_error;

/// See the module documentation.
#[derive(Clone, Debug)]
pub struct HouseCx {
    dir: PathBuf,
}

impl HouseCx {
    pub fn new(dir: PathBuf) -> Self {
        HouseCx { dir }
    }

    /// The current data directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The sibling a new version is built in.
    pub fn sibling(&self) -> PathBuf {
        let mut name = self
            .dir
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push(".migrate-new");
        self.dir.with_file_name(name)
    }

    /// The directory's stamped version (absent: 1).
    pub fn stamp(&self) -> Result<u16, MigrationError> {
        formats::read_dir_stamp(&self.dir, HOUSE_DIR).map_err(|e| io_error("FORMAT", e))
    }

    /// Stamps `dir` (the sibling, before the switch) at `version`, through a
    /// temporary file and a rename.
    pub fn write_stamp(dir: &Path, version: u16) -> Result<(), MigrationError> {
        let tmp = dir.join(format!("{STAMP_FILE}.tmp"));
        write_synced(
            &tmp,
            format!("{} {version}\n", HOUSE_DIR.as_str()).as_bytes(),
        )?;
        rename_over(&tmp, &dir.join(STAMP_FILE))
    }

    /// The switch: exchanges the sibling and the current directory
    /// atomically; the old version is left at the sibling's path for
    /// [`HouseCx::discard_sibling`] (or a rollback) to remove.
    pub fn swap_dir(&self) -> Result<(), MigrationError> {
        let sibling = self.sibling();
        exchange(&sibling, &self.dir).map_err(|e| {
            io_error(
                &format!(
                    "exchanging {} and {}",
                    sibling.display(),
                    self.dir.display()
                ),
                e,
            )
        })?;
        sync_parent(&self.dir)
    }

    /// Removes the sibling (a partial new version, or the old version after
    /// a switch).
    pub fn discard_sibling(&self) -> Result<(), MigrationError> {
        match std::fs::remove_dir_all(self.sibling()) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("discarding the sibling directory", e)),
        }
    }
}

/// Exchanges two directories atomically: renameat2(RENAME_EXCHANGE) on
/// Linux, renamex_np(RENAME_SWAP) on macOS.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        a,
        rustix::fs::CWD,
        b,
        rustix::fs::RenameFlags::EXCHANGE,
    )
    .map_err(std::io::Error::from)
}

/// Elsewhere there is no atomic exchange, so House directory migrations are
/// refused (the server runs on Linux; this keeps the binary building).
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn exchange(_a: &Path, _b: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "an atomic directory exchange needs Linux or macOS",
    ))
}
