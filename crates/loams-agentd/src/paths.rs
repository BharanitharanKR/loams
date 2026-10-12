//! The daemon's directory (design §50 §5.2, plan DD1 "Shared contracts").
//!
//! Loams Desktop keeps it at `<userData>/agentd`, beside `config.toml`:
//!
//! ```text
//! config.toml   agentd.json   agentd.token   agentd.lock
//! store/        the sessions engine's data (DocsStore, journals)
//! adapters/     managed installs of the harnesses' ACP adapters
//! runtime/      the installed binaries: <version>/, current (DD1 Task 8)
//! logs/         the daemon's own log (agentd.log)
//! ```
//!
//! The directory is owner-only (0700, or an owner-and-SYSTEM DACL on Windows).

use std::io;
use std::path::{Path, PathBuf};

/// Where the daemon's files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    dir: PathBuf,
}

impl Paths {
    /// `<data_dir>/agentd`, created owner-only.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        Self::create(data_dir.join("agentd"))
    }

    /// The directory holding `config`, created owner-only: the daemon keeps
    /// its files beside its config (`<userData>/agentd/config.toml`).
    pub fn for_config(config: &Path) -> io::Result<Self> {
        Self::create(config_dir(config)?)
    }

    /// The daemon directory `dir`, as it is: nothing is created or changed.
    pub fn at(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    /// [`Self::at`] the directory holding `config`.
    pub fn of_config(config: &Path) -> io::Result<Self> {
        Ok(Self::at(&config_dir(config)?))
    }

    fn create(dir: PathBuf) -> io::Result<Self> {
        crate::owner_only::create_dir(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn config(&self) -> PathBuf {
        self.dir.join("config.toml")
    }

    /// `agentd.json`, the discovery file.
    pub fn discovery(&self) -> PathBuf {
        self.dir.join("agentd.json")
    }

    /// `agentd.token`, the RPC token.
    pub fn token(&self) -> PathBuf {
        self.dir.join("agentd.token")
    }

    /// `agentd.lock`, the single-instance lock.
    pub fn lock(&self) -> PathBuf {
        self.dir.join("agentd.lock")
    }

    /// `store/`, the sessions engine's data directory.
    pub fn store(&self) -> PathBuf {
        self.dir.join("store")
    }

    /// `adapters/`, where harness ACP adapters are installed (DD1 Task 7;
    /// the fork used `~/.loams-desktop/adapters`).
    pub fn adapters(&self) -> PathBuf {
        self.dir.join("adapters")
    }

    /// `runtime/`, the installed binaries (design §50 §5.2), unless
    /// `[paths] runtime` puts them elsewhere (Windows, T0-7).
    pub fn runtime(&self) -> PathBuf {
        self.dir.join("runtime")
    }

    pub fn logs(&self) -> PathBuf {
        self.dir.join("logs")
    }
}

/// The absolute directory of `config`.
fn config_dir(config: &Path) -> io::Result<PathBuf> {
    let config = std::path::absolute(config)?;
    config.parent().map(Path::to_path_buf).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no parent directory", config.display()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_the_agentd_directory() {
        let root = tempfile::tempdir().expect("temp dir");
        let paths = Paths::new(root.path()).expect("paths");
        assert_eq!(paths.dir(), root.path().join("agentd"));
        assert!(paths.dir().is_dir());
        assert_eq!(paths.token(), root.path().join("agentd/agentd.token"));
        assert_eq!(paths.discovery(), root.path().join("agentd/agentd.json"));
        assert_eq!(paths.lock(), root.path().join("agentd/agentd.lock"));
        assert_eq!(paths.store(), root.path().join("agentd/store"));
    }

    #[test]
    fn the_config_names_the_directory() {
        let root = tempfile::tempdir().expect("temp dir");
        let config = root.path().join("Loams Desktop/agentd/config.toml");
        let paths = Paths::for_config(&config).expect("paths");
        assert_eq!(paths.dir(), root.path().join("Loams Desktop/agentd"));
        assert!(paths.dir().is_dir());
        assert_eq!(paths.config(), config);
        assert_eq!(Paths::of_config(&config).expect("paths"), paths);
    }

    #[test]
    fn at_touches_nothing() {
        let root = tempfile::tempdir().expect("temp dir");
        let paths = Paths::at(&root.path().join("missing"));
        assert!(!paths.dir().exists());
    }
}
