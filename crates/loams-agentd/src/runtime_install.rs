//! `loams-agentd install-runtime --from <dir> --to <dir>` (design §50 §5.2,
//! plan DD1 ruling 5).
//!
//! Both binaries run from a per-user, versioned directory, never from the
//! app bundle. Loams Desktop runs its bundled `resources/bin/loams-agentd`
//! with `--from resources/bin --to <runtime>`, and this module:
//! 1. reads `VERSION` (a semantic version, so it can only name a directory
//!    inside `<to>`) and `SHA256SUMS`;
//! 2. copies `loams-agentd` and `loams` (`.exe` on Windows) into a staging
//!    directory and checks each copy's SHA-256 against `SHA256SUMS`;
//! 3. renames the staging directory to `<to>/<VERSION>/` (a version already
//!    there whose binaries match is kept as it is);
//! 4. switches `current` atomically: a symlink renamed over the old one, or
//!    `current.txt` naming the version on Windows;
//! 5. removes older versions, keeping the newest two: the one `current`
//!    names and the newest other one.
//!
//! A failure before step 4 leaves `current` where it was.

use std::io::{self, Read};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The binaries the runtime holds, without the platform's executable suffix.
pub const BINARIES: [&str; 2] = ["loams-agentd", "loams"];
/// The version that `current` names on Windows, where it is a file.
pub const CURRENT_TXT: &str = "current.txt";
/// The symlink to the current version elsewhere.
pub const CURRENT: &str = "current";
/// How many versions are kept, `current` included.
pub const KEEP: usize = 2;

/// Why the runtime was not installed.
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{path} is not a version: {text:?}")]
    BadVersion { path: PathBuf, text: String },
    #[error("{file} is not listed in {sums}")]
    Unlisted { file: String, sums: PathBuf },
    #[error("the SHA-256 of {file} is {actual}, but SHA256SUMS lists {expected}")]
    Checksum {
        file: String,
        expected: String,
        actual: String,
    },
}

/// Map an I/O error to [`InstallError::Io`] on `path`.
fn io_error(path: &Path) -> impl FnOnce(io::Error) -> InstallError + '_ {
    move |source| InstallError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// What [`install`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installed {
    pub version: String,
    pub dir: PathBuf,
    /// Versions removed to keep [`KEEP`].
    pub removed: Vec<String>,
    /// Versions that could not be removed (on Windows, a running binary).
    pub kept_in_use: Vec<String>,
}

/// The file name of `binary` on this platform.
pub fn binary_name(binary: &str) -> String {
    format!("{binary}{}", std::env::consts::EXE_SUFFIX)
}

/// Install the bundle in `from` into the runtime directory `to`.
pub fn install(from: &Path, to: &Path) -> Result<Installed, InstallError> {
    let version = read_version(&from.join("VERSION"))?;
    let sums_path = from.join("SHA256SUMS");
    let sums = std::fs::read_to_string(&sums_path).map_err(io_error(&sums_path))?;
    let expected = BINARIES
        .iter()
        .map(|binary| {
            let file = binary_name(binary);
            let sum = listed_sum(&sums, &file).ok_or_else(|| InstallError::Unlisted {
                file: file.clone(),
                sums: sums_path.clone(),
            })?;
            Ok((file, sum))
        })
        .collect::<Result<Vec<_>, InstallError>>()?;

    crate::owner_only::create_dir(to).map_err(io_error(to))?;
    // One installer at a time, from staging through garbage collection.
    let _lock = lock(to)?;
    let dir = to.join(&version);
    if !matches_sums(&dir, &expected) {
        let staging = to.join(format!(".{version}.{}.staging", std::process::id()));
        let staged =
            stage(from, &staging, &expected).and_then(|()| publish(&staging, &dir, &version));
        if let Err(error) = staged {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
    }
    switch_current(to, &version)?;
    let (removed, kept_in_use) = collect_garbage(to, &version);
    Ok(Installed {
        version,
        dir,
        removed,
        kept_in_use,
    })
}

/// The version `current` names in the runtime directory `runtime`, if any.
pub fn current(runtime: &Path) -> Option<String> {
    let name = if cfg!(windows) {
        std::fs::read_to_string(runtime.join(CURRENT_TXT))
            .ok()?
            .trim()
            .to_owned()
    } else {
        std::fs::read_link(runtime.join(CURRENT))
            .ok()?
            .to_str()?
            .to_owned()
    };
    semver::Version::parse(&name).ok().map(|_| name)
}

/// The daemon binary a service manager launches from `runtime`: through the
/// `current` symlink, so an upgrade needs no new unit; on Windows, which has
/// no symlink, the version `current.txt` names. `None` when the runtime is
/// not installed.
pub fn service_exe(runtime: &Path) -> Option<PathBuf> {
    let exe = if cfg!(windows) {
        runtime.join(current(runtime)?)
    } else {
        current(runtime)?;
        runtime.join(CURRENT)
    }
    .join(binary_name("loams-agentd"));
    exe.is_file().then_some(exe)
}

/// The trimmed `VERSION` file, which must be a semantic version.
fn read_version(path: &Path) -> Result<String, InstallError> {
    let text = std::fs::read_to_string(path).map_err(io_error(path))?;
    let version = text.trim();
    // A version has no separators or dots-only names, so it cannot leave `<to>`.
    match semver::Version::parse(version) {
        Ok(_) => Ok(version.to_owned()),
        Err(_) => Err(InstallError::BadVersion {
            path: path.to_path_buf(),
            text: version.to_owned(),
        }),
    }
}

/// The lowercase hex SHA-256 that `sums` (`sha256sum` output: `<hex>  <file>`,
/// or `<hex> *<file>` in binary mode) lists for `file`.
fn listed_sum(sums: &str, file: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let (sum, name) = line.trim_end_matches('\r').split_once(' ')?;
        let name = name.strip_prefix(' ').or_else(|| name.strip_prefix('*'))?;
        (name == file && sum.len() == 64 && sum.bytes().all(|b| b.is_ascii_hexdigit()))
            .then(|| sum.to_ascii_lowercase())
    })
}

/// The lowercase hex SHA-256 of the file at `path`.
fn sha256(path: &Path) -> io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Whether `dir` holds every binary with its listed SHA-256.
fn matches_sums(dir: &Path, expected: &[(String, String)]) -> bool {
    expected
        .iter()
        .all(|(file, sum)| sha256(&dir.join(file)).is_ok_and(|actual| &actual == sum))
}

/// Copy the binaries into `staging` and check each copy.
fn stage(from: &Path, staging: &Path, expected: &[(String, String)]) -> Result<(), InstallError> {
    if staging.exists() {
        std::fs::remove_dir_all(staging).map_err(io_error(staging))?;
    }
    std::fs::create_dir_all(staging).map_err(io_error(staging))?;
    for (file, sum) in expected {
        let target = staging.join(file);
        std::fs::copy(from.join(file), &target).map_err(io_error(&from.join(file)))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
                .map_err(io_error(&target))?;
        }
        std::fs::File::open(&target)
            .and_then(|copy| copy.sync_all())
            .map_err(io_error(&target))?;
        let actual = sha256(&target).map_err(io_error(&target))?;
        if &actual != sum {
            return Err(InstallError::Checksum {
                file: file.clone(),
                expected: sum.clone(),
                actual,
            });
        }
    }
    Ok(())
}

/// How long an installer waits for another to finish.
const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// `<to>/.install.lock`, waiting up to [`LOCK_WAIT`] for another installer.
fn lock(to: &Path) -> Result<crate::lock::InstanceLock, InstallError> {
    let path = to.join(".install.lock");
    let deadline = std::time::Instant::now() + LOCK_WAIT;
    loop {
        match crate::lock::InstanceLock::acquire_file(&path) {
            Ok(lock) => return Ok(lock),
            Err(crate::lock::LockError::Held { .. }) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(crate::lock::LockError::Held { holder }) => {
                return Err(InstallError::Io {
                    path,
                    source: io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("another install (pid {holder}) is still running"),
                    ),
                });
            }
            Err(crate::lock::LockError::Io(source)) => {
                return Err(InstallError::Io { path, source });
            }
        }
    }
}

/// Move the checked `staging` directory to `dir`. A `dir` already there (a
/// damaged copy, which may be the one `current` names) is moved aside first
/// and moved back if the swap fails, so `dir` is missing for two renames at
/// most and never left missing. On Windows a running binary refuses the first
/// rename, and nothing changes.
fn publish(staging: &Path, dir: &Path, version: &str) -> Result<(), InstallError> {
    if !dir.exists() {
        return std::fs::rename(staging, dir).map_err(io_error(dir));
    }
    let parent = dir.parent().unwrap_or(dir);
    let aside = parent.join(format!(".{version}.{}.replaced", std::process::id()));
    std::fs::rename(dir, &aside).map_err(io_error(dir))?;
    if let Err(source) = std::fs::rename(staging, dir) {
        let _ = std::fs::rename(&aside, dir);
        return Err(InstallError::Io {
            path: dir.to_path_buf(),
            source,
        });
    }
    // A leftover is swept by the next install.
    let _ = std::fs::remove_dir_all(&aside);
    Ok(())
}

/// Point `current` at `version` atomically.
fn switch_current(to: &Path, version: &str) -> Result<(), InstallError> {
    if cfg!(windows) {
        let path = to.join(CURRENT_TXT);
        return crate::owner_only::write(&path, format!("{version}\n").as_bytes())
            .map_err(io_error(&path));
    }
    #[cfg(unix)]
    {
        let link = to.join(CURRENT);
        let temp = to.join(format!(".{CURRENT}.{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&temp);
        std::os::unix::fs::symlink(version, &temp).map_err(io_error(&temp))?;
        // rename(2) replaces the old link in one step.
        if let Err(source) = std::fs::rename(&temp, &link) {
            let _ = std::fs::remove_file(&temp);
            return Err(InstallError::Io { path: link, source });
        }
    }
    Ok(())
}

/// Remove every version but `current` and the newest other one, and the
/// staging and replaced directories that an interrupted install left (the
/// caller holds the install lock, so no other install owns them). Returns
/// the versions removed and those that could not be (a running binary on
/// Windows).
fn collect_garbage(to: &Path, current: &str) -> (Vec<String>, Vec<String>) {
    let Ok(entries) = std::fs::read_dir(to) else {
        return (Vec::new(), Vec::new());
    };
    let mut others = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') && (name.ends_with(".staging") || name.ends_with(".replaced")) {
            let _ = std::fs::remove_dir_all(entry.path());
        } else if name != current
            && let Ok(version) = semver::Version::parse(&name)
        {
            others.push((version, name));
        }
    }
    others.sort();
    let stale = others.len().saturating_sub(KEEP - 1);
    let (mut removed, mut in_use) = (Vec::new(), Vec::new());
    for (_, name) in others.into_iter().take(stale) {
        match std::fs::remove_dir_all(to.join(&name)) {
            Ok(()) => removed.push(name),
            Err(_) => in_use.push(name),
        }
    }
    (removed, in_use)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUM: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    #[test]
    fn sums_are_read_in_text_and_binary_mode() {
        let sums = format!("{SUM}  loams\n{} *loams-agentd\r\n", SUM.to_uppercase());
        assert_eq!(listed_sum(&sums, "loams").as_deref(), Some(SUM));
        assert_eq!(listed_sum(&sums, "loams-agentd").as_deref(), Some(SUM));
        assert_eq!(listed_sum(&sums, "loams.exe"), None);
        assert_eq!(listed_sum("abc  loams\n", "loams"), None);
        assert_eq!(listed_sum(&format!("{SUM} loams\n"), "loams"), None);
    }
}
