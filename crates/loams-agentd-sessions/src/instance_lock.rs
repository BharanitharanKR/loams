//! Single-instance lock — an exclusive OS lock on `{data_dir}/engine.lock`
//! held for the engine's lifetime. Two engines sharing one data dir would race the
//! SQLite snapshots DB and the append-only run journals (WAL + `busy_timeout` guard
//! individual statements, not whole-file ownership), so the second instance must
//! fail fast with a clear error instead of corrupting state.
//!
//! The lock is taken in `EngineCore::assemble_with_profile` BEFORE any store is opened
//! and before the IPC port binds. `loams-agentd` takes the same kind of lock on its
//! own `agentd.lock` ([`InstanceLock::acquire_file`]) before anything else.

use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::io::Write;
use std::path::Path;

use crate::EngineError;

/// Held lock on the data dir. Dropping it (engine shutdown / process exit)
/// releases the advisory lock; a crash releases it too (kernel-owned).
#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

/// Why [`InstanceLock::acquire_file`] failed.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// Another process holds the lock. `holder` is the pid it stamped, or
    /// `"unknown"` when it could not be read.
    #[error("the lock is held by pid {holder}")]
    Held { holder: String },
    /// The lock file could not be opened or locked (not contention).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(unix)]
impl Drop for InstanceLock {
    fn drop(&mut self) {
        use std::os::unix::io::AsRawFd;
        // A child between fork and exec can still own a duplicate descriptor.
        // Release the shared open-file-description lock explicitly instead of
        // waiting for every inherited descriptor to close.
        loop {
            // SAFETY: flock on a descriptor this lock owns; it touches no memory.
            #[allow(unsafe_code)]
            let rc = unsafe { libc::flock(self._file.as_raw_fd(), libc::LOCK_UN) };
            if rc == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}

impl InstanceLock {
    /// Acquire the engine's exclusive lock on `{data_dir}/engine.lock`,
    /// non-blocking. Errors with a descriptive message (including the holder's
    /// pid when readable) if another engine already owns this data dir.
    pub fn acquire(data_dir: &Path) -> Result<Self, EngineError> {
        Self::acquire_file(&data_dir.join("engine.lock")).map_err(|error| match error {
            LockError::Held { holder } => EngineError::Other(format!(
                "another loams-desktop engine is already running on {} (pid {holder}); \
                 stop it or use a different data dir",
                data_dir.display(),
            )),
            LockError::Io(error) => EngineError::Io(error),
        })
    }

    /// Acquire an exclusive lock on the file at `path` (created if missing),
    /// non-blocking apart from a short retry budget, and stamp this process's
    /// pid for [`Self::holder_file`].
    pub fn acquire_file(path: &Path) -> Result<Self, LockError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // Bounded EWOULDBLOCK retries: a fork→exec window in ANY process
            // that inherited the previous holder's fd (git scans, harness
            // spawns — fds are duplicated between fork and CLOEXEC-at-exec)
            // keeps the flock alive for a few milliseconds after release. A
            // real second engine holds it forever; transient artifacts clear
            // well within the budget.
            let mut retries = 40u32; // × 25ms = 1s budget
            loop {
                // SAFETY: flock on a descriptor this function owns; it touches no memory.
                #[allow(unsafe_code)]
                let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if rc == 0 {
                    break;
                }
                let errno = std::io::Error::last_os_error();
                match errno.raw_os_error() {
                    Some(libc::EINTR) => continue, // signal-interrupted: retry
                    Some(libc::EWOULDBLOCK) if retries > 0 => {
                        retries -= 1;
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                    Some(libc::EWOULDBLOCK) => {
                        let holder = std::fs::read_to_string(path).unwrap_or_default();
                        return Err(LockError::Held {
                            holder: holder_or_unknown(&holder),
                        });
                    }
                    // Anything else (ENOLCK, filesystem without flock, …) is an
                    // environment problem, not a second engine — surface it as-is.
                    _ => return Err(LockError::Io(errno)),
                }
            }
        }

        #[cfg(windows)]
        {
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(LockError::Held {
                        holder: holder_or_unknown(&windows_holder_pid(path)),
                    });
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(LockError::Io(error)),
            }
        }

        // Best-effort pid stamp for the contention error message above.
        #[cfg(unix)]
        {
            let mut file = &file;
            let _ = file.set_len(0);
            let _ = write!(file, "{}", std::process::id());
            let _ = file.flush();
        }
        #[cfg(windows)]
        {
            // Windows locks the complete byte range, so another handle cannot read the
            // locked file. Keep diagnostics in an unlocked sidecar; its contents are
            // considered only after the OS lock probe reports contention.
            let _ = std::fs::write(windows_pid_path(path), std::process::id().to_string());
        }
        Ok(Self { _file: file })
    }

    /// Best-effort liveness probe of the engine's `{data_dir}/engine.lock`;
    /// see [`Self::holder_file`].
    pub fn holder(data_dir: &Path) -> Option<String> {
        Self::holder_file(&data_dir.join("engine.lock"))
    }

    /// Best-effort liveness probe: the pid stamped by the process currently holding
    /// the lock at `path`, `None` when nothing holds it (or the platform cannot
    /// test a lock without taking it). A single non-blocking try — no retry
    /// budget — so a starting holder's transient fork-window artifacts read as
    /// "running", which is the safe direction for the callers (`status`, and
    /// `loams-agentd`'s check of its discovery file).
    pub fn holder_file(path: &Path) -> Option<String> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .ok()?;
            // SAFETY: flock on a descriptor this function owns; it touches no memory.
            #[allow(unsafe_code)]
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc == 0 {
                // We took it: nothing is running. Closing the fd releases it, but
                // unlock explicitly so the window is as small as possible.
                // SAFETY: flock on a descriptor this function owns; it touches no memory.
                #[allow(unsafe_code)]
                unsafe {
                    libc::flock(file.as_raw_fd(), libc::LOCK_UN)
                };
                return None;
            }
            let pid = std::fs::read_to_string(path).unwrap_or_default();
            Some(holder_or_unknown(&pid))
        }
        #[cfg(windows)]
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .ok()?;
            match file.try_lock() {
                Ok(()) => {
                    let _ = file.unlock();
                    None
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    Some(holder_or_unknown(&windows_holder_pid(path)))
                }
                Err(std::fs::TryLockError::Error(_)) => None,
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            None
        }
    }
}

/// The stamped pid, trimmed, or `"unknown"` when the stamp is empty.
fn holder_or_unknown(stamp: &str) -> String {
    match stamp.trim() {
        "" => "unknown".to_string(),
        pid => pid.to_string(),
    }
}

/// `<lock file>.pid`, the unlocked sidecar the Windows holder writes.
#[cfg(windows)]
fn windows_pid_path(lock: &Path) -> std::path::PathBuf {
    let mut name = lock.as_os_str().to_owned();
    name.push(".pid");
    std::path::PathBuf::from(name)
}

#[cfg(windows)]
fn windows_holder_pid(lock: &Path) -> String {
    std::fs::read_to_string(windows_pid_path(lock)).unwrap_or_default()
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn drop_releases_lock_while_a_duplicate_descriptor_remains_open() {
        let dir = tempfile::tempdir().unwrap();
        let lock = InstanceLock::acquire(dir.path()).expect("acquire");
        // dup shares the same open file description as a descriptor inherited
        // across fork, without needing to fork the multithreaded test process.
        let inherited = lock._file.try_clone().expect("duplicate descriptor");
        drop(lock);
        assert_eq!(InstanceLock::holder(dir.path()), None);
        let replacement = InstanceLock::acquire(dir.path()).expect("acquire after release");
        drop(inherited);
        assert!(InstanceLock::holder(dir.path()).is_some());
        drop(replacement);
        assert_eq!(InstanceLock::holder(dir.path()), None);
    }

    #[test]
    fn holder_probe_reports_pid_without_disturbing_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(InstanceLock::holder(dir.path()), None, "unlocked dir");
        let lock = InstanceLock::acquire(dir.path()).expect("acquire");
        assert_eq!(
            InstanceLock::holder(dir.path()).as_deref(),
            Some(std::process::id().to_string().as_str()),
        );
        // The probe must not have stolen the lock from the holder.
        InstanceLock::acquire(dir.path()).expect_err("still held after probe");
        drop(lock);
        assert_eq!(InstanceLock::holder(dir.path()), None, "released");
    }

    #[test]
    fn any_lock_file_reports_contention_as_held_with_the_pid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agentd.lock");
        assert_eq!(InstanceLock::holder_file(&path), None);
        let lock = InstanceLock::acquire_file(&path).expect("acquire");
        let pid = std::process::id().to_string();
        assert_eq!(InstanceLock::holder_file(&path), Some(pid.clone()));
        match InstanceLock::acquire_file(&path) {
            Err(LockError::Held { holder }) => assert_eq!(holder, pid),
            other => panic!("expected Held, got {other:?}"),
        }
        // The engine's lock is a different file: it is free.
        drop(InstanceLock::acquire(dir.path()).expect("engine lock is separate"));
        drop(lock);
        assert_eq!(InstanceLock::holder_file(&path), None);
    }

    #[test]
    fn second_acquire_fails_while_held_then_succeeds_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let lock = InstanceLock::acquire(dir.path()).expect("first acquire");
        let err = InstanceLock::acquire(dir.path()).expect_err("second acquire must fail");
        let msg = err.to_string();
        assert!(msg.contains("already running"), "unexpected error: {msg}");
        assert!(
            msg.contains(&std::process::id().to_string()),
            "holder pid missing from error: {msg}"
        );
        drop(lock);
        InstanceLock::acquire(dir.path()).expect("acquire after release");
    }

    #[cfg(windows)]
    #[test]
    fn subprocess_lock_holder() {
        let Some(dir) = std::env::var_os("LOAMS_DESKTOP_INSTANCE_LOCK_TEST_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let _lock = InstanceLock::acquire(&dir).expect("child acquire");
        std::fs::write(dir.join("ready"), []).expect("signal ready");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.join("release").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "release signal timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[cfg(windows)]
    #[test]
    fn subprocess_contention_reports_pid_and_releases_on_exit() {
        use std::process::{Command, Stdio};

        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("subprocess_lock_holder")
            .arg("--nocapture")
            .env("LOAMS_DESKTOP_INSTANCE_LOCK_TEST_DIR", dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn lock holder");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !dir.path().join("ready").exists() {
            if let Some(status) = child.try_wait().expect("poll lock holder") {
                panic!("lock holder exited before becoming ready: {status}");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "ready signal timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let child_pid = child.id().to_string();
        assert_eq!(
            InstanceLock::holder(dir.path()).as_deref(),
            Some(child_pid.as_str())
        );
        let err = InstanceLock::acquire(dir.path()).expect_err("parent acquire must contend");
        assert!(
            err.to_string().contains(&child_pid),
            "holder pid missing: {err}"
        );

        std::fs::write(dir.path().join("release"), []).expect("signal release");
        assert!(child.wait().expect("wait for lock holder").success());
        assert_eq!(
            InstanceLock::holder(dir.path()),
            None,
            "released after child exit"
        );
        InstanceLock::acquire(dir.path()).expect("acquire after child exit");
    }
}
