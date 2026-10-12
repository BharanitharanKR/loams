//! The per-user service (design §50 §5.1, D784) and Linux linger (§5.6,
//! D786): `loams-agentd service install|uninstall|start|stop|status` and
//! `service enable-linger|disable-linger`.
//!
//! | OS | Mechanism | Module |
//! |---|---|---|
//! | Linux | `~/.config/systemd/user/loams-agentd.service` | [`systemd`] |
//! | macOS | `~/Library/LaunchAgents/dev.loams.agentd.plist` | [`launchd`] |
//! | Windows | `HKCU\…\Run`, value `LoamsAgentd`, and a parent that restarts its worker | [`windows`] |
//!
//! Every mechanism launches `<runtime>/current/loams-agentd run --service
//! --config <config>` (on Windows the version `current.txt` names), with no
//! environment captured: the daemon reads its config file and resolves the
//! login shell's `PATH` itself. Rendering is pure and compiles everywhere, so
//! the golden tests run on every platform.

pub mod launchd;
pub mod linger;
pub mod systemd;
pub mod windows;

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::{Config, ConfigError};
use crate::paths::Paths;

/// The systemd user unit.
pub const SYSTEMD_UNIT: &str = "loams-agentd.service";
/// The launchd label.
pub const LAUNCHD_LABEL: &str = "dev.loams.agentd";
/// The value under `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`.
pub const RUN_VALUE: &str = "LoamsAgentd";
/// What a service manager captures from the daemon's stdout and stderr, in
/// the daemon's `logs/` (ruling T6-8; the daemon itself writes `agentd.log`).
pub const STDOUT_LOG: &str = "agentd.stdout.log";

/// What a service manager launches: `<exe> run --service --config <config>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch {
    pub exe: PathBuf,
    pub config: PathBuf,
    /// Where stdout and stderr go when the manager captures them (launchd,
    /// and the Windows parent); systemd sends them to the journal.
    pub stdout_log: PathBuf,
}

impl Launch {
    /// The launch of the daemon whose config is `config`, from the installed
    /// runtime: `[paths] runtime` when set (Windows keeps it in Local
    /// AppData, T0-7), else `runtime/` in the daemon directory. The config
    /// must load, and the runtime must be installed.
    pub fn for_config(config: &Path) -> Result<Self, ServiceError> {
        let config = std::path::absolute(config).map_err(ServiceError::io(config))?;
        let loaded = Config::load(&config)?;
        let paths = Paths::of_config(&config).map_err(ServiceError::io(&config))?;
        let runtime = loaded
            .paths
            .runtime
            .map_or_else(|| paths.runtime(), |runtime| paths.dir().join(runtime));
        let exe = crate::runtime_install::service_exe(&runtime).ok_or_else(|| {
            ServiceError::NotInstalled(format!(
                "no runtime is installed in {}: run `loams-agentd install-runtime` first",
                runtime.display()
            ))
        })?;
        Ok(Self {
            exe,
            config,
            stdout_log: paths.logs().join(STDOUT_LOG),
        })
    }
}

/// Why a service command failed.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// The mechanism does not exist on this OS (linger off Linux).
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("{0}")]
    NotInstalled(String),
    /// A path that the unit, plist or Run value cannot hold.
    #[error("{0}")]
    Unrepresentable(String),
    /// A service manager tool failed; its error text is passed on.
    #[error("`{command}` failed ({status}): {output}")]
    Tool {
        command: String,
        status: String,
        output: String,
    },
    #[error("cannot run `{program}`: {source}")]
    Spawn {
        program: String,
        #[source]
        source: io::Error,
    },
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{0}")]
    Other(String),
}

impl ServiceError {
    fn io(path: &Path) -> impl FnOnce(io::Error) -> Self + '_ {
        move |source| Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// A service command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Install,
    Uninstall,
    Start,
    Stop,
    Status,
}

/// What `service status` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Whether the unit, plist or Run value exists.
    pub installed: bool,
    /// Where it lives.
    pub location: String,
    /// The manager's word for it: systemd's `is-active` (`active`,
    /// `inactive`, `failed`, …), launchd's `state` (`running`, `not loaded`,
    /// …), or `running` / `stopped` on Windows.
    pub state: String,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "installed: {}\nlocation: {}\nstate: {}",
            self.installed, self.location, self.state
        )
    }
}

/// Run `action` for the daemon whose config is `config`, with this OS's
/// mechanism. Returns what to print.
pub fn run(action: Action, config: &Path) -> Result<String, ServiceError> {
    let os = std::env::consts::OS;
    match (os, action) {
        ("linux", Action::Install) => systemd::install(&Launch::for_config(config)?),
        ("linux", Action::Uninstall) => systemd::uninstall(),
        ("linux", Action::Start) => systemd::start(),
        ("linux", Action::Stop) => systemd::stop(),
        ("linux", Action::Status) => systemd::status().map(|status| status.to_string()),
        ("macos", Action::Install) => launchd::install(&Launch::for_config(config)?),
        ("macos", Action::Uninstall) => launchd::uninstall(),
        ("macos", Action::Start) => launchd::start(),
        ("macos", Action::Stop) => launchd::stop(),
        ("macos", Action::Status) => launchd::status().map(|status| status.to_string()),
        #[cfg(windows)]
        ("windows", action) => windows::run(action, config),
        _ => Err(ServiceError::Unsupported(format!(
            "no per-user service on {os}"
        ))),
    }
}

/// Run a service manager tool; a non-zero exit is an error carrying its
/// stderr (or stdout when stderr is empty). Returns stdout.
fn tool(program: &str, args: &[&str]) -> Result<String, ServiceError> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|source| ServiceError::Spawn {
            program: program.to_owned(),
            source,
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if output.status.success() {
        return Ok(stdout);
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(ServiceError::Tool {
        command: std::iter::once(program)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" "),
        status: output.status.to_string(),
        output: if stderr.is_empty() { stdout } else { stderr },
    })
}

/// Run a query tool whose exit code is part of the answer (`systemctl
/// is-active` exits 3 for an inactive unit). Returns stdout.
fn query(program: &str, args: &[&str]) -> Result<String, ServiceError> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|source| ServiceError::Spawn {
            program: program.to_owned(),
            source,
        })?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Replace `path` with `text` atomically (a sibling temporary file renamed
/// over it), creating its directory. The file is the user's ordinary 0644 (or
/// as the umask has it): a unit and a plist hold paths, not secrets.
fn write_file(path: &Path, text: &str) -> Result<(), ServiceError> {
    let dir = path
        .parent()
        .ok_or_else(|| ServiceError::Other(format!("{} has no parent", path.display())))?;
    std::fs::create_dir_all(dir).map_err(ServiceError::io(dir))?;
    let temp = dir.join(format!(".loams-agentd.{}.tmp", std::process::id()));
    let written = std::fs::write(&temp, text).and_then(|()| std::fs::rename(&temp, path));
    if written.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    written.map_err(ServiceError::io(path))
}

/// Remove `path`; `false` when it was not there.
fn remove_file(path: &Path) -> Result<bool, ServiceError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(ServiceError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// `path` as UTF-8 without control characters: a newline would end the
/// unit's line, and a plist or Run value cannot carry one either.
fn plain(path: &Path) -> Result<&str, ServiceError> {
    let text = path.to_str().ok_or_else(|| {
        ServiceError::Unrepresentable(format!("{} is not valid UTF-8", path.display()))
    })?;
    if text.chars().any(char::is_control) {
        return Err(ServiceError::Unrepresentable(format!(
            "{text:?} has a control character"
        )));
    }
    Ok(text)
}

/// `$HOME`, which every per-user location hangs from.
fn home() -> Result<PathBuf, ServiceError> {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| ServiceError::Other("HOME is not set".to_owned()))
}
