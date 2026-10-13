//! Linux: a systemd user unit, `~/.config/systemd/user/loams-agentd.service`
//! (design §50 §5.1).
//!
//! `Restart=on-failure` with `RestartPreventExitStatus=3 4` restarts the
//! daemon on 70 and on signals only; `StartLimitBurst=5` in
//! `StartLimitIntervalSec=600` stops a crash loop. `KillMode=mixed` sends
//! SIGTERM to the daemon alone, so it drains (§6.2), and kills the engine and
//! harness children only if they outlive `TimeoutStopSec=30`.
//! `WantedBy=default.target` starts it at login, and at boot only with linger
//! (§5.6). The unit has no `Environment=` lines.

use std::path::{Path, PathBuf};

use super::{Launch, SYSTEMD_UNIT, ServiceError, Status, plain, query, tool};

/// The unit for `launch`.
pub fn render_unit(launch: &Launch) -> Result<String, ServiceError> {
    Ok(format!(
        "# Written by `loams-agentd service install`; it is rewritten on every install.
[Unit]
Description=Loams Desktop agent daemon
StartLimitIntervalSec=600
StartLimitBurst=5

[Service]
Type=exec
ExecStart={exe} run --service --config {config}
Restart=on-failure
RestartSec=2
RestartPreventExitStatus=3 4
KillMode=mixed
TimeoutStopSec=30

[Install]
WantedBy=default.target
",
        exe = quote(&launch.exe)?,
        config = quote(&launch.config)?,
    ))
}

/// One `ExecStart` word: double-quoted, because Electron's `userData` has a
/// space, with `\` and `"` backslash-escaped, `%` (a specifier) written `%%`
/// and `$` (a variable) written `$$` (plan DD1 ruling T0-7).
fn quote(path: &Path) -> Result<String, ServiceError> {
    let mut word = String::from('"');
    for c in plain(path)?.chars() {
        match c {
            '\\' => word.push_str("\\\\"),
            '"' => word.push_str("\\\""),
            '%' => word.push_str("%%"),
            '$' => word.push_str("$$"),
            c => word.push(c),
        }
    }
    word.push('"');
    Ok(word)
}

/// `$XDG_CONFIG_HOME/systemd/user` (when absolute), else
/// `~/.config/systemd/user`: where the user manager looks for units.
pub fn unit_dir() -> Result<PathBuf, ServiceError> {
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute());
    let config = match config {
        Some(config) => config,
        None => super::home()?.join(".config"),
    };
    Ok(config.join("systemd/user"))
}

/// The unit file in [`unit_dir`].
fn unit_path() -> Result<PathBuf, ServiceError> {
    Ok(unit_dir()?.join(SYSTEMD_UNIT))
}

/// `systemctl --user <args>`.
fn systemctl(args: &[&str]) -> Result<String, ServiceError> {
    let mut all = vec!["--user"];
    all.extend_from_slice(args);
    tool("systemctl", &all)
}

/// Write the unit, reload the user manager, then enable and start it.
pub fn install(launch: &Launch) -> Result<String, ServiceError> {
    let unit = unit_path()?;
    super::write_file(&unit, &render_unit(launch)?)?;
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", "--now", SYSTEMD_UNIT])?;
    Ok(format!(
        "Installed and started {SYSTEMD_UNIT} ({}).",
        unit.display()
    ))
}

/// Stop and disable the unit, remove it and reload. Not installed is fine.
pub fn uninstall() -> Result<String, ServiceError> {
    let unit = unit_path()?;
    if !unit.exists() {
        return Ok(format!("{SYSTEMD_UNIT} is not installed."));
    }
    systemctl(&["disable", "--now", SYSTEMD_UNIT])?;
    super::remove_file(&unit)?;
    systemctl(&["daemon-reload"])?;
    Ok(format!("Removed {SYSTEMD_UNIT} ({}).", unit.display()))
}

/// Start the installed unit.
pub fn start() -> Result<String, ServiceError> {
    systemctl(&["start", SYSTEMD_UNIT])?;
    Ok(format!("Started {SYSTEMD_UNIT}."))
}

/// Stop until the next login or `start` (the unit stays enabled).
pub fn stop() -> Result<String, ServiceError> {
    systemctl(&["stop", SYSTEMD_UNIT])?;
    Ok(format!("Stopped {SYSTEMD_UNIT}."))
}

/// Whether the unit file exists, and `systemctl --user is-active`.
pub fn status() -> Result<Status, ServiceError> {
    let unit = unit_path()?;
    let state = query("systemctl", &["--user", "is-active", SYSTEMD_UNIT])?;
    Ok(Status {
        installed: unit.exists(),
        location: unit.display().to_string(),
        state: if state.is_empty() {
            "unknown".to_owned()
        } else {
            state
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_start_words_escape_what_systemd_expands() {
        assert_eq!(
            quote(Path::new(r#"/a "b" \c/50% $HOME"#)).expect("quote"),
            r#""/a \"b\" \\c/50%% $$HOME""#
        );
        assert!(matches!(
            quote(Path::new("/a\nExecStartPre=/bin/evil")),
            Err(ServiceError::Unrepresentable(_))
        ));
    }
}
