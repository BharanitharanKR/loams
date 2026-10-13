//! macOS: a LaunchAgent, `~/Library/LaunchAgents/dev.loams.agentd.plist`,
//! loaded with `launchctl bootstrap gui/<uid>` (design §50 §5.1).
//!
//! A plist, not `SMAppService`, while the app is unsigned (Q701).
//! `KeepAlive { SuccessfulExit = false; Crashed = true }` with
//! `ThrottleInterval 10` restarts a failed or crashed daemon; `RunAtLoad`
//! starts it at login. launchd captures stdout and stderr in
//! `logs/agentd.stdout.log`; the daemon writes its own `logs/agentd.log`
//! (ruling T6-8).

use std::path::PathBuf;

use super::{LAUNCHD_LABEL, Launch, ServiceError, Status, plain, query, tool};

/// The plist for `launch`.
pub fn render_plist(launch: &Launch) -> Result<String, ServiceError> {
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Written by `loams-agentd service install`; it is rewritten on every install. -->
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>run</string>
    <string>--service</string>
    <string>--config</string>
    <string>{config}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
    <key>Crashed</key>
    <true/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#,
        exe = xml_text(&launch.exe)?,
        config = xml_text(&launch.config)?,
        log = xml_text(&launch.stdout_log)?,
    ))
}

/// XML text for a `<string>`: only `&`, `<` and `>` need escaping (T0-7).
fn xml_text(path: &std::path::Path) -> Result<String, ServiceError> {
    Ok(plain(path)?
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;"))
}

/// `~/Library/LaunchAgents/dev.loams.agentd.plist`.
fn plist_path() -> Result<PathBuf, ServiceError> {
    Ok(super::home()?
        .join("Library/LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

/// `gui/<uid>`, the logged-in user's domain.
fn domain() -> String {
    format!("gui/{}", uid())
}

/// `gui/<uid>/dev.loams.agentd`.
fn target() -> String {
    format!("{}/{LAUNCHD_LABEL}", domain())
}

/// The real user id.
#[cfg(unix)]
fn uid() -> u32 {
    // SAFETY: getuid takes no arguments, touches no memory and cannot fail.
    #[allow(unsafe_code)]
    unsafe {
        libc::getuid()
    }
}

/// No uid off Unix; launchd exists only on macOS.
#[cfg(not(unix))]
fn uid() -> u32 {
    0
}

/// Whether the agent is loaded in the user's domain.
fn loaded() -> bool {
    tool("launchctl", &["print", &target()]).is_ok()
}

/// Boot a loaded agent out; its failure is an error, because the agent
/// would keep running. An agent that is not loaded needs nothing.
fn boot_out_if_loaded() -> Result<(), ServiceError> {
    if loaded() {
        tool("launchctl", &["bootout", &target()])?;
    }
    Ok(())
}

/// Write the plist and bootstrap it (a loaded older one is booted out first).
pub fn install(launch: &Launch) -> Result<String, ServiceError> {
    let plist = plist_path()?;
    if let Some(logs) = launch.stdout_log.parent() {
        crate::owner_only::create_dir(logs).map_err(ServiceError::io(logs))?;
    }
    boot_out_if_loaded()?;
    super::write_file(&plist, &render_plist(launch)?)?;
    tool(
        "launchctl",
        &["bootstrap", &domain(), &plist.to_string_lossy()],
    )?;
    Ok(format!(
        "Installed and started {LAUNCHD_LABEL} ({}).",
        plist.display()
    ))
}

/// Boot the agent out and remove the plist. Not installed is fine; a loaded
/// agent that does not boot out is an error, and the plist stays.
pub fn uninstall() -> Result<String, ServiceError> {
    let plist = plist_path()?;
    boot_out_if_loaded()?;
    if super::remove_file(&plist)? {
        Ok(format!("Removed {LAUNCHD_LABEL} ({}).", plist.display()))
    } else {
        Ok(format!("{LAUNCHD_LABEL} is not installed."))
    }
}

/// `stop` boots the agent out, so start bootstraps it again (already loaded
/// is fine) and kickstarts it.
pub fn start() -> Result<String, ServiceError> {
    let plist = plist_path()?;
    if !plist.exists() {
        return Err(ServiceError::NotInstalled(format!(
            "{LAUNCHD_LABEL} is not installed: run `loams-agentd service install` first"
        )));
    }
    let _ = tool(
        "launchctl",
        &["bootstrap", &domain(), &plist.to_string_lossy()],
    );
    tool("launchctl", &["kickstart", &target()])?;
    Ok(format!("Started {LAUNCHD_LABEL}."))
}

/// Boot the agent out: with `KeepAlive`, killing it would restart it. It is
/// loaded again at the next login.
pub fn stop() -> Result<String, ServiceError> {
    tool("launchctl", &["bootout", &target()])?;
    Ok(format!("Stopped {LAUNCHD_LABEL}."))
}

/// Whether the plist exists, and launchd's `state` for the agent (`not
/// loaded` when `launchctl print` knows nothing of it).
pub fn status() -> Result<Status, ServiceError> {
    let plist = plist_path()?;
    let printed = query("launchctl", &["print", &target()])?;
    let state = printed
        .lines()
        .find_map(|line| line.trim().strip_prefix("state = "))
        .map_or_else(|| "not loaded".to_owned(), str::to_owned);
    Ok(Status {
        installed: plist.exists(),
        location: plist.display().to_string(),
        state,
    })
}
