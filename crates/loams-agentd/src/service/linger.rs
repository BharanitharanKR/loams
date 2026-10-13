//! Linux linger (design §50 §5.6, D786): `service enable-linger` and
//! `disable-linger` run `loginctl enable-linger|disable-linger "$USER"`, so
//! the user's services start at boot and survive logout.
//!
//! It is off by default, and the daemon never turns it on by itself: Loams
//! Desktop runs these commands when the user ticks "Keep running after I log
//! out". polkit lets an active local session change its own linger on common
//! distributions; elsewhere `loginctl` fails and its error text is returned.
//! Other OSes have no linger: `unsupported`.

use std::process::Command;

use super::{ServiceError, tool};

/// The `loginctl` command for `os` (`std::env::consts::OS`).
pub fn command(os: &str, enable: bool) -> Result<Command, ServiceError> {
    if os != "linux" {
        return Err(ServiceError::Unsupported(format!(
            "linger is a Linux (systemd-logind) setting; there is none on {os}"
        )));
    }
    let mut command = Command::new("loginctl");
    command
        .arg(if enable {
            "enable-linger"
        } else {
            "disable-linger"
        })
        .arg(user());
    Ok(command)
}

/// `$USER`, else the numeric uid (`loginctl` takes either).
fn user() -> String {
    if let Some(user) = std::env::var_os("USER").filter(|user| !user.is_empty()) {
        return user.to_string_lossy().into_owned();
    }
    #[cfg(unix)]
    {
        // SAFETY: getuid takes no arguments, touches no memory and cannot fail.
        #[allow(unsafe_code)]
        let uid = unsafe { libc::getuid() };
        uid.to_string()
    }
    #[cfg(not(unix))]
    String::new()
}

/// Turn linger on or off for this user, on this OS.
pub fn set(enable: bool) -> Result<String, ServiceError> {
    let command = command(std::env::consts::OS, enable)?;
    let program = command.get_program().to_string_lossy().into_owned();
    let args: Vec<String> = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    tool(&program, &args)?;
    Ok(if enable {
        "Linger is on: the service starts at boot and keeps running after logout.".to_owned()
    } else {
        "Linger is off: the service runs only while you are logged in.".to_owned()
    })
}
