//! Windows: a Run entry, `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`
//! value `LoamsAgentd`, and a parent that restarts its worker (design §50
//! §5.1).
//!
//! A Run entry needs no elevation but has no crash restart, so the daemon
//! supervises itself: `run --service` is a parent that spawns `run --worker`
//! in a Job Object with kill-on-close, and restarts it when it exits with
//! anything but 0, 3 or 4, after 1, 2, 4, 8, 16 then 30 s, at most 5 times in
//! 10 minutes (the engine supervisor's policy). The worker's stdout and
//! stderr, and the parent's own lines, go to `logs/agentd.stdout.log`.
//!
//! Windows has no `current` symlink, so the Run value names the version that
//! `current.txt` names when it is written; an upgrade writes it again with
//! `service install` (plan DD1 ruling T8-1).
//!
//! The value, the policy and the parent's loop are plain Rust and compile
//! everywhere; the registry, the Job Object and the detached start are
//! Windows-only.

use std::io;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};

use super::{Launch, ServiceError, plain};
use crate::paths::Paths;
use crate::{EXIT_ALREADY_RUNNING, EXIT_INTERNAL, EXIT_INVALID_CONFIG, EXIT_STOPPED};

/// The Run value for `launch`: `"<exe>" run --service --config "<config>"`,
/// stored as `REG_SZ`, so `%` is never expanded.
pub fn render_run_value(launch: &Launch) -> Result<String, ServiceError> {
    Ok(format!(
        "{} run --service --config {}",
        quote_arg(&launch.exe)?,
        quote_arg(&launch.config)?
    ))
}

/// One command-line argument in double quotes, by the MSVC rules: the
/// backslashes before the closing quote are doubled. A Windows path has no
/// `"`; one is refused rather than escaped.
fn quote_arg(path: &Path) -> Result<String, ServiceError> {
    let text = plain(path)?;
    if text.contains('"') {
        return Err(ServiceError::Unrepresentable(format!(
            "{text:?} has a double quote"
        )));
    }
    let trailing = text.len() - text.trim_end_matches('\\').len();
    Ok(format!("\"{text}{}\"", "\\".repeat(trailing)))
}

/// The delays before the 1st, 2nd, … restart.
pub const BACKOFF: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];
/// At most this many restarts in [`RESTART_WINDOW`]; then the parent gives up.
pub const MAX_RESTARTS: usize = 5;
pub const RESTART_WINDOW: Duration = Duration::from_secs(10 * 60);

/// What the parent does when its worker exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Exit with this code.
    Exit(u8),
    /// Start a new worker after this delay.
    Restart(Duration),
}

/// The restart policy: 0 (stopped), 3 (already running) and 4 (invalid
/// config) end the parent with the same code; anything else is a crash.
#[derive(Debug, Default)]
pub struct RestartPolicy {
    restarts: Vec<Instant>,
}

impl RestartPolicy {
    /// The worker exited with `code` (`None`: killed, no code) at `now`.
    pub fn on_exit(&mut self, code: Option<i32>, now: Instant) -> Decision {
        let final_code = code
            .and_then(|code| u8::try_from(code).ok())
            .filter(|code| {
                [EXIT_STOPPED, EXIT_ALREADY_RUNNING, EXIT_INVALID_CONFIG].contains(code)
            });
        if let Some(code) = final_code {
            return Decision::Exit(code);
        }
        self.restarts
            .retain(|at| now.saturating_duration_since(*at) < RESTART_WINDOW);
        if self.restarts.len() >= MAX_RESTARTS {
            return Decision::Exit(EXIT_INTERNAL);
        }
        self.restarts.push(now);
        Decision::Restart(BACKOFF[(self.restarts.len() - 1).min(BACKOFF.len() - 1)])
    }
}

/// The parent's loop: `spawn` a worker, wait for it, and restart it per
/// [`RestartPolicy`], calling `sleep` for each delay and `log` for each line
/// worth keeping. Returns the parent's exit code. A worker that cannot be
/// spawned or waited for counts as a crash.
pub fn supervise_with(
    mut spawn: impl FnMut() -> io::Result<Child>,
    mut sleep: impl FnMut(Duration),
    mut log: impl FnMut(&str),
) -> u8 {
    let mut policy = RestartPolicy::default();
    loop {
        let code = match spawn().and_then(|mut worker| worker.wait()) {
            Ok(status) => status.code(),
            Err(error) => {
                log(&format!("[service] the worker did not run: {error}"));
                None
            }
        };
        match policy.on_exit(code, Instant::now()) {
            Decision::Exit(EXIT_INTERNAL) => {
                log(&format!(
                    "[service] the worker exited ({code:?}) and was restarted {MAX_RESTARTS} \
                     times in 10 minutes; giving up"
                ));
                return EXIT_INTERNAL;
            }
            Decision::Exit(code) => return code,
            Decision::Restart(delay) => {
                log(&format!(
                    "[service] the worker exited ({code:?}); restart in {}s",
                    delay.as_secs()
                ));
                sleep(delay);
            }
        }
    }
}

/// Ask the daemon of `paths` to stop over its RPC, and wait up to 30 s for
/// it to release its lock. `false` when no daemon runs there.
pub fn request_stop(paths: &Paths) -> Result<bool, ServiceError> {
    use loams_agentd_rpc::methods;
    let Some(found) = crate::discovery::read(paths) else {
        return Ok(false);
    };
    let token = crate::token::read(paths).map_err(ServiceError::io(&paths.token()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| ServiceError::Other(error.to_string()))?;
    runtime
        .block_on(async {
            let client = loams_agentd_rpc::connect_ws_with_token(
                &format!("ws://127.0.0.1:{}", found.port),
                token.expose(),
            )
            .await?;
            let hello = serde_json::to_value(crate::hello::cli_hello())
                .map_err(|error| loams_agentd_rpc::RpcError::BadParams(error.to_string()))?;
            client.call(methods::HELLO, hello).await?;
            client
                .call(methods::STOP_ENGINE, serde_json::json!({}))
                .await
        })
        .map_err(|error| ServiceError::Other(format!("the daemon did not stop: {error}")))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while crate::lock::holder(paths).is_some() {
        if Instant::now() > deadline {
            return Err(ServiceError::Other(
                "the daemon accepted the stop but still runs after 30 s".to_owned(),
            ));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(true)
}

#[cfg(windows)]
pub use imp::{run, supervise};

#[cfg(windows)]
mod imp {
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::CommandExt;
    use std::path::Path;
    use std::process::{Command, Stdio};

    use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;
    use windows_sys::Win32::System::Threading::{
        CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, DETACHED_PROCESS,
    };

    use super::super::{Action, Launch, RUN_VALUE, STDOUT_LOG, ServiceError, Status};
    use super::{render_run_value, request_stop, supervise_with};
    use crate::paths::Paths;

    /// A service command on Windows.
    pub fn run(action: Action, config: &Path) -> Result<String, ServiceError> {
        let config = std::path::absolute(config).map_err(ServiceError::io(config))?;
        let paths = Paths::of_config(&config).map_err(ServiceError::io(&config))?;
        match action {
            Action::Install => {
                let launch = Launch::for_config(&config)?;
                registry::set(RUN_VALUE, &render_run_value(&launch)?)?;
                start_detached(&launch)?;
                Ok(format!("Installed and started the {RUN_VALUE} Run entry."))
            }
            Action::Uninstall => {
                request_stop(&paths)?;
                if registry::delete(RUN_VALUE)? {
                    Ok(format!("Removed the {RUN_VALUE} Run entry."))
                } else {
                    Ok(format!("The {RUN_VALUE} Run entry is not installed."))
                }
            }
            Action::Start => {
                if registry::get(RUN_VALUE)?.is_none() {
                    return Err(ServiceError::NotInstalled(format!(
                        "the {RUN_VALUE} Run entry is not installed: run `loams-agentd service install` first"
                    )));
                }
                start_detached(&Launch::for_config(&config)?)?;
                Ok("Started.".to_owned())
            }
            Action::Stop => Ok(if request_stop(&paths)? {
                "Stopped.".to_owned()
            } else {
                "Not running.".to_owned()
            }),
            Action::Status => {
                let value = registry::get(RUN_VALUE)?;
                let running = crate::discovery::read(&paths).is_some();
                Ok(Status {
                    installed: value.is_some(),
                    location: format!(
                        r"HKCU\{}\{RUN_VALUE}{}",
                        registry::RUN_KEY,
                        value.map(|v| format!(" = {v}")).unwrap_or_default()
                    ),
                    state: if running { "running" } else { "stopped" }.to_owned(),
                }
                .to_string())
            }
        }
    }

    /// Start `run --service` outside Electron's console and job, as the Run
    /// entry would at logon.
    fn start_detached(launch: &Launch) -> Result<(), ServiceError> {
        let spawn = |flags: u32| {
            Command::new(&launch.exe)
                .args(["run", "--service", "--config"])
                .arg(&launch.config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .creation_flags(flags)
                .spawn()
        };
        let flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
        // Leave the caller's job when it allows it, so closing Loams Desktop
        // does not take the service with it; a job that forbids breakaway
        // refuses the flag, and the plain start is the fallback.
        spawn(flags | CREATE_BREAKAWAY_FROM_JOB)
            .or_else(|_| spawn(flags))
            .map(drop)
            .map_err(|source| ServiceError::Spawn {
                program: launch.exe.display().to_string(),
                source,
            })
    }

    /// `run --service`: spawn `run --worker` in a kill-on-close Job Object
    /// and restart it per the policy. Returns the exit code.
    pub fn supervise(config: &Path) -> u8 {
        use std::io::Write;
        let logs = Paths::of_config(config).map(|paths| paths.logs());
        let log_path = logs.as_ref().ok().map(|logs| logs.join(STDOUT_LOG));
        if let Ok(logs) = &logs {
            let _ = crate::owner_only::create_dir(logs);
        }
        let open_log = || {
            log_path.as_ref().and_then(|path| {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .ok()
            })
        };
        let exe = match std::env::current_exe() {
            Ok(exe) => exe,
            Err(_) => return crate::EXIT_INTERNAL,
        };
        // Dropping the job (the parent exits or dies) kills the worker.
        let job = match loams_agentd_harness::windows_process::Job::new() {
            Ok(job) => job,
            Err(error) => {
                if let Some(mut file) = open_log() {
                    let _ = writeln!(file, "[service] no Job Object: {error}");
                }
                return crate::EXIT_INTERNAL;
            }
        };
        supervise_with(
            || {
                let stdout = open_log().map_or_else(Stdio::null, Stdio::from);
                let stderr = open_log().map_or_else(Stdio::null, Stdio::from);
                let worker = Command::new(&exe)
                    .args(["run", "--worker", "--config"])
                    .arg(config)
                    .stdin(Stdio::null())
                    .stdout(stdout)
                    .stderr(stderr)
                    .creation_flags(CREATE_NO_WINDOW)
                    .spawn()?;
                // SAFETY: both handles are live for the call: the job is owned
                // above and the worker's handle by `worker`.
                #[allow(unsafe_code)]
                let assigned = unsafe {
                    AssignProcessToJobObject(
                        job.as_handle().as_raw_handle(),
                        worker.as_raw_handle(),
                    )
                };
                if assigned == 0 {
                    let error = std::io::Error::last_os_error();
                    let mut worker = worker;
                    let _ = worker.kill();
                    return Err(error);
                }
                Ok(worker)
            },
            std::thread::sleep,
            |line| {
                if let Some(mut file) = open_log() {
                    let _ = writeln!(file, "{line}");
                }
            },
        )
    }

    /// The current user's Run key.
    mod registry {
        use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_SUCCESS};
        use windows_sys::Win32::System::Registry::{
            HKEY_CURRENT_USER, REG_SZ, RRF_RT_REG_SZ, RegDeleteKeyValueW, RegGetValueW,
            RegSetKeyValueW,
        };

        use super::super::super::ServiceError;

        pub const RUN_KEY: &str = r"Software\Microsoft\Windows\CurrentVersion\Run";

        fn wide(text: &str) -> Vec<u16> {
            text.encode_utf16().chain(std::iter::once(0)).collect()
        }

        fn error(code: u32, what: &str) -> ServiceError {
            ServiceError::Other(format!(
                r"HKCU\{RUN_KEY}\{what}: {}",
                std::io::Error::from_raw_os_error(code as i32)
            ))
        }

        /// Write `name` as `REG_SZ` (the key is created if missing).
        pub fn set(name: &str, value: &str) -> Result<(), ServiceError> {
            let key = wide(RUN_KEY);
            let name_w = wide(name);
            let data = wide(value);
            let bytes = u32::try_from(data.len() * 2)
                .map_err(|_| ServiceError::Other("the Run value is too long".to_owned()))?;
            // SAFETY: every pointer is a live, NUL-terminated UTF-16 buffer and
            // `bytes` is the size of `data`.
            #[allow(unsafe_code)]
            let code = unsafe {
                RegSetKeyValueW(
                    HKEY_CURRENT_USER,
                    key.as_ptr(),
                    name_w.as_ptr(),
                    REG_SZ,
                    data.as_ptr().cast(),
                    bytes,
                )
            };
            if code == ERROR_SUCCESS {
                Ok(())
            } else {
                Err(error(code, name))
            }
        }

        /// The value of `name`, `None` when it is absent.
        pub fn get(name: &str) -> Result<Option<String>, ServiceError> {
            let key = wide(RUN_KEY);
            let name_w = wide(name);
            let mut size: u32 = 0;
            // SAFETY: a null data pointer asks only for the size, into `size`.
            #[allow(unsafe_code)]
            let code = unsafe {
                RegGetValueW(
                    HKEY_CURRENT_USER,
                    key.as_ptr(),
                    name_w.as_ptr(),
                    RRF_RT_REG_SZ,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut size,
                )
            };
            if code == ERROR_FILE_NOT_FOUND {
                return Ok(None);
            }
            if code != ERROR_SUCCESS {
                return Err(error(code, name));
            }
            let mut data = vec![0u16; (size as usize).div_ceil(2)];
            // SAFETY: `data` holds `size` bytes, and `size` says so.
            #[allow(unsafe_code)]
            let code = unsafe {
                RegGetValueW(
                    HKEY_CURRENT_USER,
                    key.as_ptr(),
                    name_w.as_ptr(),
                    RRF_RT_REG_SZ,
                    std::ptr::null_mut(),
                    data.as_mut_ptr().cast(),
                    &mut size,
                )
            };
            if code == ERROR_FILE_NOT_FOUND {
                return Ok(None);
            }
            if code != ERROR_SUCCESS {
                return Err(error(code, name));
            }
            let len = data
                .iter()
                .position(|&unit| unit == 0)
                .unwrap_or(data.len());
            Ok(Some(String::from_utf16_lossy(&data[..len])))
        }

        /// Delete `name`; `false` when it was absent.
        pub fn delete(name: &str) -> Result<bool, ServiceError> {
            let key = wide(RUN_KEY);
            let name_w = wide(name);
            // SAFETY: both pointers are live, NUL-terminated UTF-16 buffers.
            #[allow(unsafe_code)]
            let code =
                unsafe { RegDeleteKeyValueW(HKEY_CURRENT_USER, key.as_ptr(), name_w.as_ptr()) };
            match code {
                ERROR_SUCCESS => Ok(true),
                ERROR_FILE_NOT_FOUND => Ok(false),
                code => Err(error(code, name)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_are_quoted_by_the_msvc_rules() {
        assert_eq!(
            quote_arg(Path::new(r"C:\a b\x.exe")).expect("quote"),
            r#""C:\a b\x.exe""#
        );
        assert_eq!(
            quote_arg(Path::new(r"C:\a b\")).expect("quote"),
            r#""C:\a b\\""#
        );
        assert!(quote_arg(Path::new(r#"C:\a"b"#)).is_err());
    }

    #[test]
    fn crashes_back_off_then_give_up() {
        let mut policy = RestartPolicy::default();
        let start = Instant::now();
        let delays: Vec<Decision> = (0..6)
            .map(|n| policy.on_exit(Some(70), start + Duration::from_secs(n)))
            .collect();
        let secs = |s| Decision::Restart(Duration::from_secs(s));
        assert_eq!(
            delays,
            [
                secs(1),
                secs(2),
                secs(4),
                secs(8),
                secs(16),
                Decision::Exit(EXIT_INTERNAL)
            ]
        );
    }

    #[test]
    fn restarts_older_than_the_window_are_forgotten() {
        let mut policy = RestartPolicy::default();
        let start = Instant::now();
        for n in 0..5 {
            policy.on_exit(None, start + Duration::from_secs(n));
        }
        let later = start + RESTART_WINDOW + Duration::from_secs(10);
        assert_eq!(
            policy.on_exit(Some(70), later),
            Decision::Restart(Duration::from_secs(1))
        );
    }

    #[test]
    fn stop_already_running_and_bad_config_end_the_parent() {
        for code in [0, 3, 4] {
            assert_eq!(
                RestartPolicy::default().on_exit(Some(code), Instant::now()),
                Decision::Exit(u8::try_from(code).expect("small"))
            );
        }
        assert!(matches!(
            RestartPolicy::default().on_exit(Some(1), Instant::now()),
            Decision::Restart(_)
        ));
        assert!(matches!(
            RestartPolicy::default().on_exit(Some(-1073741819), Instant::now()),
            Decision::Restart(_)
        ));
    }
}
