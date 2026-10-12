//! Per-user services, the runtime directory and linger (plan DD1 Task 8,
//! design §50 §5.1, §5.2 and §5.6).
//!
//! Rendering is checked against golden files whose paths hold a space, `%`,
//! `$`, `&` and `<>`. The installers run against a temporary
//! `XDG_CONFIG_HOME` and fake `systemctl` and `loginctl` scripts on `PATH`;
//! only `systemd_install_roundtrip` touches a real user manager, and only
//! when `LOAMS_AGENTD_TEST_SYSTEMD=1` asks for it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use loams_agentd::service::{Launch, launchd, linger, systemd, windows};
use sha2::{Digest, Sha256};

const EXE: &str = std::env::consts::EXE_SUFFIX;

fn temp_root(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("temp root under the target directory")
}

fn golden(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .replace("\r\n", "\n")
}

fn agentd() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loams-agentd"));
    command
        .env_remove("LOAMS_AGENTD_CONFIG")
        .env_remove("RUST_LOG");
    command
}

fn text(output: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Linux userData is `~/.config/Loams Desktop` (T0-7); the home directory
/// here also carries `&`, `%` and `$`.
fn linux_launch() -> Launch {
    let agentd = PathBuf::from("/home/r&d 50% $x/.config/Loams Desktop/agentd");
    Launch {
        exe: agentd.join("runtime/current/loams-agentd"),
        config: agentd.join("config.toml"),
        stdout_log: agentd.join("logs/agentd.stdout.log"),
    }
}

#[test]
fn systemd_unit_golden() {
    let unit = systemd::render_unit(&linux_launch()).expect("render the unit");
    assert_eq!(unit, golden("loams-agentd.service"));
}

#[test]
fn unit_has_no_environment_lines() {
    let unit = systemd::render_unit(&linux_launch()).expect("render the unit");
    for line in unit.lines() {
        assert!(
            !line.trim_start().starts_with("Environment"),
            "the unit captures no environment: {line}"
        );
    }
    // The installed unit too, with secrets-shaped variables in the
    // installer's environment (the fork baked these into its unit).
    let fake = FakeSystem::new();
    let output = fake
        .agentd(&["service", "install"])
        .env("LOAMS_DESKTOP_EDGE_TOKEN", "edge-secret-canary")
        .env("RUST_LOG", "debug")
        .output()
        .expect("run service install");
    assert!(output.status.success(), "{}", text(&output));
    let installed = std::fs::read_to_string(fake.unit()).expect("read the installed unit");
    assert!(!installed.contains("Environment"), "{installed}");
    assert!(!installed.contains("edge-secret-canary"), "{installed}");
    assert!(!installed.contains("RUST_LOG"), "{installed}");
}

#[test]
fn launchd_plist_golden() {
    let agentd =
        PathBuf::from("/Users/r&d 50% $x <y>/Library/Application Support/Loams Desktop/agentd");
    let plist = launchd::render_plist(&Launch {
        exe: agentd.join("runtime/current/loams-agentd"),
        config: agentd.join("config.toml"),
        stdout_log: agentd.join("logs/agentd.stdout.log"),
    })
    .expect("render the plist");
    assert_eq!(plist, golden("dev.loams.agentd.plist"));
}

#[test]
fn windows_run_value_golden() {
    // The runtime is under Local AppData and the config under Roaming (T0-7).
    let value = windows::render_run_value(&Launch {
        exe: PathBuf::from(
            r"C:\Users\r&d 50% $x\AppData\Local\Loams Desktop\agentd\runtime\0.2.1\loams-agentd.exe",
        ),
        config: PathBuf::from(r"C:\Users\r&d 50% $x\AppData\Roaming\Loams Desktop\agentd\config.toml"),
        stdout_log: PathBuf::from(
            r"C:\Users\r&d 50% $x\AppData\Roaming\Loams Desktop\agentd\logs\agentd.stdout.log",
        ),
    })
    .expect("render the Run value");
    assert_eq!(value, golden("windows-run-value.txt").trim_end());
}

// ---------------------------------------------------------------------------
// install-runtime
// ---------------------------------------------------------------------------

/// A bundled `resources/bin`: both binaries, `SHA256SUMS` and `VERSION`.
fn bundle(root: &Path, version: &str) -> PathBuf {
    let dir = root.join(format!("bundle-{version}"));
    std::fs::create_dir_all(&dir).expect("create the bundle");
    let mut sums = String::new();
    for name in ["loams-agentd", "loams"] {
        let file = format!("{name}{EXE}");
        let bytes = format!("#!/bin/sh\n# {name} {version}\n").into_bytes();
        std::fs::write(dir.join(&file), &bytes).expect("write a binary");
        sums.push_str(&format!(
            "{}  {file}\n",
            hex::encode(Sha256::digest(&bytes))
        ));
    }
    std::fs::write(dir.join("SHA256SUMS"), sums).expect("write SHA256SUMS");
    std::fs::write(dir.join("VERSION"), format!("{version}\n")).expect("write VERSION");
    dir
}

fn install_runtime(from: &Path, to: &Path) -> Output {
    agentd()
        .args(["install-runtime", "--from"])
        .arg(from)
        .arg("--to")
        .arg(to)
        .output()
        .expect("run install-runtime")
}

/// The version `current` names (`current.txt` on Windows).
fn current(runtime: &Path) -> Option<String> {
    if cfg!(windows) {
        std::fs::read_to_string(runtime.join("current.txt"))
            .ok()
            .map(|text| text.trim().to_owned())
    } else {
        std::fs::read_link(runtime.join("current"))
            .ok()
            .map(|target| target.to_string_lossy().into_owned())
    }
}

fn versions(runtime: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(runtime)
        .expect("read the runtime directory")
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn install_runtime_checks_hashes() {
    let root = temp_root("runtime-");
    let runtime = root.path().join("Loams Desktop/agentd/runtime");
    let good = bundle(root.path(), "0.2.0");
    let output = install_runtime(&good, &runtime);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(current(&runtime).as_deref(), Some("0.2.0"));
    let installed = runtime.join("0.2.0").join(format!("loams-agentd{EXE}"));
    assert_eq!(
        std::fs::read(&installed).expect("read the installed binary"),
        std::fs::read(good.join(format!("loams-agentd{EXE}"))).expect("read the bundle")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&installed)
            .expect("stat")
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "installed binaries are executable");
        // The resolved unit path works through `current`.
        assert!(
            runtime
                .join("current")
                .join(format!("loams-agentd{EXE}"))
                .is_file()
        );
    }

    // A tampered binary fails, nothing of 0.2.1 is left and `current` stays.
    let tampered = bundle(root.path(), "0.2.1");
    std::fs::write(tampered.join(format!("loams{EXE}")), b"tampered").expect("tamper");
    let output = install_runtime(&tampered, &runtime);
    assert!(!output.status.success(), "{}", text(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("SHA-256"), "{stderr}");
    assert!(stderr.contains(&format!("loams{EXE}")), "{stderr}");
    assert_eq!(current(&runtime).as_deref(), Some("0.2.0"));
    assert_eq!(versions(&runtime), ["0.2.0"]);

    // A binary missing from SHA256SUMS is refused too.
    let unlisted = bundle(root.path(), "0.2.2");
    std::fs::write(
        unlisted.join("SHA256SUMS"),
        std::fs::read_to_string(unlisted.join("SHA256SUMS"))
            .expect("read sums")
            .lines()
            .filter(|line| !line.ends_with(&format!(" loams{EXE}")))
            .map(|line| format!("{line}\n"))
            .collect::<String>(),
    )
    .expect("drop a line");
    let output = install_runtime(&unlisted, &runtime);
    assert!(!output.status.success(), "{}", text(&output));
    assert_eq!(current(&runtime).as_deref(), Some("0.2.0"));

    // A VERSION that is not a version (it names a directory) is refused.
    let escape = bundle(root.path(), "0.2.3");
    std::fs::write(escape.join("VERSION"), "../escape\n").expect("write VERSION");
    let output = install_runtime(&escape, &runtime);
    assert!(!output.status.success(), "{}", text(&output));
    assert!(!runtime.parent().expect("parent").join("escape").exists());
    assert_eq!(current(&runtime).as_deref(), Some("0.2.0"));
}

#[test]
fn install_runtime_keeps_two_versions() {
    let root = temp_root("runtime-");
    let runtime = root.path().join("runtime");
    for version in ["0.1.0", "0.2.0", "0.10.0"] {
        let output = install_runtime(&bundle(root.path(), version), &runtime);
        assert!(output.status.success(), "{}", text(&output));
        assert_eq!(current(&runtime).as_deref(), Some(version));
    }
    // Versions order as versions, not as text: 0.10.0 is the newest.
    assert_eq!(versions(&runtime), ["0.10.0", "0.2.0"]);

    // Installing the current version again changes nothing.
    let output = install_runtime(&bundle(root.path(), "0.10.0"), &runtime);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(versions(&runtime), ["0.10.0", "0.2.0"]);

    // A downgrade (the bundled version is authoritative, §50 §5.3) keeps the
    // version it replaces and never removes the one `current` names.
    let output = install_runtime(&bundle(root.path(), "0.1.0"), &runtime);
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(current(&runtime).as_deref(), Some("0.1.0"));
    assert_eq!(versions(&runtime), ["0.1.0", "0.10.0"]);
}

// ---------------------------------------------------------------------------
// Linger
// ---------------------------------------------------------------------------

#[test]
fn linger_unsupported_off_linux() {
    for os in ["macos", "windows"] {
        for enable in [true, false] {
            let error = linger::command(os, enable).expect_err("linger is Linux only");
            assert!(
                error.to_string().starts_with("unsupported:"),
                "{os}: {error}"
            );
        }
    }
    let command = linger::command("linux", true).expect("linger on Linux");
    assert_eq!(command.get_program(), "loginctl");
    assert_eq!(
        command.get_args().next().and_then(|arg| arg.to_str()),
        Some("enable-linger")
    );
    let command = linger::command("linux", false).expect("linger on Linux");
    assert_eq!(
        command.get_args().next().and_then(|arg| arg.to_str()),
        Some("disable-linger")
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linger_runs_loginctl_and_returns_its_error() {
    let fake = FakeSystem::new();
    let output = fake
        .agentd_without_config(&["service", "enable-linger"])
        .env("USER", "r&d user")
        .output()
        .expect("run enable-linger");
    assert!(output.status.success(), "{}", text(&output));
    let output = fake
        .agentd_without_config(&["service", "disable-linger"])
        .env("USER", "r&d user")
        .output()
        .expect("run disable-linger");
    assert!(output.status.success(), "{}", text(&output));
    assert_eq!(
        fake.calls("loginctl"),
        ["enable-linger r&d user", "disable-linger r&d user"]
    );

    // polkit refuses: the error text reaches the caller.
    let output = fake
        .agentd_without_config(&["service", "enable-linger"])
        .env("USER", "r&d user")
        .env(
            "FAKE_LOGINCTL_FAIL",
            "Could not enable linger: Access denied",
        )
        .output()
        .expect("run enable-linger");
    assert!(!output.status.success(), "{}", text(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Could not enable linger: Access denied"),
        "{stderr}"
    );
}

// ---------------------------------------------------------------------------
// systemd installer, against fakes
// ---------------------------------------------------------------------------

/// A daemon directory with an installed runtime, a temporary
/// `XDG_CONFIG_HOME` and `HOME`, and fake `systemctl` and `loginctl` that
/// record their arguments. `PATH` holds only the fakes, so nothing reaches
/// the real user manager.
struct FakeSystem {
    _root: tempfile::TempDir,
    root: PathBuf,
    config: PathBuf,
    bin: PathBuf,
}

impl FakeSystem {
    fn new() -> Self {
        let root = temp_root("service-");
        let path = root.path().to_path_buf();
        let agentd = path.join("Loams Desktop 50% $x/agentd");
        std::fs::create_dir_all(&agentd).expect("create the daemon directory");
        let config = agentd.join("config.toml");
        std::fs::write(&config, "[daemon]\nmode = \"service\"\n").expect("write config.toml");
        let output = install_runtime(&bundle(&path, "0.2.0"), &agentd.join("runtime"));
        assert!(output.status.success(), "{}", text(&output));
        let bin = path.join("bin");
        std::fs::create_dir_all(&bin).expect("create the fake bin");
        for name in ["systemctl", "loginctl", "launchctl"] {
            write_fake(&bin, name);
        }
        std::fs::create_dir_all(path.join("home")).expect("create HOME");
        Self {
            _root: root,
            root: path,
            config,
            bin,
        }
    }

    fn agentd_without_config(&self, args: &[&str]) -> Command {
        let mut command = agentd();
        command
            .args(args)
            .env("PATH", &self.bin)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("xdg"))
            .env("FAKE_LOG_DIR", &self.root);
        command
    }

    fn agentd(&self, args: &[&str]) -> Command {
        let mut command = self.agentd_without_config(args);
        command.arg("--config").arg(&self.config);
        command
    }

    fn unit(&self) -> PathBuf {
        self.root.join("xdg/systemd/user/loams-agentd.service")
    }

    fn calls(&self, name: &str) -> Vec<String> {
        std::fs::read_to_string(self.root.join(format!("{name}.calls")))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

#[cfg(unix)]
fn write_fake(bin: &Path, name: &str) {
    use std::os::unix::fs::PermissionsExt;
    let upper = name.to_uppercase();
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_LOG_DIR/{name}.calls"
if [ -n "$FAKE_{upper}_FAIL" ]; then
  printf '%s\n' "$FAKE_{upper}_FAIL" >&2
  exit 1
fi
case "$*" in
  *is-active*) echo "${{FAKE_STATE:-active}}" ;;
  *is-enabled*) echo enabled ;;
esac
exit 0
"#
    );
    let path = bin.join(name);
    std::fs::write(&path, script).expect("write a fake");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod a fake");
}

#[cfg(not(unix))]
fn write_fake(_bin: &Path, _name: &str) {}

#[cfg(target_os = "linux")]
#[test]
fn systemd_install_uses_the_user_manager() {
    let fake = FakeSystem::new();
    let output = fake
        .agentd(&["service", "install"])
        .output()
        .expect("run service install");
    assert!(output.status.success(), "{}", text(&output));
    let unit = std::fs::read_to_string(fake.unit()).expect("read the unit");
    let agentd = fake.config.parent().expect("daemon directory");
    let expected = systemd::render_unit(&Launch {
        exe: agentd.join("runtime/current/loams-agentd"),
        config: fake.config.clone(),
        stdout_log: agentd.join("logs/agentd.stdout.log"),
    })
    .expect("render");
    assert_eq!(unit, expected);
    assert!(unit.contains("$$x"), "`$` is escaped: {unit}");
    assert_eq!(
        fake.calls("systemctl"),
        [
            "--user daemon-reload",
            "--user enable --now loams-agentd.service"
        ]
    );

    let output = fake
        .agentd(&["service", "status"])
        .output()
        .expect("run service status");
    assert!(output.status.success(), "{}", text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("installed: true"), "{stdout}");
    assert!(stdout.contains("state: active"), "{stdout}");

    for (action, call) in [
        ("stop", "--user stop loams-agentd.service"),
        ("start", "--user start loams-agentd.service"),
    ] {
        let output = fake
            .agentd(&["service", action])
            .output()
            .expect("run service");
        assert!(output.status.success(), "{}", text(&output));
        assert_eq!(
            fake.calls("systemctl").last().map(String::as_str),
            Some(call)
        );
    }

    let output = fake
        .agentd(&["service", "uninstall"])
        .output()
        .expect("run service uninstall");
    assert!(output.status.success(), "{}", text(&output));
    assert!(!fake.unit().exists(), "uninstall removes the unit");
    let calls = fake.calls("systemctl");
    assert_eq!(
        calls[calls.len() - 2..],
        [
            "--user disable --now loams-agentd.service",
            "--user daemon-reload"
        ]
    );

    // Uninstalling again is not an error.
    let output = fake
        .agentd(&["service", "uninstall"])
        .output()
        .expect("run service uninstall");
    assert!(output.status.success(), "{}", text(&output));

    let output = fake
        .agentd(&["service", "status"])
        .env("FAKE_STATE", "inactive")
        .output()
        .expect("run service status");
    assert!(output.status.success(), "{}", text(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("installed: false"), "{stdout}");
}

#[cfg(target_os = "linux")]
#[test]
fn systemd_install_reports_systemctl_errors() {
    let fake = FakeSystem::new();
    let output = fake
        .agentd(&["service", "install"])
        .env(
            "FAKE_SYSTEMCTL_FAIL",
            "Failed to connect to bus: No medium found",
        )
        .output()
        .expect("run service install");
    assert!(!output.status.success(), "{}", text(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("No medium found"), "{stderr}");
}

#[cfg(target_os = "linux")]
#[test]
fn service_install_needs_the_runtime_and_a_valid_config() {
    let fake = FakeSystem::new();
    let runtime = fake.config.parent().expect("dir").join("runtime");
    std::fs::remove_dir_all(&runtime).expect("remove the runtime");
    let output = fake
        .agentd(&["service", "install"])
        .output()
        .expect("run service install");
    assert!(!output.status.success(), "{}", text(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("install-runtime"), "{stderr}");
    assert!(!fake.unit().exists());
    assert!(fake.calls("systemctl").is_empty());

    std::fs::write(&fake.config, "[daemon]\nmode = 7\n").expect("write a bad config");
    let output = fake
        .agentd(&["service", "install"])
        .output()
        .expect("run service install");
    assert_eq!(output.status.code(), Some(4), "{}", text(&output));
    assert!(String::from_utf8_lossy(&output.stderr).contains("daemon.mode"));
}

/// Install into this user's real systemd manager, see it active, uninstall.
/// It replaces any installed `loams-agentd.service`, so it runs only when
/// `LOAMS_AGENTD_TEST_SYSTEMD=1` (the CI VM job; never a developer session).
#[cfg(target_os = "linux")]
#[test]
fn systemd_install_roundtrip() {
    if std::env::var_os("LOAMS_AGENTD_TEST_SYSTEMD").is_none_or(|value| value != "1") {
        eprintln!("skipped: set LOAMS_AGENTD_TEST_SYSTEMD=1 to install into the user manager");
        return;
    }
    let manager = Command::new("systemctl")
        .args(["--user", "is-system-running"])
        .output();
    let state = manager
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default();
    if !matches!(state.as_str(), "running" | "degraded") {
        eprintln!("skipped: needs a systemd user manager ({state:?})");
        return;
    }

    let root = temp_root("roundtrip-");
    let dir = root.path().join("Loams Desktop/agentd");
    std::fs::create_dir_all(&dir).expect("create the daemon directory");
    let config = dir.join("config.toml");
    std::fs::write(&config, "[daemon]\nmode = \"service\"\n").expect("write config.toml");
    // The bundle holds the real daemon.
    let from = root.path().join("bundle");
    std::fs::create_dir_all(&from).expect("create the bundle");
    let mut sums = String::new();
    for (name, source) in [
        (
            "loams-agentd",
            PathBuf::from(env!("CARGO_BIN_EXE_loams-agentd")),
        ),
        ("loams", PathBuf::from(env!("CARGO_BIN_EXE_loams-agentd"))),
    ] {
        let bytes = std::fs::read(&source).expect("read the daemon binary");
        std::fs::write(from.join(name), &bytes).expect("write a binary");
        sums.push_str(&format!(
            "{}  {name}\n",
            hex::encode(Sha256::digest(&bytes))
        ));
    }
    std::fs::write(from.join("SHA256SUMS"), sums).expect("write sums");
    std::fs::write(from.join("VERSION"), "0.0.1\n").expect("write VERSION");
    let output = install_runtime(&from, &dir.join("runtime"));
    assert!(output.status.success(), "{}", text(&output));

    let service = |action: &str| {
        agentd()
            .args(["service", action, "--config"])
            .arg(&config)
            .output()
            .expect("run service")
    };
    let output = service("install");
    assert!(output.status.success(), "{}", text(&output));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let output = service("status");
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        if stdout.contains("state: active") && dir.join("agentd.json").is_file() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the service never became active: {stdout}"
        );
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    let output = service("uninstall");
    assert!(output.status.success(), "{}", text(&output));
    let unit_dir = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").expect("HOME")).join(".config"));
    assert!(!unit_dir.join("systemd/user/loams-agentd.service").exists());
    let output = service("status");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("installed: false"),
        "{}",
        text(&output)
    );
}

// ---------------------------------------------------------------------------
// The Windows parent (`run --service` spawns `run --worker`)
// ---------------------------------------------------------------------------

/// The parent's loop with stand-in workers: `exit_codes` are the workers'
/// exits in order. Returns the parent's exit code and how many it spawned.
fn supervise(exit_codes: &[i32]) -> (u8, usize) {
    let mut spawned = 0;
    let mut slept = Vec::new();
    let code = windows::supervise_with(
        || {
            let code = exit_codes[spawned.min(exit_codes.len() - 1)];
            spawned += 1;
            exit_with(code).spawn()
        },
        |delay| slept.push(delay),
        |_line| {},
    );
    assert_eq!(slept.len(), spawned - 1, "a sleep before every restart");
    (code, spawned)
}

#[cfg(windows)]
fn exit_with(code: i32) -> Command {
    let mut command = Command::new("cmd");
    command.args(["/C", &format!("exit {code}")]);
    command
}

#[cfg(not(windows))]
fn exit_with(code: i32) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", &format!("exit {code}")]);
    command
}

#[cfg(windows)]
#[test]
fn windows_parent_restarts_worker() {
    assert_eq!(supervise(&[70, 0]), (0, 2), "70 is restarted");
    assert_eq!(supervise(&[4]), (4, 1), "4 is not");
    assert_eq!(supervise(&[3]), (3, 1), "3 is not");
}

/// The same loop on the other platforms (the Job Object is Windows-only).
#[test]
fn parent_restarts_worker_on_crash_only() {
    assert_eq!(supervise(&[70, 0]), (0, 2), "70 is restarted");
    assert_eq!(supervise(&[4]), (4, 1), "4 is not");
    assert_eq!(supervise(&[3]), (3, 1), "3 is not");
    assert_eq!(supervise(&[0]), (0, 1), "0 is not");
    // Five restarts in ten minutes, then the parent gives up with 70.
    assert_eq!(supervise(&[70]), (70, 6));
}
