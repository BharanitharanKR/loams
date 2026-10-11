//! Single instance and exit codes (design §50 §5.1 and §5.4, plan DD1 Task 6).
#![allow(clippy::unwrap_used)]

mod support;

use loams_agentd_proto::rpc::AGENTD_PROTOCOL;
use support::{Daemon, Home};

const CHILD: &str = "[daemon]\nmode = \"child\"\n";

#[test]
fn second_instance_exits_3() {
    let home = Home::new(CHILD);
    let first = Daemon::start(&home, &["--child"]);
    let token = std::fs::read_to_string(home.file("agentd.token")).unwrap();

    let second = home.run_to_exit(&["run", "--child"]);
    assert_eq!(second.code(), Some(3), "{}", home.output());
    let message = format!("already running (pid {})", first.pid);
    assert!(
        home.stderr().contains(&message),
        "expected {message:?} in:\n{}",
        home.stderr()
    );

    // The first daemon is untouched: same discovery, same token, still serving.
    assert_eq!(home.discovery_file(), Some(first.discovery.clone()));
    assert_eq!(
        std::fs::read_to_string(home.file("agentd.token")).unwrap(),
        token
    );
    assert!(first.hello(AGENTD_PROTOCOL).is_ok());
    assert_eq!(first.stop().code(), Some(0));
}

#[test]
fn invalid_config_exits_4() {
    for (config, field) in [
        ("[daemon]\nmode = \"sideways\"\n", "daemon.mode"),
        ("[engine]\nauto_start = \"yes\"\n", "engine.auto_start"),
        (
            "[tool_outputs]\nbudget_bytes = 0\n",
            "tool_outputs.budget_bytes",
        ),
        ("[daemon\nmode = \"child\"\n", "line 1"),
    ] {
        let home = Home::new(config);
        let status = home.run_to_exit(&["run", "--child"]);
        assert_eq!(status.code(), Some(4), "{config:?}: {}", home.output());
        assert!(
            home.stderr().contains(field),
            "{config:?}: {field:?} not named in:\n{}",
            home.stderr()
        );
        for name in ["agentd.json", "agentd.token"] {
            assert!(!home.file(name).exists(), "{config:?} wrote {name}");
        }
    }

    // A config that cannot be read is invalid too.
    let home = Home::new(CHILD);
    std::fs::remove_file(&home.config).unwrap();
    let status = home.run_to_exit(&["run", "--child"]);
    assert_eq!(status.code(), Some(4), "{}", home.output());
    assert!(home.stderr().contains("config.toml"), "{}", home.stderr());

    // No config at all is invalid too.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_loams-agentd"))
        .args(["run", "--child"])
        .env_remove("LOAMS_AGENTD_CONFIG")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("--config"));
}

#[test]
fn status_reports_the_running_daemon() {
    let home = Home::new(CHILD);
    let status = |home: &Home| {
        let output = home
            .command(&["status"])
            .stdout(std::process::Stdio::piped())
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    };
    assert!(status(&home).contains("not running"));
    let daemon = Daemon::start(&home, &["--child"]);
    let running = status(&home);
    assert!(
        running.contains(&format!("running (pid {}", daemon.pid)),
        "{running}"
    );
    assert_eq!(daemon.stop().code(), Some(0));
    assert!(status(&home).contains("not running"));
}
