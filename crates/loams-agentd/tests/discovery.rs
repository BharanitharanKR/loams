//! Discovery, the token and `Hello` (design §50 §5.4 and §7, plan DD1 Task 6).
#![allow(clippy::unwrap_used)]

mod support;

use loams_agentd::discovery;
use loams_agentd::paths::Paths;
use loams_agentd_proto::daemon::{DaemonMode, Discovery};
use loams_agentd_proto::rpc::{AGENTD_PROTOCOL, PROTOCOL_MISMATCH, Role};
use loams_agentd_rpc::{RpcError, connect_ws_with_token, methods};
use support::{Daemon, Home, block_on, hello_params};

const CHILD: &str = "[daemon]\nmode = \"child\"\n";

/// The version the binary reports: `LOAMS_AGENTD_VERSION` at build time, else
/// the crate version (plan DD1 ruling T0-10b).
const VERSION: &str = match option_env!("LOAMS_AGENTD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

#[test]
fn discovery_written_after_bind_and_removed_on_exit() {
    let home = Home::new(CHILD);
    let daemon = Daemon::start(&home, &["--child"]);
    let published = daemon.discovery.clone();
    assert_eq!(published.protocol, AGENTD_PROTOCOL);
    assert_eq!(published.version, VERSION);
    assert_eq!(published.pid, daemon.pid);
    assert_eq!(published.mode, DaemonMode::Child);
    assert_ne!(published.port, 0);
    // The listener was bound before the file was written: it answers now,
    // and it is this daemon's.
    std::net::TcpStream::connect(("127.0.0.1", published.port)).expect("port is listening");
    assert_eq!(daemon.hello(AGENTD_PROTOCOL).unwrap().pid, daemon.pid);
    assert_eq!(
        discovery::read(&Paths::at(&home.dir)),
        Some(published),
        "a live daemon's file is read back"
    );

    assert_eq!(daemon.stop().code(), Some(0));
    assert!(
        !home.file("agentd.json").exists(),
        "discovery left behind after a clean exit"
    );
    assert_eq!(discovery::read(&Paths::at(&home.dir)), None);
}

#[cfg(unix)]
#[test]
fn sigterm_is_a_clean_exit() {
    let home = Home::new(CHILD);
    let daemon = Daemon::start(&home, &["--child"]);
    assert_eq!(daemon.terminate().code(), Some(0));
    assert!(!home.file("agentd.json").exists());
}

#[test]
fn token_file_is_owner_only() {
    let home = Home::new(CHILD);
    let daemon = Daemon::start(&home, &["--child"]);
    let token = std::fs::read_to_string(home.file("agentd.token")).unwrap();
    // 32 random bytes, base64url without padding.
    assert_eq!(token.len(), 43, "{token:?}");
    assert!(
        token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "not base64url"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |name: &str| {
            std::fs::metadata(home.file(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode("agentd.token"), 0o600, "token");
        assert_eq!(mode("agentd.json"), 0o600, "discovery");
        let dir = std::fs::metadata(&home.dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir, 0o700, "daemon directory");
    }
    #[cfg(windows)]
    {
        let owner = loams_agentd::owner_only::current_user_sid().unwrap();
        for path in [
            home.file("agentd.token"),
            home.file("agentd.json"),
            home.dir.clone(),
        ] {
            let mut sids = loams_agentd::owner_only::dacl_sids(&path).unwrap();
            sids.sort();
            let mut expected = vec![owner.clone(), "S-1-5-18".to_string()];
            expected.sort();
            assert_eq!(sids, expected, "{}", path.display());
        }
    }

    assert_eq!(daemon.stop().code(), Some(0));
    assert!(
        !home.output().contains(&token),
        "the token reached the daemon's output or logs"
    );
}

#[test]
fn token_rotates_every_start() {
    let home = Home::new(CHILD);
    let first = Daemon::start(&home, &["--child"]);
    let before = std::fs::read_to_string(home.file("agentd.token")).unwrap();
    assert_eq!(first.stop().code(), Some(0));

    let second = Daemon::start(&home, &["--child"]);
    let after = std::fs::read_to_string(home.file("agentd.token")).unwrap();
    assert_ne!(before, after, "the token was reused across starts");
    assert_eq!(second.stop().code(), Some(0));
}

#[test]
fn hello_reports_versions_and_mode() {
    // `--child` wins over the config's mode.
    let home = Home::new("[daemon]\nmode = \"service\"\n");
    let daemon = Daemon::start(&home, &["--child"]);
    let reply = daemon.hello(AGENTD_PROTOCOL).unwrap();
    assert_eq!(reply.protocol, AGENTD_PROTOCOL);
    assert_eq!(reply.version, VERSION);
    assert!(!reply.build_sha.is_empty());
    assert_eq!(reply.mode, DaemonMode::Child);
    assert_eq!(reply.pid, daemon.pid);
    assert_eq!(reply.started_at_ms, daemon.discovery.started_at_ms);
    assert_eq!(reply.role, Role::Owner);
    assert_eq!(daemon.stop().code(), Some(0));

    // Without a flag the config's mode is used.
    let daemon = Daemon::start(&home, &[]);
    let reply = daemon.hello(AGENTD_PROTOCOL).unwrap();
    assert_eq!(reply.mode, DaemonMode::Service);
    assert_eq!(daemon.discovery.mode, DaemonMode::Service);
    assert_eq!(daemon.stop().code(), Some(0));
}

#[test]
fn protocol_mismatch_closes() {
    let home = Home::new(CHILD);
    let daemon = Daemon::start(&home, &["--child"]);
    let url = daemon.url();
    let token = daemon.token.clone();
    block_on(async move {
        let client = connect_ws_with_token(&url, &token).await.unwrap();
        let err = client
            .call(methods::HELLO, hello_params(AGENTD_PROTOCOL + 1))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, RpcError::Failed(text) if text.starts_with(PROTOCOL_MISMATCH)),
            "{err:?}"
        );
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.call(methods::HELLO, hello_params(AGENTD_PROTOCOL)),
        )
        .await
        .expect("a closed connection fails at once");
        assert!(next.is_err(), "the connection stayed open: {next:?}");
    });
    // Other connections are unaffected.
    assert!(daemon.hello(AGENTD_PROTOCOL).is_ok());
    assert_eq!(daemon.stop().code(), Some(0));
}

#[test]
fn stale_discovery_is_ignored() {
    let home = Home::new(CHILD);
    // A daemon that crashed: its pid is gone and nothing serves its port.
    let mut gone = std::process::Command::new(env!("CARGO_BIN_EXE_loams-agentd"))
        .arg("version")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let dead_pid = gone.id();
    gone.wait().unwrap();
    let dead_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let stale = Discovery {
        protocol: AGENTD_PROTOCOL,
        version: "0.0.0-stale".into(),
        pid: dead_pid,
        port: dead_port,
        started_at_ms: 1,
        mode: DaemonMode::Service,
    };
    std::fs::write(
        home.file("agentd.json"),
        serde_json::to_vec(&stale).unwrap(),
    )
    .unwrap();
    assert_eq!(
        discovery::read(&Paths::at(&home.dir)),
        None,
        "stale file read"
    );

    let daemon = Daemon::start(&home, &["--child"]);
    assert_eq!(daemon.discovery.mode, DaemonMode::Child);
    assert_eq!(daemon.hello(AGENTD_PROTOCOL).unwrap().pid, daemon.pid);
    assert_eq!(daemon.stop().code(), Some(0));
}
