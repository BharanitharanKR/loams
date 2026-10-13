//! `serve --remote` and `[remote]` (plan SD1 Task 7): the remote listener is
//! off unless asked for, and a missing flag exits 4 naming it.

use std::process::Command;

use loams_agentd::config::Config;
use loams_agentd::remote::{self, RemoteFlags};

fn agentd() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loams-agentd"));
    command
        .env_remove("LOAMS_AGENTD_CONFIG")
        .env_remove(remote::TEST_ISSUER_ENV);
    command
}

/// Every flag of the Shared contracts' command line, with values.
fn flags(config: &str) -> Vec<(&'static str, String)> {
    vec![
        ("--listen", "127.0.0.1:0".into()),
        ("--tls-cert", "/nonexistent/cert.pem".into()),
        ("--tls-key", "/nonexistent/key.pem".into()),
        (
            "--issuer",
            "https://auth.example.com/application/o/loams/".into(),
        ),
        ("--audience", "loams-agentd".into()),
        ("--subject", "s1".into()),
        ("--allow-origin", "https://console.example.com".into()),
        ("--config", config.into()),
    ]
}

fn serve(args: &[(&str, String)], remote: bool) -> std::process::Output {
    let mut command = agentd();
    command.arg("serve");
    if remote {
        command.arg("--remote");
    }
    for (flag, value) in args {
        command.arg(flag).arg(value);
    }
    command.output().expect("run loams-agentd")
}

#[test]
fn remote_off_by_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");

    // An empty config: no remote listener.
    std::fs::write(&path, "").expect("write");
    let config = Config::load(&path).expect("load");
    assert!(!config.remote.enabled);
    assert_eq!(remote::resolve(None, &config.remote), Ok(None));

    // Every key but `enabled`: still off.
    std::fs::write(
        &path,
        r#"
        [remote]
        listen = "0.0.0.0:8443"
        tls_cert = "/c.pem"
        tls_key = "/k.pem"
        issuer = "https://auth.example.com/application/o/loams/"
        audience = "loams-agentd"
        subject = "s1"
        allow_origins = ["https://console.example.com"]
        "#,
    )
    .expect("write");
    let config = Config::load(&path).expect("load");
    assert_eq!(remote::resolve(None, &config.remote), Ok(None));

    // `run` takes no remote flag at all.
    let output = agentd()
        .args(["run", "--remote", "--config"])
        .arg(&path)
        .output()
        .expect("run loams-agentd");
    assert_eq!(output.status.code(), Some(2), "{output:?}");

    // `serve` without `--remote` does not serve at all.
    let all = flags(path.to_str().expect("utf-8"));
    let output = serve(&all, false);
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--remote"),
        "{output:?}"
    );

    // Flags and `[remote]` only: nothing else turns it on.
    assert_eq!(
        RemoteFlags::default().settings(),
        Err(remote::FlagError::Missing("--remote"))
    );
}

#[test]
fn remote_missing_flag_exits_4() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "").expect("write");
    let all = flags(path.to_str().expect("utf-8"));
    for (missing, _) in &all {
        if *missing == "--allow-origin" {
            continue;
        }
        let args: Vec<_> = all
            .iter()
            .filter(|(flag, _)| flag != missing)
            .cloned()
            .collect();
        let output = serve(&args, true);
        assert_eq!(
            output.status.code(),
            Some(4),
            "without {missing}: {output:?}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(missing), "without {missing}: {stderr}");
    }

    // Every flag, but the certificate cannot be read: also exit 4, naming
    // the flag, before the daemon takes its lock or opens a port.
    let output = serve(&all, true);
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--tls-cert"), "{stderr}");
    assert!(
        !dir.path().join("agentd.lock").exists() && !dir.path().join("agentd.json").exists(),
        "nothing started"
    );

    // An invalid origin names its flag.
    let mut args = all.clone();
    args.push(("--allow-origin", "*".into()));
    let output = serve(&args, true);
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("--allow-origin"));
}
