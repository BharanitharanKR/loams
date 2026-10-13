//! Starts real `loams-agentd run` processes for the process-model tests
//! (plan DD1 Task 6). Each test gets its own data directory under Cargo's
//! target-relative temp root (never `/tmp`), its own `HOME`, and the mock
//! harness, and captures the daemon's stdout and stderr in files.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use loams_agentd_proto::daemon::Discovery;
use loams_agentd_proto::rpc::{AGENTD_PROTOCOL, HelloReply};
use loams_agentd_rpc::{RpcClient, RpcError, connect_ws_with_token, methods};

/// How long a daemon may take to assemble its engine and publish discovery.
const START_TIMEOUT: Duration = Duration::from_secs(90);
/// How long a stopping daemon may take to drain and exit.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// A daemon directory (`<root>/agentd`) with its `config.toml`.
pub struct Home {
    _root: tempfile::TempDir,
    pub root: PathBuf,
    pub dir: PathBuf,
    pub config: PathBuf,
}

impl Home {
    pub fn new(config: &str) -> Self {
        let root = tempfile::Builder::new()
            .prefix("agentd-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("temp root under the target directory");
        let dir = root.path().join("agentd");
        std::fs::create_dir_all(&dir).expect("create the daemon directory");
        std::fs::create_dir_all(root.path().join("home")).expect("create HOME");
        let config_path = dir.join("config.toml");
        std::fs::write(&config_path, config).expect("write config.toml");
        Self {
            root: root.path().to_path_buf(),
            dir,
            config: config_path,
            _root: root,
        }
    }

    pub fn file(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// The main token the running daemon wrote.
    pub fn token(&self) -> String {
        std::fs::read_to_string(self.file("agentd.token")).expect("read agentd.token")
    }

    /// `loams-agentd <args> --config <config>`, isolated from the developer's
    /// environment, with stdout and stderr appended to files under the root.
    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_loams-agentd"));
        let home = self.root.join("home");
        command
            .args(args)
            .arg("--config")
            .arg(&self.config)
            .env_remove("LOAMS_AGENTD_CONFIG")
            .env_remove("RUST_LOG")
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("LOAMS_DESKTOP_HARNESS", "mock")
            .stdin(Stdio::null())
            .stdout(self.capture("stdout.log"))
            .stderr(self.capture("stderr.log"));
        command
    }

    fn capture(&self, name: &str) -> Stdio {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(name))
            .expect("open capture file")
            .into()
    }

    /// Everything the daemons of this home printed or logged.
    pub fn output(&self) -> String {
        let mut text = String::new();
        for path in [self.root.join("stdout.log"), self.root.join("stderr.log")]
            .into_iter()
            .chain(log_files(&self.dir.join("logs")))
        {
            text.push_str(&std::fs::read_to_string(path).unwrap_or_default());
        }
        text
    }

    pub fn stderr(&self) -> String {
        std::fs::read_to_string(self.root.join("stderr.log")).unwrap_or_default()
    }

    /// The raw discovery file, without the library's staleness check.
    pub fn discovery_file(&self) -> Option<Discovery> {
        let text = std::fs::read(self.file("agentd.json")).ok()?;
        serde_json::from_slice(&text).ok()
    }

    /// Run a command that is expected to exit on its own.
    pub fn run_to_exit(&self, args: &[&str]) -> ExitStatus {
        let child = self.command(args).spawn().expect("spawn loams-agentd");
        wait_for_exit(child, STOP_TIMEOUT)
    }
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default()
}

pub fn wait_for_exit(mut child: Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll loams-agentd") {
            return status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "loams-agentd (pid {}) did not exit in {timeout:?}",
                child.id()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A running daemon. Dropping it kills the process.
pub struct Daemon {
    child: Option<Child>,
    pub pid: u32,
    pub discovery: Discovery,
    /// The main token this daemon wrote at start.
    pub token: String,
}

impl Daemon {
    /// `run <args>` and wait until it has published its own discovery file.
    pub fn start(home: &Home, args: &[&str]) -> Self {
        let mut run_args = vec!["run"];
        run_args.extend_from_slice(args);
        let mut child = home.command(&run_args).spawn().expect("spawn loams-agentd");
        let pid = child.id();
        let deadline = Instant::now() + START_TIMEOUT;
        loop {
            if let Some(discovery) = home.discovery_file().filter(|d| d.pid == pid) {
                return Self {
                    child: Some(child),
                    pid,
                    discovery,
                    token: home.token(),
                };
            }
            if let Some(status) = child.try_wait().expect("poll loams-agentd") {
                panic!(
                    "loams-agentd exited before publishing discovery ({status}):\n{}",
                    home.output()
                );
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("no discovery after {START_TIMEOUT:?}:\n{}", home.output());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.discovery.port)
    }

    /// `Hello` on a fresh connection with the main token.
    pub fn hello(&self, protocol: u32) -> Result<HelloReply, RpcError> {
        let url = self.url();
        let token = self.token.clone();
        block_on(async move {
            let client = connect_ws_with_token(&url, &token).await?;
            client.call_as(methods::HELLO, hello_params(protocol)).await
        })
    }

    /// A connection with the main token that has said `Hello`.
    pub async fn owner(&self) -> Result<RpcClient, RpcError> {
        let client = connect_ws_with_token(&self.url(), &self.token).await?;
        client
            .call(methods::HELLO, hello_params(AGENTD_PROTOCOL))
            .await?;
        Ok(client)
    }

    /// Stop it over the RPC (`Shutdown`) and wait for it to exit.
    pub fn stop(mut self) -> ExitStatus {
        let reply = block_on(async {
            self.owner()
                .await?
                .call(methods::SHUTDOWN, serde_json::json!({}))
                .await
        });
        assert!(reply.is_ok(), "Shutdown failed: {reply:?}");
        wait_for_exit(self.child.take().expect("running"), STOP_TIMEOUT)
    }

    /// Send SIGTERM, as a service manager does, and wait for it to exit.
    #[cfg(unix)]
    pub fn terminate(mut self) -> ExitStatus {
        let child = self.child.take().expect("running");
        let pid = i32::try_from(child.id()).expect("pid fits i32");
        // SAFETY: kill(2) on a child this test spawned and has not reaped.
        #[allow(unsafe_code)]
        let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
        assert_eq!(rc, 0, "kill: {}", std::io::Error::last_os_error());
        wait_for_exit(child, STOP_TIMEOUT)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub fn hello_params(protocol: u32) -> serde_json::Value {
    serde_json::json!({ "client": "cli", "clientVersion": "test", "protocol": protocol })
}

pub fn current_protocol() -> u32 {
    AGENTD_PROTOCOL
}

pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}
