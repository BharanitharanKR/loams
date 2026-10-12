//! The command line (plan DD1 "Shared contracts").

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use loams_agentd_proto::daemon::DaemonMode;

/// Development override of `--config`: when set and non-empty it wins over
/// the flag, so a daemon that Loams Desktop starts can be pointed elsewhere.
pub const CONFIG_ENV: &str = "LOAMS_AGENTD_CONFIG";

#[derive(Debug, Parser)]
#[command(
    name = "loams-agentd",
    about = "Loams Desktop's per-user agent daemon",
    disable_version_flag = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the daemon in the foreground: the sessions engine, its harnesses
    /// and the loopback RPC server.
    Run(RunArgs),
    /// Serve the MCP (Model Context Protocol) server on stdin/stdout,
    /// proxying to the running daemon's RPC. Harness runs get it injected,
    /// with LOAMS_AGENTD_RUN_TOKEN, LOAMS_AGENTD_PORT and
    /// LOAMS_AGENTD_SESSION_ID set; without LOAMS_AGENTD_PORT the port is read
    /// from the discovery file beside --config. Logs go to stderr; stdout is
    /// the protocol.
    Mcp(ConfigArg),
    /// Show the daemon directory and whether a daemon is running there.
    Status(ConfigArg),
    /// Manage the per-user service: a systemd user unit (Linux), a
    /// LaunchAgent (macOS) or a Run entry (Windows).
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Copy the bundled binaries into the versioned runtime directory, check
    /// them against SHA256SUMS and switch `current` to them.
    InstallRuntime(InstallRuntimeArgs),
    /// Print the daemon's version.
    Version,
    /// Loams integration used by the Loams Bot harness (`loams bot-acp`).
    #[command(hide = true)]
    Loams {
        #[command(subcommand)]
        command: LoamsCommand,
    },
}

/// The link crate's CLI also has `login`, `logout`, `status`, `bot` and
/// `mock`; the daemon exposes only the entry point the Loams Bot harness
/// launches, so anything else is a usage error (plan DD1 ruling T1-2).
#[derive(Debug, Subcommand)]
pub enum LoamsCommand {
    /// Speak ACP on stdin/stdout as the Loams Bot agent.
    BotAcp,
}

#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    /// Install the service for this config and start it. The runtime must be
    /// installed first (`install-runtime`).
    Install(ConfigArg),
    /// Stop the service and remove it.
    Uninstall(ConfigArg),
    /// Start the installed service.
    Start(ConfigArg),
    /// Stop the service until the next login or `start`.
    Stop(ConfigArg),
    /// Whether the service is installed, and its state.
    Status(ConfigArg),
    /// Let the service start at boot and survive logout (Linux:
    /// `loginctl enable-linger`).
    EnableLinger,
    /// Undo `enable-linger`.
    DisableLinger,
}

#[derive(Debug, Args)]
pub struct InstallRuntimeArgs {
    /// The bundled binaries: `loams-agentd`, `loams`, `SHA256SUMS` and `VERSION`.
    #[arg(long, value_name = "DIR")]
    pub from: PathBuf,
    /// The runtime directory, `<userData>/agentd/runtime`.
    #[arg(long, value_name = "DIR")]
    pub to: PathBuf,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Run as the per-user service. On Windows this is the parent that runs
    /// and restarts the `--worker`.
    #[arg(long, conflicts_with_all = ["child", "worker"])]
    pub service: bool,
    /// Run as Loams Desktop's child process (background agents off).
    #[arg(long, conflicts_with = "worker")]
    pub child: bool,
    /// Run as the service's worker, under the Windows parent. Elsewhere it
    /// is the same as `--service`.
    #[arg(long)]
    pub worker: bool,
    #[command(flatten)]
    pub config: ConfigArg,
}

impl RunArgs {
    /// The mode the flags ask for; `None` leaves it to `config.toml`.
    pub fn mode(&self) -> Option<DaemonMode> {
        match (self.service || self.worker, self.child) {
            (true, _) => Some(DaemonMode::Service),
            (_, true) => Some(DaemonMode::Child),
            _ => None,
        }
    }
}

#[derive(Debug, Args)]
pub struct ConfigArg {
    /// The daemon's config.toml. The daemon keeps its other files (token,
    /// discovery, lock, store, logs) in the same directory.
    /// LOAMS_AGENTD_CONFIG overrides it.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
}

impl ConfigArg {
    /// `LOAMS_AGENTD_CONFIG` when set, else `--config`.
    pub fn resolve(&self) -> Option<PathBuf> {
        std::env::var_os(CONFIG_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| self.config.clone())
    }
}
