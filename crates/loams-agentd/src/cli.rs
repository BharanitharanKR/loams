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
    /// proxying to the running daemon's RPC. Harness runs get it injected.
    /// Logs go to stderr; stdout is the protocol.
    Mcp,
    /// Show the daemon directory and whether a daemon is running there.
    Status(ConfigArg),
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

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Run as the per-user service.
    #[arg(long, conflicts_with = "child")]
    pub service: bool,
    /// Run as Loams Desktop's child process (background agents off).
    #[arg(long)]
    pub child: bool,
    #[command(flatten)]
    pub config: ConfigArg,
}

impl RunArgs {
    /// The mode the flags ask for; `None` leaves it to `config.toml`.
    pub fn mode(&self) -> Option<DaemonMode> {
        match (self.service, self.child) {
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
