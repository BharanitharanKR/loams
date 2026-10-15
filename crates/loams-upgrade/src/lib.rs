//! `loams upgrade` (design §54 §10, D934; UP1 Tasks 12–14): upgrades a
//! self-hosted compose stack one release at a time, with a snapshot first
//! and `rollback` to undo it.
//!
//! This is a client: it reads the stack directory and drives `docker
//! compose` (or `podman compose`), and every step that touches data runs
//! `loams migrate …` in a helper container of the target image
//! ([`helpers`]). It links no storage crate (CL1's guard, second named
//! exception), which `upgrade_client_has_no_storage_dependencies` checks.

pub mod bundle;
pub mod check;
pub mod errors;
pub mod helpers;
pub mod path;
pub mod runtime;
pub mod snapshots;
pub mod stackdir;
pub mod status;

use std::io::IsTerminal as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};

use crate::errors::{Result, UpgradeError};
use crate::runtime::Runtime;
use crate::stackdir::StackDir;

/// `loams upgrade …`.
#[derive(Debug, Args)]
pub struct UpgradeArgs {
    #[command(subcommand)]
    pub command: Option<UpgradeCommand>,
    #[command(flatten)]
    pub common: Common,
}

/// Flags every form takes.
#[derive(Debug, Args, Clone)]
pub struct Common {
    /// The stack's directory [default: $LOAMS_STACK_DIR, else the current
    /// directory].
    #[arg(long, global = true)]
    pub dir: Option<PathBuf>,
    /// The target release, or `latest`.
    #[arg(long, global = true)]
    pub to: Option<String>,
    /// A local bundle (its `release` directory, or a checkout's
    /// `deploy/release`) instead of the downloaded one; unverified.
    #[arg(long, global = true)]
    pub bundle: Option<PathBuf>,
    /// Do it without asking.
    #[arg(long, global = true)]
    pub yes: bool,
    #[arg(long, global = true, value_enum, default_value_t = Output::Json)]
    pub output: Output,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Output {
    Json,
    Text,
}

#[derive(Debug, Subcommand)]
pub enum UpgradeCommand {
    /// What an upgrade would do, and what would stop it (read-only).
    Check,
    /// The stack record and the ledger, and the last attempt (read-only).
    Status,
    /// The snapshots kept on this host.
    Snapshots {
        #[command(subcommand)]
        command: SnapshotsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SnapshotsCommand {
    List,
    /// Remove a snapshot: its GC root, then its files.
    Prune {
        #[arg(long)]
        id: String,
    },
}

/// Whether a mutating form may go on: `--yes`, or a typed `yes` on a TTY.
pub fn confirmed(yes: bool, question: &str) -> bool {
    if yes {
        return true;
    }
    if !std::io::stdin().is_terminal() {
        return false;
    }
    eprint!("{question} Type yes to go on: ");
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).is_ok() && answer.trim() == "yes"
}

fn stack_of(common: &Common) -> Result<StackDir> {
    let env_dir = std::env::var_os("LOAMS_STACK_DIR").map(PathBuf::from);
    let cwd = std::env::current_dir()
        .map_err(|e| UpgradeError::new("internal", format!("the current directory: {e}")))?;
    StackDir::discover(common.dir.as_deref(), env_dir.as_deref(), &cwd)
}

fn target_path(common: &Common, current: &str) -> Result<(bundle::Bundle, String)> {
    let bundle = match &common.bundle {
        Some(dir) => bundle::Bundle::local(dir)?,
        None => {
            let version = match common.to.as_deref() {
                Some(v) if v != "latest" => v.to_string(),
                _ => {
                    return Err(UpgradeError::new(
                        "release_not_found",
                        "name the release with --to (or pass --bundle)",
                    ));
                }
            };
            let work = std::env::temp_dir().join(format!("loams-upgrade-{version}"));
            bundle::Bundle::download(&version, &work)?
        }
    };
    let target = match common.to.as_deref() {
        Some(v) if v != "latest" => v.to_string(),
        _ => bundle
            .path
            .latest()
            .map(|r| r.version.clone())
            .ok_or_else(|| UpgradeError::new("manifest_invalid", "the release train is empty"))?,
    };
    let _ = current;
    Ok((bundle, target))
}

fn print_json(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

/// Runs one `loams upgrade` form with `runtime`.
pub fn run_with(args: &UpgradeArgs, runtime: &dyn Runtime) -> Result<serde_json::Value> {
    let stack = stack_of(&args.common)?;
    let current = stack.release().ok_or_else(|| {
        UpgradeError::new(
            "stack_unrecognised",
            "the stack's .env names no LOAMS_VERSION",
        )
    })?;
    match &args.command {
        Some(UpgradeCommand::Check) => {
            let (bundle, target) = target_path(&args.common, &current)?;
            let report = check::check(runtime, &stack, &bundle.path, &target)?;
            let mut value = serde_json::to_value(&report)
                .map_err(|e| UpgradeError::new("internal", e.to_string()))?;
            if !bundle.verified {
                value["bundle"] = serde_json::json!("local, unverified");
            }
            Ok(value)
        }
        Some(UpgradeCommand::Status) => {
            let report = status::status(runtime, &stack, &current)?;
            serde_json::to_value(&report).map_err(|e| UpgradeError::new("internal", e.to_string()))
        }
        Some(UpgradeCommand::Snapshots { command }) => {
            let image = bundle::loams_image(&stack.env, &current);
            match command {
                SnapshotsCommand::List => snapshots::list(runtime, &stack, &image),
                SnapshotsCommand::Prune { id } => {
                    let ok = confirmed(args.common.yes, &format!("Prune snapshot {id}?"));
                    snapshots::prune(runtime, &stack, &image, id, ok)?;
                    Ok(serde_json::json!({"pruned": id}))
                }
            }
        }
        None => Err(UpgradeError::new(
            "usage",
            "`loams upgrade --to <version>` arrives with UP1 Task 13; use `check`",
        )),
    }
}

/// `loams upgrade …` on the real container runtime.
pub fn run(args: &UpgradeArgs) -> ExitCode {
    let result = runtime::Cli::detect().and_then(|cli| run_with(args, &cli));
    match result {
        Ok(value) => {
            match args.common.output {
                Output::Json => print_json(&value),
                Output::Text => println!("{value}"),
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            match args.common.output {
                Output::Json => print_json(&err.to_json()),
                Output::Text => eprintln!("loams upgrade: {err}"),
            }
            ExitCode::from(err.exit_code())
        }
    }
}
