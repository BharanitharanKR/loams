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
pub mod compose;
pub mod door;
pub mod errors;
pub mod helpers;
pub mod history;
pub mod install;
pub mod path;
pub mod rollback;
pub mod run;
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
    /// On a failure after the snapshot, leave the stack stopped with the
    /// door closed instead of rolling back.
    #[arg(long)]
    pub no_auto_rollback: bool,
    /// Keep the earlier snapshots after this upgrade verifies.
    #[arg(long)]
    pub keep_snapshot: bool,
}

/// `release/companions.toml`, compiled in: the helper images' pins.
const COMPANIONS: &str = include_str!("../../../release/companions.toml");

/// BR's image by digest, from `release/companions.toml`.
pub fn br_image() -> String {
    let parsed: toml::Value = toml::from_str(COMPANIONS).unwrap_or(toml::Value::Integer(0));
    let br = &parsed["br"];
    format!(
        "{}@{}",
        br["image"].as_str().unwrap_or("docker.io/pingcap/br"),
        br["digest"].as_str().unwrap_or_default()
    )
}

/// `.env` settings that are not secrets and that messages name.
const PLAIN_ENV: [&str; 8] = [
    "LOAMS_VERSION",
    "LOAMS_REGISTRY",
    "TIKV_TAG",
    "TIDB_IMAGE",
    "COMPOSE_PROFILES",
    "LOAMS_UPGRADE_SNAPSHOT_DIR",
    "LOAMS_POSTGRES_COMPOSE",
    "RUST_LOG",
];

/// `text` with the stack's `.env` values replaced by `<redacted>`: every
/// value of a secret-looking name (`KEY`, `SECRET`, `TOKEN`, `PASS`) of
/// four characters or more, and every other value of eight or more but
/// those of [`PLAIN_ENV`]. Nothing `upgrade` prints carries one.
pub fn redact(text: &str, env: &std::collections::BTreeMap<String, String>) -> String {
    let secret_name = |k: &str| {
        ["KEY", "SECRET", "TOKEN", "PASS", "CREDENTIAL"]
            .iter()
            .any(|m| k.to_ascii_uppercase().contains(m))
    };
    let mut out = text.to_string();
    let mut values: Vec<&str> = env
        .iter()
        .map(|(k, v)| (k, v.trim_matches(['"', '\''])))
        .filter(|(k, v)| {
            (secret_name(k) && v.len() >= 4) || (v.len() >= 8 && !PLAIN_ENV.contains(&k.as_str()))
        })
        .map(|(_, v)| v)
        .collect();
    values.sort_by_key(|v| std::cmp::Reverse(v.len()));
    for v in values {
        out = out.replace(v, "<redacted>");
    }
    out
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
    /// Restore the stack from the last upgrade's snapshot and start the
    /// previous release (or, with --keep-writes, swap the images back).
    Rollback {
        /// The snapshot to restore [default: the last upgrade's].
        #[arg(long)]
        snapshot: Option<String>,
        /// Keep the data and swap the images back: only when every migration
        /// applied was reversible and no third-party pin changed.
        #[arg(long)]
        keep_writes: bool,
    },
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
        Some(UpgradeCommand::Rollback {
            snapshot,
            keep_writes,
        }) => {
            let mut stack = stack;
            let opts = rollback::RollbackOptions {
                snapshot: snapshot.clone(),
                keep_writes: *keep_writes,
                br_image: br_image(),
            };
            let yes = args.common.yes;
            let report = rollback::rollback(runtime, &mut stack, &opts, &|q| confirmed(yes, q))?;
            serde_json::to_value(&report).map_err(|e| UpgradeError::new("internal", e.to_string()))
        }
        None => upgrade(args, runtime, stack, &current),
    }
}

/// `loams upgrade --to <version>`: one release step at a time (Q825), each
/// verified before the next; the walk stops at the first failure.
fn upgrade(
    args: &UpgradeArgs,
    runtime: &dyn Runtime,
    mut stack: StackDir,
    current: &str,
) -> Result<serde_json::Value> {
    let (first, target) = target_path(&args.common, current)?;
    // A resumed attempt keeps its own source.
    let at = history::History::of(&stack.dir)
        .open_attempt()
        .map_or_else(|| current.to_string(), |e| e[0].from.clone());
    let steps = if at == target {
        vec![target.clone()]
    } else {
        first.path.steps(&at, &target)?
    };
    if steps.is_empty() {
        return Err(UpgradeError::new(
            "release_not_found",
            format!("the stack is already at {target}"),
        ));
    }
    if steps.len() > 1 && args.common.bundle.is_some() {
        return Err(UpgradeError::new(
            "upgrade_step_not_adjacent",
            format!(
                "a local bundle holds one release; upgrade one step at a time: {}",
                steps.join(" -> ")
            ),
        ));
    }
    let question = format!(
        "Upgrade the stack in {} from {at} to {target} ({})? It stops the stack for the upgrade.",
        stack.dir.display(),
        steps.join(" -> ")
    );
    if !confirmed(args.common.yes, &question) {
        return Err(UpgradeError::new(
            "confirmation_required",
            "an upgrade stops the stack: pass --yes",
        ));
    }
    let opts = run::Options {
        no_auto_rollback: args.no_auto_rollback,
        keep_snapshot: args.keep_snapshot,
        br_image: br_image(),
    };
    let reports = upgrade_chain(runtime, &mut stack, &at, &steps, &opts, |version| {
        if *version == target {
            Ok(first.clone())
        } else {
            let work = std::env::temp_dir().join(format!("loams-upgrade-{version}"));
            bundle::Bundle::download(version, &work)
        }
    })?;
    serde_json::to_value(serde_json::json!({ "upgraded": reports }))
        .map_err(|e| UpgradeError::new("internal", e.to_string()))
}

/// Walks `steps` (releases after `from`), one verified step at a time, with
/// each release's bundle from `bundle_of`; stops at the first failure. Every
/// step's snapshot is kept until the last step verifies (§7.3).
pub fn upgrade_chain(
    runtime: &dyn Runtime,
    stack: &mut StackDir,
    from: &str,
    steps: &[String],
    opts: &run::Options,
    mut bundle_of: impl FnMut(&String) -> Result<bundle::Bundle>,
) -> Result<Vec<run::UpgradeReport>> {
    let mut reports = Vec::new();
    let mut from = from.to_string();
    for (i, version) in steps.iter().enumerate() {
        let bundle = bundle_of(version)?;
        let step_opts = run::Options {
            keep_snapshot: opts.keep_snapshot || i + 1 < steps.len(),
            ..opts.clone()
        };
        let report = run::upgrade_step(runtime, stack, &bundle, &from, version, &step_opts)?;
        from.clone_from(version);
        reports.push(report);
    }
    Ok(reports)
}

/// [`run_with`], with the stack's `.env` values taken out of the output and
/// the error (`upgrade_output_has_no_env_values`).
pub fn run_redacted(args: &UpgradeArgs, runtime: &dyn Runtime) -> Result<serde_json::Value> {
    let result = run_with(args, runtime);
    let env = stack_of(&args.common).map(|s| s.env).unwrap_or_default();
    match result {
        Ok(value) => Ok(serde_json::from_str(&redact(&value.to_string(), &env)).unwrap_or(value)),
        Err(mut err) => {
            err.message = redact(&err.message, &env);
            Err(err)
        }
    }
}

/// `loams upgrade …` on the real container runtime.
pub fn run(args: &UpgradeArgs) -> ExitCode {
    let result = runtime::Cli::detect().and_then(|cli| run_redacted(args, &cli));
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
