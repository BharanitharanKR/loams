//! The compose calls an upgrade makes on a stack directory, over
//! [`Runtime`].

use std::path::Path;

use crate::errors::{Result, UpgradeError};
use crate::runtime::{Output, Runtime};

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

fn checked(out: Output, what: &str) -> Result<Output> {
    if out.ok() {
        Ok(out)
    } else {
        let last = out
            .stderr
            .lines()
            .last()
            .unwrap_or_default()
            .trim()
            .to_string();
        Err(UpgradeError::new(
            "stack_unhealthy",
            format!("compose {what} failed (exit {}): {last}", out.status),
        ))
    }
}

/// The resolved compose file of `dir` (its `.env` and profiles applied).
pub fn config(runtime: &dyn Runtime, dir: &Path) -> Result<serde_json::Value> {
    let out = checked(
        runtime.compose(dir, &strings(&["config", "--format", "json"]))?,
        "config",
    )?;
    serde_json::from_str(out.stdout.trim())
        .map_err(|e| UpgradeError::new("manifest_invalid", format!("compose config: {e}")))
}

/// The services `config` enables.
pub fn services(config: &serde_json::Value) -> Vec<String> {
    config["services"]
        .as_object()
        .map(|s| s.keys().cloned().collect())
        .unwrap_or_default()
}

/// `want`, in its order, kept to the services the stack enables.
pub fn present(enabled: &[String], want: &[&str]) -> Vec<String> {
    want.iter()
        .filter(|w| enabled.iter().any(|e| e == *w))
        .map(|w| (*w).to_string())
        .collect()
}

/// `compose stop <services>` (nothing to do for none).
pub fn stop(runtime: &dyn Runtime, dir: &Path, services: &[String]) -> Result<()> {
    if services.is_empty() {
        return Ok(());
    }
    let mut args = strings(&["stop"]);
    args.extend(services.iter().cloned());
    checked(runtime.compose(dir, &args)?, "stop").map(drop)
}

/// `compose up -d --wait <services>`: started, and healthy where a
/// healthcheck exists.
pub fn up(runtime: &dyn Runtime, dir: &Path, services: &[String]) -> Result<()> {
    if services.is_empty() {
        return Ok(());
    }
    let mut args = strings(&["up", "-d", "--wait"]);
    args.extend(services.iter().cloned());
    checked(runtime.compose(dir, &args)?, "up").map(drop)
}

/// `compose up -d --wait` of every enabled service.
pub fn up_all(runtime: &dyn Runtime, dir: &Path) -> Result<()> {
    checked(
        runtime.compose(dir, &strings(&["up", "-d", "--wait"]))?,
        "up",
    )
    .map(drop)
}

/// One container of `compose ps -a`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PsRow {
    pub service: String,
    pub state: String,
    pub health: String,
    pub exit_code: i64,
}

/// `compose ps -a --format json` (one object per line, or one array).
pub fn ps(runtime: &dyn Runtime, dir: &Path) -> Result<Vec<PsRow>> {
    let out = checked(
        runtime.compose(dir, &strings(&["ps", "-a", "--format", "json"]))?,
        "ps",
    )?;
    let rows: Vec<serde_json::Value> =
        match serde_json::from_str::<serde_json::Value>(out.stdout.trim()) {
            Ok(serde_json::Value::Array(rows)) => rows,
            Ok(row) => vec![row],
            Err(_) => out
                .stdout
                .lines()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect(),
        };
    Ok(rows
        .iter()
        .map(|row| PsRow {
            service: row["Service"].as_str().unwrap_or_default().to_string(),
            state: row["State"].as_str().unwrap_or_default().to_string(),
            health: row["Health"].as_str().unwrap_or_default().to_string(),
            exit_code: row["ExitCode"].as_i64().unwrap_or(0),
        })
        .collect())
}

/// The services not running, or not healthy (a one-shot service that exited
/// 0 is fine), sorted.
pub fn unhealthy(rows: &[PsRow]) -> Vec<String> {
    let mut bad: Vec<String> = rows
        .iter()
        .filter(|r| match r.state.as_str() {
            "running" => !(r.health.is_empty() || r.health == "healthy"),
            "exited" => r.exit_code != 0,
            _ => true,
        })
        .map(|r| r.service.clone())
        .collect();
    bad.sort();
    bad.dedup();
    bad
}

/// The services with a running container.
pub fn running(rows: &[PsRow]) -> Vec<String> {
    rows.iter()
        .filter(|r| r.state == "running")
        .map(|r| r.service.clone())
        .collect()
}

/// The long-running services of `config` (a one-shot service, `restart:
/// "no"`, is started by its dependants).
pub fn long_running(config: &serde_json::Value) -> Vec<String> {
    config["services"]
        .as_object()
        .map(|s| {
            s.iter()
                .filter(|(_, v)| v["restart"] != "no")
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default()
}
