//! The release train as the target's bundle ships it (`upgrade-path.toml`,
//! design §54 §5, §11): enough of it for the client to plan an upgrade. The
//! one-step rule is the same as `loams-migrate`'s (`path.rs`), which the
//! target image enforces again in `loams migrate run`; this copy only lets
//! `check` say so before anything starts.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::errors::{Result, UpgradeError};

#[derive(Clone, Debug, Deserialize)]
pub struct UpgradePath {
    #[serde(rename = "release")]
    pub releases: Vec<Release>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Release {
    pub version: String,
    pub from: String,
    #[serde(default)]
    pub free_factor: Option<f64>,
    #[serde(default)]
    pub downtime_seconds: Option<u64>,
    #[serde(default, rename = "migration")]
    pub migrations: Vec<PathMigration>,
    #[serde(default)]
    pub components: BTreeMap<String, String>,
    #[serde(default)]
    pub env: Vec<EnvChange>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PathMigration {
    pub id: String,
    pub store: String,
    pub class: String,
    pub summary: String,
}

/// A `.env` change a release asks for: a new variable and its default.
#[derive(Clone, Debug, Deserialize)]
pub struct EnvChange {
    pub add: String,
    #[serde(default)]
    pub default: String,
}

fn parse_version(text: &str) -> Option<(u64, u64, u64, Option<&str>)> {
    let (core, pre) = match text.split_once('-') {
        Some((core, pre)) if !pre.is_empty() => (core, Some(pre)),
        Some(_) => return None,
        None => (text, None),
    };
    let mut parts = core.split('.');
    let v = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    parts.next().is_none().then_some((v.0, v.1, v.2, pre))
}

impl UpgradePath {
    pub fn parse(text: &str) -> Result<Self> {
        toml::from_str(text)
            .map_err(|e| UpgradeError::new("manifest_invalid", format!("upgrade-path.toml: {e}")))
    }

    pub fn release(&self, version: &str) -> Option<&Release> {
        self.releases.iter().find(|r| r.version == version)
    }

    /// The newest release of the train.
    pub fn latest(&self) -> Option<&Release> {
        self.releases.first()
    }

    /// `Ok` when a stack at `stored` upgrades to `target` in one step.
    pub fn check_adjacent(&self, stored: &str, target: &str) -> Result<()> {
        let release = self.release(target).ok_or_else(|| {
            UpgradeError::new(
                "release_not_found",
                format!("{target} is not in the release train"),
            )
        })?;
        let refuse = || {
            UpgradeError::new(
                "upgrade_step_not_adjacent",
                format!(
                    "the stack is at {stored}; {target} upgrades from {}; chain: {}",
                    release.from,
                    self.chain(target).join(" -> ")
                ),
            )
        };
        if release.from == "fresh" {
            return Err(refuse());
        }
        if release.from.matches('.').count() == 1 {
            let (major, minor) = release
                .from
                .split_once('.')
                .and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))
                .ok_or_else(refuse)?;
            let t = parse_version(target).ok_or_else(refuse)?;
            return match parse_version(stored) {
                Some((a, b, patch, None)) if a == major && b == minor && patch < t.2 => Ok(()),
                _ => Err(refuse()),
            };
        }
        if stored == release.from {
            Ok(())
        } else {
            Err(refuse())
        }
    }

    /// The releases from the first that keeps data up to `target`.
    pub fn chain(&self, target: &str) -> Vec<String> {
        let mut chain = Vec::new();
        let mut at = Some(target.to_string());
        while let Some(version) = at.take() {
            if chain.contains(&version) {
                break;
            }
            chain.push(version.clone());
            at = self
                .release(&version)
                .map(|r| r.from.clone())
                .filter(|from| from != "fresh" && self.release(from).is_some());
        }
        chain.reverse();
        chain
    }

    /// The steps from `stored` to `target`, one release at a time (Q825:
    /// `--to` beyond one step walks the chain).
    pub fn steps(&self, stored: &str, target: &str) -> Result<Vec<String>> {
        let chain = self.chain(target);
        match chain.iter().position(|v| v == stored) {
            Some(i) => Ok(chain[i + 1..].to_vec()),
            None => {
                self.check_adjacent(stored, target)?;
                Ok(vec![target.to_string()])
            }
        }
    }
}

/// Compares two versions of one component (`v8.5.8`, `v8.5.8-loams.1`,
/// `26.9.0`, a digest): `Some(Less)` when `to` is older than `from`. A digest
/// or anything else that does not parse is not ordered (`None`).
pub fn compare_pins(from: &str, to: &str) -> Option<std::cmp::Ordering> {
    let parse = |v: &str| -> Option<Vec<u64>> {
        let core = v.trim_start_matches('v');
        let core = core.split(['-', '+']).next()?;
        core.split('.').map(|p| p.parse().ok()).collect()
    };
    Some(parse(to)?.cmp(&parse(from)?))
}
