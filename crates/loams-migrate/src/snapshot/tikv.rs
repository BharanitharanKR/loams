//! TiKV in an upgrade snapshot (design §54 §7.2, §8; Q821 ruled BR in UP1
//! Task 0, T0-7). The backup itself runs in a BR helper container the CLI
//! starts (`br backup txn` over every keyspace into `<snapshot>/tikv`, a
//! `local://` path mounted into `tikv` and BR at the same place); `migrate
//! snapshot` records it as the snapshot's `tikv` component, with the
//! cluster's version. A restore goes into a fresh cluster **at that
//! version** (§41 §8.2's `noDowngrade`: no component starts at an older
//! version on newer files), which [`check_restore_target`] enforces.

use serde::{Deserialize, Serialize};

use super::SnapshotError;
use super::manifest::Component;

/// How the TiKV component was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TikvMethod {
    /// `br backup txn` (T0-7).
    Br,
    /// A cold copy of `pd-data` and `tikv-data`, both stopped (the fallback).
    Cold,
}

impl TikvMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            TikvMethod::Br => "br",
            TikvMethod::Cold => "cold",
        }
    }
}

/// The `tikv` component of a snapshot taken by `method` at TiKV `version`
/// (`v8.5.8`), into `<snapshot>/tikv`. `snapshot()` fills in its digest.
pub fn component(method: TikvMethod, version: &str, bytes: u64) -> Component {
    Component {
        store: "tikv".to_string(),
        method: method.as_str().to_string(),
        path: "tikv".to_string(),
        bytes,
        sha256: String::new(),
        version: Some(version.to_string()),
    }
}

/// The TiKV version a `tikv` component was taken at.
pub fn component_version(component: &Component) -> Option<&str> {
    component.version.as_deref()
}

/// Refuses to restore a snapshot taken at TiKV `snapshot_version` into a
/// cluster running `cluster_version`: the cluster must be fresh **and** at
/// the snapshot's version (`component_downgrade` otherwise).
pub fn check_restore_target(
    snapshot_version: &str,
    cluster_version: &str,
) -> Result<(), SnapshotError> {
    let norm = |v: &str| v.trim().trim_start_matches('v').to_string();
    if norm(snapshot_version) == norm(cluster_version) {
        Ok(())
    } else {
        Err(SnapshotError::Io(format!(
            "component_downgrade: the snapshot was taken at TiKV {snapshot_version}; restore it \
             into a fresh cluster at that version, not {cluster_version}"
        )))
    }
}
