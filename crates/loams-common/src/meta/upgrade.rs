//! Upgrade snapshots as GC roots (design §54 §7.2, D930; UP1 Task 8).
//!
//! `loams migrate snapshot` records each snapshot as the pointer
//! `upgrade/snapshots/<ulid>` in the `_system` namespace, valued
//! `v1;<created_ms>;<sha256 of snapshot.json>`. A snapshot's metastore state
//! can name any object that existed when it was taken, so while a snapshot
//! is kept no GC deletes an object written at or before its time, nor an
//! object retired after it. Pruning a snapshot sets its value to
//! [`PRUNED`] (the metastore cannot delete a pointer).

use super::{Consistency, MetaResult, MetaStore, SYSTEM_NAMESPACE};

/// The snapshot roots' pointer key prefix in `_system`.
pub const SNAPSHOT_ROOT_PREFIX: &str = "upgrade/snapshots/";
/// The value of a pruned snapshot's root.
pub const PRUNED: &str = "pruned";

/// The `created_ms` of a root value `v1;<created_ms>;…`; `None` for a pruned
/// or unreadable one.
pub fn snapshot_root_time(value: &str) -> Option<u64> {
    let mut parts = value.split(';');
    match (parts.next(), parts.next()) {
        (Some("v1"), Some(ms)) => ms.parse().ok(),
        _ => None,
    }
}

/// The time of the oldest kept upgrade snapshot (ms since the epoch), or
/// `None` when none is kept: the floor below which GC deletes nothing.
pub async fn snapshot_gc_floor(meta: &dyn MetaStore) -> MetaResult<Option<u64>> {
    let Some(ns) = meta
        .namespace_by_name(Consistency::Linearizable, SYSTEM_NAMESPACE)
        .await?
    else {
        return Ok(None);
    };
    let roots = meta
        .pointers_with_prefix(Consistency::Linearizable, ns.id, SNAPSHOT_ROOT_PREFIX)
        .await?;
    Ok(roots
        .iter()
        .filter_map(|(_, pointer)| snapshot_root_time(&pointer.value))
        .min())
}

/// Whether an object last written at `written_ms` is kept by `floor`.
pub fn pinned_by(floor: Option<u64>, written_ms: u64) -> bool {
    floor.is_some_and(|t| written_ms <= t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_values_parse() {
        assert_eq!(
            snapshot_root_time("v1;1792000000000;abcd"),
            Some(1_792_000_000_000)
        );
        assert_eq!(snapshot_root_time(PRUNED), None);
        assert_eq!(snapshot_root_time("v2;1;x"), None);
        assert!(pinned_by(Some(10), 10));
        assert!(!pinned_by(Some(10), 11));
        assert!(!pinned_by(None, 0));
    }
}
