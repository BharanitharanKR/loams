//! Insert deduplication on lake tables (HS1 Task 12, §49 §6.1).
//!
//! Lake tables deduplicate inserts by default, as ClickHouse's replicated and
//! shared tables do. Each insert has a key: `token:<t>` when the request sets
//! `insert_deduplication_token`, else `sha256:<hex>` of the insert's data (the
//! format, the column list and the body as they arrived, or the rows an `INSERT
//! … SELECT` produced). A commit records the keys of the inserts it carries in
//! its snapshot's summary as `loams.dedup.<n>`, so the table's own metadata is
//! the record: an insert whose key is among the last `loams_dedup_window` keys on
//! `main` (default 1 000), within [`MAX_AGE_MS`], answers success with no rows
//! written, as ClickHouse does. `insert_deduplicate = 0` gives an insert no key.
//!
//! Every commit also records `loams.commit-id`, which is how a retry after an
//! ambiguous catalog failure (a timeout after the catalog may have committed)
//! finds that its snapshot landed, so an acknowledged insert is never applied
//! twice ([`committed`]).
//!
//! ClickHouse 26.7 (`clickhouse local`, measured 2026-10-13) keeps
//! `replicated_deduplication_window = 10000` and
//! `replicated_deduplication_window_seconds = 3600`; the House keeps the
//! Shared contracts' 1 000 and seven days (ruling R12.4).

use loams_house_lake::spec::{Snapshot, TableMetadata};
use sha2::{Digest, Sha256};

use crate::errors::{ChError, HouseError};

/// A snapshot summary key holding one insert's deduplication key.
pub const SUMMARY_PREFIX: &str = "loams.dedup.";
/// The snapshot summary key holding the commit's own id.
pub const COMMIT_ID: &str = "loams.commit-id";
/// `loams_dedup_window`'s default (Shared contracts).
pub const DEFAULT_WINDOW: usize = 1_000;
/// The largest `loams_dedup_window` (ClickHouse's own default window).
pub const MAX_WINDOW: usize = 10_000;
/// Keys older than this are forgotten whatever the window (§49 §6.1).
pub const MAX_AGE_MS: i64 = 7 * 24 * 3600 * 1000;
/// The longest `insert_deduplication_token` kept in a summary.
pub const MAX_TOKEN_BYTES: usize = 1024;

/// The key of an insert with `insert_deduplication_token = token`.
pub fn token_key(token: &str) -> Result<String, HouseError> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(HouseError::from(ChError::bad_arguments(format!(
            "insert_deduplication_token is longer than the {MAX_TOKEN_BYTES} bytes the House keeps"
        ))));
    }
    Ok(format!("token:{token}"))
}

/// The key of an insert's data, from its running hash.
pub fn hash_key(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(7 + digest.len() * 2);
    hex.push_str("sha256:");
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// What a commit adds to its snapshot's summary.
pub fn summary(keys: &[String], commit_id: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = keys
        .iter()
        .enumerate()
        .map(|(n, key)| (format!("{SUMMARY_PREFIX}{n}"), key.clone()))
        .collect();
    out.push((COMMIT_ID.to_string(), commit_id.to_string()));
    out
}

/// `main`'s snapshots, newest first.
fn ancestry(metadata: &TableMetadata) -> impl Iterator<Item = &Snapshot> + '_ {
    let mut next = metadata.current_snapshot().map(|s| s.snapshot_id());
    std::iter::from_fn(move || {
        let snapshot = metadata.snapshot_by_id(next?)?;
        next = snapshot.parent_snapshot_id();
        Some(snapshot.as_ref())
    })
}

/// A snapshot's deduplication keys, in the order its commit wrote them.
fn keys_of(snapshot: &Snapshot) -> Vec<&str> {
    let properties = &snapshot.summary().additional_properties;
    let mut keys: Vec<(u64, &str)> = properties
        .iter()
        .filter_map(|(name, value)| {
            let n = name.strip_prefix(SUMMARY_PREFIX)?.parse().ok()?;
            Some((n, value.as_str()))
        })
        .collect();
    keys.sort_unstable();
    keys.into_iter().map(|(_, key)| key).collect()
}

/// The snapshot holding `key` among the last `window` keys on `main` no older
/// than [`MAX_AGE_MS`] at `now_ms`, if any.
pub fn find(metadata: &TableMetadata, key: &str, window: usize, now_ms: i64) -> Option<i64> {
    let mut counted = 0;
    for snapshot in ancestry(metadata) {
        if counted >= window || snapshot.timestamp_ms() < now_ms - MAX_AGE_MS {
            return None;
        }
        // Newest last within a snapshot: count from its end.
        for held in keys_of(snapshot).into_iter().rev() {
            if counted >= window {
                return None;
            }
            counted += 1;
            if held == key {
                return Some(snapshot.snapshot_id());
            }
        }
    }
    None
}

/// The snapshot on `main` whose commit id is `commit_id`, looking no further
/// back than `parent` (the snapshot `main` was at before the commit).
pub fn committed(metadata: &TableMetadata, commit_id: &str, parent: Option<i64>) -> Option<i64> {
    for snapshot in ancestry(metadata) {
        if Some(snapshot.snapshot_id()) == parent {
            return None;
        }
        if snapshot
            .summary()
            .additional_properties
            .get(COMMIT_ID)
            .is_some_and(|id| id == commit_id)
        {
            return Some(snapshot.snapshot_id());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use loams_house_lake::spec::{
        NestedField, Operation, PrimitiveType, Schema, SnapshotReference, SnapshotRetention,
        Summary, Type, UnboundPartitionSpec,
    };
    use loams_house_lake::{TableCreation, TableUpdate};

    use super::*;

    fn table() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "k", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("s3://local/ns/1/house/default/t-x".to_string())
            .schema(schema)
            .partition_spec(UnboundPartitionSpec::builder().build())
            .properties(HashMap::new())
            .build();
        loams_house_lake::metadata::create(creation).expect("metadata")
    }

    /// Commits a snapshot with `keys` at `at_ms`.
    fn commit(metadata: TableMetadata, id: i64, keys: &[&str], at_ms: i64) -> TableMetadata {
        let parent = metadata.current_snapshot().map(|s| s.snapshot_id());
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(metadata.next_sequence_number())
            .with_timestamp_ms(at_ms)
            .with_manifest_list(format!("s3://local/m{id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: summary(&keys, &format!("c{id}")).into_iter().collect(),
            })
            .with_schema_id(0)
            .build();
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                reference: SnapshotReference::new(id, SnapshotRetention::branch(None, None, None)),
            },
        ];
        let file = loams_house_lake::MetadataFile::first(metadata.location());
        loams_house_lake::metadata::commit(&metadata, &file, &[], updates).expect("commit")
    }

    #[test]
    fn keys_are_found_within_the_window_and_age() {
        let mut metadata = table();
        let now = metadata.last_updated_ms() + 1_000;
        metadata = commit(metadata, 1, &["a", "b"], now - 10);
        metadata = commit(metadata, 2, &["c"], now - 5);
        metadata = commit(metadata, 3, &[], now - 4);
        assert_eq!(find(&metadata, "a", 10, now), Some(1));
        assert_eq!(find(&metadata, "c", 10, now), Some(2));
        assert_eq!(find(&metadata, "z", 10, now), None);
        // The window counts keys, newest first: c, b, a.
        assert_eq!(find(&metadata, "b", 2, now), Some(1));
        assert_eq!(find(&metadata, "a", 2, now), None);
        assert_eq!(find(&metadata, "c", 0, now), None);
        // Seven days on, nothing is held.
        assert_eq!(find(&metadata, "c", 10, now + MAX_AGE_MS), None);
    }

    #[test]
    fn a_commit_is_found_by_its_id_after_its_parent_only() {
        let mut metadata = table();
        let at = metadata.last_updated_ms();
        metadata = commit(metadata, 1, &[], at + 100);
        metadata = commit(metadata, 2, &[], at + 200);
        assert_eq!(committed(&metadata, "c2", Some(1)), Some(2));
        assert_eq!(committed(&metadata, "c1", Some(1)), None);
        assert_eq!(committed(&metadata, "c1", None), Some(1));
    }

    #[test]
    fn keys_are_tokens_or_hashes() {
        assert_eq!(token_key("x").expect("short"), "token:x");
        assert!(token_key(&"x".repeat(MAX_TOKEN_BYTES + 1)).is_err());
        let mut hasher = Sha256::new();
        hasher.update(b"abc");
        assert_eq!(
            hash_key(hasher),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
