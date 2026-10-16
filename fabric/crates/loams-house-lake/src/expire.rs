//! Snapshot expiry (HS1 Task 15, §49 §11): which snapshots a table may forget
//! once they are older than its retention, and the commit that forgets them.
//!
//! A snapshot is **kept** when any of these holds:
//!
//! * a ref (`main`, or any branch or tag another writer made) points at it;
//! * it was made at or after the cutoff (`now - retention`);
//! * `main`'s history names it at or after the cutoff (a `RestoreTable` makes
//!   an old snapshot current again, with a new history entry);
//! * it is the one `main` pointed at when the cutoff passed (the newest history
//!   entry before it), so time travel to any instant within the retention
//!   still finds the snapshot that was current then.
//!
//! Every other snapshot is expired. The commit requires `main` to be where it
//! was when the table was read, so a concurrent `RestoreTable` (which may make
//! an expired snapshot current) wins and the expiry tries again later.
//!
//! What files the expired snapshots free is the caller's to work out, with
//! [`crate::files`]: a file is deleted only when no kept snapshot still lists
//! it.

use std::collections::HashSet;

use iceberg::spec::TableMetadata;
use iceberg::{TableRequirement, TableUpdate};

use crate::append::MAIN;

/// An expiry ready to commit.
#[derive(Debug, Clone, PartialEq)]
pub struct Expiry {
    /// The snapshots it forgets.
    pub expired: Vec<i64>,
    /// The snapshots that stay.
    pub kept: Vec<i64>,
    /// `main` is still where it was read.
    pub requirements: Vec<TableRequirement>,
    /// `remove-snapshots`.
    pub updates: Vec<TableUpdate>,
}

/// The expiry of `metadata`'s snapshots older than `cutoff_ms` (see the module
/// docs); `None` when nothing expires.
pub fn expire(metadata: &TableMetadata, cutoff_ms: i64) -> Option<Expiry> {
    let mut kept = ref_snapshots(metadata);
    let history = metadata.history();
    for entry in history.iter().filter(|e| e.timestamp_ms >= cutoff_ms) {
        kept.insert(entry.snapshot_id);
    }
    if let Some(at_cutoff) = history.iter().rev().find(|e| e.timestamp_ms < cutoff_ms) {
        kept.insert(at_cutoff.snapshot_id);
    }
    let mut expired = Vec::new();
    let mut stays = Vec::new();
    for snapshot in metadata.snapshots() {
        let id = snapshot.snapshot_id();
        if kept.contains(&id) || snapshot.timestamp_ms() >= cutoff_ms {
            stays.push(id);
        } else {
            expired.push(id);
        }
    }
    if expired.is_empty() {
        return None;
    }
    expired.sort_unstable();
    stays.sort_unstable();
    Some(Expiry {
        requirements: vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN.to_string(),
            snapshot_id: metadata.current_snapshot_id(),
        }],
        updates: vec![TableUpdate::RemoveSnapshots {
            snapshot_ids: expired.clone(),
        }],
        expired,
        kept: stays,
    })
}

/// The snapshots the table's refs point at. iceberg 0.10 keeps the refs
/// private but writes them into the metadata file, so they are read from its
/// JSON form; `main` is read directly in case that ever fails.
pub fn ref_snapshots(metadata: &TableMetadata) -> HashSet<i64> {
    let mut out: HashSet<i64> = metadata.current_snapshot_id().into_iter().collect();
    if let Ok(value) = serde_json::to_value(metadata)
        && let Some(refs) = value.get("refs").and_then(|r| r.as_object())
    {
        out.extend(
            refs.values()
                .filter_map(|r| r.get("snapshot-id").and_then(serde_json::Value::as_i64)),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::TableCreation;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SnapshotReference, SnapshotRetention, Struct, Type,
        UnboundPartitionSpec,
    };

    use super::*;
    use crate::append::{NewDataFile, fast_append};
    use crate::metadata::{self, MetadataFile};

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
        metadata::create(creation).expect("metadata")
    }

    /// Appends one file at `at`; `lists` holds the manifest lists written.
    async fn append(
        metadata: TableMetadata,
        lists: &mut HashMap<String, Vec<u8>>,
        at: i64,
    ) -> (TableMetadata, i64) {
        let file = NewDataFile {
            location: format!("s3://local/ns/1/house/default/t-x/data/{at}.parquet"),
            record_count: 1,
            size_bytes: 1,
            partition: Struct::empty(),
        };
        let parent = metadata
            .current_snapshot()
            .map(|s| lists[s.manifest_list()].clone());
        let built = fast_append(&metadata, parent.as_deref(), vec![file], at)
            .await
            .expect("append");
        for (location, bytes) in &built.files {
            lists.insert(location.clone(), bytes.to_vec());
        }
        let file = MetadataFile::first(metadata.location());
        let next =
            metadata::commit(&metadata, &file, &built.requirements, built.updates).expect("commit");
        (next, built.snapshot_id)
    }

    #[tokio::test]
    async fn keeps_refs_recent_and_the_one_current_at_the_cutoff() {
        // A new table's metadata is stamped now, and no snapshot may be older
        // than its table's last change, so times are counted from it.
        let mut m = table();
        let t0 = m.last_updated_ms();
        let mut lists = HashMap::new();
        let mut ids = Vec::new();
        for at in [t0 + 1_000, t0 + 2_000, t0 + 3_000, t0 + 4_000] {
            let (next, id) = append(m, &mut lists, at).await;
            m = next;
            ids.push(id);
        }
        // Nothing older than the first snapshot: nothing expires.
        assert!(expire(&m, t0 + 500).is_none());
        // Cutoff 3 500: snapshot 3 was current at the cutoff, 4 is current.
        let expiry = expire(&m, t0 + 3_500).expect("expires");
        let mut want = vec![ids[0], ids[1]];
        want.sort_unstable();
        assert_eq!(expiry.expired, want);
        let file = MetadataFile::first(m.location());
        let after =
            metadata::commit(&m, &file, &expiry.requirements, expiry.updates).expect("commits");
        assert_eq!(after.snapshots().len(), 2);
        assert_eq!(after.current_snapshot_id(), Some(ids[3]));
        assert_eq!(
            after
                .history()
                .iter()
                .map(|e| e.snapshot_id)
                .collect::<Vec<_>>(),
            vec![ids[2], ids[3]]
        );

        // A tag on the oldest keeps it whatever its age.
        let file = MetadataFile::first(m.location());
        let tagged = metadata::commit(
            &m,
            &file,
            &[],
            vec![TableUpdate::SetSnapshotRef {
                ref_name: "keep".to_string(),
                reference: SnapshotReference::new(
                    ids[0],
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            }],
        )
        .expect("tag");
        let expiry = expire(&tagged, t0 + 3_500).expect("expires");
        assert_eq!(expiry.expired, vec![ids[1]]);

        // A stale expiry (main moved since) is a conflict.
        let (moved, _) = append(m.clone(), &mut lists, t0 + 5_000).await;
        let stale = expire(&m, t0 + 3_500).expect("expires");
        let file = MetadataFile::first(m.location());
        assert!(matches!(
            metadata::commit(&moved, &file, &stale.requirements, stale.updates),
            Err(crate::LakeError::Conflict(_))
        ));
    }
}
