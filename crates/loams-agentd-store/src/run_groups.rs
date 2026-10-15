//! The `run_groups` table (plan SD1 Task 12, Ruling 5): a run group's record
//! survives the window closing and a daemon restart. The record is the
//! sessions crate's JSON; the store keeps it opaque.

use rusqlite::params;

use crate::store::{DocsStore, StoreError, store_blocking};

/// One stored group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRunGroup {
    pub group_id: String,
    pub space_id: Option<String>,
    pub created_at: i64,
    /// The record, as JSON.
    pub record: String,
}

impl DocsStore {
    /// Insert or replace one group's record.
    pub fn save_run_group(&self, group: &StoredRunGroup) -> Result<(), StoreError> {
        store_blocking(|| {
            self.conn().execute(
                "INSERT INTO run_groups (group_id, space_id, created_at, record)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(group_id) DO UPDATE SET
                   space_id = excluded.space_id,
                   record = excluded.record",
                params![
                    group.group_id,
                    group.space_id,
                    group.created_at,
                    group.record
                ],
            )?;
            Ok(())
        })
    }

    /// Every stored group, oldest first.
    pub fn run_groups(&self) -> Result<Vec<StoredRunGroup>, StoreError> {
        store_blocking(|| {
            let conn = self.conn();
            let mut statement = conn.prepare(
                "SELECT group_id, space_id, created_at, record FROM run_groups
                 ORDER BY created_at, group_id",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(StoredRunGroup {
                    group_id: row.get(0)?,
                    space_id: row.get(1)?,
                    created_at: row.get(2)?,
                    record: row.get(3)?,
                })
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
    }

    /// Forget one group; true when it existed.
    pub fn delete_run_group(&self, group_id: &str) -> Result<bool, StoreError> {
        store_blocking(|| {
            let deleted = self.conn().execute(
                "DELETE FROM run_groups WHERE group_id = ?1",
                params![group_id],
            )?;
            Ok(deleted > 0)
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn run_groups_round_trip_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let group = StoredRunGroup {
            group_id: "g-1".into(),
            space_id: Some("space-1".into()),
            created_at: 7,
            record: r#"{"id":"g-1"}"#.into(),
        };
        {
            let store = DocsStore::open(dir.path()).unwrap();
            store.save_run_group(&group).unwrap();
            let changed = StoredRunGroup {
                record: r#"{"id":"g-1","state":"ready"}"#.into(),
                ..group.clone()
            };
            store.save_run_group(&changed).unwrap();
        }
        let store = DocsStore::open(dir.path()).unwrap();
        let groups = store.run_groups().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].record, r#"{"id":"g-1","state":"ready"}"#);
        assert!(store.delete_run_group("g-1").unwrap());
        assert!(store.run_groups().unwrap().is_empty());
        assert!(!store.delete_run_group("g-1").unwrap());
    }
}
