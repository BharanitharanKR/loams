//! Loams Studio's review comments (design §53 §5.6, §6.5 G9; plan SD1 Task
//! 11): one row per comment in `review_comments`, keyed by checkout. Every
//! read and write names the checkout, so a comment is never reached through
//! another checkout's id.

use rusqlite::{OptionalExtension, Row, params};

use crate::store::{DocsStore, StoreError, now_ms, store_blocking};

/// One stored comment. `side` is `old` or `new`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewCommentRow {
    pub id: String,
    pub checkout_id: String,
    pub path: String,
    pub side: String,
    pub line: u32,
    pub end_line: Option<u32>,
    pub body: String,
    pub diff_checksum: String,
    pub created_at_ms: i64,
    pub resolved: bool,
}

const COLUMNS: &str =
    "id, checkout_id, path, side, line, end_line, body, diff_checksum, created_at, resolved";

fn row(row: &Row<'_>) -> rusqlite::Result<ReviewCommentRow> {
    Ok(ReviewCommentRow {
        id: row.get(0)?,
        checkout_id: row.get(1)?,
        path: row.get(2)?,
        side: row.get(3)?,
        line: row.get(4)?,
        end_line: row.get(5)?,
        body: row.get(6)?,
        diff_checksum: row.get(7)?,
        created_at_ms: row.get(8)?,
        resolved: row.get::<_, i64>(9)? != 0,
    })
}

impl DocsStore {
    /// A checkout's comments, oldest first.
    ///
    /// # Errors
    ///
    /// The database could not be read.
    pub fn review_comments(&self, checkout_id: &str) -> Result<Vec<ReviewCommentRow>, StoreError> {
        store_blocking(|| {
            let conn = self.conn();
            let mut stmt = conn.prepare(&format!(
                "SELECT {COLUMNS} FROM review_comments WHERE checkout_id = ?1
                 ORDER BY created_at, id"
            ))?;
            Ok(stmt
                .query_map(params![checkout_id], row)?
                .collect::<Result<_, _>>()?)
        })
    }

    /// One comment of a checkout.
    ///
    /// # Errors
    ///
    /// The database could not be read.
    pub fn review_comment(
        &self,
        checkout_id: &str,
        id: &str,
    ) -> Result<Option<ReviewCommentRow>, StoreError> {
        store_blocking(|| {
            Ok(self
                .conn()
                .query_row(
                    &format!(
                        "SELECT {COLUMNS} FROM review_comments WHERE checkout_id = ?1 AND id = ?2"
                    ),
                    params![checkout_id, id],
                    row,
                )
                .optional()?)
        })
    }

    /// How many comments a checkout has.
    ///
    /// # Errors
    ///
    /// The database could not be read.
    pub fn review_comment_count(&self, checkout_id: &str) -> Result<usize, StoreError> {
        store_blocking(|| {
            let count: i64 = self.conn().query_row(
                "SELECT COUNT(*) FROM review_comments WHERE checkout_id = ?1",
                params![checkout_id],
                |row| row.get(0),
            )?;
            Ok(usize::try_from(count).unwrap_or(usize::MAX))
        })
    }

    /// Insert `comment`, or replace the comment of the same id in the same
    /// checkout (keeping its `created_at`). Returns `false` when the id
    /// exists in another checkout, which is never overwritten.
    ///
    /// # Errors
    ///
    /// The database could not be written.
    pub fn upsert_review_comment(&self, comment: &ReviewCommentRow) -> Result<bool, StoreError> {
        store_blocking(|| {
            let changed = self.conn().execute(
                "INSERT INTO review_comments
                    (id, checkout_id, path, side, line, end_line, body, diff_checksum,
                     created_at, updated_at, resolved)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(id) DO UPDATE SET
                    path = excluded.path,
                    side = excluded.side,
                    line = excluded.line,
                    end_line = excluded.end_line,
                    body = excluded.body,
                    diff_checksum = excluded.diff_checksum,
                    updated_at = excluded.updated_at,
                    resolved = excluded.resolved
                 WHERE review_comments.checkout_id = excluded.checkout_id",
                params![
                    comment.id,
                    comment.checkout_id,
                    comment.path,
                    comment.side,
                    comment.line,
                    comment.end_line,
                    comment.body,
                    comment.diff_checksum,
                    comment.created_at_ms,
                    now_ms(),
                    i64::from(comment.resolved),
                ],
            )?;
            Ok(changed > 0)
        })
    }

    /// Delete one comment; `false` when the checkout has no such comment.
    ///
    /// # Errors
    ///
    /// The database could not be written.
    pub fn delete_review_comment(&self, checkout_id: &str, id: &str) -> Result<bool, StoreError> {
        store_blocking(|| {
            let changed = self.conn().execute(
                "DELETE FROM review_comments WHERE checkout_id = ?1 AND id = ?2",
                params![checkout_id, id],
            )?;
            Ok(changed > 0)
        })
    }

    /// Mark one comment resolved or not; `false` when the checkout has no
    /// such comment.
    ///
    /// # Errors
    ///
    /// The database could not be written.
    pub fn resolve_review_comment(
        &self,
        checkout_id: &str,
        id: &str,
        resolved: bool,
    ) -> Result<bool, StoreError> {
        store_blocking(|| {
            let changed = self.conn().execute(
                "UPDATE review_comments SET resolved = ?3, updated_at = ?4
                 WHERE checkout_id = ?1 AND id = ?2",
                params![checkout_id, id, i64::from(resolved), now_ms()],
            )?;
            Ok(changed > 0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(id: &str, checkout: &str) -> ReviewCommentRow {
        ReviewCommentRow {
            id: id.into(),
            checkout_id: checkout.into(),
            path: "src/main.rs".into(),
            side: "new".into(),
            line: 3,
            end_line: None,
            body: "Why?".into(),
            diff_checksum: "sum".into(),
            created_at_ms: 10,
            resolved: false,
        }
    }

    #[test]
    fn review_comments_are_keyed_by_checkout() -> Result<(), StoreError> {
        let dir = tempfile::tempdir()?;
        let store = DocsStore::open(dir.path())?;
        assert!(store.upsert_review_comment(&comment("c1", "a"))?);
        assert!(store.upsert_review_comment(&comment("c2", "a"))?);
        // The same id from another checkout never overwrites the first.
        assert!(!store.upsert_review_comment(&comment("c1", "b"))?);
        assert_eq!(store.review_comment_count("a")?, 2);
        assert_eq!(store.review_comment_count("b")?, 0);

        let mut edited = comment("c1", "a");
        edited.body = "Still why?".into();
        edited.created_at_ms = 99;
        assert!(store.upsert_review_comment(&edited)?);
        let stored = store.review_comment("a", "c1")?.expect("stored");
        assert_eq!(stored.body, "Still why?");
        assert_eq!(stored.created_at_ms, 10, "created_at is kept");

        assert!(store.resolve_review_comment("a", "c1", true)?);
        assert!(!store.resolve_review_comment("b", "c1", true)?);
        assert!(store.review_comment("a", "c1")?.expect("stored").resolved);
        assert!(store.review_comment("b", "c1")?.is_none());

        assert!(!store.delete_review_comment("b", "c2")?);
        assert!(store.delete_review_comment("a", "c2")?);
        let ids: Vec<String> = store
            .review_comments("a")?
            .into_iter()
            .map(|row| row.id)
            .collect();
        assert_eq!(ids, ["c1"]);
        Ok(())
    }
}
