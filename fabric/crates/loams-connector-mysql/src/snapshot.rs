//! The initial CDC snapshot (CN2 Task 11).
//!
//! **Without locks** (`snapshot.locking: watermark`, the default) it is
//! Netflix's DBLog algorithm, interleaved with the binlog stream: for each
//! chunk of a table, the source writes a *low* watermark row to the
//! watermark table, `SELECT`s the chunk by primary key, and writes a *high*
//! watermark row. Reading the binlog, a change to a chunk row committed
//! between the two watermarks removes that row from the chunk (the change
//! carries the newer state); when the high watermark arrives, what is left
//! of the chunk is emitted (`loamsop = r`), and the next chunk starts. Every
//! change is delivered in binlog order, every row is delivered at least
//! once, and nothing is locked. The watermark table is created by the
//! runbook (`docs/runbooks/connectors/mysql-cdc.md`) and needs only
//! `INSERT`/`UPDATE` on it.
//!
//! **With a brief global lock** (`snapshot.locking: global-read-lock`), for
//! an account that cannot write: `FLUSH TABLES WITH READ LOCK`, read the
//! binlog position, `START TRANSACTION WITH CONSISTENT SNAPSHOT`, `UNLOCK
//! TABLES`; the tables are read in that transaction, and the stream starts
//! at the recorded position afterwards. A crash mid-way starts it over.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use loams_connector_sdk::ConnectorError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::TableName;
use crate::conn::MyConn;
use crate::table::{TableInfo, chunk_sql, key_of};
use crate::types::{quote_ident, quote_str, text_to_json};

/// The watermark table's DDL (the runbook's).
pub fn watermark_ddl(t: &TableName) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (instance VARCHAR(255) NOT NULL PRIMARY KEY, \
         mark VARCHAR(64) NOT NULL, updated_at TIMESTAMP(6) NOT NULL \
         DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)) ENGINE=InnoDB",
        t.quoted()
    )
}

/// Snapshot progress, stored in the CDC position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "locking")]
pub enum SnapshotPos {
    /// DBLog: the tables still to read (the first in progress) and the
    /// last key of the last chunk delivered from it.
    Watermark {
        tables: VecDeque<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<Vec<Value>>,
    },
    /// A locked snapshot in progress; it cannot resume, so a restart takes
    /// a new one.
    Locked { table: usize },
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A fresh watermark value.
pub fn new_mark(kind: char) -> String {
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "{ns:x}-{:x}-{}-{kind}",
        SEQ.fetch_add(1, Ordering::Relaxed),
        std::process::id()
    )
}

/// Rows of a chunk as canonical JSON, in `t.def`'s column order.
pub fn rows_json(
    t: &TableInfo,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<Vec<Vec<Value>>, ConnectorError> {
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .zip(&t.def.columns)
                .map(|(v, c)| text_to_json(v, c))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| ConnectorError::poison(format!("{}: {e}", t.name)))
        })
        .collect()
}

/// One chunk after `after`, at most `limit` rows.
pub async fn read_chunk(
    conn: &mut MyConn,
    t: &TableInfo,
    after: Option<&[Value]>,
    limit: usize,
) -> Result<Vec<Vec<Value>>, ConnectorError> {
    let sql = chunk_sql(t, after, limit).map_err(ConnectorError::fatal)?;
    let rs = conn
        .query(&sql)
        .await
        .map_err(|e| e.to_connector(&format!("reading {}", t.name)))?;
    rows_json(t, rs.rows)
}

/// Where a DBLog window is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowState {
    /// Waiting for the low watermark in the binlog.
    Pending,
    /// Between the low and the high watermark.
    Open,
}

/// One DBLog chunk in flight.
#[derive(Debug)]
pub struct Window {
    pub low: String,
    pub high: String,
    pub state: WindowState,
    pub table: TableName,
    /// The table's Arrow schema when the chunk was read.
    pub schema: arrow_schema::SchemaRef,
    /// The chunk's rows by key (JSON text of the key values).
    pub rows: BTreeMap<String, (usize, Vec<Value>)>,
    /// The chunk's last key (the cursor once delivered); `None` if empty.
    pub last_key: Option<Vec<Value>>,
    /// The chunk was short: the table is done once it is delivered.
    pub exhausted: bool,
    pub opened: Instant,
}

/// A key's map key.
pub fn key_text(k: &[Value]) -> String {
    Value::Array(k.to_vec()).to_string()
}

impl Window {
    /// Write the low watermark, read the chunk, write the high watermark.
    pub async fn open(
        conn: &mut MyConn,
        watermark: &TableName,
        owner: &str,
        t: &TableInfo,
        after: Option<&[Value]>,
        limit: usize,
    ) -> Result<Self, ConnectorError> {
        let low = new_mark('l');
        let high = new_mark('h');
        let write = |mark: &str| {
            format!(
                "INSERT INTO {} ({}, {}) VALUES ({}, {}) ON DUPLICATE KEY UPDATE {2} = VALUES({2})",
                watermark.quoted(),
                quote_ident("instance"),
                quote_ident("mark"),
                quote_str(owner),
                quote_str(mark)
            )
        };
        conn.exec(&write(&low))
            .await
            .map_err(|e| e.to_connector("writing the low watermark"))?;
        let chunk = read_chunk(conn, t, after, limit).await?;
        conn.exec(&write(&high))
            .await
            .map_err(|e| e.to_connector("writing the high watermark"))?;
        let exhausted = chunk.len() < limit;
        let last_key = chunk.last().map(|r| key_of(t, r));
        let rows = chunk
            .into_iter()
            .enumerate()
            .map(|(i, r)| (key_text(&key_of(t, &r)), (i, r)))
            .collect();
        Ok(Self {
            low,
            high,
            state: WindowState::Pending,
            table: t.name.clone(),
            schema: t.schema.clone(),
            rows,
            last_key,
            exhausted,
            opened: Instant::now(),
        })
    }

    /// A change to `table`'s row `key` seen in the binlog.
    pub fn changed(&mut self, table: &TableName, key: &[Value]) {
        if self.state == WindowState::Open && *table == self.table {
            self.rows.remove(&key_text(key));
        }
    }

    /// A watermark value seen in the binlog: `true` once the high one
    /// closes the window.
    pub fn watermark(&mut self, value: &str) -> bool {
        if value == self.low {
            self.state = WindowState::Open;
        }
        value == self.high && self.state == WindowState::Open
    }

    /// What is left of the chunk, in key order.
    pub fn remaining(self) -> Vec<Vec<Value>> {
        let mut v: Vec<(usize, Vec<Value>)> = self.rows.into_values().collect();
        v.sort_by_key(|(i, _)| *i);
        v.into_iter().map(|(_, r)| r).collect()
    }
}

/// Take the global read lock (the caller reads the binlog position under
/// it, then calls [`locked_begin`]).
pub async fn locked_start(conn: &mut MyConn) -> Result<(), ConnectorError> {
    conn.exec("FLUSH TABLES WITH READ LOCK")
        .await
        .map_err(|e| e.to_connector("FLUSH TABLES WITH READ LOCK (snapshot.locking)"))?;
    Ok(())
}

/// Begin the consistent snapshot and release the global lock.
pub async fn locked_begin(conn: &mut MyConn) -> Result<(), ConnectorError> {
    let r = conn
        .exec("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        .await
        .map_err(|e| e.to_connector("starting the consistent snapshot"));
    let unlock = conn
        .exec("UNLOCK TABLES")
        .await
        .map_err(|e| e.to_connector("UNLOCK TABLES"));
    r.and(unlock).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::TableDef;
    use crate::types::ColumnDef;
    use loams_connector_sdk::schema::Lossy;
    use serde_json::json;

    fn window(rows: &[(i64, &str)]) -> Window {
        Window {
            low: "L".into(),
            high: "H".into(),
            state: WindowState::Pending,
            table: TableName::new("d", "t"),
            schema: std::sync::Arc::new(arrow_schema::Schema::empty()),
            rows: rows
                .iter()
                .enumerate()
                .map(|(i, (k, v))| (key_text(&[json!(k)]), (i, vec![json!(k), json!(v)])))
                .collect(),
            last_key: rows.last().map(|(k, _)| vec![json!(k)]),
            exhausted: false,
            opened: Instant::now(),
        }
    }

    /// DBLog's reconciliation: only changes between the watermarks remove
    /// chunk rows, and the rest come out in key order at the high mark.
    #[test]
    fn changes_inside_the_window_win_over_the_chunk() {
        let t = TableName::new("d", "t");
        let mut w = window(&[(1, "a"), (2, "b"), (3, "c"), (10, "j")]);
        // Before the low watermark: ignored.
        w.changed(&t, &[json!(1)]);
        assert!(!w.watermark("other"));
        assert!(!w.watermark("L"));
        w.changed(&t, &[json!(2)]);
        w.changed(&TableName::new("d", "other"), &[json!(3)]);
        w.changed(&t, &[json!(99)]);
        assert!(w.watermark("H"));
        let left: Vec<i64> = w
            .remaining()
            .iter()
            .map(|r| r[0].as_i64().expect("id"))
            .collect();
        assert_eq!(left, [1, 3, 10]);
    }

    #[test]
    fn a_high_mark_before_the_low_one_does_not_close() {
        let mut w = window(&[(1, "a")]);
        assert!(!w.watermark("H"));
        assert_eq!(w.state, WindowState::Pending);
    }

    #[test]
    fn marks_are_unique_and_positions_serialise() {
        assert_ne!(new_mark('l'), new_mark('l'));
        let p = SnapshotPos::Watermark {
            tables: ["d.a".to_owned(), "d.b".to_owned()].into(),
            after: Some(vec![json!(5)]),
        };
        let s = serde_json::to_string(&p).expect("json");
        assert_eq!(serde_json::from_str::<SnapshotPos>(&s).expect("back"), p);
    }

    #[test]
    fn chunk_rows_become_canonical_json() {
        let def = TableDef {
            columns: vec![
                ColumnDef::new("id", "bigint", false),
                ColumnDef::new("v", "varchar(5)", true),
            ],
            key: vec!["id".into()],
        };
        let t =
            crate::table::info(&TableName::new("d", "t"), def, &[], &Lossy::none()).expect("info");
        let rows = rows_json(&t, vec![vec![Some(b"7".to_vec()), None]]).expect("rows");
        assert_eq!(rows, vec![vec![json!(7), json!(null)]]);
        assert!(rows_json(&t, vec![vec![Some(b"x".to_vec()), None]]).is_err());
    }
}
