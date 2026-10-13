//! Reading a collection in `_id` order at one cluster time: the batch
//! source's splits and the CDC source's snapshot share it.
//!
//! A scan is a `find` sorted by `_id` with `readConcern: snapshot` (and
//! `atClusterTime` once known), continued with `getMore`. Its resumable state
//! is the cluster time and the last `_id` handed out; a lost cursor
//! (`CursorNotFound`, a reconnect) reopens after that `_id`. A bounded scan
//! (an `_id` range of one type bracket) resumes with `$gt`; an unbounded one
//! uses `$expr`, which compares across brackets as the sort does.

use std::collections::VecDeque;

use loams_connector_sdk::ConnectorError;

use crate::bson::{Bson, Document, Timestamp};
use crate::client::Client;
use crate::doc;
use crate::wire::{codes, server_error};

/// The largest `batchSize` a scan asks for.
pub const MAX_BATCH: usize = 10_000;

#[derive(Debug, Clone)]
pub struct Scan {
    pub collection: String,
    /// `[lo, hi)`, both of one type bracket; `None` is open on that side
    /// (still within the bracket when the other side is set).
    pub lo: Option<Bson>,
    pub hi: Option<Bson>,
    /// Read at this cluster time (`None`: the first `find` picks it, or the
    /// deployment has no snapshot reads).
    pub at: Option<Timestamp>,
    /// Snapshot reads (replica sets and sharded clusters).
    pub snapshot: bool,
    /// The last `_id` handed out.
    pub last: Option<Bson>,
    cursor: Option<i64>,
    pub pending: VecDeque<Document>,
    pub exhausted: bool,
    avg_bytes: usize,
}

impl Scan {
    pub fn new(collection: impl Into<String>, snapshot: bool) -> Self {
        Self {
            collection: collection.into(),
            lo: None,
            hi: None,
            at: None,
            snapshot,
            last: None,
            cursor: None,
            pending: VecDeque::new(),
            exhausted: false,
            avg_bytes: 256,
        }
    }

    /// The `find` filter for what is left to read.
    pub fn filter(&self) -> Document {
        let bounded = self.lo.is_some() || self.hi.is_some();
        match (&self.last, bounded) {
            (Some(last), false) => doc! {
                "$expr" => doc! { "$gt" => vec![Bson::from("$_id"), Bson::from(doc! { "$literal" => last.clone() })] },
            },
            (last, _) => {
                let mut range = Document::new();
                match (last, &self.lo) {
                    (Some(l), _) => range.insert("$gt", l.clone()),
                    (None, Some(lo)) => range.insert("$gte", lo.clone()),
                    (None, None) => {}
                }
                if let Some(hi) = &self.hi {
                    range.insert("$lt", hi.clone());
                }
                if range.is_empty() {
                    Document::new()
                } else {
                    doc! { "_id" => range }
                }
            }
        }
    }

    /// Fill `pending` with up to about `rows` documents (fewer when their
    /// average size says `bytes` would be passed), unless documents are
    /// already pending or the scan is done.
    pub async fn fill(
        &mut self,
        client: &mut Client,
        db: &str,
        rows: usize,
        bytes: usize,
    ) -> Result<(), ConnectorError> {
        if !self.pending.is_empty() || self.exhausted {
            return Ok(());
        }
        let n = rows.min(bytes / self.avg_bytes.max(1)).clamp(1, MAX_BATCH) as i32;
        let reply = match self.cursor {
            Some(id) => {
                let r = client
                    .command(
                        db,
                        doc! { "getMore" => id, "collection" => self.collection.clone(), "batchSize" => n },
                    )
                    .await;
                match r {
                    Err(e)
                        if server_error(&e).is_some_and(|s| s.code == codes::CURSOR_NOT_FOUND) =>
                    {
                        // Reopen after the last document handed out.
                        self.cursor = None;
                        return Err(e);
                    }
                    Err(e) => {
                        if e.class.is_transient() {
                            self.cursor = None;
                        }
                        return Err(e);
                    }
                    Ok(r) => r,
                }
            }
            None => {
                let mut cmd = doc! {
                    "find" => self.collection.clone(),
                    "filter" => self.filter(),
                    "sort" => doc! { "_id" => 1 },
                    "batchSize" => n,
                };
                if self.snapshot {
                    let mut rc = doc! { "level" => "snapshot" };
                    if let Some(at) = self.at {
                        rc.insert("atClusterTime", at);
                    }
                    cmd.insert("readConcern", rc);
                }
                client.command(db, cmd).await?
            }
        };
        let cursor = reply
            .get_document("cursor")
            .ok_or_else(|| ConnectorError::fatal("find: the reply has no cursor"))?;
        if self.snapshot
            && self.at.is_none()
            && let Some(at) = cursor.get("atClusterTime").and_then(Bson::as_timestamp)
        {
            self.at = Some(at);
        }
        let batch = cursor
            .get_array("firstBatch")
            .or_else(|| cursor.get_array("nextBatch"))
            .unwrap_or_default();
        let mut total = 0usize;
        for d in batch {
            if let Bson::Document(d) = d {
                total += d.encoded_len().unwrap_or(0);
                self.pending.push_back(d.clone());
            }
        }
        if !batch.is_empty() {
            self.avg_bytes = (total / batch.len()).max(16);
        }
        let id = cursor.get_i64("id").unwrap_or(0);
        if id == 0 {
            self.cursor = None;
            self.exhausted = true;
        } else {
            self.cursor = Some(id);
        }
        Ok(())
    }

    /// Hand out the first `n` pending documents; `last` moves to the last
    /// one's `_id`.
    pub fn take(&mut self, n: usize) -> Vec<Document> {
        let out: Vec<Document> = self.pending.drain(..n.min(self.pending.len())).collect();
        if let Some(id) = out.last().and_then(|d| d.get("_id")) {
            self.last = Some(id.clone());
        }
        out
    }

    /// Whether every document has been handed out.
    pub fn done(&self) -> bool {
        self.exhausted && self.pending.is_empty()
    }

    /// Forget the cursor and what is pending (after a reconnect or a
    /// snapshot that aged out): the next fill reopens after `last`.
    pub fn reopen(&mut self) {
        self.cursor = None;
        self.pending.clear();
        self.exhausted = false;
    }

    /// The best-effort `killCursors` on close.
    pub async fn kill(&mut self, client: &mut Client, db: &str) {
        if let Some(id) = self.cursor.take() {
            let _ = client
                .command(
                    db,
                    doc! { "killCursors" => self.collection.clone(), "cursors" => vec![Bson::Int64(id)] },
                )
                .await;
        }
    }
}

/// Whether `e` says the snapshot's cluster time is older than the server
/// keeps (`minSnapshotHistoryWindowInSeconds`, five minutes by default).
pub fn snapshot_too_old(e: &ConnectorError) -> bool {
    server_error(e).is_some_and(|s| {
        s.code == codes::SNAPSHOT_TOO_OLD
            || s.code == codes::SNAPSHOT_UNAVAILABLE
            || (s.code == codes::BAD_VALUE && s.message.contains("atClusterTime"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_resume_after_the_last_id() {
        let mut s = Scan::new("c", true);
        assert_eq!(s.filter(), Document::new());
        s.last = Some(Bson::Int32(5));
        // Unbounded: $expr compares across type brackets, as the sort does.
        assert!(s.filter().contains_key("$expr"));
        s.lo = Some(Bson::Int32(0));
        s.hi = Some(Bson::Int32(10));
        assert_eq!(
            s.filter(),
            doc! { "_id" => doc! { "$gt" => 5, "$lt" => 10 } }
        );
        s.last = None;
        assert_eq!(
            s.filter(),
            doc! { "_id" => doc! { "$gte" => 0, "$lt" => 10 } }
        );
    }

    #[test]
    fn take_moves_last() {
        let mut s = Scan::new("c", false);
        s.pending.extend([
            doc! { "_id" => 1 },
            doc! { "_id" => 2 },
            doc! { "_id" => 3 },
        ]);
        assert_eq!(s.take(2).len(), 2);
        assert_eq!(s.last, Some(Bson::Int32(2)));
        assert!(!s.done());
        s.exhausted = true;
        s.take(5);
        assert!(s.done());
        assert_eq!(s.last, Some(Bson::Int32(3)));
    }
}
