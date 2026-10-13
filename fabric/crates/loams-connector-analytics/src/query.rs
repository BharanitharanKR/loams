//! A query read as pages: the shared batch source of Trino (`nextUri`),
//! Redshift's Data API (`NextToken`) and Databricks (result chunks).
//!
//! The source is bounded: one split per instance (`query`), read once. The
//! position is the cursor of the page being read, the rows already taken
//! from it, and the rows taken in all; so a restart re-fetches that page and
//! skips what the Fabric already has. A system that has forgotten the
//! cursor (Trino's token acknowledged, a Redshift result expired, a
//! Databricks statement closed) answers [`Fetch::Gone`], and the query is
//! run again and the rows already taken are skipped: exact when the query's
//! order is deterministic (`ORDER BY`), at-least-once otherwise (R17.9).
//!
//! Event ids are design §33 §6's batch-read ids: the run id (instance,
//! query, as-of time of the first open) and the global row range, so a
//! re-read yields the same ids and `ingest` drops them (D334).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, ConnectorError, EnvelopeBuilder, EventBatch, InstanceKey, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState, ids,
};
use serde::{Deserialize, Serialize};

use crate::rows::take;

/// The one split of a query source.
pub const QUERY_SPLIT: &str = "query";

const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// Re-runs in a row before the source gives up (as `Retryable`).
const MAX_RERUNS: u32 = 3;

/// A page of results.
#[derive(Debug, Clone)]
pub struct Page {
    /// The cursor that returned this page (re-fetching it returns it again,
    /// while the system remembers it).
    pub at: serde_json::Value,
    pub rows: RecordBatch,
    /// The next page's cursor; `None` at the end.
    pub next: Option<serde_json::Value>,
}

/// What fetching a cursor gave.
#[derive(Debug, Clone)]
pub enum Fetch {
    Page(Page),
    /// The results are complete and nothing more follows.
    End,
    /// The system no longer has the cursor: run the query again.
    Gone,
}

/// A system's paging protocol.
#[async_trait]
pub trait PagedQuery: Send {
    /// Run the query; the first page (or the end).
    async fn start(&mut self) -> Result<Fetch, ConnectorError>;
    /// Fetch the page a cursor names.
    async fn fetch(&mut self, cursor: &serde_json::Value) -> Result<Fetch, ConnectorError>;
    /// The query text, for the run id.
    fn query(&self) -> &str;
}

/// Where a query source is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPosition {
    /// The as-of of the run (seconds since the epoch at the first open).
    pub as_of: String,
    /// The cursor of the page being read; `None` before the first page and
    /// after the end.
    pub cursor: Option<serde_json::Value>,
    /// Rows of that page already taken.
    pub taken: u64,
    /// Rows taken in all.
    pub rows: u64,
    pub done: bool,
}

impl QueryPosition {
    pub fn to_position(&self) -> Position {
        let display = if self.done {
            format!("done after {} rows", self.rows)
        } else {
            format!("{} rows", self.rows)
        };
        Position::new(serde_json::to_vec(self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("query position: {e}")))
    }
}

/// The source over a [`PagedQuery`].
pub struct QuerySource {
    query: Box<dyn PagedQuery>,
    envelope: EnvelopeBuilder,
    instance: InstanceKey,
    resource: String,
    pos: QueryPosition,
    page: Option<Page>,
    /// Re-runs since the last page that was read.
    reruns: u32,
    /// Rows still to skip after a re-run.
    skip: u64,
    opened: bool,
}

impl std::fmt::Debug for QuerySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuerySource")
            .field("resource", &self.resource)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

fn now_secs() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        .to_string()
}

impl QuerySource {
    pub fn new(
        query: Box<dyn PagedQuery>,
        connector: &str,
        instance: &InstanceKey,
        resource: &str,
    ) -> Self {
        Self {
            query,
            envelope: EnvelopeBuilder::new(connector, instance),
            instance: instance.clone(),
            resource: resource.to_owned(),
            pos: QueryPosition {
                as_of: String::new(),
                cursor: None,
                taken: 0,
                rows: 0,
                done: false,
            },
            page: None,
            skip: 0,
            reruns: 0,
            opened: false,
        }
    }

    /// Run the query again and skip the rows already taken.
    async fn rerun(&mut self) -> Result<(), ConnectorError> {
        self.reruns += 1;
        if self.reruns > MAX_RERUNS {
            return Err(ConnectorError::retryable(format!(
                "{}: the system keeps forgetting the query's results",
                self.resource
            )));
        }
        self.skip = self.pos.rows;
        self.pos.taken = 0;
        let first = self.query.start().await?;
        self.accept(first)
    }

    fn accept(&mut self, f: Fetch) -> Result<(), ConnectorError> {
        match f {
            Fetch::Page(p) => {
                self.pos.cursor = Some(p.at.clone());
                self.page = Some(p);
                Ok(())
            }
            Fetch::End => {
                self.page = None;
                self.pos.cursor = None;
                self.pos.done = true;
                Ok(())
            }
            Fetch::Gone => Err(ConnectorError::retryable(
                "the query's results were forgotten while it was being re-run",
            )),
        }
    }

    async fn load_current(&mut self) -> Result<(), ConnectorError> {
        match &self.pos.cursor {
            None => {
                let first = self.query.start().await?;
                self.accept(first)
            }
            Some(c) => {
                let c = c.clone();
                match self.query.fetch(&c).await? {
                    Fetch::Gone => self.rerun().await,
                    other => self.accept(other),
                }
            }
        }
    }
}

#[async_trait]
impl Source for QuerySource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.pos = match pos {
            Some(p) => QueryPosition::from_position(&p)?,
            None => QueryPosition {
                as_of: now_secs(),
                cursor: None,
                taken: 0,
                rows: 0,
                done: false,
            },
        };
        self.page = None;
        self.skip = 0;
        self.opened = false;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.pos.done {
                return Ok(None);
            }
            if !self.opened {
                self.load_current().await?;
                self.opened = true;
                continue;
            }
            let Some(page) = self.page.clone() else {
                self.load_current().await?;
                continue;
            };
            let total = page.rows.num_rows() as u64;
            // Skip what a re-run must not deliver twice.
            if self.skip > 0 {
                let here = (total - self.pos.taken).min(self.skip);
                self.skip -= here;
                self.pos.taken += here;
            }
            if self.pos.taken >= total {
                match page.next {
                    Some(next) => {
                        self.pos.cursor = Some(next);
                        self.pos.taken = 0;
                        self.page = None;
                    }
                    None => {
                        self.page = None;
                        self.pos.cursor = None;
                        self.pos.done = true;
                        // The end, as a position: nothing left to read.
                        return Ok(None);
                    }
                }
                continue;
            }
            let Some(rows) = take(
                &page.rows,
                self.pos.taken as usize,
                budget.max_rows,
                budget.max_bytes,
            )?
            else {
                return Err(ConnectorError::poison(format!(
                    "a row of {} is larger than the batch budget",
                    self.resource
                )));
            };
            let n = rows.num_rows() as u64;
            self.reruns = 0;
            let first = self.pos.rows;
            self.pos.taken += n;
            self.pos.rows += n;
            let run = ids::run_id(&self.instance, self.query.query(), &self.pos.as_of);
            let envelope = self
                .envelope
                .event(
                    "rows",
                    &self.resource,
                    ids::rows(&run, 0, first, first + n - 1),
                )
                .with_subject(self.resource.clone())
                .with_data(ARROW_STREAM, Bytes::new());
            let mut at = self.pos.clone();
            if at.taken >= total {
                // The page is done: name the next one, or the end.
                match &page.next {
                    Some(next) => {
                        at.cursor = Some(next.clone());
                        at.taken = 0;
                    }
                    None => {
                        at.cursor = None;
                        at.done = true;
                    }
                }
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Arrow {
                    batch: rows,
                    envelope,
                },
                position: at.to_position(),
            }));
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.page = None;
        Ok(())
    }
}

/// One split: the query.
#[derive(Debug, Default)]
pub struct QueryEnumerator;

#[async_trait]
impl SplitEnumerator for QueryEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(QUERY_SPLIT, serde_json::json!({}))])
    }
}

/// Makes a [`QuerySource`] with a fresh [`PagedQuery`] per split.
pub struct QuerySourceFactory {
    make: Arc<dyn Fn() -> QuerySource + Send + Sync>,
}

impl std::fmt::Debug for QuerySourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuerySourceFactory")
    }
}

impl QuerySourceFactory {
    pub fn new(make: impl Fn() -> QuerySource + Send + Sync + 'static) -> Self {
        Self {
            make: Arc::new(make),
        }
    }
}

impl SourceFactory for QuerySourceFactory {
    fn create(
        &self,
        _split: &Split,
    ) -> Result<Box<dyn loams_connector_sdk::Source>, ConnectorError> {
        Ok(Box::new((self.make)()))
    }
}

/// Wait between polls of a statement that is still running.
pub async fn poll_pause(attempt: u32) {
    let ms = (50u64 << attempt.min(5)).min(2_000);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    /// Pages of 0..n in pages of `per`; cursors are page numbers; cursors
    /// below `forget_below` are Gone (an acknowledged Trino token).
    struct Fake {
        n: i64,
        per: i64,
        forget_below: i64,
        starts: u32,
    }

    impl Fake {
        fn page(&self, k: i64) -> Fetch {
            let from = k * self.per;
            if from >= self.n {
                return Fetch::End;
            }
            let to = (from + self.per).min(self.n);
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            let rows = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from((from..to).collect::<Vec<_>>()))],
            )
            .expect("batch");
            Fetch::Page(Page {
                at: serde_json::json!(k),
                rows,
                next: (to < self.n).then(|| serde_json::json!(k + 1)),
            })
        }
    }

    #[async_trait]
    impl PagedQuery for Fake {
        async fn start(&mut self) -> Result<Fetch, ConnectorError> {
            self.starts += 1;
            Ok(self.page(0))
        }
        async fn fetch(&mut self, c: &serde_json::Value) -> Result<Fetch, ConnectorError> {
            let k = c.as_i64().expect("page number");
            // Only a previous run's cursors are forgotten.
            if self.starts == 0 && k < self.forget_below {
                return Ok(Fetch::Gone);
            }
            Ok(self.page(k))
        }
        fn query(&self) -> &str {
            "SELECT x FROM t ORDER BY x"
        }
    }

    async fn drain(src: &mut QuerySource, max_rows: usize) -> Vec<(Vec<i64>, Position)> {
        let mut out = Vec::new();
        let budget = Budget {
            max_rows,
            max_bytes: 1 << 20,
        };
        while let Some(b) = src.next(budget).await.expect("next") {
            let EventBatch::Arrow { batch, .. } = b.events else {
                panic!("arrow")
            };
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .values()
                .to_vec();
            out.push((col, b.position));
        }
        out
    }

    #[tokio::test]
    async fn pages_resume_and_rerun_when_gone() {
        let split = Split::new(QUERY_SPLIT, serde_json::json!({}));
        let i = InstanceKey::new(1, "q");
        let fake = || Fake {
            n: 10,
            per: 4,
            forget_below: 0,
            starts: 0,
        };
        let mut s = QuerySource::new(Box::new(fake()), "trino", &i, "query");
        s.open(&split, None).await.expect("open");
        let all = drain(&mut s, 3).await;
        let flat: Vec<i64> = all.iter().flat_map(|(v, _)| v.clone()).collect();
        assert_eq!(flat, (0..10).collect::<Vec<_>>());

        // Resume from the third batch (mid page 1): the cursor is still
        // known, so no re-run.
        let at = all[2].1.clone();
        let mut s = QuerySource::new(Box::new(fake()), "trino", &i, "query");
        s.open(&split, Some(at.clone())).await.expect("open");
        let rest: Vec<i64> = drain(&mut s, 3)
            .await
            .into_iter()
            .flat_map(|(v, _)| v)
            .collect();
        let done_so_far: Vec<i64> = all[..3].iter().flat_map(|(v, _)| v.clone()).collect();
        assert_eq!(
            [done_so_far.clone(), rest].concat(),
            (0..10).collect::<Vec<_>>()
        );

        // The same, after the system forgot every cursor: a re-run that
        // skips what was taken.
        let mut s = QuerySource::new(
            Box::new(Fake {
                forget_below: 99,
                ..fake()
            }),
            "trino",
            &i,
            "query",
        );
        s.open(&split, Some(at)).await.expect("open");
        let rest: Vec<i64> = drain(&mut s, 3)
            .await
            .into_iter()
            .flat_map(|(v, _)| v)
            .collect();
        assert_eq!([done_so_far, rest].concat(), (0..10).collect::<Vec<_>>());
    }
}
