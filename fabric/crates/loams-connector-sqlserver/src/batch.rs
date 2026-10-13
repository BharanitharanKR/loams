//! Batch reads (`mode: batch`): Arrow batches of `rows` events (D356).
//!
//! **Key ranges.** A table is one split, or with `partitions: n` and a
//! single integer key, `n` splits over `[MIN(key), MAX(key)]`. A split reads
//! its range in key order, `SELECT TOP (n) … WHERE (key) > (last) ORDER BY
//! key`, inside one `SNAPSHOT` transaction per run (or `READ COMMITTED` with
//! `isolation: read_committed`). The position is the run's as-of, the last
//! key delivered and the row count, so a restart resumes after the last key
//! (in a new snapshot); ids are `ids::rows(run_id(instance, split, as_of), 0,
//! first, last)`, the same across re-reads of a run.
//!
//! **Incremental** (`cursor_column`): one split per table, ordered by the
//! cursor column then the key, each page a statement of its own. At the end
//! the split waits `poll_interval_ms` and reads on from its position (an
//! unbounded source), or finishes when no interval is set. Rows whose cursor
//! is NULL are never read. The run id is fixed by the cursor column, and the
//! row count carries across polls, so ids stay stable.

use std::collections::VecDeque;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::envelope::ids;
use loams_connector_sdk::schema::SchemaPolicy;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tiberius::ToSql;

use crate::config::{Isolation, SqlConfig, TableName};
use crate::conn::{SqlClient, cells, connect, exec, rows};
use crate::table::{TableInfo, after, batch_within, describe, order_by, row_cost};
use crate::types::{Cell, quote_ident};

/// The media type of an Arrow batch's envelope.
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// A batch split's position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPos {
    /// The run's as-of (key ranges), or `cursor:<column>` (incremental).
    pub as_of: String,
    /// The last row's key (with the cursor first, incremental), as text.
    #[serde(default)]
    pub last: Option<Vec<Option<String>>>,
    pub rows: u64,
    #[serde(default)]
    pub done: bool,
}

impl BatchPos {
    pub fn to_position(&self) -> Position {
        let display = if self.done {
            format!("as of {}: done, {} rows", self.as_of, self.rows)
        } else {
            format!("as of {}: {} rows", self.as_of, self.rows)
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("not a SQL Server batch position: {e}")))
    }
}

/// An inclusive key range of an integer key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub lo: i64,
    pub hi: i64,
}

/// `[min, max]` cut into at most `n` contiguous, non-empty ranges.
pub fn key_ranges(min: i64, max: i64, n: u32) -> Vec<KeyRange> {
    if min > max {
        return Vec::new();
    }
    let span = (i128::from(max) - i128::from(min)) + 1;
    let n = i128::from(n.max(1)).min(span);
    let step = span / n;
    let rem = span % n;
    let mut out = Vec::new();
    let mut lo = i128::from(min);
    for i in 0..n {
        let len = step + i128::from(i < rem);
        let hi = lo + len - 1;
        out.push(KeyRange {
            lo: lo as i64,
            hi: hi as i64,
        });
        lo = hi + 1;
    }
    out
}

/// The splits of the configured tables.
pub struct BatchEnumerator {
    cfg: SqlConfig,
    ctx: ConnectorContext,
}

impl std::fmt::Debug for BatchEnumerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchEnumerator")
            .field("tables", &self.cfg.tables)
            .finish_non_exhaustive()
    }
}

impl BatchEnumerator {
    pub fn new(cfg: &SqlConfig, ctx: &ConnectorContext) -> Self {
        Self {
            cfg: cfg.clone(),
            ctx: ctx.clone(),
        }
    }
}

fn whole(t: &TableName) -> Split {
    Split::new(t.to_string(), json!({ "table": t.to_string() }))
}

#[async_trait]
impl SplitEnumerator for BatchEnumerator {
    async fn discover(&mut self, known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        if self.cfg.tables.is_empty() {
            return Err(ConnectorError::fatal("config: tables is required"));
        }
        if self.cfg.partitions <= 1 || self.cfg.cursor_column.is_some() {
            return Ok(self.cfg.tables.iter().map(whole).collect());
        }
        let mut out = Vec::new();
        let mut client: Option<SqlClient> = None;
        for t in &self.cfg.tables {
            // Ranges are fixed at the first discovery: a split keeps its
            // range (and its checkpoint) for the life of the instance.
            let mine: Vec<Split> = known
                .iter()
                .filter(|s| s.split.spec["table"] == t.to_string())
                .map(|s| s.split.clone())
                .collect();
            if !mine.is_empty() {
                out.extend(mine);
                continue;
            }
            if client.is_none() {
                client = Some(connect(&self.cfg, &self.ctx).await?);
            }
            let c = client.as_mut().expect("set above");
            let info = describe(
                c,
                t,
                &self.cfg.key,
                &SchemaPolicy::from_config(&self.ctx.config)?.lossy,
            )
            .await?;
            let integer = info.key.len() == 1
                && matches!(
                    info.column(&info.key[0]).map(|c| c.base()).as_deref(),
                    Some("tinyint" | "smallint" | "int" | "bigint")
                );
            if !integer {
                out.push(whole(t));
                continue;
            }
            let k = quote_ident(&info.key[0]);
            let r = rows(
                c,
                &format!(
                    "SELECT CAST(MIN({k}) AS bigint), CAST(MAX({k}) AS bigint) FROM {}",
                    t.quoted()
                ),
                &[],
                &format!("reading the key range of {t}"),
            )
            .await?;
            let (min, max) = r
                .first()
                .map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1)))
                .unwrap_or((None, None));
            let (Some(min), Some(max)) = (min, max) else {
                out.push(whole(t));
                continue;
            };
            let ranges = key_ranges(min, max, self.cfg.partitions);
            let n = ranges.len();
            for (i, r) in ranges.into_iter().enumerate() {
                // The first and last ranges are open-ended, so rows inserted
                // outside [min, max] after discovery are still read.
                let lo = (i > 0).then_some(r.lo);
                let hi = (i + 1 < n).then_some(r.hi);
                out.push(Split::new(
                    format!("{t}#{}of{n}", i + 1),
                    json!({ "table": t.to_string(), "lo": lo, "hi": hi }),
                ));
            }
        }
        Ok(out)
    }
}

#[derive(Debug)]
pub struct BatchFactory {
    cfg: SqlConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl BatchFactory {
    pub fn new(cfg: SqlConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for BatchFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let table = split.spec["table"]
            .as_str()
            .map(TableName::parse)
            .unwrap_or_else(|| TableName::parse(split.id.as_str()))?;
        Ok(Box::new(BatchSource {
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            policy: SchemaPolicy::from_config(&self.ctx.config)?,
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            name: table,
            split: split.id.as_str().to_owned(),
            lo: split.spec["lo"].as_i64(),
            hi: split.spec["hi"].as_i64(),
            client: None,
            table: None,
            pos: None,
            pending: VecDeque::new(),
            page_done: false,
            avg_cost: 0,
            idle_until: None,
        }))
    }
}

/// Reads one split.
pub struct BatchSource {
    cfg: SqlConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    name: TableName,
    split: String,
    lo: Option<i64>,
    hi: Option<i64>,
    client: Option<SqlClient>,
    table: Option<TableInfo>,
    pos: Option<BatchPos>,
    pending: VecDeque<Vec<Cell>>,
    page_done: bool,
    avg_cost: usize,
    /// Incremental: no read before this instant (the poll interval).
    idle_until: Option<Instant>,
}

impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("split", &self.split)
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

/// The page query of a split: its columns, range and paging, with the
/// parameters' text (`@P1` is the page size).
pub fn page_sql(
    table: &TableInfo,
    order: &[String],
    range: (Option<i64>, Option<i64>),
    last: Option<&[Option<String>]>,
) -> Result<String, ConnectorError> {
    let mut wh = Vec::new();
    if let (Some(lo), Some(k)) = (range.0, table.key.first()) {
        wh.push(format!("{} >= {lo}", quote_ident(k)));
    }
    if let (Some(hi), Some(k)) = (range.1, table.key.first()) {
        wh.push(format!("{} <= {hi}", quote_ident(k)));
    }
    if order.len() > table.key.len() {
        // Incremental: rows with a NULL cursor have no place in the order.
        wh.push(format!("{} IS NOT NULL", quote_ident(&order[0])));
    }
    if last.is_some() {
        wh.push(after(&table.declared(order)?, 2));
    }
    let wh = if wh.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", wh.join(" AND "))
    };
    Ok(format!(
        "SELECT TOP (@P1) {} FROM {}{wh} {}",
        table.select_list(),
        table.name.quoted(),
        order_by(order)
    ))
}

impl BatchSource {
    fn order(&self, table: &TableInfo) -> Vec<String> {
        let mut o: Vec<String> = self.cfg.cursor_column.iter().cloned().collect();
        o.extend(
            table
                .key
                .iter()
                .filter(|k| Some(*k) != self.cfg.cursor_column.as_ref())
                .cloned(),
        );
        o
    }

    async fn fetch(&mut self, limit: usize) -> Result<bool, ConnectorError> {
        let table = self.table.as_ref().expect("open");
        let order = self.order(table);
        let pos = self.pos.as_ref().expect("open");
        let sql = page_sql(table, &order, (self.lo, self.hi), pos.last.as_deref())?;
        let limit = limit as i64;
        let mut params: Vec<&dyn ToSql> = vec![&limit];
        if let Some(last) = &pos.last {
            params.extend(last.iter().map(|v| v as &dyn ToSql));
        }
        let what = format!("reading {}", self.name);
        let client = self.client.as_mut().expect("open");
        let got = rows(client, &sql, &params, &what).await?;
        let n = got.len();
        for r in &got {
            self.pending.push_back(cells(r));
        }
        if n > 0 {
            let cost: usize = self.pending.iter().map(|r| row_cost(r)).sum();
            self.avg_cost = cost / self.pending.len().max(1);
        }
        Ok(n as i64 == limit)
    }

    fn last_of(
        &self,
        table: &TableInfo,
        row: &[Cell],
    ) -> Result<Vec<Option<String>>, ConnectorError> {
        Ok(table
            .indexes(&self.order(table))?
            .into_iter()
            .map(|i| row[i].to_text())
            .collect())
    }
}

#[async_trait]
impl Source for BatchSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let mut client = connect(&self.cfg, &self.ctx).await?;
        let table = describe(&mut client, &self.name, &self.cfg.key, &self.policy.lossy).await?;
        if table.key.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "{} has no primary key: set `key` to the columns to read it in order by",
                self.name
            )));
        }
        if let Some(c) = &self.cfg.cursor_column {
            table.indexes(std::slice::from_ref(c))?;
        }
        let iso = match self.cfg.isolation {
            Isolation::Snapshot => "SNAPSHOT",
            Isolation::ReadCommitted => "READ COMMITTED",
        };
        let resume = pos.as_ref().map(BatchPos::from_position).transpose()?;
        let as_of = if let Some(c) = &self.cfg.cursor_column {
            exec(
                &mut client,
                &format!("SET TRANSACTION ISOLATION LEVEL {iso}"),
                "isolation",
            )
            .await?;
            format!("cursor:{c}")
        } else {
            exec(
                &mut client,
                &format!("SET TRANSACTION ISOLATION LEVEL {iso}; BEGIN TRANSACTION"),
                "starting the read",
            )
            .await?;
            match &resume {
                Some(p) => p.as_of.clone(),
                None => {
                    let r = rows(
                        &mut client,
                        "SELECT CONVERT(nvarchar(40), SYSUTCDATETIME(), 127)",
                        &[],
                        "reading the server time",
                    )
                    .await?;
                    r.first()
                        .and_then(|r| crate::conn::text_at(r, 0))
                        .unwrap_or_default()
                }
            }
        };
        self.page_done = resume.as_ref().is_some_and(|p| p.done);
        self.pos = Some(resume.unwrap_or(BatchPos {
            as_of,
            last: None,
            rows: 0,
            done: false,
        }));
        self.table = Some(table);
        self.client = Some(client);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.client.is_none() || self.pos.is_none() {
            return Err(ConnectorError::fatal("the batch source is not open"));
        }
        if self.pos.as_ref().is_some_and(|p| p.done) {
            return Ok(None);
        }
        let incremental = self.cfg.cursor_column.is_some();
        if self.idle_until.is_some_and(|t| Instant::now() < t) {
            return Ok(None);
        }
        if self.pending.is_empty() && !self.page_done {
            let by_bytes = (budget.max_bytes / 2)
                .checked_div(self.avg_cost)
                .map_or(1000, |n| n.max(1));
            let limit = budget.max_rows.min(by_bytes).clamp(1, 50_000);
            let full = self.fetch(limit).await?;
            self.page_done = !full;
        }
        let table = self.table.clone().expect("open");
        match batch_within(&table, &mut self.pending, budget, 512)? {
            Some((batch, last)) => {
                let last = self.last_of(&table, &last)?;
                let pos = self.pos.as_mut().expect("open");
                let first = pos.rows;
                pos.rows += batch.num_rows() as u64;
                pos.last = Some(last);
                if self.pending.is_empty() && self.page_done {
                    if incremental {
                        self.page_done = false;
                        if let Some(every) = self.cfg.poll_interval {
                            self.idle_until = Some(Instant::now() + every);
                        } else {
                            pos.done = true;
                        }
                    } else {
                        pos.done = true;
                    }
                }
                let run = ids::run_id(&self.ctx.instance, &self.split, &pos.as_of);
                let mut envelope = self
                    .envelope
                    .event(
                        "rows",
                        &table.name.to_string(),
                        ids::rows(&run, 0, first, pos.rows - 1),
                    )
                    .with_subject(table.name.to_string());
                envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
                Ok(Some(SourceBatch {
                    events: EventBatch::Arrow { batch, envelope },
                    position: pos.to_position(),
                }))
            }
            None if self.page_done => {
                let pos = self.pos.as_mut().expect("open");
                if incremental {
                    self.page_done = false;
                    match self.cfg.poll_interval {
                        Some(every) => {
                            self.idle_until = Some(Instant::now() + every);
                            return Ok(None);
                        }
                        None => pos.done = true,
                    }
                } else {
                    pos.done = true;
                }
                Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: pos.to_position(),
                }))
            }
            None => Ok(None),
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut c) = self.client.take() {
            let _ = exec(&mut c, "IF @@TRANCOUNT > 0 COMMIT", "ending the read").await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, SqlServerTypeMapper, arrow_schema};

    fn table() -> TableInfo {
        let col = |n: &str, t: &str| Column {
            name: n.into(),
            declared: t.into(),
            nullable: false,
            identity: false,
            generated: false,
        };
        let columns = vec![
            col("id", "bigint"),
            col("v", "nvarchar(max)"),
            col("updated", "int"),
        ];
        TableInfo {
            name: TableName::parse("dbo.items").expect("name"),
            schema: arrow_schema("dbo.items", &columns, &SqlServerTypeMapper::default())
                .expect("schema"),
            columns,
            key: vec!["id".into()],
        }
    }

    #[test]
    fn positions_round_trip_and_never_hold_config() {
        let p = BatchPos {
            as_of: "2026-10-13T10:00:00Z".into(),
            last: Some(vec![Some("eu".into()), Some("42".into())]),
            rows: 1000,
            done: false,
        };
        let pos = p.to_position();
        assert_eq!(BatchPos::from_position(&pos).expect("round trip"), p);
        assert_eq!(pos.display, "as of 2026-10-13T10:00:00Z: 1000 rows");
    }

    #[test]
    fn key_ranges_cover_the_span_without_gaps() {
        let r = key_ranges(1, 10, 3);
        assert_eq!(
            r,
            vec![
                KeyRange { lo: 1, hi: 4 },
                KeyRange { lo: 5, hi: 7 },
                KeyRange { lo: 8, hi: 10 }
            ]
        );
        assert_eq!(key_ranges(5, 6, 8).len(), 2);
        assert!(key_ranges(2, 1, 4).is_empty());
        let wide = key_ranges(i64::MIN, i64::MAX, 4);
        assert_eq!(wide.first().map(|r| r.lo), Some(i64::MIN));
        assert_eq!(wide.last().map(|r| r.hi), Some(i64::MAX));
    }

    #[test]
    fn page_queries_bound_the_range_and_page_by_key() {
        let t = table();
        assert_eq!(
            page_sql(&t, &["id".into()], (None, None), None).expect("sql"),
            "SELECT TOP (@P1) [id], [v], [updated] FROM [dbo].[items] ORDER BY [id]"
        );
        let last = [Some("7".to_owned())];
        assert_eq!(
            page_sql(&t, &["id".into()], (Some(1), Some(10)), Some(&last)).expect("sql"),
            "SELECT TOP (@P1) [id], [v], [updated] FROM [dbo].[items] WHERE [id] >= 1 AND [id] <= 10 \
             AND (([id] > CAST(@P2 AS bigint))) ORDER BY [id]"
        );
        let inc = page_sql(
            &t,
            &["updated".into(), "id".into()],
            (None, None),
            Some(&[None, None]),
        )
        .expect("sql");
        assert!(inc.contains("[updated] IS NOT NULL"), "{inc}");
        assert!(inc.ends_with("ORDER BY [updated], [id]"), "{inc}");
    }
}
