//! Native CDC (OR-9) over SQL Server's change tables (`mode: cdc`). One split
//! per configured table; CDC must be enabled on the database and the table
//! (`docs/runbooks/connectors/sqlserver-cdc.md`).
//!
//! **Snapshot.** With `snapshot: initial` (the default) a new split records
//! `sys.fn_cdc_get_max_lsn()` as its consistent point, then reads the table
//! in key order, page by page, under `SNAPSHOT` isolation: Arrow batches of
//! `change` events with `loamsop = r`. Every change harvested at or below the
//! point was committed before the first page, so it is in the snapshot; every
//! later change has a larger LSN and is streamed. A change committed but not
//! yet harvested when the point is read is both read and streamed (an
//! overlap a fold absorbs), never lost. A restart mid-snapshot resumes after
//! the last key delivered.
//!
//! **Stream.** Each poll reads `cdc.fn_cdc_get_all_changes_<ci>(from, to,
//! N'all update old')` up to `sys.fn_cdc_get_max_lsn()`, strictly after the
//! position, in `(__$start_lsn, __$seqval, __$operation)` order, at most a
//! budget's worth (`TOP`). The position is that **LSN triple** of the last
//! row delivered, so a restart resumes exactly after it; an update's before
//! (3) and after (4) images become one `u` event, and a page never ends
//! between them. When nothing is pending the position moves up to the
//! maximum LSN, as a keepalive, so the cleanup job's low-water mark never
//! passes a position that has read everything.
//!
//! **DDL.** SQL Server allows two capture instances per table: after an
//! `ALTER TABLE` the operator creates a second one (the runbook). The source
//! reads the old instance up to the new one's `start_lsn`, then follows the
//! new one and emits a `schema-change` (Task 5's tracker and policy decide
//! whether the change is accepted or pauses the instance).
//!
//! **Retention.** A position below `sys.fn_cdc_get_min_lsn` was cleaned up:
//! `PositionLost`, with the re-snapshot hint. The position's age against the
//! cleanup job's retention is reported every `metrics_interval_ms` as
//! `position_risk` (and the lag behind the maximum LSN as `lag`).
//!
//! Event ids are `<capture instance>/<start_lsn>/<seqval>/<operation>`
//! (hex), stable across re-reads; `loamslsn` is `<start_lsn>/<seqval>`.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::schema::{SchemaPolicy, SchemaTracker};
use loams_connector_sdk::{
    Budget, CdcOp, CloudEvent, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch,
    Position, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitId, SplitState,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tiberius::ToSql;

use crate::batch::ARROW_STREAM;
use crate::config::{Isolation, SqlConfig, TableName};
use crate::conn::{self, SqlClient, cells, connect, exec, rows};
use crate::table::{TableInfo, after, batch_within, order_by, row_cost};
use crate::types::{Cell, SqlServerTypeMapper, arrow_schema, quote_ident};

/// A 10-byte SQL Server LSN (or `__$seqval`), big-endian, so byte order is
/// LSN order.
pub type Lsn = [u8; 10];

/// The lowest and highest values of a triple's parts.
pub const LSN_ZERO: Lsn = [0; 10];
pub const LSN_MAX: Lsn = [0xFF; 10];

/// The cleanup job's default retention (three days), used when the
/// instance cannot read `msdb.dbo.cdc_jobs` and sets no `retention_minutes`.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(4320 * 60);

pub fn lsn_hex(l: &[u8]) -> String {
    l.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn parse_lsn(s: &str) -> Result<Lsn, ConnectorError> {
    let bad = || ConnectorError::fatal(format!("not an LSN: {s:?}"));
    if s.len() != 20 {
        return Err(bad());
    }
    let mut out = LSN_ZERO;
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| bad())?;
    }
    Ok(out)
}

fn to_lsn(b: &[u8]) -> Result<Lsn, ConnectorError> {
    b.try_into()
        .map_err(|_| ConnectorError::fatal(format!("an LSN of {} bytes", b.len())))
}

/// A CDC split's position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum CdcPos {
    /// Snapshot rows up to `last` (the key, as text) delivered; streaming
    /// starts after `at`, the maximum LSN read before the first page.
    Snapshot {
        ci: String,
        at: String,
        #[serde(default)]
        last: Option<Vec<Option<String>>>,
        rows: u64,
    },
    /// Every change of `ci` up to the triple `(lsn, seq, op)` delivered.
    Stream {
        ci: String,
        lsn: String,
        seq: String,
        op: u8,
    },
}

impl CdcPos {
    /// The streaming start after a snapshot at `at` of a capture instance
    /// starting at `start`: strictly after `at`, or from `start` inclusive
    /// when nothing at or above `start` had been harvested yet.
    pub fn after_snapshot(ci: &str, at: Lsn, start: Lsn) -> Self {
        if at >= start {
            Self::stream(ci, at, LSN_MAX, u8::MAX)
        } else {
            Self::stream(ci, start, LSN_ZERO, 0)
        }
    }

    pub fn stream(ci: &str, lsn: Lsn, seq: Lsn, op: u8) -> Self {
        CdcPos::Stream {
            ci: ci.to_owned(),
            lsn: lsn_hex(&lsn),
            seq: lsn_hex(&seq),
            op,
        }
    }

    pub fn ci(&self) -> &str {
        match self {
            CdcPos::Snapshot { ci, .. } | CdcPos::Stream { ci, .. } => ci,
        }
    }

    /// The streaming triple (`None` while snapshotting).
    pub fn triple(&self) -> Result<Option<(Lsn, Lsn, u8)>, ConnectorError> {
        match self {
            CdcPos::Snapshot { .. } => Ok(None),
            CdcPos::Stream { lsn, seq, op, .. } => {
                Ok(Some((parse_lsn(lsn)?, parse_lsn(seq)?, *op)))
            }
        }
    }

    /// The LSN whose age the retention metric measures.
    pub fn lsn(&self) -> Result<Lsn, ConnectorError> {
        match self {
            CdcPos::Snapshot { at, .. } => parse_lsn(at),
            CdcPos::Stream { lsn, .. } => parse_lsn(lsn),
        }
    }

    pub fn to_position(&self) -> Position {
        let display = match self {
            CdcPos::Snapshot { ci, at, rows, .. } => {
                format!("{ci}: snapshot at {at}, {rows} rows")
            }
            CdcPos::Stream { ci, lsn, seq, op } => format!("{ci}: {lsn}:{seq}:{op}"),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("not a SQL Server CDC position: {e}")))
    }
}

/// A capture instance of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureInstance {
    pub name: String,
    pub start: Lsn,
}

/// A capture instance's name is `[A-Za-z0-9_]` (SQL Server derives it from
/// `schema_table`); anything else is refused before it reaches SQL text.
pub fn check_capture_instance(name: &str) -> Result<(), ConnectorError> {
    if !name.is_empty()
        && name.len() <= 100
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        Ok(())
    } else {
        Err(ConnectorError::fatal(format!(
            "capture instance {name:?} is not a plain identifier"
        )))
    }
}

/// One row of a change function.
#[derive(Debug, Clone, PartialEq)]
pub struct RawChange {
    pub lsn: Lsn,
    pub seq: Lsn,
    pub op: u8,
    pub cells: Vec<Cell>,
}

/// One change event's content.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub op: CdcOp,
    /// The triple of the change's last row (an update's after image).
    pub lsn: Lsn,
    pub seq: Lsn,
    pub row_op: u8,
    pub before: Option<Vec<Cell>>,
    pub after: Option<Vec<Cell>>,
}

/// Pair `all update old` rows into changes. `full` says the page was cut by
/// `TOP`: a trailing before image (3) then waits for the next page, and the
/// returned count is the rows consumed.
pub fn pair(rows: &[RawChange], full: bool) -> (Vec<Change>, usize) {
    let mut out = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let r = &rows[i];
        let change = |op, before, after, last: &RawChange| Change {
            op,
            lsn: last.lsn,
            seq: last.seq,
            row_op: last.op,
            before,
            after,
        };
        match r.op {
            1 => out.push(change(CdcOp::Delete, Some(r.cells.clone()), None, r)),
            2 => out.push(change(CdcOp::Create, None, Some(r.cells.clone()), r)),
            3 => match rows.get(i + 1) {
                Some(n) if n.op == 4 && n.lsn == r.lsn && n.seq == r.seq => {
                    out.push(change(
                        CdcOp::Update,
                        Some(r.cells.clone()),
                        Some(n.cells.clone()),
                        n,
                    ));
                    i += 1;
                }
                // Cut by the page: read the pair together next time.
                None if full => break,
                // A before image without its after image: nothing to apply.
                _ => {}
            },
            4 => out.push(change(CdcOp::Update, None, Some(r.cells.clone()), r)),
            _ => {}
        }
        i += 1;
    }
    (out, i)
}

fn object(table: &TableInfo, cells: &[Cell]) -> Value {
    Value::Object(
        table
            .columns
            .iter()
            .zip(cells)
            .map(|(c, v)| (c.name.clone(), v.to_json()))
            .collect::<Map<_, _>>(),
    )
}

/// The `change` event of `c`.
pub fn change_event(
    envelope: &EnvelopeBuilder,
    ci: &str,
    table: &TableInfo,
    key: &[usize],
    c: &Change,
) -> CloudEvent {
    let image = c.after.as_ref().or(c.before.as_ref());
    let key_obj: Map<String, Value> = image
        .map(|cells| {
            key.iter()
                .map(|&i| (table.columns[i].name.clone(), cells[i].to_json()))
                .collect()
        })
        .unwrap_or_default();
    let name = table.name.to_string();
    let data = json!({
        "table": name,
        "op": c.op.as_str(),
        "key": key_obj,
        "before": c.before.as_ref().map(|b| object(table, b)),
        "after": c.after.as_ref().map(|a| object(table, a)),
    });
    let (lsn, seq) = (lsn_hex(&c.lsn), lsn_hex(&c.seq));
    envelope
        .event("change", &name, format!("{ci}/{lsn}/{seq}/{}", c.row_op))
        .with_subject(name)
        .with_cdc(c.op, format!("{lsn}/{seq}"))
        .with_data("application/json", data.to_string())
}

/// The change function's query for `ci` with `cols`; parameters: `@P1` the
/// page size, `@P2`/`@P3` from/to, `@P4`–`@P6` the triple to read after,
/// `@P7` (with `before_next`) the next capture instance's start.
pub fn changes_sql(ci: &str, cols: &str, before_next: bool) -> String {
    format!(
        "SELECT TOP (@P1) __$start_lsn, __$seqval, __$operation, {cols} \
         FROM cdc.{}(@P2, @P3, N'all update old') \
         WHERE (__$start_lsn > @P4 OR (__$start_lsn = @P4 AND (__$seqval > @P5 \
         OR (__$seqval = @P5 AND __$operation > @P6)))){} \
         ORDER BY __$start_lsn, __$seqval, __$operation",
        quote_ident(&format!("fn_cdc_get_all_changes_{ci}")),
        if before_next {
            " AND __$start_lsn < @P7"
        } else {
            ""
        }
    )
}

/// The capture instances of `table`, oldest first.
pub async fn capture_instances(
    client: &mut SqlClient,
    table: &TableName,
) -> Result<Vec<CaptureInstance>, ConnectorError> {
    let name = table.quoted();
    let r = rows(
        client,
        "SELECT ct.capture_instance, ct.start_lsn FROM cdc.change_tables ct \
         WHERE ct.source_object_id = OBJECT_ID(@P1) ORDER BY ct.create_date, ct.start_lsn",
        &[&name.as_str()],
        &format!("listing the capture instances of {table}"),
    )
    .await
    .map_err(|e| {
        if e.message.contains("Invalid object name") {
            ConnectorError::fatal(format!(
                "CDC is not enabled on this database: EXEC sys.sp_cdc_enable_db \
                 (docs/runbooks/connectors/sqlserver-cdc.md); {}",
                e.message
            ))
        } else {
            e
        }
    })?;
    r.iter()
        .map(|row| {
            let name = conn::text_at(row, 0).unwrap_or_default();
            check_capture_instance(&name)?;
            Ok(CaptureInstance {
                name,
                start: to_lsn(row.get::<&[u8], _>(1).unwrap_or(&LSN_ZERO))?,
            })
        })
        .collect()
}

/// The table as a capture instance records it: the change table's columns
/// (without the `__$` ones), the source table's key.
pub async fn describe_ci(
    client: &mut SqlClient,
    table: &TableName,
    ci: &str,
    key: &[String],
    policy: &SchemaPolicy,
) -> Result<TableInfo, ConnectorError> {
    let ct = TableName {
        schema: "cdc".into(),
        name: format!("{ci}_CT"),
    };
    let columns: Vec<_> = conn::describe(client, &ct)
        .await?
        .into_iter()
        .filter(|c| !c.name.starts_with("__$"))
        .collect();
    let key = if key.is_empty() {
        conn::primary_key(client, table).await?
    } else {
        key.to_vec()
    };
    if key.is_empty() {
        return Err(ConnectorError::fatal(format!(
            "{table} has no primary key: set `key`"
        )));
    }
    let schema = arrow_schema(
        &table.to_string(),
        &columns,
        &SqlServerTypeMapper::new(policy.lossy.clone()),
    )?;
    let t = TableInfo {
        name: table.clone(),
        columns,
        key,
        schema,
    };
    t.indexes(&t.key).map_err(|_| {
        ConnectorError::fatal(format!(
            "capture instance {ci} does not capture the key of {table}"
        ))
    })?;
    Ok(t)
}

/// One split per configured table.
#[derive(Debug)]
pub struct CdcEnumerator {
    tables: Vec<TableName>,
}

impl CdcEnumerator {
    pub fn new(cfg: &SqlConfig) -> Self {
        Self {
            tables: cfg.tables.clone(),
        }
    }
}

#[async_trait]
impl SplitEnumerator for CdcEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        if self.tables.is_empty() {
            return Err(ConnectorError::fatal("config: tables is required"));
        }
        Ok(self
            .tables
            .iter()
            .map(|t| Split::new(t.to_string(), json!({ "table": t.to_string() })))
            .collect())
    }
}

#[derive(Debug)]
pub struct CdcFactory {
    cfg: SqlConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl CdcFactory {
    pub fn new(cfg: SqlConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for CdcFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let policy = SchemaPolicy::from_config(&self.ctx.config)?;
        Ok(Box::new(CdcSource {
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            tracker: SchemaTracker::new(policy.evolution),
            policy,
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            name: TableName::parse(split.id.as_str())?,
            split: split.id.clone(),
            client: None,
            table: None,
            pos: None,
            pending: VecDeque::new(),
            page_done: false,
            avg_cost: 0,
            idle_until: None,
            next_sample: Instant::now(),
            retention: None,
            pending_schema: None,
        }))
    }
}

/// Reads one table's changes.
pub struct CdcSource {
    cfg: SqlConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    tracker: SchemaTracker,
    name: TableName,
    split: SplitId,
    client: Option<SqlClient>,
    /// The table as the position's capture instance records it.
    table: Option<TableInfo>,
    pos: Option<CdcPos>,
    pending: VecDeque<Vec<Cell>>,
    page_done: bool,
    avg_cost: usize,
    idle_until: Option<Instant>,
    next_sample: Instant,
    retention: Option<Duration>,
    /// A `schema-change` to send before anything else.
    pending_schema: Option<CloudEvent>,
}

impl std::fmt::Debug for CdcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcSource")
            .field("table", &self.name.to_string())
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

fn lost(table: &TableName, why: &str) -> ConnectorError {
    ConnectorError::position_lost(format!(
        "{table}: {why}; the changes after the position were cleaned up. Re-snapshot: delete the \
         instance's checkpoint so it starts again (docs/runbooks/connectors/sqlserver-cdc.md), \
         or raise the cleanup job's retention"
    ))
}

impl CdcSource {
    fn client(&mut self) -> &mut SqlClient {
        self.client.as_mut().expect("open")
    }

    /// `(max LSN, min LSN of ci)`; the maximum is `None` before the capture
    /// job's first harvest.
    async fn bounds(&mut self, ci: &str) -> Result<(Option<Lsn>, Lsn), ConnectorError> {
        let r = rows(
            self.client(),
            "SELECT sys.fn_cdc_get_max_lsn(), sys.fn_cdc_get_min_lsn(@P1)",
            &[&ci],
            "reading the CDC LSN bounds",
        )
        .await?;
        let row = r.first();
        let max = row
            .and_then(|r| r.get::<&[u8], _>(0))
            .map(to_lsn)
            .transpose()?;
        let min = row
            .and_then(|r| r.get::<&[u8], _>(1))
            .map(to_lsn)
            .transpose()?
            .unwrap_or(LSN_ZERO);
        Ok((max, min))
    }

    /// Track `info`'s schema; an accepted change becomes an event.
    fn observe(
        &mut self,
        info: &TableInfo,
        at: &Position,
    ) -> Result<Option<CloudEvent>, ConnectorError> {
        let change = self
            .tracker
            .observe(&self.name.to_string(), info.schema.clone(), at)?;
        Ok(change.map(|c| c.event(&self.envelope)))
    }

    async fn sample(&mut self) -> Result<(), ConnectorError> {
        let Some(pos) = self.pos.clone() else {
            return Ok(());
        };
        let lsn = pos.lsn()?;
        if self.retention.is_none() {
            self.retention = Some(match self.cfg.retention {
                Some(r) => r,
                None => {
                    let r = rows(
                        self.client(),
                        "SELECT CAST(retention AS bigint) FROM msdb.dbo.cdc_jobs \
                         WHERE database_id = DB_ID() AND job_type = N'cleanup'",
                        &[],
                        "reading the cleanup job's retention",
                    )
                    .await;
                    r.ok()
                        .and_then(|r| r.first().and_then(|r| r.get::<i64, _>(0)))
                        .filter(|m| *m > 0)
                        .map_or(DEFAULT_RETENTION, |m| Duration::from_secs(m as u64 * 60))
                }
            });
        }
        let lsn = lsn.to_vec();
        let r = rows(
            self.client(),
            "DECLARE @t datetime = (SELECT MAX(tran_end_time) FROM cdc.lsn_time_mapping WHERE start_lsn <= @P1); \
             DECLARE @m datetime = (SELECT MAX(tran_end_time) FROM cdc.lsn_time_mapping); \
             SELECT DATEDIFF_BIG(MILLISECOND, @t, GETDATE()), DATEDIFF_BIG(MILLISECOND, @t, @m), \
             CAST(CASE WHEN @P1 >= ISNULL(sys.fn_cdc_get_max_lsn(), 0x00) THEN 1 ELSE 0 END AS bit)",
            &[&lsn.as_slice()],
            "sampling CDC lag",
        )
        .await?;
        let Some(row) = r.first() else {
            return Ok(());
        };
        let age = row.get::<i64, _>(0).unwrap_or(0).max(0) as u64;
        let lag = row.get::<i64, _>(1).unwrap_or(0).max(0) as u64;
        let caught_up = row.get::<bool, _>(2).unwrap_or(false);
        let retention = self.retention.unwrap_or(DEFAULT_RETENTION);
        let ratio = if caught_up {
            0.0
        } else {
            retention_use(Duration::from_millis(age), retention)
        };
        let lag = if caught_up { 0 } else { lag };
        let obs = self.ctx.observe.clone();
        obs.lag(
            &self.ctx.instance,
            Some(&self.split),
            Duration::from_millis(lag),
        );
        obs.position_risk(&self.ctx.instance, Some(&self.split), ratio);
        Ok(())
    }

    async fn next_snapshot(
        &mut self,
        budget: Budget,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let table = self.table.clone().expect("open");
        let Some(CdcPos::Snapshot {
            ci,
            at,
            last,
            rows: done_rows,
        }) = self.pos.clone()
        else {
            unreachable!("called in the snapshot phase")
        };
        if self.pending.is_empty() && !self.page_done {
            let by_bytes = (budget.max_bytes / 2)
                .checked_div(self.avg_cost)
                .map_or(1000, |n| n.max(1));
            let limit = budget.max_rows.min(by_bytes).clamp(1, 50_000) as i64;
            let wh = if last.is_some() {
                format!(" WHERE {}", after(&table.declared(&table.key)?, 2))
            } else {
                String::new()
            };
            let sql = format!(
                "SELECT TOP (@P1) {} FROM {}{wh} {}",
                table.select_list(),
                table.name.quoted(),
                order_by(&table.key)
            );
            let mut params: Vec<&dyn ToSql> = vec![&limit];
            if let Some(l) = &last {
                params.extend(l.iter().map(|v| v as &dyn ToSql));
            }
            let what = format!("snapshotting {}", self.name);
            let got = rows(self.client(), &sql, &params, &what).await?;
            self.page_done = (got.len() as i64) < limit;
            for r in &got {
                self.pending.push_back(cells(r));
            }
            if !got.is_empty() {
                let cost: usize = self.pending.iter().map(|r| row_cost(r)).sum();
                self.avg_cost = cost / self.pending.len().max(1);
            }
        }
        match batch_within(&table, &mut self.pending, budget, 512)? {
            Some((batch, last_row)) => {
                let key = table.indexes(&table.key)?;
                let first = done_rows;
                let n = done_rows + batch.num_rows() as u64;
                self.pos = Some(CdcPos::Snapshot {
                    ci: ci.clone(),
                    at: at.clone(),
                    last: Some(key.iter().map(|&i| last_row[i].to_text()).collect()),
                    rows: n,
                });
                let name = table.name.to_string();
                let mut envelope = self
                    .envelope
                    .event("change", &name, format!("{ci}/{at}/r/{first}-{}", n - 1))
                    .with_subject(name)
                    .with_cdc(CdcOp::Read, at.clone());
                envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
                Ok(Some(SourceBatch {
                    events: EventBatch::Arrow { batch, envelope },
                    position: self.pos.as_ref().expect("set").to_position(),
                }))
            }
            None if self.page_done => {
                let cis =
                    capture_instances(self.client.as_mut().expect("open"), &self.name).await?;
                let start = cis
                    .iter()
                    .find(|c| c.name == ci)
                    .map_or(LSN_ZERO, |c| c.start);
                self.pos = Some(CdcPos::after_snapshot(&ci, parse_lsn(&at)?, start));
                Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: self.pos.as_ref().expect("set").to_position(),
                }))
            }
            None => Ok(None),
        }
    }

    /// Follow `next` from `pos`, emitting its schema as a change.
    async fn switch(
        &mut self,
        next: &CaptureInstance,
        from: (Lsn, Lsn, u8),
    ) -> Result<SourceBatch, ConnectorError> {
        let info = describe_ci(
            self.client.as_mut().expect("open"),
            &self.name,
            &next.name,
            &self.cfg.key,
            &self.policy,
        )
        .await?;
        let pos = if from.0 >= next.start {
            CdcPos::stream(&next.name, from.0, from.1, from.2)
        } else {
            CdcPos::stream(&next.name, next.start, LSN_ZERO, 0)
        };
        let position = pos.to_position();
        let event = self.observe(&info, &position)?;
        self.table = Some(info);
        self.pos = Some(pos);
        Ok(SourceBatch {
            events: EventBatch::Events(event.into_iter().collect()),
            position,
        })
    }

    async fn next_stream(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let pos = self.pos.clone().expect("open");
        let (lsn, seq, op) = pos.triple()?.expect("streaming");
        let ci = pos.ci().to_owned();
        let cis = capture_instances(self.client.as_mut().expect("open"), &self.name).await?;
        let Some(idx) = cis.iter().position(|c| c.name == ci) else {
            // The position's capture instance was dropped: carry on with the
            // newest only if it starts at or before the position.
            return match cis.last() {
                Some(newest) if newest.start <= lsn => {
                    Ok(Some(self.switch(newest, (lsn, seq, op)).await?))
                }
                _ => Err(lost(
                    &self.name,
                    &format!("capture instance {ci} was dropped"),
                )),
            };
        };
        let next = cis.get(idx + 1).cloned();
        let (max, min) = self.bounds(&ci).await?;
        if lsn < min {
            return Err(lost(
                &self.name,
                &format!(
                    "the position {} is below {ci}'s low-water mark {}",
                    lsn_hex(&lsn),
                    lsn_hex(&min)
                ),
            ));
        }
        let Some(max) = max else {
            self.idle_until = Some(Instant::now() + self.cfg.cdc_poll_interval());
            return Ok(None);
        };
        let to = match &next {
            Some(n) if n.start < max => n.start,
            _ => max,
        };
        let table = self.table.clone().expect("open");
        let mut full = false;
        let mut events = Vec::new();
        let mut last = None;
        if lsn <= to {
            let limit = budget.max_rows.clamp(2, 10_000) as i64;
            let sql = changes_sql(&ci, &table.select_list(), next.is_some());
            let (from, to_b, lsn_b, seq_b) =
                (lsn.to_vec(), to.to_vec(), lsn.to_vec(), seq.to_vec());
            let op_i = i32::from(op);
            let next_start = next.as_ref().map(|n| n.start.to_vec()).unwrap_or_default();
            let mut params: Vec<&dyn ToSql> = vec![&limit, &from, &to_b, &lsn_b, &seq_b, &op_i];
            if next.is_some() {
                params.push(&next_start);
            }
            let what = format!("reading the changes of {}", self.name);
            let got = rows(self.client(), &sql, &params, &what).await?;
            full = got.len() as i64 == limit;
            let raw = got
                .iter()
                .map(|r| {
                    let c = cells(r);
                    let lsn = match &c[0] {
                        Cell::Bytes(b) => to_lsn(b)?,
                        _ => return Err(ConnectorError::fatal("__$start_lsn is not binary")),
                    };
                    let seq = match &c[1] {
                        Cell::Bytes(b) => to_lsn(b)?,
                        _ => return Err(ConnectorError::fatal("__$seqval is not binary")),
                    };
                    let op = match &c[2] {
                        Cell::Int(i) => *i as u8,
                        _ => return Err(ConnectorError::fatal("__$operation is not an integer")),
                    };
                    Ok(RawChange {
                        lsn,
                        seq,
                        op,
                        cells: c[3..].to_vec(),
                    })
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            let (changes, consumed) = pair(&raw, full);
            let key = table.indexes(&table.key)?;
            let mut bytes = 0;
            for c in &changes {
                let ev = change_event(&self.envelope, &ci, &table, &key, c);
                let size = ev.size_bytes();
                if !events.is_empty()
                    && (events.len() >= budget.max_rows || bytes + size > budget.max_bytes)
                {
                    // The rest is read again next time.
                    full = true;
                    break;
                }
                if size > budget.max_bytes {
                    return Err(ConnectorError::poison(format!(
                        "{}: a change of {size} bytes is larger than the batch budget",
                        self.name
                    )));
                }
                bytes += size;
                events.push(ev);
                last = Some((c.lsn, c.seq, c.row_op));
            }
            // Rows consumed without an event (a lone before image) still
            // move the position when nothing else was emitted.
            if events.is_empty() && consumed > 0 && !full {
                let r = &raw[consumed - 1];
                last = Some((r.lsn, r.seq, r.op));
            }
        }
        if let Some((l, s, o)) = last {
            self.pos = Some(CdcPos::stream(&ci, l, s, o));
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(events),
                position: self.pos.as_ref().expect("set").to_position(),
            }));
        }
        if !full {
            // Caught up with this capture instance.
            if let Some(n) = &next
                && max >= n.start
            {
                return Ok(Some(self.switch(n, (lsn, seq, op)).await?));
            }
            if max > lsn {
                // Keepalive: everything up to the maximum LSN is read.
                self.pos = Some(CdcPos::stream(&ci, max, LSN_MAX, u8::MAX));
                return Ok(Some(SourceBatch {
                    events: EventBatch::Events(Vec::new()),
                    position: self.pos.as_ref().expect("set").to_position(),
                }));
            }
            self.idle_until = Some(Instant::now() + self.cfg.cdc_poll_interval());
        }
        Ok(None)
    }
}

/// How much of the retention a position of `age` has used.
pub fn retention_use(age: Duration, retention: Duration) -> f64 {
    if retention.is_zero() {
        return 1.0;
    }
    age.as_secs_f64() / retention.as_secs_f64()
}

#[async_trait]
impl Source for CdcSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let mut client = connect(&self.cfg, &self.ctx).await?;
        if self.cfg.isolation == Isolation::Snapshot {
            exec(
                &mut client,
                "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                "isolation",
            )
            .await?;
        }
        let cis = capture_instances(&mut client, &self.name).await?;
        if cis.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "CDC is not enabled on {}: EXEC sys.sp_cdc_enable_table (docs/runbooks/connectors/sqlserver-cdc.md)",
                self.name
            )));
        }
        self.client = Some(client);
        let pos = match pos.as_ref().map(CdcPos::from_position).transpose()? {
            Some(p) => p,
            None => {
                let newest = cis.last().expect("checked").clone();
                let (max, _) = self.bounds(&newest.name).await?;
                if self.cfg.snapshot {
                    CdcPos::Snapshot {
                        ci: newest.name.clone(),
                        at: lsn_hex(&max.unwrap_or(LSN_ZERO)),
                        last: None,
                        rows: 0,
                    }
                } else {
                    CdcPos::after_snapshot(&newest.name, max.unwrap_or(LSN_ZERO), newest.start)
                }
            }
        };
        let ci = match cis.iter().find(|c| c.name == pos.ci()) {
            Some(c) => c.name.clone(),
            // Dropped while stopped: `next` decides between the newest and
            // `PositionLost`; read with the newest's columns until then.
            None => cis.last().expect("checked").name.clone(),
        };
        let info = describe_ci(
            self.client.as_mut().expect("open"),
            &self.name,
            &ci,
            &self.cfg.key,
            &self.policy,
        )
        .await?;
        // The first schema seen is a new table's (Task 5): a `schema-change`
        // event precedes the rows, at the position the rows start from.
        let first = self.observe(&info, &pos.to_position())?;
        if let Some(ev) = first {
            self.pending_schema = Some(ev);
        }
        self.table = Some(info);
        self.pos = Some(pos);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.client.is_none() {
            return Err(ConnectorError::fatal("the CDC source is not open"));
        }
        if let Some(ev) = self.pending_schema.take() {
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(vec![ev]),
                position: self.pos.as_ref().expect("open").to_position(),
            }));
        }
        if Instant::now() >= self.next_sample {
            self.next_sample = Instant::now() + self.cfg.metrics_interval;
            // A sample that fails (permissions on msdb, a busy server) does
            // not stop the instance.
            if let Err(e) = self.sample().await {
                tracing::debug!(table = %self.name, "CDC metrics sample failed: {}", e.message);
                if e.class.is_transient() {
                    self.client = Some(connect(&self.cfg, &self.ctx).await?);
                }
            }
        }
        if self.idle_until.is_some_and(|t| Instant::now() < t) {
            return Ok(None);
        }
        self.idle_until = None;
        let r = match self.pos {
            Some(CdcPos::Snapshot { .. }) => self.next_snapshot(budget).await,
            _ => self.next_stream(budget).await,
        };
        if let Err(e) = &r
            && e.class.is_transient()
        {
            // A dropped connection: reopen on the next call.
            if let Ok(c) = connect(&self.cfg, &self.ctx).await {
                self.client = Some(c);
                if self.cfg.isolation == Isolation::Snapshot {
                    let _ = exec(
                        self.client(),
                        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
                        "isolation",
                    )
                    .await;
                }
            }
        }
        r
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // SQL Server keeps changes for the cleanup job's retention whatever
        // a reader has acknowledged: there is nothing to release upstream.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Column;
    use loams_connector_sdk::InstanceKey;

    fn l(n: u8) -> Lsn {
        let mut x = LSN_ZERO;
        x[9] = n;
        x
    }

    fn raw(lsn: u8, seq: u8, op: u8, v: i64) -> RawChange {
        RawChange {
            lsn: l(lsn),
            seq: l(seq),
            op,
            cells: vec![Cell::Int(v), Cell::Text(format!("v{v}"))],
        }
    }

    fn table() -> TableInfo {
        let col = |n: &str, t: &str| Column {
            name: n.into(),
            declared: t.into(),
            nullable: true,
            identity: false,
            generated: false,
        };
        let columns = vec![col("id", "bigint"), col("v", "nvarchar(20)")];
        TableInfo {
            name: TableName::parse("dbo.items").expect("name"),
            schema: arrow_schema("dbo.items", &columns, &SqlServerTypeMapper::default())
                .expect("schema"),
            columns,
            key: vec!["id".into()],
        }
    }

    #[test]
    fn lsn_triples_round_trip_and_order() {
        let p = CdcPos::stream("dbo_items", l(7), l(3), 4);
        let pos = p.to_position();
        assert_eq!(CdcPos::from_position(&pos).expect("round trip"), p);
        assert_eq!(
            pos.display,
            "dbo_items: 00000000000000000007:00000000000000000003:4"
        );
        assert_eq!(p.triple().expect("triple"), Some((l(7), l(3), 4)));
        assert!((l(7), l(3), 4u8) < (l(7), l(4), 1u8));
        assert!(parse_lsn("00").is_err() && parse_lsn("zz000000000000000000").is_err());
        assert_eq!(
            CdcPos::after_snapshot("ci", l(9), l(5)),
            CdcPos::stream("ci", l(9), LSN_MAX, 255)
        );
        assert_eq!(
            CdcPos::after_snapshot("ci", l(2), l(5)),
            CdcPos::stream("ci", l(5), LSN_ZERO, 0)
        );
    }

    #[test]
    fn update_images_pair_and_a_page_never_splits_them() {
        let rows = vec![
            raw(1, 1, 2, 1),
            raw(2, 1, 3, 1),
            raw(2, 1, 4, 1),
            raw(3, 1, 1, 1),
            raw(4, 1, 3, 2),
        ];
        let (c, consumed) = pair(&rows, true);
        assert_eq!(consumed, 4, "the trailing before image waits");
        assert_eq!(
            c.iter().map(|c| c.op).collect::<Vec<_>>(),
            vec![CdcOp::Create, CdcOp::Update, CdcOp::Delete]
        );
        assert_eq!((c[1].lsn, c[1].row_op), (l(2), 4));
        assert!(c[1].before.is_some() && c[1].after.is_some());
        let (c, consumed) = pair(&rows, false);
        assert_eq!((c.len(), consumed), (3, 5));
    }

    #[test]
    fn change_events_carry_ids_ops_and_images() {
        let t = table();
        let env = EnvelopeBuilder::new("sql-server", &InstanceKey::new(1, "orders"));
        let (c, _) = pair(&[raw(2, 1, 3, 1), raw(2, 1, 4, 1)], false);
        let ev = change_event(&env, "dbo_items", &t, &[0], &c[0]);
        assert_eq!(
            ev.id,
            "dbo_items/00000000000000000002/00000000000000000001/4"
        );
        assert_eq!(ev.ty, "io.loams.dev.flow.sql-server.change.v1");
        assert_eq!(ev.extensions["loamsop"], "u");
        assert_eq!(
            ev.extensions["loamslsn"],
            "00000000000000000002/00000000000000000001"
        );
        let d: Value = serde_json::from_slice(&ev.data).expect("json");
        assert_eq!(d["key"], json!({"id": 1}));
        assert_eq!(d["after"], json!({"id": 1, "v": "v1"}));
        assert_eq!(d["table"], "dbo.items");
        let (c, _) = pair(&[raw(3, 1, 1, 9)], false);
        let del = change_event(&env, "dbo_items", &t, &[0], &c[0]);
        let d: Value = serde_json::from_slice(&del.data).expect("json");
        assert_eq!(
            (d["after"].clone(), d["key"].clone()),
            (Value::Null, json!({"id": 9}))
        );
    }

    #[test]
    fn the_change_query_reads_after_the_triple_and_before_the_next_instance() {
        let sql = changes_sql("dbo_items", "[id], [v]", true);
        assert!(sql.starts_with(
            "SELECT TOP (@P1) __$start_lsn, __$seqval, __$operation, [id], [v] \
             FROM cdc.[fn_cdc_get_all_changes_dbo_items](@P2, @P3, N'all update old')"
        ));
        assert!(sql.contains("__$operation > @P6"));
        assert!(sql.contains("AND __$start_lsn < @P7"));
        assert!(!changes_sql("x", "[a]", false).contains("@P7"));
        assert!(check_capture_instance("dbo_items_v2").is_ok());
        assert!(check_capture_instance("x]; DROP TABLE t; --").is_err());
    }

    #[test]
    fn retention_use_is_age_over_retention() {
        let day = Duration::from_secs(86_400);
        assert!((retention_use(day, DEFAULT_RETENTION) - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(retention_use(day, Duration::ZERO), 1.0);
    }
}
