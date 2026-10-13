//! Batch reads (`mode: batch`): one split per configured table, read in key
//! order as Arrow batches (`rows` events, D356).
//!
//! Postgres pages with binary `COPY (SELECT … WHERE (key) > (last) ORDER BY
//! key LIMIT n)` inside one `REPEATABLE READ` transaction per run;
//! CockroachDB pages with `SELECT … AS OF SYSTEM TIME <as_of>` (no binary
//! `COPY TO`, R0.20), so a resumed run reads the same snapshot. The position
//! is the run's as-of, the last key delivered and the row count, so a
//! restart resumes after the last key; ids are `ids::rows(run_id(instance,
//! table, as_of), 0, first, last)` and stay the same across re-reads of a run.

use std::collections::VecDeque;

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::envelope::ids;
use loams_connector_sdk::schema::SchemaPolicy;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState,
};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::cdc::ARROW_STREAM;
use crate::config::{PgConfig, TableName, Target};
use crate::conn::{classify, connect};
use crate::rows::{RawRow, row_cost};
use crate::table::{Pager, TableInfo, batch_within, describe, key_text};

/// A batch split's position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPos {
    /// The run's as-of: CockroachDB's HLC timestamp, or Postgres's
    /// transaction start (for ids).
    pub as_of: String,
    #[serde(default)]
    pub last: Option<Vec<String>>,
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
            .map_err(|e| ConnectorError::fatal(format!("not a Postgres batch position: {e}")))
    }
}

/// One split per configured table.
#[derive(Debug)]
pub struct BatchEnumerator {
    tables: Vec<TableName>,
}

impl BatchEnumerator {
    pub fn new(cfg: &PgConfig) -> Self {
        Self {
            tables: cfg.tables.clone(),
        }
    }
}

#[async_trait]
impl SplitEnumerator for BatchEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        if self.tables.is_empty() {
            return Err(ConnectorError::fatal("config: tables is required"));
        }
        Ok(self
            .tables
            .iter()
            .map(|t| Split::new(t.to_string(), serde_json::json!({ "table": t.to_string() })))
            .collect())
    }
}

#[derive(Debug)]
pub struct BatchFactory {
    cfg: PgConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl BatchFactory {
    pub fn new(cfg: PgConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for BatchFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let table = TableName::parse(split.id.as_str())?;
        Ok(Box::new(BatchSource {
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            policy: SchemaPolicy::from_config(&self.ctx.config)?,
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            name: table,
            client: None,
            table: None,
            pager: Pager {
                cockroach_as_of: None,
            },
            pos: None,
            pending: VecDeque::new(),
            page_done: false,
            avg_cost: 0,
        }))
    }
}

/// Reads one table.
pub struct BatchSource {
    cfg: PgConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    name: TableName,
    client: Option<Client>,
    table: Option<TableInfo>,
    pager: Pager,
    pos: Option<BatchPos>,
    pending: VecDeque<RawRow>,
    page_done: bool,
    avg_cost: usize,
}

impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("table", &self.name.to_string())
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Source for BatchSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let client = connect(&self.cfg, &self.ctx).await?;
        let table = describe(&client, &self.name, &self.cfg.key, &self.policy.lossy).await?;
        if table.key.is_empty() {
            return Err(ConnectorError::fatal(format!(
                "{} has no primary key: set `key` to the columns to read it in order by",
                self.name
            )));
        }
        let resume = pos.as_ref().map(BatchPos::from_position).transpose()?;
        let cockroach = self.cfg.target == Target::Cockroach;
        let as_of = if cockroach {
            match &resume {
                Some(p) => p.as_of.clone(),
                None => {
                    let r = client
                        .query_one("SELECT cluster_logical_timestamp()::text", &[])
                        .await
                        .map_err(|e| classify(&e, "reading the cluster time"))?;
                    r.get::<_, String>(0)
                }
            }
        } else {
            client
                .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await
                .map_err(|e| classify(&e, "starting the read"))?;
            match &resume {
                Some(p) => p.as_of.clone(),
                None => {
                    let r = client
                        .query_one("SELECT now()::text", &[])
                        .await
                        .map_err(|e| classify(&e, "reading the transaction time"))?;
                    r.get::<_, String>(0)
                }
            }
        };
        self.pager = Pager {
            cockroach_as_of: cockroach.then(|| as_of.clone()),
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
        let (Some(client), Some(table), Some(pos)) = (&self.client, &self.table, &mut self.pos)
        else {
            return Err(ConnectorError::fatal("the batch source is not open"));
        };
        if pos.done {
            return Ok(None);
        }
        if self.pending.is_empty() && !self.page_done {
            let by_bytes = (budget.max_bytes / 2)
                .checked_div(self.avg_cost)
                .map_or(1000, |n| n.max(1));
            let limit = budget.max_rows.min(by_bytes).clamp(1, 50_000);
            let (n, full) = self
                .pager
                .fetch(client, table, pos.last.as_deref(), limit, &mut self.pending)
                .await?;
            if n > 0 {
                let cost: usize = self.pending.iter().map(row_cost).sum();
                self.avg_cost = cost / self.pending.len().max(1);
            }
            self.page_done = !full;
        }
        match batch_within(table, &mut self.pending, budget, 512)? {
            Some((batch, last)) => {
                let first = pos.rows;
                pos.rows += batch.num_rows() as u64;
                pos.last = Some(key_text(table, &last)?);
                if self.pending.is_empty() && self.page_done {
                    pos.done = true;
                }
                let run = ids::run_id(&self.ctx.instance, &table.name.to_string(), &pos.as_of);
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
                pos.done = true;
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
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_round_trip_and_never_hold_config() {
        let p = BatchPos {
            as_of: "2026-10-13 10:00:00+00".into(),
            last: Some(vec!["eu".into(), "42".into()]),
            rows: 1000,
            done: false,
        };
        let pos = p.to_position();
        assert_eq!(BatchPos::from_position(&pos).expect("round trip"), p);
        assert_eq!(pos.display, "as of 2026-10-13 10:00:00+00: 1000 rows");
    }
}
