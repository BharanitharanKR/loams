//! Batch reads (the ★ MySQL source, CN1 Task 7's chunked key-range read,
//! built here directly on the SDK; R0.2): one split per table, pages of
//! `SELECT … WHERE (key) > (last) ORDER BY key LIMIT n` sized from the
//! budget, as Arrow batches of type `rows` (D356's bulk path).
//!
//! A run reads inside one `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ
//! ONLY`; the position is the last key delivered, so a resumed run continues
//! after it in a new snapshot, and keeps its ids (`<run>/0/<first>-<last>`,
//! the run named by the instance, the table and the run's start).

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::schema::SchemaPolicy;
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source,
    SourceBatch, SourceFactory, Split, SplitEnumerator, SplitState, ids,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cdc::ARROW_STREAM;
use crate::config::{MyConfig, TableName};
use crate::conn::{MyConn, connect};
use crate::rows::pieces;
use crate::snapshot::read_chunk;
use crate::table::{TableInfo, describe, key_of};

/// A table split's progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPos {
    /// When the run started (Unix milliseconds), naming the run.
    pub run: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<Value>>,
    pub rows: u64,
    #[serde(default)]
    pub done: bool,
}

impl BatchPos {
    pub fn to_position(&self) -> Position {
        let display = if self.done {
            format!("run {} done, {} rows", self.run, self.rows)
        } else {
            format!("run {}, {} rows", self.run, self.rows)
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("not a MySQL batch position: {e}")))
    }
}

/// One split per configured table.
#[derive(Debug)]
pub struct BatchEnumerator {
    tables: Vec<TableName>,
}

impl BatchEnumerator {
    pub fn new(cfg: &MyConfig) -> Self {
        Self {
            tables: cfg.tables.clone(),
        }
    }
}

#[async_trait]
impl SplitEnumerator for BatchEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(self
            .tables
            .iter()
            .map(|t| Split::new(t.to_string(), serde_json::json!({ "table": t.to_string() })))
            .collect())
    }
}

/// Makes a batch source per table.
#[derive(Debug)]
pub struct BatchFactory {
    cfg: MyConfig,
    ctx: ConnectorContext,
    connector: &'static str,
}

impl BatchFactory {
    pub fn new(cfg: MyConfig, ctx: ConnectorContext, connector: &'static str) -> Self {
        Self {
            cfg,
            ctx,
            connector,
        }
    }
}

impl SourceFactory for BatchFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let table = split
            .spec
            .get("table")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::fatal("a batch split without a table"))?;
        Ok(Box::new(BatchSource {
            table: TableName::parse(table, &self.cfg.database)?,
            policy: SchemaPolicy::from_config(&self.ctx.config)?,
            envelope: EnvelopeBuilder::new(self.connector, &self.ctx.instance),
            cfg: self.cfg.clone(),
            ctx: self.ctx.clone(),
            conn: None,
            info: None,
            pos: None,
        }))
    }
}

/// Reads one table.
pub struct BatchSource {
    cfg: MyConfig,
    ctx: ConnectorContext,
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    table: TableName,
    conn: Option<MyConn>,
    info: Option<TableInfo>,
    pos: Option<BatchPos>,
}

impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("table", &self.table.to_string())
            .finish_non_exhaustive()
    }
}

impl BatchSource {
    async fn conn(&mut self) -> Result<&mut MyConn, ConnectorError> {
        if self.conn.as_ref().is_none_or(MyConn::is_broken) {
            let mut c = connect(&self.cfg, &self.ctx).await?;
            c.exec("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                .await
                .map_err(|e| e.to_connector("starting the read"))?;
            self.conn = Some(c);
        }
        Ok(self.conn.as_mut().expect("set above"))
    }
}

#[async_trait]
impl Source for BatchSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let table = self.table.clone();
        let key = self.cfg.key.clone();
        let lossy = self.policy.lossy.clone();
        let info = describe(self.conn().await?, &table, &key, &lossy).await?;
        self.info = Some(info);
        self.pos = Some(match pos {
            Some(p) => BatchPos::from_position(&p)?,
            None => BatchPos {
                run: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64),
                after: None,
                rows: 0,
                done: false,
            },
        });
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let pos = self
            .pos
            .clone()
            .ok_or_else(|| ConnectorError::fatal("the batch source is not open"))?;
        if pos.done {
            return Ok(None);
        }
        let info = self.info.clone().expect("described at open");
        let limit = self.cfg.chunk_size.min(budget.max_rows).max(1);
        let rows = read_chunk(self.conn().await?, &info, pos.after.as_deref(), limit).await?;
        if rows.is_empty() {
            if let Some(mut c) = self.conn.take() {
                let _ = c.exec("COMMIT").await;
                c.quit().await;
            }
            let done = BatchPos { done: true, ..pos };
            self.pos = Some(done.clone());
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(Vec::new()),
                position: done.to_position(),
            }));
        }
        let mut parts = pieces(
            &info.schema,
            &rows,
            budget.max_rows,
            budget.max_bytes.saturating_sub(1024),
        );
        let (_, part) = parts.remove(0);
        let taken = part.as_ref().map_or(1, |b| b.num_rows());
        let next = BatchPos {
            after: Some(key_of(&info, &rows[taken - 1])),
            rows: pos.rows + taken as u64,
            ..pos.clone()
        };
        self.pos = Some(next.clone());
        let batch = part.map_err(|e| ConnectorError::poison(format!("{}: {e}", self.table)))?;
        let run = ids::run_id(
            &self.ctx.instance,
            &self.table.to_string(),
            &pos.run.to_string(),
        );
        let mut envelope = self
            .envelope
            .event(
                "rows",
                &self.table.to_string(),
                ids::rows(&run, 0, pos.rows, next.rows - 1),
            )
            .with_subject(self.table.to_string());
        envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
        Ok(Some(SourceBatch {
            events: EventBatch::Arrow { batch, envelope },
            position: next.to_position(),
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(c) = self.conn.take() {
            c.quit().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_positions_roundtrip() {
        let p = BatchPos {
            run: 1_700_000_000_000,
            after: Some(vec![json!(10), json!("x")]),
            rows: 10,
            done: false,
        };
        let pos = p.to_position();
        assert_eq!(BatchPos::from_position(&pos).expect("back"), p);
        assert_eq!(pos.display, "run 1700000000000, 10 rows");
    }
}
