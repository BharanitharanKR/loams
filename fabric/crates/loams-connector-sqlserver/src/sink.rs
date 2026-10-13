//! The MERGE sink, at-least-once (idempotent by key) or exactly-once (OR-7).
//!
//! Input events are rows: a JSON object, a CDC change (`{before, after,
//! key}`, `loamsop = d` deletes), or Arrow batches (each row an upsert).
//! Within one batch the last operation per key wins. Each group of
//! operations with the same column set is **bulk-loaded** (TDS `INSERT
//! BULK`) into the session temp table `#loams_stage` as text, then applied
//! with one `MERGE … WITH (HOLDLOCK)` by key — values cast from text to each
//! column's declared type, so no client codec is needed per type — or one
//! `DELETE … JOIN` for deletes. Computed and `rowversion` columns are never
//! written; an `IDENTITY` column is written under `IDENTITY_INSERT`.
//!
//! **Exactly-once** (`transactional: true`, the default): a batch is
//! written in one transaction (each `write` under a savepoint, so a poison
//! event is rolled back alone and goes to the DLQ), `prepare` upserts the
//! checkpoint into `<checkpoint_schema>._loams_checkpoints (instance, split,
//! position, display, epoch, txn)` in that same transaction — fenced: a row
//! with a newer epoch is never overwritten — and `commit` commits. A crash
//! before the commit loses data and checkpoint together; `recover` reads the
//! checkpoint the target committed. If the connection drops at `COMMIT`,
//! `commit` reconnects and looks for its own `txn` id in the checkpoint
//! table: there, it committed; absent, it replays the staged batch, the
//! checkpoint and the commit.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Checkpoint, CloudEvent, ConnectorContext, ConnectorError, ErrorClass, EventBatch, InstanceKey,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, SplitId, TransactionalSink, TxnToken,
};
use serde_json::Value;
use tiberius::{ColumnData, TokenRow};

use crate::config::{SqlConfig, TableName};
use crate::conn::{SqlClient, classify, connect, exec, rows};
use crate::types::{Column, arrow_cell_text, cast_expr, json_to_text, quote_ident};

/// The checkpoint table's name.
pub const CHECKPOINTS: &str = "_loams_checkpoints";
/// The session temp table batches are staged in.
pub const STAGE: &str = "#loams_stage";

/// One operation on a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Column name → text value (`None` is NULL). Columns not present are
    /// left as they are.
    Upsert(BTreeMap<String, Option<String>>),
    /// The key's text values, in key order.
    Delete(Vec<Option<String>>),
}

fn key_of(op: &Op, key: &[String]) -> Result<Vec<Option<String>>, String> {
    match op {
        Op::Delete(k) => Ok(k.clone()),
        Op::Upsert(row) => key
            .iter()
            .map(|k| {
                row.get(k)
                    .cloned()
                    .ok_or_else(|| format!("the row has no key column {k}"))
            })
            .collect(),
    }
}

fn object_row(v: &Value) -> Result<BTreeMap<String, Option<String>>, String> {
    let Value::Object(m) = v else {
        return Err("a row must be a JSON object".to_owned());
    };
    Ok(m.iter()
        .map(|(k, v)| (k.clone(), json_to_text(v)))
        .collect())
}

/// The operation one event carries.
pub fn event_op(ev: &CloudEvent, key: &[String]) -> Result<Op, String> {
    let data: Value =
        serde_json::from_slice(&ev.data).map_err(|e| format!("event {}: not JSON: {e}", ev.id))?;
    let op = ev.extensions.get("loamsop").map(String::as_str);
    let is_change = data.get("after").is_some() || data.get("before").is_some();
    if op == Some("d") {
        let src = if is_change {
            data.get("key")
                .filter(|k| k.as_object().is_some_and(|m| !m.is_empty()))
                .or_else(|| data.get("before"))
                .unwrap_or(&Value::Null)
        } else {
            &data
        };
        let row = object_row(src)?;
        return key
            .iter()
            .map(|k| {
                row.get(k)
                    .cloned()
                    .ok_or_else(|| format!("event {}: the delete has no key column {k}", ev.id))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Op::Delete);
    }
    if op == Some("t") {
        return Err(format!(
            "event {}: TRUNCATE is not applied by the sink",
            ev.id
        ));
    }
    let row = if is_change {
        data.get("after").unwrap_or(&Value::Null)
    } else {
        &data
    };
    Ok(Op::Upsert(object_row(row)?))
}

/// Each Arrow row as an upsert.
pub fn arrow_ops(batch: &RecordBatch) -> Result<Vec<Op>, String> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|r| {
            let mut row = BTreeMap::new();
            for (c, f) in schema.fields().iter().enumerate() {
                row.insert(
                    f.name().clone(),
                    arrow_cell_text(batch.column(c).as_ref(), r)?,
                );
            }
            Ok(Op::Upsert(row))
        })
        .collect()
}

/// Collapse operations so each key appears once, the last one winning.
pub fn collapse(ops: Vec<Op>, key: &[String]) -> Result<Vec<Op>, String> {
    let mut last: BTreeMap<Vec<Option<String>>, (usize, Op)> = BTreeMap::new();
    for (i, op) in ops.into_iter().enumerate() {
        let k = key_of(&op, key)?;
        let merged = match (last.remove(&k), op) {
            // A partial upsert after an upsert keeps the earlier columns.
            (Some((_, Op::Upsert(mut a))), Op::Upsert(b)) => {
                a.extend(b);
                Op::Upsert(a)
            }
            (_, op) => op,
        };
        last.insert(k, (i, merged));
    }
    let mut v: Vec<(usize, Op)> = last.into_values().collect();
    v.sort_by_key(|(i, _)| *i);
    Ok(v.into_iter().map(|(_, op)| op).collect())
}

/// The target table as the sink sees it.
#[derive(Debug, Clone)]
pub struct Target {
    pub name: TableName,
    pub columns: Vec<Column>,
    pub key: Vec<String>,
}

impl Target {
    fn column(&self, n: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == n)
    }
}

/// One staged group: the columns, the rows (text), and the statement that
/// applies `#loams_stage` to the target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub apply: String,
}

/// `CREATE TABLE #loams_stage` with `n` text columns `[c0]…`.
pub fn stage_ddl(n: usize) -> String {
    let cols: Vec<String> = (0..n)
        .map(|i| format!("[c{i}] nvarchar(max) NULL"))
        .collect();
    format!(
        "IF OBJECT_ID(N'tempdb..{STAGE}') IS NOT NULL DROP TABLE {STAGE}; \
         CREATE TABLE {STAGE} ({})",
        cols.join(", ")
    )
}

/// The groups for a set of collapsed operations: deletes first (each key's
/// final operation is unique, so the order between keys does not matter),
/// then one MERGE per column set.
pub fn groups(t: &Target, ops: &[Op]) -> Result<Vec<Group>, String> {
    let declared = |c: &str| -> Result<&Column, String> {
        t.column(c)
            .ok_or_else(|| format!("{} has no column {c}", t.name))
    };
    let keys: Vec<&Column> = t
        .key
        .iter()
        .map(|k| declared(k))
        .collect::<Result<_, _>>()?;
    let on: Vec<String> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("t.{} = s.[k{i}]", quote_ident(&k.name)))
        .collect();
    let mut out = Vec::new();
    let deletes: Vec<Vec<Option<String>>> = ops
        .iter()
        .filter_map(|o| match o {
            Op::Delete(k) => Some(k.clone()),
            Op::Upsert(_) => None,
        })
        .collect();
    if !deletes.is_empty() {
        let src: Vec<String> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| format!("{} AS [k{i}]", cast_expr(&format!("[c{i}]"), &k.declared)))
            .collect();
        out.push(Group {
            columns: t.key.clone(),
            rows: deletes,
            apply: format!(
                "DELETE t FROM {} AS t JOIN (SELECT {} FROM {STAGE}) AS s ON {}",
                t.name.quoted(),
                src.join(", "),
                on.join(" AND ")
            ),
        });
    }
    let mut by_cols: BTreeMap<Vec<String>, Vec<&BTreeMap<String, Option<String>>>> =
        BTreeMap::new();
    for o in ops {
        if let Op::Upsert(row) = o {
            // Generated columns are derived by the server: never written.
            let cols = row
                .keys()
                .filter(|c| t.column(c).is_none_or(|c| !c.generated))
                .cloned()
                .collect();
            by_cols.entry(cols).or_default().push(row);
        }
    }
    for (cols, rows) in by_cols {
        let cs: Vec<&Column> = cols.iter().map(|c| declared(c)).collect::<Result<_, _>>()?;
        for k in &t.key {
            if !cols.contains(k) {
                return Err(format!("the row has no key column {k}"));
            }
        }
        let src: Vec<String> = cs
            .iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "{} AS {}",
                    cast_expr(&format!("[c{i}]"), &c.declared),
                    quote_ident(&c.name)
                )
            })
            .collect();
        let merge_on: Vec<String> = t
            .key
            .iter()
            .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
            .collect();
        let set: Vec<String> = cs
            .iter()
            .filter(|c| !t.key.contains(&c.name) && !c.identity)
            .map(|c| format!("t.{0} = s.{0}", quote_ident(&c.name)))
            .collect();
        let names: Vec<String> = cs.iter().map(|c| quote_ident(&c.name)).collect();
        let values: Vec<String> = cs
            .iter()
            .map(|c| format!("s.{}", quote_ident(&c.name)))
            .collect();
        let matched = if set.is_empty() {
            String::new()
        } else {
            format!(" WHEN MATCHED THEN UPDATE SET {}", set.join(", "))
        };
        let merge = format!(
            "MERGE INTO {} WITH (HOLDLOCK) AS t USING (SELECT {} FROM {STAGE}) AS s ON {}{matched} \
             WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});",
            t.name.quoted(),
            src.join(", "),
            merge_on.join(" AND "),
            names.join(", "),
            values.join(", ")
        );
        let apply = if cs.iter().any(|c| c.identity) {
            format!(
                "SET IDENTITY_INSERT {0} ON; {merge} SET IDENTITY_INSERT {0} OFF;",
                t.name.quoted()
            )
        } else {
            merge
        };
        out.push(Group {
            rows: rows
                .iter()
                .map(|r| cols.iter().map(|c| r.get(c).cloned().flatten()).collect())
                .collect(),
            columns: cols,
            apply,
        });
    }
    Ok(out)
}

/// `[schema].[_loams_checkpoints]`.
pub fn checkpoints_table(schema: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(CHECKPOINTS))
}

/// Create the checkpoint table if it does not exist.
pub fn checkpoints_ddl(schema: &str) -> String {
    let t = checkpoints_table(schema);
    format!(
        "IF OBJECT_ID(N'{}', N'U') IS NULL CREATE TABLE {t} (instance nvarchar(200) NOT NULL, \
         split nvarchar(200) NOT NULL, position varbinary(max) NOT NULL, display nvarchar(max) NOT NULL, \
         epoch bigint NOT NULL, txn nvarchar(200) NOT NULL, \
         updated_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(), PRIMARY KEY (instance, split))",
        t.replace('\'', "''")
    )
}

/// The fenced upsert of one split's checkpoint: `SELECT 1` when written,
/// `0` when a newer epoch holds the row. `@P1`–`@P6`: instance, split,
/// position, display, epoch, txn.
pub fn checkpoint_upsert(schema: &str) -> String {
    let t = checkpoints_table(schema);
    format!(
        "DECLARE @r int = 0; \
         UPDATE {t} WITH (UPDLOCK, SERIALIZABLE) SET position = @P3, display = @P4, epoch = @P5, \
         txn = @P6, updated_at = SYSUTCDATETIME() WHERE instance = @P1 AND split = @P2 AND epoch <= @P5; \
         SET @r = @@ROWCOUNT; \
         IF @r = 0 AND NOT EXISTS (SELECT 1 FROM {t} WITH (UPDLOCK, SERIALIZABLE) \
         WHERE instance = @P1 AND split = @P2) \
         BEGIN INSERT INTO {t} (instance, split, position, display, epoch, txn) \
         VALUES (@P1, @P2, @P3, @P4, @P5, @P6); SET @r = 1; END; \
         SELECT @r"
    )
}

fn instance_key(i: &InstanceKey) -> String {
    format!("{}/{}", i.namespace, i.name)
}

/// Makes the instance's sink.
#[derive(Debug)]
pub struct MergeSinkFactory {
    cfg: SqlConfig,
    ctx: ConnectorContext,
}

impl MergeSinkFactory {
    pub fn new(cfg: SqlConfig, ctx: ConnectorContext) -> Result<Self, ConnectorError> {
        if cfg.table.is_none() {
            return Err(ConnectorError::fatal(
                "config: table is required for a sink",
            ));
        }
        Ok(Self { cfg, ctx })
    }
}

impl SinkFactory for MergeSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let sink = MergeSink::new(self.cfg.clone(), self.ctx.clone());
        Ok(if self.cfg.transactional {
            SinkHandle::Transactional(Box::new(sink))
        } else {
            SinkHandle::Plain(Box::new(sink))
        })
    }
}

static TXN_SEQ: AtomicU64 = AtomicU64::new(0);

/// See the module documentation.
pub struct MergeSink {
    cfg: SqlConfig,
    ctx: ConnectorContext,
    client: Option<SqlClient>,
    target: Option<Target>,
    /// Exactly-once: the batch's operations written so far in the open
    /// transaction, to replay after a lost connection.
    staged: Vec<Op>,
    in_txn: bool,
    prepared: Option<(TxnToken, Checkpoint)>,
}

impl std::fmt::Debug for MergeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeSink")
            .field("table", &self.cfg.table.as_ref().map(ToString::to_string))
            .field("transactional", &self.cfg.transactional)
            .finish_non_exhaustive()
    }
}

impl MergeSink {
    pub fn new(cfg: SqlConfig, ctx: ConnectorContext) -> Self {
        Self {
            cfg,
            ctx,
            client: None,
            target: None,
            staged: Vec::new(),
            in_txn: false,
            prepared: None,
        }
    }

    async fn client(&mut self) -> Result<&mut SqlClient, ConnectorError> {
        if self.client.is_none() {
            self.in_txn = false;
            let mut client = connect(&self.cfg, &self.ctx).await?;
            if self.target.is_none() {
                let name = self.cfg.table.clone().expect("checked by the factory");
                let columns = crate::conn::describe(&mut client, &name).await?;
                let key = if self.cfg.key.is_empty() {
                    crate::conn::primary_key(&mut client, &name).await?
                } else {
                    self.cfg.key.clone()
                };
                if key.is_empty() {
                    return Err(ConnectorError::fatal(format!(
                        "{name} has no primary key: set `key`"
                    )));
                }
                self.target = Some(Target { name, columns, key });
            }
            self.client = Some(client);
        }
        Ok(self.client.as_mut().expect("set above"))
    }

    fn ops(&self, batch: &EventBatch) -> Result<Vec<Op>, ConnectorError> {
        let key = &self.target.as_ref().expect("described").key;
        match batch {
            EventBatch::Events(evs) => evs
                .iter()
                .map(|e| event_op(e, key))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ConnectorError::poison),
            EventBatch::Arrow { batch, .. } => arrow_ops(batch).map_err(ConnectorError::poison),
        }
    }

    async fn apply(&mut self, ops: &[Op]) -> Result<(), ConnectorError> {
        let target = self.target.clone().expect("described");
        let ops = collapse(ops.to_vec(), &target.key).map_err(ConnectorError::poison)?;
        let groups = groups(&target, &ops).map_err(ConnectorError::schema_incompatible)?;
        let what = format!("writing {}", target.name);
        let client = self.client().await?;
        for g in groups {
            exec(client, &stage_ddl(g.columns.len()), "staging").await?;
            let mut req = client
                .bulk_insert(STAGE)
                .await
                .map_err(|e| classify(&e, "bulk-loading the stage"))?;
            for r in g.rows {
                let mut row = TokenRow::with_capacity(r.len());
                for v in r {
                    row.push(ColumnData::String(v.map(Cow::Owned)));
                }
                req.send(row)
                    .await
                    .map_err(|e| classify(&e, "bulk-loading the stage"))?;
            }
            req.finalize()
                .await
                .map_err(|e| classify(&e, "bulk-loading the stage"))?;
            exec(client, &g.apply, &what).await?;
        }
        Ok(())
    }

    async fn begin(&mut self) -> Result<(), ConnectorError> {
        if self.in_txn {
            return Ok(());
        }
        exec(self.client().await?, "BEGIN TRANSACTION", "BEGIN").await?;
        self.in_txn = true;
        // Replay what this batch had written before a lost connection.
        if !self.staged.is_empty() {
            let staged = self.staged.clone();
            self.apply(&staged).await?;
        }
        Ok(())
    }

    /// A lost connection or a deadlock: the transaction is gone, the staged
    /// operations are kept and replayed in the next one.
    fn reset_on(&mut self, e: &ConnectorError) {
        if e.class.is_transient() {
            self.in_txn = false;
            self.client = None;
        }
    }

    async fn write_txn(&mut self, ops: Vec<Op>) -> Result<(), ConnectorError> {
        self.begin().await?;
        exec(
            self.client().await?,
            "SAVE TRANSACTION loams_write",
            "SAVE TRANSACTION",
        )
        .await?;
        match self.apply(&ops).await {
            Ok(()) => {
                self.staged.extend(ops);
                Ok(())
            }
            Err(e)
                if e.class == ErrorClass::Poison || e.class == ErrorClass::SchemaIncompatible =>
            {
                // XACT_ABORT is off, so a row error leaves the transaction
                // open; one that dooms it (XACT_STATE() = -1) loses the
                // whole batch, which is replayed in a new transaction.
                let client = self.client().await?;
                let state = rows(
                    client,
                    "SELECT CAST(XACT_STATE() AS int)",
                    &[],
                    "XACT_STATE",
                )
                .await?
                .first()
                .and_then(|r| r.get::<i32, _>(0))
                .unwrap_or(0);
                if state == 1 {
                    exec(
                        client,
                        "ROLLBACK TRANSACTION loams_write",
                        "ROLLBACK TRANSACTION",
                    )
                    .await?;
                } else {
                    let _ = exec(client, "IF @@TRANCOUNT > 0 ROLLBACK", "ROLLBACK").await;
                    self.in_txn = false;
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn write_checkpoint(
        &mut self,
        token: &TxnToken,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        let sql = checkpoint_upsert(&self.cfg.checkpoint_schema);
        let instance = instance_key(&cp.instance);
        let epoch = cp.epoch as i64;
        let table = checkpoints_table(&self.cfg.checkpoint_schema);
        let client = self.client().await?;
        for (split, pos) in &cp.splits {
            let opaque: &[u8] = &pos.opaque;
            let r = rows(
                client,
                &sql,
                &[
                    &instance.as_str(),
                    &split.as_str(),
                    &opaque,
                    &pos.display.as_str(),
                    &epoch,
                    &token.id.as_str(),
                ],
                "writing the checkpoint",
            )
            .await?;
            let written = r.first().and_then(|r| r.get::<i32, _>(0)).unwrap_or(0);
            if written == 0 {
                return Err(ConnectorError::fatal(format!(
                    "checkpoint fenced: {table} holds a newer epoch than {epoch}"
                )));
            }
        }
        Ok(())
    }

    /// Whether the checkpoint table holds `txn` (the transaction committed).
    async fn committed_txn(&mut self, txn: &str, instance: &str) -> Result<bool, ConnectorError> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE instance = @P1 AND txn = @P2",
            checkpoints_table(&self.cfg.checkpoint_schema)
        );
        let r = rows(
            self.client().await?,
            &sql,
            &[&instance, &txn],
            "reading the checkpoint",
        )
        .await?;
        Ok(!r.is_empty())
    }

    async fn commit_now(&mut self) -> Result<(), ConnectorError> {
        exec(self.client().await?, "COMMIT TRANSACTION", "COMMIT").await
    }

    fn done(&mut self) {
        self.in_txn = false;
        self.staged.clear();
        self.prepared = None;
    }
}

#[async_trait]
impl Sink for MergeSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        self.client().await?;
        let ack = SinkAck {
            rows: batch.rows() as u64,
            bytes: batch.size_bytes() as u64,
        };
        let ops = self.ops(&batch)?;
        let r = if self.cfg.transactional {
            self.write_txn(ops).await
        } else {
            self.apply(&ops).await
        };
        if let Err(e) = &r {
            self.reset_on(e);
        }
        r.map(|()| ack)
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Plain mode writes each statement in its own implicit transaction.
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for MergeSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        let ddl = checkpoints_ddl(&self.cfg.checkpoint_schema);
        exec(self.client().await?, &ddl, "creating the checkpoint table").await?;
        self.staged.clear();
        self.prepared = None;
        let instance = instance_key(&self.ctx.instance);
        let sql = format!(
            "SELECT split, position, display, epoch FROM {} WHERE instance = @P1",
            checkpoints_table(&self.cfg.checkpoint_schema)
        );
        let r = rows(
            self.client().await?,
            &sql,
            &[&instance.as_str()],
            "reading the checkpoint",
        )
        .await?;
        if r.is_empty() {
            return Ok(None);
        }
        let mut cp = Checkpoint::new(self.ctx.instance.clone(), 0);
        for row in r {
            let split: &str = row.get(0).unwrap_or_default();
            let opaque: &[u8] = row.get(1).unwrap_or_default();
            let display: &str = row.get(2).unwrap_or_default();
            let epoch: i64 = row.get(3).unwrap_or(0);
            cp.epoch = cp.epoch.max(epoch as u64);
            cp.splits.insert(
                SplitId::new(split),
                Position::new(Bytes::copy_from_slice(opaque), display),
            );
        }
        Ok(Some(cp))
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        let token = TxnToken {
            id: format!(
                "{}-{}-{}",
                cp.epoch,
                std::process::id(),
                TXN_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
            opaque: Bytes::new(),
        };
        let r = async {
            self.begin().await?;
            self.write_checkpoint(&token, cp).await
        }
        .await;
        if let Err(e) = r {
            self.reset_on(&e);
            return Err(e);
        }
        self.prepared = Some((token.clone(), cp.clone()));
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let instance = instance_key(&self.ctx.instance);
        if self.in_txn && self.client.is_some() {
            match self.commit_now().await {
                Ok(()) => {
                    self.done();
                    return Ok(());
                }
                Err(e) => {
                    self.reset_on(&e);
                    if !e.class.is_transient() {
                        return Err(e);
                    }
                }
            }
        }
        // The outcome is unknown (or the transaction was lost): the
        // checkpoint table says.
        self.in_txn = false;
        if self.committed_txn(&t.id, &instance).await? {
            self.done();
            return Ok(());
        }
        let Some((_, cp)) = self.prepared.clone() else {
            return Err(ConnectorError::fatal(format!(
                "commit of unknown transaction {}",
                t.id
            )));
        };
        // Not committed: replay the batch, the checkpoint and the commit.
        let r = async {
            self.begin().await?;
            self.write_checkpoint(&t, &cp).await?;
            self.commit_now().await
        }
        .await;
        match r {
            Ok(()) => {
                self.done();
                Ok(())
            }
            Err(e) => {
                self.reset_on(&e);
                Err(e)
            }
        }
    }

    async fn abort(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        if self.in_txn
            && let Some(c) = self.client.as_mut()
        {
            let _ = exec(c, "IF @@TRANCOUNT > 0 ROLLBACK", "ROLLBACK").await;
        }
        self.done();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(op: Option<&str>, data: Value) -> CloudEvent {
        let mut e = CloudEvent::new("id-1", "/connectors/x/t", "t")
            .with_data("application/json", data.to_string());
        if let Some(op) = op {
            e = e.with_extension("loamsop", op);
        }
        e
    }

    fn col(n: &str, t: &str) -> Column {
        Column {
            name: n.into(),
            declared: t.into(),
            nullable: true,
            identity: false,
            generated: false,
        }
    }

    fn target() -> Target {
        let mut total = col("total", "decimal(10,2)");
        total.generated = true;
        Target {
            name: TableName::parse("dbo.items").expect("name"),
            columns: vec![
                col("id", "bigint"),
                col("name", "nvarchar(40)"),
                col("at", "datetime2(7)"),
                total,
            ],
            key: vec!["id".into()],
        }
    }

    fn up(pairs: &[(&str, Option<&str>)]) -> Op {
        Op::Upsert(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
                .collect(),
        )
    }

    #[test]
    fn events_become_operations() {
        let key = vec!["id".to_owned()];
        assert_eq!(
            event_op(&ev(None, json!({"id": 1, "name": "a"})), &key),
            Ok(up(&[("id", Some("1")), ("name", Some("a"))]))
        );
        let change = json!({"op": "u", "key": {"id": 2}, "before": {"id": 2, "name": "x"}, "after": {"id": 2, "name": "b"}});
        assert_eq!(
            event_op(&ev(Some("u"), change), &key),
            Ok(up(&[("id", Some("2")), ("name", Some("b"))]))
        );
        let del = json!({"op": "d", "key": {"id": 3}, "before": {"id": 3}, "after": null});
        assert_eq!(
            event_op(&ev(Some("d"), del), &key),
            Ok(Op::Delete(vec![Some("3".into())]))
        );
        assert!(event_op(&ev(Some("t"), json!({})), &key).is_err());
        assert!(event_op(&ev(None, json!([1])), &key).is_err());
    }

    #[test]
    fn the_last_operation_per_key_wins() {
        let key = vec!["id".to_owned()];
        let ops = vec![
            up(&[("id", Some("1")), ("name", Some("a"))]),
            up(&[("id", Some("2")), ("name", Some("b"))]),
            Op::Delete(vec![Some("1".into())]),
            up(&[("id", Some("2")), ("at", Some("2026-10-13T10:00:00Z"))]),
        ];
        assert_eq!(
            collapse(ops, &key),
            Ok(vec![
                Op::Delete(vec![Some("1".into())]),
                up(&[
                    ("id", Some("2")),
                    ("name", Some("b")),
                    ("at", Some("2026-10-13T10:00:00Z"))
                ]),
            ])
        );
    }

    #[test]
    fn groups_stage_text_and_merge_by_key() {
        let t = target();
        let ops = vec![
            Op::Delete(vec![Some("9".into())]),
            up(&[("id", Some("1")), ("name", Some("a")), ("total", Some("3"))]),
            up(&[("id", Some("2")), ("name", None)]),
        ];
        let g = groups(&t, &ops).expect("groups");
        assert_eq!(g.len(), 2);
        assert_eq!(
            g[0].apply,
            "DELETE t FROM [dbo].[items] AS t JOIN (SELECT CAST([c0] AS bigint) AS [k0] FROM #loams_stage) AS s \
             ON t.[id] = s.[k0]"
        );
        assert_eq!(g[0].rows, vec![vec![Some("9".to_owned())]]);
        assert_eq!(
            g[1].columns,
            vec!["id".to_owned(), "name".to_owned()],
            "generated columns are dropped"
        );
        assert_eq!(
            g[1].apply,
            "MERGE INTO [dbo].[items] WITH (HOLDLOCK) AS t USING (SELECT CAST([c0] AS bigint) AS [id], \
             CAST([c1] AS nvarchar(40)) AS [name] FROM #loams_stage) AS s ON t.[id] = s.[id] \
             WHEN MATCHED THEN UPDATE SET t.[name] = s.[name] WHEN NOT MATCHED THEN INSERT ([id], [name]) \
             VALUES (s.[id], s.[name]);"
        );
        assert_eq!(
            g[1].rows,
            vec![
                vec![Some("1".to_owned()), Some("a".to_owned())],
                vec![Some("2".to_owned()), None]
            ]
        );
        let unknown = groups(&t, &[up(&[("id", Some("1")), ("color", Some("red"))])]);
        assert!(
            unknown
                .expect_err("unknown column")
                .contains("no column color")
        );
        assert_eq!(
            stage_ddl(2),
            "IF OBJECT_ID(N'tempdb..#loams_stage') IS NOT NULL DROP TABLE #loams_stage; \
             CREATE TABLE #loams_stage ([c0] nvarchar(max) NULL, [c1] nvarchar(max) NULL)"
        );
    }

    #[test]
    fn identity_keys_are_written_under_identity_insert() {
        let mut t = target();
        t.columns[0].identity = true;
        let g = groups(&t, &[up(&[("id", Some("1")), ("name", Some("a"))])]).expect("groups");
        assert!(
            g[0].apply
                .starts_with("SET IDENTITY_INSERT [dbo].[items] ON; MERGE"),
            "{}",
            g[0].apply
        );
        assert!(
            g[0].apply
                .ends_with("SET IDENTITY_INSERT [dbo].[items] OFF;")
        );
    }

    #[test]
    fn the_checkpoint_upsert_is_fenced_by_epoch() {
        let sql = checkpoint_upsert("dbo");
        assert!(sql.contains("UPDATE [dbo].[_loams_checkpoints] WITH (UPDLOCK, SERIALIZABLE)"));
        assert!(sql.contains("AND epoch <= @P5"));
        assert!(sql.ends_with("SELECT @r"));
        assert!(
            checkpoints_ddl("dbo")
                .starts_with("IF OBJECT_ID(N'[dbo].[_loams_checkpoints]', N'U') IS NULL")
        );
    }
}
