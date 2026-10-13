//! The upsert sink, at-least-once (idempotent by key) or exactly-once (OR-7).
//!
//! Input events are rows: a JSON object, or a CDC change (`{before, after,
//! key, unchanged}`, `loamsop = d` deletes), or Arrow batches (each row an
//! upsert). Rows are written with `INSERT … ON CONFLICT (key) DO UPDATE`,
//! values passed as text and cast to each column's type (`$n::text::<type>`),
//! so every type the server can parse (including pgvector's `vector`)
//! needs no client codec. Within one batch the last operation per key wins,
//! which is what applying them in order would leave.
//!
//! **Exactly-once** (`transactional: true`, the default): a batch is written
//! in one transaction (each `write` under a savepoint, so a poison event is
//! rolled back alone and goes to the DLQ), `prepare` upserts the checkpoint
//! into `<checkpoint_schema>._loams_checkpoints (instance, split, position,
//! display, epoch, txn)` in that same transaction — fenced: a row with a
//! newer epoch is never overwritten — and `commit` commits. A crash before
//! the commit loses data and checkpoint together; `recover` reads the
//! checkpoint the target committed. If the connection drops at `COMMIT`,
//! `commit` reconnects and looks for its own `txn` id in the checkpoint
//! table: there, it committed; absent, it replays the staged batch, the
//! checkpoint and the commit (so `commit` is idempotent, as the contract
//! asks). CockroachDB's retry errors (40001) replay the same way.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Checkpoint, CloudEvent, ConnectorContext, ConnectorError, ErrorClass, EventBatch, Position,
    Sink, SinkAck, SinkFactory, SinkHandle, SplitId, TransactionalSink, TxnToken,
};
use serde_json::Value;
use tokio_postgres::Client;
use tokio_postgres::types::ToSql;

use crate::config::{PgConfig, TableName};
use crate::conn::{classify, connect};
use crate::rows::cell_text;
use crate::table::TableInfo;
use crate::types::{json_to_text, quote_ident};

/// The checkpoint table's name.
pub const CHECKPOINTS: &str = "_loams_checkpoints";

/// One operation on a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Column name → text value (`None` is NULL). Columns not present are
    /// left as they are.
    Upsert(BTreeMap<String, Option<String>>),
    /// The key's text values, in key order.
    Delete(Vec<Option<String>>),
}

/// The key of an operation, as text.
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

fn object_row(
    v: &Value,
    unchanged: &BTreeSet<String>,
) -> Result<BTreeMap<String, Option<String>>, String> {
    let Value::Object(m) = v else {
        return Err("a row must be a JSON object".to_owned());
    };
    Ok(m.iter()
        .filter(|(k, _)| !unchanged.contains(*k))
        .map(|(k, v)| (k.clone(), json_to_text(v)))
        .collect())
}

/// The operation one event carries.
pub fn event_op(ev: &CloudEvent, key: &[String]) -> Result<Op, String> {
    let data: Value =
        serde_json::from_slice(&ev.data).map_err(|e| format!("event {}: not JSON: {e}", ev.id))?;
    let op = ev.extensions.get("loamsop").map(String::as_str);
    let is_change = data.get("after").is_some() || data.get("before").is_some();
    let unchanged: BTreeSet<String> = data
        .get("unchanged")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    if op == Some("d") {
        let src = if is_change {
            data.get("key")
                .filter(|k| k.as_object().is_some_and(|m| !m.is_empty()))
                .or_else(|| data.get("before"))
                .unwrap_or(&Value::Null)
        } else {
            &data
        };
        let row = object_row(src, &BTreeSet::new())?;
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
    Ok(Op::Upsert(object_row(row, &unchanged)?))
}

/// Each Arrow row as an upsert.
pub fn arrow_ops(batch: &RecordBatch) -> Result<Vec<Op>, String> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|r| {
            let mut row = BTreeMap::new();
            for (c, f) in schema.fields().iter().enumerate() {
                row.insert(f.name().clone(), cell_text(batch.column(c).as_ref(), r)?);
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

/// A statement and its text parameters.
pub type Statement = (String, Vec<Option<String>>);

/// The statements for a set of collapsed operations: deletes first (each
/// key's final op is unique, so order between keys does not matter), then
/// one upsert per column set. Values are text parameters.
pub fn statements(table: &TableInfo, ops: &[Op]) -> Result<Vec<Statement>, String> {
    let ty = |c: &str| -> Result<&str, String> {
        table
            .column(c)
            .map(|c| c.type_name.as_str())
            .ok_or_else(|| format!("{} has no column {c}", table.name))
    };
    let mut out = Vec::new();
    let deletes: Vec<&Vec<Option<String>>> = ops
        .iter()
        .filter_map(|o| match o {
            Op::Delete(k) => Some(k),
            Op::Upsert(_) => None,
        })
        .collect();
    let keys: Vec<String> = table.key.iter().map(|k| quote_ident(k)).collect();
    for chunk in deletes.chunks(1000) {
        let mut params = Vec::new();
        let mut tuples = Vec::new();
        for k in chunk {
            let mut t = Vec::new();
            for (col, v) in table.key.iter().zip(k.iter()) {
                params.push(v.clone());
                t.push(format!("${}::text::{}", params.len(), ty(col)?));
            }
            tuples.push(format!("({})", t.join(", ")));
        }
        out.push((
            format!(
                "DELETE FROM {} WHERE ({}) IN ({})",
                table.name.quoted(),
                keys.join(", "),
                tuples.join(", ")
            ),
            params,
        ));
    }
    let mut groups: BTreeMap<Vec<String>, Vec<&BTreeMap<String, Option<String>>>> = BTreeMap::new();
    for o in ops {
        if let Op::Upsert(row) = o {
            groups
                .entry(row.keys().cloned().collect())
                .or_default()
                .push(row);
        }
    }
    for (cols, rows) in groups {
        for c in &cols {
            ty(c)?;
        }
        let per_stmt = (60_000 / cols.len().max(1)).max(1);
        for chunk in rows.chunks(per_stmt) {
            let mut params = Vec::new();
            let mut tuples = Vec::new();
            for row in chunk {
                let mut t = Vec::new();
                for c in &cols {
                    params.push(row.get(c).cloned().flatten());
                    t.push(format!("${}::text::{}", params.len(), ty(c)?));
                }
                tuples.push(format!("({})", t.join(", ")));
            }
            let set: Vec<String> = cols
                .iter()
                .filter(|c| !table.key.contains(c))
                .map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(c)))
                .collect();
            let action = if set.is_empty() {
                "DO NOTHING".to_owned()
            } else {
                format!("DO UPDATE SET {}", set.join(", "))
            };
            out.push((
                format!(
                    "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) {action}",
                    table.name.quoted(),
                    cols.iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    tuples.join(", "),
                    keys.join(", ")
                ),
                params,
            ));
        }
    }
    Ok(out)
}

/// Makes the instance's sink.
#[derive(Debug)]
pub struct PgSinkFactory {
    cfg: PgConfig,
    ctx: ConnectorContext,
}

impl PgSinkFactory {
    pub fn new(cfg: PgConfig, ctx: ConnectorContext) -> Result<Self, ConnectorError> {
        if cfg.table.is_none() {
            return Err(ConnectorError::fatal(
                "config: table is required for a sink",
            ));
        }
        Ok(Self { cfg, ctx })
    }
}

impl SinkFactory for PgSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let sink = PgSink::new(self.cfg.clone(), self.ctx.clone());
        Ok(if self.cfg.transactional {
            SinkHandle::Transactional(Box::new(sink))
        } else {
            SinkHandle::Plain(Box::new(sink))
        })
    }
}

static TXN_SEQ: AtomicU64 = AtomicU64::new(0);

/// See the module documentation.
pub struct PgSink {
    cfg: PgConfig,
    ctx: ConnectorContext,
    client: Option<Client>,
    table: Option<TableInfo>,
    /// Exactly-once: the batch's operations written so far in the open
    /// transaction, to replay after a lost connection.
    staged: Vec<Op>,
    in_txn: bool,
    prepared: Option<(TxnToken, Checkpoint)>,
}

impl std::fmt::Debug for PgSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSink")
            .field("table", &self.cfg.table.as_ref().map(ToString::to_string))
            .field("transactional", &self.cfg.transactional)
            .finish_non_exhaustive()
    }
}

fn instance_key(cp_or: &loams_connector_sdk::InstanceKey) -> String {
    format!("{}/{}", cp_or.namespace, cp_or.name)
}

impl PgSink {
    pub fn new(cfg: PgConfig, ctx: ConnectorContext) -> Self {
        Self {
            cfg,
            ctx,
            client: None,
            table: None,
            staged: Vec::new(),
            in_txn: false,
            prepared: None,
        }
    }

    fn table_name(&self) -> TableName {
        self.cfg.table.clone().expect("checked by the factory")
    }

    fn checkpoints(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.cfg.checkpoint_schema),
            quote_ident(CHECKPOINTS)
        )
    }

    async fn client(&mut self) -> Result<&Client, ConnectorError> {
        if self.client.as_ref().is_none_or(Client::is_closed) {
            self.in_txn = false;
            let client = connect(&self.cfg, &self.ctx).await?;
            if self.table.is_none() {
                // The sink casts text, so it needs only names and types.
                let t = describe_for_sink(&client, &self.table_name(), &self.cfg.sink_key).await?;
                if t.key.is_empty() {
                    return Err(ConnectorError::fatal(format!(
                        "{} has no primary key: set `key`",
                        t.name
                    )));
                }
                self.table = Some(t);
            }
            self.client = Some(client);
        }
        Ok(self.client.as_ref().expect("set above"))
    }

    fn ops(&self, batch: &EventBatch) -> Result<Vec<Op>, ConnectorError> {
        let key = &self.table.as_ref().expect("described").key;
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
        let table = self.table.clone().expect("described");
        let ops = collapse(ops.to_vec(), &table.key).map_err(ConnectorError::poison)?;
        let stmts = statements(&table, &ops).map_err(ConnectorError::schema_incompatible)?;
        let client = self.client().await?;
        for (sql, params) in stmts {
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            client
                .execute(sql.as_str(), &refs)
                .await
                .map_err(|e| classify(&e, &format!("writing {}", table.name)))?;
        }
        Ok(())
    }

    async fn begin(&mut self) -> Result<(), ConnectorError> {
        if self.in_txn {
            return Ok(());
        }
        self.client()
            .await?
            .batch_execute("BEGIN")
            .await
            .map_err(|e| classify(&e, "BEGIN"))?;
        self.in_txn = true;
        // Replay what this batch had written before a lost connection.
        if !self.staged.is_empty() {
            let staged = self.staged.clone();
            self.apply(&staged).await?;
        }
        Ok(())
    }

    /// A lost connection or a retry error: the transaction is gone, the
    /// staged operations are kept and replayed in the next one.
    fn reset_on(&mut self, e: &ConnectorError) {
        if e.class.is_transient() {
            // Dropping the connection aborts whatever transaction it held
            // (a 40001 leaves it aborted anyway).
            self.in_txn = false;
            self.client = None;
        }
    }

    async fn write_txn(&mut self, ops: Vec<Op>) -> Result<(), ConnectorError> {
        self.begin().await?;
        let client = self.client().await?;
        client
            .batch_execute("SAVEPOINT loams_write")
            .await
            .map_err(|e| classify(&e, "SAVEPOINT"))?;
        match self.apply(&ops).await {
            Ok(()) => {
                self.client()
                    .await?
                    .batch_execute("RELEASE SAVEPOINT loams_write")
                    .await
                    .map_err(|e| classify(&e, "RELEASE SAVEPOINT"))?;
                self.staged.extend(ops);
                Ok(())
            }
            Err(e)
                if e.class == ErrorClass::Poison || e.class == ErrorClass::SchemaIncompatible =>
            {
                self.client()
                    .await?
                    .batch_execute("ROLLBACK TO SAVEPOINT loams_write")
                    .await
                    .map_err(|e| classify(&e, "ROLLBACK TO SAVEPOINT"))?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn ensure_checkpoint_table(&mut self) -> Result<(), ConnectorError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (instance text NOT NULL, split text NOT NULL, \
             position bytea NOT NULL, display text NOT NULL, epoch bigint NOT NULL, txn text NOT NULL, \
             updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (instance, split))",
            self.checkpoints()
        );
        self.client()
            .await?
            .batch_execute(&sql)
            .await
            .map_err(|e| classify(&e, "creating the checkpoint table"))
    }

    async fn write_checkpoint(
        &mut self,
        token: &TxnToken,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        let sql = format!(
            "INSERT INTO {0} AS c (instance, split, position, display, epoch, txn, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, now()) \
             ON CONFLICT (instance, split) DO UPDATE SET position = EXCLUDED.position, \
             display = EXCLUDED.display, epoch = EXCLUDED.epoch, txn = EXCLUDED.txn, updated_at = now() \
             WHERE c.epoch <= EXCLUDED.epoch",
            self.checkpoints()
        );
        let instance = instance_key(&cp.instance);
        let epoch = cp.epoch as i64;
        for (split, pos) in &cp.splits {
            let opaque: &[u8] = &pos.opaque;
            let n = self
                .client()
                .await?
                .execute(
                    sql.as_str(),
                    &[
                        &instance,
                        &split.as_str(),
                        &opaque,
                        &pos.display,
                        &epoch,
                        &token.id,
                    ],
                )
                .await
                .map_err(|e| classify(&e, "writing the checkpoint"))?;
            if n == 0 {
                return Err(ConnectorError::fatal(format!(
                    "checkpoint fenced: {} holds a newer epoch than {epoch}",
                    self.checkpoints()
                )));
            }
        }
        Ok(())
    }

    /// Whether the checkpoint table holds `txn` (the transaction committed).
    async fn committed_txn(&mut self, txn: &str, instance: &str) -> Result<bool, ConnectorError> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE instance = $1 AND txn = $2",
            self.checkpoints()
        );
        let rows = self
            .client()
            .await?
            .query(sql.as_str(), &[&instance, &txn])
            .await
            .map_err(|e| classify(&e, "reading the checkpoint"))?;
        Ok(!rows.is_empty())
    }
}

/// The columns and key without type mapping (a sink only needs names and
/// `format_type`).
async fn describe_for_sink(
    client: &Client,
    table: &TableName,
    key: &[String],
) -> Result<TableInfo, ConnectorError> {
    let rows = client
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)::text, NOT a.attnotnull \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = $1::text::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            &[&table.quoted()],
        )
        .await
        .map_err(|e| classify(&e, &format!("describing {table}")))?;
    let key = if key.is_empty() {
        crate::table::primary_key(client, table).await?
    } else {
        key.to_vec()
    };
    let columns = rows
        .iter()
        .map(|r| crate::table::Column {
            name: r.get(0),
            type_name: r.get(1),
            nullable: r.get(2),
            kind: crate::types::ColKind::Text,
        })
        .collect();
    Ok(TableInfo {
        name: table.clone(),
        columns,
        key,
        schema: std::sync::Arc::new(arrow_schema::Schema::empty()),
    })
}

#[async_trait]
impl Sink for PgSink {
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
        // Plain mode writes each batch in its own implicit transaction.
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for PgSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        self.client().await?;
        self.ensure_checkpoint_table().await?;
        self.staged.clear();
        self.prepared = None;
        let instance = instance_key(&self.ctx.instance);
        let sql = format!(
            "SELECT split, position, display, epoch FROM {} WHERE instance = $1",
            self.checkpoints()
        );
        let rows = self
            .client()
            .await?
            .query(sql.as_str(), &[&instance])
            .await
            .map_err(|e| classify(&e, "reading the checkpoint"))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut cp = Checkpoint::new(self.ctx.instance.clone(), 0);
        for r in rows {
            let split: String = r.get(0);
            let opaque: Vec<u8> = r.get(1);
            let display: String = r.get(2);
            let epoch: i64 = r.get(3);
            cp.epoch = cp.epoch.max(epoch as u64);
            cp.splits.insert(
                SplitId::new(split),
                Position::new(Bytes::from(opaque), display),
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
        if let Err(e) = &r {
            self.reset_on(e);
            return Err(r.expect_err("checked"));
        }
        self.prepared = Some((token.clone(), cp.clone()));
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let instance = instance_key(&self.ctx.instance);
        if self.in_txn && self.client.as_ref().is_some_and(|c| !c.is_closed()) {
            let r = self
                .client
                .as_ref()
                .expect("checked")
                .batch_execute("COMMIT")
                .await
                .map_err(|e| classify(&e, "COMMIT"));
            match r {
                Ok(()) => {
                    self.in_txn = false;
                    self.staged.clear();
                    self.prepared = None;
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
            self.staged.clear();
            self.prepared = None;
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
            self.client()
                .await?
                .batch_execute("COMMIT")
                .await
                .map_err(|e| classify(&e, "COMMIT"))
        }
        .await;
        match r {
            Ok(()) => {
                self.in_txn = false;
                self.staged.clear();
                self.prepared = None;
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
            && let Some(c) = &self.client
        {
            let _ = c.batch_execute("ROLLBACK").await;
        }
        self.in_txn = false;
        self.staged.clear();
        self.prepared = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Column;
    use crate::types::ColKind;
    use serde_json::json;

    fn ev(op: Option<&str>, data: Value) -> CloudEvent {
        let mut e = CloudEvent::new("id-1", "/connectors/x/t", "t")
            .with_data("application/json", data.to_string());
        if let Some(op) = op {
            e = e.with_extension("loamsop", op);
        }
        e
    }

    fn table() -> TableInfo {
        let col = |n: &str, t: &str| Column {
            name: n.into(),
            type_name: t.into(),
            nullable: true,
            kind: ColKind::Text,
        };
        TableInfo {
            name: TableName::parse("public.items").expect("name"),
            columns: vec![
                col("id", "bigint"),
                col("name", "text"),
                col("emb", "vector(3)"),
            ],
            key: vec!["id".into()],
            schema: std::sync::Arc::new(arrow_schema::Schema::empty()),
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
            event_op(
                &ev(None, json!({"id": 1, "name": "a", "emb": [1.0, 2.0, 3.0]})),
                &key
            ),
            Ok(up(&[
                ("emb", Some("[1.0,2.0,3.0]")),
                ("id", Some("1")),
                ("name", Some("a"))
            ]))
        );
        let change = json!({"op": "u", "key": {"id": 2}, "before": null, "after": {"id": 2, "name": "b", "emb": null}, "unchanged": ["emb"]});
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
            up(&[("id", Some("2")), ("emb", Some("[1,2,3]"))]),
        ];
        assert_eq!(
            collapse(ops, &key),
            Ok(vec![
                Op::Delete(vec![Some("1".into())]),
                up(&[
                    ("id", Some("2")),
                    ("name", Some("b")),
                    ("emb", Some("[1,2,3]"))
                ]),
            ])
        );
    }

    #[test]
    fn statements_cast_text_to_each_column_type() {
        let t = table();
        let ops = vec![
            Op::Delete(vec![Some("9".into())]),
            up(&[("id", Some("1")), ("emb", Some("[1,2,3]"))]),
            up(&[("id", Some("2")), ("emb", None)]),
        ];
        let s = statements(&t, &ops).expect("statements");
        assert_eq!(s.len(), 2);
        assert_eq!(
            s[0].0,
            "DELETE FROM \"public\".\"items\" WHERE (\"id\") IN (($1::text::bigint))"
        );
        assert_eq!(
            s[1].0,
            "INSERT INTO \"public\".\"items\" (\"emb\", \"id\") VALUES ($1::text::vector(3), $2::text::bigint), \
             ($3::text::vector(3), $4::text::bigint) ON CONFLICT (\"id\") DO UPDATE SET \"emb\" = EXCLUDED.\"emb\""
        );
        assert_eq!(
            s[1].1,
            vec![
                Some("[1,2,3]".into()),
                Some("1".into()),
                None,
                Some("2".into())
            ]
        );
        let unknown = statements(&t, &[up(&[("id", Some("1")), ("color", Some("red"))])]);
        assert!(
            unknown
                .expect_err("unknown column")
                .contains("no column color")
        );
    }
}
