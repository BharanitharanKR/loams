//! The upsert sink, at-least-once (idempotent by key) or exactly-once
//! (OR-7; CN1's `INSERT … ON DUPLICATE KEY UPDATE` sink with a transactional
//! checkpoint).
//!
//! Input events are rows: a JSON object, a CDC change (`{before, after,
//! key}`, `loamsop = d` deletes), or Arrow batches (each row an upsert).
//! Values are SQL literals that need no escaping (hex for strings and
//! bytes, [`crate::table::literal`]) and the server casts them to each
//! column's type, so every type it can parse needs no client codec. Within
//! one batch the last operation per key wins. A column the table lacks is
//! `SchemaIncompatible` (the instance pauses), never dropped; `TRUNCATE`
//! (`loamsop = t`) is refused as `Poison` rather than emptying a table.
//!
//! **Exactly-once** (`transactional: true`, the default): MySQL has no
//! client-visible prepared transactions short of XA, so as R10.12 does for
//! Postgres, a batch is written in one transaction (each `write` under a
//! savepoint, so a poison event rolls back alone and goes to the DLQ),
//! `prepare` upserts the checkpoint into
//! `<checkpoint_database>._loams_checkpoints (instance, split, position,
//! display, epoch, txn)` in that same transaction — fenced: the row is
//! locked and a newer epoch is never overwritten — and `commit` commits. A
//! crash before the commit loses data and checkpoint together; `recover`
//! reads the checkpoint the target committed. If the connection drops at
//! `COMMIT`, `commit` reconnects and looks for its own `txn` id in the
//! checkpoint table: there, it committed; absent, it replays the staged
//! batch, the checkpoint and the commit. A deadlock or a lock timeout drops
//! the connection (rolling the transaction back on the server) and replays
//! the same way.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Checkpoint, CloudEvent, ConnectorContext, ConnectorError, ErrorClass, EventBatch, Position,
    Sink, SinkAck, SinkFactory, SinkHandle, SplitId, TransactionalSink, TxnToken,
};
use serde_json::Value;

use crate::config::{MyConfig, TableName};
use crate::conn::{MyConn, connect};
use crate::history::TableDef;
use crate::rows::cell;
use crate::table::{describe_def, literal};
use crate::types::{hex_literal, quote_ident, quote_str};

/// The checkpoint table's name.
pub const CHECKPOINTS: &str = "_loams_checkpoints";
/// Rows per statement.
const ROWS_PER_STATEMENT: usize = 500;

/// One operation on a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Column name → value. Columns not present are left as they are.
    Upsert(BTreeMap<String, Value>),
    /// The key's values, in key order.
    Delete(Vec<Value>),
}

fn key_of(op: &Op, key: &[String]) -> Result<Vec<Value>, String> {
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

fn object_row(v: &Value) -> Result<BTreeMap<String, Value>, String> {
    let Value::Object(m) = v else {
        return Err("a row must be a JSON object".to_owned());
    };
    Ok(m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// The operation one event carries (`None`: nothing to do, a delete with
/// `apply_deletes: false`).
pub fn event_op(ev: &CloudEvent, key: &[String], deletes: bool) -> Result<Option<Op>, String> {
    let data: Value =
        serde_json::from_slice(&ev.data).map_err(|e| format!("event {}: not JSON: {e}", ev.id))?;
    let op = ev.extensions.get("loamsop").map(String::as_str);
    let is_change = data.get("after").is_some() || data.get("before").is_some();
    match op {
        Some("t") => {
            return Err(format!(
                "event {}: TRUNCATE is not applied by the sink",
                ev.id
            ));
        }
        Some("d") => {
            if !deletes {
                return Ok(None);
            }
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
                    row.iter()
                        .find(|(c, _)| c.eq_ignore_ascii_case(k))
                        .map(|(_, v)| v.clone())
                        .ok_or_else(|| format!("event {}: the delete has no key column {k}", ev.id))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|k| Some(Op::Delete(k)));
        }
        _ => {}
    }
    let row = if is_change {
        data.get("after").unwrap_or(&Value::Null)
    } else {
        &data
    };
    Ok(Some(Op::Upsert(object_row(row)?)))
}

/// Each Arrow row as an upsert.
pub fn arrow_ops(batch: &RecordBatch) -> Result<Vec<Op>, String> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|r| {
            let mut row = BTreeMap::new();
            for (c, f) in schema.fields().iter().enumerate() {
                row.insert(f.name().clone(), cell(batch.column(c).as_ref(), r)?);
            }
            Ok(Op::Upsert(row))
        })
        .collect()
}

/// Collapse operations so each key appears once, the last one winning.
pub fn collapse(ops: Vec<Op>, key: &[String]) -> Result<Vec<Op>, String> {
    let mut last: BTreeMap<String, (usize, Op)> = BTreeMap::new();
    for (i, op) in ops.into_iter().enumerate() {
        let k = Value::Array(key_of(&op, key)?).to_string();
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

/// The table a sink writes: its columns and upsert key.
#[derive(Debug, Clone)]
pub struct SinkTable {
    pub name: TableName,
    pub def: TableDef,
    pub key: Vec<String>,
}

/// The statements for collapsed operations: deletes first (each key's
/// final operation is unique, so order between keys does not matter), then
/// one upsert per column set.
pub fn statements(t: &SinkTable, ops: &[Op]) -> Result<Vec<String>, String> {
    let col = |c: &str| {
        t.def
            .column(c)
            .ok_or_else(|| format!("{} has no column {c}", t.name))
    };
    let keys: Vec<String> = t.key.iter().map(|k| quote_ident(k)).collect();
    let mut out = Vec::new();
    let deletes: Vec<&Vec<Value>> = ops
        .iter()
        .filter_map(|o| match o {
            Op::Delete(k) => Some(k),
            Op::Upsert(_) => None,
        })
        .collect();
    for chunk in deletes.chunks(ROWS_PER_STATEMENT) {
        let mut tuples = Vec::new();
        for k in chunk {
            let lits = t
                .key
                .iter()
                .zip(k.iter())
                .map(|(c, v)| literal(v, col(c)?))
                .collect::<Result<Vec<_>, _>>()?;
            tuples.push(format!("({})", lits.join(", ")));
        }
        out.push(format!(
            "DELETE FROM {} WHERE ({}) IN ({})",
            t.name.quoted(),
            keys.join(", "),
            tuples.join(", ")
        ));
    }
    let mut groups: BTreeMap<Vec<String>, Vec<&BTreeMap<String, Value>>> = BTreeMap::new();
    for o in ops {
        if let Op::Upsert(row) = o {
            groups
                .entry(row.keys().cloned().collect())
                .or_default()
                .push(row);
        }
    }
    for (cols, rows) in groups {
        let defs = cols.iter().map(|c| col(c)).collect::<Result<Vec<_>, _>>()?;
        let names: Vec<String> = defs.iter().map(|c| quote_ident(&c.name)).collect();
        let set: Vec<String> = defs
            .iter()
            .filter(|c| !t.key.iter().any(|k| k.eq_ignore_ascii_case(&c.name)))
            .map(|c| format!("{0} = VALUES({0})", quote_ident(&c.name)))
            .collect();
        let action = if set.is_empty() {
            // Keys only: an upsert that changes nothing.
            format!("{0} = {0}", keys[0])
        } else {
            set.join(", ")
        };
        for chunk in rows.chunks(ROWS_PER_STATEMENT) {
            let mut tuples = Vec::new();
            for row in chunk {
                let lits = defs
                    .iter()
                    .zip(&cols)
                    .map(|(d, c)| literal(row.get(c).unwrap_or(&Value::Null), d))
                    .collect::<Result<Vec<_>, _>>()?;
                tuples.push(format!("({})", lits.join(", ")));
            }
            out.push(format!(
                "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {action}",
                t.name.quoted(),
                names.join(", "),
                tuples.join(", ")
            ));
        }
    }
    Ok(out)
}

/// Makes the instance's sink.
#[derive(Debug)]
pub struct MySinkFactory {
    cfg: MyConfig,
    ctx: ConnectorContext,
}

impl MySinkFactory {
    pub fn new(cfg: MyConfig, ctx: ConnectorContext) -> Result<Self, ConnectorError> {
        if cfg.table.is_none() {
            return Err(ConnectorError::fatal(
                "config: table is required for a sink",
            ));
        }
        Ok(Self { cfg, ctx })
    }
}

impl SinkFactory for MySinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let sink = MySink::new(self.cfg.clone(), self.ctx.clone());
        Ok(if self.cfg.transactional {
            SinkHandle::Transactional(Box::new(sink))
        } else {
            SinkHandle::Plain(Box::new(sink))
        })
    }
}

static TXN_SEQ: AtomicU64 = AtomicU64::new(0);

/// See the module documentation.
pub struct MySink {
    cfg: MyConfig,
    ctx: ConnectorContext,
    conn: Option<MyConn>,
    target: Option<SinkTable>,
    /// Exactly-once: the batch's operations written so far in the open
    /// transaction, to replay after a lost connection.
    staged: Vec<Op>,
    in_txn: bool,
    prepared: Option<(TxnToken, Checkpoint)>,
}

impl std::fmt::Debug for MySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySink")
            .field("table", &self.cfg.table.as_ref().map(ToString::to_string))
            .field("transactional", &self.cfg.transactional)
            .finish_non_exhaustive()
    }
}

fn instance_key(i: &loams_connector_sdk::InstanceKey) -> String {
    format!("{}/{}", i.namespace, i.name)
}

impl MySink {
    pub fn new(cfg: MyConfig, ctx: ConnectorContext) -> Self {
        Self {
            cfg,
            ctx,
            conn: None,
            target: None,
            staged: Vec::new(),
            in_txn: false,
            prepared: None,
        }
    }

    fn checkpoints(&self) -> String {
        TableName::new(&self.cfg.checkpoint_database, CHECKPOINTS).quoted()
    }

    async fn conn(&mut self) -> Result<&mut MyConn, ConnectorError> {
        if self.conn.as_ref().is_none_or(MyConn::is_broken) {
            self.in_txn = false;
            let mut conn = connect(&self.cfg, &self.ctx).await?;
            if self.target.is_none() {
                let name = self.cfg.table.clone().expect("checked by the factory");
                let def = describe_def(&mut conn, &name)
                    .await?
                    .ok_or_else(|| ConnectorError::fatal(format!("table {name} does not exist")))?;
                let key = if self.cfg.key.is_empty() {
                    def.key.clone()
                } else {
                    self.cfg.key.clone()
                };
                if key.is_empty() {
                    return Err(ConnectorError::fatal(format!(
                        "{name} has no primary key: set `key`"
                    )));
                }
                self.target = Some(SinkTable { name, def, key });
            }
            self.conn = Some(conn);
        }
        Ok(self.conn.as_mut().expect("set above"))
    }

    async fn exec(&mut self, sql: &str, what: &str) -> Result<u64, ConnectorError> {
        let r = self.conn().await?.exec(sql).await;
        r.map_err(|e| e.to_connector(what))
    }

    fn ops(&self, batch: &EventBatch) -> Result<Vec<Op>, ConnectorError> {
        let key = &self.target.as_ref().expect("described").key;
        match batch {
            EventBatch::Events(evs) => evs
                .iter()
                .map(|e| event_op(e, key, self.cfg.apply_deletes))
                .filter_map(Result::transpose)
                .collect::<Result<Vec<_>, _>>()
                .map_err(ConnectorError::poison),
            EventBatch::Arrow { batch, .. } => arrow_ops(batch).map_err(ConnectorError::poison),
        }
    }

    async fn apply(&mut self, ops: &[Op]) -> Result<(), ConnectorError> {
        let target = self.target.clone().expect("described");
        let ops = collapse(ops.to_vec(), &target.key).map_err(ConnectorError::poison)?;
        let stmts = statements(&target, &ops).map_err(|e| {
            if e.contains("has no column") {
                ConnectorError::schema_incompatible(e)
            } else {
                ConnectorError::poison(e)
            }
        })?;
        for sql in stmts {
            self.exec(&sql, &format!("writing {}", target.name)).await?;
        }
        Ok(())
    }

    async fn begin(&mut self) -> Result<(), ConnectorError> {
        if self.in_txn {
            return Ok(());
        }
        self.exec("START TRANSACTION", "START TRANSACTION").await?;
        self.in_txn = true;
        // Replay what this batch had written before a lost connection.
        if !self.staged.is_empty() {
            let staged = self.staged.clone();
            self.apply(&staged).await?;
        }
        Ok(())
    }

    /// A lost connection, a deadlock or a lock timeout: the transaction is
    /// gone (dropping the connection rolls it back), the staged operations
    /// are kept and replayed in the next one.
    fn reset_on(&mut self, e: &ConnectorError) {
        if e.class.is_transient() {
            self.in_txn = false;
            self.conn = None;
        }
    }

    async fn write_txn(&mut self, ops: Vec<Op>) -> Result<(), ConnectorError> {
        self.begin().await?;
        self.exec("SAVEPOINT loams_write", "SAVEPOINT").await?;
        match self.apply(&ops).await {
            Ok(()) => {
                self.exec("RELEASE SAVEPOINT loams_write", "RELEASE SAVEPOINT")
                    .await?;
                self.staged.extend(ops);
                Ok(())
            }
            Err(e)
                if e.class == ErrorClass::Poison || e.class == ErrorClass::SchemaIncompatible =>
            {
                self.exec("ROLLBACK TO SAVEPOINT loams_write", "ROLLBACK TO SAVEPOINT")
                    .await?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn ensure_checkpoint_table(&mut self) -> Result<(), ConnectorError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (instance VARCHAR(255) NOT NULL, split VARCHAR(255) NOT NULL, \
             position LONGBLOB NOT NULL, display TEXT NOT NULL, epoch BIGINT UNSIGNED NOT NULL, \
             txn VARCHAR(128) NOT NULL, updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
             ON UPDATE CURRENT_TIMESTAMP(6), PRIMARY KEY (instance, split), KEY (instance, txn)) ENGINE=InnoDB",
            self.checkpoints()
        );
        self.exec(&sql, "creating the checkpoint table")
            .await
            .map(|_| ())
    }

    async fn write_checkpoint(
        &mut self,
        token: &TxnToken,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        let instance = instance_key(&cp.instance);
        let table = self.checkpoints();
        for (split, pos) in &cp.splits {
            let rs = self
                .conn()
                .await?
                .query(&format!(
                    "SELECT epoch FROM {table} WHERE instance = {} AND split = {} FOR UPDATE",
                    quote_str(&instance),
                    quote_str(split.as_str())
                ))
                .await
                .map_err(|e| e.to_connector("reading the checkpoint"))?;
            if let Some(stored) = rs.scalar().and_then(|s| s.parse::<u64>().ok())
                && stored > cp.epoch
            {
                return Err(ConnectorError::fatal(format!(
                    "checkpoint fenced: {table} holds epoch {stored}, newer than {}",
                    cp.epoch
                )));
            }
            let sql = format!(
                "INSERT INTO {table} (instance, split, position, display, epoch, txn) \
                 VALUES ({}, {}, {}, {}, {}, {}) ON DUPLICATE KEY UPDATE position = VALUES(position), \
                 display = VALUES(display), epoch = VALUES(epoch), txn = VALUES(txn)",
                quote_str(&instance),
                quote_str(split.as_str()),
                hex_literal(&pos.opaque),
                quote_str(&pos.display),
                cp.epoch,
                quote_str(&token.id)
            );
            self.exec(&sql, "writing the checkpoint").await?;
        }
        Ok(())
    }

    /// Whether the checkpoint table holds `txn` (the transaction committed).
    async fn committed_txn(&mut self, txn: &str, instance: &str) -> Result<bool, ConnectorError> {
        let sql = format!(
            "SELECT 1 FROM {} WHERE instance = {} AND txn = {}",
            self.checkpoints(),
            quote_str(instance),
            quote_str(txn)
        );
        let rs = self
            .conn()
            .await?
            .query(&sql)
            .await
            .map_err(|e| e.to_connector("reading the checkpoint"))?;
        Ok(!rs.rows.is_empty())
    }

    async fn commit_now(&mut self) -> Result<(), ConnectorError> {
        self.exec("COMMIT", "COMMIT").await?;
        self.in_txn = false;
        self.staged.clear();
        self.prepared = None;
        Ok(())
    }
}

#[async_trait]
impl Sink for MySink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let r = async {
            self.conn().await?;
            let ack = SinkAck {
                rows: batch.rows() as u64,
                bytes: batch.size_bytes() as u64,
            };
            let ops = self.ops(&batch)?;
            if self.cfg.transactional {
                self.write_txn(ops).await?;
            } else {
                self.apply(&ops).await?;
            }
            Ok(ack)
        }
        .await;
        if let Err(e) = &r {
            self.reset_on(e);
        }
        r
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Plain mode writes each statement in its own implicit transaction.
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for MySink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        if self.in_txn {
            let _ = self.exec("ROLLBACK", "ROLLBACK").await;
            self.in_txn = false;
        }
        self.ensure_checkpoint_table().await?;
        self.staged.clear();
        self.prepared = None;
        let instance = instance_key(&self.ctx.instance);
        let sql = format!(
            "SELECT split, HEX(position) AS position_hex, display, epoch FROM {} WHERE instance = {}",
            self.checkpoints(),
            quote_str(&instance)
        );
        let rs = self
            .conn()
            .await?
            .query(&sql)
            .await
            .map_err(|e| e.to_connector("reading the checkpoint"))?;
        if rs.rows.is_empty() {
            return Ok(None);
        }
        let mut cp = Checkpoint::new(self.ctx.instance.clone(), 0);
        for i in 0..rs.rows.len() {
            let split = rs.get(i, "split").unwrap_or_default();
            let hex = rs.get(i, "position_hex").unwrap_or_default();
            let opaque = (0..hex.len() / 2)
                .map(|j| u8::from_str_radix(&hex[2 * j..2 * j + 2], 16))
                .collect::<Result<Vec<u8>, _>>()
                .map_err(|_| ConnectorError::fatal("a malformed checkpoint position"))?;
            let display = rs.get(i, "display").unwrap_or_default();
            let epoch: u64 = rs
                .get(i, "epoch")
                .and_then(|e| e.parse().ok())
                .unwrap_or_default();
            cp.epoch = cp.epoch.max(epoch);
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
        if let Err(e) = r {
            self.reset_on(&e);
            return Err(e);
        }
        self.prepared = Some((token.clone(), cp.clone()));
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let instance = instance_key(&self.ctx.instance);
        if self.in_txn && self.conn.as_ref().is_some_and(|c| !c.is_broken()) {
            match self.commit_now().await {
                Ok(()) => return Ok(()),
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
            self.commit_now().await
        }
        .await;
        if let Err(e) = &r {
            self.reset_on(e);
        }
        r
    }

    async fn abort(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        if self.in_txn && self.conn.as_ref().is_some_and(|c| !c.is_broken()) {
            let _ = self.exec("ROLLBACK", "ROLLBACK").await;
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
    use crate::types::ColumnDef;
    use serde_json::json;

    fn ev(op: Option<&str>, data: Value) -> CloudEvent {
        let mut e = CloudEvent::new("id-1", "/connectors/x/t", "t")
            .with_data("application/json", data.to_string());
        if let Some(op) = op {
            e = e.with_extension("loamsop", op);
        }
        e
    }

    fn target() -> SinkTable {
        SinkTable {
            name: TableName::new("shop", "items"),
            def: TableDef {
                columns: vec![
                    ColumnDef::new("id", "bigint", false),
                    ColumnDef::new("name", "varchar(20)", true),
                    ColumnDef::new("blob", "varbinary(8)", true),
                ],
                key: vec!["id".into()],
            },
            key: vec!["id".into()],
        }
    }

    fn up(pairs: &[(&str, Value)]) -> Op {
        Op::Upsert(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn events_become_operations() {
        let key = vec!["id".to_owned()];
        assert_eq!(
            event_op(&ev(None, json!({"id": 1, "name": "a"})), &key, true),
            Ok(Some(up(&[("id", json!(1)), ("name", json!("a"))])))
        );
        let change =
            json!({"op": "u", "key": {"id": 2}, "before": null, "after": {"id": 2, "name": "b"}});
        assert_eq!(
            event_op(&ev(Some("u"), change), &key, true),
            Ok(Some(up(&[("id", json!(2)), ("name", json!("b"))])))
        );
        let del = json!({"op": "d", "key": {"ID": 3}, "before": {"id": 3}, "after": null});
        assert_eq!(
            event_op(&ev(Some("d"), del.clone()), &key, true),
            Ok(Some(Op::Delete(vec![json!(3)])))
        );
        assert_eq!(event_op(&ev(Some("d"), del), &key, false), Ok(None));
        assert!(event_op(&ev(Some("t"), json!({})), &key, true).is_err());
        assert!(event_op(&ev(None, json!([1])), &key, true).is_err());
    }

    #[test]
    fn the_last_operation_per_key_wins() {
        let key = vec!["id".to_owned()];
        let ops = vec![
            up(&[("id", json!(1)), ("name", json!("a"))]),
            up(&[("id", json!(2)), ("name", json!("b"))]),
            Op::Delete(vec![json!(1)]),
            up(&[("id", json!(2)), ("blob", json!("AA=="))]),
        ];
        assert_eq!(
            collapse(ops, &key),
            Ok(vec![
                Op::Delete(vec![json!(1)]),
                up(&[
                    ("id", json!(2)),
                    ("name", json!("b")),
                    ("blob", json!("AA=="))
                ]),
            ])
        );
    }

    #[test]
    fn statements_use_literals_that_need_no_escaping() {
        let t = target();
        let ops = vec![
            Op::Delete(vec![json!(9)]),
            up(&[("id", json!(1)), ("name", json!("it's"))]),
            up(&[("id", json!(2)), ("name", json!(null))]),
        ];
        let s = statements(&t, &ops).expect("statements");
        assert_eq!(s.len(), 2);
        assert_eq!(s[0], "DELETE FROM `shop`.`items` WHERE (`id`) IN ((9))");
        assert_eq!(
            s[1],
            "INSERT INTO `shop`.`items` (`id`, `name`) VALUES (1, CONVERT(X'69742773' USING utf8mb4)), \
             (2, NULL) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        let keys_only = statements(&t, &[up(&[("id", json!(5))])]).expect("keys only");
        assert!(keys_only[0].ends_with("ON DUPLICATE KEY UPDATE `id` = `id`"));
        let unknown = statements(&t, &[up(&[("id", json!(1)), ("color", json!("red"))])]);
        assert!(unknown.expect_err("unknown").contains("no column color"));
        let bin = statements(&t, &[up(&[("id", json!(1)), ("blob", json!("AP8="))])]).expect("bin");
        assert!(bin[0].contains("X'00FF'"), "{}", bin[0]);
    }

    use crate::config::Target as SystemTarget;
    use crate::conn::fake::{Reply, Seen, row, script};
    use loams_connector_sdk::{InstanceKey, Scrubber, StaticSecrets};
    use std::sync::Arc;

    /// A scripted server for the sink: table `d.t (id, v)`, and a stored
    /// checkpoint epoch of `epoch` (none if `None`).
    async fn sink_against(epoch: Option<&'static str>) -> (MySink, Seen) {
        let (port, seen) = script(
            "8.4.10",
            move |sql| {
                if sql.starts_with("SELECT COLUMN_NAME, COLUMN_TYPE") {
                    vec![
                        Reply::Rows(
                            vec!["COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE"],
                            vec![
                                row(&["id", "bigint", "NO"]),
                                row(&["v", "varchar(255)", "YES"]),
                            ],
                        ),
                        Reply::Rows(vec!["COLUMN_NAME"], vec![row(&["id"])]),
                    ]
                } else if sql.starts_with("SELECT split") {
                    vec![Reply::Rows(
                        vec!["split", "position_hex", "display", "epoch"],
                        vec![],
                    )]
                } else if sql.starts_with("SELECT epoch") {
                    vec![Reply::Rows(
                        vec!["epoch"],
                        epoch.map(|e| row(&[e])).into_iter().collect(),
                    )]
                } else {
                    vec![Reply::Ok(1)]
                }
            },
            vec![],
        )
        .await;
        let inst = InstanceKey::new(1, "i");
        let config =
            json!({"host": "127.0.0.1", "port": port, "user": "u", "database": "d", "table": "t"});
        let ctx = ConnectorContext::new(
            inst.clone(),
            config.clone(),
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        );
        let cfg = MyConfig::parse(SystemTarget::Mysql, &inst, &config).expect("config");
        (MySink::new(cfg, ctx), seen)
    }

    fn statements_seen(seen: &Seen) -> Vec<String> {
        seen.lock()
            .expect("seen")
            .iter()
            .filter(|p| p[0] == 0x03)
            .map(|p| String::from_utf8_lossy(&p[1..]).into_owned())
            .filter(|s| !s.starts_with("SET NAMES") && !s.starts_with("SELECT COLUMN_NAME"))
            .collect()
    }

    fn checkpoint(epoch: u64) -> Checkpoint {
        let mut cp = Checkpoint::new(InstanceKey::new(1, "i"), epoch);
        cp.splits
            .insert(SplitId::new("fabric"), Position::from_u64(42));
        cp
    }

    /// The exactly-once order: the batch and the checkpoint in one
    /// transaction, the checkpoint row locked first, then `COMMIT`.
    #[tokio::test]
    async fn a_batch_and_its_checkpoint_commit_together() {
        let (mut sink, seen) = sink_against(Some("1")).await;
        assert_eq!(sink.recover().await.expect("recover"), None);
        let ev = ev(None, json!({"id": 7, "v": "x"}));
        sink.write(EventBatch::Events(vec![ev]))
            .await
            .expect("write");
        let token = sink.prepare(&checkpoint(2)).await.expect("prepare");
        sink.commit(token).await.expect("commit");
        let sql = statements_seen(&seen);
        let pos = |prefix: &str| {
            sql.iter()
                .position(|s| s.starts_with(prefix))
                .unwrap_or_else(|| panic!("no {prefix} in {sql:?}"))
        };
        assert!(
            pos("CREATE TABLE IF NOT EXISTS `d`.`_loams_checkpoints`") < pos("START TRANSACTION")
        );
        assert!(pos("START TRANSACTION") < pos("SAVEPOINT loams_write"));
        assert!(pos("SAVEPOINT loams_write") < pos("INSERT INTO `d`.`t`"));
        assert!(pos("INSERT INTO `d`.`t`") < pos("RELEASE SAVEPOINT"));
        assert!(pos("RELEASE SAVEPOINT") < pos("SELECT epoch"));
        assert!(sql[pos("SELECT epoch")].ends_with("FOR UPDATE"));
        assert!(pos("SELECT epoch") < pos("INSERT INTO `d`.`_loams_checkpoints`"));
        assert!(pos("INSERT INTO `d`.`_loams_checkpoints`") < pos("COMMIT"));
        assert!(sql[pos("INSERT INTO `d`.`_loams_checkpoints`")].contains("X'000000000000002A'"));
    }

    /// Review Focus 12: a newer epoch in the target fences a stale sink.
    #[tokio::test]
    async fn a_newer_epoch_fences_the_checkpoint() {
        let (mut sink, _) = sink_against(Some("9")).await;
        sink.recover().await.expect("recover");
        sink.write(EventBatch::Events(vec![ev(
            None,
            json!({"id": 1, "v": "y"}),
        )]))
        .await
        .expect("write");
        let e = sink.prepare(&checkpoint(2)).await.expect_err("fenced");
        assert_eq!(e.class, ErrorClass::Fatal);
        assert!(e.message.contains("fenced"), "{}", e.message);
    }

    #[tokio::test]
    async fn an_unknown_column_pauses_and_truncate_is_poison() {
        let (mut sink, _) = sink_against(None).await;
        sink.recover().await.expect("recover");
        let e = sink
            .write(EventBatch::Events(vec![ev(
                None,
                json!({"id": 1, "colour": "red"}),
            )]))
            .await
            .expect_err("unknown column");
        assert_eq!(e.class, ErrorClass::SchemaIncompatible);
        let e = sink
            .write(EventBatch::Events(vec![ev(Some("t"), json!({}))]))
            .await
            .expect_err("truncate");
        assert_eq!(e.class, ErrorClass::Poison);
    }
}
