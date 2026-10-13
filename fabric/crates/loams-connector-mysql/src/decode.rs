//! Binlog events to `change` events (CN2 Task 11, design §33 §7).
//!
//! Each row change is one CloudEvent: `type`
//! `io.loams.dev.flow.<connector>.change.v1`, `source`
//! `/connectors/<instance>/<db.table>`, `subject` the table, `id`
//! `<transaction>/<ordinal>` where the transaction is its GTID
//! (`uuid:gno`, `uuid:tag:gno`, MariaDB's `domain-server-seq`) or, with GTIDs
//! off, `<file>:<offset>` of its first event — stable across re-reads, so
//! `ingest`'s dedup makes the stream effectively once. `loamsop` is `c`,
//! `u`, `d` (and `t` for `TRUNCATE`, as R10.5); `loamslsn` is
//! `<file>/<end offset, 20 digits>/<ordinal, 10 digits>`, sortable as text
//! within a server's binlog. `data` is JSON `{table, op, key, before,
//! after}` in the canonical value form of [`crate::types`].
//!
//! Columns are named from `TABLE_MAP`'s metadata when the server logs it
//! (`binlog_row_metadata=FULL`) and otherwise from the schema history
//! ([`crate::history`]); either way the binlog's column types are checked
//! against the history's, and a mismatch asks the source to refresh the
//! table from `information_schema` (once) before it is a pause.
//!
//! The position after each event is a [`Mark`]: the binlog position after
//! the last whole transaction, plus, mid-transaction, how many changes of
//! the open one were delivered (a restart re-reads it and skips them).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use loams_connector_sdk::schema::{SchemaPolicy, SchemaTracker};
use loams_connector_sdk::{CdcOp, CloudEvent, ConnectorError, EnvelopeBuilder, Position};
use mysql_common::binlog::consts::EventType;
use mysql_common::binlog::events::{
    EventData, GtidEvent, QueryEvent, RotateEvent, RowsEventData, TableMapEvent,
};
use mysql_common::binlog::row::BinlogRow;
use mysql_common::constants::ColumnType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlparser::ast::Statement;

use crate::binlog::RawEvent;
use crate::config::TableName;
use crate::gtid::{
    BinlogPos, MARIADB_COMPRESSED, MARIADB_GTID, MariaGtid, MariaGtidEvent, uuid_text,
};
use crate::history::{History, TableDef};
use crate::table::arrow_schema;
use crate::types::{ColumnDef, Kind, binlog_to_json};

/// Part of a transaction delivered: its id and how many changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partial {
    pub txn: String,
    pub delivered: u64,
}

/// The binlog side of a position (see the module documentation).
#[derive(Debug, Clone, PartialEq)]
pub struct Mark {
    pub binlog: BinlogPos,
    pub partial: Option<Partial>,
    pub history: Arc<History>,
}

/// What one binlog event produces.
// Unboxed, as the SDK's `EventBatch` is: most values are `Event`s, and each
// moves once into the source's queue.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Emitted {
    /// An event to deliver: a change (with its table and key, for the
    /// snapshot's reconciliation), a truncate, or a schema change.
    Event {
        event: CloudEvent,
        table: TableName,
        key: Option<Vec<Value>>,
        mark: Mark,
    },
    /// A row this instance wrote to the watermark table (DBLog).
    Watermark { value: String, mark: Mark },
    /// A row the decoder could not read: the source reports it as `Poison`
    /// (a DLQ record) and carries on past it (R4.10).
    Poison { message: String, mark: Mark },
}

/// Why [`Decoder::feed`] stopped.
#[derive(Debug)]
pub enum FeedError {
    /// The history does not match the binlog for these tables: refresh
    /// them from `information_schema` and feed the same event again.
    Refresh(Vec<TableName>),
    Error(ConnectorError),
}

impl From<ConnectorError> for FeedError {
    fn from(e: ConnectorError) -> Self {
        FeedError::Error(e)
    }
}

#[derive(Debug, Clone)]
enum TxnGtid {
    Mysql([u8; 16], Option<String>, u64),
    Maria(MariaGtid),
    Anonymous,
}

#[derive(Debug, Clone)]
struct Txn {
    id: String,
    gtid: TxnGtid,
    ordinal: u64,
    skip: u64,
    /// `BEGIN` was seen: a statement does not end the transaction.
    began: bool,
}

/// Which tables are captured.
#[derive(Debug, Clone)]
pub enum Captured {
    Tables(BTreeSet<TableName>),
    /// Every table of a database but these.
    Database {
        database: String,
        except: BTreeSet<TableName>,
    },
}

impl Captured {
    pub fn contains(&self, t: &TableName) -> bool {
        match self {
            Captured::Tables(s) => s.contains(t),
            Captured::Database { database, except } => {
                &t.database == database && !except.contains(t)
            }
        }
    }
}

/// Every column of `s`, nullable (a CDC stream checks types, not
/// nullability, as R10.4 does for Postgres).
pub fn all_nullable(s: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        s.fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ))
}

/// Whether a binlog column type can hold a value of `kind`.
pub fn compatible(bt: ColumnType, kind: &Kind) -> bool {
    use ColumnType::*;
    let stringy = matches!(
        bt,
        MYSQL_TYPE_VARCHAR
            | MYSQL_TYPE_VAR_STRING
            | MYSQL_TYPE_STRING
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB
            | MYSQL_TYPE_BLOB
    );
    match kind {
        Kind::Int { .. } => matches!(
            bt,
            MYSQL_TYPE_TINY
                | MYSQL_TYPE_SHORT
                | MYSQL_TYPE_INT24
                | MYSQL_TYPE_LONG
                | MYSQL_TYPE_LONGLONG
        ),
        Kind::Float | Kind::Double => matches!(bt, MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE),
        Kind::Decimal { .. } => matches!(bt, MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL),
        Kind::Date => matches!(bt, MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE),
        Kind::DateTime => matches!(bt, MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2),
        Kind::Timestamp => matches!(bt, MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2),
        Kind::Time => matches!(bt, MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2),
        Kind::Year => bt == MYSQL_TYPE_YEAR,
        Kind::Text { .. } | Kind::Binary => stringy,
        Kind::Bit => bt == MYSQL_TYPE_BIT,
        Kind::Enum(_) | Kind::Set(_) => {
            matches!(bt, MYSQL_TYPE_ENUM | MYSQL_TYPE_SET | MYSQL_TYPE_STRING)
        }
        Kind::Json => bt == MYSQL_TYPE_JSON || stringy,
        Kind::Other(_) => true,
    }
}

/// The `loamslsn` of a change.
pub fn loamslsn(file: &str, end: u64, ordinal: u64) -> String {
    format!("{file}/{end:020}/{ordinal:010}")
}

/// See the module documentation.
pub struct Decoder {
    envelope: EnvelopeBuilder,
    policy: SchemaPolicy,
    tracker: SchemaTracker,
    captured: Captured,
    watermark: TableName,
    watermark_owner: String,
    pos: BinlogPos,
    file: String,
    last_end: u64,
    txn: Option<Txn>,
    resume: Option<Partial>,
    maps: HashMap<u64, TableMapEvent<'static>>,
    history: Arc<History>,
    /// Tables a DDL statement changed in a way the history does not model:
    /// the source re-reads them from `information_schema` after the event
    /// ([`Decoder::take_refresh`], [`Decoder::apply_refresh`]).
    after_ddl: Vec<TableName>,
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("pos", &self.pos)
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

impl Decoder {
    /// A decoder resuming at `pos` (`partial`: changes of the next
    /// transaction already delivered) with `history`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        envelope: EnvelopeBuilder,
        policy: &SchemaPolicy,
        captured: Captured,
        watermark: TableName,
        watermark_owner: String,
        pos: BinlogPos,
        partial: Option<Partial>,
        history: Arc<History>,
    ) -> Self {
        let (file, last_end) = {
            let (f, p) = pos.file_pos();
            (f.to_owned(), p)
        };
        Self {
            envelope,
            policy: policy.clone(),
            tracker: SchemaTracker::new(policy.evolution),
            captured,
            watermark,
            watermark_owner,
            pos,
            file,
            last_end,
            txn: None,
            resume: partial,
            maps: HashMap::new(),
            history,
            after_ddl: Vec::new(),
        }
    }

    /// Tables to re-read from `information_schema` after the last event.
    pub fn take_refresh(&mut self) -> Vec<TableName> {
        std::mem::take(&mut self.after_ddl)
    }

    /// A table re-read after a DDL statement: the history takes it, and a
    /// captured table's new schema is announced (or refused by the
    /// evolution policy).
    pub fn apply_refresh(
        &mut self,
        table: &TableName,
        def: Option<TableDef>,
    ) -> Result<Option<Emitted>, ConnectorError> {
        self.refreshed(table, def);
        if !self.captured.contains(table) {
            return Ok(None);
        }
        let Some(def) = self.history.get(table) else {
            return Ok(None);
        };
        let schema = all_nullable(&*arrow_schema(table, def, &self.policy.lossy)?);
        let at = self.at_position(&format!("{}:{}", self.file, self.last_end));
        Ok(self
            .tracker
            .observe(&table.to_string(), schema, &at)?
            .map(|c| Emitted::Event {
                event: c.event(&self.envelope),
                table: table.clone(),
                key: None,
                mark: self.position(),
            }))
    }

    /// Track `table`'s schema without announcing it (a table known at
    /// start).
    pub fn seed(&mut self, table: &TableName) -> Result<(), ConnectorError> {
        if let Some(def) = self.history.get(table) {
            let schema = arrow_schema(table, def, &self.policy.lossy)?;
            self.tracker.seed(table.to_string(), all_nullable(&schema));
        }
        Ok(())
    }

    /// Track `table`'s schema and return its new-table `schema-change`
    /// event (the snapshot announces each table it starts).
    pub fn announce(
        &mut self,
        table: &TableName,
        at: &Position,
    ) -> Result<Option<CloudEvent>, ConnectorError> {
        let Some(def) = self.history.get(table) else {
            return Ok(None);
        };
        let schema = all_nullable(&*arrow_schema(table, def, &self.policy.lossy)?);
        Ok(self
            .tracker
            .observe(&table.to_string(), schema, at)?
            .map(|c| c.event(&self.envelope)))
    }

    pub fn history(&self) -> &Arc<History> {
        &self.history
    }

    /// Replace `table`'s definition (from `information_schema`) or drop it.
    pub fn refreshed(&mut self, table: &TableName, def: Option<TableDef>) {
        let h = Arc::make_mut(&mut self.history);
        match def {
            Some(d) => h.set(table, d),
            None => h.remove(table),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// The position after everything fed so far.
    pub fn position(&self) -> Mark {
        Mark {
            binlog: self.pos.clone(),
            partial: self
                .txn
                .as_ref()
                .filter(|t| t.ordinal > 0)
                .map(|t| Partial {
                    txn: t.id.clone(),
                    delivered: t.ordinal,
                }),
            history: self.history.clone(),
        }
    }

    fn begin(&mut self, id: String, gtid: TxnGtid, began: bool) {
        let skip = match &self.resume {
            Some(p) if p.txn == id => p.delivered,
            _ => 0,
        };
        self.resume = None;
        self.txn = Some(Txn {
            id,
            gtid,
            ordinal: 0,
            skip,
            began,
        });
    }

    fn commit(&mut self, end: u64) {
        if let Some(t) = self.txn.take() {
            match (&mut self.pos, t.gtid) {
                (BinlogPos::Mysql { executed, .. }, TxnGtid::Mysql(u, tag, gno)) => {
                    executed.add(&u, tag.as_deref(), gno);
                }
                (BinlogPos::Mariadb { state, .. }, TxnGtid::Maria(g)) => state.update(g),
                _ => {}
            }
        }
        let file = self.file.clone();
        self.pos.set_file_pos(&file, end);
    }

    /// The mark of the next change of the open transaction.
    fn next_mark(&self) -> Mark {
        Mark {
            binlog: self.pos.clone(),
            partial: self.txn.as_ref().map(|t| Partial {
                txn: t.id.clone(),
                delivered: t.ordinal,
            }),
            history: self.history.clone(),
        }
    }

    fn at_position(&self, txn: &str) -> Position {
        Position::new(Bytes::new(), txn.to_owned())
    }

    /// Decode one event. On [`FeedError::Refresh`] nothing has changed, and
    /// the same event may be fed again.
    pub fn feed(&mut self, raw: &RawEvent) -> Result<Vec<Emitted>, FeedError> {
        let ev = &raw.event;
        let ty = ev.header().event_type_raw();
        let end = raw.log_pos;
        let mut out = Vec::new();
        let err =
            |e: std::io::Error| FeedError::Error(ConnectorError::fatal(format!("binlog: {e}")));
        if MARIADB_COMPRESSED.contains(&ty) {
            return Err(FeedError::Error(ConnectorError::fatal(
                "the binlog has compressed events: set log_bin_compress = OFF on the server",
            )));
        }
        if ty == MARIADB_GTID {
            let g = MariaGtidEvent::parse(ev.data(), ev.header().server_id()).ok_or_else(|| {
                FeedError::Error(ConnectorError::fatal("a malformed MariaDB GTID event"))
            })?;
            self.begin(g.gtid.to_string(), TxnGtid::Maria(g.gtid), !g.standalone);
            self.last_end = end;
            return Ok(out);
        }
        match ty {
            t if t == EventType::ROTATE_EVENT as u8 => {
                let r = ev.read_event::<RotateEvent<'_>>().map_err(err)?;
                self.file = r.name().into_owned();
                self.last_end = r.position();
                if self.txn.is_none() {
                    let file = self.file.clone();
                    self.pos.set_file_pos(&file, self.last_end);
                }
                return Ok(out);
            }
            t if t == EventType::GTID_EVENT as u8
                || t == EventType::GTID_TAGGED_LOG_EVENT as u8 =>
            {
                let g = ev.read_event::<GtidEvent>().map_err(err)?;
                let tag = g.tag().map(|t| t.to_string().to_ascii_lowercase());
                let sid = g.sid();
                let id = match &tag {
                    Some(tag) => format!("{}:{tag}:{}", uuid_text(&sid), g.gno()),
                    None => format!("{}:{}", uuid_text(&sid), g.gno()),
                };
                self.begin(id, TxnGtid::Mysql(sid, tag, g.gno()), false);
            }
            t if t == EventType::ANONYMOUS_GTID_EVENT as u8 => {
                let id = format!("{}:{}", self.file, self.last_end);
                self.begin(id, TxnGtid::Anonymous, false);
            }
            t if t == EventType::QUERY_EVENT as u8 => {
                let q = ev.read_event::<QueryEvent<'_>>().map_err(err)?;
                let sql = q.query().trim().to_owned();
                let upper = sql.to_ascii_uppercase();
                if upper == "BEGIN" || upper.starts_with("XA START") {
                    if self.txn.is_none() {
                        let id = format!("{}:{}", self.file, self.last_end);
                        self.begin(id, TxnGtid::Anonymous, true);
                    }
                    if let Some(t) = &mut self.txn {
                        t.began = true;
                    }
                } else if upper == "COMMIT" || upper == "ROLLBACK" || upper.starts_with("XA ") {
                    self.commit(end);
                } else {
                    if self.txn.is_none() {
                        let id = format!("{}:{}", self.file, self.last_end);
                        self.begin(id, TxnGtid::Anonymous, false);
                    }
                    out = self.statement(&q.schema(), &sql, end)?;
                    if self.txn.as_ref().is_some_and(|t| !t.began) {
                        self.commit(end);
                    }
                }
            }
            t if t == EventType::TABLE_MAP_EVENT as u8 => {
                let tme = ev.read_event::<TableMapEvent<'_>>().map_err(err)?;
                self.maps.insert(tme.table_id(), tme.into_owned());
            }
            t if t == EventType::XID_EVENT as u8 || t == EventType::XA_PREPARE_LOG_EVENT as u8 => {
                self.commit(end);
            }
            t if [
                EventType::WRITE_ROWS_EVENT_V1,
                EventType::UPDATE_ROWS_EVENT_V1,
                EventType::DELETE_ROWS_EVENT_V1,
                EventType::WRITE_ROWS_EVENT,
                EventType::UPDATE_ROWS_EVENT,
                EventType::DELETE_ROWS_EVENT,
                EventType::PARTIAL_UPDATE_ROWS_EVENT,
            ]
            .iter()
            .any(|e| *e as u8 == t) =>
            {
                let Some(EventData::RowsEvent(rows)) = ev.read_data().map_err(err)? else {
                    return Err(FeedError::Error(ConnectorError::fatal(
                        "a malformed rows event",
                    )));
                };
                out = self.rows(&rows, end)?;
            }
            _ => {}
        }
        // Only events of transactions move the offset a GTID-less
        // transaction is named by: format descriptions, heartbeats and GTID
        // summaries arrive with offsets that differ between a first read and
        // a resumed one, and would make its id unstable.
        let meta = [
            EventType::FORMAT_DESCRIPTION_EVENT as u8,
            EventType::HEARTBEAT_EVENT as u8,
            0x29, // HEARTBEAT_LOG_EVENT_V2
            EventType::PREVIOUS_GTIDS_EVENT as u8,
            crate::gtid::MARIADB_GTID_LIST,
            crate::gtid::MARIADB_BINLOG_CHECKPOINT,
        ];
        if end != 0 && !meta.contains(&ty) {
            self.last_end = end;
        }
        Ok(out)
    }

    /// A statement that is not BEGIN/COMMIT: DDL moves the history (and
    /// announces captured tables' new schemas); TRUNCATE is a `t` change.
    fn statement(&mut self, schema: &str, sql: &str, end: u64) -> Result<Vec<Emitted>, FeedError> {
        let mut out = Vec::new();
        let captured = self.captured.clone();
        let watermark = self.watermark.clone();
        let is_captured = move |t: &TableName| captured.contains(t) || *t == watermark;
        // TRUNCATE has no row events.
        if sql.len() > 8 && sql[..8].eq_ignore_ascii_case("TRUNCATE") {
            if let Ok(statements) =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
            {
                for st in statements {
                    if let Statement::Truncate(t) = st {
                        for target in &t.table_names {
                            let name = target
                                .name
                                .0
                                .iter()
                                .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
                                .collect::<Vec<_>>();
                            let table = match name.as_slice() {
                                [n] => TableName::new(schema, n.clone()),
                                [d, n] => TableName::new(d.clone(), n.clone()),
                                _ => continue,
                            };
                            if !self.captured.contains(&table) {
                                continue;
                            }
                            out.push(self.change(
                                &table,
                                CdcOp::Delete,
                                "t",
                                None,
                                None,
                                None,
                                end,
                            ));
                        }
                    }
                }
            }
            return Ok(out);
        }
        let mut next = (*self.history).clone();
        let outcome = next.apply_ddl(schema, sql, &is_captured);
        if outcome.is_empty() {
            return Ok(out);
        }
        self.after_ddl.extend(outcome.refresh);
        self.history = Arc::new(next);
        let txn = self.txn.as_ref().map(|t| t.id.clone()).unwrap_or_default();
        let at = self.at_position(&txn);
        for t in outcome.changed {
            if !self.captured.contains(&t) {
                continue;
            }
            let Some(def) = self.history.get(&t) else {
                continue;
            };
            let schema = all_nullable(&*arrow_schema(&t, def, &self.policy.lossy)?);
            if let Some(change) = self
                .tracker
                .observe(&t.to_string(), schema, &at)
                .map_err(ConnectorError::from)?
            {
                out.push(Emitted::Event {
                    event: change.event(&self.envelope),
                    table: t.clone(),
                    key: None,
                    mark: self.next_mark(),
                });
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn change(
        &mut self,
        table: &TableName,
        op: CdcOp,
        op_text: &str,
        key: Option<Value>,
        before: Option<Value>,
        after: Option<Value>,
        end: u64,
    ) -> Emitted {
        let mark = self.next_mark();
        let txn = self
            .txn
            .as_mut()
            .expect("changes happen inside a transaction");
        let ordinal = txn.ordinal;
        txn.ordinal += 1;
        let mut mark = mark;
        if let Some(p) = &mut mark.partial {
            p.delivered = ordinal + 1;
        }
        let id = format!("{}/{ordinal}", txn.id);
        let data = json!({
            "table": table.to_string(),
            "op": op_text,
            "key": key.clone().unwrap_or(Value::Null),
            "before": before.unwrap_or(Value::Null),
            "after": after.unwrap_or(Value::Null),
        });
        let mut event = self
            .envelope
            .event("change", &table.to_string(), id)
            .with_subject(table.to_string())
            .with_data("application/json", data.to_string())
            .with_cdc(op, loamslsn(&self.file, end, ordinal));
        if op_text == "t" {
            event = event.with_extension("loamsop", "t");
        }
        let key = key.and_then(|k| match k {
            Value::Object(m) => Some(m.into_iter().map(|(_, v)| v).collect()),
            _ => None,
        });
        Emitted::Event {
            event,
            table: table.clone(),
            key,
            mark,
        }
    }

    fn rows(&mut self, rows: &RowsEventData<'_>, end: u64) -> Result<Vec<Emitted>, FeedError> {
        let Some(tme) = self.maps.get(&rows.table_id()).cloned() else {
            return Err(FeedError::Error(ConnectorError::fatal(
                "a rows event without its TABLE_MAP",
            )));
        };
        let table = TableName::new(tme.database_name(), tme.table_name());
        let is_watermark = table == self.watermark;
        if !is_watermark && !self.captured.contains(&table) {
            return Ok(Vec::new());
        }
        let Some(def) = self.history.get(&table).cloned() else {
            return Err(FeedError::Refresh(vec![table]));
        };
        let n = usize::try_from(rows.num_columns()).unwrap_or(usize::MAX);
        let full_before = rows
            .columns_before_image()
            .is_none_or(|b| b.iter().take(n).all(|x| *x));
        let full_after = rows
            .columns_after_image()
            .is_none_or(|b| b.iter().take(n).all(|x| *x));
        if !full_before || !full_after {
            return Err(FeedError::Error(ConnectorError::fatal(format!(
                "{table}: the binlog holds partial row images; set binlog_row_image = FULL"
            ))));
        }
        let (op, op_text) = match rows {
            RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => {
                (CdcOp::Create, "c")
            }
            RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
                (CdcOp::Delete, "d")
            }
            _ => (CdcOp::Update, "u"),
        };
        let mut out = Vec::new();
        let mut columns: Option<Vec<ColumnDef>> = None;
        for r in rows.rows(&tme) {
            let (before, after) = r.map_err(|e| {
                FeedError::Error(ConnectorError::fatal(format!("{table}: rows event: {e}")))
            })?;
            if columns.is_none() {
                let sample = after.as_ref().or(before.as_ref());
                columns = Some(self.columns_for(&table, &def, &tme, sample, n)?);
            }
            let cols = columns.as_ref().expect("set above");
            let obj = |row: Option<BinlogRow>| -> Result<Option<Map<String, Value>>, String> {
                let Some(mut row) = row else {
                    return Ok(None);
                };
                let mut m = Map::new();
                for (i, c) in cols.iter().enumerate() {
                    let v = match row.take(i) {
                        Some(v) => binlog_to_json(v, c)?,
                        None => Value::Null,
                    };
                    m.insert(c.name.clone(), v);
                }
                Ok(Some(m))
            };
            let decoded = obj(before).and_then(|b| obj(after).map(|a| (b, a)));
            let (before, after) = match decoded {
                Ok(x) => x,
                Err(message) => {
                    if !is_watermark {
                        // The ordinal is spent, so ids after it stay stable.
                        if let Some(t) = &mut self.txn {
                            t.ordinal += 1;
                        }
                        let mark = self.next_mark();
                        out.push(Emitted::Poison {
                            message: format!("{table}: {message}"),
                            mark,
                        });
                    }
                    continue;
                }
            };
            if is_watermark {
                let row = after.as_ref().or(before.as_ref());
                let owner = row.and_then(|r| r.get("instance")).and_then(Value::as_str);
                let value = row.and_then(|r| r.get("mark")).and_then(Value::as_str);
                if let (Some(owner), Some(value)) = (owner, value)
                    && owner == self.watermark_owner
                    && after.is_some()
                {
                    out.push(Emitted::Watermark {
                        value: value.to_owned(),
                        mark: self.next_mark(),
                    });
                }
                continue;
            }
            let key_src = after.as_ref().or(before.as_ref());
            let key = (!def.key.is_empty()).then(|| {
                let mut k = Map::new();
                for name in &def.key {
                    let v = key_src
                        .and_then(|r| {
                            r.iter()
                                .find(|(c, _)| c.eq_ignore_ascii_case(name))
                                .map(|(_, v)| v.clone())
                        })
                        .unwrap_or(Value::Null);
                    k.insert(name.clone(), v);
                }
                Value::Object(k)
            });
            let skip = self.txn.as_ref().is_some_and(|t| t.ordinal < t.skip);
            let e = self.change(
                &table,
                op,
                op_text,
                key,
                before.map(Value::Object),
                after.map(Value::Object),
                end,
            );
            if !skip {
                out.push(e);
            }
        }
        // Skipped changes of a resumed transaction are not re-delivered.
        Ok(out)
    }

    /// The columns of a rows event: by name (FULL metadata) or by position
    /// (the history), checked against the binlog's types.
    fn columns_for(
        &self,
        table: &TableName,
        def: &TableDef,
        tme: &TableMapEvent<'static>,
        sample: Option<&BinlogRow>,
        n: usize,
    ) -> Result<Vec<ColumnDef>, FeedError> {
        let names: Vec<String> = sample
            .map(|r| {
                r.columns_ref()
                    .iter()
                    .map(|c| c.name_str().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        let named = !names.is_empty() && names.iter().all(|n| !n.starts_with('@'));
        let cols: Vec<ColumnDef> = if named {
            let mut v = Vec::with_capacity(n);
            for name in &names {
                match def.column(name) {
                    Some(c) => v.push(c.clone()),
                    None => return Err(FeedError::Refresh(vec![table.clone()])),
                }
            }
            v
        } else {
            if def.columns.len() != n {
                return Err(FeedError::Refresh(vec![table.clone()]));
            }
            def.columns.clone()
        };
        for (i, c) in cols.iter().enumerate() {
            if let Ok(Some(bt)) = tme.get_column_type(i)
                && !compatible(bt, &c.kind())
            {
                return Err(FeedError::Refresh(vec![table.clone()]));
            }
        }
        Ok(cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binlog_types_match_kinds() {
        use ColumnType::*;
        assert!(compatible(
            MYSQL_TYPE_LONGLONG,
            &Kind::Int {
                unsigned: false,
                bits: 64
            }
        ));
        assert!(compatible(MYSQL_TYPE_VARCHAR, &Kind::Text { max: Some(3) }));
        assert!(compatible(MYSQL_TYPE_STRING, &Kind::Enum(vec![])));
        assert!(compatible(MYSQL_TYPE_DATETIME2, &Kind::DateTime));
        assert!(!compatible(
            MYSQL_TYPE_VARCHAR,
            &Kind::Int {
                unsigned: false,
                bits: 64
            }
        ));
        assert!(!compatible(MYSQL_TYPE_LONG, &Kind::Text { max: None }));
    }

    #[test]
    fn lsns_sort_as_text() {
        let a = loamslsn("binlog.000002", 900, 3);
        let b = loamslsn("binlog.000002", 1200, 0);
        let c = loamslsn("binlog.000010", 4, 0);
        assert!(a < b && b < c);
    }

    #[test]
    fn captured_tables() {
        let c = Captured::Database {
            database: "shop".into(),
            except: [TableName::new("shop", "_loams_watermarks")].into(),
        };
        assert!(c.contains(&TableName::new("shop", "orders")));
        assert!(!c.contains(&TableName::new("shop", "_loams_watermarks")));
        assert!(!c.contains(&TableName::new("crm", "orders")));
    }

    use crate::gtid::GtidSet;
    use crate::history::TableDef;
    use crate::synth::*;
    use loams_connector_sdk::InstanceKey;
    use loams_connector_sdk::schema::Lossy;

    fn table() -> TableName {
        TableName::new("d", "t")
    }

    fn history() -> Arc<History> {
        let mut h = History::default();
        h.set(
            &table(),
            TableDef {
                columns: vec![
                    ColumnDef::new("id", "bigint", false),
                    ColumnDef::new("v", "varchar(40)", true),
                ],
                key: vec!["id".into()],
            },
        );
        h.set(
            &TableName::new("d", "_loams_watermarks"),
            TableDef {
                columns: vec![
                    ColumnDef::new("instance", "varchar(255)", false),
                    ColumnDef::new("mark", "varchar(64)", false),
                ],
                key: vec!["instance".into()],
            },
        );
        Arc::new(h)
    }

    fn decoder(pos: BinlogPos, partial: Option<Partial>) -> Decoder {
        let inst = InstanceKey::new(1, "i");
        let policy = SchemaPolicy {
            evolution: loams_connector_sdk::schema::Evolution::Additive,
            lossy: Lossy::none(),
        };
        let mut d = Decoder::new(
            EnvelopeBuilder::new("mysql", &inst),
            &policy,
            Captured::Tables([table()].into()),
            TableName::new("d", "_loams_watermarks"),
            "1/i".into(),
            pos,
            partial,
            history(),
        );
        d.seed(&table()).expect("seed");
        d
    }

    fn mysql_pos() -> BinlogPos {
        BinlogPos::Mysql {
            executed: GtidSet::default(),
            file: "binlog.000001".into(),
            pos: 4,
        }
    }

    fn feed_all(d: &mut Decoder, events: &[RawEvent]) -> Vec<Emitted> {
        events
            .iter()
            .flat_map(|e| match d.feed(e) {
                Ok(v) => v,
                Err(FeedError::Refresh(t)) => panic!("refresh {t:?}"),
                Err(FeedError::Error(e)) => panic!("{e}"),
            })
            .collect()
    }

    fn changes(out: &[Emitted]) -> Vec<(CloudEvent, Mark)> {
        out.iter()
            .filter_map(|e| match e {
                Emitted::Event { event, mark, .. } if event.ty.ends_with(".change.v1") => {
                    Some((event.clone(), mark.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn data(e: &CloudEvent) -> Value {
        serde_json::from_slice(&e.data).expect("json")
    }

    /// One transaction: an insert of two rows, an update and a delete.
    fn transaction(s: &mut Synth, gno: u64) -> Vec<RawEvent> {
        vec![
            s.gtid(gno),
            s.query("d", "BEGIN"),
            s.table_map(42, "d", "t", &[ID, V40], &[]),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                42,
                &[ID, V40],
                &[&[Val::Int(1), Val::Str(b"a")], &[Val::Int(2), Val::Null]],
            ),
            s.rows(
                EventType::UPDATE_ROWS_EVENT,
                42,
                &[ID, V40],
                &[&[Val::Int(2), Val::Null], &[Val::Int(2), Val::Str(b"b")]],
            ),
            s.rows(
                EventType::DELETE_ROWS_EVENT,
                42,
                &[ID, V40],
                &[&[Val::Int(1), Val::Str(b"a")]],
            ),
            s.xid(),
        ]
    }

    #[test]
    fn changes_carry_ids_ops_values_and_the_gtid_position() {
        let mut s = Synth::new();
        let rotate = s.rotate("binlog.000007");
        let events = transaction(&mut s, 7);
        let mut d = decoder(mysql_pos(), None);
        let mut all = feed_all(&mut d, &[rotate]);
        all.extend(feed_all(&mut d, &events));
        let ch = changes(&all);
        let ids: Vec<&str> = ch.iter().map(|(e, _)| e.id.as_str()).collect();
        assert_eq!(
            ids,
            [0, 1, 2, 3]
                .map(|i| format!("{UUID_TEXT}:7/{i}"))
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        let ops: Vec<&str> = ch
            .iter()
            .map(|(e, _)| e.extensions["loamsop"].as_str())
            .collect();
        assert_eq!(ops, ["c", "c", "u", "d"]);
        assert_eq!(ch[0].0.ty, "io.loams.dev.flow.mysql.change.v1");
        assert_eq!(ch[0].0.subject.as_deref(), Some("d.t"));
        assert_eq!(
            data(&ch[0].0),
            json!({"table": "d.t", "op": "c", "key": {"id": 1}, "before": null, "after": {"id": 1, "v": "a"}})
        );
        assert_eq!(data(&ch[1].0)["after"], json!({"id": 2, "v": null}));
        assert_eq!(data(&ch[2].0)["before"], json!({"id": 2, "v": null}));
        assert_eq!(data(&ch[2].0)["after"], json!({"id": 2, "v": "b"}));
        assert_eq!(data(&ch[3].0)["key"], json!({"id": 1}));
        assert!(
            ch[0].0.extensions["loamslsn"].starts_with("binlog.000007/"),
            "{}",
            ch[0].0.extensions["loamslsn"]
        );
        // Mid-transaction marks: the position before it, and the changes
        // delivered so far.
        assert_eq!(
            ch[1].1.partial,
            Some(Partial {
                txn: format!("{UUID_TEXT}:7"),
                delivered: 2
            })
        );
        assert!(
            matches!(&ch[1].1.binlog, BinlogPos::Mysql { executed, .. } if executed.is_empty())
        );
        // After the XID: the GTID is in the set, nothing partial.
        let end = d.position();
        assert_eq!(end.partial, None);
        let BinlogPos::Mysql { executed, file, .. } = &end.binlog else {
            panic!("flavour")
        };
        assert!(executed.contains(&UUID, None, 7));
        assert_eq!(file, "binlog.000007");
    }

    #[test]
    fn a_resumed_transaction_skips_what_was_delivered() {
        let mut s = Synth::new();
        let events = transaction(&mut s, 9);
        let mut d = decoder(
            mysql_pos(),
            Some(Partial {
                txn: format!("{UUID_TEXT}:9"),
                delivered: 2,
            }),
        );
        let ch = changes(&feed_all(&mut d, &events));
        let ids: Vec<String> = ch.iter().map(|(e, _)| e.id.clone()).collect();
        assert_eq!(
            ids,
            [format!("{UUID_TEXT}:9/2"), format!("{UUID_TEXT}:9/3")]
        );
        // A different transaction is not skipped.
        let mut d = decoder(
            mysql_pos(),
            Some(Partial {
                txn: format!("{UUID_TEXT}:8"),
                delivered: 2,
            }),
        );
        assert_eq!(changes(&feed_all(&mut d, &events)).len(), 4);
    }

    #[test]
    fn ddl_moves_the_history_and_columns_follow_it() {
        let mut s = Synth::new();
        let tv = Col(VARCHAR, &[80, 0]);
        let events = vec![
            s.gtid(1),
            s.query("d", "ALTER TABLE t ADD COLUMN note VARCHAR(20) AFTER id"),
            s.gtid(2),
            s.query("d", "BEGIN"),
            s.table_map(43, "d", "t", &[ID, tv, V40], &[]),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                43,
                &[ID, tv, V40],
                &[&[Val::Int(3), Val::Str(b"n3"), Val::Str(b"c")]],
            ),
            s.xid(),
        ];
        let mut d = decoder(mysql_pos(), None);
        let out = feed_all(&mut d, &events);
        let schema_changes: Vec<&CloudEvent> = out
            .iter()
            .filter_map(|e| match e {
                Emitted::Event { event, .. } if event.ty.ends_with(".schema-change.v1") => {
                    Some(event)
                }
                _ => None,
            })
            .collect();
        assert_eq!(schema_changes.len(), 1);
        assert!(String::from_utf8_lossy(&schema_changes[0].data).contains("\"note\""));
        let ch = changes(&out);
        assert_eq!(
            data(&ch[0].0)["after"],
            json!({"id": 3, "note": "n3", "v": "c"})
        );
        // The DDL's transaction is complete on its own (no BEGIN).
        let BinlogPos::Mysql { executed, .. } = &d.position().binlog else {
            panic!("flavour")
        };
        assert!(executed.contains(&UUID, None, 1) && executed.contains(&UUID, None, 2));
        assert_eq!(names_of(&d), ["id", "note", "v"]);
    }

    fn names_of(d: &Decoder) -> Vec<String> {
        d.history()
            .get(&table())
            .expect("t")
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    #[test]
    fn a_table_map_the_history_cannot_explain_asks_for_a_refresh() {
        let mut s = Synth::new();
        let three = [ID, V40, V40];
        let tm = s.table_map(44, "d", "t", &three, &[]);
        let row = s.rows(
            EventType::WRITE_ROWS_EVENT,
            44,
            &three,
            &[&[Val::Int(1), Val::Str(b"x"), Val::Str(b"y")]],
        );
        let g = s.gtid(3);
        let mut d = decoder(mysql_pos(), None);
        assert!(d.feed(&g).is_ok());
        assert!(d.feed(&tm).is_ok());
        match d.feed(&row) {
            Err(FeedError::Refresh(t)) => assert_eq!(t, vec![table()]),
            other => panic!("expected a refresh, got {other:?}"),
        }
        d.refreshed(
            &table(),
            Some(TableDef {
                columns: vec![
                    ColumnDef::new("id", "bigint", false),
                    ColumnDef::new("v", "varchar(40)", true),
                    ColumnDef::new("w", "varchar(40)", true),
                ],
                key: vec!["id".into()],
            }),
        );
        let out = d.feed(&row).expect("after the refresh");
        assert_eq!(
            data(&changes(&out)[0].0)["after"],
            json!({"id": 1, "v": "x", "w": "y"})
        );
        // A type the history disagrees with is a refresh too.
        let mut s = Synth::new();
        let wrong = [Col(VARCHAR, &[40, 0]), V40];
        let tm = s.table_map(45, "d", "t", &wrong, &[]);
        let row = s.rows(
            EventType::WRITE_ROWS_EVENT,
            45,
            &wrong,
            &[&[Val::Str(b"1"), Val::Str(b"x")]],
        );
        let mut d = decoder(mysql_pos(), None);
        d.feed(&s.gtid(4)).expect("gtid");
        d.feed(&tm).expect("table map");
        assert!(matches!(d.feed(&row), Err(FeedError::Refresh(_))));
    }

    #[test]
    fn full_metadata_names_the_columns() {
        let mut s = Synth::new();
        // The binlog's order is (v, id); the names say so.
        let cols = [V40, ID];
        let events = vec![
            s.gtid(5),
            s.query("d", "BEGIN"),
            s.table_map(46, "d", "t", &cols, &["v", "id"]),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                46,
                &cols,
                &[&[Val::Str(b"z"), Val::Int(9)]],
            ),
            s.xid(),
        ];
        let mut d = decoder(mysql_pos(), None);
        let ch = changes(&feed_all(&mut d, &events));
        assert_eq!(data(&ch[0].0)["after"], json!({"v": "z", "id": 9}));
        assert_eq!(data(&ch[0].0)["key"], json!({"id": 9}));
    }

    #[test]
    fn mariadb_gtid_events_drive_the_position() {
        let mut s = Synth::new();
        let events = vec![
            s.maria_gtid(5, 0, false),
            s.query("d", "BEGIN"),
            s.table_map(47, "d", "t", &[ID, V40], &[]),
            s.rows(
                EventType::WRITE_ROWS_EVENT_V1,
                47,
                &[ID, V40],
                &[&[Val::Int(1), Val::Str(b"m")]],
            ),
            s.xid(),
            // A standalone DDL group: no BEGIN, complete after its query.
            s.maria_gtid(6, 0, true),
            s.query("d", "CREATE TABLE other (id INT PRIMARY KEY)"),
        ];
        let pos = BinlogPos::Mariadb {
            state: crate::gtid::GtidList::parse("0-1-4").expect("list"),
            file: "mariadb-bin.000001".into(),
            pos: 4,
        };
        let mut d = decoder(pos, None);
        let ch = changes(&feed_all(&mut d, &events));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].0.id, "0-1-5/0");
        assert_eq!(data(&ch[0].0)["after"], json!({"id": 1, "v": "m"}));
        let BinlogPos::Mariadb { state, .. } = d.position().binlog else {
            panic!("flavour")
        };
        assert_eq!(state.to_string(), "0-1-6");
        assert!(!d.in_transaction());
    }

    #[test]
    fn gtids_off_use_file_positions() {
        let mut s = Synth::new();
        let rotate = s.rotate("binlog.000003");
        let mut events = vec![rotate, s.anonymous()];
        events.extend([
            s.query("d", "BEGIN"),
            s.table_map(48, "d", "t", &[ID, V40], &[]),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                48,
                &[ID, V40],
                &[&[Val::Int(1), Val::Str(b"f")]],
            ),
        ]);
        let xid = s.xid();
        let end = xid.log_pos;
        events.push(xid);
        let mut d = decoder(
            BinlogPos::File {
                file: "binlog.000003".into(),
                pos: 4,
            },
            None,
        );
        let ch = changes(&feed_all(&mut d, &events));
        assert!(ch[0].0.id.starts_with("binlog.000003:"), "{}", ch[0].0.id);
        assert_eq!(
            d.position().binlog,
            BinlogPos::File {
                file: "binlog.000003".into(),
                pos: end
            }
        );
    }

    #[test]
    fn watermark_rows_are_marks_not_changes() {
        let mut s = Synth::new();
        let events = vec![
            s.gtid(6),
            s.query("d", "BEGIN"),
            s.table_map(50, "d", "_loams_watermarks", &WM_COLS, &[]),
            s.watermark(50, b"1/i", b"L-1"),
            s.watermark(50, b"9/other", b"L-2"),
            s.xid(),
        ];
        let mut d = decoder(mysql_pos(), None);
        let out = feed_all(&mut d, &events);
        let marks: Vec<&str> = out
            .iter()
            .filter_map(|e| match e {
                Emitted::Watermark { value, .. } => Some(value.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(marks, ["L-1"]);
        assert!(changes(&out).is_empty());
    }

    /// DDL the history does not model asks for a re-read after the event,
    /// which then announces the table's new schema.
    #[test]
    fn unmodelled_ddl_is_refreshed_after_the_event() {
        let mut s = Synth::new();
        let events = vec![s.gtid(9), s.query("d", "ALTER TABLE t FROBNICATE wildly")];
        let mut d = decoder(mysql_pos(), None);
        assert!(feed_all(&mut d, &events).is_empty());
        assert!(!d.in_transaction(), "the DDL completed its transaction");
        assert_eq!(d.take_refresh(), vec![table()]);
        assert!(d.take_refresh().is_empty());
        let def = TableDef {
            columns: vec![
                ColumnDef::new("id", "bigint", false),
                ColumnDef::new("v", "varchar(40)", true),
                ColumnDef::new("extra", "int", true),
            ],
            key: vec!["id".into()],
        };
        let e = d.apply_refresh(&table(), Some(def)).expect("refresh");
        assert!(matches!(
            e,
            Some(Emitted::Event { ref event, .. })
                if String::from_utf8_lossy(&event.data).contains("\"extra\"")
        ));
        assert_eq!(names_of(&d), ["id", "v", "extra"]);
    }

    #[test]
    fn an_undecodable_row_is_poison_and_spends_its_ordinal() {
        let mut s = Synth::new();
        let events = vec![
            s.gtid(7),
            s.query("d", "BEGIN"),
            s.table_map(51, "d", "t", &[ID, V40], &[]),
            s.rows(
                EventType::WRITE_ROWS_EVENT,
                51,
                &[ID, V40],
                &[
                    &[Val::Int(1), Val::Str(b"\xff\xfe")],
                    &[Val::Int(2), Val::Str(b"ok")],
                ],
            ),
            s.xid(),
        ];
        let mut d = decoder(mysql_pos(), None);
        let out = feed_all(&mut d, &events);
        assert!(matches!(&out[0], Emitted::Poison { message, .. } if message.contains("UTF-8")));
        let ch = changes(&out);
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].0.id, format!("{UUID_TEXT}:7/1"));
    }

    #[test]
    fn truncate_is_a_t_change() {
        let mut s = Synth::new();
        let events = vec![s.gtid(8), s.query("d", "TRUNCATE TABLE t")];
        let mut d = decoder(mysql_pos(), None);
        let ch = changes(&feed_all(&mut d, &events));
        assert_eq!(ch.len(), 1);
        assert_eq!(ch[0].0.extensions["loamsop"], "t");
        assert!(!d.in_transaction());
    }
}
