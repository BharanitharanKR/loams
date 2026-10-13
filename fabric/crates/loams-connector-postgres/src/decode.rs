//! pgoutput messages to `change` events (CN2 Task 10, design §33 §7).
//!
//! Each row change is one CloudEvent: `type`
//! `io.loams.dev.flow.<connector>.change.v1`, `source`
//! `/connectors/<instance>/<schema.table>`, `subject` the table, `id`
//! `<slot>/<commit_lsn>/<ordinal>` (stable across re-reads, so `ingest`'s
//! dedup makes the source effectively once), `loamsop` (`c`, `u`, `d`; `t`
//! for a TRUNCATE, ruling R10.5) and `loamslsn` (the commit LSN as 16 hex
//! digits, `/`, the ordinal as 10 digits, so it sorts as text). `data` is
//! JSON: `{table, op, key, before, after, unchanged}`, values from
//! pgoutput's text format ([`crate::types::text_to_json`]); `unchanged`
//! lists TOASTed columns the update did not touch (their value is not in
//! the message; `REPLICA IDENTITY FULL` does not change that, the guide
//! says how to get full rows).
//!
//! The position ([`CdcPos`]) is the end LSN of the last whole transaction
//! the Fabric has, plus, mid-transaction, how many of the next
//! transaction's changes it has: a restart re-reads that transaction from
//! the slot and skips them.
//!
//! `Relation` messages drive the evolution policy (CN2 Task 5): the
//! relation's columns are mapped to Arrow, the [`SchemaTracker`] decides,
//! an accepted change is a `schema-change` event, and a refused one stops
//! the decoder with `SchemaIncompatible` (the instance pauses) until the
//! instance is restarted under another policy.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use bytes::Bytes;
use loams_connector_sdk::schema::{NativeType, SchemaPolicy, SchemaTracker, TypeMapper};
use loams_connector_sdk::{CloudEvent, ConnectorError, EnvelopeBuilder, Position};
use pg_walstream::{ColumnInfo, LogicalReplicationMessage, LogicalReplicationParser, TupleData};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::replication::{Lsn, format_lsn};
use crate::types::{ColKind, PgTypeMapper, text_to_json, type_name};

/// Where a CDC split is. Serialised as JSON in `Position::opaque`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
pub enum CdcPos {
    /// The initial snapshot: the slot's consistent point, the table being
    /// read and the rows of it delivered. A restart cannot resume an
    /// exported snapshot (it dies with its connection), so it re-creates the
    /// slot and snapshots again (R10.3).
    Snapshot {
        consistent: Lsn,
        table: usize,
        rows: u64,
    },
    /// Streaming: `flushed` is the end LSN of the last whole transaction
    /// delivered; `partial` the transaction in progress.
    Stream {
        flushed: Lsn,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        partial: Option<Partial>,
    },
}

/// Part of a transaction delivered: its commit LSN and how many changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partial {
    pub commit: Lsn,
    pub delivered: u64,
}

impl CdcPos {
    pub fn to_position(&self) -> Position {
        let display = match self {
            CdcPos::Snapshot {
                consistent,
                table,
                rows,
            } => {
                format!(
                    "snapshot at {} table {table} row {rows}",
                    format_lsn(*consistent)
                )
            }
            CdcPos::Stream {
                flushed,
                partial: None,
            } => format!("lsn {}", format_lsn(*flushed)),
            CdcPos::Stream {
                flushed,
                partial: Some(p),
            } => format!(
                "lsn {} (+{} of {})",
                format_lsn(*flushed),
                p.delivered,
                format_lsn(p.commit)
            ),
        };
        Position::new(
            Bytes::from(serde_json::to_vec(self).unwrap_or_default()),
            display,
        )
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("not a Postgres CDC position: {e}")))
    }

    /// The LSN the server may release WAL up to.
    pub fn flush_lsn(&self) -> Option<Lsn> {
        match self {
            CdcPos::Stream { flushed, .. } => Some(*flushed),
            CdcPos::Snapshot { .. } => None,
        }
    }
}

/// The `loamslsn` of a change: sortable as text.
pub fn loamslsn(commit: Lsn, ordinal: u64) -> String {
    format!("{commit:016X}/{ordinal:010}")
}

struct Rel {
    table: String,
    columns: Vec<(String, Option<ColKind>, bool)>,
}

#[derive(Debug, Clone, Copy)]
struct Txn {
    commit: Lsn,
    ordinal: u64,
    skip: u64,
}

/// An event and the position just after it.
#[derive(Debug, Clone)]
pub struct Emitted {
    pub event: CloudEvent,
    pub after: CdcPos,
}

/// The `heartbeat` messages' prefix (`pg_logical_emit_message`).
pub const HEARTBEAT_PREFIX: &str = "loams-heartbeat";

/// See the module documentation.
pub struct Decoder {
    parser: LogicalReplicationParser,
    slot: String,
    envelope: EnvelopeBuilder,
    mapper: PgTypeMapper,
    tracker: SchemaTracker,
    relations: HashMap<u32, Rel>,
    custom_types: HashMap<u32, String>,
    flushed: Lsn,
    txn: Option<Txn>,
    resume: Option<Partial>,
    /// Streamed in-progress transactions (`streaming on`), until their
    /// commit or abort.
    streams: HashMap<u32, Vec<Bytes>>,
    stream_bytes: usize,
    stream_limit: usize,
    blocked: Option<String>,
    last_commit_time: Option<i64>,
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoder")
            .field("slot", &self.slot)
            .field("flushed", &format_lsn(self.flushed))
            .finish_non_exhaustive()
    }
}

/// Every field nullable: pgoutput's `Relation` carries no NOT NULL, so CDC
/// tracks schemas without it on both sides (snapshot and stream).
pub fn all_nullable(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<Field>>(),
    ))
}

impl Decoder {
    pub fn new(
        slot: &str,
        envelope: EnvelopeBuilder,
        policy: &SchemaPolicy,
        protocol_version: u32,
        resume: &CdcPos,
        stream_limit: usize,
    ) -> Self {
        let (flushed, partial) = match resume {
            CdcPos::Stream { flushed, partial } => (*flushed, *partial),
            CdcPos::Snapshot { consistent, .. } => (*consistent, None),
        };
        Self {
            parser: LogicalReplicationParser::with_protocol_version(protocol_version),
            slot: slot.to_owned(),
            envelope,
            mapper: PgTypeMapper::new(policy.lossy.clone()),
            tracker: SchemaTracker::new(policy.evolution),
            relations: HashMap::new(),
            custom_types: HashMap::new(),
            flushed,
            txn: None,
            resume: partial,
            streams: HashMap::new(),
            stream_bytes: 0,
            stream_limit,
            blocked: None,
            last_commit_time: None,
        }
    }

    /// The commit time of the last transaction delivered (microseconds
    /// since 2000-01-01), for the slot's lag in seconds.
    pub fn last_commit_time(&self) -> Option<i64> {
        self.last_commit_time
    }

    /// Seed a table's schema (from the snapshot or the catalog), so the
    /// first `Relation` is compared against it rather than taken as new.
    pub fn seed(&mut self, table: &str, schema: &Schema) {
        self.tracker.seed(table, all_nullable(schema));
    }

    /// Names of types without a fixed OID (pgvector's `vector`).
    pub fn learn_type(&mut self, oid: u32, name: &str) {
        self.custom_types.insert(oid, name.to_owned());
    }

    /// The resumable position after everything fed so far.
    pub fn position(&self) -> CdcPos {
        CdcPos::Stream {
            flushed: self.flushed,
            partial: self.txn.map(|t| Partial {
                commit: t.commit,
                delivered: t.ordinal.max(t.skip),
            }),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some() || !self.streams.is_empty()
    }

    /// A refused schema change, still standing.
    pub fn blocked(&self) -> Option<ConnectorError> {
        self.blocked
            .as_ref()
            .map(|m| ConnectorError::schema_incompatible(m.clone()))
    }

    /// A keepalive: the server has sent everything before `wal_end`. Outside
    /// a transaction the position may move there (nothing of ours is before
    /// it unsent). `true` when it moved.
    pub fn keepalive(&mut self, wal_end: Lsn) -> bool {
        if self.in_transaction() || wal_end <= self.flushed {
            return false;
        }
        self.flushed = wal_end;
        true
    }

    /// Feed one pgoutput message (one XLogData body).
    pub fn feed(&mut self, body: Bytes) -> Result<Vec<Emitted>, ConnectorError> {
        if let Some(e) = self.blocked() {
            return Err(e);
        }
        let msg = self
            .parser
            .parse_wal_message_bytes(body.clone())
            .map_err(|e| ConnectorError::fatal(format!("pgoutput: {e}")))?;
        // Inside a streamed transaction's chunk, data messages are buffered
        // until the transaction commits (or dropped if it aborts).
        if msg.is_streaming
            && let Some(xid) = msg.xid
            && !matches!(
                msg.message,
                LogicalReplicationMessage::StreamStart { .. }
                    | LogicalReplicationMessage::StreamStop
                    | LogicalReplicationMessage::StreamCommit { .. }
                    | LogicalReplicationMessage::StreamAbort { .. }
            )
        {
            // Relations and types are session state: apply them now too.
            match &msg.message {
                LogicalReplicationMessage::Relation { .. }
                | LogicalReplicationMessage::Type { .. } => {
                    return self.apply(msg.message);
                }
                _ => {}
            }
            self.stream_bytes += body.len();
            if self.stream_bytes > self.stream_limit {
                return Err(ConnectorError::fatal(format!(
                    "a streamed transaction is larger than stream_buffer_bytes ({}); \
                     set streaming: false to let the server spill it instead",
                    self.stream_limit
                )));
            }
            self.streams.entry(xid).or_default().push(body);
            return Ok(Vec::new());
        }
        self.apply(msg.message)
    }

    fn apply(&mut self, msg: LogicalReplicationMessage) -> Result<Vec<Emitted>, ConnectorError> {
        use LogicalReplicationMessage as M;
        let mut out = Vec::new();
        match msg {
            M::Begin { final_lsn, .. } => self.begin(final_lsn),
            M::Commit {
                end_lsn, timestamp, ..
            } => {
                self.last_commit_time = Some(timestamp);
                self.commit(end_lsn);
            }
            M::Type {
                type_id, type_name, ..
            } => {
                self.custom_types.insert(type_id, type_name);
            }
            M::Relation {
                relation_id,
                namespace,
                relation_name,
                columns,
                ..
            } => {
                let table = format!("{namespace}.{relation_name}");
                if let Some(ev) = self.relation(relation_id, &table, &columns)? {
                    out.push(ev);
                }
            }
            M::Insert { relation_id, tuple } => {
                if let Some(ev) = self.change(relation_id, "c", None, Some(&tuple))? {
                    out.push(ev);
                }
            }
            M::Update {
                relation_id,
                old_tuple,
                new_tuple,
                ..
            } => {
                if let Some(ev) =
                    self.change(relation_id, "u", old_tuple.as_ref(), Some(&new_tuple))?
                {
                    out.push(ev);
                }
            }
            M::Delete {
                relation_id,
                old_tuple,
                ..
            } => {
                if let Some(ev) = self.change(relation_id, "d", Some(&old_tuple), None)? {
                    out.push(ev);
                }
            }
            M::Truncate { relation_ids, .. } => {
                for r in relation_ids {
                    if let Some(ev) = self.change(r, "t", None, None)? {
                        out.push(ev);
                    }
                }
            }
            M::Message { prefix, lsn, .. } => {
                // A heartbeat outside a transaction moves the position.
                if prefix == HEARTBEAT_PREFIX && self.txn.is_none() && lsn > self.flushed {
                    self.flushed = lsn;
                }
            }
            M::StreamCommit {
                xid,
                commit_lsn,
                end_lsn,
                ..
            } => {
                let chunks = self.streams.remove(&xid).unwrap_or_default();
                self.stream_bytes = self.streams.values().flatten().map(Bytes::len).sum();
                self.begin(commit_lsn);
                let mut parser = LogicalReplicationParser::with_protocol_version(1);
                for c in chunks {
                    let m = parser
                        .parse_wal_message_bytes(strip_xid(&c))
                        .map_err(|e| ConnectorError::fatal(format!("pgoutput (streamed): {e}")))?;
                    out.extend(self.apply(m.message)?);
                }
                self.commit(end_lsn);
            }
            M::StreamAbort {
                xid,
                subtransaction_xid,
                ..
            } => {
                if xid == subtransaction_xid {
                    self.streams.remove(&xid);
                }
                // A subtransaction's abort inside a streamed transaction
                // keeps the rest: its changes were already sent under the
                // top-level xid and pgoutput does not mark them; the whole
                // transaction is kept (R10.6).
                self.stream_bytes = self.streams.values().flatten().map(Bytes::len).sum();
            }
            M::StreamStart { .. } | M::StreamStop | M::Origin { .. } => {}
            other => {
                return Err(ConnectorError::fatal(format!(
                    "pgoutput: two-phase message {other:?} on a slot created without two_phase"
                )));
            }
        }
        Ok(out)
    }

    fn begin(&mut self, commit: Lsn) {
        let skip = match self.resume {
            Some(p) if p.commit == commit => p.delivered,
            _ => 0,
        };
        self.resume = None;
        self.txn = Some(Txn {
            commit,
            ordinal: 0,
            skip,
        });
    }

    fn commit(&mut self, end_lsn: Lsn) {
        self.txn = None;
        if end_lsn > self.flushed {
            self.flushed = end_lsn;
        }
    }

    fn relation(
        &mut self,
        id: u32,
        table: &str,
        columns: &[ColumnInfo],
    ) -> Result<Option<Emitted>, ConnectorError> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut cols = Vec::with_capacity(columns.len());
        for c in columns {
            let ty = type_name(c.type_id, c.type_modifier, &self.custom_types);
            let native = NativeType::new(format!("{table}.{}", c.name), ty.clone(), true);
            let field = match self.mapper.to_arrow(&native) {
                Ok(f) => f,
                Err(e) => {
                    let e = ConnectorError::from(e);
                    self.blocked = Some(e.message.clone());
                    return Err(e);
                }
            };
            cols.push((c.name.to_string(), ColKind::of(&field, &ty), c.is_key()));
            fields.push(field);
        }
        self.relations.insert(
            id,
            Rel {
                table: table.to_owned(),
                columns: cols,
            },
        );
        let schema = Arc::new(Schema::new(fields));
        let at = self.position();
        match self.tracker.observe(table, schema, &at.to_position()) {
            Ok(None) => Ok(None),
            Ok(Some(change)) => Ok(Some(Emitted {
                event: change.event(&self.envelope),
                after: at,
            })),
            Err(e) => {
                let e = ConnectorError::from(e);
                self.blocked = Some(e.message.clone());
                Err(e)
            }
        }
    }

    fn change(
        &mut self,
        relation: u32,
        op: &str,
        old: Option<&TupleData>,
        new: Option<&TupleData>,
    ) -> Result<Option<Emitted>, ConnectorError> {
        let Some(txn) = self.txn.as_mut() else {
            return Err(ConnectorError::fatal(
                "pgoutput: a change outside a transaction",
            ));
        };
        txn.ordinal += 1;
        if txn.ordinal <= txn.skip {
            return Ok(None);
        }
        let (commit, ordinal) = (txn.commit, txn.ordinal);
        let rel = self.relations.get(&relation).ok_or_else(|| {
            ConnectorError::fatal(format!(
                "pgoutput: a change to relation {relation} before its Relation message"
            ))
        })?;
        let mut unchanged = Vec::new();
        let row = |t: &TupleData, unchanged: &mut Vec<String>| -> Value {
            let mut m = Map::new();
            for (i, (name, kind, _)) in rel.columns.iter().enumerate() {
                let Some(c) = t.get_column(i) else { continue };
                if c.is_unchanged() {
                    unchanged.push(name.clone());
                    continue;
                }
                let v = if c.is_null() {
                    Value::Null
                } else {
                    match c.as_str() {
                        Some(s) => text_to_json(*kind, &s),
                        None => Value::String(String::from_utf8_lossy(c.as_bytes()).into_owned()),
                    }
                };
                m.insert(name.clone(), v);
            }
            Value::Object(m)
        };
        let before = old.map(|t| row(t, &mut Vec::new()));
        let after = new.map(|t| row(t, &mut unchanged));
        let source = after.as_ref().or(before.as_ref());
        let key: Map<String, Value> = rel
            .columns
            .iter()
            .filter(|(_, _, k)| *k)
            .filter_map(|(n, _, _)| {
                source
                    .and_then(|s| s.get(n))
                    .map(|v| (n.clone(), v.clone()))
            })
            .collect();
        let data = json!({
            "table": rel.table,
            "op": op,
            "key": key,
            "before": before,
            "after": after,
            "unchanged": unchanged,
        });
        let id = format!("{}/{}/{ordinal}", self.slot, format_lsn(commit));
        let event = self
            .envelope
            .event("change", &rel.table, id)
            .with_subject(rel.table.clone())
            .with_data("application/json", data.to_string())
            .with_extension("loamsop", op)
            .with_extension("loamslsn", loamslsn(commit, ordinal));
        Ok(Some(Emitted {
            event,
            after: CdcPos::Stream {
                flushed: self.flushed,
                partial: Some(Partial {
                    commit,
                    delivered: ordinal,
                }),
            },
        }))
    }
}

/// A streamed data message carries the xid after its type byte; drop it so
/// a protocol-1 parser reads the rest.
fn strip_xid(b: &Bytes) -> Bytes {
    if b.len() < 5 {
        return b.clone();
    }
    let mut v = Vec::with_capacity(b.len() - 4);
    v.push(b[0]);
    v.extend_from_slice(&b[5..]);
    Bytes::from(v)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;
    use loams_connector_sdk::schema::Evolution;
    use pg_walstream::{
        ColumnData, StreamingReplicationMessage, encode_message_to_bytes,
        encode_streaming_message_to_bytes,
    };

    pub fn enc(m: LogicalReplicationMessage) -> Bytes {
        encode_message_to_bytes(&m, 2).freeze()
    }

    pub fn relation(id: u32, cols: &[(&str, u32, i32, bool)]) -> LogicalReplicationMessage {
        LogicalReplicationMessage::Relation {
            relation_id: id,
            namespace: "public".into(),
            relation_name: "orders".into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|(n, t, m, k)| ColumnInfo::new(u8::from(*k), (*n).to_owned(), *t, *m))
                .collect(),
        }
    }

    pub fn tuple(values: &[Option<&str>]) -> TupleData {
        TupleData::new(
            values
                .iter()
                .map(|v| match v {
                    Some(s) => ColumnData::text(s.as_bytes().to_vec()),
                    None => ColumnData::null(),
                })
                .collect(),
        )
    }

    pub fn begin(final_lsn: Lsn) -> LogicalReplicationMessage {
        LogicalReplicationMessage::Begin {
            final_lsn,
            timestamp: 0,
            xid: 7,
        }
    }

    pub fn commit(commit_lsn: Lsn, end_lsn: Lsn) -> LogicalReplicationMessage {
        LogicalReplicationMessage::Commit {
            flags: 0,
            commit_lsn,
            end_lsn,
            timestamp: 0,
        }
    }

    pub fn insert(id: &str, total: &str) -> LogicalReplicationMessage {
        LogicalReplicationMessage::Insert {
            relation_id: 1,
            tuple: tuple(&[Some(id), Some(total)]),
        }
    }

    pub const COLS: &[(&str, u32, i32, bool)] = &[("id", 20, -1, true), ("total", 1700, -1, false)];

    fn decoder(resume: CdcPos) -> Decoder {
        Decoder::new(
            "loams_1_orders",
            EnvelopeBuilder::new("postgresql", &InstanceKey::new(1, "orders")),
            &SchemaPolicy::default(),
            2,
            &resume,
            1 << 20,
        )
    }

    fn feed_all(d: &mut Decoder, msgs: Vec<LogicalReplicationMessage>) -> Vec<Emitted> {
        let mut out = Vec::new();
        for m in msgs {
            out.extend(d.feed(enc(m)).expect("feed"));
        }
        out
    }

    #[test]
    fn changes_carry_ids_ops_lsns_and_rows() {
        let mut d = decoder(CdcPos::Stream {
            flushed: 0x10,
            partial: None,
        });
        let out = feed_all(
            &mut d,
            vec![
                relation(1, COLS),
                begin(0x500),
                insert("1", "9.50"),
                LogicalReplicationMessage::Update {
                    relation_id: 1,
                    old_tuple: None,
                    new_tuple: tuple(&[Some("1"), Some("10.00")]),
                    key_type: None,
                },
                LogicalReplicationMessage::Delete {
                    relation_id: 1,
                    old_tuple: tuple(&[Some("1"), None]),
                    key_type: 'K',
                },
                commit(0x500, 0x520),
            ],
        );
        // The first Relation of a table is a schema change (a new table).
        assert_eq!(out.len(), 4);
        assert_eq!(
            out[0].event.ty,
            "io.loams.dev.flow.postgresql.schema-change.v1"
        );
        let ops: Vec<&str> = out[1..]
            .iter()
            .map(|e| e.event.extensions["loamsop"].as_str())
            .collect();
        assert_eq!(ops, ["c", "u", "d"]);
        let ins = &out[1].event;
        assert_eq!(ins.id, "loams_1_orders/0/500/1");
        assert_eq!(ins.ty, "io.loams.dev.flow.postgresql.change.v1");
        assert_eq!(ins.source, "/connectors/orders/public.orders");
        assert_eq!(ins.subject.as_deref(), Some("public.orders"));
        assert_eq!(ins.extensions["loamslsn"], "0000000000000500/0000000001");
        let data: Value = serde_json::from_slice(&ins.data).expect("json");
        assert_eq!(data["after"], json!({"id": 1, "total": "9.50"}));
        assert_eq!(data["key"], json!({"id": 1}));
        let del: Value = serde_json::from_slice(&out[3].event.data).expect("json");
        assert_eq!(del["key"], json!({"id": 1}));
        assert_eq!(del["after"], Value::Null);
        assert_eq!(
            out[2].after,
            CdcPos::Stream {
                flushed: 0x10,
                partial: Some(Partial {
                    commit: 0x500,
                    delivered: 2
                })
            }
        );
        assert_eq!(
            d.position(),
            CdcPos::Stream {
                flushed: 0x520,
                partial: None
            }
        );
    }

    #[test]
    fn a_resumed_transaction_skips_what_was_delivered() {
        let resume = CdcPos::Stream {
            flushed: 0x400,
            partial: Some(Partial {
                commit: 0x500,
                delivered: 2,
            }),
        };
        let pos = resume.to_position();
        assert_eq!(CdcPos::from_position(&pos).expect("round trip"), resume);
        assert_eq!(pos.display, "lsn 0/400 (+2 of 0/500)");
        let mut d = decoder(resume);
        let out = feed_all(
            &mut d,
            vec![
                relation(1, COLS),
                begin(0x500),
                insert("1", "1"),
                insert("2", "2"),
                insert("3", "3"),
                commit(0x500, 0x520),
            ],
        );
        let ids: Vec<&str> = out
            .iter()
            .filter(|e| e.event.ty.ends_with(".change.v1"))
            .map(|e| e.event.id.as_str())
            .collect();
        assert_eq!(ids, ["loams_1_orders/0/500/3"]);
    }

    #[test]
    fn keepalives_and_heartbeats_advance_only_between_transactions() {
        let mut d = decoder(CdcPos::Stream {
            flushed: 0x10,
            partial: None,
        });
        assert!(d.keepalive(0x40));
        assert!(!d.keepalive(0x30));
        feed_all(&mut d, vec![relation(1, COLS), begin(0x500)]);
        assert!(!d.keepalive(0x600), "never inside a transaction");
        feed_all(&mut d, vec![commit(0x500, 0x520)]);
        feed_all(
            &mut d,
            vec![LogicalReplicationMessage::Message {
                flags: 0,
                lsn: 0x700,
                prefix: HEARTBEAT_PREFIX.into(),
                content: Bytes::new(),
            }],
        );
        assert_eq!(d.position().flush_lsn(), Some(0x700));
    }

    #[test]
    fn an_added_column_evolves_and_a_type_change_pauses() {
        let mut d = decoder(CdcPos::Stream {
            flushed: 0,
            partial: None,
        });
        let seeded = Schema::new(vec![
            PgTypeMapper::default()
                .to_arrow(&NativeType::new("public.orders.id", "bigint", false))
                .expect("id"),
            PgTypeMapper::default()
                .to_arrow(&NativeType::new("public.orders.total", "numeric", true))
                .expect("total"),
        ]);
        d.seed("public.orders", &seeded);
        assert!(
            feed_all(&mut d, vec![relation(1, COLS)]).is_empty(),
            "same as the snapshot"
        );
        let added = feed_all(
            &mut d,
            vec![relation(
                1,
                &[
                    ("id", 20, -1, true),
                    ("total", 1700, -1, false),
                    ("note", 25, -1, false),
                ],
            )],
        );
        assert_eq!(added.len(), 1);
        let data: Value = serde_json::from_slice(&added[0].event.data).expect("json");
        assert_eq!(data["after"]["fields"].as_array().map(Vec::len), Some(3));
        let err = d
            .feed(enc(relation(
                1,
                &[
                    ("id", 25, -1, true),
                    ("total", 1700, -1, false),
                    ("note", 25, -1, false),
                ],
            )))
            .expect_err("bigint to text is incompatible");
        assert_eq!(
            err.class,
            loams_connector_sdk::ErrorClass::SchemaIncompatible
        );
        assert!(d.feed(enc(begin(0x1000))).is_err(), "stays blocked");
        let _ = Evolution::Additive;
    }

    #[test]
    fn streamed_transactions_wait_for_their_commit() {
        let mut d = decoder(CdcPos::Stream {
            flushed: 0,
            partial: None,
        });
        feed_all(&mut d, vec![relation(1, COLS)]);
        let s = |m: LogicalReplicationMessage| {
            encode_streaming_message_to_bytes(&StreamingReplicationMessage::new_streaming(m, 42), 2)
                .freeze()
        };
        assert!(
            d.feed(enc(LogicalReplicationMessage::StreamStart {
                xid: 42,
                first_segment: true
            }))
            .expect("start")
            .is_empty()
        );
        assert!(d.feed(s(insert("1", "1"))).expect("buffered").is_empty());
        assert!(d.feed(s(insert("2", "2"))).expect("buffered").is_empty());
        assert!(d.in_transaction());
        d.feed(enc(LogicalReplicationMessage::StreamStop))
            .expect("stop");
        let out = d
            .feed(enc(LogicalReplicationMessage::StreamCommit {
                xid: 42,
                flags: 0,
                commit_lsn: 0x900,
                end_lsn: 0x910,
                timestamp: 0,
            }))
            .expect("commit");
        let ids: Vec<&str> = out.iter().map(|e| e.event.id.as_str()).collect();
        assert_eq!(ids, ["loams_1_orders/0/900/1", "loams_1_orders/0/900/2"]);
        assert_eq!(d.position().flush_lsn(), Some(0x910));
        // An aborted streamed transaction emits nothing.
        d.feed(enc(LogicalReplicationMessage::StreamStart {
            xid: 43,
            first_segment: true,
        }))
        .expect("start");
        let s43 = encode_streaming_message_to_bytes(
            &StreamingReplicationMessage::new_streaming(insert("3", "3"), 43),
            2,
        )
        .freeze();
        d.feed(s43).expect("buffered");
        d.feed(enc(LogicalReplicationMessage::StreamAbort {
            xid: 43,
            subtransaction_xid: 43,
            abort_lsn: None,
            abort_timestamp: None,
        }))
        .expect("abort");
        assert!(!d.in_transaction());
    }
}
