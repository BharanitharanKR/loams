//! The byte form of a [`GraphRecord`]: version 1 (GR1 Task 10; design §48 §6.4).
//!
//! A record is the value of one log record in the graph's implicit stream. It starts with the
//! version byte [`RECORD_VERSION`], as every Loams record codec does (the collection stream's is
//! `0x01 ‖ postcard`). The body is written by hand rather than through serde, for three reasons:
//!
//! * **Total decoding.** [`decode`] is fed whatever the log holds, so it answers every input with a
//!   record or an error. Nesting is bounded ([`MAX_VALUE_DEPTH`]) so hostile bytes cannot overflow
//!   the stack, and no count is believed before the bytes behind it exist, so nothing is allocated
//!   for data that is not there. A serde decoder recurses without a bound.
//! * **One encoding per record.** Varints must be minimal, booleans and option tags are 0 or 1, and
//!   map keys are strictly increasing, so whatever [`decode`] accepts is exactly what [`encode`]
//!   writes for the record it returns. Equal records are equal bytes.
//! * **Errors that say where.** Each refusal carries the byte offset and what is wrong there.
//!
//! The layout is specified in `conformance/graph/records/v1/README.md`, and the golden vectors
//! beside it freeze it. A reader needs neither Grafeo nor Loams to understand it (§48 §6.1 I6).
//!
//! **Values** are the typed model of `loams.graph.v1.Value` (GR1 Task 2): the same kinds with the
//! same meaning, in exact units (days since 1970-01-01, nanoseconds since midnight, microseconds
//! since the epoch, offsets in seconds within ±18 hours). The protobuf encoding itself is not used:
//! its map fields are hash maps (no stable byte order), the package is unstable until GR1 Task 39,
//! and the API answers a node-shaped map as a `Node`. Kinds Grafeo cannot hold (`uint64` above
//! `INT64`, `decimal`) do not occur, and a node or relationship is the map the engine holds.
//!
//! A record carries no checksum: the log's batches already carry a crc32c (R0.18).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use grafeo::Value;
use grafeo_common::types::{Date, Duration, PropertyKey, Time, Timestamp, ZonedDatetime};
use loams_common::meta::LinkId;
use loams_proto::loams::graph::v1::QueryLanguage;

use super::{ChangeOp, ElementKind, GraphChangeSet, GraphRecord, LinkApply, PropMap, SchemaChange};
use crate::GraphId;

/// The first byte of every graph record.
pub const RECORD_VERSION: u8 = 1;

/// How many lists, maps and paths may nest inside one property value. Grafeo's parser allows 128
/// levels of brackets in a statement, so no value a statement writes is deeper.
pub const MAX_VALUE_DEPTH: usize = 128;

/// The largest UTC offset, in seconds: 18 hours, as ISO 8601, GQL and `loams.graph.v1` allow.
const MAX_OFFSET_SECONDS: i32 = 18 * 3600;

const NANOS_PER_DAY: u64 = 86_400_000_000_000;

/// Elements reserved ahead of decoding a counted list, whatever the count says.
const PREALLOCATE: usize = 1024;

/// Record kinds (the byte after the version).
mod kind {
    pub(super) const CHANGE_SET: u8 = 1;
    pub(super) const SCHEMA: u8 = 2;
}

/// Change operation tags.
mod op {
    pub(super) const UPSERT_NODE: u8 = 1;
    pub(super) const DELETE_NODE: u8 = 2;
    pub(super) const UPSERT_EDGE: u8 = 3;
    pub(super) const DELETE_EDGE: u8 = 4;
    pub(super) const SET_PROPS: u8 = 5;
    pub(super) const NODE: u8 = 1;
    pub(super) const EDGE: u8 = 2;
}

/// Value tags.
mod tag {
    pub(super) const NULL: u8 = 0;
    pub(super) const BOOL: u8 = 1;
    pub(super) const INT64: u8 = 2;
    pub(super) const FLOAT64: u8 = 3;
    pub(super) const STRING: u8 = 4;
    pub(super) const BYTES: u8 = 5;
    pub(super) const DATE: u8 = 6;
    pub(super) const LOCAL_TIME: u8 = 7;
    pub(super) const ZONED_TIME: u8 = 8;
    pub(super) const LOCAL_DATETIME: u8 = 9;
    pub(super) const ZONED_DATETIME: u8 = 10;
    pub(super) const DURATION: u8 = 11;
    pub(super) const LIST: u8 = 12;
    pub(super) const MAP: u8 = 13;
    pub(super) const VECTOR: u8 = 14;
    pub(super) const PATH: u8 = 15;
    pub(super) const GCOUNTER: u8 = 16;
    pub(super) const ONCOUNTER: u8 = 17;
}

/// Why a record could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// No bytes at all.
    #[error("a graph record is empty")]
    Empty,
    /// A version byte this build does not read.
    #[error("graph record version {0} is not readable by this build, which reads version 1")]
    UnknownVersion(u8),
    /// The input ended inside the record, or a count promised more than the input holds.
    #[error("graph record truncated: {at} bytes are not enough")]
    Truncated {
        /// The input's length.
        at: usize,
    },
    /// Bytes that are not a version-1 record.
    #[error("malformed graph record at byte {at}: {what}")]
    Malformed {
        /// The offset of the offending item.
        at: usize,
        /// What is wrong there.
        what: String,
    },
    /// A record version 1 cannot hold. Never says what the value was.
    #[error("a graph record cannot hold {0}")]
    Unrepresentable(String),
}

// ---------------------------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------------------------

/// Encodes a record: [`RECORD_VERSION`], then the body.
///
/// # Errors
///
/// [`CodecError::Unrepresentable`] for a property value nested more than [`MAX_VALUE_DEPTH`]
/// deep, a UTC offset outside ±18 hours, or a Grafeo value kind added after 0.5.43. The writer
/// refuses nothing [`decode`] would accept.
pub fn encode(record: &GraphRecord) -> Result<Bytes, CodecError> {
    let mut out = Writer(vec![RECORD_VERSION]);
    match record {
        GraphRecord::ChangeSet(set) => {
            out.byte(kind::CHANGE_SET);
            out.header(set.graph, set.lease_epoch, set.commit_epoch, set.txn_seq);
            out.option_str(set.idempotency_key.as_deref());
            match &set.link {
                None => out.byte(0),
                Some(link) => {
                    out.byte(1);
                    out.uvar(link.link.0);
                    out.uvar(link.version);
                    out.len(link.applied_after.len());
                    for (partition, offset) in &link.applied_after {
                        out.uvar(u64::from(*partition));
                        out.uvar(*offset);
                    }
                }
            }
            out.len(set.ops.len());
            for change in &set.ops {
                out.op(change)?;
            }
        }
        GraphRecord::Schema(schema) => {
            out.byte(kind::SCHEMA);
            out.header(
                schema.graph,
                schema.lease_epoch,
                schema.commit_epoch,
                schema.txn_seq,
            );
            out.option_str(schema.idempotency_key.as_deref());
            out.byte(language_tag(schema.language));
            out.str(&schema.statement);
        }
    }
    Ok(Bytes::from(out.0))
}

/// Encodes a property map on its own: [`RECORD_VERSION`], then the map exactly as a record's
/// `UpsertNode` writes it. It is the `properties` column of a portable snapshot (GR1 Task 13), so
/// a snapshot's values are the log's values, byte for byte.
///
/// # Errors
///
/// What [`encode`] refuses for a value.
pub fn encode_props(props: &PropMap) -> Result<Bytes, CodecError> {
    let mut out = Writer(vec![RECORD_VERSION]);
    out.props(props)?;
    Ok(Bytes::from(out.0))
}

fn language_tag(language: QueryLanguage) -> u8 {
    // The `loams.graph.v1` numbers, frozen here: a new language is a new tag, never a reuse.
    match language {
        QueryLanguage::Unspecified => 0,
        QueryLanguage::Gql => 1,
        QueryLanguage::Cypher => 2,
        QueryLanguage::SqlPgq => 3,
        QueryLanguage::Gremlin => 4,
        QueryLanguage::Graphql => 5,
        QueryLanguage::Sparql => 6,
    }
}

fn language_from_tag(tag: u8) -> Option<QueryLanguage> {
    Some(match tag {
        0 => QueryLanguage::Unspecified,
        1 => QueryLanguage::Gql,
        2 => QueryLanguage::Cypher,
        3 => QueryLanguage::SqlPgq,
        4 => QueryLanguage::Gremlin,
        5 => QueryLanguage::Graphql,
        6 => QueryLanguage::Sparql,
        _ => return None,
    })
}

fn checked_offset(seconds: i32) -> Result<i32, CodecError> {
    if seconds.unsigned_abs() > MAX_OFFSET_SECONDS.unsigned_abs() {
        return Err(CodecError::Unrepresentable(
            "a UTC offset outside ±18 hours".into(),
        ));
    }
    Ok(seconds)
}

struct Writer(Vec<u8>);

impl Writer {
    fn byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    /// LEB128, minimal.
    fn uvar(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.0.push((n as u8) | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    /// Zigzag, then LEB128.
    fn svar(&mut self, n: i64) {
        self.uvar(((n << 1) ^ (n >> 63)) as u64);
    }

    fn len(&mut self, n: usize) {
        self.uvar(n as u64);
    }

    fn str(&mut self, s: &str) {
        self.len(s.len());
        self.0.extend_from_slice(s.as_bytes());
    }

    fn option_str(&mut self, s: Option<&str>) {
        match s {
            None => self.byte(0),
            Some(s) => {
                self.byte(1);
                self.str(s);
            }
        }
    }

    fn header(&mut self, graph: GraphId, lease_epoch: u64, commit_epoch: u64, txn_seq: u64) {
        self.0.extend_from_slice(&graph.0.to_bytes());
        self.uvar(lease_epoch);
        self.uvar(commit_epoch);
        self.uvar(txn_seq);
    }

    fn strs(&mut self, items: &[String]) {
        self.len(items.len());
        for item in items {
            self.str(item);
        }
    }

    fn props(&mut self, props: &PropMap) -> Result<(), CodecError> {
        self.len(props.len());
        for (key, value) in props {
            self.str(key);
            self.value(value, 0)?;
        }
        Ok(())
    }

    fn op(&mut self, change: &ChangeOp) -> Result<(), CodecError> {
        match change {
            ChangeOp::UpsertNode { id, labels, props } => {
                self.byte(op::UPSERT_NODE);
                self.uvar(*id);
                self.strs(labels);
                self.props(props)?;
            }
            ChangeOp::DeleteNode { id } => {
                self.byte(op::DELETE_NODE);
                self.uvar(*id);
            }
            ChangeOp::UpsertEdge {
                id,
                ty,
                src,
                dst,
                props,
            } => {
                self.byte(op::UPSERT_EDGE);
                self.uvar(*id);
                self.str(ty);
                self.uvar(*src);
                self.uvar(*dst);
                self.props(props)?;
            }
            ChangeOp::DeleteEdge { id } => {
                self.byte(op::DELETE_EDGE);
                self.uvar(*id);
            }
            ChangeOp::SetProps {
                id,
                kind,
                set,
                removed,
            } => {
                self.byte(op::SET_PROPS);
                self.uvar(*id);
                self.byte(match kind {
                    ElementKind::Node => op::NODE,
                    ElementKind::Edge => op::EDGE,
                });
                self.props(set)?;
                self.strs(removed);
            }
        }
        Ok(())
    }

    fn counter(&mut self, counts: &HashMap<String, u64>) {
        let sorted: BTreeMap<&str, u64> = counts.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        self.len(sorted.len());
        for (replica, n) in sorted {
            self.str(replica);
            self.uvar(n);
        }
    }

    fn values(&mut self, values: &[Value], depth: usize) -> Result<(), CodecError> {
        self.len(values.len());
        for value in values {
            self.value(value, depth)?;
        }
        Ok(())
    }

    /// One value, inside `depth` containers.
    fn value(&mut self, value: &Value, depth: usize) -> Result<(), CodecError> {
        let inner = || {
            if depth >= MAX_VALUE_DEPTH {
                Err(CodecError::Unrepresentable(format!(
                    "a value nested more than {MAX_VALUE_DEPTH} levels deep"
                )))
            } else {
                Ok(depth + 1)
            }
        };
        match value {
            Value::Null => self.byte(tag::NULL),
            Value::Bool(b) => {
                self.byte(tag::BOOL);
                self.byte(u8::from(*b));
            }
            Value::Int64(i) => {
                self.byte(tag::INT64);
                self.svar(*i);
            }
            Value::Float64(f) => {
                self.byte(tag::FLOAT64);
                self.0.extend_from_slice(&f.to_le_bytes());
            }
            Value::String(s) => {
                self.byte(tag::STRING);
                self.str(s);
            }
            Value::Bytes(bytes) => {
                self.byte(tag::BYTES);
                self.len(bytes.len());
                self.0.extend_from_slice(bytes);
            }
            Value::Date(date) => {
                self.byte(tag::DATE);
                self.svar(i64::from(date.as_days()));
            }
            Value::Time(time) => match time.offset_seconds() {
                None => {
                    self.byte(tag::LOCAL_TIME);
                    self.uvar(time.as_nanos());
                }
                Some(offset) => {
                    let offset = checked_offset(offset)?;
                    self.byte(tag::ZONED_TIME);
                    self.uvar(time.as_nanos());
                    self.svar(i64::from(offset));
                }
            },
            Value::Timestamp(ts) => {
                self.byte(tag::LOCAL_DATETIME);
                self.svar(ts.as_micros());
            }
            Value::ZonedDatetime(z) => {
                let offset = checked_offset(z.offset_seconds())?;
                self.byte(tag::ZONED_DATETIME);
                self.svar(z.as_timestamp().as_micros());
                self.svar(i64::from(offset));
            }
            Value::Duration(d) => {
                self.byte(tag::DURATION);
                self.svar(d.months());
                self.svar(d.days());
                self.svar(d.nanos());
            }
            Value::List(items) => {
                let depth = inner()?;
                self.byte(tag::LIST);
                self.values(items, depth)?;
            }
            Value::Map(entries) => {
                let depth = inner()?;
                self.byte(tag::MAP);
                self.len(entries.len());
                for (key, value) in entries.iter() {
                    self.str(key.as_str());
                    self.value(value, depth)?;
                }
            }
            Value::Vector(values) => {
                self.byte(tag::VECTOR);
                self.len(values.len());
                for x in values.iter() {
                    self.0.extend_from_slice(&x.to_le_bytes());
                }
            }
            Value::Path { nodes, edges } => {
                let depth = inner()?;
                self.byte(tag::PATH);
                self.values(nodes, depth)?;
                self.values(edges, depth)?;
            }
            Value::GCounter(counts) => {
                self.byte(tag::GCOUNTER);
                self.counter(counts);
            }
            Value::OnCounter { pos, neg } => {
                self.byte(tag::ONCOUNTER);
                self.counter(pos);
                self.counter(neg);
            }
            // `grafeo::Value` is `#[non_exhaustive]`. A kind added after 0.5.43 needs a new tag
            // (and a review of this codec when the pin moves, D759); until then the write that
            // produced it is refused rather than logged as something else.
            _ => {
                return Err(CodecError::Unrepresentable(
                    "a Grafeo value kind that version 1 does not know".into(),
                ));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------------------------

/// Decodes a record that [`encode`] wrote.
///
/// Total: every input gives a record or an error, never a panic, and nesting and allocation are
/// bounded by the input. A record it accepts re-encodes to exactly `bytes`.
///
/// # Errors
///
/// [`CodecError::Empty`], [`CodecError::UnknownVersion`], [`CodecError::Truncated`] or
/// [`CodecError::Malformed`].
pub fn decode(bytes: &[u8]) -> Result<GraphRecord, CodecError> {
    let (&version, _) = bytes.split_first().ok_or(CodecError::Empty)?;
    if version != RECORD_VERSION {
        return Err(CodecError::UnknownVersion(version));
    }
    let mut input = Reader { bytes, at: 1 };
    let kind_at = input.at;
    let record = match input.byte()? {
        kind::CHANGE_SET => {
            let (graph, lease_epoch, commit_epoch, txn_seq) = input.header()?;
            let idempotency_key = input.option_str()?;
            let link = if input.option()? {
                Some(input.link()?)
            } else {
                None
            };
            let count = input.count(1)?;
            let mut ops = Vec::with_capacity(count.min(PREALLOCATE));
            for _ in 0..count {
                ops.push(input.op()?);
            }
            GraphRecord::ChangeSet(GraphChangeSet {
                graph,
                lease_epoch,
                commit_epoch,
                txn_seq,
                idempotency_key,
                link,
                ops,
            })
        }
        kind::SCHEMA => {
            let (graph, lease_epoch, commit_epoch, txn_seq) = input.header()?;
            let idempotency_key = input.option_str()?;
            let language_at = input.at;
            let language = language_from_tag(input.byte()?).ok_or_else(|| {
                input.malformed(
                    language_at,
                    format!("unknown language {}", bytes[language_at]),
                )
            })?;
            let statement = input.str()?;
            GraphRecord::Schema(SchemaChange {
                graph,
                lease_epoch,
                commit_epoch,
                txn_seq,
                idempotency_key,
                statement,
                language,
            })
        }
        other => {
            return Err(input.malformed(kind_at, format!("unknown record kind {other}")));
        }
    };
    let rest = bytes.len() - input.at;
    if rest != 0 {
        return Err(input.malformed(input.at, format!("{rest} trailing bytes after the record")));
    }
    Ok(record)
}

/// Decodes a property map [`encode_props`] wrote. Total, as [`decode`] is.
///
/// # Errors
///
/// [`CodecError::Empty`], [`CodecError::UnknownVersion`], [`CodecError::Truncated`] or
/// [`CodecError::Malformed`].
pub fn decode_props(bytes: &[u8]) -> Result<PropMap, CodecError> {
    let (&version, _) = bytes.split_first().ok_or(CodecError::Empty)?;
    if version != RECORD_VERSION {
        return Err(CodecError::UnknownVersion(version));
    }
    let mut input = Reader { bytes, at: 1 };
    let props = input.props()?;
    let rest = bytes.len() - input.at;
    if rest != 0 {
        return Err(input.malformed(input.at, format!("{rest} trailing bytes after the map")));
    }
    Ok(props)
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn malformed(&self, at: usize, what: impl Into<String>) -> CodecError {
        CodecError::Malformed {
            at,
            what: what.into(),
        }
    }

    fn truncated(&self) -> CodecError {
        CodecError::Truncated {
            at: self.bytes.len(),
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if n > self.remaining() {
            return Err(self.truncated());
        }
        let out = &self.bytes[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }

    fn byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    /// LEB128: at most ten bytes, and minimal.
    fn uvar(&mut self) -> Result<u64, CodecError> {
        let start = self.at;
        let mut n = 0u64;
        for i in 0..10 {
            let b = self.byte()?;
            if i == 9 && b > 1 {
                return Err(self.malformed(start, "a varint overflows 64 bits"));
            }
            n |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                if b == 0 && i > 0 {
                    return Err(self.malformed(start, "a varint is not minimal"));
                }
                return Ok(n);
            }
        }
        unreachable!("the tenth byte is at most 1, so it ends the varint")
    }

    fn svar(&mut self) -> Result<i64, CodecError> {
        let n = self.uvar()?;
        Ok(((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    fn i32(&mut self) -> Result<i32, CodecError> {
        let start = self.at;
        let n = self.svar()?;
        i32::try_from(n).map_err(|_| self.malformed(start, format!("{n} does not fit 32 bits")))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let start = self.at;
        let n = self.uvar()?;
        u32::try_from(n).map_err(|_| self.malformed(start, format!("{n} does not fit 32 bits")))
    }

    /// A count of items each at least `min_size` bytes long, believed only if the input still
    /// holds that many bytes.
    fn count(&mut self, min_size: usize) -> Result<usize, CodecError> {
        let n = self.uvar()?;
        match usize::try_from(n)
            .ok()
            .and_then(|n| n.checked_mul(min_size).map(|bytes| (n, bytes)))
        {
            Some((n, bytes)) if bytes <= self.remaining() => Ok(n),
            _ => Err(self.truncated()),
        }
    }

    fn bool(&mut self) -> Result<bool, CodecError> {
        let at = self.at;
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(self.malformed(at, format!("a boolean is {other}, not 0 or 1"))),
        }
    }

    fn option(&mut self) -> Result<bool, CodecError> {
        let at = self.at;
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(self.malformed(at, format!("an option tag is {other}, not 0 or 1"))),
        }
    }

    fn str(&mut self) -> Result<String, CodecError> {
        let start = self.at;
        let n = self.count(1)?;
        let bytes = self.take(n)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| self.malformed(start, "a string is not UTF-8"))
    }

    fn option_str(&mut self) -> Result<Option<String>, CodecError> {
        Ok(if self.option()? {
            Some(self.str()?)
        } else {
            None
        })
    }

    fn strs(&mut self) -> Result<Vec<String>, CodecError> {
        let count = self.count(1)?;
        let mut out = Vec::with_capacity(count.min(PREALLOCATE));
        for _ in 0..count {
            out.push(self.str()?);
        }
        Ok(out)
    }

    /// A key that must sort strictly after `last`.
    fn key(&mut self, last: Option<&str>) -> Result<String, CodecError> {
        let start = self.at;
        let key = self.str()?;
        if last.is_some_and(|last| key.as_str() <= last) {
            return Err(self.malformed(start, "keys out of order or repeated"));
        }
        Ok(key)
    }

    fn header(&mut self) -> Result<(GraphId, u64, u64, u64), CodecError> {
        let graph = GraphId(ulid::Ulid::from_bytes(self.array()?));
        Ok((graph, self.uvar()?, self.uvar()?, self.uvar()?))
    }

    fn link(&mut self) -> Result<LinkApply, CodecError> {
        let link = LinkId(self.uvar()?);
        let version = self.uvar()?;
        let count = self.count(2)?;
        let mut applied_after = BTreeMap::new();
        for _ in 0..count {
            let start = self.at;
            let partition = self.u32()?;
            if applied_after
                .last_key_value()
                .is_some_and(|(last, _)| partition <= *last)
            {
                return Err(self.malformed(start, "partitions out of order or repeated"));
            }
            applied_after.insert(partition, self.uvar()?);
        }
        Ok(LinkApply {
            link,
            version,
            applied_after,
        })
    }

    fn props(&mut self) -> Result<PropMap, CodecError> {
        let count = self.count(2)?;
        let mut props = PropMap::new();
        for _ in 0..count {
            let key = self.key(props.last_key_value().map(|(k, _)| k.as_str()))?;
            let value = self.value(0)?;
            props.insert(key, value);
        }
        Ok(props)
    }

    fn op(&mut self) -> Result<ChangeOp, CodecError> {
        let at = self.at;
        Ok(match self.byte()? {
            op::UPSERT_NODE => ChangeOp::UpsertNode {
                id: self.uvar()?,
                labels: self.strs()?,
                props: self.props()?,
            },
            op::DELETE_NODE => ChangeOp::DeleteNode { id: self.uvar()? },
            op::UPSERT_EDGE => ChangeOp::UpsertEdge {
                id: self.uvar()?,
                ty: self.str()?,
                src: self.uvar()?,
                dst: self.uvar()?,
                props: self.props()?,
            },
            op::DELETE_EDGE => ChangeOp::DeleteEdge { id: self.uvar()? },
            op::SET_PROPS => {
                let id = self.uvar()?;
                let kind_at = self.at;
                let kind = match self.byte()? {
                    op::NODE => ElementKind::Node,
                    op::EDGE => ElementKind::Edge,
                    other => {
                        return Err(
                            self.malformed(kind_at, format!("unknown element kind {other}"))
                        );
                    }
                };
                ChangeOp::SetProps {
                    id,
                    kind,
                    set: self.props()?,
                    removed: self.strs()?,
                }
            }
            other => return Err(self.malformed(at, format!("unknown change operation {other}"))),
        })
    }

    fn offset(&mut self) -> Result<i32, CodecError> {
        let start = self.at;
        let offset = self.i32()?;
        checked_offset(offset)
            .map_err(|_| self.malformed(start, format!("a UTC offset of {offset} seconds")))
    }

    fn counter(&mut self) -> Result<Arc<HashMap<String, u64>>, CodecError> {
        let count = self.count(2)?;
        let mut sorted = BTreeMap::new();
        for _ in 0..count {
            let replica = self.key(
                sorted
                    .last_key_value()
                    .map(|(k, _): (&String, _)| k.as_str()),
            )?;
            sorted.insert(replica, self.uvar()?);
        }
        Ok(Arc::new(sorted.into_iter().collect()))
    }

    fn values(&mut self, depth: usize) -> Result<Arc<[Value]>, CodecError> {
        let count = self.count(1)?;
        let mut out = Vec::with_capacity(count.min(PREALLOCATE));
        for _ in 0..count {
            out.push(self.value(depth)?);
        }
        Ok(Arc::from(out))
    }

    /// One value, inside `depth` containers.
    fn value(&mut self, depth: usize) -> Result<Value, CodecError> {
        let at = self.at;
        let tag = self.byte()?;
        let inner = |reader: &Self| {
            if depth >= MAX_VALUE_DEPTH {
                Err(reader.malformed(
                    at,
                    format!("a value nested deeper than {MAX_VALUE_DEPTH} levels"),
                ))
            } else {
                Ok(depth + 1)
            }
        };
        Ok(match tag {
            tag::NULL => Value::Null,
            tag::BOOL => Value::Bool(self.bool()?),
            tag::INT64 => Value::Int64(self.svar()?),
            tag::FLOAT64 => Value::Float64(f64::from_le_bytes(self.array()?)),
            tag::STRING => Value::String(self.str()?.into()),
            tag::BYTES => {
                let n = self.count(1)?;
                Value::Bytes(Arc::from(self.take(n)?))
            }
            tag::DATE => Value::Date(Date::from_days(self.i32()?)),
            tag::LOCAL_TIME => Value::Time(self.time()?),
            tag::ZONED_TIME => {
                let time = self.time()?;
                Value::Time(time.with_offset(self.offset()?))
            }
            tag::LOCAL_DATETIME => Value::Timestamp(Timestamp::from_micros(self.svar()?)),
            tag::ZONED_DATETIME => {
                let utc = Timestamp::from_micros(self.svar()?);
                Value::ZonedDatetime(ZonedDatetime::from_timestamp_offset(utc, self.offset()?))
            }
            tag::DURATION => {
                Value::Duration(Duration::new(self.svar()?, self.svar()?, self.svar()?))
            }
            tag::LIST => {
                let depth = inner(self)?;
                Value::List(self.values(depth)?)
            }
            tag::MAP => {
                let depth = inner(self)?;
                let count = self.count(2)?;
                let mut entries: BTreeMap<PropertyKey, Value> = BTreeMap::new();
                for _ in 0..count {
                    let key = self.key(entries.last_key_value().map(|(k, _)| k.as_str()))?;
                    let value = self.value(depth)?;
                    entries.insert(PropertyKey::new(key.as_str()), value);
                }
                Value::Map(Arc::new(entries))
            }
            tag::VECTOR => {
                let count = self.count(4)?;
                let mut out = Vec::with_capacity(count);
                for _ in 0..count {
                    out.push(f32::from_le_bytes(self.array()?));
                }
                Value::Vector(Arc::from(out))
            }
            tag::PATH => {
                let depth = inner(self)?;
                Value::Path {
                    nodes: self.values(depth)?,
                    edges: self.values(depth)?,
                }
            }
            tag::GCOUNTER => Value::GCounter(self.counter()?),
            tag::ONCOUNTER => Value::OnCounter {
                pos: self.counter()?,
                neg: self.counter()?,
            },
            other => return Err(self.malformed(at, format!("unknown value tag {other}"))),
        })
    }

    fn time(&mut self) -> Result<Time, CodecError> {
        let start = self.at;
        let nanos = self.uvar()?;
        if nanos >= NANOS_PER_DAY {
            return Err(self.malformed(start, format!("{nanos} ns is not a time of day")));
        }
        Time::from_nanos(nanos).ok_or_else(|| self.malformed(start, "not a time of day"))
    }
}
