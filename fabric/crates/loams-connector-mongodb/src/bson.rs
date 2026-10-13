//! BSON 1.1 (<https://bsonspec.org/spec.html>): the values, an ordered
//! document, and the binary encoding the wire protocol carries.
//!
//! Every element type is decoded, including the deprecated ones (undefined,
//! DBPointer, symbol, code with scope), so a document read from a server is
//! never refused for a type Loams does not otherwise use. Decoding checks
//! every length against the input, string terminators, UTF-8 and the nesting
//! depth, so a malformed reply is an error, never a panic.

use std::cmp::Ordering;
use std::fmt;

/// A malformed BSON document, or a value that cannot be encoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("bson: {0}")]
pub struct BsonError(pub String);

fn err<T>(msg: impl Into<String>) -> Result<T, BsonError> {
    Err(BsonError(msg.into()))
}

/// How deep documents and arrays may nest (MongoDB's own limit is 100 for
/// stored documents; replies add a few levels of envelope).
pub const MAX_DEPTH: usize = 128;

/// A 12-byte ObjectId.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub [u8; 12]);

impl ObjectId {
    /// The lowercase hex form (24 digits).
    pub fn to_hex(&self) -> String {
        hex(&self.0)
    }

    pub fn parse_hex(s: &str) -> Result<Self, BsonError> {
        let bytes = unhex(s).ok_or_else(|| BsonError(format!("{s:?} is not 24 hex digits")))?;
        let arr: [u8; 12] = bytes
            .try_into()
            .map_err(|_| BsonError(format!("{s:?} is not 24 hex digits")))?;
        Ok(ObjectId(arr))
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.to_hex())
    }
}

/// The internal replication timestamp: seconds and an ordinal within the
/// second. Ordered by `(time, increment)`, as the server orders them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp {
    pub time: u32,
    pub increment: u32,
}

impl Timestamp {
    pub const fn new(time: u32, increment: u32) -> Self {
        Self { time, increment }
    }

    /// The next timestamp in the server's order.
    pub fn successor(self) -> Self {
        match self.increment.checked_add(1) {
            Some(i) => Self::new(self.time, i),
            None => Self::new(self.time.saturating_add(1), 0),
        }
    }

    fn to_u64(self) -> u64 {
        (u64::from(self.time) << 32) | u64::from(self.increment)
    }

    fn from_u64(v: u64) -> Self {
        Self::new((v >> 32) as u32, v as u32)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.time, self.increment)
    }
}

/// A BSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Bson {
    Double(f64),
    String(String),
    Document(Document),
    Array(Vec<Bson>),
    Binary {
        subtype: u8,
        bytes: Vec<u8>,
    },
    Undefined,
    ObjectId(ObjectId),
    Boolean(bool),
    /// Milliseconds since the Unix epoch, UTC.
    DateTime(i64),
    Null,
    Regex {
        pattern: String,
        options: String,
    },
    DbPointer {
        namespace: String,
        id: ObjectId,
    },
    JavaScript(String),
    Symbol(String),
    JavaScriptWithScope {
        code: String,
        scope: Document,
    },
    Int32(i32),
    Timestamp(Timestamp),
    Int64(i64),
    Decimal128(crate::decimal::Decimal128),
    MinKey,
    MaxKey,
}

impl Bson {
    /// The type alias MongoDB's `$type` uses (`double`, `string`, `objectId`…).
    pub fn type_name(&self) -> &'static str {
        match self {
            Bson::Double(_) => "double",
            Bson::String(_) => "string",
            Bson::Document(_) => "object",
            Bson::Array(_) => "array",
            Bson::Binary { .. } => "binData",
            Bson::Undefined => "undefined",
            Bson::ObjectId(_) => "objectId",
            Bson::Boolean(_) => "bool",
            Bson::DateTime(_) => "date",
            Bson::Null => "null",
            Bson::Regex { .. } => "regex",
            Bson::DbPointer { .. } => "dbPointer",
            Bson::JavaScript(_) => "javascript",
            Bson::Symbol(_) => "symbol",
            Bson::JavaScriptWithScope { .. } => "javascriptWithScope",
            Bson::Int32(_) => "int",
            Bson::Timestamp(_) => "timestamp",
            Bson::Int64(_) => "long",
            Bson::Decimal128(_) => "decimal",
            Bson::MinKey => "minKey",
            Bson::MaxKey => "maxKey",
        }
    }

    fn element_type(&self) -> u8 {
        match self {
            Bson::Double(_) => 0x01,
            Bson::String(_) => 0x02,
            Bson::Document(_) => 0x03,
            Bson::Array(_) => 0x04,
            Bson::Binary { .. } => 0x05,
            Bson::Undefined => 0x06,
            Bson::ObjectId(_) => 0x07,
            Bson::Boolean(_) => 0x08,
            Bson::DateTime(_) => 0x09,
            Bson::Null => 0x0A,
            Bson::Regex { .. } => 0x0B,
            Bson::DbPointer { .. } => 0x0C,
            Bson::JavaScript(_) => 0x0D,
            Bson::Symbol(_) => 0x0E,
            Bson::JavaScriptWithScope { .. } => 0x0F,
            Bson::Int32(_) => 0x10,
            Bson::Timestamp(_) => 0x11,
            Bson::Int64(_) => 0x12,
            Bson::Decimal128(_) => 0x13,
            Bson::MinKey => 0xFF,
            Bson::MaxKey => 0x7F,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Bson::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_document(&self) -> Option<&Document> {
        match self {
            Bson::Document(d) => Some(d),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Bson]> {
        match self {
            Bson::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Bson::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// An integral number: int32, int64, or a double with no fraction (the
    /// server answers `ok: 1.0` and counts as either).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Bson::Int32(v) => Some(i64::from(*v)),
            Bson::Int64(v) => Some(*v),
            Bson::Double(v) if v.fract() == 0.0 && v.abs() < 9.0e15 => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            Bson::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    /// The canonical type bracket of a value: the server sorts and compares
    /// across types by bracket first (numbers < strings < objects < arrays <
    /// binary < ObjectId < booleans < dates < timestamps < regexes), and the
    /// query operators `$gt`/`$lt` only match within one bracket.
    pub fn bracket(&self) -> u8 {
        match self {
            Bson::MinKey => 1,
            Bson::Undefined | Bson::Null => 2,
            Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 3,
            Bson::String(_) | Bson::Symbol(_) => 4,
            Bson::Document(_) => 5,
            Bson::Array(_) => 6,
            Bson::Binary { .. } => 7,
            Bson::ObjectId(_) => 8,
            Bson::Boolean(_) => 9,
            Bson::DateTime(_) => 10,
            Bson::Timestamp(_) => 11,
            Bson::Regex { .. } => 12,
            Bson::DbPointer { .. } => 13,
            Bson::JavaScript(_) => 14,
            Bson::JavaScriptWithScope { .. } => 15,
            Bson::MaxKey => 16,
        }
    }

    /// The server's order for values of the scalar types an `_id` usually
    /// has (numbers, strings, ObjectIds, dates, booleans, binary); `None`
    /// when the two are not comparable here (documents and arrays compare
    /// element by element on the server, which this does not reproduce).
    pub fn partial_order(&self, other: &Bson) -> Option<Ordering> {
        let (a, b) = (self.bracket(), other.bracket());
        if a != b {
            return Some(a.cmp(&b));
        }
        match (self, other) {
            (Bson::String(x) | Bson::Symbol(x), Bson::String(y) | Bson::Symbol(y)) => {
                Some(x.as_bytes().cmp(y.as_bytes()))
            }
            (Bson::ObjectId(x), Bson::ObjectId(y)) => Some(x.cmp(y)),
            (Bson::Boolean(x), Bson::Boolean(y)) => Some(x.cmp(y)),
            (Bson::DateTime(x), Bson::DateTime(y)) => Some(x.cmp(y)),
            (Bson::Timestamp(x), Bson::Timestamp(y)) => Some(x.cmp(y)),
            (
                Bson::Binary {
                    subtype: s1,
                    bytes: b1,
                },
                Bson::Binary {
                    subtype: s2,
                    bytes: b2,
                },
            ) => Some(
                b1.len()
                    .cmp(&b2.len())
                    .then(s1.cmp(s2))
                    .then_with(|| b1.cmp(b2)),
            ),
            (x, y) if a == 3 => {
                let (fx, fy) = (x.as_f64_lossy()?, y.as_f64_lossy()?);
                match (x.as_i64_exact(), y.as_i64_exact()) {
                    (Some(ix), Some(iy)) => Some(ix.cmp(&iy)),
                    _ => fx.partial_cmp(&fy),
                }
            }
            (Bson::MinKey, Bson::MinKey) | (Bson::MaxKey, Bson::MaxKey) => Some(Ordering::Equal),
            (Bson::Null | Bson::Undefined, Bson::Null | Bson::Undefined) => Some(Ordering::Equal),
            _ => None,
        }
    }

    fn as_i64_exact(&self) -> Option<i64> {
        match self {
            Bson::Int32(v) => Some(i64::from(*v)),
            Bson::Int64(v) => Some(*v),
            _ => None,
        }
    }

    fn as_f64_lossy(&self) -> Option<f64> {
        match self {
            Bson::Int32(v) => Some(f64::from(*v)),
            Bson::Int64(v) => Some(*v as f64),
            Bson::Double(v) => Some(*v),
            Bson::Decimal128(d) => d.to_string().parse().ok(),
            _ => None,
        }
    }
}

impl From<f64> for Bson {
    fn from(v: f64) -> Self {
        Bson::Double(v)
    }
}
impl From<i32> for Bson {
    fn from(v: i32) -> Self {
        Bson::Int32(v)
    }
}
impl From<i64> for Bson {
    fn from(v: i64) -> Self {
        Bson::Int64(v)
    }
}
impl From<bool> for Bson {
    fn from(v: bool) -> Self {
        Bson::Boolean(v)
    }
}
impl From<&str> for Bson {
    fn from(v: &str) -> Self {
        Bson::String(v.to_owned())
    }
}
impl From<String> for Bson {
    fn from(v: String) -> Self {
        Bson::String(v)
    }
}
impl From<Document> for Bson {
    fn from(v: Document) -> Self {
        Bson::Document(v)
    }
}
impl From<Vec<Bson>> for Bson {
    fn from(v: Vec<Bson>) -> Self {
        Bson::Array(v)
    }
}
impl From<ObjectId> for Bson {
    fn from(v: ObjectId) -> Self {
        Bson::ObjectId(v)
    }
}
impl From<Timestamp> for Bson {
    fn from(v: Timestamp) -> Self {
        Bson::Timestamp(v)
    }
}

/// An ordered BSON document. Keys keep their insertion order, which the wire
/// protocol needs: a command's name must be its first key.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Document(Vec<(String, Bson)>);

/// Build a [`Document`]: `doc! { "find": "c", "limit": 1 }`.
#[macro_export]
macro_rules! doc {
    () => { $crate::bson::Document::new() };
    ( $( $k:expr => $v:expr ),+ $(,)? ) => {{
        let mut d = $crate::bson::Document::new();
        $( d.insert($k, $v); )+
        d
    }};
}

impl Document {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Set `key`, replacing its value in place when it exists.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Bson>) {
        let key = key.into();
        let value = value.into();
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.0.push((key, value)),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Bson> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn remove(&mut self, key: &str) -> Option<Bson> {
        let i = self.0.iter().position(|(k, _)| k == key)?;
        Some(self.0.remove(i).1)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Bson::as_str)
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(Bson::as_i64)
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(Bson::as_bool)
    }

    pub fn get_document(&self, key: &str) -> Option<&Document> {
        self.get(key).and_then(Bson::as_document)
    }

    pub fn get_array(&self, key: &str) -> Option<&[Bson]> {
        self.get(key).and_then(Bson::as_array)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Bson)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(k, _)| k.as_str())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The encoded document.
    pub fn to_bytes(&self) -> Result<Vec<u8>, BsonError> {
        let mut out = Vec::with_capacity(64);
        encode_document(&mut out, self, 0)?;
        Ok(out)
    }

    /// Decode exactly one document that spans all of `bytes`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Document, BsonError> {
        let mut r = Reader { buf: bytes, pos: 0 };
        let d = r.document(0)?;
        if r.pos != bytes.len() {
            return err(format!(
                "{} trailing bytes after the document",
                bytes.len() - r.pos
            ));
        }
        Ok(d)
    }

    /// Decode one document at the start of `bytes`, returning it and its
    /// length (for document sequences).
    pub fn read_prefix(bytes: &[u8]) -> Result<(Document, usize), BsonError> {
        let mut r = Reader { buf: bytes, pos: 0 };
        let d = r.document(0)?;
        Ok((d, r.pos))
    }

    /// The encoded size, without allocating the encoding twice when the
    /// caller only needs the length.
    pub fn encoded_len(&self) -> Result<usize, BsonError> {
        self.to_bytes().map(|b| b.len())
    }
}

impl IntoIterator for Document {
    type Item = (String, Bson);
    type IntoIter = std::vec::IntoIter<(String, Bson)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<(String, Bson)> for Document {
    fn from_iter<I: IntoIterator<Item = (String, Bson)>>(iter: I) -> Self {
        let mut d = Document::new();
        for (k, v) in iter {
            d.insert(k, v);
        }
        d
    }
}

fn put_cstring(out: &mut Vec<u8>, s: &str) -> Result<(), BsonError> {
    if s.as_bytes().contains(&0) {
        return err(format!("{s:?}: a key or pattern may not contain NUL"));
    }
    out.extend_from_slice(s.as_bytes());
    out.push(0);
    Ok(())
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&((s.len() + 1) as i32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn encode_document(out: &mut Vec<u8>, d: &Document, depth: usize) -> Result<(), BsonError> {
    if depth > MAX_DEPTH {
        return err("documents nest too deeply");
    }
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    for (k, v) in &d.0 {
        encode_element(out, k, v, depth)?;
    }
    out.push(0);
    let len = out.len() - start;
    let len = i32::try_from(len).map_err(|_| BsonError("document over 2 GiB".into()))?;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

fn encode_element(out: &mut Vec<u8>, key: &str, v: &Bson, depth: usize) -> Result<(), BsonError> {
    out.push(v.element_type());
    put_cstring(out, key)?;
    match v {
        Bson::Double(f) => out.extend_from_slice(&f.to_le_bytes()),
        Bson::String(s) | Bson::JavaScript(s) | Bson::Symbol(s) => put_string(out, s),
        Bson::Document(d) => encode_document(out, d, depth + 1)?,
        Bson::Array(items) => {
            let d: Document = items
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), v.clone()))
                .collect();
            encode_document(out, &d, depth + 1)?;
        }
        Bson::Binary { subtype, bytes } => {
            out.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
            out.push(*subtype);
            out.extend_from_slice(bytes);
        }
        Bson::Undefined | Bson::Null | Bson::MinKey | Bson::MaxKey => {}
        Bson::ObjectId(o) => out.extend_from_slice(&o.0),
        Bson::Boolean(b) => out.push(u8::from(*b)),
        Bson::DateTime(ms) => out.extend_from_slice(&ms.to_le_bytes()),
        Bson::Regex { pattern, options } => {
            put_cstring(out, pattern)?;
            // The spec requires the options in alphabetical order.
            let mut o: Vec<char> = options.chars().collect();
            o.sort_unstable();
            put_cstring(out, &o.into_iter().collect::<String>())?;
        }
        Bson::DbPointer { namespace, id } => {
            put_string(out, namespace);
            out.extend_from_slice(&id.0);
        }
        Bson::JavaScriptWithScope { code, scope } => {
            let start = out.len();
            out.extend_from_slice(&[0; 4]);
            put_string(out, code);
            encode_document(out, scope, depth + 1)?;
            let len = (out.len() - start) as i32;
            out[start..start + 4].copy_from_slice(&len.to_le_bytes());
        }
        Bson::Int32(i) => out.extend_from_slice(&i.to_le_bytes()),
        Bson::Timestamp(t) => out.extend_from_slice(&t.to_u64().to_le_bytes()),
        Bson::Int64(i) => out.extend_from_slice(&i.to_le_bytes()),
        Bson::Decimal128(d) => out.extend_from_slice(&d.bytes()),
    }
    Ok(())
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], BsonError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| BsonError("truncated".into()))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, BsonError> {
        Ok(self.take(1)?[0])
    }

    fn i32(&mut self) -> Result<i32, BsonError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn i64(&mut self) -> Result<i64, BsonError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }

    fn cstring(&mut self) -> Result<String, BsonError> {
        let rest = &self.buf[self.pos..];
        let n = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| BsonError("unterminated cstring".into()))?;
        let s = std::str::from_utf8(&rest[..n])
            .map_err(|_| BsonError("a cstring is not UTF-8".into()))?
            .to_owned();
        self.pos += n + 1;
        Ok(s)
    }

    fn string(&mut self) -> Result<String, BsonError> {
        let len = self.i32()?;
        if len < 1 {
            return err(format!("string length {len}"));
        }
        let b = self.take(len as usize)?;
        if b[b.len() - 1] != 0 {
            return err("a string is not NUL-terminated");
        }
        String::from_utf8(b[..b.len() - 1].to_vec())
            .map_err(|_| BsonError("a string is not UTF-8".into()))
    }

    fn document(&mut self, depth: usize) -> Result<Document, BsonError> {
        if depth > MAX_DEPTH {
            return err("documents nest too deeply");
        }
        let start = self.pos;
        let len = self.i32()?;
        if len < 5 {
            return err(format!("document length {len}"));
        }
        let end = start
            .checked_add(len as usize)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| BsonError("document length past the input".into()))?;
        let mut d = Document::new();
        loop {
            if self.pos >= end {
                return err("document has no terminator");
            }
            let t = self.u8()?;
            if t == 0 {
                break;
            }
            let key = self.cstring()?;
            let v = self.value(t, depth)?;
            // Duplicate keys keep the last value (as the server reads them).
            d.insert(key, v);
        }
        if self.pos != end {
            return err("document length does not match its contents");
        }
        Ok(d)
    }

    fn value(&mut self, t: u8, depth: usize) -> Result<Bson, BsonError> {
        Ok(match t {
            0x01 => Bson::Double(f64::from_le_bytes(self.take(8)?.try_into().expect("8"))),
            0x02 => Bson::String(self.string()?),
            0x03 => Bson::Document(self.document(depth + 1)?),
            0x04 => Bson::Array(
                self.document(depth + 1)?
                    .into_iter()
                    .map(|(_, v)| v)
                    .collect(),
            ),
            0x05 => {
                let len = self.i32()?;
                if len < 0 {
                    return err("negative binary length");
                }
                let subtype = self.u8()?;
                let mut bytes = self.take(len as usize)?.to_vec();
                if subtype == 0x02 {
                    // The old binary subtype repeats the length inside.
                    if bytes.len() < 4 {
                        return err("binary subtype 2 without its inner length");
                    }
                    let inner = i32::from_le_bytes(bytes[..4].try_into().expect("4"));
                    if inner as usize != bytes.len() - 4 {
                        return err("binary subtype 2 inner length mismatch");
                    }
                    bytes.drain(..4);
                }
                Bson::Binary { subtype, bytes }
            }
            0x06 => Bson::Undefined,
            0x07 => Bson::ObjectId(ObjectId(self.take(12)?.try_into().expect("12"))),
            0x08 => match self.u8()? {
                0 => Bson::Boolean(false),
                1 => Bson::Boolean(true),
                b => return err(format!("boolean byte {b}")),
            },
            0x09 => Bson::DateTime(self.i64()?),
            0x0A => Bson::Null,
            0x0B => Bson::Regex {
                pattern: self.cstring()?,
                options: self.cstring()?,
            },
            0x0C => Bson::DbPointer {
                namespace: self.string()?,
                id: ObjectId(self.take(12)?.try_into().expect("12")),
            },
            0x0D => Bson::JavaScript(self.string()?),
            0x0E => Bson::Symbol(self.string()?),
            0x0F => {
                let start = self.pos;
                let len = self.i32()?;
                let code = self.string()?;
                let scope = self.document(depth + 1)?;
                if self.pos - start != len as usize {
                    return err("code with scope length mismatch");
                }
                Bson::JavaScriptWithScope { code, scope }
            }
            0x10 => Bson::Int32(self.i32()?),
            0x11 => Bson::Timestamp(Timestamp::from_u64(self.i64()? as u64)),
            0x12 => Bson::Int64(self.i64()?),
            0x13 => Bson::Decimal128(crate::decimal::Decimal128::from_bytes(
                self.take(16)?.try_into().expect("16"),
            )),
            0xFF => Bson::MinKey,
            0x7F => Bson::MaxKey,
            other => return err(format!("unknown element type 0x{other:02x}")),
        })
    }
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    s
}

/// Hex (either case) to bytes.
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decimal::Decimal128;

    #[test]
    fn spec_examples_encode_byte_for_byte() {
        // bsonspec.org's two examples.
        let hello = doc! { "hello" => "world" };
        assert_eq!(
            hello.to_bytes().expect("encode"),
            b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00"
        );
        let arr =
            doc! { "BSON" => vec![Bson::from("awesome"), Bson::Double(5.05), Bson::Int32(1986)] };
        let expected: &[u8] = b"\x31\x00\x00\x00\x04BSON\x00\x26\x00\x00\x00\x020\x00\x08\x00\x00\x00awesome\x00\x011\x00\x33\x33\x33\x33\x33\x33\x14\x40\x102\x00\xc2\x07\x00\x00\x00\x00";
        assert_eq!(arr.to_bytes().expect("encode"), expected);
        assert_eq!(Document::from_bytes(expected).expect("decode"), arr);
    }

    #[test]
    fn every_type_round_trips() {
        let d = doc! {
            "double" => 1.5,
            "string" => "s",
            "doc" => doc! { "a" => 1 },
            "array" => vec![Bson::Int32(1), Bson::Null],
            "bin" => Bson::Binary { subtype: 4, bytes: vec![1; 16] },
            "undef" => Bson::Undefined,
            "oid" => ObjectId([7; 12]),
            "bool" => true,
            "date" => Bson::DateTime(-1),
            "null" => Bson::Null,
            "regex" => Bson::Regex { pattern: "a.*".into(), options: "im".into() },
            "dbptr" => Bson::DbPointer { namespace: "db.c".into(), id: ObjectId([1; 12]) },
            "js" => Bson::JavaScript("x()".into()),
            "sym" => Bson::Symbol("y".into()),
            "jsws" => Bson::JavaScriptWithScope { code: "f()".into(), scope: doc! { "v" => 1 } },
            "int" => 42,
            "ts" => Timestamp::new(1_700_000_000, 3),
            "long" => i64::MIN,
            "dec" => Bson::Decimal128(Decimal128::parse("1.5E-3").expect("dec")),
            "min" => Bson::MinKey,
            "max" => Bson::MaxKey,
        };
        let bytes = d.to_bytes().expect("encode");
        assert_eq!(Document::from_bytes(&bytes).expect("decode"), d);
    }

    #[test]
    fn regex_options_are_sorted_on_encode() {
        let d = doc! { "r" => Bson::Regex { pattern: "x".into(), options: "xmi".into() } };
        let back = Document::from_bytes(&d.to_bytes().expect("encode")).expect("decode");
        assert_eq!(
            back.get("r"),
            Some(&Bson::Regex {
                pattern: "x".into(),
                options: "imx".into()
            })
        );
    }

    #[test]
    fn malformed_input_is_an_error_never_a_panic() {
        let good = doc! { "a" => "b", "n" => doc! { "x" => 1 } }
            .to_bytes()
            .expect("encode");
        // Every truncation and every single-byte corruption is handled.
        for n in 0..good.len() {
            let _ = Document::from_bytes(&good[..n]);
        }
        for i in 0..good.len() {
            for b in [0u8, 1, 0x7f, 0xff] {
                let mut bad = good.clone();
                bad[i] = b;
                let _ = Document::from_bytes(&bad);
            }
        }
        assert!(Document::from_bytes(&good[..good.len() - 1]).is_err());
        assert!(Document::from_bytes(b"\x05\x00\x00\x00\x01").is_err());
        // A NUL inside a key cannot be encoded.
        assert!(doc! { "a\0b" => 1 }.to_bytes().is_err());
        // Deep nesting is refused.
        let mut deep = doc! { "x" => 1 };
        for _ in 0..(MAX_DEPTH + 2) {
            deep = doc! { "x" => deep };
        }
        assert!(deep.to_bytes().is_err());
    }

    #[test]
    fn old_binary_subtype_drops_its_inner_length() {
        let bytes = b"\x14\x00\x00\x00\x05x\x00\x07\x00\x00\x00\x02\x03\x00\x00\x00abc\x00";
        let d = Document::from_bytes(bytes).expect("decode");
        assert_eq!(
            d.get("x"),
            Some(&Bson::Binary {
                subtype: 2,
                bytes: b"abc".to_vec()
            })
        );
    }

    #[test]
    fn insert_replaces_in_place_and_keeps_order() {
        let mut d = doc! { "a" => 1, "b" => 2 };
        d.insert("a", 3);
        assert_eq!(d.keys().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(d.get_i64("a"), Some(3));
        assert_eq!(doc! { "ok" => 1.0 }.get_i64("ok"), Some(1));
    }

    #[test]
    fn brackets_order_across_types() {
        let oid = Bson::ObjectId(ObjectId([0; 12]));
        assert_eq!(
            Bson::Int32(5).partial_order(&Bson::from("a")),
            Some(Ordering::Less)
        );
        assert_eq!(
            Bson::Int64(5).partial_order(&Bson::Double(4.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(Bson::from("z").partial_order(&oid), Some(Ordering::Less));
        assert_eq!(
            Bson::Int32(2).bracket(),
            Bson::Decimal128(Decimal128::parse("2").expect("d")).bracket()
        );
        assert_eq!(
            Timestamp::new(1, u32::MAX).successor(),
            Timestamp::new(2, 0)
        );
        assert!(Timestamp::new(1, 9) < Timestamp::new(2, 0));
    }

    #[test]
    fn hex_round_trips() {
        let oid = ObjectId::parse_hex("507F1F77BCF86CD799439011").expect("oid");
        assert_eq!(oid.to_hex(), "507f1f77bcf86cd799439011");
        assert!(ObjectId::parse_hex("xyz").is_err());
        assert!(ObjectId::parse_hex("507f1f77bcf86cd79943901").is_err());
    }
}
