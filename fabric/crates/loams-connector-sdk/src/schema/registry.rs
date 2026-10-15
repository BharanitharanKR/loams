//! Schema-registry hooks at Kafka boundaries (D356): Confluent's wire framing
//! for Avro and Protobuf, and the [`SchemaRegistry`] trait a registry client
//! implements. Until Q344 settles the registry, [`MemorySchemaRegistry`] is
//! the stub (CN1's registry client stub has the same shape).
//!
//! Confluent framing: a magic byte `0`, the schema id as a big-endian `u32`,
//! then (Protobuf only) the message-index path as zigzag varints, a count
//! then each index, with `[0]` shortened to a single `0` byte; then the
//! payload.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::SchemaError;
use crate::error::ConnectorError;

/// Confluent's magic byte.
pub const CONFLUENT_MAGIC: u8 = 0;

/// The schema languages a registry holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaFormat {
    Avro,
    Protobuf,
    Json,
}

/// A schema as the registry stores it: Avro JSON text, a `.proto` file or a
/// serialized `FileDescriptorSet`, or a JSON Schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: u32,
    pub format: SchemaFormat,
    pub schema: Bytes,
}

/// One framed record, unframed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfluentFrame<'a> {
    pub schema_id: u32,
    /// Protobuf only: the path of the message type within its file (`[0]` is
    /// the first top-level message). Empty for Avro and JSON.
    pub message_indexes: Vec<usize>,
    pub payload: &'a [u8],
}

/// The subject of a topic's value or key schema (TopicNameStrategy).
pub fn subject_for(topic: &str, key: bool) -> String {
    format!("{topic}-{}", if key { "key" } else { "value" })
}

/// Frame an Avro (or JSON) payload.
pub fn frame(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(CONFLUENT_MAGIC);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Frame a Protobuf payload with its message-index path.
pub fn frame_protobuf(schema_id: u32, message_indexes: &[usize], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(7 + payload.len());
    out.push(CONFLUENT_MAGIC);
    out.extend_from_slice(&schema_id.to_be_bytes());
    if message_indexes == [0] {
        out.push(0);
    } else {
        put_zigzag(&mut out, message_indexes.len() as i64);
        for i in message_indexes {
            put_zigzag(&mut out, *i as i64);
        }
    }
    out.extend_from_slice(payload);
    out
}

fn header(bytes: &[u8]) -> Result<(u32, &[u8]), SchemaError> {
    match bytes {
        [CONFLUENT_MAGIC, a, b, c, d, rest @ ..] => {
            Ok((u32::from_be_bytes([*a, *b, *c, *d]), rest))
        }
        [m, ..] if *m != CONFLUENT_MAGIC => Err(SchemaError::Invalid(format!(
            "not a Confluent-framed record: magic byte {m}"
        ))),
        _ => Err(SchemaError::Invalid(
            "a Confluent-framed record is at least 5 bytes".to_owned(),
        )),
    }
}

/// Unframe an Avro (or JSON) record.
pub fn unframe(bytes: &[u8]) -> Result<ConfluentFrame<'_>, SchemaError> {
    let (schema_id, payload) = header(bytes)?;
    Ok(ConfluentFrame {
        schema_id,
        message_indexes: Vec::new(),
        payload,
    })
}

/// Unframe a Protobuf record.
pub fn unframe_protobuf(bytes: &[u8]) -> Result<ConfluentFrame<'_>, SchemaError> {
    let (schema_id, mut rest) = header(bytes)?;
    let count = get_zigzag(&mut rest)?;
    let message_indexes = if count == 0 {
        vec![0]
    } else {
        if !(0..=64).contains(&count) {
            return Err(SchemaError::Invalid(format!(
                "a message-index path of {count} entries"
            )));
        }
        let mut v = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let i = get_zigzag(&mut rest)?;
            v.push(
                usize::try_from(i)
                    .map_err(|_| SchemaError::Invalid(format!("a negative message index {i}")))?,
            );
        }
        v
    };
    Ok(ConfluentFrame {
        schema_id,
        message_indexes,
        payload: rest,
    })
}

fn put_zigzag(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let byte = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn get_zigzag(buf: &mut &[u8]) -> Result<i64, SchemaError> {
    let mut z: u64 = 0;
    for shift in (0..64).step_by(7) {
        let Some((&byte, rest)) = buf.split_first() else {
            return Err(SchemaError::Invalid("a truncated varint".to_owned()));
        };
        *buf = rest;
        z |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(((z >> 1) as i64) ^ -((z & 1) as i64));
        }
    }
    Err(SchemaError::Invalid("a varint over 64 bits".to_owned()))
}

/// A schema registry as a connector sees it.
#[async_trait]
pub trait SchemaRegistry: Send + Sync {
    /// The schema a framed record names.
    async fn by_id(&self, id: u32) -> Result<RegisteredSchema, ConnectorError>;
    /// Register `schema` under `subject` and return its id; registering the
    /// same schema again returns the same id.
    async fn register(
        &self,
        subject: &str,
        format: SchemaFormat,
        schema: Bytes,
    ) -> Result<u32, ConnectorError>;
}

/// An in-memory registry: the stub until Q344, and the tests' registry.
#[derive(Debug, Default)]
pub struct MemorySchemaRegistry {
    state: Mutex<MemoryState>,
}

#[derive(Debug, Default)]
struct MemoryState {
    by_id: BTreeMap<u32, RegisteredSchema>,
    subjects: BTreeMap<String, Vec<u32>>,
}

impl MemorySchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// The ids registered under `subject`, oldest first.
    pub fn versions(&self, subject: &str) -> Vec<u32> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.subjects.get(subject).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl SchemaRegistry for MemorySchemaRegistry {
    async fn by_id(&self, id: u32) -> Result<RegisteredSchema, ConnectorError> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| ConnectorError::poison(format!("schema id {id} is not registered")))
    }

    async fn register(
        &self,
        subject: &str,
        format: SchemaFormat,
        schema: Bytes,
    ) -> Result<u32, ConnectorError> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(id) = s
            .by_id
            .values()
            .find(|r| r.format == format && r.schema == schema)
            .map(|r| r.id)
        {
            let versions = s.subjects.entry(subject.to_owned()).or_default();
            if !versions.contains(&id) {
                versions.push(id);
            }
            return Ok(id);
        }
        let id = s.by_id.keys().next_back().map_or(1, |k| k + 1);
        s.by_id.insert(id, RegisteredSchema { id, format, schema });
        s.subjects.entry(subject.to_owned()).or_default().push(id);
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avro_frame_roundtrip() {
        let framed = frame(0x0102_0304, b"payload");
        assert_eq!(&framed[..5], &[0, 1, 2, 3, 4]);
        let f = unframe(&framed).expect("framed");
        assert_eq!((f.schema_id, f.payload), (0x0102_0304, &b"payload"[..]));
        assert!(unframe(b"\x01\0\0\0\x01x").is_err());
        assert!(unframe(b"\0\0").is_err());
    }

    #[test]
    fn protobuf_frame_roundtrip() {
        // `[0]` is the single byte 0.
        let framed = frame_protobuf(7, &[0], b"m");
        assert_eq!(framed, [0, 0, 0, 0, 7, 0, b'm']);
        for path in [vec![0], vec![1], vec![2, 0, 300]] {
            let framed = frame_protobuf(9, &path, b"msg");
            let f = unframe_protobuf(&framed).expect("framed");
            assert_eq!(f.schema_id, 9);
            assert_eq!(f.message_indexes, path);
            assert_eq!(f.payload, b"msg");
        }
    }

    #[tokio::test]
    async fn memory_registry_dedups() {
        let r = MemorySchemaRegistry::new();
        let s = Bytes::from_static(br#"{"type":"string"}"#);
        let a = r
            .register(&subject_for("orders", false), SchemaFormat::Avro, s.clone())
            .await
            .expect("registers");
        let b = r
            .register(&subject_for("orders", false), SchemaFormat::Avro, s.clone())
            .await
            .expect("registers");
        assert_eq!(a, b);
        assert_eq!(r.by_id(a).await.expect("found").schema, s);
        assert_eq!(r.versions("orders-value"), [a]);
        assert!(r.by_id(99).await.is_err());
    }
}
