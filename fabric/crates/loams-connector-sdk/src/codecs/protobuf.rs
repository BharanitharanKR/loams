//! Protobuf over `prost-reflect` (CN2 Task 5, OR-12): dynamic messages of a
//! type named in a user-supplied `FileDescriptorSet`, with no generated code
//! and no protoc, decoded to Arrow columns and encoded back.
//!
//! | Protobuf | Arrow |
//! |---|---|
//! | `double`, `float` | `Float64`, `Float32` |
//! | `int32`, `sint32`, `sfixed32` | `Int32` |
//! | `int64`, `sint64`, `sfixed64` | `Int64` |
//! | `uint32`, `fixed32` | `UInt32` |
//! | `uint64`, `fixed64` | `UInt64` |
//! | `bool`, `string`, `bytes` | `Boolean`, `Utf8`, `Binary` |
//! | an enum | `Int32` (the number, so unknown numbers survive) + `loams.proto.enum` |
//! | a message | `Struct` |
//! | `repeated T` | `List<T>` (non-null items) |
//! | `map<K, V>` | `Map<K, V>` (entries in key order) |
//!
//! A field is nullable exactly when it has presence (a message field, a
//! proto2 or proto3 `optional` scalar, a oneof member): an absent value is
//! null, and an explicit zero stays apart from it. Recursive message types
//! have no Arrow schema and are refused.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    ListArray, MapArray, RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
pub use prost_reflect;
use prost_reflect::prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value,
};

use super::csv::DEFAULT_BATCH_ROWS;
use super::{Codec, CodecError, CodecStats};

/// Field metadata: the full name of an enum column's type.
pub const ENUM_KEY: &str = "loams.proto.enum";

/// Protobuf messages of one type ↔ Arrow rows.
#[derive(Debug)]
pub struct ProtobufCodec {
    message: MessageDescriptor,
    schema: SchemaRef,
    batch_rows: usize,
    stats: CodecStats,
}

impl ProtobufCodec {
    /// `descriptor_set` is a serialized `google.protobuf.FileDescriptorSet`
    /// (`protoc --descriptor_set_out --include_imports`, `buf build -o`);
    /// `message` is the full name (`shop.Order`).
    pub fn new(descriptor_set: &[u8], message: &str) -> Result<Self, CodecError> {
        let pool = DescriptorPool::decode(descriptor_set)
            .map_err(|e| CodecError::Schema(format!("descriptor set: {e}")))?;
        let message = pool.get_message_by_name(message).ok_or_else(|| {
            CodecError::Schema(format!("the descriptor set has no message {message}"))
        })?;
        Self::for_message(message)
    }

    pub fn for_message(message: MessageDescriptor) -> Result<Self, CodecError> {
        let fields = message_fields(&message, &mut Vec::new())?;
        Ok(Self {
            message,
            schema: Arc::new(Schema::new(fields)),
            batch_rows: DEFAULT_BATCH_ROWS,
            stats: CodecStats::default(),
        })
    }

    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = rows.max(1);
        self
    }

    pub fn descriptor(&self) -> &MessageDescriptor {
        &self.message
    }

    /// One message per row (a Kafka record value each, unframed).
    pub fn decode_messages<B: AsRef<[u8]>>(
        &self,
        messages: &[B],
    ) -> Result<RecordBatch, CodecError> {
        let msgs = messages
            .iter()
            .map(|b| {
                DynamicMessage::decode(self.message.clone(), b.as_ref())
                    .map_err(|e| CodecError::Decode(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.batch_from(&msgs)
    }

    /// Dynamic messages of the codec's type to one batch.
    pub fn batch_from(&self, msgs: &[DynamicMessage]) -> Result<RecordBatch, CodecError> {
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for (fd, field) in self.message.fields().zip(self.schema.fields()) {
            let values = msgs.iter().map(|m| field_value(m, &fd)).collect();
            columns.push(column(&fd, field.data_type(), values)?);
        }
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(msgs.len()));
        RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
            .map_err(|e| CodecError::Decode(e.to_string()))
    }

    /// One message per row of `batch`.
    pub fn messages_from(&self, batch: &RecordBatch) -> Result<Vec<DynamicMessage>, CodecError> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(CodecError::Encode(
                "the batch's schema is not the message's".to_owned(),
            ));
        }
        (0..batch.num_rows())
            .map(|row| message_at(&self.message, batch.columns(), row))
            .collect()
    }

    /// One encoded message per row of `batch`.
    pub fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, CodecError> {
        Ok(self
            .messages_from(batch)?
            .iter()
            .map(|m| m.encode_to_vec())
            .collect())
    }
}

/// The `Codec` form reads and writes a stream of length-delimited messages
/// (a varint length before each), the usual framing of a Protobuf file.
impl Codec for ProtobufCodec {
    fn content_type(&self) -> &'static str {
        "application/x-protobuf"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, bytes: &[u8]) -> Result<Vec<RecordBatch>, CodecError> {
        let mut rest = bytes;
        let mut out = Vec::new();
        let mut msgs = Vec::new();
        while !rest.is_empty() {
            let len = prost_reflect::prost::encoding::decode_varint(&mut rest)
                .map_err(|e| CodecError::Decode(e.to_string()))?;
            let len = usize::try_from(len)
                .ok()
                .filter(|l| *l <= rest.len())
                .ok_or_else(|| CodecError::Decode("a truncated message".to_owned()))?;
            let (one, tail) = rest.split_at(len);
            rest = tail;
            msgs.push(
                DynamicMessage::decode(self.message.clone(), one)
                    .map_err(|e| CodecError::Decode(e.to_string()))?,
            );
            if msgs.len() == self.batch_rows {
                out.push(self.batch_from(&msgs)?);
                msgs.clear();
            }
        }
        if !msgs.is_empty() {
            out.push(self.batch_from(&msgs)?);
        }
        Ok(out)
    }

    fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        for b in batches {
            for m in self.messages_from(b)? {
                m.encode_length_delimited(&mut out)
                    .map_err(|e| CodecError::Encode(e.to_string()))?;
            }
        }
        Ok(out)
    }

    fn stats(&self) -> &CodecStats {
        &self.stats
    }
}

// ---------------------------------------------------------------- schema --

fn message_fields(m: &MessageDescriptor, stack: &mut Vec<String>) -> Result<Fields, CodecError> {
    if stack.iter().any(|n| n == m.full_name()) {
        return Err(CodecError::Schema(format!(
            "{} is recursive, and a recursive message has no Arrow schema",
            m.full_name()
        )));
    }
    stack.push(m.full_name().to_owned());
    let fields = m
        .fields()
        .map(|fd| field_for(&fd, stack))
        .collect::<Result<Vec<_>, _>>()?;
    stack.pop();
    Ok(fields.into())
}

fn field_for(fd: &FieldDescriptor, stack: &mut Vec<String>) -> Result<Field, CodecError> {
    let kind = fd.kind();
    if fd.is_map() {
        let Kind::Message(entry) = kind else {
            return Err(CodecError::Schema(format!(
                "{}: a map without an entry",
                fd.full_name()
            )));
        };
        let key = entry.map_entry_key_field();
        let value = entry.map_entry_value_field();
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", kind_type(&key.kind(), stack)?, false),
                    Field::new("value", kind_type(&value.kind(), stack)?, false),
                ]
                .into(),
            ),
            false,
        );
        return Ok(Field::new(
            fd.name(),
            DataType::Map(Arc::new(entries), false),
            false,
        ));
    }
    let mut data_type = kind_type(&kind, stack)?;
    let mut field = if fd.is_list() {
        let mut item = Field::new("item", data_type, false);
        if let Kind::Enum(e) = &kind {
            item = item.with_metadata([(ENUM_KEY.to_owned(), e.full_name().to_owned())].into());
        }
        data_type = DataType::List(Arc::new(item));
        Field::new(fd.name(), data_type, false)
    } else {
        Field::new(fd.name(), data_type, fd.supports_presence())
    };
    if let (Kind::Enum(e), false) = (&kind, fd.is_list()) {
        field = field.with_metadata([(ENUM_KEY.to_owned(), e.full_name().to_owned())].into());
    }
    Ok(field)
}

fn kind_type(kind: &Kind, stack: &mut Vec<String>) -> Result<DataType, CodecError> {
    Ok(match kind {
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Enum(_) => DataType::Int32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
        Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
        Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
        Kind::Bool => DataType::Boolean,
        Kind::String => DataType::Utf8,
        Kind::Bytes => DataType::Binary,
        Kind::Message(m) => DataType::Struct(message_fields(m, stack)?),
    })
}

// ---------------------------------------------------------------- decode --

/// A field's value, or `None` when it has presence and is absent.
fn field_value(m: &DynamicMessage, fd: &FieldDescriptor) -> Option<Value> {
    if fd.supports_presence() && !m.has_field(fd) {
        None
    } else {
        Some(m.get_field(fd).into_owned())
    }
}

fn nulls(values: &[Option<Value>]) -> Option<NullBuffer> {
    if values.iter().all(Option::is_some) {
        None
    } else {
        Some(
            values
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>()
                .into(),
        )
    }
}

fn column(
    fd: &FieldDescriptor,
    dt: &DataType,
    values: Vec<Option<Value>>,
) -> Result<ArrayRef, CodecError> {
    if fd.is_map() {
        map_array(fd, dt, values)
    } else if fd.is_list() {
        list_array(&fd.kind(), dt, values)
    } else {
        single(&fd.kind(), dt, values)
    }
}

fn mismatch(dt: &DataType) -> CodecError {
    CodecError::Decode(format!("a value does not fit its column of type {dt}"))
}

fn single(kind: &Kind, dt: &DataType, values: Vec<Option<Value>>) -> Result<ArrayRef, CodecError> {
    macro_rules! prim {
        ($arr:ty, $get:expr) => {{
            let get = $get;
            Arc::new(<$arr>::from(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(get))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        }};
    }
    Ok(match dt {
        DataType::Boolean => prim!(BooleanArray, Value::as_bool),
        DataType::Int32 => prim!(Int32Array, |v: &Value| v
            .as_i32()
            .or_else(|| v.as_enum_number())),
        DataType::Int64 => prim!(Int64Array, Value::as_i64),
        DataType::UInt32 => prim!(UInt32Array, Value::as_u32),
        DataType::UInt64 => prim!(UInt64Array, Value::as_u64),
        DataType::Float32 => prim!(Float32Array, Value::as_f32),
        DataType::Float64 => prim!(Float64Array, Value::as_f64),
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(Value::as_str))
                .collect::<Vec<_>>(),
        )),
        DataType::Binary => Arc::new(BinaryArray::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(Value::as_bytes).map(Bytes::as_ref))
                .collect::<Vec<_>>(),
        )),
        DataType::Struct(fields) => {
            let Kind::Message(m) = kind else {
                return Err(mismatch(dt));
            };
            let mut children = Vec::with_capacity(fields.len());
            for (child_fd, child) in m.fields().zip(fields.iter()) {
                let cv = values
                    .iter()
                    .map(|v| match v {
                        Some(Value::Message(msg)) => field_value(msg, &child_fd),
                        _ => None,
                    })
                    .collect();
                children.push(column(&child_fd, child.data_type(), cv)?);
            }
            Arc::new(
                StructArray::try_new(fields.clone(), children, nulls(&values))
                    .map_err(|e| CodecError::Decode(e.to_string()))?,
            )
        }
        _ => return Err(mismatch(dt)),
    })
}

fn offsets(lens: impl Iterator<Item = usize>) -> Result<OffsetBuffer<i32>, CodecError> {
    let mut offsets = vec![0i32];
    let mut total = 0usize;
    for l in lens {
        total += l;
        offsets.push(
            i32::try_from(total)
                .map_err(|_| CodecError::Decode("a list column over 2^31 items".to_owned()))?,
        );
    }
    Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
}

fn list_array(
    kind: &Kind,
    dt: &DataType,
    values: Vec<Option<Value>>,
) -> Result<ArrayRef, CodecError> {
    let DataType::List(item) = dt else {
        return Err(mismatch(dt));
    };
    let lens = values.iter().map(|v| match v {
        Some(Value::List(items)) => items.len(),
        _ => 0,
    });
    let offsets = offsets(lens)?;
    let flat: Vec<Option<Value>> = values
        .iter()
        .flat_map(|v| match v {
            Some(Value::List(items)) => items.clone(),
            _ => Vec::new(),
        })
        .map(Some)
        .collect();
    let child = single(kind, item.data_type(), flat)?;
    Ok(Arc::new(
        ListArray::try_new(item.clone(), offsets, child, nulls(&values))
            .map_err(|e| CodecError::Decode(e.to_string()))?,
    ))
}

fn key_value(k: &MapKey) -> Value {
    match k {
        MapKey::Bool(b) => Value::Bool(*b),
        MapKey::I32(i) => Value::I32(*i),
        MapKey::I64(i) => Value::I64(*i),
        MapKey::U32(i) => Value::U32(*i),
        MapKey::U64(i) => Value::U64(*i),
        MapKey::String(s) => Value::String(s.clone()),
    }
}

fn map_array(
    fd: &FieldDescriptor,
    dt: &DataType,
    values: Vec<Option<Value>>,
) -> Result<ArrayRef, CodecError> {
    let (DataType::Map(entries_field, sorted), Kind::Message(entry)) = (dt, fd.kind()) else {
        return Err(mismatch(dt));
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(mismatch(dt));
    };
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    let mut lens = Vec::with_capacity(values.len());
    for v in &values {
        match v {
            Some(Value::Map(m)) => {
                let mut entries: Vec<_> = m.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                lens.push(entries.len());
                for (k, v) in entries {
                    keys.push(Some(key_value(k)));
                    vals.push(Some(v.clone()));
                }
            }
            _ => lens.push(0),
        }
    }
    let key_fd = entry.map_entry_key_field();
    let value_fd = entry.map_entry_value_field();
    let k = single(&key_fd.kind(), entry_fields[0].data_type(), keys)?;
    let v = single(&value_fd.kind(), entry_fields[1].data_type(), vals)?;
    let entries = StructArray::try_new(entry_fields.clone(), vec![k, v], None)
        .map_err(|e| CodecError::Decode(e.to_string()))?;
    Ok(Arc::new(
        MapArray::try_new(
            entries_field.clone(),
            offsets(lens.into_iter())?,
            entries,
            nulls(&values),
            *sorted,
        )
        .map_err(|e| CodecError::Decode(e.to_string()))?,
    ))
}

// ---------------------------------------------------------------- encode --

fn message_at(
    desc: &MessageDescriptor,
    columns: &[ArrayRef],
    row: usize,
) -> Result<DynamicMessage, CodecError> {
    let mut m = DynamicMessage::new(desc.clone());
    for (fd, col) in desc.fields().zip(columns) {
        if col.is_null(row) {
            continue;
        }
        let v = value_at(&fd, col, row)?;
        // A proto3 scalar without presence is not set to its default, so the
        // message equals the one that was decoded.
        if !fd.supports_presence() && v.is_default_for_field(&fd) {
            continue;
        }
        m.try_set_field(&fd, v)
            .map_err(|e| CodecError::Encode(e.to_string()))?;
    }
    Ok(m)
}

fn wrong(what: &str) -> CodecError {
    CodecError::Encode(format!("a column is not the {what} its field needs"))
}

fn value_at(fd: &FieldDescriptor, col: &ArrayRef, row: usize) -> Result<Value, CodecError> {
    let kind = fd.kind();
    if fd.is_map() {
        let Kind::Message(entry) = kind else {
            return Err(wrong("map"));
        };
        let map = col.as_map_opt().ok_or_else(|| wrong("map"))?;
        let entries = map.value(row);
        let key_kind = entry.map_entry_key_field().kind();
        let value_kind = entry.map_entry_value_field().kind();
        let mut out = HashMap::with_capacity(entries.len());
        for i in 0..entries.len() {
            let k = match single_at(&key_kind, entries.column(0), i)? {
                Value::Bool(b) => MapKey::Bool(b),
                Value::I32(x) => MapKey::I32(x),
                Value::I64(x) => MapKey::I64(x),
                Value::U32(x) => MapKey::U32(x),
                Value::U64(x) => MapKey::U64(x),
                Value::String(s) => MapKey::String(s),
                _ => return Err(wrong("map key")),
            };
            out.insert(k, single_at(&value_kind, entries.column(1), i)?);
        }
        return Ok(Value::Map(out));
    }
    if fd.is_list() {
        let list = col.as_list_opt::<i32>().ok_or_else(|| wrong("list"))?;
        let items = list.value(row);
        return (0..items.len())
            .map(|i| single_at(&kind, &items, i))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List);
    }
    single_at(&kind, col, row)
}

fn single_at(kind: &Kind, arr: &ArrayRef, i: usize) -> Result<Value, CodecError> {
    macro_rules! prim {
        ($t:ty, $variant:expr, $what:literal) => {
            $variant(
                arr.as_primitive_opt::<$t>()
                    .ok_or_else(|| wrong($what))?
                    .value(i),
            )
        };
    }
    Ok(match kind {
        Kind::Double => prim!(Float64Type, Value::F64, "Float64"),
        Kind::Float => prim!(Float32Type, Value::F32, "Float32"),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => prim!(Int32Type, Value::I32, "Int32"),
        Kind::Enum(_) => prim!(Int32Type, Value::EnumNumber, "Int32"),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => prim!(Int64Type, Value::I64, "Int64"),
        Kind::Uint32 | Kind::Fixed32 => prim!(UInt32Type, Value::U32, "UInt32"),
        Kind::Uint64 | Kind::Fixed64 => prim!(UInt64Type, Value::U64, "UInt64"),
        Kind::Bool => Value::Bool(
            arr.as_boolean_opt()
                .ok_or_else(|| wrong("Boolean"))?
                .value(i),
        ),
        Kind::String => Value::String(
            arr.as_string_opt::<i32>()
                .ok_or_else(|| wrong("Utf8"))?
                .value(i)
                .to_owned(),
        ),
        Kind::Bytes => Value::Bytes(Bytes::copy_from_slice(
            arr.as_binary_opt::<i32>()
                .ok_or_else(|| wrong("Binary"))?
                .value(i),
        )),
        Kind::Message(m) => {
            let s = arr.as_struct_opt().ok_or_else(|| wrong("Struct"))?;
            Value::Message(message_at(m, s.columns(), i)?)
        }
    })
}
