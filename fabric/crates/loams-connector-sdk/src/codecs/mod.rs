//! Codecs (CN2 Task 5, D356): CSV and NDJSON over `arrow-csv` and
//! `arrow-json`, and Protobuf over `prost-reflect` with a user-supplied
//! `FileDescriptorSet`.
//!
//! Every codec decodes bytes straight to Arrow `RecordBatch`es with a known
//! schema: the **bulk path** ([`Codec::decode_bulk`]) hands those batches on
//! as [`EventBatch::Arrow`] with one envelope per batch, and never
//! materialises a row. The **row path** ([`Codec::decode_rows`]) exists for
//! consumers that need one CloudEvent per row (HTTP-API profiles); it is the
//! only path that counts [`CodecCounters::row_decodes`].

use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::envelope::CloudEvent;
use crate::error::ConnectorError;
use crate::source::EventBatch;

pub mod csv;
pub mod ndjson;
pub mod protobuf;

/// The `datacontenttype` of a bulk batch's envelope: an Arrow IPC stream
/// (CN1 Ruling 3).
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// Why bytes could not be decoded or a batch encoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// The input does not fit the schema or is not well-formed.
    #[error("decode: {0}")]
    Decode(String),
    #[error("encode: {0}")]
    Encode(String),
    /// The schema or descriptor itself cannot be used.
    #[error("schema: {0}")]
    Schema(String),
}

/// Input the codec cannot decode is `Poison` (to the DLQ, the pipeline goes
/// on); a batch it cannot encode is `SchemaIncompatible` (the target's
/// schema no longer fits); an unusable schema is `Fatal`.
impl From<CodecError> for ConnectorError {
    fn from(e: CodecError) -> Self {
        let msg = e.to_string();
        match e {
            CodecError::Decode(_) => ConnectorError::poison(msg),
            CodecError::Encode(_) => ConnectorError::schema_incompatible(msg),
            CodecError::Schema(_) => ConnectorError::fatal(msg),
        }
    }
}

// ------------------------------------------------- UTC under a fixed offset --
//
// Arrow's CSV and JSON formatters and parsers resolve a named zone only with
// the `chrono-tz` feature, which the workspace does not build; a fixed
// offset needs nothing. Loams' canonical zone is `UTC` (schema::arrow), so
// the text codecs relabel a top-level `Timestamp(_, "UTC")` column (and its
// aliases) as `+00:00` around arrow-csv and arrow-json: the values are the
// same instants, only the label changes.

fn is_utc_alias(tz: &str) -> bool {
    matches!(tz, "UTC" | "utc" | "Z" | "Etc/UTC" | "GMT" | "Etc/GMT")
}

/// `schema` with each UTC-alias timestamp relabelled `+00:00`.
pub(crate) fn offset_schema(schema: &SchemaRef) -> SchemaRef {
    use arrow_schema::{DataType, Field, Schema};
    if !schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Timestamp(_, Some(tz)) if is_utc_alias(tz)))
    {
        return schema.clone();
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(unit, Some(tz)) if is_utc_alias(tz) => f
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(*unit, Some("+00:00".into()))),
            _ => f.as_ref().clone(),
        })
        .collect();
    std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// `batch` under `schema`, which differs from the batch's own at most in the
/// timezone label of top-level timestamp columns.
pub(crate) fn relabel(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, CodecError> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    use arrow_schema::{DataType, TimeUnit};
    if batch.schema().fields() == schema.fields() {
        return Ok(batch);
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (col, field) in batch.columns().iter().zip(schema.fields()) {
        let col: arrow_array::ArrayRef = match (col.data_type(), field.data_type()) {
            (DataType::Timestamp(from, _), DataType::Timestamp(to, Some(tz))) if from == to => {
                match to {
                    TimeUnit::Second => std::sync::Arc::new(
                        col.as_primitive::<TimestampSecondType>()
                            .clone()
                            .with_timezone(tz.clone()),
                    ),
                    TimeUnit::Millisecond => std::sync::Arc::new(
                        col.as_primitive::<TimestampMillisecondType>()
                            .clone()
                            .with_timezone(tz.clone()),
                    ),
                    TimeUnit::Microsecond => std::sync::Arc::new(
                        col.as_primitive::<TimestampMicrosecondType>()
                            .clone()
                            .with_timezone(tz.clone()),
                    ),
                    TimeUnit::Nanosecond => std::sync::Arc::new(
                        col.as_primitive::<TimestampNanosecondType>()
                            .clone()
                            .with_timezone(tz.clone()),
                    ),
                }
            }
            _ => col.clone(),
        };
        columns.push(col);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| CodecError::Schema(e.to_string()))
}

/// The rows one bulk batch covers, numbered from 0 within the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    /// The batch's index within the input.
    pub index: usize,
    pub first_row: u64,
    /// Inclusive.
    pub last_row: u64,
}

/// What a codec has done, for metrics and for `bulk_path_has_no_row_decode`.
#[derive(Debug, Default)]
pub struct CodecStats {
    bulk_batches: AtomicU64,
    bulk_rows: AtomicU64,
    row_decodes: AtomicU64,
}

/// A copy of [`CodecStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodecCounters {
    pub bulk_batches: u64,
    pub bulk_rows: u64,
    /// Rows turned into one event each (the row path).
    pub row_decodes: u64,
}

impl CodecStats {
    pub fn snapshot(&self) -> CodecCounters {
        CodecCounters {
            bulk_batches: self.bulk_batches.load(Ordering::Relaxed),
            bulk_rows: self.bulk_rows.load(Ordering::Relaxed),
            row_decodes: self.row_decodes.load(Ordering::Relaxed),
        }
    }

    fn bulk(&self, rows: usize) {
        self.bulk_batches.fetch_add(1, Ordering::Relaxed);
        self.bulk_rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    fn rows(&self, rows: usize) {
        self.row_decodes.fetch_add(rows as u64, Ordering::Relaxed);
    }
}

/// A format that decodes bytes to `RecordBatch`es of [`Codec::schema`] and
/// encodes them back.
pub trait Codec: Send + Sync + std::fmt::Debug {
    /// The MIME type of the encoded bytes.
    fn content_type(&self) -> &'static str;
    fn schema(&self) -> SchemaRef;
    /// Bytes to batches of at most the codec's batch size, column by column.
    fn decode(&self, bytes: &[u8]) -> Result<Vec<RecordBatch>, CodecError>;
    fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>, CodecError>;
    fn stats(&self) -> &CodecStats;

    /// The bulk path: one [`EventBatch::Arrow`] per decoded batch, its
    /// envelope from `envelope` (derive the id from the range with
    /// [`crate::ids::rows`]) with `datacontenttype` set to [`ARROW_STREAM`].
    fn decode_bulk(
        &self,
        bytes: &[u8],
        envelope: &mut dyn FnMut(RowRange) -> CloudEvent,
    ) -> Result<Vec<EventBatch>, CodecError> {
        let batches = self.decode(bytes)?;
        let mut first = 0u64;
        let mut out = Vec::with_capacity(batches.len());
        for (index, batch) in batches.into_iter().enumerate() {
            let n = batch.num_rows() as u64;
            if n == 0 {
                continue;
            }
            let range = RowRange {
                index,
                first_row: first,
                last_row: first + n - 1,
            };
            first += n;
            let mut env = envelope(range);
            env.datacontenttype = Some(ARROW_STREAM.to_owned());
            self.stats().bulk(batch.num_rows());
            out.push(EventBatch::Arrow {
                batch,
                envelope: env,
            });
        }
        Ok(out)
    }

    /// The row path: one event per row, its `data` the row as a JSON object
    /// (`application/json`), its envelope from `envelope(row_number)`.
    fn decode_rows(
        &self,
        bytes: &[u8],
        envelope: &mut dyn FnMut(u64) -> CloudEvent,
    ) -> Result<Vec<CloudEvent>, CodecError> {
        let mut out = Vec::new();
        let mut row = 0u64;
        for batch in self.decode(bytes)? {
            let batch = relabel(batch.clone(), &offset_schema(&batch.schema()))?;
            let mut buf = Vec::new();
            {
                let mut w = arrow_json::LineDelimitedWriter::new(&mut buf);
                w.write(&batch)
                    .map_err(|e| CodecError::Encode(e.to_string()))?;
                w.finish().map_err(|e| CodecError::Encode(e.to_string()))?;
            }
            let lines: Vec<&[u8]> = buf
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .collect();
            debug_assert_eq!(lines.len(), batch.num_rows());
            for line in lines {
                out.push(
                    envelope(row)
                        .with_data("application/json", bytes::Bytes::copy_from_slice(line)),
                );
                row += 1;
            }
            self.stats().rows(batch.num_rows());
        }
        Ok(out)
    }
}
