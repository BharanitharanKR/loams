//! Rows between JSON, Arrow, Arrow IPC and Parquet, shared by the family.
//!
//! JSON rows decode against a schema the system declared (Trino's columns,
//! Redshift's column metadata). arrow-json 59 resolves a named time zone
//! only with `chrono-tz`, which the workspace does not build (R5.5), so a
//! `UTC` timestamp column is decoded as `+00:00` and relabelled after: the
//! same instants.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use loams_connector_sdk::{CloudEvent, ConnectorError, EventBatch};
use sha2::{Digest, Sha256};

/// `UTC` (and its aliases) on a timestamp column.
fn is_utc(tz: &str) -> bool {
    matches!(tz, "UTC" | "utc" | "Z" | "Etc/UTC" | "GMT")
}

/// `schema` with `UTC` timestamp columns relabelled `+00:00`.
fn fixed_offset(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Timestamp(u, Some(tz)) if is_utc(tz) => Arc::new(
                    Field::new(
                        f.name(),
                        DataType::Timestamp(*u, Some("+00:00".into())),
                        f.is_nullable(),
                    )
                    .with_metadata(f.metadata().clone()),
                ),
                _ => f.clone(),
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

/// Put `schema`'s labels back on columns decoded under [`fixed_offset`].
fn relabel(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ConnectorError> {
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| match (c.data_type(), f.data_type()) {
            (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Timestamp(_, Some(tz))) => {
                match c.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Some(a) => Arc::new(a.clone().with_timezone(tz.clone())) as ArrayRef,
                    None => c.clone(),
                }
            }
            _ => c.clone(),
        })
        .collect();
    RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| ConnectorError::fatal(format!("rows: {e}")))
}

/// JSON objects (one per row, keyed by column name) as one batch of `schema`.
pub fn json_rows(
    schema: &SchemaRef,
    rows: &[serde_json::Value],
) -> Result<RecordBatch, ConnectorError> {
    let decode_schema = Arc::new(fixed_offset(schema));
    let mut decoder = arrow_json::ReaderBuilder::new(decode_schema)
        .with_strict_mode(true)
        .build_decoder()
        .map_err(|e| ConnectorError::fatal(format!("JSON decoder: {e}")))?;
    decoder
        .serialize(rows)
        .map_err(|e| ConnectorError::poison(format!("rows do not fit the schema: {e}")))?;
    let batch = decoder
        .flush()
        .map_err(|e| ConnectorError::poison(format!("rows do not fit the schema: {e}")))?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(fixed_offset(schema))));
    relabel(batch, schema)
}

/// A value as a warehouse renders it, made decodable by arrow-json for
/// `field`: binary as hex (warehouses send base64), a UUID's 16 bytes as
/// hex, and a zoned timestamp's suffix (` UTC`, `Z`, `+00`) as an offset.
/// A timestamp in a named zone other than UTC is refused (`Poison`): cast it
/// to UTC in the query.
pub fn normalise(
    value: serde_json::Value,
    field: &Field,
) -> Result<serde_json::Value, ConnectorError> {
    use base64::Engine as _;
    let serde_json::Value::String(s) = value else {
        return Ok(value);
    };
    let out = match field.data_type() {
        DataType::FixedSizeBinary(16) if s.len() == 36 && s.contains('-') => s.replace('-', ""),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(s.trim())
                .map_err(|e| {
                    ConnectorError::poison(format!("{}: not base64: {e}", field.name()))
                })?;
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        DataType::Timestamp(_, Some(_)) => zone_to_offset(&s).ok_or_else(|| {
            ConnectorError::poison(format!(
                "{}: {s:?} is in a named time zone; cast it to UTC in the query",
                field.name()
            ))
        })?,
        _ => s,
    };
    Ok(serde_json::Value::String(out))
}

/// `2026-10-01 12:00:00.000 UTC` → `…+00:00`; `…Z` and `…+05:30` stay;
/// `…+00` (Postgres-style) → `…+00:00`; no zone stays (read as UTC).
fn zone_to_offset(s: &str) -> Option<String> {
    let t = s.trim();
    if let Some(head) = t
        .strip_suffix(" UTC")
        .or_else(|| t.strip_suffix(" Z"))
        .or_else(|| t.strip_suffix(" Etc/UTC"))
    {
        return Some(format!("{head}+00:00"));
    }
    if t.ends_with('Z') {
        return Some(t.to_owned());
    }
    // A trailing named zone (`… America/New_York`).
    if let Some((_, last)) = t.rsplit_once(' ')
        && last.chars().any(|c| c.is_ascii_alphabetic())
    {
        return None;
    }
    // `+HH` or `-HH` without minutes.
    let tail = &t[t.len().saturating_sub(3)..];
    if (tail.starts_with('+') || tail.starts_with('-'))
        && tail[1..].chars().all(|c| c.is_ascii_digit())
        && t.len() > 10
    {
        return Some(format!("{t}:00"));
    }
    Some(t.to_owned())
}

/// The rows of a sink batch: an Arrow batch as is (D356), or one JSON
/// object per event's `data`, against `schema` when given or the schema the
/// events imply.
pub fn to_record_batch(
    batch: &EventBatch,
    schema: Option<&SchemaRef>,
) -> Result<RecordBatch, ConnectorError> {
    match batch {
        EventBatch::Arrow { batch, .. } => Ok(batch.clone()),
        EventBatch::Events(events) => {
            let values = event_objects(events)?;
            let schema = match schema {
                Some(s) => s.clone(),
                None => Arc::new(
                    arrow_json::reader::infer_json_schema_from_iterator(values.iter().map(Ok))
                        .map_err(|e| {
                            ConnectorError::poison(format!(
                                "cannot infer a schema from the events: {e}"
                            ))
                        })?,
                ),
            };
            json_rows(&schema, &values)
        }
    }
}

/// Each event's `data` as a JSON object; anything else is `Poison`.
pub fn event_objects(events: &[CloudEvent]) -> Result<Vec<serde_json::Value>, ConnectorError> {
    events
        .iter()
        .map(|ev| {
            let v: serde_json::Value = serde_json::from_slice(&ev.data)
                .map_err(|e| ConnectorError::poison(format!("event {} is not JSON: {e}", ev.id)))?;
            if v.is_object() {
                Ok(v)
            } else {
                Err(ConnectorError::poison(format!(
                    "event {} is not a JSON object",
                    ev.id
                )))
            }
        })
        .collect()
}

/// A batch's identity, stable across retries of the same batch: SHA-256 over
/// its event ids (or its Arrow envelope's id), first 32 hex digits. The
/// ClickHouse dedup token and the staged file names of Redshift and
/// Databricks.
pub fn batch_id(batch: &EventBatch) -> String {
    let mut h = Sha256::new();
    match batch {
        EventBatch::Events(events) => {
            for ev in events {
                h.update((ev.source.len() as u64).to_be_bytes());
                h.update(ev.source.as_bytes());
                h.update((ev.id.len() as u64).to_be_bytes());
                h.update(ev.id.as_bytes());
            }
        }
        EventBatch::Arrow { envelope, batch } => {
            h.update(envelope.source.as_bytes());
            h.update([0]);
            h.update(envelope.id.as_bytes());
            h.update((batch.num_rows() as u64).to_be_bytes());
        }
    }
    h.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A batch as an Arrow IPC stream.
pub fn ipc_stream(batch: &RecordBatch) -> Result<Bytes, ConnectorError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| ConnectorError::fatal(format!("Arrow IPC: {e}")))?;
        w.write(batch)
            .map_err(|e| ConnectorError::fatal(format!("Arrow IPC: {e}")))?;
        w.finish()
            .map_err(|e| ConnectorError::fatal(format!("Arrow IPC: {e}")))?;
    }
    Ok(Bytes::from(buf))
}

/// Read an Arrow IPC stream.
pub fn read_ipc_stream(body: Bytes) -> Result<Vec<RecordBatch>, ConnectorError> {
    let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None)
        .map_err(|e| ConnectorError::retryable(format!("Arrow IPC stream: {e}")))?;
    r.collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConnectorError::retryable(format!("Arrow IPC stream: {e}")))
}

/// A batch as one Parquet file (Snappy-free, uncompressed: the warehouses'
/// COPY readers accept it and nothing extra is compiled).
pub fn parquet_file(batch: &RecordBatch) -> Result<Bytes, ConnectorError> {
    let mut buf = Vec::new();
    {
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None)
            .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))?;
        w.write(batch)
            .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))?;
        w.close()
            .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))?;
    }
    Ok(Bytes::from(buf))
}

/// Read a Parquet file.
pub fn read_parquet(body: Bytes) -> Result<Vec<RecordBatch>, ConnectorError> {
    let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(body)
        .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))?
        .build()
        .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))?;
    r.collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConnectorError::fatal(format!("Parquet: {e}")))
}

/// Cut `batch` from row `at` to fit `max_rows` and `max_bytes` (its rows'
/// average size), copied into its own buffers so its memory size is its own.
/// `None` when not even one row fits.
pub fn take(
    batch: &RecordBatch,
    at: usize,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Option<RecordBatch>, ConnectorError> {
    let left = batch.num_rows().saturating_sub(at);
    let per_row = (batch.get_array_memory_size() / batch.num_rows().max(1)).max(1);
    let by_bytes = (max_bytes.saturating_sub(1024) * 9 / 10) / per_row;
    let n = left.min(max_rows).min(by_bytes);
    if n == 0 {
        return Ok(None);
    }
    let slice = batch.slice(at, n);
    arrow_select::concat::concat_batches(&slice.schema(), [&slice])
        .map(Some)
        .map_err(|e| ConnectorError::fatal(format!("copy rows: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    #[test]
    fn utc_timestamps_decode_without_chrono_tz() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let rows = vec![
            serde_json::json!({"id": 1, "at": "2026-10-01 12:00:00.000"}),
            serde_json::json!({"id": 2, "at": null}),
        ];
        let b = json_rows(&schema, &rows).expect("decodes");
        assert_eq!(b.schema(), schema);
        let at = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert_eq!(at.value(0), 1_790_856_000_000_000);
        assert!(at.is_null(1));
        let unknown = vec![serde_json::json!({"id": 1, "nope": 2})];
        assert!(json_rows(&schema, &unknown).is_err());
    }

    #[test]
    fn warehouse_values_normalise() {
        let ts = Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        );
        let n = |v: &str, f: &Field| normalise(serde_json::json!(v), f);
        assert_eq!(
            n("2026-10-01 12:00:00.000 UTC", &ts).expect("ok"),
            "2026-10-01 12:00:00.000+00:00"
        );
        assert_eq!(
            n("2026-10-01 12:00:00+00", &ts).expect("ok"),
            "2026-10-01 12:00:00+00:00"
        );
        assert!(n("2026-10-01 12:00:00 America/New_York", &ts).is_err());
        let bin = Field::new("b", DataType::Binary, true);
        assert_eq!(n("AAEC", &bin).expect("ok"), "000102");
        let uuid = Field::new("u", DataType::FixedSizeBinary(16), true);
        assert_eq!(
            n("12345678-1234-1234-1234-123456789abc", &uuid).expect("ok"),
            "12345678123412341234123456789abc"
        );
        assert_eq!(
            normalise(serde_json::json!(5), &bin).expect("ok"),
            serde_json::json!(5)
        );
    }

    #[test]
    fn batch_ids_are_stable_and_ipc_round_trips() {
        let ev = |i: u32| CloudEvent::new(format!("e{i}"), "/s", "t");
        let a = EventBatch::Events(vec![ev(1), ev(2)]);
        let b = EventBatch::Events(vec![ev(1), ev(2)]);
        let c = EventBatch::Events(vec![ev(1), ev(3)]);
        assert_eq!(batch_id(&a), batch_id(&b));
        assert_ne!(batch_id(&a), batch_id(&c));
        assert_eq!(batch_id(&a).len(), 32);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let rb = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("batch");
        let back = read_ipc_stream(ipc_stream(&rb).expect("ipc")).expect("read");
        assert_eq!(back, vec![rb.clone()]);
        let back = read_parquet(parquet_file(&rb).expect("parquet")).expect("read");
        assert_eq!(back, vec![rb.clone()]);
        let t = take(&rb, 1, 1, 1 << 20).expect("take").expect("one row");
        assert_eq!(t.num_rows(), 1);
    }
}
