//! Rows between canonical JSON ([`crate::types`]) and Arrow: the bulk path
//! (D356) for batch reads and the snapshot, and Arrow input for the sink.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, DurationMicrosecondType, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    DurationMicrosecondArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use base64::Engine as _;
use serde_json::{Number, Value};

use crate::types::{
    civil_from_days, datetime_micros, days_from_civil, fmt_datetime, fmt_time, fmt_timestamp,
    parse_datetime, parse_time,
};

fn int(v: &Value, col: &str) -> Result<Option<i64>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Number(n) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("column {col}: {n} is out of range")),
        Value::String(s) => s
            .parse()
            .map(Some)
            .map_err(|_| format!("column {col}: not an integer")),
        _ => Err(format!("column {col}: not an integer")),
    }
}

fn uint(v: &Value, col: &str) -> Result<Option<u64>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Number(n) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("column {col}: {n} is out of range")),
        Value::String(s) => s
            .parse()
            .map(Some)
            .map_err(|_| format!("column {col}: not an unsigned integer")),
        _ => Err(format!("column {col}: not an unsigned integer")),
    }
}

fn float(v: &Value, col: &str) -> Result<Option<f64>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Number(n) => Ok(n.as_f64()),
        Value::String(s) => s
            .parse()
            .map(Some)
            .map_err(|_| format!("column {col}: not a number")),
        _ => Err(format!("column {col}: not a number")),
    }
}

fn string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// A decimal's text at `scale` as the unscaled integer.
pub fn decimal_i128(s: &str, scale: i8) -> Option<i128> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    let scale = usize::try_from(scale).ok()?;
    if frac_part.len() > scale
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part:0<scale$}");
    let n: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse().ok()?
    };
    Some(if neg { -n } else { n })
}

fn decimal_text(n: i128, scale: i8) -> String {
    let scale = usize::try_from(scale).unwrap_or(0);
    let neg = n < 0;
    let digits = n.unsigned_abs().to_string();
    let s = if scale == 0 {
        digits
    } else {
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (i, f) = padded.split_at(padded.len() - scale);
        format!("{i}.{f}")
    };
    if neg { format!("-{s}") } else { s }
}

/// One column of canonical values as an Arrow array of `dt`.
fn column(dt: &DataType, name: &str, vals: &[&Value]) -> Result<ArrayRef, String> {
    macro_rules! ints {
        ($arr:ty, $t:ty, $f:ident) => {{
            let v = vals
                .iter()
                .map(|x| {
                    $f(x, name)?
                        .map(<$t>::try_from)
                        .transpose()
                        .map_err(|_| format!("column {name}: out of range"))
                })
                .collect::<Result<Vec<Option<$t>>, String>>()?;
            Arc::new(<$arr>::from(v)) as ArrayRef
        }};
    }
    Ok(match dt {
        DataType::Int8 => ints!(Int8Array, i8, int),
        DataType::Int16 => ints!(Int16Array, i16, int),
        DataType::Int32 => ints!(Int32Array, i32, int),
        DataType::Int64 => ints!(Int64Array, i64, int),
        DataType::UInt8 => ints!(UInt8Array, u8, uint),
        DataType::UInt16 => ints!(UInt16Array, u16, uint),
        DataType::UInt32 => ints!(UInt32Array, u32, uint),
        DataType::UInt64 => ints!(UInt64Array, u64, uint),
        DataType::Float32 => Arc::new(Float32Array::from(
            vals.iter()
                .map(|x| float(x, name).map(|f| f.map(|f| f as f32)))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            vals.iter()
                .map(|x| float(x, name))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        DataType::Boolean => Arc::new(BooleanArray::from(
            vals.iter()
                .map(|x| match x {
                    Value::Null => Ok(None),
                    Value::Bool(b) => Ok(Some(*b)),
                    other => int(other, name).map(|i| i.map(|i| i != 0)),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        DataType::Decimal128(p, s) => Arc::new(
            Decimal128Array::from(
                vals.iter()
                    .map(|x| match string(x) {
                        None => Ok(None),
                        Some(t) => decimal_i128(&t, *s)
                            .map(Some)
                            .ok_or_else(|| format!("column {name}: {t} is not decimal({p},{s})")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(*p, *s)
            .map_err(|e| format!("column {name}: {e}"))?,
        ),
        DataType::Date32 => Arc::new(Date32Array::from(
            vals.iter()
                .map(|x| match string(x) {
                    None => Ok(None),
                    Some(t) => parse_datetime(&t)
                        .and_then(|(y, m, d, ..)| {
                            i32::try_from(days_from_civil(i64::from(y), m, d)).ok()
                        })
                        .map(Some)
                        .ok_or_else(|| format!("column {name}: {t} is not a date")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a = TimestampMicrosecondArray::from(
                vals.iter()
                    .map(|x| match string(x) {
                        None => Ok(None),
                        Some(t) => datetime_micros(&t)
                            .map(Some)
                            .ok_or_else(|| format!("column {name}: {t} is not a timestamp")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
            Arc::new(match tz {
                Some(tz) => a.with_timezone(tz.clone()),
                None => a,
            })
        }
        DataType::Duration(TimeUnit::Microsecond) => Arc::new(DurationMicrosecondArray::from(
            vals.iter()
                .map(|x| match string(x) {
                    None => Ok(None),
                    Some(t) => parse_time(&t)
                        .map(|(neg, h, m, s, us)| {
                            let v = ((i64::from(h) * 60 + i64::from(m)) * 60 + i64::from(s))
                                * 1_000_000
                                + i64::from(us);
                            Some(if neg { -v } else { v })
                        })
                        .ok_or_else(|| format!("column {name}: {t} is not a time")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        DataType::Utf8 => Arc::new(StringArray::from(
            vals.iter().map(|x| string(x)).collect::<Vec<_>>(),
        )),
        DataType::Binary => {
            let decoded = vals
                .iter()
                .map(|x| match x {
                    Value::Null => Ok(None),
                    Value::String(s) => base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map(Some)
                        .map_err(|_| format!("column {name}: not base64")),
                    _ => Err(format!("column {name}: not binary")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(BinaryArray::from(
                decoded.iter().map(|b| b.as_deref()).collect::<Vec<_>>(),
            ))
        }
        other => return Err(format!("column {name}: no Arrow builder for {other}")),
    })
}

/// Canonical rows (each in `schema`'s column order) as a record batch.
pub fn to_batch(schema: &SchemaRef, rows: &[Vec<Value>]) -> Result<RecordBatch, String> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let vals: Vec<&Value> = rows
                .iter()
                .map(|r| r.get(i).unwrap_or(&Value::Null))
                .collect();
            column(f.data_type(), f.name(), &vals)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())
}

/// `rows` as record batches of at most `max_rows` rows and `max_bytes`
/// bytes each (by `get_array_memory_size`, as the SDK's budgets count), in
/// order, each with the index of its first row. A row that cannot be built,
/// or that alone is over `max_bytes`, is an `Err` in its place.
pub fn pieces(
    schema: &SchemaRef,
    rows: &[Vec<Value>],
    max_rows: usize,
    max_bytes: usize,
) -> Vec<(usize, Result<RecordBatch, String>)> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        // A first guess from the rows' sizes, then halve until it fits.
        let mut n = 0;
        let mut bytes = 0;
        while start + n < rows.len() && n < max_rows.max(1) {
            let s = row_size(&rows[start + n]) * 2 + 64;
            if n > 0 && bytes + s > max_bytes {
                break;
            }
            bytes += s;
            n += 1;
        }
        loop {
            match to_batch(schema, &rows[start..start + n]) {
                Ok(b) if b.get_array_memory_size() <= max_bytes => {
                    out.push((start, Ok(b)));
                    break;
                }
                Ok(_) if n > 1 => n /= 2,
                Ok(b) => {
                    out.push((
                        start,
                        Err(format!(
                            "a row of {} bytes is over the batch budget of {max_bytes}",
                            b.get_array_memory_size()
                        )),
                    ));
                    break;
                }
                Err(_) if n > 1 => n /= 2,
                Err(e) => {
                    out.push((start, Err(e)));
                    break;
                }
            }
        }
        start += n;
    }
    out
}

/// About how many bytes a canonical row takes in Arrow and JSON.
pub fn row_size(row: &[Value]) -> usize {
    row.iter()
        .map(|v| match v {
            Value::String(s) => s.len() + 8,
            _ => 16,
        })
        .sum()
}

/// One Arrow cell as a canonical JSON value (the sink's Arrow input).
pub fn cell(array: &dyn Array, row: usize) -> Result<Value, String> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    let f = |x: f64| Number::from_f64(x).map_or(Value::Null, Value::Number);
    Ok(match array.data_type() {
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).into(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).into(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).into(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).into(),
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).into(),
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).into(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).into(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).into(),
        DataType::Float32 => f(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => f(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, s) => Value::String(decimal_text(
            array.as_primitive::<Decimal128Type>().value(row),
            *s,
        )),
        DataType::Date32 => {
            let (y, m, d) =
                civil_from_days(i64::from(array.as_primitive::<Date32Type>().value(row)));
            Value::String(format!("{y:04}-{m:02}-{d:02}"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let us = array.as_primitive::<TimestampMicrosecondType>().value(row);
            let (secs, frac) = (us.div_euclid(1_000_000), us.rem_euclid(1_000_000) as u32);
            if tz.is_some() {
                Value::String(fmt_timestamp(secs, frac))
            } else {
                let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
                let rem = secs.rem_euclid(86_400) as u32;
                Value::String(fmt_datetime(
                    y as u32,
                    m,
                    d,
                    rem / 3600,
                    (rem / 60) % 60,
                    rem % 60,
                    frac,
                ))
            }
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let us = array.as_primitive::<DurationMicrosecondType>().value(row);
            let a = us.unsigned_abs();
            let secs = a / 1_000_000;
            Value::String(fmt_time(
                us < 0,
                (secs / 3600) as u32,
                ((secs / 60) % 60) as u32,
                (secs % 60) as u32,
                (a % 1_000_000) as u32,
            ))
        }
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_owned()),
        DataType::LargeUtf8 => Value::String(array.as_string::<i64>().value(row).to_owned()),
        DataType::Binary => Value::String(
            base64::engine::general_purpose::STANDARD.encode(array.as_binary::<i32>().value(row)),
        ),
        other => return Err(format!("no MySQL value for Arrow {other}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use serde_json::json;

    #[test]
    fn decimals() {
        assert_eq!(decimal_i128("12.5", 2), Some(1250));
        assert_eq!(decimal_i128("-0.07", 2), Some(-7));
        assert_eq!(decimal_i128("3", 0), Some(3));
        assert_eq!(decimal_i128("1.234", 2), None);
        assert_eq!(decimal_text(1250, 2), "12.50");
        assert_eq!(decimal_text(-7, 2), "-0.07");
        assert_eq!(decimal_text(3, 0), "3");
    }

    #[test]
    fn rows_roundtrip_through_arrow() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("u", DataType::UInt64, true),
            Field::new("d", DataType::Decimal128(10, 2), true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("t", DataType::Duration(TimeUnit::Microsecond), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let rows = vec![
            vec![
                json!(1),
                json!(18_446_744_073_709_551_615u64),
                json!("12.50"),
                json!("2024-02-29"),
                json!("2024-01-02T03:04:05.000006Z"),
                json!("-26:00:01"),
                json!("héllo"),
                json!("AP8="),
            ],
            vec![
                json!(2),
                json!(null),
                json!(null),
                json!(null),
                json!(null),
                json!(null),
                json!(null),
                json!(null),
            ],
        ];
        let batch = to_batch(&schema, &rows).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        for (r, row) in rows.iter().enumerate() {
            for (c, want) in row.iter().enumerate() {
                assert_eq!(
                    &cell(batch.column(c).as_ref(), r).expect("cell"),
                    want,
                    "{r}/{c}"
                );
            }
        }
        assert!(to_batch(&schema, &[vec![json!("x")]]).is_err());
    }

    #[test]
    fn pieces_fit_the_budget_and_isolate_bad_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        let mut rows: Vec<Vec<Value>> = (0..100)
            .map(|i| vec![json!(i), json!("x".repeat(100))])
            .collect();
        rows[40] = vec![json!("not a number"), json!(null)];
        let out = pieces(&schema, &rows, 1000, 4096);
        let mut seen = 0;
        let mut bad = 0;
        for (first, piece) in &out {
            assert_eq!(*first, seen);
            match piece {
                Ok(b) => {
                    assert!(b.get_array_memory_size() <= 4096);
                    seen += b.num_rows();
                }
                Err(_) => {
                    assert_eq!(*first, 40);
                    bad += 1;
                    seen += 1;
                }
            }
        }
        assert_eq!((seen, bad), (100, 1));
    }
}
