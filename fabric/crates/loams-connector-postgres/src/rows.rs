//! Rows to Arrow and back: binary column values (binary `COPY` and binary
//! result rows) become a `RecordBatch` (the bulk path, D356), and Arrow
//! cells become the text a Postgres input function takes (the sink).

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    FixedSizeListBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use bytes::Bytes;

use crate::types::{
    ColKind, PG_EPOCH_DAYS, PG_EPOCH_MICROS, decimal_text_to_i128, format_date, format_time,
    format_timestamp, format_uuid, format_vector, i128_to_decimal_text, numeric_binary_to_text,
    vector_binary,
};

/// One row of binary column values (`None` is SQL NULL).
pub type RawRow = Vec<Option<Bytes>>;

/// The size a buffered row counts for against a budget: its bytes plus a
/// fixed overhead per column (offsets, validity, the array headers).
pub fn row_cost(row: &RawRow) -> usize {
    row.iter()
        .map(|v| v.as_ref().map_or(0, Bytes::len) + 16)
        .sum()
}

fn be<const N: usize>(raw: &[u8], what: &str) -> Result<[u8; N], String> {
    raw.try_into()
        .map_err(|_| format!("{what}: expected {N} bytes, got {}", raw.len()))
}

/// Build a `RecordBatch` of `schema` from binary rows; `kinds[i]` says how
/// column `i` is encoded.
pub fn build_batch(
    schema: &SchemaRef,
    kinds: &[ColKind],
    rows: &[RawRow],
) -> Result<RecordBatch, String> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(kinds.len());
    for (i, (field, kind)) in schema.fields().iter().zip(kinds).enumerate() {
        let cells = rows.iter().map(|r| r.get(i).and_then(|c| c.as_deref()));
        arrays.push(
            build_column(field, *kind, rows.len(), cells)
                .map_err(|e| format!("column {}: {e}", field.name()))?,
        );
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(|e| e.to_string())
}

fn build_column<'a>(
    field: &Field,
    kind: ColKind,
    n: usize,
    cells: impl Iterator<Item = Option<&'a [u8]>>,
) -> Result<ArrayRef, String> {
    macro_rules! fixed {
        ($builder:ty, $n:literal, $conv:expr) => {{
            let mut b = <$builder>::with_capacity(n);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => {
                        let bytes = be::<$n>(raw, field.name())?;
                        b.append_value($conv(bytes));
                    }
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    Ok(match kind {
        ColKind::Bool => fixed!(BooleanBuilder, 1, |b: [u8; 1]| b[0] != 0),
        ColKind::Int2 => fixed!(Int16Builder, 2, i16::from_be_bytes),
        ColKind::Int4 => fixed!(Int32Builder, 4, i32::from_be_bytes),
        ColKind::Int8 => fixed!(Int64Builder, 8, i64::from_be_bytes),
        ColKind::Float4 => fixed!(Float32Builder, 4, f32::from_be_bytes),
        ColKind::Float8 => fixed!(Float64Builder, 8, f64::from_be_bytes),
        ColKind::Date => fixed!(Date32Builder, 4, |b| i32::from_be_bytes(b) + PG_EPOCH_DAYS),
        ColKind::Time => fixed!(Time64MicrosecondBuilder, 8, i64::from_be_bytes),
        ColKind::Timestamp | ColKind::Timestamptz => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => b.append_value(
                        i64::from_be_bytes(be::<8>(raw, field.name())?) + PG_EPOCH_MICROS,
                    ),
                }
            }
            let tz = match field.data_type() {
                DataType::Timestamp(_, tz) => tz.clone(),
                _ => None,
            };
            Arc::new(b.finish().with_timezone_opt(tz))
        }
        ColKind::Decimal { precision, scale } => {
            let mut b = Decimal128Builder::with_capacity(n);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => {
                        let text = numeric_binary_to_text(raw)?;
                        b.append_value(decimal_text_to_i128(&text, scale)?);
                    }
                }
            }
            Arc::new(
                b.finish()
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| e.to_string())?,
            )
        }
        ColKind::NumericText | ColKind::Text | ColKind::Json | ColKind::Jsonb => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => {
                        let text = match kind {
                            ColKind::NumericText => numeric_binary_to_text(raw)?,
                            // jsonb's binary form is a version byte, then text.
                            ColKind::Jsonb => match raw.split_first() {
                                Some((1, rest)) => utf8(rest)?.to_owned(),
                                _ => return Err("jsonb: unknown binary version".to_owned()),
                            },
                            _ => utf8(raw)?.to_owned(),
                        };
                        b.append_value(text);
                    }
                }
            }
            Arc::new(b.finish())
        }
        ColKind::Bytea => {
            let mut b = BinaryBuilder::with_capacity(n, n * 16);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => b.append_value(raw),
                }
            }
            Arc::new(b.finish())
        }
        ColKind::Uuid => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, 16);
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(raw) => b.append_value(raw).map_err(|e| e.to_string())?,
                }
            }
            Arc::new(b.finish())
        }
        ColKind::Vector(dim) => {
            let item = match field.data_type() {
                DataType::FixedSizeList(item, _) | DataType::List(item) => item.clone(),
                other => return Err(format!("a vector column mapped to {other}")),
            };
            match dim {
                Some(d) => {
                    let mut b = FixedSizeListBuilder::with_capacity(
                        Float32Builder::with_capacity(n * d as usize),
                        d,
                        n,
                    )
                    .with_field(item);
                    for c in cells {
                        match c {
                            None => {
                                for _ in 0..d {
                                    b.values().append_null();
                                }
                                b.append(false);
                            }
                            Some(raw) => {
                                let v = vector_binary(raw)?;
                                if v.len() != d as usize {
                                    return Err(format!(
                                        "a vector of dimension {} in a vector({d}) column",
                                        v.len()
                                    ));
                                }
                                b.values().append_slice(&v);
                                b.append(true);
                            }
                        }
                    }
                    Arc::new(b.finish())
                }
                None => {
                    let mut b =
                        ListBuilder::with_capacity(Float32Builder::new(), n).with_field(item);
                    for c in cells {
                        match c {
                            None => b.append(false),
                            Some(raw) => {
                                b.values().append_slice(&vector_binary(raw)?);
                                b.append(true);
                            }
                        }
                    }
                    Arc::new(b.finish())
                }
            }
        }
    })
}

fn utf8(raw: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(raw).map_err(|_| "invalid UTF-8".to_owned())
}

/// Cell `row` of `array` as the text a Postgres input function takes, or
/// `None` for NULL. Covers the Arrow types [`crate::types::PgTypeMapper`]
/// produces and the common ones an upstream bulk batch carries.
pub fn cell_text(array: &dyn Array, row: usize) -> Result<Option<String>, String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    if array.is_null(row) {
        return Ok(None);
    }
    let s = match array.data_type() {
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).to_string(),
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
        DataType::Float32 => array.as_primitive::<Float32Type>().value(row).to_string(),
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row).to_string(),
        DataType::Decimal128(_, scale) => {
            i128_to_decimal_text(array.as_primitive::<Decimal128Type>().value(row), *scale)
        }
        DataType::Utf8 => array.as_string::<i32>().value(row).to_owned(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_owned(),
        DataType::Binary => bytea_text(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => bytea_text(array.as_binary::<i64>().value(row)),
        DataType::FixedSizeBinary(16) => format_uuid(array.as_fixed_size_binary().value(row)),
        DataType::FixedSizeBinary(_) => bytea_text(array.as_fixed_size_binary().value(row)),
        DataType::Date32 => format_date(i64::from(array.as_primitive::<Date32Type>().value(row))),
        DataType::Time64(TimeUnit::Microsecond) => {
            format_time(array.as_primitive::<Time64MicrosecondType>().value(row))
        }
        DataType::Timestamp(unit, tz) => {
            let v = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    array.as_primitive::<TimestampMillisecondType>().value(row) * 1_000
                }
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => {
                    array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000
                }
            };
            format_timestamp(v, tz.is_some())
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list().value(row);
            format_vector(float_items(list.as_ref())?.as_slice())
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>().value(row);
            format_vector(float_items(list.as_ref())?.as_slice())
        }
        other => return Err(format!("no Postgres text form for Arrow {other}")),
    };
    Ok(Some(s))
}

fn float_items(a: &dyn Array) -> Result<Vec<f32>, String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Float64Type};
    match a.data_type() {
        DataType::Float32 => Ok(a.as_primitive::<Float32Type>().values().to_vec()),
        DataType::Float64 => Ok(a
            .as_primitive::<Float64Type>()
            .values()
            .iter()
            .map(|x| *x as f32)
            .collect()),
        other => Err(format!("a list of {other} is not a vector")),
    }
}

fn bytea_text(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + 2 * b.len());
    s.push_str("\\x");
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PgTypeMapper;
    use arrow_schema::Schema;
    use loams_connector_sdk::schema::{NativeType, TypeMapper};

    fn field(name: &str, ty: &str) -> (Field, ColKind) {
        let f = PgTypeMapper::default()
            .to_arrow(&NativeType::new(format!("public.t.{name}"), ty, true))
            .expect(ty);
        let k = ColKind::of(&f, ty).expect(ty);
        (f, k)
    }

    #[test]
    fn binary_rows_build_a_batch_and_read_back_as_text() {
        let cols = [
            field("id", "bigint"),
            field("name", "text"),
            field("price", "numeric(10,2)"),
            field("raw", "numeric"),
            field("at", "timestamp with time zone"),
            field("day", "date"),
            field("emb", "vector(2)"),
            field("doc", "jsonb"),
            field("ok", "boolean"),
        ];
        let schema: SchemaRef = Arc::new(Schema::new(
            cols.iter().map(|(f, _)| f.clone()).collect::<Vec<_>>(),
        ));
        let kinds: Vec<ColKind> = cols.iter().map(|(_, k)| *k).collect();
        // 12.50 as numeric: ndigits 2, weight 0, sign +, dscale 2; 12, 5000.
        let price = Bytes::from_static(&[0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 0x13, 0x88]);
        let mut emb = vec![0u8, 2, 0, 0];
        emb.extend_from_slice(&1.5f32.to_be_bytes());
        emb.extend_from_slice(&(-2.0f32).to_be_bytes());
        let row: RawRow = vec![
            Some(Bytes::copy_from_slice(&7i64.to_be_bytes())),
            Some(Bytes::from_static(b"seven")),
            Some(price.clone()),
            Some(price),
            Some(Bytes::copy_from_slice(&1_000_000i64.to_be_bytes())),
            Some(Bytes::copy_from_slice(&1i32.to_be_bytes())),
            Some(Bytes::from(emb)),
            Some(Bytes::from_static(b"\x01{\"a\": 1}")),
            Some(Bytes::from_static(&[1])),
        ];
        let nulls: RawRow = vec![None; kinds.len()];
        let batch = build_batch(&schema, &kinds, &[row, nulls]).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let texts: Vec<Option<String>> = (0..batch.num_columns())
            .map(|c| cell_text(batch.column(c).as_ref(), 0).expect("text"))
            .collect();
        assert_eq!(
            texts,
            vec![
                Some("7".to_owned()),
                Some("seven".to_owned()),
                Some("12.50".to_owned()),
                Some("12.50".to_owned()),
                Some("2000-01-01 00:00:01.000000+00".to_owned()),
                Some("2000-01-02".to_owned()),
                Some("[1.5,-2]".to_owned()),
                Some("{\"a\": 1}".to_owned()),
                Some("true".to_owned()),
            ]
        );
        for c in 0..batch.num_columns() {
            assert_eq!(cell_text(batch.column(c).as_ref(), 1).expect("null"), None);
        }
    }

    #[test]
    fn a_wrong_width_value_is_an_error() {
        let (f, k) = field("id", "integer");
        let schema: SchemaRef = Arc::new(Schema::new(vec![f]));
        let err = build_batch(&schema, &[k], &[vec![Some(Bytes::from_static(&[1, 2]))]])
            .expect_err("two bytes for an int4");
        assert!(err.contains("column id"), "{err}");
    }
}
