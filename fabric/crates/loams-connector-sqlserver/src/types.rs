//! SQL Server types: the family's `TypeMapper` (CN2 Task 5's contract), the
//! values `tiberius` returns as owned [`Cell`]s, Arrow batches built from
//! them, and the text forms the sink and the key-range reads cast back to a
//! column's declared type.
//!
//! | SQL Server | Arrow |
//! |---|---|
//! | `bit` | `Boolean` |
//! | `tinyint` | `UInt8` (0–255) |
//! | `smallint`, `int`, `bigint` | `Int16`, `Int32`, `Int64` |
//! | `real`, `float` | `Float32`, `Float64` (`float(n≤24)` is `real`) |
//! | `decimal(p,s)`, `numeric(p,s)` | `Decimal128(p,s)` |
//! | `money`, `smallmoney` | `Decimal128(19,4)`, `Decimal128(10,4)` |
//! | `char`, `varchar`, `nchar`, `nvarchar` (`n` or `max`), `text`, `ntext`, `xml` | `Utf8` (`loams.max_length` for `n`) |
//! | `binary`, `varbinary`, `image`, `rowversion` | `Binary` |
//! | `uniqueidentifier` | `FixedSizeBinary(16)`, `arrow.uuid` |
//! | `date` | `Date32` |
//! | `time(n)` | `Time64(µs)`, `Time64(ns)` for `n = 7` |
//! | `datetimeoffset(n)` | `Timestamp(µs, UTC)`, `ns` for `n = 7` |
//! | `datetime`, `smalldatetime`, `datetime2(n)` | as `datetimeoffset`, **lossy** (no timezone, R5.2) |
//! | `sql_variant`, `geography`, `geometry`, `hierarchyid`, CLR types | `Unsupported` |
//!
//! Scale 7 (100 ns, SQL Server's default for `time`, `datetime2` and
//! `datetimeoffset`) maps to nanoseconds so no precision is lost; such a
//! value outside 1677–2262 does not fit and its row is `Poison`.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder, UInt8Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use loams_connector_sdk::schema::arrow::EXTENSION_NAME_KEY;
use loams_connector_sdk::schema::{
    Lossy, MAX_LENGTH_KEY, NativeType, PATH_KEY, SchemaError, TypeMapper,
};
use serde_json::Value;
use tiberius::ColumnData;

/// `[name]`, with `]` doubled.
pub fn quote_ident(s: &str) -> String {
    format!("[{}]", s.replace(']', "]]"))
}

/// `N'text'`, with `'` doubled.
pub fn quote_literal(s: &str) -> String {
    format!("N'{}'", s.replace('\'', "''"))
}

/// The declared type of a column from `sys.columns`: the base type's name
/// with its length, precision or scale (`nvarchar(max)`, `decimal(18,2)`).
pub fn declared_type(base: &str, max_length: i16, precision: u8, scale: u8) -> String {
    let base = base.to_ascii_lowercase();
    let len = |unit: i16| {
        if max_length == -1 {
            "max".to_owned()
        } else {
            (max_length / unit).to_string()
        }
    };
    match base.as_str() {
        "varchar" | "char" | "varbinary" | "binary" => format!("{base}({})", len(1)),
        "nvarchar" | "nchar" => format!("{base}({})", len(2)),
        "decimal" | "numeric" => format!("{base}({precision},{scale})"),
        "time" | "datetime2" | "datetimeoffset" => format!("{base}({scale})"),
        "float" => {
            if precision <= 24 {
                "real".to_owned()
            } else {
                "float".to_owned()
            }
        }
        "timestamp" => "rowversion".to_owned(),
        _ => base,
    }
}

fn parse(type_name: &str) -> (String, Vec<String>) {
    let lower = type_name.trim().to_ascii_lowercase();
    match lower.find('(') {
        Some(open) => {
            let close = lower.rfind(')').unwrap_or(lower.len());
            let args = lower[open + 1..close.max(open + 1)]
                .split(',')
                .map(|a| a.trim().to_owned())
                .filter(|a| !a.is_empty())
                .collect();
            (lower[..open].trim().to_owned(), args)
        }
        None => (lower, Vec::new()),
    }
}

fn scale_of(args: &[String], default: u32) -> u32 {
    args.first()
        .and_then(|a| a.parse().ok())
        .unwrap_or(default)
        .min(7)
}

fn ts(scale: u32) -> DataType {
    let unit = if scale > 6 {
        TimeUnit::Nanosecond
    } else {
        TimeUnit::Microsecond
    };
    DataType::Timestamp(unit, Some("UTC".into()))
}

/// The family's mapper (see the module's table). `lossy` is the instance's.
#[derive(Debug, Clone, Default)]
pub struct SqlServerTypeMapper {
    lossy: Lossy,
}

impl SqlServerTypeMapper {
    pub fn new(lossy: Lossy) -> Self {
        Self { lossy }
    }
}

impl TypeMapper for SqlServerTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let unsupported = || SchemaError::Unsupported {
            path: native.path.clone(),
            native: native.type_name.clone(),
        };
        let (base, args) = parse(&native.type_name);
        let mut md = std::collections::HashMap::from([(PATH_KEY.to_owned(), native.path.clone())]);
        let field =
            |dt: DataType, md| Field::new(native.column(), dt, native.nullable).with_metadata(md);
        let dt = match base.as_str() {
            "bit" => DataType::Boolean,
            "tinyint" => DataType::UInt8,
            "smallint" => DataType::Int16,
            "int" => DataType::Int32,
            "bigint" => DataType::Int64,
            "real" => DataType::Float32,
            "float" => match args.first().and_then(|a| a.parse::<u32>().ok()) {
                Some(n) if n <= 24 => DataType::Float32,
                _ => DataType::Float64,
            },
            "decimal" | "numeric" => {
                let p: u8 = args.first().and_then(|a| a.parse().ok()).unwrap_or(18);
                let s: i8 = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(0);
                if p == 0 || p > 38 || s < 0 || s as u8 > p {
                    return Err(unsupported());
                }
                DataType::Decimal128(p, s)
            }
            "money" => DataType::Decimal128(19, 4),
            "smallmoney" => DataType::Decimal128(10, 4),
            "char" | "varchar" | "nchar" | "nvarchar" => {
                if let Some(n) = args.first().filter(|a| a.as_str() != "max") {
                    md.insert(MAX_LENGTH_KEY.to_owned(), n.clone());
                }
                DataType::Utf8
            }
            "text" | "ntext" | "sysname" => DataType::Utf8,
            "xml" => DataType::Utf8,
            "binary" | "varbinary" | "image" | "rowversion" | "timestamp" => DataType::Binary,
            "uniqueidentifier" => {
                md.insert(EXTENSION_NAME_KEY.to_owned(), "arrow.uuid".to_owned());
                DataType::FixedSizeBinary(16)
            }
            "date" => DataType::Date32,
            "time" => {
                if scale_of(&args, 7) > 6 {
                    DataType::Time64(TimeUnit::Nanosecond)
                } else {
                    DataType::Time64(TimeUnit::Microsecond)
                }
            }
            "datetimeoffset" => ts(scale_of(&args, 7)),
            "datetime2" | "datetime" | "smalldatetime" => {
                let scale = if base == "datetime2" {
                    scale_of(&args, 7)
                } else {
                    3
                };
                return self.lossy.accept(
                    &native.path,
                    &native.type_name,
                    field(ts(scale), md),
                    "a timestamp without a timezone is read as UTC",
                );
            }
            _ => return Err(unsupported()),
        };
        Ok(field(dt, md))
    }

    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError> {
        let path = field
            .metadata()
            .get(PATH_KEY)
            .cloned()
            .unwrap_or_else(|| field.name().clone());
        let ext = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
        let lossy = |to: &str, reason: &str| -> Result<String, SchemaError> {
            if self.lossy.allows(&path) {
                Ok(to.to_owned())
            } else {
                Err(SchemaError::Lossy {
                    path: path.clone(),
                    from: loams_connector_sdk::schema::render_type(field.data_type()),
                    to: to.to_owned(),
                    reason: reason.to_owned(),
                })
            }
        };
        let type_name = match field.data_type() {
            DataType::Boolean => "bit".to_owned(),
            DataType::UInt8 => "tinyint".to_owned(),
            DataType::Int8 | DataType::Int16 => "smallint".to_owned(),
            DataType::UInt16 | DataType::Int32 => "int".to_owned(),
            DataType::UInt32 | DataType::Int64 => "bigint".to_owned(),
            DataType::UInt64 => lossy("bigint", "values over 2^63-1 do not fit bigint")?,
            DataType::Float32 => "real".to_owned(),
            DataType::Float64 => "float".to_owned(),
            DataType::Decimal128(p, s) if *s >= 0 => format!("decimal({p},{s})"),
            DataType::Utf8 | DataType::LargeUtf8 => match field.metadata().get(MAX_LENGTH_KEY) {
                Some(n) => format!("nvarchar({n})"),
                None => "nvarchar(max)".to_owned(),
            },
            DataType::FixedSizeBinary(16) if ext == Some("arrow.uuid") => {
                "uniqueidentifier".to_owned()
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                "varbinary(max)".to_owned()
            }
            DataType::Date32 => "date".to_owned(),
            DataType::Time64(TimeUnit::Microsecond) => "time(6)".to_owned(),
            DataType::Time64(TimeUnit::Nanosecond) => "time(7)".to_owned(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => "datetimeoffset(7)".to_owned(),
            DataType::Timestamp(_, Some(_)) => "datetimeoffset(6)".to_owned(),
            DataType::Timestamp(_, None) => lossy(
                "datetime2(7)",
                "a timestamp without a timezone has no instant",
            )?,
            _ => {
                return Err(SchemaError::Unsupported {
                    path,
                    native: loams_connector_sdk::schema::render_type(field.data_type()),
                });
            }
        };
        Ok(NativeType {
            path,
            type_name,
            nullable: field.is_nullable(),
        })
    }
}

/// One column of a described table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// The declared type (`nvarchar(40)`).
    pub declared: String,
    pub nullable: bool,
    pub identity: bool,
    /// Computed or `rowversion`: read, never written.
    pub generated: bool,
}

impl Column {
    pub fn base(&self) -> String {
        parse(&self.declared).0
    }
}

/// An expression casting the text `expr` to `declared` (the form the
/// key-range reads and the sink use, so no client codec is needed per type).
pub fn cast_expr(expr: &str, declared: &str) -> String {
    let (base, _) = parse(declared);
    match base.as_str() {
        "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" => {
            format!("CONVERT({declared}, {expr}, 127)")
        }
        "binary" | "varbinary" | "image" | "rowversion" | "timestamp" => {
            format!("CONVERT({declared}, {expr}, 1)")
        }
        "bit" => format!(
            "CAST(CASE {expr} WHEN N'true' THEN N'1' WHEN N'false' THEN N'0' ELSE {expr} END AS bit)"
        ),
        _ => format!("CAST({expr} AS {declared})"),
    }
}

/// An owned value read from SQL Server.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// The unscaled value and its scale.
    Decimal(i128, u8),
    Text(String),
    Bytes(Vec<u8>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Nanoseconds since midnight.
    Time(i64),
    /// Seconds and nanoseconds since 1970-01-01 UTC.
    Timestamp(i64, u32),
    Uuid([u8; 16]),
}

/// Days from 1970-01-01 to the civil date (proleptic Gregorian).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day count from 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 0001-01-01 (TDS `date` day 0) and 1900-01-01 (`datetime` day 0).
const DAY0_0001: i64 = -719_162;
const DAY0_1900: i64 = -25_567;

fn time_nanos(t: tiberius::time::Time) -> i64 {
    let scale = u32::from(t.scale().min(9));
    t.increments() as i64 * 10_i64.pow(9 - scale)
}

fn stamp(days: i64, nanos_of_day: i64) -> Cell {
    let secs = days * 86_400 + nanos_of_day.div_euclid(1_000_000_000);
    Cell::Timestamp(secs, nanos_of_day.rem_euclid(1_000_000_000) as u32)
}

impl Cell {
    /// The owned value of a cell `tiberius` decoded.
    pub fn from_column(d: &ColumnData<'_>) -> Cell {
        match d {
            ColumnData::U8(v) => v.map_or(Cell::Null, |x| Cell::Int(i64::from(x))),
            ColumnData::I16(v) => v.map_or(Cell::Null, |x| Cell::Int(i64::from(x))),
            ColumnData::I32(v) => v.map_or(Cell::Null, |x| Cell::Int(i64::from(x))),
            ColumnData::I64(v) => v.map_or(Cell::Null, Cell::Int),
            ColumnData::F32(v) => v.map_or(Cell::Null, |x| Cell::Float(f64::from(x))),
            ColumnData::F64(v) => v.map_or(Cell::Null, Cell::Float),
            ColumnData::Bit(v) => v.map_or(Cell::Null, Cell::Bool),
            ColumnData::String(v) => v.as_ref().map_or(Cell::Null, |s| Cell::Text(s.to_string())),
            ColumnData::Guid(v) => v.map_or(Cell::Null, |u| Cell::Uuid(*u.as_bytes())),
            ColumnData::Binary(v) => v.as_ref().map_or(Cell::Null, |b| Cell::Bytes(b.to_vec())),
            ColumnData::Numeric(v) => v.map_or(Cell::Null, |n| Cell::Decimal(n.value(), n.scale())),
            ColumnData::Xml(v) => v.as_ref().map_or(Cell::Null, |x| Cell::Text(x.to_string())),
            ColumnData::DateTime(v) => v.map_or(Cell::Null, |dt| {
                // 1/300 s ticks.
                let nanos = i64::from(dt.seconds_fragments()) * 10_000_000 / 3;
                stamp(DAY0_1900 + i64::from(dt.days()), nanos)
            }),
            ColumnData::SmallDateTime(v) => v.map_or(Cell::Null, |dt| {
                stamp(
                    DAY0_1900 + i64::from(dt.days()),
                    i64::from(dt.seconds_fragments()) * 60_000_000_000,
                )
            }),
            ColumnData::Time(v) => v.map_or(Cell::Null, |t| Cell::Time(time_nanos(t))),
            ColumnData::Date(v) => v.map_or(Cell::Null, |d| {
                Cell::Date((DAY0_0001 + i64::from(d.days())) as i32)
            }),
            ColumnData::DateTime2(v) => v.map_or(Cell::Null, |dt| {
                stamp(
                    DAY0_0001 + i64::from(dt.date().days()),
                    time_nanos(dt.time()),
                )
            }),
            // TDS carries a datetimeoffset as UTC plus the offset, so the
            // instant is the UTC part.
            ColumnData::DateTimeOffset(v) => v.map_or(Cell::Null, |dto| {
                let dt = dto.datetime2();
                stamp(
                    DAY0_0001 + i64::from(dt.date().days()),
                    time_nanos(dt.time()),
                )
            }),
        }
    }

    /// A rough in-memory size, for budgets.
    pub fn cost(&self) -> usize {
        match self {
            Cell::Text(s) => s.len() + 8,
            Cell::Bytes(b) => b.len() + 8,
            Cell::Uuid(_) | Cell::Decimal(..) => 16,
            _ => 8,
        }
    }

    /// The JSON form in `change` events: numbers stay numbers (decimals as
    /// exact strings), bytes are `0x…` hex, times are ISO 8601 (UTC `Z`).
    pub fn to_json(&self) -> Value {
        match self {
            Cell::Null => Value::Null,
            Cell::Bool(b) => Value::Bool(*b),
            Cell::Int(i) => Value::from(*i),
            Cell::Float(f) => serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number),
            other => Value::String(other.to_text().unwrap_or_default()),
        }
    }

    /// The text form [`cast_expr`] converts back (`None` is NULL).
    pub fn to_text(&self) -> Option<String> {
        Some(match self {
            Cell::Null => return None,
            Cell::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
            Cell::Int(i) => i.to_string(),
            Cell::Float(f) => f.to_string(),
            Cell::Decimal(v, s) => decimal_text(*v, *s),
            Cell::Text(s) => s.clone(),
            Cell::Bytes(b) => hex(b),
            Cell::Date(d) => date_text(i64::from(*d)),
            Cell::Time(n) => time_text(*n),
            Cell::Timestamp(s, n) => timestamp_text(*s, *n),
            Cell::Uuid(u) => uuid_text(u),
        })
    }
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for x in b {
        s.push_str(&format!("{x:02X}"));
    }
    s
}

pub fn decimal_text(v: i128, scale: u8) -> String {
    if scale == 0 {
        return v.to_string();
    }
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{}{int}.{frac}", if neg { "-" } else { "" })
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn time_text(nanos: i64) -> String {
    let s = nanos / 1_000_000_000;
    let frac = nanos % 1_000_000_000;
    let base = format!("{:02}:{:02}:{:02}", s / 3600, (s / 60) % 60, s % 60);
    if frac == 0 {
        base
    } else {
        // 100 ns is SQL Server's finest unit: seven digits.
        format!("{base}.{:07}", frac / 100)
    }
}

pub fn timestamp_text(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) * 1_000_000_000 + i64::from(nanos);
    format!("{}T{}Z", date_text(days), time_text(tod))
}

fn uuid_text(u: &[u8; 16]) -> String {
    let h: String = u.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// An Arrow batch of `rows` (cells in the schema's column order).
pub fn build_batch(schema: &SchemaRef, rows: &[Vec<Cell>]) -> Result<RecordBatch, String> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        let cells = rows.iter().map(|r| r.get(i).unwrap_or(&Cell::Null));
        cols.push(column(f, cells)?);
    }
    RecordBatch::try_new(schema.clone(), cols).map_err(|e| e.to_string())
}

fn mismatch(f: &Field, c: &Cell) -> String {
    format!("column {}: {c:?} does not fit {}", f.name(), f.data_type())
}

fn column<'a>(f: &Field, cells: impl Iterator<Item = &'a Cell>) -> Result<ArrayRef, String> {
    macro_rules! prim {
        ($b:expr, $pat:pat => $v:expr) => {{
            let mut b = $b;
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    $pat => b.append_value($v),
                    other => return Err(mismatch(f, other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    let ns = |s: i64, n: u32| -> Result<i64, String> {
        s.checked_mul(1_000_000_000)
            .and_then(|x| x.checked_add(i64::from(n)))
            .ok_or_else(|| format!("column {}: a timestamp outside 1677–2262", f.name()))
    };
    Ok(match f.data_type() {
        DataType::Boolean => prim!(BooleanBuilder::new(), Cell::Bool(b) => *b),
        DataType::UInt8 => {
            prim!(UInt8Builder::new(), Cell::Int(i) => u8::try_from(*i).map_err(|e| e.to_string())?)
        }
        DataType::Int16 => {
            prim!(Int16Builder::new(), Cell::Int(i) => i16::try_from(*i).map_err(|e| e.to_string())?)
        }
        DataType::Int32 => {
            prim!(Int32Builder::new(), Cell::Int(i) => i32::try_from(*i).map_err(|e| e.to_string())?)
        }
        DataType::Int64 => prim!(Int64Builder::new(), Cell::Int(i) => *i),
        DataType::Float32 => prim!(Float32Builder::new(), Cell::Float(x) => *x as f32),
        DataType::Float64 => prim!(Float64Builder::new(), Cell::Float(x) => *x),
        DataType::Decimal128(p, s) => {
            let mut b = Decimal128Builder::new()
                .with_precision_and_scale(*p, *s)
                .map_err(|e| e.to_string())?;
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Decimal(v, sc) => b.append_value(rescale(*v, *sc, *s as u8)?),
                    Cell::Int(i) => b.append_value(rescale(i128::from(*i), 0, *s as u8)?),
                    other => return Err(mismatch(f, other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Utf8 => prim!(StringBuilder::new(), Cell::Text(t) => t),
        DataType::Binary => prim!(BinaryBuilder::new(), Cell::Bytes(x) => x),
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::new(16);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Uuid(u) => b.append_value(u).map_err(|e| e.to_string())?,
                    other => return Err(mismatch(f, other)),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Date32 => prim!(Date32Builder::new(), Cell::Date(d) => *d),
        DataType::Time64(TimeUnit::Microsecond) => {
            prim!(Time64MicrosecondBuilder::new(), Cell::Time(n) => n / 1000)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            prim!(Time64NanosecondBuilder::new(), Cell::Time(n) => *n)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => prim!(
            TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()),
            Cell::Timestamp(s, n) => s * 1_000_000 + i64::from(*n / 1000)
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => prim!(
            TimestampNanosecondBuilder::new().with_timezone_opt(tz.clone()),
            Cell::Timestamp(s, n) => ns(*s, *n)?
        ),
        other => return Err(format!("column {}: no builder for {other}", f.name())),
    })
}

fn rescale(v: i128, from: u8, to: u8) -> Result<i128, String> {
    if from == to {
        return Ok(v);
    }
    if to > from {
        v.checked_mul(10_i128.pow(u32::from(to - from)))
            .ok_or_else(|| "decimal overflow".to_owned())
    } else {
        Ok(v / 10_i128.pow(u32::from(from - to)))
    }
}

/// The text form of one Arrow cell (the sink's input), `None` for NULL.
pub fn arrow_cell_text(a: &dyn Array, row: usize) -> Result<Option<String>, String> {
    if a.is_null(row) {
        return Ok(None);
    }
    macro_rules! p {
        ($t:ty) => {
            a.as_primitive::<$t>().value(row).to_string()
        };
    }
    Ok(Some(match a.data_type() {
        DataType::Boolean => if a.as_boolean().value(row) { "1" } else { "0" }.to_owned(),
        DataType::Int8 => p!(Int8Type),
        DataType::Int16 => p!(Int16Type),
        DataType::Int32 => p!(Int32Type),
        DataType::Int64 => p!(Int64Type),
        DataType::UInt8 => p!(UInt8Type),
        DataType::UInt16 => p!(UInt16Type),
        DataType::UInt32 => p!(UInt32Type),
        DataType::UInt64 => p!(UInt64Type),
        DataType::Float32 => p!(Float32Type),
        DataType::Float64 => p!(Float64Type),
        DataType::Decimal128(_, s) => decimal_text(
            a.as_primitive::<Decimal128Type>().value(row),
            (*s).max(0) as u8,
        ),
        DataType::Utf8 => a.as_string::<i32>().value(row).to_owned(),
        DataType::LargeUtf8 => a.as_string::<i64>().value(row).to_owned(),
        DataType::Binary => hex(a.as_binary::<i32>().value(row)),
        DataType::LargeBinary => hex(a.as_binary::<i64>().value(row)),
        DataType::FixedSizeBinary(16) => {
            let v = a.as_fixed_size_binary().value(row);
            uuid_text(v.try_into().map_err(|_| "uuid width")?)
        }
        DataType::FixedSizeBinary(_) => hex(a.as_fixed_size_binary().value(row)),
        DataType::Date32 => date_text(i64::from(a.as_primitive::<Date32Type>().value(row))),
        DataType::Time64(TimeUnit::Microsecond) => {
            time_text(a.as_primitive::<Time64MicrosecondType>().value(row) * 1000)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            time_text(a.as_primitive::<Time64NanosecondType>().value(row))
        }
        DataType::Timestamp(unit, _) => {
            let (secs, nanos) = match unit {
                TimeUnit::Second => (a.as_primitive::<TimestampSecondType>().value(row), 0),
                TimeUnit::Millisecond => {
                    let v = a.as_primitive::<TimestampMillisecondType>().value(row);
                    (v.div_euclid(1000), v.rem_euclid(1000) * 1_000_000)
                }
                TimeUnit::Microsecond => {
                    let v = a.as_primitive::<TimestampMicrosecondType>().value(row);
                    (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) * 1000)
                }
                TimeUnit::Nanosecond => {
                    let v = a.as_primitive::<TimestampNanosecondType>().value(row);
                    (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000))
                }
            };
            timestamp_text(secs, nanos as u32)
        }
        other => return Err(format!("no text form for Arrow {other}")),
    }))
}

/// A JSON value as the text [`cast_expr`] converts (`None` is NULL).
pub fn json_to_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_owned()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// An Arrow schema for `columns` under `mapper`.
pub fn arrow_schema(
    table: &str,
    columns: &[Column],
    mapper: &SqlServerTypeMapper,
) -> Result<SchemaRef, SchemaError> {
    let fields = columns
        .iter()
        .map(|c| {
            mapper.to_arrow(&NativeType::new(
                format!("{table}.{}", c.name),
                c.declared.clone(),
                c.nullable,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiberius::numeric::Numeric;
    use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, Time};

    fn to(t: &str, lossy: &[&str]) -> Result<Field, SchemaError> {
        SqlServerTypeMapper::new(Lossy::paths(lossy.iter().copied()))
            .to_arrow(&NativeType::new("dbo.t.c", t, true))
    }

    #[test]
    fn declared_types_from_sys_columns() {
        assert_eq!(declared_type("nvarchar", -1, 0, 0), "nvarchar(max)");
        assert_eq!(declared_type("NVARCHAR", 80, 0, 0), "nvarchar(40)");
        assert_eq!(declared_type("varbinary", 16, 0, 0), "varbinary(16)");
        assert_eq!(declared_type("decimal", 9, 18, 2), "decimal(18,2)");
        assert_eq!(declared_type("datetime2", 8, 27, 7), "datetime2(7)");
        assert_eq!(declared_type("float", 4, 24, 0), "real");
        assert_eq!(declared_type("timestamp", 8, 0, 0), "rowversion");
        assert_eq!(declared_type("int", 4, 10, 0), "int");
    }

    #[test]
    fn the_mapper_follows_the_table() {
        assert_eq!(to("tinyint", &[]).unwrap().data_type(), &DataType::UInt8);
        assert_eq!(
            to("decimal(18,2)", &[]).unwrap().data_type(),
            &DataType::Decimal128(18, 2)
        );
        assert_eq!(
            to("money", &[]).unwrap().data_type(),
            &DataType::Decimal128(19, 4)
        );
        let s = to("nvarchar(40)", &[]).unwrap();
        assert_eq!(
            s.metadata().get(MAX_LENGTH_KEY).map(String::as_str),
            Some("40")
        );
        assert!(
            to("nvarchar(max)", &[])
                .unwrap()
                .metadata()
                .get(MAX_LENGTH_KEY)
                .is_none()
        );
        assert_eq!(
            to("time(7)", &[]).unwrap().data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            to("datetimeoffset(3)", &[]).unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(matches!(
            to("datetime2(7)", &[]),
            Err(SchemaError::Lossy { .. })
        ));
        let accepted = to("datetime2(7)", &["dbo.t.c"]).expect("opted in");
        assert_eq!(
            accepted.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        for t in ["sql_variant", "geography", "hierarchyid", "decimal(40,2)"] {
            assert!(
                matches!(to(t, &[]), Err(SchemaError::Unsupported { .. })),
                "{t}"
            );
        }
        let m = SqlServerTypeMapper::default();
        let back = m.from_arrow(&to("uniqueidentifier", &[]).unwrap()).unwrap();
        assert_eq!(back.type_name, "uniqueidentifier");
        assert!(
            m.from_arrow(&Field::new("u", DataType::UInt64, true))
                .is_err()
        );
    }

    #[test]
    fn dates_round_trip_through_the_civil_calendar() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1, 1, 1), DAY0_0001);
        assert_eq!(days_from_civil(1900, 1, 1), DAY0_1900);
        for d in [-800_000, -1, 0, 59, 11_016, 2_932_896] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
    }

    #[test]
    fn tds_values_become_cells_and_text() {
        // 2026-10-13 is day 739_901 of 0001-01-01.
        let date = Date::new(739_901);
        assert_eq!(
            Cell::from_column(&ColumnData::Date(Some(date)))
                .to_text()
                .as_deref(),
            Some("2026-10-13")
        );
        let t = Time::new(360_001_234_567, 7); // 10:00:00.1234567
        let dt2 = DateTime2::new(date, t);
        assert_eq!(
            Cell::from_column(&ColumnData::DateTime2(Some(dt2)))
                .to_text()
                .as_deref(),
            Some("2026-10-13T10:00:00.1234567Z")
        );
        assert_eq!(
            Cell::from_column(&ColumnData::DateTimeOffset(Some(DateTimeOffset::new(
                dt2, 120
            )))),
            Cell::from_column(&ColumnData::DateTime2(Some(dt2)))
        );
        // datetime: 1900-01-02 00:00:01 (300 ticks).
        assert_eq!(
            Cell::from_column(&ColumnData::DateTime(Some(DateTime::new(1, 300))))
                .to_text()
                .as_deref(),
            Some("1900-01-02T00:00:01Z")
        );
        let n = Numeric::new_with_scale(-1205, 2);
        let c = Cell::from_column(&ColumnData::Numeric(Some(n)));
        assert_eq!(c.to_text().as_deref(), Some("-12.05"));
        assert_eq!(c.to_json(), Value::String("-12.05".into()));
        assert_eq!(decimal_text(5, 3), "0.005");
        assert_eq!(
            Cell::from_column(&ColumnData::Binary(Some(vec![0xde, 0xad].into())))
                .to_text()
                .as_deref(),
            Some("0xDEAD")
        );
        assert_eq!(Cell::from_column(&ColumnData::I32(None)), Cell::Null);
        assert_eq!(Cell::Int(7).to_json(), Value::from(7));
    }

    #[test]
    fn batches_build_and_render_back_to_text() {
        let cols = [
            Column {
                name: "id".into(),
                declared: "bigint".into(),
                nullable: false,
                identity: false,
                generated: false,
            },
            Column {
                name: "amount".into(),
                declared: "decimal(10,2)".into(),
                nullable: true,
                identity: false,
                generated: false,
            },
            Column {
                name: "at".into(),
                declared: "datetimeoffset(7)".into(),
                nullable: true,
                identity: false,
                generated: false,
            },
            Column {
                name: "g".into(),
                declared: "uniqueidentifier".into(),
                nullable: true,
                identity: false,
                generated: false,
            },
        ];
        let schema = arrow_schema("dbo.t", &cols, &SqlServerTypeMapper::default()).expect("schema");
        let rows = vec![
            vec![
                Cell::Int(1),
                Cell::Decimal(1205, 2),
                Cell::Timestamp(1_760_349_600, 123_456_700),
                Cell::Uuid([0x11; 16]),
            ],
            vec![Cell::Int(2), Cell::Null, Cell::Null, Cell::Null],
        ];
        let b = build_batch(&schema, &rows).expect("batch");
        assert_eq!(b.num_rows(), 2);
        let text: Vec<Option<String>> = (0..4)
            .map(|c| arrow_cell_text(b.column(c).as_ref(), 0).expect("text"))
            .collect();
        assert_eq!(
            text,
            vec![
                Some("1".into()),
                Some("12.05".into()),
                Some("2025-10-13T10:00:00.1234567Z".into()),
                Some("11111111-1111-1111-1111-111111111111".into())
            ]
        );
        assert_eq!(
            arrow_cell_text(b.column(1).as_ref(), 1).expect("null"),
            None
        );
        let bad = build_batch(&schema, &[vec![Cell::Text("x".into())]]);
        assert!(bad.expect_err("mismatch").contains("does not fit"));
    }

    #[test]
    fn identifiers_literals_and_casts_are_quoted() {
        assert_eq!(quote_ident("a]b"), "[a]]b]");
        assert_eq!(quote_literal("it's"), "N'it''s'");
        assert_eq!(cast_expr("@P1", "bigint"), "CAST(@P1 AS bigint)");
        assert_eq!(
            cast_expr("s.[c1]", "datetime2(7)"),
            "CONVERT(datetime2(7), s.[c1], 127)"
        );
        assert_eq!(
            cast_expr("x", "varbinary(max)"),
            "CONVERT(varbinary(max), x, 1)"
        );
        assert_eq!(json_to_text(&serde_json::json!(true)).as_deref(), Some("1"));
        assert_eq!(
            json_to_text(&serde_json::json!({"a": 1})).as_deref(),
            Some("{\"a\":1}")
        );
    }
}
