//! Postgres types: the family's [`TypeMapper`] (CN2 Task 5's contract) and
//! the value codecs every path uses.
//!
//! | Postgres (`format_type`) | Arrow | Notes |
//! |---|---|---|
//! | `boolean` | `Boolean` | |
//! | `smallint`, `integer`, `bigint` | `Int16`, `Int32`, `Int64` | |
//! | `real`, `double precision` | `Float32`, `Float64` | |
//! | `numeric(p,s)`, p ≤ 38 | `Decimal128(p,s)` | p > 38 is lossy (R5.2) |
//! | `numeric` (unconstrained) | `Utf8` + `loams.pg.type = numeric` | the exact decimal text; no precision is lost (R10.4) |
//! | `character varying(n)`, `character(n)` | `Utf8` + `loams.max_length` | |
//! | `character varying`, `text`, `name` | `Utf8` | |
//! | `bytea` | `Binary` | |
//! | `date` | `Date32` | |
//! | `time[(p)] without time zone` | `Time64(µs)` | |
//! | `timestamp[(p)] with time zone` | `Timestamp(µs, "UTC")` | |
//! | `timestamp[(p)] without time zone` | `Timestamp(µs, "UTC")` | lossy: refused unless listed in `lossy` (R5.2) |
//! | `uuid` | `FixedSizeBinary(16)` + `arrow.uuid` | |
//! | `json`, `jsonb` | `Utf8` + `arrow.json` | |
//! | `vector(n)` (pgvector) | `FixedSizeList<Float32>(n)` | |
//! | `vector` (no dimension) | `List<Float32>` | |
//!
//! Anything else (arrays, `interval`, `inet`, ranges, user enums) is
//! `Unsupported`, which pauses the instance (R5.2: never guessed).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use loams_connector_sdk::schema::arrow::{EXTENSION_NAME_KEY, SqlTypeMapper};
use loams_connector_sdk::schema::{Lossy, NativeType, PATH_KEY, SchemaError, TypeMapper};

/// Field metadata naming the Postgres type a `Utf8` stands for when Arrow has
/// no exact equivalent (`numeric` without a precision).
pub const PG_TYPE_KEY: &str = "loams.pg.type";

/// Well-known type OIDs (`pg_type.dat`). pgvector's `vector` has no fixed
/// OID; the codecs learn it per connection.
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// The Postgres family's mapper: Postgres type names (as `format_type`
/// renders them) to Arrow and back. It extends the SDK's
/// [`SqlTypeMapper`] (R5.2) with `numeric` without a precision, precision
/// modifiers on `time`/`timestamp`, unbounded `character varying`, `name`
/// and pgvector's `vector`.
#[derive(Debug, Clone, Default)]
pub struct PgTypeMapper {
    sql: SqlTypeMapper,
}

impl PgTypeMapper {
    pub fn new(lossy: Lossy) -> Self {
        Self {
            sql: SqlTypeMapper::new(lossy).signed_only(),
        }
    }
}

/// `vector(3)` → `Some(Some(3))`, `vector` → `Some(None)`, else `None`.
fn vector_dim(type_name: &str) -> Option<Option<i32>> {
    let t = type_name.trim().to_ascii_lowercase();
    let t = t.strip_prefix("public.").unwrap_or(&t);
    if t == "vector" {
        return Some(None);
    }
    let inner = t.strip_prefix("vector(")?.strip_suffix(')')?;
    inner.trim().parse::<i32>().ok().map(Some)
}

/// Drop a precision modifier from `time(3)…`/`timestamp(6)…`, which the
/// SDK's mapper does not take (microseconds hold every Postgres precision).
fn strip_time_precision(type_name: &str) -> Option<String> {
    let t = type_name.trim().to_ascii_lowercase();
    for head in ["timestamp", "time"] {
        if let Some(rest) = t.strip_prefix(head)
            && let Some(rest) = rest.trim_start().strip_prefix('(')
            && let Some(close) = rest.find(')')
            && rest[..close].trim().parse::<u32>().is_ok()
        {
            return Some(format!("{head}{}", &rest[close + 1..]));
        }
    }
    None
}

fn vector_item() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float32, false))
}

impl TypeMapper for PgTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let lower = native.type_name.trim().to_ascii_lowercase();
        let md = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            let mut m: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            m.insert(PATH_KEY.to_owned(), native.path.clone());
            m
        };
        let field = |dt: DataType, m: HashMap<String, String>| {
            Ok(Field::new(native.column(), dt, native.nullable).with_metadata(m))
        };
        if let Some(dim) = vector_dim(&lower) {
            return match dim {
                Some(n) if n > 0 => field(DataType::FixedSizeList(vector_item(), n), md(&[])),
                Some(_) => Err(SchemaError::Unsupported {
                    path: native.path.clone(),
                    native: native.type_name.clone(),
                }),
                None => field(DataType::List(vector_item()), md(&[])),
            };
        }
        match lower.as_str() {
            "numeric" | "decimal" => {
                return field(DataType::Utf8, md(&[(PG_TYPE_KEY, "numeric")]));
            }
            "character varying" | "varchar" | "name" | "citext" => {
                return field(DataType::Utf8, md(&[]));
            }
            "character" | "char" | "bpchar" => {
                // `character` without a length is `character(1)`.
                let mut m = md(&[]);
                m.insert(
                    loams_connector_sdk::schema::MAX_LENGTH_KEY.to_owned(),
                    "1".to_owned(),
                );
                return field(DataType::Utf8, m);
            }
            _ => {}
        }
        if let Some(stripped) = strip_time_precision(&lower) {
            return self.sql.to_arrow(&NativeType {
                type_name: stripped,
                ..native.clone()
            });
        }
        self.sql.to_arrow(native)
    }

    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError> {
        let path = field
            .metadata()
            .get(PATH_KEY)
            .cloned()
            .unwrap_or_else(|| field.name().clone());
        let type_name = match field.data_type() {
            DataType::FixedSizeList(item, n) if item.data_type() == &DataType::Float32 => {
                format!("vector({n})")
            }
            DataType::List(item) if item.data_type() == &DataType::Float32 => "vector".to_owned(),
            DataType::Utf8 | DataType::LargeUtf8
                if field.metadata().get(PG_TYPE_KEY).map(String::as_str) == Some("numeric") =>
            {
                "numeric".to_owned()
            }
            _ => return self.sql.from_arrow(field),
        };
        Ok(NativeType {
            path,
            type_name,
            nullable: field.is_nullable(),
        })
    }
}

/// How a column's values are encoded and decoded, from its type. Every path
/// (binary `COPY`, binary result rows, pgoutput text) agrees on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    /// `numeric(p,s)` as `Decimal128(p,s)`.
    Decimal {
        precision: u8,
        scale: i8,
    },
    /// `numeric` without a precision, as its exact text.
    NumericText,
    Text,
    Json,
    Jsonb,
    Bytea,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Uuid,
    /// pgvector's `vector`, with its dimension when declared.
    Vector(Option<i32>),
}

impl ColKind {
    /// The kind for an Arrow field as [`PgTypeMapper`] maps it, with the
    /// Postgres type name to tell `json` from `jsonb`.
    pub fn of(field: &Field, type_name: &str) -> Option<ColKind> {
        let lower = type_name.trim().to_ascii_lowercase();
        Some(match field.data_type() {
            DataType::Boolean => ColKind::Bool,
            DataType::Int16 => ColKind::Int2,
            DataType::Int32 => ColKind::Int4,
            DataType::Int64 => ColKind::Int8,
            DataType::Float32 => ColKind::Float4,
            DataType::Float64 => ColKind::Float8,
            DataType::Decimal128(p, s) => ColKind::Decimal {
                precision: *p,
                scale: *s,
            },
            DataType::Utf8 => {
                if field.metadata().get(PG_TYPE_KEY).map(String::as_str) == Some("numeric") {
                    ColKind::NumericText
                } else if field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str)
                    == Some("arrow.json")
                {
                    if lower == "jsonb" {
                        ColKind::Jsonb
                    } else {
                        ColKind::Json
                    }
                } else {
                    ColKind::Text
                }
            }
            DataType::Binary => ColKind::Bytea,
            DataType::Date32 => ColKind::Date,
            DataType::Time64(TimeUnit::Microsecond) => ColKind::Time,
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if lower.contains("with time zone") || lower == "timestamptz" {
                    ColKind::Timestamptz
                } else {
                    ColKind::Timestamp
                }
            }
            DataType::FixedSizeBinary(16) => ColKind::Uuid,
            DataType::FixedSizeList(_, n) => ColKind::Vector(Some(*n)),
            DataType::List(_) => ColKind::Vector(None),
            _ => return None,
        })
    }
}

/// The `format_type`-style name of a type known by OID and modifier, as a
/// pgoutput `Relation` message gives them. `custom` holds the names of
/// types without a fixed OID (pgvector's `vector`), learnt from the
/// catalog or from pgoutput `Type` messages.
pub fn type_name(oid: u32, typmod: i32, custom: &HashMap<u32, String>) -> String {
    let with_len = |base: &str| {
        if typmod >= 4 {
            format!("{base}({})", typmod - 4)
        } else {
            base.to_owned()
        }
    };
    match oid {
        oid::BOOL => "boolean".to_owned(),
        oid::BYTEA => "bytea".to_owned(),
        oid::NAME => "name".to_owned(),
        oid::INT8 => "bigint".to_owned(),
        oid::INT2 => "smallint".to_owned(),
        oid::INT4 => "integer".to_owned(),
        oid::TEXT => "text".to_owned(),
        oid::JSON => "json".to_owned(),
        oid::FLOAT4 => "real".to_owned(),
        oid::FLOAT8 => "double precision".to_owned(),
        oid::BPCHAR => with_len("character"),
        oid::VARCHAR => with_len("character varying"),
        oid::DATE => "date".to_owned(),
        oid::TIME => "time without time zone".to_owned(),
        oid::TIMESTAMP => "timestamp without time zone".to_owned(),
        oid::TIMESTAMPTZ => "timestamp with time zone".to_owned(),
        oid::NUMERIC if typmod >= 4 => {
            let m = typmod - 4;
            format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff)
        }
        oid::NUMERIC => "numeric".to_owned(),
        oid::UUID => "uuid".to_owned(),
        oid::JSONB => "jsonb".to_owned(),
        other => match custom.get(&other) {
            // pgvector's typmod is the dimension itself.
            Some(name) if name == "vector" && typmod > 0 => format!("vector({typmod})"),
            Some(name) => name.clone(),
            None => format!("oid {other}"),
        },
    }
}

/// Days from 1970-01-01 to 2000-01-01, Postgres's epoch.
pub const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds from 1970-01-01 to 2000-01-01.
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// The civil date of a day count since 1970-01-01 (H. Hinnant's algorithm).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `YYYY-MM-DD` of a day count since 1970-01-01.
pub fn format_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `HH:MM:SS.ffffff` of microseconds since midnight.
pub fn format_time(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    format!(
        "{:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// `YYYY-MM-DD HH:MM:SS.ffffff` of microseconds since 1970-01-01, plus
/// `+00` when `utc`.
pub fn format_timestamp(micros: i64, utc: bool) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let rest = micros.rem_euclid(86_400_000_000);
    let mut s = format!("{} {}", format_date(days), format_time(rest));
    if utc {
        s.push_str("+00");
    }
    s
}

/// A UUID's canonical hyphenated text.
pub fn format_uuid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    if hex.len() != 32 {
        return hex;
    }
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Decode Postgres's binary `numeric` to its exact decimal text (`NaN`,
/// `Infinity` and `-Infinity` included).
pub fn numeric_binary_to_text(raw: &[u8]) -> Result<String, String> {
    let rd = |i: usize| -> Result<u16, String> {
        raw.get(i..i + 2)
            .map(|b| u16::from_be_bytes([b[0], b[1]]))
            .ok_or_else(|| "truncated numeric".to_owned())
    };
    let ndigits = rd(0)? as usize;
    let weight = rd(2)? as i16 as i32;
    let sign = rd(4)?;
    let dscale = rd(6)? as usize;
    match sign {
        0xC000 => return Ok("NaN".to_owned()),
        0xD000 => return Ok("Infinity".to_owned()),
        0xF000 => return Ok("-Infinity".to_owned()),
        0x0000 | 0x4000 => {}
        other => return Err(format!("numeric sign {other:#x}")),
    }
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let d = rd(8 + 2 * i)?;
        if d > 9999 {
            return Err("numeric digit out of range".to_owned());
        }
        digits.push(d);
    }
    // Integer part: base-10000 digits at weights weight..=0.
    let mut int = String::new();
    if weight < 0 {
        int.push('0');
    } else {
        for w in 0..=weight {
            let d = digits.get(w as usize).copied().unwrap_or(0);
            if int.is_empty() {
                int.push_str(&d.to_string());
            } else {
                int.push_str(&format!("{d:04}"));
            }
        }
    }
    let mut frac = String::new();
    if dscale > 0 {
        // Fractional base-10000 digits at weights -1, -2, …; the digit of
        // weight `w` is at index `weight - w`.
        let mut w = -1;
        while frac.len() < dscale {
            let i = weight - w;
            let d = if i >= 0 {
                digits.get(i as usize).copied().unwrap_or(0)
            } else {
                0
            };
            frac.push_str(&format!("{d:04}"));
            w -= 1;
        }
        frac.truncate(dscale);
    }
    let mut s = String::new();
    if sign == 0x4000 {
        s.push('-');
    }
    s.push_str(&int);
    if dscale > 0 {
        s.push('.');
        s.push_str(&frac);
    }
    Ok(s)
}

/// Parse a decimal's text to the unscaled `i128` of `Decimal128(_, scale)`.
/// Refuses values with more fractional digits than `scale` would keep only
/// when they are not zeros.
pub fn decimal_text_to_i128(text: &str, scale: i8) -> Result<i128, String> {
    let t = text.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int, frac) = t.split_once('.').unwrap_or((t, ""));
    if int.is_empty() && frac.is_empty() {
        return Err(format!("not a decimal: {text}"));
    }
    let scale = scale.max(0) as usize;
    let mut digits = String::from(int);
    let (keep, drop) = frac.split_at(frac.len().min(scale));
    if drop.chars().any(|c| c != '0') {
        return Err(format!("{text} has more than {scale} fractional digits"));
    }
    digits.push_str(keep);
    digits.extend(std::iter::repeat_n('0', scale - keep.len()));
    let v: i128 = if digits.is_empty() {
        0
    } else {
        digits
            .parse()
            .map_err(|_| format!("not a decimal: {text}"))?
    };
    Ok(if neg { -v } else { v })
}

/// The text of `Decimal128` unscaled `v` at `scale`.
pub fn i128_to_decimal_text(v: i128, scale: i8) -> String {
    if scale <= 0 {
        let mut s = v.to_string();
        s.extend(std::iter::repeat_n('0', (-scale) as usize));
        return s;
    }
    let scale = scale as usize;
    let neg = v < 0;
    let mut digits = v.unsigned_abs().to_string();
    if digits.len() <= scale {
        digits = format!("{}{digits}", "0".repeat(scale - digits.len() + 1));
    }
    let (int, frac) = digits.split_at(digits.len() - scale);
    format!("{}{int}.{frac}", if neg { "-" } else { "" })
}

/// pgvector's binary form: `u16` dimension, `u16` unused, then `f32`s.
pub fn vector_binary(raw: &[u8]) -> Result<Vec<f32>, String> {
    if raw.len() < 4 {
        return Err("truncated vector".to_owned());
    }
    let dim = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    if raw.len() != 4 + 4 * dim {
        return Err(format!("vector of dimension {dim} has {} bytes", raw.len()));
    }
    Ok(raw[4..]
        .chunks_exact(4)
        .map(|c| f32::from_be_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// pgvector's text form `[1,2.5,3]`.
pub fn vector_text(text: &str) -> Result<Vec<f32>, String> {
    let inner = text
        .trim()
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(|| format!("not a vector: {text}"))?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|x| {
            x.trim()
                .parse::<f32>()
                .map_err(|_| format!("not a vector element: {x}"))
        })
        .collect()
}

/// The text form pgvector's input function accepts.
pub fn format_vector(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| x.to_string()).collect();
    format!("[{}]", parts.join(","))
}

/// A value of pgoutput's text format as JSON: numbers and booleans as
/// themselves, `json`/`jsonb` embedded, vectors as arrays, and everything
/// else (numeric, timestamps, bytea's `\x…`) as Postgres's own text, so no
/// precision is lost.
pub fn text_to_json(kind: Option<ColKind>, text: &str) -> serde_json::Value {
    use serde_json::Value;
    let s = || Value::String(text.to_owned());
    match kind {
        Some(ColKind::Bool) => match text {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            _ => s(),
        },
        Some(ColKind::Int2 | ColKind::Int4 | ColKind::Int8) => {
            text.parse::<i64>().map(Value::from).unwrap_or_else(|_| s())
        }
        Some(ColKind::Float4 | ColKind::Float8) => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(s),
        Some(ColKind::Json | ColKind::Jsonb) => serde_json::from_str(text).unwrap_or_else(|_| s()),
        Some(ColKind::Vector(_)) => match vector_text(text) {
            Ok(v) => Value::Array(
                v.into_iter()
                    .map(|x| {
                        serde_json::Number::from_f64(f64::from(x))
                            .map(Value::Number)
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            ),
            Err(_) => s(),
        },
        _ => s(),
    }
}

/// A JSON value as the text a Postgres input function takes (`$n::type`
/// with a text parameter): strings as they are, numbers and booleans in
/// their JSON spelling, arrays and objects as JSON text (which `json`,
/// `jsonb` and pgvector's `vector` all accept). `null` is SQL NULL.
pub fn json_to_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// Quote an identifier (`"a""b"`).
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Quote a literal (`'it''s'`), for the few statements that cannot take
/// parameters (`COPY (…) TO`, replication commands).
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::schema::MAX_LENGTH_KEY;

    fn arrow(type_name: &str) -> Result<Field, SchemaError> {
        PgTypeMapper::default().to_arrow(&NativeType::new("public.t.c", type_name, true))
    }

    #[test]
    fn pg_types_map_to_arrow() {
        let cases: &[(&str, DataType)] = &[
            ("boolean", DataType::Boolean),
            ("smallint", DataType::Int16),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("numeric(12,2)", DataType::Decimal128(12, 2)),
            ("numeric", DataType::Utf8),
            ("character varying", DataType::Utf8),
            ("character varying(20)", DataType::Utf8),
            ("text", DataType::Utf8),
            ("name", DataType::Utf8),
            ("bytea", DataType::Binary),
            ("date", DataType::Date32),
            (
                "time without time zone",
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                "time(3) without time zone",
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                "timestamp with time zone",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "timestamp(3) with time zone",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            ("uuid", DataType::FixedSizeBinary(16)),
            ("jsonb", DataType::Utf8),
            ("vector(3)", DataType::FixedSizeList(vector_item(), 3)),
            ("vector", DataType::List(vector_item())),
        ];
        for (name, dt) in cases {
            let f = arrow(name).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(f.data_type(), dt, "{name}");
            assert_eq!(f.name(), "c");
        }
        assert_eq!(
            arrow("character varying(20)")
                .expect("varchar")
                .metadata()
                .get(MAX_LENGTH_KEY)
                .map(String::as_str),
            Some("20")
        );
    }

    #[test]
    fn unsupported_and_lossy_are_refused() {
        assert!(matches!(
            arrow("integer[]"),
            Err(SchemaError::Unsupported { .. })
        ));
        assert!(matches!(
            arrow("interval"),
            Err(SchemaError::Unsupported { .. })
        ));
        assert!(matches!(
            arrow("timestamp without time zone"),
            Err(SchemaError::Lossy { .. })
        ));
        let ok = PgTypeMapper::new(Lossy::paths(["public.t.c"])).to_arrow(&NativeType::new(
            "public.t.c",
            "timestamp(6) without time zone",
            true,
        ));
        assert!(ok.is_ok());
    }

    #[test]
    fn vector_and_numeric_round_trip_through_from_arrow() {
        let m = PgTypeMapper::default();
        for name in ["vector(3)", "vector", "numeric", "integer", "jsonb", "uuid"] {
            let f = arrow(name).expect(name);
            let back = m.from_arrow(&f).expect(name);
            let again = m.to_arrow(&back).expect(name);
            assert_eq!(again.data_type(), f.data_type(), "{name}");
        }
        assert_eq!(
            m.from_arrow(&arrow("vector(3)").expect("vector"))
                .expect("vector")
                .type_name,
            "vector(3)"
        );
    }

    #[test]
    fn relation_oids_render_like_format_type() {
        let mut custom = HashMap::new();
        custom.insert(16_400, "vector".to_owned());
        assert_eq!(
            type_name(oid::VARCHAR, 24, &custom),
            "character varying(20)"
        );
        assert_eq!(type_name(oid::VARCHAR, -1, &custom), "character varying");
        assert_eq!(
            type_name(oid::NUMERIC, ((12 << 16) | 2) + 4, &custom),
            "numeric(12,2)"
        );
        assert_eq!(type_name(oid::NUMERIC, -1, &custom), "numeric");
        assert_eq!(type_name(16_400, 3, &custom), "vector(3)");
        assert_eq!(type_name(99_999, -1, &custom), "oid 99999");
    }

    #[test]
    fn numeric_binary_decodes_exactly() {
        // 12345.678: ndigits 3, weight 1, sign +, dscale 3; digits 1, 2345, 6780.
        let raw = [0, 3, 0, 1, 0, 0, 0, 3, 0, 1, 0x09, 0x29, 0x1a, 0x7c];
        assert_eq!(numeric_binary_to_text(&raw).expect("numeric"), "12345.678");
        // -0.05: ndigits 1, weight -1, sign -, dscale 2; digit 500.
        let raw = [0, 1, 0xff, 0xff, 0x40, 0, 0, 2, 0x01, 0xf4];
        assert_eq!(numeric_binary_to_text(&raw).expect("numeric"), "-0.05");
        // 0 with scale 2.
        let raw = [0, 0, 0, 0, 0, 0, 0, 2];
        assert_eq!(numeric_binary_to_text(&raw).expect("numeric"), "0.00");
        // 10000 (weight 1, digits [1]).
        let raw = [0, 1, 0, 1, 0, 0, 0, 0, 0, 1];
        assert_eq!(numeric_binary_to_text(&raw).expect("numeric"), "10000");
        let nan = [0, 0, 0, 0, 0xC0, 0, 0, 0];
        assert_eq!(numeric_binary_to_text(&nan).expect("nan"), "NaN");
    }

    #[test]
    fn decimals_convert_both_ways() {
        assert_eq!(decimal_text_to_i128("123.45", 2), Ok(12_345));
        assert_eq!(decimal_text_to_i128("-0.5", 2), Ok(-50));
        assert_eq!(decimal_text_to_i128("7", 3), Ok(7_000));
        assert_eq!(decimal_text_to_i128("1.230", 2), Ok(123));
        assert!(decimal_text_to_i128("1.234", 2).is_err());
        assert_eq!(i128_to_decimal_text(12_345, 2), "123.45");
        assert_eq!(i128_to_decimal_text(-5, 2), "-0.05");
        assert_eq!(i128_to_decimal_text(7, 0), "7");
    }

    #[test]
    fn dates_and_times_format_like_postgres() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(i64::from(PG_EPOCH_DAYS)), "2000-01-01");
        assert_eq!(format_date(-1), "1969-12-31");
        assert_eq!(format_date(19_723), "2024-01-01");
        assert_eq!(format_time(3_723_000_001), "01:02:03.000001");
        assert_eq!(
            format_timestamp(PG_EPOCH_MICROS + 1_500_000, true),
            "2000-01-01 00:00:01.500000+00"
        );
        assert_eq!(
            format_uuid(&[0x12; 16]),
            "12121212-1212-1212-1212-121212121212"
        );
    }

    #[test]
    fn vectors_parse_both_forms() {
        let mut raw = vec![0, 3, 0, 0];
        for x in [1.0f32, 2.5, -3.0] {
            raw.extend_from_slice(&x.to_be_bytes());
        }
        assert_eq!(vector_binary(&raw).expect("binary"), vec![1.0, 2.5, -3.0]);
        assert_eq!(
            vector_text("[1,2.5,-3]").expect("text"),
            vec![1.0, 2.5, -3.0]
        );
        assert_eq!(format_vector(&[1.0, 2.5]), "[1,2.5]");
        assert!(vector_binary(&raw[..7]).is_err());
    }

    #[test]
    fn pgoutput_text_becomes_json() {
        use serde_json::json;
        assert_eq!(text_to_json(Some(ColKind::Bool), "t"), json!(true));
        assert_eq!(text_to_json(Some(ColKind::Int8), "42"), json!(42));
        assert_eq!(text_to_json(Some(ColKind::Float8), "1.5"), json!(1.5));
        assert_eq!(
            text_to_json(Some(ColKind::NumericText), "1.10"),
            json!("1.10")
        );
        assert_eq!(
            text_to_json(Some(ColKind::Jsonb), r#"{"a": 1}"#),
            json!({"a": 1})
        );
        assert_eq!(
            text_to_json(Some(ColKind::Vector(Some(2))), "[1,2]"),
            json!([1.0, 2.0])
        );
        assert_eq!(
            json_to_text(&json!([1.0, 2.0])).as_deref(),
            Some("[1.0,2.0]")
        );
        assert_eq!(json_to_text(&json!(null)), None);
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }
}
