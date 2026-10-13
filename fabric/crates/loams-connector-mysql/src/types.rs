//! MySQL column types: their Arrow mapping ([`MyTypeMapper`], over the SDK's
//! [`SqlTypeMapper`]), and one canonical JSON form for every value, whether
//! it came from a text-protocol `SELECT` (batch reads, the snapshot) or from
//! a binlog row image, so the same row reads the same either way.
//!
//! | MySQL (`COLUMN_TYPE`) | Arrow | JSON in `change` events |
//! |---|---|---|
//! | `tinyint`…`bigint` (`unsigned`) | `Int8`…`Int64` (`UInt8`…`UInt64`); `mediumint` → 32 bits | number |
//! | `float`, `double` | `Float32`, `Float64` | number |
//! | `decimal(p,s)` | `Decimal128(p,s)`; p > 38 lossy (R5.2) | string (exact) |
//! | `date` | `Date32` | `"YYYY-MM-DD"` |
//! | `datetime(n)` | `Timestamp(µs, "UTC")`, lossy: no zone (R5.2) | `"YYYY-MM-DD HH:MM:SS[.ffffff]"` |
//! | `timestamp(n)` | `Timestamp(µs, "UTC")` | `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` |
//! | `time(n)` | `Duration(µs)` (MySQL's range is ±838 h) | `"[-]HH:MM:SS[.ffffff]"` |
//! | `year` | `Int16` | number |
//! | `char(n)`, `varchar(n)`, `*text` | `Utf8` (+ `loams.max_length`) | string |
//! | `binary(n)`, `varbinary(n)`, `*blob` | `Binary` | base64 string |
//! | `bit(n)` | `UInt64` | number |
//! | `enum(…)`, `set(…)` | `Utf8` | string (`set`: comma-separated) |
//! | `json` | `Utf8` + `arrow.json` | the JSON value |
//! | spatial types, `vector` | refused: `Unsupported` (R5.2, never guessed) | — |
//!
//! Text values must be UTF-8 (`utf8mb4`, `utf8mb3`, `ascii`); a value that
//! is not is `Poison`, never coerced (Review Focus 7).

use std::collections::HashMap;

use arrow_schema::{DataType, Field, TimeUnit};
use base64::Engine as _;
use loams_connector_sdk::schema::arrow::{EXTENSION_NAME_KEY, SqlTypeMapper};
use loams_connector_sdk::schema::{
    Lossy, MAX_LENGTH_KEY, NativeType, PATH_KEY, SchemaError, TypeMapper,
};
use mysql_common::Value as MyValue;
use mysql_common::binlog::value::BinlogValue;
use serde_json::{Number, Value};

/// The metadata key recording a MySQL type Arrow cannot name exactly.
pub const MYSQL_TYPE_KEY: &str = "loams.mysql.type";

/// `` `ident` `` with backticks doubled.
pub fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// A string as a literal that needs no escaping and survives
/// `NO_BACKSLASH_ESCAPES`: `CONVERT(X'…' USING utf8mb4)`.
pub fn quote_str(s: &str) -> String {
    format!("CONVERT({} USING utf8mb4)", hex_literal(s.as_bytes()))
}

/// Bytes as `X'…'`.
pub fn hex_literal(b: &[u8]) -> String {
    let mut out = String::with_capacity(3 + 2 * b.len());
    out.push_str("X'");
    for x in b {
        out.push_str(&format!("{x:02X}"));
    }
    out.push('\'');
    out
}

/// What a column holds, from its `COLUMN_TYPE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Int {
        unsigned: bool,
        bits: u8,
    },
    Float,
    Double,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    /// `max` is `char(n)`/`varchar(n)`'s n.
    Text {
        max: Option<u32>,
    },
    Binary,
    Bit,
    Enum(Vec<String>),
    Set(Vec<String>),
    Json,
    /// Spatial types, `vector` and anything unknown.
    Other(String),
}

/// A column: its name, its `COLUMN_TYPE` (lowercase, as
/// `information_schema.COLUMNS` writes it) and its nullability.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into().to_ascii_lowercase(),
            nullable,
        }
    }

    pub fn kind(&self) -> Kind {
        kind_of(&self.column_type)
    }
}

/// `'a','b'` → `[a, b]` (MySQL doubles a quote inside a value).
fn quoted_values(args: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = args.chars().peekable();
    while let Some(c) = chars.next() {
        match (in_q, c) {
            (false, '\'') => in_q = true,
            (true, '\'') if chars.peek() == Some(&'\'') => {
                chars.next();
                cur.push('\'');
            }
            (true, '\'') => {
                in_q = false;
                out.push(std::mem::take(&mut cur));
            }
            (true, c) => cur.push(c),
            (false, _) => {}
        }
    }
    out
}

/// The [`Kind`] of a `COLUMN_TYPE` (`bigint unsigned`, `varchar(255)`,
/// `enum('a','b')`, `decimal(10,2)`, `datetime(3)`, …).
pub fn kind_of(column_type: &str) -> Kind {
    let t = column_type.trim().to_ascii_lowercase();
    let (head, args, tail) = match t.find('(') {
        Some(open) => match t.rfind(')') {
            Some(close) if close > open => (&t[..open], &t[open + 1..close], &t[close + 1..]),
            _ => (t.as_str(), "", ""),
        },
        None => (t.as_str(), "", ""),
    };
    let words: Vec<&str> = head
        .split_whitespace()
        .chain(tail.split_whitespace())
        .collect();
    let unsigned = words.contains(&"unsigned");
    let base = words.first().copied().unwrap_or("");
    let nums: Vec<u32> = args
        .split(',')
        .filter_map(|a| a.trim().parse().ok())
        .collect();
    let int = |bits| Kind::Int { unsigned, bits };
    match base {
        "tinyint" | "bool" | "boolean" => int(8),
        "smallint" => int(16),
        "mediumint" => int(24),
        "int" | "integer" => int(32),
        "bigint" => int(64),
        "float" if nums.first().is_some_and(|p| *p > 24) => Kind::Double,
        "float" => Kind::Float,
        "double" | "real" => Kind::Double,
        "decimal" | "numeric" | "dec" | "fixed" => Kind::Decimal {
            precision: nums.first().copied().unwrap_or(10),
            scale: nums.get(1).copied().unwrap_or(0),
        },
        "date" => Kind::Date,
        "datetime" => Kind::DateTime,
        "timestamp" => Kind::Timestamp,
        "time" => Kind::Time,
        "year" => Kind::Year,
        "char" | "varchar" | "nchar" | "nvarchar" => Kind::Text {
            max: nums.first().copied(),
        },
        "tinytext" | "text" | "mediumtext" | "longtext" => Kind::Text { max: None },
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => Kind::Binary,
        "bit" => Kind::Bit,
        "enum" => Kind::Enum(quoted_values(args)),
        "set" => Kind::Set(quoted_values(args)),
        "json" => Kind::Json,
        other => Kind::Other(other.to_owned()),
    }
}

/// The family's [`TypeMapper`]: MySQL's `COLUMN_TYPE`s (see the module's
/// table), the SDK's shared mapper for decimals and `datetime`.
#[derive(Debug, Clone, Default)]
pub struct MyTypeMapper {
    lossy: Lossy,
}

impl MyTypeMapper {
    pub fn new(lossy: Lossy) -> Self {
        Self { lossy }
    }
}

fn md(pairs: &[(&str, String)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

impl TypeMapper for MyTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let sql = SqlTypeMapper::new(self.lossy.clone());
        let mut metadata = md(&[(PATH_KEY, native.path.clone())]);
        let field = |dt: DataType, metadata: HashMap<String, String>| {
            Ok(Field::new(native.column(), dt, native.nullable).with_metadata(metadata))
        };
        match kind_of(&native.type_name) {
            Kind::Int { unsigned, bits } => field(
                match (unsigned, bits) {
                    (false, 8) => DataType::Int8,
                    (false, 16) => DataType::Int16,
                    (false, 24 | 32) => DataType::Int32,
                    (false, _) => DataType::Int64,
                    (true, 8) => DataType::UInt8,
                    (true, 16) => DataType::UInt16,
                    (true, 24 | 32) => DataType::UInt32,
                    (true, _) => DataType::UInt64,
                },
                metadata,
            ),
            Kind::Float => field(DataType::Float32, metadata),
            Kind::Double => field(DataType::Float64, metadata),
            Kind::Decimal { precision, scale } => sql.to_arrow(&NativeType::new(
                native.path.clone(),
                format!("decimal({precision},{scale})"),
                native.nullable,
            )),
            Kind::Date => field(DataType::Date32, metadata),
            Kind::DateTime => sql.to_arrow(&NativeType::new(
                native.path.clone(),
                "datetime",
                native.nullable,
            )),
            Kind::Timestamp => field(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                metadata,
            ),
            Kind::Time => {
                metadata.insert(MYSQL_TYPE_KEY.to_owned(), "time".to_owned());
                field(DataType::Duration(TimeUnit::Microsecond), metadata)
            }
            Kind::Year => field(DataType::Int16, metadata),
            Kind::Text { max } => {
                if let Some(n) = max {
                    metadata.insert(MAX_LENGTH_KEY.to_owned(), n.to_string());
                }
                field(DataType::Utf8, metadata)
            }
            Kind::Binary => field(DataType::Binary, metadata),
            Kind::Bit => field(DataType::UInt64, metadata),
            Kind::Enum(_) | Kind::Set(_) => {
                metadata.insert(MYSQL_TYPE_KEY.to_owned(), native.type_name.clone());
                field(DataType::Utf8, metadata)
            }
            Kind::Json => {
                metadata.insert(EXTENSION_NAME_KEY.to_owned(), "arrow.json".to_owned());
                field(DataType::Utf8, metadata)
            }
            Kind::Other(_) => Err(SchemaError::Unsupported {
                path: native.path.clone(),
                native: native.type_name.clone(),
            }),
        }
    }

    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError> {
        // MySQL has unsigned integers, so the shared mapper's rules hold.
        SqlTypeMapper::new(self.lossy.clone()).from_arrow(field)
    }
}

// ---------------------------------------------------------------- dates --

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month, day)` of a day count since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn frac(us: u32) -> String {
    if us == 0 {
        String::new()
    } else {
        format!(".{us:06}")
    }
}

/// `"YYYY-MM-DD HH:MM:SS[.ffffff]"`.
pub fn fmt_datetime(y: u32, mo: u32, d: u32, h: u32, mi: u32, s: u32, us: u32) -> String {
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}{}", frac(us))
}

/// `"YYYY-MM-DDTHH:MM:SS[.ffffff]Z"` of Unix seconds and microseconds.
pub fn fmt_timestamp(secs: i64, us: u32) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}{}Z",
        rem / 3600,
        (rem / 60) % 60,
        rem % 60,
        frac(us)
    )
}

/// `"[-]HH:MM:SS[.ffffff]"`.
pub fn fmt_time(negative: bool, hours: u32, mi: u32, s: u32, us: u32) -> String {
    format!(
        "{}{hours:02}:{mi:02}:{s:02}{}",
        if negative { "-" } else { "" },
        frac(us)
    )
}

/// The microseconds of a fraction's digits (`"12"` → 120000).
fn parse_frac(f: &str) -> Option<u32> {
    if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u32 = f.parse().ok()?;
    Some(n * 10u32.pow(6 - f.len() as u32))
}

/// `(y, mo, d, h, mi, s, us)` of `"YYYY-MM-DD[ |T]HH:MM:SS[.f]"`; the date
/// alone is midnight.
pub fn parse_datetime(s: &str) -> Option<(u32, u32, u32, u32, u32, u32, u32)> {
    let s = s.trim_end_matches('Z');
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let mut dp = date.split('-');
    let y = dp.next()?.parse().ok()?;
    let mo = dp.next()?.parse().ok()?;
    let d = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let (h, mi, sec, us) = match time {
        None => (0, 0, 0, 0),
        Some(t) => {
            let (hms, f) = t.split_once('.').unwrap_or((t, ""));
            let mut tp = hms.split(':');
            let h = tp.next()?.parse().ok()?;
            let mi = tp.next()?.parse().ok()?;
            let sec = tp.next()?.parse().ok()?;
            let us = if f.is_empty() { 0 } else { parse_frac(f)? };
            (h, mi, sec, us)
        }
    };
    Some((y, mo, d, h, mi, sec, us))
}

/// `(negative, hours, mi, s, us)` of MySQL's `"[-]H:MM:SS[.f]"`.
pub fn parse_time(s: &str) -> Option<(bool, u32, u32, u32, u32)> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (hms, f) = s.split_once('.').unwrap_or((s, ""));
    let mut tp = hms.split(':');
    let h = tp.next()?.parse().ok()?;
    let mi = tp.next()?.parse().ok()?;
    let sec = tp.next()?.parse().ok()?;
    let us = if f.is_empty() { 0 } else { parse_frac(f)? };
    Some((neg, h, mi, sec, us))
}

/// Microseconds since the epoch of a canonical `datetime`/`timestamp`.
pub fn datetime_micros(s: &str) -> Option<i64> {
    let (y, mo, d, h, mi, sec, us) = parse_datetime(s)?;
    let days = days_from_civil(i64::from(y), mo, d);
    Some(
        (days * 86_400 + i64::from(h) * 3600 + i64::from(mi) * 60 + i64::from(sec)) * 1_000_000
            + i64::from(us),
    )
}

// --------------------------------------------------------------- values --

fn utf8(b: Vec<u8>, col: &str) -> Result<Value, String> {
    String::from_utf8(b)
        .map(Value::String)
        .map_err(|_| format!("column {col}: the value is not UTF-8"))
}

fn num_i(i: i64) -> Value {
    Value::Number(i.into())
}

fn num_u(u: u64) -> Value {
    Value::Number(u.into())
}

fn num_f(f: f64) -> Value {
    Number::from_f64(f).map_or_else(|| Value::String(f.to_string()), Value::Number)
}

fn be_u64(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, x| (acc << 8) | u64::from(*x))
}

fn set_names(mask: u64, names: &[String]) -> String {
    names
        .iter()
        .enumerate()
        .filter(|(i, _)| *i < 64 && mask & (1 << i) != 0)
        .map(|(_, n)| n.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// The canonical JSON of a text-protocol value of `col` (`None` is NULL).
/// Timestamps must have been read with `time_zone = '+00:00'`.
pub fn text_to_json(raw: Option<Vec<u8>>, col: &ColumnDef) -> Result<Value, String> {
    let Some(b) = raw else {
        return Ok(Value::Null);
    };
    let bad = |what: &str| format!("column {}: {what}", col.name);
    let text: Result<&str, String> = std::str::from_utf8(&b).map_err(|_| bad("not UTF-8"));
    Ok(match col.kind() {
        Kind::Int { unsigned: true, .. } => num_u(
            text.clone()?
                .parse()
                .map_err(|_| bad("not an unsigned integer"))?,
        ),
        Kind::Int { .. } | Kind::Year => {
            num_i(text.clone()?.parse().map_err(|_| bad("not an integer"))?)
        }
        Kind::Float | Kind::Double => {
            num_f(text.clone()?.parse().map_err(|_| bad("not a number"))?)
        }
        Kind::Decimal { .. } | Kind::Date => Value::String(text.clone()?.to_owned()),
        Kind::DateTime => {
            let s = text.clone()?;
            match parse_datetime(s) {
                Some((y, mo, d, h, mi, sec, us)) => {
                    Value::String(fmt_datetime(y, mo, d, h, mi, sec, us))
                }
                None => Value::String(s.to_owned()),
            }
        }
        Kind::Timestamp => {
            let s = text.clone()?;
            match datetime_micros(s) {
                Some(us) => Value::String(fmt_timestamp(
                    us.div_euclid(1_000_000),
                    us.rem_euclid(1_000_000) as u32,
                )),
                None => Value::String(s.to_owned()),
            }
        }
        Kind::Time => {
            let s = text.clone()?;
            match parse_time(s) {
                Some((neg, h, mi, sec, us)) => Value::String(fmt_time(neg, h, mi, sec, us)),
                None => Value::String(s.to_owned()),
            }
        }
        Kind::Text { .. } | Kind::Enum(_) | Kind::Set(_) => Value::String(text?.to_owned()),
        Kind::Binary | Kind::Other(_) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(&b))
        }
        Kind::Bit => num_u(be_u64(&b)),
        Kind::Json => serde_json::from_slice(&b).map_err(|_| bad("not JSON"))?,
    })
}

/// The canonical JSON of a binlog row value of `col`.
pub fn binlog_to_json(v: BinlogValue<'_>, col: &ColumnDef) -> Result<Value, String> {
    let bad = |what: &str| format!("column {}: {what}", col.name);
    let kind = col.kind();
    let value = match v {
        BinlogValue::Jsonb(j) => {
            return serde_json::Value::try_from(j).map_err(|e| bad(&format!("JSON: {e}")));
        }
        BinlogValue::JsonDiff(_) => {
            return Err(bad(
                "a partial JSON update (binlog_row_value_options=PARTIAL_JSON) has no full value; \
                 set binlog_row_value_options to ''",
            ));
        }
        BinlogValue::Value(v) => v,
    };
    Ok(match (value, kind) {
        (MyValue::NULL, _) => Value::Null,
        (
            MyValue::Int(i),
            Kind::Int {
                unsigned: true,
                bits,
            },
        ) if i < 0 => num_u(
            (i as u64)
                & if bits >= 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                },
        ),
        (MyValue::Int(i), Kind::Enum(names)) => match usize::try_from(i) {
            Ok(n) if n >= 1 && n <= names.len() => Value::String(names[n - 1].clone()),
            _ => Value::String(String::new()),
        },
        (MyValue::Int(i), Kind::Timestamp) => Value::String(fmt_timestamp(i, 0)),
        (MyValue::Int(i), _) => num_i(i),
        (MyValue::UInt(u), _) => num_u(u),
        (MyValue::Float(f), _) => num_f(f64::from(f)),
        (MyValue::Double(f), _) => num_f(f),
        (MyValue::Date(y, mo, d, ..), Kind::Date) => {
            Value::String(format!("{y:04}-{mo:02}-{d:02}"))
        }
        (MyValue::Date(y, mo, d, h, mi, s, us), _) => Value::String(fmt_datetime(
            u32::from(y),
            u32::from(mo),
            u32::from(d),
            u32::from(h),
            u32::from(mi),
            u32::from(s),
            us,
        )),
        (MyValue::Time(neg, days, h, mi, s, us), _) => Value::String(fmt_time(
            neg,
            days * 24 + u32::from(h),
            u32::from(mi),
            u32::from(s),
            us,
        )),
        (MyValue::Bytes(b), Kind::Timestamp) => {
            let s = std::str::from_utf8(&b).map_err(|_| bad("not a timestamp"))?;
            let (sec, us) = s.split_once('.').unwrap_or((s, ""));
            let sec: i64 = sec.parse().map_err(|_| bad("not a timestamp"))?;
            let us = if us.is_empty() {
                0
            } else {
                parse_frac(us).ok_or_else(|| bad("not a timestamp"))?
            };
            Value::String(fmt_timestamp(sec, us))
        }
        (MyValue::Bytes(b), Kind::Year) => num_i(
            std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| bad("not a year"))?,
        ),
        (MyValue::Bytes(b), Kind::Bit) => num_u(be_u64(&b)),
        (MyValue::Bytes(b), Kind::Set(names)) => {
            // The binlog keeps a set as its bitmask, little-endian.
            let mask = b
                .iter()
                .rev()
                .fold(0u64, |acc, x| (acc << 8) | u64::from(*x));
            Value::String(set_names(mask, &names))
        }
        (MyValue::Bytes(b), Kind::Binary | Kind::Other(_)) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        (MyValue::Bytes(b), Kind::Json) => {
            serde_json::from_slice(&b).map_err(|_| bad("not JSON"))?
        }
        (MyValue::Bytes(b), _) => utf8(b, &col.name)?,
    })
}

/// A canonical JSON value as the text a statement sends (`None` is NULL).
pub fn json_to_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_owned()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nt(t: &str) -> NativeType {
        NativeType::new("t.c", t, true)
    }

    #[test]
    fn kinds_of_column_types() {
        assert_eq!(
            kind_of("bigint unsigned"),
            Kind::Int {
                unsigned: true,
                bits: 64
            }
        );
        assert_eq!(
            kind_of("INT(11) UNSIGNED"),
            Kind::Int {
                unsigned: true,
                bits: 32
            }
        );
        assert_eq!(kind_of("varchar(255)"), Kind::Text { max: Some(255) });
        assert_eq!(
            kind_of("decimal(10,2)"),
            Kind::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            kind_of("enum('a','it''s')"),
            Kind::Enum(vec!["a".into(), "it's".into()])
        );
        assert_eq!(kind_of("float(30)"), Kind::Double);
        assert_eq!(kind_of("point"), Kind::Other("point".into()));
    }

    #[test]
    fn arrow_mapping() {
        let m = MyTypeMapper::default();
        let dt = |t: &str| m.to_arrow(&nt(t)).map(|f| f.data_type().clone());
        assert_eq!(dt("bigint"), Ok(DataType::Int64));
        assert_eq!(dt("tinyint unsigned"), Ok(DataType::UInt8));
        assert_eq!(dt("mediumint"), Ok(DataType::Int32));
        assert_eq!(dt("decimal(12,3)"), Ok(DataType::Decimal128(12, 3)));
        assert_eq!(
            dt("timestamp(6)"),
            Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
        );
        assert_eq!(dt("time(3)"), Ok(DataType::Duration(TimeUnit::Microsecond)));
        assert_eq!(dt("json"), Ok(DataType::Utf8));
        assert!(matches!(dt("datetime"), Err(SchemaError::Lossy { .. })));
        assert!(matches!(
            dt("decimal(65,2)"),
            Err(SchemaError::Lossy { .. })
        ));
        assert!(matches!(
            dt("geometry"),
            Err(SchemaError::Unsupported { .. })
        ));
        let lenient = MyTypeMapper::new(Lossy::paths(["t.c"]));
        assert!(lenient.to_arrow(&nt("datetime(3)")).is_ok());
        let f = m.to_arrow(&nt("varchar(40)")).expect("varchar");
        assert_eq!(
            f.metadata().get(MAX_LENGTH_KEY).map(String::as_str),
            Some("40")
        );
    }

    #[test]
    fn dates_roundtrip() {
        for (y, m, d) in [(1970, 1, 1), (2000, 2, 29), (1969, 12, 31), (2038, 1, 19)] {
            assert_eq!(civil_from_days(days_from_civil(y, m, d)), (y, m, d));
        }
        assert_eq!(fmt_timestamp(0, 0), "1970-01-01T00:00:00Z");
        assert_eq!(
            fmt_timestamp(1_700_000_000, 5),
            "2023-11-14T22:13:20.000005Z"
        );
        assert_eq!(
            datetime_micros("2023-11-14 22:13:20.5"),
            Some(1_700_000_000_500_000)
        );
    }

    fn col(t: &str) -> ColumnDef {
        ColumnDef::new("c", t, true)
    }

    #[test]
    fn text_and_binlog_values_agree() {
        let cases: Vec<(&str, &[u8], BinlogValue<'static>)> = vec![
            ("bigint", b"-5", BinlogValue::Value(MyValue::Int(-5))),
            (
                "bigint unsigned",
                b"18446744073709551615",
                BinlogValue::Value(MyValue::Int(-1)),
            ),
            (
                "tinyint unsigned",
                b"200",
                BinlogValue::Value(MyValue::Int(-56)),
            ),
            (
                "decimal(10,2)",
                b"12.50",
                BinlogValue::Value(MyValue::Bytes(b"12.50".to_vec())),
            ),
            (
                "datetime(3)",
                b"2024-01-02 03:04:05.120",
                BinlogValue::Value(MyValue::Date(2024, 1, 2, 3, 4, 5, 120_000)),
            ),
            (
                "timestamp",
                b"2023-11-14 22:13:20",
                BinlogValue::Value(MyValue::Bytes(b"1700000000".to_vec())),
            ),
            (
                "date",
                b"2024-01-02",
                BinlogValue::Value(MyValue::Date(2024, 1, 2, 0, 0, 0, 0)),
            ),
            (
                "time",
                b"-26:00:01",
                BinlogValue::Value(MyValue::Time(true, 1, 2, 0, 1, 0)),
            ),
            (
                "year",
                b"2024",
                BinlogValue::Value(MyValue::Bytes(b"2024".to_vec())),
            ),
            (
                "varchar(10)",
                "héllo".as_bytes(),
                BinlogValue::Value(MyValue::Bytes("héllo".as_bytes().to_vec())),
            ),
            (
                "varbinary(4)",
                b"\x00\xff",
                BinlogValue::Value(MyValue::Bytes(b"\x00\xff".to_vec())),
            ),
            (
                "bit(10)",
                b"\x01\x02",
                BinlogValue::Value(MyValue::Bytes(b"\x01\x02".to_vec())),
            ),
            ("enum('x','y')", b"y", BinlogValue::Value(MyValue::Int(2))),
            (
                "set('a','b','c')",
                b"a,c",
                BinlogValue::Value(MyValue::Bytes(vec![0b101])),
            ),
        ];
        for (t, text, bin) in cases {
            let c = col(t);
            let a = text_to_json(Some(text.to_vec()), &c).expect(t);
            let b = binlog_to_json(bin, &c).expect(t);
            assert_eq!(a, b, "{t}");
        }
        assert_eq!(text_to_json(None, &col("int")), Ok(Value::Null));
        assert!(text_to_json(Some(vec![0xff]), &col("text")).is_err());
        assert_eq!(
            text_to_json(Some(br#"{"a":1}"#.to_vec()), &col("json")),
            Ok(serde_json::json!({"a": 1}))
        );
    }

    #[test]
    fn literals_need_no_escaping() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(quote_str("it's"), "CONVERT(X'69742773' USING utf8mb4)");
        assert_eq!(hex_literal(b""), "X''");
    }
}
