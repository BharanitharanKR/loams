//! [`SqlTypeMapper`]: the shared mapper for SQL-shaped type names, which the
//! relational families (Postgres, MySQL, SQL Server, CockroachDB, Trino, …)
//! extend with their own types. Its table is the reference every family's
//! guide starts from.
//!
//! | Native | Arrow | Lossy |
//! |---|---|---|
//! | `boolean`, `bool` | `Boolean` | |
//! | `tinyint`, `smallint`/`int2`, `integer`/`int`/`int4`/`mediumint`, `bigint`/`int8` | `Int8`…`Int64` | |
//! | the same with `unsigned` | `UInt8`…`UInt64` | `UInt64` into a signed-only target |
//! | `real`/`float4`, `double precision`/`double`/`float8`/`float` | `Float32`, `Float64` | |
//! | `decimal(p,s)`, `numeric(p,s)`, p ≤ 38 | `Decimal128(p,s)` | p > 38 → `Decimal128(38,s)` |
//! | `char(n)`, `varchar(n)`, `character varying(n)` | `Utf8` + `loams.max_length` | |
//! | `text`, `string`, `clob` | `Utf8` | |
//! | `varbinary`, `binary`, `bytea`, `blob` | `Binary` | |
//! | `date` | `Date32` | |
//! | `time` | `Time64(µs)` | |
//! | `timestamp with time zone`, `timestamptz` | `Timestamp(µs, "UTC")` | |
//! | `timestamp`, `timestamp without time zone`, `datetime` | `Timestamp(µs, "UTC")` | always: the zone is assumed |
//! | `uuid` | `FixedSizeBinary(16)` + `arrow.uuid` | |
//! | `json`, `jsonb` | `Utf8` + `arrow.json` | |

use std::collections::HashMap;

use arrow_schema::{DataType, Field, TimeUnit};

use super::{Lossy, MAX_LENGTH_KEY, NativeType, PATH_KEY, SchemaError, TypeMapper, render_type};

/// Arrow's canonical extension-name metadata key.
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

/// The largest precision `Decimal128` holds.
pub const MAX_DECIMAL128_PRECISION: u8 = 38;

/// The mapper for SQL-shaped type names (see the module's table).
#[derive(Debug, Clone, Default)]
pub struct SqlTypeMapper {
    lossy: Lossy,
    signed_only: bool,
}

impl SqlTypeMapper {
    pub fn new(lossy: Lossy) -> Self {
        Self {
            lossy,
            signed_only: false,
        }
    }

    /// A target without unsigned integers (Postgres, SQL Server): `from_arrow`
    /// widens `UInt8`–`UInt32` to the next signed type and refuses `UInt64`
    /// as lossy.
    pub fn signed_only(mut self) -> Self {
        self.signed_only = true;
        self
    }
}

struct Parsed {
    base: String,
    args: Vec<u32>,
    unsigned: bool,
}

fn parse(type_name: &str) -> Result<Parsed, ()> {
    let lower = type_name.trim().to_ascii_lowercase();
    let (head, args, tail) = match lower.find('(') {
        Some(open) => {
            let close = lower[open..].find(')').ok_or(())? + open;
            let args = lower[open + 1..close]
                .split(',')
                .map(|a| a.trim().parse::<u32>().map_err(|_| ()))
                .collect::<Result<Vec<_>, _>>()?;
            (&lower[..open], args, &lower[close + 1..])
        }
        None => (lower.as_str(), Vec::new(), ""),
    };
    let words: Vec<&str> = head
        .split_whitespace()
        .chain(tail.split_whitespace())
        .collect();
    let unsigned = words.last() == Some(&"unsigned");
    let base = words
        .iter()
        .filter(|w| **w != "unsigned")
        .copied()
        .collect::<Vec<_>>()
        .join(" ");
    Ok(Parsed {
        base,
        args,
        unsigned,
    })
}

fn md(pairs: &[(&str, String)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn utc_micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

impl TypeMapper for SqlTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let unsupported = || SchemaError::Unsupported {
            path: native.path.clone(),
            native: native.type_name.clone(),
        };
        let p = parse(&native.type_name).map_err(|_| unsupported())?;
        let mut metadata = md(&[(PATH_KEY, native.path.clone())]);
        let int = |signed: DataType, unsigned: DataType| if p.unsigned { unsigned } else { signed };
        let data_type = match (p.base.as_str(), p.args.as_slice()) {
            ("boolean" | "bool", []) => DataType::Boolean,
            ("tinyint", _) => int(DataType::Int8, DataType::UInt8),
            ("smallint" | "int2", _) => int(DataType::Int16, DataType::UInt16),
            ("integer" | "int" | "int4" | "mediumint", _) => int(DataType::Int32, DataType::UInt32),
            ("bigint" | "int8", _) => int(DataType::Int64, DataType::UInt64),
            ("real" | "float4", []) => DataType::Float32,
            ("double precision" | "double" | "float8" | "float", []) => DataType::Float64,
            ("decimal" | "numeric", [p_, rest @ ..]) if rest.len() <= 1 => {
                let scale = rest.first().copied().unwrap_or(0);
                if scale > *p_ {
                    return Err(unsupported());
                }
                if *p_ <= u32::from(MAX_DECIMAL128_PRECISION) {
                    DataType::Decimal128(*p_ as u8, scale as i8)
                } else {
                    let s = scale.min(u32::from(MAX_DECIMAL128_PRECISION)) as i8;
                    let field = Field::new(
                        native.column(),
                        DataType::Decimal128(MAX_DECIMAL128_PRECISION, s),
                        native.nullable,
                    )
                    .with_metadata(metadata);
                    return self.lossy.accept(
                        &native.path,
                        &native.type_name,
                        field,
                        "a decimal over 38 digits is truncated to 38",
                    );
                }
            }
            (
                "char" | "varchar" | "character varying" | "character" | "nvarchar" | "nchar",
                [n],
            ) => {
                metadata.insert(MAX_LENGTH_KEY.to_owned(), n.to_string());
                DataType::Utf8
            }
            ("text" | "string" | "clob", []) => DataType::Utf8,
            ("varbinary" | "binary" | "bytea" | "blob", _) => DataType::Binary,
            ("date", []) => DataType::Date32,
            ("time" | "time without time zone", []) => DataType::Time64(TimeUnit::Microsecond),
            ("timestamp with time zone" | "timestamptz", []) => utc_micros(),
            ("timestamp" | "timestamp without time zone" | "datetime", []) => {
                let field = Field::new(native.column(), utc_micros(), native.nullable)
                    .with_metadata(metadata);
                return self.lossy.accept(
                    &native.path,
                    &native.type_name,
                    field,
                    "a timestamp without a timezone is read as UTC",
                );
            }
            ("uuid", []) => {
                metadata.insert(EXTENSION_NAME_KEY.to_owned(), "arrow.uuid".to_owned());
                DataType::FixedSizeBinary(16)
            }
            ("json" | "jsonb", []) => {
                metadata.insert(EXTENSION_NAME_KEY.to_owned(), "arrow.json".to_owned());
                DataType::Utf8
            }
            _ => return Err(unsupported()),
        };
        Ok(Field::new(native.column(), data_type, native.nullable).with_metadata(metadata))
    }

    fn from_arrow(&self, field: &Field) -> Result<NativeType, SchemaError> {
        let path = field
            .metadata()
            .get(PATH_KEY)
            .cloned()
            .unwrap_or_else(|| field.name().clone());
        let extension = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
        let lossy = |to: &str, reason: &str| -> Result<String, SchemaError> {
            if self.lossy.allows(&path) {
                Ok(to.to_owned())
            } else {
                Err(SchemaError::Lossy {
                    path: path.clone(),
                    from: render_type(field.data_type()),
                    to: to.to_owned(),
                    reason: reason.to_owned(),
                })
            }
        };
        let unsigned = |signed_only: &str, name: &str| {
            if self.signed_only {
                signed_only.to_owned()
            } else {
                name.to_owned()
            }
        };
        let type_name = match field.data_type() {
            DataType::Boolean => "boolean".to_owned(),
            DataType::Int8 if self.signed_only => "smallint".to_owned(),
            DataType::Int8 => "tinyint".to_owned(),
            DataType::Int16 => "smallint".to_owned(),
            DataType::Int32 => "integer".to_owned(),
            DataType::Int64 => "bigint".to_owned(),
            DataType::UInt8 => unsigned("smallint", "tinyint unsigned"),
            DataType::UInt16 => unsigned("integer", "smallint unsigned"),
            DataType::UInt32 => unsigned("bigint", "integer unsigned"),
            DataType::UInt64 if self.signed_only => lossy(
                "bigint",
                "values over 2^63-1 do not fit a signed 64-bit integer",
            )?,
            DataType::UInt64 => "bigint unsigned".to_owned(),
            DataType::Float32 => "real".to_owned(),
            DataType::Float64 => "double precision".to_owned(),
            DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
            DataType::Decimal256(p, s) if *p <= MAX_DECIMAL128_PRECISION => {
                format!("decimal({p},{s})")
            }
            DataType::Decimal256(_, s) => lossy(
                &format!("decimal(38,{})", (*s).clamp(0, 38)),
                "a decimal over 38 digits is truncated to 38",
            )?,
            DataType::Utf8 | DataType::LargeUtf8 if extension == Some("arrow.json") => {
                "json".to_owned()
            }
            DataType::Utf8 | DataType::LargeUtf8 => match field.metadata().get(MAX_LENGTH_KEY) {
                Some(n) => format!("varchar({n})"),
                None => "text".to_owned(),
            },
            DataType::FixedSizeBinary(16) if extension == Some("arrow.uuid") => "uuid".to_owned(),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                "varbinary".to_owned()
            }
            DataType::Date32 => "date".to_owned(),
            DataType::Time64(TimeUnit::Microsecond) => "time".to_owned(),
            DataType::Time64(TimeUnit::Nanosecond) => {
                lossy("time", "nanoseconds are truncated to microseconds")?
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => lossy(
                "timestamp with time zone",
                "nanoseconds are truncated to microseconds",
            )?,
            DataType::Timestamp(_, Some(_)) => "timestamp with time zone".to_owned(),
            DataType::Timestamp(_, None) => {
                lossy("timestamp", "a timestamp without a timezone has no instant")?
            }
            _ => {
                return Err(SchemaError::Unsupported {
                    path,
                    native: render_type(field.data_type()),
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
