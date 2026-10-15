//! BSON ↔ Arrow (Task 5's `TypeMapper` for the family), and the schema of a
//! schemaless collection.
//!
//! | BSON | Arrow | `loams.bson.type` |
//! |---|---|---|
//! | `double` | `Float64` | `double` |
//! | `int`, `long` | `Int64` | `int`, `long` |
//! | `int` and `double` in one field | `Float64` | `double` |
//! | `string`, `symbol` | `Utf8` | `string` |
//! | `bool` | `Boolean` | `bool` |
//! | `date` | `Timestamp(ms, "UTC")` | `date` |
//! | `objectId` | `Utf8` (24 hex digits) | `objectId` |
//! | `decimal` | `Utf8` (the exact decimal string) | `decimal` |
//! | `binData` subtype 0 | `Binary` | `binData` |
//! | anything else, or a field whose values have types no one column holds | `LargeUtf8`, relaxed Extended JSON (self-describing) | the BSON type, or `mixed` |
//!
//! A collection has no schema, so a source infers one per batch from the
//! documents' top-level fields and **unions** it with the schema it already
//! tracks: a field missing from a batch stays (null), a field seen only as
//! null is left out until it has a typed value (then it is an additive
//! change), and a value an existing column can hold keeps that column (an
//! `int` in a `long` or `double` column, anything in a `mixed` one). Only a
//! value no existing column can hold changes a column's type, which Task 5's
//! tracker then judges (`Additive` pauses on it; plan ruling R16.6).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use loams_connector_sdk::schema::{Lossy, NativeType, PATH_KEY, SchemaError, TypeMapper};

use crate::bson::{Bson, Document, ObjectId};
use crate::decimal::Decimal128;
use crate::extjson::{self, Mode};

/// Field metadata: the BSON type a column was mapped from.
pub const BSON_TYPE_KEY: &str = "loams.bson.type";

/// What one column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Double,
    String,
    Int,
    Long,
    Bool,
    Date,
    ObjectId,
    Decimal,
    Binary,
    /// Relaxed Extended JSON text: the BSON type's name, or `mixed`.
    Json(&'static str),
}

impl Kind {
    /// The kind of one value; `None` for null and undefined.
    pub fn of(v: &Bson) -> Option<Kind> {
        Some(match v {
            Bson::Null | Bson::Undefined => return None,
            Bson::Double(_) => Kind::Double,
            Bson::String(_) | Bson::Symbol(_) => Kind::String,
            Bson::Int32(_) => Kind::Int,
            Bson::Int64(_) => Kind::Long,
            Bson::Boolean(_) => Kind::Bool,
            Bson::DateTime(_) => Kind::Date,
            Bson::ObjectId(_) => Kind::ObjectId,
            Bson::Decimal128(_) => Kind::Decimal,
            Bson::Binary { subtype: 0, .. } => Kind::Binary,
            other => Kind::Json(other.type_name()),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Kind::Double => "double",
            Kind::String => "string",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Bool => "bool",
            Kind::Date => "date",
            Kind::ObjectId => "objectId",
            Kind::Decimal => "decimal",
            Kind::Binary => "binData",
            Kind::Json(t) => t,
        }
    }

    /// The kind a metadata name names.
    pub fn parse(name: &str) -> Option<Kind> {
        Some(match name {
            "double" => Kind::Double,
            "string" | "symbol" => Kind::String,
            "int" => Kind::Int,
            "long" => Kind::Long,
            "bool" => Kind::Bool,
            "date" => Kind::Date,
            "objectId" => Kind::ObjectId,
            "decimal" => Kind::Decimal,
            "binData" => Kind::Binary,
            "object" => Kind::Json("object"),
            "array" => Kind::Json("array"),
            "mixed" => Kind::Json("mixed"),
            "timestamp" => Kind::Json("timestamp"),
            "regex" => Kind::Json("regex"),
            "javascript" => Kind::Json("javascript"),
            "javascriptWithScope" => Kind::Json("javascriptWithScope"),
            "dbPointer" => Kind::Json("dbPointer"),
            "minKey" => Kind::Json("minKey"),
            "maxKey" => Kind::Json("maxKey"),
            _ => return None,
        })
    }

    pub fn data_type(self) -> DataType {
        match self {
            Kind::Double => DataType::Float64,
            Kind::Int | Kind::Long => DataType::Int64,
            Kind::Bool => DataType::Boolean,
            Kind::Date => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            Kind::Binary => DataType::Binary,
            Kind::String | Kind::ObjectId | Kind::Decimal => DataType::Utf8,
            Kind::Json(_) => DataType::LargeUtf8,
        }
    }

    /// The one kind that holds values of both kinds.
    pub fn merge(a: Kind, b: Kind) -> Kind {
        use Kind::*;
        match (a, b) {
            (x, y) if x == y => x,
            (Int, Long) | (Long, Int) => Long,
            (Int, Double) | (Double, Int) => Double,
            _ => Json("mixed"),
        }
    }

    /// Whether a column of kind `self` holds a value of kind `v` as it is.
    pub fn holds(self, v: Kind) -> bool {
        use Kind::*;
        self == v || matches!((self, v), (Long, Int) | (Double, Int) | (Json("mixed"), _))
    }
}

/// The family's mapper. `type_name` is a BSON type alias (`$type`'s names,
/// plus `mixed`).
#[derive(Debug, Clone, Default)]
pub struct BsonTypeMapper {
    lossy: Lossy,
}

impl BsonTypeMapper {
    pub fn new(lossy: Lossy) -> Self {
        Self { lossy }
    }

    fn lossy(&self, path: &str, from: &str, to: &str, reason: &str) -> Result<(), SchemaError> {
        if self.lossy.allows(path) {
            Ok(())
        } else {
            Err(SchemaError::Lossy {
                path: path.to_owned(),
                from: from.to_owned(),
                to: to.to_owned(),
                reason: reason.to_owned(),
            })
        }
    }
}

/// A column of `kind` at `path`, nullable (a document may lack the field).
pub fn field(path: &str, kind: Kind) -> Field {
    let name = path.rsplit('.').next().unwrap_or(path);
    let md = [
        (BSON_TYPE_KEY.to_owned(), kind.name().to_owned()),
        (PATH_KEY.to_owned(), path.to_owned()),
    ];
    Field::new(name, kind.data_type(), true).with_metadata(md.into_iter().collect())
}

/// The kind a column holds, from its metadata or its Arrow type.
pub fn kind_of_field(f: &Field) -> Option<Kind> {
    if let Some(k) = f.metadata().get(BSON_TYPE_KEY).and_then(|n| Kind::parse(n))
        && k.data_type() == *f.data_type()
    {
        return Some(k);
    }
    None
}

#[allow(clippy::wrong_self_convention)]
impl TypeMapper for BsonTypeMapper {
    fn to_arrow(&self, native: &NativeType) -> Result<Field, SchemaError> {
        let kind = Kind::parse(&native.type_name).ok_or_else(|| SchemaError::Unsupported {
            path: native.path.clone(),
            native: native.type_name.clone(),
        })?;
        let f = field(&native.path, kind);
        Ok(f.with_nullable(native.nullable))
    }

    fn from_arrow(&self, f: &Field) -> Result<NativeType, SchemaError> {
        let path = f
            .metadata()
            .get(PATH_KEY)
            .cloned()
            .unwrap_or_else(|| f.name().clone());
        if let Some(k) = kind_of_field(f) {
            return Ok(NativeType::new(path, k.name(), f.is_nullable()));
        }
        let rendered = loams_connector_sdk::schema::render_type(f.data_type());
        let name = match f.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => "int",
            DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "long",
            DataType::UInt64 => {
                self.lossy(&path, &rendered, "long", "values above 2^63-1 do not fit")?;
                "long"
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => "double",
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
            DataType::Boolean => "bool",
            DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, _)
            | DataType::Date32
            | DataType::Date64 => "date",
            DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _) => {
                self.lossy(&path, &rendered, "date", "dates hold milliseconds")?;
                "date"
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binData",
            DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => {
                if *p > 34 {
                    self.lossy(&path, &rendered, "decimal", "decimal128 holds 34 digits")?;
                }
                "decimal"
            }
            DataType::Null => "null",
            _ => {
                return Err(SchemaError::Unsupported {
                    path,
                    native: rendered,
                });
            }
        };
        Ok(NativeType::new(path, name, f.is_nullable()))
    }
}

/// The top-level fields of `docs`, each with the one kind that holds all
/// its values (first-seen order; null-only fields left out).
pub fn infer(docs: &[Document]) -> Vec<(String, Kind)> {
    let mut order: Vec<String> = Vec::new();
    let mut kinds: BTreeMap<String, Kind> = BTreeMap::new();
    for d in docs {
        for (k, v) in d.iter() {
            let Some(kind) = Kind::of(v) else { continue };
            match kinds.get(k) {
                Some(have) => {
                    let merged = Kind::merge(*have, kind);
                    kinds.insert(k.to_owned(), merged);
                }
                None => {
                    order.push(k.to_owned());
                    kinds.insert(k.to_owned(), kind);
                }
            }
        }
    }
    order
        .into_iter()
        .map(|k| {
            let kind = kinds[&k];
            (k, kind)
        })
        .collect()
}

/// `current` extended by what `docs` hold (see the module documentation).
/// `prefix` is `<db>.<collection>`, for the fields' paths.
pub fn union_schema(current: Option<&Schema>, docs: &[Document], prefix: &str) -> Schema {
    let mut fields: Vec<Field> = current
        .map(|s| s.fields().iter().map(|f| f.as_ref().clone()).collect())
        .unwrap_or_default();
    for (name, kind) in infer(docs) {
        match fields.iter_mut().find(|f| f.name() == &name) {
            Some(f) => {
                let have = kind_of_field(f);
                if !have.is_some_and(|h| h.holds(kind)) {
                    let merged = have.map_or(kind, |h| Kind::merge(h, kind));
                    *f = field(&format!("{prefix}.{name}"), merged);
                }
            }
            None => fields.push(field(&format!("{prefix}.{name}"), kind)),
        }
    }
    Schema::new(fields)
}

/// The documents as one Arrow batch under `schema` (from [`union_schema`]).
pub fn to_batch(schema: SchemaRef, docs: &[Document]) -> Result<RecordBatch, String> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let kind = kind_of_field(f).ok_or_else(|| format!("{}: no BSON kind", f.name()))?;
        let values = docs.iter().map(|d| d.get(f.name()));
        let col: ArrayRef = match kind {
            Kind::Double => {
                let mut b = Float64Builder::with_capacity(docs.len());
                for v in values {
                    b.append_option(match v {
                        Some(Bson::Double(x)) => Some(*x),
                        Some(Bson::Int32(x)) => Some(f64::from(*x)),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            Kind::Int | Kind::Long => {
                let mut b = Int64Builder::with_capacity(docs.len());
                for v in values {
                    b.append_option(match v {
                        Some(Bson::Int32(x)) => Some(i64::from(*x)),
                        Some(Bson::Int64(x)) => Some(*x),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            Kind::Bool => {
                let mut b = BooleanBuilder::with_capacity(docs.len());
                for v in values {
                    b.append_option(v.and_then(Bson::as_bool));
                }
                Arc::new(b.finish())
            }
            Kind::Date => {
                let mut b =
                    TimestampMillisecondBuilder::with_capacity(docs.len()).with_timezone("UTC");
                for v in values {
                    b.append_option(match v {
                        Some(Bson::DateTime(ms)) => Some(*ms),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            Kind::Binary => {
                let mut b = BinaryBuilder::new();
                for v in values {
                    match v {
                        Some(Bson::Binary { bytes, .. }) => b.append_value(bytes),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::Json(_) => {
                let mut b = LargeStringBuilder::new();
                for v in values {
                    match v {
                        None | Some(Bson::Null | Bson::Undefined) => b.append_null(),
                        Some(v) => b.append_value(extjson::to_json(v, Mode::Relaxed).to_string()),
                    }
                }
                Arc::new(b.finish())
            }
            Kind::String | Kind::ObjectId | Kind::Decimal => {
                let mut b = StringBuilder::new();
                for v in values {
                    match (kind, v) {
                        (_, None | Some(Bson::Null | Bson::Undefined)) => b.append_null(),
                        (Kind::String, Some(Bson::String(s) | Bson::Symbol(s))) => {
                            b.append_value(s)
                        }
                        (Kind::ObjectId, Some(Bson::ObjectId(o))) => b.append_value(o.to_hex()),
                        (Kind::Decimal, Some(Bson::Decimal128(d))) => b.append_value(d.to_string()),
                        (_, Some(other)) => {
                            return Err(format!(
                                "{}: a {} value in a {} column",
                                f.name(),
                                other.type_name(),
                                kind.name()
                            ));
                        }
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(col);
    }
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(docs.len())),
    )
    .map_err(|e| e.to_string())
}

/// One Arrow row as a document: each non-null cell, by its column's BSON
/// kind (from metadata) or its Arrow type.
pub fn row_to_document(batch: &RecordBatch, row: usize) -> Result<Document, String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    let schema = batch.schema();
    let mut d = Document::new();
    for (i, f) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if col.is_null(row) {
            continue;
        }
        let name = f.name();
        let kind = kind_of_field(f);
        let v = match col.data_type() {
            DataType::Int8 => Bson::Int32(i32::from(col.as_primitive::<Int8Type>().value(row))),
            DataType::Int16 => Bson::Int32(i32::from(col.as_primitive::<Int16Type>().value(row))),
            DataType::Int32 => Bson::Int32(col.as_primitive::<Int32Type>().value(row)),
            DataType::Int64 => {
                let v = col.as_primitive::<Int64Type>().value(row);
                match (kind, i32::try_from(v)) {
                    (Some(Kind::Int), Ok(small)) => Bson::Int32(small),
                    _ => Bson::Int64(v),
                }
            }
            DataType::UInt8 => Bson::Int32(i32::from(col.as_primitive::<UInt8Type>().value(row))),
            DataType::UInt16 => Bson::Int32(i32::from(col.as_primitive::<UInt16Type>().value(row))),
            DataType::UInt32 => Bson::Int64(i64::from(col.as_primitive::<UInt32Type>().value(row))),
            DataType::UInt64 => Bson::Int64(
                i64::try_from(col.as_primitive::<UInt64Type>().value(row))
                    .map_err(|_| format!("{name}: an unsigned value above 2^63-1"))?,
            ),
            DataType::Float32 => {
                Bson::Double(f64::from(col.as_primitive::<Float32Type>().value(row)))
            }
            DataType::Float64 => Bson::Double(col.as_primitive::<Float64Type>().value(row)),
            DataType::Boolean => Bson::Boolean(col.as_boolean().value(row)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let s: &str = match col.data_type() {
                    DataType::Utf8 => col.as_string::<i32>().value(row),
                    DataType::LargeUtf8 => col.as_string::<i64>().value(row),
                    _ => col.as_string_view().value(row),
                };
                match kind {
                    Some(Kind::ObjectId) => {
                        Bson::ObjectId(ObjectId::parse_hex(s).map_err(|e| format!("{name}: {e}"))?)
                    }
                    Some(Kind::Decimal) => {
                        Bson::Decimal128(Decimal128::parse(s).map_err(|e| format!("{name}: {e}"))?)
                    }
                    Some(Kind::Json(_)) => {
                        let j: serde_json::Value = serde_json::from_str(s)
                            .map_err(|e| format!("{name}: not Extended JSON: {e}"))?;
                        extjson::from_json(&j).map_err(|e| format!("{name}: {e}"))?
                    }
                    _ => Bson::String(s.to_owned()),
                }
            }
            DataType::Binary => Bson::Binary {
                subtype: 0,
                bytes: col.as_binary::<i32>().value(row).to_vec(),
            },
            DataType::LargeBinary => Bson::Binary {
                subtype: 0,
                bytes: col.as_binary::<i64>().value(row).to_vec(),
            },
            DataType::FixedSizeBinary(_) => Bson::Binary {
                subtype: 0,
                bytes: col.as_fixed_size_binary().value(row).to_vec(),
            },
            DataType::Timestamp(unit, _) => {
                let v = match unit {
                    TimeUnit::Second => col
                        .as_primitive::<TimestampSecondType>()
                        .value(row)
                        .saturating_mul(1000),
                    TimeUnit::Millisecond => {
                        col.as_primitive::<TimestampMillisecondType>().value(row)
                    }
                    TimeUnit::Microsecond => col
                        .as_primitive::<TimestampMicrosecondType>()
                        .value(row)
                        .div_euclid(1000),
                    TimeUnit::Nanosecond => col
                        .as_primitive::<TimestampNanosecondType>()
                        .value(row)
                        .div_euclid(1_000_000),
                };
                Bson::DateTime(v)
            }
            DataType::Date32 => {
                Bson::DateTime(i64::from(col.as_primitive::<Date32Type>().value(row)) * 86_400_000)
            }
            DataType::Date64 => Bson::DateTime(col.as_primitive::<Date64Type>().value(row)),
            DataType::Decimal128(_, scale) => {
                let v = col.as_primitive::<Decimal128Type>().value(row);
                Bson::Decimal128(
                    Decimal128::parse(&format!("{v}E{}", -i32::from(*scale)))
                        .map_err(|e| format!("{name}: {e}"))?,
                )
            }
            other => return Err(format!("{name}: Arrow type {other} has no BSON mapping")),
        };
        d.insert(name.clone(), v);
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, TimestampMillisecondType};

    #[test]
    fn kinds_merge_to_the_column_that_holds_both() {
        assert_eq!(Kind::merge(Kind::Int, Kind::Long), Kind::Long);
        assert_eq!(Kind::merge(Kind::Int, Kind::Double), Kind::Double);
        assert_eq!(Kind::merge(Kind::Long, Kind::Double), Kind::Json("mixed"));
        assert_eq!(Kind::merge(Kind::String, Kind::Bool), Kind::Json("mixed"));
        assert!(Kind::Long.holds(Kind::Int) && !Kind::Int.holds(Kind::Long));
        assert!(Kind::Json("mixed").holds(Kind::Date));
    }

    #[test]
    fn a_page_of_documents_becomes_one_arrow_batch() {
        let docs = vec![
            doc! { "_id" => ObjectId([1; 12]), "n" => 1, "x" => 1.5, "when" => Bson::DateTime(5),
            "tags" => vec![Bson::from("a")], "maybe" => Bson::Null },
            doc! { "_id" => ObjectId([2; 12]), "n" => 1i64 << 40, "x" => 2,
            "dec" => Bson::Decimal128(Decimal128::parse("1.10").expect("d")) },
        ];
        let schema = union_schema(None, &docs, "app.items");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["_id", "n", "x", "when", "tags", "dec"]);
        let batch = to_batch(Arc::new(schema), &docs).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "010101010101010101010101"
        );
        assert_eq!(
            batch.column(1).as_primitive::<Int64Type>().value(1),
            1 << 40
        );
        assert_eq!(batch.column(2).as_primitive::<Float64Type>().value(1), 2.0);
        assert_eq!(
            batch
                .column(3)
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            5
        );
        assert!(batch.column(3).is_null(1));
        assert_eq!(batch.column(4).as_string::<i64>().value(0), "[\"a\"]");
        assert_eq!(batch.column(5).as_string::<i32>().value(1), "1.10");
        // Every field is nullable and names its path and BSON type.
        let f = batch.schema().field(1).clone();
        assert!(f.is_nullable());
        assert_eq!(f.metadata()[PATH_KEY], "app.items.n");
        assert_eq!(f.metadata()[BSON_TYPE_KEY], "long");
    }

    #[test]
    fn the_union_keeps_columns_and_changes_a_type_only_when_it_must() {
        let first = union_schema(None, &[doc! { "a" => 1i64, "b" => "s" }], "d.c");
        // `b` missing, `a` an int: both columns stay as they were.
        let same = union_schema(Some(&first), &[doc! { "a" => 2 }], "d.c");
        assert_eq!(same.fields(), first.fields());
        // A new field is appended.
        let more = union_schema(Some(&first), &[doc! { "c" => true }], "d.c");
        assert_eq!(more.fields().len(), 3);
        // A string where a long was: the column's type must change.
        let changed = union_schema(Some(&first), &[doc! { "a" => "x" }], "d.c");
        assert_eq!(
            changed.field_with_name("a").expect("a").data_type(),
            &DataType::LargeUtf8
        );
    }

    #[test]
    fn rows_round_trip_through_arrow() {
        let docs = vec![doc! {
            "_id" => ObjectId([9; 12]), "i" => 7, "l" => 1i64 << 35, "s" => "x", "b" => false,
            "d" => Bson::DateTime(1_700_000_000_000), "o" => doc! { "k" => 1 },
            "bin" => Bson::Binary { subtype: 0, bytes: vec![1, 2] },
            "dec" => Bson::Decimal128(Decimal128::parse("-0.5").expect("d")),
        }];
        let schema = Arc::new(union_schema(None, &docs, "d.c"));
        let batch = to_batch(schema, &docs).expect("batch");
        assert_eq!(row_to_document(&batch, 0).expect("row"), docs[0]);
    }

    #[test]
    fn the_mapper_refuses_what_bson_cannot_hold_unless_listed() {
        let m = BsonTypeMapper::default();
        let f = Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true);
        assert!(matches!(m.from_arrow(&f), Err(SchemaError::Lossy { .. })));
        let lenient = BsonTypeMapper::new(Lossy::paths(["t"]));
        assert_eq!(lenient.from_arrow(&f).expect("listed").type_name, "date");
        let list = Field::new_list("l", Field::new("item", DataType::Int32, true), true);
        assert!(matches!(
            m.from_arrow(&list),
            Err(SchemaError::Unsupported { .. })
        ));
        assert_eq!(
            m.from_arrow(&Field::new("x", DataType::Int32, true))
                .expect("int")
                .type_name,
            "int"
        );
        let native = NativeType::new("db.c.when", "date", true);
        assert_eq!(
            m.to_arrow(&native).expect("date").data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert!(
            m.to_arrow(&NativeType::new("db.c.x", "whatever", true))
                .is_err()
        );
    }
}
