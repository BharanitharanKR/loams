//! JSON to and from dynamic protobuf messages, for the gRPC connector
//! (ruling R21.3). It follows the proto3 JSON mapping where a connector
//! meets it: fields by their JSON name or their proto name; 64-bit integers
//! as numbers or decimal strings in, decimal strings out; `bytes` as
//! base64; enums by name or number in, by name out (an unknown number stays
//! a number); maps as objects; `NaN` and the infinities as strings. Well-known
//! types are plain messages, as the SDK's Protobuf codec treats them (R5.5).
//! prost-reflect's own JSON support (its `serde` feature) would add two
//! crates to `fabric/Cargo.lock`.

use base64::Engine;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value};
use serde_json::{Map, Number, Value as Json};

/// Why a JSON value does not fit a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonProtoError(pub String);

impl std::fmt::Display for JsonProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(path: &str, why: impl std::fmt::Display) -> Result<T, JsonProtoError> {
    Err(JsonProtoError(format!("{path}: {why}")))
}

/// Build a message of type `desc` from a JSON object.
pub fn from_json(desc: &MessageDescriptor, j: &Json) -> Result<DynamicMessage, JsonProtoError> {
    message(desc, j, "$")
}

fn message(
    desc: &MessageDescriptor,
    j: &Json,
    path: &str,
) -> Result<DynamicMessage, JsonProtoError> {
    let Json::Object(obj) = j else {
        return err(path, format!("{} needs a JSON object", desc.full_name()));
    };
    let mut msg = DynamicMessage::new(desc.clone());
    for (k, v) in obj {
        let field = desc
            .get_field_by_json_name(k)
            .or_else(|| desc.get_field_by_name(k));
        let Some(field) = field else {
            return err(path, format!("{} has no field {k:?}", desc.full_name()));
        };
        if v.is_null() {
            continue;
        }
        let fpath = format!("{path}.{k}");
        let value = if field.is_map() {
            map(&field, v, &fpath)?
        } else if field.is_list() {
            let Json::Array(items) = v else {
                return err(&fpath, "a repeated field needs an array");
            };
            Value::List(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| single(&field.kind(), item, &format!("{fpath}[{i}]")))
                    .collect::<Result<_, _>>()?,
            )
        } else {
            single(&field.kind(), v, &fpath)?
        };
        msg.set_field(&field, value);
    }
    Ok(msg)
}

fn map(field: &FieldDescriptor, v: &Json, path: &str) -> Result<Value, JsonProtoError> {
    let Json::Object(obj) = v else {
        return err(path, "a map field needs an object");
    };
    let Kind::Message(entry) = field.kind() else {
        return err(path, "a map without an entry type");
    };
    let (kf, vf) = (entry.map_entry_key_field(), entry.map_entry_value_field());
    let mut out = std::collections::HashMap::new();
    for (k, val) in obj {
        let key = match kf.kind() {
            Kind::String => MapKey::String(k.clone()),
            Kind::Bool => MapKey::Bool(k == "true"),
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => MapKey::I32(
                k.parse()
                    .map_err(|_| JsonProtoError(format!("{path}: bad key {k}")))?,
            ),
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => MapKey::I64(
                k.parse()
                    .map_err(|_| JsonProtoError(format!("{path}: bad key {k}")))?,
            ),
            Kind::Uint32 | Kind::Fixed32 => MapKey::U32(
                k.parse()
                    .map_err(|_| JsonProtoError(format!("{path}: bad key {k}")))?,
            ),
            Kind::Uint64 | Kind::Fixed64 => MapKey::U64(
                k.parse()
                    .map_err(|_| JsonProtoError(format!("{path}: bad key {k}")))?,
            ),
            _ => return err(path, "an unsupported map key type"),
        };
        out.insert(key, single(&vf.kind(), val, &format!("{path}.{k}"))?);
    }
    Ok(Value::Map(out))
}

fn int(v: &Json, path: &str) -> Result<i128, JsonProtoError> {
    match v {
        Json::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
            .or_else(|| n.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i128))
            .ok_or_else(|| JsonProtoError(format!("{path}: not an integer"))),
        Json::String(s) => s
            .parse()
            .map_err(|_| JsonProtoError(format!("{path}: not an integer"))),
        _ => err(path, "needs an integer"),
    }
}

fn float(v: &Json, path: &str) -> Result<f64, JsonProtoError> {
    match v {
        Json::Number(n) => n
            .as_f64()
            .ok_or_else(|| JsonProtoError(format!("{path}: not a number"))),
        Json::String(s) => match s.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            other => other
                .parse()
                .map_err(|_| JsonProtoError(format!("{path}: not a number"))),
        },
        _ => err(path, "needs a number"),
    }
}

fn ranged<T: TryFrom<i128>>(v: i128, path: &str) -> Result<T, JsonProtoError> {
    T::try_from(v).map_err(|_| JsonProtoError(format!("{path}: out of range")))
}

fn single(kind: &Kind, v: &Json, path: &str) -> Result<Value, JsonProtoError> {
    Ok(match kind {
        Kind::Double => Value::F64(float(v, path)?),
        Kind::Float => Value::F32(float(v, path)? as f32),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Value::I32(ranged(int(v, path)?, path)?),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Value::I64(ranged(int(v, path)?, path)?),
        Kind::Uint32 | Kind::Fixed32 => Value::U32(ranged(int(v, path)?, path)?),
        Kind::Uint64 | Kind::Fixed64 => Value::U64(ranged(int(v, path)?, path)?),
        Kind::Bool => match v {
            Json::Bool(b) => Value::Bool(*b),
            _ => return err(path, "needs a boolean"),
        },
        Kind::String => match v {
            Json::String(s) => Value::String(s.clone()),
            _ => return err(path, "needs a string"),
        },
        Kind::Bytes => match v {
            Json::String(s) => Value::Bytes(
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(s))
                    .map_err(|_| JsonProtoError(format!("{path}: not base64")))?
                    .into(),
            ),
            _ => return err(path, "needs base64 text"),
        },
        Kind::Enum(e) => match v {
            Json::String(s) => match e.get_value_by_name(s) {
                Some(ev) => Value::EnumNumber(ev.number()),
                None => return err(path, format!("{} has no value {s}", e.full_name())),
            },
            other => Value::EnumNumber(ranged(int(other, path)?, path)?),
        },
        Kind::Message(m) => Value::Message(message(m, v, path)?),
    })
}

/// A message as JSON (fields by their JSON names; fields at their default
/// are left out, as proto3 JSON does).
pub fn to_json(msg: &DynamicMessage) -> Json {
    let mut out = Map::new();
    for (field, value) in msg.fields() {
        out.insert(field.json_name().to_owned(), field_json(&field, value));
    }
    Json::Object(out)
}

fn field_json(field: &FieldDescriptor, v: &Value) -> Json {
    match v {
        Value::List(items) => {
            Json::Array(items.iter().map(|i| value_json(&field.kind(), i)).collect())
        }
        Value::Map(m) => {
            let vk = match field.kind() {
                Kind::Message(entry) => entry.map_entry_value_field().kind(),
                other => other,
            };
            let mut entries: Vec<(String, Json)> = m
                .iter()
                .map(|(k, val)| {
                    let key = match k {
                        MapKey::Bool(b) => b.to_string(),
                        MapKey::I32(i) => i.to_string(),
                        MapKey::I64(i) => i.to_string(),
                        MapKey::U32(i) => i.to_string(),
                        MapKey::U64(i) => i.to_string(),
                        MapKey::String(s) => s.clone(),
                    };
                    (key, value_json(&vk, val))
                })
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Json::Object(entries.into_iter().collect())
        }
        other => value_json(&field.kind(), other),
    }
}

fn f64_json(f: f64) -> Json {
    if f.is_nan() {
        Json::String("NaN".into())
    } else if f.is_infinite() {
        Json::String(if f > 0.0 { "Infinity" } else { "-Infinity" }.into())
    } else {
        Number::from_f64(f).map_or(Json::Null, Json::Number)
    }
}

fn value_json(kind: &Kind, v: &Value) -> Json {
    match v {
        Value::Bool(b) => Json::Bool(*b),
        Value::I32(i) => Json::from(*i),
        Value::U32(i) => Json::from(*i),
        Value::I64(i) => Json::String(i.to_string()),
        Value::U64(i) => Json::String(i.to_string()),
        Value::F32(f) => f64_json(f64::from(*f)),
        Value::F64(f) => f64_json(*f),
        Value::String(s) => Json::String(s.clone()),
        Value::Bytes(b) => Json::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::EnumNumber(n) => match kind {
            Kind::Enum(e) => e
                .get_value(*n)
                .map_or_else(|| Json::from(*n), |ev| Json::String(ev.name().to_owned())),
            _ => Json::from(*n),
        },
        Value::Message(m) => to_json(m),
        Value::List(items) => Json::Array(items.iter().map(|i| value_json(kind, i)).collect()),
        Value::Map(_) => Json::Null,
    }
}
