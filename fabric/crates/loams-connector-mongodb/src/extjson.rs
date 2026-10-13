//! MongoDB Extended JSON v2: BSON values to JSON and back.
//!
//! Events carry **relaxed** Extended JSON (numbers as JSON numbers, dates as
//! ISO-8601 strings where the year is 1970–9999), so a consumer that knows
//! nothing of BSON reads plain JSON, while a value JSON cannot hold (an
//! ObjectId, a decimal, a binary) keeps its type in a `$`-wrapper. Positions
//! and split bounds use **canonical** Extended JSON, which round-trips every
//! value exactly. [`from_json`] reads both forms (and the legacy `$binary`,
//! `$date` and `$uuid` shapes), so the sink writes back what the source read.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::{Map, Value, json};

use crate::bson::{Bson, Document, ObjectId, Timestamp, hex, unhex};
use crate::decimal::Decimal128;

/// Which Extended JSON form to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Relaxed,
    Canonical,
}

/// A value as Extended JSON.
pub fn to_json(v: &Bson, mode: Mode) -> Value {
    match v {
        Bson::Double(f) => {
            if !f.is_finite() {
                let s = if f.is_nan() {
                    "NaN"
                } else if *f > 0.0 {
                    "Infinity"
                } else {
                    "-Infinity"
                };
                json!({ "$numberDouble": s })
            } else if mode == Mode::Canonical {
                json!({ "$numberDouble": format!("{f:?}") })
            } else {
                serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
            }
        }
        Bson::String(s) => Value::String(s.clone()),
        Bson::Document(d) => document_to_json(d, mode),
        Bson::Array(items) => Value::Array(items.iter().map(|v| to_json(v, mode)).collect()),
        Bson::Binary { subtype, bytes } => json!({
            "$binary": { "base64": B64.encode(bytes), "subType": format!("{subtype:02x}") }
        }),
        Bson::Undefined => json!({ "$undefined": true }),
        Bson::ObjectId(o) => json!({ "$oid": o.to_hex() }),
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::DateTime(ms) => {
            if mode == Mode::Relaxed
                && let Some(iso) = iso_millis(*ms)
            {
                json!({ "$date": iso })
            } else {
                json!({ "$date": { "$numberLong": ms.to_string() } })
            }
        }
        Bson::Null => Value::Null,
        Bson::Regex { pattern, options } => json!({
            "$regularExpression": { "pattern": pattern, "options": options }
        }),
        Bson::DbPointer { namespace, id } => json!({
            "$dbPointer": { "$ref": namespace, "$id": { "$oid": id.to_hex() } }
        }),
        Bson::JavaScript(code) => json!({ "$code": code }),
        Bson::Symbol(s) => json!({ "$symbol": s }),
        Bson::JavaScriptWithScope { code, scope } => json!({
            "$code": code, "$scope": document_to_json(scope, mode)
        }),
        Bson::Int32(i) => match mode {
            Mode::Relaxed => json!(i),
            Mode::Canonical => json!({ "$numberInt": i.to_string() }),
        },
        Bson::Timestamp(t) => json!({ "$timestamp": { "t": t.time, "i": t.increment } }),
        Bson::Int64(i) => match mode {
            Mode::Relaxed => json!(i),
            Mode::Canonical => json!({ "$numberLong": i.to_string() }),
        },
        Bson::Decimal128(d) => json!({ "$numberDecimal": d.to_string() }),
        Bson::MinKey => json!({ "$minKey": 1 }),
        Bson::MaxKey => json!({ "$maxKey": 1 }),
    }
}

/// A document as an Extended JSON object (key order kept as far as the
/// JSON map keeps it).
pub fn document_to_json(d: &Document, mode: Mode) -> Value {
    let mut m = Map::new();
    for (k, v) in d.iter() {
        m.insert(k.to_owned(), to_json(v, mode));
    }
    Value::Object(m)
}

/// The canonical Extended JSON text of a value: an exact, comparable key.
pub fn canonical_string(v: &Bson) -> String {
    to_json(v, Mode::Canonical).to_string()
}

/// A JSON value (plain, relaxed or canonical Extended JSON) as BSON.
/// Integers that fit 32 bits become `int`, larger ones `long`, other numbers
/// `double`.
pub fn from_json(v: &Value) -> Result<Bson, String> {
    Ok(match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                match i32::try_from(i) {
                    Ok(i) => Bson::Int32(i),
                    Err(_) => Bson::Int64(i),
                }
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                return Err(format!("{n} is not a representable number"));
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(items) => Bson::Array(items.iter().map(from_json).collect::<Result<_, _>>()?),
        Value::Object(m) => match wrapper(m)? {
            Some(b) => b,
            None => Bson::Document(object_to_document(m)?),
        },
    })
}

/// A JSON object as a document (wrappers inside it are read; the object
/// itself is a document even if its keys look like a wrapper's).
pub fn object_to_document(m: &Map<String, Value>) -> Result<Document, String> {
    let mut d = Document::new();
    for (k, v) in m {
        d.insert(k.clone(), from_json(v)?);
    }
    Ok(d)
}

fn str_of<'a>(v: &'a Value, what: &str) -> Result<&'a str, String> {
    v.as_str().ok_or_else(|| format!("{what} must be a string"))
}

fn wrapper(m: &Map<String, Value>) -> Result<Option<Bson>, String> {
    let Some(first) = m.keys().next() else {
        return Ok(None);
    };
    if !first.starts_with('$') {
        return Ok(None);
    }
    let one = m.len() == 1;
    let get = |k: &str| m.get(k);
    Ok(Some(match (first.as_str(), one) {
        ("$oid", true) => Bson::ObjectId(
            ObjectId::parse_hex(str_of(&m["$oid"], "$oid")?).map_err(|e| e.to_string())?,
        ),
        ("$numberInt", true) => Bson::Int32(
            str_of(&m["$numberInt"], "$numberInt")?
                .parse()
                .map_err(|_| "$numberInt is not a 32-bit integer".to_owned())?,
        ),
        ("$numberLong", true) => Bson::Int64(
            str_of(&m["$numberLong"], "$numberLong")?
                .parse()
                .map_err(|_| "$numberLong is not a 64-bit integer".to_owned())?,
        ),
        ("$numberDouble", true) => {
            let s = str_of(&m["$numberDouble"], "$numberDouble")?;
            Bson::Double(match s {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                other => other
                    .parse()
                    .map_err(|_| format!("$numberDouble {other:?}"))?,
            })
        }
        ("$numberDecimal", true) => Bson::Decimal128(Decimal128::parse(str_of(
            &m["$numberDecimal"],
            "$numberDecimal",
        )?)?),
        ("$binary", true) => match &m["$binary"] {
            Value::Object(b) => {
                let data = B64
                    .decode(str_of(b.get("base64").unwrap_or(&Value::Null), "base64")?)
                    .map_err(|e| format!("$binary.base64: {e}"))?;
                let st = str_of(b.get("subType").unwrap_or(&Value::Null), "subType")?;
                let subtype =
                    u8::from_str_radix(st, 16).map_err(|_| format!("$binary.subType {st:?}"))?;
                Bson::Binary {
                    subtype,
                    bytes: data,
                }
            }
            _ => return Err("$binary must be an object".into()),
        },
        // Legacy: { "$binary": "<base64>", "$type": "<hex>" }.
        ("$binary", false) if m.len() == 2 && m.contains_key("$type") => {
            let data = B64
                .decode(str_of(&m["$binary"], "$binary")?)
                .map_err(|e| format!("$binary: {e}"))?;
            let st = str_of(&m["$type"], "$type")?;
            Bson::Binary {
                subtype: u8::from_str_radix(st, 16).map_err(|_| format!("$type {st:?}"))?,
                bytes: data,
            }
        }
        ("$uuid", true) => {
            let s = str_of(&m["$uuid"], "$uuid")?.replace('-', "");
            let bytes = unhex(&s)
                .filter(|b| b.len() == 16)
                .ok_or_else(|| "$uuid is not a UUID".to_owned())?;
            Bson::Binary { subtype: 4, bytes }
        }
        ("$date", true) => Bson::DateTime(match &m["$date"] {
            Value::String(s) => parse_iso_millis(s)?,
            Value::Number(n) => n.as_i64().ok_or("$date must be integral milliseconds")?,
            Value::Object(o) => match o.get("$numberLong") {
                Some(Value::String(s)) => s
                    .parse()
                    .map_err(|_| "$date.$numberLong is not an integer".to_owned())?,
                _ => return Err("$date object must hold $numberLong".into()),
            },
            _ => return Err("$date must be a string, a number or $numberLong".into()),
        }),
        ("$timestamp", true) => {
            let t = m["$timestamp"]
                .as_object()
                .ok_or("$timestamp must be an object")?;
            let num = |k: &str| {
                t.get(k)
                    .and_then(Value::as_u64)
                    .and_then(|v| u32::try_from(v).ok())
                    .ok_or_else(|| format!("$timestamp.{k} must be a 32-bit unsigned integer"))
            };
            Bson::Timestamp(Timestamp::new(num("t")?, num("i")?))
        }
        ("$regularExpression", true) => {
            let r = m["$regularExpression"]
                .as_object()
                .ok_or("$regularExpression must be an object")?;
            Bson::Regex {
                pattern: str_of(r.get("pattern").unwrap_or(&Value::Null), "pattern")?.to_owned(),
                options: str_of(r.get("options").unwrap_or(&Value::Null), "options")?.to_owned(),
            }
        }
        ("$dbPointer", true) => {
            let p = m["$dbPointer"]
                .as_object()
                .ok_or("$dbPointer must be an object")?;
            let ns = str_of(p.get("$ref").unwrap_or(&Value::Null), "$ref")?.to_owned();
            let Some(Bson::ObjectId(id)) = p.get("$id").map(from_json).transpose()? else {
                return Err("$dbPointer.$id must be an ObjectId".into());
            };
            Bson::DbPointer { namespace: ns, id }
        }
        ("$code", true) => Bson::JavaScript(str_of(&m["$code"], "$code")?.to_owned()),
        ("$code", false) if m.len() == 2 && m.contains_key("$scope") => {
            let scope = match get("$scope") {
                Some(Value::Object(s)) => object_to_document(s)?,
                _ => return Err("$scope must be an object".into()),
            };
            Bson::JavaScriptWithScope {
                code: str_of(&m["$code"], "$code")?.to_owned(),
                scope,
            }
        }
        ("$symbol", true) => Bson::Symbol(str_of(&m["$symbol"], "$symbol")?.to_owned()),
        ("$minKey", true) => Bson::MinKey,
        ("$maxKey", true) => Bson::MaxKey,
        ("$undefined", true) => Bson::Undefined,
        _ => return Ok(None),
    }))
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of a day number (`civil_from_days`).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` for years 1970–9999 (relaxed Extended JSON's
/// range), else `None`.
pub fn iso_millis(ms: i64) -> Option<String> {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, mo, d) = civil_from_days(days);
    if !(1970..=9999).contains(&y) {
        return None;
    }
    let (h, mi, s, milli) = (
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000,
    );
    Some(format!(
        "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z"
    ))
}

/// Parse an RFC 3339 date-time (`Z` or `±HH:MM`, fraction optional,
/// truncated to milliseconds) to milliseconds since the epoch.
pub fn parse_iso_millis(s: &str) -> Result<i64, String> {
    let bad = || format!("{s:?} is not an ISO-8601 date-time");
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return Err(bad());
    }
    let num = |r: std::ops::Range<usize>| -> Result<i64, String> {
        let part = s.get(r).ok_or_else(bad)?;
        if !part.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        part.parse().map_err(|_| bad())
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    if b[13] != b':' || b[16] != b':' {
        return Err(bad());
    }
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return Err(bad());
    }
    let mut i = 19;
    let mut millis = 0i64;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return Err(bad());
        }
        let frac = &s[start..i];
        let three: String = frac.chars().chain("000".chars()).take(3).collect();
        millis = three.parse().map_err(|_| bad())?;
    }
    let offset_min = match &s[i..] {
        "Z" | "z" => 0,
        tz if tz.len() == 6 && (tz.starts_with('+') || tz.starts_with('-')) && &tz[3..4] == ":" => {
            let oh = num(i + 1..i + 3)?;
            let om = num(i + 4..i + 6)?;
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            sign * (oh * 60 + om)
        }
        _ => return Err(bad()),
    };
    let days = days_from_civil(y, mo, d);
    Ok(days * 86_400_000 + h * 3_600_000 + mi * 60_000 + sec * 1000 + millis - offset_min * 60_000)
}

/// Hex of a binary value, for keys and messages.
pub fn binary_hex(bytes: &[u8]) -> String {
    hex(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc;

    #[test]
    fn relaxed_is_plain_json_where_json_can_hold_the_value() {
        let d = doc! {
            "i" => 1, "l" => 1i64 << 40, "f" => 1.5, "s" => "x", "b" => true, "n" => Bson::Null,
            "a" => vec![Bson::Int32(1)], "o" => doc! { "x" => 2 },
            "date" => Bson::DateTime(1_700_000_000_123),
        };
        assert_eq!(
            document_to_json(&d, Mode::Relaxed),
            json!({
                "i": 1, "l": 1i64 << 40, "f": 1.5, "s": "x", "b": true, "n": null,
                "a": [1], "o": {"x": 2},
                "date": {"$date": "2023-11-14T22:13:20.123Z"},
            })
        );
    }

    #[test]
    fn canonical_round_trips_every_value_exactly() {
        let d = doc! {
            "i" => 1, "l" => 1i64, "f" => 1.0, "nan" => f64::NAN,
            "oid" => ObjectId([0xab; 12]),
            "dec" => Bson::Decimal128(Decimal128::parse("-12.30").expect("dec")),
            "bin" => Bson::Binary { subtype: 0x80, bytes: vec![0, 1, 2] },
            "ts" => Timestamp::new(5, 6),
            "old" => Bson::DateTime(-62_135_596_800_000),
            "re" => Bson::Regex { pattern: "^a".into(), options: "i".into() },
            "code" => Bson::JavaScriptWithScope { code: "f".into(), scope: doc! { "v" => 1 } },
            "ptr" => Bson::DbPointer { namespace: "d.c".into(), id: ObjectId([1; 12]) },
            "min" => Bson::MinKey, "max" => Bson::MaxKey, "u" => Bson::Undefined,
            "sym" => Bson::Symbol("s".into()), "js" => Bson::JavaScript("x".into()),
        };
        let j = document_to_json(&d, Mode::Canonical);
        let back = from_json(&j).expect("parse");
        let Bson::Document(back) = back else {
            panic!("not a document")
        };
        for (k, v) in d.iter() {
            let got = back.get(k).expect(k);
            if k == "nan" {
                assert!(matches!(got, Bson::Double(f) if f.is_nan()));
            } else {
                assert_eq!(got, v, "{k}");
            }
        }
        assert_eq!(j["i"], json!({"$numberInt": "1"}));
        assert_eq!(j["l"], json!({"$numberLong": "1"}));
        assert_eq!(
            j["old"],
            json!({"$date": {"$numberLong": "-62135596800000"}})
        );
    }

    #[test]
    fn legacy_and_plain_inputs_are_read() {
        assert_eq!(
            from_json(&json!({"$binary": "AAE=", "$type": "00"})).expect("legacy"),
            Bson::Binary {
                subtype: 0,
                bytes: vec![0, 1]
            }
        );
        assert_eq!(
            from_json(&json!({"$uuid": "00112233-4455-6677-8899-aabbccddeeff"})).expect("uuid"),
            Bson::Binary {
                subtype: 4,
                bytes: unhex("00112233445566778899aabbccddeeff").expect("hex")
            }
        );
        assert_eq!(
            from_json(&json!({"$date": 1000})).expect("num"),
            Bson::DateTime(1000)
        );
        assert_eq!(
            from_json(&json!(1i64 << 33)).expect("long"),
            Bson::Int64(1 << 33)
        );
        // A key that merely starts with `$` among others is a document.
        assert!(matches!(
            from_json(&json!({"$oid": "x", "other": 1})).expect("doc"),
            Bson::Document(_)
        ));
        assert!(from_json(&json!({"$oid": "zz"})).is_err());
        assert!(from_json(&json!({"$numberInt": "99999999999"})).is_err());
    }

    #[test]
    fn iso_dates_parse_with_offsets_and_fractions() {
        assert_eq!(parse_iso_millis("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(parse_iso_millis("1970-01-01T01:00:00.5+01:00"), Ok(500));
        assert_eq!(
            parse_iso_millis("2023-11-14T22:13:20.123456Z"),
            Ok(1_700_000_000_123)
        );
        assert_eq!(
            parse_iso_millis("2000-02-29T00:00:00Z"),
            Ok(951_782_400_000)
        );
        for bad in [
            "2023-13-01T00:00:00Z",
            "2023-11-14",
            "2023-11-14T22:13:20",
            "x",
        ] {
            assert!(parse_iso_millis(bad).is_err(), "{bad}");
        }
        assert_eq!(iso_millis(0).as_deref(), Some("1970-01-01T00:00:00.000Z"));
        assert_eq!(iso_millis(-1), None);
        for ms in [0i64, 951_782_400_000, 253_402_300_799_999] {
            let iso = iso_millis(ms).expect("in range");
            assert_eq!(parse_iso_millis(&iso), Ok(ms));
        }
    }
}
