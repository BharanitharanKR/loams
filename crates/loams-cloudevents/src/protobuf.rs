//! The protobuf format of CloudEvents (`io.cloudevents.v1`, the encoding
//! D270 uses on gRPC and §36 §4.3 stores in Git WAL segments).
//!
//! The schema is the CloudEvents project's, vendored unchanged in
//! `proto/io/cloudevents/v1/cloudevents.proto` and generated once, here, so
//! the gRPC service (`loams-stream-grpc`, through `extern_path`) and the Git
//! core share one type without the core depending on tonic (plan GT1 Ruling
//! 23, R0.4).
//!
//! An attribute keeps its exact string form through [`to_proto`] and
//! [`from_proto`]. A typed value (a timestamp, an integer, a boolean, bytes)
//! is written with its protobuf type when that type prints back the same
//! string, and as `ce_string` otherwise, so `2026-09-30T10:00:00.250Z` is
//! not read back as `….25Z`.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use prost::Message;
use prost_types::Timestamp;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::{Attr, AttrType, CONTEXT_ATTRIBUTES, CloudEvent, DataKind, Error, json};

/// The generated `io.cloudevents.v1` types.
#[allow(clippy::all, missing_docs)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/io.cloudevents.v1.rs"));
}

use pb::cloud_event::cloud_event_attribute_value::Attr as Value;
use pb::cloud_event::{CloudEventAttributeValue, Data};

/// Why a protobuf batch could not be read.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// The bytes are not a `CloudEventBatch` message.
    #[error("not a CloudEventBatch in the protobuf format: {0}")]
    Decode(#[from] prost::DecodeError),
    /// Event `index` of the batch is not a valid CloudEvent.
    #[error("event {index}: {source}")]
    Event {
        /// The event's position in the batch, from 0.
        index: usize,
        /// Why the event was refused.
        source: Error,
    },
}

fn timestamp_string(timestamp: &Timestamp) -> Result<String, Error> {
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    let time = OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|_| Error::Type {
        name: "time".into(),
        expected: "a timestamp in range",
    })?;
    time.format(&Rfc3339)
        .map_err(|err| Error::Json(err.to_string()))
}

/// The timestamp `value` names, if printing it back gives `value` again.
fn lossless_timestamp(value: &str) -> Option<Timestamp> {
    let time = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    let nanos = time.unix_timestamp_nanos();
    let timestamp = Timestamp {
        seconds: i64::try_from(nanos.div_euclid(1_000_000_000)).ok()?,
        nanos: i32::try_from(nanos.rem_euclid(1_000_000_000)).ok()?,
    };
    (timestamp_string(&timestamp).ok()? == value).then_some(timestamp)
}

/// The protobuf value of one attribute, typed when the type keeps the
/// string form, else `ce_string`.
fn attribute_value(attr: &Attr) -> Value {
    let typed = match (attr.name.as_str(), attr.kind) {
        ("time", _) | (_, AttrType::Timestamp) => {
            lossless_timestamp(&attr.value).map(Value::CeTimestamp)
        }
        ("dataschema", _) | (_, AttrType::Uri) => Some(Value::CeUri(attr.value.clone())),
        (_, AttrType::UriRef) => Some(Value::CeUriRef(attr.value.clone())),
        (_, AttrType::Integer) => attr
            .value
            .parse::<i32>()
            .ok()
            .filter(|n| n.to_string() == attr.value)
            .map(Value::CeInteger),
        (_, AttrType::Boolean) => match attr.value.as_str() {
            "true" => Some(Value::CeBoolean(true)),
            "false" => Some(Value::CeBoolean(false)),
            _ => None,
        },
        (_, AttrType::Binary) => BASE64
            .decode(&attr.value)
            .ok()
            .filter(|bytes| BASE64.encode(bytes) == attr.value)
            .map(Value::CeBytes),
        (_, AttrType::String) => None,
    };
    typed.unwrap_or_else(|| Value::CeString(attr.value.clone()))
}

/// Reads one event of the protobuf format. Context attributes keep their
/// string form; an extension keeps its type (`ce_integer`, …). The map has
/// no order, so the event's attributes are the required four, then the map's
/// entries by name.
///
/// `proto_data` is refused: its `type_url` has nowhere to live in a record.
pub fn from_proto(event: &pb::CloudEvent) -> Result<CloudEvent, Error> {
    let mut attrs = vec![
        Attr::string("specversion", event.spec_version.clone()),
        Attr::string("id", event.id.clone()),
        Attr::string("source", event.source.clone()),
        Attr::string("type", event.r#type.clone()),
    ];
    for (name, value) in &event.attributes {
        let context = CONTEXT_ATTRIBUTES.contains(&name.as_str());
        let mismatch = |expected: &'static str| Error::Type {
            name: name.clone(),
            expected,
        };
        let (value, kind) = match value.attr.as_ref() {
            Some(Value::CeString(s)) => (s.clone(), AttrType::String),
            Some(Value::CeUri(s)) => (s.clone(), AttrType::Uri),
            Some(Value::CeUriRef(s)) => (s.clone(), AttrType::UriRef),
            Some(Value::CeTimestamp(t)) => (timestamp_string(t)?, AttrType::Timestamp),
            _ if context => return Err(mismatch("a string")),
            Some(Value::CeBoolean(b)) => (b.to_string(), AttrType::Boolean),
            Some(Value::CeInteger(i)) => (i.to_string(), AttrType::Integer),
            Some(Value::CeBytes(b)) => (BASE64.encode(b), AttrType::Binary),
            None => return Err(mismatch("a value")),
        };
        // A context attribute is a string whatever its protobuf type, as in
        // the JSON format.
        let kind = if context { AttrType::String } else { kind };
        attrs.push(Attr {
            name: name.clone(),
            value,
            kind,
        });
    }
    let data = match &event.data {
        None => None,
        Some(Data::BinaryData(bytes)) => Some(Bytes::copy_from_slice(bytes)),
        Some(Data::TextData(text)) => Some(Bytes::copy_from_slice(text.as_bytes())),
        Some(Data::ProtoData(_)) => {
            return Err(Error::Type {
                name: "proto_data".into(),
                expected: "binary_data or text_data",
            });
        }
    };
    CloudEvent::new(attrs, data)
}

/// Writes an event in the protobuf format: textual data as `text_data`,
/// anything else as `binary_data`.
pub fn to_proto(event: &CloudEvent) -> pb::CloudEvent {
    let mut attributes = BTreeMap::new();
    for attr in event.attrs() {
        if matches!(attr.name.as_str(), "specversion" | "id" | "source" | "type") {
            continue;
        }
        attributes.insert(
            attr.name.clone(),
            CloudEventAttributeValue {
                attr: Some(attribute_value(attr)),
            },
        );
    }
    let data = event.data().map(|bytes| match event.data_kind() {
        DataKind::Json | DataKind::Text => {
            Data::TextData(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => Data::BinaryData(bytes.to_vec()),
    });
    pb::CloudEvent {
        id: event.id().to_string(),
        source: event.source().to_string(),
        spec_version: event.attr("specversion").unwrap_or_default().to_string(),
        r#type: event.ty().to_string(),
        attributes,
        data,
    }
}

/// Reads a batch in the JSON format, as sent in `json_batch`.
pub fn from_json_batch(body: &[u8]) -> Result<Vec<CloudEvent>, String> {
    json::parse_batch(body).map_err(|err| err.to_string())
}

/// Reads a protobuf batch; the error names the event's index.
pub fn from_proto_batch(batch: &pb::CloudEventBatch) -> Result<Vec<CloudEvent>, String> {
    events_of(batch).map_err(|err| err.to_string())
}

fn events_of(batch: &pb::CloudEventBatch) -> Result<Vec<CloudEvent>, BatchError> {
    batch
        .events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            from_proto(event).map_err(|source| BatchError::Event { index, source })
        })
        .collect()
}

/// Encodes `events` as one `CloudEventBatch` message. The bytes are
/// deterministic: attributes are written in name order.
pub fn encode_batch(events: &[CloudEvent]) -> Bytes {
    let batch = pb::CloudEventBatch {
        events: events.iter().map(to_proto).collect(),
    };
    Bytes::from(batch.encode_to_vec())
}

/// Decodes a `CloudEventBatch` message and validates each event.
pub fn decode_batch(bytes: &[u8]) -> Result<Vec<CloudEvent>, BatchError> {
    events_of(&pb::CloudEventBatch::decode(bytes)?)
}
