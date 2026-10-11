//! The protobuf format (feature `protobuf`): events and batches round-trip
//! with every attribute's string form intact.

use bytes::Bytes;
use loams_cloudevents::protobuf::{
    BatchError, decode_batch, encode_batch, from_proto, from_proto_batch, pb, to_proto,
};
use loams_cloudevents::{Attr, AttrType, CloudEvent, format_time_ms};
use proptest::prelude::*;

/// The attributes as (name, value) pairs in name order: the protobuf format
/// keeps optional attributes in a map, so their order is not carried.
fn by_name(event: &CloudEvent) -> Vec<(String, String)> {
    let mut attrs: Vec<_> = event
        .attrs()
        .iter()
        .map(|attr| (attr.name.clone(), attr.value.clone()))
        .collect();
    attrs.sort();
    attrs
}

fn extension_kind(event: &CloudEvent, name: &str) -> Option<AttrType> {
    event
        .attrs()
        .iter()
        .find(|attr| attr.name == name)
        .map(|attr| attr.kind)
}

fn event(id: &str, extra: Vec<Attr>, data: Option<&'static [u8]>) -> CloudEvent {
    let mut attrs = vec![
        Attr::string("specversion", "1.0"),
        Attr::string("id", id),
        Attr::string("source", "/ns/1/repos/r01"),
        Attr::string("type", "io.loams.dev.git.reftxn.v1"),
    ];
    attrs.extend(extra);
    CloudEvent::new(attrs, data.map(Bytes::from_static)).expect("valid event")
}

/// A strategy for one extension: a name that is not a context attribute, a
/// value and its type. Values are valid for their type, as `CloudEvent::new`
/// requires, but not always in the form the protobuf type would print.
fn extension() -> impl Strategy<Value = Attr> {
    let name = "x[a-z0-9]{0,10}";
    let value = prop_oneof![
        any::<String>().prop_map(|v| (v, AttrType::String)),
        "[a-z]{1,8}:[a-z0-9/._-]{0,20}".prop_map(|v| (v, AttrType::Uri)),
        "[a-z0-9/._-]{0,20}".prop_map(|v| (v, AttrType::UriRef)),
        any::<i32>().prop_map(|v| (v.to_string(), AttrType::Integer)),
        any::<bool>().prop_map(|v| (v.to_string(), AttrType::Boolean)),
        // Canonical base64, unpadded base64 and text that is not base64.
        prop::collection::vec(any::<u8>(), 0..24).prop_map(|v| {
            use base64::Engine;
            (
                base64::engine::general_purpose::STANDARD.encode(v),
                AttrType::Binary,
            )
        }),
        "[A-Za-z0-9+/]{1,7}".prop_map(|v| (v, AttrType::Binary)),
        timestamp().prop_map(|v| (v, AttrType::Timestamp)),
    ];
    (name, value).prop_map(|(name, (value, kind))| Attr { name, value, kind })
}

/// RFC 3339 timestamps in the forms senders use: Loams' own millisecond
/// form, seconds only, a numeric offset and nanoseconds.
fn timestamp() -> impl Strategy<Value = String> {
    let ms = 0i64..4_102_444_800_000;
    prop_oneof![
        ms.clone().prop_map(format_time_ms),
        ms.clone()
            .prop_map(|ms| format_time_ms(ms - ms % 1000).replace(".000Z", "Z")),
        ms.clone()
            .prop_map(|ms| format_time_ms(ms).replace('Z', "+00:00")),
        (ms, 0u32..1_000_000)
            .prop_map(|(ms, ns)| { format_time_ms(ms).replace('Z', &format!("{ns:06}Z")) }),
    ]
}

fn arbitrary_event() -> impl Strategy<Value = CloudEvent> {
    (
        "[a-z0-9]{1,26}",
        prop::collection::btree_map("x[a-z0-9]{0,10}", extension(), 0..6),
        prop::option::of(timestamp()),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..64)),
        prop::option::of(prop_oneof![
            Just("application/protobuf"),
            Just("application/json"),
            Just("text/plain"),
        ]),
    )
        .prop_map(|(id, extensions, time, data, content_type)| {
            let mut attrs = vec![
                Attr::string("specversion", "1.0"),
                Attr::string("id", id),
                Attr::string("source", "/ns/1/repos/r01"),
                Attr::string("type", "io.loams.dev.git.reftxn.v1"),
                Attr::string("dataschema", "urn:loams:proto:loams.git.v1.RefTransaction"),
            ];
            if let Some(time) = time {
                attrs.push(Attr::string("time", time));
            }
            if let Some(content_type) = content_type {
                attrs.push(Attr::string("datacontenttype", content_type));
            }
            // The map's keys stand for the names, so they are unique.
            attrs.extend(extensions.into_iter().map(|(name, mut attr)| {
                attr.name = name;
                attr
            }));
            CloudEvent::new(attrs, data.map(Bytes::from)).expect("valid event")
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every attribute comes back as the exact string it was written with and
    /// the data keeps its bytes. Extensions keep their type, except a binary
    /// or timestamp value in a form its protobuf type would not print back,
    /// which travels as a string.
    #[test]
    fn proto_round_trip_preserves_attribute_strings(event in arbitrary_event()) {
        let back = from_proto(&to_proto(&event)).expect("decodes");
        prop_assert_eq!(by_name(&back), by_name(&event));
        for attr in event.attrs() {
            let context = loams_cloudevents::CONTEXT_ATTRIBUTES.contains(&attr.name.as_str());
            if context || matches!(attr.kind, AttrType::Binary | AttrType::Timestamp) {
                continue;
            }
            prop_assert_eq!(extension_kind(&back, &attr.name), Some(attr.kind), "{}", attr.name);
        }
        prop_assert_eq!(back.data(), event.data());
    }
}

#[test]
fn loams_time_form_survives_the_round_trip() {
    // Loams writes `time` with three fraction digits; the protobuf
    // timestamp alone would print ".25Z".
    let time = format_time_ms(1_790_762_400_250);
    assert_eq!(time, "2026-09-30T10:00:00.250Z");
    let event = event("t-1", vec![Attr::string("time", time.clone())], None);
    assert_eq!(
        from_proto(&to_proto(&event)).unwrap().attr("time"),
        Some(time.as_str())
    );
}

#[test]
fn typed_values_use_their_protobuf_types_when_the_form_is_canonical() {
    use pb::cloud_event::cloud_event_attribute_value::Attr as Value;
    let event = event(
        "t-2",
        vec![
            Attr::string("time", "2026-09-30T10:00:00.25Z"),
            Attr {
                name: "loamsseq".into(),
                value: "42".into(),
                kind: AttrType::String,
            },
            Attr {
                name: "retries".into(),
                value: "3".into(),
                kind: AttrType::Integer,
            },
        ],
        None,
    );
    let proto = to_proto(&event);
    let value = |name: &str| proto.attributes[name].attr.clone();
    assert!(matches!(value("time"), Some(Value::CeTimestamp(_))));
    assert_eq!(value("loamsseq"), Some(Value::CeString("42".into())));
    assert_eq!(value("retries"), Some(Value::CeInteger(3)));
}

#[test]
fn batch_round_trip() {
    let events = vec![
        event(
            "a1",
            vec![
                Attr::string("time", format_time_ms(1_790_762_400_000)),
                Attr::string("datacontenttype", "application/protobuf"),
                Attr::string("tenantid", "acme/1"),
                Attr::string("loamsseq", "7"),
            ],
            Some(b"\x0a\x04main\x00\xff"),
        ),
        event("a2", vec![Attr::string("loamsseq", "7")], None),
        event(
            "a3",
            vec![Attr::string("datacontenttype", "text/plain")],
            Some(b"hello"),
        ),
    ];
    let bytes = encode_batch(&events);
    let back = decode_batch(&bytes).expect("decodes");
    assert_eq!(back.len(), events.len());
    for (back, event) in back.iter().zip(&events) {
        assert_eq!(back.id(), event.id());
        assert_eq!(by_name(back), by_name(event));
        assert_eq!(back.data(), event.data());
    }
    // The encoding is deterministic: the attribute map is written in name
    // order, so equal batches have equal bytes.
    assert_eq!(encode_batch(&back), bytes);
    // The empty batch is the empty message.
    assert!(encode_batch(&[]).is_empty());
    assert_eq!(decode_batch(&[]).unwrap(), Vec::new());
}

#[test]
fn batch_errors_name_what_failed() {
    let err = decode_batch(b"\x0a\xff\xff").unwrap_err();
    assert!(matches!(err, BatchError::Decode(_)), "{err}");

    let good = to_proto(&event("b1", vec![], None));
    let mut bad = good.clone();
    bad.spec_version = "0.3".into();
    let batch = pb::CloudEventBatch {
        events: vec![good, bad],
    };
    let bytes = prost::Message::encode_to_vec(&batch);
    let err = decode_batch(&bytes).unwrap_err();
    assert!(matches!(err, BatchError::Event { index: 1, .. }), "{err}");
    assert!(err.to_string().starts_with("event 1: "), "{err}");
    assert_eq!(
        from_proto_batch(&batch).unwrap_err(),
        "event 1: unsupported specversion '0.3': only '1.0' is accepted"
    );
}
