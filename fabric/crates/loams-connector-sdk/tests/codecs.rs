//! CSV, NDJSON and Protobuf codecs (CN2 Task 5).
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use loams_connector_sdk::codecs::csv::CsvCodec;
use loams_connector_sdk::codecs::ndjson::NdjsonCodec;
use loams_connector_sdk::codecs::protobuf::ProtobufCodec;
use loams_connector_sdk::codecs::protobuf::prost_reflect::prost::Message as _;
use loams_connector_sdk::codecs::protobuf::prost_reflect::prost::encoding::decode_varint;
use loams_connector_sdk::codecs::protobuf::prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
    field_descriptor_proto::{Label, Type},
};
use loams_connector_sdk::codecs::protobuf::prost_reflect::{DynamicMessage, MapKey, Value};
use loams_connector_sdk::codecs::{ARROW_STREAM, Codec, RowRange};
use loams_connector_sdk::{CloudEvent, EventBatch};

// ------------------------------------------------------------ CSV, NDJSON --

/// Every scalar type both text formats carry, each column with a null.
fn scalar_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new("name", DataType::Utf8, true),
        Field::new("day", DataType::Date32, true),
        Field::new("clock", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn scalar_batch() -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(Int8Array::from(vec![Some(-128), Some(127), None])),
        Arc::new(Int16Array::from(vec![Some(-32768), Some(32767), None])),
        Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
        Arc::new(Float32Array::from(vec![Some(1.5), Some(-0.25), None])),
        Arc::new(Float64Array::from(vec![Some(1e300), Some(-2.5e-10), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(123_456_789_012), Some(-1), None])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![
            Some("plain"),
            Some("comma, \"quote\" and\nnewline"),
            None,
        ])),
        Arc::new(Date32Array::from(vec![Some(0), Some(20_000), None])),
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(0),
            Some(86_399_999_999),
            None,
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_760_000_000_123_456),
                Some(-1_000_000),
                None,
            ])
            .with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(scalar_schema(), columns).unwrap()
}

/// The test inputs are a few rows, well under one batch.
fn concat(batches: &[RecordBatch]) -> RecordBatch {
    assert_eq!(batches.len(), 1, "a few rows decode to one batch");
    batches[0].clone()
}

#[test]
fn csv_ndjson_roundtrip_types() {
    let batch = scalar_batch();

    // CSV: encode, decode with the same schema, and get the same batch.
    let csv = CsvCodec::new(scalar_schema());
    let bytes = csv.encode(std::slice::from_ref(&batch)).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(text.starts_with("flag,i8,i16,i32"), "a header row: {text}");
    let back = concat(&csv.decode(&bytes).unwrap());
    assert_eq!(back, batch, "CSV round trip\n{text}");

    // Without a header, and with another delimiter.
    let tsv = CsvCodec::new(scalar_schema())
        .with_header(false)
        .with_delimiter(b'\t');
    let bytes = tsv.encode(std::slice::from_ref(&batch)).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).starts_with("flag"));
    assert_eq!(concat(&tsv.decode(&bytes).unwrap()), batch);

    // NDJSON: the same scalars …
    let ndjson = NdjsonCodec::new(scalar_schema());
    let bytes = ndjson.encode(std::slice::from_ref(&batch)).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert_eq!(text.lines().count(), 3, "one object per line: {text}");
    assert_eq!(concat(&ndjson.decode(&bytes).unwrap()), batch, "{text}");

    // … plus the nested and binary types JSON can carry and CSV cannot.
    let item = Field::new("item", DataType::Utf8, true);
    let address = Fields::from(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("zip", DataType::Int32, true),
    ]);
    let nested_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("tags", DataType::List(Arc::new(item.clone())), true),
        Field::new("address", DataType::Struct(address.clone()), true),
        Field::new("blob", DataType::Binary, true),
    ]));
    let mut tags = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(item));
    tags.append_value([Some("a"), Some("b")]);
    tags.append_value(Vec::<Option<&str>>::new());
    tags.append_null();
    let address = StructArray::try_new(
        address,
        vec![
            Arc::new(StringArray::from(vec![Some("Pune"), None, None])),
            Arc::new(Int32Array::from(vec![Some(411001), Some(1), None])),
        ],
        Some(vec![true, true, false].into()),
    )
    .unwrap();
    let nested = RecordBatch::try_new(
        nested_schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values([1, 2, 3])),
            Arc::new(tags.finish()),
            Arc::new(address),
            Arc::new(BinaryArray::from(vec![
                Some(&b"\x00\xffbytes"[..]),
                Some(&b""[..]),
                None,
            ])),
        ],
    )
    .unwrap();
    let codec = NdjsonCodec::new(nested_schema);
    let bytes = codec.encode(std::slice::from_ref(&nested)).unwrap();
    assert_eq!(
        concat(&codec.decode(&bytes).unwrap()),
        nested,
        "{}",
        String::from_utf8_lossy(&bytes)
    );

    // Input that does not fit the schema is an error, not a coerced value.
    let e = CsvCodec::new(scalar_schema())
        .decode(b"flag,i8,i16,i32,i64,u64,f32,f64,amount,name,day,clock,at\ntrue,1,1,notanumber,,,,,,,,,\n")
        .unwrap_err();
    assert!(e.to_string().contains("decode"), "{e}");
    assert!(
        NdjsonCodec::new(scalar_schema())
            .decode(b"{\"i32\": \"x\"}\n")
            .is_err()
    );
}

// ---------------------------------------------------------------- bulk --

#[test]
fn bulk_path_has_no_row_decode() {
    const ROWS: usize = 10_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|i| format!("row-{i}")),
            )),
        ],
    )
    .unwrap();

    let codecs: Vec<Box<dyn Fn() -> Box<dyn Codec>>> = vec![
        Box::new({
            let s = schema.clone();
            move || Box::new(CsvCodec::new(s.clone()).with_batch_rows(4_096))
        }),
        Box::new({
            let s = schema.clone();
            move || Box::new(NdjsonCodec::new(s.clone()).with_batch_rows(4_096))
        }),
    ];
    for make in codecs {
        let codec = make();
        let bytes = codec.encode(std::slice::from_ref(&batch)).unwrap();

        // The bulk path: bytes → RecordBatches with one envelope each.
        let mut ranges = Vec::new();
        let out = codec
            .decode_bulk(&bytes, &mut |r: RowRange| {
                ranges.push(r);
                CloudEvent::new(
                    format!("run/0/{}-{}", r.first_row, r.last_row),
                    "/connectors/orders/export.csv",
                    "io.loams.dev.flow.s3.rows.v1",
                )
            })
            .unwrap();
        assert_eq!(out.len(), 3, "{} rows in batches of 4096", ROWS);
        let mut rows = 0;
        for ev in &out {
            let EventBatch::Arrow { batch, envelope } = ev else {
                panic!("the bulk path yields Arrow batches, not events");
            };
            assert_eq!(envelope.datacontenttype.as_deref(), Some(ARROW_STREAM));
            assert_eq!(batch.schema(), schema);
            rows += batch.num_rows();
        }
        assert_eq!(rows, ROWS);
        assert_eq!(
            ranges
                .iter()
                .map(|r| (r.first_row, r.last_row))
                .collect::<Vec<_>>(),
            [(0, 4095), (4096, 8191), (8192, 9999)]
        );

        let stats = codec.stats().snapshot();
        assert_eq!(stats.bulk_rows, ROWS as u64);
        assert_eq!(stats.bulk_batches, 3);
        assert_eq!(
            stats.row_decodes,
            0,
            "{}: the bulk path decoded rows one at a time",
            codec.content_type()
        );

        // The row path, for contrast, is the one that counts row decodes.
        let events = codec
            .decode_rows(&bytes, &mut |i| {
                CloudEvent::new(format!("r{i}"), "/s", "io.loams.dev.flow.http.item.v1")
            })
            .unwrap();
        assert_eq!(events.len(), ROWS);
        let first: serde_json::Value = serde_json::from_slice(&events[0].data).unwrap();
        assert_eq!(first, serde_json::json!({ "id": 0, "name": "row-0" }));
        assert_eq!(codec.stats().snapshot().row_decodes, ROWS as u64);
    }
}

// ------------------------------------------------------------ Protobuf --

fn field(name: &str, number: i32, ty: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_owned()),
        number: Some(number),
        r#type: Some(ty as i32),
        label: Some(label as i32),
        json_name: None,
        ..Default::default()
    }
}

fn typed(mut f: FieldDescriptorProto, type_name: &str) -> FieldDescriptorProto {
    f.type_name = Some(type_name.to_owned());
    f
}

/// `shop.proto` (proto3), written as a descriptor rather than compiled, so
/// the test needs no protoc:
///
/// ```proto
/// syntax = "proto3";
/// package shop;
/// enum Status { STATUS_UNKNOWN = 0; PAID = 1; SHIPPED = 2; }
/// message Address { string city = 1; uint32 zip = 2; }
/// message Line { string sku = 1; int32 qty = 2; }
/// message Order {
///   int64 id = 1;  string customer = 2;  double amount = 3;
///   optional int32 priority = 4;  Status status = 5;
///   repeated string tags = 6;  Address address = 7;
///   map<string, int64> counters = 8;  bytes blob = 9;  bool paid = 10;
///   uint64 big = 11;  float ratio = 12;  repeated Line lines = 13;
///   sint32 delta = 14;  fixed64 checksum = 15;
/// }
/// ```
fn shop_descriptor_set() -> Vec<u8> {
    let mut priority = field("priority", 4, Type::Int32, Label::Optional);
    priority.proto3_optional = Some(true);
    priority.oneof_index = Some(0);
    let counters_entry = DescriptorProto {
        name: Some("CountersEntry".to_owned()),
        field: vec![
            field("key", 1, Type::String, Label::Optional),
            field("value", 2, Type::Int64, Label::Optional),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let order = DescriptorProto {
        name: Some("Order".to_owned()),
        field: vec![
            field("id", 1, Type::Int64, Label::Optional),
            field("customer", 2, Type::String, Label::Optional),
            field("amount", 3, Type::Double, Label::Optional),
            priority,
            typed(
                field("status", 5, Type::Enum, Label::Optional),
                ".shop.Status",
            ),
            field("tags", 6, Type::String, Label::Repeated),
            typed(
                field("address", 7, Type::Message, Label::Optional),
                ".shop.Address",
            ),
            typed(
                field("counters", 8, Type::Message, Label::Repeated),
                ".shop.Order.CountersEntry",
            ),
            field("blob", 9, Type::Bytes, Label::Optional),
            field("paid", 10, Type::Bool, Label::Optional),
            field("big", 11, Type::Uint64, Label::Optional),
            field("ratio", 12, Type::Float, Label::Optional),
            typed(
                field("lines", 13, Type::Message, Label::Repeated),
                ".shop.Line",
            ),
            field("delta", 14, Type::Sint32, Label::Optional),
            field("checksum", 15, Type::Fixed64, Label::Optional),
        ],
        nested_type: vec![counters_entry],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("_priority".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("shop.proto".to_owned()),
        package: Some("shop".to_owned()),
        syntax: Some("proto3".to_owned()),
        message_type: vec![
            DescriptorProto {
                name: Some("Address".to_owned()),
                field: vec![
                    field("city", 1, Type::String, Label::Optional),
                    field("zip", 2, Type::Uint32, Label::Optional),
                ],
                ..Default::default()
            },
            DescriptorProto {
                name: Some("Line".to_owned()),
                field: vec![
                    field("sku", 1, Type::String, Label::Optional),
                    field("qty", 2, Type::Int32, Label::Optional),
                ],
                ..Default::default()
            },
            order,
        ],
        enum_type: vec![EnumDescriptorProto {
            name: Some("Status".to_owned()),
            value: ["STATUS_UNKNOWN", "PAID", "SHIPPED"]
                .iter()
                .enumerate()
                .map(|(i, n)| EnumValueDescriptorProto {
                    name: Some((*n).to_owned()),
                    number: Some(i as i32),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

#[test]
fn protobuf_descriptor_roundtrip() {
    let set = shop_descriptor_set();
    let codec = ProtobufCodec::new(&set, "shop.Order").unwrap();
    let desc = codec.descriptor().clone();

    // The Arrow schema follows the descriptor.
    let schema = codec.schema();
    let ty = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    let nullable = |name: &str| schema.field_with_name(name).unwrap().is_nullable();
    assert_eq!(ty("id"), DataType::Int64);
    assert_eq!(ty("customer"), DataType::Utf8);
    assert_eq!(ty("amount"), DataType::Float64);
    assert_eq!(ty("priority"), DataType::Int32);
    assert_eq!(ty("status"), DataType::Int32);
    assert_eq!(ty("blob"), DataType::Binary);
    assert_eq!(ty("paid"), DataType::Boolean);
    assert_eq!(ty("big"), DataType::UInt64);
    assert_eq!(ty("ratio"), DataType::Float32);
    assert_eq!(ty("delta"), DataType::Int32);
    assert_eq!(ty("checksum"), DataType::UInt64);
    assert!(matches!(ty("tags"), DataType::List(f) if f.data_type() == &DataType::Utf8));
    assert!(matches!(ty("address"), DataType::Struct(f) if f.len() == 2));
    assert!(
        matches!(ty("lines"), DataType::List(f) if matches!(f.data_type(), DataType::Struct(_)))
    );
    assert!(matches!(ty("counters"), DataType::Map(_, false)));
    // Presence decides nullability: proto3 scalars always have a value, an
    // `optional` scalar and a message field may be absent.
    assert!(!nullable("id") && !nullable("tags"));
    assert!(nullable("priority") && nullable("address"));

    // Three messages: full, sparse (defaults and absences), and a third.
    let mut full = DynamicMessage::new(desc.clone());
    full.set_field_by_name("id", Value::I64(-9_000_000_000));
    full.set_field_by_name("customer", Value::String("Asha".into()));
    full.set_field_by_name("amount", Value::F64(1234.5));
    full.set_field_by_name("priority", Value::I32(0)); // present, and zero
    full.set_field_by_name("status", Value::EnumNumber(2));
    full.set_field_by_name(
        "tags",
        Value::List(vec![Value::String("a".into()), Value::String("b".into())]),
    );
    let mut address = DynamicMessage::new(
        desc.parent_pool()
            .get_message_by_name("shop.Address")
            .unwrap(),
    );
    address.set_field_by_name("city", Value::String("Pune".into()));
    address.set_field_by_name("zip", Value::U32(411_001));
    full.set_field_by_name("address", Value::Message(address));
    full.set_field_by_name(
        "counters",
        Value::Map(HashMap::from([
            (MapKey::String("views".into()), Value::I64(10)),
            (MapKey::String("clicks".into()), Value::I64(-3)),
        ])),
    );
    full.set_field_by_name("blob", Value::Bytes(vec![0, 255, 7].into()));
    full.set_field_by_name("paid", Value::Bool(true));
    full.set_field_by_name("big", Value::U64(u64::MAX));
    full.set_field_by_name("ratio", Value::F32(0.25));
    let line = |sku: &str, qty: i32| {
        let mut l =
            DynamicMessage::new(desc.parent_pool().get_message_by_name("shop.Line").unwrap());
        l.set_field_by_name("sku", Value::String(sku.into()));
        l.set_field_by_name("qty", Value::I32(qty));
        Value::Message(l)
    };
    full.set_field_by_name("lines", Value::List(vec![line("x-1", 2), line("y-2", 1)]));
    full.set_field_by_name("delta", Value::I32(-7));
    full.set_field_by_name("checksum", Value::U64(0xdead_beef));

    let sparse = DynamicMessage::new(desc.clone());

    let mut third = DynamicMessage::new(desc.clone());
    third.set_field_by_name("id", Value::I64(3));
    third.set_field_by_name("status", Value::EnumNumber(7)); // an unknown enum number survives
    third.set_field_by_name("lines", Value::List(vec![line("z", 0)]));

    let wire: Vec<Vec<u8>> = [full, sparse, third]
        .iter()
        .map(|m| m.encode_to_vec())
        .collect();
    // `DynamicMessage` equality is structural (an explicitly set proto3
    // default differs from an absent one), so compare with what the wire
    // carries: the messages as decoded from their own bytes.
    let originals: Vec<DynamicMessage> = wire
        .iter()
        .map(|b| DynamicMessage::decode(desc.clone(), b.as_slice()).unwrap())
        .collect();

    // Decode: one message per row.
    let batch = codec.decode_messages(&wire).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema(), schema);
    let priority = batch
        .column_by_name("priority")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        priority.iter().collect::<Vec<_>>(),
        [Some(0), None, None],
        "an explicit zero is kept apart from an absent value"
    );
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values().to_vec(), [-9_000_000_000, 0, 3]);
    let address = batch.column_by_name("address").unwrap();
    let address = address.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(address.null_count(), 2, "absent messages are null structs");
    let zip = address.column_by_name("zip").unwrap();
    let zip = zip.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(zip.value(0), 411_001);

    // Encode back: the same messages.
    let encoded = codec.encode_messages(&batch).unwrap();
    assert_eq!(encoded.len(), 3);
    for (i, bytes) in encoded.iter().enumerate() {
        let back = DynamicMessage::decode(desc.clone(), bytes.as_slice()).unwrap();
        assert_eq!(back, originals[i], "message {i}");
    }

    // The `Codec` form: a length-delimited stream, the bulk path's input.
    let mut stream = Vec::new();
    for m in &originals {
        m.encode_length_delimited(&mut stream).unwrap();
    }
    let batches = codec.decode(&stream).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);
    // Map entries have no wire order, so compare messages, not bytes.
    let encoded = codec.encode(&batches).unwrap();
    let mut rest = encoded.as_slice();
    for (i, original) in originals.iter().enumerate() {
        let len = decode_varint(&mut rest).unwrap() as usize;
        let (one, tail) = rest.split_at(len);
        rest = tail;
        let back = DynamicMessage::decode(desc.clone(), one).unwrap();
        assert_eq!(&back, original, "delimited message {i}");
    }
    assert!(rest.is_empty());

    // A message the descriptor set does not have, and bytes that are not a
    // message, are errors.
    assert!(ProtobufCodec::new(&set, "shop.Missing").is_err());
    assert!(ProtobufCodec::new(b"not a descriptor set", "shop.Order").is_err());
    assert!(codec.decode_messages(&[vec![0xff, 0xff, 0xff]]).is_err());
}
