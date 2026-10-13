//! The envelope of design §33 §6 (CN2 Task 4).
#![allow(clippy::unwrap_used)]

mod common;

use bytes::Bytes;
use common::*;
use loams_connector_sdk::testing::reference::{CounterConfig, CounterFactory, CounterHandle};
use loams_connector_sdk::*;

async fn read_from(factory: &CounterFactory, split: &Split, at: u64, n: usize) -> Vec<CloudEvent> {
    let mut src = factory.create(split).unwrap();
    src.open(split, Some(Position::from_u64(at))).await.unwrap();
    let batch = src
        .next(Budget {
            max_rows: n,
            max_bytes: usize::MAX,
        })
        .await
        .unwrap()
        .unwrap();
    src.close().await.unwrap();
    match batch.events {
        EventBatch::Events(v) => v,
        EventBatch::Arrow { .. } => unreachable!(),
    }
}

#[tokio::test]
async fn envelope_ids_stable_across_reread() {
    let factory = CounterFactory::new(
        CounterHandle::new(&["a"]),
        CounterConfig::default(),
        &instance(),
    );
    let split = Split::new("a", serde_json::json!({}));
    let first = read_from(&factory, &split, 10, 5).await;
    let again = read_from(&factory, &split, 10, 5).await;
    assert_eq!(
        first, again,
        "a re-read from the same position changed the events"
    );
    assert_eq!(first[0].id, "a/10");
    assert_eq!(first[0].ty, "io.loams.dev.flow.reference.record.v1");
    assert_eq!(first[0].source, "/connectors/orders/a");
    assert_eq!(first[0].extensions["loamsconnector"], "reference");
    assert_eq!(first[0].extensions["loamsinstance"], "orders");

    // The §33 §6 id forms.
    let i = instance();
    assert_eq!(ids::log_record("orders", 3, 42), "orders/3/42");
    assert_eq!(ids::object("bkt", "a/b.json", "e1"), "bkt/a/b.json@e1");
    assert_eq!(
        ids::change("0/16B3748", "public.t", "7"),
        "0/16B3748/public.t/7"
    );
    let run = ids::run_id(&i, "SELECT * FROM t", "snapshot 9");
    assert_eq!(run.len(), 16);
    assert!(run.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(
        run,
        ids::run_id(&i, "SELECT * FROM t", "snapshot 9"),
        "a retry of the run"
    );
    assert_ne!(
        run,
        ids::run_id(&i, "SELECT * FROM t", "snapshot 10"),
        "a later run"
    );
    assert_ne!(
        run,
        ids::run_id(
            &InstanceKey::new(8, "orders"),
            "SELECT * FROM t",
            "snapshot 9"
        )
    );
    assert_ne!(ids::run_id(&i, "ab", "c"), ids::run_id(&i, "a", "bc"));
    assert_eq!(ids::rows(&run, 2, 0, 65_535), format!("{run}/2/0-65535"));

    // CDC extensions.
    let b = EnvelopeBuilder::new("postgresql", &i);
    let ev = b
        .event("change", "public.t", ids::change("0/1", "public.t", "7"))
        .with_cdc(CdcOp::Update, "0/1");
    assert_eq!(ev.extensions["loamsop"], "u");
    assert_eq!(ev.extensions["loamslsn"], "0/1");
}

#[test]
fn passthrough_keeps_type_source_id() {
    let b = EnvelopeBuilder::new("kafka", &instance());

    // Kafka binary mode (`ce_` headers, D270).
    let headers: Vec<(&str, &[u8])> = vec![
        ("ce_specversion", b"1.0"),
        ("ce_id", b"order-1"),
        ("ce_source", b"/shop/orders"),
        ("ce_type", b"com.shop.order.created"),
        ("ce_subject", b"orders/1"),
        ("ce_traceparent", b"00-abc-def-01"),
        ("ce_loamsconnector", b"someone-else"),
        ("content-type", b"application/json"),
    ];
    let ev = b
        .from_binary(headers, Bytes::from_static(b"{\"total\":3}"))
        .unwrap();
    assert_eq!(ev.id, "order-1");
    assert_eq!(ev.source, "/shop/orders");
    assert_eq!(ev.ty, "com.shop.order.created");
    assert_eq!(ev.subject.as_deref(), Some("orders/1"));
    assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
    assert_eq!(ev.extensions["traceparent"], "00-abc-def-01");
    assert_eq!(ev.extensions["loamsconnector"], "kafka");
    assert_eq!(ev.extensions["loamsinstance"], "orders");
    assert_eq!(ev.data.as_ref(), b"{\"total\":3}");

    // HTTP binary mode (`ce-` headers).
    let headers: Vec<(&str, &[u8])> = vec![
        ("Ce-Specversion", b"1.0"),
        ("Ce-Id", b"evt-9"),
        ("Ce-Source", b"urn:x"),
        ("Ce-Type", b"x.y"),
    ];
    let ev = b.from_binary(headers, Bytes::new()).unwrap();
    assert_eq!(
        (ev.id.as_str(), ev.source.as_str(), ev.ty.as_str()),
        ("evt-9", "urn:x", "x.y")
    );

    // Structured mode, JSON data and base64 data.
    let body = br#"{"specversion":"1.0","id":"s-1","source":"/s","type":"t.s","data":{"a":1},"partitionkey":"k"}"#;
    let ev = b.from_structured_json(body).unwrap();
    assert_eq!(
        (ev.id.as_str(), ev.source.as_str(), ev.ty.as_str()),
        ("s-1", "/s", "t.s")
    );
    assert_eq!(ev.data.as_ref(), br#"{"a":1}"#);
    assert_eq!(ev.extensions["partitionkey"], "k");
    let body =
        br#"{"specversion":"1.0","id":"s-2","source":"/s","type":"t.s","data_base64":"aGk="}"#;
    assert_eq!(b.from_structured_json(body).unwrap().data.as_ref(), b"hi");

    // Not a CloudEvent: no pass-through; the connector wraps it instead.
    let headers: Vec<(&str, &[u8])> = vec![("ce_id", b"x"), ("ce_source", b"/s")];
    assert!(b.from_binary(headers, Bytes::new()).is_none(), "no ce_type");
    assert!(
        b.from_binary(Vec::<(&str, &[u8])>::new(), Bytes::new())
            .is_none()
    );
    assert!(b.from_structured_json(br#"{"id":"x"}"#).is_none());
    let wrapped = b.event("record", "orders", ids::log_record("orders", 0, 5));
    assert_eq!(wrapped.ty, "io.loams.dev.flow.kafka.record.v1");
    assert_eq!(wrapped.source, "/connectors/orders/orders");
    assert_eq!(wrapped.id, "orders/0/5");
    assert!(b.passthrough(CloudEvent::new("", "/s", "t")).is_err());
}
