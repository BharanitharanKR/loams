//! Transactions as CloudEvents (§36 §4.3's attribute table; D364, D415).

use bytes::Bytes;
use loams_cloudevents::{Attr, CloudEvent};
use loams_git::Seq;
use loams_git::format::FormatError;
use loams_git::format::event::{TxnBody, from_cloudevent, to_cloudevent};
use loams_git::format::pb;
use prost::Message;

use crate::fixtures::{event, ref_txn};

const KEY: &str = "6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c6d5b3c1f0e9a8b7c";

fn pairs(ce: &CloudEvent) -> Vec<(&str, &str)> {
    ce.attrs()
        .iter()
        .map(|attr| (attr.name.as_str(), attr.value.as_str()))
        .collect()
}

/// `ce` with attribute `name` set to `value` (`None` removes it).
fn with(ce: &CloudEvent, name: &str, value: Option<&str>) -> CloudEvent {
    let mut attrs: Vec<Attr> = ce
        .attrs()
        .iter()
        .filter(|attr| attr.name != name)
        .cloned()
        .collect();
    if let Some(value) = value {
        attrs.push(Attr::string(name, value));
    }
    CloudEvent::new(attrs, ce.data().cloned()).expect("still a CloudEvent")
}

#[test]
fn event_attributes_match_design_table() {
    let txn = event(KEY, TxnBody::RefTxn(ref_txn()), true);
    let ce = to_cloudevent(&txn, Seq(41));
    assert_eq!(
        pairs(&ce),
        [
            ("specversion", "1.0"),
            ("id", KEY),
            ("source", "/ns/7/repos/r01j9z3m5q8x0000000000000000"),
            ("type", "io.loams.dev.git.reftxn.v1"),
            ("datacontenttype", "application/protobuf"),
            ("dataschema", "urn:loams:proto:loams.git.v1.RefTransaction"),
            ("loamsseq", "41"),
            ("tenantid", "acme/prod"),
            ("time", "2026-09-30T10:00:00.250Z"),
            (
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            ),
        ]
    );
    assert_eq!(
        ce.data().map(|d| d.to_vec()),
        Some(ref_txn().encode_to_vec())
    );
    assert_eq!(from_cloudevent(&ce), Ok(txn));

    // The other two types, without a trace context.
    let packset = event(
        "01j9z3m5q8abcdefghjkmnpqrs",
        TxnBody::PackSet(pb::PackSetChange::default()),
        false,
    );
    let ce = to_cloudevent(&packset, Seq(1));
    assert_eq!(ce.ty(), "io.loams.dev.git.packset.v1");
    assert_eq!(
        ce.attr("dataschema"),
        Some("urn:loams:proto:loams.git.v1.PackSetChange")
    );
    assert_eq!(ce.attr("traceparent"), None);
    assert_eq!(from_cloudevent(&ce), Ok(packset));
    let config = event(
        "01j9z3m5q8abcdefghjkmnpqrt",
        TxnBody::Config(pb::ConfigChange::default()),
        false,
    );
    let ce = to_cloudevent(&config, Seq(2));
    assert_eq!(ce.ty(), "io.loams.dev.git.config.v1");
    assert_eq!(
        ce.attr("dataschema"),
        Some("urn:loams:proto:loams.git.v1.ConfigChange")
    );
    assert_eq!(from_cloudevent(&ce), Ok(config));
    // No `idempotencykey` or `schemaversion` extension (D364, D415).
    assert!(ce.attr("idempotencykey").is_none() && ce.attr("schemaversion").is_none());
}

#[test]
fn event_without_tenantid_is_refused() {
    let ce = to_cloudevent(&event(KEY, TxnBody::RefTxn(ref_txn()), false), Seq(3));
    for missing in [
        "tenantid",
        "loamsseq",
        "time",
        "dataschema",
        "datacontenttype",
    ] {
        match from_cloudevent(&with(&ce, missing, None)) {
            Err(FormatError::Event { reason }) => assert!(reason.contains(missing), "{reason}"),
            other => panic!("without {missing}: {other:?}"),
        }
    }
    assert!(matches!(
        from_cloudevent(&with(&ce, "tenantid", Some(""))),
        Err(FormatError::Event { .. })
    ));
}

#[test]
fn dataschema_must_match_type() {
    let ce = to_cloudevent(&event(KEY, TxnBody::RefTxn(ref_txn()), false), Seq(3));
    let wrong = with(
        &ce,
        "dataschema",
        Some("urn:loams:proto:loams.git.v1.ConfigChange"),
    );
    match from_cloudevent(&wrong) {
        Err(FormatError::Event { reason }) => {
            assert!(reason.contains("does not match"), "{reason}")
        }
        other => panic!("{other:?}"),
    }
    let unknown_type = with(&ce, "type", Some("io.loams.dev.git.other.v1"));
    assert!(matches!(
        from_cloudevent(&unknown_type),
        Err(FormatError::Event { .. })
    ));
    let v2 = with(&ce, "type", Some("io.loams.dev.git.reftxn.v2"));
    assert!(matches!(
        from_cloudevent(&v2),
        Err(FormatError::Event { .. })
    ));
}

#[test]
fn other_malformed_events_are_refused() {
    let ce = to_cloudevent(&event(KEY, TxnBody::RefTxn(ref_txn()), false), Seq(3));
    let cases = [
        with(&ce, "id", Some("not a key")),
        with(&ce, "source", Some("/ns/x/repos/r1")),
        with(&ce, "source", Some("/ns/7/repo/r1")),
        with(&ce, "traceparent", Some("00-zz")),
        with(&ce, "loamsseq", Some("007")),
        with(&ce, "datacontenttype", Some("application/json")),
        with(&ce, "idempotencykey", Some(KEY)),
        with(&ce, "subject", Some("x")),
    ];
    for case in cases {
        assert!(
            matches!(from_cloudevent(&case), Err(FormatError::Event { .. })),
            "{:?}",
            pairs(&case)
        );
    }
    let garbage = CloudEvent::new(
        ce.attrs().to_vec(),
        Some(Bytes::from_static(b"\xff\xff\xff")),
    )
    .expect("event");
    assert!(matches!(
        from_cloudevent(&garbage),
        Err(FormatError::Event { .. })
    ));
}
