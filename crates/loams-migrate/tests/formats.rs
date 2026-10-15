//! The format registry and the stack record (UP1 Task 1).

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use loams_migrate::formats::{
    self, FormatId, FormatSpec, REGISTRY, Store, WIDER_READ, check, with_override,
};
use loams_migrate::stack::{STACK_RECORD_VERSION, StackRecord};

#[test]
fn registry_ids_are_unique() {
    let mut ids: Vec<_> = REGISTRY.iter().map(|s| s.id).collect();
    ids.sort();
    let before = ids.len();
    ids.dedup();
    assert_eq!(before, ids.len(), "duplicate format ids in REGISTRY");
}

#[test]
fn registry_min_read_is_current_or_one_below() {
    for s in REGISTRY {
        assert!(s.current >= 1, "{} writes version 0", s.id);
        assert!(s.min_read >= 1, "{} reads version 0", s.id);
        assert!(s.min_read <= s.current, "{} reads above current", s.id);
        if !WIDER_READ.contains(&s.id) {
            assert!(
                s.current - s.min_read <= 1,
                "{} reads {}..={}: more than N-1 needs a WIDER_READ entry",
                s.id,
                s.min_read,
                s.current
            );
        }
    }
}

fn test_spec(current: u16, min_read: u16) -> FormatSpec {
    FormatSpec {
        id: FormatId("test.format"),
        store: Store::Bucket,
        current,
        min_read,
    }
}

#[test]
fn readable_range_accepts_n_and_n_minus_one() {
    let s = test_spec(3, 2);
    with_override(s, || {
        assert!(check(s.id, 3).is_ok());
        assert!(check(s.id, 2).is_ok());
    });
    let record = record_with(&[("test.format", 2)]);
    assert!(record.stored().readable_by(&[s]).is_ok());
}

#[test]
fn readable_range_refuses_n_plus_one_and_n_minus_two() {
    let s = test_spec(3, 2);
    with_override(s, || {
        let newer = check(s.id, 4).unwrap_err();
        assert!(newer.is_newer());
        assert_eq!((newer.min_read, newer.current), (2, 3));
        let older = check(s.id, 1).unwrap_err();
        assert!(!older.is_newer());
        assert!(
            older
                .to_string()
                .starts_with("format_unreadable: test.format"),
            "{older}"
        );
    });
    let newer = record_with(&[("test.format", 4)]);
    let err = newer.stored().readable_by(&[s]).unwrap_err();
    assert!(err.is_newer());
    let older = record_with(&[("test.format", 1)]);
    let err = older.stored().readable_by(&[s]).unwrap_err();
    assert!(!err.is_newer());
    // A format this binary does not know was written by a newer release.
    let unknown = record_with(&[("test.from_the_future", 1)]);
    let err = unknown.stored().readable_by(&[s]).unwrap_err();
    assert!(err.is_newer());
    assert_eq!(err.current, None);
}

#[test]
fn every_registry_id_has_its_constant_and_checks() {
    for s in REGISTRY {
        assert!(formats::check(s.id, s.current).is_ok(), "{}", s.id);
        assert!(formats::check(s.id, s.current + 1).is_err(), "{}", s.id);
    }
}

fn record_with(formats: &[(&str, u16)]) -> StackRecord {
    StackRecord {
        release: "0.1.0-preview.3".to_string(),
        formats: formats
            .iter()
            .map(|(id, v)| ((*id).to_string(), *v))
            .collect::<BTreeMap<_, _>>(),
        migrated_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1_792_000_000),
        image_digest: Some("sha256:00".to_string()),
    }
}

#[test]
fn stack_record_round_trips() {
    let record = StackRecord::current(
        "0.1.0-preview.3",
        REGISTRY,
        SystemTime::UNIX_EPOCH + Duration::from_millis(1_792_000_000_123),
        None,
    );
    let bytes = record.encode();
    assert_eq!(bytes[0], STACK_RECORD_VERSION);
    assert_eq!(StackRecord::decode(&bytes).unwrap(), record);
    assert!(record.stored().readable_by(REGISTRY).is_ok());
}

#[test]
fn stack_record_value_version_is_pinned() {
    let record = record_with(&[("log.wal", 1)]);
    let golden: &[u8] = &[
        0x01, // the value version
        0x0f, b'0', b'.', b'1', b'.', b'0', b'-', b'p', b'r', b'e', b'v', b'i', b'e', b'w', b'.',
        b'3', // release
        0x01, 0x07, b'l', b'o', b'g', b'.', b'w', b'a', b'l', 0x01, // formats
        0x80, 0x80, 0xbf, 0xd6, 0x06, 0x00, // migrated_at: secs, nanos
        0x01, 0x09, b's', b'h', b'a', b'2', b'5', b'6', b':', b'0', b'0', // image_digest
    ];
    assert_eq!(record.encode(), golden);
    assert_eq!(StackRecord::decode(golden).unwrap(), record);
    assert!(StackRecord::decode(&[0x02]).is_err());
    assert!(StackRecord::decode(&[]).is_err());
}
