//! The WAL object format.

use std::collections::BTreeMap;

use bytes::Bytes;
use loams_common::{NamespaceId, StreamId};
use loams_log::wal::{self, WalObjectBuilder};
use loams_log::{Encoding, LogError, Record, batch, paths};
use loams_meta::WalClass;
use proptest::prelude::*;
use ulid::Ulid;

fn records(tag: u64, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: Some(Bytes::from(format!("{tag}-{i}"))),
            value: Some(Bytes::from(vec![tag as u8; i + 1])),
            headers: vec![],
            timestamp_ms: 1_000 * tag as i64 + i as i64,
        })
        .collect()
}

/// Pushes one batch per `(stream, partition, count)` and returns the object
/// plus the records pushed per partition, in order.
fn build(pushes: &[(u64, u32, usize)]) -> (Bytes, BTreeMap<(u64, u32), Vec<Record>>) {
    let mut builder = WalObjectBuilder::new(7, WalClass::Standard, Ulid::from_parts(1_234, 99));
    let mut expected: BTreeMap<(u64, u32), Vec<Record>> = BTreeMap::new();
    for (i, &(stream, partition, count)) in pushes.iter().enumerate() {
        let batch_records = records(i as u64, count);
        let max_ts = batch_records
            .iter()
            .map(|r| r.timestamp_ms)
            .max()
            .expect("non-empty");
        let encoded = batch::encode(&batch_records).expect("encode");
        builder.push(StreamId(stream), partition, encoded, count as u32, max_ts);
        expected
            .entry((stream, partition))
            .or_default()
            .extend(batch_records);
    }
    let (bytes, _) = builder.finish();
    (bytes, expected)
}

fn small_object() -> Bytes {
    build(&[(1, 0, 2), (2, 1, 1), (1, 0, 1)]).0
}

proptest! {
    #[test]
    fn wal_objects_round_trip(
        pushes in prop::collection::vec((1u64..4, 0u32..3, 1usize..5), 1..12),
    ) {
        let (bytes, expected) = build(&pushes);
        let (header, metas) = wal::parse(&bytes).unwrap();
        prop_assert_eq!(header.version, 1);
        prop_assert_eq!(header.class, WalClass::Standard);
        prop_assert_eq!(header.node_id, 7);
        prop_assert_eq!(header.ulid, Ulid::from_parts(1_234, 99));
        let keys: Vec<(u64, u32)> = metas.iter().map(|m| (m.stream.0, m.partition)).collect();
        let expected_keys: Vec<(u64, u32)> = expected.keys().copied().collect();
        prop_assert_eq!(keys, expected_keys);
        for meta in &metas {
            let want = &expected[&(meta.stream.0, meta.partition)];
            prop_assert_eq!(meta.records as usize, want.len());
            prop_assert_eq!(meta.encoding, Encoding::Kafka);
            prop_assert_eq!(meta.byte_range(), meta.offset..meta.offset + meta.len);
            let chunk = wal::read_chunk(&bytes, meta).unwrap();
            let got: Vec<Record> = batch::decode(chunk, 0).unwrap().into_iter().map(|r| r.record).collect();
            prop_assert_eq!(&got, want);
            let max = want.iter().map(|r| r.timestamp_ms).max().unwrap();
            prop_assert_eq!(meta.max_timestamp_ms, max);
        }
    }
}

#[test]
fn every_single_byte_corruption_is_detected() {
    let bytes = small_object();
    let (_, metas) = wal::parse(&bytes).unwrap();
    for at in 0..bytes.len() {
        let mut corrupted = bytes.to_vec();
        corrupted[at] ^= 0x01;
        let err = wal::parse(&corrupted).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "byte {at}: {err:?}");
        // A chunk read with the good index still catches payload corruption.
        for meta in &metas {
            if meta.byte_range().contains(&(at as u64)) {
                let err = wal::read_chunk(&corrupted, meta).unwrap_err();
                assert!(matches!(err, LogError::Corrupt(_)), "byte {at}: {err:?}");
            }
        }
    }
}

#[test]
fn truncated_objects_are_rejected() {
    let bytes = small_object();
    for len in 0..bytes.len() {
        let err = wal::parse(&bytes[..len]).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "len {len}: {err:?}");
    }
    let (_, metas) = wal::parse(&bytes).unwrap();
    let last = metas.last().unwrap();
    let short = &bytes[..(last.offset + last.len - 1) as usize];
    assert!(matches!(
        wal::read_chunk(short, last),
        Err(LogError::Corrupt(_))
    ));
}

#[test]
fn an_unknown_version_is_rejected() {
    let mut bytes = small_object().to_vec();
    bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
    match wal::parse(&bytes) {
        Err(LogError::Corrupt(message)) => assert!(message.contains("version"), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_arrow_chunk_is_unsupported_on_read() {
    let bytes = small_object();
    let (_, metas) = wal::parse(&bytes).unwrap();
    let mut arrow = metas[0].clone();
    arrow.encoding = Encoding::Arrow;
    assert!(matches!(
        wal::read_chunk(&bytes, &arrow),
        Err(LogError::UnsupportedEncoding(1))
    ));
}

#[test]
fn paths_follow_the_layout() {
    let ulid = Ulid::from_parts(1_700_000_000_000, 5);
    assert_eq!(
        paths::wal_object(WalClass::Standard, 3, ulid),
        format!("wal/standard/3/{ulid}.wal")
    );
    assert_eq!(
        paths::segment(NamespaceId(4), StreamId(9), 2, 1_234, ulid),
        format!("ns/4/streams/9/2/00000000000000001234-{ulid}.seg")
    );
    assert_eq!(ulid.to_string().len(), 26);
}

proptest! {
    /// Arbitrary bytes never make the parser panic.
    #[test]
    fn garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
        let _ = wal::parse(&bytes);
        let mut prefixed = b"OPNWAL\0\0\x01\0\0\0".to_vec();
        prefixed.extend_from_slice(&bytes);
        let _ = wal::parse(&prefixed);
    }
}

/// UP1 Task 1: the WAL reader checks the registry's readable range.
mod formats {
    use loams_common::formats::{self, FormatSpec, LOG_WAL};
    use loams_log::wal;

    /// `object` with its header's version set to `version` and the index
    /// checksum (which covers the header) fixed up.
    fn at_version(object: &[u8], version: u16) -> Vec<u8> {
        let mut out = object.to_vec();
        out[8..10].copy_from_slice(&version.to_le_bytes());
        let trailer_at = out.len() - wal::TRAILER_LEN;
        let index_offset =
            u64::from_le_bytes(out[trailer_at..trailer_at + 8].try_into().expect("8 bytes"))
                as usize;
        let crc = crc32c::crc32c_append(
            crc32c::crc32c(&out[..wal::HEADER_LEN]),
            &out[index_offset..trailer_at],
        );
        out[trailer_at + 12..trailer_at + 16].copy_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn format_constants_match_registry() {
        assert_eq!(wal::VERSION, formats::spec_of(LOG_WAL).current);
    }

    #[test]
    fn reader_accepts_previous_version() {
        let current = wal::VERSION;
        let older = at_version(&super::small_object(), current - 1);
        assert!(
            wal::parse(&older).is_err(),
            "N-1 refused while min_read is N"
        );
        let spec = FormatSpec {
            min_read: current - 1,
            ..formats::spec_of(LOG_WAL)
        };
        formats::with_override(spec, || {
            let (header, _) = wal::parse(&older).expect("N-1 opens once min_read allows it");
            assert_eq!(header.version, current - 1);
        });
    }

    #[test]
    fn reader_refuses_next_version() {
        let newer = at_version(&super::small_object(), wal::VERSION + 1);
        let err = wal::parse(&newer).unwrap_err();
        assert!(err.to_string().contains("format_unreadable"), "{err}");
    }
}
