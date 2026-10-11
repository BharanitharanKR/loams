//! WAL segments (§36 §4.3).

use bytes::Bytes;
use loams_cloudevents::{Attr, CloudEvent};
use loams_git::Seq;
use loams_git::format::FormatError;
use loams_git::format::event::{TxnBody, to_cloudevent};
use loams_git::format::segment::{
    SEGMENT_END, SEGMENT_HEADER_LEN, SEGMENT_MAGIC, WalBatch, decode_segment, encode_segment,
};
use loams_git::limits::MAX_SEGMENT_BYTES;
use proptest::prelude::*;

use crate::fixtures::{event, golden, golden_batch};

const CREATED: i64 = 1_790_762_400_123;

fn encoded(seq: Seq) -> Vec<u8> {
    encode_segment(seq, &golden_batch(seq), CREATED)
        .expect("encodes")
        .to_vec()
}

/// A batch of `n` reference transactions, each pushing `options` push
/// options of `option_len` bytes, at `seq`.
fn batch(seq: Seq, keys: &[String], options: usize, option_len: usize, id: [u8; 16]) -> WalBatch {
    let events = keys
        .iter()
        .map(|key| {
            let mut txn = crate::fixtures::ref_txn();
            txn.push_options = vec!["o".repeat(option_len); options];
            to_cloudevent(&event(key, TxnBody::RefTxn(txn), false), seq)
        })
        .collect();
    WalBatch {
        batch_id: id,
        events,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn segment_round_trip(
        seq in 1u64..u64::MAX,
        keys in prop::collection::vec("[a-f0-9]{8,64}", 0..12),
        options in 0usize..8,
        option_len in prop_oneof![0usize..64, 16_000usize..24_000],
        id in any::<[u8; 16]>(),
        created in any::<i64>(),
    ) {
        let seq = Seq(seq);
        let batch = batch(seq, &keys, options, option_len, id);
        match encode_segment(seq, &batch, created) {
            Ok(bytes) => {
                let back = decode_segment(seq, &bytes).expect("decodes");
                prop_assert_eq!(back.seq, seq);
                prop_assert_eq!(back.created_unix_ms, created);
                prop_assert_eq!(back.batch, batch);
            }
            Err(FormatError::TooLarge { len, max }) => {
                prop_assert!(len > MAX_SEGMENT_BYTES && max == MAX_SEGMENT_BYTES);
            }
            Err(other) => prop_assert!(false, "unexpected {other}"),
        }
    }
}

#[test]
fn segment_over_one_mib_is_too_large() {
    let seq = Seq(3);
    // One event whose data alone is just under the limit fits; a little more
    // does not.
    let event_with_options = |len: usize| {
        let mut txn = crate::fixtures::ref_txn();
        txn.push_options = vec!["o".repeat(len)];
        WalBatch {
            batch_id: [1; 16],
            events: vec![to_cloudevent(
                &event("k1", TxnBody::RefTxn(txn), false),
                seq,
            )],
        }
    };
    let limit = MAX_SEGMENT_BYTES as usize;
    let fits = event_with_options(limit - 2048);
    let body = encode_segment(seq, &fits, 0).expect("under the limit");
    assert!(body.len() <= SEGMENT_HEADER_LEN + limit + 12);
    let too_big = event_with_options(limit);
    match encode_segment(seq, &too_big, 0) {
        Err(FormatError::TooLarge { len, max }) => {
            assert!(len > MAX_SEGMENT_BYTES);
            assert_eq!(max, MAX_SEGMENT_BYTES);
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[test]
fn corrupt_crc_is_error() {
    let seq = Seq(5);
    let bytes = encoded(seq);
    // Flip one bit in the header, the body and the CRC itself.
    for at in [12, SEGMENT_HEADER_LEN + 3, bytes.len() - 12] {
        let mut bad = bytes.clone();
        bad[at] ^= 0x10;
        match decode_segment(seq, &bad) {
            Err(FormatError::Corrupt { reason }) => assert!(reason.contains("CRC"), "{reason}"),
            other => panic!("byte {at}: expected Corrupt, got {other:?}"),
        }
    }
    let mut bad_end = bytes.clone();
    let last = bad_end.len() - 1;
    bad_end[last] = b'X';
    assert!(matches!(
        decode_segment(seq, &bad_end),
        Err(FormatError::Corrupt { .. })
    ));
    let mut bad_magic = bytes;
    bad_magic[0] = b'X';
    assert!(matches!(
        decode_segment(seq, &bad_magic),
        Err(FormatError::Corrupt { .. })
    ));
}

#[test]
fn truncated_segment_is_error() {
    let seq = Seq(5);
    let bytes = encoded(seq);
    for len in [
        0,
        7,
        10,
        SEGMENT_HEADER_LEN - 1,
        SEGMENT_HEADER_LEN,
        bytes.len() - 13,
        bytes.len() - 1,
    ] {
        assert!(
            matches!(
                decode_segment(seq, &bytes[..len]),
                Err(FormatError::Corrupt { .. })
            ),
            "length {len}"
        );
    }
    let mut longer = bytes;
    longer.push(0);
    assert!(matches!(
        decode_segment(seq, &longer),
        Err(FormatError::Corrupt { .. })
    ));
}

#[test]
fn seq_mismatch_is_error() {
    let bytes = encoded(Seq(5));
    assert_eq!(
        decode_segment(Seq(6), &bytes),
        Err(FormatError::SeqMismatch {
            expected: Seq(6),
            found: Seq(5)
        })
    );
    // An event stamped with another seq is refused when writing.
    let batch = golden_batch(Seq(5));
    assert!(matches!(
        encode_segment(Seq(6), &batch, CREATED),
        Err(FormatError::Event { .. })
    ));
}

#[test]
fn golden_segment_v1() {
    let seq = Seq(41);
    let fresh = encoded(seq);
    let golden = golden("segment_v1.lgw", &fresh);
    assert_eq!(&golden[..8], SEGMENT_MAGIC);
    assert_eq!(&golden[golden.len() - 8..], SEGMENT_END);
    let segment = decode_segment(seq, &golden).expect("the golden segment decodes");
    assert_eq!(segment.batch, golden_batch(seq));
    assert_eq!(segment.created_unix_ms, CREATED);
    assert_eq!(
        fresh, golden,
        "encoding the golden batch reproduces the file byte for byte"
    );
}

/// Readers accept versions N and N−1 (§03 §6). Version 1 is the first, so
/// today version 0 and version 2 are refused. When version 2 arrives, this
/// test keeps decoding `segment_v1.lgw` with the version 2 reader.
#[test]
fn n_minus_one_is_read() {
    let seq = Seq(41);
    let v1 = golden("segment_v1.lgw", &encoded(seq));
    assert!(decode_segment(seq, &v1).is_ok(), "N reads version 1");
    for version in [0u16, 2] {
        let mut other = v1.clone();
        other[8..10].copy_from_slice(&version.to_le_bytes());
        assert_eq!(
            decode_segment(seq, &other),
            Err(FormatError::UnknownVersion { found: version })
        );
    }
}

#[test]
fn an_event_that_is_not_a_transaction_still_round_trips() {
    // Segments carry CloudEvents; validating them as transactions is the
    // state machine's job, so a segment keeps any event with its loamsseq.
    let seq = Seq(9);
    let event = CloudEvent::new(
        vec![
            Attr::string("specversion", "1.0"),
            Attr::string("id", "x"),
            Attr::string("source", "/s"),
            Attr::string("type", "t"),
            Attr::string("loamsseq", "9"),
        ],
        Some(Bytes::from_static(b"\x00\x01")),
    )
    .expect("event");
    let batch = WalBatch {
        batch_id: [7; 16],
        events: vec![event],
    };
    let bytes = encode_segment(seq, &batch, 1).expect("encodes");
    assert_eq!(decode_segment(seq, &bytes).expect("decodes").batch, batch);
}
