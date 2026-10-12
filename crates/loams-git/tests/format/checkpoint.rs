//! Checkpoints (§36 §4.3).

use loams_git::format::FormatError;
use loams_git::format::checkpoint::{
    CHECKPOINT_END, CHECKPOINT_MAGIC, decode_checkpoint, encode_checkpoint,
};
use loams_git::format::pb;

use crate::fixtures::{golden, golden_checkpoint};

#[test]
fn checkpoint_round_trip() {
    let checkpoint = golden_checkpoint();
    let bytes = encode_checkpoint(&checkpoint);
    assert_eq!(decode_checkpoint(&bytes), Ok(checkpoint.clone()));
    // Equal checkpoints have equal bytes (refs are written in name order),
    // so a retried create-only PUT can compare bytes.
    let mut reordered = checkpoint.clone();
    reordered.refs = checkpoint.refs.clone().into_iter().rev().collect();
    assert_eq!(encode_checkpoint(&reordered), bytes);
    // The empty repository's checkpoint 0.
    let empty = pb::Checkpoint {
        object_format: pb::ObjectFormat::Sha1 as i32,
        ..Default::default()
    };
    assert_eq!(decode_checkpoint(&encode_checkpoint(&empty)), Ok(empty));
}

#[test]
fn corrupt_or_truncated_checkpoint_is_error() {
    let bytes = encode_checkpoint(&golden_checkpoint()).to_vec();
    for at in [0, 11, 20, bytes.len() - 10, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[at] ^= 0x01;
        assert!(
            matches!(decode_checkpoint(&bad), Err(FormatError::Corrupt { .. })),
            "byte {at}"
        );
    }
    for len in [0, 17, 29, bytes.len() - 1] {
        assert!(matches!(
            decode_checkpoint(&bytes[..len]),
            Err(FormatError::Corrupt { .. })
        ));
    }
    let mut v2 = bytes;
    v2[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_checkpoint(&v2),
        Err(FormatError::UnknownVersion { found: 2 })
    );
}

#[test]
fn golden_checkpoint_v1() {
    let fresh = encode_checkpoint(&golden_checkpoint()).to_vec();
    let golden = golden("checkpoint_v1.lgc", &fresh);
    assert_eq!(&golden[..8], CHECKPOINT_MAGIC);
    assert_eq!(&golden[golden.len() - 8..], CHECKPOINT_END);
    assert_eq!(decode_checkpoint(&golden), Ok(golden_checkpoint()));
    assert_eq!(
        fresh, golden,
        "encoding the golden checkpoint reproduces the file byte for byte"
    );
}
