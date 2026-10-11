//! WAL segments, `wal/<seq:020>.lgw` (§36 §4.3, format version 1).
//!
//! ```text
//! header   magic "LGITWAL\0" (8) | format_version u16 LE = 1 | flags u16 = 0 | seq u64 LE
//!          | batch_id [16] | created_unix_ms i64 LE | body_len u32 LE
//! body     io.cloudevents.v1.CloudEventBatch (protobuf format)
//! trailer  crc32c u32 LE over header ‖ body | magic "LGITWEND" (8)
//! ```
//!
//! Every event of a segment carries `loamsseq` equal to the header's seq.

use bytes::{BufMut, Bytes, BytesMut};
use loams_cloudevents::CloudEvent;
use loams_cloudevents::protobuf::{decode_batch, encode_batch};

use super::{FormatError, Reader, crc32c_of};
use crate::ids::Seq;
use crate::limits::MAX_SEGMENT_BYTES;

/// The segment header's magic.
pub const SEGMENT_MAGIC: &[u8; 8] = b"LGITWAL\0";
/// The segment trailer's magic.
pub const SEGMENT_END: &[u8; 8] = b"LGITWEND";
/// The format version this crate writes.
pub const SEGMENT_VERSION: u16 = 1;
/// The oldest format version this crate reads (N−1, §03 §6; version 1 is
/// the first, so it is also 1).
pub const SEGMENT_MIN_VERSION: u16 = 1;
/// Header bytes before the body.
pub const SEGMENT_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 16 + 8 + 4;
/// Trailer bytes after the body.
pub const SEGMENT_TRAILER_LEN: usize = 4 + 8;

/// One batch of transaction events (§36 §5.1). `batch_id` is random per
/// batch, so a writer recognises its own segment after an unknown outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBatch {
    /// Random per batch.
    pub batch_id: [u8; 16],
    /// One event per transaction, in commit order.
    pub events: Vec<CloudEvent>,
}

/// A committed segment (§36 §5.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalSegment {
    /// The segment's sequence number.
    pub seq: Seq,
    /// Its batch.
    pub batch: WalBatch,
    /// The sequencer's clock when it wrote the segment.
    pub created_unix_ms: i64,
}

/// The `loamsseq` value of every event in segment `seq`.
fn seq_attr(seq: Seq) -> String {
    seq.0.to_string()
}

fn check_events(seq: Seq, events: &[CloudEvent]) -> Result<(), String> {
    let expected = seq_attr(seq);
    for (index, event) in events.iter().enumerate() {
        if event.attr("loamsseq") != Some(expected.as_str()) {
            return Err(format!(
                "event {index}: loamsseq {:?} is not the segment's seq {expected}",
                event.attr("loamsseq")
            ));
        }
    }
    Ok(())
}

/// Encodes segment `seq`. Refuses a body over `MAX_SEGMENT_BYTES` (1 MiB)
/// with `TooLarge`, and an event whose `loamsseq` is not `seq` with `Event`.
pub fn encode_segment(
    seq: Seq,
    batch: &WalBatch,
    created_unix_ms: i64,
) -> Result<Bytes, FormatError> {
    check_events(seq, &batch.events).map_err(FormatError::event)?;
    let body = encode_batch(&batch.events);
    let len = body.len() as u64;
    if len > MAX_SEGMENT_BYTES {
        return Err(FormatError::TooLarge {
            len,
            max: MAX_SEGMENT_BYTES,
        });
    }
    let mut out = BytesMut::with_capacity(SEGMENT_HEADER_LEN + body.len() + SEGMENT_TRAILER_LEN);
    out.put_slice(SEGMENT_MAGIC);
    out.put_u16_le(SEGMENT_VERSION);
    out.put_u16_le(0);
    out.put_u64_le(seq.0);
    out.put_slice(&batch.batch_id);
    out.put_i64_le(created_unix_ms);
    // The length fits: it is at most MAX_SEGMENT_BYTES.
    out.put_u32_le(len as u32);
    out.put_slice(&body);
    let crc = crc32c_of(&[&out]);
    out.put_u32_le(crc);
    out.put_slice(SEGMENT_END);
    Ok(out.freeze())
}

/// Decodes the segment stored as `wal/<expected_seq>.lgw`.
///
/// Errors: `UnknownVersion` for a version outside N−1..=N; `Corrupt` for a
/// bad magic, length, flags, CRC or body, or an event whose `loamsseq` is
/// not the header's seq; `SeqMismatch` when the header names another seq.
pub fn decode_segment(expected_seq: Seq, bytes: &[u8]) -> Result<WalSegment, FormatError> {
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *SEGMENT_MAGIC {
        return Err(FormatError::corrupt("not a segment: bad magic"));
    }
    let version = reader.u16()?;
    if !(SEGMENT_MIN_VERSION..=SEGMENT_VERSION).contains(&version) {
        return Err(FormatError::UnknownVersion { found: version });
    }
    if reader.u16()? != 0 {
        return Err(FormatError::corrupt("unknown segment flags"));
    }
    let seq = Seq(reader.u64()?);
    let batch_id = reader.array::<16>()?;
    let created_unix_ms = reader.i64()?;
    let body_len = reader.u32()? as usize;
    let expected_len = SEGMENT_HEADER_LEN + body_len + SEGMENT_TRAILER_LEN;
    if bytes.len() != expected_len {
        return Err(FormatError::corrupt(format!(
            "segment is {} bytes, its header says {expected_len}",
            bytes.len()
        )));
    }
    if body_len as u64 > MAX_SEGMENT_BYTES {
        return Err(FormatError::corrupt("segment body over 1 MiB"));
    }
    let body = reader.take(body_len)?;
    let crc = reader.u32()?;
    if reader.array::<8>()? != *SEGMENT_END {
        return Err(FormatError::corrupt("bad segment end magic"));
    }
    if crc != crc32c_of(&[&bytes[..SEGMENT_HEADER_LEN + body_len]]) {
        return Err(FormatError::corrupt("segment CRC mismatch"));
    }
    if seq != expected_seq {
        return Err(FormatError::SeqMismatch {
            expected: expected_seq,
            found: seq,
        });
    }
    let events = decode_batch(body).map_err(|err| FormatError::corrupt(err.to_string()))?;
    check_events(seq, &events).map_err(FormatError::corrupt)?;
    Ok(WalSegment {
        seq,
        batch: WalBatch { batch_id, events },
        created_unix_ms,
    })
}
