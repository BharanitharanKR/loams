//! Checkpoints, `checkpoints/<seq:020>.lgc` (§36 §4.3, format version 1).
//!
//! ```text
//! header   magic "LGITCKPT" (8) | format_version u16 LE = 1 | seq u64 LE
//! body     loams.git.v1.Checkpoint (protobuf; maps in key order)
//! trailer  crc32c u32 LE over header ‖ body | magic "LGITCEND" (8)
//! ```
//!
//! There is no size cap: a repository with a million refs has a large
//! checkpoint, written with a multipart upload above 64 MiB.

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;

use super::{FormatError, Reader, crc32c_of, pb};

/// The checkpoint header's magic.
pub const CHECKPOINT_MAGIC: &[u8; 8] = b"LGITCKPT";
/// The checkpoint trailer's magic.
pub const CHECKPOINT_END: &[u8; 8] = b"LGITCEND";
/// The format version this crate writes.
pub const CHECKPOINT_VERSION: u16 = 1;
/// The oldest format version this crate reads (N−1; see `segment`).
pub const CHECKPOINT_MIN_VERSION: u16 = 1;
/// Header bytes before the body.
pub const CHECKPOINT_HEADER_LEN: usize = 8 + 2 + 8;
/// Trailer bytes after the body.
pub const CHECKPOINT_TRAILER_LEN: usize = 4 + 8;

/// Encodes a checkpoint; the header's seq is `c.seq`. The bytes are
/// deterministic, so a retried PUT of an equal checkpoint writes equal bytes.
pub fn encode_checkpoint(c: &pb::Checkpoint) -> Bytes {
    let body = c.encode_to_vec();
    let mut out =
        BytesMut::with_capacity(CHECKPOINT_HEADER_LEN + body.len() + CHECKPOINT_TRAILER_LEN);
    out.put_slice(CHECKPOINT_MAGIC);
    out.put_u16_le(CHECKPOINT_VERSION);
    out.put_u64_le(c.seq);
    out.put_slice(&body);
    let crc = crc32c_of(&[&out]);
    out.put_u32_le(crc);
    out.put_slice(CHECKPOINT_END);
    out.freeze()
}

/// Decodes a checkpoint. Errors: `UnknownVersion`; `Corrupt` for a bad
/// magic, CRC or body, or a header seq that differs from the body's.
pub fn decode_checkpoint(bytes: &[u8]) -> Result<pb::Checkpoint, FormatError> {
    if bytes.len() < CHECKPOINT_HEADER_LEN + CHECKPOINT_TRAILER_LEN {
        return Err(FormatError::corrupt("truncated"));
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *CHECKPOINT_MAGIC {
        return Err(FormatError::corrupt("not a checkpoint: bad magic"));
    }
    let version = reader.u16()?;
    if !(CHECKPOINT_MIN_VERSION..=CHECKPOINT_VERSION).contains(&version) {
        return Err(FormatError::UnknownVersion { found: version });
    }
    let seq = reader.u64()?;
    let body_end = bytes.len() - CHECKPOINT_TRAILER_LEN;
    let mut trailer = Reader::new(&bytes[body_end..]);
    let crc = trailer.u32()?;
    if trailer.array::<8>()? != *CHECKPOINT_END {
        return Err(FormatError::corrupt("bad checkpoint end magic"));
    }
    if crc != crc32c_of(&[&bytes[..body_end]]) {
        return Err(FormatError::corrupt("checkpoint CRC mismatch"));
    }
    let checkpoint = pb::Checkpoint::decode(&bytes[CHECKPOINT_HEADER_LEN..body_end])
        .map_err(|err| FormatError::corrupt(format!("checkpoint body: {err}")))?;
    if checkpoint.seq != seq {
        return Err(FormatError::corrupt(format!(
            "checkpoint header seq {seq} differs from its body's {}",
            checkpoint.seq
        )));
    }
    Ok(checkpoint)
}
