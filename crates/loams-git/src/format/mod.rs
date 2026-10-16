//! The `loams.git.v1` on-disk formats (§36 §4.3, §19.4.2, D390, D415): WAL
//! segments, checkpoints, pack objects, the CloudEvents each transaction is,
//! and workspace snapshots.
//!
//! Every format is little-endian, framed by a magic at each end and checked
//! by CRC32C (Castagnoli). Readers accept format versions N and N−1 (§03 §6);
//! version 1 is the first, so today only 1 is read.

pub mod checkpoint;
pub mod event;
pub mod lpk;
pub mod segment;
pub mod snapshot;

use crate::ids::Seq;

/// The generated `loams.git.v1` messages (prost; never exposed on the API,
/// Ruling 20).
#[allow(clippy::all, missing_docs, missing_debug_implementations)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/loams.git.v1.rs"));
}

/// Why bytes could not be encoded or decoded in a `loams.git.v1` format.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    /// An encoded body is larger than its format allows.
    #[error("body of {len} bytes is larger than the {max}-byte limit")]
    TooLarge {
        /// The body's length.
        len: u64,
        /// The limit.
        max: u64,
    },
    /// The bytes are not a valid object of the format: a bad magic, CRC,
    /// length or body.
    #[error("corrupt: {reason}")]
    Corrupt {
        /// What is wrong.
        reason: String,
    },
    /// A segment holds another sequence number than the one its name says.
    #[error("segment holds seq {found}, expected {expected}")]
    SeqMismatch {
        /// The seq the caller read.
        expected: Seq,
        /// The seq in the header.
        found: Seq,
    },
    /// A format version this reader does not know (it reads N and N−1).
    #[error("unknown format version {found}")]
    UnknownVersion {
        /// The version in the header.
        found: u16,
    },
    /// A CloudEvent that is not a valid Loams Git transaction event (D415).
    #[error("invalid git event: {reason}")]
    Event {
        /// What is wrong.
        reason: String,
    },
}

impl FormatError {
    pub(crate) fn corrupt(reason: impl Into<String>) -> Self {
        Self::Corrupt {
            reason: reason.into(),
        }
    }

    pub(crate) fn event(reason: impl Into<String>) -> Self {
        Self::Event {
            reason: reason.into(),
        }
    }
}

/// CRC32C over the concatenation of `parts`.
pub(crate) fn crc32c_of(parts: &[&[u8]]) -> u32 {
    parts
        .iter()
        .fold(0, |crc, part| crc32c::crc32c_append(crc, part))
}

/// A little-endian reader over a byte slice that has been length-checked.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        if self.bytes.len() < n {
            return Err(FormatError::corrupt("truncated"));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub(crate) fn u16(&mut self) -> Result<u16, FormatError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, FormatError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, FormatError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, FormatError> {
        Ok(i64::from_le_bytes(self.array()?))
    }
}
