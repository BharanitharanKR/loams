//! Pack objects, `packs/<checksum>.lpk` (§36 §4.3, D390): the pack bytes as
//! received or generated, then the version 2 `.idx`, then a 32-byte footer.
//!
//! ```text
//! footer   magic "LGITLPK\0" (8) | pack_len u64 LE | idx_len u64 LE
//!          | crc32c u32 LE over pack ‖ idx | reserved u32 = 0
//! ```
//!
//! Range reads fetch the footer, then the idx, without the pack.

use super::{FormatError, Reader, crc32c_of};
use crate::ids::ObjectId;

/// The footer's magic.
pub const LPK_MAGIC: &[u8; 8] = b"LGITLPK\0";
/// The footer's length.
pub const LPK_FOOTER_LEN: usize = 32;

/// A pack object's footer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LpkFooter {
    /// Bytes of the pack section, which starts at offset 0.
    pub pack_len: u64,
    /// Bytes of the idx section, which starts at `pack_len`.
    pub idx_len: u64,
    /// CRC32C over the pack and idx sections, for scrubs.
    pub crc32c: u32,
}

impl LpkFooter {
    /// The idx section's offset in the object.
    pub fn idx_offset(&self) -> u64 {
        self.pack_len
    }

    /// The whole object's length.
    pub fn object_len(&self) -> u64 {
        self.pack_len + self.idx_len + LPK_FOOTER_LEN as u64
    }
}

/// The footer of the object `pack ‖ idx ‖ footer`.
pub fn encode_footer(pack: &[u8], idx: &[u8]) -> [u8; LPK_FOOTER_LEN] {
    let mut out = [0u8; LPK_FOOTER_LEN];
    out[..8].copy_from_slice(LPK_MAGIC);
    out[8..16].copy_from_slice(&(pack.len() as u64).to_le_bytes());
    out[16..24].copy_from_slice(&(idx.len() as u64).to_le_bytes());
    out[24..28].copy_from_slice(&crc32c_of(&[pack, idx]).to_le_bytes());
    out
}

/// Reads a footer. Errors: `Corrupt` for a bad magic, a reserved word that
/// is not 0, or section lengths that overflow.
pub fn decode_footer(tail: &[u8; LPK_FOOTER_LEN]) -> Result<LpkFooter, FormatError> {
    let mut reader = Reader::new(tail);
    if reader.array::<8>()? != *LPK_MAGIC {
        return Err(FormatError::corrupt("not a pack object: bad footer magic"));
    }
    let pack_len = reader.u64()?;
    let idx_len = reader.u64()?;
    let crc32c = reader.u32()?;
    if reader.u32()? != 0 {
        return Err(FormatError::corrupt(
            "pack object footer: reserved is not 0",
        ));
    }
    if pack_len
        .checked_add(idx_len)
        .and_then(|n| n.checked_add(LPK_FOOTER_LEN as u64))
        .is_none()
    {
        return Err(FormatError::corrupt("pack object footer: lengths overflow"));
    }
    Ok(LpkFooter {
        pack_len,
        idx_len,
        crc32c,
    })
}

/// The pack's checksum: its 20-byte SHA-1 trailer, which names the object.
/// The pack must start with `PACK` and hold a header and a trailer.
pub fn pack_checksum(pack: &[u8]) -> Result<ObjectId, FormatError> {
    const HEADER: usize = 12;
    const TRAILER: usize = 20;
    if pack.len() < HEADER + TRAILER || &pack[..4] != b"PACK" {
        return Err(FormatError::corrupt("not a git pack"));
    }
    ObjectId::try_from(&pack[pack.len() - TRAILER..])
        .map_err(|_| FormatError::corrupt("pack trailer is not a SHA-1"))
}
