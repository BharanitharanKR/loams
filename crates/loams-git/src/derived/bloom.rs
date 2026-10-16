//! Changed-path Bloom filters (§36 §19.8, D833; plan GT1 Task 20), as
//! git's commit-graph format documents them (`gitformat-commit-graph(5)`,
//! chunks `BIDX` and `BDAT`; implemented from that page, not git's code).
//!
//! A commit's filter holds every path that differs between it and its first
//! parent (the empty tree for a root), and each of those paths' leading
//! directories. A path is hashed with the 32-bit murmur3 under two seeds,
//! and the filter's bits `h0 + i·h1` (`i` below the number of hashes,
//! modulo the filter's bits) are set. The filter is the fewest bytes that
//! hold `n · bits_per_entry` bits for `n` paths (the format page says
//! 64-bit words; git 2.55 writes bytes, and `tests/history.rs` compares
//! with it byte for byte). A commit that changes
//! nothing has the one-byte filter `00`; one whose paths, directories
//! included, number more than `BLOOM_MAX_CHANGED_PATHS` has `ff`, which
//! answers "maybe" to every path.
//!
//! Only hash version 2 (murmur3 over unsigned bytes) is written or read.
//! Version 1 filters (git's default before `commitGraph.changedPathsVersion`
//! 2) hash bytes at or above `0x80` differently; a layer that has them is
//! read as having no filters, so every check falls back to a tree diff.

use std::collections::BTreeSet;

use crate::limits::BLOOM_MAX_CHANGED_PATHS;

/// The hash version this crate writes and reads (murmur3 over unsigned
/// bytes).
pub const BLOOM_HASH_VERSION: u32 = 2;
/// Bit positions per path.
pub const BLOOM_NUM_HASHES: u32 = 7;
/// Bits per path, at least.
pub const BLOOM_BITS_PER_ENTRY: u32 = 10;
/// The two murmur3 seeds of the double hashing.
const SEEDS: [u32; 2] = [0x293a_e76f, 0x7e64_6e2c];

/// The `BDAT` header: hash version, number of hashes, bits per entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BloomSettings {
    /// The murmur3 variant: 2 (1 is not read).
    pub hash_version: u32,
    /// Bit positions per path.
    pub num_hashes: u32,
    /// Bits per path, at least.
    pub bits_per_entry: u32,
}

impl BloomSettings {
    /// What this crate writes.
    pub const DEFAULT: Self = Self {
        hash_version: BLOOM_HASH_VERSION,
        num_hashes: BLOOM_NUM_HASHES,
        bits_per_entry: BLOOM_BITS_PER_ENTRY,
    };
}

/// The 32-bit murmur3 of `data` under `seed` (the algorithm as
/// <https://en.wikipedia.org/wiki/MurmurHash#Algorithm> describes it, with
/// the four-byte blocks read little-endian).
pub fn murmur3_32(seed: u32, data: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let scramble = |k: u32| k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    let mut h = seed;
    let blocks = data.chunks_exact(4);
    let tail = blocks.remainder();
    for block in blocks {
        let k = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        h ^= scramble(k);
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let mut k = 0u32;
    for (i, byte) in tail.iter().enumerate() {
        k |= u32::from(*byte) << (8 * i);
    }
    if !tail.is_empty() {
        h ^= scramble(k);
    }
    // The length is mixed in modulo 2^32, as the algorithm does.
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

/// The bit positions of `path` in a filter of `bits` bits.
fn positions(settings: &BloomSettings, path: &[u8], bits: u64) -> impl Iterator<Item = u64> {
    let h0 = murmur3_32(SEEDS[0], path);
    let h1 = murmur3_32(SEEDS[1], path);
    (0..settings.num_hashes).map(move |i| u64::from(h0.wrapping_add(i.wrapping_mul(h1))) % bits)
}

/// What a commit changed against its first parent, for its filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangedPaths {
    /// More than `BLOOM_MAX_CHANGED_PATHS` files changed (so more paths).
    TooMany,
    /// The changed files (not directories), each once.
    Files(BTreeSet<Vec<u8>>),
}

/// The filter of a commit whose first-parent diff changed `changed`.
pub fn build_filter(settings: &BloomSettings, changed: &ChangedPaths) -> Vec<u8> {
    let files = match changed {
        ChangedPaths::TooMany => return vec![0xff],
        ChangedPaths::Files(files) if files.len() > BLOOM_MAX_CHANGED_PATHS => {
            return vec![0xff];
        }
        ChangedPaths::Files(files) => files,
    };
    let mut keys: BTreeSet<&[u8]> = BTreeSet::new();
    for file in files {
        let mut path: &[u8] = file;
        loop {
            keys.insert(path);
            match path.iter().rposition(|b| *b == b'/') {
                Some(slash) => path = &path[..slash],
                None => break,
            }
        }
    }
    if keys.is_empty() {
        return vec![0];
    }
    // git counts the directories too (checked against git 2.55).
    if keys.len() > BLOOM_MAX_CHANGED_PATHS {
        return vec![0xff];
    }
    let bits = keys.len() as u64 * u64::from(settings.bits_per_entry);
    let len = bits.div_ceil(8);
    let mut filter = vec![0u8; len as usize];
    let bits = len * 8;
    for key in keys {
        for pos in positions(settings, key, bits) {
            filter[(pos / 8) as usize] |= 1 << (pos % 8);
        }
    }
    filter
}

/// Whether the commit whose filter is `filter` may have changed `path` (a
/// file or a directory, without a trailing `/`). `false` is certain; `true`
/// needs a tree diff to confirm.
pub fn maybe_contains(settings: &BloomSettings, filter: &[u8], path: &[u8]) -> bool {
    if filter.is_empty() {
        return true;
    }
    let bits = filter.len() as u64 * 8;
    positions(settings, path, bits).all(|pos| filter[(pos / 8) as usize] & (1 << (pos % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur3_known_answers() {
        // The reference vectors of the 32-bit x86 variant.
        assert_eq!(murmur3_32(0, b""), 0);
        assert_eq!(murmur3_32(1, b""), 0x514e_28b7);
        assert_eq!(murmur3_32(0xffff_ffff, b""), 0x81f1_6f39);
        assert_eq!(murmur3_32(0, b"\xff\xff\xff\xff"), 0x7629_3b50);
        assert_eq!(murmur3_32(0, b"\x21\x43\x65\x87"), 0xf55b_516b);
        assert_eq!(murmur3_32(0x5082_edee, b"\x21\x43\x65\x87"), 0x2362_f9de);
        assert_eq!(murmur3_32(0, b"\x21\x43\x65"), 0x7e4a_8634);
        assert_eq!(murmur3_32(0, b"\x21\x43"), 0xa0f7_b07a);
        assert_eq!(murmur3_32(0, b"\x21"), 0x7266_1cf4);
        assert_eq!(murmur3_32(0x9747_b28c, b"Hello, world!"), 0x2488_4cba);
    }

    #[test]
    fn filters_hold_their_paths_and_directories() {
        let settings = BloomSettings::DEFAULT;
        let files: BTreeSet<Vec<u8>> = [b"src/deep/a.rs".to_vec(), b"README".to_vec()].into();
        let filter = build_filter(&settings, &ChangedPaths::Files(files));
        // Four keys: 40 bits, five bytes.
        assert_eq!(filter.len(), 5);
        for path in [&b"src/deep/a.rs"[..], b"src/deep", b"src", b"README"] {
            assert!(maybe_contains(&settings, &filter, path), "{path:?}");
        }
        assert_eq!(
            build_filter(&settings, &ChangedPaths::Files(BTreeSet::new())),
            vec![0]
        );
        assert!(!maybe_contains(&settings, &[0], b"anything"));
        assert_eq!(build_filter(&settings, &ChangedPaths::TooMany), vec![0xff]);
        assert!(maybe_contains(&settings, &[0xff], b"anything"));
    }
}
