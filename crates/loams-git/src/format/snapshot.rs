//! Workspace snapshots, `ws/<ws_id>/snap/<n:020>.lws` (§36 §19.4.2, format
//! version 1; plan GT1 Task 22).
//!
//! ```text
//! header   magic "LGITWSNP" (8) | format_version u16 LE = 1 | n u64 LE
//! body     loams.git.v1.WorkspaceSnapshot (protobuf), at most 1 MiB
//! trailer  crc32c u32 LE over header ‖ body | magic "LGITWSEN" (8)
//! ```
//!
//! A snapshot is complete, not a delta (Ruling 32): mounting reads one. Its
//! body is capped at `MAX_SNAPSHOT_BYTES`, so a PUT of it is atomic on every
//! store (§36 §4.2). [`snapshot_commit`] is the deterministic commit that
//! names a snapshot to stock git (Ruling 38, D835).

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;

use super::{FormatError, Reader, crc32c_of, pb};
use crate::ids::ObjectId;
use crate::limits::{MAX_CHANGED_HINT, MAX_SNAPSHOT_BYTES};

/// The snapshot header's magic.
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"LGITWSNP";
/// The snapshot trailer's magic.
pub const SNAPSHOT_END: &[u8; 8] = b"LGITWSEN";
/// The format version this crate writes.
pub const SNAPSHOT_VERSION: u16 = 1;
/// The oldest format version this crate reads (N−1; see `segment`).
pub const SNAPSHOT_MIN_VERSION: u16 = 1;
/// Header bytes before the body.
pub const SNAPSHOT_HEADER_LEN: usize = 8 + 2 + 8;
/// Trailer bytes after the body.
pub const SNAPSHOT_TRAILER_LEN: usize = 4 + 8;
/// The name and email of every snapshot commit (Ruling 38).
pub const SNAPSHOT_IDENT: &str = "Loams Snapshot <snapshot@loams.invalid>";

/// Encodes a snapshot; the header's `n` is `snap.n`. Equal snapshots have
/// equal bytes, so a retried create-only PUT can compare them. A body over
/// `MAX_SNAPSHOT_BYTES` is `TooLarge`.
pub fn encode_snapshot(snap: &pb::WorkspaceSnapshot) -> Result<Bytes, FormatError> {
    let body = snap.encode_to_vec();
    if body.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(FormatError::TooLarge {
            len: body.len() as u64,
            max: MAX_SNAPSHOT_BYTES,
        });
    }
    let mut out = BytesMut::with_capacity(SNAPSHOT_HEADER_LEN + body.len() + SNAPSHOT_TRAILER_LEN);
    out.put_slice(SNAPSHOT_MAGIC);
    out.put_u16_le(SNAPSHOT_VERSION);
    out.put_u64_le(snap.n);
    out.put_slice(&body);
    let crc = crc32c_of(&[&out]);
    out.put_u32_le(crc);
    out.put_slice(SNAPSHOT_END);
    Ok(out.freeze())
}

/// Decodes a snapshot. Errors: `UnknownVersion`; `TooLarge` for a body over
/// `MAX_SNAPSHOT_BYTES`; `Corrupt` for a bad magic, CRC or body, or a
/// header `n` that differs from the body's.
pub fn decode_snapshot(bytes: &[u8]) -> Result<pb::WorkspaceSnapshot, FormatError> {
    if bytes.len() < SNAPSHOT_HEADER_LEN + SNAPSHOT_TRAILER_LEN {
        return Err(FormatError::corrupt("truncated"));
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *SNAPSHOT_MAGIC {
        return Err(FormatError::corrupt("not a workspace snapshot: bad magic"));
    }
    let version = reader.u16()?;
    if !(SNAPSHOT_MIN_VERSION..=SNAPSHOT_VERSION).contains(&version) {
        return Err(FormatError::UnknownVersion { found: version });
    }
    let n = reader.u64()?;
    let body_end = bytes.len() - SNAPSHOT_TRAILER_LEN;
    let body_len = (body_end - SNAPSHOT_HEADER_LEN) as u64;
    if body_len > MAX_SNAPSHOT_BYTES {
        return Err(FormatError::TooLarge {
            len: body_len,
            max: MAX_SNAPSHOT_BYTES,
        });
    }
    let mut trailer = Reader::new(&bytes[body_end..]);
    let crc = trailer.u32()?;
    if trailer.array::<8>()? != *SNAPSHOT_END {
        return Err(FormatError::corrupt("bad snapshot end magic"));
    }
    if crc != crc32c_of(&[&bytes[..body_end]]) {
        return Err(FormatError::corrupt("snapshot CRC mismatch"));
    }
    let snap = pb::WorkspaceSnapshot::decode(&bytes[SNAPSHOT_HEADER_LEN..body_end])
        .map_err(|err| FormatError::corrupt(format!("snapshot body: {err}")))?;
    if snap.n != n {
        return Err(FormatError::corrupt(format!(
            "snapshot header n {n} differs from its body's {}",
            snap.n
        )));
    }
    Ok(snap)
}

/// Sets `snap`'s `changed` hint: the paths, when there are at most
/// `MAX_CHANGED_HINT`; otherwise none, with `changed_overflow` set, so
/// readers diff the trees instead (§36 §19.4.2).
pub fn set_changed_hint(snap: &mut pb::WorkspaceSnapshot, changed: Vec<pb::ChangedPath>) {
    if changed.len() > MAX_CHANGED_HINT {
        snap.changed = Vec::new();
        snap.changed_overflow = true;
    } else {
        snap.changed = changed;
        snap.changed_overflow = false;
    }
}

fn oid_of(bytes: &[u8], what: &str) -> Result<ObjectId, FormatError> {
    ObjectId::try_from(bytes)
        .map_err(|_| FormatError::corrupt(format!("snapshot {what}: not a SHA-1 id")))
}

/// The snapshot commit of `snap` (Ruling 38, D835): tree `worktree_tree`,
/// parent `head` (none when `head` is empty, an unborn workspace), author
/// and committer [`SNAPSHOT_IDENT`] at `created_unix_ms` truncated to
/// seconds in `+0000`, message `loams snapshot <ws_id>@<n>`. Equal states
/// give equal ids on every host. Answers the commit's id and its bytes (the
/// object body, without git's `commit <len>\0` header).
pub fn snapshot_commit(snap: &pb::WorkspaceSnapshot) -> Result<(ObjectId, Bytes), FormatError> {
    let tree = oid_of(&snap.worktree_tree, "worktree_tree")?;
    let seconds = snap.created_unix_ms.div_euclid(1000);
    let mut body = format!("tree {tree}\n");
    if !snap.head.is_empty() {
        body.push_str(&format!("parent {}\n", oid_of(&snap.head, "head")?));
    }
    body.push_str(&format!("author {SNAPSHOT_IDENT} {seconds} +0000\n"));
    body.push_str(&format!("committer {SNAPSHOT_IDENT} {seconds} +0000\n"));
    body.push_str(&format!("\nloams snapshot {}@{}\n", snap.ws_id, snap.n));
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(format!("commit {}\0", body.len()).as_bytes());
    hasher.update(body.as_bytes());
    let oid = hasher
        .try_finalize()
        .map_err(|err| FormatError::corrupt(format!("hashing the snapshot commit: {err}")))?;
    Ok((oid, Bytes::from(body)))
}
