//! Tree aux data (§36 §19.5.1; plan GT1 Task 19): a tree's entries with
//! their sizes, so `stat` and `ls -l` never fetch a blob.
//!
//! Sizes come from pack entry headers ([`RangeOdb::header`]): a delta's
//! header carries its result size, so no blob is inflated. The result is
//! cached as `derived/tree-aux/<tree_oid>.lta`:
//!
//! ```text
//! header   magic "LGITTAUX" (8) | format_version u16 LE = 1 | tree oid [20]
//! body     loams.git.v1.TreeAux (protobuf)
//! trailer  crc32c u32 LE over header ‖ body | magic "LGITTEND" (8)
//! ```

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use gix_object::Kind;
use gix_object::tree::EntryKind;
use prost::Message;

use super::{DerivedError, DerivedStore};
use crate::format::{FormatError, Reader, crc32c_of, pb};
use crate::ids::ObjectId;
use crate::odb::{ObjectDb, OdbError, RangeOdb};

/// The header's magic.
pub const TREE_AUX_MAGIC: &[u8; 8] = b"LGITTAUX";
/// The trailer's magic.
pub const TREE_AUX_END: &[u8; 8] = b"LGITTEND";
/// The format version this crate writes.
pub const TREE_AUX_VERSION: u16 = 1;
/// The oldest format version this crate reads.
pub const TREE_AUX_MIN_VERSION: u16 = 1;
/// Header bytes before the body.
pub const TREE_AUX_HEADER_LEN: usize = 8 + 2 + 20;
/// Trailer bytes after the body.
pub const TREE_AUX_TRAILER_LEN: usize = 4 + 8;

/// One entry of a tree, as `git ls-tree -l` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuxEntry {
    /// The entry's name (bytes, as git stores them).
    pub name: Vec<u8>,
    /// git's mode bits: `0o100644`, `0o100755`, `0o120000`, `0o40000` or
    /// `0o160000`.
    pub mode: u32,
    /// The entry's object.
    pub oid: ObjectId,
    /// The object's size, for blobs and symlinks; `None` for trees and
    /// submodule commits (`ls-tree -l` prints `-`).
    pub size: Option<u64>,
}

impl AuxEntry {
    /// Whether the entry is a tree.
    pub fn is_tree(&self) -> bool {
        self.mode == 0o40000
    }

    /// The object type `git ls-tree` prints: `blob`, `tree` or `commit`.
    pub fn kind_name(&self) -> &'static str {
        match self.mode {
            0o40000 => "tree",
            0o160000 => "commit",
            _ => "blob",
        }
    }
}

/// A tree's aux data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeAux {
    /// The tree.
    pub tree: ObjectId,
    /// Its entries, in the tree's order.
    pub entries: Vec<AuxEntry>,
}

/// Encodes `aux`; equal aux data has equal bytes.
pub fn encode_tree_aux(aux: &TreeAux) -> Bytes {
    let body = pb::TreeAux {
        tree: aux.tree.as_bytes().to_vec(),
        entries: aux
            .entries
            .iter()
            .map(|entry| pb::TreeAuxEntry {
                name: entry.name.clone(),
                mode: entry.mode,
                oid: entry.oid.as_bytes().to_vec(),
                size: entry.size,
            })
            .collect(),
    }
    .encode_to_vec();
    let mut out = BytesMut::with_capacity(TREE_AUX_HEADER_LEN + body.len() + TREE_AUX_TRAILER_LEN);
    out.put_slice(TREE_AUX_MAGIC);
    out.put_u16_le(TREE_AUX_VERSION);
    out.put_slice(aux.tree.as_bytes());
    out.put_slice(&body);
    let crc = crc32c_of(&[&out]);
    out.put_u32_le(crc);
    out.put_slice(TREE_AUX_END);
    out.freeze()
}

fn oid_from(bytes: &[u8], what: &str) -> Result<ObjectId, FormatError> {
    ObjectId::try_from(bytes).map_err(|_| FormatError::corrupt(format!("{what}: not a SHA-1 id")))
}

/// Decodes tree aux data. Errors: `UnknownVersion`; `Corrupt` for a bad
/// magic, CRC or body, or a header tree that differs from the body's.
pub fn decode_tree_aux(bytes: &[u8]) -> Result<TreeAux, FormatError> {
    if bytes.len() < TREE_AUX_HEADER_LEN + TREE_AUX_TRAILER_LEN {
        return Err(FormatError::corrupt("truncated"));
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *TREE_AUX_MAGIC {
        return Err(FormatError::corrupt("not tree aux data: bad magic"));
    }
    let version = reader.u16()?;
    if !(TREE_AUX_MIN_VERSION..=TREE_AUX_VERSION).contains(&version) {
        return Err(FormatError::UnknownVersion { found: version });
    }
    let tree = oid_from(&reader.array::<20>()?, "tree")?;
    let body_end = bytes.len() - TREE_AUX_TRAILER_LEN;
    let mut trailer = Reader::new(&bytes[body_end..]);
    let crc = trailer.u32()?;
    if trailer.array::<8>()? != *TREE_AUX_END {
        return Err(FormatError::corrupt("bad tree aux end magic"));
    }
    if crc != crc32c_of(&[&bytes[..body_end]]) {
        return Err(FormatError::corrupt("tree aux CRC mismatch"));
    }
    let body = pb::TreeAux::decode(&bytes[TREE_AUX_HEADER_LEN..body_end])
        .map_err(|err| FormatError::corrupt(format!("tree aux body: {err}")))?;
    if oid_from(&body.tree, "tree")? != tree {
        return Err(FormatError::corrupt(
            "tree aux header names another tree than its body",
        ));
    }
    let entries = body
        .entries
        .into_iter()
        .map(|entry| {
            Ok(AuxEntry {
                name: entry.name,
                mode: entry.mode,
                oid: oid_from(&entry.oid, "entry")?,
                size: entry.size,
            })
        })
        .collect::<Result<_, FormatError>>()?;
    Ok(TreeAux { tree, entries })
}

/// The aux data of `tree` (plan GT1 Task 19): from `derived/` when it is
/// there, else computed from `odb`'s entry headers and stored create-only.
/// Concurrent computers store equal bytes; one PUT wins and every caller
/// gets the same answer. A stored object that does not decode is logged
/// and recomputed.
pub async fn tree_aux(
    odb: &RangeOdb,
    derived: &DerivedStore,
    tree: &ObjectId,
) -> Result<Arc<TreeAux>, DerivedError> {
    let path = derived.paths().derived_tree_aux(tree);
    if let Some(bytes) = derived.get(&path).await? {
        match decode_tree_aux(&bytes) {
            Ok(aux) if aux.tree == *tree => return Ok(Arc::new(aux)),
            Ok(_) => tracing::warn!(path, "tree aux data names another tree; recomputing"),
            Err(err) => tracing::warn!(path, error = %err, "corrupt tree aux data; recomputing"),
        }
    }
    let aux = compute(odb, tree).await?;
    let bytes = encode_tree_aux(&aux);
    let stored = derived.put(&path, bytes.clone()).await?;
    if stored != bytes {
        // Another writer won with other bytes: a corrupt object, or a
        // writer of another version. This computation's answer stands.
        tracing::warn!(
            path,
            "stored tree aux data differs from a fresh computation"
        );
    }
    Ok(Arc::new(aux))
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Blob => "blob",
        Kind::Tree => "tree",
        Kind::Commit => "commit",
        Kind::Tag => "tag",
    }
}

async fn compute(odb: &RangeOdb, tree: &ObjectId) -> Result<TreeAux, DerivedError> {
    let (kind, data) = odb.read(tree).await?;
    if kind != Kind::Tree {
        return Err(DerivedError::WrongKind {
            oid: *tree,
            expected: "tree",
            found: kind_name(kind),
        });
    }
    let parsed = gix_object::TreeRef::from_bytes(&data, gix_hash::Kind::Sha1).map_err(|err| {
        OdbError::CorruptObject {
            oid: *tree,
            reason: err.to_string(),
        }
    })?;
    let mut entries = Vec::with_capacity(parsed.entries.len());
    for entry in &parsed.entries {
        let oid = entry.oid.to_owned();
        let size = match entry.mode.kind() {
            EntryKind::Tree | EntryKind::Commit => None,
            EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => {
                let loc = odb.locate(&oid).await?.ok_or(OdbError::NotFound(oid))?;
                Some(odb.header(&loc).await?.size)
            }
        };
        entries.push(AuxEntry {
            name: entry.filename.to_vec(),
            mode: u32::from(entry.mode.value()),
            oid,
            size,
        });
    }
    Ok(TreeAux {
        tree: *tree,
        entries,
    })
}
