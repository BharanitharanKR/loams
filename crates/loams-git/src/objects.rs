//! Lazy object reads (§36 §19.5.1–§19.5.2, D829–D830; plan GT1 Task 18):
//! what `ObjectService` and the immutable object route serve, without a
//! transport.
//!
//! - [`get_objects`]: up to `FETCH_BATCH_OIDS` (1,024) objects, located
//!   first and then read in pack order (layer, pack, offset), so the reads
//!   of one pack run forward through it and the H1 cache's blocks coalesce
//!   them; one object is held at a time.
//! - [`get_trees`]: a tree and its subtrees to a depth, each with its tree
//!   aux data (Task 19).
//! - [`has_objects`]: which of a list of objects the repository (and the
//!   workspace) holds.
//! - [`loose_object`]: an object in git's loose format, zlib-compressed,
//!   for `GET …/loams/objects/<oid>`.
//!
//! **Workspaces** (ruling T18.1). Objects in a workspace's packs are read
//! only through that workspace: the caller names it, [`WsLookup`] answers
//! its owner and packs, and the caller's access decides. Until the
//! workspace store (Tasks 22–25) the server's lookup is [`NoWorkspaces`];
//! [`MemWsLookup`] serves tests.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
pub use gix_object::Kind;

use crate::auth::{GitGrant, WsMatch};
use crate::blob::BlobStore;
use crate::derived::{DerivedError, DerivedStore, TreeAux, tree_aux};
use crate::format::pb;
use crate::ids::{ObjectId, Principal, RepoId, WsId};
use crate::limits::FETCH_BATCH_OIDS;
use crate::odb::{Location, ObjectDb, OdbError, RangeOdb, WsOdb};

/// The deepest `get_trees` walk accepted.
pub const MAX_TREE_DEPTH: u8 = 8;

/// Errors of the object reads.
#[derive(Debug, thiserror::Error)]
pub enum ObjectsError {
    /// More object ids than one call takes.
    #[error("{got} object ids, more than the {max} one call takes")]
    TooMany {
        /// The largest batch.
        max: usize,
        /// The batch asked for.
        got: usize,
    },
    /// A tree walk deeper than [`MAX_TREE_DEPTH`].
    #[error("depth {got} is deeper than the {max} served")]
    TooDeep {
        /// The deepest walk.
        max: u8,
        /// The depth asked for.
        got: u8,
    },
    /// The object a tree walk starts from does not exist.
    #[error("object {0} not found")]
    NotFound(ObjectId),
    /// The workspace lookup failed.
    #[error("workspace lookup: {0}")]
    Lookup(String),
    /// Reading objects failed.
    #[error(transparent)]
    Odb(#[from] OdbError),
    /// Computing or reading tree aux data failed.
    #[error(transparent)]
    Derived(#[from] DerivedError),
}

/// What a read needs to know of a workspace (ruling T18.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsView {
    /// Who owns it.
    pub owner: Principal,
    /// Its packs, oldest first, as its latest snapshot names them.
    pub packs: Vec<pb::PackRef>,
}

/// Finds a workspace of a repository (ruling T18.1; the bucket's
/// implementation arrives with the workspace store, Task 25).
#[async_trait]
pub trait WsLookup: Send + Sync + fmt::Debug {
    /// The workspace, or `None` when the repository has none of that id.
    async fn workspace(&self, repo: &RepoId, ws: &WsId) -> Result<Option<WsView>, ObjectsError>;
}

/// The lookup of a server without workspaces yet: every workspace is
/// unknown.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoWorkspaces;

#[async_trait]
impl WsLookup for NoWorkspaces {
    async fn workspace(&self, _: &RepoId, _: &WsId) -> Result<Option<WsView>, ObjectsError> {
        Ok(None)
    }
}

/// Workspaces held in memory, for tests.
#[derive(Debug, Default)]
pub struct MemWsLookup {
    workspaces: Mutex<BTreeMap<(String, String), WsView>>,
}

impl MemWsLookup {
    /// Adds (or replaces) workspace `ws` of `repo`.
    pub fn insert(&self, repo: &RepoId, ws: &WsId, view: WsView) {
        self.workspaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((repo.to_string(), ws.to_string()), view);
    }
}

#[async_trait]
impl WsLookup for MemWsLookup {
    async fn workspace(&self, repo: &RepoId, ws: &WsId) -> Result<Option<WsView>, ObjectsError> {
        Ok(self
            .workspaces
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(repo.to_string(), ws.to_string()))
            .cloned())
    }
}

/// Whether a git-listener token may read workspace `ws` (`WsMatch`,
/// ruling T18.2). Main-port callers are not checked this way: whoever may
/// read the repository may read its workspaces (GT-Q13's default).
pub fn grant_reads_workspace(grant: &GitGrant, ws: &WsId, view: &WsView) -> bool {
    match &grant.workspaces {
        WsMatch::All => true,
        WsMatch::Own => view.owner == grant.principal,
        WsMatch::Ids(ids) => ids.contains(ws),
    }
}

/// `repo_odb` with workspace `view`'s packs (in `ws_blobs`) searched last.
pub fn workspace_odb(
    repo_odb: &RangeOdb,
    ws_blobs: Arc<dyn BlobStore>,
    view: &WsView,
) -> Result<RangeOdb, ObjectsError> {
    Ok(WsOdb::open(repo_odb, ws_blobs, &view.packs)?.into_odb())
}

fn check_batch(len: usize) -> Result<(), ObjectsError> {
    if len > FETCH_BATCH_OIDS {
        return Err(ObjectsError::TooMany {
            max: FETCH_BATCH_OIDS,
            got: len,
        });
    }
    Ok(())
}

/// One answer of [`get_objects`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectRead {
    /// The object, deltas resolved.
    Found {
        /// Its id.
        oid: ObjectId,
        /// Its kind.
        kind: Kind,
        /// Its bytes.
        data: Bytes,
    },
    /// Neither the repository nor the workspace holds it.
    Missing(ObjectId),
}

/// Reads `oids` (at most `FETCH_BATCH_OIDS`, else `TooMany` before any
/// read). The objects come in pack order (each once, duplicates dropped),
/// then the missing ones in request order.
pub async fn get_objects(
    odb: Arc<RangeOdb>,
    oids: Vec<ObjectId>,
) -> Result<BoxStream<'static, Result<ObjectRead, ObjectsError>>, ObjectsError> {
    check_batch(oids.len())?;
    let mut seen = std::collections::HashSet::new();
    let mut found: Vec<(Location, ObjectId)> = Vec::new();
    let mut missing: VecDeque<ObjectId> = VecDeque::new();
    for oid in oids {
        if !seen.insert(oid) {
            continue;
        }
        match odb.locate(&oid).await? {
            Some(loc) => found.push((loc, oid)),
            None => missing.push_back(oid),
        }
    }
    found.sort_by_key(|(loc, _)| (loc.layer, loc.pack, loc.offset));
    let found: VecDeque<(Location, ObjectId)> = found.into();
    let reads = stream::unfold((odb, found), |(odb, mut found)| async move {
        let (loc, oid) = found.pop_front()?;
        let read = odb
            .read_at(&loc)
            .await
            .map(|(kind, data)| ObjectRead::Found { oid, kind, data })
            .map_err(ObjectsError::from);
        Some((read, (odb, found)))
    });
    let missing = stream::iter(missing.into_iter().map(|oid| Ok(ObjectRead::Missing(oid))));
    Ok(reads.chain(missing).boxed())
}

/// Which of `oids` (at most `FETCH_BATCH_OIDS`) `odb` holds, in order.
pub async fn has_objects(odb: &RangeOdb, oids: &[ObjectId]) -> Result<Vec<bool>, ObjectsError> {
    check_batch(oids.len())?;
    let mut held = Vec::with_capacity(oids.len());
    for oid in oids {
        held.push(odb.locate(oid).await?.is_some());
    }
    Ok(held)
}

/// `held` as a bitmap: bit `i % 8` (least significant first) of byte
/// `i / 8` is set when object `i` is held.
pub fn bitmap(held: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; held.len().div_ceil(8)];
    for (i, _) in held.iter().enumerate().filter(|(_, held)| **held) {
        out[i / 8] |= 1 << (i % 8);
    }
    out
}

/// What [`get_trees`] answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trees {
    /// The trees, breadth first from the root, each once.
    pub trees: Vec<Arc<TreeAux>>,
    /// Whether the walk stopped at `FETCH_BATCH_OIDS` trees before `depth`.
    pub truncated: bool,
}

/// The tree `root` names (a tree, or a commit or tag peeled to its tree)
/// and its subtrees down to `depth` levels below it (0 is the root alone),
/// with their aux data; at most `FETCH_BATCH_OIDS` trees.
pub async fn get_trees(
    odb: &RangeOdb,
    derived: &DerivedStore,
    root: &ObjectId,
    depth: u8,
) -> Result<Trees, ObjectsError> {
    if depth > MAX_TREE_DEPTH {
        return Err(ObjectsError::TooDeep {
            max: MAX_TREE_DEPTH,
            got: depth,
        });
    }
    let root = peel_to_tree(odb, root).await?;
    let mut trees = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut level = vec![root];
    seen.insert(root);
    for at in 0..=depth {
        let mut next = Vec::new();
        for tree in level {
            if trees.len() == FETCH_BATCH_OIDS {
                return Ok(Trees {
                    trees,
                    truncated: true,
                });
            }
            let aux = tree_aux(odb, derived, &tree).await?;
            if at < depth {
                next.extend(
                    aux.entries
                        .iter()
                        .filter(|entry| entry.is_tree() && seen.insert(entry.oid))
                        .map(|entry| entry.oid),
                );
            }
            trees.push(aux);
        }
        level = next;
    }
    Ok(Trees {
        trees,
        truncated: false,
    })
}

async fn peel_to_tree(odb: &RangeOdb, oid: &ObjectId) -> Result<ObjectId, ObjectsError> {
    let mut cur = *oid;
    // A tag of a tag of … ends at a commit or tree; git bounds nothing
    // here, so bound it ourselves.
    for _ in 0..16 {
        let Some(loc) = odb.locate(&cur).await? else {
            return Err(ObjectsError::NotFound(cur));
        };
        let header = odb.header(&loc).await?;
        let corrupt = |err: &dyn fmt::Display| OdbError::CorruptObject {
            oid: cur,
            reason: err.to_string(),
        };
        match header.kind {
            Kind::Tree => return Ok(cur),
            Kind::Blob => {
                return Err(DerivedError::WrongKind {
                    oid: cur,
                    expected: "tree",
                    found: "blob",
                }
                .into());
            }
            Kind::Commit => {
                let (_, data) = odb.read_at(&loc).await?;
                let commit = gix_object::CommitRef::from_bytes(&data, gix_hash::Kind::Sha1)
                    .map_err(|err| corrupt(&err))?;
                return Ok(commit.tree());
            }
            Kind::Tag => {
                let (_, data) = odb.read_at(&loc).await?;
                let tag = gix_object::TagRef::from_bytes(&data, gix_hash::Kind::Sha1)
                    .map_err(|err| corrupt(&err))?;
                cur = tag.target();
            }
        }
    }
    Err(OdbError::CorruptObject {
        oid: *oid,
        reason: "tags nest more than 16 deep".into(),
    }
    .into())
}

/// The object `oid` in git's loose format (`<kind> <len>\0<bytes>`,
/// zlib-compressed), or `None` when it is not held.
pub async fn loose_object(odb: &RangeOdb, oid: &ObjectId) -> Result<Option<Bytes>, ObjectsError> {
    let (kind, data) = match odb.read(oid).await {
        Ok(found) => found,
        Err(OdbError::NotFound(_)) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut out = gix_zlib::stream::deflate::Write::new(
        Vec::with_capacity(data.len() / 2 + 64),
        gix_zlib::Compression::default(),
    );
    let header = format!("{} {}\0", kind_name(kind), data.len());
    let write = |out: &mut gix_zlib::stream::deflate::Write<Vec<u8>>| -> std::io::Result<()> {
        out.write_all(header.as_bytes())?;
        out.write_all(&data)?;
        out.flush()
    };
    write(&mut out).map_err(|err| OdbError::CorruptObject {
        oid: *oid,
        reason: format!("compressing: {err}"),
    })?;
    Ok(Some(Bytes::from(out.into_inner())))
}

/// git's name of a kind.
pub fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Blob => "blob",
        Kind::Tree => "tree",
        Kind::Commit => "commit",
        Kind::Tag => "tag",
    }
}
