//! O(1) forks (§36 §4.7; plan GT1 Task 11, old GT1 Task 8).
//!
//! Forking repository A at seq `s` is one create-only PUT: repository B's
//! checkpoint 0, with A's refs, symbolic refs and protections at `s`, no
//! packs, and `parent = {A, s}`. No object is copied: B's object lookup
//! falls through to A's packs at `s` ([`open_fork_odb`]), recursively, at
//! most `MAX_FORK_DEPTH` (8) parents deep.

use std::sync::Arc;

use loams_store::Store;

use crate::blob::StoreBlobStore;
use crate::checkpointer::state_at;
use crate::format::pb;
use crate::ids::{NamespaceId, RepoId, Seq};
use crate::limits::MAX_FORK_DEPTH;
use crate::odb::{OdbError, PackCache, PackCacheOdb};
use crate::paths::RepoPaths;
use crate::reflog::{ReadAt, RefError, RefLog, RefSnapshot};
use crate::wal::{StoreWalStore, WalStore};

/// Errors of opening a fork's object database.
#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    /// A parent's state could not be read.
    #[error(transparent)]
    Ref(#[from] RefError),
    /// The object database refused the chain.
    #[error(transparent)]
    Odb(#[from] OdbError),
}

/// Forks `src_repo` (whose ref log is `src`) at `at` into the repository
/// whose WAL is `dst_wal`: one checkpoint PUT, nothing copied. Returns the
/// seq forked at. A fork of a fork is allowed; lookup refuses a chain
/// deeper than `MAX_FORK_DEPTH` when the database is opened, and the
/// catalog refuses to create one (Task 13).
///
/// Idempotent: forking again at the same seq writes the same checkpoint.
/// The object format is SHA-1, the only one served (Ruling 8).
pub async fn fork_repo(
    src: &dyn RefLog,
    dst_wal: &dyn WalStore,
    src_repo: &RepoId,
    at: ReadAt,
) -> Result<Seq, RefError> {
    let snapshot = src.snapshot(at).await?;
    let checkpoint = pb::Checkpoint {
        seq: 0,
        object_format: pb::ObjectFormat::Sha1 as i32,
        refs: snapshot
            .refs
            .iter()
            .map(|(name, oid)| (name.as_str().to_string(), oid.as_bytes().to_vec()))
            .collect(),
        symrefs: snapshot
            .symrefs
            .iter()
            .map(|(name, target)| pb::SymRef {
                name: name.clone(),
                target: target.as_str().to_string(),
            })
            .collect(),
        protections: snapshot.protections.clone(),
        packs: Vec::new(),
        parent: Some(pb::ForkParent {
            repo_id: src_repo.as_str().to_string(),
            seq: snapshot.seq.0,
        }),
        idempotency: Vec::new(),
    };
    dst_wal.put_checkpoint(&checkpoint).await?;
    Ok(snapshot.seq)
}

/// The object database of repository `repo`'s `snapshot` in namespace
/// `ns` of `store`, with every fork parent opened at the seq its child
/// names (each parent's state is read from its own WAL). Refuses a chain
/// of more than `MAX_FORK_DEPTH` parents (`ForkTooDeep`).
pub async fn open_fork_odb(
    store: &Store,
    ns: NamespaceId,
    repo: &RepoId,
    snapshot: &RefSnapshot,
    cache: Arc<PackCache>,
) -> Result<PackCacheOdb, ForkError> {
    // The chain from `repo` up to its root, each with its snapshot.
    let mut chain: Vec<(RepoId, RefSnapshot)> = vec![(repo.clone(), snapshot.clone())];
    while let Some(parent) = chain.last().and_then(|(_, snap)| snap.parent.clone()) {
        if chain.len() > MAX_FORK_DEPTH {
            return Err(OdbError::ForkTooDeep {
                max: MAX_FORK_DEPTH,
            }
            .into());
        }
        let id = RepoId::parse(&parent.repo_id).map_err(|err| RefError::Invalid {
            reason: format!("fork parent: {err}"),
        })?;
        let wal = StoreWalStore::new(store.clone(), RepoPaths::new(ns, &id));
        let state = state_at(&wal, Seq(parent.seq)).await?;
        chain.push((id, state.snapshot()));
    }
    let mut odb: Option<Arc<PackCacheOdb>> = None;
    for (id, snap) in chain.into_iter().rev() {
        let blobs = Arc::new(StoreBlobStore::new(store.clone(), RepoPaths::new(ns, &id)));
        let opened = PackCacheOdb::open(id, blobs, Arc::clone(&cache), &snap, odb).await?;
        odb = Some(Arc::new(opened));
    }
    let odb = odb.expect("the chain holds `repo`");
    Ok(Arc::try_unwrap(odb).expect("only this function holds the database"))
}
