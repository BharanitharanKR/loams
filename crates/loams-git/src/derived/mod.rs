//! Derived data (§36 §19.5.1, D830; plan GT1 Tasks 19–21): immutable
//! objects under a repository's `derived/`, keyed by the object id they
//! describe, computed on first use and shared by every reader.
//!
//! [`DerivedStore`] writes them create-only. Two nodes that compute the
//! same object at once write equal bytes; one PUT wins and the other reads
//! the winner back. Reads go through the H1 cache when there is one. A
//! derived object can be deleted at any time: it is computed again.

pub mod tree_aux;

use bytes::Bytes;
use loams_cache::{CacheError, RangeCache};
use loams_store::{Store, StoreError};

use crate::format::FormatError;
use crate::ids::ObjectId;
use crate::odb::OdbError;
use crate::paths::RepoPaths;
use crate::retry;

pub use tree_aux::{AuxEntry, TreeAux, tree_aux};

/// Errors of derived data.
#[derive(Debug, thiserror::Error)]
pub enum DerivedError {
    /// The object asked about is not of the kind the data derives from.
    #[error("{oid} is a {found}, not a {expected}")]
    WrongKind {
        /// The object.
        oid: ObjectId,
        /// The kind it must be.
        expected: &'static str,
        /// The kind it is.
        found: &'static str,
    },
    /// Reading objects failed (a missing object is `Odb(NotFound)`).
    #[error(transparent)]
    Odb(#[from] OdbError),
    /// A derived object that does not decode, written by this computation.
    #[error(transparent)]
    Format(#[from] FormatError),
    /// The object store failed (after retries, when transient).
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// A repository's `derived/` objects (plan GT1 Task 19).
#[derive(Clone, Debug)]
pub struct DerivedStore {
    store: Store,
    paths: RepoPaths,
    h1: Option<RangeCache>,
}

impl DerivedStore {
    /// The derived objects of the repository at `paths`.
    pub fn new(store: Store, paths: RepoPaths) -> Self {
        Self {
            store,
            paths,
            h1: None,
        }
    }

    /// Reads through `cache`, the H1 range cache over the same store.
    /// Derived objects are immutable and content-keyed, so a cached copy of
    /// one deleted since is still correct.
    pub fn with_range_cache(mut self, cache: RangeCache) -> Self {
        self.h1 = Some(cache);
        self
    }

    /// The repository's paths.
    pub fn paths(&self) -> &RepoPaths {
        &self.paths
    }

    /// The object at `path`, or `None` when there is none. An H1 failure
    /// falls back to the store.
    pub async fn get(&self, path: &str) -> Result<Option<Bytes>, DerivedError> {
        if let Some(h1) = &self.h1 {
            let read = async {
                let size = h1.size(path).await?;
                h1.read(path, 0..size).await
            };
            match read.await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(CacheError::Store(StoreError::NotFound { .. })) => {
                    // Deleted, perhaps after its size was cached.
                    h1.forget(path).await;
                    return Ok(None);
                }
                Err(err) => {
                    tracing::warn!(path, error = %err, "reading derived data through H1 failed");
                }
            }
        }
        match retry::retry(|| self.store.get(path)).await {
            Ok((bytes, _)) => Ok(Some(bytes)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Writes `bytes` at `path` unless an object is there, and returns the
    /// stored bytes: `bytes`, or the object another writer stored first.
    pub async fn put(&self, path: &str, bytes: Bytes) -> Result<Bytes, DerivedError> {
        match retry::retry(|| self.store.put_if_absent(path, bytes.clone())).await {
            Ok(_) => Ok(bytes),
            Err(StoreError::AlreadyExists { .. }) => {
                match retry::retry(|| self.store.get(path)).await {
                    Ok((stored, _)) => Ok(stored),
                    // Deleted again (a scrub): this writer's bytes stand.
                    Err(StoreError::NotFound { .. }) => Ok(bytes),
                    Err(err) => Err(err.into()),
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}
