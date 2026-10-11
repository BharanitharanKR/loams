//! Loams Git's portable core (design §36, §19; plan GT1).
//!
//! This crate holds the identifiers and object paths ([`ids`], [`paths`]),
//! every limit the GT1 plan names ([`limits`]), the `loams.git.v1` on-disk
//! formats ([`format`]), and, as later tasks land, the WAL, the ref log, the
//! workspace store and the transport-free protocol module. It depends on
//! `loams-store`, `loams-cloudevents`, `loams-fs` and gitoxide crates only,
//! and opens no listener.

pub mod blob;
pub mod error;
pub mod format;
pub mod ids;
pub mod limits;
pub mod paths;
mod retry;

pub use blob::{BlobError, BlobId, BlobStore, LpkWriter, Put, StoreBlobStore};
pub use error::IdError;
pub use format::FormatError;
pub use format::segment::{WalBatch, WalSegment};
pub use ids::{
    IdemKey, MountId, NamespaceId, ObjectId, Principal, RefName, RepoId, RepoName, RepoPath,
    SecretId, Seq, SnapN, TraceParent, WsId, WsName,
};
pub use paths::RepoPaths;
