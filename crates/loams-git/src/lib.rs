//! Loams Git's portable core (design §36, §19; plan GT1).
//!
//! This crate holds the identifiers and object paths ([`ids`], [`paths`]),
//! every limit the GT1 plan names ([`limits`]), the `loams.git.v1` on-disk
//! formats ([`format`]), and, as later tasks land, the WAL, the ref log, the
//! workspace store and the transport-free protocol module. It depends on
//! `loams-store`, `loams-cloudevents`, `loams-fs` and gitoxide crates only,
//! and opens no listener.

pub mod blob;
pub mod checkpointer;
pub mod clock;
pub mod error;
pub mod fork;
pub mod format;
pub mod gc;
pub mod ids;
pub mod limits;
pub mod odb;
pub mod paths;
pub mod reflog;
mod retry;
pub mod sequencer;
pub mod state;
pub mod testing;
pub mod wal;

pub use blob::{BlobError, BlobId, BlobStore, LpkWriter, Put, StoreBlobStore};
pub use checkpointer::{CheckpointPolicy, create_repo, state_at};
pub use clock::{Clock, SystemClock};
pub use error::IdError;
pub use fork::{ForkError, fork_repo, open_fork_odb};
pub use format::FormatError;
pub use format::segment::{WalBatch, WalSegment};
pub use gc::{SegmentGcPlan, plan_segment_gc, run_segment_gc};
pub use ids::{
    IdemKey, MountId, NamespaceId, ObjectId, Principal, RefName, RepoId, RepoName, RepoPath,
    SecretId, Seq, SnapN, TraceParent, WsId, WsName,
};
pub use odb::{ObjectDb, OdbError, PackCache, PackCacheOdb};
pub use paths::RepoPaths;
pub use reflog::{
    CommittedTxn, Expect, ReadAt, Receipt, RefError, RefLog, RefSnapshot, RefTxn, RefUpdate,
    RejectReason, Scope,
};
pub use sequencer::{BucketRefLog, GitEvent, SequencerConfig};
pub use state::{Check, IdemIndex, RepoState, StateError};
pub use wal::{Appended, Hint, StoreWalStore, WalError, WalStore};
