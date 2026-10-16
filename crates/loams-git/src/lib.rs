//! Loams Git's portable core (design §36, §19; plan GT1).
//!
//! This crate holds the identifiers and object paths ([`ids`], [`paths`]),
//! every limit the GT1 plan names ([`limits`]), the `loams.git.v1` on-disk
//! formats ([`format`]), and, as later tasks land, the WAL, the ref log, the
//! workspace store and the transport-free protocol module. It depends on
//! `loams-store`, `loams-cloudevents`, `loams-fs` and gitoxide crates only,
//! and opens no listener.

pub mod auth;
pub mod blob;
pub mod catalog;
pub mod checkpointer;
pub mod clock;
pub mod derived;
pub mod error;
pub mod fork;
pub mod format;
pub mod forward;
pub mod gc;
pub mod ids;
pub mod limits;
pub mod mirror_events;
pub mod odb;
pub mod owner;
pub mod paths;
pub mod reflog;
mod retry;
pub mod sequencer;
pub mod state;
pub mod testing;
pub mod wal;

pub use auth::{
    EnvAccess, EnvGrant, GitAccess, GitAuthError, GitAuthorizer, GitDenied, GitGrant, RepoMatch,
    Secret, TrustClass, WsMatch,
};
pub use blob::{BlobError, BlobId, BlobStore, LpkWriter, Put, StoreBlobStore};
pub use catalog::{
    BucketRepoCatalog, CatalogError, Created, RepoCatalog, RepoLifecycle, RepoRecord,
};
pub use checkpointer::{CheckpointPolicy, create_repo, latest_state, state_at};
pub use clock::{Clock, SystemClock};
pub use derived::{DerivedError, DerivedStore};
pub use error::IdError;
pub use fork::{ForkError, fork_repo, open_fork_odb};
pub use format::FormatError;
pub use format::segment::{WalBatch, WalSegment};
pub use forward::{FORWARD_PATH, SeqCommit, SeqForwarder};
pub use gc::{SegmentGcPlan, plan_segment_gc, run_segment_gc};
pub use ids::{
    IdemKey, MountId, NamespaceId, ObjectId, Principal, RefName, RepoId, RepoName, RepoPath,
    SecretId, Seq, SnapN, TraceParent, WsId, WsName,
};
pub use mirror_events::{GitEventSink, MirrorCursor, repair_repo, run_mirror};
pub use odb::{ObjectDb, OdbError, PackCache, PackCacheOdb, RangeOdb, WsOdb};
pub use owner::{
    AlwaysHere, Cluster, LeaseError, OwnerConfig, OwnerStatsSnapshot, RepoSequencers, SeqLeases,
    SeqOwner, SeqPlacement, repo_placement_hash, seq_lease_key,
};
pub use paths::RepoPaths;
pub use reflog::{
    CommittedTxn, Expect, ReadAt, Receipt, RefError, RefLog, RefSnapshot, RefTxn, RefUpdate,
    RejectReason, Scope,
};
pub use sequencer::{BucketRefLog, GitEvent, SequencerConfig, config_digest};
pub use state::{Check, IdemIndex, RepoState, StateError};
pub use wal::{Appended, Hint, StoreWalStore, WalError, WalStore};
