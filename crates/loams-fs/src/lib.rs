//! The `Fs` trait (design §36 §17, D382): where Loams keeps warm,
//! rebuildable state and scratch, so the same code runs where there is no
//! `std::fs`.
//!
//! Durable truth stays in the object store behind conditional writes (D1).
//! An [`Fs`] is a cache and scratch space, never the source of truth: a
//! pack cache, a spill buffer for a push, vfsd's content-addressed cache.
//!
//! - [`Fs`], [`FsPath`], [`WriteMode`], [`FsMeta`], [`FsCaps`] and
//!   [`FsError`] are §36 §17.2's, with one addition: [`MaybeSendSync`]
//!   (GT1 Task 0, T0-32).
//! - [`NativeFs`] keeps files under a root directory with `tokio::fs`;
//!   [`MemFs`] keeps them in memory and is the conformance suite's
//!   reference.
//! - [`SpillDir`] is a per-request scratch directory, removed on drop.
//! - [`conformance`] holds every backend to the same semantics
//!   ([`fs_conformance!`]).

pub mod conformance;
mod error;
mod mem;
mod native;
mod path;
mod spill;

use std::fmt;
use std::ops::Range;

use bytes::Bytes;

pub use error::FsError;
pub use mem::MemFs;
pub use native::NativeFs;
pub use path::{FsPath, FsPathError, MAX_PATH_BYTES};
pub use spill::SpillDir;

/// How [`Fs::write`] treats an existing file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace the file atomically: a reader sees the old or the new bytes.
    Overwrite,
    /// Fail with [`FsError::AlreadyExists`] if the file exists.
    CreateNew,
}

/// A file's metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsMeta {
    /// The file's length in bytes.
    pub len: u64,
    /// When the file was last written, in ms since the Unix epoch.
    pub modified_unix_ms: i64,
}

/// What a backend can do. The conformance suite runs the append and rename
/// cases only where these say so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsCaps {
    /// [`Fs::append`] works ([`NativeFs`], [`MemFs`]).
    pub append: bool,
    /// [`Fs::rename`] replaces its target atomically ([`NativeFs`], [`MemFs`]).
    pub atomic_rename: bool,
    /// The largest file the backend stores, in bytes.
    pub max_file: u64,
    /// Whether files survive a restart of the process.
    pub survives_restart: bool,
}

/// `Send + Sync` on native targets, and nothing on `wasm32`, whose futures
/// are `!Send`. [`Fs`] requires it so an `Arc<dyn Fs>` can cross tokio tasks
/// natively (GT1 Task 0, T0-32: an addition to §36 §17.2).
#[cfg(not(target_family = "wasm"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_family = "wasm"))]
impl<T: Send + Sync + ?Sized> MaybeSendSync for T {}

/// `Send + Sync` on native targets, and nothing on `wasm32`, whose futures
/// are `!Send`.
#[cfg(target_family = "wasm")]
pub trait MaybeSendSync {}
#[cfg(target_family = "wasm")]
impl<T: ?Sized> MaybeSendSync for T {}

/// Warm, rebuildable state and scratch (§36 §17.2).
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait Fs: fmt::Debug + MaybeSendSync {
    /// What this backend can do.
    fn caps(&self) -> FsCaps;

    /// The whole file.
    async fn read(&self, path: &FsPath) -> Result<Bytes, FsError>;

    /// Bytes `range` of the file. The range must lie inside the file: an
    /// empty range anywhere up to its length reads nothing, and a range past
    /// the end (or inverted) is an error, never a short read.
    async fn read_range(&self, path: &FsPath, range: Range<u64>) -> Result<Bytes, FsError>;

    /// `CreateNew` fails with `AlreadyExists`; a returned write is visible to
    /// every later read, and no reader ever sees part of it.
    async fn write(&self, path: &FsPath, data: Bytes, mode: WriteMode) -> Result<(), FsError>;

    /// Appends to the file, creating it if absent, and returns the new
    /// length. `Unsupported` on blob-only backends.
    async fn append(&self, path: &FsPath, data: Bytes) -> Result<u64, FsError>;

    /// Atomic replace of `to`. `Unsupported` on blob-only backends.
    async fn rename(&self, from: &FsPath, to: &FsPath) -> Result<(), FsError>;

    /// Removes the file. `NotFound` is `Ok`.
    async fn remove(&self, path: &FsPath) -> Result<(), FsError>;

    /// The file's metadata, or `None` if there is no file at `path`.
    async fn stat(&self, path: &FsPath) -> Result<Option<FsMeta>, FsError>;

    /// Files under `dir` (recursively; [`FsPath::root`] for all of them),
    /// sorted, strictly after `after`, at most `limit`. `dir` is a prefix of
    /// whole segments: `a` holds `a/x` but not `ab/x`.
    async fn list(
        &self,
        dir: &FsPath,
        after: Option<&FsPath>,
        limit: usize,
    ) -> Result<Vec<(FsPath, FsMeta)>, FsError>;
}

/// Checks a read range against a file length: the shared rule of
/// [`Fs::read_range`].
pub(crate) fn check_range(path: &FsPath, range: &Range<u64>, len: u64) -> Result<(), FsError> {
    if range.start > range.end || range.end > len {
        return Err(FsError::Backend(format!(
            "range {}..{} is outside {path} ({len} bytes)",
            range.start, range.end
        )));
    }
    Ok(())
}

/// Milliseconds since the Unix epoch, now.
pub(crate) fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
