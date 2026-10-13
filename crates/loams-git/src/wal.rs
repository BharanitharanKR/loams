//! `WalStore` (§36 §4.2, §5.1, D389; plan GT1 Task 7): the bucket WAL.
//!
//! - **Segments** `wal/<seq:020>.lgw` are written with a create-only PUT,
//!   which is the commit point: at most one batch is ever stored under a
//!   seq, across every writer. A writer recognises its own segment by its
//!   `batch_id` after an unknown outcome (a timeout, a 5xx after the request
//!   was sent, or R2's 429 when writers race one name): it reads the
//!   segment back, and resends the same bytes only when it is absent.
//! - **Reads** go forward from a seq and stop at the first missing number,
//!   so a segment above a gap is unreachable. A segment that is present but
//!   unreadable is an error, never a silent stop.
//! - **The hint** `head` is a small JSON object, overwritten best effort.
//!   It only speeds readers up.
//! - **Checkpoints** `checkpoints/<seq:020>.lgc` are create-only. A second
//!   writer of the same seq is accepted when its checkpoint states the same
//!   repository (ruling T7.2).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use loams_store::{Store, StoreError};
use serde::{Deserialize, Serialize};

use crate::clock::{Clock, SystemClock};
use crate::format::FormatError;
use crate::format::checkpoint::{decode_checkpoint, encode_checkpoint};
use crate::format::pb;
use crate::format::segment::{WalBatch, WalSegment, decode_segment, encode_segment};
use crate::ids::Seq;
use crate::limits::MULTIPART_THRESHOLD;
use crate::paths::RepoPaths;
use crate::retry::{self, is_transient, pause};

/// The best-effort pointer readers start from (§36 §4.1, §4.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hint {
    /// A committed seq: the tail when the hint was written.
    pub seq: Seq,
    /// A checkpoint at or below `seq`.
    pub checkpoint: Seq,
    /// When the hint was written.
    pub written_unix_ms: i64,
}

impl Hint {
    /// The hint of a repository that has none: nothing is known.
    pub fn empty() -> Self {
        Self {
            seq: Seq::ZERO,
            checkpoint: Seq::ZERO,
            written_unix_ms: 0,
        }
    }

    /// Whether this is [`Hint::empty`].
    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }
}

/// The outcome of [`WalStore::append`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Appended {
    /// The batch owns `seq`, now or from an earlier attempt with the same batch id.
    Committed {
        /// The segment's seq.
        seq: Seq,
    },
    /// Another batch owns `seq`; the caller applies it and re-validates.
    Fenced {
        /// The segment that owns the seq.
        existing: WalSegment,
    },
}

/// Errors of a [`WalStore`].
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// The encoded segment body is over `MAX_SEGMENT_BYTES` (1 MiB).
    #[error("segment body over 1 MiB")]
    TooLarge,
    /// A stored segment or checkpoint cannot be read, or a different
    /// checkpoint is already stored at a seq.
    #[error("corrupt WAL object at seq {seq}: {reason}")]
    Corrupt {
        /// The seq of the object.
        seq: Seq,
        /// What is wrong.
        reason: String,
    },
    /// A batch that cannot be a segment: an event whose `loamsseq` is not
    /// the seq it is appended at (ruling T7.1).
    #[error("invalid batch: {reason}")]
    Invalid {
        /// What is wrong.
        reason: String,
    },
    /// The object store failed (after retries, when transient).
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl WalError {
    fn corrupt(seq: Seq, err: impl ToString) -> Self {
        Self::Corrupt {
            seq,
            reason: err.to_string(),
        }
    }
}

/// The bucket WAL of one repository (§36 §5.1).
#[async_trait]
pub trait WalStore: Send + Sync + std::fmt::Debug {
    /// Writes `batch` as segment `expect_seq` with a create-only PUT.
    /// Precondition (caller's): segment `expect_seq - 1` exists or `expect_seq == Seq(1)`.
    /// Fenced: at most one batch is ever stored under a seq, across every writer.
    /// Idempotent: a retry with the same `batch_id` after an unknown outcome returns `Committed`.
    /// Errors: `TooLarge` (encoded body > 1 MiB), `Store(StoreError)` (retryable per StoreError::is_retryable).
    async fn append(&self, expect_seq: Seq, batch: &WalBatch) -> Result<Appended, WalError>;

    /// Segments `from, from+1, …` up to `max` of them, stopping at the first missing number.
    /// A CRC or magic mismatch is `Corrupt { seq }`, never a silent stop.
    async fn read_from(&self, from: Seq, max: usize) -> Result<Vec<WalSegment>, WalError>;

    /// The hint, or `Hint::empty()` if none was written. Never ahead of the true tail.
    async fn hint(&self) -> Result<Hint, WalError>;
    /// Best effort and coalesced by the caller (≤ 1 write/s); failures are logged, not returned to pushers.
    async fn publish_hint(&self, hint: &Hint) -> Result<(), WalError>;

    /// Create-only; an existing checkpoint at the same seq is `Ok` if its bytes are equal.
    ///
    /// As built (rulings T7.2, T7.4): equal but for the idempotency entries
    /// is `Ok` too. Above 64 MiB the write is a HEAD and then an
    /// unconditional multipart upload (no store offers a create-only
    /// multipart completion through `loams-store`), read back afterwards:
    /// two writers of one seq can both upload and the last one wins, and a
    /// different state is detected only by a read-back that sees the other
    /// writer's checkpoint. The WAL fixes the state at a seq, so a correct
    /// writer never offers a different one.
    async fn put_checkpoint(&self, checkpoint: &pb::Checkpoint) -> Result<(), WalError>;
    /// The checkpoint with the greatest seq ≤ `at_or_below`, using the hint, else a LIST of `checkpoints/`.
    ///
    /// As built (ruling T7.3): a non-empty hint whose checkpoint is at or
    /// below the bound is trusted, so a stale hint returns an older
    /// checkpoint than the greatest; the caller replays more segments. Only
    /// the LIST path promises the greatest.
    async fn latest_checkpoint(&self, at_or_below: Seq)
    -> Result<Option<pb::Checkpoint>, WalError>;
}

/// PUTs of one segment before an unknown outcome is returned as an error.
const APPEND_ATTEMPTS: usize = 5;
/// Segment GETs [`WalStore::read_from`] keeps in flight.
const READ_PARALLELISM: usize = 8;
/// Checkpoint names per LIST page.
const LIST_PAGE: usize = 1000;
/// Listings [`WalStore::latest_checkpoint`] makes when the checkpoint it
/// chose disappears under it (GC).
const LIST_ROUNDS: usize = 3;
/// The part size of a large checkpoint's multipart upload.
const PART_SIZE: usize = 8 * 1024 * 1024;

/// The JSON form of [`Hint`] in `head`.
#[derive(Debug, Serialize, Deserialize)]
struct HintJson {
    seq: u64,
    checkpoint: u64,
    written_unix_ms: i64,
}

/// [`WalStore`] over a `loams_store::Store` (§36 §4.1's layout).
#[derive(Clone, Debug)]
pub struct StoreWalStore {
    store: Store,
    paths: RepoPaths,
    clock: Arc<dyn Clock>,
}

impl StoreWalStore {
    /// The WAL of the repository at `paths`, stamping segments with the
    /// system clock.
    pub fn new(store: Store, paths: RepoPaths) -> Self {
        Self {
            store,
            paths,
            clock: Arc::new(SystemClock),
        }
    }

    /// Stamps segments with `clock` instead.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The repository's object paths.
    pub fn paths(&self) -> &RepoPaths {
        &self.paths
    }

    /// Segment `seq`, or `None` if it does not exist.
    async fn read_segment(&self, seq: Seq) -> Result<Option<WalSegment>, WalError> {
        let path = self.paths.segment(seq);
        match retry::retry(|| self.store.get(&path)).await {
            Ok((bytes, _)) => decode_segment(seq, &bytes)
                .map(Some)
                .map_err(|err| WalError::corrupt(seq, err)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Checkpoint `seq`, or `None` if it does not exist.
    async fn read_checkpoint(&self, seq: Seq) -> Result<Option<pb::Checkpoint>, WalError> {
        let path = self.paths.checkpoint(seq);
        match retry::retry(|| self.store.get(&path)).await {
            Ok((bytes, _)) => {
                let checkpoint =
                    decode_checkpoint(&bytes).map_err(|e| WalError::corrupt(seq, e))?;
                if checkpoint.seq != seq.0 {
                    return Err(WalError::corrupt(
                        seq,
                        format!("checkpoint names seq {}", checkpoint.seq),
                    ));
                }
                Ok(Some(checkpoint))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// The greatest checkpoint seq ≤ `at_or_below` in a LIST of
    /// `checkpoints/`.
    async fn list_latest_checkpoint(&self, at_or_below: Seq) -> Result<Option<Seq>, WalError> {
        let prefix = self.paths.checkpoint_prefix();
        let mut best = None;
        let mut after: Option<String> = None;
        loop {
            let page =
                retry::retry(|| self.store.list_page(&prefix, after.as_deref(), LIST_PAGE)).await?;
            let Some(last) = page.last() else {
                return Ok(best);
            };
            after = Some(last.path.clone());
            for info in &page {
                match RepoPaths::parse_checkpoint(&info.path) {
                    Some(seq) if seq <= at_or_below => best = Some(seq),
                    // Listings are sorted, so every later name is greater.
                    Some(_) => return Ok(best),
                    None => {}
                }
            }
        }
    }

    /// Whether the checkpoint stored at `c.seq` states the same repository as
    /// `c`: equal bytes, or equal but for the idempotency entries, which
    /// depend on when each writer pruned its window (ruling T7.2).
    async fn check_existing_checkpoint(
        &self,
        c: &pb::Checkpoint,
        bytes: &Bytes,
    ) -> Result<(), WalError> {
        let seq = Seq(c.seq);
        let path = self.paths.checkpoint(seq);
        let (stored, _) = retry::retry(|| self.store.get(&path)).await?;
        if stored == *bytes {
            return Ok(());
        }
        let mut stored = decode_checkpoint(&stored).map_err(|e| WalError::corrupt(seq, e))?;
        let mut offered = c.clone();
        stored.idempotency.clear();
        offered.idempotency.clear();
        if stored == offered {
            Ok(())
        } else {
            Err(WalError::corrupt(
                seq,
                "another checkpoint is stored at this seq",
            ))
        }
    }

    /// Resolves an unknown or refused append by reading the segment back.
    /// `None` when it is absent and the same bytes may be resent.
    async fn read_back(&self, seq: Seq, batch: &WalBatch) -> Result<Option<Appended>, WalError> {
        Ok(self.read_segment(seq).await?.map(|existing| {
            if existing.batch.batch_id == batch.batch_id {
                Appended::Committed { seq }
            } else {
                Appended::Fenced { existing }
            }
        }))
    }
}

#[async_trait]
impl WalStore for StoreWalStore {
    async fn append(&self, expect_seq: Seq, batch: &WalBatch) -> Result<Appended, WalError> {
        // Encoded once, so every resend writes the same bytes.
        let bytes =
            encode_segment(expect_seq, batch, self.clock.now_unix_ms()).map_err(
                |err| match err {
                    FormatError::TooLarge { .. } => WalError::TooLarge,
                    other => WalError::Invalid {
                        reason: other.to_string(),
                    },
                },
            )?;
        let path = self.paths.segment(expect_seq);
        let mut attempt = 0;
        loop {
            // The last transient error, returned if no attempt resolves.
            let mut unknown = match self.store.put_if_absent(&path, bytes.clone()).await {
                Ok(_) => return Ok(Appended::Committed { seq: expect_seq }),
                // Taken: by this batch on an earlier attempt, or by another.
                Err(StoreError::AlreadyExists { .. }) => None,
                Err(err) if is_transient(&err) => Some(err),
                Err(err) => return Err(err.into()),
            };
            match self.read_back(expect_seq, batch).await {
                Ok(Some(appended)) => return Ok(appended),
                Ok(None) => {}
                // Still unknown: the read failed too. Resend; a later
                // attempt answers it.
                Err(WalError::Store(err)) if is_transient(&err) => unknown = Some(err),
                Err(err) => return Err(err),
            }
            attempt += 1;
            if attempt >= APPEND_ATTEMPTS {
                return Err(match unknown {
                    Some(err) => err.into(),
                    // Refused as existing, then not found: nothing deletes a
                    // segment that young, so the store is misbehaving.
                    None => WalError::corrupt(
                        expect_seq,
                        "the store refused the segment as existing but does not return it",
                    ),
                });
            }
            tracing::debug!(seq = %expect_seq, attempt, "resending a segment after an unknown outcome");
            pause(attempt - 1).await;
        }
    }

    async fn read_from(&self, from: Seq, max: usize) -> Result<Vec<WalSegment>, WalError> {
        let seqs = (0..max as u64).map_while(|i| from.0.checked_add(i).map(Seq));
        let mut gets = stream::iter(seqs)
            .map(|seq| self.read_segment(seq))
            .buffered(READ_PARALLELISM);
        let mut out = Vec::new();
        while let Some(segment) = gets.next().await {
            match segment? {
                Some(segment) => out.push(segment),
                None => break,
            }
        }
        Ok(out)
    }

    async fn hint(&self) -> Result<Hint, WalError> {
        let path = self.paths.head();
        let bytes = match retry::retry(|| self.store.get(&path)).await {
            Ok((bytes, _)) => bytes,
            Err(StoreError::NotFound { .. }) => return Ok(Hint::empty()),
            Err(err) => return Err(err.into()),
        };
        match serde_json::from_slice::<HintJson>(&bytes) {
            Ok(json) if json.checkpoint <= json.seq => Ok(Hint {
                seq: Seq(json.seq),
                checkpoint: Seq(json.checkpoint),
                written_unix_ms: json.written_unix_ms,
            }),
            // The hint only speeds readers up: an unreadable one is no hint.
            Ok(_) | Err(_) => {
                tracing::warn!(path, "ignoring an unreadable WAL hint");
                Ok(Hint::empty())
            }
        }
    }

    async fn publish_hint(&self, hint: &Hint) -> Result<(), WalError> {
        let json = HintJson {
            seq: hint.seq.0,
            checkpoint: hint.checkpoint.0,
            written_unix_ms: hint.written_unix_ms,
        };
        let bytes = serde_json::to_vec(&json).expect("a hint serializes");
        self.store.put(&self.paths.head(), bytes.into()).await?;
        Ok(())
    }

    async fn put_checkpoint(&self, checkpoint: &pb::Checkpoint) -> Result<(), WalError> {
        let bytes = encode_checkpoint(checkpoint);
        let path = self.paths.checkpoint(Seq(checkpoint.seq));
        if bytes.len() as u64 > MULTIPART_THRESHOLD {
            // A multipart upload cannot be create-only: look first, upload,
            // then read back, so a racing writer's different state is at
            // least detected (T7.4). Writers of one seq offer the same state.
            match retry::retry(|| self.store.head(&path)).await {
                Ok(_) => return self.check_existing_checkpoint(checkpoint, &bytes).await,
                Err(StoreError::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
            retry::retry(|| {
                let body = bytes.clone();
                self.store.put_multipart(
                    &path,
                    stream::once(async move { Ok(body) }).boxed(),
                    PART_SIZE,
                )
            })
            .await?;
            return self.check_existing_checkpoint(checkpoint, &bytes).await;
        }
        match retry::retry(|| self.store.put_if_absent(&path, bytes.clone())).await {
            Ok(_) => Ok(()),
            Err(StoreError::AlreadyExists { .. }) => {
                self.check_existing_checkpoint(checkpoint, &bytes).await
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn latest_checkpoint(
        &self,
        at_or_below: Seq,
    ) -> Result<Option<pb::Checkpoint>, WalError> {
        let hint = self.hint().await?;
        if !hint.is_empty()
            && hint.checkpoint <= at_or_below
            && let Some(checkpoint) = self.read_checkpoint(hint.checkpoint).await?
        {
            return Ok(Some(checkpoint));
        }
        for _ in 0..LIST_ROUNDS {
            let Some(seq) = self.list_latest_checkpoint(at_or_below).await? else {
                return Ok(None);
            };
            if let Some(checkpoint) = self.read_checkpoint(seq).await? {
                return Ok(Some(checkpoint));
            }
            // Deleted between the LIST and the GET: list again.
        }
        Err(StoreError::NotFound {
            path: self.paths.checkpoint_prefix(),
        }
        .into())
    }
}
