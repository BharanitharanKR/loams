//! Checkpoints (§36 §4.1, §7; plan GT1 Task 11, old GT1 Task 8).
//!
//! - [`create_repo`] writes checkpoint 0 of an empty repository.
//! - [`CheckpointPolicy`] says when the sequencer writes a checkpoint: after
//!   the commit that brings the segments or bytes since the last one to the
//!   policy. The write runs beside the sequencer, after that commit's
//!   receipts are answered, so it never delays an acknowledgement; at most
//!   one is in flight, and the hint names it from the next commit on.
//! - [`state_at`] rebuilds the state after any retained seq: the latest
//!   checkpoint at or below it, then its segments.

use crate::format::pb;
use crate::ids::Seq;
use crate::limits::{CHECKPOINT_EVERY_BYTES, CHECKPOINT_EVERY_SEGMENTS};
use crate::reflog::RefError;
use crate::state::{RepoState, StateError};
use crate::wal::WalStore;

/// Segments read per WAL request while replaying.
const READ_BATCH: usize = 64;

/// When the sequencer writes a checkpoint (§36 §7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointPolicy {
    /// After this many segments since the last checkpoint (256).
    pub every_segments: u64,
    /// After this many encoded event bytes since the last checkpoint (8 MiB).
    pub every_bytes: u64,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            every_segments: CHECKPOINT_EVERY_SEGMENTS,
            every_bytes: CHECKPOINT_EVERY_BYTES,
        }
    }
}

impl CheckpointPolicy {
    /// Whether `segments` segments of `bytes` event bytes since the last
    /// checkpoint call for a new one.
    pub fn is_due(&self, segments: u64, bytes: u64) -> bool {
        segments >= self.every_segments || bytes >= self.every_bytes
    }
}

/// Creates a repository: its checkpoint 0, with no refs (old GT1 Task 8).
/// Idempotent: an equal checkpoint 0 is accepted. Refuses SHA-256 (Ruling 8).
pub async fn create_repo(wal: &dyn WalStore, format: pb::ObjectFormat) -> Result<(), RefError> {
    if format != pb::ObjectFormat::Sha1 {
        return Err(RefError::State(StateError::UnsupportedFormat {
            format: match format {
                pb::ObjectFormat::Sha256 => "sha256",
                _ => "unspecified",
            },
        }));
    }
    wal.put_checkpoint(&RepoState::empty(format).to_checkpoint(0))
        .await?;
    Ok(())
}

/// Applies segments from `state.seq + 1` until one is missing or `stop` is
/// reached.
pub(crate) async fn replay(
    wal: &dyn WalStore,
    state: &mut RepoState,
    stop: Option<Seq>,
) -> Result<(), RefError> {
    loop {
        let want = match stop {
            Some(stop) if state.seq >= stop => return Ok(()),
            Some(stop) => {
                usize::try_from(stop.0 - state.seq.0).map_or(READ_BATCH, |n| n.min(READ_BATCH))
            }
            None => READ_BATCH,
        };
        let segments = wal.read_from(state.seq.next(), want).await?;
        if segments.is_empty() {
            return Ok(());
        }
        for segment in &segments {
            state.apply_segment(segment)?;
        }
    }
}

/// The state after `seq`: the latest checkpoint at or below it, replayed to
/// it. `Invalid` when the log does not hold that state (not committed yet,
/// or no longer retained), `NotFound` when the repository has no
/// checkpoint at all.
pub async fn state_at(wal: &dyn WalStore, seq: Seq) -> Result<RepoState, RefError> {
    let checkpoint = wal.latest_checkpoint(seq).await?.ok_or_else(|| {
        if seq == Seq(u64::MAX) {
            RefError::NotFound
        } else {
            RefError::Invalid {
                reason: format!("no checkpoint at or below seq {seq} is retained"),
            }
        }
    })?;
    let mut state = RepoState::from_checkpoint(checkpoint)?;
    replay(wal, &mut state, Some(seq)).await?;
    if state.seq == seq {
        Ok(state)
    } else {
        Err(RefError::Invalid {
            reason: format!(
                "the state at seq {seq} is not held: the log stops at seq {}",
                state.seq
            ),
        })
    }
}
