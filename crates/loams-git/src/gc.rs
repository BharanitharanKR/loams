//! Segment GC (§36 §7; plan GT1 Task 11, old GT1 Task 8, ruling T7.3).
//!
//! The plan keeps every segment a reader can still need and deletes the
//! rest once they are older than the grace period:
//!
//! - **`Exactly` reads.** A state that was current at any time in the last
//!   `exactly_retention` (24 h) must stay readable, so the checkpoint
//!   `keep_from` is the newest one at or below the last seq that was
//!   already committed at the retention cutoff (the newest checkpoint when
//!   no segment is that young). Segments above it are kept.
//! - **The hint.** Readers trust the checkpoint the hint names (T7.3), so
//!   `keep_from` is never above it: a stale hint keeps more.
//! - **Deleted:** segments at or below `keep_from`, checkpoints below it,
//!   and segments above a gap (unreachable: a correct writer never writes
//!   them), each only once its last-modified time is older than `grace`.
//!
//! Listings go through `Store::list_page`; deletes are idempotent.

use std::time::Duration;

use loams_store::{ObjectInfo, Store};

use crate::ids::Seq;
use crate::paths::RepoPaths;
use crate::reflog::RefError;
use crate::wal::{WalError, WalStore};

/// Names per listing page.
const LIST_PAGE: usize = 1000;

/// What [`run_segment_gc`] deletes (old GT1 Task 8).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SegmentGcPlan {
    /// Object paths to delete, oldest seq first.
    pub delete: Vec<String>,
    /// The checkpoint every kept read starts at or after; segments above it
    /// are kept.
    pub keep_from: Seq,
}

/// Every object under `prefix` whose name `parse` reads as a seq, sorted.
async fn list_seqs(
    store: &Store,
    prefix: &str,
    parse: fn(&str) -> Option<Seq>,
) -> Result<Vec<(Seq, ObjectInfo)>, RefError> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = store
            .list_page(prefix, after.as_deref(), LIST_PAGE)
            .await
            .map_err(WalError::Store)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.path.clone());
        out.extend(
            page.into_iter()
                .filter_map(|info| parse(&info.path).map(|seq| (seq, info))),
        );
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// An object's last-modified time as signed ms (the store reports `u64`).
fn modified(info: &ObjectInfo) -> i64 {
    i64::try_from(info.last_modified_ms).unwrap_or(i64::MAX)
}

/// Plans segment GC for the repository at `paths` at `now_unix_ms`.
pub async fn plan_segment_gc(
    wal: &dyn WalStore,
    store: &Store,
    paths: &RepoPaths,
    now_unix_ms: i64,
    exactly_retention: Duration,
    grace: Duration,
) -> Result<SegmentGcPlan, RefError> {
    let hint = wal.hint().await?;
    let checkpoints = list_seqs(
        store,
        &paths.checkpoint_prefix(),
        RepoPaths::parse_checkpoint,
    )
    .await?;
    let Some((newest, _)) = checkpoints.last() else {
        // No checkpoint: not a repository, or not created yet.
        return Ok(SegmentGcPlan::default());
    };
    let newest = *newest;
    let segments = list_seqs(store, &paths.wal_prefix(), RepoPaths::parse_segment).await?;

    // The tail: segments run without a gap from the newest checkpoint.
    let mut tail = newest;
    for (seq, _) in segments.iter().filter(|(seq, _)| *seq > newest) {
        if *seq == tail.next() {
            tail = *seq;
        } else {
            break;
        }
    }

    // The oldest state readers may still ask for exactly: the one current
    // at the cutoff, i.e. just before the first segment written after it.
    let cutoff = now_unix_ms.saturating_sub(millis(exactly_retention));
    let oldest_needed = segments
        .iter()
        .filter(|(seq, _)| *seq <= tail)
        .find(|(_, info)| modified(info) >= cutoff)
        .map_or(tail, |(seq, _)| Seq(seq.0 - 1));
    let retained = checkpoints
        .iter()
        .rev()
        .map(|(seq, _)| *seq)
        .find(|seq| *seq <= oldest_needed)
        // No checkpoint is that old: keep from the oldest there is.
        .unwrap_or(checkpoints[0].0);
    let keep_from = if hint.is_empty() {
        retained
    } else {
        retained.min(hint.checkpoint)
    };

    let old = |info: &ObjectInfo| modified(info) <= now_unix_ms.saturating_sub(millis(grace));
    let mut delete: Vec<(Seq, String)> = Vec::new();
    for (seq, info) in &segments {
        let unneeded = *seq <= keep_from;
        let unreachable = *seq > tail;
        if (unneeded || unreachable) && old(info) {
            delete.push((*seq, info.path.clone()));
        }
    }
    for (seq, info) in &checkpoints {
        if *seq < keep_from && old(info) {
            delete.push((*seq, info.path.clone()));
        }
    }
    delete.sort();
    Ok(SegmentGcPlan {
        delete: delete.into_iter().map(|(_, path)| path).collect(),
        keep_from,
    })
}

/// Deletes what `plan` names, oldest first; returns how many objects were
/// deleted (deletes are idempotent, so a rerun counts them again).
pub async fn run_segment_gc(store: &Store, plan: &SegmentGcPlan) -> Result<u64, RefError> {
    let mut deleted = 0;
    for path in &plan.delete {
        store.delete(path).await.map_err(WalError::Store)?;
        deleted += 1;
    }
    Ok(deleted)
}
