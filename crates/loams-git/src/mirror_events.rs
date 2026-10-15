//! The `_git` event-stream mirror (§36 §4.8; plan GT1 Task 15, the morning
//! version's Task 17).
//!
//! Every committed transaction's CloudEvent (the one its segment already
//! holds, D415) is appended to the namespace stream `_git`, keyed by
//! repository id, so consumers (webhooks, indexers, Task 65's promoter) read
//! repository changes as events. The mirror runs beside the sequencer and
//! never delays an acknowledgement: it follows the WAL with
//! [`RefLog::watch`] and publishes through a [`GitEventSink`], retrying with
//! backoff while the sink fails.
//!
//! Delivery is at least once. The sink deduplicates by (`source`, `id`)
//! (D270's ledger), and a best-effort cursor in the bucket
//! (`ns/<ns>/repos/<repo_id>/mirror.json`) records how far the mirror got,
//! so a restarted mirror and [`repair_repo`] re-send only the tail it had
//! not recorded.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use loams_cloudevents::CloudEvent;
use loams_store::{Store, StoreError};
use serde::{Deserialize, Serialize};

use crate::format::event::to_cloudevent;
use crate::ids::{NamespaceId, RepoId, Seq};
use crate::paths::RepoPaths;
use crate::reflog::{RefError, RefLog};
use crate::wal::WalStore;

/// Events per publish.
const BATCH: usize = 64;
/// The first and the longest pause between failed publishes.
const RETRY: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(5));
/// The cursor is written at most this often while events flow.
const CURSOR_EVERY: Duration = Duration::from_secs(1);

/// Where the mirror appends: the namespace's `_git` stream (the server
/// implements it over its CloudEvents ingest, which deduplicates by
/// `source` and `id`).
#[async_trait]
pub trait GitEventSink: Send + Sync + fmt::Debug {
    /// Appends `events` (one repository's, in commit order) to namespace
    /// `ns`'s `_git` stream, keyed by `repo`. A repeat of an event already
    /// appended inside the dedup window is not appended again.
    async fn publish(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
        events: &[CloudEvent],
    ) -> Result<(), String>;
}

/// How far a repository's mirror got: best effort, never ahead of what was
/// published.
#[derive(Clone, Debug)]
pub struct MirrorCursor {
    store: Store,
    paths: RepoPaths,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorDoc {
    seq: u64,
}

impl MirrorCursor {
    /// The cursor of `repo` in `ns`.
    pub fn new(store: Store, ns: NamespaceId, repo: &RepoId) -> Self {
        let paths = RepoPaths::new(ns, repo);
        Self {
            store,
            path: paths.mirror_cursor(),
            paths,
        }
    }

    /// The first segment the WAL still holds after `after`, by a listing: past a gap that
    /// segment GC left (PR #612 review), the retained suffix starts there. `None` when no
    /// later segment exists.
    pub async fn first_retained_after(&self, after: Seq) -> Result<Option<Seq>, StoreError> {
        let page = self
            .store
            .list_page(
                &self.paths.wal_prefix(),
                Some(&self.paths.segment(after)),
                1,
            )
            .await?;
        Ok(page
            .first()
            .and_then(|info| RepoPaths::parse_segment(&info.path)))
    }

    /// The last seq published, `0` when none (or the cursor is unreadable:
    /// the mirror then re-sends, and the sink deduplicates).
    pub async fn get(&self) -> Seq {
        match self.store.get(&self.path).await {
            Ok((bytes, _)) => serde_json::from_slice::<CursorDoc>(&bytes)
                .map(|doc| Seq(doc.seq))
                .unwrap_or(Seq::ZERO),
            Err(StoreError::NotFound { .. }) => Seq::ZERO,
            Err(err) => {
                tracing::warn!(path = %self.path, %err, "reading the mirror cursor failed");
                Seq::ZERO
            }
        }
    }

    /// Records that every event up to `seq` was published (best effort).
    pub async fn set(&self, seq: Seq) {
        let body = serde_json::to_vec(&CursorDoc { seq: seq.0 }).unwrap_or_default();
        if let Err(err) = self.store.put(&self.path, Bytes::from(body)).await {
            tracing::warn!(path = %self.path, %err, "writing the mirror cursor failed");
        }
    }
}

/// Publishes `events`, retrying with backoff until the sink takes them.
async fn publish_until_taken(
    sink: &dyn GitEventSink,
    ns: NamespaceId,
    repo: &RepoId,
    events: &[CloudEvent],
) {
    let mut pause = RETRY.0;
    while let Err(err) = sink.publish(ns, repo, events).await {
        tracing::warn!(%ns, %repo, error = %err, "mirroring to _git failed; retrying");
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(RETRY.1);
    }
}

/// Mirrors `log` (repository `repo` of `ns`) into `_git` from the cursor
/// on, until the task is aborted (the sequencer closes). Never returns.
pub async fn run_mirror(
    log: Arc<dyn RefLog>,
    cursor: MirrorCursor,
    sink: Arc<dyn GitEventSink>,
    ns: NamespaceId,
    repo: RepoId,
) {
    let mut from = cursor.get().await.next();
    loop {
        let mut changes = log.watch(from).ready_chunks(BATCH);
        let mut written = Instant::now();
        let mut unwritten: Option<Seq> = None;
        while let Some(chunk) = changes.next().await {
            let mut events = Vec::with_capacity(chunk.len());
            let mut last = None;
            let mut failure = None;
            for item in chunk {
                match item {
                    Ok(txn) => {
                        events.push(to_cloudevent(&txn.event, txn.seq));
                        last = Some(txn.seq);
                    }
                    Err(err) => {
                        failure = Some(err);
                        break;
                    }
                }
            }
            if !events.is_empty() {
                publish_until_taken(sink.as_ref(), ns, &repo, &events).await;
            }
            if let Some(seq) = last {
                from = seq.next();
                unwritten = Some(seq);
                if written.elapsed() >= CURSOR_EVERY {
                    cursor.set(seq).await;
                    written = Instant::now();
                    unwritten = None;
                }
            }
            if let Some(err) = failure {
                // Segments the mirror needed are gone (GC after a long outage): their events
                // are lost to `_git`, and the mirror resumes at the first segment the WAL
                // still holds, never past one it has not published (PR #612 review).
                if let RefError::Invalid { reason } = &err {
                    match cursor
                        .first_retained_after(Seq(from.0.saturating_sub(1)))
                        .await
                    {
                        Ok(Some(next)) if next > from => {
                            tracing::error!(
                                %ns, %repo, %reason, lost_from = %from, lost_to = %Seq(next.0 - 1),
                                "the _git mirror lost events: the WAL no longer holds these segments"
                            );
                            from = next;
                        }
                        Ok(_) => {}
                        Err(err) => tracing::warn!(%ns, %repo, %err, "listing the WAL failed"),
                    }
                } else {
                    tracing::warn!(%ns, %repo, error = %err, "following the WAL for _git failed");
                }
                break;
            }
            // The watch has caught up for now: record the position.
            if let Some(seq) = unwritten.take() {
                cursor.set(seq).await;
                written = Instant::now();
            }
        }
        tokio::time::sleep(RETRY.0).await;
    }
}

/// Publishes whatever `repo`'s WAL holds past its cursor (the hourly repair
/// after a crash between commit and mirror; plan GT1 Task 15). Returns how
/// many events it published.
pub async fn repair_repo(
    wal: &dyn WalStore,
    cursor: &MirrorCursor,
    sink: &dyn GitEventSink,
    ns: NamespaceId,
    repo: &RepoId,
) -> Result<usize, RefError> {
    let mut at = cursor.get().await;
    let mut published = 0;
    loop {
        let segments = wal.read_from(at.next(), BATCH).await?;
        let Some(last) = segments.last().map(|s| s.seq) else {
            // Nothing at `at + 1`: either the mirror is current, or segment GC left a gap
            // and later segments are still held (PR #612 review). Their events are lost to
            // `_git`; the retained suffix is published, and the gap reported.
            let next = cursor
                .first_retained_after(at)
                .await
                .map_err(|err| RefError::Wal(err.into()))?;
            match next {
                Some(next) if next > at.next() => {
                    tracing::error!(
                        %ns, %repo, lost_from = %at.next(), lost_to = %Seq(next.0 - 1),
                        "the _git repair found events the WAL no longer holds"
                    );
                    at = Seq(next.0 - 1);
                    continue;
                }
                _ => return Ok(published),
            }
        };
        let events: Vec<CloudEvent> = segments
            .iter()
            .flat_map(|segment| segment.batch.events.iter().cloned())
            .collect();
        if !events.is_empty() {
            publish_until_taken(sink, ns, repo, &events).await;
            published += events.len();
        }
        cursor.set(last).await;
        at = last;
    }
}
