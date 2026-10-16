//! A reader's pinned view of the adjacency (GR1 Task 22; design §48 §26.3 "Reading it"; D1064).
//!
//! An algorithm first pins an offset `M`: the owner's durable offset when it starts, or at least a
//! token's. Then:
//!
//! * **The index path.** It loads the published index (covering `N`) from memory, the local
//!   cache or the bucket, reads the stream's records `[N, M)` and overlays them, so the view is
//!   the graph at exactly `M`. The log and the index file are immutable, so this path holds no
//!   gate: writes are acknowledged while it runs.
//! * **The canonical fallback.** When the tail is no longer in the stream, or longer than
//!   [`AdjacencyConfig::overlay_max_records`](super::AdjacencyConfig), or the file does not
//!   check, it builds from the engine under the shared gate (at an offset at or above `M`) and
//!   publishes that. It never answers from a stale index.
//! * A graph with no index yet gets one: a full build, published (the first algorithm run).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use loams_common::NamespaceId;

use super::{AdjacencyIndex, Decline, Indexer, Tail, patch};
use crate::consistency::Consistency;
use crate::engine::Graph;
use crate::snapshot::SnapshotError;

/// Where a pinned view came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinSource {
    /// The published index covered the pinned offset: no tail.
    Index,
    /// The published index plus this many records of the tail.
    Overlay(u64),
    /// No index yet: a full build, now published.
    Built,
    /// The canonical path, for this reason.
    Fallback(Decline),
}

/// The adjacency of a graph at one offset (shared contract `PinnedAdjacency`).
#[derive(Debug, Clone)]
pub struct PinnedAdjacency {
    /// The view: the graph's vertices and edges at [`PinnedAdjacency::offset`].
    pub view: Arc<AdjacencyIndex>,
    /// The offset pinned: every record below it, and none above, is in the view.
    pub offset: u64,
    /// Where it came from.
    pub source: PinSource,
}

impl Indexer {
    /// Pins `graph` (open and owned here, in `namespace`) at `consistency` and answers its
    /// adjacency there (shared contract `PinnedAdjacency::pin`). `AtLeast` waits up to `wait` for
    /// the graph to reach the token; `Strong` and `Eventual` pin the durable offset now.
    ///
    /// # Errors
    ///
    /// [`SnapshotError::NotServing`] for a graph not durable here,
    /// [`crate::engine::GraphError::ConsistencyWaitTimeout`] (as [`SnapshotError::Engine`]) when an `AtLeast`
    /// token is not reached within `wait`, or what
    /// reading the stream, the bucket or the engine answers.
    pub async fn pin(
        &self,
        graph: Arc<Graph>,
        namespace: NamespaceId,
        consistency: &Consistency,
        wait: Duration,
    ) -> Result<PinnedAdjacency, SnapshotError> {
        let (Some(id), Some(durable)) = (graph.id(), graph.durable_state()) else {
            return Err(SnapshotError::NotServing(
                "the graph is not durable here".to_string(),
            ));
        };
        if let Consistency::AtLeast(token) = consistency {
            crate::consistency::wait_for(&graph, *token, wait)
                .await
                .map_err(SnapshotError::Engine)?;
        }
        let target = durable.position.offset();
        let (log, stream) = (durable.log.clone(), durable.stream);
        let Some(entry) = self.entry(namespace, id).await? else {
            let (_, index) = self.build(Arc::clone(&graph), namespace).await?;
            return Ok(PinnedAdjacency {
                offset: index.covered_offset,
                view: index,
                source: PinSource::Built,
            });
        };
        let reason = match self.load(id, &entry).await? {
            Err(decline) => decline,
            // Published after the pin was read: newer, never stale.
            Ok(index) if index.covered_offset >= target => {
                #[cfg(feature = "test-hooks")]
                self.pin_hook().await;
                return Ok(PinnedAdjacency {
                    offset: index.covered_offset,
                    view: index,
                    source: PinSource::Index,
                });
            }
            Ok(index) => match self
                .tail(
                    &log,
                    stream,
                    index.covered_offset,
                    target,
                    self.config.overlay_max_records,
                )
                .await?
            {
                Err(decline) => decline,
                Ok(tail) => match overlay(&index, &tail) {
                    Err(decline) => decline,
                    Ok((view, records, delta_edges)) => {
                        self.stats
                            .delta_edges
                            .fetch_add(delta_edges, Ordering::Relaxed);
                        self.stats.overlay(records);
                        #[cfg(feature = "test-hooks")]
                        self.pin_hook().await;
                        return Ok(PinnedAdjacency {
                            offset: view.covered_offset,
                            view: Arc::new(view),
                            source: PinSource::Overlay(records),
                        });
                    }
                },
            },
        };
        // The canonical path: a build from the engine at (or above) the pinned offset, published.
        self.stats.fallback(reason);
        let (index, scanned) = self.scan(&graph).await?;
        let fence = durable.fence.clone();
        let (_, view) = self
            .finish(
                &graph,
                namespace,
                fence.as_ref(),
                index,
                crate::snapshot::manifest::AdjacencyMode::Full,
                Some(reason),
                0,
                scanned,
            )
            .await?;
        Ok(PinnedAdjacency {
            offset: view.covered_offset,
            view,
            source: PinSource::Fallback(reason),
        })
    }
}

/// `index` with `tail` overlaid: the view at the tail's end, the records and the edge
/// operations it applied.
fn overlay(index: &AdjacencyIndex, tail: &Tail) -> Result<(AdjacencyIndex, u64, u64), Decline> {
    if tail.records.is_empty() {
        return Ok((index.clone(), 0, 0));
    }
    let patched = patch(index, tail)?;
    Ok((patched.index, patched.records, patched.delta_edges))
}
