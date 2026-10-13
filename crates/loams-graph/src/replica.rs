//! Followers: read replicas of a durable graph on nodes that do not own it (GR1 Task 15; design
//! §48 §7.2).
//!
//! A graph's catalog record names how many followers it has (`replicas`, at most
//! [`crate::service::admin::MAX_REPLICAS`]). They run on the graph nodes the rendezvous ranks
//! right after the owner's place ([`crate::owner::Ownership::follows`]), and serve `eventual` and
//! `at_least` reads there without a hop to the owner. A `strong` read and every write still go to
//! the owner.
//!
//! * **What a follower is.** A Grafeo engine of its own, in memory, in an [`Engine`] apart from
//!   the owned graphs' (so checkpoints, eviction, stamping and the owner's open never see it). It
//!   opens the way an owner does (a snapshot from the manifest and the records after it, or the
//!   whole stream; [`crate::recovery::open`]), but without a lease: it never appends, and every
//!   writing statement is refused before it reaches the engine.
//! * **The tail.** A task per follower fetches the stream from the follower's position, waiting at
//!   its end for new records ([`FOLLOW_WAIT`] per fetch), and applies each record the way a
//!   replay does, under the graph's gate held exclusively, so a read sees whole records only.
//!   Then it publishes the new position, which wakes the reads waiting for a token
//!   ([`crate::consistency::wait_for`]).
//! * **When a follower stops.** A record that does not apply, or a stream trimmed past the
//!   follower's position, marks it for reload: its next read rebuilds it, from a snapshot.
//!   [`Replicas::close`] stops one (this node took the graph's lease, the graph was deleted or
//!   placed elsewhere, or it sat idle: `GraphAdmin::evict_due`).
//!
//! A follower's state is never newer than the stream, so it never shows a write that is not
//! durable (I2): it applies only records the log has committed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use loams_common::StreamId;
use loams_log::LogError;

use crate::GraphId;
use crate::durable::GraphLog;
use crate::engine::{Engine, Graph};

/// How long one fetch of a follower's tail waits at the end of the stream for new records.
pub const FOLLOW_WAIT: Duration = Duration::from_millis(500);

/// How long a tail waits after the log failed a fetch before it tries again.
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// The followers of one node.
pub struct Replicas {
    engine: Arc<Engine>,
    tails: Mutex<HashMap<GraphId, Tail>>,
    /// **Tests only:** tails apply nothing while set.
    #[cfg(feature = "test-hooks")]
    paused: Arc<std::sync::atomic::AtomicBool>,
}

/// A follower's tail task.
struct Tail {
    graph: Weak<Graph>,
    task: tokio::task::AbortHandle,
}

impl std::fmt::Debug for Replicas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replicas")
            .field("tails", &self.tails_map().len())
            .finish_non_exhaustive()
    }
}

impl Replicas {
    /// Followers in memory, opened with `main`'s statement time limit and property size.
    #[must_use]
    pub fn new(main: &Engine) -> Self {
        Self {
            engine: Arc::new(
                Engine::new()
                    .with_query_timeout(main.query_timeout())
                    .with_max_property_bytes(main.max_property_bytes()),
            ),
            tails: Mutex::new(HashMap::new()),
            #[cfg(feature = "test-hooks")]
            paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The engine the followers are open in.
    #[must_use]
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    fn tails_map(&self) -> std::sync::MutexGuard<'_, HashMap<GraphId, Tail>> {
        self.tails.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The followers open here.
    #[must_use]
    pub fn followers(&self) -> Vec<Arc<Graph>> {
        self.engine.list(None).unwrap_or_default()
    }

    /// Starts tailing follower `graph` from `log`, unless its tail already runs. Called after
    /// every open of a follower, so a follower rebuilt after a reload is tailed again.
    pub(crate) fn follow(&self, graph: &Arc<Graph>, log: &GraphLog) {
        let (Some(id), Some(stream)) = (graph.id(), graph.stream()) else {
            return;
        };
        let mut tails = self.tails_map();
        if let Some(tail) = tails.get(&id)
            && std::ptr::eq(tail.graph.as_ptr(), Arc::as_ptr(graph))
            && !tail.task.is_finished()
        {
            return;
        }
        if let Some(old) = tails.remove(&id) {
            old.task.abort();
        }
        let task = tokio::spawn(tail(
            Arc::downgrade(graph),
            log.clone(),
            stream,
            #[cfg(feature = "test-hooks")]
            Arc::clone(&self.paused),
        ));
        tails.insert(
            id,
            Tail {
                graph: Arc::downgrade(graph),
                task: task.abort_handle(),
            },
        );
    }

    /// Stops following graph `namespace`/`name` and closes its follower, unless a read holds it
    /// (then it is closed by a later call). Answers whether a follower was closed.
    pub fn close(&self, namespace: &str, name: &str) -> bool {
        let Ok(open) = self.engine.list(Some(namespace)) else {
            return false;
        };
        let Some(graph) = open.into_iter().find(|g| g.name() == name) else {
            return false;
        };
        if let Some(id) = graph.id()
            && let Some(tail) = self.tails_map().remove(&id)
        {
            tail.task.abort();
        }
        drop(graph);
        match self.engine.discard(namespace, name) {
            Ok(closed) => closed,
            Err(err) => {
                tracing::debug!(%namespace, %name, error = %err, "a follower is still in use; closed by a later pass");
                false
            }
        }
    }

    /// Stops every tail and closes every follower nobody holds (shutdown).
    pub fn close_all(&self) {
        for graph in self.followers() {
            let (namespace, name) = (graph.namespace().to_string(), graph.name().to_string());
            drop(graph);
            self.close(&namespace, &name);
        }
        for (_, tail) in self.tails_map().drain() {
            tail.task.abort();
        }
    }

    /// **Tests only** (feature `test-hooks`): while `paused`, tails fetch and apply nothing, so a
    /// follower falls behind its owner.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn pause(&self, paused: bool) {
        self.paused
            .store(paused, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for Replicas {
    fn drop(&mut self) {
        for (_, tail) in self.tails_map().drain() {
            tail.task.abort();
        }
    }
}

/// A follower's tail: fetch from its position, apply, publish; until the follower is dropped or
/// marked for reload.
async fn tail(
    graph: Weak<Graph>,
    log: GraphLog,
    stream: StreamId,
    #[cfg(feature = "test-hooks")] paused: Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        // Only a weak handle is kept between fetches, so a follower nobody reads can be closed.
        let from = match graph.upgrade() {
            Some(follower) if !follower.needs_reopen() => {
                follower.consistency_token().map_or(0, |token| token.offset)
            }
            _ => return,
        };
        #[cfg(feature = "test-hooks")]
        if paused.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let fetching = tokio::time::Instant::now();
        match log.fetch(stream, from, FOLLOW_WAIT).await {
            // An empty answer before the wait was up: the metastore stopped (nothing will
            // change) or the read raced its end. Back off rather than spin, which would also
            // hold a runtime worker (GR1 Task 16).
            Ok(fetched) if fetched.records.is_empty() => {
                if fetching.elapsed() < FOLLOW_WAIT / 2 {
                    tokio::time::sleep(RETRY_AFTER).await;
                }
            }
            Ok(fetched) => {
                // A fetch that was waiting when the tail was paused applies nothing.
                #[cfg(feature = "test-hooks")]
                if paused.load(std::sync::atomic::Ordering::Acquire) {
                    continue;
                }
                let Some(follower) = graph.upgrade() else {
                    return;
                };
                let applied = tokio::task::spawn_blocking(move || {
                    let applied = follower.follow(&fetched.records);
                    if applied.is_err() {
                        follower.mark_reloading();
                    }
                    applied
                })
                .await;
                match applied {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        tracing::warn!(%stream, error = %err, "a follower could not apply its stream; it is rebuilt on its next read");
                        return;
                    }
                    Err(err) => {
                        tracing::error!(%stream, error = %err, "a follower's tail failed");
                        return;
                    }
                }
            }
            // Trimmed past this follower's position: only a snapshot has what it lacks.
            Err(LogError::OffsetOutOfRange {
                log_start_offset, ..
            }) if log_start_offset > from => {
                if let Some(follower) = graph.upgrade() {
                    follower.mark_reloading();
                }
                tracing::info!(%stream, from, log_start_offset, "a follower fell behind its stream's trim; it is rebuilt on its next read");
                return;
            }
            Err(err) => {
                tracing::debug!(%stream, error = %err, "a follower's fetch failed; retrying");
                tokio::time::sleep(RETRY_AFTER).await;
            }
        }
    }
}
