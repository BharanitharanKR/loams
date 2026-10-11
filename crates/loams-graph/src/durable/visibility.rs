//! What a client may see (design §48 §6.1 I2, §6.2; rulings R9.3, R9.5).
//!
//! Grafeo 0.5.43's viewing epoch hides only element *creation*: property, label and delete
//! changes, committed or not, are visible to a session pinned at an older epoch (R9.3). So I2 is
//! kept by a per-graph [`Gate`], a reader-writer lock: the write lane holds it exclusively from
//! `begin_transaction` until its group is durable (or the graph is marked for reload), and every
//! client statement holds it shared. A client statement therefore never runs while a write is
//! pending, and the engine's current state is the durable state whenever it runs.
//!
//! Sessions are still pinned at the durable state's engine epoch ([`Visibility::client_session`]).
//! It is redundant under the gate and harmless (R9.3), and it keeps a future engine whose viewing
//! epoch does isolate (upstream Q679) correct without the gate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use grafeo_common::types::EpochId;

use crate::classify::Access;
use crate::engine::Graph;

/// A graph's reader-writer gate (R9.3).
#[derive(Debug, Default)]
pub(crate) struct Gate(RwLock<()>);

impl Gate {
    /// Held by a client statement while it runs.
    pub(crate) fn shared(&self) -> RwLockReadGuard<'_, ()> {
        // Nothing is ever done under the gate that a panic could leave half-done for it: the
        // lane contains its own panics, so a poisoned gate carries no meaning.
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Held by the write lane from a group's first `begin_transaction` until it is durable.
    pub(crate) fn exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The durable state's position: the stream's next offset (R9.5: the durable epoch is a log
/// offset, not a Grafeo epoch), published through a watch channel for followers and consistency
/// waits (Task 15), with the engine's epoch and counts at that state.
#[derive(Debug)]
pub(crate) struct Position {
    offset: tokio::sync::watch::Sender<u64>,
    engine_epoch: AtomicU64,
    nodes: AtomicU64,
    edges: AtomicU64,
}

impl Position {
    /// The position after a replay or adoption: `offset`, with the engine as it now is.
    pub(crate) fn new(offset: u64, engine_epoch: u64, counts: (u64, u64)) -> Self {
        Self {
            offset: tokio::sync::watch::Sender::new(offset),
            engine_epoch: AtomicU64::new(engine_epoch),
            nodes: AtomicU64::new(counts.0),
            edges: AtomicU64::new(counts.1),
        }
    }

    /// The stream's next offset after every durable record.
    pub(crate) fn offset(&self) -> u64 {
        *self.offset.borrow()
    }

    /// A receiver of the durable offset.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.offset.subscribe()
    }

    /// The engine's epoch at the durable state.
    pub(crate) fn engine_epoch(&self) -> u64 {
        self.engine_epoch.load(Ordering::Acquire)
    }

    /// Node and edge counts at the durable state (`GetGraph`'s stats, read without the gate).
    pub(crate) fn counts(&self) -> (u64, u64) {
        (
            self.nodes.load(Ordering::Acquire),
            self.edges.load(Ordering::Acquire),
        )
    }

    /// Publishes a new durable state. Called by the lane under the exclusive gate.
    pub(crate) fn publish(&self, offset: u64, engine_epoch: u64, counts: (u64, u64)) {
        self.engine_epoch.store(engine_epoch, Ordering::Release);
        self.nodes.store(counts.0, Ordering::Release);
        self.edges.store(counts.1, Ordering::Release);
        self.offset.send_replace(offset);
    }
}

/// Client sessions (shared contract, §48 §6.2).
#[derive(Debug, Clone, Copy)]
pub struct Visibility;

impl Visibility {
    /// A read session for a client statement on `graph`: the read-only role, pinned at the durable
    /// state's engine epoch when the graph is durable. The caller holds the graph's gate shared
    /// while the session runs; [`Graph::execute`] and its siblings do.
    #[must_use]
    pub fn client_session(graph: &Graph) -> grafeo::Session {
        Self::session(graph, Access::Read)
    }

    /// [`Visibility::client_session`] with the role `access` needs.
    pub(crate) fn session(graph: &Graph, access: Access) -> grafeo::Session {
        let session = graph.session_for(access);
        if let Some(epoch) = graph.durable_engine_epoch() {
            session.set_viewing_epoch(EpochId::new(epoch));
        }
        session
    }
}
