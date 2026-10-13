//! An in-memory Fabric (one topic, consumer groups, a DLQ, and `ingest`'s
//! `(source, id)` dedup of D334) and an in-memory checkpoint store with epoch
//! fencing.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::budget::Budget;
use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::delivery::{DeadLetter, DeadLetterQueue};
use crate::envelope::CloudEvent;
use crate::error::{CheckpointError, FabricError};
use crate::fabric::{FabricAck, FabricReader, FabricWriter};
use crate::position::{InstanceKey, Position};
use crate::source::{EventBatch, SourceBatch};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// One entry of the topic.
#[derive(Debug, Clone)]
pub enum Entry {
    Event(CloudEvent),
    Arrow(EventBatch),
}

#[derive(Debug, Default)]
struct FabricState {
    log: Vec<Entry>,
    seen: HashSet<(String, String)>,
    appends: u64,
    duplicates: u64,
    groups: HashMap<String, u64>,
    dlq: Vec<DeadLetter>,
    append_delay: Option<Duration>,
    failures: VecDeque<FabricError>,
}

/// The in-memory Fabric. Cheap to clone; clones share the topic.
#[derive(Debug, Clone)]
pub struct MemoryFabric {
    state: Arc<Mutex<FabricState>>,
    gate: Arc<watch::Sender<bool>>,
}

impl Default for MemoryFabric {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryFabric {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FabricState::default())),
            gate: Arc::new(watch::Sender::new(true)),
        }
    }

    /// Hold every `append` until [`MemoryFabric::release`]: the Fabric has
    /// the batch but has not acknowledged it.
    pub fn hold(&self) {
        self.gate.send_replace(false);
    }

    pub fn release(&self) {
        self.gate.send_replace(true);
    }

    /// Delay every `append` (a slow Fabric).
    pub fn set_append_delay(&self, d: Option<Duration>) {
        lock(&self.state).append_delay = d;
    }

    /// Fail the next `append`s with these errors, in order.
    pub fn fail_appends(&self, errors: impl IntoIterator<Item = FabricError>) {
        lock(&self.state).failures.extend(errors);
    }

    /// Put an event on the topic directly (a sink test's input).
    pub fn push(&self, ev: CloudEvent) {
        lock(&self.state).log.push(Entry::Event(ev));
    }

    /// Every event on the topic, in order (Arrow entries contribute their
    /// envelope).
    pub fn events(&self) -> Vec<CloudEvent> {
        lock(&self.state)
            .log
            .iter()
            .map(|e| match e {
                Entry::Event(ev) => ev.clone(),
                Entry::Arrow(EventBatch::Arrow { envelope, .. }) => envelope.clone(),
                Entry::Arrow(EventBatch::Events(_)) => unreachable!("stored as events"),
            })
            .collect()
    }

    pub fn ids(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.id).collect()
    }

    pub fn len(&self) -> usize {
        lock(&self.state).log.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `append` calls acknowledged so far.
    pub fn appends(&self) -> u64 {
        lock(&self.state).appends
    }

    /// Events dropped by the `(source, id)` dedup.
    pub fn duplicates(&self) -> u64 {
        lock(&self.state).duplicates
    }

    /// A consumer group's committed offset.
    pub fn committed(&self, group: &str) -> u64 {
        lock(&self.state).groups.get(group).copied().unwrap_or(0)
    }

    /// Move a group's committed offset (a crash that lost the Fabric commit).
    pub fn set_committed(&self, group: &str, offset: u64) {
        lock(&self.state).groups.insert(group.to_owned(), offset);
    }

    pub fn dead_letters(&self) -> Vec<DeadLetter> {
        lock(&self.state).dlq.clone()
    }

    /// A reader for consumer group `group`, starting at its committed offset.
    pub fn reader(&self, group: &str) -> MemoryFabricReader {
        let cursor = self.committed(group);
        MemoryFabricReader {
            fabric: self.clone(),
            group: group.to_owned(),
            cursor,
        }
    }
}

#[async_trait]
impl FabricWriter for MemoryFabric {
    async fn append(&self, b: &SourceBatch) -> Result<FabricAck, FabricError> {
        let delay = lock(&self.state).append_delay;
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }
        let mut gate = self.gate.subscribe();
        let _ = gate.wait_for(|open| *open).await;
        let mut s = lock(&self.state);
        if let Some(e) = s.failures.pop_front() {
            return Err(e);
        }
        s.appends += 1;
        let mut ack = FabricAck::default();
        let mut admit = |s: &mut FabricState, ev: &CloudEvent| {
            if s.seen.insert((ev.source.clone(), ev.id.clone())) {
                ack.accepted += 1;
                true
            } else {
                s.duplicates += 1;
                ack.duplicates += 1;
                false
            }
        };
        match &b.events {
            EventBatch::Events(v) => {
                for ev in v {
                    if admit(&mut s, ev) {
                        s.log.push(Entry::Event(ev.clone()));
                    }
                }
            }
            arrow @ EventBatch::Arrow { envelope, .. } => {
                if admit(&mut s, envelope) {
                    s.log.push(Entry::Arrow(arrow.clone()));
                }
            }
        }
        Ok(ack)
    }
}

#[async_trait]
impl DeadLetterQueue for MemoryFabric {
    async fn send(&self, dead: DeadLetter) -> Result<(), FabricError> {
        lock(&self.state).dlq.push(dead);
        Ok(())
    }
}

/// A consumer group's reader. Its positions are offsets (the index just
/// after the batch), encoded with [`Position::from_u64`].
#[derive(Debug)]
pub struct MemoryFabricReader {
    fabric: MemoryFabric,
    group: String,
    cursor: u64,
}

#[async_trait]
impl FabricReader for MemoryFabricReader {
    async fn poll(
        &mut self,
        budget: Budget,
    ) -> Result<Option<(EventBatch, Position)>, FabricError> {
        let s = lock(&self.fabric.state);
        let start = self.cursor as usize;
        if start >= s.log.len() {
            return Ok(None);
        }
        if let Entry::Arrow(batch) = &s.log[start] {
            self.cursor += 1;
            return Ok(Some((batch.clone(), Position::from_u64(self.cursor))));
        }
        let mut events = Vec::new();
        let mut bytes = 0;
        for entry in &s.log[start..] {
            let Entry::Event(ev) = entry else { break };
            let size = ev.size_bytes();
            if events.len() >= budget.max_rows || bytes + size > budget.max_bytes {
                break;
            }
            bytes += size;
            events.push(ev.clone());
        }
        if events.is_empty() {
            return Err(FabricError::fatal(format!(
                "the event at offset {start} is larger than the budget of {} bytes",
                budget.max_bytes
            )));
        }
        self.cursor += events.len() as u64;
        Ok(Some((
            EventBatch::Events(events),
            Position::from_u64(self.cursor),
        )))
    }

    async fn commit(&mut self, upto: &Position) -> Result<(), FabricError> {
        let offset = upto
            .as_u64()
            .ok_or_else(|| FabricError::fatal("not a memory-fabric position"))?;
        lock(&self.fabric.state)
            .groups
            .insert(self.group.clone(), offset);
        Ok(())
    }

    async fn seek(&mut self, to: &Position) -> Result<(), FabricError> {
        self.cursor = to
            .as_u64()
            .ok_or_else(|| FabricError::fatal("not a memory-fabric position"))?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct StoreState {
    checkpoints: BTreeMap<InstanceKey, Checkpoint>,
    saves: Vec<Checkpoint>,
    unavailable: u32,
}

/// An in-memory [`CheckpointStore`] with epoch fencing. Clones share state.
#[derive(Debug, Clone, Default)]
pub struct MemoryCheckpointStore {
    state: Arc<Mutex<StoreState>>,
}

impl MemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every successful save, in order.
    pub fn saves(&self) -> Vec<Checkpoint> {
        lock(&self.state).saves.clone()
    }

    pub fn get(&self, i: &InstanceKey) -> Option<Checkpoint> {
        lock(&self.state).checkpoints.get(i).cloned()
    }

    /// Fail the next `n` saves with `Unavailable`.
    pub fn fail_saves(&self, n: u32) {
        lock(&self.state).unavailable = n;
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn load(&self, i: &InstanceKey) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(lock(&self.state).checkpoints.get(i).cloned())
    }

    async fn save(&self, cp: &Checkpoint) -> Result<(), CheckpointError> {
        let mut s = lock(&self.state);
        if s.unavailable > 0 {
            s.unavailable -= 1;
            return Err(CheckpointError::Unavailable("injected".to_owned()));
        }
        if let Some(stored) = s.checkpoints.get(&cp.instance)
            && cp.epoch < stored.epoch
        {
            return Err(CheckpointError::Fenced {
                stored: stored.epoch,
                attempted: cp.epoch,
            });
        }
        s.checkpoints.insert(cp.instance.clone(), cp.clone());
        s.saves.push(cp.clone());
        Ok(())
    }
}
