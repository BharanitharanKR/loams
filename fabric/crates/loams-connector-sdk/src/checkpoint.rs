//! Checkpoints, fenced by the lease epoch (OR-16, Review Focus 12).

use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::error::CheckpointError;
use crate::position::{InstanceKey, Position, SplitId};

/// The positions of an instance's splits, written under a lease `epoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub instance: InstanceKey,
    pub epoch: u64,
    pub splits: BTreeMap<SplitId, Position>,
}

impl Checkpoint {
    pub fn new(instance: InstanceKey, epoch: u64) -> Self {
        Self {
            instance,
            epoch,
            splits: BTreeMap::new(),
        }
    }
}

/// Where checkpoints live. The default store is CN1's
/// `_fabric.flow_positions` with an epoch column (loams-flow); tests use
/// [`crate::testing::memory::MemoryCheckpointStore`].
///
/// `save` replaces the instance's checkpoint and returns
/// `Err(CheckpointError::Fenced)` when `cp.epoch` is older than the stored
/// epoch: a stale supervisor can never overwrite its successor's positions.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, i: &InstanceKey) -> Result<Option<Checkpoint>, CheckpointError>;
    async fn save(&self, cp: &Checkpoint) -> Result<(), CheckpointError>;
}
