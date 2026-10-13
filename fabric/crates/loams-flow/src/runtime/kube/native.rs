//! Native connectors in the `loams-fabric flow` pods (OR-16).
//!
//! Each flow pod runs a [`NativeScheduler`]: every tick it renews its member
//! lease, then holds a fair share of the instance leases (the holder of an
//! instance's lease runs its enumerator and owns its instance checkpoint)
//! and a fair share of every instance's split leases (the holder of a
//! split's lease reads that split, checkpointing under
//! [`split_checkpoint_key`](super::lease::split_checkpoint_key) with the
//! split lease's epoch). A lost pod's leases expire and the survivors take
//! them, each with a new epoch, so a stale pod's writes are fenced.
//!
//! The Deployment's size follows the instances' budgets ([`desired_replicas`],
//! written by the fleet's leader as a `Scale`, R26.7).
//!
//! Starting and stopping the SDK pipelines for what a tick returns is
//! `NativeRuntime`'s (CN1 Task 3, R4.1, R26.1).

use std::collections::{BTreeMap, BTreeSet};

use loams_connector_sdk::{InstanceKey, SplitId};

use super::FleetConfig;
use super::lease::{
    Balancer, Lease, LeaseClient, LeaseError, instance_lease_key, member_lease_key, split_lease_key,
};

/// How far the `flow` Deployment scales, and what one pod holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowScale {
    pub min_replicas: u32,
    pub max_replicas: u32,
    /// The memory one pod gives native connectors.
    pub pod_memory_bytes: u64,
    pub max_instances_per_pod: u32,
}

impl From<&FleetConfig> for FlowScale {
    fn from(c: &FleetConfig) -> Self {
        Self {
            min_replicas: c.flow.min_replicas,
            max_replicas: c.flow.max_replicas,
            pod_memory_bytes: c.flow.pod_memory_bytes,
            max_instances_per_pod: c.flow.max_instances_per_pod,
        }
    }
}

/// The `flow` pods the native fleet needs: enough for the instances' summed
/// memory budgets (CN2 Task 4's `memory_budget_bytes`, per instance; Task 27
/// sets them per connector) and for their count, within the configured
/// bounds.
pub fn desired_replicas(scale: &FlowScale, instance_budgets: &[u64]) -> u32 {
    let total: u64 = instance_budgets.iter().sum();
    let by_memory = total.div_ceil(scale.pod_memory_bytes.max(1));
    let by_count =
        (instance_budgets.len() as u64).div_ceil(u64::from(scale.max_instances_per_pod.max(1)));
    let want = u32::try_from(by_memory.max(by_count)).unwrap_or(u32::MAX);
    want.clamp(
        scale.min_replicas,
        scale.max_replicas.max(scale.min_replicas),
    )
}

/// One flow pod's share of the native fleet.
#[derive(Debug)]
pub struct NativeScheduler {
    client: LeaseClient,
    member: Option<Lease>,
    instances: Balancer,
    splits: Balancer,
    instance_index: BTreeMap<String, InstanceKey>,
    split_index: BTreeMap<String, (InstanceKey, SplitId)>,
}

impl NativeScheduler {
    pub fn new(client: LeaseClient) -> Self {
        Self {
            client,
            member: None,
            instances: Balancer::new(),
            splits: Balancer::new(),
            instance_index: BTreeMap::new(),
            split_index: BTreeMap::new(),
        }
    }

    pub fn holder(&self) -> &str {
        self.client.holder()
    }

    /// One scheduling round over the native `instances` and each one's
    /// current `splits` (as its enumerator last published them).
    pub async fn tick(
        &mut self,
        instances: &[InstanceKey],
        splits: &BTreeMap<InstanceKey, Vec<SplitId>>,
        now_ms: u64,
    ) -> Result<(), LeaseError> {
        self.heartbeat(now_ms).await?;
        let members = self
            .client
            .live_holders("member/", now_ms)
            .await?
            .len()
            .max(1);

        self.instance_index = instances
            .iter()
            .map(|i| (instance_lease_key(i), i.clone()))
            .collect();
        let keys: BTreeSet<String> = self.instance_index.keys().cloned().collect();
        self.instances
            .balance(&self.client, &keys, members, now_ms)
            .await?;

        self.split_index = splits
            .iter()
            .filter(|(i, _)| self.instance_index.contains_key(&instance_lease_key(i)))
            .flat_map(|(i, list)| {
                list.iter()
                    .map(move |s| (split_lease_key(i, s), (i.clone(), s.clone())))
            })
            .collect();
        let keys: BTreeSet<String> = self.split_index.keys().cloned().collect();
        self.splits
            .balance(&self.client, &keys, members, now_ms)
            .await
    }

    async fn heartbeat(&mut self, now_ms: u64) -> Result<(), LeaseError> {
        let key = member_lease_key(self.client.holder());
        if let Some(m) = &self.member {
            match self.client.renew(m, now_ms).await {
                Ok(l) => {
                    self.member = Some(l);
                    return Ok(());
                }
                Err(LeaseError::Fenced { .. }) => self.member = None,
                Err(e) => return Err(e),
            }
        }
        self.member = self.client.acquire(&key, now_ms).await?;
        Ok(())
    }

    /// The epoch this pod runs `instance` under, while its lease is live.
    pub fn runs(&self, instance: &InstanceKey, now_ms: u64) -> Option<u64> {
        self.instances
            .held()
            .get(&instance_lease_key(instance))
            .filter(|l| l.is_live(now_ms))
            .map(|l| l.epoch)
    }

    /// The instances this pod runs, with their epochs.
    pub fn instances_run(&self, now_ms: u64) -> BTreeMap<InstanceKey, u64> {
        self.instances
            .held()
            .iter()
            .filter(|(_, l)| l.is_live(now_ms))
            .filter_map(|(k, l)| self.instance_index.get(k).map(|i| (i.clone(), l.epoch)))
            .collect()
    }

    /// The splits of `instance` this pod reads, while their leases are live.
    pub fn splits_held(&self, instance: &InstanceKey, now_ms: u64) -> Vec<SplitId> {
        self.splits
            .held()
            .iter()
            .filter(|(_, l)| l.is_live(now_ms))
            .filter_map(|(k, _)| self.split_index.get(k))
            .filter(|(i, _)| i == instance)
            .map(|(_, s)| s.clone())
            .collect()
    }

    /// The epoch of this pod's lease on a split.
    pub fn split_epoch(&self, instance: &InstanceKey, split: &SplitId) -> Option<u64> {
        self.splits
            .held()
            .get(&split_lease_key(instance, split))
            .map(|l| l.epoch)
    }
}
