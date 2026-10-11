//! One fenced owner per graph (GR1 Task 12; design §48 §7.1, D744, I5).
//!
//! * **The lease.** A graph's owner holds the metastore lease `graph/<gr_id>/owner` (TTL 10 s,
//!   renewed every 3 s). Every append of its write lane carries the lease's epoch as a fence, and
//!   the metastore refuses a WAL commit whose fence is broken, atomically with the offset
//!   assignment, so two owners can never both commit (I5).
//! * **One epoch per engine.** An engine instance is opened under exactly one epoch, taken fresh
//!   (strictly above every epoch an earlier engine of the graph appended under) before it reads
//!   the stream. A reload after a failed or unknown append therefore takes a new epoch first, so a
//!   commit still in flight from the old engine is refused when it lands, instead of adding a
//!   record the rebuilt engine never read (R11.8's residual race).
//! * **Placement.** A graph whose lease is free goes to the rendezvous choice among the live nodes
//!   with the `graph` role; a node that receives a call for a graph another node owns forwards it
//!   there ([`Forwarder`]), and a forwarded call is never forwarded again.
//! * **Losing the lease.** A renewal refused (`LeaseLost`), or one that cannot reach the metastore
//!   before the lease's deadline, ends the ownership: the graph stops serving here and its engine
//!   is dropped ([`Ownership::set_on_lost`]).

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use connectrpc::ConnectError;
use loams_common::meta::{ApplyError, Consistency, Fence, MetaError, MetaStore};
use ulid::Ulid;

use crate::GraphId;
use crate::engine::GraphError;

/// How long an owner lease lasts without a renewal (§48 §7.1).
pub const OWNER_LEASE_TTL: Duration = Duration::from_secs(10);

/// How often an owner renews its lease (§48 §7.1).
pub const OWNER_RENEW_EVERY: Duration = Duration::from_secs(3);

/// The lease the cluster's graph maintenance (purging deleted graphs, sweeping catalog documents)
/// runs under, so one node does it at a time (Task 4 review M3).
pub const MAINTENANCE_LEASE: &str = "graph/maintenance";

/// The owner lease of `graph`: `graph/<gr_id>/owner`.
#[must_use]
pub fn owner_lease_key(graph: GraphId) -> String {
    format!("graph/{graph}/owner")
}

/// A node that can own graphs, as the ownership layer knows it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GraphNode {
    /// The node's id in the cluster (0 for a single node).
    pub node_id: u64,
    /// New on every process start, so a restarted node is another lease owner than its
    /// predecessor (a lease's owner string must be unique per incarnation).
    pub incarnation: Ulid,
    /// Where other nodes reach its internal routes (`host:port`).
    pub addr: String,
}

impl GraphNode {
    /// A node of its own: a single-node deployment, or a test.
    #[must_use]
    pub fn local() -> Self {
        Self {
            node_id: 0,
            incarnation: Ulid::generate(),
            addr: "local".to_string(),
        }
    }

    /// The owner string of its leases: `v1;<node_id>;<incarnation>;<addr>`.
    #[must_use]
    pub fn lease_owner(&self) -> String {
        format!("v1;{};{};{}", self.node_id, self.incarnation, self.addr)
    }

    /// The node an owner string names, if it is one [`GraphNode::lease_owner`] wrote.
    #[must_use]
    pub fn from_lease_owner(owner: &str) -> Option<Self> {
        let mut parts = owner.splitn(4, ';');
        let (Some("v1"), Some(node_id), Some(incarnation), Some(addr)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        Some(Self {
            node_id: node_id.parse().ok()?,
            incarnation: incarnation.parse().ok()?,
            addr: addr.to_string(),
        })
    }
}

/// Who the graph nodes are: the server's node registry, or a fixed set (tests, a single node).
pub trait Membership: Send + Sync + fmt::Debug {
    /// This node.
    fn me(&self) -> GraphNode;
    /// The live nodes with the `graph` role, this one included when it has the role.
    fn graph_nodes(&self) -> Vec<GraphNode>;
}

/// A membership that changes only when told to.
#[derive(Debug)]
pub struct FixedMembership {
    me: GraphNode,
    nodes: RwLock<Vec<GraphNode>>,
}

impl FixedMembership {
    /// `me` and the graph nodes `nodes` (which include `me` when it owns graphs).
    #[must_use]
    pub fn new(me: GraphNode, nodes: Vec<GraphNode>) -> Self {
        Self {
            me,
            nodes: RwLock::new(nodes),
        }
    }

    /// A node alone, owning every graph.
    #[must_use]
    pub fn single(me: GraphNode) -> Self {
        let nodes = vec![me.clone()];
        Self::new(me, nodes)
    }

    /// Replaces the graph nodes (a node joined or left).
    pub fn set(&self, nodes: Vec<GraphNode>) {
        *self.nodes.write().unwrap_or_else(PoisonError::into_inner) = nodes;
    }
}

impl Membership for FixedMembership {
    fn me(&self) -> GraphNode {
        self.me.clone()
    }

    fn graph_nodes(&self) -> Vec<GraphNode> {
        self.nodes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The rendezvous choice for `graph` among `nodes`: the highest score, ties to the lowest id
/// (§48 §7.1, §04 §5's affinity). Stable across processes.
#[must_use]
pub fn rendezvous(graph: GraphId, nodes: &[GraphNode]) -> Option<&GraphNode> {
    nodes
        .iter()
        .max_by_key(|node| (score(graph, node.node_id), std::cmp::Reverse(node.node_id)))
}

/// FNV-1a over the graph id and the node id, then a SplitMix64 finaliser so that close ids
/// spread.
fn score(graph: GraphId, node_id: u64) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in graph.0.to_bytes().into_iter().chain(node_id.to_be_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// The data-plane calls a node passes to a graph's owner (`GraphForward`, internal only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForwardMethod {
    /// `GraphService/Execute`.
    Execute,
    /// `GraphService/ExecuteBatch`.
    ExecuteBatch,
    /// `GraphService/ExecuteStream`: the answer is every chunk, length-delimited.
    ExecuteStream,
    /// `GraphService/Explain`.
    Explain,
    /// `GraphAdminService/GetSchema`.
    GetSchema,
}

impl ForwardMethod {
    /// Every method.
    pub const ALL: [Self; 5] = [
        Self::Execute,
        Self::ExecuteBatch,
        Self::ExecuteStream,
        Self::Explain,
        Self::GetSchema,
    ];

    /// Its name in the internal route (`/internal/v1/graph/<name>`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::ExecuteBatch => "execute-batch",
            Self::ExecuteStream => "execute-stream",
            Self::Explain => "explain",
            Self::GetSchema => "get-schema",
        }
    }

    /// The method named `name`.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|method| method.as_str() == name)
    }
}

/// What a forward answers: the owner's encoded answer, or its error.
pub type ForwardFuture = Pin<Box<dyn Future<Output = Result<Bytes, ConnectError>> + Send>>;

/// Passes a call to another node (the server: `POST /internal/v1/graph/<method>` with the
/// request's protobuf bytes; tests: in process).
pub trait Forwarder: Send + Sync + fmt::Debug {
    /// Calls `method` on node `to` with the encoded request, waiting at most `timeout`.
    fn forward(
        &self,
        to: &GraphNode,
        method: ForwardMethod,
        request: Bytes,
        timeout: Duration,
    ) -> ForwardFuture;
}

/// Lease timing (§48 §7.1 defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerConfig {
    /// How long a lease lasts without a renewal.
    pub ttl: Duration,
    /// How often a held lease is renewed.
    pub renew_every: Duration,
}

impl Default for OwnerConfig {
    fn default() -> Self {
        Self {
            ttl: OWNER_LEASE_TTL,
            renew_every: OWNER_RENEW_EVERY,
        }
    }
}

/// Where a graph's calls run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Here, under the owner lease at this epoch.
    Here(u64),
    /// On the node that holds the lease, or the one that should take it.
    There(GraphNode),
}

/// Called when this node stops owning a graph it held (its lease was taken or could not be
/// renewed in time), with the epoch it held.
pub type LostHook = Arc<dyn Fn(GraphId, u64) + Send + Sync>;

/// A lease this node holds.
struct Held {
    epoch: u64,
    /// The lease's deadline by the metastore clock.
    deadline_ms: u64,
    renewer: Option<tokio::task::AbortHandle>,
}

/// The graph leases of this node.
pub struct Ownership {
    meta: Arc<dyn MetaStore>,
    membership: Arc<dyn Membership>,
    forwarder: Option<Arc<dyn Forwarder>>,
    config: OwnerConfig,
    held: Mutex<HashMap<GraphId, Held>>,
    /// Lease takes run one at a time, so two calls for one graph never take two epochs.
    taking: tokio::sync::Mutex<()>,
    on_lost: Mutex<Option<LostHook>>,
    /// **Tests only:** renewals stop, as for a node cut off from the metastore.
    #[cfg(feature = "test-hooks")]
    renewals_paused: std::sync::atomic::AtomicBool,
}

impl fmt::Debug for Ownership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ownership")
            .field("me", &self.membership.me())
            .field("config", &self.config)
            .field("held", &self.held_graphs().len())
            .finish_non_exhaustive()
    }
}

/// The metastore is unavailable: details to the server log, a generic message to the client.
fn unavailable(what: &str, err: &MetaError) -> GraphError {
    tracing::warn!(error = %err, "{what} failed");
    GraphError::LogUnavailable(format!("{what} failed; retry"))
}

impl Ownership {
    /// The leases of a node in `membership`, forwarding through `forwarder` (`None`: this node
    /// never forwards, so it takes every graph it is asked for).
    #[must_use]
    pub fn new(
        meta: Arc<dyn MetaStore>,
        membership: Arc<dyn Membership>,
        forwarder: Option<Arc<dyn Forwarder>>,
        config: OwnerConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            meta,
            membership,
            forwarder,
            config,
            held: Mutex::new(HashMap::new()),
            taking: tokio::sync::Mutex::new(()),
            on_lost: Mutex::new(None),
            #[cfg(feature = "test-hooks")]
            renewals_paused: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// A single node's leases: it owns every graph, and still fences its appends.
    #[must_use]
    pub fn single(meta: Arc<dyn MetaStore>) -> Arc<Self> {
        Self::new(
            meta,
            Arc::new(FixedMembership::single(GraphNode::local())),
            None,
            OwnerConfig::default(),
        )
    }

    /// This node.
    #[must_use]
    pub fn me(&self) -> GraphNode {
        self.membership.me()
    }

    /// Whether this node owns graphs at all (it has the `graph` role).
    #[must_use]
    pub fn can_own(&self) -> bool {
        let me = self.me();
        self.membership
            .graph_nodes()
            .iter()
            .any(|node| node.node_id == me.node_id)
    }

    /// The forwarder, when this node forwards.
    #[must_use]
    pub fn forwarder(&self) -> Option<&Arc<dyn Forwarder>> {
        self.forwarder.as_ref()
    }

    /// The lease timing.
    #[must_use]
    pub fn config(&self) -> OwnerConfig {
        self.config
    }

    /// Sets what runs when this node loses a graph's lease.
    pub fn set_on_lost(&self, hook: Option<LostHook>) {
        *self.on_lost.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    fn held_map(&self) -> std::sync::MutexGuard<'_, HashMap<GraphId, Held>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The graphs whose leases this node holds, with their epochs.
    #[must_use]
    pub fn held_graphs(&self) -> Vec<(GraphId, u64)> {
        self.held_map()
            .iter()
            .map(|(graph, held)| (*graph, held.epoch))
            .collect()
    }

    /// The epoch of `graph`'s lease, if this node holds it and its deadline has not passed by
    /// this node's view of the metastore clock.
    #[must_use]
    pub fn held(&self, graph: GraphId) -> Option<u64> {
        let now = self.meta.now_ms();
        self.held_map()
            .get(&graph)
            .filter(|held| now < held.deadline_ms)
            .map(|held| held.epoch)
    }

    /// The fence of `graph`'s lease at `epoch`.
    #[must_use]
    pub fn fence(graph: GraphId, epoch: u64) -> Fence {
        Fence {
            lease: owner_lease_key(graph),
            epoch,
        }
    }

    /// Where `graph`'s calls run. A held lease is here. Otherwise the lease is read: a node that
    /// holds it is there; a free (or expired) lease goes to the rendezvous choice, which is this
    /// node when it is the choice, or when the call may not be forwarded (`may_forward` false: a
    /// forwarded call, or a node with no forwarder). Taking the lease takes a fresh epoch.
    ///
    /// # Errors
    ///
    /// [`GraphError::OwnerMoved`] when another node owns the graph and the call may not go there,
    /// or when no node can own it; [`GraphError::LogUnavailable`] when the metastore is.
    pub async fn place(
        self: &Arc<Self>,
        graph: GraphId,
        may_forward: bool,
    ) -> Result<Placement, GraphError> {
        if let Some(epoch) = self.held(graph) {
            return Ok(Placement::Here(epoch));
        }
        let forwards = may_forward && self.forwarder.is_some();
        if let Some(owner) = self.other_holder(graph).await? {
            return match owner {
                Some(node) if forwards => Ok(Placement::There(node)),
                other => Err(moved(graph, other.as_ref())),
            };
        }
        if forwards {
            let me = self.me();
            let nodes = self.membership.graph_nodes();
            match rendezvous(graph, &nodes) {
                Some(choice) if choice.node_id != me.node_id => {
                    return Ok(Placement::There(choice.clone()));
                }
                None => {
                    return Err(GraphError::OwnerMoved(
                        "no node runs the graph role; retry".to_string(),
                    ));
                }
                Some(_) => {}
            }
        }
        if !self.can_own() {
            return Err(GraphError::OwnerMoved(format!(
                "this node does not run the graph role and cannot serve graph {graph}"
            )));
        }
        self.take(graph, None).await
    }

    /// A fresh epoch for `graph`, whose engine at `stale` must not serve again (a reload after a
    /// failed or unknown append, or a panic): unless another call already took a newer one, the
    /// lease is given up and taken again, so every commit made under `stale` is refused from now
    /// on (R11.8's race). Answers where the graph's calls now run.
    ///
    /// # Errors
    ///
    /// As [`Ownership::place`].
    pub async fn refresh(
        self: &Arc<Self>,
        graph: GraphId,
        stale: u64,
    ) -> Result<Placement, GraphError> {
        self.take(graph, Some(stale)).await
    }

    /// Whether another owner than this process holds `graph`'s lease now, and which node, when
    /// its owner string names one.
    async fn other_holder(&self, graph: GraphId) -> Result<Option<Option<GraphNode>>, GraphError> {
        let key = owner_lease_key(graph);
        let lease = self
            .meta
            .lease(Consistency::Linearizable, &key)
            .await
            .map_err(|err| unavailable("reading a graph's owner lease", &err))?;
        let now = self.meta.now_ms();
        let mine = self.me().lease_owner();
        Ok(lease
            .filter(|lease| lease.is_held_at(now))
            .and_then(|lease| lease.owner)
            .filter(|owner| *owner != mine)
            .map(|owner| GraphNode::from_lease_owner(&owner)))
    }

    /// Takes `graph`'s lease at a fresh epoch: one this process never served under. With
    /// `stale`, a held epoch other than it is answered as it is (someone refreshed already).
    async fn take(
        self: &Arc<Self>,
        graph: GraphId,
        stale: Option<u64>,
    ) -> Result<Placement, GraphError> {
        let _taking = self.taking.lock().await;
        if let Some(epoch) = self.held(graph)
            && stale != Some(epoch)
        {
            return Ok(Placement::Here(epoch));
        }
        self.forget(graph, None);
        let key = owner_lease_key(graph);
        let owner = self.me().lease_owner();
        // Held by this process already (a lease it forgot, or the one being refreshed): give it
        // up first, so the take below bumps the epoch rather than extending it.
        let current = self
            .meta
            .lease(Consistency::Linearizable, &key)
            .await
            .map_err(|err| unavailable("reading a graph's owner lease", &err))?;
        if let Some(lease) = current
            && lease.owner.as_deref() == Some(owner.as_str())
        {
            match self.meta.release_lease(&key, &owner, lease.epoch).await {
                Ok(()) | Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {}
                Err(err) => return Err(unavailable("giving up a graph's owner lease", &err)),
            }
        }
        match self.meta.acquire_lease(&key, &owner, self.config.ttl).await {
            Ok(grant) => {
                let renewer = self.spawn_renewer(graph, grant.epoch);
                self.held_map().insert(
                    graph,
                    Held {
                        epoch: grant.epoch,
                        deadline_ms: grant.deadline_ms,
                        renewer,
                    },
                );
                tracing::info!(%graph, epoch = grant.epoch, "took a graph's owner lease");
                Ok(Placement::Here(grant.epoch))
            }
            Err(MetaError::Rejected(ApplyError::LeaseHeld { owner, .. })) => {
                match GraphNode::from_lease_owner(&owner) {
                    Some(node) if self.forwarder.is_some() => Ok(Placement::There(node)),
                    node => Err(moved(graph, node.as_ref())),
                }
            }
            Err(err) => Err(unavailable("taking a graph's owner lease", &err)),
        }
    }

    /// Forgets `graph`'s lease (at `epoch`, when given) without giving it up, and stops renewing
    /// it: it expires, or the next take here gives it up and takes it again.
    pub fn forget(&self, graph: GraphId, epoch: Option<u64>) {
        let mut held = self.held_map();
        if held
            .get(&graph)
            .is_some_and(|h| epoch.is_none_or(|e| e == h.epoch))
            && let Some(old) = held.remove(&graph)
            && let Some(renewer) = old.renewer
        {
            renewer.abort();
        }
    }

    /// Gives `graph`'s lease up (a graceful move: the node leaves, or the graph is closed here),
    /// so the next node takes it at once rather than after its TTL.
    pub async fn release(&self, graph: GraphId) {
        let epoch = self.held_map().get(&graph).map(|held| held.epoch);
        let Some(epoch) = epoch else { return };
        self.forget(graph, Some(epoch));
        let owner = self.me().lease_owner();
        if let Err(err) = self
            .meta
            .release_lease(&owner_lease_key(graph), &owner, epoch)
            .await
        {
            tracing::warn!(%graph, error = %err, "giving up a graph's owner lease failed; it expires");
        }
    }

    /// Gives every held lease up (shutdown).
    pub async fn release_all(&self) {
        for (graph, _) in self.held_graphs() {
            self.release(graph).await;
        }
    }

    /// Ends this node's ownership of `graph` at `epoch`: forgets the lease and calls the lost
    /// hook.
    fn lose(&self, graph: GraphId, epoch: u64) {
        let was_held = self
            .held_map()
            .get(&graph)
            .is_some_and(|held| held.epoch == epoch);
        if !was_held {
            return;
        }
        self.forget(graph, Some(epoch));
        tracing::warn!(%graph, epoch, "lost a graph's owner lease; the graph stops serving here");
        let hook = self
            .on_lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(graph, epoch);
        }
    }

    /// Renews the lease every `renew_every` until it is lost or forgotten.
    fn spawn_renewer(
        self: &Arc<Self>,
        graph: GraphId,
        epoch: u64,
    ) -> Option<tokio::task::AbortHandle> {
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let me: Weak<Self> = Arc::downgrade(self);
        let every = self.config.renew_every;
        let task = runtime.spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(this) = me.upgrade() else { return };
                if !this.renew_once(graph, epoch).await {
                    return;
                }
            }
        });
        Some(task.abort_handle())
    }

    /// One renewal; `false` once the ownership has ended.
    async fn renew_once(&self, graph: GraphId, epoch: u64) -> bool {
        let deadline = match self.held_map().get(&graph) {
            Some(held) if held.epoch == epoch => held.deadline_ms,
            _ => return false,
        };
        #[cfg(feature = "test-hooks")]
        let paused = self
            .renewals_paused
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(feature = "test-hooks"))]
        let paused = false;
        let renewed = if paused {
            Err(MetaError::Unavailable("renewals paused (test)".to_string()))
        } else {
            self.meta
                .renew_lease(
                    &owner_lease_key(graph),
                    &self.me().lease_owner(),
                    epoch,
                    self.config.ttl,
                )
                .await
        };
        match renewed {
            Ok(grant) => {
                if let Some(held) = self.held_map().get_mut(&graph)
                    && held.epoch == epoch
                {
                    held.deadline_ms = grant.deadline_ms;
                }
                true
            }
            Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {
                self.lose(graph, epoch);
                false
            }
            Err(err) => {
                if self.meta.now_ms() >= deadline {
                    tracing::warn!(%graph, error = %err, "could not renew a graph's owner lease before its deadline");
                    self.lose(graph, epoch);
                    return false;
                }
                true
            }
        }
    }

    /// **Tests only** (feature `test-hooks`): stops (or resumes) every renewal, as for a node cut
    /// off from the metastore; its leases then run out and it stops serving their graphs.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn pause_renewals(&self, paused: bool) {
        self.renewals_paused
            .store(paused, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for Ownership {
    fn drop(&mut self) {
        for held in self.held_map().values_mut() {
            if let Some(renewer) = held.renewer.take() {
                renewer.abort();
            }
        }
    }
}

/// Another node owns `graph` and the call cannot go there.
fn moved(graph: GraphId, owner: Option<&GraphNode>) -> GraphError {
    GraphError::OwnerMoved(match owner {
        Some(node) => format!(
            "graph {graph} is served by node {} now; retry",
            node.node_id
        ),
        None => format!("graph {graph} is served by another node now; retry"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_strings_round_trip() {
        let node = GraphNode {
            node_id: 7,
            incarnation: Ulid::from_parts(1, 2),
            addr: "10.0.0.7:7001".to_string(),
        };
        assert_eq!(GraphNode::from_lease_owner(&node.lease_owner()), Some(node));
        for bad in [
            "",
            "v2;1;01J0000000000000000000000;a",
            "v1;x;01J0000000000000000000000;a",
            "v1;1;nope;a",
        ] {
            assert_eq!(GraphNode::from_lease_owner(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn rendezvous_is_stable_and_spreads() {
        let nodes: Vec<GraphNode> = (1..=3)
            .map(|node_id| GraphNode {
                node_id,
                incarnation: Ulid::nil(),
                addr: String::new(),
            })
            .collect();
        let mut counts = [0usize; 3];
        for i in 0..300u64 {
            let graph = GraphId(Ulid::from_parts(1_700_000_000_000 + i, u128::from(i)));
            let choice = rendezvous(graph, &nodes).expect("a choice").node_id;
            // The same answer whatever the order of the nodes.
            let reversed: Vec<GraphNode> = nodes.iter().rev().cloned().collect();
            assert_eq!(
                rendezvous(graph, &reversed).map(|n| n.node_id),
                Some(choice)
            );
            counts[usize::try_from(choice - 1).expect("small")] += 1;
        }
        assert!(counts.iter().all(|&c| c > 50), "{counts:?}");
        assert_eq!(rendezvous(GraphId::new(), &[]), None);
    }

    #[test]
    fn forward_methods_round_trip() {
        for method in ForwardMethod::ALL {
            assert_eq!(ForwardMethod::parse(method.as_str()), Some(method));
        }
        assert_eq!(ForwardMethod::parse("delete"), None);
    }
}
