//! `GraphAdminService` over the graph catalog, and the data plane's entry point that opens a
//! catalog graph lazily (GR1 Task 4).
//!
//! The catalog ([`GraphCatalog`]) is the source of truth for which graphs exist; the engine holds
//! the ones that are open. A graph is opened on its first statement (or `GetSchema`), from
//! `<data_dir>/graphs/<id>/`, so a restarted server serves every graph its catalog lists without
//! opening any of them up front.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use buffa::Message as _;
use buffa_types::google::protobuf::Timestamp;
use connectrpc::{ConnectError, ErrorCode};
use loams_proto::loams::graph::v1 as pb;
use loams_proto::loams::operations::v1 as ops;

use super::errors::{map_catalog, map_engine, refuse, refuse_with};
use super::{data, engine_info, stream};
use crate::catalog::{
    CatalogError, CatalogState, GraphCatalog, GraphLimits, GraphMeta, GraphMode, NewGraph,
};
use crate::consistency::{Consistency, DEFAULT_CONSISTENCY_WAIT};
use crate::durable::ConsistencyToken;
use crate::engine::{Closed, Engine, Graph, GraphState, OpenSpec};
use crate::evict::{EvictPolicy, EvictReason, Evicted, Evictor, Rehydration, Resident};
use crate::limits::{
    DEFAULT_NAMESPACE_DETACHED, DEFAULT_NAMESPACE_STATEMENTS, Detached, NamespaceSlots,
    StatementLimits, StatementPool, Watch, namespace_cap,
};
use crate::owner::{ForwardMethod, GraphNode, Ownership, Placement};
use crate::replica::Replicas;
use crate::snapshot::{
    CheckpointPolicy, Checkpointed, Checkpointer, ManifestStore, SnapshotKind, SnapshotSource,
};

/// How long a deleted graph's storage is kept before it is purged, by default.
pub const DEFAULT_RETENTION_HOLD: Duration = Duration::from_secs(24 * 3600);

/// How long [`GraphAdmin::shutdown`] waits for running statements, by default.
pub const DEFAULT_SHUTDOWN_WAIT: Duration = Duration::from_secs(10);

/// The largest statement cap [`GraphAdmin::with_statement_slots`] accepts.
///
/// Each slot can hold a pool worker with a 256 MiB stack reservation (review fix 1, M4: 4096
/// slots would reserve 1 TiB of address space).
pub const MAX_STATEMENT_SLOTS: usize = 256;

/// The most followers a graph may ask for (review M6).
pub const MAX_REPLICAS: u32 = 8;
/// The longest idempotency key accepted (review M6).
pub const MAX_IDEMPOTENCY_KEY: usize = 128;
/// The server's per-statement maximums (§48 §13.1, [`StatementLimits::MAXIMUM`]); a graph's
/// limits are capped to them.
pub const MAX_LIMITS: GraphLimits = GraphLimits {
    timeout_ms: StatementLimits::MAXIMUM.timeout.as_millis() as u32,
    max_rows: StatementLimits::MAXIMUM.max_rows,
    max_result_bytes: StatementLimits::MAXIMUM.max_result_bytes,
    memory_bytes: 64 << 30,
    max_path_hops: StatementLimits::MAXIMUM.max_path_hops,
};

/// The time a forwarded call may take beyond its statement's deadline: the hop to the owner and
/// back, and the owner's own open (GR1 Task 12).
pub const FORWARD_SLACK: Duration = Duration::from_secs(5);

/// How long one maintenance pass may hold the cluster's maintenance lease (GR1 Task 12).
pub const MAINTENANCE_LEASE_TTL: Duration = Duration::from_secs(120);

/// A graph's local cache directory younger than this is never swept (an open may be filling
/// it; GR1 Task 12).
pub const LOCAL_CACHE_GRACE: Duration = Duration::from_secs(600);

/// Pool workers beyond the statement slots, for opens that hold no slot (a direct
/// [`GraphAdmin::open`]).
const POOL_SPARE: usize = 2;

fn check_replicas(replicas: u32) -> Result<(), ConnectError> {
    if replicas > MAX_REPLICAS {
        return Err(refuse(
            ErrorCode::InvalidArgument,
            "invalid_argument",
            format!("replicas must be at most {MAX_REPLICAS}, got {replicas}"),
        ));
    }
    Ok(())
}

fn check_key(field: &str, key: &str) -> Result<Option<String>, ConnectError> {
    if key.len() > MAX_IDEMPOTENCY_KEY {
        return Err(refuse(
            ErrorCode::InvalidArgument,
            "invalid_argument",
            format!(
                "{field} must be at most {MAX_IDEMPOTENCY_KEY} bytes, got {}",
                key.len()
            ),
        ));
    }
    Ok((!key.is_empty()).then(|| key.to_string()))
}

/// The fields `UpdateGraph`'s mask may name.
const UPDATABLE: [&str; 3] = ["languages", "limits", "replicas"];

/// `GraphAdminService`, and the data plane over catalog graphs.
#[derive(Clone)]
pub struct GraphAdmin {
    engine: Arc<Engine>,
    catalog: GraphCatalog,
    retention_hold: Duration,
    /// The process-wide cap on graph statements running at once (review fix 1, I1). A permit is
    /// taken before the graph opens and released when the statement's work ends, so a statement
    /// whose client went away, or that ran past its deadline, still holds its slot.
    statements: Arc<tokio::sync::Semaphore>,
    statement_slots: usize,
    /// The threads statements and opens run on (GR1 Task 6), sized to the slots.
    pool: Arc<StatementPool>,
    /// The server's statement limits; a graph's own are applied on top (GR1 Task 6).
    limits: StatementLimits,
    /// Statements of one namespace at once (§48 §13.2).
    namespaces: Arc<NamespaceSlots>,
    /// The namespace cap asked for; [`namespace_cap`] keeps a reserve of the process slots
    /// from it (review fix 1, I1).
    namespace_configured: usize,
    /// Detached statements one namespace may have, across its graphs (review fix 1, I1).
    namespace_detached: usize,
    /// Statements running on past their deadline or their client (R0.8 (a)).
    detached: Arc<Detached>,
    /// The client's own deadline for the call this handle serves ([`GraphAdmin::for_call`]).
    client_deadline: Option<std::time::Instant>,
    /// Set when [`GraphAdmin::shutdown`] starts: from then on no graph opens and no statement
    /// starts; each answers `UNAVAILABLE` (review fix 1, I3).
    closed: Arc<AtomicBool>,
    /// Loams' log, where every graph's writes are durable (GR1 Task 11). `None` is GR1a's
    /// engine-only mode: graphs persist through Grafeo's own files, which is not durable (a
    /// write can be acknowledged and lost). The server always sets it; tests and the mock may not.
    log: Option<crate::durable::GraphLog>,
    /// The graph leases of this node (GR1 Task 12): a durable graph is served only by the node
    /// that holds its owner lease, and its appends carry the lease's epoch. Set with the log.
    ownership: Option<Arc<Ownership>>,
    /// Takes the graphs' snapshots, commits their manifests and trims their streams (GR1 Task
    /// 13). Set with the log: a graph with no log has no snapshots.
    checkpointer: Option<Checkpointer>,
    /// Builds, publishes and serves the graphs' adjacency indexes (GR1 Task 22). Set with the
    /// log: an index is built from a graph's stream.
    indexer: Option<crate::adjacency::Indexer>,
    /// Where an algorithm's write-back patches a LINKED graph's vertex documents (GR1 Task 22):
    /// the server's collections.
    documents: Option<Arc<dyn crate::retrieval::algos::DocumentPatcher>>,
    /// This call came from another node (`GraphForward`): it is served here or refused, never
    /// forwarded again.
    forwarded: bool,
    /// When open graphs are evicted (GR1 Task 14).
    evict_policy: EvictPolicy,
    /// The graphs this node evicted and its rehydrations, shared by every clone (GR1 Task 14).
    evictor: Arc<Evictor>,
    /// This node's followers (GR1 Task 15). Set with the log: only a durable graph has them.
    replicas: Option<Arc<Replicas>>,
    /// How long an `at_least` read waits for its replica (§48 §7.2; default 5 s).
    consistency_wait: Duration,
    /// When a replica read last found the catalog unavailable (GR1 Task 16).
    catalog_down: Arc<CatalogDown>,
    #[cfg(feature = "test-hooks")]
    after_open_hook: Arc<std::sync::Mutex<Option<crate::catalog::AckHook>>>,
}

impl std::fmt::Debug for GraphAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphAdmin")
            .field("engine", &self.engine)
            .field("catalog", &self.catalog)
            .field("retention_hold", &self.retention_hold)
            .field("statement_slots", &self.statement_slots)
            .field("limits", &self.limits)
            .field("namespace_statements", &self.namespaces.per_namespace())
            .finish_non_exhaustive()
    }
}

fn language_name(language: pb::QueryLanguage) -> &'static str {
    use pb::QueryLanguage as L;
    match language {
        L::Unspecified | L::Gql => "GQL",
        L::Cypher => "CYPHER",
        L::SqlPgq => "SQL_PGQ",
        L::Gremlin => "GREMLIN",
        L::Graphql => "GRAPHQL",
        L::Sparql => "SPARQL",
    }
}

fn language_value(name: &str) -> pb::QueryLanguage {
    use pb::QueryLanguage as L;
    match name {
        "CYPHER" => L::Cypher,
        "SQL_PGQ" => L::SqlPgq,
        "GREMLIN" => L::Gremlin,
        "GRAPHQL" => L::Graphql,
        "SPARQL" => L::Sparql,
        _ => L::Gql,
    }
}

/// The languages of a request, checked against this build and named.
fn languages(
    requested: &[buffa::EnumValue<pb::QueryLanguage>],
) -> Result<Vec<String>, ConnectError> {
    let mut out = Vec::new();
    for language in requested {
        data::check_language(language.as_known(), language)?;
        let name = language.as_known().map_or("GQL", language_name).to_string();
        if !out.contains(&name) {
            out.push(name);
        }
    }
    Ok(out)
}

/// A request's graph limits, each capped to the server's maximum, as `GraphLimits` documents.
/// The timeout is capped to `timeout_ceiling` (the engine's own `query_timeout`, when lower than
/// 300 s), so the record says the value in force (review fix 1, M5).
fn limits_from(limits: Option<&pb::GraphLimits>, timeout_ceiling: Duration) -> GraphLimits {
    let ceiling_ms = u32::try_from(timeout_ceiling.as_millis()).unwrap_or(u32::MAX);
    limits.map_or_else(GraphLimits::default, |l| GraphLimits {
        timeout_ms: l.timeout_ms.min(MAX_LIMITS.timeout_ms).min(ceiling_ms),
        max_rows: l.max_rows.min(MAX_LIMITS.max_rows),
        max_result_bytes: l.max_result_bytes.min(MAX_LIMITS.max_result_bytes),
        memory_bytes: l.memory_bytes.min(MAX_LIMITS.memory_bytes),
        max_path_hops: l.max_path_hops.min(MAX_LIMITS.max_path_hops),
    })
}

fn limits_to(limits: &GraphLimits) -> pb::GraphLimits {
    pb::GraphLimits {
        timeout_ms: limits.timeout_ms,
        max_rows: limits.max_rows,
        max_result_bytes: limits.max_result_bytes,
        memory_bytes: limits.memory_bytes,
        max_path_hops: limits.max_path_hops,
        ..Default::default()
    }
}

/// Runs blocking engine work (a close, a purge) on the blocking pool, off the async runtime
/// (review M8). Opens and statements run on the statement pool ([`GraphAdmin::on_pool`]).
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ConnectError> + Send + 'static,
) -> Result<T, ConnectError> {
    tokio::task::spawn_blocking(work)
        .await
        .unwrap_or_else(|err| {
            tracing::error!(error = %err, "a blocking graph task failed");
            Err(refuse(
                ErrorCode::Internal,
                "internal",
                "internal error running the statement",
            ))
        })
}

/// The default cap on graph statements running at once: twice the cores, at most 32.
#[must_use]
pub fn default_statement_slots() -> usize {
    std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .saturating_mul(2)
        .min(32)
}

/// `UNAVAILABLE` once [`GraphAdmin::shutdown`] has started.
fn shutting_down() -> ConnectError {
    refuse(
        ErrorCode::Unavailable,
        "unavailable",
        "the graph service is shutting down",
    )
}

/// A short operation id: `op-` and 26 hex characters (D146).
fn operation_id() -> String {
    let hex = format!("{:032x}", u128::from(ulid::Ulid::generate()));
    format!("op-{}", &hex[..26])
}

impl GraphAdmin {
    /// Admin over `engine` and `catalog`, with the default retention hold.
    #[must_use]
    pub fn new(engine: Arc<Engine>, catalog: GraphCatalog) -> Self {
        let statement_slots = default_statement_slots();
        Self {
            engine,
            catalog,
            retention_hold: DEFAULT_RETENTION_HOLD,
            statements: Arc::new(tokio::sync::Semaphore::new(statement_slots)),
            statement_slots,
            pool: Arc::new(StatementPool::new(statement_slots + POOL_SPARE)),
            limits: StatementLimits::DEFAULT,
            namespaces: Arc::new(NamespaceSlots::new(namespace_cap(
                DEFAULT_NAMESPACE_STATEMENTS,
                statement_slots,
            ))),
            namespace_configured: DEFAULT_NAMESPACE_STATEMENTS,
            namespace_detached: DEFAULT_NAMESPACE_DETACHED,
            detached: Arc::default(),
            client_deadline: None,
            closed: Arc::default(),
            log: None,
            ownership: None,
            checkpointer: None,
            indexer: None,
            documents: None,
            forwarded: false,
            evict_policy: EvictPolicy::default(),
            evictor: Arc::default(),
            replicas: None,
            consistency_wait: DEFAULT_CONSISTENCY_WAIT,
            catalog_down: Arc::default(),
            #[cfg(feature = "test-hooks")]
            after_open_hook: Arc::default(),
        }
    }

    /// The same admin with every graph durable in `log` (GR1 Task 11): a graph opens from its
    /// stream, and a write is acknowledged once its change set is appended there.
    #[must_use]
    pub fn with_log(mut self, log: crate::durable::GraphLog) -> Self {
        let ownership = Ownership::single(log.meta().clone());
        let manifests = ManifestStore::new(log.meta().clone(), self.catalog.store().clone());
        self.indexer = Some(crate::adjacency::Indexer::new(
            manifests.clone(),
            crate::adjacency::AdjacencyConfig::default(),
            self.engine.data_dir().map(std::path::Path::to_path_buf),
        ));
        self.checkpointer = Some(Checkpointer::new(manifests, CheckpointPolicy::default()));
        self.log = Some(log);
        self.replicas = Some(Arc::new(Replicas::new(&self.engine)));
        self.with_ownership(ownership)
    }

    /// The same admin with another wait for `at_least` reads (GR1 Task 15; §48 §7.2's
    /// `consistency_wait_ms`, default 5 s). Zero answers at once.
    #[must_use]
    pub fn with_consistency_wait(mut self, wait: Duration) -> Self {
        self.consistency_wait = wait;
        self
    }

    /// How long an `at_least` read waits for its replica.
    #[must_use]
    pub fn consistency_wait(&self) -> Duration {
        self.consistency_wait
    }

    /// This node's followers, when graphs are durable (GR1 Task 15).
    #[must_use]
    pub fn followers(&self) -> Option<&Arc<Replicas>> {
        self.replicas.as_ref()
    }

    /// The same admin with another checkpoint policy (GR1 Task 13). Only after
    /// [`GraphAdmin::with_log`].
    #[must_use]
    pub fn with_checkpoint_policy(mut self, policy: CheckpointPolicy) -> Self {
        if let Some(checkpointer) = &self.checkpointer {
            self.checkpointer = Some(Checkpointer::new(checkpointer.manifests().clone(), policy));
        }
        self
    }

    /// The checkpointer, when graphs are durable.
    #[must_use]
    pub fn checkpointer(&self) -> Option<&Checkpointer> {
        self.checkpointer.as_ref()
    }

    /// The same admin with another adjacency index configuration (GR1 Task 22). Only after
    /// [`GraphAdmin::with_log`]; the indexer's counters and cache are kept.
    #[must_use]
    pub fn with_adjacency_config(mut self, config: crate::adjacency::AdjacencyConfig) -> Self {
        self.indexer = self.indexer.map(|indexer| indexer.with_config(config));
        self
    }

    /// The adjacency indexer, when graphs are durable (GR1 Task 22).
    #[must_use]
    pub fn indexer(&self) -> Option<&crate::adjacency::Indexer> {
        self.indexer.as_ref()
    }

    /// The same admin writing algorithm results back into LINKED graphs' vertex documents
    /// through `documents` (GR1 Task 22).
    #[must_use]
    pub fn with_documents(
        mut self,
        documents: Arc<dyn crate::retrieval::algos::DocumentPatcher>,
    ) -> Self {
        self.documents = Some(documents);
        self
    }

    /// Where write-backs patch LINKED graphs' documents, when set.
    #[must_use]
    pub fn documents(&self) -> Option<&Arc<dyn crate::retrieval::algos::DocumentPatcher>> {
        self.documents.as_ref()
    }

    /// The same admin with another eviction policy (GR1 Task 14). Its clones share what was
    /// evicted and rehydrated.
    #[must_use]
    pub fn with_evict_policy(mut self, policy: EvictPolicy) -> Self {
        self.evict_policy = policy;
        self
    }

    /// The eviction policy.
    #[must_use]
    pub fn evict_policy(&self) -> EvictPolicy {
        self.evict_policy
    }

    /// The first statements on evicted graphs, newest last (§17.2's cold gate; GR1 Task 14).
    #[must_use]
    pub fn rehydrations(&self) -> Vec<Rehydration> {
        self.evictor.rehydrations()
    }

    /// The same admin with another node's view of graph ownership (GR1 Task 12): the cluster's
    /// graph nodes and the forwarder to reach them. Only after [`GraphAdmin::with_log`]: a graph
    /// with no log has no owner.
    #[must_use]
    pub fn with_ownership(mut self, ownership: Arc<Ownership>) -> Self {
        // Losing a lease stops the graph here at once: its engine is fenced (no statement and no
        // append runs on it again) and dropped as soon as nobody holds it.
        let engine = Arc::clone(&self.engine);
        ownership.set_on_lost(Some(Arc::new(move |graph, epoch| {
            let engine = Arc::clone(&engine);
            let drop_engine = move || fence_open(&engine, graph, Some(epoch));
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(drop_engine);
            } else {
                drop_engine();
            }
        })));
        self.ownership = Some(ownership);
        self
    }

    /// The graph leases of this node, when its graphs are durable.
    #[must_use]
    pub fn ownership(&self) -> Option<&Arc<Ownership>> {
        self.ownership.as_ref()
    }

    /// The same admin with another retention hold.
    #[must_use]
    pub fn with_retention_hold(mut self, hold: Duration) -> Self {
        self.retention_hold = hold;
        self
    }

    /// The same admin with another cap on statements running at once (1 to
    /// [`MAX_STATEMENT_SLOTS`]).
    #[must_use]
    pub fn with_statement_slots(mut self, slots: usize) -> Self {
        let slots = slots.clamp(1, MAX_STATEMENT_SLOTS);
        self.statements = Arc::new(tokio::sync::Semaphore::new(slots));
        self.statement_slots = slots;
        self.pool = Arc::new(StatementPool::new(slots + POOL_SPARE));
        self.namespaces = Arc::new(NamespaceSlots::new(namespace_cap(
            self.namespace_configured,
            slots,
        )));
        self
    }

    /// The same admin with other server statement limits (each capped at
    /// [`StatementLimits::MAXIMUM`]); a graph's own limits apply on top.
    #[must_use]
    pub fn with_limits(mut self, limits: StatementLimits) -> Self {
        self.limits = limits.capped();
        self
    }

    /// The server's statement limits.
    #[must_use]
    pub fn limits(&self) -> StatementLimits {
        self.limits
    }

    /// The same admin with another cap on one namespace's statements at once (§48 §13.2;
    /// default [`DEFAULT_NAMESPACE_STATEMENTS`]). The cap in force is never more than the
    /// process slots less a reserve ([`namespace_cap`]), so one namespace cannot take every slot.
    #[must_use]
    pub fn with_namespace_statements(mut self, per_namespace: usize) -> Self {
        self.namespace_configured = per_namespace;
        self.namespaces = Arc::new(NamespaceSlots::new(namespace_cap(
            per_namespace,
            self.statement_slots,
        )));
        self
    }

    /// The same admin with another cap on one namespace's detached statements, across its
    /// graphs (default [`DEFAULT_NAMESPACE_DETACHED`], at least 1).
    #[must_use]
    pub fn with_namespace_detached(mut self, per_namespace: usize) -> Self {
        self.namespace_detached = per_namespace.max(1);
        self
    }

    /// Namespaces with a statement running now (review fix 1, M6: an idle or deleted namespace
    /// keeps no entry).
    #[must_use]
    pub fn namespaces_in_use(&self) -> usize {
        self.namespaces.namespaces()
    }

    /// The cap on one namespace's statements at once, as in force.
    #[must_use]
    pub fn namespace_statements(&self) -> usize {
        self.namespaces.per_namespace()
    }

    /// This admin for one call whose client set its own deadline (Connect's timeout header):
    /// the call's statement ends at the earlier of that and its own limit.
    #[must_use]
    pub fn for_call(&self, client_deadline: Option<std::time::Instant>) -> Self {
        Self {
            client_deadline,
            ..self.clone()
        }
    }

    /// Statements running on past their deadline or their client, on every graph (R0.8 (a);
    /// Task 27's `loams_graph_detached_statements`).
    #[must_use]
    pub fn detached_statements(&self) -> usize {
        self.detached.total()
    }

    /// The largest deadline a request may ask for: [`StatementLimits::MAXIMUM`]'s, or the
    /// engine's own `query_timeout` when that is lower, since the engine may end the statement
    /// then anyway (R0.8 (a)).
    fn timeout_ceiling(&self) -> Duration {
        self.engine
            .query_timeout()
            .map_or(StatementLimits::MAXIMUM.timeout, |engine| {
                engine.min(StatementLimits::MAXIMUM.timeout)
            })
    }

    /// Runs `work` on the statement pool (GR1 Task 6): a statement, or a disk open.
    async fn on_pool<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> Result<T, ConnectError> + Send + 'static,
    ) -> Result<T, ConnectError> {
        match self.pool.run(work).await {
            Ok(Ok(result)) => result,
            _ => {
                tracing::error!("a graph statement failed on its worker");
                Err(refuse(
                    ErrorCode::Internal,
                    "internal",
                    "internal error running the statement",
                ))
            }
        }
    }

    /// The cap on statements running at once.
    #[must_use]
    pub fn statement_slots(&self) -> usize {
        self.statement_slots
    }

    /// Statements running now, including any whose client has gone away.
    #[must_use]
    pub fn statements_in_flight(&self) -> usize {
        self.statement_slots
            .saturating_sub(self.statements.available_permits())
    }

    /// A statement slot, or `RESOURCE_EXHAUSTED` when every one is taken: the caller retries,
    /// rather than queueing behind work that may never end (R0.8).
    fn statement_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit, ConnectError> {
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let slot = Arc::clone(&self.statements)
            .try_acquire_owned()
            .map_err(|err| match err {
                tokio::sync::TryAcquireError::NoPermits => refuse(
                    ErrorCode::ResourceExhausted,
                    "resource_exhausted",
                    format!(
                        "all {} graph statement slots on this server are busy; retry",
                        self.statement_slots
                    ),
                ),
                tokio::sync::TryAcquireError::Closed => shutting_down(),
            })?;
        // Shutdown may have started meanwhile; it waits for this slot, but need not.
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        Ok(slot)
    }

    /// Whether [`GraphAdmin::shutdown`] has started.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Runs `work` on a catalog graph within its limits (GR1 Task 6).
    ///
    /// 1. The graph's catalog record gives its limits, and the request's `timeout_ms` (or the
    ///    graph's timeout) and the client's own deadline give the statement's deadline, counted
    ///    from now.
    /// 2. It takes a slot of its namespace (`quota_exceeded` when none is free, §48 §13.2), is
    ///    refused when the graph already has `max_detached` statements running past their
    ///    deadline, and takes a process slot (`resource_exhausted`). Nothing queues.
    /// 3. The open and the work run as one spawned task on the statement pool that owns both
    ///    slots, so a client that goes away cannot free them early (fix round 2): they are freed
    ///    when the work ends.
    /// 4. At the deadline the RPC answers `graph_statement_timeout`. Grafeo cannot stop the
    ///    statement (R0.8), so it runs on, detached and counted, until it ends; a client that
    ///    disconnects detaches it the same way.
    async fn on_graph<T: Send + 'static>(
        &self,
        namespace: &str,
        name: &str,
        timeout_ms: u32,
        work: impl FnOnce(&Graph, &StatementLimits) -> Result<T, ConnectError> + Send + 'static,
    ) -> Result<T, ConnectError> {
        self.on_graph_serving(namespace, name, timeout_ms, Serve::default(), work)
            .await
    }

    /// [`GraphAdmin::on_graph`] on the engine `serve` names (GR1 Task 15).
    async fn on_graph_serving<T: Send + 'static>(
        &self,
        namespace: &str,
        name: &str,
        timeout_ms: u32,
        serve: Serve,
        work: impl FnOnce(&Graph, &StatementLimits) -> Result<T, ConnectError> + Send + 'static,
    ) -> Result<T, ConnectError> {
        self.on_graph_held(namespace, name, timeout_ms, serve, work)
            .await
            .map(|(result, _slots)| result)
    }

    /// [`GraphAdmin::on_graph`], answering the statement's slots with its result, so a caller
    /// that goes on using the result (a stream) keeps them (review fix 1, I4). The statement runs
    /// where `serve` says: the owner's engine or this node's follower, after waiting (within its
    /// deadline) for the replica to apply `serve.at_least` (GR1 Task 15).
    async fn on_graph_held<T: Send + 'static>(
        &self,
        namespace: &str,
        name: &str,
        timeout_ms: u32,
        serve: Serve,
        work: impl FnOnce(&Graph, &StatementLimits) -> Result<T, ConnectError> + Send + 'static,
    ) -> Result<(T, Slots), ConnectError> {
        let started = tokio::time::Instant::now();
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let meta = self.record(namespace, name, serve.follower).await?;
        let limits = self.limits.for_graph(&meta.limits);
        let timeout = limits.timeout_for(timeout_ms, self.timeout_ceiling());
        let mut deadline = started + timeout;
        if let Some(client) = self.client_deadline {
            deadline = deadline.min(tokio::time::Instant::from_std(client));
        }
        let namespace_slot = self.namespaces.try_acquire(namespace).ok_or_else(|| {
            refuse_with(
                ErrorCode::ResourceExhausted,
                "quota_exceeded",
                format!(
                    "namespace {namespace} already runs {} graph statements at once; retry",
                    self.namespaces.per_namespace()
                ),
                &[("quota", "concurrent_statements")],
            )
        })?;
        let namespace_detached = self.detached.of_namespace(namespace);
        if namespace_detached >= self.namespace_detached {
            return Err(refuse_with(
                ErrorCode::ResourceExhausted,
                "quota_exceeded",
                format!(
                    "namespace {namespace} has {namespace_detached} graph statements still \
                     running past their deadline; retry when they end"
                ),
                &[("quota", "detached_statements")],
            ));
        }
        let slot = self.statement_slot()?;
        // Once a statement of this graph is detached, every one in flight counts against
        // `max_detached` (review fix 1, I2).
        let watch = Watch::admit(
            Arc::clone(&self.detached),
            namespace,
            name,
            limits.max_detached,
        )
        .ok_or_else(|| {
            refuse(
                ErrorCode::ResourceExhausted,
                "resource_exhausted",
                format!(
                    "graph {namespace}/{name} has {} statements running past their deadline and \
                     {} more in flight, against a limit of {}; retry when they end",
                    self.detached.of(namespace, name),
                    self.detached.running(namespace, name),
                    limits.max_detached
                ),
            )
        })?;
        let task = {
            let admin = self.clone();
            let watch = watch.clone();
            tokio::spawn(async move {
                // Settles the detached count however the task ends.
                let _finished = Finished(watch);
                // Freed when the task ends, unless the result carries them on.
                let slots = Slots {
                    _process: slot,
                    _namespace: namespace_slot,
                };
                // The first statement on a graph this node evicted is a cold read (§17.2).
                let cold = meta
                    .graph_id()
                    .ok()
                    .filter(|id| !serve.follower && admin.evictor.is_evicted(*id));
                let graph = if serve.follower {
                    admin.open_follower(&meta).await?
                } else {
                    admin.open_meta(&meta).await?
                };
                let opened = started.elapsed();
                graph.touch();
                if let Some(token) = serve.at_least {
                    crate::consistency::wait_for(&graph, token, admin.consistency_wait)
                        .await
                        .map_err(map_engine)?;
                }
                let source = graph.recovery().map(|recovery| recovery.source);
                let result = admin
                    .on_pool(move || {
                        let result = work(&graph, &limits);
                        drop(graph);
                        result
                    })
                    .await?;
                if let Some(id) = cold {
                    admin.evictor.rehydrated(Rehydration {
                        graph: id,
                        source,
                        open: opened,
                        first_read: started.elapsed(),
                    });
                }
                Ok((result, slots))
            })
        };
        // Dropped when this call ends, however it ends: a call whose client went away (this
        // future dropped) or that timed out leaves the statement detached.
        let _waiting = Waiting(watch);
        match tokio::time::timeout_at(deadline, task).await {
            Ok(joined) => joined.unwrap_or_else(|err| {
                tracing::error!(error = %err, "a graph statement task failed");
                Err(refuse(
                    ErrorCode::Internal,
                    "internal",
                    "internal error running the statement",
                ))
            }),
            Err(_) => Err(refuse(
                ErrorCode::DeadlineExceeded,
                "graph_statement_timeout",
                format!(
                    "the statement ran past its {} ms deadline; it is detached and ends on its \
                     own (Loams cannot stop a running graph statement yet)",
                    deadline.saturating_duration_since(started).as_millis()
                ),
            )),
        }
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// The catalog.
    #[must_use]
    pub fn catalog(&self) -> &GraphCatalog {
        &self.catalog
    }

    /// Loams' log the graphs are durable in, when the admin has one.
    #[must_use]
    pub fn log(&self) -> Option<&crate::durable::GraphLog> {
        self.log.as_ref()
    }

    /// Opens catalog graph `meta` here, as its owner, for one of its links (GR1 Task 18).
    ///
    /// # Errors
    ///
    /// As [`GraphAdmin::open`]: `NOT_FOUND` for a deleted graph, `UNAVAILABLE` when another
    /// node owns it.
    pub(crate) async fn link_graph(&self, meta: &GraphMeta) -> Result<Arc<Graph>, ConnectError> {
        self.open_meta(meta).await
    }

    /// Commits a link's batch to `graph` on the statement pool (GR1 Task 18). The outer error is
    /// the pool's; the inner one the graph's.
    pub(crate) async fn link_commit(
        &self,
        graph: Arc<Graph>,
        job: crate::durable::lane::LinkJob,
    ) -> Result<Result<u64, crate::GraphError>, ConnectError> {
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        self.on_pool(move || Ok(graph.commit_link(job))).await
    }

    /// Fills a `Graph` message: the catalog record, with the engine's state and counts when the
    /// graph is open.
    fn message(&self, meta: &GraphMeta) -> pb::Graph {
        let open = self
            .open_graph(&meta.namespace, &meta.name)
            .filter(|g| g.id().is_some_and(|id| id.to_string() == meta.id));
        let state = match (meta.state, open.as_ref().map(|g| g.state())) {
            (CatalogState::Deleting { .. }, _) => pb::GraphState::Deleting,
            (_, Some(GraphState::Poisoned | GraphState::Reloading)) => pb::GraphState::Reloading,
            (_, Some(GraphState::Failed)) => pb::GraphState::Failed,
            (_, Some(GraphState::Ready)) => pb::GraphState::Ready,
            // Closed here to free memory: its next statement opens it again (GR1 Task 14).
            (_, None) if meta.graph_id().is_ok_and(|id| self.evictor.is_evicted(id)) => {
                pb::GraphState::Evicted
            }
            // Listed and not open: it is served, and opens on its next statement.
            (_, None) => pb::GraphState::Ready,
        };
        let stats = open.as_ref().map(|g| {
            let (nodes, edges) = g.counts();
            pb::GraphStats {
                nodes,
                edges,
                ..Default::default()
            }
        });
        pb::Graph {
            namespace: meta.namespace.clone(),
            name: meta.name.clone(),
            id: meta.id.clone(),
            mode: match meta.mode {
                GraphMode::Owned => pb::GraphMode::Owned,
                GraphMode::Linked => pb::GraphMode::Linked,
            }
            .into(),
            languages: meta
                .languages
                .iter()
                .map(|l| language_value(l).into())
                .collect(),
            limits: limits_to(&meta.limits).into(),
            replicas: meta.replicas,
            mapping: meta
                .mapping
                .as_ref()
                .map(crate::mapping::GraphMapping::to_proto)
                .into(),
            state: state.into(),
            stats: stats.into(),
            create_time: Timestamp {
                seconds: i64::try_from(meta.created_at_ms / 1000).unwrap_or(i64::MAX),
                nanos: i32::try_from((meta.created_at_ms % 1000) * 1_000_000).unwrap_or(0),
                ..Default::default()
            }
            .into(),
            version: meta.version,
            ..Default::default()
        }
    }

    /// The id of namespace `namespace`, which holds graph `name`: `NOT_FOUND` for the graph
    /// when the namespace is gone.
    pub async fn namespace_of(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<loams_common::NamespaceId, ConnectError> {
        self.catalog
            .namespace_id(namespace)
            .await
            .map_err(map_catalog)?
            .ok_or_else(|| {
                map_catalog(CatalogError::NotFound {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                })
            })
    }

    /// The engine's open graph under a name, if any.
    fn open_graph(&self, namespace: &str, name: &str) -> Option<Arc<Graph>> {
        self.engine
            .list(Some(namespace))
            .ok()?
            .into_iter()
            .find(|g| g.name() == name)
    }

    /// Opens a catalog graph for a statement: from its own storage, lazily, and never a stale
    /// graph of the same name that a delete could not close.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND`/`graph_not_found` when the catalog has no such graph, or the engine's error.
    pub async fn open(&self, namespace: &str, name: &str) -> Result<Arc<Graph>, ConnectError> {
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let meta = self
            .catalog
            .get_by_name(namespace, name)
            .await
            .map_err(map_catalog)?;
        self.open_meta(&meta).await
    }

    /// [`GraphAdmin::open`] for a catalog record the caller has just read.
    async fn open_meta(&self, meta: &GraphMeta) -> Result<Arc<Graph>, ConnectError> {
        let (namespace, name) = (meta.namespace.as_str(), meta.name.as_str());
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let graph = self.open_catalog_graph(meta).await?;
        // A shutdown that started while this graph opened may already have closed the others:
        // close this one too rather than leave it open (review fix 1, I3). A statement holds a
        // slot across its open, so shutdown waits for it; this covers any other caller.
        if self.is_shutting_down() {
            drop(graph);
            let (engine, ns, nm) = (
                Arc::clone(&self.engine),
                namespace.to_string(),
                name.to_string(),
            );
            let _ = blocking(move || {
                let _ = engine.close(&ns, &nm);
                Ok(())
            })
            .await;
            return Err(shutting_down());
        }
        Ok(graph)
    }

    /// [`GraphAdmin::open_meta`] without the shutdown checks. Opens run on the statement pool
    /// (Task 4 review M8, GR1 Task 6).
    async fn open_catalog_graph(&self, meta: &GraphMeta) -> Result<Arc<Graph>, ConnectError> {
        let (namespace, name) = (meta.namespace.as_str(), meta.name.as_str());
        // A deleted graph is never recovered, whatever record a caller read (Task 4 review M3).
        if meta.is_deleting() {
            return Err(map_catalog(CatalogError::NotFound {
                namespace: namespace.to_string(),
                name: name.to_string(),
            }));
        }
        let id = meta.graph_id().map_err(map_catalog)?;
        // The owner lease epoch this node serves the graph under (GR1 Task 12): a durable graph is
        // served only here, by the node holding its lease, and its engine only at that epoch.
        let owned = match (&self.log, &self.ownership) {
            (Some(_), Some(ownership)) => Some(Arc::clone(ownership)),
            _ => None,
        };
        let mut epoch = match &owned {
            Some(ownership) => Some(here(ownership.place(id, false).await, id)?),
            None => None,
        };
        // Already open with this id: the one up-to-date catalog read above (a pointer read; the
        // document is cached by pointer version) is the whole cost of a statement, and a delete
        // is effective for every statement that starts after `DeleteGraph` returns (re-review 2).
        if let Some(graph) = self
            .open_graph(namespace, name)
            .filter(|g| g.id() == Some(id))
        {
            // An adopted graph whose stream did not reach its record (R11.5): try again.
            if meta.stream.is_none()
                && let Some(stream) = graph.stream()
                && let Err(err) = self.catalog.set_stream(namespace, &meta.id, stream).await
            {
                tracing::warn!(%namespace, %name, error = %err, "recording the graph's stream failed; retried on its next statement");
            }
            if let (Some(ownership), Some(current)) = (&owned, epoch) {
                if !graph.needs_reopen() && graph.lease_epoch() == Some(current) {
                    return Ok(graph);
                }
                // A reload (a failed or unknown append, a refused capture, a panic) under the
                // epoch this engine served: take a fresh one before the stream is read again, so
                // a commit of the old engine still in flight is refused when it lands (R11.8).
                if graph.lease_epoch() == Some(current) {
                    epoch = Some(here(ownership.refresh(id, current).await, id)?);
                }
                drop(graph);
                let (engine, ns, nm) = (
                    Arc::clone(&self.engine),
                    namespace.to_string(),
                    name.to_string(),
                );
                self.on_pool(move || engine.discard(&ns, &nm).map_err(map_engine))
                    .await?;
                return self.open_durable(meta, id, epoch).await;
            }
            if !graph.is_poisoned_or_failed() {
                return Ok(graph);
            }
            let engine = Arc::clone(&self.engine);
            return self
                .on_pool(move || engine.reopen_if_poisoned(graph).map_err(map_engine))
                .await;
        }
        self.open_durable(meta, id, epoch).await
    }

    /// Opens catalog graph `meta` (not open here): durable under its owner lease at `epoch`
    /// when the admin has a log.
    async fn open_durable(
        &self,
        meta: &GraphMeta,
        id: crate::GraphId,
        epoch: Option<u64>,
    ) -> Result<Arc<Graph>, ConnectError> {
        let (namespace, name) = (meta.namespace.as_str(), meta.name.as_str());
        // A durable graph opens from its stream, created with it (R11.2). A graph from before the
        // log has none yet: its local files are adopted into the new stream (R11.5). The stream
        // is ensured on every open: one of GR1 Task 11's name (`graph.<gr_id>`) moves to the
        // reserved `_graph.<gr_id>` in place, idempotently (GR1 Task 12).
        let durable = match &self.log {
            None => None,
            Some(log) => {
                let ensured = self
                    .catalog
                    .ensure_stream(meta)
                    .await
                    .map_err(map_catalog)?;
                let (stream, adopt) = match meta.stream {
                    Some(stream) if stream == ensured => (stream, false),
                    Some(stream) => {
                        tracing::error!(graph = %meta.id, recorded = %stream, %ensured, "a graph's catalog record names another stream than its own");
                        return Err(map_catalog(CatalogError::Corrupt(format!(
                            "graph {} records stream {stream}, not its stream {ensured}",
                            meta.id
                        ))));
                    }
                    None => (ensured, true),
                };
                // A trimmed stream opens from a snapshot (GR1 Task 13).
                let snapshots = self.snapshot_source(namespace).await?;
                Some(crate::durable::DurableSpec {
                    log: log.clone(),
                    stream,
                    adopt,
                    fence: epoch.map(|epoch| Ownership::fence(id, epoch)),
                    snapshots,
                })
            }
        };
        for _ in 0..2 {
            let graph = {
                let linked = meta.mode == GraphMode::Linked;
                let (engine, ns, nm, durable) = (
                    Arc::clone(&self.engine),
                    namespace.to_string(),
                    name.to_string(),
                    durable.clone(),
                );
                self.on_pool(move || {
                    Graph::open_or_existing(&engine, &ns, &nm, || {
                        let spec = OpenSpec::for_catalog(&engine, id).linked(linked);
                        match durable {
                            Some(durable) => spec.durable(durable),
                            None => spec,
                        }
                    })
                    .map_err(map_engine)
                })
                .await?
            };
            if graph.id() == Some(id) && graph.lease_epoch() == epoch {
                // Recorded only once the graph has opened (and an adopted one is in its
                // stream), so a crash in between adopts again rather than losing local files.
                if let Some(durable) = &durable
                    && durable.adopt
                    && let Err(err) = self
                        .catalog
                        .set_stream(namespace, &meta.id, durable.stream)
                        .await
                {
                    tracing::warn!(%namespace, %name, error = %err, "recording the graph's stream failed; retried on its next statement");
                }
                #[cfg(feature = "test-hooks")]
                self.after_open().await;
                // Re-check after the (possibly slow) open: a delete that landed meanwhile must
                // not leave this graph open and serving (review M1). Cheap: the document is
                // cached unless the pointer moved.
                let still = self.catalog.get_by_name(namespace, name).await;
                if !matches!(&still, Ok(now) if now.id == meta.id) {
                    drop(graph);
                    let (engine, ns, nm) = (
                        Arc::clone(&self.engine),
                        namespace.to_string(),
                        name.to_string(),
                    );
                    let _ = blocking(move || {
                        let _ = engine.close(&ns, &nm);
                        Ok(())
                    })
                    .await;
                    return Err(match still {
                        Err(err) => map_catalog(err),
                        Ok(_) => map_catalog(CatalogError::NotFound {
                            namespace: namespace.to_string(),
                            name: name.to_string(),
                        }),
                    });
                }
                // The owner serves every read here now: a follower of the graph goes (GR1 Task
                // 15).
                if let Some(replicas) = &self.replicas {
                    let (replicas, ns, nm) = (
                        Arc::clone(replicas),
                        namespace.to_string(),
                        name.to_string(),
                    );
                    let _ = blocking(move || Ok(replicas.close(&ns, &nm))).await;
                }
                let engine = Arc::clone(&self.engine);
                return self
                    .on_pool(move || engine.reopen_if_poisoned(graph).map_err(map_engine))
                    .await;
            }
            // An older graph of this name is still open: close it and open this one.
            drop(graph);
            let (engine, ns, nm) = (
                Arc::clone(&self.engine),
                namespace.to_string(),
                name.to_string(),
            );
            blocking(move || {
                engine.close(&ns, &nm).map(|_| ()).map_err(|err| {
                    refuse(ErrorCode::Unavailable, "graph_reloading", err.to_string())
                })
            })
            .await?;
        }
        Err(refuse(
            ErrorCode::Unavailable,
            "graph_reloading",
            format!("graph {namespace}/{name} is being replaced; retry"),
        ))
    }

    /// **Tests only** (feature `test-hooks`): runs between the engine open and the catalog
    /// re-check of [`GraphAdmin::open`] (review M1).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_after_open_hook(&self, hook: Option<crate::catalog::AckHook>) {
        if let Ok(mut slot) = self.after_open_hook.lock() {
            *slot = hook;
        }
    }

    #[cfg(feature = "test-hooks")]
    async fn after_open(&self) {
        let hook = self
            .after_open_hook
            .lock()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(hook) = hook {
            let _ = hook().await;
        }
    }

    /// `GetEngineInfo`.
    pub async fn get_engine_info(&self, _req: pb::GetEngineInfoRequest) -> pb::EngineInfo {
        engine_info(&self.engine)
    }

    /// `CreateGraph`: idempotent by `idempotency_key`; the name must be free.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a bad name, `UNIMPLEMENTED` for LINKED or a language this build
    /// lacks, `ALREADY_EXISTS`, or `UNAVAILABLE` when the catalog is.
    pub async fn create_graph(
        &self,
        req: pb::CreateGraphRequest,
    ) -> Result<pb::Graph, ConnectError> {
        crate::engine::validate_names(&req.namespace, &req.name).map_err(map_engine)?;
        check_replicas(req.replicas)?;
        let (mode, mapping) = self.mode_of(&req).await?;
        let new = NewGraph {
            mode,
            mapping,
            languages: languages(&req.languages)?,
            limits: limits_from(req.limits.as_option(), self.timeout_ceiling()),
            replicas: req.replicas,
            idempotency_key: check_key("idempotency_key", &req.idempotency_key)?,
            with_stream: self.log.is_some(),
        };
        let meta = self
            .catalog
            .create(&req.namespace, &req.name, new)
            .await
            .map_err(map_catalog)?;
        Ok(self.message(&meta))
    }

    /// A `CreateGraph`'s mode and mapping (GR1 Task 17): an OWNED graph has no mapping, and a
    /// LINKED one has a mapping valid on its own and against its collections' schemas.
    async fn mode_of(
        &self,
        req: &pb::CreateGraphRequest,
    ) -> Result<(GraphMode, Option<crate::mapping::GraphMapping>), ConnectError> {
        let invalid =
            |message: String| refuse(ErrorCode::InvalidArgument, "invalid_argument", message);
        let given = req.mapping.as_option();
        match req.mode.as_known() {
            Some(pb::GraphMode::Linked) => {}
            Some(pb::GraphMode::Unspecified | pb::GraphMode::Owned) => {
                if given.is_some_and(|m| !m.vertices.is_empty() || !m.edges.is_empty()) {
                    return Err(invalid(
                        "a mapping is for a LINKED graph; an OWNED graph is written with GQL"
                            .to_string(),
                    ));
                }
                return Ok((GraphMode::Owned, None));
            }
            // A wire number that names no mode is refused, not read as OWNED.
            None => {
                return Err(invalid(format!("mode {:?} is not a graph mode", req.mode)));
            }
        }
        let mapping = crate::mapping::GraphMapping::try_from_proto(
            given.ok_or_else(|| invalid("a LINKED graph needs a mapping".to_string()))?,
        )
        .map_err(|err| invalid(err.to_string()))?;
        mapping.check().map_err(|err| invalid(err.to_string()))?;
        // The collections are the namespace's, read now: a mapping is checked when it is
        // created, as an index is (a collection dropped later leaves its labels empty).
        let meta = self.catalog.meta();
        let namespace = self
            .catalog
            .namespace_id(&req.namespace)
            .await
            .map_err(map_catalog)?;
        let mut schemas = std::collections::HashMap::new();
        for collection in mapping
            .vertices
            .iter()
            .map(|v| v.collection.as_str())
            .chain(mapping.edges.iter().map(|e| e.collection.as_str()))
        {
            if schemas.contains_key(collection) {
                continue;
            }
            let found = match namespace {
                Some(namespace) => meta
                    .resolve_collection(
                        loams_common::meta::Consistency::Linearizable,
                        namespace,
                        collection,
                    )
                    .await
                    .map_err(|err| {
                        tracing::warn!(error = %err, "reading a LINKED graph's collections failed");
                        refuse(
                            ErrorCode::Unavailable,
                            "unavailable",
                            "the namespace's collections could not be read; retry",
                        )
                    })?,
                None => None,
            };
            schemas.insert(collection.to_string(), found.map(|c| c.schema));
        }
        mapping
            .check_against(|collection| schemas.get(collection).cloned().flatten())
            .map_err(|err| invalid(err.to_string()))?;
        // Its streams exist now (GR1 Task 19); one deleted later stops its link.
        for stream in &mapping.streams {
            let found = match namespace {
                Some(namespace) => meta
                    .stream_by_name(
                        loams_common::meta::Consistency::Linearizable,
                        namespace,
                        &stream.stream,
                    )
                    .await
                    .map_err(|err| {
                        tracing::warn!(error = %err, "reading a LINKED graph's streams failed");
                        refuse(
                            ErrorCode::Unavailable,
                            "unavailable",
                            "the namespace's streams could not be read; retry",
                        )
                    })?,
                None => None,
            };
            if found.is_none() {
                return Err(invalid(format!(
                    "no stream {:?} in the namespace",
                    stream.stream
                )));
            }
        }
        Ok((GraphMode::Linked, Some(mapping)))
    }

    /// `GetGraph`.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT`, `NOT_FOUND` or `UNAVAILABLE`.
    pub async fn get_graph(&self, req: pb::GetGraphRequest) -> Result<pb::Graph, ConnectError> {
        let meta = self
            .catalog
            .get_by_name(&req.namespace, &req.name)
            .await
            .map_err(map_catalog)?;
        Ok(self.message(&meta))
    }

    /// `ListGraphs` (AIP-158).
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a bad namespace, size or token, or `UNAVAILABLE`.
    pub async fn list_graphs(
        &self,
        req: pb::ListGraphsRequest,
    ) -> Result<pb::ListGraphsResponse, ConnectError> {
        let page = self
            .catalog
            .list(&req.namespace, req.page_size, &req.page_token)
            .await
            .map_err(map_catalog)?;
        Ok(pb::ListGraphsResponse {
            graphs: page.graphs.iter().map(|g| self.message(g)).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
            ..Default::default()
        })
    }

    /// `UpdateGraph`: the fields `update_mask` names (`languages`, `limits`, `replicas`; all
    /// three when the mask is empty), at `expected_version` when given.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for another field or a bad value, `NOT_FOUND`, `ABORTED` with
    /// `graph_catalog_version_mismatch`, or `UNAVAILABLE`.
    pub async fn update_graph(
        &self,
        req: pb::UpdateGraphRequest,
    ) -> Result<pb::Graph, ConnectError> {
        let graph = req.graph.as_option().cloned().unwrap_or_default();
        let mut paths: Vec<String> = req
            .update_mask
            .as_option()
            .map(|m| m.paths.clone())
            .unwrap_or_default();
        if paths.is_empty() {
            paths = UPDATABLE.iter().map(ToString::to_string).collect();
        }
        if let Some(bad) = paths.iter().find(|p| !UPDATABLE.contains(&p.as_str())) {
            return Err(refuse(
                ErrorCode::InvalidArgument,
                "invalid_argument",
                format!(
                    "update_mask path {bad:?} is not updatable; only languages, limits and replicas are"
                ),
            ));
        }
        if paths.iter().any(|p| p == "replicas") {
            check_replicas(graph.replicas)?;
        }
        check_key("idempotency_key", &req.idempotency_key)?;
        let new_languages = languages(&graph.languages)?;
        let new_limits = limits_from(graph.limits.as_option(), self.timeout_ceiling());
        let meta = self
            .catalog
            .update(
                &graph.namespace,
                &graph.name,
                req.expected_version,
                |meta| {
                    for path in &paths {
                        match path.as_str() {
                            "languages" => {
                                meta.languages = if new_languages.is_empty() {
                                    vec!["GQL".to_string()]
                                } else {
                                    new_languages.clone()
                                };
                            }
                            "limits" => meta.limits = new_limits,
                            "replicas" => meta.replicas = graph.replicas,
                            _ => return Err(CatalogError::Invalid(format!("path {path}"))),
                        }
                    }
                    Ok(())
                },
            )
            .await
            .map_err(map_catalog)?;
        Ok(self.message(&meta))
    }

    /// `DeleteGraph`: the graph leaves the catalog's listing and its engine is closed at once;
    /// its storage is purged by [`GraphAdmin::purge_expired`] after the retention hold. Answers
    /// the finished operation. A retry with the same `idempotency_key` answers it again.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT`, `NOT_FOUND` or `UNAVAILABLE`.
    pub async fn delete_graph(
        &self,
        req: pb::DeleteGraphRequest,
    ) -> Result<ops::Operation, ConnectError> {
        let key = check_key("idempotency_key", &req.idempotency_key)?;
        let meta = self
            .catalog
            .mark_deleting(&req.namespace, &req.name, key)
            .await
            .map_err(map_catalog)?;
        if let Some(open) = self.open_graph(&req.namespace, &req.name)
            && open.id().is_some_and(|id| id.to_string() == meta.id)
        {
            drop(open);
            // A holder still mid-statement keeps it open; the catalog no longer serves it, and
            // `open` replaces it if the name is created again.
            let (engine, ns, nm) = (
                Arc::clone(&self.engine),
                req.namespace.clone(),
                req.name.clone(),
            );
            let _ = blocking(move || {
                if let Err(err) = engine.close(&ns, &nm) {
                    tracing::warn!(namespace = %ns, name = %nm, error = %err, "a deleted graph is still in use; it closes when released");
                }
                Ok(())
            })
            .await;
        }
        // Another node may own it: its engine there closes on that node's next maintenance pass,
        // and every statement is refused from now on, since each reads the catalog first.
        if let (Some(ownership), Ok(id)) = (&self.ownership, meta.graph_id()) {
            ownership.release(id).await;
        }
        let mut operation = ops::Operation {
            id: operation_id(),
            kind: "graph.delete".to_string(),
            namespace: req.namespace.clone(),
            state: ops::OperationState::Succeeded.into(),
            ..Default::default()
        };
        operation.target.insert("graph".to_string(), req.name);
        operation.target.insert("graph_id".to_string(), meta.id);
        Ok(operation)
    }

    /// `GetSchema`: labels, edge types, property keys, counts and indexes. Opens the graph.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND`, or the engine's error.
    pub async fn get_schema(
        &self,
        req: pb::GetSchemaRequest,
    ) -> Result<pb::GraphSchema, ConnectError> {
        if let Some(owner) = self.owner_elsewhere(&req.namespace, &req.name).await? {
            return self
                .forward(&owner, ForwardMethod::GetSchema, &req, 0)
                .await;
        }
        let summary = self
            .on_graph(&req.namespace, &req.name, 0, |graph, _| {
                graph.schema().map_err(map_engine)
            })
            .await?;
        Ok(pb::GraphSchema {
            labels: summary
                .labels
                .into_iter()
                .map(|(label, count)| pb::LabelInfo {
                    label,
                    count,
                    ..Default::default()
                })
                .collect(),
            edge_types: summary
                .edge_types
                .into_iter()
                .map(|(r#type, count)| pb::EdgeTypeInfo {
                    r#type,
                    count,
                    ..Default::default()
                })
                .collect(),
            property_keys: summary.property_keys,
            indexes: summary
                .indexes
                .into_iter()
                .map(|(name, target, kind)| pb::IndexInfo {
                    name,
                    target,
                    kind,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    /// `Execute` on a catalog graph, opening it lazily.
    ///
    /// # Errors
    ///
    /// As [`data::execute`], and `NOT_FOUND` for a graph the catalog does not list.
    pub async fn execute(
        &self,
        req: pb::ExecuteRequest,
    ) -> Result<pb::ExecuteResponse, ConnectError> {
        let (namespace, name, timeout) = (req.namespace.clone(), req.graph.clone(), req.timeout_ms);
        let consistency =
            Consistency::from_proto(req.consistency.as_option()).map_err(map_engine)?;
        let reads = consistency.any_replica() && self.reads(&req.statement).await;
        let serve = match self.route(&namespace, &name, consistency, reads).await? {
            Route::There(owner) => {
                return self
                    .forward(&owner, ForwardMethod::Execute, &req, timeout)
                    .await;
            }
            Route::Here(serve) => serve,
        };
        self.on_graph_serving(&namespace, &name, timeout, serve, move |graph, limits| {
            data::execute_on(graph, req, limits)
        })
        .await
    }

    /// `ExecuteStream` on a catalog graph: every row of the statement, in chunks of at most
    /// 1 000 rows (or `chunk_rows`) and 1 MiB (GR1 Task 6). The request's `max_rows` caps the
    /// whole stream (0: no cap); the unary row and byte limits do not apply. The statement runs
    /// to its end, within its deadline, before the first chunk (R6.3).
    ///
    /// # Errors
    ///
    /// As [`GraphAdmin::execute`], before any chunk.
    pub async fn execute_stream(
        &self,
        req: pb::ExecuteStreamRequest,
    ) -> Result<connectrpc::ServiceStream<pb::ResultChunk>, ConnectError> {
        let request = req.request.as_option().cloned().unwrap_or_default();
        let (namespace, name, timeout) = (
            request.namespace.clone(),
            request.graph.clone(),
            request.timeout_ms,
        );
        let consistency =
            Consistency::from_proto(request.consistency.as_option()).map_err(map_engine)?;
        let reads = consistency.any_replica() && self.reads(&request.statement).await;
        let route = self.route(&namespace, &name, consistency, reads).await?;
        if let Route::There(owner) = route {
            let body = self
                .forward_bytes(
                    &owner,
                    ForwardMethod::ExecuteStream,
                    req.encode_to_bytes(),
                    timeout,
                )
                .await?;
            let chunks = decode_chunks(&body)?;
            return Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))));
        }
        let serve = match route {
            Route::Here(serve) => serve,
            Route::There(_) => Serve::default(),
        };
        let max_rows = (request.max_rows > 0).then_some(request.max_rows as usize);
        let ((result, max_bytes, token), slots) = self
            .on_graph_held(&namespace, &name, timeout, serve, move |graph, limits| {
                data::run_statement(graph, &request, limits, max_rows)
                    .map(|result| (result, limits.max_stream_bytes, graph.consistency_token()))
            })
            .await?;
        // The stream keeps the statement's slots until it ends or is dropped (I4).
        Ok(Box::pin(futures::stream::iter(
            stream::Chunks::new(result, req.chunk_rows, max_bytes)
                .with_token(token)
                .holding(slots),
        )))
    }

    /// `ExecuteBatch` on a catalog graph, opening it lazily.
    ///
    /// # Errors
    ///
    /// As [`data::execute_batch`].
    pub async fn execute_batch(
        &self,
        req: pb::ExecuteBatchRequest,
    ) -> Result<pb::ExecuteBatchResponse, ConnectError> {
        let (namespace, name, timeout) = (req.namespace.clone(), req.graph.clone(), req.timeout_ms);
        if let Some(owner) = self.owner_elsewhere(&namespace, &name).await? {
            return self
                .forward(&owner, ForwardMethod::ExecuteBatch, &req, timeout)
                .await;
        }
        self.on_graph(&namespace, &name, timeout, move |graph, limits| {
            data::execute_batch_on(graph, req, limits)
        })
        .await
    }

    /// `Explain` on a catalog graph.
    ///
    /// # Errors
    ///
    /// As [`data::explain`].
    pub async fn explain(&self, req: pb::ExplainRequest) -> Result<pb::Plan, ConnectError> {
        let (namespace, name, timeout) = (req.namespace.clone(), req.graph.clone(), req.timeout_ms);
        let consistency =
            Consistency::from_proto(req.consistency.as_option()).map_err(map_engine)?;
        // An EXPLAIN only plans; a PROFILE runs the statement, so it must read.
        let reads = consistency.any_replica() && (!req.profile || self.reads(&req.statement).await);
        let serve = match self.route(&namespace, &name, consistency, reads).await? {
            Route::There(owner) => {
                return self
                    .forward(&owner, ForwardMethod::Explain, &req, timeout)
                    .await;
            }
            Route::Here(serve) => serve,
        };
        self.on_graph_serving(&namespace, &name, timeout, serve, move |graph, limits| {
            data::explain_on(graph, req, limits)
        })
        .await
    }

    /// Whether `statement` only reads, as the server's gate classifies it: only a read may be
    /// served by a follower (GR1 Task 15). A statement the gate refuses goes to the owner, which
    /// answers the refusal. The gate parses the statement, so it runs off the async runtime.
    async fn reads(&self, statement: &str) -> bool {
        let (statement, limits) = (statement.to_string(), self.limits);
        blocking(move || {
            Ok(
                crate::classify::gate_verdict(&statement, pb::QueryLanguage::Gql, &limits)
                    .is_ok_and(|verdict| verdict.access == crate::Access::Read),
            )
        })
        .await
        .unwrap_or(false)
    }

    /// Where a data-plane call on `namespace/name` runs (GR1 Task 15; §48 §7.2).
    ///
    /// * A `strong` call, and any call that writes (`reads` false), is the owner's: here, or
    ///   forwarded ([`GraphAdmin::owner_elsewhere`]).
    /// * An `eventual` or `at_least` read is served by this node's follower when this node runs
    ///   one of the graph's followers ([`Ownership::follows`]) and the graph is durable (its
    ///   stream is recorded); otherwise by the owner, here or there. A forwarded read is served
    ///   by a follower here too: a follower never forwards. An `at_least` read waits for its
    ///   token on whichever replica serves it.
    async fn route(
        &self,
        namespace: &str,
        name: &str,
        consistency: Consistency,
        reads: bool,
    ) -> Result<Route, ConnectError> {
        let at_least = consistency.token().filter(|_| reads);
        let Some(ownership) = self.ownership.as_ref().filter(|_| self.log.is_some()) else {
            return Ok(Route::Here(Serve {
                follower: false,
                at_least,
            }));
        };
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let meta = self
            .record(namespace, name, consistency.any_replica() && reads)
            .await?;
        let id = meta.graph_id().map_err(map_catalog)?;
        if consistency.any_replica()
            && reads
            && self.replicas.is_some()
            && meta.stream.is_some()
            && !meta.is_deleting()
            && ownership.follows(id, meta.replicas)
        {
            return Ok(Route::Here(Serve {
                follower: true,
                at_least,
            }));
        }
        Ok(match self.owner_of(ownership, namespace, name, id).await? {
            Some(owner) => Route::There(owner),
            None => Route::Here(Serve {
                follower: false,
                at_least,
            }),
        })
    }

    /// The catalog record of graph `namespace`/`name`. A read a follower may serve takes the
    /// record this process last read when the catalog is unavailable, or was within
    /// [`CATALOG_RETRY_AFTER`] (`stale_ok`; GR1 Task 16):
    /// a follower serves what it has applied anyway, so a metastore outage stops writes and
    /// `strong` reads, not `eventual` ones.
    async fn record(
        &self,
        namespace: &str,
        name: &str,
        stale_ok: bool,
    ) -> Result<GraphMeta, ConnectError> {
        // A catalog that just failed is not waited for again (each metastore call retries for
        // seconds): a replica read takes the record it last read for a while.
        if stale_ok
            && self.catalog_down.recently()
            && let Some(meta) = self.catalog.last_read(namespace, name)
        {
            return Ok(meta);
        }
        match self.catalog.get_by_name(namespace, name).await {
            Ok(meta) => Ok(meta),
            Err(CatalogError::Unavailable(err)) if stale_ok => {
                self.catalog_down.mark();
                match self.catalog.last_read(namespace, name) {
                    Some(meta) => {
                        tracing::debug!(%namespace, %name, error = %err, "the graph catalog is unavailable; a follower read uses the record last read");
                        Ok(meta)
                    }
                    None => Err(map_catalog(CatalogError::Unavailable(err))),
                }
            }
            Err(err) => Err(map_catalog(err)),
        }
    }

    /// Opens this node's follower of catalog graph `meta`, or answers the one open (GR1 Task
    /// 15): in memory, from the manifest's newest usable snapshot and the stream after it (or
    /// the whole stream), with no lease, and tailed from then on. One already open and serving
    /// is answered without the metastore (GR1 Task 16).
    async fn open_follower(&self, meta: &GraphMeta) -> Result<Arc<Graph>, ConnectError> {
        let (namespace, name) = (meta.namespace.as_str(), meta.name.as_str());
        let (Some(replicas), Some(log)) = (self.replicas.clone(), self.log.clone()) else {
            return Err(refuse(
                ErrorCode::Unavailable,
                "graph_reloading",
                "this node runs no graph followers; retry",
            ));
        };
        if meta.is_deleting() {
            return Err(map_catalog(CatalogError::NotFound {
                namespace: namespace.to_string(),
                name: name.to_string(),
            }));
        }
        let id = meta.graph_id().map_err(map_catalog)?;
        if let Some(open) = replicas
            .engine()
            .list(Some(namespace))
            .ok()
            .and_then(|open| open.into_iter().find(|g| g.name() == name))
            .filter(|g| g.id() == Some(id) && !g.is_poisoned_or_failed())
        {
            replicas.follow(&open, &log);
            return Ok(open);
        }
        let stream = meta.stream.ok_or_else(|| {
            refuse(
                ErrorCode::Unavailable,
                "graph_reloading",
                format!("graph {namespace}/{name} has not opened yet; retry"),
            )
        })?;
        let durable = crate::durable::DurableSpec {
            log: log.clone(),
            stream,
            adopt: false,
            fence: None,
            snapshots: self.snapshot_source(namespace).await?,
        };
        let linked = meta.mode == GraphMode::Linked;
        let (engine, ns, nm) = (
            Arc::clone(replicas.engine()),
            namespace.to_string(),
            name.to_string(),
        );
        let graph = self
            .on_pool(move || {
                let spec = || {
                    OpenSpec::in_memory_for(id)
                        .follower()
                        .linked(linked)
                        .durable(durable.clone())
                };
                let mut graph =
                    Graph::open_or_existing(&engine, &ns, &nm, spec).map_err(map_engine)?;
                if graph.id() != Some(id) {
                    // The follower of an older graph of this name (deleted, then created again).
                    drop(graph);
                    engine.discard(&ns, &nm).map_err(map_engine)?;
                    graph = Graph::open_or_existing(&engine, &ns, &nm, spec).map_err(map_engine)?;
                }
                engine.reopen_if_poisoned(graph).map_err(map_engine)
            })
            .await?;
        replicas.follow(&graph, &log);
        Ok(graph)
    }

    /// Where a graph's snapshots are read from, when graphs are snapshotted (GR1 Task 13).
    async fn snapshot_source(
        &self,
        namespace: &str,
    ) -> Result<Option<SnapshotSource>, ConnectError> {
        let Some(checkpointer) = &self.checkpointer else {
            return Ok(None);
        };
        Ok(self
            .catalog
            .namespace_id(namespace)
            .await
            .map_err(map_catalog)?
            .map(|namespace| SnapshotSource {
                manifests: checkpointer.manifests().clone(),
                namespace,
            }))
    }

    /// Closes the followers this node should no longer run (GR1 Task 15): the graph was deleted
    /// or replaced, this node took its lease, it is placed elsewhere now (fewer `replicas`, or
    /// nodes joined), or it was not read for the eviction policy's `idle_after`. A follower a
    /// read holds is closed by a later pass. Answers how many it closed. Part of
    /// [`GraphAdmin::evict_due`].
    pub async fn retire_followers(&self) -> usize {
        let Some(replicas) = &self.replicas else {
            return 0;
        };
        let mut closed = 0;
        for follower in replicas.followers() {
            let (namespace, name, id, idle_for) = (
                follower.namespace().to_string(),
                follower.name().to_string(),
                follower.id(),
                follower.idle_for(),
            );
            drop(follower);
            let keep = match self.catalog.get_by_name(&namespace, &name).await {
                Ok(meta) => {
                    meta.graph_id().ok() == id
                        && !meta.is_deleting()
                        && idle_for < self.evict_policy.idle_after
                        && id.is_some_and(|id| {
                            self.ownership
                                .as_ref()
                                .is_some_and(|ownership| ownership.follows(id, meta.replicas))
                        })
                }
                Err(CatalogError::NotFound { .. }) => false,
                Err(err) => {
                    tracing::warn!(%namespace, %name, error = %err, "retiring followers: the graph's catalog record could not be read");
                    continue;
                }
            };
            if keep {
                continue;
            }
            let replicas = Arc::clone(replicas);
            if blocking(move || Ok(replicas.close(&namespace, &name)))
                .await
                .unwrap_or(false)
            {
                closed += 1;
            }
        }
        closed
    }

    /// The node a data-plane call on `namespace/name` must go to, or `None` to serve it here
    /// (GR1 Task 12). A durable graph is served only by the node holding its owner lease: a held
    /// lease is here; another node's is there; a free one goes to the rendezvous choice among
    /// the graph nodes, which takes it. A forwarded call is never sent on: it is served here or
    /// refused with `graph_reloading`, and the client retries.
    async fn owner_elsewhere(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<GraphNode>, ConnectError> {
        let Some(ownership) = self.ownership.as_ref().filter(|_| self.log.is_some()) else {
            return Ok(None);
        };
        if self.is_shutting_down() {
            return Err(shutting_down());
        }
        let meta = self
            .catalog
            .get_by_name(namespace, name)
            .await
            .map_err(map_catalog)?;
        let id = meta.graph_id().map_err(map_catalog)?;
        self.owner_of(ownership, namespace, name, id).await
    }

    /// [`GraphAdmin::owner_elsewhere`] for graph `id`, whose catalog record the caller has read.
    async fn owner_of(
        &self,
        ownership: &Arc<Ownership>,
        namespace: &str,
        name: &str,
        id: crate::GraphId,
    ) -> Result<Option<GraphNode>, ConnectError> {
        // An engine here whose owner lease moved on (an append its fence refused) is dropped,
        // and the graph placed again.
        if self
            .open_graph(namespace, name)
            .is_some_and(|g| g.id() == Some(id) && g.is_fenced())
        {
            ownership.forget(id, None);
            let engine = Arc::clone(&self.engine);
            let _ = blocking(move || {
                fence_open(&engine, id, None);
                Ok(())
            })
            .await;
        }
        match ownership
            .place(id, !self.forwarded)
            .await
            .map_err(map_engine)?
        {
            Placement::Here(_) => Ok(None),
            Placement::There(node) => Ok(Some(node)),
        }
    }

    /// Passes `request` to graph owner `node` and decodes its answer.
    async fn forward<Req: buffa::Message, Resp: buffa::Message>(
        &self,
        node: &GraphNode,
        method: ForwardMethod,
        request: &Req,
        timeout_ms: u32,
    ) -> Result<Resp, ConnectError> {
        let body = self
            .forward_bytes(node, method, request.encode_to_bytes(), timeout_ms)
            .await?;
        Resp::decode_from_slice(&body).map_err(|err| {
            tracing::error!(node = node.node_id, method = method.as_str(), error = %err, "a graph owner's answer did not decode");
            refuse(
                ErrorCode::Internal,
                "internal",
                "internal error forwarding the statement to the graph's owner",
            )
        })
    }

    /// Passes an encoded request to graph owner `node`, waiting at most the statement's
    /// deadline (its `timeout_ms`, or the server's, and the client's own) plus
    /// [`FORWARD_SLACK`] for the hop.
    async fn forward_bytes(
        &self,
        node: &GraphNode,
        method: ForwardMethod,
        body: bytes::Bytes,
        timeout_ms: u32,
    ) -> Result<bytes::Bytes, ConnectError> {
        let forwarder = self
            .ownership
            .as_ref()
            .and_then(|ownership| ownership.forwarder())
            .ok_or_else(|| {
                refuse(
                    ErrorCode::Unavailable,
                    "graph_reloading",
                    "the graph is served by another node, which this node cannot reach; retry",
                )
            })?;
        let mut timeout =
            self.limits.timeout_for(timeout_ms, self.timeout_ceiling()) + FORWARD_SLACK;
        if let Some(client) = self.client_deadline {
            timeout = timeout.min(client.saturating_duration_since(std::time::Instant::now()));
        }
        match tokio::time::timeout(timeout, forwarder.forward(node, method, body, timeout)).await {
            Ok(answer) => answer,
            Err(_) => Err(refuse(
                ErrorCode::DeadlineExceeded,
                "graph_statement_timeout",
                format!(
                    "the graph's owner did not answer within {} ms",
                    timeout.as_millis()
                ),
            )),
        }
    }

    /// Serves a call another node forwarded here (`GraphForward`, GR1 Task 12): the encoded
    /// request of `method`, answered encoded. It is served by this node or refused, never
    /// forwarded again. `ExecuteStream` answers every chunk, each as a 4-byte big-endian length
    /// and its bytes.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` for a request that does not decode, or the call's own error.
    pub async fn serve_forwarded(
        &self,
        method: ForwardMethod,
        request: bytes::Bytes,
    ) -> Result<bytes::Bytes, ConnectError> {
        fn decode<M: buffa::Message>(bytes: &[u8]) -> Result<M, ConnectError> {
            M::decode_from_slice(bytes).map_err(|err| {
                refuse(
                    ErrorCode::InvalidArgument,
                    "invalid_argument",
                    format!("a forwarded graph request did not decode: {err}"),
                )
            })
        }
        let this = Self {
            forwarded: true,
            ..self.clone()
        };
        Ok(match method {
            ForwardMethod::Execute => this.execute(decode(&request)?).await?.encode_to_bytes(),
            ForwardMethod::ExecuteBatch => this
                .execute_batch(decode(&request)?)
                .await?
                .encode_to_bytes(),
            ForwardMethod::Explain => this.explain(decode(&request)?).await?.encode_to_bytes(),
            ForwardMethod::GetSchema => this.get_schema(decode(&request)?).await?.encode_to_bytes(),
            ForwardMethod::ExecuteStream => {
                use futures::StreamExt as _;
                let mut chunks = this.execute_stream(decode(&request)?).await?;
                let mut out = Vec::new();
                while let Some(chunk) = chunks.next().await {
                    let chunk = chunk?.encode_to_vec();
                    let len = u32::try_from(chunk.len()).map_err(|_| {
                        refuse(
                            ErrorCode::ResourceExhausted,
                            "graph_result_too_large",
                            "a result chunk is too large to forward",
                        )
                    })?;
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(&chunk);
                }
                bytes::Bytes::from(out)
            }
        })
    }

    /// Purges every deleted graph whose retention hold (`hold`, or the admin's own when `None`)
    /// has passed: its engine is closed, its storage directory removed and its catalog record
    /// dropped. Answers how many were purged. Meant for a periodic sweep (Task 5 wires it).
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when the catalog is.
    pub async fn purge_expired(&self, hold: Duration) -> Result<usize, ConnectError> {
        self.purge_expired_under(hold, None).await
    }

    /// [`GraphAdmin::purge_expired`] under the cluster's maintenance lease (GR1 Task 12): each
    /// graph's stream is dropped while `fence` holds, so a node that lost the lease mid-pass
    /// drops nothing more.
    async fn purge_expired_under(
        &self,
        hold: Duration,
        fence: Option<loams_common::meta::Fence>,
    ) -> Result<usize, ConnectError> {
        let now = self.catalog.now_ms();
        let before = now.saturating_sub(u64::try_from(hold.as_millis()).unwrap_or(u64::MAX));
        let mut purged = 0;
        for namespace in self.catalog.namespaces().await.map_err(map_catalog)? {
            // A namespace whose catalog fails is logged and skipped; the sweep goes on (M3).
            let deleting = match self.catalog.deleting(&namespace, before).await {
                Ok(deleting) => deleting,
                Err(err) => {
                    tracing::warn!(%namespace, error = %err, "purging deleted graphs: the namespace's catalog failed; skipped");
                    continue;
                }
            };
            for meta in deleting {
                if let Some(open) = self.open_graph(&namespace, &meta.name)
                    && open.id().is_some_and(|id| id.to_string() == meta.id)
                {
                    drop(open);
                    let (engine, ns, nm) = (
                        Arc::clone(&self.engine),
                        namespace.clone(),
                        meta.name.clone(),
                    );
                    let closed = blocking(move || Ok(engine.close(&ns, &nm).is_ok())).await;
                    if !matches!(closed, Ok(true)) {
                        // Still in use: purge on a later sweep.
                        continue;
                    }
                }
                if let (Some(data_dir), Ok(id)) = (self.engine.data_dir(), meta.graph_id()) {
                    let dir = data_dir.join("graphs").join(id.to_string());
                    let removed = blocking(move || {
                        Ok(match std::fs::remove_dir_all(&dir) {
                            Ok(()) => true,
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                            Err(err) => {
                                tracing::warn!(dir = %dir.display(), error = %err, "could not purge a deleted graph's storage");
                                false
                            }
                        })
                    })
                    .await;
                    if !matches!(removed, Ok(true)) {
                        continue;
                    }
                }
                // Its snapshots and manifests go before its stream and its record (GR1 Task 13),
                // so a purge cut short is finished by the next sweep.
                if let Err(err) = self.purge_snapshots(&namespace, &meta).await {
                    tracing::warn!(%namespace, id = %meta.id, error = %err, "deleting a deleted graph's snapshots failed; retried on the next sweep");
                    continue;
                }
                // The graph's stream goes before its record, so a purge cut short is finished
                // by the next sweep, which still finds the record (GR1 Task 12).
                if self.log.is_some()
                    && let Err(err) = self.catalog.drop_stream(&meta, fence.clone()).await
                {
                    tracing::warn!(%namespace, id = %meta.id, error = %err, "dropping a deleted graph's stream failed; retried on the next sweep");
                    continue;
                }
                if let (Some(ownership), Ok(id)) = (&self.ownership, meta.graph_id()) {
                    ownership.release(id).await;
                }
                match self.catalog.purge(&namespace, &meta.id).await {
                    Ok(true) => purged += 1,
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(%namespace, id = %meta.id, error = %err, "purging a deleted graph's record failed; retried on the next sweep");
                    }
                }
            }
        }
        Ok(purged)
    }

    /// One maintenance pass (GR1 Task 12; Task 4 review M3), run periodically on every graph
    /// node:
    ///
    /// 1. **On this node:** every open graph whose catalog record is gone or being deleted is
    ///    closed and its owner lease given up, and the local cache of every graph the catalog
    ///    no longer serves is removed ([`GraphAdmin::sweep_local`]).
    /// 2. **On one node at a time,** the holder of the cluster's maintenance lease
    ///    ([`crate::owner::MAINTENANCE_LEASE`]): deleted graphs past `hold` are purged (their
    ///    streams dropped under the lease's fence) and catalog documents older than `grace`
    ///    swept. A node that cannot take the lease skips this part.
    ///
    /// Answers how many graphs were purged here.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when the catalog or the metastore is.
    pub async fn maintain(&self, hold: Duration, grace: Duration) -> Result<usize, ConnectError> {
        if let Err(err) = self.sweep_local().await {
            tracing::warn!(error = %err, "sweeping this node's graphs failed");
        }
        let Some(ownership) = &self.ownership else {
            let purged = self.purge_expired(hold).await?;
            self.sweep_documents(grace).await?;
            return Ok(purged);
        };
        let meta = self.catalog.meta();
        let owner = ownership.me().lease_owner();
        let grant = match meta
            .acquire_lease(
                crate::owner::MAINTENANCE_LEASE,
                &owner,
                MAINTENANCE_LEASE_TTL,
            )
            .await
        {
            Ok(grant) => grant,
            Err(loams_common::meta::MetaError::Rejected(
                loams_common::meta::ApplyError::LeaseHeld { .. },
            )) => return Ok(0),
            Err(err) => {
                return Err(refuse(
                    ErrorCode::Unavailable,
                    "unavailable",
                    format!("taking the graph maintenance lease failed: {err}"),
                ));
            }
        };
        let fence = loams_common::meta::Fence {
            lease: crate::owner::MAINTENANCE_LEASE.to_string(),
            epoch: grant.epoch,
        };
        let purged = self.purge_expired_under(hold, Some(fence)).await;
        if let Err(err) = self.sweep_snapshots(grace).await {
            tracing::warn!(error = %err, "sweeping graph snapshots failed");
        }
        let swept = self.sweep_documents(grace).await;
        if let Err(err) = meta
            .release_lease(crate::owner::MAINTENANCE_LEASE, &owner, grant.epoch)
            .await
        {
            tracing::warn!(error = %err, "giving up the graph maintenance lease failed; it expires");
        }
        swept?;
        purged
    }

    /// This node's part of [`GraphAdmin::maintain`]: closes every open graph the catalog no
    /// longer serves (deleted, or replaced under its name) and gives up its owner lease, then
    /// removes the local cache directory of every graph the catalog does not serve, unless it is
    /// open or was written in the last [`LOCAL_CACHE_GRACE`] (an open in progress). A durable
    /// graph's local files are a cache, so nothing is lost. Answers how many directories went.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when the catalog is.
    pub async fn sweep_local(&self) -> Result<usize, ConnectError> {
        let mut served = std::collections::HashSet::new();
        for namespace in self.catalog.namespaces().await.map_err(map_catalog)? {
            let mut token = String::new();
            loop {
                let page = self
                    .catalog
                    .list(&namespace, 1000, &token)
                    .await
                    .map_err(map_catalog)?;
                served.extend(page.graphs.iter().map(|g| g.id.clone()));
                match page.next_page_token {
                    Some(next) => token = next,
                    None => break,
                }
            }
        }
        for graph in self.engine.list(None).unwrap_or_default() {
            let Some(id) = graph.id() else { continue };
            if served.contains(&id.to_string()) {
                continue;
            }
            let (namespace, name) = (graph.namespace().to_string(), graph.name().to_string());
            drop(graph);
            let engine = Arc::clone(&self.engine);
            let closed = blocking(move || Ok(engine.close(&namespace, &name).is_ok())).await;
            if matches!(closed, Ok(true))
                && let Some(ownership) = &self.ownership
            {
                ownership.release(id).await;
            }
        }
        let Some(data_dir) = self.engine.data_dir().map(|dir| dir.join("graphs")) else {
            return Ok(0);
        };
        let open: std::collections::HashSet<String> = self
            .engine
            .list(None)
            .unwrap_or_default()
            .iter()
            .filter_map(|g| g.id().map(|id| id.to_string()))
            .collect();
        blocking(move || {
            let mut removed = 0;
            let Ok(entries) = std::fs::read_dir(&data_dir) else {
                return Ok(0);
            };
            for entry in entries.flatten() {
                let id = entry.file_name().to_string_lossy().into_owned();
                if id.parse::<crate::GraphId>().is_err() || served.contains(&id) || open.contains(&id)
                {
                    continue;
                }
                let recent = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_none_or(|age| age < LOCAL_CACHE_GRACE);
                if recent {
                    continue;
                }
                match std::fs::remove_dir_all(entry.path()) {
                    Ok(()) => removed += 1,
                    Err(err) => {
                        tracing::warn!(dir = %entry.path().display(), error = %err, "could not remove a graph's local cache");
                    }
                }
            }
            Ok(removed)
        })
        .await
    }

    /// Deletes every bucket object of deleted graph `meta` (its snapshots and manifests).
    async fn purge_snapshots(&self, namespace: &str, meta: &GraphMeta) -> Result<(), ConnectError> {
        let Some(checkpointer) = &self.checkpointer else {
            return Ok(());
        };
        let id = meta.graph_id().map_err(map_catalog)?;
        let Some(ns) = self
            .catalog
            .namespace_id(namespace)
            .await
            .map_err(map_catalog)?
        else {
            return Ok(());
        };
        checkpointer
            .manifests()
            .purge(ns, id)
            .await
            .map(|_| ())
            .map_err(|err| map_engine(err.into()))
    }

    /// Deletes the snapshot objects and manifest documents of every graph in the catalog that no
    /// reader still needs ([`ManifestStore::sweep`]), written at least `grace` (floor 5 minutes
    /// outside tests) before the graph's current manifest. Runs under the maintenance lease.
    /// Answers how many objects went.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when the catalog is; a graph whose sweep fails is logged and skipped.
    pub async fn sweep_snapshots(&self, grace: Duration) -> Result<usize, ConnectError> {
        let Some(checkpointer) = &self.checkpointer else {
            return Ok(0);
        };
        let grace = if cfg!(feature = "test-hooks") {
            grace
        } else {
            grace.max(crate::catalog::MIN_DOCUMENT_GRACE)
        };
        let mut deleted = 0;
        for namespace in self.catalog.namespaces().await.map_err(map_catalog)? {
            let Some(ns) = self
                .catalog
                .namespace_id(&namespace)
                .await
                .map_err(map_catalog)?
            else {
                continue;
            };
            let mut token = String::new();
            loop {
                let page = match self.catalog.list(&namespace, 1000, &token).await {
                    Ok(page) => page,
                    Err(err) => {
                        tracing::warn!(%namespace, error = %err, "sweeping graph snapshots: the namespace's catalog failed; skipped");
                        break;
                    }
                };
                for meta in &page.graphs {
                    let Ok(id) = meta.graph_id() else { continue };
                    match checkpointer.manifests().sweep(ns, id, grace).await {
                        Ok(n) => deleted += n,
                        Err(err) => {
                            tracing::warn!(%namespace, id = %meta.id, error = %err, "sweeping a graph's snapshots failed");
                        }
                    }
                }
                match page.next_page_token {
                    Some(next) => token = next,
                    None => break,
                }
            }
        }
        Ok(deleted)
    }

    /// Takes the snapshots `kinds` of graph `namespace`/`name` now, commits them to its manifest
    /// and trims its stream (GR1 Task 13: on demand, and before eviction and engine upgrades,
    /// Tasks 14 and 28). Runs on the graph's owner: the graph is opened here if it is not.
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` when graphs are not durable in this server, what opening the graph
    /// answers (`NOT_FOUND`, `UNAVAILABLE`/`graph_reloading` when another node owns it), and
    /// `UNAVAILABLE` when the bucket or the metastore fails or the owner lease moved on.
    pub async fn checkpoint(
        &self,
        namespace: &str,
        name: &str,
        kinds: &[SnapshotKind],
    ) -> Result<Checkpointed, ConnectError> {
        let Some(checkpointer) = &self.checkpointer else {
            return Err(refuse(
                ErrorCode::FailedPrecondition,
                "failed_precondition",
                "graphs are not durable in this server, so they have no snapshots",
            ));
        };
        let graph = self.open(namespace, name).await?;
        let ns = self
            .catalog
            .namespace_id(namespace)
            .await
            .map_err(map_catalog)?
            .ok_or_else(|| {
                map_catalog(CatalogError::NotFound {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                })
            })?;
        checkpointer
            .checkpoint(graph, ns, kinds)
            .await
            .map_err(|err| map_engine(err.into()))
    }

    /// Takes every snapshot that is due of every durable graph open (and so owned) here, under
    /// the checkpoint policy (GR1 Task 13), and keeps each one's adjacency index fresh, if it has
    /// one (GR1 Task 22). Run periodically by the server. Answers how many graphs were
    /// checkpointed; a failure is logged and the graph is tried again next time.
    pub async fn checkpoint_due(&self) -> usize {
        let Some(checkpointer) = &self.checkpointer else {
            return 0;
        };
        if self.is_shutting_down() {
            return 0;
        }
        // Identities only: a graph handle held across the awaits below would keep a graph that
        // was closed or fenced meanwhile from being dropped (PR #451 review). Each graph is looked
        // up again right before its turn.
        let open: Vec<(String, String, crate::GraphId)> = self
            .engine
            .list(None)
            .unwrap_or_default()
            .iter()
            .filter(|graph| graph.is_durable())
            .filter_map(|graph| {
                let id = graph.id()?;
                Some((graph.namespace().to_string(), graph.name().to_string(), id))
            })
            .collect();
        let mut namespaces = std::collections::HashMap::new();
        let mut done = 0;
        for (namespace, name, id) in open {
            if self.is_shutting_down() {
                break;
            }
            let ns = match namespaces.get(&namespace) {
                Some(ns) => *ns,
                None => match self.catalog.namespace_id(&namespace).await {
                    Ok(Some(ns)) => {
                        namespaces.insert(namespace.clone(), ns);
                        ns
                    }
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(%namespace, error = %err, "checkpointing: the namespace could not be read");
                        continue;
                    }
                },
            };
            let Some(graph) = self
                .open_graph(&namespace, &name)
                .filter(|graph| graph.id() == Some(id))
            else {
                continue;
            };
            if graph.is_poisoned_or_failed() || graph.is_fenced() {
                continue;
            }
            // The adjacency index, when an algorithm gave the graph one (GR1 Task 22): kept
            // within `rebuild_after_records` and `interval`, off the write lane.
            if let Some(indexer) = &self.indexer
                && let Err(err) = indexer.maintain(Arc::clone(&graph), ns).await
            {
                tracing::warn!(%namespace, %name, error = %err, "maintaining a graph's adjacency index failed; retried next time");
            }
            let kinds = match checkpointer.due(&graph, ns).await {
                Ok(kinds) if kinds.is_empty() => continue,
                Ok(kinds) => kinds,
                Err(err) => {
                    tracing::warn!(%namespace, %name, error = %err, "checkpointing: reading the graph's manifest failed");
                    continue;
                }
            };
            match checkpointer.checkpoint(graph, ns, &kinds).await {
                Ok(_) => done += 1,
                Err(err) => {
                    tracing::warn!(%namespace, %name, error = %err, "checkpointing a graph failed; retried next time");
                }
            }
        }
        done
    }

    /// Evicts graph `namespace`/`name` now, if it is open here and nobody holds it (GR1 Task
    /// 14): as an idle eviction does, it is checkpointed first, closed with its local files kept
    /// and stamped, and its owner lease given up. Answers whether it was evicted.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` for a graph the catalog does not serve, `UNAVAILABLE` when the catalog is.
    pub async fn evict(&self, namespace: &str, name: &str) -> Result<bool, ConnectError> {
        let meta = self
            .catalog
            .get_by_name(namespace, name)
            .await
            .map_err(map_catalog)?;
        let id = meta.graph_id().map_err(map_catalog)?;
        let ns = self
            .catalog
            .namespace_id(namespace)
            .await
            .map_err(map_catalog)?;
        Ok(self
            .evict_one(namespace, name, id, ns, EvictReason::OnDemand)
            .await
            .is_some())
    }

    /// One eviction pass over the graphs open here (GR1 Task 14; [`crate::evict`]): graphs
    /// deleted from the catalog are closed and their local caches removed, as are the caches of
    /// evicted graphs deleted since; then, least recently used first, graphs idle for the
    /// policy's time, and while the open graphs' memory is over the node's budget, any graph
    /// nobody holds. Answers what it evicted, in order. Run periodically by the server; a
    /// failure is logged and the graph is tried again next time.
    pub async fn evict_due(&self) -> Vec<Evicted> {
        let mut done = Vec::new();
        if self.is_shutting_down() {
            return done;
        }
        self.retire_followers().await;
        let policy = self.evict_policy;
        let open: Vec<(String, String, crate::GraphId)> = self
            .engine
            .list(None)
            .unwrap_or_default()
            .iter()
            .filter_map(|graph| {
                let id = graph.id()?;
                Some((graph.namespace().to_string(), graph.name().to_string(), id))
            })
            .collect();
        // Deleted graphs first: open here, or evicted earlier.
        let mut residents = Vec::new();
        let mut namespaces = std::collections::HashMap::new();
        for (namespace, name, id) in open {
            match self.served(&namespace, &name, id).await {
                Some(true) => {}
                Some(false) => {
                    if let Some(evicted) = self.release_deleted(&namespace, &name, id).await {
                        done.push(evicted);
                    }
                    continue;
                }
                None => continue,
            }
            let Some(graph) = self
                .open_graph(&namespace, &name)
                .filter(|graph| graph.id() == Some(id) && graph.is_durable())
            else {
                continue;
            };
            let idle_for = graph.idle_for();
            let bytes = if policy.node_memory.is_some() {
                let held = Arc::clone(&graph);
                blocking(move || Ok(held.resident_bytes()))
                    .await
                    .unwrap_or(0)
            } else {
                0
            };
            drop(graph);
            if !namespaces.contains_key(&namespace) {
                match self.catalog.namespace_id(&namespace).await {
                    Ok(ns) => {
                        namespaces.insert(namespace.clone(), ns);
                    }
                    Err(err) => {
                        tracing::warn!(%namespace, error = %err, "evicting: the namespace could not be read");
                        continue;
                    }
                }
            }
            residents.push(Resident {
                namespace,
                name,
                graph: id,
                idle_for,
                bytes,
            });
        }
        for (id, namespace, name) in self.evictor.evicted() {
            if self.served(&namespace, &name, id).await == Some(false) {
                self.evictor.forget(id);
                if let Some(evicted) = self.release_deleted(&namespace, &name, id).await {
                    done.push(evicted);
                }
            }
        }
        let mut total: u64 = residents.iter().map(|r| r.bytes).sum();
        for resident in crate::evict::by_lru(residents) {
            if self.is_shutting_down() {
                break;
            }
            let Some(reason) = crate::evict::reason(&policy, &resident, total) else {
                continue;
            };
            let ns = namespaces.get(&resident.namespace).copied().flatten();
            if let Some(evicted) = self
                .evict_one(
                    &resident.namespace,
                    &resident.name,
                    resident.graph,
                    ns,
                    reason,
                )
                .await
            {
                total = total.saturating_sub(resident.bytes);
                done.push(evicted);
            }
        }
        done
    }

    /// Whether the catalog still serves graph `namespace`/`name` as `id`: `None` when it cannot
    /// be read (nothing is done to the graph then).
    async fn served(&self, namespace: &str, name: &str, id: crate::GraphId) -> Option<bool> {
        match self.catalog.get_by_name(namespace, name).await {
            Ok(meta) => Some(meta.graph_id().is_ok_and(|served| served == id)),
            Err(CatalogError::NotFound { .. }) => Some(false),
            Err(err) => {
                tracing::warn!(%namespace, %name, error = %err, "evicting: the graph's catalog record could not be read");
                None
            }
        }
    }

    /// Closes deleted graph `id` if it is open here and removes its local cache (Task 4 review
    /// M3), and gives its owner lease up.
    async fn release_deleted(
        &self,
        namespace: &str,
        name: &str,
        id: crate::GraphId,
    ) -> Option<Evicted> {
        let (engine, ns, nm) = (
            Arc::clone(&self.engine),
            namespace.to_string(),
            name.to_string(),
        );
        let closed =
            blocking(move || engine.evict(&ns, &nm, Some(id), false).map_err(map_engine)).await;
        match closed {
            Ok(Closed::Removed) => {}
            Ok(Closed::Busy) => return None,
            Ok(_) => {
                if let Some(ownership) = &self.ownership {
                    ownership.release(id).await;
                }
                return None;
            }
            Err(err) => {
                tracing::warn!(%namespace, %name, error = %err, "releasing a deleted graph's local cache failed; retried next time");
                return None;
            }
        }
        if let Some(ownership) = &self.ownership {
            ownership.release(id).await;
        }
        tracing::info!(%namespace, %name, graph = %id, "released a deleted graph's local cache");
        Some(Evicted {
            namespace: namespace.to_string(),
            name: name.to_string(),
            graph: id,
            reason: EvictReason::Deleted,
            checkpointed: false,
            cache_kept: false,
        })
    }

    /// Evicts graph `id`, open here as `namespace`/`name`, for `reason`: checkpointed first when
    /// its snapshots do not cover its durable state (an idle or on-demand eviction whose
    /// checkpoint fails waits for the next pass; a memory one goes on, since the log is the
    /// graph), then closed with its local files stamped, its owner lease given up, and marked
    /// evicted. `None` when it is not open, not durable, not serving cleanly, or held.
    async fn evict_one(
        &self,
        namespace: &str,
        name: &str,
        id: crate::GraphId,
        ns: Option<loams_common::NamespaceId>,
        reason: EvictReason,
    ) -> Option<Evicted> {
        let graph = self
            .open_graph(namespace, name)
            .filter(|graph| graph.id() == Some(id) && graph.is_durable())?;
        if graph.is_poisoned_or_failed() || graph.is_fenced() {
            return None;
        }
        let checkpointed = match (ns, &self.checkpointer) {
            (Some(ns), Some(checkpointer)) => {
                match self.cover(checkpointer, Arc::clone(&graph), ns).await {
                    Ok(took) => took,
                    Err(err) if reason == EvictReason::Memory => {
                        tracing::warn!(%namespace, %name, error = %err, "checkpointing before an eviction failed; evicting anyway (the log is the graph)");
                        false
                    }
                    Err(err) => {
                        tracing::warn!(%namespace, %name, error = %err, "checkpointing before an eviction failed; the eviction waits for the next pass");
                        return None;
                    }
                }
            }
            _ => false,
        };
        // The owner lease may have moved on while the checkpoint ran (a refused manifest CAS or
        // trim fences the graph, and so does a lost renewal): the fence path drops the engine,
        // and this node must neither give up a lease it no longer holds nor call the graph
        // evicted here (PR #459 review).
        if graph.is_fenced() {
            tracing::info!(%namespace, %name, "a graph was fenced while it was being evicted; left to the fence");
            return None;
        }
        drop(graph);
        let (engine, ns_name, nm) = (
            Arc::clone(&self.engine),
            namespace.to_string(),
            name.to_string(),
        );
        let closed = blocking(move || {
            engine
                .evict(&ns_name, &nm, Some(id), true)
                .map_err(map_engine)
        })
        .await;
        let cache_kept = match closed {
            Ok(Closed::Stamped) => true,
            Ok(Closed::Closed) => false,
            Ok(_) => return None,
            Err(err) => {
                tracing::warn!(%namespace, %name, error = %err, "evicting a graph failed");
                return None;
            }
        };
        // A statement that opened it again since (under a fresh lease or this one) keeps the
        // lease; otherwise the next node may take the graph at once.
        if let Some(ownership) = &self.ownership
            && self
                .open_graph(namespace, name)
                .filter(|graph| graph.id() == Some(id))
                .is_none()
        {
            ownership.release(id).await;
        }
        self.evictor.mark_evicted(id, namespace, name);
        tracing::info!(%namespace, %name, graph = %id, reason = reason.as_str(), checkpointed, cache_kept, "evicted a graph");
        Some(Evicted {
            namespace: namespace.to_string(),
            name: name.to_string(),
            graph: id,
            reason,
            checkpointed,
            cache_kept,
        })
    }

    /// Takes a snapshot of `graph` unless its manifest already has one at its durable offset that
    /// this node could open it from (on disk any kind, in memory a portable one). Answers whether
    /// it took one.
    async fn cover(
        &self,
        checkpointer: &Checkpointer,
        graph: Arc<Graph>,
        ns: loams_common::NamespaceId,
    ) -> Result<bool, crate::snapshot::SnapshotError> {
        let (Some(id), Some(offset)) = (
            graph.id(),
            graph
                .durable_state()
                .map(|durable| durable.position.offset()),
        ) else {
            return Ok(false);
        };
        if offset == 0 {
            return Ok(false);
        }
        let manifest = checkpointer.manifests().load(ns, id).await?;
        let kind = graph
            .storage_dir()
            .map_or(Some(SnapshotKind::Portable), |_| None);
        let covered = manifest
            .as_ref()
            .and_then(|loaded| loaded.manifest.newest(kind))
            .is_some_and(|entry| entry.offset >= offset);
        if covered {
            return Ok(false);
        }
        checkpointer
            .checkpoint(graph, ns, &[SnapshotKind::Native])
            .await
            .map(|_| true)
    }

    /// Deletes catalog documents older than `grace` that no reader still needs (the catalog's
    /// [`GraphCatalog::sweep_documents`]); run with [`crate::catalog::DOCUMENT_GRACE`] beside
    /// [`GraphAdmin::purge_expired`].
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` when the catalog is.
    pub async fn sweep_documents(&self, grace: Duration) -> Result<usize, ConnectError> {
        self.catalog
            .sweep_documents(grace)
            .await
            .map_err(map_catalog)
    }

    /// The retention hold this admin purges with.
    #[must_use]
    pub fn retention_hold(&self) -> Duration {
        self.retention_hold
    }

    /// Stops serving and closes every open graph, releasing its storage (for a shutdown or a test
    /// restart; review fix 1, I3). Answers how many statements were still running when it gave
    /// up waiting.
    ///
    /// 1. From the start, `open` and every statement answer `UNAVAILABLE`.
    /// 2. It waits, at most `wait`, for running statements to end and free their slots.
    /// 3. It closes each graph on the blocking pool, flushing its WAL.
    ///
    /// A statement still running after `wait` (one whose client went away, R0.8: Grafeo cannot
    /// stop it) is detached and logged. Its graph cannot be closed while the statement holds it,
    /// so it stays registered in the engine until the engine itself drops (the last `Arc` of
    /// the engine and of the statement's handle). Grafeo's `Drop for GrafeoDB` calls the same
    /// `close` (WAL sync and checkpoint), so the data is flushed then, but a failure there is
    /// only logged by Grafeo, not reported to Loams. It runs on a statement pool thread, not
    /// tokio's blocking pool, so it no longer delays the runtime's drop (GR1 Task 6, R6.5):
    /// the process exits with it unfinished, and its transaction never commits (its client
    /// was already answered with an error). Task 26's watchdog is what bounds such a statement
    /// while the process runs.
    pub async fn shutdown(&self, wait: Duration) -> usize {
        self.closed.store(true, Ordering::SeqCst);
        let slots = u32::try_from(self.statement_slots).unwrap_or(u32::MAX);
        // Every slot, so nothing is running; held until the graphs are closed.
        let drained =
            tokio::time::timeout(wait, Arc::clone(&self.statements).acquire_many_owned(slots))
                .await;
        let running = match &drained {
            Ok(Ok(_)) => 0,
            _ => self.statements_in_flight(),
        };
        if running > 0 {
            tracing::warn!(
                running,
                ?wait,
                "graph statements still running at shutdown are detached; their graphs stay registered until the engine drops, and they end with the process"
            );
        }
        // Each graph closes cleanly and keeps its local files, stamped, so the next open here
        // starts from them (GR1 Task 14).
        for graph in self.engine.list(None).unwrap_or_default() {
            let (namespace, name, id) = (
                graph.namespace().to_string(),
                graph.name().to_string(),
                graph.id(),
            );
            drop(graph);
            let (engine, ns, nm) = (Arc::clone(&self.engine), namespace.clone(), name.clone());
            match blocking(move || engine.evict(&ns, &nm, id, true).map_err(map_engine)).await {
                Ok(Closed::Busy) => {
                    tracing::warn!(%namespace, %name, "a graph still in use at shutdown stays open until it is released");
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(%namespace, %name, error = %err, "closing a graph at shutdown failed");
                }
            }
        }
        // Followers keep nothing: they are in memory (GR1 Task 15).
        if let Some(replicas) = &self.replicas {
            let replicas = Arc::clone(replicas);
            let _ = blocking(move || {
                replicas.close_all();
                Ok(())
            })
            .await;
        }
        // Graceful: the next owner takes each lease at once instead of after its TTL (GR1 Task
        // 12, `owner_moves_on_node_leave`).
        if let Some(ownership) = &self.ownership {
            ownership.release_all().await;
        }
        drop(drained);
        running
    }
}

/// The epoch of a placement that must be here: a call that reached the open path is served by
/// this node or refused (`graph_reloading`, the client retries and reaches the owner).
fn here(
    placed: Result<Placement, crate::GraphError>,
    graph: crate::GraphId,
) -> Result<u64, ConnectError> {
    match placed.map_err(map_engine)? {
        Placement::Here(epoch) => Ok(epoch),
        Placement::There(node) => Err(map_engine(crate::GraphError::OwnerMoved(format!(
            "graph {graph} is served by node {} now; retry",
            node.node_id
        )))),
    }
}

/// Fences the engine of `graph` open here (at `epoch`, when given) and drops it once nobody
/// holds it: its owner lease moved on (GR1 Task 12).
fn fence_open(engine: &Engine, graph: crate::GraphId, epoch: Option<u64>) {
    let Ok(open) = engine.list(None) else { return };
    for handle in open {
        if handle.id() != Some(graph) || epoch.is_some_and(|e| handle.lease_epoch() != Some(e)) {
            continue;
        }
        handle.mark_fenced();
        let (namespace, name) = (handle.namespace().to_string(), handle.name().to_string());
        drop(handle);
        if let Err(err) = engine.discard(&namespace, &name) {
            tracing::debug!(%namespace, %name, error = %err, "a fenced graph is still in use; dropped by its next call");
        }
    }
}

/// The chunks of a forwarded `ExecuteStream` answer ([`GraphAdmin::serve_forwarded`]).
fn decode_chunks(mut body: &[u8]) -> Result<Vec<pb::ResultChunk>, ConnectError> {
    let malformed = || {
        refuse(
            ErrorCode::Internal,
            "internal",
            "internal error forwarding the statement to the graph's owner",
        )
    };
    let mut out = Vec::new();
    while !body.is_empty() {
        let (len, rest) = body.split_at_checked(4).ok_or_else(malformed)?;
        let len = u32::from_be_bytes(len.try_into().map_err(|_| malformed())?) as usize;
        let (chunk, rest) = rest.split_at_checked(len).ok_or_else(malformed)?;
        out.push(buffa::Message::decode_from_slice(chunk).map_err(|_| malformed())?);
        body = rest;
    }
    Ok(out)
}

/// Held by a statement's task: settles its [`Watch`] when the task ends, however it ends.
struct Finished(Watch);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Held by the caller waiting for a statement: when the wait ends without the statement having
/// ended (its deadline, or its client went away), the statement is detached.
struct Waiting(Watch);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.abandon();
    }
}

/// How long replica reads use the catalog record they last read, without asking, after the
/// catalog was found unavailable (GR1 Task 16).
pub const CATALOG_RETRY_AFTER: Duration = Duration::from_secs(5);

/// When the catalog was last found unavailable by a replica read.
#[derive(Debug, Default)]
struct CatalogDown(std::sync::Mutex<Option<std::time::Instant>>);

impl CatalogDown {
    fn mark(&self) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::time::Instant::now());
    }

    fn recently(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|at| at.elapsed() < CATALOG_RETRY_AFTER)
    }
}

/// Which engine of this node a statement runs on (GR1 Task 15).
#[derive(Debug, Clone, Copy, Default)]
struct Serve {
    /// This node's follower, rather than the owner's engine.
    follower: bool,
    /// The token the replica must have applied before the statement runs.
    at_least: Option<ConsistencyToken>,
}

/// Where a data-plane call runs ([`GraphAdmin::route`]).
enum Route {
    /// On the graph's owner, another node.
    There(GraphNode),
    /// Here.
    Here(Serve),
}

/// A statement's process and namespace slots, freed when dropped.
pub(crate) struct Slots {
    _process: tokio::sync::OwnedSemaphorePermit,
    _namespace: crate::limits::NamespaceSlot,
}
