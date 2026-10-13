//! One fenced owner per graph (GR1 Task 12; design §48 §7.1, D744, I5; Review Focus 2).
//!
//! Several in-process nodes share one metastore and one bucket. Each has its own engine, its own
//! log writer and its own view of the graph nodes ([`FixedMembership`], which a test changes to
//! model a node leaving or a partition). Forwarding runs in process through
//! [`GraphAdmin::serve_forwarded`], with a node that can be cut off.
//!
//! * A graph has one writer: every record of its stream carries one lease epoch, `txn_seq` is
//!   dense, and a write sent to another node is forwarded and answers the owner's token.
//! * A stale owner's append is refused by the metastore (its fence), not by the stale owner, and
//!   no acknowledged write is lost.
//! * A reload takes a fresh epoch before it reads the stream, so a commit of the old engine that
//!   lands late is refused (R11.8's race).
//! * A graph's stream has the reserved name `_graph.<gr_id>`, and GR1 Task 11's `graph.<gr_id>`
//!   moves to it on open; a purge drops it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use connectrpc::{ConnectError, ErrorCode};
use loams_cache::{RangeCache, RangeCacheConfig};
use loams_common::StreamId;
use loams_common::meta::{
    Consistency, MetaStore, Retention, WalClass, graph_stream_name, legacy_graph_stream_name,
};
use loams_graph::catalog::{GraphCatalog, NewGraph};
use loams_graph::changeset::{GraphRecord, decode};
use loams_graph::durable::{ConsistencyToken, GraphLog, GroupCommit};
use loams_graph::owner::{
    FixedMembership, ForwardFuture, ForwardMethod, Forwarder, GraphNode, OwnerConfig, Ownership,
    owner_lease_key, rendezvous,
};
use loams_graph::service::admin::GraphAdmin;
use loams_graph::{Engine, GraphId};
use loams_log::{FetchRequest, LogReader, LogWriter, OffsetRecord};
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_proto::loams::errors::v1::ErrorInfo;
use loams_proto::loams::graph::v1 as pb;
use loams_store::{Fault, FaultyStore, Op, Store};
use pb::__buffa::oneof::value::Kind;
use tempfile::TempDir;

/// Calls another node's `serve_forwarded` in process; a node missing from the map is
/// unreachable.
#[derive(Debug, Default)]
struct InProcess {
    nodes: Mutex<HashMap<u64, GraphAdmin>>,
}

impl InProcess {
    fn cut_off(&self, node_id: u64) -> Option<GraphAdmin> {
        self.nodes.lock().expect("lock").remove(&node_id)
    }

    fn reconnect(&self, node_id: u64, admin: GraphAdmin) {
        self.nodes.lock().expect("lock").insert(node_id, admin);
    }
}

impl Forwarder for InProcess {
    fn forward(
        &self,
        to: &GraphNode,
        method: ForwardMethod,
        request: Bytes,
        _timeout: Duration,
    ) -> ForwardFuture {
        let admin = self.nodes.lock().expect("lock").get(&to.node_id).cloned();
        Box::pin(async move {
            match admin {
                Some(admin) => admin.serve_forwarded(method, request).await,
                None => Err(ConnectError::new(
                    ErrorCode::Unavailable,
                    "the graph's owner is unreachable",
                )),
            }
        })
    }
}

/// One node: an engine, a log writer of its own, its view of the graph nodes, its leases.
struct Node {
    me: GraphNode,
    admin: GraphAdmin,
    log: GraphLog,
    membership: Arc<FixedMembership>,
    ownership: Arc<Ownership>,
    _writer: LogWriter,
    _data: TempDir,
}

/// A metastore, a bucket and `n` nodes.
struct Cluster {
    _meta_node: MetaNode,
    _meta_dir: TempDir,
    meta: Arc<dyn MetaStore>,
    store: Store,
    reader: LogReader,
    forwarder: Arc<InProcess>,
    nodes: Vec<Node>,
}

/// Leases short enough that a test can wait one out.
const SHORT: OwnerConfig = OwnerConfig {
    ttl: Duration::from_millis(1_500),
    renew_every: Duration::from_millis(300),
};

impl Cluster {
    async fn start(n: u64, config: OwnerConfig) -> Self {
        Self::start_on(n, config, Store::in_memory(), GroupCommit::default()).await
    }

    async fn start_on(n: u64, config: OwnerConfig, store: Store, group: GroupCommit) -> Self {
        let meta_dir = TempDir::new().expect("temp dir");
        let clock = Arc::new(SystemClock);
        let mut meta_config = MetaConfig::new(1, meta_dir.path(), Store::in_memory());
        meta_config.clock = clock.clone();
        let meta_node = MetaNode::start(meta_config, &Router::new())
            .await
            .expect("start meta");
        meta_node.initialize([1]).await.expect("initialize");
        meta_node
            .wait_for_leader(Duration::from_secs(30))
            .await
            .expect("leader");
        let meta: Arc<dyn MetaStore> = Arc::new(MetaClient::new(
            meta_node.clone(),
            vec![],
            clock,
            MetaClientConfig::default(),
        ));
        let cache = RangeCache::new(
            store.clone(),
            RangeCacheConfig {
                memory_bytes: 16 << 20,
                ..RangeCacheConfig::default()
            },
        )
        .await
        .expect("range cache");
        let reader = LogReader::new(meta.clone(), cache);
        let forwarder = Arc::new(InProcess::default());
        let everyone: Vec<GraphNode> = (1..=n)
            .map(|node_id| GraphNode {
                node_id,
                incarnation: ulid::Ulid::generate(),
                addr: format!("node-{node_id}"),
            })
            .collect();
        let mut nodes = Vec::new();
        for me in &everyone {
            let writer = LogWriter::start(
                meta.clone(),
                store.clone(),
                GraphLog::log_config(me.node_id),
            )
            .expect("log writer");
            let log = GraphLog::new(
                meta.clone(),
                writer.clone(),
                reader.clone(),
                tokio::runtime::Handle::current(),
            )
            .with_group_commit(group);
            let membership = Arc::new(FixedMembership::new(me.clone(), everyone.clone()));
            let ownership = Ownership::new(
                meta.clone(),
                membership.clone(),
                Some(forwarder.clone() as Arc<dyn Forwarder>),
                config,
            );
            let data = TempDir::new().expect("data dir");
            let admin = GraphAdmin::new(
                Arc::new(Engine::with_data_dir(data.path())),
                GraphCatalog::new(meta.clone(), store.clone()),
            )
            .with_log(log.clone())
            .with_ownership(ownership.clone());
            forwarder.reconnect(me.node_id, admin.clone());
            nodes.push(Node {
                me: me.clone(),
                admin,
                log,
                membership,
                ownership,
                _writer: writer,
                _data: data,
            });
        }
        Self {
            _meta_node: meta_node,
            _meta_dir: meta_dir,
            meta,
            store,
            reader,
            forwarder,
            nodes,
        }
    }

    fn node(&self, node_id: u64) -> &Node {
        &self.nodes[usize::try_from(node_id - 1).expect("small")]
    }

    fn catalog(&self) -> GraphCatalog {
        GraphCatalog::new(self.meta.clone(), self.store.clone())
    }

    async fn graph_id(&self, name: &str) -> GraphId {
        self.node(1)
            .admin
            .catalog()
            .get_by_name("acme", name)
            .await
            .expect("catalog record")
            .graph_id()
            .expect("graph id")
    }

    async fn stream_of(&self, name: &str) -> StreamId {
        self.node(1)
            .admin
            .catalog()
            .get_by_name("acme", name)
            .await
            .expect("catalog record")
            .stream
            .expect("a stream")
    }

    /// The node the rendezvous places `graph` on, among every node.
    fn preferred(&self, graph: GraphId) -> u64 {
        let all: Vec<GraphNode> = self.nodes.iter().map(|n| n.me.clone()).collect();
        rendezvous(graph, &all).expect("a node").node_id
    }

    /// Every record of a stream, from its start.
    async fn records(&self, stream: StreamId) -> Vec<OffsetRecord> {
        let mut out = Vec::new();
        let mut offset = 0;
        loop {
            let fetched = self
                .reader
                .fetch(FetchRequest {
                    stream,
                    partition: 0,
                    offset,
                    max_bytes: 1 << 20,
                    max_wait: Duration::ZERO,
                })
                .await
                .expect("fetch");
            out.extend(fetched.records);
            if fetched.next_offset >= fetched.high_watermark {
                return out;
            }
            offset = fetched.next_offset;
        }
    }

    /// `(lease_epoch, txn_seq)` of every change set of a stream, in log order.
    async fn epochs(&self, stream: StreamId) -> Vec<(u64, u64)> {
        self.records(stream)
            .await
            .iter()
            .map(
                |r| match decode(r.record.value.as_deref().expect("value")) {
                    Ok(GraphRecord::ChangeSet(change)) => (change.lease_epoch, change.txn_seq),
                    other => panic!("{other:?}"),
                },
            )
            .collect()
    }
}

fn info(err: &ConnectError) -> ErrorInfo {
    use base64::Engine as _;
    use buffa::Message as _;
    let detail = err
        .details
        .iter()
        .find(|d| d.type_url == "loams.errors.v1.ErrorInfo")
        .unwrap_or_else(|| panic!("no ErrorInfo in {err:?}"));
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(detail.value.as_deref().unwrap_or_default())
        .expect("unpadded base64");
    ErrorInfo::decode_from_slice(&bytes).expect("an ErrorInfo")
}

async fn create(admin: &GraphAdmin, name: &str) {
    admin
        .create_graph(pb::CreateGraphRequest {
            namespace: "acme".to_string(),
            name: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("create");
}

fn request(graph: &str, statement: &str, key: &str) -> pb::ExecuteRequest {
    pb::ExecuteRequest {
        namespace: "acme".to_string(),
        graph: graph.to_string(),
        statement: statement.to_string(),
        idempotency_key: key.to_string(),
        ..Default::default()
    }
}

async fn run(admin: &GraphAdmin, graph: &str, statement: &str) -> pb::ExecuteResponse {
    admin
        .execute(request(graph, statement, ""))
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err:?}"))
}

fn token(response: &pb::ExecuteResponse) -> ConsistencyToken {
    response
        .consistency_token
        .parse()
        .unwrap_or_else(|err| panic!("{:?}: {err}", response.consistency_token))
}

/// `k → v` of every `:N` node, read through `admin`.
async fn values(admin: &GraphAdmin, graph: &str) -> BTreeMap<i64, String> {
    run(admin, graph, "MATCH (n:N) RETURN n.k, n.v")
        .await
        .rows
        .as_option()
        .expect("rows")
        .rows
        .iter()
        .map(|row| match (&row.values[0].kind, &row.values[1].kind) {
            (Some(Kind::Int64(k)), Some(Kind::String(v))) => (*k, v.clone()),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Whether `admin`'s engine has the graph open.
fn is_open(admin: &GraphAdmin, graph: &str) -> bool {
    admin
        .engine()
        .list(Some("acme"))
        .expect("list")
        .iter()
        .any(|g| g.name() == graph)
}

/// Waits until the metastore clock is past `graph`'s lease deadline.
async fn wait_out_the_lease(cluster: &Cluster, graph: GraphId) {
    let lease = cluster
        .meta
        .lease(Consistency::Linearizable, &owner_lease_key(graph))
        .await
        .expect("read")
        .expect("a lease");
    while cluster.meta.now_ms() <= lease.deadline_ms {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_one_writer() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let owner = cluster.preferred(graph);

    // Both nodes take writes at once; each is keyed, so a retry would be safe.
    let mut tasks = Vec::new();
    for i in 0..16_i64 {
        let admin = cluster
            .node(1 + u64::try_from(i % 2).expect("0 or 1"))
            .admin
            .clone();
        tasks.push(tokio::spawn(async move {
            // Sixteen at once can pass the namespace's statement cap (an
            // eighth of the process slots is kept back, so a small runner
            // admits fewer): the refusal says retry, and the key makes it safe.
            let mut attempt = 0;
            loop {
                match admin
                    .execute(request(
                        "kg",
                        &format!("INSERT (:N {{k: {i}, v: 'w{i}'}})"),
                        &format!("w{i}"),
                    ))
                    .await
                {
                    Err(err) if err.code == ErrorCode::ResourceExhausted && attempt < 100 => {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    other => break other.expect("a write through either node"),
                }
            }
        }));
    }
    let mut offsets = Vec::new();
    for task in tasks {
        offsets.push(token(&task.await.expect("join")).offset);
    }
    offsets.sort_unstable();
    assert_eq!(
        offsets,
        (1..=16).collect::<Vec<u64>>(),
        "one token per record"
    );

    // One writer: one epoch on every record, `txn_seq` dense, and one engine.
    let stream = cluster.stream_of("kg").await;
    let epochs = cluster.epochs(stream).await;
    assert_eq!(epochs.len(), 16);
    let epoch = epochs[0].0;
    assert!(epoch >= 1);
    assert!(epochs.iter().all(|(e, _)| *e == epoch), "{epochs:?}");
    assert_eq!(
        epochs.iter().map(|(_, seq)| *seq).collect::<Vec<_>>(),
        (1..=16).collect::<Vec<u64>>()
    );
    let other = 3 - owner;
    assert!(is_open(&cluster.node(owner).admin, "kg"));
    assert!(!is_open(&cluster.node(other).admin, "kg"));
    assert_eq!(
        cluster.node(owner).ownership.held(graph),
        Some(epoch),
        "the owner holds the lease at the records' epoch"
    );
    assert_eq!(cluster.node(other).ownership.held(graph), None);
    // Reads through either node see every write.
    assert_eq!(values(&cluster.node(other).admin, "kg").await.len(), 16);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_write_answers_owner_token() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let owner = cluster.preferred(graph);
    let other = 3 - owner;
    run(
        &cluster.node(owner).admin,
        "kg",
        "INSERT (:N {k: 1, v: 'a'})",
    )
    .await;

    let forwarded = run(
        &cluster.node(other).admin,
        "kg",
        "INSERT (:N {k: 2, v: 'b'})",
    )
    .await;
    let owner_graph = cluster
        .node(owner)
        .admin
        .engine()
        .list(Some("acme"))
        .expect("list")
        .into_iter()
        .find(|g| g.name() == "kg")
        .expect("open on the owner");
    // The answer is the owner's: its token names the owner's durable state.
    assert_eq!(Some(token(&forwarded)), owner_graph.consistency_token());
    assert_eq!(token(&forwarded).offset, 2);
    assert!(forwarded.commit_epoch > 0);
    assert!(
        !is_open(&cluster.node(other).admin, "kg"),
        "the other node never opens it"
    );

    // Every data-plane call is forwarded the same way.
    let batch = cluster
        .node(other)
        .admin
        .execute_batch(pb::ExecuteBatchRequest {
            namespace: "acme".to_string(),
            graph: "kg".to_string(),
            statements: vec![pb::Statement {
                statement: "INSERT (:N {k: 3, v: 'c'})".to_string(),
                ..Default::default()
            }],
            atomic: true,
            ..Default::default()
        })
        .await
        .expect("a forwarded batch");
    let batch_token: ConsistencyToken = batch.consistency_token.parse().expect("a token");
    assert_eq!(batch_token.offset, 3);
    let schema = cluster
        .node(other)
        .admin
        .get_schema(pb::GetSchemaRequest {
            namespace: "acme".to_string(),
            name: "kg".to_string(),
            ..Default::default()
        })
        .await
        .expect("a forwarded GetSchema");
    assert_eq!(schema.labels.first().map(|l| l.count), Some(3));
    let plan = cluster
        .node(other)
        .admin
        .explain(pb::ExplainRequest {
            namespace: "acme".to_string(),
            graph: "kg".to_string(),
            statement: "MATCH (n:N) RETURN n.k".to_string(),
            ..Default::default()
        })
        .await
        .expect("a forwarded Explain");
    assert!(!plan.text.is_empty());
    let stream = cluster
        .node(other)
        .admin
        .execute_stream(pb::ExecuteStreamRequest {
            request: request("kg", "MATCH (n:N) RETURN n.k ORDER BY n.k", "").into(),
            chunk_rows: 1,
            ..Default::default()
        })
        .await
        .expect("a forwarded stream");
    use futures::StreamExt as _;
    let chunks: Vec<pb::ResultChunk> = stream.map(|c| c.expect("chunk")).collect().await;
    assert_eq!(chunks.iter().map(|c| c.rows.len()).sum::<usize>(), 3);
    assert!(chunks.last().is_some_and(|c| c.last));
    assert!(!is_open(&cluster.node(other).admin, "kg"));
}

/// Review Focus 2: partition the owner, let another node take the lease, and release the old
/// owner's pending append. The metastore refuses it by its fence; nothing acknowledged is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_owner_append_is_fenced() {
    let cluster = Cluster::start(2, SHORT).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let old = cluster.preferred(graph);
    let new = 3 - old;
    let acked = run(
        &cluster.node(old).admin,
        "kg",
        "INSERT (:N {k: 1, v: 'kept'})",
    )
    .await;
    let first_epoch = cluster.node(old).ownership.held(graph).expect("held");

    // The old owner's next append is held in its log...
    let (reached_tx, reached) = tokio::sync::oneshot::channel::<()>();
    let reached_tx = Arc::new(Mutex::new(Some(reached_tx)));
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let (reached_tx, release) = (reached_tx.clone(), release.clone());
        cluster
            .node(old)
            .log
            .set_append_fault(Some(Arc::new(move || {
                let (reached_tx, release) = (reached_tx.clone(), release.clone());
                Box::pin(async move {
                    let first = reached_tx.lock().expect("lock").take();
                    if let Some(tx) = first {
                        let _ = tx.send(());
                        release.notified().await;
                    }
                    Ok(())
                })
            })));
    }
    let stale = {
        let admin = cluster.node(old).admin.clone();
        tokio::spawn(async move {
            admin
                .execute(request("kg", "INSERT (:N {k: 2, v: 'stale'})", ""))
                .await
        })
    };
    reached.await.expect("the stale append is pending");

    // ...and the old owner is cut off: it renews nothing, the others no longer see it, and
    // nobody can reach it.
    cluster.node(old).ownership.pause_renewals(true);
    let cut = cluster.forwarder.cut_off(old).expect("connected");
    cluster
        .node(new)
        .membership
        .set(vec![cluster.node(new).me.clone()]);
    wait_out_the_lease(&cluster, graph).await;

    // The new owner takes the lease at a newer epoch and serves every acknowledged write.
    let taken = run(
        &cluster.node(new).admin,
        "kg",
        "INSERT (:N {k: 3, v: 'new'})",
    )
    .await;
    let second_epoch = cluster.node(new).ownership.held(graph).expect("held");
    assert!(second_epoch > first_epoch);
    assert_eq!(token(&taken).offset, token(&acked).offset + 1);

    // The old owner's append goes out under its old epoch now: the metastore refuses it.
    release.notify_one();
    let refused = stale
        .await
        .expect("join")
        .expect_err("a fenced write is not acknowledged");
    assert_eq!(refused.code, ErrorCode::Unavailable);
    assert_eq!(info(&refused).reason, "graph_reloading");
    let epochs = cluster.epochs(cluster.stream_of("kg").await).await;
    assert_eq!(
        epochs.iter().map(|(epoch, _)| *epoch).collect::<Vec<_>>(),
        vec![first_epoch, second_epoch],
        "the stale write never reached the log"
    );

    // Back on the network, the old owner forwards to the new one and never serves its stale
    // engine again.
    cluster.node(old).log.set_append_fault(None);
    cluster.forwarder.reconnect(old, cut);
    let seen = values(&cluster.node(old).admin, "kg").await;
    assert_eq!(
        seen,
        BTreeMap::from([(1, "kept".to_string()), (3, "new".to_string())])
    );
    assert!(!is_open(&cluster.node(old).admin, "kg"));
    assert_eq!(cluster.node(old).ownership.held(graph), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_moves_on_node_leave() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let old = cluster.preferred(graph);
    let new = 3 - old;
    run(&cluster.node(old).admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    let first_epoch = cluster.node(old).ownership.held(graph).expect("held");

    // A graceful leave: the node stops serving and gives its leases up.
    cluster
        .node(old)
        .admin
        .shutdown(Duration::from_secs(5))
        .await;
    cluster.forwarder.cut_off(old);
    cluster
        .node(new)
        .membership
        .set(vec![cluster.node(new).me.clone()]);
    let lease = cluster
        .meta
        .lease(Consistency::Linearizable, &owner_lease_key(graph))
        .await
        .expect("read")
        .expect("a lease");
    assert_eq!(lease.owner, None, "released, not left to expire");

    // The next node takes it at once (no TTL wait) and opens it from the stream.
    let started = std::time::Instant::now();
    let moved = run(&cluster.node(new).admin, "kg", "INSERT (:N {k: 2, v: 'b'})").await;
    assert!(started.elapsed() < OwnerConfig::default().ttl);
    assert_eq!(token(&moved).offset, 2);
    assert!(cluster.node(new).ownership.held(graph).expect("held") > first_epoch);
    assert_eq!(
        values(&cluster.node(new).admin, "kg").await,
        BTreeMap::from([(1, "a".to_string()), (2, "b".to_string())])
    );
}

/// Fault injection for forwarding: an owner nobody can reach is `UNAVAILABLE` (retryable) and
/// nothing runs; once its lease is out and it has left, the call is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_owner_is_unavailable_until_its_lease_moves() {
    let cluster = Cluster::start(2, SHORT).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let old = cluster.preferred(graph);
    let new = 3 - old;
    run(&cluster.node(old).admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;

    cluster.forwarder.cut_off(old);
    let err = cluster
        .node(new)
        .admin
        .execute(request("kg", "INSERT (:N {k: 2, v: 'b'})", "k2"))
        .await
        .expect_err("the owner is unreachable");
    assert_eq!(err.code, ErrorCode::Unavailable);
    assert!(!is_open(&cluster.node(new).admin, "kg"));

    // The owner dies: its lease runs out and the registry drops it.
    cluster.node(old).ownership.pause_renewals(true);
    cluster
        .node(new)
        .membership
        .set(vec![cluster.node(new).me.clone()]);
    wait_out_the_lease(&cluster, graph).await;
    // The client's retry, with its key, is served by the new owner, once.
    cluster
        .node(new)
        .admin
        .execute(request("kg", "INSERT (:N {k: 2, v: 'b'})", "k2"))
        .await
        .expect("served by the new owner");
    assert_eq!(values(&cluster.node(new).admin, "kg").await.len(), 2);
}

/// A forwarded call reaching a node that is not the owner is refused, never sent on: two nodes
/// with different views cannot bounce a call between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_call_is_never_forwarded_again() {
    use buffa::Message as _;
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let owner = cluster.preferred(graph);
    let other = 3 - owner;
    run(
        &cluster.node(owner).admin,
        "kg",
        "INSERT (:N {k: 1, v: 'a'})",
    )
    .await;
    let err = cluster
        .node(other)
        .admin
        .serve_forwarded(
            ForwardMethod::Execute,
            request("kg", "INSERT (:N {k: 2, v: 'b'})", "").encode_to_bytes(),
        )
        .await
        .expect_err("not the owner");
    assert_eq!(err.code, ErrorCode::Unavailable);
    assert_eq!(info(&err).reason, "graph_reloading");
    assert!(!is_open(&cluster.node(other).admin, "kg"));
    assert_eq!(values(&cluster.node(owner).admin, "kg").await.len(), 1);
}

/// R11.8's residual race, closed: an append whose outcome is unknown (its WAL object is slow to
/// write) makes the graph reload; the reload takes a fresh epoch before it reads the stream, so
/// when the old append's commit finally lands it is refused, and the stream never holds a
/// record the rebuilt engine did not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_commit_after_a_reload_is_fenced() {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    let group = GroupCommit {
        append_timeout: Duration::from_millis(300),
        ..GroupCommit::default()
    };
    let cluster =
        Cluster::start_on(1, OwnerConfig::default(), Store::new(faulty.clone()), group).await;
    let admin = &cluster.node(1).admin;
    create(admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    run(admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    let first_epoch = cluster.node(1).ownership.held(graph).expect("held");

    // The next WAL object takes 2 s to write: the lane gives up on it after 300 ms.
    faulty.inject(Op::PutCreate, Fault::Delay(Duration::from_secs(2)));
    let err = admin
        .execute(request("kg", "INSERT (:N {k: 2, v: 'late'})", ""))
        .await
        .expect_err("an unknown outcome is not acknowledged");
    assert_eq!(info(&err).reason, "graph_reloading");
    // The next statement reloads the graph under a fresh epoch, before the object is written.
    assert_eq!(values(admin, "kg").await.len(), 1);
    let second_epoch = cluster.node(1).ownership.held(graph).expect("held");
    assert!(second_epoch > first_epoch);

    // The late commit lands under the old epoch, and is refused.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let stream = cluster.stream_of("kg").await;
    assert_eq!(
        cluster.epochs(stream).await,
        vec![(first_epoch, 1)],
        "the late append never reached the log"
    );
    let next = run(admin, "kg", "INSERT (:N {k: 3, v: 'c'})").await;
    assert_eq!(token(&next).offset, 2);
    assert_eq!(
        values(admin, "kg").await,
        BTreeMap::from([(1, "a".to_string()), (3, "c".to_string())])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_streams_have_the_reserved_name_and_old_ones_move_to_it() {
    let cluster = Cluster::start(1, OwnerConfig::default()).await;
    let admin = &cluster.node(1).admin;
    create(admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let ns = cluster
        .meta
        .namespace_by_name(Consistency::Linearizable, "acme")
        .await
        .expect("read")
        .expect("namespace")
        .id;
    let stream = cluster
        .meta
        .stream_by_name(
            Consistency::Linearizable,
            ns,
            &graph_stream_name(&graph.to_string()),
        )
        .await
        .expect("read")
        .expect("the reserved stream");
    assert_eq!(Some(stream.id), Some(cluster.stream_of("kg").await));

    // A graph from GR1 Task 11, whose stream is `graph.<gr_id>`: its record names that stream.
    let catalog = cluster.catalog();
    let old = catalog
        .create(
            "acme",
            "old",
            NewGraph {
                with_stream: false,
                ..NewGraph::default()
            },
        )
        .await
        .expect("create");
    let legacy = cluster
        .meta
        .create_stream(
            ns,
            &legacy_graph_stream_name(&old.id),
            1,
            WalClass::Standard,
            Retention::default(),
        )
        .await
        .expect("legacy stream");
    catalog
        .set_stream("acme", &old.id, legacy)
        .await
        .expect("record it");
    // Opening it moves the stream to the reserved name, keeping its id; it serves as before.
    run(admin, "old", "INSERT (:N {k: 1, v: 'a'})").await;
    let moved = cluster
        .meta
        .stream(Consistency::Linearizable, legacy)
        .await
        .expect("read")
        .expect("the same stream");
    assert_eq!(moved.name, graph_stream_name(&old.id));
    assert_eq!(cluster.records(legacy).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_drops_the_graphs_stream_under_the_maintenance_lease() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let graph = cluster.graph_id("kg").await;
    let owner = cluster.preferred(graph);
    run(
        &cluster.node(owner).admin,
        "kg",
        "INSERT (:N {k: 1, v: 'a'})",
    )
    .await;
    let stream = cluster.stream_of("kg").await;
    let other = 3 - owner;
    cluster
        .node(other)
        .admin
        .delete_graph(pb::DeleteGraphRequest {
            namespace: "acme".to_string(),
            name: "kg".to_string(),
            ..Default::default()
        })
        .await
        .expect("delete");
    tokio::time::sleep(Duration::from_millis(20)).await;
    // The owner's maintenance closes its engine and gives the lease up; the purge (on whichever
    // node takes the maintenance lease) drops the stream and the record.
    cluster
        .node(owner)
        .admin
        .maintain(Duration::ZERO, Duration::from_secs(600))
        .await
        .expect("maintain");
    assert!(!is_open(&cluster.node(owner).admin, "kg"));
    assert_eq!(cluster.node(owner).ownership.held(graph), None);
    assert_eq!(
        cluster
            .meta
            .stream(Consistency::Linearizable, stream)
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        cluster
            .node(other)
            .admin
            .maintain(Duration::ZERO, Duration::from_secs(600))
            .await
            .expect("maintain again"),
        0
    );
}
