//! The durability fault gate (GR1 Task 16; design §48 §6.1 I1, I2, I4, I5; Review Focus 1).
//!
//! Two in-process nodes share a metastore and a bucket (`common`). Clients run a list-append
//! workload against the node that does not own the graph, so every write is forwarded to the
//! owner. Each key is a `:K` node whose `l` property is the list, as text (`,3,8,11`), appended
//! with GQL's `||`. Every operation goes into an Elle history (`loams-sim`): an acknowledged one
//! as committed, a failed or timed-out append as an unknown outcome.
//!
//! * `kill_matrix_acked_writes_survive`: for each [`KillPoint`] (§48 §6.2's eight steps, the
//!   checkpoint upload, the manifest CAS and the log trim) the owner stops dead there. The other
//!   node takes the graph once the lease is out, recovers it from its snapshot and stream, and
//!   serves on. Every acknowledged append is in the final state, and the history has no
//!   anomaly: no client ever saw a value that was later lost, nor one that never committed.
//! * `bucket_errors_are_retried_not_lost`: the bucket fails and stalls calls at random (and on
//!   cue: a lost WAL object, a stalled one) while writes and checkpoints run; the graph then moves
//!   to the other node, which rebuilds it from the bucket alone.
//! * `metastore_unavailable_stops_writes_not_reads`: with the metastore gone, a write is refused,
//!   and a follower goes on serving `eventual` reads from what it applied.
//!
//! The three take about 80 s together, so they are `#[ignore]`d in the workspace run (every pull
//! request) and run by CI's graph job (`--ignored`), with the heavy suites.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use connectrpc::ErrorCode;
use loams_graph::consistency::Consistency as ReadConsistency;
use loams_graph::durable::GroupCommit;
use loams_graph::durable::faults::{KillHook, KillPoint};
use loams_graph::owner::OwnerConfig;
use loams_graph::recovery::RecoverySource;
use loams_graph::service::admin::GraphAdmin;
use loams_graph::snapshot::{CheckpointPolicy, SnapshotKind};
use loams_proto::loams::graph::v1 as pb;
use loams_sim::elle::{History, Mop, Op, Outcome, check_list_append};
use loams_store::{Fault, FaultRates, FaultyStore, Op as StoreOp, Store};
use pb::__buffa::oneof::value::Kind;
use tokio::sync::watch;

/// Keys of the workload.
const KEYS: u64 = 4;

/// Clients running at once.
const CLIENTS: u64 = 4;

/// How long a client waits for one operation before it counts it as an unknown outcome.
const OP_TIMEOUT: Duration = Duration::from_secs(4);

/// A process number for the final reads in the history.
const CHECKER: u64 = 1_000;

/// Snapshots on demand, and a PITR window of zero (tests only), so each checkpoint trims the
/// stream below it and recovery must start from a snapshot.
fn checkpoints() -> CheckpointPolicy {
    CheckpointPolicy {
        pitr_window: Duration::ZERO,
        ..CheckpointPolicy::default()
    }
}

fn int(value: i64) -> pb::Value {
    pb::Value {
        kind: Some(Kind::Int64(value)),
        ..Default::default()
    }
}

fn text(value: String) -> pb::Value {
    pb::Value {
        kind: Some(Kind::String(value)),
        ..Default::default()
    }
}

const APPEND: &str = "MATCH (n:K {k: $k}) SET n.l = n.l || $s";
const READ: &str = "MATCH (n:K {k: $k}) RETURN n.l";

fn key_param(key: u64) -> pb::Value {
    int(i64::try_from(key).expect("small"))
}

fn append_statement(key: u64, value: u64) -> pb::Statement {
    let mut statement = pb::Statement {
        statement: APPEND.to_string(),
        ..Default::default()
    };
    statement.parameters.insert("k".to_string(), key_param(key));
    statement
        .parameters
        .insert("s".to_string(), text(format!(",{value}")));
    statement
}

fn read_request(key: u64, consistency: ReadConsistency) -> pb::ExecuteRequest {
    let mut request = request("kg", READ, "");
    request.parameters.insert("k".to_string(), key_param(key));
    request.consistency = consistency.to_proto().into();
    request
}

/// The list a read answered.
fn list_of(response: &pb::ExecuteResponse) -> Option<Vec<u64>> {
    let rows = response.rows.as_option()?;
    match &rows.rows.first()?.values.first()?.kind {
        Some(Kind::String(list)) => Some(
            list.split(',')
                .filter(|v| !v.is_empty())
                .map(|v| v.parse().expect("a value"))
                .collect(),
        ),
        _ => None,
    }
}

/// xorshift64*: the workload's choices, from a seed.
fn next(state: &mut u64) -> u64 {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    state.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

/// The history of one run, shared by its clients.
#[derive(Default)]
struct Recorder {
    history: Mutex<History>,
    values: AtomicU64,
    acked: AtomicU64,
}

impl Recorder {
    fn push(&self, op: Op) {
        if op.outcome == Outcome::Ok {
            self.acked.fetch_add(1, Ordering::SeqCst);
        }
        self.history.lock().expect("lock").push(op);
    }

    fn value(&self) -> u64 {
        self.values.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn acked(&self) -> u64 {
        self.acked.load(Ordering::SeqCst)
    }
}

/// One operation of client `process` through `admin`: half single appends, a fifth atomic
/// two-key appends, the rest reads. Answers whether it succeeded.
async fn one_op(admin: &GraphAdmin, process: u64, recorder: &Recorder, rng: &mut u64) -> bool {
    let key = next(rng) % KEYS;
    match next(rng) % 10 {
        0..=4 => {
            let value = recorder.value();
            let statement = append_statement(key, value);
            let mut request = request("kg", APPEND, "");
            request.parameters = statement.parameters;
            let answer = tokio::time::timeout(OP_TIMEOUT, admin.execute(request)).await;
            let mops = vec![Mop::append(key, value)];
            let ok = matches!(answer, Ok(Ok(_)));
            recorder.push(if ok {
                Op::ok(process, mops)
            } else {
                Op::info(process, mops)
            });
            ok
        }
        5..=6 => {
            let other = (key + 1) % KEYS;
            let (first, second) = (recorder.value(), recorder.value());
            let batch = pb::ExecuteBatchRequest {
                namespace: "acme".to_string(),
                graph: "kg".to_string(),
                statements: vec![
                    append_statement(key, first),
                    append_statement(other, second),
                ],
                atomic: true,
                ..Default::default()
            };
            let answer = tokio::time::timeout(OP_TIMEOUT, admin.execute_batch(batch)).await;
            let mops = vec![Mop::append(key, first), Mop::append(other, second)];
            let ok = matches!(answer, Ok(Ok(_)));
            recorder.push(if ok {
                Op::ok(process, mops)
            } else {
                Op::info(process, mops)
            });
            ok
        }
        _ => {
            let answer = tokio::time::timeout(
                OP_TIMEOUT,
                admin.execute(read_request(key, ReadConsistency::Strong)),
            )
            .await;
            match answer {
                Ok(Ok(response)) => {
                    let list = list_of(&response).expect("a key's list");
                    recorder.push(Op::ok(process, vec![Mop::read(key, list)]));
                    true
                }
                _ => {
                    recorder.push(Op::fail(process, vec![Mop::read_unknown(key)]));
                    false
                }
            }
        }
    }
}

/// Clients running the workload through `admin` until `stop`.
fn start_clients(
    admin: &GraphAdmin,
    recorder: &Arc<Recorder>,
    stop: &Arc<AtomicBool>,
    seed: u64,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..CLIENTS)
        .map(|process| {
            let (admin, recorder, stop) = (admin.clone(), recorder.clone(), stop.clone());
            let mut rng = seed ^ (process + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    if !one_op(&admin, process, &recorder, &mut rng).await {
                        // Not a busy loop while the graph has no owner.
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            })
        })
        .collect()
}

/// Stops the clients and waits for them.
async fn stop_clients(stop: &AtomicBool, clients: Vec<tokio::task::JoinHandle<()>>) {
    stop.store(true, Ordering::SeqCst);
    for client in clients {
        tokio::time::timeout(OP_TIMEOUT * 2, client)
            .await
            .expect("a client ends")
            .expect("a client");
    }
}

/// Waits until `recorder` has `count` more acknowledged operations than `from`, at most 30 s.
async fn until_acked(recorder: &Recorder, from: u64, count: u64, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while recorder.acked() < from + count {
        assert!(
            std::time::Instant::now() < deadline,
            "{what}: only {} of {count} operations acknowledged",
            recorder.acked() - from
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Creates graph `kg` and its keys (keyed, so a retry after a fault is safe).
async fn create_keys(admin: &GraphAdmin, replicas: u32) {
    retry("create", || {
        admin.create_graph(pb::CreateGraphRequest {
            namespace: "acme".to_string(),
            name: "kg".to_string(),
            replicas,
            idempotency_key: "create-kg".to_string(),
            ..Default::default()
        })
    })
    .await;
    for key in 0..KEYS {
        retry("a key", || {
            admin.execute(request(
                "kg",
                &format!("INSERT (:K {{k: {key}, l: ''}})"),
                &format!("key-{key}"),
            ))
        })
        .await;
    }
}

/// `call` until it succeeds, at most 50 times.
async fn retry<T, F: std::future::Future<Output = Result<T, connectrpc::ConnectError>>>(
    what: &str,
    mut call: impl FnMut() -> F,
) -> T {
    let mut last = None;
    for _ in 0..50 {
        match call().await {
            Ok(answer) => return answer,
            Err(err) => last = Some(err),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what}: {last:?}");
}

/// Every key's list, read `strong` through `admin`, retried while the graph moves.
async fn final_lists(admin: &GraphAdmin) -> BTreeMap<u64, Vec<u64>> {
    let mut lists = BTreeMap::new();
    for key in 0..KEYS {
        let response = retry("a final read", || {
            admin.execute(read_request(key, ReadConsistency::Strong))
        })
        .await;
        lists.insert(key, list_of(&response).expect("a key's list"));
    }
    lists
}

/// The checks of a finished run: every acknowledged append is in the final lists, and the
/// history (with the final reads) has no anomaly.
fn check(case: &str, recorder: &Recorder, finals: &BTreeMap<u64, Vec<u64>>) {
    let mut history = recorder.history.lock().expect("lock").clone();
    let mut acked = 0;
    for op in history.ops.iter().filter(|op| op.outcome == Outcome::Ok) {
        for mop in &op.mops {
            if let Mop::Append { key, value } = mop {
                acked += 1;
                assert!(
                    finals[key].contains(value),
                    "{case}: acknowledged append {value} to key {key} is lost: {:?}",
                    finals[key]
                );
            }
        }
    }
    assert!(acked > 0, "{case}: nothing was acknowledged");
    history.push(Op::ok(
        CHECKER,
        finals
            .iter()
            .map(|(key, list)| Mop::read(*key, list.clone()))
            .collect(),
    ));
    if let Err(anomaly) = check_list_append(&history) {
        panic!("{case}: {anomaly:?}");
    }
}

/// A hook that stops the node at the `occurrence`th time it reaches `point`: it says so on
/// `reached` and waits there until `release`.
fn kill_at(
    point: KillPoint,
    occurrence: u64,
    reached: watch::Sender<bool>,
    release: watch::Receiver<bool>,
) -> KillHook {
    let seen = Arc::new(AtomicU64::new(0));
    let reached = Arc::new(reached);
    Arc::new(move |at| {
        let stop = at == point && seen.fetch_add(1, Ordering::SeqCst) + 1 == occurrence;
        let (reached, mut release) = (reached.clone(), release.clone());
        Box::pin(async move {
            if stop {
                let _ = reached.send(true);
                let _ = release.wait_for(|released| *released).await;
            }
        })
    })
}

/// The node dies: it renews nothing, nobody reaches it, and the other node sees only itself.
fn kill(cluster: &Cluster, victim: u64, survivor: u64) {
    cluster.node(victim).ownership.pause_renewals(true);
    cluster.forwarder.cut_off(victim);
    cluster
        .node(survivor)
        .membership
        .set(vec![cluster.node(survivor).me.clone()]);
}

/// One case of the kill matrix: the owner dies at `point`.
async fn kill_case(point: KillPoint, seed: u64) {
    let case = format!("{point:?}");
    let cluster = Cluster::start_with(
        2,
        SHORT,
        Store::in_memory(),
        GroupCommit::default(),
        Some(checkpoints()),
    )
    .await;
    create_keys(&cluster.node(1).admin, 0).await;
    let graph = cluster.graph_id("kg").await;
    let victim = cluster.preferred(graph);
    let survivor = 3 - victim;
    let owner = cluster.node(victim).admin.clone();
    // A snapshot to recover from: the survivor starts from it, not from offset 0.
    owner
        .checkpoint(
            "acme",
            "kg",
            &[SnapshotKind::Native, SnapshotKind::Portable],
        )
        .await
        .expect("a first checkpoint");

    let (reached_tx, mut reached) = watch::channel(false);
    let (release, released) = watch::channel(false);
    // A write point stops the fourth time it is reached, a checkpoint point the first time
    // (the checkpoints below).
    let occurrence = if point.in_checkpoint() { 1 } else { 4 };
    cluster
        .node(victim)
        .log
        .set_kill_hook(Some(kill_at(point, occurrence, reached_tx, released)));
    let recorder = Arc::new(Recorder::default());
    let stop = Arc::new(AtomicBool::new(false));
    let clients = start_clients(&cluster.node(survivor).admin, &recorder, &stop, seed);
    let checkpointing = point.in_checkpoint().then(|| {
        let (owner, reached) = (owner.clone(), reached.clone());
        tokio::spawn(async move {
            while !*reached.borrow() {
                tokio::time::sleep(Duration::from_millis(150)).await;
                let _ = owner
                    .checkpoint(
                        "acme",
                        "kg",
                        &[SnapshotKind::Native, SnapshotKind::Portable],
                    )
                    .await;
            }
        })
    });

    tokio::time::timeout(Duration::from_secs(30), reached.wait_for(|r| *r))
        .await
        .unwrap_or_else(|_| panic!("{case}: the kill point was never reached"))
        .expect("the hook");
    kill(&cluster, victim, survivor);
    // The survivor takes the graph once the lease is out, recovers it, and serves on.
    until_acked(&recorder, recorder.acked(), 24, &case).await;
    stop_clients(&stop, clients).await;
    let finals = final_lists(&cluster.node(survivor).admin).await;
    let recovered = cluster
        .node(survivor)
        .admin
        .engine()
        .list(Some("acme"))
        .expect("list")
        .into_iter()
        .find(|g| g.name() == "kg")
        .and_then(|g| g.recovery())
        .unwrap_or_else(|| panic!("{case}: the survivor never opened the graph"));
    assert!(
        matches!(recovered.source, RecoverySource::Snapshot(_)),
        "{case}: recovered from {:?}",
        recovered.source
    );
    assert!(cluster.node(survivor).ownership.held(graph).is_some());

    // The dead node comes back to finish what it was doing: its lease is gone, so nothing of it
    // lands.
    let _ = release.send(true);
    if let Some(task) = checkpointing {
        tokio::time::timeout(Duration::from_secs(20), task)
            .await
            .unwrap_or_else(|_| panic!("{case}: the dead node's checkpoint never ended"))
            .expect("the checkpoint task");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        final_lists(&cluster.node(survivor).admin).await,
        finals,
        "{case}: the dead node changed the graph after its lease moved"
    );
    check(&case, &recorder, &finals);
}

/// Review Focus 1: the owner dies at each step of the write path and of a checkpoint; another
/// node recovers the graph from its snapshot and stream. No acknowledged write is lost, and no
/// client ever observed a value that was later lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the durability fault gate: CI's heavy graph job runs it with --ignored (GR1 Task 16)"]
async fn kill_matrix_acked_writes_survive() {
    for (n, point) in KillPoint::ALL.into_iter().enumerate() {
        kill_case(point, 0x5eed_0016 + n as u64).await;
    }
}

/// The bucket fails calls at random (errors, applied-then-failed calls and delays), and on cue
/// a WAL object is lost and another stalls past the append timeout, while writes and checkpoints
/// run. A write either is acknowledged and survives or is not acknowledged; checkpoints that
/// fail are retried by the next. The graph then moves to the other node, which rebuilds it from
/// the bucket and the stream alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the durability fault gate: CI's heavy graph job runs it with --ignored (GR1 Task 16)"]
async fn bucket_errors_are_retried_not_lost() {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    let group = GroupCommit {
        append_timeout: Duration::from_millis(500),
        ..GroupCommit::default()
    };
    let cluster = Cluster::start_with(
        2,
        SHORT,
        Store::new(faulty.clone()),
        group,
        Some(checkpoints()),
    )
    .await;
    create_keys(&cluster.node(1).admin, 0).await;
    let graph = cluster.graph_id("kg").await;
    let owner_node = cluster.preferred(graph);
    let other = 3 - owner_node;
    let owner = cluster.node(owner_node).admin.clone();

    faulty.set_rates(FaultRates {
        error: 0.05,
        error_after_apply: 0.02,
        delay: 0.1,
        max_delay: Duration::from_millis(40),
        ..FaultRates::none()
    });
    faulty.inject(StoreOp::PutCreate, Fault::Error);
    faulty.inject(StoreOp::PutCreate, Fault::Delay(Duration::from_secs(1)));
    let recorder = Arc::new(Recorder::default());
    let stop = Arc::new(AtomicBool::new(false));
    let clients = start_clients(&owner, &recorder, &stop, 0x5eed_b0c7);
    let (mut taken, mut failed) = (0, 0);
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        match owner
            .checkpoint(
                "acme",
                "kg",
                &[SnapshotKind::Native, SnapshotKind::Portable],
            )
            .await
        {
            Ok(_) => taken += 1,
            Err(_) => failed += 1,
        }
    }
    until_acked(&recorder, 0, 40, "bucket faults").await;
    stop_clients(&stop, clients).await;
    faulty.set_rates(FaultRates::none());
    faulty.clear();
    // One clean checkpoint, so the other node has a snapshot taken after every fault.
    retry("a clean checkpoint", || {
        owner.checkpoint(
            "acme",
            "kg",
            &[SnapshotKind::Native, SnapshotKind::Portable],
        )
    })
    .await;
    assert!(taken + failed > 0, "no checkpoint ran");
    // The faults did reach the workload: the lost WAL object at least left an append unknown.
    let unknown = recorder
        .history
        .lock()
        .expect("lock")
        .ops
        .iter()
        .filter(|op| op.outcome == Outcome::Info)
        .count();
    assert!(unknown > 0, "no write saw a bucket fault");

    // The owner leaves; the other node rebuilds the graph from the bucket and the stream.
    owner.shutdown(Duration::from_secs(5)).await;
    cluster.forwarder.cut_off(owner_node);
    cluster
        .node(other)
        .membership
        .set(vec![cluster.node(other).me.clone()]);
    let finals = final_lists(&cluster.node(other).admin).await;
    let recovered = cluster
        .node(other)
        .admin
        .engine()
        .list(Some("acme"))
        .expect("list")
        .into_iter()
        .find(|g| g.name() == "kg")
        .and_then(|g| g.recovery())
        .expect("recovered on the other node");
    assert!(
        matches!(recovered.source, RecoverySource::Snapshot(_)),
        "{:?}",
        recovered.source
    );
    check("bucket faults", &recorder, &finals);
}

/// With the metastore gone, a write is refused (it cannot be made durable) and a `strong` read
/// on a node that does not own the graph cannot reach its owner; a follower goes on serving
/// `eventual` reads from what it applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the durability fault gate: CI's heavy graph job runs it with --ignored (GR1 Task 16)"]
async fn metastore_unavailable_stops_writes_not_reads() {
    let group = GroupCommit {
        append_timeout: Duration::from_secs(1),
        ..GroupCommit::default()
    };
    let cluster =
        Cluster::start_with(2, OwnerConfig::default(), Store::in_memory(), group, None).await;
    create_keys(&cluster.node(1).admin, 1).await;
    let graph = cluster.graph_id("kg").await;
    let owner = cluster.preferred(graph);
    let follower = 3 - owner;
    for value in 1..=3 {
        cluster
            .node(owner)
            .admin
            .execute({
                let statement = append_statement(0, value);
                let mut request = request("kg", APPEND, "");
                request.parameters = statement.parameters;
                request
            })
            .await
            .expect("a write");
    }
    let eventual = |admin: GraphAdmin| async move {
        admin
            .execute(read_request(0, ReadConsistency::Eventual))
            .await
            .map(|response| list_of(&response).expect("a list"))
    };
    // The follower opens and catches up.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = eventual(cluster.node(follower).admin.clone()).await;
        if seen.as_deref().ok() == Some(&[1, 2, 3][..]) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{seen:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cluster
        .meta_node
        .shutdown()
        .await
        .expect("the metastore stops");

    let write = tokio::time::timeout(
        Duration::from_secs(30),
        cluster.node(owner).admin.execute({
            let statement = append_statement(0, 4);
            let mut request = request("kg", APPEND, "");
            request.parameters = statement.parameters;
            request
        }),
    )
    .await
    .expect("the write is answered")
    .expect_err("no write without the metastore");
    assert_eq!(write.code, ErrorCode::Unavailable, "{write:?}");

    for _ in 0..5 {
        let seen = eventual(cluster.node(follower).admin.clone())
            .await
            .expect("an eventual read on the follower");
        assert_eq!(seen, vec![1, 2, 3]);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let strong = tokio::time::timeout(
        Duration::from_secs(30),
        cluster
            .node(follower)
            .admin
            .execute(read_request(0, ReadConsistency::Strong)),
    )
    .await
    .expect("the strong read is answered")
    .expect_err("a strong read needs the owner");
    assert_eq!(strong.code, ErrorCode::Unavailable, "{strong:?}");
}
