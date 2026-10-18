//! Placement, sequencer ownership and forwarding (plan GT1 Task 14; the
//! morning version's Task 16): two nodes' `RepoSequencers` over one bucket,
//! with an in-process lease table, placement and forwarder.

use std::sync::Arc;
use std::time::{Duration, Instant};

use loams_git::testing::{
    LoopbackForwarder, MemLeaseTable, SetPlacement, TEST_NS, ensure_repo, test_paths, test_repo,
};
use loams_git::{
    Cluster, Expect, GitEvent, IdemKey, ObjectId, OwnerConfig, Principal, ReadAt, RefError, RefLog,
    RefName, RefTxn, RefUpdate, RepoSequencers, SeqOwner, StoreWalStore, SystemClock,
    seq_lease_key,
};
use loams_store::Store;

const TENANT: &str = "local/1";

fn oid(n: u32) -> ObjectId {
    let mut bytes = [0x44u8; 20];
    bytes[16..].copy_from_slice(&n.to_be_bytes());
    ObjectId::try_from(&bytes[..]).expect("oid")
}

/// A create of `refs/heads/<branch>` at `oid(n)`, under key `key`.
fn create(key: &str, branch: &str, n: u32) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [n as u8; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/{branch}")).expect("ref"),
            expect: Expect::Absent,
            new: Some(oid(n)),
        }],
        packs: Vec::new(),
        principal: Principal::parse("tester").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

fn config(idle_evict: Duration) -> OwnerConfig {
    OwnerConfig {
        lease_ttl: Duration::from_millis(600),
        renew_every: Duration::from_millis(50),
        forward_timeout: Duration::from_millis(500),
        idle_evict,
    }
}

/// Node `a` (address `a:1`, id 1) and node `b` (address `b:1`, id 2) over
/// one in-memory bucket holding the test repository.
struct Nodes {
    store: Store,
    table: Arc<MemLeaseTable>,
    forwarder: Arc<LoopbackForwarder>,
    a: RepoSequencers,
    a_view: Arc<SetPlacement>,
    b: RepoSequencers,
    b_view: Arc<SetPlacement>,
}

fn there(node_id: u64) -> SeqOwner {
    SeqOwner::There {
        node_id,
        addr: if node_id == 1 { "a:1" } else { "b:1" }.to_string(),
    }
}

async fn nodes(idle_evict: Duration) -> Nodes {
    let store = Store::in_memory();
    ensure_repo(&StoreWalStore::new(store.clone(), test_paths()))
        .await
        .expect("create");
    let table = Arc::new(MemLeaseTable::default());
    let forwarder = Arc::new(LoopbackForwarder::default());
    // Both nodes agree that `a` owns the repository.
    let a_view = SetPlacement::new(SeqOwner::Here);
    let b_view = SetPlacement::new(there(1));
    let node = |view: &Arc<SetPlacement>, owner: &str| {
        RepoSequencers::clustered(
            store.clone(),
            Arc::new(SystemClock),
            Cluster {
                placement: view.clone(),
                leases: table.client(owner),
                forwarder: forwarder.clone(),
                config: config(idle_evict),
            },
        )
    };
    let a = node(&a_view, "v1;1;a");
    let b = node(&b_view, "v1;2;b");
    forwarder.register("a:1", a.clone());
    forwarder.register("b:1", b.clone());
    Nodes {
        store,
        table,
        forwarder,
        a,
        a_view,
        b,
        b_view,
    }
}

async fn tail(n: &Nodes) -> Arc<loams_git::RefSnapshot> {
    let state = loams_git::latest_state(&StoreWalStore::new(n.store.clone(), test_paths()))
        .await
        .expect("read");
    Arc::new(state.snapshot())
}

/// Polls `check` until it holds, for at most 5 s.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn owner_runs_the_only_leased_sequencer() {
    let n = nodes(Duration::from_secs(60)).await;
    let repo = test_repo();
    for i in 0..5 {
        n.a.commit(
            TEST_NS,
            TENANT,
            &repo,
            create(&format!("a{i}"), &format!("a{i}"), i),
        )
        .await
        .expect("commit on a");
        n.b.commit(
            TEST_NS,
            TENANT,
            &repo,
            create(&format!("b{i}"), &format!("b{i}"), 10 + i),
        )
        .await
        .expect("commit through b");
    }
    let key = seq_lease_key(TEST_NS, &repo);
    assert_eq!(
        n.table.holder(&key).map(|(owner, _)| owner),
        Some("v1;1;a".into())
    );
    assert_eq!(n.a.leased().await.len(), 1);
    assert!(n.b.leased().await.is_empty(), "b holds no lease");
    assert!(n.b.is_empty().await, "b opened no sequencer");
    assert_eq!(n.a.len().await, 1);
    assert_eq!(n.b.stats().forwarded_out, 5);
    assert_eq!(n.a.stats().forwarded_in, 5);
    assert_eq!(n.b.stats().fallbacks, 0);
    assert!(n.a_view.unreachable().is_empty() && n.b_view.unreachable().is_empty());
    assert_eq!(tail(&n).await.refs.len(), 10);
    n.a.shutdown().await;
    assert_eq!(n.table.holder(&key), None, "shutdown releases the lease");
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarded_push_commits_on_owner() {
    let n = nodes(Duration::from_secs(60)).await;
    let repo = test_repo();
    let txn = create("push-1", "main", 1);
    let receipt =
        n.b.commit(TEST_NS, TENANT, &repo, txn.clone())
            .await
            .expect("forwarded");
    assert!(!receipt.replayed);
    // The owner's sequencer committed it: its state has the ref at that seq.
    let a_log = n.a.get(TEST_NS, TENANT, &repo).await.expect("a's log");
    assert_eq!(a_log.seq(), receipt.seq);
    let snapshot = a_log.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(
        snapshot
            .refs
            .get(&RefName::parse("refs/heads/main").expect("ref")),
        Some(&oid(1))
    );
    // A retry through the forwarder replays the first receipt.
    let again =
        n.b.commit(TEST_NS, TENANT, &repo, txn)
            .await
            .expect("retry");
    assert!(again.replayed);
    assert_eq!(again.seq, receipt.seq);
    // A refusal travels back whole.
    match n
        .b
        .commit(TEST_NS, TENANT, &repo, create("push-2", "main", 2))
        .await
    {
        Err(RefError::Rejected(rejected)) => assert_eq!(rejected.len(), 1),
        other => panic!("{other:?}"),
    }
    // A configuration change is forwarded too.
    n.b.commit_config(
        TEST_NS,
        TENANT,
        &repo,
        IdemKey::parse("cfg-1").expect("key"),
        loams_git::format::pb::ConfigChange {
            symrefs: vec![loams_git::format::pb::SymRef {
                name: "HEAD".into(),
                target: "refs/heads/main".into(),
            }],
            protections: Vec::new(),
            request_digest: Vec::new(),
        },
    )
    .await
    .expect("config");
    assert!(n.b.is_empty().await);
    assert_eq!(n.b.stats().forwarded_out, 4);
    // Garbage on the internal route is refused, not committed.
    let answer = n.a.serve_forwarded(b"not a commit").await;
    assert!(matches!(
        loams_git::forward::decode_answer(&answer).expect("answer"),
        Err(RefError::Invalid { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn owner_down_fallback_is_fenced() {
    let n = nodes(Duration::from_secs(60)).await;
    let repo = test_repo();
    // `a` owns the repository and has committed through its sequencer.
    n.a.commit(TEST_NS, TENANT, &repo, create("a1", "a1", 1))
        .await
        .expect("a1");
    let a_log = n.a.get(TEST_NS, TENANT, &repo).await.expect("a's log");
    let mut a_events = a_log.events();
    // `b` cannot reach `a`: it commits through its own, unleased sequencer.
    n.forwarder.set_down("a:1", true);
    n.b.commit(TEST_NS, TENANT, &repo, create("b1", "b1", 2))
        .await
        .expect("fallback");
    assert_eq!(n.b.stats().fallbacks, 1);
    assert_eq!(n.b_view.unreachable(), vec![1]);
    assert!(n.b.leased().await.is_empty(), "the fallback takes no lease");
    // `a` is alive behind the partition and commits again: its append at
    // the seq `b` took is fenced, and it decides again after `b`'s segment.
    n.a.commit(TEST_NS, TENANT, &repo, create("a2", "a2", 3))
        .await
        .expect("a2");
    let mut fenced = false;
    while let Ok(event) = a_events.try_recv() {
        fenced |= matches!(event, GitEvent::Fenced { .. });
    }
    assert!(fenced, "a's append was not fenced");
    let refs = tail(&n).await.refs.clone();
    for (branch, n) in [("a1", 1), ("b1", 2), ("a2", 3)] {
        assert_eq!(
            refs.get(&RefName::parse(&format!("refs/heads/{branch}")).expect("ref")),
            Some(&oid(n)),
            "{branch}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zombie_owner_is_fenced() {
    let n = nodes(Duration::from_secs(60)).await;
    let repo = test_repo();
    let key = seq_lease_key(TEST_NS, &repo);
    n.a.commit(TEST_NS, TENANT, &repo, create("a1", "a1", 1))
        .await
        .expect("a1");
    let (_, first_epoch) = n.table.holder(&key).expect("a holds the lease");
    let a_log = n.a.get(TEST_NS, TENANT, &repo).await.expect("a's log");
    let mut a_events = a_log.events();
    // `a` pauses (a long GC pause, a frozen VM): its lease runs out and the
    // placement moves the repository to `b`, which takes the lease.
    n.a.pause_renewals(true);
    n.table.expire(&key);
    n.b_view.set(SeqOwner::Here);
    n.b.commit(TEST_NS, TENANT, &repo, create("b1", "b1", 2))
        .await
        .expect("b1");
    let (holder, epoch) = n.table.holder(&key).expect("b holds the lease");
    assert_eq!(holder, "v1;2;b");
    assert!(epoch > first_epoch);
    // The zombie still believes it owns the repository: its append is fenced.
    n.a.commit(TEST_NS, TENANT, &repo, create("a2", "a2", 3))
        .await
        .expect("a2 after the fence");
    let mut fenced = false;
    while let Ok(event) = a_events.try_recv() {
        fenced |= matches!(event, GitEvent::Fenced { .. });
    }
    assert!(fenced, "the zombie's append was not fenced");
    assert_eq!(tail(&n).await.refs.len(), 3, "no commit was lost");
    // Waking up, the zombie learns it lost the lease and closes its sequencer.
    n.a.pause_renewals(false);
    eventually("the zombie to close its sequencer", async || {
        n.a.is_empty().await
    })
    .await;
    assert_eq!(n.a.stats().leases_lost, 1);
    assert_eq!(n.table.holder(&key).map(|(h, _)| h), Some("v1;2;b".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_sequencer_is_evicted_and_reloads() {
    let n = nodes(Duration::from_millis(150)).await;
    let repo = test_repo();
    let key = seq_lease_key(TEST_NS, &repo);
    let first =
        n.a.commit(TEST_NS, TENANT, &repo, create("a1", "a1", 1))
            .await
            .expect("a1");
    assert_eq!(n.a.len().await, 1);
    eventually("the idle sequencer to close", async || n.a.is_empty().await).await;
    // The sweep takes the sequencer out of the table first, then counts the
    // eviction and releases the lease: wait for the release too.
    eventually("the eviction to release the lease", async || {
        n.table.holder(&key).is_none()
    })
    .await;
    assert_eq!(n.a.stats().evictions, 1);
    // The next commit loads the repository again and continues its log.
    let second =
        n.a.commit(TEST_NS, TENANT, &repo, create("a2", "a2", 2))
            .await
            .expect("a2");
    assert_eq!(second.seq.0, first.seq.0 + 1);
    assert!(n.table.holder(&key).is_some());
}
