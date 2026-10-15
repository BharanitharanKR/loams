//! The `_git` mirror and in-process subscribers (plan GT1 Task 15): every
//! committed transaction reaches the sink in order, the sink's failures
//! never delay an acknowledgement, a crash gap is repaired, and a restarted
//! subscriber resumes by seq.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use loams_cloudevents::CloudEvent;
use loams_git::format::event::from_cloudevent;
use loams_git::testing::{TEST_NS, ensure_repo, test_paths, test_repo};
use loams_git::{
    Expect, GitEventSink, IdemKey, MirrorCursor, NamespaceId, ObjectId, Principal, RefName, RefTxn,
    RefUpdate, RepoId, RepoSequencers, Seq, StoreWalStore, SystemClock, repair_repo,
};
use loams_store::Store;

const TENANT: &str = "local/1";

/// A sink that records what it is given and refuses the first `fail` calls.
#[derive(Debug, Default)]
struct MemSink {
    events: Mutex<Vec<CloudEvent>>,
    fail: Mutex<usize>,
}

impl MemSink {
    fn failing(fail: usize) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            fail: Mutex::new(fail),
        })
    }

    /// The idempotency keys published, in order, repeats included.
    fn keys(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("lock")
            .iter()
            .map(|e| from_cloudevent(e).expect("decode").key.as_str().to_string())
            .collect()
    }
}

#[async_trait]
impl GitEventSink for MemSink {
    async fn publish(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
        events: &[CloudEvent],
    ) -> Result<(), String> {
        assert_eq!((ns, repo), (TEST_NS, &test_repo()));
        let mut fail = self.fail.lock().expect("lock");
        if *fail > 0 {
            *fail -= 1;
            return Err("the stream is down".into());
        }
        self.events
            .lock()
            .expect("lock")
            .extend(events.iter().cloned());
        Ok(())
    }
}

fn create(key: &str, n: u8) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [n; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/b{n}")).expect("ref"),
            expect: Expect::Absent,
            new: Some(ObjectId::try_from(&[n; 20][..]).expect("oid")),
        }],
        packs: Vec::new(),
        principal: Principal::parse("tester").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

async fn store_with_repo() -> Store {
    let store = Store::in_memory();
    ensure_repo(&StoreWalStore::new(store.clone(), test_paths()))
        .await
        .expect("create");
    store
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_commit_appears_once_in_git_stream() {
    let store = store_with_repo().await;
    let sink = MemSink::failing(0);
    let seqs = RepoSequencers::new(store.clone(), Arc::new(SystemClock)).with_mirror(sink.clone());
    let repo = test_repo();
    let keys: Vec<String> = (1..=10).map(|i| format!("k{i}")).collect();
    for (i, key) in keys.iter().enumerate() {
        seqs.commit(TEST_NS, TENANT, &repo, create(key, i as u8 + 1))
            .await
            .expect("commit");
    }
    eventually("ten events", || sink.keys().len() >= 10).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(sink.keys(), keys, "each commit once, in commit order");
    // The cursor records how far the mirror got.
    seqs.shutdown().await;
    let cursor = MirrorCursor::new(store, TEST_NS, &repo).get().await;
    assert!(cursor.0 >= 1, "{cursor:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn mirror_failure_never_blocks_ack() {
    let store = store_with_repo().await;
    // The stream refuses the first four publishes (backing off 0.1 + 0.2 + 0.4 + 0.8 s).
    let sink = MemSink::failing(4);
    let seqs = RepoSequencers::new(store, Arc::new(SystemClock)).with_mirror(sink.clone());
    let repo = test_repo();
    let started = Instant::now();
    for i in 1..=5u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "commits waited for the mirror: {:?}",
        started.elapsed()
    );
    assert!(
        sink.keys().is_empty(),
        "nothing published while the stream is down"
    );
    eventually("the backlog", || sink.keys().len() >= 5).await;
    assert_eq!(sink.keys(), vec!["k1", "k2", "k3", "k4", "k5"]);
    seqs.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repair_fills_a_gap_after_crash() {
    let store = store_with_repo().await;
    let repo = test_repo();
    // Commits made with no mirror running: a node that crashed before
    // mirroring them.
    let seqs = RepoSequencers::new(store.clone(), Arc::new(SystemClock));
    for i in 1..=3u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    seqs.shutdown().await;
    let sink = MemSink::failing(0);
    let wal = StoreWalStore::new(store.clone(), test_paths());
    let cursor = MirrorCursor::new(store, TEST_NS, &repo);
    let published = repair_repo(&wal, &cursor, sink.as_ref(), TEST_NS, &repo)
        .await
        .expect("repair");
    assert_eq!(published, 3);
    assert_eq!(sink.keys(), vec!["k1", "k2", "k3"]);
    assert_eq!(cursor.get().await, Seq(3));
    // Nothing is left to repair.
    assert_eq!(
        repair_repo(&wal, &cursor, sink.as_ref(), TEST_NS, &repo)
            .await
            .expect("again"),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replayed_events_are_deduplicated() {
    // At least once: a mirror restarted from a stale cursor re-sends the
    // tail, with the same `source` and `id`, which the stream's dedup
    // ledger drops (the server's sink; `loams` tests the stream itself).
    let store = store_with_repo().await;
    let repo = test_repo();
    let sink = MemSink::failing(0);
    let seqs = RepoSequencers::new(store.clone(), Arc::new(SystemClock)).with_mirror(sink.clone());
    for i in 1..=2u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    eventually("two events", || sink.keys().len() >= 2).await;
    seqs.shutdown().await;
    let cursor = MirrorCursor::new(store.clone(), TEST_NS, &repo);
    cursor.set(Seq::ZERO).await;
    let wal = StoreWalStore::new(store, test_paths());
    repair_repo(&wal, &cursor, sink.as_ref(), TEST_NS, &repo)
        .await
        .expect("repair");
    let sent = sink.events.lock().expect("lock").clone();
    let mut ids: Vec<(String, String)> = sent
        .iter()
        .map(|e| {
            let event = from_cloudevent(e).expect("decode");
            (
                loams_git::format::event::source_of(event.ns, &event.repo),
                event.key.as_str().to_string(),
            )
        })
        .collect();
    assert_eq!(ids.len(), 4, "the tail was sent twice");
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 2, "a repeat has the first send's source and id");
}

#[tokio::test(flavor = "multi_thread")]
async fn subscriber_resumes_from_seq_after_restart() {
    let store = store_with_repo().await;
    let repo = test_repo();
    let seqs = RepoSequencers::new(store.clone(), Arc::new(SystemClock));
    for i in 1..=3u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    let mut first = seqs
        .subscribe(TEST_NS, TENANT, &repo, Seq(1))
        .await
        .expect("subscribe");
    let mut handled = Seq::ZERO;
    for _ in 0..3 {
        let txn = first.next().await.expect("an item").expect("a txn");
        handled = txn.seq;
    }
    drop(first);
    seqs.shutdown().await;
    // A new process: more commits, then the consumer resumes after `handled`.
    let seqs = RepoSequencers::new(store, Arc::new(SystemClock));
    for i in 4..=5u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    let resumed: Vec<String> = seqs
        .subscribe(TEST_NS, TENANT, &repo, handled.next())
        .await
        .expect("subscribe")
        .take(2)
        .map(|txn| txn.expect("txn").event.key.as_str().to_string())
        .collect()
        .await;
    assert_eq!(resumed, vec!["k4", "k5"]);
    seqs.shutdown().await;
}

/// PR #612 review: segment GC left a gap behind the cursor. Repair publishes
/// the segments the WAL still holds after the gap, never stops at it, and
/// moves the cursor only past what it published.
#[tokio::test(flavor = "multi_thread")]
async fn repair_publishes_the_retained_suffix_past_a_gap() {
    let store = store_with_repo().await;
    let repo = test_repo();
    let seqs = RepoSequencers::new(store.clone(), Arc::new(SystemClock));
    for i in 1..=4u8 {
        seqs.commit(TEST_NS, TENANT, &repo, create(&format!("k{i}"), i))
            .await
            .expect("commit");
    }
    seqs.shutdown().await;
    // Segments 2 and 3 are gone, as GC after a long outage would leave them.
    let paths = test_paths();
    for seq in [2, 3] {
        store
            .delete(&paths.segment(Seq(seq)))
            .await
            .expect("delete");
    }
    let sink = MemSink::failing(0);
    let wal = StoreWalStore::new(store.clone(), test_paths());
    let cursor = MirrorCursor::new(store, TEST_NS, &repo);
    let published = repair_repo(&wal, &cursor, sink.as_ref(), TEST_NS, &repo)
        .await
        .expect("repair");
    assert_eq!(published, 2);
    assert_eq!(sink.keys(), vec!["k1", "k4"]);
    assert_eq!(cursor.get().await, Seq(4));
    assert_eq!(
        cursor.first_retained_after(Seq(1)).await.expect("list"),
        Some(Seq(4))
    );
}
