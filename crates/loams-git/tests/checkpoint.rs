//! Checkpoints (§36 §4.1, §7; plan GT1 Task 11, old GT1 Task 8).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use loams_git::format::checkpoint::decode_checkpoint;
use loams_git::format::pb;
use loams_git::testing::{ManualClock, ensure_repo, mem_reflog_with, test_paths, test_repo};
use loams_git::wal::{Appended, Hint, WalError, WalStore};
use loams_git::{
    BucketRefLog, Expect, IdemKey, ObjectId, Principal, RefError, RefLog, RefName, RefTxn,
    RefUpdate, RepoPaths, RepoState, Seq, SequencerConfig, StoreBlobStore, StoreWalStore, WalBatch,
    WalSegment, create_repo,
};
use loams_store::Store;
use proptest::prelude::*;
use tokio::sync::Semaphore;

/// A time well inside every idempotency window the tests open.
const NOW: i64 = 1_000_000;

fn oid(n: u32) -> ObjectId {
    let mut bytes = [0x33u8; 20];
    bytes[16..].copy_from_slice(&n.to_be_bytes());
    ObjectId::try_from(&bytes[..]).expect("oid")
}

fn txn(key: &str, branch: &str, expect: Expect, new: Option<u32>) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [key.len() as u8; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/{branch}")).expect("ref"),
            expect,
            new: new.map(oid),
        }],
        packs: Vec::new(),
        principal: Principal::parse("tester").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

/// The state after `seq`, replayed from checkpoint 0 alone.
async fn replayed_from_zero(wal: &StoreWalStore, seq: Seq) -> RepoState {
    let zero = wal
        .latest_checkpoint(Seq::ZERO)
        .await
        .expect("read")
        .expect("checkpoint 0");
    let mut state = RepoState::from_checkpoint(zero).expect("state");
    let segments = wal.read_from(Seq(1), seq.0 as usize).await.expect("read");
    for segment in &segments {
        state.apply_segment(segment).expect("apply");
    }
    assert_eq!(state.seq, seq);
    state
}

/// Every checkpoint seq stored for the test repository.
async fn checkpoint_seqs(store: &Store, paths: &RepoPaths) -> Vec<Seq> {
    store
        .list_page(&paths.checkpoint_prefix(), None, 10_000)
        .await
        .expect("list")
        .iter()
        .filter_map(|info| RepoPaths::parse_checkpoint(&info.path))
        .collect()
}

/// Commits `ops` (ref index, kind) one by one, each chosen so it applies.
async fn run_ops(log: &BucketRefLog, ops: &[(usize, u8)]) {
    let mut model: BTreeMap<usize, u32> = BTreeMap::new();
    for (i, (slot, kind)) in ops.iter().enumerate() {
        let branch = format!("r{slot}");
        let key = format!("k{i}");
        let value = i as u32 + 1;
        let txn = match (model.get(slot).copied(), kind % 3) {
            (None, _) => txn(&key, &branch, Expect::Absent, Some(value)),
            (Some(old), 0) => txn(&key, &branch, Expect::Oid(oid(old)), None),
            (Some(_), 1) => txn(&key, &branch, Expect::Any, Some(value)),
            (Some(old), _) => txn(&key, &branch, Expect::Oid(oid(old)), Some(value)),
        };
        let new = txn.updates[0].new;
        log.commit(txn).await.expect("commit");
        match new {
            Some(_) => model.insert(*slot, value),
            None => model.remove(slot),
        };
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Every checkpoint the sequencer wrote, plus the segments after it,
    /// gives the state replayed from checkpoint 0; and each checkpoint is
    /// that state at its seq.
    #[test]
    fn checkpoint_plus_replay_equals_state(
        ops in prop::collection::vec((0usize..3, any::<u8>()), 1..40),
        every in 1u64..6,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let store = Store::in_memory();
            // A clock that moves on every read.
            let clock = Arc::new(ManualClock::stepping(NOW, 3));
            let log = mem_reflog_with(store.clone(), clock.clone(), |config| {
                config.checkpoint.every_segments = every;
            })
            .await;
            run_ops(&log, &ops).await;
            let tail = log.seq();
            let live = log.checkpoint(NOW);
            log.shutdown().await;

            let paths = test_paths();
            let wal = StoreWalStore::new(store.clone(), paths.clone());
            let truth = replayed_from_zero(&wal, tail).await;
            assert_eq!(live, truth.to_checkpoint(NOW), "the live state is the replayed one");
            let seqs = checkpoint_seqs(&store, &paths).await;
            if tail.0 >= every {
                assert!(seqs.len() > 1, "the policy wrote a checkpoint ({seqs:?}, tail {tail})");
            }
            for seq in &seqs {
                let (bytes, _) = store.get(&paths.checkpoint(*seq)).await.expect("get");
                let stored = decode_checkpoint(&bytes).expect("decode");
                let mut from = RepoState::from_checkpoint(stored).expect("state");
                assert_eq!(
                    from.to_checkpoint(NOW),
                    replayed_from_zero(&wal, *seq).await.to_checkpoint(NOW),
                    "checkpoint {seq} is the state at {seq}"
                );
                let segments = wal
                    .read_from(seq.next(), (tail.0 - seq.0) as usize)
                    .await
                    .expect("read");
                for segment in &segments {
                    from.apply_segment(segment).expect("apply");
                }
                assert_eq!(from.to_checkpoint(NOW), truth.to_checkpoint(NOW));
            }
            // The hint names the newest checkpoint once the sequencer stops.
            let hint = wal.hint().await.expect("hint");
            assert_eq!(hint.seq, tail);
            assert_eq!(Some(&hint.checkpoint), seqs.iter().max());
            // A reopened log loads it and replays the rest.
            let reopened = mem_reflog_with(store, clock, |_| {}).await;
            assert_eq!(reopened.checkpoint(NOW), truth.to_checkpoint(NOW));
        });
    }
}

/// A WAL whose checkpoint writes wait until the test lets them through.
#[derive(Debug)]
struct Stalled {
    inner: StoreWalStore,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl WalStore for Stalled {
    async fn append(&self, expect_seq: Seq, batch: &WalBatch) -> Result<Appended, WalError> {
        self.inner.append(expect_seq, batch).await
    }

    async fn read_from(&self, from: Seq, max: usize) -> Result<Vec<WalSegment>, WalError> {
        self.inner.read_from(from, max).await
    }

    async fn hint(&self) -> Result<Hint, WalError> {
        self.inner.hint().await
    }

    async fn publish_hint(&self, hint: &Hint) -> Result<(), WalError> {
        self.inner.publish_hint(hint).await
    }

    async fn put_checkpoint(&self, checkpoint: &pb::Checkpoint) -> Result<(), WalError> {
        if checkpoint.seq > 0 {
            let _permit = self.gate.acquire().await.expect("gate");
        }
        self.inner.put_checkpoint(checkpoint).await
    }

    async fn latest_checkpoint(
        &self,
        at_or_below: Seq,
    ) -> Result<Option<pb::Checkpoint>, WalError> {
        self.inner.latest_checkpoint(at_or_below).await
    }
}

#[tokio::test]
async fn checkpoint_never_delays_ack() {
    let store = Store::in_memory();
    let gate = Arc::new(Semaphore::new(0));
    let wal = Arc::new(Stalled {
        inner: StoreWalStore::new(store.clone(), test_paths()),
        gate: gate.clone(),
    });
    ensure_repo(wal.as_ref()).await.expect("repo");
    let mut config = SequencerConfig::new("org/1", loams_git::testing::TEST_NS, test_repo());
    config.checkpoint.every_segments = 1;
    let log = BucketRefLog::open(
        wal,
        Arc::new(StoreBlobStore::new(store.clone(), test_paths())),
        config,
        Arc::new(ManualClock::new(NOW)),
    )
    .await
    .expect("open");

    // The first commit starts a checkpoint that cannot finish; the next
    // commits are acknowledged anyway.
    for i in 0..5u32 {
        let commit = log.commit(txn(
            &format!("k{i}"),
            &format!("b{i}"),
            Expect::Absent,
            Some(i),
        ));
        tokio::time::timeout(Duration::from_secs(10), commit)
            .await
            .expect("the receipt is not held by the checkpoint")
            .expect("commit");
    }
    assert_eq!(log.seq(), Seq(5));
    assert_eq!(
        checkpoint_seqs(&store, &test_paths()).await,
        vec![Seq(0)],
        "the checkpoint is still stalled"
    );

    // Released, it lands, and the hint names it after shutdown.
    gate.add_permits(16);
    log.shutdown().await;
    let seqs = checkpoint_seqs(&store, &test_paths()).await;
    assert_eq!(seqs.first(), Some(&Seq(0)));
    assert!(seqs.contains(&Seq(1)), "{seqs:?}");
    let hint = StoreWalStore::new(store, test_paths())
        .hint()
        .await
        .expect("hint");
    assert_eq!(hint.seq, Seq(5));
    assert_eq!(Some(&hint.checkpoint), seqs.iter().max());
}

#[tokio::test]
async fn create_repo_is_idempotent_and_sha1_only() {
    let store = Store::in_memory();
    let wal = StoreWalStore::new(store.clone(), test_paths());
    create_repo(&wal, pb::ObjectFormat::Sha1)
        .await
        .expect("create");
    create_repo(&wal, pb::ObjectFormat::Sha1)
        .await
        .expect("again");
    assert_eq!(checkpoint_seqs(&store, &test_paths()).await, vec![Seq(0)]);
    match create_repo(&wal, pb::ObjectFormat::Sha256).await {
        Err(err @ RefError::State(_)) => assert_eq!(
            err.to_string(),
            "object format sha256 is not supported yet (Q388)"
        ),
        other => panic!("{other:?}"),
    }
    // A repository without a checkpoint is not found, by reads too.
    let none = StoreWalStore::new(
        store.clone(),
        RepoPaths::new(
            loams_git::NamespaceId(1),
            &loams_git::RepoId::parse("rabsent").expect("id"),
        ),
    );
    assert!(matches!(
        loams_git::state_at(&none, Seq(3)).await,
        Err(RefError::NotFound)
    ));
    assert!(matches!(
        loams_git::state_at(&wal, Seq(3)).await,
        Err(RefError::Invalid { .. })
    ));
    let other = StoreWalStore::new(
        store.clone(),
        RepoPaths::new(
            loams_git::NamespaceId(1),
            &loams_git::RepoId::parse("rnone").expect("id"),
        ),
    );
    assert!(matches!(
        BucketRefLog::open(
            Arc::new(other),
            Arc::new(StoreBlobStore::new(store, test_paths())),
            SequencerConfig::new("org/1", loams_git::NamespaceId(1), test_repo()),
            Arc::new(ManualClock::new(NOW)),
        )
        .await,
        Err(RefError::NotFound)
    ));
}

/// `latest_state` reads the tail without a sequencer and writes nothing:
/// the latest checkpoint, then the segments after it (plan GT1 Task 51's
/// read path for the serverless helper).
#[tokio::test]
async fn latest_state_reads_the_tail_and_writes_nothing() {
    let store = Store::in_memory();
    let clock = Arc::new(ManualClock::stepping(NOW, 3));
    let log = mem_reflog_with(store.clone(), clock, |config| {
        config.checkpoint.every_segments = 2;
    })
    .await;
    run_ops(&log, &[(0, 0), (1, 0), (0, 2), (2, 0), (1, 1)]).await;
    let tail = log.seq();
    let expected = log
        .snapshot(loams_git::ReadAt::Latest)
        .await
        .expect("snapshot");
    // Stopped first, so no checkpoint or hint write races the count.
    log.shutdown().await;
    let wal = StoreWalStore::new(store.clone(), test_paths());
    let before = store.list_page("", None, 10_000).await.expect("list").len();
    let state = loams_git::latest_state(&wal).await.expect("read");
    assert_eq!(state.seq, tail);
    assert_eq!(state.snapshot(), *expected);
    let after = store.list_page("", None, 10_000).await.expect("list").len();
    assert_eq!(before, after, "a read wrote an object");
    let absent = StoreWalStore::new(
        store,
        RepoPaths::new(
            loams_git::NamespaceId(1),
            &loams_git::RepoId::parse("rabsent").expect("id"),
        ),
    );
    assert!(matches!(
        loams_git::latest_state(&absent).await,
        Err(RefError::NotFound)
    ));
}
