//! Segment GC (§36 §7; plan GT1 Task 11, old GT1 Task 8, ruling T7.3).
//!
//! The store is a `file://` directory, so the tests can age objects by
//! setting their files' modification times.

mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use loams_git::limits::{EXACTLY_RETENTION, GC_GRACE};
use loams_git::testing::ManualClock;
use loams_git::wal::{Hint, WalStore};
use loams_git::{
    BucketRefLog, Expect, IdemKey, ObjectId, Principal, RefError, RefLog, RefName, RefTxn,
    RefUpdate, RepoId, RepoPaths, RepoState, SegmentGcPlan, Seq, StoreWalStore, plan_segment_gc,
    run_segment_gc, state_at,
};
use loams_store::Store;

use support::open_log;

const HOUR: i64 = 3_600_000;

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    store: Store,
    paths: RepoPaths,
    wal: StoreWalStore,
    now: i64,
}

impl Fixture {
    /// A repository with 30 one-ref commits and checkpoints 0, 10 and 20.
    async fn new(test: &str) -> (Self, BucketRefLog) {
        let dir = tempfile::Builder::new()
            .prefix(test)
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let root = dir.path().to_path_buf();
        let store = Store::from_url(
            &format!("file://{}", root.display()),
            std::iter::empty::<(&str, String)>(),
        )
        .expect("store");
        let repo = RepoId::parse("rgc").expect("repo");
        let paths = RepoPaths::new(loams_git::NamespaceId(7), &repo);
        let log = open_log(
            &store,
            &repo,
            Arc::new(ManualClock::new(1_000_000)),
            |config| {
                config.checkpoint.every_segments = u64::MAX;
                config.checkpoint.every_bytes = u64::MAX;
            },
        )
        .await;
        let wal = StoreWalStore::new(store.clone(), paths.clone());
        for i in 1..=30u32 {
            log.commit(update(i)).await.expect("commit");
            if i % 10 == 0 && i < 30 {
                wal.put_checkpoint(&log.checkpoint(1_000_000))
                    .await
                    .expect("checkpoint");
            }
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64;
        let fixture = Self {
            _dir: dir,
            root,
            store,
            paths,
            wal,
            now,
        };
        (fixture, log)
    }

    fn file(&self, object: &str) -> PathBuf {
        self.root.join(object)
    }

    /// Sets the modification time of `object` to `age_ms` before `now`.
    fn age(&self, object: &str, age_ms: i64) {
        set_mtime(&self.file(object), self.now - age_ms);
    }

    fn age_segments(&self, seqs: impl IntoIterator<Item = u64>, age_ms: i64) {
        for seq in seqs {
            self.age(&self.paths.segment(Seq(seq)), age_ms);
        }
    }

    fn age_checkpoints(&self, seqs: impl IntoIterator<Item = u64>, age_ms: i64) {
        for seq in seqs {
            self.age(&self.paths.checkpoint(Seq(seq)), age_ms);
        }
    }

    async fn plan(&self) -> SegmentGcPlan {
        plan_segment_gc(
            &self.wal,
            &self.store,
            &self.paths,
            self.now,
            EXACTLY_RETENTION,
            GC_GRACE,
        )
        .await
        .expect("plan")
    }

    async fn publish_hint(&self, seq: u64, checkpoint: u64) {
        self.wal
            .publish_hint(&Hint {
                seq: Seq(seq),
                checkpoint: Seq(checkpoint),
                written_unix_ms: self.now,
            })
            .await
            .expect("hint");
    }

    fn segments(&self, seqs: impl IntoIterator<Item = u64>) -> Vec<String> {
        seqs.into_iter()
            .map(|seq| self.paths.segment(Seq(seq)))
            .collect()
    }

    /// The state after `seq`, replayed from checkpoint 0 before any GC.
    async fn truth(&self, seq: u64) -> RepoState {
        state_at(&self.wal, Seq(seq)).await.expect("state")
    }
}

fn set_mtime(path: &Path, unix_ms: i64) {
    let time = UNIX_EPOCH + Duration::from_millis(unix_ms as u64);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap_or_else(|err| panic!("{}: {err}", path.display()))
        .set_modified(time)
        .expect("set mtime");
}

fn update(i: u32) -> RefTxn {
    let mut new = [0x44u8; 20];
    new[16..].copy_from_slice(&i.to_be_bytes());
    RefTxn {
        key: IdemKey::parse(&format!("k{i}")).expect("key"),
        request_digest: [2; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/b{i}")).expect("ref"),
            expect: Expect::Absent,
            new: Some(ObjectId::try_from(&new[..]).expect("oid")),
        }],
        packs: Vec::new(),
        principal: Principal::parse("tester").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

#[tokio::test]
async fn gc_keeps_exactly_retention() {
    let (fx, log) = Fixture::new("gc-retention").await;
    log.shutdown().await;
    let before: Vec<RepoState> = {
        let mut states = Vec::new();
        for seq in 0..=30 {
            states.push(fx.truth(seq).await);
        }
        states
    };
    // Segments 1..=15 and checkpoints 0 and 10 were written 30 h ago;
    // segment 16 exactly at the 24 h cutoff, the rest 2 h ago.
    fx.age_segments(1..=15, 30 * HOUR);
    fx.age_checkpoints([0, 10], 30 * HOUR);
    fx.age_segments([16], 24 * HOUR);
    fx.age_segments(17..=30, 2 * HOUR);
    fx.age_checkpoints([20], 2 * HOUR);
    fx.publish_hint(30, 20).await;

    // The state at 15 was current at the cutoff, so checkpoint 10 is kept
    // and everything above it.
    let plan = fx.plan().await;
    assert_eq!(plan.keep_from, Seq(10));
    let mut expected = fx.segments(1..=10);
    expected.insert(0, fx.paths.checkpoint(Seq(0)));
    expected.sort();
    let mut got = plan.delete.clone();
    got.sort();
    assert_eq!(got, expected);
    assert_eq!(run_segment_gc(&fx.store, &plan).await.expect("gc"), 11);

    for seq in 10..=30 {
        assert_eq!(
            fx.truth(seq).await.to_checkpoint(0),
            before[seq as usize].to_checkpoint(0),
            "Exactly({seq}) is still served"
        );
    }
    assert!(matches!(
        state_at(&fx.wal, Seq(9)).await,
        Err(RefError::Invalid { .. })
    ));

    // One ms past the cutoff, segment 16 is old too: the state at 16 is the
    // oldest needed, and checkpoint 10 is still the newest at or below it.
    fx.age_segments([16], 24 * HOUR + 1);
    assert_eq!(fx.plan().await.keep_from, Seq(10));
    // With every segment past the retention, the newest checkpoint
    // (named by the hint) is enough.
    fx.age_segments(16..=30, 30 * HOUR);
    fx.age_checkpoints([20], 30 * HOUR);
    let plan = fx.plan().await;
    assert_eq!(plan.keep_from, Seq(20));
    run_segment_gc(&fx.store, &plan).await.expect("gc");
    let reopened = open_log(
        &fx.store,
        &RepoId::parse("rgc").expect("repo"),
        Arc::new(ManualClock::new(1_000_000)),
        |_| {},
    )
    .await;
    assert_eq!(
        reopened.checkpoint(0),
        before[30].to_checkpoint(0),
        "a cold reader still rebuilds the tail"
    );
}

#[tokio::test]
async fn gc_deletes_unreachable_above_gap_after_grace() {
    let (fx, log) = Fixture::new("gc-gap").await;
    log.shutdown().await;
    // A segment above a gap (31 is missing), as only a faulty writer
    // leaves.
    let stray = fx.paths.segment(Seq(35));
    fx.store
        .put(&stray, Bytes::from_static(b"not reachable"))
        .await
        .expect("put");

    // Inside the grace period it is kept, and so is everything young.
    let plan = fx.plan().await;
    assert!(plan.delete.is_empty(), "{:?}", plan.delete);

    // At the grace period it goes; nothing reachable does.
    fx.age(&stray, GC_GRACE.as_millis() as i64);
    let plan = fx.plan().await;
    assert_eq!(plan.delete, vec![stray.clone()]);
    run_segment_gc(&fx.store, &plan).await.expect("gc");
    assert!(!fx.file(&stray).exists());
    assert_eq!(state_at(&fx.wal, Seq(30)).await.expect("tail").seq, Seq(30));

    // An old segment inside the chain is never unreachable.
    fx.age_segments(25..=30, 2 * HOUR);
    assert!(fx.plan().await.delete.is_empty());
}

#[tokio::test]
async fn gc_never_deletes_segments_a_reader_needs() {
    let (fx, log) = Fixture::new("gc-reader").await;
    log.shutdown().await;
    let truth = fx.truth(30).await;
    // Everything is past the retention and the grace period.
    fx.age_segments(1..=30, 30 * HOUR);
    fx.age_checkpoints([0, 10, 20], 30 * HOUR);
    // A stale hint names checkpoint 10 although 20 exists (T7.3).
    fx.publish_hint(30, 10).await;

    // A reader starts at the hinted checkpoint before GC runs...
    let start = fx
        .wal
        .latest_checkpoint(Seq(u64::MAX))
        .await
        .expect("read")
        .expect("checkpoint");
    assert_eq!(start.seq, 10, "readers trust the hint");
    let mut reader = RepoState::from_checkpoint(start).expect("state");

    // ...GC keeps every segment above it...
    let plan = fx.plan().await;
    assert_eq!(plan.keep_from, Seq(10));
    run_segment_gc(&fx.store, &plan).await.expect("gc");

    // ...and the reader finishes its replay at the tail.
    let segments = fx.wal.read_from(Seq(11), 100).await.expect("read");
    for segment in &segments {
        reader.apply_segment(segment).expect("apply");
    }
    assert_eq!(reader.to_checkpoint(0), truth.to_checkpoint(0));

    // Once the hint names checkpoint 20, GC may go up to it.
    fx.publish_hint(30, 20).await;
    let plan = fx.plan().await;
    assert_eq!(plan.keep_from, Seq(20));
    run_segment_gc(&fx.store, &plan).await.expect("gc");
    assert_eq!(fx.truth(30).await.to_checkpoint(0), truth.to_checkpoint(0));
}
