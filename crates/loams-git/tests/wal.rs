//! The bucket WAL (§36 §4.2, §5.1, D389; plan GT1 Task 7).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loams_git::format::checkpoint::encode_checkpoint;
use loams_git::format::event::{TxnBody, TxnEvent, to_cloudevent};
use loams_git::format::pb;
use loams_git::format::segment::encode_segment;
use loams_git::{
    Appended, Hint, IdemKey, NamespaceId, RepoId, RepoPaths, Seq, StoreWalStore, WalBatch,
    WalError, WalStore,
};
use loams_store::{Fault, FaultRates, FaultyStore, Op, Store};
use object_store::memory::InMemory;
use proptest::prelude::*;

fn repo() -> RepoId {
    RepoId::parse("r01j9z3m5q8x0000000000000000").expect("repo id")
}

fn paths() -> RepoPaths {
    RepoPaths::new(NamespaceId(7), &repo())
}

/// A WAL over a fault-injecting in-memory store, and a second handle on
/// the same bytes with no faults.
fn faulty() -> (Arc<FaultyStore>, StoreWalStore, Store) {
    let inner = Arc::new(InMemory::new());
    let faulty = Arc::new(FaultyStore::new(inner.clone()));
    let store = Store::new(faulty.clone()).with_sorted_listing(true);
    let clean = Store::new(inner).with_sorted_listing(true);
    (faulty, StoreWalStore::new(store, paths()), clean)
}

/// A one-transaction batch for segment `seq`, told apart by `tag`.
fn batch(seq: Seq, tag: u64) -> WalBatch {
    let event = TxnEvent {
        ns: NamespaceId(7),
        repo: repo(),
        tenant: "acme/prod".into(),
        key: IdemKey::parse(&format!("key-{tag}")).expect("key"),
        trace: None,
        time_unix_ms: 1_790_762_400_250,
        body: TxnBody::RefTxn(pb::RefTransaction {
            updates: vec![pb::RefUpdate {
                name: "refs/heads/main".into(),
                expect: Some(pb::ref_update::Expect::Any(true)),
                new_oid: vec![(tag % 251) as u8; 20],
            }],
            principal: "alice".into(),
            object_format: pb::ObjectFormat::Sha1 as i32,
            ..Default::default()
        }),
    };
    let mut batch_id = [0u8; 16];
    batch_id[..8].copy_from_slice(&tag.to_le_bytes());
    batch_id[8..].copy_from_slice(&seq.0.to_le_bytes());
    WalBatch {
        batch_id,
        events: vec![to_cloudevent(&event, seq)],
    }
}

fn checkpoint(seq: u64) -> pb::Checkpoint {
    pb::Checkpoint {
        seq,
        object_format: pb::ObjectFormat::Sha1 as i32,
        refs: BTreeMap::from([("refs/heads/main".to_string(), vec![seq as u8; 20])]),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_is_exclusive_across_writers() {
    let store = Store::in_memory();
    let racers = (0..32u64).map(|tag| {
        let wal = StoreWalStore::new(store.clone(), paths());
        tokio::spawn(async move { (tag, wal.append(Seq(1), &batch(Seq(1), tag)).await) })
    });
    let mut committed = Vec::new();
    let mut fenced = Vec::new();
    for racer in racers {
        let (tag, appended) = racer.await.expect("join");
        match appended.expect("append") {
            Appended::Committed { seq } => {
                assert_eq!(seq, Seq(1));
                committed.push(tag);
            }
            Appended::Fenced { existing } => fenced.push(existing),
        }
    }
    assert_eq!(committed.len(), 1, "exactly one writer owns seq 1");
    assert_eq!(fenced.len(), 31);
    let winner = batch(Seq(1), committed[0]);
    for existing in &fenced {
        assert_eq!(existing.seq, Seq(1));
        assert_eq!(existing.batch, winner, "every loser is told the winner");
    }
    let wal = StoreWalStore::new(store, paths());
    let stored = wal.read_from(Seq(1), 10).await.expect("read");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].batch, winner);
}

#[tokio::test]
async fn retry_after_lost_ack_is_committed() {
    let (faulty, wal, _) = faulty();
    // Applied, and the answer lost: the read-back finds this batch.
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    let appended = wal.append(Seq(1), &batch(Seq(1), 1)).await.expect("append");
    assert_eq!(appended, Appended::Committed { seq: Seq(1) });
    assert_eq!(faulty.calls(Op::PutCreate), 1, "resolved without a resend");

    // Applied, and reported as already existing (a lost ack seen by a
    // retry): the same.
    faulty.inject(Op::PutCreate, Fault::Precondition);
    let appended = wal.append(Seq(2), &batch(Seq(2), 2)).await.expect("append");
    assert_eq!(appended, Appended::Committed { seq: Seq(2) });

    // A caller's own retry of a committed batch is committed too.
    let appended = wal.append(Seq(2), &batch(Seq(2), 2)).await.expect("append");
    assert_eq!(appended, Appended::Committed { seq: Seq(2) });
    let stored = wal.read_from(Seq(1), 10).await.expect("read");
    assert_eq!(stored.len(), 2);
}

#[tokio::test]
async fn retry_after_lost_ack_fenced_by_other() {
    let (faulty, wal, clean) = faulty();
    // Another writer takes seq 1; this writer's PUT fails before it is
    // applied, so the outcome is unknown until the read-back.
    let other = StoreWalStore::new(clean, paths());
    assert_eq!(
        other
            .append(Seq(1), &batch(Seq(1), 9))
            .await
            .expect("other"),
        Appended::Committed { seq: Seq(1) }
    );
    faulty.inject(Op::PutCreate, Fault::Error);
    let appended = wal.append(Seq(1), &batch(Seq(1), 1)).await.expect("append");
    match appended {
        Appended::Fenced { existing } => assert_eq!(existing.batch, batch(Seq(1), 9)),
        other => panic!("expected Fenced, got {other:?}"),
    }
    assert_eq!(
        faulty.calls(Op::PutCreate),
        1,
        "no resend over another's segment"
    );
}

/// R2 answers 429 when writers race one name, sometimes after applying the
/// write. The read-back resolves it; an absent segment is resent with the
/// same bytes, and repeated failures end in the error.
#[tokio::test]
async fn r2_style_429_resolves_by_read_back() {
    let (faulty, wal, _) = faulty();
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    assert_eq!(
        wal.append(Seq(1), &batch(Seq(1), 1)).await.expect("append"),
        Appended::Committed { seq: Seq(1) }
    );

    // Throttled before it was applied: the read-back finds nothing and the
    // same bytes are resent.
    faulty.inject(Op::PutCreate, Fault::Error);
    let before = faulty.calls(Op::PutCreate);
    assert_eq!(
        wal.append(Seq(2), &batch(Seq(2), 2)).await.expect("append"),
        Appended::Committed { seq: Seq(2) }
    );
    assert_eq!(faulty.calls(Op::PutCreate) - before, 2);

    // Throttled five times: the error, and no segment.
    for _ in 0..5 {
        faulty.inject(Op::PutCreate, Fault::Error);
    }
    let before = faulty.calls(Op::PutCreate);
    let err = wal
        .append(Seq(3), &batch(Seq(3), 3))
        .await
        .expect_err("every attempt throttled");
    assert!(matches!(err, WalError::Store(_)), "{err:?}");
    assert_eq!(faulty.calls(Op::PutCreate) - before, 5);
    assert_eq!(wal.read_from(Seq(3), 1).await.expect("read"), Vec::new());
}

#[tokio::test]
async fn read_stops_at_gap() {
    let (faulty, wal, _) = faulty();
    for seq in [1, 2, 4] {
        wal.append(Seq(seq), &batch(Seq(seq), seq))
            .await
            .expect("append");
    }
    let read = wal.read_from(Seq(1), 10).await.expect("read");
    assert_eq!(
        read.iter().map(|s| s.seq).collect::<Vec<_>>(),
        [Seq(1), Seq(2)],
        "segment 4 is above a gap"
    );
    assert_eq!(read[1].batch, batch(Seq(2), 2));
    assert_eq!(wal.read_from(Seq(3), 10).await.expect("read"), Vec::new());
    assert_eq!(wal.read_from(Seq(4), 10).await.expect("read").len(), 1);
    assert_eq!(
        wal.read_from(Seq(1), 1).await.expect("read").len(),
        1,
        "max"
    );
    assert_eq!(wal.read_from(Seq(1), 0).await.expect("read"), Vec::new());
    let gets = faulty.calls(Op::Get);
    wal.read_from(Seq(u64::MAX), usize::MAX)
        .await
        .expect("read at the end of the seq space");
    assert_eq!(faulty.calls(Op::Get) - gets, 1);
}

#[tokio::test]
async fn corrupt_segment_is_error_not_stop() {
    let (_, wal, clean) = faulty();
    wal.append(Seq(1), &batch(Seq(1), 1)).await.expect("append");
    clean
        .put_if_absent(
            &paths().segment(Seq(2)),
            Bytes::from_static(b"not a segment"),
        )
        .await
        .expect("put");
    let err = wal.read_from(Seq(1), 10).await.expect_err("corrupt");
    assert!(
        matches!(err, WalError::Corrupt { seq: Seq(2), .. }),
        "{err:?}"
    );

    // A valid segment stored under another seq's name is corrupt as well.
    let moved = encode_segment(Seq(5), &batch(Seq(5), 5), 0).expect("encode");
    clean
        .put_if_absent(&paths().segment(Seq(3)), moved)
        .await
        .expect("put");
    let err = wal.read_from(Seq(3), 10).await.expect_err("corrupt");
    assert!(
        matches!(err, WalError::Corrupt { seq: Seq(3), .. }),
        "{err:?}"
    );

    // A fenced append reads the corrupt segment back and says so.
    let err = wal
        .append(Seq(2), &batch(Seq(2), 2))
        .await
        .expect_err("the seq holds a corrupt segment");
    assert!(
        matches!(err, WalError::Corrupt { seq: Seq(2), .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn hint_missing_is_empty() {
    let (_, wal, clean) = faulty();
    assert_eq!(wal.hint().await.expect("hint"), Hint::empty());
    assert!(Hint::empty().is_empty());

    let hint = Hint {
        seq: Seq(300),
        checkpoint: Seq(256),
        written_unix_ms: 1_790_762_400_250,
    };
    wal.publish_hint(&hint).await.expect("publish");
    assert_eq!(wal.hint().await.expect("hint"), hint);
    let (stored, _) = clean.get(&paths().head()).await.expect("get");
    assert_eq!(
        &stored[..],
        br#"{"seq":300,"checkpoint":256,"written_unix_ms":1790762400250}"#
    );
    let newer = Hint {
        seq: Seq(301),
        ..hint
    };
    wal.publish_hint(&newer).await.expect("overwrite");
    assert_eq!(wal.hint().await.expect("hint"), newer);

    // An unreadable hint is no hint.
    for junk in [
        &b"{"[..],
        br#"{"seq":1,"checkpoint":2,"written_unix_ms":0}"#,
    ] {
        clean
            .put(&paths().head(), Bytes::from_static(junk))
            .await
            .expect("put");
        assert_eq!(wal.hint().await.expect("hint"), Hint::empty());
    }
}

#[tokio::test]
async fn latest_checkpoint_uses_hint_then_list() {
    let (faulty, wal, _) = faulty();
    assert_eq!(
        wal.latest_checkpoint(Seq(u64::MAX)).await.expect("none"),
        None
    );
    for seq in [0, 256, 512] {
        wal.put_checkpoint(&checkpoint(seq)).await.expect("put");
    }

    // No hint: a LIST.
    let lists = faulty.calls(Op::List);
    let latest = wal.latest_checkpoint(Seq(u64::MAX)).await.expect("latest");
    assert_eq!(latest, Some(checkpoint(512)));
    assert!(faulty.calls(Op::List) > lists);

    // A hint at or below the bound: no LIST.
    wal.publish_hint(&Hint {
        seq: Seq(600),
        checkpoint: Seq(512),
        written_unix_ms: 1,
    })
    .await
    .expect("hint");
    let lists = faulty.calls(Op::List);
    let latest = wal.latest_checkpoint(Seq(u64::MAX)).await.expect("latest");
    assert_eq!(latest, Some(checkpoint(512)));
    assert_eq!(faulty.calls(Op::List), lists, "the hint was enough");

    // A hint above the bound: a LIST, for the greatest at or below it.
    for (bound, expected) in [(511, 256), (512, 512), (300, 256), (255, 0), (0, 0)] {
        let latest = wal.latest_checkpoint(Seq(bound)).await.expect("latest");
        assert_eq!(latest, Some(checkpoint(expected)), "at or below {bound}");
    }

    // A hint naming a missing checkpoint falls back to the LIST.
    wal.publish_hint(&Hint {
        seq: Seq(800),
        checkpoint: Seq(768),
        written_unix_ms: 2,
    })
    .await
    .expect("hint");
    let latest = wal.latest_checkpoint(Seq(u64::MAX)).await.expect("latest");
    assert_eq!(latest, Some(checkpoint(512)));
}

#[tokio::test]
async fn checkpoint_put_is_idempotent_on_equal_bytes() {
    let (faulty, wal, clean) = faulty();
    let c = checkpoint(256);
    wal.put_checkpoint(&c).await.expect("put");
    wal.put_checkpoint(&c).await.expect("equal bytes again");
    assert_eq!(faulty.calls(Op::PutCreate), 2);

    // A lost acknowledgement is resolved by the same comparison.
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    wal.put_checkpoint(&checkpoint(512))
        .await
        .expect("lost ack");

    // Another state at the same seq is corruption.
    let mut other = c.clone();
    other.refs.insert("refs/heads/x".into(), vec![9; 20]);
    let err = wal.put_checkpoint(&other).await.expect_err("another state");
    assert!(
        matches!(err, WalError::Corrupt { seq: Seq(256), .. }),
        "{err:?}"
    );

    // The same state with other idempotency entries (another writer pruned
    // its window at another time) is accepted, and the stored one stays.
    let mut pruned_later = c.clone();
    pruned_later.idempotency.push(pb::IdemEntry {
        key: "push-1".into(),
        request_digest: vec![1; 32],
        seq: 200,
        index: 0,
        expires_unix_ms: 5,
    });
    wal.put_checkpoint(&pruned_later).await.expect("same state");
    let (stored, _) = clean.get(&paths().checkpoint(Seq(256))).await.expect("get");
    assert_eq!(stored, encode_checkpoint(&c));
}

/// A checkpoint over the multipart threshold is uploaded in parts, after a
/// HEAD finds no checkpoint at its seq.
#[tokio::test]
async fn large_checkpoint_uses_multipart() {
    let (faulty, wal, _) = faulty();
    let mut c = checkpoint(1024);
    c.protections.push(pb::Protection {
        pattern: "x".repeat(65 * 1024 * 1024),
        deny_delete: true,
        deny_force: false,
    });
    let gets = faulty.calls(Op::Get) - faulty.heads();
    wal.put_checkpoint(&c).await.expect("put");
    assert_eq!(faulty.calls(Op::PutMultipart), 1);
    assert_eq!(faulty.calls(Op::PutCreate), 0);
    assert_eq!(
        faulty.calls(Op::Get) - faulty.heads() - gets,
        1,
        "read back after the upload"
    );

    // Another state at the seq is refused, before any upload.
    let mut other = c.clone();
    other.refs.insert("refs/heads/x".into(), vec![9; 20]);
    let err = wal.put_checkpoint(&other).await.expect_err("another state");
    assert!(
        matches!(err, WalError::Corrupt { seq: Seq(1024), .. }),
        "{err:?}"
    );
    assert_eq!(faulty.calls(Op::PutMultipart), 1);
    wal.put_checkpoint(&c).await.expect("again");
    assert_eq!(faulty.calls(Op::PutMultipart), 1, "not uploaded twice");
    let latest = wal.latest_checkpoint(Seq(u64::MAX)).await.expect("latest");
    assert_eq!(latest.as_ref(), Some(&c));
}

/// Writers that each append at the tail they know, through random store
/// faults.
async fn race_under_faults(seed: u64, rates: FaultRates) {
    let inner = Arc::new(InMemory::new());
    let faulty = Arc::new(FaultyStore::random(inner.clone(), seed, rates));
    let store = Store::new(faulty.clone()).with_sorted_listing(true);
    let writers = (0..4u64).map(|writer| {
        let wal = StoreWalStore::new(store.clone(), paths());
        async move {
            let mut owned = Vec::new();
            let mut next = Seq(1);
            for attempt in 0..50u64 {
                let tag = writer * 1000 + attempt;
                match wal.append(next, &batch(next, tag)).await {
                    Ok(Appended::Committed { seq }) => {
                        owned.push((seq, batch(next, tag).batch_id));
                        next = seq.next();
                    }
                    Ok(Appended::Fenced { existing }) => next = existing.seq.next(),
                    // Unknown: try again at the same seq with a new batch.
                    Err(WalError::Store(_)) => {}
                    Err(err) => panic!("writer {writer}: {err}"),
                }
            }
            owned
        }
    });
    let owned: Vec<_> = futures::future::join_all(writers)
        .await
        .into_iter()
        .flatten()
        .collect();
    let clean = StoreWalStore::new(Store::new(inner).with_sorted_listing(true), paths());
    let log = clean.read_from(Seq(1), usize::MAX).await.expect("read");
    assert!(!log.is_empty(), "some appends got through");
    let mut claimed = BTreeMap::new();
    for (seq, batch_id) in owned {
        assert!(
            claimed.insert(seq, batch_id).is_none(),
            "two writers were told they own {seq}"
        );
        let segment = log
            .get((seq.0 - 1) as usize)
            .unwrap_or_else(|| panic!("{seq} is committed but not readable"));
        assert_eq!(
            segment.batch.batch_id, batch_id,
            "{seq} holds another batch"
        );
    }
    assert_eq!(
        log.iter().map(|s| s.seq.0).collect::<Vec<_>>(),
        (1..=log.len() as u64).collect::<Vec<_>>(),
        "the log is contiguous"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn store_faults_never_produce_two_owners(
        seed in any::<u64>(),
        error in 0.0..0.2f64,
        error_after_apply in 0.0..0.2f64,
        precondition in 0.0..0.1f64,
        delay in 0.0..0.3f64,
    ) {
        let rates = FaultRates {
            error,
            error_after_apply,
            precondition,
            delay,
            max_delay: Duration::from_millis(5),
        };
        // Paused time: backoff pauses and injected delays cost nothing.
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
            .block_on(race_under_faults(seed, rates));
    }
}
