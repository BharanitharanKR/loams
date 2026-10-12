//! The store-backed suite: the WAL core against a real S3 endpoint (RustFS
//! in CI, plan GT1 Task 11). Skipped, with a printed reason, unless
//! `LOAMS_TEST_S3_URL=http://<host>:<port>/<bucket>` is set (with
//! `LOAMS_TEST_S3_KEY` and `LOAMS_TEST_S3_SECRET`); `scripts/git/rustfs.sh
//! start` prints them.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use loams_git::testing::ManualClock;
use loams_git::{
    Expect, IdemKey, ObjectId, Principal, ReadAt, RefError, RefLog, RefName, RefTxn, RefUpdate,
    RejectReason, RepoId, RepoPaths, Seq, StoreWalStore, fork_repo, plan_segment_gc,
};
use loams_store::{ObjectVersion, Store, StoreError};

use support::open_log;

/// The S3 store under test, or `None` with the reason printed.
fn s3(test: &str) -> Option<Store> {
    let Ok(url) = std::env::var("LOAMS_TEST_S3_URL") else {
        eprintln!("LOAMS_TEST_S3_URL not set: {test} skipped");
        return None;
    };
    let (endpoint, bucket) = url.rsplit_once('/').expect("<endpoint>/<bucket>");
    let key = std::env::var("LOAMS_TEST_S3_KEY").expect("LOAMS_TEST_S3_KEY");
    let secret = std::env::var("LOAMS_TEST_S3_SECRET").expect("LOAMS_TEST_S3_SECRET");
    let store = Store::from_url(
        &format!("s3://{bucket}"),
        [
            ("aws_endpoint", endpoint.to_string()),
            ("aws_access_key_id", key),
            ("aws_secret_access_key", secret),
            ("aws_region", "us-east-1".to_string()),
            ("aws_allow_http", "true".to_string()),
            ("aws_virtual_hosted_style_request", "false".to_string()),
        ],
    )
    .expect("store");
    Some(store)
}

/// A repository id no earlier run used.
fn fresh_repo(tag: &str) -> RepoId {
    RepoId::parse(&format!(
        "r{tag}{}",
        loams_git::ids::lowercase_ulid(ulid::Ulid::generate())
    ))
    .expect("id")
}

fn oid(n: u8) -> ObjectId {
    ObjectId::try_from(&[n; 20][..]).expect("oid")
}

fn create(key: &str, branch: &str, new: u8) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [new; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/{branch}")).expect("ref"),
            expect: Expect::Absent,
            new: Some(oid(new)),
        }],
        packs: Vec::new(),
        principal: Principal::parse("s3").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

#[tokio::test]
async fn s3_sequencers_race_one_ref() {
    let Some(store) = s3("s3_sequencers_race_one_ref") else {
        return;
    };
    let repo = fresh_repo("race");
    let clock = Arc::new(ManualClock::new(1_000_000));
    let mut logs = Vec::new();
    for _ in 0..8 {
        logs.push(open_log(&store, &repo, clock.clone(), |_| {}).await);
    }
    // Eight sequencers on one repository each create `main`: the
    // create-only segment PUT lets exactly one win.
    let racers: Vec<_> = logs
        .iter()
        .enumerate()
        .map(|(i, log)| {
            let log = log.clone();
            tokio::spawn(async move {
                log.commit(create(&format!("k{i}"), "main", i as u8 + 1))
                    .await
            })
        })
        .collect();
    let mut won = 0;
    for racer in racers {
        match racer.await.expect("join") {
            Ok(receipt) => {
                assert_eq!(receipt.seq, Seq(1));
                won += 1;
            }
            Err(RefError::Rejected(rejected)) => {
                assert_eq!(rejected[0].1, RejectReason::Exists);
            }
            Err(other) => panic!("{other}"),
        }
    }
    assert_eq!(won, 1);
    let snap = logs[0].snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(snap.seq, Seq(1));
    for log in logs {
        log.shutdown().await;
    }
}

#[tokio::test]
async fn s3_commit_fork_and_gc_plan() {
    let Some(store) = s3("s3_commit_fork_and_gc_plan") else {
        return;
    };
    let clock = Arc::new(ManualClock::new(1_000_000));
    let src_id = fresh_repo("src");
    let src = open_log(&store, &src_id, clock.clone(), |_| {}).await;
    for i in 0..5u8 {
        src.commit(create(&format!("k{i}"), &format!("b{i}"), i + 1))
            .await
            .expect("commit");
    }
    let dst_id = fresh_repo("dst");
    let paths = RepoPaths::new(loams_git::NamespaceId(7), &dst_id);
    let dst_wal = StoreWalStore::new(store.clone(), paths);
    assert_eq!(
        fork_repo(&src, &dst_wal, &src_id, ReadAt::Latest)
            .await
            .expect("fork"),
        Seq(5)
    );
    let dst = open_log(&store, &dst_id, clock, |_| {}).await;
    let snap = dst.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(snap.refs.len(), 5);
    // Everything is young: GC plans nothing on S3 either.
    let src_paths = RepoPaths::new(loams_git::NamespaceId(7), &src_id);
    let src_wal = StoreWalStore::new(store.clone(), src_paths.clone());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_millis() as i64;
    let plan = plan_segment_gc(
        &src_wal,
        &store,
        &src_paths,
        now,
        loams_git::limits::EXACTLY_RETENTION,
        loams_git::limits::GC_GRACE,
    )
    .await
    .expect("plan");
    assert!(plan.delete.is_empty(), "{:?}", plan.delete);
    src.shutdown().await;
    dst.shutdown().await;
}

/// T0-6: whether the store honours `If-Match`. Recorded, not required: no
/// GT1 correctness path uses it (R0.3).
#[tokio::test]
async fn s3_if_match_is_recorded() {
    let Some(store) = s3("s3_if_match_is_recorded") else {
        return;
    };
    let path = format!("probe/if-match/{}", ulid::Ulid::generate());
    let version = store
        .put(&path, Bytes::from_static(b"one"))
        .await
        .expect("put");
    let stale = ObjectVersion {
        e_tag: Some("\"00000000000000000000000000000000\"".into()),
        version: None,
    };
    let refused = store
        .put_if_match(&path, Bytes::from_static(b"two"), &stale)
        .await;
    let honoured = store
        .put_if_match(&path, Bytes::from_static(b"three"), &version)
        .await;
    eprintln!(
        "If-Match: stale tag -> {}, current tag -> {}",
        match &refused {
            Err(StoreError::PreconditionFailed { .. }) => "412 (honoured)".to_string(),
            Ok(_) => "applied (NOT honoured)".to_string(),
            Err(err) => format!("error: {err}"),
        },
        match &honoured {
            Ok(_) => "applied".to_string(),
            Err(err) => format!("error: {err}"),
        }
    );
    store.delete(&path).await.expect("delete");
}
