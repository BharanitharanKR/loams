//! The repository catalog (§36 §4.1, Ruling 21, ruling T0-6; plan GT1 Task
//! 13): names in the bucket, changed only by create-only writes.

mod support;

use loams_git::catalog::{
    BucketRepoCatalog, CatalogError, CreateRepo, Created, ForkRepo, Idempotency, RepoCatalog,
    RepoLifecycle,
};
use loams_git::format::pb;
use loams_git::limits::{GC_GRACE, MAX_FORK_DEPTH};
use loams_git::testing::ManualClock;
use loams_git::{
    BucketRefLog, Expect, IdemKey, NamespaceId, ObjectId, ReadAt, RefLog, RefName, RepoName,
    RepoPaths, SequencerConfig, StoreBlobStore, StoreWalStore,
};
use loams_store::{Fault, Op, Store};
use std::sync::Arc;

use support::faulty_store;

const NS: NamespaceId = NamespaceId(7);

fn name(s: &str) -> RepoName {
    RepoName::parse(s).expect("repo name")
}

fn main_branch() -> RefName {
    RefName::parse("refs/heads/main").expect("ref")
}

fn idem(key: Option<&str>, digest: u8) -> Idempotency {
    Idempotency {
        key: key.map(|k| IdemKey::parse(k).expect("key")),
        request_digest: [digest; 32],
    }
}

fn create(repo: &str, key: Option<&str>, digest: u8) -> CreateRepo {
    CreateRepo {
        name: name(repo),
        object_format: pb::ObjectFormat::Sha1,
        default_branch: main_branch(),
        idempotency: idem(key, digest),
    }
}

fn oid(n: u8) -> ObjectId {
    ObjectId::try_from(&[n; 20][..]).expect("oid")
}

/// The ref log of a catalogued repository.
async fn open_log(store: &Store, catalog: &BucketRepoCatalog, repo: &str) -> BucketRefLog {
    let record = catalog
        .get(NS, &name(repo))
        .await
        .expect("get")
        .expect("exists");
    let paths = RepoPaths::new(NS, &record.repo_id);
    BucketRefLog::open(
        Arc::new(StoreWalStore::new(store.clone(), paths.clone())),
        Arc::new(StoreBlobStore::new(store.clone(), paths)),
        SequencerConfig::new("local/test", NS, record.repo_id),
        Arc::new(ManualClock::new(1_790_000_000_000)),
    )
    .await
    .expect("open")
}

#[tokio::test]
async fn create_repo_replay_returns_same_repo() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store.clone());
    let first = catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("create");
    let Created::New(record) = &first else {
        panic!("{first:?}")
    };
    assert_eq!(record.state, RepoLifecycle::Active);
    let again = catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("replay");
    assert_eq!(again, Created::Replayed(record.clone()));
    // The same key with another request is refused.
    let err = catalog
        .create(NS, create("app", Some("k1"), 2))
        .await
        .expect_err("mismatch");
    assert!(matches!(err, CatalogError::IdempotencyMismatch), "{err:?}");
    // The repository exists, with HEAD on the default branch.
    let log = open_log(&store, &catalog, "app").await;
    let snap = log.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(snap.symrefs["HEAD"], main_branch());
    log.shutdown().await;
}

#[tokio::test]
async fn create_same_name_other_key_is_already_exists() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store);
    catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("create");
    for (key, digest) in [(Some("k2"), 1), (None, 1)] {
        let err = catalog
            .create(NS, create("app", key, digest))
            .await
            .expect_err("taken");
        assert!(matches!(err, CatalogError::AlreadyExists), "{err:?}");
    }
    // SHA-256 is refused before anything is written.
    let mut sha256 = create("other", Some("k3"), 1);
    sha256.object_format = pb::ObjectFormat::Sha256;
    let err = catalog.create(NS, sha256).await.expect_err("sha256");
    assert!(err.to_string().contains("sha256"), "{err}");
    assert!(
        catalog
            .get(NS, &name("other"))
            .await
            .expect("get")
            .is_none()
    );
}

/// The name PUT lands but its answer is lost: the retry with the same key
/// reads the record back and answers the same repository.
#[tokio::test]
async fn undetermined_create_then_retry_returns_same_repo() {
    let (faulty, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store);
    // The checkpoint lands; the name PUT lands and reports a failure, and
    // every retry of it fails too: the outcome is unknown to the caller.
    faulty.inject_nth(Op::PutCreate, 2, Fault::ErrorAfterApply);
    for _ in 0..8 {
        faulty.inject(Op::PutCreate, Fault::Error);
    }
    let lost = catalog.create(NS, create("app", Some("k1"), 1)).await;
    assert!(lost.is_err(), "{lost:?}");
    faulty.clear();
    let written = catalog
        .get(NS, &name("app"))
        .await
        .expect("get")
        .expect("the name landed");
    let retried = catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("retry");
    assert_eq!(retried, Created::Replayed(written));
}

#[tokio::test]
async fn recreate_after_purge_gets_new_repo_id() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store.clone());
    let first = catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("create")
        .into_record();
    let deleted = catalog
        .mark_deleting(NS, &name("app"), 1_000)
        .await
        .expect("delete");
    assert_eq!(deleted.state, RepoLifecycle::Deleting);
    // Taken until purged.
    let err = catalog
        .create(NS, create("app", Some("k2"), 1))
        .await
        .expect_err("deleting");
    assert!(matches!(err, CatalogError::Deleting), "{err:?}");
    // Not purged inside the grace period.
    let grace = GC_GRACE.as_millis() as i64;
    let report = catalog
        .purge_deleted(NS, 1_000 + grace - 1, GC_GRACE)
        .await
        .expect("sweep");
    assert_eq!((report.purged, report.in_grace), (0, 1));
    let report = catalog
        .purge_deleted(NS, 1_000 + grace, GC_GRACE)
        .await
        .expect("sweep");
    assert_eq!(report.purged, 1);
    assert!(catalog.get(NS, &name("app")).await.expect("get").is_none());
    let old_root = format!("{}/", RepoPaths::new(NS, &first.repo_id).root());
    assert!(store.list(&old_root).await.expect("list").is_empty());
    let second = catalog
        .create(NS, create("app", Some("k2"), 1))
        .await
        .expect("recreate")
        .into_record();
    assert_ne!(second.repo_id, first.repo_id);
    assert_eq!(second.state, RepoLifecycle::Active);
}

/// Forking is one checkpoint PUT plus the name record, and nothing is
/// copied; the fork sees the parent's refs at the fork point.
#[tokio::test]
async fn fork_repo_is_one_checkpoint_put_plus_name() {
    let (faulty, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store.clone());
    catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("create");
    let source = open_log(&store, &catalog, "app").await;
    let main = main_branch();
    let at = source
        .cas_ref(
            main.clone(),
            Expect::Absent,
            Some(oid(1)),
            IdemKey::parse("push-1").expect("key"),
        )
        .await
        .expect("push")
        .seq;
    let puts = faulty.calls(Op::Put);
    let fork = catalog
        .fork(
            NS,
            ForkRepo {
                source: name("app"),
                name: name("app-fork"),
                at: ReadAt::Latest,
                idempotency: idem(Some("f1"), 1),
            },
            &source,
        )
        .await
        .expect("fork")
        .into_record();
    assert_eq!(
        faulty.calls(Op::Put) - puts,
        2,
        "one checkpoint and the name"
    );
    let origin = fork.parent.clone().expect("a parent");
    assert_eq!((origin.name.as_str(), origin.seq), ("app", at));
    assert_eq!(fork.fork_depth, 1);
    let child = open_log(&store, &catalog, "app-fork").await;
    let snap = child.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(snap.refs[&main], oid(1));
    assert!(snap.packs.is_empty());
    child.shutdown().await;
    source.shutdown().await;
}

/// A fork may be at most `MAX_FORK_DEPTH` forks deep.
#[tokio::test]
async fn fork_past_max_depth_is_refused() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store.clone());
    catalog
        .create(NS, create("r0", Some("k0"), 0))
        .await
        .expect("create");
    for depth in 1..=MAX_FORK_DEPTH + 1 {
        let parent = format!("r{}", depth - 1);
        let log = open_log(&store, &catalog, &parent).await;
        let result = catalog
            .fork(
                NS,
                ForkRepo {
                    source: name(&parent),
                    name: name(&format!("r{depth}")),
                    at: ReadAt::Latest,
                    idempotency: idem(Some(&format!("f{depth}")), 1),
                },
                &log,
            )
            .await;
        log.shutdown().await;
        if depth <= MAX_FORK_DEPTH {
            assert_eq!(result.expect("fork").record().fork_depth as usize, depth);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CatalogError::ForkTooDeep {
                        max: MAX_FORK_DEPTH
                    })
                ),
                "{result:?}"
            );
        }
    }
}

/// A deleted repository that a fork reads is kept (and its name stays
/// taken) until the fork is purged too.
#[tokio::test]
async fn delete_with_live_fork_waits() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store.clone());
    catalog
        .create(NS, create("app", Some("k1"), 1))
        .await
        .expect("create");
    let source = open_log(&store, &catalog, "app").await;
    catalog
        .fork(
            NS,
            ForkRepo {
                source: name("app"),
                name: name("fork"),
                at: ReadAt::Latest,
                idempotency: idem(Some("f1"), 1),
            },
            &source,
        )
        .await
        .expect("fork");
    source.shutdown().await;
    catalog
        .mark_deleting(NS, &name("app"), 0)
        .await
        .expect("delete");
    let late = GC_GRACE.as_millis() as i64 * 10;
    let report = catalog
        .purge_deleted(NS, late, GC_GRACE)
        .await
        .expect("sweep");
    assert_eq!((report.purged, report.waiting_for_forks), (0, 1));
    let parent = catalog.get(NS, &name("app")).await.expect("get");
    assert_eq!(parent.map(|r| r.state), Some(RepoLifecycle::Deleting));
    // Hidden from listings.
    let (listed, more) = catalog.list(NS, None, 10).await.expect("list");
    assert_eq!(
        listed.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        ["fork"]
    );
    assert!(!more);
    // Once the fork is deleted and purged, the parent goes too.
    catalog
        .mark_deleting(NS, &name("fork"), 0)
        .await
        .expect("delete fork");
    let report = catalog
        .purge_deleted(NS, late, GC_GRACE)
        .await
        .expect("sweep");
    assert_eq!(report.purged, 1, "the fork first");
    let report = catalog
        .purge_deleted(NS, late, GC_GRACE)
        .await
        .expect("sweep");
    assert_eq!(report.purged, 1, "then the parent");
    assert!(catalog.get(NS, &name("app")).await.expect("get").is_none());
}

#[tokio::test]
async fn catalog_list_paginates() {
    let (_, store) = faulty_store();
    let catalog = BucketRepoCatalog::new(store);
    for i in 0..7 {
        catalog
            .create(NS, create(&format!("repo-{i}"), Some(&format!("k{i}")), 1))
            .await
            .expect("create");
    }
    catalog
        .mark_deleting(NS, &name("repo-3"), 0)
        .await
        .expect("delete");
    let mut seen = Vec::new();
    let mut after: Option<RepoName> = None;
    loop {
        let (page, more) = catalog.list(NS, after.as_ref(), 2).await.expect("list");
        assert!(page.len() <= 2);
        seen.extend(page.iter().map(|r| r.name.as_str().to_string()));
        after = page.last().map(|r| r.name.clone());
        if !more {
            break;
        }
    }
    assert_eq!(
        seen,
        ["repo-0", "repo-1", "repo-2", "repo-4", "repo-5", "repo-6"]
    );
}
