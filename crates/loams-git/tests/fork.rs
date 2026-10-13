//! O(1) forks (§36 §4.7; plan GT1 Task 11, old GT1 Task 8).

mod support;

use std::sync::Arc;

use loams_fs::MemFs;
use loams_git::format::pb;
use loams_git::limits::MAX_FORK_DEPTH;
use loams_git::odb::{ObjectDb, OdbError};
use loams_git::testing::ManualClock;
use loams_git::{
    BucketRefLog, Expect, ForkError, IdemKey, NamespaceId, ObjectId, PackCache, Principal, ReadAt,
    RefLog, RefName, RefTxn, RefUpdate, RepoId, RepoPaths, Seq, StoreBlobStore, StoreWalStore,
    fork_repo, open_fork_odb,
};
use loams_store::{Op, Store};

use support::{GitRepo, faulty_store, open_log, put_pack};

const NS: NamespaceId = NamespaceId(7);

fn repo(name: &str) -> RepoId {
    RepoId::parse(name).expect("repo id")
}

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(1_000_000))
}

fn wal(store: &Store, id: &RepoId) -> StoreWalStore {
    StoreWalStore::new(store.clone(), RepoPaths::new(NS, id))
}

fn blobs(store: &Store, id: &RepoId) -> StoreBlobStore {
    StoreBlobStore::new(store.clone(), RepoPaths::new(NS, id))
}

fn cache() -> Arc<PackCache> {
    Arc::new(PackCache::new(Arc::new(MemFs::new(1 << 30)), 1 << 30))
}

/// Points `branch` at `new` (from `old`), adding `packs`.
fn set(
    key: &str,
    branch: &str,
    old: Option<ObjectId>,
    new: ObjectId,
    packs: Vec<pb::PackRef>,
) -> RefTxn {
    RefTxn {
        key: IdemKey::parse(key).expect("key"),
        request_digest: [1; 32],
        updates: vec![RefUpdate {
            name: RefName::parse(&format!("refs/heads/{branch}")).expect("ref"),
            expect: old.map_or(Expect::Absent, Expect::Oid),
            new: Some(new),
        }],
        packs,
        principal: Principal::parse("tester").expect("principal"),
        scope: None,
        trace: None,
        push_options: Vec::new(),
    }
}

/// Opens a fork's ref log (its checkpoint 0 already written).
async fn open_fork(store: &Store, id: &RepoId) -> BucketRefLog {
    open_log(store, id, clock(), |_| {}).await
}

#[tokio::test]
async fn fork_is_one_put() {
    let (faulty, store) = faulty_store();
    let src_id = repo("rsrc");
    let src = open_log(&store, &src_id, clock(), |_| {}).await;
    for i in 0..3u8 {
        src.commit(set(
            &format!("k{i}"),
            &format!("b{i}"),
            None,
            ObjectId::try_from(&[i + 1; 20][..]).expect("oid"),
            vec![],
        ))
        .await
        .expect("commit");
    }
    // A sequencer writes its hint beside its commits; reopen the source so
    // the counts below are the fork's alone.
    src.shutdown().await;
    let src = open_log(&store, &src_id, clock(), |_| {}).await;
    let dst_id = repo("rdst");
    let dst_wal = wal(&store, &dst_id);
    let (puts, creates, multiparts) = (
        faulty.calls(Op::Put),
        faulty.calls(Op::PutCreate),
        faulty.calls(Op::PutMultipart),
    );
    let at = fork_repo(&src, &dst_wal, &src_id, ReadAt::Latest)
        .await
        .expect("fork");
    assert_eq!(at, Seq(3));
    assert_eq!(
        faulty.calls(Op::PutCreate) - creates,
        1,
        "one create-only PUT"
    );
    assert_eq!(
        faulty.calls(Op::Put) - puts,
        1,
        "one PUT in all (`Put` counts every mode)"
    );
    assert_eq!(faulty.calls(Op::PutMultipart), multiparts);

    // Idempotent at the same seq.
    fork_repo(&src, &dst_wal, &src_id, ReadAt::Exactly(Seq(3)))
        .await
        .expect("again");

    let dst = open_fork(&store, &dst_id).await;
    let snap = dst.snapshot(ReadAt::Latest).await.expect("snapshot");
    let src_snap = src.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert_eq!(snap.seq, Seq(0), "the fork starts its own log");
    assert_eq!(snap.refs, src_snap.refs);
    assert!(snap.packs.is_empty(), "nothing is copied");
    assert_eq!(
        snap.parent,
        Some(pb::ForkParent {
            repo_id: "rsrc".into(),
            seq: 3
        })
    );
    // The fork commits independently.
    let new = ObjectId::try_from(&[9; 20][..]).expect("oid");
    dst.commit(set("kf", "fork", None, new, vec![]))
        .await
        .expect("commit");
    assert_eq!(dst.seq(), Seq(1));
    assert_eq!(src.seq(), Seq(3));
}

#[tokio::test]
async fn fork_sees_parent_refs_and_objects() {
    let Some(git) = GitRepo::init("fork_sees_parent_refs_and_objects") else {
        return;
    };
    let first = git.commit(&[("a.txt", "one\n"), ("d/b.txt", "bee\n")], "first");
    let second = git.commit(&[("a.txt", "two\n")], "second");
    let store = Store::in_memory();
    let src_id = repo("rparent");
    let src = open_log(&store, &src_id, clock(), |_| {}).await;
    let p1 = put_pack(
        &blobs(&store, &src_id),
        git.pack_revs(&[&first.to_string()], &[]),
    )
    .await;
    src.commit(set("k1", "main", None, first, vec![p1]))
        .await
        .expect("commit");

    let dst_id = repo("rchild");
    let at = fork_repo(&src, &wal(&store, &dst_id), &src_id, ReadAt::Latest)
        .await
        .expect("fork");
    assert_eq!(at, Seq(1));

    // The parent moves on after the fork.
    let range = format!("{first}..{second}");
    let p2 = put_pack(&blobs(&store, &src_id), git.pack_revs(&[&range], &[])).await;
    src.commit(set("k2", "main", Some(first), second, vec![p2]))
        .await
        .expect("commit");

    let dst = open_fork(&store, &dst_id).await;
    let snap = dst.snapshot(ReadAt::Latest).await.expect("snapshot");
    let main = RefName::parse("refs/heads/main").expect("ref");
    assert_eq!(
        snap.refs.get(&main),
        Some(&first),
        "the parent's refs at the fork"
    );

    let odb = open_fork_odb(&store, NS, &dst_id, &snap, cache())
        .await
        .expect("odb");
    assert_eq!(odb.depth(), 1);
    for id in git.objects(&[&first.to_string()]) {
        let (_, data) = odb.read(&id).await.expect("read through the parent");
        assert_eq!(data.as_ref(), git.cat(&id).1.as_slice());
    }
    assert!(
        !odb.contains(&second).await.expect("contains"),
        "the parent's later objects are not the fork's"
    );
    assert!(
        odb.missing_from_closure(&[first], &[], None)
            .await
            .expect("walk")
            .is_empty()
    );
}

#[tokio::test]
async fn fork_of_fork_falls_through() {
    let Some(git) = GitRepo::init("fork_of_fork_falls_through") else {
        return;
    };
    let first = git.commit(&[("a.txt", "one\n")], "first");
    let second = git.commit(&[("b.txt", "two\n")], "second");
    let store = Store::in_memory();
    let a_id = repo("ra");
    let b_id = repo("rb");
    let c_id = repo("rc");
    let a = open_log(&store, &a_id, clock(), |_| {}).await;
    let pa = put_pack(
        &blobs(&store, &a_id),
        git.pack_revs(&[&first.to_string()], &[]),
    )
    .await;
    a.commit(set("k1", "main", None, first, vec![pa]))
        .await
        .expect("commit");
    fork_repo(&a, &wal(&store, &b_id), &a_id, ReadAt::Latest)
        .await
        .expect("fork b");
    let b = open_fork(&store, &b_id).await;
    let range = format!("{first}..{second}");
    let pb_pack = put_pack(&blobs(&store, &b_id), git.pack_revs(&[&range], &[])).await;
    b.commit(set("k2", "main", Some(first), second, vec![pb_pack]))
        .await
        .expect("commit");
    fork_repo(&b, &wal(&store, &c_id), &b_id, ReadAt::Latest)
        .await
        .expect("fork c");

    let c = open_fork(&store, &c_id).await;
    let snap = c.snapshot(ReadAt::Latest).await.expect("snapshot");
    let odb = open_fork_odb(&store, NS, &c_id, &snap, cache())
        .await
        .expect("odb");
    assert_eq!(odb.depth(), 2);
    let mut ids = git.objects(&["main"]);
    ids.sort();
    ids.dedup();
    for id in &ids {
        let (_, data) = odb.read(id).await.expect("read through two parents");
        assert_eq!(data.as_ref(), git.cat(id).1.as_slice());
    }
    assert!(
        odb.missing_from_closure(&[second], &[], None)
            .await
            .expect("walk")
            .is_empty()
    );
}

#[tokio::test]
async fn fork_chain_depth_is_bounded() {
    let store = Store::in_memory();
    let mut parent_id = repo("rf0");
    let mut parent = open_log(&store, &parent_id, clock(), |_| {}).await;
    let mut chain = Vec::new();
    for level in 1..=MAX_FORK_DEPTH + 1 {
        let id = repo(&format!("rf{level}"));
        fork_repo(&parent, &wal(&store, &id), &parent_id, ReadAt::Latest)
            .await
            .expect("fork");
        parent = open_fork(&store, &id).await;
        parent_id = id.clone();
        chain.push((id, parent.clone()));
    }
    // MAX_FORK_DEPTH parents open; one more is refused.
    let (at_limit, log) = &chain[MAX_FORK_DEPTH - 1];
    let snap = log.snapshot(ReadAt::Latest).await.expect("snapshot");
    let odb = open_fork_odb(&store, NS, at_limit, &snap, cache())
        .await
        .expect("at the limit");
    assert_eq!(odb.depth(), MAX_FORK_DEPTH);
    let (past, log) = &chain[MAX_FORK_DEPTH];
    let snap = log.snapshot(ReadAt::Latest).await.expect("snapshot");
    assert!(matches!(
        open_fork_odb(&store, NS, past, &snap, cache()).await,
        Err(ForkError::Odb(OdbError::ForkTooDeep { max })) if max == MAX_FORK_DEPTH
    ));
}
