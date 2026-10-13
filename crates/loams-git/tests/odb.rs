//! The object database trait and the pack-cache `Odb` (§36 §4.7, §5.3;
//! plan GT1 Task 9, old GT1 Task 5).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use loams_fs::{Fs, FsPath, MemFs};
use loams_git::format::pb;
use loams_git::limits::MAX_FORK_DEPTH;
use loams_git::odb::{ObjectDb, OdbError, PackCache, PackCacheOdb};
use loams_git::{ObjectId, RefSnapshot, Seq, StoreBlobStore};
use loams_store::Op;

use support::{GitRepo, faulty_store, put_pack, repo_paths};

const CACHE_BYTES: u64 = 64 * 1024 * 1024;

fn cache(max_bytes: u64) -> (Arc<MemFs>, Arc<PackCache>) {
    let fs = Arc::new(MemFs::new(CACHE_BYTES));
    let cache = Arc::new(PackCache::new(fs.clone(), max_bytes));
    (fs, cache)
}

/// A snapshot at `seq` with `packs` (oldest first) and a fork parent.
fn snapshot(seq: u64, packs: Vec<pb::PackRef>, parent: Option<(&str, u64)>) -> RefSnapshot {
    RefSnapshot {
        seq: Seq(seq),
        refs: BTreeMap::new(),
        symrefs: BTreeMap::new(),
        protections: Vec::new(),
        packs,
        parent: parent.map(|(repo_id, seq)| pb::ForkParent {
            repo_id: repo_id.to_string(),
            seq,
        }),
    }
}

fn kind_name(kind: gix_object::Kind) -> &'static str {
    match kind {
        gix_object::Kind::Blob => "blob",
        gix_object::Kind::Tree => "tree",
        gix_object::Kind::Commit => "commit",
        gix_object::Kind::Tag => "tag",
    }
}

/// Reads every object in `ids` and compares it with what git stores.
async fn assert_reads_like_git(odb: &dyn ObjectDb, git: &GitRepo, ids: &[ObjectId]) {
    for id in ids {
        assert!(odb.contains(id).await.expect("contains"), "{id} is held");
        let (kind, data) = odb.read(id).await.expect("read");
        let (git_kind, git_data) = git.cat(id);
        assert_eq!(kind_name(kind), git_kind, "{id}'s kind");
        assert_eq!(data.as_ref(), git_data.as_slice(), "{id}'s bytes");
    }
}

fn absent() -> ObjectId {
    ObjectId::from_hex(b"00000000000000000000000000000000deadbeef").expect("oid")
}

#[tokio::test]
async fn reads_objects_written_by_git() {
    let Some(git) = GitRepo::init("reads_objects_written_by_git") else {
        return;
    };
    git.commit(&[("a.txt", "one\n"), ("dir/b.txt", "bee\n")], "first");
    git.commit(
        &[("a.txt", "one\ntwo\n"), ("dir/c/d.txt", "dee\n")],
        "second",
    );
    git.git(&["tag", "-a", "-m", "annotated", "v1"], None);
    let tag = git.rev("v1");

    let (_, store) = faulty_store();
    let (repo, paths) = repo_paths(1);
    let blobs = Arc::new(StoreBlobStore::new(store, paths));
    let pack = put_pack(&blobs, git.pack_revs(&["main", "v1"], &[])).await;
    let (_, cache) = cache(CACHE_BYTES);
    let odb = PackCacheOdb::open(repo, blobs, cache, &snapshot(1, vec![pack], None), None)
        .await
        .expect("open");

    let mut ids = git.objects(&["--all"]);
    assert!(ids.contains(&tag), "the annotated tag is packed");
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 12, "2 commits, 5 trees, 4 blobs, 1 tag");
    assert_reads_like_git(&odb, &git, &ids).await;

    assert!(!odb.contains(&absent()).await.expect("contains"));
    assert!(matches!(
        odb.read(&absent()).await,
        Err(OdbError::NotFound(id)) if id == absent()
    ));
}

#[tokio::test]
async fn delta_objects_resolve() {
    let Some(git) = GitRepo::init("delta_objects_resolve") else {
        return;
    };
    // One large file changed a little in each commit packs as a delta chain.
    let mut text: String = (0..400)
        .map(|i| format!("line {i} of the file\n"))
        .collect();
    for round in 0..8 {
        text.push_str(&format!("appended in round {round}\n"));
        git.commit(&[("big.txt", &text)], &format!("round {round}"));
    }
    let mut ids = git.objects(&["main"]);
    ids.sort();

    // REF_DELTA (git's default for pack-objects) and OFS_DELTA packs.
    for flags in [&[][..], &["--delta-base-offset"][..]] {
        let (pack, idx) = git.pack_revs(&["main"], flags);
        let verify_dir = git.path().join("verify");
        std::fs::create_dir_all(&verify_dir).expect("mkdir");
        std::fs::write(verify_dir.join("pack-x.pack"), &pack).expect("write");
        std::fs::write(verify_dir.join("pack-x.idx"), &idx).expect("write");
        let verified =
            String::from_utf8(git.git(&["verify-pack", "-v", "verify/pack-x.idx"], None))
                .expect("utf-8");
        assert!(
            verified.contains("chain length = "),
            "the pack holds deltas ({flags:?}): {verified}"
        );

        let (_, store) = faulty_store();
        let (repo, paths) = repo_paths(2);
        let blobs = Arc::new(StoreBlobStore::new(store, paths));
        let pack = put_pack(&blobs, (pack, idx)).await;
        let (_, cache) = cache(CACHE_BYTES);
        let odb = PackCacheOdb::open(repo, blobs, cache, &snapshot(1, vec![pack], None), None)
            .await
            .expect("open");
        assert_reads_like_git(&odb, &git, &ids).await;
    }
}

#[tokio::test]
async fn fork_falls_through_to_parent_at_seq() {
    let Some(git) = GitRepo::init("fork_falls_through_to_parent_at_seq") else {
        return;
    };
    let first = git.commit(&[("a.txt", "first\n")], "first");
    let second = git.commit(&[("a.txt", "second\n")], "second");
    git.git(&["checkout", "-q", "-b", "fork", &first.to_string()], None);
    let forked = git.commit(&[("f.txt", "fork only\n")], "fork");

    // The parent committed two packs (seqs 1 and 2); the fork was taken at
    // seq 1 and committed its own pack.
    let (_, store) = faulty_store();
    let (parent_id, parent_paths) = repo_paths(3);
    let (child_id, child_paths) = repo_paths(4);
    let parent_blobs = Arc::new(StoreBlobStore::new(store.clone(), parent_paths));
    let child_blobs = Arc::new(StoreBlobStore::new(store, child_paths));
    let p1 = put_pack(&parent_blobs, git.pack_revs(&[&first.to_string()], &[])).await;
    let range = format!("{first}..{second}");
    let p2 = put_pack(&parent_blobs, git.pack_revs(&[&range], &[])).await;
    let range = format!("{first}..{forked}");
    let c1 = put_pack(&child_blobs, git.pack_revs(&[&range], &[])).await;
    let (_, cache) = cache(CACHE_BYTES);

    // The parent at seq 2 holds both packs; at seq 1 only the first.
    let parent_now = PackCacheOdb::open(
        parent_id.clone(),
        parent_blobs.clone(),
        cache.clone(),
        &snapshot(2, vec![p1.clone(), p2], None),
        None,
    )
    .await
    .expect("open");
    assert!(parent_now.contains(&second).await.expect("contains"));
    let parent_at_fork = PackCacheOdb::open(
        parent_id.clone(),
        parent_blobs,
        cache.clone(),
        &snapshot(1, vec![p1], None),
        None,
    )
    .await
    .expect("open");
    let child = PackCacheOdb::open(
        child_id,
        child_blobs,
        cache,
        &snapshot(1, vec![c1], Some((parent_id.as_str(), 1))),
        Some(Arc::new(parent_at_fork)),
    )
    .await
    .expect("open");

    // The fork's own objects and the parent's at seq 1 are read; the
    // parent's later objects are not.
    let mut ids = git.objects(&["fork"]);
    ids.sort();
    ids.dedup();
    assert_reads_like_git(&child, &git, &ids).await;
    let only_later: Vec<ObjectId> = git
        .objects(&[&second.to_string(), &format!("^{first}")])
        .into_iter()
        .filter(|id| !ids.contains(id))
        .collect();
    assert!(only_later.contains(&second));
    for id in &only_later {
        assert!(
            !child.contains(id).await.expect("contains"),
            "{id} is later"
        );
        assert!(matches!(child.read(id).await, Err(OdbError::NotFound(_))));
    }
}

#[tokio::test]
async fn fork_depth_is_bounded() {
    let (_, store) = faulty_store();
    let (_, cache) = cache(CACHE_BYTES);
    // Repository `byte`, forked from `parent` at seq 1 when there is one.
    let open = |byte: u8, parent: Option<Arc<PackCacheOdb>>| {
        let (repo, paths) = repo_paths(byte);
        let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
        let named = parent.as_ref().map(|p| p.repo().as_str().to_string());
        let snap = snapshot(1, Vec::new(), named.as_deref().map(|id| (id, 1)));
        let cache = cache.clone();
        async move { PackCacheOdb::open(repo, blobs, cache, &snap, parent).await }
    };

    // A root and MAX_FORK_DEPTH forks below it open; one more does not.
    let mut odb = Arc::new(open(0, None).await.expect("root"));
    for level in 1..=MAX_FORK_DEPTH {
        odb = Arc::new(
            open(level as u8, Some(odb))
                .await
                .unwrap_or_else(|err| panic!("fork level {level}: {err}")),
        );
    }
    assert_eq!(odb.depth(), MAX_FORK_DEPTH);
    let deeper = open(MAX_FORK_DEPTH as u8 + 1, Some(odb)).await;
    assert!(
        matches!(deeper, Err(OdbError::ForkTooDeep { max }) if max == MAX_FORK_DEPTH),
        "{deeper:?}"
    );

    // A fork opened without its parent, a parent the snapshot does not
    // name, another repository at the same seq, and the parent at another
    // seq are refused.
    let root = Arc::new(open(0x41, None).await.expect("root"));
    let other = Arc::new(open(0x42, None).await.expect("other"));
    let (repo, paths) = repo_paths(0x40);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let fork = snapshot(1, Vec::new(), Some((root.repo().as_str(), 1)));
    let cases = [
        (fork.clone(), None),
        (snapshot(1, Vec::new(), None), Some(root.clone())),
        (fork, Some(other)),
        (
            snapshot(1, Vec::new(), Some((root.repo().as_str(), 7))),
            Some(root.clone()),
        ),
    ];
    for (snap, parent) in cases {
        let refused =
            PackCacheOdb::open(repo.clone(), blobs.clone(), cache.clone(), &snap, parent).await;
        assert!(
            matches!(refused, Err(OdbError::ParentMismatch { .. })),
            "{refused:?}"
        );
    }
    let fork = snapshot(1, Vec::new(), Some((root.repo().as_str(), 1)));
    PackCacheOdb::open(repo, blobs, cache, &fork, Some(root))
        .await
        .expect("the named parent at its seq");
}

#[tokio::test]
async fn missing_from_closure_stops_at_known() {
    let Some(git) = GitRepo::init("missing_from_closure_stops_at_known") else {
        return;
    };
    let first = git.commit(&[("a.txt", "old\n")], "first");
    let old_blob = git.rev(&format!("{first}:a.txt"));
    let tip = git.commit(&[("a.txt", "new\n")], "second");

    // The stored pack lacks a blob only the first commit reaches.
    let stored: Vec<ObjectId> = git
        .objects(&[&tip.to_string()])
        .into_iter()
        .filter(|id| *id != old_blob)
        .collect();
    let (_, store) = faulty_store();
    let (repo, paths) = repo_paths(8);
    let blobs = Arc::new(StoreBlobStore::new(store, paths));
    let pack = put_pack(&blobs, git.pack_objects(&stored)).await;
    let (_, cache) = cache(CACHE_BYTES);
    let odb = PackCacheOdb::open(repo, blobs, cache, &snapshot(1, vec![pack], None), None)
        .await
        .expect("open");

    // The whole history is walked without a boundary; with the first
    // commit known (a current ref tip), the walk does not enter it.
    assert_eq!(
        odb.missing_from_closure(&[tip], &[], None)
            .await
            .expect("walk"),
        vec![old_blob]
    );
    assert_eq!(
        odb.missing_from_closure(&[tip], &[first], None)
            .await
            .expect("walk"),
        Vec::<ObjectId>::new()
    );
    assert_eq!(
        odb.missing_from_closure(&[first], &[first], None)
            .await
            .expect("walk"),
        Vec::<ObjectId>::new(),
        "a known tip is not checked"
    );
}

#[tokio::test]
async fn missing_from_closure_finds_a_missing_blob() {
    let Some(git) = GitRepo::init("missing_from_closure_finds_a_missing_blob") else {
        return;
    };
    let first = git.commit(
        &[("a.txt", "kept\n"), ("dir/b.txt", "also kept\n")],
        "first",
    );
    let tip = git.commit(&[("dir/new.txt", "left out\n")], "second");
    let left_out = git.rev(&format!("{tip}:dir/new.txt"));

    // The stored pack holds every object of both commits but one blob.
    let all = git.objects(&[&tip.to_string()]);
    let stored: Vec<ObjectId> = all.iter().copied().filter(|id| *id != left_out).collect();
    let (faulty, store) = faulty_store();
    let (repo, paths) = repo_paths(5);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let pack = put_pack(&blobs, git.pack_objects(&stored)).await;
    let (_, cache) = cache(CACHE_BYTES);
    let odb = PackCacheOdb::open(
        repo,
        blobs,
        cache.clone(),
        &snapshot(1, vec![pack], None),
        None,
    )
    .await
    .expect("open");

    assert_eq!(
        odb.missing_from_closure(&[tip], &[], None)
            .await
            .expect("walk"),
        vec![left_out],
        "the one missing blob"
    );
    assert_eq!(
        odb.missing_from_closure(&[first], &[], None)
            .await
            .expect("walk"),
        Vec::<ObjectId>::new(),
        "the first commit is whole"
    );
    assert_eq!(
        odb.missing_from_closure(&[absent()], &[], None)
            .await
            .expect("walk"),
        vec![absent()],
        "a missing tip"
    );

    // A pushed pack (`extra`) that carries the blob completes the closure.
    let (pushed_id, pushed_paths) = repo_paths(6);
    let pushed_blobs = Arc::new(StoreBlobStore::new(store, pushed_paths));
    let pushed = put_pack(&pushed_blobs, git.pack_objects(&[left_out])).await;
    // The pushed pack is read through a cache that keeps nothing, so every
    // section it needs is a download.
    let (_, uncached) = self::cache(1);
    let extra = PackCacheOdb::open(
        pushed_id,
        pushed_blobs,
        uncached,
        &snapshot(1, vec![pushed], None),
        None,
    )
    .await
    .expect("open");
    let gets = faulty.calls(Op::Get);
    assert_eq!(
        odb.missing_from_closure(&[tip], &[], Some(&extra))
            .await
            .expect("walk"),
        Vec::<ObjectId>::new()
    );
    assert!(
        faulty.calls(Op::Get) - gets <= 2,
        "the pushed pack's sections are read once per walk, not once per object"
    );
}

#[tokio::test]
async fn cache_eviction_never_breaks_reads() {
    let Some(git) = GitRepo::init("cache_eviction_never_breaks_reads") else {
        return;
    };
    let first = git.commit(&[("a.txt", "one\n")], "first");
    let second = git.commit(&[("b.txt", &"two\n".repeat(500))], "second");
    let (faulty, store) = faulty_store();
    let (repo, paths) = repo_paths(7);
    let blobs = Arc::new(StoreBlobStore::new(store, paths));
    let p1 = put_pack(&blobs, git.pack_revs(&[&first.to_string()], &[])).await;
    let range = format!("{first}..{second}");
    let p2 = put_pack(&blobs, git.pack_revs(&[&range], &[])).await;
    let mut ids = git.objects(&["main"]);
    ids.sort();
    let snap = snapshot(2, vec![p1.clone(), p2.clone()], None);

    // A cache of one byte keeps nothing: every read downloads again.
    let (fs, tiny) = cache(1);
    let odb = PackCacheOdb::open(repo.clone(), blobs.clone(), tiny.clone(), &snap, None)
        .await
        .expect("open");
    for _ in 0..2 {
        let gets = faulty.calls(Op::Get);
        assert_reads_like_git(&odb, &git, &ids).await;
        assert!(faulty.calls(Op::Get) > gets, "every pass downloads");
    }
    assert_eq!(tiny.cached_bytes(), 0);
    assert!(
        fs.list(&FsPath::root(), None, 100)
            .await
            .expect("list")
            .is_empty(),
        "nothing is kept"
    );

    // A cache that holds everything serves the second pass without a GET.
    let (fs, roomy) = cache(CACHE_BYTES);
    let odb = PackCacheOdb::open(repo.clone(), blobs.clone(), roomy.clone(), &snap, None)
        .await
        .expect("open");
    assert_reads_like_git(&odb, &git, &ids).await;
    let gets = faulty.calls(Op::Get);
    assert_reads_like_git(&odb, &git, &ids).await;
    assert_eq!(
        faulty.calls(Op::Get),
        gets,
        "a warm cache downloads nothing"
    );
    let held = roomy.cached_bytes();
    assert_eq!(held, p1.pack_len + p1.idx_len + p2.pack_len + p2.idx_len);

    // Deleting the cache's files mid-run only costs downloads.
    for (path, _) in fs.list(&FsPath::root(), None, 100).await.expect("list") {
        fs.remove(&path).await.expect("remove");
    }
    assert_reads_like_git(&odb, &git, &ids).await;
    assert!(
        faulty.calls(Op::Get) > gets,
        "deleted files are downloaded again"
    );

    // A cache that holds one pack evicts the least recently used one.
    let budget = p2.pack_len + p2.idx_len + p1.idx_len;
    let (fs, small) = cache(budget);
    let odb = PackCacheOdb::open(repo, blobs, small.clone(), &snap, None)
        .await
        .expect("open");
    assert_reads_like_git(&odb, &git, &ids).await;
    assert!(small.cached_bytes() <= budget, "the budget holds");
    let kept = fs.list(&FsPath::root(), None, 100).await.expect("list");
    let kept_bytes: u64 = kept.iter().map(|(_, meta)| meta.len).sum();
    assert_eq!(
        kept_bytes,
        small.cached_bytes(),
        "accounting matches the files"
    );
}
