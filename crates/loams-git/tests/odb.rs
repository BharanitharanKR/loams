//! The object database trait and the pack-cache `Odb` (§36 §4.7, §5.3;
//! plan GT1 Task 9, old GT1 Task 5).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use loams_cache::{RangeCache, RangeCacheConfig};
use loams_fs::{Fs, FsPath, MemFs};
use loams_git::format::pb;
use loams_git::limits::MAX_FORK_DEPTH;
use loams_git::odb::{ObjectDb, OdbError, PackCache, PackCacheOdb, RangeOdb, StoreRange, WsOdb};
use loams_git::{ObjectId, RefSnapshot, Seq, StoreBlobStore, WsId};
use loams_store::Op;

use support::{GitRepo, faulty_store, oid, put_pack, repo_paths};

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

// --- `RangeOdb` and `WsOdb` (plan GT1 Task 17) -------------------------

/// `git verify-pack -v` of `pack`: per object, its offset and its bytes in
/// the pack.
fn verify_pack(git: &GitRepo, (pack, idx): &(Bytes, Bytes)) -> BTreeMap<ObjectId, (u64, u64)> {
    let dir = git.path().join("verify");
    std::fs::create_dir_all(&dir).expect("mkdir");
    std::fs::write(dir.join("pack-v.pack"), pack).expect("write");
    std::fs::write(dir.join("pack-v.idx"), idx).expect("write");
    let out = String::from_utf8(git.git(&["verify-pack", "-v", "verify/pack-v.idx"], None))
        .expect("utf-8");
    let mut found = BTreeMap::new();
    for line in out.lines() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 5 || cols[0].len() != 40 {
            continue;
        }
        let in_pack: u64 = cols[3].parse().expect("size in pack");
        let offset: u64 = cols[4].parse().expect("offset");
        found.insert(oid(cols[0]), (offset, in_pack));
    }
    found
}

/// Every object's kind and size, as `git cat-file --batch-check` says.
fn cat_file_sizes(git: &GitRepo) -> BTreeMap<ObjectId, (String, u64)> {
    let out = String::from_utf8(git.git(
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ],
        None,
    ))
    .expect("utf-8");
    out.lines()
        .map(|line| {
            let mut cols = line.split(' ');
            let id = oid(cols.next().expect("id"));
            let kind = cols.next().expect("kind").to_string();
            let size = cols.next().expect("size").parse().expect("size");
            (id, (kind, size))
        })
        .collect()
}

/// A history whose packs hold delta chains: one large file changed a little
/// in each commit, beside small files and an annotated tag.
fn delta_history(git: &GitRepo) {
    let mut text: String = (0..400)
        .map(|i| format!("line {i} of the file\n"))
        .collect();
    for round in 0..8 {
        text.push_str(&format!("appended in round {round}\n"));
        let small = format!("small {round}\n");
        git.commit(
            &[("big.txt", &text), (&format!("d/{round}.txt"), &small)],
            &format!("round {round}"),
        );
    }
    git.commit(&[("empty.txt", "")], "an empty blob");
    git.git(&["tag", "-a", "-m", "annotated", "v1"], None);
}

async fn range_odb(blobs: Arc<StoreBlobStore>, packs: Vec<pb::PackRef>) -> RangeOdb {
    let (repo, _) = repo_paths(20);
    RangeOdb::open(repo, blobs, &snapshot(1, packs, None), None)
        .await
        .expect("open")
}

async fn h1(store: &loams_store::Store, block: u64, memory: usize) -> RangeCache {
    RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: block,
            memory_bytes: memory,
            disk: None,
        },
    )
    .await
    .expect("range cache")
}

#[tokio::test]
async fn locate_matches_git_cat_file() {
    let Some(git) = GitRepo::init("locate_matches_git_cat_file") else {
        return;
    };
    delta_history(&git);
    let sizes = cat_file_sizes(&git);
    for flags in [&[][..], &["--delta-base-offset"][..]] {
        let files = git.pack_revs(&["main", "v1"], flags);
        let verified = verify_pack(&git, &files);
        assert_eq!(verified.len(), sizes.len(), "every object is packed");

        let (_, store) = faulty_store();
        let (_, paths) = repo_paths(20);
        let blobs = Arc::new(StoreBlobStore::new(store, paths));
        let pack = put_pack(&blobs, files).await;
        let odb = range_odb(blobs, vec![pack.clone()]).await;
        for (id, (offset, _)) in &verified {
            let loc = odb.locate(id).await.expect("locate").expect("found");
            assert_eq!((loc.layer, loc.pack, loc.offset), (0, 0, *offset), "{id}");
            assert_eq!(odb.pack_ref(&loc), Some(&pack));
        }
        assert_eq!(odb.locate(&absent()).await.expect("locate"), None);
    }
}

#[tokio::test]
async fn read_resolves_ofs_and_ref_deltas() {
    let Some(git) = GitRepo::init("read_resolves_ofs_and_ref_deltas") else {
        return;
    };
    delta_history(&git);
    let mut ids = git.objects(&["--all"]);
    ids.sort();
    ids.dedup();
    for flags in [&[][..], &["--delta-base-offset"][..]] {
        let files = git.pack_revs(&["main", "v1"], flags);
        let verified = verify_pack(&git, &files);
        let listing = String::from_utf8(git.git(&["verify-pack", "-v", "verify/pack-v.idx"], None))
            .expect("utf-8");
        assert!(
            listing.contains("chain length = "),
            "the pack holds deltas ({flags:?})"
        );

        let (_, store) = faulty_store();
        let (_, paths) = repo_paths(21);
        let blobs = Arc::new(StoreBlobStore::new(store, paths));
        let pack = put_pack(&blobs, files.clone()).await;
        let odb = range_odb(blobs, vec![pack]).await;
        assert_reads_like_git(&odb, &git, &ids).await;
        // A second pass is served partly from the resolved bases.
        assert_reads_like_git(&odb, &git, &ids).await;
        assert!(odb.stats().base_hits > 0, "bases are cached");

        // Raw entries are the stored bytes, header and compressed data.
        for (id, (offset, in_pack)) in &verified {
            let loc = odb.locate(id).await.expect("locate").expect("found");
            let raw = odb.raw_entry(&loc).await.expect("raw entry");
            let start = *offset as usize;
            assert_eq!(raw.len() as u64, *in_pack, "{id}'s entry length");
            assert_eq!(
                raw.as_ref(),
                &files.0[start..start + raw.len()],
                "{id}'s entry"
            );
        }
    }
}

#[tokio::test]
async fn header_size_matches_cat_file_s_without_inflate() {
    let Some(git) = GitRepo::init("header_size_matches_cat_file_s_without_inflate") else {
        return;
    };
    delta_history(&git);
    let sizes = cat_file_sizes(&git);
    for flags in [&[][..], &["--delta-base-offset"][..]] {
        let (_, store) = faulty_store();
        let (_, paths) = repo_paths(22);
        let blobs = Arc::new(StoreBlobStore::new(store, paths));
        let pack = put_pack(&blobs, git.pack_revs(&["main", "v1"], flags)).await;
        let odb = range_odb(blobs, vec![pack]).await;
        let mut deltas = 0;
        for (id, (kind, size)) in &sizes {
            let loc = odb.locate(id).await.expect("locate").expect("found");
            let header = odb.header(&loc).await.expect("header");
            assert_eq!(kind_name(header.kind), kind, "{id}'s kind");
            assert_eq!(header.size, *size, "{id}'s size");
            deltas += usize::from(header.base.is_some());
        }
        assert!(deltas > 0, "some entries are deltas ({flags:?})");
        let stats = odb.stats();
        assert_eq!(stats.inflates, 0, "no object is inflated");
        assert_eq!(
            stats.prefix_inflates, deltas as u64,
            "each delta inflates only its size prefix"
        );
    }
}

#[tokio::test]
async fn midx_lookup_matches_per_pack() {
    let Some(git) = GitRepo::init("midx_lookup_matches_per_pack") else {
        return;
    };
    // Three packs in the repository, then a multi-pack index over them, then
    // a fourth pack the index does not cover. No automatic gc may repack.
    git.git(&["config", "gc.auto", "0"], None);
    git.git(&["config", "maintenance.auto", "false"], None);
    for round in 0..3 {
        git.commit(
            &[(&format!("f{round}.txt"), &format!("file {round}\n"))],
            &format!("c{round}"),
        );
        git.git(&["repack", "-q"], None);
    }
    git.git(&["multi-pack-index", "write"], None);
    git.commit(&[("late.txt", "late\n")], "late");
    git.git(&["repack", "-q"], None);

    let pack_dir = git.path().join(".git/objects/pack");
    let (_, store) = faulty_store();
    let (_, paths) = repo_paths(23);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let mut packs = Vec::new();
    for entry in std::fs::read_dir(&pack_dir).expect("pack dir") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|ext| ext == "pack") {
            let pack = std::fs::read(&path).expect("pack");
            let idx = std::fs::read(path.with_extension("idx")).expect("idx");
            packs.push(put_pack(&blobs, (pack.into(), idx.into())).await);
        }
    }
    assert_eq!(packs.len(), 4, "four packs");
    let midx = std::fs::read(pack_dir.join("multi-pack-index")).expect("midx");
    let midx_path = "ns/7/repos/midx-test/midx/00000000000000000001.midx";
    store
        .put(midx_path, Bytes::from(midx.clone()))
        .await
        .expect("put midx");

    let per_pack = range_odb(blobs.clone(), packs.clone()).await;
    let with_midx = range_odb(blobs, packs)
        .await
        .with_midx(Arc::new(StoreRange::new(
            store,
            midx_path,
            midx.len() as u64,
        )));
    let mut ids = git.objects(&["--all"]);
    ids.sort();
    ids.dedup();
    for id in &ids {
        let a = per_pack.locate(id).await.expect("locate").expect("found");
        let b = with_midx.locate(id).await.expect("locate").expect("found");
        assert_eq!(per_pack.pack_ref(&a), with_midx.pack_ref(&b), "{id}'s pack");
        assert_eq!(a.offset, b.offset, "{id}'s offset");
    }
    // The index answered for the first three packs' objects; the late
    // pack's were found by its idx.
    let late = git.objects(&["main", "^main~1"]);
    assert_eq!(
        with_midx.stats().midx_hits,
        (ids.len() - late.len()) as u64,
        "the multi-pack index answers for the packs it covers"
    );
    assert_eq!(per_pack.stats().midx_hits, 0);
    assert_reads_like_git(&with_midx, &git, &ids).await;
    assert_eq!(with_midx.locate(&absent()).await.expect("locate"), None);
}

#[tokio::test]
async fn range_reads_are_coalesced() {
    let Some(git) = GitRepo::init("range_reads_are_coalesced") else {
        return;
    };
    let names: Vec<String> = (0..1000).map(|i| format!("f/{i:04}.txt")).collect();
    let texts: Vec<String> = (0..1000)
        .map(|i| format!("file {i}: {}\n", "x".repeat(i % 97)))
        .collect();
    let files: Vec<(&str, &str)> = names
        .iter()
        .zip(&texts)
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    git.commit(&files, "a thousand files");
    let mut ids = git.objects(&["main"]);
    ids.sort();
    assert!(ids.len() > 1000);

    let (faulty, store) = faulty_store();
    let (_, paths) = repo_paths(24);
    let cache = h1(&store, 1024 * 1024, 64 * 1024 * 1024).await;
    let blobs = Arc::new(StoreBlobStore::new(store, paths).with_range_cache(cache));
    let pack = put_pack(&blobs, git.pack_revs(&["main"], &[])).await;
    let odb = range_odb(blobs, vec![pack]).await;
    let gets = faulty.calls(Op::Get);
    assert_reads_like_git(&odb, &git, &ids).await;
    let used = faulty.calls(Op::Get) - gets;
    assert!(used < 50, "{} objects took {used} GETs", ids.len());
}

#[tokio::test]
async fn fork_parent_lookup() {
    let Some(git) = GitRepo::init("fork_parent_lookup") else {
        return;
    };
    let first = git.commit(&[("a.txt", "first\n")], "first");
    let forked = git.commit(&[("f.txt", "fork only\n")], "fork");

    let (_, store) = faulty_store();
    let (parent_id, parent_paths) = repo_paths(25);
    let (child_id, child_paths) = repo_paths(26);
    let parent_blobs = Arc::new(StoreBlobStore::new(store.clone(), parent_paths));
    let child_blobs = Arc::new(StoreBlobStore::new(store, child_paths));
    let p1 = put_pack(&parent_blobs, git.pack_revs(&[&first.to_string()], &[])).await;
    let range = format!("{first}..{forked}");
    let c1 = put_pack(&child_blobs, git.pack_revs(&[&range], &[])).await;

    let parent = Arc::new(
        RangeOdb::open(
            parent_id.clone(),
            parent_blobs,
            &snapshot(1, vec![p1], None),
            None,
        )
        .await
        .expect("open"),
    );
    // A parent at another seq, or none, is refused.
    let wrong = snapshot(1, vec![c1.clone()], Some((parent_id.as_str(), 2)));
    let refused = RangeOdb::open(
        child_id.clone(),
        child_blobs.clone(),
        &wrong,
        Some(parent.clone()),
    );
    assert!(matches!(
        refused.await,
        Err(OdbError::ParentMismatch { .. })
    ));
    let named = snapshot(1, vec![c1], Some((parent_id.as_str(), 1)));
    let refused = RangeOdb::open(child_id.clone(), child_blobs.clone(), &named, None);
    assert!(matches!(
        refused.await,
        Err(OdbError::ParentMismatch { .. })
    ));

    let child = RangeOdb::open(child_id, child_blobs, &named, Some(parent))
        .await
        .expect("open");
    assert_eq!(child.depth(), 1);
    let mut ids = git.objects(&["main"]);
    ids.sort();
    ids.dedup();
    assert_reads_like_git(&child, &git, &ids).await;
    let own = child.locate(&forked).await.expect("locate").expect("found");
    assert_eq!(own.layer, 0, "the fork's commit is its own");
    let inherited = child.locate(&first).await.expect("locate").expect("found");
    assert_eq!(inherited.layer, 1, "the first commit is the parent's");
    let missing = child
        .missing_from_closure(&[forked], &[], None)
        .await
        .expect("closure");
    assert!(
        missing.is_empty(),
        "the closure is complete through the parent"
    );
}

#[tokio::test]
async fn ws_packs_shadow_nothing_and_add_objects() {
    let Some(git) = GitRepo::init("ws_packs_shadow_nothing_and_add_objects") else {
        return;
    };
    let base = git.commit(&[("a.txt", "base\n")], "base");
    let edit = git.commit(&[("b.txt", "workspace edit\n")], "edit");
    let base_objects = git.objects(&[&base.to_string()]);
    let ws_objects: Vec<ObjectId> = git
        .objects(&[&edit.to_string(), &format!("^{base}")])
        .into_iter()
        .collect();

    let (_, store) = faulty_store();
    let (repo, paths) = repo_paths(27);
    let ws = WsId::parse("w01j9z3m5q8x000000000000001").expect("ws id");
    let repo_blobs = Arc::new(StoreBlobStore::new(store.clone(), paths.clone()));
    let ws_blobs = Arc::new(StoreBlobStore::workspace(store.clone(), &paths, &ws));
    let repo_pack = put_pack(&repo_blobs, git.pack_revs(&[&base.to_string()], &[])).await;
    // The workspace pack also carries a copy of a repository object.
    let mut ws_pack_objects = ws_objects.clone();
    ws_pack_objects.push(base);
    let ws_pack = put_pack(&ws_blobs, git.pack_objects(&ws_pack_objects)).await;
    assert!(
        store
            .head(&paths.ws_pack(&ws, &oid(&ws_pack.checksum)))
            .await
            .is_ok(),
        "the workspace pack is under the workspace's prefix"
    );

    let repo_odb = RangeOdb::open(repo, repo_blobs, &snapshot(1, vec![repo_pack], None), None)
        .await
        .expect("open");
    let ws_odb = WsOdb::open(&repo_odb, ws_blobs, &[ws_pack]).expect("open");
    let mut all = git.objects(&["main"]);
    all.sort();
    all.dedup();
    assert_reads_like_git(&ws_odb, &git, &all).await;
    for id in &base_objects {
        let loc = ws_odb.locate(id).await.expect("locate").expect("found");
        assert!(
            !ws_odb.is_workspace(&loc),
            "{id} is read from the repository"
        );
    }
    for id in &ws_objects {
        let loc = ws_odb.locate(id).await.expect("locate").expect("found");
        assert!(ws_odb.is_workspace(&loc), "{id} is the workspace's");
        assert!(
            !repo_odb.contains(id).await.expect("contains"),
            "{id} is not the repository's"
        );
    }
}

#[tokio::test]
async fn cache_dropped_mid_read_still_correct() {
    let Some(git) = GitRepo::init("cache_dropped_mid_read_still_correct") else {
        return;
    };
    delta_history(&git);
    let mut ids = git.objects(&["--all"]);
    ids.sort();
    ids.dedup();

    let (faulty, store) = faulty_store();
    let (_, paths) = repo_paths(28);
    // Small blocks and a small budget, so blocks are evicted as well.
    let cache = h1(&store, 4096, 64 * 1024).await;
    let raw_blobs = StoreBlobStore::new(store, paths);
    let blobs = Arc::new(raw_blobs.clone().with_range_cache(cache.clone()));
    let pack = put_pack(
        &blobs,
        git.pack_revs(&["main", "v1"], &["--delta-base-offset"]),
    )
    .await;
    let path = raw_blobs.path(&loams_git::BlobId::parse(&pack.checksum).expect("id"));
    let odb = range_odb(blobs, vec![pack]).await;

    let (first, second) = ids.split_at(ids.len() / 2);
    assert_reads_like_git(&odb, &git, first).await;
    // Drop every cache: the H1 blocks and size, and the database's own.
    cache.forget(&path).await;
    odb.drop_caches();
    let gets = faulty.calls(Op::Get);
    assert_reads_like_git(&odb, &git, second).await;
    assert_reads_like_git(&odb, &git, first).await;
    assert!(faulty.calls(Op::Get) > gets, "dropped data is read again");
    for id in &ids {
        let loc = odb.locate(id).await.expect("locate").expect("found");
        odb.header(&loc).await.expect("header");
    }
}
