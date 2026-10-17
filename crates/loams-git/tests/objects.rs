//! Lazy object reads for `ObjectService` and the object route (§36
//! §19.5.1–§19.5.2; plan GT1 Task 18).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use loams_git::format::pb;
use loams_git::objects::{
    MAX_TREE_DEPTH, MemWsLookup, ObjectRead, ObjectsError, WsLookup, WsView, bitmap, get_objects,
    get_trees, grant_reads_workspace, has_objects, kind_name, loose_object, workspace_odb,
};
use loams_git::odb::RangeOdb;
use loams_git::{
    DerivedStore, GitAccess, GitGrant, NamespaceId, ObjectId, Principal, RefSnapshot, RepoMatch,
    Seq, StoreBlobStore, WsId, WsMatch,
};
use loams_store::Store;

use support::{GitRepo, faulty_store, oid, put_pack, repo_paths};

fn snapshot(packs: Vec<pb::PackRef>) -> RefSnapshot {
    RefSnapshot {
        seq: Seq(1),
        refs: BTreeMap::new(),
        symrefs: BTreeMap::new(),
        protections: Vec::new(),
        packs,
        parent: None,
    }
}

async fn odb_of(store: &Store, byte: u8, packs: Vec<(bytes::Bytes, bytes::Bytes)>) -> RangeOdb {
    let (repo, paths) = repo_paths(byte);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let mut refs = Vec::new();
    for files in packs {
        refs.push(put_pack(&blobs, files).await);
    }
    RangeOdb::open(repo, blobs, &snapshot(refs), None)
        .await
        .expect("open")
}

fn absent() -> ObjectId {
    oid("00000000000000000000000000000000deadbeef")
}

async fn collect(odb: Arc<RangeOdb>, oids: Vec<ObjectId>) -> Vec<ObjectRead> {
    get_objects(odb, oids)
        .await
        .expect("a batch")
        .try_collect()
        .await
        .expect("reads")
}

#[tokio::test]
async fn get_objects_round_trip_and_order() {
    let Some(git) = GitRepo::init("get_objects_round_trip_and_order") else {
        return;
    };
    let mut text: String = (0..200).map(|i| format!("line {i}\n")).collect();
    let first = git.commit(&[("a.txt", &text), ("d/b.txt", "bee\n")], "first");
    text.push_str("more\n");
    let second = git.commit(&[("a.txt", &text)], "second");
    let (_, store) = faulty_store();
    let range = format!("{first}..{second}");
    let odb = Arc::new(
        odb_of(
            &store,
            50,
            vec![
                git.pack_revs(&[&first.to_string()], &[]),
                git.pack_revs(&[&range], &["--delta-base-offset"]),
            ],
        )
        .await,
    );
    let mut ids = git.objects(&["main"]);
    ids.reverse();
    let mut asked = ids.clone();
    asked.insert(1, absent());
    asked.push(ids[0]);

    let reads = collect(odb.clone(), asked).await;
    assert_eq!(
        reads.len(),
        ids.len() + 1,
        "each object once, and the absent one"
    );
    assert_eq!(
        reads.last(),
        Some(&ObjectRead::Missing(absent())),
        "missing last"
    );
    let mut order = Vec::new();
    for read in &reads[..ids.len()] {
        let ObjectRead::Found { oid, kind, data } = read else {
            panic!("found {read:?}");
        };
        let (git_kind, git_data) = git.cat(oid);
        assert_eq!(kind_name(*kind), git_kind);
        assert_eq!(data.as_ref(), git_data.as_slice());
        let loc = odb.locate(oid).await.expect("locate").expect("held");
        order.push((loc.layer, loc.pack, loc.offset));
    }
    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(order, sorted, "objects come in pack order");
}

#[tokio::test]
async fn batch_over_limit_is_invalid_argument() {
    let (_, store) = faulty_store();
    let odb = Arc::new(odb_of(&store, 51, Vec::new()).await);
    let at_limit = vec![absent(); loams_git::limits::FETCH_BATCH_OIDS];
    assert!(get_objects(odb.clone(), at_limit.clone()).await.is_ok());
    assert!(has_objects(&odb, &at_limit).await.is_ok());
    let mut over = at_limit;
    over.push(absent());
    assert!(matches!(
        get_objects(odb.clone(), over.clone()).await,
        Err(ObjectsError::TooMany {
            max: 1024,
            got: 1025
        })
    ));
    assert!(matches!(
        has_objects(&odb, &over).await,
        Err(ObjectsError::TooMany { .. })
    ));
}

#[tokio::test]
async fn get_trees_depth_two_has_sizes() {
    let Some(git) = GitRepo::init("get_trees_depth_two_has_sizes") else {
        return;
    };
    let head = git.commit(
        &[
            ("top.txt", "top\n"),
            ("a/one.txt", "one one\n"),
            ("a/b/two.txt", "two two two\n"),
            ("a/b/c/three.txt", "three\n"),
            ("z/four.txt", "four\n"),
        ],
        "nested",
    );
    let (_, store) = faulty_store();
    let odb = odb_of(&store, 52, vec![git.pack_revs(&["main"], &[])]).await;
    let (_, paths) = repo_paths(52);
    let derived = DerivedStore::new(store, paths);

    // From the commit (peeled to its tree), two levels below the root.
    let got = get_trees(&odb, &derived, &head, 2).await.expect("trees");
    assert!(!got.truncated);
    let names: Vec<ObjectId> = got.trees.iter().map(|t| t.tree).collect();
    let expect: Vec<ObjectId> = ["main^{tree}", "main:a", "main:z", "main:a/b"]
        .iter()
        .map(|rev| git.rev(rev))
        .collect();
    assert_eq!(names, expect, "breadth first, to depth 2");
    let top = &got.trees[0];
    let size = |name: &str| {
        top.entries
            .iter()
            .find(|e| e.name == name.as_bytes())
            .and_then(|e| e.size)
    };
    assert_eq!(size("top.txt"), Some(4));
    assert_eq!(size("a"), None, "trees have no size");
    let b = &got.trees[3];
    assert_eq!(
        b.entries
            .iter()
            .find(|e| e.name == b"two.txt")
            .and_then(|e| e.size),
        Some(12)
    );

    let only_root = get_trees(&odb, &derived, &git.rev("main^{tree}"), 0)
        .await
        .expect("root");
    assert_eq!(only_root.trees.len(), 1);
    assert!(matches!(
        get_trees(&odb, &derived, &head, MAX_TREE_DEPTH + 1).await,
        Err(ObjectsError::TooDeep { .. })
    ));
    assert!(matches!(
        get_trees(&odb, &derived, &absent(), 1).await,
        Err(ObjectsError::NotFound(_))
    ));
}

#[tokio::test]
async fn has_objects_bitmap() {
    let Some(git) = GitRepo::init("has_objects_bitmap") else {
        return;
    };
    git.commit(&[("a.txt", "a\n")], "one");
    let (_, store) = faulty_store();
    let odb = odb_of(&store, 53, vec![git.pack_revs(&["main"], &[])]).await;
    let ids = git.objects(&["main"]);
    // held, absent, held, absent... over more than a byte.
    let mut asked = Vec::new();
    for i in 0..10 {
        asked.push(if i % 2 == 0 {
            ids[i % ids.len()]
        } else {
            absent()
        });
    }
    let held = has_objects(&odb, &asked).await.expect("has");
    assert_eq!(held, (0..10).map(|i| i % 2 == 0).collect::<Vec<_>>());
    assert_eq!(bitmap(&held), vec![0b0101_0101, 0b0000_0001]);
    assert_eq!(bitmap(&[]), Vec::<u8>::new());
}

#[tokio::test]
async fn ws_object_needs_ws_access() {
    let Some(git) = GitRepo::init("ws_object_needs_ws_access") else {
        return;
    };
    let base = git.commit(&[("a.txt", "base\n")], "base");
    let edit = git.commit(&[("w.txt", "workspace only\n")], "edit");
    let only_ws = git.rev("main:w.txt");
    let (_, store) = faulty_store();
    let (repo, paths) = repo_paths(54);
    let repo_blobs = Arc::new(StoreBlobStore::new(store.clone(), paths.clone()));
    let pack = put_pack(&repo_blobs, git.pack_revs(&[&base.to_string()], &[])).await;
    let repo_odb = RangeOdb::open(repo.clone(), repo_blobs, &snapshot(vec![pack]), None)
        .await
        .expect("open");
    let ws = WsId::parse("w01j9z3m5q8x000000000000001").expect("ws id");
    let ws_blobs = Arc::new(StoreBlobStore::workspace(store.clone(), &paths, &ws));
    let range = format!("{base}..{edit}");
    let ws_pack = put_pack(&ws_blobs, git.pack_revs(&[&range], &[])).await;
    let lookup = MemWsLookup::default();
    let owner = Principal::parse("alice").expect("principal");
    lookup.insert(
        &repo,
        &ws,
        WsView {
            owner: owner.clone(),
            packs: vec![ws_pack],
            sources: Vec::new(),
        },
    );

    // Without the workspace the object does not exist.
    assert!(
        loose_object(&repo_odb, &only_ws)
            .await
            .expect("read")
            .is_none()
    );
    let held = has_objects(&repo_odb, &[only_ws]).await.expect("has");
    assert_eq!(held, vec![false]);

    // Through the workspace it does.
    let view = lookup
        .workspace(&repo, &ws)
        .await
        .expect("lookup")
        .expect("ws");
    let ws_odb = workspace_odb(&repo_odb, ws_blobs, &view).expect("ws odb");
    assert_eq!(
        has_objects(&ws_odb, &[only_ws]).await.expect("has"),
        vec![true]
    );
    let other = WsId::parse("w01j9z3m5q8x000000000000002").expect("ws id");
    assert!(
        lookup
            .workspace(&repo, &other)
            .await
            .expect("lookup")
            .is_none()
    );

    // A listener token reads it only when its `WsMatch` reaches it.
    let grant = |principal: &str, workspaces: WsMatch| GitGrant {
        org: "local".into(),
        namespace: NamespaceId(7),
        principal: Principal::parse(principal).expect("principal"),
        repos: RepoMatch::All,
        access: GitAccess::Read,
        branch_prefixes: Vec::new(),
        cones: Vec::new(),
        workspaces,
        env: Vec::new(),
        cache_class: None,
        registry_read: false,
    };
    assert!(grant_reads_workspace(
        &grant("alice", WsMatch::Own),
        &ws,
        &view
    ));
    assert!(!grant_reads_workspace(
        &grant("bob", WsMatch::Own),
        &ws,
        &view
    ));
    assert!(grant_reads_workspace(
        &grant("bob", WsMatch::All),
        &ws,
        &view
    ));
    assert!(grant_reads_workspace(
        &grant("bob", WsMatch::Ids(vec![ws.clone()])),
        &ws,
        &view
    ));
    assert!(!grant_reads_workspace(
        &grant("bob", WsMatch::Ids(vec![other])),
        &ws,
        &view
    ));
}

#[tokio::test]
async fn object_route_is_immutable_cacheable() {
    // The route's body: git's loose format, which git itself reads.
    let Some(git) = GitRepo::init("object_route_is_immutable_cacheable") else {
        return;
    };
    let head = git.commit(&[("a.txt", "loose\n")], "one");
    let (_, store) = faulty_store();
    let odb = odb_of(&store, 55, vec![git.pack_revs(&["main"], &[])]).await;
    let Some(reader) = GitRepo::init("object_route_reader") else {
        return;
    };
    for id in git.objects(&["main"]) {
        let body = loose_object(&odb, &id).await.expect("read").expect("held");
        let hex = id.to_hex().to_string();
        let dir = reader.path().join(".git/objects").join(&hex[..2]);
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join(&hex[2..]), &body).expect("write");
        assert_eq!(reader.cat(&id), git.cat(&id), "{id}");
    }
    // Equal objects compress to equal bytes, so the route's answer is
    // immutable.
    assert_eq!(
        loose_object(&odb, &head).await.expect("read"),
        loose_object(&odb, &head).await.expect("read")
    );
    assert!(loose_object(&odb, &absent()).await.expect("read").is_none());
}

/// The process's resident set, in bytes (Linux).
fn rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

#[tokio::test]
async fn get_objects_bounded_memory() {
    let Some(git) = GitRepo::init("get_objects_bounded_memory") else {
        return;
    };
    // 100,000 blobs through fast-import, in one pack.
    const COUNT: usize = 100_000;
    let mut input = Vec::new();
    for i in 0..COUNT {
        let body = format!("blob number {i} with some text to read back\n");
        input.extend_from_slice(format!("blob\nmark :{}\ndata {}\n", i + 1, body.len()).as_bytes());
        input.extend_from_slice(body.as_bytes());
        input.push(b'\n');
    }
    git.git(
        &["fast-import", "--quiet", "--export-marks=marks"],
        Some(&input),
    );
    let marks = std::fs::read_to_string(git.path().join("marks")).expect("marks");
    let ids: Vec<ObjectId> = marks
        .lines()
        .map(|line| oid(line.split(' ').nth(1).expect("an id")))
        .collect();
    assert_eq!(ids.len(), COUNT);
    let (_, store) = faulty_store();
    let odb = Arc::new(odb_of(&store, 56, vec![git.pack_objects(&ids)]).await);

    let before = rss();
    let mut peak = before.unwrap_or(0);
    let mut read = 0usize;
    for batch in ids.chunks(loams_git::limits::FETCH_BATCH_OIDS) {
        let mut stream = get_objects(odb.clone(), batch.to_vec())
            .await
            .expect("batch");
        while let Some(object) = stream.try_next().await.expect("read") {
            assert!(matches!(object, ObjectRead::Found { .. }));
            read += 1;
        }
        peak = peak.max(rss().unwrap_or(0));
    }
    assert_eq!(read, COUNT);
    if let Some(before) = before {
        let grew = peak.saturating_sub(before);
        assert!(grew < 128 * 1024 * 1024, "RSS grew by {grew} bytes");
    }
}
