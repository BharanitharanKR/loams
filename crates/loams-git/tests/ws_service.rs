//! What `WorkspaceService` (plan GT1 Task 25) builds on: the server's pack
//! writer, the save checks, and the salvage replay.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use gix_object::Kind;
use loams_git::format::pb;
use loams_git::format::snapshot::snapshot_commit;
use loams_git::objects::{WsView, workspace_odb};
use loams_git::odb::RangeOdb;
use loams_git::ws::{
    Edit, FileMode, NoScan, SaveCheckError, TreeBuilder, check_save_objects, put_workspace_pack,
    replay_salvage, write_ws_pack,
};
use loams_git::{ObjectId, Principal, RefSnapshot, RepoPath, Seq, StoreBlobStore, WsId};
use loams_store::Store;

use support::{GitRepo, put_pack, repo_paths};

fn ws() -> WsId {
    WsId::parse("w01j9z3m5q8x000000000000001").expect("ws id")
}

/// The objects `revs` reach, as a repository's object database.
async fn repo_odb(store: &Store, git: &GitRepo, revs: &[&str]) -> RangeOdb {
    let (repo, paths) = repo_paths(25);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let pack = put_pack(&blobs, git.pack_revs(revs, &[])).await;
    let snapshot = RefSnapshot {
        seq: Seq(1),
        refs: BTreeMap::new(),
        symrefs: BTreeMap::new(),
        protections: Vec::new(),
        packs: vec![pack],
        parent: None,
    };
    RangeOdb::open(repo, blobs, &snapshot, None)
        .await
        .expect("open")
}

/// `repo` with workspace packs `packs` searched last.
fn with_packs(store: &Store, repo: &RangeOdb, packs: Vec<pb::PackRef>) -> RangeOdb {
    let blobs = StoreBlobStore::workspace(store.clone(), &repo_paths(25).1, &ws());
    let view = WsView {
        owner: Principal::parse("alice").expect("principal"),
        packs,
        sources: Vec::new(),
    };
    workspace_odb(repo, Arc::new(blobs), &view).expect("ws odb")
}

/// Stores `objects` as one workspace pack; its `PackRef`.
async fn store_pack(store: &Store, objects: Vec<(Kind, Bytes)>) -> pb::PackRef {
    let (_, _, lpk) = write_ws_pack(objects).expect("pack");
    let blobs = StoreBlobStore::workspace(store.clone(), &repo_paths(25).1, &ws());
    let (verified, _) = put_workspace_pack(&repo_paths(25).0, &blobs, lpk, &NoScan)
        .await
        .expect("a verified pack");
    verified.pack
}

fn kind_of(name: &str) -> Kind {
    match name {
        "blob" => Kind::Blob,
        "tree" => Kind::Tree,
        "commit" => Kind::Commit,
        _ => Kind::Tag,
    }
}

/// The server's packs verify as uploads do, and git indexes them to the
/// same idx.
#[tokio::test]
async fn ws_pack_writer_matches_git_index_pack() {
    let Some(git) = GitRepo::init("ws_pack_writer_matches_git_index_pack") else {
        return;
    };
    let big: String = (0..20_000).map(|i| format!("line {i}\n")).collect();
    git.commit(
        &[("a.txt", "a\n"), ("dir/b.txt", "b\n"), ("big.txt", &big)],
        "one",
    );
    let objects: Vec<(Kind, Bytes)> = git
        .objects(&["HEAD"])
        .iter()
        .map(|oid| {
            let (kind, body) = git.cat(oid);
            (kind_of(&kind), Bytes::from(body))
        })
        .collect();
    let (_, pack_ref, lpk) = write_ws_pack(objects.clone()).expect("pack");
    assert_eq!(pack_ref.object_count as usize, objects.len());
    let pack = &lpk[..pack_ref.pack_len as usize];
    let idx = &lpk[pack_ref.idx_offset as usize..(pack_ref.idx_offset + pack_ref.idx_len) as usize];
    let file = git.path().join("written.pack");
    std::fs::write(&file, pack).expect("write");
    git.git(&["index-pack", "written.pack"], None);
    let by_git = std::fs::read(git.path().join("written.idx")).expect("idx");
    assert_eq!(idx, &by_git[..], "git's idx of the pack is the one written");

    let store = Store::in_memory();
    store_pack(&store, objects).await;
}

/// A save names trees, a head and a snapshot commit that exist, and its
/// new trees name only objects that exist.
#[tokio::test]
async fn save_check_needs_every_object() {
    let Some(git) = GitRepo::init("save_check_needs_every_object") else {
        return;
    };
    let head = git.commit(&[("a.txt", "a\n"), ("dir/b.txt", "b\n")], "one");
    let tree = git.rev("HEAD^{tree}");
    let store = Store::in_memory();
    let repo = repo_odb(&store, &git, &["HEAD"]).await;

    // A new file in a new directory: its blob and two new trees.
    let blob = Bytes::from_static(b"new\n");
    let blob_id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, &blob).expect("hash");
    let (root, trees) = TreeBuilder::apply(
        tree,
        [(
            RepoPath::parse("dir/sub/c.txt").expect("path"),
            Edit::Write {
                mode: FileMode::Regular,
                oid: blob_id,
            },
        )],
        &repo,
    )
    .await
    .expect("trees");
    assert_eq!(trees.len(), 3, "sub, dir and the root");
    let mut snap = pb::WorkspaceSnapshot {
        ws_id: ws().to_string(),
        n: 1,
        head: head.as_bytes().to_vec(),
        index_tree: tree.as_bytes().to_vec(),
        worktree_tree: root.as_bytes().to_vec(),
        created_unix_ms: 1_760_000_000_000,
        ..Default::default()
    };
    let (commit, commit_body) = snapshot_commit(&snap).expect("commit");
    snap.snapshot_commit = commit.as_bytes().to_vec();
    let tree_objects: Vec<(Kind, Bytes)> = trees
        .iter()
        .map(|(_, body)| (Kind::Tree, body.clone()))
        .collect();

    // The trees and the commit, but not the blob: not connected.
    let mut objects = tree_objects.clone();
    objects.push((Kind::Commit, commit_body.clone()));
    let thin = store_pack(&store, objects.clone()).await;
    let odb = with_packs(&store, &repo, vec![thin.clone()]);
    match check_save_objects(&odb, &snap).await {
        Err(SaveCheckError::Missing { oid, .. }) => assert_eq!(oid, blob_id),
        other => panic!("expected the blob missing: {other:?}"),
    }

    // Without the snapshot commit.
    let no_commit = store_pack(&store, {
        let mut objects = tree_objects.clone();
        objects.push((Kind::Blob, blob.clone()));
        objects
    })
    .await;
    let odb = with_packs(&store, &repo, vec![no_commit]);
    match check_save_objects(&odb, &snap).await {
        Err(SaveCheckError::Missing { oid, what }) => {
            assert_eq!((oid, what.as_str()), (commit, "snapshot_commit"));
        }
        other => panic!("expected the commit missing: {other:?}"),
    }

    // Everything: the save is accepted.
    objects.push((Kind::Blob, blob));
    let whole = store_pack(&store, objects).await;
    let odb = with_packs(&store, &repo, vec![thin, whole]);
    check_save_objects(&odb, &snap).await.expect("connected");

    // A head that is a tree, and a worktree that does not exist.
    let wrong = pb::WorkspaceSnapshot {
        head: tree.as_bytes().to_vec(),
        ..snap.clone()
    };
    assert!(matches!(
        check_save_objects(&odb, &wrong).await,
        Err(SaveCheckError::WrongKind { field: "head", .. })
    ));
    let absent = pb::WorkspaceSnapshot {
        worktree_tree: vec![0x5a; 20],
        ..snap.clone()
    };
    assert!(matches!(
        check_save_objects(&odb, &absent).await,
        Err(SaveCheckError::Missing { .. })
    ));
    let short = pb::WorkspaceSnapshot {
        index_tree: vec![1, 2, 3],
        ..snap
    };
    assert!(matches!(
        check_save_objects(&odb, &short).await,
        Err(SaveCheckError::Invalid {
            field: "index_tree"
        })
    ));
}

/// A salvaged state replays on the current one: changes to paths the
/// other writer left alone are written, the same change twice is one, and
/// a path both changed keeps the current entry and is a conflict.
#[tokio::test]
async fn salvage_replays_as_edits() {
    let Some(git) = GitRepo::init("salvage_replays_as_edits") else {
        return;
    };
    let files = |pairs: &[(&str, &str)]| -> ObjectId {
        for entry in std::fs::read_dir(git.path()).expect("dir") {
            let entry = entry.expect("entry");
            if entry.file_name() != ".git" {
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(path).expect("rm");
                } else {
                    std::fs::remove_file(path).expect("rm");
                }
            }
        }
        git.commit(pairs, "state");
        git.rev("HEAD^{tree}")
    };
    let base = files(&[("a", "1\n"), ("b", "1\n"), ("c", "1\n"), ("same", "1\n")]);
    // The other writer: changed a and same.
    let current = files(&[("a", "2\n"), ("b", "1\n"), ("c", "1\n"), ("same", "2\n")]);
    // The fenced writer: changed a and b and same (as the other did),
    // removed c, added dir/d.
    let salvaged = files(&[
        ("a", "3\n"),
        ("b", "3\n"),
        ("same", "2\n"),
        ("dir/d", "3\n"),
    ]);
    let want = files(&[
        ("a", "2\n"),
        ("b", "3\n"),
        ("same", "2\n"),
        ("dir/d", "3\n"),
    ]);
    let store = Store::in_memory();
    let odb = repo_odb(&store, &git, &["HEAD", "HEAD~1", "HEAD~2", "HEAD~3"]).await;
    let snap = |tree: ObjectId| pb::WorkspaceSnapshot {
        worktree_tree: tree.as_bytes().to_vec(),
        index_tree: base.as_bytes().to_vec(),
        ..Default::default()
    };
    let replayed = replay_salvage(&odb, &snap(base), &snap(salvaged), &snap(current))
        .await
        .expect("replay");
    assert_eq!(replayed.worktree_tree, want, "b, c and dir/d replayed");
    assert_eq!(replayed.index_tree, base, "the index did not change");
    assert_eq!(replayed.applied, 3);
    let blob = |rev: &str| git.rev(rev).as_bytes().to_vec();
    assert_eq!(
        replayed.conflicts,
        vec![pb::Conflict {
            path: "a".into(),
            base: blob("HEAD~3:a"),
            ours: blob("HEAD~2:a"),
            theirs: blob("HEAD~1:a"),
            mode: 0o100644,
        }]
    );
    // The trees it made are exactly the ones the result needs beyond the
    // repository's: the new root and `dir`.
    let made: Vec<ObjectId> = replayed
        .objects
        .iter()
        .map(|(kind, body)| gix_object::compute_hash(gix_hash::Kind::Sha1, *kind, body).unwrap())
        .collect();
    assert!(made.contains(&want), "{made:?}");
    assert!(made.contains(&git.rev("HEAD:dir")));
}
