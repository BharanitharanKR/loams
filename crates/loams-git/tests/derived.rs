//! Derived data: tree aux (§36 §19.5.1, D830; plan GT1 Task 19).

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use loams_git::derived::blame::{BLAME_END, BLAME_MAGIC, decode_blame, encode_blame};
use loams_git::derived::tree_aux::{
    TREE_AUX_END, TREE_AUX_MAGIC, decode_tree_aux, encode_tree_aux,
};
use loams_git::derived::{AuxEntry, BlameHunk, FileBlame, TreeAux, tree_aux};
use loams_git::format::FormatError;
use loams_git::odb::RangeOdb;
use loams_git::{DerivedStore, ObjectId, RefSnapshot, RepoPaths, Seq, StoreBlobStore};
use loams_store::{Op, Store};

use support::{GitRepo, faulty_store, oid, put_pack, repo_paths};

fn snapshot(packs: Vec<loams_git::format::pb::PackRef>) -> RefSnapshot {
    RefSnapshot {
        seq: Seq(1),
        refs: BTreeMap::new(),
        symrefs: BTreeMap::new(),
        protections: Vec::new(),
        packs,
        parent: None,
    }
}

/// A repository with blobs of several sizes (some stored as deltas), an
/// executable, a symlink, an empty file, nested trees and a submodule.
fn fixture(git: &GitRepo) {
    let mut big: String = (0..300).map(|i| format!("line {i}\n")).collect();
    for round in 0..4 {
        big.push_str(&format!("round {round}\n"));
        git.commit(
            &[
                ("big.txt", &big),
                ("src/lib.rs", &format!("// v{round}\n")),
                ("src/deep/a/b.txt", "deep\n"),
                ("empty", ""),
            ],
            &format!("round {round}"),
        );
    }
    std::fs::write(git.path().join("run.sh"), "#!/bin/sh\necho hi\n").expect("write");
    git.git(&["add", "run.sh"], None);
    git.git(&["update-index", "--chmod=+x", "run.sh"], None);
    std::os::unix::fs::symlink("big.txt", git.path().join("link")).expect("symlink");
    git.git(&["add", "link"], None);
    let sub = "1111111111111111111111111111111111111111";
    git.git(
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{sub},vendor/sub"),
        ],
        None,
    );
    git.git(&["commit", "-q", "-m", "modes"], None);
}

/// `git ls-tree -l` of `tree`, as aux entries.
fn ls_tree_long(git: &GitRepo, tree: &ObjectId) -> Vec<AuxEntry> {
    let out = git.git(&["ls-tree", "-l", "-z", &tree.to_string()], None);
    out.split(|b| *b == 0)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let tab = line.iter().position(|b| *b == b'\t').expect("a tab");
            let meta = std::str::from_utf8(&line[..tab]).expect("utf-8");
            let cols: Vec<&str> = meta.split_whitespace().collect();
            AuxEntry {
                name: line[tab + 1..].to_vec(),
                mode: u32::from_str_radix(cols[0], 8).expect("mode"),
                oid: oid(cols[2]),
                size: (cols[3] != "-").then(|| cols[3].parse().expect("size")),
            }
        })
        .collect()
}

/// Every tree reachable from `main`.
fn trees(git: &GitRepo) -> Vec<ObjectId> {
    let out = String::from_utf8(git.git(
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype)",
        ],
        None,
    ))
    .expect("utf-8");
    out.lines()
        .filter(|line| line.ends_with(" tree"))
        .map(|line| oid(&line[..40]))
        .collect()
}

async fn open(git: &GitRepo, byte: u8, store: Store) -> (RangeOdb, DerivedStore, RepoPaths) {
    let (repo, paths) = repo_paths(byte);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths.clone()));
    let pack = put_pack(&blobs, git.pack_revs(&["main"], &["--delta-base-offset"])).await;
    let odb = RangeOdb::open(repo, blobs, &snapshot(vec![pack]), None)
        .await
        .expect("open");
    (odb, DerivedStore::new(store, paths.clone()), paths)
}

#[tokio::test]
async fn tree_aux_matches_ls_tree_long() {
    let Some(git) = GitRepo::init("tree_aux_matches_ls_tree_long") else {
        return;
    };
    fixture(&git);
    let (_, store) = faulty_store();
    let (odb, derived, _) = open(&git, 40, store).await;
    let trees = trees(&git);
    assert!(trees.len() > 5, "several trees");
    let mut modes = std::collections::BTreeSet::new();
    for tree in &trees {
        let aux = tree_aux(&odb, &derived, tree).await.expect("tree aux");
        assert_eq!(aux.tree, *tree);
        assert_eq!(aux.entries, ls_tree_long(&git, tree), "tree {tree}");
        modes.extend(aux.entries.iter().map(|entry| entry.mode));
    }
    assert_eq!(
        modes.into_iter().collect::<Vec<_>>(),
        vec![0o40000, 0o100644, 0o100755, 0o120000, 0o160000],
        "every mode is covered"
    );
    assert_eq!(
        odb.stats().inflates as usize,
        trees.len(),
        "only trees are inflated"
    );
    // A blob is not a tree.
    let blob = git.rev("main:big.txt");
    assert!(matches!(
        tree_aux(&odb, &derived, &blob).await,
        Err(loams_git::DerivedError::WrongKind { .. })
    ));
}

#[tokio::test]
async fn tree_aux_is_cached_once() {
    let Some(git) = GitRepo::init("tree_aux_is_cached_once") else {
        return;
    };
    fixture(&git);
    let (faulty, store) = faulty_store();
    let (odb_a, derived_a, paths) = open(&git, 41, store.clone()).await;
    // A second node: its own object database and derived store.
    let (odb_b, derived_b, _) = open(&git, 41, store.clone()).await;
    let root = git.rev("main^{tree}");

    let puts = faulty.calls(Op::PutCreate);
    let (a, b) = tokio::join!(
        tree_aux(&odb_a, &derived_a, &root),
        tree_aux(&odb_b, &derived_b, &root)
    );
    let (a, b) = (a.expect("a"), b.expect("b"));
    assert_eq!(a, b, "both computers answer the same");
    assert!(faulty.calls(Op::PutCreate) - puts <= 2);
    let (stored, _) = store
        .get(&paths.derived_tree_aux(&root))
        .await
        .expect("stored");
    assert_eq!(decode_tree_aux(&stored).expect("decode"), *a);

    // Later reads use the stored object: no tree is read, nothing is put.
    let (odb_c, derived_c, _) = open(&git, 41, store).await;
    let puts = faulty.calls(Op::PutCreate);
    let again = tree_aux(&odb_c, &derived_c, &root).await.expect("again");
    assert_eq!(again, a);
    assert_eq!(odb_c.stats().inflates, 0, "the cached object is used");
    assert_eq!(faulty.calls(Op::PutCreate), puts, "nothing is written");
}

fn golden_aux() -> TreeAux {
    let id = |byte: u8| ObjectId::try_from(&[byte; 20][..]).expect("oid");
    TreeAux {
        tree: id(0xa1),
        entries: vec![
            AuxEntry {
                name: b"README.md".to_vec(),
                mode: 0o100644,
                oid: id(0x01),
                size: Some(1234),
            },
            AuxEntry {
                name: b"empty".to_vec(),
                mode: 0o100644,
                oid: id(0x02),
                size: Some(0),
            },
            AuxEntry {
                name: b"run.sh".to_vec(),
                mode: 0o100755,
                oid: id(0x03),
                size: Some(19),
            },
            AuxEntry {
                name: b"link".to_vec(),
                mode: 0o120000,
                oid: id(0x04),
                size: Some(7),
            },
            AuxEntry {
                name: b"src".to_vec(),
                mode: 0o40000,
                oid: id(0x05),
                size: None,
            },
            AuxEntry {
                name: b"vendor".to_vec(),
                mode: 0o160000,
                oid: id(0x06),
                size: None,
            },
        ],
    }
}

#[test]
fn golden_tree_aux_v1() {
    let fresh = encode_tree_aux(&golden_aux()).to_vec();
    let path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/tree_aux_v1.lta");
    if std::env::var_os("LOAMS_GIT_BLESS").is_some() {
        std::fs::write(&path, &fresh).expect("write golden file");
    }
    let golden = std::fs::read(&path).expect("read the golden file");
    assert_eq!(&golden[..8], TREE_AUX_MAGIC);
    assert_eq!(&golden[golden.len() - 8..], TREE_AUX_END);
    assert_eq!(decode_tree_aux(&golden), Ok(golden_aux()));
    assert_eq!(
        fresh, golden,
        "encoding reproduces the golden file byte for byte"
    );

    // A flipped byte anywhere, a truncation, or another version is refused.
    for at in [
        0,
        11,
        29,
        golden.len() / 2,
        golden.len() - 10,
        golden.len() - 1,
    ] {
        let mut bad = golden.clone();
        bad[at] ^= 0x01;
        assert!(
            matches!(decode_tree_aux(&bad), Err(FormatError::Corrupt { .. })),
            "byte {at}"
        );
    }
    assert!(matches!(
        decode_tree_aux(&golden[..golden.len() - 1]),
        Err(FormatError::Corrupt { .. })
    ));
    let mut v2 = golden;
    v2[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_tree_aux(&v2),
        Err(FormatError::UnknownVersion { found: 2 })
    );
}

#[tokio::test]
async fn derived_deleted_is_recomputed() {
    let Some(git) = GitRepo::init("derived_deleted_is_recomputed") else {
        return;
    };
    fixture(&git);
    let (_, store) = faulty_store();
    let (odb, derived, paths) = open(&git, 42, store.clone()).await;
    let root = git.rev("main^{tree}");
    let path = paths.derived_tree_aux(&root);

    let first = tree_aux(&odb, &derived, &root).await.expect("first");
    store.delete(&path).await.expect("delete");
    let inflates = odb.stats().inflates;
    let second = tree_aux(&odb, &derived, &root).await.expect("second");
    assert_eq!(first, second);
    assert!(odb.stats().inflates > inflates, "recomputed");
    assert!(store.head(&path).await.is_ok(), "stored again");

    // A corrupt stored object is recomputed too (and left for the scrub).
    store.delete(&path).await.expect("delete");
    store
        .put(&path, bytes::Bytes::from_static(b"LGITTAUX garbage"))
        .await
        .expect("put garbage");
    let third = tree_aux(&odb, &derived, &root).await.expect("third");
    assert_eq!(third, first);
}

fn golden_blame() -> FileBlame {
    let id = |byte: u8| ObjectId::try_from(&[byte; 20][..]).expect("oid");
    FileBlame {
        commit: id(0xc1),
        path: b"src/lib.rs".to_vec(),
        blob: id(0xb1),
        lines: 5,
        hunks: vec![
            BlameHunk {
                start: 0,
                source_start: 0,
                len: 2,
                commit: id(0x01),
                source_path: Some(b"lib.rs".to_vec()),
                previous: None,
            },
            BlameHunk {
                start: 2,
                source_start: 1,
                len: 3,
                commit: id(0xc1),
                source_path: None,
                previous: Some((id(0x01), b"src/lib.rs".to_vec())),
            },
        ],
    }
}

#[test]
fn golden_blame_v1() {
    let fresh = encode_blame(&golden_blame()).to_vec();
    let path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/blame_v1.lbl");
    if std::env::var_os("LOAMS_GIT_BLESS").is_some() {
        std::fs::write(&path, &fresh).expect("write golden file");
    }
    let golden = std::fs::read(&path).expect("read the golden file");
    assert_eq!(&golden[..8], BLAME_MAGIC);
    assert_eq!(&golden[golden.len() - 8..], BLAME_END);
    assert_eq!(decode_blame(&golden), Ok(golden_blame()));
    assert_eq!(
        fresh, golden,
        "encoding reproduces the golden file byte for byte"
    );
    for at in [
        0,
        11,
        29,
        golden.len() / 2,
        golden.len() - 10,
        golden.len() - 1,
    ] {
        let mut bad = golden.clone();
        bad[at] ^= 0x01;
        assert!(
            matches!(decode_blame(&bad), Err(FormatError::Corrupt { .. })),
            "byte {at}"
        );
    }
    let mut v2 = golden.clone();
    v2[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_blame(&v2),
        Err(FormatError::UnknownVersion { found: 2 })
    );
    // Hunks that leave a line out are refused.
    let mut gap = golden_blame();
    gap.hunks[1].start = 3;
    assert!(matches!(
        decode_blame(&encode_blame(&gap)),
        Err(FormatError::Corrupt { .. })
    ));
}
