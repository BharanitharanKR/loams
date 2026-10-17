//! Workspace packs and tree building (§36 §19.4–§19.5; plan GT1 Task 24):
//! `TreeBuilder` against `git update-index` plus `git write-tree`, and
//! `PutWorkspacePack`'s checks.

mod support;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use loams_git::blob::Put;
use loams_git::format::pb;
use loams_git::limits::{MAX_PACK_BYTES, MAX_WS_PACK_UPLOADS};
use loams_git::objects::{WsView, grant_writes_workspace};
use loams_git::odb::{ObjectDb, OdbError, RangeOdb, WsOdb};
use loams_git::ws::{
    Edit, FileMode, NoScan, PackScan, ScanRefusal, TreeBuilder, WsPackBlobs, WsPackError,
    WsPackUploads, check_pack_len, fork_sources, put_workspace_pack,
};
use loams_git::{
    BlobId, BlobStore, CreateWs, GitAccess, GitGrant, LpkWriter, MemWorkspaceStore, MountId,
    NamespaceId, ObjectId, Principal, RefName, RefSnapshot, RepoId, RepoMatch, RepoPath, Seq,
    SnapAppended, SnapN, StoreBlobStore, WorkspaceStore, WsBase, WsId, WsMatch,
};
use loams_store::{Op, Store};
use proptest::prelude::*;

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

/// The objects `HEAD` reaches, as a repository's object database.
async fn head_odb(store: &Store, byte: u8, git: &GitRepo) -> RangeOdb {
    let (repo, paths) = repo_paths(byte);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths));
    let pack = put_pack(&blobs, git.pack_revs(&["HEAD"], &[])).await;
    RangeOdb::open(repo, blobs, &snapshot(vec![pack]), None)
        .await
        .expect("open")
}

/// An object database that counts reads.
#[derive(Debug)]
struct Counting<'a> {
    inner: &'a dyn ObjectDb,
    reads: AtomicU64,
}

#[async_trait]
impl ObjectDb for Counting<'_> {
    async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError> {
        self.inner.contains(oid).await
    }

    async fn read(&self, oid: &ObjectId) -> Result<(gix_object::Kind, Bytes), OdbError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(oid).await
    }

    async fn missing_from_closure(
        &self,
        tips: &[ObjectId],
        known: &[ObjectId],
        extra: Option<&dyn ObjectDb>,
    ) -> Result<Vec<ObjectId>, OdbError> {
        self.inner.missing_from_closure(tips, known, extra).await
    }
}

/// Runs git in `dir` with a separate index file; its stdout.
fn git_index(dir: &Path, index: &str, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .env("GIT_INDEX_FILE", dir.join(".git").join(index))
        .env("GIT_LITERAL_PATHSPECS", "1")
        .stderr(Stdio::inherit())
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8(out.stdout).expect("utf-8")
}

/// What `git update-index` and `git write-tree` make of `edits` on an index
/// read from `base`.
fn git_write_tree(git: &GitRepo, base: &ObjectId, edits: &[(RepoPath, Edit)]) -> ObjectId {
    let dir = git.path();
    let index = "oracle-index";
    let _ = std::fs::remove_file(dir.join(".git").join(index));
    git_index(dir, index, &["read-tree", &base.to_string()]);
    for (path, edit) in edits {
        match edit {
            Edit::Write { mode, oid } => {
                let info = format!("{:o},{oid},{path}", mode.bits());
                git_index(
                    dir,
                    index,
                    &["update-index", "--add", "--replace", "--cacheinfo", &info],
                );
            }
            Edit::Remove => {
                git_index(
                    dir,
                    index,
                    &[
                        "rm",
                        "--cached",
                        "-r",
                        "-f",
                        "-q",
                        "--ignore-unmatch",
                        "--",
                        path.as_str(),
                    ],
                );
            }
        }
    }
    oid(git_index(dir, index, &["write-tree"]).trim())
}

/// Every tree `root` reaches, itself included.
fn trees_of(git: &GitRepo, root: &ObjectId) -> BTreeSet<ObjectId> {
    let out = git.git(&["ls-tree", "-r", "-t", &root.to_string()], None);
    let mut trees: BTreeSet<ObjectId> = String::from_utf8(out)
        .expect("utf-8")
        .lines()
        .filter(|line| line.split(' ').nth(1) == Some("tree"))
        .map(|line| oid(&line.split(['\t', ' ']).nth(2).expect("an id")[..40]))
        .collect();
    trees.insert(*root);
    trees
}

/// Applies `edits` with the builder and with git, and checks they agree:
/// the same root, every tree written is git's byte for byte, and every tree
/// git's root has that the base lacks was written.
async fn check_against_git(
    git: &GitRepo,
    odb: &dyn ObjectDb,
    base: ObjectId,
    edits: Vec<(RepoPath, Edit)>,
) -> (ObjectId, Vec<(ObjectId, Bytes)>) {
    let (root, written) = TreeBuilder::apply(base, edits.clone(), odb)
        .await
        .expect("apply");
    let want = git_write_tree(git, &base, &edits);
    assert_eq!(root, want, "edits {edits:?}");
    for (id, body) in &written {
        let (kind, stored) = git.cat(id);
        assert_eq!(kind, "tree");
        assert_eq!(&stored[..], &body[..], "tree {id}");
    }
    let written_ids: HashSet<ObjectId> = written.iter().map(|(id, _)| *id).collect();
    assert_eq!(written_ids.len(), written.len(), "each tree once");
    let base_trees = trees_of(git, &base);
    for tree in trees_of(git, &root) {
        if !base_trees.contains(&tree) && tree != empty_tree() {
            assert!(written_ids.contains(&tree), "tree {tree} not written");
        }
    }
    (root, written)
}

fn empty_tree() -> ObjectId {
    oid("4b825dc642cb6eb9a060e54bf8d69288fbee4904")
}

fn path(s: &str) -> RepoPath {
    RepoPath::parse(s).expect("path")
}

/// Writes `files` (path, content, mode) into the work tree and commits.
fn commit_files(git: &GitRepo, files: &[(String, String, FileMode)]) -> ObjectId {
    for (name, text, mode) in files {
        let file = git.path().join(name);
        std::fs::create_dir_all(file.parent().expect("a parent")).expect("mkdir");
        match mode {
            FileMode::Symlink => std::os::unix::fs::symlink(text, &file).expect("symlink"),
            _ => {
                std::fs::write(&file, text).expect("write");
                if *mode == FileMode::Executable {
                    use std::os::unix::fs::PermissionsExt as _;
                    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755))
                        .expect("chmod");
                }
            }
        }
    }
    git.git(&["add", "-A", "."], None);
    git.git(&["commit", "-q", "--allow-empty", "-m", "base"], None);
    git.rev("HEAD^{tree}")
}

/// Stores `text` as a blob in git's object database; its id.
fn blob(git: &GitRepo, text: &str) -> ObjectId {
    let out = git.git(&["hash-object", "-w", "--stdin"], Some(text.as_bytes()));
    oid(String::from_utf8(out).expect("utf-8").trim())
}

const NAMES: &[&str] = &["a", "b", "a.b", "a-b", "c"];

fn arb_path() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(NAMES), 1..4).prop_map(|parts| parts.join("/"))
}

/// Base files with no path below another file.
fn arb_base() -> impl Strategy<Value = Vec<(String, usize, u8)>> {
    prop::collection::vec((arb_path(), 0usize..4, 0u8..3), 0..12).prop_map(|files| {
        let mut kept: Vec<(String, usize, u8)> = Vec::new();
        for (p, text, mode) in files {
            let clash = kept.iter().any(|(q, _, _)| {
                q == &p || q.starts_with(&format!("{p}/")) || p.starts_with(&format!("{q}/"))
            });
            if !clash {
                kept.push((p, text, mode));
            }
        }
        kept
    })
}

/// Edits: `None` removes, `Some((text, mode))` writes.
fn arb_edits() -> impl Strategy<Value = Vec<(String, Option<(usize, u8)>)>> {
    prop::collection::vec(
        (arb_path(), prop::option::weighted(0.7, (0usize..4, 0u8..4))),
        1..10,
    )
}

fn mode_of(n: u8) -> FileMode {
    match n {
        0 => FileMode::Regular,
        1 => FileMode::Executable,
        2 => FileMode::Symlink,
        _ => FileMode::Gitlink,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// The builder's root and trees are git's for any edit set, over any
    /// base: files replacing directories and the other way round, removals
    /// of absent paths, directories emptied, names that sort around `/`.
    #[test]
    fn tree_builder_matches_git_write_tree(base in arb_base(), edits in arb_edits()) {
        let Some(git) = GitRepo::init("tree_builder_matches_git_write_tree") else {
            return Ok(());
        };
        let texts: Vec<String> = (0..4).map(|i| format!("content {i}\n")).collect();
        let files: Vec<(String, String, FileMode)> = base
            .iter()
            .map(|(p, text, mode)| (p.clone(), texts[*text].clone(), mode_of(*mode)))
            .collect();
        let base_tree = commit_files(&git, &files);
        let blobs: Vec<ObjectId> = texts.iter().map(|text| blob(&git, text)).collect();
        let head = git.rev("HEAD");
        let edits: Vec<(RepoPath, Edit)> = edits
            .into_iter()
            .map(|(p, op)| {
                let edit = match op {
                    None => Edit::Remove,
                    Some((text, mode)) => {
                        let mode = mode_of(mode);
                        let oid = if mode == FileMode::Gitlink { head } else { blobs[text] };
                        Edit::Write { mode, oid }
                    }
                };
                (path(&p), edit)
            })
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async {
            let (_, store) = faulty_store();
            let odb = head_odb(&store, 60, &git).await;
            check_against_git(&git, &odb, base_tree, edits).await;
        });
    }
}

#[tokio::test]
async fn tree_builder_writes_k_times_depth_trees() {
    let Some(git) = GitRepo::init("tree_builder_writes_k_times_depth_trees") else {
        return;
    };
    let files: Vec<(String, String, FileMode)> = (0..10)
        .map(|i| {
            (
                format!("d{i}/s/t/f.txt"),
                format!("{i}\n"),
                FileMode::Regular,
            )
        })
        .collect();
    let base = commit_files(&git, &files);
    let (_, store) = faulty_store();
    let odb = head_odb(&store, 61, &git).await;
    let counting = Counting {
        inner: &odb,
        reads: AtomicU64::new(0),
    };
    // k = 3 files at depth 4 (three directories and the file), each with
    // its own content so no two new trees are equal.
    let edits: Vec<(RepoPath, Edit)> = (0..3)
        .map(|i| {
            (
                path(&format!("d{i}/s/t/f.txt")),
                Edit::Write {
                    mode: FileMode::Regular,
                    oid: blob(&git, &format!("changed {i}\n")),
                },
            )
        })
        .collect();
    let (_, written) = check_against_git(&git, &counting, base, edits).await;
    // The root, then three trees along each path; the other seven
    // directories are neither read nor written.
    assert_eq!(written.len(), 1 + 3 * 3);
    assert_eq!(counting.reads.load(Ordering::Relaxed), 1 + 3 * 3);

    // Writing what is already there writes nothing.
    let same = blob(&git, "0\n");
    let (root, written) = TreeBuilder::apply(
        base,
        [(
            path("d0/s/t/f.txt"),
            Edit::Write {
                mode: FileMode::Regular,
                oid: same,
            },
        )],
        &odb,
    )
    .await
    .expect("apply");
    assert_eq!(root, base);
    assert!(written.is_empty());
}

#[tokio::test]
async fn mode_and_symlink_and_exec_bit_round_trip() {
    let Some(git) = GitRepo::init("mode_and_symlink_and_exec_bit_round_trip") else {
        return;
    };
    let base = commit_files(
        &git,
        &[
            ("README".into(), "read me\n".into(), FileMode::Regular),
            ("run.sh".into(), "#!/bin/sh\n".into(), FileMode::Executable),
            ("link".into(), "README".into(), FileMode::Symlink),
        ],
    );
    let (_, store) = faulty_store();
    let odb = head_odb(&store, 62, &git).await;
    let readme = git.rev("HEAD:README");
    let run = git.rev("HEAD:run.sh");
    let target = blob(&git, "run.sh");
    let head = git.rev("HEAD");
    let edits = vec![
        (
            path("README"),
            Edit::Write {
                mode: FileMode::Executable,
                oid: readme,
            },
        ),
        (
            path("run.sh"),
            Edit::Write {
                mode: FileMode::Regular,
                oid: run,
            },
        ),
        (
            path("link"),
            Edit::Write {
                mode: FileMode::Regular,
                oid: readme,
            },
        ),
        (
            path("bin/tool"),
            Edit::Write {
                mode: FileMode::Symlink,
                oid: target,
            },
        ),
        (
            path("vendor/lib"),
            Edit::Write {
                mode: FileMode::Gitlink,
                oid: head,
            },
        ),
    ];
    let (root, _) = check_against_git(&git, &odb, base, edits).await;
    let listing =
        String::from_utf8(git.git(&["ls-tree", "-r", &root.to_string()], None)).expect("utf-8");
    let modes: BTreeMap<String, String> = listing
        .lines()
        .map(|line| {
            let (meta, name) = line.split_once('\t').expect("a tab");
            (
                name.to_owned(),
                meta.split(' ').next().expect("a mode").to_owned(),
            )
        })
        .collect();
    let want: BTreeMap<String, String> = [
        ("README", "100755"),
        ("bin/tool", "120000"),
        ("link", "100644"),
        ("run.sh", "100644"),
        ("vendor/lib", "160000"),
    ]
    .into_iter()
    .map(|(name, mode)| (name.to_owned(), mode.to_owned()))
    .collect();
    assert_eq!(modes, want);
    for bits in [0o100644, 0o100755, 0o120000, 0o160000] {
        assert_eq!(FileMode::from_bits(bits).expect("a file mode").bits(), bits);
    }
    assert_eq!(FileMode::from_bits(0o040000), None);

    // Removing everything leaves the empty tree, which needs no object.
    let all = ["README", "run.sh", "link"].map(|p| (path(p), Edit::Remove));
    let (empty, written) = TreeBuilder::apply(base, all, &odb).await.expect("apply");
    assert_eq!(empty, empty_tree());
    assert_eq!(written.len(), 1);
    let (again, written) = TreeBuilder::apply(empty_tree(), Vec::new(), &odb)
        .await
        .expect("apply");
    assert_eq!(again, empty_tree());
    assert!(written.is_empty());
    // The root itself is not a file.
    assert!(
        TreeBuilder::apply(base, [(RepoPath::root(), Edit::Remove)], &odb)
            .await
            .is_err()
    );
}

/// A pack of one blob, `result`, stored as a ref delta against `base` (an
/// object of `base_len` bytes the pack does not hold), with its idx: a thin
/// pack, which git cannot index without its base.
fn ref_delta_pack(base: ObjectId, base_len: usize, result: &[u8]) -> (Bytes, Bytes) {
    fn varint(mut n: usize, out: &mut Vec<u8>) {
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    let mut delta = Vec::new();
    varint(base_len, &mut delta);
    varint(result.len(), &mut delta);
    for chunk in result.chunks(127) {
        delta.push(chunk.len() as u8);
        delta.extend_from_slice(chunk);
    }
    let mut pack = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
    // Type 7 (ref delta) and the delta's size.
    let mut size = delta.len();
    let mut byte = (7 << 4) | (size & 0x0f) as u8;
    size >>= 4;
    while size > 0 {
        pack.push(byte | 0x80);
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    pack.push(byte);
    pack.extend_from_slice(base.as_bytes());
    let mut z = gix_zlib::stream::deflate::Write::new(Vec::new(), gix_zlib::Compression::default());
    std::io::Write::write_all(&mut z, &delta).expect("deflate");
    std::io::Write::flush(&mut z).expect("deflate");
    pack.extend_from_slice(&z.into_inner());
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(&pack);
    let pack_sum = hasher.try_finalize().expect("sha1");
    pack.extend_from_slice(pack_sum.as_bytes());

    let name = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, result)
        .expect("hash");
    let mut idx = vec![0xff, b't', b'O', b'c', 0, 0, 0, 2];
    for byte in 0..256usize {
        let n = u32::from(byte >= usize::from(name.as_bytes()[0]));
        idx.extend_from_slice(&n.to_be_bytes());
    }
    idx.extend_from_slice(name.as_bytes());
    idx.extend_from_slice(&[0; 4]);
    idx.extend_from_slice(&12u32.to_be_bytes());
    idx.extend_from_slice(pack_sum.as_bytes());
    idx.extend_from_slice(&[0; 20]);
    reseal_idx(&mut idx);
    (Bytes::from(pack), Bytes::from(idx))
}

/// `pack ‖ idx` from git as a bundle.
fn bundle((pack, idx): (Bytes, Bytes)) -> Bytes {
    LpkWriter::from_parts(pack, idx).expect("bundle").2
}

/// Rewrites an idx's SHA-1 trailer after an edit.
fn reseal_idx(idx: &mut [u8]) {
    let body = idx.len() - 20;
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(&idx[..body]);
    let sum = hasher.try_finalize().expect("sha1");
    idx[body..].copy_from_slice(sum.as_bytes());
}

fn repo() -> RepoId {
    repo_paths(70).0
}

fn ws_blobs(store: &Store, ws: &WsId) -> StoreBlobStore {
    StoreBlobStore::workspace(store.clone(), &repo_paths(70).1, ws)
}

fn ws_id(n: u8) -> WsId {
    WsId::parse(&format!("w01j9z3m5q8x0000000000000{n:02}")).expect("ws id")
}

#[tokio::test]
async fn put_ws_pack_rejects_bad_hash() {
    let Some(git) = GitRepo::init("put_ws_pack_rejects_bad_hash") else {
        return;
    };
    let one = blob(&git, "a workspace file\n");
    let (pack, idx) = git.pack_objects(&[one]);
    let (_, store) = faulty_store();
    let blobs = ws_blobs(&store, &ws_id(1));

    // The idx names another id for the object.
    let mut lying = idx.to_vec();
    lying[8 + 256 * 4 + 19] ^= 1;
    reseal_idx(&mut lying);
    let lpk = bundle((pack.clone(), Bytes::from(lying)));
    let err = put_workspace_pack(&repo(), &blobs, lpk.clone(), &NoScan)
        .await
        .expect_err("a bad hash");
    let WsPackError::BadHash { actual, offset, .. } = err else {
        panic!("not a bad hash: {err}");
    };
    assert_eq!(actual, one);
    assert_eq!(offset, 12);
    let id = BlobId::from_checksum(&loams_git::format::lpk::pack_checksum(&pack).expect("sum"));
    assert_eq!(blobs.head(&id).await.expect("head"), None, "nothing stored");

    // A corrupt pack trailer, a corrupt idx trailer and a bad footer CRC.
    let mut bad_pack = pack.to_vec();
    let at = bad_pack.len() - 30;
    bad_pack[at] ^= 0xff;
    let mut bad_idx = idx.to_vec();
    let at = bad_idx.len() - 1;
    bad_idx[at] ^= 1;
    let mut bad_crc = bundle((pack.clone(), idx.clone())).to_vec();
    let at = bad_crc.len() - 8;
    bad_crc[at] ^= 1;
    for (what, lpk) in [
        ("pack trailer", bundle((Bytes::from(bad_pack), idx.clone()))),
        ("idx trailer", bundle((pack.clone(), Bytes::from(bad_idx)))),
        ("footer", Bytes::from(bad_crc)),
        ("short", Bytes::from_static(b"LGITLPK")),
    ] {
        let err = put_workspace_pack(&repo(), &blobs, lpk, &NoScan)
            .await
            .expect_err(what);
        assert!(matches!(err, WsPackError::Invalid(_)), "{what}: {err}");
    }

    // A thin pack (a delta whose base is outside it) is refused, even
    // where the base is a repository object; the same object stored whole
    // is accepted.
    let base_text = b"a workspace file\n";
    let result = b"a workspace file, edited\n";
    let err = put_workspace_pack(
        &repo(),
        &blobs,
        bundle(ref_delta_pack(one, base_text.len(), result)),
        &NoScan,
    )
    .await
    .expect_err("a thin pack");
    assert!(
        matches!(&err, WsPackError::Invalid(reason) if reason.contains(&format!("delta base {one} not found"))),
        "{err}"
    );
    let edited = blob(&git, std::str::from_utf8(result).expect("utf-8"));
    put_workspace_pack(
        &repo(),
        &blobs,
        bundle(git.pack_objects(&[edited])),
        &NoScan,
    )
    .await
    .expect("a self-contained pack");

    // `MAX_WS_PACK_UPLOADS` permits at once, then none until one ends.
    let uploads = WsPackUploads::default();
    let held: Vec<_> = (0..MAX_WS_PACK_UPLOADS)
        .map(|_| uploads.try_acquire().expect("a permit"))
        .collect();
    assert!(uploads.try_acquire().is_none(), "one past the limit");
    drop(held);
    assert!(uploads.try_acquire().is_some());

    // `MAX_PACK_BYTES`, at the limit and one past it.
    assert!(check_pack_len(MAX_PACK_BYTES).is_ok());
    assert!(matches!(
        check_pack_len(MAX_PACK_BYTES + 1),
        Err(WsPackError::TooLarge { .. })
    ));
}

/// Refuses blobs that contain `SECRET`.
#[derive(Debug)]
struct RefuseSecret;

impl PackScan for RefuseSecret {
    fn object(
        &self,
        oid: &ObjectId,
        kind: gix_object::Kind,
        data: &[u8],
    ) -> Result<(), ScanRefusal> {
        if kind == gix_object::Kind::Blob && data.windows(6).any(|w| w == b"SECRET") {
            return Err(ScanRefusal {
                oid: *oid,
                rule: "test-secret".into(),
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn put_ws_pack_is_create_only() {
    let Some(git) = GitRepo::init("put_ws_pack_is_create_only") else {
        return;
    };
    let tip = git.commit(&[("a.txt", "a\n"), ("d/b.txt", "b\n")], "one");
    let (faulty, store) = faulty_store();
    let ws = ws_id(2);
    let blobs = ws_blobs(&store, &ws);
    let lpk = bundle(git.pack_revs(&[&tip.to_string()], &["--delta-base-offset"]));

    let (verified, put) = put_workspace_pack(&repo(), &blobs, lpk.clone(), &NoScan)
        .await
        .expect("put");
    assert_eq!(put, Put::Created);
    assert_eq!(
        verified.objects.len(),
        git.objects(&[&tip.to_string()]).len()
    );
    let path = format!(
        "{}packs/{}.lpk",
        repo_paths(70).1.ws_prefix(&ws),
        verified.pack.checksum
    );
    assert_eq!(store.get(&path).await.expect("stored").0, lpk);

    // Again: the same pack, nothing overwritten.
    let puts = faulty.calls(Op::Put);
    let (again, put) = put_workspace_pack(&repo(), &blobs, lpk.clone(), &NoScan)
        .await
        .expect("put again");
    assert_eq!(put, Put::Existed);
    assert_eq!(again.pack, verified.pack);
    assert_eq!(faulty.calls(Op::Put) - puts, 1, "one create-only attempt");
    assert_eq!(store.get(&path).await.expect("stored").0, lpk);

    // Another object already under a pack's name is never replaced.
    let other = git.commit(&[("c.txt", "c\n")], "two");
    let (pack, idx) = git.pack_revs(&[&other.to_string(), &format!("^{tip}")], &[]);
    let (_, other_ref, other_lpk) = LpkWriter::from_parts(pack, idx).expect("bundle");
    let squatter = format!(
        "{}packs/{}.lpk",
        repo_paths(70).1.ws_prefix(&ws),
        other_ref.checksum
    );
    store
        .put_if_absent(&squatter, Bytes::from_static(b"not this pack"))
        .await
        .expect("squat");
    let err = put_workspace_pack(&repo(), &blobs, other_lpk, &NoScan)
        .await
        .expect_err("a mismatch");
    assert!(matches!(err, WsPackError::Blob(_)), "{err}");
    assert_eq!(
        store.get(&squatter).await.expect("kept").0,
        Bytes::from_static(b"not this pack")
    );

    // The scanner's hook sees every object and can refuse the pack.
    let secret = git.commit(&[("e.txt", "token SECRET\n")], "three");
    let lpk = bundle(git.pack_revs(&[&secret.to_string(), &format!("^{other}")], &[]));
    let err = put_workspace_pack(&repo(), &blobs, lpk, &RefuseSecret)
        .await
        .expect_err("refused");
    let WsPackError::Refused(refusal) = err else {
        panic!("not refused: {err}");
    };
    assert_eq!(refusal.oid, git.rev(&format!("{secret}:e.txt")));
}

#[test]
fn put_ws_pack_needs_ws_write() {
    let ws = ws_id(3);
    let view = WsView {
        owner: Principal::parse("alice").expect("principal"),
        packs: Vec::new(),
        sources: Vec::new(),
    };
    let grant = |principal: &str, access: GitAccess, workspaces: WsMatch| GitGrant {
        org: "local".into(),
        namespace: NamespaceId(7),
        principal: Principal::parse(principal).expect("principal"),
        repos: RepoMatch::All,
        access,
        branch_prefixes: Vec::new(),
        cones: Vec::new(),
        workspaces,
        env: Vec::new(),
        cache_class: None,
        registry_read: false,
    };
    let cases = [
        ("alice", GitAccess::Write, WsMatch::Own, true),
        ("alice", GitAccess::Admin, WsMatch::Own, true),
        ("alice", GitAccess::Read, WsMatch::Own, false),
        (
            "alice",
            GitAccess::Write,
            WsMatch::Ids(vec![ws.clone()]),
            true,
        ),
        (
            "alice",
            GitAccess::Write,
            WsMatch::Ids(vec![ws_id(4)]),
            false,
        ),
        ("bob", GitAccess::Write, WsMatch::Own, false),
        (
            "bob",
            GitAccess::Write,
            WsMatch::Ids(vec![ws.clone()]),
            false,
        ),
        ("bob", GitAccess::Read, WsMatch::All, false),
        ("bob", GitAccess::Write, WsMatch::All, true),
    ];
    for (principal, access, workspaces, want) in cases {
        let g = grant(principal, access, workspaces.clone());
        assert_eq!(
            grant_writes_workspace(&g, &ws, &view),
            want,
            "{principal} {access:?} {workspaces:?}"
        );
    }
}

fn ws_spec(ws: WsId, base: Option<WsBase>) -> CreateWs {
    CreateWs {
        repo: repo(),
        ws_id: ws,
        owner: Principal::parse("alice").expect("principal"),
        name: None,
        principal: Principal::parse("alice").expect("principal"),
        writer: None,
        base,
    }
}

#[tokio::test]
async fn forked_workspace_reads_its_sources_packs() {
    let Some(git) = GitRepo::init("forked_workspace_reads_its_sources_packs") else {
        return;
    };
    let base_commit = git.commit(&[("a.txt", "base\n")], "base");
    let edit = git.commit(&[("w.txt", "made in the source\n")], "edit");
    let only_ws = git.rev("HEAD:w.txt");
    let (_, store) = faulty_store();
    let (repo_id, paths) = repo_paths(70);
    let repo_blobs = Arc::new(StoreBlobStore::new(store.clone(), paths.clone()));
    let pack = put_pack(&repo_blobs, git.pack_revs(&[&base_commit.to_string()], &[])).await;
    let repo_odb = RangeOdb::open(repo_id.clone(), repo_blobs, &snapshot(vec![pack]), None)
        .await
        .expect("open");

    // Workspace A uploads a pack and names it; B forks A, C forks B.
    let workspaces = MemWorkspaceStore::new();
    let (a, b, c) = (ws_id(10), ws_id(11), ws_id(12));
    let zero = workspaces
        .create(ws_spec(
            a.clone(),
            Some(WsBase {
                track: RefName::parse("refs/heads/main").expect("ref"),
                commit: Some(base_commit),
                tree: git.rev(&format!("{base_commit}^{{tree}}")),
                seq: Seq(1),
                head_ref: None,
            }),
        ))
        .await
        .expect("create");
    let range = format!("{base_commit}..{edit}");
    let (verified, _) = put_workspace_pack(
        &repo_id,
        &ws_blobs(&store, &a),
        bundle(git.pack_revs(&[&range], &[])),
        &NoScan,
    )
    .await
    .expect("put");
    let one = pb::WorkspaceSnapshot {
        n: 1,
        writer: MountId::generate().to_string(),
        packs: vec![verified.pack.clone()],
        ..(*zero).clone()
    };
    assert_eq!(
        workspaces.append(&one).await.expect("append"),
        SnapAppended::Committed { n: SnapN(1) }
    );
    workspaces
        .fork((&repo_id, &a, SnapN(1)), ws_spec(b.clone(), None))
        .await
        .expect("fork b");
    let forked = workspaces
        .fork((&repo_id, &b, SnapN(0)), ws_spec(c.clone(), None))
        .await
        .expect("fork c");
    assert_eq!(forked.packs, vec![verified.pack.clone()]);

    assert!(
        fork_sources(&workspaces, &repo_id, &a)
            .await
            .expect("a")
            .is_empty()
    );
    assert_eq!(
        fork_sources(&workspaces, &repo_id, &b).await.expect("b"),
        vec![a.clone()]
    );
    let sources = fork_sources(&workspaces, &repo_id, &c).await.expect("c");
    assert_eq!(sources, vec![b.clone(), a.clone()]);

    // C reads A's pack through the chain; without it the pack is missing.
    let chain = Arc::new(WsPackBlobs::new(&store, &paths, &c, &sources, None));
    let ws_odb = WsOdb::open(&repo_odb, chain, &forked.packs).expect("ws odb");
    let (kind, data) = ws_odb.read(&only_ws).await.expect("read");
    assert_eq!(kind, gix_object::Kind::Blob);
    assert_eq!(&data[..], b"made in the source\n");
    let alone = Arc::new(WsPackBlobs::new(&store, &paths, &c, &[], None));
    let ws_odb = WsOdb::open(&repo_odb, alone, &forked.packs).expect("ws odb");
    assert!(ws_odb.read(&only_ws).await.is_err());
}
