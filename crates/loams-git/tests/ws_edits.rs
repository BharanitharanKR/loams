//! Mount-less editing (plan GT1 Task 26): the line diff and `merge3`
//! against git, and the workspace edits, diffs and rebases built on them.

mod support;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use loams_git::diff::{DiffSide, write_file_patch};
use loams_git::format::pb;
use loams_git::history::{Pathspec, TreeReader};
use loams_git::limits::MAX_EDITS;
use loams_git::odb::RangeOdb;
use loams_git::ws::{
    EditError, FileEdit, FileMode, MergeLabels, NoEnv, apply_edits, changed_paths, file_patch,
    merge3, read_file, rebase_trees,
};
use loams_git::{ObjectId, RefSnapshot, RepoPath, Seq, StoreBlobStore};
use loams_store::Store;

use support::{GitRepo, put_pack, repo_paths};

/// A small seeded generator (SplitMix64), so cases replay by seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// Lines that repeat in real files (so changes can slide), or a fresh one.
fn line(rng: &mut Rng, fresh: &mut usize) -> String {
    const COMMON: &[&str] = &["", "}", "{", "    return;", "x", "fn f() {", "// --"];
    if rng.chance(30) {
        format!("{}\n", COMMON[rng.below(COMMON.len())])
    } else {
        *fresh += 1;
        format!("line {fresh}\n")
    }
}

fn base_file(rng: &mut Rng, fresh: &mut usize) -> Vec<String> {
    let len = rng.below(40);
    (0..len).map(|_| line(rng, fresh)).collect()
}

/// `base` with a few insertions, deletions and replacements.
fn edit(rng: &mut Rng, base: &[String], fresh: &mut usize) -> Vec<String> {
    let mut out = base.to_vec();
    for _ in 0..rng.below(5) {
        let at = rng.below(out.len() + 1);
        match rng.below(3) {
            0 => {
                for _ in 0..1 + rng.below(3) {
                    let new = line(rng, fresh);
                    out.insert(at.min(out.len()), new);
                }
            }
            1 if !out.is_empty() => {
                let end = (at + 1 + rng.below(3)).min(out.len());
                out.drain(at.min(out.len())..end);
            }
            _ if at < out.len() => {
                let new = line(rng, fresh);
                out[at] = new;
            }
            _ => {}
        }
    }
    out
}

fn text(lines: &[String], rng: &mut Rng) -> Vec<u8> {
    let mut out: Vec<u8> = lines.concat().into_bytes();
    if out.last() == Some(&b'\n') && rng.chance(10) {
        out.pop();
    }
    out
}

fn git_in(dir: &Path, args: &[&str]) -> (Vec<u8>, i32) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .stderr(Stdio::null())
        .output()
        .expect("git runs");
    (out.stdout, out.status.code().expect("an exit code"))
}

fn cases() -> u64 {
    std::env::var("LOAMS_GIT_DIFF_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

/// Lines added and removed in a patch's hunks.
fn counts(patch: &[u8]) -> (usize, usize) {
    let mut out = (0, 0);
    let mut in_hunks = false;
    for line in patch.split(|b| *b == b'\n') {
        if line.starts_with(b"@@") {
            in_hunks = true;
        } else if in_hunks && line.starts_with(b"+") {
            out.0 += 1;
        } else if in_hunks && line.starts_with(b"-") {
            out.1 += 1;
        }
    }
    out
}

/// The hunks of `git diff --no-index --no-indent-heuristic` (from the
/// first `@@`) equal ours on seeded edits. Where git's search breaks a tie
/// between equally short edit scripts differently (7 seeds in 10,000 when
/// last measured; ruling T26.2), both scripts must be equally short, and
/// such seeds stay rare.
#[test]
fn myers_hunks_match_git_diff() {
    let Some(git) = GitRepo::init("myers_hunks_match_git_diff") else {
        return;
    };
    let mut ties = Vec::new();
    let total = cases();
    for seed in 0..total {
        let mut rng = Rng(seed);
        let mut fresh = 0;
        let base = base_file(&mut rng, &mut fresh);
        let new = edit(&mut rng, &base, &mut fresh);
        let (a, b) = (text(&base, &mut rng), text(&new, &mut rng));
        std::fs::write(git.path().join("a.txt"), &a).expect("write");
        std::fs::write(git.path().join("b.txt"), &b).expect("write");
        let (by_git, _) = git_in(
            git.path(),
            &[
                "diff",
                "--no-index",
                "--no-indent-heuristic",
                "a.txt",
                "b.txt",
            ],
        );
        let mut ours = Vec::new();
        write_file_patch(
            b"x",
            Some(DiffSide {
                mode: 0o100644,
                oid: blob_id(&a),
                data: &a,
            }),
            Some(DiffSide {
                mode: 0o100644,
                oid: blob_id(&b),
                data: &b,
            }),
            3,
            &mut ours,
        );
        let hunks = |out: &[u8]| -> Vec<u8> {
            match out.windows(3).position(|w| w == b"\n@@") {
                Some(p) => out[p + 1..].to_vec(),
                None => Vec::new(),
            }
        };
        let (theirs, ours) = (hunks(&by_git), hunks(&ours));
        if theirs != ours {
            assert_eq!(counts(&theirs), counts(&ours), "seed {seed}: not a tie");
            ties.push(seed);
        }
    }
    assert!(
        (ties.len() as u64) * 500 <= total,
        "more than 0.2% of seeds break ties unlike git: {ties:?}"
    );
}

/// `merge3` equals `git merge-file -p --diff3` on output and exit code.
#[test]
fn merge3_matches_git_merge_file() {
    let Some(git) = GitRepo::init("merge3_matches_git_merge_file") else {
        return;
    };
    let labels = MergeLabels {
        ours: "ours",
        base: "base",
        theirs: "theirs",
    };
    let mut failures = Vec::new();
    let (mut clean, mut conflicted) = (0, 0);
    for seed in 0..cases() {
        let mut rng = Rng(seed ^ 0x5eed);
        let mut fresh = 0;
        let base = base_file(&mut rng, &mut fresh);
        let ours = edit(&mut rng, &base, &mut fresh);
        let theirs = edit(&mut rng, &base, &mut fresh);
        let (b, o, t) = (
            text(&base, &mut rng),
            text(&ours, &mut rng),
            text(&theirs, &mut rng),
        );
        std::fs::write(git.path().join("base"), &b).expect("write");
        std::fs::write(git.path().join("ours"), &o).expect("write");
        std::fs::write(git.path().join("theirs"), &t).expect("write");
        let (by_git, code) = git_in(
            git.path(),
            &["merge-file", "-p", "--diff3", "ours", "base", "theirs"],
        );
        let merged = merge3(&b, &o, &t, labels);
        if merged.content != by_git || merged.conflicts as i32 != code {
            failures.push(seed);
        }
        if code == 0 {
            clean += 1;
        } else {
            conflicted += 1;
        }
    }
    assert!(
        clean > 100 && conflicted > 100,
        "{clean} clean, {conflicted} conflicting"
    );
    assert!(
        failures.is_empty(),
        "seeds differing from git: {failures:?}"
    );
}

fn blob_id(data: &[u8]) -> ObjectId {
    gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, data).expect("hash")
}

/// The objects `revs` reach, as a repository's object database.
async fn repo_odb(store: &Store, git: &GitRepo, revs: &[&str]) -> RangeOdb {
    let (repo, paths) = repo_paths(26);
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

fn sh(git: &GitRepo, script: &str) {
    let status = Command::new("sh")
        .args(["-c", script])
        .current_dir(git.path())
        .status()
        .expect("sh runs");
    assert!(status.success(), "{script}");
}

/// Every path's patch between two commits, as `git diff --no-renames
/// --no-indent-heuristic` prints it: modified, added, deleted, empty and
/// binary files, mode changes, symlinks, a file becoming a symlink and a
/// directory, quoted names, long function lines, missing final newlines.
#[tokio::test]
async fn diff_matches_git_diff() {
    let Some(git) = GitRepo::init("diff_matches_git_diff") else {
        return;
    };
    let long = "x".repeat(120);
    let body: String = (0..40).map(|i| format!("    line {i}\n")).collect();
    sh(
        &git,
        &format!(
            "mkdir -p src/deep d && printf 'fn main() {{\n{body}}}\nfn {long}() {{\n{body}}}\n' > src/lib.rs \
             && printf 'a\nb\nc\n' > keep.txt && printf 'gone\n' > gone.txt && printf 'x\n' > mode.sh \
             && printf 'x\n' > mode2.sh && ln -s keep.txt link && printf 'f\n' > typechange \
             && printf 'bin\\000ary' > blob.bin && printf 'd\n' > d/file && printf 'no newline' > nl.txt \
             && printf 'q\n' > \"caf\\303\\251.txt\" && printf 'deep\n' > src/deep/x.txt"
        ),
    );
    git.git(&["add", "-A", "."], None);
    git.git(&["commit", "-q", "-m", "one"], None);
    let changed_body: String = (0..40)
        .map(|i| {
            if i == 5 || i == 33 {
                format!("    changed {i}\n")
            } else {
                format!("    line {i}\n")
            }
        })
        .collect();
    sh(
        &git,
        &format!(
            "printf 'fn main() {{\n{changed_body}}}\nfn {long}() {{\n{changed_body}}}\n' > src/lib.rs \
             && rm gone.txt && printf 'new\n' > added.txt && : > empty.txt && chmod +x mode.sh \
             && chmod +x mode2.sh && printf 'y\n' > mode2.sh && rm link && ln -s gone.txt link \
             && rm typechange && ln -s keep.txt typechange && printf 'bin\\000ary2' > blob.bin \
             && rm -r d && printf 'now a file\n' > d && printf 'no newline either' > nl.txt \
             && printf 'q2\n' > \"caf\\303\\251.txt\" && rm -r src/deep"
        ),
    );
    git.git(&["add", "-A", "."], None);
    git.git(&["commit", "-q", "-m", "two"], None);
    let (by_git, _) = git_in(
        git.path(),
        &[
            "diff",
            "--no-renames",
            "--no-indent-heuristic",
            "HEAD~1",
            "HEAD",
        ],
    );
    let store = Store::in_memory();
    let odb = repo_odb(&store, &git, &["HEAD"]).await;
    let reader = TreeReader::new(&odb);
    let changes = changed_paths(
        &reader,
        git.rev("HEAD~1^{tree}"),
        git.rev("HEAD^{tree}"),
        &Pathspec::all(),
    )
    .await
    .expect("changes");
    let mut ours = Vec::new();
    for change in &changes {
        ours.extend(file_patch(&odb, change).await.expect("patch"));
    }
    assert_eq!(
        String::from_utf8_lossy(&ours),
        String::from_utf8_lossy(&by_git)
    );
}

/// A rebase reads the blobs of the paths both sides changed and no other,
/// and merges them as `git merge-file` does.
#[tokio::test]
async fn rebase_reads_only_changed_paths() {
    let Some(git) = GitRepo::init("rebase_reads_only_changed_paths") else {
        return;
    };
    let lines: String = (0..20).map(|i| format!("line {i}\n")).collect();
    let files: Vec<(String, String)> = (0..100)
        .map(|i| (format!("dir{}/f{i:03}.txt", i % 5), lines.clone()))
        .collect();
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, t)| (p.as_str(), t.as_str()))
        .collect();
    git.commit(&refs, "base");
    git.git(&["tag", "base"], None);
    let edit = |at: usize, with: &str| -> String {
        lines.replace(&format!("line {at}\n"), &format!("{with}\n"))
    };
    // Upstream changes f001, f002 and f003 (its top).
    git.commit(
        &[
            ("dir1/f001.txt", &edit(1, "up")),
            ("dir2/f002.txt", &edit(2, "up")),
            ("dir3/f003.txt", &edit(0, "up top")),
        ],
        "upstream",
    );
    git.git(&["branch", "up"], None);
    // The workspace changes f003 (its bottom), f004 and adds a file.
    git.git(&["checkout", "-q", "base"], None);
    git.commit(
        &[
            ("dir3/f003.txt", &edit(19, "ws bottom")),
            ("dir4/f004.txt", &edit(4, "ws")),
            ("new/file.txt", "new\n"),
        ],
        "workspace",
    );
    let store = Store::in_memory();
    let odb = repo_odb(&store, &git, &["base", "up", "HEAD"]).await;
    let worktree = git.rev("HEAD^{tree}");
    let labels = MergeLabels {
        ours: "workspace",
        base: "base",
        theirs: "upstream",
    };
    let before = odb.stats().blob_reads;
    let rebased = rebase_trees(
        &odb,
        git.rev("base^{tree}"),
        git.rev("up^{tree}"),
        worktree,
        worktree,
        labels,
    )
    .await
    .expect("rebase");
    assert_eq!(
        odb.stats().blob_reads - before,
        3,
        "f003's base, ours and theirs only"
    );
    assert!(rebased.conflicts.is_empty(), "{:?}", rebased.conflicts);
    assert_eq!(rebased.worktree_tree, rebased.index_tree);
    // git agrees: the workspace commit rebased onto upstream.
    git.git(&["rebase", "-q", "up"], None);
    assert_eq!(rebased.worktree_tree, git.rev("HEAD^{tree}"));
    // The same path changed on the same line conflicts, recorded with
    // every side, and the worktree holds the markers.
    git.git(&["checkout", "-q", "base"], None);
    git.commit(&[("dir3/f003.txt", &edit(0, "ws top"))], "clash");
    let odb = repo_odb(&store, &git, &["base", "up", "HEAD"]).await;
    let worktree = git.rev("HEAD^{tree}");
    let rebased = rebase_trees(
        &odb,
        git.rev("base^{tree}"),
        git.rev("up^{tree}"),
        worktree,
        worktree,
        labels,
    )
    .await
    .expect("rebase");
    assert_eq!(rebased.conflicts.len(), 1);
    assert_ne!(
        rebased.index_tree, rebased.worktree_tree,
        "the index keeps the workspace's entry, not the markers"
    );
    let conflict = &rebased.conflicts[0];
    assert_eq!(conflict.path, "dir3/f003.txt");
    assert_eq!(conflict.ours, git.rev("HEAD:dir3/f003.txt").as_bytes());
    assert_eq!(conflict.theirs, git.rev("up:dir3/f003.txt").as_bytes());
    assert_eq!(conflict.base, git.rev("base:dir3/f003.txt").as_bytes());
    let merged = rebased
        .objects
        .iter()
        .find(|(kind, _)| *kind == gix_object::Kind::Blob)
        .expect("the merged blob");
    let text = String::from_utf8_lossy(&merged.1);
    assert!(
        text.starts_with(
            "<<<<<<< workspace\nws top\n||||||| base\nline 0\n=======\nup top\n>>>>>>> upstream\n"
        ),
        "{text}"
    );
}

fn path(p: &str) -> RepoPath {
    RepoPath::parse(p).expect("path")
}

/// Edits apply in order to the worktree (and the index when staged), and
/// read back; env files are refused until they can be sealed.
#[tokio::test]
async fn apply_edits_builds_trees() {
    let Some(git) = GitRepo::init("apply_edits_builds_trees") else {
        return;
    };
    git.commit(
        &[
            ("a.txt", "a\n"),
            ("dir/b.txt", "b\n"),
            ("env/prod.env", "# sealed\nK=ref\n"),
        ],
        "one",
    );
    let tree = git.rev("HEAD^{tree}");
    let store = Store::in_memory();
    let odb = repo_odb(&store, &git, &["HEAD"]).await;
    let snap = pb::WorkspaceSnapshot {
        worktree_tree: tree.as_bytes().to_vec(),
        index_tree: tree.as_bytes().to_vec(),
        ..Default::default()
    };
    let edits = vec![
        FileEdit::Write {
            path: path("new/c.txt"),
            mode: FileMode::Executable,
            data: Bytes::from_static(b"c\n"),
        },
        FileEdit::Rename {
            from: path("a.txt"),
            to: path("dir/a.txt"),
        },
        FileEdit::Delete {
            path: path("dir/b.txt"),
        },
    ];
    let edited = apply_edits(&odb, &snap, edits.clone(), false, &NoEnv)
        .await
        .expect("edits");
    assert_eq!(edited.index_tree, tree, "not staged");
    assert_ne!(edited.worktree_tree, tree);
    let staged = apply_edits(&odb, &snap, edits, true, &NoEnv)
        .await
        .expect("edits");
    assert_eq!(staged.index_tree, staged.worktree_tree);
    // git builds the same tree.
    sh(
        &git,
        "mkdir -p new && printf 'c\\n' > new/c.txt && chmod +x new/c.txt && git mv a.txt dir/a.txt && git rm -q dir/b.txt && git add new/c.txt",
    );
    let (want, _) = git_in(git.path(), &["write-tree"]);
    assert_eq!(
        edited.worktree_tree.to_string(),
        String::from_utf8_lossy(&want).trim()
    );

    // A deleted path, a renamed source, and an env write are refused.
    let refused = |edits: Vec<FileEdit>| apply_edits(&odb, &snap, edits, false, &NoEnv);
    assert!(matches!(
        refused(vec![FileEdit::Delete { path: path("nope") }]).await,
        Err(EditError::Invalid { index: 0, .. })
    ));
    assert!(matches!(
        refused(vec![
            FileEdit::Delete {
                path: path("a.txt")
            },
            FileEdit::Rename {
                from: path("a.txt"),
                to: path("z")
            },
        ])
        .await,
        Err(EditError::Invalid { index: 1, .. })
    ));
    assert!(matches!(
        refused(vec![FileEdit::Write {
            path: path("env/prod.env"),
            mode: FileMode::Regular,
            data: Bytes::from_static(b"K=plaintext\n"),
        }])
        .await,
        Err(EditError::Env(_))
    ));
    // An env file reads back redacted.
    let reader = TreeReader::new(&odb);
    let read = read_file(&reader, tree, &path("env/prod.env"), &NoEnv)
        .await
        .expect("read");
    assert!(read.redacted);
    assert_eq!(read.data.as_deref(), Some(&b"# sealed\nK=<redacted>\n"[..]));
    let read = read_file(&reader, tree, &path("dir"), &NoEnv)
        .await
        .expect("read");
    assert_eq!(read.entry.map(|(mode, _)| mode), Some(0o040000));
    assert!(read.data.is_none());
    let read = read_file(&reader, tree, &path("missing"), &NoEnv)
        .await
        .expect("read");
    assert_eq!(read.entry, None);
}

/// `MAX_EDITS` edits apply; one more is refused.
#[tokio::test]
async fn max_edits_at_and_past_the_limit() {
    let Some(git) = GitRepo::init("max_edits_at_and_past_the_limit") else {
        return;
    };
    git.commit(&[("a.txt", "a\n")], "one");
    let tree = git.rev("HEAD^{tree}");
    let store = Store::in_memory();
    let odb = repo_odb(&store, &git, &["HEAD"]).await;
    let snap = pb::WorkspaceSnapshot {
        worktree_tree: tree.as_bytes().to_vec(),
        index_tree: tree.as_bytes().to_vec(),
        ..Default::default()
    };
    let edits = |n: usize| -> Vec<FileEdit> {
        (0..n)
            .map(|i| FileEdit::Write {
                path: path(&format!("d{}/f{i}", i % 100)),
                mode: FileMode::Regular,
                data: Bytes::from(format!("{i}\n")),
            })
            .collect()
    };
    let edited = apply_edits(&odb, &snap, edits(MAX_EDITS), false, &NoEnv)
        .await
        .expect("at the limit");
    assert_eq!(edited.touched.len(), MAX_EDITS);
    assert!(matches!(
        apply_edits(&odb, &snap, edits(MAX_EDITS + 1), false, &NoEnv).await,
        Err(EditError::TooMany { .. })
    ));
}
