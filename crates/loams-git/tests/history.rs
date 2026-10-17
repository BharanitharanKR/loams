//! History on the server: the commit graph, `Log` and `DiffStat` (§36
//! §19.8, D833; plan GT1 Task 20), checked against git itself.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use loams_git::derived::DerivedError;
use loams_git::derived::graph::{GraphView, Layer, graph_view};
use loams_git::history::{
    BlameAnswer, BlameQuery, ChangeKind, GrepFile, GrepFlags, GrepQuery, GrepStats, HistoryError,
    LogEntry, LogFields, LogQuery, LogStats, Pathspec, blame, diff_stat, grep, log, resolve_tree,
};
use loams_git::limits::{GREP_MAX_MATCHES, MAX_GREP_PATTERNS};
use loams_git::odb::{ObjectDb, RangeOdb};
use loams_git::{
    DerivedStore, ObjectId, RefName, RefSnapshot, RepoPath, RepoPaths, Seq, StoreBlobStore,
};
use loams_store::{Fault, Op, Store};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;

use support::{GitRepo, faulty_store, oid, put_pack, repo_paths};

// --- Seeded histories, written with `git fast-import` -------------------

type Files = BTreeMap<String, (u32, Vec<u8>)>;

const PATHS: &[&str] = &[
    "README",
    "a.txt",
    "src/lib.rs",
    "src/main.rs",
    "src/deep/x.rs",
    "docs/guide.md",
    "docs/img.bin",
    "tools/run.sh",
    "link",
];
const WORDS: &[&str] = &[
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
];
const SPECS: &[&str] = &[
    "README",
    "a.txt",
    "src",
    "src/deep",
    "src/deep/x.rs",
    "docs",
    "tools/run.sh",
    "link",
    "conf",
    "missing",
];

struct History {
    rng: StdRng,
    stream: Vec<u8>,
    commits: Vec<Files>,
    heads: Vec<usize>,
    time: i64,
    tags: usize,
    /// Each path's lines come from its own vocabulary, and files are
    /// sometimes renamed (the blame corpus, ruling T21.1).
    vocab: bool,
    renames: usize,
}

impl History {
    fn content(&mut self, path: &str) -> (u32, Vec<u8>) {
        match path {
            "link" => (
                0o120000,
                format!("target-{}", self.rng.random_range(0..3)).into_bytes(),
            ),
            "docs/img.bin" if self.rng.random_bool(0.5) => {
                (0o100644, vec![0, 1, 2, self.rng.random_range(0..4)])
            }
            _ => {
                let mode = if path == "tools/run.sh" && self.rng.random_bool(0.5) {
                    0o100755
                } else {
                    0o100644
                };
                (mode, self.text(path))
            }
        }
    }

    /// A word for a line of `path`.
    fn word(&mut self, path: &str) -> String {
        let word = WORDS[self.rng.random_range(0..WORDS.len())];
        if self.vocab {
            let tag: u32 = path.bytes().map(u32::from).sum();
            format!("{word}-{tag}")
        } else {
            word.to_owned()
        }
    }

    fn text(&mut self, path: &str) -> Vec<u8> {
        let lines = self.rng.random_range(0..12);
        let mut out = String::new();
        for _ in 0..lines {
            let word = self.word(path);
            out.push_str(&word);
            out.push('\n');
        }
        if self.rng.random_bool(0.1) {
            out.push_str("no newline");
        }
        out.into_bytes()
    }

    /// Edits a few lines of `old`, so diffs have context.
    fn edit(&mut self, path: &str, old: &[u8]) -> Vec<u8> {
        let text = String::from_utf8_lossy(old).to_string();
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        for _ in 0..self.rng.random_range(1..4) {
            let at = self.rng.random_range(0..=lines.len());
            match self.rng.random_range(0..3) {
                0 => {
                    let word = self.word(path);
                    lines.insert(at, word);
                }
                1 if at < lines.len() => {
                    lines.remove(at);
                }
                _ if at < lines.len() => {
                    lines[at] = self.word(path);
                }
                _ => lines.push("tail".into()),
            }
        }
        let mut out = lines.join("\n");
        if !out.is_empty() && self.rng.random_bool(0.9) {
            out.push('\n');
        }
        out.into_bytes()
    }

    fn mutate(&mut self, files: &mut Files) {
        for _ in 0..self.rng.random_range(1..4) {
            if self.rng.random_bool(0.1) {
                // A file becomes a directory, or back.
                if files.remove("conf").is_some() {
                    let content = self.text("conf/a");
                    files.insert("conf/a".into(), (0o100644, content));
                } else {
                    files.retain(|path, _| !path.starts_with("conf/"));
                    let content = self.text("conf");
                    files.insert("conf".into(), (0o100644, content));
                }
                continue;
            }
            if self.vocab && self.rng.random_bool(0.08) {
                // A text file is renamed.
                let movable: Vec<String> = files
                    .iter()
                    .filter(|(path, (mode, content))| {
                        *mode == 0o100644 && !path.starts_with("conf") && !content.contains(&0)
                    })
                    .map(|(path, _)| path.clone())
                    .collect();
                if !movable.is_empty() {
                    let from = movable[self.rng.random_range(0..movable.len())].clone();
                    // Unedited: git's and gix's rename scores differ for
                    // edited files near the threshold (ruling T21.1).
                    let (mode, content) = files.remove(&from).expect("listed");
                    self.renames += 1;
                    files.insert(format!("moved/{}.txt", self.renames), (mode, content));
                }
                continue;
            }
            let path = PATHS[self.rng.random_range(0..PATHS.len())];
            match files.get(path).cloned() {
                Some(_) if self.rng.random_bool(0.2) => {
                    files.remove(path);
                }
                Some((mode, old)) if mode == 0o100644 && self.rng.random_bool(0.7) => {
                    let new = self.edit(path, &old);
                    files.insert(path.into(), (mode, new));
                }
                _ => {
                    let content = self.content(path);
                    files.insert(path.into(), content);
                }
            }
        }
    }

    fn signature(&mut self, who: &str) -> String {
        let tz = ["+0000", "+0530", "-0700"][self.rng.random_range(0..3)];
        let name = ["A U Thor", "Bea", "C"][self.rng.random_range(0..3)];
        format!(
            "{who} {name} <{}@example.com> {} {tz}\n",
            name.len(),
            self.time
        )
    }

    fn data(&mut self, bytes: &[u8]) {
        self.stream
            .extend_from_slice(format!("data {}\n", bytes.len()).as_bytes());
        self.stream.extend_from_slice(bytes);
        self.stream.push(b'\n');
    }

    /// Commit number `self.commits.len()` with `parents` and `files`.
    fn commit(&mut self, branch: usize, parents: &[usize], files: Files) -> usize {
        let mark = self.commits.len();
        // Mostly later; sometimes the same second.
        self.time += [0, 0, 1, 7, 60, 3600][self.rng.random_range(0..6)];
        if parents.is_empty() {
            self.stream
                .extend_from_slice(format!("reset refs/heads/b{branch}\n").as_bytes());
        }
        self.stream.extend_from_slice(
            format!("commit refs/heads/b{branch}\nmark :{}\n", mark + 1).as_bytes(),
        );
        let author = self.signature("author");
        let committer = self.signature("committer");
        self.stream.extend_from_slice(author.as_bytes());
        self.stream.extend_from_slice(committer.as_bytes());
        let message = if self.rng.random_bool(0.3) {
            format!("commit {mark}\n\nbody of {mark}\n\nlast line\n")
        } else {
            format!("commit {mark}\n")
        };
        self.data(message.as_bytes());
        for (i, parent) in parents.iter().enumerate() {
            let verb = if i == 0 { "from" } else { "merge" };
            self.stream
                .extend_from_slice(format!("{verb} :{}\n", parent + 1).as_bytes());
        }
        self.stream.extend_from_slice(b"deleteall\n");
        for (path, (mode, content)) in &files {
            let line = format!("M {mode:o} inline {path}\n");
            self.stream.extend_from_slice(line.as_bytes());
            let content = content.clone();
            self.data(&content);
        }
        self.stream.push(b'\n');
        self.commits.push(files);
        mark
    }

    /// A history of up to `steps` operations from `seed`.
    /// A history of up to `steps` operations from `seed`; with `vocab`,
    /// per-path vocabularies and renames.
    fn generate_with(seed: u64, steps: usize, vocab: bool) -> Self {
        let mut h = Self {
            rng: StdRng::seed_from_u64(seed),
            stream: Vec::new(),
            commits: Vec::new(),
            heads: Vec::new(),
            time: 1_760_000_000,
            tags: 0,
            vocab,
            renames: 0,
        };
        let mut files = Files::new();
        h.mutate(&mut files);
        let root = h.commit(0, &[], files);
        h.heads.push(root);
        for _ in 0..steps {
            let roll = h.rng.random_range(0..100);
            let branch = h.rng.random_range(0..h.heads.len());
            let head = h.heads[branch];
            if roll < 50 {
                let mut files = h.commits[head].clone();
                h.mutate(&mut files);
                h.heads[branch] = h.commit(branch, &[head], files);
            } else if roll < 62 && h.heads.len() < 5 {
                let from = h.rng.random_range(0..h.commits.len());
                h.heads.push(from);
            } else if roll < 88 && h.heads.len() > 1 {
                let other = h.rng.random_range(0..h.heads.len());
                let theirs = h.heads[other];
                if theirs == head {
                    continue;
                }
                let mut parents = vec![head, theirs];
                if h.rng.random_bool(0.1) {
                    let third = h.heads[h.rng.random_range(0..h.heads.len())];
                    if !parents.contains(&third) {
                        parents.push(third);
                    }
                }
                let files = match h.rng.random_range(0..5) {
                    0 => h.commits[head].clone(),
                    1 => h.commits[theirs].clone(),
                    choice => {
                        let mut files = h.commits[head].clone();
                        for (path, value) in h.commits[theirs].clone() {
                            if h.rng.random_bool(0.6) {
                                if path.starts_with("conf/") {
                                    files.remove("conf");
                                } else if path == "conf" {
                                    files.retain(|p, _| !p.starts_with("conf/"));
                                }
                                files.insert(path, value);
                            }
                        }
                        if choice == 4 {
                            h.mutate(&mut files);
                        }
                        files
                    }
                };
                h.heads[branch] = h.commit(branch, &parents, files);
            } else if roll < 93 {
                let files = h.commits[head].clone();
                h.heads[branch] = h.commit(branch, &[head], files);
            } else if roll < 96 && h.heads.len() < 5 {
                let mut files = Files::new();
                h.mutate(&mut files);
                let branch = h.heads.len();
                let root = h.commit(branch, &[], files);
                h.heads.push(root);
            } else {
                let at = h.rng.random_range(0..h.commits.len());
                let tag = h.tags;
                h.tags += 1;
                if h.rng.random_bool(0.5) {
                    h.stream.extend_from_slice(
                        format!("reset refs/tags/v{tag}\nfrom :{}\n\n", at + 1).as_bytes(),
                    );
                } else {
                    h.stream.extend_from_slice(
                        format!(
                            "tag v{tag}\nfrom :{}\ntagger T <t@example.com> {} +0000\n",
                            at + 1,
                            h.time
                        )
                        .as_bytes(),
                    );
                    h.data(format!("tag {tag}\n").as_bytes());
                }
            }
        }
        for (branch, head) in h.heads.clone().into_iter().enumerate() {
            let name = if branch == 0 {
                "main".to_owned()
            } else {
                format!("b{branch}")
            };
            h.stream.extend_from_slice(
                format!("reset refs/heads/{name}\nfrom :{}\n\n", head + 1).as_bytes(),
            );
        }
        h.stream.extend_from_slice(
            b"reset refs/heads/b0\nfrom 0000000000000000000000000000000000000000\n\n",
        );
        h
    }

    fn branches(&self) -> Vec<String> {
        (0..self.heads.len())
            .map(|b| {
                if b == 0 {
                    "main".into()
                } else {
                    format!("b{b}")
                }
            })
            .collect()
    }
}

/// A repository holding seeded history `seed`.
fn seeded(test: &str, seed: u64, steps: usize) -> Option<(GitRepo, History)> {
    seeded_with(test, seed, steps, false)
}

/// As [`seeded`], with per-path vocabularies and renames when `vocab`.
fn seeded_with(test: &str, seed: u64, steps: usize, vocab: bool) -> Option<(GitRepo, History)> {
    let git = GitRepo::init(test)?;
    let history = History::generate_with(seed, steps, vocab);
    git.git(&["fast-import", "--quiet"], Some(&history.stream));
    git.git(&["symbolic-ref", "HEAD", "refs/heads/main"], None);
    Some((git, history))
}

/// Every ref of `git` with its object.
fn refs(git: &GitRepo) -> BTreeMap<String, ObjectId> {
    let out =
        String::from_utf8(git.git(&["for-each-ref", "--format=%(objectname) %(refname)"], None))
            .expect("utf-8");
    out.lines()
        .map(|line| {
            let (id, name) = line.split_once(' ').expect("two columns");
            (name.to_owned(), oid(id))
        })
        .collect()
}

fn snapshot_of(git: &GitRepo, seq: u64, packs: Vec<loams_git::format::pb::PackRef>) -> RefSnapshot {
    RefSnapshot {
        seq: Seq(seq),
        refs: refs(git)
            .into_iter()
            .map(|(name, id)| (RefName::parse(&name).expect("ref name"), id))
            .collect(),
        symrefs: [(
            "HEAD".to_owned(),
            RefName::parse("refs/heads/main").expect("ref"),
        )]
        .into(),
        protections: Vec::new(),
        packs,
        parent: None,
    }
}

/// Stores `git`'s refs' objects as one pack in repository `byte` of
/// `store`; the database, the derived store and the snapshot at `seq`.
async fn load(
    git: &GitRepo,
    byte: u8,
    store: Store,
    seq: u64,
) -> (RangeOdb, DerivedStore, RefSnapshot) {
    let (repo, paths) = repo_paths(byte);
    let blobs = Arc::new(StoreBlobStore::new(store.clone(), paths.clone()));
    let names: Vec<String> = refs(git).into_keys().collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let pack = put_pack(&blobs, git.pack_revs(&names, &["--delta-base-offset"])).await;
    let snapshot = snapshot_of(git, seq, vec![pack]);
    let odb = RangeOdb::open(repo, blobs, &snapshot, None)
        .await
        .expect("open");
    (odb, DerivedStore::new(store, paths), snapshot)
}

async fn run_log(
    odb: &RangeOdb,
    derived: &DerivedStore,
    snapshot: &RefSnapshot,
    query: &LogQuery,
) -> (Vec<LogEntry>, LogStats) {
    let (tx, mut rx) = mpsc::channel(16);
    let collect = async {
        let mut out = Vec::new();
        while let Some(entry) = rx.recv().await {
            out.push(entry);
        }
        out
    };
    let (stats, entries) = tokio::join!(
        async {
            let stats = log(odb, derived, snapshot, query, &tx).await;
            drop(tx);
            stats
        },
        collect
    );
    (entries, stats.expect("log"))
}

/// `entries` as `git log --format=raw` prints them.
fn raw(entries: &[LogEntry]) -> String {
    let mut out = String::new();
    for (i, entry) in entries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        writeln!(out, "commit {}", entry.oid).expect("write");
        writeln!(out, "tree {}", entry.tree).expect("write");
        for parent in &entry.parents {
            writeln!(out, "parent {parent}").expect("write");
        }
        for (who, sig) in [("author", &entry.author), ("committer", &entry.committer)] {
            let sig = sig.as_ref().expect("signatures");
            writeln!(
                out,
                "{who} {} <{}> {} {}",
                String::from_utf8_lossy(&sig.name),
                String::from_utf8_lossy(&sig.email),
                sig.time,
                sig.tz
            )
            .expect("write");
        }
        out.push('\n');
        let message = String::from_utf8_lossy(entry.message.as_ref().expect("message")).to_string();
        let mut lines: Vec<&str> = message.split('\n').collect();
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
        for line in lines {
            writeln!(out, "    {line}").expect("write");
        }
    }
    out
}

fn git_log(git: &GitRepo, args: &[&str]) -> String {
    let mut full = vec!["log", "--format=raw"];
    full.extend_from_slice(args);
    String::from_utf8(git.git(&full, None)).expect("utf-8")
}

/// Compares `Log(revs, spec)` with `git log --format=raw revs -- spec`.
async fn check_log(
    git: &GitRepo,
    (odb, derived, snapshot): &(RangeOdb, DerivedStore, RefSnapshot),
    revs: &[&str],
    spec: &[&str],
    limit: Option<usize>,
    what: &str,
) {
    let query = LogQuery {
        revs: revs.iter().map(|r| (*r).to_owned()).collect(),
        pathspec: Pathspec::parse(&spec.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
            .expect("pathspec"),
        limit,
        fields: LogFields::default(),
    };
    let (entries, _) = run_log(odb, derived, snapshot, &query).await;
    let mut args: Vec<String> = Vec::new();
    if let Some(n) = limit {
        args.push(format!("-n{n}"));
    }
    args.extend(revs.iter().map(|r| (*r).to_owned()));
    args.push("--".into());
    args.extend(spec.iter().map(|s| (*s).to_owned()));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let expected = git_log(git, &args);
    let got = raw(&entries);
    assert!(
        got.trim_end() == expected.trim_end(),
        "{what}: log {revs:?} -- {spec:?} differs\n--- ours\n{got}\n--- git\n{expected}"
    );
}

const SEEDS: u64 = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_matches_git_log_format_raw() {
    for seed in 0..SEEDS {
        let Some((git, history)) = seeded("log_matches_git_log_format_raw", seed, 24) else {
            return;
        };
        let (_, store) = faulty_store();
        let loaded = load(&git, 60, store, 1).await;
        let what = format!("seed {seed}");
        check_log(&git, &loaded, &[], &[], None, &what).await;
        let branches = history.branches();
        let all: Vec<&str> = branches.iter().map(String::as_str).collect();
        check_log(&git, &loaded, &all, &[], None, &what).await;
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed);
        for _ in 0..3 {
            let spec = SPECS[rng.random_range(0..SPECS.len())];
            check_log(&git, &loaded, &[], &[spec], None, &what).await;
        }
        let two = [
            SPECS[rng.random_range(0..SPECS.len())],
            SPECS[rng.random_range(0..SPECS.len())],
        ];
        check_log(&git, &loaded, &all, &two, None, &what).await;
        check_log(&git, &loaded, &[], &[], Some(3), &what).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_ranges_and_merges_match_git() {
    let mut merges = 0;
    for seed in 1000..1000 + SEEDS {
        let Some((git, history)) = seeded("log_ranges_and_merges_match_git", seed, 30) else {
            return;
        };
        let (_, store) = faulty_store();
        let loaded = load(&git, 61, store, 1).await;
        let what = format!("seed {seed}");
        merges += history.stream.windows(6).filter(|w| w == b"merge ").count();
        let branches = history.branches();
        let mut rng = StdRng::seed_from_u64(seed ^ 0xa11);
        for _ in 0..4 {
            let a = &branches[rng.random_range(0..branches.len())];
            let b = &branches[rng.random_range(0..branches.len())];
            let range = format!("{a}..{b}");
            check_log(&git, &loaded, &[&range], &[], None, &what).await;
            let exclude = format!("^{a}");
            let spec = SPECS[rng.random_range(0..SPECS.len())];
            check_log(&git, &loaded, &[&exclude, b, "main"], &[spec], None, &what).await;
            check_log(&git, &loaded, &[&range], &[spec], Some(2), &what).await;
        }
        let tag = refs(&git)
            .into_keys()
            .find(|name| name.starts_with("refs/tags/"));
        if let Some(tag) = tag {
            check_log(&git, &loaded, &[&tag], &[], None, &what).await;
        }
    }
    assert!(merges > 200, "the histories have merges ({merges})");
}

#[tokio::test]
async fn log_pathspec_uses_bloom_then_diff() {
    let Some(git) = GitRepo::init("log_pathspec_uses_bloom_then_diff") else {
        return;
    };
    // 60 commits, each touching one of 20 files three levels deep.
    let mut stream = Vec::new();
    for i in 0..60u32 {
        let from = if i == 0 {
            String::new()
        } else {
            format!("from :{i}\n")
        };
        let body = format!("v{i}\n");
        let message = format!("c{i}");
        let time = 1_760_000_000 + i;
        stream.extend_from_slice(
            format!(
                "commit refs/heads/main\nmark :{mark}\nauthor A <a@x> {time} +0000\n\
                 committer A <a@x> {time} +0000\ndata {}\n{message}\n{from}\
                 M 100644 inline d{}/e/f{}.txt\ndata {}\n{body}\n",
                message.len(),
                i % 10,
                (i / 10) % 2,
                body.len(),
                mark = i + 1,
            )
            .as_bytes(),
        );
    }
    git.git(&["fast-import", "--quiet"], Some(&stream));
    git.git(&["symbolic-ref", "HEAD", "refs/heads/main"], None);
    let (_, store) = faulty_store();
    let loaded = load(&git, 62, store, 1).await;
    let spec = ["d3/e/f1.txt"];
    // The first call builds the graph; the second reads it.
    check_log(&git, &loaded, &[], &spec, None, "bloom").await;
    let query = LogQuery {
        pathspec: Pathspec::parse(&["d3/e/f1.txt".to_owned()]).expect("spec"),
        ..LogQuery::default()
    };
    let (odb, derived, snapshot) = &loaded;
    let (entries, stats) = run_log(odb, derived, snapshot, &query).await;
    assert_eq!(entries.len(), 3, "commits 13, 33 and 53 touch it");
    assert!(!stats.built_layer, "the stored layer is used");
    assert!(
        stats.bloom_definitely_not >= 50,
        "most commits are skipped by their filter: {stats:?}"
    );
    // Each "maybe" compares two paths of depth 3: at most 6 tree reads, and
    // the root commit reads its own path.
    assert!(
        stats.tree_reads <= 6 * (stats.bloom_maybe + 1),
        "trees are read only where a filter says maybe: {stats:?}"
    );
    assert!(stats.tree_reads < 60, "{stats:?}");
}

/// The chunks of a commit-graph file, by id.
fn chunks(bytes: &[u8]) -> BTreeMap<[u8; 4], Vec<u8>> {
    let count = usize::from(bytes[6]);
    let mut out = BTreeMap::new();
    for i in 0..count {
        let at = 8 + i * 12;
        let id: [u8; 4] = bytes[at..at + 4].try_into().expect("write");
        let start = u64::from_be_bytes(bytes[at + 4..at + 12].try_into().expect("bytes")) as usize;
        let end = u64::from_be_bytes(bytes[at + 16..at + 24].try_into().expect("bytes")) as usize;
        out.insert(id, bytes[start..end].to_vec());
    }
    out
}

#[tokio::test]
async fn graph_layer_matches_git_commit_graph_write() {
    for seed in 0..20 {
        let Some((git, _)) = seeded("graph_layer_matches_git_commit_graph_write", seed, 40) else {
            return;
        };
        let (_, store) = faulty_store();
        let (odb, derived, snapshot) = load(&git, 63, store.clone(), 5).await;
        let view = graph_view(&odb, &derived, &snapshot).await.expect("graph");
        assert!(view.built_layer());
        let (ours, _) = store
            .get(&derived.paths().derived_graph(Seq(5)))
            .await
            .expect("stored layer");
        git.git(
            &[
                "-c",
                "commitGraph.changedPathsVersion=2",
                "commit-graph",
                "write",
                "--reachable",
                "--changed-paths",
                "--no-progress",
            ],
            None,
        );
        let theirs = std::fs::read(git.path().join(".git/objects/info/commit-graph"))
            .expect("git's commit graph");
        let (ours, theirs) = (chunks(&ours), chunks(&theirs));
        for id in [b"OIDF", b"OIDL", b"CDAT", b"EDGE", b"BIDX", b"BDAT"] {
            assert_eq!(
                ours.get(id),
                theirs.get(id),
                "seed {seed}: chunk {}",
                String::from_utf8_lossy(id)
            );
        }
    }
}

#[tokio::test]
async fn graph_filters_match_git_at_the_changed_path_limit() {
    let Some(git) = GitRepo::init("graph_filters_match_git_at_the_changed_path_limit") else {
        return;
    };
    // Commits changing 300 files in 300 directories (600 paths), 511 files
    // in one directory (512 paths) and 512 files in one (513 paths).
    let mut stream = Vec::new();
    let mut mark = 0;
    for (files, dirs) in [(300, 300), (511, 1), (512, 1)] {
        mark += 1;
        let from = if mark == 1 {
            String::new()
        } else {
            format!("from :{}\n", mark - 1)
        };
        let mut commit = format!(
            "commit refs/heads/main\nmark :{mark}\nauthor A <a@x> {t} +0000\n\
             committer A <a@x> {t} +0000\ndata 1\nc\n{from}",
            t = 1_760_000_000 + mark
        );
        for i in 0..files {
            commit.push_str(&format!(
                "M 100644 inline d{}/f{i}-{mark}.txt\ndata 2\n{mark}\n\n",
                i % dirs
            ));
        }
        commit.push('\n');
        stream.extend_from_slice(commit.as_bytes());
    }
    git.git(&["fast-import", "--quiet"], Some(&stream));
    git.git(&["symbolic-ref", "HEAD", "refs/heads/main"], None);
    let (_, store) = faulty_store();
    let (odb, derived, snapshot) = load(&git, 68, store.clone(), 1).await;
    graph_view(&odb, &derived, &snapshot).await.expect("graph");
    let (ours, _) = store
        .get(&derived.paths().derived_graph(Seq(1)))
        .await
        .expect("stored layer");
    git.git(
        &[
            "-c",
            "commitGraph.changedPathsVersion=2",
            "commit-graph",
            "write",
            "--reachable",
            "--changed-paths",
            "--no-progress",
        ],
        None,
    );
    let theirs = std::fs::read(git.path().join(".git/objects/info/commit-graph")).expect("graph");
    let (ours, theirs) = (chunks(&ours), chunks(&theirs));
    for id in [b"BIDX", b"BDAT"] {
        assert_eq!(
            ours.get(id),
            theirs.get(id),
            "chunk {}",
            String::from_utf8_lossy(id)
        );
    }
}

/// Installs `view`'s layers in `git` as a commit-graph chain.
async fn install_chain(git: &GitRepo, store: &Store, paths: &RepoPaths, view: &GraphView) {
    let dir = git.path().join(".git/objects/info/commit-graphs");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut chain = String::new();
    for source in view.sources() {
        let (bytes, _) = store
            .get(&paths.derived_graph(source.seq))
            .await
            .expect("layer");
        let layer = Layer::parse(bytes.clone()).expect("parse");
        let hex = layer.checksum().to_string();
        std::fs::write(dir.join(format!("graph-{hex}.graph")), &bytes).expect("write");
        chain.push_str(&hex);
        chain.push('\n');
    }
    std::fs::write(dir.join("commit-graph-chain"), chain).expect("chain");
}

#[tokio::test]
async fn graph_layer_built_once_per_seq() {
    let Some((git, _)) = seeded("graph_layer_built_once_per_seq", 7, 30) else {
        return;
    };
    let (faulty, store) = faulty_store();
    let (odb_a, derived_a, snapshot) = load(&git, 64, store.clone(), 3).await;
    let (odb_b, derived_b, _) = load(&git, 64, store.clone(), 3).await;
    let puts = faulty.calls(Op::PutCreate);
    let (a, b) = tokio::join!(
        graph_view(&odb_a, &derived_a, &snapshot),
        graph_view(&odb_b, &derived_b, &snapshot)
    );
    let (a, b) = (a.expect("a"), b.expect("b"));
    assert!(
        faulty.calls(Op::PutCreate) - puts <= 2,
        "one PUT each, one wins"
    );
    assert_eq!(a.sources(), b.sources());
    let layers = store
        .list(&derived_a.paths().derived_graph_prefix())
        .await
        .expect("list");
    assert_eq!(layers.len(), 1, "one layer for seq 3");
    let all = git.objects(&["--all"]).len();
    assert!(a.stored_commits() > 0 && (a.stored_commits() as usize) < all);

    // A later open of the same seq reads it and writes nothing.
    let puts = faulty.calls(Op::PutCreate);
    let again = graph_view(&odb_a, &derived_a, &snapshot)
        .await
        .expect("again");
    assert!(!again.built_layer());
    assert_eq!(faulty.calls(Op::PutCreate), puts);

    // New commits at later seqs add layers, merged geometrically, and git
    // reads the chain.
    let mut lengths = Vec::new();
    for (i, seq) in (4..20u64).enumerate() {
        let out = git.git(
            &[
                "commit-tree",
                "-p",
                "main",
                "-m",
                &format!("more {i}"),
                "main^{tree}",
            ],
            None,
        );
        let next = String::from_utf8(out).expect("utf-8");
        git.git(&["update-ref", "refs/heads/main", next.trim()], None);
        let (odb, derived, snapshot) = load(&git, 64, store.clone(), seq).await;
        let view = graph_view(&odb, &derived, &snapshot).await.expect("view");
        assert!(view.built_layer(), "seq {seq} has new commits");
        assert!(view.contains(&git.rev("main")));
        lengths.push(view.sources().len());
        if seq == 19 {
            let (_, paths) = repo_paths(64);
            install_chain(&git, &store, &paths, &view).await;
            git.git(&["commit-graph", "verify", "--no-progress"], None);
            let with = git.git(&["log", "--format=%H %P", "--all"], None);
            let without = git.git(
                &[
                    "-c",
                    "core.commitGraph=false",
                    "log",
                    "--format=%H %P",
                    "--all",
                ],
                None,
            );
            assert_eq!(with, without, "git reads the chain");
        }
    }
    assert!(
        lengths.iter().all(|len| *len <= 6),
        "chains stay short: {lengths:?}"
    );
}

#[tokio::test]
async fn bloom_filters_are_used_by_git() {
    let Some((git, _)) = seeded("bloom_filters_are_used_by_git", 11, 40) else {
        return;
    };
    let (_, store) = faulty_store();
    let (odb, derived, snapshot) = load(&git, 65, store.clone(), 2).await;
    let view = graph_view(&odb, &derived, &snapshot).await.expect("view");
    let (_, paths) = repo_paths(65);
    install_chain(&git, &store, &paths, &view).await;
    for spec in SPECS {
        let args = ["log", "--format=%H", "main", "--", spec];
        let with = git.git(
            &[&["-c", "commitGraph.changedPathsVersion=2"][..], &args[..]].concat(),
            None,
        );
        let without = git.git(
            &[&["-c", "core.commitGraph=false"][..], &args[..]].concat(),
            None,
        );
        assert_eq!(with, without, "git log -- {spec} with our filters");
    }
}

#[tokio::test]
async fn log_names_changed_paths() {
    let Some((git, _)) = seeded("log_names_changed_paths", 21, 30) else {
        return;
    };
    let (_, store) = faulty_store();
    let (odb, derived, snapshot) = load(&git, 66, store, 1).await;
    let query = LogQuery {
        fields: LogFields {
            changed: true,
            ..LogFields::default()
        },
        ..LogQuery::default()
    };
    let (entries, _) = run_log(&odb, &derived, &snapshot, &query).await;
    let mut ours = String::new();
    for entry in entries.iter().filter(|entry| entry.parents.len() < 2) {
        writeln!(ours, "{}", entry.oid).expect("write");
        for change in entry.changed.as_ref().expect("changed") {
            let letter = match change.kind {
                ChangeKind::Added => 'A',
                ChangeKind::Deleted => 'D',
                ChangeKind::Modified => 'M',
                ChangeKind::TypeChanged => 'T',
            };
            writeln!(ours, "{letter}\t{}", String::from_utf8_lossy(&change.path)).expect("write");
        }
    }
    let theirs = String::from_utf8(git.git(
        &[
            "log",
            "--no-merges",
            "--no-renames",
            "--format=%H",
            "--name-status",
        ],
        None,
    ))
    .expect("utf-8");
    let theirs: String = theirs
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| format!("{l}\n"))
        .collect();
    assert_eq!(ours, theirs);
}

#[tokio::test]
async fn diffstat_matches_git_numstat() {
    let mut files = 0;
    for seed in 0..60 {
        let Some((git, history)) = seeded("diffstat_matches_git_numstat", seed, 20) else {
            return;
        };
        let (_, store) = faulty_store();
        let (odb, _, snapshot) = load(&git, 67, store, 1).await;
        let mut rng = StdRng::seed_from_u64(seed);
        let branches = history.branches();
        for _ in 0..4 {
            let a = branches[rng.random_range(0..branches.len())].clone();
            let b = branches[rng.random_range(0..branches.len())].clone();
            let spec: Vec<String> = if rng.random_bool(0.4) {
                vec![SPECS[rng.random_range(0..SPECS.len())].to_owned()]
            } else {
                Vec::new()
            };
            let old = resolve_tree(&odb, &snapshot, &a).await.expect("a");
            let new = resolve_tree(&odb, &snapshot, &b).await.expect("b");
            let stats = diff_stat(
                &odb,
                Some(old),
                Some(new),
                &Pathspec::parse(&spec).expect("spec"),
            )
            .await
            .expect("diffstat");
            files += stats.len();
            let mut ours = Vec::new();
            for stat in &stats {
                let count = |n: Option<u64>| n.map_or("-".to_owned(), |n| n.to_string());
                ours.extend_from_slice(
                    format!("{}\t{}\t", count(stat.added), count(stat.removed)).as_bytes(),
                );
                ours.extend_from_slice(&stat.change.path);
                ours.push(0);
            }
            let mut args = vec!["diff", "--numstat", "--no-renames", "-z", &a, &b, "--"];
            args.extend(spec.iter().map(String::as_str));
            let theirs = git.git(&args, None);
            assert_eq!(
                String::from_utf8_lossy(&ours),
                String::from_utf8_lossy(&theirs),
                "seed {seed}: diff {a} {b} -- {spec:?}"
            );
        }
    }
    assert!(files > 100, "the pairs differ ({files} files)");
}

#[test]
fn pathspec_refuses_globs() {
    let spec: BTreeSet<&str> = ["*.rs", "src/*", ":(glob)x"].into();
    for item in spec {
        assert!(Pathspec::parse(&[item.to_owned()]).is_err(), "{item}");
    }
}

// --- Blame (Task 21) ----------------------------------------------------

/// `answer` as `git blame --porcelain` prints it, with `content` the
/// blamed file.
fn porcelain(answer: &BlameAnswer, path: &str, content: &[u8]) -> String {
    let lines: Vec<&[u8]> = content.split_inclusive(|b| *b == b'\n').collect();
    let mut out = String::new();
    let mut shown: BTreeSet<ObjectId> = BTreeSet::new();
    for hunk in &answer.hunks {
        let commit = answer
            .commits
            .iter()
            .find(|c| c.oid == hunk.commit)
            .expect("the hunk's commit");
        for i in 0..hunk.len {
            let (source, final_line) = (hunk.source_start + i + 1, hunk.start + i + 1);
            if i == 0 {
                writeln!(out, "{} {source} {final_line} {}", hunk.commit, hunk.len).expect("write");
                if shown.insert(hunk.commit) {
                    for (who, sig) in [("author", &commit.author), ("committer", &commit.committer)]
                    {
                        writeln!(out, "{who} {}", String::from_utf8_lossy(&sig.name)).expect("w");
                        writeln!(out, "{who}-mail <{}>", String::from_utf8_lossy(&sig.email))
                            .expect("w");
                        writeln!(out, "{who}-time {}", sig.time).expect("w");
                        writeln!(out, "{who}-tz {}", sig.tz).expect("w");
                    }
                    writeln!(out, "summary {}", String::from_utf8_lossy(&commit.summary))
                        .expect("w");
                    if commit.boundary {
                        out.push_str("boundary\n");
                    }
                    if let Some((prev, prev_path)) = &hunk.previous {
                        writeln!(
                            out,
                            "previous {prev} {}",
                            String::from_utf8_lossy(prev_path)
                        )
                        .expect("w");
                    }
                    let name = hunk
                        .source_path
                        .as_deref()
                        .map(|p| String::from_utf8_lossy(p).to_string())
                        .unwrap_or_else(|| path.to_owned());
                    writeln!(out, "filename {name}").expect("w");
                }
            } else {
                writeln!(out, "{} {source} {final_line}", hunk.commit).expect("write");
            }
            let line = lines[(hunk.start + i) as usize];
            out.push('\t');
            out.push_str(&String::from_utf8_lossy(
                line.strip_suffix(b"\n").unwrap_or(line),
            ));
            out.push('\n');
        }
    }
    out
}

async fn run_blame(
    (odb, derived, snapshot): &(RangeOdb, DerivedStore, RefSnapshot),
    rev: &str,
    path: &str,
    lines: Option<std::ops::RangeInclusive<u32>>,
) -> (BlameAnswer, String) {
    let query = BlameQuery {
        rev: rev.to_owned(),
        path: RepoPath::parse(path).expect("path"),
        lines,
    };
    let answer = blame(odb, derived, snapshot, &query).await.expect("blame");
    let (_, content) = odb.read(&answer.blob).await.expect("blob");
    let text = porcelain(&answer, path, &content);
    (answer, text)
}

/// The text files of `files` (blame is asked of text, as people do).
fn text_files(files: &Files) -> Vec<String> {
    files
        .iter()
        .filter(|(_, (mode, content))| *mode != 0o120000 && !content.contains(&0))
        .map(|(path, _)| path.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blame_porcelain_matches_git() {
    let mut checked = 0;
    let mut ranges = 0;
    let (mut commits, mut merges, mut previous, mut unsupported) = (0, 0, 0, 0);
    let mut seed = 0;
    while checked < 50 {
        let Some((git, history)) = seeded_with("blame_porcelain_matches_git", seed, 150, true)
        else {
            return;
        };
        let (_, store) = faulty_store();
        let loaded = load(&git, 68, store, 1).await;
        let mut rng = StdRng::seed_from_u64(seed ^ 0xb1a3e);
        let branches = history.branches();
        let branch = branches[rng.random_range(0..branches.len())].clone();
        let tip = history.heads[branches.iter().position(|b| *b == branch).expect("b")];
        for path in text_files(&history.commits[tip]) {
            let query = BlameQuery {
                rev: branch.clone(),
                path: RepoPath::parse(&path).expect("path"),
                lines: None,
            };
            if let Err(err) = blame(&loaded.0, &loaded.1, &loaded.2, &query).await {
                // A file `gix-blame` cannot walk is refused, never answered
                // wrongly (ruling T21.1).
                assert!(
                    matches!(
                        err,
                        HistoryError::Derived(DerivedError::BlameUnsupported(_))
                    ),
                    "seed {seed}: blame {branch} -- {path}: {err:?}"
                );
                unsupported += 1;
                continue;
            }
            let (answer, ours) = run_blame(&loaded, &branch, &path, None).await;
            let theirs = git.git(&["blame", "--porcelain", &branch, "--", &path], None);
            assert_eq!(
                ours,
                String::from_utf8_lossy(&theirs),
                "seed {seed}: blame {branch} -- {path}"
            );
            checked += 1;
            commits += answer.commits.len();
            previous += answer.hunks.iter().filter(|h| h.previous.is_some()).count();
            merges += answer
                .hunks
                .iter()
                .filter(|h| {
                    git.git(
                        &["rev-list", "--parents", "-n1", &h.commit.to_string()],
                        None,
                    )
                    .split(|b| *b == b' ')
                    .count()
                        > 2
                })
                .count();
            if answer.lines >= 2 && rng.random_bool(0.5) {
                let a = rng.random_range(1..=answer.lines);
                let b = rng.random_range(a..=answer.lines + 2);
                let (_, ours) = run_blame(&loaded, &branch, &path, Some(a..=b)).await;
                let range = format!("{a},{b}");
                let theirs = git.git(
                    &["blame", "--porcelain", "-L", &range, &branch, "--", &path],
                    None,
                );
                assert_eq!(
                    ours,
                    String::from_utf8_lossy(&theirs),
                    "seed {seed}: blame -L {range} {branch} -- {path}"
                );
                ranges += 1;
            }
        }
        seed += 1;
    }
    eprintln!(
        "blame: {checked} files over {seed} histories, {ranges} ranges, {commits} commits, {merges} merge-blamed, {previous} previous, {unsupported} unsupported"
    );
    assert!(ranges > 5, "line ranges were checked ({ranges})");
    assert!(merges > 0, "lines are blamed on merges ({merges})");
    assert!(
        unsupported * 5 < checked,
        "most files are served ({unsupported} refused of {checked})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blame_is_cached() {
    let Some((git, history)) = seeded("blame_is_cached", 7, 30) else {
        return;
    };
    let (faulty, store) = faulty_store();
    let loaded = load(&git, 69, store, 1).await;
    let path = text_files(&history.commits[history.heads[0]])
        .into_iter()
        .next()
        .expect("a text file");
    let (first, text) = run_blame(&loaded, "main", &path, None).await;
    assert!(!first.stats.cached);
    let commit = git.rev("main");
    let key = loaded
        .1
        .paths()
        .derived_blame(&commit, &RepoPath::parse(&path).expect("path"));
    assert!(
        loaded.1.get(&key).await.expect("get").is_some(),
        "stored at {key}"
    );
    let puts = faulty.calls(Op::PutCreate);
    let (second, again) = run_blame(&loaded, "main", &path, None).await;
    assert!(second.stats.cached, "the second blame reads the stored one");
    assert_eq!(second.stats.commits_traversed, 0);
    assert_eq!(faulty.calls(Op::PutCreate), puts, "nothing is stored again");
    assert_eq!(text, again);
    // A corrupt stored blame is recomputed, and the answer is the same.
    let (_, store2) = faulty_store();
    let loaded2 = load(&git, 69, store2.clone(), 1).await;
    store2
        .put(&key, bytes::Bytes::from_static(b"LGITBLAMgarbage"))
        .await
        .expect("put");
    let (third, text3) = run_blame(&loaded2, "main", &path, None).await;
    assert!(!third.stats.cached);
    assert_eq!(text, text3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blame_refuses_what_is_not_a_file() {
    let Some((git, _)) = seeded("blame_refuses_what_is_not_a_file", 3, 10) else {
        return;
    };
    let (_, store) = faulty_store();
    let (odb, derived, snapshot) = load(&git, 70, store, 1).await;
    for path in ["missing/file", "src"] {
        let query = BlameQuery {
            rev: "main".into(),
            path: RepoPath::parse(path).expect("path"),
            lines: None,
        };
        let err = blame(&odb, &derived, &snapshot, &query).await;
        assert!(
            matches!(err, Err(HistoryError::Path { .. })),
            "{path}: {err:?}"
        );
    }
}

// --- Grep (Task 21) -----------------------------------------------------

async fn run_grep(
    odb: &RangeOdb,
    snapshot: &RefSnapshot,
    query: &GrepQuery,
) -> (Vec<GrepFile>, GrepStats) {
    let (tx, mut rx) = mpsc::channel(16);
    let collect = async {
        let mut out = Vec::new();
        while let Some(file) = rx.recv().await {
            out.push(file);
        }
        out
    };
    let (stats, files) = tokio::join!(
        async {
            let stats = grep(odb, snapshot, query, &tx).await;
            drop(tx);
            stats
        },
        collect
    );
    (files, stats.expect("grep"))
}

/// `files` as `git grep -n` (or `-l`) prints them for `rev`.
fn grep_output(rev: &str, files: &[GrepFile], files_only: bool) -> String {
    let mut out = String::new();
    for file in files {
        let path = String::from_utf8_lossy(&file.path);
        if files_only {
            writeln!(out, "{rev}:{path}").expect("write");
        } else if file.binary {
            writeln!(out, "Binary file {rev}:{path} matches").expect("write");
        } else {
            for line in &file.lines {
                writeln!(
                    out,
                    "{rev}:{path}:{}:{}",
                    line.number,
                    String::from_utf8_lossy(&line.line)
                )
                .expect("write");
            }
        }
    }
    out
}

const BASIC: &[&str] = &[
    "alph",
    "^beta",
    "a$",
    "e.a",
    "t\\(h\\|o\\)",
    "z*eta",
    "[ab]eta",
    "[[:upper:]]",
    "x\\{0,1\\}eta",
    "a+",
    "ta\\>",
    "\\<eta",
    "new",
    "[^a-z]",
    "o\\?n",
    "target-1",
    "\\.",
];
const EXTENDED: &[&str] = &[
    "(alpha|beta)$",
    "e+t",
    "^(z|th)eta",
    "p{2}",
    "[^a-z]",
    "ta\\b",
    "^.{4}$",
    "o?ne",
    "a|tail",
];
const FIXED: &[&str] = &["eta", "a.b", "*", "no newline", "ALPHA", "[x"];
const WORD: &[&str] = &["eta", "alpha", "new", "a", "tail"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grep_matches_git_grep() {
    let mut matched = 0;
    for seed in 0..40 {
        let Some((git, history)) = seeded("grep_matches_git_grep", seed, 20) else {
            return;
        };
        let (_, store) = faulty_store();
        let (odb, _, snapshot) = load(&git, 71, store, 1).await;
        let mut rng = StdRng::seed_from_u64(seed ^ 0x9e3);
        let branches = history.branches();
        for _ in 0..8 {
            let rev = branches[rng.random_range(0..branches.len())].clone();
            let mut flags = GrepFlags {
                ignore_case: rng.random_bool(0.3),
                word: rng.random_bool(0.25),
                files_only: rng.random_bool(0.25),
                ..GrepFlags::default()
            };
            let kind = rng.random_range(0..3);
            let pool = match kind {
                0 => BASIC,
                1 => {
                    flags.extended = true;
                    EXTENDED
                }
                _ => {
                    flags.fixed = true;
                    FIXED
                }
            };
            let pool = if flags.word { WORD } else { pool };
            let mut patterns = vec![pool[rng.random_range(0..pool.len())].to_owned()];
            if rng.random_bool(0.2) {
                patterns.push(pool[rng.random_range(0..pool.len())].to_owned());
            }
            let spec: Vec<String> = if rng.random_bool(0.3) {
                vec![SPECS[rng.random_range(0..SPECS.len())].to_owned()]
            } else {
                Vec::new()
            };
            let mut query = GrepQuery::new(rev.clone(), patterns.clone());
            query.flags = flags;
            query.pathspec = Pathspec::parse(&spec).expect("spec");
            let (files, stats) = run_grep(&odb, &snapshot, &query).await;
            assert!(!stats.truncated && !stats.deadline_exceeded);
            matched += files.len();
            let mut args = vec!["grep".to_owned()];
            for (on, flag) in [
                (flags.ignore_case, "-i"),
                (flags.word, "-w"),
                (flags.extended, "-E"),
                (flags.fixed, "-F"),
            ] {
                if on {
                    args.push(flag.into());
                }
            }
            args.push(if flags.files_only { "-l" } else { "-n" }.into());
            for pattern in &patterns {
                args.push("-e".into());
                args.push(pattern.clone());
            }
            args.push(rev.clone());
            args.push("--".into());
            args.extend(spec.iter().cloned());
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let theirs = git.git_lenient(&args);
            assert_eq!(
                grep_output(&rev, &files, flags.files_only),
                String::from_utf8_lossy(&theirs),
                "seed {seed}: git {args:?}"
            );
        }
    }
    assert!(matched > 100, "the patterns match ({matched} files)");
}

/// A repository with one commit of `files` (path, content), loaded.
async fn grep_repo(
    test: &str,
    files: &[(String, Vec<u8>)],
    byte: u8,
) -> Option<(
    GitRepo,
    Arc<loams_store::FaultyStore>,
    RangeOdb,
    RefSnapshot,
)> {
    let git = GitRepo::init(test)?;
    for (path, content) in files {
        let at = git.path().join(path);
        std::fs::create_dir_all(at.parent().expect("parent")).expect("mkdir");
        std::fs::write(at, content).expect("write");
    }
    git.git(&["add", "-A", "."], None);
    git.git(&["commit", "-q", "-m", "files"], None);
    let (faulty, store) = faulty_store();
    let (odb, _, snapshot) = load(&git, byte, store, 1).await;
    Some((git, faulty, odb, snapshot))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grep_deadline_returns_partial_with_flag() {
    // Incompressible files, so each blob is its own range read.
    let mut rng = StdRng::seed_from_u64(21);
    let files: Vec<(String, Vec<u8>)> = (0..200)
        .map(|i| {
            let mut content: Vec<u8> = (0..70_000)
                .map(|_| b"abcdefghijklmnopqrstuvwxyz"[rng.random_range(0..26)])
                .collect();
            content.extend_from_slice(b"\nneedle\n");
            (format!("f{i:03}.txt"), content)
        })
        .collect();
    let Some((_git, faulty, odb, snapshot)) =
        grep_repo("grep_deadline_returns_partial_with_flag", &files, 72).await
    else {
        return;
    };
    let mut query = GrepQuery::new("HEAD", vec!["needle".into()]);
    // A first search reads the idx fan-outs, which stay in memory.
    let (all, _) = run_grep(&odb, &snapshot, &query).await;
    assert_eq!(all.len(), files.len());
    for _ in 0..2000 {
        faulty.inject(Op::Get, Fault::Delay(std::time::Duration::from_millis(100)));
    }
    query.deadline = std::time::Duration::from_secs(2);
    let (found, stats) = run_grep(&odb, &snapshot, &query).await;
    faulty.clear();
    assert!(stats.deadline_exceeded, "{stats:?}");
    assert!(
        !found.is_empty() && found.len() < files.len(),
        "a partial answer: {} of {}",
        found.len(),
        files.len()
    );
    for (i, file) in found.iter().enumerate() {
        assert_eq!(file.path, format!("f{i:03}.txt").into_bytes(), "path order");
        assert_eq!(file.lines.len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_skips_binary_like_git() {
    let mut late = vec![b'x'; 9000];
    late.extend_from_slice(b"\0needle\n");
    let files = vec![
        (
            "bin.dat".to_owned(),
            b"\0\x01needle\nneedle again\n".to_vec(),
        ),
        ("late-nul.txt".to_owned(), late),
        ("text.txt".to_owned(), b"a needle\nno\n".to_vec()),
        ("other.bin".to_owned(), b"\0nothing here\n".to_vec()),
    ];
    let Some((git, _, odb, snapshot)) = grep_repo("grep_skips_binary_like_git", &files, 73).await
    else {
        return;
    };
    for files_only in [false, true] {
        let mut query = GrepQuery::new("HEAD", vec!["needle".into()]);
        query.flags.files_only = files_only;
        let (found, _) = run_grep(&odb, &snapshot, &query).await;
        let bin = found
            .iter()
            .find(|f| f.path == b"bin.dat")
            .expect("bin.dat matches");
        assert!(bin.binary && bin.lines.is_empty());
        let late = found
            .iter()
            .find(|f| f.path == b"late-nul.txt")
            .expect("late NUL");
        assert!(
            !late.binary,
            "a NUL past 8,000 bytes is text, as git decides"
        );
        let flag = if files_only { "-l" } else { "-n" };
        let theirs = git.git_lenient(&["grep", flag, "-e", "needle", "HEAD"]);
        assert_eq!(
            grep_output("HEAD", &found, files_only),
            String::from_utf8_lossy(&theirs)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_stops_at_max_matches() {
    let lines = |n: usize| "hit\n".repeat(n).into_bytes();
    let files = vec![
        ("at.txt".to_owned(), lines(GREP_MAX_MATCHES)),
        ("past.txt".to_owned(), lines(1)),
    ];
    let Some((_git, _, odb, snapshot)) = grep_repo("grep_stops_at_max_matches", &files, 74).await
    else {
        return;
    };
    let mut query = GrepQuery::new("HEAD", vec!["hit".into()]);
    query.pathspec = Pathspec::parse(&["at.txt".into()]).expect("spec");
    let (found, stats) = run_grep(&odb, &snapshot, &query).await;
    assert_eq!(found[0].lines.len(), GREP_MAX_MATCHES);
    assert!(!stats.truncated, "at the limit");
    query.pathspec = Pathspec::all();
    let (found, stats) = run_grep(&odb, &snapshot, &query).await;
    assert!(stats.truncated, "one past the limit");
    assert_eq!(stats.matches, GREP_MAX_MATCHES as u64);
    assert_eq!(found.len(), 1, "past.txt is not answered");
}

#[tokio::test]
async fn grep_refuses_what_it_cannot_answer_like_git() {
    let at: Vec<String> = (0..MAX_GREP_PATTERNS).map(|i| format!("p{i}")).collect();
    assert!(loams_git::history::grep::matcher(&at, GrepFlags::default()).is_ok());
    let past: Vec<String> = (0..=MAX_GREP_PATTERNS).map(|i| format!("p{i}")).collect();
    assert!(matches!(
        loams_git::history::grep::matcher(&past, GrepFlags::default()),
        Err(HistoryError::TooMany { .. })
    ));
    for pattern in ["\\(a\\)\\1", "x\\", "^$", "x*"] {
        assert!(
            matches!(
                loams_git::history::grep::matcher(&[pattern.to_owned()], GrepFlags::default()),
                Err(HistoryError::Pattern { .. })
            ),
            "{pattern}"
        );
    }
}
