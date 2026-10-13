//! Shared fixtures: the real `git` binary (a process, never a library:
//! D394) driving the built `git-remote-loams` against a `file://` bucket
//! under the cargo target's tmp directory.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use loams_git::catalog::{CreateRepo, Idempotency};
use loams_git::format::pb;
use loams_git::{
    BucketRefLog, BucketRepoCatalog, NamespaceId, RefName, RepoCatalog, RepoId, RepoName,
    RepoPaths, RepoState, SequencerConfig, StoreBlobStore, StoreWalStore, SystemClock, WalStore,
    format::event::{TxnBody, TxnEvent, from_cloudevent},
};
use loams_store::Store;

/// The namespace every test repository lives in.
pub const NS: NamespaceId = NamespaceId(1);

/// A scratch directory holding a `file://` bucket and git working trees.
#[derive(Debug)]
pub struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    /// A new environment, or `None` (with a printed reason) when git is not
    /// installed.
    pub fn new(test: &str) -> Option<Self> {
        if Command::new("git").arg("--version").output().is_err() {
            eprintln!("git not found: {test} skipped");
            return None;
        }
        let dir = tempfile::Builder::new()
            .prefix("git-remote-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let env = Self { dir };
        std::fs::create_dir_all(env.store_dir()).expect("store dir");
        std::fs::write(
            env.dir.path().join(".gitconfig"),
            "[user]\n\tname = t\n\temail = t@example.com\n[init]\n\tdefaultBranch = main\n\
             [advice]\n\tdetachedHead = false\n",
        )
        .expect("gitconfig");
        Some(env)
    }

    /// The scratch directory.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    fn store_dir(&self) -> PathBuf {
        self.dir.path().join("bucket")
    }

    /// The bucket's URL.
    pub fn store_url(&self) -> String {
        format!("file://{}", self.store_dir().display())
    }

    /// The bucket.
    pub fn store(&self) -> Store {
        Store::from_url(&self.store_url(), std::iter::empty::<(&str, &str)>()).expect("store")
    }

    /// The git URL of repository (id or name) `repo`.
    pub fn url(&self, repo: &str) -> String {
        format!("loams::{}/ns/{}/repos/{repo}", self.store_url(), NS)
    }

    /// A working directory path under the scratch directory (not created).
    pub fn work(&self, name: &str) -> PathBuf {
        self.dir.path().join("work").join(name)
    }

    /// `git <args>` in `cwd` with `extra` environment, the helper first on
    /// `PATH`, and a private git configuration.
    pub fn git_env(&self, cwd: &Path, args: &[&str], extra: &[(&str, &str)]) -> Output {
        std::fs::create_dir_all(cwd).expect("cwd");
        let helper = PathBuf::from(env!("CARGO_BIN_EXE_git-remote-loams"));
        let bin = helper.parent().expect("bin dir");
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(cwd)
            .env("PATH", path)
            .env("HOME", self.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", self.path().join(".gitconfig"))
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("LOAMS_GIT_UNAUTHENTICATED_TESTS", "1")
            .env_remove("LOAMS_GIT_CREATE_ON_PUSH")
            .env_remove("LOAMS_GIT_FAULTS")
            .stdin(Stdio::null());
        for (key, value) in extra {
            cmd.env(key, value);
        }
        cmd.output().expect("git runs")
    }

    /// `git <args>` in `cwd`.
    pub fn git(&self, cwd: &Path, args: &[&str]) -> Output {
        self.git_env(cwd, args, &[])
    }

    /// `git <args>` in `cwd`, which must succeed; its stdout.
    pub fn ok(&self, cwd: &Path, args: &[&str]) -> String {
        let out = self.git(cwd, args);
        assert!(
            out.status.success(),
            "git {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).expect("utf-8")
    }

    /// `git <args>` in `cwd`, which must fail; its stderr.
    pub fn fails(&self, cwd: &Path, args: &[&str]) -> String {
        let out = self.git(cwd, args);
        assert!(!out.status.success(), "git {args:?} succeeded");
        String::from_utf8(out.stderr).expect("utf-8")
    }

    /// A fresh local repository at `work/<name>` on branch `main`.
    pub fn init(&self, name: &str) -> PathBuf {
        let dir = self.work(name);
        self.ok(&dir, &["init", "-q", "-b", "main", "."]);
        dir
    }

    /// Clones `repo` into `work/<name>`.
    pub fn clone_repo(&self, repo: &str, name: &str) -> PathBuf {
        let dir = self.work(name);
        let parent = dir.parent().expect("parent").to_path_buf();
        self.ok(&parent, &["clone", "-q", &self.url(repo), name]);
        dir
    }

    /// Writes `file` in `dir` and commits it; returns the commit id.
    pub fn commit(&self, dir: &Path, file: &str, text: &str) -> String {
        let path = dir.join(file);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, text).expect("write");
        self.ok(dir, &["add", "-A", "."]);
        self.ok(dir, &["commit", "-q", "-m", &format!("write {file}")]);
        self.rev(dir, "HEAD")
    }

    /// The object `rev` names in `dir`.
    pub fn rev(&self, dir: &Path, rev: &str) -> String {
        self.ok(dir, &["rev-parse", rev]).trim().to_string()
    }

    /// The remote's refs (`git ls-remote`), as (oid, name) pairs.
    pub fn ls_remote(&self, repo: &str) -> Vec<(String, String)> {
        let out = self.ok(self.path(), &["ls-remote", &self.url(repo)]);
        out.lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(oid, name)| (oid.to_string(), name.to_string()))
            .collect()
    }

    /// The remote's value of `name`, if any.
    pub fn remote_ref(&self, repo: &str, name: &str) -> Option<String> {
        self.ls_remote(repo)
            .into_iter()
            .find(|(_, n)| n == name)
            .map(|(oid, _)| oid)
    }

    /// The id's paths.
    pub fn paths(&self, repo: &str) -> RepoPaths {
        RepoPaths::new(NS, &RepoId::parse(repo).expect("repo id"))
    }

    /// The WAL of repository id `repo`.
    pub fn wal(&self, repo: &str) -> StoreWalStore {
        StoreWalStore::new(self.store(), self.paths(repo))
    }

    /// Creates an empty repository with id `repo` whose `HEAD` is `main`.
    pub async fn create_repo(&self, repo: &str) {
        let mut state = RepoState::empty(pb::ObjectFormat::Sha1);
        state.symrefs.insert(
            "HEAD".into(),
            RefName::parse("refs/heads/main").expect("ref"),
        );
        self.wal(repo)
            .put_checkpoint(&state.to_checkpoint(0))
            .await
            .expect("create");
    }

    /// Creates an empty repository named `name` through the catalog;
    /// returns its id.
    pub async fn create_named(&self, name: &str) -> RepoId {
        let catalog = BucketRepoCatalog::new(self.store());
        let created = catalog
            .create(
                NS,
                CreateRepo {
                    name: RepoName::parse(name).expect("name"),
                    object_format: pb::ObjectFormat::Sha1,
                    default_branch: RefName::parse("refs/heads/main").expect("ref"),
                    idempotency: Idempotency {
                        key: None,
                        request_digest: [7; 32],
                    },
                },
            )
            .await
            .expect("create");
        created.into_record().repo_id
    }

    /// The ref log of repository id `repo`, as a server would open it.
    pub async fn open_log(&self, repo: &str) -> BucketRefLog {
        let paths = self.paths(repo);
        BucketRefLog::open(
            Arc::new(StoreWalStore::new(self.store(), paths.clone())),
            Arc::new(StoreBlobStore::new(self.store(), paths)),
            SequencerConfig::new(
                format!("local/{NS}"),
                NS,
                RepoId::parse(repo).expect("repo id"),
            ),
            Arc::new(SystemClock),
        )
        .await
        .expect("open")
    }

    /// Every transaction event in repository id `repo`'s WAL, in order.
    pub async fn events(&self, repo: &str) -> Vec<TxnEvent> {
        let segments = self
            .wal(repo)
            .read_from(loams_git::Seq(1), 100_000)
            .await
            .expect("read the WAL");
        segments
            .iter()
            .flat_map(|segment| segment.batch.events.iter())
            .map(|event| from_cloudevent(event).expect("decode"))
            .collect()
    }

    /// The ref transactions among [`Env::events`].
    pub async fn ref_txns(&self, repo: &str) -> Vec<(TxnEvent, pb::RefTransaction)> {
        self.events(repo)
            .await
            .into_iter()
            .filter_map(|event| match &event.body {
                TxnBody::RefTxn(txn) => {
                    let txn = txn.clone();
                    Some((event, txn))
                }
                _ => None,
            })
            .collect()
    }
}

/// A repository id unique to `tag`.
pub fn repo_id(tag: &str) -> String {
    format!("r{tag}")
}
