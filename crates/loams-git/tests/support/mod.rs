//! Shared fixtures: scratch git repositories driven by the real `git`
//! binary (a process, never a library: D394), and the pack objects they
//! produce.

#![allow(dead_code)]

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use loams_git::format::pb;
use loams_git::{BlobStore, LpkWriter, NamespaceId, ObjectId, RepoId, RepoPaths, StoreBlobStore};
use loams_store::{FaultyStore, Store};
use object_store::memory::InMemory;

/// The paths of a repository named by `byte` in namespace 7.
pub fn repo_paths(byte: u8) -> (RepoId, RepoPaths) {
    let repo = RepoId::parse(&format!("r01j9z3m5q8x00000000000000{byte:02x}")).expect("repo id");
    let paths = RepoPaths::new(NamespaceId(7), &repo);
    (repo, paths)
}

/// An in-memory store behind a fault injector, which also counts calls.
pub fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone()).with_sorted_listing(true);
    (faulty, store)
}

/// A scratch git repository under the cargo target's tmp directory.
#[derive(Debug)]
pub struct GitRepo {
    dir: tempfile::TempDir,
}

impl GitRepo {
    /// A new repository, or `None` (with a printed reason) when git is not
    /// installed.
    pub fn init(test: &str) -> Option<Self> {
        let dir = tempfile::Builder::new()
            .prefix("git-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("tempdir");
        let repo = Self { dir };
        if Command::new("git").arg("--version").output().is_err() {
            eprintln!("git not found: {test} skipped");
            return None;
        }
        repo.git(&["init", "-q", "-b", "main", "."], None);
        Some(repo)
    }

    /// The working directory.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// Runs git and returns its stdout; panics when it fails.
    pub fn git(&self, args: &[&str], stdin: Option<&[u8]>) -> Vec<u8> {
        let mut child = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
            .args(args)
            .current_dir(self.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", self.path())
            .env("GIT_AUTHOR_DATE", "2026-10-12T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2026-10-12T00:00:00Z")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("git runs");
        if let Some(input) = stdin {
            child
                .stdin
                .take()
                .expect("stdin")
                .write_all(input)
                .expect("write stdin");
        }
        drop(child.stdin.take());
        let out = child.wait_with_output().expect("git runs");
        assert!(out.status.success(), "git {args:?} failed");
        out.stdout
    }

    /// Writes `files` and commits them; returns the commit id.
    pub fn commit(&self, files: &[(&str, &str)], message: &str) -> ObjectId {
        for (name, text) in files {
            let path = self.path().join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("mkdir");
            }
            std::fs::write(path, text).expect("write");
        }
        self.git(&["add", "-A", "."], None);
        self.git(&["commit", "-q", "--allow-empty", "-m", message], None);
        self.rev("HEAD")
    }

    /// The object id `rev` names.
    pub fn rev(&self, rev: &str) -> ObjectId {
        let out = self.git(&["rev-parse", rev], None);
        oid(String::from_utf8(out).expect("utf-8").trim())
    }

    /// Every object reachable from `revs` (as `git rev-list --objects`).
    pub fn objects(&self, revs: &[&str]) -> Vec<ObjectId> {
        let mut args = vec!["rev-list", "--objects"];
        args.extend_from_slice(revs);
        let out = String::from_utf8(self.git(&args, None)).expect("utf-8");
        out.lines()
            .map(|line| oid(line.split(' ').next().expect("an id")))
            .collect()
    }

    /// An object's type and contents, as git stores them.
    pub fn cat(&self, id: &ObjectId) -> (String, Vec<u8>) {
        let hex = id.to_hex().to_string();
        let kind = String::from_utf8(self.git(&["cat-file", "-t", &hex], None)).expect("utf-8");
        let kind = kind.trim().to_string();
        let body = self.git(&["cat-file", &kind, &hex], None);
        (kind, body)
    }

    /// A pack of the objects `revs` reach (`pack-objects --revs`; `extra`
    /// adds flags such as `--delta-base-offset`), with its idx.
    pub fn pack_revs(&self, revs: &[&str], extra: &[&str]) -> (Bytes, Bytes) {
        let mut args = vec!["pack-objects", "--revs", "-q"];
        args.extend_from_slice(extra);
        args.push("out");
        let input = revs.join("\n") + "\n";
        self.read_pack_files(&args, input.as_bytes())
    }

    /// A pack of exactly `objects` (`pack-objects` from a list), with its idx.
    pub fn pack_objects(&self, objects: &[ObjectId]) -> (Bytes, Bytes) {
        let input: String = objects.iter().map(|id| format!("{id}\n")).collect();
        self.read_pack_files(&["pack-objects", "-q", "out"], input.as_bytes())
    }

    fn read_pack_files(&self, args: &[&str], input: &[u8]) -> (Bytes, Bytes) {
        let printed = String::from_utf8(self.git(args, Some(input))).expect("utf-8");
        let name = printed.trim();
        let pack = std::fs::read(self.path().join(format!("out-{name}.pack"))).expect("pack");
        let idx = std::fs::read(self.path().join(format!("out-{name}.idx"))).expect("idx");
        (pack.into(), idx.into())
    }
}

/// Parses 40 hex digits.
pub fn oid(hex: &str) -> ObjectId {
    ObjectId::from_hex(hex.as_bytes()).expect("object id")
}

/// Bundles a pack and its idx and stores it; returns its `PackRef`.
pub async fn put_pack(blobs: &StoreBlobStore, (pack, idx): (Bytes, Bytes)) -> pb::PackRef {
    let (id, pack_ref, bundle) = LpkWriter::from_parts(pack, idx).expect("bundle");
    blobs.put(&id, bundle).await.expect("put");
    pack_ref
}
