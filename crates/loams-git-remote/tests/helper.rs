//! `git-remote-loams` driven by the real `git` binary against a `file://`
//! bucket (plan GT1 Task 51; the morning version's Task 12, old GT1 Task 9).

mod support;

use loams_git::format::pb;
use loams_git::{
    BucketRepoCatalog, IdemKey, ReadAt, RepoCatalog, RepoId, RepoName, StoreWalStore, WalStore,
    fork_repo,
};

use support::{Env, NS, repo_id};

#[tokio::test(flavor = "multi_thread")]
async fn clone_of_missing_repo_fails() {
    let Some(env) = Env::new("clone_of_missing_repo_fails") else {
        return;
    };
    let url = env.url("rmissing");
    let err = env.fails(env.path(), &["clone", "-q", &url, "x"]);
    let address = url.trim_start_matches("loams::");
    assert!(
        err.contains(&format!("fatal: repository '{address}' not found")),
        "{err}"
    );
    // A malformed address names the expected shape.
    let bad = format!("loams::{}/nope", env.store_url());
    let err = env.fails(env.path(), &["clone", "-q", &bad, "y"]);
    assert!(
        err.contains(&format!(
            "fatal: loams: expected <store-url>/ns/<ns>/repos/<repo_id>, got {}/nope",
            env.store_url()
        )),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn push_to_missing_repo_fails_without_create_flag() {
    let Some(env) = Env::new("push_to_missing_repo_fails_without_create_flag") else {
        return;
    };
    let repo = repo_id("nocreate");
    let dir = env.init("a");
    env.commit(&dir, "a.txt", "a\n");
    let err = env.fails(&dir, &["push", &env.url(&repo), "main"]);
    assert!(err.contains("not found"), "{err}");
    let checkpoint = env
        .wal(&repo)
        .latest_checkpoint(loams_git::Seq(u64::MAX))
        .await
        .expect("read");
    assert!(checkpoint.is_none(), "a typo must not create a repository");
}

#[tokio::test(flavor = "multi_thread")]
async fn first_push_creates_repo_with_flag() {
    let Some(env) = Env::new("first_push_creates_repo_with_flag") else {
        return;
    };
    let repo = repo_id("created");
    let dir = env.init("a");
    let head = env.commit(&dir, "a.txt", "a\n");
    env.ok(&dir, &["branch", "-m", "trunk"]);
    let out = env.git_env(
        &dir,
        &["push", &env.url(&repo), "trunk"],
        &[("LOAMS_GIT_CREATE_ON_PUSH", "1")],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        env.remote_ref(&repo, "refs/heads/trunk"),
        Some(head.clone())
    );
    // HEAD names the first branch pushed, so a clone checks it out.
    let clone = env.clone_repo(&repo, "b");
    assert_eq!(env.rev(&clone, "HEAD"), head);
    assert_eq!(
        std::fs::read_to_string(clone.join("a.txt")).expect("read"),
        "a\n"
    );
    assert_eq!(
        env.ok(&clone, &["symbolic-ref", "HEAD"]).trim(),
        "refs/heads/trunk"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_fetch_push_round_trip() {
    let Some(env) = Env::new("clone_fetch_push_round_trip") else {
        return;
    };
    let repo = repo_id("roundtrip");
    env.create_repo(&repo).await;
    // An empty repository clones as empty.
    let empty = env.work("empty");
    let out = env.git(
        empty.parent().expect("parent"),
        &["clone", &env.url(&repo), "empty"],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // First push from a fresh repository.
    let a = env.init("a");
    env.commit(&a, "src/lib.rs", "pub fn one() {}\n");
    env.ok(&a, &["remote", "add", "origin", &env.url(&repo)]);
    env.ok(&a, &["push", "-q", "-u", "origin", "main"]);
    env.ok(&a, &["tag", "-a", "v1", "-m", "release one"]);
    env.ok(&a, &["push", "-q", "origin", "v1"]);
    // A clone sees the branch, the file and the annotated tag.
    let b = env.clone_repo(&repo, "b");
    assert_eq!(env.rev(&b, "HEAD"), env.rev(&a, "HEAD"));
    assert_eq!(env.rev(&b, "v1"), env.rev(&a, "v1"));
    assert_eq!(env.ok(&b, &["cat-file", "-t", "v1"]).trim(), "tag");
    env.ok(&b, &["fsck", "--strict", "--no-progress"]);
    // The clone pushes; the first repository fetches it.
    let pushed = env.commit(&b, "src/two.rs", "pub fn two() {}\n");
    env.ok(&b, &["push", "-q"]);
    env.ok(&a, &["pull", "-q", "--ff-only"]);
    assert_eq!(env.rev(&a, "HEAD"), pushed);
    assert_eq!(
        std::fs::read_to_string(a.join("src/two.rs")).expect("read"),
        "pub fn two() {}\n"
    );
    env.ok(&a, &["fsck", "--strict", "--no-progress"]);
    // A tag that exists is not moved without force.
    env.ok(&b, &["tag", "-f", "-a", "v1", "-m", "moved"]);
    let err = env.fails(&b, &["push", "origin", "v1"]);
    assert!(err.contains("already exists"), "{err}");
    // A dry run writes nothing.
    env.commit(&b, "src/three.rs", "pub fn three() {}\n");
    let before = env.ref_txns(&repo).await.len();
    env.ok(&b, &["push", "-q", "--dry-run", "origin", "main"]);
    assert_eq!(env.ref_txns(&repo).await.len(), before);
    assert_eq!(env.remote_ref(&repo, "refs/heads/main"), Some(pushed));
}

#[tokio::test(flavor = "multi_thread")]
async fn non_fast_forward_is_rejected_with_fetch_first() {
    let Some(env) = Env::new("non_fast_forward_is_rejected_with_fetch_first") else {
        return;
    };
    let repo = repo_id("nonff");
    env.create_repo(&repo).await;
    let a = env.init("a");
    env.commit(&a, "a.txt", "a\n");
    env.ok(&a, &["push", "-q", &env.url(&repo), "main"]);
    let b = env.clone_repo(&repo, "b");
    let c = env.clone_repo(&repo, "c");
    let winner = env.commit(&b, "b.txt", "b\n");
    env.ok(&b, &["push", "-q"]);
    // `c` has not seen `b`'s commit: git refuses with "fetch first".
    env.commit(&c, "c.txt", "c\n");
    let err = env.fails(&c, &["push"]);
    assert!(err.contains("fetch first"), "{err}");
    // Having fetched, `c`'s history diverges: "non-fast-forward".
    env.ok(&c, &["fetch", "-q"]);
    let err = env.fails(&c, &["push"]);
    assert!(err.contains("non-fast-forward"), "{err}");
    assert_eq!(env.remote_ref(&repo, "refs/heads/main"), Some(winner));
}

#[tokio::test(flavor = "multi_thread")]
async fn force_push_with_plus_succeeds() {
    let Some(env) = Env::new("force_push_with_plus_succeeds") else {
        return;
    };
    let repo = repo_id("force");
    env.create_repo(&repo).await;
    let a = env.init("a");
    env.commit(&a, "a.txt", "a\n");
    env.ok(&a, &["push", "-q", &env.url(&repo), "main"]);
    let b = env.clone_repo(&repo, "b");
    env.commit(&a, "a.txt", "a2\n");
    env.ok(&a, &["push", "-q", &env.url(&repo), "main"]);
    // `b` rewrites history and forces it over `a`'s commit.
    env.ok(&b, &["commit", "-q", "--amend", "-m", "rewritten"]);
    let forced = env.rev(&b, "HEAD");
    env.ok(&b, &["fetch", "-q"]);
    let err = env.fails(&b, &["push", "origin", "main"]);
    assert!(err.contains("non-fast-forward"), "{err}");
    env.ok(&b, &["push", "-q", "origin", "+main"]);
    assert_eq!(env.remote_ref(&repo, "refs/heads/main"), Some(forced));
    // A fresh clone has the forced history.
    let c = env.clone_repo(&repo, "c");
    assert_eq!(env.rev(&c, "HEAD"), env.rev(&b, "HEAD"));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_ref() {
    let Some(env) = Env::new("delete_ref") else {
        return;
    };
    let repo = repo_id("delete");
    env.create_repo(&repo).await;
    let a = env.init("a");
    env.commit(&a, "a.txt", "a\n");
    env.ok(&a, &["remote", "add", "origin", &env.url(&repo)]);
    env.ok(
        &a,
        &["push", "-q", "origin", "main", "main:refs/heads/feature"],
    );
    assert!(env.remote_ref(&repo, "refs/heads/feature").is_some());
    env.ok(&a, &["push", "-q", "origin", "--delete", "feature"]);
    assert_eq!(env.remote_ref(&repo, "refs/heads/feature"), None);
    assert!(env.remote_ref(&repo, "refs/heads/main").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_multi_ref_push_all_or_none() {
    let Some(env) = Env::new("atomic_multi_ref_push_all_or_none") else {
        return;
    };
    let repo = repo_id("atomic");
    env.create_repo(&repo).await;
    let a = env.init("a");
    let first = env.commit(&a, "a.txt", "a\n");
    env.ok(&a, &["remote", "add", "origin", &env.url(&repo)]);
    env.ok(
        &a,
        &["push", "-q", "origin", "main", "main:refs/heads/keep"],
    );
    // `keep` may not be deleted: a protection every sequencer checks.
    let log = env.open_log(&repo).await;
    log.commit_config(
        IdemKey::parse("protect-keep").expect("key"),
        pb::ConfigChange {
            symrefs: vec![pb::SymRef {
                name: "HEAD".into(),
                target: "refs/heads/main".into(),
            }],
            protections: vec![pb::Protection {
                pattern: "refs/heads/keep".into(),
                deny_delete: true,
                deny_force: false,
            }],
            request_digest: Vec::new(),
        },
    )
    .await
    .expect("protect");
    log.shutdown().await;
    let second = env.commit(&a, "a.txt", "a2\n");
    // Atomic: the refused delete refuses the update of main too.
    let err = env.fails(
        &a,
        &["push", "--atomic", "origin", "main", ":refs/heads/keep"],
    );
    assert!(err.contains("protected"), "{err}");
    assert_eq!(
        env.remote_ref(&repo, "refs/heads/main"),
        Some(first.clone())
    );
    assert_eq!(
        env.remote_ref(&repo, "refs/heads/keep"),
        Some(first.clone())
    );
    // Not atomic: main lands, the delete is refused on its own.
    let err = env.fails(&a, &["push", "origin", "main", ":refs/heads/keep"]);
    assert!(err.contains("protected"), "{err}");
    assert_eq!(env.remote_ref(&repo, "refs/heads/main"), Some(second));
    assert_eq!(env.remote_ref(&repo, "refs/heads/keep"), Some(first));
}

#[tokio::test(flavor = "multi_thread")]
async fn push_options_are_recorded() {
    let Some(env) = Env::new("push_options_are_recorded") else {
        return;
    };
    let repo = repo_id("options");
    env.create_repo(&repo).await;
    let a = env.init("a");
    env.commit(&a, "a.txt", "a\n");
    env.ok(
        &a,
        &[
            "push",
            "-q",
            "-o",
            "ci.skip",
            "-o",
            "reviewer=two words",
            &env.url(&repo),
            "main",
        ],
    );
    let txns = env.ref_txns(&repo).await;
    assert_eq!(txns.len(), 1);
    let (event, txn) = &txns[0];
    assert_eq!(txn.push_options, vec!["ci.skip", "reviewer=two words"]);
    assert_eq!(txn.principal, "t@example.com");
    assert!(event.key.as_str().starts_with("git-push:"), "{}", event.key);
    assert_eq!(txn.packs.len(), 1, "one pack per push");
}

#[tokio::test(flavor = "multi_thread")]
async fn fork_then_clone() {
    let Some(env) = Env::new("fork_then_clone") else {
        return;
    };
    let parent = repo_id("parent");
    let child = repo_id("child");
    env.create_repo(&parent).await;
    let a = env.init("a");
    let head = env.commit(&a, "a.txt", "from the parent\n");
    env.ok(&a, &["push", "-q", &env.url(&parent), "main"]);
    let log = env.open_log(&parent).await;
    let dst = StoreWalStore::new(env.store(), env.paths(&child));
    fork_repo(
        &log,
        &dst,
        &RepoId::parse(&parent).expect("id"),
        ReadAt::Latest,
    )
    .await
    .expect("fork");
    log.shutdown().await;
    // The fork has no pack of its own: its objects come from the parent's.
    let b = env.clone_repo(&child, "b");
    assert_eq!(env.rev(&b, "HEAD"), head);
    assert_eq!(
        std::fs::read_to_string(b.join("a.txt")).expect("read"),
        "from the parent\n"
    );
    // The fork diverges without touching the parent.
    let pushed = env.commit(&b, "b.txt", "in the fork\n");
    env.ok(&b, &["push", "-q"]);
    assert_eq!(
        env.remote_ref(&child, "refs/heads/main"),
        Some(pushed.clone())
    );
    assert_eq!(env.remote_ref(&parent, "refs/heads/main"), Some(head));
    let c = env.clone_repo(&child, "c");
    assert_eq!(env.rev(&c, "HEAD"), pushed);
    env.ok(&c, &["fsck", "--no-progress"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_after_other_push_gets_new_pack() {
    let Some(env) = Env::new("fetch_after_other_push_gets_new_pack") else {
        return;
    };
    let repo = repo_id("fetchnew");
    env.create_repo(&repo).await;
    let a = env.init("a");
    env.commit(&a, "a.txt", "a\n");
    env.ok(&a, &["remote", "add", "origin", &env.url(&repo)]);
    env.ok(&a, &["push", "-q", "-u", "origin", "main"]);
    let b = env.clone_repo(&repo, "b");
    let record = b.join(".git/loams/origin/packs");
    let packs = |path: &std::path::Path| {
        std::fs::read_to_string(path)
            .expect("record")
            .lines()
            .count()
    };
    assert_eq!(packs(&record), 1);
    let new = env.commit(&a, "a.txt", "a2\n");
    env.ok(&a, &["push", "-q"]);
    env.ok(&b, &["fetch", "-q"]);
    assert_eq!(env.rev(&b, "origin/main"), new);
    env.ok(&b, &["cat-file", "-e", &new]);
    assert_eq!(packs(&record), 2, "only the new pack was fetched");
    // The pusher recorded its own pack: its next fetch downloads nothing.
    let before: Vec<_> = std::fs::read_dir(a.join(".git/objects/pack"))
        .expect("packs")
        .collect();
    env.ok(&a, &["fetch", "-q"]);
    let after: Vec<_> = std::fs::read_dir(a.join(".git/objects/pack"))
        .expect("packs")
        .collect();
    assert_eq!(before.len(), after.len());
}

/// Ruling T51.2: the last segment may be a catalog name; a repository
/// being deleted is not found.
#[tokio::test(flavor = "multi_thread")]
async fn catalog_name_resolves_and_deleted_is_not_found() {
    let Some(env) = Env::new("catalog_name_resolves_and_deleted_is_not_found") else {
        return;
    };
    let id = env.create_named("web").await;
    let a = env.init("a");
    let head = env.commit(&a, "index.html", "<p>hi</p>\n");
    env.ok(&a, &["push", "-q", &env.url("web"), "main"]);
    // The push landed in the repository the name maps to.
    assert_eq!(
        env.remote_ref(id.as_str(), "refs/heads/main"),
        Some(head.clone())
    );
    let b = env.clone_repo("web", "b");
    assert_eq!(env.rev(&b, "HEAD"), head);
    BucketRepoCatalog::new(env.store())
        .mark_deleting(NS, &RepoName::parse("web").expect("name"), 1)
        .await
        .expect("delete");
    let err = env.fails(env.path(), &["clone", "-q", &env.url("web"), "c"]);
    assert!(err.contains("not found"), "{err}");
    let err = env.fails(env.path(), &["clone", "-q", &env.url(id.as_str()), "d"]);
    assert!(err.contains("not found"), "{err}");
}

/// `MAX_REFS_PER_PUSH` (4096) refspecs are served; one more is refused
/// before anything is read or written. Driven on the helper's own
/// protocol, as git would.
#[tokio::test(flavor = "multi_thread")]
async fn push_ref_limit_at_and_past() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let Some(env) = Env::new("push_ref_limit_at_and_past") else {
        return;
    };
    let repo = repo_id("reflimit");
    env.create_repo(&repo).await;
    let a = env.init("a");
    let run = |refs: usize| {
        let mut input = String::from("list for-push\n");
        for i in 0..refs {
            input.push_str(&format!("push :refs/heads/gone{i}\n"));
        }
        input.push('\n');
        let mut child = Command::new(env!("CARGO_BIN_EXE_git-remote-loams"))
            .arg("origin")
            .arg(env.url(&repo).trim_start_matches("loams::"))
            .env("GIT_DIR", a.join(".git"))
            .env("LOAMS_GIT_UNAUTHENTICATED_TESTS", "1")
            .env("HOME", env.path())
            .env("GIT_CONFIG_GLOBAL", env.path().join(".gitconfig"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("helper");
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input.as_bytes())
            .expect("write");
        child.wait_with_output().expect("helper")
    };
    let at = run(4096);
    assert!(
        at.status.success(),
        "{}",
        String::from_utf8_lossy(&at.stderr)
    );
    let answers = String::from_utf8(at.stdout).expect("utf-8");
    assert_eq!(
        answers
            .lines()
            .filter(|l| l.ends_with(" remote ref does not exist"))
            .count(),
        4096
    );
    let past = run(4097);
    assert!(!past.status.success());
    assert!(
        String::from_utf8_lossy(&past.stderr).contains("at most 4096 refs"),
        "{}",
        String::from_utf8_lossy(&past.stderr)
    );
    assert!(env.ref_txns(&repo).await.is_empty());
}

/// The release packages' smoke test: the installed helper answers
/// `--version` without a remote, a bucket or a repository.
#[test]
fn version_flag_prints_the_version() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_git-remote-loams"))
        .arg("--version")
        .output()
        .expect("run git-remote-loams");
    assert!(out.status.success(), "{out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("git-remote-loams {}", env!("CARGO_PKG_VERSION"))
    );
}
