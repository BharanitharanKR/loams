//! Many `git push` processes, each its own sequencer, on one repository
//! (plan GT1 Task 51; old GT1 Task 9's `concurrent.rs`). They are fenced
//! by the create-only segment PUT (§36 §4.2, Ruling 2).

mod support;

use std::path::PathBuf;
use std::sync::Arc;

use support::{Env, repo_id};

/// Pushers per test.
const PUSHERS: usize = 8;

/// Runs `f(i)` for every pusher on its own thread, all at once.
fn all_at_once<T: Send + 'static>(f: impl Fn(usize) -> T + Send + Sync + 'static) -> Vec<T> {
    let f = Arc::new(f);
    let barrier = Arc::new(std::sync::Barrier::new(PUSHERS));
    let handles: Vec<_> = (0..PUSHERS)
        .map(|i| {
            let f = Arc::clone(&f);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                f(i)
            })
        })
        .collect();
    handles
        .into_iter()
        .map(|h| h.join().expect("pusher"))
        .collect()
}

/// A repository with one commit on `main`, and `PUSHERS` clones of it.
async fn setup(env: &Arc<Env>, repo: &str) -> Vec<PathBuf> {
    env.create_repo(repo).await;
    let seed = env.init("seed");
    env.commit(&seed, "base.txt", "base\n");
    env.ok(&seed, &["push", "-q", &env.url(repo), "main"]);
    (0..PUSHERS)
        .map(|i| env.clone_repo(repo, &format!("p{i}")))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pushes_to_distinct_branches_all_land() {
    let Some(env) = Env::new("concurrent_pushes_to_distinct_branches_all_land") else {
        return;
    };
    let env = Arc::new(env);
    let repo = repo_id("distinct");
    let clones = setup(&env, &repo).await;
    let heads: Vec<String> = clones
        .iter()
        .enumerate()
        .map(|(i, dir)| env.commit(dir, &format!("f{i}.txt"), &format!("{i}\n")))
        .collect();
    let pushes = {
        let env = Arc::clone(&env);
        let clones = clones.clone();
        all_at_once(move |i| {
            env.git(
                &clones[i],
                &["push", "-q", "origin", &format!("HEAD:refs/heads/b{i}")],
            )
        })
    };
    for (i, out) in pushes.iter().enumerate() {
        assert!(
            out.status.success(),
            "pusher {i}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    for (i, head) in heads.iter().enumerate() {
        assert_eq!(
            env.remote_ref(&repo, &format!("refs/heads/b{i}")).as_ref(),
            Some(head)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pushes_to_one_branch_one_wins_per_round() {
    let Some(env) = Env::new("concurrent_pushes_to_one_branch_one_wins_per_round") else {
        return;
    };
    let env = Arc::new(env);
    let repo = repo_id("onebranch");
    let clones = setup(&env, &repo).await;
    for round in 0..5 {
        let heads: Vec<String> = clones
            .iter()
            .enumerate()
            .map(|(i, dir)| {
                env.ok(dir, &["fetch", "-q"]);
                env.ok(dir, &["reset", "-q", "--hard", "origin/main"]);
                env.commit(dir, &format!("r{round}-{i}.txt"), "x\n")
            })
            .collect();
        let pushes = {
            let env = Arc::clone(&env);
            let clones = clones.clone();
            all_at_once(move |i| env.git(&clones[i], &["push", "origin", "main"]))
        };
        let winners: Vec<usize> = (0..PUSHERS)
            .filter(|&i| pushes[i].status.success())
            .collect();
        assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
        for (i, out) in pushes.iter().enumerate() {
            if !out.status.success() {
                let err = String::from_utf8_lossy(&out.stderr);
                assert!(
                    err.contains("fetch first"),
                    "round {round}, pusher {i}: {err}"
                );
            }
        }
        assert_eq!(
            env.remote_ref(&repo, "refs/heads/main").as_ref(),
            Some(&heads[winners[0]])
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pushes_never_lose_an_acked_push() {
    let Some(env) = Env::new("concurrent_pushes_never_lose_an_acked_push") else {
        return;
    };
    let env = Arc::new(env);
    let repo = repo_id("noloss");
    let clones = setup(&env, &repo).await;
    // Each pusher tries five times: fetch, rebuild on the tip, push. Every
    // push git reports as done is a fast-forward, so every acknowledged
    // commit must be in the final history.
    let acked: Vec<Vec<String>> = {
        let env = Arc::clone(&env);
        let clones = clones.clone();
        all_at_once(move |i| {
            let dir = &clones[i];
            let mut acked = Vec::new();
            for attempt in 0..5 {
                env.ok(dir, &["fetch", "-q"]);
                env.ok(dir, &["reset", "-q", "--hard", "origin/main"]);
                let head = env.commit(dir, &format!("p{i}-{attempt}.txt"), "x\n");
                if env
                    .git(dir, &["push", "-q", "origin", "main"])
                    .status
                    .success()
                {
                    acked.push(head);
                }
            }
            acked
        })
    };
    let acked: Vec<String> = acked.into_iter().flatten().collect();
    assert!(!acked.is_empty(), "no push was acknowledged");
    let check = env.clone_repo(&repo, "check");
    let history = env.ok(&check, &["rev-list", "HEAD"]);
    for oid in &acked {
        assert!(
            history.lines().any(|line| line == oid),
            "acknowledged {oid} is not in the final history"
        );
    }
    env.ok(&check, &["fsck", "--no-progress"]);
}

/// §36 risk 8 (plan GT1 Task 14): serverless helpers and a server's
/// sequencer write one repository at once. Each is fenced by the other's
/// segments; every acknowledged write lands, and the log stays one line.
#[tokio::test(flavor = "multi_thread")]
async fn helper_and_server_race_one_repo() {
    use loams_git::{
        Expect, IdemKey, NamespaceId, ObjectId, Principal, ReadAt, RefLog, RefName, RefTxn,
        RefUpdate, RepoId, RepoSequencers, SystemClock,
    };

    let Some(env) = Env::new("helper_and_server_race_one_repo") else {
        return;
    };
    let env = Arc::new(env);
    let repo = repo_id("race");
    let clones = setup(&env, &repo).await;
    let base = env.rev(&clones[0], "HEAD");
    let base_oid = ObjectId::from_hex(base.as_bytes()).expect("oid");
    let heads: Vec<String> = clones
        .iter()
        .enumerate()
        .map(|(i, dir)| env.commit(dir, &format!("h{i}.txt"), "x\n"))
        .collect();
    let server = RepoSequencers::new(env.store(), Arc::new(SystemClock));
    let id = RepoId::parse(&repo).expect("id");
    let server_writes = {
        let server = server.clone();
        let id = id.clone();
        tokio::spawn(async move {
            for i in 0..30u32 {
                let txn = RefTxn {
                    key: IdemKey::parse(&format!("server-{i}")).expect("key"),
                    request_digest: [i as u8; 32],
                    updates: vec![RefUpdate {
                        name: RefName::parse(&format!("refs/heads/s{i}")).expect("ref"),
                        expect: Expect::Absent,
                        new: Some(base_oid),
                    }],
                    packs: Vec::new(),
                    principal: Principal::parse("server").expect("principal"),
                    scope: None,
                    trace: None,
                    push_options: Vec::new(),
                };
                server
                    .commit(NamespaceId(1), "local/1", &id, txn)
                    .await
                    .expect("server commit");
            }
        })
    };
    let pushes = {
        let env = Arc::clone(&env);
        let clones = clones.clone();
        tokio::task::spawn_blocking(move || {
            all_at_once(move |i| {
                env.git(
                    &clones[i],
                    &["push", "-q", "origin", &format!("HEAD:refs/heads/h{i}")],
                )
            })
        })
        .await
        .expect("pushers")
    };
    server_writes.await.expect("server");
    for (i, out) in pushes.iter().enumerate() {
        assert!(
            out.status.success(),
            "pusher {i}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let log = server
        .get(NamespaceId(1), "local/1", &id)
        .await
        .expect("log");
    let snapshot = log.snapshot(ReadAt::Latest).await.expect("snapshot");
    for i in 0..30 {
        assert_eq!(
            snapshot
                .refs
                .get(&RefName::parse(&format!("refs/heads/s{i}")).expect("ref")),
            Some(&base_oid)
        );
    }
    for (i, head) in heads.iter().enumerate() {
        assert_eq!(
            env.remote_ref(&repo, &format!("refs/heads/h{i}")).as_ref(),
            Some(head)
        );
    }
    // One line: every segment from 1 to the tail is there, each once.
    let events = env.events(&repo).await;
    assert_eq!(events.len(), 1 + 30 + PUSHERS, "seed push, server, helpers");
    server.shutdown().await;
}
