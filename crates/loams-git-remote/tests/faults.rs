//! Lost acknowledgements and lost answers (plan GT1 Task 51; old GT1 Task
//! 9's `faults.rs`), through the `LOAMS_GIT_FAULTS` hook of the `faults`
//! feature.

mod support;

use loams_git::format::event::TxnBody;

use support::{Env, repo_id};

#[tokio::test(flavor = "multi_thread")]
async fn push_survives_lost_acks() {
    let Some(env) = Env::new("push_survives_lost_acks") else {
        return;
    };
    for seed in 1..=4u64 {
        let repo = repo_id(&format!("lostacks{seed}"));
        env.create_repo(&repo).await;
        let dir = env.init(&format!("a{seed}"));
        env.ok(&dir, &["remote", "add", "origin", &env.url(&repo)]);
        let faults = format!("{seed}:0.5");
        let mut heads = Vec::new();
        for round in 0..3 {
            heads.push(env.commit(&dir, &format!("f{round}.txt"), &format!("{round}\n")));
            // Half of the create-only PUTs (packs, segments) land and then
            // answer an error.
            let out = env.git_env(
                &dir,
                &[
                    "push",
                    "-q",
                    "origin",
                    "main",
                    &format!("main:refs/heads/r{round}"),
                ],
                &[("LOAMS_GIT_FAULTS", faults.as_str())],
            );
            assert!(
                out.status.success(),
                "seed {seed}, round {round}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let head = heads.last().cloned();
        assert_eq!(env.remote_ref(&repo, "refs/heads/main"), head);
        let clone = env.clone_repo(&repo, &format!("check{seed}"));
        env.ok(&clone, &["fsck", "--no-progress"]);
        for (round, head) in heads.iter().enumerate() {
            assert_eq!(
                env.remote_ref(&repo, &format!("refs/heads/r{round}"))
                    .as_ref(),
                Some(head)
            );
        }
        // Each ref of each push committed exactly once (a push that is not
        // atomic commits one transaction per ref).
        assert_eq!(env.ref_txns(&repo).await.len(), 6, "seed {seed}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_push_after_lost_answer_is_replayed() {
    let Some(env) = Env::new("repeated_push_after_lost_answer_is_replayed") else {
        return;
    };
    let repo = repo_id("lostanswer");
    env.create_repo(&repo).await;
    let dir = env.init("a");
    env.ok(&dir, &["remote", "add", "origin", &env.url(&repo)]);
    for round in 0..2 {
        let head = env.commit(&dir, "f.txt", &format!("{round}\n"));
        // The first answer of every commit is lost; the helper repeats the
        // commit with the same key and is answered with the first receipt.
        let out = env.git_env(
            &dir,
            &["push", "origin", "main"],
            &[("LOAMS_GIT_FAULTS", "lose-answer")],
        );
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "round {round}: {err}");
        assert!(
            err.contains("loams: replayed the push committed at seq"),
            "round {round}: {err}"
        );
        assert_eq!(env.remote_ref(&repo, "refs/heads/main"), Some(head));
    }
    // Two pushes, two transactions: the repeats were never applied twice.
    let txns = env.ref_txns(&repo).await;
    assert_eq!(txns.len(), 2);
    for (event, _) in &txns {
        assert!(matches!(event.body, TxnBody::RefTxn(_)));
    }
}
