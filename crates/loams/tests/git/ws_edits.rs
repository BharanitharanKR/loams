//! Mount-less editing served (GT1 Task 26, design §36 §19.10, §19.11): `ReadFiles`,
//! `ApplyEdits`, `Diff` and `Rebase` on the main port.
//! `cargo test -p loams --features git --test git ws_edits::`.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::Running;
use super::objects::{history, load_ids, move_main, rev};
use super::workspaces::{Caller, OBJECTS, WS, create_ws, git_env, n_of, tree_with};

const MAX_READ_PATHS: usize = loams_git::limits::MAX_READ_PATHS;

fn b64(text: &str) -> String {
    STANDARD.encode(text.as_bytes())
}

fn unb64(value: &Value) -> Vec<u8> {
    STANDARD
        .decode(value.as_str().unwrap_or_default())
        .expect("base64")
}

fn write(path: &str, text: &str) -> Value {
    json!({"write": {"path": path, "content": b64(text)}})
}

async fn new_ws(op: &Caller) -> String {
    create_ws(op, json!({"namespace": "acme", "name": "web"})).await["workspace"]
        .as_str()
        .expect("id")
        .to_owned()
}

fn apply_body(ws: &str, expect_n: u64, edits: Value, stage: bool) -> Value {
    json!({"namespace": "acme", "name": "web", "workspace": ws, "expectN": expect_n,
           "edits": edits, "stage": stage})
}

/// `ReadFiles` of `paths` (at `n`): each path's found flag, mode and whole contents, joined
/// across its messages, with whether it was redacted.
async fn read_files(
    op: &Caller,
    ws: &str,
    n: Option<u64>,
    paths: &[&str],
) -> Vec<(String, bool, u64, Vec<u8>, bool)> {
    let mut body = json!({"namespace": "acme", "name": "web", "workspace": ws, "paths": paths});
    if let Some(n) = n {
        body["n"] = json!(n);
    }
    let (messages, end) = op.stream(&format!("{WS}/ReadFiles"), &body).await;
    assert!(end.is_null(), "ReadFiles: {end}");
    let mut out: Vec<(String, bool, u64, Vec<u8>, bool)> = Vec::new();
    for message in messages {
        let path = message["path"].as_str().expect("path").to_owned();
        let offset = message["offset"]
            .as_str()
            .map_or(0, |s| s.parse().expect("offset"));
        let data = unb64(&message["data"]);
        if offset > 0 {
            let last = out.last_mut().expect("a file");
            assert_eq!(last.0, path);
            assert_eq!(last.3.len() as u64, offset);
            last.3.extend(data);
            continue;
        }
        out.push((
            path,
            message["found"].as_bool().unwrap_or(false),
            message["mode"].as_u64().unwrap_or(0),
            data,
            message["redacted"].as_bool().unwrap_or(false),
        ));
    }
    out
}

/// Edits read back, at the new snapshot and (unchanged) at the old one; a retry answers the
/// same snapshot; a big file arrives in parts; env writes are refused; the objects are
/// readable through the workspace.
#[tokio::test]
async fn apply_edits_then_read_files_round_trip() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    load_ids(&running, &dir).await;
    let op = Caller::of(&running, None);
    let ws = new_ws(&op).await;
    let big: String = (0..150_000).map(|i| format!("line {i}\n")).collect();
    let edits = json!([
        write("src/new.rs", "fn new() {}\n"),
        write("big.txt", &big),
        {"rename": {"oldPath": "README.md", "newPath": "docs/README.md"}},
        {"delete": {"path": "src/deep/x.txt"}},
        {"write": {"path": "run.sh", "content": b64("#!/bin/sh\n"), "mode": "WORKSPACE_FILE_MODE_EXECUTABLE"}},
    ]);
    let body = apply_body(&ws, 0, edits, true);
    let applied = op.ok(&format!("{WS}/ApplyEdits"), &body).await;
    let snap = &applied["snapshot"];
    assert_eq!(n_of(snap), 1);
    assert_eq!(snap["worktreeTree"], snap["indexTree"], "staged");
    let read = read_files(
        &op,
        &ws,
        None,
        &[
            "src/new.rs",
            "docs/README.md",
            "README.md",
            "src",
            "run.sh",
            "big.txt",
        ],
    )
    .await;
    assert_eq!(read[0].3, b"fn new() {}\n");
    assert_eq!(read[1].3, b"hello\n");
    assert!(!read[2].1, "README.md moved");
    assert_eq!((read[3].1, read[3].2, read[3].3.len()), (true, 0o40000, 0));
    assert_eq!(read[4].2, 0o100755);
    assert_eq!(read[5].3, big.as_bytes(), "a big file in parts");
    let old = read_files(&op, &ws, Some(0), &["README.md"]).await;
    assert_eq!(old[0].3, b"hello\n");

    // The retry answers snapshot 1; another change at 0 is fenced.
    let again = op.ok(&format!("{WS}/ApplyEdits"), &body).await;
    assert_eq!(again["snapshot"], applied["snapshot"]);
    // Env files are refused until they can be sealed.
    let env = apply_body(
        &ws,
        1,
        json!([write("env/prod.env", "TOKEN=plaintext\n")]),
        false,
    );
    assert_eq!(
        op.refused(&format!("{WS}/ApplyEdits"), &env).await,
        "env_unsealed"
    );
    // The new objects are readable through the workspace.
    let trees = op
        .ok(
            &format!("{OBJECTS}/GetTrees"),
            &json!({"namespace": "acme", "name": "web", "workspace": ws,
                    "root": snap["snapshotCommit"], "depth": 1}),
        )
        .await;
    assert_eq!(trees["trees"][0]["oid"], snap["worktreeTree"]);

    // ReadFiles takes MAX_READ_PATHS paths, and no more.
    let many: Vec<String> = (0..=MAX_READ_PATHS).map(|i| format!("nope/{i}")).collect();
    let at: Vec<&str> = many[..MAX_READ_PATHS].iter().map(String::as_str).collect();
    assert_eq!(read_files(&op, &ws, None, &at).await.len(), MAX_READ_PATHS);
    let (messages, end) = op
        .stream(
            &format!("{WS}/ReadFiles"),
            &json!({"namespace": "acme", "name": "web", "workspace": ws, "paths": many}),
        )
        .await;
    assert!(messages.is_empty());
    assert_eq!(end["code"], "invalid_argument", "{end}");
    running.shutdown().await;
}

/// `ApplyEdits` appends `expect_n + 1` only: a second change on the same snapshot is fenced,
/// and a snapshot past the latest is refused.
#[tokio::test]
async fn apply_edits_fenced_on_expect_n() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    load_ids(&running, &dir).await;
    let op = Caller::of(&running, None);
    let ws = new_ws(&op).await;
    let one = apply_body(&ws, 0, json!([write("a.txt", "one\n")]), false);
    let two = apply_body(&ws, 0, json!([write("a.txt", "two\n")]), false);
    op.ok(&format!("{WS}/ApplyEdits"), &one).await;
    assert_eq!(
        op.refused(&format!("{WS}/ApplyEdits"), &two).await,
        "workspace_fenced"
    );
    let ahead = apply_body(&ws, 5, json!([write("a.txt", "x\n")]), false);
    assert_eq!(
        op.refused(&format!("{WS}/ApplyEdits"), &ahead).await,
        "invalid_argument"
    );
    // After a newer save, the edits apply on it.
    op.ok(
        &format!("{WS}/ApplyEdits"),
        &apply_body(&ws, 1, json!([write("a.txt", "three\n")]), false),
    )
    .await;
    assert_eq!(
        op.refused(&format!("{WS}/ApplyEdits"), &one).await,
        "workspace_fenced",
        "two snapshots on, a retry is no longer recognised"
    );
    let read = read_files(&op, &ws, None, &["a.txt"]).await;
    assert_eq!(read[0].3, b"three\n");
    running.shutdown().await;
}

/// Commits `files` on `dir`'s `main` and moves the repository's `main` there; the new tip.
async fn upstream(
    running: &Running,
    ids: &(String, loams_git::NamespaceId, loams_git::RepoId),
    dir: &TempDir,
    files: &[(&str, &str)],
) -> String {
    let old = rev(dir, "main");
    for (path, text) in files {
        std::fs::write(dir.path().join(path), text).expect("write");
    }
    let paths: Vec<&str> = files.iter().map(|(p, _)| *p).collect();
    let mut args = vec!["commit", "-q", "-m", "upstream", "--"];
    args.extend(paths);
    git_env(dir.path(), &[], &args, b"");
    move_main(running, ids.1, &ids.2, dir, Some(&old)).await;
    rev(dir, "main")
}

/// Rebasing onto a tip that changed other paths takes them and keeps the workspace's; a second
/// rebase changes nothing.
#[tokio::test]
async fn rebase_disjoint_paths_is_clean() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    let ids = load_ids(&running, &dir).await;
    let op = Caller::of(&running, None);
    let ws = new_ws(&op).await;
    op.ok(
        &format!("{WS}/ApplyEdits"),
        &apply_body(&ws, 0, json!([write("src/lib.rs", "// ws\n")]), false),
    )
    .await;
    let tip = upstream(&running, &ids, &dir, &[("README.md", "upstream\n")]).await;
    let rebase = json!({"namespace": "acme", "name": "web", "workspace": ws});
    let rebased = op.ok(&format!("{WS}/Rebase"), &rebase).await;
    let snap = &rebased["snapshot"];
    assert_eq!(n_of(snap), 2);
    assert_eq!(snap["base"], tip.as_str());
    assert_eq!(snap["head"], tip.as_str());
    assert!(rebased["conflicts"].as_array().is_none_or(Vec::is_empty));
    assert_eq!(
        snap["worktreeTree"],
        tree_with(&dir, &[("src/lib.rs", "// ws\n")], &[]).as_str()
    );
    assert_eq!(
        snap["indexTree"],
        rev(&dir, "main^{tree}").as_str(),
        "nothing was staged"
    );
    let again = op.ok(&format!("{WS}/Rebase"), &rebase).await;
    assert_eq!(n_of(&again["snapshot"]), 2, "already on the tip");
    running.shutdown().await;
}

/// The same path changed on both sides becomes a conflict: markers in the worktree, the three
/// sides recorded; the workspace is then not rebased again until it is resolved.
#[tokio::test]
async fn rebase_same_path_conflict_is_recorded() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    let ids = load_ids(&running, &dir).await;
    let op = Caller::of(&running, None);
    let ws = new_ws(&op).await;
    let base_blob = rev(&dir, "main:README.md");
    let applied = op
        .ok(
            &format!("{WS}/ApplyEdits"),
            &apply_body(&ws, 0, json!([write("README.md", "ours\n")]), false),
        )
        .await;
    let ours_blob = git_env(dir.path(), &[], &["hash-object", "--stdin"], b"ours\n");
    let tip = upstream(&running, &ids, &dir, &[("README.md", "theirs\n")]).await;
    let rebase = json!({"namespace": "acme", "name": "web", "workspace": ws});
    let rebased = op.ok(&format!("{WS}/Rebase"), &rebase).await;
    assert_eq!(rebased["conflicts"], json!(["README.md"]));
    let conflict = &rebased["snapshot"]["conflicts"][0];
    assert_eq!(conflict["path"], "README.md");
    assert_eq!(conflict["base"], base_blob.as_str());
    assert_eq!(conflict["ours"], ours_blob.as_str());
    assert_eq!(conflict["theirs"], rev(&dir, "main:README.md").as_str());
    let read = read_files(&op, &ws, None, &["README.md"]).await;
    assert_eq!(
        String::from_utf8_lossy(&read[0].3),
        "<<<<<<< workspace\nours\n||||||| base\nhello\n=======\ntheirs\n>>>>>>> refs/heads/main\n"
    );
    assert_ne!(n_of(&applied["snapshot"]), n_of(&rebased["snapshot"]));
    let _ = tip;
    assert_eq!(
        op.refused(&format!("{WS}/Rebase"), &rebase).await,
        "failed_precondition"
    );
    // Staging the resolution clears the conflict.
    let n = n_of(&rebased["snapshot"]);
    let resolved = op
        .ok(
            &format!("{WS}/ApplyEdits"),
            &apply_body(&ws, n, json!([write("README.md", "both\n")]), true),
        )
        .await;
    assert!(
        resolved["snapshot"]["conflicts"]
            .as_array()
            .is_none_or(Vec::is_empty)
    );
    running.shutdown().await;
}

/// A `Diff` of `ws` against `against` (with `extra` fields), its patches joined.
async fn diff(op: &Caller, ws: &str, against: &str, extra: Value) -> Vec<u8> {
    let mut body = json!({"namespace": "acme", "name": "web", "workspace": ws,
                          "against": against});
    if let Value::Object(more) = extra {
        for (key, value) in more {
            body[key] = value;
        }
    }
    let (messages, end) = op.stream(&format!("{WS}/Diff"), &body).await;
    assert!(end.is_null(), "Diff: {end}");
    messages.iter().flat_map(|m| unb64(&m["patch"])).collect()
}

/// `Diff` streams what `git diff --no-renames --no-indent-heuristic` prints for the same trees,
/// against HEAD, the index and a commit, and for a path.
#[tokio::test]
async fn diff_matches_git_diff() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    load_ids(&running, &dir).await;
    let op = Caller::of(&running, None);
    let ws = new_ws(&op).await;
    let lines: String = (0..30).map(|i| format!("    line {i}\n")).collect();
    let edits = json!([
        write("README.md", "hello\nworld\n"),
        write("src/lib.rs", &format!("fn lib() {{\n{lines}}}\n")),
        write("new.txt", "no newline"),
        {"delete": {"path": "src/deep/x.txt"}},
    ]);
    let applied = op
        .ok(
            &format!("{WS}/ApplyEdits"),
            &apply_body(&ws, 0, edits, false),
        )
        .await;
    let tree = applied["snapshot"]["worktreeTree"]
        .as_str()
        .expect("tree")
        .to_owned();
    let want = tree_with(
        &dir,
        &[
            ("README.md", "hello\nworld\n"),
            ("src/lib.rs", &format!("fn lib() {{\n{lines}}}\n")),
            ("new.txt", "no newline"),
        ],
        &["src/deep/x.txt"],
    );
    assert_eq!(tree, want);
    let git_diff = |args: &[&str]| {
        let mut all = vec!["diff", "--no-renames", "--no-indent-heuristic"];
        all.extend_from_slice(args);
        git_env(dir.path(), &[], &all, b"") + "\n"
    };
    let by_git = git_diff(&["main", &tree]);
    for against in [
        "DIFF_AGAINST_HEAD",
        "DIFF_AGAINST_INDEX",
        "DIFF_AGAINST_BASE",
    ] {
        assert_eq!(
            String::from_utf8_lossy(&diff(&op, &ws, against, Value::Null).await),
            by_git,
            "{against}"
        );
    }
    let main = rev(&dir, "main");
    assert_eq!(
        String::from_utf8_lossy(
            &diff(&op, &ws, "DIFF_AGAINST_COMMIT", json!({"commit": main})).await
        ),
        by_git
    );
    assert_eq!(
        String::from_utf8_lossy(
            &diff(&op, &ws, "DIFF_AGAINST_HEAD", json!({"paths": ["src"]})).await
        ),
        git_diff(&["main", &tree, "--", "src"])
    );
    // Snapshot 0 against HEAD is empty.
    assert!(
        diff(&op, &ws, "DIFF_AGAINST_HEAD", json!({"n": 0}))
            .await
            .is_empty()
    );
    running.shutdown().await;
}
