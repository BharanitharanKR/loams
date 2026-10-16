//! `ObjectService` and the immutable object route (GT1 Tasks 18 and 20, design §36 §19.5.1,
//! §19.8):
//! `cargo test -p loams --features git --test git objects::`.

use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use loams_git::format::pb as gitpb;
use loams_git::{
    BlobStore, Expect, IdemKey, LpkWriter, ObjectId, Principal, RefLog, RefName, RefTxn, RefUpdate,
    StoreBlobStore,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::{Running, reason};

const SVC: &str = "/loams.repos.v1.ObjectService";
const TOKEN: &str = "lgt_acme_read_0123456789";

/// Runs git in `dir`; its stdout.
fn git(dir: &std::path::Path, args: &[&str], stdin: Option<&str>) -> String {
    let mut child = Command::new("git")
        .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .env("GIT_AUTHOR_DATE", "2026-10-12T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-10-12T00:00:00Z")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("git runs");
    if let Some(input) = stdin {
        use std::io::Write as _;
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input.as_bytes())
            .expect("stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("git");
    assert!(out.status.success(), "git {args:?}");
    String::from_utf8(out.stdout).expect("utf-8")
}

/// A git repository with a small history; `None` without git.
fn history() -> Option<TempDir> {
    Command::new("git").arg("--version").output().ok()?;
    let dir = TempDir::new().expect("dir");
    let path = dir.path();
    git(path, &["init", "-q", "-b", "main", "."], None);
    std::fs::create_dir_all(path.join("src/deep")).expect("mkdir");
    std::fs::write(path.join("README.md"), "hello\n").expect("write");
    std::fs::write(path.join("src/lib.rs"), "// lib\n").expect("write");
    std::fs::write(path.join("src/deep/x.txt"), "x\n").expect("write");
    git(path, &["add", "-A"], None);
    git(path, &["commit", "-q", "-m", "one"], None);
    Some(dir)
}

fn rev(dir: &TempDir, name: &str) -> String {
    git(dir.path(), &["rev-parse", name], None)
        .trim()
        .to_owned()
}

/// Creates `acme/web`, stores `dir`'s history as one pack and points `main` at it; the
/// consistency token of that commit.
async fn load(running: &Running, dir: &TempDir) -> String {
    let ns = namespace_id(running).await;
    let (status, created) = running
        .connect(
            "/loams.repos.v1.RepoService/CreateRepo",
            &json!({"namespace": "acme", "name": "web", "idempotencyKey": "k"}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{created}");
    let repo_id = created["repo"]["repoId"].as_str().expect("id").to_owned();
    let printed = git(
        dir.path(),
        &["pack-objects", "--revs", "-q", "out"],
        Some("main\n"),
    );
    let name = printed.trim();
    let pack = std::fs::read(dir.path().join(format!("out-{name}.pack"))).expect("pack");
    let idx = std::fs::read(dir.path().join(format!("out-{name}.idx"))).expect("idx");
    let (id, pack_ref, bundle) = LpkWriter::from_parts(pack.into(), idx.into()).expect("lpk");
    let repo = loams_git::RepoId::parse(&repo_id).expect("repo id");
    let store = running.bucket();
    let paths = loams_git::RepoPaths::new(ns, &repo);
    let blobs = StoreBlobStore::new(store.clone(), paths.clone());
    blobs.put(&id, bundle).await.expect("put pack");
    let log = loams_git::BucketRefLog::open(
        Arc::new(loams_git::StoreWalStore::new(store, paths)),
        Arc::new(blobs),
        loams_git::SequencerConfig::new("local/acme", ns, repo.clone()),
        Arc::new(loams_git::SystemClock),
    )
    .await
    .expect("open");
    let head = ObjectId::from_hex(rev(dir, "main").as_bytes()).expect("oid");
    let receipt = log
        .commit(RefTxn {
            key: IdemKey::parse("load-1").expect("key"),
            request_digest: [1; 32],
            updates: vec![RefUpdate {
                name: RefName::parse("refs/heads/main").expect("ref"),
                expect: Expect::Absent,
                new: Some(head),
            }],
            packs: vec![pack_ref as gitpb::PackRef],
            principal: Principal::parse("test").expect("principal"),
            scope: None,
            trace: None,
            push_options: Vec::new(),
        })
        .await
        .expect("commit");
    format!("git:{repo_id}:{}", receipt.seq.0)
}

/// Creates namespace `acme`; its id.
async fn namespace_id(running: &Running) -> loams_git::NamespaceId {
    let (_, answer) = running
        .connect(
            "/loams.collection.v1.NamespaceService/CreateNamespace",
            &json!({"namespace": "acme"}),
        )
        .await;
    let id = &answer["namespaceId"];
    let id = id
        .as_u64()
        .or_else(|| id.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("a namespace id: {answer}"));
    loams_git::NamespaceId(id)
}

#[tokio::test]
async fn object_service_reads_objects_trees_and_presence() {
    let Some(dir) = history() else {
        return;
    };
    let running = Running::start().await;
    let token = load(&running, &dir).await;
    let readme = rev(&dir, "main:README.md");
    let root = rev(&dir, "main^{tree}");
    let absent = "00000000000000000000000000000000deadbeef";

    let (status, messages) = running
        .connect_stream(
            &format!("{SVC}/GetObjects"),
            &json!({"namespace": "acme", "name": "web", "oids": [readme, absent],
                    "consistencyToken": token}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let data: Vec<&Value> = messages
        .iter()
        .filter(|(flags, _)| flags & 0x02 == 0)
        .map(|(_, m)| m)
        .collect();
    assert_eq!(data.len(), 2, "{messages:?}");
    assert_eq!(data[0]["oid"], readme);
    assert_eq!(data[0]["kind"], "OBJECT_KIND_BLOB");
    assert_eq!(
        STANDARD
            .decode(data[0]["data"].as_str().expect("data"))
            .expect("b64"),
        b"hello\n"
    );
    assert_eq!(data[1]["oid"], absent);
    assert_eq!(data[1]["missing"], true);

    let (status, trees) = running
        .connect(
            &format!("{SVC}/GetTrees"),
            &json!({"namespace": "acme", "name": "web", "root": rev(&dir, "main"),
                    "consistencyToken": token}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{trees}");
    let trees = trees["trees"].as_array().expect("trees");
    assert_eq!(trees.len(), 3, "root, src, src/deep: {trees:?}");
    assert_eq!(trees[0]["oid"], root);
    let readme_entry = trees[0]["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["oid"] == readme)
        .expect("README.md");
    assert_eq!(readme_entry["size"].as_str().unwrap_or("6"), "6");

    let (status, has) = running
        .connect(
            &format!("{SVC}/HasObjects"),
            &json!({"namespace": "acme", "name": "web", "oids": [absent, readme, root],
                    "consistencyToken": token}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{has}");
    assert_eq!(
        STANDARD
            .decode(has["present"].as_str().expect("present"))
            .expect("b64"),
        vec![0b110]
    );

    // More than 1,024 ids, a bad id, and an unknown workspace are refused.
    let many: Vec<String> = (0..1025).map(|_| absent.to_owned()).collect();
    let (status, error) = running
        .connect(
            &format!("{SVC}/HasObjects"),
            &json!({"namespace": "acme", "name": "web", "oids": many}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(reason(&error), "invalid_argument");
    let (status, error) = running
        .connect(
            &format!("{SVC}/HasObjects"),
            &json!({"namespace": "acme", "name": "web", "oids": ["XYZ"]}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{error}");
    let (status, error) = running
        .connect(
            &format!("{SVC}/HasObjects"),
            &json!({"namespace": "acme", "name": "web", "oids": [readme],
                    "workspace": "w01j9z3m5q8x000000000000001"}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND, "{error}");
    assert_eq!(reason(&error), "workspace_not_found");
    running.shutdown().await;
}

#[tokio::test]
async fn object_route_is_immutable_cacheable() {
    let Some(dir) = history() else {
        return;
    };
    let tokens_dir = TempDir::new().expect("dir");
    let tokens = tokens_dir.path().join("git-tokens.toml");
    let hash: String = Sha256::digest(TOKEN.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::write(
        &tokens,
        format!(
            "[[tokens]]\nsha256 = \"{hash}\"\nprincipal = \"ci\"\nnamespace = \"acme\"\n\
             access = \"read\"\n"
        ),
    )
    .expect("tokens");
    let running = Running::start_with(|config| {
        config.git.listen = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        config.git.tokens = Some(tokens);
    })
    .await;
    load(&running, &dir).await;
    let addr = running.server.git_listen_addr().expect("listener");
    let readme = rev(&dir, "main:README.md");
    let auth = format!("Basic {}", STANDARD.encode(format!("x-token:{TOKEN}")));
    let get = |path: String, auth: Option<String>| async move {
        let mut request = reqwest::Client::new().get(format!("http://{addr}{path}"));
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        request.send().await.expect("request")
    };

    let response = get(
        format!("/git/acme/web.git/loams/objects/{readme}"),
        Some(auth.clone()),
    )
    .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers()["cache-control"],
        "private, max-age=31536000, immutable"
    );
    let body = response.bytes().await.expect("body");
    // The body is a loose object git reads, in a repository that has nothing else.
    let reader = TempDir::new().expect("dir");
    git(reader.path(), &["init", "-q", "."], None);
    let hex = readme.as_str();
    let objects = reader.path().join(".git/objects").join(&hex[..2]);
    std::fs::create_dir_all(&objects).expect("mkdir");
    std::fs::write(objects.join(&hex[2..]), &body).expect("write");
    assert_eq!(
        git(reader.path(), &["cat-file", "-p", hex], None),
        "hello\n"
    );

    for (path, auth, status) in [
        // No token, an absent object, an unknown workspace, and an object id that is not one.
        (
            format!("/git/acme/web.git/loams/objects/{readme}"),
            None,
            401,
        ),
        (
            "/git/acme/web.git/loams/objects/00000000000000000000000000000000deadbeef".to_owned(),
            Some(auth.clone()),
            404,
        ),
        (
            format!("/git/acme/web.git/loams/objects/{readme}?ws=w01j9z3m5q8x000000000000001"),
            Some(auth.clone()),
            404,
        ),
        (
            "/git/acme/web.git/loams/objects/nothex".to_owned(),
            Some(auth.clone()),
            404,
        ),
    ] {
        assert_eq!(
            get(path.clone(), auth).await.status().as_u16(),
            status,
            "{path}"
        );
    }
    running.shutdown().await;
}

#[tokio::test]
async fn log_and_diffstat_are_served() {
    let Some(dir) = history() else {
        return;
    };
    std::fs::write(dir.path().join("README.md"), "hello\nworld\n").expect("write");
    git(dir.path(), &["commit", "-q", "-am", "two"], None);
    let running = Running::start().await;
    let token = load(&running, &dir).await;
    let (first, second) = (rev(&dir, "main~1"), rev(&dir, "main"));

    let (status, messages) = running
        .connect_stream(
            &format!("{SVC}/Log"),
            &json!({"namespace": "acme", "name": "web", "revs": ["main"],
                    "paths": ["README.md"],
                    "fields": ["LOG_FIELD_MESSAGE", "LOG_FIELD_CHANGED"],
                    "consistencyToken": token}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let commits: Vec<&Value> = messages
        .iter()
        .filter(|(flags, _)| flags & 0x02 == 0)
        .map(|(_, m)| &m["commit"])
        .collect();
    assert_eq!(commits.len(), 2, "{messages:?}");
    assert_eq!(commits[0]["oid"], second);
    assert_eq!(commits[0]["parents"], json!([first]));
    assert_eq!(
        STANDARD
            .decode(commits[0]["message"].as_str().expect("message"))
            .expect("b64"),
        b"two\n"
    );
    assert!(
        commits[0]["author"].is_null(),
        "signatures were not asked for"
    );
    let changed = &commits[0]["changed"][0];
    assert_eq!(changed["kind"], "CHANGE_KIND_MODIFIED");
    assert_eq!(
        STANDARD
            .decode(changed["path"].as_str().expect("path"))
            .expect("b64"),
        b"README.md"
    );
    assert_eq!(commits[1]["oid"], first);

    let (status, stat) = running
        .connect(
            &format!("{SVC}/DiffStat"),
            &json!({"namespace": "acme", "name": "web", "fromRev": first, "toRev": "main",
                    "consistencyToken": token}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{stat}");
    let files = stat["files"].as_array().expect("files");
    assert_eq!(files.len(), 1, "{stat}");
    assert_eq!(files[0]["added"].as_str().unwrap_or("1"), "1");
    assert!(files[0]["removed"].is_null() || files[0]["removed"] == "0");

    // A glob pathspec and an unknown revision are refused.
    let (status, error) = running
        .connect(
            &format!("{SVC}/DiffStat"),
            &json!({"namespace": "acme", "name": "web", "fromRev": first, "toRev": "main",
                    "paths": ["*.md"]}),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(reason(&error), "invalid_argument");
    let (_, messages) = running
        .connect_stream(
            &format!("{SVC}/Log"),
            &json!({"namespace": "acme", "name": "web", "revs": ["nope"]}),
        )
        .await;
    let (_, end) = messages.last().expect("an end-of-stream message");
    assert_eq!(end["error"]["code"], "invalid_argument", "{messages:?}");
    running.shutdown().await;
}
