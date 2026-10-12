//! `RepoService` served (GT1 Task 13, design §36 §19.10): the repository catalog and the
//! sequencers behind the main port's RPCs. `cargo test -p loams --features git --test git`.

use std::sync::Arc;
use std::time::Duration;

use loams_git::testing::ManualClock;
use loams_git::{
    BucketRefLog, Expect, IdemKey, NamespaceId, ObjectId, RefLog, RefName, RepoId, RepoPaths,
    SequencerConfig, StoreBlobStore, StoreWalStore,
};
use serde_json::{Value, json};

use super::{Running, frames, reason};

const SVC: &str = "/loams.repos.v1.RepoService";

async fn call(running: &Running, method: &str, body: Value) -> (reqwest::StatusCode, Value) {
    running.connect(&format!("{SVC}/{method}"), &body).await
}

async fn ok(running: &Running, method: &str, body: Value) -> Value {
    let (status, answer) = call(running, method, body).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{method}: {answer}");
    answer
}

/// Creates namespace `ns` and answers its id.
async fn namespace(running: &Running, ns: &str) -> NamespaceId {
    let answer = running
        .connect(
            "/loams.collection.v1.NamespaceService/CreateNamespace",
            &json!({"namespace": ns}),
        )
        .await
        .1;
    let id = &answer["namespaceId"];
    let id = id
        .as_u64()
        .or_else(|| id.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("a namespace id: {answer}"));
    NamespaceId(id)
}

async fn create(running: &Running, name: &str, key: &str) -> Value {
    ok(
        running,
        "CreateRepo",
        json!({"namespace": "acme", "name": name, "idempotencyKey": key}),
    )
    .await
}

fn oid(n: u8) -> ObjectId {
    ObjectId::try_from(&[n; 20][..]).expect("oid")
}

/// A second writer of a repository the server serves: a ref log on the server's own bucket,
/// fenced against the server's sequencer by the segment PUT.
async fn second_writer(running: &Running, ns: NamespaceId, repo_id: &str) -> BucketRefLog {
    let store = running.bucket();
    let repo = RepoId::parse(repo_id).expect("repo id");
    let paths = RepoPaths::new(ns, &repo);
    BucketRefLog::open(
        Arc::new(StoreWalStore::new(store.clone(), paths.clone())),
        Arc::new(StoreBlobStore::new(store, paths)),
        SequencerConfig::new("local/acme", ns, repo),
        Arc::new(ManualClock::new(1_790_000_000_000)),
    )
    .await
    .expect("open")
}

async fn push(log: &BucketRefLog, branch: &str, new: u8, key: &str) -> u64 {
    log.cas_ref(
        RefName::parse(&format!("refs/heads/{branch}")).expect("ref"),
        Expect::Any,
        Some(oid(new)),
        IdemKey::parse(key).expect("key"),
    )
    .await
    .expect("push")
    .seq
    .0
}

#[tokio::test]
async fn create_repo_replays_and_refuses_a_taken_name() {
    let running = Running::start().await;
    let first = create(&running, "app", "k1").await;
    assert_eq!(first["repo"]["name"], "app");
    assert_eq!(first["repo"]["defaultBranch"], "refs/heads/main");
    assert_eq!(first["repo"]["state"], "REPO_STATE_ACTIVE");
    let repo_id = first["repo"]["repoId"].as_str().expect("an id").to_owned();
    assert!(repo_id.starts_with('r'), "{first}");
    assert_eq!(first["consistencyToken"], format!("git:{repo_id}:0"));
    let again = create(&running, "app", "k1").await;
    assert_eq!(again["repo"]["repoId"], repo_id.as_str());
    let (status, error) = call(
        &running,
        "CreateRepo",
        json!({"namespace": "acme", "name": "app", "idempotencyKey": "k2"}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT, "{error}");
    assert_eq!(reason(&error), "repo_already_exists");
    let (_, error) = call(
        &running,
        "CreateRepo",
        json!({"namespace": "acme", "name": "sha", "objectFormat": "OBJECT_FORMAT_SHA256"}),
    )
    .await;
    assert_eq!(reason(&error), "object_format_unsupported", "{error}");
    let (_, error) = call(
        &running,
        "GetRepo",
        json!({"namespace": "acme", "name": "nope"}),
    )
    .await;
    assert_eq!(reason(&error), "repo_not_found", "{error}");
    running.shutdown().await;
}

#[tokio::test]
async fn delete_returns_operation_and_hides_repo() {
    let running = Running::start().await;
    let created = create(&running, "app", "k1").await;
    let repo_id = created["repo"]["repoId"].as_str().expect("id").to_owned();
    let answer = ok(
        &running,
        "DeleteRepo",
        json!({"namespace": "acme", "name": "app", "idempotencyKey": "d1"}),
    )
    .await;
    let operation = &answer["operation"];
    assert_eq!(operation["kind"], "repo.delete", "{answer}");
    assert_eq!(operation["state"], "OPERATION_STATE_RUNNING");
    assert_eq!(operation["target"]["repo_id"], repo_id.as_str());
    // A repeat answers the same operation.
    let again = ok(
        &running,
        "DeleteRepo",
        json!({"namespace": "acme", "name": "app", "idempotencyKey": "d1"}),
    )
    .await;
    assert_eq!(again["operation"]["id"], operation["id"]);
    // Hidden: deleting to GetRepo, gone from ListRepos, refused elsewhere.
    let got = ok(
        &running,
        "GetRepo",
        json!({"namespace": "acme", "name": "app"}),
    )
    .await;
    assert_eq!(got["repo"]["state"], "REPO_STATE_DELETING");
    let listed = ok(&running, "ListRepos", json!({"namespace": "acme"})).await;
    assert!(listed.get("repos").is_none(), "{listed}");
    for (method, body) in [
        ("ListRefs", json!({"namespace": "acme", "name": "app"})),
        (
            "CreateRepo",
            json!({"namespace": "acme", "name": "app", "idempotencyKey": "k9"}),
        ),
    ] {
        let (_, error) = call(&running, method, body).await;
        assert_eq!(reason(&error), "repo_deleting", "{method}: {error}");
    }
    running.shutdown().await;
}

#[tokio::test]
async fn update_config_sets_head_and_protections() {
    let running = Running::start().await;
    create(&running, "app", "k1").await;
    let body = json!({
        "namespace": "acme",
        "name": "app",
        "defaultBranch": "refs/heads/trunk",
        "protections": [{"pattern": "refs/heads/trunk", "denyDelete": true, "denyForce": true}],
        "updateMask": "defaultBranch,protections",
        "idempotencyKey": "cfg-1",
    });
    let answer = ok(&running, "UpdateRepoConfig", body.clone()).await;
    assert_eq!(
        answer["repo"]["defaultBranch"], "refs/heads/trunk",
        "{answer}"
    );
    assert_eq!(
        answer["repo"]["protections"][0]["pattern"],
        "refs/heads/trunk"
    );
    let token = answer["consistencyToken"].clone();
    // A retry replays: the same seq.
    let again = ok(&running, "UpdateRepoConfig", body).await;
    assert_eq!(again["consistencyToken"], token);
    let got = ok(
        &running,
        "GetRepo",
        json!({"namespace": "acme", "name": "app", "consistencyToken": token}),
    )
    .await;
    assert_eq!(got["repo"]["defaultBranch"], "refs/heads/trunk");
    assert_eq!(got["repo"]["protections"][0]["denyForce"], true);
    // An empty mask, or a field the RPC does not change, is refused.
    for mask in ["", "name"] {
        let (_, error) = call(
            &running,
            "UpdateRepoConfig",
            json!({"namespace": "acme", "name": "app", "updateMask": mask}),
        )
        .await;
        assert_eq!(reason(&error), "invalid_argument", "{mask:?}: {error}");
    }
    // The same key with another change is refused.
    let (_, error) = call(
        &running,
        "UpdateRepoConfig",
        json!({"namespace": "acme", "name": "app", "defaultBranch": "refs/heads/dev",
               "updateMask": "defaultBranch", "idempotencyKey": "cfg-1"}),
    )
    .await;
    assert_eq!(reason(&error), "idempotency_key_reused", "{error}");
    running.shutdown().await;
}

/// A ref written by another writer of the bucket is seen by a read given that write's token,
/// and every page of one listing reads the same seq.
#[tokio::test]
async fn list_refs_honours_consistency_token() {
    let running = Running::start().await;
    let ns = namespace(&running, "acme").await;
    let created = create(&running, "app", "k1").await;
    let repo_id = created["repo"]["repoId"].as_str().expect("id").to_owned();
    // The server's sequencer is open (and its state at seq 0) before the other writer pushes.
    ok(
        &running,
        "ListRefs",
        json!({"namespace": "acme", "name": "app"}),
    )
    .await;
    let writer = second_writer(&running, ns, &repo_id).await;
    push(&writer, "a", 1, "p1").await;
    push(&writer, "b", 2, "p2").await;
    let seq = push(&writer, "c", 3, "p3").await;
    let token = format!("git:{repo_id}:{seq}");
    let first = ok(
        &running,
        "ListRefs",
        json!({"namespace": "acme", "name": "app", "consistencyToken": token,
               "prefix": "refs/heads/", "pageSize": 2}),
    )
    .await;
    let names = |page: &Value| -> Vec<String> {
        page["refs"]
            .as_array()
            .map(|refs| {
                refs.iter()
                    .map(|r| r["name"].as_str().unwrap_or_default().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    assert_eq!(names(&first), ["refs/heads/a", "refs/heads/b"], "{first}");
    assert_eq!(first["consistencyToken"], token.as_str());
    // A push after the first page is not in the second.
    push(&writer, "d", 4, "p4").await;
    let page_token = first["nextPageToken"]
        .as_str()
        .expect("a next page")
        .to_owned();
    let second = ok(
        &running,
        "ListRefs",
        json!({"namespace": "acme", "name": "app", "prefix": "refs/heads/",
               "pageSize": 2, "pageToken": page_token}),
    )
    .await;
    assert_eq!(names(&second), ["refs/heads/c"], "{second}");
    assert!(second.get("nextPageToken").is_none(), "{second}");
    // Without a token the read is the latest; HEAD comes first.
    let latest = ok(
        &running,
        "ListRefs",
        json!({"namespace": "acme", "name": "app"}),
    )
    .await;
    assert_eq!(
        names(&latest),
        [
            "HEAD",
            "refs/heads/a",
            "refs/heads/b",
            "refs/heads/c",
            "refs/heads/d"
        ]
    );
    assert_eq!(latest["refs"][0]["symrefTarget"], "refs/heads/main");
    // A token of another repository is refused.
    let (_, error) = call(
        &running,
        "ListRefs",
        json!({"namespace": "acme", "name": "app", "consistencyToken": "git:r01jzzzzzzzzzzzzzzzzzzzzzz:1"}),
    )
    .await;
    assert_eq!(reason(&error), "invalid_argument", "{error}");
    writer.shutdown().await;
    running.shutdown().await;
}

/// Reads a server stream's messages until `done` holds for them or `limit` passes.
async fn watch_until(
    running: &Running,
    body: &Value,
    limit: Duration,
    done: impl Fn(&[Value]) -> bool,
) -> Vec<Value> {
    let payload = body.to_string().into_bytes();
    let mut framed = vec![0u8];
    framed.extend_from_slice(&u32::try_from(payload.len()).expect("small").to_be_bytes());
    framed.extend_from_slice(&payload);
    let mut response = reqwest::Client::new()
        .post(format!("{}{SVC}/WatchRepo", running.base))
        .header("content-type", "application/connect+json")
        .body(framed)
        .send()
        .await
        .expect("watch");
    let mut buffer = Vec::new();
    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + limit;
    while !done(&messages) {
        let chunk = tokio::time::timeout_at(deadline, response.chunk())
            .await
            .expect("the stream answers in time")
            .expect("a chunk");
        let Some(chunk) = chunk else { break };
        buffer.extend_from_slice(&chunk);
        let parsed = frames(&buffer);
        let used: usize = parsed.iter().map(|(_, data)| data.len() + 5).sum();
        for (flags, data) in &parsed {
            let value: Value = serde_json::from_slice(data).unwrap_or(Value::Null);
            assert_eq!(flags & 0x02, 0, "the stream ended: {value}");
            messages.push(value);
        }
        buffer.drain(..used);
    }
    messages
}

#[tokio::test]
async fn watch_repo_snapshot_then_changes_then_heartbeat() {
    let running = Running::start_with(|config| {
        config.git.watch_heartbeat = Duration::from_millis(300);
    })
    .await;
    let ns = namespace(&running, "acme").await;
    let created = create(&running, "app", "k1").await;
    let repo_id = created["repo"]["repoId"].as_str().expect("id").to_owned();
    let writer = second_writer(&running, ns, &repo_id).await;
    let body = json!({"namespace": "acme", "name": "app"});
    let watching = watch_until(&running, &body, Duration::from_secs(20), |messages| {
        messages.iter().any(|m| m.get("transaction").is_some())
            && messages
                .iter()
                .skip_while(|m| m.get("transaction").is_none())
                .any(|m| m.get("heartbeat").is_some())
    });
    let pushing = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        push(&writer, "main", 7, "p1").await
    };
    let (messages, seq) = tokio::join!(watching, pushing);
    let first = &messages[0];
    assert!(first.get("snapshot").is_some(), "{first}");
    assert_eq!(first["snapshot"]["repo"]["name"], "app");
    let txn = messages
        .iter()
        .find(|m| m.get("transaction").is_some())
        .expect("a transaction");
    assert_eq!(
        txn["transaction"]["seq"].to_string().trim_matches('"'),
        seq.to_string()
    );
    assert_eq!(
        txn["transaction"]["refUpdates"]["updates"][0]["name"],
        "refs/heads/main"
    );
    assert_eq!(txn["consistencyToken"], format!("git:{repo_id}:{seq}"));
    // Resuming from the token sends no snapshot: the next message is a change or a heartbeat.
    let resumed = watch_until(
        &running,
        &json!({"namespace": "acme", "name": "app", "consistencyToken": format!("git:{repo_id}:{seq}")}),
        Duration::from_secs(10),
        |messages| !messages.is_empty(),
    )
    .await;
    assert!(resumed[0].get("heartbeat").is_some(), "{:?}", resumed[0]);
    writer.shutdown().await;
    running.shutdown().await;
}

#[tokio::test]
async fn list_repos_paginates() {
    let running = Running::start().await;
    for i in 0..5 {
        create(&running, &format!("repo-{i}"), &format!("k{i}")).await;
    }
    let mut seen = Vec::new();
    let mut token = String::new();
    let mut pages = 0;
    loop {
        let page = ok(
            &running,
            "ListRepos",
            json!({"namespace": "acme", "pageSize": 2, "pageToken": token}),
        )
        .await;
        pages += 1;
        let repos = page["repos"].as_array().expect("repos");
        assert!(repos.len() <= 2, "{page}");
        seen.extend(
            repos
                .iter()
                .map(|r| r["name"].as_str().expect("name").to_owned()),
        );
        match page.get("nextPageToken").and_then(Value::as_str) {
            Some(next) => token = next.to_owned(),
            None => break,
        }
    }
    assert_eq!(seen, ["repo-0", "repo-1", "repo-2", "repo-3", "repo-4"]);
    assert_eq!(pages, 3);
    // A namespace with no repository lists none.
    let empty = ok(&running, "ListRepos", json!({"namespace": "nobody"})).await;
    assert!(empty.get("repos").is_none(), "{empty}");
    running.shutdown().await;
}
