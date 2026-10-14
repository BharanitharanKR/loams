//! The loopback Connect listener in front of a real, in-process engine with
//! the Mock harness (plan SD1 Task 5): the §50 §7 rules, and unary and
//! streaming calls that answer what the ndjson wire answers.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use connectrpc::ErrorCode;
use connectrpc::client::{ClientConfig, HttpClient};
use loams_agentd_connect::map::Wire;
use loams_agentd_connect::proto::loams::agentd::v1 as pb;
use loams_agentd_connect::{Agentd, ConnectListener, HostInfo, LoopbackOptions};
use loams_agentd_proto as rs;
use loams_agentd_proto::daemon::DaemonMode;
use loams_agentd_rpc::{RpcClient, RpcService, Secret, memory_client, methods};
use loams_agentd_sessions::{Engine, EngineConfig, EngineProfile, EngineRuntime, HarnessId};

const TOKEN: &str = "loopback-test-token";

struct Daemon {
    _dir: tempfile::TempDir,
    runtime: EngineRuntime,
    listener: ConnectListener,
    ndjson: RpcClient,
}

impl Daemon {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().to_path_buf(),
            default_harness: HarnessId::Mock,
            terminal_shell: None,
            tool_outputs: Default::default(),
            mcp_endpoint: None,
        };
        let runtime = Engine::assemble_runtime(&config, EngineProfile::local(dir.path()).unwrap())
            .await
            .unwrap();
        let engine: Arc<dyn RpcService> = runtime.core().rpc_service();
        let agentd = Agentd::new(
            engine.clone(),
            HostInfo {
                protocol: 1,
                version: "0.0.0-test".into(),
                build_sha: "test".into(),
                mode: DaemonMode::Child,
                pid: std::process::id(),
                started_at_ms: 1,
                features: vec!["studio.v1".into()],
            },
        );
        let listener = ConnectListener::bind(
            agentd,
            LoopbackOptions {
                token: Secret::new(TOKEN),
                auth_failure_delay: Duration::from_millis(10),
            },
        )
        .await
        .unwrap();
        Self {
            _dir: dir,
            ndjson: memory_client(engine),
            runtime,
            listener,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.listener.port())
    }

    fn config(&self) -> ClientConfig {
        ClientConfig::new(self.url("").parse().unwrap())
            .with_default_header("authorization", format!("Bearer {TOKEN}"))
    }

    async fn stop(self) {
        self.listener.stop().await;
        self.runtime.shutdown().await;
    }
}

/// The next stream message, owned, within 5 s.
macro_rules! next {
    ($stream:expr) => {
        tokio::time::timeout(Duration::from_secs(5), $stream.message())
            .await
            .expect("a stream message within 5 s")
            .unwrap()
            .expect("the stream is open")
            .to_owned_message()
    };
}

const LIST_HARNESSES: &str = "/loams.agentd.v1.HarnessService/ListHarnesses";

/// A raw Connect unary POST with JSON, so headers are fully under test.
async fn post(url: &str, headers: &[(&str, &str)]) -> (u16, serde_json::Value) {
    let mut request = reqwest::Client::new()
        .post(url)
        .header("content-type", "application/json")
        .body("{}");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

#[tokio::test]
async fn connect_without_token_is_401() {
    let daemon = Daemon::start().await;
    let url = daemon.url(LIST_HARNESSES);
    let (status, body) = post(&url, &[]).await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["code"], "unauthenticated");
    let (status, _) = post(&url, &[("authorization", "Bearer wrong-token")]).await;
    assert_eq!(status, 401);
    let (status, _) = post(&url, &[("authorization", TOKEN)]).await;
    assert_eq!(status, 401, "a token without the Bearer scheme");
    let bearer = format!("Bearer {TOKEN}");
    let (status, body) = post(&url, &[("authorization", &bearer)]).await;
    assert_eq!(status, 200, "{body}");
    daemon.stop().await;
}

#[tokio::test]
async fn connect_origin_is_403() {
    let daemon = Daemon::start().await;
    let bearer = format!("Bearer {TOKEN}");
    for origin in ["https://evil.example", "null", "loams-app://console"] {
        let (status, body) = post(
            &daemon.url(LIST_HARNESSES),
            &[("authorization", &bearer), ("origin", origin)],
        )
        .await;
        assert_eq!(status, 403, "{origin}: {body}");
        assert_eq!(body["code"], "permission_denied");
    }
    daemon.stop().await;
}

#[tokio::test]
async fn connect_foreign_host_is_403() {
    let daemon = Daemon::start().await;
    let bearer = format!("Bearer {TOKEN}");
    let port = daemon.listener.port();
    for host in [
        format!("localhost:{port}"),
        "127.0.0.1".to_owned(),
        format!("127.0.0.1:{}", port.wrapping_add(1)),
        "evil.example".to_owned(),
    ] {
        let (status, _) = post(
            &daemon.url(LIST_HARNESSES),
            &[("authorization", &bearer), ("host", &host)],
        )
        .await;
        assert_eq!(status, 403, "Host {host}");
    }
    daemon.stop().await;
}

#[tokio::test]
async fn connect_unary_and_stream_work() {
    let daemon = Daemon::start().await;

    // Unary: ListHarnesses answers what the ndjson wire answers.
    let harnesses = pb::HarnessServiceClient::new(HttpClient::plaintext(), daemon.config())
        .list_harnesses(pb::ListHarnessesRequest::default())
        .await
        .unwrap()
        .into_owned();
    let ndjson: rs::HarnessCatalog = daemon
        .ndjson
        .call_as(methods::LIST_HARNESSES, serde_json::json!({}))
        .await
        .unwrap();
    assert!(!harnesses.harnesses.is_empty());
    assert_eq!(harnesses.harnesses, ndjson.0.to_pb());

    // Stream: WatchThreads sends the list, then the list again on a change.
    let threads = pb::ThreadServiceClient::new(HttpClient::plaintext(), daemon.config());
    let mut stream = threads
        .watch_threads(pb::WatchThreadsRequest::default())
        .await
        .unwrap();
    let first = next!(stream);
    assert!(first.chats.is_empty(), "{first:?}");
    let host = pb::StudioServiceClient::new(HttpClient::plaintext(), daemon.config())
        .get_host(pb::GetHostRequest::default())
        .await
        .unwrap()
        .into_owned();
    let device_id = host.host.as_option().unwrap().device_id.clone();
    let created = threads
        .create_thread(pb::CreateThreadRequest {
            chat_id: "chat-connect-1".into(),
            device_id: Some(device_id),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert!(created.ok);
    let mut seen = next!(stream);
    for _ in 0..10 {
        if !seen.chats.is_empty() {
            break;
        }
        seen = next!(stream);
    }
    assert_eq!(seen.chats.len(), 1);
    assert_eq!(seen.chats[0].id, "chat-connect-1");

    // The same frame over ndjson maps to the same message.
    let mut ndjson = daemon
        .ndjson
        .subscribe(methods::WATCH_CHATS, serde_json::json!({}))
        .await
        .unwrap();
    let item = tokio::time::timeout(Duration::from_secs(5), ndjson.recv())
        .await
        .unwrap()
        .unwrap();
    let chats: rs::ChatList = serde_json::from_value(item).unwrap();
    assert_eq!(chats.0.to_pb(), seen.chats);
    daemon.stop().await;
}

#[tokio::test]
async fn gaps_and_bad_requests_answer_connect_errors() {
    let daemon = Daemon::start().await;
    let threads = pb::ThreadServiceClient::new(HttpClient::plaintext(), daemon.config());
    let error = threads
        .list_checkpoints(pb::ListCheckpointsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Unimplemented);
    assert!(!error.details.is_empty(), "the ErrorInfo detail");

    let harness = pb::HarnessServiceClient::new(HttpClient::plaintext(), daemon.config());
    let error = harness
        .list_models(pb::ListModelsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument, "an unset harness");

    let host = pb::StudioServiceClient::new(HttpClient::plaintext(), daemon.config())
        .get_host(pb::GetHostRequest::default())
        .await
        .unwrap()
        .into_owned();
    let host = host.host.as_option().unwrap();
    assert!(host.features.iter().any(|feature| feature == "studio.v1"));
    assert!(!host.device_id.is_empty());
    daemon.stop().await;
}

fn git(cwd: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// SD1 Task 9: the git writes are served over Connect and answer what the
/// ndjson wire answers, their refusals with the Shared contracts' codes.
#[tokio::test]
async fn git_writes_over_connect() {
    let daemon = Daemon::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.name", "Loams Test"]);
    git(&repo, &["config", "user.email", "test@loams.invalid"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("notes.txt"), "first\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "initial"]);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    std::fs::write(repo.join("notes.txt"), "feature\n").unwrap();
    git(&repo, &["commit", "-q", "-am", "feature"]);
    let feature = git(&repo, &["rev-parse", "HEAD"]);
    git(&repo, &["checkout", "-q", "main"]);
    std::fs::write(repo.join("notes.txt"), "main\n").unwrap();
    git(&repo, &["commit", "-q", "-am", "main"]);
    let repo_path = repo.canonicalize().unwrap().to_string_lossy().into_owned();

    let git_service = pb::GitServiceClient::new(HttpClient::plaintext(), daemon.config());
    // A repository the daemon doesn't know is refused.
    let error = git_service
        .list_worktrees(pb::ListWorktreesRequest {
            repo_path: repo_path.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);

    daemon
        .ndjson
        .call(methods::ADD_REPO, serde_json::json!({ "path": repo_path }))
        .await
        .unwrap();
    let listed = git_service
        .list_worktrees(pb::ListWorktreesRequest {
            repo_path: repo_path.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let ndjson: rs::WorktreeList = daemon
        .ndjson
        .call_as(
            methods::LIST_WORKTREES,
            serde_json::json!({ "repoPath": repo_path }),
        )
        .await
        .unwrap();
    assert_eq!(listed.worktrees, ndjson.0.to_pb());
    assert_eq!(listed.worktrees.len(), 1);
    assert_eq!(listed.worktrees[0].branch, "main");

    // A stale checksum is failed_precondition (`stale_diff`).
    std::fs::write(repo.join("notes.txt"), "edited\n").unwrap();
    let diff = pb::DiffServiceClient::new(HttpClient::plaintext(), daemon.config())
        .get_checkout_diff(pb::GetCheckoutDiffRequest {
            cwd: repo_path.clone(),
            mode: "working".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let diff = diff.diff.as_option().unwrap().clone();
    let error = git_service
        .commit_worktree(pb::CommitWorktreeRequest {
            checkout_id: diff.checkout_id.clone(),
            cwd: repo_path.clone(),
            message: "edit".into(),
            all: true,
            expected_checksum: "stale".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition);
    assert!(
        error
            .message
            .as_deref()
            .unwrap_or_default()
            .starts_with("stale_diff")
    );
    let committed = git_service
        .commit_worktree(pb::CommitWorktreeRequest {
            checkout_id: diff.checkout_id,
            cwd: repo_path.clone(),
            message: "edit".into(),
            all: true,
            expected_checksum: diff.checksum,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(committed.sha, git(&repo, &["rev-parse", "HEAD"]));
    assert_eq!(committed.summary, "edit");

    // A conflict is `aborted` (`merge_conflict`), with a clean tree.
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let error = git_service
        .merge_branch(pb::MergeBranchRequest {
            repo_path: repo_path.clone(),
            source: "feature".into(),
            target: "main".into(),
            strategy: pb::MergeStrategy::MERGE_STRATEGY_MERGE.into(),
            expected_source_head: feature,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Aborted);
    assert_eq!(
        error.message.as_deref(),
        Some("merge_conflict: [\"notes.txt\"]")
    );
    assert_eq!(error.details.len(), 1, "the ErrorInfo with its paths");
    assert_eq!(git(&repo, &["rev-parse", "HEAD"]), head);
    assert_eq!(git(&repo, &["status", "--porcelain"]), "");

    // An unset strategy is bad_params.
    let error = git_service
        .merge_branch(pb::MergeBranchRequest {
            repo_path: repo_path.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    daemon.stop().await;
}

/// SD1 Task 11: review comments and content search are served over Connect
/// and answer what the ndjson wire answers.
#[tokio::test]
async fn review_and_search_over_connect() {
    let daemon = Daemon::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.name", "Loams Test"]);
    git(&repo, &["config", "user.email", "test@loams.invalid"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("notes.txt"), "first\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "initial"]);
    std::fs::write(repo.join("notes.txt"), "first\nsecond needle\n").unwrap();
    let repo_path = repo.canonicalize().unwrap().to_string_lossy().into_owned();
    daemon
        .ndjson
        .call(methods::ADD_REPO, serde_json::json!({ "path": repo_path }))
        .await
        .unwrap();
    let diff = pb::DiffServiceClient::new(HttpClient::plaintext(), daemon.config())
        .get_checkout_diff(pb::GetCheckoutDiffRequest {
            cwd: repo_path.clone(),
            mode: "working".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let diff = diff.diff.as_option().unwrap().clone();

    let review = pb::ReviewServiceClient::new(HttpClient::plaintext(), daemon.config());
    let comment = |side: pb::DiffSide| pb::UpsertReviewCommentRequest {
        comment: buffa::MessageField::some(pb::ReviewComment {
            checkout_id: diff.checkout_id.clone(),
            path: "notes.txt".into(),
            side: side.into(),
            line: 2,
            body: "Why?".into(),
            diff_checksum: diff.checksum.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };
    // An unset side is bad_params.
    let error = review
        .upsert_review_comment(comment(pb::DiffSide::DIFF_SIDE_UNSPECIFIED))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    let created = review
        .upsert_review_comment(comment(pb::DiffSide::DIFF_SIDE_NEW))
        .await
        .unwrap()
        .into_owned();
    assert!(!created.id.is_empty());

    let mut stream = review
        .watch_review_comments(pb::WatchReviewCommentsRequest {
            checkout_id: diff.checkout_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let first = next!(stream);
    let mut ndjson = daemon
        .ndjson
        .subscribe(
            methods::WATCH_REVIEW_COMMENTS,
            serde_json::json!({ "checkoutId": diff.checkout_id }),
        )
        .await
        .unwrap();
    let item = tokio::time::timeout(Duration::from_secs(5), ndjson.recv())
        .await
        .unwrap()
        .unwrap();
    let frame: rs::ReviewCommentsFrame = serde_json::from_value(item).unwrap();
    assert_eq!(first, frame.to_pb());
    let rs::ReviewCommentsFrame::Reset { reset } = frame else {
        panic!("a reset first");
    };
    assert_eq!(reset.len(), 1);
    assert_eq!(reset[0].id, created.id);

    // A missing comment is not_found.
    let error = review
        .resolve_review_comment(pb::ResolveReviewCommentRequest {
            checkout_id: diff.checkout_id.clone(),
            id: "no-such-comment".into(),
            resolved: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);

    // Content search over a project.
    let host = pb::StudioServiceClient::new(HttpClient::plaintext(), daemon.config())
        .get_host(pb::GetHostRequest::default())
        .await
        .unwrap()
        .into_owned();
    let device_id = host.host.as_option().unwrap().device_id.clone();
    daemon
        .ndjson
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createSpace", "spaceId": "space-connect", "deviceId": device_id,
                "path": repo_path, "gitDetected": true,
            }),
        )
        .await
        .unwrap();
    let files = pb::FileServiceClient::new(HttpClient::plaintext(), daemon.config());
    let mut found = files
        .search_content(pb::SearchContentRequest {
            space_id: Some("space-connect".into()),
            query: "needle".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let hit = next!(found);
    assert_eq!(
        (
            hit.path.as_str(),
            hit.line,
            hit.column,
            hit.preview.as_str()
        ),
        ("notes.txt", 2, 8, "second needle")
    );
    daemon.stop().await;
}
