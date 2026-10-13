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
    let git = pb::GitServiceClient::new(HttpClient::plaintext(), daemon.config());
    let error = git
        .list_worktrees(pb::ListWorktreesRequest::default())
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

async fn git(cwd: &std::path::Path, args: &[&str]) -> String {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "git {args:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// SD1 Task 10: `ListCheckpoints` and `RestoreCheckpoint` over Connect
/// answer what the ndjson wire answers, and a stale checksum or an unknown
/// turn is the reason's Connect code.
#[tokio::test]
async fn checkpoints_over_connect_match_ndjson() {
    let daemon = Daemon::start().await;
    let repo = daemon._dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]).await;
    std::fs::write(repo.join("notes.txt"), "first\n").unwrap();
    git(&repo, &["add", "."]).await;
    git(&repo, &["commit", "-m", "initial"]).await;
    let core = daemon.runtime.core();
    daemon
        .ndjson
        .call(
            methods::MUTATE,
            serde_json::json!({
                "op": "createChat",
                "chatId": "chat-cp",
                "deviceId": core.device_id,
                "cwd": repo.to_string_lossy(),
            }),
        )
        .await
        .unwrap();
    core.checkpoints
        .capture_turn_start("chat-cp", &repo, "msg-1")
        .await
        .unwrap();
    std::fs::write(repo.join("notes.txt"), "changed\n").unwrap();

    let threads = pb::ThreadServiceClient::new(HttpClient::plaintext(), daemon.config());
    let listed = threads
        .list_checkpoints(pb::ListCheckpointsRequest {
            chat_id: "chat-cp".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    let ndjson: rs::checkpoints::CheckpointList = daemon
        .ndjson
        .call_as(
            methods::LIST_CHECKPOINTS,
            serde_json::json!({ "chatId": "chat-cp" }),
        )
        .await
        .unwrap();
    assert_eq!(listed.checkpoints.len(), 1);
    assert_eq!(listed.checkpoints[0].turn_id, "msg-1");
    assert_eq!(listed.checkpoints, ndjson.to_pb().checkpoints);

    let restore = |checksum: &str, turn_id: &str| pb::RestoreCheckpointRequest {
        chat_id: "chat-cp".into(),
        turn_id: turn_id.into(),
        expected_checksum: checksum.into(),
        scope: pb::CheckpointScope::CHECKPOINT_SCOPE_WORKTREE.into(),
        ..Default::default()
    };
    let error = threads
        .restore_checkpoint(restore("stale", "msg-1"))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::FailedPrecondition, "stale_diff");
    assert!(!error.details.is_empty(), "the ErrorInfo detail");
    assert_eq!(
        std::fs::read_to_string(repo.join("notes.txt")).unwrap(),
        "changed\n"
    );

    let diff: rs::CheckoutDiff = daemon
        .ndjson
        .call_as(
            methods::GET_CHECKOUT_DIFF,
            serde_json::json!({ "cwd": repo.to_string_lossy() }),
        )
        .await
        .unwrap();
    let error = threads
        .restore_checkpoint(restore(&diff.checksum, "msg-unknown"))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound, "checkpoint_missing");

    let restored = threads
        .restore_checkpoint(restore(&diff.checksum, "msg-1"))
        .await
        .unwrap()
        .into_owned();
    let pre = git(
        &repo,
        &["rev-parse", "refs/loams/checkpoints/chat-cp/turn/1/pre"],
    )
    .await;
    assert_eq!(restored.restored_to, pre);
    assert_eq!(restored.forked_chat_id, None);
    assert_eq!(
        std::fs::read_to_string(repo.join("notes.txt")).unwrap(),
        "first\n"
    );
    daemon.stop().await;
}
