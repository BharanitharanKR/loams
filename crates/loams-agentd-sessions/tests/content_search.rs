//! SD1 Task 11: content search (design §53 §6.5 G23), against a real
//! repository in a temporary directory, through the engine's dispatch.
#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use futures::StreamExt as _;
use loams_agentd_proto::{ContentSearchMatch, HarnessId};
use loams_agentd_rpc::{RpcError, RpcReply, RpcService as _, methods};
use loams_agentd_sessions::content_search::{MAX_FILE_BYTES, MAX_RESULTS};
use loams_agentd_sessions::{EngineCore, HarnessRegistry};

const SPACE: &str = "space-search";

fn git(cwd: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .status()
        .expect("git runs");
    assert!(status.success(), "git {args:?}");
}

fn write(root: &Path, path: &str, text: impl AsRef<[u8]>) {
    let full = root.join(path);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(full, text).unwrap();
}

struct Engine {
    _tmp: tempfile::TempDir,
    core: EngineCore,
    repo: std::path::PathBuf,
}

impl Engine {
    async fn start(files: &[(&str, &[u8])]) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        for (path, text) in files {
            write(&repo, path, text);
        }
        let repo = repo.canonicalize().unwrap();
        let core = EngineCore::assemble(
            &tmp.path().join("data"),
            Arc::new(HarnessRegistry::new()),
            HarnessId::Mock,
        )
        .unwrap();
        core.workspace
            .create_space(
                SPACE,
                &core.device_id,
                &repo.to_string_lossy(),
                Some("Repo".into()),
                true,
            )
            .unwrap();
        Self {
            _tmp: tmp,
            core,
            repo,
        }
    }

    async fn search(&self, params: serde_json::Value) -> Result<Vec<ContentSearchMatch>, RpcError> {
        let rpc = self.core.rpc_service();
        match rpc
            .handle(methods::SEARCH_WORKSPACE_CONTENT, params)
            .await?
        {
            RpcReply::Stream(items) => Ok(items
                .map(|item| serde_json::from_value(item).unwrap())
                .collect()
                .await),
            RpcReply::Value(value) => panic!("a stream, not {value}"),
        }
    }

    async fn stop(self) {
        self.core.shutdown().await;
    }
}

fn paths(found: &[ContentSearchMatch]) -> Vec<&str> {
    let mut paths: Vec<&str> = found.iter().map(|m| m.path.as_str()).collect();
    paths.sort_unstable();
    paths.dedup();
    paths
}

#[tokio::test(flavor = "multi_thread")]
async fn content_search_respects_gitignore() {
    let engine = Engine::start(&[
        (".gitignore", b"ignored/\n*.log\n"),
        ("src/main.rs", b"fn main() {\n    let needle = 1;\n}\n"),
        ("docs/Readme.md", b"A NEEDLE in caps\n"),
        ("ignored/hidden.rs", b"needle\n"),
        ("trace.log", b"needle\n"),
        ("blob.bin", b"needle\0binary\n"),
        (".github/ci.yml", b"needle: yes\n"),
    ])
    .await;
    // `.git` itself is never searched.
    write(&engine.repo, ".git/needle-note", "needle\n");

    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle"}))
        .await
        .unwrap();
    assert_eq!(
        paths(&found),
        [".github/ci.yml", "docs/Readme.md", "src/main.rs"],
        "{found:?}"
    );
    let main = found.iter().find(|m| m.path == "src/main.rs").unwrap();
    assert_eq!((main.line, main.column), (2, 9));
    assert_eq!(main.preview, "    let needle = 1;");

    // Case-sensitive, a regex, and globs.
    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle", "caseSensitive": true}))
        .await
        .unwrap();
    assert_eq!(paths(&found), [".github/ci.yml", "src/main.rs"]);
    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "let \\w+ = \\d", "regex": true}))
        .await
        .unwrap();
    assert_eq!(paths(&found), ["src/main.rs"]);
    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle", "globs": ["*.md"]}))
        .await
        .unwrap();
    assert_eq!(paths(&found), ["docs/Readme.md"]);
    // A glob never reaches an ignored file.
    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle", "globs": ["*.log"]}))
        .await
        .unwrap();
    assert!(found.is_empty(), "{found:?}");

    // The target is jailed: exactly one of spaceId or chatId, and a known one.
    for params in [
        serde_json::json!({"query": "needle"}),
        serde_json::json!({"spaceId": SPACE, "chatId": "c", "query": "needle"}),
        serde_json::json!({"spaceId": "no-such-space", "query": "needle"}),
        serde_json::json!({"spaceId": SPACE, "query": ""}),
        serde_json::json!({"spaceId": SPACE, "query": "(", "regex": true}),
    ] {
        assert!(engine.search(params.clone()).await.is_err(), "{params}");
    }
    engine.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn content_search_caps_results() {
    let many = "needle\n".repeat(MAX_RESULTS + 500);
    let mut big = vec![b'x'; usize::try_from(MAX_FILE_BYTES).unwrap()];
    big.extend_from_slice(b"\nfar needle\n");
    let engine = Engine::start(&[("many.txt", many.as_bytes()), ("big.txt", &big)]).await;

    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle"}))
        .await
        .unwrap();
    assert_eq!(found.len(), MAX_RESULTS);
    assert!(found.iter().all(|m| m.path == "many.txt"), "past 1 MiB");

    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle", "maxResults": 5}))
        .await
        .unwrap();
    assert_eq!(found.len(), 5);
    let found = engine
        .search(serde_json::json!({"spaceId": SPACE, "query": "needle", "maxResults": 100_000}))
        .await
        .unwrap();
    assert_eq!(found.len(), MAX_RESULTS, "clamped to the cap");
    engine.stop().await;
}
