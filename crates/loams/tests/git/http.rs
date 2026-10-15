//! The git listener and git tokens (GT1 Task 16, design §36 §10, D182, D111):
//! `cargo test -p loams --features git --test git http::`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use loams::{Server, ServerConfig};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::Running;

const WRITE_TOKEN: &str = "lgt_acme_write_0123456789";
const UNKNOWN_TOKEN: &str = "lgt_nobody_9876543210";

fn sha256_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A server with the git listener on loopback and one token for namespace `acme`, holding
/// repository `web` in both `acme` and `other`.
async fn start(dir: &TempDir) -> (Running, SocketAddr) {
    let tokens = dir.path().join("git-tokens.toml");
    std::fs::write(
        &tokens,
        format!(
            "[[tokens]]\nsha256 = \"{}\"\nprincipal = \"ci\"\nnamespace = \"acme\"\n\
             access = \"write\"\n",
            sha256_hex(WRITE_TOKEN)
        ),
    )
    .expect("tokens file");
    let running = Running::start_with(|config| {
        config.git.listen = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
        config.git.tokens = Some(tokens);
    })
    .await;
    for ns in ["acme", "other"] {
        let (status, body) = running
            .connect(
                "/loams.repos.v1.RepoService/CreateRepo",
                &json!({"namespace": ns, "name": "web", "idempotencyKey": format!("k-{ns}")}),
            )
            .await;
        assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    }
    let addr = running.server.git_listen_addr().expect("the git listener");
    (running, addr)
}

async fn get(addr: SocketAddr, path: &str, auth: Option<String>) -> (u16, String, Option<String>) {
    let mut request = reqwest::Client::new().get(format!("http://{addr}{path}"));
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    let response = request.send().await.expect("request");
    let status = response.status().as_u16();
    let challenge = response
        .headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    (status, response.text().await.expect("body"), challenge)
}

fn basic(token: &str) -> Option<String> {
    Some(format!(
        "Basic {}",
        STANDARD.encode(format!("x-token:{token}"))
    ))
}

const REFS: &str = "/info/refs?service=git-upload-pack";

#[tokio::test]
async fn namespace_from_token_not_url() {
    let dir = TempDir::new().expect("dir");
    let (_running, addr) = start(&dir).await;
    // The token's own namespace: admitted (Smart HTTP itself is Task 46).
    let (status, body, _) = get(
        addr,
        &format!("/git/acme/web.git{REFS}"),
        basic(WRITE_TOKEN),
    )
    .await;
    assert_eq!(status, 501, "{body}");
    let bearer = Some(format!("Bearer {WRITE_TOKEN}"));
    assert_eq!(
        get(addr, &format!("/git/acme/web.git{REFS}"), bearer)
            .await
            .0,
        501
    );
    // The same repository name in another namespace does not exist for this token.
    let (status, _, _) = get(
        addr,
        &format!("/git/other/web.git{REFS}"),
        basic(WRITE_TOKEN),
    )
    .await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn other_namespace_is_404() {
    let dir = TempDir::new().expect("dir");
    let (_running, addr) = start(&dir).await;
    // Another namespace's repository answers exactly as a repository that does not exist.
    let other = get(
        addr,
        &format!("/git/other/web.git{REFS}"),
        basic(WRITE_TOKEN),
    )
    .await;
    let missing = get(
        addr,
        &format!("/git/acme/nope.git{REFS}"),
        basic(WRITE_TOKEN),
    )
    .await;
    let no_ns = get(
        addr,
        &format!("/git/nowhere/web.git{REFS}"),
        basic(WRITE_TOKEN),
    )
    .await;
    assert_eq!(other.0, 404);
    assert_eq!(other, missing);
    assert_eq!(other, no_ns);
    // Without a credential, or with an unknown one: 401 and the Basic challenge.
    for auth in [None, basic(UNKNOWN_TOKEN)] {
        let (status, _, challenge) = get(addr, &format!("/git/acme/web.git{REFS}"), auth).await;
        assert_eq!(status, 401);
        assert_eq!(challenge.as_deref(), Some("Basic realm=\"Loams Git\""));
    }
}

#[tokio::test]
async fn non_loopback_listen_is_refused() {
    let dir = TempDir::new().expect("dir");
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.git.listen = Some("0.0.0.0:7801".parse().expect("addr"));
    config.git.tokens = Some(dir.path().join("tokens.toml"));
    let err = Server::start(config).await.expect_err("refused");
    assert_eq!(
        err.to_string(),
        "git listen on 0.0.0.0:7801: only loopback addresses are served until the unified auth \
         plan (D111)"
    );
    // A loopback listener needs a token file.
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.git.listen = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    let err = Server::start(config).await.expect_err("refused");
    assert!(err.to_string().contains("--git-tokens"), "{err}");
}

/// A `tracing` writer into a shared buffer.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn token_never_logged() {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    // Global, so the server's tasks are captured on every thread; this is the only test of
    // the binary that installs a subscriber.
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("a global subscriber is already set: token_never_logged skipped");
        return;
    }
    let dir = TempDir::new().expect("dir");
    let (running, addr) = start(&dir).await;
    for (auth, path) in [
        (basic(WRITE_TOKEN), "/git/acme/web.git"),
        (basic(UNKNOWN_TOKEN), "/git/acme/web.git"),
        (basic(WRITE_TOKEN), "/git/other/web.git"),
        (Some(format!("Bearer {UNKNOWN_TOKEN}")), "/git/acme/web.git"),
    ] {
        let (_, body, _) = get(addr, &format!("{path}{REFS}"), auth).await;
        assert!(!body.contains("lgt_"), "an answer carries a token: {body}");
    }
    drop(running);
    let logs = String::from_utf8_lossy(&captured.0.lock().expect("lock")).into_owned();
    assert!(!logs.is_empty(), "nothing was captured");
    for token in [WRITE_TOKEN, UNKNOWN_TOKEN] {
        assert!(!logs.contains(token), "a token reached the logs");
    }
}
