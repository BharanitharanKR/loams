//! The remote listener (plan SD1 Task 7): TLS with a self-signed
//! certificate, OIDC tokens from a stand-in issuer on loopback, the origin
//! allowlist and CORS, and the `studio` role, in front of a real in-process
//! engine with the Mock harness. Nothing here reaches a real service.
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use loams_agentd_connect::cors::{ALLOW_HEADERS, CorsPolicy};
use loams_agentd_connect::oidc::{JwksSource, OidcOptions};
use loams_agentd_connect::remote::tls_config;
use loams_agentd_connect::{Agentd, HostInfo, RemoteListener, RemoteOptions};
use loams_agentd_proto::daemon::DaemonMode;
use loams_agentd_rpc::RpcService;
use loams_agentd_sessions::{Engine, EngineConfig, EngineProfile, EngineRuntime, HarnessId};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use serde_json::{Value, json};

const AUDIENCE: &str = "loams-agentd";
const SUBJECT: &str = "enrolled-subject-1";
const CONSOLE: &str = "https://console.example.com";
const LIST_HARNESSES: &str = "/loams.agentd.v1.HarnessService/ListHarnesses";

/// An Ed25519 signing key and its JWK `x`.
fn ed25519() -> (EncodingKey, String) {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let x = URL_SAFE_NO_PAD.encode(pair.public_key().as_ref());
    (EncodingKey::from_ed_der(pkcs8.as_ref()), x)
}

fn jwk(kid: &str, x: &str) -> Value {
    json!({"kty": "OKP", "crv": "Ed25519", "x": x, "kid": kid, "alg": "EdDSA", "use": "sig"})
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A stand-in OIDC issuer on loopback HTTP: discovery and a JWKS with one
/// Ed25519 key (`k1`), counting JWKS fetches.
struct Issuer {
    url: String,
    signer: EncodingKey,
    /// A key the issuer does not publish.
    stranger: EncodingKey,
    jwks_fetches: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Issuer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/application/o/loams/",
            listener.local_addr().unwrap()
        );
        let (signer, x) = ed25519();
        let (stranger, _) = ed25519();
        let jwks = json!({"keys": [jwk("k1", &x)]});
        let discovery = json!({"issuer": url, "jwks_uri": format!("{url}jwks/")});
        let jwks_fetches = Arc::new(AtomicUsize::new(0));
        let counter = jwks_fetches.clone();
        let app = axum::Router::new()
            .route(
                "/application/o/loams/.well-known/openid-configuration",
                axum::routing::get(move || async move { axum::Json(discovery) }),
            )
            .route(
                "/application/o/loams/jwks/",
                axum::routing::get(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let jwks = jwks.clone();
                    async move { axum::Json(jwks) }
                }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url,
            signer,
            stranger,
            jwks_fetches,
            task,
        }
    }

    fn claims(&self) -> Value {
        let now = now();
        json!({"iss": self.url, "aud": AUDIENCE, "sub": SUBJECT, "iat": now, "exp": now + 300})
    }

    /// A token for `claims`, signed by `key` with `kid`.
    fn sign(&self, claims: &Value, key: &EncodingKey, kid: &str) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_owned());
        jsonwebtoken::encode(&header, claims, key).unwrap()
    }

    /// A good token, after `edit` changes its claims.
    fn token(&self, edit: impl FnOnce(&mut Value)) -> String {
        let mut claims = self.claims();
        edit(&mut claims);
        self.sign(&claims, &self.signer, "k1")
    }
}

impl Drop for Issuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Remote {
    _dir: tempfile::TempDir,
    runtime: EngineRuntime,
    listener: RemoteListener,
    client: jwks_http::Client,
}

impl Remote {
    async fn start(oidc: OidcOptions) -> Self {
        Self::start_with_preview(oidc, None).await
    }

    async fn start_with_preview(oidc: OidcOptions, preview_port: Option<u16>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            data_dir: dir.path().join("store"),
            default_harness: HarnessId::Mock,
            terminal_shell: None,
            tool_outputs: Default::default(),
            mcp_endpoint: None,
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let runtime =
            Engine::assemble_runtime(&config, EngineProfile::local(&config.data_dir).unwrap())
                .await
                .unwrap();
        let engine: Arc<dyn RpcService> = runtime.core().rpc_service();
        let agentd = Agentd::new(
            engine,
            HostInfo {
                protocol: 1,
                version: "0.0.0-test".into(),
                build_sha: "test".into(),
                mode: DaemonMode::Service,
                pid: std::process::id(),
                started_at_ms: 1,
                features: vec!["studio.v1".into()],
            },
        );

        // A self-signed certificate for localhost, written as PEM files as
        // `--tls-cert` and `--tls-key` name them.
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let cert_pem = certified.cert.pem();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, &cert_pem).unwrap();
        std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();

        let listener = RemoteListener::bind(
            agentd,
            RemoteOptions {
                listen: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                tls: tls_config(&cert, &key).unwrap(),
                oidc,
                cors: CorsPolicy::new([CONSOLE]).unwrap(),
                auth_failure_delay: Duration::from_millis(5),
                preview_port,
            },
        )
        .await
        .unwrap();
        let client = jwks_http::Client::builder()
            .add_root_certificate(jwks_http::Certificate::from_pem(cert_pem.as_bytes()).unwrap())
            .resolve("localhost", listener.local_addr())
            .build()
            .unwrap();
        Self {
            _dir: dir,
            runtime,
            listener,
            client,
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "https://localhost:{}{path}",
            self.listener.local_addr().port()
        )
    }

    /// A Connect unary POST with JSON and `headers`.
    async fn post(&self, path: &str, headers: &[(&str, &str)]) -> Answer {
        let mut request = self
            .client
            .post(self.url(path))
            .header("content-type", "application/json")
            .body("{}");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        Answer::of(request.send().await.unwrap()).await
    }

    async fn stop(self) {
        self.listener.stop().await;
        self.runtime.shutdown().await;
    }
}

struct Answer {
    status: u16,
    headers: http::HeaderMap,
    body: Value,
}

impl Answer {
    async fn of(response: jwks_http::Response) -> Self {
        let status = response.status().as_u16();
        let mut headers = http::HeaderMap::new();
        for (name, value) in response.headers() {
            headers.append(
                http::HeaderName::from_bytes(name.as_str().as_bytes()).unwrap(),
                http::HeaderValue::from_bytes(value.as_bytes()).unwrap(),
            );
        }
        let body = response.json().await.unwrap_or(Value::Null);
        Self {
            status,
            headers,
            body,
        }
    }

    /// The Connect code of a refusal.
    fn code(&self) -> Option<&str> {
        self.body["code"].as_str()
    }

    /// Whether a `loams.errors.v1.ErrorInfo` detail carries `reason`.
    fn has_reason(&self, reason: &str) -> bool {
        use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
        self.body["details"].as_array().is_some_and(|details| {
            details.iter().any(|detail| {
                let value = detail["value"].as_str().unwrap_or_default();
                let bytes = STANDARD_NO_PAD
                    .decode(value.trim_end_matches('='))
                    .or_else(|_| STANDARD.decode(value))
                    .unwrap_or_default();
                detail["type"] == "loams.errors.v1.ErrorInfo"
                    && bytes
                        .windows(reason.len())
                        .any(|window| window == reason.as_bytes())
            })
        })
    }

    fn has_cors(&self) -> bool {
        loams_agentd_connect::cors::has_cors_headers(&self.headers)
    }
}

fn options(issuer: &Issuer) -> OidcOptions {
    OidcOptions::new(&issuer.url, AUDIENCE, SUBJECT)
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[tokio::test]
async fn remote_accepts_the_enrolled_subject() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let answer = remote.post(LIST_HARNESSES, &[]).await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    assert_eq!(answer.code(), Some("unauthenticated"));
    assert!(answer.has_reason("unauthenticated"), "{}", answer.body);
    let token = bearer(&issuer.token(|_| {}));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(
        answer.body["harnesses"]
            .as_array()
            .is_some_and(|h| !h.is_empty())
    );
    // A non-browser client (no Origin) gets no CORS headers.
    assert!(!answer.has_cors());
    remote.stop().await;
}

#[tokio::test]
async fn remote_wrong_audience_is_401() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    for audience in [json!("loams-console"), json!(["loams-console", "other"])] {
        let token = bearer(&issuer.token(|claims| claims["aud"] = audience.clone()));
        let answer = remote
            .post(LIST_HARNESSES, &[("authorization", &token)])
            .await;
        assert_eq!(answer.status, 401, "aud {audience}: {}", answer.body);
        assert_eq!(answer.code(), Some("unauthenticated"));
    }
    // A token with the right audience among others is fine.
    let token = bearer(&issuer.token(|claims| claims["aud"] = json!(["x", AUDIENCE])));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    // Another issuer, a key the issuer does not publish, HMAC, and garbage.
    let token = bearer(&issuer.token(|claims| claims["iss"] = json!("https://evil.example/")));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    let stranger = bearer(&issuer.sign(&issuer.claims(), &issuer.stranger, "k1"));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &stranger)])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    let hmac = jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &issuer.claims(),
        &EncodingKey::from_secret(b"public"),
    )
    .unwrap();
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &bearer(&hmac))])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", "Bearer not-a-jwt")])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    remote.stop().await;
}

#[tokio::test]
async fn remote_expired_is_401() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let now = now();
    // Past the 60 s leeway.
    let expired = bearer(&issuer.token(|claims| claims["exp"] = json!(now - 120)));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &expired)])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    // Not yet valid, past the leeway.
    let early = bearer(&issuer.token(|claims| claims["nbf"] = json!(now + 600)));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &early)])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    // Within the leeway.
    let skewed = bearer(&issuer.token(|claims| claims["exp"] = json!(now - 20)));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &skewed)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    // No exp at all.
    let forever = bearer(&issuer.token(|claims| {
        claims.as_object_mut().unwrap().remove("exp");
    }));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &forever)])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    remote.stop().await;
}

#[tokio::test]
async fn remote_wrong_subject_is_403() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|claims| claims["sub"] = json!("someone-else")));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(answer.code(), Some("permission_denied"));
    assert!(answer.has_reason("forbidden_role"), "{}", answer.body);
    remote.stop().await;
}

#[tokio::test]
async fn remote_disallowed_origin_gets_no_cors() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    for origin in [
        "https://evil.example",
        "https://console.example.com.evil.example",
        "http://console.example.com",
        "null",
    ] {
        // The preflight.
        let response = remote
            .client
            .request(http::Method::OPTIONS, remote.url(LIST_HARNESSES))
            .header("origin", origin)
            .header("access-control-request-method", "POST")
            .header(
                "access-control-request-headers",
                "authorization,content-type",
            )
            .send()
            .await
            .unwrap();
        let answer = Answer::of(response).await;
        assert_eq!(answer.status, 403, "{origin}: {}", answer.body);
        assert!(!answer.has_cors(), "{origin}: {:?}", answer.headers);
        // The request itself, even with a good token.
        let answer = remote
            .post(
                LIST_HARNESSES,
                &[("authorization", &token), ("origin", origin)],
            )
            .await;
        assert_eq!(answer.status, 403, "{origin}: {}", answer.body);
        assert_eq!(answer.code(), Some("permission_denied"));
        assert!(answer.has_reason("origin_not_allowed"), "{}", answer.body);
        assert!(
            answer.body["message"]
                .as_str()
                .is_some_and(|m| m.contains("origin")),
            "{}",
            answer.body
        );
        assert!(!answer.has_cors(), "{origin}: {:?}", answer.headers);
    }
    remote.stop().await;
}

#[tokio::test]
async fn remote_allowed_origin_preflight_ok() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let response = remote
        .client
        .request(http::Method::OPTIONS, remote.url(LIST_HARNESSES))
        .header("origin", CONSOLE)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization,content-type,connect-protocol-version",
        )
        .send()
        .await
        .unwrap();
    let answer = Answer::of(response).await;
    assert_eq!(answer.status, 204, "{}", answer.body);
    let h = &answer.headers;
    assert_eq!(h["access-control-allow-origin"], CONSOLE);
    assert_eq!(h["access-control-allow-headers"], ALLOW_HEADERS);
    assert!(
        !h.contains_key("access-control-allow-credentials"),
        "never credentials: {h:?}"
    );
    let allowed: Vec<&str> = h["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .split(", ")
        .collect();
    assert!(allowed.contains(&"authorization"));
    assert!(!allowed.contains(&"cookie"));
    assert!(allowed.iter().all(|name| *name == "authorization"
        || *name == "content-type"
        || name.starts_with("connect-")
        || name.starts_with("grpc-")
        || name.starts_with("x-grpc-")
        || *name == "x-user-agent"));

    // The call itself from the allowed origin carries the CORS headers,
    // and so does a refusal, so the console can read it.
    let token = bearer(&issuer.token(|_| {}));
    let answer = remote
        .post(
            LIST_HARNESSES,
            &[("authorization", &token), ("origin", CONSOLE)],
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.headers["access-control-allow-origin"], CONSOLE);
    assert!(
        !answer
            .headers
            .contains_key("access-control-allow-credentials")
    );
    let answer = remote.post(LIST_HARNESSES, &[("origin", CONSOLE)]).await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    assert_eq!(answer.headers["access-control-allow-origin"], CONSOLE);
    remote.stop().await;
}

#[tokio::test]
async fn remote_studio_role_cannot_shutdown() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    // There is no Shutdown on the Connect surface, under any service.
    for path in [
        "/loams.agentd.v1.StudioService/Shutdown",
        "/loams.agentd.v1.HarnessService/Shutdown",
        "/Shutdown",
    ] {
        let answer = remote.post(path, &[("authorization", &token)]).await;
        assert!(
            answer.status >= 400,
            "{path}: {} {}",
            answer.status,
            answer.body
        );
        assert_eq!(answer.code(), Some("unimplemented"), "{path}");
    }
    // The daemon still answers.
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    remote.stop().await;
}

#[tokio::test]
async fn remote_studio_role_cannot_install_harness() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    for method in [
        "InstallHarness",
        "CancelInstall",
        "ApplyHarnessUpdate",
        "StartAgentLogin",
        "CompleteAgentLogin",
        "ActivateAgentAccount",
    ] {
        let answer = remote
            .post(
                &format!("/loams.agentd.v1.HarnessService/{method}"),
                &[("authorization", &token)],
            )
            .await;
        assert_eq!(answer.status, 403, "{method}: {}", answer.body);
        assert_eq!(answer.code(), Some("permission_denied"), "{method}");
        assert!(
            answer.has_reason("forbidden_role"),
            "{method}: {}",
            answer.body
        );
    }
    remote.stop().await;
}

#[tokio::test]
async fn remote_never_serves_ndjson() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    // The ndjson wire is a WebSocket upgrade on the loopback listener; here
    // it is not a method, with or without a token.
    for path in ["/", "/rpc", "/ws"] {
        let response = remote
            .client
            .get(remote.url(path))
            .header("authorization", &token)
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        assert_ne!(status, 101, "{path} upgraded");
        assert!(status >= 400, "{path}: {status}");
        let response = remote
            .client
            .post(remote.url(path))
            .header("authorization", &token)
            .header("content-type", "application/x-ndjson")
            .body("{\"id\":1,\"method\":\"Hello\",\"params\":{}}\n")
            .send()
            .await
            .unwrap();
        assert!(response.status().as_u16() >= 400, "{path}");
    }
    remote.stop().await;
}

#[tokio::test]
async fn jwks_refresh_rate_limited() {
    let issuer = Issuer::start().await;
    let mut oidc = options(&issuer);
    oidc.min_refetch = Duration::from_millis(400);
    let remote = Remote::start(oidc).await;
    let good = bearer(&issuer.token(|_| {}));
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &good)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(issuer.jwks_fetches.load(Ordering::SeqCst), 1);

    // Unknown kids within the interval refetch nothing.
    for kid in ["k2", "k3", "k4", "k5"] {
        let token = bearer(&issuer.sign(&issuer.claims(), &issuer.stranger, kid));
        let answer = remote
            .post(LIST_HARNESSES, &[("authorization", &token)])
            .await;
        assert_eq!(answer.status, 401, "{}", answer.body);
    }
    assert_eq!(
        issuer.jwks_fetches.load(Ordering::SeqCst),
        1,
        "an unknown kid refetches at most once per interval"
    );

    // After the interval, one unknown kid refetches once.
    tokio::time::sleep(Duration::from_millis(450)).await;
    for kid in ["k6", "k7"] {
        let token = bearer(&issuer.sign(&issuer.claims(), &issuer.stranger, kid));
        let answer = remote
            .post(LIST_HARNESSES, &[("authorization", &token)])
            .await;
        assert_eq!(answer.status, 401, "{}", answer.body);
    }
    assert_eq!(issuer.jwks_fetches.load(Ordering::SeqCst), 2);

    // A known kid never refetches.
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &good)])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(issuer.jwks_fetches.load(Ordering::SeqCst), 2);
    remote.stop().await;
}

#[test]
fn the_default_refetch_interval_is_a_minute() {
    assert_eq!(
        loams_agentd_connect::oidc::JWKS_MIN_REFETCH,
        Duration::from_secs(60)
    );
    assert_eq!(loams_agentd_connect::oidc::LEEWAY_SECS, 60);
}

#[tokio::test]
async fn test_issuer_reads_a_local_jwks_file() {
    // `LOAMS_AGENTD_TEST_ISSUER`: the keys come from a file, never from
    // the network.
    let dir = tempfile::tempdir().unwrap();
    let (signer, x) = ed25519();
    let jwks = dir.path().join("jwks.json");
    std::fs::write(&jwks, json!({"keys": [jwk("t1", &x)]}).to_string()).unwrap();
    let issuer = "https://auth.invalid/application/o/loams/";
    let mut oidc = OidcOptions::new(issuer, AUDIENCE, SUBJECT);
    oidc.jwks = JwksSource::File(jwks);
    let remote = Remote::start(oidc).await;
    let now = now();
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("t1".into());
    let token = jsonwebtoken::encode(
        &header,
        &json!({"iss": issuer, "aud": AUDIENCE, "sub": SUBJECT, "exp": now + 60}),
        &signer,
    )
    .unwrap();
    let answer = remote
        .post(LIST_HARNESSES, &[("authorization", &bearer(&token))])
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    remote.stop().await;
}

#[tokio::test]
async fn remote_streams_over_tls() {
    // A Connect server stream over TLS: WatchThreads' first frame.
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    let response = remote
        .client
        .post(remote.url("/loams.agentd.v1.ThreadService/WatchThreads"))
        .header("authorization", &token)
        .header("origin", CONSOLE)
        .header("content-type", "application/connect+json")
        .body({
            let payload = b"{}";
            let mut frame = vec![0u8];
            frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
            frame.extend_from_slice(payload);
            frame
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["access-control-allow-origin"], CONSOLE);
    let mut response = response;
    let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(chunk.len() >= 5, "a Connect envelope");
    assert_eq!(chunk[0], 0, "a message, not the end of the stream");
    remote.stop().await;
}

/// A stand-in for the daemon's preview proxy: echoes what reached it.
async fn fake_preview_proxy() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = axum::Router::new().fallback(|request: axum::extract::Request| async move {
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        axum::Json(json!({
            "path": request.uri().to_string(),
            "host": header("host"),
            "authorization": header("authorization"),
            "prefix": header("x-forwarded-prefix"),
        }))
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (port, task)
}

impl Remote {
    async fn get(&self, path: &str, headers: &[(&str, &str)]) -> Answer {
        let mut request = self.client.get(self.url(path));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        Answer::of(request.send().await.unwrap()).await
    }

    /// `POST /preview/-/ticket` for `preview`.
    async fn ticket(&self, preview: &str, headers: &[(&str, &str)]) -> Answer {
        let mut request = self
            .client
            .post(self.url("/preview/-/ticket"))
            .header("content-type", "application/json")
            .body(json!({ "preview": preview }).to_string());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        Answer::of(request.send().await.unwrap()).await
    }

    /// The listener's own origin, as a preview page sends it.
    fn own_origin(&self) -> String {
        format!("https://localhost:{}", self.listener.local_addr().port())
    }
}

#[tokio::test]
async fn remote_preview_requires_token() {
    let issuer = Issuer::start().await;
    let (port, upstream) = fake_preview_proxy().await;
    let remote = Remote::start_with_preview(options(&issuer), Some(port)).await;

    // No token, a bad token, another subject: refused before the dev server.
    let answer = remote.get("/preview/dev.my-app/", &[]).await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    assert_eq!(answer.code(), Some("unauthenticated"));
    let answer = remote
        .get("/preview/dev.my-app/", &[("authorization", "Bearer x.y.z")])
        .await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    let other = bearer(&issuer.token(|claims| claims["sub"] = json!("someone-else")));
    let answer = remote
        .get("/preview/dev.my-app/", &[("authorization", &other)])
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    // A ticket that was never minted, and a ticket request without a token.
    let fake = format!("/preview/pt_{}/", "0".repeat(64));
    let answer = remote.get(&fake, &[]).await;
    assert_eq!(answer.status, 401, "{}", answer.body);
    let answer = remote.ticket("dev.my-app", &[("origin", CONSOLE)]).await;
    assert_eq!(answer.status, 401, "{}", answer.body);

    // With the token: forwarded to the preview proxy under the preview's
    // hostname, the prefix stripped, and the token never passed on.
    let token = bearer(&issuer.token(|_| {}));
    let answer = remote
        .get(
            "/preview/dev.my-app/src/main.tsx?v=1",
            &[("authorization", &token)],
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.body["path"], "/src/main.tsx?v=1");
    assert_eq!(
        answer.body["host"],
        format!("dev.my-app.localhost:{port}").as_str()
    );
    assert_eq!(answer.body["authorization"], Value::Null);
    assert_eq!(answer.body["prefix"], "/preview/dev.my-app");

    // A ticket from the console reaches that preview only, without a token.
    let minted = remote
        .ticket(
            "dev.my-app",
            &[("origin", CONSOLE), ("authorization", &token)],
        )
        .await;
    assert_eq!(minted.status, 200, "{}", minted.body);
    assert!(minted.has_cors());
    let base = minted.body["base"].as_str().unwrap().to_owned();
    assert!(
        base.starts_with("/preview/pt_") && base.ends_with('/'),
        "{base}"
    );
    assert!(minted.body["expiresAtMs"].as_u64().unwrap() > now() * 1000);
    let answer = remote.get(&format!("{base}index.html"), &[]).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.body["path"], "/index.html");
    assert_eq!(
        answer.body["host"],
        format!("dev.my-app.localhost:{port}").as_str()
    );
    // A preview page may not mint tickets itself.
    let own = remote.own_origin();
    let answer = remote
        .ticket("dev.my-app", &[("origin", &own), ("authorization", &token)])
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    // Nonsense names are refused.
    let answer = remote
        .ticket("../etc", &[("origin", CONSOLE), ("authorization", &token)])
        .await;
    assert_eq!(answer.status, 400, "{}", answer.body);

    remote.stop().await;
    upstream.abort();
}

#[tokio::test]
async fn remote_preview_frame_ancestors_restricted() {
    let issuer = Issuer::start().await;
    let (port, upstream) = fake_preview_proxy().await;
    let remote = Remote::start_with_preview(options(&issuer), Some(port)).await;
    let token = bearer(&issuer.token(|_| {}));
    let minted = remote
        .ticket(
            "dev.my-app",
            &[("origin", CONSOLE), ("authorization", &token)],
        )
        .await;
    let base = minted.body["base"].as_str().unwrap().to_owned();

    // Framable by the configured console only; no Referer carries the ticket
    // away; no CORS for the console to read the page's bytes.
    let answer = remote.get(&base, &[]).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    let csp: Vec<_> = answer
        .headers
        .get_all("content-security-policy")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(csp, vec![format!("frame-ancestors {CONSOLE}")]);
    assert_eq!(answer.headers["referrer-policy"], "same-origin");
    assert!(!answer.has_cors());

    // Refusals carry it too.
    let answer = remote.get("/preview/dev.my-app/", &[]).await;
    assert_eq!(answer.status, 401);
    assert_eq!(
        answer.headers["content-security-policy"],
        format!("frame-ancestors {CONSOLE}").as_str()
    );

    // The page's own origin may call its preview (fetch, HMR); another site
    // may not, even with the ticket.
    let own = remote.own_origin();
    let answer = remote.get(&base, &[("origin", &own)]).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    let answer = remote
        .get(&base, &[("origin", "https://evil.example")])
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert!(answer.has_reason("origin_not_allowed"));
    assert!(!answer.has_cors());
    // The page's origin is still refused by the API.
    let answer = remote
        .post(
            LIST_HARNESSES,
            &[("origin", &own), ("authorization", &token)],
        )
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);

    // A preflight is answered for the console's ticket request only.
    let preflight = remote
        .client
        .request(http::Method::OPTIONS, remote.url("/preview/-/ticket"))
        .header("origin", CONSOLE)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization, content-type",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status().as_u16(), 204);
    let preflight = remote
        .client
        .request(http::Method::OPTIONS, remote.url(&base))
        .header("origin", CONSOLE)
        .header("access-control-request-method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status().as_u16(), 403);

    remote.stop().await;
    upstream.abort();
}

#[tokio::test]
async fn remote_preview_off_without_a_proxy() {
    let issuer = Issuer::start().await;
    let remote = Remote::start(options(&issuer)).await;
    let token = bearer(&issuer.token(|_| {}));
    let answer = remote
        .ticket(
            "dev.my-app",
            &[("origin", CONSOLE), ("authorization", &token)],
        )
        .await;
    assert_eq!(answer.status, 503, "{}", answer.body);
    let answer = remote
        .get("/preview/dev.my-app/", &[("authorization", &token)])
        .await;
    assert_eq!(answer.status, 503, "{}", answer.body);
    remote.stop().await;
}
