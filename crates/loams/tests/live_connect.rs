//! Loams Live on the main Connect port (LV1 plan Task 12, design §45 §6,
//! D688): with Live running, `LiveService` and `LiveAdminService` answer on
//! `--listen` over Connect, gRPC and gRPC-Web, behind the port's
//! authentication layer, and `GetInstance` reports the package available.
//! Until LV1 Task 18 they are served on a loopback main port and on the
//! `tailscale serve` target only. Without Live, the package answers
//! `feature_not_in_variant`.

use std::net::SocketAddr;
use std::time::Duration;

use loams::{Server, ServerConfig};
use serde_json::{Value, json};
use tempfile::TempDir;

const LOOPBACK0: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

/// A deployment with one public query, `notes:ping`, answering `"pong"`.
#[cfg(feature = "live")]
const BUNDLE: &str = r#"
import { query } from "loams:server";
export const notes = { ping: query(async () => "pong") };
"#;

/// A server on an ephemeral loopback port with fast background work, and
/// Loams Live on an embedded store when `live`.
fn config(dir: &TempDir, live: bool) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = LOOPBACK0;
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    #[cfg(feature = "live")]
    if live {
        let store = loams_live::EmbeddedConfig::new(
            dir.path().join("live").join("store.redb"),
            loams_live::keyspace_of("t12"),
        );
        let mut live =
            loams_live::LiveConfig::with_store("t12", loams_live::StoreConfig::Embedded(store));
        live.listen = LOOPBACK0;
        config.live = Some(live);
    }
    #[cfg(not(feature = "live"))]
    let _ = live;
    config
}

/// A Connect unary call over JSON: the status, the `loams-auth-reason`
/// header and the body.
async fn call(
    addr: SocketAddr,
    path: &str,
    body: &Value,
    headers: &[(&str, &str)],
) -> (u16, Option<String>, Value) {
    let mut request = reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .body(body.to_string());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("a call");
    let status = response.status().as_u16();
    let reason = response
        .headers()
        .get("loams-auth-reason")
        .map(|v| v.to_str().expect("text").to_string());
    let text = response.text().await.expect("a body");
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, reason, body)
}

/// Deploys [`BUNDLE`] over `LiveAdminService` on `addr`.
#[cfg(feature = "live")]
async fn deploy(addr: SocketAddr, headers: &[(&str, &str)]) -> (u16, Value) {
    use base64::Engine as _;
    let (status, _, body) = call(
        addr,
        "/loams.live.v1.LiveAdminService/Deploy",
        &json!({
            "bundle": base64::engine::general_purpose::STANDARD.encode(BUNDLE),
            "message": "over the main port",
        }),
        headers,
    )
    .await;
    (status, body)
}

/// `GetInstance`'s `loams.live.v1` row on `addr`: whether it is available,
/// and whether `apiVersions` lists it.
async fn live_row(addr: SocketAddr, headers: &[(&str, &str)]) -> (bool, bool, Value) {
    let (status, _, info) = call(
        addr,
        "/loams.instance.v1.InstanceService/GetInstance",
        &json!({}),
        headers,
    )
    .await;
    assert_eq!(status, 200, "{info}");
    let row = info["services"]
        .as_array()
        .and_then(|list| list.iter().find(|s| s["package"] == "loams.live.v1"))
        .unwrap_or_else(|| panic!("no loams.live.v1 row in {info}"))
        .clone();
    // proto3 JSON leaves a false bool out.
    let available = row["available"] == true;
    let versioned = info["apiVersions"]
        .as_array()
        .is_some_and(|list| list.iter().any(|p| p == "loams.live.v1"));
    (available, versioned, row)
}

/// The reason of a Connect error's first `ErrorInfo` detail.
fn reason(error: &Value) -> String {
    use base64::Engine as _;
    use buffa::Message as _;
    let value = error["details"][0]["value"]
        .as_str()
        .unwrap_or_else(|| panic!("no ErrorInfo detail in {error}"));
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(value)
        .expect("unpadded base64");
    loams_proto::loams::errors::v1::ErrorInfo::decode_from_slice(&bytes)
        .expect("an ErrorInfo")
        .reason
}

/// The frames of a gRPC or gRPC-Web body: each is a flag byte, a big-endian
/// length and that many bytes.
#[cfg(feature = "live")]
fn frames(body: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 5 <= body.len() {
        let len =
            u32::from_be_bytes([body[at + 1], body[at + 2], body[at + 3], body[at + 4]]) as usize;
        let end = at + 5 + len;
        assert!(end <= body.len(), "a frame runs past the body: {body:?}");
        out.push((body[at], &body[at + 5..end]));
        at = end;
    }
    assert_eq!(at, body.len(), "a trailing partial frame: {body:?}");
    out
}

/// Live's services answer on `--listen` over the Connect protocol, gRPC
/// (HTTP/2) and gRPC-Web (HTTP/1.1), and they are the running Live server's:
/// a deployment made on the main port serves on Live's own listener too.
/// With authentication off the admin service records `dev-loopback`.
#[cfg(feature = "live")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_on_main_port_over_connect_grpc_and_grpc_web() {
    use buffa::Message as _;
    use loams_live_proto::loams::live::v1::QueryRequest;

    let dir = TempDir::new().expect("a temp dir");
    let server = Server::start(config(&dir, true)).await.expect("starts");
    let main = server.local_addr();
    let (status, deployed) = deploy(main, &[]).await;
    assert_eq!(status, 200, "{deployed}");
    assert_eq!(deployed["deployment"]["createdBy"], "dev-loopback");
    assert_eq!(deployed["deployment"]["state"], "DEPLOYMENT_STATE_ACTIVE");

    let query = json!({"function": "notes:ping"});
    for addr in [main, server.live_addr().expect("Live's own listener")] {
        let (status, _, answer) = call(addr, "/loams.live.v1.LiveService/Query", &query, &[]).await;
        assert_eq!(status, 200, "{addr}: {answer}");
        assert_eq!(answer["result"]["stringValue"], "pong", "{addr}: {answer}");
    }
    let (status, _, listed) = call(
        main,
        "/loams.live.v1.LiveAdminService/ListDeployments",
        &json!({}),
        &[],
    )
    .await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["deployments"].as_array().map(Vec::len), Some(1));

    let request = QueryRequest {
        function: "notes:ping".to_owned(),
        ..Default::default()
    }
    .encode_to_vec();
    let mut frame = vec![0u8];
    frame.extend_from_slice(&u32::try_from(request.len()).expect("small").to_be_bytes());
    frame.extend_from_slice(&request);
    for (content_type, http2) in [
        ("application/grpc+proto", true),
        ("application/grpc-web+proto", false),
    ] {
        let mut builder = reqwest::Client::builder();
        if http2 {
            builder = builder.http2_prior_knowledge();
        }
        let response = builder
            .build()
            .expect("a client")
            .post(format!("http://{main}/loams.live.v1.LiveService/Query"))
            .header("content-type", content_type)
            .header("te", "trailers")
            .body(frame.clone())
            .send()
            .await
            .unwrap_or_else(|err| panic!("{content_type}: {err}"));
        assert_eq!(response.status().as_u16(), 200, "{content_type}");
        if http2 {
            assert_eq!(
                response
                    .headers()
                    .get("grpc-status")
                    .map(|v| v.to_str().expect("text").to_owned()),
                None,
                "{content_type}: a trailers-only error"
            );
        }
        let body = response.bytes().await.expect("a framed body");
        let frames = frames(&body);
        let message = frames
            .first()
            .unwrap_or_else(|| panic!("{content_type}: no frame in {body:?}"));
        assert_eq!(message.0, 0, "{content_type}: a message frame first");
        assert!(
            message.1.windows(4).any(|w| w == b"pong"),
            "{content_type}: {body:?}"
        );
        if !http2 {
            let trailer = frames.last().expect("a trailer frame");
            assert_eq!(trailer.0 & 0x80, 0x80, "{content_type}: {body:?}");
            assert!(
                String::from_utf8_lossy(trailer.1).contains("grpc-status: 0"),
                "{content_type}: {body:?}"
            );
        }
    }
    server.shutdown().await.expect("stops");
}

/// With Live running, `GetInstance` lists `loams.live.v1` as available (and
/// still unstable) and `apiVersions` has it.
#[cfg(feature = "live")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_instance_reports_live_available() {
    let dir = TempDir::new().expect("a temp dir");
    let server = Server::start(config(&dir, true)).await.expect("starts");
    let (available, versioned, row) = live_row(server.local_addr(), &[]).await;
    assert!(available, "{row}");
    assert!(versioned);
    assert_eq!(row["unstable"], true, "{row}");
    assert_eq!(
        row["services"],
        json!([
            "loams.live.v1.LiveService",
            "loams.live.v1.LiveAdminService"
        ])
    );
    server.shutdown().await.expect("stops");
}

/// Without Live (`--no-live`, or a build without the feature) the package
/// is listed as unavailable and both services answer
/// `feature_not_in_variant`.
async fn live_absent(server: &Server) {
    let main = server.local_addr();
    let (available, versioned, row) = live_row(main, &[]).await;
    assert!(!available, "{row}");
    assert!(!versioned);
    for path in [
        "/loams.live.v1.LiveService/Query",
        "/loams.live.v1.LiveAdminService/ListDeployments",
    ] {
        let (status, _, error) = call(main, path, &json!({}), &[]).await;
        assert_eq!(status, 501, "{path}: {error}");
        assert_eq!(reason(&error), "feature_not_in_variant", "{path}");
    }
}

/// `--no-live`: Live is not running, so the main port answers for it with
/// the typed refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_absent_when_live_is_off() {
    let dir = TempDir::new().expect("a temp dir");
    let server = Server::start(config(&dir, false)).await.expect("starts");
    live_absent(&server).await;
    server.shutdown().await.expect("stops");
}

/// A build without the `live` feature keeps the typed refusal.
#[cfg(not(feature = "live"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_absent_without_feature() {
    let dir = TempDir::new().expect("a temp dir");
    let server = Server::start(config(&dir, true)).await.expect("starts");
    live_absent(&server).await;
    server.shutdown().await.expect("stops");
}

/// On the main port Live sits behind the port's authentication layer: with
/// API keys on, both services refuse a call without a key (`token_missing`),
/// and a deployment made with one records the key's id, never the key.
#[cfg(feature = "live")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_on_main_port_sits_behind_the_auth_layer() {
    let dir = TempDir::new().expect("a temp dir");
    let (key, line) = loams::auth::api_key::new_key("live");
    let keys = dir.path().join("api-keys");
    std::fs::write(&keys, format!("{line}\n")).expect("the key file");
    let key_id = line.split_whitespace().next().expect("an id").to_string();
    let mut config = config(&dir, true);
    config.auth = loams::auth::AuthConfig::api_keys(&keys);
    let server = Server::start(config).await.expect("starts");
    let main = server.local_addr();
    for path in [
        "/loams.live.v1.LiveAdminService/Deploy",
        "/loams.live.v1.LiveAdminService/ListDeployments",
    ] {
        let (status, reason, body) = call(main, path, &json!({}), &[]).await;
        assert_eq!(status, 401, "{path}: {body}");
        assert_eq!(reason.as_deref(), Some("token_missing"), "{path}");
    }
    // LiveService leaves a call without a credential to the app (LV1 plan
    // Task 16), which takes no anonymous calls: Live's own refusal.
    for path in [
        "/loams.live.v1.LiveService/Query",
        "/loams.live.v1.LiveService/Mutate",
    ] {
        let (status, _, body) = call(main, path, &json!({"function": "notes:ping"}), &[]).await;
        assert_eq!(status, 401, "{path}: {body}");
        assert_eq!(reason(&body), "token_missing", "{path}");
    }
    let bearer = format!("Bearer {key}");
    let (status, deployed) = deploy(main, &[("authorization", &bearer)]).await;
    assert_eq!(status, 200, "{deployed}");
    assert_eq!(
        deployed["deployment"]["createdBy"],
        format!("apikey:{key_id}")
    );
    assert!(
        !deployed.to_string().contains(&key),
        "the key never comes back"
    );
    server.shutdown().await.expect("stops");
}

/// A stand-in OIDC issuer for an app's end users: discovery and a JWKS with
/// one Ed25519 key, on loopback.
#[cfg(feature = "live")]
struct EndUserIssuer {
    url: String,
    signer: jsonwebtoken::EncodingKey,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "live")]
impl EndUserIssuer {
    async fn start() -> Self {
        use base64::Engine as _;
        use ring::signature::KeyPair as _;
        let listener = tokio::net::TcpListener::bind(LOOPBACK0)
            .await
            .expect("test fixture");
        let url = format!(
            "http://{}/o/shop/",
            listener.local_addr().expect("test fixture")
        );
        let pkcs8 =
            ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
                .expect("test fixture");
        let pair =
            ring::signature::Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("test fixture");
        let x = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pair.public_key().as_ref());
        let jwks = json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "x": x, "kid": "k1", "alg": "EdDSA"}]});
        let discovery = json!({"issuer": url, "jwks_uri": format!("{url}jwks")});
        let app = axum::Router::new()
            .route(
                "/o/shop/.well-known/openid-configuration",
                axum::routing::get(move || async move { axum::Json(discovery) }),
            )
            .route(
                "/o/shop/jwks",
                axum::routing::get(move || async move { axum::Json(jwks) }),
            );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test fixture");
        });
        Self {
            url,
            signer: jsonwebtoken::EncodingKey::from_ed_der(pkcs8.as_ref()),
            task,
        }
    }

    fn token(&self, sub: &str) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test fixture")
            .as_secs();
        let claims = json!({"iss": self.url, "aud": "shop-web", "sub": sub, "exp": now + 300});
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
        header.kid = Some("k1".into());
        jsonwebtoken::encode(&header, &claims, &self.signer).expect("test fixture")
    }
}

#[cfg(feature = "live")]
impl Drop for EndUserIssuer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// LV1 plan Task 16 through the engine's layer: on an engine that takes only
/// API keys, an app end user's token from the app's issuer reaches
/// `LiveService` and calls a public function; a token from another issuer
/// is refused by Live; the admin service and a malformed bearer stay the
/// engine's to refuse.
#[cfg(feature = "live")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_end_users_reach_live_through_the_engine_layer() {
    use base64::Engine as _;
    let dir = TempDir::new().expect("a temp dir");
    let (key, line) = loams::auth::api_key::new_key("live");
    let keys = dir.path().join("api-keys");
    std::fs::write(&keys, format!("{line}\n")).expect("the key file");
    let mut config = config(&dir, true);
    config.auth = loams::auth::AuthConfig::api_keys(&keys);
    let server = Server::start(config).await.expect("starts");
    let main = server.local_addr();
    let issuer = EndUserIssuer::start().await;
    let stranger = EndUserIssuer::start().await;
    let bearer = format!("Bearer {key}");
    let (status, _, body) = call(
        main,
        "/loams.live.v1.LiveAdminService/CreateApp",
        &json!({"name": "shop", "issuers": [{"issuer": issuer.url, "audiences": ["shop-web"]}]}),
        &[("authorization", &bearer)],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _, body) = call(
        main,
        "/loams.live.v1.LiveAdminService/Deploy",
        &json!({"bundle": base64::engine::general_purpose::STANDARD.encode(BUNDLE)}),
        &[("authorization", &bearer), ("loams-live-app", "shop")],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let ping = json!({"function": "notes:ping"});
    let end_user = format!("Bearer {}", issuer.token("ada"));
    let as_end_user = [
        ("authorization", end_user.as_str()),
        ("loams-live-app", "shop"),
    ];
    let (status, _, body) = call(
        main,
        "/loams.live.v1.LiveService/Query",
        &ping,
        &as_end_user,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let other = format!("Bearer {}", stranger.token("ada"));
    let (status, _, body) = call(
        main,
        "/loams.live.v1.LiveService/Query",
        &ping,
        &[("authorization", &other), ("loams-live-app", "shop")],
    )
    .await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(reason(&body), "token_wrong_issuer");
    let (status, header, body) = call(
        main,
        "/loams.live.v1.LiveAdminService/ListDeployments",
        &json!({}),
        &as_end_user,
    )
    .await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(header.as_deref(), Some("credential_not_accepted"));
    let (status, header, _) = call(
        main,
        "/loams.live.v1.LiveService/Query",
        &ping,
        &[
            ("authorization", "Bearer not-a-token"),
            ("loams-live-app", "shop"),
        ],
    )
    .await;
    assert_eq!(status, 401);
    assert_eq!(header.as_deref(), Some("token_malformed"));
    server.shutdown().await.expect("stops");
}

/// In tailscale mode Live is reached over the tailnet: the `tailscale serve`
/// target (loopback, TLS from the tailnet) serves it and names the tailnet
/// user as the deployer. A main port bound beyond loopback does not serve
/// it until LV1 Task 18 (TLS): there the package is unavailable and its
/// calls answer `feature_not_in_variant`, even with a valid credential.
#[cfg(feature = "live")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_over_the_tailnet_but_not_on_a_non_loopback_main_port() {
    use loams::auth::tailscale::TailscaleConfig;
    use loams::auth::{AuthConfig, AuthMode};

    let dir = TempDir::new().expect("a temp dir");
    let (key, line) = loams::auth::api_key::new_key("live");
    let keys = dir.path().join("api-keys");
    std::fs::write(&keys, format!("{line}\n")).expect("the key file");
    let mut config = config(&dir, true);
    config.listen = SocketAddr::from(([0, 0, 0, 0], 0));
    config.auth = AuthConfig {
        mode: AuthMode::On {
            api_keys: true,
            oidc: false,
            tailscale: true,
        },
        tailscale: Some(TailscaleConfig {
            listen: LOOPBACK0,
            allowed_users: ["me@example.com".to_owned()].into(),
        }),
        ..AuthConfig::api_keys(&keys)
    };
    let server = Server::start(config).await.expect("starts");
    let target = server.tailscale_addr().expect("the tailscale target");
    let tailnet = [
        ("tailscale-user-login", "me@example.com"),
        ("tailscale-user-name", "Me"),
    ];
    let (available, _, row) = live_row(target, &tailnet).await;
    assert!(available, "{row}");
    let (status, deployed) = deploy(target, &tailnet).await;
    assert_eq!(status, 200, "{deployed}");
    assert_eq!(
        deployed["deployment"]["createdBy"],
        "tailscale:me@example.com"
    );

    let main = SocketAddr::from(([127, 0, 0, 1], server.local_addr().port()));
    let bearer = format!("Bearer {key}");
    let with_key = [("authorization", bearer.as_str())];
    let (available, versioned, row) = live_row(main, &with_key).await;
    assert!(!available && !versioned, "{row}");
    let (status, _, error) = call(
        main,
        "/loams.live.v1.LiveAdminService/ListDeployments",
        &json!({}),
        &with_key,
    )
    .await;
    assert_eq!(status, 501, "{error}");
    assert_eq!(reason(&error), "feature_not_in_variant");
    server.shutdown().await.expect("stops");
}

/// `--live-listen` is deprecated (D688): `loams dev` says so on stderr and
/// still serves Live there, beside the main port.
#[cfg(feature = "live")]
#[test]
fn live_listen_flag_warns_and_serves() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let dir = TempDir::new().expect("a temp dir");
    let mut child = Command::new(env!("CARGO_BIN_EXE_loams"))
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
            "--es-listen",
            "127.0.0.1:0",
            "--no-qdrant",
            "--live-listen",
            "127.0.0.1:0",
            "--data-dir",
        ])
        .arg(dir.path())
        .env_remove("LOAMS_TEST_PD")
        .env_remove("LOAMS_AUTH_MODE")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn loams");
    let stderr = child.stderr.take().expect("stderr");
    let warned = std::thread::spawn(move || {
        BufReader::new(stderr)
            .lines()
            .map_while(Result::ok)
            .any(|line| {
                line.contains("--live-listen is deprecated; Live is served on --listen (D688)")
            })
    });
    let stdout = child.stdout.take().expect("stdout");
    let mut lines = BufReader::new(stdout).lines();
    let after = |line: &str, prefix: &str| -> Option<SocketAddr> {
        line.strip_prefix(prefix)?.trim().parse().ok()
    };
    let (mut http, mut live, mut seen) = (None, None, Vec::new());
    while http.is_none() {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("loams exited before listening; printed {seen:?}"))
            .expect("read stdout");
        live = live.or_else(|| after(&line, "loams live listening on http://"));
        http = after(&line, "loams listening on http://");
        seen.push(line);
    }
    std::thread::spawn(move || for _ in lines {});
    let live = live.unwrap_or_else(|| panic!("no Live line: {seen:?}"));
    let http = http.expect("the main port");
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    runtime.block_on(async {
        for addr in [live, http] {
            let (status, _, body) = call(
                addr,
                "/loams.live.v1.LiveService/Query",
                &json!({"function": "_system:tables", "args": {"objectValue": {}}}),
                &[],
            )
            .await;
            assert_eq!(status, 200, "{addr}: {body}");
        }
    });
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        warned.join().expect("the stderr reader"),
        "no deprecation warning"
    );
}
