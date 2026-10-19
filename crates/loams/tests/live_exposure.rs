//! Loams Live beyond loopback (LV1 plan Task 18, design §45 §6.1 and §6.4):
//! a non-loopback `--listen` serves Live only behind TLS (here, or ended at
//! a trusted proxy) and authentication; browsers get CORS for the union of
//! the active apps' origins, and each actual request is held to the
//! addressed app's own list. `Access-Control-Allow-Credentials` is never
//! sent.
#![cfg(feature = "live")]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use loams::auth::AuthConfig;
use loams::tls::{IpNet, TlsConfig};
use loams::{Server, ServerConfig};
use serde_json::{Value, json};
use tempfile::TempDir;

const LOOPBACK0: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
const ANY0: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);

const BUNDLE: &str = r#"
import { query } from "loams:server";
export const notes = { ping: query(async () => "pong") };
"#;

const CONSOLE: &str = "https://console.example";
const SHOP: &str = "https://shop.example";
const BLOG: &str = "https://blog.example";

/// A server on `listen` with Live on an embedded store, taking the API key
/// it returns.
fn config(dir: &TempDir, listen: SocketAddr) -> (ServerConfig, String) {
    let mut config = ServerConfig::new(dir.path());
    config.listen = listen;
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    let store = loams_live::EmbeddedConfig::new(
        dir.path().join("live").join("store.redb"),
        loams_live::keyspace_of("t18"),
    );
    let mut live =
        loams_live::LiveConfig::with_store("t18", loams_live::StoreConfig::Embedded(store));
    live.listen = LOOPBACK0;
    config.live = Some(live);
    let (key, line) = loams::auth::api_key::new_key("live");
    let keys = dir.path().join("api-keys");
    std::fs::write(&keys, format!("{line}\n")).expect("the key file");
    config.auth = AuthConfig::api_keys(&keys);
    (config, key)
}

/// A self-signed certificate for `localhost` and 127.0.0.1, written to
/// `dir`: the certificate's and the key's paths.
fn self_signed(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf, String) {
    let made =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("a certificate");
    let cert = dir.join("main.crt");
    let key = dir.join("main.key");
    std::fs::write(&cert, made.cert.pem()).expect("write");
    std::fs::write(&key, made.signing_key.serialize_pem()).expect("write");
    (cert, key, made.cert.pem())
}

/// A client trusting `pem`, offering HTTP/2 and HTTP/1.1 by ALPN.
fn tls_client(pem: &str) -> reqwest::Client {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject as _;
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(pem.as_bytes()).expect("pem"))
        .expect("a root");
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .build()
        .expect("a client")
}

/// A Connect unary call over JSON with `client` to `url`: the status, the
/// response headers and the body.
async fn call(
    client: &reqwest::Client,
    url: &str,
    body: &Value,
    headers: &[(&str, &str)],
) -> (u16, reqwest::header::HeaderMap, Value) {
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .body(body.to_string());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("a call");
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let text = response.text().await.expect("a body");
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, headers, body)
}

/// The reason of a Connect error's first `ErrorInfo` detail.
fn reason(error: &Value) -> String {
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

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).map(|v| v.to_str().expect("text"))
}

/// The start's error, as text.
async fn refused(config: ServerConfig) -> String {
    match Server::start(config).await {
        Ok(server) => {
            server.shutdown().await.expect("stops");
            panic!("started");
        }
        Err(err) => err.to_string(),
    }
}

/// A non-loopback `--listen` with Live is refused without TLS (even with
/// authentication), and without authentication (even with TLS); `--no-live`
/// leaves the rest of the engine as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_loopback_without_tls_and_auth_refused() {
    let dir = TempDir::new().expect("a temp dir");
    let (config, _) = config(&dir, ANY0);
    let err = refused(config).await;
    assert!(
        err.contains(
            "--listen listen on 0.0.0.0:0: non-loopback addresses need [tls] and [auth] (MT1)"
        ),
        "{err}"
    );
    assert!(err.contains("--no-live"), "{err}");

    let (cert, key, _) = self_signed(dir.path());
    let (mut config, _) = self::config(&dir, ANY0);
    config.tls = TlsConfig::files(&cert, &key);
    config.auth = AuthConfig::off();
    let err = refused(config).await;
    assert!(err.contains("not a loopback address"), "{err}");

    let (mut config, _) = self::config(&dir, ANY0);
    config.live = None;
    let server = Server::start(config).await.expect("starts without Live");
    server.shutdown().await.expect("stops");
}

/// With TLS and API keys, a non-loopback `--listen` serves Live over HTTPS
/// (HTTP/2 by ALPN), and plain HTTP gets no answer there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_loopback_with_tls_and_auth_starts() {
    let dir = TempDir::new().expect("a temp dir");
    let (cert, key_file, pem) = self_signed(dir.path());
    let (mut config, key) = config(&dir, ANY0);
    config.tls = TlsConfig::files(&cert, &key_file);
    let server = Server::start(config).await.expect("starts");
    let port = server.local_addr().port();
    let client = tls_client(&pem);
    let base = format!("https://127.0.0.1:{port}");
    let bearer = format!("Bearer {key}");
    let with_key = [("authorization", bearer.as_str())];

    let (status, _, info) = call(
        &client,
        &format!("{base}/loams.instance.v1.InstanceService/GetInstance"),
        &json!({}),
        &with_key,
    )
    .await;
    assert_eq!(status, 200, "{info}");
    let live = info["services"]
        .as_array()
        .and_then(|l| l.iter().find(|s| s["package"] == "loams.live.v1"))
        .expect("a loams.live.v1 row");
    assert_eq!(live["available"], true, "{live}");

    let (status, _, deployed) = call(
        &client,
        &format!("{base}/loams.live.v1.LiveAdminService/Deploy"),
        &json!({"bundle": base64::engine::general_purpose::STANDARD.encode(BUNDLE)}),
        &with_key,
    )
    .await;
    assert_eq!(status, 200, "{deployed}");
    let response = client
        .post(format!("{base}/loams.live.v1.LiveService/Query"))
        .header("content-type", "application/json")
        .header("authorization", &bearer)
        .body(json!({"function": "notes:ping"}).to_string())
        .send()
        .await
        .expect("a query");
    assert_eq!(response.version(), reqwest::Version::HTTP_2, "ALPN h2");
    let answer: Value = response.json().await.expect("json");
    assert_eq!(answer["result"]["stringValue"], "pong", "{answer}");

    let plain = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/loams.live.v1.LiveService/Query"
        ))
        .timeout(Duration::from_secs(15))
        .body("{}")
        .send()
        .await;
    assert!(
        plain.is_err() || !plain.expect("checked").status().is_success(),
        "plain HTTP is not served on a TLS port"
    );
    server.shutdown().await.expect("stops");
}

/// TLS ended at a proxy: plain connections from the trusted proxies only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminated_by_proxy_takes_trusted_peers_only() {
    for (proxies, served) in [("127.0.0.0/8", true), ("10.0.0.0/8", false)] {
        let dir = TempDir::new().expect("a temp dir");
        let (mut config, key) = config(&dir, ANY0);
        config.tls = TlsConfig::behind_proxy(vec![IpNet::parse(proxies).expect("a block")]);
        let server = Server::start(config).await.expect("starts");
        let port = server.local_addr().port();
        let bearer = format!("Bearer {key}");
        let sent = reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{port}/loams.instance.v1.InstanceService/GetInstance"
            ))
            .header("content-type", "application/json")
            .header("authorization", &bearer)
            .timeout(Duration::from_secs(15))
            .body("{}")
            .send()
            .await;
        match served {
            true => assert_eq!(sent.expect("served").status().as_u16(), 200, "{proxies}"),
            false => assert!(sent.is_err(), "{proxies}: an untrusted peer is closed"),
        }
        server.shutdown().await.expect("stops");
    }
}

/// An engine (API keys, the console at [`CONSOLE`]) with two apps, `shop`
/// (allowing [`SHOP`]) and `blog` (allowing [`BLOG`]), each with
/// [`BUNDLE`] deployed: the server, its base URL and the key.
async fn two_apps(dir: &TempDir) -> (Server, String, String) {
    let (mut config, key) = config(dir, LOOPBACK0);
    config.auth.cors_origins = vec![CONSOLE.to_owned()];
    let server = Server::start(config).await.expect("starts");
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::new();
    let bearer = format!("Bearer {key}");
    for (app, origin) in [("shop", SHOP), ("blog", BLOG)] {
        let (status, _, body) = call(
            &client,
            &format!("{base}/loams.live.v1.LiveAdminService/CreateApp"),
            &json!({"name": app, "allowedOrigins": [origin]}),
            &[("authorization", &bearer)],
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let (status, _, body) = call(
            &client,
            &format!("{base}/loams.live.v1.LiveAdminService/Deploy"),
            &json!({"bundle": base64::engine::general_purpose::STANDARD.encode(BUNDLE)}),
            &[("authorization", &bearer), ("loams-live-app", app)],
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    (server, base, key)
}

async fn preflight(base: &str, path: &str, origin: &str) -> reqwest::Response {
    reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("{base}{path}"))
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization, content-type, loams-live-app",
        )
        .send()
        .await
        .expect("a preflight")
}

/// A preflight on `LiveService` is answered for every active app's origin
/// (it cannot name the app) and the console's; `LiveAdminService` only for
/// the console's; any other origin is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cors_preflight_allows_union_of_origins() {
    let dir = TempDir::new().expect("a temp dir");
    let (server, base, _) = two_apps(&dir).await;
    let query = "/loams.live.v1.LiveService/Query";
    let admin = "/loams.live.v1.LiveAdminService/ListDeployments";
    for (path, origin, allowed) in [
        (query, SHOP, true),
        (query, BLOG, true),
        (query, CONSOLE, true),
        (query, "https://evil.example", false),
        (admin, SHOP, false),
        (admin, CONSOLE, true),
    ] {
        let response = preflight(&base, path, origin).await;
        let headers = response.headers();
        match allowed {
            true => {
                assert_eq!(response.status().as_u16(), 204, "{path} {origin}");
                assert_eq!(
                    header(headers, "access-control-allow-origin"),
                    Some(origin),
                    "{path} {origin}"
                );
                let allow = header(headers, "access-control-allow-headers").unwrap_or_default();
                assert!(allow.contains("loams-live-app"), "{allow}");
            }
            false => {
                assert_eq!(response.status().as_u16(), 403, "{path} {origin}");
                assert_eq!(header(headers, "access-control-allow-origin"), None);
            }
        }
        assert_eq!(header(headers, "access-control-allow-credentials"), None);
    }
    server.shutdown().await.expect("stops");
}

/// The actual request names its app, and is held to that app's origins:
/// `blog`'s origin cannot call `shop` (`live_origin_not_allowed`), while
/// `shop`'s can, and the console's can with a platform credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cors_actual_request_checked_per_app() {
    let dir = TempDir::new().expect("a temp dir");
    let (server, base, key) = two_apps(&dir).await;
    let client = reqwest::Client::new();
    let bearer = format!("Bearer {key}");
    let url = format!("{base}/loams.live.v1.LiveService/Query");
    let ping = json!({"function": "notes:ping"});
    for (origin, ok) in [(SHOP, true), (CONSOLE, true), (BLOG, false)] {
        let (status, headers, body) = call(
            &client,
            &url,
            &ping,
            &[
                ("authorization", &bearer),
                ("loams-live-app", "shop"),
                ("origin", origin),
            ],
        )
        .await;
        match ok {
            true => {
                assert_eq!(status, 200, "{origin}: {body}");
                assert_eq!(body["result"]["stringValue"], "pong");
            }
            false => {
                assert_eq!(status, 403, "{origin}: {body}");
                assert_eq!(reason(&body), "live_origin_not_allowed");
            }
        }
        assert_eq!(
            header(&headers, "access-control-allow-origin"),
            Some(origin),
            "{origin}: the browser may read the answer"
        );
        assert_eq!(header(&headers, "access-control-allow-credentials"), None);
    }
    // An origin no app lists gets no CORS at all, and is refused by Live.
    let (status, headers, body) = call(
        &client,
        &url,
        &ping,
        &[
            ("authorization", &bearer),
            ("loams-live-app", "shop"),
            ("origin", "https://evil.example"),
        ],
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(header(&headers, "access-control-allow-origin"), None);
    // Settings changes reach the check: `shop` now allows `blog`'s origin.
    let (status, _, body) = call(
        &client,
        &format!("{base}/loams.live.v1.LiveAdminService/UpdateApp"),
        &json!({"name": "shop", "allowedOrigins": {"origins": [SHOP, BLOG]}}),
        &[("authorization", &bearer)],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _, body) = call(
        &client,
        &url,
        &ping,
        &[
            ("authorization", &bearer),
            ("loams-live-app", "shop"),
            ("origin", BLOG),
        ],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    server.shutdown().await.expect("stops");
}

/// The engine's own origin is the console's too (0.1.0 release e2e, B11):
/// the desktop and the engine-served console call Live from the origin the
/// engine itself answers on, which no app lists and `LOAMS_CORS_ORIGINS`
/// need not. A platform principal from it is admitted; an app end user from
/// it is not, and a look-alike origin on another port is not the engine's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_engines_own_origin_is_admitted_for_platform_principals() {
    let dir = TempDir::new().expect("a temp dir");
    let (server, base, key) = two_apps(&dir).await;
    let client = reqwest::Client::new();
    let bearer = format!("Bearer {key}");
    let url = format!("{base}/loams.live.v1.LiveService/Query");
    let ping = json!({"function": "notes:ping"});
    let (status, headers, body) = call(
        &client,
        &url,
        &ping,
        &[
            ("authorization", &bearer),
            ("loams-live-app", "shop"),
            ("origin", &base),
        ],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"]["stringValue"], "pong");
    assert_eq!(header(&headers, "access-control-allow-credentials"), None);
    // Without a platform credential the engine's origin earns nothing.
    let (status, _, body) = call(
        &client,
        &url,
        &ping,
        &[("loams-live-app", "shop"), ("origin", &base)],
    )
    .await;
    assert_ne!(status, 200, "{body}");
    // Another port on the same host is another origin.
    let port = server.local_addr().port();
    let other = format!("http://127.0.0.1:{}", port.wrapping_add(1));
    let (status, headers, body) = call(
        &client,
        &url,
        &ping,
        &[
            ("authorization", &bearer),
            ("loams-live-app", "shop"),
            ("origin", &other),
        ],
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(reason(&body), "live_origin_not_allowed");
    assert_eq!(header(&headers, "access-control-allow-origin"), None);
    server.shutdown().await.expect("stops");
}

/// No answer, preflight or actual, allowed or refused, carries
/// `Access-Control-Allow-Credentials`: credentials are bearer headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_allow_credentials_header() {
    let dir = TempDir::new().expect("a temp dir");
    let (server, base, key) = two_apps(&dir).await;
    let client = reqwest::Client::new();
    let bearer = format!("Bearer {key}");
    let mut answers = Vec::new();
    for origin in [SHOP, BLOG, CONSOLE, "https://evil.example"] {
        for path in [
            "/loams.live.v1.LiveService/Query",
            "/loams.live.v1.LiveAdminService/ListDeployments",
        ] {
            answers.push(preflight(&base, path, origin).await.headers().clone());
            for auth in [Some(bearer.as_str()), None] {
                let mut headers = vec![("loams-live-app", "shop"), ("origin", origin)];
                if let Some(auth) = auth {
                    headers.push(("authorization", auth));
                }
                let (_, h, _) = call(
                    &client,
                    &format!("{base}{path}"),
                    &json!({"function": "notes:ping"}),
                    &headers,
                )
                .await;
                answers.push(h);
            }
        }
    }
    for headers in &answers {
        assert_eq!(
            header(headers, "access-control-allow-credentials"),
            None,
            "{headers:?}"
        );
    }
    server.shutdown().await.expect("stops");
}
