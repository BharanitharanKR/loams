//! Authentication on the main port and `loams.auth.v1` (API2 Task 6, server
//! half), against the real server: the allowlist, console sessions as a
//! cookie and as a bearer, API keys kept in the metastore, refusals with
//! their reasons, credentials never logged, and the `Authenticator` hook.

mod console_support;

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use console_support::{
    Client, Engine, LOOPBACK0, OWNER_EMAIL, OWNER_PASSWORD, SETUP_TOKEN, TestAuthenticator,
    reason_of,
};
use loams::auth::{AuthConfig, PUBLIC_RPCS};
use loams::{Server, ServerConfig};
use serde_json::{Value, json};

/// Everything the engine logs at `debug` and above in this test binary.
fn captured_logs() -> Arc<Mutex<Vec<u8>>> {
    static LOGS: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let sink = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || Sink(sink.clone()))
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        logs
    })
    .clone()
}

struct Sink(Arc<Mutex<Vec<u8>>>);

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// An engine with API keys from a file (the operator's) and the setup token.
async fn engine_with_keys() -> (Engine, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("test fixture");
    let (key, line) = loams::auth::api_key::new_key("operator");
    let path = dir.path().join("api-keys");
    std::fs::write(&path, format!("{line}\n")).expect("test fixture");
    let engine = Engine::start(AuthConfig {
        setup_token: Some(SETUP_TOKEN.to_owned()),
        ..AuthConfig::api_keys(&path)
    })
    .await;
    (engine, key, dir)
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `(package, service) -> [(method, server_streaming)]` from `proto/loams`.
fn declared_rpcs() -> Vec<(String, String, String, bool)> {
    fn walk(dir: &Path, into: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("test fixture").flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, into);
            } else if path.extension().is_some_and(|e| e == "proto") {
                into.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&workspace().join("proto/loams"), &mut files);
    let mut rpcs = Vec::new();
    for file in files {
        let text: String = std::fs::read_to_string(&file)
            .expect("test fixture")
            .lines()
            .map(|l| l.split("//").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        let Some(package) = text
            .split("package ")
            .nth(1)
            .and_then(|rest| rest.split(';').next())
        else {
            continue;
        };
        let mut service = None::<String>;
        let mut depth = 0usize;
        let tokens: Vec<&str> = text.split_whitespace().collect();
        for (i, token) in tokens.iter().enumerate() {
            if *token == "service" && depth == 0 {
                service = tokens
                    .get(i + 1)
                    .map(|s| s.trim_end_matches('{').to_owned());
            }
            if *token == "rpc"
                && let Some(service) = &service
            {
                let rest = tokens[i + 1..].join(" ");
                let name = rest.split('(').next().unwrap_or_default().trim().to_owned();
                let returns = rest.split("returns").nth(1).unwrap_or_default();
                let streaming = returns
                    .trim_start()
                    .trim_start_matches('(')
                    .trim_start()
                    .starts_with("stream ");
                rpcs.push((package.trim().to_owned(), service.clone(), name, streaming));
            }
            depth += token.matches('{').count();
            let closes = token.matches('}').count();
            depth = depth.saturating_sub(closes);
            if closes > 0 && depth == 0 {
                service = None;
            }
        }
    }
    rpcs
}

/// Walks every RPC of every catalogue service (`GetInstance.services[]`,
/// with the protos for the methods) over Connect, gRPC and gRPC-Web with no
/// credential: only `PUBLIC_RPCS` answer, everything else is
/// `unauthenticated`.
#[tokio::test]
async fn every_served_rpc_requires_auth_except_allowlist() {
    let (engine, _key, _dir) = engine_with_keys().await;
    let instance = engine
        .client()
        .call("instance.v1.InstanceService", "GetInstance", json!({}))
        .await;
    let services: BTreeSet<String> = instance.ok()["services"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["services"].as_array().unwrap().iter())
        .map(|s| s.as_str().unwrap().to_owned())
        .collect();
    for wanted in [
        "loams.auth.v1.AuthService",
        "loams.admin.v1.OrgService",
        "loams.admin.v1.KeyService",
        "loams.admin.v1.AuditService",
    ] {
        assert!(services.contains(wanted), "{wanted} not in the catalogue");
    }
    // Only an engine that runs Loams Live leaves its calls to the app.
    let live_served = instance.ok()["services"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["package"] == "loams.live.v1" && s["available"] == true);
    let rpcs: Vec<_> = declared_rpcs()
        .into_iter()
        .filter(|(package, service, _, _)| services.contains(&format!("{package}.{service}")))
        .collect();
    assert!(rpcs.len() > 60, "only {} RPCs found", rpcs.len());
    let h2 = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let http = reqwest::Client::new();
    let mut checked = 0;
    for (package, service, method, streaming) in &rpcs {
        let path = format!("/{package}.{service}/{method}");
        let public = PUBLIC_RPCS.contains(&path.as_str());
        // LV1 plan Task 16: the layer leaves these to the addressed Live
        // app, which refuses a call with no credential itself.
        let left_to_app = live_served
            && loams::auth::layer::APP_END_USER_PATHS
                .iter()
                .any(|p| path.starts_with(p));
        let url = format!("{}{path}", engine.base);
        // Connect: JSON unary, or an enveloped stream.
        let (content_type, body) = if *streaming {
            let mut frame = vec![0u8, 0, 0, 0, 2];
            frame.extend_from_slice(b"{}");
            ("application/connect+json", frame)
        } else {
            ("application/json", b"{}".to_vec())
        };
        let response = http
            .post(&url)
            .header("content-type", content_type)
            .header("connect-protocol-version", "1")
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let reason = response
            .headers()
            .get("loams-auth-reason")
            .map(|v| v.to_str().unwrap().to_owned());
        if public {
            assert_ne!(status, 401, "{path} over Connect");
        } else if left_to_app {
            assert_eq!(reason, None, "{path}: Live's refusal, not the layer's");
            let text = response.text().await.unwrap();
            if *streaming {
                assert!(text.contains("unauthenticated"), "{path}: {text}");
            } else {
                assert_eq!(status, 401, "{path} over Connect");
                let body: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(reason_of(&body).as_deref(), Some("token_missing"), "{path}");
            }
        } else {
            assert_eq!(status, 401, "{path} over Connect");
            assert_eq!(reason.as_deref(), Some("token_missing"), "{path}");
            let body: Value = response.json().await.unwrap();
            assert_eq!(reason_of(&body).as_deref(), Some("token_missing"), "{path}");
        }
        // gRPC and gRPC-Web: an empty message.
        for (client, content_type) in [
            (&h2, "application/grpc"),
            (&http, "application/grpc-web+proto"),
        ] {
            let response = client
                .post(&url)
                .header("content-type", content_type)
                .header("te", "trailers")
                .header("x-grpc-web", "1")
                .body(vec![0u8, 0, 0, 0, 0])
                .send()
                .await
                .unwrap();
            let grpc_status = response
                .headers()
                .get("grpc-status")
                .map(|v| v.to_str().unwrap().to_owned());
            if public {
                assert_ne!(
                    grpc_status.as_deref(),
                    Some("16"),
                    "{path} over {content_type}"
                );
            } else {
                assert_eq!(
                    grpc_status.as_deref(),
                    Some("16"),
                    "{path} over {content_type}"
                );
            }
        }
        checked += 1;
    }
    assert_eq!(checked, rpcs.len());
    // Every allowlisted RPC that is in the catalogue was walked.
    for path in PUBLIC_RPCS.iter().filter(|p| !p.starts_with("/grpc.")) {
        assert!(
            rpcs.iter()
                .any(|(p, s, m, _)| format!("/{p}.{s}/{m}") == *path),
            "{path} was not walked"
        );
    }
    engine.stop().await;
}

/// An API key `CreateKey` makes is stored as its argon2id hash in the
/// metastore and admitted as a bearer until it is revoked.
#[tokio::test]
async fn api_key_round_trip() {
    let (engine, operator, _dir) = engine_with_keys().await;
    let mut owner = engine.client();
    owner.setup(SETUP_TOKEN).await;
    // The operator's file key is an owner too.
    let as_operator = engine.client().with_bearer(&operator);
    as_operator
        .call(
            "admin.v1.ProjectService",
            "CreateProject",
            json!({"name": "Docs search"}),
        )
        .await
        .ok();
    as_operator
        .call(
            "admin.v1.ProjectService",
            "CreateEnvironment",
            json!({"project": "docs-search", "name": "Production"}),
        )
        .await
        .ok();
    let created = owner
        .call(
            "admin.v1.KeyService",
            "CreateKey",
            json!({"project": "docs-search", "environment": "production", "name": "ci",
                   "idempotencyKey": "round-trip-1"}),
        )
        .await;
    let secret = created.ok()["secret"].as_str().unwrap().to_owned();
    let id = created.ok()["key"]["id"].as_str().unwrap().to_owned();
    assert!(
        secret.starts_with("lk_") && secret.contains(&id),
        "{secret}"
    );
    assert!(
        !created.ok()["key"].to_string().contains(&secret),
        "the key record holds no secret"
    );
    let key = engine.client().with_bearer(&secret);
    let who = key
        .call("instance.v1.InstanceService", "WhoAmI", json!({}))
        .await;
    assert_eq!(who.ok()["principal"]["id"], format!("apikey:{id}"));
    // It acts as its creator, an owner.
    key.call("admin.v1.ProjectService", "ListProjects", json!({}))
        .await
        .ok();
    // A key with the right id and a wrong secret is not.
    let mut wrong = secret.clone();
    let last = wrong.pop().unwrap();
    wrong.push(if last == 'a' { 'b' } else { 'a' });
    let refused = engine
        .client()
        .with_bearer(&wrong)
        .call("instance.v1.InstanceService", "WhoAmI", json!({}))
        .await;
    assert_eq!(
        refused.failure(),
        ("unauthenticated".into(), "api_key_invalid".into())
    );
    owner
        .call("admin.v1.KeyService", "RevokeKey", json!({"key": id}))
        .await
        .ok();
    let revoked = key
        .call("instance.v1.InstanceService", "WhoAmI", json!({}))
        .await;
    assert_eq!(
        revoked.failure(),
        ("unauthenticated".into(), "token_revoked".into())
    );
    assert!(
        revoked.headers["www-authenticate"]
            .to_str()
            .unwrap()
            .contains("token_revoked")
    );
    engine.stop().await;
}

/// A session is a cookie for the console (with the CSRF token on anything
/// but `GetSession`) and the same token as a bearer for anything else.
#[tokio::test]
async fn session_cookie_and_bearer_both_work() {
    let (engine, _key, _dir) = engine_with_keys().await;
    let mut console = engine.client();
    let setup = console.setup(SETUP_TOKEN).await;
    let cookie = setup.set_cookies().join("\n");
    for part in ["Path=/", "HttpOnly", "Secure", "SameSite=Lax"] {
        assert!(cookie.contains(part), "{cookie}");
    }
    assert_eq!(setup.ok()["session"]["role"], "ORG_ROLE_OWNER");
    // Setup runs once.
    let again = engine
        .client()
        .call(
            "auth.v1.AuthService",
            "CompleteSetup",
            json!({"setupToken": SETUP_TOKEN, "orgName": "B", "email": "b@example.test",
                   "password": "another password"}),
        )
        .await;
    assert_eq!(again.failure().0, "failed_precondition");
    // GetSession by cookie needs no CSRF token; a mutation does.
    let mut no_csrf = console.clone();
    no_csrf.send_csrf = false;
    let session = no_csrf
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    assert_eq!(session.ok()["session"]["user"]["email"], OWNER_EMAIL);
    let forged = no_csrf
        .call(
            "admin.v1.ProjectService",
            "CreateProject",
            json!({"name": "x"}),
        )
        .await;
    assert_eq!(
        forged.failure(),
        ("permission_denied".into(), "csrf_token_invalid".into())
    );
    console
        .call(
            "admin.v1.ProjectService",
            "CreateProject",
            json!({"name": "Docs"}),
        )
        .await
        .ok();
    // The same token as a bearer, no CSRF token.
    let (token, _) = console.session.clone().unwrap();
    let bearer = engine.client().with_bearer(&token);
    let who = bearer
        .call("instance.v1.InstanceService", "WhoAmI", json!({}))
        .await;
    assert_eq!(who.ok()["principal"]["kind"], "PRINCIPAL_KIND_USER");
    bearer
        .call("admin.v1.ProjectService", "ListProjects", json!({}))
        .await
        .ok();
    // Sign in again with the password: a second session.
    let wrong = engine
        .client()
        .call(
            "auth.v1.AuthService",
            "CreateSession",
            json!({"email": OWNER_EMAIL, "password": "not the password"}),
        )
        .await;
    assert_eq!(wrong.failure().0, "unauthenticated");
    let mut second = engine.client();
    let signed_in = second
        .call(
            "auth.v1.AuthService",
            "CreateSession",
            json!({"email": OWNER_EMAIL.to_uppercase(), "password": OWNER_PASSWORD}),
        )
        .await;
    second.keep_session(&signed_in);
    // Sign out: the cookie is cleared and the session ends; the other one
    // stays.
    let out = console
        .call("auth.v1.AuthService", "DeleteSession", json!({}))
        .await;
    out.ok();
    assert!(out.set_cookies().join("\n").contains("Max-Age=0"));
    let ended = console
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    assert_eq!(
        ended.failure(),
        ("unauthenticated".into(), "token_revoked".into())
    );
    second
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await
        .ok();
    // Signing out without a session is not an error.
    engine
        .client()
        .call("auth.v1.AuthService", "DeleteSession", json!({}))
        .await
        .ok();
    engine.stop().await;
}

/// `ListProviders` is answered without a credential; setup needs the token
/// when authentication is on.
#[tokio::test]
async fn list_providers_and_setup_need_no_credential() {
    let (engine, _key, _dir) = engine_with_keys().await;
    let anonymous = engine.client();
    let providers = anonymous
        .call("auth.v1.AuthService", "ListProviders", json!({}))
        .await;
    assert_eq!(providers.ok()["password"], true);
    let refused = anonymous
        .call(
            "auth.v1.AuthService",
            "CompleteSetup",
            json!({"setupToken": "wrong", "orgName": "A", "email": OWNER_EMAIL,
                   "password": OWNER_PASSWORD}),
        )
        .await;
    assert_eq!(refused.failure().0, "permission_denied");
    let unset = anonymous
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    assert_eq!(
        unset.failure(),
        ("unauthenticated".into(), "token_missing".into())
    );
    engine.stop().await;
}

/// `?access_token=` and `?token=` are refused, even on a public RPC.
#[tokio::test]
async fn query_string_token_refused() {
    let (engine, key, _dir) = engine_with_keys().await;
    for (path, param) in [
        ("loams.instance.v1.InstanceService/WhoAmI", "access_token"),
        ("loams.auth.v1.AuthService/ListProviders", "token"),
    ] {
        let response = reqwest::Client::new()
            .post(format!("{}/{path}?{param}={key}", engine.base))
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401, "{path}");
        let body: Value = response.json().await.unwrap();
        assert_eq!(
            reason_of(&body).as_deref(),
            Some("credential_in_query_string"),
            "{path}"
        );
        assert!(!body.to_string().contains(&key));
    }
    engine.stop().await;
}

/// No credential reaches a log line or an error body: an API key from the
/// file, a stored key, a session token, the setup token and a password, good
/// or bad, across every refusal this suite provokes.
#[tokio::test]
async fn bearer_never_logged() {
    let logs = captured_logs();
    let (engine, operator, _dir) = engine_with_keys().await;
    let mut owner = engine.client();
    owner.setup(SETUP_TOKEN).await;
    let as_operator = engine.client().with_bearer(&operator);
    as_operator
        .call(
            "admin.v1.ProjectService",
            "CreateProject",
            json!({"name": "Docs search"}),
        )
        .await
        .ok();
    as_operator
        .call(
            "admin.v1.ProjectService",
            "CreateEnvironment",
            json!({"project": "docs-search", "name": "Production"}),
        )
        .await
        .ok();
    let created = owner
        .call(
            "admin.v1.KeyService",
            "CreateKey",
            json!({"project": "docs-search", "environment": "production", "name": "ci",
                   "idempotencyKey": "never-logged"}),
        )
        .await;
    let stored = created.ok()["secret"].as_str().unwrap().to_owned();
    let (session, csrf) = owner.session.clone().unwrap();
    let mut bodies = Vec::new();
    let mut bad_key = stored.clone();
    bad_key.replace_range(bad_key.len() - 4.., "zzzz");
    let bad_session = format!("{}Zz", &session[..session.len() - 2]);
    for bearer in [
        operator.as_str(),
        stored.as_str(),
        session.as_str(),
        bad_key.as_str(),
        bad_session.as_str(),
        "not-a-credential-at-all",
        "a.b.c",
    ] {
        let client = engine.client().with_bearer(bearer);
        for (service, method) in [
            ("instance.v1.InstanceService", "WhoAmI"),
            ("admin.v1.OrgService", "GetOrg"),
            ("admin.v1.KeyService", "CreateKey"),
        ] {
            bodies.push(
                client
                    .call(service, method, json!({}))
                    .await
                    .body
                    .to_string(),
            );
        }
    }
    bodies.push(
        engine
            .client()
            .call(
                "auth.v1.AuthService",
                "CreateSession",
                json!({"email": OWNER_EMAIL, "password": "wrong-password-xyz"}),
            )
            .await
            .body
            .to_string(),
    );
    let logs = String::from_utf8_lossy(&logs.lock().unwrap()).into_owned();
    assert!(!logs.is_empty(), "the subscriber captured nothing");
    for secret in [
        operator.as_str(),
        stored.as_str(),
        session.as_str(),
        csrf.as_str(),
        bad_key.as_str(),
        bad_session.as_str(),
        SETUP_TOKEN,
        OWNER_PASSWORD,
        "wrong-password-xyz",
    ] {
        // The id half of a key is not secret; its random half is.
        let tail = &secret[secret.len().saturating_sub(16)..];
        assert!(!logs.contains(tail), "a credential reached the log");
        for body in &bodies {
            assert!(
                !body.contains(tail),
                "a credential reached an error body: {body}"
            );
        }
    }
    engine.stop().await;
}

/// The suite's flows pass with a test `Authenticator` injected next to the
/// built-in ones: a person it admits who was invited joins the org, and
/// every handler serves them unchanged (MT1 adds its own the same way).
#[tokio::test]
async fn authenticator_is_swappable() {
    let engine = Engine::guarded(vec![Arc::new(TestAuthenticator)]).await;
    let mut owner = engine.client();
    owner.setup(SETUP_TOKEN).await;
    let grace = engine
        .client()
        .with_bearer(&TestAuthenticator::token("grace"));
    // Not in the org yet.
    let outside = grace.call("admin.v1.OrgService", "GetOrg", json!({})).await;
    assert_eq!(
        outside.failure(),
        ("permission_denied".into(), "role_required".into())
    );
    owner
        .call(
            "admin.v1.OrgService",
            "CreateInvitation",
            json!({"email": "grace@example.test", "role": "ORG_ROLE_ADMIN"}),
        )
        .await
        .ok();
    let who = grace
        .call("instance.v1.InstanceService", "WhoAmI", json!({}))
        .await;
    assert_eq!(who.ok()["principal"]["email"], "grace@example.test");
    assert_eq!(
        grace
            .call("admin.v1.OrgService", "GetOrg", json!({}))
            .await
            .ok()["org"]["name"],
        "Acme"
    );
    grace
        .call(
            "admin.v1.ProjectService",
            "CreateProject",
            json!({"name": "Graph"}),
        )
        .await
        .ok();
    let members = owner
        .call("admin.v1.OrgService", "ListMembers", json!({}))
        .await;
    let roles: Vec<&str> = members.ok()["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["ORG_ROLE_OWNER", "ORG_ROLE_ADMIN"]);
    let invitations = owner
        .call("admin.v1.OrgService", "ListInvitations", json!({}))
        .await;
    assert!(invitations.ok().get("invitations").is_none(), "accepted");
    engine.stop().await;
}

/// Development mode refuses a listener that is not loopback (ruling 0.3:
/// `LOAMS_AUTH_MODE=off` is the plan's `dev` authenticator).
#[tokio::test]
async fn dev_loopback_refused_on_public_address() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = ServerConfig::new(dir.path());
    config.listen = "0.0.0.0:0".parse().unwrap();
    config.auth = AuthConfig::off();
    let err = Server::start(config).await.unwrap_err().to_string();
    assert!(err.contains("loopback"), "{err}");
    let _ = LOOPBACK0;
}

/// `LOAMS_AUTH_MODE=jwt` (the plan's `--auth jwt`) without an issuer does not
/// start, and says what is missing.
#[test]
fn jwt_without_issuer_refused_at_startup() {
    let err = AuthConfig::from_vars(|name| (name == "LOAMS_AUTH_MODE").then(|| "jwt".to_owned()))
        .unwrap_err();
    assert!(err.contains("LOAMS_OIDC_ISSUER"), "{err}");
}

#[tokio::test]
#[ignore = "the OIDC start and callback endpoints are served only with MT1's issuer, which has \
            not merged (API2-Q1, ruling 0.3); without it they are absent, not stubbed"]
async fn oidc_start_remains_http_redirect() {}

/// The hosted console signs in elsewhere (an OIDC token, an API key, the
/// tailnet) and reads who it is from `GetSession` (API2 Task 10, rulings
/// 10.4 and 10.5): a bearer caller gets its person and org role, with no
/// CSRF token; someone outside the org gets no role. `GetInstance` says
/// first-run setup is due until it has run.
#[tokio::test]
async fn get_session_names_a_bearer_caller() {
    let engine = Engine::guarded(vec![Arc::new(TestAuthenticator)]).await;
    let anonymous = engine.client();
    let instance = |client: Client| async move {
        client
            .call("instance.v1.InstanceService", "GetInstance", json!({}))
            .await
            .ok()
            .clone()
    };
    assert_eq!(instance(anonymous.clone()).await["setupRequired"], true);
    let grace = engine
        .client()
        .with_bearer(&TestAuthenticator::token("grace"));
    let before = grace
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    let session = &before.ok()["session"];
    assert_eq!(session["user"]["email"], "grace@example.test");
    assert!(session.get("role").is_none(), "{session}");
    assert!(session.get("csrfToken").is_none(), "{session}");
    let mut owner = engine.client();
    owner.setup(SETUP_TOKEN).await;
    assert!(
        instance(anonymous.clone())
            .await
            .get("setupRequired")
            .is_none()
    );
    let ada = engine
        .client()
        .with_bearer(&TestAuthenticator::token("ada"));
    let answer = ada
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    let session = &answer.ok()["session"];
    assert_eq!(session["user"]["email"], OWNER_EMAIL);
    assert_eq!(session["user"]["name"], "Ada Admin");
    assert_eq!(session["role"], "ORG_ROLE_OWNER");
    assert_eq!(session["org"]["name"], "Acme");
    assert!(session.get("csrfToken").is_none(), "{session}");
    // Outside the org: named, but no role.
    let outside = grace
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    assert!(outside.ok()["session"].get("role").is_none());
    // Still no answer without a credential.
    assert_eq!(
        anonymous
            .call("auth.v1.AuthService", "GetSession", json!({}))
            .await
            .failure(),
        ("unauthenticated".into(), "token_missing".into())
    );
    engine.stop().await;
}

/// An operator's key from `LOAMS_API_KEYS_FILE` (the hosted console's
/// `apikey` mode) is the owner in `GetSession`.
#[tokio::test]
async fn get_session_names_a_key_file_key_the_owner() {
    let (engine, key, _dir) = engine_with_keys().await;
    engine.client().setup(SETUP_TOKEN).await;
    let answer = engine
        .client()
        .with_bearer(&key)
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    let session = &answer.ok()["session"];
    assert_eq!(session["role"], "ORG_ROLE_OWNER");
    assert_eq!(session["org"]["name"], "Acme");
    assert!(session.get("csrfToken").is_none(), "{session}");
    engine.stop().await;
}

/// With authentication on and no `LOAMS_SETUP_TOKEN` nobody can run setup,
/// so `GetInstance` does not send the console to it.
#[tokio::test]
async fn setup_is_not_required_when_it_cannot_run() {
    let engine = Engine::start(AuthConfig {
        extra_authenticators: loams::auth::Authenticators(vec![Arc::new(TestAuthenticator)]),
        ..AuthConfig::off()
    })
    .await;
    let instance = engine
        .client()
        .call("instance.v1.InstanceService", "GetInstance", json!({}))
        .await;
    assert!(instance.ok().get("setupRequired").is_none());
    engine.stop().await;
}

/// With authentication off the developer on loopback is the owner, and
/// setup is due until it has run.
#[tokio::test]
async fn dev_loopback_session_is_the_owner() {
    let engine = Engine::dev().await;
    let anonymous = engine.client();
    let instance = anonymous
        .call("instance.v1.InstanceService", "GetInstance", json!({}))
        .await;
    assert_eq!(instance.ok()["setupRequired"], true);
    let answer = anonymous
        .call("auth.v1.AuthService", "GetSession", json!({}))
        .await;
    assert_eq!(answer.ok()["session"]["role"], "ORG_ROLE_OWNER");
    engine.stop().await;
}
