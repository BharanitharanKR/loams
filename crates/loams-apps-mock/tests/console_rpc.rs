//! The console API on Connect (`loams.auth.v1`, `loams.admin.v1`; API2 Tasks
//! 6 and 7, the mock half), over real sockets.
//!
//! Each console route test of `loams-console-mock` (`tests/contract.rs`) and
//! of this crate (`tests/mock.rs`, the `console_*` tests) is ported here with
//! an `_rpc` suffix, and the REST and Connect surfaces are held to the same
//! answers: [`normalize`] reads both as plain JSON (keys without case or
//! underscores, enums without their prefix, numbers as strings, defaults
//! dropped), so a field one surface carries and the other does not is a
//! failure, and so is a different value.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode, Protocol};
use loams_apps_mock::proto::loams::admin::v1::__buffa::oneof::watch_audit_events_response::Event;
use loams_apps_mock::proto::loams::admin::v1::{AgentServiceClient, SuspendAgentRequest};
use loams_apps_mock::proto::loams::admin::v1::{
    AuditServiceClient, CreateKeyRequest, KeyServiceClient, ListAuditEventsRequest,
    ListKeysRequest, ListProjectsRequest, ProjectServiceClient, RevokeKeyRequest,
    WatchAuditEventsRequest,
};
use loams_apps_mock::proto::loams::auth::v1::{
    AuthServiceClient, DeleteSessionRequest, GetSessionRequest,
};
use loams_apps_mock::proto::loams::errors::v1::ErrorInfo;
use loams_apps_mock::proto::loams::instance::v1::{
    Edition, GetInstanceRequest, InstanceServiceClient,
};
use loams_apps_mock::{MockConfig, MockHandle, serve};
use loams_console_mock::{CONNECT_ONLY_RPCS, CONNECT_RPCS, Route, load_seed, routes};
use serde_json::{Value, json};

async fn start(signed_in: bool, heartbeat: Duration) -> MockHandle {
    serve(MockConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        heartbeat,
        console_signed_in: signed_in,
        ..MockConfig::default()
    })
    .await
    .unwrap()
}

fn config(mock: &MockHandle, protocol: Protocol) -> ClientConfig {
    ClientConfig::new(mock.url().parse().unwrap()).with_protocol(protocol)
}

fn http(protocol: Protocol) -> HttpClient {
    if protocol == Protocol::Grpc {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    }
}

fn reason(err: &ConnectError) -> String {
    use buffa::Message as _;
    let detail = err.details.first().expect("an ErrorInfo detail");
    assert_eq!(detail.type_url, "loams.errors.v1.ErrorInfo");
    let bytes = base64_decode(detail.value.as_deref().unwrap());
    ErrorInfo::decode_from_slice(&bytes).unwrap().reason
}

fn base64_decode(s: &str) -> Vec<u8> {
    // Unpadded standard alphabet (the Connect error-detail encoding).
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = ALPHABET.iter().position(|a| *a == c).unwrap() as u32;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    out
}

/// A Connect unary call with a JSON body: `Ok(answer)` or `Err((HTTP status,
/// Connect error body))`.
async fn rpc(base: &str, rpc: &str, request: &Value) -> Result<Value, (u16, Value)> {
    let response = reqwest::Client::new()
        .post(format!("{base}/{rpc}"))
        .header("content-type", "application/json")
        .json(request)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    let body: Value = if text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    };
    if status == 200 {
        Ok(body)
    } else {
        Err((status, body))
    }
}

/// A REST call: the status and the JSON body (`null` for an empty one).
async fn rest(base: &str, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let url = format!("{base}{path}");
    let request = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PATCH" => client.patch(url),
        "DELETE" => client.delete(url),
        other => panic!("no {other} in the console contract"),
    };
    let request = match body {
        Some(body) => request.json(body),
        None => request,
    };
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    let body = if text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap()
    };
    (status, body)
}

/// The enum prefixes of `loams.auth.v1` and `loams.admin.v1`, which proto3
/// JSON spells and the REST contract does not (`ORG_ROLE_OWNER` is `owner`).
const ENUM_PREFIXES: &[&str] = &[
    "ORG_ROLE_",
    "PROJECT_ROLE_",
    "SUBJECT_KIND_",
    "AGENT_STATUS_",
    "TOKEN_FLOW_",
    "AUDIT_OUTCOME_",
    "OIDC_PROVIDER_KIND_",
];

/// Either surface's JSON in one comparable form.
///
/// - Keys lose case and underscores: proto3 JSON writes `queries24h` and
///   `maxTokenTtlS` for the contract's `queries_24h` and `max_token_ttl_s`.
/// - An enum value loses its prefix and case.
/// - A number becomes its decimal string: proto3 JSON writes an `int64` as a
///   string.
/// - A default (`null`, `""`, `0`, `false`, `[]`, `{}`) is dropped: proto3
///   JSON omits defaults and the contract writes `null` for "none".
fn normalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter_map(|(key, value)| {
                    let value = normalize(value);
                    (!is_default(&value)).then(|| (key.replace('_', "").to_lowercase(), value))
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(normalize).collect()),
        Value::String(s) => {
            for prefix in ENUM_PREFIXES {
                if let Some(rest) = s.strip_prefix(prefix) {
                    return Value::String(rest.to_lowercase());
                }
            }
            Value::String(s.clone())
        }
        Value::Number(n) => Value::String(n.to_string()),
        other => other.clone(),
    }
}

/// Whether a REST body and an RPC answer say the same thing. Wrapped, so an
/// empty REST body (`204`) and an empty message (`{}`) are both nothing.
fn same(rest: &Value, connect: &Value) -> bool {
    normalize(&json!({ "body": rest })) == normalize(&json!({ "body": connect }))
}

fn is_default(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.is_empty() || s == "0",
        Value::Bool(b) => !b,
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
        Value::Number(_) => false,
    }
}

/// The comparison the tests below rest on can fail: a different enum, a
/// missing field, a different number and a renamed key all differ, while
/// proto3 JSON's spellings of the same record do not.
#[test]
fn normalize_tells_records_apart() {
    let rest = json!({"role": "owner", "member_count": 2, "idp_group": null,
                      "queries_24h": 5, "user": {"email": "a@b.dev"}});
    let connect = json!({"role": "ORG_ROLE_OWNER", "memberCount": 2,
                         "queries24h": "5", "user": {"email": "a@b.dev"}});
    assert!(same(&rest, &connect));
    for wrong in [
        json!({"role": "ORG_ROLE_ADMIN", "memberCount": 2, "queries24h": "5",
               "user": {"email": "a@b.dev"}}),
        json!({"role": "ORG_ROLE_OWNER", "memberCount": 2, "queries24h": "5"}),
        json!({"role": "ORG_ROLE_OWNER", "memberCount": 3, "queries24h": "5",
               "user": {"email": "a@b.dev"}}),
        json!({"role": "ORG_ROLE_OWNER", "members": 2, "queries24h": "5",
               "user": {"email": "a@b.dev"}}),
    ] {
        assert!(!same(&rest, &wrong), "{wrong} should differ");
    }
}

/// The values a route template's `{name}`s took in `path`.
fn params(template: &str, path: &str) -> Vec<(String, String)> {
    template
        .split('/')
        .zip(path.split('/'))
        .filter_map(|(t, p)| {
            t.strip_prefix('{')
                .and_then(|t| t.strip_suffix('}'))
                .map(|name| (name.to_owned(), p.to_owned()))
        })
        .collect()
}

/// The RPC a REST call maps to, its request, and the response field the
/// REST body is (`None` when the whole response is).
fn rpc_for(
    method: &str,
    template: &str,
    path: &str,
    query: &[(String, String)],
) -> Option<(&'static str, Value, Option<&'static str>)> {
    let rpc = CONNECT_RPCS
        .iter()
        .find(|(m, t, _)| *m == method && *t == template)
        .and_then(|(_, _, rpc)| *rpc)?;
    let p: serde_json::Map<String, Value> = params(template, path)
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect();
    let with = |extra: Value| {
        let mut request = Value::Object(p.clone());
        for (k, v) in extra.as_object().unwrap() {
            request[k] = v.clone();
        }
        request
    };
    let method_name = rpc.rsplit('/').next().unwrap();
    let (request, unwrap) = match method_name {
        "GetInstance" | "ListProviders" => return None,
        "CompleteSetup" => (
            json!({"org_name": "Acme Robotics", "name": "Priya Raman",
                   "email": "priya@acme.dev", "password": "correct horse"}),
            Some("session"),
        ),
        "GetSession" => (json!({}), Some("session")),
        "CreateSession" => (
            json!({"email": "priya@acme.dev", "password": "correct horse"}),
            Some("session"),
        ),
        "GetOrg" | "UpdateOrg" => (json!({}), Some("org")),
        "CreateInvitation" => (
            json!({"email": "new@acme.dev", "role": "ORG_ROLE_MEMBER"}),
            Some("invitation"),
        ),
        "CreateTeam" => (json!({"name": "New team"}), Some("team")),
        "AddTeamMember" => (with(json!({"user": "usr_02"})), None),
        "CreateProject" => (json!({"name": "New project"}), Some("project")),
        "GetProject" => (with(json!({})), Some("project")),
        "GrantAccess" => (
            with(
                json!({"subject": {"kind": "SUBJECT_KIND_TEAM", "id": "team_platform"},
                        "role": "PROJECT_ROLE_VIEWER"}),
            ),
            Some("grant"),
        ),
        "CreateEnvironment" => (with(json!({"name": "Preview"})), Some("environment")),
        "GetEnvironment" => (with(json!({})), Some("environment")),
        "CreateAgent" => (
            with(json!({"name": "new-agent",
                        "owner": {"kind": "SUBJECT_KIND_USER", "id": "usr_01"}})),
            Some("agent"),
        ),
        "GetAgent" | "SuspendAgent" | "ResumeAgent" => (with(json!({})), Some("agent")),
        "UpdateAgent" => (
            json!({"agent": {"id": p.get("agent").cloned().unwrap_or_default()}}),
            Some("agent"),
        ),
        "CreateTrustPolicy" => (
            with(json!({"name": "New trust policy",
                        "issuer": "https://token.actions.githubusercontent.com",
                        "audience": "https://loams.acme.dev",
                        "subject_pattern": "repo:acme/docs:*",
                        "environments": ["staging"]})),
            Some("trust_policy"),
        ),
        "CreateServiceAccount" => (
            with(json!({"name": "new-service-account"})),
            Some("service_account"),
        ),
        "CreateKey" => (
            with(json!({"name": "new key", "scopes": ["query"], "idempotency_key": path})),
            None,
        ),
        "ListAuditEvents" => {
            let mut request = json!({});
            for (k, v) in query {
                request[k] = Value::String(v.clone());
            }
            (request, None)
        }
        "DecideConsent" => (
            json!({"decision": "CONSENT_DECISION_ALLOW", "client_id": "claude-code",
                   "redirect_uri": "http://127.0.0.1:33418/callback", "state": "mock",
                   "code_challenge": "mock", "code_challenge_method": "S256"}),
            None,
        ),
        // Lists, removals and revocations take exactly the path's names.
        _ => (with(json!({})), None),
    };
    Some((rpc, request, unwrap))
}

/// `body[key]` when the RPC wraps the resource, else the body.
fn unwrapped(body: Value, unwrap: Option<&str>) -> Value {
    // proto3 JSON writes the field's lowerCamelCase name (`serviceAccount`).
    let wanted = |k: &str| k.replace('_', "").to_lowercase();
    match unwrap {
        Some(key) => body
            .as_object()
            .and_then(|map| map.iter().find(|(k, _)| wanted(k) == wanted(key)))
            .map_or(Value::Null, |(_, v)| v.clone()),
        None => body,
    }
}

/// Port of `loams-console-mock`'s
/// `every_operation_has_a_mock_and_every_mock_an_operation`: every RPC that
/// replaces a contract operation, and the two Connect-only ones, is served
/// by the mock; and the check can fail (an unknown procedure does).
#[tokio::test]
async fn every_operation_has_a_mock_and_every_mock_an_operation_rpc() {
    let mock = start(true, Duration::from_secs(15)).await;
    let base = mock.url();
    // An RPC is served when its answer is not the router's for an unknown
    // procedure. A served RPC may still refuse `{}` (a missing name is
    // `invalid_argument`, an unknown one `not_found`), and that is fine.
    let served = |answer: &Result<Value, (u16, Value)>| match answer {
        Ok(_) => true,
        Err((_, body)) => body
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|c| c != "unimplemented"),
    };
    let negative = rpc(&base, "loams.admin.v1.OrgService/NoSuchMethod", &json!({})).await;
    assert!(
        !served(&negative),
        "the check passes an unknown procedure: {negative:?}"
    );

    let mut checked = BTreeSet::new();
    for name in CONNECT_RPCS
        .iter()
        .filter_map(|(_, _, rpc)| *rpc)
        .chain(CONNECT_ONLY_RPCS.iter().copied())
    {
        if name.ends_with("/WatchAuditEvents") {
            // Server-streaming: a unary JSON post cannot call it. It is
            // called by `watch_audit_resumes_from_cursor`.
            continue;
        }
        let answer = rpc(&base, name, &json!({})).await;
        assert!(served(&answer), "{name} is not served: {answer:?}");
        checked.insert(name);
    }
    assert_eq!(checked.len(), 42, "{checked:#?}");
    mock.stop().await;
}

/// Port of `every_mocked_body_matches_its_response_schema`: every REST route
/// of the seed and its RPC answer the same body. The reads go first, because
/// a Connect mutation appends to the audit log and the REST one does not.
#[tokio::test]
async fn every_mocked_body_matches_its_response_schema_rpc() {
    let mock = start(true, Duration::from_secs(15)).await;
    let base = mock.url();
    let seed = load_seed(SystemTime::now()).unwrap();
    let mut all: Vec<Route> = routes(&seed, true).unwrap();
    all.sort_by_key(|r| r.method != "GET");
    let mut compared = 0;
    for route in all {
        let Some((name, request, unwrap)) =
            rpc_for(route.method, route.template, &route.path, &route.query)
        else {
            continue;
        };
        let query = route
            .query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let path = if query.is_empty() {
            route.path.clone()
        } else {
            format!("{}?{query}", route.path)
        };
        let body = (route.method != "GET").then(|| json!({}));
        let (status, rest_body) = rest(&base, route.method, &path, body.as_ref()).await;
        assert!(
            status < 300,
            "{} {path}: REST answered {status}",
            route.method
        );
        let connect = rpc(&base, name, &request)
            .await
            .unwrap_or_else(|e| panic!("{name} for {} {path}: {e:?}", route.method));
        let connect = unwrapped(connect, unwrap);
        assert!(
            same(&rest_body, &connect),
            "{} {path} and {name} answer differently:\nREST    {}\nConnect {}",
            route.method,
            normalize(&rest_body),
            normalize(&connect),
        );
        compared += 1;
    }
    assert!(compared > 90, "only {compared} routes compared");
    mock.stop().await;
}

/// Port of `the_server_answers_from_the_seed`, through the generated clients.
#[tokio::test]
async fn the_server_answers_from_the_seed_rpc() {
    let mock = start(true, Duration::from_secs(15)).await;
    let projects =
        ProjectServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect))
            .list_projects(ListProjectsRequest::default())
            .await
            .unwrap()
            .into_owned();
    let slugs: Vec<&str> = projects.projects.iter().map(|p| p.slug.as_str()).collect();
    assert_eq!(slugs, ["docs-search", "support-assistant", "code-index"]);
    assert!(projects.next_page_token.is_empty());

    let audit = AuditServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let filtered = audit
        .list_audit_events(ListAuditEventsRequest {
            project: "code-index".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert!(!filtered.events.is_empty());
    assert!(filtered.events.iter().all(|e| e.project == "code-index"));

    AuthServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect))
        .delete_session(DeleteSessionRequest::default())
        .await
        .unwrap();
    mock.stop().await;
}

/// Port of `console_rest_is_served_beside_the_app_protos`: the console API
/// answers on the app protos' listener, on every protocol.
#[tokio::test]
async fn console_rest_is_served_beside_the_app_protos_rpc() {
    let mock = start(true, Duration::from_secs(15)).await;
    for protocol in [Protocol::Connect, Protocol::Grpc, Protocol::GrpcWeb] {
        let session = AuthServiceClient::new(http(protocol), config(&mock, protocol))
            .get_session(GetSessionRequest::default())
            .await
            .unwrap_or_else(|e| panic!("{protocol:?}: {e}"))
            .into_owned();
        let session = session.session.as_option().unwrap();
        assert_eq!(session.user.as_option().unwrap().email, "priya@acme.dev");
    }
    let info =
        InstanceServiceClient::new(http(Protocol::Connect), config(&mock, Protocol::Connect))
            .get_instance(GetInstanceRequest::default())
            .await
            .unwrap()
            .into_owned();
    assert_eq!(info.edition.as_known(), Some(Edition::EDITION_OSS));
    mock.stop().await;
}

/// Port of `console_query_constraints_pick_the_right_answer`: the audit
/// filters, and AIP-158 pages over the same list.
#[tokio::test]
async fn console_query_constraints_pick_the_right_answer_rpc() {
    let mock = start(true, Duration::from_secs(15)).await;
    let audit = AuditServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let all = audit
        .list_audit_events(ListAuditEventsRequest::default())
        .await
        .unwrap()
        .into_owned();
    let filtered = audit
        .list_audit_events(ListAuditEventsRequest {
            actor: "agt_claude_code".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert_eq!(all.events.len(), 20);
    assert_eq!(filtered.events.len(), 3);

    // Pages of 7 walk the same 20 events in the same order.
    let mut paged = Vec::new();
    let mut token = String::new();
    loop {
        let page = audit
            .list_audit_events(ListAuditEventsRequest {
                page_size: 7,
                page_token: token.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_owned();
        assert!(page.events.len() <= 7);
        paged.extend(page.events.into_iter().map(|e| e.id));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    let ids: Vec<String> = all.events.into_iter().map(|e| e.id).collect();
    assert_eq!(paged, ids);
    mock.stop().await;
}

/// Port of `console_signed_out_answers_401_on_session`: signed out,
/// `GetSession` is `unauthenticated`; `CreateSession` sets the session
/// cookie, and with it `GetSession` answers.
#[tokio::test]
async fn console_signed_out_answers_401_on_session_rpc() {
    let mock = start(false, Duration::from_secs(15)).await;
    let base = mock.url();
    let err = AuthServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect))
        .get_session(GetSessionRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert_eq!(reason(&err), "unauthenticated");

    let response = reqwest::Client::new()
        .post(format!("{base}/loams.auth.v1.AuthService/CreateSession"))
        .header("content-type", "application/json")
        .json(&json!({"email": "priya@acme.dev", "password": "correct horse"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let cookie = response
        .headers()
        .get("set-cookie")
        .expect("CreateSession sets the session cookie")
        .to_str()
        .unwrap()
        .to_owned();
    for attribute in ["HttpOnly", "Secure", "SameSite=Lax"] {
        assert!(cookie.contains(attribute), "{cookie} lacks {attribute}");
    }
    let pair = cookie.split(';').next().unwrap();
    let session = AuthServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect))
        .get_session_with_options(
            GetSessionRequest::default(),
            CallOptions::default().with_header("cookie", pair),
        )
        .await
        .unwrap()
        .into_owned();
    assert_eq!(
        session
            .session
            .as_option()
            .unwrap()
            .user
            .as_option()
            .unwrap()
            .id,
        "usr_01"
    );
    // A bearer token is a session too (§44 §5.2: cookie or bearer).
    AuthServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect))
        .get_session_with_options(
            GetSessionRequest::default(),
            CallOptions::default().with_header("authorization", "Bearer mock-access-usr_01"),
        )
        .await
        .unwrap();
    mock.stop().await;
}

/// API2 Task 7's `mock_and_server_answer_alike`, mock half: a console session
/// scripted against both surfaces of one mock gives equal answers, so the
/// console can be tested against either while it moves to Connect.
#[tokio::test]
async fn mock_rest_and_connect_answer_alike() {
    let mock = start(false, Duration::from_secs(15)).await;
    let base = mock.url();

    // Signed out, both refuse the session.
    let (status, _) = rest(&base, "GET", "/api/v1/session", None).await;
    assert_eq!(status, 401);
    let (status, body) = rpc(&base, "loams.auth.v1.AuthService/GetSession", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(
        (status, body["code"].as_str()),
        (401, Some("unauthenticated"))
    );

    // The sign-in page: the providers are `GET /instance`'s `sign_in`.
    let (_, instance) = rest(&base, "GET", "/api/v1/instance", None).await;
    let providers = rpc(&base, "loams.auth.v1.AuthService/ListProviders", &json!({}))
        .await
        .unwrap();
    assert_eq!(normalize(&providers), normalize(&instance["sign_in"]));

    // Sign in, then walk the console the way its pages do.
    let login = json!({"email": "priya@acme.dev", "password": "correct horse"});
    let script: &[(&str, &str, Option<Value>)] = &[
        ("POST", "/api/v1/session", Some(login)),
        ("GET", "/api/v1/org", None),
        ("GET", "/api/v1/org/members", None),
        ("GET", "/api/v1/teams", None),
        ("GET", "/api/v1/teams/team_platform", None),
        ("GET", "/api/v1/projects", None),
        ("GET", "/api/v1/projects/docs-search", None),
        ("GET", "/api/v1/projects/docs-search/access", None),
        ("GET", "/api/v1/projects/docs-search/environments", None),
        (
            "GET",
            "/api/v1/projects/docs-search/environments/production",
            None,
        ),
        (
            "GET",
            "/api/v1/projects/docs-search/environments/production/usage",
            None,
        ),
        (
            "GET",
            "/api/v1/projects/docs-search/environments/production/keys",
            None,
        ),
        ("GET", "/api/v1/projects/docs-search/agents", None),
        ("GET", "/api/v1/agents/agt_docs_qa", None),
        ("GET", "/api/v1/agents/agt_docs_qa/tokens", None),
        ("GET", "/api/v1/agents/agt_docs_qa/trust-policies", None),
        ("GET", "/api/v1/projects/docs-search/service-accounts", None),
        ("GET", "/api/v1/audit", None),
        // The mutations answer alike too.
        (
            "POST",
            "/api/v1/projects/docs-search/environments/production/keys",
            None,
        ),
        ("POST", "/api/v1/agents/agt_docs_qa/suspend", None),
        ("POST", "/api/v1/agents/agt_docs_qa/resume", None),
        ("DELETE", "/api/v1/tokens/tok_a1b0", None),
        ("POST", "/api/v1/oauth/consent", None),
        ("DELETE", "/api/v1/session", None),
    ];
    for (method, path, body) in script {
        // The contract's template for the path: its literal segments match,
        // and its `{name}`s match anything.
        let template = CONNECT_RPCS
            .iter()
            .map(|(m, t, _)| (*m, *t))
            .find(|(m, t)| {
                m == method
                    && t.split('/').count() == path.split('/').count()
                    && t.split('/')
                        .zip(path.split('/'))
                        .all(|(a, b)| a.starts_with('{') || a == b)
            })
            .unwrap_or_else(|| panic!("no template for {method} {path}"))
            .1;
        let (name, request, unwrap) = rpc_for(method, template, path, &[]).unwrap();
        let (status, rest_body) = rest(&base, method, path, body.as_ref()).await;
        assert!(status < 300, "{method} {path}: {status}");
        let connect = rpc(&base, name, &request)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let connect = unwrapped(connect, unwrap);
        assert!(
            same(&rest_body, &connect),
            "{method} {path} and {name} answer differently:\nREST    {}\nConnect {}",
            normalize(&rest_body),
            normalize(&connect),
        );
    }

    // The Connect mutations were audited; the REST mock's never are. So the
    // Connect log is the REST log with those events on top, newest first.
    let (_, rest_audit) = rest(&base, "GET", "/api/v1/audit", None).await;
    let connect_audit = rpc(
        &base,
        "loams.admin.v1.AuditService/ListAuditEvents",
        &json!({}),
    )
    .await
    .unwrap();
    let connect_events = connect_audit["events"].as_array().unwrap();
    let rest_events = rest_audit["events"].as_array().unwrap();
    let added = connect_events.len() - rest_events.len();
    let actions: Vec<&str> = connect_events[..added]
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions,
        [
            "token.revoke",
            "agent.resume",
            "agent.suspend",
            "api_key.create"
        ]
    );
    assert_eq!(
        normalize(&Value::Array(connect_events[added..].to_vec())),
        normalize(&Value::Array(rest_events.clone()))
    );
    mock.stop().await;
}

/// API2 Task 7: `CreateKey` answers the secret once. A replay of the same
/// `idempotency_key` is `secret_already_issued` and never carries the secret;
/// a new key is a new key; no list or audit event carries a secret.
#[tokio::test]
async fn create_key_returns_secret_once() {
    let mock = start(true, Duration::from_secs(15)).await;
    let keys = KeyServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let request = CreateKeyRequest {
        project: "docs-search".into(),
        environment: "staging".into(),
        name: "ci".into(),
        scopes: vec!["query".into()],
        idempotency_key: "01J9ZKEYONE".into(),
        ..Default::default()
    };
    let created = keys.create_key(request.clone()).await.unwrap().into_owned();
    let secret = created.secret.clone();
    assert!(!secret.is_empty(), "the first answer carries the secret");
    let key = created.key.as_option().unwrap();
    assert!(secret.starts_with(&key.prefix), "{secret} / {}", key.prefix);

    let err = keys.create_key(request.clone()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert_eq!(reason(&err), "secret_already_issued");
    assert!(!err.message.as_deref().unwrap_or_default().contains(&secret));

    // A new idempotency key creates (and answers) a new key.
    let again = keys
        .create_key(CreateKeyRequest {
            idempotency_key: "01J9ZKEYTWO".into(),
            ..request.clone()
        })
        .await
        .unwrap()
        .into_owned();
    assert!(!again.secret.is_empty());

    // Without a key the call is refused: its secret could not be recognised
    // on a retry.
    let err = keys
        .create_key(CreateKeyRequest {
            idempotency_key: String::new(),
            ..request
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    assert_eq!(reason(&err), "invalid_argument");

    // Nothing else answers the secret.
    let base = mock.url();
    for (name, body) in [
        (
            "loams.admin.v1.KeyService/ListKeys",
            json!({"project": "docs-search", "environment": "staging"}),
        ),
        ("loams.admin.v1.AuditService/ListAuditEvents", json!({})),
    ] {
        let answer = rpc(&base, name, &body).await.unwrap().to_string();
        assert!(!answer.contains(&secret), "{name} answers the secret");
    }
    let listed = keys
        .list_keys(ListKeysRequest {
            project: "docs-search".into(),
            environment: "staging".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_owned();
    assert!(!listed.keys.is_empty());
    mock.stop().await;
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

/// API2 Task 7: `WatchAuditEvents` sends a snapshot and its cursor, then each
/// new event and heartbeats; a client that reconnects with a cursor gets
/// what it missed and no snapshot; an unknown cursor gets a fresh snapshot
/// with `snapshot_reset`.
#[tokio::test]
async fn watch_audit_resumes_from_cursor() {
    let mock = start(true, Duration::from_millis(200)).await;
    let audit = AuditServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let keys = KeyServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let agents = AgentServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));

    let mut stream = audit
        .watch_audit_events(WatchAuditEventsRequest::default())
        .await
        .unwrap();
    let first = next!(stream);
    let Some(Event::Snapshot(snapshot)) = first.event else {
        panic!("a snapshot first, got {first:?}");
    };
    assert_eq!(snapshot.events.len(), 20, "the seed's log");
    assert!(!first.snapshot_reset);
    let at_snapshot = first.cursor.clone();

    // A mutation appears as an `append` with a new cursor.
    keys.revoke_key(RevokeKeyRequest {
        key: "key_p3x8a1".into(),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|e| panic!("revoke a seed key: {e}"));
    let appended = loop {
        let msg = next!(stream);
        match msg.event {
            Some(Event::Heartbeat(_)) => continue,
            Some(Event::Append(event)) => break (event, msg.cursor),
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(appended.0.action, "api_key.revoke");
    assert_ne!(appended.1, at_snapshot);
    let beat = next!(stream);
    assert!(matches!(beat.event, Some(Event::Heartbeat(_))));
    assert_eq!(
        beat.cursor, appended.1,
        "a heartbeat carries the last cursor"
    );
    drop(stream);

    // Missed while disconnected.
    agents
        .suspend_agent(SuspendAgentRequest {
            agent: "agt_docs_qa".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Resuming from the append's cursor replays only the missed event.
    let mut resumed = audit
        .watch_audit_events(WatchAuditEventsRequest {
            resume_cursor: appended.1.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let replay = next!(resumed);
    assert!(!replay.snapshot_reset);
    match replay.event {
        Some(Event::Append(event)) => assert_eq!(event.action, "agent.suspend"),
        other => panic!("expected the missed event, got {other:?}"),
    }
    // From the snapshot's cursor, both.
    let mut from_start = audit
        .watch_audit_events(WatchAuditEventsRequest {
            resume_cursor: at_snapshot,
            ..Default::default()
        })
        .await
        .unwrap();
    let a = next!(from_start);
    let b = next!(from_start);
    let actions: Vec<String> = [a, b]
        .into_iter()
        .map(|m| match m.event {
            Some(Event::Append(event)) => event.action,
            other => panic!("expected an append, got {other:?}"),
        })
        .collect();
    assert_eq!(actions, ["api_key.revoke", "agent.suspend"]);

    // A cursor the log never issued resets to a fresh snapshot, newest first.
    let mut reset = audit
        .watch_audit_events(WatchAuditEventsRequest {
            resume_cursor: "a999".into(),
            snapshot_size: 5,
            ..Default::default()
        })
        .await
        .unwrap();
    let fresh = next!(reset);
    assert!(fresh.snapshot_reset);
    let Some(Event::Snapshot(snapshot)) = fresh.event else {
        panic!("a snapshot, got {fresh:?}");
    };
    let actions: Vec<&str> = snapshot.events.iter().map(|e| e.action.as_str()).collect();
    assert_eq!(actions.len(), 5);
    assert_eq!(actions[..2], ["agent.suspend", "api_key.revoke"]);

    // A filtered watch skips what does not match.
    let mut filtered = audit
        .watch_audit_events(WatchAuditEventsRequest {
            project: "code-index".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let Some(Event::Snapshot(snapshot)) = next!(filtered).event else {
        panic!("a snapshot first");
    };
    assert!(snapshot.events.iter().all(|e| e.project == "code-index"));
    mock.stop().await;
}
