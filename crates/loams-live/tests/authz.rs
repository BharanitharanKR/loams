//! LV1 plan Task 15: per-app and per-table authorization (design §45
//! §5.2), through a real server. A test guard stands in for the engine's
//! authentication layer: it turns `x-test-*` headers into the request's
//! `Caller`, as `api::live::caller` does from an admitted principal.
//!
//! `authz_matrix` runs every combination of caller kind × scopes × app
//! match × target and compares the outcomes with the blessed table in
//! `authz_matrix.expected.md`; `LOAMS_BLESS=1` rewrites it, and a reviewer
//! reads the diff. App end users (Task 16) join it with a token from the
//! app's issuer, as requests the engine's layer left to the app
//! (`x-test-kind: anonymous`), and so does a call with no credential.

#[path = "support/oidc.rs"]
mod oidc;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use buffa::MessageField;
use connectrpc::ConnectError;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use loams_live::auth::{Caller, CallerKind};
use loams_live::system::{GET, INSERT, QUERY};
use loams_live::testing::TestStore;
use loams_live::{APP_HEADER, LiveHandle, LiveServer, LiveValue, live_test, pb};
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Admin = pb::LiveAdminServiceClient<HttpClient>;

/// The app every matrix call addresses, and the environment it belongs to.
const APP: &str = "shop";
const APP_ENV: &str = "shop/prod";
/// Another app, in another environment.
const OTHER: &str = "other";
const OTHER_ENV: &str = "other/prod";

// ---- the test authentication layer ----

/// Builds the request's `Caller` from `x-test-kind`, `x-test-id`,
/// `x-test-env` and `x-test-scopes` (space-separated; absent: every
/// scope). No `x-test-kind`: no caller, as a bare server.
async fn test_layer(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(caller) = test_caller(request.headers()) {
        request.extensions_mut().insert(caller);
    }
    next.run(request).await
}

fn test_caller(headers: &axum::http::HeaderMap) -> Option<Caller> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let kind = match header("x-test-kind")?.as_str() {
        "anonymous" => return Some(Caller::anonymous()),
        "user" => CallerKind::User,
        "session" => CallerKind::Session,
        "api_key" => CallerKind::ApiKey,
        _ => CallerKind::DevLoopback,
    };
    Some(Caller {
        environment: header("x-test-env"),
        scopes: header("x-test-scopes").map(|s| {
            s.split_whitespace()
                .map(str::to_owned)
                .collect::<BTreeSet<_>>()
        }),
        ..Caller::new(header("x-test-id").unwrap_or_else(|| "tester".into()), kind)
    })
}

/// A server for app `main` with the JS runtime, behind the test layer, with
/// the apps `shop` (in `shop/prod`) and `other` (in `other/prod`), each with
/// the notes bundle deployed.
async fn server(store: &TestStore) -> (LiveHandle, String) {
    server_trusting(store, Vec::new()).await
}

/// [`server`], with `shop` trusting `issuers` for its end users.
async fn server_trusting(
    store: &TestStore,
    issuers: Vec<pb::TrustedIssuer>,
) -> (LiveHandle, String) {
    let mut config = store.live_config("main");
    config.listen = "127.0.0.1:0".parse().expect("an address");
    let loader = loams_live_js::JsLoader::new(loams_live_js::JsConfig {
        contexts: 1,
        ..loams_live_js::JsConfig::default()
    })
    .expect("an in-process loader");
    let guard: loams_live::service::Guard =
        Arc::new(|router: axum::Router| router.layer(axum::middleware::from_fn(test_layer)));
    let handle = LiveServer::start_guarded(
        config,
        loams_live::deploy::Runtime {
            bundles: loams_store::Store::in_memory(),
            loader: Arc::new(loader),
        },
        Some(guard),
        CancellationToken::new(),
    )
    .await
    .expect("the server starts");
    let a = admin(&handle);
    let mut active = String::new();
    for (name, env) in [(APP, APP_ENV), (OTHER, OTHER_ENV)] {
        a.create_app(pb::CreateAppRequest {
            name: name.into(),
            namespace: env.into(),
            issuers: if name == APP {
                issuers.clone()
            } else {
                Vec::new()
            },
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| panic!("CreateApp {name}: {e}"));
        let deployed = a
            .deploy_with_options(
                pb::DeployRequest {
                    bundle: bundle(),
                    ..Default::default()
                },
                CallOptions::default().with_header(APP_HEADER, name),
            )
            .await
            .unwrap_or_else(|e| panic!("Deploy {name}: {e}"))
            .into_owned();
        if name == APP {
            active = deployed.deployment.into_option().expect("a deployment").id;
        }
    }
    (handle, active)
}

fn bundle() -> Vec<u8> {
    br#"
import { query, mutation, internalQuery, v } from "loams:server";
export const notes = {
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
  add: mutation({
    args: { body: v.string() },
    handler: async (ctx, { body }) => await ctx.db.insert("notes", { body }),
  }),
  audit: internalQuery(async () => null),
};
"#
    .to_vec()
}

fn config(handle: &LiveHandle) -> ClientConfig {
    ClientConfig::new(format!("http://{}", handle.addr).parse().expect("a uri"))
}

fn client(handle: &LiveHandle) -> Client {
    pb::LiveServiceClient::new(HttpClient::plaintext(), config(handle))
}

fn admin(handle: &LiveHandle) -> Admin {
    pb::LiveAdminServiceClient::new(HttpClient::plaintext(), config(handle))
}

fn obj(pairs: &[(&str, LiveValue)]) -> pb::Value {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
    .to_proto()
}

/// The caller headers of one matrix row.
#[derive(Debug, Clone)]
struct Who {
    kind: Option<&'static str>,
    scopes: Option<&'static str>,
    env: Option<&'static str>,
    /// An end user's token (`Authorization: Bearer`).
    bearer: Option<String>,
}

impl Who {
    fn options(&self, app: Option<&str>) -> CallOptions {
        let mut o = CallOptions::default();
        if let Some(app) = app {
            o = o.with_header(APP_HEADER, app);
        }
        if let Some(kind) = self.kind {
            o = o.with_header("x-test-kind", kind);
        }
        if let Some(scopes) = self.scopes {
            o = o.with_header("x-test-scopes", scopes);
        }
        if let Some(env) = self.env {
            o = o.with_header("x-test-env", env);
        }
        if let Some(token) = &self.bearer {
            o = o.with_header("authorization", format!("Bearer {token}"));
        }
        o
    }
}

/// `allowed`, or the refusal's Connect code and `ErrorInfo.reason`.
fn outcome(result: Result<(), ConnectError>) -> String {
    use buffa::Message as _;
    let Err(e) = result else {
        return "allowed".to_string();
    };
    let reason = e
        .details
        .iter()
        .find(|d| d.type_url.ends_with("loams.errors.v1.ErrorInfo"))
        .and_then(|d| d.value.as_deref())
        .and_then(|v| {
            data_encoding::BASE64_NOPAD
                .decode(v.trim_end_matches('=').as_bytes())
                .ok()
        })
        .and_then(|b| loams_proto::loams::errors::v1::ErrorInfo::decode_from_slice(&b).ok())
        .map(|i| i.reason)
        .unwrap_or_default();
    format!("{:?} {reason}", e.code).to_lowercase()
}

/// The targets of the matrix.
const TARGETS: [&str; 9] = [
    "Query public fn",
    "Mutate public fn",
    "Query internal fn",
    "Query _system:query notes",
    "Query _system:get",
    "Watch _system:query notes",
    "Mutate _system:insert notes",
    "admin Deploy (dry run)",
    "admin UpdateApp",
];

async fn run(c: &Client, a: &Admin, who: &Who, active: &str, target: &str) -> String {
    let o = || who.options(Some(APP));
    let r: Result<(), ConnectError> = match target {
        "Query public fn" => c
            .query_with_options(
                pb::QueryRequest {
                    function: "notes:count".into(),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "Mutate public fn" => c
            .mutate_with_options(
                pb::MutateRequest {
                    function: "notes:add".into(),
                    args: MessageField::some(obj(&[("body", LiveValue::Str("hi".into()))])),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "Query internal fn" => c
            .query_with_options(
                pb::QueryRequest {
                    function: "notes:audit".into(),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "Query _system:query notes" => c
            .query_with_options(
                pb::QueryRequest {
                    function: QUERY.into(),
                    args: MessageField::some(obj(&[("table", LiveValue::Str("notes".into()))])),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        // A malformed id: allowed calls fail with INVALID_ARGUMENT, so the
        // table shows that authorization let them through.
        "Query _system:get" => c
            .query_with_options(
                pb::QueryRequest {
                    function: GET.into(),
                    args: MessageField::some(obj(&[("id", LiveValue::Str("none".into()))])),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "Watch _system:query notes" => {
            let spec = pb::QuerySpec {
                query_id: 1,
                function: QUERY.into(),
                args: MessageField::some(obj(&[("table", LiveValue::Str("notes".into()))])),
                ..Default::default()
            };
            let set = pb::QuerySet {
                version: 1,
                queries: vec![spec],
                ..Default::default()
            };
            let opened = c
                .watch_with_options(
                    pb::WatchRequest {
                        start: Some(pb::__buffa::oneof::watch_request::Start::Initial(Box::new(
                            set,
                        ))),
                        ..Default::default()
                    },
                    o(),
                )
                .await;
            match opened {
                Err(e) => Err(e),
                Ok(mut stream) => stream.message().await.map(drop),
            }
        }
        "Mutate _system:insert notes" => c
            .mutate_with_options(
                pb::MutateRequest {
                    function: INSERT.into(),
                    args: MessageField::some(obj(&[
                        ("table", LiveValue::Str("notes".into())),
                        (
                            "fields",
                            LiveValue::Object(
                                [("body".to_string(), LiveValue::Str("x".into()))].into(),
                            ),
                        ),
                    ])),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "admin Deploy (dry run)" => a
            .deploy_with_options(
                pb::DeployRequest {
                    bundle: bundle(),
                    expected_active: Some(active.to_string()),
                    dry_run: true,
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        "admin UpdateApp" => a
            .update_app_with_options(
                pb::UpdateAppRequest {
                    name: APP.into(),
                    allow_anonymous: Some(false),
                    ..Default::default()
                },
                o(),
            )
            .await
            .map(drop),
        other => panic!("unknown target {other}"),
    };
    outcome(r)
}

const EXPECTED: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/authz_matrix.expected.md"
);

/// Every caller kind × scope set × app match × target, against the blessed
/// table.
async fn authz_matrix(store: TestStore) {
    let issuer = oidc::Issuer::start().await;
    let (handle, active) = server_trusting(&store, vec![issuer.trusted_pb()]).await;
    let (c, a) = (client(&handle), admin(&handle));
    let mut rows = Vec::new();
    let mut whos = vec![(
        "none",
        "-",
        "-",
        Who {
            kind: None,
            scopes: None,
            env: None,
            bearer: None,
        },
    )];
    // Task 16: an app end user (a token naming every Live scope changes
    // nothing), and a call with no credential to an app that takes no
    // anonymous calls.
    let every_scope = issuer.token_with(&issuer.key, "ada", |claims| {
        claims["scp"] = serde_json::json!([
            "live:call",
            "live:read",
            "live:write",
            "live:deploy",
            "live:admin"
        ]);
    });
    for (kind, scopes, bearer) in [
        ("end_user", "-", Some(issuer.token("ada"))),
        ("end_user", "(token: every live scope)", Some(every_scope)),
        ("anonymous", "-", None),
    ] {
        whos.push((
            kind,
            scopes,
            "-",
            Who {
                kind: Some("anonymous"),
                scopes: None,
                env: None,
                bearer,
            },
        ));
    }
    for kind in ["user", "session", "api_key"] {
        for scopes in [
            None,
            Some("live:call"),
            Some("live:read"),
            Some("live:write"),
            Some("live:read:notes"),
            Some("live:deploy"),
            Some("live:admin"),
        ] {
            for (matching, env) in [
                ("unbound", None),
                ("same", Some(APP_ENV)),
                ("other", Some(OTHER_ENV)),
            ] {
                whos.push((
                    kind,
                    scopes.unwrap_or("(all)"),
                    matching,
                    Who {
                        kind: Some(kind),
                        scopes,
                        env,
                        bearer: None,
                    },
                ));
            }
        }
    }
    for (kind, scopes, matching, who) in &whos {
        for target in TARGETS {
            let got = run(&c, &a, who, &active, target).await;
            rows.push(format!(
                "| {kind} | {scopes} | {matching} | {target} | {got} |"
            ));
        }
    }
    let table = format!(
        "# The Live authorization matrix (LV1 plan Tasks 15 and 16)\n\n\
         Generated by `authz_matrix` (`crates/loams-live/tests/authz.rs`); \
         `LOAMS_BLESS=1` rewrites it. Every call addresses the app `{APP}` \
         (environment `{APP_ENV}`). \"app\" is the credential's environment: \
         unbound (none, the self-hosted default), same, or another's. \
         `end_user` holds a token from the app's trusted issuer and `anonymous` \
         no credential (the app takes no anonymous calls), both as the engine's \
         layer leaves them to Live on `LiveService`; on `LiveAdminService` the \
         engine refuses them first, so the admin rows show Live's own second \
         check.\n\n\
         | caller | scopes | app | target | outcome |\n|---|---|---|---|---|\n{}\n",
        rows.join("\n")
    );
    if std::env::var_os("LOAMS_BLESS").is_some() {
        std::fs::write(EXPECTED, &table).expect("bless the matrix");
    } else {
        let expected = std::fs::read_to_string(EXPECTED).unwrap_or_default();
        if expected != table {
            let diff: Vec<_> = table
                .lines()
                .zip(expected.lines().chain(std::iter::repeat("")))
                .filter(|(got, want)| got != want)
                .take(20)
                .map(|(got, want)| format!("got:  {got}\nwant: {want}"))
                .collect();
            panic!(
                "the authorization matrix differs from authz_matrix.expected.md (LOAMS_BLESS=1 \
                 rewrites it):\n{}",
                diff.join("\n")
            );
        }
    }
    handle.stop().await;
}
live_test!(authz_matrix);

/// A credential bound to one app's environment is refused on another app,
/// addresses its own app without the header, and cannot create apps.
async fn other_app_credential_denied(store: TestStore) {
    let (handle, _) = server(&store).await;
    let (c, a) = (client(&handle), admin(&handle));
    let who = Who {
        kind: Some("api_key"),
        scopes: None,
        env: Some(APP_ENV),
        bearer: None,
    };
    let count = |app: Option<&'static str>| {
        let c = &c;
        let o = who.options(app);
        async move {
            c.query_with_options(
                pb::QueryRequest {
                    function: "notes:count".into(),
                    ..Default::default()
                },
                o,
            )
            .await
            .map(drop)
        }
    };
    assert_eq!(
        outcome(count(Some(OTHER)).await),
        "permissiondenied live_app_mismatch"
    );
    assert_eq!(
        outcome(count(Some("main")).await),
        "permissiondenied live_app_mismatch"
    );
    assert_eq!(outcome(count(Some(APP)).await), "allowed");
    // Without the header, the app bound to the credential's environment.
    assert_eq!(outcome(count(None).await), "allowed");
    let lost = Who {
        env: Some("nowhere/prod"),
        ..who.clone()
    };
    let e = c
        .query_with_options(
            pb::QueryRequest {
                function: "notes:count".into(),
                ..Default::default()
            },
            lost.options(None),
        )
        .await
        .expect_err("no app in that environment");
    assert_eq!(e.code, connectrpc::ErrorCode::NotFound, "{e:?}");
    let create = a
        .create_app_with_options(
            pb::CreateAppRequest {
                name: "mine".into(),
                ..Default::default()
            },
            who.options(None),
        )
        .await
        .map(drop);
    assert_eq!(outcome(create), "permissiondenied live_app_mismatch");
    handle.stop().await;
}
live_test!(other_app_credential_denied);

/// `live:read:<table>` allows `_system:query` of that table only, and no
/// id-addressed read (whose table the request does not name).
async fn table_level_read_grant_limits_system_query(store: TestStore) {
    let (handle, _) = server(&store).await;
    let c = client(&handle);
    let who = Who {
        kind: Some("api_key"),
        scopes: Some("live:read:notes"),
        env: None,
        bearer: None,
    };
    let query = |table: &str| {
        let c = &c;
        let o = who.options(Some(APP));
        let args = MessageField::some(obj(&[("table", LiveValue::Str(table.into()))]));
        async move {
            c.query_with_options(
                pb::QueryRequest {
                    function: QUERY.into(),
                    args,
                    ..Default::default()
                },
                o,
            )
            .await
            .map(drop)
        }
    };
    assert_eq!(outcome(query("notes").await), "allowed");
    assert_eq!(
        outcome(query("users").await),
        "permissiondenied permission_denied"
    );
    let get = c
        .query_with_options(
            pb::QueryRequest {
                function: GET.into(),
                args: MessageField::some(obj(&[("id", LiveValue::Str("none".into()))])),
                ..Default::default()
            },
            who.options(Some(APP)),
        )
        .await
        .map(drop);
    assert_eq!(outcome(get), "permissiondenied permission_denied");
    handle.stop().await;
}
live_test!(table_level_read_grant_limits_system_query);

/// The log lines every test of this binary writes.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("the log").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured() -> &'static Captured {
    static LOG: OnceLock<Captured> = OnceLock::new();
    LOG.get_or_init(|| {
        let log = Captured::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("one global subscriber");
        log
    })
}

/// A deploy and a `_system:*` write each write an audit line naming the
/// caller, the action and the resource.
async fn audit_line_for_deploy_and_system_write(store: TestStore) {
    let log = captured();
    let (handle, active) = server(&store).await;
    let (c, a) = (client(&handle), admin(&handle));
    let id = format!("auditor-{}", ulid::Ulid::generate());
    let opts = || {
        CallOptions::default()
            .with_header(APP_HEADER, APP)
            .with_header("x-test-kind", "api_key")
            .with_header("x-test-id", id.as_str())
    };
    a.deploy_with_options(
        pb::DeployRequest {
            bundle: bundle(),
            message: "v2".into(),
            expected_active: Some(active),
            ..Default::default()
        },
        opts(),
    )
    .await
    .expect("Deploy");
    c.mutate_with_options(
        pb::MutateRequest {
            function: INSERT.into(),
            args: MessageField::some(obj(&[
                ("table", LiveValue::Str("notes".into())),
                (
                    "fields",
                    LiveValue::Object([("body".to_string(), LiveValue::Str("x".into()))].into()),
                ),
            ])),
            ..Default::default()
        },
        opts(),
    )
    .await
    .expect("an insert");
    let text = String::from_utf8(log.0.lock().expect("the log").clone()).expect("utf-8");
    let mine: Vec<&str> = text
        .lines()
        .filter(|l| l.contains(&id) && l.contains("audit=true"))
        .collect();
    assert!(
        mine.iter()
            .any(|l| l.contains("action=\"live:deploy\"") || l.contains("action=live:deploy")),
        "no deploy audit line: {mine:?}"
    );
    assert!(
        mine.iter()
            .any(|l| l.contains("live_table:shop/notes") && l.contains("live:write")),
        "no system-write audit line: {mine:?}"
    );
    handle.stop().await;
}
live_test!(audit_line_for_deploy_and_system_write);

/// LV1 plan Task 17: `Authenticate` with a refreshed credential of the same
/// principal is authorized again for the session's queries; one whose scopes
/// no longer allow them is refused, and the session keeps its identity.
async fn authenticate_rechecks_a_refreshed_credential(store: TestStore) {
    let (handle, _) = server(&store).await;
    let c = client(&handle);
    let who = |scopes: Option<&'static str>| Who {
        kind: Some("api_key"),
        scopes,
        env: None,
        bearer: None,
    };
    let spec = pb::QuerySpec {
        query_id: 1,
        function: "notes:count".into(),
        ..Default::default()
    };
    let set = pb::QuerySet {
        version: 1,
        queries: vec![spec],
        ..Default::default()
    };
    let mut stream = c
        .watch_with_options(
            pb::WatchRequest {
                start: Some(pb::__buffa::oneof::watch_request::Start::Initial(Box::new(
                    set,
                ))),
                ..Default::default()
            },
            who(None).options(Some(APP)),
        )
        .await
        .expect("Watch");
    let first = stream
        .message()
        .await
        .expect("the stream")
        .expect("a Transition")
        .to_owned_message();
    let authenticate = |scopes| {
        let c = &c;
        let session = first.session_id.clone();
        async move {
            c.authenticate_with_options(
                pb::AuthenticateRequest {
                    session_id: session,
                    ..Default::default()
                },
                who(scopes).options(Some(APP)),
            )
            .await
            .map(drop)
        }
    };
    assert_eq!(outcome(authenticate(Some("live:call")).await), "allowed");
    assert_eq!(
        outcome(authenticate(Some("live:read")).await),
        "permissiondenied permission_denied"
    );
    handle.stop().await;
}
live_test!(authenticate_rechecks_a_refreshed_credential);
