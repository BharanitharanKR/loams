//! LV1 plan Task 13: the app directory and many apps per node (design §45
//! §7, D690). Every test runs on the embedded store and, with
//! `LOAMS_TEST_PD`, on TiKV; the directory and the apps' keyspaces live
//! under the test's root there (`Layout::Rooted`).

use std::sync::Arc;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use loams_kv::{Fault, FaultPlan, FaultPoint};
use loams_live::apps::{AppsOptions, NewApp, SYSTEM_KEYSPACE};
use loams_live::system::{INSERT, QUERY};
use loams_live::testing::TestStore;
use loams_live::{
    APP_HEADER, AppState, Apps, LiveConfig, LiveError, LiveHandle, LiveServer, LiveValue,
    live_test, pb,
};
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Admin = pb::LiveAdminServiceClient<HttpClient>;

/// A server for app `main` on the test's store, with `tune` applied to its
/// config.
async fn server(store: &TestStore, tune: impl FnOnce(&mut LiveConfig)) -> LiveHandle {
    let mut config = store.live_config("main");
    config.listen = "127.0.0.1:0".parse().expect("an address");
    tune(&mut config);
    LiveServer::start(config, CancellationToken::new())
        .await
        .expect("the server starts")
}

fn config(handle: &LiveHandle) -> ClientConfig {
    ClientConfig::new(format!("http://{}", handle.addr).parse().expect("a uri"))
}

fn client(handle: &LiveHandle) -> Client {
    pb::LiveServiceClient::new(HttpClient::plaintext(), config(handle))
}

fn admin(handle: &LiveHandle) -> Admin {
    pb::LiveAdminServiceClient::new(HttpClient::plaintext(), config(handle).json())
}

/// Call options addressing `app` (the server's own app when `None`).
fn to(app: Option<&str>) -> CallOptions {
    match app {
        Some(app) => CallOptions::default().with_header(APP_HEADER, app),
        None => CallOptions::default(),
    }
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

async fn insert(c: &Client, app: Option<&str>, body: &str) -> Result<u64, ConnectError> {
    let req = pb::MutateRequest {
        function: INSERT.into(),
        args: MessageField::some(
            obj(&[
                ("table", LiveValue::Str("notes".into())),
                ("fields", obj(&[("body", LiveValue::Str(body.into()))])),
            ])
            .to_proto(),
        ),
        ..Default::default()
    };
    Ok(c.mutate_with_options(req, to(app))
        .await?
        .into_owned()
        .commit_ts)
}

/// The bodies of app `app`'s notes (none when it has no `notes` table).
async fn bodies(c: &Client, app: Option<&str>) -> Result<Vec<String>, ConnectError> {
    let req = pb::QueryRequest {
        function: QUERY.into(),
        args: MessageField::some(obj(&[("table", LiveValue::Str("notes".into()))]).to_proto()),
        ..Default::default()
    };
    let answer = match c.query_with_options(req, to(app)).await {
        Ok(answer) => answer.into_owned(),
        Err(e) if e.code == ErrorCode::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let result = LiveValue::from_proto(answer.result.into_option().expect("a result"))
        .expect("a valid value");
    let LiveValue::Array(docs) = result else {
        panic!("a query of a table answers an array: {result:?}");
    };
    Ok(docs
        .into_iter()
        .map(|doc| match doc {
            LiveValue::Object(fields) => match fields.get("body") {
                Some(LiveValue::Str(body)) => body.clone(),
                other => panic!("a note's body: {other:?}"),
            },
            other => panic!("a document: {other:?}"),
        })
        .collect())
}

/// The bodies of app `app`'s notes once they equal `want`, or the last read
/// after 10 s: a query without a timestamp reads at the app's latest tick,
/// which follows a commit within a moment.
async fn settled(c: &Client, app: Option<&str>, want: &[&str]) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = bodies(c, app).await.expect("a read");
        if got == want || tokio::time::Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create(a: &Admin, name: &str) -> pb::App {
    a.create_app(pb::CreateAppRequest {
        name: name.into(),
        namespace: "acme/dev".into(),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|e| panic!("CreateApp {name}: {e}"))
    .into_owned()
    .app
    .into_option()
    .expect("an app")
}

/// CreateApp, ListApps (paged), GetApp and UpdateApp over Connect JSON.
async fn create_list_get_update_app(store: TestStore) {
    let handle = server(&store, |_| {}).await;
    let a = admin(&handle);
    let chat = create(&a, "chat").await;
    assert_eq!(chat.name, "chat");
    assert_eq!(chat.namespace, "acme/dev");
    assert_eq!(chat.keyspace, format!("loams_live_a{}", chat.id));
    assert_eq!(chat.state.as_known(), Some(pb::AppState::APP_STATE_ACTIVE));
    assert!(chat.created_at_ms > 0);
    let orders = create(&a, "orders").await;
    assert!(orders.id > chat.id, "ids grow");
    // A taken name is refused.
    let taken = a
        .create_app(pb::CreateAppRequest {
            name: "chat".into(),
            ..Default::default()
        })
        .await
        .expect_err("chat is taken");
    assert_eq!(taken.code, ErrorCode::FailedPrecondition, "{taken}");
    let bad = a
        .create_app(pb::CreateAppRequest {
            name: "1bad".into(),
            ..Default::default()
        })
        .await
        .expect_err("a bad name");
    assert_eq!(bad.code, ErrorCode::InvalidArgument, "{bad}");

    // The list holds the server's own app and both, by id, over pages of 2.
    let mut names = Vec::new();
    let mut token = String::new();
    loop {
        let page = a
            .list_apps(pb::ListAppsRequest {
                page_size: 2,
                page_token: token.clone(),
                ..Default::default()
            })
            .await
            .expect("ListApps")
            .into_owned();
        assert!(page.apps.len() <= 2);
        names.extend(page.apps.iter().map(|app| app.name.clone()));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    assert_eq!(names, ["main", "chat", "orders"]);

    let got = a
        .get_app(pb::GetAppRequest {
            name: "chat".into(),
            ..Default::default()
        })
        .await
        .expect("GetApp")
        .into_owned()
        .app
        .into_option()
        .expect("an app");
    assert_eq!(got, chat);
    let missing = a
        .get_app(pb::GetAppRequest {
            name: "nope".into(),
            ..Default::default()
        })
        .await
        .expect_err("no such app");
    assert_eq!(missing.code, ErrorCode::NotFound);

    // UpdateApp changes what it names and keeps the rest.
    let updated = a
        .update_app(pb::UpdateAppRequest {
            name: "chat".into(),
            limits: MessageField::some(pb::AppLimits {
                max_js_cpu_ms: Some(250),
                ..Default::default()
            }),
            allowed_origins: MessageField::some(pb::AllowedOrigins {
                origins: vec!["https://chat.example.com".into()],
                ..Default::default()
            }),
            issuers: MessageField::some(pb::TrustedIssuers {
                issuers: vec![pb::TrustedIssuer {
                    issuer: "https://id.example.com/application/o/chat/".into(),
                    audiences: vec!["chat".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            allow_anonymous: Some(true),
            ..Default::default()
        })
        .await
        .expect("UpdateApp")
        .into_owned()
        .app
        .into_option()
        .expect("an app");
    assert_eq!(
        updated.limits.as_option().and_then(|l| l.max_js_cpu_ms),
        Some(250)
    );
    assert_eq!(updated.allowed_origins, ["https://chat.example.com"]);
    assert_eq!(updated.issuers.len(), 1);
    assert!(updated.allow_anonymous);
    assert_eq!(updated.namespace, "acme/dev");
    assert_eq!(updated.keyspace, chat.keyspace);
    let bad_origin = a
        .update_app(pb::UpdateAppRequest {
            name: "chat".into(),
            allowed_origins: MessageField::some(pb::AllowedOrigins {
                origins: vec!["https://*.example.com".into()],
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect_err("a wildcard origin");
    assert_eq!(bad_origin.code, ErrorCode::InvalidArgument);
    // The app serves calls.
    let c = client(&handle);
    insert(&c, Some("chat"), "hi")
        .await
        .expect("a write to chat");
    assert_eq!(settled(&c, Some("chat"), &["hi"]).await, ["hi"]);
    handle.stop().await;
}
live_test!(create_list_get_update_app);

/// Refuses every create's record write.
struct RefuseCreate;

impl FaultPlan for RefuseCreate {
    fn at(&self, op: &str, point: FaultPoint, _attempt: u32) -> Option<Fault> {
        (op == "live.apps.create" && point == FaultPoint::BeforePrewrite).then_some(Fault::Refuse)
    }
}

/// A create whose record write fails after its keyspace was ensured leaves
/// no record, and the janitor reports the keyspace as an orphan; the name
/// stays free.
async fn create_app_creates_keyspace_first(store: TestStore) {
    let keyspaces = store.keyspaces();
    let system = keyspaces
        .open(SYSTEM_KEYSPACE)
        .await
        .expect("the system keyspace");
    let faulty = Apps::with_store(
        system.clone().with_faults(Arc::new(RefuseCreate)),
        keyspaces.clone(),
        AppsOptions::default(),
    );
    let new = NewApp {
        name: "chat".into(),
        ..NewApp::default()
    };
    let err = faulty
        .create(new.clone())
        .await
        .expect_err("the record write fails");
    assert!(matches!(err, LiveError::Txn(_)), "{err}");
    let apps = Apps::with_store(system, keyspaces, AppsOptions::default());
    assert_eq!(apps.get_by_name("chat").await.expect("a read"), None);
    assert!(apps.list(None, 0).await.expect("a list").apps.is_empty());
    // Young intents are creates in progress, not orphans.
    let report = apps.janitor_once().await.expect("the janitor");
    assert!(report.orphans.is_empty(), "{report:?}");
    let later = u64::MAX / 2;
    let report = apps.janitor_at(later).await.expect("the janitor");
    assert_eq!(report.orphans, ["loams_live_a1"]);
    assert!(report.purged.is_empty());
    // The name was never taken.
    let chat = apps.create(new).await.expect("a create");
    assert_eq!((chat.id, chat.keyspace.as_str()), (2, "loams_live_a2"));
}
live_test!(create_app_creates_keyspace_first);

/// An app's engine starts on its first request and stops once idle; the
/// server's own app always runs.
async fn engine_starts_lazily_and_stops_when_idle(store: TestStore) {
    let idle = Duration::from_millis(300);
    let handle = server(&store, |c| c.idle_after = idle).await;
    let engines = handle.engines().clone();
    let a = admin(&handle);
    create(&a, "lazy").await;
    assert_eq!(engines.running(), ["main"], "created, not started");
    let c = client(&handle);
    insert(&c, Some("lazy"), "one").await.expect("a write");
    assert_eq!(engines.running(), ["lazy", "main"]);
    // Not idle yet: a reaper round leaves it.
    assert!(engines.reap_idle().await.is_empty());
    tokio::time::sleep(idle + Duration::from_millis(100)).await;
    // The reaper (every idle/4) stopped it, or this round does.
    engines.reap_idle().await;
    assert_eq!(engines.running(), ["main"]);
    // It starts again, with its data.
    assert_eq!(settled(&c, Some("lazy"), &["one"]).await, ["one"]);
    assert_eq!(engines.running(), ["lazy", "main"]);
    handle.stop().await;
}
live_test!(engine_starts_lazily_and_stops_when_idle);

/// Each app reads only its own documents, though every one has a `notes`
/// table; the server's own app is one of them.
async fn two_apps_never_see_each_others_documents(store: TestStore) {
    let handle = server(&store, |_| {}).await;
    let a = admin(&handle);
    create(&a, "left").await;
    create(&a, "right").await;
    let c = client(&handle);
    insert(&c, Some("left"), "l").await.expect("left");
    assert_eq!(
        bodies(&c, Some("right")).await.expect("right"),
        Vec::<String>::new()
    );
    insert(&c, Some("right"), "r").await.expect("right");
    insert(&c, None, "m").await.expect("main");
    assert_eq!(settled(&c, Some("left"), &["l"]).await, ["l"]);
    assert_eq!(settled(&c, Some("right"), &["r"]).await, ["r"]);
    assert_eq!(settled(&c, Some("main"), &["m"]).await, ["m"]);
    assert_eq!(settled(&c, None, &["m"]).await, ["m"]);
    // An unknown app is NOT_FOUND, a malformed header INVALID_ARGUMENT.
    let err = insert(&c, Some("nowhere"), "x").await.expect_err("no app");
    assert_eq!(err.code, ErrorCode::NotFound, "{err}");
    let err = insert(&c, Some("no-dashes"), "x")
        .await
        .expect_err("a bad name");
    assert_eq!(err.code, ErrorCode::InvalidArgument, "{err}");
    handle.stop().await;
}
live_test!(two_apps_never_see_each_others_documents);

/// A disabled app refuses every call with `live_app_disabled`, and serves
/// again once enabled.
async fn disabled_app_refuses_calls(store: TestStore) {
    let handle = server(&store, |_| {}).await;
    let a = admin(&handle);
    create(&a, "paused").await;
    let c = client(&handle);
    insert(&c, Some("paused"), "before").await.expect("a write");
    let set = |state: pb::AppState| pb::UpdateAppRequest {
        name: "paused".into(),
        state: Some(state.into()),
        ..Default::default()
    };
    let disabled = a
        .update_app(set(pb::AppState::APP_STATE_DISABLED))
        .await
        .expect("UpdateApp")
        .into_owned()
        .app
        .into_option()
        .expect("an app");
    assert_eq!(
        disabled.state.as_known(),
        Some(pb::AppState::APP_STATE_DISABLED)
    );
    assert_eq!(handle.engines().running(), ["main"], "its engine stopped");
    let err = insert(&c, Some("paused"), "after")
        .await
        .expect_err("disabled");
    assert_eq!(err.code, ErrorCode::FailedPrecondition, "{err}");
    let err = bodies(&c, Some("paused")).await.expect_err("disabled");
    assert_eq!(err.code, ErrorCode::FailedPrecondition, "{err}");
    let err = a
        .list_deployments_with_options(pb::ListDeploymentsRequest::default(), to(Some("paused")))
        .await
        .expect_err("the admin calls too");
    assert_eq!(err.code, ErrorCode::FailedPrecondition, "{err}");
    let refused = handle
        .engines()
        .resolve(Some("paused"))
        .await
        .expect_err("disabled");
    assert_eq!(refused.reason(), Some("live_app_disabled"));
    // Deleting is DeleteApp's.
    let err = a
        .update_app(set(pb::AppState::APP_STATE_DELETING))
        .await
        .expect_err("not through UpdateApp");
    assert_eq!(err.code, ErrorCode::InvalidArgument, "{err}");
    a.update_app(set(pb::AppState::APP_STATE_ACTIVE))
        .await
        .expect("enabled again");
    assert_eq!(settled(&c, Some("paused"), &["before"]).await, ["before"]);
    handle.stop().await;
}
live_test!(disabled_app_refuses_calls);

/// A deleted app is refused at once and purged after the retention: its
/// data deleted, its record gone, its name free.
async fn deleted_app_purged_after_retention(store: TestStore) {
    let handle = server(&store, |_| {}).await;
    let a = admin(&handle);
    let app = create(&a, "gone").await;
    let c = client(&handle);
    insert(&c, Some("gone"), "doomed").await.expect("a write");
    let deleted = a
        .delete_app(pb::DeleteAppRequest {
            name: "gone".into(),
            ..Default::default()
        })
        .await
        .expect("DeleteApp")
        .into_owned()
        .app
        .into_option()
        .expect("an app");
    assert_eq!(
        deleted.state.as_known(),
        Some(pb::AppState::APP_STATE_DELETING)
    );
    let retention = u64::try_from(loams_live::apps::DEFAULT_RETENTION.as_millis()).expect("ms");
    assert!(deleted.purge_after_ms >= deleted.created_at_ms + retention);
    let err = bodies(&c, Some("gone")).await.expect_err("deleted");
    assert_eq!(err.code, ErrorCode::FailedPrecondition, "{err}");
    // The server's own app cannot be deleted.
    let err = a
        .delete_app(pb::DeleteAppRequest {
            name: "main".into(),
            ..Default::default()
        })
        .await
        .expect_err("the server's own app");
    assert_eq!(err.code, ErrorCode::FailedPrecondition, "{err}");

    let engines = handle.engines();
    let early = engines
        .janitor_at(deleted.purge_after_ms - 1)
        .await
        .expect("the janitor");
    assert!(early.purged.is_empty(), "{early:?}");
    let report = engines
        .janitor_at(deleted.purge_after_ms)
        .await
        .expect("the janitor");
    assert_eq!(
        report
            .purged
            .iter()
            .map(|r| (r.name.as_str(), r.state))
            .collect::<Vec<_>>(),
        [(
            "gone",
            AppState::Deleting {
                purge_after_ms: deleted.purge_after_ms
            }
        )]
    );
    let err = a
        .get_app(pb::GetAppRequest {
            name: "gone".into(),
            ..Default::default()
        })
        .await
        .expect_err("purged");
    assert_eq!(err.code, ErrorCode::NotFound);
    // Its keyspace holds nothing.
    let data = engines
        .apps()
        .keyspaces()
        .open(&app.keyspace)
        .await
        .expect("the keyspace");
    let left = data
        .run(loams_kv::TxnOptions::new("test.scan"), |txn| {
            Box::pin(async move { txn.scan(&[], Some(&[0xFE]), 10).await })
        })
        .await
        .expect("a scan")
        .value;
    assert!(left.is_empty(), "{} keys left", left.len());
    // The name is free again, for a new app with a new keyspace.
    let again = create(&a, "gone").await;
    assert_ne!(again.keyspace, app.keyspace);
    assert_eq!(
        bodies(&c, Some("gone")).await.expect("a read"),
        Vec::<String>::new()
    );
    handle.stop().await;
}
live_test!(deleted_app_purged_after_retention);

/// `CreateApp` refuses a cluster's app beyond `max_apps`; the server's own
/// app counts.
async fn max_apps_per_cluster_enforced(store: TestStore) {
    let handle = server(&store, |c| {
        c.apps = AppsOptions {
            max_apps: 2,
            ..AppsOptions::default()
        }
    })
    .await;
    let a = admin(&handle);
    create(&a, "second").await;
    let err = a
        .create_app(pb::CreateAppRequest {
            name: "third".into(),
            ..Default::default()
        })
        .await
        .expect_err("over the limit");
    assert_eq!(err.code, ErrorCode::ResourceExhausted, "{err}");
    let refused = handle
        .engines()
        .apps()
        .create(NewApp {
            name: "third".into(),
            ..NewApp::default()
        })
        .await
        .expect_err("over the limit");
    assert!(
        matches!(
            refused,
            LiveError::LimitExceeded {
                limit: "max_apps_per_cluster",
                ..
            }
        ),
        "{refused}"
    );
    handle.stop().await;
}
live_test!(max_apps_per_cluster_enforced);
