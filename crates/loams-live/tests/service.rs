//! R1 plan Task 12: the connect-rust sync service, through the generated
//! `LiveServiceClient` over HTTP/1.1 and HTTP/2 (design §20 §7). The
//! loopback check needs no store; the rest are `live_test!`s, each starting
//! a `LiveServer` on the embedded store and on TiKV with `LOAMS_TEST_PD`
//! (LV1 plan Task 22).

use std::net::SocketAddr;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode};
use loams_kv::testing::TEST_LIVE;
use loams_live::pb::__buffa::oneof::query_set_change::Change;
use loams_live::pb::__buffa::oneof::query_update::Update;
use loams_live::pb::__buffa::oneof::watch_request::Start;
use loams_live::session::{ClientState, QueryResult, SESSION_HEADER, SessionConfig, Version};
use loams_live::system::{INSERT, QUERY};
use loams_live::testing::TestStore;
use loams_live::{
    LiveConfig, LiveError, LiveHandle, LiveServer, LiveValue, check_listen, live_test, pb,
};
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Watch = connectrpc::client::ServerStream<
    <HttpClient as connectrpc::client::ClientTransport>::ResponseBody,
    pb::__buffa::view::TransitionView<'static>,
>;

// ---- without a store ----

/// Semantics 7 and the loopback rule (D111): only loopback addresses pass.
#[test]
fn only_loopback_addresses_pass_the_listen_check() {
    for ok in [
        "127.0.0.1:7710",
        "127.1.2.3:0",
        "[::1]:0",
        "[::ffff:127.0.0.1]:1",
    ] {
        let addr: SocketAddr = ok.parse().expect("an address");
        check_listen(addr).unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    for bad in [
        "0.0.0.0:0",
        "192.168.1.10:7710",
        "[::]:0",
        "10.0.0.1:1",
        "[fe80::1]:0",
    ] {
        let addr: SocketAddr = bad.parse().expect("an address");
        let err = check_listen(addr).expect_err(bad);
        assert!(
            matches!(err, LiveError::NotLoopback(a) if a == addr),
            "{bad}: {err}"
        );
        assert!(
            err.to_string().contains("is not a loopback address")
                && err.to_string().contains("D111"),
            "{err}"
        );
    }
}

/// Semantics 7: `0.0.0.0:0` and a LAN address fail startup before anything
/// connects; `127.0.0.1:0` and `[::1]:0` start (on the test's store).
async fn non_loopback_bind_is_refused(store: TestStore) {
    for bad in ["0.0.0.0:0", "192.168.1.10:0"] {
        let mut config = LiveConfig::with_tikv(
            "t12",
            loams_kv::TikvConfig::new(vec!["127.0.0.1:1".into()], TEST_LIVE),
        );
        config.listen = bad.parse().expect("an address");
        let err = LiveServer::start(config, CancellationToken::new())
            .await
            .expect_err(bad);
        assert!(matches!(err, LiveError::NotLoopback(_)), "{bad}: {err}");
    }
    for ok in ["127.0.0.1:0", "[::1]:0"] {
        let mut config = store.live_config("t12");
        config.listen = ok.parse().expect("an address");
        let handle = LiveServer::start(config, CancellationToken::new())
            .await
            .unwrap_or_else(|e| panic!("{ok}: {e}"));
        assert!(handle.addr.ip().is_loopback());
        assert_ne!(handle.addr.port(), 0);
        handle.stop().await;
    }
}
live_test!(non_loopback_bind_is_refused);

/// LV1 row T20-9: a `LiveServer` starts on whatever store its config
/// names; only a TiKV store has a TiKV handle for the cluster GC loop.
async fn server_runs_on_the_configured_store(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    assert_eq!(handle.store().backend(), store.backend());
    assert_eq!(
        handle.store().as_tikv().is_some(),
        store.backend() == loams_kv::Backend::Tikv
    );
    handle.stop().await;
}
live_test!(server_runs_on_the_configured_store);

// ---- on every backend ----

/// A server for app `t12` on the test's store, on an ephemeral loopback port.
async fn server(store: &TestStore, session: SessionConfig) -> LiveHandle {
    let mut config = store.live_config("t12");
    config.listen = "127.0.0.1:0".parse().expect("an address");
    config.session = session;
    LiveServer::start(config, CancellationToken::new())
        .await
        .expect("the server starts")
}

fn client(handle: &LiveHandle, http2: bool) -> Client {
    let transport = if http2 {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    };
    let uri = format!("http://{}", handle.addr).parse().expect("a uri");
    pb::LiveServiceClient::new(transport, ClientConfig::new(uri))
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn spec(query_id: u32, table: &str) -> pb::QuerySpec {
    pb::QuerySpec {
        query_id,
        function: QUERY.into(),
        args: MessageField::some(obj(&[("table", LiveValue::Str(table.into()))]).to_proto()),
        ..Default::default()
    }
}

fn set(version: u64, queries: Vec<pb::QuerySpec>) -> pb::QuerySet {
    pb::QuerySet {
        version,
        queries,
        ..Default::default()
    }
}

async fn watch(c: &Client, start: Start) -> Watch {
    c.watch(pb::WatchRequest {
        start: Some(start),
        ..Default::default()
    })
    .await
    .expect("Watch opens")
}

async fn next(w: &mut Watch) -> pb::Transition {
    tokio::time::timeout(Duration::from_secs(20), w.message())
        .await
        .expect("a Transition within 20 s")
        .expect("the stream")
        .expect("not the end")
        .to_owned_message()
}

/// Receives Transitions (applying each to `client`) until `done` holds.
async fn until(
    w: &mut Watch,
    client: &mut ClientState,
    mut done: impl FnMut(&ClientState, &pb::Transition) -> bool,
) -> pb::Transition {
    loop {
        let t = next(w).await;
        client
            .apply(&t)
            .expect("each Transition starts at the last one's end");
        if done(client, &t) {
            return t;
        }
    }
}

async fn insert(c: &Client, table: &str, n: i64, session: Option<&str>) -> u64 {
    let req = pb::MutateRequest {
        function: INSERT.into(),
        args: MessageField::some(
            obj(&[
                ("table", LiveValue::Str(table.into())),
                ("fields", obj(&[("n", LiveValue::I64(n))])),
            ])
            .to_proto(),
        ),
        ..Default::default()
    };
    let mut options = CallOptions::default();
    if let Some(id) = session {
        options = options.with_header(SESSION_HEADER, id);
    }
    c.mutate_with_options(req, options)
        .await
        .expect("Mutate commits")
        .into_owned()
        .commit_ts
}

fn docs(client: &ClientState, q: u32) -> usize {
    match client.results.get(&q) {
        Some(QueryResult::Value(LiveValue::Array(docs))) => docs.len(),
        other => panic!("query {q}: {other:?}"),
    }
}

fn end(t: &pb::Transition) -> Version {
    Version::from_proto(t.end.as_option())
}

/// Semantics 1 and 6, over HTTP/1.1 and HTTP/2: the first Transition starts
/// at the zero version with every result; a Mutate's write reaches the
/// session in a Transition at or after its commit timestamp.
async fn watch_receives_update_after_mutate(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    for http2 in [false, true] {
        let c = client(&handle, http2);
        let table = if http2 { "msgs2" } else { "msgs1" };
        let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(7, table)])))).await;
        let first = next(&mut w).await;
        assert_eq!(
            Version::from_proto(first.start.as_option()),
            Version::default()
        );
        assert_eq!(end(&first).query_set, 1);
        let mut state = ClientState::new();
        state.apply(&first).expect("from the zero version");
        assert_eq!(docs(&state, 7), 0);
        let commit_ts = insert(&c, table, 1, None).await;
        let t = until(&mut w, &mut state, |s, _| docs(s, 7) == 1).await;
        assert!(end(&t).ts >= commit_ts, "http2 {http2}");
        // A one-shot Query at the latest tick agrees.
        let q = c
            .query(pb::QueryRequest {
                function: QUERY.into(),
                args: spec(0, table).args,
                ..Default::default()
            })
            .await
            .expect("Query")
            .into_owned();
        let result = LiveValue::from_proto(q.result.into_option().expect("a result")).expect("ok");
        assert!(matches!(result, LiveValue::Array(d) if d.len() == 1));
    }
    handle.stop().await;
}
live_test!(watch_receives_update_after_mutate);

/// Semantics 1 and §20 §8.2 step 3: two sessions watching one query share
/// one subscription, and one write reaches both at the same tick.
async fn two_sessions_share_one_tick(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    let c = client(&handle, true);
    let mut a = watch(
        &c,
        Start::Initial(Box::new(set(1, vec![spec(1, "shared")]))),
    )
    .await;
    let mut b = watch(
        &c,
        Start::Initial(Box::new(set(1, vec![spec(9, "shared")]))),
    )
    .await;
    let (mut sa, mut sb) = (ClientState::new(), ClientState::new());
    sa.apply(&next(&mut a).await).expect("a");
    sb.apply(&next(&mut b).await).expect("b");
    assert_eq!(handle.stats().subscriptions, 1, "one shared subscription");
    assert_eq!(handle.sessions(), 2);
    insert(&c, "shared", 1, None).await;
    let ta = until(&mut a, &mut sa, |s, _| docs(s, 1) == 1).await;
    let tb = until(&mut b, &mut sb, |s, _| docs(s, 9) == 1).await;
    assert_eq!(end(&ta).ts, end(&tb).ts, "one tick for both sessions");
    drop(a);
    drop(b);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while handle.sessions() > 0 || handle.stats().subscriptions > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sessions end when clients go"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await;
}
live_test!(two_sessions_share_one_tick);

/// Semantics 2: `ModifyQuerySet` adds and removes queries from the
/// session's current query-set version (else FAILED_PRECONDITION), and the
/// next Transition reflects it.
async fn modify_query_set_adds_and_removes(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    let c = client(&handle, false);
    let second_ts = insert(&c, "second", 1, None).await;
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(1, "first")])))).await;
    let first = next(&mut w).await;
    let mut state = ClientState::new();
    state.apply(&first).expect("first");
    let session = first.session_id.clone();
    let modify = |base: u64, new: u64, changes: Vec<Change>| pb::ModifyQuerySetRequest {
        session_id: session.clone(),
        base_version: base,
        new_version: new,
        changes: changes
            .into_iter()
            .map(|c| pb::QuerySetChange {
                change: Some(c),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    c.modify_query_set(modify(1, 2, vec![Change::Add(Box::new(spec(2, "second")))]))
        .await
        .expect("an add");
    let t = next(&mut w).await;
    state.apply(&t).expect("the add");
    assert_eq!(end(&t).query_set, 2);
    assert!(
        t.updates.iter().any(|u| u.query_id == 2),
        "the added query's result"
    );
    // Results are those of the session's tick, which reads 50 ms back: the
    // insert shows once the tick passes its commit.
    if end(&t).ts < second_ts {
        until(&mut w, &mut state, |s, _| docs(s, 2) == 1).await;
    }
    assert_eq!(docs(&state, 2), 1);
    assert_eq!(docs(&state, 1), 0);
    let stale = c
        .modify_query_set(modify(1, 3, vec![Change::Remove(1)]))
        .await
        .expect_err("a stale base version");
    assert_eq!(stale.code, ErrorCode::FailedPrecondition, "{stale:?}");
    c.modify_query_set(modify(2, 3, vec![Change::Remove(1)]))
        .await
        .expect("a remove");
    let t = next(&mut w).await;
    assert!(
        t.updates
            .iter()
            .any(|u| u.query_id == 1 && matches!(u.update, Some(Update::Removed(_))))
    );
    state.apply(&t).expect("the remove");
    assert_eq!(end(&t).query_set, 3);
    assert_eq!(state.results.keys().copied().collect::<Vec<_>>(), vec![2]);
    // The removed query no longer follows writes; the kept one does.
    insert(&c, "first", 1, None).await;
    insert(&c, "second", 2, None).await;
    until(&mut w, &mut state, |s, _| docs(s, 2) == 2).await;
    assert!(!state.results.contains_key(&1));
    let unknown = c
        .modify_query_set(pb::ModifyQuerySetRequest {
            session_id: "1-nope".into(),
            ..Default::default()
        })
        .await
        .expect_err("no such session");
    assert_eq!(unknown.code, ErrorCode::NotFound);
    handle.stop().await;
}
live_test!(modify_query_set_adds_and_removes);

/// Semantics 4: empty Transitions arrive every `heartbeat`, and a session
/// with a pending mutation gets ts-only Transitions up to its commit.
async fn heartbeats_arrive(store: TestStore) {
    let handle = server(
        &store,
        SessionConfig {
            heartbeat: Duration::from_millis(300),
            ts_only_interval: Duration::from_millis(200),
            ..SessionConfig::default()
        },
    )
    .await;
    let c = client(&handle, true);
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![spec(1, "quiet")])))).await;
    let mut state = ClientState::new();
    let first = next(&mut w).await;
    state.apply(&first).expect("first");
    let started = std::time::Instant::now();
    let beat = next(&mut w).await;
    state.apply(&beat).expect("a heartbeat");
    assert!(beat.updates.is_empty());
    assert_eq!(beat.start, beat.end, "a heartbeat keeps the version");
    assert!(started.elapsed() >= Duration::from_millis(200));
    // A mutation of this session on another table: ts-only Transitions
    // carry the session's ts to its commit, with no update.
    let commit_ts = insert(&c, "elsewhere", 1, Some(&first.session_id)).await;
    let t = until(&mut w, &mut state, |_, t| end(t).ts >= commit_ts).await;
    assert!(t.updates.is_empty(), "{t:?}");
    handle.stop().await;
}
live_test!(heartbeats_arrive);

/// Semantics 5 (reconnect): a client that lost its stream resumes from its
/// last version and converges to the writes made while it was away.
async fn watch_resumes_after_reconnect(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    let c = client(&handle, false);
    let queries = vec![spec(1, "resumed"), spec(2, "resumed_other")];
    let mut w = watch(&c, Start::Initial(Box::new(set(4, queries.clone())))).await;
    let mut state = ClientState::new();
    state.apply(&next(&mut w).await).expect("first");
    let last = state.version;
    drop(w);
    let commit_ts = insert(&c, "resumed", 1, None).await;
    let mut w = watch(
        &c,
        Start::Resume(Box::new(pb::Resume {
            last_version: MessageField::some(last.to_proto()),
            query_set: MessageField::some(set(4, queries)),
            ..Default::default()
        })),
    )
    .await;
    let first = next(&mut w).await;
    assert_eq!(Version::from_proto(first.start.as_option()), last);
    assert!(end(&first).ts >= last.ts);
    assert_eq!(first.updates.len(), 2, "full results");
    state.apply(&first).expect("applies at the last version");
    let t = until(&mut w, &mut state, |s, _| docs(s, 1) == 1).await;
    assert!(end(&t).ts >= commit_ts);
    assert_eq!(docs(&state, 2), 0);
    handle.stop().await;
}
live_test!(watch_resumes_after_reconnect);

/// Per-query errors stay inside the stream; call errors use Connect codes;
/// a server without a function runtime refuses `Deploy` (LV1 Task 6, now on
/// `LiveAdminService`, Task 10).
async fn errors_map_to_connect_codes(store: TestStore) {
    let handle = server(&store, SessionConfig::default()).await;
    let c = client(&handle, false);
    let unknown = pb::QuerySpec {
        query_id: 3,
        function: "app:missing".into(),
        ..Default::default()
    };
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![unknown])))).await;
    let mut state = ClientState::new();
    state.apply(&next(&mut w).await).expect("first");
    assert!(matches!(
        &state.results[&3],
        QueryResult::Error(e) if e.code.as_known() == Some(pb::ErrorCode::ERROR_CODE_NOT_FOUND)
    ));
    let e: ConnectError = c
        .mutate(pb::MutateRequest {
            function: QUERY.into(),
            ..Default::default()
        })
        .await
        .expect_err("a query is not a mutation");
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    let e = admin(&handle, false)
        .deploy(pb::DeployRequest::default())
        .await
        .expect_err("this server has no function runtime");
    assert_eq!(e.code, ErrorCode::FailedPrecondition);
    let dup = c
        .watch(pb::WatchRequest {
            start: Some(Start::Initial(Box::new(set(
                1,
                vec![spec(1, "a"), spec(1, "b")],
            )))),
            ..Default::default()
        })
        .await;
    let err = match dup {
        Err(e) => e,
        Ok(mut s) => s.message().await.expect_err("a duplicate query id"),
    };
    assert_eq!(err.code, ErrorCode::InvalidArgument);
    handle.stop().await;
}
live_test!(errors_map_to_connect_codes);

// ---- LV1 plan Task 10: LiveAdminService and function logs ----

type Admin = pb::LiveAdminServiceClient<HttpClient>;

fn admin(handle: &LiveHandle, json: bool) -> Admin {
    let uri = format!("http://{}", handle.addr).parse().expect("a uri");
    let config = if json {
        ClientConfig::new(uri).json()
    } else {
        ClientConfig::new(uri)
    };
    pb::LiveAdminServiceClient::new(HttpClient::plaintext(), config)
}

/// Version `n` of the notes app. Its functions write console lines.
fn notes_bundle(n: u32) -> Vec<u8> {
    format!(
        r#"
import {{ query, mutation, internalQuery, v }} from "loams:server";
export const notes = {{
  version: query(async () => {n}n),
  add: mutation({{
    args: {{ body: v.string() }},
    handler: async (ctx, {{ body }}) => {{
      console.log("adding a note, version {n}");
      console.warn("second line");
      return await ctx.db.insert("notes", {{ body }});
    }},
  }}),
  count: query(async (ctx) => {{
    console.info("counting");
    return (await ctx.db.query("notes").collect()).length;
  }}),
  fail: mutation(async () => {{ throw new Error("no notes today"); }}),
  audit: internalQuery(async () => null),
}};
"#
    )
    .into_bytes()
}

fn notes_schema() -> pb::Schema {
    pb::Schema {
        tables: vec![pb::TableSchema {
            name: "notes".into(),
            indexes: vec![pb::IndexSchema {
                name: "by_body".into(),
                fields: vec!["body".into()],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A server for app `t10` with the in-process function runtime.
async fn js_server(store: &TestStore) -> LiveHandle {
    let mut config = store.live_config("t10");
    config.listen = "127.0.0.1:0".parse().expect("an address");
    let loader = loams_live_js::JsLoader::new(loams_live_js::JsConfig {
        contexts: 1,
        ..loams_live_js::JsConfig::default()
    })
    .expect("an in-process loader");
    LiveServer::start_with(
        config,
        loams_live::deploy::Runtime {
            bundles: loams_store::Store::in_memory(),
            loader: std::sync::Arc::new(loader),
        },
        CancellationToken::new(),
    )
    .await
    .expect("the server starts")
}

async fn deploy_notes(a: &Admin, n: u32, expected_active: Option<&str>) -> pb::Deployment {
    a.deploy(pb::DeployRequest {
        bundle: notes_bundle(n),
        schema: MessageField::some(notes_schema()),
        expected_active: expected_active.map(str::to_string),
        message: format!("version {n}"),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|e| panic!("deploying version {n}: {e:?}"))
    .into_owned()
    .deployment
    .into_option()
    .expect("a deployment")
}

fn state(d: &pb::Deployment) -> pb::DeploymentState {
    d.state.as_known().expect("a known state")
}

/// Deploy, list, get, activate and index status over `LiveAdminService`, in
/// both of Connect's codecs: binary deploys the first version, JSON the
/// second, binary rolls back, and JSON reads the result.
async fn deploy_over_admin_service_roundtrip(store: TestStore) {
    use sha2::Digest;
    let handle = js_server(&store).await;
    let binary = admin(&handle, false);
    let json = admin(&handle, true);

    // A dry run writes nothing and reports the plan.
    let dry = binary
        .deploy(pb::DeployRequest {
            bundle: notes_bundle(1),
            schema: MessageField::some(notes_schema()),
            dry_run: true,
            ..Default::default()
        })
        .await
        .expect("a dry run")
        .into_owned();
    let plan = dry.changes.into_option().expect("changes");
    assert_eq!(plan.tables_created, ["notes"]);
    assert_eq!(
        state(dry.deployment.as_option().expect("the record")),
        pb::DeploymentState::DEPLOYMENT_STATE_PENDING
    );
    let none = json
        .list_deployments(pb::ListDeploymentsRequest::default())
        .await
        .expect("the list")
        .into_owned();
    assert!(none.deployments.is_empty(), "{none:?}");

    let v1 = deploy_notes(&binary, 1, None).await;
    assert_eq!(state(&v1), pb::DeploymentState::DEPLOYMENT_STATE_ACTIVE);
    assert_eq!(v1.message, "version 1");
    assert_eq!(v1.created_by, loams_live::deploy::UNAUTHENTICATED);
    assert_eq!(v1.parent, "");
    assert_eq!(
        v1.bundle_sha256,
        data_encoding::HEXLOWER.encode(&sha2::Sha256::digest(notes_bundle(1)))
    );

    // A stale `expected_active` is the deploy conflict.
    let stale = json
        .deploy(pb::DeployRequest {
            bundle: notes_bundle(2),
            schema: MessageField::some(notes_schema()),
            ..Default::default()
        })
        .await
        .expect_err("no deployment is not the active one");
    assert_eq!(stale.code, ErrorCode::FailedPrecondition, "{stale:?}");
    assert!(
        stale.message.as_deref().unwrap_or("").contains(&v1.id),
        "{stale:?}"
    );
    let v2 = deploy_notes(&json, 2, Some(&v1.id)).await;
    assert_eq!(v2.parent, v1.id);

    // Newest first, a page at a time: v2, the conflicting deploy's failed
    // record, v1.
    let first = binary
        .list_deployments(pb::ListDeploymentsRequest {
            page_size: 1,
            ..Default::default()
        })
        .await
        .expect("page 1")
        .into_owned();
    let ids: Vec<&str> = first.deployments.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(ids, [v2.id.as_str()]);
    assert!(!first.next_page_token.is_empty());
    let rest = json
        .list_deployments(pb::ListDeploymentsRequest {
            page_size: 5,
            page_token: first.next_page_token.clone(),
            ..Default::default()
        })
        .await
        .expect("page 2")
        .into_owned();
    assert_eq!(rest.deployments.len(), 2, "{rest:?}");
    assert!(rest.next_page_token.is_empty(), "the last page");
    let lost = &rest.deployments[0];
    assert_eq!(state(lost), pb::DeploymentState::DEPLOYMENT_STATE_FAILED);
    assert!(lost.failure_reason.contains(&v1.id), "{lost:?}");
    assert_eq!(rest.deployments[1].id, v1.id);
    assert_eq!(
        state(&rest.deployments[1]),
        pb::DeploymentState::DEPLOYMENT_STATE_SUPERSEDED
    );

    // Rollback: activate v1 against v2.
    let back = binary
        .activate_deployment(pb::ActivateDeploymentRequest {
            id: v1.id.clone(),
            expected_active: Some(v2.id.clone()),
            ..Default::default()
        })
        .await
        .expect("the rollback")
        .into_owned();
    let back = back.deployment.into_option().expect("a deployment");
    assert_eq!(back.id, v1.id);
    assert_eq!(state(&back), pb::DeploymentState::DEPLOYMENT_STATE_ACTIVE);
    let c = client(&handle, false);
    let version = c
        .query(pb::QueryRequest {
            function: "notes:version".into(),
            args: MessageField::some(obj(&[]).to_proto()),
            ..Default::default()
        })
        .await
        .expect("notes:version")
        .into_owned();
    assert_eq!(
        LiveValue::from_proto(version.result.into_option().expect("a result")).expect("a value"),
        LiveValue::I64(1)
    );

    // A deployment with its schema and its functions, by path.
    let got = json
        .get_deployment(pb::GetDeploymentRequest {
            id: v2.id.clone(),
            ..Default::default()
        })
        .await
        .expect("GetDeployment")
        .into_owned();
    assert_eq!(
        state(got.deployment.as_option().expect("the deployment")),
        pb::DeploymentState::DEPLOYMENT_STATE_SUPERSEDED
    );
    assert_eq!(
        got.schema.as_option().expect("a schema").tables[0].name,
        "notes"
    );
    let paths: Vec<&str> = got.functions.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "notes:add",
            "notes:audit",
            "notes:count",
            "notes:fail",
            "notes:version"
        ]
    );
    let add = &got.functions[0];
    assert_eq!(
        add.kind.as_known(),
        Some(pb::FunctionKind::FUNCTION_KIND_MUTATION)
    );
    let fields = &add.args.as_option().expect("an args validator");
    assert!(matches!(
        &fields.kind,
        Some(pb::__buffa::oneof::validator::Kind::Object(o)) if o.fields.contains_key("body")
    ));
    assert_eq!(
        got.functions[1].visibility.as_known(),
        Some(pb::FunctionVisibility::FUNCTION_VISIBILITY_INTERNAL)
    );
    assert!(got.functions[2].args.as_option().is_none());

    // Index status: the declared index is ready; an unknown one is not
    // found.
    let status = binary
        .get_index_status(pb::GetIndexStatusRequest {
            table: "notes".into(),
            index: "by_body".into(),
            ..Default::default()
        })
        .await
        .expect("GetIndexStatus")
        .into_owned();
    assert_eq!(
        status.state.as_known(),
        Some(pb::IndexState::INDEX_STATE_READY)
    );
    let missing = json
        .get_index_status(pb::GetIndexStatusRequest {
            table: "notes".into(),
            index: "by_nothing".into(),
            ..Default::default()
        })
        .await
        .expect_err("no such index");
    assert_eq!(missing.code, ErrorCode::NotFound);

    // Malformed ids are invalid arguments; unknown ones are not found.
    for bad in ["", "not-a-ulid"] {
        let e = binary
            .get_deployment(pb::GetDeploymentRequest {
                id: bad.into(),
                ..Default::default()
            })
            .await
            .expect_err(bad);
        assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad}: {e:?}");
    }
    let e = json
        .activate_deployment(pb::ActivateDeploymentRequest {
            id: ulid::Ulid::from_parts(1, 1).to_string(),
            expected_active: Some(v1.id.clone()),
            ..Default::default()
        })
        .await
        .expect_err("an unknown deployment");
    assert_eq!(e.code, ErrorCode::NotFound);
    handle.stop().await;
}
live_test!(deploy_over_admin_service_roundtrip);

/// D689: `Deploy` left `LiveService`. The RPC is not routed, and the
/// listener's reflection lists `LiveService` without it and
/// `LiveAdminService` with it.
async fn live_service_has_no_deploy(store: TestStore) {
    use connectrpc_reflection::ServerReflectionClient;
    use connectrpc_reflection::wire::v1::ServerReflectionRequest;
    use connectrpc_reflection::wire::v1::server_reflection_request::MessageRequest;
    use connectrpc_reflection::wire::v1::server_reflection_response::MessageResponse;

    let handle = server(&store, SessionConfig::default()).await;
    let uri: axum::http::Uri = format!("http://{}", handle.addr).parse().expect("a uri");
    let reflection =
        ServerReflectionClient::new(HttpClient::plaintext_http2_only(), ClientConfig::new(uri));
    let ask = |request: MessageRequest| ServerReflectionRequest {
        message_request: Some(request),
        ..Default::default()
    };
    let mut stream = reflection
        .server_reflection_info()
        .await
        .expect("reflection");
    for request in [
        MessageRequest::ListServices(String::new()),
        MessageRequest::FileContainingSymbol("loams.live.v1.LiveService.Deploy".into()),
        MessageRequest::FileContainingSymbol("loams.live.v1.LiveService.Mutate".into()),
        MessageRequest::FileContainingSymbol("loams.live.v1.LiveAdminService.Deploy".into()),
    ] {
        stream
            .send(ask(request))
            .await
            .expect("a reflection request");
    }
    stream.close_send();
    let mut answers = Vec::new();
    while let Some(answer) = stream.message().await.expect("an answer") {
        answers.push(
            answer
                .to_owned_message()
                .message_response
                .expect("a response"),
        );
    }
    assert_eq!(answers.len(), 4, "{answers:?}");
    let MessageResponse::ListServicesResponse(list) = &answers[0] else {
        panic!("{:?}", answers[0]);
    };
    let names: Vec<&str> = list.service.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"loams.live.v1.LiveService"), "{names:?}");
    assert!(
        names.contains(&"loams.live.v1.LiveAdminService"),
        "{names:?}"
    );
    assert!(
        matches!(&answers[1], MessageResponse::ErrorResponse(_)),
        "LiveService.Deploy is gone: {:?}",
        answers[1]
    );
    assert!(
        matches!(&answers[2], MessageResponse::FileDescriptorResponse(_)),
        "{:?}",
        answers[2]
    );
    assert!(
        matches!(&answers[3], MessageResponse::FileDescriptorResponse(_)),
        "{:?}",
        answers[3]
    );
    // The old path is not routed.
    let routed = post_status(handle.addr, "/loams.live.v1.LiveService/Deploy").await;
    assert_eq!(routed, 404, "LiveService/Deploy answered {routed}");
    handle.stop().await;
}
live_test!(live_service_has_no_deploy);

/// The status of a Connect JSON POST of `{}` to `path`, over a plain TCP
/// HTTP/1.1 exchange (the dev-dependencies have no HTTP client but
/// connect-rust's, which only speaks generated paths).
async fn post_status(addr: SocketAddr, path: &str) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "POST {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\n\
         content-length: 2\r\nconnection: close\r\n\r\n{{}}"
    );
    tcp.write_all(request.as_bytes()).await.expect("write");
    let mut answer = Vec::new();
    tcp.read_to_end(&mut answer).await.expect("read");
    let text = String::from_utf8_lossy(&answer);
    text.split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("an HTTP status line: {text}"))
}

/// Reads `TailLogs` without following, to its end.
async fn tail(a: &Admin, request: pb::TailLogsRequest) -> Vec<pb::LogEntry> {
    let mut stream = a.tail_logs(request).await.expect("TailLogs");
    let mut out = Vec::new();
    loop {
        let batch = tokio::time::timeout(Duration::from_secs(20), stream.message())
            .await
            .expect("an answer within 20 s")
            .expect("the stream");
        match batch {
            None => return out,
            Some(b) => out.extend(b.to_owned_message().entries),
        }
    }
}

/// Runs mutation `function` with `args`, and returns its request id (or
/// the error's) and whether it committed.
async fn call(c: &Client, function: &str, args: LiveValue) -> (String, bool) {
    let request = pb::MutateRequest {
        function: function.into(),
        args: MessageField::some(args.to_proto()),
        ..Default::default()
    };
    match c.mutate(request).await {
        Ok(r) => (header_id(r.headers()), true),
        Err(e) => (header_id(e.response_headers()), false),
    }
}

fn header_id(headers: &axum::http::HeaderMap) -> String {
    headers
        .get(loams_live::logs::REQUEST_ID_HEADER)
        .expect("a loams-request-id header")
        .to_str()
        .expect("text")
        .to_string()
}

/// §45 §3.3: console output reaches `TailLogs` with the call's request id
/// (the `loams-request-id` header), its function and its deployment; a
/// call that throws leaves its error; a follower sees new lines as they
/// come; and arguments never appear.
async fn tail_logs_streams_console_output_with_request_ids(store: TestStore) {
    let handle = js_server(&store).await;
    let a = admin(&handle, false);
    let c = client(&handle, false);
    let v1 = deploy_notes(&a, 1, None).await;
    let canary = "canary-7f3a";
    let (added, ok) = call(
        &c,
        "notes:add",
        obj(&[("body", LiveValue::Str(canary.into()))]),
    )
    .await;
    assert!(ok);
    let (failed, ok) = call(&c, "notes:fail", obj(&[])).await;
    assert!(!ok);
    assert_ne!(added, failed);

    let lines = tail(&a, pb::TailLogsRequest::default()).await;
    let of =
        |id: &str| -> Vec<&pb::LogEntry> { lines.iter().filter(|l| l.request_id == id).collect() };
    let add = of(&added);
    assert_eq!(add.len(), 2, "{lines:?}");
    assert_eq!(add[0].line, "adding a note, version 1");
    assert_eq!(add[0].level.as_known(), Some(pb::LogLevel::LOG_LEVEL_LOG));
    assert_eq!(add[1].level.as_known(), Some(pb::LogLevel::LOG_LEVEL_WARN));
    assert_eq!(add[0].function, "notes:add");
    assert_eq!(add[0].deployment, v1.id);
    assert!(add[0].ts_ms > 0);
    let fail = of(&failed);
    assert_eq!(fail.len(), 1, "{lines:?}");
    assert_eq!(
        fail[0].level.as_known(),
        Some(pb::LogLevel::LOG_LEVEL_ERROR)
    );
    assert!(fail[0].line.contains("no notes today"), "{:?}", fail[0]);
    assert!(
        lines.iter().all(|l| !l.line.contains(canary)),
        "arguments never reach the logs: {lines:?}"
    );

    // A follower gets the lines that are there, then new ones.
    let mut follow = a
        .tail_logs(pb::TailLogsRequest {
            follow: true,
            ..Default::default()
        })
        .await
        .expect("a follow");
    let mut seen = Vec::new();
    while seen.len() < lines.len() {
        let batch = tokio::time::timeout(Duration::from_secs(20), follow.message())
            .await
            .expect("the backlog")
            .expect("the stream")
            .expect("not the end");
        seen.extend(batch.to_owned_message().entries);
    }
    let counted = c
        .query(pb::QueryRequest {
            function: "notes:count".into(),
            args: MessageField::some(obj(&[]).to_proto()),
            ..Default::default()
        })
        .await
        .expect("notes:count");
    let counted_id = header_id(counted.headers());
    let fresh = loop {
        let batch = tokio::time::timeout(Duration::from_secs(20), follow.message())
            .await
            .expect("a new line within 20 s")
            .expect("the stream")
            .expect("not the end")
            .to_owned_message();
        if let Some(line) = batch.entries.into_iter().next() {
            break line;
        }
    };
    assert_eq!(fresh.request_id, counted_id);
    assert_eq!(fresh.line, "counting");
    assert_eq!(fresh.level.as_known(), Some(pb::LogLevel::LOG_LEVEL_INFO));

    // A subscription's evaluation logs under a request id of its own.
    let count = pb::QuerySpec {
        query_id: 1,
        function: "notes:count".into(),
        args: MessageField::some(obj(&[]).to_proto()),
        ..Default::default()
    };
    let mut w = watch(&c, Start::Initial(Box::new(set(1, vec![count])))).await;
    next(&mut w).await;
    let sub = loop {
        let batch = tokio::time::timeout(Duration::from_secs(20), follow.message())
            .await
            .expect("the subscription's line within 20 s")
            .expect("the stream")
            .expect("not the end")
            .to_owned_message();
        if let Some(line) = batch
            .entries
            .into_iter()
            .find(|l| l.function == "notes:count")
        {
            break line;
        }
    };
    assert!(!sub.request_id.is_empty());
    assert_ne!(sub.request_id, counted_id);
    handle.stop().await;
}
live_test!(tail_logs_streams_console_output_with_request_ids);

/// `TailLogs{function, since_ms}` reads only that function's lines, and
/// only lines at or after the time.
async fn tail_logs_filters_by_function(store: TestStore) {
    let handle = js_server(&store).await;
    let a = admin(&handle, true);
    let c = client(&handle, false);
    deploy_notes(&a, 1, None).await;
    let (added, _) = call(
        &c,
        "notes:add",
        obj(&[("body", LiveValue::Str("x".into()))]),
    )
    .await;
    call(&c, "notes:fail", obj(&[])).await;
    let adds = tail(
        &a,
        pb::TailLogsRequest {
            function: Some("notes:add".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(adds.len(), 2, "{adds:?}");
    assert!(
        adds.iter()
            .all(|l| l.function == "notes:add" && l.request_id == added)
    );
    let fails = tail(
        &a,
        pb::TailLogsRequest {
            function: Some("notes:fail".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(fails.len(), 1, "{fails:?}");
    let none = tail(
        &a,
        pb::TailLogsRequest {
            function: Some("notes:nothing".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(none.is_empty());
    let later = tail(
        &a,
        pb::TailLogsRequest {
            since_ms: Some(adds[0].ts_ms + 3_600_000),
            ..Default::default()
        },
    )
    .await;
    assert!(later.is_empty(), "{later:?}");
    let since = tail(
        &a,
        pb::TailLogsRequest {
            since_ms: Some(adds[0].ts_ms),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(since.len(), 3);
    handle.stop().await;
}
live_test!(tail_logs_filters_by_function);

/// The ring keeps the newest lines: the oldest go first, are counted, and
/// a reader that started before them is told how many it missed.
#[tokio::test]
async fn logs_ring_drops_oldest_and_counts() {
    use futures::StreamExt;
    use loams_live::logs::{CallInfo, RING_LINES, Ring, TailFilter};
    use loams_live::{CallOutput, LogLevel, LogLine};

    assert_eq!(Ring::default().capacity(), RING_LINES);
    assert_eq!(RING_LINES, 10_000);
    let ring = std::sync::Arc::new(Ring::new(3));
    let line = |n: u32| CallOutput {
        logs: vec![LogLine {
            level: LogLevel::Log,
            line: format!("line {n}"),
            truncated: false,
        }],
        dropped: 0,
    };
    let call = CallInfo {
        request_id: Some("r"),
        function: "m:f",
        deployment: ulid::Ulid::nil(),
    };
    ring.record(&call, Ok(&line(0)));
    let before = ring.read(0, &TailFilter::default(), 100);
    assert_eq!((before.entries.len(), before.skipped), (1, 0));
    for n in 1..5 {
        ring.record(&call, Ok(&line(n)));
    }
    assert_eq!(ring.len(), 3);
    assert_eq!(ring.dropped(), 2);
    let read = ring.read(0, &TailFilter::default(), 100);
    assert_eq!(read.skipped, 2);
    assert_eq!(
        read.entries
            .iter()
            .map(|e| e.line.as_str())
            .collect::<Vec<_>>(),
        ["line 2", "line 3", "line 4"]
    );
    assert_eq!(read.entries[0].seq, 2);
    assert!(read.entries[0].deployment.is_none(), "a built-in");
    // A tail starts at the oldest line held, so it skips nothing.
    let tailed: Vec<_> = ring
        .tail(TailFilter::default(), false, CancellationToken::new())
        .collect()
        .await;
    assert_eq!(tailed.len(), 1);
    assert_eq!((tailed[0].entries.len(), tailed[0].skipped), (3, 0));
}
