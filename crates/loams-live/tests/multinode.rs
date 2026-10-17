//! LV1 plan Task 24: sessions across several Live nodes (design §45 §9),
//! through two real servers on one store (both backends). A session call
//! that lands on the node that does not hold the session is forwarded to
//! the one that does; an owner that is gone is `session_gone`; a draining
//! node closes its streams with `node_draining`; and a deploy on one node
//! switches the other's sessions at the same timestamp.
//!
//! A test guard stands in for the engine's authentication layer:
//! `Authorization: Bearer user:<id>` is the platform user `<id>`. It is the
//! client's own header, so a forwarded call reaches the owner's guard with
//! it, as it reaches the owner's engine layer in production.

use std::sync::Arc;
use std::time::{Duration, Instant};

use buffa::MessageField;
use connectrpc::ConnectError;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use loams_live::auth::{Caller, CallerKind};
use loams_live::forward::{FORWARDED_HEADER, ForwardConfig, Nodes};
use loams_live::session::{ClientState, QueryResult};
use loams_live::testing::TestStore;
use loams_live::{LiveHandle, LiveServer, LiveValue, live_test, pb};
use tokio_util::sync::CancellationToken;

type Client = pb::LiveServiceClient<HttpClient>;
type Admin = pb::LiveAdminServiceClient<HttpClient>;
type Watch = connectrpc::client::ServerStream<
    <HttpClient as connectrpc::client::ClientTransport>::ResponseBody,
    pb::__buffa::view::TransitionView<'static>,
>;

/// The servers' own app.
const APP: &str = "main";

async fn test_layer(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let user = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer user:"))
        .map(str::to_owned);
    if let Some(user) = user {
        request
            .extensions_mut()
            .insert(Caller::new(user, CallerKind::User));
    }
    next.run(request).await
}

fn bundle(version: u32) -> Vec<u8> {
    format!(
        r#"
import {{ query, mutation }} from "loams:server";
export const notes = {{
  version: query(async () => {version}n),
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
  add: mutation(async (ctx) => {{
    await ctx.db.insert("notes", {{ body: "x" }});
    return null;
  }}),
  slow: mutation(async (ctx) => {{
    let x = 0;
    for (let i = 0; i < 1000000; i++) {{ x += i % 7; }}
    await ctx.db.insert("notes", {{ body: "slow", x }});
    return null;
  }}),
}};
"#
    )
    .into_bytes()
}

/// A node `node` serving app `main` on `store`, with `bundles` (shared by
/// the nodes, as a cluster shares its bucket) and `forward`'s settings.
async fn server(
    store: &TestStore,
    bundles: &loams_store::Store,
    node: &str,
    forward: ForwardConfig,
) -> LiveHandle {
    let mut config = store.live_config(APP);
    config.listen = "127.0.0.1:0".parse().expect("an address");
    config.node = node.to_string();
    config.forward = forward;
    let loader = loams_live_js::JsLoader::new(loams_live_js::JsConfig {
        contexts: 1,
        ..loams_live_js::JsConfig::default()
    })
    .expect("an in-process loader");
    let guard: loams_live::service::Guard =
        Arc::new(|router: axum::Router| router.layer(axum::middleware::from_fn(test_layer)));
    LiveServer::start_guarded(
        config,
        loams_live::deploy::Runtime {
            bundles: bundles.clone(),
            loader: Arc::new(loader),
        },
        Some(guard),
        CancellationToken::new(),
    )
    .await
    .expect("the server starts")
}

/// Two nodes, `a` and `b`, with version 1 deployed (through `a`).
async fn cluster(store: &TestStore) -> (LiveHandle, LiveHandle, String) {
    let bundles = loams_store::Store::in_memory();
    let a = server(store, &bundles, "a", ForwardConfig::default()).await;
    let v1 = deploy(&a, 1, None).await;
    let b = server(store, &bundles, "b", ForwardConfig::default()).await;
    (a, b, v1)
}

/// Deploys `version` through `node` over the active deployment, if any;
/// returns the new deployment's id.
async fn deploy(node: &LiveHandle, version: u32, expected: Option<String>) -> String {
    admin(node)
        .deploy(pb::DeployRequest {
            bundle: bundle(version),
            expected_active: expected,
            ..Default::default()
        })
        .await
        .expect("Deploy")
        .into_owned()
        .deployment
        .into_option()
        .expect("a deployment")
        .id
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

fn as_user(user: &str) -> CallOptions {
    CallOptions::default().with_header("authorization", format!("Bearer user:{user}"))
}

/// `allowed`, or the refusal's Connect code and `ErrorInfo.reason`.
fn outcome<T>(result: Result<T, ConnectError>) -> String {
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

fn spec(query_id: u32, function: &str) -> pb::QuerySpec {
    pb::QuerySpec {
        query_id,
        function: function.into(),
        args: MessageField::some(LiveValue::Object(Default::default()).to_proto()),
        ..Default::default()
    }
}

/// A Watch of `notes:version` (query 1) as `o`'s caller.
async fn watch(c: &Client, o: CallOptions) -> Result<Watch, ConnectError> {
    let set = pb::QuerySet {
        version: 1,
        queries: vec![spec(1, "notes:version")],
        ..Default::default()
    };
    c.watch_with_options(
        pb::WatchRequest {
            start: Some(pb::__buffa::oneof::watch_request::Start::Initial(Box::new(
                set,
            ))),
            ..Default::default()
        },
        o,
    )
    .await
}

/// The next message of `w`: a Transition, or the stream's end (`Ok(None)`)
/// or error.
async fn next(w: &mut Watch) -> Result<Option<pb::Transition>, ConnectError> {
    tokio::time::timeout(Duration::from_secs(20), w.message())
        .await
        .expect("a message within 20 s")
        .map(|m| m.map(|t| t.to_owned_message()))
}

/// Applies Transitions until `done` holds for the client's state; returns
/// the last Transition's session id.
async fn until(
    w: &mut Watch,
    client: &mut ClientState,
    done: impl Fn(&ClientState) -> bool,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "the state was not reached within 30 s"
        );
        let t = next(w).await.expect("the stream").expect("not the end");
        client.apply(&t).expect("in order");
        if !t.more && done(client) {
            return t.session_id;
        }
    }
}

fn holds(client: &ClientState, query: u32, value: &LiveValue) -> bool {
    matches!(client.results.get(&query), Some(QueryResult::Value(v)) if v == value)
}

async fn modify(
    c: &Client,
    session: &str,
    changes: Vec<pb::QuerySetChange>,
    version: u64,
    o: CallOptions,
) -> Result<(), ConnectError> {
    c.modify_query_set_with_options(
        pb::ModifyQuerySetRequest {
            session_id: session.into(),
            base_version: version - 1,
            new_version: version,
            changes,
            ..Default::default()
        },
        o,
    )
    .await
    .map(|_| ())
}

fn add(spec: pb::QuerySpec) -> pb::QuerySetChange {
    pb::QuerySetChange {
        change: Some(pb::__buffa::oneof::query_set_change::Change::Add(Box::new(
            spec,
        ))),
        ..Default::default()
    }
}

async fn authenticate(c: &Client, session: &str, o: CallOptions) -> Result<u64, ConnectError> {
    Ok(c.authenticate_with_options(
        pb::AuthenticateRequest {
            session_id: session.into(),
            ..Default::default()
        },
        o,
    )
    .await?
    .into_owned()
    .identity)
}

async fn mutate(c: &Client, function: &str) -> Result<(), ConnectError> {
    c.mutate(pb::MutateRequest {
        function: function.into(),
        args: MessageField::some(LiveValue::Object(Default::default()).to_proto()),
        ..Default::default()
    })
    .await
    .map(|_| ())
}

/// A session id of node `node`.
fn session_of(node: &str) -> String {
    format!("{node}-{}", "0".repeat(32))
}

// ---- the tests ----

/// A `ModifyQuerySet` sent to node B for a session node A holds is
/// forwarded to A: the query it adds arrives on A's stream.
async fn modify_on_other_node_is_forwarded(store: TestStore) {
    let (a, b, _) = cluster(&store).await;
    let mut w = watch(&client(&a), as_user("ada"))
        .await
        .expect("Watch on A");
    let mut state = ClientState::default();
    let session = until(&mut w, &mut state, |s| holds(s, 1, &LiveValue::I64(1))).await;
    assert!(session.starts_with("a-"), "{session}");

    modify(
        &client(&b),
        &session,
        vec![add(spec(2, "notes:count"))],
        2,
        as_user("ada"),
    )
    .await
    .expect("ModifyQuerySet through B");
    until(&mut w, &mut state, |s| s.results.contains_key(&2)).await;
    assert_eq!(state.version.query_set, 2);

    // The forwarded call ran as the client's caller on A, refused there
    // like any call it may not make (a version that does not follow).
    let e = modify(&client(&b), &session, Vec::new(), 7, as_user("ada")).await;
    assert!(e.is_err(), "A answered the forwarded call itself");
    b.stop().await;
    a.stop().await;
}
live_test!(modify_on_other_node_is_forwarded);

/// An `Authenticate` sent to node B for A's session is forwarded: another
/// caller bumps the identity version on A's stream; the same caller then
/// keeps it.
async fn authenticate_on_other_node_is_forwarded(store: TestStore) {
    let (a, b, _) = cluster(&store).await;
    let mut w = watch(&client(&a), as_user("ada"))
        .await
        .expect("Watch on A");
    let mut state = ClientState::default();
    let session = until(&mut w, &mut state, |s| holds(s, 1, &LiveValue::I64(1))).await;
    assert_eq!(state.version.identity, 0);

    let identity = authenticate(&client(&b), &session, as_user("bob"))
        .await
        .expect("Authenticate through B");
    assert_eq!(identity, 1);
    until(&mut w, &mut state, |s| s.version.identity == 1).await;

    let again = authenticate(&client(&b), &session, as_user("bob"))
        .await
        .expect("a refresh through B");
    assert_eq!(again, 1, "the same caller keeps the identity version");
    b.stop().await;
    a.stop().await;
}
live_test!(authenticate_on_other_node_is_forwarded);

/// An owner that is gone is `NOT_FOUND`, reason `session_gone`: a stopped
/// node (its record removed), a node that no longer answers (its record
/// still fresh), a record older than the TTL, an unknown node, a session
/// id naming no node, a session this node does not hold, and a call that
/// was already forwarded once.
async fn dead_owner_gives_session_gone(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let a = server(&store, &bundles, "a", ForwardConfig::default()).await;
    deploy(&a, 1, None).await;
    let b = server(
        &store,
        &bundles,
        "b",
        ForwardConfig {
            node_ttl: Duration::from_millis(500),
            ..ForwardConfig::default()
        },
    )
    .await;
    let cb = client(&b);
    let gone = "notfound session_gone";
    let modify_on = |c: Client, session: String| async move {
        outcome(modify(&c, &session, Vec::new(), 2, as_user("ada")).await)
    };

    let mut w = watch(&client(&a), as_user("ada"))
        .await
        .expect("Watch on A");
    let mut state = ClientState::default();
    let session = until(&mut w, &mut state, |s| holds(s, 1, &LiveValue::I64(1))).await;
    a.stop().await;
    assert_eq!(
        modify_on(cb.clone(), session.clone()).await,
        gone,
        "a stopped owner"
    );

    // A node that died with its record in place: nothing listens at its URL.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let url = format!("http://{}", closed.local_addr().expect("an address"));
    drop(closed);
    let system = b.engines().apps().store().clone();
    let c = Nodes::new(system.clone(), "c", url, ForwardConfig::default()).expect("node c");
    c.register().await.expect("c registers");
    assert_eq!(
        modify_on(cb.clone(), session_of("c")).await,
        gone,
        "a dead owner"
    );

    // A record past B's TTL, though its URL (B's own) answers.
    let d = Nodes::new(
        system,
        "d",
        format!("http://{}", b.addr),
        ForwardConfig::default(),
    )
    .expect("node d");
    d.register().await.expect("d registers");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        modify_on(cb.clone(), session_of("d")).await,
        gone,
        "a stale record"
    );

    assert_eq!(
        modify_on(cb.clone(), session_of("z")).await,
        gone,
        "an unknown node"
    );
    assert_eq!(
        modify_on(cb.clone(), "nobody".into()).await,
        gone,
        "no node named"
    );
    assert_eq!(
        modify_on(cb.clone(), session_of("b")).await,
        gone,
        "not open on B"
    );
    assert_eq!(
        outcome(authenticate(&cb, &session_of("c"), as_user("ada")).await),
        gone,
        "Authenticate too"
    );
    let forwarded = as_user("ada").with_header(FORWARDED_HEADER, "c");
    assert_eq!(
        outcome(modify(&cb, &session_of("c"), Vec::new(), 2, forwarded).await),
        gone,
        "a forwarded call is not forwarded again"
    );
    b.stop().await;
}
live_test!(dead_owner_gives_session_gone);

/// A node registering under an id whose record is still fresh at another
/// URL (a restart after a crash, on a new port) takes it over and is told
/// which URL it replaced (the server logs it); at the same URL it simply
/// takes its id back.
async fn a_node_id_in_use_is_taken_over(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let a = server(&store, &bundles, "a", ForwardConfig::default()).await;
    let system = a.engines().apps().store().clone();
    let same_url = Nodes::new(
        system.clone(),
        "a",
        a.advertised().to_string(),
        ForwardConfig::default(),
    )
    .expect("a node");
    assert_eq!(same_url.register().await.expect("registers"), None);
    let other = Nodes::new(
        system,
        "a",
        "http://127.0.0.1:1".into(),
        ForwardConfig::default(),
    )
    .expect("a node");
    assert_eq!(
        other.register().await.expect("registers"),
        Some(a.advertised().to_string()),
        "the run it replaced"
    );
    let record = other.lookup("a").await.expect("a lookup").expect("fresh");
    assert_eq!(record.url, "http://127.0.0.1:1");
    a.stop().await;
}
live_test!(a_node_id_in_use_is_taken_over);

/// A deploy on node A reaches node B's sessions through the journal, and
/// both nodes switch at one timestamp: every version-1 result on either
/// node is older than every version-2 result.
async fn deploy_on_one_node_reaches_the_other_at_the_same_tick(store: TestStore) {
    let (a, b, v1) = cluster(&store).await;
    let mut watches = Vec::new();
    for node in [&a, &b, &a, &b] {
        let mut w = watch(&client(node), as_user("ada")).await.expect("Watch");
        let mut state = ClientState::default();
        until(&mut w, &mut state, |s| holds(s, 1, &LiveValue::I64(1))).await;
        let seen = vec![(state.version.ts, 1)];
        watches.push((w, state, seen));
    }
    deploy(&a, 2, Some(v1)).await;
    let (mut last_v1, mut first_v2) = (0, u64::MAX);
    for (w, state, seen) in &mut watches {
        loop {
            let t = next(w).await.expect("the stream").expect("not the end");
            state.apply(&t).expect("in order");
            if t.more {
                continue;
            }
            let version = match state.results.get(&1) {
                Some(QueryResult::Value(LiveValue::I64(v))) => *v,
                other => panic!("notes:version: {other:?}"),
            };
            seen.push((state.version.ts, version));
            if version == 2 {
                break;
            }
        }
        for &(ts, version) in seen.iter() {
            if version == 1 {
                last_v1 = last_v1.max(ts);
            } else {
                first_v2 = first_v2.min(ts);
            }
        }
    }
    assert!(
        last_v1 < first_v2,
        "a version-1 result at ts {last_v1} is not older than a version-2 one at {first_v2}"
    );
    b.stop().await;
    a.stop().await;
}
live_test!(deploy_on_one_node_reaches_the_other_at_the_same_tick);

/// Draining node A: its readiness goes false, its stream closes with
/// `UNAVAILABLE`, reason `node_draining`, and so does a new call; B answers
/// A's sessions `session_gone` (A's record is gone); a mutation already
/// running on A finishes; B's own sessions go on.
async fn drain_closes_streams_with_node_draining(store: TestStore) {
    let (a, b, _) = cluster(&store).await;
    let (ca, cb) = (client(&a), client(&b));
    let mut wa = watch(&ca, as_user("ada")).await.expect("Watch on A");
    let mut sa = ClientState::default();
    let on_a = until(&mut wa, &mut sa, |s| holds(s, 1, &LiveValue::I64(1))).await;
    let mut wb = watch(&cb, as_user("ada")).await.expect("Watch on B");
    let mut sb = ClientState::default();
    let on_b = until(&mut wb, &mut sb, |s| holds(s, 1, &LiveValue::I64(1))).await;

    let running = {
        let ca = ca.clone();
        tokio::spawn(async move {
            let result = mutate(&ca, "notes:slow").await;
            (result, Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!a.routes().draining());
    a.drain().await;
    let drained = Instant::now();
    assert!(a.routes().draining(), "readiness goes false");

    let closed = loop {
        match next(&mut wa).await {
            Ok(Some(t)) => {
                sa.apply(&t).expect("in order");
            }
            Ok(None) => panic!("the stream ended without node_draining"),
            Err(e) => break e,
        }
    };
    assert_eq!(outcome::<()>(Err(closed)), "unavailable node_draining");
    let refused = match watch(&ca, as_user("ada")).await {
        Ok(mut w) => next(&mut w).await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert_eq!(outcome(refused), "unavailable node_draining");
    assert_eq!(
        outcome(mutate(&ca, "notes:add").await),
        "unavailable node_draining"
    );
    assert_eq!(
        outcome(modify(&cb, &on_a, Vec::new(), 2, as_user("ada")).await),
        "notfound session_gone",
        "A left the cluster"
    );

    let (result, finished) = running.await.expect("the mutation's task");
    result.expect("the mutation running when A drained finishes");
    assert!(
        finished <= drained + Duration::from_millis(100),
        "the drain waited for the running mutation"
    );

    modify(
        &cb,
        &on_b,
        vec![add(spec(2, "notes:count"))],
        2,
        as_user("ada"),
    )
    .await
    .expect("B serves on");
    until(&mut wb, &mut sb, |s| s.results.contains_key(&2)).await;
    a.stop().await;
    b.stop().await;
}
live_test!(drain_closes_streams_with_node_draining);
