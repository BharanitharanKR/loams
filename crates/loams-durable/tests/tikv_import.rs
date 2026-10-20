//! The 0.1.0 → 0.1.1 durable import (`tikv_import`): a SQLite store with
//! promises, tasks, callbacks, listeners and a schedule in every state the
//! SQLite backend keeps, imported into the blob backend's documents, reads
//! back the same through the blob server as through the SQLite one.
//!
//! The blob side runs on Resonate's in-memory object store, so this needs no
//! TiKV; `tests/tikv.rs` covers the TiKV store itself.
#![cfg(feature = "tikv")]

use std::net::TcpListener;
use std::sync::Arc;

use loams_durable::tikv::key_space;
use loams_durable::tikv_import::{
    ImportReport, MARKER, import_once_into, read_blob, read_sqlite, verify, write, write_sqlite,
};
use loams_durable::{DurableConfig, DurableServer, DurableStore, ResonateServer};
use resonate_core::types::{RequestEnvelope, SUPPORTED_VERSIONS};
use resonate_server_blob::sender::NullRouter;
use resonate_server_blob::server::{Server, ServerCfg};
use resonate_server_blob::store::{ObjectStoreAdapter, Store};
use serde_json::{Value, json};

const FAR: i64 = 4_000_000_000_000;
const WORKER: &str = "poll://any@workers";
const PID: &str = "pid-1";
const TTL: i64 = 600_000;

/// One request at `now`, on the SQLite server.
async fn sqlite_call(server: &DurableServer, kind: &str, data: Value, now: i64) -> Value {
    let reply = server
        .process(json!({ "kind": kind, "head": { "resonate:debug_time": now }, "data": data }))
        .await
        .expect("the SQLite server answers");
    let status = reply["head"]["status"].as_i64().unwrap_or_default();
    assert!(status < 300, "{kind} {data}: {reply}");
    reply
}

/// One request at `now`, on the blob server.
async fn blob_call(server: &Server, kind: &str, data: Value, now: i64) -> Value {
    let req = RequestEnvelope {
        kind: kind.into(),
        head: resonate_core::types::RequestHead {
            corr_id: "import".into(),
            version: SUPPORTED_VERSIONS[0].into(),
            auth: None,
            debug_time: Some(now),
        },
        data,
    };
    let reply = server.process(&req).await.expect("the blob server answers");
    serde_json::to_value(&reply).expect("a reply serializes")
}

/// Everything durable in a `debug.snap` except the held messages (each
/// backend holds its own sends, and nothing is sent on import).
fn durable_state(snap: &Value) -> Value {
    let mut data = snap["data"].clone();
    data.as_object_mut()
        .expect("a snapshot object")
        .remove("messages");
    data
}

/// A SQLite store with one of everything 0.1.0 can hold.
async fn seed(server: &DurableServer) {
    let create = |id: &str, tags: Value| {
        json!({ "id": id, "timeoutAt": FAR, "param": { "headers": { "h": "v" }, "data": "cGFyYW0=" },
                "tags": tags })
    };
    sqlite_call(
        server,
        "promise.create",
        create("plain", json!({ "k": "v" })),
        1_000,
    )
    .await;
    sqlite_call(server, "promise.create", create("done", json!({})), 1_010).await;
    sqlite_call(
        server,
        "promise.settle",
        json!({ "id": "done", "state": "resolved", "value": { "data": "ZG9uZQ==" } }),
        1_020,
    )
    .await;
    sqlite_call(server, "promise.create", create("gone", json!({})), 1_030).await;
    sqlite_call(
        server,
        "promise.settle",
        json!({ "id": "gone", "state": "rejected_canceled", "value": {} }),
        1_040,
    )
    .await;
    // An external promise with a timeout armed and a listener.
    sqlite_call(
        server,
        "promise.create",
        create("heard", json!({ "resonate:scope": "global" })),
        1_050,
    )
    .await;
    sqlite_call(
        server,
        "promise.register_listener",
        json!({ "awaited": "heard", "address": "poll://any@listener" }),
        1_060,
    )
    .await;
    // A workflow whose task this worker holds (acquired, leased), with an
    // unfired callback on one child and a fired one on another.
    sqlite_call(
        server,
        "task.create",
        json!({ "pid": PID, "ttl": TTL, "action": { "kind": "promise.create", "head": {},
                "data": create("wf", json!({ "resonate:target": WORKER })) } }),
        1_100,
    )
    .await;
    for child in ["wf:1", "wf:2"] {
        sqlite_call(
            server,
            "promise.create",
            create(child, json!({ "resonate:scope": "global" })),
            1_110,
        )
        .await;
        sqlite_call(
            server,
            "promise.register_callback",
            json!({ "awaited": child, "awaiter": "wf" }),
            1_120,
        )
        .await;
    }
    sqlite_call(
        server,
        "promise.settle",
        json!({ "id": "wf:2", "state": "resolved", "value": {} }),
        1_130,
    )
    .await;
    // A promise whose task waits to be dispatched (pending, retry armed).
    sqlite_call(
        server,
        "promise.create",
        create("queued", json!({ "resonate:target": WORKER })),
        1_200,
    )
    .await;
    sqlite_call(
        server,
        "schedule.create",
        json!({ "id": "nightly", "cron": "0 3 * * *", "promiseId": "nightly.{{.timestamp}}",
                "promiseTimeout": 60_000, "promiseParam": { "data": "eA==" },
                "promiseTags": { "resonate:target": WORKER } }),
        1_300,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sqlite_store_reads_back_the_same_from_the_blob_documents() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("durable").join("default.db");
    let mut config = DurableConfig::sqlite(&path);
    config.listen = TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("its address");
    config.debug = true;
    let server = DurableServer::start(config, "import-test")
        .await
        .expect("the SQLite server starts");
    seed(&server).await;
    let at = 2_000;
    let want = durable_state(&sqlite_call(&server, "debug.snap", json!({}), at).await);
    let want_schedule =
        sqlite_call(&server, "schedule.get", json!({ "id": "nightly" }), at).await["data"].clone();
    server.stop().await;

    let state = read_sqlite(&path).expect("the SQLite store reads");
    assert_eq!(
        state.report,
        ImportReport {
            origins: 6,
            promises: 8,
            tasks: 2,
            callbacks: 2,
            listeners: 1,
            schedules: 1,
            dropped_callbacks: 0,
        }
    );

    let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
    // Something already there (a store a dev build served from) is replaced.
    store
        .put(&key_space().doc_key("stale"), b"not a document".to_vec())
        .await
        .expect("a stale object");
    let report = write(store.as_ref(), &key_space(), &state)
        .await
        .expect("the import writes");
    assert_eq!(report, state.report);
    verify(store.as_ref(), &key_space(), &state)
        .await
        .expect("the import reads back");

    let blob = Server::build(
        Arc::clone(&store),
        Arc::new(NullRouter),
        ServerCfg {
            keys: key_space(),
            debug: true,
            search: true,
            ..Default::default()
        },
    );
    let got = durable_state(&blob_call(&blob, "debug.snap", json!({}), at).await);
    assert_eq!(got, want);
    let got_schedule =
        blob_call(&blob, "schedule.get", json!({ "id": "nightly" }), at).await["data"].clone();
    assert_eq!(got_schedule, want_schedule);

    // The imported workflow carries on: its worker settles the unfired
    // child, and the held task's version is the one 0.1.0 handed out.
    let settled = blob_call(
        &blob,
        "promise.settle",
        json!({ "id": "wf:1", "state": "resolved", "value": {} }),
        at + 10,
    )
    .await;
    assert_eq!(settled["head"]["status"], 200, "{settled}");
    let task = blob_call(&blob, "task.get", json!({ "id": "wf" }), at + 20).await;
    assert_eq!(task["data"]["task"]["state"], "acquired", "{task}");
    assert_eq!(task["data"]["task"]["version"], 1, "{task}");
    assert_eq!(task["data"]["task"]["resumes"], 2, "{task}");

    // And back (`rollback --keep-writes`): the SQLite server reads what the
    // blob server holds now, the settled child included.
    let now = blob_call(&blob, "debug.snap", json!({}), at + 30).await;
    let exported = read_blob(store.as_ref(), &key_space())
        .await
        .expect("the blob store reads");
    write_sqlite(&path, &exported).expect("the export writes");
    let mut config = DurableConfig::sqlite(&path);
    config.listen = TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("its address");
    config.debug = true;
    let server = DurableServer::start(config, "import-test")
        .await
        .expect("the SQLite server reopens its store");
    let back = sqlite_call(&server, "debug.snap", json!({}), at + 30).await;
    assert_eq!(durable_state(&back), durable_state(&now));
    let back_schedule = sqlite_call(&server, "schedule.get", json!({ "id": "nightly" }), at + 30)
        .await["data"]
        .clone();
    assert_eq!(back_schedule, want_schedule);
    server.stop().await;
}

/// SQLite refuses a callback across origins ("Awaiter and awaited must
/// belong to the same origin"), so 0.1.0 cannot have written one; a row
/// planted by hand is left out and counted rather than failing the import.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_callback_the_blob_backend_cannot_hold_is_counted_not_imported() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("default.db");
    let mut config = DurableConfig::sqlite(&path);
    config.listen = TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("its address");
    config.debug = true;
    let server = DurableServer::start(config, "import-test")
        .await
        .expect("the SQLite server starts");
    let create =
        |id: &str, tags: Value| json!({ "id": id, "timeoutAt": FAR, "param": {}, "tags": tags });
    sqlite_call(
        &server,
        "promise.create",
        create("awaiter", json!({ "resonate:target": WORKER })),
        1_000,
    )
    .await;
    sqlite_call(
        &server,
        "promise.create",
        create("other", json!({ "resonate:scope": "global" })),
        1_000,
    )
    .await;
    server.stop().await;
    rusqlite::Connection::open(&path)
        .expect("open the store")
        .execute(
            "INSERT INTO callbacks (awaited_id, awaiter_id, ready) VALUES ('other', 'awaiter', 0)",
            [],
        )
        .expect("plant a cross-origin callback");

    let state = read_sqlite(&path).expect("the SQLite store reads");
    assert_eq!(state.report.dropped_callbacks, 1);
    assert_eq!(state.report.callbacks, 0);
    assert!(
        state
            .origins
            .values()
            .all(|doc| doc.promises.values().all(|p| p.callbacks.is_empty()))
    );
}

#[test]
fn the_imported_store_is_the_one_the_server_opens() {
    // The import and the TiKV server must agree on the root and the layout.
    let DurableStore::Tikv { root, .. } =
        DurableStore::tikv(vec!["pd:2379".into()], "loams_durable").expect("a TiKV store")
    else {
        panic!("a TiKV store");
    };
    assert_eq!(root, b"resonate-durable/".to_vec());
    assert_eq!(key_space().doc_key("o"), "wf/o");
}

/// A SQLite server on `path`, in debug mode.
async fn sqlite_server(path: &std::path::Path) -> DurableServer {
    let mut config = DurableConfig::sqlite(path);
    config.listen = TcpListener::bind("127.0.0.1:0")
        .expect("a free port")
        .local_addr()
        .expect("its address");
    config.debug = true;
    DurableServer::start(config, "import-test")
        .await
        .expect("the SQLite server starts")
}

/// The engine imports on every start with a TiKV store: once per state of the
/// SQLite file. Unchanged, the second start leaves TiKV alone (its writes
/// since are kept); changed (0.1.0 served again after a rollback), it is
/// imported again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_import_runs_once_per_state_of_the_sqlite_file() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("default.db");
    let create = |id: &str| json!({ "id": id, "timeoutAt": FAR, "param": {}, "tags": {} });
    let server = sqlite_server(&path).await;
    sqlite_call(&server, "promise.create", create("first"), 1_000).await;
    server.stop().await;

    let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
    let keys = key_space();
    let first = import_once_into(store.as_ref(), &keys, &path)
        .await
        .expect("the first import")
        .expect("it imports");
    assert_eq!(first.promises, 1);
    let marker = store.get(MARKER).await.expect("read").expect("a marker");
    let marker: Value = serde_json::from_slice(&marker.0).expect("JSON");
    assert_eq!(marker["report"]["promises"], 1);

    // What the TiKV server wrote since an unchanged file's import stays.
    store
        .put(&keys.doc_key("served"), b"kept".to_vec())
        .await
        .expect("a write after the import");
    let again = import_once_into(store.as_ref(), &keys, &path)
        .await
        .expect("the second start");
    assert_eq!(again, None);
    assert!(
        store
            .get(&keys.doc_key("served"))
            .await
            .expect("read")
            .is_some()
    );

    let server = sqlite_server(&path).await;
    sqlite_call(&server, "promise.create", create("second"), 2_000).await;
    server.stop().await;
    let changed = import_once_into(store.as_ref(), &keys, &path)
        .await
        .expect("the import of a changed file")
        .expect("it imports");
    assert_eq!(changed.promises, 2);
    assert!(
        store
            .get(&keys.doc_key("served"))
            .await
            .expect("read")
            .is_none()
    );
}
