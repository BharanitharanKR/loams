//! LV1 plan Task 6 (design §45 §3.3, D683): versioned deployments. A
//! deploy validates its bundle in a scratch runtime, stores it in the
//! bundle store and checks its hash, writes a `pending` record, applies the
//! schema (only additive table creation here) and compares-and-swaps the
//! app's deployment pointer. LV1 plan Task 7: rollback is activating an
//! earlier deployment through the same path, and at the activation's tick
//! every subscription of the app reruns once on the new deployment. Every
//! case runs on the embedded store and, with `LOAMS_TEST_PD`, on TiKV;
//! bundles run in process (isolated workers are tested in `loams-live-js`'s
//! `worker` suite).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use buffa::{Message, MessageField};
use bytes::Bytes;
use connectrpc::ErrorCode;
use connectrpc::client::{ClientConfig, HttpClient};
use futures::future::BoxFuture;
use loams_kv::TxnOptions;
use loams_live::deploy::{
    self, AppEngine, BundleLoader, DeployRequest, DeploymentRecord, DeploymentState,
    KEEP_DEPLOYMENTS, LoadedBundle, PENDING_GRACE, PageRequest, Runtime,
};
use loams_live::schema::{Schema, TableSchema};
use loams_live::session::{ClientState, QueryResult, Session, SessionConfig, Sessions, Start};
use loams_live::subs::{SubsConfig, Subscriptions};
use loams_live::system::{INSERT, TABLES};
use loams_live::testing::TestStore;
use loams_live::{
    Function, IndexSpec, LiveError, LiveServer, LiveValue, Runner, SubKey, live_test, pb,
};
use loams_live_js::{JsConfig, JsLoader};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const APP: &str = "deploytest";

/// Version `n` of the app: `notes:version` answers `n`.
fn bundle(n: u32) -> Vec<u8> {
    format!(
        r#"
import {{ query, mutation }} from "loams:server";
export const notes = {{
  version: query(async () => {n}n),
  add: mutation(async (ctx, {{ body }}) => await ctx.db.insert("notes", {{ body }})),
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
}};
"#
    )
    .into_bytes()
}

fn notes_schema() -> Schema {
    Schema {
        tables: vec![TableSchema {
            name: "notes".into(),
            indexes: vec![IndexSpec {
                name: "by_body".into(),
                fields: vec!["body".into()],
            }],
            ..TableSchema::default()
        }],
    }
}

fn request(bundle: Vec<u8>, expected_active: Option<Ulid>) -> DeployRequest {
    DeployRequest {
        bundle,
        schema: notes_schema(),
        expected_active,
        message: "test".into(),
        dry_run: false,
    }
}

/// The in-process runtime with one slot per bundle (cheap to start).
fn js() -> Arc<dyn BundleLoader> {
    Arc::new(
        JsLoader::new(JsConfig {
            contexts: 1,
            ..JsConfig::default()
        })
        .expect("an in-process loader"),
    )
}

/// The app's engine on `store`, as one node: its runner, the bundle store
/// `bundles` and `loader`.
async fn node(
    store: &TestStore,
    bundles: &loams_store::Store,
    loader: Arc<dyn BundleLoader>,
) -> AppEngine {
    let runner = Runner::open(store.store(), &store.live_config(APP))
        .await
        .expect("the runner opens");
    AppEngine::open(
        APP,
        runner,
        Runtime {
            bundles: bundles.clone(),
            loader,
        },
    )
    .await
    .expect("the engine opens")
}

fn args(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

/// Runs query `name` of `app` with no arguments, at now.
async fn query(app: &AppEngine, name: &str) -> Result<LiveValue, LiveError> {
    let f = deploy::resolve(app, name)?;
    let at = app.runner().store().now().await.expect("now");
    Ok(app.runner().query(&*f, args(&[]), at).await?.result)
}

async fn version(app: &AppEngine) -> LiveValue {
    query(app, "notes:version").await.expect("notes:version")
}

async fn records(app: &AppEngine) -> Vec<DeploymentRecord> {
    let mut all = Vec::new();
    let mut page = PageRequest::default();
    loop {
        let got = deploy::list_deployments(app, page.clone())
            .await
            .expect("the deployments list");
        all.extend(got.items);
        match got.next {
            Some(after) => page.after = Some(after),
            None => return all,
        }
    }
}

fn record(records: &[DeploymentRecord], id: Ulid) -> &DeploymentRecord {
    records
        .iter()
        .find(|r| r.id == id)
        .unwrap_or_else(|| panic!("no record of {id}"))
}

async fn objects(bundles: &loams_store::Store) -> Vec<String> {
    let mut paths: Vec<String> = bundles
        .list(&format!("live/{APP}/deployments/"))
        .await
        .expect("the bundle store lists")
        .into_iter()
        .map(|o| o.path)
        .collect();
    paths.sort();
    paths
}

fn conflict(result: Result<impl std::fmt::Debug, LiveError>, what: &str) -> LiveError {
    match result {
        Err(e) if e.reason() == Some("live_deploy_conflict") => {
            assert_eq!(
                e.code(),
                pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION,
                "{what}"
            );
            e
        }
        other => panic!("{what}: live_deploy_conflict, not {other:?}"),
    }
}

/// R1's name: a deploy swaps the functions new calls resolve; a function
/// taken before keeps its deployment; records, parents and states follow.
async fn deploy_swaps_functions_for_new_calls(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    assert!(matches!(
        deploy::resolve(&app, "notes:version"),
        Err(LiveError::NotFound(_))
    ));
    assert_eq!(app.active(), None);

    let v1 = deploy::deploy(&app, request(bundle(1), None), "alice")
        .await
        .expect("the first deploy");
    assert!(!v1.dry_run);
    assert_eq!(v1.record.state, DeploymentState::Active);
    assert_eq!(v1.record.parent, None);
    assert_eq!(v1.record.created_by, "alice");
    assert_eq!(v1.changes.create, ["notes"]);
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(version(&app).await, LiveValue::I64(1));
    let old = deploy::resolve(&app, "notes:version").expect("v1's function");
    assert_eq!(old.deployment(), v1.record.id);

    let v2 = deploy::deploy(&app, request(bundle(2), Some(v1.record.id)), "bob")
        .await
        .expect("the second deploy");
    assert_eq!(v2.record.parent, Some(v1.record.id));
    assert!(v2.changes.create.is_empty(), "notes exists already");
    assert_eq!(version(&app).await, LiveValue::I64(2));
    let new = deploy::resolve(&app, "notes:version").expect("v2's function");
    assert_eq!(new.deployment(), v2.record.id);
    // Subscriptions of the two deployments never share a result.
    assert_ne!(
        SubKey::for_function(&*old, &args(&[])),
        SubKey::for_function(&*new, &args(&[]))
    );
    // A call already holding v1's function still runs v1.
    let at = app.runner().store().now().await.expect("now");
    let ran = app
        .runner()
        .query(&*old, args(&[]), at)
        .await
        .expect("v1 still runs");
    assert_eq!(ran.result, LiveValue::I64(1));

    // A mutation of the active deployment writes through `ctx.db`.
    let add = deploy::resolve(&app, "notes:add").expect("notes:add");
    app.runner()
        .mutate(add, args(&[("body", LiveValue::Str("hi".into()))]), None)
        .await
        .expect("notes:add runs");
    assert_eq!(
        query(&app, "notes:count").await.expect("count"),
        LiveValue::F64(1.0)
    );

    let all = records(&app).await;
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, v2.record.id, "newest first");
    assert_eq!(
        record(&all, v1.record.id).state,
        DeploymentState::Superseded
    );
    assert_eq!(record(&all, v2.record.id).state, DeploymentState::Active);
    assert_eq!(record(&all, v2.record.id).message, "test");
    assert_eq!(
        record(&all, v2.record.id).bundle_sha256,
        <sha2::Sha256 as sha2::Digest>::digest(bundle(2)).as_slice()
    );
    assert_eq!(record(&all, v2.record.id).schema, notes_schema());
    assert_eq!(
        objects(&bundles).await,
        [v1.record.id, v2.record.id].map(|id| deploy::bundle_path(APP, id))
    );

    // A second node opening the app serves the active deployment.
    let other = node(&store, &bundles, js()).await;
    assert_eq!(other.active(), Some(v2.record.id));
    assert_eq!(version(&other).await, LiveValue::I64(2));
}
live_test!(deploy_swaps_functions_for_new_calls);

/// Two nodes deploy against the same active deployment at once: exactly one
/// wins; the other is `FAILED_PRECONDITION`, reason `live_deploy_conflict`,
/// its record is `failed`, and its node then serves the winner.
async fn concurrent_deploys_one_wins(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let a = node(&store, &bundles, js()).await;
    let b = node(&store, &bundles, js()).await;
    let (ra, rb) = tokio::join!(
        deploy::deploy(&a, request(bundle(1), None), "a"),
        deploy::deploy(&b, request(bundle(2), None), "b"),
    );
    let (winner, loser_node, loser) = match (ra, rb) {
        (Ok(w), l) => (w, &b, conflict(l, "b loses")),
        (l, Ok(w)) => (w, &a, conflict(l, "a loses")),
        (ra, rb) => panic!("one deploy wins: {ra:?} / {rb:?}"),
    };
    assert!(
        loser.to_string().contains(&winner.record.id.to_string()),
        "{loser}"
    );
    let all = records(&a).await;
    assert_eq!(all.len(), 2, "{all:?}");
    let failed = all
        .iter()
        .find(|r| r.id != winner.record.id)
        .expect("the loser's record");
    assert!(
        matches!(&failed.state, DeploymentState::Failed { reason } if reason.contains("conflict")),
        "{failed:?}"
    );
    assert_eq!(
        record(&all, winner.record.id).state,
        DeploymentState::Active
    );
    let expected = if winner.record.created_by == "a" {
        1
    } else {
        2
    };
    for app in [&a, &b] {
        assert_eq!(app.active(), Some(winner.record.id));
        assert_eq!(version(app).await, LiveValue::I64(expected));
    }
    // A deploy against the stale pointer conflicts too.
    conflict(
        deploy::deploy(loser_node, request(bundle(3), None), "late").await,
        "a stale expected_active",
    );
}
live_test!(concurrent_deploys_one_wins);

/// A dry run validates the bundle and the schema and reports the changes,
/// and writes nothing: no record, no pointer, no object, no table.
async fn dry_run_writes_nothing(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let dry = deploy::deploy(
        &app,
        DeployRequest {
            dry_run: true,
            ..request(bundle(1), None)
        },
        "alice",
    )
    .await
    .expect("a dry run");
    assert!(dry.dry_run);
    assert_eq!(dry.changes.create, ["notes"]);
    assert_eq!(dry.record.state, DeploymentState::Pending);
    assert!(records(&app).await.is_empty());
    assert!(objects(&bundles).await.is_empty());
    assert_eq!(app.active(), None);
    assert!(matches!(
        deploy::resolve(&app, "notes:version"),
        Err(LiveError::NotFound(_))
    ));
    assert_eq!(
        query(&app, TABLES).await.expect("_system:tables"),
        LiveValue::Array(Vec::new())
    );
    // A dry run reports what the real deploy would hit.
    let bad = deploy::deploy(
        &app,
        DeployRequest {
            dry_run: true,
            ..request(b"export const = ;".to_vec(), None)
        },
        "alice",
    )
    .await
    .expect_err("an invalid bundle");
    assert_eq!(
        bad.code(),
        pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT,
        "{bad}"
    );
    conflict(
        deploy::deploy(
            &app,
            DeployRequest {
                dry_run: true,
                ..request(bundle(1), Some(Ulid::generate()))
            },
            "alice",
        )
        .await,
        "a dry run against a deployment that is not active",
    );
    assert!(records(&app).await.is_empty());
    assert!(objects(&bundles).await.is_empty());
}
live_test!(dry_run_writes_nothing);

/// A node that loads a deployment whose stored bundle does not match its
/// record's SHA-256 refuses it and keeps serving the deployment it had; a
/// node with nothing loaded serves the system functions only.
async fn bundle_hash_mismatch_refused_and_previous_keeps_serving(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let a = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&a, request(bundle(1), None), "a")
        .await
        .expect("v1");
    // Another node deploys v2; then its stored object is corrupted.
    let b = node(&store, &bundles, js()).await;
    let v2 = deploy::deploy(&b, request(bundle(2), Some(v1.record.id)), "b")
        .await
        .expect("v2");
    bundles
        .put(
            &deploy::bundle_path(APP, v2.record.id),
            Bytes::from(bundle(666)),
        )
        .await
        .expect("the object is overwritten");

    let err = a
        .refresh()
        .await
        .expect_err("the corrupt bundle is refused");
    assert!(matches!(err, LiveError::Corrupt(_)), "{err}");
    assert!(err.to_string().contains("SHA-256"), "{err}");
    assert_eq!(a.active(), Some(v1.record.id), "a keeps v1");
    assert_eq!(version(&a).await, LiveValue::I64(1));

    // A node starting now has no previous deployment: its deployed
    // functions fail with the load error, the system functions serve.
    let c = node(&store, &bundles, js()).await;
    assert_eq!(c.active(), None);
    let err = query(&c, "notes:version")
        .await
        .expect_err("nothing loaded");
    assert!(matches!(err, LiveError::Corrupt(_)), "{err}");
    query(&c, TABLES).await.expect("_system:tables serves");

    // Restoring the object lets the nodes load v2.
    bundles
        .put(
            &deploy::bundle_path(APP, v2.record.id),
            Bytes::from(bundle(2)),
        )
        .await
        .expect("restored");
    assert_eq!(a.refresh().await.expect("a loads v2"), Some(v2.record.id));
    assert_eq!(version(&a).await, LiveValue::I64(2));
    assert_eq!(c.refresh().await.expect("c loads v2"), Some(v2.record.id));
    assert_eq!(version(&c).await, LiveValue::I64(2));
}
live_test!(bundle_hash_mismatch_refused_and_previous_keeps_serving);

/// A bundle that fails validation, or a schema that cannot apply, leaves
/// the pointer, the records and the bundle store as they were.
async fn invalid_bundle_fails_without_pointer_change(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&app, request(bundle(1), None), "a")
        .await
        .expect("v1");
    let bad_bundles: [&[u8]; 4] = [
        b"export const = ;",
        b"throw new Error('at load');",
        b"import { query } from \"loams:server\"; export const _system = { insert: query(async () => 1) };",
        &[0xff, 0xfe, 0x00],
    ];
    for bad in bad_bundles {
        let err = deploy::deploy(&app, request(bad.to_vec(), Some(v1.record.id)), "a")
            .await
            .expect_err("an invalid bundle");
        assert!(
            matches!(
                err,
                LiveError::InvalidArgument(_) | LiveError::FunctionError(_)
            ),
            "{err}"
        );
    }
    let bad_schemas = [
        Schema {
            tables: vec![TableSchema {
                name: "1bad".into(),
                indexes: Vec::new(),
                ..TableSchema::default()
            }],
        },
        Schema {
            tables: vec![
                TableSchema {
                    name: "twice".into(),
                    indexes: Vec::new(),
                    ..TableSchema::default()
                },
                TableSchema {
                    name: "twice".into(),
                    indexes: Vec::new(),
                    ..TableSchema::default()
                },
            ],
        },
    ];
    for schema in bad_schemas {
        let err = deploy::deploy(
            &app,
            DeployRequest {
                schema: schema.clone(),
                ..request(bundle(2), Some(v1.record.id))
            },
            "a",
        )
        .await
        .expect_err("an invalid schema");
        assert!(
            matches!(err, LiveError::InvalidArgument(_)),
            "{schema:?}: {err}"
        );
    }
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(version(&app).await, LiveValue::I64(1));
    let all = records(&app).await;
    assert_eq!(all.len(), 1, "{all:?}");
    assert_eq!(all[0].state, DeploymentState::Active);
    assert_eq!(objects(&bundles).await.len(), 1);

    // A schema the catalog refuses (an index change on an existing table
    // waits for online index changes, LV1 plan Task 9) fails at step 4:
    // its record says why, and the pointer stays.
    let err = deploy::deploy(
        &app,
        DeployRequest {
            schema: Schema {
                tables: vec![TableSchema {
                    name: "notes".into(),
                    indexes: Vec::new(),
                    ..TableSchema::default()
                }],
            },
            ..request(bundle(2), Some(v1.record.id))
        },
        "a",
    )
    .await
    .expect_err("an index change");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
    assert!(err.to_string().contains("notes"), "{err}");
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(version(&app).await, LiveValue::I64(1));
    let all = records(&app).await;
    assert_eq!(all.len(), 2, "{all:?}");
    assert!(
        matches!(&all[0].state, DeploymentState::Failed { reason } if reason.contains("notes")),
        "{all:?}"
    );
    assert_eq!(record(&all, v1.record.id).state, DeploymentState::Active);
    // The next valid deploy goes through.
    deploy::deploy(&app, request(bundle(2), Some(v1.record.id)), "a")
        .await
        .expect("v2");
    assert_eq!(version(&app).await, LiveValue::I64(2));
}
live_test!(invalid_bundle_fails_without_pointer_change);

/// The janitor keeps the newest 50 deployments plus the active one and its
/// parent, and deletes the others' records and bundles, and bundles no
/// record names once they are an hour old.
async fn retention_keeps_fifty_and_the_active_parent(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let q0 = deploy::deploy(&app, request(bundle(0), None), "a")
        .await
        .expect("q0");
    let p = deploy::deploy(&app, request(bundle(1), Some(q0.record.id)), "a")
        .await
        .expect("p");
    let x = deploy::deploy(&app, request(bundle(2), Some(p.record.id)), "a")
        .await
        .expect("x");
    // 55 deploys against a stale pointer: each leaves a failed record and a
    // bundle, newer than p and x.
    let mut failed = Vec::new();
    for n in 0..KEEP_DEPLOYMENTS + 5 {
        let err = deploy::deploy(
            &app,
            request(bundle(100 + n as u32), Some(p.record.id)),
            "a",
        )
        .await;
        conflict(err, "a stale deploy");
    }
    for r in records(&app).await {
        if matches!(r.state, DeploymentState::Failed { .. }) {
            failed.push(r.id);
        }
    }
    failed.sort();
    assert_eq!(failed.len(), KEEP_DEPLOYMENTS + 5);
    // Orphans: an object no record names, an hour old, and a fresh one (a
    // deploy that stored its bundle and has not written its record yet).
    let hour_ago = std::time::SystemTime::now() - Duration::from_secs(3600 + 60);
    let old_orphan = Ulid::from_datetime(hour_ago);
    let new_orphan = Ulid::generate();
    for orphan in [old_orphan, new_orphan] {
        bundles
            .put(&deploy::bundle_path(APP, orphan), Bytes::from(bundle(9)))
            .await
            .expect("an orphan");
    }

    let report = deploy::retain(&app).await.expect("the janitor runs");
    let mut deleted = vec![q0.record.id];
    deleted.extend_from_slice(&failed[..5]);
    deleted.sort();
    let mut reported = report.records_deleted.clone();
    reported.sort();
    assert_eq!(reported, deleted);
    assert_eq!(
        report.bundles_deleted,
        deleted.len() + 1,
        "and the old orphan"
    );

    let left = records(&app).await;
    assert_eq!(left.len(), KEEP_DEPLOYMENTS + 2);
    let mut kept: Vec<Ulid> = failed[5..].to_vec();
    kept.push(p.record.id);
    kept.push(x.record.id);
    let mut expected: Vec<String> = kept
        .iter()
        .map(|id| deploy::bundle_path(APP, *id))
        .collect();
    expected.push(deploy::bundle_path(APP, new_orphan));
    expected.sort();
    assert_eq!(objects(&bundles).await, expected);
    assert_eq!(record(&left, x.record.id).state, DeploymentState::Active);
    assert_eq!(record(&left, x.record.id).parent, Some(p.record.id));
    assert_eq!(version(&app).await, LiveValue::I64(2));

    // A second run finds nothing more to delete.
    let again = deploy::retain(&app).await.expect("again");
    assert!(again.records_deleted.is_empty());
    assert_eq!(again.bundles_deleted, 0);

    // Pages: newest first, `after` continues, and no page is longer than
    // its limit.
    let first = deploy::list_deployments(
        &app,
        PageRequest {
            after: None,
            limit: 10,
        },
    )
    .await
    .expect("a page");
    assert_eq!(first.items.len(), 10);
    assert!(first.items.windows(2).all(|w| w[0].id > w[1].id));
    let next = first.next.expect("more pages");
    let second = deploy::list_deployments(
        &app,
        PageRequest {
            after: Some(next),
            limit: 10,
        },
    )
    .await
    .expect("a second page");
    assert!(second.items[0].id < first.items[9].id);
}
live_test!(retention_keeps_fifty_and_the_active_parent);

/// The `_system:*` functions resolve before any deployment: no bundle can
/// shadow them, and they serve with no deployment, through any deploy, and
/// on a server without a function runtime.
async fn system_functions_survive_any_deploy(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let insert = |app: &AppEngine| {
        let f = deploy::resolve(app, INSERT).expect("_system:insert");
        assert_eq!(f.deployment(), Ulid::nil());
        f
    };
    let note = args(&[
        ("table", LiveValue::Str("notes".into())),
        ("fields", args(&[("body", LiveValue::Str("x".into()))])),
    ]);
    app.runner()
        .mutate(insert(&app), note.clone(), None)
        .await
        .expect("_system:insert before any deploy");
    let v1 = deploy::deploy(
        &app,
        DeployRequest {
            // `notes` exists with no user index: keep it that way.
            schema: Schema {
                tables: vec![TableSchema {
                    name: "notes".into(),
                    indexes: Vec::new(),
                    ..TableSchema::default()
                }],
            },
            ..request(bundle(1), None)
        },
        "a",
    )
    .await
    .expect("v1");
    app.runner()
        .mutate(insert(&app), note.clone(), None)
        .await
        .expect("_system:insert after a deploy");
    let shadow = b"import { query } from \"loams:server\"; export const _system = { tables: query(async () => 1) };";
    deploy::deploy(&app, request(shadow.to_vec(), Some(v1.record.id)), "a")
        .await
        .expect_err("a bundle cannot define _system functions");
    let tables = query(&app, TABLES).await.expect("_system:tables");
    assert!(
        matches!(&tables, LiveValue::Array(t) if t.len() == 1),
        "{tables:?}"
    );
    assert_eq!(
        query(&app, "notes:count").await.expect("count"),
        LiveValue::F64(2.0)
    );

    // A node without a function runtime serves the system functions and
    // refuses deploys.
    let bare = node(
        &store,
        &loams_store::Store::in_memory(),
        Runtime::none().loader,
    )
    .await;
    query(&bare, TABLES).await.expect("_system:tables");
    let err = deploy::deploy(&bare, request(bundle(1), Some(v1.record.id)), "a")
        .await
        .expect_err("no runtime");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
}
live_test!(system_functions_survive_any_deploy);

/// Without a schema, tables are created on insert; a schema creates its
/// missing tables with their indexes, never changes an existing table, and
/// makes inserts into undeclared tables fail.
async fn schema_creates_tables_additively(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(
        &app,
        DeployRequest {
            schema: Schema::default(),
            ..request(bundle(1), None)
        },
        "a",
    )
    .await
    .expect("a deploy without a schema");
    assert!(v1.changes.create.is_empty());
    let add = deploy::resolve(&app, "notes:add").expect("notes:add");
    app.runner()
        .mutate(add, args(&[("body", LiveValue::Str("hi".into()))]), None)
        .await
        .expect("notes is created on insert");

    let schema = Schema {
        tables: vec![
            TableSchema {
                name: "notes".into(),
                indexes: Vec::new(),
                ..TableSchema::default()
            },
            TableSchema {
                name: "tags".into(),
                indexes: vec![IndexSpec {
                    name: "by_name".into(),
                    fields: vec!["name".into()],
                }],
                ..TableSchema::default()
            },
        ],
    };
    let v2 = deploy::deploy(
        &app,
        DeployRequest {
            schema: schema.clone(),
            ..request(bundle(2), Some(v1.record.id))
        },
        "a",
    )
    .await
    .expect("a schema adding tags");
    assert_eq!(v2.changes.create, ["tags"]);
    let tables = query(&app, TABLES).await.expect("_system:tables");
    let LiveValue::Array(tables) = tables else {
        panic!("{tables:?}");
    };
    assert_eq!(tables.len(), 2);
    assert!(
        format!("{tables:?}").contains("by_name"),
        "tags has its index: {tables:?}"
    );
    let insert = deploy::resolve(&app, INSERT).expect("_system:insert");
    let err = app
        .runner()
        .mutate(
            insert,
            args(&[
                ("table", LiveValue::Str("other".into())),
                ("fields", args(&[])),
            ]),
            None,
        )
        .await
        .expect_err("other is not in the schema");
    assert!(matches!(err, LiveError::NotFound(_)), "{err}");
}
live_test!(schema_creates_tables_additively);

/// A bundle whose degraded signal fires (its isolated workers crashed five
/// times within a minute) marks its record `degraded`; it still serves, and
/// a later deploy supersedes it.
async fn degraded_workers_mark_the_deployment_degraded(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let fake = Arc::new(Flagged::default());
    let app = node(&store, &bundles, fake.clone()).await;
    let v1 = deploy::deploy(&app, request(bundle(1), None), "a")
        .await
        .expect("v1");
    assert_eq!(app.active(), Some(v1.record.id));
    fake.degrade(0);
    let state = wait_for_state(&app, v1.record.id, |s| *s == DeploymentState::Degraded).await;
    assert_eq!(state, DeploymentState::Degraded);
    assert_eq!(
        query(&app, "notes:version").await.expect("served"),
        LiveValue::I64(7)
    );
    let v2 = deploy::deploy(&app, request(bundle(2), Some(v1.record.id)), "a")
        .await
        .expect("v2");
    let all = records(&app).await;
    assert_eq!(
        record(&all, v1.record.id).state,
        DeploymentState::Superseded
    );
    assert_eq!(record(&all, v2.record.id).state, DeploymentState::Active);
    // v1's signal no longer touches the records.
    fake.degrade(0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        record(&records(&app).await, v1.record.id).state,
        DeploymentState::Superseded
    );
}
live_test!(degraded_workers_mark_the_deployment_degraded);

async fn wait_for_state(
    app: &AppEngine,
    id: Ulid,
    done: impl Fn(&DeploymentState) -> bool,
) -> DeploymentState {
    for _ in 0..100 {
        let all = records(app).await;
        let state = record(&all, id).state.clone();
        if done(&state) {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{id} never reached the state");
}

/// A loader whose bundles answer `notes:version` with 7 and report the
/// degraded signal the test fires (the `n`th bundle loaded).
#[derive(Default)]
struct Flagged {
    loaded: Mutex<Vec<watch::Sender<bool>>>,
}

impl Flagged {
    fn degrade(&self, n: usize) {
        let loaded = self.loaded.lock().expect("unpoisoned");
        loaded[n].send_replace(true);
    }
}

impl BundleLoader for Flagged {
    fn load<'a>(
        &'a self,
        _app: &'a str,
        _bundle: &'a [u8],
    ) -> BoxFuture<'a, Result<Arc<dyn LoadedBundle>, LiveError>> {
        Box::pin(async move {
            let (tx, rx) = watch::channel(false);
            self.loaded.lock().expect("unpoisoned").push(tx);
            Ok(Arc::new(Seven(rx)) as Arc<dyn LoadedBundle>)
        })
    }
}

struct Seven(watch::Receiver<bool>);

impl LoadedBundle for Seven {
    fn function(&self, path: &str) -> Option<Arc<dyn Function>> {
        (path == "notes:version").then(|| Arc::new(SevenFn) as Arc<dyn Function>)
    }

    fn degraded(&self) -> Option<watch::Receiver<bool>> {
        Some(self.0.clone())
    }
}

struct SevenFn;

impl Function for SevenFn {
    fn name(&self) -> &str {
        "notes:version"
    }

    fn kind(&self) -> loams_live::FnKind {
        loams_live::FnKind::Query
    }

    fn call<'a>(
        &'a self,
        _txn: &'a mut loams_live::LiveTxn<'_>,
        _args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async { Ok(LiveValue::I64(7)) })
    }
}

/// The server deploys over `LiveService.Deploy` and runs the deployment's
/// functions for `Mutate` and `Query`; a server without a function runtime
/// refuses the deploy.
async fn live_service_deploys_and_serves_functions(store: TestStore) {
    let mut config = store.live_config(APP);
    config.listen = "127.0.0.1:0".parse().expect("an address");
    let handle = LiveServer::start_with(
        config.clone(),
        Runtime {
            bundles: loams_store::Store::in_memory(),
            loader: js(),
        },
        CancellationToken::new(),
    )
    .await
    .expect("the server starts");
    let uri = format!("http://{}", handle.addr).parse().expect("a uri");
    let c = pb::LiveServiceClient::new(HttpClient::plaintext(), ClientConfig::new(uri));
    let deployed = c
        .deploy(pb::DeployRequest {
            bundle: bundle(1),
            schema: MessageField::some(notes_schema().to_proto()),
            ..Default::default()
        })
        .await
        .expect("Deploy")
        .into_owned();
    let id: Ulid = deployed.deployment_id.parse().expect("a ULID");
    let mutated = c
        .mutate(pb::MutateRequest {
            function: "notes:add".into(),
            args: MessageField::some(args(&[("body", LiveValue::Str("hi".into()))]).to_proto()),
            ..Default::default()
        })
        .await
        .expect("Mutate runs the deployed mutation")
        .into_owned();
    let counted = c
        .query(pb::QueryRequest {
            function: "notes:count".into(),
            args: MessageField::some(args(&[]).to_proto()),
            ts: Some(mutated.commit_ts),
            ..Default::default()
        })
        .await
        .expect("Query runs the deployed query")
        .into_owned();
    assert_eq!(
        LiveValue::from_proto(counted.result.into_option().expect("a result")).expect("a value"),
        LiveValue::F64(1.0)
    );
    // A second deploy replaces the first (the service deploys against the
    // active deployment).
    let again = c
        .deploy(pb::DeployRequest {
            bundle: bundle(2),
            schema: MessageField::some(notes_schema().to_proto()),
            ..Default::default()
        })
        .await
        .expect("a second Deploy")
        .into_owned();
    assert_ne!(again.deployment_id, id.to_string());
    handle.stop().await;

    let bare = LiveServer::start(config, CancellationToken::new())
        .await
        .expect("a server without a runtime");
    let uri = format!("http://{}", bare.addr).parse().expect("a uri");
    let c = pb::LiveServiceClient::new(HttpClient::plaintext(), ClientConfig::new(uri));
    let e = c
        .deploy(pb::DeployRequest {
            bundle: bundle(3),
            ..Default::default()
        })
        .await
        .expect_err("no function runtime");
    assert_eq!(e.code, ErrorCode::FailedPrecondition, "{e:?}");
    bare.stop().await;
}
live_test!(live_service_deploys_and_serves_functions);

// ---- LV1 plan Task 7: rollback and activation semantics ----

/// Rollback activates an earlier deployment through the deploy path: its
/// stored bundle is loaded (hash checked) and its schema re-applied, and
/// the pointer is compared-and-swapped against `expected_active`. The
/// records keep their contents; only their states move.
async fn rollback_restores_previous_functions(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&app, request(bundle(1), None), "alice")
        .await
        .expect("v1");
    let v2 = deploy::deploy(&app, request(bundle(2), Some(v1.record.id)), "bob")
        .await
        .expect("v2");
    assert_eq!(version(&app).await, LiveValue::I64(2));

    let back = deploy::activate(&app, v1.record.id, Some(v2.record.id), "carol")
        .await
        .expect("the rollback");
    assert!(!back.dry_run);
    assert_eq!(back.record.id, v1.record.id);
    assert_eq!(back.record.state, DeploymentState::Active);
    assert_eq!(back.record.created_by, "alice", "the record is v1's own");
    assert_eq!(back.record.parent, None);
    assert!(back.changes.create.is_empty(), "notes exists already");
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(
        app.read_active().await.expect("the pointer"),
        Some(v1.record.id)
    );
    assert_eq!(version(&app).await, LiveValue::I64(1));
    let all = records(&app).await;
    assert_eq!(all.len(), 2, "a rollback writes no new record");
    assert_eq!(record(&all, v1.record.id).state, DeploymentState::Active);
    assert_eq!(
        record(&all, v2.record.id).state,
        DeploymentState::Superseded
    );
    // Another node opening the app serves the rolled-back deployment.
    let other = node(&store, &bundles, js()).await;
    assert_eq!(version(&other).await, LiveValue::I64(1));

    // A stale `expected_active` conflicts and changes nothing.
    conflict(
        deploy::activate(&app, v2.record.id, Some(v2.record.id), "carol").await,
        "an activation against a stale pointer",
    );
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(
        record(&records(&app).await, v2.record.id).state,
        DeploymentState::Superseded
    );
    // An unknown deployment is not found.
    let err = deploy::activate(&app, Ulid::generate(), Some(v1.record.id), "carol")
        .await
        .expect_err("no such deployment");
    assert!(matches!(err, LiveError::NotFound(_)), "{err}");

    // Roll forward again, then activate the active deployment again (a
    // no-op for the pointer).
    deploy::activate(&app, v2.record.id, Some(v1.record.id), "carol")
        .await
        .expect("roll forward");
    assert_eq!(version(&app).await, LiveValue::I64(2));
    let again = deploy::activate(&app, v2.record.id, Some(v2.record.id), "carol")
        .await
        .expect("the active deployment again");
    assert_eq!(again.record.state, DeploymentState::Active);
    assert_eq!(version(&app).await, LiveValue::I64(2));
    let all = records(&app).await;
    assert_eq!(
        record(&all, v1.record.id).state,
        DeploymentState::Superseded
    );
    assert_eq!(record(&all, v2.record.id).state, DeploymentState::Active);

    // A deployment whose stored bundle no longer matches its record cannot
    // be activated: the pointer stays.
    bundles
        .put(
            &deploy::bundle_path(APP, v1.record.id),
            Bytes::from(bundle(666)),
        )
        .await
        .expect("the object is overwritten");
    let err = deploy::activate(&app, v1.record.id, Some(v2.record.id), "carol")
        .await
        .expect_err("a corrupt bundle");
    assert!(matches!(err, LiveError::Corrupt(_)), "{err}");
    assert_eq!(app.active(), Some(v2.record.id));
    assert_eq!(
        app.read_active().await.expect("the pointer"),
        Some(v2.record.id)
    );
    assert_eq!(version(&app).await, LiveValue::I64(2));
}
live_test!(rollback_restores_previous_functions);

/// Rollback re-applies the earlier deployment's schema: an index the later
/// deployment added is dropped.
async fn rollback_reapplies_previous_schema(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&app, request(bundle(1), None), "a")
        .await
        .expect("v1");
    let mut wider = notes_schema();
    wider.tables[0].indexes.push(IndexSpec {
        name: "by_len".into(),
        fields: vec!["len".into()],
    });
    let v2 = deploy::deploy(
        &app,
        DeployRequest {
            schema: wider,
            ..request(bundle(2), Some(v1.record.id))
        },
        "a",
    )
    .await
    .expect("v2 adds an index to notes");
    let tables = query(&app, TABLES).await.expect("_system:tables");
    assert!(format!("{tables:?}").contains("by_len"), "{tables:?}");
    deploy::activate(&app, v1.record.id, Some(v2.record.id), "a")
        .await
        .expect("the rollback");
    let tables = query(&app, TABLES).await.expect("_system:tables");
    assert!(!format!("{tables:?}").contains("by_len"), "{tables:?}");
    assert!(format!("{tables:?}").contains("by_body"), "{tables:?}");
}
live_test!(
    #[ignore = "an index change on an existing table needs LV1 plan Task 9 (online index changes), which removes this ignore"]
    rollback_reapplies_previous_schema
);

/// Version `n` of the app for the subscription tests: every query's result
/// names `n`.
fn watched(n: u32) -> Vec<u8> {
    format!(
        r#"
import {{ query, mutation }} from "loams:server";
export const notes = {{
  version: query(async () => {n}n),
  add: mutation(async (ctx, {{ body }}) => await ctx.db.insert("notes", {{ body }})),
  count: query(async (ctx) => ({{ v: {n}n, n: (await ctx.db.query("notes").collect()).length }})),
  byBody: query(async (ctx, {{ body }}) => ({{
    v: {n}n,
    n: (await ctx.db.query("notes").withIndex("by_body", (q) => q.eq("body", body)).collect()).length,
  }})),
}};
"#
    )
    .into_bytes()
}

/// The deployment version a result names: `notes:version`'s value, or the
/// `v` of the other queries' objects.
fn named_version(result: &QueryResult) -> Option<i64> {
    match result {
        QueryResult::Value(LiveValue::I64(n)) => Some(*n),
        QueryResult::Value(LiveValue::Object(fields)) => match fields.get("v") {
            Some(LiveValue::I64(n)) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

/// An app's subscription manager, following the app's engine, and its
/// sessions.
struct Watching {
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    stop: CancellationToken,
}

impl Drop for Watching {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn watching(app: &AppEngine) -> Watching {
    let stop = CancellationToken::new();
    let subs = Arc::new(Subscriptions::spawn_for(
        app,
        SubsConfig {
            // Off: every rerun the tests count is a tick's own.
            safety_rerun: Duration::from_secs(24 * 3600),
            ..SubsConfig::default()
        },
        stop.clone(),
    ));
    let resolver = app.clone();
    let sessions = Sessions::new(
        subs.clone(),
        Arc::new(move |name: &str| deploy::resolve(&resolver, name)),
        SessionConfig::default(),
        "node".into(),
        stop.clone(),
    );
    Watching {
        subs,
        sessions,
        stop,
    }
}

fn spec(query_id: u32, function: &str, args: LiveValue) -> pb::QuerySpec {
    pb::QuerySpec {
        query_id,
        function: function.into(),
        args: MessageField::some(args.to_proto()),
        ..Default::default()
    }
}

/// What a test client saw: its state after every complete Transition, and
/// the Transitions whose state mixed the results of two deployments.
#[derive(Default)]
struct Seen {
    states: Vec<BTreeMap<u32, QueryResult>>,
    mixed: Vec<String>,
    error: Option<String>,
}

/// Applies `session`'s Transitions to a client until the session ends.
fn client(session: Session) -> Arc<Mutex<Seen>> {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let out = seen.clone();
    tokio::spawn(async move {
        let mut client = ClientState::new();
        while let Some(item) = session.outbox.pop().await {
            let applied = item
                .map_err(|e| e.to_string())
                .and_then(|t| client.apply(&t).map_err(|e| e.to_string()));
            let mut seen = out.lock().expect("unpoisoned");
            match applied {
                Ok(true) => {
                    let versions: BTreeSet<i64> =
                        client.results.values().filter_map(named_version).collect();
                    if versions.len() > 1 {
                        seen.mixed
                            .push(format!("{:?}: {:?}", client.version, client.results));
                    }
                    seen.states.push(client.results.clone());
                }
                Ok(false) => {}
                Err(e) => {
                    seen.error = Some(e);
                    return;
                }
            }
        }
    });
    seen
}

/// The latest state `seen` holds.
fn latest(seen: &Mutex<Seen>) -> BTreeMap<u32, QueryResult> {
    let seen = seen.lock().expect("unpoisoned");
    assert_eq!(seen.error, None, "the client failed");
    seen.states.last().cloned().unwrap_or_default()
}

/// Waits up to 30 s for `done`.
async fn until(what: &str, done: impl Fn() -> bool) {
    let give_up = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < give_up, "{what}: not within 30 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether every deployed query of `state` (all but `_system:*` ones)
/// names version `n`.
fn all_at(state: &BTreeMap<u32, QueryResult>, queries: usize, n: i64) -> bool {
    state.len() == queries
        && state
            .values()
            .filter(|r| !matches!(r, QueryResult::Value(LiveValue::Array(_))))
            .all(|r| named_version(r) == Some(n))
}

/// At the activation's tick every subscription of the app reruns once on
/// the new deployment: the rerun counter rises by exactly the number of
/// distinct `SubKey`s, however many sessions share them; a rollback is an
/// activation too.
async fn activation_reruns_every_subscription_once(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&app, request(watched(1), None), "a")
        .await
        .expect("v1");
    let w = watching(&app);
    let body = |b: &str| args(&[("body", LiveValue::Str(b.into()))]);
    // Five distinct keys over three sessions: notes:version, notes:count,
    // notes:byBody {a}, notes:byBody {b} and _system:tables.
    let sets = [
        vec![
            spec(1, "notes:version", args(&[])),
            spec(2, "notes:count", args(&[])),
            spec(3, "notes:byBody", body("a")),
        ],
        vec![
            spec(1, "notes:version", args(&[])),
            spec(2, "notes:byBody", body("b")),
            spec(3, TABLES, args(&[])),
        ],
        vec![
            spec(1, "notes:count", args(&[])),
            spec(2, "notes:byBody", body("a")),
        ],
    ];
    let distinct = 5;
    let clients: Vec<(usize, Arc<Mutex<Seen>>)> = sets
        .iter()
        .map(|queries| {
            let session = w
                .sessions
                .open(Start::Initial(pb::QuerySet {
                    version: 1,
                    queries: queries.clone(),
                    ..Default::default()
                }))
                .expect("a session");
            (queries.len(), client(session))
        })
        .collect();
    let at = |n: i64| {
        let clients = &clients;
        move || clients.iter().all(|(q, c)| all_at(&latest(c), *q, n))
    };
    until("every client at v1", at(1)).await;
    assert_eq!(w.subs.stats().subscriptions, distinct);
    // Quiet: no tick reruns anything without writes or errors.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = w.subs.stats().reruns;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        w.subs.stats().reruns,
        before,
        "reruns without an activation"
    );

    let v2 = deploy::deploy(&app, request(watched(2), Some(v1.record.id)), "a")
        .await
        .expect("v2");
    until("every client at v2", at(2)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        w.subs.stats().reruns - before,
        distinct,
        "one rerun per distinct SubKey at the activation"
    );
    assert_eq!(w.subs.stats().subscriptions, distinct);

    let before = w.subs.stats().reruns;
    deploy::activate(&app, v1.record.id, Some(v2.record.id), "a")
        .await
        .expect("the rollback");
    until("every client back at v1", at(1)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(w.subs.stats().reruns - before, distinct);
    for (_, c) in &clients {
        assert!(c.lock().expect("unpoisoned").mixed.is_empty());
    }
}
live_test!(activation_reruns_every_subscription_once);

/// Sessions opened before any deploy, watching deployed queries while a
/// writer runs and deployments are activated and rolled back: every
/// Transition's results carry one deployment (the sessions' test hook, and
/// the versions the results name), queries that did not resolve before the
/// first deploy resolve after it, and every client ends on the last
/// deployment.
async fn no_session_sees_mixed_deployments(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let w = watching(&app);
    let mut trace = w.sessions.trace_deployments();
    let body = |b: &str| args(&[("body", LiveValue::Str(b.into()))]);
    let queries = vec![
        spec(1, "notes:version", args(&[])),
        spec(2, "notes:count", args(&[])),
        spec(3, "notes:byBody", body("a")),
        spec(4, "notes:byBody", body("b")),
    ];
    let clients: Vec<Arc<Mutex<Seen>>> = (0..3)
        .map(|_| {
            let session = w
                .sessions
                .open(Start::Initial(pb::QuerySet {
                    version: 1,
                    queries: queries.clone(),
                    ..Default::default()
                }))
                .expect("a session");
            client(session)
        })
        .collect();
    // Nothing is deployed yet: every query holds NOT_FOUND.
    until("every client holds its errors", || {
        clients.iter().all(|c| {
            let state = latest(c);
            state.len() == 4
                && state.values().all(|r| {
                    matches!(r, QueryResult::Error(e)
                        if e.code == pb::ErrorCode::ERROR_CODE_NOT_FOUND)
                })
        })
    })
    .await;

    let v1 = deploy::deploy(&app, request(watched(1), None), "a")
        .await
        .expect("v1");
    // A writer runs through the activations.
    let writing = CancellationToken::new();
    let writer = {
        let (app, writing) = (app.clone(), writing.clone());
        tokio::spawn(async move {
            let insert = deploy::resolve(&app, INSERT).expect("_system:insert");
            let mut n = 0u32;
            while !writing.is_cancelled() {
                let b = if n.is_multiple_of(2) { "a" } else { "b" };
                app.runner()
                    .mutate(
                        insert.clone(),
                        args(&[
                            ("table", LiveValue::Str("notes".into())),
                            ("fields", args(&[("body", LiveValue::Str(b.into()))])),
                        ]),
                        None,
                    )
                    .await
                    .expect("an insert");
                n += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            n
        })
    };
    let pause = || tokio::time::sleep(Duration::from_millis(150));
    pause().await;
    let v2 = deploy::deploy(&app, request(watched(2), Some(v1.record.id)), "a")
        .await
        .expect("v2");
    pause().await;
    deploy::activate(&app, v1.record.id, Some(v2.record.id), "a")
        .await
        .expect("the rollback to v1");
    pause().await;
    let v3 = deploy::deploy(&app, request(watched(3), Some(v1.record.id)), "a")
        .await
        .expect("v3");
    pause().await;
    writing.cancel();
    let inserted = writer.await.expect("the writer");
    assert!(inserted > 0);

    until("every client at v3 with every insert", || {
        clients.iter().all(|c| {
            let state = latest(c);
            all_at(&state, 4, 3)
                && matches!(&state[&2], QueryResult::Value(LiveValue::Object(f))
                    if f.get("n") == Some(&LiveValue::F64(f64::from(inserted))))
        })
    })
    .await;
    for c in &clients {
        let seen = c.lock().expect("unpoisoned");
        assert!(seen.mixed.is_empty(), "mixed: {:?}", seen.mixed);
        // Each client saw the deployments in order, never going back to one
        // it left except through the rollback.
        let mut order: Vec<i64> = Vec::new();
        for state in &seen.states {
            if let Some(n) = state.values().find_map(named_version)
                && order.last() != Some(&n)
            {
                order.push(n);
            }
        }
        assert_eq!(order, [1, 2, 1, 3], "the deployments a client saw");
    }
    // The test hook: the deployments behind each Transition's results.
    let mut traced = 0;
    let mut last = BTreeMap::new();
    while let Ok(t) = trace.try_recv() {
        traced += 1;
        assert!(t.deployments.len() <= 1, "{t:?}");
        last.insert(t.session.clone(), t.deployments.clone());
    }
    assert!(traced > 0, "the hook traced nothing");
    assert_eq!(last.len(), 3);
    for deployments in last.values() {
        assert_eq!(*deployments, BTreeSet::from([v3.record.id]));
    }
}
live_test!(no_session_sees_mixed_deployments);

/// A deploy that crashed after writing its `pending` record leaves it
/// behind; the janitor fails it once it is older than the grace period,
/// and only then (idempotently). A pending deployment cannot be activated;
/// once failed, it can.
async fn janitor_fails_abandoned_pending_deploys(store: TestStore) {
    let bundles = loams_store::Store::in_memory();
    let app = node(&store, &bundles, js()).await;
    let v1 = deploy::deploy(&app, request(bundle(1), None), "a")
        .await
        .expect("v1");
    // The crashed deploy: steps 2 and 3 done (the bundle stored, the record
    // pending), step 5 never.
    let id = Ulid::generate();
    bundles
        .put(&deploy::bundle_path(APP, id), Bytes::from(bundle(2)))
        .await
        .expect("the bundle");
    let keys = app.runner().app().clone();
    let pending = DeploymentRecord {
        id,
        bundle_sha256: <sha2::Sha256 as sha2::Digest>::digest(bundle(2)).into(),
        schema: notes_schema(),
        created_at_ms: 0,
        created_by: "crashed".into(),
        message: String::new(),
        parent: Some(v1.record.id),
        state: DeploymentState::Pending,
    };
    app.runner()
        .store()
        .run(TxnOptions::new("test.pending"), move |txn| {
            let (keys, mut pending) = (keys.clone(), pending.clone());
            Box::pin(async move {
                pending.created_at_ms = txn.start_ts().physical_ms();
                txn.put(
                    &keys.deployment(&id.to_bytes()),
                    pending.to_proto().encode_to_vec(),
                )
                .await
            })
        })
        .await
        .expect("the pending record");

    let err = deploy::activate(&app, id, Some(v1.record.id), "a")
        .await
        .expect_err("a pending deployment");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
    assert!(err.to_string().contains("pending"), "{err}");

    // Younger than the grace period: left alone.
    assert!(PENDING_GRACE >= Duration::from_secs(60));
    let report = deploy::retain(&app).await.expect("the janitor");
    assert!(report.abandoned.is_empty(), "{report:?}");
    assert_eq!(
        record(&records(&app).await, id).state,
        DeploymentState::Pending
    );
    // Past it: failed, with the reason.
    let report = deploy::retain_with(&app, Duration::ZERO)
        .await
        .expect("the janitor");
    assert_eq!(report.abandoned, [id]);
    let failed = record(&records(&app).await, id).state.clone();
    assert!(
        matches!(&failed, DeploymentState::Failed { reason } if reason.contains("did not finish")),
        "{failed:?}"
    );
    // Idempotent; and no other record moved.
    let again = deploy::retain_with(&app, Duration::ZERO)
        .await
        .expect("again");
    assert!(again.abandoned.is_empty(), "{again:?}");
    assert_eq!(record(&records(&app).await, id).state, failed);
    assert_eq!(
        record(&records(&app).await, v1.record.id).state,
        DeploymentState::Active
    );
    assert_eq!(version(&app).await, LiveValue::I64(1));

    // A failed deployment can still be activated, through the same path.
    deploy::activate(&app, id, Some(v1.record.id), "a")
        .await
        .expect("the failed deployment activates");
    assert_eq!(version(&app).await, LiveValue::I64(2));
    assert_eq!(
        record(&records(&app).await, id).state,
        DeploymentState::Active
    );
}
live_test!(janitor_fails_abandoned_pending_deploys);
