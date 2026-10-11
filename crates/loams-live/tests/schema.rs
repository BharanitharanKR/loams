//! LV1 plan Task 8 (design §45 §4.1, D684): schema validators. A deployed
//! schema gives a table a validator, which every write to the table must
//! match — through a deployed function or a `_system:*` one alike — or it
//! fails with `INVALID_ARGUMENT`, reason `live_schema_violation`, naming the
//! field paths. A deploy that tightens a validator first enforces the new
//! one on new writes, then scans the table's documents in a leased,
//! checkpointed, rate-limited job (`backfill::TableJob`) while its record
//! is `validating`; a violation fails the deploy, reverts the validator and
//! lists up to 100 offending ids. Loosening applies at once, and
//! `schema_validation: false` skips the scan. Every case runs on the
//! embedded store and, with `LOAMS_TEST_PD`, on TiKV.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use loams_kv::{Txn, TxnOptions};
use loams_live::backfill::{self, Finding, JobKind, JobOptions, TableJob, Visitor};
use loams_live::deploy::{
    self, AppEngine, BundleLoader, DeployOutcome, DeployRequest, DeploymentRecord, DeploymentState,
    PageRequest, Runtime,
};
use loams_live::schema::{Schema, TableSchema};
use loams_live::system::{GET, INSERT, PATCH, REPLACE};
use loams_live::testing::TestStore;
use loams_live::validate::{Validator, render_path};
use loams_live::{
    AppKeys, Doc, LiveError, LiveValue, Runner, RunnerOptions, TableDef, catalog, live_test, pb,
};
use loams_live_js::{JsConfig, JsLoader};
use tokio::sync::oneshot;
use ulid::Ulid;

const APP: &str = "schematest";

/// The app's functions: `notes:add` inserts `doc`, `notes:patch` and
/// `notes:replace` write document `id`, and `notes:caught` inserts `doc`
/// and returns the message of what its handler caught.
const BUNDLE: &str = r#"
import { query, mutation } from "loams:server";
export const notes = {
  add: mutation(async (ctx, { doc }) => await ctx.db.insert("notes", doc)),
  patch: mutation(async (ctx, { id, fields }) => { await ctx.db.patch(id, fields); return null; }),
  replace: mutation(async (ctx, { id, doc }) => { await ctx.db.replace(id, doc); return null; }),
  caught: mutation(async (ctx, { doc }) => {
    try { await ctx.db.insert("notes", doc); return "inserted"; } catch (e) { return String(e.message); }
  }),
  count: query(async (ctx) => (await ctx.db.query("notes").collect()).length),
};
"#;

fn s(v: &str) -> LiveValue {
    LiveValue::Str(v.into())
}

fn i(v: i64) -> LiveValue {
    LiveValue::I64(v)
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn object(fields: &[(&str, Validator)]) -> Validator {
    Validator::Object(
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn optional(v: Validator) -> Validator {
    Validator::Optional(Box::new(v))
}

/// `{ body: string, n?: int64 }`.
fn loose() -> Validator {
    object(&[
        ("body", Validator::String),
        ("n", optional(Validator::Int64)),
    ])
}

/// `{ body: string, n: int64 }`: `loose` with `n` required.
fn strict() -> Validator {
    object(&[("body", Validator::String), ("n", Validator::Int64)])
}

/// A schema of `notes` with `validator`, and `users` without one.
fn schema(validator: Option<Validator>, schema_validation: bool) -> Schema {
    Schema {
        tables: vec![
            TableSchema {
                name: "notes".into(),
                validator,
                schema_validation,
                ..TableSchema::default()
            },
            TableSchema {
                name: "users".into(),
                ..TableSchema::default()
            },
        ],
    }
}

fn request(schema: Schema, expected_active: Option<Ulid>) -> DeployRequest {
    DeployRequest {
        bundle: BUNDLE.as_bytes().to_vec(),
        schema,
        expected_active,
        message: "test".into(),
        dry_run: false,
    }
}

fn js() -> Arc<dyn BundleLoader> {
    Arc::new(
        JsLoader::new(JsConfig {
            contexts: 1,
            ..JsConfig::default()
        })
        .expect("an in-process loader"),
    )
}

/// The app's engine on `store`, its runner with `options`.
async fn node_with(store: &TestStore, options: RunnerOptions) -> AppEngine {
    let runner = Runner::open_with(store.store(), &store.live_config(APP), options)
        .await
        .expect("the runner opens");
    AppEngine::open(
        APP,
        runner,
        Runtime {
            bundles: loams_store::Store::in_memory(),
            loader: js(),
        },
    )
    .await
    .expect("the engine opens")
}

async fn node(store: &TestStore) -> AppEngine {
    node_with(store, RunnerOptions::default()).await
}

/// Validation scans of `batch` documents at `docs_per_s`.
fn slow_scans(batch: usize, docs_per_s: u32, lease: Duration) -> RunnerOptions {
    RunnerOptions {
        jobs: JobOptions {
            batch,
            docs_per_s,
            lease,
        },
        ..RunnerOptions::default()
    }
}

async fn deploy(app: &AppEngine, schema: Schema, expected: Option<Ulid>) -> DeployOutcome {
    deploy::deploy(app, request(schema, expected), "alice")
        .await
        .expect("the deploy")
}

async fn mutate(app: &AppEngine, name: &str, args: LiveValue) -> Result<LiveValue, LiveError> {
    let f = deploy::resolve(app, name)?;
    Ok(app.runner().mutate(f, args, None).await?.result)
}

async fn query(app: &AppEngine, name: &str, args: LiveValue) -> Result<LiveValue, LiveError> {
    let f = deploy::resolve(app, name)?;
    let at = app.runner().store().now().await.expect("now");
    Ok(app.runner().query(&*f, args, at).await?.result)
}

/// Inserts `fields` into `notes` with `_system:insert`; returns the id.
async fn add(app: &AppEngine, fields: LiveValue) -> Result<String, LiveError> {
    match mutate(
        app,
        INSERT,
        obj(&[("table", s("notes")), ("fields", fields)]),
    )
    .await?
    {
        LiveValue::Str(id) => Ok(id),
        other => panic!("an id, not {other:?}"),
    }
}

async fn records(app: &AppEngine) -> Vec<DeploymentRecord> {
    deploy::list_deployments(app, PageRequest::default())
        .await
        .expect("the deployments list")
        .items
}

async fn state_of(app: &AppEngine, id: Ulid) -> DeploymentState {
    records(app)
        .await
        .into_iter()
        .find(|r| r.id == id)
        .unwrap_or_else(|| panic!("no record of {id}"))
        .state
}

/// The table `name` as the catalog holds it now.
async fn table(app: &AppEngine, name: &str) -> TableDef {
    let store = app.runner().store();
    let mut snap = store
        .snapshot(store.now().await.expect("now"))
        .await
        .expect("a snapshot");
    catalog::load_table(&mut snap, &AppKeys::dedicated(), name)
        .await
        .expect("the catalog reads")
        .unwrap_or_else(|| panic!("no table {name}"))
}

/// The validation job of table `name`, if one is recorded.
async fn job(app: &AppEngine, name: &str) -> Option<backfill::JobRecord> {
    let id = table(app, name).await.id;
    let store = app.runner().store();
    let mut snap = store
        .snapshot(store.now().await.expect("now"))
        .await
        .expect("a snapshot");
    backfill::load_job(&mut snap, &AppKeys::dedicated(), JobKind::Validate, id)
        .await
        .expect("the job record reads")
}

/// Asserts `result` is a schema violation at `paths` that does not show
/// `secret`, and returns it.
#[track_caller]
fn violation(result: Result<impl std::fmt::Debug, LiveError>, paths: &[&str]) -> LiveError {
    let err = match result {
        Err(e) => e,
        Ok(v) => panic!("a schema violation at {paths:?}, not {v:?}"),
    };
    assert_eq!(err.reason(), Some("live_schema_violation"), "{err}");
    assert_eq!(
        err.code(),
        pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT,
        "{err}"
    );
    let found: Vec<String> = err
        .field_violations()
        .iter()
        .map(|v| render_path(&v.path))
        .collect();
    assert_eq!(found, paths, "{err}");
    assert!(!err.to_string().contains("secret"), "a value leaked: {err}");
    err
}

/// Every write to a table with a validator must match it: inserts, patches
/// (the merged document) and replaces through deployed functions, with
/// field paths and no values in the error, and `id(table)` checked against
/// the named table. Refused writes leave nothing behind.
async fn write_violating_validator_refused(store: TestStore) {
    let app = node(&store).await;
    let validator = object(&[
        ("body", Validator::String),
        ("n", optional(Validator::Int64)),
        ("owner", optional(Validator::Id("users".into()))),
    ]);
    let v1 = deploy(&app, schema(Some(validator), true), None).await;
    assert_eq!(v1.changes.create, ["notes", "users"]);

    let add_doc = |doc: LiveValue| mutate(&app, "notes:add", obj(&[("doc", doc)]));
    let id = match add_doc(obj(&[("body", s("a")), ("n", i(1))])).await {
        Ok(LiveValue::Str(id)) => id,
        other => panic!("a valid insert: {other:?}"),
    };
    violation(
        add_doc(obj(&[("body", s("b")), ("n", s("secret"))])).await,
        &["$.n"],
    );
    violation(add_doc(obj(&[("n", i(2))])).await, &["$.body"]);
    let err = violation(
        add_doc(obj(&[("body", s("b")), ("extra", s("secret"))])).await,
        &["$.extra"],
    );
    assert!(err.to_string().contains("'notes'"), "{err}");

    // A patch is checked as the merged document; a replace as the new one.
    violation(
        mutate(
            &app,
            "notes:patch",
            obj(&[("id", s(&id)), ("fields", obj(&[("n", s("secret"))]))]),
        )
        .await,
        &["$.n"],
    );
    violation(
        mutate(
            &app,
            "notes:replace",
            obj(&[("id", s(&id)), ("doc", obj(&[("n", i(3))]))]),
        )
        .await,
        &["$.body"],
    );
    mutate(
        &app,
        "notes:patch",
        obj(&[("id", s(&id)), ("fields", obj(&[("n", i(4))]))]),
    )
    .await
    .expect("a valid patch");
    let doc = query(&app, GET, obj(&[("id", s(&id))]))
        .await
        .expect("_system:get");
    let LiveValue::Object(fields) = doc else {
        panic!("a document, not {doc:?}")
    };
    assert_eq!(fields["body"], s("a"));
    assert_eq!(fields["n"], i(4));

    // `id(users)`: an id of `users` matches, one of `notes` does not.
    let user = match mutate(
        &app,
        INSERT,
        obj(&[("table", s("users")), ("fields", obj(&[]))]),
    )
    .await
    {
        Ok(LiveValue::Str(id)) => id,
        other => panic!("a user: {other:?}"),
    };
    add_doc(obj(&[("body", s("c")), ("owner", s(&user))]))
        .await
        .expect("an owner of users");
    violation(
        add_doc(obj(&[("body", s("d")), ("owner", s(&id))])).await,
        &["$.owner"],
    );

    // A handler sees a catchable error naming the field, not the value.
    let caught = mutate(
        &app,
        "notes:caught",
        obj(&[("doc", obj(&[("body", i(7)), ("n", s("secret"))]))]),
    )
    .await
    .expect("the handler catches the violation");
    let LiveValue::Str(message) = caught else {
        panic!("a message, not {caught:?}")
    };
    assert!(
        message.contains("$.n") && message.contains("$.body"),
        "{message}"
    );
    assert!(!message.contains("secret"), "{message}");

    // Only the valid documents were written.
    assert_eq!(
        query(&app, "notes:count", obj(&[])).await.expect("count"),
        LiveValue::F64(2.0)
    );
}
live_test!(write_violating_validator_refused);

/// The `_system:*` mutations are checked like deployed functions.
async fn system_insert_is_validated_too(store: TestStore) {
    let app = node(&store).await;
    deploy(&app, schema(Some(strict()), true), None).await;
    violation(add(&app, obj(&[("body", s("secret"))])).await, &["$.n"]);
    violation(
        add(&app, obj(&[("body", i(1)), ("n", i(1))])).await,
        &["$.body"],
    );
    let id = add(&app, obj(&[("body", s("a")), ("n", i(1))]))
        .await
        .expect("a valid insert");
    violation(
        mutate(
            &app,
            PATCH,
            obj(&[("id", s(&id)), ("fields", obj(&[("n", s("secret"))]))]),
        )
        .await,
        &["$.n"],
    );
    violation(
        mutate(
            &app,
            REPLACE,
            obj(&[("id", s(&id)), ("fields", obj(&[("body", s("b"))]))]),
        )
        .await,
        &["$.n"],
    );
    mutate(
        &app,
        REPLACE,
        obj(&[
            ("id", s(&id)),
            ("fields", obj(&[("body", s("b")), ("n", i(2))])),
        ]),
    )
    .await
    .expect("a valid replace");
    // A table without a validator takes anything.
    mutate(
        &app,
        INSERT,
        obj(&[("table", s("users")), ("fields", obj(&[("x", i(1))]))]),
    )
    .await
    .expect("users has no validator");
}
live_test!(system_insert_is_validated_too);

/// A deploy whose validator is stricter scans the table, finds every
/// document valid, and activates; the new validator then holds for writes,
/// and no validation state is left behind.
async fn tightening_with_valid_data_succeeds(store: TestStore) {
    let app = node(&store).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    for n in 0..10 {
        add(&app, obj(&[("body", s("a")), ("n", i(n))]))
            .await
            .expect("a valid insert");
    }
    let v2 = deploy(&app, schema(Some(strict()), true), Some(v1.record.id)).await;
    assert_eq!(v2.record.state, DeploymentState::Active);
    assert_eq!(v2.changes.validators, ["notes"]);
    assert_eq!(v2.changes.validate, ["notes"]);
    assert_eq!(app.active(), Some(v2.record.id));
    let notes = table(&app, "notes").await;
    assert_eq!(notes.validation.validator, Some(strict()));
    assert_eq!(notes.validation.enforcing, None);
    assert!(job(&app, "notes").await.is_none(), "the job record is gone");
    violation(add(&app, obj(&[("body", s("b"))])).await, &["$.n"]);
    add(&app, obj(&[("body", s("b")), ("n", i(11))]))
        .await
        .expect("valid under the new validator");
}
live_test!(tightening_with_valid_data_succeeds);

/// A tightening that existing documents violate fails the deploy: its
/// record is `failed` and lists the offending ids (at most 100), the
/// pointer and the old validator stay, and nothing of the validation is
/// left. Once the data is fixed, the same schema deploys.
async fn tightening_with_invalid_data_fails_and_reverts(store: TestStore) {
    let app = node(&store).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    let mut good = Vec::new();
    for n in 0..5 {
        good.push(
            add(&app, obj(&[("body", s("ok")), ("n", i(n))]))
                .await
                .expect("a valid insert"),
        );
    }
    let mut bad = BTreeSet::new();
    for _ in 0..3 {
        bad.insert(
            add(&app, obj(&[("body", s("no n"))]))
                .await
                .expect("valid under the loose validator"),
        );
    }

    let err = deploy::deploy(
        &app,
        request(schema(Some(strict()), true), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("existing documents violate the new validator");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
    let text = err.to_string();
    for id in &bad {
        assert!(text.contains(id.as_str()), "{id} missing from: {text}");
    }
    for id in &good {
        assert!(!text.contains(id.as_str()), "{id} is valid: {text}");
    }
    assert!(
        text.contains("$.n"),
        "the first violation is described: {text}"
    );
    let all = records(&app).await;
    let failed = &all[0];
    assert_ne!(failed.id, v1.record.id);
    match &failed.state {
        DeploymentState::Failed { reason } => {
            for id in &bad {
                assert!(reason.contains(id.as_str()), "{reason}");
            }
        }
        other => panic!("the deploy failed, not {other:?}"),
    }
    assert_eq!(app.active(), Some(v1.record.id));
    assert_eq!(
        app.read_active().await.expect("pointer"),
        Some(v1.record.id)
    );
    let notes = table(&app, "notes").await;
    assert_eq!(notes.validation.validator, Some(loose()));
    assert_eq!(notes.validation.enforcing, None, "the validator reverted");
    assert!(job(&app, "notes").await.is_none(), "the job record is gone");
    add(&app, obj(&[("body", s("still loose"))]))
        .await
        .map(|id| bad.insert(id))
        .expect("the old validator holds again");

    // More than 100 offenders: the first 100 are listed.
    for _ in 0..100 {
        bad.insert(add(&app, obj(&[("body", s("no n"))])).await.expect("loose"));
    }
    let err = deploy::deploy(
        &app,
        request(schema(Some(strict()), true), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("104 violations");
    let listed = bad
        .iter()
        .filter(|id| err.to_string().contains(id.as_str()))
        .count();
    assert_eq!(listed, backfill::MAX_FINDINGS, "{err}");

    // Fix the data (a user migration), then the same schema deploys.
    for id in &bad {
        mutate(
            &app,
            PATCH,
            obj(&[("id", s(id)), ("fields", obj(&[("n", i(0))]))]),
        )
        .await
        .expect("a migration patch");
    }
    let v2 = deploy(&app, schema(Some(strict()), true), Some(v1.record.id)).await;
    assert_eq!(v2.changes.validate, ["notes"]);
    assert_eq!(app.active(), Some(v2.record.id));
}
live_test!(tightening_with_invalid_data_fails_and_reverts);

/// A looser validator (a field made optional, a union widened) applies at
/// once: no scan, so a document that only the stricter validator's
/// skipped scan let through does not fail it.
async fn loosening_skips_scan(store: TestStore) {
    let app = node(&store).await;
    let v0 = deploy(&app, schema(None, true), None).await;
    // Valid under no validator, invalid under every one below.
    add(&app, obj(&[("body", i(1))]))
        .await
        .expect("no validator");
    let v1 = deploy(&app, schema(Some(strict()), false), Some(v0.record.id)).await;
    assert!(v1.changes.validate.is_empty());

    let v2 = deploy(&app, schema(Some(loose()), true), Some(v1.record.id)).await;
    assert_eq!(v2.changes.validators, ["notes"]);
    assert!(v2.changes.validate.is_empty(), "loosening scans nothing");
    assert_eq!(v2.record.state, DeploymentState::Active);
    assert_eq!(
        table(&app, "notes").await.validation.validator,
        Some(loose())
    );
    add(&app, obj(&[("body", s("no n"))]))
        .await
        .expect("valid under the looser validator");

    let wider = object(&[
        (
            "body",
            Validator::Union(vec![Validator::String, Validator::Bytes]),
        ),
        ("n", optional(Validator::Int64)),
        ("tag", optional(Validator::String)),
    ]);
    let v3 = deploy(&app, schema(Some(wider.clone()), true), Some(v2.record.id)).await;
    assert_eq!(v3.changes.validators, ["notes"]);
    assert!(v3.changes.validate.is_empty(), "widening scans nothing");
    mutate(
        &app,
        INSERT,
        obj(&[
            ("table", s("notes")),
            (
                "fields",
                obj(&[("body", LiveValue::Bytes(vec![1])), ("tag", s("t"))]),
            ),
        ]),
    )
    .await
    .expect("valid under the wider validator");

    // Removing the validator is the loosest change.
    let v4 = deploy(&app, schema(None, true), Some(v3.record.id)).await;
    assert!(v4.changes.validate.is_empty());
    assert_eq!(table(&app, "notes").await.validation.validator, None);
}
live_test!(loosening_skips_scan);

/// `schema_validation: false` applies a stricter validator without
/// scanning existing documents, which stay; new writes are checked. A dry
/// run reports what a deploy would validate and writes nothing.
async fn schema_validation_false_skips_scan_but_checks_writes(store: TestStore) {
    let app = node(&store).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    let old = add(&app, obj(&[("body", s("no n"))]))
        .await
        .expect("valid under the loose validator");

    let dry = deploy::deploy(
        &app,
        DeployRequest {
            dry_run: true,
            ..request(schema(Some(strict()), true), Some(v1.record.id))
        },
        "alice",
    )
    .await
    .expect("a dry run");
    assert_eq!(dry.changes.validators, ["notes"]);
    assert_eq!(
        dry.changes.validate,
        ["notes"],
        "the dry run reports the scan"
    );
    assert_eq!(records(&app).await.len(), 1, "a dry run writes nothing");
    assert_eq!(
        table(&app, "notes").await.validation.validator,
        Some(loose())
    );

    let v2 = deploy(&app, schema(Some(strict()), false), Some(v1.record.id)).await;
    assert_eq!(v2.changes.validators, ["notes"]);
    assert!(v2.changes.validate.is_empty(), "no scan");
    assert_eq!(v2.record.state, DeploymentState::Active);
    assert_eq!(
        table(&app, "notes").await.validation.validator,
        Some(strict())
    );
    let doc = query(&app, GET, obj(&[("id", s(&old))]))
        .await
        .expect("_system:get");
    assert_ne!(doc, LiveValue::Null, "the old document stays");
    violation(add(&app, obj(&[("body", s("no n"))])).await, &["$.n"]);
    add(&app, obj(&[("body", s("n")), ("n", i(1))]))
        .await
        .expect("valid under the new validator");
}
live_test!(schema_validation_false_skips_scan_but_checks_writes);

/// Waits until a deployment's record is `validating`; returns its id.
async fn until_validating(app: &AppEngine) -> Ulid {
    let start = Instant::now();
    loop {
        for r in records(app).await {
            if r.state == DeploymentState::Validating {
                return r.id;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "no deployment became validating"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// While a tightening scans, new writes must match the new validator and
/// the old one: a write that only the old one accepts is refused (naming
/// the deployment being validated), as is one neither accepts; valid ones
/// go through, and the deploy then activates.
async fn writes_during_validation_scan_are_checked(store: TestStore) {
    let app = node_with(&store, slow_scans(2, 20, Duration::from_secs(30))).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    let mut ids = Vec::new();
    for n in 0..60 {
        ids.push(
            add(&app, obj(&[("body", s("a")), ("n", i(n))]))
                .await
                .expect("a valid insert"),
        );
    }
    let task = tokio::spawn({
        let app = app.clone();
        async move {
            deploy::deploy(
                &app,
                request(schema(Some(strict()), true), Some(v1.record.id)),
                "alice",
            )
            .await
        }
    });
    let v2 = until_validating(&app).await;
    let started = Instant::now();

    let err = violation(add(&app, obj(&[("body", s("no n"))])).await, &["$.n"]);
    assert!(err.to_string().contains(&v2.to_string()), "{err}");
    violation(
        add(&app, obj(&[("body", i(1)), ("n", i(1))])).await,
        &["$.body"],
    );
    violation(
        mutate(
            &app,
            REPLACE,
            obj(&[("id", s(&ids[59])), ("fields", obj(&[("body", s("b"))]))]),
        )
        .await,
        &["$.n"],
    );
    add(&app, obj(&[("body", s("both")), ("n", i(100))]))
        .await
        .expect("valid under both validators");
    assert_eq!(
        state_of(&app, v2).await,
        DeploymentState::Validating,
        "the writes ran during the scan"
    );

    let done = task.await.expect("the deploy task").expect("the deploy");
    assert_eq!(done.record.id, v2);
    assert_eq!(done.record.state, DeploymentState::Active);
    assert!(
        started.elapsed() >= Duration::from_millis(500),
        "the scan is rate-limited"
    );
    assert_eq!(
        table(&app, "notes").await.validation.validator,
        Some(strict())
    );
}
live_test!(writes_during_validation_scan_are_checked);

/// While one deploy validates a table, another that tightens the same
/// table is refused (and fails); the first one still activates.
async fn a_table_is_validated_by_one_deploy_at_a_time(store: TestStore) {
    let app = node_with(&store, slow_scans(2, 20, Duration::from_secs(30))).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    for n in 0..40 {
        add(&app, obj(&[("body", s("a")), ("n", i(n))]))
            .await
            .expect("a valid insert");
    }
    let task = tokio::spawn({
        let app = app.clone();
        async move {
            deploy::deploy(
                &app,
                request(schema(Some(strict()), true), Some(v1.record.id)),
                "alice",
            )
            .await
        }
    });
    let first = until_validating(&app).await;
    let tighter = object(&[("body", Validator::String), ("n", Validator::Int64)]);
    let err = deploy::deploy(
        &app,
        request(schema(Some(tighter), true), Some(v1.record.id)),
        "bob",
    )
    .await
    .expect_err("the table is being validated");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
    assert!(err.to_string().contains(&first.to_string()), "{err}");
    let second = records(&app).await[0].clone();
    assert!(
        matches!(second.state, DeploymentState::Failed { .. }),
        "{second:?}"
    );
    let done = task.await.expect("the deploy task").expect("the deploy");
    assert_eq!(done.record.id, first);
    assert_eq!(app.active(), Some(first));
}
live_test!(a_table_is_validated_by_one_deploy_at_a_time);

/// A validation whose node died (its lease expired) is released by the
/// janitor: the record fails, the new validator is no longer enforced, the
/// job is gone, and the next tightening validates afresh. A live lease
/// keeps the janitor off.
async fn janitor_releases_abandoned_validations(store: TestStore) {
    let app = node_with(&store, slow_scans(4, 10_000, Duration::from_millis(300))).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    for n in 0..20 {
        add(&app, obj(&[("body", s("a")), ("n", i(n))]))
            .await
            .expect("a valid insert");
    }
    app.crash_after_next_fence();
    let err = deploy::deploy(
        &app,
        request(schema(Some(strict()), true), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("the node dies after the fence");
    assert!(matches!(err, LiveError::Internal(_)), "{err}");
    let v2 = records(&app).await[0].id;
    assert_eq!(state_of(&app, v2).await, DeploymentState::Validating);
    violation(add(&app, obj(&[("body", s("no n"))])).await, &["$.n"]);
    let early = deploy::retain(&app).await.expect("a janitor round");
    assert!(early.validations_released.is_empty(), "the lease lives");
    assert_eq!(state_of(&app, v2).await, DeploymentState::Validating);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let retained = deploy::retain(&app).await.expect("the janitor round");
    assert_eq!(retained.validations_released, [v2]);
    assert!(
        matches!(state_of(&app, v2).await, DeploymentState::Failed { ref reason } if reason.contains("validat")),
        "{:?}",
        state_of(&app, v2).await
    );
    let notes = table(&app, "notes").await;
    assert_eq!(notes.validation.enforcing, None);
    assert_eq!(notes.validation.validator, Some(loose()));
    assert!(job(&app, "notes").await.is_none());
    let lacking = add(&app, obj(&[("body", s("no n"))]))
        .await
        .expect("only the old validator holds");

    // A later tightening is not blocked, and validates afresh.
    let err = deploy::deploy(
        &app,
        request(schema(Some(strict()), true), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("one document lacks n");
    assert!(err.to_string().contains(&lacking), "{err}");
    let again = deploy::retain(&app).await.expect("another round");
    assert!(again.validations_released.is_empty());
}
live_test!(janitor_releases_abandoned_validations);

/// Steps 4 and 5 run detached from the caller: a deploy whose caller goes
/// away mid-scan still finishes, so its stricter validator is never left
/// enforced past the deploy's own outcome.
async fn a_deploy_finishes_when_its_caller_goes_away(store: TestStore) {
    let app = node_with(&store, slow_scans(2, 40, Duration::from_secs(30))).await;
    let v1 = deploy(&app, schema(Some(loose()), true), None).await;
    for n in 0..40 {
        add(&app, obj(&[("body", s("a")), ("n", i(n))]))
            .await
            .expect("a valid insert");
    }
    let task = tokio::spawn({
        let app = app.clone();
        async move {
            deploy::deploy(
                &app,
                request(schema(Some(strict()), true), Some(v1.record.id)),
                "alice",
            )
            .await
        }
    });
    let v2 = until_validating(&app).await;
    task.abort();
    let _ = task.await;
    let start = Instant::now();
    while state_of(&app, v2).await == DeploymentState::Validating {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "the deploy hangs"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state_of(&app, v2).await, DeploymentState::Active);
    assert_eq!(app.read_active().await.expect("pointer"), Some(v2));
    assert_eq!(table(&app, "notes").await.validation.enforcing, None);
}
live_test!(a_deploy_finishes_when_its_caller_goes_away);

/// Reports every document of a batch as a finding, and fails the batch
/// after `fail_after` batches (if set).
struct Every {
    batches: Mutex<usize>,
    fail_after: Option<usize>,
}

impl Visitor for Every {
    fn visit<'a>(
        &'a self,
        _txn: &'a mut Txn,
        _app: &'a AppKeys,
        docs: &'a [Doc],
    ) -> BoxFuture<'a, Result<Vec<Finding>, LiveError>> {
        Box::pin(async move {
            let mut batches = self.batches.lock().expect("unpoisoned");
            *batches += 1;
            if self.fail_after.is_some_and(|n| *batches > n) {
                return Err(LiveError::Internal("the visitor stops".into()));
            }
            Ok(docs
                .iter()
                .map(|d| Finding {
                    id: d.id,
                    detail: String::new(),
                })
                .collect())
        })
    }
}

/// A table job holds a lease and checkpoints every batch: a job that fails
/// keeps its checkpoint, another owner cannot take it while the lease
/// lives, and after it expires the new owner resumes from the checkpoint,
/// so every document is visited once (a failed batch's findings roll back
/// with it).
async fn table_job_resumes_from_its_checkpoint(store: TestStore) {
    let app = node_with(&store, slow_scans(4, 10_000, Duration::from_millis(400))).await;
    deploy(&app, schema(None, true), None).await;
    let mut all = BTreeSet::new();
    for _ in 0..30 {
        all.insert(add(&app, obj(&[("x", i(1))])).await.expect("insert"));
    }
    let notes = table(&app, "notes").await.id;
    let keys = AppKeys::dedicated();
    let deployment = Ulid::from_parts(1_700_000_000_000, 1);
    let lease = Duration::from_millis(400);
    app.runner()
        .store()
        .run(TxnOptions::new("test.job.create"), |txn| {
            let keys = keys.clone();
            Box::pin(async move {
                backfill::create_job(
                    txn,
                    &keys,
                    JobKind::Validate,
                    notes,
                    deployment,
                    "first",
                    lease,
                )
                .await
                .map_err(|e| e.into_txn().expect_err("a storage error only"))
            })
        })
        .await
        .expect("the job record");

    let first = TableJob::new(app.runner().clone(), JobKind::Validate, notes, "first");
    let failing = Arc::new(Every {
        batches: Mutex::new(0),
        fail_after: Some(3),
    });
    first
        .run(failing, usize::MAX)
        .await
        .expect_err("the visitor fails on the fourth batch");
    let kept = job(&app, "notes").await.expect("the job record stays");
    assert_eq!(kept.scanned, 12);
    assert_eq!(kept.findings.len(), 12);
    assert_eq!(kept.owner, "first");

    let second = TableJob::new(app.runner().clone(), JobKind::Validate, notes, "second");
    let every = Arc::new(Every {
        batches: Mutex::new(0),
        fail_after: None,
    });
    let err = second
        .run(every.clone(), usize::MAX)
        .await
        .expect_err("the first owner's lease lives");
    assert!(matches!(err, LiveError::FailedPrecondition(_)), "{err}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let done = second
        .run(every, usize::MAX)
        .await
        .expect("the second owner takes over");
    assert!(done.done);
    assert_eq!(done.owner, "second");
    assert_eq!(done.scanned, 30);
    let found: Vec<String> = done.findings.iter().map(|f| f.id.to_string()).collect();
    let distinct: BTreeSet<String> = found.iter().cloned().collect();
    assert_eq!(found.len(), 30, "no document twice");
    assert_eq!(distinct, all, "no document missed");
}
live_test!(table_job_resumes_from_its_checkpoint);

/// The fence a tightening deploy commits with its new validators
/// (`Journal::fence`): a transaction that appends to the journal, started
/// before the fence and committing after it, conflicts and reruns, whether
/// its shard's head existed before or not. So no mutation that read the
/// old validators commits after them. (On the embedded store `lock_keys`
/// would not do this: a lock leaves no version for others to conflict on.)
async fn the_journal_fence_makes_mutations_in_flight_rerun(store: TestStore) {
    let runner = Runner::open(store.store(), &store.live_config(APP))
        .await
        .expect("the runner opens");
    let journal = runner.journal().await.expect("the journal");
    for round in ["an absent head", "a present head"] {
        let attempts = Arc::new(AtomicU32::new(0));
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (go_tx, go_rx) = oneshot::channel::<()>();
        let gate = Arc::new(Mutex::new(Some((started_tx, go_rx))));
        let writer = tokio::spawn({
            let (store, journal, attempts) = (store.store(), journal.clone(), attempts.clone());
            async move {
                store
                    .run(TxnOptions::new("test.in_flight"), |txn| {
                        let (journal, attempts, gate) =
                            (journal.clone(), attempts.clone(), gate.clone());
                        Box::pin(async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            let entry = pb::JournalEntry {
                                writes: vec![pb::WriteRecord {
                                    table_id: 1,
                                    doc_id: vec![7; 16],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            };
                            journal
                                .append_to(txn, 0, entry)
                                .await
                                .map_err(|e| e.into_txn().expect_err("storage errors only"))?;
                            let first = gate.lock().expect("unpoisoned").take();
                            if let Some((started, go)) = first {
                                let _ = started.send(());
                                let _ = go.await;
                            }
                            Ok(())
                        })
                    })
                    .await
            }
        });
        started_rx.await.expect("the writer started");
        store
            .run(TxnOptions::new("test.fence"), |txn| {
                let journal = journal.clone();
                Box::pin(async move {
                    journal
                        .fence(txn)
                        .await
                        .map_err(|e| e.into_txn().expect_err("storage errors only"))
                })
            })
            .await
            .expect("the fence commits");
        go_tx.send(()).expect("the writer waits");
        writer
            .await
            .expect("the writer task")
            .expect("the writer commits on its second attempt");
        assert_eq!(attempts.load(Ordering::SeqCst), 2, "{round}: one rerun");
    }
}
live_test!(the_journal_fence_makes_mutations_in_flight_rerun);
