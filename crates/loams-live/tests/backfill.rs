//! LV1 plan Task 9 (design §45 §4.2, D685): online index changes. A deploy
//! that adds an index to an existing table adds it `backfilling` (every
//! write maintains it, no query reads it) in a transaction that fences the
//! journal, so every mutation in flight reruns and maintains it too (row
//! T9-1: the fence of T8-4 instead of a mutation lifetime wait). A leased,
//! checkpointed, rate-limited `backfill::TableJob` then writes the entries
//! of the documents already there, each batch locking the documents it
//! indexes; the swap makes the index `ready`. A deploy that drops an index
//! makes it `dropping` (writes stop maintaining it) in a fenced swap, and
//! its entries are then deleted in batches before the definition goes.
//! Every case runs on the embedded store and, with `LOAMS_TEST_PD`, on
//! TiKV.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use loams_kv::{Txn, TxnOptions};
use loams_live::backfill::{self, Finding, IndexBackfill, JobKind, JobOptions, TableJob, Visitor};
use loams_live::catalog::IndexState;
use loams_live::deploy::{self, AppEngine, BundleLoader, DeployOutcome, DeployRequest, Runtime};
use loams_live::schema::{IndexChange, Schema, TableSchema};
use loams_live::system::{INSERT, PATCH, QUERY, TABLES};
use loams_live::testing::TestStore;
use loams_live::testing::checker::{ViolationKind, run_reactive_checker};
use loams_live::testing::workload::{Disturbance, Workload};
use loams_live::{
    AppKeys, Doc, FnKind, Function, IndexSpec, LiveError, LiveTxn, LiveValue, Runner,
    RunnerOptions, TableDef, catalog, docs, live_test, pb,
};
use loams_live_js::{JsConfig, JsLoader};
use tokio::sync::Notify;
use ulid::Ulid;

const APP: &str = "backfilltest";

const BUNDLE: &str = r#"
import { query } from "loams:server";
export const notes = {
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

/// A schema of `notes` with these indexes.
fn notes(indexes: &[(&str, &[&str])]) -> Schema {
    Schema {
        tables: vec![TableSchema {
            name: "notes".into(),
            indexes: indexes
                .iter()
                .map(|(name, fields)| IndexSpec {
                    name: (*name).into(),
                    fields: fields.iter().map(|f| (*f).to_string()).collect(),
                })
                .collect(),
            ..TableSchema::default()
        }],
    }
}

fn change(index: &str) -> IndexChange {
    IndexChange {
        table: "notes".into(),
        index: index.into(),
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

/// The app's engine. Mutations get 30 s (not 10): a test holds one open
/// across a whole deploy, which a loaded TiKV can stretch.
async fn node_with(store: &TestStore, options: RunnerOptions) -> AppEngine {
    let mut config = store.live_config(APP);
    config.limits.mutation_deadline = Duration::from_secs(30);
    let runner = Runner::open_with(store.store(), &config, options)
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

/// Jobs of `batch` documents at `docs_per_s`, with leases of `lease`.
fn jobs(batch: usize, docs_per_s: u32, lease: Duration) -> RunnerOptions {
    RunnerOptions {
        jobs: JobOptions {
            batch,
            docs_per_s,
            lease,
        },
        ..RunnerOptions::default()
    }
}

fn request(schema: Schema, expected: Option<Ulid>) -> DeployRequest {
    DeployRequest {
        bundle: BUNDLE.as_bytes().to_vec(),
        schema,
        expected_active: expected,
        message: "test".into(),
        dry_run: false,
    }
}

async fn deploy(app: &AppEngine, schema: Schema, expected: Option<Ulid>) -> DeployOutcome {
    deploy::deploy(app, request(schema, expected), "alice")
        .await
        .expect("the deploy")
}

/// `{ from, n }`: inserts `{ n: from + k }` into `notes` for each `k <
/// n`, in a scrambled order of `n` values.
struct InsertMany;

impl Function for InsertMany {
    fn name(&self) -> &str {
        "test:insert_many"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let LiveValue::Object(args) = args else {
                panic!("an object")
            };
            let (LiveValue::I64(from), LiveValue::I64(n)) = (&args["from"], &args["n"]) else {
                panic!("from and n")
            };
            for k in 0..*n {
                // Scrambled: index order is not insertion order.
                let value = from + (k * 7_919) % n;
                txn.insert("notes", BTreeMap::from([("n".to_string(), i(value))]))
                    .await?;
            }
            Ok(LiveValue::Null)
        })
    }
}

/// Inserts `n` documents `{ n: from.. }` into `notes`, 250 per mutation.
async fn insert_many(app: &AppEngine, from: i64, n: i64) {
    let mut done = 0;
    while done < n {
        let chunk = (n - done).min(250);
        app.runner()
            .mutate(
                Arc::new(InsertMany),
                obj(&[("from", i(from + done)), ("n", i(chunk))]),
                None,
            )
            .await
            .expect("the inserts commit");
        done += chunk;
    }
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

/// The `n` of every document `_system:query` returns from `notes` by
/// `index`, in index order.
async fn by_index(app: &AppEngine, index: &str) -> Result<Vec<i64>, LiveError> {
    let LiveValue::Array(docs) = query(
        app,
        QUERY,
        obj(&[("table", s("notes")), ("index", s(index))]),
    )
    .await?
    else {
        panic!("an array")
    };
    Ok(docs
        .into_iter()
        .map(|d| match d {
            LiveValue::Object(f) => match f.get("n") {
                Some(LiveValue::I64(n)) => *n,
                other => panic!("n is an int, not {other:?}"),
            },
            other => panic!("a document, not {other:?}"),
        })
        .collect())
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

/// The id of `notes`' index `name` in any state.
async fn index_id(app: &AppEngine, name: &str) -> loams_live::IndexId {
    table(app, "notes")
        .await
        .indexes
        .iter()
        .find(|i| i.name == name)
        .unwrap_or_else(|| panic!("no index {name}"))
        .id
}

/// Every key under `prefix`, read now.
async fn keys_under(app: &AppEngine, prefix: Vec<u8>) -> BTreeSet<Vec<u8>> {
    let hi = loams_kv::tuple::successor(&prefix);
    app.runner()
        .store()
        .run(TxnOptions::new("test.keys"), move |txn| {
            let (lo, hi) = (prefix.clone(), hi.clone());
            Box::pin(async move { txn.scan(&lo, Some(&hi), usize::MAX).await })
        })
        .await
        .expect("a scan")
        .value
        .into_iter()
        .map(|(k, _)| k)
        .collect()
}

/// The entries of `notes`' index `index` as stored, and as a rebuild from
/// the documents gives them, read now.
async fn entries_and_rebuild(
    app: &AppEngine,
    index: loams_live::IndexId,
) -> (BTreeSet<Vec<u8>>, BTreeSet<Vec<u8>>) {
    let keys = AppKeys::dedicated();
    let notes = table(app, "notes").await;
    let prefix = keys.index_prefix(notes.id, index);
    let stored = keys_under(app, prefix.clone()).await;
    let docs_range = keys.documents(notes.id);
    let pairs = app
        .runner()
        .store()
        .run(TxnOptions::new("test.docs"), move |txn| {
            let (lo, hi) = (docs_range.lo.clone(), docs_range.hi.clone());
            Box::pin(async move { txn.scan(&lo, Some(&hi), usize::MAX).await })
        })
        .await
        .expect("a scan")
        .value;
    let mut rebuilt = BTreeSet::new();
    for (key, value) in pairs {
        let id = keys
            .doc_id_of_document(notes.id, &key)
            .expect("a document key");
        let doc = Doc::from_record(
            id,
            <pb::DocumentRecord as buffa::Message>::decode_from_slice(&value).expect("a record"),
        )
        .expect("a document");
        for entry in docs::index_keys(&keys, &notes, &doc, &Default::default()).expect("keys") {
            if entry.starts_with(&prefix) {
                rebuilt.insert(entry);
            }
        }
    }
    (stored, rebuilt)
}

/// Asserts `notes`' index `name` holds exactly the entries of its
/// documents, and returns how many.
async fn assert_complete(app: &AppEngine, name: &str) -> usize {
    let (stored, rebuilt) = entries_and_rebuild(app, index_id(app, name).await).await;
    assert_eq!(
        stored.len(),
        rebuilt.len(),
        "index {name}: {} entries stored, {} documents",
        stored.len(),
        rebuilt.len()
    );
    assert_eq!(stored, rebuilt, "index {name} equals a rebuild");
    stored.len()
}

/// `{ n }` (first call only): inserts `{ n }` into `notes`, so the table's
/// record is read, then tells the test it has started and waits for its
/// go. Counts its calls.
struct PausedInsert {
    started: Notify,
    go: Notify,
    calls: AtomicUsize,
}

impl PausedInsert {
    fn new() -> Arc<Self> {
        Arc::new(PausedInsert {
            started: Notify::new(),
            go: Notify::new(),
            calls: AtomicUsize::new(0),
        })
    }
}

impl Function for PausedInsert {
    fn name(&self) -> &str {
        "test:paused_insert"
    }
    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }
    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let LiveValue::Object(args) = args else {
                panic!("an object")
            };
            txn.insert(
                "notes",
                BTreeMap::from([("n".to_string(), args["n"].clone())]),
            )
            .await?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.go.notified().await;
            }
            Ok(LiveValue::Null)
        })
    }
}

/// Row T9-1 (replaces `mutation_older_than_lifetime_refuses_to_prewrite`):
/// a mutation that read `notes`' record before the deploy that adds an
/// index, and commits after the backfill has passed its document, reruns
/// past the deploy's fence and writes the new index's entry itself. A
/// lifetime wait is not needed.
async fn index_change_fence_reruns_mutations_in_flight(store: TestStore) {
    let app = node(&store).await;
    let v1 = deploy(&app, notes(&[]), None).await;
    insert_many(&app, 0, 50).await;
    let f = PausedInsert::new();
    let in_flight = {
        let (runner, f) = (app.runner().clone(), f.clone());
        tokio::spawn(async move { runner.mutate(f, obj(&[("n", i(999))]), None).await })
    };
    f.started.notified().await;
    let v2 = deploy(&app, notes(&[("by_n", &["n"])]), Some(v1.record.id)).await;
    assert_eq!(v2.changes.indexes_added, [change("by_n")]);
    f.go.notify_one();
    let m = in_flight
        .await
        .expect("the task")
        .expect("the mutation commits");
    assert_eq!(m.attempts, 2, "the fence made the mutation rerun");
    assert_eq!(assert_complete(&app, "by_n").await, 51);
    assert_eq!(by_index(&app, "by_n").await.expect("by_n").len(), 51);
}
live_test!(index_change_fence_reruns_mutations_in_flight);

/// An index added to a table of 10 000 documents is backfilled: afterwards
/// a query on it returns every document in index order, it equals a
/// rebuild, and its status is ready with every document done.
async fn index_add_on_non_empty_table_backfills(store: TestStore) {
    let app = node_with(&store, jobs(512, 50_000, Duration::from_secs(30))).await;
    let v1 = deploy(&app, notes(&[]), None).await;
    insert_many(&app, 0, 10_000).await;
    let dry = deploy::deploy(
        &app,
        DeployRequest {
            dry_run: true,
            ..request(notes(&[("by_n", &["n"])]), Some(v1.record.id))
        },
        "alice",
    )
    .await
    .expect("a dry run");
    assert_eq!(dry.changes.indexes_added, [change("by_n")]);
    assert!(
        matches!(by_index(&app, "by_n").await, Err(LiveError::NotFound(_))),
        "a dry run adds nothing"
    );
    let v2 = deploy(&app, notes(&[("by_n", &["n"])]), Some(v1.record.id)).await;
    assert_eq!(v2.changes.indexes_added, [change("by_n")]);
    assert!(v2.changes.indexes_dropped.is_empty());
    let ns = by_index(&app, "by_n").await.expect("by_n is ready");
    assert_eq!(
        ns,
        (0..10_000).collect::<Vec<_>>(),
        "every document, in order"
    );
    assert_eq!(assert_complete(&app, "by_n").await, 10_000);
    let status = backfill::index_status(app.runner(), "notes", "by_n")
        .await
        .expect("a status");
    assert_eq!(status.state, IndexState::Ready);
    assert_eq!(status.done, 10_000);
    assert_eq!(status.total_estimate, 10_000);
    let tables = query(&app, TABLES, obj(&[])).await.expect("_system:tables");
    assert!(format!("{tables:?}").contains("by_n"), "{tables:?}");
}
live_test!(index_add_on_non_empty_table_backfills);

/// Inserts and patches run throughout a slow backfill: afterwards the
/// index equals a rebuild from the documents, and a query on it returns
/// them all in order.
async fn concurrent_inserts_during_backfill_are_indexed(store: TestStore) {
    let app = node_with(&store, jobs(64, 2_000, Duration::from_secs(30))).await;
    let v1 = deploy(&app, notes(&[]), None).await;
    insert_many(&app, 0, 1_500).await;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (app, stop) = (app.clone(), stop.clone());
        tokio::spawn(async move {
            let mut ids = Vec::new();
            let mut k = 0_i64;
            while !stop.load(Ordering::SeqCst) {
                let id = mutate(
                    &app,
                    INSERT,
                    obj(&[("table", s("notes")), ("fields", obj(&[("n", i(-1 - k))]))]),
                )
                .await
                .expect("an insert");
                ids.push(id);
                // Patch an earlier document to a new value: its old entry
                // goes, its new one comes.
                let target = ids[(k as usize * 31) % ids.len()].clone();
                mutate(
                    &app,
                    PATCH,
                    obj(&[("id", target), ("fields", obj(&[("n", i(100_000 + k))]))]),
                )
                .await
                .expect("a patch");
                k += 1;
            }
            k
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    deploy(&app, notes(&[("by_n", &["n"])]), Some(v1.record.id)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.store(true, Ordering::SeqCst);
    let written = writer.await.expect("the writer");
    assert!(written > 0, "the writer ran during the backfill");
    let total = assert_complete(&app, "by_n").await;
    assert_eq!(total, 1_500 + written as usize);
    let ns = by_index(&app, "by_n").await.expect("by_n");
    let mut sorted = ns.clone();
    sorted.sort_unstable();
    assert_eq!(ns, sorted, "index order");
    assert_eq!(ns.len(), total);
}
live_test!(concurrent_inserts_during_backfill_are_indexed);

/// While an index backfills, a query on it is `FAILED_PRECONDITION`,
/// reason `live_index_not_ready`, and its status says so; writes already
/// maintain it.
async fn query_on_backfilling_index_is_failed_precondition(store: TestStore) {
    let app = node(&store).await;
    let v1 = deploy(&app, notes(&[]), None).await;
    insert_many(&app, 0, 10).await;
    app.crash_after_next_fence();
    let err = deploy::deploy(
        &app,
        request(notes(&[("by_n", &["n"])]), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("the node dies after the fence");
    assert!(matches!(err, LiveError::Internal(_)), "{err}");
    let err = by_index(&app, "by_n").await.expect_err("not ready");
    assert_eq!(err.reason(), Some("live_index_not_ready"), "{err}");
    assert_eq!(
        err.code(),
        pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION,
        "{err}"
    );
    assert!(err.to_string().contains("by_n"), "{err}");
    let status = backfill::index_status(app.runner(), "notes", "by_n")
        .await
        .expect("a status");
    assert!(
        matches!(status.state, IndexState::Backfilling { .. }),
        "{status:?}"
    );
    assert_eq!(status.done, 0);
    assert_eq!(status.total_estimate, 10);
    let tables = query(&app, TABLES, obj(&[])).await.expect("_system:tables");
    assert!(!format!("{tables:?}").contains("by_n"), "{tables:?}");

    // Writes maintain it: a new document has its entry already.
    mutate(
        &app,
        INSERT,
        obj(&[("table", s("notes")), ("fields", obj(&[("n", i(77))]))]),
    )
    .await
    .expect("an insert");
    let (stored, _) = entries_and_rebuild(&app, index_id(&app, "by_n").await).await;
    assert_eq!(stored.len(), 1, "the insert's own entry");
}
live_test!(query_on_backfilling_index_is_failed_precondition);

/// Fails every batch after `fail_after` batches.
struct FailAfter {
    inner: IndexBackfill,
    batches: Mutex<usize>,
    fail_after: Option<usize>,
}

impl Visitor for FailAfter {
    fn visit<'a>(
        &'a self,
        txn: &'a mut Txn,
        app: &'a AppKeys,
        docs: &'a [Doc],
    ) -> BoxFuture<'a, Result<Vec<Finding>, LiveError>> {
        Box::pin(async move {
            {
                let mut n = self.batches.lock().expect("unpoisoned");
                *n += 1;
                if self.fail_after.is_some_and(|f| *n > f) {
                    return Err(LiveError::Internal("the node died mid-backfill".into()));
                }
            }
            self.inner.visit(txn, app, docs).await
        })
    }
}

/// The leases of [`backfill_resumes_after_crash`].
const LEASE: Duration = Duration::from_secs(3);

/// A backfill whose node dies mid-way is resumed by another owner once the
/// lease expires, from the checkpoint: every document is indexed once, and
/// a redeploy of the schema then activates it.
async fn backfill_resumes_after_crash(store: TestStore) {
    let app = node_with(&store, jobs(4, 10_000, LEASE)).await;
    let v1 = deploy(&app, notes(&[]), None).await;
    insert_many(&app, 0, 30).await;
    app.crash_after_next_fence();
    deploy::deploy(
        &app,
        request(notes(&[("by_n", &["n"])]), Some(v1.record.id)),
        "alice",
    )
    .await
    .expect_err("the node dies after the fence");
    let notes_id = table(&app, "notes").await.id;
    let IndexState::Backfilling { deployment } = table(&app, "notes")
        .await
        .indexes
        .iter()
        .find(|i| i.name == "by_n")
        .expect("by_n")
        .state
    else {
        panic!("by_n backfills")
    };
    let visitor = |fail_after| {
        Arc::new(FailAfter {
            inner: IndexBackfill::new(deployment, app.runner().limits().clone()),
            batches: Mutex::new(0),
            fail_after,
        })
    };

    // The dead deploy's lease (taken over once it expires; Task 8's
    // `table_job_resumes_from_its_checkpoint` covers a live one).
    tokio::time::sleep(LEASE + Duration::from_millis(200)).await;
    let first = TableJob::new(app.runner().clone(), JobKind::Backfill, notes_id, "first");
    first
        .run(visitor(Some(3)), usize::MAX)
        .await
        .expect_err("the first owner dies on its fourth batch");
    let (stored, _) = entries_and_rebuild(&app, index_id(&app, "by_n").await).await;
    assert_eq!(stored.len(), 12, "three batches of four");

    let second = TableJob::new(app.runner().clone(), JobKind::Backfill, notes_id, "second");
    second
        .run(visitor(None), usize::MAX)
        .await
        .expect_err("the first owner's lease lives");
    tokio::time::sleep(LEASE + Duration::from_millis(200)).await;
    let done = second
        .run(visitor(None), usize::MAX)
        .await
        .expect("the second owner resumes");
    assert!(done.done);
    assert_eq!(done.owner, "second");
    assert_eq!(done.scanned, 30, "from the checkpoint: no document twice");
    assert_eq!(assert_complete(&app, "by_n").await, 30, "and none missed");

    // A redeploy takes the backfilled index over and activates it.
    tokio::time::sleep(LEASE + Duration::from_millis(200)).await;
    deploy(&app, notes(&[("by_n", &["n"])]), Some(v1.record.id)).await;
    assert_eq!(
        by_index(&app, "by_n").await.expect("ready"),
        (0..30).collect::<Vec<_>>()
    );
}
live_test!(backfill_resumes_after_crash);

/// A deploy that leaves an index out drops it: queries no longer find it,
/// a mutation in flight across the swap leaves no entry of it (it reruns
/// past the drop's fence, or commits before it and its entry is deleted),
/// and its entries are deleted in batches before its definition goes. (The
/// plan's "wait" is the fence, row T9-1.) The other index is untouched.
async fn drop_index_deletes_entries_after_wait(store: TestStore) {
    let app = node_with(&store, jobs(16, 50_000, Duration::from_secs(30))).await;
    let both = notes(&[("by_n", &["n"]), ("by_m", &["m", "n"])]);
    let v1 = deploy(&app, both, None).await;
    insert_many(&app, 0, 100).await;
    let by_m = index_id(&app, "by_m").await;
    let notes_id = table(&app, "notes").await.id;
    let prefix = AppKeys::dedicated().index_prefix(notes_id, by_m);
    assert_eq!(keys_under(&app, prefix.clone()).await.len(), 100);

    let f = PausedInsert::new();
    let in_flight = {
        let (runner, f) = (app.runner().clone(), f.clone());
        tokio::spawn(async move { runner.mutate(f, obj(&[("n", i(500))]), None).await })
    };
    f.started.notified().await;
    let v2 = deploy(&app, notes(&[("by_n", &["n"])]), Some(v1.record.id)).await;
    assert_eq!(v2.changes.indexes_dropped, [change("by_m")]);
    assert!(v2.changes.indexes_added.is_empty());
    let err = by_index(&app, "by_m").await.expect_err("dropped");
    assert!(matches!(err, LiveError::NotFound(_)), "{err}");
    f.go.notify_one();
    in_flight.await.expect("the task").expect("commits");

    backfill::drop_indexes(app.runner())
        .await
        .expect("the dropped index's entries are deleted");
    assert!(
        keys_under(&app, prefix).await.is_empty(),
        "no entry of the dropped index is left"
    );
    let def = table(&app, "notes").await;
    assert!(
        def.indexes.iter().all(|i| i.name != "by_m"),
        "{:?}",
        def.indexes
    );
    assert_eq!(assert_complete(&app, "by_n").await, 101);
    let again = backfill::drop_indexes(app.runner())
        .await
        .expect("idempotent");
    assert!(again.is_empty());
    let tables = query(&app, TABLES, obj(&[])).await.expect("_system:tables");
    assert!(!format!("{tables:?}").contains("by_m"), "{tables:?}");
}
live_test!(drop_index_deletes_entries_after_wait);

/// The reactive checker (Task 1) passes with indexes added and dropped by
/// deploys in the middle of the workload; its end check finds every ready
/// index equal to a rebuild.
async fn checker_passes_with_index_changes_mid_workload(store: TestStore) {
    let w = Workload {
        seed: 9,
        sessions: 3,
        tables: 2,
        ops: 600,
        disturb: vec![
            Disturbance::AddIndex { at_op: 100 },
            Disturbance::DropIndex { at_op: 300 },
            Disturbance::AddIndex { at_op: 450 },
        ],
    };
    let report = run_reactive_checker(store.store(), w).await;
    assert!(
        report.violations.is_empty(),
        "{} violations: {:?}",
        report.violations.len(),
        report
            .violations
            .iter()
            .take(3)
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
    );
    assert!(
        report
            .violations
            .iter()
            .all(|v| v.kind != ViolationKind::Unwired)
    );
    assert_eq!(report.activations, 3, "each index change is a deploy");
}
live_test!(checker_passes_with_index_changes_mid_workload);

/// R1's quiesce gate (row T9-1 of R1, `Runner::try_quiesce`) is gone: an
/// index change no longer needs mutations to stop. Checked on the source
/// of the runner, since an absent method cannot be called.
#[test]
fn try_quiesce_is_gone() {
    let runner = include_str!("../src/txn.rs");
    for gone in ["fn try_quiesce", "struct Quiesced", "gate.read()"] {
        assert!(!runner.contains(gone), "`{gone}` is still in txn.rs");
    }
}
