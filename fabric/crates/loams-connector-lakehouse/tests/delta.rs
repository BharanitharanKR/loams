//! Delta Lake (OR-18): the exactly-once sink under crashes, and the source's
//! versions and change data feed.
//!
//! Every test runs on a local directory (a real Delta table on the file
//! system, never counted toward a suite, OR-2) and again on S3 storage when
//! a job provides it:
//!
//! | Variable | System |
//! |---|---|
//! | `LOAMS_CONNECTORS_P2_STACK` + `LOAMS_RUSTFS_{ENDPOINT,BUCKET,ACCESS_KEY,SECRET_KEY}` | RustFS (S3) |
//! | `LOAMS_DELTA_TABLE_URI` (an `s3://` prefix) + `LOAMS_AWS_{ACCESS_KEY_ID,SECRET_ACCESS_KEY,REGION}` | Amazon S3, `connectors-nightly` |
//!
//! Without them the real halves print why they skipped.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, AddCDCFile, Remove};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use loams_connector_lakehouse::delta::{
    DeltaConfig, DeltaLake, DeltaSink, DeltaSinkConfig, DeltaSinkFactory, DeltaSource,
    DeltaSourceConfig, DeltaTarget, SourceMode, Start, TABLE_SPLIT, app_id,
};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{
    AllowAll, Budget, Checkpoint, CloudEvent, ConnectorContext, ConnectorFactory, EventBatch,
    InstanceKey, Pipeline, PipelineConfig, Position, Scrubber, Sink, Source, Split, SplitId,
    StaticSecrets, TransactionalSink,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

const GROUP: &str = "flow-delta";

fn instance() -> InstanceKey {
    InstanceKey::new(7, "orders")
}

fn small_config() -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: 10,
        max_batch_bytes: 256 << 10,
        linger: Duration::from_millis(5),
        max_in_flight_batches: 2,
        memory_budget_bytes: 4 << 20,
        checkpoint_interval: Duration::ZERO,
        ..PipelineConfig::default()
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn event(i: u64) -> CloudEvent {
    CloudEvent::new(
        format!("e/{i}"),
        "/test",
        "io.loams.dev.flow.test.record.v1",
    )
    .with_data(
        "application/json",
        format!("{{\"i\":{i},\"name\":\"n{i}\"}}"),
    )
}

fn push_events(fabric: &MemoryFabric, from: u64, n: u64) {
    for i in from..from + n {
        fabric.push(event(i));
    }
}

/// A local-directory table: `file:///…/<name>`.
fn local_target(dir: &tempfile::TempDir, name: &str) -> DeltaTarget {
    let path = dir.path().join(name);
    std::fs::create_dir_all(&path).expect("table dir");
    let url = Url::from_directory_path(&path).expect("file url");
    DeltaTarget::new(url, Arc::new(object_store::local::LocalFileSystem::new()))
}

fn env(v: &str) -> String {
    std::env::var(v).unwrap_or_default()
}

/// A table on RustFS when the job provides it.
async fn rustfs_target(test: &str) -> Option<DeltaTarget> {
    let vars = [
        "LOAMS_CONNECTORS_P2_STACK",
        "LOAMS_RUSTFS_ENDPOINT",
        "LOAMS_RUSTFS_BUCKET",
        "LOAMS_RUSTFS_ACCESS_KEY",
        "LOAMS_RUSTFS_SECRET_KEY",
    ];
    let missing: Vec<&str> = vars.iter().copied().filter(|v| env(v).is_empty()).collect();
    if !missing.is_empty() {
        eprintln!(
            "skipping the RustFS half of {test}: {} not set",
            missing.join(", ")
        );
        return None;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let endpoint = env("LOAMS_RUSTFS_ENDPOINT");
    let cfg = DeltaConfig::parse(&serde_json::json!({
        "table_uri": format!("s3://{}/loams-tests/{test}/{nanos}/t", env("LOAMS_RUSTFS_BUCKET")),
        "storage": {
            "endpoint": endpoint,
            "allow_http": endpoint.starts_with("http://"),
            "region": "us-east-1",
            "auth": {"access_key_id": "key", "secret_access_key": "secret"}
        }
    }))
    .expect("config");
    let secrets = StaticSecrets::new()
        .with("key", env("LOAMS_RUSTFS_ACCESS_KEY"))
        .with("secret", env("LOAMS_RUSTFS_SECRET_KEY"));
    Some(
        DeltaTarget::from_config(&cfg, &secrets, Arc::new(AllowAll))
            .await
            .expect("RustFS target"),
    )
}

async fn start_sink(
    fabric: &MemoryFabric,
    target: &DeltaTarget,
    epoch: u64,
) -> loams_connector_sdk::PipelineHandle {
    Pipeline::sink(instance(), small_config())
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::new(DeltaSinkFactory::new(
            target.clone(),
            DeltaSinkConfig::default(),
            instance(),
        )))
        .dead_letters(Arc::new(fabric.clone()))
        .epoch(epoch)
        .start()
        .await
        .expect("sink pipeline starts")
}

/// Every row the table holds, read with the family's own source from
/// version 0 (`rows` mode): the `i` column, in read order.
async fn read_all(target: &DeltaTarget, mode: SourceMode) -> Vec<RecordBatch> {
    let mut src = DeltaSource::new(
        target.clone(),
        DeltaSourceConfig {
            mode,
            start: Start::Earliest,
            poll_interval_ms: 0,
            ..DeltaSourceConfig::default()
        },
        instance(),
    );
    src.open(&Split::new(TABLE_SPLIT, serde_json::json!({})), None)
        .await
        .expect("open");
    let mut out = Vec::new();
    let budget = Budget {
        max_rows: 1000,
        max_bytes: 8 << 20,
    };
    while let Some(b) = src.next(budget).await.expect("next") {
        match b.events {
            EventBatch::Arrow { batch, .. } => out.push(batch),
            EventBatch::Events(_) => panic!("the Delta source emits Arrow batches"),
        }
    }
    out
}

fn column_i(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column_by_name("i").expect("column i");
        let col = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i is Int64");
        out.extend(col.iter().map(|v| v.expect("i is set")));
    }
    out
}

/// The sink crashes mid-stream twice (once before a Fabric commit, so the
/// advisory offset lags), and successors with newer epochs resume: every
/// event lands exactly once.
async fn crash_resume(target: DeltaTarget) {
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 60);
    let first = start_sink(&fabric, &target, 1).await;
    eventually("first run is part way", || fabric.committed(GROUP) >= 20).await;
    first.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The Fabric's offset is advisory: make it lag well behind the table.
    fabric.set_committed(GROUP, 5);

    push_events(&fabric, 60, 40);
    let second = start_sink(&fabric, &target, 2).await;
    eventually("second run is part way", || fabric.committed(GROUP) >= 70).await;
    second.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let third = start_sink(&fabric, &target, 3).await;
    eventually("all delivered", || fabric.committed(GROUP) >= 100).await;
    third.stop().await;

    let got = column_i(&read_all(&target, SourceMode::Versions).await);
    let distinct: BTreeSet<i64> = got.iter().copied().collect();
    assert_eq!(got.len(), 100, "every event exactly once: {got:?}");
    assert_eq!(distinct, (0..100).collect::<BTreeSet<_>>());
}

/// The two-phase protocol by hand: a commit is idempotent, a prepared but
/// uncommitted batch is invisible after a crash, and `recover` returns the
/// checkpoint of the last commit.
async fn two_phase_by_hand(target: DeltaTarget) {
    let cp = |offset: u64, epoch: u64| {
        let mut c = Checkpoint::new(instance(), epoch);
        c.splits
            .insert(SplitId::new("fabric"), Position::from_u64(offset));
        c
    };
    let batch = |from: u64, n: u64| EventBatch::Events((from..from + n).map(event).collect());

    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    assert_eq!(s.recover().await.expect("recover"), None);
    s.write(batch(0, 5)).await.expect("write");
    let t1 = s.prepare(&cp(5, 1)).await.expect("prepare");
    s.commit(t1.clone()).await.expect("commit");
    // A retried commit (the first one landed, its reply was lost).
    s.commit(t1).await.expect("commit again");

    s.write(batch(5, 5)).await.expect("write");
    let _t2 = s.prepare(&cp(10, 1)).await.expect("prepare");
    drop(s); // crash after prepare

    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    assert_eq!(s.recover().await.expect("recover"), Some(cp(5, 1)));
    let got = column_i(&read_all(&target, SourceMode::Versions).await);
    assert_eq!(got, vec![0, 1, 2, 3, 4], "the prepared batch is invisible");

    s.write(batch(5, 5)).await.expect("write");
    let t3 = s.prepare(&cp(10, 2)).await.expect("prepare");
    s.commit(t3).await.expect("commit");
    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    assert_eq!(s.recover().await.expect("recover"), Some(cp(10, 2)));
    assert_eq!(
        column_i(&read_all(&target, SourceMode::Versions).await).len(),
        10
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_txn_exactly_once_under_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    two_phase_by_hand(local_target(&dir, "by-hand")).await;
    crash_resume(local_target(&dir, "crash")).await;
    if let Some(t) = rustfs_target("delta-by-hand").await {
        two_phase_by_hand(t).await;
    }
    if let Some(t) = rustfs_target("delta-crash").await {
        crash_resume(t).await;
    }
    if let Some(t) = nightly_target("delta-by-hand").await {
        two_phase_by_hand(t).await;
    }
    if let Some(t) = nightly_target("delta-crash").await {
        crash_resume(t).await;
    }
}

/// A table under the nightly job's S3 prefix (repository secrets).
async fn nightly_target(test: &str) -> Option<DeltaTarget> {
    let vars = [
        "LOAMS_DELTA_TABLE_URI",
        "LOAMS_AWS_ACCESS_KEY_ID",
        "LOAMS_AWS_SECRET_ACCESS_KEY",
        "LOAMS_AWS_REGION",
    ];
    let missing: Vec<&str> = vars.iter().copied().filter(|v| env(v).is_empty()).collect();
    if !missing.is_empty() {
        eprintln!(
            "skipping the S3 nightly half of {test}: {} not set",
            missing.join(", ")
        );
        return None;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let base = env("LOAMS_DELTA_TABLE_URI");
    let cfg = DeltaConfig::parse(&serde_json::json!({
        "table_uri": format!("{}/{test}/{nanos}", base.trim_end_matches('/')),
        "storage": {
            "region": env("LOAMS_AWS_REGION"),
            "auth": {"access_key_id": "key", "secret_access_key": "secret"}
        }
    }))
    .expect("config");
    let secrets = StaticSecrets::new()
        .with("key", env("LOAMS_AWS_ACCESS_KEY_ID"))
        .with("secret", env("LOAMS_AWS_SECRET_ACCESS_KEY"));
    Some(
        DeltaTarget::from_config(&cfg, &secrets, Arc::new(AllowAll))
            .await
            .expect("S3 target"),
    )
}

/// The table's `txn` action names the instance and the last sequence, as
/// other Delta writers and readers see it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_txn_action_is_visible_to_delta_readers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = local_target(&dir, "txn");
    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    s.recover().await.expect("recover");
    for n in 0..3u64 {
        s.write(EventBatch::Events(vec![event(n)]))
            .await
            .expect("write");
        let mut c = Checkpoint::new(instance(), 1);
        c.splits
            .insert(SplitId::new("fabric"), Position::from_u64(n + 1));
        let t = s.prepare(&c).await.expect("prepare");
        s.commit(t).await.expect("commit");
    }
    let table = target.load().await.expect("load").expect("table exists");
    let v = table
        .snapshot()
        .expect("snapshot")
        .transaction_version(&*table.log_store(), app_id(&instance()))
        .await
        .expect("txn version");
    assert_eq!(v, Some(3));
}

/// Write a Parquet file under the table and return its size.
async fn put_parquet(store: &dyn ObjectStore, path: &str, batch: &RecordBatch) -> i64 {
    let mut buf = Vec::new();
    {
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None)
            .expect("parquet writer");
        w.write(batch).expect("write");
        w.close().expect("close");
    }
    let size = buf.len() as i64;
    store
        .put(&Path::from(path), Bytes::from(buf).into())
        .await
        .expect("put");
    size
}

/// The change data feed by the Delta protocol's rules: a version's `cdc`
/// files when it has them (`update_preimage`, `update_postimage`,
/// `delete`), else its adds as `insert` and its data-changing removes as
/// `delete`. The `cdc` commit is written with deltalake's own action types,
/// as a CDF writer (Spark, delta-rs MERGE) writes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_cdf_source_ops() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = local_target(&dir, "cdf");
    // Version 1 (after the create at 0): rows 0..3 through the sink.
    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    s.recover().await.expect("recover");
    s.write(EventBatch::Events((0..3).map(event).collect()))
        .await
        .expect("write");
    let t = s
        .prepare(&Checkpoint::new(instance(), 1))
        .await
        .expect("prepare");
    s.commit(t).await.expect("commit");

    let table = target.load().await.expect("load").expect("table");
    let store = table.object_store();
    let data_file = table
        .snapshot()
        .expect("snapshot")
        .log_data()
        .into_iter()
        .next()
        .expect("one data file")
        .object_store_path();

    // Version 2: an update of row 1 and a delete of row 2, with cdc files.
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("_change_type", DataType::Utf8, false),
    ]));
    let cdc = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec!["n1", "renamed", "n2"])),
            Arc::new(StringArray::from(vec![
                "update_preimage",
                "update_postimage",
                "delete",
            ])),
        ],
    )
    .expect("cdc batch");
    let size = put_parquet(&*store, "_change_data/cdc-00002.parquet", &cdc).await;
    let snapshot = table.snapshot().expect("snapshot");
    CommitBuilder::from(CommitProperties::default())
        .with_actions(vec![Action::Cdc(AddCDCFile {
            path: "_change_data/cdc-00002.parquet".to_owned(),
            size,
            partition_values: Default::default(),
            data_change: false,
            tags: None,
        })])
        .build(
            Some(snapshot),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .expect("cdc commit");

    // Version 3: the first data file removed with no cdc files (a writer
    // without CDF): its rows read as deletes.
    let mut table = target.load().await.expect("load").expect("table");
    table.update_state().await.expect("refresh");
    let snapshot = table.snapshot().expect("snapshot");
    CommitBuilder::from(CommitProperties::default())
        .with_actions(vec![Action::Remove(Remove {
            path: data_file.to_string(),
            data_change: true,
            deletion_timestamp: Some(0),
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        })])
        .build(
            Some(snapshot),
            table.log_store(),
            DeltaOperation::Delete { predicate: None },
        )
        .await
        .expect("remove commit");

    let batches = read_all(&target, SourceMode::Cdf).await;
    let mut ops = Vec::new();
    let mut versions = Vec::new();
    for b in &batches {
        let ct = b
            .column_by_name("_change_type")
            .expect("_change_type")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .clone();
        let cv = b
            .column_by_name("_commit_version")
            .expect("_commit_version")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .clone();
        assert!(b.column_by_name("_commit_timestamp").is_some());
        for r in 0..b.num_rows() {
            ops.push(ct.value(r).to_owned());
            versions.push(cv.value(r));
        }
    }
    assert_eq!(
        ops,
        [
            "insert",
            "insert",
            "insert",
            "update_preimage",
            "update_postimage",
            "delete",
            "delete",
            "delete",
            "delete"
        ]
    );
    assert_eq!(versions, [1, 1, 1, 2, 2, 2, 3, 3, 3]);
}

/// Ids derive from the version, file and rows: a re-read yields the same
/// ids, and a resumed source continues after its position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_source_ids_stable_and_resumable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = local_target(&dir, "resume");
    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    s.recover().await.expect("recover");
    for chunk in 0..3u64 {
        s.write(EventBatch::Events(
            (chunk * 10..chunk * 10 + 10).map(event).collect(),
        ))
        .await
        .expect("write");
        let t = s
            .prepare(&Checkpoint::new(instance(), 1))
            .await
            .expect("prepare");
        s.commit(t).await.expect("commit");
    }
    let cfg = DeltaSourceConfig {
        start: Start::Earliest,
        poll_interval_ms: 0,
        ..DeltaSourceConfig::default()
    };
    let budget = Budget {
        max_rows: 4,
        max_bytes: 1 << 20,
    };
    let split = Split::new(TABLE_SPLIT, serde_json::json!({}));
    let read = |from: Option<Position>, n: usize| {
        let target = target.clone();
        let cfg = cfg.clone();
        let split = split.clone();
        async move {
            let mut src = DeltaSource::new(target, cfg, instance());
            src.open(&split, from).await.expect("open");
            let mut out = Vec::new();
            while out.len() < n {
                match src.next(budget).await.expect("next") {
                    Some(b) => out.push(b),
                    None => break,
                }
            }
            out
        }
    };
    let all = read(None, usize::MAX).await;
    let again = read(None, usize::MAX).await;
    let ids = |v: &[loams_connector_sdk::SourceBatch]| -> Vec<String> {
        v.iter()
            .map(|b| match &b.events {
                EventBatch::Arrow { envelope, .. } => envelope.id.clone(),
                EventBatch::Events(_) => unreachable!(),
            })
            .collect()
    };
    assert_eq!(ids(&all), ids(&again));
    assert!(all.iter().all(|b| b.events.rows() <= 4));
    let rows: usize = all.iter().map(|b| b.events.rows()).sum();
    assert_eq!(rows, 30);
    // Resume from the third batch's position: the rest follows exactly.
    let rest = read(Some(all[2].position.clone()), usize::MAX).await;
    assert_eq!(ids(&rest), ids(&all[3..]));
    match &all[0].events {
        EventBatch::Arrow { envelope, .. } => {
            assert_eq!(envelope.ty, "io.loams.dev.flow.delta-lake.rows.v1");
            assert_eq!(
                envelope.datacontenttype.as_deref(),
                Some("application/vnd.apache.arrow.stream")
            );
        }
        EventBatch::Events(_) => unreachable!(),
    }
}

/// A column the table lacks pauses the instance (drift), rather than being
/// dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_sink_unknown_column_is_schema_incompatible() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = local_target(&dir, "drift");
    let mut s = DeltaSink::new(target.clone(), DeltaSinkConfig::default(), instance());
    s.recover().await.expect("recover");
    s.write(EventBatch::Events(vec![event(0)]))
        .await
        .expect("write");
    let t = s
        .prepare(&Checkpoint::new(instance(), 1))
        .await
        .expect("prepare");
    s.commit(t).await.expect("commit");
    let extra = CloudEvent::new("e/x", "/test", "t")
        .with_data("application/json", r#"{"i":1,"name":"a","extra":true}"#);
    let err = s
        .write(EventBatch::Events(vec![extra]))
        .await
        .expect_err("drift refused");
    assert_eq!(
        err.class,
        loams_connector_sdk::ErrorClass::SchemaIncompatible
    );
}

/// The factory parses the instance config and builds both directions.
#[tokio::test]
async fn delta_factory_builds_both_directions() {
    let ctx = ConnectorContext::new(
        instance(),
        serde_json::json!({"table_uri": "memory:///t"}),
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    );
    assert_eq!(DeltaLake.id(), "delta-lake");
    assert!(DeltaLake.source(&ctx).expect("source").is_some());
    assert!(DeltaLake.sink(&ctx).expect("sink").is_some());
    let bad = ConnectorContext::new(
        instance(),
        serde_json::json!({"table_uri": "memory:///t", "bogus": 1}),
        Arc::new(StaticSecrets::new()),
        Scrubber::new(),
    );
    assert!(DeltaLake.sink(&bad).is_err());
}
