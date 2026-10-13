//! Helpers shared by the family's integration tests.
//!
//! Every test runs its logic on `object_store`'s `InMemory` store (or a
//! local directory for SFTP), which never counts toward a suite (OR-2), and
//! again against the real system when the CI job's environment names one:
//!
//! | Variable | System | Job |
//! |---|---|---|
//! | `LOAMS_CONNECTORS_P2_STACK` + `LOAMS_RUSTFS_ENDPOINT` | RustFS | `connectors-p2` (objects) |
//! | `LOAMS_CONNECTORS_P2_STACK` + `LOAMS_FAKE_GCS_ENDPOINT` | fake-gcs-server | `connectors-p2` |
//! | `LOAMS_CONNECTORS_P2_STACK` + `LOAMS_AZURITE_ENDPOINT` | Azurite | `connectors-p2` |
//! | `LOAMS_CONNECTORS_P2_STACK` + `LOAMS_SFTP_HOST` | `atmoz/sftp` | `connectors-p2` |
//! | `LOAMS_CONNECTORS_CI_ONLY_STACK` + `LOAMS_MINIO_ENDPOINT` | MinIO (`ci-service`, R0.28) | `connectors-ci-only` |
//! | `LOAMS_CONNECTORS_NIGHTLY` + `LOAMS_R2_*`, `LOAMS_GCS_*`, `LOAMS_AZURE_*`, `LOAMS_ADLS_*` | real accounts | `connectors-nightly` |
//!
//! Without them, the real-system half prints why it skipped; a skipped
//! suite never counts toward `preview`.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use futures::StreamExt;
use loams_connector_objects::config::ObjectsConfig;
use loams_connector_objects::config::{SinkConfig, SinkFormat};
use loams_connector_objects::sink::{
    ObjectSinkFactory, Publish, STAGING, SinkSettings, batch_to_events, parse_range,
};
use loams_connector_objects::store::{Backend, StoreHandle, build};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{
    AllowAll, CloudEvent, InstanceKey, Pipeline, PipelineConfig, PipelineHandle, StaticSecrets,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

pub const GROUP: &str = "flow-objects";

pub fn instance() -> InstanceKey {
    InstanceKey::new(7, "orders")
}

pub fn small_config() -> PipelineConfig {
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

pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !cond() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub fn push_events(fabric: &MemoryFabric, from: u64, n: u64) {
    for i in from..from + n {
        fabric.push(
            CloudEvent::new(
                format!("e/{i}"),
                "/test",
                "io.loams.dev.flow.test.record.v1",
            )
            .with_data("application/json", format!("{{\"i\":{i}}}")),
        );
    }
}

/// `true` when a real-system variable set is present; otherwise says why
/// the real half is skipped.
pub fn real(stack: &str, vars: &[&str], what: &str) -> bool {
    let missing: Vec<&str> = std::iter::once(stack)
        .chain(vars.iter().copied())
        .filter(|v| std::env::var(v).map_or(true, |s| s.is_empty()))
        .collect();
    if missing.is_empty() {
        true
    } else {
        eprintln!("skipping {what}: {} not set", missing.join(", "));
        false
    }
}

pub fn env(v: &str) -> String {
    std::env::var(v).unwrap_or_default()
}

/// A store for a real system, built through the family's own builder (the
/// path production takes), with credentials passed as secrets.
pub async fn real_store(
    backend: Backend,
    config: serde_json::Value,
    secrets: StaticSecrets,
) -> Arc<dyn ObjectStore> {
    let cfg = ObjectsConfig::parse(&config, None).expect("config");
    build(backend, &cfg, &secrets, Arc::new(AllowAll))
        .await
        .expect("store builds")
}

/// A unique prefix per test run, so reruns against a shared bucket do not
/// see each other.
pub fn run_prefix(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("loams-tests/{test}/{nanos}")
}

pub fn sink_config(format: SinkFormat, roll_bytes: u64, roll_interval_secs: u64) -> SinkConfig {
    SinkConfig {
        format,
        roll_bytes,
        roll_interval_secs,
        ..SinkConfig::default()
    }
}

pub async fn start_sink(
    fabric: &MemoryFabric,
    store: Arc<dyn ObjectStore>,
    sink: &SinkConfig,
    prefix: &str,
    epoch: u64,
) -> PipelineHandle {
    start_sink_with(fabric, store, sink, prefix, epoch, Publish::ConditionalPut).await
}

pub async fn start_sink_with(
    fabric: &MemoryFabric,
    store: Arc<dyn ObjectStore>,
    sink: &SinkConfig,
    prefix: &str,
    epoch: u64,
    publish: Publish,
) -> PipelineHandle {
    let settings = SinkSettings::new(sink, prefix, &instance(), publish);
    Pipeline::sink(instance(), small_config())
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::new(ObjectSinkFactory::new(
            Arc::new(StoreHandle::ready(store)),
            settings,
            instance(),
        )))
        .dead_letters(Arc::new(fabric.clone()))
        .epoch(epoch)
        .start()
        .await
        .expect("sink pipeline starts")
}

/// The sink's base for a prefix.
pub fn base(prefix: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        format!("{}/fabric", instance().name)
    } else {
        format!("{p}/{}/fabric", instance().name)
    }
}

pub async fn keys(store: &dyn ObjectStore, under: &str) -> Vec<String> {
    let mut out: Vec<String> = store
        .list(Some(&Path::from(under)))
        .map(|m| m.expect("list").location.to_string())
        .collect()
        .await;
    out.sort();
    out
}

/// The published files under a sink's base (not the staged parts).
pub async fn published(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    keys(store, &base(prefix))
        .await
        .into_iter()
        .filter(|k| !k.contains(&format!("/{STAGING}/")))
        .collect()
}

pub async fn read(store: &dyn ObjectStore, key: &str) -> bytes::Bytes {
    store
        .get(&Path::from(key))
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}

/// Event ids in published NDJSON files and in staged parts, in order.
pub async fn delivered_ids(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for k in published(store, prefix).await {
        for line in read(store, &k).await.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_slice(line).expect("json line");
            ids.push(v["id"].as_str().expect("id").to_owned());
        }
    }
    for k in keys(store, &format!("{}/{STAGING}", base(prefix))).await {
        if !k.ends_with(".arrows") {
            continue;
        }
        let body = read(store, &k).await;
        let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None)
            .expect("ipc");
        for b in r {
            let b: RecordBatch = b.expect("batch");
            ids.extend(
                batch_to_events(&b)
                    .expect("events")
                    .into_iter()
                    .map(|e| e.id),
            );
        }
    }
    ids
}

/// A listing source pipeline over `store`.
pub async fn start_source(
    fabric: &MemoryFabric,
    checkpoints: &loams_connector_sdk::testing::memory::MemoryCheckpointStore,
    store: Arc<dyn ObjectStore>,
    bucket: &str,
    prefixes: Vec<String>,
    source: serde_json::Value,
) -> PipelineHandle {
    use loams_connector_objects::config::SourceConfig;
    use loams_connector_objects::list::{
        ListingEnumerator, ListingSourceFactory, SourceSettings, StoreLister,
    };
    let cfg: SourceConfig = serde_json::from_value(source).expect("source config");
    let lister = Arc::new(StoreLister::new(
        Arc::new(StoreHandle::ready(store)),
        bucket,
    ));
    Pipeline::source(instance(), small_config())
        .enumerator(Box::new(ListingEnumerator::new(prefixes)))
        .sources(Arc::new(ListingSourceFactory::new(
            lister,
            SourceSettings::from_config(&cfg).expect("settings"),
            loams_connector_sdk::EnvelopeBuilder::new("s3", &instance()),
        )))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(checkpoints.clone()))
        .dead_letters(Arc::new(fabric.clone()))
        .discover_every(Duration::from_secs(3600))
        .epoch(1)
        .start()
        .await
        .expect("source pipeline starts")
}

/// Put `n` small objects `<prefix>/obj-<i>.json`, starting at `from`.
pub async fn put_objects(store: &dyn ObjectStore, prefix: &str, from: u32, n: u32) {
    for i in from..from + n {
        store
            .put(
                &Path::from(format!("{prefix}/obj-{i:05}.json")),
                format!("{{\"i\":{i}}}").into_bytes().into(),
            )
            .await
            .expect("put");
    }
}

// ------------------------------------------------------------- suites --
// Each body runs on `InMemory` in every build and on the real system when
// its variables are set.

/// The sink crashes mid-stream (after some files are published and some
/// parts staged), the Fabric's advisory offset lags, and a successor with a
/// newer epoch resumes: every event is delivered exactly once, in order,
/// under contiguous deterministic names.
pub async fn sink_crash_resume(store: Arc<dyn ObjectStore>, prefix: &str) {
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 100);
    // About three staged batches per file; no time roll.
    let sink = sink_config(SinkFormat::Ndjson, 4 << 10, 3600);
    let first = start_sink(&fabric, store.clone(), &sink, prefix, 1).await;
    eventually("first run is half way", || fabric.committed(GROUP) >= 50).await;
    first.abort();
    // `abort` cancels the tasks at their next await; let them go before the
    // successor starts (a crashed process is gone at once).
    tokio::time::sleep(Duration::from_millis(200)).await;
    // A crash before the Fabric commit: its (advisory) offset lags.
    fabric.set_committed(GROUP, 20);
    push_events(&fabric, 100, 20);
    let second = start_sink(&fabric, store.clone(), &sink, prefix, 2).await;
    eventually("second run caught up", || {
        let st = second.status();
        assert!(
            st.last_error.is_none(),
            "{:?} {:?}",
            st.state,
            st.last_error
        );
        fabric.committed(GROUP) == 120
    })
    .await;
    let status = second.stop().await;
    assert!(status.last_error.is_none(), "{:?}", status.last_error);

    let ids = delivered_ids(&*store, prefix).await;
    let expected: Vec<String> = (0..120).map(|i| format!("e/{i}")).collect();
    assert_eq!(ids, expected, "every event exactly once, in order");
    let names = published(&*store, prefix).await;
    assert!(names.len() >= 2, "files were rolled: {names:?}");
    assert!(names.iter().all(|n| n.ends_with(".ndjson")));
    assert_contiguous(&names);
}

/// The same input into two fresh prefixes gives the same file names.
pub async fn sink_names_deterministic(store: Arc<dyn ObjectStore>, prefix: &str) {
    let mut runs = Vec::new();
    for run in ["a", "b"] {
        let p = format!("{prefix}/{run}");
        let fabric = MemoryFabric::new();
        push_events(&fabric, 0, 60);
        let sink = sink_config(SinkFormat::Parquet, 4 << 10, 3600);
        let h = start_sink(&fabric, store.clone(), &sink, &p, 1).await;
        eventually("run done", || fabric.committed(GROUP) == 60).await;
        h.stop().await;
        let names: Vec<String> = published(&*store, &p)
            .await
            .into_iter()
            .map(|n| n.rsplit('/').next().unwrap_or_default().to_owned())
            .collect();
        assert!(!names.is_empty());
        runs.push(names);
    }
    assert_eq!(runs[0], runs[1]);
}

/// A file already sitting at the sink's next name, with other content, is
/// never overwritten: the publish is refused and the instance fails.
pub async fn conditional_put_check(store: Arc<dyn ObjectStore>, prefix: &str) {
    use loams_connector_sdk::PipelineState;
    use object_store::{PutMode, PutOptions};
    // The store itself refuses a second create.
    let probe = Path::from(format!("{prefix}/probe"));
    store
        .put(&probe, b"one".to_vec().into())
        .await
        .expect("put");
    let second = store
        .put_opts(
            &probe,
            b"two".to_vec().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(second, Err(object_store::Error::AlreadyExists { .. })),
        "the store accepted an overwrite: {second:?}"
    );
    assert_eq!(&read(&*store, probe.as_ref()).await[..], b"one");

    // The sink's first file is 0-9 (one batch of 10, rolled at once).
    let foreign = format!("{}/{:020}-{:020}.ndjson", base(prefix), 0, 9);
    store
        .put(&Path::from(foreign.as_str()), b"foreign".to_vec().into())
        .await
        .expect("put");
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 10);
    let sink = sink_config(SinkFormat::Ndjson, 1 << 30, 0);
    let h = start_sink(&fabric, store.clone(), &sink, prefix, 1).await;
    let status = h.wait_for(|s| s.state == PipelineState::Failed).await;
    let err = status.last_error.expect("an error");
    assert!(err.message.contains("already exists"), "{}", err.message);
    assert_eq!(&read(&*store, &foreign).await[..], b"foreign");
    assert_eq!(
        fabric.committed(GROUP),
        0,
        "the Fabric offset moved past an unpublished batch"
    );
}

/// The listing source reads every object once; after a stop, new objects
/// above the high-water key are read and nothing is read twice.
pub async fn listing_high_water_resume(store: Arc<dyn ObjectStore>, bucket: &str, prefix: &str) {
    use loams_connector_sdk::testing::memory::MemoryCheckpointStore;
    put_objects(&*store, prefix, 0, 25).await;
    let fabric = MemoryFabric::new();
    let ckpt = MemoryCheckpointStore::new();
    let src = serde_json::json!({});
    let h = start_source(
        &fabric,
        &ckpt,
        store.clone(),
        bucket,
        vec![prefix.to_owned()],
        src.clone(),
    )
    .await;
    eventually("25 objects", || fabric.len() == 25).await;
    h.stop().await;
    put_objects(&*store, prefix, 25, 10).await;
    let h = start_source(
        &fabric,
        &ckpt,
        store.clone(),
        bucket,
        vec![prefix.to_owned()],
        src,
    )
    .await;
    eventually("35 objects", || fabric.len() == 35).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.stop().await;
    assert_eq!(fabric.len(), 35);
    assert_eq!(fabric.duplicates(), 0, "an object was re-read");
    let ids = fabric.ids();
    assert!(
        ids[0].starts_with(&format!("{bucket}/{prefix}/obj-00000.json@")),
        "{}",
        ids[0]
    );
    let keys: Vec<String> = fabric
        .events()
        .iter()
        .map(|e| e.subject.clone().unwrap_or_default())
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "keys in order");
}

/// The published names cover `0..rows` contiguously, in order.
pub fn assert_contiguous(names: &[String]) -> u64 {
    let mut next = 0;
    for n in names {
        let file = n.rsplit('/').next().expect("name");
        let (first, last) = parse_range(file).expect("a sink name");
        assert_eq!(first, next, "gap or overlap at {n}");
        next = last + 1;
    }
    next
}
