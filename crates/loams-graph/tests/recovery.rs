//! Snapshots in the bucket and the graph manifest (GR1 Task 13; design §48 §6.4, §14.1; D742).
//!
//! * A checkpoint takes a native snapshot (Grafeo's `backup_full`) and a portable one (Parquet)
//!   at one durable offset, under the graph's gate, while writes go on; the snapshot plus the
//!   records after its offset is the graph.
//! * The manifest is committed by a fenced CAS: a lost CAS retries on the winner's manifest, a
//!   lost acknowledgement is recognised, and a stale owner commits nothing and trims nothing.
//! * The stream is trimmed below the oldest retained snapshot and never inside the PITR window,
//!   and a trimmed graph opens from its snapshot.
//! * Objects no manifest names are swept, and a purge deletes a graph's objects.
//!
//! Bucket faults are injected with `loams-store`'s `FaultyStore`, manifest CAS races and lost
//! acknowledgements with the manifest store's test hooks (feature `test-hooks`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use connectrpc::ConnectError;
use loams_cache::{RangeCache, RangeCacheConfig};
use loams_common::meta::{Consistency, Fence, MetaStore};
use loams_common::{NamespaceId, StreamId};
use loams_graph::catalog::GraphCatalog;
use loams_graph::changeset::{GraphRecord, apply_to_store, decode};
use loams_graph::durable::GraphLog;
use loams_graph::service::admin::GraphAdmin;
use loams_graph::snapshot::manifest::pointer_key;
use loams_graph::snapshot::portable::GraphRows;
use loams_graph::snapshot::{
    CheckpointPolicy, GraphManifest, ManifestStore, SnapshotEntry, SnapshotKind, graph_prefix,
    native, portable, retain,
};
use loams_graph::{Engine, GraphId};
use loams_log::{FetchRequest, LogReader, LogWriter};
use loams_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use loams_proto::loams::graph::v1 as pb;
use loams_store::{Fault, FaultyStore, Op, Store};
use pb::__buffa::oneof::value::Kind;
use proptest::prelude::*;
use tempfile::TempDir;

/// A single-node metastore, the log, a bucket with fault injection for the catalog and the
/// snapshots, and a data directory.
struct Fixture {
    _node: MetaNode,
    meta: Arc<dyn MetaStore>,
    writer: LogWriter,
    reader: LogReader,
    /// The catalog's and the snapshots' bucket.
    bucket: Store,
    faults: Arc<FaultyStore>,
    data_dir: TempDir,
    _meta_dir: TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let meta_dir = TempDir::new().expect("temp dir");
        let clock = Arc::new(SystemClock);
        let mut config = MetaConfig::new(1, meta_dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(Duration::from_secs(30))
            .await
            .expect("leader");
        let meta: Arc<dyn MetaStore> = Arc::new(MetaClient::new(
            node.clone(),
            vec![],
            clock,
            MetaClientConfig::default(),
        ));
        // The log has a bucket of its own, so a fault injected for the snapshots never hits a
        // WAL object.
        let log_store = Store::in_memory();
        let cache = RangeCache::new(
            log_store.clone(),
            RangeCacheConfig {
                memory_bytes: 16 << 20,
                ..RangeCacheConfig::default()
            },
        )
        .await
        .expect("range cache");
        let writer =
            LogWriter::start(meta.clone(), log_store, GraphLog::log_config(1)).expect("log writer");
        let reader = LogReader::new(meta.clone(), cache);
        let faults = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
        let bucket = Store::new(faults.clone()).with_sorted_listing(true);
        Self {
            _node: node,
            meta,
            writer,
            reader,
            bucket,
            faults,
            data_dir: TempDir::new().expect("data dir"),
            _meta_dir: meta_dir,
        }
    }

    fn log(&self) -> GraphLog {
        GraphLog::new(
            self.meta.clone(),
            self.writer.clone(),
            self.reader.clone(),
            tokio::runtime::Handle::current(),
        )
    }

    fn catalog(&self) -> GraphCatalog {
        GraphCatalog::new(self.meta.clone(), self.bucket.clone())
    }

    fn manifests(&self) -> ManifestStore {
        ManifestStore::new(self.meta.clone(), self.bucket.clone())
    }

    /// An admin over a fresh engine on the data directory, with `policy`.
    fn admin(&self, policy: CheckpointPolicy) -> GraphAdmin {
        GraphAdmin::new(
            Arc::new(Engine::with_data_dir(self.data_dir.path())),
            self.catalog(),
        )
        .with_log(self.log())
        .with_checkpoint_policy(policy)
    }

    /// An admin whose engine keeps graphs in memory (no data directory).
    fn admin_in_memory(&self, policy: CheckpointPolicy) -> GraphAdmin {
        GraphAdmin::new(Arc::new(Engine::new()), self.catalog())
            .with_log(self.log())
            .with_checkpoint_policy(policy)
    }

    async fn namespace(&self) -> NamespaceId {
        self.meta
            .namespace_by_name(Consistency::Linearizable, "acme")
            .await
            .expect("namespace")
            .expect("acme exists")
            .id
    }

    async fn graph_id(&self, name: &str) -> GraphId {
        self.catalog()
            .get_by_name("acme", name)
            .await
            .expect("catalog record")
            .graph_id()
            .expect("graph id")
    }

    async fn stream_of(&self, name: &str) -> StreamId {
        self.catalog()
            .get_by_name("acme", name)
            .await
            .expect("catalog record")
            .stream
            .expect("a durable graph has its stream")
    }

    /// The stream's first readable offset and its high watermark.
    async fn bounds(&self, stream: StreamId) -> (u64, u64) {
        let state = self
            .meta
            .stream_state(Consistency::Linearizable, stream)
            .await
            .expect("stream state")
            .expect("the stream exists");
        let bounds = state.partitions[0].expect("partition 0");
        (bounds.log_start_offset, bounds.high_watermark)
    }

    /// The records of a stream in `from..to`, decoded.
    async fn records(&self, stream: StreamId, from: u64, to: u64) -> Vec<(u64, GraphRecord)> {
        let mut out = Vec::new();
        let mut offset = from;
        while offset < to {
            let fetched = self
                .reader
                .fetch(FetchRequest {
                    stream,
                    partition: 0,
                    offset,
                    max_bytes: 1 << 20,
                    max_wait: Duration::from_millis(50),
                })
                .await
                .expect("fetch");
            for record in fetched.records {
                if record.offset >= to {
                    break;
                }
                offset = record.offset + 1;
                let value = record.record.value.as_deref().expect("a record value");
                out.push((record.offset, decode(value).expect("a GraphRecord")));
            }
        }
        out
    }

    /// The manifest of graph `name`.
    async fn manifest(&self, name: &str) -> Option<GraphManifest> {
        let id = self.graph_id(name).await;
        self.manifests()
            .load(self.namespace().await, id)
            .await
            .expect("load the manifest")
            .map(|loaded| loaded.manifest)
    }

    /// Every bucket object of graph `name`.
    async fn objects(&self, name: &str) -> Vec<String> {
        let prefix = graph_prefix(self.namespace().await, self.graph_id(name).await);
        let mut paths: Vec<String> = self
            .bucket
            .list(&prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|o| o.path)
            .collect();
        paths.sort();
        paths
    }
}

/// Snapshots every time, a PITR window of `window` and no idempotency floor (`test-hooks`).
fn policy(window: Duration) -> CheckpointPolicy {
    CheckpointPolicy {
        interval: Duration::ZERO,
        log_bytes: u64::MAX,
        portable_every: Duration::ZERO,
        pitr_window: window,
        max_snapshots: 48,
    }
}

const BOTH: [SnapshotKind; 2] = [SnapshotKind::Native, SnapshotKind::Portable];

async fn create(admin: &GraphAdmin, name: &str) {
    admin
        .create_graph(pb::CreateGraphRequest {
            namespace: "acme".to_string(),
            name: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("create");
}

fn request(graph: &str, statement: &str, key: &str) -> pb::ExecuteRequest {
    pb::ExecuteRequest {
        namespace: "acme".to_string(),
        graph: graph.to_string(),
        statement: statement.to_string(),
        idempotency_key: key.to_string(),
        ..Default::default()
    }
}

async fn run(admin: &GraphAdmin, graph: &str, statement: &str) -> pb::ExecuteResponse {
    admin
        .execute(request(graph, statement, ""))
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err:?}"))
}

/// `(k, v)` of every `:N` node, by `k`.
async fn values(admin: &GraphAdmin, graph: &str) -> BTreeMap<i64, String> {
    let response = run(admin, graph, "MATCH (n:N) RETURN n.k, n.v").await;
    response
        .rows
        .as_option()
        .expect("rows")
        .rows
        .iter()
        .map(|row| match (&row.values[0].kind, &row.values[1].kind) {
            (Some(Kind::Int64(k)), Some(Kind::String(v))) => (*k, v.clone()),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// The live graph's rows (its durable state).
async fn live_rows(admin: &GraphAdmin, graph: &str) -> GraphRows {
    admin
        .open("acme", graph)
        .await
        .expect("open")
        .rows()
        .expect("rows")
}

/// Applies the change sets of `records` to `db`.
fn replay(db: &grafeo::GrafeoDB, records: &[(u64, GraphRecord)]) {
    for (_, record) in records {
        if let GraphRecord::ChangeSet(change) = record {
            apply_to_store(db, &change.ops).expect("replay");
        }
    }
}

/// The graph a snapshot and the records after it make: restored into a fresh engine.
async fn rebuilt(fx: &Fixture, entry: &SnapshotEntry, records: &[(u64, GraphRecord)]) -> GraphRows {
    let after: Vec<_> = records
        .iter()
        .filter(|(offset, _)| *offset >= entry.offset)
        .cloned()
        .collect();
    match entry.kind {
        SnapshotKind::Native => {
            let dir = TempDir::new().expect("temp dir");
            let file = dir.path().join("restored.grafeo");
            native::restore(&fx.bucket, entry, &file)
                .await
                .expect("restore the native snapshot");
            let db = native::open(&file).expect("open the native snapshot");
            replay(&db, &after);
            let rows = GraphRows::read(&db);
            db.close().expect("close");
            rows
        }
        SnapshotKind::Portable => {
            let (manifest, rows) = portable::read(&fx.bucket, entry)
                .await
                .expect("read the portable snapshot");
            assert_eq!(manifest.offset, entry.offset);
            let db = grafeo::GrafeoDB::new_in_memory();
            rows.import(&db).expect("import");
            replay(&db, &after);
            GraphRows::read(&db)
        }
    }
}

fn kinds(manifest: &GraphManifest) -> Vec<SnapshotKind> {
    manifest.snapshots.iter().map(|e| e.kind).collect()
}

// ---------------------------------------------------------------------------------------------
// The manifest
// ---------------------------------------------------------------------------------------------

fn entry(id: &str, kind: SnapshotKind, offset: u64, created_at_ms: u64) -> SnapshotEntry {
    SnapshotEntry {
        id: id.to_string(),
        kind,
        offset,
        epoch: 0,
        next_txn_seq: 1,
        lease_epoch: 1,
        keys: vec![format!("snap/{offset:020}/{id}/x")],
        bytes: 1,
        crc32c: None,
        engine_version: loams_graph::ENGINE_VERSION.to_string(),
        created_at_ms,
        nodes: 0,
        edges: 0,
    }
}

/// A manifest change that adds `entry` once.
fn add(
    entry: SnapshotEntry,
) -> impl Fn(&mut GraphManifest) -> Result<(bool, ()), loams_graph::snapshot::SnapshotError> {
    move |manifest| {
        if manifest.snapshots.iter().any(|e| e.id == entry.id) {
            return Ok((false, ()));
        }
        manifest.snapshots.push(entry.clone());
        Ok((true, ()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_manifest_cas_conflict_retries() {
    // Two writers of one manifest: the one whose CAS loses re-reads the winner's manifest and
    // applies its change on top, so neither entry is lost.
    let fx = Fixture::start().await;
    let ns = fx.meta.create_namespace("acme").await.expect("namespace");
    let graph = GraphId::new();
    let mine = fx.manifests();
    let other = fx.manifests();
    let raced = Arc::new(AtomicBool::new(false));
    let hook: loams_graph::catalog::AckHook = {
        let (other, raced) = (other.clone(), raced.clone());
        Arc::new(move || {
            let (other, raced) = (other.clone(), raced.clone());
            Box::pin(async move {
                if !raced.swap(true, Ordering::SeqCst) {
                    other
                        .update(
                            ns,
                            graph,
                            None,
                            add(entry("theirs", SnapshotKind::Portable, 5, 1)),
                        )
                        .await
                        .expect("the concurrent writer commits");
                }
                false
            })
        })
    };
    mine.set_hooks(Some(hook), None);
    let (manifest, ()) = mine
        .update(
            ns,
            graph,
            None,
            add(entry("mine", SnapshotKind::Native, 9, 2)),
        )
        .await
        .expect("the losing writer retries and commits");
    assert!(raced.load(Ordering::SeqCst));
    assert!(mine.counters().cas_retries.load(Ordering::SeqCst) >= 1);
    let ids: Vec<&str> = manifest.snapshots.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["theirs", "mine"], "sorted by offset, both kept");
    let loaded = fx
        .manifests()
        .load(ns, graph)
        .await
        .expect("load")
        .expect("a manifest");
    assert_eq!(loaded.manifest, manifest);
    assert_eq!(loaded.version, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_manifest_acknowledgement_is_recognised() {
    // The CAS commits and its answer is lost: the retry finds its own entry already in and
    // writes nothing more.
    let fx = Fixture::start().await;
    let ns = fx.meta.create_namespace("acme").await.expect("namespace");
    let graph = GraphId::new();
    let manifests = fx.manifests();
    let lost = Arc::new(AtomicUsize::new(0));
    let hook: loams_graph::catalog::AckHook = {
        let lost = lost.clone();
        Arc::new(move || {
            let lost = lost.clone();
            Box::pin(async move { lost.fetch_add(1, Ordering::SeqCst) == 0 })
        })
    };
    manifests.set_hooks(None, Some(hook));
    let (manifest, ()) = manifests
        .update(
            ns,
            graph,
            None,
            add(entry("once", SnapshotKind::Native, 3, 1)),
        )
        .await
        .expect("commits");
    assert_eq!(
        lost.load(Ordering::SeqCst),
        1,
        "one acknowledgement was lost"
    );
    assert_eq!(manifest.snapshots.len(), 1);
    let pointer = fx
        .meta
        .pointer(Consistency::Linearizable, ns, &pointer_key(graph))
        .await
        .expect("pointer")
        .expect("set");
    assert_eq!(pointer.version, 1, "the retry wrote nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_fence_commits_no_manifest() {
    let fx = Fixture::start().await;
    let ns = fx.meta.create_namespace("acme").await.expect("namespace");
    let graph = GraphId::new();
    let key = format!("graph/{graph}/owner");
    let old = fx
        .meta
        .acquire_lease(&key, "old", Duration::from_secs(10))
        .await
        .expect("lease");
    fx.meta
        .release_lease(&key, "old", old.epoch)
        .await
        .expect("release");
    fx.meta
        .acquire_lease(&key, "new", Duration::from_secs(10))
        .await
        .expect("the lease moves on");
    let stale = Fence {
        lease: key,
        epoch: old.epoch,
    };
    let refused = fx
        .manifests()
        .update(
            ns,
            graph,
            Some(&stale),
            add(entry("stale", SnapshotKind::Native, 1, 1)),
        )
        .await
        .expect_err("a stale owner commits nothing");
    assert_eq!(refused, loams_graph::snapshot::SnapshotError::Fenced);
    assert!(
        fx.manifests()
            .load(ns, graph)
            .await
            .expect("load")
            .is_none()
    );
}

// ---------------------------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------------------------

/// A stream's history for the retention rule: when each record was appended, where snapshots
/// were taken, and the rule's inputs.
#[derive(Debug, Clone)]
struct History {
    /// Each record's timestamp, by offset, non-decreasing.
    times: Vec<u64>,
    /// Each snapshot's offset (modulo the record count plus one) and whether it is portable.
    points: Vec<(usize, bool)>,
    previous: u64,
    window: u64,
    now: u64,
    max: usize,
}

fn history() -> impl Strategy<Value = History> {
    (
        prop::collection::vec(0u64..50, 1..60),
        prop::collection::vec((0usize..60, any::<bool>()), 0..12),
        0u64..100,
        0u64..3000,
        0u64..200,
        1usize..10,
    )
        .prop_map(|(gaps, points, previous, window, extra, max)| {
            // Record timestamps, non-decreasing in offset.
            let mut at = 0;
            let times: Vec<u64> = gaps
                .iter()
                .map(|gap| {
                    at += gap;
                    at
                })
                .collect();
            History {
                times,
                points,
                previous,
                window,
                now: at + extra,
                max,
            }
        })
}

fn snapshots_of(times: &[u64], points: &[(usize, bool)]) -> Vec<SnapshotEntry> {
    points
        .iter()
        .enumerate()
        .map(|(i, (point, portable))| {
            let offset = (*point % (times.len() + 1)) as u64;
            // Taken right after the last record below `offset`, so no later than the record at
            // `offset`.
            let created = match offset as usize {
                0 => times[0],
                n => times[n - 1],
            };
            let kind = if *portable {
                SnapshotKind::Portable
            } else {
                SnapshotKind::Native
            };
            entry(&format!("s{i:02}"), kind, offset, created)
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn trim_never_passes_oldest_snapshot(history in history()) {
        let History { times, points, window, now, max, .. } = history;
        let snapshots = snapshots_of(&times, &points);
        let retained = retain(&snapshots, 0, now, window, max);
        if retained.trim > 0 {
            // The stream is trimmed only below a snapshot the manifest keeps.
            prop_assert!(retained.kept.iter().any(|e| e.offset == retained.trim));
        }
        for kept in &retained.kept {
            prop_assert!(kept.offset >= retained.trim);
        }
        prop_assert!(retained.kept.len() <= max.max(3));
        prop_assert_eq!(retained.kept.len() + retained.retired.len(), snapshots.len());
        // A trim point once reached never moves back.
        let again = retain(&retained.kept, retained.trim, now, window, max);
        prop_assert!(again.trim >= retained.trim);
    }

    #[test]
    fn trim_never_inside_pitr_window(history in history()) {
        let History { times, points, previous: previous_raw, window, now, max } = history;
        let snapshots = snapshots_of(&times, &points);
        // A previous trim point that was legal: at a snapshot outside the window.
        let previous = snapshots
            .iter()
            .filter(|e| e.created_at_ms + window <= now)
            .map(|e| e.offset)
            .min()
            .map_or(0, |offset| offset.min(previous_raw));
        let retained = retain(&snapshots, previous, now, window, max);
        for (offset, time) in times.iter().enumerate() {
            if time + window > now {
                // A record inside the window is never trimmed.
                prop_assert!((offset as u64) >= retained.trim,
                    "record {} at {} is inside the window ({} at {}) but below the trim {}",
                    offset, time, window, now, retained.trim);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_snapshot_round_trip() {
    // Export, import into a fresh engine: the canonical dumps are equal, element ids included.
    let fx = Fixture::start().await;
    let admin = fx.admin(policy(Duration::from_secs(3600)));
    create(&admin, "kg").await;
    run(
        &admin,
        "kg",
        "INSERT (:Person:Admin {name: 'Ada', age: 36, score: 1.5, ok: true, tags: ['x', 'y'], \
         born: date('1815-12-10')})-[:KNOWS {since: 2020, w: 0.25}]->(:Person {name: 'Bob'})",
    )
    .await;
    run(&admin, "kg", "INSERT (:Thing)").await;
    run(
        &admin,
        "kg",
        "MATCH (n:Person {name: 'Bob'}) SET n.nick = 'B', n.age = -1",
    )
    .await;
    run(&admin, "kg", "MATCH (n:Thing) DELETE n").await;
    let done = admin
        .checkpoint("acme", "kg", &[SnapshotKind::Portable])
        .await
        .expect("checkpoint");
    let [snapshot] = done.taken.as_slice() else {
        panic!("one snapshot: {:?}", done.taken)
    };
    assert_eq!(snapshot.kind, SnapshotKind::Portable);
    assert_eq!((snapshot.nodes, snapshot.edges), (2, 1));
    let live = live_rows(&admin, "kg").await;
    assert_eq!(live.nodes.len(), 2);
    assert_eq!(rebuilt(&fx, snapshot, &[]).await, live);

    // The files alone, without the bucket: the format decodes what it encodes.
    let header = portable::Header {
        graph: "gr_x".to_string(),
        offset: 7,
        next_txn_seq: 3,
        created_at_ms: 1,
    };
    let files = portable::encode(&live, &header).expect("encode");
    let names: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        [
            "nodes/part-00000.parquet",
            "edges/part-00000.parquet",
            "manifest.json"
        ]
    );
    let files: BTreeMap<String, bytes::Bytes> = files.into_iter().collect();
    let (manifest, rows) = portable::decode(|name| {
        let bytes = files.get(&name).cloned();
        async move { bytes.ok_or(loams_graph::snapshot::SnapshotError::Corrupt(name)) }
    })
    .await
    .expect("decode");
    assert_eq!((manifest.offset, manifest.next_txn_seq), (7, 3));
    assert_eq!(rows, live);
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_concurrent_with_writes_is_consistent() {
    // Snapshots taken while writers run: each one, plus the records after its offset, is the
    // live graph, element ids included.
    let fx = Fixture::start().await;
    let admin = Arc::new(fx.admin(policy(Duration::from_secs(3600))));
    create(&admin, "kg").await;
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let admin = Arc::clone(&admin);
            tokio::spawn(async move {
                for i in 0..30 {
                    let k = w * 100 + i;
                    run(&admin, "kg", &format!("INSERT (:N {{k: {k}, v: 'w{w}'}})")).await;
                    if i % 3 == 0 {
                        run(
                            &admin,
                            "kg",
                            &format!("MATCH (n:N {{k: {k}}}) SET n.v = 'set{k}'"),
                        )
                        .await;
                    }
                    if i % 5 == 4 {
                        run(
                            &admin,
                            "kg",
                            &format!("MATCH (n:N {{k: {}}}) DELETE n", k - 1),
                        )
                        .await;
                    }
                }
            })
        })
        .collect();
    let mut taken = Vec::new();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let done = admin
            .checkpoint("acme", "kg", &BOTH)
            .await
            .expect("checkpoint during writes");
        taken.extend(done.taken);
    }
    for writer in writers {
        writer.await.expect("writer");
    }
    let stream = fx.stream_of("kg").await;
    let (start, end) = fx.bounds(stream).await;
    assert_eq!(start, 0, "a window of an hour trims nothing");
    let records = fx.records(stream, 0, end).await;
    let live = live_rows(&admin, "kg").await;
    assert!(!live.nodes.is_empty());
    assert_eq!(taken.len(), 6);
    assert!(
        taken.iter().any(|e| e.offset > 0 && e.offset < end),
        "a snapshot was taken mid-workload: {taken:?}"
    );
    for entry in &taken {
        assert_eq!(
            rebuilt(&fx, entry, &records).await,
            live,
            "{:?} snapshot at {} plus the records after it",
            entry.kind,
            entry.offset
        );
    }
    // Both kinds of one checkpoint cover the same offset.
    for pair in taken.chunks(2) {
        assert_eq!(pair[0].offset, pair[1].offset);
    }
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stream_is_trimmed_below_snapshots_outside_the_window_only() {
    let fx = Fixture::start().await;
    // Inside an hour's window, checkpoints keep every record.
    let admin = fx.admin(policy(Duration::from_secs(3600)));
    create(&admin, "kg").await;
    for k in 0..3 {
        run(&admin, "kg", &format!("INSERT (:N {{k: {k}, v: 'a'}})")).await;
        admin
            .checkpoint("acme", "kg", &BOTH)
            .await
            .expect("checkpoint");
    }
    let stream = fx.stream_of("kg").await;
    assert_eq!(fx.bounds(stream).await.0, 0);
    let manifest = fx.manifest("kg").await.expect("manifest");
    assert_eq!(manifest.log_trim_offset, 0);
    assert_eq!(manifest.snapshots.len(), 6);
    admin.shutdown(Duration::from_secs(5)).await;

    // With no window, the next checkpoint trims below its snapshots.
    let admin = fx.admin(policy(Duration::ZERO));
    run(&admin, "kg", "INSERT (:N {k: 9, v: 'b'})").await;
    let done = admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect("checkpoint");
    let (start, end) = fx.bounds(stream).await;
    assert_eq!(done.log_start, Some(start));
    assert_eq!(start, end, "every record is in the newest snapshot");
    assert_eq!(done.manifest.log_trim_offset, start);
    let oldest = done.manifest.snapshots.first().expect("a snapshot").offset;
    assert!(
        start <= oldest,
        "trimmed to {start}, oldest snapshot at {oldest}"
    );
    assert_eq!(kinds(&done.manifest), BOTH);
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trimmed_graph_opens_from_its_snapshot() {
    // On disk it opens from its newest native snapshot, in memory from its newest portable one;
    // either way the records after it are replayed. On disk, the stream is trimmed at the older
    // portable snapshot, so records between the two are read for their idempotency keys only: a
    // key written there still answers its first run.
    for on_disk in [true, false] {
        let fx = Fixture::start().await;
        let make = |fx: &Fixture| match on_disk {
            true => fx.admin(policy(Duration::ZERO)),
            false => fx.admin_in_memory(policy(Duration::ZERO)),
        };
        let admin = make(&fx);
        create(&admin, "kg").await;
        run(&admin, "kg", "INSERT (:N {k: 1, v: 'one'})").await;
        run(&admin, "kg", "INSERT (:N {k: 2, v: 'two'})").await;
        admin
            .checkpoint("acme", "kg", &BOTH)
            .await
            .expect("checkpoint");
        let keyed = admin
            .execute(request("kg", "INSERT (:N {k: 3, v: 'three'})", "key-3"))
            .await
            .expect("keyed write");
        let done = admin
            .checkpoint("acme", "kg", &[SnapshotKind::Native])
            .await
            .expect("checkpoint");
        let stream = fx.stream_of("kg").await;
        let (start, _) = fx.bounds(stream).await;
        if on_disk {
            assert_eq!(
                kinds(&done.manifest),
                [
                    SnapshotKind::Native,
                    SnapshotKind::Portable,
                    SnapshotKind::Native
                ]
            );
            assert_eq!(start, 2, "trimmed at the portable snapshot");
        } else {
            assert_eq!(kinds(&done.manifest), [SnapshotKind::Portable]);
            assert_eq!(start, 3);
        }
        run(&admin, "kg", "INSERT (:N {k: 4, v: 'four'})").await;
        run(&admin, "kg", "MATCH (n:N {k: 2}) SET n.v = 'TWO'").await;
        let before = values(&admin, "kg").await;
        let rows = live_rows(&admin, "kg").await;
        admin.shutdown(Duration::from_secs(5)).await;

        let reopened = make(&fx);
        assert_eq!(values(&reopened, "kg").await, before, "on disk: {on_disk}");
        assert_eq!(live_rows(&reopened, "kg").await, rows);
        if on_disk {
            let again = reopened
                .execute(request("kg", "INSERT (:N {k: 3, v: 'three'})", "key-3"))
                .await
                .expect("keyed retry");
            assert_eq!(again.consistency_token, keyed.consistency_token);
        }
        // New ids do not collide with restored ones, and the stream goes on where it was.
        run(&reopened, "kg", "INSERT (:N {k: 5, v: 'five'})").await;
        assert_eq!(values(&reopened, "kg").await.len(), 5);
        reopened.shutdown(Duration::from_secs(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoints_follow_the_policy() {
    // Due once the graph changed and the interval passed; a portable one when there is none;
    // nothing for an unchanged graph.
    let fx = Fixture::start().await;
    let admin = fx.admin(CheckpointPolicy {
        portable_every: Duration::from_secs(3600),
        ..policy(Duration::from_secs(3600))
    });
    create(&admin, "kg").await;
    run(&admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    assert_eq!(admin.checkpoint_due().await, 1);
    assert_eq!(kinds(&fx.manifest("kg").await.expect("manifest")), BOTH);
    assert_eq!(admin.checkpoint_due().await, 0, "unchanged since");
    run(&admin, "kg", "INSERT (:N {k: 2, v: 'b'})").await;
    assert_eq!(admin.checkpoint_due().await, 1);
    let manifest = fx.manifest("kg").await.expect("manifest");
    assert_eq!(
        kinds(&manifest),
        [
            SnapshotKind::Native,
            SnapshotKind::Portable,
            SnapshotKind::Native
        ],
        "the weekly portable snapshot is not due again"
    );
    admin.shutdown(Duration::from_secs(5)).await;
}

// ---------------------------------------------------------------------------------------------
// Faults, fencing, sweeping and purging
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_upload_commits_nothing_and_the_next_checkpoint_succeeds() {
    let fx = Fixture::start().await;
    let admin = fx.admin(policy(Duration::ZERO));
    create(&admin, "kg").await;
    run(&admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    let stream = fx.stream_of("kg").await;
    fx.faults.inject(Op::Put, Fault::Error);
    let err: ConnectError = admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect_err("the upload fails");
    assert_eq!(err.code, connectrpc::ErrorCode::Unavailable);
    assert!(fx.manifest("kg").await.is_none(), "no manifest names it");
    assert_eq!(fx.bounds(stream).await.0, 0, "nothing trimmed");
    // A portable file whose PUT applies but is answered as failed: the same.
    fx.faults.inject_nth(Op::Put, 2, Fault::ErrorAfterApply);
    admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect_err("the upload is answered as failed");
    assert!(fx.manifest("kg").await.is_none());
    let done = admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect("checkpoint");
    assert_eq!(fx.bounds(stream).await.0, done.manifest.log_trim_offset);
    assert!(done.manifest.log_trim_offset > 0);
    assert_eq!(values(&admin, "kg").await.len(), 1);
    // The staging directory is cleaned up whatever happened.
    let id = fx.graph_id("kg").await;
    let staging = fx
        .data_dir
        .path()
        .join("graphs")
        .join(id.to_string())
        .join("snapshots");
    let left = std::fs::read_dir(&staging).map_or(0, |dir| dir.count());
    assert_eq!(left, 0, "no staging directory is left behind");
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_owner_commits_no_snapshot_and_trims_nothing() {
    // The graph's owner lease moves on while this node still has the graph open: its checkpoint
    // is refused (by the fence at the manifest CAS, or because the lost lease already fenced the
    // graph), nothing is committed or trimmed, and the graph is fenced here.
    let fx = Fixture::start().await;
    let admin = fx.admin(policy(Duration::ZERO));
    create(&admin, "kg").await;
    run(&admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    let graph = admin.open("acme", "kg").await.expect("open");
    let id = fx.graph_id("kg").await;
    let epoch = graph.lease_epoch().expect("served under a lease");
    let ownership = admin.ownership().expect("ownership").clone();
    ownership.pause_renewals(true);
    let key = loams_graph::owner::owner_lease_key(id);
    fx.meta
        .release_lease(&key, &ownership.me().lease_owner(), epoch)
        .await
        .expect("release");
    fx.meta
        .acquire_lease(&key, "v1;99;1;127.0.0.1:1", Duration::from_secs(10))
        .await
        .expect("another node takes the lease");
    let checkpointer = admin.checkpointer().expect("checkpointer");
    let refused = checkpointer
        .checkpoint(graph.clone(), fx.namespace().await, &BOTH)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            refused,
            loams_graph::snapshot::SnapshotError::Fenced
                | loams_graph::snapshot::SnapshotError::NotServing(_)
        ),
        "{refused:?}"
    );
    assert!(graph.is_fenced());
    assert!(fx.manifest("kg").await.is_none());
    assert_eq!(fx.bounds(fx.stream_of("kg").await).await.0, 0);
    drop(graph);
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreferenced_snapshot_objects_are_swept() {
    // Retired snapshots, and the objects of a checkpoint that never committed, go; what the
    // manifest names and the manifest itself stay.
    let fx = Fixture::start().await;
    let admin = fx.admin(CheckpointPolicy {
        max_snapshots: 2,
        ..policy(Duration::ZERO)
    });
    create(&admin, "kg").await;
    for k in 0..4 {
        run(&admin, "kg", &format!("INSERT (:N {{k: {k}, v: 'a'}})")).await;
        admin
            .checkpoint("acme", "kg", &BOTH)
            .await
            .expect("checkpoint");
    }
    // An orphan: its snapshots uploaded (the native file, the node file and the portable
    // manifest), then the PUT of its manifest document fails, so no manifest names them.
    run(&admin, "kg", "INSERT (:N {k: 9, v: 'b'})").await;
    let before = fx.objects("kg").await;
    fx.faults.inject_nth(Op::Put, 4, Fault::Error);
    admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect_err("the manifest write fails");
    assert!(
        fx.objects("kg").await.len() > before.len(),
        "the orphan's objects were uploaded"
    );
    // The sweep compares against the current manifest's write time: write a new one.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let done = admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect("checkpoint");
    let swept = admin.sweep_snapshots(Duration::ZERO).await.expect("sweep");
    assert!(swept > 0);
    let id = fx.graph_id("kg").await;
    let ns = fx.namespace().await;
    let current = fx
        .manifests()
        .load(ns, id)
        .await
        .expect("load")
        .expect("manifest");
    let mut expected: Vec<String> = done
        .manifest
        .keys()
        .into_iter()
        .map(str::to_string)
        .collect();
    expected.push(current.key.clone());
    expected.sort();
    assert_eq!(fx.objects("kg").await, expected);
    // What is left still opens.
    for entry in &current.manifest.snapshots {
        assert_eq!(rebuilt(&fx, entry, &[]).await.nodes.len(), 5);
    }
    admin.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_deletes_the_graphs_snapshots() {
    let fx = Fixture::start().await;
    let admin = fx.admin(policy(Duration::ZERO));
    create(&admin, "kg").await;
    run(&admin, "kg", "INSERT (:N {k: 1, v: 'a'})").await;
    admin
        .checkpoint("acme", "kg", &BOTH)
        .await
        .expect("checkpoint");
    let ns = fx.namespace().await;
    let id = fx.graph_id("kg").await;
    let prefix = graph_prefix(ns, id);
    assert!(!fx.bucket.list(&prefix).await.expect("list").is_empty());
    admin
        .delete_graph(pb::DeleteGraphRequest {
            namespace: "acme".to_string(),
            name: "kg".to_string(),
            ..Default::default()
        })
        .await
        .expect("delete");
    // A bucket failure leaves the purge for the next sweep, record and all.
    fx.faults.inject(Op::List, Fault::Error);
    assert_eq!(admin.purge_expired(Duration::ZERO).await.expect("purge"), 0);
    assert_eq!(admin.purge_expired(Duration::ZERO).await.expect("purge"), 1);
    assert!(fx.bucket.list(&prefix).await.expect("list").is_empty());
    admin.shutdown(Duration::from_secs(5)).await;
}
