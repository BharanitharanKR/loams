//! The change-log adjacency index (GR1 Task 22; design §48 §26.3; D1063, D1064).
//!
//! * The format: golden vectors, and a patched index is a full build byte for byte
//!   (`incremental_build_equals_full_build`, a proptest over change sets as replay applies them).
//! * Publication: create-only files, a manifest CAS that only moves forward and is fenced, the
//!   sweep of unreferenced files.
//! * Builds and readers: every `Decline` is a full build and counted, readers overlay the tail or
//!   fall back, the index path holds no gate, a new owner rebuilds nothing, a graph no algorithm
//!   touched has no index, and the counters.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use grafeo::Value;
use loams_common::meta::{Fence, LinkId};
use loams_graph::GraphId;
use loams_graph::adjacency::format::{
    AdjacencyIndex, EdgeState, Topology, VertexKey, VertexState, sha256_hex,
};
use loams_graph::adjacency::{
    AdjacencyConfig, BuildMode, Decline, PinSource, Tail, build, patch, publish,
};
use loams_graph::changeset::{
    ChangeOp, ElementKind, GraphChangeSet, GraphRecord, LinkApply, PropMap, SchemaChange,
    apply_to_store, encode,
};
use loams_graph::consistency::Consistency;
use loams_graph::owner::{OwnerConfig, owner_lease_key};
use loams_graph::snapshot::manifest::AdjacencyMode;
use loams_graph::snapshot::{CheckpointPolicy, SnapshotError, SnapshotKind};
use loams_log::{OffsetRecord, Record};
use loams_proto::loams::graph::v1::QueryLanguage;
use loams_store::Store;
use proptest::prelude::*;

use common::{Cluster, create, run, token};

// ---------------------------------------------------------------------------------------------
// The format
// ---------------------------------------------------------------------------------------------

fn golden_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance/graph/adjacency/v1")
}

fn graph_id() -> GraphId {
    GraphId(ulid::Ulid::from(
        0x0123_4567_89ab_cdef_0011_2233_4455_6677_u128,
    ))
}

fn vertex(labels: &[&str], key: Option<VertexKey>) -> VertexState {
    VertexState {
        labels: labels.iter().map(|l| (*l).to_string()).collect(),
        key,
    }
}

fn edge(ty: &str, src: u64, dst: u64, weight: f64) -> EdgeState {
    EdgeState {
        ty: ty.to_string(),
        src,
        dst,
        weight,
    }
}

fn golden_cases() -> Vec<(&'static str, AdjacencyIndex)> {
    let small = Topology {
        vertices: BTreeMap::from([
            (2, vertex(&["entity"], Some(VertexKey::Int(7)))),
            (
                5,
                vertex(&["chunk", "entity"], Some(VertexKey::Str("c-1".into()))),
            ),
            (9, vertex(&["chunk"], None)),
            (11, vertex(&["entity"], Some(VertexKey::Int(-3)))),
            (40, vertex(&[], Some(VertexKey::Str("é".into())))),
        ]),
        edges: BTreeMap::from([
            (100, edge("RELATED", 2, 5, 1.0)),
            (101, edge("RELATED", 2, 5, 2.5)),
            (102, edge("MENTIONS", 9, 2, 1.0)),
            (103, edge("RELATED", 11, 11, 1.0)),
            (104, edge("MENTIONS", 5, 40, 0.5)),
            (90, edge("RELATED", 40, 2, 3.0)),
            (7, edge("MENTIONS", 2, 9, -1.0)),
        ]),
    };
    let isolated = Topology {
        vertices: BTreeMap::from([
            (1, vertex(&["a"], None)),
            (3, vertex(&["b"], Some(VertexKey::Int(0)))),
        ]),
        edges: BTreeMap::new(),
    };
    let index =
        |offset, t: &Topology| AdjacencyIndex::from_topology(graph_id(), offset, t).expect("index");
    vec![
        ("empty", index(0, &Topology::default())),
        ("small", index(1234, &small)),
        ("isolated", index(7, &isolated)),
    ]
}

#[test]
fn adjacency_format_golden_vectors() {
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    for (name, expected) in golden_cases() {
        let path = golden_dir().join(format!("{name}.bin"));
        if update {
            std::fs::create_dir_all(golden_dir()).expect("golden directory");
            std::fs::write(&path, expected.encode()).expect("golden written");
        }
        let bytes = std::fs::read(&path).unwrap_or_else(|err| {
            panic!(
                "{}: {err} (generate with UPDATE_GOLDEN=1 cargo test -p loams-graph --test adjacency)",
                path.display()
            )
        });
        let decoded = AdjacencyIndex::decode(&bytes).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(
            decoded, expected,
            "{name}: frozen bytes decode to the index"
        );
        assert_eq!(
            expected.encode(),
            bytes,
            "{name}: the index encodes to the frozen bytes (version 1 changed?)"
        );
        // The topology round-trips and re-encodes to the same bytes.
        let again =
            AdjacencyIndex::from_topology(graph_id(), decoded.covered_offset, &decoded.topology())
                .expect("index");
        assert_eq!(again.encode(), bytes, "{name}: canonical");
    }
    // Non-canonical bytes with a good checksum are refused: swap the small file's two edge types.
    let mut bytes = std::fs::read(golden_dir().join("small.bin")).expect("small");
    let at = bytes
        .windows(8)
        .position(|w| w == b"MENTIONS")
        .expect("the type table");
    bytes[at..at + 8].copy_from_slice(b"ZENTIONS");
    let body = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    assert!(
        AdjacencyIndex::decode(&bytes).is_err(),
        "an out-of-order type table"
    );
    // Another version is unknown, not malformed.
    let mut bytes = std::fs::read(golden_dir().join("small.bin")).expect("small");
    bytes[8] = 2;
    assert!(matches!(
        AdjacencyIndex::decode(&bytes),
        Err(loams_graph::adjacency::format::FormatError::UnknownVersion(
            2
        ))
    ));
}

// ---------------------------------------------------------------------------------------------
// incremental_build_equals_full_build
// ---------------------------------------------------------------------------------------------

/// One step of a random history, resolved against the model of what exists.
#[derive(Debug, Clone)]
enum Step {
    AddNode {
        labels: u8,
        key: u8,
    },
    Relabel {
        pick: u16,
        labels: u8,
        key: u8,
    },
    SetKey {
        pick: u16,
        key: u8,
    },
    SetOther {
        pick: u16,
    },
    AddEdge {
        a: u16,
        b: u16,
        ty: bool,
        weight: u8,
    },
    SetWeight {
        pick: u16,
        weight: u8,
    },
    DeleteEdge {
        pick: u16,
    },
    DeleteNode {
        pick: u16,
    },
    Schema,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (any::<u8>(), any::<u8>()).prop_map(|(labels, key)| Step::AddNode { labels, key }),
        1 => (any::<u16>(), any::<u8>(), any::<u8>()).prop_map(|(pick, labels, key)| Step::Relabel { pick, labels, key }),
        1 => (any::<u16>(), any::<u8>()).prop_map(|(pick, key)| Step::SetKey { pick, key }),
        1 => any::<u16>().prop_map(|pick| Step::SetOther { pick }),
        4 => (any::<u16>(), any::<u16>(), any::<bool>(), any::<u8>()).prop_map(|(a, b, ty, weight)| Step::AddEdge { a, b, ty, weight }),
        1 => (any::<u16>(), any::<u8>()).prop_map(|(pick, weight)| Step::SetWeight { pick, weight }),
        1 => any::<u16>().prop_map(|pick| Step::DeleteEdge { pick }),
        1 => any::<u16>().prop_map(|pick| Step::DeleteNode { pick }),
        1 => Just(Step::Schema),
    ]
}

fn labels_of(bits: u8) -> Vec<String> {
    ["Chunk", "Entity", "Topic"]
        .iter()
        .enumerate()
        .filter(|(i, _)| bits & (1 << i) != 0)
        .map(|(_, l)| (*l).to_string())
        .collect()
}

fn key_of(key: u8) -> Option<Value> {
    match key % 4 {
        0 => None,
        1 => Some(Value::Int64(i64::from(key) - 100)),
        2 => Some(Value::String(format!("k{key}").into())),
        _ => Some(Value::Float64(1.5)), // not a key: no `_key` in the index
    }
}

fn weight_of(weight: u8) -> Option<Value> {
    match weight % 4 {
        0 => None,
        1 => Some(Value::Int64(i64::from(weight))),
        2 => Some(Value::Float64(f64::from(weight) / 8.0)),
        _ => Some(Value::String("heavy".into())),
    }
}

#[derive(Default)]
struct Model {
    next: u64,
    nodes: BTreeMap<u64, ()>,
    edges: BTreeMap<u64, (u64, u64)>,
}

impl Model {
    fn pick<T: Copy>(map: &BTreeMap<u64, T>, pick: u16) -> Option<u64> {
        (!map.is_empty()).then(|| {
            *map.keys()
                .nth(usize::from(pick) % map.len())
                .expect("in range")
        })
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next * 3
    }

    /// The operations of one step, in canonical order, or none.
    fn ops(&mut self, step: &Step) -> Option<Vec<ChangeOp>> {
        let props = |key: Option<Value>| -> PropMap {
            let mut props = PropMap::new();
            if let Some(key) = key {
                props.insert("_key".to_string(), key);
            }
            props.insert("name".to_string(), Value::String("x".into()));
            props
        };
        Some(match step {
            Step::AddNode { labels, key } => {
                let id = self.id();
                self.nodes.insert(id, ());
                vec![ChangeOp::UpsertNode {
                    id,
                    labels: labels_of(*labels),
                    props: props(key_of(*key)),
                }]
            }
            Step::Relabel { pick, labels, key } => vec![ChangeOp::UpsertNode {
                id: Self::pick(&self.nodes, *pick)?,
                labels: labels_of(*labels),
                props: props(key_of(*key)),
            }],
            Step::SetKey { pick, key } => {
                let id = Self::pick(&self.nodes, *pick)?;
                let (set, removed) = match key_of(*key) {
                    Some(value) => (PropMap::from([("_key".to_string(), value)]), Vec::new()),
                    None => (PropMap::new(), vec!["_key".to_string()]),
                };
                vec![ChangeOp::SetProps {
                    id,
                    kind: ElementKind::Node,
                    set,
                    removed,
                }]
            }
            Step::SetOther { pick } => vec![ChangeOp::SetProps {
                id: Self::pick(&self.nodes, *pick)?,
                kind: ElementKind::Node,
                set: PropMap::from([("name".to_string(), Value::String("y".into()))]),
                removed: Vec::new(),
            }],
            Step::AddEdge { a, b, ty, weight } => {
                let (src, dst) = (Self::pick(&self.nodes, *a)?, Self::pick(&self.nodes, *b)?);
                let id = self.id();
                self.edges.insert(id, (src, dst));
                let mut props = PropMap::new();
                if let Some(w) = weight_of(*weight) {
                    props.insert("weight".to_string(), w);
                }
                vec![ChangeOp::UpsertEdge {
                    id,
                    ty: if *ty { "RELATED" } else { "MENTIONS" }.to_string(),
                    src,
                    dst,
                    props,
                }]
            }
            Step::SetWeight { pick, weight } => {
                let id = Self::pick(&self.edges, *pick)?;
                let (set, removed) = match weight_of(*weight) {
                    Some(w) => (PropMap::from([("weight".to_string(), w)]), Vec::new()),
                    None => (PropMap::new(), vec!["weight".to_string()]),
                };
                vec![ChangeOp::SetProps {
                    id,
                    kind: ElementKind::Edge,
                    set,
                    removed,
                }]
            }
            Step::DeleteEdge { pick } => {
                let id = Self::pick(&self.edges, *pick)?;
                self.edges.remove(&id);
                vec![ChangeOp::DeleteEdge { id }]
            }
            Step::DeleteNode { pick } => {
                let id = Self::pick(&self.nodes, *pick)?;
                let incident: Vec<u64> = self
                    .edges
                    .iter()
                    .filter(|(_, (s, d))| *s == id || *d == id)
                    .map(|(e, _)| *e)
                    .collect();
                let mut ops: Vec<ChangeOp> = incident
                    .iter()
                    .map(|e| {
                        self.edges.remove(e);
                        ChangeOp::DeleteEdge { id: *e }
                    })
                    .collect();
                self.nodes.remove(&id);
                ops.push(ChangeOp::DeleteNode { id });
                ops
            }
            Step::Schema => return None,
        })
    }
}

fn change_set(seq: u64, ops: Vec<ChangeOp>, link: bool) -> GraphRecord {
    GraphRecord::ChangeSet(GraphChangeSet {
        graph: graph_id(),
        lease_epoch: 1,
        commit_epoch: seq,
        txn_seq: seq,
        idempotency_key: None,
        link: link.then(|| LinkApply {
            link: LinkId(1 << 63 | 4),
            version: seq,
            applied_after: BTreeMap::from([(0, seq)]),
        }),
        ops,
    })
}

fn offset_record(offset: u64, record: &GraphRecord) -> OffsetRecord {
    OffsetRecord {
        offset,
        record: Record {
            key: None,
            value: Some(encode(record).expect("encodes")),
            headers: Vec::new(),
            timestamp_ms: 0,
        },
    }
}

/// The full build of a fresh engine with `records` replayed into it.
fn full_after(records: &[GraphRecord]) -> AdjacencyIndex {
    let db = grafeo::GrafeoDB::new_in_memory();
    for record in records {
        if let GraphRecord::ChangeSet(change) = record {
            apply_to_store(&db, &change.ops).expect("replays");
        }
    }
    AdjacencyIndex::from_topology(graph_id(), records.len() as u64, &build::scan(&db))
        .expect("index")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    #[test]
    fn incremental_build_equals_full_build(
        steps in prop::collection::vec((step(), any::<bool>()), 1..60),
        cut in any::<prop::sample::Index>(),
        second in any::<prop::sample::Index>(),
    ) {
        let mut model = Model::default();
        let mut records = Vec::new();
        for (step, link) in &steps {
            let seq = records.len() as u64 + 1;
            match model.ops(step) {
                Some(ops) => records.push(change_set(seq, ops, *link)),
                None if matches!(step, Step::Schema) => records.push(GraphRecord::Schema(SchemaChange {
                    graph: graph_id(),
                    lease_epoch: 1,
                    commit_epoch: seq,
                    txn_seq: seq,
                    idempotency_key: None,
                    statement: "CREATE NODE TYPE T (name STRING)".to_string(),
                    language: QueryLanguage::Gql,
                })),
                None => {}
            }
        }
        let n = records.len();
        let offsets: Vec<OffsetRecord> = records.iter().enumerate().map(|(i, r)| offset_record(i as u64, r)).collect();
        let full = full_after(&records);
        // Patch from a cut point, and again through a second cut: both are the full build.
        let k = cut.index(n + 1);
        let base = full_after(&records[..k]);
        let tail = Tail::decode(k as u64, n as u64, &offsets[k..]).expect("decodes");
        let patched = patch(&base, &tail).expect("patches").index;
        prop_assert_eq!(&patched.encode(), &full.encode());
        prop_assert_eq!(sha256_hex(&patched.encode()), sha256_hex(&full.encode()));
        let j = k + second.index(n - k + 1);
        let first = patch(&base, &Tail::decode(k as u64, j as u64, &offsets[k..j]).expect("decodes")).expect("patches").index;
        let twice = patch(&first, &Tail::decode(j as u64, n as u64, &offsets[j..]).expect("decodes")).expect("patches").index;
        prop_assert_eq!(twice.encode(), full.encode());
    }
}

#[test]
fn a_tail_with_a_gap_or_from_elsewhere_is_declined() {
    let records = [
        change_set(1, Vec::new(), false),
        change_set(2, Vec::new(), false),
    ];
    let offsets: Vec<OffsetRecord> = records
        .iter()
        .enumerate()
        .map(|(i, r)| offset_record(i as u64 + 5, r))
        .collect();
    assert_eq!(Tail::decode(4, 7, &offsets).err(), Some(Decline::Trimmed));
    assert_eq!(Tail::decode(5, 8, &offsets).err(), Some(Decline::Trimmed));
    let mut unknown = offsets.clone();
    let mut bytes = unknown[1].record.value.clone().expect("value").to_vec();
    bytes[0] = 9;
    unknown[1].record.value = Some(Bytes::from(bytes));
    assert_eq!(
        Tail::decode(5, 7, &unknown).err(),
        Some(Decline::UnknownVersion)
    );
    let base = full_after(&[]);
    let tail = Tail::decode(5, 7, &offsets).expect("decodes");
    assert_eq!(
        patch(&base, &tail).err(),
        Some(Decline::Trimmed),
        "the tail must start at the index"
    );
}

// ---------------------------------------------------------------------------------------------
// The cluster tests
// ---------------------------------------------------------------------------------------------

/// A one-node cluster with `config`, and graph `kg` written `writes` times.
async fn one_node(
    config: AdjacencyConfig,
    policy: Option<CheckpointPolicy>,
    writes: u64,
) -> Cluster {
    let mut cluster = Cluster::start_with(
        1,
        OwnerConfig::default(),
        Store::in_memory(),
        loams_graph::durable::GroupCommit::default(),
        policy,
    )
    .await;
    let node = &mut cluster.nodes[0];
    node.admin = node.admin.clone().with_adjacency_config(config);
    create(&cluster.node(1).admin, "kg").await;
    write(&cluster, 1, 0, writes).await;
    cluster
}

/// Writes `count` vertices `k = from..`, each with a `_key` and an edge to the previous one.
async fn write(
    cluster: &Cluster,
    node: u64,
    from: u64,
    count: u64,
) -> Option<loams_graph::durable::ConsistencyToken> {
    let mut last = None;
    for k in from..from + count {
        let statement = if k == 0 {
            "INSERT (:N {_key: 0, k: 0, v: 'v'})".to_string()
        } else {
            format!(
                "MATCH (p:N {{k: {}}}) INSERT (p)-[:NEXT {{weight: {k}}}]->(:N {{_key: {k}, k: {k}, v: 'v'}})",
                k - 1
            )
        };
        last = Some(token(
            &run(&cluster.node(node).admin, "kg", &statement).await,
        ));
    }
    last
}

async fn namespace(cluster: &Cluster, node: u64) -> loams_common::NamespaceId {
    cluster
        .node(node)
        .admin
        .namespace_of("acme", "kg")
        .await
        .expect("namespace")
}

async fn graph(cluster: &Cluster, node: u64) -> Arc<loams_graph::Graph> {
    cluster
        .node(node)
        .admin
        .open("acme", "kg")
        .await
        .expect("open")
}

async fn pin(cluster: &Cluster, node: u64) -> loams_graph::adjacency::PinnedAdjacency {
    let admin = &cluster.node(node).admin;
    admin
        .indexer()
        .expect("indexer")
        .pin(
            graph(cluster, node).await,
            namespace(cluster, node).await,
            &Consistency::Strong,
            Duration::from_secs(5),
        )
        .await
        .expect("pin")
}

/// The full build of the graph on `node` now.
async fn full(cluster: &Cluster, node: u64) -> AdjacencyIndex {
    let g = graph(cluster, node).await;
    tokio::task::spawn_blocking(move || build::build_full(&g).expect("full build").0)
        .await
        .expect("join")
}

fn stats(cluster: &Cluster, node: u64) -> loams_graph::adjacency::AdjacencySnapshot {
    cluster
        .node(node)
        .admin
        .indexer()
        .expect("indexer")
        .stats()
        .snapshot()
}

async fn manifest(cluster: &Cluster) -> loams_graph::snapshot::GraphManifest {
    let graph = cluster.graph_id("kg").await;
    cluster
        .node(1)
        .admin
        .checkpointer()
        .expect("checkpointer")
        .manifests()
        .load(namespace(cluster, 1).await, graph)
        .await
        .expect("load")
        .expect("a manifest")
        .manifest
}

async fn adj_files(cluster: &Cluster) -> BTreeSet<String> {
    let graph = cluster.graph_id("kg").await;
    let prefix = format!(
        "{}{}",
        loams_graph::snapshot::graph_prefix(namespace(cluster, 1).await, graph),
        publish::ADJ_DIR
    );
    cluster
        .store
        .list(&prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|o| o.path)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_publish_is_create_only_and_cas_monotonic() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let id = cluster.graph_id("kg").await;
    let owner = cluster.preferred(id);
    write(&cluster, owner, 0, 3).await;
    let ns = namespace(&cluster, owner).await;
    let admin = cluster.node(owner).admin.clone();
    let first = admin.build_adjacency("acme", "kg").await.expect("first");
    assert_eq!(
        (first.mode, first.reason, first.covered_offset),
        (BuildMode::Full, Some(Decline::NoIndex), 3)
    );
    let older = full(&cluster, owner).await;
    write(&cluster, owner, 3, 2).await;
    let second = admin.build_adjacency("acme", "kg").await.expect("second");
    assert_eq!((second.covered_offset, second.published), (5, true));
    let manifests = admin
        .checkpointer()
        .expect("checkpointer")
        .manifests()
        .clone();
    let held = cluster.node(owner).ownership.held(id).expect("held");
    let fence = Fence {
        lease: owner_lease_key(id),
        epoch: held,
    };

    // An index at a lower offset never replaces a higher one: the build is dropped, its file left.
    let dropped = publish::publish(
        &manifests,
        ns,
        id,
        Some(&fence),
        &older,
        older.encode(),
        AdjacencyMode::Full,
        2,
    )
    .await
    .expect("publish");
    assert!(!dropped.advanced);
    let m = manifest(&cluster).await;
    assert_eq!(m.adjacency.as_ref().map(|e| e.covered_offset), Some(5));
    assert_eq!(
        m.adjacency_previous
            .iter()
            .map(|e| e.covered_offset)
            .collect::<Vec<_>>(),
        [3]
    );
    // Create-only: publishing the same bytes again is the same file; other bytes under its name
    // are refused by the bucket.
    let again = publish::publish(
        &manifests,
        ns,
        id,
        Some(&fence),
        &older,
        older.encode(),
        AdjacencyMode::Full,
        2,
    )
    .await
    .expect("publish again");
    assert_eq!(again.entry.key, dropped.entry.key);
    assert!(matches!(
        cluster
            .store
            .put_if_absent(&dropped.entry.key, Bytes::from_static(b"other"))
            .await,
        Err(loams_store::StoreError::AlreadyExists { .. })
    ));
    // The file of the previous index (offset 3) is the dropped build's own bytes: the same name.
    assert!(m.adjacency_keys().contains(dropped.entry.key.as_str()));

    // A lost CAS: a build at offset 4 loses to the published 5; its file is unreferenced and the
    // sweep removes it once a later manifest is written.
    let at_four = AdjacencyIndex::from_topology(id, 4, &older.topology()).expect("index");
    let lost = publish::publish(
        &manifests,
        ns,
        id,
        Some(&fence),
        &at_four,
        at_four.encode(),
        AdjacencyMode::Full,
        2,
    )
    .await
    .expect("publish");
    assert!(!lost.advanced);
    assert!(adj_files(&cluster).await.contains(&lost.entry.key));
    tokio::time::sleep(Duration::from_millis(20)).await;
    write(&cluster, owner, 5, 1).await;
    admin.build_adjacency("acme", "kg").await.expect("third");
    let swept = manifests
        .sweep(ns, id, Duration::ZERO)
        .await
        .expect("sweep");
    assert!(swept >= 1);
    let files = adj_files(&cluster).await;
    let m = manifest(&cluster).await;
    assert!(
        !files.contains(&lost.entry.key),
        "the lost build's file is swept"
    );
    for key in m.adjacency_keys() {
        assert!(files.contains(key), "{key} is named and kept");
    }
    assert_eq!(
        files.len(),
        2,
        "keep = 2: the current index and the one before it"
    );

    // A stale owner's CAS is fenced: the owner leaves, the other node takes the lease.
    cluster
        .node(owner)
        .admin
        .shutdown(Duration::from_secs(5))
        .await;
    cluster.forwarder.cut_off(owner);
    let other = 3 - owner;
    cluster
        .node(other)
        .membership
        .set(vec![cluster.node(other).me.clone()]);
    write(&cluster, other, 6, 1).await;
    let newer = full(&cluster, other).await;
    let stale = publish::publish(
        &manifests,
        ns,
        id,
        Some(&fence),
        &newer,
        newer.encode(),
        AdjacencyMode::Full,
        2,
    )
    .await;
    assert_eq!(stale.err(), Some(SnapshotError::Fenced));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_build_fallback_is_control_flow() {
    let policy = CheckpointPolicy {
        pitr_window: Duration::ZERO,
        ..CheckpointPolicy::default()
    };
    let config = AdjacencyConfig {
        incremental_max_records: 3,
        ..AdjacencyConfig::default()
    };
    let cluster = one_node(config, Some(policy), 4).await;
    let admin = cluster.node(1).admin.clone();
    let id = cluster.graph_id("kg").await;
    let ns = namespace(&cluster, 1).await;
    let manifests = admin
        .checkpointer()
        .expect("checkpointer")
        .manifests()
        .clone();
    let indexer = admin.indexer().expect("indexer").clone();
    let full_of = |reason: Decline| {
        let snapshot = indexer.stats().snapshot();
        snapshot.builds_of("full", Some(reason.as_str()))
    };

    // no_index
    let built = admin
        .build_adjacency("acme", "kg")
        .await
        .expect("no_index is a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::NoIndex))
    );
    assert_eq!(full_of(Decline::NoIndex), 1);

    // tail_too_long: four records behind, three allowed.
    write(&cluster, 1, 4, 4).await;
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::TailTooLong))
    );
    // ... and by fraction: 25 % of 7 edges is under two topology operations.
    write(&cluster, 1, 8, 2).await;
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::TailTooLong))
    );
    assert_eq!(full_of(Decline::TailTooLong), 2);
    // Within both limits it patches.
    write(&cluster, 1, 10, 1).await;
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!((built.mode, built.reason), (BuildMode::Incremental, None));

    // unknown_version: the manifest names a format this build does not read.
    write(&cluster, 1, 11, 1).await;
    manifests
        .update(ns, id, None, |m| {
            if let Some(entry) = &mut m.adjacency {
                entry.format = "loams.graph.adjacency/2".to_string();
            }
            Ok((true, ()))
        })
        .await
        .expect("edit");
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::UnknownVersion))
    );

    // hash_mismatch: the file is not what the manifest's hash names.
    write(&cluster, 1, 12, 1).await;
    manifests
        .update(ns, id, None, |m| {
            if let Some(entry) = &mut m.adjacency {
                entry.sha256 = "0".repeat(64);
            }
            Ok((true, ()))
        })
        .await
        .expect("edit");
    indexer.forget(id);
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::HashMismatch))
    );

    // trimmed: a checkpoint trims the stream past the index.
    write(&cluster, 1, 13, 1).await;
    admin
        .checkpoint(
            "acme",
            "kg",
            &[SnapshotKind::Native, SnapshotKind::Portable],
        )
        .await
        .expect("checkpoint");
    assert!(
        manifest(&cluster).await.log_trim_offset
            > manifest(&cluster)
                .await
                .adjacency
                .expect("index")
                .covered_offset
    );
    let built = admin.build_adjacency("acme", "kg").await.expect("a build");
    assert_eq!(
        (built.mode, built.reason),
        (BuildMode::Full, Some(Decline::Trimmed))
    );

    // Every reason counted once or more; and each build equals the engine's.
    for reason in Decline::ALL {
        assert!(full_of(reason) >= 1, "{reason:?} counted");
    }
    let pinned = pin(&cluster, 1).await;
    assert_eq!(pinned.source, PinSource::Index);
    assert_eq!(pinned.view.encode(), full(&cluster, 1).await.encode());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_overlay_sees_tail_writes() {
    let cluster = one_node(AdjacencyConfig::default(), None, 3).await;
    let first = pin(&cluster, 1).await;
    assert_eq!((first.source, first.offset), (PinSource::Built, 3));
    let acked = write(&cluster, 1, 3, 4).await.expect("token");
    let pinned = pin(&cluster, 1).await;
    assert_eq!(pinned.source, PinSource::Overlay(4));
    assert_eq!(pinned.offset, acked.offset);
    let full = full(&cluster, 1).await;
    assert_eq!(full.covered_offset, acked.offset);
    assert_eq!(
        pinned.view.encode(),
        full.encode(),
        "the overlay is the graph at M"
    );
    // The last write's vertex is in the view.
    assert!(
        pinned
            .view
            .vertices
            .iter()
            .any(|v| v.key == Some(VertexKey::Int(6)))
    );
    // Overlays publish nothing.
    assert_eq!(
        manifest(&cluster)
            .await
            .adjacency
            .expect("index")
            .covered_offset,
        3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_falls_back_when_tail_missing_or_long() {
    let policy = CheckpointPolicy {
        pitr_window: Duration::ZERO,
        ..CheckpointPolicy::default()
    };
    let config = AdjacencyConfig {
        overlay_max_records: 2,
        ..AdjacencyConfig::default()
    };
    let cluster = one_node(config, Some(policy), 2).await;
    assert_eq!(pin(&cluster, 1).await.source, PinSource::Built);
    // Long: three records over two.
    write(&cluster, 1, 2, 3).await;
    let pinned = pin(&cluster, 1).await;
    assert_eq!(pinned.source, PinSource::Fallback(Decline::TailTooLong));
    assert_eq!(pinned.view.encode(), full(&cluster, 1).await.encode());
    assert_eq!(
        manifest(&cluster)
            .await
            .adjacency
            .expect("published")
            .covered_offset,
        5,
        "the fallback publishes"
    );
    // Missing: the stream is trimmed past the index.
    write(&cluster, 1, 5, 1).await;
    cluster
        .node(1)
        .admin
        .checkpoint(
            "acme",
            "kg",
            &[SnapshotKind::Native, SnapshotKind::Portable],
        )
        .await
        .expect("checkpoint");
    let pinned = pin(&cluster, 1).await;
    assert_eq!(pinned.source, PinSource::Fallback(Decline::Trimmed));
    assert_eq!(pinned.view.encode(), full(&cluster, 1).await.encode());
    let s = stats(&cluster, 1);
    assert_eq!(s.fallbacks.get("tail_too_long"), Some(&1));
    assert_eq!(s.fallbacks.get("trimmed"), Some(&1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_path_holds_no_gate() {
    let cluster = Arc::new(one_node(AdjacencyConfig::default(), None, 3).await);
    pin(&cluster, 1).await;
    write(&cluster, 1, 3, 1).await;
    let acked = Arc::new(std::sync::Mutex::new(None));
    let hook: loams_graph::catalog::AckHook = {
        let (cluster, acked) = (Arc::clone(&cluster), Arc::clone(&acked));
        Arc::new(move || {
            let (cluster, acked) = (Arc::clone(&cluster), Arc::clone(&acked));
            Box::pin(async move {
                // A write while the reader is on its index path: acknowledged, not blocked.
                let write = tokio::time::timeout(
                    Duration::from_secs(10),
                    cluster.node(1).admin.execute(common::request(
                        "kg",
                        "INSERT (:N {_key: 100, k: 100, v: 'during'})",
                        "",
                    )),
                )
                .await;
                let response = write
                    .expect("acknowledged while the algorithm ran")
                    .expect("written");
                *acked.lock().expect("lock") = Some(token(&response));
                false
            })
        })
    };
    let indexer = cluster.node(1).admin.indexer().expect("indexer").clone();
    indexer.set_pin_hook(Some(hook));
    let pinned = pin(&cluster, 1).await;
    indexer.set_pin_hook(None);
    assert_eq!(pinned.source, PinSource::Overlay(1));
    let during = acked.lock().expect("lock").expect("the hook ran");
    assert!(
        during.offset > pinned.offset,
        "the write landed after the pin"
    );
    assert!(
        !pinned
            .view
            .vertices
            .iter()
            .any(|v| v.key == Some(VertexKey::Int(100))),
        "and is not in the pinned view"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_survives_owner_move() {
    let cluster = Cluster::start(2, OwnerConfig::default()).await;
    create(&cluster.node(1).admin, "kg").await;
    let id = cluster.graph_id("kg").await;
    let old = cluster.preferred(id);
    let new = 3 - old;
    write(&cluster, old, 0, 4).await;
    assert_eq!(pin(&cluster, old).await.source, PinSource::Built);
    cluster
        .node(old)
        .admin
        .shutdown(Duration::from_secs(5))
        .await;
    cluster.forwarder.cut_off(old);
    cluster
        .node(new)
        .membership
        .set(vec![cluster.node(new).me.clone()]);
    write(&cluster, new, 4, 1).await;
    let pinned = pin(&cluster, new).await;
    assert_eq!(
        pinned.source,
        PinSource::Overlay(1),
        "the published index plus the new write"
    );
    assert_eq!(
        stats(&cluster, new).builds_total(),
        0,
        "the new owner rebuilds nothing"
    );
    assert_eq!(pinned.view.encode(), full(&cluster, new).await.encode());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_without_algorithms_has_no_index() {
    let config = AdjacencyConfig {
        rebuild_after_records: 2,
        incremental_max_fraction: 10.0,
        ..AdjacencyConfig::default()
    };
    let cluster = one_node(config, None, 5).await;
    let admin = cluster.node(1).admin.clone();
    admin.checkpoint_due().await;
    assert_eq!(
        manifest_or_none(&cluster).await.and_then(|m| m.adjacency),
        None
    );
    assert!(adj_files(&cluster).await.is_empty());
    assert_eq!(stats(&cluster, 1).builds_total(), 0);
    // After an algorithm, the maintenance pass keeps it within `rebuild_after_records`.
    pin(&cluster, 1).await;
    write(&cluster, 1, 5, 2).await;
    admin.checkpoint_due().await;
    assert_eq!(
        manifest(&cluster)
            .await
            .adjacency
            .expect("index")
            .covered_offset,
        5,
        "two behind: not yet"
    );
    write(&cluster, 1, 7, 1).await;
    admin.checkpoint_due().await;
    let entry = manifest(&cluster).await.adjacency.expect("index");
    assert_eq!(
        (entry.covered_offset, entry.mode),
        (8, AdjacencyMode::Incremental)
    );
}

async fn manifest_or_none(cluster: &Cluster) -> Option<loams_graph::snapshot::GraphManifest> {
    let graph = cluster.graph_id("kg").await;
    cluster
        .node(1)
        .admin
        .checkpointer()
        .expect("checkpointer")
        .manifests()
        .load(namespace(cluster, 1).await, graph)
        .await
        .expect("load")
        .map(|l| l.manifest)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adjacency_counters_reported() {
    let config = AdjacencyConfig {
        overlay_max_records: 3,
        incremental_max_fraction: 10.0,
        ..AdjacencyConfig::default()
    };
    let cluster = one_node(config, None, 4).await;
    let admin = cluster.node(1).admin.clone();
    let id = cluster.graph_id("kg").await;
    pin(&cluster, 1).await; // full, no_index: 3 edges scanned
    write(&cluster, 1, 4, 2).await;
    pin(&cluster, 1).await; // overlay of 2 records, 2 edge operations
    admin
        .build_adjacency("acme", "kg")
        .await
        .expect("incremental"); // 2 more delta edges
    write(&cluster, 1, 6, 4).await;
    pin(&cluster, 1).await; // fallback, tail_too_long: 9 edges scanned
    let s = stats(&cluster, 1);
    assert_eq!(s.builds_of("full", Some("no_index")), 1);
    assert_eq!(s.builds_of("full", Some("tail_too_long")), 1);
    assert_eq!(s.builds_of("incremental", None), 1);
    assert_eq!(
        s.delta_edges, 4,
        "delta edges from the overlay and the incremental build"
    );
    assert_eq!(s.scanned_edges, 3 + 9, "edges scanned by the full builds");
    assert_eq!(s.overlay_records, [2]);
    assert_eq!(s.fallbacks.get("tail_too_long"), Some(&1));
    assert_eq!(s.lag.get(&id), Some(&0));
    write(&cluster, 1, 10, 1).await;
    admin.checkpoint_due().await;
    assert_eq!(
        stats(&cluster, 1).lag.get(&id),
        Some(&1),
        "the maintenance pass reports the lag"
    );
}
