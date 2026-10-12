//! GR1 Task 10: the `GraphRecord` codec (design §48 §6.4).
//!
//! The golden vectors in `conformance/graph/records/v1/` are frozen bytes: a build that decodes
//! them differently, or encodes the same records to other bytes, has changed version 1 of the
//! format, which the graph's log and every snapshot's replay tail depend on. Regenerate them only
//! for a new case, never to make a changed encoder pass:
//! `UPDATE_GOLDEN=1 cargo test -p loams-graph --test codec golden_vectors_decode`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use grafeo::{Config, GrafeoDB, Value};
use grafeo_common::types::{Date, Duration, PropertyKey, Time, Timestamp, ZonedDatetime};
use loams_common::meta::LinkId;
use loams_graph::GraphId;
use loams_graph::changeset::capture::{CdcCapture, ChangeCapture};
use loams_graph::changeset::codec::MAX_VALUE_DEPTH;
use loams_graph::changeset::{
    ChangeOp, CodecError, ElementKind, GraphChangeSet, GraphRecord, LinkApply, PropMap,
    RECORD_VERSION, SchemaChange, apply_to_store, decode, encode,
};
use loams_proto::loams::graph::v1::QueryLanguage;
use proptest::prelude::*;

// ---------------------------------------------------------------------------------------------
// Golden vectors
// ---------------------------------------------------------------------------------------------

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/graph/records/v1")
}

fn graph() -> GraphId {
    GraphId(ulid::Ulid::from_parts(
        1_760_054_400_000,
        0x0123_4567_89ab_cdef_0123,
    ))
}

fn props(entries: Vec<(&str, Value)>) -> PropMap {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn map(entries: Vec<(&str, Value)>) -> Value {
    Value::Map(Arc::new(
        entries
            .into_iter()
            .map(|(key, value)| (PropertyKey::new(key), value))
            .collect(),
    ))
}

fn counts(entries: &[(&str, u64)]) -> Arc<HashMap<String, u64>> {
    Arc::new(entries.iter().map(|(k, v)| (k.to_string(), *v)).collect())
}

/// Every value kind the typed model has (GR1 Task 2), at its edges where it has edges.
fn every_value_kind() -> PropMap {
    props(vec![
        ("a_null", Value::Null),
        ("b_bool", Value::Bool(true)),
        ("c_int_min", Value::Int64(i64::MIN)),
        ("c_int_max", Value::Int64(i64::MAX)),
        ("c_int_neg", Value::Int64(-1)),
        ("d_float", Value::Float64(1.5)),
        ("d_float_inf", Value::Float64(f64::INFINITY)),
        ("d_float_neg_zero", Value::Float64(-0.0)),
        ("e_string", Value::String("Grüße, 世界".into())),
        ("f_bytes", Value::Bytes(Arc::from(&[0u8, 255, 7][..]))),
        (
            "g_date",
            Value::Date(Date::from_ymd(2026, 10, 10).expect("a date")),
        ),
        (
            "g_date_before_epoch",
            Value::Date(Date::from_days(-719_162)),
        ),
        (
            "h_local_time",
            Value::Time(Time::from_hms_nano(23, 59, 59, 999_999_999).expect("a time")),
        ),
        (
            "i_zoned_time",
            Value::Time(
                Time::from_hms(9, 30, 0)
                    .expect("a time")
                    .with_offset(-64_800),
            ),
        ),
        (
            "j_local_datetime",
            Value::Timestamp(Timestamp::from_micros(1_760_054_400_123_456)),
        ),
        (
            "k_zoned_datetime",
            Value::ZonedDatetime(ZonedDatetime::from_timestamp_offset(
                Timestamp::from_micros(-1),
                19_800,
            )),
        ),
        (
            "l_duration",
            Value::Duration(Duration::new(-14, 3, 1_000_000_001)),
        ),
        (
            "m_list",
            Value::List(Arc::from(vec![
                Value::Int64(1),
                Value::String("two".into()),
                Value::List(Arc::from(Vec::<Value>::new())),
            ])),
        ),
        (
            "n_map",
            map(vec![
                ("inner", map(vec![("x", Value::Bool(false))])),
                ("k", Value::Null),
            ]),
        ),
        (
            "o_vector",
            Value::Vector(Arc::from(&[0.25f32, -1.0, f32::MAX][..])),
        ),
        (
            "p_path",
            Value::Path {
                nodes: Arc::from(vec![Value::Int64(0), Value::Int64(1)]),
                edges: Arc::from(vec![Value::Int64(7)]),
            },
        ),
        (
            "q_gcounter",
            Value::GCounter(counts(&[("r1", 3), ("r2", 0)])),
        ),
        (
            "r_oncounter",
            Value::OnCounter {
                pos: counts(&[("r1", 5)]),
                neg: counts(&[("r2", 2)]),
            },
        ),
    ])
}

/// The frozen cases: file name and the record its bytes are.
fn golden_cases() -> Vec<(&'static str, GraphRecord)> {
    vec![
        (
            "changeset_empty",
            GraphRecord::ChangeSet(GraphChangeSet {
                graph: graph(),
                lease_epoch: 1,
                commit_epoch: 0,
                txn_seq: 0,
                idempotency_key: None,
                link: None,
                ops: Vec::new(),
            }),
        ),
        (
            "changeset_ops",
            GraphRecord::ChangeSet(GraphChangeSet {
                graph: graph(),
                lease_epoch: 3,
                commit_epoch: 41,
                txn_seq: 17,
                idempotency_key: Some("req-42".into()),
                link: Some(LinkApply {
                    link: LinkId(7),
                    version: 3,
                    applied_after: BTreeMap::from([(0, 100), (3, u64::MAX)]),
                }),
                ops: vec![
                    ChangeOp::UpsertNode {
                        id: 0,
                        labels: vec!["Admin".into(), "Person".into()],
                        props: props(vec![
                            ("age", Value::Int64(30)),
                            ("name", Value::String("Alix".into())),
                        ]),
                    },
                    ChangeOp::UpsertNode {
                        id: 1,
                        labels: Vec::new(),
                        props: PropMap::new(),
                    },
                    ChangeOp::UpsertEdge {
                        id: 0,
                        ty: "KNOWS".into(),
                        src: 0,
                        dst: 1,
                        props: props(vec![(
                            "since",
                            Value::Date(Date::from_ymd(2020, 2, 29).expect("a date")),
                        )]),
                    },
                    ChangeOp::SetProps {
                        id: 1,
                        kind: ElementKind::Node,
                        set: props(vec![("x", Value::Int64(1))]),
                        removed: vec!["y".into()],
                    },
                    ChangeOp::SetProps {
                        id: 0,
                        kind: ElementKind::Edge,
                        set: PropMap::new(),
                        removed: vec!["w".into()],
                    },
                    ChangeOp::DeleteEdge { id: 9 },
                    ChangeOp::DeleteNode { id: u64::MAX },
                ],
            }),
        ),
        (
            "changeset_values",
            GraphRecord::ChangeSet(GraphChangeSet {
                graph: graph(),
                lease_epoch: 2,
                commit_epoch: 5,
                txn_seq: 4,
                idempotency_key: None,
                link: None,
                ops: vec![ChangeOp::UpsertNode {
                    id: 300,
                    labels: vec!["Every".into()],
                    props: every_value_kind(),
                }],
            }),
        ),
        (
            "schema_change",
            GraphRecord::Schema(SchemaChange {
                graph: graph(),
                lease_epoch: 2,
                commit_epoch: 5,
                txn_seq: 9,
                idempotency_key: Some("ddl-1".into()),
                statement: "CREATE NODE TYPE Person (name STRING)".into(),
                language: QueryLanguage::Gql,
            }),
        ),
    ]
}

fn golden_bytes(name: &str) -> Vec<u8> {
    let path = golden_dir().join(format!("{name}.bin"));
    std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "{}: {err} (generate with UPDATE_GOLDEN=1 cargo test -p loams-graph --test codec)",
            path.display()
        )
    })
}

#[test]
fn golden_vectors_decode() {
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    for (name, expected) in golden_cases() {
        if update {
            let path = golden_dir().join(format!("{name}.bin"));
            std::fs::create_dir_all(golden_dir()).expect("golden directory");
            std::fs::write(&path, encode(&expected).expect("encodes")).expect("golden written");
        }
        let bytes = golden_bytes(name);
        assert_eq!(bytes[0], RECORD_VERSION, "{name}: version byte");
        let decoded = decode(&bytes).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(
            decoded, expected,
            "{name}: frozen bytes decode to the record"
        );
        assert_eq!(
            encode(&expected).expect("encodes").as_ref(),
            bytes.as_slice(),
            "{name}: the record encodes to the frozen bytes (version 1 changed?)"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------------------------

#[test]
fn unknown_version_refused() {
    assert_eq!(decode(&[]), Err(CodecError::Empty));
    let mut bytes = golden_bytes("changeset_ops");
    for version in [0u8, 2, 0x7f, 0xff] {
        bytes[0] = version;
        assert_eq!(decode(&bytes), Err(CodecError::UnknownVersion(version)));
    }
}

#[test]
fn truncated_input_refused_not_panics() {
    for (name, _) in golden_cases() {
        let bytes = golden_bytes(name);
        for end in 1..bytes.len() {
            match decode(&bytes[..end]) {
                Err(CodecError::Truncated { at }) => assert!(at <= end, "{name}[..{end}]: at {at}"),
                other => panic!("{name}[..{end}]: expected Truncated, got {other:?}"),
            }
        }
    }
}

#[test]
fn trailing_bytes_refused() {
    let mut bytes = golden_bytes("changeset_empty");
    let end = bytes.len();
    bytes.push(0);
    assert_eq!(
        decode(&bytes),
        Err(CodecError::Malformed {
            at: end,
            what: "1 trailing bytes after the record".into()
        })
    );
}

#[test]
fn non_canonical_encodings_refused() {
    // `changeset_empty` is: version, kind 1, the graph id (16 bytes), lease epoch 1, commit epoch
    // 0, txn_seq 0, no key, no link, no ops; each number one varint byte.
    let bytes = golden_bytes("changeset_empty");
    assert_eq!(
        bytes.len(),
        1 + 1 + 16 + 3 + 2 + 1,
        "layout of changeset_empty"
    );
    let lease = 18;
    assert_eq!(bytes[lease], 1);

    // An overlong varint (1 as two bytes).
    let mut overlong = bytes.clone();
    overlong.splice(lease..=lease, [0x81, 0x00]);
    assert!(
        matches!(decode(&overlong), Err(CodecError::Malformed { at, .. }) if at == lease),
        "{:?}",
        decode(&overlong)
    );

    // An option tag that is neither 0 nor 1.
    let mut option = bytes.clone();
    option[lease + 3] = 2;
    assert!(matches!(decode(&option), Err(CodecError::Malformed { .. })));

    // A record kind that does not exist.
    let mut kind = bytes.clone();
    kind[1] = 9;
    assert_eq!(
        decode(&kind),
        Err(CodecError::Malformed {
            at: 1,
            what: "unknown record kind 9".into()
        })
    );

    // Property keys out of order, made by swapping the two keys of a two-key node.
    let record = GraphRecord::ChangeSet(GraphChangeSet {
        graph: graph(),
        lease_epoch: 1,
        commit_epoch: 0,
        txn_seq: 0,
        idempotency_key: None,
        link: None,
        ops: vec![ChangeOp::UpsertNode {
            id: 0,
            labels: Vec::new(),
            props: props(vec![("a", Value::Null), ("b", Value::Null)]),
        }],
    });
    let mut swapped = encode(&record).expect("encodes").to_vec();
    let a = swapped.len() - 5; // ... 1 'a' NULL 1 'b' NULL
    assert_eq!(&swapped[a - 1..], &[1, b'a', 0, 1, b'b', 0]);
    swapped[a] = b'b';
    swapped[a + 3] = b'a';
    assert!(
        matches!(decode(&swapped), Err(CodecError::Malformed { ref what, .. }) if what.contains("order")),
        "{:?}",
        decode(&swapped)
    );
}

/// A record whose one property is `value`.
fn holding(value: Value) -> GraphRecord {
    GraphRecord::ChangeSet(GraphChangeSet {
        graph: graph(),
        lease_epoch: 1,
        commit_epoch: 0,
        txn_seq: 0,
        idempotency_key: None,
        link: None,
        ops: vec![ChangeOp::UpsertNode {
            id: 0,
            labels: Vec::new(),
            props: props(vec![("v", value)]),
        }],
    })
}

fn nested(depth: usize) -> Value {
    let mut value = Value::Null;
    for _ in 0..depth {
        value = Value::List(Arc::from(vec![value]));
    }
    value
}

#[test]
fn value_depth_is_bounded_both_ways() {
    let deepest = holding(nested(MAX_VALUE_DEPTH));
    let bytes = encode(&deepest).expect("the deepest allowed value encodes");
    assert_eq!(decode(&bytes).expect("and decodes"), deepest);

    assert!(matches!(
        encode(&holding(nested(MAX_VALUE_DEPTH + 1))),
        Err(CodecError::Unrepresentable(_))
    ));

    // Bytes no encoder wrote: a million nested one-element lists. Refused at the limit, with no
    // stack overflow. The tags are the README's: LIST = 12, NULL = 0.
    let mut hostile = encode(&holding(Value::Null)).expect("encodes").to_vec();
    assert_eq!(hostile.pop(), Some(0));
    for _ in 0..1_000_000 {
        hostile.extend_from_slice(&[12, 1]);
    }
    hostile.push(0);
    assert!(
        matches!(decode(&hostile), Err(CodecError::Malformed { ref what, .. }) if what.contains("deeper")),
        "{:?}",
        decode(&hostile).map(|_| ())
    );
}

#[test]
fn offsets_outside_eighteen_hours_refused() {
    for value in [
        Value::Time(Time::from_hms(1, 0, 0).expect("a time").with_offset(64_801)),
        Value::ZonedDatetime(ZonedDatetime::from_timestamp_offset(
            Timestamp::from_micros(0),
            -64_801,
        )),
    ] {
        assert!(matches!(
            encode(&holding(value)),
            Err(CodecError::Unrepresentable(_))
        ));
    }
    // The decoder refuses what the encoder would not write: the zoned time of
    // `changeset_values` with its offset pushed past the bound.
    let at_bound = encode(&holding(Value::Time(
        Time::from_hms(1, 0, 0)
            .expect("a time")
            .with_offset(-64_800),
    )))
    .expect("encodes")
    .to_vec();
    // -64800 zigzags to 129599 = varint bf f4 07; -64801 to 129601 = c1 f4 07.
    assert_eq!(&at_bound[at_bound.len() - 3..], &[0xbf, 0xf4, 0x07]);
    let mut past = at_bound.clone();
    let n = past.len();
    past[n - 3..].copy_from_slice(&[0xc1, 0xf4, 0x07]);
    assert!(matches!(decode(&past), Err(CodecError::Malformed { .. })));
}

#[test]
fn float_bits_survive() {
    for bits in [
        f64::NAN.to_bits(),
        0x7ff8_0000_dead_beef,
        (-0.0f64).to_bits(),
    ] {
        let bytes = encode(&holding(Value::Float64(f64::from_bits(bits)))).expect("encodes");
        let GraphRecord::ChangeSet(set) = decode(&bytes).expect("decodes") else {
            panic!("a change set")
        };
        let ChangeOp::UpsertNode { props, .. } = &set.ops[0] else {
            panic!("an upsert")
        };
        match props["v"] {
            Value::Float64(f) => assert_eq!(f.to_bits(), bits),
            ref other => panic!("{other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------------------------

const NANOS_PER_DAY: u64 = 86_400_000_000_000;

fn key() -> impl Strategy<Value = String> {
    prop_oneof!["[a-z_]{0,6}", ".{0,4}"]
}

fn counter_map() -> impl Strategy<Value = Arc<HashMap<String, u64>>> {
    prop::collection::hash_map(key(), any::<u64>(), 0..3).prop_map(Arc::new)
}

fn finite_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("NaN is not equal to itself", |f| !f.is_nan())
}

fn offset() -> impl Strategy<Value = i32> {
    -64_800i32..=64_800
}

fn value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Int64),
        finite_f64().prop_map(Value::Float64),
        ".{0,8}".prop_map(|s: String| Value::String(s.as_str().into())),
        prop::collection::vec(any::<u8>(), 0..8).prop_map(|b| Value::Bytes(Arc::from(b))),
        any::<i32>().prop_map(|d| Value::Date(Date::from_days(d))),
        (0..NANOS_PER_DAY).prop_map(|n| Value::Time(Time::from_nanos(n).expect("in range"))),
        (0..NANOS_PER_DAY, offset()).prop_map(|(n, o)| {
            Value::Time(Time::from_nanos(n).expect("in range").with_offset(o))
        }),
        any::<i64>().prop_map(|m| Value::Timestamp(Timestamp::from_micros(m))),
        (any::<i64>(), offset()).prop_map(|(m, o)| {
            Value::ZonedDatetime(ZonedDatetime::from_timestamp_offset(
                Timestamp::from_micros(m),
                o,
            ))
        }),
        (any::<i64>(), any::<i64>(), any::<i64>())
            .prop_map(|(m, d, n)| Value::Duration(Duration::new(m, d, n))),
        prop::collection::vec(any::<f32>().prop_filter("not NaN", |f| !f.is_nan()), 0..4)
            .prop_map(|v| Value::Vector(Arc::from(v))),
        counter_map().prop_map(Value::GCounter),
        (counter_map(), counter_map()).prop_map(|(pos, neg)| Value::OnCounter { pos, neg }),
    ];
    leaf.prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(|v| Value::List(Arc::from(v))),
            prop::collection::btree_map(key(), inner.clone(), 0..4).prop_map(|m| {
                Value::Map(Arc::new(
                    m.into_iter()
                        .map(|(k, v)| (PropertyKey::new(k.as_str()), v))
                        .collect(),
                ))
            }),
            (
                prop::collection::vec(inner.clone(), 0..3),
                prop::collection::vec(inner, 0..3)
            )
                .prop_map(|(nodes, edges)| Value::Path {
                    nodes: Arc::from(nodes),
                    edges: Arc::from(edges),
                }),
        ]
    })
}

fn prop_map() -> impl Strategy<Value = PropMap> {
    prop::collection::btree_map(key(), value(), 0..4)
}

fn names() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(key(), 0..3)
}

fn change_op() -> impl Strategy<Value = ChangeOp> {
    prop_oneof![
        (any::<u64>(), names(), prop_map()).prop_map(|(id, labels, props)| ChangeOp::UpsertNode {
            id,
            labels,
            props
        }),
        any::<u64>().prop_map(|id| ChangeOp::DeleteNode { id }),
        (any::<u64>(), key(), any::<u64>(), any::<u64>(), prop_map()).prop_map(
            |(id, ty, src, dst, props)| ChangeOp::UpsertEdge {
                id,
                ty,
                src,
                dst,
                props
            }
        ),
        any::<u64>().prop_map(|id| ChangeOp::DeleteEdge { id }),
        (any::<u64>(), any::<bool>(), prop_map(), names()).prop_map(|(id, node, set, removed)| {
            ChangeOp::SetProps {
                id,
                kind: if node {
                    ElementKind::Node
                } else {
                    ElementKind::Edge
                },
                set,
                removed,
            }
        }),
    ]
}

fn graph_id() -> impl Strategy<Value = GraphId> {
    any::<u128>().prop_map(|n| GraphId(ulid::Ulid(n)))
}

fn language() -> impl Strategy<Value = QueryLanguage> {
    prop::sample::select(vec![
        QueryLanguage::Unspecified,
        QueryLanguage::Gql,
        QueryLanguage::Cypher,
        QueryLanguage::SqlPgq,
        QueryLanguage::Gremlin,
        QueryLanguage::Graphql,
        QueryLanguage::Sparql,
    ])
}

fn link() -> impl Strategy<Value = Option<LinkApply>> {
    prop::option::of(
        (
            any::<u64>(),
            any::<u64>(),
            prop::collection::btree_map(any::<u32>(), any::<u64>(), 0..4),
        )
            .prop_map(|(link, version, applied_after)| LinkApply {
                link: LinkId(link),
                version,
                applied_after,
            }),
    )
}

fn record() -> impl Strategy<Value = GraphRecord> {
    let change_set = (
        graph_id(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        prop::option::of(".{0,12}"),
        link(),
        prop::collection::vec(change_op(), 0..6),
    )
        .prop_map(
            |(graph, lease_epoch, commit_epoch, txn_seq, idempotency_key, link, ops)| {
                GraphRecord::ChangeSet(GraphChangeSet {
                    graph,
                    lease_epoch,
                    commit_epoch,
                    txn_seq,
                    idempotency_key,
                    link,
                    ops,
                })
            },
        );
    let schema = (
        graph_id(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        prop::option::of(".{0,12}"),
        ".{0,40}",
        language(),
    )
        .prop_map(
            |(graph, lease_epoch, commit_epoch, txn_seq, idempotency_key, statement, language)| {
                GraphRecord::Schema(SchemaChange {
                    graph,
                    lease_epoch,
                    commit_epoch,
                    txn_seq,
                    idempotency_key,
                    statement,
                    language,
                })
            },
        );
    prop_oneof![4 => change_set, 1 => schema]
}

/// Bytes worth decoding: noise, noise behind a valid header, and golden vectors with bytes
/// changed, so the decoder is driven past its first checks.
fn hostile_bytes() -> impl Strategy<Value = Vec<u8>> {
    let goldens: Vec<Vec<u8>> = golden_cases()
        .into_iter()
        .map(|(name, _)| golden_bytes(name))
        .collect();
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..64),
        (1u8..=2, prop::collection::vec(any::<u8>(), 0..128)).prop_map(|(kind, rest)| {
            let mut bytes = vec![RECORD_VERSION, kind];
            bytes.extend(rest);
            bytes
        }),
        (
            prop::sample::select(goldens),
            prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..4),
        )
            .prop_map(|(mut bytes, edits)| {
                for (at, byte) in edits {
                    let i = at.index(bytes.len());
                    bytes[i] = byte;
                }
                bytes
            }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    /// Every record survives the codec, and its bytes are a function of the record: the
    /// re-encoding is byte-equal (which also checks what `PartialEq` ignores, such as a zoned
    /// datetime's offset and the sign of a zero).
    #[test]
    fn round_trip_proptest(record in record()) {
        let bytes = encode(&record).expect("every generated record is representable");
        prop_assert_eq!(bytes[0], RECORD_VERSION);
        let decoded = decode(&bytes).expect("decodes");
        prop_assert_eq!(&decoded, &record);
        prop_assert_eq!(encode(&decoded).expect("re-encodes"), bytes);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 20_000, ..ProptestConfig::default() })]

    /// Decoding never panics, and whatever it accepts is the unique encoding of what it returns.
    #[test]
    fn decode_is_total_on_random_bytes(bytes in hostile_bytes()) {
        if let Ok(record) = decode(&bytes) {
            let again = encode(&record).expect("a decoded record re-encodes");
            prop_assert_eq!(again.as_ref(), bytes.as_slice());
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The codec over a real capture
// ---------------------------------------------------------------------------------------------

/// Change sets captured from Grafeo (GR1 Task 9's mechanism) survive the codec, and the decoded
/// operations rebuild the graph.
#[test]
fn captured_change_sets_round_trip_and_replay() {
    let db = GrafeoDB::with_config(Config::in_memory().with_cdc()).expect("opens");
    let capture = CdcCapture::new(&db).expect("CDC is on");
    let statements = [
        "INSERT (:Person {name: 'Alix', born: date('1990-01-02'), tags: ['a', 'b'], \
         score: 1.5, n: 9223372036854775807, meta: {k: [1, {deep: true}]}})\
         -[:KNOWS {since: 2020}]->(:Person:Admin {name: 'Gus'})",
        "MATCH (p:Person {name: 'Alix'}) SET p.score = 2.5 REMOVE p.tags",
        "MATCH (p:Person {name: 'Gus'}) SET p:Owner",
        "MATCH (:Person)-[k:KNOWS]->(:Person) DELETE k",
    ];
    let mut log = Vec::new();
    for (seq, statement) in statements.iter().enumerate() {
        let mut session = db.session();
        capture.begin(&mut session);
        session.begin_transaction().expect("begin");
        session
            .execute(statement)
            .unwrap_or_else(|err| panic!("{statement}: {err}"));
        let epoch = session
            .prepare_commit()
            .expect("prepare")
            .commit()
            .expect("commit");
        let ops = capture.take(epoch.as_u64()).expect("captured");
        assert!(!ops.is_empty(), "{statement}: captured nothing");
        let record = GraphRecord::ChangeSet(GraphChangeSet {
            graph: graph(),
            lease_epoch: 1,
            commit_epoch: epoch.as_u64(),
            txn_seq: seq as u64,
            idempotency_key: None,
            link: None,
            ops,
        });
        let bytes = encode(&record).expect("encodes");
        let decoded = decode(&bytes).expect("decodes");
        assert_eq!(decoded, record, "{statement}");
        log.push(decoded);
    }

    let replayed = GrafeoDB::with_config(Config::in_memory().with_cdc()).expect("opens");
    for record in &log {
        let GraphRecord::ChangeSet(set) = record else {
            panic!("a change set")
        };
        apply_to_store(&replayed, &set.ops).expect("replays");
    }
    let read = |db: &GrafeoDB| -> Vec<Vec<Value>> {
        db.session()
            .execute(
                "MATCH (p:Person) RETURN p.name, labels(p), p.born, p.tags, p.score, p.n, p.meta \
                 ORDER BY p.name",
            )
            .expect("reads")
            .rows()
            .to_vec()
    };
    assert_eq!(read(&replayed), read(&db));
    let edges = |db: &GrafeoDB| -> Vec<Vec<Value>> {
        db.session()
            .execute("MATCH ()-[k]->() RETURN count(k)")
            .expect("reads")
            .rows()
            .to_vec()
    };
    assert_eq!(edges(&replayed), edges(&db));
}
