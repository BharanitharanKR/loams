//! Schema mapping and the evolution policy (CN2 Task 5).
// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use common::*;
use loams_connector_sdk::schema::arrow::SqlTypeMapper;
use loams_connector_sdk::schema::{
    Evolution, Lossy, NativeType, SchemaChange, SchemaError, SchemaPolicy, SchemaTracker,
    TypeMapper,
};
use loams_connector_sdk::testing::memory::{MemoryCheckpointStore, MemoryFabric};
use loams_connector_sdk::*;

const TABLE: &str = "public.orders";

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn v1() -> SchemaRef {
    schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

// ------------------------------------------------------------- evolution --

#[test]
fn additive_change_evolves() {
    let at = Position::from_u64(42);
    let mut t = SchemaTracker::new(Evolution::Additive);

    // A new table is additive: it is recorded and reported as a change from
    // the empty schema.
    let first = t.observe(TABLE, v1(), &at).unwrap().expect("a new table");
    assert_eq!(first.table, TABLE);
    assert!(first.before.fields().is_empty());
    assert_eq!(first.after, v1());
    assert_eq!(t.current(TABLE), Some(v1()));

    // The same schema again is no change.
    assert!(t.observe(TABLE, v1(), &at).unwrap().is_none());

    // A new nullable column evolves the table.
    let v2 = schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ]);
    let change = t
        .observe(TABLE, v2.clone(), &at)
        .unwrap()
        .expect("a change");
    assert_eq!(change.before, v1());
    assert_eq!(change.after, v2);
    assert_eq!(change.at, at);
    assert_eq!(t.current(TABLE), Some(v2.clone()));

    // Column order is not a change of shape: a column added in the middle
    // is still additive.
    let v3 = schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("tag", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ]);
    assert!(t.observe(TABLE, v3, &at).unwrap().is_some());

    // A second table is independent of the first.
    assert!(t.observe("public.items", v1(), &at).unwrap().is_some());

    // A new column that is NOT NULL is not additive: old rows have no value.
    let mut t = SchemaTracker::new(Evolution::Additive);
    t.seed(TABLE, v1());
    let required = schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("qty", DataType::Int32, false),
    ]);
    let e = t.observe(TABLE, required, &at).unwrap_err();
    assert!(matches!(e, SchemaError::Incompatible { .. }), "{e}");
    assert!(e.to_string().contains("qty"), "{e}");
    // A refused change leaves the tracked schema as it was.
    assert_eq!(t.current(TABLE), Some(v1()));

    // A new nullable field inside a struct is additive too.
    let nested = |extra: bool| {
        let mut children = vec![Field::new("city", DataType::Utf8, true)];
        if extra {
            children.push(Field::new("zip", DataType::Utf8, true));
        }
        schema(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(children.into()), true),
        ])
    };
    let mut t = SchemaTracker::new(Evolution::Additive);
    t.seed(TABLE, nested(false));
    assert!(t.observe(TABLE, nested(true), &at).unwrap().is_some());
}

#[test]
fn widen_accepted_only_when_configured() {
    let at = Position::from_u64(7);
    let varchar = |n: &str| {
        Field::new("code", DataType::Utf8, true).with_metadata(
            [(schema::MAX_LENGTH_KEY.to_owned(), n.to_owned())]
                .into_iter()
                .collect(),
        )
    };
    // (before, after) pairs that are lossless widenings.
    let widenings: Vec<(Field, Field)> = vec![
        (
            Field::new("n", DataType::Int32, false),
            Field::new("n", DataType::Int64, false),
        ),
        (
            Field::new("n", DataType::Int8, false),
            Field::new("n", DataType::Int32, false),
        ),
        (
            Field::new("n", DataType::UInt32, false),
            Field::new("n", DataType::Int64, false),
        ),
        (
            Field::new("n", DataType::Float32, true),
            Field::new("n", DataType::Float64, true),
        ),
        (
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("amount", DataType::Decimal128(18, 2), true),
        ),
        (varchar("10"), varchar("40")),
        (
            Field::new("s", DataType::Utf8, true),
            Field::new("s", DataType::LargeUtf8, true),
        ),
        // NOT NULL → nullable relaxes a constraint without changing a value.
        (
            Field::new("n", DataType::Int32, false),
            Field::new("n", DataType::Int32, true),
        ),
    ];
    for (before, after) in widenings {
        let b = schema(vec![before.clone()]);
        let a = schema(vec![after.clone()]);

        let mut additive = SchemaTracker::new(Evolution::Additive);
        additive.seed(TABLE, b.clone());
        let e = additive.observe(TABLE, a.clone(), &at).unwrap_err();
        assert!(
            matches!(e, SchemaError::Incompatible { .. }),
            "{before:?} → {after:?} must need `widen`: {e}"
        );
        assert!(e.to_string().contains("widen"), "{e}");

        let mut widen = SchemaTracker::new(Evolution::Widen);
        widen.seed(TABLE, b.clone());
        let change = widen
            .observe(TABLE, a.clone(), &at)
            .unwrap_or_else(|e| panic!("{before:?} → {after:?} widens: {e}"))
            .expect("a change");
        assert_eq!((change.before, change.after), (b, a));
    }

    // Narrowings and lossy changes are refused even under `widen`.
    let refused: Vec<(Field, Field)> = vec![
        (
            Field::new("n", DataType::Int64, false),
            Field::new("n", DataType::Int32, false),
        ),
        (
            Field::new("n", DataType::Int64, false),
            Field::new("n", DataType::UInt64, false),
        ),
        (
            Field::new("n", DataType::Int64, false),
            Field::new("n", DataType::Float64, false),
        ),
        (
            Field::new("amount", DataType::Decimal128(18, 4), true),
            Field::new("amount", DataType::Decimal128(18, 2), true),
        ),
        (
            Field::new("amount", DataType::Decimal128(18, 2), true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ),
        (varchar("40"), varchar("10")),
        (
            Field::new("n", DataType::Int32, true),
            Field::new("n", DataType::Int32, false),
        ),
        (
            Field::new("n", DataType::Int32, true),
            Field::new("n", DataType::Utf8, true),
        ),
    ];
    for (before, after) in refused {
        let mut widen = SchemaTracker::new(Evolution::Widen);
        widen.seed(TABLE, schema(vec![before.clone()]));
        assert!(
            widen
                .observe(TABLE, schema(vec![after.clone()]), &at)
                .is_err(),
            "{before:?} → {after:?} is not a lossless widening"
        );
    }

    // `pause` accepts no change at all, not even an additive one or a new
    // table after the start.
    let mut pause = SchemaTracker::new(Evolution::Pause);
    pause.seed(TABLE, v1());
    assert!(pause.observe(TABLE, v1(), &at).unwrap().is_none());
    let v2 = schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ]);
    assert!(pause.observe(TABLE, v2, &at).is_err());
    assert!(pause.observe("public.items", v1(), &at).is_err());

    // The policy comes from the instance's config, `additive` by default.
    let p = SchemaPolicy::from_config(&serde_json::json!({})).unwrap();
    assert_eq!(p.evolution, Evolution::Additive);
    let p = SchemaPolicy::from_config(&serde_json::json!({ "evolution": "widen" })).unwrap();
    assert_eq!(p.evolution, Evolution::Widen);
    let p = SchemaPolicy::from_config(&serde_json::json!({ "evolution": "pause" })).unwrap();
    assert_eq!(p.evolution, Evolution::Pause);
    let e = SchemaPolicy::from_config(&serde_json::json!({ "evolution": "anything" })).unwrap_err();
    assert_eq!(e.class, ErrorClass::Fatal);
}

// ------------------------------------------------------- lossy mappings --

#[test]
fn lossy_mapping_refused() {
    let strict = SqlTypeMapper::new(Lossy::none());

    // Lossless mappings work without any opt-in.
    let f = strict
        .to_arrow(&NativeType::new("public.orders.id", "bigint", false))
        .unwrap();
    assert_eq!(
        (f.name().as_str(), f.data_type(), f.is_nullable()),
        ("id", &DataType::Int64, false)
    );
    let f = strict
        .to_arrow(&NativeType::new(
            "public.orders.big",
            "bigint unsigned",
            true,
        ))
        .unwrap();
    assert_eq!(f.data_type(), &DataType::UInt64);
    let f = strict
        .to_arrow(&NativeType::new(
            "public.orders.amount",
            "numeric(38,9)",
            true,
        ))
        .unwrap();
    assert_eq!(f.data_type(), &DataType::Decimal128(38, 9));
    let f = strict
        .to_arrow(&NativeType::new(
            "public.orders.at",
            "timestamp with time zone",
            true,
        ))
        .unwrap();
    assert_eq!(
        f.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let f = strict
        .to_arrow(&NativeType::new("public.orders.code", "varchar(12)", true))
        .unwrap();
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get(schema::MAX_LENGTH_KEY).map(String::as_str),
        Some("12")
    );

    // The three lossy classes of CN2 Task 5, in both directions.
    let decimal40 = NativeType::new("public.orders.total", "decimal(40,2)", true);
    let local_ts = NativeType::new("public.orders.placed", "timestamp", true);
    let signed_only = SqlTypeMapper::new(Lossy::none()).signed_only();
    let unsigned_field = Field::new("big", DataType::UInt64, true)
        .with_metadata([(schema::PATH_KEY.to_owned(), "public.orders.big".to_owned())].into());

    for (what, err) in [
        ("decimal(40,2)", strict.to_arrow(&decimal40).unwrap_err()),
        ("timestamp", strict.to_arrow(&local_ts).unwrap_err()),
        (
            "UInt64",
            signed_only.from_arrow(&unsigned_field).unwrap_err(),
        ),
        (
            "Timestamp without tz",
            strict
                .from_arrow(&Field::new(
                    "placed",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))
                .unwrap_err(),
        ),
    ] {
        assert!(matches!(err, SchemaError::Lossy { .. }), "{what}: {err}");
        assert!(err.to_string().contains("lossy"), "{what}: {err}");
        // A refused mapping pauses the instance, like any schema drift.
        assert_eq!(
            ConnectorError::from(err).class,
            ErrorClass::SchemaIncompatible
        );
    }

    // Listing the path in `lossy` accepts exactly that path.
    let policy = SchemaPolicy::from_config(&serde_json::json!({
        "lossy": ["public.orders.total", "public.orders.placed", "public.orders.big"]
    }))
    .unwrap();
    let lenient = SqlTypeMapper::new(policy.lossy.clone());
    let f = lenient.to_arrow(&decimal40).unwrap();
    assert_eq!(f.data_type(), &DataType::Decimal128(38, 2));
    assert!(f.metadata().contains_key(schema::LOSSY_KEY));
    let f = lenient.to_arrow(&local_ts).unwrap();
    assert_eq!(
        f.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let n = SqlTypeMapper::new(policy.lossy.clone())
        .signed_only()
        .from_arrow(&unsigned_field)
        .unwrap();
    assert_eq!(n.type_name, "bigint");
    // Another path with the same type is still refused.
    assert!(
        lenient
            .to_arrow(&NativeType::new(
                "public.orders.subtotal",
                "decimal(40,2)",
                true
            ))
            .is_err()
    );

    // An unknown native type is refused, not guessed.
    assert!(matches!(
        strict
            .to_arrow(&NativeType::new("public.orders.g", "geometry", true))
            .unwrap_err(),
        SchemaError::Unsupported { .. }
    ));

    // Lossless types map back to the native type they came from.
    for ty in [
        "boolean",
        "smallint",
        "integer",
        "bigint",
        "bigint unsigned",
        "real",
        "double precision",
        "decimal(18,2)",
        "varchar(12)",
        "text",
        "varbinary",
        "date",
        "time",
        "timestamp with time zone",
        "uuid",
        "json",
    ] {
        let native = NativeType::new(format!("t.{ty}"), ty, true);
        let field = strict.to_arrow(&native).unwrap();
        let back = strict.from_arrow(&field).unwrap();
        assert_eq!(back.type_name, ty, "{ty} → {:?} → {back:?}", field);
    }
}

// --------------------------------------------- through a running pipeline --

/// A CDC-like source over one table whose batches carry their own schema. It
/// tracks the table's schema, emits a `schema-change` event before the first
/// batch of a new shape, and returns the tracker's error on an incompatible
/// one.
struct TableSource {
    batches: Arc<Mutex<VecDeque<RecordBatch>>>,
    tracker: SchemaTracker,
    envelope: EnvelopeBuilder,
    offset: u64,
}

#[async_trait]
impl Source for TableSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let skip = pos.and_then(|p| p.as_u64()).unwrap_or(0);
        let mut q = self.batches.lock().unwrap();
        while self.offset < skip {
            q.pop_front();
            self.offset += 1;
        }
        Ok(())
    }

    async fn next(&mut self, _budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(batch) = self.batches.lock().unwrap().front().cloned() else {
            return Ok(None);
        };
        let at = Position::from_u64(self.offset + 1);
        if let Some(change) = self.tracker.observe(TABLE, batch.schema(), &at)? {
            let ev = change.event(&self.envelope);
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(vec![ev]),
                position: Position::from_u64(self.offset),
            }));
        }
        self.batches.lock().unwrap().pop_front();
        self.offset += 1;
        let envelope =
            self.envelope
                .event("change", TABLE, ids::change(&at.display, TABLE, "batch"));
        Ok(Some(SourceBatch {
            events: EventBatch::Arrow { batch, envelope },
            position: at,
        }))
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

struct OneSplit;

#[async_trait]
impl SplitEnumerator for OneSplit {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new("orders", serde_json::Value::Null)])
    }
}

struct TableFactory {
    batches: Arc<Mutex<VecDeque<RecordBatch>>>,
    evolution: Evolution,
}

impl SourceFactory for TableFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new(TableSource {
            batches: self.batches.clone(),
            tracker: SchemaTracker::new(self.evolution),
            envelope: EnvelopeBuilder::new("postgresql", &instance()),
            offset: 0,
        }))
    }
}

fn rows(schema: SchemaRef, n: i32) -> RecordBatch {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| -> ArrayRef {
            match f.data_type() {
                DataType::Int32 => Arc::new(Int32Array::from_iter_values(0..n)),
                DataType::Int64 => Arc::new(Int64Array::from_iter_values(0..n as i64)),
                _ => Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("r{i}")),
                )),
            }
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

async fn run_table(
    batches: Vec<RecordBatch>,
    evolution: Evolution,
) -> (PipelineHandle, MemoryFabric) {
    let fabric = MemoryFabric::new();
    let p = Pipeline::source(instance(), small_config())
        .enumerator(Box::new(OneSplit))
        .sources(Arc::new(TableFactory {
            batches: Arc::new(Mutex::new(batches.into())),
            evolution,
        }))
        .writer(Arc::new(fabric.clone()))
        .checkpoints(Arc::new(MemoryCheckpointStore::new()))
        .discover_every(Duration::from_secs(3600))
        .epoch(1)
        .start()
        .await
        .unwrap();
    (p, fabric)
}

fn with_note() -> SchemaRef {
    schema(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ])
}

#[tokio::test(start_paused = true)]
async fn incompatible_change_pauses() {
    // `id` changes from Int32 to Utf8 in the third batch.
    let retyped = schema(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("note", DataType::Utf8, true),
    ]);
    let (p, fabric) = run_table(
        vec![rows(v1(), 3), rows(with_note(), 2), rows(retyped, 2)],
        Evolution::Additive,
    )
    .await;

    let st = p.wait_for(|s| s.state == PipelineState::Paused).await;
    let e = st.last_error.expect("a paused instance says why");
    assert_eq!(e.class, ErrorClass::SchemaIncompatible);
    // The before and after schemas are in `last_error`.
    assert!(e.message.contains(TABLE), "{}", e.message);
    assert!(e.message.contains("before"), "{}", e.message);
    assert!(e.message.contains("after"), "{}", e.message);
    assert!(e.message.contains("id: Int32"), "{}", e.message);
    assert!(e.message.contains("id: Utf8"), "{}", e.message);

    // Nothing of the new shape reached the Fabric, and the instance stays
    // paused rather than coercing it.
    eventually("the compatible batches appended", || fabric.len() == 4).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(p.status().state, PipelineState::Paused);
    let types: Vec<String> = fabric.events().into_iter().map(|e| e.ty).collect();
    assert_eq!(
        types,
        [
            "io.loams.dev.flow.postgresql.schema-change.v1",
            "io.loams.dev.flow.postgresql.change.v1",
            "io.loams.dev.flow.postgresql.schema-change.v1",
            "io.loams.dev.flow.postgresql.change.v1",
        ]
    );
    p.stop().await;
}

#[tokio::test(start_paused = true)]
async fn schema_change_event_emitted() {
    let (p, fabric) = run_table(
        vec![rows(v1(), 3), rows(with_note(), 2)],
        Evolution::Additive,
    )
    .await;
    eventually("both batches and both changes", || fabric.len() == 4).await;
    p.stop().await;

    let changes: Vec<CloudEvent> = fabric
        .events()
        .into_iter()
        .filter(|e| e.ty == "io.loams.dev.flow.postgresql.schema-change.v1")
        .collect();
    assert_eq!(changes.len(), 2, "the new table, then the new column");
    let ev = &changes[1];
    assert_eq!(ev.subject.as_deref(), Some(TABLE));
    assert_eq!(ev.source, "/connectors/orders/public.orders");
    assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
    assert_eq!(ev.extensions["loamsconnector"], "postgresql");
    assert_eq!(ev.extensions["loamsinstance"], "orders");
    // The id derives from the position, so a re-read emits the same id.
    assert_eq!(ev.id, ids::change("2", TABLE, "schema-change"));

    let data: serde_json::Value = serde_json::from_slice(&ev.data).unwrap();
    assert_eq!(data["table"], TABLE);
    assert_eq!(data["at"], "2");
    let names = |side: &str| -> Vec<String> {
        data[side]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names("before"), ["id", "name"]);
    assert_eq!(names("after"), ["id", "name", "note"]);
    assert_eq!(data["after"]["fields"][2]["type"], "Utf8");
    assert_eq!(data["after"]["fields"][2]["nullable"], true);

    // The first change is the new table, from the empty schema.
    let first: serde_json::Value = serde_json::from_slice(&changes[0].data).unwrap();
    assert_eq!(first["before"]["fields"].as_array().unwrap().len(), 0);

    // Routes deliver schema changes to `<db>.schema_changes` (§33 §7).
    assert_eq!(SchemaChange::topic("shop"), "shop.schema_changes");
}
