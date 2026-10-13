//! DuckDB as an ADBC target (the `duckdb` profile on the ADBC module):
//! DuckDB's own shared library, loaded by path from the runtime's allowlist
//! with a SHA-256 check (CN1 Ruling 9), written by bulk ingest and read
//! back by a query. DuckDB is in-process, so this is the real system.
//!
//! The library is not built here (R17.7): `LOAMS_DUCKDB_DRIVER` names a
//! `libduckdb` (DuckDB's release asset `libduckdb-linux-amd64.zip`), and
//! `LOAMS_DUCKDB_DRIVER_SHA256` its digest (computed from the file when
//! unset). Without it the test says why it skipped.

mod common;

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::*;
use loams_connector_analytics::adbc::{Adbc, AllowedDriver, DriverAllowlist};
use loams_connector_analytics::query::QUERY_SPLIT;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorFactory, EventBatch, SinkHandle, Split, StaticSecrets,
};
use sha2::{Digest, Sha256};

fn allowlist(path: &str) -> DriverAllowlist {
    let sha = std::env::var("LOAMS_DUCKDB_DRIVER_SHA256").unwrap_or_else(|_| {
        let body = std::fs::read(path).expect("driver file");
        Sha256::digest(&body)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    });
    DriverAllowlist::default().with(
        "duckdb",
        AllowedDriver {
            path: path.into(),
            sha256: sha,
            entrypoint: Some("duckdb_adbc_init".to_owned()),
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_adbc_target() {
    // A driver outside the allowlist never loads, whatever the config says.
    let refused = Adbc::new("duckdb", Arc::new(DriverAllowlist::default()));
    let c = ctx(
        serde_json::json!({"driver": "duckdb", "query": "SELECT 1"}),
        StaticSecrets::new(),
    );
    let Err(e) = refused.source(&c) else {
        panic!("a driver outside the allowlist must be refused")
    };
    assert!(e.message.contains("allowlist"), "{}", e.message);

    if !real(&["LOAMS_DUCKDB_DRIVER"], "the DuckDB half (no libduckdb)") {
        return;
    }
    let driver = env("LOAMS_DUCKDB_DRIVER");
    let adbc = Adbc::new("duckdb", Arc::new(allowlist(&driver)));
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("loams.duckdb");
    let db = db.to_str().expect("utf8 path");

    // The sink: an Arrow batch and an events batch, bulk-ingested.
    let sink_ctx = ctx(
        serde_json::json!({
            "driver": "duckdb",
            "database": {"path": db},
            "sink": {"table": "orders", "mode": "create_append"}
        }),
        StaticSecrets::new(),
    );
    let f = adbc.sink(&sink_ctx).expect("config").expect("sink");
    let SinkHandle::Plain(mut sink) = f.create().expect("create") else {
        panic!("plain")
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec!["n0", "n1", "n2"])),
        ],
    )
    .expect("batch");
    let ack = sink
        .write(EventBatch::Arrow {
            batch: rb,
            envelope: CloudEvent::new(
                "r/0/0-2",
                "/connectors/orders/q",
                "io.loams.dev.flow.x.rows.v1",
            ),
        })
        .await
        .expect("ingest");
    assert_eq!(ack.rows, 3);
    let events: Vec<CloudEvent> = (3..5)
        .map(|i| {
            CloudEvent::new(format!("e/{i}"), "/t", "t").with_data(
                "application/json",
                format!("{{\"id\":{i},\"name\":\"n{i}\"}}"),
            )
        })
        .collect();
    sink.write(EventBatch::Events(events))
        .await
        .expect("ingest");
    drop(sink);
    drop(f);

    // The source: a query over the same file, as Arrow pages.
    let src_ctx = ctx(
        serde_json::json!({
            "driver": "duckdb",
            "database": {"path": db},
            "query": "SELECT id, name FROM orders ORDER BY id"
        }),
        StaticSecrets::new(),
    );
    let (_, sf) = adbc.source(&src_ctx).expect("config").expect("source");
    let split = Split::new(QUERY_SPLIT, serde_json::json!({}));
    let mut src = sf.create(&split).expect("create");
    src.open(&split, None).await.expect("open");
    let budget = Budget {
        max_rows: 2,
        max_bytes: 1 << 20,
    };
    let (mut ids, mut names) = (Vec::new(), Vec::new());
    while let Some(b) = src.next(budget).await.expect("next") {
        let EventBatch::Arrow { batch, envelope } = b.events else {
            panic!("arrow")
        };
        assert_eq!(envelope.ty, "io.loams.dev.flow.duckdb.rows.v1");
        assert!(batch.num_rows() <= 2);
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        for i in 0..id.len() {
            ids.push(id.value(i));
            names.push(name.value(i).to_owned());
        }
    }
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    assert_eq!(names, ["n0", "n1", "n2", "n3", "n4"]);
}
