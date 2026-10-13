//! Databricks through the SQL Statement Execution API: Arrow results behind
//! external links, and the volume-file sink with `COPY INTO`/`MERGE`.
//!
//! The local halves run against an endpoint that answers like the API; the
//! fixture half replays recordings from a real workspace once the nightly
//! job has made them (R17.10); the nightly half runs against a real SQL
//! warehouse.

mod common;

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::*;
use http::{Response, StatusCode};
use loams_connector_analytics::databricks::Databricks;
use loams_connector_analytics::query::QUERY_SPLIT;
use loams_connector_analytics::rows::{ipc_stream, read_parquet};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorFactory, EventBatch, SinkHandle, Split, StaticSecrets,
};

fn chunk(ids: std::ops::Range<i64>) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let rb = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids.collect::<Vec<_>>()))],
    )
    .expect("batch");
    ipc_stream(&rb).expect("ipc")
}

async fn fake_workspace() -> Fake {
    Fake::start(|req, _n| {
        let base = format!(
            "http://{}",
            req.header("host").unwrap_or_else(|| "localhost".to_owned())
        );
        let path = req.uri.split('?').next().unwrap_or("").to_owned();
        if path.starts_with("/files/") {
            // A presigned link: its own credentials, never the token.
            assert!(
                req.header("authorization").is_none(),
                "token sent to a presigned link"
            );
            assert!(req.param("sig").is_some());
            let body = if path == "/files/chunk0" {
                chunk(0..3)
            } else {
                chunk(3..5)
            };
            return Response::new(body);
        }
        assert_eq!(
            req.header("authorization").as_deref(),
            Some("Bearer dapi-test")
        );
        match (req.method.as_str(), path.as_str()) {
            ("POST", "/api/2.0/sql/statements") => {
                let b = req.json();
                assert_eq!(b["warehouse_id"], "wh1");
                if b["format"] == "ARROW_STREAM" {
                    assert_eq!(b["disposition"], "EXTERNAL_LINKS");
                    ok(serde_json::json!({"statement_id": "st1", "status": {"state": "PENDING"}}))
                } else {
                    ok(serde_json::json!({"statement_id": "st2", "status": {"state": "SUCCEEDED"}}))
                }
            }
            ("GET", "/api/2.0/sql/statements/st1") => ok(serde_json::json!({
                "statement_id": "st1",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"format": "ARROW_STREAM", "total_chunk_count": 2},
                "result": {"external_links": [{
                    "chunk_index": 0, "row_offset": 0, "row_count": 3,
                    "external_link": format!("{base}/files/chunk0?sig=presigned-secret"),
                    "next_chunk_index": 1
                }]}
            })),
            ("GET", "/api/2.0/sql/statements/st1/result/chunks/1") => ok(serde_json::json!({
                "external_links": [{
                    "chunk_index": 1, "row_offset": 3, "row_count": 2,
                    "external_link": format!("{base}/files/chunk1?sig=presigned-secret")
                }]
            })),
            ("GET", p) if p.starts_with("/api/2.0/sql/statements/gone") => json_reply(
                StatusCode::NOT_FOUND,
                serde_json::json!({"error_code": "NOT_FOUND", "message": "statement closed"}),
            ),
            ("PUT", p) if p.starts_with("/api/2.0/fs/files/Volumes/main/sales/landing/") => {
                assert_eq!(req.param("overwrite").as_deref(), Some("false"));
                Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Bytes::new())
                    .expect("response")
            }
            (m, p) => panic!("unexpected {m} {p}"),
        }
    })
    .await
}

fn config(url: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut c = serde_json::json!({"host": url, "warehouse_id": "wh1", "token": "dbx"});
    for (k, v) in extra.as_object().expect("object") {
        c[k] = v.clone();
    }
    c
}

/// Chunks as pages: chunk 0's link comes with the statement, chunk 1's from
/// the chunk endpoint; each link's Arrow stream is read without the token.
async fn external_links_local() {
    let fake = fake_workspace().await;
    let c = ctx(
        config(
            &fake.url(),
            serde_json::json!({"query": "SELECT id FROM t ORDER BY id"}),
        ),
        StaticSecrets::new().with("dbx", "dapi-test"),
    );
    let (_, f) = Databricks.source(&c).expect("config").expect("source");
    let split = Split::new(QUERY_SPLIT, serde_json::json!({}));
    let mut src = f.create(&split).expect("create");
    src.open(&split, None).await.expect("open");
    let budget = Budget {
        max_rows: 1000,
        max_bytes: 1 << 20,
    };
    let mut ids = Vec::new();
    while let Some(b) = src.next(budget).await.expect("next") {
        let EventBatch::Arrow { batch, envelope } = b.events else {
            panic!("arrow")
        };
        assert_eq!(envelope.ty, "io.loams.dev.flow.databricks.rows.v1");
        ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .values()
                .iter()
                .copied(),
        );
    }
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

/// The sink uploads the batch as Parquet to the volume, then `COPY INTO`;
/// a second upload of the same batch (a retry) keeps the same name.
async fn volume_sink_local() {
    let fake = fake_workspace().await;
    let c = ctx(
        config(
            &fake.url(),
            serde_json::json!({"sink": {"table": "main.sales.orders", "volume": "/Volumes/main/sales/landing/loams"}}),
        ),
        StaticSecrets::new().with("dbx", "dapi-test"),
    );
    let f = Databricks.sink(&c).expect("config").expect("sink");
    let SinkHandle::Plain(mut sink) = f.create().expect("create") else {
        panic!("plain")
    };
    let batch = EventBatch::Events(
        (0..3)
            .map(|i| {
                CloudEvent::new(format!("e/{i}"), "/t", "t")
                    .with_data("application/json", format!("{{\"id\":{i}}}"))
            })
            .collect(),
    );
    assert_eq!(sink.write(batch.clone()).await.expect("write").rows, 3);
    sink.write(batch).await.expect("retry");
    let seen = fake.seen();
    let puts: Vec<_> = seen.iter().filter(|s| s.method == "PUT").collect();
    assert_eq!(puts.len(), 2);
    assert_eq!(
        puts[0].uri, puts[1].uri,
        "a retried batch keeps its file name"
    );
    assert!(
        puts[0]
            .uri
            .starts_with("/api/2.0/fs/files/Volumes/main/sales/landing/loams/orders/")
    );
    assert_eq!(
        read_parquet(puts[0].body.clone()).expect("parquet")[0].num_rows(),
        3
    );
    let stmt = seen
        .iter()
        .find(|s| s.method == "POST")
        .expect("statement")
        .json();
    let sql = stmt["statement"].as_str().expect("sql");
    assert!(sql.starts_with(
        "COPY INTO main.sales.orders FROM '/Volumes/main/sales/landing/loams/orders/"
    ));
    assert!(sql.ends_with(".parquet' FILEFORMAT = PARQUET"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn databricks_arrow_external_links_fixture() {
    external_links_local().await;
    volume_sink_local().await;
    recorded("databricks");
    if !real(
        &[
            "LOAMS_DATABRICKS_HOST",
            "LOAMS_DATABRICKS_TOKEN",
            "LOAMS_DATABRICKS_WAREHOUSE_ID",
        ],
        "the Databricks nightly",
    ) {
        return;
    }
    let c = ctx(
        serde_json::json!({
            "host": env("LOAMS_DATABRICKS_HOST"),
            "warehouse_id": env("LOAMS_DATABRICKS_WAREHOUSE_ID"),
            "token": "t",
            "query": "SELECT id FROM range(0, 100000) ORDER BY id",
        }),
        StaticSecrets::new().with("t", env("LOAMS_DATABRICKS_TOKEN")),
    );
    let (_, f) = Databricks.source(&c).expect("config").expect("source");
    let split = Split::new(QUERY_SPLIT, serde_json::json!({}));
    let mut src = f.create(&split).expect("create");
    src.open(&split, None).await.expect("open");
    let budget = Budget {
        max_rows: 65_536,
        max_bytes: 8 << 20,
    };
    let mut rows = 0;
    while let Some(b) = src.next(budget).await.expect("next") {
        rows += b.events.rows();
    }
    assert_eq!(rows, 100_000);
}
