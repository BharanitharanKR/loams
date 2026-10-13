//! Amazon Redshift through the Data API: the rows path, the `UNLOAD` bulk
//! path and the `COPY`/`MERGE` sink.
//!
//! The local halves run against an endpoint that answers like the Data API
//! (signed JSON requests, `X-Amz-Target` operations) with `object_store`'s
//! in-memory store as the bucket. The fixture halves replay recordings from
//! real Redshift once the nightly job has made them (R17.10), and the
//! nightly half runs against a real Serverless workgroup.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Array, BooleanArray, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::*;
use loams_connector_analytics::query::{PagedQuery, QUERY_SPLIT, QuerySource};
use loams_connector_analytics::redshift::{
    Redshift, RedshiftConfig, RedshiftQuery, RedshiftSink, UnloadQuery,
};
use loams_connector_analytics::rows::{parquet_file, read_parquet};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorFactory, EventBatch, Sink, Source, Split, StaticSecrets,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// What the fake Data API holds between calls.
#[derive(Default)]
struct DataApi {
    /// Statements and batches received, in order.
    sql: Mutex<Vec<String>>,
    /// `DescribeStatement` answers `STARTED` until this is set.
    finished: AtomicBool,
}

async fn fake_data_api(state: Arc<DataApi>) -> Fake {
    Fake::start(move |req, _n| {
        assert_eq!(req.method, "POST");
        assert_eq!(
            req.header("content-type").as_deref(),
            Some("application/x-amz-json-1.1")
        );
        let auth = req.header("authorization").expect("signed");
        assert!(
            auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDLOAMS/")
                && auth.contains("/us-east-1/redshift-data/aws4_request")
                && auth.contains("SignedHeaders=content-type;host;x-amz-date;x-amz-security-token;x-amz-target"),
            "{auth}"
        );
        assert_eq!(req.header("x-amz-security-token").as_deref(), Some("session-tok"));
        let body = req.json();
        let target = req.header("x-amz-target").expect("target");
        match target.as_str() {
            "RedshiftData.ExecuteStatement" => {
                assert_eq!(body["WorkgroupName"], "wg");
                assert_eq!(body["Database"], "dev");
                state
                    .sql
                    .lock()
                    .expect("lock")
                    .push(body["Sql"].as_str().unwrap_or("").to_owned());
                ok(serde_json::json!({"Id": "s1"}))
            }
            "RedshiftData.BatchExecuteStatement" => {
                for s in body["Sqls"].as_array().expect("Sqls") {
                    state
                        .sql
                        .lock()
                        .expect("lock")
                        .push(s.as_str().unwrap_or("").to_owned());
                }
                ok(serde_json::json!({"Id": "b1"}))
            }
            "RedshiftData.DescribeStatement" => {
                if state.finished.load(Ordering::SeqCst) {
                    ok(serde_json::json!({"Id": body["Id"], "Status": "FINISHED", "HasResultSet": body["Id"] == "s1"}))
                } else {
                    ok(serde_json::json!({"Id": body["Id"], "Status": "STARTED"}))
                }
            }
            "RedshiftData.GetStatementResult" => {
                let cols = serde_json::json!([
                    {"name": "id", "typeName": "int8"},
                    {"name": "amount", "typeName": "numeric", "precision": 10, "scale": 2},
                    {"name": "at", "typeName": "timestamptz"},
                    {"name": "name", "typeName": "varchar"},
                    {"name": "flag", "typeName": "bool"},
                ]);
                let rec = |i: i64| {
                    serde_json::json!([
                        {"longValue": i},
                        {"stringValue": format!("{i}.25")},
                        {"stringValue": "2026-10-01 12:00:00+00"},
                        if i == 4 { serde_json::json!({"isNull": true}) } else { serde_json::json!({"stringValue": format!("n{i}")}) },
                        {"booleanValue": i % 2 == 0},
                    ])
                };
                match body.get("NextToken").and_then(|t| t.as_str()) {
                    None => ok(serde_json::json!({
                        "ColumnMetadata": cols, "Records": (0..3).map(rec).collect::<Vec<_>>(),
                        "NextToken": "t2", "TotalNumRows": 5
                    })),
                    Some("t2") => ok(serde_json::json!({
                        "ColumnMetadata": cols, "Records": (3..5).map(rec).collect::<Vec<_>>(),
                        "TotalNumRows": 5
                    })),
                    Some(_) => json_reply(
                        http::StatusCode::BAD_REQUEST,
                        serde_json::json!({"__type": "ResourceNotFoundException", "message": "Query does not exist."}),
                    ),
                }
            }
            other => panic!("unexpected Data API operation {other}"),
        }
    })
    .await
}

fn config(endpoint: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut c = serde_json::json!({
        "region": "us-east-1",
        "endpoint": endpoint,
        "workgroup_name": "wg",
        "database": "dev",
        "auth": {"access_key_id": "aws-key", "secret_access_key": "aws-secret", "session_token": "aws-token"},
    });
    for (k, v) in extra.as_object().expect("object") {
        c[k] = v.clone();
    }
    c
}

fn secrets() -> StaticSecrets {
    StaticSecrets::new()
        .with("aws-key", "AKIDLOAMS")
        .with("aws-secret", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
        .with("aws-token", "session-tok")
}

async fn drain(src: &mut QuerySource, rows: usize) -> Vec<RecordBatch> {
    let budget = Budget {
        max_rows: rows,
        max_bytes: 1 << 20,
    };
    let mut out = Vec::new();
    while let Some(b) = src.next(budget).await.expect("next") {
        let EventBatch::Arrow { batch, .. } = b.events else {
            panic!("arrow")
        };
        out.push(batch);
    }
    out
}

fn split() -> Split {
    Split::new(QUERY_SPLIT, serde_json::json!({}))
}

/// The rows path: `ExecuteStatement`, `DescribeStatement` until finished,
/// `GetStatementResult` page by page, typed by the column metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redshift_rows_paging_local() {
    let state = Arc::new(DataApi::default());
    state.finished.store(true, Ordering::SeqCst);
    let fake = fake_data_api(state.clone()).await;
    let cfg_json = config(
        &fake.url(),
        serde_json::json!({"query": "SELECT * FROM sales ORDER BY id"}),
    );
    let c = ctx(cfg_json.clone(), secrets());
    let cfg = RedshiftConfig::parse(&cfg_json).expect("config");
    let q: Box<dyn PagedQuery> = Box::new(RedshiftQuery::new(&cfg, &c).expect("query"));
    let mut src = QuerySource::new(q, "redshift", &instance(), "query");
    src.open(&split(), None).await.expect("open");
    let batches = drain(&mut src, 2).await;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 5);
    let b = &batches[0];
    assert_eq!(b.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(
        b.schema().field(1).data_type(),
        &DataType::Decimal128(10, 2)
    );
    let amount = b
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal");
    assert_eq!(amount.value(1), 125);
    let flag = b
        .column(4)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool");
    assert!(flag.value(0));
    let last = batches.last().expect("batches");
    let name = last
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8");
    assert!(name.is_null(name.len() - 1));
    assert_eq!(
        state.sql.lock().expect("lock").as_slice(),
        ["SELECT * FROM sales ORDER BY id"]
    );
    // The factory builds the same source.
    assert!(Redshift.source(&c).expect("ok").is_some());
    assert!(
        Redshift.sink(&c).expect("ok").is_none(),
        "no sink configured"
    );
}

fn parquet_rows(ids: std::ops::Range<i64>) -> bytes::Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let rb = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids.collect::<Vec<_>>()))],
    )
    .expect("batch");
    parquet_file(&rb).expect("parquet")
}

/// `UNLOAD … TO 's3://…' FORMAT AS PARQUET`, then the files read back as
/// Arrow, one page per file in name order.
async fn unload_local() {
    let state = Arc::new(DataApi::default());
    let fake = fake_data_api(state.clone()).await;
    let cfg_json = config(
        &fake.url(),
        serde_json::json!({
            "query": "SELECT id FROM sales WHERE note = 'x' ORDER BY id",
            "unload": {"s3_uri": "s3://unload-bucket/loams", "iam_role": "arn:aws:iam::123456789012:role/unload"}
        }),
    );
    let c = ctx(cfg_json.clone(), secrets());
    let cfg = RedshiftConfig::parse(&cfg_json).expect("config");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let q = UnloadQuery::new(&cfg, &c)
        .expect("query")
        .with_store(store.clone());
    let mut src = QuerySource::new(Box::new(q), "redshift", &instance(), "query");
    src.open(&split(), None).await.expect("open");
    let reader = tokio::spawn(async move { drain(&mut src, 1000).await });

    // When the UNLOAD arrives, "Redshift" writes two Parquet files under
    // the prefix it names, then reports the statement finished.
    eventually("the UNLOAD statement", || {
        !state.sql.lock().expect("lock").is_empty()
    })
    .await;
    let sql = state.sql.lock().expect("lock")[0].clone();
    assert!(
        sql.starts_with("UNLOAD ('SELECT id FROM sales WHERE note = ''x'' ORDER BY id') TO 's3://unload-bucket/loams/orders/"),
        "{sql}"
    );
    assert!(sql.ends_with(
        "IAM_ROLE 'arn:aws:iam::123456789012:role/unload' FORMAT AS PARQUET ALLOWOVERWRITE"
    ));
    let prefix = sql
        .split("TO 's3://unload-bucket/")
        .nth(1)
        .and_then(|s| s.split('\'').next())
        .expect("prefix")
        .to_owned();
    store
        .put(
            &Path::from(format!("{prefix}0000_part_00.parquet")),
            parquet_rows(0..3).into(),
        )
        .await
        .expect("put");
    store
        .put(
            &Path::from(format!("{prefix}0001_part_00.parquet")),
            parquet_rows(3..5).into(),
        )
        .await
        .expect("put");
    state.finished.store(true, Ordering::SeqCst);
    let batches = tokio::time::timeout(Duration::from_secs(30), reader)
        .await
        .expect("reads")
        .expect("task");
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    assert_eq!(batches.len(), 2, "one page per file");
}

/// Each batch as one Parquet file, then `COPY` into a temporary table and
/// `MERGE` on the keys, in one `BatchExecuteStatement`.
async fn copy_merge_local() {
    let state = Arc::new(DataApi::default());
    let fake = fake_data_api(state.clone()).await;
    let cfg_json = config(
        &fake.url(),
        serde_json::json!({"sink": {
            "table": "public.orders", "mode": "merge", "keys": ["id"],
            "staging": {"s3_uri": "s3://stage-bucket/in", "iam_role": "arn:aws:iam::123456789012:role/copy"}
        }}),
    );
    let c = ctx(cfg_json.clone(), secrets());
    let cfg = RedshiftConfig::parse(&cfg_json).expect("config");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut sink = RedshiftSink::new(&cfg, &c)
        .expect("sink")
        .with_store(store.clone());
    let batch = EventBatch::Events(
        (0..3)
            .map(|i| {
                CloudEvent::new(format!("e/{i}"), "/t", "t").with_data(
                    "application/json",
                    format!("{{\"id\":{i},\"name\":\"n{i}\"}}"),
                )
            })
            .collect(),
    );
    let writer = tokio::spawn(async move { sink.write(batch).await });
    eventually("the batch statement", || {
        state.sql.lock().expect("lock").len() >= 4
    })
    .await;
    let sqls = state.sql.lock().expect("lock").clone();
    let copy = &sqls[1];
    let key = copy
        .split("FROM 's3://stage-bucket/")
        .nth(1)
        .and_then(|s| s.split('\'').next())
        .expect("staged key")
        .to_owned();
    assert!(
        key.starts_with("in/orders/") && key.ends_with(".parquet"),
        "{key}"
    );
    // The staged file is the batch, as Parquet.
    let body = store
        .get(&Path::from(key.as_str()))
        .await
        .expect("staged")
        .bytes()
        .await
        .expect("bytes");
    let staged = read_parquet(body).expect("parquet");
    assert_eq!(staged[0].num_rows(), 3);
    assert!(sqls[0].starts_with("CREATE TEMP TABLE loams_stage_"));
    assert!(sqls[2].starts_with("MERGE INTO public.orders USING loams_stage_"));
    assert!(sqls[2].contains("ON public.orders.\"id\" = s.\"id\""));
    assert!(sqls[3].starts_with("DROP TABLE loams_stage_"));
    state.finished.store(true, Ordering::SeqCst);
    let ack = writer.await.expect("task").expect("write");
    assert_eq!(ack.rows, 3);
    assert!(
        store.get(&Path::from(key.as_str())).await.is_err(),
        "the staged file is removed after the load"
    );
}

/// Nightly against a real Serverless workgroup: the rows and `UNLOAD`
/// paths over the same query, and the sink.
async fn nightly() -> bool {
    real(
        &[
            "LOAMS_REDSHIFT_WORKGROUP",
            "LOAMS_REDSHIFT_DATABASE",
            "LOAMS_REDSHIFT_UNLOAD_URI",
            "LOAMS_REDSHIFT_IAM_ROLE",
            "LOAMS_AWS_ACCESS_KEY_ID",
            "LOAMS_AWS_SECRET_ACCESS_KEY",
            "LOAMS_AWS_REGION",
        ],
        "the Redshift nightly",
    )
}

fn nightly_config(extra: serde_json::Value) -> (serde_json::Value, StaticSecrets) {
    let mut c = serde_json::json!({
        "region": env("LOAMS_AWS_REGION"),
        "workgroup_name": env("LOAMS_REDSHIFT_WORKGROUP"),
        "database": env("LOAMS_REDSHIFT_DATABASE"),
        "auth": {"access_key_id": "k", "secret_access_key": "s"},
    });
    for (k, v) in extra.as_object().expect("object") {
        c[k] = v.clone();
    }
    (
        c,
        StaticSecrets::new()
            .with("k", env("LOAMS_AWS_ACCESS_KEY_ID"))
            .with("s", env("LOAMS_AWS_SECRET_ACCESS_KEY")),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redshift_unload_bulk_arrow_fixture() {
    unload_local().await;
    recorded("redshift");
    if !nightly().await {
        return;
    }
    let q = "SELECT n FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY n";
    let (cfg_json, secrets) = nightly_config(serde_json::json!({
        "query": q,
        "unload": {"s3_uri": env("LOAMS_REDSHIFT_UNLOAD_URI"), "iam_role": env("LOAMS_REDSHIFT_IAM_ROLE")},
    }));
    let c = ctx(cfg_json.clone(), secrets);
    let (_, f) = Redshift.source(&c).expect("config").expect("source");
    let mut src = f.create(&split()).expect("create");
    src.open(&split(), None).await.expect("open");
    let mut rows = 0;
    let budget = Budget {
        max_rows: 1000,
        max_bytes: 8 << 20,
    };
    while let Some(b) = src.next(budget).await.expect("next") {
        rows += b.events.rows();
    }
    assert_eq!(rows, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redshift_copy_merge_sink_fixture() {
    copy_merge_local().await;
    recorded("redshift");
    if !nightly().await || !real(&["LOAMS_REDSHIFT_SINK_TABLE"], "the Redshift sink nightly") {
        return;
    }
    let (cfg_json, secrets) = nightly_config(serde_json::json!({"sink": {
        "table": env("LOAMS_REDSHIFT_SINK_TABLE"), "mode": "merge", "keys": ["id"],
        "staging": {"s3_uri": env("LOAMS_REDSHIFT_UNLOAD_URI"), "iam_role": env("LOAMS_REDSHIFT_IAM_ROLE")}
    }}));
    let c = ctx(cfg_json, secrets);
    let f = Redshift.sink(&c).expect("config").expect("sink");
    let loams_connector_sdk::SinkHandle::Plain(mut sink) = f.create().expect("create") else {
        panic!("plain")
    };
    let batch = EventBatch::Events(
        (0..3)
            .map(|i| {
                CloudEvent::new(format!("e/{i}"), "/t", "t").with_data(
                    "application/json",
                    format!("{{\"id\":{i},\"name\":\"n{i}\"}}"),
                )
            })
            .collect(),
    );
    sink.write(batch.clone()).await.expect("write");
    // A retried batch merges onto the same keys.
    sink.write(batch).await.expect("write again");
}
