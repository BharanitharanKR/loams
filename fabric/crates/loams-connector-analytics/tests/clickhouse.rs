//! The ClickHouse ★'s native sink (OR-8): one insert per batch with a dedup
//! token that survives the pipeline's retries; against a local endpoint
//! always, against real ClickHouse and Loams House when the job provides
//! them.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::*;
use http::{Response, StatusCode};
use loams_connector_analytics::clickhouse_sink::ClickHouse;
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{
    CloudEvent, ConnectorFactory, EventBatch, Pipeline, PipelineConfig, SinkHandle, StaticSecrets,
};

const GROUP: &str = "flow-clickhouse";

fn small() -> PipelineConfig {
    PipelineConfig {
        max_batch_rows: 5,
        max_batch_bytes: 256 << 10,
        linger: Duration::from_millis(5),
        max_in_flight_batches: 1,
        memory_budget_bytes: 4 << 20,
        checkpoint_interval: Duration::ZERO,
        retry: loams_connector_sdk::RetryPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            ..loams_connector_sdk::RetryPolicy::default()
        },
    }
}

fn events(from: u64, n: u64) -> Vec<CloudEvent> {
    (from..from + n)
        .map(|i| {
            CloudEvent::new(
                format!("e/{i}"),
                "/test",
                "io.loams.dev.flow.test.record.v1",
            )
            .with_data(
                "application/json",
                format!("{{\"id\":{i},\"name\":\"n{i}\"}}"),
            )
        })
        .collect()
}

fn sink_for(
    config: serde_json::Value,
    secrets: StaticSecrets,
) -> Box<dyn loams_connector_sdk::Sink> {
    let f = ClickHouse
        .sink(&ctx(config, secrets))
        .expect("config")
        .expect("a sink");
    match f.create().expect("create") {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("the ClickHouse sink is plain"),
    }
}

/// The pipeline retries a batch after a 503: both attempts carry the same
/// `insert_deduplication_token`, and different batches carry different ones.
async fn token_survives_retry_locally() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let fake = Fake::start(move |_req, _n| {
        // The first insert fails as a busy server does.
        if c.fetch_add(1, Ordering::SeqCst) == 0 {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("x-clickhouse-exception-code", "241")
                .body(Bytes::from_static(
                    b"Code: 241. DB::Exception: Memory limit",
                ))
                .expect("response");
        }
        Response::new(Bytes::new())
    })
    .await;
    let fabric = MemoryFabric::new();
    for ev in events(0, 10) {
        fabric.push(ev);
    }
    let factory = ClickHouse
        .sink(&ctx(
            serde_json::json!({"url": fake.url(), "database": "analytics", "table": "events",
                               "user": "loams", "password": "ch"}),
            StaticSecrets::new().with("ch", "s3cret-pass"),
        ))
        .expect("config")
        .expect("a sink");
    let handle = Pipeline::sink(instance(), small())
        .reader(Box::new(fabric.reader(GROUP)))
        .sink(Arc::from(factory))
        .start()
        .await
        .expect("starts");
    eventually("all delivered", || fabric.committed(GROUP) >= 10).await;
    handle.stop().await;

    let seen = fake.seen();
    assert_eq!(seen.len(), 3, "two batches, the first retried once");
    let token = |i: usize| seen[i].param("insert_deduplication_token").expect("token");
    assert_eq!(token(0), token(1), "the retry carries the same token");
    assert_ne!(token(1), token(2), "another batch, another token");
    assert_eq!(seen[0].body, seen[1].body);
    for s in &seen {
        assert_eq!(s.method, "POST");
        assert_eq!(
            s.param("query").as_deref(),
            Some("INSERT INTO analytics.events FORMAT JSONEachRow")
        );
        assert_eq!(s.param("insert_deduplicate").as_deref(), Some("1"));
        assert_eq!(s.param("async_insert").as_deref(), Some("0"));
        assert_eq!(s.header("x-clickhouse-user").as_deref(), Some("loams"));
        assert_eq!(s.header("x-clickhouse-key").as_deref(), Some("s3cret-pass"));
    }
    let first: Vec<&[u8]> = seen[1]
        .body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(first.len(), 5);
    assert_eq!(first[0], br#"{"id":0,"name":"n0"}"#);
}

/// An Arrow batch goes as `FORMAT ArrowStream`, a stream the server can
/// read back.
async fn arrow_batches_go_as_arrow_stream() {
    let fake = Fake::start(|_req, _n| Response::new(Bytes::new())).await;
    let mut sink = sink_for(
        serde_json::json!({"url": fake.url(), "table": "events"}),
        StaticSecrets::new(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .expect("batch");
    let envelope = CloudEvent::new(
        "run/0/0-1",
        "/connectors/orders/t",
        "io.loams.dev.flow.t.rows.v1",
    );
    let ack = sink
        .write(EventBatch::Arrow {
            batch: rb.clone(),
            envelope,
        })
        .await
        .expect("write");
    assert_eq!(ack.rows, 2);
    let seen = fake.seen();
    assert_eq!(
        seen[0].param("query").as_deref(),
        Some("INSERT INTO default.events FORMAT ArrowStream")
    );
    assert!(
        seen[0].header("x-clickhouse-key").is_none(),
        "no password configured"
    );
    let back =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(seen[0].body.clone()), None)
            .expect("ipc")
            .collect::<Result<Vec<_>, _>>()
            .expect("batches");
    assert_eq!(back, vec![rb]);
}

/// A plain HTTP query against a real server (setup and counts).
async fn ch_query(url: &str, user: &str, password: &str, sql: &str) -> String {
    let client = loams_connector_sdk::http::client(Arc::new(loams_connector_sdk::AllowAll));
    let req = http::Request::post(format!("{}/", url.trim_end_matches('/')))
        .header("x-clickhouse-user", user)
        .header("x-clickhouse-key", password)
        .body(Bytes::from(sql.to_owned()))
        .expect("request");
    let resp = client.send(req).await.expect("query");
    assert!(resp.status().is_success(), "{sql}: {:?}", resp.body());
    String::from_utf8_lossy(resp.body()).trim().to_owned()
}

/// The same batch written twice lands once (the server drops the second
/// block by its token), and Arrow batches insert too.
async fn dedup_on_real_server(url: &str, user: &str, password: &str, what: &str) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let table = format!("loams_dedup_{nanos}");
    ch_query(
        url,
        user,
        password,
        &format!(
            "CREATE TABLE default.{table} (id Int64, name String) ENGINE = MergeTree ORDER BY id \
             SETTINGS non_replicated_deduplication_window = 100"
        ),
    )
    .await;
    let mut sink = sink_for(
        serde_json::json!({"url": url, "table": table, "user": user, "password": "pw"}),
        StaticSecrets::new().with("pw", password.to_owned()),
    );
    let batch = EventBatch::Events(events(0, 7));
    sink.write(batch.clone()).await.expect("write");
    sink.write(batch).await.expect("retried write");
    sink.write(EventBatch::Events(events(7, 3)))
        .await
        .expect("write");
    let n = ch_query(
        url,
        user,
        password,
        &format!("SELECT count() FROM default.{table}"),
    )
    .await;
    assert_eq!(n, "10", "{what}: the retried batch landed once");
    ch_query(url, user, password, &format!("DROP TABLE default.{table}")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clickhouse_sink_dedup_token_on_retry() {
    token_survives_retry_locally().await;
    arrow_batches_go_as_arrow_stream().await;
    if real(
        &["LOAMS_CONNECTORS_STACK", "LOAMS_CLICKHOUSE_URL"],
        "the real ClickHouse half",
    ) {
        let user = std::env::var("LOAMS_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
        dedup_on_real_server(
            &env("LOAMS_CLICKHOUSE_URL"),
            &user,
            &env("LOAMS_CLICKHOUSE_PASSWORD"),
            "ClickHouse",
        )
        .await;
    }
}

/// Loams House serves ClickHouse's HTTP interface: the same sink against it,
/// after the real ClickHouse (OR-2: House is an extra target, never a
/// stand-in).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clickhouse_house_extra_target() {
    if !real(
        &["LOAMS_HOUSE_CLICKHOUSE_URL"],
        "Loams House as an extra ClickHouse target",
    ) {
        return;
    }
    let user =
        std::env::var("LOAMS_HOUSE_CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_owned());
    dedup_on_real_server(
        &env("LOAMS_HOUSE_CLICKHOUSE_URL"),
        &user,
        &env("LOAMS_HOUSE_CLICKHOUSE_PASSWORD"),
        "Loams House",
    )
    .await;
}

/// The ★'s source stays CN1's until Task 8: this factory is the sink.
#[test]
fn clickhouse_factory_is_the_sink() {
    let c = ctx(
        serde_json::json!({"url": "http://ch:8123", "table": "events"}),
        StaticSecrets::new(),
    );
    assert!(ClickHouse.source(&c).expect("ok").is_none());
    assert!(ClickHouse.sink(&c).expect("ok").is_some());
    let bad = ctx(
        serde_json::json!({"url": "http://ch:8123", "table": "x;DROP"}),
        StaticSecrets::new(),
    );
    assert!(ClickHouse.sink(&bad).is_err());
}
