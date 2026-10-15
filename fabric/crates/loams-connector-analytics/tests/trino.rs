//! Trino, a source only: the client protocol's `nextUri` paging, resumed
//! after a restart; against a local endpoint always and against real Trino
//! (compose `trino`) in `connectors-p2`.

mod common;

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use bytes::Bytes;
use common::*;
use http::{Response, StatusCode};
use loams_connector_analytics::query::QUERY_SPLIT;
use loams_connector_analytics::trino::Trino;
use loams_connector_sdk::{
    Budget, ConnectorFactory, EventBatch, Position, Source, Split, StaticSecrets,
};

/// A source made by the factory, as the runtime makes it.
fn source(url: &str, query: &str) -> Box<dyn Source> {
    let c = ctx(
        serde_json::json!({"url": url, "user": "loams", "catalog": "tpch", "schema": "tiny", "query": query}),
        StaticSecrets::new(),
    );
    let (_, factory) = Trino.source(&c).expect("config").expect("a source");
    factory
        .create(&Split::new(QUERY_SPLIT, serde_json::json!({})))
        .expect("create")
}

/// Read up to `max` batches of `rows` rows: (the `id`-column values, the
/// event ids, the position after each batch).
async fn read(
    src: &mut Box<dyn Source>,
    rows: usize,
    max: usize,
    column: &str,
) -> (Vec<i64>, Vec<String>, Vec<Position>) {
    let (mut vals, mut ids, mut pos) = (Vec::new(), Vec::new(), Vec::new());
    let budget = Budget {
        max_rows: rows,
        max_bytes: 1 << 20,
    };
    while pos.len() < max {
        let Some(b) = src.next(budget).await.expect("next") else {
            break;
        };
        let EventBatch::Arrow { batch, envelope } = b.events else {
            panic!("Trino batches are Arrow")
        };
        assert!(batch.num_rows() <= rows);
        let col = batch
            .column_by_name(column)
            .expect("column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bigint")
            .clone();
        vals.extend((0..col.len()).map(|i| col.value(i)));
        ids.push(envelope.id);
        pos.push(b.position);
    }
    (vals, ids, pos)
}

/// A fake coordinator: 10 rows in pages of 4. A token is forgotten (`410`)
/// once the next one has been requested, as Trino does.
async fn fake_trino() -> Fake {
    let latest = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Fake::start(move |req, _n| {
        let base = format!(
            "http://{}",
            req.headers
                .get("host")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("localhost")
        );
        assert_eq!(req.header("x-trino-user").as_deref(), Some("loams"));
        if req.method == "POST" {
            assert_eq!(req.uri, "/v1/statement");
            assert_eq!(&req.body[..], b"SELECT id FROM t ORDER BY id");
            latest.store(0, std::sync::atomic::Ordering::SeqCst);
            return ok(serde_json::json!({
                "id": "q1", "nextUri": format!("{base}/v1/statement/executing/q1/t/1"),
                "stats": {"state": "QUEUED"}
            }));
        }
        let k: usize = req
            .uri
            .rsplit('/')
            .next()
            .and_then(|s| s.parse().ok())
            .expect("token");
        let prev = latest.fetch_max(k, std::sync::atomic::Ordering::SeqCst);
        if k < prev {
            return Response::builder()
                .status(StatusCode::GONE)
                .body(Bytes::new())
                .expect("response");
        }
        let from = (k as i64 - 1) * 4;
        let to = (from + 4).min(10);
        let data: Vec<serde_json::Value> = (from..to).map(|i| serde_json::json!([i])).collect();
        let mut reply = serde_json::json!({
            "id": "q1",
            "columns": [{"name": "id", "type": "bigint"}],
            "data": data,
        });
        if to < 10 {
            reply["nextUri"] =
                serde_json::json!(format!("{base}/v1/statement/executing/q1/t/{}", k + 1));
        }
        ok(reply)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trino_nexturi_paging_resumes() {
    let fake = fake_trino().await;
    let q = "SELECT id FROM t ORDER BY id";
    let split = Split::new(QUERY_SPLIT, serde_json::json!({}));

    // A reads three batches of three rows (rows 0..=6) and stops.
    let mut a = source(&fake.url(), q);
    a.open(&split, None).await.expect("open");
    let (va, ia, pa) = read(&mut a, 3, 3, "id").await;
    drop(a);
    assert_eq!(va, (0..7).collect::<Vec<_>>());

    // B resumes from A's last position: Trino still has that page, so B
    // fetches it again (no new query) and reads to the end.
    let mut b = source(&fake.url(), q);
    b.open(&split, Some(pa[2].clone())).await.expect("open");
    let (vb, ib, _) = read(&mut b, 3, usize::MAX, "id").await;
    assert_eq!(
        [va.clone(), vb.clone()].concat(),
        (0..10).collect::<Vec<_>>()
    );
    let posts = fake.seen().iter().filter(|s| s.method == "POST").count();
    assert_eq!(posts, 1, "the resume did not run the query again");

    // C resumes from the same position after Trino moved past the page
    // (410): it runs the query again and skips the rows already taken,
    // yielding the same rows and the same ids as B.
    let mut c = source(&fake.url(), q);
    c.open(&split, Some(pa[2].clone())).await.expect("open");
    let (vc, ic, _) = read(&mut c, 3, usize::MAX, "id").await;
    assert_eq!(vc, vb);
    assert_eq!(ic, ib);
    let posts = fake.seen().iter().filter(|s| s.method == "POST").count();
    assert_eq!(posts, 2);
    assert!(ia.iter().all(|i| !ib.contains(i)), "ids are per row range");

    if real(&["LOAMS_CONNECTORS_P2_STACK"], "the real Trino half") {
        let url =
            std::env::var("LOAMS_TRINO_URL").unwrap_or_else(|_| "http://localhost:8080".to_owned());
        let q = "SELECT nationkey FROM tpch.tiny.nation ORDER BY nationkey";
        let mut a = source(&url, q);
        a.open(&split, None).await.expect("open");
        let (va, _, pa) = read(&mut a, 4, 2, "nationkey").await;
        drop(a);
        let mut b = source(&url, q);
        b.open(&split, Some(pa[1].clone())).await.expect("open");
        let (vb, _, _) = read(&mut b, 4, usize::MAX, "nationkey").await;
        assert_eq!([va, vb].concat(), (0..25).collect::<Vec<_>>());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trino_source_only() {
    let c = ctx(
        serde_json::json!({"url": "http://trino:8080", "user": "loams", "query": "SELECT 1"}),
        StaticSecrets::new(),
    );
    assert_eq!(Trino.id(), "trino");
    assert!(Trino.source(&c).expect("ok").is_some());
    assert!(Trino.sink(&c).expect("ok").is_none(), "Trino has no sink");

    if real(&["LOAMS_CONNECTORS_P2_STACK"], "the real Trino half") {
        let url =
            std::env::var("LOAMS_TRINO_URL").unwrap_or_else(|_| "http://localhost:8080".to_owned());
        let mut s = source(
            &url,
            "SELECT nationkey FROM tpch.tiny.nation ORDER BY nationkey",
        );
        s.open(&Split::new(QUERY_SPLIT, serde_json::json!({})), None)
            .await
            .expect("open");
        let (v, ids, _) = read(&mut s, 1000, usize::MAX, "nationkey").await;
        assert_eq!(v, (0..25).collect::<Vec<_>>());
        assert_eq!(ids.len(), 1);
    }
}
