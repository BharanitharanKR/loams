//! Grafeo through `loams.graph.v1` over Connect: against a loopback Connect
//! peer that checks the generated messages and the API key (everywhere),
//! and against a real Loams graph endpoint when `LOAMS_GRAFEO_URL` and
//! `LOAMS_GRAFEO_API_KEY` are set.
#![allow(clippy::unwrap_used)]

mod common;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use buffa::Message;
use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use loams_connector_protocols::grafeo::{Grafeo, envelope, to_json, unframe};
use loams_connector_sdk::{
    Budget, ConnectorContext, ConnectorFactory, ErrorClass, EventBatch, Position, SinkHandle,
    Source,
};
use loams_flow_proto::loams::graph::v1 as pb;
use pb::__buffa::oneof::value::Kind;
use serde_json::{Value, json};

const KEY: &str = "loams_k1_synthetic-graph-secret";

#[derive(Default)]
struct Peer {
    batches: Vec<pb::ExecuteBatchRequest>,
    streams: Vec<pb::ExecuteStreamRequest>,
    /// (cursor, node id) of the stored nodes.
    nodes: Vec<(i64, i64)>,
}

fn int(v: Option<&pb::Value>) -> Option<i64> {
    match v.and_then(|v| v.kind.clone()) {
        Some(Kind::Int64(i)) => Some(i),
        _ => None,
    }
}

fn val(k: Kind) -> pb::Value {
    pb::Value {
        kind: Some(k),
        ..Default::default()
    }
}

async fn handle(
    peer: Arc<Mutex<Peer>>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let path = req.uri().path().to_owned();
    let body = req.into_body().collect().await.unwrap().to_bytes();
    let json_err = |status: u16, code: &str| {
        let mut r = Response::new(Full::new(Bytes::from(
            json!({"code": code, "message": code}).to_string(),
        )));
        *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
        r
    };
    if auth.as_deref() != Some(&format!("Bearer {KEY}")) {
        return Ok(json_err(401, "unauthenticated"));
    }
    match path.as_str() {
        "/loams.graph.v1.GraphService/ExecuteBatch" => {
            let r = pb::ExecuteBatchRequest::decode_from_slice(&body).unwrap();
            let n = r.statements.len() as u32;
            peer.lock().unwrap().batches.push(r);
            let resp = pb::ExecuteBatchResponse {
                committed: true,
                committed_through: n,
                ..Default::default()
            };
            Ok(Response::new(Full::new(Bytes::from(resp.encode_to_vec()))))
        }
        "/loams.graph.v1.GraphService/ExecuteStream" => {
            let msgs = unframe(&[body.as_ref(), &envelope(2, b"{}")].concat(), false).unwrap();
            let r = pb::ExecuteStreamRequest::decode_from_slice(&msgs[0]).unwrap();
            let inner = r.request.as_option().unwrap();
            let cursor = int(inner.parameters.get("cursor"));
            let after = int(inner.parameters.get("after")).unwrap();
            let limit = int(inner.parameters.get("limit")).unwrap() as usize;
            let mut p = peer.lock().unwrap();
            p.streams.push(r.clone());
            let mut rows: Vec<_> = p
                .nodes
                .iter()
                .filter(|(c, id)| cursor.is_none_or(|k| *c > k || (*c == k && *id > after)))
                .copied()
                .collect();
            rows.sort();
            rows.truncate(limit);
            let mut out = Vec::new();
            // Two chunks, as a server cuts a stream.
            let half = rows.len() / 2;
            for (i, part) in [&rows[..half], &rows[half..]].iter().enumerate() {
                let chunk = pb::ResultChunk {
                    columns: if i == 0 {
                        vec!["n".into(), "cursor".into(), "eid".into()]
                    } else {
                        vec![]
                    },
                    rows: part
                        .iter()
                        .map(|(c, id)| pb::Row {
                            values: vec![
                                val(Kind::Node(Box::new(pb::Node {
                                    id: *id as u64,
                                    labels: vec!["Doc".into()],
                                    ..Default::default()
                                }))),
                                val(Kind::Int64(*c)),
                                val(Kind::Int64(*id)),
                            ],
                            ..Default::default()
                        })
                        .collect(),
                    last: i == 1,
                    ..Default::default()
                };
                out.extend_from_slice(&envelope(0, &chunk.encode_to_vec()));
            }
            out.extend_from_slice(&envelope(2, b"{}"));
            let mut resp = Response::new(Full::new(Bytes::from(out)));
            resp.headers_mut()
                .insert("content-type", "application/connect+proto".parse().unwrap());
            Ok(resp)
        }
        _ => Ok(json_err(404, "unimplemented")),
    }
}

async fn peer(nodes: Vec<(i64, i64)>) -> (String, Arc<Mutex<Peer>>) {
    let state = Arc::new(Mutex::new(Peer {
        nodes,
        ..Default::default()
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let st = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                return;
            };
            let st = st.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |r| handle(st.clone(), r));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(s), svc)
                    .await;
            });
        }
    });
    (format!("http://{addr}"), state)
}

fn config(url: &str, key: Option<&str>) -> Value {
    let mut c = json!({
        "url": url, "namespace": "acme", "graph": "docs",
        "sink": {"statement": "MERGE (d:Doc {id: $id}) SET d.title = $title", "batch": 10},
        "source": {"label": "Doc", "cursor": "rev", "page_size": 3}
    });
    if let Some(k) = key {
        c["api_key"] = json!(k);
    }
    c
}

async fn source(ctx: &ConnectorContext, pos: Option<Position>) -> Box<dyn Source> {
    let (mut en, f) = Grafeo.source(ctx).unwrap().unwrap();
    let split = en.discover(&[]).await.unwrap().remove(0);
    let mut s = f.create(&split).unwrap();
    s.open(&split, pos).await.unwrap();
    s
}

#[tokio::test]
async fn grafeo_sink_batches_through_loams_graph_v1() {
    let (url, state) = peer(vec![]).await;
    let ctx = ctx(config(&url, Some("gk")), &json!({"gk": KEY}));
    let SinkHandle::Plain(mut sink) = Grafeo.sink(&ctx).unwrap().unwrap().create().unwrap() else {
        panic!("plain")
    };
    let evs: Vec<_> = (0..12)
        .map(|i| {
            event(
                &format!("e{i}"),
                None,
                &json!({"id": i, "title": format!("t{i}")}),
            )
        })
        .collect();
    sink.write(EventBatch::Events(evs.clone())).await.unwrap();
    let first = state.lock().unwrap().batches.clone();
    let p = Peer {
        batches: first,
        ..Default::default()
    };
    assert_eq!(p.batches.len(), 2, "batch: 10 statements per ExecuteBatch");
    let b = &p.batches[0];
    assert_eq!(
        (b.namespace.as_str(), b.graph.as_str(), b.atomic),
        ("acme", "docs", true)
    );
    assert_eq!(b.statements.len(), 10);
    assert_eq!(b.language, pb::QueryLanguage::QUERY_LANGUAGE_GQL);
    let st = &b.statements[3];
    assert_eq!(st.statement, "MERGE (d:Doc {id: $id}) SET d.title = $title");
    assert_eq!(to_json(st.parameters.get("title").unwrap()), json!("t3"));
    assert_eq!(to_json(st.parameters.get("ce_id").unwrap()), json!("e3"));
    assert!(b.idempotency_key.starts_with("loams-flow-"));
    // A retried batch is the same request: the same idempotency key.
    sink.write(EventBatch::Events(evs)).await.unwrap();
    let p = state.lock().unwrap();
    assert_eq!(p.batches[2].idempotency_key, p.batches[0].idempotency_key);
}

#[tokio::test]
async fn grafeo_source_cursor_resumes() {
    // Two nodes share cursor 20: the tiebreak on the node id keeps both.
    let (url, state) = peer(vec![(10, 1), (20, 2), (20, 3), (30, 4), (40, 5)]).await;
    let ctx = ctx(config(&url, Some("gk")), &json!({"gk": KEY}));
    let mut src = source(&ctx, None).await;
    let first = src
        .next(Budget {
            max_rows: 3,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    let ids: Vec<_> = events(&first).iter().map(|e| e.id.clone()).collect();
    assert_eq!(ids, ["docs/Doc/1@10", "docs/Doc/2@20", "docs/Doc/3@20"]);
    assert_eq!(first.position.display, "cursor 20 after 3");
    drop(src);
    let mut resumed = source(&ctx, Some(first.position)).await;
    let rest: Vec<_> = drain(&mut *resumed)
        .await
        .iter()
        .map(|e| e.id.clone())
        .collect();
    assert_eq!(rest, ["docs/Doc/4@30", "docs/Doc/5@40"]);
    let p = state.lock().unwrap();
    let req = p.streams[1].request.as_option().unwrap();
    assert!(req.read_only);
    assert!(req.statement.contains("$cursor") && req.statement.contains("$after"));
}

#[tokio::test]
async fn grafeo_requires_api_key() {
    let (url, _) = peer(vec![(1, 1)]).await;
    // No key in the config: refused before any request (auth: key).
    let Err(err) = Grafeo.sink(&ctx(config(&url, None), &json!({}))) else {
        panic!("refused")
    };
    assert_eq!(err.class, ErrorClass::Auth);
    assert!(Grafeo.source(&ctx(config(&url, None), &json!({}))).is_err());
    // A wrong key: the server's `unauthenticated` is an Auth error.
    let ctx = ctx(config(&url, Some("gk")), &json!({"gk": "loams_k1_wrong"}));
    let mut src = source(&ctx, None).await;
    let err = src.next(BUDGET).await.unwrap_err();
    assert_eq!(err.class, ErrorClass::Auth);
    assert!(!err.message.contains("loams_k1_wrong"));
}

/// A real Loams graph endpoint (`LOAMS_GRAFEO_URL`, `LOAMS_GRAFEO_API_KEY`,
/// and a graph `LOAMS_GRAFEO_NAMESPACE`/`LOAMS_GRAFEO_GRAPH`).
#[tokio::test]
async fn grafeo_against_a_loams_graph() {
    let Some(v) = env_all(&[
        "LOAMS_GRAFEO_URL",
        "LOAMS_GRAFEO_API_KEY",
        "LOAMS_GRAFEO_NAMESPACE",
        "LOAMS_GRAFEO_GRAPH",
    ]) else {
        return skip("grafeo_against_a_loams_graph", "LOAMS_GRAFEO_* unset");
    };
    let label = format!("LoamsT{}", std::process::id());
    let ctx = ctx(
        json!({"url": v[0], "api_key": "k", "namespace": v[2], "graph": v[3],
            "sink": {"statement": format!("MERGE (d:{label} {{key: $key}}) SET d.rev = $rev")},
            "source": {"label": label, "cursor": "rev", "page_size": 2}}),
        &json!({"k": v[1]}),
    );
    let SinkHandle::Plain(mut sink) = Grafeo.sink(&ctx).unwrap().unwrap().create().unwrap() else {
        panic!()
    };
    let evs: Vec<_> = (1..=3)
        .map(|i| {
            event(
                &format!("e{i}"),
                None,
                &json!({"key": format!("k{i}"), "rev": i}),
            )
        })
        .collect();
    sink.write(EventBatch::Events(evs)).await.unwrap();
    let mut src = source(&ctx, None).await;
    let first = src
        .next(Budget {
            max_rows: 2,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
        .unwrap();
    let mut resumed = source(&ctx, Some(first.position.clone())).await;
    assert_eq!(events(&first).len() + drain(&mut *resumed).await.len(), 3);
}
