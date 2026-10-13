//! Prometheus remote write: the sink into the receiver over loopback
//! (everywhere), and real Prometheus as both receiver and sender when the
//! `connectors-p2` stack is up.
#![allow(clippy::unwrap_used)]

mod common;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::*;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use loams_connector_protocols::prometheus::{
    Label, Prometheus, RemoteWriteReceiver, Sample, TimeSeries, WriteRequest, encode, route_path,
};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{
    ConnectorFactory, ErrorClass, EventBatch, FabricError, InstanceKey, SinkHandle,
};
use serde_json::{Value, json};

/// Serve `receiver` on `addr` (any path is the route).
async fn serve(receiver: Arc<RemoteWriteReceiver>, addr: &str) -> String {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                return;
            };
            let r = receiver.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let r = r.clone();
                    async move {
                        let headers = req.headers().clone();
                        let path = req.uri().path().to_owned();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let resp = if path == r.path() {
                            r.receive(&headers, &body).await
                        } else {
                            let mut x = Response::new(Bytes::new());
                            *x.status_mut() = http::StatusCode::NOT_FOUND;
                            x
                        };
                        let (parts, b) = resp.into_parts();
                        Ok::<_, Infallible>(Response::from_parts(parts, Full::new(b)))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(s), svc)
                    .await;
            });
        }
    });
    format!("http://127.0.0.1:{}", local.port())
}

fn metric(name: &str, job: &str, samples: &[(f64, i64)]) -> Value {
    json!({"labels": {"__name__": name, "job": job},
           "samples": samples.iter().map(|(v, t)| json!({"value": v, "timestamp": t})).collect::<Vec<_>>()})
}

#[tokio::test]
async fn prometheus_remote_write_roundtrip() {
    let instance = InstanceKey::new(7, "prom-in");
    let fabric = MemoryFabric::new();
    let receiver = Arc::new(RemoteWriteReceiver::new(
        &instance,
        Arc::new(fabric.clone()),
    ));
    assert_eq!(
        receiver.path(),
        "/v1/namespaces/7/fabric/prometheus/prom-in/write"
    );
    let base = serve(receiver, "127.0.0.1:0").await;

    let ctx = ctx(
        json!({"url": format!("{base}{}", route_path(7, "prom-in")), "batch": 1}),
        &json!({}),
    );
    let SinkHandle::Plain(mut sink) = Prometheus.sink(&ctx).unwrap().unwrap().create().unwrap()
    else {
        panic!("plain")
    };
    let inputs = [
        metric(
            "http_requests_total",
            "api",
            &[(1.0, 1_700_000_000_000), (3.0, 1_700_000_015_000)],
        ),
        metric("up", "db", &[(1.0, 1_700_000_000_000)]),
    ];
    let evs: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, m)| event(&format!("m{i}"), None, m))
        .collect();
    sink.write(EventBatch::Events(evs.clone())).await.unwrap();

    let got = fabric.events();
    assert_eq!(got.len(), 2, "one metric event per series");
    assert_eq!(got[0].ty, "io.loams.dev.flow.prometheus.metric.v1");
    assert_eq!(
        got[0].subject.as_deref(),
        Some("http_requests_total{job=\"api\"}")
    );
    assert_eq!(data(&got[0]), inputs[0]);
    assert_eq!(data(&got[1]), inputs[1]);
    // A resent request (Prometheus retrying) is deduplicated by id.
    sink.write(EventBatch::Events(evs)).await.unwrap();
    assert_eq!(fabric.events().len(), 2);
    assert_eq!(fabric.duplicates(), 2);
}

#[tokio::test]
async fn receiver_answers_by_fabric_state() {
    let instance = InstanceKey::new(7, "prom-in");
    let fabric = MemoryFabric::new();
    let r = RemoteWriteReceiver::new(&instance, Arc::new(fabric.clone()));
    let mut h = http::HeaderMap::new();
    h.insert("content-encoding", "snappy".parse().unwrap());
    h.insert("content-type", "application/x-protobuf".parse().unwrap());
    let body = encode(&WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".into(),
                value: "x".into(),
            }],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
            }],
        }],
        metadata: vec![],
    })
    .unwrap();
    assert_eq!(
        r.receive(&h, b"garbage").await.status(),
        400,
        "Prometheus drops a 4xx"
    );
    let mut v2 = h.clone();
    v2.insert(
        "content-type",
        "application/x-protobuf;proto=io.prometheus.write.v2.Request"
            .parse()
            .unwrap(),
    );
    assert_eq!(r.receive(&v2, &body).await.status(), 415);
    fabric.fail_appends([FabricError::retryable("busy")]);
    assert_eq!(
        r.receive(&h, &body).await.status(),
        503,
        "Prometheus retries a 5xx"
    );
    fabric.fail_appends([FabricError::new(
        ErrorClass::Throttled {
            after: Duration::from_secs(3),
        },
        "slow",
    )]);
    let t = r.receive(&h, &body).await;
    assert_eq!(t.status(), 429);
    assert_eq!(t.headers().get("retry-after").unwrap(), "3");
    assert_eq!(r.receive(&h, &body).await.status(), 204);
    assert_eq!(fabric.events().len(), 1);
}

/// Real Prometheus (`prometheus` on :9090, remote-write receiver enabled):
/// the sink writes a series and the query API reads it back; then
/// Prometheus, configured by `deploy/fabric/prometheus/prometheus.yml` to
/// remote-write to the host's port 19091, sends its own series to the
/// receiver.
#[tokio::test]
async fn prometheus_real_sender_and_receiver() {
    if !p2_stack() {
        return skip(
            "prometheus_real_sender_and_receiver",
            "LOAMS_CONNECTORS_P2_STACK unset",
        );
    }
    let prom =
        std::env::var("LOAMS_PROMETHEUS_URL").unwrap_or_else(|_| "http://localhost:9090".into());
    // Prometheus as receiver.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let name = format!("loams_test_{}", std::process::id());
    let ctx = ctx(json!({"url": format!("{prom}/api/v1/write")}), &json!({}));
    let SinkHandle::Plain(mut sink) = Prometheus.sink(&ctx).unwrap().unwrap().create().unwrap()
    else {
        panic!()
    };
    sink.write(EventBatch::Events(vec![event(
        "p",
        None,
        &metric(&name, "loams", &[(42.0, now)]),
    )]))
    .await
    .unwrap();
    let q = loams_connector_protocols::client::RestClient::connect(
        &prom,
        &loams_connector_protocols::client::AuthSpec::None,
        &*ctx.secrets,
        ctx.egress.clone(),
        Duration::from_secs(10),
        "",
    )
    .await
    .unwrap();
    let mut found = false;
    for _ in 0..20 {
        let v = q
            .json(
                http::Method::GET,
                &format!("/api/v1/query?query={name}"),
                None,
            )
            .await
            .unwrap();
        if v["data"]["result"][0]["value"][1] == "42" {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(found, "the written sample is queryable");

    // Prometheus as sender.
    let fabric = MemoryFabric::new();
    let receiver = Arc::new(RemoteWriteReceiver::new(
        &InstanceKey::new(7, "test"),
        Arc::new(fabric.clone()),
    ));
    serve(receiver, "0.0.0.0:19091").await;
    for _ in 0..90 {
        if fabric
            .events()
            .iter()
            .any(|e| e.subject.as_deref().is_some_and(|s| s.starts_with("up{")))
        {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("Prometheus sent no `up` series to the receiver within 90 s");
}
