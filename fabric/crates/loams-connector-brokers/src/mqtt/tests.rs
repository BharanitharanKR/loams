//! The MQTT connector against the in-process broker of [`super::fake`]. The
//! same scenarios run against Eclipse Mosquitto in `tests/mqtt.rs`.

// `unwrap_used` is a warning and CI runs `-D warnings`; tests unwrap freely.
#![allow(clippy::unwrap_used)]
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorContext, ConnectorFactory, EventBatch, InstanceKey, Scrubber,
    Sink, SinkHandle, Source, StaticSecrets,
};
use serde_json::json;

use super::codec::Publish;
use super::fake::FakeBroker;
use super::*;

fn ctx(config: serde_json::Value) -> ConnectorContext {
    let secrets = Arc::new(StaticSecrets::new().with("mqtt-pw", "s3cret-pw"));
    ConnectorContext::new(
        InstanceKey::new(1, "devices"),
        config,
        secrets,
        Scrubber::new(),
    )
}

async fn source(config: serde_json::Value) -> Box<dyn Source> {
    let c = ctx(config);
    let (mut en, f) = Mqtt.source(&c).unwrap().unwrap();
    let splits = en.discover(&[]).await.unwrap();
    let mut s = f.create(&splits[0]).unwrap();
    s.open(&splits[0], None).await.unwrap();
    s
}

fn sink(config: serde_json::Value) -> Box<dyn Sink> {
    let c = ctx(config);
    match Mqtt.sink(&c).unwrap().unwrap().create().unwrap() {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("plain sink"),
    }
}

fn ce(i: usize) -> CloudEvent {
    CloudEvent::new(format!("evt-{i}"), "/devices", "com.example.reading.v1")
        .with_data("application/json", Bytes::from(format!("{{\"n\":{i}}}")))
}

fn ce_publish(i: usize) -> Publish {
    publish(&ce(i), "devices/1/temp".into(), false)
}

const BUDGET: Budget = Budget {
    max_rows: 10,
    max_bytes: 1 << 20,
};

fn ids(b: &Option<loams_connector_sdk::SourceBatch>) -> Vec<String> {
    match b.as_ref().map(|b| &b.events) {
        Some(EventBatch::Events(v)) => v.iter().map(|e| e.id.clone()).collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
async fn source_and_sink_round_trip_through_a_broker() {
    let broker = FakeBroker::start().await;
    let cfg = json!({
        "url": broker.url(), "username": "loams", "password": "mqtt-pw",
        "source": {"topics": ["devices/#"], "poll_wait_ms": 200},
        "sink": {"topic": "devices/1/temp"}
    });
    let mut src = source(cfg.clone()).await;
    let mut out = sink(cfg);
    let batch: Vec<CloudEvent> = (0..25).map(ce).collect();
    let ack = out.write(EventBatch::Events(batch)).await.unwrap();
    assert_eq!(ack.rows, 25);
    let published = broker.published();
    assert_eq!(published.len(), 25);
    assert!(published.iter().all(|p| p.qos == 1));
    assert_eq!(
        published[0].content_type.as_deref(),
        Some("application/json")
    );
    let mut got = Vec::new();
    for _ in 0..10 {
        let b = src.next(BUDGET).await.unwrap();
        got.extend(ids(&b));
        if let Some(b) = b {
            src.committed(&b.position).await.unwrap();
        }
        if got.len() == 25 {
            break;
        }
    }
    let want: Vec<String> = (0..25).map(|i| format!("evt-{i}")).collect();
    assert_eq!(
        got, want,
        "CloudEvents pass through with their ids, in order"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.unacked("loams-devices-0"), 0);
    src.close().await.unwrap();
}

#[tokio::test]
async fn mqtt5_qos1_no_loss_on_restart() {
    // A message is acknowledged only after `committed`; a source that dies
    // before that leaves it in its persistent session, and the next one gets
    // it again (DUP). Nothing committed comes back; nothing is lost.
    let broker = FakeBroker::start().await;
    let cfg =
        json!({"url": broker.url(), "source": {"topics": ["devices/#"], "poll_wait_ms": 200}});
    let mut first = source(cfg.clone()).await;
    for i in 0..20 {
        broker.inject(ce_publish(i));
    }
    let a = first.next(BUDGET).await.unwrap();
    let committed = ids(&a);
    assert_eq!(committed.len(), 10);
    first.committed(&a.unwrap().position).await.unwrap();
    let b = first.next(BUDGET).await.unwrap();
    let uncommitted = ids(&b);
    assert_eq!(uncommitted.len(), 10);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Crash: no DISCONNECT, no PUBACK for the second batch.
    drop(first);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.unacked("loams-devices-0"), 10);

    let mut second = source(cfg).await;
    let mut again = Vec::new();
    for _ in 0..5 {
        let b = second.next(BUDGET).await.unwrap();
        again.extend(ids(&b));
        if let Some(b) = b {
            second.committed(&b.position).await.unwrap();
        }
        if again.len() >= 10 {
            break;
        }
    }
    assert_eq!(
        again, uncommitted,
        "the unacknowledged batch is redelivered"
    );
    assert!(again.iter().all(|id| !committed.contains(id)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.unacked("loams-devices-0"), 0);
}

#[tokio::test]
async fn receive_maximum_is_the_backpressure() {
    let broker = FakeBroker::start().await;
    let cfg = json!({
        "url": broker.url(),
        "source": {"topics": ["devices/#"], "max_batch": 5, "poll_wait_ms": 100},
        "pipeline": {"max_in_flight_batches": 1}
    });
    let mut src = source(cfg).await;
    for i in 0..20 {
        broker.inject(ce_publish(i));
    }
    let a = src.next(BUDGET).await.unwrap();
    assert_eq!(ids(&a).len(), 5);
    // Nothing acknowledged: the broker holds the other 15 back.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.unacked("loams-devices-0"), 5);
    assert_eq!(broker.queued("loams-devices-0"), 15);
    assert!(ids(&src.next(BUDGET).await.unwrap()).is_empty());
    src.committed(&a.unwrap().position).await.unwrap();
    let b = src.next(BUDGET).await.unwrap();
    assert_eq!(ids(&b).len(), 5);
}

#[tokio::test]
async fn plain_messages_get_message_events() {
    let broker = FakeBroker::start().await;
    let cfg =
        json!({"url": broker.url(), "source": {"topics": ["devices/+/temp"], "poll_wait_ms": 200}});
    let mut src = source(cfg).await;
    broker.inject(Publish {
        topic: "devices/9/temp".into(),
        qos: 1,
        payload: Bytes::from_static(b"21.5"),
        user_properties: vec![("unit".into(), "C".into())],
        ..Publish::default()
    });
    let b = src.next(BUDGET).await.unwrap().unwrap();
    let EventBatch::Events(v) = &b.events else {
        panic!("events")
    };
    assert_eq!(v[0].ty, "io.loams.dev.flow.mqtt.message.v1");
    assert_eq!(v[0].subject.as_deref(), Some("devices/9/temp"));
    assert_eq!(&v[0].data[..], b"21.5");
}

#[tokio::test]
async fn a_refused_login_is_an_auth_error_without_the_password() {
    // A server that refuses every CONNECT with 0x86 (bad user name or
    // password).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 256];
        let _ = s.read(&mut buf).await;
        let _ = s.write_all(&[0x20, 3, 0, 0x86, 0]).await;
    });
    let c = ctx(json!({
        "url": format!("mqtt://{addr}"), "username": "loams", "password": "mqtt-pw",
        "source": {"topics": ["x"]}
    }));
    let (mut en, f) = Mqtt.source(&c).unwrap().unwrap();
    let splits = en.discover(&[]).await.unwrap();
    let mut s = f.create(&splits[0]).unwrap();
    let e = s.open(&splits[0], None).await.unwrap_err();
    assert_eq!(e.class, loams_connector_sdk::ErrorClass::Auth, "{e}");
    assert!(!e.message.contains("s3cret-pw"));
}

#[test]
fn configs_are_checked() {
    assert!(MqttConfig::parse(&json!({"url": "tcp://h"})).is_err());
    assert!(MqttConfig::parse(&json!({"url": "mqtt://u:p@h"})).is_err());
    assert!(MqttConfig::parse(&json!({"url": "mqtt://h", "password": "x"})).is_err());
    assert!(MqttConfig::parse(&json!({"url": "mqtt://h", "source": {"topics": []}})).is_err());
    assert_eq!(declared().sink, Delivery::AtLeastOnce);
}
