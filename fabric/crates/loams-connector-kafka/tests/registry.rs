//! The Confluent Schema Registry client (R5.4) against a fake registry that
//! answers as Confluent's REST API documents. No Kafka involved.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use loams_connector_kafka::ConfluentRegistry;
use loams_connector_sdk::schema::registry::{SchemaFormat, SchemaRegistry, frame, unframe};
use loams_connector_sdk::{AllowAll, DenyLocal, ErrorClass, Secret};
use serde_json::{Value, json};

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    auth: Option<String>,
    body: Value,
}

async fn fake_registry() -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let log = log.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = body.collect().await.expect("body").to_bytes();
                        let s = Seen {
                            method: parts.method.to_string(),
                            path: parts.uri.path().to_owned(),
                            auth: parts
                                .headers
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned),
                            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                        };
                        log.lock().expect("log").push(s.clone());
                        let (status, body) = match (s.method.as_str(), s.path.as_str()) {
                            ("GET", "/schemas/ids/7") => (
                                200,
                                json!({ "schema": "syntax = \"proto3\"; message M { int64 a = 1; }", "schemaType": "PROTOBUF" }),
                            ),
                            ("GET", "/schemas/ids/8") => {
                                (200, json!({ "schema": "{\"type\":\"string\"}" }))
                            }
                            ("POST", "/subjects/orders-value/versions") => {
                                (200, json!({ "id": 7 }))
                            }
                            ("POST", "/subjects/strict-value/versions") => (
                                409,
                                json!({ "error_code": 409, "message": "Schema being registered is incompatible" }),
                            ),
                            _ => (
                                404,
                                json!({ "error_code": 40403, "message": "Schema not found" }),
                            ),
                        };
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::from_u16(status).expect("status"))
                                .header("content-type", "application/vnd.schemaregistry.v1+json")
                                .body(Full::new(Bytes::from(body.to_string())))
                                .expect("response"),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (base, seen)
}

#[tokio::test]
async fn registry_by_id_register_and_errors() {
    let (base, seen) = fake_registry().await;
    let reg = ConfluentRegistry::new(&base, Arc::new(AllowAll))
        .with_basic_auth("api-key", Secret::new(b"api-secret".to_vec()));

    let s = reg.by_id(7).await.expect("schema 7");
    assert_eq!(s.format, SchemaFormat::Protobuf);
    assert!(s.schema.starts_with(b"syntax"));
    // Cached: a schema id never changes.
    reg.by_id(7).await.expect("schema 7 again");
    assert_eq!(
        seen.lock()
            .expect("log")
            .iter()
            .filter(|s| s.path == "/schemas/ids/7")
            .count(),
        1
    );
    // AVRO is the registry's default type.
    assert_eq!(
        reg.by_id(8).await.expect("schema 8").format,
        SchemaFormat::Avro
    );
    let missing = reg.by_id(9).await.expect_err("unknown id");
    assert_eq!(missing.class, ErrorClass::Poison);

    let id = reg
        .register(
            "orders-value",
            SchemaFormat::Protobuf,
            Bytes::from_static(b"syntax = \"proto3\";"),
        )
        .await
        .expect("register");
    assert_eq!(id, 7);
    let post = seen
        .lock()
        .expect("log")
        .iter()
        .find(|s| s.method == "POST")
        .cloned()
        .expect("a POST");
    assert_eq!(post.body["schemaType"], "PROTOBUF");
    // Basic credentials, never echoed in an error.
    assert_eq!(post.auth.as_deref(), Some("Basic YXBpLWtleTphcGktc2VjcmV0"));
    let refused = reg
        .register(
            "strict-value",
            SchemaFormat::Avro,
            Bytes::from_static(b"{\"type\":\"int\"}"),
        )
        .await
        .expect_err("incompatible");
    assert_eq!(refused.class, ErrorClass::SchemaIncompatible);
    assert!(refused.message.contains("incompatible"));
    assert!(!refused.message.contains("api-secret"));
    assert!(
        reg.register("../admin", SchemaFormat::Avro, Bytes::from_static(b"{}"))
            .await
            .is_err()
    );

    // Framing round-trips with the registry's ids.
    let framed = frame(id, b"payload");
    assert_eq!(unframe(&framed).expect("frame").schema_id, 7);
}

#[tokio::test]
async fn registry_requests_are_egress_checked() {
    let (base, _) = fake_registry().await;
    let reg = ConfluentRegistry::new(&base, Arc::new(DenyLocal));
    let err = reg.by_id(7).await.expect_err("loopback refused");
    assert!(err.message.contains("egress refused"), "{err}");
}
