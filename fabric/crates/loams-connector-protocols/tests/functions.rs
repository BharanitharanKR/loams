//! Lambda, Cloud Run and Azure Functions sinks: synthetic fixtures of the
//! documented shapes, and Lambda against floci when the `connectors-p2`
//! stack is up.
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use common::*;
use loams_connector_protocols::functions::Functions;
use loams_connector_protocols::{OneTopic, ReplyTopics};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{ConnectorFactory, ErrorClass, EventBatch, Sink, SinkHandle};
use serde_json::{Value, json};

fn sink(cfg: Value, secrets: &Value, fabric: &MemoryFabric) -> Box<dyn Sink> {
    let topics: Arc<dyn ReplyTopics> = Arc::new(OneTopic(Arc::new(fabric.clone())));
    let ctx = ctx(cfg, secrets);
    match Functions::new()
        .with_replies(topics)
        .sink(&ctx)
        .unwrap()
        .unwrap()
        .create()
        .unwrap()
    {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("plain"),
    }
}

#[tokio::test]
async fn lambda_invoke_emits_reply() {
    let (_, scenario, proxy) = replay("lambda-cloud-run-azure-functions/invoke.json").await;
    let fabric = MemoryFabric::new();
    let cfg = with_proxy(&scenario["lambda"], &proxy.base_url());
    let mut s = sink(cfg, &scenario["secrets"], &fabric);

    s.write(EventBatch::Events(vec![event(
        "o-7",
        Some("order-7"),
        &json!({"order": 7}),
    )]))
    .await
    .unwrap();
    let replies = fabric.events();
    assert_eq!(replies.len(), 1);
    assert_eq!(
        replies[0].ty,
        "io.loams.dev.flow.lambda-cloud-run-azure-functions.reply.v1"
    );
    assert_eq!(replies[0].subject.as_deref(), Some("order-7"));
    assert_eq!(data(&replies[0]), json!({"echo": {"order": 7}}));

    // `Invoke` is SigV4-signed for `lambda`, synchronous.
    let req = &proxy.received()[0];
    let auth = req.headers.get("authorization").unwrap();
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDSYNTHETIC000000/"),
        "{auth}"
    );
    assert!(auth.contains("/eu-west-1/lambda/aws4_request"));
    assert!(
        auth.contains("x-amz-invocation-type"),
        "the invocation type is signed: {auth}"
    );
    assert_eq!(
        req.headers.get("x-amz-invocation-type").map(String::as_str),
        Some("RequestResponse")
    );
    let reply = String::from_utf8_lossy(&replies[0].data).into_owned();
    assert!(!reply.contains("synthetic-aws-secret"));

    // A function error is poison: the event goes to the DLQ.
    let err = s
        .write(EventBatch::Events(vec![event(
            "o-8",
            None,
            &json!({"order": "boom"}),
        )]))
        .await
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Poison);
    assert!(err.message.contains("Unhandled") && err.message.contains("boom"));
    proxy.finish().unwrap();
}

#[tokio::test]
async fn lambda_function_error_as_reply() {
    let (_, scenario, proxy) = replay("lambda-cloud-run-azure-functions/invoke.json").await;
    let fabric = MemoryFabric::new();
    let mut cfg = with_proxy(&scenario["lambda"], &proxy.base_url());
    cfg["function_error"] = json!("reply");
    let mut s = sink(cfg, &scenario["secrets"], &fabric);
    s.write(EventBatch::Events(vec![event(
        "o-8",
        None,
        &json!({"order": "boom"}),
    )]))
    .await
    .unwrap();
    assert_eq!(
        data(&fabric.events()[0]),
        json!({"function_error": "Unhandled", "payload": {"errorMessage": "boom", "errorType": "ValueError"}})
    );
}

#[tokio::test]
async fn cloud_run_and_azure_functions_send_their_credentials() {
    let (_, scenario, proxy) = replay("lambda-cloud-run-azure-functions/invoke.json").await;
    let fabric = MemoryFabric::new();
    for (platform, header, value) in [
        ("cloud_run", "authorization", "Bearer synthetic-id-token"),
        (
            "azure_functions",
            "x-functions-key",
            "synthetic-function-key",
        ),
    ] {
        let cfg = with_proxy(&scenario[platform], &proxy.base_url());
        let mut s = sink(cfg, &scenario["secrets"], &fabric);
        s.write(EventBatch::Events(vec![event(
            platform,
            None,
            &json!({"order": 7}),
        )]))
        .await
        .unwrap();
        let req = proxy.received().pop().unwrap();
        assert_eq!(
            req.headers.get(header).map(String::as_str),
            Some(value),
            "{platform}"
        );
    }
    // Without reply_topic, no replies are sent.
    assert!(fabric.events().is_empty());
    proxy.finish().unwrap();
}

/// floci (D60) as Lambda: create an echo function, invoke it, read the
/// reply. Runs with the `connectors-p2` stack (`floci` on :4566).
#[tokio::test]
async fn lambda_invoke_against_floci() {
    if !p2_stack() {
        return skip(
            "lambda_invoke_against_floci",
            "LOAMS_CONNECTORS_P2_STACK unset",
        );
    }
    let endpoint =
        std::env::var("LOAMS_FLOCI_URL").unwrap_or_else(|_| "http://localhost:4566".into());
    let secrets = json!({"id": "test", "secret": "test"});
    let ctx = ctx(json!({}), &secrets);
    let client = loams_connector_protocols::client::RestClient::connect(
        &endpoint,
        &loams_connector_protocols::client::AuthSpec::Aws(
            loams_connector_protocols::client::AwsAuth {
                access_key_id: "id".into(),
                secret_access_key: "secret".into(),
                session_token: None,
                region: "us-east-1".into(),
                service: None,
            },
        ),
        &*ctx.secrets,
        ctx.egress.clone(),
        std::time::Duration::from_secs(120),
        "lambda",
    )
    .await
    .unwrap();
    let zip = echo_zip();
    use base64::Engine;
    let create = json!({
        "FunctionName": "loams-echo",
        "Runtime": "python3.12",
        "Role": "arn:aws:iam::000000000000:role/lambda-role",
        "Handler": "index.handler",
        "Code": {"ZipFile": base64::engine::general_purpose::STANDARD.encode(zip)},
    });
    // Created once per stack; a second create is a 409 we accept.
    let _ = client
        .json(http::Method::POST, "/2015-03-31/functions", Some(&create))
        .await;
    let fabric = MemoryFabric::new();
    let cfg = json!({"provider": "lambda", "function": "loams-echo", "endpoint": endpoint,
        "aws": {"access_key_id": "id", "secret_access_key": "secret", "region": "us-east-1"},
        "reply_topic": "r"});
    let mut s = sink(cfg, &secrets, &fabric);
    let mut last = None;
    for _ in 0..30 {
        // A new function may still be Pending.
        match s
            .write(EventBatch::Events(vec![event(
                "f1",
                Some("s"),
                &json!({"n": 1}),
            )]))
            .await
        {
            Ok(_) => {
                last = None;
                break;
            }
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    assert!(last.is_none(), "{last:?}");
    assert_eq!(data(&fabric.events()[0]), json!({"echo": {"n": 1}}));
}

/// A stored (uncompressed) zip of `index.py`.
fn echo_zip() -> Vec<u8> {
    let name = b"index.py";
    let body = b"def handler(event, context):\n    return {\"echo\": event}\n";
    let crc = crc32(body);
    let mut z = Vec::new();
    let local = |z: &mut Vec<u8>| {
        z.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        z.extend_from_slice(&[20, 0, 0, 0, 0, 0, 0, 0, 0x21, 0]); // version, flags, method 0, time, date
        z.extend_from_slice(&crc.to_le_bytes());
        z.extend_from_slice(&(body.len() as u32).to_le_bytes());
        z.extend_from_slice(&(body.len() as u32).to_le_bytes());
        z.extend_from_slice(&(name.len() as u16).to_le_bytes());
        z.extend_from_slice(&0u16.to_le_bytes());
    };
    local(&mut z);
    z.extend_from_slice(name);
    z.extend_from_slice(body);
    let cd_start = z.len() as u32;
    z.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
    z.extend_from_slice(&[20, 3, 20, 0, 0, 0, 0, 0, 0, 0, 0x21, 0]);
    z.extend_from_slice(&crc.to_le_bytes());
    z.extend_from_slice(&(body.len() as u32).to_le_bytes());
    z.extend_from_slice(&(body.len() as u32).to_le_bytes());
    z.extend_from_slice(&(name.len() as u16).to_le_bytes());
    z.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // extra, comment, disk, internal attrs
    z.extend_from_slice(&(0o100_644u32 << 16).to_le_bytes());
    z.extend_from_slice(&0u32.to_le_bytes());
    z.extend_from_slice(name);
    let cd_len = z.len() as u32 - cd_start;
    z.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    z.extend_from_slice(&[0, 0, 0, 0, 1, 0, 1, 0]);
    z.extend_from_slice(&cd_len.to_le_bytes());
    z.extend_from_slice(&cd_start.to_le_bytes());
    z.extend_from_slice(&0u16.to_le_bytes());
    z
}

fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for b in data {
        c ^= u32::from(*b);
        for _ in 0..8 {
            c = if c & 1 != 0 {
                (c >> 1) ^ 0xEDB8_8320
            } else {
                c >> 1
            };
        }
    }
    !c
}

#[test]
fn crc32_check_value() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
}
