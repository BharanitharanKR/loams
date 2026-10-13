//! CN2 plan Task 19: Amazon SNS. The `Publish` sink against a fake SNS (the
//! Query protocol) and floci or AWS; the HTTP-subscription source's signature
//! check and subscription-confirmation handshake on messages signed with a
//! test-only key (SNS pushes reach Loams only once Task 28 opens remote
//! ingress, so the real push half waits for it); and SNS → SQS fan-out read
//! back through the SQS source.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use common::fake_sqs::FakeSqs;
use common::*;
use loams_connector_cloud::aws::{AwsClient, AwsService};
use loams_connector_cloud::sns::{SnsFactory, SnsTrust, string_to_sign};
use loams_connector_cloud::sqs::SqsFactory;
use loams_connector_sdk::StaticSecrets;
use loams_connector_sdk::testing::memory::MemoryFabric;
use serde_json::{Value, json};

const TOPIC: &str = "arn:aws:sns:us-west-2:123456789012:MyTopic";
const CERT_PEM: &str = include_str!("data/test-only-cert.pem");
const KEY_PEM: &str = include_str!("data/test-only-rsa-key.pem");
const V1_SIGNATURE: &str = include_str!("data/sns-v1-signature.b64");

fn pem_der(pem: &str) -> Vec<u8> {
    let b64: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .unwrap()
}

/// SHA256withRSA (SignatureVersion 2) over the message's string to sign,
/// with the test-only key.
fn sign_v2(msg: &mut Value) {
    let key = ring::signature::RsaKeyPair::from_pkcs8(&pem_der(KEY_PEM)).unwrap();
    msg["SignatureVersion"] = json!("2");
    let fields: BTreeMap<String, String> = msg
        .as_object()
        .unwrap()
        .iter()
        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
        .collect();
    let sts = string_to_sign(&fields).unwrap();
    let mut sig = vec![0u8; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        sts.as_bytes(),
        &mut sig,
    )
    .unwrap();
    msg["Signature"] = json!(base64::engine::general_purpose::STANDARD.encode(sig));
}

fn post(body: &Value) -> http::Request<Bytes> {
    http::Request::post("/hooks/sns")
        .header(
            "x-amz-sns-message-type",
            body["Type"].as_str().unwrap_or(""),
        )
        .header("content-type", "text/plain; charset=UTF-8")
        .body(Bytes::from(body.to_string()))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_signature_and_subscription_confirmation() {
    // The fake AWS side: the signing certificate and the SubscribeURL.
    let api = FakeApi::start(Arc::new(|seen: &Seen| match seen.path.as_str() {
        "/SimpleNotificationService-test.pem" => (200, vec![], Bytes::from(CERT_PEM)),
        "/confirm" => xml_reply(
            200,
            "<ConfirmSubscriptionResponse><ConfirmSubscriptionResult><SubscriptionArn>arn:aws:sns:us-west-2:123456789012:MyTopic:1</SubscriptionArn></ConfirmSubscriptionResult></ConfirmSubscriptionResponse>",
        ),
        _ => (404, vec![], Bytes::new()),
    }))
    .await;
    let host = api.base.trim_start_matches("http://").to_owned();
    let cert_url = format!("{}/SimpleNotificationService-test.pem", api.base);
    let factory = SnsFactory::new().with_trust(SnsTrust::insecure_for_tests(&host));
    let config = json!({"region": "us-west-2", "endpoint": api.base, "topic_arn": TOPIC});
    let fabric = MemoryFabric::new();
    let ctx = ctx(config, StaticSecrets::new());
    let p = start_source(&factory, &ctx, &fabric).await;
    let hook = factory
        .webhook(&instance())
        .expect("the source registered its webhook");

    // 1. The subscription-confirmation handshake: verified, then confirmed.
    let mut confirm = json!({
        "Type": "SubscriptionConfirmation",
        "MessageId": "165545c9-2a5c-472c-8df2-7ff2be2b3b1b",
        "Token": "2336412f37",
        "TopicArn": TOPIC,
        "Message": "You have chosen to subscribe to the topic.",
        "SubscribeURL": format!("{}/confirm?Action=ConfirmSubscription&TopicArn={TOPIC}&Token=2336412f37", api.base),
        "Timestamp": "2012-04-26T20:45:04.751Z",
        "SigningCertURL": cert_url,
    });
    sign_v2(&mut confirm);
    let resp = hook.handle(post(&confirm)).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        api.seen().iter().filter(|s| s.path == "/confirm").count(),
        1,
        "the SubscribeURL was visited once"
    );
    assert!(fabric.is_empty(), "a confirmation is not an event");

    // 2. A notification signed with SignatureVersion 1 (SHA1withRSA; the
    //    signature was made with openssl over this exact string to sign).
    let note = json!({
        "Type": "Notification",
        "MessageId": "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324",
        "TopicArn": TOPIC,
        "Subject": "My First Message",
        "Message": "Hello world!",
        "Timestamp": "2012-05-02T00:54:06.655Z",
        "SignatureVersion": "1",
        "Signature": V1_SIGNATURE.trim(),
        "SigningCertURL": cert_url,
    });
    let resp = hook.handle(post(&note)).await;
    assert_eq!(resp.status(), 200, "{:?}", resp.body());
    let evs = fabric.events();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].id, "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324");
    assert_eq!(evs[0].ty, "io.loams.dev.flow.aws-sns.message.v1");
    assert_eq!(evs[0].subject.as_deref(), Some("My First Message"));
    assert_eq!(evs[0].data.as_ref(), b"Hello world!");
    assert_eq!(evs[0].time.as_deref(), Some("2012-05-02T00:54:06.655Z"));

    // 3. A notification carrying CloudEvents attributes passes through.
    let mut ce = json!({
        "Type": "Notification",
        "MessageId": "m-2",
        "TopicArn": TOPIC,
        "Message": "{\"i\":1}",
        "Timestamp": "2026-10-13T00:00:00.000Z",
        "SigningCertURL": cert_url,
        "MessageAttributes": {
            "ce_id": {"Type": "String", "Value": "e-1"},
            "ce_source": {"Type": "String", "Value": "/test/upstream"},
            "ce_type": {"Type": "String", "Value": "com.example.order.created.v1"},
            "ce_specversion": {"Type": "String", "Value": "1.0"},
            "content-type": {"Type": "String", "Value": "application/json"},
        },
    });
    sign_v2(&mut ce);
    assert_eq!(hook.handle(post(&ce)).await.status(), 200);
    let last = fabric.events().pop().unwrap();
    assert_eq!(
        (last.id.as_str(), last.ty.as_str(), last.source.as_str()),
        ("e-1", "com.example.order.created.v1", "/test/upstream")
    );

    // 4. Refusals: a tampered message, another topic, an untrusted
    //    certificate host. None reaches the Fabric.
    let before = fabric.len();
    let mut tampered = note.clone();
    tampered["Message"] = json!("Hello world?");
    assert_eq!(hook.handle(post(&tampered)).await.status(), 403);
    let mut other_topic = confirm.clone();
    other_topic["TopicArn"] = json!("arn:aws:sns:us-west-2:123456789012:Other");
    sign_v2(&mut other_topic);
    assert_eq!(hook.handle(post(&other_topic)).await.status(), 403);
    let mut evil = note.clone();
    evil["SigningCertURL"] = json!("http://evil.example/cert.pem");
    assert_eq!(hook.handle(post(&evil)).await.status(), 403);
    assert_eq!(fabric.len(), before);
    assert_eq!(
        api.seen().iter().filter(|s| s.path == "/confirm").count(),
        1,
        "no refused message was confirmed"
    );

    // The production trust accepts only SNS's own hosts over HTTPS.
    let prod = SnsTrust::default();
    assert!(
        prod.cert_url_ok("https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-abc.pem")
    );
    assert!(
        prod.cert_url_ok(
            "https://sns.cn-north-1.amazonaws.com.cn/SimpleNotificationService-abc.pem"
        )
    );
    assert!(
        !prod.cert_url_ok("http://sns.eu-west-1.amazonaws.com/SimpleNotificationService-abc.pem")
    );
    assert!(!prod.cert_url_ok("https://sns.eu-west-1.amazonaws.com.evil.example/x.pem"));
    assert!(!prod.cert_url_ok("https://evil.example/sns.eu-west-1.amazonaws.com/x.pem"));
    p.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_publish_sink_maps_ce_attributes_and_retries_failed_entries() {
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let f = first.clone();
    let api = FakeApi::start(Arc::new(move |seen: &Seen| {
        let form = seen.form();
        assert_eq!(form.get("Action").map(String::as_str), Some("PublishBatch"));
        let ids: Vec<String> = (1..=10)
            .filter_map(|n| form.get(&format!("PublishBatchRequestEntries.member.{n}.Id")).cloned())
            .collect();
        let mut ok = String::new();
        let mut failed = String::new();
        for (n, id) in ids.iter().enumerate() {
            if n == 1 && f.swap(false, std::sync::atomic::Ordering::SeqCst) {
                failed.push_str(&format!("<member><Id>{id}</Id><Code>InternalError</Code><Message>try again</Message><SenderFault>false</SenderFault></member>"));
            } else {
                ok.push_str(&format!("<member><Id>{id}</Id><MessageId>m-{id}</MessageId></member>"));
            }
        }
        xml_reply(200, &format!("<PublishBatchResponse><PublishBatchResult><Successful>{ok}</Successful><Failed>{failed}</Failed></PublishBatchResult></PublishBatchResponse>"))
    }))
    .await;
    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 3);
    let config = json!({"region": "us-west-2", "endpoint": api.base, "topic_arn": TOPIC});
    let p = start_sink(
        &SnsFactory::new(),
        &ctx(config, StaticSecrets::new()),
        &fabric,
    )
    .await;
    eventually("committed", || fabric.committed(GROUP) == 3).await;
    p.stop().await;
    let calls = api.seen();
    assert_eq!(calls.len(), 2, "one batch, then only the failed entry");
    let retry = calls[1].form();
    assert_eq!(
        retry
            .keys()
            .filter(|k| k.ends_with(".Id") && k.starts_with("PublishBatchRequestEntries"))
            .count(),
        1
    );
    let form = calls[0].form();
    let attr = |n: usize, name: &str| -> Option<String> {
        (1..=10).find_map(|a| {
            let key = format!("PublishBatchRequestEntries.member.{n}.MessageAttributes.entry.{a}");
            (form.get(&format!("{key}.Name"))? == name)
                .then(|| form.get(&format!("{key}.Value.StringValue")).cloned())
                .flatten()
        })
    };
    assert_eq!(attr(1, "ce_id").as_deref(), Some("e-0"));
    assert_eq!(
        attr(1, "ce_type").as_deref(),
        Some("com.example.order.created.v1")
    );
    assert_eq!(attr(1, "content-type").as_deref(), Some("application/json"));
    assert_eq!(
        form.get("PublishBatchRequestEntries.member.1.Message")
            .map(String::as_str),
        Some("{\"i\":0}")
    );
    assert_eq!(form.get("TopicArn").map(String::as_str), Some(TOPIC));
    assert!(
        calls[0]
            .header("authorization")
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 ")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sns_to_sqs_envelope_passes_through() {
    // SNS → SQS without raw delivery: the SQS body is SNS's JSON envelope.
    let fake = FakeSqs::start().await;
    let envelope = json!({
        "Type": "Notification",
        "MessageId": "sns-1",
        "TopicArn": TOPIC,
        "Message": "{\"i\":7}",
        "Timestamp": "2026-10-13T00:00:00.000Z",
        "MessageAttributes": {
            "ce_id": {"Type": "String", "Value": "e-7"},
            "ce_source": {"Type": "String", "Value": "/test/upstream"},
            "ce_type": {"Type": "String", "Value": "com.example.order.created.v1"},
            "ce_specversion": {"Type": "String", "Value": "1.0"},
        },
    });
    fake.send(&envelope.to_string(), Value::Null);
    let plain = json!({"Type": "Notification", "MessageId": "sns-2", "TopicArn": TOPIC,
        "Message": "plain text", "Timestamp": "2026-10-13T00:00:01.000Z"});
    fake.send(&plain.to_string(), Value::Null);
    let fabric = MemoryFabric::new();
    let config = json!({"region": "us-west-2", "endpoint": fake.api.base,
        "queue_url": fake.queue_url("fanout"), "wait_time_seconds": 0});
    let p = start_source(
        &SqsFactory::new(),
        &ctx(config, StaticSecrets::new()),
        &fabric,
    )
    .await;
    eventually("both read", || fabric.len() == 2).await;
    p.stop().await;
    let evs = fabric.events();
    let ce = evs.iter().find(|e| e.id == "e-7").expect("passed through");
    assert_eq!(ce.ty, "com.example.order.created.v1");
    assert_eq!(ce.data.as_ref(), b"{\"i\":7}");
    let msg = evs.iter().find(|e| e.id == "sns-2").expect("unwrapped");
    assert_eq!(msg.data.as_ref(), b"plain text");
    assert_eq!(msg.ty, "io.loams.dev.flow.aws-sqs.message.v1");

    for (label, base, secrets) in aws_targets("sns_to_sqs_envelope_passes_through") {
        real_fanout(label, base, secrets).await;
    }
}

/// Publish through the SNS sink to a topic with an SQS subscription, and
/// read the queue back with the SQS source: identity survives both hops.
async fn real_fanout(label: &str, base: Value, secrets: Secrets) {
    let admin = ctx(base.clone(), secrets.clone());
    let sns = AwsClient::from_context(&admin, AwsService::Sns).unwrap();
    let sqs = AwsClient::from_context(&admin, AwsService::Sqs).unwrap();
    let name = run_name("loams-sns");
    let topic_xml = sns
        .query(&[("Action", "CreateTopic"), ("Name", name.as_str())])
        .await
        .unwrap();
    let topic = loams_connector_cloud::aws::xml_text(&topic_xml, "TopicArn").unwrap();
    let url = sqs
        .call("CreateQueue", &json!({"QueueName": name}))
        .await
        .unwrap()["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned();
    let arn = sqs
        .call(
            "GetQueueAttributes",
            &json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
        )
        .await
        .unwrap()["Attributes"]["QueueArn"]
        .as_str()
        .unwrap()
        .to_owned();
    let policy = json!({"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
        "Principal": {"Service": "sns.amazonaws.com"}, "Action": "sqs:SendMessage",
        "Resource": arn, "Condition": {"ArnEquals": {"aws:SourceArn": topic}}}]});
    sqs.call(
        "SetQueueAttributes",
        &json!({"QueueUrl": url, "Attributes": {"Policy": policy.to_string()}}),
    )
    .await
    .unwrap();
    sns.query(&[
        ("Action", "Subscribe"),
        ("TopicArn", topic.as_str()),
        ("Protocol", "sqs"),
        ("Endpoint", arn.as_str()),
    ])
    .await
    .unwrap();

    let fabric = MemoryFabric::new();
    push_events(&fabric, 0, 3);
    let mut sink_cfg = base.clone();
    sink_cfg["topic_arn"] = json!(topic);
    let p = start_sink(&SnsFactory::new(), &ctx(sink_cfg, secrets.clone()), &fabric).await;
    eventually(&format!("{label}: published"), || {
        fabric.committed(GROUP) == 3
    })
    .await;
    p.stop().await;
    let mut src_cfg = base;
    src_cfg["queue_url"] = json!(url);
    src_cfg["wait_time_seconds"] = json!(2);
    let out = MemoryFabric::new();
    let p = start_source(&SqsFactory::new(), &ctx(src_cfg, secrets), &out).await;
    eventually(&format!("{label}: fanned out"), || out.len() == 3).await;
    p.stop().await;
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(ids, vec!["e-0", "e-1", "e-2"], "{label}: ids pass through");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = sqs.call("DeleteQueue", &json!({"QueueUrl": url})).await;
    let _ = sns
        .query(&[("Action", "DeleteTopic"), ("TopicArn", topic.as_str())])
        .await;
}
