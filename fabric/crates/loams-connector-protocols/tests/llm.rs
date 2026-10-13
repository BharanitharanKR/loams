//! OpenAI and Anthropic sinks against synthetic fixtures of the documented
//! shapes, and (nightly, with real keys) against the real APIs.
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use loams_connector_protocols::{OneTopic, ReplyTopics, anthropic, openai};
use loams_connector_sdk::testing::memory::MemoryFabric;
use loams_connector_sdk::{ConnectorFactory, ErrorClass, EventBatch, SinkHandle};
use serde_json::{Value, json};

fn sink_of(
    factory: &dyn ConnectorFactory,
    config: Value,
    secrets: &Value,
) -> Box<dyn loams_connector_sdk::Sink> {
    let ctx = ctx(config, secrets);
    match factory.sink(&ctx).unwrap().unwrap().create().unwrap() {
        SinkHandle::Plain(s) => s,
        SinkHandle::Transactional(_) => panic!("plain"),
    }
}

fn input(scenario: &Value, i: usize) -> loams_connector_sdk::CloudEvent {
    let e = &scenario["events"][i];
    event(e["id"].as_str().unwrap(), e["subject"].as_str(), &e["data"])
}

#[tokio::test]
async fn openai_reply_has_no_key_and_keeps_subject() {
    let (_, scenario, proxy) = replay("openai/chat-completions.json").await;
    let fabric = MemoryFabric::new();
    let topics: Arc<dyn ReplyTopics> = Arc::new(OneTopic(Arc::new(fabric.clone())));
    let factory = openai::factory().with_replies(topics);
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let mut sink = sink_of(&factory, config, &scenario["secrets"]);

    let ack = sink
        .write(EventBatch::Events(vec![input(&scenario, 0)]))
        .await
        .unwrap();
    assert_eq!(ack.rows, 1);

    let replies = fabric.events();
    assert_eq!(replies.len(), 1);
    let r = &replies[0];
    assert_eq!(
        r.subject.as_deref(),
        Some("ticket-41"),
        "the reply keeps the subject"
    );
    assert_eq!(r.ty, "io.loams.dev.flow.openai.reply.v1");
    assert_eq!(r.id, "ticket-41-v1/reply");
    let d = data(r);
    assert_eq!(d["text"], "urgent");
    assert_eq!(d["stop_reason"], "stop");
    assert_eq!(d["usage"]["total_tokens"], 21);
    // The key went out as a bearer token and is nowhere in the reply.
    let key = scenario["secrets"]["openai-key"].as_str().unwrap();
    let sent = proxy.received();
    assert_eq!(
        sent[0].headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {key}").as_str())
    );
    let whole = format!("{r:?}{}", String::from_utf8_lossy(&r.data));
    assert!(
        !whole.contains(key) && !whole.contains("synthetic-sk"),
        "no key in the reply"
    );

    // A 429 is Throttled with Retry-After, and the provider's headers hold
    // the next request.
    let err = sink
        .write(EventBatch::Events(vec![input(&scenario, 1)]))
        .await
        .unwrap_err();
    assert_eq!(
        err.class,
        ErrorClass::Throttled {
            after: Duration::from_secs(2)
        }
    );
    assert!(!err.message.contains(key));
    assert_eq!(fabric.events().len(), 1, "no reply for a failed call");
    proxy.finish().unwrap();
}

#[tokio::test]
async fn anthropic_reply_fixture() {
    let (_, scenario, proxy) = replay("anthropic/messages.json").await;
    let fabric = MemoryFabric::new();
    let factory = anthropic::factory().with_replies(Arc::new(OneTopic(Arc::new(fabric.clone()))));
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let mut sink = sink_of(&factory, config, &scenario["secrets"]);

    sink.write(EventBatch::Events(vec![input(&scenario, 0)]))
        .await
        .unwrap();
    let replies = fabric.events();
    assert_eq!(replies.len(), 1);
    let r = &replies[0];
    assert_eq!(r.ty, "io.loams.dev.flow.anthropic.reply.v1");
    assert_eq!(r.subject.as_deref(), Some("ticket-41"));
    assert_eq!(r.source, "/connectors/test/claude-opus-5-5");
    assert_eq!(
        r.extensions.get("loamsinreplyto").map(String::as_str),
        Some("ticket-41-v1")
    );
    let d = data(r);
    assert_eq!(
        d,
        json!({
            "provider": "anthropic", "model": "claude-opus-5-5", "id": "msg_synthetic0001",
            "text": "urgent", "stop_reason": "end_turn",
            "usage": {"input_tokens": 21, "output_tokens": 3}
        })
    );
    let sent = proxy.received();
    let key = scenario["secrets"]["anthropic-key"].as_str().unwrap();
    assert_eq!(
        sent[0].headers.get("x-api-key").map(String::as_str),
        Some(key)
    );
    assert_eq!(
        sent[0].headers.get("anthropic-version").map(String::as_str),
        Some("2023-06-01")
    );
    assert!(!String::from_utf8_lossy(&r.data).contains(key));

    // 529 overloaded is retryable, never poison.
    let err = sink
        .write(EventBatch::Events(vec![input(&scenario, 1)]))
        .await
        .unwrap_err();
    assert!(matches!(err.class, ErrorClass::Retryable { .. }), "{err:?}");
    assert!(err.message.contains("overloaded_error"));
    proxy.finish().unwrap();
}

#[tokio::test]
async fn a_reply_topic_without_reply_topics_is_refused() {
    let (_, scenario, proxy) = replay("openai/chat-completions.json").await;
    let config = with_proxy(&scenario["config"], &proxy.base_url());
    let ctx = ctx(config, &scenario["secrets"]);
    let err = openai::factory()
        .sink(&ctx)
        .unwrap()
        .unwrap()
        .create()
        .unwrap_err();
    assert_eq!(err.class, ErrorClass::Fatal);
}

/// Nightly: the real APIs with real keys (`LOAMS_OPENAI_API_KEY`,
/// `LOAMS_ANTHROPIC_API_KEY`). Never runs without them.
#[tokio::test]
async fn llm_replies_against_the_real_apis() {
    for (provider, var, model) in [
        ("openai", "LOAMS_OPENAI_API_KEY", "LOAMS_OPENAI_MODEL"),
        (
            "anthropic",
            "LOAMS_ANTHROPIC_API_KEY",
            "LOAMS_ANTHROPIC_MODEL",
        ),
    ] {
        let Some(vals) = env_all(&[var]) else {
            skip("llm_replies_against_the_real_apis", &format!("{var} unset"));
            continue;
        };
        let model = std::env::var(model).unwrap_or_else(|_| {
            if provider == "openai" {
                "gpt-4o-mini".into()
            } else {
                "claude-haiku-5-5".into()
            }
        });
        let fabric = MemoryFabric::new();
        let topics: Arc<dyn ReplyTopics> = Arc::new(OneTopic(Arc::new(fabric.clone())));
        let factory = if provider == "openai" {
            openai::factory()
        } else {
            anthropic::factory()
        }
        .with_replies(topics);
        let config = json!({"model": model, "api_key": "key", "max_tokens": 32,
            "prompt": {"user": "Reply with one word: {{/text}}"}, "reply_topic": "t"});
        let mut sink = sink_of(&factory, config, &json!({"key": vals[0]}));
        sink.write(EventBatch::Events(vec![event(
            "n1",
            Some("s1"),
            &json!({"text": "hello"}),
        )]))
        .await
        .unwrap();
        let r = &fabric.events()[0];
        assert_eq!(r.subject.as_deref(), Some("s1"));
        assert!(!String::from_utf8_lossy(&r.data).contains(&vals[0]));
    }
}
