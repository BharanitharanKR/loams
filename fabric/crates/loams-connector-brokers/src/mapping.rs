//! Broker messages ↔ CloudEvents (design §33 §6, D270).
//!
//! A message that already carries a CloudEvent passes through with its
//! `type`, `source` and `id` (Review Focus 10): in binary mode, as headers or
//! properties named with a CloudEvents prefix — `ce_` (Kafka and the
//! convention RabbitMQ, MQTT and Pulsar users follow), `ce-` (HTTP),
//! `cloudEvents:` (the CloudEvents AMQP binding) or `cloudEvents_` (its JMS
//! spelling) — or in structured mode, as an `application/cloudevents+json`
//! body. Any other message becomes an
//! `io.loams.dev.flow.<connector>.message.v1` event whose id the connector
//! chooses (a broker-assigned id where the broker has one).
//!
//! Sinks write binary mode: the event's attributes as prefixed headers, its
//! `datacontenttype` as the message's content type, and its data as the body.

use bytes::Bytes;
use loams_connector_sdk::{CloudEvent, ConnectorError, EnvelopeBuilder, EventBatch};

/// The CloudEvents header prefixes a source recognises, case-insensitively.
pub const CE_PREFIXES: &[&str] = &["ce_", "ce-", "cloudevents:", "cloudevents_"];

/// The event name every broker family emits (Shared contracts, Event names).
pub const MESSAGE_EVENT: &str = "message";

/// A message as a source received it.
#[derive(Debug, Clone, Default)]
pub struct Incoming {
    /// Headers, application properties or user properties, as text.
    pub headers: Vec<(String, String)>,
    pub content_type: Option<String>,
    pub data: Bytes,
    /// The id when the message is not a CloudEvent.
    pub fallback_id: String,
    /// The queue, subject or topic (the event's `source` resource).
    pub resource: String,
    /// A routing key, subject or topic name for `subject`, when not a
    /// CloudEvent.
    pub subject: Option<String>,
}

/// `ce_<attr>` for a header in any recognised prefix, else `None`.
pub fn ce_attribute(name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    CE_PREFIXES
        .iter()
        .find_map(|p| lower.strip_prefix(p))
        .filter(|a| !a.is_empty())
        .map(str::to_owned)
}

/// The CloudEvent a message carries, or a new `message` event.
pub fn to_event(env: &EnvelopeBuilder, inc: Incoming) -> CloudEvent {
    let ce_headers: Vec<(String, Vec<u8>)> = inc
        .headers
        .iter()
        .filter_map(|(k, v)| ce_attribute(k).map(|a| (format!("ce_{a}"), v.as_bytes().to_vec())))
        .collect();
    if !ce_headers.is_empty() {
        let mut headers = ce_headers;
        if let Some(ct) = &inc.content_type {
            headers.push(("content-type".to_owned(), ct.as_bytes().to_vec()));
        }
        if let Some(ev) = env.from_binary(
            headers.iter().map(|(k, v)| (k.as_str(), v.as_slice())),
            inc.data.clone(),
        ) {
            return ev;
        }
    }
    if inc
        .content_type
        .as_deref()
        .is_some_and(|ct| ct.starts_with("application/cloudevents+json"))
        && let Some(ev) = env.from_structured_json(&inc.data)
    {
        return ev;
    }
    let mut ev = env.event(MESSAGE_EVENT, &inc.resource, inc.fallback_id);
    ev.datacontenttype = inc.content_type;
    ev.data = inc.data;
    if let Some(s) = inc.subject.filter(|s| !s.is_empty()) {
        ev.subject = Some(s);
    }
    ev
}

/// The event's attributes as headers with `prefix` (binary mode), without
/// `datacontenttype`, which a broker carries as the message's content type.
pub fn binary_headers(ev: &CloudEvent, prefix: &str) -> Vec<(String, String)> {
    let mut h = vec![
        (format!("{prefix}specversion"), ev.specversion.clone()),
        (format!("{prefix}id"), ev.id.clone()),
        (format!("{prefix}source"), ev.source.clone()),
        (format!("{prefix}type"), ev.ty.clone()),
    ];
    if let Some(s) = &ev.subject {
        h.push((format!("{prefix}subject"), s.clone()));
    }
    if let Some(t) = &ev.time {
        h.push((format!("{prefix}time"), t.clone()));
    }
    for (k, v) in &ev.extensions {
        h.push((format!("{prefix}{k}"), v.clone()));
    }
    h
}

/// The id a header list carries under any CloudEvents prefix.
pub fn header_id(headers: &[(String, String)]) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| ce_attribute(k).as_deref() == Some("id"))
        .map(|(_, v)| v.as_str())
}

/// The events of a batch, refusing the Arrow bulk path: a broker message is
/// one event, and a sink never splits an Arrow batch into messages on its own
/// (as the Kafka family does, R9.4).
pub fn events(batch: EventBatch, connector: &str) -> Result<Vec<CloudEvent>, ConnectorError> {
    match batch {
        EventBatch::Events(v) => Ok(v),
        EventBatch::Arrow { .. } => Err(ConnectorError::poison(format!(
            "{connector}: an Arrow batch cannot be written as broker messages; decode it to events first"
        ))),
    }
}

/// A content type broker headers can carry, defaulting to octet-stream.
pub fn content_type(ev: &CloudEvent) -> &str {
    ev.datacontenttype
        .as_deref()
        .unwrap_or("application/octet-stream")
}

/// True when the event's data is text a text frame (WebSocket) may carry.
pub fn is_text(ev: &CloudEvent) -> bool {
    let ct = content_type(ev);
    (ct.starts_with("text/") || ct.contains("json") || ct.contains("xml"))
        && std::str::from_utf8(&ev.data).is_ok()
}

/// The event in structured mode (`application/cloudevents+json`).
pub fn structured_json(ev: &CloudEvent) -> Vec<u8> {
    use base64::Engine as _;
    let mut m = serde_json::Map::new();
    m.insert("specversion".into(), ev.specversion.clone().into());
    m.insert("id".into(), ev.id.clone().into());
    m.insert("source".into(), ev.source.clone().into());
    m.insert("type".into(), ev.ty.clone().into());
    if let Some(s) = &ev.subject {
        m.insert("subject".into(), s.clone().into());
    }
    if let Some(t) = &ev.time {
        m.insert("time".into(), t.clone().into());
    }
    if let Some(ct) = &ev.datacontenttype {
        m.insert("datacontenttype".into(), ct.clone().into());
    }
    for (k, v) in &ev.extensions {
        m.insert(k.clone(), v.clone().into());
    }
    if !ev.data.is_empty() {
        let json = ev
            .datacontenttype
            .as_deref()
            .is_some_and(|ct| ct.contains("json"))
            .then(|| serde_json::from_slice::<serde_json::Value>(&ev.data).ok())
            .flatten();
        match json {
            Some(v) => {
                m.insert("data".into(), v);
            }
            None => match std::str::from_utf8(&ev.data) {
                Ok(s)
                    if ev
                        .datacontenttype
                        .as_deref()
                        .is_some_and(|c| c.starts_with("text/")) =>
                {
                    m.insert("data".into(), s.to_owned().into());
                }
                _ => {
                    m.insert(
                        "data_base64".into(),
                        base64::engine::general_purpose::STANDARD
                            .encode(&ev.data)
                            .into(),
                    );
                }
            },
        }
    }
    serde_json::to_vec(&serde_json::Value::Object(m)).unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    fn env() -> EnvelopeBuilder {
        EnvelopeBuilder::new("rabbitmq-amqp", &InstanceKey::new(1, "orders"))
    }

    #[test]
    fn plain_messages_become_message_events() {
        let ev = to_event(
            &env(),
            Incoming {
                headers: vec![("x-trace".into(), "abc".into())],
                content_type: Some("application/json".into()),
                data: Bytes::from_static(b"{}"),
                fallback_id: "orders/7".into(),
                resource: "orders".into(),
                subject: Some("eu.orders".into()),
            },
        );
        assert_eq!(ev.ty, "io.loams.dev.flow.rabbitmq-amqp.message.v1");
        assert_eq!(ev.source, "/connectors/orders/orders");
        assert_eq!(ev.id, "orders/7");
        assert_eq!(ev.subject.as_deref(), Some("eu.orders"));
        assert_eq!(ev.datacontenttype.as_deref(), Some("application/json"));
        assert_eq!(ev.extensions["loamsconnector"], "rabbitmq-amqp");
    }

    #[test]
    fn every_prefix_passes_through() {
        for prefix in ["ce_", "ce-", "cloudEvents:", "cloudEvents_", "CE_"] {
            let headers = vec![
                (format!("{prefix}specversion"), "1.0".to_owned()),
                (format!("{prefix}id"), "evt-1".to_owned()),
                (format!("{prefix}source"), "/upstream".to_owned()),
                (format!("{prefix}type"), "com.example.thing.v1".to_owned()),
                (format!("{prefix}tenant"), "acme".to_owned()),
            ];
            let ev = to_event(
                &env(),
                Incoming {
                    headers,
                    content_type: Some("text/plain".into()),
                    data: Bytes::from_static(b"hi"),
                    fallback_id: "never".into(),
                    resource: "q".into(),
                    subject: None,
                },
            );
            assert_eq!(ev.id, "evt-1", "{prefix}");
            assert_eq!(ev.source, "/upstream");
            assert_eq!(ev.ty, "com.example.thing.v1");
            assert_eq!(ev.extensions["tenant"], "acme");
            assert_eq!(ev.datacontenttype.as_deref(), Some("text/plain"));
        }
    }

    #[test]
    fn an_incomplete_cloudevent_is_a_plain_message() {
        let ev = to_event(
            &env(),
            Incoming {
                headers: vec![("ce_id".into(), "x".into())],
                fallback_id: "q/1".into(),
                resource: "q".into(),
                ..Incoming::default()
            },
        );
        assert_eq!(ev.id, "q/1");
    }

    #[test]
    fn structured_round_trip() {
        let mut ev = CloudEvent::new("a", "/s", "t")
            .with_data("application/json", Bytes::from_static(b"{\"n\":1}"))
            .with_extension("tenant", "acme");
        ev.subject = Some("x".into());
        let body = structured_json(&ev);
        let back = to_event(
            &env(),
            Incoming {
                content_type: Some("application/cloudevents+json; charset=utf-8".into()),
                data: Bytes::from(body),
                fallback_id: "never".into(),
                resource: "q".into(),
                ..Incoming::default()
            },
        );
        assert_eq!(back.id, "a");
        assert_eq!(back.subject.as_deref(), Some("x"));
        assert_eq!(back.extensions["tenant"], "acme");
        assert_eq!(&back.data[..], b"{\"n\":1}");
    }

    #[test]
    fn binary_headers_round_trip_through_a_source() {
        let ev = CloudEvent::new("id-9", "/s", "t")
            .with_subject("sub")
            .with_time("2026-10-13T00:00:00Z")
            .with_extension("tenant", "acme")
            .with_data("text/plain", Bytes::from_static(b"x"));
        let headers = binary_headers(&ev, "ce_");
        assert_eq!(header_id(&headers), Some("id-9"));
        let back = to_event(
            &env(),
            Incoming {
                headers,
                content_type: ev.datacontenttype.clone(),
                data: ev.data.clone(),
                fallback_id: "never".into(),
                resource: "q".into(),
                subject: None,
            },
        );
        assert_eq!(back.id, ev.id);
        assert_eq!(back.ty, ev.ty);
        assert_eq!(back.source, ev.source);
        assert_eq!(back.subject, ev.subject);
        assert_eq!(back.time, ev.time);
        assert_eq!(back.extensions["tenant"], "acme");
    }

    #[test]
    fn arrow_batches_are_poison() {
        use loams_connector_sdk::ErrorClass;
        assert!(events(EventBatch::Events(vec![]), "x").is_ok());
        let schema = std::sync::Arc::new(arrow_schema::Schema::empty());
        let batch = arrow_array::RecordBatch::new_empty(schema);
        let e = events(
            EventBatch::Arrow {
                batch,
                envelope: CloudEvent::new("a", "/s", "t"),
            },
            "x",
        )
        .unwrap_err();
        assert_eq!(e.class, ErrorClass::Poison);
    }
}
