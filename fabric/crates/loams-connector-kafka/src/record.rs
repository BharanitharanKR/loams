//! Kafka records and CloudEvents (design §33 §6, D270).
//!
//! **Reading.** A record whose headers carry CloudEvents binary-mode
//! attributes (`ce_id`, `ce_source`, `ce_type`, `ce_specversion`, …) is passed
//! through: its `type`, `source` and `id` are kept (Review Focus 10). A record
//! whose `content-type` is `application/cloudevents+json` is a structured-mode
//! event and is passed through likewise. Anything else becomes
//! `io.loams.dev.flow.<connector>.record.v1` with the id
//! `<topic>/<partition>/<offset>` (`ids::log_record`, stable across re-reads),
//! the payload as `data`, the record's `content-type` header (or
//! `application/octet-stream`) as `datacontenttype`, its timestamp as `time`,
//! and its key as the `partitionkey` extension (the CloudEvents Kafka
//! binding's), or `loamskeyb64` when the key is not UTF-8.
//!
//! **Writing.** An event becomes a binary-mode record (D270): the payload is
//! `data`, `content-type` is `datacontenttype`, every other attribute and
//! extension is a `ce_<name>` header, and the key is `partitionkey` (or the
//! decoded `loamskeyb64`), so a record read back is the same event.

use base64::Engine as _;
use bytes::Bytes;
use loams_connector_sdk::{CloudEvent, EnvelopeBuilder, ids};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};

/// The event name of a plain record.
pub const RECORD_EVENT: &str = "record";

/// The CloudEvents Kafka binding's key extension.
pub const PARTITION_KEY: &str = "partitionkey";

/// A key that is not UTF-8, base64 (standard alphabet).
pub const KEY_B64: &str = "loamskeyb64";

const STRUCTURED: &str = "application/cloudevents+json";
const OCTETS: &str = "application/octet-stream";

/// The CloudEvent a consumed record carries (see the module comment).
pub fn event_from_message<M: Message>(env: &EnvelopeBuilder, m: &M) -> CloudEvent {
    let payload = Bytes::copy_from_slice(m.payload().unwrap_or_default());
    let mut headers: Vec<(&str, &[u8])> = Vec::new();
    if let Some(h) = m.headers() {
        for Header { key, value } in h.iter() {
            headers.push((key, value.unwrap_or_default()));
        }
    }
    let content_type = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        .and_then(|(_, v)| std::str::from_utf8(v).ok());

    let passed = if headers.iter().any(|(k, _)| is_ce_header(k)) {
        env.from_binary(headers.iter().copied(), payload.clone())
    } else if content_type.is_some_and(|ct| ct.starts_with(STRUCTURED)) {
        env.from_structured_json(&payload)
    } else {
        None
    };
    let mut ev = match passed {
        Some(ev) => ev,
        None => {
            let id = ids::log_record(
                m.topic(),
                m.partition().max(0) as u32,
                m.offset().max(0) as u64,
            );
            let mut ev = env
                .event(RECORD_EVENT, m.topic(), id)
                .with_data(content_type.unwrap_or(OCTETS).to_owned(), payload);
            if let Some(ms) = m.timestamp().to_millis() {
                ev = ev.with_time(rfc3339_millis(ms));
            }
            ev
        }
    };
    if let Some(key) = m.key()
        && !ev.extensions.contains_key(PARTITION_KEY)
        && !ev.extensions.contains_key(KEY_B64)
    {
        match std::str::from_utf8(key) {
            Ok(k) => {
                ev.extensions.insert(PARTITION_KEY.to_owned(), k.to_owned());
            }
            Err(_) => {
                ev.extensions.insert(
                    KEY_B64.to_owned(),
                    base64::engine::general_purpose::STANDARD.encode(key),
                );
            }
        }
    }
    ev
}

fn is_ce_header(k: &str) -> bool {
    k.len() > 3 && (k[..3].eq_ignore_ascii_case("ce_") || k[..3].eq_ignore_ascii_case("ce-"))
}

/// A record about to be produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutRecord {
    pub key: Option<Vec<u8>>,
    pub payload: Bytes,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl OutRecord {
    /// Bytes on the wire, near enough for `message.max.bytes`: key, value
    /// and headers.
    pub fn size(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len)
            + self.payload.len()
            + self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }

    pub fn owned_headers(&self) -> OwnedHeaders {
        let mut h = OwnedHeaders::new_with_capacity(self.headers.len());
        for (k, v) in &self.headers {
            h = h.insert(Header {
                key: k,
                value: Some(v),
            });
        }
        h
    }
}

/// The binary-mode record of an event (D270).
pub fn record_from_event(ev: &CloudEvent) -> OutRecord {
    let mut headers: Vec<(String, Vec<u8>)> = vec![
        (
            "ce_specversion".to_owned(),
            ev.specversion.clone().into_bytes(),
        ),
        ("ce_id".to_owned(), ev.id.clone().into_bytes()),
        ("ce_source".to_owned(), ev.source.clone().into_bytes()),
        ("ce_type".to_owned(), ev.ty.clone().into_bytes()),
    ];
    if let Some(s) = &ev.subject {
        headers.push(("ce_subject".to_owned(), s.clone().into_bytes()));
    }
    if let Some(t) = &ev.time {
        headers.push(("ce_time".to_owned(), t.clone().into_bytes()));
    }
    if let Some(ct) = &ev.datacontenttype {
        headers.push(("content-type".to_owned(), ct.clone().into_bytes()));
    }
    let mut key = None;
    for (k, v) in &ev.extensions {
        match k.as_str() {
            PARTITION_KEY => key = Some(v.clone().into_bytes()),
            KEY_B64 => {
                if let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(v) {
                    key = Some(raw);
                }
            }
            _ => headers.push((format!("ce_{k}"), v.clone().into_bytes())),
        }
    }
    OutRecord {
        key,
        payload: ev.data.clone(),
        headers,
    }
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` for milliseconds since the Unix epoch.
pub fn rfc3339_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to a
/// proleptic-Gregorian date.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;
    use rdkafka::message::{OwnedMessage, Timestamp};

    fn env() -> EnvelopeBuilder {
        EnvelopeBuilder::new("kafka", &InstanceKey::new(1, "orders-in"))
    }

    fn message(key: Option<&[u8]>, payload: &[u8], headers: Option<OwnedHeaders>) -> OwnedMessage {
        OwnedMessage::new(
            Some(payload.to_vec()),
            key.map(<[u8]>::to_vec),
            "orders".to_owned(),
            Timestamp::CreateTime(1_760_000_000_123),
            3,
            42,
            headers,
        )
    }

    #[test]
    fn dates() {
        assert_eq!(rfc3339_millis(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339_millis(1_760_000_000_123),
            "2025-10-09T08:53:20.123Z"
        );
        assert_eq!(rfc3339_millis(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(rfc3339_millis(-1), "1969-12-31T23:59:59.999Z");
    }

    #[test]
    fn plain_record_gets_a_stable_id() {
        let m = message(Some(b"k1"), b"{\"a\":1}", None);
        let ev = event_from_message(&env(), &m);
        assert_eq!(ev.id, "orders/3/42");
        assert_eq!(ev.ty, "io.loams.dev.flow.kafka.record.v1");
        assert_eq!(ev.source, "/connectors/orders-in/orders");
        assert_eq!(ev.datacontenttype.as_deref(), Some(OCTETS));
        assert_eq!(ev.time.as_deref(), Some("2025-10-09T08:53:20.123Z"));
        assert_eq!(
            ev.extensions.get(PARTITION_KEY).map(String::as_str),
            Some("k1")
        );
        assert_eq!(
            ev.extensions.get("loamsconnector").map(String::as_str),
            Some("kafka")
        );
        // The same record read again is the same event.
        assert_eq!(event_from_message(&env(), &m), ev);
    }

    #[test]
    fn binary_mode_passes_through_and_round_trips() {
        let upstream = CloudEvent::new("evt-1", "/upstream/svc", "com.example.order.created")
            .with_subject("order/9")
            .with_time("2026-10-01T00:00:00Z")
            .with_data("application/json", Bytes::from_static(b"{\"x\":1}"))
            .with_extension("traceparent", "00-abc-def-01")
            .with_extension(PARTITION_KEY, "order-9");
        let rec = record_from_event(&upstream);
        assert_eq!(rec.key.as_deref(), Some(&b"order-9"[..]));
        assert!(
            rec.headers
                .iter()
                .any(|(k, v)| k == "ce_id" && v == b"evt-1")
        );
        assert!(rec.headers.iter().any(|(k, _)| k == "content-type"));
        assert!(!rec.headers.iter().any(|(k, _)| k == "ce_partitionkey"));

        let m = message(rec.key.as_deref(), &rec.payload, Some(rec.owned_headers()));
        let back = event_from_message(&env(), &m);
        assert_eq!(back.id, "evt-1");
        assert_eq!(back.ty, "com.example.order.created");
        assert_eq!(back.source, "/upstream/svc");
        assert_eq!(back.subject.as_deref(), Some("order/9"));
        assert_eq!(back.time.as_deref(), Some("2026-10-01T00:00:00Z"));
        assert_eq!(back.datacontenttype.as_deref(), Some("application/json"));
        assert_eq!(back.data, upstream.data);
        assert_eq!(
            back.extensions.get("traceparent").map(String::as_str),
            Some("00-abc-def-01")
        );
        assert_eq!(
            back.extensions.get(PARTITION_KEY).map(String::as_str),
            Some("order-9")
        );
    }

    #[test]
    fn structured_mode_passes_through() {
        let body =
            br#"{"specversion":"1.0","id":"s-1","source":"/s","type":"t.v1","data":{"k":2}}"#;
        let headers = OwnedHeaders::new().insert(Header {
            key: "content-type",
            value: Some("application/cloudevents+json; charset=utf-8"),
        });
        let ev = event_from_message(&env(), &message(None, body, Some(headers)));
        assert_eq!((ev.id.as_str(), ev.ty.as_str()), ("s-1", "t.v1"));
    }

    #[test]
    fn binary_key_survives() {
        let key = [0xff, 0x00, 0x10];
        let ev = event_from_message(&env(), &message(Some(&key), b"x", None));
        assert!(ev.extensions.contains_key(KEY_B64));
        assert_eq!(record_from_event(&ev).key.as_deref(), Some(&key[..]));
    }
}
