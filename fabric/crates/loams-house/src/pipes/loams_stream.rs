//! The source of `LoamsStream` pipes: a Loams stream, read through the engine's
//! native stream API (M0.3's fetch; `loams.stream.v1` replaces it when API1
//! lands, behind the same [`StreamSource`]).
//!
//! * `GET /v1/namespaces/{ns}/streams/{stream}` gives each partition's bounds
//!   (`log_start_offset`, `high_watermark`);
//! * `GET …/partitions/{p}/records?offset=&max_bytes=` gives records, their
//!   values base64 in JSON, and `next_offset`;
//! * `GET …/partitions/{p}/events?offset=&max_bytes=` gives the same records
//!   as a structured CloudEvents batch, with `loams-next-offset` and
//!   `loams-high-watermark` headers: the `CloudEvents` format reads it.
//!
//! A record becomes one row of the pipe's format: a `JSONEachRow` or `CSV`
//! record's value is a row as it is (a trailing line break is added when it
//! has none); a CloudEvent becomes one JSON object of §32 §5.4's `_ce_*`
//! columns ([`cloudevent_row`]).
//!
//! The engine is reached with the House's own key for it (`[house.engine]`),
//! never a caller's, and the House namespace's stream namespace is the
//! operator's mapping, so a pipe reads only its own namespace's streams.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{Map, Value};

use crate::errors::{ChError, HouseError};

/// A partition's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    /// The partition.
    pub partition: u32,
    /// Its first retained offset.
    pub log_start_offset: u64,
    /// The offset its next record gets.
    pub high_watermark: u64,
}

/// What one fetch gave.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fetched {
    /// One row per record, in the pipe's format, each ending in a line break.
    pub rows: Vec<Vec<u8>>,
    /// Where the next fetch starts.
    pub next_offset: u64,
    /// The partition's high watermark when it answered.
    pub high_watermark: u64,
}

/// Where a pipe's records come from.
#[async_trait]
pub trait StreamSource: Send + Sync + fmt::Debug {
    /// The stream's partitions that hold or held records.
    async fn partitions(&self, namespace: &str, stream: &str) -> Result<Vec<Bounds>, HouseError>;
    /// Records of one partition from `offset`, at most about `max_bytes`;
    /// `events`: as CloudEvents rows.
    async fn fetch(
        &self,
        namespace: &str,
        stream: &str,
        partition: u32,
        offset: u64,
        max_bytes: usize,
        events: bool,
    ) -> Result<Fetched, HouseError>;
}

/// The engine's stream API over HTTP.
pub struct EngineStreams {
    base: String,
    key: Option<String>,
    client: reqwest::Client,
}

impl fmt::Debug for EngineStreams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EngineStreams")
            .field("base", &self.base)
            .field("key", &self.key.as_ref().map(|_| "[redacted]"))
            .finish()
    }
}

fn unavailable(what: &str) -> HouseError {
    HouseError::from(ChError::network_error(format!(
        "the engine's stream API failed: {what}"
    )))
}

/// A path segment, percent-encoded.
fn segment(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl EngineStreams {
    /// The API at `base` (`https://engine:8080`), with the House's key for it.
    pub fn new(base: &str, key: Option<String>) -> Result<Self, HouseError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| unavailable("its HTTP client could not be built"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            key,
            client,
        })
    }

    fn stream_url(&self, namespace: &str, stream: &str) -> String {
        format!(
            "{}/v1/namespaces/{}/streams/{}",
            self.base,
            segment(namespace),
            segment(stream)
        )
    }

    async fn get(&self, url: &str) -> Result<reqwest::Response, HouseError> {
        let mut request = self.client.get(url);
        if let Some(key) = &self.key {
            request = request.bearer_auth(key);
        }
        // The error's own words may name the engine's address: kept out.
        let response = request.send().await.map_err(|_| unavailable("no answer"))?;
        match response.status().as_u16() {
            200..=299 => Ok(response),
            404 => Err(HouseError::from(ChError::bad_arguments(
                "the pipe's stream does not exist in its namespace",
            ))),
            401 | 403 => Err(unavailable(
                "the House's key for the engine was refused ([house.engine])",
            )),
            status => Err(unavailable(&format!("status {status}"))),
        }
    }
}

async fn json(response: reqwest::Response, what: &str) -> Result<Value, HouseError> {
    let bytes = response.bytes().await.map_err(|_| unavailable(what))?;
    serde_json::from_slice(&bytes).map_err(|_| unavailable(what))
}

fn number(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

#[async_trait]
impl StreamSource for EngineStreams {
    async fn partitions(&self, namespace: &str, stream: &str) -> Result<Vec<Bounds>, HouseError> {
        let response = self.get(&self.stream_url(namespace, stream)).await?;
        let body = json(response, "an unreadable stream description").await?;
        Ok(body
            .get("partitions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|p| Bounds {
                partition: number(p, "partition") as u32,
                log_start_offset: number(p, "log_start_offset"),
                high_watermark: number(p, "high_watermark"),
            })
            .collect())
    }

    async fn fetch(
        &self,
        namespace: &str,
        stream: &str,
        partition: u32,
        offset: u64,
        max_bytes: usize,
        events: bool,
    ) -> Result<Fetched, HouseError> {
        let url = format!(
            "{}/partitions/{partition}/{}?offset={offset}&max_bytes={max_bytes}",
            self.stream_url(namespace, stream),
            if events { "events" } else { "records" }
        );
        let response = self.get(&url).await?;
        if events {
            let header = |name: &str| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
            };
            let next = header("loams-next-offset").unwrap_or(offset);
            let high = header("loams-high-watermark").unwrap_or(next);
            let body: Value = if response.status().as_u16() == 204 {
                Value::Array(Vec::new())
            } else {
                json(response, "an unreadable CloudEvents batch").await?
            };
            let rows = body
                .as_array()
                .into_iter()
                .flatten()
                .map(cloudevent_row)
                .collect();
            return Ok(Fetched {
                rows,
                next_offset: next,
                high_watermark: high,
            });
        }
        let body = json(response, "an unreadable fetch").await?;
        let mut rows = Vec::new();
        for record in body
            .get("records")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let value = record.get("value").and_then(Value::as_str).unwrap_or("");
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| unavailable("a record value that is not base64"))?;
            rows.push(row(bytes));
        }
        Ok(Fetched {
            rows,
            next_offset: number(&body, "next_offset").max(offset),
            high_watermark: number(&body, "high_watermark"),
        })
    }
}

/// A record's value as one row: with a line break at its end.
pub fn row(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.last() != Some(&b'\n') {
        bytes.push(b'\n');
    }
    bytes
}

/// The `_ce_*` columns of a pipe in the `CloudEvents` format (§32 §5.4):
/// `_ce_id`, `_ce_source`, `_ce_type`, `_ce_subject`, `_ce_time`,
/// `_ce_dataschema`, `_ce_datacontenttype`, `_ce_data` (the data as text: JSON
/// as written, `data_base64` decoded) and `_ce_ext` (every other attribute).
pub const CLOUDEVENT_COLUMNS: &[&str] = &[
    "_ce_id",
    "_ce_source",
    "_ce_type",
    "_ce_subject",
    "_ce_time",
    "_ce_dataschema",
    "_ce_datacontenttype",
    "_ce_data",
    "_ce_ext",
];

const KNOWN: &[&str] = &[
    "id",
    "source",
    "type",
    "subject",
    "time",
    "dataschema",
    "datacontenttype",
    "data",
    "data_base64",
    "specversion",
];

/// One structured CloudEvent as a `JSONEachRow` row of [`CLOUDEVENT_COLUMNS`].
pub fn cloudevent_row(event: &Value) -> Vec<u8> {
    let text = |key: &str| match event.get(key) {
        Some(Value::String(s)) => Value::String(s.clone()),
        Some(Value::Null) | None => Value::String(String::new()),
        Some(other) => Value::String(other.to_string()),
    };
    let data = match (event.get("data"), event.get("data_base64")) {
        (Some(Value::String(s)), _) => s.clone(),
        (Some(Value::Null) | None, Some(Value::String(b64))) => {
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(_) => b64.clone(),
            }
        }
        (Some(Value::Null) | None, _) => String::new(),
        (Some(other), _) => other.to_string(),
    };
    let mut ext = Map::new();
    if let Some(object) = event.as_object() {
        for (key, value) in object {
            if !KNOWN.contains(&key.as_str()) {
                let value = match value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                ext.insert(key.clone(), Value::String(value));
            }
        }
    }
    let mut out = Map::new();
    for (column, attribute) in [
        ("_ce_id", "id"),
        ("_ce_source", "source"),
        ("_ce_type", "type"),
        ("_ce_subject", "subject"),
        ("_ce_time", "time"),
        ("_ce_dataschema", "dataschema"),
        ("_ce_datacontenttype", "datacontenttype"),
    ] {
        out.insert(column.to_string(), text(attribute));
    }
    out.insert("_ce_data".to_string(), Value::String(data));
    out.insert("_ce_ext".to_string(), Value::Object(ext));
    let mut bytes = serde_json::to_vec(&Value::Object(out)).unwrap_or_default();
    bytes.push(b'\n');
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudevents_become_ce_columns() {
        let event = serde_json::json!({
            "specversion": "1.0",
            "id": "e1",
            "source": "/orders",
            "type": "order.created",
            "time": "2026-10-16T10:00:00Z",
            "datacontenttype": "application/json",
            "data": {"k": 1},
            "region": "eu",
        });
        let row: Value = serde_json::from_slice(&cloudevent_row(&event)).expect("json");
        assert_eq!(row["_ce_id"], "e1");
        assert_eq!(row["_ce_source"], "/orders");
        assert_eq!(row["_ce_subject"], "");
        assert_eq!(row["_ce_data"], "{\"k\":1}");
        assert_eq!(row["_ce_ext"]["region"], "eu");
        let binary = serde_json::json!({"id": "e2", "source": "s", "type": "t",
            "data_base64": base64::engine::general_purpose::STANDARD.encode(b"hi")});
        let row: Value = serde_json::from_slice(&cloudevent_row(&binary)).expect("json");
        assert_eq!(row["_ce_data"], "hi");
        assert_eq!(super::row(b"a".to_vec()), b"a\n");
        assert_eq!(segment("a b/c"), "a%20b%2Fc");
    }

    /// A one-request-at-a-time HTTP server answering from `answer(path)`,
    /// recording each request's head.
    async fn engine(
        answer: fn(&str) -> (String, String),
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                log.lock().expect("lock").push(head);
                let (headers, body) = answer(&path);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{headers}content-length: \
                     {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn the_engine_api_is_read_with_the_houses_key() {
        let (base, seen) = engine(|path| {
            if path.contains("/records?") {
                let v = base64::engine::general_purpose::STANDARD.encode(b"{\"k\":1}");
                (
                    String::new(),
                    format!(
                        "{{\"records\":[{{\"offset\":5,\"value\":\"{v}\"}}],\"next_offset\":6,\
                         \"high_watermark\":9}}"
                    ),
                )
            } else if path.contains("/events?") {
                (
                    "loams-next-offset: 3\r\nloams-high-watermark: 4\r\n".to_string(),
                    "[{\"id\":\"e\",\"source\":\"s\",\"type\":\"t\"}]".to_string(),
                )
            } else {
                (
                    String::new(),
                    "{\"partitions\":[{\"partition\":1,\"log_start_offset\":2,\"high_watermark\":9}]}"
                        .to_string(),
                )
            }
        })
        .await;
        let api = EngineStreams::new(&base, Some("lk_secret".to_string())).expect("client");
        assert!(!format!("{api:?}").contains("lk_secret"));
        assert_eq!(
            api.partitions("my ns", "orders").await.expect("bounds"),
            vec![Bounds {
                partition: 1,
                log_start_offset: 2,
                high_watermark: 9
            }]
        );
        let fetched = api
            .fetch("my ns", "orders", 1, 5, 1024, false)
            .await
            .expect("records");
        assert_eq!(fetched.rows, vec![b"{\"k\":1}\n".to_vec()]);
        assert_eq!((fetched.next_offset, fetched.high_watermark), (6, 9));
        let events = api
            .fetch("my ns", "orders", 0, 2, 1024, true)
            .await
            .expect("events");
        assert_eq!(events.rows.len(), 1);
        assert_eq!((events.next_offset, events.high_watermark), (3, 4));
        let heads = seen.lock().expect("lock").clone();
        assert!(heads[0].starts_with("GET /v1/namespaces/my%20ns/streams/orders HTTP/1.1"));
        assert!(heads[1].contains("/partitions/1/records?offset=5&max_bytes=1024"));
        assert!(heads[2].contains("/partitions/0/events?offset=2"));
        assert!(heads.iter().all(|h| {
            h.to_ascii_lowercase()
                .contains("authorization: bearer lk_secret")
        }));
    }
}
