//! Event notifications: the low-latency path beside listing, where the cloud
//! has one (CN2 Task 13): S3 through SQS (or EventBridge), GCS through
//! Pub/Sub, Azure Blob through Event Grid. All three run nightly only,
//! against the real clouds.
//!
//! This module owns what is the family's: the three notification dialects
//! parsed into `object` and `object-deleted` events (§33 event names), and a
//! [`Source`] over any [`NotificationFeed`] that acknowledges a message only
//! after the Fabric has its event. The queue clients themselves (SQS,
//! Pub/Sub, Event Grid delivery) are the cloud messaging family's (Task 19)
//! and reach this source through the trait (R13.4).

use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source, SourceBatch,
    Split, ids,
};
use serde_json::Value;

use crate::list::OBJECT_EVENT;
use crate::store::pct_decode;

/// The event a deleted object becomes.
pub const OBJECT_DELETED_EVENT: &str = "object-deleted";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    Created,
    Deleted,
}

/// One object notification, whatever the cloud.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub kind: NotificationKind,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub size: Option<u64>,
    /// The store's ordering token (S3 `sequencer`, GCS generation, Azure
    /// `sequencer`), which makes a deletion's id stable.
    pub sequencer: Option<String>,
    pub time: Option<String>,
}

impl Notification {
    /// The event's id: `"<bucket>/<key>@<etag>"` for a new object (the same
    /// id the listing source gives it, so `ingest` keeps one), and
    /// `"<bucket>/<key>@deleted:<sequencer>"` for a deletion.
    pub fn id(&self) -> String {
        match self.kind {
            NotificationKind::Created => ids::object(
                &self.bucket,
                &self.key,
                self.etag
                    .as_deref()
                    .or(self.sequencer.as_deref())
                    .unwrap_or("unknown"),
            ),
            NotificationKind::Deleted => format!(
                "{}/{}@deleted:{}",
                self.bucket,
                self.key,
                self.sequencer
                    .as_deref()
                    .or(self.time.as_deref())
                    .unwrap_or("unknown")
            ),
        }
    }

    pub fn to_event(&self, envelope: &EnvelopeBuilder) -> CloudEvent {
        let name = match self.kind {
            NotificationKind::Created => OBJECT_EVENT,
            NotificationKind::Deleted => OBJECT_DELETED_EVENT,
        };
        let data = serde_json::json!({
            "bucket": self.bucket,
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
        });
        let mut ev = envelope
            .event(name, &self.bucket, self.id())
            .with_subject(self.key.clone())
            .with_data("application/json", data.to_string());
        if let Some(t) = &self.time {
            ev = ev.with_time(t.clone());
        }
        ev
    }
}

fn s(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn u(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn bad(what: &str) -> ConnectorError {
    ConnectorError::poison(format!("not a {what} notification"))
}

/// An S3 event notification (the `Records` document SQS and SNS carry), or
/// an EventBridge `Object Created` / `Object Deleted` event. `s3:TestEvent`
/// messages yield nothing.
pub fn parse_s3(body: &[u8]) -> Result<Vec<Notification>, ConnectorError> {
    let v: Value = serde_json::from_slice(body).map_err(|_| bad("S3"))?;
    // SNS wraps the document in `Message`.
    if let Some(Value::String(inner)) = v.get("Message")
        && v.get("Type").and_then(Value::as_str) == Some("Notification")
    {
        return parse_s3(inner.as_bytes());
    }
    if v.get("Event").and_then(Value::as_str) == Some("s3:TestEvent") {
        return Ok(Vec::new());
    }
    if let Some(records) = v.get("Records").and_then(Value::as_array) {
        let mut out = Vec::new();
        for r in records {
            let name = r
                .get("eventName")
                .and_then(Value::as_str)
                .ok_or_else(|| bad("S3"))?;
            let kind = if name.starts_with("ObjectCreated:") {
                NotificationKind::Created
            } else if name.starts_with("ObjectRemoved:") {
                NotificationKind::Deleted
            } else {
                continue;
            };
            let s3 = r.get("s3").ok_or_else(|| bad("S3"))?;
            let obj = s3.get("object").ok_or_else(|| bad("S3"))?;
            out.push(Notification {
                kind,
                bucket: s3["bucket"]["name"]
                    .as_str()
                    .ok_or_else(|| bad("S3"))?
                    .to_owned(),
                // S3 form-encodes keys in notifications.
                key: pct_decode(obj["key"].as_str().ok_or_else(|| bad("S3"))?),
                etag: s(&obj["eTag"]),
                size: u(&obj["size"]),
                sequencer: s(&obj["sequencer"]),
                time: s(&r["eventTime"]),
            });
        }
        return Ok(out);
    }
    if v.get("source").and_then(Value::as_str) == Some("aws.s3") {
        let kind = match v.get("detail-type").and_then(Value::as_str) {
            Some("Object Created") => NotificationKind::Created,
            Some("Object Deleted") => NotificationKind::Deleted,
            _ => return Ok(Vec::new()),
        };
        let d = &v["detail"];
        return Ok(vec![Notification {
            kind,
            bucket: d["bucket"]["name"]
                .as_str()
                .ok_or_else(|| bad("EventBridge S3"))?
                .to_owned(),
            key: d["object"]["key"]
                .as_str()
                .ok_or_else(|| bad("EventBridge S3"))?
                .to_owned(),
            etag: s(&d["object"]["etag"]),
            size: u(&d["object"]["size"]),
            sequencer: s(&d["object"]["sequencer"]),
            time: s(&v["time"]),
        }]);
    }
    Err(bad("S3"))
}

/// A GCS Pub/Sub notification: the message attributes (`eventType`,
/// `bucketId`, `objectId`, `objectGeneration`) and, with payload format
/// `JSON_API_V1`, the object resource as data.
pub fn parse_gcs(
    attributes: &BTreeMap<String, String>,
    data: &[u8],
) -> Result<Option<Notification>, ConnectorError> {
    let kind = match attributes.get("eventType").map(String::as_str) {
        Some("OBJECT_FINALIZE") => NotificationKind::Created,
        Some("OBJECT_DELETE") => NotificationKind::Deleted,
        Some(_) => return Ok(None),
        None => return Err(bad("GCS")),
    };
    let resource: Value = serde_json::from_slice(data).unwrap_or(Value::Null);
    Ok(Some(Notification {
        kind,
        bucket: attributes
            .get("bucketId")
            .cloned()
            .ok_or_else(|| bad("GCS"))?,
        key: attributes
            .get("objectId")
            .cloned()
            .ok_or_else(|| bad("GCS"))?,
        etag: s(&resource["etag"]),
        size: u(&resource["size"]),
        sequencer: attributes
            .get("objectGeneration")
            .cloned()
            .or_else(|| s(&resource["generation"])),
        time: attributes
            .get("eventTime")
            .cloned()
            .or_else(|| s(&resource["updated"])),
    }))
}

/// Event Grid's `Microsoft.Storage.BlobCreated` and `BlobDeleted`, in the
/// Event Grid schema (`eventType`) or the CloudEvents schema (`type`), one
/// event or an array.
pub fn parse_event_grid(body: &[u8]) -> Result<Vec<Notification>, ConnectorError> {
    let v: Value = serde_json::from_slice(body).map_err(|_| bad("Event Grid"))?;
    let items = match v {
        Value::Array(a) => a,
        one => vec![one],
    };
    let mut out = Vec::new();
    for e in items {
        let ty = e
            .get("eventType")
            .or_else(|| e.get("type"))
            .and_then(Value::as_str)
            .ok_or_else(|| bad("Event Grid"))?;
        let kind = match ty {
            "Microsoft.Storage.BlobCreated" => NotificationKind::Created,
            "Microsoft.Storage.BlobDeleted" => NotificationKind::Deleted,
            _ => continue,
        };
        let d = &e["data"];
        let url = d["url"].as_str().ok_or_else(|| bad("Event Grid"))?;
        let path = url
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('/'))
            .map(|(_, p)| p)
            .ok_or_else(|| bad("Event Grid"))?;
        let path = path.split('?').next().unwrap_or(path);
        let (container, key) = path.split_once('/').ok_or_else(|| bad("Event Grid"))?;
        out.push(Notification {
            kind,
            bucket: pct_decode(container),
            key: pct_decode(key),
            etag: s(&d["eTag"]).map(|e| e.trim_matches('"').to_owned()),
            size: u(&d["contentLength"]),
            sequencer: s(&d["sequencer"]),
            time: s(&e["eventTime"]).or_else(|| s(&e["time"])),
        });
    }
    Ok(out)
}

/// Which dialect a feed's messages are in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    S3,
    Gcs,
    EventGrid,
}

/// A message received from a queue or subscription.
#[derive(Debug, Clone)]
pub struct Received {
    /// What `ack` takes: an SQS receipt handle, a Pub/Sub ack id.
    pub handle: String,
    pub body: Bytes,
    pub attributes: BTreeMap<String, String>,
}

/// A queue of notifications (SQS, a Pub/Sub subscription, an Event Grid
/// delivery queue). Unacknowledged messages come back after the queue's
/// visibility timeout or ack deadline, which is the source's at-least-once.
#[async_trait]
pub trait NotificationFeed: Send {
    /// Up to `max` messages; empty when none is waiting.
    async fn receive(&mut self, max: usize) -> Result<Vec<Received>, ConnectorError>;
    /// Delete or acknowledge messages whose events the Fabric has.
    async fn ack(&mut self, handles: Vec<String>) -> Result<(), ConnectorError>;
}

/// A [`Source`] over a [`NotificationFeed`]. Its position is a batch
/// counter: on a restart the queue redelivers what was not acknowledged.
pub struct NotificationSource {
    feed: Box<dyn NotificationFeed>,
    dialect: Dialect,
    envelope: EnvelopeBuilder,
    seq: u64,
    inflight: BTreeMap<u64, Vec<String>>,
    held: VecDeque<(String, CloudEvent)>,
    poisoned: VecDeque<ConnectorError>,
    poisoned_handles: Vec<String>,
}

impl std::fmt::Debug for NotificationSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationSource")
            .field("dialect", &self.dialect)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

impl NotificationSource {
    pub fn new(
        feed: Box<dyn NotificationFeed>,
        dialect: Dialect,
        envelope: EnvelopeBuilder,
    ) -> Self {
        Self {
            feed,
            dialect,
            envelope,
            seq: 0,
            inflight: BTreeMap::new(),
            held: VecDeque::new(),
            poisoned: VecDeque::new(),
            poisoned_handles: Vec::new(),
        }
    }

    fn parse(&self, m: &Received) -> Result<Vec<Notification>, ConnectorError> {
        match self.dialect {
            Dialect::S3 => parse_s3(&m.body),
            Dialect::Gcs => Ok(parse_gcs(&m.attributes, &m.body)?.into_iter().collect()),
            Dialect::EventGrid => parse_event_grid(&m.body),
        }
    }
}

#[async_trait]
impl Source for NotificationSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        self.seq = pos.and_then(|p| p.as_u64()).unwrap_or(0);
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(e) = self.poisoned.pop_front() {
            return Err(e);
        }
        if self.held.is_empty() {
            for m in self.feed.receive(budget.max_rows.clamp(1, 10)).await? {
                match self.parse(&m) {
                    Ok(ns) if ns.is_empty() => self.poisoned_handles.push(m.handle.clone()),
                    Ok(ns) => {
                        for n in ns {
                            self.held
                                .push_back((m.handle.clone(), n.to_event(&self.envelope)));
                        }
                    }
                    Err(e) => {
                        // The message goes to the DLQ as an error and is
                        // acknowledged with the next batch.
                        self.poisoned.push_back(e);
                        self.poisoned_handles.push(m.handle.clone());
                    }
                }
            }
        }
        let mut events = Vec::new();
        let mut handles = std::mem::take(&mut self.poisoned_handles);
        let mut bytes = 0;
        while let Some((h, ev)) = self.held.pop_front() {
            let size = ev.size_bytes();
            if events.len() >= budget.max_rows
                || (bytes + size > budget.max_bytes && !events.is_empty())
            {
                self.held.push_front((h, ev));
                break;
            }
            bytes += size;
            events.push(ev);
            // A message with several records is acknowledged with its last.
            if !self.held.iter().any(|(other, _)| *other == h) {
                handles.push(h);
            }
        }
        if events.is_empty() {
            if !handles.is_empty() {
                self.feed.ack(handles).await?;
            }
            return match self.poisoned.pop_front() {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        self.seq += 1;
        self.inflight.insert(self.seq, handles);
        Ok(Some(SourceBatch {
            events: EventBatch::Events(events),
            position: Position::from_u64(self.seq),
        }))
    }

    async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError> {
        let Some(upto) = upto.as_u64() else {
            return Ok(());
        };
        let keep = self.inflight.split_off(&(upto + 1));
        let done = std::mem::replace(&mut self.inflight, keep);
        let handles: Vec<String> = done.into_values().flatten().collect();
        if !handles.is_empty() {
            self.feed.ack(handles).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loams_connector_sdk::InstanceKey;

    #[test]
    fn s3_records_parse_with_decoded_keys() {
        let body = br#"{"Records":[
          {"eventVersion":"2.1","eventSource":"aws:s3","eventTime":"2026-10-12T10:00:00.000Z",
           "eventName":"ObjectCreated:Put",
           "s3":{"bucket":{"name":"b"},"object":{"key":"in/a+b%3D.csv","size":10,"eTag":"e1","sequencer":"0A1"}}},
          {"eventName":"ObjectRemoved:Delete","eventTime":"2026-10-12T10:00:01.000Z",
           "s3":{"bucket":{"name":"b"},"object":{"key":"in/old.csv","sequencer":"0A2"}}}
        ]}"#;
        let n = parse_s3(body).expect("parses");
        assert_eq!(n.len(), 2);
        assert_eq!(n[0].key, "in/a b=.csv");
        assert_eq!(n[0].id(), "b/in/a b=.csv@e1");
        assert_eq!(n[1].kind, NotificationKind::Deleted);
        assert_eq!(n[1].id(), "b/in/old.csv@deleted:0A2");
        assert!(
            parse_s3(br#"{"Event":"s3:TestEvent"}"#)
                .expect("test event")
                .is_empty()
        );
        assert!(parse_s3(b"nope").is_err());
    }

    #[test]
    fn eventbridge_and_sns_forms_parse() {
        let eb = br#"{"source":"aws.s3","detail-type":"Object Created","time":"t",
          "detail":{"bucket":{"name":"b"},"object":{"key":"k","size":3,"etag":"e"}}}"#;
        assert_eq!(parse_s3(eb).expect("eb")[0].id(), "b/k@e");
        let sns = serde_json::json!({"Type":"Notification","Message": String::from_utf8_lossy(eb)});
        assert_eq!(parse_s3(sns.to_string().as_bytes()).expect("sns").len(), 1);
    }

    #[test]
    fn gcs_attributes_parse() {
        let mut a = BTreeMap::new();
        a.insert("eventType".into(), "OBJECT_FINALIZE".into());
        a.insert("bucketId".into(), "b".into());
        a.insert("objectId".into(), "dir/x.json".into());
        a.insert("objectGeneration".into(), "17".into());
        let n = parse_gcs(&a, br#"{"etag":"CJ","size":"5"}"#)
            .expect("parses")
            .expect("some");
        assert_eq!((n.size, n.id()), (Some(5), "b/dir/x.json@CJ".to_owned()));
        a.insert("eventType".into(), "OBJECT_METADATA_UPDATE".into());
        assert!(parse_gcs(&a, b"").expect("parses").is_none());
    }

    #[test]
    fn event_grid_both_schemas_parse() {
        let eg = br#"[{"eventType":"Microsoft.Storage.BlobCreated","eventTime":"t",
          "data":{"url":"https://acct.blob.core.windows.net/c/dir/a%20b.json","eTag":"\"0x8D\"","contentLength":4,"sequencer":"01"}}]"#;
        let n = parse_event_grid(eg).expect("parses");
        assert_eq!(
            (n[0].bucket.as_str(), n[0].key.as_str()),
            ("c", "dir/a b.json")
        );
        assert_eq!(n[0].etag.as_deref(), Some("0x8D"));
        let ce = br#"{"specversion":"1.0","type":"Microsoft.Storage.BlobDeleted","time":"t",
          "data":{"url":"https://acct.blob.core.windows.net/c/k","sequencer":"02"}}"#;
        assert_eq!(parse_event_grid(ce).expect("ce")[0].id(), "c/k@deleted:02");
    }

    #[test]
    fn notification_events_use_the_family_names() {
        let env = EnvelopeBuilder::new("s3", &InstanceKey::new(1, "i"));
        let n = parse_s3(
            br#"{"Records":[{"eventName":"ObjectRemoved:Delete",
          "s3":{"bucket":{"name":"b"},"object":{"key":"k","sequencer":"9"}}}]}"#,
        )
        .expect("parses");
        let ev = n[0].to_event(&env);
        assert_eq!(ev.ty, "io.loams.dev.flow.s3.object-deleted.v1");
        assert_eq!(ev.subject.as_deref(), Some("k"));
    }
}
