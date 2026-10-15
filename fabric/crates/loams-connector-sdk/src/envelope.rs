//! The CloudEvents envelope of design §33 §6 (D355): ids derived from source
//! positions so a re-read yields the same ids, the `loamsconnector` and
//! `loamsinstance` extensions, `loamsop` and `loamslsn` for CDC, and
//! pass-through of input that already is a CloudEvent.

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::position::InstanceKey;

/// A CloudEvent 1.0 as the SDK carries it. `data` is the payload in
/// `datacontenttype`; extensions are string-valued (the Kafka and HTTP binary
/// bindings carry every attribute as a string).
///
/// FL1's `loams-fabric-envelope` is not built yet; when it lands, this type
/// converts to and from it one to one (plan, "Rulings made during execution").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudEvent {
    pub id: String,
    pub source: String,
    pub ty: String,
    pub specversion: String,
    pub subject: Option<String>,
    pub time: Option<String>,
    pub datacontenttype: Option<String>,
    pub data: Bytes,
    pub extensions: BTreeMap<String, String>,
}

impl CloudEvent {
    /// A bare CloudEvent 1.0 with no data.
    pub fn new(id: impl Into<String>, source: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            ty: ty.into(),
            specversion: "1.0".to_owned(),
            subject: None,
            time: None,
            datacontenttype: None,
            data: Bytes::new(),
            extensions: BTreeMap::new(),
        }
    }

    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    pub fn with_time(mut self, time: impl Into<String>) -> Self {
        self.time = Some(time.into());
        self
    }

    pub fn with_data(mut self, content_type: impl Into<String>, data: impl Into<Bytes>) -> Self {
        self.datacontenttype = Some(content_type.into());
        self.data = data.into();
        self
    }

    pub fn with_extension(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extensions.insert(name.into(), value.into());
        self
    }

    /// The CDC extensions: `loamsop` (`c`, `u`, `d`, `r`) and `loamslsn`.
    pub fn with_cdc(self, op: CdcOp, lsn: impl Into<String>) -> Self {
        self.with_extension("loamsop", op.as_str())
            .with_extension("loamslsn", lsn)
    }

    /// The event's size for budgets: payload plus attributes.
    pub fn size_bytes(&self) -> usize {
        let attrs = self.id.len()
            + self.source.len()
            + self.ty.len()
            + self.specversion.len()
            + self.subject.as_ref().map_or(0, String::len)
            + self.time.as_ref().map_or(0, String::len)
            + self.datacontenttype.as_ref().map_or(0, String::len)
            + self
                .extensions
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        attrs + self.data.len()
    }

    /// Whether the required attributes of CloudEvents 1.0 are present.
    pub fn is_valid(&self) -> bool {
        self.specversion == "1.0"
            && !self.id.is_empty()
            && !self.source.is_empty()
            && !self.ty.is_empty()
    }
}

/// The CDC operation of `loamsop`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Create,
    Update,
    Delete,
    Read,
}

impl CdcOp {
    pub fn as_str(self) -> &'static str {
        match self {
            CdcOp::Create => "c",
            CdcOp::Update => "u",
            CdcOp::Delete => "d",
            CdcOp::Read => "r",
        }
    }
}

/// Why input could not be passed through.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    #[error("not a CloudEvent: {0}")]
    NotACloudEvent(String),
}

/// Builds the envelope of every event a connector instance emits
/// (CN2 Shared contracts).
#[derive(Debug, Clone)]
pub struct EnvelopeBuilder {
    connector: String,
    instance: String,
}

impl EnvelopeBuilder {
    pub fn new(connector: impl Into<String>, instance: &InstanceKey) -> Self {
        Self {
            connector: connector.into(),
            instance: instance.name.clone(),
        }
    }

    /// `io.loams.dev.flow.<connector-id>.<event>.v1` (CN1 Ruling 2).
    pub fn type_for(&self, event: &str) -> String {
        format!("io.loams.dev.flow.{}.{event}.v1", self.connector)
    }

    /// `/connectors/<instance>/<resource>` (design §33 §1).
    pub fn source_for(&self, resource: &str) -> String {
        format!("/connectors/{}/{resource}", self.instance)
    }

    /// A Loams-typed event: `type` from `event`, `source` from `resource`,
    /// `id` as given (use [`ids`] to derive it from the position), and the
    /// `loamsconnector` and `loamsinstance` extensions.
    pub fn event(&self, event: &str, resource: &str, id: impl Into<String>) -> CloudEvent {
        let ev = CloudEvent::new(id, self.source_for(resource), self.type_for(event));
        self.stamp(ev)
    }

    /// Pass-through (design §33 §6): input that already is a valid CloudEvent
    /// keeps its `type`, `source` and `id`; only `loamsconnector` and
    /// `loamsinstance` are added.
    pub fn passthrough(&self, ev: CloudEvent) -> Result<CloudEvent, EnvelopeError> {
        if !ev.is_valid() {
            return Err(EnvelopeError::NotACloudEvent(
                "specversion 1.0, id, source and type are required".to_owned(),
            ));
        }
        Ok(self.stamp(ev))
    }

    /// Pass-through from the binary content mode: Kafka's `ce_` headers
    /// (D270) or HTTP's `ce-` headers. Returns `None` when the headers do not
    /// make a CloudEvent (the caller wraps the record instead); a
    /// `content-type` header becomes `datacontenttype`.
    pub fn from_binary<'a>(
        &self,
        headers: impl IntoIterator<Item = (&'a str, &'a [u8])>,
        data: Bytes,
    ) -> Option<CloudEvent> {
        let mut ev = CloudEvent::new("", "", "");
        ev.specversion.clear();
        let mut any = false;
        for (name, value) in headers {
            let lower = name.to_ascii_lowercase();
            let Ok(value) = std::str::from_utf8(value) else {
                continue;
            };
            if lower == "content-type" || lower == "content_type" {
                ev.datacontenttype = Some(value.to_owned());
                continue;
            }
            let Some(attr) = lower
                .strip_prefix("ce_")
                .or_else(|| lower.strip_prefix("ce-"))
            else {
                continue;
            };
            any = true;
            match attr {
                "id" => ev.id = value.to_owned(),
                "source" => ev.source = value.to_owned(),
                "type" => ev.ty = value.to_owned(),
                "specversion" => ev.specversion = value.to_owned(),
                "subject" => ev.subject = Some(value.to_owned()),
                "time" => ev.time = Some(value.to_owned()),
                "datacontenttype" => ev.datacontenttype = Some(value.to_owned()),
                other => {
                    ev.extensions.insert(other.to_owned(), value.to_owned());
                }
            }
        }
        if !any {
            return None;
        }
        ev.data = data;
        self.passthrough(ev).ok()
    }

    /// Pass-through from the structured content mode
    /// (`application/cloudevents+json`). Returns `None` when the body is not a
    /// CloudEvent.
    pub fn from_structured_json(&self, body: &[u8]) -> Option<CloudEvent> {
        let serde_json::Value::Object(mut m) = serde_json::from_slice(body).ok()? else {
            return None;
        };
        let mut take = |k: &str| match m.remove(k) {
            Some(serde_json::Value::String(s)) => Some(s),
            _ => None,
        };
        let mut ev = CloudEvent::new(take("id")?, take("source")?, take("type")?);
        ev.specversion = take("specversion")?;
        ev.subject = take("subject");
        ev.time = take("time");
        ev.datacontenttype = take("datacontenttype");
        if let Some(b64) = take("data_base64") {
            ev.data = Bytes::from(base64::engine::general_purpose::STANDARD.decode(b64).ok()?);
        } else if let Some(data) = m.remove("data") {
            ev.data = match data {
                serde_json::Value::String(s) => Bytes::from(s),
                other => Bytes::from(serde_json::to_vec(&other).ok()?),
            };
            ev.datacontenttype
                .get_or_insert_with(|| "application/json".to_owned());
        }
        for (k, v) in m {
            let v = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            ev.extensions.insert(k, v);
        }
        self.passthrough(ev).ok()
    }

    fn stamp(&self, ev: CloudEvent) -> CloudEvent {
        ev.with_extension("loamsconnector", self.connector.clone())
            .with_extension("loamsinstance", self.instance.clone())
    }
}

/// Event ids derived from source positions (design §33 §6), so a re-read of
/// the same position produces the same id and `ingest`'s `(source, id)`
/// dedup (D334) makes the source effectively once.
pub mod ids {
    use super::*;

    /// A record of a partitioned log: `"<topic>/<partition>/<offset>"`.
    pub fn log_record(topic: &str, partition: u32, offset: u64) -> String {
        format!("{topic}/{partition}/{offset}")
    }

    /// An object: `"<bucket>/<key>@<etag>"`.
    pub fn object(bucket: &str, key: &str, etag: &str) -> String {
        format!("{bucket}/{key}@{etag}")
    }

    /// A row change: the change's position plus table and key.
    pub fn change(position: &str, table: &str, key: &str) -> String {
        format!("{position}/{table}/{key}")
    }

    /// The run id of a batch read: the first 16 hex digits of
    /// SHA-256(instance ‖ query or object ‖ as-of position). A retry or a
    /// re-read of the same run at the same as-of position yields the same id;
    /// a later run (a new as-of) yields a new one.
    pub fn run_id(instance: &InstanceKey, query: &str, as_of: &str) -> String {
        let mut h = Sha256::new();
        for part in [instance.to_string().as_str(), query, as_of] {
            // Length-prefixed, so ("ab", "c") and ("a", "bc") differ.
            h.update((part.len() as u64).to_be_bytes());
            h.update(part.as_bytes());
        }
        let digest = h.finalize();
        digest[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A batch-of-rows event: `"<run-id>/<partition>/<first-row>-<last-row>"`.
    pub fn rows(run_id: &str, partition: u32, first_row: u64, last_row: u64) -> String {
        format!("{run_id}/{partition}/{first_row}-{last_row}")
    }
}
