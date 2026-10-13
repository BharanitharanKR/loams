//! The listing source: one split per prefix, read by listing with a
//! high-water key (CN1's S3 source, CN2 Task 13).
//!
//! A split's position is the last key whose events were all emitted (the
//! high-water key), plus, while a decoded object is being emitted in
//! several batches, that object's key, etag and next row. A listing resumes
//! strictly after the high-water key (S3's `start-after`, GCS's
//! `startOffset`, Azure's ordered listing), so a re-read after a crash starts
//! at the first object whose events the Fabric had not acknowledged, and the
//! ids `"<bucket>/<key>@<etag>"` (§33 §6) let `ingest` drop what it already
//! has. Keys are emitted in lexicographic order, which is the order every
//! store of the family lists in; an object written later under a key below
//! the high-water key is not seen by the listing (the event notifications of
//! [`crate::notify`] cover that case).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use loams_connector_sdk::codecs::csv::CsvCodec;
use loams_connector_sdk::codecs::ndjson::NdjsonCodec;
use loams_connector_sdk::codecs::{ARROW_STREAM, Codec};
use loams_connector_sdk::{
    Budget, CloudEvent, ConnectorError, EnvelopeBuilder, EventBatch, Position, Source, SourceBatch,
    SourceFactory, Split, SplitEnumerator, SplitState, ids,
};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::config::{Decode, SourceConfig};
use crate::store::{StoreHandle, map_err};

/// The event a listed or notified object becomes (§33 event names).
pub const OBJECT_EVENT: &str = "object";

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub size: u64,
    /// The store's etag without quotes (SFTP: `<mtime>-<size>`).
    pub etag: Option<String>,
    /// RFC 3339.
    pub last_modified: Option<String>,
}

impl Listed {
    /// The version part of the object's id: its etag, or its modification
    /// time and size when the store gives no etag.
    pub fn version(&self) -> String {
        match &self.etag {
            Some(e) => e.clone(),
            None => format!(
                "{}-{}",
                self.last_modified.as_deref().unwrap_or("0"),
                self.size
            ),
        }
    }
}

/// Lists and fetches objects: `object_store` ([`StoreLister`]) or SFTP
/// ([`crate::sftp::SftpLister`]).
#[async_trait]
pub trait Lister: Send + Sync {
    /// The bucket part of an object's id.
    fn bucket(&self) -> &str;
    /// Objects under `prefix` (by path segment) whose key is greater than
    /// `after`, in key order.
    async fn list_after(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<BoxStream<'static, Result<Listed, ConnectorError>>, ConnectorError>;
    /// An object's bytes, or `None` when it is gone since it was listed.
    async fn fetch(&self, key: &str) -> Result<Option<Bytes>, ConnectorError>;
}

/// A [`Lister`] over an `object_store` client.
#[derive(Debug)]
pub struct StoreLister {
    store: Arc<StoreHandle>,
    bucket: String,
}

impl StoreLister {
    pub fn new(store: Arc<StoreHandle>, bucket: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
        }
    }
}

fn unquote(etag: &str) -> String {
    etag.trim_start_matches("W/").trim_matches('"').to_owned()
}

#[async_trait]
impl Lister for StoreLister {
    fn bucket(&self) -> &str {
        &self.bucket
    }

    async fn list_after(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<BoxStream<'static, Result<Listed, ConnectorError>>, ConnectorError> {
        let store = self.store.get().await?;
        let prefix = (!prefix.is_empty()).then(|| Path::from(prefix));
        let stream = match after {
            Some(a) => store.list_with_offset(prefix.as_ref(), &Path::from(a)),
            None => store.list(prefix.as_ref()),
        };
        Ok(stream
            .map(|r| {
                r.map(|m| Listed {
                    key: m.location.to_string(),
                    size: m.size,
                    etag: m.e_tag.as_deref().map(unquote),
                    last_modified: Some(m.last_modified.to_rfc3339()),
                })
                .map_err(|e| map_err(e, "list"))
            })
            .boxed())
    }

    async fn fetch(&self, key: &str) -> Result<Option<Bytes>, ConnectorError> {
        let store = self.store.get().await?;
        match store.get_opts(&Path::from(key), Default::default()).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(|e| map_err(e, "get"))?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(map_err(e, "get")),
        }
    }
}

/// What a source does with each object, from the instance config.
#[derive(Debug, Clone)]
pub struct SourceSettings {
    pub decode: Decode,
    pub schema: Option<arrow_schema::SchemaRef>,
    pub csv_header: bool,
    pub poll_interval: Option<Duration>,
    pub max_object_bytes: u64,
    pub decode_batch_rows: usize,
}

impl SourceSettings {
    pub fn from_config(cfg: &SourceConfig) -> Result<Self, ConnectorError> {
        cfg.validate()?;
        let schema = match cfg.decode {
            Decode::Csv | Decode::Ndjson => Some(cfg.arrow_schema()?),
            _ => None,
        };
        Ok(Self {
            decode: cfg.decode,
            schema,
            csv_header: cfg.csv_header,
            poll_interval: cfg.poll_interval_ms.map(Duration::from_millis),
            max_object_bytes: cfg.max_object_bytes,
            decode_batch_rows: cfg.decode_batch_rows,
        })
    }

    fn decode_rows(&self, body: &[u8]) -> Result<Vec<RecordBatch>, ConnectorError> {
        let rows = self.decode_batch_rows;
        match self.decode {
            Decode::Csv => {
                let schema = self
                    .schema
                    .clone()
                    .ok_or_else(|| ConnectorError::fatal("no schema"))?;
                Ok(CsvCodec::new(schema)
                    .with_header(self.csv_header)
                    .with_batch_rows(rows)
                    .decode(body)?)
            }
            Decode::Ndjson => {
                let schema = self
                    .schema
                    .clone()
                    .ok_or_else(|| ConnectorError::fatal("no schema"))?;
                Ok(NdjsonCodec::new(schema)
                    .with_batch_rows(rows)
                    .decode(body)?)
            }
            Decode::Parquet => {
                use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
                let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(body))
                    .and_then(|b| b.with_batch_size(rows).build())
                    .map_err(|e| ConnectorError::poison(format!("parquet: {e}")))?;
                reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| ConnectorError::poison(format!("parquet: {e}")))
            }
            Decode::Metadata | Decode::Bytes => Ok(Vec::new()),
        }
    }
}

/// One split per prefix; the set is fixed by the config.
#[derive(Debug, Clone)]
pub struct ListingEnumerator {
    prefixes: Vec<String>,
}

impl ListingEnumerator {
    pub fn new(prefixes: Vec<String>) -> Self {
        Self { prefixes }
    }
}

/// The split id of a prefix (`/` for the whole bucket).
pub fn split_id(prefix: &str) -> String {
    if prefix.is_empty() {
        "/".to_owned()
    } else {
        prefix.to_owned()
    }
}

#[async_trait]
impl SplitEnumerator for ListingEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(self
            .prefixes
            .iter()
            .map(|p| Split::new(split_id(p), serde_json::json!({ "prefix": p })))
            .collect())
    }
}

/// Makes a [`ListingSource`] per split.
pub struct ListingSourceFactory {
    lister: Arc<dyn Lister>,
    settings: SourceSettings,
    envelope: EnvelopeBuilder,
}

impl std::fmt::Debug for ListingSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListingSourceFactory")
            .field("bucket", &self.lister.bucket())
            .field("settings", &self.settings)
            .finish()
    }
}

impl ListingSourceFactory {
    pub fn new(
        lister: Arc<dyn Lister>,
        settings: SourceSettings,
        envelope: EnvelopeBuilder,
    ) -> Self {
        Self {
            lister,
            settings,
            envelope,
        }
    }
}

impl SourceFactory for ListingSourceFactory {
    fn create(&self, split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        let prefix = split
            .spec
            .get("prefix")
            .and_then(|p| p.as_str())
            .ok_or_else(|| ConnectorError::fatal("split has no prefix"))?
            .to_owned();
        Ok(Box::new(ListingSource {
            lister: self.lister.clone(),
            settings: self.settings.clone(),
            envelope: self.envelope.clone(),
            prefix,
            hw: None,
            resume: None,
            partial: None,
            stream: None,
            peeked: None,
            idle_until: None,
            finished: false,
        }))
    }
}

/// The position of a listing split, as stored in `Position::opaque`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPosition {
    /// Every object up to and including this key is fully emitted.
    pub hw: Option<String>,
    /// An object partly emitted: rows before `row` are acknowledged.
    pub obj: Option<PartialPos>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialPos {
    pub key: String,
    pub version: String,
    pub row: u64,
}

impl ListPosition {
    pub fn to_position(&self) -> Position {
        let display = match (&self.obj, &self.hw) {
            (Some(o), _) => format!("{} row {}", o.key, o.row),
            (None, Some(k)) => format!("after {k}"),
            (None, None) => "start".to_owned(),
        };
        let opaque = serde_json::to_vec(self).unwrap_or_default();
        Position::new(opaque, display)
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::position_lost(format!("not a listing position: {e}")))
    }
}

struct Partial {
    listed: Listed,
    batches: VecDeque<RecordBatch>,
    next_row: u64,
}

/// Reads one prefix by listing.
pub struct ListingSource {
    lister: Arc<dyn Lister>,
    settings: SourceSettings,
    envelope: EnvelopeBuilder,
    prefix: String,
    hw: Option<String>,
    resume: Option<PartialPos>,
    partial: Option<Partial>,
    stream: Option<BoxStream<'static, Result<Listed, ConnectorError>>>,
    peeked: Option<Listed>,
    idle_until: Option<Instant>,
    finished: bool,
}

impl std::fmt::Debug for ListingSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListingSource")
            .field("prefix", &self.prefix)
            .field("hw", &self.hw)
            .finish_non_exhaustive()
    }
}

impl ListingSource {
    fn position(&self) -> Position {
        ListPosition {
            hw: self.hw.clone(),
            obj: self.partial.as_ref().map(|p| PartialPos {
                key: p.listed.key.clone(),
                version: p.listed.version(),
                row: p.next_row,
            }),
        }
        .to_position()
    }

    fn object_id(&self, l: &Listed) -> String {
        ids::object(self.lister.bucket(), &l.key, &l.version())
    }

    fn metadata_event(&self, l: &Listed) -> CloudEvent {
        let data = serde_json::json!({
            "bucket": self.lister.bucket(),
            "key": l.key,
            "size": l.size,
            "etag": l.etag,
            "last_modified": l.last_modified,
        });
        let mut ev = self
            .envelope
            .event(OBJECT_EVENT, self.lister.bucket(), self.object_id(l))
            .with_subject(l.key.clone())
            .with_data("application/json", data.to_string());
        if let Some(t) = &l.last_modified {
            ev = ev.with_time(t.clone());
        }
        ev
    }

    /// The next item of the current listing, opening one when due. `None`:
    /// nothing to read now.
    async fn next_listed(&mut self) -> Result<Option<Listed>, ConnectorError> {
        if let Some(l) = self.peeked.take() {
            return Ok(Some(l));
        }
        loop {
            if self.stream.is_none() {
                if self.finished {
                    return Ok(None);
                }
                if let Some(t) = self.idle_until {
                    if Instant::now() < t {
                        return Ok(None);
                    }
                    self.idle_until = None;
                }
                self.stream = Some(
                    self.lister
                        .list_after(&self.prefix, self.hw.as_deref())
                        .await?,
                );
            }
            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            match stream.next().await {
                Some(Ok(l)) => {
                    // Defensive: a store that lists out of order cannot move
                    // the high-water key backwards.
                    if self.hw.as_deref().is_some_and(|hw| l.key.as_str() <= hw) {
                        continue;
                    }
                    return Ok(Some(l));
                }
                Some(Err(e)) => {
                    self.stream = None;
                    return Err(e);
                }
                None => {
                    self.stream = None;
                    match self.settings.poll_interval {
                        Some(d) => self.idle_until = Some(Instant::now() + d),
                        None => self.finished = true,
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Cut the next Arrow batch of the partly emitted object to `budget`.
    fn next_chunk(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(p) = self.partial.as_mut() else {
            return Ok(None);
        };
        let Some(front) = p.batches.pop_front() else {
            let key = p.listed.key.clone();
            self.partial = None;
            self.hw = Some(key);
            return Ok(None);
        };
        let mut n = front.num_rows().min(budget.max_rows.max(1));
        let chunk = loop {
            let c = compact(&front.slice(0, n))?;
            let size = c.get_array_memory_size() + 512;
            if size <= budget.max_bytes {
                break c;
            }
            if n == 1 {
                let key = p.listed.key.clone();
                self.partial = None;
                self.hw = Some(key.clone());
                return Err(ConnectorError::poison(format!(
                    "a row of {key} is larger than the batch budget of {} bytes",
                    budget.max_bytes
                )));
            }
            n = n.div_ceil(2);
        };
        if n < front.num_rows() {
            p.batches.push_front(front.slice(n, front.num_rows() - n));
        }
        let first = p.next_row;
        let last = first + n as u64 - 1;
        p.next_row = last + 1;
        let listed = p.listed.clone();
        let done = p.batches.is_empty();
        if done {
            self.partial = None;
            self.hw = Some(listed.key.clone());
        }
        let id = format!("{}/{first}-{last}", self.object_id(&listed));
        let mut envelope = self
            .envelope
            .event(OBJECT_EVENT, self.lister.bucket(), id)
            .with_subject(listed.key.clone());
        envelope.datacontenttype = Some(ARROW_STREAM.to_owned());
        Ok(Some(SourceBatch {
            events: EventBatch::Arrow {
                batch: chunk,
                envelope,
            },
            position: self.position(),
        }))
    }

    async fn start_rows(&mut self, l: Listed) -> Result<(), ConnectorError> {
        let Some(body) = self.lister.fetch(&l.key).await? else {
            self.hw = Some(l.key);
            return Ok(());
        };
        let batches = match self.settings.decode_rows(&body) {
            Ok(b) => b,
            Err(e) => {
                self.hw = Some(l.key.clone());
                return Err(ConnectorError::poison(format!("{}: {}", l.key, e.message)));
            }
        };
        let mut batches: VecDeque<RecordBatch> =
            batches.into_iter().filter(|b| b.num_rows() > 0).collect();
        let mut next_row = 0u64;
        if let Some(r) = self.resume.take()
            && r.key == l.key
            && r.version == l.version()
        {
            // Skip the rows the Fabric already acknowledged.
            let mut skip = r.row;
            while skip > 0 {
                let Some(b) = batches.pop_front() else { break };
                let n = b.num_rows() as u64;
                if n <= skip {
                    skip -= n;
                } else {
                    batches.push_front(b.slice(skip as usize, (n - skip) as usize));
                    skip = 0;
                }
            }
            next_row = r.row;
        }
        if batches.is_empty() {
            self.hw = Some(l.key);
            return Ok(());
        }
        self.partial = Some(Partial {
            listed: l,
            batches,
            next_row,
        });
        Ok(())
    }
}

/// A slice copied into its own buffers, so its memory size is its own.
fn compact(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    arrow_select::concat::concat_batches(&batch.schema(), [batch])
        .map_err(|e| ConnectorError::fatal(format!("arrow: {e}")))
}

/// Envelope attributes and the JSON around a metadata event.
const EVENT_OVERHEAD: usize = 512;

#[async_trait]
impl Source for ListingSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        if let Some(p) = pos {
            let lp = ListPosition::from_position(&p)?;
            self.hw = lp.hw;
            self.resume = lp.obj;
        }
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.partial.is_some() {
                if let Some(b) = self.next_chunk(budget)? {
                    return Ok(Some(b));
                }
                continue;
            }
            let rows_mode = matches!(
                self.settings.decode,
                Decode::Csv | Decode::Ndjson | Decode::Parquet
            );
            let mut events: Vec<CloudEvent> = Vec::new();
            let mut bytes = 0usize;
            while events.len() < budget.max_rows {
                let Some(l) = self.next_listed().await? else {
                    break;
                };
                let carries_body = rows_mode || self.settings.decode == Decode::Bytes;
                if carries_body && l.size > self.settings.max_object_bytes {
                    if !events.is_empty() {
                        self.peeked = Some(l);
                        break;
                    }
                    self.hw = Some(l.key.clone());
                    return Err(ConnectorError::poison(format!(
                        "{} is {} bytes, over source.max_object_bytes ({})",
                        l.key, l.size, self.settings.max_object_bytes
                    )));
                }
                if rows_mode {
                    if !events.is_empty() {
                        self.peeked = Some(l);
                        break;
                    }
                    self.start_rows(l).await?;
                    if self.partial.is_some() {
                        break;
                    }
                    continue;
                }
                let estimate = EVENT_OVERHEAD
                    + if self.settings.decode == Decode::Bytes {
                        l.size as usize
                    } else {
                        0
                    };
                if bytes + estimate > budget.max_bytes {
                    if events.is_empty() {
                        self.hw = Some(l.key.clone());
                        return Err(ConnectorError::poison(format!(
                            "{} does not fit the batch budget of {} bytes",
                            l.key, budget.max_bytes
                        )));
                    }
                    self.peeked = Some(l);
                    break;
                }
                let ev = if self.settings.decode == Decode::Bytes {
                    let Some(body) = self.lister.fetch(&l.key).await? else {
                        self.hw = Some(l.key);
                        continue;
                    };
                    let mut ev = self.metadata_event(&l);
                    ev.datacontenttype = Some("application/octet-stream".to_owned());
                    ev.data = body;
                    ev
                } else {
                    self.metadata_event(&l)
                };
                bytes += ev.size_bytes();
                self.hw = Some(l.key);
                events.push(ev);
            }
            if self.partial.is_some() {
                continue;
            }
            if events.is_empty() {
                return Ok(None);
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Events(events),
                position: self.position(),
            }));
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        // A listing has nothing to commit upstream: the checkpoint is the
        // position.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.stream = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_roundtrip_and_display_no_bytes() {
        let lp = ListPosition {
            hw: Some("a/b".into()),
            obj: Some(PartialPos {
                key: "a/c".into(),
                version: "e1".into(),
                row: 10,
            }),
        };
        let p = lp.to_position();
        assert_eq!(p.display, "a/c row 10");
        assert_eq!(ListPosition::from_position(&p).expect("parses"), lp);
        assert!(ListPosition::from_position(&Position::from_u64(1)).is_err());
    }

    #[test]
    fn etags_lose_quotes() {
        assert_eq!(unquote("\"abc\""), "abc");
        assert_eq!(unquote("W/\"abc\""), "abc");
    }
}
