//! The native object sink (CN2 Task 13; replaces Iggy's `s3_sink` as the
//! default, OR-8).
//!
//! Files are rolled by size (128 MiB) or time (5 min) and named
//! deterministically, `<prefix>/<instance>/<split>/<first>-<last>.<ext>`,
//! where `<first>` and `<last>` are the sink's own row sequence numbers
//! (20 digits, so names sort in order) and `<split>` is the Fabric split the
//! sink reads (`fabric`). Each file is published with a conditional write
//! (`If-None-Match: *` on S3, R2, MinIO/RustFS and Azure; generation 0 on
//! GCS), so a replay can never overwrite or duplicate a published file.
//!
//! A file collects many Fabric batches, and the Fabric offset of a plain
//! sink is committed after every batch, so the sink is a
//! [`TransactionalSink`] (R13.2): `prepare` stages the batch as an Arrow IPC
//! part under `<…>/_staging/` **together with the checkpoint** (the Fabric
//! position and lease epoch, in the part's user metadata), and `commit`
//! rolls the staged parts into a published file when one is due, then
//! deletes them. `recover` lists the published files and the staged parts,
//! finds the newest checkpoint, and the reader seeks there; staged parts
//! already covered by a published file are deleted, and the rest wait for
//! the next roll. Data and checkpoint are written by one PUT, so a crash at
//! any point loses nothing and publishes nothing twice.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::StreamExt;
use loams_connector_sdk::codecs::Codec;
use loams_connector_sdk::codecs::ndjson::NdjsonCodec;
use loams_connector_sdk::pipeline::FABRIC_SPLIT;
use loams_connector_sdk::{
    Checkpoint, CloudEvent, ConnectorError, EventBatch, InstanceKey, Position, Sink, SinkAck,
    SinkFactory, SinkHandle, SplitId, TransactionalSink, TxnToken,
};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, ObjectStore, ObjectStoreExt, PutMode,
    PutOptions,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::adls_gen2::{DfsClient, RenameOutcome};
use crate::config::{Compression, SinkConfig, SinkFormat};
use crate::store::{StoreHandle, map_err};

/// The user-metadata key that carries a file's or part's checkpoint. Azure
/// metadata names must be C# identifiers, so no `-`.
pub const CHECKPOINT_METADATA: &str = "loamscheckpoint";
/// The directory, under a sink's base, of staged parts.
pub const STAGING: &str = "_staging";
const PART_EXT: &str = "arrows";

/// The digits of a sequence number in a name.
const SEQ_DIGITS: usize = 20;

/// The schema events are written with (Parquet; the staged parts of events).
pub fn envelope_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("specversion", DataType::Utf8, false),
        Field::new("subject", DataType::Utf8, true),
        Field::new("time", DataType::Utf8, true),
        Field::new("datacontenttype", DataType::Utf8, true),
        Field::new("data", DataType::Binary, false),
        Field::new("extensions", DataType::Utf8, true),
    ]))
}

/// Events as rows of [`envelope_schema`].
pub fn events_to_batch(events: &[CloudEvent]) -> Result<RecordBatch, ConnectorError> {
    let s = |f: fn(&CloudEvent) -> &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(events.iter().map(f)))
    };
    let o = |f: fn(&CloudEvent) -> Option<&str>| -> ArrayRef {
        Arc::new(StringArray::from(events.iter().map(f).collect::<Vec<_>>()))
    };
    let extensions: Vec<Option<String>> = events
        .iter()
        .map(|e| {
            (!e.extensions.is_empty())
                .then(|| serde_json::to_string(&e.extensions).unwrap_or_default())
        })
        .collect();
    RecordBatch::try_new(
        envelope_schema(),
        vec![
            s(|e| &e.id),
            s(|e| &e.source),
            s(|e| &e.ty),
            s(|e| &e.specversion),
            o(|e| e.subject.as_deref()),
            o(|e| e.time.as_deref()),
            o(|e| e.datacontenttype.as_deref()),
            Arc::new(BinaryArray::from_iter_values(
                events.iter().map(|e| e.data.as_ref()),
            )),
            Arc::new(StringArray::from(extensions)),
        ],
    )
    .map_err(|e| ConnectorError::fatal(format!("envelope batch: {e}")))
}

/// Rows of [`envelope_schema`] back as events.
pub fn batch_to_events(batch: &RecordBatch) -> Result<Vec<CloudEvent>, ConnectorError> {
    if batch.schema().fields() != envelope_schema().fields() {
        return Err(ConnectorError::poison("not an envelope batch"));
    }
    let str_col = |i: usize| -> Result<&StringArray, ConnectorError> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ConnectorError::poison("envelope column type"))
    };
    let opt = |a: &StringArray, r: usize| (!a.is_null(r)).then(|| a.value(r).to_owned());
    let (id, source, ty, spec) = (str_col(0)?, str_col(1)?, str_col(2)?, str_col(3)?);
    let (subject, time, ct, ext) = (str_col(4)?, str_col(5)?, str_col(6)?, str_col(8)?);
    let data = batch
        .column(7)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ConnectorError::poison("envelope data column type"))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut ev = CloudEvent::new(id.value(r), source.value(r), ty.value(r));
        ev.specversion = spec.value(r).to_owned();
        ev.subject = opt(subject, r);
        ev.time = opt(time, r);
        ev.datacontenttype = opt(ct, r);
        ev.data = Bytes::copy_from_slice(data.value(r));
        if let Some(x) = opt(ext, r) {
            ev.extensions = serde_json::from_str(&x).unwrap_or_default();
        }
        out.push(ev);
    }
    Ok(out)
}

/// A structured-mode CloudEvent (JSON), as the NDJSON format writes it.
pub fn structured_json(ev: &CloudEvent) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert("specversion".into(), ev.specversion.clone().into());
    m.insert("id".into(), ev.id.clone().into());
    m.insert("source".into(), ev.source.clone().into());
    m.insert("type".into(), ev.ty.clone().into());
    for (k, v) in [
        ("subject", &ev.subject),
        ("time", &ev.time),
        ("datacontenttype", &ev.datacontenttype),
    ] {
        if let Some(v) = v {
            m.insert(k.into(), v.clone().into());
        }
    }
    for (k, v) in &ev.extensions {
        m.entry(k.clone()).or_insert_with(|| v.clone().into());
    }
    let json_data = ev
        .datacontenttype
        .as_deref()
        .is_none_or(|c| c.contains("json"));
    match json_data
        .then(|| serde_json::from_slice::<serde_json::Value>(&ev.data).ok())
        .flatten()
    {
        Some(v) => {
            m.insert("data".into(), v);
        }
        None if ev.data.is_empty() => {}
        None => {
            m.insert(
                "data_base64".into(),
                base64::engine::general_purpose::STANDARD
                    .encode(&ev.data)
                    .into(),
            );
        }
    }
    serde_json::Value::Object(m)
}

/// Encode the rows of one file in the sink's format.
pub fn encode_file(
    format: SinkFormat,
    compression: Compression,
    separator: &str,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, ConnectorError> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let schema = first.schema();
    let is_envelope = schema.fields() == envelope_schema().fields();
    match format {
        SinkFormat::Parquet => {
            use parquet::arrow::ArrowWriter;
            use parquet::basic::{Compression as C, GzipLevel, ZstdLevel};
            use parquet::file::properties::WriterProperties;
            let c = match compression {
                Compression::Zstd => C::ZSTD(ZstdLevel::default()),
                Compression::Snappy => C::SNAPPY,
                Compression::Gzip => C::GZIP(GzipLevel::default()),
                Compression::None => C::UNCOMPRESSED,
            };
            let props = WriterProperties::builder().set_compression(c).build();
            let mut buf = Vec::new();
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))
                .map_err(|e| ConnectorError::schema_incompatible(format!("parquet: {e}")))?;
            for b in batches {
                w.write(b)
                    .map_err(|e| ConnectorError::schema_incompatible(format!("parquet: {e}")))?;
            }
            w.close()
                .map_err(|e| ConnectorError::fatal(format!("parquet: {e}")))?;
            Ok(buf)
        }
        SinkFormat::Ndjson if is_envelope => {
            let mut buf = Vec::new();
            for b in batches {
                for ev in batch_to_events(b)? {
                    serde_json::to_writer(&mut buf, &structured_json(&ev))
                        .map_err(|e| ConnectorError::fatal(format!("ndjson: {e}")))?;
                    buf.push(b'\n');
                }
            }
            Ok(buf)
        }
        SinkFormat::Ndjson => Ok(NdjsonCodec::new(schema).encode(batches)?),
        SinkFormat::Raw => {
            if !is_envelope {
                return Err(ConnectorError::poison(
                    "the raw format writes events' data; an Arrow batch has none",
                ));
            }
            let mut buf = Vec::new();
            for b in batches {
                for ev in batch_to_events(b)? {
                    buf.extend_from_slice(&ev.data);
                    buf.extend_from_slice(separator.as_bytes());
                }
            }
            Ok(buf)
        }
    }
}

fn ipc_encode(batches: &[RecordBatch]) -> Result<Vec<u8>, ConnectorError> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| ConnectorError::fatal("nothing to stage"))?;
    let mut buf = Vec::new();
    let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| ConnectorError::fatal(format!("arrow ipc: {e}")))?;
    for b in batches {
        w.write(b)
            .map_err(|e| ConnectorError::fatal(format!("arrow ipc: {e}")))?;
    }
    w.finish()
        .map_err(|e| ConnectorError::fatal(format!("arrow ipc: {e}")))?;
    drop(w);
    Ok(buf)
}

fn ipc_decode(bytes: &[u8]) -> Result<Vec<RecordBatch>, ConnectorError> {
    let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| ConnectorError::fatal(format!("staged part: {e}")))?;
    r.collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConnectorError::fatal(format!("staged part: {e}")))
}

/// The checkpoint a part or file carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCheckpoint {
    pub epoch: u64,
    /// The Fabric position, base64 of its opaque bytes.
    pub position: String,
    pub display: String,
    /// The sequence number of the next row after this part or file.
    pub next_seq: u64,
}

impl StoredCheckpoint {
    fn encode(&self) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(self).unwrap_or_default())
    }

    fn decode(s: &str) -> Option<Self> {
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s.trim())
            .ok()?;
        serde_json::from_slice(&raw).ok()
    }

    fn checkpoint(&self, instance: &InstanceKey) -> Result<Checkpoint, ConnectorError> {
        let opaque = base64::engine::general_purpose::STANDARD
            .decode(&self.position)
            .map_err(|_| ConnectorError::position_lost("stored checkpoint is not base64"))?;
        let mut cp = Checkpoint::new(instance.clone(), self.epoch);
        cp.splits.insert(
            SplitId::new(FABRIC_SPLIT),
            Position::new(opaque, self.display.clone()),
        );
        Ok(cp)
    }
}

/// `<first>-<last>` of a name, if it is one of the sink's.
pub fn parse_range(file_name: &str) -> Option<(u64, u64)> {
    let stem = file_name.split('.').next()?;
    let (a, b) = stem.split_once('-')?;
    if a.len() != SEQ_DIGITS || b.len() != SEQ_DIGITS {
        return None;
    }
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn range_name(first: u64, last: u64, ext: &str) -> String {
    format!("{first:0w$}-{last:0w$}.{ext}", w = SEQ_DIGITS)
}

/// How a rolled file is made visible.
#[derive(Debug, Clone)]
pub enum Publish {
    /// One conditional PUT of the file (S3, R2, MinIO/RustFS, GCS, Azure).
    ConditionalPut,
    /// ADLS Gen2 with a hierarchical namespace: PUT to a temporary name, then
    /// an atomic DFS rename that refuses an existing destination (R0.21).
    DfsRename(Arc<DfsClient>),
}

/// The sink's settings, from the instance config.
#[derive(Debug, Clone)]
pub struct SinkSettings {
    pub config: SinkConfig,
    /// `<prefix>/<instance>/<split>`.
    pub base: String,
    pub publish: Publish,
}

impl SinkSettings {
    pub fn new(cfg: &SinkConfig, prefix: &str, instance: &InstanceKey, publish: Publish) -> Self {
        let mut parts: Vec<&str> = Vec::new();
        let prefix = prefix.trim_matches('/');
        if !prefix.is_empty() {
            parts.push(prefix);
        }
        parts.push(&instance.name);
        parts.push(FABRIC_SPLIT);
        Self {
            config: cfg.clone(),
            base: parts.join("/"),
            publish,
        }
    }
}

#[derive(Debug, Clone)]
struct Part {
    path: Path,
    first: u64,
    last: u64,
    bytes: u64,
    cp: Option<StoredCheckpoint>,
}

/// Makes the instance's [`ObjectSink`].
#[derive(Debug)]
pub struct ObjectSinkFactory {
    store: Arc<StoreHandle>,
    settings: SinkSettings,
    instance: InstanceKey,
}

impl ObjectSinkFactory {
    pub fn new(store: Arc<StoreHandle>, settings: SinkSettings, instance: InstanceKey) -> Self {
        Self {
            store,
            settings,
            instance,
        }
    }
}

impl SinkFactory for ObjectSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Transactional(Box::new(ObjectSink::new(
            self.store.clone(),
            self.settings.clone(),
            self.instance.clone(),
        ))))
    }
}

/// The rolling, staged object sink.
pub struct ObjectSink {
    store: Arc<StoreHandle>,
    settings: SinkSettings,
    instance: InstanceKey,
    recovered: bool,
    next_seq: u64,
    parts: Vec<Part>,
    window_started: Option<Instant>,
    window_schema: Option<SchemaRef>,
    pending: Vec<RecordBatch>,
}

impl std::fmt::Debug for ObjectSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectSink")
            .field("base", &self.settings.base)
            .field("next_seq", &self.next_seq)
            .field("parts", &self.parts.len())
            .finish_non_exhaustive()
    }
}

impl ObjectSink {
    pub fn new(store: Arc<StoreHandle>, settings: SinkSettings, instance: InstanceKey) -> Self {
        Self {
            store,
            settings,
            instance,
            recovered: false,
            next_seq: 0,
            parts: Vec::new(),
            window_started: None,
            window_schema: None,
            pending: Vec::new(),
        }
    }

    fn staging_dir(&self) -> Path {
        Path::from(format!("{}/{STAGING}", self.settings.base))
    }

    fn file_path(&self, first: u64, last: u64) -> Path {
        Path::from(format!(
            "{}/{}",
            self.settings.base,
            range_name(first, last, self.settings.config.format.extension())
        ))
    }

    fn put_mode(&self) -> PutMode {
        if self.settings.config.conditional_put {
            PutMode::Create
        } else {
            PutMode::Overwrite
        }
    }

    async fn stored_checkpoint(
        store: &dyn ObjectStore,
        path: &Path,
    ) -> Result<Option<StoredCheckpoint>, ConnectorError> {
        let opts = GetOptions {
            head: true,
            ..Default::default()
        };
        match store.get_opts(path, opts).await {
            Ok(r) => Ok(r
                .attributes
                .get(&Attribute::Metadata(CHECKPOINT_METADATA.into()))
                .and_then(|v| StoredCheckpoint::decode(v.as_ref()))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(map_err(e, "read checkpoint")),
        }
    }

    fn attributes(cp: &StoredCheckpoint, content_type: &str) -> Attributes {
        let mut a = Attributes::new();
        a.insert(
            Attribute::Metadata(CHECKPOINT_METADATA.into()),
            AttributeValue::from(cp.encode()),
        );
        a.insert(
            Attribute::ContentType,
            AttributeValue::from(content_type.to_owned()),
        );
        a
    }

    /// Write `body` at `path` with `cp`. An existing object with the same
    /// checkpoint is this write, replayed; any other is refused.
    async fn put_once(
        &self,
        store: &dyn ObjectStore,
        path: &Path,
        body: Vec<u8>,
        cp: &StoredCheckpoint,
        content_type: &str,
    ) -> Result<(), ConnectorError> {
        let opts = PutOptions {
            mode: self.put_mode(),
            attributes: Self::attributes(cp, content_type),
            ..Default::default()
        };
        match store.put_opts(path, Bytes::from(body).into(), opts).await {
            Ok(_) => Ok(()),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => self.check_existing(store, path, cp).await,
            Err(e) => Err(map_err(e, "put")),
        }
    }

    async fn check_existing(
        &self,
        store: &dyn ObjectStore,
        path: &Path,
        cp: &StoredCheckpoint,
    ) -> Result<(), ConnectorError> {
        match Self::stored_checkpoint(store, path).await? {
            Some(existing) if &existing == cp => Ok(()),
            _ => Err(ConnectorError::fatal(format!(
                "{path} already exists with other content: another writer shares this prefix, or the sink's state was lost"
            ))),
        }
    }

    async fn ensure_recovered(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        if self.recovered {
            return Ok(None);
        }
        let cp = self.do_recover().await?;
        self.recovered = true;
        Ok(cp)
    }

    async fn do_recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        let store = self.store.get().await?;
        let base = Path::from(self.settings.base.as_str());
        // Published files sit directly under the base.
        let listing = store
            .list_with_delimiter(Some(&base))
            .await
            .map_err(|e| map_err(e, "list"))?;
        // The newest published file that carries a checkpoint; a file at
        // one of the sink's names without one is not the sink's, and the
        // conditional publish refuses to overwrite it when its turn comes.
        let mut files: Vec<(u64, Path)> = listing
            .objects
            .iter()
            .filter_map(|m| {
                let (_, last) = parse_range(m.location.filename()?)?;
                Some((last, m.location.clone()))
            })
            .collect();
        files.sort_by_key(|f| std::cmp::Reverse(f.0));
        let mut newest_file: Option<(u64, Path, StoredCheckpoint)> = None;
        for (last, path) in files {
            if let Some(cp) = Self::stored_checkpoint(&*store, &path).await? {
                newest_file = Some((last, path, cp));
                break;
            }
            tracing::warn!(%path, "a file without a Loams checkpoint sits among the sink's files");
        }
        // Staged parts, and the temporary files of a DFS-rename publish.
        let mut parts = Vec::new();
        let mut stale = Vec::new();
        let mut staged = store.list(Some(&self.staging_dir()));
        while let Some(m) = staged.next().await {
            let m = m.map_err(|e| map_err(e, "list"))?;
            let Some(name) = m.location.filename() else {
                continue;
            };
            let Some((first, last)) = parse_range(name) else {
                continue;
            };
            if !name.ends_with(&format!(".{PART_EXT}")) {
                stale.push(m.location.clone());
                continue;
            }
            if newest_file.as_ref().is_some_and(|(l, _, _)| last <= *l) {
                stale.push(m.location.clone());
                continue;
            }
            parts.push(Part {
                path: m.location.clone(),
                first,
                last,
                bytes: m.size,
                cp: None,
            });
        }
        for p in stale {
            match store.delete(&p).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(map_err(e, "delete")),
            }
        }
        parts.sort_by_key(|p| p.first);
        let stored = match parts.last() {
            Some(p) => Some(
                Self::stored_checkpoint(&*store, &p.path)
                    .await?
                    .ok_or_else(|| {
                        ConnectorError::position_lost(format!("{} carries no checkpoint", p.path))
                    })?,
            ),
            None => newest_file.map(|(_, _, cp)| cp),
        };
        if let (Some(last), Some(s)) = (parts.last_mut(), &stored) {
            last.cp = Some(s.clone());
        }
        self.next_seq = stored.as_ref().map_or(0, |s| s.next_seq);
        if !parts.is_empty() {
            self.window_started = Some(Instant::now());
            // The window's schema is read back at the roll.
            self.window_schema = None;
        }
        self.parts = parts;
        stored
            .as_ref()
            .map(|s| s.checkpoint(&self.instance))
            .transpose()
    }

    fn roll_due(&self) -> bool {
        let bytes: u64 = self.parts.iter().map(|p| p.bytes).sum();
        bytes >= self.settings.config.roll_bytes
            || self
                .window_started
                .is_some_and(|t| t.elapsed() >= self.settings.config.roll_interval())
    }

    /// Publish the staged parts as one file, then delete them.
    async fn roll(&mut self) -> Result<(), ConnectorError> {
        let (Some(first), Some(last)) = (self.parts.first(), self.parts.last()) else {
            return Ok(());
        };
        let (first, last_seq) = (first.first, last.last);
        let store = self.store.get().await?;
        let cp = match &last.cp {
            Some(cp) => cp.clone(),
            None => Self::stored_checkpoint(&*store, &last.path)
                .await?
                .ok_or_else(|| {
                    ConnectorError::position_lost("a staged part lost its checkpoint")
                })?,
        };
        let mut batches = Vec::new();
        for p in &self.parts {
            let r = match store.get(&p.path).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(ConnectorError::fatal(format!(
                        "staged part {} vanished: another writer shares this prefix",
                        p.path
                    )));
                }
                Err(e) => return Err(map_err(e, "read staged part")),
            };
            let body = r
                .bytes()
                .await
                .map_err(|e| map_err(e, "read staged part"))?;
            batches.extend(ipc_decode(&body)?);
        }
        let c = &self.settings.config;
        let body = encode_file(c.format, c.compression, &c.raw_separator, &batches)?;
        let target = self.file_path(first, last_seq);
        let content_type = match c.format {
            SinkFormat::Parquet => "application/vnd.apache.parquet",
            SinkFormat::Ndjson => "application/x-ndjson",
            SinkFormat::Raw => "application/octet-stream",
        };
        match &self.settings.publish {
            Publish::ConditionalPut => {
                self.put_once(&*store, &target, body, &cp, content_type)
                    .await?;
            }
            Publish::DfsRename(dfs) => {
                let tmp = Path::from(format!(
                    "{}/{}.tmp",
                    self.staging_dir(),
                    range_name(first, last_seq, c.format.extension())
                ));
                let opts = PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: Self::attributes(&cp, content_type),
                    ..Default::default()
                };
                store
                    .put_opts(&tmp, Bytes::from(body).into(), opts)
                    .await
                    .map_err(|e| map_err(e, "put"))?;
                match dfs.rename(tmp.as_ref(), target.as_ref()).await? {
                    RenameOutcome::Renamed => {}
                    RenameOutcome::DestinationExists => {
                        self.check_existing(&*store, &target, &cp).await?;
                        let _ = store.delete(&tmp).await;
                    }
                    RenameOutcome::SourceMissing => {
                        // An earlier attempt renamed it already.
                        self.check_existing(&*store, &target, &cp).await?;
                    }
                }
            }
        }
        for p in std::mem::take(&mut self.parts) {
            match store.delete(&p.path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(map_err(e, "delete staged part")),
            }
        }
        self.window_started = None;
        self.window_schema = None;
        Ok(())
    }

    /// Published files under the sink's base, in order (for tests and
    /// operators).
    pub async fn published(&self) -> Result<Vec<Path>, ConnectorError> {
        let store = self.store.get().await?;
        let base = Path::from(self.settings.base.as_str());
        let l = store
            .list_with_delimiter(Some(&base))
            .await
            .map_err(|e| map_err(e, "list"))?;
        let mut out: Vec<Path> = l
            .objects
            .into_iter()
            .map(|m| m.location)
            .filter(|p| p.filename().and_then(parse_range).is_some())
            .collect();
        out.sort();
        Ok(out)
    }
}

#[async_trait]
impl Sink for ObjectSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let bytes = batch.size_bytes() as u64;
        let rb = match batch {
            EventBatch::Events(events) => {
                if events.is_empty() {
                    return Ok(SinkAck::default());
                }
                events_to_batch(&events)?
            }
            EventBatch::Arrow { batch, .. } => {
                if self.settings.config.format == SinkFormat::Raw {
                    return Err(ConnectorError::poison(
                        "the raw format writes events' data; an Arrow batch has none",
                    ));
                }
                batch
            }
        };
        if let Some(p) = self.pending.first()
            && p.schema().fields() != rb.schema().fields()
        {
            return Err(ConnectorError::poison(
                "a batch with another schema arrived inside one transaction",
            ));
        }
        let rows = rb.num_rows() as u64;
        if rows > 0 {
            self.pending.push(rb);
        }
        Ok(SinkAck { rows, bytes })
    }

    /// Data becomes durable at `prepare`; the pipeline drives a
    /// transactional sink through `prepare` and `commit`, never `flush`.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for ObjectSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        self.recovered = false;
        self.pending.clear();
        self.ensure_recovered().await
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        self.ensure_recovered().await?;
        if self.pending.is_empty() {
            return Ok(TxnToken {
                id: String::new(),
                opaque: Bytes::new(),
            });
        }
        let schema = self.pending[0].schema();
        if self
            .window_schema
            .as_ref()
            .is_some_and(|w| w.fields() != schema.fields())
        {
            // A file has one schema: publish what is staged before the
            // first batch of the new one.
            self.roll().await?;
        }
        let rows: u64 = self.pending.iter().map(|b| b.num_rows() as u64).sum();
        let (first, last) = (self.next_seq, self.next_seq + rows - 1);
        let position = cp
            .splits
            .get(&SplitId::new(FABRIC_SPLIT))
            .or_else(|| cp.splits.values().next())
            .ok_or_else(|| ConnectorError::fatal("checkpoint has no Fabric position"))?;
        let stored = StoredCheckpoint {
            epoch: cp.epoch,
            position: base64::engine::general_purpose::STANDARD.encode(&position.opaque),
            display: position.display.clone(),
            next_seq: last + 1,
        };
        let body = ipc_encode(&self.pending)?;
        let size = body.len() as u64;
        let path = Path::from(format!(
            "{}/{}",
            self.staging_dir(),
            range_name(first, last, PART_EXT)
        ));
        let store = self.store.get().await?;
        self.put_once(
            &*store,
            &path,
            body,
            &stored,
            "application/vnd.apache.arrow.stream",
        )
        .await?;
        self.pending.clear();
        self.next_seq = last + 1;
        self.window_started.get_or_insert_with(Instant::now);
        if let Some(prev) = self.parts.last_mut() {
            // Only the newest part's checkpoint is needed at the roll.
            prev.cp = None;
        }
        self.parts.push(Part {
            path: path.clone(),
            first,
            last,
            bytes: size,
            cp: Some(stored),
        });
        self.window_schema = Some(schema);
        Ok(TxnToken {
            id: path.to_string(),
            opaque: Bytes::new(),
        })
    }

    /// The staged part is already durable with its checkpoint; commit rolls
    /// a file when one is due. Idempotent: a retried roll finds its file
    /// published with the same checkpoint.
    async fn commit(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        if self.roll_due() {
            self.roll().await?;
        }
        Ok(())
    }

    async fn abort(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        // Nothing staged is undone: a part carries data and checkpoint
        // together, and `recover` resumes after it.
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_sort_by_sequence_and_parse_back() {
        let a = range_name(9, 10, "parquet");
        let b = range_name(11, 200, "parquet");
        assert!(a < b);
        assert_eq!(parse_range(&a), Some((9, 10)));
        assert_eq!(
            parse_range("00000000000000000011-00000000000000000200.arrows.tmp"),
            Some((11, 200))
        );
        assert_eq!(parse_range("notes.txt"), None);
    }

    #[test]
    fn events_survive_the_envelope_schema() {
        let ev = CloudEvent::new("e/1", "/s", "t")
            .with_subject("k")
            .with_data("application/json", "{\"a\":1}")
            .with_extension("loamsinstance", "i");
        let b = events_to_batch(std::slice::from_ref(&ev)).expect("batch");
        assert_eq!(batch_to_events(&b).expect("events"), vec![ev.clone()]);
        let j = structured_json(&ev);
        assert_eq!(j["data"]["a"], 1);
        assert_eq!(j["loamsinstance"], "i");
        let bin =
            CloudEvent::new("e/2", "/s", "t").with_data("application/octet-stream", vec![0u8, 255]);
        assert_eq!(structured_json(&bin)["data_base64"], "AP8=");
    }

    #[test]
    fn stored_checkpoints_roundtrip() {
        let s = StoredCheckpoint {
            epoch: 3,
            position: base64::engine::general_purpose::STANDARD.encode(7u64.to_be_bytes()),
            display: "7".into(),
            next_seq: 12,
        };
        let back = StoredCheckpoint::decode(&s.encode()).expect("decodes");
        assert_eq!(back, s);
        let cp = back.checkpoint(&InstanceKey::new(1, "i")).expect("cp");
        assert_eq!(cp.splits[&SplitId::new(FABRIC_SPLIT)].as_u64(), Some(7));
    }

    #[test]
    fn raw_refuses_arrow_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))])
            .expect("batch");
        assert!(encode_file(SinkFormat::Raw, Compression::None, "\n", &[b]).is_err());
    }
}
