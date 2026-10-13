//! Delta Lake in both directions (OR-18), on `deltalake` 1.1.0 (CN2 R0.14).
//!
//! **Sink** ([`DeltaSink`]): a [`TransactionalSink`]. `write` stages Arrow
//! batches in memory; `prepare` writes them as Parquet data files (not yet
//! visible: no log entry names them); `commit` makes one Delta commit that
//! adds the files **and** carries Delta's `txn` action (`appId` = the
//! instance, `version` = the sink's commit sequence) with the Fabric
//! checkpoint in the commit's `commitInfo` (`loams.checkpoint`). Data and
//! checkpoint land atomically, so `exactly_once` (OR-7). `recover` reads
//! `transaction_version(appId)` and the checkpoint of that commit; a crash
//! after `prepare` leaves only unreferenced files, which `VACUUM` removes.
//!
//! **Source** ([`DeltaSource`]): one split per table; the position is a table
//! version plus the file and row within it. Without a position it starts
//! from the latest snapshot (or version 0 with `source.start: earliest`) and
//! then follows the log version by version. With `source.mode: cdf` it reads
//! the change data feed by the Delta protocol's rules: a version's `cdc`
//! files when it has any, else its `add` files as `insert` and its
//! data-changing `remove` files as `delete`, with `_change_type`,
//! `_commit_version` and `_commit_timestamp` columns. The log is read
//! directly (R17.6), so deltalake's `datafusion` feature is never built.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use deltalake_core::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, StructType, Transaction};
use deltalake_core::logstore::{LogStore, LogStoreFactory, StorageConfig, logstore_factories};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures::TryStreamExt;
use loams_connector_sdk::{
    Budget, Checkpoint, CloudEvent, ConnectorContext, ConnectorError, ConnectorFactory,
    EgressCheck, EnvelopeBuilder, EventBatch, InstanceKey, Position, SecretResolver, Sink, SinkAck,
    SinkFactory, SinkHandle, Source, SourceBatch, SourceFactory, Split, SplitEnumerator, SplitId,
    SplitState, TransactionalSink, TxnToken, ids,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Deserialize;
use url::Url;

use crate::rows::to_record_batch;
use crate::store::{StorageConfig as Storage, map_err as store_err, root_store};

/// The manifest id (`connectors/registry/delta-lake.yaml`).
pub const CONNECTOR: &str = "delta-lake";

/// The one split of a Delta source: the table.
pub const TABLE_SPLIT: &str = "table";

/// `commitInfo` keys the sink writes beside Delta's `txn` action.
pub const META_APP: &str = "loams.app_id";
pub const META_TXN: &str = "loams.txn";
pub const META_CHECKPOINT: &str = "loams.checkpoint";

const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

// ---------------------------------------------------------------------------
// Configuration

/// The instance config of a `delta-lake` instance.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaConfig {
    /// `s3://bucket/path`, `file:///path` or `memory:///path`.
    pub table_uri: String,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub sink: DeltaSinkConfig,
    #[serde(default)]
    pub source: DeltaSourceConfig,
    /// The runtime's `pipeline.*` overrides; read by the pipeline, not here.
    #[serde(default)]
    pub pipeline: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaSinkConfig {
    /// `append` (the only mode built, R17.6).
    #[serde(default)]
    pub mode: SinkMode,
    /// Partition columns of a table the sink creates.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Create the table from the first batch's schema when it does not exist.
    #[serde(default = "yes")]
    pub create: bool,
    /// Roll data files at this size (bytes); one file per partition per
    /// commit when unset.
    #[serde(default)]
    pub target_file_size: Option<u64>,
}

impl Default for DeltaSinkConfig {
    fn default() -> Self {
        Self {
            mode: SinkMode::Append,
            partition_by: Vec::new(),
            create: true,
            target_file_size: None,
        }
    }
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    #[default]
    Append,
    /// Refused when the instance starts: Delta `MERGE` lives behind
    /// deltalake's `datafusion` feature (R17.6).
    Merge,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaSourceConfig {
    #[serde(default)]
    pub mode: SourceMode,
    #[serde(default)]
    pub start: Start,
    /// How often a caught-up source looks for a new version.
    #[serde(default = "default_poll")]
    pub poll_interval_ms: u64,
    /// A data file larger than this stops the instance (it is read whole).
    #[serde(default = "default_max_file")]
    pub max_file_bytes: u64,
}

impl Default for DeltaSourceConfig {
    fn default() -> Self {
        Self {
            mode: SourceMode::Versions,
            start: Start::Snapshot,
            poll_interval_ms: default_poll(),
            max_file_bytes: default_max_file(),
        }
    }
}

fn default_poll() -> u64 {
    5_000
}

fn default_max_file() -> u64 {
    512 << 20
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMode {
    /// Rows added by each version (`rows` events).
    #[default]
    Versions,
    /// The change data feed (`change` events with `_change_type`).
    Cdf,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Start {
    /// The latest snapshot, then every later version.
    #[default]
    Snapshot,
    /// Every version from 0 (fails if the log has been cleaned up).
    Earliest,
}

impl DeltaConfig {
    pub fn parse(config: &serde_json::Value) -> Result<Self, ConnectorError> {
        let cfg: DeltaConfig = serde_json::from_value(config.clone())
            .map_err(|e| ConnectorError::fatal(format!("delta-lake config: {e}")))?;
        cfg.url()?;
        if cfg.sink.mode == SinkMode::Merge {
            return Err(ConnectorError::fatal(
                "delta-lake sink.mode merge is not built: Delta MERGE needs deltalake's \
                 datafusion feature (CN2 R17.6); use append",
            ));
        }
        Ok(cfg)
    }

    pub fn url(&self) -> Result<Url, ConnectorError> {
        Url::parse(&self.table_uri)
            .map_err(|e| ConnectorError::fatal(format!("table_uri is not a URL: {e}")))
    }
}

// ---------------------------------------------------------------------------
// The table

/// A table location and the store that reaches it.
#[derive(Clone)]
pub struct DeltaTarget {
    url: Url,
    root: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for DeltaTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTarget")
            .field("url", &self.url.as_str())
            .finish_non_exhaustive()
    }
}

impl DeltaTarget {
    /// A table at `url` reached through `root`, the store whose `/` is the
    /// bucket's root (S3) or the file system's (local).
    pub fn new(url: Url, root: Arc<dyn ObjectStore>) -> Self {
        register_s3_logstore();
        Self { url, root }
    }

    /// Build the store an instance's config names (egress-checked, R13.3).
    pub async fn from_config(
        cfg: &DeltaConfig,
        secrets: &dyn SecretResolver,
        egress: Arc<dyn EgressCheck>,
    ) -> Result<Self, ConnectorError> {
        let url = cfg.url()?;
        let root = root_store(&url, &cfg.storage, secrets, egress).await?;
        Ok(Self::new(url, root))
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    fn builder(&self) -> Result<DeltaTableBuilder, ConnectorError> {
        Ok(DeltaTableBuilder::from_url(self.url.clone())
            .map_err(|e| delta_err(e, "table location"))?
            .with_storage_backend(self.root.clone(), self.url.clone()))
    }

    /// The table, or `None` when nothing is at the location yet.
    pub async fn load(&self) -> Result<Option<DeltaTable>, ConnectorError> {
        let mut table = self
            .builder()?
            .build()
            .map_err(|e| delta_err(e, "open table"))?;
        match table.load().await {
            Ok(()) => Ok(Some(table)),
            Err(DeltaTableError::NotATable(_)) => Ok(None),
            Err(e) => Err(delta_err(e, "load table")),
        }
    }

    /// An unloaded handle (for `create`).
    fn handle(&self) -> Result<DeltaTable, ConnectorError> {
        self.builder()?
            .build()
            .map_err(|e| delta_err(e, "open table"))
    }
}

/// deltalake 1.1.0 registers log stores for `file://` and `memory://` only
/// (S3 support is the separate `deltalake-aws` crate, which brings the AWS
/// SDK). The default log store commits with `PutMode::Create`, which
/// `object_store` 0.13.2 sends to S3 as `If-None-Match: *` (R0.13), so it is
/// registered for `s3://` too unless something else already is (R17.5).
fn register_s3_logstore() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let registry = logstore_factories();
        for scheme in ["s3://", "s3a://"] {
            if let Ok(url) = Url::parse(scheme)
                && !registry.contains_key(&url)
            {
                registry.insert(url, Arc::new(ConditionalPutLogStore));
            }
        }
    });
}

#[derive(Debug)]
struct ConditionalPutLogStore;

impl LogStoreFactory for ConditionalPutLogStore {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> deltalake_core::DeltaResult<Arc<dyn LogStore>> {
        Ok(deltalake_core::logstore::default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// A `DeltaTableError` as a connector error, by class.
pub fn delta_err(e: DeltaTableError, what: &str) -> ConnectorError {
    use DeltaTableError as E;
    let class_msg = format!("{what}: {}", crate::store::redact_urls(&e.to_string()));
    match &e {
        E::ObjectStore { source } => store_err(source, what),
        E::SchemaMismatch { .. } => ConnectorError::schema_incompatible(class_msg),
        E::Transaction { .. } | E::VersionAlreadyExists(_) | E::Io { .. } => {
            ConnectorError::retryable(class_msg)
        }
        E::Arrow { .. } | E::InvalidData { .. } => ConnectorError::poison(class_msg),
        _ => ConnectorError::fatal(class_msg),
    }
}

/// Delta's `appId` for an instance.
pub fn app_id(instance: &InstanceKey) -> String {
    format!("loams/{}/{}", instance.namespace, instance.name)
}

// ---------------------------------------------------------------------------
// Checkpoint encoding (commitInfo `loams.checkpoint`)

fn encode_checkpoint(cp: &Checkpoint) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD;
    let splits: serde_json::Map<String, serde_json::Value> = cp
        .splits
        .iter()
        .map(|(id, p)| {
            (
                id.0.clone(),
                serde_json::json!({"opaque": b64.encode(&p.opaque), "display": p.display}),
            )
        })
        .collect();
    serde_json::json!({"epoch": cp.epoch, "splits": splits})
}

fn decode_checkpoint(
    instance: &InstanceKey,
    v: &serde_json::Value,
) -> Result<Checkpoint, ConnectorError> {
    let bad = || ConnectorError::fatal("the table's loams.checkpoint is malformed");
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut cp = Checkpoint::new(
        instance.clone(),
        v.get("epoch")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(bad)?,
    );
    for (id, p) in v
        .get("splits")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(bad)?
    {
        let opaque = b64
            .decode(p.get("opaque").and_then(|o| o.as_str()).ok_or_else(bad)?)
            .map_err(|_| bad())?;
        let display = p.get("display").and_then(|d| d.as_str()).unwrap_or("");
        cp.splits
            .insert(SplitId::new(id.clone()), Position::new(opaque, display));
    }
    Ok(cp)
}

// ---------------------------------------------------------------------------
// Sink

/// The config, secrets and egress a target is built from on first use (a
/// factory is synchronous; building the store is not).
type Pending = (DeltaConfig, Arc<dyn SecretResolver>, Arc<dyn EgressCheck>);

/// What `prepare` staged, carried in a [`TxnToken`]'s `opaque`.
#[derive(serde::Serialize, Deserialize)]
struct Staged {
    txn: i64,
    checkpoint: serde_json::Value,
    adds: Vec<Add>,
}

/// The exactly-once Delta sink.
pub struct DeltaSink {
    target: Option<DeltaTarget>,
    pending: Option<Pending>,
    cfg: DeltaSinkConfig,
    instance: InstanceKey,
    app: String,
    table: Option<DeltaTable>,
    schema: Option<SchemaRef>,
    staged: Vec<RecordBatch>,
    next_txn: Option<i64>,
}

impl std::fmt::Debug for DeltaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaSink")
            .field("app", &self.app)
            .field("staged", &self.staged.len())
            .field("next_txn", &self.next_txn)
            .finish_non_exhaustive()
    }
}

impl DeltaSink {
    /// A sink on an already-built target (tests, and callers that share a
    /// store).
    pub fn new(target: DeltaTarget, cfg: DeltaSinkConfig, instance: InstanceKey) -> Self {
        Self {
            target: Some(target),
            pending: None,
            cfg,
            app: app_id(&instance),
            instance,
            table: None,
            schema: None,
            staged: Vec::new(),
            next_txn: None,
        }
    }

    fn from_config(
        cfg: DeltaConfig,
        secrets: Arc<dyn SecretResolver>,
        egress: Arc<dyn EgressCheck>,
        instance: InstanceKey,
    ) -> Self {
        let sink_cfg = cfg.sink.clone();
        Self {
            target: None,
            pending: Some((cfg, secrets, egress)),
            cfg: sink_cfg,
            app: app_id(&instance),
            instance,
            table: None,
            schema: None,
            staged: Vec::new(),
            next_txn: None,
        }
    }

    async fn target(&mut self) -> Result<DeltaTarget, ConnectorError> {
        if let Some(t) = &self.target {
            return Ok(t.clone());
        }
        let (cfg, secrets, egress) = self
            .pending
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("delta sink has no target"))?;
        let t = DeltaTarget::from_config(cfg, &**secrets, egress.clone()).await?;
        self.target = Some(t.clone());
        Ok(t)
    }

    /// Load (or refresh) the table; `None` while it does not exist.
    async fn refresh(&mut self) -> Result<(), ConnectorError> {
        match &mut self.table {
            Some(t) => t
                .update_state()
                .await
                .map_err(|e| delta_err(e, "refresh table"))?,
            None => {
                let target = self.target().await?;
                self.table = target.load().await?;
            }
        }
        if let Some(t) = &self.table {
            let st = t.snapshot().map_err(|e| delta_err(e, "snapshot"))?;
            let schema: Schema = (&*st.schema())
                .try_into_arrow()
                .map_err(|e| ConnectorError::fatal(format!("table schema: {e}")))?;
            self.schema = Some(Arc::new(schema));
        }
        Ok(())
    }

    async fn create(&mut self, schema: &SchemaRef) -> Result<(), ConnectorError> {
        if !self.cfg.create {
            return Err(ConnectorError::fatal(
                "the Delta table does not exist and sink.create is false",
            ));
        }
        let kernel: StructType = schema
            .clone()
            .try_into_kernel()
            .map_err(|e| ConnectorError::schema_incompatible(format!("schema for Delta: {e}")))?;
        let handle = self.target().await?.handle()?;
        let mut create = handle
            .create()
            .with_columns(kernel.fields().cloned())
            .with_save_mode(SaveMode::Ignore);
        if !self.cfg.partition_by.is_empty() {
            create = create.with_partition_columns(self.cfg.partition_by.clone());
        }
        let table = create.await.map_err(|e| delta_err(e, "create table"))?;
        self.table = Some(table);
        self.refresh().await
    }

    fn table(&self) -> Result<&DeltaTable, ConnectorError> {
        self.table
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("the Delta table is not loaded"))
    }

    async fn committed_txn(&mut self) -> Result<Option<i64>, ConnectorError> {
        let Some(t) = &self.table else {
            return Ok(None);
        };
        let st = t.snapshot().map_err(|e| delta_err(e, "snapshot"))?;
        st.transaction_version(&*t.log_store(), &self.app)
            .await
            .map_err(|e| delta_err(e, "txn version"))
    }

    /// The checkpoint stored with the commit that carries `txn`.
    async fn checkpoint_of(&self, txn: i64) -> Result<Option<Checkpoint>, ConnectorError> {
        let t = self.table()?;
        let infos: Vec<_> = t
            .history(None)
            .try_collect()
            .await
            .map_err(|e| delta_err(e, "history"))?;
        for info in infos {
            let ours = info.info.get(META_APP).and_then(|v| v.as_str()) == Some(self.app.as_str());
            let this = info.info.get(META_TXN).and_then(serde_json::Value::as_i64) == Some(txn);
            if ours
                && this
                && let Some(cp) = info.info.get(META_CHECKPOINT)
            {
                return decode_checkpoint(&self.instance, cp).map(Some);
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl Sink for DeltaSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        if self.schema.is_none() {
            self.refresh().await?;
        }
        if let (EventBatch::Events(events), Some(schema)) = (&batch, &self.schema) {
            check_columns(events, schema)?;
        }
        let rb = to_record_batch(&batch, self.schema.as_ref())?;
        if self.table.is_none() {
            self.create(&rb.schema()).await?;
        }
        let ack = SinkAck {
            rows: rb.num_rows() as u64,
            bytes: rb.get_array_memory_size() as u64,
        };
        if rb.num_rows() > 0 {
            self.staged.push(rb);
        }
        Ok(ack)
    }

    /// Data becomes visible at `commit`; nothing to do here.
    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        Ok(SinkAck::default())
    }
}

/// Columns an event names that the table lacks are drift: the instance
/// pauses (R5.1) rather than drop them (the Delta sink's evolution is
/// `pause`, R17.6).
fn check_columns(events: &[CloudEvent], schema: &Schema) -> Result<(), ConnectorError> {
    for ev in events {
        if let Ok(serde_json::Value::Object(m)) =
            serde_json::from_slice::<serde_json::Value>(&ev.data)
        {
            for k in m.keys() {
                if schema.field_with_name(k).is_err() {
                    return Err(ConnectorError::schema_incompatible(format!(
                        "column {k:?} is not in the Delta table"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[async_trait]
impl TransactionalSink for DeltaSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        self.staged.clear();
        self.refresh().await?;
        let Some(txn) = self.committed_txn().await? else {
            self.next_txn = Some(1);
            return Ok(None);
        };
        self.next_txn = Some(txn + 1);
        match self.checkpoint_of(txn).await? {
            Some(cp) => Ok(Some(cp)),
            None => Err(ConnectorError::position_lost(format!(
                "the table records txn {txn} for {} but the commit carrying its checkpoint is \
                 no longer in the log",
                self.app
            ))),
        }
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        if self.next_txn.is_none() {
            self.refresh().await?;
            self.next_txn = Some(self.committed_txn().await?.unwrap_or(0) + 1);
        }
        let txn = self.next_txn.unwrap_or(1);
        let adds = if self.staged.is_empty() {
            Vec::new()
        } else {
            let table = self.table()?;
            let mut writer = RecordBatchWriter::for_table(table)
                .map_err(|e| delta_err(e, "writer"))?
                .with_target_file_size(self.cfg.target_file_size.unwrap_or(0));
            for rb in &self.staged {
                writer
                    .write(rb.clone())
                    .await
                    .map_err(|e| delta_err(e, "write data file"))?;
            }
            writer
                .flush()
                .await
                .map_err(|e| delta_err(e, "flush data files"))?
        };
        self.staged.clear();
        let staged = Staged {
            txn,
            checkpoint: encode_checkpoint(cp),
            adds,
        };
        let opaque = serde_json::to_vec(&staged)
            .map_err(|e| ConnectorError::fatal(format!("staging token: {e}")))?;
        Ok(TxnToken {
            id: format!("{}@{txn}", self.app),
            opaque: Bytes::from(opaque),
        })
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        let staged: Staged = serde_json::from_slice(&t.opaque)
            .map_err(|e| ConnectorError::fatal(format!("staging token: {e}")))?;
        self.refresh().await?;
        if self.committed_txn().await?.is_some_and(|c| c >= staged.txn) {
            // Already committed (a retry after the commit landed).
            self.next_txn = Some(staged.txn + 1);
            return Ok(());
        }
        let table = self.table()?;
        let snapshot = table.snapshot().map_err(|e| delta_err(e, "snapshot"))?;
        let partition_by: Vec<String> = snapshot.metadata().partition_columns().to_vec();
        let props = CommitProperties::default()
            .with_application_transaction(Transaction::new(&self.app, staged.txn))
            .with_metadata([
                (META_APP.to_owned(), serde_json::json!(self.app)),
                (META_TXN.to_owned(), serde_json::json!(staged.txn)),
                (META_CHECKPOINT.to_owned(), staged.checkpoint),
            ]);
        CommitBuilder::from(props)
            .with_actions(staged.adds.into_iter().map(Action::Add).collect())
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: (!partition_by.is_empty()).then_some(partition_by),
                    predicate: None,
                },
            )
            .await
            .map_err(|e| delta_err(e, "commit"))?;
        self.next_txn = Some(staged.txn + 1);
        Ok(())
    }

    /// Nothing visible was written; the staged files stay unreferenced for
    /// `VACUUM` (removing them here could race a commit that did land).
    async fn abort(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        self.staged.clear();
        self.next_txn = None;
        Ok(())
    }
}

/// Makes the instance's [`DeltaSink`].
pub struct DeltaSinkFactory {
    make: Box<dyn Fn() -> DeltaSink + Send + Sync>,
}

impl std::fmt::Debug for DeltaSinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeltaSinkFactory")
    }
}

impl DeltaSinkFactory {
    /// A factory over an already-built target.
    pub fn new(target: DeltaTarget, cfg: DeltaSinkConfig, instance: InstanceKey) -> Self {
        Self {
            make: Box::new(move || DeltaSink::new(target.clone(), cfg.clone(), instance.clone())),
        }
    }

    fn from_context(cfg: DeltaConfig, ctx: &ConnectorContext) -> Self {
        let (secrets, egress, instance) = (
            ctx.secrets.clone(),
            ctx.egress.clone(),
            ctx.instance.clone(),
        );
        Self {
            make: Box::new(move || {
                DeltaSink::from_config(
                    cfg.clone(),
                    secrets.clone(),
                    egress.clone(),
                    instance.clone(),
                )
            }),
        }
    }
}

impl SinkFactory for DeltaSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(SinkHandle::Transactional(Box::new((self.make)())))
    }
}

// ---------------------------------------------------------------------------
// Source

/// Where a source is: `version`, and within it the file and the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Deserialize)]
pub struct DeltaPosition {
    pub version: u64,
    /// `true` while reading the snapshot at `version`, `false` while reading
    /// the commit `version` itself.
    pub snapshot: bool,
    pub file: usize,
    pub row: u64,
}

impl DeltaPosition {
    pub fn to_position(self) -> Position {
        let display = format!(
            "{} {} file {} row {}",
            if self.snapshot { "snapshot" } else { "version" },
            self.version,
            self.file,
            self.row
        );
        Position::new(serde_json::to_vec(&self).unwrap_or_default(), display)
    }

    pub fn from_position(p: &Position) -> Result<Self, ConnectorError> {
        serde_json::from_slice(&p.opaque)
            .map_err(|e| ConnectorError::fatal(format!("delta position: {e}")))
    }
}

/// One file of a version to read.
#[derive(Debug, Clone)]
struct FileWork {
    path: Path,
    partition_values: BTreeMap<String, Option<String>>,
    /// `_change_type` to add (CDF mode, when the file has none).
    change_type: Option<&'static str>,
    commit_timestamp_ms: i64,
}

/// The Delta source of one table.
pub struct DeltaSource {
    target: Option<DeltaTarget>,
    pending: Option<Pending>,
    cfg: DeltaSourceConfig,
    envelope: EnvelopeBuilder,
    instance: InstanceKey,
    table: Option<DeltaTable>,
    schema: Option<SchemaRef>,
    partition_columns: Vec<String>,
    pos: DeltaPosition,
    work: Option<Vec<FileWork>>,
    /// The current file's rows, from `pos.row` on.
    rows: Option<(RecordBatch, u64)>,
    last_poll: Option<Instant>,
}

impl std::fmt::Debug for DeltaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaSource")
            .field("pos", &self.pos)
            .finish_non_exhaustive()
    }
}

impl DeltaSource {
    pub fn new(target: DeltaTarget, cfg: DeltaSourceConfig, instance: InstanceKey) -> Self {
        Self::make(Some(target), None, cfg, instance)
    }

    fn make(
        target: Option<DeltaTarget>,
        pending: Option<Pending>,
        cfg: DeltaSourceConfig,
        instance: InstanceKey,
    ) -> Self {
        Self {
            target,
            pending,
            cfg,
            envelope: EnvelopeBuilder::new(CONNECTOR, &instance),
            instance,
            table: None,
            schema: None,
            partition_columns: Vec::new(),
            pos: DeltaPosition {
                version: 0,
                snapshot: false,
                file: 0,
                row: 0,
            },
            work: None,
            rows: None,
            last_poll: None,
        }
    }

    async fn target(&mut self) -> Result<DeltaTarget, ConnectorError> {
        if let Some(t) = &self.target {
            return Ok(t.clone());
        }
        let (cfg, secrets, egress) = self
            .pending
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("delta source has no target"))?;
        let t = DeltaTarget::from_config(cfg, &**secrets, egress.clone()).await?;
        self.target = Some(t.clone());
        Ok(t)
    }

    fn table(&self) -> Result<&DeltaTable, ConnectorError> {
        self.table
            .as_ref()
            .ok_or_else(|| ConnectorError::fatal("the Delta table is not loaded"))
    }

    fn store(&self) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        Ok(self.table()?.object_store())
    }

    fn load_schema(&mut self) -> Result<(), ConnectorError> {
        let t = self.table()?;
        let st = t.snapshot().map_err(|e| delta_err(e, "snapshot"))?;
        let schema: Schema = (&*st.schema())
            .try_into_arrow()
            .map_err(|e| ConnectorError::fatal(format!("table schema: {e}")))?;
        let parts = st.metadata().partition_columns().to_vec();
        self.schema = Some(Arc::new(schema));
        self.partition_columns = parts;
        Ok(())
    }

    /// The files of the snapshot at `pos.version`.
    async fn snapshot_work(&mut self) -> Result<Vec<FileWork>, ConnectorError> {
        let version = self.pos.version;
        let target = self.target().await?;
        let mut t = target
            .load()
            .await?
            .ok_or_else(|| ConnectorError::fatal("the Delta table does not exist"))?;
        t.load_version(version)
            .await
            .map_err(|e| delta_err(e, "load snapshot"))?;
        let st = t.snapshot().map_err(|e| delta_err(e, "snapshot"))?;
        let ts = st.version_timestamp(version).unwrap_or(0);
        let mut files = Vec::new();
        for f in st.log_data() {
            if f.deletion_vector_descriptor().is_some() {
                return Err(deletion_vectors());
            }
            files.push(FileWork {
                path: f.object_store_path(),
                partition_values: f.partition_values_map().into_iter().collect(),
                change_type: Some("insert"),
                commit_timestamp_ms: ts,
            });
        }
        // A stable order, so a resumed position names the same file.
        files.sort_by(|a, b| a.path.as_ref().cmp(b.path.as_ref()));
        Ok(files)
    }

    /// The files of commit `pos.version`, or `None` when it does not exist
    /// yet.
    async fn commit_work(&mut self) -> Result<Option<Vec<FileWork>>, ConnectorError> {
        let version = self.pos.version;
        let store = self.store()?;
        let path = Path::from(format!("_delta_log/{version:020}.json"));
        let got = match store.get(&path).await {
            Ok(g) => g,
            Err(object_store::Error::NotFound { .. }) => {
                let t = self.table()?;
                let latest = t
                    .get_latest_version()
                    .await
                    .map_err(|e| delta_err(e, "latest version"))?;
                if latest < version {
                    return Ok(None);
                }
                return Err(ConnectorError::position_lost(format!(
                    "Delta log entry {version} has been cleaned up; restart from a snapshot \
                     (source.start: snapshot)"
                )));
            }
            Err(e) => return Err(store_err(&e, "read Delta log")),
        };
        let modified = got.meta.last_modified.timestamp_millis();
        let body = got
            .bytes()
            .await
            .map_err(|e| store_err(&e, "read Delta log"))?;
        let actions = parse_commit(&body)?;
        if actions.metadata_changed {
            // A schema change between versions: re-read the schema at this
            // version before decoding its files.
            let target = self.target().await?;
            let mut t = target
                .load()
                .await?
                .ok_or_else(|| ConnectorError::fatal("the Delta table does not exist"))?;
            t.load_version(version)
                .await
                .map_err(|e| delta_err(e, "load version"))?;
            self.table = Some(t);
            self.load_schema()?;
        }
        let ts = actions.timestamp_ms.unwrap_or(modified);
        let cdf = self.cfg.mode == SourceMode::Cdf;
        let mut files = Vec::new();
        if cdf && !actions.cdc.is_empty() {
            for (path, parts) in actions.cdc {
                files.push(FileWork {
                    path,
                    partition_values: parts,
                    change_type: None,
                    commit_timestamp_ms: ts,
                });
            }
        } else {
            for (path, parts) in actions.adds {
                files.push(FileWork {
                    path,
                    partition_values: parts,
                    change_type: Some("insert"),
                    commit_timestamp_ms: ts,
                });
            }
            if cdf {
                for (path, parts) in actions.removes {
                    files.push(FileWork {
                        path,
                        partition_values: parts,
                        change_type: Some("delete"),
                        commit_timestamp_ms: ts,
                    });
                }
            }
        }
        Ok(Some(files))
    }

    /// Read file `pos.file` of the current work whole, with partition and
    /// CDF columns, positioned at `pos.row`.
    async fn load_file(&mut self, w: &FileWork) -> Result<RecordBatch, ConnectorError> {
        let store = self.store()?;
        let got = store
            .get(&w.path)
            .await
            .map_err(|e| store_err(&e, "read data file"))?;
        if got.meta.size > self.cfg.max_file_bytes {
            return Err(ConnectorError::fatal(format!(
                "data file {} is {} bytes, over source.max_file_bytes",
                w.path, got.meta.size
            )));
        }
        let bytes = got
            .bytes()
            .await
            .map_err(|e| store_err(&e, "read data file"))?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| ConnectorError::fatal(format!("data file {}: {e}", w.path)))?
            .build()
            .map_err(|e| ConnectorError::fatal(format!("data file {}: {e}", w.path)))?;
        let mut batches = Vec::new();
        for b in reader {
            batches
                .push(b.map_err(|e| ConnectorError::fatal(format!("data file {}: {e}", w.path)))?);
        }
        let file_schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        };
        let data = arrow_select::concat::concat_batches(&file_schema, &batches)
            .map_err(|e| ConnectorError::fatal(format!("data file {}: {e}", w.path)))?;
        self.decorate(data, w)
    }

    /// Add the partition columns (typed by the table schema) and, in CDF
    /// mode, `_change_type`, `_commit_version` and `_commit_timestamp`.
    fn decorate(&self, data: RecordBatch, w: &FileWork) -> Result<RecordBatch, ConnectorError> {
        let n = data.num_rows();
        let mut fields: Vec<Arc<Field>> = data.schema().fields().iter().cloned().collect();
        let mut cols: Vec<ArrayRef> = data.columns().to_vec();
        let table_schema = self.schema.clone();
        for name in &self.partition_columns {
            if data.schema().field_with_name(name).is_ok() {
                continue;
            }
            let ty = table_schema
                .as_ref()
                .and_then(|s| s.field_with_name(name).ok().map(|f| f.data_type().clone()))
                .unwrap_or(DataType::Utf8);
            let value = w.partition_values.get(name).cloned().flatten();
            let strings: ArrayRef = Arc::new(StringArray::from(vec![value; n]));
            let typed = arrow_cast::cast(&strings, &ty)
                .map_err(|e| ConnectorError::fatal(format!("partition value of {name}: {e}")))?;
            fields.push(Arc::new(Field::new(name, ty, true)));
            cols.push(typed);
        }
        if self.cfg.mode == SourceMode::Cdf {
            if let Some(ct) = w.change_type {
                fields.push(Arc::new(Field::new("_change_type", DataType::Utf8, false)));
                cols.push(Arc::new(StringArray::from(vec![ct; n])));
            }
            fields.push(Arc::new(Field::new(
                "_commit_version",
                DataType::Int64,
                false,
            )));
            cols.push(Arc::new(Int64Array::from(vec![self.pos.version as i64; n])));
            fields.push(Arc::new(Field::new(
                "_commit_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )));
            cols.push(Arc::new(
                TimestampMicrosecondArray::from(vec![w.commit_timestamp_ms * 1000; n])
                    .with_timezone("UTC"),
            ));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
            .map_err(|e| ConnectorError::fatal(format!("decorate rows: {e}")))
    }

    /// Move to the next file, or the next version when the work is done.
    fn advance_file(&mut self) {
        self.rows = None;
        self.pos.file += 1;
        self.pos.row = 0;
    }

    fn advance_version(&mut self) {
        self.work = None;
        self.rows = None;
        self.pos = DeltaPosition {
            version: self.pos.version + 1,
            snapshot: false,
            file: 0,
            row: 0,
        };
    }
}

#[async_trait]
impl Source for DeltaSource {
    async fn open(&mut self, _split: &Split, pos: Option<Position>) -> Result<(), ConnectorError> {
        let target = self.target().await?;
        let table = target
            .load()
            .await?
            .ok_or_else(|| ConnectorError::retryable("the Delta table does not exist yet"))?;
        let latest = table.version().unwrap_or(0);
        self.table = Some(table);
        self.load_schema()?;
        self.pos = match pos {
            Some(p) => DeltaPosition::from_position(&p)?,
            None => match self.cfg.start {
                Start::Snapshot => DeltaPosition {
                    version: latest,
                    snapshot: true,
                    file: 0,
                    row: 0,
                },
                Start::Earliest => DeltaPosition {
                    version: 0,
                    snapshot: false,
                    file: 0,
                    row: 0,
                },
            },
        };
        self.work = None;
        self.rows = None;
        Ok(())
    }

    async fn next(&mut self, budget: Budget) -> Result<Option<SourceBatch>, ConnectorError> {
        loop {
            if self.work.is_none() {
                let work = if self.pos.snapshot {
                    Some(self.snapshot_work().await?)
                } else {
                    let poll = Duration::from_millis(self.cfg.poll_interval_ms);
                    if self.last_poll.is_some_and(|t| t.elapsed() < poll) {
                        return Ok(None);
                    }
                    let t = self
                        .table
                        .as_mut()
                        .ok_or_else(|| ConnectorError::fatal("the Delta table is not loaded"))?;
                    t.update_state()
                        .await
                        .map_err(|e| delta_err(e, "refresh table"))?;
                    match self.commit_work().await? {
                        Some(w) => w,
                        None => {
                            self.last_poll = Some(Instant::now());
                            return Ok(None);
                        }
                    }
                    .into()
                };
                self.work = work;
            }
            let files = self.work.clone().unwrap_or_default();
            if self.pos.file >= files.len() {
                if self.pos.snapshot {
                    // The snapshot is done; follow the log after it.
                    self.work = None;
                    self.rows = None;
                    self.pos = DeltaPosition {
                        version: self.pos.version + 1,
                        snapshot: false,
                        file: 0,
                        row: 0,
                    };
                } else {
                    self.advance_version();
                }
                continue;
            }
            if self.rows.is_none() {
                let w = files[self.pos.file].clone();
                let rb = self.load_file(&w).await?;
                self.rows = Some((rb, self.pos.row));
            }
            let Some((rb, at)) = self.rows.clone() else {
                continue;
            };
            let total = rb.num_rows() as u64;
            if at >= total {
                self.advance_file();
                continue;
            }
            let left = (total - at) as usize;
            let per_row = (rb.get_array_memory_size() / rb.num_rows().max(1)).max(1);
            let by_bytes = (budget.max_bytes.saturating_sub(1024) * 9 / 10) / per_row;
            let take = left.min(budget.max_rows).min(by_bytes);
            if take == 0 {
                return Err(ConnectorError::poison(format!(
                    "a row of {} is larger than the batch budget",
                    files[self.pos.file].path
                )));
            }
            let slice = rb.slice(at as usize, take);
            let owned = arrow_select::concat::concat_batches(&slice.schema(), [&slice])
                .map_err(|e| ConnectorError::fatal(format!("copy rows: {e}")))?;
            let first = at;
            let last = at + take as u64 - 1;
            self.pos.row = at + take as u64;
            self.rows = Some((rb, self.pos.row));
            let run = ids::run_id(
                &self.instance,
                self.target.as_ref().map_or("", |t| t.url().as_str()),
                &format!(
                    "{}{}",
                    if self.pos.snapshot { "s" } else { "v" },
                    self.pos.version
                ),
            );
            let event = match self.cfg.mode {
                SourceMode::Versions => "rows",
                SourceMode::Cdf => "change",
            };
            let resource = self
                .target
                .as_ref()
                .and_then(|t| t.url().path_segments().and_then(|mut s| s.next_back()))
                .unwrap_or("table")
                .to_owned();
            let envelope = self
                .envelope
                .event(
                    event,
                    &resource,
                    ids::rows(&run, self.pos.file as u32, first, last),
                )
                .with_subject(resource.clone())
                .with_data(ARROW_STREAM, Bytes::new())
                .with_extension("loamslsn", self.pos.version.to_string());
            let mut position = self.pos;
            if self.pos.row >= total {
                // The file is done: the position names the next one, so a
                // resume does not re-open it.
                position.file += 1;
                position.row = 0;
            }
            return Ok(Some(SourceBatch {
                events: EventBatch::Arrow {
                    batch: owned,
                    envelope,
                },
                position: position.to_position(),
            }));
        }
    }

    async fn committed(&mut self, _upto: &Position) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.table = None;
        self.rows = None;
        self.work = None;
        Ok(())
    }
}

/// A commit's file actions, as the CDF rules need them.
#[derive(Debug, Default)]
struct CommitActions {
    adds: Vec<(Path, BTreeMap<String, Option<String>>)>,
    removes: Vec<(Path, BTreeMap<String, Option<String>>)>,
    cdc: Vec<(Path, BTreeMap<String, Option<String>>)>,
    metadata_changed: bool,
    timestamp_ms: Option<i64>,
}

/// Files with deletion vectors need the vector applied to their rows, which
/// the source does not do yet (R17.6): refused rather than read wrong.
fn deletion_vectors() -> ConnectorError {
    ConnectorError::fatal(
        "the Delta table uses deletion vectors, which the source does not read yet (CN2 R17.6)",
    )
}

/// Parse one `_delta_log/<version>.json` (newline-delimited actions).
fn parse_commit(body: &[u8]) -> Result<CommitActions, ConnectorError> {
    let mut out = CommitActions::default();
    for line in body.split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let v: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| ConnectorError::fatal(format!("Delta log line: {e}")))?;
        let file = |a: &serde_json::Value| -> Result<_, ConnectorError> {
            let p = a
                .get("path")
                .and_then(|p| p.as_str())
                .ok_or_else(|| ConnectorError::fatal("Delta file action has no path"))?;
            if p.contains("://") {
                return Err(ConnectorError::fatal(format!(
                    "Delta file action with an absolute path ({p}) is not supported"
                )));
            }
            let path = Path::from_url_path(p)
                .map_err(|e| ConnectorError::fatal(format!("Delta file path {p}: {e}")))?;
            let parts = a
                .get("partitionValues")
                .and_then(|m| m.as_object())
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().map(str::to_owned)))
                        .collect()
                })
                .unwrap_or_default();
            Ok((path, parts))
        };
        if let Some(a) = v.get("add").or_else(|| v.get("remove"))
            && a.get("deletionVector").is_some_and(|d| !d.is_null())
        {
            return Err(deletion_vectors());
        }
        if let Some(a) = v.get("add") {
            if a.get("dataChange").and_then(serde_json::Value::as_bool) != Some(false) {
                out.adds.push(file(a)?);
            }
        } else if let Some(a) = v.get("remove") {
            if a.get("dataChange").and_then(serde_json::Value::as_bool) != Some(false) {
                out.removes.push(file(a)?);
            }
        } else if let Some(a) = v.get("cdc") {
            out.cdc.push(file(a)?);
        } else if v.get("metaData").is_some() {
            out.metadata_changed = true;
        } else if let Some(ci) = v.get("commitInfo") {
            out.timestamp_ms = ci
                .get("inCommitTimestamp")
                .or_else(|| ci.get("timestamp"))
                .and_then(serde_json::Value::as_i64);
        }
    }
    // A stable order within the version.
    out.adds.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    out.removes.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    out.cdc.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    Ok(out)
}

/// One split: the table.
#[derive(Debug, Default)]
pub struct DeltaEnumerator;

#[async_trait]
impl SplitEnumerator for DeltaEnumerator {
    async fn discover(&mut self, _known: &[SplitState]) -> Result<Vec<Split>, ConnectorError> {
        Ok(vec![Split::new(TABLE_SPLIT, serde_json::json!({}))])
    }
}

/// Makes the instance's [`DeltaSource`].
pub struct DeltaSourceFactory {
    make: Box<dyn Fn() -> DeltaSource + Send + Sync>,
}

impl std::fmt::Debug for DeltaSourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeltaSourceFactory")
    }
}

impl DeltaSourceFactory {
    pub fn new(target: DeltaTarget, cfg: DeltaSourceConfig, instance: InstanceKey) -> Self {
        Self {
            make: Box::new(move || DeltaSource::new(target.clone(), cfg.clone(), instance.clone())),
        }
    }

    fn from_context(cfg: DeltaConfig, ctx: &ConnectorContext) -> Self {
        let (secrets, egress, instance) = (
            ctx.secrets.clone(),
            ctx.egress.clone(),
            ctx.instance.clone(),
        );
        Self {
            make: Box::new(move || {
                DeltaSource::make(
                    None,
                    Some((cfg.clone(), secrets.clone(), egress.clone())),
                    cfg.source.clone(),
                    instance.clone(),
                )
            }),
        }
    }
}

impl SourceFactory for DeltaSourceFactory {
    fn create(&self, _split: &Split) -> Result<Box<dyn Source>, ConnectorError> {
        Ok(Box::new((self.make)()))
    }
}

/// The `delta-lake` connector (Task 2's ref
/// `loams_connector_lakehouse::delta`).
#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaLake;

impl ConnectorFactory for DeltaLake {
    fn id(&self) -> &'static str {
        CONNECTOR
    }

    fn source(
        &self,
        ctx: &ConnectorContext,
    ) -> Result<Option<(Box<dyn SplitEnumerator>, Box<dyn SourceFactory>)>, ConnectorError> {
        let cfg = DeltaConfig::parse(&ctx.config)?;
        Ok(Some((
            Box::new(DeltaEnumerator),
            Box::new(DeltaSourceFactory::from_context(cfg, ctx)),
        )))
    }

    fn sink(&self, ctx: &ConnectorContext) -> Result<Option<Box<dyn SinkFactory>>, ConnectorError> {
        let cfg = DeltaConfig::parse(&ctx.config)?;
        Ok(Some(Box::new(DeltaSinkFactory::from_context(cfg, ctx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_round_trips() {
        let i = InstanceKey::new(3, "orders");
        let mut cp = Checkpoint::new(i.clone(), 7);
        cp.splits.insert(
            SplitId::new("fabric"),
            Position::new(vec![0, 1, 2], "offset 2"),
        );
        let back = decode_checkpoint(&i, &encode_checkpoint(&cp)).expect("decodes");
        assert_eq!(back, cp);
    }

    #[test]
    fn commit_actions_parse() {
        let log = br#"{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE"}}
{"add":{"path":"p%3D1/a.parquet","partitionValues":{"p":"1"},"size":1,"modificationTime":0,"dataChange":true}}
{"add":{"path":"b.parquet","partitionValues":{},"size":1,"modificationTime":0,"dataChange":false}}
{"remove":{"path":"c.parquet","dataChange":true}}
{"cdc":{"path":"_change_data/d.parquet","partitionValues":{},"size":1,"dataChange":false}}
{"txn":{"appId":"x","version":1}}"#;
        let a = parse_commit(log).expect("parses");
        assert_eq!(a.adds.len(), 1);
        assert_eq!(a.adds[0].0.as_ref(), "p=1/a.parquet");
        assert_eq!(a.adds[0].1.get("p"), Some(&Some("1".to_owned())));
        assert_eq!(a.removes.len(), 1);
        assert_eq!(a.cdc.len(), 1);
        assert_eq!(a.timestamp_ms, Some(1_700_000_000_000));
        assert!(parse_commit(br#"{"add":{"path":"s3://x/y.parquet","dataChange":true}}"#).is_err());
    }

    #[test]
    fn merge_mode_is_refused_naming_the_ruling() {
        let err = DeltaConfig::parse(&serde_json::json!({
            "table_uri": "memory:///t", "sink": {"mode": "merge"}
        }))
        .expect_err("refused");
        assert!(err.message.contains("R17.6"));
    }
}
