//! The sink: upserts by key and deletes (`write: upsert`, the default), or
//! inserts (`write: insert`), as ordered `update`/`delete`/`insert` commands
//! (the documents travel as `OP_MSG` document sequences, up to the server's
//! batch limits).
//!
//! **Exactly once** (`transactional: true`, the default, on a replica set
//! or a sharded cluster): a batch is written inside one multi-document
//! transaction, and `prepare` upserts the checkpoint into
//! `<database>.<checkpoint_collection>` (`_loams_checkpoints`) in the same
//! transaction, fenced by epoch (a newer epoch's document is never
//! overwritten: the stale writer fails). `commit` commits with
//! `w: "majority"`. After a crash, `recover` reads the checkpoint the
//! database committed; data and checkpoint are both there or both not.
//!
//! **One session per instance.** The transaction's logical session id is
//! derived from the instance, and transaction numbers only grow (they start
//! from the clock), so a restarted sink's first transaction aborts whatever
//! a crashed predecessor left open on that session instead of waiting for
//! it to time out (plan ruling R16.9). A commit whose outcome is unknown is
//! retried as `commitTransaction` (idempotent); `NoSuchTransaction` means it
//! did not commit, and the sink replays the batch, the checkpoint and the
//! commit in a new transaction (looking first for its own token in the
//! checkpoint collection).
//!
//! **A standalone server has no transactions**: the same handle writes each
//! batch directly and the checkpoint after it, so `upsert` is effectively
//! once (a replayed batch rewrites the same documents) and `insert` at least
//! once. [`sink_delivery`] says which a deployment gives.

use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use loams_connector_sdk::{
    Checkpoint, CloudEvent, ConnectorContext, ConnectorError, Delivery, EventBatch, InstanceKey,
    Position, Sink, SinkAck, SinkFactory, SinkHandle, SplitId, TransactionalSink, TxnToken,
};
use ring::digest;
use serde_json::Value;

use crate::bson::{Bson, Document};
use crate::client::Client;
use crate::config::{MongoConfig, SinkConfig, Write};
use crate::doc;
use crate::extjson;
use crate::types;
use crate::wire::{Sequence, codes, server_error};

pub use crate::config::CHECKPOINTS;

/// The most operations in one write command.
const MAX_OPS: usize = 10_000;
/// The most document bytes in one write command (the server's message
/// limit is 48 MB; documents are 16 MiB at most).
const MAX_COMMAND_BYTES: usize = 32 * 1024 * 1024;
/// `TransactionTooOld`: a transaction number below the session's.
const TRANSACTION_TOO_OLD: i32 = 225;

/// One write.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// Replace the document matching `filter`, inserting it when absent.
    Replace {
        filter: Document,
        doc: Document,
    },
    /// `$set`/`$unset` on the document matching `filter` (a change event's
    /// update description); nothing when absent.
    Patch {
        filter: Document,
        set: Document,
        unset: Vec<String>,
    },
    Insert(Document),
    Delete {
        filter: Document,
    },
}

/// The value at a dotted path.
fn lookup<'a>(d: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut cur = d;
    let mut parts = path.split('.').peekable();
    while let Some(p) = parts.next() {
        let v = cur.get(p)?;
        if parts.peek().is_none() {
            return Some(v);
        }
        cur = v.as_document()?;
    }
    None
}

/// The equality filter on the key fields of `d`.
pub fn key_filter(d: &Document, key: &[String]) -> Result<Document, String> {
    let mut f = Document::new();
    for k in key {
        let v = lookup(d, k).ok_or_else(|| format!("the document has no key field {k}"))?;
        f.insert(k.clone(), v.clone());
    }
    Ok(f)
}

fn as_document(v: &Value, what: &str) -> Result<Document, String> {
    match extjson::from_json(v)? {
        Bson::Document(d) => Ok(d),
        _ => Err(format!("{what} must be a JSON object")),
    }
}

/// The write a whole document asks for: a replace by key (with `ce_id` as
/// the `_id` of a document that has none, when the key is `_id`), or an
/// insert.
fn put(sc: &SinkConfig, mut d: Document, ce_id: &str) -> Result<Op, String> {
    match sc.write {
        Write::Insert => Ok(Op::Insert(d)),
        Write::Upsert => {
            if sc.key == ["_id"] && !d.contains_key("_id") {
                d.insert("_id", ce_id);
            }
            Ok(Op::Replace {
                filter: key_filter(&d, &sc.key)?,
                doc: d,
            })
        }
    }
}

/// The write one event asks for; `None` for a delete the instance does not
/// apply. A `change` event (`loamsop` set and `{key, before, after,
/// update}` data) replaces with `after`, patches with `update`, or deletes
/// by `key`; any other event's data is the document itself.
pub fn event_op(ev: &CloudEvent, sc: &SinkConfig) -> Result<Option<Op>, String> {
    let data: Value =
        serde_json::from_slice(&ev.data).map_err(|e| format!("event {}: not JSON: {e}", ev.id))?;
    let Value::Object(obj) = &data else {
        return Err(format!("event {}: the data must be a JSON object", ev.id));
    };
    let op = ev.extensions.get("loamsop").map(String::as_str);
    let is_change = op.is_some()
        && ["after", "before", "key", "update"]
            .iter()
            .any(|k| obj.contains_key(*k));
    let wrap = |e: String| format!("event {}: {e}", ev.id);
    if !is_change {
        let d = as_document(&data, "the data").map_err(wrap)?;
        return match op {
            Some("d") if !sc.apply_deletes => Ok(None),
            Some("d") => Ok(Some(Op::Delete {
                filter: key_filter(&d, &sc.key).map_err(wrap)?,
            })),
            _ => put(sc, d, &ev.id).map(Some).map_err(wrap),
        };
    }
    let part = |k: &str| -> Result<Option<Document>, String> {
        match obj.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => as_document(v, k).map(Some),
        }
    };
    let key_source = || -> Result<Document, String> {
        // The change's key when it holds the key fields, else its before.
        for k in ["key", "before", "after"] {
            if let Some(d) = part(k)?
                && let Ok(f) = key_filter(&d, &sc.key)
            {
                return Ok(f);
            }
        }
        Err(format!("the change has no key field {}", sc.key.join(", ")))
    };
    match op {
        Some("d") if !sc.apply_deletes => Ok(None),
        Some("d") => Ok(Some(Op::Delete {
            filter: key_source().map_err(wrap)?,
        })),
        Some("c" | "r" | "u") => {
            if let Some(after) = part("after").map_err(wrap)? {
                return put(sc, after, &ev.id).map(Some).map_err(wrap);
            }
            let Some(Value::Object(u)) = obj.get("update") else {
                return Err(wrap("the change has neither after nor update".into()));
            };
            if sc.write == Write::Insert {
                return Err(wrap(
                    "an update without a full document cannot be inserted".into(),
                ));
            }
            let set = match u.get("updated") {
                None | Some(Value::Null) => Document::new(),
                Some(v) => as_document(v, "update.updated").map_err(wrap)?,
            };
            let unset = match u.get("removed") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(a)) => a
                    .iter()
                    .map(|f| f.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| wrap("update.removed must list field names".into()))?,
                Some(_) => return Err(wrap("update.removed must be a list".into())),
            };
            Ok(Some(Op::Patch {
                filter: key_source().map_err(wrap)?,
                set,
                unset,
            }))
        }
        Some(other) => Err(wrap(format!(
            "loamsop {other:?} is not applied by the sink"
        ))),
        None => unreachable!("is_change needs loamsop"),
    }
}

/// Each Arrow row as a whole document (`_id` defaults to
/// `<ce_id>/<row>`).
pub fn arrow_ops(
    batch: &RecordBatch,
    envelope: &CloudEvent,
    sc: &SinkConfig,
) -> Result<Vec<Op>, String> {
    (0..batch.num_rows())
        .map(|r| {
            let d = types::row_to_document(batch, r)?;
            put(sc, d, &format!("{}/{r}", envelope.id))
        })
        .collect()
}

/// The write commands for `ops`, in order: consecutive operations of one
/// kind share a command (ordered, so per-key order holds).
pub fn commands(ops: &[Op], collection: &str) -> Vec<(Document, Sequence)> {
    let mut out: Vec<(Document, Sequence)> = Vec::new();
    let mut bytes = 0usize;
    for op in ops {
        let (cmd, ident, d) = match op {
            Op::Replace { filter, doc } => (
                "update",
                "updates",
                doc! { "q" => filter.clone(), "u" => doc.clone(), "upsert" => true, "multi" => false },
            ),
            Op::Patch { filter, set, unset } => {
                let mut u = Document::new();
                if !set.is_empty() {
                    u.insert("$set", set.clone());
                }
                if !unset.is_empty() {
                    u.insert(
                        "$unset",
                        unset
                            .iter()
                            .map(|f| (f.clone(), Bson::from("")))
                            .collect::<Document>(),
                    );
                }
                if u.is_empty() {
                    continue;
                }
                (
                    "update",
                    "updates",
                    doc! { "q" => filter.clone(), "u" => u, "upsert" => false, "multi" => false },
                )
            }
            Op::Insert(d) => ("insert", "documents", d.clone()),
            Op::Delete { filter } => (
                "delete",
                "deletes",
                doc! { "q" => filter.clone(), "limit" => 1 },
            ),
        };
        let size = d.encoded_len().unwrap_or(0);
        let fits = out.last().is_some_and(|(c, s)| {
            c.keys().next() == Some(cmd)
                && s.documents.len() < MAX_OPS
                && bytes + size <= MAX_COMMAND_BYTES
        });
        if !fits {
            bytes = 0;
            out.push((
                doc! { cmd => collection, "ordered" => true },
                Sequence {
                    identifier: ident.to_owned(),
                    documents: Vec::new(),
                },
            ));
        }
        bytes += size;
        out.last_mut().expect("pushed").1.documents.push(d);
    }
    out
}

/// What the deployment behind `cfg` gives this sink: exactly once with
/// transactions, else effectively once for `upsert` and at least once for
/// `insert` (Review Focus 1: the manifest may declare no more).
pub async fn sink_delivery(
    cfg: &MongoConfig,
    ctx: &ConnectorContext,
) -> Result<Delivery, ConnectorError> {
    let sc = cfg
        .sink
        .as_ref()
        .ok_or_else(|| ConnectorError::fatal("config: collection is required for a sink"))?;
    let mut client = Client::new(cfg.clone(), ctx.clone());
    let hello = client.hello().await?;
    client.reset();
    Ok(
        match (sc.transactional && hello.supports_transactions(), sc.write) {
            (true, _) => Delivery::ExactlyOnce,
            (false, Write::Upsert) => Delivery::EffectivelyOnce,
            (false, Write::Insert) => Delivery::AtLeastOnce,
        },
    )
}

/// Makes the instance's sink.
#[derive(Debug)]
pub struct MongoSinkFactory {
    cfg: MongoConfig,
    ctx: ConnectorContext,
}

impl MongoSinkFactory {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext) -> Result<Self, ConnectorError> {
        if cfg.sink.is_none() {
            return Err(ConnectorError::fatal(
                "config: collection is required for a sink",
            ));
        }
        Ok(Self { cfg, ctx })
    }
}

impl SinkFactory for MongoSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        let sink = MongoSink::new(self.cfg.clone(), self.ctx.clone());
        Ok(if sink.sc.transactional {
            SinkHandle::Transactional(Box::new(sink))
        } else {
            SinkHandle::Plain(Box::new(sink))
        })
    }
}

static TOKEN_SEQ: AtomicU64 = AtomicU64::new(0);

/// The instance's logical session id: 16 bytes of SHA-256 over its key.
pub fn session_id(instance: &InstanceKey) -> Document {
    let d = digest::digest(
        &digest::SHA256,
        format!(
            "loams-connector-mongodb/session/{}/{}",
            instance.namespace, instance.name
        )
        .as_bytes(),
    );
    doc! { "id" => Bson::Binary { subtype: 4, bytes: d.as_ref()[..16].to_vec() } }
}

/// See the module documentation.
pub struct MongoSink {
    sc: SinkConfig,
    db: String,
    instance: InstanceKey,
    client: Client,
    lsid: Document,
    txn_number: i64,
    /// A transaction is open on the server (as far as this sink knows).
    in_txn: bool,
    /// The open transaction's first command has been sent.
    started: bool,
    /// The deployment supports transactions (known after connecting).
    txn_capable: Option<bool>,
    ready: bool,
    /// The batch's operations written so far, replayed if the transaction
    /// is lost before it commits.
    staged: Vec<Op>,
    prepared: Option<(TxnToken, Checkpoint)>,
}

impl std::fmt::Debug for MongoSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoSink")
            .field("collection", &self.sc.collection)
            .field("transactional", &self.sc.transactional)
            .finish_non_exhaustive()
    }
}

impl MongoSink {
    pub fn new(cfg: MongoConfig, ctx: ConnectorContext) -> Self {
        let sc = cfg.sink.clone().expect("checked by the factory");
        let start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_micros() as i64);
        Self {
            db: cfg.database.clone(),
            instance: ctx.instance.clone(),
            lsid: session_id(&ctx.instance),
            client: Client::new(cfg, ctx),
            sc,
            txn_number: start,
            in_txn: false,
            started: false,
            txn_capable: None,
            ready: false,
            staged: Vec::new(),
            prepared: None,
        }
    }

    /// What this sink gives on the deployment it reached (after the first
    /// write or `recover`).
    pub fn delivery(&self) -> Option<Delivery> {
        let txn = self.txn_capable? && self.sc.transactional;
        Some(match (txn, self.sc.write) {
            (true, _) => Delivery::ExactlyOnce,
            (false, Write::Upsert) => Delivery::EffectivelyOnce,
            (false, Write::Insert) => Delivery::AtLeastOnce,
        })
    }

    fn transactions(&self) -> bool {
        self.sc.transactional && self.txn_capable == Some(true)
    }

    /// Connect; the first time, learn whether transactions exist and create
    /// the collections (a transaction may not create one on older servers).
    async fn ready(&mut self) -> Result<(), ConnectorError> {
        if self.ready {
            return Ok(());
        }
        let hello = self.client.hello().await?;
        self.txn_capable = Some(hello.supports_transactions());
        if self.sc.transactional && !hello.supports_transactions() {
            tracing::warn!(
                collection = %self.sc.collection,
                "the server has no multi-document transactions (standalone): the sink is effectively once for upserts, at least once for inserts"
            );
        }
        if self.sc.transactional {
            for c in [
                self.sc.collection.clone(),
                self.sc.checkpoint_collection.clone(),
            ] {
                match self.client.command(&self.db, doc! { "create" => c }).await {
                    Ok(_) => {}
                    Err(e)
                        if server_error(&e).is_some_and(|s| s.code == codes::NAMESPACE_EXISTS) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        self.ready = true;
        Ok(())
    }

    fn txn_fields(&mut self, cmd: &mut Document) {
        cmd.insert("lsid", self.lsid.clone());
        cmd.insert("txnNumber", self.txn_number);
        cmd.insert("autocommit", false);
        if !self.started {
            cmd.insert("startTransaction", true);
            self.started = true;
        }
    }

    /// Run `ops` (in the open transaction when there is one).
    async fn apply(&mut self, ops: &[Op]) -> Result<(), ConnectorError> {
        for (mut cmd, seq) in commands(ops, &self.sc.collection) {
            if self.in_txn {
                self.txn_fields(&mut cmd);
            }
            self.client
                .command_with(&self.db, cmd, std::slice::from_ref(&seq))
                .await?;
        }
        Ok(())
    }

    /// Open a transaction (a new number) and replay the staged operations.
    async fn begin(&mut self) -> Result<(), ConnectorError> {
        if self.in_txn {
            return Ok(());
        }
        self.txn_number += 1;
        self.in_txn = true;
        self.started = false;
        if !self.staged.is_empty() {
            let staged = self.staged.clone();
            self.apply(&staged).await?;
        }
        Ok(())
    }

    /// After a failure inside a transaction: the server aborts it on any
    /// error, so it is gone; the staged operations stay for the replay.
    fn lose_txn(&mut self, e: &ConnectorError) {
        if self.in_txn {
            self.in_txn = false;
            self.started = false;
        }
        if server_error(e).is_some_and(|s| s.code == TRANSACTION_TOO_OLD) {
            // The session saw a higher number (a clock that went back):
            // jump past it.
            self.txn_number += 1 << 32;
        }
    }

    fn ops(&self, batch: &EventBatch) -> Result<Vec<Op>, ConnectorError> {
        let r = match batch {
            EventBatch::Events(evs) => evs
                .iter()
                .map(|e| event_op(e, &self.sc))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| v.into_iter().flatten().collect()),
            EventBatch::Arrow { batch, envelope } => arrow_ops(batch, envelope, &self.sc),
        };
        r.map_err(ConnectorError::poison)
    }

    fn checkpoint_id(&self, split: &SplitId) -> String {
        format!("{}/{split}", MongoConfig::instance_id(&self.instance))
    }

    async fn write_checkpoint(
        &mut self,
        token: &TxnToken,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        let instance = MongoConfig::instance_id(&cp.instance);
        let mut updates = Vec::new();
        for (split, pos) in &cp.splits {
            updates.push(doc! {
                "q" => doc! { "_id" => self.checkpoint_id(split), "epoch" => doc! { "$lte" => cp.epoch as i64 } },
                "u" => doc! { "$set" => doc! {
                    "instance" => instance.clone(),
                    "split" => split.as_str(),
                    "position" => Bson::Binary { subtype: 0, bytes: pos.opaque.to_vec() },
                    "display" => pos.display.clone(),
                    "epoch" => cp.epoch as i64,
                    "txn" => token.id.clone(),
                } },
                "upsert" => true,
                "multi" => false,
            });
        }
        if updates.is_empty() {
            return Ok(());
        }
        let mut cmd = doc! { "update" => self.sc.checkpoint_collection.clone(), "ordered" => true };
        if self.in_txn {
            self.txn_fields(&mut cmd);
        }
        let seq = Sequence {
            identifier: "updates".into(),
            documents: updates,
        };
        match self
            .client
            .command_with(&self.db, cmd, std::slice::from_ref(&seq))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if server_error(&e).is_some_and(|s| s.code == codes::DUPLICATE_KEY) => {
                Err(ConnectorError::fatal(format!(
                    "checkpoint fenced: {}.{} holds a newer epoch than {} for {instance}",
                    self.db, self.sc.checkpoint_collection, cp.epoch
                )))
            }
            Err(e) => Err(e),
        }
    }

    /// Whether the checkpoint collection holds `txn` (that commit landed).
    async fn committed_txn(&mut self, txn: &str) -> Result<bool, ConnectorError> {
        let reply = self
            .client
            .command(
                &self.db,
                doc! {
                    "find" => self.sc.checkpoint_collection.clone(),
                    "filter" => doc! { "instance" => MongoConfig::instance_id(&self.instance), "txn" => txn },
                    "limit" => 1, "singleBatch" => true,
                },
            )
            .await?;
        Ok(reply
            .get_document("cursor")
            .and_then(|c| c.get_array("firstBatch"))
            .is_some_and(|b| !b.is_empty()))
    }

    async fn commit_txn(&mut self) -> Result<(), ConnectorError> {
        let cmd = doc! {
            "commitTransaction" => 1,
            "lsid" => self.lsid.clone(),
            "txnNumber" => self.txn_number,
            "autocommit" => false,
            "writeConcern" => doc! { "w" => "majority" },
        };
        self.client.command("admin", cmd).await.map(|_| ())
    }

    fn finished(&mut self) {
        self.in_txn = false;
        self.started = false;
        self.staged.clear();
        self.prepared = None;
    }
}

#[async_trait]
impl Sink for MongoSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let ack = SinkAck {
            rows: batch.rows() as u64,
            bytes: batch.size_bytes() as u64,
        };
        let ops = self.ops(&batch)?;
        self.ready().await?;
        if !self.transactions() {
            self.apply(&ops).await?;
            return Ok(ack);
        }
        let r = async {
            self.begin().await?;
            self.apply(&ops).await
        }
        .await;
        match r {
            Ok(()) => {
                self.staged.extend(ops);
                Ok(ack)
            }
            Err(e) => {
                self.lose_txn(&e);
                Err(e)
            }
        }
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // Every write is acknowledged by the server when it returns.
        Ok(SinkAck::default())
    }
}

#[async_trait]
impl TransactionalSink for MongoSink {
    async fn recover(&mut self) -> Result<Option<Checkpoint>, ConnectorError> {
        self.ready().await?;
        self.finished();
        let reply = self
            .client
            .command(
                &self.db,
                doc! {
                    "find" => self.sc.checkpoint_collection.clone(),
                    "filter" => doc! { "instance" => MongoConfig::instance_id(&self.instance) },
                },
            )
            .await?;
        let rows = reply
            .get_document("cursor")
            .and_then(|c| c.get_array("firstBatch"))
            .unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }
        let mut cp = Checkpoint::new(self.instance.clone(), 0);
        for r in rows {
            let Some(d) = r.as_document() else { continue };
            let (Some(split), Some(Bson::Binary { bytes, .. })) =
                (d.get_str("split"), d.get("position"))
            else {
                continue;
            };
            cp.epoch = cp.epoch.max(d.get_i64("epoch").unwrap_or(0).max(0) as u64);
            cp.splits.insert(
                SplitId::new(split),
                Position::new(
                    Bytes::from(bytes.clone()),
                    d.get_str("display").unwrap_or("").to_owned(),
                ),
            );
        }
        Ok(Some(cp))
    }

    async fn prepare(&mut self, cp: &Checkpoint) -> Result<TxnToken, ConnectorError> {
        let token = TxnToken {
            id: format!(
                "{}@{}#{}-{}",
                MongoConfig::instance_id(&cp.instance),
                cp.epoch,
                std::process::id(),
                TOKEN_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
            opaque: Bytes::new(),
        };
        self.ready().await?;
        let r = async {
            if self.transactions() {
                self.begin().await?;
            }
            self.write_checkpoint(&token, cp).await
        }
        .await;
        if let Err(e) = r {
            self.lose_txn(&e);
            return Err(e);
        }
        self.prepared = Some((token.clone(), cp.clone()));
        Ok(token)
    }

    async fn commit(&mut self, t: TxnToken) -> Result<(), ConnectorError> {
        if !self.transactions() {
            // Written as it went (standalone): nothing to commit.
            self.finished();
            return Ok(());
        }
        if self.in_txn {
            match self.commit_txn().await {
                Ok(()) => {
                    self.finished();
                    return Ok(());
                }
                Err(e) => {
                    let gone = server_error(&e).is_some_and(|s| {
                        s.code == codes::NO_SUCH_TRANSACTION
                            || (s.has_label("TransientTransactionError")
                                && !s.has_label("UnknownTransactionCommitResult"))
                    });
                    if !gone {
                        // Unknown outcome: the pipeline retries the commit,
                        // which is idempotent on the same session and number.
                        return Err(e);
                    }
                    self.lose_txn(&e);
                }
            }
        }
        // The transaction is gone: did it commit before it went?
        if self.committed_txn(&t.id).await? {
            self.finished();
            return Ok(());
        }
        let Some((_, cp)) = self.prepared.clone() else {
            return Err(ConnectorError::fatal(format!(
                "commit of unknown transaction {}",
                t.id
            )));
        };
        let r = async {
            self.begin().await?;
            self.write_checkpoint(&t, &cp).await?;
            self.commit_txn().await
        }
        .await;
        match r {
            Ok(()) => {
                self.finished();
                Ok(())
            }
            Err(e) => {
                self.lose_txn(&e);
                Err(e)
            }
        }
    }

    async fn abort(&mut self, _t: TxnToken) -> Result<(), ConnectorError> {
        if self.in_txn && self.started {
            let cmd = doc! {
                "abortTransaction" => 1,
                "lsid" => self.lsid.clone(),
                "txnNumber" => self.txn_number,
                "autocommit" => false,
            };
            let _ = self.client.command("admin", cmd).await;
        }
        self.finished();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson::ObjectId;
    use crate::testkit::FakeServer;
    use loams_connector_sdk::{Scrubber, StaticSecrets};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn sc(write: Write) -> SinkConfig {
        SinkConfig {
            collection: "out".into(),
            write,
            key: vec!["_id".into()],
            apply_deletes: true,
            transactional: true,
            checkpoint_collection: CHECKPOINTS.into(),
        }
    }

    fn ev(id: &str, op: Option<&str>, data: Value) -> CloudEvent {
        let mut e = CloudEvent::new(id, "/connectors/x/t", "t")
            .with_data("application/json", data.to_string());
        if let Some(op) = op {
            e = e.with_extension("loamsop", op);
        }
        e
    }

    #[test]
    fn events_become_writes() {
        let s = sc(Write::Upsert);
        // A plain document without _id takes the event id.
        assert_eq!(
            event_op(&ev("e-1", None, json!({"v": 1})), &s).expect("op"),
            Some(Op::Replace {
                filter: doc! { "_id" => "e-1" },
                doc: doc! { "v" => 1, "_id" => "e-1" },
            })
        );
        // Extended JSON keeps BSON types.
        let oid = "65a1b2c3d4e5f60718293a4b";
        let op = event_op(
            &ev(
                "e-2",
                None,
                json!({"_id": {"$oid": oid}, "n": {"$numberLong": "5"}}),
            ),
            &s,
        )
        .expect("op")
        .expect("some");
        let Op::Replace { filter, doc } = op else {
            panic!("replace")
        };
        assert_eq!(
            filter.get("_id"),
            Some(&Bson::ObjectId(ObjectId::parse_hex(oid).expect("oid")))
        );
        assert_eq!(doc.get("n"), Some(&Bson::Int64(5)));
        // A change: after replaces, update patches, delete deletes by key.
        let after =
            json!({"ns": "d.c", "op": "c", "key": {"_id": 1}, "after": {"_id": 1, "v": "a"}});
        assert!(matches!(
            event_op(&ev("e", Some("c"), after), &s).expect("op"),
            Some(Op::Replace { .. })
        ));
        let patch = json!({"key": {"_id": 1}, "after": null,
                           "update": {"updated": {"a.b": 2}, "removed": ["gone"]}});
        assert_eq!(
            event_op(&ev("e", Some("u"), patch), &s).expect("op"),
            Some(Op::Patch {
                filter: doc! { "_id" => 1 },
                set: doc! { "a.b" => 2 },
                unset: vec!["gone".into()],
            })
        );
        let del = json!({"key": {"_id": 1}, "before": null, "after": null});
        assert_eq!(
            event_op(&ev("e", Some("d"), del.clone()), &s).expect("op"),
            Some(Op::Delete {
                filter: doc! { "_id" => 1 }
            })
        );
        let keep = SinkConfig {
            apply_deletes: false,
            ..s.clone()
        };
        assert_eq!(event_op(&ev("e", Some("d"), del), &keep).expect("op"), None);
        // Insert mode inserts as given.
        assert_eq!(
            event_op(&ev("e", None, json!({"n": 1})), &sc(Write::Insert)).expect("op"),
            Some(Op::Insert(doc! { "n" => 1 }))
        );
        // Poison: not an object, no key, unknown op.
        assert!(event_op(&ev("e", None, json!([1])), &s).is_err());
        let by_sku = SinkConfig {
            key: vec!["sku".into()],
            ..s.clone()
        };
        assert!(event_op(&ev("e", None, json!({"x": 1})), &by_sku).is_err());
        assert!(event_op(&ev("e", Some("t"), json!({"key": {}})), &s).is_err());
    }

    #[test]
    fn commands_group_runs_and_keep_order() {
        let ops = vec![
            Op::Replace {
                filter: doc! { "_id" => 1 },
                doc: doc! { "_id" => 1 },
            },
            Op::Replace {
                filter: doc! { "_id" => 2 },
                doc: doc! { "_id" => 2 },
            },
            Op::Delete {
                filter: doc! { "_id" => 1 },
            },
            Op::Patch {
                filter: doc! { "_id" => 2 },
                set: doc! { "v" => 1 },
                unset: vec![],
            },
            Op::Insert(doc! { "n" => 1 }),
        ];
        let cmds = commands(&ops, "out");
        let kinds: Vec<(&str, usize)> = cmds
            .iter()
            .map(|(c, s)| (c.keys().next().expect("name"), s.documents.len()))
            .collect();
        assert_eq!(
            kinds,
            [("update", 2), ("delete", 1), ("update", 1), ("insert", 1)]
        );
        assert_eq!(cmds[0].0.get_bool("ordered"), Some(true));
        assert_eq!(
            cmds[2].1.documents[0],
            doc! { "q" => doc! { "_id" => 2 }, "u" => doc! { "$set" => doc! { "v" => 1 } },
            "upsert" => false, "multi" => false }
        );
        let many: Vec<Op> = (0..(MAX_OPS + 1) as i32)
            .map(|i| Op::Insert(doc! { "n" => i }))
            .collect();
        assert_eq!(commands(&many, "out").len(), 2);
    }

    #[test]
    fn the_session_id_is_the_instances() {
        let a = session_id(&InstanceKey::new(1, "a"));
        assert_eq!(a, session_id(&InstanceKey::new(1, "a")));
        assert_ne!(a, session_id(&InstanceKey::new(2, "a")));
        assert!(
            matches!(a.get("id"), Some(Bson::Binary { subtype: 4, bytes }) if bytes.len() == 16)
        );
    }

    fn ctx() -> ConnectorContext {
        ConnectorContext::new(
            InstanceKey::new(1, "i"),
            json!({}),
            Arc::new(StaticSecrets::new()),
            Scrubber::new(),
        )
    }

    fn cfg(host: &str, write: &str) -> MongoConfig {
        MongoConfig::parse(
            &json!({"hosts": [host], "database": "app", "collection": "out", "write": write}),
        )
        .expect("config")
    }

    /// The decision behind `mongodb_standalone_narrows_to_effectively_once`
    /// (tests/sink.rs runs it against a real standalone server).
    #[tokio::test]
    async fn standalone_hello_narrows_delivery() {
        let standalone = FakeServer::start(|_| {
            doc! { "ok" => 1, "isWritablePrimary" => true, "maxWireVersion" => 25,
            "logicalSessionTimeoutMinutes" => 30 }
        })
        .await;
        let rs = FakeServer::start(|_| {
            doc! { "ok" => 1, "isWritablePrimary" => true, "setName" => "rs0", "maxWireVersion" => 25,
                   "logicalSessionTimeoutMinutes" => 30 }
        })
        .await;
        assert_eq!(
            sink_delivery(&cfg(&standalone.host(), "upsert"), &ctx())
                .await
                .expect("d"),
            Delivery::EffectivelyOnce
        );
        assert_eq!(
            sink_delivery(&cfg(&standalone.host(), "insert"), &ctx())
                .await
                .expect("d"),
            Delivery::AtLeastOnce
        );
        assert_eq!(
            sink_delivery(&cfg(&rs.host(), "insert"), &ctx())
                .await
                .expect("d"),
            Delivery::ExactlyOnce
        );
    }

    /// The transactional path's command flow against a fake replica set:
    /// the first command starts the transaction, the checkpoint rides in
    /// it, and the commit names the same session and number.
    #[tokio::test]
    async fn a_batch_and_its_checkpoint_share_one_transaction() {
        let seen = Arc::new(Mutex::new(Vec::<Document>::new()));
        let s2 = seen.clone();
        let server = FakeServer::start(move |cmd| {
            if cmd.contains_key("hello") {
                return doc! { "ok" => 1, "isWritablePrimary" => true, "setName" => "rs0",
                "maxWireVersion" => 25, "logicalSessionTimeoutMinutes" => 30 };
            }
            s2.lock().expect("lock").push(cmd.clone());
            if cmd.contains_key("find") {
                return doc! { "ok" => 1, "cursor" => doc! { "id" => 0i64, "ns" => "app.x",
                "firstBatch" => Vec::<Bson>::new() } };
            }
            doc! { "ok" => 1, "n" => 1 }
        })
        .await;
        let mut sink = MongoSink::new(cfg(&server.host(), "insert"), ctx());
        assert_eq!(sink.recover().await.expect("recover"), None);
        assert_eq!(sink.delivery(), Some(Delivery::ExactlyOnce));
        sink.write(EventBatch::Events(vec![ev("e1", None, json!({"n": 1}))]))
            .await
            .expect("write");
        let mut cp = Checkpoint::new(InstanceKey::new(1, "i"), 3);
        cp.splits
            .insert(SplitId::new("fabric"), Position::from_u64(7));
        let t = sink.prepare(&cp).await.expect("prepare");
        sink.commit(t).await.expect("commit");
        let seen = seen.lock().expect("lock").clone();
        let names: Vec<&str> = seen
            .iter()
            .map(|c| c.keys().next().expect("name"))
            .collect();
        assert_eq!(
            names,
            [
                "create",
                "create",
                "find",
                "insert",
                "update",
                "commitTransaction"
            ]
        );
        let (insert, ckpt, commit) = (&seen[3], &seen[4], &seen[5]);
        assert_eq!(insert.get_bool("startTransaction"), Some(true));
        assert_eq!(ckpt.get_bool("startTransaction"), None);
        for c in [insert, ckpt, commit] {
            assert_eq!(
                c.get("lsid"),
                Some(&Bson::Document(session_id(&InstanceKey::new(1, "i"))))
            );
            assert_eq!(c.get_i64("txnNumber"), insert.get_i64("txnNumber"));
            assert_eq!(c.get_bool("autocommit"), Some(false));
        }
        let upd = ckpt.get_array("updates").expect("updates")[0]
            .as_document()
            .expect("doc")
            .clone();
        assert_eq!(
            upd.get_document("q"),
            Some(&doc! { "_id" => "1/i/fabric", "epoch" => doc! { "$lte" => 3i64 } })
        );
    }
}
