//! Table jobs (design §45 §4.1–§4.2; LV1 plan Tasks 8 and 9): leased,
//! checkpointed, rate-limited scans of one table's documents in batches.
//! The validation scan of a stricter validator (Task 8) runs as one, and so
//! does the backfill of new indexes ([`IndexBackfill`], Task 9), which
//! starts once [`Journal::fence_after`](crate::Journal::fence_after) has
//! fenced the journal after the transaction that added them. Dropped
//! indexes' entries are deleted by [`drop_indexes`], which fences the
//! journal after the swap that dropped them before it deletes.
//!
//! A job is a catalog record ([`JobRecord`], `0x01 ‖ 0x07 ‖ table_id ‖
//! kind`), written with its owner's lease by the transaction that starts
//! the work ([`create_job`]). [`TableJob::run`] then takes batches of
//! [`JobOptions::batch`] documents in key order, each batch **one
//! transaction** that reads the record, checks the lease, reads the
//! documents after the checkpoint, hands them to a [`Visitor`] (which may
//! write in the same transaction) and writes the record back with the new
//! checkpoint, the visitor's [`Finding`]s and a renewed lease. Every batch
//! reads at its own start timestamp, so a batch sees every write committed
//! before it.
//!
//! **The lease** lives in the app's own keyspace, so every node of the app
//! sees it, on both backends. A job whose lease has expired (by the store's
//! clock) may be taken over by another owner, which resumes from the
//! checkpoint; a batch of the old owner then fails, since it reads the
//! record and finds another owner, and two owners' batches conflict on the
//! record. A batch that fails rolls back with its findings, so a document is
//! never visited twice into the record nor skipped.
//!
//! **The rate** is [`JobOptions::docs_per_s`]: after a batch of `n`
//! documents the job sleeps until `n / docs_per_s` has passed since the
//! batch began.

use std::sync::Arc;
use std::time::{Duration, Instant};

use buffa::Message;
use futures::future::BoxFuture;
use loams_kv::{Txn, TxnError};
use ulid::Ulid;

use crate::catalog::{self, IndexState};
use crate::docs::{self, Doc, Reads};
use crate::ids::{DOC_ID_BYTES, DocId, IndexId, TableId};
use crate::keys::{AppKeys, KeyRange, key_after, prefix_range};
use crate::txn::{Refusal, atomic, latest_snapshot};
use crate::{Limits, LiveError, Runner, pb};

/// Documents per batch by default (design §45 §4.2).
pub const DEFAULT_BATCH: usize = 256;

/// Documents per second by default (design §45 §4.2,
/// `backfill_docs_per_s`).
pub const DEFAULT_DOCS_PER_S: u32 = 5_000;

/// How long a lease lasts after its owner's last batch, by default.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// Batches in a row a job lets fail on conflicts (halving the batch each
/// time) before it gives up: from 256 documents, eight to reach one, then
/// eight at one.
const MAX_BATCH_CONFLICTS: u32 = 16;

/// The most findings a job keeps (design §45 §4.1: a failed validation
/// reports up to 100 offending ids).
pub const MAX_FINDINGS: usize = 100;

/// How an app's table jobs scan ([`RunnerOptions::jobs`](crate::RunnerOptions)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobOptions {
    /// Documents per batch (each batch is one transaction); at least 1.
    pub batch: usize,
    /// Documents per second; at least 1.
    pub docs_per_s: u32,
    /// How long a lease lasts after each batch.
    pub lease: Duration,
}

impl Default for JobOptions {
    fn default() -> Self {
        JobOptions {
            batch: DEFAULT_BATCH,
            docs_per_s: DEFAULT_DOCS_PER_S,
            lease: DEFAULT_LEASE,
        }
    }
}

/// What a table job does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobKind {
    /// Checks the documents against a stricter validator (Task 8).
    Validate,
    /// Writes the entries of the table's backfilling indexes (Task 9).
    Backfill,
}

impl JobKind {
    /// The kind byte of the job's catalog key.
    fn byte(self) -> u8 {
        match self {
            JobKind::Validate => 1,
            JobKind::Backfill => 2,
        }
    }

    fn to_proto(self) -> pb::TableJobKind {
        match self {
            JobKind::Validate => pb::TableJobKind::TABLE_JOB_KIND_VALIDATE,
            JobKind::Backfill => pb::TableJobKind::TABLE_JOB_KIND_BACKFILL,
        }
    }

    /// The job's name in messages.
    pub fn name(self) -> &'static str {
        match self {
            JobKind::Validate => "validation",
            JobKind::Backfill => "index backfill",
        }
    }
}

/// A document a job found, with what is wrong (no values).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub id: DocId,
    pub detail: String,
}

/// A table job's record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub kind: JobKind,
    pub table: TableId,
    /// The deployment the job works for.
    pub deployment: Ulid,
    /// Who holds the lease, and until when (TSO physical time, ms).
    pub owner: String,
    pub lease_expires_ms: u64,
    /// The last document key done; empty before the first batch.
    pub checkpoint: Vec<u8>,
    /// Documents visited so far.
    pub scanned: u64,
    /// The scan reached the end of the table.
    pub done: bool,
    /// What the job found, at most [`MAX_FINDINGS`].
    pub findings: Vec<Finding>,
}

impl JobRecord {
    /// Whether the lease is held at `now_ms` (store time).
    pub fn lease_live(&self, now_ms: u64) -> bool {
        now_ms < self.lease_expires_ms
    }

    fn to_proto(&self) -> pb::TableJobRecord {
        pb::TableJobRecord {
            format: crate::RECORD_FORMAT,
            kind: self.kind.to_proto().into(),
            table_id: self.table.0,
            deployment: self.deployment.to_bytes().to_vec(),
            owner: self.owner.clone(),
            lease_expires_ms: self.lease_expires_ms,
            checkpoint: self.checkpoint.clone(),
            scanned: self.scanned,
            done: self.done,
            findings: self
                .findings
                .iter()
                .map(|f| pb::JobFinding {
                    doc_id: f.id.bytes.to_vec(),
                    detail: f.detail.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let corrupt = |what: String| LiveError::Corrupt(format!("table job record: {what}"));
        let p = pb::TableJobRecord::decode_from_slice(bytes).map_err(|e| corrupt(e.to_string()))?;
        if !crate::record_format_readable(p.format) {
            return Err(corrupt(format!(
                "format {} (this release reads {})",
                p.format,
                crate::record_format_range()
            )));
        }
        let kind = match p.kind.as_known() {
            Some(pb::TableJobKind::TABLE_JOB_KIND_VALIDATE) => JobKind::Validate,
            Some(pb::TableJobKind::TABLE_JOB_KIND_BACKFILL) => JobKind::Backfill,
            _ => return Err(corrupt(format!("unknown kind {:?}", p.kind))),
        };
        let deployment: [u8; 16] = p
            .deployment
            .as_slice()
            .try_into()
            .map_err(|_| corrupt(format!("a {}-byte deployment", p.deployment.len())))?;
        let table = TableId(p.table_id);
        let findings =
            p.findings
                .into_iter()
                .map(|f| {
                    let bytes: [u8; DOC_ID_BYTES] =
                        f.doc_id.as_slice().try_into().map_err(|_| {
                            corrupt(format!("a {}-byte document id", f.doc_id.len()))
                        })?;
                    Ok(Finding {
                        id: DocId { table, bytes },
                        detail: f.detail,
                    })
                })
                .collect::<Result<_, LiveError>>()?;
        Ok(JobRecord {
            kind,
            table,
            deployment: Ulid::from_bytes(deployment),
            owner: p.owner,
            lease_expires_ms: p.lease_expires_ms,
            checkpoint: p.checkpoint,
            scanned: p.scanned,
            done: p.done,
            findings,
        })
    }
}

/// What a job does with each batch, inside the batch's transaction: it may
/// read and write there, and returns what it found. Its error rolls the
/// batch back and stops the job (the record keeps the last checkpoint).
pub trait Visitor: Send + Sync {
    fn visit<'a>(
        &'a self,
        txn: &'a mut Txn,
        app: &'a AppKeys,
        docs: &'a [Doc],
    ) -> BoxFuture<'a, Result<Vec<Finding>, LiveError>>;
}

/// Table `table`'s job of kind `kind`, if one is recorded.
pub async fn load_job(
    r: &mut impl Reads,
    app: &AppKeys,
    kind: JobKind,
    table: TableId,
) -> Result<Option<JobRecord>, LiveError> {
    match r.get(&app.table_job(table, kind.byte())).await? {
        Some(bytes) => JobRecord::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

/// Starts table `table`'s job of kind `kind` for `deployment` in `txn`: a
/// fresh record (no checkpoint) whose lease `owner` holds for `lease` from
/// the transaction's start. A job of the table held by another live lease
/// is [`LiveError::FailedPrecondition`]; an expired one is replaced.
pub async fn create_job(
    txn: &mut Txn,
    app: &AppKeys,
    kind: JobKind,
    table: TableId,
    deployment: Ulid,
    owner: &str,
    lease: Duration,
) -> Result<JobRecord, LiveError> {
    let now_ms = txn.start_ts().physical_ms();
    if let Some(held) = load_job(txn, app, kind, table).await?
        && held.owner != owner
        && held.lease_live(now_ms)
    {
        return Err(leased(&held));
    }
    let record = JobRecord {
        kind,
        table,
        deployment,
        owner: owner.to_string(),
        lease_expires_ms: now_ms.saturating_add(millis(lease)),
        checkpoint: Vec::new(),
        scanned: 0,
        done: false,
        findings: Vec::new(),
    };
    txn.put(
        &app.table_job(table, kind.byte()),
        record.to_proto().encode_to_vec(),
    )
    .await?;
    Ok(record)
}

/// Deletes table `table`'s job of kind `kind` in `txn`.
pub async fn delete_job(
    txn: &mut Txn,
    app: &AppKeys,
    kind: JobKind,
    table: TableId,
) -> Result<(), LiveError> {
    txn.delete(&app.table_job(table, kind.byte())).await?;
    Ok(())
}

fn leased(held: &JobRecord) -> LiveError {
    LiveError::FailedPrecondition(format!(
        "table {}'s {} job (deployment {}) is leased by another owner",
        held.table.0,
        held.kind.name(),
        held.deployment
    ))
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// One owner's run of a table job: see the module documentation.
#[derive(Debug, Clone)]
pub struct TableJob {
    runner: Runner,
    kind: JobKind,
    table: TableId,
    owner: String,
}

impl TableJob {
    /// The job of kind `kind` on table `table` of `runner`'s app, run as
    /// `owner`. Its options are the runner's [`JobOptions`].
    pub fn new(runner: Runner, kind: JobKind, table: TableId, owner: impl Into<String>) -> Self {
        TableJob {
            runner,
            kind,
            table,
            owner: owner.into(),
        }
    }

    /// Runs the job from its checkpoint until it is done or holds at least
    /// `stop_after` findings, and returns its record. The record must exist
    /// ([`create_job`]): a missing one is [`LiveError::FailedPrecondition`]
    /// (the job was cancelled), as is one leased by another live owner. An
    /// expired lease of another owner is taken over. A visitor's error, or a
    /// storage error past the runner's retries, stops the run; the record
    /// keeps the last checkpoint.
    pub async fn run(
        &self,
        visitor: Arc<dyn Visitor>,
        stop_after: usize,
    ) -> Result<JobRecord, LiveError> {
        let options = self.runner.options().jobs;
        let full = options.batch.max(1);
        let mut batch = full;
        let mut conflicts = 0;
        let docs_per_s = f64::from(options.docs_per_s.max(1));
        loop {
            let started = Instant::now();
            let (record, scanned) =
                match self.batch(&visitor, batch, options.lease, docs_per_s).await {
                    Ok(done) => done,
                    // Writers kept touching the batch's documents past the
                    // runner's attempts: halve the batch (a batch that locks
                    // fewer documents meets fewer writers), down to one
                    // document, which gets a few more tries (row T9-4).
                    Err(LiveError::Txn(TxnError::Conflict)) if conflicts < MAX_BATCH_CONFLICTS => {
                        conflicts += 1;
                        batch = (batch / 2).max(1);
                        continue;
                    }
                    Err(e) => return Err(e),
                };
            if record.done || record.findings.len() >= stop_after {
                return Ok(record);
            }
            // A batch went through: grow back towards the configured size.
            conflicts = 0;
            batch = (batch * 2).min(full);
            let budget = Duration::from_secs_f64(scanned as f64 / docs_per_s);
            if let Some(pause) = budget.checked_sub(started.elapsed()) {
                tokio::time::sleep(pause).await;
            }
        }
    }

    /// One batch in one transaction: the record (checked and taken over if
    /// its lease expired), up to `batch` documents after the checkpoint,
    /// the visitor, and the record written back. The renewed lease covers
    /// `lease` plus the rate pause that follows the batch, so a slow rate
    /// never lets the lease lapse between batches (PR #446 review).
    async fn batch(
        &self,
        visitor: &Arc<dyn Visitor>,
        batch: usize,
        lease: Duration,
        docs_per_s: f64,
    ) -> Result<(JobRecord, usize), LiveError> {
        let app = self.runner.app().clone();
        let (kind, table) = (self.kind, self.table);
        let owner = self.owner.clone();
        let committed = atomic(&self.runner, "live.backfill.batch", |txn| {
            let (app, owner, visitor) = (app.clone(), owner.clone(), visitor.clone());
            Box::pin(async move {
                let now_ms = txn.start_ts().physical_ms();
                let mut record = load_job(txn, &app, kind, table).await?.ok_or_else(|| {
                    LiveError::FailedPrecondition(format!(
                        "table {}'s {} job was cancelled",
                        table.0,
                        kind.name()
                    ))
                })?;
                if record.owner != owner {
                    if record.lease_live(now_ms) {
                        return Err(leased(&record));
                    }
                    record.owner = owner;
                }
                if record.done {
                    return Ok((record, 0));
                }
                let all = app.documents(table);
                let range = KeyRange {
                    lo: if record.checkpoint.is_empty() {
                        all.lo
                    } else {
                        key_after(&record.checkpoint)
                    },
                    hi: all.hi,
                };
                let pairs = Reads::scan(txn, &range, batch, false).await?;
                let docs = pairs
                    .iter()
                    .map(|(key, value)| {
                        let id = app.doc_id_of_document(table, key).ok_or_else(|| {
                            LiveError::Corrupt("a document key of the wrong length".into())
                        })?;
                        Doc::decode(id, value)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let found = visitor.visit(txn, &app, &docs).await?;
                let room = MAX_FINDINGS.saturating_sub(record.findings.len());
                record.findings.extend(found.into_iter().take(room));
                record.scanned += docs.len() as u64;
                record.done = pairs.len() < batch;
                if let Some((last, _)) = pairs.last() {
                    record.checkpoint = last.clone();
                }
                let pause = Duration::from_secs_f64(docs.len() as f64 / docs_per_s);
                record.lease_expires_ms = now_ms.saturating_add(millis(lease + pause));
                txn.put(
                    &app.table_job(table, kind.byte()),
                    record.to_proto().encode_to_vec(),
                )
                .await?;
                Ok((record, docs.len()))
            })
        })
        .await;
        match committed {
            Ok(c) => Ok(c.value),
            Err(Refusal::Own(e) | Refusal::Storage(e)) => Err(e),
        }
    }
}

/// The index backfill's [`Visitor`] (LV1 plan Task 9): for each document
/// of the batch, the entry of every index of the table that `deployment`
/// is backfilling. It first locks the batch's documents (`lock_keys`), so a
/// write to one of them that commits while the batch runs makes one of the
/// two rerun: no entry of an old version of a document is written after
/// that version is gone. A document whose entry would pass
/// `max_index_key_bytes` is a [`Finding`], and gets no entry. An index no
/// longer backfilling for `deployment` (released, or taken over by another
/// deploy) is skipped.
#[derive(Debug, Clone)]
pub struct IndexBackfill {
    /// The deployment whose backfilling indexes are filled.
    pub deployment: Ulid,
    /// The app's limits (`max_index_key_bytes`).
    pub limits: Limits,
}

impl IndexBackfill {
    pub fn new(deployment: Ulid, limits: Limits) -> Self {
        IndexBackfill { deployment, limits }
    }
}

impl Visitor for IndexBackfill {
    fn visit<'a>(
        &'a self,
        txn: &'a mut Txn,
        app: &'a AppKeys,
        docs: &'a [Doc],
    ) -> BoxFuture<'a, Result<Vec<Finding>, LiveError>> {
        Box::pin(async move {
            let Some(table) = docs.first().map(|d| d.id.table) else {
                return Ok(Vec::new());
            };
            let def = catalog::load_table_by_id(txn, app, table)
                .await?
                .ok_or_else(|| {
                    LiveError::FailedPrecondition(format!("table {} is gone", table.0))
                })?;
            let ours = IndexState::Backfilling {
                deployment: self.deployment,
            };
            let indexes: Vec<_> = def.indexes.iter().filter(|i| i.state == ours).collect();
            if indexes.is_empty() {
                return Ok(Vec::new());
            }
            txn.lock_keys(docs.iter().map(|d| app.document(&d.id)))
                .await?;
            let mut findings = Vec::new();
            for doc in docs {
                for index in &indexes {
                    match docs::index_key(app, &def, index, doc, &self.limits) {
                        Ok(key) => txn.put(&key, Vec::new()).await?,
                        Err(
                            e @ (LiveError::LimitExceeded { .. } | LiveError::InvalidArgument(_)),
                        ) => {
                            findings.push(Finding {
                                id: doc.id,
                                detail: e.to_string(),
                            });
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            Ok(findings)
        })
    }
}

/// An index's status (LV1 plan Task 9, `GetIndexStatus`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStatus {
    pub state: IndexState,
    /// Documents the backfill has done (every document, once ready).
    pub done: u64,
    /// The table's documents now (its `by_creation_time` entries).
    pub total_estimate: u64,
}

/// The status of index `index` of table `table` of `runner`'s app, read
/// now: a ready or backfilling one of that name first, else one being
/// dropped. An unknown table or index is [`LiveError::NotFound`].
pub async fn index_status(
    runner: &Runner,
    table: &str,
    index: &str,
) -> Result<IndexStatus, LiveError> {
    let app = runner.app();
    let mut snap = latest_snapshot(runner.store()).await?;
    let def = catalog::load_table(&mut snap, app, table)
        .await?
        .ok_or_else(|| LiveError::NotFound(format!("table '{table}'")))?;
    let found = def
        .index_named(index)
        .or_else(|| def.indexes.iter().find(|i| i.name == index))
        .ok_or_else(|| LiveError::NotFound(format!("index '{index}' of table '{table}'")))?;
    // Counted in pages, so a large table is never held in memory at once.
    let mut entries = prefix_range(app.index_prefix(def.id, IndexId::BY_CREATION_TIME));
    let mut total = 0_u64;
    loop {
        let page = Reads::scan(&mut snap, &entries, STATUS_PAGE, false).await?;
        total += page.len() as u64;
        match page.last() {
            Some((last, _)) if page.len() == STATUS_PAGE => entries.lo = key_after(last),
            _ => break,
        }
    }
    let done = match found.state {
        IndexState::Ready => total,
        IndexState::Dropping => 0,
        IndexState::Backfilling { deployment } => {
            load_job(&mut snap, app, JobKind::Backfill, def.id)
                .await?
                .filter(|j| j.deployment == deployment)
                .map_or(0, |j| j.scanned)
        }
    };
    Ok(IndexStatus {
        state: found.state,
        done,
        total_estimate: total,
    })
}

/// The keys [`index_status`] counts per read.
const STATUS_PAGE: usize = 4_096;

/// The index entries one janitor round deletes at most
/// ([`drop_indexes_within`]); the rest wait for the next round.
pub const JANITOR_DROP_KEYS: usize = 100_000;

/// Deletes the entries of every index being dropped, in batches of the
/// runner's [`JobOptions::batch`] keys at its rate (one transaction each),
/// then removes its definition; returns the indexes removed. It first
/// fences the journal ([`Journal::fence_after`](crate::Journal::fence_after)),
/// so every mutation that read the index as maintained has committed (its
/// entries are then deleted here) or reruns and skips it. Idempotent: the
/// server runs it after a deploy that drops an index; each janitor round
/// runs [`drop_indexes_within`] [`JANITOR_DROP_KEYS`].
pub async fn drop_indexes(runner: &Runner) -> Result<Vec<(TableId, IndexId)>, LiveError> {
    drop_indexes_within(runner, usize::MAX).await
}

/// Like [`drop_indexes`], deleting at most about `budget` entries (whole
/// batches); an index not finished stays `dropping` for the next call.
pub async fn drop_indexes_within(
    runner: &Runner,
    budget: usize,
) -> Result<Vec<(TableId, IndexId)>, LiveError> {
    let app = runner.app().clone();
    let mut snap = latest_snapshot(runner.store()).await?;
    let dropping: Vec<(TableId, IndexId)> = catalog::list_tables(&mut snap, &app)
        .await?
        .iter()
        .flat_map(|t| {
            t.indexes
                .iter()
                .filter(|i| i.state == IndexState::Dropping)
                .map(|i| (t.id, i.id))
        })
        .collect();
    if dropping.is_empty() {
        return Ok(Vec::new());
    }
    let shards = catalog::load_journal_shards(&mut snap, &app)
        .await?
        .ok_or_else(|| LiveError::Internal("the app has no journal record".into()))?;
    drop(snap);
    crate::Journal::new(app.clone(), shards)?
        .fence_after(runner.store(), runner.options().commit_mode)
        .await?;
    let options = runner.options().jobs;
    let batch = options.batch.max(1);
    let per_s = f64::from(options.docs_per_s.max(1));
    let mut removed = Vec::new();
    let mut deleted = 0_usize;
    'indexes: for (table, index) in dropping {
        loop {
            if deleted >= budget {
                break 'indexes;
            }
            let started = Instant::now();
            let app = app.clone();
            let step = atomic(runner, "live.backfill.drop", |txn| {
                let app = app.clone();
                Box::pin(async move {
                    let Some(mut def) = catalog::load_table_by_id(txn, &app, table).await? else {
                        return Ok(Dropped::Gone);
                    };
                    if !def
                        .indexes
                        .iter()
                        .any(|i| i.id == index && i.state == IndexState::Dropping)
                    {
                        return Ok(Dropped::Gone);
                    }
                    let range = prefix_range(app.index_prefix(table, index));
                    let keys = Reads::scan(txn, &range, batch, false).await?;
                    for (key, _) in &keys {
                        txn.delete(key).await?;
                    }
                    if keys.len() < batch {
                        def.indexes.retain(|i| i.id != index);
                        catalog::put_table(txn, &app, &def).await?;
                        return Ok(Dropped::Removed);
                    }
                    Ok(Dropped::Deleted(keys.len()))
                })
            })
            .await;
            match step {
                Ok(c) => match c.value {
                    Dropped::Gone => break,
                    Dropped::Removed => {
                        removed.push((table, index));
                        break;
                    }
                    Dropped::Deleted(n) => {
                        deleted += n;
                        let budget = Duration::from_secs_f64(n as f64 / per_s);
                        if let Some(pause) = budget.checked_sub(started.elapsed()) {
                            tokio::time::sleep(pause).await;
                        }
                    }
                },
                Err(Refusal::Own(e) | Refusal::Storage(e)) => return Err(e),
            }
        }
    }
    Ok(removed)
}

/// What one batch of [`drop_indexes`] did.
enum Dropped {
    /// The index is no longer being dropped (another node finished it).
    Gone,
    /// It deleted this many entries; more may be left.
    Deleted(usize),
    /// It deleted the last entries and the definition.
    Removed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_records_round_trip() {
        let table = TableId(9);
        let record = JobRecord {
            kind: JobKind::Validate,
            table,
            deployment: Ulid::from_parts(1_700_000_000_000, 7),
            owner: "node-a".into(),
            lease_expires_ms: 1_700_000_030_000,
            checkpoint: vec![1, 2, 3],
            scanned: 512,
            done: false,
            findings: vec![Finding {
                id: DocId {
                    table,
                    bytes: [5; DOC_ID_BYTES],
                },
                detail: "$.n: missing required field".into(),
            }],
        };
        let bytes = record.to_proto().encode_to_vec();
        assert_eq!(JobRecord::decode(&bytes).expect("decodes"), record);
        assert!(record.lease_live(1_700_000_029_999));
        assert!(!record.lease_live(1_700_000_030_000));
        let mut p = record.to_proto();
        p.format = 2;
        assert!(JobRecord::decode(&p.encode_to_vec()).is_err());
        let mut p = record.to_proto();
        p.kind = pb::TableJobKind::TABLE_JOB_KIND_UNSPECIFIED.into();
        assert!(JobRecord::decode(&p.encode_to_vec()).is_err());
        let mut p = record.to_proto();
        p.findings[0].doc_id.pop();
        assert!(JobRecord::decode(&p.encode_to_vec()).is_err());
    }

    #[test]
    fn job_keys_name_the_table_and_the_kind() {
        let app = AppKeys::dedicated();
        let key = app.table_job(TableId(3), JobKind::Validate.byte());
        assert_eq!(key, vec![0x01, 0x07, 0, 0, 0, 3, 1]);
        let key = app.table_job(TableId(3), JobKind::Backfill.byte());
        assert_eq!(key, vec![0x01, 0x07, 0, 0, 0, 3, 2]);
    }
}
