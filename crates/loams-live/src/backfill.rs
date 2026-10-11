//! Table jobs (design §45 §4.1–§4.2; LV1 plan Task 8): leased,
//! checkpointed, rate-limited scans of one table's documents in batches.
//! The validation scan of a stricter validator (Task 8) runs as one, and the
//! index backfill of Task 9 will too.
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
use loams_kv::Txn;
use ulid::Ulid;

use crate::docs::{Doc, Reads};
use crate::ids::{DOC_ID_BYTES, DocId, TableId};
use crate::keys::{AppKeys, KeyRange, key_after};
use crate::txn::{Refusal, atomic};
use crate::{LiveError, Runner, pb};

/// Documents per batch by default (design §45 §4.2).
pub const DEFAULT_BATCH: usize = 256;

/// Documents per second by default (design §45 §4.2,
/// `backfill_docs_per_s`).
pub const DEFAULT_DOCS_PER_S: u32 = 5_000;

/// How long a lease lasts after its owner's last batch, by default.
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

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
}

impl JobKind {
    /// The kind byte of the job's catalog key.
    fn byte(self) -> u8 {
        match self {
            JobKind::Validate => 1,
        }
    }

    fn to_proto(self) -> pb::TableJobKind {
        match self {
            JobKind::Validate => pb::TableJobKind::TABLE_JOB_KIND_VALIDATE,
        }
    }

    /// The job's name in messages.
    pub fn name(self) -> &'static str {
        match self {
            JobKind::Validate => "validation",
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
            format: 1,
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
        if p.format != 1 {
            return Err(corrupt(format!("format {} (expected 1)", p.format)));
        }
        let kind = match p.kind.as_known() {
            Some(pb::TableJobKind::TABLE_JOB_KIND_VALIDATE) => JobKind::Validate,
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
        let batch = options.batch.max(1);
        let docs_per_s = f64::from(options.docs_per_s.max(1));
        loop {
            let started = Instant::now();
            let (record, scanned) = self.batch(&visitor, batch, options.lease).await?;
            if record.done || record.findings.len() >= stop_after {
                return Ok(record);
            }
            let budget = Duration::from_secs_f64(scanned as f64 / docs_per_s);
            if let Some(pause) = budget.checked_sub(started.elapsed()) {
                tokio::time::sleep(pause).await;
            }
        }
    }

    /// One batch in one transaction: the record (checked and taken over if
    /// its lease expired), up to `batch` documents after the checkpoint,
    /// the visitor, and the record written back.
    async fn batch(
        &self,
        visitor: &Arc<dyn Visitor>,
        batch: usize,
        lease: Duration,
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
                record.lease_expires_ms = now_ms.saturating_add(millis(lease));
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
    }
}
