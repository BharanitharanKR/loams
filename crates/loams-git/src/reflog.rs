//! The ref log (§36 §5.2): what a writer asks the sequencer to commit,
//! what it gets back, and the [`RefLog`] trait.
//!
//! [`BucketRefLog`](crate::BucketRefLog), the sequencer over a
//! [`WalStore`](crate::WalStore), implements it (GT1 Task 10); the state
//! machine that checks and applies these transactions is [`crate::state`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use sha2::{Digest, Sha256};

use crate::format::event::TxnEvent;
use crate::format::pb;
use crate::ids::{IdemKey, ObjectId, Principal, RefName, RepoId, RepoPath, Seq, TraceParent};
use crate::state::StateError;
use crate::wal::WalError;

/// What a ref must point at for an update to apply (§36 §5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Expect {
    /// The ref must currently point here.
    Oid(ObjectId),
    /// The ref must not exist (a create).
    Absent,
    /// Any current value: a force update, which protections may refuse.
    Any,
}

/// One ref update (§36 §5.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdate {
    /// The ref.
    pub name: RefName,
    /// What it must point at now.
    pub expect: Expect,
    /// What it will point at; `None` deletes it.
    pub new: Option<ObjectId>,
}

/// A write admission scope (§36 §5.4, D397): cone-mode sparse-checkout
/// paths at a commit; empty `cones` is the whole tree. Unchecked until GT4.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope {
    /// The repository.
    pub repo: RepoId,
    /// The commit the cones are read at.
    pub commit: ObjectId,
    /// The paths a writer may change.
    pub cones: Vec<RepoPath>,
}

/// A ref transaction: every update applies, or none (§36 §5.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefTxn {
    /// The idempotency key (§36 §4.5): the client's, or the derived push key.
    pub key: IdemKey,
    /// The digest of the request the key is bound to; a reuse of the key
    /// with another digest is refused.
    pub request_digest: [u8; 32],
    /// 1..=`MAX_REFS_PER_TXN` (4096) updates.
    pub updates: Vec<RefUpdate>,
    /// Packs the transaction adds; they must already exist in the `BlobStore`.
    pub packs: Vec<pb::PackRef>,
    /// Who pushed (the credential's subject).
    pub principal: Principal,
    /// Write admission (D397); `None` is the whole repository.
    pub scope: Option<Scope>,
    /// The request's trace context.
    pub trace: Option<TraceParent>,
    /// git push options.
    pub push_options: Vec<String>,
}

impl RefTxn {
    /// The transaction's data message, as its WAL event carries it.
    pub fn to_pb(&self, format: pb::ObjectFormat) -> pb::RefTransaction {
        use pb::ref_update::Expect as PbExpect;
        let updates = self
            .updates
            .iter()
            .map(|update| pb::RefUpdate {
                name: update.name.as_str().to_string(),
                expect: Some(match update.expect {
                    Expect::Oid(oid) => PbExpect::OldOid(oid.as_bytes().to_vec()),
                    Expect::Absent => PbExpect::MustNotExist(true),
                    Expect::Any => PbExpect::Any(true),
                }),
                new_oid: update
                    .new
                    .map_or_else(Vec::new, |oid| oid.as_bytes().to_vec()),
            })
            .collect();
        pb::RefTransaction {
            updates,
            packs: self.packs.clone(),
            principal: self.principal.as_str().to_string(),
            push_options: self.push_options.clone(),
            object_format: format as i32,
            request_digest: self.request_digest.to_vec(),
        }
    }
}

/// Where a committed transaction landed (§36 §5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Receipt {
    /// Its segment.
    pub seq: Seq,
    /// Its index in the segment's batch.
    pub index: u32,
    /// Whether this answer repeats an earlier commit of the same key.
    pub replayed: bool,
}

/// Why one ref of a transaction was refused (§36 §5.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// The ref does not point where the update expected.
    Stale {
        /// Where it points (`None`: it does not exist).
        current: Option<ObjectId>,
    },
    /// A create of a ref that exists.
    Exists,
    /// A protection rule refuses the delete or the force update.
    Protected,
    /// The writer's scope does not cover a path the update changes.
    OutOfScope {
        /// The first such path.
        path: RepoPath,
    },
    /// The ref name is not valid here (for example, named twice in one
    /// transaction).
    InvalidName,
}

/// The refs, configuration and pack set after a seq (§36 §5.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefSnapshot {
    /// The last applied segment.
    pub seq: Seq,
    /// Every ref.
    pub refs: BTreeMap<RefName, ObjectId>,
    /// Symbolic refs: `HEAD` or a ref name, to the ref it points at
    /// (ruling T8.2).
    pub symrefs: BTreeMap<String, RefName>,
    /// Protection rules.
    pub protections: Vec<pb::Protection>,
    /// The live pack set.
    pub packs: Vec<pb::PackRef>,
    /// The fork parent, whose packs objects fall through to.
    pub parent: Option<pb::ForkParent>,
}

/// Errors of a [`RefLog`] (§36 §5.2).
#[derive(Debug, thiserror::Error)]
pub enum RefError {
    /// Some updates do not apply; nothing was applied.
    #[error("rejected: {}", describe_rejections(.0))]
    Rejected(Vec<(RefName, RejectReason)>),
    /// The idempotency key was committed inside its window with another
    /// request digest.
    #[error("the idempotency key was used for another request")]
    IdempotencyMismatch,
    /// The transaction alone would exceed a segment (1 MiB).
    #[error("the transaction is larger than one segment")]
    TooLarge,
    /// The sequencer is shutting down, lost its lease, or could not learn
    /// whether the transaction committed; retry with the same key.
    #[error("the sequencer is unavailable; retry")]
    Unavailable,
    /// A request no sequencer accepts (ruling T10.1): a transaction with no
    /// updates or more than `MAX_REFS_PER_TXN` (4096), a pack named by an
    /// invalid checksum, or a read at a seq the log does not hold.
    #[error("invalid request: {reason}")]
    Invalid {
        /// What is wrong.
        reason: String,
    },
    /// The repository has no checkpoint: it was never created (ruling T10.1).
    #[error("repository not found")]
    NotFound,
    /// A checkpoint or segment that cannot be state (ruling T10.1).
    #[error(transparent)]
    State(#[from] StateError),
    /// The WAL failed.
    #[error(transparent)]
    Wal(#[from] WalError),
}

fn describe_rejections(rejected: &[(RefName, RejectReason)]) -> String {
    rejected
        .iter()
        .map(|(name, reason)| format!("{name}: {reason:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Which state a read sees (§36 §4.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadAt {
    /// The tail: linearizable.
    Latest,
    /// At least this seq: every commit up to it is reflected.
    AtLeast(Seq),
    /// Exactly the state after this seq (retained `EXACTLY_RETENTION`, 24 h).
    Exactly(Seq),
}

/// A committed transaction, as [`RefLog::watch`] streams it.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedTxn {
    /// Its segment.
    pub seq: Seq,
    /// Its index in the segment's batch.
    pub index: u32,
    /// The transaction's event.
    pub event: TxnEvent,
}

/// The principal of [`RefLog::cas_ref`]'s transactions.
pub const CAS_REF_PRINCIPAL: &str = "loams-cas-ref";

/// The request digest of a [`RefLog::cas_ref`] call: SHA-256 over the
/// update, so a retry with the same key and update replays and another
/// update under the key is a mismatch.
pub fn cas_ref_digest(name: &RefName, old: &Expect, new: Option<&ObjectId>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cas_ref\0");
    hash.update(name.as_str().as_bytes());
    match old {
        Expect::Oid(oid) => {
            hash.update(b"\0oid\0");
            hash.update(oid.as_bytes());
        }
        Expect::Absent => hash.update(b"\0absent"),
        Expect::Any => hash.update(b"\0any"),
    }
    match new {
        Some(oid) => {
            hash.update(b"\0new\0");
            hash.update(oid.as_bytes());
        }
        None => hash.update(b"\0delete"),
    }
    hash.finalize().into()
}

/// The ref log of one repository (§36 §5.2).
#[async_trait]
pub trait RefLog: Send + Sync + fmt::Debug {
    /// Applies every update or none, at one sequence number, through group commit.
    /// Linearizable: the transaction takes effect at its segment's PUT, between call and return.
    /// Acknowledged only after the segment is durable.
    async fn commit(&self, txn: RefTxn) -> Result<Receipt, RefError>;

    /// The draft's `cas_ref(name, old, new)`: one update, through `commit`,
    /// by [`CAS_REF_PRINCIPAL`] with [`cas_ref_digest`].
    async fn cas_ref(
        &self,
        name: RefName,
        old: Expect,
        new: Option<ObjectId>,
        key: IdemKey,
    ) -> Result<Receipt, RefError> {
        let request_digest = cas_ref_digest(&name, &old, new.as_ref());
        // A fixed segment of 13 printable ASCII bytes with no slash or
        // backslash: the parser's rules hold, so this cannot fail.
        let principal = Principal::parse(CAS_REF_PRINCIPAL).expect("a valid principal");
        self.commit(RefTxn {
            key,
            request_digest,
            updates: vec![RefUpdate {
                name,
                expect: old,
                new,
            }],
            packs: Vec::new(),
            principal,
            scope: None,
            trace: None,
            push_options: Vec::new(),
        })
        .await
    }

    /// `Latest` is linearizable; `AtLeast(s)` reflects every commit up to `s`; `Exactly(s)` is retained 24 h.
    async fn snapshot(&self, at: ReadAt) -> Result<Arc<RefSnapshot>, RefError>;

    /// Committed transactions from `from`, in order, without gaps; resumable by seq.
    fn watch(&self, from: Seq) -> BoxStream<'static, Result<CommittedTxn, RefError>>;
}
