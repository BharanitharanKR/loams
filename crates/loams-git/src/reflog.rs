//! The ref log's transaction types (§36 §5.2): what a writer asks the
//! sequencer to commit, and what it gets back.
//!
//! The `RefLog` trait and `BucketRefLog`, the sequencer over a
//! [`WalStore`](crate::WalStore), come with GT1 Task 10; the state machine
//! that checks and applies these transactions is [`crate::state`].

use std::collections::BTreeMap;

use crate::format::pb;
use crate::ids::{IdemKey, ObjectId, Principal, RefName, RepoId, RepoPath, Seq, TraceParent};

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
