//! The repository state machine and its idempotency index (§36 §4.4,
//! §4.5; plan GT1 Task 8, old GT1 Task 6).
//!
//! [`RepoState`] is the state after a seq: refs, symbolic refs, protection
//! rules, the live pack set, the fork parent and the idempotency index.
//! [`RepoState::check`] decides a transaction without changing anything;
//! the sequencer applies what it accepts to a copy of the state
//! ([`RepoState::apply_txn`]), so later transactions of the same group see
//! earlier ones, and replays committed segments with
//! [`RepoState::apply_segment`].
//!
//! `check` decides in this order:
//! 1. **Idempotency.** A key still inside its window answers its first
//!    receipt (`Replay`) when the request digest matches, and `Mismatch`
//!    otherwise.
//! 2. **Names.** A ref named twice in one transaction is `InvalidName`.
//! 3. **Expectations**, against the refs: `Stale { current }` or `Exists`.
//! 4. **Protections**: `deny_delete` refuses a delete, `deny_force` refuses
//!    an update that expects `Any`.
//!
//! The writer's scope (`OutOfScope`) is not checked yet: Task 16 adds
//! branch prefixes and GT4 adds cones.
//!
//! A configuration change (`HEAD`, protections) is decided by the
//! idempotency index alone ([`RepoState::check_key`]) and applied with
//! [`RepoState::apply_config`]; one that carries a request digest keeps its
//! key in the window like a ref transaction (GT1 Task 13, ruling T13.2).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::format::event::{TxnBody, from_cloudevent};
use crate::format::pb;
use crate::format::segment::WalSegment;
use crate::ids::{IdemKey, ObjectId, RefName, Seq};
use crate::limits::IDEMPOTENCY_WINDOW;
use crate::reflog::{Expect, Receipt, RefSnapshot, RefTxn, RefUpdate, RejectReason};

/// Errors of the state machine.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// A segment that does not directly follow the state's seq.
    #[error("segment {found} does not follow seq {current}")]
    Gap {
        /// The state's seq.
        current: Seq,
        /// The segment's seq.
        found: Seq,
    },
    /// An object format Loams does not serve (Ruling 8).
    #[error("object format {format} is not supported yet (Q388)")]
    UnsupportedFormat {
        /// The format's name.
        format: &'static str,
    },
    /// A checkpoint or segment whose contents cannot be state.
    #[error("corrupt repository state at seq {seq}: {reason}")]
    Corrupt {
        /// The checkpoint's or segment's seq.
        seq: Seq,
        /// What is wrong.
        reason: String,
    },
}

/// The decision on one transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Check {
    /// Every update applies.
    Accept,
    /// The key was committed inside its window with the same digest: answer
    /// the first receipt.
    Replay(Receipt),
    /// Some updates do not apply, so none does.
    Reject(Vec<(RefName, RejectReason)>),
    /// The key was committed inside its window with another digest.
    Mismatch,
}

/// One remembered idempotency key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IdemRecord {
    digest: [u8; 32],
    seq: Seq,
    index: u32,
    expires_unix_ms: i64,
}

/// Idempotency keys and the receipts they answer (§36 §4.5), each kept
/// until `expires_unix_ms`. Expired entries answer nothing; they are pruned
/// as time passes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdemIndex {
    by_key: HashMap<IdemKey, IdemRecord>,
    by_expiry: BTreeSet<(i64, IdemKey)>,
}

impl IdemIndex {
    /// The entries held, expired ones not yet pruned included.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether no entry is held.
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    fn get(&self, key: &IdemKey, now_unix_ms: i64) -> Option<&IdemRecord> {
        self.by_key
            .get(key)
            .filter(|record| now_unix_ms < record.expires_unix_ms)
    }

    fn insert(&mut self, key: IdemKey, record: IdemRecord) {
        if let Some(old) = self.by_key.insert(key.clone(), record) {
            self.by_expiry.remove(&(old.expires_unix_ms, key.clone()));
        }
        self.by_expiry.insert((record.expires_unix_ms, key));
    }

    /// Drops every entry expired at `now_unix_ms`.
    fn prune(&mut self, now_unix_ms: i64) {
        while let Some((expires, _)) = self.by_expiry.first() {
            if *expires > now_unix_ms {
                break;
            }
            if let Some((_, key)) = self.by_expiry.pop_first() {
                self.by_key.remove(&key);
            }
        }
    }

    /// Entries still valid at `now_unix_ms`, sorted by key.
    fn live(&self, now_unix_ms: i64) -> Vec<pb::IdemEntry> {
        let mut entries: Vec<pb::IdemEntry> = self
            .by_key
            .iter()
            .filter(|(_, record)| now_unix_ms < record.expires_unix_ms)
            .map(|(key, record)| pb::IdemEntry {
                key: key.as_str().to_string(),
                request_digest: record.digest.to_vec(),
                seq: record.seq.0,
                index: record.index,
                expires_unix_ms: record.expires_unix_ms,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries
    }
}

/// The idempotency window in ms.
fn window_ms() -> i64 {
    i64::try_from(IDEMPOTENCY_WINDOW.as_millis()).unwrap_or(i64::MAX)
}

/// Whether `name` matches a protection `pattern`: equal, or with each `*`
/// matching any run of characters (`/` included), as in a refspec.
pub fn pattern_matches(pattern: &str, name: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = name.strip_prefix(first) else {
        return false;
    };
    let parts: Vec<&str> = parts.collect();
    let Some((last, middle)) = parts.split_last() else {
        // No `*`: the whole name must match.
        return rest.is_empty();
    };
    for part in middle {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

/// A decoded, validated ref transaction, ready to apply.
struct ParsedTxn {
    updates: Vec<(RefName, Option<ObjectId>)>,
    packs: Vec<pb::PackRef>,
}

/// One validated segment event.
enum Op {
    Ref {
        key: IdemKey,
        digest: [u8; 32],
        time_unix_ms: i64,
        txn: ParsedTxn,
    },
    PackSet(pb::PackSetChange),
    Config {
        key: IdemKey,
        digest: Option<[u8; 32]>,
        time_unix_ms: i64,
        symrefs: BTreeMap<String, RefName>,
        protections: Vec<pb::Protection>,
    },
}

/// A configuration change's request digest: `None` when it has none (no
/// idempotency entry), an error when it is not 32 bytes.
fn config_digest(change: &pb::ConfigChange) -> Result<Option<[u8; 32]>, String> {
    if change.request_digest.is_empty() {
        Ok(None)
    } else {
        parse_digest(&change.request_digest).map(Some)
    }
}

fn parse_oid(bytes: &[u8]) -> Result<ObjectId, String> {
    ObjectId::try_from(bytes).map_err(|_| format!("{} bytes are not a SHA-1 id", bytes.len()))
}

fn parse_ref(name: &str) -> Result<RefName, String> {
    RefName::parse(name).map_err(|err| err.to_string())
}

/// A symbolic ref's name: `HEAD` or a ref name.
fn parse_symref_name(name: &str) -> Result<String, String> {
    if name == "HEAD" {
        Ok(name.to_string())
    } else {
        parse_ref(name).map(|name| name.as_str().to_string())
    }
}

fn parse_symrefs(symrefs: &[pb::SymRef]) -> Result<BTreeMap<String, RefName>, String> {
    symrefs
        .iter()
        .map(|symref| Ok((parse_symref_name(&symref.name)?, parse_ref(&symref.target)?)))
        .collect()
}

fn parse_digest(bytes: &[u8]) -> Result<[u8; 32], String> {
    bytes
        .try_into()
        .map_err(|_| format!("a request digest of {} bytes", bytes.len()))
}

/// The protobuf enum's value, refusing any format but SHA-1.
fn parse_format(value: i32, seq: Seq) -> Result<pb::ObjectFormat, StateError> {
    match pb::ObjectFormat::try_from(value) {
        Ok(pb::ObjectFormat::Sha1) => Ok(pb::ObjectFormat::Sha1),
        Ok(pb::ObjectFormat::Sha256) => Err(StateError::UnsupportedFormat { format: "sha256" }),
        Ok(pb::ObjectFormat::Unspecified) | Err(_) => Err(StateError::Corrupt {
            seq,
            reason: format!("object format {value}"),
        }),
    }
}

/// The repository's state after `seq` (§36 §4.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoState {
    /// The last applied segment.
    pub seq: Seq,
    /// The repository's object format (SHA-1 only, Ruling 8).
    pub format: pb::ObjectFormat,
    /// Every ref.
    pub refs: BTreeMap<RefName, ObjectId>,
    /// Symbolic refs: `HEAD` or a ref name, to the ref it points at.
    pub symrefs: BTreeMap<String, RefName>,
    /// Protection rules.
    pub protections: Vec<pb::Protection>,
    /// The live pack set.
    pub packs: Vec<pb::PackRef>,
    /// The fork parent.
    pub parent: Option<pb::ForkParent>,
    idem: IdemIndex,
}

impl RepoState {
    /// The state of a new repository: seq 0, no refs.
    pub fn empty(format: pb::ObjectFormat) -> Self {
        Self {
            seq: Seq::ZERO,
            format,
            refs: BTreeMap::new(),
            symrefs: BTreeMap::new(),
            protections: Vec::new(),
            packs: Vec::new(),
            parent: None,
            idem: IdemIndex::default(),
        }
    }

    /// The idempotency index.
    pub fn idempotency(&self) -> &IdemIndex {
        &self.idem
    }

    /// The state a checkpoint holds. Refuses a SHA-256 repository
    /// (`object format sha256 is not supported yet (Q388)`) and any
    /// malformed ref, symbolic ref or idempotency entry.
    pub fn from_checkpoint(c: pb::Checkpoint) -> Result<Self, StateError> {
        let seq = Seq(c.seq);
        let corrupt = |reason: String| StateError::Corrupt { seq, reason };
        let format = parse_format(c.object_format, seq)?;
        let refs = c
            .refs
            .iter()
            .map(|(name, oid)| Ok((parse_ref(name)?, parse_oid(oid)?)))
            .collect::<Result<_, String>>()
            .map_err(corrupt)?;
        let symrefs = parse_symrefs(&c.symrefs).map_err(corrupt)?;
        let mut idem = IdemIndex::default();
        for entry in &c.idempotency {
            let key = IdemKey::parse(&entry.key).map_err(|err| corrupt(err.to_string()))?;
            let digest = parse_digest(&entry.request_digest).map_err(corrupt)?;
            idem.insert(
                key,
                IdemRecord {
                    digest,
                    seq: Seq(entry.seq),
                    index: entry.index,
                    expires_unix_ms: entry.expires_unix_ms,
                },
            );
        }
        Ok(Self {
            seq,
            format,
            refs,
            symrefs,
            protections: c.protections,
            packs: c.packs,
            parent: c.parent,
            idem,
        })
    }

    /// The checkpoint of this state, with the idempotency entries still
    /// inside their window at `now_unix_ms` only. Equal states give equal
    /// checkpoints (refs, symbolic refs and entries are in name order).
    pub fn to_checkpoint(&self, now_unix_ms: i64) -> pb::Checkpoint {
        pb::Checkpoint {
            seq: self.seq.0,
            object_format: self.format as i32,
            refs: self
                .refs
                .iter()
                .map(|(name, oid)| (name.as_str().to_string(), oid.as_bytes().to_vec()))
                .collect(),
            symrefs: self
                .symrefs
                .iter()
                .map(|(name, target)| pb::SymRef {
                    name: name.clone(),
                    target: target.as_str().to_string(),
                })
                .collect(),
            protections: self.protections.clone(),
            packs: self.packs.clone(),
            parent: self.parent.clone(),
            idempotency: self.idem.live(now_unix_ms),
        }
    }

    /// Decides `txn` against this state at `now_unix_ms`. Pure.
    pub fn check(&self, txn: &RefTxn, now_unix_ms: i64) -> Check {
        if let Some(answer) = self.check_key(&txn.key, &txn.request_digest, now_unix_ms) {
            return answer;
        }
        let mut counts: HashMap<&RefName, usize> = HashMap::new();
        for update in &txn.updates {
            *counts.entry(&update.name).or_default() += 1;
        }
        let mut reported: HashSet<&RefName> = HashSet::new();
        let mut rejected = Vec::new();
        for update in &txn.updates {
            let name = &update.name;
            if counts[name] > 1 {
                if reported.insert(name) {
                    rejected.push((name.clone(), RejectReason::InvalidName));
                }
                continue;
            }
            if let Some(reason) = self.refuse(update) {
                rejected.push((name.clone(), reason));
            }
        }
        if rejected.is_empty() {
            Check::Accept
        } else {
            Check::Reject(rejected)
        }
    }

    /// The idempotency answer for `key` with `digest` at `now_unix_ms`:
    /// `Replay` with the first receipt or `Mismatch` while the key is inside
    /// its window, `None` otherwise. Pure.
    pub fn check_key(&self, key: &IdemKey, digest: &[u8; 32], now_unix_ms: i64) -> Option<Check> {
        let record = self.idem.get(key, now_unix_ms)?;
        if record.digest != *digest {
            return Some(Check::Mismatch);
        }
        Some(Check::Replay(Receipt {
            seq: record.seq,
            index: record.index,
            replayed: true,
        }))
    }

    /// Applies a configuration change the sequencer accepted, as entry
    /// `index` of segment `seq` at `now_unix_ms` (the time its event
    /// carries): the symbolic refs and protections it lists replace the
    /// current ones, and a change with a request digest keeps `key` in the
    /// idempotency window (ruling T13.2). Validates first: an error changes
    /// nothing.
    pub fn apply_config(
        &mut self,
        change: &pb::ConfigChange,
        key: &IdemKey,
        seq: Seq,
        index: u32,
        now_unix_ms: i64,
    ) -> Result<(), StateError> {
        let corrupt = |reason: String| StateError::Corrupt { seq, reason };
        let symrefs = parse_symrefs(&change.symrefs).map_err(corrupt)?;
        let digest = config_digest(change).map_err(corrupt)?;
        let entry = digest.map(|digest| (key, digest));
        self.set_config(
            symrefs,
            change.protections.clone(),
            entry,
            seq,
            index,
            now_unix_ms,
        );
        Ok(())
    }

    /// Replaces the configuration; with `entry`, keeps its key and digest in
    /// the idempotency window as entry `index` of `seq`.
    fn set_config(
        &mut self,
        symrefs: BTreeMap<String, RefName>,
        protections: Vec<pb::Protection>,
        entry: Option<(&IdemKey, [u8; 32])>,
        seq: Seq,
        index: u32,
        now_unix_ms: i64,
    ) {
        self.symrefs = symrefs;
        self.protections = protections;
        if let Some((key, digest)) = entry {
            self.idem.prune(now_unix_ms);
            self.idem.insert(
                key.clone(),
                IdemRecord {
                    digest,
                    seq,
                    index,
                    expires_unix_ms: now_unix_ms.saturating_add(window_ms()),
                },
            );
        }
    }

    /// Why one update does not apply, if it does not.
    fn refuse(&self, update: &RefUpdate) -> Option<RejectReason> {
        let current = self.refs.get(&update.name).copied();
        match update.expect {
            Expect::Oid(oid) if current != Some(oid) => {
                return Some(RejectReason::Stale { current });
            }
            Expect::Absent if current.is_some() => return Some(RejectReason::Exists),
            _ => {}
        }
        let protected = self
            .protections
            .iter()
            .filter(|rule| pattern_matches(&rule.pattern, update.name.as_str()))
            .any(|rule| {
                (rule.deny_delete && update.new.is_none())
                    || (rule.deny_force && matches!(update.expect, Expect::Any))
            });
        protected.then_some(RejectReason::Protected)
    }

    /// Applies a transaction the sequencer accepted, as entry `index` of
    /// segment `seq`. `now_unix_ms` must be the time its event carries, so a
    /// replay of the segment gives the same idempotency expiry. Validates
    /// the whole transaction before changing anything.
    pub fn apply_txn(
        &mut self,
        txn: &pb::RefTransaction,
        key: &IdemKey,
        digest: [u8; 32],
        seq: Seq,
        index: u32,
        now_unix_ms: i64,
    ) -> Result<(), StateError> {
        let parsed = self.parse_txn(txn, seq)?;
        self.apply_parsed(parsed, key.clone(), digest, seq, index, now_unix_ms);
        Ok(())
    }

    fn parse_txn(&self, txn: &pb::RefTransaction, seq: Seq) -> Result<ParsedTxn, StateError> {
        let corrupt = |reason: String| StateError::Corrupt { seq, reason };
        if parse_format(txn.object_format, seq)? != self.format {
            return Err(corrupt("a transaction in another object format".into()));
        }
        let updates = txn
            .updates
            .iter()
            .map(|update| {
                let name = parse_ref(&update.name)?;
                let new = if update.new_oid.is_empty() {
                    None
                } else {
                    Some(parse_oid(&update.new_oid)?)
                };
                Ok((name, new))
            })
            .collect::<Result<_, String>>()
            .map_err(corrupt)?;
        Ok(ParsedTxn {
            updates,
            packs: txn.packs.clone(),
        })
    }

    fn apply_parsed(
        &mut self,
        txn: ParsedTxn,
        key: IdemKey,
        digest: [u8; 32],
        seq: Seq,
        index: u32,
        now_unix_ms: i64,
    ) {
        for (name, new) in txn.updates {
            match new {
                Some(oid) => self.refs.insert(name, oid),
                None => self.refs.remove(&name),
            };
        }
        self.add_packs(txn.packs);
        self.idem.prune(now_unix_ms);
        self.idem.insert(
            key,
            IdemRecord {
                digest,
                seq,
                index,
                expires_unix_ms: now_unix_ms.saturating_add(window_ms()),
            },
        );
    }

    fn add_packs(&mut self, packs: Vec<pb::PackRef>) {
        for pack in packs {
            if !self.packs.iter().any(|p| p.checksum == pack.checksum) {
                self.packs.push(pack);
            }
        }
    }

    /// Applies committed segment `seg`, which must be `self.seq + 1`: its
    /// ref transactions (with their idempotency entries), pack-set changes
    /// and configuration changes, in batch order. Validates every event
    /// first, so an error leaves the state unchanged.
    pub fn apply_segment(&mut self, seg: &WalSegment) -> Result<(), StateError> {
        if seg.seq != self.seq.next() {
            return Err(StateError::Gap {
                current: self.seq,
                found: seg.seq,
            });
        }
        let corrupt = |index: usize, reason: String| StateError::Corrupt {
            seq: seg.seq,
            reason: format!("event {index}: {reason}"),
        };
        let mut ops = Vec::with_capacity(seg.batch.events.len());
        for (index, event) in seg.batch.events.iter().enumerate() {
            let event = from_cloudevent(event).map_err(|err| corrupt(index, err.to_string()))?;
            ops.push(match event.body {
                TxnBody::RefTxn(txn) => Op::Ref {
                    digest: parse_digest(&txn.request_digest).map_err(|e| corrupt(index, e))?,
                    txn: self.parse_txn(&txn, seg.seq)?,
                    key: event.key,
                    time_unix_ms: event.time_unix_ms,
                },
                TxnBody::PackSet(change) => Op::PackSet(change),
                TxnBody::Config(change) => Op::Config {
                    symrefs: parse_symrefs(&change.symrefs).map_err(|e| corrupt(index, e))?,
                    digest: config_digest(&change).map_err(|e| corrupt(index, e))?,
                    protections: change.protections,
                    key: event.key,
                    time_unix_ms: event.time_unix_ms,
                },
            });
        }
        let index_of = |index: usize| {
            u32::try_from(index).map_err(|_| corrupt(index, "too many events".into()))
        };
        // Every index fits: checked before anything changes.
        index_of(ops.len())?;
        for (index, op) in ops.into_iter().enumerate() {
            match op {
                Op::Ref {
                    key,
                    digest,
                    time_unix_ms,
                    txn,
                } => {
                    let index = index_of(index)?;
                    self.apply_parsed(txn, key, digest, seg.seq, index, time_unix_ms);
                }
                Op::PackSet(change) => {
                    self.packs
                        .retain(|pack| !change.removed.contains(&pack.checksum));
                    self.add_packs(change.added);
                }
                Op::Config {
                    key,
                    digest,
                    time_unix_ms,
                    symrefs,
                    protections,
                } => {
                    let index = index_of(index)?;
                    self.set_config(
                        symrefs,
                        protections,
                        digest.map(|digest| (&key, digest)),
                        seg.seq,
                        index,
                        time_unix_ms,
                    );
                }
            }
        }
        self.seq = seg.seq;
        Ok(())
    }

    /// The refs, configuration and pack set, for readers.
    pub fn snapshot(&self) -> RefSnapshot {
        RefSnapshot {
            seq: self.seq,
            refs: self.refs.clone(),
            symrefs: self.symrefs.clone(),
            protections: self.protections.clone(),
            packs: self.packs.clone(),
            parent: self.parent.clone(),
        }
    }
}
