//! The durable write path (GR1 Task 11; design §48 §6, §7.3; D742).
//!
//! Every committed write transaction becomes one [`GraphRecord`] in the graph's implicit stream in
//! Loams' log and is acknowledged after that append (I1). The pieces:
//!
//! * [`lane`]: the per-graph write lane. One transaction at a time; several queued ones form a
//!   group that is appended in one log batch (§48 §6.2 steps 1–8, group commit).
//! * [`visibility`]: the per-graph gate that keeps every non-durable write from every client
//!   (I2, R9.3), and client sessions pinned at the durable state.
//! * [`append`]: [`GraphLog`], the log as the graphs use it: append a group, read the stream back.
//! * [`idempotency`]: `key → first run`, rebuilt from the stream on open.
//!
//! A graph's local Grafeo files are a cache (I4). [`crate::recovery`] (GR1 Task 14) chooses what
//! an engine starts from, its [`Base`]: the local cache a clean close left, the manifest's newest
//! usable snapshot, or nothing (the whole stream). [`Durable::open`] then replays the records
//! after the base. A graph whose append failed is rebuilt the same way before it serves again,
//! never from its local files.

pub mod append;
pub mod faults;
pub mod idempotency;
pub mod lane;
pub mod visibility;

use std::fmt;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use grafeo::GrafeoDB;
use loams_common::StreamId;
use loams_log::OffsetRecord;

pub use append::{AppendFault, GraphLog, GroupCommit, is_fenced, stream_name};
pub use visibility::Visibility;

use crate::GraphId;
use crate::changeset::{
    ChangeOp, Counters, GraphChangeSet, GraphRecord, PropMap, apply_to_store, decode, encode,
};
use crate::engine::GraphError;
use idempotency::Idempotency;
use lane::WriteLane;
use visibility::Position;

/// Where a write is durable, as `loams.graph.v1` answers it (D76's shape over the graph's stream,
/// one partition; §48 §7.2): the stream and the offset after the write's record. A read that
/// waits until the graph has applied every record below `offset` sees the write.
///
/// The text form is `loams.collection.v1`'s (`v1:s<stream>/p0@<offset>`), so one
/// `loams-consistency-token` header serves both (R11.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConsistencyToken {
    /// The graph's implicit stream.
    pub stream: StreamId,
    /// The offset after the write's record: the graph's version once the write is durable.
    pub offset: u64,
}

impl fmt::Display for ConsistencyToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v1:s{}/p0@{}", self.stream, self.offset)
    }
}

impl FromStr for ConsistencyToken {
    type Err = GraphError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || GraphError::InvalidValue(format!("{text:?} is not a graph token"));
        let body = text.strip_prefix("v1:s").ok_or_else(invalid)?;
        let (stream, offset) = body.split_once("/p0@").ok_or_else(invalid)?;
        Ok(Self {
            stream: stream.parse().map_err(|_| invalid())?,
            offset: canonical(offset).ok_or_else(invalid)?,
        })
    }
}

/// A canonical decimal `u64`: digits, no sign, no leading zero.
fn canonical(text: &str) -> Option<u64> {
    let ok = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    ok.then(|| text.parse().ok()).flatten()
}

/// A transaction's isolation (`ExecuteBatchRequest.isolation`, §48 §7.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Isolation {
    /// Snapshot isolation (Grafeo's default).
    #[default]
    Snapshot,
    /// Serializable snapshot isolation.
    Serializable,
}

/// What a write asks of the lane besides its statements (shared contract `ExecOptions`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// Answer an earlier run of this key instead of running again (§48 §7.3).
    pub idempotency_key: Option<String>,
    /// The transaction's isolation.
    pub isolation: Isolation,
    /// Run only if the graph's version (its token offset) is this one (`graph_version_mismatch`).
    pub if_version: Option<u64>,
}

/// What a durable write answers besides its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// Where the write is durable.
    pub token: ConsistencyToken,
    /// Grafeo's commit epoch (informational, R9.5; 0 for a transaction that wrote no record).
    pub commit_epoch: u64,
    /// What the write changed.
    pub counters: Counters,
    /// Answered from the idempotency index: the statement did not run again.
    pub replayed: bool,
}

/// How a graph's engine is opened durably (part of [`crate::OpenSpec`]).
#[derive(Clone)]
pub struct DurableSpec {
    /// The log.
    pub log: GraphLog,
    /// The graph's implicit stream.
    pub stream: StreamId,
    /// The catalog record had no stream yet: a graph from before the log keeps its local files
    /// and appends them as its first record, if its stream is still empty (R11.5). Cleared once
    /// the graph has opened, so a reload never trusts local files.
    pub adopt: bool,
    /// The owner lease this engine serves under (GR1 Task 12): every append carries it, and the
    /// metastore refuses one once the lease has moved on. `None` appends unfenced (tests of the
    /// write path alone).
    pub fence: Option<loams_common::meta::Fence>,
    /// Where the graph's snapshots are read from when its stream has been trimmed below them
    /// (GR1 Task 13). `None` opens only an untrimmed stream.
    pub snapshots: Option<crate::snapshot::SnapshotSource>,
}

impl fmt::Debug for DurableSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableSpec")
            .field("stream", &self.stream)
            .field("adopt", &self.adopt)
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

/// Two specs agree when they name the same stream under the same lease epoch.
impl PartialEq for DurableSpec {
    fn eq(&self, other: &Self) -> bool {
        self.stream == other.stream && self.fence == other.fence
    }
}

impl Eq for DurableSpec {}

/// Grafeo's CDC retention for a durable graph (R9.9): 64 epochs and no event count, since a
/// count limit could prune a large commit before its capture. The lane runs `gc` after every
/// group, under the gate.
pub(crate) fn cdc_retention() -> grafeo::cdc::CdcRetentionConfig {
    grafeo::cdc::CdcRetentionConfig {
        max_epochs: Some(64),
        max_events: None,
    }
}

/// A durable graph's write path state.
pub(crate) struct Durable {
    pub(crate) log: GraphLog,
    pub(crate) graph: GraphId,
    pub(crate) stream: StreamId,
    /// The owner lease every append carries (GR1 Task 12).
    pub(crate) fence: Option<loams_common::meta::Fence>,
    pub(crate) lane: WriteLane,
    pub(crate) position: Position,
    pub(crate) idempotency: Mutex<Idempotency>,
    /// The next record's `txn_seq` (one per record of either kind, R10.4). Changed only by the
    /// lane, under the exclusive gate.
    pub(crate) next_txn_seq: AtomicU64,
    /// Bytes of records this engine appended since its last snapshot (the checkpointer's
    /// "64 MiB of log" trigger, GR1 Task 13).
    pub(crate) log_bytes: AtomicU64,
    /// When this engine opened (the checkpointer's interval for a graph with no snapshot yet).
    pub(crate) opened_at: std::time::Instant,
    /// How many records its open applied on top of its base (GR1 Task 14's `Recovery`).
    pub(crate) replayed: u64,
}

/// What a durable engine was restored from before its replay: a snapshot (GR1 Task 13) or a
/// local cache (GR1 Task 14). The stream's records below `offset` are in the engine already, and
/// `next_txn_seq` is the base's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Base {
    /// The offset the base covers.
    pub(crate) offset: u64,
    /// The `txn_seq` the base's next record takes.
    pub(crate) next_txn_seq: u64,
    /// The idempotency index at `offset`, when the base kept it (a local cache does): the
    /// records below `offset` are then not read at all. `None` (a snapshot) reads them for their
    /// keys.
    pub(crate) keys: Option<Vec<(String, u64, idempotency::Outcome)>>,
}

impl fmt::Debug for Durable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Durable")
            .field("graph", &self.graph)
            .field("stream", &self.stream)
            .field("offset", &self.position.offset())
            .finish_non_exhaustive()
    }
}

impl Durable {
    /// The token of the durable state.
    pub(crate) fn token(&self) -> ConsistencyToken {
        ConsistencyToken {
            stream: self.stream,
            offset: self.position.offset(),
        }
    }

    /// Opens a durable graph's state over `db`, a fresh engine (or, when `adopt`, the graph's
    /// local files from before the log; or, with `base`, an engine restored from a snapshot or a
    /// local cache): replays the stream `from..to` into it, or appends the adopted state as the
    /// first record, and checkpoints it (R9.5). With `base`, the records below its offset are
    /// only read for their idempotency keys and `txn_seq`s, since the base holds their effect,
    /// and not even read when the base kept its keys.
    ///
    /// # Errors
    ///
    /// [`GraphError::LogUnavailable`] when the stream cannot be read or appended to, and
    /// [`GraphError::Engine`] when a record does not decode or apply.
    pub(crate) fn open(
        db: &GrafeoDB,
        spec: &DurableSpec,
        graph: GraphId,
        bounds: (u64, u64),
        adopt: bool,
        base: Option<Base>,
        persistent: bool,
    ) -> Result<Self, GraphError> {
        let (start, end) = bounds;
        let log = spec.log.clone();
        let now = log.now_ms();
        let mut idempotency = Idempotency::default();
        let mut next_txn_seq = base.as_ref().map_or(1, |base| base.next_txn_seq);
        let applied_below = base.as_ref().map_or(0, |base| base.offset);
        // A base that kept its keys needs nothing below its offset.
        let mut read_from = start;
        if let Some(keys) = base.as_ref().and_then(|base| base.keys.as_ref()) {
            for (key, recorded, outcome) in keys {
                idempotency.insert(key, *outcome, *recorded, now);
            }
            read_from = applied_below.max(start);
        }
        let mut offset = end;
        let mut replayed = 0;
        if adopt {
            let ops = baseline(db);
            if !ops.is_empty() {
                let record = GraphRecord::ChangeSet(GraphChangeSet {
                    graph,
                    lease_epoch: spec.fence.as_ref().map_or(0, |fence| fence.epoch),
                    commit_epoch: db.current_epoch().as_u64(),
                    txn_seq: next_txn_seq,
                    idempotency_key: None,
                    link: None,
                    ops,
                });
                let bytes = encode(&record).map_err(|err| {
                    GraphError::engine(format!("adopting the graph's local files: {err}"))
                })?;
                let ack = log
                    .append(
                        spec.stream,
                        vec![lane::log_record(bytes, now)],
                        spec.fence.as_ref(),
                    )
                    .map_err(|err| {
                        tracing::warn!(error = %err, "appending an adopted graph failed");
                        GraphError::LogUnavailable(
                            "appending the graph's local state to its stream failed; retry"
                                .to_string(),
                        )
                    })?;
                next_txn_seq += 1;
                offset = ack.last_offset + 1;
            }
        } else {
            if start > applied_below {
                // Trimmed above the snapshot the engine holds: records in between are lost. The
                // checkpointer trims only at or below a snapshot it has committed (GR1 Task 13).
                return Err(GraphError::LogUnavailable(format!(
                    "the graph's stream starts at offset {start}, above the {applied_below} its \
                     snapshot covers"
                )));
            }
            for record in log.read(spec.stream, read_from, end)? {
                // Below the snapshot's offset the engine holds the record's effect already.
                let apply = record.offset >= applied_below;
                let (seq, applied) =
                    replay_record(db, graph, &record, apply, &mut idempotency, now)?;
                next_txn_seq = next_txn_seq.max(seq + 1);
                replayed += u64::from(applied);
            }
        }
        // Replayed writes bypass Grafeo's WAL (R9.5): checkpoint before the local files stand for
        // anything. They stand for an offset only once a clean close stamps them (GR1 Task 14).
        if persistent {
            db.wal_checkpoint().map_err(|err| {
                GraphError::engine(format!("checkpointing the rebuilt graph: {err}"))
            })?;
        }
        let counts = (db.node_count() as u64, db.edge_count() as u64);
        Ok(Self {
            graph,
            stream: spec.stream,
            fence: spec.fence.clone(),
            lane: WriteLane::default(),
            position: Position::new(offset, db.current_epoch().as_u64(), counts),
            idempotency: Mutex::new(idempotency),
            next_txn_seq: AtomicU64::new(next_txn_seq),
            log_bytes: AtomicU64::new(0),
            opened_at: std::time::Instant::now(),
            replayed,
            log,
        })
    }

    /// Applies the records a follower fetched from its stream (GR1 Task 15): every record from
    /// its position on, in order, with their idempotency keys and `txn_seq`s, then publishes the
    /// new position, which wakes every read waiting for a token. Records below the position are
    /// skipped. The caller holds the gate exclusively.
    ///
    /// # Errors
    ///
    /// As a replay's, and [`GraphError::Engine`] for a gap in the records.
    pub(crate) fn follow(
        &self,
        db: &GrafeoDB,
        records: &[OffsetRecord],
    ) -> Result<u64, GraphError> {
        let mut offset = self.position.offset();
        let now = self.log.now_ms();
        let mut applied = 0;
        let mut idempotency = self
            .idempotency
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for record in records {
            // Below the position: a fetch that overlapped the last one.
            if record.offset < offset {
                continue;
            }
            if record.offset != offset {
                return Err(GraphError::engine(format!(
                    "the graph's stream skipped from offset {offset} to {}",
                    record.offset
                )));
            }
            let (seq, _) = replay_record(db, self.graph, record, true, &mut idempotency, now)?;
            self.next_txn_seq.fetch_max(seq + 1, Ordering::AcqRel);
            offset = record.offset + 1;
            applied += 1;
        }
        if applied > 0 {
            self.position.publish(
                offset,
                db.current_epoch().as_u64(),
                (db.node_count() as u64, db.edge_count() as u64),
            );
        }
        Ok(applied)
    }

    /// The idempotency index, for the stamp a clean close leaves beside the local files (GR1
    /// Task 14).
    pub(crate) fn idempotency_entries(&self) -> Vec<(String, u64, idempotency::Outcome)> {
        self.idempotency
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries()
    }

    /// The lease epoch this engine's appends carry (0 when unfenced).
    pub(crate) fn lease_epoch(&self) -> u64 {
        self.fence.as_ref().map_or(0, |fence| fence.epoch)
    }

    /// The next `txn_seq`, taken. Only the lane calls it, under the exclusive gate. A group that
    /// fails leaves a gap here, which does not matter: the graph then reloads, and the reloaded
    /// state counts on from its stream's last record.
    pub(crate) fn take_txn_seq(&self) -> u64 {
        self.next_txn_seq.fetch_add(1, Ordering::AcqRel)
    }
}

/// Reads one record of graph `graph`'s stream: applies it to `db` when `apply` (a record a base
/// already holds is only read), and records its idempotency key. Answers its `txn_seq` and
/// whether it was applied. Shared by an open's replay and a follower's tail (GR1 Task 15).
fn replay_record(
    db: &GrafeoDB,
    graph: GraphId,
    record: &OffsetRecord,
    apply: bool,
    idempotency: &mut Idempotency,
    now: u64,
) -> Result<(u64, bool), GraphError> {
    let value = record.record.value.as_deref().unwrap_or_default();
    let decoded = decode(value).map_err(|err| {
        GraphError::engine(format!(
            "record {} of the graph's stream: {err}",
            record.offset
        ))
    })?;
    let (seq, key, epoch) = match &decoded {
        GraphRecord::ChangeSet(change) => {
            if change.graph != graph {
                return Err(GraphError::engine(format!(
                    "record {} of the graph's stream belongs to {}",
                    record.offset, change.graph
                )));
            }
            if apply {
                apply_to_store(db, &change.ops)?;
            }
            (change.txn_seq, &change.idempotency_key, change.commit_epoch)
        }
        GraphRecord::Schema(schema) => {
            if apply {
                // DDL is refused until Task 24 (R5.8), so no stream holds one yet; a record
                // that does is replayed by running it, as §48 §6.3 says.
                let session = db.session_with_role(grafeo_engine::auth::Role::Admin);
                session.execute(&schema.statement).map_err(|err| {
                    GraphError::engine(format!("replaying record {}: {err}", record.offset))
                })?;
            }
            (schema.txn_seq, &schema.idempotency_key, schema.commit_epoch)
        }
    };
    if let Some(key) = key {
        let recorded = u64::try_from(record.record.timestamp_ms).unwrap_or(0);
        idempotency.insert(
            key,
            idempotency::Outcome {
                token_offset: record.offset + 1,
                commit_epoch: epoch,
            },
            recorded,
            now,
        );
    }
    Ok((seq, apply))
}

/// The whole graph as one change set: every node, then every edge (R11.5).
fn baseline(db: &GrafeoDB) -> Vec<ChangeOp> {
    let store = db.store();
    let mut nodes: Vec<ChangeOp> = store
        .all_nodes()
        .map(|node| {
            let mut labels: Vec<String> = node.labels.iter().map(ToString::to_string).collect();
            labels.sort();
            ChangeOp::UpsertNode {
                id: node.id.as_u64(),
                labels,
                props: props_of(node.properties.iter()),
            }
        })
        .collect();
    nodes.sort_by_key(op_id);
    let mut edges: Vec<ChangeOp> = store
        .all_edges()
        .map(|edge| ChangeOp::UpsertEdge {
            id: edge.id.as_u64(),
            ty: edge.edge_type.to_string(),
            src: edge.src.as_u64(),
            dst: edge.dst.as_u64(),
            props: props_of(edge.properties.iter()),
        })
        .collect();
    edges.sort_by_key(op_id);
    nodes.extend(edges);
    nodes
}

fn props_of<'a>(
    properties: impl Iterator<Item = (&'a grafeo_common::types::PropertyKey, &'a grafeo::Value)>,
) -> PropMap {
    properties
        .map(|(key, value)| (key.as_str().to_string(), value.clone()))
        .collect()
}

fn op_id(op: &ChangeOp) -> u64 {
    match op {
        ChangeOp::UpsertNode { id, .. }
        | ChangeOp::DeleteNode { id }
        | ChangeOp::UpsertEdge { id, .. }
        | ChangeOp::DeleteEdge { id }
        | ChangeOp::SetProps { id, .. } => *id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_round_trip_in_the_collection_form() {
        let token = ConsistencyToken {
            stream: StreamId(12),
            offset: 340,
        };
        assert_eq!(token.to_string(), "v1:s12/p0@340");
        assert_eq!(
            "v1:s12/p0@340".parse::<ConsistencyToken>().ok(),
            Some(token)
        );
        for bad in [
            "",
            "v1:s12/p1@3",
            "v1:s12/p0@03",
            "v1:s/p0@3",
            "v2:s1/p0@1",
            "v1:s1/p0@-1",
        ] {
            assert!(bad.parse::<ConsistencyToken>().is_err(), "{bad:?}");
        }
    }
}
