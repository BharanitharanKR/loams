//! The link target of a LINKED graph's sources (GR1 Tasks 18 and 19): `loams-link`'s
//! [`LinkTarget`] for one collection or stream feeding the graph.
//!
//! Its `load` answers the link's progress from the graph's durable state. Its `commit` turns the
//! batch into change operations ([`super::collection::Plan`], [`super::stream::Plan`]) and
//! appends them, with the batch's offsets, as one record of the graph's stream
//! ([`crate::engine::Graph::commit_link`]). A stream record a rule cannot read is written to a
//! dead-letter object before the record that skips it (R19.3).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use loams_common::NamespaceId;
use loams_common::meta::{Fence, LinkId};
use loams_link::{ApplyBatch, CommitError, LinkError, LinkTarget, TargetState};
use loams_log::OffsetRecord;

use super::{collection, stream};
use crate::catalog::GraphMeta;
use crate::changeset::LinkApply;
use crate::engine::{Graph, GraphError};
use crate::service::admin::GraphAdmin;

/// The most source records one record of the graph's stream applies: a larger batch is
/// committed in several records, each with its own offsets.
pub const CHUNK_RECORDS: usize = 2_048;

/// The most source bytes (record values) one record of the graph's stream applies, well below
/// the 32 MiB a graph record may hold.
pub const CHUNK_BYTES: usize = 4 << 20;

/// How a source's records become change operations.
#[derive(Debug, Clone)]
pub(crate) enum SourcePlan {
    /// A collection's `DocOp`s (GR1 Task 18).
    Collection(Arc<collection::Plan>),
    /// A stream's JSON or CloudEvents records (GR1 Task 19).
    Stream(Arc<stream::Plan>),
}

/// One source of a LINKED graph, as a link target.
pub struct GraphLinkTarget {
    admin: GraphAdmin,
    graph: GraphMeta,
    namespace: NamespaceId,
    plan: SourcePlan,
    link: LinkId,
    commits: AtomicUsize,
    dead_letters: AtomicUsize,
}

impl std::fmt::Debug for GraphLinkTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphLinkTarget")
            .field("graph", &self.graph.id)
            .field("link", &self.link)
            .finish_non_exhaustive()
    }
}

impl GraphLinkTarget {
    pub(crate) fn new(
        admin: GraphAdmin,
        graph: GraphMeta,
        namespace: NamespaceId,
        plan: SourcePlan,
        link: LinkId,
    ) -> Self {
        Self {
            admin,
            graph,
            namespace,
            plan,
            link,
            commits: AtomicUsize::new(0),
            dead_letters: AtomicUsize::new(0),
        }
    }

    /// Records of the graph's stream this target has committed.
    #[must_use]
    pub fn commits(&self) -> usize {
        self.commits.load(Ordering::Acquire)
    }

    /// Source records this target has dead-lettered.
    #[must_use]
    pub fn dead_letters(&self) -> usize {
        self.dead_letters.load(Ordering::Acquire)
    }

    /// The graph, open here as its owner.
    async fn open(&self) -> Result<Arc<Graph>, LinkError> {
        self.admin.link_graph(&self.graph).await.map_err(retryable)
    }

    /// The change operations of `records`, as a job for the write lane at `progress`; a stream's
    /// dead letters are written first.
    async fn job(
        &self,
        records: Vec<(u32, OffsetRecord)>,
        progress: LinkApply,
    ) -> Result<crate::durable::lane::LinkJob, LinkError> {
        let apply: crate::durable::lane::LinkApplyFn = match &self.plan {
            SourcePlan::Collection(plan) => {
                let mut ops = Vec::with_capacity(records.len());
                for (partition, record) in &records {
                    match loams_collection::decode(&record.record) {
                        Ok(op) => ops.push(op),
                        // A collection's writer encodes every record it appends: one that does
                        // not decode is corrupt, and must not stop the link.
                        Err(err) => {
                            tracing::warn!(collection = %plan.collection(), partition, offset = record.offset, error = %err, "a record of a LINKED graph's collection does not decode; the link skips it");
                        }
                    }
                }
                let plan = Arc::clone(plan);
                Box::new(move |db, index| plan.apply(db, index, &ops))
            }
            SourcePlan::Stream(plan) => {
                let mut ops = Vec::new();
                for (partition, record) in &records {
                    match plan.resolve(*partition, record) {
                        Ok(resolved) => ops.extend(resolved),
                        Err(letter) => {
                            stream::write_dead_letter(
                                self.admin.catalog().store(),
                                self.namespace,
                                &self.graph,
                                self.link,
                                &letter,
                            )
                            .await?;
                            self.dead_letters.fetch_add(1, Ordering::AcqRel);
                        }
                    }
                }
                let plan = Arc::clone(plan);
                Box::new(move |db, index| plan.apply(db, index, &ops))
            }
        };
        Ok(crate::durable::lane::LinkJob {
            progress: Some(progress),
            apply,
        })
    }
}

fn retryable(err: impl std::error::Error + Send + Sync + 'static) -> LinkError {
    LinkError::Target {
        retryable: true,
        source: Box::new(err),
    }
}

#[async_trait]
impl LinkTarget for GraphLinkTarget {
    async fn load(&self) -> Result<TargetState, LinkError> {
        let graph = self.open().await?;
        Ok(graph
            .link_progress(self.link)
            .map(|progress| TargetState {
                version: progress.version,
                applied: progress.applied_after,
            })
            .unwrap_or_default())
    }

    /// Commits `batch` in one record of the graph's stream, or several when it is larger than
    /// [`CHUNK_RECORDS`] or [`CHUNK_BYTES`]: each record carries its own records' offsets, so a
    /// failure between two leaves the graph at a consistent point. The fence is not used: the
    /// graph's owner lease fences every append, and the version, checked in the write lane,
    /// refuses a stale commit.
    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        _fence: &Fence,
    ) -> Result<u64, CommitError> {
        let graph = self.open().await.map_err(CommitError::Other)?;
        let current = graph.link_progress(self.link);
        let mut version = current.as_ref().map_or(0, |progress| progress.version);
        if version != expected_version {
            return Err(CommitError::Conflict);
        }
        let mut applied = current
            .map(|progress| progress.applied_after)
            .unwrap_or_default();
        for chunk in chunks(batch) {
            applied.extend(chunk.advance);
            let progress = LinkApply {
                link: self.link,
                version: version + 1,
                applied_after: applied.clone(),
            };
            let job = self
                .job(chunk.records, progress)
                .await
                .map_err(CommitError::Other)?;
            match self.admin.link_commit(Arc::clone(&graph), job).await {
                Ok(Ok(_)) => {
                    version += 1;
                    self.commits.fetch_add(1, Ordering::AcqRel);
                }
                Ok(Err(GraphError::VersionMismatch { .. })) => return Err(CommitError::Conflict),
                Ok(Err(err)) => return Err(CommitError::Other(retryable(err))),
                Err(err) => return Err(CommitError::Other(retryable(err))),
            }
        }
        Ok(version)
    }
}

/// Part of a batch committed as one record.
struct Chunk {
    records: Vec<(u32, OffsetRecord)>,
    /// The next offset to apply, per partition, once the chunk is durable.
    advance: BTreeMap<u32, u64>,
}

/// Splits `batch` into chunks of at most [`CHUNK_RECORDS`] records and [`CHUNK_BYTES`] bytes,
/// in batch order. The last chunk carries the whole batch's `applied_after`, which also covers
/// offsets trimmed before the link reached them.
fn chunks(batch: ApplyBatch) -> Vec<Chunk> {
    let empty = || Chunk {
        records: Vec::new(),
        advance: BTreeMap::new(),
    };
    let mut out = Vec::new();
    let mut current = empty();
    let mut bytes = 0;
    for (partition, record) in batch.records {
        let size = record.record.value.as_ref().map_or(0, bytes::Bytes::len);
        if !current.records.is_empty()
            && (current.records.len() >= CHUNK_RECORDS || bytes + size > CHUNK_BYTES)
        {
            out.push(std::mem::replace(&mut current, empty()));
            bytes = 0;
        }
        bytes += size;
        current.advance.insert(partition, record.offset + 1);
        current.records.push((partition, record));
    }
    current.advance.extend(batch.applied_after);
    out.push(current);
    out
}
