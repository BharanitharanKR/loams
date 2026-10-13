//! The graph's side of Loams' log (design §48 §6.2 step 7, §6.4): appending a group's records to
//! a graph's implicit stream, and reading the stream back to rebuild the graph.
//!
//! [`GraphLog`] is shared by every graph of a process. It wraps a [`LogWriter`] that the server
//! starts with [`GraphLog::log_config`] (a flush every 2 ms or 256 KiB, the group commit's
//! defaults), the server's [`LogReader`] and the metastore. The write lane runs on the statement
//! pool's threads, not on the async runtime, so the calls here block on the runtime's handle.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use loams_common::StreamId;
use loams_common::meta::{Consistency, Fence, MetaError, MetaStore};
use loams_log::{AppendAck, FetchRequest, LogConfig, LogError, LogReader, LogWriter, OffsetRecord};

use crate::engine::GraphError;

/// How the write lane groups transactions (§48 §6.2; defaults 2 ms and 256 KiB).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommit {
    /// A group takes no new transaction once this long has passed since its first one began.
    pub window: Duration,
    /// A group takes no new transaction once its records hold this many bytes.
    pub max_bytes: usize,
    /// The largest change set one transaction may produce, encoded. Default 32 MiB, half the log
    /// writer's default buffer, so one record can always be appended. A transaction past it is
    /// answered `INVALID_ARGUMENT`, and the graph reloads, which undoes its commit.
    pub max_record_bytes: usize,
    /// The longest a group's append may take. The lane holds the graph's gate exclusively while
    /// it waits, so a stalled log would stall every read of the graph (PR #424 review). Past it
    /// the append's outcome is unknown, and the graph reloads from its stream as for a failed
    /// append (R11.8). Default 10 s.
    pub append_timeout: Duration,
}

impl Default for GroupCommit {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(2),
            max_bytes: 256 * 1024,
            max_record_bytes: 32 << 20,
            append_timeout: Duration::from_secs(10),
        }
    }
}

/// **Tests only** (feature `test-hooks`): run before each append; an `Err` fails the append as
/// the log would, and a future that waits holds the append (and so the write lane) until it ends.
pub type AppendFault =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), LogError>> + Send>> + Send + Sync>;

/// The bytes one fetch of a replay asks for.
const REPLAY_FETCH_BYTES: usize = 4 << 20;

/// How many fetches in a row may return nothing below the high watermark before a replay gives
/// up (the local metastore node lagging the leader it read the watermark from).
const REPLAY_STALLS: usize = 100;

/// Loams' log as the graphs of one process use it (GR1 Task 11). Cheap to clone.
#[derive(Clone)]
pub struct GraphLog {
    inner: Arc<Inner>,
}

struct Inner {
    meta: Arc<dyn MetaStore>,
    writer: LogWriter,
    reader: LogReader,
    runtime: tokio::runtime::Handle,
    group: GroupCommit,
    #[cfg(feature = "test-hooks")]
    fault: std::sync::Mutex<Option<AppendFault>>,
    #[cfg(feature = "test-hooks")]
    kill: std::sync::Mutex<Option<super::faults::KillHook>>,
}

impl std::fmt::Debug for GraphLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphLog")
            .field("group", &self.inner.group)
            .finish_non_exhaustive()
    }
}

impl GraphLog {
    /// The graph log over a writer, a reader and the metastore they share. `runtime` is the
    /// runtime the writer's flush task runs on; the lane blocks on it from the statement pool.
    #[must_use]
    pub fn new(
        meta: Arc<dyn MetaStore>,
        writer: LogWriter,
        reader: LogReader,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                meta,
                writer,
                reader,
                runtime,
                group: GroupCommit::default(),
                #[cfg(feature = "test-hooks")]
                fault: std::sync::Mutex::new(None),
                #[cfg(feature = "test-hooks")]
                kill: std::sync::Mutex::new(None),
            }),
        }
    }

    /// The same log with another group commit. Only before any graph opens with it.
    #[must_use]
    pub fn with_group_commit(self, group: GroupCommit) -> Self {
        let inner = &self.inner;
        Self {
            inner: Arc::new(Inner {
                meta: inner.meta.clone(),
                writer: inner.writer.clone(),
                reader: inner.reader.clone(),
                runtime: inner.runtime.clone(),
                group,
                #[cfg(feature = "test-hooks")]
                fault: std::sync::Mutex::new(None),
                #[cfg(feature = "test-hooks")]
                kill: std::sync::Mutex::new(None),
            }),
        }
    }

    /// The writer settings for the graph log of log node `node_id`: a flush every 2 ms or
    /// 256 KiB, the group commit's defaults (§48 §6.2). The collection writer's 250 ms would be
    /// every graph write's latency.
    #[must_use]
    pub fn log_config(node_id: u64) -> LogConfig {
        let group = GroupCommit::default();
        LogConfig {
            flush_interval: group.window,
            flush_bytes: group.max_bytes,
            ..LogConfig::new(node_id)
        }
    }

    /// The metastore the log commits through.
    #[must_use]
    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.inner.meta
    }

    /// The group commit the lane uses.
    pub(crate) fn group(&self) -> GroupCommit {
        self.inner.group
    }

    /// The metastore clock, in milliseconds: the time the lane stamps on a record, which the
    /// idempotency window counts from (R10.4).
    pub(crate) fn now_ms(&self) -> u64 {
        self.inner.meta.now_ms()
    }

    /// **Tests only** (feature `test-hooks`): sets the fault run before every append.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_append_fault(&self, fault: Option<AppendFault>) {
        *self
            .inner
            .fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = fault;
    }

    /// **Tests only** (feature `test-hooks`): sets the hook awaited at every kill point this
    /// node reaches (GR1 Task 16, [`super::faults`]).
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn set_kill_hook(&self, hook: Option<super::faults::KillHook>) {
        *self
            .inner
            .kill
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
    }

    /// Reaches kill point `point` from async code: awaits the node's kill hook, if any. Nothing
    /// without `test-hooks`.
    #[allow(clippy::unused_async)]
    pub(crate) async fn reach(&self, point: super::faults::KillPoint) {
        #[cfg(feature = "test-hooks")]
        {
            let hook = self
                .inner
                .kill
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(hook) = hook {
                hook(point).await;
            }
        }
        #[cfg(not(feature = "test-hooks"))]
        let _ = point;
    }

    /// [`GraphLog::reach`] from the write lane, which runs off the runtime: blocks.
    pub(crate) fn reach_blocking(&self, point: super::faults::KillPoint) {
        #[cfg(feature = "test-hooks")]
        {
            let hooked = self
                .inner
                .kill
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
            if hooked {
                self.wait(self.reach(point));
            }
        }
        #[cfg(not(feature = "test-hooks"))]
        let _ = point;
    }

    /// Runs `future` to its end from a thread that is not one of the runtime's.
    ///
    /// The lane and the opens run on the statement pool, where this blocks on the runtime's
    /// handle. A caller already inside a runtime (a test calling the engine directly) gets a
    /// scoped thread, since a runtime thread cannot block on its own runtime.
    pub(crate) fn wait<T: Send>(&self, future: impl Future<Output = T> + Send) -> T {
        let runtime = &self.inner.runtime;
        if tokio::runtime::Handle::try_current().is_err() {
            return runtime.block_on(future);
        }
        std::thread::scope(|scope| {
            scope
                .spawn(|| runtime.block_on(future))
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
    }

    /// The implicit stream of graph `graph` in `namespace`, `_graph.<gr_id>`: one partition,
    /// standard WAL class, never trimmed by retention (Task 13 trims it below its snapshots).
    /// The name is reserved, so only this metastore command creates it (GR1 Task 12). Creating it
    /// again answers the same stream, and a stream of GR1 Task 11's name (`graph.<gr_id>`) is
    /// renamed to it in place, keeping its id and records.
    pub(crate) async fn ensure_stream(
        meta: &Arc<dyn MetaStore>,
        namespace: loams_common::NamespaceId,
        graph: &str,
    ) -> Result<StreamId, MetaError> {
        meta.ensure_graph_stream(namespace, graph).await
    }

    /// Appends one group's records to `stream` as one batch, in order, under the owner's
    /// `fence` (GR1 Task 12), and waits until they are durable, at most
    /// [`GroupCommit::append_timeout`]: past it the answer is [`LogError::CommitUnknown`]. A
    /// broken fence fails the append with `ApplyError::Fenced` and nothing of it is committed.
    /// Blocks (see [`GraphLog::wait`]).
    ///
    /// Past the timeout the records may still be in the writer's buffer and commit later; the
    /// reload that follows takes a new epoch first, so such a late commit is refused.
    pub(crate) fn append(
        &self,
        stream: StreamId,
        records: Vec<loams_log::Record>,
        fence: Option<&Fence>,
    ) -> Result<AppendAck, LogError> {
        let limit = self.inner.group.append_timeout;
        let append = async {
            #[cfg(feature = "failpoints")]
            fail::fail_point!("loams_graph::durable::append", |_| Err(LogError::Closed));
            #[cfg(feature = "test-hooks")]
            {
                let fault = self
                    .inner
                    .fault
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(fault) = fault {
                    fault().await?;
                }
            }
            match fence {
                Some(fence) => {
                    self.inner
                        .writer
                        .append_fenced(stream, 0, records, fence.clone())
                        .await
                }
                None => self.inner.writer.append(stream, 0, records).await,
            }
        };
        self.wait(async {
            tokio::time::timeout(limit, append)
                .await
                .unwrap_or_else(|_| {
                    Err(LogError::CommitUnknown(format!(
                        "the graph's append took longer than {limit:?}"
                    )))
                })
        })
    }

    /// The stream's first readable offset and its high watermark, read linearizably.
    pub(crate) fn bounds(&self, stream: StreamId) -> Result<(u64, u64), GraphError> {
        self.wait(async {
            let state = self
                .inner
                .meta
                .stream_state(Consistency::Linearizable, stream)
                .await
                .map_err(|err| unavailable("reading the graph's stream", &err))?
                .ok_or_else(|| {
                    GraphError::LogUnavailable(format!("the graph's stream {stream} is gone"))
                })?;
            Ok(state
                .partitions
                .first()
                .copied()
                .flatten()
                .map_or((0, 0), |bounds| {
                    (bounds.log_start_offset, bounds.high_watermark)
                }))
        })
    }

    /// One fetch of `stream` from `offset`: the records there, or, at its end, those committed
    /// within `max_wait` (none when nothing was). A follower's tail (GR1 Task 15).
    pub(crate) async fn fetch(
        &self,
        stream: StreamId,
        offset: u64,
        max_wait: Duration,
    ) -> Result<loams_log::FetchResponse, LogError> {
        self.inner
            .reader
            .fetch(FetchRequest {
                stream,
                partition: 0,
                offset,
                max_bytes: REPLAY_FETCH_BYTES,
                max_wait,
            })
            .await
    }

    /// Every record of `stream` in `from..to`, in offset order. Blocks.
    pub(crate) fn read(
        &self,
        stream: StreamId,
        from: u64,
        to: u64,
    ) -> Result<Vec<OffsetRecord>, GraphError> {
        self.wait(async {
            let mut out = Vec::new();
            let mut offset = from;
            let mut stalls = 0;
            while offset < to {
                let fetched = self
                    .inner
                    .reader
                    .fetch(FetchRequest {
                        stream,
                        partition: 0,
                        offset,
                        max_bytes: REPLAY_FETCH_BYTES,
                        max_wait: Duration::from_millis(50),
                    })
                    .await
                    .map_err(|err| unavailable("reading the graph's stream", &err))?;
                if fetched.records.is_empty() {
                    stalls += 1;
                    if stalls > REPLAY_STALLS {
                        return Err(GraphError::LogUnavailable(format!(
                            "the graph's stream {stream} has no record at offset {offset} below \
                             its high watermark {to}"
                        )));
                    }
                    continue;
                }
                stalls = 0;
                for record in fetched.records {
                    if record.offset >= to {
                        break;
                    }
                    offset = record.offset + 1;
                    out.push(record);
                }
            }
            Ok(out)
        })
    }
}

/// The name of graph `graph`'s implicit stream, `_graph.gr_<ULID>` (GR1 Task 12). A stream a
/// user creates may not start with `_`, so nobody else can create or seed it.
#[must_use]
pub fn stream_name(graph: &str) -> String {
    loams_common::meta::graph_stream_name(graph)
}

/// Whether `err` is the metastore refusing an append whose owner lease moved on (GR1 Task 12).
#[must_use]
pub fn is_fenced(err: &LogError) -> bool {
    matches!(
        err,
        LogError::Meta(MetaError::Rejected(
            loams_common::meta::ApplyError::Fenced { .. }
        ))
    )
}

/// The log is unavailable: the detail goes to the server log, the client gets a generic message.
fn unavailable(what: &str, err: &dyn std::fmt::Display) -> GraphError {
    tracing::warn!(error = %err, "{what} failed");
    GraphError::LogUnavailable(format!("{what} failed; retry"))
}
