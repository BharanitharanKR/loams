//! Loams Git's `_git` mirror on the server (GT1 Task 15, design §36 §4.8): the
//! [`GitEventSink`] that appends committed transactions to each namespace's reserved `_git`
//! stream through the CloudEvents ingest, which deduplicates by `source` and `id` (D270), and
//! the repair pass that publishes whatever a crash left unmirrored.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use loams_cloudevents::{CloudEvent, partition};
use loams_common::meta::{Consistency, GIT_STREAM, MetaStore, system_stream_partitions};
use loams_git::catalog::{BucketRepoCatalog, RepoCatalog, RepoLifecycle};
use loams_git::{GitEventSink, MirrorCursor, NamespaceId, RepoId, StoreWalStore, repair_repo};
use loams_log::LogWriter;
use loams_store::Store;
use tokio::sync::Mutex;

use super::events::{EventsConfig, IngestContext, ingest};

/// Records read per catalog page during a repair pass.
const REPAIR_PAGE: usize = 256;
/// The longest one repository's repair may take, so a sink that keeps failing cannot hold
/// up the others (PR #612 review); the next pass tries again.
const REPAIR_REPO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Appends to `_git` through this node's ingest.
pub(crate) struct StreamSink {
    meta: Arc<dyn MetaStore>,
    writer: LogWriter,
    config: EventsConfig,
    node_id: u64,
    /// Namespaces whose `_git` stream exists, with the namespace's name.
    ready: Mutex<HashMap<NamespaceId, String>>,
}

impl std::fmt::Debug for StreamSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSink")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl StreamSink {
    pub(crate) fn new(
        meta: Arc<dyn MetaStore>,
        writer: LogWriter,
        config: EventsConfig,
        node_id: u64,
    ) -> Self {
        Self {
            meta,
            writer,
            config,
            node_id,
            ready: Mutex::new(HashMap::new()),
        }
    }

    /// The name of namespace `ns`, with its `_git` stream ensured.
    async fn namespace(&self, ns: NamespaceId) -> Result<String, String> {
        if let Some(name) = self.ready.lock().await.get(&ns) {
            return Ok(name.clone());
        }
        let name = self
            .meta
            .namespaces(Consistency::Local)
            .await
            .map_err(|err| err.to_string())?
            .into_iter()
            .find(|namespace| namespace.id == ns)
            .map(|namespace| namespace.name)
            .ok_or_else(|| format!("namespace {ns} does not exist"))?;
        self.meta
            .ensure_system_stream(ns, GIT_STREAM)
            .await
            .map_err(|err| err.to_string())?;
        self.ready.lock().await.insert(ns, name.clone());
        Ok(name)
    }
}

#[async_trait]
impl GitEventSink for StreamSink {
    async fn publish(
        &self,
        ns: NamespaceId,
        repo: &RepoId,
        events: &[CloudEvent],
    ) -> Result<(), String> {
        let name = self.namespace(ns).await?;
        let partitions = system_stream_partitions(GIT_STREAM).unwrap_or(1);
        // Keyed by repository: one repository's events stay in order in one partition.
        let partition = partition::kafka_partition(repo.as_str().as_bytes(), partitions);
        let ctx = IngestContext {
            meta: &self.meta,
            writer: &self.writer,
            config: &self.config,
            node_id: self.node_id,
        };
        let report = ingest(&ctx, &name, GIT_STREAM, Some(partition), events)
            .await
            .map_err(|err| format!("{err:?}"))?;
        // Another writer's claim on one of these events is still open: try again later.
        if let Some(ms) = report.retry_after_ms() {
            return Err(format!("an event is still in flight; retry after {ms} ms"));
        }
        Ok(())
    }
}

/// One repair pass over every active repository of every namespace (GT1 Task 15): publishes
/// what each WAL holds past its mirror cursor. A failure is logged; the next pass tries again.
pub(crate) async fn repair_all(meta: &dyn MetaStore, store: &Store, sink: &dyn GitEventSink) {
    let namespaces = match meta.namespaces(Consistency::Local).await {
        Ok(namespaces) => namespaces,
        Err(err) => {
            tracing::warn!(%err, "listing namespaces for the _git repair failed");
            return;
        }
    };
    let catalog = BucketRepoCatalog::new(store.clone());
    for namespace in namespaces {
        let mut after = None;
        loop {
            let (records, more) = match catalog
                .list(namespace.id, after.as_ref(), REPAIR_PAGE)
                .await
            {
                Ok(page) => page,
                Err(err) => {
                    tracing::warn!(ns = %namespace.id, %err, "listing repositories for the _git repair failed");
                    break;
                }
            };
            for record in &records {
                if record.state != RepoLifecycle::Active {
                    continue;
                }
                let wal = StoreWalStore::new(
                    store.clone(),
                    loams_git::RepoPaths::new(namespace.id, &record.repo_id),
                );
                let cursor = MirrorCursor::new(store.clone(), namespace.id, &record.repo_id);
                let repaired = tokio::time::timeout(
                    REPAIR_REPO_TIMEOUT,
                    repair_repo(&wal, &cursor, sink, namespace.id, &record.repo_id),
                )
                .await;
                let Ok(repaired) = repaired else {
                    tracing::warn!(
                        ns = %namespace.id, repo = %record.repo_id,
                        "repairing the _git mirror timed out; the next pass resumes from its cursor"
                    );
                    continue;
                };
                match repaired {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(
                        ns = %namespace.id, repo = %record.repo_id, events = n,
                        "the _git repair published events a crash left unmirrored"
                    ),
                    Err(err) => tracing::warn!(
                        ns = %namespace.id, repo = %record.repo_id, %err,
                        "repairing the _git mirror failed"
                    ),
                }
            }
            match (more, records.last()) {
                (true, Some(last)) => after = Some(last.name.clone()),
                _ => break,
            }
        }
    }
}
