//! The operations of the long-running project and branch RPCs (PG2 Task 5):
//! `loams.operations.v1.Operation`s, kept per project at
//! `O/<project_id>/<operation_id>` ([`OperationRec`]).
//!
//! The API service writes an operation `Pending`, with its indexes
//! (`Q/i/<id>` for `GetOperation`, `Q/n/<ns>/<id>` for `ListOperations` and
//! `WatchOperations`), in the same batch as the record it acts on. The
//! project's reconciler (Task 7) finds it by listing the project's
//! operations and moves it to `Running`, then `Succeeded` or `Failed`, with
//! its progress, under its lease.

use std::time::Duration;

use ulid::Ulid;

pub use crate::model::{
    OperationError, OperationKind, OperationProgress, OperationRec, OperationState,
};

use super::{NeonRead, PgService, ServiceError, check_namespace, list_error, page};
use crate::model::{
    AllOperationIds, NamespaceOperationKey, NamespaceOperationRec, NamespaceOperations,
    OperationIdKey, OperationIdRec, OperationKey, Record as _,
};
use crate::store::{Batch, BatchError, MAX_PAGE_SIZE, Page, PgControlStore, StoreError};

/// How long a finished operation stays readable before
/// [`PgService::collect_operations`] removes it (R5.13; Task 46's runbook
/// owns the operator's choice).
pub const OPERATION_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// A new operation id: "op-" and 26 lower-case hex characters (D146), the
/// first 104 bits of a fresh ULID (time first, so ids sort by creation).
pub fn new_operation_id() -> String {
    let hex = format!("{:032x}", Ulid::generate().0);
    format!("op-{}", &hex[..26])
}

/// A `Pending` operation of `kind` on a project (and a branch).
pub(crate) fn pending(
    kind: OperationKind,
    namespace: &str,
    project_id: &str,
    branch_id: Option<&str>,
    now_ms: u64,
) -> OperationRec {
    OperationRec {
        id: new_operation_id(),
        kind,
        namespace: namespace.into(),
        project_id: project_id.into(),
        branch_id: branch_id.map(Into::into),
        state: OperationState::Pending,
        error: None,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        progress: None,
        endpoint_id: None,
    }
}

/// A `Pending` endpoint operation of `kind` (Task 11): its target names
/// the endpoint and its branch.
pub(crate) fn pending_endpoint(
    kind: OperationKind,
    namespace: &str,
    endpoint: &crate::model::EndpointRec,
    now_ms: u64,
) -> OperationRec {
    let mut op = pending(
        kind,
        namespace,
        &endpoint.project_id,
        Some(&endpoint.branch_id),
        now_ms,
    );
    op.endpoint_id = Some(endpoint.id.clone());
    op
}

/// Adds `op` and its two indexes (by id, and in its namespace) to
/// `batch`, all created; returns the operation's index.
pub(crate) fn add_operation(batch: &mut Batch, op: &OperationRec) -> Result<usize, StoreError> {
    let at = batch.put(op, None)?;
    batch.put(
        &OperationIdRec {
            id: op.id.clone(),
            namespace: op.namespace.clone(),
            project_id: op.project_id.clone(),
        },
        None,
    )?;
    batch.put(
        &NamespaceOperationRec {
            namespace: op.namespace.clone(),
            id: op.id.clone(),
            project_id: op.project_id.clone(),
        },
        None,
    )?;
    Ok(at)
}

impl<N: NeonRead> PgService<N> {
    /// `GetOperation`: an operation by its id alone, through `Q/i/`. The
    /// caller (Task 9) authorizes it by its `namespace` and project.
    ///
    /// # Errors
    ///
    /// `invalid_argument` for a malformed id; `not_found`.
    pub async fn get_operation(&self, operation_id: &str) -> Result<OperationRec, ServiceError> {
        if !operation_id.starts_with("op-") || operation_id.contains('/') {
            return Err(ServiceError::invalid(
                "operation_id",
                "an operation id starts with 'op-'",
            ));
        }
        let missing = || ServiceError::not_found("operation", operation_id);
        let index = self
            .store
            .get::<OperationIdRec>(&OperationIdKey {
                id: operation_id.into(),
            })
            .await?
            .ok_or_else(missing)?;
        let key = OperationKey {
            project_id: index.record.project_id,
            id: operation_id.into(),
        };
        self.store
            .get::<OperationRec>(&key)
            .await?
            .map(|v| v.record)
            .ok_or_else(missing)
    }

    /// `ListOperations` for one namespace: a page in id (creation) order,
    /// through `Q/n/<ns>/`, and the next page's token (empty on the last).
    ///
    /// # Errors
    ///
    /// `invalid_argument` for a bad namespace, size or token.
    pub async fn list_operations(
        &self,
        namespace: &str,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<OperationRec>, String), ServiceError> {
        check_namespace(namespace)?;
        let prefix = NamespaceOperations {
            namespace: namespace.into(),
        };
        let (index, next) = self
            .store
            .list::<NamespaceOperationRec>(&prefix, page(page_size, page_token)?)
            .await
            .map_err(list_error)?;
        let mut out = Vec::with_capacity(index.len());
        for entry in index {
            let key = OperationKey {
                project_id: entry.record.project_id,
                id: entry.record.id,
            };
            // An index whose operation was collected meanwhile is skipped.
            if let Some(op) = self.store.get::<OperationRec>(&key).await? {
                out.push(op.record);
            }
        }
        Ok((out, next.unwrap_or_default()))
    }
}

impl<N: NeonRead> PgService<N> {
    /// Removes every operation that finished (`Succeeded` or `Failed`) more
    /// than `retention` before `now_ms`, with its two indexes, one
    /// compare-and-set batch each; answers how many it removed. An
    /// operation that moved meanwhile is left for the next run. Task 9 runs
    /// it on a timer.
    ///
    /// # Errors
    ///
    /// The store's, when it does not answer.
    pub async fn collect_operations(
        &self,
        now_ms: u64,
        retention: Duration,
    ) -> Result<usize, ServiceError> {
        let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        let mut removed = 0;
        let mut at = Page::first(MAX_PAGE_SIZE);
        loop {
            let (ids, next) = self
                .store
                .list::<OperationIdRec>(&AllOperationIds, at)
                .await?;
            for index in ids {
                let key = OperationKey {
                    project_id: index.record.project_id.clone(),
                    id: index.record.id.clone(),
                };
                let ns_key = NamespaceOperationKey {
                    namespace: index.record.namespace.clone(),
                    id: index.record.id.clone(),
                };
                let op = self.store.get::<OperationRec>(&key).await?;
                let finished = op.as_ref().is_none_or(|op| {
                    matches!(
                        op.record.state,
                        OperationState::Succeeded | OperationState::Failed
                    ) && now_ms.saturating_sub(op.record.updated_at_ms) > retention_ms
                });
                if !finished {
                    continue;
                }
                let mut batch = Batch::default();
                if let Some(op) = &op {
                    batch.delete::<OperationRec>(&key, op.version)?;
                }
                batch.delete::<OperationIdRec>(&index.record.key(), index.version)?;
                if let Some(entry) = self.store.get::<NamespaceOperationRec>(&ns_key).await? {
                    batch.delete::<NamespaceOperationRec>(&ns_key, entry.version)?;
                }
                match self.writer.commit(batch).await {
                    Ok(_) => removed += 1,
                    Err(BatchError {
                        error: StoreError::Conflict { .. } | StoreError::NotFound,
                        ..
                    }) => {}
                    Err(e) => return Err(e.error.into()),
                }
            }
            match next {
                Some(token) => at = Page::after(MAX_PAGE_SIZE, token),
                None => return Ok(removed),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_ids_follow_d146() {
        let id = new_operation_id();
        assert_eq!(id.len(), 3 + 26);
        assert!(
            id[3..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_ne!(new_operation_id(), new_operation_id());
    }
}
