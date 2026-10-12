//! `pg-control`'s records as `loams.postgres.v1` and `loams.operations.v1`
//! messages, and the requests' fields as the service's types (PG2 Task 9).
//! Pure functions, no I/O: `connect_postgres` calls them.

use std::time::Duration;

use buffa::MessageField;
use buffa::enumeration::EnumValue;
use loams_pg_control::Versioned;
use loams_pg_control::model::{
    BranchState, DatabaseRec, OperationRec, OperationState, ProjectRec, ProjectState, RoleRec,
    WalService,
};
use loams_pg_control::service::ServiceError;
use loams_pg_control::service::branches::BranchView;
use loams_postgres::Lsn;
use loams_proto::google::protobuf::{Duration as PbDuration, FieldMask, Timestamp};
use loams_proto::loams::operations::v1 as ops;
use loams_proto::loams::postgres::v1 as pb;

/// A `Timestamp` from ms since the Unix epoch.
pub(super) fn timestamp(ms: u64) -> Timestamp {
    Timestamp {
        seconds: i64::try_from(ms / 1000).unwrap_or(i64::MAX),
        nanos: i32::try_from((ms % 1000) * 1_000_000).unwrap_or(0),
        ..Default::default()
    }
}

/// Ms since the Unix epoch from a `Timestamp`; `invalid_argument` on
/// `field` before the epoch.
pub(super) fn timestamp_ms(t: &Timestamp, field: &str) -> Result<u64, ServiceError> {
    let seconds = u64::try_from(t.seconds)
        .map_err(|_| ServiceError::invalid(field, "a time before 1970 is not accepted"))?;
    let nanos = u64::try_from(t.nanos)
        .map_err(|_| ServiceError::invalid(field, "nanos is not negative"))?;
    Ok(seconds
        .saturating_mul(1000)
        .saturating_add(nanos / 1_000_000))
}

/// A proto `Duration` from whole seconds.
fn seconds(s: u64) -> PbDuration {
    PbDuration {
        seconds: i64::try_from(s).unwrap_or(i64::MAX),
        ..Default::default()
    }
}

/// A `std` duration from a proto one; `invalid_argument` on `field` when
/// negative.
pub(super) fn duration(d: &PbDuration, field: &str) -> Result<Duration, ServiceError> {
    let s = u64::try_from(d.seconds)
        .map_err(|_| ServiceError::invalid(field, "a duration is not negative"))?;
    let n = u32::try_from(d.nanos)
        .map_err(|_| ServiceError::invalid(field, "a duration is not negative"))?;
    Ok(Duration::new(s, n))
}

/// The paths of an update mask, snake_case; `invalid_argument` when empty
/// (R1.7) or naming a field outside `allowed`.
pub(super) fn mask<'a>(
    mask: Option<&'a FieldMask>,
    allowed: &[&str],
) -> Result<Vec<&'a str>, ServiceError> {
    let paths: Vec<&str> = mask
        .map(|m| m.paths.iter().map(String::as_str).collect())
        .unwrap_or_default();
    if paths.is_empty() {
        return Err(ServiceError::invalid(
            "update_mask",
            format!(
                "the update mask names at least one of {}",
                allowed.join(", ")
            ),
        ));
    }
    if let Some(bad) = paths.iter().find(|p| !allowed.contains(p)) {
        return Err(ServiceError::invalid(
            "update_mask",
            format!(
                "{bad} cannot be updated; the mask takes {}",
                allowed.join(", ")
            ),
        ));
    }
    Ok(paths)
}

pub(super) fn project(p: &Versioned<ProjectRec>) -> pb::Project {
    let r = &p.record;
    let WalService::LoamsWal { pool } = &r.wal;
    pb::Project {
        id: r.id.clone(),
        namespace: r.namespace.clone(),
        name: r.name.clone(),
        pg_version: r.pg_version,
        region: r.region.clone(),
        history_retention: MessageField::some(seconds(r.history_retention_s)),
        default_branch_id: r.default_branch_id.clone().unwrap_or_default(),
        wal_pool: pool.clone(),
        state: EnumValue::Known(match r.state {
            ProjectState::Creating => pb::ProjectState::PROJECT_STATE_CREATING,
            ProjectState::Ready => pb::ProjectState::PROJECT_STATE_READY,
            ProjectState::Deleting => pb::ProjectState::PROJECT_STATE_DELETING,
            ProjectState::Failed => pb::ProjectState::PROJECT_STATE_FAILED,
        }),
        create_time: MessageField::some(timestamp(r.created_at_ms)),
        update_time: MessageField::some(timestamp(r.updated_at_ms.max(r.created_at_ms))),
        version: p.version,
        ..Default::default()
    }
}

fn lsn(l: Lsn) -> String {
    l.to_string()
}

pub(super) fn branch(v: &BranchView) -> pb::Branch {
    let r = &v.branch.record;
    pb::Branch {
        id: r.id.clone(),
        project_id: r.project_id.clone(),
        name: r.name.clone(),
        parent_id: r.parent_id.clone().unwrap_or_default(),
        parent_lsn: r.ancestor_lsn.map(|l| lsn(Lsn(l))).unwrap_or_default(),
        parent_time: r
            .parent_time_ms
            .map_or_else(MessageField::none, |ms| MessageField::some(timestamp(ms))),
        is_default: v.is_default,
        protected: r.protected,
        expire_time: r
            .expires_at_ms
            .map_or_else(MessageField::none, |ms| MessageField::some(timestamp(ms))),
        state: EnumValue::Known(match r.state {
            BranchState::Creating => pb::BranchState::BRANCH_STATE_CREATING,
            BranchState::Ready => pb::BranchState::BRANCH_STATE_READY,
            BranchState::Deleting => pb::BranchState::BRANCH_STATE_DELETING,
            BranchState::Failed => pb::BranchState::BRANCH_STATE_FAILED,
        }),
        wal: v.wal.as_ref().map_or_else(MessageField::none, |w| {
            MessageField::some(pb::WalHeads {
                commit_lsn: lsn(w.commit_lsn),
                flush_lsn: lsn(w.flush_lsn),
                remote_consistent_lsn: lsn(w.remote_consistent_lsn),
                backup_lsn: lsn(w.backup_lsn),
                ..Default::default()
            })
        }),
        logical_size_bytes: v.logical_size_bytes.unwrap_or_default(),
        create_time: MessageField::some(timestamp(r.created_at_ms)),
        update_time: MessageField::some(timestamp(r.updated_at_ms.max(r.created_at_ms))),
        version: v.branch.version,
        ..Default::default()
    }
}

pub(super) fn role(r: &RoleRec) -> pb::Role {
    pb::Role {
        name: r.name.clone(),
        branch_id: r.branch_id.clone(),
        system: r.system,
        create_time: MessageField::some(timestamp(r.created_at_ms)),
        ..Default::default()
    }
}

pub(super) fn database(d: &DatabaseRec) -> pb::Database {
    pb::Database {
        name: d.name.clone(),
        branch_id: d.branch_id.clone(),
        owner_role: d.owner.clone(),
        create_time: MessageField::some(timestamp(d.created_at_ms)),
        ..Default::default()
    }
}

pub(super) fn operation_state(s: OperationState) -> ops::OperationState {
    match s {
        OperationState::Pending => ops::OperationState::OPERATION_STATE_PENDING,
        OperationState::AwaitingApproval => ops::OperationState::OPERATION_STATE_AWAITING_APPROVAL,
        OperationState::Running => ops::OperationState::OPERATION_STATE_RUNNING,
        OperationState::Succeeded => ops::OperationState::OPERATION_STATE_SUCCEEDED,
        OperationState::Failed => ops::OperationState::OPERATION_STATE_FAILED,
    }
}

/// An operation; `target` is `{project, branch?}` (§46 §4.1), and an
/// error's `code` is its registered reason.
pub(super) fn operation(o: &OperationRec) -> ops::Operation {
    let target = std::iter::once(("project".to_owned(), o.project_id.clone()))
        .chain(o.branch_id.iter().map(|b| ("branch".to_owned(), b.clone())))
        .collect();
    ops::Operation {
        id: o.id.clone(),
        kind: o.kind.as_str().to_owned(),
        namespace: o.namespace.clone(),
        target,
        state: EnumValue::Known(operation_state(o.state)),
        progress: o.progress.as_ref().map_or_else(MessageField::none, |p| {
            MessageField::some(ops::Progress {
                done: p.done,
                total: p.total,
                unit: p.unit.clone(),
                phase: p.phase.clone(),
                ..Default::default()
            })
        }),
        created_at: MessageField::some(timestamp(o.created_at_ms)),
        updated_at: MessageField::some(timestamp(o.updated_at_ms.max(o.created_at_ms))),
        error: o.error.as_ref().map_or_else(MessageField::none, |e| {
            MessageField::some(ops::OperationError {
                code: e.reason.clone(),
                message: e.message.clone(),
                ..Default::default()
            })
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_round_trip_through_timestamps() {
        let t = timestamp(1_791_590_400_123);
        assert_eq!((t.seconds, t.nanos), (1_791_590_400, 123_000_000));
        assert_eq!(timestamp_ms(&t, "t").expect("ms"), 1_791_590_400_123);
        let before = Timestamp {
            seconds: -1,
            ..Default::default()
        };
        assert!(timestamp_ms(&before, "t").is_err());
    }

    #[test]
    fn an_update_mask_is_checked() {
        let allowed = ["name", "history_retention"];
        assert!(mask(None, &allowed).is_err());
        let m = FieldMask {
            paths: vec!["name".into()],
            ..Default::default()
        };
        assert_eq!(mask(Some(&m), &allowed).expect("ok"), ["name"]);
        let m = FieldMask {
            paths: vec!["region".into()],
            ..Default::default()
        };
        let e = mask(Some(&m), &allowed).expect_err("refused");
        assert_eq!(
            e.metadata.get("field").map(String::as_str),
            Some("update_mask")
        );
    }
}
