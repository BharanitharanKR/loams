//! [`LiveError`], the error of every Loams Live operation, and its wire code.

use std::sync::LazyLock;

use loams_kv::TxnError;
use regex::Regex;

use crate::pb;
use crate::validate::FieldViolation;

/// The reason of a deployed function's own error (LV1 plan Task 19).
pub const FUNCTION_ERROR: &str = "live_function_error";
/// The reason of a function that ran past its CPU limit.
pub const FUNCTION_TIMEOUT: &str = "live_function_timeout";
/// The reason of a function that ran past its memory limit.
pub const FUNCTION_OUT_OF_MEMORY: &str = "live_function_out_of_memory";
/// The reason of a call past one of the app's [`Limits`](crate::Limits).
pub const LIMIT_EXCEEDED: &str = "live_limit_exceeded";

/// Every reason a Live error carries beyond the generic ones, for the
/// registry check (`docs/api/reasons.md`).
pub const LIVE_REASONS: &[&str] = &[
    "live_bad_cursor",
    "live_schema_violation",
    "live_deploy_conflict",
    "live_index_not_ready",
    "live_worker_crashed",
    "live_app_disabled",
    "live_app_exists",
    FUNCTION_ERROR,
    FUNCTION_TIMEOUT,
    FUNCTION_OUT_OF_MEMORY,
    LIMIT_EXCEEDED,
];

/// Why a Loams Live operation failed. Each variant maps to one
/// [`pb::ErrorCode`], which is what clients see (design §20 §7.1).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LiveError {
    /// The request is malformed: a bad value, name, id or range.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A pagination cursor that is malformed, forged, or from another app
    /// or index (LV1 plan Task 4): `INVALID_ARGUMENT`, reason
    /// `live_bad_cursor`.
    #[error("invalid argument: {0}")]
    BadCursor(String),
    /// A write whose document does not match its table's validator (LV1
    /// plan Task 8, design §45 §4.1): `INVALID_ARGUMENT`, reason
    /// `live_schema_violation`. The message names the table and the field
    /// paths (never the values); `violations` holds them (up to
    /// [`MAX_VIOLATIONS`](crate::validate::MAX_VIOLATIONS)).
    #[error("invalid argument: {message}")]
    SchemaViolation {
        message: String,
        violations: Vec<FieldViolation>,
    },
    /// A document, table or index does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A call to an app that is disabled or deleted (LV1 plan Task 13,
    /// design §45 §7): `FAILED_PRECONDITION`, reason `live_app_disabled`.
    #[error("failed precondition: {0}")]
    AppDisabled(String),
    /// `CreateApp` with a name another app has (LV1 plan Task 13):
    /// `FAILED_PRECONDITION`, reason `live_app_exists`.
    #[error("failed precondition: {0}")]
    AppExists(String),
    /// The request is valid but the state refuses it (for example an index
    /// change on a non-empty table, R1 plan Ruling 5).
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// A query on an index a deploy is still backfilling (LV1 plan Task 9,
    /// design §45 §4.2): `FAILED_PRECONDITION`, reason
    /// `live_index_not_ready`. The text names the index and the table.
    #[error("failed precondition: {0}")]
    IndexNotReady(String),
    /// A deploy whose `expected_active` is not the active deployment (LV1
    /// plan Task 6, design §45 §3.3): `FAILED_PRECONDITION`, reason
    /// `live_deploy_conflict`. The text names both deployments.
    #[error("failed precondition: {0}")]
    DeployConflict(String),
    /// A limit of [`Limits`](crate::Limits) was exceeded. `limit` is the
    /// field name of the limit.
    #[error("limit {limit} exceeded: {message}")]
    LimitExceeded {
        limit: &'static str,
        message: String,
    },
    /// A journal consumer's position is below what the janitor has kept:
    /// the entries after `position` in `shard` are gone (`first` is the
    /// oldest one left, if any). The consumer must resynchronise (rerun
    /// everything) and start again from the current heads.
    #[error(
        "journal shard {shard} was trimmed past position {position} (oldest entry left: {first:?})"
    )]
    JournalTrimmed {
        shard: u16,
        position: u64,
        first: Option<u64>,
    },
    /// The Live listener was asked to bind a non-loopback address, which R1
    /// refuses (D111, §20 §7.1; [`check_listen`](crate::check_listen)).
    #[error(
        "--live-listen {0} is not a loopback address; the Live API has no authentication \
         until the unified auth plan (D111)"
    )]
    NotLoopback(std::net::SocketAddr),
    /// `tenancy = "multi"` without isolated workers (design §45 §3.1, LV1
    /// plan Task 5): strangers' code never runs in the server's process.
    #[error(r#"live: tenancy = "multi" needs isolation = "isolated" (Linux only)"#)]
    IsolationRequired,
    /// `isolation = "isolated"` where the worker sandbox does not exist.
    #[error(r#"live: isolation = "isolated" needs Linux (seccomp and landlock)"#)]
    IsolationUnavailable,
    /// A stored record could not be decoded.
    #[error("corrupt record: {0}")]
    Corrupt(String),
    /// A failure of the server itself (the OS random source, a bug).
    #[error("internal error: {0}")]
    Internal(String),
    /// A deployed function threw, or its bundle is invalid (LV1 plan
    /// Task 3). The text is the function's own error message.
    #[error("function error: {0}")]
    FunctionError(String),
    /// A deployed function ran past its JavaScript CPU limit.
    #[error("function {function} ran past its CPU limit of {limit:?}")]
    FunctionTimeout {
        function: String,
        limit: std::time::Duration,
    },
    /// The isolated worker running the call crashed or was killed (LV1
    /// plan Task 5): `FUNCTION_ERROR`, reason `live_worker_crashed`. The
    /// call's transaction never committed; a mutation is not retried.
    #[error("function error: {0}")]
    WorkerCrashed(String),
    /// A deployed function ran past its runtime's memory limit.
    #[error("function {function} ran past its memory limit of {limit} bytes")]
    FunctionOutOfMemory { function: String, limit: usize },
    /// A store transaction or read failed. Inside [`Store::run`] bodies this
    /// carries the runner's retry signals ([`TxnError::Conflict`],
    /// [`TxnError::NotApplied`]); [`LiveError::into_txn`] hands them back.
    ///
    /// [`Store::run`]: loams_kv::Store::run
    #[error("storage: {0}")]
    Txn(#[from] TxnError),
}

impl LiveError {
    /// The wire code of this error.
    pub fn code(&self) -> pb::ErrorCode {
        match self {
            LiveError::InvalidArgument(_)
            | LiveError::BadCursor(_)
            | LiveError::SchemaViolation { .. }
            | LiveError::NotLoopback(_)
            | LiveError::IsolationRequired
            | LiveError::IsolationUnavailable => pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT,
            LiveError::NotFound(_) => pb::ErrorCode::ERROR_CODE_NOT_FOUND,
            LiveError::FailedPrecondition(_)
            | LiveError::AppDisabled(_)
            | LiveError::AppExists(_)
            | LiveError::IndexNotReady(_)
            | LiveError::DeployConflict(_)
            | LiveError::JournalTrimmed { .. } => pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION,
            LiveError::LimitExceeded { .. } => pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED,
            LiveError::Corrupt(_) | LiveError::Internal(_) => pb::ErrorCode::ERROR_CODE_INTERNAL,
            LiveError::FunctionError(_) | LiveError::WorkerCrashed(_) => {
                pb::ErrorCode::ERROR_CODE_FUNCTION_ERROR
            }
            LiveError::FunctionTimeout { .. } => pb::ErrorCode::ERROR_CODE_FUNCTION_TIMEOUT,
            LiveError::FunctionOutOfMemory { .. } => {
                pb::ErrorCode::ERROR_CODE_FUNCTION_OUT_OF_MEMORY
            }
            LiveError::Txn(e) => match e {
                TxnError::Conflict
                | TxnError::NotApplied(_)
                | TxnError::Undetermined { .. }
                | TxnError::Deadline => pb::ErrorCode::ERROR_CODE_UNAVAILABLE,
                TxnError::AlreadyExists(_) | TxnError::Fatal(_) => {
                    pb::ErrorCode::ERROR_CODE_INTERNAL
                }
            },
        }
    }

    /// The stable `ErrorInfo.reason` of this error, where it has one
    /// (`docs/api/reasons.md`); the other errors are told apart by their
    /// code.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            LiveError::BadCursor(_) => Some("live_bad_cursor"),
            LiveError::SchemaViolation { .. } => Some("live_schema_violation"),
            LiveError::DeployConflict(_) => Some("live_deploy_conflict"),
            LiveError::IndexNotReady(_) => Some("live_index_not_ready"),
            LiveError::WorkerCrashed(_) => Some("live_worker_crashed"),
            LiveError::AppDisabled(_) => Some("live_app_disabled"),
            LiveError::AppExists(_) => Some("live_app_exists"),
            _ => None,
        }
    }

    /// The `ErrorInfo.reason` this error carries on the wire (LV1 plan Task
    /// 19): its own [`reason`](Self::reason), else the generic reason of its
    /// Connect code (`docs/api/reasons.md`). Every Live error has one.
    pub fn wire_reason(&self) -> &'static str {
        if let Some(reason) = self.reason() {
            return reason;
        }
        match self {
            LiveError::FunctionError(_) => FUNCTION_ERROR,
            LiveError::FunctionTimeout { .. } => FUNCTION_TIMEOUT,
            LiveError::FunctionOutOfMemory { .. } => FUNCTION_OUT_OF_MEMORY,
            LiveError::LimitExceeded { .. } => LIMIT_EXCEEDED,
            _ => match self.code() {
                pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT => "invalid_argument",
                pb::ErrorCode::ERROR_CODE_NOT_FOUND => "not_found",
                pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION => "failed_precondition",
                pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED => "resource_exhausted",
                pb::ErrorCode::ERROR_CODE_UNAVAILABLE => "unavailable",
                _ => "internal",
            },
        }
    }

    /// Whether this error's text comes from the store or the server's
    /// internals, so it may hold raw keys: the client gets it scrubbed,
    /// with a request id the server log carries (LV1 plan Task 19).
    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            LiveError::Internal(_) | LiveError::Corrupt(_) | LiveError::Txn(_)
        )
    }

    /// The message a client may see, and the request id that goes with it
    /// (LV1 plan Task 19). An [internal](Self::is_internal) error's text
    /// is [scrubbed](scrub) of anything that looks like a raw key and names
    /// a request id (`request_id`, else a new one), under which the scrubbed
    /// text is logged for the operator; any other error's text is its own.
    pub fn public_message(&self, request_id: Option<&str>) -> (String, Option<String>) {
        if !self.is_internal() {
            return (self.to_string(), request_id.map(str::to_owned));
        }
        let id = request_id.map_or_else(crate::logs::Ring::request_id, str::to_owned);
        let scrubbed = scrub(&self.to_string());
        tracing::warn!(request_id = %id, error = %scrubbed, "a Live call failed inside the server");
        (format!("{scrubbed} (request {id})"), Some(id))
    }

    /// The field violations of a [`LiveError::SchemaViolation`]; empty for
    /// every other error. (They reach the wire's `field_violations` when
    /// Live's errors are mapped onto the main Connect port, LV1 Task 19.)
    pub fn field_violations(&self) -> &[FieldViolation] {
        match self {
            LiveError::SchemaViolation { violations, .. } => violations,
            _ => &[],
        }
    }

    /// The wire form of this error, its message as
    /// [`public_message`](Self::public_message) gives it.
    pub fn to_proto(&self) -> pb::LiveError {
        pb::LiveError {
            code: self.code().into(),
            message: self.public_message(None).0,
            ..Default::default()
        }
    }

    /// Splits a storage error off: `Err(txn_error)` for [`LiveError::Txn`],
    /// so a [`Store::run`](loams_kv::Store::run) body can return it and let
    /// the runner retry, and `Ok(self)` for every other error, which the body
    /// returns inside its value so the runner does not retry it.
    pub fn into_txn(self) -> Result<LiveError, TxnError> {
        match self {
            LiveError::Txn(e) => Err(e),
            other => Ok(other),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        LiveError::InvalidArgument(message.into())
    }

    pub(crate) fn limit(limit: &'static str, message: impl Into<String>) -> Self {
        LiveError::LimitExceeded {
            limit,
            message: message.into(),
        }
    }
}

/// What [`scrub`] replaces: debug-printed byte arrays of four or more
/// bytes, runs of 16 or more hex digits, and quoted strings with `\x`
/// escapes (how `tikv-client` and the store print keys).
static KEY_LIKE: LazyLock<[(Regex, &'static str); 3]> = LazyLock::new(|| {
    let re = |pattern: &str| Regex::new(pattern).expect("a valid scrub pattern");
    [
        (re(r"\[\s*\d{1,3}(?:\s*,\s*\d{1,3}){3,}\s*,?\s*\]"), "[…]"),
        (
            re(r#"b?"(?:[^"\\]|\\.)*\\x[0-9a-fA-F]{2}(?:[^"\\]|\\.)*""#),
            "\"…\"",
        ),
        (re(r"(?i)\b(?:0x)?[0-9a-f]{16,}\b"), "…"),
    ]
});

/// `text` with every byte string that looks like a key replaced by `…`
/// (LV1 plan Task 19, §20 §11.4: `tikv-client` errors carry raw keys).
pub fn scrub(text: &str) -> String {
    let mut out = text.to_string();
    for (pattern, with) in KEY_LIKE.iter() {
        out = pattern.replace_all(&out, *with).into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_removes_key_like_bytes() {
        let raw = "region error: key [1, 2, 3, 4, 120, 0] not in region; \
                   lock on b\"\\x01\\x02doc\" at 0a0b0c0d0e0f10111213 ts 42";
        let clean = scrub(raw);
        assert!(!clean.contains("120"), "{clean}");
        assert!(!clean.contains("x01"), "{clean}");
        assert!(!clean.contains("0a0b0c0d0e0f"), "{clean}");
        assert!(clean.contains("ts 42"), "{clean}");
        assert!(clean.contains("region error"), "{clean}");
    }

    #[test]
    fn every_error_has_a_wire_reason_and_internal_text_is_scrubbed() {
        let err = LiveError::Internal("key [9, 9, 9, 9] lost".into());
        let (message, id) = err.public_message(Some("01ABC"));
        assert_eq!(id.as_deref(), Some("01ABC"));
        assert!(message.ends_with("(request 01ABC)"), "{message}");
        assert!(!message.contains("9, 9"), "{message}");
        assert_eq!(err.wire_reason(), "internal");
        let limit = LiveError::limit("max_fields", "too many");
        assert_eq!(limit.wire_reason(), LIMIT_EXCEEDED);
        assert_eq!(
            LiveError::NotFound("x".into()).public_message(None),
            ("not found: x".to_string(), None)
        );
    }
}
