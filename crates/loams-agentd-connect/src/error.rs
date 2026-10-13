//! The daemon's errors as Connect errors (SD1 Shared contracts, "Error
//! reasons"; `docs/api/reasons.md`, "agentd reasons").
//!
//! An ndjson error's text starts with its reason (`not_found: …`). The same
//! reason goes into a `loams.errors.v1.ErrorInfo` detail, under the Connect
//! code the table below names. A text with no known reason is `unknown` with
//! no detail: the client sees the message only.

use connectrpc::{ConnectError, ErrorCode, ErrorDetail};
use loams_agentd_rpc::RpcError;

use crate::map::MapError;
use crate::proto::loams::errors::v1::ErrorInfo;

/// Every reason the daemon raises on the Connect surface, with its code: the
/// ndjson `ERROR_REASONS` and SD1's additions.
pub const REASON_CODES: &[(&str, ErrorCode)] = &[
    ("unauthenticated", ErrorCode::Unauthenticated),
    ("forbidden_role", ErrorCode::PermissionDenied),
    ("protocol_mismatch", ErrorCode::FailedPrecondition),
    ("agentd_draining", ErrorCode::Unavailable),
    ("unknown_method", ErrorCode::Unimplemented),
    ("bad_params", ErrorCode::InvalidArgument),
    ("key_required", ErrorCode::FailedPrecondition),
    ("unknown_provider", ErrorCode::InvalidArgument),
    ("invalid_url", ErrorCode::InvalidArgument),
    (
        "tool_unavailable_remote_server",
        ErrorCode::FailedPrecondition,
    ),
    ("not_found", ErrorCode::NotFound),
    ("engine_not_ready", ErrorCode::Unavailable),
    ("keyring_unavailable", ErrorCode::Unavailable),
    ("limit_exceeded", ErrorCode::ResourceExhausted),
    ("harness_unsupported", ErrorCode::FailedPrecondition),
    ("not_implemented", ErrorCode::Unimplemented),
    ("stale_diff", ErrorCode::FailedPrecondition),
    ("merge_conflict", ErrorCode::Aborted),
    ("nothing_to_commit", ErrorCode::FailedPrecondition),
    ("push_rejected", ErrorCode::FailedPrecondition),
    ("provider_not_configured", ErrorCode::FailedPrecondition),
    ("checkpoint_missing", ErrorCode::NotFound),
    ("group_not_ready", ErrorCode::FailedPrecondition),
    ("approval_not_pending", ErrorCode::FailedPrecondition),
    ("origin_not_allowed", ErrorCode::PermissionDenied),
];

/// The Connect code of a reason, if the daemon knows it.
pub fn code_of(reason: &str) -> Option<ErrorCode> {
    REASON_CODES
        .iter()
        .find(|(known, _)| *known == reason)
        .map(|(_, code)| *code)
}

/// A Connect error carrying `reason` in a `loams.errors.v1.ErrorInfo`.
pub fn refuse(code: ErrorCode, reason: &str, message: impl Into<String>) -> ConnectError {
    let info = ErrorInfo {
        reason: reason.to_owned(),
        ..Default::default()
    };
    ConnectError::new(code, message).with_detail(ErrorDetail::from_message(
        "loams.errors.v1.ErrorInfo",
        &info,
    ))
}

/// The reason an ndjson error text starts with, if it is a known one:
/// `reason`, `reason: …` or `reason …`.
pub fn reason_of(text: &str) -> Option<&'static str> {
    REASON_CODES
        .iter()
        .map(|(reason, _)| *reason)
        .find(|reason| {
            text.strip_prefix(reason).is_some_and(|rest| {
                rest.is_empty() || rest.starts_with(':') || rest.starts_with(' ')
            })
        })
}

/// An engine error as the Connect error a client sees.
pub fn from_rpc(error: RpcError) -> ConnectError {
    match error {
        RpcError::UnknownMethod(method) => refuse(
            ErrorCode::Unimplemented,
            "unknown_method",
            format!("unknown method: {method}"),
        ),
        RpcError::BadParams(message) => refuse(ErrorCode::InvalidArgument, "bad_params", message),
        RpcError::Failed(text) | RpcError::Fatal(text) => match reason_of(&text) {
            Some(reason) => refuse(code_of(reason).unwrap_or(ErrorCode::Unknown), reason, text),
            None => ConnectError::new(ErrorCode::Unknown, text),
        },
        RpcError::Transport(text) => ConnectError::new(ErrorCode::Unavailable, text),
        RpcError::Closed => ConnectError::new(ErrorCode::Unavailable, "the engine closed the call"),
    }
}

/// A request the daemon cannot map to its ndjson params.
pub fn from_map(error: MapError) -> ConnectError {
    refuse(ErrorCode::InvalidArgument, "bad_params", error.0)
}

/// The answer of a §53 §6.5 gap until its SD1 task lands.
pub fn not_implemented(rpc: &str, task: &str) -> ConnectError {
    refuse(
        ErrorCode::Unimplemented,
        "not_implemented",
        format!("{rpc} is not implemented yet ({task})"),
    )
}

/// A failure inside this crate (a reply the engine's own types do not parse).
pub fn internal(message: impl Into<String>) -> ConnectError {
    ConnectError::new(ErrorCode::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_cover_the_ndjson_wire() {
        for reason in loams_agentd_proto::rpc::ERROR_REASONS {
            assert!(code_of(reason).is_some(), "{reason} has no Connect code");
        }
    }

    #[test]
    fn a_reason_is_read_from_the_text_prefix() {
        assert_eq!(reason_of("not_found: chat c1"), Some("not_found"));
        assert_eq!(reason_of("not_found"), Some("not_found"));
        assert_eq!(reason_of("bad_params bad"), Some("bad_params"));
        assert_eq!(reason_of("not_foundling"), None);
        assert_eq!(reason_of("Project space not found"), None);
    }

    #[test]
    fn engine_errors_keep_their_reason() {
        let error = from_rpc(RpcError::Failed("not_found: chat c1".into()));
        assert_eq!(error.code, ErrorCode::NotFound);
        let error = from_rpc(RpcError::Failed("plain failure".into()));
        assert_eq!(error.code, ErrorCode::Unknown);
        let error = from_rpc(RpcError::BadParams("missing field".into()));
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }
}
