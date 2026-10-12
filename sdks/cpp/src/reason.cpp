// The reason registry, transcribed from `docs/api/reasons.md`.
//
// **Transcribed, not generated.** Design §44 §7.3 has `protoc-gen-loams-facade`
// read the registry page and render the reason table per language; that renderer
// has no C++ template (see `facade.hpp`'s header comment), so this is the
// hand-written equivalent and it says so rather than pretending.
//
// The authority is the page. `cpp_error_reason_mapping` walks this table and
// asserts that it has all fifty-two registry reasons, that every spelling is
// `snake_case`, and that the three distinct cases of R8 stay distinct — so a
// reason added to the page without being added here fails a test rather than
// being dropped.

#include "loams/reason.hpp"

#include <algorithm>
#include <cctype>
#include <ostream>

namespace loams {
namespace {

/// One row of the registry page, in the page's order.
struct Row {
  Reason reason;
  const char* name;
};

/// The registry, in `docs/api/reasons.md`'s order. The order is the page's
/// because it is the order a reader of either has to keep in their head.
constexpr Row kRows[] = {
    {Reason::kApprovalExpired, "approval_expired"},
    {Reason::kApprovalAlreadyDecided, "approval_already_decided"},
    {Reason::kApprovalStaleRevision, "approval_stale_revision"},
    {Reason::kRequesterCannotApprove, "requester_cannot_approve"},
    {Reason::kDecisionProofInvalid, "decision_proof_invalid"},
    {Reason::kStepUpRequired, "step_up_required"},
    {Reason::kReasonRequired, "reason_required"},
    {Reason::kInvalidDecision, "invalid_decision"},
    {Reason::kPairingExpired, "pairing_expired"},
    {Reason::kPairingUsed, "pairing_used"},
    {Reason::kDeviceRevoked, "device_revoked"},
    {Reason::kPushTargetUnknown, "push_target_unknown"},
    {Reason::kNotImplemented, "not_implemented"},
    {Reason::kFeatureNotInVariant, "feature_not_in_variant"},
    {Reason::kHouseNotConfigured, "house_not_configured"},
    {Reason::kHouseSyntaxError, "house_syntax_error"},
    {Reason::kHouseUnknownTable, "house_unknown_table"},
    {Reason::kHouseUnknownDatabase, "house_unknown_database"},
    {Reason::kHouseAccessDenied, "house_access_denied"},
    {Reason::kHouseReadOnly, "house_read_only"},
    {Reason::kHouseTimeout, "house_timeout"},
    {Reason::kHouseMemoryLimit, "house_memory_limit"},
    {Reason::kHouseTooManyQueries, "house_too_many_queries"},
    {Reason::kHouseQuotaExceeded, "house_quota_exceeded"},
    {Reason::kHouseDisabledFunction, "house_disabled_function"},
    {Reason::kHouseCancelled, "house_cancelled"},
    {Reason::kHouseQueryNotFound, "house_query_not_found"},
    {Reason::kHouseQueryFailed, "house_query_failed"},
    {Reason::kProjectNotFound, "project_not_found"},
    {Reason::kBranchHasChildren, "branch_has_children"},
    {Reason::kBranchProtected, "branch_protected"},
    {Reason::kLsnOutOfRetention, "lsn_out_of_retention"},
    {Reason::kEndpointExistsForBranch, "endpoint_exists_for_branch"},
    {Reason::kComputeStartFailed, "compute_start_failed"},
    {Reason::kQuotaExceeded, "quota_exceeded"},
    {Reason::kStorageUnavailable, "storage_unavailable"},
    {Reason::kSecretAlreadyIssued, "secret_already_issued"},
    {Reason::kLiveBadCursor, "live_bad_cursor"},
    {Reason::kLiveWorkerCrashed, "live_worker_crashed"},
    {Reason::kLiveDeployConflict, "live_deploy_conflict"},
    {Reason::kInvalidArgument, "invalid_argument"},
    {Reason::kNotFound, "not_found"},
    {Reason::kAlreadyExists, "already_exists"},
    {Reason::kPermissionDenied, "permission_denied"},
    {Reason::kTokenExpired, "token_expired"},
    {Reason::kUnauthenticated, "unauthenticated"},
    {Reason::kFailedPrecondition, "failed_precondition"},
    {Reason::kResourceExhausted, "resource_exhausted"},
    {Reason::kUnavailable, "unavailable"},
    {Reason::kDeadlineExceeded, "deadline_exceeded"},
    {Reason::kAborted, "aborted"},
    {Reason::kInternal, "internal"},
};

}  // namespace

std::string_view ToString(Reason reason) {
  for (const Row& row : kRows) {
    if (row.reason == reason) {
      return row.name;
    }
  }
  // `kUnknown` maps to the empty string, not to `"unknown"`: `"unknown"` is not
  // a reason the registry has, and a caller comparing strings must not be
  // offered a value that could also arrive from a newer server.
  return reason == Reason::kUnknown ? std::string_view() : std::string_view("<unknown reason>");
}

std::optional<Reason> ReasonFromString(std::string_view name) {
  for (const Row& row : kRows) {
    if (name == row.name) {
      return row.reason;
    }
  }
  return std::nullopt;
}

std::ostream& operator<<(std::ostream& out, Reason reason) { return out << ToString(reason); }

const std::vector<Reason>& AllReasons() {
  static const std::vector<Reason>* const reasons = [] {
    auto* const built = new std::vector<Reason>();
    built->reserve(std::size(kRows));
    for (const Row& row : kRows) {
      built->push_back(row.reason);
    }
    return built;
  }();
  return *reasons;
}

bool IsRegistryReason(Reason reason) { return ReasonFromString(ToString(reason)).has_value(); }

}  // namespace loams