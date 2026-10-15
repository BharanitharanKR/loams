// @loams/studio-ui/groups: parallel runs (SD1 Task 25; design §53 §5.5, D904). Loams' own
// code: T3 Code has no compare view (inventory (l)). Its own entry, like `./diff`: the pair
// diff brings @pierre/diffs, which Studio's first screen does not need.

export { DiffMatrix } from "./DiffMatrix";
export { CheckResults, checkLabel, GroupGrid, MEMBER_STATUS_LABEL, useNow } from "./GroupGrid";
export {
  type CheckStatus,
  type ComparisonRowView,
  changeLevel,
  fenceFor,
  formatElapsed,
  type GroupMemberView,
  type GroupResolutionView,
  type GroupState,
  type GroupView,
  isLive,
  JUDGE_PROMPT_DIFF_BUDGET,
  judgePrompt,
  type MatrixCell,
  type MatrixRow,
  type MemberCheckView,
  type MemberStatus,
  matrixCells,
  memberElapsedMs,
  type PendingApprovalView,
  type ResolveActionName,
  type SharedApproval,
  sharedApprovals,
  stableJson,
} from "./model";
export { PairDiff } from "./PairDiff";
export {
  type ChangeRequestProviderName,
  type ResolveChoice,
  ResolveDialog,
  type ResolveError,
  type ResolveOutcome,
  resolveSteps,
  type WinnerState,
} from "./ResolveDialog";
export { SharedApprovals } from "./SharedApprovals";
