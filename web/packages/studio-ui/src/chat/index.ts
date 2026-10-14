// Studio's chat view (SD1 Task 18): the timeline, its rows and logic, the thread header
// and find bar, and the transcript store the page and the dock share (D917).
export { DiffStatLabel, hasNonZeroStat } from "./DiffStatLabel";
export { formatThreadFindCount, ThreadFindBar } from "./find/ThreadFindBar";
export { MessageCopyButton } from "./MessageCopyButton";
export { TasksBadge, taskStepsOf, tasksProgressOf } from "./plan/TasksBadge";
export { type TimelineActions, TimelineRowView } from "./rows/rows";
export {
  captureTimelineAnchor,
  resolveTimelineIsAtEnd,
  restoreTimelineAnchor,
  rowsWerePrepended,
  TIMELINE_FOLLOW_REARM_THRESHOLD_PX,
} from "./scroll/timelineScrollAnchoring";
export {
  type ActivePlanState,
  deriveActivePlanState,
  derivePendingUserInputs,
  deriveTurns,
  entryText,
  formatDuration,
  isThreadRunning,
  type PendingApproval,
  type PendingUserInput,
  type TurnSummary,
} from "./session.logic";
export {
  type ThreadTranscript,
  TranscriptStore,
  type TranscriptWatch,
  transcriptStoreFor,
  useThreadTranscript,
} from "./store";
export { ThreadHeader, type ThreadHeaderProps, type ThreadUsage } from "./ThreadHeader";
export { Timeline, type TimelineProps, useTimelineRows, VIRTUALIZE_AFTER } from "./Timeline";
export {
  computeStableTimelineRows,
  deriveTimelineRows,
  findTimelineMatches,
  type TimelineRow,
} from "./timeline.logic";
export {
  resolveToolPresentation,
  summarizeToolGroup,
  type ToolPart,
  toolGroupAction,
} from "./tools/presentation";
