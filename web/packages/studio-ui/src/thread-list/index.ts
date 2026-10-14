// Studio's thread list and sidebar (SD1 Task 17; §53 §5.3; inventory (a)): the rules
// vendored from T3 Code's sidebar, the model that places threads in projects, and the
// row badges. The plugin's container (`@loams/plugin-studio` sidebar/) wires them to
// agentd.
export {
  buildThreadActionMenuItems,
  type ThreadActionMenuId,
  type ThreadActionMenuItem,
  type ThreadActionMenuState,
  threadActionRequiresOperate,
} from "./actionMenu";
export {
  createThreadJumpHintVisibilityController,
  formatWorkingDurationLabel,
  getFallbackThreadIdAfterDelete,
  getProjectSortTimestamp,
  hasUnseenCompletion,
  isContextMenuPointerDown,
  isTrailingDoubleClick,
  orderItemsByPreferredIds,
  type PinPosition,
  pinPositionInOrder,
  planPinnedMove,
  planSidebarThreadDrop,
  resolveAdjacentThreadId,
  resolveProjectStatusIndicator,
  resolveSidebarDropTarget,
  resolveSidebarDropVerb,
  resolveSidebarRowAccessibility,
  resolveSidebarThreadSection,
  resolveSidebarThreadStatus,
  resolveThreadStatusPill,
  type SidebarDropTarget,
  type SidebarDropVerb,
  type SidebarListItem,
  type SidebarListMarker,
  type SidebarProjectSortOrder,
  type SidebarSection,
  type SidebarThreadDropPlan,
  type SidebarThreadStatus,
  searchSidebarThreads,
  shouldShowSidebarDuration,
  sidebarListItemId,
  sidebarMarkerId,
  sortProjectsForSidebar,
  THREAD_JUMP_HINT_SHOW_DELAY_MS,
  type ThreadJumpHintVisibilityController,
  type ThreadStatusPill,
  type ThreadTraversalDirection,
  useThreadJumpHintVisibility,
} from "./logic";
export {
  buildSidebarModel,
  isPathWithin,
  type ProjectWorktree,
  placeThread,
  type SidebarGroupRows,
  type SidebarModel,
  type SidebarProjectInput,
  type SidebarProjectRows,
} from "./model";
export type { SidebarThreadSortOrder } from "./rules/threadSort";
export { RunGroupBadge, ThreadStatusLabel, ThreadWorktreeIndicator } from "./StatusIndicators";
export {
  getLatestThreadForProject,
  getThreadSortTimestamp,
  sortThreads as sortThreadsByRecency,
  type ThreadSortInput,
  toSortableTimestamp,
} from "./threadSort";
export type { SidebarRunGroup, SidebarThread, ThreadRunStatus, ThreadShell } from "./types";
export { formatWorktreePathForDisplay, getOrphanedWorktreePathForThread } from "./worktreeCleanup";
