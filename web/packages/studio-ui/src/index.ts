// @loams/studio-ui: Loams Studio's components and logic, vendored from T3 Code and rebuilt
// on the Loams stack (design §53 §9, D912). See NOTICE and provenance.json.

export {
  buildGitActionProgressStages,
  buildGitPaletteActions,
  buildMenuItems,
  CHANGE_REQUEST_TERMINOLOGY,
  type ChangeRequestProviderKind,
  formatGitActionElapsed,
  GIT_ACTION_SUCCESS_VISIBLE_MS,
  type GitActionMenuItem,
  type GitChangeRequest,
  type GitDialogAction,
  type GitPaletteAction,
  type GitQuickAction,
  type GitStackedAction,
  type GitStatusView,
  type GitWriteStep,
  getMenuActionDisabledReason,
  PROVIDER_LABEL,
  requiresDefaultBranchConfirmation,
  resolveDefaultBranchActionDialogCopy,
  resolveLiveThreadBranchUpdate,
  resolveQuickAction,
  stepsOf,
} from "./git/actions.logic";
export { BranchSelector, type BranchSelectorProps } from "./git/BranchSelector";
export { BranchToolbar, type BranchToolbarProps } from "./git/BranchToolbar";
export {
  type BranchRef,
  ENV_MODES,
  type EnvMode,
  isEnvMode,
  type PreviousWorktreeSeed,
  resolveBranchSelectionTarget,
  resolveBranchToolbarValue,
  resolveBranchTriggerLabel,
  resolveEffectiveEnvMode,
  resolveEnvModeLabel,
  resolvePreviousWorktreeLabel,
  resolvePreviousWorktreeSeed,
  shouldIncludeBranchPickerItem,
} from "./git/BranchToolbar.logic";
export { EnvModeSelector, type EnvModeSelectorProps } from "./git/EnvModeSelector";
export {
  type GitActionError,
  type GitActionOutcome,
  GitActions,
  type GitActionsProps,
  type GitChangedFile,
  type GitCommitInput,
  type GitMergeInput,
  type GitPullRequestInput,
  type GitPushInput,
  type GitRunPlan,
  GitStatusChips,
  type MergeStrategyName,
} from "./git/GitActions";
export {
  forgeRepositoryOfUrl,
  isTemporaryWorktreeBranch,
  resolveAutoFeatureBranchName,
  sanitizeNewRefName,
  WORKTREE_BRANCH_PREFIX,
} from "./git/shared";
export {
  WORKTREE_SETUP_STAGE_LABEL,
  WorktreeSetupCard,
  type WorktreeSetupSnapshot,
  type WorktreeSetupStage,
  type WorktreeSetupStageId,
  type WorktreeSetupStageStatus,
} from "./git/WorktreeSetupCard";
export { type MediaQueryInput, useIsMobile, useMediaQuery } from "./hooks/useMediaQuery";
export { useResizeDrag } from "./hooks/useResizeDrag";
export * from "./keybindings";
export { AnimatedHeight } from "./lib/AnimatedHeight";
export { LRUCache } from "./lib/lruCache";
export { observeResize } from "./lib/observeResize";
export {
  buildFileItems,
  buildProjectActionItems,
  buildRunGroupItems,
  buildThreadActionItems,
  type PaletteProject,
  type PaletteRunGroup,
  type PaletteThread,
  RECENT_THREAD_LIMIT,
  type StudioPaletteItem,
  sortThreads,
} from "./palette/logic";
export {
  formatTerminalContextLabel,
  normalizeTerminalContextSelection,
  type TerminalContextSelection,
} from "./terminal/context";
export {
  type TerminalBackend,
  type TerminalInfo,
  type TerminalSessionState,
  type TerminalSessionStatus,
  TerminalSessions,
  type TerminalStreamEvent,
} from "./terminal/session";
export {
  type CreateTerminalSurface,
  createXtermSurface,
  TerminalDrawer,
  type TerminalDrawerProps,
  type TerminalProjectAction,
  type TerminalSurface,
  type TerminalSurfaceOptions,
  terminalThemeFromTokens,
} from "./terminal/TerminalDrawer";
export { type FileLinkTarget, fileLinkTarget } from "./terminal/targets";
export { TERMINALS_STORAGE_KEY, useTerminalUiStateStore } from "./terminal/uiStore";
