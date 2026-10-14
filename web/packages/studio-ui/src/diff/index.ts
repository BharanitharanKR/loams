// The diff panel and review comments (SD1 Task 20; inventory (d)). Its own entry, like
// `./files` (T21-7): Shiki and @pierre/diffs stay out of Studio's first screen.

export { DiffPanel, type DiffPanelProps, defaultBaseRef } from "./DiffPanel";
export { DEFAULT_VIEW, type DiffViewPreferences, useDiffPanelStore } from "./diffPanelStore";
export { getRenderablePatch, resolveFileDiffPath } from "./diffRendering";
export { DIFF_TOKEN_VARIABLES, diffSurfaceCss } from "./diffTheme";
export {
  anchorToSelection,
  formatAnchorLabel,
  type ReviewAnchor,
  selectionToAnchor,
} from "./reviewCommentContext";
export {
  type ChangeRequestInfo,
  type CheckoutDiffView,
  type DiffFileStat,
  type DiffFileText,
  type DiffSelection,
  type DiffSource,
  type DiffSourceKind,
  type HistoryCommit,
  type ReviewComment,
  type ReviewCommentDraft,
  type ReviewEvent,
  type ReviewSide,
  StaleDiffError,
} from "./source";
export { diffFileTreeRows } from "./tree/DiffFileTree";
export { diffFileTreeEntries } from "./tree/diffFileTree.logic";
