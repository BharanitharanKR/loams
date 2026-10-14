// @loams/studio-ui: Loams Studio's components and logic, vendored from T3 Code and rebuilt
// on the Loams stack (design §53 §9, D912). See NOTICE and provenance.json.
export * from "./composer";
export { type MediaQueryInput, useIsMobile, useMediaQuery } from "./hooks/useMediaQuery";
export { useResizeDrag } from "./hooks/useResizeDrag";
export * from "./keybindings";
export { AnimatedHeight } from "./lib/AnimatedHeight";
export { isEditableFocused } from "./lib/editableFocus";
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
export * from "./thread-list";
