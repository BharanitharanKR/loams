// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/diffPanelStore.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// keyed by thread id; agentd's four sources (working tree, branch, commit, latest turn) in place of T3's scopes and turn ids; view preferences and viewed marks added; kept in memory, with no storage key (T21-9). Loams' changes are Apache-2.0.

// The diff panel's UI state (SD1 Task 20): which diff each thread shows, the view's
// preferences, and the files the reader marked viewed. Server state is not here: diffs and
// comments come from the daemon. Nothing is persisted (Shared contracts' storage keys).

import { create } from "zustand";

import type { DiffSelection } from "./source";

export type DiffLayout = "unified" | "split";

export interface DiffViewPreferences {
  readonly layout: DiffLayout;
  readonly wrap: boolean;
  readonly ignoreWhitespace: boolean;
  readonly wordDiff: boolean;
  readonly treeOpen: boolean;
}

// The working tree is the default: it is what the agent just changed, and what discard and
// comments act on.
const DEFAULT_SELECTION: DiffSelection = { kind: "working" };

export const DEFAULT_VIEW: DiffViewPreferences = {
  layout: "unified",
  wrap: false,
  ignoreWhitespace: false,
  wordDiff: true,
  treeOpen: false,
};

interface DiffPanelStoreState {
  byThread: Record<string, DiffSelection>;
  branchBaseRefByThread: Record<string, string | null>;
  /** Per thread, the viewed files as `path` → the content version they were viewed at. */
  viewedByThread: Record<string, Record<string, number>>;
  view: DiffViewPreferences;
  select: (threadId: string, selection: DiffSelection) => void;
  selectBranchBaseRef: (threadId: string, baseRef: string | null) => void;
  setViewed: (threadId: string, path: string, version: number | null) => void;
  setView: (patch: Partial<DiffViewPreferences>) => void;
  removeThread: (threadId: string) => void;
}

function normalizeBaseRef(baseRef: string | null): string | null {
  const normalized = baseRef?.trim();
  return normalized ? normalized : null;
}

export const useDiffPanelStore = create<DiffPanelStoreState>()((set) => ({
  byThread: {},
  branchBaseRefByThread: {},
  viewedByThread: {},
  view: DEFAULT_VIEW,
  select: (threadId, selection) =>
    set((state) => {
      const previous = state.byThread[threadId];
      // Coming back to "branch" keeps the base the reader picked last time.
      const next: DiffSelection =
        selection.kind === "branch" && selection.baseRef === null
          ? { kind: "branch", baseRef: state.branchBaseRefByThread[threadId] ?? null }
          : selection;
      return {
        byThread: { ...state.byThread, [threadId]: next },
        branchBaseRefByThread:
          previous?.kind === "branch"
            ? { ...state.branchBaseRefByThread, [threadId]: previous.baseRef }
            : state.branchBaseRefByThread,
      };
    }),
  selectBranchBaseRef: (threadId, baseRef) =>
    set((state) => {
      const normalizedBaseRef = normalizeBaseRef(baseRef);
      return {
        byThread: { ...state.byThread, [threadId]: { kind: "branch", baseRef: normalizedBaseRef } },
        branchBaseRefByThread: { ...state.branchBaseRefByThread, [threadId]: normalizedBaseRef },
      };
    }),
  setViewed: (threadId, path, version) =>
    set((state) => {
      const current = { ...(state.viewedByThread[threadId] ?? {}) };
      if (version === null) delete current[path];
      else current[path] = version;
      return { viewedByThread: { ...state.viewedByThread, [threadId]: current } };
    }),
  setView: (patch) => set((state) => ({ view: { ...state.view, ...patch } })),
  removeThread: (threadId) =>
    set((state) => {
      if (
        !(threadId in state.byThread) &&
        !(threadId in state.branchBaseRefByThread) &&
        !(threadId in state.viewedByThread)
      ) {
        return state;
      }
      const { [threadId]: _removed, ...byThread } = state.byThread;
      const { [threadId]: _removedBaseRef, ...branchBaseRefByThread } = state.branchBaseRefByThread;
      const { [threadId]: _removedViewed, ...viewedByThread } = state.viewedByThread;
      return { byThread, branchBaseRefByThread, viewedByThread };
    }),
}));

export function selectThreadDiffSelection(
  byThread: Record<string, DiffSelection>,
  threadId: string | null | undefined,
): DiffSelection {
  if (!threadId) return DEFAULT_SELECTION;
  return byThread[threadId] ?? DEFAULT_SELECTION;
}
