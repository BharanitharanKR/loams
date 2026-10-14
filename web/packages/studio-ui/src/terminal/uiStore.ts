// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/terminalUiStateStore.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// keyed by thread id; tabs carry the daemon's terminal id, label, cwd and shell; open state and height left to Studio's layout; server-metadata reconciliation and suppression dropped; persisted per browser tab in sessionStorage with every access wrapped. Loams' changes are Apache-2.0.

/**
 * Terminal tabs and splits per thread (§53 §5.7), in one zustand store (Q819).
 *
 * loams-agentd chooses terminal ids (`OpenTerminal`), so a tab is added once the
 * daemon has opened its terminal. The groups are upstream's: a group is the set of
 * terminals shown side by side (or stacked), at most four. The store is kept in
 * sessionStorage (`loams.studio.terminals`), so a reload of this browser tab finds its
 * terminals again and reattaches with the daemon's replay.
 */

import { create } from "zustand";
import { createJSONStorage, persist, type StateStorage } from "zustand/middleware";

export const MAX_TERMINALS_PER_GROUP = 4;
export const TERMINALS_STORAGE_KEY = "loams.studio.terminals";

export interface ThreadTerminalGroup {
  id: string;
  terminalIds: string[];
  splitDirection?: "horizontal" | "vertical";
}

/** A tab: the daemon's terminal and how the drawer names it. */
export interface TerminalTab {
  id: string;
  label: string;
  cwd: string;
  shell: string;
}

export interface ThreadTerminalUiState {
  terminalIds: string[];
  activeTerminalId: string;
  terminalGroups: ThreadTerminalGroup[];
  activeTerminalGroupId: string;
  tabs: Record<string, TerminalTab>;
}

function normalizeTerminalIds(terminalIds: string[]): string[] {
  const normalizedIds: string[] = [];
  const seen = new Set<string>();
  for (const id of terminalIds) {
    const trimmedId = id.trim();
    if (trimmedId.length === 0 || seen.has(trimmedId)) continue;
    seen.add(trimmedId);
    normalizedIds.push(trimmedId);
  }
  return normalizedIds;
}

function fallbackGroupId(terminalId: string): string {
  return `group-${terminalId}`;
}

function assignUniqueGroupId(baseId: string, usedGroupIds: Set<string>): string {
  let candidate = baseId;
  let index = 2;
  while (usedGroupIds.has(candidate)) {
    candidate = `${baseId}-${index}`;
    index += 1;
  }
  usedGroupIds.add(candidate);
  return candidate;
}

function findGroupIndexByTerminalId(
  terminalGroups: ThreadTerminalGroup[],
  terminalId: string,
): number {
  return terminalGroups.findIndex((group) => group.terminalIds.includes(terminalId));
}

export function normalizeTerminalGroups(
  terminalGroups: ThreadTerminalGroup[],
  terminalIds: string[],
): ThreadTerminalGroup[] {
  if (terminalIds.length === 0) {
    return [];
  }

  const validTerminalIdSet = new Set(terminalIds);
  const assignedTerminalIds = new Set<string>();
  const nextGroups: ThreadTerminalGroup[] = [];
  const usedGroupIds = new Set<string>();

  for (const group of terminalGroups) {
    const groupTerminalIds = normalizeTerminalIds(group.terminalIds).filter((terminalId) => {
      if (!validTerminalIdSet.has(terminalId)) return false;
      if (assignedTerminalIds.has(terminalId)) return false;
      return true;
    });
    if (groupTerminalIds.length === 0) continue;
    for (const terminalId of groupTerminalIds) {
      assignedTerminalIds.add(terminalId);
    }
    const baseGroupId =
      group.id.trim().length > 0
        ? group.id.trim()
        : fallbackGroupId(groupTerminalIds[0] ?? terminalIds[0] ?? "");
    nextGroups.push({
      id: assignUniqueGroupId(baseGroupId, usedGroupIds),
      terminalIds: groupTerminalIds,
      ...(group.splitDirection === "vertical" ? { splitDirection: "vertical" as const } : {}),
    });
  }

  for (const terminalId of terminalIds) {
    if (assignedTerminalIds.has(terminalId)) continue;
    nextGroups.push({
      id: assignUniqueGroupId(fallbackGroupId(terminalId), usedGroupIds),
      terminalIds: [terminalId],
    });
  }

  return nextGroups;
}

export const DEFAULT_THREAD_TERMINAL_UI_STATE: ThreadTerminalUiState = Object.freeze({
  terminalIds: [],
  activeTerminalId: "",
  terminalGroups: [],
  activeTerminalGroupId: "",
  tabs: {},
});

function normalizeThreadTerminalUiState(state: ThreadTerminalUiState): ThreadTerminalUiState {
  const nextTerminalIds = normalizeTerminalIds(state.terminalIds).filter(
    (id) => state.tabs[id] !== undefined,
  );
  const activeTerminalId = nextTerminalIds.includes(state.activeTerminalId)
    ? state.activeTerminalId
    : (nextTerminalIds[0] ?? "");
  const terminalGroups = normalizeTerminalGroups(state.terminalGroups, nextTerminalIds);
  const activeGroupIdFromState = terminalGroups.some(
    (group) => group.id === state.activeTerminalGroupId,
  )
    ? state.activeTerminalGroupId
    : null;
  const activeGroupIdFromTerminal =
    terminalGroups.find((group) => group.terminalIds.includes(activeTerminalId))?.id ?? null;
  const tabs = Object.fromEntries(nextTerminalIds.map((id) => [id, state.tabs[id] as TerminalTab]));
  const normalized: ThreadTerminalUiState = {
    terminalIds: nextTerminalIds,
    activeTerminalId,
    terminalGroups,
    activeTerminalGroupId:
      activeGroupIdFromState ?? activeGroupIdFromTerminal ?? terminalGroups[0]?.id ?? "",
    tabs,
  };
  return threadTerminalUiStateEqual(state, normalized) ? state : normalized;
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let index = 0; index < a.length; index += 1) {
    if (a[index] !== b[index]) return false;
  }
  return true;
}

function terminalGroupsEqual(left: ThreadTerminalGroup[], right: ThreadTerminalGroup[]): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const leftGroup = left[index];
    const rightGroup = right[index];
    if (!leftGroup || !rightGroup) return false;
    if (leftGroup.id !== rightGroup.id) return false;
    if (
      (leftGroup.splitDirection ?? "horizontal") !== (rightGroup.splitDirection ?? "horizontal")
    ) {
      return false;
    }
    if (!arraysEqual(leftGroup.terminalIds, rightGroup.terminalIds)) return false;
  }
  return true;
}

function threadTerminalUiStateEqual(
  left: ThreadTerminalUiState,
  right: ThreadTerminalUiState,
): boolean {
  const leftTabs = Object.keys(left.tabs);
  return (
    left.activeTerminalId === right.activeTerminalId &&
    left.activeTerminalGroupId === right.activeTerminalGroupId &&
    arraysEqual(left.terminalIds, right.terminalIds) &&
    terminalGroupsEqual(left.terminalGroups, right.terminalGroups) &&
    leftTabs.length === Object.keys(right.tabs).length &&
    leftTabs.every((id) => left.tabs[id] === right.tabs[id])
  );
}

function copyTerminalGroups(groups: ThreadTerminalGroup[]): ThreadTerminalGroup[] {
  return groups.map((group) => ({
    id: group.id,
    terminalIds: [...group.terminalIds],
    ...(group.splitDirection === "vertical" ? { splitDirection: "vertical" as const } : {}),
  }));
}

/** Whether a split of the active group can take one more terminal. */
export function canSplit(state: ThreadTerminalUiState): boolean {
  const group =
    state.terminalGroups.find((g) => g.id === state.activeTerminalGroupId) ??
    state.terminalGroups.find((g) => g.terminalIds.includes(state.activeTerminalId));
  return (group?.terminalIds.length ?? 0) < MAX_TERMINALS_PER_GROUP;
}

export function upsertTerminalIntoGroups(
  state: ThreadTerminalUiState,
  tab: TerminalTab,
  mode: "split" | "new",
  splitDirection: "horizontal" | "vertical" = "horizontal",
): ThreadTerminalUiState {
  const normalized = normalizeThreadTerminalUiState(state);
  const terminalId = tab.id.trim();
  if (terminalId.length === 0) {
    return normalized;
  }
  const effectiveMode: "split" | "new" = normalized.terminalIds.length === 0 ? "new" : mode;

  const isNewTerminal = !normalized.terminalIds.includes(terminalId);
  const terminalIds = isNewTerminal
    ? [...normalized.terminalIds, terminalId]
    : normalized.terminalIds;
  const tabs = { ...normalized.tabs, [terminalId]: { ...tab, id: terminalId } };
  const terminalGroups = copyTerminalGroups(normalized.terminalGroups);

  const existingGroupIndex = findGroupIndexByTerminalId(terminalGroups, terminalId);
  if (existingGroupIndex >= 0) {
    const group = terminalGroups[existingGroupIndex] as ThreadTerminalGroup;
    group.terminalIds = group.terminalIds.filter((id) => id !== terminalId);
    if (group.terminalIds.length === 0) {
      terminalGroups.splice(existingGroupIndex, 1);
    }
  }

  if (effectiveMode === "new") {
    const usedGroupIds = new Set(terminalGroups.map((group) => group.id));
    const nextGroupId = assignUniqueGroupId(fallbackGroupId(terminalId), usedGroupIds);
    terminalGroups.push({ id: nextGroupId, terminalIds: [terminalId] });
    return normalizeThreadTerminalUiState({
      terminalIds,
      activeTerminalId: terminalId,
      terminalGroups,
      activeTerminalGroupId: nextGroupId,
      tabs,
    });
  }

  let activeGroupIndex = terminalGroups.findIndex(
    (group) => group.id === normalized.activeTerminalGroupId,
  );
  if (activeGroupIndex < 0) {
    activeGroupIndex = findGroupIndexByTerminalId(terminalGroups, normalized.activeTerminalId);
  }
  if (activeGroupIndex < 0) {
    const usedGroupIds = new Set(terminalGroups.map((group) => group.id));
    const nextGroupId = assignUniqueGroupId(
      fallbackGroupId(normalized.activeTerminalId),
      usedGroupIds,
    );
    terminalGroups.push({ id: nextGroupId, terminalIds: [normalized.activeTerminalId] });
    activeGroupIndex = terminalGroups.length - 1;
  }

  const destinationGroup = terminalGroups[activeGroupIndex];
  if (!destinationGroup) {
    return normalized;
  }
  const destinationTerminalIdSet = new Set(destinationGroup.terminalIds);

  if (
    isNewTerminal &&
    !destinationTerminalIdSet.has(terminalId) &&
    destinationGroup.terminalIds.length >= MAX_TERMINALS_PER_GROUP
  ) {
    return normalized;
  }

  if (!destinationTerminalIdSet.has(terminalId)) {
    const anchorIndex = destinationGroup.terminalIds.indexOf(normalized.activeTerminalId);
    if (anchorIndex >= 0) {
      destinationGroup.terminalIds.splice(anchorIndex + 1, 0, terminalId);
    } else {
      destinationGroup.terminalIds.push(terminalId);
    }
  }
  if (splitDirection === "vertical") {
    destinationGroup.splitDirection = "vertical";
  } else {
    delete destinationGroup.splitDirection;
  }

  return normalizeThreadTerminalUiState({
    terminalIds,
    activeTerminalId: terminalId,
    terminalGroups,
    activeTerminalGroupId: destinationGroup.id,
    tabs,
  });
}

export function setThreadActiveTerminal(
  state: ThreadTerminalUiState,
  terminalId: string,
): ThreadTerminalUiState {
  const normalized = normalizeThreadTerminalUiState(state);
  if (!normalized.terminalIds.includes(terminalId)) {
    return normalized;
  }
  const activeTerminalGroupId =
    normalized.terminalGroups.find((group) => group.terminalIds.includes(terminalId))?.id ??
    normalized.activeTerminalGroupId;
  if (
    normalized.activeTerminalId === terminalId &&
    normalized.activeTerminalGroupId === activeTerminalGroupId
  ) {
    return normalized;
  }
  return {
    ...normalized,
    activeTerminalId: terminalId,
    activeTerminalGroupId,
  };
}

export function closeThreadTerminal(
  state: ThreadTerminalUiState,
  terminalId: string,
): ThreadTerminalUiState {
  const normalized = normalizeThreadTerminalUiState(state);
  if (!normalized.terminalIds.includes(terminalId)) {
    return normalized;
  }

  const remainingTerminalIds = normalized.terminalIds.filter((id) => id !== terminalId);
  if (remainingTerminalIds.length === 0) {
    return DEFAULT_THREAD_TERMINAL_UI_STATE;
  }

  const closedTerminalIndex = normalized.terminalIds.indexOf(terminalId);
  const nextActiveTerminalId =
    normalized.activeTerminalId === terminalId
      ? (remainingTerminalIds[Math.min(closedTerminalIndex, remainingTerminalIds.length - 1)] ??
        remainingTerminalIds[0] ??
        "")
      : normalized.activeTerminalId;

  const terminalGroups: ThreadTerminalGroup[] = [];
  for (const group of normalized.terminalGroups) {
    const terminalIds = group.terminalIds.filter((id) => id !== terminalId);
    if (terminalIds.length > 0) {
      terminalGroups.push({ ...group, terminalIds });
    }
  }

  const nextActiveTerminalGroupId =
    terminalGroups.find((group) => group.terminalIds.includes(nextActiveTerminalId))?.id ??
    terminalGroups[0]?.id ??
    fallbackGroupId(nextActiveTerminalId);

  const { [terminalId]: _closed, ...tabs } = normalized.tabs;
  return normalizeThreadTerminalUiState({
    terminalIds: remainingTerminalIds,
    activeTerminalId: nextActiveTerminalId,
    terminalGroups,
    activeTerminalGroupId: nextActiveTerminalGroupId,
    tabs,
  });
}

export function selectThreadTerminalUiState(
  terminalUiStateByThreadId: Record<string, ThreadTerminalUiState>,
  threadId: string | null | undefined,
): ThreadTerminalUiState {
  if (!threadId) return DEFAULT_THREAD_TERMINAL_UI_STATE;
  return terminalUiStateByThreadId[threadId] ?? DEFAULT_THREAD_TERMINAL_UI_STATE;
}

function updateTerminalUiStateByThreadId(
  terminalUiStateByThreadId: Record<string, ThreadTerminalUiState>,
  threadId: string,
  updater: (state: ThreadTerminalUiState) => ThreadTerminalUiState,
): Record<string, ThreadTerminalUiState> {
  if (threadId.length === 0) return terminalUiStateByThreadId;
  const current = selectThreadTerminalUiState(terminalUiStateByThreadId, threadId);
  const next = updater(current);
  if (next === current) return terminalUiStateByThreadId;
  if (next.terminalIds.length === 0) {
    if (terminalUiStateByThreadId[threadId] === undefined) return terminalUiStateByThreadId;
    const { [threadId]: _removed, ...rest } = terminalUiStateByThreadId;
    return rest;
  }
  return { ...terminalUiStateByThreadId, [threadId]: next };
}

export interface TerminalUiStateStoreState {
  terminalUiStateByThreadId: Record<string, ThreadTerminalUiState>;
  /** Adds a terminal the daemon opened: its own group (`new`) or a split of the active one. */
  addTerminal: (
    threadId: string,
    tab: TerminalTab,
    mode?: "new" | "split" | "splitVertical",
  ) => void;
  setActiveTerminal: (threadId: string, terminalId: string) => void;
  closeTerminal: (threadId: string, terminalId: string) => void;
  clearThread: (threadId: string) => void;
}

/** sessionStorage, where every access may throw (a private window, blocked storage). */
const safeSessionStorage: StateStorage = {
  getItem(name) {
    try {
      return globalThis.sessionStorage?.getItem(name) ?? null;
    } catch {
      return null;
    }
  },
  setItem(name, value) {
    try {
      globalThis.sessionStorage?.setItem(name, value);
    } catch {
      // Storage blocked: the tabs still work for this page.
    }
  },
  removeItem(name) {
    try {
      globalThis.sessionStorage?.removeItem(name);
    } catch {
      // As above.
    }
  },
};

function isTab(value: unknown): value is TerminalTab {
  const tab = value as Partial<TerminalTab> | null;
  return (
    typeof tab === "object" &&
    tab !== null &&
    typeof tab.id === "string" &&
    typeof tab.label === "string" &&
    typeof tab.cwd === "string" &&
    typeof tab.shell === "string"
  );
}

/** Keeps only well-formed stored threads; anything else is dropped, not trusted. */
export function sanitizePersistedTerminals(
  persisted: unknown,
): Pick<TerminalUiStateStoreState, "terminalUiStateByThreadId"> {
  const raw = (persisted as { terminalUiStateByThreadId?: unknown } | null)
    ?.terminalUiStateByThreadId;
  const out: Record<string, ThreadTerminalUiState> = {};
  if (!raw || typeof raw !== "object") return { terminalUiStateByThreadId: out };
  for (const [threadId, value] of Object.entries(raw as Record<string, unknown>)) {
    const state = value as Partial<ThreadTerminalUiState> | null;
    if (!state || typeof state !== "object" || !Array.isArray(state.terminalIds)) continue;
    const tabs = Object.fromEntries(
      Object.entries(state.tabs ?? {}).filter(([id, tab]) => isTab(tab) && tab.id === id),
    ) as Record<string, TerminalTab>;
    const groups = Array.isArray(state.terminalGroups)
      ? state.terminalGroups.filter(
          (g): g is ThreadTerminalGroup =>
            typeof g?.id === "string" &&
            Array.isArray(g.terminalIds) &&
            g.terminalIds.every((id) => typeof id === "string"),
        )
      : [];
    const normalized = normalizeThreadTerminalUiState({
      terminalIds: state.terminalIds.filter((id): id is string => typeof id === "string"),
      activeTerminalId: typeof state.activeTerminalId === "string" ? state.activeTerminalId : "",
      terminalGroups: groups,
      activeTerminalGroupId:
        typeof state.activeTerminalGroupId === "string" ? state.activeTerminalGroupId : "",
      tabs,
    });
    if (normalized.terminalIds.length > 0) out[threadId] = normalized;
  }
  return { terminalUiStateByThreadId: out };
}

export const useTerminalUiStateStore = create<TerminalUiStateStoreState>()(
  persist(
    (set) => {
      const update = (
        threadId: string,
        updater: (state: ThreadTerminalUiState) => ThreadTerminalUiState,
      ) =>
        set((state) => {
          const next = updateTerminalUiStateByThreadId(
            state.terminalUiStateByThreadId,
            threadId,
            updater,
          );
          return next === state.terminalUiStateByThreadId
            ? state
            : { terminalUiStateByThreadId: next };
        });
      return {
        terminalUiStateByThreadId: {},
        addTerminal: (threadId, tab, mode = "new") =>
          update(threadId, (state) =>
            upsertTerminalIntoGroups(
              state,
              tab,
              mode === "new" ? "new" : "split",
              mode === "splitVertical" ? "vertical" : "horizontal",
            ),
          ),
        setActiveTerminal: (threadId, terminalId) =>
          update(threadId, (state) => setThreadActiveTerminal(state, terminalId)),
        closeTerminal: (threadId, terminalId) =>
          update(threadId, (state) => closeThreadTerminal(state, terminalId)),
        clearThread: (threadId) => update(threadId, () => DEFAULT_THREAD_TERMINAL_UI_STATE),
      };
    },
    {
      name: TERMINALS_STORAGE_KEY,
      version: 1,
      storage: createJSONStorage(() => safeSessionStorage),
      migrate: (persisted) => sanitizePersistedTerminals(persisted) as TerminalUiStateStoreState,
      merge: (persisted, current) => ({ ...current, ...sanitizePersistedTerminals(persisted) }),
      partialize: (state) => ({ terminalUiStateByThreadId: state.terminalUiStateByThreadId }),
    },
  ),
);
