// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/Sidebar.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the row, status, search, sort, traversal, jump-hint and drag-plan rules kept as plain TypeScript over agentd threads and sessions (millisecond times, two sections: pinned and the rest, pins placed before or after a neighbour); settle, snooze, the working shelf, lineage, multi-select and pull-request terms dropped; no Effect, contracts or client-runtime. Loams' changes are Apache-2.0.

// The sidebar's pure rules (SD1 Task 17; §53 §5.3; inventory (a)). Upstream's sidebar
// has five shelves (pinned, active, working, snoozed, settled); agentd has pins and
// everything else, so Studio keeps two sections: the pinned block, in the order agentd
// stores, and the rest, newest first. A drop inside the pinned block or into it becomes
// one `ChangeSidebarPin` placed before or after a neighbour; a drop out of it unpins.
// Statuses come from the thread's session and its pending approvals.

import * as React from "react";
import { sortThreads, type ThreadSortInput } from "./threadSort";
import type { ThreadRunStatus } from "./types";

export const THREAD_JUMP_HINT_SHOW_DELAY_MS = 200;

export function resolveSidebarRowAccessibility(input: {
  readonly title: string;
  readonly statusLabel: string | null;
  readonly projectDisplayName: string | null;
  readonly isActive: boolean;
}): { readonly label: string; readonly current: "page" | undefined } {
  return {
    // The title is the row's identity and must lead when users scan tasks.
    // Only static context belongs here; nested action labels remain separate controls.
    label: [input.title, input.statusLabel, input.projectDisplayName].filter(Boolean).join(", "),
    current: input.isActive ? "page" : undefined,
  };
}

// Rows and section markers share one sortable list. The divider resolves the
// lifecycle action: above it a thread is pinned, below it it is not. Pinned
// threads keep the dragged position; the rest use time order.

export type SidebarSection = "pinned" | "active";

/** Resolve the section a visible thread belongs to. */
export function resolveSidebarThreadSection(input: { readonly pinned: boolean }): SidebarSection {
  return input.pinned ? "pinned" : "active";
}

/** Sortable ids: thread rows use their key; structural items use a prefix no
    thread key starts with. */
const SIDEBAR_MARKER_PREFIX = "sidebar-marker-";

export type SidebarListMarker =
  /** The top boundary is also a landing target when there are no pins. */
  | "pinned-header"
  /** A stand-in row so an empty section has somewhere for the gap to open. */
  | "active-placeholder"
  /** The boundary between pinned and active rows. */
  | "pinned-divider";

export function sidebarMarkerId(marker: SidebarListMarker): string {
  return `${SIDEBAR_MARKER_PREFIX}${marker}`;
}

export type SidebarListItem =
  | { readonly kind: "thread"; readonly key: string; readonly section: SidebarSection }
  | { readonly kind: "marker"; readonly marker: SidebarListMarker };

export function sidebarListItemId(item: SidebarListItem): string {
  return item.kind === "thread" ? item.key : sidebarMarkerId(item.marker);
}

/** The section a slot belongs to, read off the markers around it: everything
    before the pinned divider is pinned, everything after it is not. */
function sectionAtSidebarSlot(items: readonly SidebarListItem[], index: number): SidebarSection {
  let section: SidebarSection = "pinned";
  for (let i = 0; i < index && i < items.length; i += 1) {
    const item = items[i];
    if (item?.kind === "marker" && item.marker === "pinned-divider") section = "active";
  }
  return section;
}

/** Resolve the destination section and the pinned order from an arrayMove across
 * the divider. */
export type SidebarDropTarget = {
  readonly section: SidebarSection;
  readonly pinnedOrder: readonly string[];
  readonly activeOrder: readonly string[];
};

export function resolveSidebarDropTarget(
  items: readonly SidebarListItem[],
  activeKey: string,
  overId: string,
): SidebarDropTarget | null {
  const activeIndex = items.findIndex((item) => sidebarListItemId(item) === activeKey);
  const overIndex = items.findIndex((item) => sidebarListItemId(item) === overId);
  const active = items[activeIndex];
  if (activeIndex === -1 || overIndex === -1 || active?.kind !== "thread") return null;
  const moved = items.filter((_, index) => index !== activeIndex);
  moved.splice(overIndex, 0, active);
  const section = sectionAtSidebarSlot(moved, overIndex);
  const pinnedOrder: string[] = [];
  const activeOrder: string[] = [];
  let currentSection: SidebarSection = "pinned";
  for (const item of moved) {
    if (item.kind === "marker") {
      if (item.marker === "pinned-divider") currentSection = "active";
    } else if (currentSection === "pinned") pinnedOrder.push(item.key);
    else activeOrder.push(item.key);
  }
  return { section, pinnedOrder, activeOrder };
}

/** Where agentd puts a pin: after one neighbour, before another, or last when
    neither is given (`SidebarPinPosition`). */
export interface PinPosition {
  readonly after?: string;
  readonly before?: string;
}

/** The position that realizes `order` for `movedId`: after the row above it,
    else before the row below it. */
export function pinPositionInOrder(order: readonly string[], movedId: string): PinPosition {
  const index = order.indexOf(movedId);
  if (index > 0) return { after: order[index - 1] as string };
  const below = index === -1 ? undefined : order[index + 1];
  return below === undefined ? {} : { before: below };
}

export type SidebarThreadDropPlan =
  | { readonly kind: "none" }
  /** Within the pinned block: one move. */
  | {
      readonly kind: "reorder-pinned";
      readonly order: readonly string[];
      readonly position: PinPosition;
    }
  /** From the rest into the pinned block. */
  | { readonly kind: "pin"; readonly order: readonly string[]; readonly position: PinPosition }
  /** Out of the pinned block. The rest is time-ordered, so the drop has no placement. */
  | { readonly kind: "unpin" };

/** What dropping in `to` does to a thread lifted from `from`, for the badge
    on the lifted row. Null while reordering inside one section. */
export type SidebarDropVerb = "pin" | "unpin";

export function resolveSidebarDropVerb(
  from: SidebarSection,
  to: SidebarSection | null,
): SidebarDropVerb | null {
  if (to === null || to === from) return null;
  return to === "pinned" ? "pin" : "unpin";
}

export function planSidebarThreadDrop(input: {
  readonly activeKey: string;
  readonly activeSection: SidebarSection;
  readonly target: SidebarDropTarget;
  /** All pinned keys in displayed order before the drop. */
  readonly pinnedOrder: readonly string[];
}): SidebarThreadDropPlan {
  const { activeKey, activeSection, target, pinnedOrder } = input;
  if (target.section === "active") {
    // Like upstream's time-ordered inbox: threads can enter it, but not be
    // arranged inside it.
    return activeSection === "pinned" ? { kind: "unpin" } : { kind: "none" };
  }
  const order = target.pinnedOrder;
  // Dropped back where it started: nothing to write.
  if (
    activeSection === "pinned" &&
    order.length === pinnedOrder.length &&
    order.every((key, index) => key === pinnedOrder[index])
  ) {
    return { kind: "none" };
  }
  const position = pinPositionInOrder(order, activeKey);
  return activeSection === "pinned"
    ? { kind: "reorder-pinned", order, position }
    : { kind: "pin", order, position };
}

/**
 * Move up / Move down for a pinned thread (upstream's `planPinnedMove`, mobile's menu
 * actions; Studio's keyboard alternative to dragging): swap the moved thread with its
 * displayed neighbour. Null when the move falls off either end of the list.
 */
export function planPinnedMove(input: {
  readonly orderedIds: readonly string[];
  readonly movedId: string;
  readonly direction: "up" | "down";
}): { readonly order: readonly string[]; readonly position: PinPosition } | null {
  const { orderedIds, movedId, direction } = input;
  const from = orderedIds.indexOf(movedId);
  if (from === -1) return null;
  const to = direction === "up" ? from - 1 : from + 1;
  if (to < 0 || to >= orderedIds.length) return null;
  const order = [...orderedIds];
  order.splice(from, 1);
  order.splice(to, 0, movedId);
  return { order, position: pinPositionInOrder(order, movedId) };
}

export type ThreadTraversalDirection = "previous" | "next";

export interface ThreadJumpHintVisibilityController {
  sync: (shouldShow: boolean) => void;
  dispose: () => void;
}

export function createThreadJumpHintVisibilityController(input: {
  delayMs: number;
  onVisibilityChange: (visible: boolean) => void;
  setTimeoutFn?: typeof globalThis.setTimeout;
  clearTimeoutFn?: typeof globalThis.clearTimeout;
}): ThreadJumpHintVisibilityController {
  const setTimeoutFn = input.setTimeoutFn ?? globalThis.setTimeout;
  const clearTimeoutFn = input.clearTimeoutFn ?? globalThis.clearTimeout;
  let isVisible = false;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  const clearPendingShow = () => {
    if (timeoutId === null) {
      return;
    }
    clearTimeoutFn(timeoutId);
    timeoutId = null;
  };

  return {
    sync: (shouldShow) => {
      if (!shouldShow) {
        clearPendingShow();
        if (isVisible) {
          isVisible = false;
          input.onVisibilityChange(false);
        }
        return;
      }

      if (isVisible || timeoutId !== null) {
        return;
      }

      timeoutId = setTimeoutFn(() => {
        timeoutId = null;
        isVisible = true;
        input.onVisibilityChange(true);
      }, input.delayMs);
    },
    dispose: () => {
      clearPendingShow();
    },
  };
}

export function useThreadJumpHintVisibility(): {
  showThreadJumpHints: boolean;
  updateThreadJumpHintsVisibility: (shouldShow: boolean) => void;
} {
  const [showThreadJumpHints, setShowThreadJumpHints] = React.useState(false);
  const controllerRef = React.useRef<ThreadJumpHintVisibilityController | null>(null);

  React.useEffect(() => {
    const controller = createThreadJumpHintVisibilityController({
      delayMs: THREAD_JUMP_HINT_SHOW_DELAY_MS,
      onVisibilityChange: (visible) => {
        setShowThreadJumpHints(visible);
      },
      setTimeoutFn: window.setTimeout.bind(window),
      clearTimeoutFn: window.clearTimeout.bind(window),
    });
    controllerRef.current = controller;

    return () => {
      controller.dispose();
      controllerRef.current = null;
    };
  }, []);

  const updateThreadJumpHintsVisibility = React.useCallback((shouldShow: boolean) => {
    controllerRef.current?.sync(shouldShow);
  }, []);

  return {
    showThreadJumpHints,
    updateThreadJumpHintsVisibility,
  };
}

type ThreadStatusInput = {
  readonly status: ThreadRunStatus;
  readonly pendingApprovals: number;
  readonly lastMessageAt: number | null;
  readonly lastSeenAt: number | null;
};

/**
 * Whether a thread finished something the person has not seen: its last message is
 * newer than their last visit, and it is not running. A thread never opened counts as
 * read, as upstream treats a missing visit marker.
 */
export function hasUnseenCompletion(thread: ThreadStatusInput): boolean {
  if (thread.status === "working") return false;
  if (thread.lastMessageAt === null || !Number.isFinite(thread.lastMessageAt)) return false;
  if (thread.lastSeenAt === null) return false;
  if (!Number.isFinite(thread.lastSeenAt)) return true;
  return thread.lastMessageAt > thread.lastSeenAt;
}

// A double-click dispatches two `click` events before `dblclick`: the first has
// `detail === 1`, the second `detail === 2`. The second click must not run the
// row's single-click navigation, otherwise double-click-to-rename would also
// navigate. `MouseEvent.detail` is 0 for synthetic/keyboard activations, which
// still count as a normal single activation.
export function isTrailingDoubleClick(detail: number): boolean {
  return detail > 1;
}

export function orderItemsByPreferredIds<TItem, TId>(input: {
  items: readonly TItem[];
  preferredIds: readonly TId[];
  getId: (item: TItem) => TId;
  getPreferenceIds?: (item: TItem) => readonly TId[];
}): TItem[] {
  const { getId, getPreferenceIds, items, preferredIds } = input;
  if (preferredIds.length === 0) {
    return [...items];
  }

  const indexesByPreferenceId = new Map<TId, number[]>();
  for (const [index, item] of items.entries()) {
    const preferenceIds = getPreferenceIds?.(item) ?? [getId(item)];
    for (const preferenceId of new Set(preferenceIds)) {
      const indexes = indexesByPreferenceId.get(preferenceId);
      if (indexes) {
        indexes.push(index);
      } else {
        indexesByPreferenceId.set(preferenceId, [index]);
      }
    }
  }

  const emittedIndexes = new Set<number>();
  const ordered = preferredIds.flatMap((id) => {
    const index = indexesByPreferenceId
      .get(id)
      ?.find((candidate) => !emittedIndexes.has(candidate));
    if (index === undefined) {
      return [];
    }
    emittedIndexes.add(index);
    return [items[index] as TItem];
  });
  const remaining = items.filter((_, index) => !emittedIndexes.has(index));
  return [...ordered, ...remaining];
}

export function resolveAdjacentThreadId<T>(input: {
  threadIds: readonly T[];
  currentThreadId: T | null;
  direction: ThreadTraversalDirection;
}): T | null {
  const { currentThreadId, direction, threadIds } = input;

  if (threadIds.length === 0) {
    return null;
  }

  if (currentThreadId === null) {
    return direction === "previous" ? (threadIds.at(-1) ?? null) : (threadIds[0] ?? null);
  }

  const currentIndex = threadIds.indexOf(currentThreadId);
  if (currentIndex === -1) {
    return null;
  }

  if (direction === "previous") {
    return currentIndex > 0 ? (threadIds[currentIndex - 1] ?? null) : null;
  }

  return currentIndex < threadIds.length - 1 ? (threadIds[currentIndex + 1] ?? null) : null;
}

export function isContextMenuPointerDown(input: {
  button: number;
  ctrlKey: boolean;
  isMac: boolean;
}): boolean {
  if (input.button === 2) return true;
  return input.isMac && input.button === 0 && input.ctrlKey;
}

// ── Status model ───────────────────────────────────────────────────
// Colour is reserved for "act now" (approval, input), "in motion" (working)
// and "broken" (errored). Ready is the unlabelled resting state; an unread
// completion is tracked separately.
export type SidebarThreadStatus = "approval" | "input" | "working" | "failed" | "ready";

export function resolveSidebarThreadStatus(
  thread: Pick<ThreadStatusInput, "pendingApprovals" | "status">,
): SidebarThreadStatus {
  if (thread.pendingApprovals > 0) {
    return "approval";
  }
  if (thread.status === "awaiting-input") {
    return "input";
  }
  if (thread.status === "working") {
    return "working";
  }
  if (thread.status === "errored") {
    return "failed";
  }
  return "ready";
}

export function shouldShowSidebarDuration(status: SidebarThreadStatus): boolean {
  return status === "working";
}

export function formatWorkingDurationLabel(elapsedMs: number): string {
  const seconds = Number.isFinite(elapsedMs) ? Math.max(0, Math.floor(elapsedMs / 1000)) : 0;
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

export interface ThreadStatusPill {
  label: "Working" | "Pending Approval" | "Awaiting Input" | "Errored" | "Completed";
  /** @loams/ui token utilities, never a raw colour (console-ui-stack ruling 1). */
  colorClass: string;
  dotClass: string;
  pulse: boolean;
}

const THREAD_STATUS_PRIORITY: Record<ThreadStatusPill["label"], number> = {
  "Pending Approval": 5,
  "Awaiting Input": 4,
  Errored: 3.5,
  Working: 3,
  Completed: 1,
};

export function resolveThreadStatusPill(input: {
  thread: ThreadStatusInput;
}): ThreadStatusPill | null {
  const { thread } = input;

  if (thread.pendingApprovals > 0) {
    return {
      label: "Pending Approval",
      colorClass: "text-accent",
      dotClass: "bg-accent",
      pulse: false,
    };
  }

  if (thread.status === "awaiting-input") {
    return {
      label: "Awaiting Input",
      colorClass: "text-accent",
      dotClass: "bg-accent",
      pulse: false,
    };
  }

  if (thread.status === "working") {
    return {
      label: "Working",
      colorClass: "text-info",
      dotClass: "bg-info",
      pulse: true,
    };
  }

  if (thread.status === "errored") {
    return {
      label: "Errored",
      colorClass: "text-danger",
      dotClass: "bg-danger",
      pulse: false,
    };
  }

  if (hasUnseenCompletion(thread)) {
    return {
      label: "Completed",
      colorClass: "text-grow",
      dotClass: "bg-grow",
      pulse: false,
    };
  }

  return null;
}

export function resolveProjectStatusIndicator(
  statuses: ReadonlyArray<ThreadStatusPill | null>,
): ThreadStatusPill | null {
  let highestPriorityStatus: ThreadStatusPill | null = null;

  for (const status of statuses) {
    if (status === null) continue;
    if (
      highestPriorityStatus === null ||
      THREAD_STATUS_PRIORITY[status.label] > THREAD_STATUS_PRIORITY[highestPriorityStatus.label]
    ) {
      highestPriorityStatus = status;
    }
  }

  return highestPriorityStatus;
}

/**
 * Search the already-ordered sidebar thread collection by title, then by branch.
 * Keeping the input order means the pinned block and recency ordering remain stable
 * while the user narrows the list. Upstream also matched linked pull requests and
 * server-side content matches; Studio has neither in the sidebar.
 */
export function searchSidebarThreads<
  T extends { readonly id: string; readonly title: string; readonly branch?: string | null },
>(threads: readonly T[], query: string): T[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery.length === 0) return [];
  const titleMatches: T[] = [];
  const branchMatches: T[] = [];
  for (const thread of threads) {
    if (thread.title.toLowerCase().includes(normalizedQuery)) {
      titleMatches.push(thread);
    } else if (thread.branch?.toLowerCase().includes(normalizedQuery)) {
      branchMatches.push(thread);
    }
  }
  return [...titleMatches, ...branchMatches];
}

export function getFallbackThreadIdAfterDelete<
  T extends { readonly id: string; readonly projectId: string | null } & ThreadSortInput,
>(input: {
  threads: readonly T[];
  deletedThreadId: T["id"];
  sortOrder: "updated_at" | "created_at";
  deletedThreadIds?: ReadonlySet<T["id"]>;
}): T["id"] | null {
  const { deletedThreadId, deletedThreadIds, sortOrder, threads } = input;
  const deletedThread = threads.find((thread) => thread.id === deletedThreadId);
  if (!deletedThread) {
    return null;
  }

  return (
    sortThreads(
      threads.filter(
        (thread) =>
          thread.projectId === deletedThread.projectId &&
          thread.id !== deletedThreadId &&
          !deletedThreadIds?.has(thread.id),
      ),
      sortOrder,
    )[0]?.id ?? null
  );
}

type SidebarProject = {
  id: string;
  title: string;
  createdAt?: number | undefined;
  updatedAt?: number | undefined;
};

export type SidebarProjectSortOrder = "updated_at" | "created_at" | "manual";

export function getProjectSortTimestamp(
  project: SidebarProject,
  projectThreads: readonly ThreadSortInput[],
  sortOrder: Exclude<SidebarProjectSortOrder, "manual">,
): number {
  if (projectThreads.length > 0) {
    return projectThreads.reduce(
      (latest, thread) =>
        Math.max(
          latest,
          sortOrder === "created_at"
            ? thread.createdAt
            : (thread.latestUserMessageAt ?? thread.updatedAt),
        ),
      Number.NEGATIVE_INFINITY,
    );
  }

  if (sortOrder === "created_at") {
    return project.createdAt ?? Number.NEGATIVE_INFINITY;
  }
  return project.updatedAt ?? project.createdAt ?? Number.NEGATIVE_INFINITY;
}

export function sortProjectsForSidebar<
  TProject extends SidebarProject,
  TThread extends {
    readonly projectId: string | null;
    readonly archived: boolean;
  } & ThreadSortInput,
>(
  projects: readonly TProject[],
  threads: readonly TThread[],
  sortOrder: SidebarProjectSortOrder,
): TProject[] {
  if (sortOrder === "manual") {
    return [...projects];
  }
  const threadsByProjectId = new Map<string, TThread[]>();
  for (const thread of threads) {
    if (thread.projectId === null || thread.archived) continue;
    const existing = threadsByProjectId.get(thread.projectId) ?? [];
    existing.push(thread);
    threadsByProjectId.set(thread.projectId, existing);
  }

  // Each project's timestamp walks all of its threads, so compute it once
  // per project instead of once per comparison.
  return projects
    .map((project) => ({
      project,
      timestamp: getProjectSortTimestamp(
        project,
        threadsByProjectId.get(project.id) ?? [],
        sortOrder,
      ),
    }))
    .sort((left, right) => {
      const byTimestamp =
        right.timestamp === left.timestamp ? 0 : right.timestamp > left.timestamp ? 1 : -1;
      return (
        byTimestamp ||
        left.project.title.localeCompare(right.project.title) ||
        left.project.id.localeCompare(right.project.id)
      );
    })
    .map(({ project }) => project);
}
