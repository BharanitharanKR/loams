// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/threadActionMenu.logic.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// Studio's thread menu: pin, move, rename, fork, copy, archive and delete, with plain menu items in place of the contracts' ContextMenuItem; settle, snooze, auto-settle, title regeneration and draft rows dropped. Loams' changes are Apache-2.0.

// The per-thread action menu (SD1 Task 17; §53 §5.3): one model for the row's context
// menu and its "More" button, so both list the same actions in the same order. Pinned
// threads also get Move up and Move down, the keyboard's way to reorder the pinned
// block (dragging is the pointer's).

export type ThreadActionMenuId =
  | "pin"
  | "unpin"
  | "move-up"
  | "move-down"
  | "rename"
  | "fork"
  | "copy-path"
  | "copy-branch"
  | "copy-thread-id"
  | "archive"
  | "unarchive"
  | "delete";

/** A menu row (upstream's `ContextMenuItem`, without icons or submenus). */
export interface ThreadActionMenuItem {
  readonly id: ThreadActionMenuId;
  readonly label: string;
  readonly disabled?: boolean;
  readonly destructive?: boolean;
  readonly separatorBefore?: boolean;
}

export interface ThreadActionMenuState {
  readonly canOperate: boolean;
  readonly branch: string | null;
  /** The worktree or checkout path to copy; null when the thread has none. */
  readonly path: string | null;
  readonly isPinned: boolean;
  /** For Move up and Move down: whether the thread can move that way in the pinned block. */
  readonly canMoveUp: boolean;
  readonly canMoveDown: boolean;
  readonly isArchived: boolean;
  /** Archive rejects a running thread, so disable it here rather than let the action fail. */
  readonly isRunning: boolean;
}

/** Local navigation and copying remain available to read-only clients. */
export function threadActionRequiresOperate(action: ThreadActionMenuId): boolean {
  return !["copy-path", "copy-branch", "copy-thread-id"].includes(action);
}

/**
 * Single source for the per-thread action menu: the sidebar row's right-click
 * menu and its "More" button share labels, ordering, and capability gating.
 */
export function buildThreadActionMenuItems(
  state: ThreadActionMenuState,
): ReadonlyArray<ThreadActionMenuItem> {
  const items: ReadonlyArray<ThreadActionMenuItem> = [
    state.isPinned
      ? { id: "unpin" as const, label: "Unpin thread" }
      : { id: "pin" as const, label: "Pin thread" },
    ...(state.isPinned
      ? [
          { id: "move-up" as const, label: "Move up", disabled: !state.canMoveUp },
          { id: "move-down" as const, label: "Move down", disabled: !state.canMoveDown },
        ]
      : []),
    { id: "rename", label: "Rename thread", separatorBefore: true },
    { id: "fork", label: "Fork thread" },
    ...(state.path
      ? [{ id: "copy-path" as const, label: "Copy path", separatorBefore: true }]
      : []),
    ...(state.branch ? [{ id: "copy-branch" as const, label: "Copy branch" }] : []),
    { id: "copy-thread-id", label: "Copy thread ID", separatorBefore: !state.path },
    // Archive removes the thread from the sidebar while keeping its conversation;
    // Delete clears it for good, so it sits beside Archive with destructive styling.
    state.isArchived
      ? { id: "unarchive" as const, label: "Unarchive thread", separatorBefore: true }
      : {
          id: "archive" as const,
          label: "Archive thread",
          disabled: state.isRunning,
          separatorBefore: true,
        },
    {
      id: "delete",
      label: "Delete",
      destructive: true,
    },
  ];
  return state.canOperate
    ? items
    : items.map((item) =>
        threadActionRequiresOperate(item.id) ? { ...item, disabled: true } : item,
      );
}
