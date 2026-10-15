// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/diffs/commentSubmitShortcut.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// this header only. Loams' changes are Apache-2.0.
interface CommentSubmitShortcutEvent {
  readonly key: string;
  readonly metaKey: boolean;
  readonly ctrlKey: boolean;
}

/** Shared guard for inline comment composers that submit on Command/Ctrl+Enter. */
export function isCommentSubmitShortcut(
  event: CommentSubmitShortcutEvent,
  value: string,
  pending: boolean,
): boolean {
  return (
    !pending && (event.metaKey || event.ctrlKey) && event.key === "Enter" && value.trim().length > 0
  );
}
