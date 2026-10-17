// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/diffs/DiffCommentAnnotation.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// @loams/ui's Button, Textarea and Badge on tokens; a stored comment gains edit, resolve and an "outdated" badge (agentd's ReviewComment); the secondary action dropped. Loams' changes are Apache-2.0.

// One inline review comment, or the draft of one, under the lines it is about (SD1 Task 20).
// The body is a person's text and is rendered as text, never as markup.

import { Badge, Button, cx, Textarea } from "@loams/ui";
import { CheckIcon, MessageCircleIcon, PencilIcon, RotateCcwIcon, Trash2Icon } from "lucide-react";
import { useLayoutEffect, useRef, useState } from "react";

import { isCommentSubmitShortcut } from "./commentSubmitShortcut";

interface DiffCommentAnnotationProps {
  kind: "draft" | "comment";
  rangeLabel: string;
  text: string;
  onTextChange?: (text: string) => void;
  onCancel: () => void;
  onComment: (text: string) => void;
  onDelete?: () => void;
  /** Resolve or reopen a stored comment. */
  onResolve?: (resolved: boolean) => void;
  /** Turns a stored comment back into a draft holding its text. */
  onEdit?: () => void;
  resolved?: boolean;
  /** The daemon says the anchored lines are no longer in the current diff. */
  outdated?: boolean;
  placeholder?: string;
  submitLabel?: string;
  pending?: boolean;
  focusOnMount?: boolean;
}

/** The shared inline comment treatment for thread diffs. */
export function DiffCommentAnnotation({
  kind,
  rangeLabel,
  text,
  onTextChange,
  onCancel,
  onComment,
  onDelete,
  onResolve,
  onEdit,
  resolved = false,
  outdated = false,
  placeholder = "Add a comment…",
  submitLabel = "Comment",
  pending = false,
  focusOnMount = true,
}: DiffCommentAnnotationProps) {
  const [localDraftText, setLocalDraftText] = useState("");
  const displayedText = kind === "draft" && !onTextChange ? localDraftText : text;
  const trimmedText = displayedText.trim();
  const draftRef = useRef<HTMLDivElement | null>(null);

  useLayoutEffect(() => {
    if (kind !== "draft" || !focusOnMount) return;
    const frame = window.requestAnimationFrame(() => {
      draftRef.current?.querySelector("textarea")?.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [focusOnMount, kind]);

  if (kind === "comment") {
    return (
      <div
        data-diff-comment-annotation
        data-resolved={resolved || undefined}
        data-outdated={outdated || undefined}
        className={cx(
          "group/comment flex min-w-0 items-start gap-2.5 border-l-2 bg-surface px-3 py-2.5 font-sans text-ink",
          resolved ? "border-rule text-muted" : "border-accent",
        )}
        onPointerDown={(event) => event.stopPropagation()}
      >
        <MessageCircleIcon className="mt-0.5 size-3.5 shrink-0 text-accent-fg" aria-hidden="true" />
        <div className="min-w-0 flex-1">
          <div className="mb-0.5 flex flex-wrap items-center gap-1.5 text-[0.72rem] text-faint">
            <span>Line {rangeLabel}</span>
            {outdated ? <Badge title="The lines changed since this comment">outdated</Badge> : null}
            {resolved ? <Badge>resolved</Badge> : null}
          </div>
          <p className="m-0 min-w-0 whitespace-pre-wrap break-words text-[0.86rem] leading-5">
            {displayedText}
          </p>
        </div>
        <span className="-my-1 -mr-1 flex shrink-0 items-center gap-0.5">
          {onEdit && !resolved ? (
            <Button variant="quiet" size="icon" aria-label="Edit comment" onClick={onEdit}>
              <PencilIcon aria-hidden className="size-3" />
            </Button>
          ) : null}
          {onResolve ? (
            <Button
              variant="quiet"
              size="icon"
              aria-label={resolved ? "Reopen comment" : "Resolve comment"}
              onClick={() => onResolve(!resolved)}
            >
              {resolved ? (
                <RotateCcwIcon aria-hidden className="size-3" />
              ) : (
                <CheckIcon aria-hidden className="size-3" />
              )}
            </Button>
          ) : null}
          {onDelete ? (
            <Button variant="quiet" size="icon" aria-label="Delete comment" onClick={onDelete}>
              <Trash2Icon aria-hidden className="size-3" />
            </Button>
          ) : null}
        </span>
      </div>
    );
  }

  return (
    <div
      ref={draftRef}
      data-diff-comment-annotation
      className="bg-surface px-3 py-2 font-sans text-ink"
      onPointerDown={(event) => event.stopPropagation()}
    >
      <Textarea
        rows={3}
        className="w-full"
        value={displayedText}
        placeholder={placeholder}
        aria-label={`Comment on line ${rangeLabel}`}
        onChange={(event) => (onTextChange ?? setLocalDraftText)(event.target.value)}
        onFocus={(event) => {
          const end = event.currentTarget.value.length;
          event.currentTarget.setSelectionRange(end, end);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            onCancel();
          }
          if (isCommentSubmitShortcut(event, trimmedText, pending)) {
            event.preventDefault();
            onComment(trimmedText);
          }
        }}
      />
      <div className="mt-1.5 flex items-center gap-1">
        <span className="mr-auto text-[0.72rem] text-faint">⌘/Ctrl Enter to save</span>
        <Button variant="quiet" size="sm" onClick={onCancel}>
          Cancel
        </Button>
        <Button
          variant="primary"
          size="sm"
          disabled={pending || !trimmedText}
          onClick={() => onComment(trimmedText)}
        >
          {submitLabel}
        </Button>
      </div>
    </div>
  );
}
