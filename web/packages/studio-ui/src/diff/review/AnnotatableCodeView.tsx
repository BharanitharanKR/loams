// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/diffs/AnnotatableCodeView.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// comments are the daemon's ReviewComments (upsert, edit, resolve, delete through the caller) instead of composer-draft context; a list of per-file viewers with our own React file header (collapse, viewed, discard, open) in place of one CodeView and Pierre's header; the find-in-diff hook dropped (§53 §9.1). Loams' changes are Apache-2.0.

// The diff's files with inline review comments (SD1 Task 20). A comment is anchored to one
// side of the diff and a line or range (agentd's ReviewComment); the draft under the lines
// the reader picked becomes a stored comment through `onSave`. Comment bodies are text.

import { Button, Checkbox, cx, Tooltip } from "@loams/ui";
import type { DiffLineAnnotation, FileDiffMetadata, SelectedLineRange } from "@pierre/diffs";
import { ChevronDownIcon, ChevronRightIcon, FileTextIcon, Undo2Icon } from "lucide-react";
import { type ReactNode, useCallback, useMemo, useState } from "react";

import {
  anchorToSelection,
  formatAnchorLabel,
  type ReviewAnchor,
  selectionToAnchor,
} from "../reviewCommentContext";
import type { DiffFileStat, ReviewComment } from "../source";
import { DiffCommentAnnotation } from "./DiffCommentAnnotation";
import { StyledDiffCodeView, type StyledDiffCodeViewOptions } from "./StyledDiffCodeView";

interface DiffCommentAnnotationEntry {
  readonly id: string;
  readonly kind: "draft" | "comment";
  readonly anchor: ReviewAnchor;
  readonly comment?: ReviewComment;
}

interface DiffCommentAnnotationGroup {
  readonly entries: readonly DiffCommentAnnotationEntry[];
}

type DiffCommentLineAnnotation = DiffLineAnnotation<DiffCommentAnnotationGroup>;

export interface AnnotatableFile {
  readonly fileDiff: FileDiffMetadata;
  readonly filePath: string;
  readonly fileKey: string;
  readonly fileVersion: number;
  readonly stat?: DiffFileStat;
}

/** What a saved draft becomes: a new comment, or a stored one's new text. */
export interface CommentSave {
  readonly id?: string;
  readonly path: string;
  readonly anchor: ReviewAnchor;
  readonly body: string;
}

const DRAFT_ID = "__draft__";

function annotationSide(anchor: ReviewAnchor): "additions" | "deletions" {
  return anchor.side === "old" ? "deletions" : "additions";
}

function appendAnnotationEntry(
  annotations: ReadonlyArray<DiffCommentLineAnnotation>,
  entry: DiffCommentAnnotationEntry,
): DiffCommentLineAnnotation[] {
  const side = annotationSide(entry.anchor);
  // An outdated comment's lines may be gone from this diff: it shows above the file instead
  // (a file-level annotation, line 0), so it never disappears.
  const lineNumber = entry.comment?.outdated ? 0 : (entry.anchor.endLine ?? entry.anchor.line);
  const annotationIndex = annotations.findIndex(
    (annotation) => annotation.side === side && annotation.lineNumber === lineNumber,
  );
  if (annotationIndex < 0) {
    return [...annotations, { side, lineNumber, metadata: { entries: [entry] } }];
  }
  return annotations.map((annotation, index) =>
    index === annotationIndex
      ? { ...annotation, metadata: { entries: [...annotation.metadata.entries, entry] } }
      : annotation,
  );
}

function anchorOf(comment: ReviewComment): ReviewAnchor {
  return comment.endLine !== undefined
    ? { side: comment.side, line: comment.line, endLine: comment.endLine }
    : { side: comment.side, line: comment.line };
}

interface Draft {
  readonly fileKey: string;
  readonly filePath: string;
  readonly anchor: ReviewAnchor;
  /** Set when the draft edits a stored comment. */
  readonly editing?: string;
}

export function AnnotatableCodeView({
  files,
  comments,
  options,
  canComment,
  collapsed,
  onToggleCollapsed,
  viewed,
  onToggleViewed,
  onSave,
  onDelete,
  onResolve,
  onDiscardFile,
  onOpenFile,
  renderFileMeta,
  className,
}: {
  readonly files: readonly AnnotatableFile[];
  /** The checkout's comments; each shows under its file's anchored lines. */
  readonly comments: readonly ReviewComment[];
  readonly options: Omit<StyledDiffCodeViewOptions, "commentable">;
  /** Comments need a checkout and a diff checksum to anchor to. */
  readonly canComment: boolean;
  readonly collapsed: ReadonlySet<string>;
  readonly onToggleCollapsed: (fileKey: string) => void;
  readonly viewed: (file: AnnotatableFile) => boolean;
  readonly onToggleViewed: (file: AnnotatableFile, viewed: boolean) => void;
  readonly onSave: (save: CommentSave) => Promise<void>;
  readonly onDelete: (comment: ReviewComment) => void;
  readonly onResolve: (comment: ReviewComment, resolved: boolean) => void;
  /** Discard this file's changes; absent when the diff is not the working tree's. */
  readonly onDiscardFile?: (path: string) => void;
  readonly onOpenFile?: (path: string) => void;
  readonly renderFileMeta?: (file: AnnotatableFile) => ReactNode;
  readonly className?: string;
}) {
  const [draft, setDraft] = useState<Draft | null>(null);
  const [draftText, setDraftText] = useState("");
  const [saving, setSaving] = useState(false);

  const commentsByPath = useMemo(() => {
    const byPath = new Map<string, ReviewComment[]>();
    for (const comment of comments) {
      const list = byPath.get(comment.path) ?? [];
      list.push(comment);
      byPath.set(comment.path, list);
    }
    return byPath;
  }, [comments]);

  const annotationsFor = useCallback(
    (file: AnnotatableFile): DiffCommentLineAnnotation[] => {
      let annotations: DiffCommentLineAnnotation[] = [];
      for (const comment of commentsByPath.get(file.filePath) ?? []) {
        if (draft?.editing === comment.id) continue;
        annotations = appendAnnotationEntry(annotations, {
          id: comment.id,
          kind: "comment",
          anchor: anchorOf(comment),
          comment,
        });
      }
      if (draft?.fileKey === file.fileKey) {
        annotations = appendAnnotationEntry(annotations, {
          id: draft.editing ?? DRAFT_ID,
          kind: "draft",
          anchor: draft.anchor,
        });
      }
      return annotations;
    },
    [commentsByPath, draft],
  );

  const beginComment = (file: AnnotatableFile, range: SelectedLineRange) => {
    if (!canComment || draft) return;
    setDraftText("");
    setDraft({ fileKey: file.fileKey, filePath: file.filePath, anchor: selectionToAnchor(range) });
  };

  const cancel = () => {
    setDraft(null);
    setDraftText("");
  };

  const submit = async (text: string) => {
    if (!draft || !text) return;
    setSaving(true);
    try {
      await onSave({
        ...(draft.editing ? { id: draft.editing } : {}),
        path: draft.filePath,
        anchor: draft.anchor,
        body: text,
      });
      cancel();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className={cx("flex flex-col", className)} data-annotatable-code-view>
      {files.map((file) => {
        const isCollapsed = collapsed.has(file.fileKey);
        const isViewed = viewed(file);
        const annotations = annotationsFor(file);
        const draftHere = draft?.fileKey === file.fileKey ? draft : null;
        return (
          <section
            key={file.fileKey}
            className="border-b border-rule-soft"
            data-diff-file={file.filePath}
            aria-label={file.filePath}
          >
            <header className="sticky top-0 z-[1] flex h-8 items-center gap-1.5 border-b border-rule-soft bg-bg px-2 text-[0.8rem]">
              <Button
                variant="quiet"
                size="icon"
                aria-label={isCollapsed ? `Expand ${file.filePath}` : `Collapse ${file.filePath}`}
                aria-expanded={!isCollapsed}
                onClick={() => onToggleCollapsed(file.fileKey)}
              >
                {isCollapsed ? (
                  <ChevronRightIcon aria-hidden className="size-3.5" />
                ) : (
                  <ChevronDownIcon aria-hidden className="size-3.5" />
                )}
              </Button>
              <button
                type="button"
                className={
                  /* mono-ok: path */ "min-w-0 flex-1 cursor-pointer truncate border-0 bg-transparent p-0 text-left font-mono text-[0.78rem] text-ink hover:text-accent-fg"
                }
                title={onOpenFile ? `Open ${file.filePath}` : file.filePath}
                onClick={() => onOpenFile?.(file.filePath)}
                disabled={!onOpenFile}
              >
                {file.fileDiff.prevName && file.fileDiff.prevName !== file.filePath
                  ? `${file.fileDiff.prevName} → ${file.filePath}`
                  : file.filePath}
              </button>
              {renderFileMeta?.(file)}
              <span className="shrink-0 text-[0.72rem] tabular-nums">
                <span className="text-grow">+{file.stat?.additions ?? 0}</span>{" "}
                <span className="text-danger">−{file.stat?.deletions ?? 0}</span>
              </span>
              {onOpenFile ? (
                <Tooltip content="Open in Files">
                  <Button
                    variant="quiet"
                    size="icon"
                    aria-label={`Open ${file.filePath} in Files`}
                    onClick={() => onOpenFile(file.filePath)}
                  >
                    <FileTextIcon aria-hidden className="size-3.5" />
                  </Button>
                </Tooltip>
              ) : null}
              {onDiscardFile ? (
                <Tooltip content="Discard this file's changes">
                  <Button
                    variant="quiet"
                    size="icon"
                    aria-label={`Discard changes to ${file.filePath}`}
                    onClick={() => onDiscardFile(file.filePath)}
                  >
                    <Undo2Icon aria-hidden className="size-3.5" />
                  </Button>
                </Tooltip>
              ) : null}
              <span className="shrink-0 text-[0.75rem] text-muted">
                <Checkbox
                  label="Viewed"
                  checked={isViewed}
                  onChange={(event) => onToggleViewed(file, event.currentTarget.checked)}
                />
              </span>
            </header>
            {isCollapsed ? null : file.stat?.binary ? (
              <p className="m-0 px-3 py-2 text-[0.8rem] text-muted">Binary file not shown.</p>
            ) : (
              <StyledDiffCodeView<DiffCommentAnnotationGroup>
                fileDiff={file.fileDiff}
                annotations={annotations}
                options={{ ...options, commentable: canComment && draft === null }}
                selectedLines={draftHere ? anchorToSelection(draftHere.anchor) : null}
                onSelectLines={(range) => beginComment(file, range)}
                renderAnnotation={(annotation) => (
                  <div className="divide-y divide-rule-soft border-y border-rule-soft">
                    {annotation.metadata.entries.map((entry) => {
                      const label = formatAnchorLabel(entry.anchor);
                      if (entry.kind === "draft") {
                        return (
                          <DiffCommentAnnotation
                            key={entry.id}
                            kind="draft"
                            rangeLabel={label}
                            text={draftText}
                            onTextChange={setDraftText}
                            onCancel={cancel}
                            onComment={(text) => void submit(text)}
                            submitLabel={draftHere?.editing ? "Save" : "Comment"}
                            pending={saving}
                          />
                        );
                      }
                      const comment = entry.comment as ReviewComment;
                      return (
                        <DiffCommentAnnotation
                          key={entry.id}
                          kind="comment"
                          rangeLabel={label}
                          text={comment.body}
                          resolved={comment.resolved}
                          outdated={comment.outdated}
                          onCancel={cancel}
                          onComment={() => {}}
                          onDelete={() => onDelete(comment)}
                          onResolve={(resolved) => onResolve(comment, resolved)}
                          {...(canComment && draft === null
                            ? {
                                onEdit: () => {
                                  setDraftText(comment.body);
                                  setDraft({
                                    fileKey: file.fileKey,
                                    filePath: file.filePath,
                                    anchor: entry.anchor,
                                    editing: comment.id,
                                  });
                                },
                              }
                            : {})}
                        />
                      );
                    })}
                  </div>
                )}
              />
            )}
          </section>
        );
      })}
    </div>
  );
}
