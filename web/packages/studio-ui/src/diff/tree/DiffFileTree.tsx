// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/diffs/DiffFileTree.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// on Task 21's VirtualTree (a WAI-ARIA tree on TanStack Virtual) in place of @pierre/trees (ruling T21-1), with the change mark as a row's trailing letter; the folder expansion, diff order and "expand or collapse all" kept. Loams' changes are Apache-2.0.

// The diff's files as a directory tree (SD1 Task 20). Every directory starts open: a diff is
// a short list compared to a workspace, and the reader came for the files, not the folders.
// Folders keep the position of their first file in the diff, so the tree reads in the
// diff's order.

import { Button, cx, Tooltip } from "@loams/ui";
import { ChevronsDownUpIcon, ChevronsUpDownIcon } from "lucide-react";
import { type ReactNode, useMemo, useState } from "react";

import { type TreeRow, VirtualTree } from "../../files/VirtualTree";
import {
  collectDirectoryPaths,
  compareDiffFileTreeEntries,
  type DiffFileTreeEntry,
  diffFileTreePositions,
} from "./diffFileTree.logic";
import type { GitStatus } from "./treeTypes";

export type { DiffFileTreeEntry } from "./diffFileTree.logic";

const STATUS_MARK: Record<GitStatus, { letter: string; label: string; className: string }> = {
  added: { letter: "A", label: "added", className: "text-grow" },
  deleted: { letter: "D", label: "deleted", className: "text-danger" },
  modified: { letter: "M", label: "modified", className: "text-info" },
  renamed: { letter: "R", label: "renamed", className: "text-info" },
  untracked: { letter: "U", label: "untracked", className: "text-grow" },
  ignored: { letter: "I", label: "ignored", className: "text-faint" },
};

/** The rows the tree shows: directories (with a trailing slash in their id) and files. */
export function diffFileTreeRows(
  entries: ReadonlyArray<DiffFileTreeEntry>,
  collapsed: ReadonlySet<string>,
): TreeRow[] {
  const paths = entries.map((entry) => entry.path);
  const positions = diffFileTreePositions(paths);
  const compare = compareDiffFileTreeEntries(() => positions);
  const nodes = [
    ...collectDirectoryPaths(paths).map((path) => ({ path, directory: true })),
    ...paths.map((path) => ({ path, directory: false })),
  ].map((node) => ({
    ...node,
    depth: node.path.replace(/\/$/, "").split("/").length,
  }));
  const parentOf = (path: string) => {
    const trimmed = path.replace(/\/$/, "");
    const at = trimmed.lastIndexOf("/");
    return at < 0 ? "" : `${trimmed.slice(0, at)}/`;
  };
  const children = new Map<string, typeof nodes>();
  for (const node of nodes) {
    const parent = parentOf(node.path);
    const list = children.get(parent) ?? [];
    list.push(node);
    children.set(parent, list);
  }
  const rows: TreeRow[] = [];
  const visit = (parent: string) => {
    const list = (children.get(parent) ?? []).toSorted(compare);
    for (const node of list) {
      const trimmed = node.path.replace(/\/$/, "");
      const expanded = node.directory && !collapsed.has(node.path);
      rows.push({
        path: node.path,
        name: trimmed.slice(trimmed.lastIndexOf("/") + 1),
        kind: node.directory ? "directory" : "file",
        level: node.depth,
        expanded,
        loading: false,
        ignored: false,
        parent,
      });
      if (expanded) visit(node.path);
    }
  };
  visit("");
  return rows;
}

interface DiffFileTreeProps {
  readonly entries: ReadonlyArray<DiffFileTreeEntry>;
  /** Called with the file's path when the reader picks a file row. */
  readonly onSelectFile: (path: string) => void;
  /** The file the diff is currently showing, kept selected in the tree. */
  readonly selectedPath?: string | null;
  readonly ariaLabel: string;
  /** Right-aligned content in the header row, after the file count. */
  readonly headerAccessory?: ReactNode;
  readonly className?: string;
}

export function DiffFileTree({
  entries,
  onSelectFile,
  selectedPath = null,
  ariaLabel,
  headerAccessory,
  className,
}: DiffFileTreeProps) {
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => new Set());
  const directoryPaths = useMemo(
    () => collectDirectoryPaths(entries.map((entry) => entry.path)),
    [entries],
  );
  const rows = useMemo(() => diffFileTreeRows(entries, collapsed), [entries, collapsed]);
  const statusByPath = useMemo(
    () => new Map(entries.map((entry) => [entry.path, entry.status])),
    [entries],
  );
  const allDirectoriesExpanded = directoryPaths.every((path) => !collapsed.has(path));

  return (
    <div className={cx("flex min-h-0 flex-1 flex-col bg-bg", className)}>
      <div className="flex h-8 min-h-8 shrink-0 items-center gap-1 border-b border-rule-soft px-2 text-[0.78rem] text-muted">
        <span className="px-1 font-medium text-ink">Files</span>
        <span className="ml-auto tabular-nums">{entries.length}</span>
        {headerAccessory}
        {directoryPaths.length > 0 ? (
          <Tooltip content={allDirectoriesExpanded ? "Collapse all folders" : "Expand all folders"}>
            <Button
              variant="quiet"
              size="icon"
              aria-label={allDirectoriesExpanded ? "Collapse all folders" : "Expand all folders"}
              onClick={() =>
                setCollapsed(allDirectoriesExpanded ? new Set(directoryPaths) : new Set())
              }
            >
              {allDirectoriesExpanded ? (
                <ChevronsDownUpIcon aria-hidden className="size-3.5" />
              ) : (
                <ChevronsUpDownIcon aria-hidden className="size-3.5" />
              )}
            </Button>
          </Tooltip>
        ) : null}
      </div>
      <VirtualTree
        label={ariaLabel}
        rows={rows}
        selectedPath={selectedPath}
        revealPath={selectedPath}
        onOpen={(row) => onSelectFile(row.path)}
        onToggle={(row, expanded) =>
          setCollapsed((current) => {
            const next = new Set(current);
            if (expanded) next.delete(row.path);
            else next.add(row.path);
            return next;
          })
        }
        trailing={(row) => {
          if (row.kind === "directory") return null;
          const status = statusByPath.get(row.path);
          if (!status) return null;
          const mark = STATUS_MARK[status];
          return (
            <span
              className={cx("shrink-0 px-1 font-mono text-[0.7rem]", mark.className)}
              title={mark.label}
            >
              <span aria-hidden="true">{mark.letter}</span>
              <span className="sr-only">{mark.label}</span>
            </span>
          );
        }}
      />
    </div>
  );
}
