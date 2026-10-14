// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/FileBrowserPanel.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// on the VirtualTree fallback instead of @pierre/trees, over a FilesSource with TanStack Query; "Mention in composer" is a row action; no context menu, drag mention or local API. Loams' changes are Apache-2.0.

// The project file tree (SD1 Task 21; inventory (e)). As upstream: a refresh button, a
// path search that shows only matching files with their folders, expand and collapse all,
// folders listed only when opened, and the open file revealed and selected. Picking a file
// opens it in the viewer; "@" or the row's button mentions it in the composer.

import { Button, InputGroup, InputGroupInput, Tooltip } from "@loams/ui";
import { useQuery } from "@tanstack/react-query";
import { ChevronsDownUp, ChevronsUpDown, RefreshCwIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { fileAncestors, fileBreadcrumbParent } from "./filePath";
import type { FileEntry, FileMatch, FilesSource } from "./source";
import { compareEntries, filesQueryKey, useDirectories } from "./useDirectory";
import { type TreeRow, VirtualTree } from "./VirtualTree";

const SEARCH_LIMIT = 200;

export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}

interface FileBrowserProps {
  readonly source: FilesSource;
  readonly projectName: string;
  /** The file open in the viewer; revealed and selected in the tree. */
  readonly selectedPath: string | null;
  /** A folder to show (a breadcrumb was pressed); bump `id` to show the same one again. */
  readonly revealDirectory?: { readonly path: string; readonly id: number } | null;
  readonly onOpenFile: (relativePath: string) => void;
  readonly onMention?: (relativePath: string) => void;
}

/** The visible rows: each open folder's children under it, depth first. */
function flatten(
  children: ReadonlyMap<string, readonly FileEntry[]>,
  expanded: ReadonlySet<string>,
  loading: ReadonlySet<string>,
): TreeRow[] {
  const rows: TreeRow[] = [];
  const visit = (directory: string, level: number) => {
    for (const entry of children.get(directory) ?? []) {
      const open = entry.kind === "directory" && expanded.has(entry.path);
      rows.push({
        path: entry.path,
        name: entry.name,
        kind: entry.kind,
        level,
        expanded: open,
        loading: open && loading.has(entry.path),
        ignored: entry.ignored ?? false,
        parent: directory,
      });
      if (open) visit(entry.path, level + 1);
    }
  };
  visit("", 1);
  return rows;
}

/** Search results as a tree: each matching file under its folders, all open. */
function searchTree(matches: readonly FileMatch[]): {
  children: Map<string, FileEntry[]>;
  expanded: Set<string>;
} {
  const children = new Map<string, FileEntry[]>();
  const expanded = new Set<string>();
  const seen = new Set<string>();
  const add = (entry: FileEntry) => {
    if (seen.has(entry.path)) return;
    seen.add(entry.path);
    const parent = fileBreadcrumbParent(entry.path) ?? "";
    children.set(parent, [...(children.get(parent) ?? []), entry]);
  };
  for (const match of matches) {
    if (match.kind === "directory") continue;
    for (const folder of fileAncestors(match.path)) {
      expanded.add(folder);
      add({ path: folder, name: folder.slice(folder.lastIndexOf("/") + 1), kind: "directory" });
    }
    add({ path: match.path, name: match.name, kind: match.kind });
  }
  for (const [parent, entries] of children) children.set(parent, entries.toSorted(compareEntries));
  return { children, expanded };
}

function RefreshFilesButton(props: { isPending: boolean; onRefresh: () => void }) {
  return (
    <Tooltip content={props.isPending ? "Refreshing…" : "Refresh files"}>
      <Button
        variant="quiet"
        size="icon"
        aria-label="Refresh workspace files"
        onClick={props.onRefresh}
      >
        <RefreshCwIcon
          aria-hidden
          className={props.isPending ? "size-3.5 motion-safe:animate-spin" : "size-3.5"}
        />
      </Button>
    </Tooltip>
  );
}

function FileSearchField(props: {
  ariaLabel: string;
  onClose: () => void;
  onValueChange: (value: string) => void;
  value: string;
}) {
  return (
    <InputGroup className="h-7 min-w-0 flex-1">
      <InputGroupInput
        type="search"
        name="project-files-search"
        value={props.value}
        aria-label={props.ariaLabel}
        placeholder="Search files"
        spellCheck={false}
        onChange={(event) => props.onValueChange(event.target.value)}
        onKeyDown={(event) => {
          if (event.key !== "Escape") return;
          props.onClose();
          event.currentTarget.blur();
        }}
      />
    </InputGroup>
  );
}

export function FileBrowser({
  source,
  projectName,
  selectedPath,
  revealDirectory,
  onOpenFile,
  onMention,
}: FileBrowserProps) {
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(() => new Set());
  const [query, setQuery] = useState("");
  const directories = useDirectories(source, expanded);
  const searchQuery = useDebouncedValue(query.trim().slice(0, 256), 200);
  const searching = query.trim().length > 0;
  const search = useQuery({
    queryKey: filesQueryKey(source, "search", searchQuery),
    queryFn: ({ signal }) => source.searchFiles(searchQuery, SEARCH_LIMIT, signal),
    enabled: searchQuery.length > 0,
    staleTime: 10_000,
  });

  // The open file's folders open, so its row exists to be revealed.
  useEffect(() => {
    if (!selectedPath) return;
    const ancestors = fileAncestors(selectedPath);
    setExpanded((previous) =>
      ancestors.every((path) => previous.has(path))
        ? previous
        : new Set([...previous, ...ancestors]),
    );
  }, [selectedPath]);

  useEffect(() => {
    if (!revealDirectory) return;
    setQuery("");
    const { path } = revealDirectory;
    setExpanded(
      (previous) => new Set([...previous, ...fileAncestors(path), ...(path ? [path] : [])]),
    );
  }, [revealDirectory]);

  const rows = useMemo(() => {
    if (searching && search.data) {
      const tree = searchTree(search.data);
      return flatten(tree.children, tree.expanded, new Set());
    }
    return flatten(directories.children, expanded, directories.loading);
  }, [searching, search.data, directories.children, directories.loading, expanded]);

  const knownDirectories = useMemo(() => {
    const paths: string[] = [];
    for (const entries of directories.children.values()) {
      for (const entry of entries) if (entry.kind === "directory") paths.push(entry.path);
    }
    return paths;
  }, [directories.children]);
  const allExpanded =
    knownDirectories.length > 0 && knownDirectories.every((path) => expanded.has(path));
  const toggleAllDirectories = () => {
    setExpanded(allExpanded ? new Set() : new Set([...expanded, ...knownDirectories]));
  };

  const handleRefresh = () => {
    directories.refresh();
    if (searching) void search.refetch();
  };

  const toggle = (row: TreeRow, open: boolean) => {
    if (searching) return;
    setExpanded((previous) => {
      const next = new Set(previous);
      if (open) next.add(row.path);
      else next.delete(row.path);
      return next;
    });
  };

  const error = directories.error ?? (search.error instanceof Error ? search.error.message : null);
  const status = !directories.ready
    ? directories.error
      ? null
      : "Loading files…"
    : searching && (search.isPending || searchQuery !== query.trim().slice(0, 256))
      ? "Searching files…"
      : null;
  const emptyMessage =
    directories.ready && rows.length === 0 && !status
      ? searching
        ? "No matching files."
        : "This folder is empty."
      : null;

  return (
    <div
      className="flex min-h-0 min-w-0 flex-1 flex-col bg-bg"
      data-file-browser-panel={source.key}
    >
      <div className="flex h-10 min-h-10 shrink-0 items-center gap-1 border-b border-rule-soft px-2">
        <RefreshFilesButton isPending={directories.isFetching} onRefresh={handleRefresh} />
        <FileSearchField
          ariaLabel={`Search ${projectName} files`}
          value={query}
          onValueChange={setQuery}
          onClose={() => setQuery("")}
        />
        {knownDirectories.length > 0 && !searching ? (
          <Tooltip content={allExpanded ? "Collapse all folders" : "Expand all folders"}>
            <Button
              variant="quiet"
              size="icon"
              aria-label={allExpanded ? "Collapse all folders" : "Expand all folders"}
              onClick={toggleAllDirectories}
            >
              {allExpanded ? (
                <ChevronsDownUp aria-hidden className="size-3.5" />
              ) : (
                <ChevronsUpDown aria-hidden className="size-3.5" />
              )}
            </Button>
          </Tooltip>
        ) : null}
      </div>
      {error ? (
        <div role="alert" className="flex items-start gap-2 p-3 text-[0.78rem] leading-relaxed">
          <span className="min-w-0 flex-1 text-danger">{error}</span>
          <Button size="sm" onClick={handleRefresh}>
            Try again
          </Button>
        </div>
      ) : null}
      {directories.truncated.size > 0 && !searching ? (
        <div className="px-3 py-1 text-[0.75rem] text-muted">Some entries are not shown.</div>
      ) : null}
      {searching && (search.data?.length ?? 0) >= SEARCH_LIMIT ? (
        <div className="px-3 py-1 text-[0.75rem] text-muted">
          More matches available. Refine your search.
        </div>
      ) : null}
      {status ? (
        <div role="status" className="px-3 py-1 text-[0.75rem] text-muted">
          {status}
        </div>
      ) : null}
      {emptyMessage ? <p className="px-3 py-2 text-[0.8rem] text-muted">{emptyMessage}</p> : null}
      <VirtualTree
        label={`${projectName} files`}
        rows={rows}
        selectedPath={selectedPath}
        revealPath={selectedPath}
        onOpen={(row) => onOpenFile(row.path)}
        onToggle={toggle}
        {...(onMention ? { onMention: (row: TreeRow) => onMention(row.path) } : {})}
      />
    </div>
  );
}
