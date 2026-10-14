// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/ProjectFilePicker.tsx at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// on @loams/ui's CommandPalette over FilesSource.searchFiles (agentd's SearchWorkspaceFiles) with TanStack Query; no Effect atoms, right-panel store or Pierre icons. Loams' changes are Apache-2.0.

// Quick open (SD1 Task 21): type part of a path, pick a file, and it opens in the viewer.
// The daemon ranks (fuzzy, nucleo); `rankFileMatches` keeps its best first, drops folders
// and marks the letters that matched.

import { type CommandItem, CommandPalette } from "@loams/ui";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { FileIcon } from "lucide-react";
import { type ReactNode, useMemo, useState } from "react";

import { useDebouncedValue } from "./FileBrowser";
import { PROJECT_FILE_PICKER_RESULT_LIMIT, rankFileMatches } from "./filePickerLogic";
import type { FilesSource } from "./source";
import { filesQueryKey } from "./useDirectory";

function HighlightedFuzzyText(props: {
  readonly active: boolean;
  readonly indices: ReadonlyArray<number>;
  readonly value: string;
}) {
  if (!props.active) return props.value;

  const parts: ReactNode[] = [];
  let start = 0;
  for (const index of props.indices) {
    if (start < index) parts.push(props.value.slice(start, index));
    parts.push(
      <strong className="font-semibold text-ink" key={index}>
        {props.value[index]}
      </strong>,
    );
    start = index + 1;
  }
  if (start < props.value.length) parts.push(props.value.slice(start));

  return <span className="text-muted">{parts}</span>;
}

function emptyStateMessage(query: string, error: string | null, isPending: boolean): string {
  if (error) return error;
  const isSearching = query.trim().length > 0;
  if (isPending) return "Searching workspace files…";
  return isSearching ? "No matching files." : "Type part of a file's path.";
}

export function FilePicker(props: {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly source: FilesSource;
  readonly projectName: string;
  readonly onOpenFile: (relativePath: string) => void;
}) {
  const { source } = props;
  const [query, setQuery] = useState("");
  const matchedQuery = useDebouncedValue(query.trim().slice(0, 256), 120);
  const search = useQuery({
    queryKey: filesQueryKey(source, "search", matchedQuery),
    queryFn: ({ signal }) =>
      source.searchFiles(matchedQuery, PROJECT_FILE_PICKER_RESULT_LIMIT, signal),
    enabled: props.open && matchedQuery.length > 0,
    placeholderData: keepPreviousData,
    staleTime: 10_000,
  });
  const matches = useMemo(
    () => (matchedQuery ? rankFileMatches(search.data ?? [], matchedQuery) : []),
    [search.data, matchedQuery],
  );
  const items = useMemo<CommandItem[]>(
    () =>
      matches.map((match) => ({
        id: match.path,
        text: match.path,
        label: <HighlightedFuzzyText active value={match.name} indices={match.nameMatchIndices} />,
        detail: <HighlightedFuzzyText active value={match.path} indices={match.pathMatchIndices} />,
        icon: <FileIcon aria-hidden />,
      })),
    [matches],
  );
  const error = search.error instanceof Error ? search.error.message : null;

  return (
    <CommandPalette
      open={props.open}
      onOpenChange={(open) => {
        if (!open) setQuery("");
        props.onOpenChange(open);
      }}
      label={`Go to a file in ${props.projectName}`}
      placeholder="Search files…"
      items={items}
      filter={null}
      query={query}
      onQueryChange={setQuery}
      empty={emptyStateMessage(query, error, search.isFetching)}
      onSelect={(item) => {
        setQuery("");
        props.onOpenChange(false);
        props.onOpenFile(item.id);
      }}
      footer={
        <>
          <span>{props.projectName}</span>
          <span>↵ to open</span>
        </>
      }
    />
  );
}
