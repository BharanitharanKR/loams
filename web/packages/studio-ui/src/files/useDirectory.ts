// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/useDirectoryEntries.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// rewritten on TanStack Query over a FilesSource (no Effect atoms): one query per opened folder, file and image, and the daemon's change stream invalidates them. Loams' changes are Apache-2.0.

// The tree and viewer's server state (SD1 Task 21; console-ui-stack decision 4). As
// upstream, only the folders someone opened are listed, and a collapsed folder keeps its
// children cached. Each folder, file and image is a query in the console's one cache,
// keyed by the source (its host and checkout), and `useFilesWatch` turns agentd's
// `WatchWorkspaceFiles` batches into invalidations: a change lists its folder again and
// reads its file again; a resync drops everything.

import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { fileBreadcrumbParent } from "./filePath";
import { WORKSPACE_IMAGE_MIME_TYPES } from "./filePreview";
import type { DirectoryListing, FileEntry, FilesSource } from "./source";

const ROOT = "studio.files";

export const filesQueryKey = (source: FilesSource, ...rest: string[]) => [
  ROOT,
  source.key,
  ...rest,
];

let collator: Intl.Collator | undefined;

/** Folders first, then by name as a person sorts them (`file2` before `file10`). */
export function compareEntries(left: FileEntry, right: FileEntry): number {
  const leftDir = left.kind === "directory";
  if (leftDir !== (right.kind === "directory")) return leftDir ? -1 : 1;
  collator ??= new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });
  return collator.compare(left.name, right.name);
}

function childrenOf(listing: DirectoryListing): FileEntry[] {
  // A listing names its own children only; anything else is dropped, as upstream does.
  return listing.entries
    .filter((entry) => (fileBreadcrumbParent(entry.path) ?? "") === listing.directory)
    .toSorted(compareEntries);
}

export interface DirectoryState {
  /** Each loaded folder's children, sorted; "" is the root. */
  readonly children: ReadonlyMap<string, readonly FileEntry[]>;
  /** Folders still on their first load. */
  readonly loading: ReadonlySet<string>;
  /** Folders whose listing was cut short by the daemon. */
  readonly truncated: ReadonlySet<string>;
  readonly error: string | null;
  readonly ready: boolean;
  readonly isFetching: boolean;
  refresh(): void;
}

/** Lists the root and every folder in `open`. */
export function useDirectories(source: FilesSource, open: Iterable<string>): DirectoryState {
  const client = useQueryClient();
  const directories = ["", ...[...open].filter((path) => path !== "")];
  const results = useQueries({
    queries: directories.map((directory) => ({
      queryKey: filesQueryKey(source, "dir", directory),
      queryFn: ({ signal }: { signal: AbortSignal }) => source.listDirectory(directory, signal),
      staleTime: 30_000,
    })),
  });
  const children = new Map<string, readonly FileEntry[]>();
  const loading = new Set<string>();
  const truncated = new Set<string>();
  let error: string | null = null;
  results.forEach((result, index) => {
    const directory = directories[index] ?? "";
    if (result.data) {
      children.set(directory, childrenOf(result.data));
      if (result.data.truncated) truncated.add(directory);
    } else if (result.isPending) {
      loading.add(directory);
    }
    if (result.error && error === null) {
      error = result.error instanceof Error ? result.error.message : "Unable to load folder.";
    }
  });
  return {
    children,
    loading,
    truncated,
    error,
    ready: children.has(""),
    isFetching: results.some((result) => result.isFetching),
    refresh: () => void client.invalidateQueries({ queryKey: filesQueryKey(source, "dir") }),
  };
}

/** One folder's children (the breadcrumb and search views). */
export function useDirectory(source: FilesSource, directory: string) {
  return useQuery({
    queryKey: filesQueryKey(source, "dir", directory),
    queryFn: ({ signal }) => source.listDirectory(directory, signal),
    staleTime: 30_000,
    select: childrenOf,
  });
}

export function useFileText(source: FilesSource, path: string | null) {
  return useQuery({
    queryKey: filesQueryKey(source, "file", path ?? ""),
    queryFn: ({ signal }) => source.readFile(path ?? "", signal),
    enabled: path !== null,
  });
}

export type ImageState =
  | { readonly state: "loading" }
  | { readonly state: "error"; readonly message: string; readonly retry: () => void }
  | { readonly state: "ready"; readonly url: string };

/**
 * A checkout image as a `blob:` URL made from the bytes agentd returns. Nothing is fetched
 * by URL, and only the image types the viewer knows are shown.
 */
export function useImageUrl(source: FilesSource, path: string): ImageState {
  const query = useQuery({
    queryKey: filesQueryKey(source, "image", path),
    queryFn: ({ signal }) => source.readImage(path, signal),
  });
  const [url, setUrl] = useState<string | null>(null);
  const data = query.data;
  const allowed = data !== undefined && WORKSPACE_IMAGE_MIME_TYPES.has(data.mimeType);
  useEffect(() => {
    if (!data || !allowed) {
      setUrl(null);
      return;
    }
    const next = URL.createObjectURL(new Blob([data.bytes as BlobPart], { type: data.mimeType }));
    setUrl(next);
    return () => URL.revokeObjectURL(next);
  }, [data, allowed]);
  if (query.error) {
    return {
      state: "error",
      message: query.error instanceof Error ? query.error.message : "Unable to load the image.",
      retry: () => void query.refetch(),
    };
  }
  if (data && !allowed) {
    return {
      state: "error",
      message: "This file is not an image this viewer shows.",
      retry: () => void query.refetch(),
    };
  }
  return url ? { state: "ready", url } : { state: "loading" };
}

/**
 * Follows the checkout's changes for as long as the caller is mounted: a changed path
 * lists its folder again (and the old folder, for a rename) and reads its file again.
 */
export function useFilesWatch(source: FilesSource): void {
  const client = useQueryClient();
  useEffect(
    () =>
      source.watch((batch) => {
        if (batch.resyncRequired) {
          void client.invalidateQueries({ queryKey: filesQueryKey(source) });
          return;
        }
        for (const change of batch.changes) {
          for (const path of [change.path, change.oldPath]) {
            if (path === undefined) continue;
            const parent = fileBreadcrumbParent(path) ?? "";
            void client.invalidateQueries({ queryKey: filesQueryKey(source, "dir", parent) });
            void client.invalidateQueries({ queryKey: filesQueryKey(source, "file", path) });
            void client.invalidateQueries({ queryKey: filesQueryKey(source, "image", path) });
            // A removed or renamed folder takes its own listing with it.
            void client.invalidateQueries({ queryKey: filesQueryKey(source, "dir", path) });
          }
        }
      }),
    [client, source],
  );
}
