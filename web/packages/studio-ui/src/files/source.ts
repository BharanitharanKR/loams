// The file tree and viewer's data (SD1 Task 21; design §53 §5.5, inventory (e)). The
// components read a checkout through `FilesSource`, plain async calls that the Studio
// plugin implements over `loams.agentd.v1.FileService` (ListDirectory, ReadFile,
// ReadImage, Search, WatchFiles). Content search is its own seam, `ContentSearchSource`:
// `SearchWorkspaceContent` (G23) is SD1 Task 11's, so until that lands the plugin passes
// `pendingContentSearch()`, which says so instead of searching.
//
// Everything a source returns is data from the checkout: the components render it as
// text and never as HTML (§53 D919).

export type EntryKind = "file" | "directory" | "symlink";

export interface FileEntry {
  /** Relative to the checkout, `/`-separated, no leading slash. */
  readonly path: string;
  readonly name: string;
  readonly kind: EntryKind;
  readonly ignored?: boolean;
  readonly size?: number;
}

export interface DirectoryListing {
  /** The directory listed; "" is the checkout's root. */
  readonly directory: string;
  readonly entries: readonly FileEntry[];
  /** The daemon stopped early (a very large directory). */
  readonly truncated: boolean;
}

/** Why the daemon would not return a file's text. */
export type ReadOnlyReason =
  | "binary"
  | "unsupported-encoding"
  | "mixed-line-endings"
  | "symlink"
  | "too-large"
  | "permission-denied"
  | "not-regular-file"
  | "outside-workspace";

export interface FileText {
  readonly path: string;
  /** Absent when the file is binary or could not be decoded. */
  readonly text: string | null;
  readonly size: number;
  /** Only the first part of a large file was read. */
  readonly truncated: boolean;
  readonly contentHash?: string;
  readonly readOnlyReason?: ReadOnlyReason;
}

export interface ImageBytes {
  readonly mimeType: string;
  readonly bytes: Uint8Array;
}

export type FileChangeKind = "created" | "modified" | "removed" | "renamed";

export interface FileChange {
  readonly kind: FileChangeKind;
  readonly path: string;
  readonly oldPath?: string;
}

export interface FileChanges {
  /** The watcher missed changes: everything must be listed again. */
  readonly resyncRequired: boolean;
  readonly changes: readonly FileChange[];
}

export interface FileMatch {
  readonly path: string;
  readonly name: string;
  readonly kind: EntryKind;
  /** Higher is better; the daemon's fuzzy score. */
  readonly score: number;
}

/** One checkout, as the tree, the viewer and quick open read it. */
export interface FilesSource {
  /** Distinguishes checkouts (and hosts) in the query cache. */
  readonly key: string;
  listDirectory(directory: string, signal?: AbortSignal): Promise<DirectoryListing>;
  readFile(path: string, signal?: AbortSignal): Promise<FileText>;
  readImage(path: string, signal?: AbortSignal): Promise<ImageBytes>;
  searchFiles(query: string, limit: number, signal?: AbortSignal): Promise<readonly FileMatch[]>;
  /** Calls back with each batch of changes until the returned function is called. */
  watch(listener: (changes: FileChanges) => void): () => void;
}

export interface ContentMatch {
  readonly path: string;
  /** 1-based. */
  readonly line: number;
  /** 1-based. */
  readonly column: number;
  /** The matching line, as text. */
  readonly preview: string;
}

export interface ContentSearchQuery {
  readonly query: string;
  readonly caseSensitive: boolean;
  readonly regex: boolean;
  readonly maxResults: number;
}

export type ContentSearchAvailability =
  | { readonly available: true }
  | { readonly available: false; readonly reason: string };

/** Ripgrep-style search of a checkout's contents, one match at a time. */
export interface ContentSearchSource {
  readonly availability: ContentSearchAvailability;
  /** Streams matches to `onMatch` and resolves when the search ends. */
  search(
    query: ContentSearchQuery,
    onMatch: (match: ContentMatch) => void,
    signal: AbortSignal,
  ): Promise<{ readonly truncated: boolean }>;
}

/** Content search before SD1 Task 11's `SearchWorkspaceContent`: unavailable, and says why. */
export function pendingContentSearch(): ContentSearchSource {
  return {
    availability: {
      available: false,
      reason: "Searching file contents needs a newer loams-agentd. Use Go to file meanwhile.",
    },
    search: () => Promise.resolve({ truncated: false }),
  };
}
