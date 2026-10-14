// The file tree and viewer (SD1 Task 21; inventory (e)).

export { FileBreadcrumbs } from "./FileBreadcrumbs";
export { FileBrowser, useDebouncedValue } from "./FileBrowser";
export { FilePicker } from "./FilePicker";
export { FileViewer, type FileViewerProps } from "./FileViewer";
export { fileAncestors, fileBreadcrumbs } from "./filePath";
export { type ProjectFilePickerMatch, rankFileMatches } from "./filePickerLogic";
export { isWorkspaceImagePreviewPath } from "./filePreview";
export { type FilePreviewMode, filePreviewMode, isMarkdownPreviewFile } from "./filePreviewMode";
export { ContentSearchDialog } from "./search/ContentSearchDialog";
export { matchRanges } from "./search/HighlightedSearchLine";
export {
  type ContentMatch,
  type ContentSearchAvailability,
  type ContentSearchQuery,
  type ContentSearchSource,
  type DirectoryListing,
  type EntryKind,
  type FileChange,
  type FileChangeKind,
  type FileChanges,
  type FileEntry,
  type FileMatch,
  type FilesSource,
  type FileText,
  type ImageBytes,
  pendingContentSearch,
  type ReadOnlyReason,
} from "./source";
export { filesQueryKey, useFilesWatch } from "./useDirectory";
export { type TreeRow, VirtualTree } from "./VirtualTree";
