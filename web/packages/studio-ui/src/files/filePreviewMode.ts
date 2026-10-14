// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/filePreviewMode.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// the read-only viewer's modes over agentd's read results; no assets, task lists or host paths. Loams' changes are Apache-2.0.

import { filePreviewDelimiter } from "./delimitedPreview";
import { isWorkspaceImagePreviewPath } from "./filePreview";
import type { ReadOnlyReason } from "./source";

export const isMarkdownPreviewFile = (path: string): boolean => /\.(?:md|mdx)$/i.test(path);

/** How the viewer shows a path: an image from its bytes, or text in one of three ways. */
export type FilePreviewMode =
  | { readonly kind: "image" }
  | { readonly kind: "markdown" }
  | { readonly kind: "table"; readonly delimiter: "," | "\t" }
  | { readonly kind: "source" };

export function filePreviewMode(path: string): FilePreviewMode {
  if (isWorkspaceImagePreviewPath(path)) return { kind: "image" };
  if (isMarkdownPreviewFile(path)) return { kind: "markdown" };
  const delimiter = filePreviewDelimiter({ name: path });
  return delimiter ? { kind: "table", delimiter } : { kind: "source" };
}

/** Describe why there is no text, without the daemon's underlying cause. */
export function readOnlyReasonMessage(reason: ReadOnlyReason | undefined): string {
  switch (reason) {
    case "binary":
      return "The file is binary and cannot be displayed as text.";
    case "unsupported-encoding":
      return "The file is not UTF-8 text, so it is not shown.";
    case "not-regular-file":
      return "The path is a directory or special file, not a regular file.";
    case "outside-workspace":
      return "The path resolves to a location outside the workspace.";
    case "permission-denied":
      return "The daemon is not allowed to read this file.";
    case "too-large":
      return "The file is too large to show.";
    default:
      return "The file could not be shown as text.";
  }
}
