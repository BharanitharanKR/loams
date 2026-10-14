// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// packages/shared/src/filePreview.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// only the workspace image extensions and their types; no video, audio, PDF or HTML preview (Studio renders none). Loams' changes are Apache-2.0.

export const WORKSPACE_IMAGE_PREVIEW_EXTENSIONS = [
  ".avif",
  ".gif",
  ".ico",
  ".jpeg",
  ".jpg",
  ".png",
  ".svg",
  ".webp",
] as const;

const IMAGE_MIME_TYPE_BY_EXTENSION = new Map([
  [".avif", "image/avif"],
  [".gif", "image/gif"],
  [".ico", "image/x-icon"],
  [".jpeg", "image/jpeg"],
  [".jpg", "image/jpeg"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"],
]);

/** The image types the viewer shows; anything else the daemon answers with is refused. */
export const WORKSPACE_IMAGE_MIME_TYPES: ReadonlySet<string> = new Set(
  IMAGE_MIME_TYPE_BY_EXTENSION.values(),
);

function hasPreviewExtension(path: string, extensions: ReadonlyArray<string>): boolean {
  const literalPath = path.toLowerCase();
  return extensions.some((extension) => literalPath.endsWith(extension));
}

export function isWorkspaceImagePreviewPath(path: string): boolean {
  return hasPreviewExtension(path, WORKSPACE_IMAGE_PREVIEW_EXTENSIONS);
}
