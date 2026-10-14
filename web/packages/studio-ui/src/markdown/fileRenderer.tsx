// Markdown files in the file viewer (SD1 Task 18 for Task 21's seam, ruling T21-3): the
// same sanitised renderer as the chat (§53 D919: no raw HTML, no remote images, http(s)
// links only). A relative link in a file is relative to that file's directory, and it
// never leaves the checkout (`..` is refused by `workspacePathHref`).

import type { ReactNode } from "react";
import { Markdown, workspacePathHref } from "./Markdown";

/** `href` (already a workspace path) resolved against the directory of `fromPath`. */
export function resolveMarkdownFileLink(fromPath: string, href: string): string {
  const slash = fromPath.lastIndexOf("/");
  const dir = slash >= 0 ? fromPath.slice(0, slash) : "";
  return dir ? `${dir}/${href}` : href;
}

export interface MarkdownFileRendererOptions {
  /** Opens an http(s) link outside the console; without it the link opens in a new tab. */
  onOpenLink?: (href: string) => void;
  /** Opens a workspace file the document links to (a path from the checkout's root). */
  onOpenFile?: (path: string) => void;
}

/** The file viewer's `renderMarkdown(text, path)`. */
export function createMarkdownFileRenderer(
  options: MarkdownFileRendererOptions = {},
): (text: string, path: string) => ReactNode {
  const { onOpenLink, onOpenFile } = options;
  return (text, path) => (
    <Markdown
      text={text}
      {...(onOpenLink ? { onOpenLink } : {})}
      {...(onOpenFile
        ? {
            onOpenFile: (href: string) => {
              const relative = workspacePathHref(href);
              if (relative) onOpenFile(resolveMarkdownFileLink(path, relative));
            },
          }
        : {})}
    />
  );
}
