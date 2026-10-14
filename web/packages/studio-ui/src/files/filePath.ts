// Portions of this file are from T3 Code (https://github.com/pingdotgg/t3code),
// apps/web/src/components/files/filePath.ts at commit 6524a8d4, Copyright (c) 2026 T3 Tools Inc., MIT licence
// (see web/packages/studio-ui/LICENSE.t3code). Modified by The Loams Authors:
// workspace-relative paths only (agentd jails every read to the checkout), plus the parent and ancestors helpers. Loams' changes are Apache-2.0.

export interface FileBreadcrumb {
  label: string;
  path: string;
  kind: "project" | "directory" | "file";
}

/** Crumbs for a workspace-relative path start at the project. */
export function fileBreadcrumbs(projectName: string, relativePath: string): FileBreadcrumb[] {
  const parts = relativePath.split("/").filter(Boolean);
  return [
    { label: projectName, path: "", kind: "project" as const },
    ...parts.map((part, index) => ({
      label: part,
      path: parts.slice(0, index + 1).join("/"),
      kind: index === parts.length - 1 ? ("file" as const) : ("directory" as const),
    })),
  ];
}

export function fileBreadcrumbParent(directoryPath: string): string | null {
  if (!directoryPath) return null;
  const separatorIndex = directoryPath.lastIndexOf("/");
  return separatorIndex === -1 ? "" : directoryPath.slice(0, separatorIndex);
}

/** The folders that hold a path, outermost first: `a/b/c.ts` → `a`, `a/b`. */
export function fileAncestors(path: string): string[] {
  const parts = path.split("/").filter(Boolean);
  return parts.slice(0, -1).map((_, index) => parts.slice(0, index + 1).join("/"));
}
